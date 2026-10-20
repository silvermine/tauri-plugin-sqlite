//! Consumer-supplied scalar SQL functions, applied to every pooled connection.
//!
//! This module doc is the canonical statement of the scalar-function contract. The
//! READMEs, the plugin builder docs, and the CHANGELOG point here.
//!
//! # The contract
//!
//! A function belongs to a *connection*, not to a database file. SQLite keeps
//! user-defined functions in per-connection state, so every connection that runs a query
//! naming one must have it registered. [`register_function`] records a function in a
//! process-global set. [`crate::SqliteDatabase::connect`] snapshots that set, and the
//! pools' `after_connect` hook installs the snapshot on each connection they open. A
//! function is present before any caller receives a connection, and it stays present as
//! the pools drop idle connections and open new ones.
//!
//! Register a function before the `connect` call for the database that must serve it.
//! A database applies the functions registered before its `connect`, for every
//! connection it ever opens. A later registration applies to the next database, never to
//! part of an open one. Registration is process-global, and a function stays registered
//! for the life of the process. [`register_or_replace_function`] puts a different handler
//! under a name already registered, for the databases that connect after it.
//!
//! [`ScalarFunction::invocation_scope`] decides where SQLite accepts a call.
//! [`InvocationScope::DirectOnly`] confines the function to top-level SQL, so SQLite
//! refuses a call from inside a schema object: a view, a trigger, a CHECK constraint, a
//! DEFAULT clause, an expression index, a partial index, or a generated column. The error
//! reads `unsafe use of <name>()`. Direct-only keeps a handler out of the schema of a
//! database this application did not write, such as an attached file from sync or import.
//! It also stops a caller from naming the function in an expression index or a generated
//! column, which writes the name into the schema and leaves the table unreadable by any
//! process that opens the file without registering the function. The other two scopes
//! accept both of those risks in exchange for a function that a view or an index can
//! call.
//!
//! The handler is `Send + Sync + 'static` because sqlx runs every SQLite connection on
//! its own thread. Every connection of every database shares one handler. The handler
//! receives every argument, including SQL NULL, and decides what NULL means. Returning
//! [`SqlValue::Null`] produces NULL. A returned [`FunctionError`] fails the statement,
//! and the caller receives its message. A handler blocks its connection for as long as it
//! runs, and the write pool holds a single connection.
//!
//! If the application unwinds on panic (the Rust default), a panic inside the handler
//! becomes an error naming the function. Under `panic = "abort"`, the process aborts, and
//! nothing here can prevent that.
//!
//! Each of these fails at the [`register_function`] call, not later as a connection
//! error:
//!
//! - an empty name, a name over 255 bytes, or a name holding a NUL byte
//! - a negative arity
//! - a name a SQLite built-in already uses
//! - a name and arity pair already registered (names compare case-insensitively,
//!   matching how SQLite resolves them)
//! - a function the linked SQLite library refuses, such as an arity above its
//!   `SQLITE_MAX_FUNCTION_ARG`
//!
//! Aggregate functions, window functions, collations, and virtual tables are not
//! supported.

use std::borrow::Cow;
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, OnceLock};

use libsqlite3_sys as ffi;
use parking_lot::RwLock;
use sqlx::sqlite::SqliteConnection;
use tracing::{error, trace};

use crate::Result;
use crate::error::Error;

/// SQLite's own limit on the length of a function name, in bytes.
/// See <https://www.sqlite.org/c3ref/create_function.html>.
const MAX_NAME_BYTES: usize = 255;

/// The registered functions, in registration order.
static FUNCTIONS: OnceLock<RwLock<Vec<Arc<Registration>>>> = OnceLock::new();

fn functions() -> &'static RwLock<Vec<Arc<Registration>>> {
   FUNCTIONS.get_or_init(|| RwLock::new(Vec::new()))
}

/// The set of functions a database captures at connect time.
///
/// Both of a database's pools install this exact set on every connection they open, so
/// one database never serves connections with differing function sets.
pub(crate) type FunctionSet = Arc<Vec<Arc<Registration>>>;

/// The registered set at this moment. Called once per [`crate::SqliteDatabase::connect`].
pub(crate) fn snapshot() -> FunctionSet {
   Arc::new(functions().read().clone())
}

/// An owned value returned from a scalar function, or read from a raw SQLite value.
///
/// The five variants are SQLite's five storage classes. Returning [`SqlValue::Null`]
/// from a handler produces SQL NULL. Handlers receive their arguments as the borrowed
/// [`SqlValueRef`] instead.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
   Null,
   Integer(i64),
   Real(f64),
   Text(String),
   Blob(Vec<u8>),
}

/// A borrowed value passed to a scalar function.
///
/// Text and blob variants point into SQLite's own buffers, so a handler reads its
/// arguments without a copy. [`SqlValueRef::to_owned`] converts one into a [`SqlValue`].
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValueRef<'a> {
   Null,
   Integer(i64),
   Real(f64),
   Text(Cow<'a, str>),
   Blob(&'a [u8]),
}

impl SqlValue {
   /// Whether this value is SQL NULL.
   pub fn is_null(&self) -> bool {
      matches!(self, SqlValue::Null)
   }

   /// This value as an integer, when it holds one.
   pub fn as_integer(&self) -> Option<i64> {
      match self {
         SqlValue::Integer(value) => Some(*value),
         _ => None,
      }
   }

   /// This value as a float, when it holds one.
   pub fn as_real(&self) -> Option<f64> {
      match self {
         SqlValue::Real(value) => Some(*value),
         _ => None,
      }
   }

   /// This value as a string reference, when it holds text.
   pub fn as_text(&self) -> Option<&str> {
      match self {
         SqlValue::Text(text) => Some(text),
         _ => None,
      }
   }

   /// This value as a blob reference, when it holds one.
   pub fn as_blob(&self) -> Option<&[u8]> {
      match self {
         SqlValue::Blob(bytes) => Some(bytes),
         _ => None,
      }
   }
}

impl SqlValueRef<'_> {
   /// Copy this borrowed value into an owned [`SqlValue`].
   pub fn to_owned(&self) -> SqlValue {
      match self {
         SqlValueRef::Null => SqlValue::Null,
         SqlValueRef::Integer(value) => SqlValue::Integer(*value),
         SqlValueRef::Real(value) => SqlValue::Real(*value),
         SqlValueRef::Text(text) => SqlValue::Text(text.to_string()),
         SqlValueRef::Blob(bytes) => SqlValue::Blob(bytes.to_vec()),
      }
   }
}

/// A scalar function's failure.
///
/// The message becomes the SQLite error for the statement that invoked the function. See
/// [`ScalarFunction`] for what the caller sees.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct FunctionError {
   message: String,
}

impl FunctionError {
   /// Build an error from `message`.
   pub fn new(message: impl Into<String>) -> Self {
      Self {
         message: message.into(),
      }
   }

   /// The message this error reports to the caller.
   pub fn message(&self) -> &str {
      &self.message
   }
}

/// The closure that computes a scalar function's result.
///
/// The bound is `Send + Sync + 'static` because sqlx runs every SQLite connection on its
/// own thread. Every connection of every database shares one handler: up to
/// `max_read_connections` readers plus one writer per database.
pub type ScalarHandler =
   Arc<dyn Fn(&[SqlValueRef<'_>]) -> std::result::Result<SqlValue, FunctionError> + Send + Sync>;

/// Where SQLite accepts a call to a registered function.
///
/// A function a schema object can call runs whenever anything reads through that object,
/// including an object in a database file this application did not write. Pick
/// [`InvocationScope::DirectOnly`] unless the function has a reason to appear in a view, a
/// trigger, or an index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum InvocationScope {
   /// Top-level SQL only, through the `SQLITE_DIRECTONLY` flag. SQLite refuses a call
   /// from inside a view, a trigger, a CHECK constraint, a DEFAULT clause, an expression
   /// index, a partial index, or a generated column, with the error
   /// `unsafe use of <name>()`.
   #[default]
   DirectOnly,

   /// Schema objects as well, on a connection that trusts the schema. SQLite trusts the
   /// schema unless a caller runs `PRAGMA trusted_schema = OFF`. This scope sets neither
   /// `SQLITE_DIRECTONLY` nor `SQLITE_INNOCUOUS`.
   Schema,

   /// Schema objects as well, on every connection, including one that turns trusted
   /// schema off. This scope sets `SQLITE_INNOCUOUS`, which promises SQLite that the
   /// handler reads nothing but its arguments and changes nothing but its result. A
   /// handler that reads a file, opens a socket, or touches process state breaks that
   /// promise, and SQLite has no way to detect it.
   InnocuousSchema,
}

/// A scalar SQL function to register on every connection.
///
/// # Example
///
/// ```no_run
/// use std::sync::Arc;
/// use sqlx_sqlite_conn_mgr::{
///    FunctionError, InvocationScope, ScalarFunction, SqlValue, SqlValueRef,
///    register_function,
/// };
///
/// # fn example() -> sqlx_sqlite_conn_mgr::Result<()> {
/// register_function(ScalarFunction {
///    name: "normalize_for_search".into(),
///    arity: 1,
///    deterministic: true,
///    invocation_scope: InvocationScope::DirectOnly,
///    // SQLite's own lower() folds ASCII only, so Unicode case folding needs Rust.
///    handler: Arc::new(|args: &[SqlValueRef]| match &args[0] {
///       SqlValueRef::Text(text) => Ok(SqlValue::Text(text.to_lowercase())),
///       SqlValueRef::Null => Ok(SqlValue::Null),
///       _ => Err(FunctionError::new("normalize_for_search expects text")),
///    }),
/// })?;
/// # Ok(())
/// # }
/// ```
pub struct ScalarFunction {
   /// The SQL identifier, for example `normalize_for_search`. The name is at most 255
   /// bytes long, with no NUL byte. SQLite resolves it case-insensitively.
   pub name: String,

   /// The exact number of arguments the function takes. Two registrations can share a
   /// name when their arities differ. SQLite overloads a name this way.
   pub arity: i32,

   /// Whether SQLite can treat the result as stable for the same inputs. SQLite uses this
   /// to cache the result and reorder calls. Set this field only for a handler that reads
   /// nothing but its arguments.
   pub deterministic: bool,

   /// Where SQLite accepts a call to this function. [`InvocationScope::DirectOnly`] is
   /// the safe choice, and the one a caller with no view, trigger, or index to serve
   /// wants.
   pub invocation_scope: InvocationScope,

   /// The closure that computes the result. A returned error fails the statement and
   /// reports its message to the caller. If the application unwinds on panic (the
   /// default), a panic inside the closure becomes an error naming the function. Under
   /// `panic = "abort"`, the process aborts, and nothing here can prevent that.
   pub handler: ScalarHandler,
}

/// One registered function, with everything the FFI layer needs precomputed.
pub(crate) struct Registration {
   /// Kept beside `c_name` for the duplicate check and for error messages, so neither
   /// path must convert a `CString` back to a `str`.
   name: String,
   c_name: CString,
   arity: i32,
   flags: c_int,
   handler: ScalarHandler,
}

/// Register a scalar function on every connection the crate opens for databases that
/// connect from now on.
///
/// See the [module documentation](self) for the full contract. This function validates
/// the name and the arity, and asks the linked SQLite library to accept the function on a
/// throwaway connection. A bad registration fails here, not later as a connection error.
/// Registration is process-global: the function applies to every database whose `connect`
/// runs after this call, and no caller can remove it.
///
/// # Errors
///
/// Returns:
///
/// - [`Error::InvalidFunctionName`] for an empty name, a name over 255 bytes, or a name
///   holding an interior NUL byte.
/// - [`Error::InvalidFunctionArity`] for a negative arity. SQLite reads a negative arity
///   as variadic, and this crate does not offer variadic functions.
/// - [`Error::DuplicateFunction`] when a registration already has the same name and
///   arity. This crate compares names case-insensitively, to match how SQLite resolves
///   them. Without this check, a second registration replaces the first instead of the
///   two coexisting.
/// - [`Error::ShadowsBuiltinFunction`] when a SQLite built-in already uses the name with
///   this argument count, or variadically. SQLite silently replaces a built-in with an
///   application-defined function, so every statement on the connection that names the
///   built-in reaches the handler instead, including one inside a schema object this
///   application did not write.
/// - [`Error::FunctionRefused`] when the linked library refuses the function for a reason
///   of its own, such as an arity above its `SQLITE_MAX_FUNCTION_ARG`.
/// - [`Error::ProbeConnectionFailed`] when the in-memory connection that carries these
///   checks fails to open or query. Nothing judged the function in that case.
///
pub fn register_function(function: ScalarFunction) -> Result<()> {
   store(function, OnDuplicate::Reject)
}

/// Register a scalar function, replacing the one already registered under the same name
/// and arity.
///
/// Use this where a repeat registration is the caller's intent, such as a setup path that
/// can run twice. [`register_function`] rejects the repeat instead, because a rebuilt
/// handler is a new closure and the registry cannot tell an intended repeat from two call
/// sites that disagree about one name.
///
/// The replacement reaches the databases that connect after this call. A database that
/// already connected holds the set it captured, and it serves that handler for as long as
/// the database lives. The path registry holds a weak reference, so a path whose last
/// handle drops connects fresh and captures the current set. This is the rule every
/// registration follows: a change applies to the next database to connect, never to part
/// of an open one.
///
/// # Errors
///
/// Returns every error [`register_function`] returns except [`Error::DuplicateFunction`],
/// which is the case this function accepts.
pub fn register_or_replace_function(function: ScalarFunction) -> Result<()> {
   store(function, OnDuplicate::Replace)
}

/// What to do with a registration that already holds the name and arity.
enum OnDuplicate {
   Reject,
   Replace,
}

/// Validate, probe, and store a registration. The two public entry points differ only in
/// what they do about a name and arity already registered.
fn store(function: ScalarFunction, on_duplicate: OnDuplicate) -> Result<()> {
   let ScalarFunction {
      name,
      arity,
      deterministic,
      invocation_scope,
      handler,
   } = function;

   let c_name = validate_name(&name)?;

   if arity < 0 {
      return Err(Error::InvalidFunctionArity(arity));
   }

   // The connection is opened as UTF-8, so text crosses the boundary as UTF-8 in both
   // directions.
   //
   // The scope is the caller's, because only the caller knows whether a view or an index
   // must call the function. `DirectOnly` is the default the enum documents: a registered
   // function applies to every database this crate serves, including one attached from an
   // untrusted file whose views and triggers this application did not write.
   let mut flags = ffi::SQLITE_UTF8;

   match invocation_scope {
      InvocationScope::DirectOnly => flags |= ffi::SQLITE_DIRECTONLY,
      InvocationScope::Schema => {}
      InvocationScope::InnocuousSchema => flags |= ffi::SQLITE_INNOCUOUS,
   }

   if deterministic {
      flags |= ffi::SQLITE_DETERMINISTIC;
   }

   let registration = Arc::new(Registration {
      name,
      c_name,
      arity,
      flags,
      handler,
   });

   // Held across the match, the probe, and the store, so two threads registering the same
   // name cannot both pass the check. The probe opens a connection of its own, and
   // registration belongs to startup, so the wait this adds costs nothing.
   let mut registered = functions().write();

   let matched = registered.iter().position(|existing| {
      existing.arity == registration.arity && existing.name.eq_ignore_ascii_case(&registration.name)
   });

   // Before the probe: a rejected duplicate is the caller's own doing, and the probe
   // cannot report it. Probing first hides a duplicate behind `FunctionRefused` whenever
   // the linked library also rejects the arity.
   if matched.is_some() && matches!(on_duplicate, OnDuplicate::Reject) {
      return Err(Error::DuplicateFunction {
         name: registration.name.clone(),
         arity,
      });
   }

   probe(&registration)?;

   // A replacement takes the position it replaces, so the set stays in registration order.
   match matched {
      Some(index) => registered[index] = registration,
      None => registered.push(registration),
   }

   Ok(())
}

/// Validate a function name and convert it to the NUL-terminated form SQLite needs.
///
/// `CString::new` enforces the no-interior-NUL rule, so the conversion and the last
/// validation rule are the same step.
fn validate_name(name: &str) -> Result<CString> {
   if name.is_empty() || name.len() > MAX_NAME_BYTES {
      return Err(Error::InvalidFunctionName(name.to_string()));
   }

   CString::new(name).map_err(|_| Error::InvalidFunctionName(name.to_string()))
}

/// Check `registration` against the linked SQLite library on a throwaway in-memory
/// connection: the name must not replace a built-in, and the library must accept the
/// registration.
///
/// The check runs here because a rejection at connect time tells the caller nothing
/// useful. sqlx discards the `after_connect` error. Once the acquire deadline passes,
/// sqlx reports `PoolTimedOut` instead, and that error names neither the function nor the
/// reason. A returned error from this function names both, at the call that registered
/// the function.
///
/// The name-length rule this crate applies first comes from SQLite's documented API
/// contract. The linked library is the platform's own, built with limits this crate
/// cannot read directly. The probe asks that library directly, using a connection built
/// the same way as every pooled connection.
fn probe(registration: &Arc<Registration>) -> Result<()> {
   let mut db: *mut ffi::sqlite3 = std::ptr::null_mut();

   // SAFETY: `db` is a valid out pointer, the filename is a NUL-terminated literal, and a
   // null VFS name selects the default VFS.
   let open_code = unsafe {
      ffi::sqlite3_open_v2(
         c":memory:".as_ptr(),
         &mut db,
         ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
         std::ptr::null(),
      )
   };

   let outcome = if open_code == ffi::SQLITE_OK {
      // SAFETY: `db` points at the connection opened above, which nothing else can reach.
      unsafe { probe_on(db, registration) }
   } else {
      // A failed open means this crate cannot run the checks at all: the platform is out
      // of memory, its default VFS forbids the connection, or the library dropped
      // `:memory:` support. Naming that a refusal would tell the caller to change a
      // function that nothing has judged yet.
      Err(Error::ProbeConnectionFailed {
         name: registration.name.clone(),
         reason: describe(open_code),
      })
   };

   // SAFETY: `db` is either null, which `sqlite3_close` accepts, or the handle from the
   // call above. A failed open still produces a handle to close. `probe_on` finalizes the
   // one statement it prepares, so the connection has nothing left to finalize.
   unsafe { ffi::sqlite3_close(db) };

   outcome
}

/// The checks [`probe`] runs once its connection is open.
///
/// # Safety
///
/// `db` must point to an open sqlite3 connection nothing else is using.
unsafe fn probe_on(db: *mut ffi::sqlite3, registration: &Arc<Registration>) -> Result<()> {
   // SAFETY: `db` is open and exclusively ours (caller's guarantee).
   if unsafe { shadows_builtin(db, registration) }? {
      return Err(Error::ShadowsBuiltinFunction {
         name: registration.name.clone(),
         arity: registration.arity,
      });
   }

   // SAFETY: as above.
   let create_code = unsafe { create_function(db, registration) };

   if create_code != ffi::SQLITE_OK {
      return Err(Error::FunctionRefused {
         name: registration.name.clone(),
         arity: registration.arity,
         reason: describe(create_code),
      });
   }

   Ok(())
}

/// Whether the linked library already defines this name as a built-in the registration
/// replaces: a function with the same argument count, or a variadic one.
///
/// `sqlite3_create_function_v2` silently replaces a built-in, so every statement that
/// names it reaches the handler instead, whatever the registration's
/// [`InvocationScope`]. The probe connection holds nothing but built-ins, so a match here
/// is a built-in. `lower()` folds ASCII only, which matches how SQLite resolves function
/// names.
///
/// # Safety
///
/// `db` must point to an open sqlite3 connection nothing else is using.
unsafe fn shadows_builtin(db: *mut ffi::sqlite3, registration: &Registration) -> Result<bool> {
   // A negative `narg` marks a variadic built-in (the exact negative value encodes the
   // minimum argument count in newer SQLite versions, such as -3 for `max`). An
   // exact-arity application function takes precedence over a variadic built-in for calls
   // with that arity, so a variadic match is a replacement for those calls too.
   const SQL: &CStr = c"SELECT 1 FROM pragma_function_list \
      WHERE lower(name) = lower(?1) AND (narg = ?2 OR narg < 0) LIMIT 1";

   let probe_failure = |code: c_int| Error::ProbeConnectionFailed {
      name: registration.name.clone(),
      reason: describe(code),
   };

   let mut stmt: *mut ffi::sqlite3_stmt = std::ptr::null_mut();

   // SAFETY: `db` is open (caller's guarantee), the SQL is NUL-terminated, and `stmt` is
   // a valid out pointer.
   let prepare_code =
      unsafe { ffi::sqlite3_prepare_v2(db, SQL.as_ptr(), -1, &mut stmt, std::ptr::null_mut()) };

   if prepare_code != ffi::SQLITE_OK {
      // A failed prepare leaves `stmt` null, so there is nothing to finalize.
      return Err(probe_failure(prepare_code));
   }

   // SAFETY: `stmt` is the statement prepared above. `c_name` outlives it, so
   // `SQLITE_STATIC` applies. Its length fits in `c_int` because `validate_name` caps
   // names at 255 bytes.
   let mut code = unsafe {
      ffi::sqlite3_bind_text(
         stmt,
         1,
         registration.c_name.as_ptr(),
         registration.c_name.as_bytes().len() as c_int,
         ffi::SQLITE_STATIC(),
      )
   };

   if code == ffi::SQLITE_OK {
      // SAFETY: as above.
      code = unsafe { ffi::sqlite3_bind_int(stmt, 2, registration.arity as c_int) };
   }

   let step_code = if code == ffi::SQLITE_OK {
      // SAFETY: `stmt` is the prepared statement with both parameters bound.
      unsafe { ffi::sqlite3_step(stmt) }
   } else {
      code
   };

   // SAFETY: `stmt` came from the successful prepare above.
   unsafe { ffi::sqlite3_finalize(stmt) };

   match step_code {
      ffi::SQLITE_ROW => Ok(true),
      ffi::SQLITE_DONE => Ok(false),
      code => Err(probe_failure(code)),
   }
}

/// SQLite's own description of a result code, followed by the numeric code.
fn describe(code: c_int) -> String {
   // SAFETY: sqlite3_errstr returns a static, NUL-terminated string for any code,
   // including an unrecognized one.
   let text = unsafe { CStr::from_ptr(ffi::sqlite3_errstr(code)) }.to_string_lossy();

   format!("{text} (code {code})")
}

/// The boxed future sqlx's `after_connect` expects, borrowing the connection it
/// configures.
type HookFuture<'c> =
   std::pin::Pin<Box<dyn Future<Output = std::result::Result<(), sqlx::Error>> + Send + 'c>>;

/// Build the `after_connect` hook that installs `functions` on each connection a pool
/// opens. Both of a database's pools use one snapshot, so their connections stay uniform.
pub(crate) fn install_hook(
   functions: &FunctionSet,
) -> impl for<'c> Fn(&'c mut SqliteConnection, sqlx::pool::PoolConnectionMetadata) -> HookFuture<'c>
+ Send
+ Sync
+ 'static {
   let functions = Arc::clone(functions);

   move |conn, _meta| {
      let functions = Arc::clone(&functions);
      Box::pin(async move { apply_all(&functions, conn).await })
   }
}

/// Install `functions` on a newly opened connection.
///
/// [`install_hook`] runs this from the pools' `after_connect` hook, before sqlx hands the
/// connection to a caller, with the set the database captured at connect time. If this
/// function returns an error, sqlx closes the connection, so no caller ever gets one with
/// only some functions installed. sqlx then retries the connect until the acquire
/// deadline passes, and hands the caller `sqlx::Error::PoolTimedOut` instead, without the
/// error this function returned. [`register_function`] probes every function against the
/// same library before accepting it. A failure here means the connection ran out of
/// memory, not that the library refused a function.
async fn apply_all(
   functions: &[Arc<Registration>],
   conn: &mut SqliteConnection,
) -> std::result::Result<(), sqlx::Error> {
   // An empty set never calls `lock_handle()` and never touches FFI.
   if functions.is_empty() {
      return Ok(());
   }

   let mut handle = conn.lock_handle().await?;
   let db = handle.as_raw_handle().as_ptr();

   for registration in functions {
      // SAFETY: `db` comes from the handle we hold the lock on, so it points at an open
      // connection nothing else is using for the duration of this call.
      let code = unsafe { create_function(db, registration) };

      if code != ffi::SQLITE_OK {
         let message = format!(
            "failed to register SQL function '{}' taking {} argument(s): {}",
            registration.name,
            registration.arity,
            describe(code)
         );

         error!("{message}");

         return Err(sqlx::Error::Configuration(message.into()));
      }
   }

   trace!(
      count = functions.len(),
      "Registered scalar functions on a new connection"
   );

   Ok(())
}

/// Register one function on a raw connection handle, returning SQLite's result code.
///
/// # Safety
///
/// `db` must point to an open sqlite3 connection, and the caller must hold exclusive
/// access to that connection for the duration of the call.
unsafe fn create_function(db: *mut ffi::sqlite3, registration: &Arc<Registration>) -> c_int {
   // One strong reference per (connection, function) pair. SQLite hands the pointer back
   // to `dispatch` as user data, and passes it to `release_registration` when the
   // function is deleted: that happens when the connection closes, or when the same name
   // and arity is registered again on it. Both pools drop idle connections and open
   // fresh ones, so the reference must be scoped to the connection: one that outlives its
   // connection leaks, once per connection that replaces it.
   let user_data = Arc::into_raw(Arc::clone(registration)) as *mut c_void;

   // SAFETY: `db` is an open connection (the caller's guarantee). `c_name` is
   // NUL-terminated, and it outlives the registration because the Arc above holds it.
   // `user_data` stays valid until `release_registration` runs.
   unsafe {
      ffi::sqlite3_create_function_v2(
         db,
         registration.c_name.as_ptr(),
         registration.arity as c_int,
         registration.flags,
         user_data,
         Some(dispatch),
         // xStep and xFinal belong to an aggregate, not a scalar function. An aggregate
         // takes this call with xFunc unset, so it cannot reuse `dispatch`.
         None,
         None,
         Some(release_registration),
      )
   }

   // No manual release on the failure path: SQLite invokes xDestroy even when this call
   // fails, so a manual release here duplicates the drop.
}

/// The `xFunc` every registration shares. Invokes the handler and writes its outcome into
/// the SQLite call context.
///
/// # Safety
///
/// SQLite calls this with the context and argument vector of a live function invocation,
/// on the thread that owns the connection.
unsafe extern "C" fn dispatch(
   ctx: *mut ffi::sqlite3_context,
   argc: c_int,
   argv: *mut *mut ffi::sqlite3_value,
) {
   // SAFETY: `ctx` belongs to a live invocation, so the user data is the pointer
   // `create_function` handed SQLite for this function.
   let user_data = unsafe { ffi::sqlite3_user_data(ctx) };

   if user_data.is_null() || argv.is_null() {
      return;
   }

   // A borrow, not an owned Arc. SQLite keeps the reference alive until
   // `release_registration` runs. A connection runs one statement at a time on its own
   // thread, so this call cannot race the release.
   //
   // SAFETY: the pointer came from `Arc::into_raw` on an `Arc<Registration>` and the
   // strong count SQLite holds has not been released yet.
   let registration = unsafe { &*(user_data as *const Registration) };

   // An unwind out of an `extern "C"` function aborts the process, so everything that can
   // panic runs inside here. That includes argument conversion, because a lossy decode of
   // invalid UTF-8 allocates.
   let outcome = catch_unwind(AssertUnwindSafe(|| {
      // SAFETY: SQLite guarantees `argv` holds `argc` valid value pointers for the
      // duration of this call, and the handler runs within it. The null check above is
      // what keeps this sound at an `argc` of zero, where `from_raw_parts` still requires
      // a non-null pointer.
      let argv = unsafe { std::slice::from_raw_parts(argv, argc.max(0) as usize) };

      let mut args = Vec::with_capacity(argv.len());

      // Each argument borrows from `argv`, which is local to this closure, so the compiler
      // keeps every borrow inside the invocation.
      for value in argv {
         // SAFETY: `value` points at one of the pointers SQLite passed, valid for this
         // call.
         args.push(unsafe { SqlValueRef::from_raw(value) });
      }

      (registration.handler)(&args)
   }));

   match outcome {
      // SAFETY for all three arms: `ctx` is the context of the invocation still in
      // progress. These calls require nothing else.
      Ok(Ok(value)) => unsafe { value.write_to(ctx) },
      Ok(Err(error)) => unsafe { result_error(ctx, error.message()) },
      Err(_) => unsafe {
         // Allocating after a caught panic is safe here: Rust aborts on allocation
         // failure rather than unwinding, so `format!` has no panic path of its own.
         result_error(ctx, &format!("{}: handler panicked", registration.name))
      },
   }
}

/// Release the strong reference [`create_function`] handed to SQLite.
///
/// # Safety
///
/// SQLite calls this once per `sqlite3_create_function_v2` call, successful or not, with
/// the `user_data` pointer from that call.
unsafe extern "C" fn release_registration(user_data: *mut c_void) {
   if user_data.is_null() {
      return;
   }

   // Dropping the last reference drops the consumer's handler closure. This call is
   // guarded because an unguarded panic in that closure's `Drop` unwinds out of this
   // `extern "C"` function and aborts the process.
   let _ = catch_unwind(AssertUnwindSafe(|| {
      // SAFETY: the pointer came from `Arc::into_raw` in `create_function`, and SQLite
      // hands each pointer back exactly once.
      unsafe { Arc::decrement_strong_count(user_data as *const Registration) };
   }));
}

/// Report `message` as the error for the function call in progress.
///
/// # Safety
///
/// `ctx` must be the context of a live function invocation.
unsafe fn result_error(ctx: *mut ffi::sqlite3_context, message: &str) {
   // SQLite copies the message out, so pointing at Rust-owned memory is safe. A message
   // longer than `i32::MAX` cannot fit in the length SQLite expects, so this function
   // truncates it instead of dropping it. Nothing produces a message that long in
   // practice.
   let len = c_int::try_from(message.len()).unwrap_or(c_int::MAX);

   // SAFETY: `ctx` belongs to a live invocation, and `message` is valid for `len` bytes.
   unsafe { ffi::sqlite3_result_error(ctx, message.as_ptr() as *const c_char, len) };
}

impl<'a> SqlValueRef<'a> {
   /// Read a SQLite argument as a borrowed value pointing into SQLite's own buffers.
   ///
   /// Text is decoded lossily rather than rejected, to remove any risk of a panic
   /// crossing the FFI boundary. The connection is UTF-8, so invalid UTF-8 does not occur
   /// here in practice, and the decode borrows rather than allocates.
   ///
   /// # Safety
   ///
   /// `value` must point at a valid `sqlite3_value` pointer. The returned borrow lives as
   /// long as that reference, so a caller holding the reference no longer than the
   /// invocation cannot let the borrow escape it.
   unsafe fn from_raw(value: &'a *mut ffi::sqlite3_value) -> SqlValueRef<'a> {
      let value = *value;

      if value.is_null() {
         return SqlValueRef::Null;
      }

      // SAFETY for every call below: `value` is valid until the invocation returns
      // (caller's guarantee). This code calls `sqlite3_value_bytes` after the matching
      // `_text`/`_blob` accessor, in the order SQLite documents.
      match unsafe { ffi::sqlite3_value_type(value) } {
         ffi::SQLITE_INTEGER => SqlValueRef::Integer(unsafe { ffi::sqlite3_value_int64(value) }),
         ffi::SQLITE_FLOAT => SqlValueRef::Real(unsafe { ffi::sqlite3_value_double(value) }),
         ffi::SQLITE_TEXT => {
            let ptr = unsafe { ffi::sqlite3_value_text(value) };
            let len = unsafe { ffi::sqlite3_value_bytes(value) };

            if ptr.is_null() || len <= 0 {
               return SqlValueRef::Text(Cow::Borrowed(""));
            }

            // SAFETY: SQLite reports `len` readable bytes at `ptr`.
            let bytes = unsafe { std::slice::from_raw_parts(ptr, len as usize) };
            SqlValueRef::Text(String::from_utf8_lossy(bytes))
         }
         ffi::SQLITE_BLOB => {
            let ptr = unsafe { ffi::sqlite3_value_blob(value) };
            let len = unsafe { ffi::sqlite3_value_bytes(value) };

            if ptr.is_null() || len <= 0 {
               return SqlValueRef::Blob(&[]);
            }

            // SAFETY: SQLite reports `len` readable bytes at `ptr`.
            SqlValueRef::Blob(unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) })
         }
         // SQLITE_NULL, and any type a future SQLite adds.
         _ => SqlValueRef::Null,
      }
   }
}

impl SqlValue {
   /// Write this value as the result of the function call in progress.
   ///
   /// This function hands text and blob results over with `SQLITE_TRANSIENT`, so SQLite
   /// copies them before this value is dropped. A text or blob result over `i32::MAX`
   /// bytes fails the statement with `SQLITE_TOOBIG`.
   ///
   /// # Safety
   ///
   /// `ctx` must be the context of a live function invocation.
   unsafe fn write_to(self, ctx: *mut ffi::sqlite3_context) {
      // SAFETY for every call below: `ctx` belongs to a live invocation, and each pointer
      // passed is valid for the length passed with it.
      match self {
         SqlValue::Null => unsafe { ffi::sqlite3_result_null(ctx) },
         SqlValue::Integer(value) => unsafe { ffi::sqlite3_result_int64(ctx, value) },
         SqlValue::Real(value) => unsafe { ffi::sqlite3_result_double(ctx, value) },
         SqlValue::Text(text) => match c_int::try_from(text.len()) {
            Ok(len) => unsafe {
               ffi::sqlite3_result_text(
                  ctx,
                  text.as_ptr() as *const c_char,
                  len,
                  ffi::SQLITE_TRANSIENT(),
               )
            },
            // A value this large exceeds what SQLite holds. `SQLITE_MAX_LENGTH` bounds
            // it far lower, and the 64-bit result setters reject the same bound.
            Err(_) => unsafe { ffi::sqlite3_result_error_toobig(ctx) },
         },
         SqlValue::Blob(bytes) => match c_int::try_from(bytes.len()) {
            Ok(len) => unsafe {
               ffi::sqlite3_result_blob(
                  ctx,
                  bytes.as_ptr() as *const c_void,
                  len,
                  ffi::SQLITE_TRANSIENT(),
               )
            },
            Err(_) => unsafe { ffi::sqlite3_result_error_toobig(ctx) },
         },
      }
   }
}

#[cfg(test)]
mod tests {
   use super::*;
   use rstest::rstest;

   /// Every test in this binary shares the process-global registry, so each one uses a
   /// name of its own.
   fn function(name: &str, arity: i32) -> ScalarFunction {
      ScalarFunction {
         name: name.to_string(),
         arity,
         deterministic: true,
         invocation_scope: InvocationScope::DirectOnly,
         handler: Arc::new(|_args| Ok(SqlValue::Null)),
      }
   }

   #[rstest]
   #[case::empty("")]
   #[case::over_255_bytes(&"x".repeat(256))]
   #[case::holding_a_nul_byte("unit_n\0ul")]
   fn rejects_an_invalid_name(#[case] name: &str) {
      assert!(matches!(
         register_function(function(name, 1)),
         Err(Error::InvalidFunctionName(_))
      ));
   }

   #[test]
   fn rejects_a_negative_arity() {
      assert!(matches!(
         register_function(function("unit_variadic", -1)),
         Err(Error::InvalidFunctionArity(-1))
      ));
   }

   /// The linked library, not this crate, decides the arity ceiling. The bundled SQLite
   /// caps `SQLITE_MAX_FUNCTION_ARG` at 1000, so 1001 reaches the probe and the library
   /// refuses it there.
   #[test]
   fn refuses_an_arity_above_what_the_library_accepts() {
      assert!(matches!(
         register_function(function("unit_too_many_args", 1001)),
         Err(Error::FunctionRefused { arity: 1001, .. })
      ));
   }

   /// The "lower" and "LOWER" cases share a registry key. Both are refused before
   /// anything is stored, so they can run in parallel without colliding.
   #[rstest]
   // Exact-arity match against a built-in: lower() takes one argument.
   #[case::exact_arity("lower", 1)]
   // Case-insensitive, matching how SQLite resolves names.
   #[case::case_insensitive("LOWER", 1)]
   // A variadic built-in matches every arity: max() accepts any argument count.
   #[case::variadic_builtin("max", 3)]
   fn refuses_a_name_that_replaces_a_builtin(#[case] name: &str, #[case] arity: i32) {
      assert!(matches!(
         register_function(function(name, arity)),
         Err(Error::ShadowsBuiltinFunction { arity: refused, .. }) if refused == arity
      ));
   }

   /// A consumer that builds a name at runtime, such as one carrying a version suffix,
   /// hands over the `String` it built.
   #[test]
   fn accepts_a_name_built_at_runtime() {
      let name = format!("unit_runtime_{}", 3 + 4);

      register_function(function(&name, 1)).expect("registration");
   }

   #[test]
   fn rejects_a_duplicate_name_and_arity_whatever_the_case() {
      register_function(function("unit_dup", 1)).expect("first registration");

      assert!(matches!(
         register_function(function("unit_dup", 1)),
         Err(Error::DuplicateFunction { .. })
      ));

      assert!(matches!(
         register_function(function("UNIT_DUP", 1)),
         Err(Error::DuplicateFunction { .. })
      ));

      // A different arity is an overload. SQLite supports overloading a name this way, so
      // this registers.
      register_function(function("unit_dup", 2)).expect("overload by arity");
   }

   #[test]
   fn replaces_a_name_and_arity_the_plain_registration_rejects() {
      register_function(function("unit_replace", 1)).expect("first registration");

      assert!(matches!(
         register_function(function("unit_replace", 1)),
         Err(Error::DuplicateFunction { .. })
      ));

      register_or_replace_function(function("unit_replace", 1)).expect("replacement");

      // Case-insensitively, matching how SQLite resolves names and how the rejection above
      // compares them.
      register_or_replace_function(function("UNIT_REPLACE", 1)).expect("replacement");
   }
}
