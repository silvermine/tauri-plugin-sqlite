//! SQLite native hook registration for support observing changes to the database.
//!
//! This module provides low-level bindings to SQLite's preupdate_hook, commit_hook,
//! rollback_hook, and trace APIs for transaction-aware change tracking.
//!
//! # Why four hooks
//!
//! SQLite's commit hook runs *before* the commit is final: it is the hook whose
//! non-zero return converts a `COMMIT` into a `ROLLBACK`. A subscriber notified
//! from it and reading through another connection reads pre-commit state. So
//! the commit hook only converts and holds the transaction's changes, and the
//! trace hook publishes them. It listens for two events: a statement finishing,
//! which is after any commit it made became durable, and a statement starting,
//! which catches a commit that happened during a statement reset (an autocommit
//! `INSERT ... RETURNING` read with `fetch_one`). A reset commit is published
//! when the next statement starts or when the hooks are unregistered.
//!
//! Publication happens after SQLite released the write lock, so notifications
//! keep commit order per connection only.
//!
//! All per-transaction state lives in the `HookContext` that each connection
//! registers, never on the shared broker, so two connections observing the
//! same database can never publish or discard each other's changes. The data
//! flow diagram in [`crate::broker`] shows the whole path.
//!
//! # SQLite Requirements
//!
//! The preupdate hook requires SQLite compiled with `SQLITE_ENABLE_PREUPDATE_HOOK`.
//! Use [`is_preupdate_hook_enabled()`] to check at runtime whether the linked
//! SQLite library supports this feature.

use std::collections::HashMap;
use std::ffi::{CStr, CString, c_char, c_int, c_uint, c_void};
use std::io::Write;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::sync::Arc;

use libsqlite3_sys::{
   SQLITE_BLOB, SQLITE_DELETE, SQLITE_FLOAT, SQLITE_INSERT, SQLITE_INTEGER, SQLITE_NULL, SQLITE_OK,
   SQLITE_TEXT, SQLITE_TRACE_PROFILE, SQLITE_TRACE_STMT, SQLITE_UPDATE, sqlite3,
   sqlite3_commit_hook, sqlite3_compileoption_used, sqlite3_preupdate_count,
   sqlite3_preupdate_hook, sqlite3_preupdate_new, sqlite3_preupdate_old, sqlite3_rollback_hook,
   sqlite3_trace_v2, sqlite3_value, sqlite3_value_blob, sqlite3_value_bytes, sqlite3_value_double,
   sqlite3_value_int64, sqlite3_value_text, sqlite3_value_type,
};
use parking_lot::Mutex;
use tracing::{debug, error, trace};

use crate::broker::ObservationBroker;
use crate::change::{ChangeOperation, TableChange};

/// A SQLite value extracted from preupdate hooks.
///
/// Represents the typed value of a column before or after a change operation.
#[derive(Debug, Clone, PartialEq)]
pub enum SqliteValue {
   Null,
   Integer(i64),
   Real(f64),
   Text(String),
   Blob(Vec<u8>),
}

impl SqliteValue {
   /// Extracts a value from a raw sqlite3_value pointer.
   ///
   /// # Safety
   ///
   /// The pointer must be valid and point to a properly initialized sqlite3_value.
   unsafe fn from_raw(value: *mut sqlite3_value) -> Self {
      if value.is_null() {
         return SqliteValue::Null;
      }

      // SAFETY: value is non-null and valid for the duration of the preupdate hook callback.
      // SQLite guarantees the sqlite3_value pointer is valid until the callback returns.
      match unsafe { sqlite3_value_type(value) } {
         SQLITE_NULL => SqliteValue::Null,
         SQLITE_INTEGER => SqliteValue::Integer(unsafe { sqlite3_value_int64(value) }),
         SQLITE_FLOAT => SqliteValue::Real(unsafe { sqlite3_value_double(value) }),
         SQLITE_TEXT => {
            let text_ptr = unsafe { sqlite3_value_text(value) };
            if text_ptr.is_null() {
               SqliteValue::Null
            } else {
               // SAFETY: SQLite guarantees text is valid UTF-8 with a null terminator
               let cstr = unsafe { CStr::from_ptr(text_ptr as *const c_char) };
               SqliteValue::Text(cstr.to_string_lossy().into_owned())
            }
         }
         SQLITE_BLOB => {
            let blob_ptr = unsafe { sqlite3_value_blob(value) };
            let len = unsafe { sqlite3_value_bytes(value) } as usize;
            if blob_ptr.is_null() || len == 0 {
               SqliteValue::Blob(Vec::new())
            } else {
               // SAFETY: blob_ptr is non-null and len bytes are valid for the callback duration
               let slice = unsafe { std::slice::from_raw_parts(blob_ptr as *const u8, len) };
               SqliteValue::Blob(slice.to_vec())
            }
         }
         _ => SqliteValue::Null,
      }
   }
}

/// Raw change event captured by the preupdate hook before commit decision.
///
/// Crate-private: only `preupdate_callback` builds one, and only the broker's
/// conversion consumes one.
#[derive(Debug, Clone)]
pub(crate) struct PreUpdateEvent {
   /// The schema this change occurred under (`"main"` or an `ATTACH` alias).
   /// See [`TableChange::schema`](crate::change::TableChange::schema) for what
   /// this is - and is not - safe to rely on.
   pub schema: String,
   pub table: String,
   pub operation: ChangeOperation,
   pub old_rowid: i64,
   pub new_rowid: i64,
   pub old_values: Option<Vec<SqliteValue>>,
   pub new_values: Option<Vec<SqliteValue>>,
}

/// Context data passed to SQLite hook callbacks - one per registered connection.
///
/// Stored as user_data pointer in SQLite hooks. Brokers are keyed by schema
/// alias (`"main"` for the primary database, the `ATTACH` alias otherwise)
/// rather than held singly, so that one connection with attached databases can
/// route each change to the broker of whichever database actually owns the
/// affected table. The `Arc`s ensure each broker stays alive as long as hooks
/// are registered.
///
/// The context also owns this connection's transaction state: the events
/// buffered since the last commit or rollback, and the changes a commit hook
/// converted but has not yet published.
struct HookContext {
   brokers: HashMap<String, Arc<ObservationBroker>>,
   /// Preupdate events of the transaction in progress.
   buffer: Mutex<Vec<PreUpdateEvent>>,
   /// Changes the commit hook accepted, awaiting the statement's end to publish,
   /// each paired with the broker that converted it.
   committed: Mutex<Vec<(Arc<ObservationBroker>, TableChange)>>,
}

impl HookContext {
   fn new(brokers: HashMap<String, Arc<ObservationBroker>>) -> Self {
      Self {
         brokers,
         buffer: Mutex::new(Vec::new()),
         committed: Mutex::new(Vec::new()),
      }
   }

   /// Buffers a captured event until the transaction commits or rolls back.
   fn on_preupdate(&self, event: PreUpdateEvent) {
      trace!(table = %event.table, operation = ?event.operation, "Buffering preupdate event");
      self.buffer.lock().push(event);
   }

   /// Converts the buffered events and holds them for [`publish_committed`].
   ///
   /// The buffer is drained before conversion starts, so a panic while
   /// converting one event loses that transaction's remaining events but can
   /// never let them resurface on the connection's next commit. Lost
   /// notifications are the accepted trade for a single panic guard around the
   /// whole call, instead of one per event.
   ///
   /// [`publish_committed`]: Self::publish_committed
   fn on_commit(&self) {
      let events = std::mem::take(&mut *self.buffer.lock());

      if events.is_empty() {
         return;
      }

      debug!(count = events.len(), "Holding buffered changes on commit");

      let mut committed = self.committed.lock();

      for event in events {
         // The preupdate callback only buffers an event whose schema has a
         // broker, so a miss here cannot happen; it is reported rather than
         // unwrapped because this runs inside an FFI callback.
         let Some(broker) = self.brokers.get(&event.schema) else {
            error!(schema = %event.schema, "No broker for a buffered event's schema");
            continue;
         };

         match broker.event_to_change(event) {
            Ok(change) => committed.push((Arc::clone(broker), change)),
            Err(e) => error!(error = %e, "Failed to convert event to change"),
         }
      }
   }

   /// Discards the buffered events and any held-but-unpublished changes of a
   /// rolled-back transaction.
   ///
   /// A commit can still fail after its commit hook ran, and this hook then fires
   /// while that transaction's changes are held. They must not survive it.
   fn on_rollback(&self) {
      let discarded_buffered = std::mem::take(&mut *self.buffer.lock()).len();
      let discarded_committed = std::mem::take(&mut *self.committed.lock()).len();

      if discarded_buffered > 0 || discarded_committed > 0 {
         debug!(
            discarded_buffered,
            discarded_committed, "Discarding buffered and held changes on rollback"
         );
      }
   }

   /// Publishes every held change through its schema's broker.
   ///
   /// Called by the trace hook when a statement finishes, which is after any
   /// commit that statement performed became durable. The trace hook also
   /// calls it when the next statement starts, which covers a commit made
   /// during a statement reset. [`unregister_hooks`] also calls it as a
   /// backstop. Safe to call when nothing is held.
   fn publish_committed(&self) {
      let changes = std::mem::take(&mut *self.committed.lock());

      if changes.is_empty() {
         return;
      }

      debug!(count = changes.len(), "Publishing committed changes");

      for (broker, change) in changes {
         broker.publish(change);
      }
   }
}

/// Checks if the linked SQLite library was compiled with `SQLITE_ENABLE_PREUPDATE_HOOK`.
///
/// Returns `true` if preupdate hooks are supported, `false` otherwise.
/// This should be checked before attempting to use observation features.
///
/// # Example
///
/// ```no_run
/// use sqlx_sqlite_observer::is_preupdate_hook_enabled;
///
/// if !is_preupdate_hook_enabled() {
///     panic!("SQLite was not compiled with SQLITE_ENABLE_PREUPDATE_HOOK");
/// }
/// ```
pub fn is_preupdate_hook_enabled() -> bool {
   let opt_name = CString::new("ENABLE_PREUPDATE_HOOK").expect("CString::new failed");
   unsafe { sqlite3_compileoption_used(opt_name.as_ptr()) == 1 }
}

/// Registers all observation hooks on a raw SQLite connection.
///
/// Hooks are automatically cleaned up by SQLite when the connection is closed,
/// either explicitly or when the connection exceeds the sqlx pool's `idle_timeout`.
///
/// # Safety
///
/// - `db` must be a valid pointer to an open sqlite3 connection
/// - The broker must outlive the connection (ensured by Arc)
/// - Must be called from the same thread that owns the connection, or
///   the connection must be in serialized threading mode
///
/// # Arguments
///
/// * `db` - the connection to register hooks on
/// * `brokers` - schema alias -> broker map. `"main"` covers the primary
///   database; any other key routes changes made under that `ATTACH` alias to
///   the corresponding broker. A schema with no entry here has its changes
///   silently dropped by the preupdate callback rather than misrouted to some
///   other schema's broker - see `preupdate_callback`.
///
/// # Errors
///
/// Returns an error if preupdate hooks are not supported by the linked SQLite
/// library, or if the hooks cannot be registered.
pub unsafe fn register_hooks(
   db: *mut sqlite3,
   brokers: HashMap<String, Arc<ObservationBroker>>,
) -> crate::Result<()> {
   // Check at runtime if preupdate hook is supported
   if !is_preupdate_hook_enabled() {
      return Err(crate::Error::HookRegistration(
         "SQLite was not compiled with SQLITE_ENABLE_PREUPDATE_HOOK. \
             Ensure you're using a SQLite build with preupdate hook support, \
             or enable the 'bundled' feature on libsqlite3-sys."
            .to_string(),
      ));
   }

   debug!("Registering SQLite observation hooks");

   // Heap-allocate the context so it outlives this function. SQLite's C API
   // requires a raw pointer to pass user data to callbacks.
   let context = Box::new(HookContext::new(brokers));
   // Transfer ownership out of Rust's memory management.
   //
   // NOTE: This pointer is shared across all four hooks and is intentionally
   // leaked. SQLite does NOT free user_data - it simply passes the pointer back
   // to callbacks. The memory is reclaimed when hooks are replaced via
   // `unregister_hooks`, which reconstructs the Box from the raw pointer returned
   // by `sqlite3_preupdate_hook`. If hooks are never explicitly unregistered,
   // the memory lives until the process exits (acceptable for long-lived
   // connections where the count is bounded).
   let context_ptr = Box::into_raw(context) as *mut c_void;

   // SAFETY: db is a valid sqlite3 pointer (guaranteed by caller).
   // Each hook receives the same context_ptr, which remains valid until
   // unregister_hooks is called or the process exits.
   //
   // Two trace events fire (see the module doc). The profile event fires when
   // a statement finishes. The statement event fires when the next statement
   // starts, and this publishes a commit that happened during a reset. The
   // trace slot is single-occupancy. A caller who installs their own trace
   // hook on this connection stops notifications.
   //
   // Unlike the other three, `sqlite3_trace_v2` reports a result. It is the publish
   // point, so a failure leaves the connection unobserved rather than half-observed.
   let trace_rc = unsafe {
      sqlite3_preupdate_hook(db, Some(preupdate_callback), context_ptr);
      sqlite3_commit_hook(db, Some(commit_callback), context_ptr);
      sqlite3_rollback_hook(db, Some(rollback_callback), context_ptr);
      sqlite3_trace_v2(
         db,
         SQLITE_TRACE_STMT | SQLITE_TRACE_PROFILE,
         Some(trace_callback),
         context_ptr,
      )
   };

   if trace_rc != SQLITE_OK {
      // SAFETY: the same db the hooks were just installed on; nothing has run
      // in between, so no callback is in flight. Reclaims the context Box.
      unsafe { unregister_hooks(db) };
      return Err(crate::Error::HookRegistration(format!(
         "sqlite3_trace_v2 failed with code {trace_rc}; the trace hook publishes \
          notifications, so observation cannot be enabled on this connection"
      )));
   }

   trace!("SQLite hooks registered successfully");
   Ok(())
}

/// Unregisters all observation hooks and reclaims the context memory.
///
/// Publishes any change the commit hook accepted that the trace hook has not
/// published yet. This is a commit made during a statement reset, with no
/// statement run since. It also drops whatever was buffered but never
/// committed. The buffer dies with the context, so an abandoned transaction's
/// events cannot resurface on the connection's next commit. This function
/// also clears the connection's trace slot.
///
/// # Safety
///
/// - `db` must be the same valid sqlite3 pointer passed to `register_hooks`
/// - Must only be called once per `register_hooks` call
/// - Must not be called concurrently with hook callbacks
pub unsafe fn unregister_hooks(db: *mut sqlite3) {
   // SAFETY: Passing null callback and null user_data removes the hook.
   // sqlite3_preupdate_hook returns the previous user_data pointer, which
   // we use to reclaim the Box we leaked in register_hooks.
   let prev_user_data = unsafe { sqlite3_preupdate_hook(db, None, ptr::null_mut()) };
   unsafe {
      sqlite3_commit_hook(db, None, ptr::null_mut());
      sqlite3_rollback_hook(db, None, ptr::null_mut());
      sqlite3_trace_v2(db, 0, None, ptr::null_mut());
   }

   // Reclaim the HookContext we leaked in register_hooks
   if !prev_user_data.is_null() {
      // SAFETY: prev_user_data was created by Box::into_raw in register_hooks
      let context = unsafe { Box::from_raw(prev_user_data as *mut HookContext) };
      context.publish_committed();
      trace!("SQLite hooks unregistered and context freed");
   }
}

/// Preupdate hook callback - captures changes before they're committed.
///
/// Called by SQLite for INSERT, UPDATE, and DELETE operations. Captures old/new
/// row values, if the broker of whichever database owns the affected table
/// (selected by schema - see [`register_hooks`]) observes it, and buffers them
/// in the connection's context until commit or rollback.
///
/// Note: `user_data` is SQLite's C API term for callback context (our HookContext),
/// unrelated to our app's user data.
unsafe extern "C" fn preupdate_callback(
   user_data: *mut c_void,
   db: *mut sqlite3,
   op: c_int,
   database: *const c_char,
   table: *const c_char,
   old_rowid: i64,
   new_rowid: i64,
) {
   if user_data.is_null() || database.is_null() || table.is_null() {
      return;
   }

   // Catch any panics to prevent unwinding across the FFI boundary, which aborts
   // the process (a guarantee since Rust 1.81, UB before it; MSRV here is 1.94).
   let result = catch_unwind(|| {
      // SAFETY: user_data is a valid HookContext pointer created in register_hooks
      // and remains valid until unregister_hooks is called.
      let context = unsafe { &*(user_data as *const HookContext) };

      // SAFETY: database is a non-null C string provided by SQLite, valid for
      // this callback. SQLite reports "main" for the primary schema and the
      // caller-chosen ATTACH alias otherwise. Kept as `&str` through both
      // gates below (broker lookup, then observed-table check) rather than
      // allocated right away - `HashMap::get`/`HashSet::contains` both take
      // `&str`, so an unobserved schema or table costs no allocation on this
      // per-row FFI hot path. Only a change that clears both gates has its
      // strings turned into owned `String`s, for the `PreUpdateEvent` below.
      let schema_name = match unsafe { CStr::from_ptr(database) }.to_str() {
         Ok(s) => s,
         Err(_) => return,
      };

      // SAFETY: table is a non-null C string provided by SQLite, valid for this callback.
      let table_name = match unsafe { CStr::from_ptr(table) }.to_str() {
         Ok(s) => s,
         Err(_) => return,
      };

      // No broker is registered for this schema - a `temp` table, an attached
      // database with no observation enabled, or a `ReadOnly` attachment whose
      // broker was deliberately left out of the map (see
      // `ObservableSqliteDatabase::acquire_writer_with_attached`'s doc). Either
      // way, drop the change rather than publish it under some other schema's
      // broker. `trace!` rather than `warn!` because the `temp` case is routine
      // and a warning would cry wolf.
      let Some(broker) = context.brokers.get(schema_name) else {
         trace!(
            schema = %schema_name,
            table = %table_name,
            "Dropping change for a schema with no broker in the hook map"
         );
         return;
      };

      // Check if this table is being observed
      if !broker.is_table_observed(table_name) {
         return;
      }

      let operation = match op {
         SQLITE_INSERT => ChangeOperation::Insert,
         SQLITE_UPDATE => ChangeOperation::Update,
         SQLITE_DELETE => ChangeOperation::Delete,
         _ => return,
      };

      trace!(schema = %schema_name, table = %table_name, ?operation, old_rowid, new_rowid, "Preupdate hook fired");

      // SAFETY: db is a valid sqlite3 pointer provided by SQLite for this callback.
      let column_count = unsafe { sqlite3_preupdate_count(db) };
      if column_count < 0 {
         error!("Failed to get column count in preupdate hook");
         return;
      }
      let column_count = column_count as usize;

      // Capture old values (for UPDATE and DELETE)
      let old_values = if matches!(operation, ChangeOperation::Update | ChangeOperation::Delete) {
         let mut values = Vec::with_capacity(column_count);
         for i in 0..column_count {
            let mut value: *mut sqlite3_value = ptr::null_mut();
            // SAFETY: db is valid, i is in range [0, column_count)
            if unsafe { sqlite3_preupdate_old(db, i as c_int, &mut value) } == 0 {
               // SAFETY: value was populated by sqlite3_preupdate_old
               values.push(unsafe { SqliteValue::from_raw(value) });
            } else {
               values.push(SqliteValue::Null);
            }
         }
         Some(values)
      } else {
         None
      };

      // Capture new values (for INSERT and UPDATE)
      let new_values = if matches!(operation, ChangeOperation::Insert | ChangeOperation::Update) {
         let mut values = Vec::with_capacity(column_count);
         for i in 0..column_count {
            let mut value: *mut sqlite3_value = ptr::null_mut();
            // SAFETY: db is valid, i is in range [0, column_count)
            if unsafe { sqlite3_preupdate_new(db, i as c_int, &mut value) } == 0 {
               // SAFETY: value was populated by sqlite3_preupdate_new
               values.push(unsafe { SqliteValue::from_raw(value) });
            } else {
               values.push(SqliteValue::Null);
            }
         }
         Some(values)
      } else {
         None
      };

      let event = PreUpdateEvent {
         schema: schema_name.to_string(),
         table: table_name.to_string(),
         operation,
         old_rowid,
         new_rowid,
         old_values,
         new_values,
      };

      context.on_preupdate(event);
   });

   if result.is_err() {
      // Cannot use tracing here since it may have been the source of the panic,
      // nor eprintln!, which panics on a write failure - and that unwind would
      // abort the process. Absorbing it is the best available outcome. The other
      // two callbacks below report their own panics the same way.
      let _ = writeln!(
         std::io::stderr(),
         "sqlx-sqlite-observer: panic in preupdate_callback (absorbed to keep the process alive)"
      );
   }
}

/// Commit hook callback - converts and holds the transaction's changes.
///
/// Called by SQLite when a transaction is about to commit. Returning 0 allows
/// the commit to proceed; returning non-zero would cause a rollback. Because
/// the commit is not final yet, nothing is published here - see the module
/// doc and [`trace_callback`].
///
/// `sqlite3_commit_hook`'s callback takes no schema argument and fires exactly
/// once per transaction regardless of how many schemas (main plus any attached
/// databases) were touched. The context buffers every schema's events together
/// and routes each to its broker on conversion, so one call settles them all.
///
/// Note: `user_data` is SQLite's C API term for callback context (our HookContext),
/// unrelated to application-level user data.
unsafe extern "C" fn commit_callback(user_data: *mut c_void) -> c_int {
   if user_data.is_null() {
      return 0;
   }

   // SAFETY: user_data is a valid HookContext pointer created in register_hooks.
   let context = unsafe { &*(user_data as *const HookContext) };

   // Wrapped because an unwind out of an extern "C" fn aborts the process.
   if catch_unwind(AssertUnwindSafe(|| {
      trace!("Commit hook fired - holding changes");
      context.on_commit();
   }))
   .is_err()
   {
      // Reported via writeln! to stderr rather than tracing - see
      // preupdate_callback's equivalent for why.
      let _ = writeln!(
         std::io::stderr(),
         "sqlx-sqlite-observer: panic in commit_callback (absorbed to keep the process alive)"
      );
   }

   0 // Allow commit to proceed
}

/// Trace callback - publishes the held changes of a committed transaction.
///
/// SQLite invokes it with the profile event when a statement finishes, and
/// with the statement event when the next statement starts. A commit that a
/// finished statement performed is durable by the time the profile event
/// fires. The statement event fires before the statement runs any of its own
/// hooks. So both events are safe publish points. Most calls find nothing
/// held and return.
///
/// SQLite invokes the callback only for the events in its mask. The mask is
/// checked anyway, as a guard against a future registration that adds an event
/// without adding its handling here.
///
/// Note: `user_data` is SQLite's C API term for callback context (our HookContext),
/// unrelated to application-level user data.
unsafe extern "C" fn trace_callback(
   mask: c_uint,
   user_data: *mut c_void,
   _statement: *mut c_void,
   _nanoseconds: *mut c_void,
) -> c_int {
   if (mask != SQLITE_TRACE_STMT && mask != SQLITE_TRACE_PROFILE) || user_data.is_null() {
      return 0;
   }

   // SAFETY: user_data is a valid HookContext pointer created in register_hooks.
   let context = unsafe { &*(user_data as *const HookContext) };

   // Wrapped because an unwind out of an extern "C" fn aborts the process.
   if catch_unwind(AssertUnwindSafe(|| context.publish_committed())).is_err() {
      // Reported via writeln! to stderr rather than tracing - see
      // preupdate_callback's equivalent for why.
      let _ = writeln!(
         std::io::stderr(),
         "sqlx-sqlite-observer: panic in trace_callback (absorbed to keep the process alive)"
      );
   }

   0
}

/// Rollback hook callback - discards buffered changes.
///
/// Called by SQLite when a transaction is rolled back. One rollback hook fires
/// for the whole transaction, and the context holds every schema's buffered
/// events together, so one call discards them all.
///
/// Note: `user_data` is SQLite's C API term for callback context (our HookContext),
/// unrelated to application-level user data.
unsafe extern "C" fn rollback_callback(user_data: *mut c_void) {
   if user_data.is_null() {
      return;
   }

   // SAFETY: user_data is a valid HookContext pointer created in register_hooks.
   let context = unsafe { &*(user_data as *const HookContext) };

   // Wrapped because an unwind out of an extern "C" fn aborts the process.
   if catch_unwind(AssertUnwindSafe(|| {
      trace!("Rollback hook fired - discarding changes");
      context.on_rollback();
   }))
   .is_err()
   {
      // Reported via writeln! to stderr rather than tracing - see
      // preupdate_callback's equivalent for why.
      let _ = writeln!(
         std::io::stderr(),
         "sqlx-sqlite-observer: panic in rollback_callback (absorbed to keep the process alive)"
      );
   }
}

#[cfg(test)]
mod tests {
   use super::*;

   #[test]
   fn test_sqlite_value_from_null() {
      let value = unsafe { SqliteValue::from_raw(ptr::null_mut()) };
      assert_eq!(value, SqliteValue::Null);
   }
}
