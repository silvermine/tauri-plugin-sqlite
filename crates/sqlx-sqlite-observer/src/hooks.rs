//! SQLite native hook registration for support observing changes to the database.
//!
//! This module provides low-level bindings to SQLite's preupdate_hook, commit_hook,
//! and rollback_hook APIs for transaction-aware change tracking.
//!
//! # SQLite Requirements
//!
//! The preupdate hook requires SQLite compiled with `SQLITE_ENABLE_PREUPDATE_HOOK`.
//! Use [`is_preupdate_hook_enabled()`] to check at runtime whether the linked
//! SQLite library supports this feature.

use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::panic::catch_unwind;
use std::ptr;
use std::sync::Arc;

use libsqlite3_sys::{
   SQLITE_BLOB, SQLITE_DELETE, SQLITE_FLOAT, SQLITE_INSERT, SQLITE_INTEGER, SQLITE_NULL,
   SQLITE_TEXT, SQLITE_UPDATE, sqlite3, sqlite3_commit_hook, sqlite3_compileoption_used,
   sqlite3_preupdate_count, sqlite3_preupdate_hook, sqlite3_preupdate_new, sqlite3_preupdate_old,
   sqlite3_rollback_hook, sqlite3_value, sqlite3_value_blob, sqlite3_value_bytes,
   sqlite3_value_double, sqlite3_value_int64, sqlite3_value_text, sqlite3_value_type,
};
use tracing::{debug, error, trace};

use crate::broker::ObservationBroker;
use crate::change::ChangeOperation;

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
#[derive(Debug, Clone)]
pub struct PreUpdateEvent {
   pub table: String,
   pub operation: ChangeOperation,
   pub old_rowid: i64,
   pub new_rowid: i64,
   pub old_values: Option<Vec<SqliteValue>>,
   pub new_values: Option<Vec<SqliteValue>>,
}

/// Context data passed to SQLite hook callbacks.
///
/// Stored as user_data pointer in SQLite hooks. The Arc ensures the broker
/// stays alive as long as hooks are registered.
struct HookContext {
   broker: Arc<ObservationBroker>,
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
/// # Errors
///
/// Returns an error if preupdate hooks are not supported by the linked SQLite
/// library, or if the hooks cannot be registered.
pub unsafe fn register_hooks(
   db: *mut sqlite3,
   broker: Arc<ObservationBroker>,
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
   let context = Box::new(HookContext { broker });
   // Transfer ownership out of Rust's memory management.
   //
   // NOTE: This pointer is shared across all three hooks and is intentionally
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
   unsafe {
      sqlite3_preupdate_hook(db, Some(preupdate_callback), context_ptr);
      sqlite3_commit_hook(db, Some(commit_callback), context_ptr);
      sqlite3_rollback_hook(db, Some(rollback_callback), context_ptr);
   }

   trace!("SQLite hooks registered successfully");
   Ok(())
}

/// Unregisters all observation hooks and reclaims the context memory.
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
   }

   // Reclaim the HookContext we leaked in register_hooks
   if !prev_user_data.is_null() {
      // SAFETY: prev_user_data was created by Box::into_raw in register_hooks
      let _ = unsafe { Box::from_raw(prev_user_data as *mut HookContext) };
      trace!("SQLite hooks unregistered and context freed");
   }
}

/// Preupdate hook callback - captures changes before they're committed.
///
/// Called by SQLite for INSERT, UPDATE, and DELETE operations. Captures old/new
/// row values and buffers them in the broker until commit or rollback.
///
/// Note: `user_data` is SQLite's C API term for callback context (our HookContext),
/// unrelated to our app's user data.
unsafe extern "C" fn preupdate_callback(
   user_data: *mut c_void,
   db: *mut sqlite3,
   op: c_int,
   _database: *const c_char,
   table: *const c_char,
   old_rowid: i64,
   new_rowid: i64,
) {
   if user_data.is_null() || table.is_null() {
      return;
   }

   // Catch any panics to prevent unwinding across the FFI boundary (which is UB).
   let result = catch_unwind(|| {
      // SAFETY: user_data is a valid HookContext pointer created in register_hooks
      // and remains valid until unregister_hooks is called.
      let context = unsafe { &*(user_data as *const HookContext) };

      // SAFETY: table is a non-null C string provided by SQLite, valid for this callback.
      let table_name = match unsafe { CStr::from_ptr(table) }.to_str() {
         Ok(s) => s.to_string(),
         Err(_) => return,
      };

      // Check if this table is being observed
      if !context.broker.is_table_observed(&table_name) {
         return;
      }

      let operation = match op {
         SQLITE_INSERT => ChangeOperation::Insert,
         SQLITE_UPDATE => ChangeOperation::Update,
         SQLITE_DELETE => ChangeOperation::Delete,
         _ => return,
      };

      trace!(table = %table_name, ?operation, old_rowid, new_rowid, "Preupdate hook fired");

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
         table: table_name,
         operation,
         old_rowid,
         new_rowid,
         old_values,
         new_values,
      };

      context.broker.on_preupdate(event);
   });

   if result.is_err() {
      // Cannot use tracing here since it may have been the source of the panic.
      // The best we can do is silently absorb it to prevent UB.
      eprintln!("sqlx-sqlite-observer: panic in preupdate_callback (absorbed to prevent UB)");
   }
}

/// Commit hook callback - flushes buffered changes to subscribers.
///
/// Called by SQLite when a transaction is about to commit. Returning 0 allows
/// the commit to proceed; returning non-zero would cause a rollback.
///
/// Note: `user_data` is SQLite's C API term for callback context (our HookContext),
/// unrelated to application-level user data.
unsafe extern "C" fn commit_callback(user_data: *mut c_void) -> c_int {
   if user_data.is_null() {
      return 0;
   }

   // Catch any panics to prevent unwinding across the FFI boundary (which is UB).
   let result = catch_unwind(|| {
      // SAFETY: user_data is a valid HookContext pointer created in register_hooks.
      let context = unsafe { &*(user_data as *const HookContext) };
      trace!("Commit hook fired - flushing changes");
      context.broker.on_commit();
   });

   if result.is_err() {
      eprintln!("sqlx-sqlite-observer: panic in commit_callback (absorbed to prevent UB)");
   }

   0 // Allow commit to proceed
}

/// Rollback hook callback - discards buffered changes.
///
/// Called by SQLite when a transaction is rolled back.
///
/// Note: `user_data` is SQLite's C API term for callback context (our HookContext),
/// unrelated to application-level user data.
unsafe extern "C" fn rollback_callback(user_data: *mut c_void) {
   if user_data.is_null() {
      return;
   }

   // Catch any panics to prevent unwinding across the FFI boundary (which is UB).
   let result = catch_unwind(|| {
      // SAFETY: user_data is a valid HookContext pointer created in register_hooks.
      let context = unsafe { &*(user_data as *const HookContext) };
      trace!("Rollback hook fired - discarding changes");
      context.broker.on_rollback();
   });

   if result.is_err() {
      eprintln!("sqlx-sqlite-observer: panic in rollback_callback (absorbed to prevent UB)");
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
