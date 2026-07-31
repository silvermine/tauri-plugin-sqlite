//! Observable connection wrapper with SQLite hook integration.
//!
//! Provides change tracking via SQLite's native preupdate/commit/rollback hooks
//! instead of triggers. Changes are buffered during transactions and only
//! published to subscribers after successful commit.

use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use libsqlite3_sys::sqlite3;
use sqlx::Sqlite;
use sqlx::pool::PoolConnection;
use tracing::{debug, trace};

use crate::Result;
use crate::broker::ObservationBroker;
use crate::hooks;

/// A wrapper around a SQLite pool connection allowing observers to subscribe to
/// change notifications.
///
/// Uses SQLite's native hooks (preupdate_hook, commit_hook, rollback_hook)
/// for transaction-safe change tracking. Changes are buffered during transactions
/// and published to subscribers only after successful commit.
///
/// Implements `Deref`/`DerefMut` to allow transparent use as the underlying
/// `PoolConnection<Sqlite>`.
pub struct ObservableConnection {
   conn: Option<PoolConnection<Sqlite>>,
   broker: Arc<ObservationBroker>,
   hooks_registered: bool,
   /// Raw sqlite3 pointer, cached during register_hooks so we can
   /// call unregister_hooks synchronously in Drop without needing
   /// the async lock_handle.
   raw_db: Option<*mut sqlite3>,
}

// SAFETY: The raw_db pointer is only used for hook registration/unregistration
// and is always accessed from the same logical owner. The underlying sqlite3
// connection is already Send via sqlx's PoolConnection.
unsafe impl Send for ObservableConnection {}

impl ObservableConnection {
   pub(crate) fn new(conn: PoolConnection<Sqlite>, broker: Arc<ObservationBroker>) -> Self {
      Self {
         conn: Some(conn),
         broker,
         hooks_registered: false,
         raw_db: None,
      }
   }

   fn conn_mut(&mut self) -> &mut PoolConnection<Sqlite> {
      self.conn.as_mut().expect("connection already taken")
   }

   fn conn_ref(&self) -> &PoolConnection<Sqlite> {
      self.conn.as_ref().expect("connection already taken")
   }

   /// Registers SQLite observation hooks on this connection.
   ///
   /// This must be called before any changes can be tracked. The hooks capture
   /// changes during transactions and publish them on commit.
   ///
   /// # Safety
   ///
   /// This method accesses the raw SQLite connection handle. It is safe as long
   /// as the connection is not being used concurrently from another thread.
   pub async fn register_hooks(&mut self) -> Result<()> {
      if self.hooks_registered {
         return Ok(());
      }

      debug!("Registering SQLite observation hooks");

      let conn = self.conn.as_mut().expect("connection already taken");

      // Get raw SQLite handle through sqlx's lock mechanism
      let mut handle = conn
         .lock_handle()
         .await
         .map_err(|e| crate::Error::Database(format!("Failed to lock connection handle: {}", e)))?;

      let db: *mut sqlite3 = handle.as_raw_handle().as_ptr();

      // This crate's plain (non-conn-mgr) connection path never attaches
      // other databases, so there is only ever one schema to route: SQLite
      // always reports "main" for the primary database.
      let mut brokers = std::collections::HashMap::with_capacity(1);
      brokers.insert("main".to_string(), Arc::clone(&self.broker));

      unsafe {
         hooks::register_hooks(db, brokers)?;
      }

      // Cache the raw pointer so Drop can call unregister_hooks synchronously.
      // SAFETY: The pointer remains valid for the lifetime of the PoolConnection,
      // which we own via self.conn.
      self.raw_db = Some(db);
      self.hooks_registered = true;
      Ok(())
   }

   /// Consumes this wrapper and returns the underlying pool connection.
   ///
   /// Hooks are unregistered before returning the connection, so it can be
   /// safely returned to the pool or used without observation.
   pub fn into_inner(mut self) -> PoolConnection<Sqlite> {
      // Unregister hooks before returning the connection to prevent
      // use-after-free if the broker is dropped before the pooled connection is reused.
      if self.hooks_registered
         && let Some(db) = self.raw_db
      {
         unsafe {
            crate::hooks::unregister_hooks(db);
         }
         trace!("Hooks unregistered before returning inner connection");
      }
      self.hooks_registered = false;
      self.raw_db = None;
      // Safety: conn is always Some until this method consumes self
      self.conn.take().unwrap()
   }
}

impl Drop for ObservableConnection {
   fn drop(&mut self) {
      if self.hooks_registered
         && let Some(db) = self.raw_db
      {
         // SAFETY: db was obtained from lock_handle during register_hooks and
         // remains valid because we still own the PoolConnection (self.conn).
         // The connection has not been taken (into_inner clears hooks_registered).
         unsafe {
            hooks::unregister_hooks(db);
         }
         trace!("ObservableConnection dropped, hooks unregistered");
      }
   }
}

impl Deref for ObservableConnection {
   type Target = PoolConnection<Sqlite>;

   fn deref(&self) -> &Self::Target {
      self.conn_ref()
   }
}

impl DerefMut for ObservableConnection {
   fn deref_mut(&mut self) -> &mut Self::Target {
      self.conn_mut()
   }
}

impl AsRef<PoolConnection<Sqlite>> for ObservableConnection {
   fn as_ref(&self) -> &PoolConnection<Sqlite> {
      self.conn_ref()
   }
}

impl AsMut<PoolConnection<Sqlite>> for ObservableConnection {
   fn as_mut(&mut self) -> &mut PoolConnection<Sqlite> {
      self.conn_mut()
   }
}
