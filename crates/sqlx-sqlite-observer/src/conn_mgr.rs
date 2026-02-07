//! Integration with sqlx-sqlite-conn-mgr crate.
//!
//! This module provides observation capabilities for databases managed by
//! `sqlx-sqlite-conn-mgr`. Enable with the `conn-mgr` feature.
//!
//! Uses SQLite's native hooks for transaction-safe change tracking. Changes
//! are buffered during transactions and only published after commit.
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use sqlx_sqlite_conn_mgr::SqliteDatabase;
//! use sqlx_sqlite_observer::{ObservableSqliteDatabase, ObserverConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!    let db = SqliteDatabase::connect("mydb.db", None).await?;
//!    let config = ObserverConfig::new().with_tables(["users", "posts"]);
//!    let observable = ObservableSqliteDatabase::new(db, config);
//!
//!    let mut rx = observable.subscribe(["users"]);
//!
//!    // Use observable writer for tracked changes
//!    let mut writer = observable.acquire_writer().await?;
//!    sqlx::query("BEGIN").execute(&mut *writer).await?;
//!    sqlx::query("INSERT INTO users (name) VALUES (?)")
//!       .bind("Alice")
//!       .execute(&mut *writer)
//!       .await?;
//!
//!    sqlx::query("COMMIT").execute(&mut *writer).await?;
//!    // Changes publish on commit!
//!
//!    // Read pool works as normal (no observation needed for reads)
//!    let rows = sqlx::query("SELECT * FROM users")
//!       .fetch_all(observable.read_pool()?)
//!       .await?;
//!
//!    Ok(())
//! }
//! ```

use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use libsqlite3_sys::sqlite3;
use sqlx::sqlite::SqliteConnection;
use sqlx::{Pool, Sqlite};
use sqlx_sqlite_conn_mgr::{SqliteDatabase, WriteGuard};
use tokio::sync::broadcast;
use tracing::{debug, trace, warn};

use crate::Result;
use crate::broker::ObservationBroker;
use crate::change::TableChange;
use crate::config::ObserverConfig;
use crate::hooks;
use crate::schema::query_table_info;
use crate::stream::TableChangeStream;

/// Wrapper around `SqliteDatabase` that provides change observation.
///
/// This type integrates with `sqlx-sqlite-conn-mgr` to observe changes made
/// through the write connection while leaving read operations unaffected.
/// Uses SQLite's native hooks for transaction-safe notifications.
pub struct ObservableSqliteDatabase {
   db: Arc<SqliteDatabase>,
   broker: Arc<ObservationBroker>,
}

impl ObservableSqliteDatabase {
   /// Create a new observable database wrapper.
   ///
   /// # Arguments
   ///
   /// * `db` - The `SqliteDatabase` instance to observe
   /// * `config` - Observer configuration specifying which tables to track
   pub fn new(db: Arc<SqliteDatabase>, config: ObserverConfig) -> Self {
      let broker = ObservationBroker::new(config.channel_capacity, config.capture_values);

      if !config.tables.is_empty() {
         broker.observe_tables(config.tables.iter().map(String::as_str));
      }

      Self { db, broker }
   }

   /// Subscribe to change notifications.
   ///
   /// Returns a broadcast receiver that will receive `TableChange` events
   /// when observable tables are modified and transactions commit.
   pub fn subscribe<I, S>(&self, tables: I) -> broadcast::Receiver<TableChange>
   where
      I: IntoIterator<Item = S>,
      S: Into<String>,
   {
      let tables: Vec<String> = tables.into_iter().map(Into::into).collect();
      if !tables.is_empty() {
         self
            .broker
            .observe_tables(tables.iter().map(String::as_str));
      }
      self.broker.subscribe()
   }

   /// Subscribe and get a `Stream` for easier async iteration.
   pub fn subscribe_stream<I, S>(&self, tables: I) -> TableChangeStream
   where
      I: IntoIterator<Item = S>,
      S: Into<String>,
   {
      use crate::stream::TableChangeStreamExt;
      let tables: Vec<String> = tables.into_iter().map(Into::into).collect();
      // Register tables for observation (uses references, avoids clone)
      if !tables.is_empty() {
         self
            .broker
            .observe_tables(tables.iter().map(String::as_str));
      }
      let rx = self.broker.subscribe();
      let stream = rx.into_stream();
      if tables.is_empty() {
         stream
      } else {
         stream.filter_tables(tables)
      }
   }

   /// Get a reference to the read-only connection pool.
   ///
   /// Read operations don't need observation since they don't modify data.
   /// However, this pool is also used internally to query table schema
   /// information (primary key columns, WITHOUT ROWID status) when tables
   /// are first observed.
   pub fn read_pool(&self) -> sqlx_sqlite_conn_mgr::Result<&Pool<Sqlite>> {
      self.db.read_pool()
   }

   /// Acquire an observable write guard.
   ///
   /// The returned `ObservableWriteGuard` has observation hooks registered.
   /// Changes are published to subscribers when transactions commit.
   ///
   /// On first acquisition for each table, queries the schema to determine
   /// primary key columns and WITHOUT ROWID status.
   pub async fn acquire_writer(&self) -> Result<ObservableWriteGuard> {
      let writer = self
         .db
         .acquire_writer()
         .await
         .map_err(crate::error::Error::ConnMgr)?;

      let mut observable = ObservableWriteGuard {
         writer: Some(writer),
         hooks_registered: false,
         raw_db: None,
      };

      // Query table info for any observed tables that don't have it yet
      self.ensure_table_info().await?;

      observable.register_hooks(Arc::clone(&self.broker)).await?;
      Ok(observable)
   }

   /// Ensures TableInfo is set for all observed tables.
   ///
   /// Uses the read pool to query schema information, respecting conn-mgr's
   /// requirement that all connections be acquired through it.
   async fn ensure_table_info(&self) -> Result<()> {
      let observed = self.broker.get_observed_tables();

      // Collect tables that need schema info
      let tables_to_query: Vec<String> = observed
         .into_iter()
         .filter(|table| self.broker.get_table_info(table).is_none())
         .collect();

      if tables_to_query.is_empty() {
         return Ok(());
      }

      // Use read pool to query schema
      let pool = self.db.read_pool().map_err(crate::error::Error::ConnMgr)?;
      let mut conn = pool.acquire().await.map_err(crate::error::Error::Sqlx)?;

      for table in tables_to_query {
         match query_table_info(&mut conn, &table).await {
            Ok(Some(info)) => {
               debug!(table = %table, pk_columns = ?info.pk_columns, without_rowid = info.without_rowid, "Queried table info");
               self.broker.set_table_info(&table, info);
            }
            Ok(None) => {
               warn!(table = %table, "Table not found in schema");
            }
            Err(e) => {
               warn!(table = %table, error = %e, "Failed to query table info");
            }
         }
      }

      Ok(())
   }

   /// Get the underlying `SqliteDatabase`.
   pub fn inner(&self) -> &Arc<SqliteDatabase> {
      &self.db
   }

   /// Get the list of currently observed tables.
   pub fn observed_tables(&self) -> Vec<String> {
      self.broker.get_observed_tables()
   }

   /// Returns a reference to the underlying observation broker.
   pub fn broker(&self) -> &Arc<ObservationBroker> {
      &self.broker
   }
}

impl Clone for ObservableSqliteDatabase {
   fn clone(&self) -> Self {
      Self {
         db: Arc::clone(&self.db),
         broker: Arc::clone(&self.broker),
      }
   }
}

/// RAII guard for observable write access to the database.
///
/// This guard wraps a `WriteGuard` from `sqlx-sqlite-conn-mgr` and adds
/// change tracking via SQLite hooks. Changes are published to subscribers
/// when transactions commit.
#[must_use = "if unused, the write lock is immediately released"]
pub struct ObservableWriteGuard {
   writer: Option<WriteGuard>,
   hooks_registered: bool,
   /// Raw sqlite3 pointer, cached during register_hooks so we can
   /// call unregister_hooks synchronously in Drop without needing
   /// the async lock_handle.
   raw_db: Option<*mut sqlite3>,
}

// SAFETY: The raw_db pointer is only used for hook registration/unregistration
// and is always accessed from the same logical owner. The underlying sqlite3
// connection is already Send via sqlx's PoolConnection.
unsafe impl Send for ObservableWriteGuard {}

impl ObservableWriteGuard {
   fn writer_mut(&mut self) -> &mut WriteGuard {
      self.writer.as_mut().expect("writer already taken")
   }

   /// Registers SQLite observation hooks on this writer.
   async fn register_hooks(&mut self, broker: Arc<ObservationBroker>) -> Result<()> {
      if self.hooks_registered {
         return Ok(());
      }

      debug!("Registering SQLite observation hooks on WriteGuard");

      let writer = self.writer.as_mut().expect("writer already taken");

      // Get raw SQLite handle
      let mut handle = writer
         .lock_handle()
         .await
         .map_err(|e| crate::Error::Database(format!("Failed to lock connection handle: {}", e)))?;

      let db: *mut sqlite3 = handle.as_raw_handle().as_ptr();

      unsafe {
         hooks::register_hooks(db, broker)?;
      }

      // Cache the raw pointer so Drop can call unregister_hooks synchronously.
      // SAFETY: The pointer remains valid for the lifetime of the WriteGuard,
      // which we own via self.writer.
      self.raw_db = Some(db);
      self.hooks_registered = true;
      Ok(())
   }

   /// Consumes this wrapper and returns the underlying write guard.
   ///
   /// Hooks are unregistered before returning the guard, so it can be
   /// safely used without observation.
   pub fn into_inner(mut self) -> WriteGuard {
      // Unregister hooks before returning the writer to prevent
      // use-after-free if the broker is dropped before the connection is reused.
      if self.hooks_registered
         && let Some(db) = self.raw_db
      {
         unsafe {
            crate::hooks::unregister_hooks(db);
         }
         trace!("Hooks unregistered before returning inner WriteGuard");
      }
      self.hooks_registered = false;
      self.raw_db = None;
      self.writer.take().expect("writer already taken")
   }
}

impl Drop for ObservableWriteGuard {
   fn drop(&mut self) {
      if self.hooks_registered
         && let Some(db) = self.raw_db
      {
         // SAFETY: db was obtained from lock_handle during register_hooks and
         // remains valid because we still own the WriteGuard (self.writer).
         // The writer has not been taken (into_inner clears hooks_registered).
         unsafe {
            hooks::unregister_hooks(db);
         }
         trace!("ObservableWriteGuard dropped, hooks unregistered");
      }
   }
}

impl Deref for ObservableWriteGuard {
   type Target = SqliteConnection;

   fn deref(&self) -> &Self::Target {
      self.writer.as_ref().expect("writer already taken")
   }
}

impl DerefMut for ObservableWriteGuard {
   fn deref_mut(&mut self) -> &mut Self::Target {
      self.writer_mut()
   }
}
