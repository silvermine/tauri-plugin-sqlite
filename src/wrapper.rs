use std::fs::create_dir_all;
use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx_sqlite_conn_mgr::{SqliteDatabase, SqliteDatabaseConfig};
use tauri::{AppHandle, Manager, Runtime};

use crate::Error;

/// Result returned from write operations (e.g. INSERT, UPDATE, DELETE).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteQueryResult {
   /// The number of rows affected by the write operation.
   pub rows_affected: u64,
   /// The last inserted row ID (SQLite ROWID).
   ///
   /// Only set for INSERT operations on tables with a ROWID.
   /// Tables created with `WITHOUT ROWID` will not set this value (returns 0).
   pub last_insert_id: i64,
}

/// Wrapper around SqliteDatabase that adapts it for the plugin interface
#[derive(Clone)]
pub struct DatabaseWrapper {
   inner: Arc<SqliteDatabase>,
}

impl DatabaseWrapper {
   /// Get the inner Arc<SqliteDatabase> for advanced usage
   pub(crate) fn inner(&self) -> &Arc<SqliteDatabase> {
      &self.inner
   }

   /// Acquire writer connection (for pausable transactions)
   pub async fn acquire_writer(&self) -> Result<sqlx_sqlite_conn_mgr::WriteGuard, Error> {
      Ok(self.inner.acquire_writer().await?)
   }

   /// Connect to a SQLite database via the connection manager
   pub async fn connect<R: Runtime>(
      path: &str,
      app: &AppHandle<R>,
      custom_config: Option<SqliteDatabaseConfig>,
   ) -> Result<Self, Error> {
      // Resolve path relative to app_config_dir
      let abs_path = resolve_database_path(path, app)?;

      Self::connect_with_path(&abs_path, custom_config).await
   }

   /// Connect to a SQLite database with an absolute path.
   ///
   /// This is the core connection method used by `connect()`. It's also
   /// used by the migration task during plugin setup.
   ///
   /// Note: `SqliteDatabase::connect()` caches instances in a global registry.
   /// Multiple calls with the same path return the same underlying database,
   /// so this wrapper is lightweight - the actual connection pools are shared.
   pub async fn connect_with_path(
      abs_path: &std::path::Path,
      custom_config: Option<SqliteDatabaseConfig>,
   ) -> Result<Self, Error> {
      // Use connection manager to connect with optional custom config
      let db = SqliteDatabase::connect(abs_path, custom_config).await?;

      Ok(Self { inner: db })
   }

   /// Create a builder for write queries (INSERT/UPDATE/DELETE)
   ///
   /// Returns a builder that can optionally attach databases before executing.
   ///
   /// # Example
   ///
   /// ```ignore
   /// // Without attached databases
   /// db.execute(sql, params).await?;
   ///
   /// // With attached database(s)
   /// db.execute(sql, params)
   ///   .attach(vec![spec1, spec2])
   ///   .await?;
   /// ```
   pub fn execute(&self, query: String, values: Vec<JsonValue>) -> crate::builders::ExecuteBuilder {
      crate::builders::ExecuteBuilder::new(Arc::clone(&self.inner), query, values)
   }

   /// Create a builder for transaction execution
   ///
   /// Returns a builder that can optionally attach databases before executing.
   ///
   /// This method:
   /// 1. Begins a transaction (BEGIN)
   /// 2. Executes all statements in order
   /// 3. Commits on success (COMMIT)
   /// 4. Rolls back on any error (ROLLBACK)
   ///
   /// The writer is held for the entire transaction, ensuring atomicity.
   ///
   /// # Example
   ///
   /// ```ignore
   /// // Without attached databases
   /// db.execute_transaction(statements).await?;
   ///
   /// // With attached database(s)
   /// db.execute_transaction(statements)
   ///   .attach(vec![spec1, spec2])
   ///   .await?;
   /// ```
   pub fn execute_transaction(
      &self,
      statements: Vec<(String, Vec<JsonValue>)>,
   ) -> crate::builders::TransactionBuilder {
      crate::builders::TransactionBuilder::new(Arc::clone(&self.inner), statements)
   }

   /// Create a builder for SELECT queries returning multiple rows
   ///
   /// Returns a builder that can optionally attach databases before executing.
   ///
   /// # Example
   ///
   /// ```ignore
   /// // Without attached databases
   /// db.fetch_all(sql, params).await?;
   ///
   /// // With attached database(s)
   /// db.fetch_all(sql, params)
   ///   .attach(vec![spec1, spec2])
   ///   .await?;
   /// ```
   pub fn fetch_all(
      &self,
      query: String,
      values: Vec<JsonValue>,
   ) -> crate::builders::FetchAllBuilder {
      crate::builders::FetchAllBuilder::new(Arc::clone(&self.inner), query, values)
   }

   /// Create a builder for SELECT queries returning zero or one row
   ///
   /// Returns a builder that can optionally attach databases before executing.
   ///
   /// Returns an error if the query returns more than one row.
   ///
   /// # Example
   ///
   /// ```ignore
   /// // Without attached databases
   /// db.fetch_one(sql, params).await?;
   ///
   /// // With attached database(s)
   /// db.fetch_one(sql, params)
   ///   .attach(vec![spec1, spec2])
   ///   .await?;
   /// ```
   pub fn fetch_one(
      &self,
      query: String,
      values: Vec<JsonValue>,
   ) -> crate::builders::FetchOneBuilder {
      crate::builders::FetchOneBuilder::new(Arc::clone(&self.inner), query, values)
   }

   /// Run database migrations
   ///
   /// Runs all pending migrations from the provided migrator.
   /// SQLx tracks applied migrations, so this is safe to call multiple times.
   pub async fn run_migrations(
      &self,
      migrator: &sqlx_sqlite_conn_mgr::Migrator,
   ) -> Result<(), Error> {
      self.inner.run_migrations(migrator).await?;
      Ok(())
   }

   /// Close the database connection
   pub async fn close(self) -> Result<(), Error> {
      // Close via Arc (handles both owned and shared cases)
      self.inner.close().await?;
      Ok(())
   }

   /// Close the database connection and remove all database files
   pub async fn remove(self) -> Result<(), Error> {
      // Remove via Arc (handles both owned and shared cases)
      self.inner.remove().await?;
      Ok(())
   }
}

/// Helper function to bind a JSON value to a SQLx query
pub(crate) fn bind_value<'a>(
   query: sqlx::query::Query<'a, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'a>>,
   value: JsonValue,
) -> sqlx::query::Query<'a, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'a>> {
   if value.is_null() {
      query.bind(None::<JsonValue>)
   } else if value.is_string() {
      query.bind(value.as_str().unwrap().to_owned())
   } else if let Some(number) = value.as_number() {
      // Preserve integer precision by binding as i64 when possible
      if let Some(int_val) = number.as_i64() {
         query.bind(int_val)
      } else if let Some(uint_val) = number.as_u64() {
         // Try to fit u64 into i64 (SQLite's INTEGER type)
         if uint_val <= i64::MAX as u64 {
            query.bind(uint_val as i64)
         } else {
            // Value too large for i64, use f64 (will lose precision)
            query.bind(uint_val as f64)
         }
      } else {
         // Not an integer, bind as f64
         query.bind(number.as_f64().unwrap_or_default())
      }
   } else {
      query.bind(value)
   }
}

/// Resolve database file path relative to app config directory.
///
/// Paths are joined to `app_config_dir()` (e.g., `Library/Application Support/${bundleIdentifier}` on iOS).
/// Special paths like `:memory:` are passed through unchanged.
fn resolve_database_path<R: Runtime>(path: &str, app: &AppHandle<R>) -> Result<PathBuf, Error> {
   let app_path = app
      .path()
      .app_config_dir()
      .expect("No App config path was found!");

   create_dir_all(&app_path).expect("Couldn't create app config dir");

   // Join the relative path to the app config directory
   Ok(app_path.join(path))
}
