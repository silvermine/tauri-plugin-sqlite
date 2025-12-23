use std::fs::create_dir_all;
use std::path::PathBuf;
use std::sync::Arc;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx::{Column, Executor, Row};
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
pub struct DatabaseWrapper {
   inner: Arc<SqliteDatabase>,
}

impl DatabaseWrapper {
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

   /// Execute a write query (INSERT/UPDATE/DELETE)
   pub async fn execute(
      &self,
      query: String,
      values: Vec<JsonValue>,
   ) -> Result<WriteQueryResult, Error> {
      // Acquire writer for mutations
      let mut writer = self.inner.acquire_writer().await?;

      let mut q = sqlx::query(&query);
      for value in values {
         q = bind_value(q, value);
      }

      let result = q.execute(&mut *writer).await?;
      Ok(WriteQueryResult {
         rows_affected: result.rows_affected(),
         last_insert_id: result.last_insert_rowid(),
      })
   }

   /// Execute multiple write statements atomically within a transaction.
   ///
   /// This method:
   /// 1. Begins a transaction (BEGIN)
   /// 2. Executes all statements in order
   /// 3. Commits on success (COMMIT)
   /// 4. Rolls back on any error (ROLLBACK)
   ///
   /// The writer is held for the entire transaction, ensuring atomicity.
   /// Returns the result of each statement execution.
   pub async fn execute_transaction(
      &self,
      statements: Vec<(String, Vec<JsonValue>)>,
   ) -> Result<Vec<WriteQueryResult>, Error> {
      // Acquire writer for the entire transaction
      let mut writer = self.inner.acquire_writer().await?;

      // Begin transaction
      sqlx::query("BEGIN IMMEDIATE").execute(&mut *writer).await?;

      // Execute all statements, collecting results and rolling back on error
      let result = async {
         let mut results = Vec::new();
         for (query, values) in statements {
            let mut q = sqlx::query(&query);
            for value in values {
               q = bind_value(q, value);
            }
            let exec_result = q.execute(&mut *writer).await?;
            results.push(WriteQueryResult {
               rows_affected: exec_result.rows_affected(),
               last_insert_id: exec_result.last_insert_rowid(),
            });
         }
         Ok::<Vec<WriteQueryResult>, Error>(results)
      }
      .await;

      // Commit or rollback based on result
      match result {
         Ok(results) => {
            sqlx::query("COMMIT").execute(&mut *writer).await?;
            Ok(results)
         }
         Err(e) => {
            match sqlx::query("ROLLBACK").execute(&mut *writer).await {
               // Rollback succeeded, return original error
               Ok(_) => Err(e),

               // Rollback also failed, return the rollback error and the original error
               Err(rollback_err) => Err(Error::TransactionRollbackFailed {
                  transaction_error: e.to_string(),
                  rollback_error: rollback_err.to_string(),
               }),
            }
         }
      }
   }

   /// Execute a SELECT query, possibly returning multiple rows
   pub async fn fetch_all(
      &self,
      query: String,
      values: Vec<JsonValue>,
   ) -> Result<Vec<IndexMap<String, JsonValue>>, Error> {
      // Use read pool for queries
      let pool = self.inner.read_pool()?;

      let mut q = sqlx::query(&query);
      for value in values {
         q = bind_value(q, value);
      }

      let rows = pool.fetch_all(q).await?;

      // Decode rows to JSON
      let mut values = Vec::new();
      for row in rows {
         let mut value = IndexMap::default();
         for (i, column) in row.columns().iter().enumerate() {
            let v = row.try_get_raw(i)?;
            let v = crate::decode::to_json(v)?;
            value.insert(column.name().to_string(), v);
         }
         values.push(value);
      }

      Ok(values)
   }

   /// Execute a SELECT query expecting zero or one result
   pub async fn fetch_one(
      &self,
      query: String,
      values: Vec<JsonValue>,
   ) -> Result<Option<IndexMap<String, JsonValue>>, Error> {
      // Use read pool for queries
      let pool = self.inner.read_pool()?;

      // Add LIMIT 2 to detect if query returns multiple rows
      // We only need to fetch up to 2 rows to know if there's more than 1
      let limited_query = format!("{} LIMIT 2", query.trim_end_matches(';'));

      let mut q = sqlx::query(&limited_query);
      for value in values {
         q = bind_value(q, value);
      }

      let rows = pool.fetch_all(q).await?;

      // Validate row count
      match rows.len() {
         0 => Ok(None),
         1 => {
            // Decode single row to JSON
            let row = &rows[0];
            let mut value = IndexMap::default();
            for (i, column) in row.columns().iter().enumerate() {
               let v = row.try_get_raw(i)?;
               let v = crate::decode::to_json(v)?;
               value.insert(column.name().to_string(), v);
            }
            Ok(Some(value))
         }
         count => {
            // Multiple rows returned - this is an error
            Err(Error::MultipleRowsReturned(count))
         }
      }
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
fn bind_value<'a>(
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

/// Resolve database file path relative to app config directory
fn resolve_database_path<R: Runtime>(path: &str, app: &AppHandle<R>) -> Result<PathBuf, Error> {
   let app_path = app
      .path()
      .app_config_dir()
      .expect("No App config path was found!");

   create_dir_all(&app_path).expect("Couldn't create app config dir");

   // Join the relative path to the app config directory
   Ok(app_path.join(path))
}
