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
   ///
   /// This is useful when you need to create `AttachedSpec` instances for cross-database
   /// operations with interruptible transactions.
   ///
   /// # Example
   ///
   /// ```no_run
   /// # use tauri_plugin_sqlite::{DatabaseWrapper, AttachedSpec, AttachedMode};
   /// # use std::sync::Arc;
   /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
   /// # let db1: DatabaseWrapper = todo!();
   /// # let db2: DatabaseWrapper = todo!();
   /// // Create an attached spec using the inner database reference
   /// let spec = AttachedSpec {
   ///     database: Arc::clone(db2.inner()),
   ///     schema_name: "other".to_string(),
   ///     mode: AttachedMode::ReadOnly,
   /// };
   /// # Ok(())
   /// # }
   /// ```
   pub fn inner(&self) -> &Arc<SqliteDatabase> {
      &self.inner
   }

   #[doc(hidden)]
   pub fn inner_for_testing(&self) -> &Arc<SqliteDatabase> {
      &self.inner
   }

   /// Acquire writer connection (for pausable transactions)
   pub async fn acquire_writer(&self) -> Result<sqlx_sqlite_conn_mgr::WriteGuard, Error> {
      Ok(self.inner.acquire_writer().await?)
   }

   /// Begin an interruptible transaction that can be paused and resumed
   ///
   /// Returns a builder that allows attaching databases before executing the transaction.
   ///
   /// # Example
   ///
   /// ```no_run
   /// # use tauri_plugin_sqlite::{DatabaseWrapper, Statement};
   /// # use serde_json::json;
   /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
   /// # let db: DatabaseWrapper = todo!();
   /// // Start transaction with initial statements
   /// let mut tx = db
   ///    .begin_interruptible_transaction()
   ///    .execute(vec![
   ///       ("DELETE FROM cache WHERE expired = 1", vec![])
   ///    ])
   ///    .await?;
   ///
   /// // Continue with more work
   /// let results = tx.continue_with(vec![
   ///    Statement {
   ///       query: "INSERT INTO items (name) VALUES (?)".to_string(),
   ///       values: vec![json!("item1")],
   ///    }
   /// ]).await?;
   ///
   /// // Commit when done
   /// tx.commit().await?;
   /// # Ok(())
   /// # }
   /// ```
   pub fn begin_interruptible_transaction(&self) -> InterruptibleTransactionBuilder {
      InterruptibleTransactionBuilder::new(self.clone())
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
   /// ```no_run
   /// # use tauri_plugin_sqlite::DatabaseWrapper;
   /// # use serde_json::json;
   /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
   /// # let db: DatabaseWrapper = todo!();
   /// # let sql = "INSERT INTO users (name) VALUES (?)";
   /// # let params = vec![json!("Alice")];
   /// // Without attached databases
   /// db.execute(sql.to_string(), params.clone()).await?;
   ///
   /// // With attached database(s)
   /// # let spec1 = todo!();
   /// # let spec2 = todo!();
   /// db.execute(sql.to_string(), params)
   ///   .attach(vec![spec1, spec2])
   ///   .await?;
   /// # Ok(())
   /// # }
   /// ```
   pub fn execute(&self, query: String, values: Vec<JsonValue>) -> crate::builders::ExecuteBuilder {
      crate::builders::ExecuteBuilder::new(Arc::clone(&self.inner), query, values)
   }

   /// Execute multiple statements atomically within a transaction
   ///
   /// Returns a builder that allows attaching databases before executing the transaction.
   /// All statements either succeed together or fail together.
   ///
   /// Use this when you have a batch of writes and don't need to read data mid-transaction.
   /// For transactions requiring reads of uncommitted data, use `begin_interruptible_transaction()`.
   ///
   /// # Example
   ///
   /// ```no_run
   /// # use tauri_plugin_sqlite::DatabaseWrapper;
   /// # use serde_json::json;
   /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
   /// # let db: DatabaseWrapper = todo!();
   /// // Execute multiple inserts atomically
   /// let results = db
   ///    .execute_transaction(vec![
   ///       ("INSERT INTO users (name) VALUES (?)", vec![json!("Alice")]),
   ///       ("INSERT INTO audit_log (action) VALUES (?)", vec![json!("user_created")])
   ///    ])
   ///    .await?;
   ///
   /// println!("User ID: {}", results[0].last_insert_id);
   /// # Ok(())
   /// # }
   /// ```
   pub fn execute_transaction(
      &self,
      statements: Vec<(&str, Vec<JsonValue>)>,
   ) -> TransactionExecutionBuilder {
      TransactionExecutionBuilder::new(self.clone(), statements)
   }

   /// Create a builder for SELECT queries returning multiple rows
   ///
   /// Returns a builder that can optionally attach databases before executing.
   ///
   /// # Example
   ///
   /// ```no_run
   /// # use tauri_plugin_sqlite::DatabaseWrapper;
   /// # use serde_json::json;
   /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
   /// # let db: DatabaseWrapper = todo!();
   /// # let sql = "SELECT * FROM users";
   /// # let params = vec![];
   /// // Without attached databases
   /// db.fetch_all(sql.to_string(), params.clone()).await?;
   ///
   /// // With attached database(s)
   /// # let spec1 = todo!();
   /// # let spec2 = todo!();
   /// db.fetch_all(sql.to_string(), params)
   ///   .attach(vec![spec1, spec2])
   ///   .await?;
   /// # Ok(())
   /// # }
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
   /// ```no_run
   /// # use tauri_plugin_sqlite::DatabaseWrapper;
   /// # use serde_json::json;
   /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
   /// # let db: DatabaseWrapper = todo!();
   /// # let sql = "SELECT * FROM users WHERE id = ?";
   /// # let params = vec![json!(1)];
   /// // Without attached databases
   /// db.fetch_one(sql.to_string(), params.clone()).await?;
   ///
   /// // With attached database(s)
   /// # let spec1 = todo!();
   /// # let spec2 = todo!();
   /// db.fetch_one(sql.to_string(), params)
   ///   .attach(vec![spec1, spec2])
   ///   .await?;
   /// # Ok(())
   /// # }
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

/// Builder for interruptible transactions with optional attached databases
pub struct InterruptibleTransactionBuilder {
   db: DatabaseWrapper,
   attached: Vec<sqlx_sqlite_conn_mgr::AttachedSpec>,
}

impl InterruptibleTransactionBuilder {
   fn new(db: DatabaseWrapper) -> Self {
      Self {
         db,
         attached: Vec::new(),
      }
   }

   /// Attach databases for cross-database operations
   ///
   /// # Example
   ///
   /// ```no_run
   /// # use tauri_plugin_sqlite::DatabaseWrapper;
   /// # use sqlx_sqlite_conn_mgr::{AttachedSpec, AttachedMode};
   /// # use serde_json::json;
   /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
   /// # let db: DatabaseWrapper = todo!();
   /// # let archive_db: std::sync::Arc<sqlx_sqlite_conn_mgr::SqliteDatabase> = todo!();
   /// let mut tx = db
   ///    .begin_interruptible_transaction()
   ///    .attach(vec![AttachedSpec {
   ///       database: archive_db,
   ///       schema_name: "archive".to_string(),
   ///       mode: AttachedMode::ReadOnly,
   ///    }])
   ///    .execute(vec![
   ///       ("INSERT INTO items SELECT * FROM archive.items", vec![])
   ///    ])
   ///    .await?;
   ///
   /// tx.commit().await?;
   /// # Ok(())
   /// # }
   /// ```
   pub fn attach(mut self, specs: Vec<sqlx_sqlite_conn_mgr::AttachedSpec>) -> Self {
      self.attached = specs;
      self
   }

   /// Execute the transaction with initial statements
   ///
   /// Returns an `InterruptibleTransaction` that can be continued, read from, committed, or rolled back.
   pub async fn execute(
      self,
      initial_statements: Vec<(&str, Vec<JsonValue>)>,
   ) -> Result<InterruptibleTransaction, Error> {
      use crate::transactions::{ActiveInterruptibleTransaction, TransactionWriter};

      // Acquire appropriate writer based on whether databases are attached
      let mut writer = if self.attached.is_empty() {
         TransactionWriter::Regular(self.db.acquire_writer().await?)
      } else {
         let guard =
            sqlx_sqlite_conn_mgr::acquire_writer_with_attached(self.db.inner(), self.attached)
               .await?;
         TransactionWriter::Attached(guard)
      };

      // Begin transaction
      writer.begin_immediate().await?;

      // Create active transaction and execute initial statements
      let mut active_tx = ActiveInterruptibleTransaction::new(
         "direct_rust_api".to_string(),
         uuid::Uuid::new_v4().to_string(),
         writer,
      );

      active_tx.continue_with(initial_statements).await?;

      Ok(InterruptibleTransaction { inner: active_tx })
   }
}

/// An active interruptible transaction that can be continued, read from, committed, or rolled back
///
/// This transaction holds a write lock on the database and will automatically rollback
/// if dropped without an explicit commit.
#[must_use = "if unused, the transaction is immediately rolled back"]
pub struct InterruptibleTransaction {
   inner: crate::transactions::ActiveInterruptibleTransaction,
}

impl InterruptibleTransaction {
   /// Continue transaction with additional statements
   ///
   /// Returns write results for each statement executed.
   pub async fn continue_with(
      &mut self,
      statements: Vec<crate::transactions::Statement>,
   ) -> Result<Vec<WriteQueryResult>, Error> {
      self.inner.continue_with(statements).await
   }

   /// Execute a read query within this transaction
   ///
   /// This allows reading uncommitted changes made within the transaction.
   pub async fn read(
      &mut self,
      query: String,
      values: Vec<JsonValue>,
   ) -> Result<Vec<indexmap::IndexMap<String, JsonValue>>, Error> {
      self.inner.read(query, values).await
   }

   /// Commit this transaction
   ///
   /// Consumes the transaction, making all changes permanent.
   pub async fn commit(self) -> Result<(), Error> {
      self.inner.commit().await
   }

   /// Rollback this transaction
   ///
   /// Consumes the transaction, discarding all changes.
   pub async fn rollback(self) -> Result<(), Error> {
      self.inner.rollback().await
   }
}

/// Builder for regular atomic transactions
pub struct TransactionExecutionBuilder {
   db: DatabaseWrapper,
   statements: Vec<(String, Vec<JsonValue>)>,
   attached: Vec<sqlx_sqlite_conn_mgr::AttachedSpec>,
}

impl TransactionExecutionBuilder {
   fn new(db: DatabaseWrapper, statements: Vec<(&str, Vec<JsonValue>)>) -> Self {
      Self {
         db,
         statements: statements
            .into_iter()
            .map(|(query, values)| (query.to_string(), values))
            .collect(),
         attached: Vec::new(),
      }
   }

   /// Attach databases for cross-database operations
   pub fn attach(mut self, specs: Vec<sqlx_sqlite_conn_mgr::AttachedSpec>) -> Self {
      self.attached = specs;
      self
   }

   /// Execute the transaction atomically
   ///
   /// All statements execute within a single transaction. If any statement fails,
   /// all changes are rolled back automatically.
   pub async fn execute(self) -> Result<Vec<WriteQueryResult>, Error> {
      use crate::transactions::TransactionWriter;

      // Acquire appropriate writer based on whether databases are attached
      let mut writer = if self.attached.is_empty() {
         TransactionWriter::Regular(self.db.acquire_writer().await?)
      } else {
         let guard =
            sqlx_sqlite_conn_mgr::acquire_writer_with_attached(self.db.inner(), self.attached)
               .await?;
         TransactionWriter::Attached(guard)
      };

      // Begin transaction
      writer.begin_immediate().await?;

      // Execute all statements
      let exec_result = async {
         let mut results = Vec::new();
         for (query, values) in self.statements {
            let mut q = sqlx::query(&query);
            for value in values {
               q = bind_value(q, value);
            }
            let exec_result = writer.execute_query(q).await?;
            results.push(WriteQueryResult {
               rows_affected: exec_result.rows_affected(),
               last_insert_id: exec_result.last_insert_rowid(),
            });
         }
         Ok::<Vec<WriteQueryResult>, Error>(results)
      }
      .await;

      // Commit or rollback
      match exec_result {
         Ok(results) => {
            writer.commit().await?;
            writer.detach_if_attached().await?;
            Ok(results)
         }
         Err(e) => {
            writer.rollback().await?;
            if let Err(detach_err) = writer.detach_if_attached().await {
               tracing::error!("detach_all failed after rollback: {}", detach_err);
            }
            Err(e)
         }
      }
   }
}

impl std::future::IntoFuture for TransactionExecutionBuilder {
   type Output = Result<Vec<WriteQueryResult>, Error>;
   type IntoFuture = std::pin::Pin<Box<dyn std::future::Future<Output = Self::Output> + Send>>;

   fn into_future(self) -> Self::IntoFuture {
      Box::pin(self.execute())
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
