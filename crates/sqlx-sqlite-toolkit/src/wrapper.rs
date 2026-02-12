use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx::sqlite::SqliteConnection;
use sqlx_sqlite_conn_mgr::{SqliteDatabase, SqliteDatabaseConfig, WriteGuard};

#[cfg(feature = "observer")]
use sqlx_sqlite_observer::{ObservableSqliteDatabase, ObservableWriteGuard, ObserverConfig};

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

/// Unified writer guard that routes through observer when enabled.
///
/// Derefs to `SqliteConnection` so it can be used with `sqlx::query().execute()`.
pub enum WriterGuard {
   /// Regular writer from the connection manager.
   Regular(WriteGuard),
   /// Observable writer that tracks changes via SQLite hooks.
   #[cfg(feature = "observer")]
   Observable(ObservableWriteGuard),
}

impl Deref for WriterGuard {
   type Target = SqliteConnection;

   fn deref(&self) -> &Self::Target {
      match self {
         WriterGuard::Regular(w) => w,
         #[cfg(feature = "observer")]
         WriterGuard::Observable(w) => w,
      }
   }
}

impl DerefMut for WriterGuard {
   fn deref_mut(&mut self) -> &mut Self::Target {
      match self {
         WriterGuard::Regular(w) => &mut *w,
         #[cfg(feature = "observer")]
         WriterGuard::Observable(w) => &mut *w,
      }
   }
}

/// Wrapper around SqliteDatabase that provides a high-level API for database operations.
///
/// This struct is the main entry point for interacting with SQLite databases through
/// the toolkit. It wraps the connection manager's `SqliteDatabase` and provides
/// builder-pattern APIs for queries, transactions, and write operations.
///
/// When the `observer` feature is enabled, the wrapper can also manage an
/// `ObservableSqliteDatabase` for change notification support.
#[derive(Clone)]
pub struct DatabaseWrapper {
   inner: Arc<SqliteDatabase>,
   #[cfg(feature = "observer")]
   observer: Option<ObservableSqliteDatabase>,
}

impl DatabaseWrapper {
   /// Get the inner Arc<SqliteDatabase> for advanced usage
   ///
   /// This is useful when you need to create `AttachedSpec` instances for cross-database
   /// operations with interruptible transactions.
   pub fn inner(&self) -> &Arc<SqliteDatabase> {
      &self.inner
   }

   #[doc(hidden)]
   pub fn inner_for_testing(&self) -> &Arc<SqliteDatabase> {
      &self.inner
   }

   /// Acquire a writer guard.
   ///
   /// When observation is enabled, returns an observable writer that tracks
   /// changes via SQLite hooks. Otherwise, returns a regular writer.
   pub async fn acquire_writer(&self) -> Result<WriterGuard, Error> {
      #[cfg(feature = "observer")]
      if let Some(ref observable) = self.observer {
         let writer = observable.acquire_writer().await.map_err(Error::Observer)?;
         return Ok(WriterGuard::Observable(writer));
      }

      Ok(WriterGuard::Regular(self.inner.acquire_writer().await?))
   }

   /// Acquire a regular (non-observable) writer connection.
   ///
   /// This always bypasses the observer, even when observation is enabled.
   /// Useful when you need a writer for operations that should not trigger
   /// change notifications (e.g., internal bookkeeping).
   pub async fn acquire_regular_writer(&self) -> Result<WriteGuard, Error> {
      Ok(self.inner.acquire_writer().await?)
   }

   /// Begin an interruptible transaction that can be paused and resumed.
   ///
   /// Returns a builder that allows attaching databases before executing the transaction.
   /// Unlike `execute_transaction()`, this allows reading uncommitted data mid-transaction.
   ///
   /// # Examples
   ///
   /// ```no_run
   /// # async fn example(db: &sqlx_sqlite_toolkit::DatabaseWrapper) -> Result<(), sqlx_sqlite_toolkit::Error> {
   /// use serde_json::json;
   ///
   /// let mut tx = db.begin_interruptible_transaction()
   ///     .execute(vec![
   ///         ("INSERT INTO users (name) VALUES (?)", vec![json!("Alice")]),
   ///     ]).await?;
   ///
   /// // Read uncommitted data within the transaction
   /// let rows = tx.read("SELECT count(*) as n FROM users".into(), vec![]).await?;
   ///
   /// tx.commit().await?;
   /// # Ok(())
   /// # }
   /// ```
   pub fn begin_interruptible_transaction(&self) -> InterruptibleTransactionBuilder {
      InterruptibleTransactionBuilder::new(self.clone())
   }

   /// Connect to a SQLite database with an absolute path.
   ///
   /// This is the core connection method. It connects to the database at the given
   /// absolute path with optional configuration.
   ///
   /// Note: `SqliteDatabase::connect()` caches instances in a global registry.
   /// Multiple calls with the same path return the same underlying database,
   /// so this wrapper is lightweight - the actual connection pools are shared.
   ///
   /// # Examples
   ///
   /// ```no_run
   /// # async fn example() -> Result<(), sqlx_sqlite_toolkit::Error> {
   /// use sqlx_sqlite_toolkit::DatabaseWrapper;
   /// use std::path::Path;
   ///
   /// let db = DatabaseWrapper::connect(Path::new("/tmp/my.db"), None).await?;
   /// # Ok(())
   /// # }
   /// ```
   pub async fn connect(
      abs_path: &std::path::Path,
      custom_config: Option<SqliteDatabaseConfig>,
   ) -> Result<Self, Error> {
      let db = SqliteDatabase::connect(abs_path, custom_config).await?;

      Ok(Self {
         inner: db,
         #[cfg(feature = "observer")]
         observer: None,
      })
   }

   /// Create a builder for write queries (INSERT/UPDATE/DELETE).
   ///
   /// Returns a builder that can optionally attach databases before executing.
   ///
   /// # Examples
   ///
   /// ```no_run
   /// # async fn example(db: &sqlx_sqlite_toolkit::DatabaseWrapper) -> Result<(), sqlx_sqlite_toolkit::Error> {
   /// use serde_json::json;
   ///
   /// let result = db.execute(
   ///     "INSERT INTO users (name, age) VALUES (?, ?)".into(),
   ///     vec![json!("Alice"), json!(30)],
   /// ).execute().await?;
   ///
   /// println!("Inserted row {}", result.last_insert_id);
   /// # Ok(())
   /// # }
   /// ```
   pub fn execute(&self, query: String, values: Vec<JsonValue>) -> crate::builders::ExecuteBuilder {
      crate::builders::ExecuteBuilder::new(self.clone(), query, values)
   }

   /// Execute multiple statements atomically within a transaction.
   ///
   /// Returns a builder that allows attaching databases before executing the transaction.
   /// All statements either succeed together or fail together.
   ///
   /// Use this when you have a batch of writes and don't need to read data mid-transaction.
   /// For transactions requiring reads of uncommitted data, use `begin_interruptible_transaction()`.
   ///
   /// # Examples
   ///
   /// ```no_run
   /// # async fn example(db: &sqlx_sqlite_toolkit::DatabaseWrapper) -> Result<(), sqlx_sqlite_toolkit::Error> {
   /// use serde_json::json;
   ///
   /// let results = db.execute_transaction(vec![
   ///     ("INSERT INTO users (name) VALUES (?)", vec![json!("Alice")]),
   ///     ("INSERT INTO users (name) VALUES (?)", vec![json!("Bob")]),
   /// ]).execute().await?;
   ///
   /// println!("Inserted {} rows total", results.len());
   /// # Ok(())
   /// # }
   /// ```
   pub fn execute_transaction(
      &self,
      statements: Vec<(&str, Vec<JsonValue>)>,
   ) -> TransactionExecutionBuilder {
      TransactionExecutionBuilder::new(self.clone(), statements)
   }

   /// Create a builder for SELECT queries returning multiple rows.
   ///
   /// Returns a builder that can optionally attach databases before executing.
   ///
   /// # Examples
   ///
   /// ```no_run
   /// # async fn example(db: &sqlx_sqlite_toolkit::DatabaseWrapper) -> Result<(), sqlx_sqlite_toolkit::Error> {
   /// let rows = db.fetch_all(
   ///     "SELECT name, age FROM users WHERE age > ?".into(),
   ///     vec![serde_json::json!(21)],
   /// ).execute().await?;
   ///
   /// for row in &rows {
   ///     println!("{}: {}", row["name"], row["age"]);
   /// }
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

   /// Create a builder for SELECT queries returning zero or one row.
   ///
   /// Returns a builder that can optionally attach databases before executing.
   /// Returns an error if the query returns more than one row.
   ///
   /// # Examples
   ///
   /// ```no_run
   /// # async fn example(db: &sqlx_sqlite_toolkit::DatabaseWrapper) -> Result<(), sqlx_sqlite_toolkit::Error> {
   /// let user = db.fetch_one(
   ///     "SELECT name FROM users WHERE id = ?".into(),
   ///     vec![serde_json::json!(1)],
   /// ).execute().await?;
   ///
   /// match user {
   ///     Some(row) => println!("Found: {}", row["name"]),
   ///     None => println!("Not found"),
   /// }
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

   /// Close the database connection.
   ///
   /// Checkpoints the WAL and closes all connection pools.
   pub async fn close(self) -> Result<(), Error> {
      self.inner.close().await?;
      Ok(())
   }

   /// Close the database connection and remove all database files.
   ///
   /// Removes the main database file, WAL, and SHM files.
   pub async fn remove(self) -> Result<(), Error> {
      self.inner.remove().await?;
      Ok(())
   }

   /// Enable observation on this database for the specified tables.
   ///
   /// After calling this, write operations will be tracked and subscribers
   /// can receive change notifications.
   ///
   /// Requires the `observer` feature.
   #[cfg(feature = "observer")]
   pub fn enable_observation(&mut self, config: ObserverConfig) {
      self.observer = Some(ObservableSqliteDatabase::new(
         Arc::clone(&self.inner),
         config,
      ));
   }

   /// Disable observation on this database.
   ///
   /// Drops the observable wrapper and stops tracking changes.
   /// Existing subscribers will stop receiving notifications.
   ///
   /// Requires the `observer` feature.
   #[cfg(feature = "observer")]
   pub fn disable_observation(&mut self) {
      self.observer = None;
   }

   /// Get a reference to the observable database, if observation is enabled.
   ///
   /// Returns `None` if observation has not been enabled via `enable_observation()`.
   ///
   /// Requires the `observer` feature.
   #[cfg(feature = "observer")]
   pub fn observable(&self) -> Option<&ObservableSqliteDatabase> {
      self.observer.as_ref()
   }

   /// Returns true if observation is currently enabled on this database.
   ///
   /// Requires the `observer` feature.
   #[cfg(feature = "observer")]
   pub fn is_observing(&self) -> bool {
      self.observer.is_some()
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
         let guard = self.db.acquire_writer().await?;
         TransactionWriter::from(guard)
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
         let guard = self.db.acquire_writer().await?;
         TransactionWriter::from(guard)
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
pub fn bind_value<'a>(
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
