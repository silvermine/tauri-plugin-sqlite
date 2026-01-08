//! Query builders with attached database support

use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::Arc;

use indexmap::IndexMap;
use serde_json::Value as JsonValue;
use sqlx_sqlite_conn_mgr::{AttachedSpec, AttachedWriteGuard};

use tracing::error;

use crate::Error;
use crate::wrapper::{WriteQueryResult, bind_value};

/// Builder for SELECT queries returning multiple rows
pub struct FetchAllBuilder {
   db: Arc<sqlx_sqlite_conn_mgr::SqliteDatabase>,
   query: String,
   values: Vec<JsonValue>,
   attached: Vec<AttachedSpec>,
}

impl FetchAllBuilder {
   pub(crate) fn new(
      db: Arc<sqlx_sqlite_conn_mgr::SqliteDatabase>,
      query: String,
      values: Vec<JsonValue>,
   ) -> Self {
      Self {
         db,
         query,
         values,
         attached: Vec::new(),
      }
   }

   /// Attach additional databases for this query
   pub fn attach(mut self, attached: Vec<AttachedSpec>) -> Self {
      self.attached = attached;
      self
   }

   /// Execute the query and return all matching rows
   pub async fn execute(self) -> Result<Vec<IndexMap<String, JsonValue>>, Error> {
      if self.attached.is_empty() {
         // No attached databases - use regular read pool
         let pool = self.db.read_pool()?;
         let mut q = sqlx::query(&self.query);
         for value in self.values {
            q = bind_value(q, value);
         }
         let rows = q.fetch_all(pool).await?;
         Ok(decode_rows(rows)?)
      } else {
         // With attached database(s) - acquire reader with attached database(s)
         let mut conn =
            sqlx_sqlite_conn_mgr::acquire_reader_with_attached(&self.db, self.attached).await?;

         let mut q = sqlx::query(&self.query);
         for value in self.values {
            q = bind_value(q, value);
         }
         let rows = sqlx::Executor::fetch_all(&mut *conn, q).await?;
         let result = decode_rows(rows)?;

         // Explicit cleanup
         conn.detach_all().await?;
         Ok(result)
      }
   }
}

impl IntoFuture for FetchAllBuilder {
   type Output = Result<Vec<IndexMap<String, JsonValue>>, Error>;
   type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

   fn into_future(self) -> Self::IntoFuture {
      Box::pin(self.execute())
   }
}

/// Builder for SELECT queries returning zero or one row
pub struct FetchOneBuilder {
   db: Arc<sqlx_sqlite_conn_mgr::SqliteDatabase>,
   query: String,
   values: Vec<JsonValue>,
   attached: Vec<AttachedSpec>,
}

impl FetchOneBuilder {
   pub(crate) fn new(
      db: Arc<sqlx_sqlite_conn_mgr::SqliteDatabase>,
      query: String,
      values: Vec<JsonValue>,
   ) -> Self {
      Self {
         db,
         query,
         values,
         attached: Vec::new(),
      }
   }

   /// Attach additional databases for this query
   pub fn attach(mut self, attached: Vec<AttachedSpec>) -> Self {
      self.attached = attached;
      self
   }

   /// Execute the query and return zero or one row
   pub async fn execute(self) -> Result<Option<IndexMap<String, JsonValue>>, Error> {
      let rows = if self.attached.is_empty() {
         // No attached databases - use regular read pool
         let pool = self.db.read_pool()?;
         let mut q = sqlx::query(&self.query);
         for value in self.values {
            q = bind_value(q, value);
         }
         q.fetch_all(pool).await?
      } else {
         // With attached database(s) - acquire reader with attached database(s)
         let mut conn =
            sqlx_sqlite_conn_mgr::acquire_reader_with_attached(&self.db, self.attached).await?;

         let mut q = sqlx::query(&self.query);
         for value in self.values {
            q = bind_value(q, value);
         }
         let rows = sqlx::Executor::fetch_all(&mut *conn, q).await?;

         // Explicit cleanup
         conn.detach_all().await?;
         rows
      };

      // Validate row count
      match rows.len() {
         0 => Ok(None),
         1 => {
            let decoded = decode_rows(vec![rows.into_iter().next().unwrap()])?;
            Ok(Some(decoded.into_iter().next().unwrap()))
         }
         count => Err(Error::MultipleRowsReturned(count)),
      }
   }
}

impl IntoFuture for FetchOneBuilder {
   type Output = Result<Option<IndexMap<String, JsonValue>>, Error>;
   type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

   fn into_future(self) -> Self::IntoFuture {
      Box::pin(self.execute())
   }
}

/// Builder for write queries (INSERT/UPDATE/DELETE)
pub struct ExecuteBuilder {
   db: Arc<sqlx_sqlite_conn_mgr::SqliteDatabase>,
   query: String,
   values: Vec<JsonValue>,
   attached: Vec<AttachedSpec>,
}

impl ExecuteBuilder {
   pub(crate) fn new(
      db: Arc<sqlx_sqlite_conn_mgr::SqliteDatabase>,
      query: String,
      values: Vec<JsonValue>,
   ) -> Self {
      Self {
         db,
         query,
         values,
         attached: Vec::new(),
      }
   }

   /// Attach additional databases for this write operation
   pub fn attach(mut self, attached: Vec<AttachedSpec>) -> Self {
      self.attached = attached;
      self
   }

   /// Execute the write operation
   pub async fn execute(self) -> Result<WriteQueryResult, Error> {
      if self.attached.is_empty() {
         // No attached databases - use regular writer
         let mut writer = self.db.acquire_writer().await?;
         let mut q = sqlx::query(&self.query);
         for value in self.values {
            q = bind_value(q, value);
         }
         let result = q.execute(&mut *writer).await?;
         Ok(WriteQueryResult {
            rows_affected: result.rows_affected(),
            last_insert_id: result.last_insert_rowid(),
         })
      } else {
         // With attached database(s) - acquire writer with attached database(s)
         let mut conn =
            sqlx_sqlite_conn_mgr::acquire_writer_with_attached(&self.db, self.attached).await?;

         let mut q = sqlx::query(&self.query);
         for value in self.values {
            q = bind_value(q, value);
         }
         let result = sqlx::Executor::execute(&mut *conn, q).await?;
         let write_result = WriteQueryResult {
            rows_affected: result.rows_affected(),
            last_insert_id: result.last_insert_rowid(),
         };

         // Explicit cleanup
         conn.detach_all().await?;
         Ok(write_result)
      }
   }
}

impl IntoFuture for ExecuteBuilder {
   type Output = Result<WriteQueryResult, Error>;
   type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

   fn into_future(self) -> Self::IntoFuture {
      Box::pin(self.execute())
   }
}

/// Builder for transaction operations
pub struct TransactionBuilder {
   db: Arc<sqlx_sqlite_conn_mgr::SqliteDatabase>,
   statements: Vec<(String, Vec<JsonValue>)>,
   attached: Vec<AttachedSpec>,
}

impl TransactionBuilder {
   pub(crate) fn new(
      db: Arc<sqlx_sqlite_conn_mgr::SqliteDatabase>,
      statements: Vec<(String, Vec<JsonValue>)>,
   ) -> Self {
      Self {
         db,
         statements,
         attached: Vec::new(),
      }
   }

   /// Attach additional databases for this transaction
   pub fn attach(mut self, attached: Vec<AttachedSpec>) -> Self {
      self.attached = attached;
      self
   }

   /// Execute the transaction
   pub async fn execute(self) -> Result<Vec<WriteQueryResult>, Error> {
      if self.attached.is_empty() {
         // No attached databases - use regular writer
         execute_transaction_with_writer(self.db.acquire_writer().await?, self.statements).await
      } else {
         // With attached database(s) - acquire writer with attached database(s)
         let guard =
            sqlx_sqlite_conn_mgr::acquire_writer_with_attached(&self.db, self.attached).await?;
         execute_transaction_with_attached(guard, self.statements).await
      }
   }
}

impl IntoFuture for TransactionBuilder {
   type Output = Result<Vec<WriteQueryResult>, Error>;
   type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

   fn into_future(self) -> Self::IntoFuture {
      Box::pin(self.execute())
   }
}

/// Helper to decode SQLite rows to JSON
fn decode_rows(
   rows: Vec<sqlx::sqlite::SqliteRow>,
) -> Result<Vec<IndexMap<String, JsonValue>>, Error> {
   use sqlx::{Column, Row};

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

/// Execute a transaction with proper BEGIN/COMMIT/ROLLBACK handling
async fn execute_transaction_with_writer(
   mut writer: sqlx_sqlite_conn_mgr::WriteGuard,
   statements: Vec<(String, Vec<JsonValue>)>,
) -> Result<Vec<WriteQueryResult>, Error> {
   // Begin transaction
   sqlx::query("BEGIN IMMEDIATE").execute(&mut *writer).await?;

   // Execute all statements
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

   // Commit or rollback
   match result {
      Ok(results) => {
         sqlx::query("COMMIT").execute(&mut *writer).await?;
         Ok(results)
      }
      Err(e) => match sqlx::query("ROLLBACK").execute(&mut *writer).await {
         Ok(_) => Err(e),
         Err(rollback_err) => Err(Error::TransactionRollbackFailed {
            transaction_error: e.to_string(),
            rollback_error: rollback_err.to_string(),
         }),
      },
   }
}

/// Execute a transaction with attached databases
async fn execute_transaction_with_attached(
   mut guard: AttachedWriteGuard,
   statements: Vec<(String, Vec<JsonValue>)>,
) -> Result<Vec<WriteQueryResult>, Error> {
   // Begin transaction
   sqlx::query("BEGIN IMMEDIATE").execute(&mut *guard).await?;

   // Execute all statements
   let result = async {
      let mut results = Vec::new();
      for (query, values) in statements {
         let mut q = sqlx::query(&query);
         for value in values {
            q = bind_value(q, value);
         }
         let exec_result = q.execute(&mut *guard).await?;
         results.push(WriteQueryResult {
            rows_affected: exec_result.rows_affected(),
            last_insert_id: exec_result.last_insert_rowid(),
         });
      }
      Ok::<Vec<WriteQueryResult>, Error>(results)
   }
   .await;

   // Commit or rollback
   match result {
      Ok(results) => {
         sqlx::query("COMMIT").execute(&mut *guard).await?;
         guard.detach_all().await?;
         Ok(results)
      }
      Err(e) => match sqlx::query("ROLLBACK").execute(&mut *guard).await {
         Ok(_) => {
            if let Err(detach_err) = guard.detach_all().await {
               error!("detach_all failed after rollback: {}", detach_err);
            }
            Err(e)
         }
         Err(rollback_err) => Err(Error::TransactionRollbackFailed {
            transaction_error: e.to_string(),
            rollback_error: rollback_err.to_string(),
         }),
      },
   }
}
