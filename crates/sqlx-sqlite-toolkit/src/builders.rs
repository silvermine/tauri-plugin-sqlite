//! Query builders with attached database support

use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::Arc;

use indexmap::IndexMap;
use serde_json::Value as JsonValue;
use sqlx_sqlite_conn_mgr::AttachedSpec;

use crate::Error;
use crate::wrapper::{DatabaseWrapper, WriteQueryResult, bind_value};

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
   db: DatabaseWrapper,
   query: String,
   values: Vec<JsonValue>,
   attached: Vec<AttachedSpec>,
}

impl ExecuteBuilder {
   pub(crate) fn new(db: DatabaseWrapper, query: String, values: Vec<JsonValue>) -> Self {
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
         // No attached databases - use wrapper's writer (routes through observer when in use)
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
            sqlx_sqlite_conn_mgr::acquire_writer_with_attached(self.db.inner(), self.attached)
               .await?;

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

/// Helper to decode SQLite rows to JSON
pub(crate) fn decode_rows(
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
