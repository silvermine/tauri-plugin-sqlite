//! Query builders with attached database support

use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::Arc;

use indexmap::IndexMap;
use serde_json::Value as JsonValue;
use sqlx_sqlite_conn_mgr::AttachedSpec;

use crate::Error;
use crate::pagination::{KeysetColumn, KeysetPage, build_paginated_query};
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

/// Internal cursor position for forward vs backward pagination.
enum CursorPosition {
   Forward(Vec<JsonValue>),
   Backward(Vec<JsonValue>),
}

/// Builder for paginated SELECT queries using keyset (cursor-based) pagination
pub struct FetchPageBuilder {
   db: Arc<sqlx_sqlite_conn_mgr::SqliteDatabase>,
   query: String,
   values: Vec<JsonValue>,
   keyset: Vec<KeysetColumn>,
   page_size: usize,
   cursor: Option<CursorPosition>,
   attached: Vec<AttachedSpec>,
}

impl FetchPageBuilder {
   pub(crate) fn new(
      db: Arc<sqlx_sqlite_conn_mgr::SqliteDatabase>,
      query: String,
      values: Vec<JsonValue>,
      keyset: Vec<KeysetColumn>,
      page_size: usize,
   ) -> Self {
      Self {
         db,
         query,
         values,
         keyset,
         page_size,
         cursor: None,
         attached: Vec::new(),
      }
   }

   /// Set the cursor for fetching the next page (forward pagination).
   ///
   /// Pass the `next_cursor` from a previous `KeysetPage` to fetch the page
   /// that follows it in the original sort order.
   pub fn after(mut self, cursor: Vec<JsonValue>) -> Self {
      self.cursor = Some(CursorPosition::Forward(cursor));
      self
   }

   /// Set the cursor for fetching the previous page (backward pagination).
   ///
   /// Pass a cursor to fetch the page that precedes it in the original sort
   /// order. Rows are returned in the original sort order (not reversed).
   pub fn before(mut self, cursor: Vec<JsonValue>) -> Self {
      self.cursor = Some(CursorPosition::Backward(cursor));
      self
   }

   /// Attach additional databases for this query
   pub fn attach(mut self, attached: Vec<AttachedSpec>) -> Self {
      self.attached = attached;
      self
   }

   /// Execute the paginated query and return a page of results
   pub async fn execute(self) -> Result<KeysetPage, Error> {
      // Validate inputs
      if self.keyset.is_empty() {
         return Err(Error::EmptyKeysetColumns);
      }
      if self.page_size == 0 {
         return Err(Error::InvalidPageSize);
      }

      // Extract cursor values and direction
      let (cursor_values, backward) = match self.cursor {
         Some(CursorPosition::Forward(vals)) => (Some(vals), false),
         Some(CursorPosition::Backward(vals)) => (Some(vals), true),
         None => (None, false),
      };

      if let Some(ref vals) = cursor_values
         && vals.len() != self.keyset.len()
      {
         return Err(Error::CursorLengthMismatch {
            cursor_len: vals.len(),
            keyset_len: self.keyset.len(),
         });
      }

      // Build paginated SQL — pass the user's bind count so cursor
      // placeholders are numbered $N+1, $N+2, … and never collide with
      // the user's $1, $2, … (or positional ?) parameters.
      let (sql, cursor_bind_values) = build_paginated_query(
         &self.query,
         &self.keyset,
         cursor_values.as_deref(),
         self.page_size,
         backward,
         self.values.len(),
      )?;

      // Combine user values + cursor bind values
      let mut all_values = self.values;
      all_values.extend(cursor_bind_values);

      // Execute query
      let rows = if self.attached.is_empty() {
         let pool = self.db.read_pool()?;
         let mut q = sqlx::query(&sql);
         for value in all_values {
            q = bind_value(q, value);
         }
         q.fetch_all(pool).await?
      } else {
         let mut conn =
            sqlx_sqlite_conn_mgr::acquire_reader_with_attached(&self.db, self.attached).await?;

         let mut q = sqlx::query(&sql);
         for value in all_values {
            q = bind_value(q, value);
         }
         let rows = sqlx::Executor::fetch_all(&mut *conn, q).await?;

         // Explicit cleanup
         conn.detach_all().await?;
         rows
      };

      // Decode rows
      let mut decoded = decode_rows(rows)?;

      // Determine has_more by checking if we got more rows than page_size
      let has_more = decoded.len() > self.page_size;
      if has_more {
         decoded.truncate(self.page_size);
      }

      // Reverse rows when paginating backward to restore original sort order
      if backward {
         decoded.reverse();
      }

      // Extract continuation cursor: first row if backward, last row if forward
      let cursor_row = if backward {
         decoded.first()
      } else {
         decoded.last()
      };

      let next_cursor = if has_more {
         if let Some(row) = cursor_row {
            let mut cursor_vals = Vec::with_capacity(self.keyset.len());
            for col in &self.keyset {
               let value = row
                  .get(&col.name)
                  .ok_or_else(|| Error::CursorColumnNotFound {
                     column: col.name.clone(),
                  })?;
               cursor_vals.push(value.clone());
            }
            Some(cursor_vals)
         } else {
            None
         }
      } else {
         None
      };

      Ok(KeysetPage {
         rows: decoded,
         next_cursor,
         has_more,
      })
   }
}

impl IntoFuture for FetchPageBuilder {
   type Output = Result<KeysetPage, Error>;
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
