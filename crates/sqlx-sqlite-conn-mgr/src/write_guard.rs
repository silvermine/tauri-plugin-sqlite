//! WriteGuard for exclusive write access to the database

use sqlx::Sqlite;
use sqlx::pool::PoolConnection;
use sqlx::sqlite::SqliteConnection;
use std::ops::{Deref, DerefMut};

/// RAII guard for exclusive write access to a database connection
///
/// This guard wraps a pool connection and returns it to the pool on drop.
/// Only one `WriteGuard` can exist at a time (enforced by max_connections=1),
/// ensuring serialized write access.
///
/// The guard derefs to `SqliteConnection` allowing direct use with sqlx queries.
///
/// # Example
/// TODO: Remove ignore once implementation is complete
/// ```ignore
/// use sqlx_sqlite_conn_mgr::SqliteDatabase;
/// use sqlx::query;
///
/// # async fn example() -> Result<(), sqlx_sqlite_conn_mgr::Error> {
/// let db = SqliteDatabase::connect("test.db").await?;
/// let mut writer = db.acquire_writer().await?;
/// // Use &mut *writer for write queries (e.g. INSERT/UPDATE/DELETE)
/// query("INSERT INTO users (name) VALUES (?)")
///     .bind("Alice")
///     .execute(&mut *writer)
///     .await?;
/// // Writer is automatically returned when dropped
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct WriteGuard {
   conn: PoolConnection<Sqlite>,
}

impl WriteGuard {
   /// Create a new WriteGuard by taking ownership of a pool connection
   pub(crate) fn new(conn: PoolConnection<Sqlite>) -> Self {
      Self { conn }
   }
}

impl Deref for WriteGuard {
   type Target = SqliteConnection;

   fn deref(&self) -> &Self::Target {
      &*self.conn
   }
}

impl DerefMut for WriteGuard {
   fn deref_mut(&mut self) -> &mut Self::Target {
      &mut *self.conn
   }
}

// Drop is automatically implemented - PoolConnection returns itself to the pool

// WriteGuard is automatically Send because PoolConnection<Sqlite> is Send
