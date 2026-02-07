//! Error types for the sqlx-sqlite-observer crate.

/// Errors that can occur during observation operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
   /// Failed to register SQLite hooks.
   #[error("Hook registration failed: {0}")]
   HookRegistration(String),

   /// SQLx database error.
   #[error("SQLx error: {0}")]
   Sqlx(#[from] sqlx::Error),

   /// Failed to acquire connection from pool.
   #[error("Failed to acquire connection from pool")]
   PoolAcquire,

   /// Connection manager error.
   #[cfg(feature = "conn-mgr")]
   #[error("Connection manager error: {0}")]
   ConnMgr(#[from] sqlx_sqlite_conn_mgr::Error),

   /// Database error (non-sqlx).
   #[error("Database error: {0}")]
   Database(String),

   /// Schema mismatch - table schema changed while observing.
   #[error(
      "Schema mismatch for table '{table}': expected {expected} PK columns, but only {actual} values available"
   )]
   SchemaMismatch {
      table: String,
      expected: usize,
      actual: usize,
   },
}
