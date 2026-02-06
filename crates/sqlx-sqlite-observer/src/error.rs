//! Error types for the sqlx-sqlite-observer crate.

/// Errors that can occur during observation operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
   /// Failed to register SQLite hooks.
   #[error("Hook registration failed: {0}")]
   HookRegistration(String),

   /// SQLx database error.
   #[error("Database error: {0}")]
   Sqlx(#[from] sqlx::Error),

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
