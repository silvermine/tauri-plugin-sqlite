//! Error types for sqlx-sqlite-conn-mgr

use thiserror::Error;

/// Errors that may occur when working with sqlx-sqlite-conn-mgr
#[derive(Error, Debug)]
pub enum Error {
   /// IO error when accessing database files. Standard library IO errors
   /// are converted to this variant.
   #[error("IO error: {0}")]
   Io(#[from] std::io::Error),

   /// Error from the sqlx library. Standard sqlx errors are converted to this variant
   #[error("Sqlx error: {0}")]
   Sqlx(#[from] sqlx::Error),

   /// Migration error from the sqlx migrate framework
   #[error("Migration error: {0}")]
   Migration(#[from] sqlx::migrate::MigrateError),

   /// Database has been closed and cannot be used
   #[error("Database has been closed")]
   DatabaseClosed,

   /// Cannot attach a database as read-write to a read-only connection
   #[error("Cannot attach database as read-write to a read-only connection")]
   CannotAttachReadWriteToReader,

   /// Invalid schema name provided for attached database
   #[error(
      "Invalid schema name '{0}': must contain only alphanumeric characters and underscores, and cannot start with a digit"
   )]
   InvalidSchemaName(String),

   /// Attempted to attach the same database multiple times
   #[error(
      "Database '{0}' appears multiple times in attached database list (would cause deadlock)"
   )]
   DuplicateAttachedDatabase(String),
}
