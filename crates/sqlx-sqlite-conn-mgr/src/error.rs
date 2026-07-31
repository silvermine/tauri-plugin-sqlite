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

   /// Invalid schema name provided for attached database. See
   /// `attached::is_valid_schema_name` for the authoritative rule set this message
   /// must stay in sync with.
   #[error(
      "Invalid schema name '{0}': must be non-empty, contain only alphanumeric characters and underscores, not start with a digit, be at most 64 bytes long, and not be the reserved name 'main' or 'temp' (case-insensitive)"
   )]
   InvalidSchemaName(String),

   /// Attempted to attach the same database multiple times
   #[error(
      "Database '{0}' appears multiple times in attached database list (would cause deadlock)"
   )]
   DuplicateAttachedDatabase(String),

   /// Two attached-database specs used the same schema alias. Compared
   /// case-insensitively, matching SQLite's own schema namespace - a spec named `"x"`
   /// and one named `"X"` collide at `ATTACH` even though they compare unequal as
   /// plain strings.
   #[error("Schema name '{0}' is used by more than one attached database")]
   DuplicateSchemaName(String),
}
