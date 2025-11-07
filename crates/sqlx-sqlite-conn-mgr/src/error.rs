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

   /// Database has been closed and cannot be used
   #[error("Database has been closed")]
   DatabaseClosed,
}

/// A type alias for Results with our Error type
pub type Result<T> = std::result::Result<T, Error>;
