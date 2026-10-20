/// Result type alias for toolkit operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Error types for SQLite toolkit operations.
///
/// These are pure database-operation errors with no Tauri dependencies.
#[derive(Debug, thiserror::Error)]
pub enum Error {
   /// Error from SQLx operations.
   #[error(transparent)]
   Sqlx(#[from] sqlx::Error),

   /// Error from the connection manager.
   #[error(transparent)]
   ConnectionManager(#[from] sqlx_sqlite_conn_mgr::Error),

   /// SQLite type that cannot be mapped to JSON.
   #[error("unsupported datatype: {0}")]
   UnsupportedDatatype(String),

   /// Multiple rows returned from fetchOne query.
   #[error("fetchOne() query returned {0} rows, expected 0 or 1")]
   MultipleRowsReturned(usize),

   /// Transaction failed and rollback also failed.
   #[error("transaction failed: {transaction_error}; rollback also failed: {rollback_error}")]
   TransactionRollbackFailed {
      transaction_error: String,
      rollback_error: String,
   },

   /// Transaction has already been committed or rolled back.
   #[error("transaction has already been finalized (committed or rolled back)")]
   TransactionAlreadyFinalized,

   /// Transaction already active for this database.
   #[error("transaction already active for database: {0}")]
   TransactionAlreadyActive(String),

   /// No active transaction for this database.
   #[error("no active transaction for database: {0}")]
   NoActiveTransaction(String),

   /// Invalid transaction token provided.
   #[error("invalid transaction token")]
   InvalidTransactionToken,

   /// Transaction timed out (exceeded the configured timeout).
   #[error("transaction timed out for database: {0}")]
   TransactionTimedOut(String),

   /// Transaction was cancelled because the database was closed.
   #[error("transaction cancelled because database was closed: {0}")]
   TransactionCancelled(String),

   /// Multiple errors occurred during transaction cleanup.
   #[error("transaction cleanup failed with {} error(s): {}", .0.len(), Error::format_cleanup_errors(.0))]
   TransactionCleanupFailed(Vec<Error>),

   /// Error from the observer (change notifications).
   #[cfg(feature = "observer")]
   #[error(transparent)]
   Observer(#[from] sqlx_sqlite_observer::Error),

   /// I/O error when accessing database files.
   #[error("io error: {0}")]
   Io(#[from] std::io::Error),

   /// Keyset pagination requires at least one column.
   #[error("keyset pagination requires at least one column")]
   EmptyKeysetColumns,

   /// Page size must be greater than zero.
   #[error("page size must be greater than zero")]
   InvalidPageSize,

   /// Cursor length does not match keyset column count.
   #[error("cursor has {cursor_len} values but keyset has {keyset_len} columns")]
   CursorLengthMismatch {
      cursor_len: usize,
      keyset_len: usize,
   },

   /// Pagination base query must not contain top-level ORDER BY or LIMIT clauses.
   #[error(
      "pagination base query must not contain top-level ORDER BY or LIMIT clauses (these are added automatically; subquery usage is fine)"
   )]
   InvalidPaginationQuery,

   /// Keyset column not found in query results.
   #[error("keyset column '{column}' not found in query results")]
   CursorColumnNotFound { column: String },

   /// Keyset column name contains invalid characters.
   ///
   /// Column names must match `[a-zA-Z_][a-zA-Z0-9_.]*` (letters, digits,
   /// underscores, and dots for qualified names like `table.column`).
   #[error("invalid keyset column name '{name}': must match [a-zA-Z_][a-zA-Z0-9_.]*")]
   InvalidColumnName { name: String },

   /// Cannot provide both `after` and `before` cursors.
   #[error("cannot provide both 'after' and 'before' cursors")]
   ConflictingCursors,

   /// Generic error for operations that don't fit other categories.
   #[error("{0}")]
   Other(String),
}

impl Error {
   /// Format multiple cleanup errors for display.
   fn format_cleanup_errors(errors: &[Error]) -> String {
      errors
         .iter()
         .enumerate()
         .map(|(index, err)| format!("[{index}] {err}"))
         .collect::<Vec<_>>()
         .join("; ")
   }

   /// Returns individual errors when this is a cleanup aggregate.
   pub fn cleanup_errors(&self) -> Option<&[Error]> {
      match self {
         Self::TransactionCleanupFailed(errors) => Some(errors),
         _ => None,
      }
   }

   /// Extract a structured error code from the error type.
   ///
   /// This provides machine-readable error codes for error handling.
   pub fn error_code(&self) -> String {
      match self {
         Error::Sqlx(e) => {
            if let Some(code) = e.as_database_error().and_then(|db_err| db_err.code()) {
               return format!("SQLITE_{}", code);
            }
            "SQLX_ERROR".to_string()
         }
         Error::ConnectionManager(_) => "CONNECTION_ERROR".to_string(),
         Error::UnsupportedDatatype(_) => "UNSUPPORTED_DATATYPE".to_string(),
         Error::MultipleRowsReturned(_) => "MULTIPLE_ROWS_RETURNED".to_string(),
         Error::TransactionRollbackFailed { .. } => "TRANSACTION_ROLLBACK_FAILED".to_string(),
         Error::TransactionAlreadyFinalized => "TRANSACTION_ALREADY_FINALIZED".to_string(),
         Error::TransactionAlreadyActive(_) => "TRANSACTION_ALREADY_ACTIVE".to_string(),
         Error::NoActiveTransaction(_) => "NO_ACTIVE_TRANSACTION".to_string(),
         Error::InvalidTransactionToken => "INVALID_TRANSACTION_TOKEN".to_string(),
         Error::TransactionTimedOut(_) => "TRANSACTION_TIMED_OUT".to_string(),
         Error::TransactionCancelled(_) => "TRANSACTION_CANCELLED".to_string(),
         Error::TransactionCleanupFailed(_) => "TRANSACTION_CLEANUP_FAILED".to_string(),
         #[cfg(feature = "observer")]
         Error::Observer(_) => "OBSERVER_ERROR".to_string(),
         Error::Io(_) => "IO_ERROR".to_string(),
         Error::EmptyKeysetColumns => "EMPTY_KEYSET_COLUMNS".to_string(),
         Error::InvalidPageSize => "INVALID_PAGE_SIZE".to_string(),
         Error::CursorLengthMismatch { .. } => "CURSOR_LENGTH_MISMATCH".to_string(),
         Error::InvalidPaginationQuery => "INVALID_PAGINATION_QUERY".to_string(),
         Error::CursorColumnNotFound { .. } => "CURSOR_COLUMN_NOT_FOUND".to_string(),
         Error::InvalidColumnName { .. } => "INVALID_COLUMN_NAME".to_string(),
         Error::ConflictingCursors => "CONFLICTING_CURSORS".to_string(),
         Error::Other(_) => "ERROR".to_string(),
      }
   }
}

#[cfg(test)]
mod tests {
   use super::*;
   use rstest::rstest;

   #[rstest]
   #[case::unsupported_datatype(
      Error::UnsupportedDatatype("WEIRD".into()),
      "UNSUPPORTED_DATATYPE",
      &[]
   )]
   #[case::multiple_rows_returned(
      Error::MultipleRowsReturned(5),
      "MULTIPLE_ROWS_RETURNED",
      &["5 rows"]
   )]
   #[case::transaction_rollback_failed(
      Error::TransactionRollbackFailed {
         transaction_error: "constraint".into(),
         rollback_error: "busy".into(),
      },
      "TRANSACTION_ROLLBACK_FAILED",
      &["constraint", "busy"]
   )]
   #[case::transaction_already_finalized(
      Error::TransactionAlreadyFinalized,
      "TRANSACTION_ALREADY_FINALIZED",
      &[]
   )]
   #[case::transaction_already_active(
      Error::TransactionAlreadyActive("main.db".into()),
      "TRANSACTION_ALREADY_ACTIVE",
      &["main.db"]
   )]
   #[case::no_active_transaction(
      Error::NoActiveTransaction("test.db".into()),
      "NO_ACTIVE_TRANSACTION",
      &["test.db"]
   )]
   #[case::invalid_transaction_token(
      Error::InvalidTransactionToken,
      "INVALID_TRANSACTION_TOKEN",
      &[]
   )]
   #[case::io(
      Error::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "missing")),
      "IO_ERROR",
      &[]
   )]
   #[case::transaction_timed_out(
      Error::TransactionTimedOut("test.db".into()),
      "TRANSACTION_TIMED_OUT",
      &["test.db"]
   )]
   #[case::transaction_cancelled(
      Error::TransactionCancelled("MAIN".into()),
      "TRANSACTION_CANCELLED",
      &["MAIN"]
   )]
   #[case::transaction_cleanup_failed(
      Error::TransactionCleanupFailed(vec![
         Error::Other("first failure".into()),
         Error::Other("second failure".into()),
      ]),
      "TRANSACTION_CLEANUP_FAILED",
      &["2 error(s)", "first failure", "second failure"]
   )]
   #[case::other(Error::Other("something went wrong".into()), "ERROR", &[])]
   // RowNotFound is not a database error, so no SQLite code
   #[case::sqlx_non_database(Error::Sqlx(sqlx::Error::RowNotFound), "SQLX_ERROR", &[])]
   #[case::empty_keyset_columns(
      Error::EmptyKeysetColumns,
      "EMPTY_KEYSET_COLUMNS",
      &["at least one column"]
   )]
   #[case::invalid_page_size(Error::InvalidPageSize, "INVALID_PAGE_SIZE", &["greater than zero"])]
   #[case::cursor_length_mismatch(
      Error::CursorLengthMismatch {
         cursor_len: 2,
         keyset_len: 3,
      },
      "CURSOR_LENGTH_MISMATCH",
      &["2", "3"]
   )]
   #[case::invalid_pagination_query(
      Error::InvalidPaginationQuery,
      "INVALID_PAGINATION_QUERY",
      &["top-level ORDER BY or LIMIT"]
   )]
   #[case::cursor_column_not_found(
      Error::CursorColumnNotFound {
         column: "score".into(),
      },
      "CURSOR_COLUMN_NOT_FOUND",
      &["score"]
   )]
   #[case::invalid_column_name(
      Error::InvalidColumnName {
         name: "bad;name".into(),
      },
      "INVALID_COLUMN_NAME",
      &["bad;name"]
   )]
   #[case::conflicting_cursors(
      Error::ConflictingCursors,
      "CONFLICTING_CURSORS",
      &["after", "before"]
   )]
   fn test_error_code_and_message(
      #[case] err: Error,
      #[case] expected_code: &str,
      #[case] message_parts: &[&str],
   ) {
      assert_eq!(err.error_code(), expected_code);

      let message = err.to_string();
      for part in message_parts {
         assert!(
            message.contains(part),
            "{message:?} should contain {part:?}"
         );
      }
   }

   #[test]
   fn test_other_displays_message_verbatim() {
      let err = Error::Other("something went wrong".into());
      assert_eq!(err.to_string(), "something went wrong");
   }

   #[test]
   fn test_cleanup_errors_returns_each_error() {
      let err = Error::TransactionCleanupFailed(vec![
         Error::Other("first failure".into()),
         Error::Other("second failure".into()),
      ]);

      let messages: Vec<String> = err
         .cleanup_errors()
         .unwrap()
         .iter()
         .map(ToString::to_string)
         .collect();
      assert_eq!(messages, ["first failure", "second failure"]);
   }
}
