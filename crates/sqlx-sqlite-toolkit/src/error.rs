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

   #[test]
   fn test_error_code_unsupported_datatype() {
      let err = Error::UnsupportedDatatype("WEIRD".into());
      assert_eq!(err.error_code(), "UNSUPPORTED_DATATYPE");
   }

   #[test]
   fn test_error_code_multiple_rows_returned() {
      let err = Error::MultipleRowsReturned(5);
      assert_eq!(err.error_code(), "MULTIPLE_ROWS_RETURNED");
      assert!(err.to_string().contains("5 rows"));
   }

   #[test]
   fn test_error_code_transaction_rollback_failed() {
      let err = Error::TransactionRollbackFailed {
         transaction_error: "constraint".into(),
         rollback_error: "busy".into(),
      };
      assert_eq!(err.error_code(), "TRANSACTION_ROLLBACK_FAILED");
      assert!(err.to_string().contains("constraint"));
      assert!(err.to_string().contains("busy"));
   }

   #[test]
   fn test_error_code_transaction_already_finalized() {
      assert_eq!(
         Error::TransactionAlreadyFinalized.error_code(),
         "TRANSACTION_ALREADY_FINALIZED"
      );
   }

   #[test]
   fn test_error_code_transaction_already_active() {
      let err = Error::TransactionAlreadyActive("main.db".into());
      assert_eq!(err.error_code(), "TRANSACTION_ALREADY_ACTIVE");
      assert!(err.to_string().contains("main.db"));
   }

   #[test]
   fn test_error_code_no_active_transaction() {
      let err = Error::NoActiveTransaction("test.db".into());
      assert_eq!(err.error_code(), "NO_ACTIVE_TRANSACTION");
      assert!(err.to_string().contains("test.db"));
   }

   #[test]
   fn test_error_code_invalid_transaction_token() {
      assert_eq!(
         Error::InvalidTransactionToken.error_code(),
         "INVALID_TRANSACTION_TOKEN"
      );
   }

   #[test]
   fn test_error_code_io() {
      let err = Error::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "missing"));
      assert_eq!(err.error_code(), "IO_ERROR");
   }

   #[test]
   fn test_error_code_other() {
      let err = Error::Other("something went wrong".into());
      assert_eq!(err.error_code(), "ERROR");
      assert_eq!(err.to_string(), "something went wrong");
   }

   #[test]
   fn test_error_code_sqlx_non_database() {
      // RowNotFound is not a database error, so no SQLite code
      let err = Error::Sqlx(sqlx::Error::RowNotFound);
      assert_eq!(err.error_code(), "SQLX_ERROR");
   }

   #[test]
   fn test_error_code_empty_keyset_columns() {
      let err = Error::EmptyKeysetColumns;
      assert_eq!(err.error_code(), "EMPTY_KEYSET_COLUMNS");
      assert!(err.to_string().contains("at least one column"));
   }

   #[test]
   fn test_error_code_invalid_page_size() {
      let err = Error::InvalidPageSize;
      assert_eq!(err.error_code(), "INVALID_PAGE_SIZE");
      assert!(err.to_string().contains("greater than zero"));
   }

   #[test]
   fn test_error_code_cursor_length_mismatch() {
      let err = Error::CursorLengthMismatch {
         cursor_len: 2,
         keyset_len: 3,
      };
      assert_eq!(err.error_code(), "CURSOR_LENGTH_MISMATCH");
      assert!(err.to_string().contains("2"));
      assert!(err.to_string().contains("3"));
   }

   #[test]
   fn test_error_code_invalid_pagination_query() {
      let err = Error::InvalidPaginationQuery;
      assert_eq!(err.error_code(), "INVALID_PAGINATION_QUERY");
      assert!(err.to_string().contains("top-level ORDER BY or LIMIT"));
   }

   #[test]
   fn test_error_code_cursor_column_not_found() {
      let err = Error::CursorColumnNotFound {
         column: "score".into(),
      };
      assert_eq!(err.error_code(), "CURSOR_COLUMN_NOT_FOUND");
      assert!(err.to_string().contains("score"));
   }

   #[test]
   fn test_error_code_invalid_column_name() {
      let err = Error::InvalidColumnName {
         name: "bad;name".into(),
      };
      assert_eq!(err.error_code(), "INVALID_COLUMN_NAME");
      assert!(err.to_string().contains("bad;name"));
   }

   #[test]
   fn test_error_code_conflicting_cursors() {
      let err = Error::ConflictingCursors;
      assert_eq!(err.error_code(), "CONFLICTING_CURSORS");
      assert!(err.to_string().contains("after"));
      assert!(err.to_string().contains("before"));
   }
}
