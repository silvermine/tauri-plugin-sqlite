use serde::{Serialize, Serializer};

/// Result type alias for plugin operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Structured error response for frontend.
#[derive(Serialize)]
struct ErrorResponse {
   code: String,
   message: String,
}

/// Error types for the SQLite plugin.
///
/// Plugin-specific errors wrap toolkit errors with additional state-management variants.
#[derive(Debug, thiserror::Error)]
pub enum Error {
   /// Error from the SQLite toolkit (database operations).
   #[error(transparent)]
   Toolkit(#[from] sqlx_sqlite_toolkit::Error),

   /// Error from database migrations.
   #[error(transparent)]
   Migration(#[from] sqlx::migrate::MigrateError),

   /// Invalid database path provided.
   #[error("invalid database path: {0}")]
   InvalidPath(String),

   /// Attempted to access a database that hasn't been loaded.
   #[error("database {0} not loaded")]
   DatabaseNotLoaded(String),

   /// Generic error for operations that don't fit other categories.
   #[error("{0}")]
   Other(String),
}

impl From<sqlx_sqlite_conn_mgr::Error> for Error {
   fn from(e: sqlx_sqlite_conn_mgr::Error) -> Self {
      Error::Toolkit(sqlx_sqlite_toolkit::Error::from(e))
   }
}

impl From<sqlx::Error> for Error {
   fn from(e: sqlx::Error) -> Self {
      Error::Toolkit(sqlx_sqlite_toolkit::Error::from(e))
   }
}

impl From<std::io::Error> for Error {
   fn from(e: std::io::Error) -> Self {
      Error::Toolkit(sqlx_sqlite_toolkit::Error::from(e))
   }
}

impl Error {
   /// Extract a structured error code from the error type.
   ///
   /// This provides machine-readable error codes for frontend error handling.
   fn error_code(&self) -> String {
      match self {
         Error::Toolkit(e) => e.error_code(),
         Error::Migration(_) => "MIGRATION_ERROR".to_string(),
         Error::InvalidPath(_) => "INVALID_PATH".to_string(),
         Error::DatabaseNotLoaded(_) => "DATABASE_NOT_LOADED".to_string(),
         Error::Other(_) => "ERROR".to_string(),
      }
   }
}

impl Serialize for Error {
   fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
   where
      S: Serializer,
   {
      let response = ErrorResponse {
         code: self.error_code(),
         message: self.to_string(),
      };
      response.serialize(serializer)
   }
}

#[cfg(test)]
mod tests {
   use super::*;

   #[test]
   fn test_error_code_database_not_loaded() {
      let err = Error::DatabaseNotLoaded("test.db".into());
      assert_eq!(err.error_code(), "DATABASE_NOT_LOADED");
   }

   #[test]
   fn test_error_code_invalid_path() {
      let err = Error::InvalidPath("/bad/path".into());
      assert_eq!(err.error_code(), "INVALID_PATH");
   }

   #[test]
   fn test_error_code_unsupported_datatype() {
      let err = Error::Toolkit(sqlx_sqlite_toolkit::Error::UnsupportedDatatype(
         "WEIRD_TYPE".into(),
      ));
      assert_eq!(err.error_code(), "UNSUPPORTED_DATATYPE");
   }

   #[test]
   fn test_error_code_multiple_rows() {
      let err = Error::Toolkit(sqlx_sqlite_toolkit::Error::MultipleRowsReturned(5));
      assert_eq!(err.error_code(), "MULTIPLE_ROWS_RETURNED");
   }

   #[test]
   fn test_error_serialization_structure() {
      let err = Error::DatabaseNotLoaded("mydb.db".into());
      let json = serde_json::to_value(&err).unwrap();

      // Verify structure has both code and message fields
      assert!(json.is_object());
      assert!(json.get("code").is_some());
      assert!(json.get("message").is_some());
   }

   #[test]
   fn test_error_serialization_database_not_loaded() {
      let err = Error::DatabaseNotLoaded("mydb.db".into());
      let json = serde_json::to_value(&err).unwrap();

      assert_eq!(json["code"], "DATABASE_NOT_LOADED");
      assert!(json["message"].as_str().unwrap().contains("mydb.db"));
      assert!(json["message"].as_str().unwrap().contains("not loaded"));
   }

   #[test]
   fn test_error_serialization_invalid_path() {
      let err = Error::InvalidPath("/bad/path".into());
      let json = serde_json::to_value(&err).unwrap();

      assert_eq!(json["code"], "INVALID_PATH");
      assert!(json["message"].as_str().unwrap().contains("/bad/path"));
   }

   #[test]
   fn test_error_serialization_multiple_rows() {
      let err = Error::Toolkit(sqlx_sqlite_toolkit::Error::MultipleRowsReturned(3));
      let json = serde_json::to_value(&err).unwrap();

      assert_eq!(json["code"], "MULTIPLE_ROWS_RETURNED");
      let message = json["message"].as_str().unwrap();
      assert!(message.contains("3 rows"));
      assert!(message.contains("0 or 1"));
   }

   #[test]
   fn test_error_message_format() {
      // Verify error messages are descriptive
      let err = Error::Toolkit(sqlx_sqlite_toolkit::Error::MultipleRowsReturned(5));
      let message = err.to_string();
      assert!(message.contains("fetchOne()"));
      assert!(message.contains("5 rows"));
      assert!(message.contains("expected 0 or 1"));
   }

   #[test]
   fn test_error_code_transaction_rollback_failed() {
      let err = Error::Toolkit(sqlx_sqlite_toolkit::Error::TransactionRollbackFailed {
         transaction_error: "constraint violation".to_string(),
         rollback_error: "connection lost".to_string(),
      });
      assert_eq!(err.error_code(), "TRANSACTION_ROLLBACK_FAILED");
   }

   #[test]
   fn test_error_serialization_transaction_rollback_failed() {
      let err = Error::Toolkit(sqlx_sqlite_toolkit::Error::TransactionRollbackFailed {
         transaction_error: "constraint violation".to_string(),
         rollback_error: "connection lost".to_string(),
      });
      let json = serde_json::to_value(&err).unwrap();

      assert_eq!(json["code"], "TRANSACTION_ROLLBACK_FAILED");
      let message = json["message"].as_str().unwrap();
      assert!(message.contains("constraint violation"));
      assert!(message.contains("connection lost"));
      assert!(message.contains("transaction failed"));
      assert!(message.contains("rollback also failed"));
   }
}
