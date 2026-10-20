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

   /// Path traversal attempt detected.
   #[error("path traversal not allowed: {0}")]
   PathTraversal(String),

   /// Database key is not registered with the plugin.
   #[error("database key not registered: {0}")]
   PathNotRegistered(String),

   /// Attempted to access a database that hasn't been loaded.
   #[error("database {0} not loaded")]
   DatabaseNotLoaded(String),

   /// Observation not enabled for this database.
   #[error("observation not enabled for database: {0}")]
   ObservationNotEnabled(String),

   /// Too many databases loaded simultaneously.
   #[error("cannot load more than {0} databases")]
   TooManyDatabases(usize),

   /// Too many subscriptions for a single database.
   #[error("cannot create more than {0} subscriptions per database")]
   TooManySubscriptions(usize),

   /// Invalid configuration parameter.
   #[error("invalid configuration: {0}")]
   InvalidConfig(String),

   /// A later `observe()` call requested a `channelCapacity`/`captureValues`
   /// value that conflicts with the value already active for this database.
   #[error("{0}")]
   ObservationConfigConflict(String),

   /// Required plugin managed state was not found.
   #[error("required plugin state not found: {0}")]
   MissingState(String),

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
         Error::PathTraversal(_) => "PATH_TRAVERSAL".to_string(),
         Error::PathNotRegistered(_) => "PATH_NOT_REGISTERED".to_string(),
         Error::DatabaseNotLoaded(_) => "DATABASE_NOT_LOADED".to_string(),
         Error::ObservationNotEnabled(_) => "OBSERVATION_NOT_ENABLED".to_string(),
         Error::TooManyDatabases(_) => "TOO_MANY_DATABASES".to_string(),
         Error::TooManySubscriptions(_) => "TOO_MANY_SUBSCRIPTIONS".to_string(),
         Error::InvalidConfig(_) => "INVALID_CONFIG".to_string(),
         Error::ObservationConfigConflict(_) => "OBSERVATION_CONFIG_CONFLICT".to_string(),
         Error::MissingState(_) => "MISSING_STATE".to_string(),
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
   use rstest::rstest;

   #[rstest]
   #[case::database_not_loaded(Error::DatabaseNotLoaded("test.db".into()), "DATABASE_NOT_LOADED")]
   #[case::invalid_path(Error::InvalidPath("/bad/path".into()), "INVALID_PATH")]
   #[case::path_not_registered(Error::PathNotRegistered("MAIN".into()), "PATH_NOT_REGISTERED")]
   #[case::missing_state(Error::MissingState("DbInstances".into()), "MISSING_STATE")]
   #[case::unsupported_datatype(
      Error::Toolkit(sqlx_sqlite_toolkit::Error::UnsupportedDatatype("WEIRD_TYPE".into())),
      "UNSUPPORTED_DATATYPE"
   )]
   #[case::multiple_rows(
      Error::Toolkit(sqlx_sqlite_toolkit::Error::MultipleRowsReturned(5)),
      "MULTIPLE_ROWS_RETURNED"
   )]
   #[case::transaction_rollback_failed(
      Error::Toolkit(sqlx_sqlite_toolkit::Error::TransactionRollbackFailed {
         transaction_error: "constraint violation".to_string(),
         rollback_error: "connection lost".to_string(),
      }),
      "TRANSACTION_ROLLBACK_FAILED"
   )]
   fn test_error_code_matches_variant(#[case] err: Error, #[case] expected: &str) {
      assert_eq!(err.error_code(), expected);
   }

   #[test]
   fn test_error_serialization_path_not_registered() {
      let err = Error::PathNotRegistered("MAIN".into());
      let json = serde_json::to_value(&err).unwrap();

      assert_eq!(json["code"], "PATH_NOT_REGISTERED");
      assert_eq!(
         json["message"].as_str().unwrap(),
         "database key not registered: MAIN"
      );
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

   #[rstest]
   #[case::database_not_loaded(
      Error::DatabaseNotLoaded("mydb.db".into()),
      "DATABASE_NOT_LOADED",
      &["mydb.db", "not loaded"]
   )]
   #[case::invalid_path(Error::InvalidPath("/bad/path".into()), "INVALID_PATH", &["/bad/path"])]
   #[case::multiple_rows(
      Error::Toolkit(sqlx_sqlite_toolkit::Error::MultipleRowsReturned(3)),
      "MULTIPLE_ROWS_RETURNED",
      &["3 rows", "0 or 1"]
   )]
   #[case::transaction_rollback_failed(
      Error::Toolkit(sqlx_sqlite_toolkit::Error::TransactionRollbackFailed {
         transaction_error: "constraint violation".to_string(),
         rollback_error: "connection lost".to_string(),
      }),
      "TRANSACTION_ROLLBACK_FAILED",
      &["constraint violation", "connection lost", "transaction failed", "rollback also failed"]
   )]
   fn test_error_serialization_includes_code_and_message(
      #[case] err: Error,
      #[case] expected_code: &str,
      #[case] message_parts: &[&str],
   ) {
      let json = serde_json::to_value(&err).unwrap();

      assert_eq!(json["code"], expected_code);
      let message = json["message"].as_str().unwrap();
      for &part in message_parts {
         assert!(
            message.contains(part),
            "{message:?} should contain {part:?}"
         );
      }
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
}
