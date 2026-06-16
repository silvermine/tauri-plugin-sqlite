//! Configuration for SQLite database connection pools

use serde::{Deserialize, Serialize};

/// Configuration for SqliteDatabase connection pools
///
/// # Examples
///
/// ```
/// use sqlx_sqlite_conn_mgr::SqliteDatabaseConfig;
///
/// // Use defaults
/// let config = SqliteDatabaseConfig::default();
///
/// // Customize specific fields
/// let config = SqliteDatabaseConfig {
///     max_read_connections: 3,
///     idle_timeout_secs: 60,
/// };
///
/// // Override just one field
/// let config = SqliteDatabaseConfig {
///     max_read_connections: 3,
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SqliteDatabaseConfig {
   /// Maximum number of concurrent read connections
   ///
   /// This controls the size of the read-only connection pool.
   /// Higher values allow more concurrent read queries but consume more resources.
   ///
   /// Default: 6
   pub max_read_connections: u32,

   /// Idle timeout for both read and write connections (in seconds)
   ///
   /// Connections that remain idle for this duration will be closed automatically.
   /// This helps prevent resource exhaustion from idle threads.
   ///
   /// Default: 30
   pub idle_timeout_secs: u64,
}

impl Default for SqliteDatabaseConfig {
   fn default() -> Self {
      Self {
         max_read_connections: 6,
         idle_timeout_secs: 30,
      }
   }
}
