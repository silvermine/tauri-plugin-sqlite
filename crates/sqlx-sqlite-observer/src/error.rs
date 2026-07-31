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

   /// An attached-database spec's schema alias collided with an entry already in
   /// the broker map - either `main`'s own alias, or another spec's alias, that
   /// was about to be silently overwritten by `HashMap::insert`.
   ///
   /// This should never actually surface: `validate_attached_specs` runs first,
   /// case-insensitively, and already rejects a `main`-aliased spec and any two
   /// specs sharing an alias before the broker map is even created. This variant
   /// exists as a second, independent guard - so that if the ATTACH ordering, or
   /// the validation call itself, ever regresses, the failure is this clear error
   /// instead of one broker's changes being silently misattributed to another's
   /// subscribers, or an opaque "database main is already in use" surfacing three
   /// steps later, one layer away, out of `ATTACH` itself.
   #[cfg(feature = "conn-mgr")]
   #[error(
      "attached schema alias '{0}' collides with an existing entry in the observation broker map"
   )]
   BrokerAliasCollision(String),
}
