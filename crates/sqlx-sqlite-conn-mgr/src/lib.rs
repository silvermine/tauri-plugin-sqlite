//! # sqlx-sqlite-conn-mgr
//!
//! A minimal wrapper around SQLx that enforces pragmatic SQLite connection policies
//! for mobile and desktop applications.
//!
//! ## Core Types
//!
//! - **[`SqliteDatabase`]**: Main database type with separate read and write connection pools
//! - **[`SqliteDatabaseConfig`]**: Configuration for connection pool settings
//! - **[`WriteGuard`]**: RAII guard ensuring exclusive write access
//! - **[`Migrator`]**: Re-exported from sqlx for running database migrations
//! - **[`Error`]**: Error type for database operations
//!
//! ## Architecture
//!
//! - **Connection pooling**: Separate read-only pool and write pool with a max of 1 connection
//! - **Lazy WAL mode**: Write-Ahead Logging enabled automatically on first write
//! - **Exclusive writes**: Single-connection write pool enforces serialized write access
//! - **Concurrent reads**: Multiple readers can query simultaneously via the read pool
//!
//! ## Usage
//!
//! ```no_run
//! use sqlx_sqlite_conn_mgr::SqliteDatabase;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> sqlx_sqlite_conn_mgr::Result<()> {
//!     // Connect returns Arc<SqliteDatabase>
//!     let db = SqliteDatabase::connect("example.db", None).await?;
//!
//!     // Multiple connects to the same path return the same instance
//!     let db2 = SqliteDatabase::connect("example.db", None).await?;
//!     assert!(Arc::ptr_eq(&db, &db2));
//!
//!     // Use read_pool() for read queries (concurrent reads)
//!     let rows = sqlx::query("SELECT * FROM users")
//!         .fetch_all(db.read_pool()?)
//!         .await?;
//!
//!     // Optionally acquire writer for write queries (exclusive)
//!     // WAL mode is enabled automatically on first call
//!     let mut writer = db.acquire_writer().await?;
//!     sqlx::query("INSERT INTO users (name) VALUES (?)")
//!         .bind("Alice")
//!         .execute(&mut *writer)
//!         .await?;
//!
//!     // Close when done
//!     db.close().await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Design Principles
//!
//! - Uses sqlx's `SqlitePoolOptions` for all pool configuration
//! - Uses sqlx's `SqliteConnectOptions` for connection flags and configuration
//! - Minimal custom logic - delegates to sqlx wherever possible
//! - Global registry caches new database instances and returns existing ones
//! - WAL mode is enabled lazily only when writes are needed
//!
mod config;
mod database;
mod error;
mod registry;
mod write_guard;

// Re-export public types
pub use config::SqliteDatabaseConfig;
pub use database::SqliteDatabase;
pub use error::Error;
pub use write_guard::WriteGuard;

// Re-export sqlx migrate types for convenience
pub use sqlx::migrate::Migrator;

/// A type alias for Results with our custom Error type
pub type Result<T> = std::result::Result<T, Error>;
