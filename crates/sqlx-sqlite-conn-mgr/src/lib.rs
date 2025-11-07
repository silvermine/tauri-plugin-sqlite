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
//! - **[`Error`]**: Error type for database operations
//!
//! ## Architecture
//!
//! - **Dual pools**: Separate read-only pool (max 6 connections) and write pool (max 1 connection)
//! - **Lazy WAL mode**: Write-Ahead Logging enabled automatically on first write
//! - **Exclusive writes**: Single-connection write pool enforces serialized write access
//! - **Concurrent reads**: Multiple readers can query simultaneously via the read pool

// TODO: Remove these allows once implementation is complete
#![allow(dead_code)]
#![allow(unused)]

mod config;
mod database;
mod error;
mod write_guard;

// Re-export public types
pub use config::SqliteDatabaseConfig;
pub use database::SqliteDatabase;
pub use error::{Error, Result};
pub use write_guard::WriteGuard;
