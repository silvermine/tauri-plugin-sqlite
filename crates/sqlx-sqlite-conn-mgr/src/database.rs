//! SQLite database with connection pooling and optional write access

use sqlx::{Pool, Sqlite};
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

/// SQLite database with connection pooling for concurrent reads and optional exclusive writes.
///
/// ## Architecture
///
/// The database maintains two connection pools:
/// - **`read_pool`**: Pool of read-only connections for concurrent reads
/// - **`write_conn`**: Single-connection pool for exclusive write access (enforced by max_connections=1)
///
/// ## State Management
///
/// - **`wal_initialized`**: Tracks whether WAL journal mode has been enabled (lazy initialization)
/// - **`closed`**: Prevents use after the database has been closed
/// - **`path`**: Database file path for cleanup operations
///
/// ## Usage Pattern
///
/// ```text
/// 1. Connect to database (creates/reuses connection pools)
/// 2. Read operations: Access read_pool for concurrent reads
/// 3. Write operations: Acquire writer (lazily enables WAL on first call)
/// 4. Close database when done
/// ```
#[derive(Debug)]
pub struct SqliteDatabase {
   /// Pool of read-only connections (defaults to max_connections=6) for concurrent reads
   read_pool: Pool<Sqlite>,

   /// Single read-write connection pool (max_connections=1) for serialized writes
   write_conn: Pool<Sqlite>,

   /// Tracks if WAL mode has been initialized (set on first write)
   wal_initialized: AtomicBool,

   /// Marks database as closed to prevent further operations
   closed: AtomicBool,

   /// Path to database file (used for cleanup and registry lookups)
   path: PathBuf,
}
