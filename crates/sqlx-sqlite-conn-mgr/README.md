# SQLx Connection Manager

A minimal wrapper around SQLx that enforces pragmatic SQLite connection
policies for mobile and desktop applications. Although this crate resides
in the `tauri-plugin-sqlite` repository, it is not dependent on Tauri
and could be used in any Rust project that needs SQLx connection
management.

## Features

   * **Maintains one read connection pool and one write connection per database**:
     Prevents violation of access policies and/or a glut of open file handles and
     (mostly) idle threads
   * **Connection pooling**:
      * Read-only pool for concurrent reads (up to 6 connections)
   * **Lazy write pool**: Single write connection pool (max_connections=1) initialized on
     first use
   * **Exclusive write access**: WriteGuard ensures serialized writes
   * **WAL mode**: Automatically enabled on first `acquire_writer()` call
     (idempotent)
      * See [WAL documentation](https://www.sqlite.org/wal.html) for details
   * **30-second idle timeout**: Both read and write connections close after
     30 seconds of inactivity
   * **No perpetual connection caching**: Zero minimum connections (min_connections=0) to
     avoid idle thread overhead

## Design Philosophy

This library follows a minimal code philosophy:

   * Uses SQLx's `SqlitePoolOptions` for all pool configuration
   * Uses SQLx's `SqliteConnectOptions` for open flags and configuration
   * Wrapper with minimal logic - delegates to SQLx wherever possible

## Usage

### Basic Example

```rust
use sqlx_sqlite_conn_mgr::SqliteDatabase;
use sqlx::query;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), sqlx_sqlite_conn_mgr::Error> {
    // Connect to database (creates if missing, returns Arc<SqliteDatabase>)
    let db = SqliteDatabase::connect("example.db").await?;

    // Multiple connects to the same path return the same instance
    let db2 = SqliteDatabase::connect("example.db").await?;
    assert!(Arc::ptr_eq(&db, &db2));

    // Use read_pool() for SELECT queries (supports concurrent reads)
    let rows = query("SELECT * FROM users")
        .fetch_all(db.read_pool()?)
        .await?;

    // Optionally acquire writer for INSERT/UPDATE/DELETE (exclusive access)
    // WAL mode is enabled automatically on first call
    let mut writer = db.acquire_writer().await?;
    query("INSERT INTO users (name) VALUES (?)")
        .bind("Alice")
        .execute(&mut *writer)
        .await?;
    // Writer is automatically returned when dropped

    // Close the database when done
    db.close().await?;
    Ok(())
}
```

## API Overview

### `SqliteDatabase`

   * `connect(path)` - Connect to a database (creates if missing, returns cached
     `Arc<SqliteDatabase>` if already open)
   * `read_pool()` - Get reference to the read-only connection pool for read
     operations (returns `Result`)
   * `acquire_writer()` - Acquire exclusive write access (returns
     `Result<WriteGuard>`, enables WAL on first call)
   * `close()` - Close the database and remove from cache (operations after
     close return `DatabaseClosed` error)
   * `close_and_remove()` - Close and delete all database files (.db, .db-wal,
     .db-shm)

### `WriteGuard`

RAII guard that provides exclusive write access. Automatically returns the
connection when dropped. Derefs to `SqliteConnection` for use with SQLx
queries.

## Guarantees

1. **Single Instance Per Database**: Only one `SqliteDatabase` instance exists per
   database file path in the process. Calling `connect()` multiple times on the
   same path returns a reference to the same cached instance.

2. **Read-Only Pool**: The read pool opens connections with `read_only(true)`,
   preventing any write operations through the pool and ensuring data integrity.

3. **WAL Mode**: WAL (Write-Ahead Logging) mode is automatically enabled
   on the first call to `acquire_writer()` per `SqliteDatabase` instance.
   The operation is idempotent and safe to call across multiple sessions,
   allowing concurrent reads during writes.

4. **Exclusive Writes**: The write pool has `max_connections=1`, ensuring
   only one writer can exist at a time. Other callers to
   `acquire_writer()` will block (asynchronously) until the current writer
   is released via `WriteGuard` drop.

5. **Connection Management**:
   * Read pool: 6 concurrent connections by default, 0 cached
      * Can be configured via `SqliteDatabaseConfig`
   * Write pool: max 1 connection, 0 cached
   * Idle timeout: 30 seconds for both pools
      * Can be configured via `SqliteDatabaseConfig`
   * No perpetual caching to minimize idle thread overhead

## Error Handling

```rust
use sqlx_sqlite_conn_mgr::SqliteDatabase;

match SqliteDatabase::connect("example.db").await {
    Ok(db) => {
        // Successfully connected (either new or existing instance)
    },
    Err(e) => {
        eprintln!("Error connecting to database: {}", e);
    }
}
```

## Development

### Building

```bash
cargo build
```

### Running Tests

```bash
cargo test
```

### Linting

This project follows the [Silvermine Rust coding standards][standards].

[standards]: https://github.com/silvermine/silvermine-info/blob/master/coding-standards/rust.md

To run linting checks:

```bash
cargo lint-clippy && cargo lint-fmt
```

### Documentation

Generate and view the documentation:

```bash
cargo doc --open
```

## Future

In the future we may publish `sqlx-sqlite-conn-mgr` as a standalone crate,
allowing it to be used in any Rust project that needs SQLx connection
management.
