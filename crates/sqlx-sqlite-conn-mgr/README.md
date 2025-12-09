# SQLx SQLite Connection Manager

A minimal wrapper around SQLx that enforces pragmatic SQLite connection policies
for mobile and desktop applications. Not dependent on Tauri — usable in any Rust
project needing SQLx connection management.

## Features

   * **Single instance per database path**: Prevents duplicate pools and idle threads
   * **Read pool**: Concurrent read-only connections (default: 6, configurable)
   * **Write connection**: Single exclusive writer via `WriteGuard`

     > Wait! Why? From [SQLite docs](https://sqlite.org/whentouse.html):
     > "_SQLite ... will only allow one writer at any instant in time._"
   * **WAL mode**: Enabled on first `acquire_writer()` call
   * **Idle timeout**: Connections close after 30s inactivity (configurable)
   * **No perpetual caching**: Zero minimum connections (prevents idle thread sprawl)

Delegates to SQLx's `SqlitePoolOptions` and `SqliteConnectOptions` wherever
possible — minimal wrapper logic.

## Usage

```rust
use sqlx_sqlite_conn_mgr::SqliteDatabase;
use sqlx::query;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), sqlx_sqlite_conn_mgr::Error> {
    // Connect (creates if missing, returns Arc<SqliteDatabase>)
    let db = SqliteDatabase::connect("example.db", None).await?;

    // Multiple connects to same path return same instance
    let db2 = SqliteDatabase::connect("example.db", None).await?;
    assert!(Arc::ptr_eq(&db, &db2));

    // Read queries use the pool (concurrent)
    let rows = query("SELECT * FROM users")
        .fetch_all(db.read_pool()?)
        .await?;

    // Write queries acquire exclusive access (WAL enabled on first call)
    let mut writer = db.acquire_writer().await?;
    query("INSERT INTO users (name) VALUES (?)")
        .bind("Alice")
        .execute(&mut *writer)
        .await?;
    // Writer released on drop

    db.close().await?;
    Ok(())
}
```

### Custom Configuration

```rust
use sqlx_sqlite_conn_mgr::{SqliteDatabase, SqliteDatabaseConfig};
use std::time::Duration;

let config = SqliteDatabaseConfig {
    max_read_connections: 10,  // default: 6
    idle_timeout: Duration::from_secs(60),  // default: 30s
};
let db = SqliteDatabase::connect("example.db", Some(config)).await?;
```

## API Reference

### `SqliteDatabase`

| Method | Description |
| ------ | ----------- |
| `connect(path, config)` | Connect/create database, returns cached `Arc` if already open |
| `read_pool()` | Get read-only pool reference |
| `acquire_writer()` | Acquire exclusive `WriteGuard` (enables WAL on first call) |
| `close()` | Close and remove from cache |
| `close_and_remove()` | Close and delete database files (.db, .db-wal, .db-shm) |

### `WriteGuard`

RAII guard for exclusive write access. Derefs to `SqliteConnection`. Connection
returned to pool on drop.

## Design Details

### Read-Only Pool

The read pool opens connections with `read_only(true)`, preventing write
operations and ensuring data integrity.

### WAL Mode and Synchronous Setting

WAL mode is enabled on first `acquire_writer()` call (idempotent, safe across
sessions). This library sets `PRAGMA synchronous = NORMAL` instead of `FULL`:

   * **Performance**: 2-3x faster writes — syncs only the WAL file, not after
     every checkpoint
   * **Safety in WAL mode**: WAL transactions are atomic at the WAL file level;
     crashes recover from intact WAL on next open (unlike rollback journal mode
     where `NORMAL` could cause corruption)
   * **Mobile/Desktop context**: `NORMAL` provides the best balance; `FULL` is
     for unreliable storage or power-loss-mid-fsync scenarios

See [SQLite WAL Performance Considerations][wal-perf] for details.

[wal-perf]: https://www.sqlite.org/wal.html#performance_considerations

### Exclusive Writes

The write pool has `max_connections=1`. Callers to `acquire_writer()` block
asynchronously until the current `WriteGuard` is dropped.

## Tracing

Uses [`tracing`](https://crates.io/crates/tracing) with `release_max_level_off` —
all logs compiled out of release builds. Install a `tracing-subscriber` in your
app to see logs during development.

## Development

Follows [Silvermine Rust coding standards][standards].

[standards]: https://github.com/silvermine/silvermine-info/blob/master/coding-standards/rust.md

```bash
cargo build                          # Build
cargo test                           # Test
cargo lint-clippy && cargo lint-fmt  # Lint
cargo doc --open                     # Documentation
```
