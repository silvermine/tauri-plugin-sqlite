# SQLx SQLite Observer

Reactive change notifications for SQLite databases using sqlx.

This crate provides **transaction-safe** change notifications for SQLite databases
using SQLite's native hooks (`preupdate_hook`, `commit_hook`, `rollback_hook`).

## Features

   * **Transaction-safe notifications**: Changes only notify after successful commit
   * **Typed column values**: Access old/new values with native SQLite types
   * **Stream support**: Use `tokio_stream::Stream` for async iteration
   * **Multiple subscribers**: Broadcast channel supports multiple listeners
   * **Optional SQLx SQLite Connection Manager integration**: Works with
     `sqlx-sqlite-conn-mgr` for single-writer/multi-reader patterns

## SQLite Requirements

Requires SQLite compiled with `SQLITE_ENABLE_PREUPDATE_HOOK`.

**Important:** Most system SQLite libraries do NOT have this option enabled by
default. You have two options:

1. **Use the `bundled` feature** (recommended for most users):

   ```toml
   sqlx-sqlite-observer = { version = "0.8", features = ["bundled"] }
   ```

   This compiles SQLite from source with preupdate hook support (~1MB binary size
   increase).

2. **Provide your own SQLite** with `SQLITE_ENABLE_PREUPDATE_HOOK` compiled in.
   Use `is_preupdate_hook_enabled()` to verify at runtime.

If preupdate hooks are not available, `SqliteObserver::acquire()` will return an
error with a descriptive message.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
sqlx-sqlite-observer = "0.8"
```

For integration with `sqlx-sqlite-conn-mgr`:

```toml
[dependencies]
sqlx-sqlite-observer = { version = "0.8", features = ["conn-mgr"] }
```

## How It Works

The library uses SQLite's native hooks for transaction-safe change tracking:

```text
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ preupdate_hook  │────►│  broker.buffer  │     │   subscribers   │
│ (captures data) │     │  (Vec<Event>)   │     │                 │
└─────────────────┘     └────────┬────────┘     └─────────────────┘
                                 │                       ▲
                    ┌────────────|                       │
                    │            │                       │
              ┌─────▼────┐  ┌────▼─────┐                 │
              │  COMMIT  │  │ ROLLBACK │                 │
              └─────┬────┘  └────┬─────┘                 │
                    │            │                       │
                    ▼            ▼                       │
              on_commit()   on_rollback()                │
                    │            │                       │
                    │       buffer.clear()               │
                    │       (discard)                    │
                    │                                    │
                    └────────────────────────────────────┘
                            change_tx.send()
                            (publish)
```

1. When you acquire a connection, observation hooks are registered on the raw
   SQLite handle
2. `preupdate_hook` captures changes (table, operation, old/new values) and
   buffers them
3. `commit_hook` fires when a transaction commits, publishing buffered changes
   to subscribers
4. `rollback_hook` fires when a transaction rolls back, discarding buffered
   changes

This ensures subscribers **only receive notifications for committed changes**.

## API Reference

### Core Types

   * **`TableChange`**: Notification of a change to a database table
   * **`TableChangeEvent`**: Event yielded by `TableChangeStream` —
     either `Change(TableChange)` or `Lagged(u64)`
   * **`ChangeOperation`**: Insert, Update, or Delete
   * **`ColumnValue`**: Typed column value (Null, Integer, Real, Text, Blob)
   * **`ObserverConfig`**: Configuration for table filtering and channel
     capacity

### Observer Types

   * **`SqliteObserver`**: Main observer for `SqlitePool` connections
   * **`ObservableConnection`**: Connection wrapper with hooks registered

### Stream Types

   * **`TableChangeStream`**: Async stream of table changes
   * **`TableChangeStreamExt`**: Extension trait for converting receivers to
     streams

### SQLx SQLite Connection Manager Integration (feature: `conn-mgr`)

   * **`ObservableSqliteDatabase`**: Wrapper for `SqliteDatabase` with observation
   * **`ObservableWriteGuard`**: Write guard with hooks registered

### `TableInfo`

Schema information for observed tables (used internally, also exported).

   * `pk_columns: Vec<usize>` - Column indices forming the primary key
   * `without_rowid: bool` - Whether the table uses WITHOUT ROWID

## Primary Key Extraction

The `primary_key` field on `TableChange` always contains the actual primary key
value(s) for the affected row:

```rust
let change = rx.recv().await?;

// Single-column PK (e.g., INTEGER PRIMARY KEY)
if let Some(ColumnValue::Integer(id)) = change.primary_key.first() {
    println!("Changed row id: {}", id);
}

// Composite PK - values are in declaration order
for (i, pk_value) in change.primary_key.iter().enumerate() {
    println!("PK column {}: {:?}", i, pk_value);
}
```

**Why `primary_key` instead of just `rowid`?**

SQLite's internal `rowid` works well for tables with `INTEGER PRIMARY KEY`, but
has limitations:

   * **Text or UUID primary keys**: The `rowid` is an internal integer, not your
     actual key
   * **Composite primary keys**: The `rowid` doesn't represent your multi-column
     key
   * **WITHOUT ROWID tables**: The `rowid` from the preupdate hook is unreliable

The `primary_key` field extracts the actual primary key values from the captured
column data, giving you meaningful identifiers regardless of table structure.

### WITHOUT ROWID Tables

For tables created with `WITHOUT ROWID`, the `rowid` field in `TableChange` will
be `None`:

```rust
let change = rx.recv().await?;

if change.rowid.is_none() {
    // This is a WITHOUT ROWID table
    // Use primary_key instead
    println!("PK: {:?}", change.primary_key);
}
```

This is because SQLite's preupdate hook provides the first PRIMARY KEY column
(coerced to i64) as the "rowid" for WITHOUT ROWID tables, which may not be
meaningful/correct for non-integer or composite primary keys.

## Examples

### Basic Usage

```rust
use sqlx::SqlitePool;
use sqlx_sqlite_observer::{SqliteObserver, ObserverConfig, ColumnValue};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = SqlitePool::connect("sqlite:mydb.db").await?;
    let observer = SqliteObserver::new(pool, ObserverConfig::default());

    // Subscribe to changes on specific tables
    let mut rx = observer.subscribe(["users"]);

    // Spawn a task to handle notifications
    tokio::spawn(async move {
        while let Ok(change) = rx.recv().await {
            println!(
                "Table {} row {} was {:?}",
                change.table,
                change.rowid.unwrap_or(-1),
                change.operation
            );
            if let Some(ColumnValue::Integer(id)) = change.primary_key.first() {
                println!("  PK: {}", id);
            }
        }
    });

    // Use the observer to execute queries
    let mut conn = observer.acquire().await?;
    sqlx::query("INSERT INTO users (name) VALUES (?)")
        .bind("Alice")
        .execute(&mut **conn)
        .await?;

    Ok(())
}
```

### Stream API

```rust
use futures::StreamExt;
use sqlx::SqlitePool;
use sqlx_sqlite_observer::{
    SqliteObserver, ObserverConfig, TableChangeEvent,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = SqlitePool::connect("sqlite:mydb.db").await?;
    let config = ObserverConfig::new().with_tables(["users", "posts"]);
    let observer = SqliteObserver::new(pool, config);

    let mut stream = observer.subscribe_stream(["users"]);

    while let Some(event) = stream.next().await {
        match event {
            TableChangeEvent::Change(change) => {
                println!(
                    "Table {} row {} was {:?}",
                    change.table,
                    change.rowid.unwrap_or(-1),
                    change.operation
                );
            }
            TableChangeEvent::Lagged(n) => {
                eprintln!("Missed {} notifications", n);
            }
        }
    }

    Ok(())
}
```

### Value Capture

```rust
use sqlx::SqlitePool;
use sqlx_sqlite_observer::{SqliteObserver, ObserverConfig, ColumnValue};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = SqlitePool::connect("sqlite:mydb.db").await?;
    let config = ObserverConfig::new().with_tables(["users"]);
    let observer = SqliteObserver::new(pool, config);

    let mut rx = observer.subscribe(["users"]);
    let change = rx.recv().await?;

    // Access old/new column values
    if let Some(old) = &change.old_values {
        println!("Old values: {:?}", old);
    }
    if let Some(new) = &change.new_values {
        println!("New values: {:?}", new);
    }

    // Disable value capture for lower memory usage
    let config = ObserverConfig::new()
        .with_tables(["users"])
        .with_capture_values(false);

    let observer = SqliteObserver::new(
        SqlitePool::connect("sqlite:mydb.db").await?,
        config,
    );
    // old_values and new_values will be None

    Ok(())
}
```

### SQLx SQLite Connection Manager Integration

```rust
use std::sync::Arc;
use sqlx_sqlite_conn_mgr::SqliteDatabase;
use sqlx_sqlite_observer::{
    ObservableSqliteDatabase, ObserverConfig,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = SqliteDatabase::connect("mydb.db", None).await?;
    let config = ObserverConfig::new().with_tables(["users"]);
    let observable = ObservableSqliteDatabase::new(db, config);

    let mut rx = observable.subscribe(["users"]);

    // Write through the observable writer
    let mut writer = observable.acquire_writer().await?;
    sqlx::query("BEGIN").execute(&mut *writer).await?;
    sqlx::query("INSERT INTO users (name) VALUES (?)")
        .bind("Alice")
        .execute(&mut *writer)
        .await?;
    sqlx::query("COMMIT").execute(&mut *writer).await?;

    // Notification arrives after commit
    let change = rx.recv().await?;
    println!("Changed: {}", change.table);

    Ok(())
}
```

## Usage Notes

### Channel Capacity

The `channel_capacity` in `ObserverConfig` determines how many changes can be
buffered. All changes in a transaction are delivered at once on commit. If your
transaction contains more mutating statements than this capacity, **messages
will be dropped**.

```rust
let config = ObserverConfig::new()
    .with_tables(["users", "posts"])
    .with_channel_capacity(1000); // Handle large transactions
```

### Handling Lag

When using the Stream API, the stream yields `TableChangeEvent` values.
Most events are `Change` variants, but if a consumer falls behind, the
stream yields a `Lagged(n)` event indicating how many notifications
were missed.

```rust
use futures::StreamExt;
use sqlx_sqlite_observer::TableChangeEvent;
# use sqlx_sqlite_observer::{SqliteObserver, ObserverConfig};
# async fn example(observer: SqliteObserver) {

let mut stream = observer.subscribe_stream(["users"]);

while let Some(event) = stream.next().await {
    match event {
        TableChangeEvent::Change(change) => {
            // Process the change normally
        }
        TableChangeEvent::Lagged(n) => {
            // n notifications were missed — local state may be stale.
            // Re-query the database for current state.
            tracing::warn!("Missed {} change notifications", n);
        }
    }
}
# }
```

**When does lag happen?** The broadcast channel has a fixed capacity
(default 256). Lag occurs when the oldest unread messages are
overwritten. This can happen in two ways:

   * A subscriber processes changes slower than they arrive
   * A single transaction contains more mutating statements than the
     channel capacity, causing messages to be overwritten before the
     consumer reads them

This is rare under normal conditions but can occur during bulk
writes or large transactions.

**How to prevent it:**

   * Increase `channel_capacity` via `ObserverConfig::with_channel_capacity`
   * Process changes faster (avoid blocking in the stream consumer)
   * Use a dedicated task for stream consumption

**Note:** The `broadcast::Receiver` API (from `subscribe()`) surfaces
lag as `RecvError::Lagged(n)` — the same information, just through
the raw tokio broadcast channel interface rather than the stream.

### Disabling Value Capture

By default, `TableChange` includes `old_values` and `new_values` with the actual
column data. Disable this for lower memory usage if you only need row IDs:

```rust
let config = ObserverConfig::new()
    .with_tables(["users"])
    .with_capture_values(false); // Only track table + rowid
```

## License

MIT License - see [LICENSE](LICENSE) for details.
