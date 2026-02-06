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
                    ┌────────────┼────────────┐          │
                    │            │            │          │
              ┌─────▼────┐  ┌────▼─────┐      │          │
              │  COMMIT  │  │ ROLLBACK │      │          │
              └─────┬────┘  └────┬─────┘      │          │
                    │            │            │          │
                    ▼            ▼            │          │
              on_commit()   on_rollback()     │          │
                    │            │            │          │
                    │       buffer.clear()    │          │
                    │       (discard)         │          │
                    │                         │          │
                    └─────────────────────────┴──────────┘
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
   * **`ChangeOperation`**: Insert, Update, or Delete
   * **`ColumnValue`**: Typed column value (Null, Integer, Real, Text, Blob)
   * **`ObserverConfig`**: Configuration for table filtering and channel
     capacity <!-- COMING SOON -->

### Observer Types <!-- COMING SOON -->

   * **`SqliteObserver`**: Main observer for `SqlitePool` connections
   * **`ObservableConnection`**: Connection wrapper with hooks registered

### Stream Types <!-- COMING SOON -->

   * **`TableChangeStream`**: Async stream of table changes
   * **`TableChangeStreamExt`**: Extension trait for converting receivers to
     streams

### SQLx SQLite Connection Manager Integration (feature: `conn-mgr`) <!-- COMING SOON -->

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

> **Coming in Phase 2** - Full working examples will be added in a subsequent PR.

### Basic Usage

<!-- TODO: Add basic example showing SqliteObserver usage -->

### Stream API

<!-- TODO: Add stream example showing TableChangeStream usage -->

### Value Capture

<!-- TODO: Add example showing old/new column value access -->

### SQLx SQLite Connection Manager Integration

<!-- TODO: Add example showing ObservableSqliteDatabase usage -->

## Usage Notes

### Channel Capacity

The `channel_capacity` in `ObserverConfig` determines how many changes can be
buffered. All changes in a transaction are delivered at once on commit. If your
transaction contains more mutating statements than this capacity, **messages
will be dropped**.

<!-- COMING SOON -->

```rust
let config = ObserverConfig::new()
    .with_tables(["users", "posts"])
    .with_channel_capacity(1000); // Handle large transactions
```

### Disabling Value Capture

By default, `TableChange` includes `old_values` and `new_values` with the actual
column data. Disable this for lower memory usage if you only need row IDs:

<!-- COMING SOON -->

```rust
let config = ObserverConfig::new()
    .with_tables(["users"])
    .with_capture_values(false); // Only track table + rowid
```

## License

MIT License - see [LICENSE](LICENSE) for details.
