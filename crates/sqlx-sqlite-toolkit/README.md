# SQLx SQLite Toolkit

High-level SQLite API providing builder-pattern queries, transaction management,
and JSON type decoding. Built on top of
[`sqlx-sqlite-conn-mgr`](../sqlx-sqlite-conn-mgr) and optionally integrates
[`sqlx-sqlite-observer`](../sqlx-sqlite-observer) for reactive change
notifications.

Not dependent on Tauri — usable in any Rust project.

## Features

   * **`DatabaseWrapper`**: Main entry point wrapping a connection-managed database
   * **Builder-pattern queries**: `execute()`, `fetch_all()`, `fetch_one()` with
     optional `.attach()` for cross-database operations
   * **Transactions**: Atomic `execute_transaction()` and interruptible transactions
     with mid-transaction reads
   * **JSON type decoding**: Automatic SQLite-to-JSON value conversion
     (INTEGER, REAL, TEXT, NULL, BLOB as base64)
   * **Transaction state tracking**: `ActiveInterruptibleTransactions` and
     `ActiveRegularTransactions` for managing in-flight transactions
   * **Observer integration** (optional `observer` feature): Route writes through
     `sqlx-sqlite-observer` for change notifications

## Installation

```toml
[dependencies]
sqlx-sqlite-toolkit = { version = "0.8" }

# With observer support
sqlx-sqlite-toolkit = { version = "0.8", features = ["observer"] }
```

## Usage

### Connect

```rust
use sqlx_sqlite_toolkit::DatabaseWrapper;
use std::path::Path;

let db = DatabaseWrapper::connect(Path::new("mydb.db"), None).await?;

// With custom configuration
use sqlx_sqlite_toolkit::SqliteDatabaseConfig;
use std::time::Duration;

let config = SqliteDatabaseConfig {
   max_read_connections: 10,
   idle_timeout: Duration::from_secs(60),
};
let db = DatabaseWrapper::connect(Path::new("mydb.db"), Some(config)).await?;
```

### Write Operations

```rust
use serde_json::json;

let result = db.execute(
   "INSERT INTO users (name, email) VALUES (?, ?)".into(),
   vec![json!("Alice"), json!("alice@example.com")]
).await?;

println!("Inserted row {}, affected {}", result.last_insert_id, result.rows_affected);
```

### Read Operations

```rust
use serde_json::json;

// Multiple rows — returns Vec<IndexMap<String, JsonValue>>
let users = db.fetch_all(
   "SELECT * FROM users WHERE active = ?".into(),
   vec![json!(true)]
).await?;

// Single row — returns Option<IndexMap<String, JsonValue>>
let user = db.fetch_one(
   "SELECT * FROM users WHERE id = ?".into(),
   vec![json!(42)]
).await?;
```

### Transactions

Atomic execution of multiple statements:

```rust
use serde_json::json;

let results = db.execute_transaction(vec![
   ("UPDATE accounts SET balance = balance - ? WHERE id = ?", vec![json!(100), json!(1)]),
   ("UPDATE accounts SET balance = balance + ? WHERE id = ?", vec![json!(100), json!(2)]),
]).await?;
// Commits on success, rolls back on any failure
```

### Interruptible Transactions

For transactions that need to read data mid-transaction:

```rust
use serde_json::json;

let mut tx = db.begin_interruptible_transaction()
   .execute(vec![
      ("INSERT INTO orders (user_id, total) VALUES (?, ?)", vec![json!(123), json!(0)]),
   ])
   .await?;

// Read uncommitted data
let rows = tx.read(
   "SELECT id FROM orders WHERE user_id = ? ORDER BY id DESC LIMIT 1".into(),
   vec![json!(123)]
).await?;
let order_id = rows[0].get("id").unwrap().as_i64().unwrap();

// Continue with more statements
tx.continue_with(vec![
   ("INSERT INTO order_items (order_id, product_id) VALUES (?, ?)", vec![json!(order_id), json!(456)]),
]).await?;

tx.commit().await?;
// Or: tx.rollback().await?;
```

### Cross-Database Queries

Attach other databases using the builder pattern:

```rust
use sqlx_sqlite_toolkit::{DatabaseWrapper, AttachedSpec, AttachedMode};
use serde_json::json;
use std::sync::Arc;

let main_db = DatabaseWrapper::connect("main.db".as_ref(), None).await?;
let stats_db = DatabaseWrapper::connect("stats.db".as_ref(), None).await?;

let results = main_db.execute_transaction(vec![
   ("INSERT INTO orders (user_id) VALUES (?)", vec![json!(1)]),
   ("UPDATE stats.counters SET n = n + 1", vec![]),
])
.attach(vec![AttachedSpec {
   database: Arc::clone(stats_db.inner()),
   schema_name: "stats".to_string(),
   mode: AttachedMode::ReadWrite,
}])
.await?;
```

### Transaction State Management

Track active transactions across your application:

```rust
use sqlx_sqlite_toolkit::{
   ActiveInterruptibleTransactions, ActiveRegularTransactions,
   cleanup_all_transactions,
};

let interruptible = ActiveInterruptibleTransactions::default();
let regular = ActiveRegularTransactions::default();

// Insert/remove transactions as they start/finish
// ...

// On application exit, abort all in-flight transactions
cleanup_all_transactions(&interruptible, &regular).await;
```

## API Reference

### `DatabaseWrapper`

| Method | Description |
| ------ | ----------- |
| `connect(path, config?)` | Connect to database, returns `DatabaseWrapper` |
| `execute(query, values)` | Execute write query, returns `WriteQueryResult` |
| `execute_transaction(stmts)` | Execute atomically (builder, supports `.attach()`) |
| `begin_interruptible_transaction()` | Begin interruptible transaction (builder) |
| `fetch_all(query, values)` | Fetch all rows as JSON maps |
| `fetch_one(query, values)` | Fetch single row or `None` |
| `acquire_writer()` | Acquire exclusive `WriterGuard` |
| `run_migrations(migrator)` | Run pending migrations |
| `close()` | Close connection |
| `remove()` | Close and delete database file(s) |

### `ActiveInterruptibleTransaction`

| Method | Description |
| ------ | ----------- |
| `read(query, values)` | Read within transaction (sees uncommitted data) |
| `continue_with(statements)` | Execute additional statements |
| `commit()` | Commit and release writer |
| `rollback()` | Rollback and release writer |

### Error Codes

All errors provide an `error_code()` method returning a machine-readable string:

| Code | Description |
| ---- | ----------- |
| `SQLITE_*` | SQLite-level error (constraint, etc.) |
| `SQLX_ERROR` | SQLx error without SQLite code |
| `CONNECTION_ERROR` | Connection manager error |
| `UNSUPPORTED_DATATYPE` | Unmappable SQLite type |
| `MULTIPLE_ROWS_RETURNED` | `fetch_one` got multiple rows |
| `TRANSACTION_ROLLBACK_FAILED` | Rollback failed after error |
| `TRANSACTION_ALREADY_FINALIZED` | Double commit/rollback |
| `TRANSACTION_ALREADY_ACTIVE` | Duplicate interruptible transaction |
| `NO_ACTIVE_TRANSACTION` | Remove from empty state |
| `INVALID_TRANSACTION_TOKEN` | Wrong transaction ID |
| `IO_ERROR` | File system error |

## Development

```bash
cargo build                         # Build
cargo test -p sqlx-sqlite-toolkit   # Test
cargo lint-clippy && cargo lint-fmt # Lint
```

## License

MIT
