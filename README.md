# Tauri Plugin SQLite

[![CI][ci-badge]][ci-url]

SQLite database interface for Tauri applications using
[sqlx](https://github.com/launchbadge/sqlx) and
[sqlx-sqlite-conn-mgr](crates/sqlx-sqlite-conn-mgr).

[ci-badge]: https://github.com/silvermine/tauri-plugin-sqlite/actions/workflows/ci.yml/badge.svg
[ci-url]: https://github.com/silvermine/tauri-plugin-sqlite/actions/workflows/ci.yml

## Features

   * **Optimized Connection Pooling**: Separate read and write pools for concurrent reads
     even while writing (configurable pool size and idle timeouts)
   * **Write Serialization**: Exclusive write connection

     > Wait! Why? From [SQLite docs](https://sqlite.org/whentouse.html):
     > "_SQLite ... will only allow one writer at any instant in time._"
   * **WAL Mode**: Enabled automatically on first write operation
   * **Type Safety**: Full TypeScript bindings
   * **Migration Support**: SQLx's migration framework (coming soon)
   * **Resource Management**: Proper cleanup on application exit (coming soon)

## Architecture

| Operation Type       | Method          | Pool Used        | Concurrency         |
| -------------------- | --------------- | ---------------- | ------------------- |
| SELECT (multiple)    | `fetchAll()`    | Read pool        | Multiple concurrent |
| SELECT (single)      | `fetchOne()`    | Read pool        | Multiple concurrent |
| INSERT/UPDATE/DELETE | `execute()`     | Write connection | Serialized          |
| DDL (CREATE, etc.)   | `execute()`     | Write connection | Serialized          |

See [`crates/sqlx-sqlite-conn-mgr/README.md`](crates/sqlx-sqlite-conn-mgr/README.md) for
connection manager internals.

## Installation

_Requires Rust **1.77.2** or later_

### Rust

`src-tauri/Cargo.toml`:

```toml
[dependencies]
tauri-plugin-sqlite = { git = "https://github.com/silvermine/tauri-plugin-sqlite" }
```

### JavaScript/TypeScript

```sh
npm install @silvermine/tauri-plugin-sqlite
```

### Permissions

Add to `src-tauri/capabilities/default.json`:

```json
{
   "permissions": ["sqlite:default"]
}
```

Or specify individual permissions:

```json
{
   "permissions": [
      "sqlite:allow-load",
      "sqlite:allow-select",
      "sqlite:allow-select-one",
      "sqlite:allow-execute-write",
      "sqlite:allow-close",
      "sqlite:allow-close-all",
      "sqlite:allow-remove"
   ]
}
```

## Usage

### Setup

Register the plugin in your Tauri application:

`src-tauri/src/lib.rs`:

```rust
fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_sqlite::Builder::new().build())
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
```

### Connecting

```typescript
import Database from '@silvermine/tauri-plugin-sqlite'

// Path is relative to app config directory (no sqlite: prefix needed)
const db = await Database.load('mydb.db')

// With custom configuration
const db = await Database.load('mydb.db', {
   maxReadConnections: 10, // default: 6
   idleTimeoutSecs: 60     // default: 30
})

// Lazy initialization (connects on first query)
const db = Database.get('mydb.db')
```

### Parameter Binding

All query methods use `$1`, `$2`, etc. syntax with `SqlValue` types:

```typescript
type SqlValue = string | number | boolean | null | Uint8Array
```

| SQLite Type | TypeScript Type | Notes |
| ----------- | --------------- | ----- |
| TEXT        | `string`        | Also for DATE, TIME, DATETIME |
| INTEGER     | `number`        | Integers preserved up to i64 range |
| REAL        | `number`        | Floating point |
| BOOLEAN     | `boolean`       | |
| NULL        | `null`          | |
| BLOB        | `Uint8Array`    | Binary data |

> **Note:** JavaScript safely represents integers up to ±2^53 - 1. The plugin binds
> integers as SQLite's INTEGER type (i64), maintaining full precision within that range.

### Write Operations

Use `execute()` for INSERT, UPDATE, DELETE, CREATE, etc.:

```typescript
await db.execute(
   'CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)'
)

const result = await db.execute(
   'INSERT INTO users (name, email) VALUES ($1, $2)',
   ['Alice', 'alice@example.com']
)
console.log(result.rowsAffected, result.lastInsertId)
```

### Read Operations

```typescript
type User = { id: number; name: string; email: string }

// Multiple rows
const users = await db.fetchAll<User[]>(
   'SELECT * FROM users WHERE email LIKE $1',
   ['%@example.com']
)

// Single row (returns undefined if not found, throws if multiple rows)
const user = await db.fetchOne<User>(
   'SELECT * FROM users WHERE id = $1',
   [42]
)
```

### Transactions

Execute multiple statements atomically:

```typescript
const results = await db.executeTransaction([
   ['UPDATE accounts SET balance = balance - $1 WHERE id = $2', [100, 1]],
   ['UPDATE accounts SET balance = balance + $1 WHERE id = $2', [100, 2]],
   ['INSERT INTO transfers (from_id, to_id, amount) VALUES ($1, $2, $3)', [1, 2, 100]]
])
```

Transactions use `BEGIN IMMEDIATE`, commit on success, and rollback on any failure.

### Error Handling

```typescript
import type { SqliteError } from '@silvermine/tauri-plugin-sqlite'

try {
   await db.execute('INSERT INTO users (id) VALUES ($1)', [1])
} catch (err) {
   const error = err as SqliteError
   console.error(error.code, error.message)
}
```

Common error codes:

   * `SQLITE_CONSTRAINT` - Constraint violation (unique, foreign key, etc.)
   * `SQLITE_NOTFOUND` - Table or column not found
   * `DATABASE_NOT_LOADED` - Database hasn't been loaded yet
   * `INVALID_PATH` - Invalid database path
   * `IO_ERROR` - File system error
   * `MIGRATION_ERROR` - Migration failed
   * `MULTIPLE_ROWS_RETURNED` - `fetchOne()` returned multiple rows

### Closing and Removing

```typescript
await db.close()           // Close this connection
await Database.closeAll()  // Close all connections
await db.remove()          // Close and DELETE database file(s) - irreversible!
```

## API Reference

### Static Methods

| Method | Description |
| ------ | ----------- |
| `Database.load(path, config?)` | Connect and return Database instance (or existing) |
| `Database.get(path)` | Get instance without connecting (lazy init) |
| `Database.closeAll()` | Close all database connections |

### Instance Methods

| Method | Description |
| ------ | ----------- |
| `execute(query, values?)` | Execute write query, returns `{ rowsAffected, lastInsertId }` |
| `executeTransaction(statements)` | Execute statements atomically |
| `fetchAll<T>(query, values?)` | Execute SELECT, return all rows |
| `fetchOne<T>(query, values?)` | Execute SELECT, return single row or `undefined` |
| `close()` | Close connection, returns `true` if was loaded |
| `remove()` | Close and delete database file(s), returns `true` if was loaded |

### Types

```typescript
interface WriteQueryResult {
   rowsAffected: number
   lastInsertId: number  // 0 for WITHOUT ROWID tables
}

interface CustomConfig {
   maxReadConnections?: number  // default: 6
   idleTimeoutSecs?: number     // default: 30
}

interface SqliteError {
   code: string
   message: string
}
```

## Tracing and Logging

The plugin uses [`tracing`](https://crates.io/crates/tracing) with
`release_max_level_off`, so **all logs are compiled out of release builds**.

To see logs during development:

```toml
[dependencies]
tracing = { version = "0.1.41", default-features = false, features = ["std", "release_max_level_off"] }
tracing-subscriber = { version = "0.3.20", features = ["fmt", "env-filter"] }
```

```rust
#[cfg(debug_assertions)]
fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("trace"));
    fmt().with_env_filter(filter).compact().init();
}

#[cfg(not(debug_assertions))]
fn init_tracing() {}

fn main() {
    init_tracing();
    tauri::Builder::default()
        .plugin(tauri_plugin_sqlite::init())
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
```

## Development

This project follows
[Silvermine standardization](https://github.com/silvermine/standardization) guidelines.

```bash
npm install              # Install dependencies
npm run build            # Build TypeScript bindings
cargo build              # Build Rust plugin
cargo test               # Run tests
npm run standards        # Lint and standards checks
```

## License

MIT

## Contributing

Contributions welcome! Follow the established coding standards and commit conventions.
