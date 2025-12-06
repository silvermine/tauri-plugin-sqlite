# Tauri Plugin SQLite

[![CI][ci-badge]][ci-url]

SQLite database interface for Tauri applications using
[sqlx](https://github.com/launchbadge/sqlx) and
[sqlx-sqlite-conn-mgr](crates/sqlx-sqlite-conn-mgr/README.md).

This plugin provides a SQLite-focused database interface with optimized connection
pooling, write serialization, and proper resource management.

[ci-badge]: https://github.com/silvermine/tauri-plugin-sqlite/actions/workflows/ci.yml/badge.svg
[ci-url]: https://github.com/silvermine/tauri-plugin-sqlite/actions/workflows/ci.yml

## Features

   * **Optimized Connection Pooling**: Separate read and write pools for concurrent reads,
     even while writing
   * **Write Serialization**: Exclusive write access through connection manager
   * **Migration Support**: Uses SQLx's database migration system (coming soon)
   * **Custom Configuration**: Configure read pool size and idle timeouts
   * **Type Safety**: Full TypeScript bindings
   * **Resource Management**: Proper cleanup on application exit (coming soon)

## Crates

### sqlx-sqlite-conn-mgr

A pure Rust module with no dependencies on Tauri or its plugin architecture. It
provides connection management for SQLite databases using SQLx. It's designed to be
published as a standalone crate in the future with minimal changes.

See [`crates/sqlx-sqlite-conn-mgr/README.md`](crates/sqlx-sqlite-conn-mgr/README.md)
for more details.

### Tauri Plugin

The main plugin provides a Tauri integration layer that exposes SQLite functionality
to Tauri applications. It uses the `sqlx-sqlite-conn-mgr` module internally.

## Getting Started

### Installation

1. Install NPM dependencies:

   ```bash
   npm install
   ```

2. Build the TypeScript bindings:

   ```bash
   npm run build
   ```

3. Build the Rust plugin:

   ```bash
   cargo build
   ```

### Tests

Run Rust tests:

```bash
cargo test
```

### Linting and Standards Checks

```bash
npm run standards
```

## Install

_This plugin requires a Rust version of at least **1.77.2**_

### Rust

Add the plugin to your `Cargo.toml`:

`src-tauri/Cargo.toml`

```toml
[dependencies]
tauri-plugin-sqlite = { git = "https://github.com/silvermine/tauri-plugin-sqlite" }
```

### JavaScript/TypeScript

Install the JavaScript bindings:

```sh
npm install @silvermine/tauri-plugin-sqlite
```

## Usage

### Basic Setup

Register the plugin in your Rust application:

`src-tauri/src/lib.rs`

```rust
fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_sqlite::Builder::new().build())
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
```

### Connecting to a Database

```typescript
import Database from '@silvermine/tauri-plugin-sqlite'

// Connect to a database (path is relative to app config directory)
const db = await Database.load('mydb.db')
```

> **Note:** Database paths are relative to the app's config directory. Unlike
> `tauri-plugin-sql`, no `sqlite:` prefix is needed.

### Parameter Binding and Types

All query methods (`execute`, `fetchAll`, `fetchOne`) support parameter binding using
the `$1`, `$2`, etc. syntax. Values must be of type `SqlValue`:

```typescript
type SqlValue = string | number | boolean | null | Uint8Array
```

Supported SQLite types:

   * **TEXT** - `string` values (also used for DATE, TIME, DATETIME)
   * **INTEGER** - `number` values (integers, preserved up to i64 range)
   * **REAL** - `number` values (floating point)
   * **BOOLEAN** - `boolean` values
   * **NULL** - `null` value
   * **BLOB** - `Uint8Array` for binary data

> **Note:** JavaScript's `number` type can safely represent integers up to
> ±2^53 - 1 (±9,007,199,254,740,991). The plugin preserves integer precision by
> binding integers as SQLite's INTEGER type (i64). For values within the i64
> range (-9,223,372,036,854,775,808 to 9,223,372,036,854,775,807), full precision
> is maintained. Values outside this range may lose precision.

```typescript
// Example with different types
await db.execute(
   'INSERT INTO data (text, int, real, bool, blob) VALUES ($1, $2, $3, $4, $5)',
   ['hello', 42, 3.14, true, new Uint8Array([1, 2, 3])]
)
```

### Executing Write Operations

Use `execute()` for INSERT, UPDATE, DELETE, or any query that modifies data:

```typescript
// CREATE TABLE
await db.execute(
   'CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)'
)

// INSERT
const result = await db.execute(
   'INSERT INTO users (name, email) VALUES ($1, $2)',
   ['Alice', 'alice@example.com']
)
console.log(`Inserted ${result.rowsAffected} rows`)
console.log(`Last insert ID: ${result.lastInsertId}`)

// UPDATE
const updateResult = await db.execute(
   'UPDATE users SET email = $1 WHERE name = $2',
   ['alice.new@example.com', 'Alice']
)
console.log(`Updated ${updateResult.rowsAffected} rows`)

// DELETE
const deleteResult = await db.execute(
   'DELETE FROM users WHERE id = $1',
   [1]
)
```

### Handling Errors

Handle database errors gracefully using structured error responses:

```typescript
import type { SqliteError } from '@silvermine/tauri-plugin-sqlite';

try {
   await db.execute(
      'INSERT INTO users (id, name) VALUES ($1, $2)',
      [1, 'Alice']
   );
} catch (err) {
   const error = err as SqliteError;

   // Check error code for specific handling
   if (error.code.startsWith('SQLITE_CONSTRAINT')) {
      console.error('Constraint violation:', error.message);
   } else if (error.code === 'DATABASE_NOT_LOADED') {
      console.error('Database not initialized');
   } else {
      console.error('Database error:', error.code, error.message);
   }
}
```

Common error codes include:

   * `SQLITE_CONSTRAINT` - Constraint violation (unique, foreign key, etc.)
   * `SQLITE_NOTFOUND` - Table or column not found
   * `DATABASE_NOT_LOADED` - Database hasn't been loaded yet
   * `INVALID_PATH` - Invalid database path
   * `IO_ERROR` - File system error
   * `MIGRATION_ERROR` - Migration failed
   * `MULTIPLE_ROWS_RETURNED` - `fetchOne()` query returned multiple rows

### Executing SELECT Queries

Use `fetchAll()` or `fetchOne()` for all read operations:

```typescript
type User = {id: number, name: string, email: string}

// SELECT all rows
const allUsers = await db.fetchAll<User[]>(
   'SELECT * FROM users'
)

// SELECT with parameters
const filtered = await db.fetchAll<User[]>(
   'SELECT * FROM users WHERE name = $1 AND email LIKE $2',
   ['Alice', '%@example.com']
)

// SELECT expecting single result (returns undefined if not found)
const user = await db.fetchOne<User>(
   'SELECT * FROM users WHERE id = $1',
   [42]
)

if (user) {
   console.log(`Found user: ${user.name}`)
}
```

> **Note:** `fetchOne()` validates that the query returns exactly 0 or 1 rows. If your
> query returns multiple rows, it will throw a `MULTIPLE_ROWS_RETURNED` error. This helps
> catch bugs where a query unexpectedly returns multiple results. Use `fetchAll()` if you
> expect multiple rows.

### Using Transactions

Execute multiple database operations atomically using `executeTransaction()`. All
statements either succeed together or fail together, maintaining data consistency:

```typescript
// Execute multiple inserts atomically
const results = await db.executeTransaction([
   ['INSERT INTO users (name, email) VALUES ($1, $2)', ['Alice', 'alice@example.com']],
   ['INSERT INTO audit_log (action, user) VALUES ($1, $2)', ['user_created', 'Alice']]
]);
console.log(`User ID: ${results[0].lastInsertId}`);
console.log(`Log rows affected: ${results[1].rowsAffected}`);

// Bank transfer example - all operations succeed or all fail
const results = await db.executeTransaction([
   ['UPDATE accounts SET balance = balance - $1 WHERE id = $2', [100, 1]],
   ['UPDATE accounts SET balance = balance + $1 WHERE id = $2', [100, 2]],
   ['INSERT INTO transfers (from_id, to_id, amount) VALUES ($1, $2, $3)', [1, 2, 100]]
]);
console.log(`Transfer ID: ${results[2].lastInsertId}`);
```

**How it works:**

   * Automatically executes `BEGIN IMMEDIATE` before running statements
   * Executes all statements in order
   * Commits with `COMMIT` if all statements succeed
   * Rolls back with `ROLLBACK` if any statement fails
   * The write connection is held for the entire transaction, ensuring atomicity
   * Errors are thrown after rollback, preserving the original error message

### Closing Connections

```typescript
// Close a specific database
await db.close()

// Close all database connections
await Database.closeAll()
```

### Removing a Database

Permanently delete a database and all its files (including WAL and SHM files):

```typescript
// ⚠️ Warning: This permanently deletes the database file(s)!
await db.remove()
```

## Migrations

> **Note:** Database migration support is a planned feature and will be added in a
> future release. It will be based on SQLx's migration framework.

## Query Parameter Binding

SQLite uses the `$1`, `$2`, etc. syntax for parameter binding:

```typescript
type User = {id: number, name: string, email: string, role: string, created_at: number}

// Multiple parameters
const result = await db.execute(
   'INSERT INTO users (name, email, role) VALUES ($1, $2, $3)',
   ['Bob', 'bob@example.com', 'admin']
)

// Parameters in WHERE clause
const filtered = await db.fetchAll<User[]>(
   'SELECT * FROM users WHERE role = $1 AND created_at > $2',
   ['admin', 1609459200]
)
```

> **Note:** Use `execute()` and `executeTransaction()` for write operations.
> For SELECT queries, use `fetchAll()` or `fetchOne()`.

### Architecture

The plugin uses `sqlx-sqlite-conn-mgr` for optimized connection management:

   * **Read Pool**: Multiple concurrent read-only connections (configurable, default: 6)
   * **Write Connection**: Single exclusive write connection
   * **WAL Mode**: Enabled automatically on first write operation
   * **Connection Caching**: Databases are cached by path
   * **Idle Timeout**: Connections close after inactivity (configurable, default: 30s)

### Read vs Write Operations

| Operation Type       | Method          | Pool Used        | Concurrency        |
| -------------------- | --------------- | ---------------- | ------------------ |
| SELECT (multiple)    | `fetchAll()`    | Read pool        | Multiple concurrent|
| SELECT (single)      | `fetchOne()`    | Read pool        | Multiple concurrent|
| INSERT/UPDATE/DELETE | `execute()`     | Write connection | Serialized         |
| CREATE TABLE         | `execute()`     | Write connection | Serialized         |
| CREATE INDEX         | `execute()`     | Write connection | Serialized         |

## API Reference

### Database Class

#### Static Methods

##### `Database.load(path: string, customConfig?: CustomConfig): Promise<Database>`

Connect to a database and return a Database instance.

   * `path`: Relative path to database file (from app config directory)
   * `customConfig`: Optional connection pool configuration
   * `Returns`: Promise resolving to Database instance

```typescript
const db = await Database.load('mydb.db', {
   maxReadConnections: 10, // defaults to 6 if no config is provided
   idleTimeoutSecs: 60     // defaults to 30 if no config is provided
})
```

##### `Database.get(path: string): Database`

Get a Database instance without connecting (lazy initialization).

```typescript
const db = Database.get('mydb.db')
// Connection happens on first query
```

##### `Database.closeAll(): Promise<void>`

Close all database connections.

```typescript
await Database.closeAll()
```

#### Instance Methods

##### `execute(query: string, bindValues?: unknown[]): Promise<WriteQueryResult>`

Execute a write query (INSERT, UPDATE, DELETE, CREATE, etc.).

```typescript
const result = await db.execute(
   'INSERT INTO users (name) VALUES ($1)',
   ['Alice']
)
console.log(result.rowsAffected, result.lastInsertId)
```

##### `fetchAll<T>(query: string, bindValues?: unknown[]): Promise<T>`

Execute a SELECT query and return all matching rows.

```typescript
const users = await db.fetchAll<User[]>(
   'SELECT * FROM users WHERE role = $1',
   ['admin']
)
```

##### `fetchOne<T>(query: string, bindValues?: unknown[]): Promise<T | undefined>`

Execute a SELECT query expecting zero or one result. Returns `undefined` if no rows match.

```typescript
const user = await db.fetchOne<User>(
   'SELECT * FROM users WHERE id = $1',
   [42]
)

if (user) {
   console.log(user.name)
} else {
   console.log('User not found')
}
```

##### `close(): Promise<boolean>`

Close this database connection. Returns `true` if the database was loaded and closed,
`false` if it wasn't loaded.

```typescript
await db.close()
```

##### `remove(): Promise<boolean>`

Close the connection and permanently delete database file(s). Returns `true` if
the database was loaded and removed, `false` if it wasn't loaded.

> ⚠️ **Warning:** This cannot be undone!

```typescript
await db.remove()
```

### TypeScript Interfaces

```typescript
interface WriteQueryResult {
   rowsAffected: number  // Number of rows modified
   lastInsertId: number  // ROWID of last inserted row (not set for WITHOUT ROWID tables, returns 0)
}

interface CustomConfig {
   maxReadConnections?: number
   idleTimeoutSecs?: number
}
```

## Thread Safety

All operations are async and thread-safe. The connection manager ensures:

   * ✓ Multiple concurrent readers
   * ✓ Only one writer at a time
   * ✓ No write conflicts
   * ✓ Automatic WAL mode for writers

## Permissions

By default, the plugin has restrictive permissions. Add permissions in
`src-tauri/capabilities/default.json`:

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

Or use the `default` permission set:

```json
{
   "permissions": ["sqlite:default"]
}
```

### Tracing and Logging

This plugin and its connection manager crate use the
[`tracing`](https://crates.io/crates/tracing) ecosystem for internal logging. They are
configured with the `release_max_level_off` feature so that **all log statements are
compiled out of release builds**. This guarantees that logging from this plugin will never
reach production binaries unless you explicitly change that configuration.

To see logs during development, initialize a `tracing-subscriber` in your Tauri
application crate and keep it behind a `debug_assertions` guard, for example:

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

With this setup, `tauri dev` shows all plugin and app logs, while `tauri build` produces
a release binary that contains no logging from this plugin or your app-level `tracing`
calls.

## Development Standards

This project follows the
[Silvermine standardization](https://github.com/silvermine/standardization)
guidelines. Key standards include:

   * **EditorConfig**: Consistent editor settings across the team
   * **Markdownlint**: Markdown linting for documentation
   * **Commitlint**: Conventional commit message format
   * **Code Style**: 3-space indentation, LF line endings

### Running Standards Checks

```bash
npm run standards
```

## License

MIT

## Contributing

Contributions are welcome! Please follow the established coding standards and commit
message conventions.
