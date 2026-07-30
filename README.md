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

     > From [SQLite docs](https://sqlite.org/whentouse.html):
     > "_SQLite ... will only allow one writer at any instant in time._"
   * **WAL Mode**: Enabled automatically on first write operation
   * **Type Safety**: Full TypeScript bindings
   * **Migration Support**: SQLx's migration framework
   * **Resource Management**: Proper cleanup on application exit
   * **Optional Change Notifications**: SQLite hooks for reactive change notifications

## Architecture

The plugin is built from three standalone Rust crates, each usable independently
without Tauri:

```text
┌───────────────────────────────────────────────────────────────┐
│                   tauri-plugin-sqlite (src/)                  │
│         Tauri commands, state management, permissions         │
├───────────────────────────────────────────────────────────────┤
│                   sqlx-sqlite-toolkit (crate)                 │
│            DatabaseWrapper, builders, transactions            │
│          JSON decoding, optional observer integration         │
├───────────────────────────────────────────────────────────────┤
│  sqlx-sqlite-conn-mgr (crate) │  sqlx-sqlite-observer (crate) │
│  Connection pools,            │  Change notifications         │
│  single writer,               │  via SQLite hooks             │
│  WAL mode, attached           │  broadcast streams            │
│  databases                    │  (optional)                   │
└───────────────────────────────┴───────────────────────────────┘
```

   * **[`sqlx-sqlite-conn-mgr`](crates/sqlx-sqlite-conn-mgr/)** — Low-level connection
     management: read pool, exclusive writer, WAL mode, attached databases
   * **[`sqlx-sqlite-observer`](crates/sqlx-sqlite-observer/)** — Reactive change
     notifications using SQLite's native preupdate/commit/rollback hooks
   * **[`sqlx-sqlite-toolkit`](crates/sqlx-sqlite-toolkit/)** — High-level API:
     `DatabaseWrapper`, builder-pattern queries, interruptible transactions, JSON
     type decoding. Optionally integrates the observer behind a feature flag.
   * **`tauri-plugin-sqlite` (this package)** — Thin Tauri layer: IPC commands, path
     resolution, state management, permissions

### Query Routing

| Operation Type       | Method          | Pool Used        | Concurrency         |
| -------------------- | --------------- | ---------------- | ------------------- |
| SELECT (multiple)    | `fetchAll()`    | Read pool        | Multiple concurrent |
| SELECT (single)      | `fetchOne()`    | Read pool        | Multiple concurrent |
| SELECT (paginated)   | `fetchPage()`   | Read pool        | Multiple concurrent |
| INSERT/UPDATE/DELETE | `execute()`     | Write connection | Serialized          |
| DDL (CREATE, etc.)   | `execute()`     | Write connection | Serialized          |

See individual crate READMEs for detailed API documentation.

## Installation

_Requires Rust **1.94.0** or later_

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
      "sqlite:allow-fetch-one",
      "sqlite:allow-fetch-all",
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
      .plugin(tauri_plugin_sqlite::Builder::new().build().expect("failed to build sqlite plugin"))
      .run(tauri::generate_context!())
      .expect("error while running tauri application");
}
```

### Registering Databases

Every database must be **registered** on the Rust side before it can be opened.
Registration assigns a stable **key** (for example `"MAIN"`) to a filesystem path or
in-memory URI. The frontend and Rust callers open databases by **key**, not by path.

**Registration rules** (enforced when you call `register_database`):

   * File paths must be **absolute**, with no `..` components or null bytes
   * File paths are **canonicalized** once at registration (symlink-safe when the path or
     parent exists)
   * In-memory URIs (`:memory:`, `file::memory:*`, and `file:` URIs with an exact
     `mode=memory` query parameter) are accepted as-is
   * Each registration **key** must map to a **distinct** database path; two keys for the
     same path fail with `INVALID_CONFIG` when calling `build()` or during plugin setup
     after `on_setup` merges registrations

Invalid registration paths fail at startup with `INVALID_PATH` or `PATH_TRAVERSAL`.

**Why keys:**

1) `Database.load()` is callable from the frontend over IPC. The frontend sends
   only a registration key; unregistered keys are rejected with `PATH_NOT_REGISTERED`.
   This prevents untrusted frontend code from opening arbitrary files on disk.

2) Keys avoid cross-language path string mismatches. With path-at-load, TS and Rust would
   need identical canonical path strings on every open (slashes, symlinks, etc.). Keys
   resolve the path once at registration; all later opens use the plain string key.

3) Without registration keys, every call site would repeat that path discovery or keep its
   own `PathBuf`. Registration stores the key-to-path mapping once; `connect` reuses the
   key so callers do not supply a filesystem path on every open.
   On mobile, path discovery is not a cheap string join. Resolvers such as
   [tauri-plugin-fs-resolver](https://github.com/silvermine/tauri-plugin-fs-resolver)
   call platform-native APIs so paths match OS sandbox rules. On Android that means a
   JNI call into Kotlin `Context` (e.g. `getFilesDir()`) on each resolve — noticeably
   more expensive than a local HashMap lookup, and a different kind of boundary than
   TypeScript-to-Rust IPC (in-process JNI vs webview bridge). Register the resolved
   `PathBuf` once in `on_setup`; every later `connect(database_key)` only looks up that
   key in [`RegisteredDatabases`] — no repeat native or JNI work.

Because legitimate paths usually depend on runtime values (app data directory, platform
path resolvers, etc.), registration normally happens in the `on_setup` hook:

```rust
use tauri_plugin_sqlite::Builder;
use tauri::Manager;

const MAIN_DB_KEY: &str = "MAIN";

fn main() {
   tauri::Builder::default()
      .plugin(
         Builder::new()
            .on_setup(|app, reg| {
               let db = app.path().app_data_dir()?.join("main.db");
               reg.register_database(MAIN_DB_KEY, db, None)?;
               Ok(())
            })
            .build()
            .expect("failed to build sqlite plugin")
      )
      .run(tauri::generate_context!())
      .expect("error while running tauri application");
}
```

A static registration (compile-time path) can be registered on the builder directly:

```rust
use tauri_plugin_sqlite::Builder;
use std::path::PathBuf;

const MAIN_DB_KEY: &str = "MAIN";

# fn main() -> tauri_plugin_sqlite::Result<()> {
let _plugin = Builder::new()
   .register_database(MAIN_DB_KEY, PathBuf::from("/var/lib/myapp/main.db"), None)?
   .build()?;
# Ok(())
# }
```

The frontend then calls `Database.load(MAIN_DB_KEY)` (see [Connecting](#connecting)).

### Migrations

This plugin uses [SQLx's migration system][sqlx-migrate]. Create numbered `.sql`
files in a migrations directory:

[sqlx-migrate]: https://docs.rs/sqlx/latest/sqlx/macro.migrate.html

```text
src-tauri/migrations/
├── 0001_create_users.sql
├── 0002_add_email_column.sql
└── 0003_create_posts.sql
```

Register migrations using SQLx's `migrate!()` macro, which embeds them at compile time.
Pass the migrator as the third argument to `register_database`. The `on_setup` hook is the
usual place to register app-derived paths:

```rust
use tauri_plugin_sqlite::Builder;
use tauri::Manager;

const MAIN_DB_KEY: &str = "MAIN";

fn main() {
   tauri::Builder::default()
      .plugin(
         Builder::new()
            .on_setup(|app, reg| {
               let db = app.path().app_data_dir()?.join("main.db");
               reg.register_database(
                  MAIN_DB_KEY,
                  db,
                  Some(sqlx::migrate!("./migrations")),
               )?;
               Ok(())
            })
            .build()
            .expect("failed to build sqlite plugin")
      )
      .run(tauri::generate_context!())
      .expect("error while running tauri application");
}
```

The frontend must call `Database.load()` with the same **registration key** so migrations
are awaited correctly.

**Timing:** Migrations start automatically at plugin setup (non-blocking). When
TypeScript calls `Database.load()`, it waits for migrations to complete before
returning. If migrations fail, `load()` returns an error. Applied migrations are
tracked in `_sqlx_migrations` — re-running is safe and idempotent.

#### Retrieving Migration Events

Use `getMigrationEvents()` to retrieve cached events:

```typescript
import Database from '@silvermine/tauri-plugin-sqlite';

const MAIN_DB_KEY = 'MAIN';

// Same registration key used in Rust `register_database`
const db = await Database.load(MAIN_DB_KEY);

// Get all migration events (including ones emitted before listener could be registered)
const events = await db.getMigrationEvents();
for (const event of events) {
   console.info(`${event.status}: ${event.dbKey} (${event.dbPath})`);
   if (event.status === 'failed') {
      console.error(`Migration error: ${event.error}`);
   }
}
```

**Optional:** Listen for real-time events, globally. May miss early events due the Rust
layer completing some or all migrations before the frontend subscription initializes.

```typescript
import { listen } from '@tauri-apps/api/event';
import type { MigrationEvent } from '@silvermine/tauri-plugin-sqlite';

await listen<MigrationEvent>('sqlite:migration', (event) => {
   const { dbKey, dbPath, status, migrationCount, error } = event.payload;
   console.info(`Migration ${status} for ${dbKey} (${dbPath}): ${migrationCount} migrations`, error);
});
```

### Connecting

Pass the **registration key** from Rust `register_database` (see
[Registering Databases](#registering-databases)).

```typescript
import Database from '@silvermine/tauri-plugin-sqlite';

const MAIN_DB_KEY = 'MAIN';

// Connect (no sqlite: prefix needed)
let db = await Database.load(MAIN_DB_KEY);

// With custom configuration
db = await Database.load(MAIN_DB_KEY, {
   maxReadConnections: 10, // default: 6
   idleTimeoutSecs: 60     // default: 30
});

// Lazy initialization (connects on first query; sync — no await)
db = Database.get(MAIN_DB_KEY);

// In-memory: register first, then load by key
// reg.register_database('MEM', ':memory:', None)? in on_setup
const mem = await Database.load('MEM');
```

An unregistered key throws `PATH_NOT_REGISTERED`. Invalid paths are rejected at
registration time on the Rust side (`INVALID_PATH`, `PATH_TRAVERSAL`).

### Parameter Binding

All query methods use `$1`, `$2`, etc. syntax with `SqlValue` types:

```typescript
type SqlValue = string | number | boolean | null | Uint8Array;
```

| SQLite Type | TypeScript Type | Notes                               |
| ----------- | --------------- | ----------------------------------- |
| TEXT        | `string`        | Also for DATE, TIME, DATETIME       |
| INTEGER     | `number`        | Integers preserved up to i64 range  |
| REAL        | `number`        | Floating point                      |
| BOOLEAN     | `boolean`       |                                     |
| NULL        | `null`          |                                     |
| BLOB        | `Uint8Array`    | Binary data                         |

> **Note:** JavaScript safely represents integers up to ±2^53 - 1. The plugin binds
> integers as SQLite's INTEGER type (i64), maintaining full precision within that range.

### Write Operations

Use `execute()` for INSERT, UPDATE, DELETE, CREATE, etc.:

```typescript
await db.execute(
   'CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)'
);

const result = await db.execute(
   'INSERT INTO users (name, email) VALUES ($1, $2)',
   ['Alice', 'alice@example.com']
);
console.info(`Inserted ${result.rowsAffected} row(s), ID: ${result.lastInsertId}`);
```

### Read Operations

```typescript
type User = { id: number; name: string; email: string };

// Multiple rows
const users = await db.fetchAll<User[]>(
   'SELECT * FROM users WHERE email LIKE $1',
   ['%@example.com']
);
console.info(`Found ${users.length} users`);

// Single row (returns undefined if not found, throws if multiple rows)
const user = await db.fetchOne<User>(
   'SELECT * FROM users WHERE id = $1',
   [42]
);
if (user) {
   console.info(`Found user: ${user.name}`);
}
```

### Pagination

When working with large result sets, loading all rows at once can cause
performance degradation and excessive memory usage on both the Rust and
TypeScript sides. The plugin provides built-in pagination to fetch data in
fixed-size pages, keeping memory usage bounded and queries fast regardless
of total row count.

#### Why Keyset Pagination

The plugin uses keyset (cursor-based) pagination rather than traditional
OFFSET-based pagination. With OFFSET, the database must scan and discard
all skipped rows on every page request, making deeper pages progressively
slower. Keyset pagination uses indexed column values from the last row of
the current page to seek directly to the next page, keeping query time
constant no matter how far you paginate.

```typescript
import type { KeysetColumn } from '@silvermine/tauri-plugin-sqlite';

type Post = { id: number; title: string; category: string; score: number };

const keyset: KeysetColumn[] = [
   { name: 'category', direction: 'asc' },
   { name: 'score', direction: 'desc' },
   { name: 'id', direction: 'asc' },
];

// First page
const page = await db.fetchPage<Post>(
   'SELECT id, title, category, score FROM posts',
   [],
   keyset,
   25,
);

// Next page (forward) — pass the cursor from the previous page
if (page.nextCursor) {
   const nextPage = await db.fetchPage<Post>(
      'SELECT id, title, category, score FROM posts',
      [],
      keyset,
      25,
   ).after(page.nextCursor);

   // Previous page (backward) — rows are returned in original sort order
   const prevPage = await db.fetchPage<Post>(
      'SELECT id, title, category, score FROM posts',
      [],
      keyset,
      25,
   ).before(page.nextCursor);
}
```

The base query must not contain `ORDER BY` or `LIMIT` clauses — the builder
appends these automatically based on the keyset definition.

### Transactions

For most cases, use `executeTransaction()` to run multiple statements atomically:

```typescript
const results = await db.executeTransaction([
   ['UPDATE accounts SET balance = balance - $1 WHERE id = $2', [100, 1]],
   ['UPDATE accounts SET balance = balance + $1 WHERE id = $2', [100, 2]],
   ['INSERT INTO transfers (from_id, to_id, amount) VALUES ($1, $2, $3)', [1, 2, 100]]
]);
console.info(`Transaction completed: ${results.length} statements executed`);
```

Transactions use `BEGIN IMMEDIATE`, commit on success, and rollback on any failure.

#### Interruptible Transactions

**Use interruptible transactions when you need to read data mid-transaction to
decide how to proceed.** For example, inserting a record, reading back its
generated ID or other computed values, then using that data in subsequent writes.

```typescript
// Assuming userId, productId, itemTotal are defined in your application context
const userId = 123;
const productId = 456;
const itemTotal = 99.99;

// Begin transaction with initial insert
let tx = await db.beginInterruptibleTransaction([
   ['INSERT INTO orders (user_id, total) VALUES ($1, $2)', [userId, 0]]
]);

// Read the uncommitted data to get the generated order ID
const orders = await tx.read<Array<{ id: number }>>(
   'SELECT id FROM orders WHERE user_id = $1 ORDER BY id DESC LIMIT 1',
   [userId]
);
const orderId = orders[0].id;

// Continue transaction with the order ID
tx = await tx.continueWith([
   ['INSERT INTO order_items (order_id, product_id) VALUES ($1, $2)', [orderId, productId]],
   ['UPDATE orders SET total = $1 WHERE id = $2', [itemTotal, orderId]]
]);

// Commit the transaction
await tx.commit();
```

**Important:**

   * Only one interruptible transaction can be active per database at a time
   * The write lock is held for the entire duration - keep transactions short
   * Uncommitted writes are visible only within the transaction's `read()` method
   * If the transaction handle is dropped without calling `commit()` or
     `rollback()`, the transaction is automatically rolled back and the write
     connection is released back to the pool. This also happens on app exit
     and on transaction timeout.

To rollback instead of committing:

```typescript
await tx.rollback();
```

### Cross-Database Queries

Attach other SQLite databases to run queries across multiple database files.
Each attached database gets a schema name that acts as a namespace for its
tables.

**Builder Pattern:** All query methods (`execute`, `executeTransaction`,
`fetchAll`, `fetchOne`, `fetchPage`) return builders that support `.attach()`
for cross-database operations. Each attached database must already be loaded and is
identified by its **registration key** (see
[Registering Databases](#registering-databases)).

```typescript
const ORDERS_DB_KEY = 'ORDERS';

// Join data from multiple databases
const results = await db.fetchAll(
   'SELECT u.name, o.total FROM users u JOIN orders.orders o ON u.id = o.user_id',
   []
).attach([
   {
      databaseKey: ORDERS_DB_KEY,
      schemaName: 'orders',
      mode: 'readOnly'
   }
]);
console.info(`Found ${results.length} results from cross-database query`);

// Update main database using data from attached database
await db.execute(
   'UPDATE todos SET status = $1 WHERE id IN (SELECT todo_id FROM archive.completed)',
   ['archived']
).attach([
   {
      databaseKey: 'ARCHIVE',
      schemaName: 'archive',
      mode: 'readOnly'
   }
]);

// Atomic writes across multiple databases
const userId = 123;
const total = 99.99;

await db.executeTransaction([
   ['INSERT INTO main.orders (user_id, total) VALUES ($1, $2)', [userId, total]],
   ['UPDATE stats.order_count SET count = count + 1', []]
]).attach([
   {
      databaseKey: 'STATS',
      schemaName: 'stats',
      mode: 'readWrite'
   }
]);
```

**Attached Database Modes:**

   * `readOnly` - Read-only access (can be used with read or write operations)
   * `readWrite` - Read-write access (requires write operation, holds write
     lock)

**Important:**

   * Attached database(s) automatically detached after query completion
   * Read-write attachments acquire write locks on all involved databases
   * Attachments are connection-scoped and don't persist across queries
   * Main database is always accessible without a schema prefix

### Change Notifications

Subscribe to real-time change notifications when rows are inserted, updated, or
deleted. Changes are only published after transactions commit — you never see
partial or rolled-back data.

```typescript
// 1. Enable observation for specific tables
await db.observe(['users', 'posts']);

// 2. Subscribe to changes
const subscription = await db.subscribe(['users'], (event) => {
   if (event.event === 'change') {
      const { table, operation, primaryKey, newValues, oldValues } = event.data;

      console.info(`${operation} on ${table}, row key:`, primaryKey);

      if (operation === 'insert' || operation === 'update') {
         console.info('New values:', newValues);
      }
      if (operation === 'update' || operation === 'delete') {
         console.info('Old values:', oldValues);
      }
   } else if (event.event === 'lagged') {
      // Consumer fell behind — some notifications were missed
      console.warn(`Missed ${event.data.count} notifications`);
   }
});

// 3. Changes are now streamed to the callback
await db.execute('INSERT INTO users (name) VALUES ($1)', ['Alice']);
// callback fires: { event: 'change', data: { table: 'users', operation: 'insert', ... } }

// 4. Unsubscribe when done
await subscription.unsubscribe();

// 5. Release this window's observation registration. Observation is
//    reference-counted per window: this only fully disables tracking and
//    aborts subscriptions once every window that called observe() for this
//    database has released via unobserve().
await db.unobserve();
```

**Configuration:**

```typescript
await db.observe(['users'], {
   channelCapacity: 512,  // default: 256 — at least the number of writes in your largest transaction
   captureValues: false,  // default: true — disable to reduce memory per notification
});
```

**Important:**

   * **Every window must call `observe()` itself before `subscribe()`** —
     registration is per window and is not shared, even though the broker is.
     `subscribe()` enforces this, returning `OBSERVATION_NOT_ENABLED` when _this_
     window is not a registered observer
   * `observe()` is additive and reference-counted: calling it again merges in the
     requested tables rather than replacing anything, so existing subscriptions
     keep working. `unobserve()` releases only this window's registration; tracking
     stops and subscriptions abort once every registered window has released
   * **Registration is keyed per _webview_, not per caller** — two modules in the
     same window share one registration, so whichever calls `unobserve()` first
     tears down the other's subscriptions. Treat `observe()`/`unobserve()` as owned
     by a single module per window. Labels also survive a page reload without
     clearing the registration, so a reloaded window can pass `subscribe()`'s check
     without re-observing
   * `channelCapacity`/`captureValues` are fixed by the _first_ window to observe a
     database, since the broadcast channel behind them cannot be resized without
     dropping subscribers. Omitting either field inherits the active value; an
     explicit _different_ value fails with `OBSERVATION_CONFIG_CONFLICT`. To change
     them, every window must `unobserve()` first
   * Multiple subscriptions can be active on the same database, each filtering by
     different tables
   * `lagged` events indicate the broadcast channel filled up before the
     subscriber could read — increase `channelCapacity`
   * Column values (`oldValues`, `newValues`) are typed as `ColumnValue` — a tagged
     union of `null`, `integer`, `real`, `text`, or `blob` (base64-encoded)

### Error Handling

```typescript
import type { SqliteError } from '@silvermine/tauri-plugin-sqlite';

try {
   await db.execute('INSERT INTO users (id) VALUES ($1)', [1]);
} catch (err) {
   const error = err as SqliteError;
   console.error(error.code, error.message);
}
```

Common error codes:

   * `SQLITE_CONSTRAINT` - Constraint violation (unique, foreign key, etc.)
   * `SQLITE_NOTFOUND` - Table or column not found
   * `DATABASE_NOT_LOADED` - Database hasn't been loaded yet
   * `INVALID_PATH` - Invalid path at registration (relative or failed canonicalization)
   * `PATH_NOT_REGISTERED` - Registration key not found
   * `PATH_TRAVERSAL` - Registration path contains `..` or null bytes
   * `IO_ERROR` - File system error
   * `MIGRATION_ERROR` - Migration failed
   * `MULTIPLE_ROWS_RETURNED` - `fetchOne()` returned multiple rows
   * `OBSERVATION_NOT_ENABLED` - Called `subscribe()` before this window called
     `observe()`
   * `OBSERVATION_CONFIG_CONFLICT` - Called `observe()` with a
     `channelCapacity`/`captureValues` differing from the active one
   * `OBSERVER_ERROR` - Error from the observer subsystem

### Closing and Removing

```typescript
await db.close();            // Close this connection
await Database.close_all();  // Close all connections
await db.remove();           // Close and DELETE database file(s) - irreversible!
```

## API Reference

### Static Methods

| Method | Description |
| ------ | ----------- |
| `Database.load(dbKey, config?)` | Connect eagerly and return Database instance (or existing) |
| `Database.get(dbKey)` | Sync handle; connects on first query (no `customConfig`) |
| `Database.close_all()` | Close all database connections |

### Instance Methods

| Method | Description |
| ------ | ----------- |
| `execute(query, values?)` | Execute write query, returns `{ rowsAffected, lastInsertId }` |
| `executeTransaction(statements)` | Execute statements atomically (use for batch writes) |
| `beginInterruptibleTransaction(statements)` | Begin interruptible transaction, returns `InterruptibleTransaction` |
| `fetchAll<T>(query, values?)` | Execute SELECT, return all rows |
| `fetchOne<T>(query, values?)` | Execute SELECT, return single row or `undefined` |
| `fetchPage<T>(query, values, keyset, pageSize)` | Keyset pagination, returns `FetchPageBuilder` |
| `close()` | Close connection, returns `true` if was loaded |
| `remove()` | Close and delete database file(s), returns `true` if was loaded |
| `observe(tables, config?)` | Enable change observation for tables |
| `subscribe(tables, onEvent)` | Subscribe to change notifications, returns `Subscription` |
| `unobserve()` | Release this window's observation registration; only fully disables observation and aborts subscriptions once every registered window has released |

### Builder Methods

All query methods (`execute`, `executeTransaction`, `fetchAll`, `fetchOne`,
`fetchPage`) return builders that are directly awaitable and support method
chaining:

| Method | Description |
| ------ | ----------- |
| `attach(specs)` | Attach databases for cross-database queries, returns `this` |
| `after(cursor)` | Set cursor for forward pagination (`FetchPageBuilder` only), returns `this` |
| `before(cursor)` | Set cursor for backward pagination (`FetchPageBuilder` only), returns `this` |
| `await builder` | Execute the query (builders implement `PromiseLike`) |

### InterruptibleTransaction Methods

| Method | Description |
| ------ | ----------- |
| `read<T>(query, values?)` | Read uncommitted data within this transaction |
| `continueWith(statements)` | Execute additional statements, returns new `InterruptibleTransaction` |
| `commit()` | Commit transaction and release write lock |
| `rollback()` | Rollback transaction and release write lock |

### Subscription Methods

| Method | Description |
| ------ | ----------- |
| `unsubscribe()` | Stop receiving change notifications, returns `true` if was active |

### Types

```typescript
interface WriteQueryResult {
   rowsAffected: number;
   lastInsertId: number;  // 0 for WITHOUT ROWID tables
}

interface CustomConfig {
   maxReadConnections?: number;  // default: 6
   idleTimeoutSecs?: number;     // default: 30
}

interface AttachedDatabaseSpec {
   databaseKey: string;  // Registration key of a database already loaded via load()
   schemaName: string;    // Schema name for accessing tables (e.g., 'orders')
   mode: 'readOnly' | 'readWrite';
}

interface SqliteError {
   code: string;
   message: string;
}

interface ObserverConfig {
   channelCapacity?: number;  // default: 256
   captureValues?: boolean;   // default: true
}

type SortDirection = 'asc' | 'desc';

interface KeysetColumn {
   name: string;       // Column name in the query result set
   direction: SortDirection;
}

interface KeysetPage<T = Record<string, SqlValue>> {
   rows: T[];
   nextCursor: SqlValue[] | null;  // Cursor to continue pagination, null when no more pages
   hasMore: boolean;
}

type ChangeOperation = 'insert' | 'update' | 'delete';

type ColumnValue =
   | { type: 'null' }
   | { type: 'integer'; value: number }
   | { type: 'real'; value: number }
   | { type: 'text'; value: string }
   | { type: 'blob'; value: string };  // base64-encoded

interface TableChange {
   table: string;
   operation?: ChangeOperation;
   rowid?: number;
   primaryKey: ColumnValue[];
   oldValues?: ColumnValue[];   // update, delete
   newValues?: ColumnValue[];   // insert, update
}

type TableChangeEvent =
   | { event: 'change'; data: TableChange }
   | { event: 'lagged'; data: { count: number } };
```

## Rust-Only API

For Rust code in a Tauri app, register databases first, then open by key using the
[`Connection`](src/lib.rs) trait on `AppHandle`. This uses the same open path as the
frontend `load` command (`connect_to_database`).

For standalone Rust projects without the plugin, use `DatabaseWrapper::connect(path)`
from [`sqlx-sqlite-toolkit`](crates/sqlx-sqlite-toolkit/) directly (no registration).

### Setup (Tauri plugin)

```rust
use tauri::{Manager, Runtime};
use tauri_plugin_sqlite::{Builder, Connection, SqliteDatabaseConfig};

const MAIN_DB_KEY: &str = "MAIN";

// In lib.rs setup — register key + path
Builder::new()
   .on_setup(|app, reg| {
      let db = app.path().app_data_dir()?.join("main.db");
      reg.register_database(MAIN_DB_KEY, db, None)?;
      Ok(())
   })
   .build()?;

// Anywhere with AppHandle
async fn example<R: Runtime>(app: tauri::AppHandle<R>) -> tauri_plugin_sqlite::Result<()> {
   let db = app.connect(MAIN_DB_KEY).await?;

   let db = app.connect_with_config(
      MAIN_DB_KEY,
      SqliteDatabaseConfig {
         max_read_connections: 10,
         idle_timeout_secs: 60,
      },
   ).await?;

   Ok(())
}
```

### Basic Operations

```rust
use serde_json::json;

// Write operations
let result = db.execute(
   "INSERT INTO users (name, email) VALUES (?, ?)".into(),
   vec![json!("Alice"), json!("alice@example.com")]
).await?;

println!("Inserted row {}", result.last_insert_id);

// Read multiple rows
let users = db.fetch_all(
   "SELECT * FROM users WHERE active = ?".into(),
   vec![json!(true)]
).await?;

println!("Found {} users", users.len());

// Read single row
let user = db.fetch_one(
   "SELECT * FROM users WHERE id = ?".into(),
   vec![json!(42)]
).await?;

if let Some(user_data) = user {
   println!("Found user: {:?}", user_data);
}
```

### Pagination (Rust)

See [Pagination](#pagination) above for background on why the plugin uses
keyset pagination. The Rust API works the same way via `fetch_page`:

```rust
use sqlx_sqlite_toolkit::pagination::KeysetColumn;

let keyset = vec![
   KeysetColumn::asc("category"),
   KeysetColumn::desc("score"),
   KeysetColumn::asc("id"),
];

// First page
let page = db.fetch_page(
   "SELECT id, title, category, score FROM posts".into(),
   vec![],
   keyset.clone(),
   25,
).await?;

// Next page (forward)
if let Some(cursor) = page.next_cursor {
   let next = db.fetch_page(
      "SELECT id, title, category, score FROM posts".into(),
      vec![],
      keyset.clone(),
      25,
   ).after(cursor.clone()).await?;

   // Previous page (backward) — rows returned in original sort order
   let prev = db.fetch_page(
      "SELECT id, title, category, score FROM posts".into(),
      vec![],
      keyset,
      25,
   ).before(cursor).await?;
}
```

### Simple Transactions

Use `execute_transaction()` for atomic execution of multiple statements:

```rust
let results = db.execute_transaction(vec![
   ("UPDATE accounts SET balance = balance - ? WHERE id = ?", vec![json!(100), json!(1)]),
   ("UPDATE accounts SET balance = balance + ? WHERE id = ?", vec![json!(100), json!(2)]),
   ("INSERT INTO transfers (from_id, to_id, amount) VALUES (?, ?, ?)", vec![json!(1), json!(2), json!(100)]),
]).await?;

println!("Transaction completed: {} statements executed", results.len());

// Returns Vec<WriteQueryResult> on success, rolls back on any failure
```

### Interruptible Transactions (Rust)

For transactions that need to read data mid-transaction:

If `tx` is dropped without calling `commit()` or `rollback()` — including via
an early return from a `?` operator — the transaction is automatically rolled
back and the write connection is released back to the pool.

```rust
// Assuming user_id, product_id, item_total are defined in your application context
let user_id = 123;
let product_id = 456;
let item_total = 99.99;

// Begin transaction with initial statements
let mut tx = db.begin_interruptible_transaction()
   .execute(vec![
      ("INSERT INTO orders (user_id, total) VALUES (?, ?)", vec![json!(user_id), json!(0)]),
   ])
   .await?;

// Read uncommitted data
let orders = tx.read(
   "SELECT id FROM orders WHERE user_id = ? ORDER BY id DESC LIMIT 1".into(),
   vec![json!(user_id)]
).await?;

let order_id = orders[0].get("id").unwrap().as_i64().unwrap();

// Continue with more statements
tx.continue_with(vec![
   ("INSERT INTO order_items (order_id, product_id) VALUES (?, ?)", vec![json!(order_id), json!(product_id)]),
   ("UPDATE orders SET total = ? WHERE id = ?", vec![json!(item_total), json!(order_id)]),
]).await?;

// Commit (or rollback)
tx.commit().await?;
// tx.rollback().await?;  // Alternative: rollback changes
```

### Cross-Database Operations

Attach other databases for cross-database queries. Load each database by registration
key first (`app.connect("STATS").await?`), then create `AttachedSpec` instances using
their inner database references:

```rust
use tauri_plugin_sqlite::{Connection, AttachedSpec, AttachedMode};
use std::sync::Arc;

// After registering and connecting both databases by key
let main_db = app.connect("MAIN").await?;
let stats_db = app.connect("STATS").await?;

// Create attached spec using the inner database reference
let stats_spec = AttachedSpec {
   database: Arc::clone(stats_db.inner()),
   schema_name: "stats".to_string(),
   mode: AttachedMode::ReadWrite,
};

// Simple transaction with attached database
let results = main_db.execute_transaction(vec![
   ("INSERT INTO main.orders (user_id) VALUES (?)", vec![json!(1)]),
   ("UPDATE stats.order_count SET count = count + 1", vec![]),
])
.attach(vec![stats_spec])
.await?;
println!("Cross-database transaction completed: {} statements", results.len());

// Interruptible transaction with attached database
let inventory_db = app.connect("INVENTORY").await?;

// Create spec for inventory database
let inv_spec = AttachedSpec {
   database: Arc::clone(inventory_db.inner()),
   schema_name: "inv".to_string(),
   mode: AttachedMode::ReadWrite,
};

// Assuming product_id is defined in your application context
let product_id = 789;

let _tx = main_db.begin_interruptible_transaction()
   .attach(vec![inv_spec])
   .execute(vec![
      ("UPDATE inv.stock SET quantity = quantity - ? WHERE product_id = ?", vec![json!(1), json!(product_id)]),
   ])
   .await?;
// Continue with transaction operations...
```

### Cleanup

```rust
db.close().await?;   // Close connection
db.remove().await?;  // Close and DELETE database file(s)
```

### Rust API Reference

#### DatabaseWrapper Methods

| Method | Description |
| ------ | ----------- |
| `connect(path, config?)` | Open database by path, returns `DatabaseWrapper` |
| `execute(query, values)` | Execute write query |
| `execute_transaction(statements)` | Execute statements atomically (builder) |
| `begin_interruptible_transaction()` | Begin interruptible transaction (builder) |
| `fetch_all(query, values)` | Fetch all rows |
| `fetch_one(query, values)` | Fetch single row |
| `fetch_page(query, values, keyset, page_size)` | Keyset pagination (builder, supports `.after()`, `.before()`, `.attach()`) |
| `close()` | Close connection |
| `remove()` | Close and delete database file(s) |

#### InterruptibleTransaction Methods (Rust)

| Method | Description |
| ------ | ----------- |
| `read(query, values)` | Read uncommitted data within transaction |
| `continue_with(statements)` | Execute additional statements |
| `commit()` | Commit and release write lock |
| `rollback()` | Rollback and release write lock |

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
      .plugin(tauri_plugin_sqlite::Builder::new().build().expect("failed to build sqlite plugin"))
      .run(tauri::generate_context!())
      .expect("error while running tauri application");
}
```

## Examples

Working Tauri demo apps are in the [`examples/`](examples) directory:

   * **[`observer-demo`](examples/observer-demo)** — Real-time change
     notifications with live streaming of inserts, updates, and deletes
   * **[`pagination-demo`](examples/pagination-demo)** — Keyset pagination
     with a virtualized list and performance metrics

See the [toolkit crate README](crates/sqlx-sqlite-toolkit/README.md#examples)
for setup instructions.

## Security Considerations

### Cross-Window Shared State

Database instances are shared across all webviews/windows within the same Tauri
application. A database loaded in one window is accessible from any other window
without calling `load()` again. Writes from one window are immediately visible
to reads in another, and closing a database affects all windows.

### Resource Limits

The plugin enforces several resource limits to prevent denial-of-service from
untrusted or buggy frontend code:

   * **Database count**: Maximum 50 concurrently loaded databases (configurable
     via `Builder::max_databases()`)
   * **Interruptible transaction timeout**: Transactions that exceed the
     default (5 minutes) are automatically rolled back on the next access
     attempt (configurable via `Builder::transaction_timeout()`)
   * **Observer channel capacity**: Capped at 10,000 (default 256)
   * **Observed tables**: Maximum 100 tables per single `observe()` call — **not**
     a bound on the accumulated set for a database. `observe()` merges its tables
     into the existing broker, `subscribe()` also adds tables with no per-call
     limit, and nothing removes an individual table (the set is cleared only on a
     full teardown), so the total is currently unbounded. An observed table that
     does not exist also costs schema round trips on _every_ writer acquisition,
     indefinitely, while that database's write connection is held (#56)
   * **Subscriptions**: Maximum 100 active subscriptions per database

### Unbounded Result Sets

`fetchAll()` returns the entire result set in a single response with no built-in
size limit. For large or unbounded queries, prefer `fetchPage()` with keyset
pagination to keep memory usage bounded on both the Rust and TypeScript sides.

### Path Validation

Registration validates filesystem paths once (absolute, no traversal, canonicalized).
In-memory URIs are accepted as-is. At runtime, `Database.load(dbKey)` and
`Connection::connect(dbKey)` only accept **registered keys**; unknown keys return
`PATH_NOT_REGISTERED`.

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
