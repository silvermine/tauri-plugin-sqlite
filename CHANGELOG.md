# Changelog

All notable changes to this project will be documented in this file.

## [0.2.0] - Unreleased

### Breaking Changes

#### Database registration by key

Databases must be registered on the Rust side with a stable key before they can be opened. The frontend and Rust callers open databases by **key**, not filesystem path.

- **`Builder::add_migrations(path, migrator)`** replaced by **`Builder::register_database(key, path, migrator?)`**, which returns `Result`.
- Added **`Builder::on_setup`** and **`SetupRegistrar`** for runtime path registration (e.g. from `app.path().app_data_dir()`).
- **`Builder<R>`** is now generic over Tauri `Runtime`; use `Builder::<tauri::Wry>::new()` where a turbofish is required.

#### Frontend API

- First argument to **`Database.load()`** and **`Database.get()`** is a registration **key**, not a path. Passing a path string type-checks but fails at runtime with `PATH_NOT_REGISTERED`.
- **`Database.get(dbKey)`** is synchronous again; it defers connection until the first operation that requires a loaded database. Use **`Database.load(dbKey, customConfig?)`** to connect eagerly or pass pool configuration.
- IPC command arguments: **`db`** → **`dbKey`**; attached **`databasePath`** → **`databaseKey`**.
- **`MigrationEvent`**: adds **`dbKey`**; **`dbPath`** is now an absolute path.
- **`TransactionToken`**: **`dbPath`** → **`dbKey`**.

#### Rust API

- Added **`Connection`** trait on **`AppHandle`** for Rust-side opens by registration key.
- **`Builder::build()`** returns **`Result`**; duplicate paths across distinct registration keys fail with **`INVALID_CONFIG`**.
- File paths must be **absolute** at registration; relative path resolution at load time was removed.
- Invalid registration paths fail at startup (`INVALID_PATH`, `PATH_TRAVERSAL`); unregistered keys fail at open time (`PATH_NOT_REGISTERED`).

### Added

- Path validation module (`validate.rs`) with canonicalization at registration time.
- Parent directory auto-creation during registration validation for file paths.
- CI check that committed `api-iife.js` matches a fresh Rollup build.

### Changed

#### `close` aborts active transactions before closing

`Database.close()` (IPC `plugin:sqlite|close`), `Database.close_all()` (IPC `plugin:sqlite|close_all`), and the Rust [`Connection::close`](src/lib.rs) API now roll back or cancel in-flight transactions before closing connection pools.

- **Interruptible transactions** (`beginInterruptibleTransaction`): explicitly rolled back via `ROLLBACK` before the pool is closed.
- **Regular transactions** (`executeTransaction`): the in-flight task is aborted and awaited so pooled connections are released before the pool closes.

Previously, `close` only aborted active subscriptions; open transactions could block a clean shutdown or leave uncommitted work on pooled connections. The same cleanup logic is available to Rust callers through [`close_database`](src/lib.rs) and [`close_all_loaded_databases`](src/lib.rs).

Transaction cleanup failures propagate as errors rather than being logged and ignored, so a successful close indicates the database file is safe to delete or recreate.

### Fixed

- Regular transaction cleanup no longer uses string-prefix matching on database keys, which could abort transactions belonging to a different registered database when keys contain `:` (for example `:memory:` or `a` vs `a:b`).
- Transaction cleanup attempts all rollbacks/aborts before returning, rather than stopping at the first failure.
- `close` / `close_all` now attempt pool teardown even when transaction cleanup fails, avoiding a half-closed state where subscriptions are gone but the pool remains loaded.
- `close` / `close_all` are bounded by a 5-second timeout.
- Regular transactions cancelled by `close` now return `TRANSACTION_CANCELLED` instead of a generic "task dropped" error; task panics remain distinct.
- Transaction cleanup now returns `TRANSACTION_CLEANUP_FAILED` with all collected errors when multiple rollbacks or aborts fail, instead of discarding earlier failures.
- `load()` no longer hangs forever for databases registered without a migrator. Migration state is only tracked when a migrator is provided; otherwise `await_migrations` proceeds immediately.
- Regenerated `api-iife.js` so all IPC calls use `dbKey` consistently.
