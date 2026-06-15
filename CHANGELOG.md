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

### Fixed

- Regenerated `api-iife.js` so all IPC calls use `dbKey` consistently.
