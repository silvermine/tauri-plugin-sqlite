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
- **`observe()` no longer resets observation.** It previously aborted every subscription for the database and rebuilt the observer; it is now additive and reference-counted (#54, see Fixed below). An app relying on that implicit reset now accumulates one **live** subscription per call, eventually failing with `TOO_MANY_SUBSCRIPTIONS`. **Migration:** `unsubscribe()` the previous subscription explicitly.
- **`subscribe()` now requires the calling window to have called `observe()` itself**, failing with `OBSERVATION_NOT_ENABLED` otherwise. Previously a window could piggyback on another window's registration, then have its subscription silently aborted when that window released it. **Migration:** every window that subscribes must call `observe()` first.
- **`observe()` now rejects a conflicting `channelCapacity`/`captureValues`** with `OBSERVATION_CONFIG_CONFLICT` instead of silently ignoring them. Both are fixed by the first window to observe a database, since the broadcast channel behind them cannot be resized without dropping subscribers. Omitting either field inherits the active value; only an explicit request for a *different* value is rejected.

#### Rust API

- Added **`Connection`** trait on **`AppHandle`** for Rust-side opens by registration key.
- **`Builder::build()`** returns **`Result`**; duplicate paths across distinct registration keys fail with **`INVALID_CONFIG`**.
- File paths must be **absolute** at registration; relative path resolution at load time was removed.
- Invalid registration paths fail at startup (`INVALID_PATH`, `PATH_TRAVERSAL`); unregistered keys fail at open time (`PATH_NOT_REGISTERED`).
- **`DatabaseWrapper::enable_observation()` no longer tears down the existing broker** (#54, see Fixed below); it reuses one additively. Callers who re-called it to shed subscribers, or to change `channel_capacity`/`capture_values` on a live database, must now call `disable_observation()` first. It stays infallible and only logs a conflict — and that log is compiled out in release, so read back `broker().channel_capacity()` / `.capture_values()` to confirm.
- Added **`Error::ObservationConfigConflict`** (code `OBSERVATION_CONFIG_CONFLICT`). `Error` is not `#[non_exhaustive]`, so exhaustive matches on it need a new arm.

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

#### `remove` aborts active transactions and is bounded by a timeout

`Database.remove()` (IPC `plugin:sqlite|remove`) now rolls back or cancels in-flight transactions before tearing down the database's pools, and the whole teardown is bounded by the same 5-second timeout as `close`. Previously an abandoned transaction holding the write connection could make `remove()` wait indefinitely.

`remove()` now also holds the database registry's write lock across the file deletion (see Fixed). That lock covers every loaded database, so `remove()` briefly blocks operations on unrelated databases; the timeout bounds how long.

### Fixed

- **Re-calling `observe()` no longer terminates existing subscribers (#54).** Observation is additive and reference-counted per webview window: a second `observe()` call merges its tables into the existing broker instead of recreating it, and `unobserve()` releases only the calling window's registration. Observation is disabled once every window that called `observe()` has released. Previously any `observe()` call tore down the broker, silently ending every window's subscriptions with no error.
  - `channelCapacity` and `captureValues` are fixed by the first window to enable observation, since the broadcast channel behind them cannot be resized without dropping subscribers. Omitting them inherits the active values; an explicit request for different ones is rejected with `OBSERVATION_CONFIG_CONFLICT` (see Breaking Changes).
  - `observe()`, `unobserve()`, `remove()`, and the window-destroyed cleanup now hold a consistent lock order across both state stores, closing a race that could leave the broker and the recorded registrations out of sync.
  - Closing a window without calling `unobserve()` no longer leaks its registration; it is released when the window is destroyed.
  - **Known limitation:** the 100-observed-table limit still bounds only a single `observe()` request, not the accumulated set for a database - see the README's Resource Limits section. Because the destructive teardown was the only incremental reset of that set, a nonexistent observed table now costs schema round trips on every writer acquisition indefinitely (#56).
  - **Known limitation:** observation is reference-counted per *webview*, not per caller. Two modules in the same window share one registration, so whichever calls `unobserve()` first tears down observation - and subscriptions - for both. A window needs a single owner of the `observe()`/`unobserve()` pair (#57).
- Finished subscriptions now remove their own tracking entry when their forwarding loop ends, so entries left over from a torn-down broker no longer count against the 100-subscriptions-per-database limit. **Known limitation:** this does not cover a reloaded or destroyed webview, where delivery from Rust still succeeds and the forwarding task keeps running (#58). Call `unsubscribe()` (or `unobserve()`) before navigating away or closing a window.
- `remove()` no longer deletes the database files outside the registry write lock, where a concurrent `load()` could connect to the database being torn down and then have its files unlinked underneath it - leaving the frontend writing to unlinked inodes on Unix, or failing `remove()` with the pools already closed on Windows.
- Regular transaction cleanup no longer uses string-prefix matching on database keys, which could abort transactions belonging to a different registered database when keys contain `:` (for example `:memory:` or `a` vs `a:b`).
- Transaction cleanup attempts all rollbacks/aborts before returning, rather than stopping at the first failure.
- `close` / `close_all` now attempt pool teardown even when transaction cleanup fails, avoiding a half-closed state where subscriptions are gone but the pool remains loaded.
- `close` / `close_all` are bounded by a 5-second timeout.
- Regular transactions cancelled by `close` now return `TRANSACTION_CANCELLED` instead of a generic "task dropped" error; task panics remain distinct.
- Transaction cleanup now returns `TRANSACTION_CLEANUP_FAILED` with all collected errors when multiple rollbacks or aborts fail, instead of discarding earlier failures.
- `load()` no longer hangs forever for databases registered without a migrator. Migration state is only tracked when a migrator is provided; otherwise `await_migrations` proceeds immediately.
- Regenerated `api-iife.js` so all IPC calls use `dbKey` consistently.
