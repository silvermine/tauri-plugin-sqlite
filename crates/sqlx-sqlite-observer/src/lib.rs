//! Reactive change notifications for SQLite databases using sqlx.
//!
//! This crate provides **transaction-safe** change notifications for SQLite databases
//! using SQLite's native hooks (`preupdate_hook`, `commit_hook`, `rollback_hook`).
//!
//! # SQLite Requirements
//!
//! Requires SQLite compiled with `SQLITE_ENABLE_PREUPDATE_HOOK`.
//!
//! **Important:** Most system SQLite libraries do NOT have this option enabled by default.
//! You have two options:
//!
//! 1. **Use the `bundled` feature** (recommended for most users):
//!    ```toml
//!    sqlx-sqlite-observer = { version = "0.8", features = ["bundled"] }
//!    ```
//!    This compiles SQLite from source with preupdate hook support (~1MB binary size increase).
//!
//! 2. **Provide your own SQLite** with `SQLITE_ENABLE_PREUPDATE_HOOK` compiled in.
//!    Use [`is_preupdate_hook_enabled()`] to verify at runtime.
//!
//! # Status
//!
//! This crate is in early development. See the README for the planned API.

pub mod broker;
pub mod change;
pub mod error;
pub mod hooks;
pub mod schema;

pub use broker::ObservationBroker;
pub use change::{ChangeOperation, ColumnValue, TableChange, TableInfo};
pub use error::Error;
pub use hooks::{SqliteValue, is_preupdate_hook_enabled, unregister_hooks};

pub type Result<T> = std::result::Result<T, Error>;
