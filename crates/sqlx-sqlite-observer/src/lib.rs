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
//! If preupdate hooks are not available, [`SqliteObserver::acquire()`] will return
//! an error with a descriptive message.
//!
//! # Features
//!
//! - **Transaction-safe notifications** - changes only notify after successful commit
//! - **Typed column values** - access old/new values with native SQLite types
//! - **Stream support** - use `tokio_stream::Stream` for async iteration
//! - **Multiple subscribers** - broadcast channel supports multiple listeners
//!
//! # Basic Example
//!
//! ```rust,no_run
//! use sqlx::SqlitePool;
//! use sqlx_sqlite_observer::{SqliteObserver, ObserverConfig, ColumnValue};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let pool = SqlitePool::connect("sqlite:mydb.db").await?;
//!     let observer = SqliteObserver::new(pool, ObserverConfig::default());
//!
//!     // Subscribe to changes on specific tables
//!     let mut rx = observer.subscribe(["users"]);
//!
//!     // Spawn a task to handle notifications
//!     tokio::spawn(async move {
//!         while let Ok(change) = rx.recv().await {
//!             println!(
//!                 "Table {} row {} was {:?}",
//!                 change.table,
//!                 change.rowid.unwrap_or(-1),
//!                 change.operation
//!             );
//!             // Access typed column values
//!             if let Some(old) = &change.old_values {
//!                 println!("  Old values: {:?}", old);
//!             }
//!             if let Some(new) = &change.new_values {
//!                 println!("  New values: {:?}", new);
//!             }
//!         }
//!     });
//!
//!     // Use the observer to execute queries
//!     let mut conn = observer.acquire().await?;
//!     sqlx::query("INSERT INTO users (name) VALUES (?)")
//!         .bind("Alice")
//!         .execute(&mut **conn)
//!         .await?;
//!
//!     // Changes are published automatically when the transaction commits
//!     drop(conn);
//!
//!     Ok(())
//! }
//! ```
//!
//! # Stream Example
//!
//! ```rust,no_run
//! use futures::StreamExt;
//! use sqlx::SqlitePool;
//! use sqlx_sqlite_observer::{
//!     SqliteObserver, ObserverConfig, TableChangeEvent, TableChangeStreamExt,
//! };
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let pool = SqlitePool::connect("sqlite:mydb.db").await?;
//!     let config = ObserverConfig::new().with_tables(["users", "posts"]);
//!     let observer = SqliteObserver::new(pool, config);
//!
//!     // Get a Stream instead of broadcast::Receiver
//!     let mut stream = observer.subscribe_stream(["users"]);
//!
//!     // Stream yields TableChangeEvent variants
//!     while let Some(event) = stream.next().await {
//!         match event {
//!             TableChangeEvent::Change(change) => {
//!                 println!(
//!                     "Table {} row {} was {:?}",
//!                     change.table,
//!                     change.rowid.unwrap_or(-1),
//!                     change.operation
//!                 );
//!                 if let Some(old) = &change.old_values {
//!                     println!("  Old values: {:?}", old);
//!                 }
//!                 if let Some(new) = &change.new_values {
//!                     println!("  New values: {:?}", new);
//!                 }
//!             }
//!             TableChangeEvent::Lagged(n) => {
//!                 eprintln!("Missed {} notifications, re-query state", n);
//!             }
//!         }
//!     }
//!
//!     Ok(())
//! }
//! ```

pub mod broker;
pub mod change;
pub mod config;
pub mod connection;
pub mod error;
pub mod hooks;
pub mod observer;
pub mod schema;
pub mod stream;

#[cfg(feature = "conn-mgr")]
pub mod conn_mgr;

pub use broker::ObservationBroker;
pub use change::{ChangeOperation, ColumnValue, TableChange, TableChangeEvent, TableInfo};
pub use config::ObserverConfig;
pub use connection::ObservableConnection;
pub use error::Error;
pub use hooks::{SqliteValue, is_preupdate_hook_enabled, unregister_hooks};
pub use observer::SqliteObserver;
pub use stream::{TableChangeStream, TableChangeStreamExt};

#[cfg(feature = "conn-mgr")]
pub use conn_mgr::{ObservableSqliteDatabase, ObservableWriteGuard};

pub type Result<T> = std::result::Result<T, Error>;
