//! High-level SQLite toolkit providing builders, transactions, and type decoding.
//!
//! This crate sits between the low-level connection manager (`sqlx-sqlite-conn-mgr`)
//! and application-level code (e.g., a Tauri plugin). It provides:
//!
//! - [`DatabaseWrapper`] — main entry point wrapping a connection-managed database
//! - Builder-pattern APIs for queries ([`ExecuteBuilder`], [`FetchAllBuilder`], [`FetchOneBuilder`])
//! - Transaction support ([`TransactionExecutionBuilder`], [`InterruptibleTransactionBuilder`])
//! - JSON type decoding for SQLite values
//!
//! # Example
//!
//! ```no_run
//! use sqlx_sqlite_toolkit::DatabaseWrapper;
//! use serde_json::json;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let db = DatabaseWrapper::connect(std::path::Path::new("mydb.db"), None).await?;
//!
//! // Write
//! db.execute("INSERT INTO users (name) VALUES (?)".into(), vec![json!("Alice")]).await?;
//!
//! // Read
//! let rows = db.fetch_all("SELECT * FROM users".into(), vec![]).await?;
//!
//! // Transaction
//! let results = db.execute_transaction(vec![
//!    ("INSERT INTO users (name) VALUES (?)", vec![json!("Bob")]),
//!    ("INSERT INTO users (name) VALUES (?)", vec![json!("Charlie")]),
//! ]).await?;
//!
//! db.close().await?;
//! # Ok(())
//! # }
//! ```

pub mod builders;
pub mod decode;
pub mod error;
pub mod transactions;
pub mod wrapper;

pub use builders::{ExecuteBuilder, FetchAllBuilder, FetchOneBuilder};
pub use error::{Error, Result};
pub use transactions::{
   ActiveInterruptibleTransaction, ActiveInterruptibleTransactions, ActiveRegularTransactions,
   Statement, TransactionWriter, cleanup_all_transactions,
};
pub use wrapper::{
   DatabaseWrapper, InterruptibleTransaction, InterruptibleTransactionBuilder,
   TransactionExecutionBuilder, WriteQueryResult, WriterGuard, bind_value,
};

// Re-export commonly used types from dependencies
pub use sqlx_sqlite_conn_mgr::{
   AttachedMode, AttachedSpec, Migrator, SqliteDatabase, SqliteDatabaseConfig,
};
