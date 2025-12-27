//! Transaction management for interruptible transactions

use std::collections::HashMap;
use std::sync::Arc;

use indexmap::IndexMap;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use sqlx::{Column, Row};
use sqlx_sqlite_conn_mgr::WriteGuard;
use tokio::sync::RwLock;
use tokio::task::AbortHandle;
use tracing::debug;

use crate::{Error, Result};

/// Active transaction state holding the writer and metadata
pub struct ActiveInterruptibleTransaction {
   db_path: String,
   transaction_id: String,
   writer: WriteGuard,
}

impl ActiveInterruptibleTransaction {
   pub fn new(db_path: String, transaction_id: String, writer: WriteGuard) -> Self {
      Self {
         db_path,
         transaction_id,
         writer,
      }
   }

   pub fn db_path(&self) -> &str {
      &self.db_path
   }

   pub fn transaction_id(&self) -> &str {
      &self.transaction_id
   }

   pub fn validate_token(&self, token_id: &str) -> Result<()> {
      if self.transaction_id != token_id {
         return Err(Error::InvalidTransactionToken);
      }
      Ok(())
   }

   /// Execute a read query within this transaction and return decoded results
   pub async fn read(
      &mut self,
      query: String,
      values: Vec<JsonValue>,
   ) -> Result<Vec<IndexMap<String, JsonValue>>> {
      let mut q = sqlx::query(&query);
      for value in values {
         q = crate::wrapper::bind_value(q, value);
      }

      let rows = q.fetch_all(&mut *self.writer).await?;

      let mut results = Vec::new();
      for row in rows {
         let mut value = IndexMap::default();
         for (i, column) in row.columns().iter().enumerate() {
            let v = row.try_get_raw(i)?;
            let v = crate::decode::to_json(v)?;
            value.insert(column.name().to_string(), v);
         }
         results.push(value);
      }

      Ok(results)
   }

   /// Execute statements on this transaction
   pub async fn execute_statements(&mut self, statements: Vec<Statement>) -> Result<()> {
      for statement in statements {
         let mut q = sqlx::query(&statement.query);
         for value in statement.values {
            q = crate::wrapper::bind_value(q, value);
         }
         q.execute(&mut *self.writer).await?;
      }
      Ok(())
   }

   /// Commit this transaction
   pub async fn commit(mut self) -> Result<()> {
      sqlx::query("COMMIT").execute(&mut *self.writer).await?;
      debug!("Transaction committed for db: {}", self.db_path);
      Ok(())
   }

   /// Rollback this transaction
   pub async fn rollback(mut self) -> Result<()> {
      sqlx::query("ROLLBACK").execute(&mut *self.writer).await?;
      debug!("Transaction rolled back for db: {}", self.db_path);
      Ok(())
   }
}

/// Statement in a transaction with query and bind values
#[derive(Debug, Deserialize)]
pub struct Statement {
   pub query: String,
   pub values: Vec<JsonValue>,
}

impl Drop for ActiveInterruptibleTransaction {
   fn drop(&mut self) {
      // On drop, the WriteGuard is dropped which returns connection to pool.
      // SQLite will automatically ROLLBACK the transaction when the connection
      // is returned to the pool if no explicit COMMIT was issued.
      debug!(
         "Dropping transaction for db: {}, tx_id: {} (will auto-rollback)",
         self.db_path, self.transaction_id
      );
   }
}

/// Global state tracking all active interruptible transactions
#[derive(Clone, Default)]
pub struct ActiveInterruptibleTransactions(
   Arc<RwLock<HashMap<String, ActiveInterruptibleTransaction>>>,
);

impl ActiveInterruptibleTransactions {
   pub async fn insert(&self, db_path: String, tx: ActiveInterruptibleTransaction) -> Result<()> {
      use std::collections::hash_map::Entry;
      let mut txs = self.0.write().await;

      // Ensure only one transaction per database using Entry API
      match txs.entry(db_path.clone()) {
         Entry::Vacant(e) => {
            e.insert(tx);
            Ok(())
         }
         Entry::Occupied(_) => Err(Error::TransactionAlreadyActive(db_path)),
      }
   }

   pub async fn abort_all(&self) {
      let mut txs = self.0.write().await;
      debug!("Aborting {} active interruptible transaction(s)", txs.len());

      for db_path in txs.keys() {
         debug!(
            "Dropping interruptible transaction for database: {}",
            db_path
         );
      }

      // Clear all transactions to drop WriteGuards and release locks
      // Dropping triggers auto-rollback via Drop trait
      txs.clear();
   }

   /// Remove and return transaction for commit/rollback
   pub async fn remove(
      &self,
      db_path: &str,
      token_id: &str,
   ) -> Result<ActiveInterruptibleTransaction> {
      let mut txs = self.0.write().await;

      // Validate token before removal
      let tx = txs
         .get(db_path)
         .ok_or_else(|| Error::NoActiveTransaction(db_path.to_string()))?;

      tx.validate_token(token_id)?;

      // Safe unwrap: we just confirmed the key exists above
      Ok(txs.remove(db_path).unwrap())
   }
}

/// Tracking for regular (non-pausable) transactions that are in-flight
/// This allows us to abort them on app exit
#[derive(Clone, Default)]
pub struct ActiveRegularTransactions(Arc<RwLock<HashMap<String, AbortHandle>>>);

impl ActiveRegularTransactions {
   pub async fn insert(&self, key: String, abort_handle: AbortHandle) {
      let mut txs = self.0.write().await;
      txs.insert(key, abort_handle);
   }

   pub async fn remove(&self, key: &str) {
      let mut txs = self.0.write().await;
      txs.remove(key);
   }

   pub async fn abort_all(&self) {
      let mut txs = self.0.write().await;
      debug!("Aborting {} active regular transaction(s)", txs.len());

      for (key, abort_handle) in txs.iter() {
         debug!("Aborting regular transaction: {}", key);
         abort_handle.abort();
      }

      // Clear all tracked transactions to prevent memory leak
      txs.clear();
   }
}

/// Cleanup all transactions on app exit
pub async fn cleanup_all_transactions(
   interruptible: &ActiveInterruptibleTransactions,
   regular: &ActiveRegularTransactions,
) {
   debug!("Cleaning up all active transactions");

   // Abort all transaction tasks
   interruptible.abort_all().await;
   regular.abort_all().await;

   // Interruptible transactions will auto-rollback when dropped
   // Regular transactions will also auto-rollback when aborted task cleans up

   debug!("Transaction cleanup initiated");
}
