//! Transaction management for interruptible transactions

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use indexmap::IndexMap;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use sqlx::{Column, Row};
use sqlx_sqlite_conn_mgr::{AttachedWriteGuard, WriteGuard};
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

#[cfg(feature = "observer")]
use sqlx_sqlite_observer::ObservableWriteGuard;

use crate::wrapper::{AttachedWriterGuard, WriterGuard};
use crate::{Error, Result, WriteQueryResult};

/// Wrapper around WriteGuard, ObservableWriteGuard, or AttachedWriteGuard
/// to unify transaction handling.
pub enum TransactionWriter {
   Regular(WriteGuard),
   Attached(AttachedWriteGuard),
   /// An observable writer, attached or not. One variant rather than two
   /// because `ObservableWriteGuard` already knows which kind of writer it wraps
   /// and handles both in `detach_all()`. Two variants of the same type would
   /// duplicate that distinction with nothing but the `From` impl below keeping
   /// them aligned - and a mis-map there strands the `ATTACH` alias.
   #[cfg(feature = "observer")]
   Observable(ObservableWriteGuard),
}

impl TransactionWriter {
   /// Execute a query on either writer type
   pub async fn execute_query<'a>(
      &mut self,
      query: sqlx::query::Query<'a, sqlx::Sqlite, sqlx::sqlite::SqliteArguments>,
   ) -> Result<sqlx::sqlite::SqliteQueryResult> {
      match self {
         Self::Regular(w) => query.execute(&mut **w).await.map_err(Into::into),
         Self::Attached(w) => query.execute(&mut **w).await.map_err(Into::into),
         #[cfg(feature = "observer")]
         Self::Observable(w) => query.execute(&mut **w).await.map_err(Into::into),
      }
   }

   /// Fetch all rows from either writer type
   pub async fn fetch_all<'a>(
      &mut self,
      query: sqlx::query::Query<'a, sqlx::Sqlite, sqlx::sqlite::SqliteArguments>,
   ) -> Result<Vec<sqlx::sqlite::SqliteRow>> {
      match self {
         Self::Regular(w) => query.fetch_all(&mut **w).await.map_err(Into::into),
         Self::Attached(w) => query.fetch_all(&mut **w).await.map_err(Into::into),
         #[cfg(feature = "observer")]
         Self::Observable(w) => query.fetch_all(&mut **w).await.map_err(Into::into),
      }
   }

   /// Begin an immediate transaction
   pub async fn begin_immediate(&mut self) -> Result<()> {
      self.execute_query(sqlx::query("BEGIN IMMEDIATE")).await?;
      Ok(())
   }

   /// Commit the current transaction
   pub async fn commit(&mut self) -> Result<()> {
      self.execute_query(sqlx::query("COMMIT")).await?;
      Ok(())
   }

   /// Rollback the current transaction
   pub async fn rollback(&mut self) -> Result<()> {
      self.execute_query(sqlx::query("ROLLBACK")).await?;
      Ok(())
   }

   /// Detach all attached databases if this is an attached writer
   pub async fn detach_if_attached(self) -> Result<()> {
      match self {
         Self::Attached(w) => w.detach_all().await?,
         // Called unconditionally, deliberately. For a non-attached inner writer
         // `detach_all()` skips the DETACH but still unregisters the hooks and
         // discards the guard's buffered events - work `Drop` would otherwise do
         // at an unspecified point. Do not "optimize" this into a no-op arm for a
         // writer that looks unattached; only the guard knows, and getting it
         // wrong strands the alias on the single write connection permanently.
         #[cfg(feature = "observer")]
         Self::Observable(w) => w.detach_all().await?,
         Self::Regular(_) => {}
      }
      Ok(())
   }
}

impl From<WriterGuard> for TransactionWriter {
   fn from(guard: WriterGuard) -> Self {
      match guard {
         WriterGuard::Regular(w) => TransactionWriter::Regular(w),
         #[cfg(feature = "observer")]
         WriterGuard::Observable(w) => TransactionWriter::Observable(w),
      }
   }
}

impl From<AttachedWriterGuard> for TransactionWriter {
   fn from(guard: AttachedWriterGuard) -> Self {
      match guard {
         AttachedWriterGuard::Regular(w) => TransactionWriter::Attached(w),
         #[cfg(feature = "observer")]
         AttachedWriterGuard::Observable(w) => TransactionWriter::Observable(w),
      }
   }
}

/// Active transaction state holding the writer and metadata
#[must_use = "if unused, the transaction is immediately rolled back"]
pub struct ActiveInterruptibleTransaction {
   db_path: String,
   transaction_id: String,
   writer: Option<TransactionWriter>,
   created_at: Instant,
   // Captured at construction so Drop can always spawn the rollback task on a
   // valid runtime, even when the struct is dropped from a thread that has no
   // tokio thread-local (e.g., Tauri teardown on the main thread). Without a
   // stored handle, Drop's synchronous path through PoolConnection::Drop would
   // call sqlx's rt::spawn and panic with "this functionality requires a Tokio
   // context".
   runtime_handle: tokio::runtime::Handle,
   #[cfg(test)]
   force_rollback_failure: bool,
}

impl ActiveInterruptibleTransaction {
   /// # Panics
   ///
   /// Panics if called outside a tokio runtime context. Both production call
   /// sites (the plugin command handler and the direct Rust API) run inside
   /// async functions, so this is a programming error, not a runtime risk.
   pub fn new(db_path: String, transaction_id: String, writer: TransactionWriter) -> Self {
      Self {
         db_path,
         transaction_id,
         writer: Some(writer),
         created_at: Instant::now(),
         runtime_handle: tokio::runtime::Handle::current(),
         #[cfg(test)]
         force_rollback_failure: false,
      }
   }

   /// Test-only: force `rollback()` to fail when aborting via transaction state.
   #[cfg(test)]
   pub fn force_rollback_failure_for_test(mut self) -> Self {
      self.force_rollback_failure = true;
      self
   }

   fn writer_mut(&mut self) -> Result<&mut TransactionWriter> {
      self
         .writer
         .as_mut()
         .ok_or(Error::TransactionAlreadyFinalized)
   }

   fn take_writer(&mut self) -> Result<TransactionWriter> {
      self.writer.take().ok_or(Error::TransactionAlreadyFinalized)
   }

   pub fn db_path(&self) -> &str {
      &self.db_path
   }

   pub fn transaction_id(&self) -> &str {
      &self.transaction_id
   }

   /// Execute a read query within this transaction and return decoded results
   pub async fn read(
      &mut self,
      query: String,
      values: Vec<JsonValue>,
   ) -> Result<Vec<IndexMap<String, JsonValue>>> {
      let mut q = sqlx::query(sqlx::AssertSqlSafe(query));
      for value in values {
         q = crate::wrapper::bind_value(q, value);
      }

      let rows = self.writer_mut()?.fetch_all(q).await?;

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

   /// Continue transaction with additional statements
   ///
   /// Accepts either `Statement` structs or tuples of `(&str, Vec<JsonValue>)`.
   pub async fn continue_with<S: Into<Statement>, I: IntoIterator<Item = S>>(
      &mut self,
      statements: I,
   ) -> Result<Vec<WriteQueryResult>> {
      let mut results = Vec::new();
      let writer = self.writer_mut()?;
      for statement in statements {
         let statement = statement.into();
         let mut q = sqlx::query(sqlx::AssertSqlSafe(statement.query));
         for value in statement.values {
            q = crate::wrapper::bind_value(q, value);
         }
         let exec_result = writer.execute_query(q).await?;
         results.push(WriteQueryResult {
            rows_affected: exec_result.rows_affected(),
            last_insert_id: exec_result.last_insert_rowid(),
         });
      }
      Ok(results)
   }

   /// Commit this transaction
   pub async fn commit(mut self) -> Result<()> {
      let mut writer = self.take_writer()?;
      writer.commit().await?;

      let db_path = self.db_path.clone();
      writer.detach_if_attached().await?;

      debug!("Transaction committed for db: {}", db_path);
      Ok(())
   }

   /// Rollback this transaction
   pub async fn rollback(mut self) -> Result<()> {
      #[cfg(test)]
      if self.force_rollback_failure {
         let db_path = self.db_path.clone();
         drop(self.take_writer()?);
         return Err(Error::Other(format!(
            "forced rollback failure for test (db: {db_path})"
         )));
      }

      let mut writer = self.take_writer()?;
      writer.rollback().await?;

      let db_path = self.db_path.clone();
      if let Err(detach_err) = writer.detach_if_attached().await {
         tracing::error!("detach_all failed after rollback: {}", detach_err);
      }

      debug!("Transaction rolled back for db: {}", db_path);
      Ok(())
   }
}

/// Statement in a transaction with query and bind values
#[derive(Debug, Deserialize)]
pub struct Statement {
   pub query: String,
   pub values: Vec<JsonValue>,
}

impl From<(&str, Vec<JsonValue>)> for Statement {
   fn from((query, values): (&str, Vec<JsonValue>)) -> Self {
      Self {
         query: query.to_string(),
         values,
      }
   }
}

impl From<(String, Vec<JsonValue>)> for Statement {
   fn from((query, values): (String, Vec<JsonValue>)) -> Self {
      Self { query, values }
   }
}

/// Upper bound on how long the auto-rollback task may hold the writer permit
/// before it is considered hung and the connection is abandoned.
const DROP_ROLLBACK_TIMEOUT: Duration = Duration::from_secs(5);

impl Drop for ActiveInterruptibleTransaction {
   fn drop(&mut self) {
      // If writer is still present, commit/rollback was not called. The connection
      // is about to return to the pool — we must issue ROLLBACK explicitly because
      // sqlx pools reuse the connection (SQLite only auto-rollbacks on close, not
      // on pool return). Without this, the next acquire_writer() gets a connection
      // with an open transaction and "BEGIN IMMEDIATE" fails.
      let Some(mut writer) = self.writer.take() else {
         return;
      };
      let db_path = std::mem::take(&mut self.db_path);
      let tx_id = std::mem::take(&mut self.transaction_id);

      debug!(
         "Dropping transaction for db: {}, tx_id: {} (auto-rollback scheduled)",
         db_path, tx_id
      );

      // No race with the next acquire_writer(): `writer` owns the PoolConnection
      // (via WriteGuard / AttachedWriteGuard), which holds the single-writer
      // permit. The permit is not released until `writer` drops at the end of
      // this task — after ROLLBACK completes. The next acquire_writer() blocks
      // on that permit, so it cannot see a connection with a still-open tx.
      //
      // The timeout bounds how long a pathological ROLLBACK (stuck I/O, a
      // rogue busy lock) can keep the single-writer pool stalled. On timeout
      // we drop `writer` inside the runtime; after_release then cleans up.
      self.runtime_handle.spawn(async move {
         let result = tokio::time::timeout(DROP_ROLLBACK_TIMEOUT, async {
            if let Err(e) = writer.rollback().await {
               warn!(
                  "auto-rollback on drop failed (db: {}, tx: {}): {}",
                  db_path, tx_id, e
               );
            }
            if let Err(e) = writer.detach_if_attached().await {
               warn!(
                  "detach_all after auto-rollback failed (db: {}, tx: {}): {}",
                  db_path, tx_id, e
               );
            }
            // writer drops here — connection returns to pool clean
         })
         .await;

         if result.is_err() {
            warn!(
               "auto-rollback on drop timed out after {:?} (db: {}, tx: {}) — pool's after_release hook will reconcile",
               DROP_ROLLBACK_TIMEOUT, db_path, tx_id
            );
         }
      });
   }
}

/// Default transaction timeout (5 minutes).
const DEFAULT_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(300);

/// Collects errors from best-effort cleanup loops.
struct CleanupErrors {
   errors: Vec<Error>,
}

impl CleanupErrors {
   fn new() -> Self {
      Self { errors: Vec::new() }
   }

   fn push(&mut self, err: Error) {
      match err {
         Error::TransactionCleanupFailed(nested) => self.errors.extend(nested),
         other => self.errors.push(other),
      }
   }

   fn push_result(&mut self, result: Result<()>) {
      if let Err(err) = result {
         self.push(err);
      }
   }

   fn into_result(self) -> Result<()> {
      match self.errors.len() {
         0 => Ok(()),
         1 => Err(self.errors.into_iter().next().expect("one error")),
         _ => Err(Error::TransactionCleanupFailed(self.errors)),
      }
   }
}

/// Global state tracking all active interruptible transactions.
///
/// Enforces one interruptible transaction per database path and applies a configurable
/// timeout. Expired transactions are cleaned up lazily on the next `insert()` or
/// `remove()` call — no background task is needed.
///
/// Uses `Mutex` rather than `RwLock` because all operations require write access,
/// and `Mutex<T>` only requires `T: Send` (not `T: Sync`) — avoiding an
/// `unsafe impl Sync` that would otherwise be needed due to non-`Sync` inner
/// types (`PoolConnection`, raw pointers in observer guards).
#[derive(Clone)]
pub struct ActiveInterruptibleTransactions {
   inner: Arc<Mutex<HashMap<String, ActiveInterruptibleTransaction>>>,
   timeout: Duration,
}

impl Default for ActiveInterruptibleTransactions {
   fn default() -> Self {
      Self::new(DEFAULT_TRANSACTION_TIMEOUT)
   }
}

impl ActiveInterruptibleTransactions {
   /// Create a new instance with the given transaction timeout.
   pub fn new(timeout: Duration) -> Self {
      Self {
         inner: Arc::new(Mutex::new(HashMap::new())),
         timeout,
      }
   }

   pub async fn insert(&self, db_path: String, tx: ActiveInterruptibleTransaction) -> Result<()> {
      use std::collections::hash_map::Entry;
      let mut txs = self.inner.lock().await;

      match txs.entry(db_path.clone()) {
         Entry::Vacant(e) => {
            e.insert(tx);
            Ok(())
         }
         Entry::Occupied(mut e) => {
            // If the existing transaction has expired, roll it back and replace
            // with the new one. We rollback explicitly (rather than relying on
            // Drop) so the writer is guaranteed to return to the pool clean
            // before the caller tries to start a new transaction on it.
            if e.get().created_at.elapsed() >= self.timeout {
               warn!(
                  "Evicting expired transaction for db: {} (age: {:?}, timeout: {:?})",
                  db_path,
                  e.get().created_at.elapsed(),
                  self.timeout,
               );
               let expired = e.insert(tx);
               if let Err(err) = expired.rollback().await {
                  warn!("rollback of expired transaction failed (db: {db_path}): {err}");
               }
               Ok(())
            } else {
               Err(Error::TransactionAlreadyActive(db_path))
            }
         }
      }
   }

   pub async fn abort_all(&self) -> Result<()> {
      // Drain under the lock, then release it before awaiting rollbacks so we
      // don't hold the mutex across a chain of awaits.
      let drained: Vec<(String, ActiveInterruptibleTransaction)> = {
         let mut txs = self.inner.lock().await;
         debug!("Aborting {} active interruptible transaction(s)", txs.len());
         txs.drain().collect()
      };

      let mut cleanup_errors = CleanupErrors::new();
      for (db_path, tx) in drained {
         debug!(
            "Rolling back interruptible transaction for database: {}",
            db_path
         );
         if let Err(err) = tx.rollback().await {
            warn!("rollback during abort_all failed (db: {db_path}): {err}");
            cleanup_errors.push(err);
         }
      }

      cleanup_errors.into_result()
   }

   /// Roll back and remove the interruptible transaction for a single database, if any.
   pub async fn abort_for_db(&self, db_key: &str) -> Result<()> {
      let maybe_tx = {
         let mut txs = self.inner.lock().await;
         txs.remove(db_key)
      };

      if let Some(tx) = maybe_tx {
         debug!(
            "Rolling back interruptible transaction for database: {}",
            db_key
         );
         tx.rollback().await?;
      }

      Ok(())
   }

   /// Remove and return transaction for commit/rollback.
   ///
   /// Returns `Err(Error::TransactionTimedOut)` if the transaction has exceeded the
   /// configured timeout. The expired transaction is rolled back before the error
   /// is returned.
   pub async fn remove(
      &self,
      db_path: &str,
      token_id: &str,
   ) -> Result<ActiveInterruptibleTransaction> {
      let mut txs = self.inner.lock().await;

      let tx = txs
         .get(db_path)
         .ok_or_else(|| Error::NoActiveTransaction(db_path.to_string()))?;

      if tx.transaction_id() != token_id {
         return Err(Error::InvalidTransactionToken);
      }

      // Happy path: not expired, hand it back to the caller.
      if tx.created_at.elapsed() < self.timeout {
         // Safe unwrap: we just confirmed the key exists above.
         return Ok(txs.remove(db_path).unwrap());
      }

      // Expired: take it out, release the lock, then rollback without holding
      // it so other callers aren't blocked on an unrelated cleanup.
      warn!(
         "Transaction timed out for db: {} (age: {:?}, timeout: {:?})",
         db_path,
         tx.created_at.elapsed(),
         self.timeout,
      );
      let expired = txs.remove(db_path).unwrap();
      drop(txs);

      if let Err(err) = expired.rollback().await {
         warn!("rollback of timed-out transaction failed (db: {db_path}): {err}");
      }
      Err(Error::TransactionTimedOut(db_path.to_string()))
   }
}

/// A single in-flight regular transaction tracked for cleanup on close.
struct TrackedRegularTransaction {
   db_key: String,
   handle: JoinHandle<()>,
}

/// Tracking for regular (non-pausable) transactions that are in-flight.
///
/// Holds join handles so transactions can be cancelled and awaited on close.
#[derive(Clone, Default)]
pub struct ActiveRegularTransactions(Arc<RwLock<HashMap<String, TrackedRegularTransaction>>>);

async fn abort_and_await_regular_handles(handles: Vec<(String, JoinHandle<()>)>) -> Result<()> {
   let mut cleanup_errors = CleanupErrors::new();

   for (tx_id, handle) in handles {
      debug!("Aborting regular transaction: {}", tx_id);
      handle.abort();
      match handle.await {
         Ok(()) => {}
         Err(e) if e.is_cancelled() => {}
         Err(e) => {
            let err = Error::Other(format!("regular transaction task panicked: {e}"));
            warn!("abort during close failed (tx: {tx_id}): {err}");
            cleanup_errors.push(err);
         }
      }
   }

   cleanup_errors.into_result()
}

impl ActiveRegularTransactions {
   pub async fn insert(&self, db_key: String, tx_id: String, handle: JoinHandle<()>) {
      let mut txs = self.0.write().await;
      txs.insert(tx_id, TrackedRegularTransaction { db_key, handle });
   }

   pub async fn remove(&self, tx_id: &str) {
      let mut txs = self.0.write().await;
      txs.remove(tx_id);
   }

   /// Remove and return a tracked handle so the caller can await task completion.
   pub async fn take_handle(&self, tx_id: &str) -> Option<JoinHandle<()>> {
      let mut txs = self.0.write().await;
      txs.remove(tx_id).map(|tracked| tracked.handle)
   }

   pub async fn abort_all(&self) -> Result<()> {
      let handles: Vec<(String, JoinHandle<()>)> = {
         let mut txs = self.0.write().await;
         debug!("Aborting {} active regular transaction(s)", txs.len());
         txs.drain()
            .map(|(tx_id, tracked)| (tx_id, tracked.handle))
            .collect()
      };

      abort_and_await_regular_handles(handles).await
   }

   /// Abort in-flight regular transactions for a single database.
   pub async fn abort_for_db(&self, db_key: &str) -> Result<()> {
      let handles: Vec<(String, JoinHandle<()>)> = {
         let mut txs = self.0.write().await;
         let keys_to_remove: Vec<String> = txs
            .iter()
            .filter(|(_, tracked)| tracked.db_key == db_key)
            .map(|(tx_id, _)| tx_id.clone())
            .collect();

         keys_to_remove
            .into_iter()
            .filter_map(|tx_id| txs.remove(&tx_id).map(|tracked| (tx_id, tracked.handle)))
            .collect()
      };

      abort_and_await_regular_handles(handles).await
   }
}

/// Cleanup all transactions on app exit.
pub async fn cleanup_all_transactions(
   interruptible: &ActiveInterruptibleTransactions,
   regular: &ActiveRegularTransactions,
) -> Result<()> {
   debug!("Cleaning up all active transactions");

   let mut cleanup_errors = CleanupErrors::new();
   cleanup_errors.push_result(interruptible.abort_all().await);
   cleanup_errors.push_result(regular.abort_all().await);

   debug!("Transaction cleanup complete");

   cleanup_errors.into_result()
}

pub async fn cleanup_transactions_for_db(
   db_key: &str,
   interruptible_txs: &ActiveInterruptibleTransactions,
   regular_txs: &ActiveRegularTransactions,
) -> Result<()> {
   let mut cleanup_errors = CleanupErrors::new();
   cleanup_errors.push_result(interruptible_txs.abort_for_db(db_key).await);
   cleanup_errors.push_result(regular_txs.abort_for_db(db_key).await);
   cleanup_errors.into_result()
}

#[cfg(test)]
mod abort_error_tests {
   use super::*;
   use crate::DatabaseWrapper;
   use serde_json::json;

   async fn begin_test_transaction(
      db: &DatabaseWrapper,
      db_path: &str,
   ) -> ActiveInterruptibleTransaction {
      let guard = db.acquire_writer().await.unwrap();
      let mut writer = TransactionWriter::from(guard);
      writer.begin_immediate().await.unwrap();
      ActiveInterruptibleTransaction::new(
         db_path.to_string(),
         uuid::Uuid::new_v4().to_string(),
         writer,
      )
   }

   #[tokio::test]
   async fn test_abort_for_db_propagates_rollback_failure() {
      let temp_dir = tempfile::tempdir().unwrap();
      let db_path = temp_dir.path().join("fail.db");
      let db = DatabaseWrapper::connect(&db_path, None).await.unwrap();
      db.execute(
         "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)".into(),
         vec![],
      )
      .await
      .unwrap();

      let state = ActiveInterruptibleTransactions::default();
      let mut tx = begin_test_transaction(&db, "fail.db").await;
      tx.continue_with(vec![(
         "INSERT INTO t (val) VALUES (?)",
         vec![json!("uncommitted")],
      )])
      .await
      .unwrap();
      let tx = tx.force_rollback_failure_for_test();
      state.insert("fail.db".into(), tx).await.unwrap();

      let err = state.abort_for_db("fail.db").await.unwrap_err();
      assert!(err.to_string().contains("forced rollback failure"));
   }

   #[tokio::test]
   async fn test_abort_all_propagates_rollback_failure() {
      let temp_dir = tempfile::tempdir().unwrap();
      let db_path = temp_dir.path().join("fail-all.db");
      let db = DatabaseWrapper::connect(&db_path, None).await.unwrap();
      db.execute(
         "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)".into(),
         vec![],
      )
      .await
      .unwrap();

      let state = ActiveInterruptibleTransactions::default();
      let mut tx = begin_test_transaction(&db, "fail-all.db").await;
      tx.continue_with(vec![(
         "INSERT INTO t (val) VALUES (?)",
         vec![json!("uncommitted")],
      )])
      .await
      .unwrap();
      let tx = tx.force_rollback_failure_for_test();
      state.insert("fail-all.db".into(), tx).await.unwrap();

      let err = state.abort_all().await.unwrap_err();
      assert!(err.to_string().contains("forced rollback failure"));
   }

   #[tokio::test]
   async fn test_abort_all_attempts_all_rollbacks_before_returning_error() {
      let temp_dir = tempfile::tempdir().unwrap();
      let db_path1 = temp_dir.path().join("fail-first.db");
      let db_path2 = temp_dir.path().join("fail-second.db");
      let db1 = DatabaseWrapper::connect(&db_path1, None).await.unwrap();
      let db2 = DatabaseWrapper::connect(&db_path2, None).await.unwrap();
      for db in [&db1, &db2] {
         db.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)".into(),
            vec![],
         )
         .await
         .unwrap();
      }

      let state = ActiveInterruptibleTransactions::default();

      let mut tx1 = begin_test_transaction(&db1, "first.db").await;
      tx1.continue_with(vec![(
         "INSERT INTO t (val) VALUES (?)",
         vec![json!("first")],
      )])
      .await
      .unwrap();
      let tx1 = tx1.force_rollback_failure_for_test();
      state.insert("first.db".into(), tx1).await.unwrap();

      let mut tx2 = begin_test_transaction(&db2, "second.db").await;
      tx2.continue_with(vec![(
         "INSERT INTO t (val) VALUES (?)",
         vec![json!("second")],
      )])
      .await
      .unwrap();
      state.insert("second.db".into(), tx2).await.unwrap();

      let err = state.abort_all().await.unwrap_err();
      assert!(err.to_string().contains("forced rollback failure"));

      let rows = db2
         .fetch_all("SELECT val FROM t".into(), vec![])
         .await
         .unwrap();
      assert!(
         rows.is_empty(),
         "second transaction should still be rolled back after first rollback failed"
      );
   }

   #[tokio::test]
   async fn test_abort_all_returns_aggregate_error_for_multiple_failures() {
      let temp_dir = tempfile::tempdir().unwrap();
      let db_path1 = temp_dir.path().join("aggregate-first.db");
      let db_path2 = temp_dir.path().join("aggregate-second.db");
      let db1 = DatabaseWrapper::connect(&db_path1, None).await.unwrap();
      let db2 = DatabaseWrapper::connect(&db_path2, None).await.unwrap();
      for db in [&db1, &db2] {
         db.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)".into(),
            vec![],
         )
         .await
         .unwrap();
      }

      let state = ActiveInterruptibleTransactions::default();

      let mut tx1 = begin_test_transaction(&db1, "first.db").await;
      tx1.continue_with(vec![(
         "INSERT INTO t (val) VALUES (?)",
         vec![json!("first")],
      )])
      .await
      .unwrap();
      let tx1 = tx1.force_rollback_failure_for_test();
      state.insert("first.db".into(), tx1).await.unwrap();

      let mut tx2 = begin_test_transaction(&db2, "second.db").await;
      tx2.continue_with(vec![(
         "INSERT INTO t (val) VALUES (?)",
         vec![json!("second")],
      )])
      .await
      .unwrap();
      let tx2 = tx2.force_rollback_failure_for_test();
      state.insert("second.db".into(), tx2).await.unwrap();

      let err = state.abort_all().await.unwrap_err();
      let cleanup_errors = err
         .cleanup_errors()
         .expect("multiple cleanup failures should return aggregate error");
      assert_eq!(cleanup_errors.len(), 2);
      assert!(
         cleanup_errors[0]
            .to_string()
            .contains("forced rollback failure")
      );
      assert!(
         cleanup_errors[1]
            .to_string()
            .contains("forced rollback failure")
      );
      assert_eq!(err.error_code(), "TRANSACTION_CLEANUP_FAILED");
   }
}
