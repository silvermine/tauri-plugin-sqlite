//! Tests for transaction state management types.

use serde_json::json;
use sqlx_sqlite_toolkit::{
   ActiveInterruptibleTransactions, ActiveRegularTransactions, DatabaseWrapper, Error,
   cleanup_all_transactions,
};
use tempfile::TempDir;

/// Helper to extract Err from Result<ActiveInterruptibleTransaction, Error>
/// since ActiveInterruptibleTransaction doesn't implement Debug.
fn expect_err(
   result: std::result::Result<sqlx_sqlite_toolkit::ActiveInterruptibleTransaction, Error>,
) -> Error {
   match result {
      Err(e) => e,
      Ok(_) => panic!("expected Err, got Ok"),
   }
}

async fn create_test_db(name: &str) -> (DatabaseWrapper, TempDir) {
   let temp_dir = TempDir::new().expect("Failed to create temp directory");
   let db_path = temp_dir.path().join(name);
   let wrapper = DatabaseWrapper::connect(&db_path, None)
      .await
      .expect("Failed to connect to test database");

   (wrapper, temp_dir)
}

/// Helper to create a real ActiveInterruptibleTransaction by starting
/// an actual database transaction (the type requires a real writer).
async fn begin_transaction(
   db: &DatabaseWrapper,
   db_path: &str,
) -> sqlx_sqlite_toolkit::ActiveInterruptibleTransaction {
   use sqlx_sqlite_toolkit::TransactionWriter;

   let guard = db.acquire_writer().await.unwrap();
   let mut writer = TransactionWriter::from(guard);
   writer.begin_immediate().await.unwrap();

   sqlx_sqlite_toolkit::ActiveInterruptibleTransaction::new(
      db_path.to_string(),
      uuid::Uuid::new_v4().to_string(),
      writer,
   )
}

// ============================================================================
// ActiveInterruptibleTransactions tests
// ============================================================================

#[tokio::test]
async fn test_insert_and_remove() {
   let (db, _temp) = create_test_db("test.db").await;

   db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)".into(), vec![])
      .await
      .unwrap();

   let state = ActiveInterruptibleTransactions::default();
   let tx = begin_transaction(&db, "test.db").await;
   let tx_id = tx.transaction_id().to_string();

   state.insert("test.db".into(), tx).await.unwrap();

   let removed = state.remove("test.db", &tx_id).await.unwrap();
   assert_eq!(removed.db_path(), "test.db");
   assert_eq!(removed.transaction_id(), tx_id);
}

#[tokio::test]
async fn test_insert_duplicate_rejected() {
   // Use two separate databases so both can acquire writers independently,
   // but insert them under the same key to test duplicate rejection.
   let (db1, _temp1) = create_test_db("dup1.db").await;
   let (db2, _temp2) = create_test_db("dup2.db").await;

   for db in [&db1, &db2] {
      db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)".into(), vec![])
         .await
         .unwrap();
   }

   let state = ActiveInterruptibleTransactions::default();

   let tx1 = begin_transaction(&db1, "shared-key").await;
   state.insert("shared-key".into(), tx1).await.unwrap();

   // Second insert for same key should fail
   let tx2 = begin_transaction(&db2, "shared-key").await;
   let err = state.insert("shared-key".into(), tx2).await.unwrap_err();
   assert_eq!(err.error_code(), "TRANSACTION_ALREADY_ACTIVE");
   assert!(err.to_string().contains("shared-key"));
}

#[tokio::test]
async fn test_remove_nonexistent_db() {
   let state = ActiveInterruptibleTransactions::default();

   let err = expect_err(state.remove("nonexistent.db", "some-token").await);
   assert_eq!(err.error_code(), "NO_ACTIVE_TRANSACTION");
   assert!(err.to_string().contains("nonexistent.db"));
}

#[tokio::test]
async fn test_remove_wrong_token() {
   let (db, _temp) = create_test_db("token.db").await;

   db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)".into(), vec![])
      .await
      .unwrap();

   let state = ActiveInterruptibleTransactions::default();
   let tx = begin_transaction(&db, "token.db").await;

   state.insert("token.db".into(), tx).await.unwrap();

   let err = expect_err(state.remove("token.db", "wrong-token-id").await);
   assert_eq!(err.error_code(), "INVALID_TRANSACTION_TOKEN");
}

#[tokio::test]
async fn test_abort_all_clears_transactions() {
   let (db, _temp) = create_test_db("abort.db").await;

   db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)".into(), vec![])
      .await
      .unwrap();

   let state = ActiveInterruptibleTransactions::default();
   let tx = begin_transaction(&db, "abort.db").await;
   let tx_id = tx.transaction_id().to_string();

   state.insert("abort.db".into(), tx).await.unwrap();
   state.abort_all().await;

   // After abort_all, remove should fail (transaction was cleared)
   let err = expect_err(state.remove("abort.db", &tx_id).await);
   assert_eq!(err.error_code(), "NO_ACTIVE_TRANSACTION");
}

#[tokio::test]
async fn test_abort_all_auto_rollbacks_uncommitted_writes() {
   let (db, _temp) = create_test_db("rollback.db").await;

   db.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)".into(),
      vec![],
   )
   .await
   .unwrap();

   let state = ActiveInterruptibleTransactions::default();
   let mut tx = begin_transaction(&db, "rollback.db").await;

   // Write inside the transaction
   tx.continue_with(vec![(
      "INSERT INTO t (val) VALUES (?)",
      vec![json!("uncommitted")],
   )])
   .await
   .unwrap();

   // Store and abort (should auto-rollback on drop)
   state.insert("rollback.db".into(), tx).await.unwrap();
   state.abort_all().await;

   // The uncommitted write should not be visible
   let rows = db
      .fetch_all("SELECT * FROM t".into(), vec![])
      .await
      .unwrap();

   assert!(
      rows.is_empty(),
      "Aborted transaction writes should be rolled back"
   );
}

#[tokio::test]
async fn test_insert_after_abort_all_succeeds() {
   // Use two separate databases to avoid writer contention during abort/reacquire.
   let (db1, _temp1) = create_test_db("reuse1.db").await;
   let (db2, _temp2) = create_test_db("reuse2.db").await;

   for db in [&db1, &db2] {
      db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)".into(), vec![])
         .await
         .unwrap();
   }

   let state = ActiveInterruptibleTransactions::default();

   let tx = begin_transaction(&db1, "reuse-key").await;
   state.insert("reuse-key".into(), tx).await.unwrap();
   state.abort_all().await;

   // Should be able to insert again after abort
   let tx2 = begin_transaction(&db2, "reuse-key").await;
   state.insert("reuse-key".into(), tx2).await.unwrap();
}

// ============================================================================
// ActiveRegularTransactions tests
// ============================================================================

#[tokio::test]
async fn test_regular_insert_and_remove() {
   let state = ActiveRegularTransactions::default();

   let handle = tokio::spawn(async { /* no-op */ });
   state.insert("tx-1".into(), handle.abort_handle()).await;

   // Remove should succeed (no panic, no error)
   state.remove("tx-1").await;

   // Removing again is a no-op
   state.remove("tx-1").await;
}

#[tokio::test]
async fn test_regular_abort_all_cancels_tasks() {
   let state = ActiveRegularTransactions::default();

   // Spawn a long-running task
   let handle = tokio::spawn(async {
      tokio::time::sleep(std::time::Duration::from_secs(60)).await;
   });
   let abort_handle = handle.abort_handle();

   state.insert("long-task".into(), abort_handle).await;
   state.abort_all().await;

   // The task should have been aborted
   let result = handle.await;
   assert!(result.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn test_regular_abort_all_clears_state() {
   let state = ActiveRegularTransactions::default();

   let h1 = tokio::spawn(async {});
   let h2 = tokio::spawn(async {});

   state.insert("a".into(), h1.abort_handle()).await;
   state.insert("b".into(), h2.abort_handle()).await;

   state.abort_all().await;

   // State should be empty — inserting new keys should work
   let h3 = tokio::spawn(async {});
   state.insert("a".into(), h3.abort_handle()).await;
}

// ============================================================================
// cleanup_all_transactions tests
// ============================================================================

#[tokio::test]
async fn test_cleanup_all_transactions() {
   let (db, _temp) = create_test_db("cleanup.db").await;

   db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)".into(), vec![])
      .await
      .unwrap();

   let interruptible = ActiveInterruptibleTransactions::default();
   let regular = ActiveRegularTransactions::default();

   // Add an interruptible transaction
   let tx = begin_transaction(&db, "cleanup.db").await;
   interruptible.insert("cleanup.db".into(), tx).await.unwrap();

   // Add a regular transaction
   let handle = tokio::spawn(async {
      tokio::time::sleep(std::time::Duration::from_secs(60)).await;
   });
   regular
      .insert("regular-1".into(), handle.abort_handle())
      .await;

   // Cleanup should clear both
   cleanup_all_transactions(&interruptible, &regular).await;

   // Interruptible should be empty
   let err = expect_err(interruptible.remove("cleanup.db", "any").await);
   assert_eq!(err.error_code(), "NO_ACTIVE_TRANSACTION");

   // Regular task should be cancelled
   assert!(handle.await.unwrap_err().is_cancelled());
}
