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
   state.abort_all().await.unwrap();

   // After abort_all, remove should fail (transaction was cleared)
   let err = expect_err(state.remove("abort.db", &tx_id).await);
   assert_eq!(err.error_code(), "NO_ACTIVE_TRANSACTION");
}

#[tokio::test]
async fn test_abort_for_db_clears_only_matching_interruptible() {
   let (db1, _temp1) = create_test_db("main.db").await;
   let (db2, _temp2) = create_test_db("other.db").await;

   for db in [&db1, &db2] {
      db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)".into(), vec![])
         .await
         .unwrap();
   }

   let state = ActiveInterruptibleTransactions::default();
   let main_tx = begin_transaction(&db1, "main").await;
   let main_tx_id = main_tx.transaction_id().to_string();
   let other_tx = begin_transaction(&db2, "other").await;
   let other_tx_id = other_tx.transaction_id().to_string();

   state.insert("main".into(), main_tx).await.unwrap();
   state.insert("other".into(), other_tx).await.unwrap();

   state.abort_for_db("main").await.unwrap();

   let err = expect_err(state.remove("main", &main_tx_id).await);
   assert_eq!(err.error_code(), "NO_ACTIVE_TRANSACTION");

   assert!(state.remove("other", &other_tx_id).await.is_ok());
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
   state.abort_all().await.unwrap();

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
   state.abort_all().await.unwrap();

   // Should be able to insert again after abort
   let tx2 = begin_transaction(&db2, "reuse-key").await;
   state.insert("reuse-key".into(), tx2).await.unwrap();
}

// ============================================================================
// ActiveInterruptibleTransactions timeout tests
// ============================================================================

#[tokio::test]
async fn test_expired_transaction_evicted_on_insert() {
   let (db1, _temp1) = create_test_db("expire1.db").await;
   let (db2, _temp2) = create_test_db("expire2.db").await;

   for db in [&db1, &db2] {
      db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)".into(), vec![])
         .await
         .unwrap();
   }

   // Use a 1ms timeout so the first transaction expires immediately
   let state = ActiveInterruptibleTransactions::new(std::time::Duration::from_millis(1));

   let tx1 = begin_transaction(&db1, "shared-key").await;
   state.insert("shared-key".into(), tx1).await.unwrap();

   // Sleep to ensure the transaction expires
   tokio::time::sleep(std::time::Duration::from_millis(10)).await;

   // Second insert should succeed because the expired transaction is evicted
   let tx2 = begin_transaction(&db2, "shared-key").await;
   state.insert("shared-key".into(), tx2).await.unwrap();
}

#[tokio::test]
async fn test_remove_expired_transaction_returns_timed_out() {
   let (db, _temp) = create_test_db("timeout.db").await;

   db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)".into(), vec![])
      .await
      .unwrap();

   let state = ActiveInterruptibleTransactions::new(std::time::Duration::from_millis(1));

   let tx = begin_transaction(&db, "timeout.db").await;
   let tx_id = tx.transaction_id().to_string();

   state.insert("timeout.db".into(), tx).await.unwrap();

   // Sleep to ensure the transaction expires
   tokio::time::sleep(std::time::Duration::from_millis(10)).await;

   let err = expect_err(state.remove("timeout.db", &tx_id).await);
   assert_eq!(err.error_code(), "TRANSACTION_TIMED_OUT");
}

#[tokio::test]
async fn test_non_expired_transaction_not_evicted() {
   let (db1, _temp1) = create_test_db("live1.db").await;
   let (db2, _temp2) = create_test_db("live2.db").await;

   for db in [&db1, &db2] {
      db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)".into(), vec![])
         .await
         .unwrap();
   }

   // Use a long timeout so the first transaction does NOT expire
   let state = ActiveInterruptibleTransactions::new(std::time::Duration::from_secs(300));

   let tx1 = begin_transaction(&db1, "shared-key").await;
   state.insert("shared-key".into(), tx1).await.unwrap();

   // Second insert should still fail because the first transaction is alive
   let tx2 = begin_transaction(&db2, "shared-key").await;
   let err = state.insert("shared-key".into(), tx2).await.unwrap_err();
   assert_eq!(err.error_code(), "TRANSACTION_ALREADY_ACTIVE");
}

// ============================================================================
// ActiveRegularTransactions tests
// ============================================================================

#[tokio::test]
async fn test_regular_insert_and_remove() {
   let state = ActiveRegularTransactions::default();

   let handle = tokio::spawn(async { /* no-op */ });
   state.insert("main".into(), "tx-1".into(), handle).await;

   // Remove should succeed (no panic, no error)
   state.remove("tx-1").await;

   // Removing again is a no-op
   state.remove("tx-1").await;
}

#[tokio::test]
async fn test_regular_abort_all_cancels_tasks() {
   let state = ActiveRegularTransactions::default();

   let handle = tokio::spawn(async {
      tokio::time::sleep(std::time::Duration::from_secs(60)).await;
   });

   state
      .insert("main".into(), "long-task".into(), handle)
      .await;
   state.abort_all().await.unwrap();
}

#[tokio::test]
async fn test_regular_abort_all_clears_state() {
   let state = ActiveRegularTransactions::default();

   let h1 = tokio::spawn(async {});
   let h2 = tokio::spawn(async {});

   state.insert("main".into(), "a".into(), h1).await;
   state.insert("other".into(), "b".into(), h2).await;

   state.abort_all().await.unwrap();

   // State should be empty — inserting new keys should work
   let h3 = tokio::spawn(async {});
   state.insert("main".into(), "a".into(), h3).await;
}

#[tokio::test]
async fn test_regular_abort_for_db_only_matching_db_key() {
   let state = ActiveRegularTransactions::default();

   let main_handle = tokio::spawn(async {
      tokio::time::sleep(std::time::Duration::from_secs(60)).await;
   });
   let other_handle = tokio::spawn(async {
      tokio::time::sleep(std::time::Duration::from_secs(60)).await;
   });

   state
      .insert("main".into(), "main-one".into(), main_handle)
      .await;
   state
      .insert("other".into(), "other-two".into(), other_handle)
      .await;

   state.abort_for_db("main").await.unwrap();

   // Other database transaction should still be tracked and abortable.
   state.abort_for_db("other").await.unwrap();
}

#[tokio::test]
async fn test_regular_abort_for_db_does_not_match_colon_prefix_alias() {
   let state = ActiveRegularTransactions::default();

   let a_handle = tokio::spawn(async {
      tokio::time::sleep(std::time::Duration::from_secs(60)).await;
   });
   let ab_handle = tokio::spawn(async {
      tokio::time::sleep(std::time::Duration::from_secs(60)).await;
   });

   state.insert("a".into(), "a-tx".into(), a_handle).await;
   state.insert("a:b".into(), "ab-tx".into(), ab_handle).await;

   state.abort_for_db("a").await.unwrap();

   // Closing `a` must not abort the transaction belonging to database `a:b`.
   state.abort_for_db("a:b").await.unwrap();
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
      .insert("cleanup.db".into(), "regular-1".into(), handle)
      .await;

   // Cleanup should clear both
   cleanup_all_transactions(&interruptible, &regular)
      .await
      .unwrap();

   // Interruptible should be empty
   let err = expect_err(interruptible.remove("cleanup.db", "any").await);
   assert_eq!(err.error_code(), "NO_ACTIVE_TRANSACTION");
}
