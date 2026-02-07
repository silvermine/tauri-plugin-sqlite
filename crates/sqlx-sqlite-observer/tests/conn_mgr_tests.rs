//! Integration tests for conn-mgr feature (sqlx-sqlite-conn-mgr integration).
//!
//! Tests verify the same behaviors as integration_tests.rs but using
//! `ObservableSqliteDatabase` instead of `SqliteObserver`.
//!
//! Run with: cargo test --features conn-mgr

#![cfg(feature = "conn-mgr")]

use futures::StreamExt;
use sqlx_sqlite_conn_mgr::SqliteDatabase;
use sqlx_sqlite_observer::{ChangeOperation, ObservableSqliteDatabase, ObserverConfig};
use std::time::Duration;
use tokio::time::timeout;

struct TestDb {
   db: std::sync::Arc<SqliteDatabase>,
   _temp_file: tempfile::NamedTempFile,
}

async fn setup_test_db() -> TestDb {
   // Use temp file so read pool and writer share the same database
   let temp_file = tempfile::NamedTempFile::new().unwrap();
   let db = SqliteDatabase::connect(temp_file.path().to_str().unwrap(), None)
      .await
      .unwrap();

   // Create test tables using writer
   let mut writer = db.acquire_writer().await.unwrap();
   sqlx::query(
      r#"
      CREATE TABLE users (
         id INTEGER PRIMARY KEY AUTOINCREMENT,
         name TEXT NOT NULL
      )
      "#,
   )
   .execute(&mut *writer)
   .await
   .unwrap();

   sqlx::query(
      r#"
      CREATE TABLE posts (
         id INTEGER PRIMARY KEY AUTOINCREMENT,
         user_id INTEGER NOT NULL,
         title TEXT NOT NULL,
         FOREIGN KEY (user_id) REFERENCES users(id)
      )
      "#,
   )
   .execute(&mut *writer)
   .await
   .unwrap();

   drop(writer);

   TestDb {
      db,
      _temp_file: temp_file,
   }
}

// ============================================================================
// Observable Lifecycle
// ============================================================================

#[tokio::test]
async fn test_observable_starts_with_configured_tables() {
   let test_db = setup_test_db().await;
   let config = ObserverConfig::new().with_tables(["users"]);
   let observable = ObservableSqliteDatabase::new(test_db.db, config);

   assert_eq!(observable.observed_tables().len(), 1);
   assert!(observable.observed_tables().contains(&"users".to_string()));
}

// ============================================================================
// Transaction Semantics
// ============================================================================

#[tokio::test]
async fn test_commit_publishes_notification() {
   let test_db = setup_test_db().await;
   let config = ObserverConfig::new().with_tables(["users"]);
   let observable = ObservableSqliteDatabase::new(test_db.db.clone(), config);

   let mut rx = observable.subscribe(["users"]);
   let mut writer = observable.acquire_writer().await.unwrap();

   sqlx::query("BEGIN").execute(&mut *writer).await.unwrap();
   sqlx::query("INSERT INTO users (name) VALUES ('Alice')")
      .execute(&mut *writer)
      .await
      .unwrap();

   sqlx::query("COMMIT").execute(&mut *writer).await.unwrap();

   let result = timeout(Duration::from_millis(100), rx.recv()).await;
   assert!(result.is_ok(), "Should receive notification after commit");

   let change = result.unwrap().unwrap();
   assert_eq!(change.table, "users");
   assert_eq!(change.operation, Some(ChangeOperation::Insert));
}

#[tokio::test]
async fn test_uncommitted_changes_not_published() {
   let test_db = setup_test_db().await;
   let config = ObserverConfig::new().with_tables(["users"]);
   let observable = ObservableSqliteDatabase::new(test_db.db.clone(), config);

   let mut rx = observable.subscribe(["users"]);

   {
      let mut writer = observable.acquire_writer().await.unwrap();
      sqlx::query("BEGIN").execute(&mut *writer).await.unwrap();
      sqlx::query("INSERT INTO users (name) VALUES ('Bob')")
         .execute(&mut *writer)
         .await
         .unwrap();
      // No COMMIT - implicit rollback on drop
   }

   tokio::time::sleep(Duration::from_millis(50)).await;

   let result = timeout(Duration::from_millis(50), rx.recv()).await;
   assert!(result.is_err(), "Should NOT notify for uncommitted changes");
}

#[tokio::test]
async fn test_rollback_discards_changes() {
   let test_db = setup_test_db().await;
   let config = ObserverConfig::new().with_tables(["users"]);
   let observable = ObservableSqliteDatabase::new(test_db.db.clone(), config);

   let mut rx = observable.subscribe(["users"]);
   let mut writer = observable.acquire_writer().await.unwrap();

   sqlx::query("BEGIN").execute(&mut *writer).await.unwrap();
   sqlx::query("INSERT INTO users (name) VALUES ('Charlie')")
      .execute(&mut *writer)
      .await
      .unwrap();

   sqlx::query("ROLLBACK").execute(&mut *writer).await.unwrap();

   tokio::time::sleep(Duration::from_millis(50)).await;

   let result = timeout(Duration::from_millis(50), rx.recv()).await;
   assert!(result.is_err(), "Should NOT notify for rolled-back changes");
}

// ============================================================================
// CRUD Operations
// ============================================================================

#[tokio::test]
async fn test_update_notification() {
   let test_db = setup_test_db().await;
   let config = ObserverConfig::new().with_tables(["users"]);
   let observable = ObservableSqliteDatabase::new(test_db.db.clone(), config);

   // Seed data
   let mut writer = observable.acquire_writer().await.unwrap();
   sqlx::query("INSERT INTO users (name) VALUES ('Alice')")
      .execute(&mut *writer)
      .await
      .unwrap();

   drop(writer);

   let mut rx = observable.subscribe(["users"]);
   let mut writer = observable.acquire_writer().await.unwrap();

   sqlx::query("BEGIN").execute(&mut *writer).await.unwrap();
   sqlx::query("UPDATE users SET name = 'Bob' WHERE id = 1")
      .execute(&mut *writer)
      .await
      .unwrap();

   sqlx::query("COMMIT").execute(&mut *writer).await.unwrap();

   let change = timeout(Duration::from_millis(100), rx.recv())
      .await
      .unwrap()
      .unwrap();

   assert_eq!(change.table, "users");
   assert_eq!(change.operation, Some(ChangeOperation::Update));
}

#[tokio::test]
async fn test_delete_notification() {
   let test_db = setup_test_db().await;
   let config = ObserverConfig::new().with_tables(["users"]);
   let observable = ObservableSqliteDatabase::new(test_db.db.clone(), config);

   // Seed data
   let mut writer = observable.acquire_writer().await.unwrap();
   sqlx::query("INSERT INTO users (name) VALUES ('Alice')")
      .execute(&mut *writer)
      .await
      .unwrap();

   drop(writer);

   let mut rx = observable.subscribe(["users"]);
   let mut writer = observable.acquire_writer().await.unwrap();

   sqlx::query("BEGIN").execute(&mut *writer).await.unwrap();
   sqlx::query("DELETE FROM users WHERE id = 1")
      .execute(&mut *writer)
      .await
      .unwrap();

   sqlx::query("COMMIT").execute(&mut *writer).await.unwrap();

   let change = timeout(Duration::from_millis(100), rx.recv())
      .await
      .unwrap()
      .unwrap();

   assert_eq!(change.table, "users");
   assert_eq!(change.operation, Some(ChangeOperation::Delete));
}

// ============================================================================
// Read Pool
// ============================================================================

#[tokio::test]
async fn test_read_pool_sees_committed_writes() {
   let test_db = setup_test_db().await;
   let config = ObserverConfig::new().with_tables(["users"]);
   let observable = ObservableSqliteDatabase::new(test_db.db.clone(), config);

   // Insert via writer
   let mut writer = observable.acquire_writer().await.unwrap();
   sqlx::query("INSERT INTO users (name) VALUES ('Diana')")
      .execute(&mut *writer)
      .await
      .unwrap();

   drop(writer);

   // Read via read_pool
   let rows: Vec<(i64, String)> = sqlx::query_as("SELECT id, name FROM users")
      .fetch_all(observable.read_pool().unwrap())
      .await
      .unwrap();

   assert_eq!(rows.len(), 1);
   assert_eq!(rows[0].1, "Diana");
}

// ============================================================================
// Multi-Subscriber & Clone
// ============================================================================

#[tokio::test]
async fn test_all_subscribers_receive_notification() {
   let test_db = setup_test_db().await;
   let config = ObserverConfig::new().with_tables(["users"]);
   let observable = ObservableSqliteDatabase::new(test_db.db.clone(), config);

   let mut rx1 = observable.subscribe(["users"]);
   let mut rx2 = observable.subscribe(["users"]);

   let mut writer = observable.acquire_writer().await.unwrap();

   sqlx::query("BEGIN").execute(&mut *writer).await.unwrap();
   sqlx::query("INSERT INTO users (name) VALUES ('Alice')")
      .execute(&mut *writer)
      .await
      .unwrap();

   sqlx::query("COMMIT").execute(&mut *writer).await.unwrap();

   let result1 = timeout(Duration::from_millis(100), rx1.recv()).await;
   let result2 = timeout(Duration::from_millis(100), rx2.recv()).await;

   assert!(result1.is_ok(), "Subscriber 1 receives notification");
   assert!(result2.is_ok(), "Subscriber 2 receives notification");
}

#[tokio::test]
async fn test_cloned_observable_shares_state() {
   let test_db = setup_test_db().await;
   let config = ObserverConfig::new().with_tables(["users"]);
   let observable1 = ObservableSqliteDatabase::new(test_db.db.clone(), config);
   let observable2 = observable1.clone();

   // Subscribe on original, write through clone
   let mut rx = observable1.subscribe(["users"]);
   let mut writer = observable2.acquire_writer().await.unwrap();

   sqlx::query("BEGIN").execute(&mut *writer).await.unwrap();
   sqlx::query("INSERT INTO users (name) VALUES ('Frank')")
      .execute(&mut *writer)
      .await
      .unwrap();

   sqlx::query("COMMIT").execute(&mut *writer).await.unwrap();

   let result = timeout(Duration::from_millis(100), rx.recv()).await;
   assert!(result.is_ok(), "Receives notification through clone");
}

// ============================================================================
// Stream API
// ============================================================================

#[tokio::test]
async fn test_stream_receives_notifications() {
   let test_db = setup_test_db().await;
   let config = ObserverConfig::new().with_tables(["users"]);
   let observable = ObservableSqliteDatabase::new(test_db.db.clone(), config);

   let mut stream = observable.subscribe_stream(["users"]);
   let mut writer = observable.acquire_writer().await.unwrap();

   sqlx::query("BEGIN").execute(&mut *writer).await.unwrap();
   sqlx::query("INSERT INTO users (name) VALUES ('Eve')")
      .execute(&mut *writer)
      .await
      .unwrap();

   sqlx::query("COMMIT").execute(&mut *writer).await.unwrap();

   let result = timeout(Duration::from_millis(100), stream.next()).await;
   assert!(result.is_ok(), "Stream receives notification");

   let event = result.unwrap().unwrap();
   match event {
      sqlx_sqlite_observer::TableChangeEvent::Change(change) => {
         assert_eq!(change.table, "users");
      }
      sqlx_sqlite_observer::TableChangeEvent::Lagged(_) => {
         panic!("Expected Change event, got Lagged");
      }
   }
}
