//! Integration tests for hooks-based SQLite observation.
//!
//! Tests verify:
//! - Transaction semantics: only committed changes publish notifications
//! - CRUD notifications: insert, update, delete each trigger appropriately
//! - Value capture: old/new column values are captured per operation type
//! - Filtering: only observed tables trigger notifications
//! - Multi-subscriber: all subscribers receive notifications

use futures::StreamExt;
use sqlx::SqlitePool;
use sqlx_sqlite_observer::{ChangeOperation, ColumnValue, ObserverConfig, SqliteObserver};
use std::time::Duration;
use tokio::time::timeout;

async fn setup_test_db() -> SqlitePool {
   let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();

   sqlx::query(
      r#"
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL
        )
        "#,
   )
   .execute(&pool)
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
   .execute(&pool)
   .await
   .unwrap();

   pool
}

fn has_text_value(values: &[ColumnValue], expected: &str) -> bool {
   values
      .iter()
      .any(|v| matches!(v, ColumnValue::Text(s) if s == expected))
}

// ============================================================================
// Observer Lifecycle
// ============================================================================

#[tokio::test]
async fn test_observer_starts_with_no_tables() {
   let pool = setup_test_db().await;
   let observer = SqliteObserver::new(pool, ObserverConfig::default());

   assert!(observer.observed_tables().is_empty());
}

#[tokio::test]
async fn test_subscribe_adds_tables_to_observed_set() {
   let pool = setup_test_db().await;
   let observer = SqliteObserver::new(pool, ObserverConfig::default());

   let _rx = observer.subscribe(["users", "posts"]);

   let tables = observer.observed_tables();
   assert_eq!(tables.len(), 2);
   assert!(tables.contains(&"users".to_string()));
   assert!(tables.contains(&"posts".to_string()));
}

#[tokio::test]
async fn test_config_presets_observed_tables() {
   let pool = setup_test_db().await;
   let config = ObserverConfig::new().with_tables(["users"]);
   let observer = SqliteObserver::new(pool, config);

   assert_eq!(observer.observed_tables().len(), 1);
   assert!(observer.observed_tables().contains(&"users".to_string()));
}

// ============================================================================
// Transaction Semantics
// ============================================================================

#[tokio::test]
async fn test_commit_publishes_notification() {
   let pool = setup_test_db().await;
   let config = ObserverConfig::new().with_tables(["users"]);
   let observer = SqliteObserver::new(pool, config);

   let mut rx = observer.subscribe(["users"]);
   let mut conn = observer.acquire().await.unwrap();

   sqlx::query("BEGIN").execute(&mut **conn).await.unwrap();
   sqlx::query("INSERT INTO users (name) VALUES ('Alice')")
      .execute(&mut **conn)
      .await
      .unwrap();

   sqlx::query("COMMIT").execute(&mut **conn).await.unwrap();

   let result = timeout(Duration::from_millis(100), rx.recv()).await;
   assert!(result.is_ok(), "Should receive notification after commit");

   let change = result.unwrap().unwrap();
   assert_eq!(change.table, "users");
   assert_eq!(change.operation, Some(ChangeOperation::Insert));
}

#[tokio::test]
async fn test_rollback_discards_changes() {
   let pool = setup_test_db().await;
   let config = ObserverConfig::new().with_tables(["users"]);
   let observer = SqliteObserver::new(pool, config);

   let mut rx = observer.subscribe(["users"]);
   let mut conn = observer.acquire().await.unwrap();

   sqlx::query("BEGIN").execute(&mut **conn).await.unwrap();
   sqlx::query("INSERT INTO users (name) VALUES ('Alice')")
      .execute(&mut **conn)
      .await
      .unwrap();

   sqlx::query("ROLLBACK").execute(&mut **conn).await.unwrap();

   let result = timeout(Duration::from_millis(50), rx.recv()).await;
   assert!(
      result.is_err(),
      "Should NOT receive notification after rollback"
   );
}

#[tokio::test]
async fn test_multiple_changes_in_transaction() {
   let pool = setup_test_db().await;
   let config = ObserverConfig::new().with_tables(["users"]);
   let observer = SqliteObserver::new(pool, config);

   let mut rx = observer.subscribe(["users"]);
   let mut conn = observer.acquire().await.unwrap();

   sqlx::query("BEGIN").execute(&mut **conn).await.unwrap();
   sqlx::query("INSERT INTO users (name) VALUES ('Alice')")
      .execute(&mut **conn)
      .await
      .unwrap();

   sqlx::query("INSERT INTO users (name) VALUES ('Bob')")
      .execute(&mut **conn)
      .await
      .unwrap();

   sqlx::query("INSERT INTO users (name) VALUES ('Charlie')")
      .execute(&mut **conn)
      .await
      .unwrap();

   sqlx::query("COMMIT").execute(&mut **conn).await.unwrap();

   // Should receive all three notifications
   for expected in ["Alice", "Bob", "Charlie"] {
      let result = timeout(Duration::from_millis(100), rx.recv()).await;
      assert!(
         result.is_ok(),
         "Should receive notification for {}",
         expected
      );
      let change = result.unwrap().unwrap();
      assert!(has_text_value(
         change.new_values.as_ref().unwrap(),
         expected
      ));
   }
}

// ============================================================================
// CRUD Operations
// ============================================================================

#[tokio::test]
async fn test_insert_notification() {
   let pool = setup_test_db().await;
   let config = ObserverConfig::new().with_tables(["users"]);
   let observer = SqliteObserver::new(pool, config);

   let mut rx = observer.subscribe(["users"]);
   let mut conn = observer.acquire().await.unwrap();

   // Implicit transaction (auto-commit)
   sqlx::query("INSERT INTO users (name) VALUES ('Alice')")
      .execute(&mut **conn)
      .await
      .unwrap();

   let change = timeout(Duration::from_millis(100), rx.recv())
      .await
      .unwrap()
      .unwrap();

   assert_eq!(change.table, "users");
   assert_eq!(change.operation, Some(ChangeOperation::Insert));
   assert!(change.rowid.is_some());
   assert!(change.old_values.is_none(), "INSERT has no old_values");
   assert!(change.new_values.is_some(), "INSERT has new_values");
   assert!(has_text_value(change.new_values.as_ref().unwrap(), "Alice"));
}

#[tokio::test]
async fn test_update_notification() {
   let pool = setup_test_db().await;
   let config = ObserverConfig::new().with_tables(["users"]);
   let observer = SqliteObserver::new(pool, config);

   // Seed data
   sqlx::query("INSERT INTO users (name) VALUES ('Alice')")
      .execute(observer.pool())
      .await
      .unwrap();

   let mut rx = observer.subscribe(["users"]);
   let mut conn = observer.acquire().await.unwrap();

   sqlx::query("BEGIN").execute(&mut **conn).await.unwrap();
   sqlx::query("UPDATE users SET name = 'Bob' WHERE id = 1")
      .execute(&mut **conn)
      .await
      .unwrap();

   sqlx::query("COMMIT").execute(&mut **conn).await.unwrap();

   let change = timeout(Duration::from_millis(100), rx.recv())
      .await
      .unwrap()
      .unwrap();

   assert_eq!(change.table, "users");
   assert_eq!(change.operation, Some(ChangeOperation::Update));
   assert!(change.old_values.is_some(), "UPDATE has old_values");
   assert!(change.new_values.is_some(), "UPDATE has new_values");
   assert!(has_text_value(change.old_values.as_ref().unwrap(), "Alice"));
   assert!(has_text_value(change.new_values.as_ref().unwrap(), "Bob"));
}

#[tokio::test]
async fn test_delete_notification() {
   let pool = setup_test_db().await;
   let config = ObserverConfig::new().with_tables(["users"]);
   let observer = SqliteObserver::new(pool, config);

   // Seed data
   sqlx::query("INSERT INTO users (name) VALUES ('Alice')")
      .execute(observer.pool())
      .await
      .unwrap();

   let mut rx = observer.subscribe(["users"]);
   let mut conn = observer.acquire().await.unwrap();

   // Implicit transaction (auto-commit)
   sqlx::query("DELETE FROM users WHERE id = 1")
      .execute(&mut **conn)
      .await
      .unwrap();

   let change = timeout(Duration::from_millis(100), rx.recv())
      .await
      .unwrap()
      .unwrap();

   assert_eq!(change.table, "users");
   assert_eq!(change.operation, Some(ChangeOperation::Delete));
   assert!(change.old_values.is_some(), "DELETE has old_values");
   assert!(change.new_values.is_none(), "DELETE has no new_values");
   assert!(has_text_value(change.old_values.as_ref().unwrap(), "Alice"));
}

// ============================================================================
// Filtering
// ============================================================================

#[tokio::test]
async fn test_untracked_table_ignored() {
   let pool = setup_test_db().await;
   let config = ObserverConfig::new().with_tables(["users"]); // Only users, not posts
   let observer = SqliteObserver::new(pool, config);

   // Seed user for foreign key
   sqlx::query("INSERT INTO users (name) VALUES ('Alice')")
      .execute(observer.pool())
      .await
      .unwrap();

   let mut rx = observer.subscribe(["users"]);
   let mut conn = observer.acquire().await.unwrap();

   sqlx::query("BEGIN").execute(&mut **conn).await.unwrap();
   sqlx::query("INSERT INTO posts (user_id, title) VALUES (1, 'Hello')")
      .execute(&mut **conn)
      .await
      .unwrap();

   sqlx::query("COMMIT").execute(&mut **conn).await.unwrap();

   let result = timeout(Duration::from_millis(50), rx.recv()).await;
   assert!(result.is_err(), "Should NOT notify for untracked table");
}

// ============================================================================
// Multi-Subscriber & Clone
// ============================================================================

#[tokio::test]
async fn test_all_subscribers_receive_notification() {
   let pool = setup_test_db().await;
   let config = ObserverConfig::new().with_tables(["users"]);
   let observer = SqliteObserver::new(pool, config);

   let mut rx1 = observer.subscribe(["users"]);
   let mut rx2 = observer.subscribe(["users"]);

   let mut conn = observer.acquire().await.unwrap();

   sqlx::query("BEGIN").execute(&mut **conn).await.unwrap();
   sqlx::query("INSERT INTO users (name) VALUES ('Alice')")
      .execute(&mut **conn)
      .await
      .unwrap();

   sqlx::query("COMMIT").execute(&mut **conn).await.unwrap();

   let result1 = timeout(Duration::from_millis(100), rx1.recv()).await;
   let result2 = timeout(Duration::from_millis(100), rx2.recv()).await;

   assert!(result1.is_ok(), "Subscriber 1 receives notification");
   assert!(result2.is_ok(), "Subscriber 2 receives notification");
}

#[tokio::test]
async fn test_cloned_observer_shares_state() {
   let pool = setup_test_db().await;
   let config = ObserverConfig::new().with_tables(["users"]);
   let observer1 = SqliteObserver::new(pool, config);
   let observer2 = observer1.clone();

   // Subscribe on original, write through clone
   let mut rx = observer1.subscribe(["users"]);
   let mut conn = observer2.acquire().await.unwrap();

   sqlx::query("BEGIN").execute(&mut **conn).await.unwrap();
   sqlx::query("INSERT INTO users (name) VALUES ('Alice')")
      .execute(&mut **conn)
      .await
      .unwrap();

   sqlx::query("COMMIT").execute(&mut **conn).await.unwrap();

   let result = timeout(Duration::from_millis(100), rx.recv()).await;
   assert!(result.is_ok(), "Receives notification through clone");
}

// ============================================================================
// Stream API
// ============================================================================

#[tokio::test]
async fn test_stream_receives_notifications() {
   let pool = setup_test_db().await;
   let config = ObserverConfig::new().with_tables(["users"]);
   let observer = SqliteObserver::new(pool, config);

   let mut stream = observer.subscribe_stream(["users"]);
   let mut conn = observer.acquire().await.unwrap();

   sqlx::query("BEGIN").execute(&mut **conn).await.unwrap();
   sqlx::query("INSERT INTO users (name) VALUES ('Alice')")
      .execute(&mut **conn)
      .await
      .unwrap();

   sqlx::query("COMMIT").execute(&mut **conn).await.unwrap();

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

#[tokio::test]
async fn test_stream_filters_tables() {
   let pool = setup_test_db().await;
   let config = ObserverConfig::new().with_tables(["users", "posts"]);
   let observer = SqliteObserver::new(pool, config);

   // Seed user for foreign key
   sqlx::query("INSERT INTO users (name) VALUES ('Alice')")
      .execute(observer.pool())
      .await
      .unwrap();

   // Subscribe only to users, not posts
   let mut stream = observer.subscribe_stream(["users"]);
   let mut conn = observer.acquire().await.unwrap();

   // Insert into posts (should be filtered out by stream)
   sqlx::query("BEGIN").execute(&mut **conn).await.unwrap();
   sqlx::query("INSERT INTO posts (user_id, title) VALUES (1, 'Hello')")
      .execute(&mut **conn)
      .await
      .unwrap();

   sqlx::query("COMMIT").execute(&mut **conn).await.unwrap();

   let result = timeout(Duration::from_millis(50), stream.next()).await;
   assert!(result.is_err(), "Stream filters out non-subscribed tables");
}

#[tokio::test]
async fn test_stream_lag_when_capacity_exceeded() {
   let pool = setup_test_db().await;
   let config = ObserverConfig::new()
      .with_tables(["users"])
      .with_channel_capacity(2);

   let observer = SqliteObserver::new(pool, config);

   let mut stream = observer.subscribe_stream(["users"]);
   let mut conn = observer.acquire().await.unwrap();

   // Insert more rows than the channel capacity in a single transaction
   sqlx::query("BEGIN").execute(&mut **conn).await.unwrap();
   for i in 0..5 {
      sqlx::query("INSERT INTO users (name) VALUES (?)")
         .bind(format!("User{}", i))
         .execute(&mut **conn)
         .await
         .unwrap();
   }
   sqlx::query("COMMIT").execute(&mut **conn).await.unwrap();

   let mut saw_lagged = false;
   let mut saw_change = false;

   // Drain all available events
   while let Ok(Some(event)) = timeout(Duration::from_millis(100), stream.next()).await {
      match event {
         sqlx_sqlite_observer::TableChangeEvent::Lagged(n) => {
            assert!(n > 0, "Lagged count should be > 0");
            saw_lagged = true;
         }
         sqlx_sqlite_observer::TableChangeEvent::Change(change) => {
            assert_eq!(change.table, "users");
            saw_change = true;
         }
      }
   }

   assert!(saw_lagged, "Expected at least one Lagged event");
   assert!(saw_change, "Expected at least one Change event");
}

// ============================================================================
// Value Capture
// ============================================================================

#[tokio::test]
async fn test_column_value_types() {
   let pool = setup_test_db().await;
   let config = ObserverConfig::new().with_tables(["users"]);
   let observer = SqliteObserver::new(pool, config);

   let mut rx = observer.subscribe(["users"]);
   let mut conn = observer.acquire().await.unwrap();

   sqlx::query("BEGIN").execute(&mut **conn).await.unwrap();
   sqlx::query("INSERT INTO users (name) VALUES ('TestUser')")
      .execute(&mut **conn)
      .await
      .unwrap();

   sqlx::query("COMMIT").execute(&mut **conn).await.unwrap();

   let change = timeout(Duration::from_millis(100), rx.recv())
      .await
      .unwrap()
      .unwrap();

   let values = change.new_values.unwrap();

   let has_integer = values.iter().any(|v| matches!(v, ColumnValue::Integer(_)));
   let has_text = values.iter().any(|v| matches!(v, ColumnValue::Text(_)));

   assert!(has_integer, "Should capture Integer (id column)");
   assert!(has_text, "Should capture Text (name column)");
}

#[tokio::test]
async fn test_capture_values_disabled() {
   let pool = setup_test_db().await;
   let config = ObserverConfig::new()
      .with_tables(["users"])
      .with_capture_values(false);

   let observer = SqliteObserver::new(pool, config);

   let mut rx = observer.subscribe(["users"]);
   let mut conn = observer.acquire().await.unwrap();

   sqlx::query("BEGIN").execute(&mut **conn).await.unwrap();
   sqlx::query("INSERT INTO users (name) VALUES ('Alice')")
      .execute(&mut **conn)
      .await
      .unwrap();

   sqlx::query("COMMIT").execute(&mut **conn).await.unwrap();

   let change = timeout(Duration::from_millis(100), rx.recv())
      .await
      .unwrap()
      .unwrap();

   // With capture_values=false, we still get table/operation/rowid but no values
   assert_eq!(change.table, "users");
   assert_eq!(change.operation, Some(ChangeOperation::Insert));
   assert!(change.rowid.is_some());
   assert!(
      change.old_values.is_none(),
      "No values when capture disabled"
   );
   assert!(
      change.new_values.is_none(),
      "No values when capture disabled"
   );
}

// ============================================================================
// Primary Key Extraction
// ============================================================================

#[tokio::test]
async fn test_single_column_primary_key() {
   let pool = setup_test_db().await;
   let config = ObserverConfig::new().with_tables(["users"]);
   let observer = SqliteObserver::new(pool, config);

   let mut rx = observer.subscribe(["users"]);
   let mut conn = observer.acquire().await.unwrap();

   sqlx::query("INSERT INTO users (name) VALUES ('Alice')")
      .execute(&mut **conn)
      .await
      .unwrap();

   let change = timeout(Duration::from_millis(100), rx.recv())
      .await
      .unwrap()
      .unwrap();

   assert_eq!(change.table, "users");
   assert!(!change.primary_key.is_empty(), "Should have primary key");
   assert_eq!(change.primary_key.len(), 1, "Single-column PK");

   // The PK should be the auto-incremented id (1)
   assert_eq!(
      change.primary_key[0],
      ColumnValue::Integer(1),
      "PK should be id=1"
   );
}

#[tokio::test]
async fn test_composite_primary_key() {
   let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();

   // Create a table with a composite primary key
   sqlx::query(
      r#"
        CREATE TABLE user_roles (
            user_id INTEGER NOT NULL,
            role_id INTEGER NOT NULL,
            granted_at TEXT,
            PRIMARY KEY (user_id, role_id)
        )
        "#,
   )
   .execute(&pool)
   .await
   .unwrap();

   let config = ObserverConfig::new().with_tables(["user_roles"]);
   let observer = SqliteObserver::new(pool, config);

   let mut rx = observer.subscribe(["user_roles"]);
   let mut conn = observer.acquire().await.unwrap();

   sqlx::query(
      "INSERT INTO user_roles (user_id, role_id, granted_at) VALUES (42, 7, '2024-01-01')",
   )
   .execute(&mut **conn)
   .await
   .unwrap();

   let change = timeout(Duration::from_millis(100), rx.recv())
      .await
      .unwrap()
      .unwrap();

   assert_eq!(change.table, "user_roles");
   assert_eq!(change.primary_key.len(), 2, "Composite PK has 2 columns");

   // PK columns should be in declaration order: (user_id, role_id)
   assert_eq!(
      change.primary_key[0],
      ColumnValue::Integer(42),
      "First PK column is user_id=42"
   );
   assert_eq!(
      change.primary_key[1],
      ColumnValue::Integer(7),
      "Second PK column is role_id=7"
   );
}

#[tokio::test]
async fn test_text_primary_key() {
   let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();

   // Create a table with a TEXT primary key
   sqlx::query(
      r#"
        CREATE TABLE settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        "#,
   )
   .execute(&pool)
   .await
   .unwrap();

   let config = ObserverConfig::new().with_tables(["settings"]);
   let observer = SqliteObserver::new(pool, config);

   let mut rx = observer.subscribe(["settings"]);
   let mut conn = observer.acquire().await.unwrap();

   sqlx::query("INSERT INTO settings (key, value) VALUES ('theme', 'dark')")
      .execute(&mut **conn)
      .await
      .unwrap();

   let change = timeout(Duration::from_millis(100), rx.recv())
      .await
      .unwrap()
      .unwrap();

   assert_eq!(change.table, "settings");
   assert_eq!(change.primary_key.len(), 1, "Single TEXT PK");
   assert_eq!(
      change.primary_key[0],
      ColumnValue::Text("theme".to_string()),
      "PK should be key='theme'"
   );
}

#[tokio::test]
async fn test_without_rowid_table() {
   let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();

   // Create a WITHOUT ROWID table
   sqlx::query(
      r#"
        CREATE TABLE kv_store (
            key TEXT PRIMARY KEY,
            value BLOB
        ) WITHOUT ROWID
        "#,
   )
   .execute(&pool)
   .await
   .unwrap();

   let config = ObserverConfig::new().with_tables(["kv_store"]);
   let observer = SqliteObserver::new(pool, config);

   let mut rx = observer.subscribe(["kv_store"]);
   let mut conn = observer.acquire().await.unwrap();

   sqlx::query("INSERT INTO kv_store (key, value) VALUES ('mykey', X'DEADBEEF')")
      .execute(&mut **conn)
      .await
      .unwrap();

   let change = timeout(Duration::from_millis(100), rx.recv())
      .await
      .unwrap()
      .unwrap();

   assert_eq!(change.table, "kv_store");

   // For WITHOUT ROWID tables, rowid should be None
   assert!(
      change.rowid.is_none(),
      "WITHOUT ROWID table should have rowid=None"
   );

   // But primary_key should still be populated
   assert_eq!(change.primary_key.len(), 1);
   assert_eq!(
      change.primary_key[0],
      ColumnValue::Text("mykey".to_string()),
      "PK should be key='mykey'"
   );
}

#[tokio::test]
async fn test_delete_returns_old_primary_key() {
   let pool = setup_test_db().await;

   // Seed data
   sqlx::query("INSERT INTO users (name) VALUES ('Alice')")
      .execute(&pool)
      .await
      .unwrap();

   let config = ObserverConfig::new().with_tables(["users"]);
   let observer = SqliteObserver::new(pool, config);

   let mut rx = observer.subscribe(["users"]);
   let mut conn = observer.acquire().await.unwrap();

   sqlx::query("DELETE FROM users WHERE id = 1")
      .execute(&mut **conn)
      .await
      .unwrap();

   let change = timeout(Duration::from_millis(100), rx.recv())
      .await
      .unwrap()
      .unwrap();

   assert_eq!(change.operation, Some(ChangeOperation::Delete));

   // For DELETE, primary_key should contain the OLD key values
   assert_eq!(change.primary_key.len(), 1);
   assert_eq!(
      change.primary_key[0],
      ColumnValue::Integer(1),
      "DELETE should return old PK value"
   );
}
