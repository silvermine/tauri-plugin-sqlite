//! Integration tests for conn-mgr feature (sqlx-sqlite-conn-mgr integration).
//!
//! Tests verify the same behaviors as integration_tests.rs but using
//! `ObservableSqliteDatabase` instead of `SqliteObserver`. Also covers issue
//! #53's attached-database routing: writes into an attached database publish
//! to the broker of whichever database *owns* the affected table, not
//! necessarily the database the write was issued through.
//!
//! Run with: cargo test --features conn-mgr

#![cfg(feature = "conn-mgr")]

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use sqlx_sqlite_conn_mgr::{AttachedMode, AttachedSpec, SqliteDatabase, SqliteDatabaseConfig};
use sqlx_sqlite_observer::{ChangeOperation, ObservableSqliteDatabase, ObserverConfig};
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

// ============================================================================
// Attached-database routing (issue #53)
// ============================================================================

/// Creates a file-backed `SqliteDatabase` with the given DDL already applied.
///
/// A real file (not `:memory:`) is required here because `ATTACH DATABASE`
/// needs a path the attaching connection can open - attaching `:memory:`
/// creates a brand new, unrelated anonymous database, not a second handle
/// onto the caller's existing one.
async fn create_attachable_db(
   temp_file: &tempfile::NamedTempFile,
   create_table_sql: &str,
) -> Arc<SqliteDatabase> {
   let db = SqliteDatabase::connect(temp_file.path().to_str().unwrap(), None)
      .await
      .unwrap();
   let mut writer = db.acquire_writer().await.unwrap();
   sqlx::query(sqlx::AssertSqlSafe(create_table_sql.to_string()))
      .execute(&mut *writer)
      .await
      .unwrap();
   drop(writer);
   db
}

/// Registers `observable`'s broker in `db`'s observer slot, exactly as
/// `sqlx_sqlite_toolkit::DatabaseWrapper::enable_observation` does.
///
/// `acquire_writer_with_attached` discovers an attached database's broker by
/// reading this slot (it has no other way to reach one it wasn't directly
/// handed), so a test exercising that discovery has to set it up the same way
/// production code does rather than passing the broker in some more direct
/// way that wouldn't exercise the real lookup path.
fn register_as_observed(db: &Arc<SqliteDatabase>, observable: &ObservableSqliteDatabase) {
   let broker = Arc::clone(observable.broker());
   db.observer_slot()
      .get_or_init(|| broker)
      .expect("slot must not already hold a value of some other type");
}

#[tokio::test]
async fn attached_write_notifies_owning_database() {
   let temp_a = tempfile::NamedTempFile::new().unwrap();
   let temp_b = tempfile::NamedTempFile::new().unwrap();
   let db_a = create_attachable_db(
      &temp_a,
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
   )
   .await;
   let db_b = create_attachable_db(
      &temp_b,
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
   )
   .await;

   let observable_a =
      ObservableSqliteDatabase::new(db_a, ObserverConfig::new().with_tables(["users"]));
   let observable_b =
      ObservableSqliteDatabase::new(db_b.clone(), ObserverConfig::new().with_tables(["users"]));
   register_as_observed(&db_b, &observable_b);

   let mut rx_a = observable_a.subscribe(["users"]);
   let mut rx_b = observable_b.subscribe(["users"]);

   let specs = vec![AttachedSpec {
      database: db_b,
      schema_name: "other".to_string(),
      mode: AttachedMode::ReadWrite,
   }];

   let mut writer = observable_a
      .acquire_writer_with_attached(specs)
      .await
      .unwrap();
   sqlx::query("BEGIN").execute(&mut *writer).await.unwrap();
   sqlx::query("INSERT INTO other.users (name) VALUES ('Bob')")
      .execute(&mut *writer)
      .await
      .unwrap();
   sqlx::query("COMMIT").execute(&mut *writer).await.unwrap();
   writer.detach_all().await.unwrap();

   let change = timeout(Duration::from_millis(200), rx_b.recv())
      .await
      .expect("should not time out")
      .expect("B's subscriber should receive the change");
   assert_eq!(change.table, "users");
   assert_eq!(change.schema, "other");

   let a_result = timeout(Duration::from_millis(100), rx_a.recv()).await;
   assert!(
      a_result.is_err(),
      "A's subscriber must not see a write that landed in B's table"
   );
}

#[tokio::test]
async fn attached_write_to_unobserved_database_is_dropped() {
   let temp_a = tempfile::NamedTempFile::new().unwrap();
   let temp_b = tempfile::NamedTempFile::new().unwrap();
   let db_a = create_attachable_db(
      &temp_a,
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
   )
   .await;
   // db_b is deliberately never wrapped in an ObservableSqliteDatabase or
   // registered in its observer slot - it has no broker of its own.
   let db_b = create_attachable_db(
      &temp_b,
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
   )
   .await;

   let observable_a =
      ObservableSqliteDatabase::new(db_a, ObserverConfig::new().with_tables(["users"]));
   let mut rx_a = observable_a.subscribe(["users"]);

   let specs = vec![AttachedSpec {
      database: db_b,
      schema_name: "other".to_string(),
      mode: AttachedMode::ReadWrite,
   }];

   let mut writer = observable_a
      .acquire_writer_with_attached(specs)
      .await
      .unwrap();
   sqlx::query("BEGIN").execute(&mut *writer).await.unwrap();
   sqlx::query("INSERT INTO other.users (name) VALUES ('Ghost')")
      .execute(&mut *writer)
      .await
      .unwrap();
   // Must not panic even though there's no broker for "other" to publish to.
   sqlx::query("COMMIT").execute(&mut *writer).await.unwrap();
   writer.detach_all().await.unwrap();

   let result = timeout(Duration::from_millis(100), rx_a.recv()).await;
   assert!(
      result.is_err(),
      "a write into an unobserved attached database must not surface anywhere, \
       including on the attaching database's own broker"
   );
}

/// Pins today's `ReadOnly` contract, which the test above does not cover (it
/// exercises `ReadWrite` + unobserved): the write *succeeds*, since nothing asks
/// SQLite to refuse it, and the deliberate skip in
/// `acquire_writer_with_attached` keeps `other`'s broker out of the hook map so
/// its own subscriber never hears about it.
///
/// Asserting both halves means a future change that enforces `ReadOnly` has to
/// update this test deliberately rather than by accident.
#[tokio::test]
async fn readonly_attachment_write_lands_and_is_not_observed() {
   let temp_a = tempfile::NamedTempFile::new().unwrap();
   let temp_b = tempfile::NamedTempFile::new().unwrap();
   let db_a = create_attachable_db(
      &temp_a,
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
   )
   .await;
   // Observed, unlike the unobserved-attachment test: the point here is that
   // the skip - not a missing broker - is what suppresses the notification.
   let db_b = create_attachable_db(
      &temp_b,
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
   )
   .await;

   let observable_a =
      ObservableSqliteDatabase::new(db_a, ObserverConfig::new().with_tables(["users"]));
   let observable_b =
      ObservableSqliteDatabase::new(db_b.clone(), ObserverConfig::new().with_tables(["users"]));
   register_as_observed(&db_b, &observable_b);

   let mut rx_a = observable_a.subscribe(["users"]);
   let mut rx_b = observable_b.subscribe(["users"]);

   let specs = vec![AttachedSpec {
      database: Arc::clone(&db_b),
      schema_name: "other".to_string(),
      mode: AttachedMode::ReadOnly,
   }];

   let mut writer = observable_a
      .acquire_writer_with_attached(specs)
      .await
      .unwrap();
   sqlx::query("BEGIN").execute(&mut *writer).await.unwrap();
   sqlx::query("INSERT INTO other.users (name) VALUES ('Written anyway')")
      .execute(&mut *writer)
      .await
      .expect("SQLite is not asked to enforce ReadOnly, so this write succeeds");
   sqlx::query("COMMIT").execute(&mut *writer).await.unwrap();
   writer.detach_all().await.unwrap();

   // The row really is there - this is the part that makes the silence matter.
   let mut reader = db_b.read_pool().unwrap().acquire().await.unwrap();
   let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
      .fetch_one(&mut *reader)
      .await
      .unwrap();
   assert_eq!(count, 1, "the ReadOnly attachment was written through");

   let b_result = timeout(Duration::from_millis(100), rx_b.recv()).await;
   assert!(
      b_result.is_err(),
      "a write through a ReadOnly attachment is not observed, even though the \
       attached database has observation enabled"
   );
   let a_result = timeout(Duration::from_millis(100), rx_a.recv()).await;
   assert!(
      a_result.is_err(),
      "and it must not be misrouted to the attaching database's broker either"
   );
}

#[tokio::test]
async fn mixed_transaction_publishes_to_both_brokers() {
   let temp_a = tempfile::NamedTempFile::new().unwrap();
   let temp_b = tempfile::NamedTempFile::new().unwrap();
   let db_a = create_attachable_db(
      &temp_a,
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
   )
   .await;
   let db_b = create_attachable_db(
      &temp_b,
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
   )
   .await;

   let observable_a =
      ObservableSqliteDatabase::new(db_a.clone(), ObserverConfig::new().with_tables(["users"]));
   let observable_b =
      ObservableSqliteDatabase::new(db_b.clone(), ObserverConfig::new().with_tables(["users"]));
   register_as_observed(&db_b, &observable_b);

   let mut rx_a = observable_a.subscribe(["users"]);
   let mut rx_b = observable_b.subscribe(["users"]);

   let specs = vec![AttachedSpec {
      database: db_b,
      schema_name: "other".to_string(),
      mode: AttachedMode::ReadWrite,
   }];

   // sqlite3_commit_hook's callback takes no schema argument and fires exactly
   // once for the whole transaction, regardless of how many schemas it
   // touched - this is the test that proves the commit hook fans out to every
   // broker instead of only the one for whichever schema happened to be
   // touched last.
   let mut writer = observable_a
      .acquire_writer_with_attached(specs)
      .await
      .unwrap();
   sqlx::query("BEGIN").execute(&mut *writer).await.unwrap();
   sqlx::query("INSERT INTO main.users (name) VALUES ('MainRow')")
      .execute(&mut *writer)
      .await
      .unwrap();
   sqlx::query("INSERT INTO other.users (name) VALUES ('OtherRow')")
      .execute(&mut *writer)
      .await
      .unwrap();
   sqlx::query("COMMIT").execute(&mut *writer).await.unwrap();
   writer.detach_all().await.unwrap();

   let change_a = timeout(Duration::from_millis(200), rx_a.recv())
      .await
      .expect("should not time out")
      .expect("A's subscriber should receive its own change");
   assert_eq!(change_a.schema, "main");
   assert!(
      change_a
         .new_values
         .expect("capture_values defaults to true")
         .iter()
         .filter_map(|v| v.as_text())
         .any(|s| s == "MainRow")
   );

   let change_b = timeout(Duration::from_millis(200), rx_b.recv())
      .await
      .expect("should not time out")
      .expect("B's subscriber should receive its own change");
   assert_eq!(change_b.schema, "other");
   assert!(
      change_b
         .new_values
         .expect("capture_values defaults to true")
         .iter()
         .filter_map(|v| v.as_text())
         .any(|s| s == "OtherRow")
   );

   assert!(
      timeout(Duration::from_millis(100), rx_a.recv())
         .await
         .is_err(),
      "A must not also receive B's change"
   );
   assert!(
      timeout(Duration::from_millis(100), rx_b.recv())
         .await
         .is_err(),
      "B must not also receive A's change"
   );
}

#[tokio::test]
async fn mixed_transaction_rollback_discards_both_brokers_buffers() {
   let temp_a = tempfile::NamedTempFile::new().unwrap();
   let temp_b = tempfile::NamedTempFile::new().unwrap();
   let db_a = create_attachable_db(
      &temp_a,
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
   )
   .await;
   let db_b = create_attachable_db(
      &temp_b,
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
   )
   .await;

   let observable_a =
      ObservableSqliteDatabase::new(db_a.clone(), ObserverConfig::new().with_tables(["users"]));
   let observable_b =
      ObservableSqliteDatabase::new(db_b.clone(), ObserverConfig::new().with_tables(["users"]));
   register_as_observed(&db_b, &observable_b);

   let mut rx_a = observable_a.subscribe(["users"]);
   let mut rx_b = observable_b.subscribe(["users"]);

   let specs = vec![AttachedSpec {
      database: db_b,
      schema_name: "other".to_string(),
      mode: AttachedMode::ReadWrite,
   }];

   // Mirrors mixed_transaction_publishes_to_both_brokers, but rolls back
   // instead of committing. rollback_callback fans out to every broker in
   // the hook map exactly the way commit_callback does - discarding only
   // "main"'s buffer would leave B's buffer holding this rolled-back INSERT,
   // ready to resurface as a phantom notification on B's *next* real commit.
   let mut writer = observable_a
      .acquire_writer_with_attached(specs)
      .await
      .unwrap();
   sqlx::query("BEGIN").execute(&mut *writer).await.unwrap();
   sqlx::query("INSERT INTO main.users (name) VALUES ('RolledBackMain')")
      .execute(&mut *writer)
      .await
      .unwrap();
   sqlx::query("INSERT INTO other.users (name) VALUES ('RolledBackOther')")
      .execute(&mut *writer)
      .await
      .unwrap();
   sqlx::query("ROLLBACK").execute(&mut *writer).await.unwrap();
   writer.detach_all().await.unwrap();

   assert!(
      timeout(Duration::from_millis(100), rx_a.recv())
         .await
         .is_err(),
      "A must not receive a notification for a rolled-back change"
   );
   assert!(
      timeout(Duration::from_millis(100), rx_b.recv())
         .await
         .is_err(),
      "B must not receive a notification for a rolled-back change"
   );

   // A subsequent, unrelated commit on each database - through its own plain
   // writer, not another attached transaction, so this doesn't also depend on
   // the ATTACH alias above having been cleanly released - must publish only
   // its own change, not the rolled-back row resurfacing alongside it.
   let mut writer_a = observable_a.acquire_writer().await.unwrap();
   sqlx::query("INSERT INTO users (name) VALUES ('RealMain')")
      .execute(&mut *writer_a)
      .await
      .unwrap();
   drop(writer_a);

   let change_a = timeout(Duration::from_millis(200), rx_a.recv())
      .await
      .expect("should not time out")
      .expect("A's subscriber should receive its own change");
   assert!(
      change_a
         .new_values
         .expect("capture_values defaults to true")
         .iter()
         .filter_map(|v| v.as_text())
         .any(|s| s == "RealMain"),
      "A must publish only the real change, not the rolled-back one"
   );
   assert!(
      timeout(Duration::from_millis(100), rx_a.recv())
         .await
         .is_err(),
      "A must not receive a second, phantom notification"
   );

   let mut writer_b = observable_b.acquire_writer().await.unwrap();
   sqlx::query("INSERT INTO users (name) VALUES ('RealOther')")
      .execute(&mut *writer_b)
      .await
      .unwrap();
   drop(writer_b);

   let change_b = timeout(Duration::from_millis(200), rx_b.recv())
      .await
      .expect("should not time out")
      .expect("B's subscriber should receive its own change");
   assert!(
      change_b
         .new_values
         .expect("capture_values defaults to true")
         .iter()
         .filter_map(|v| v.as_text())
         .any(|s| s == "RealOther"),
      "B must publish only the real change, not the rolled-back one"
   );
   assert!(
      timeout(Duration::from_millis(100), rx_b.recv())
         .await
         .is_err(),
      "B must not receive a second, phantom notification"
   );
}

#[tokio::test]
async fn abandoned_attached_transaction_does_not_leak_into_next_commit() {
   let temp_a = tempfile::NamedTempFile::new().unwrap();
   let temp_b = tempfile::NamedTempFile::new().unwrap();
   let db_a = create_attachable_db(
      &temp_a,
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
   )
   .await;
   let db_b = create_attachable_db(
      &temp_b,
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
   )
   .await;

   let observable_a =
      ObservableSqliteDatabase::new(db_a.clone(), ObserverConfig::new().with_tables(["users"]));
   let observable_b =
      ObservableSqliteDatabase::new(db_b.clone(), ObserverConfig::new().with_tables(["users"]));
   register_as_observed(&db_b, &observable_b);

   let mut rx_a = observable_a.subscribe(["users"]);
   let mut rx_b = observable_b.subscribe(["users"]);

   // Abandon a transaction mid-flight on both schemas: BEGIN plus writes to
   // each, then drop the writer directly - no COMMIT, no ROLLBACK, no
   // detach_all(). This is what ObservableWriteGuard::drop's broker fan-out
   // must clean up for every broker in the hook map, not just "main". (The
   // stale ATTACH this leaves on the pooled connection is a separate,
   // pre-existing conn-mgr contract - AttachedWriteGuard::drop deliberately
   // doesn't detach either, see its own doc - so the follow-up commits below
   // use each database's own plain writer rather than reusing this alias.)
   {
      let specs = vec![AttachedSpec {
         database: db_b.clone(),
         schema_name: "other".to_string(),
         mode: AttachedMode::ReadWrite,
      }];
      let mut writer = observable_a
         .acquire_writer_with_attached(specs)
         .await
         .unwrap();
      sqlx::query("BEGIN").execute(&mut *writer).await.unwrap();
      sqlx::query("INSERT INTO main.users (name) VALUES ('AbandonedMain')")
         .execute(&mut *writer)
         .await
         .unwrap();
      sqlx::query("INSERT INTO other.users (name) VALUES ('AbandonedOther')")
         .execute(&mut *writer)
         .await
         .unwrap();
      // Dropped here with no COMMIT, ROLLBACK, or detach_all() ever sent.
   }

   assert!(
      timeout(Duration::from_millis(100), rx_a.recv())
         .await
         .is_err(),
      "A must not receive a notification for an abandoned, uncommitted change"
   );
   assert!(
      timeout(Duration::from_millis(100), rx_b.recv())
         .await
         .is_err(),
      "B must not receive a notification for an abandoned, uncommitted change"
   );

   // A subsequent, unrelated commit on each database must publish only its
   // own change, not the abandoned row resurfacing alongside it.
   let mut writer_a = observable_a.acquire_writer().await.unwrap();
   sqlx::query("INSERT INTO users (name) VALUES ('RealMain')")
      .execute(&mut *writer_a)
      .await
      .unwrap();
   drop(writer_a);

   let change_a = timeout(Duration::from_millis(200), rx_a.recv())
      .await
      .expect("should not time out")
      .expect("A's subscriber should receive its own change");
   assert!(
      change_a
         .new_values
         .expect("capture_values defaults to true")
         .iter()
         .filter_map(|v| v.as_text())
         .any(|s| s == "RealMain"),
      "A must publish only the real change, not the abandoned one"
   );
   assert!(
      timeout(Duration::from_millis(100), rx_a.recv())
         .await
         .is_err(),
      "A must not receive a second, phantom notification"
   );

   let mut writer_b = observable_b.acquire_writer().await.unwrap();
   sqlx::query("INSERT INTO users (name) VALUES ('RealOther')")
      .execute(&mut *writer_b)
      .await
      .unwrap();
   drop(writer_b);

   let change_b = timeout(Duration::from_millis(200), rx_b.recv())
      .await
      .expect("should not time out")
      .expect("B's subscriber should receive its own change");
   assert!(
      change_b
         .new_values
         .expect("capture_values defaults to true")
         .iter()
         .filter_map(|v| v.as_text())
         .any(|s| s == "RealOther"),
      "B must publish only the real change, not the abandoned one"
   );
   assert!(
      timeout(Duration::from_millis(100), rx_b.recv())
         .await
         .is_err(),
      "B must not receive a second, phantom notification"
   );
}

#[tokio::test]
async fn detach_all_discards_buffered_events_for_every_broker() {
   let temp_a = tempfile::NamedTempFile::new().unwrap();
   let temp_b = tempfile::NamedTempFile::new().unwrap();
   let db_a = create_attachable_db(
      &temp_a,
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
   )
   .await;
   let db_b = create_attachable_db(
      &temp_b,
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
   )
   .await;

   let observable_a =
      ObservableSqliteDatabase::new(db_a.clone(), ObserverConfig::new().with_tables(["users"]));
   let observable_b =
      ObservableSqliteDatabase::new(db_b.clone(), ObserverConfig::new().with_tables(["users"]));
   register_as_observed(&db_b, &observable_b);

   let mut rx_a = observable_a.subscribe(["users"]);
   let mut rx_b = observable_b.subscribe(["users"]);

   let specs = vec![AttachedSpec {
      database: db_b.clone(),
      schema_name: "other".to_string(),
      mode: AttachedMode::ReadWrite,
   }];

   let mut writer = observable_a
      .acquire_writer_with_attached(specs)
      .await
      .unwrap();
   sqlx::query("BEGIN").execute(&mut *writer).await.unwrap();
   sqlx::query("INSERT INTO main.users (name) VALUES ('AbandonedMain')")
      .execute(&mut *writer)
      .await
      .unwrap();
   sqlx::query("INSERT INTO other.users (name) VALUES ('AbandonedOther')")
      .execute(&mut *writer)
      .await
      .unwrap();

   // detach_all() calls flush_all_brokers() *before* it attempts the DETACH -
   // and that flush is the only cleanup that happens here. The transaction is
   // still open (no COMMIT/ROLLBACK was ever sent), so SQLite refuses to
   // detach "other" out from under it and this deterministically returns
   // Err(ConnMgr(Sqlx("database other is locked"))). That failure is exactly
   // what this test is pinning around - the point is the flush that already
   // ran, not whether the DETACH itself succeeded - so the result is
   // deliberately discarded. Do NOT "fix" this into an `.unwrap()`; it is
   // supposed to fail.
   let _ = writer.detach_all().await;

   // This alone would also pass under the bug: nothing has committed yet, so
   // there's nothing to leak *yet*. The real assertion is below, after a real
   // commit.
   assert!(
      timeout(Duration::from_millis(100), rx_a.recv())
         .await
         .is_err(),
      "A must not receive a notification for a never-committed change"
   );
   assert!(
      timeout(Duration::from_millis(100), rx_b.recv())
         .await
         .is_err(),
      "B must not receive a notification for a never-committed change"
   );

   // Follow-up commits go through each database's own plain writer, not a
   // re-attach: the failed DETACH above left the "other" alias stranded on
   // A's pooled connection (same reasoning as
   // abandoned_attached_transaction_does_not_leak_into_next_commit above).
   let mut writer_a = observable_a.acquire_writer().await.unwrap();
   sqlx::query("INSERT INTO users (name) VALUES ('RealMain')")
      .execute(&mut *writer_a)
      .await
      .unwrap();
   drop(writer_a);

   // The load-bearing check: A's first (and only) notification must carry the
   // real value, not a phantom replay of the discarded buffer.
   let change_a = timeout(Duration::from_millis(200), rx_a.recv())
      .await
      .expect("should not time out")
      .expect("A's subscriber should receive its own change");
   assert!(
      change_a
         .new_values
         .expect("capture_values defaults to true")
         .iter()
         .filter_map(|v| v.as_text())
         .any(|s| s == "RealMain"),
      "A's first notification must be the real change, not the buffered one"
   );
   assert!(
      timeout(Duration::from_millis(100), rx_a.recv())
         .await
         .is_err(),
      "A must not receive a second, phantom notification"
   );

   let mut writer_b = observable_b.acquire_writer().await.unwrap();
   sqlx::query("INSERT INTO users (name) VALUES ('RealOther')")
      .execute(&mut *writer_b)
      .await
      .unwrap();
   drop(writer_b);

   let change_b = timeout(Duration::from_millis(200), rx_b.recv())
      .await
      .expect("should not time out")
      .expect("B's subscriber should receive its own change");
   assert!(
      change_b
         .new_values
         .expect("capture_values defaults to true")
         .iter()
         .filter_map(|v| v.as_text())
         .any(|s| s == "RealOther"),
      "B's first notification must be the real change, not the buffered one"
   );
   assert!(
      timeout(Duration::from_millis(100), rx_b.recv())
         .await
         .is_err(),
      "B must not receive a second, phantom notification"
   );
}

#[tokio::test]
async fn into_inner_discards_buffered_events_for_every_broker() {
   let temp_a = tempfile::NamedTempFile::new().unwrap();
   let temp_b = tempfile::NamedTempFile::new().unwrap();
   let db_a = create_attachable_db(
      &temp_a,
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
   )
   .await;
   let db_b = create_attachable_db(
      &temp_b,
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
   )
   .await;

   let observable_a =
      ObservableSqliteDatabase::new(db_a.clone(), ObserverConfig::new().with_tables(["users"]));
   let observable_b =
      ObservableSqliteDatabase::new(db_b.clone(), ObserverConfig::new().with_tables(["users"]));
   register_as_observed(&db_b, &observable_b);

   let mut rx_a = observable_a.subscribe(["users"]);
   let mut rx_b = observable_b.subscribe(["users"]);

   let specs = vec![AttachedSpec {
      database: db_b.clone(),
      schema_name: "other".to_string(),
      mode: AttachedMode::ReadWrite,
   }];

   let mut writer = observable_a
      .acquire_writer_with_attached(specs)
      .await
      .unwrap();
   sqlx::query("BEGIN").execute(&mut *writer).await.unwrap();
   sqlx::query("INSERT INTO main.users (name) VALUES ('AbandonedMain')")
      .execute(&mut *writer)
      .await
      .unwrap();
   sqlx::query("INSERT INTO other.users (name) VALUES ('AbandonedOther')")
      .execute(&mut *writer)
      .await
      .unwrap();

   // into_inner() unregisters the hooks and flushes both brokers' buffers
   // before handing back the plain (attached) writer - dropping that writer
   // here doesn't detach (AttachedWriteGuard::drop deliberately can't run an
   // async DETACH), so the "other" alias is left stranded exactly as it is
   // in the detach_all test above. Same reason the follow-up commits below
   // go through each database's own plain writer instead of reusing it.
   let unobserved = writer.into_inner();
   drop(unobserved);

   // This alone would also pass under the bug, for the same reason as in the
   // detach_all test: nothing has committed yet.
   assert!(
      timeout(Duration::from_millis(100), rx_a.recv())
         .await
         .is_err(),
      "A must not receive a notification for a never-committed change"
   );
   assert!(
      timeout(Duration::from_millis(100), rx_b.recv())
         .await
         .is_err(),
      "B must not receive a notification for a never-committed change"
   );

   let mut writer_a = observable_a.acquire_writer().await.unwrap();
   sqlx::query("INSERT INTO users (name) VALUES ('RealMain')")
      .execute(&mut *writer_a)
      .await
      .unwrap();
   drop(writer_a);

   // The load-bearing check: A's first (and only) notification must carry the
   // real value, not a phantom replay of the discarded buffer.
   let change_a = timeout(Duration::from_millis(200), rx_a.recv())
      .await
      .expect("should not time out")
      .expect("A's subscriber should receive its own change");
   assert!(
      change_a
         .new_values
         .expect("capture_values defaults to true")
         .iter()
         .filter_map(|v| v.as_text())
         .any(|s| s == "RealMain"),
      "A's first notification must be the real change, not the buffered one"
   );
   assert!(
      timeout(Duration::from_millis(100), rx_a.recv())
         .await
         .is_err(),
      "A must not receive a second, phantom notification"
   );

   let mut writer_b = observable_b.acquire_writer().await.unwrap();
   sqlx::query("INSERT INTO users (name) VALUES ('RealOther')")
      .execute(&mut *writer_b)
      .await
      .unwrap();
   drop(writer_b);

   let change_b = timeout(Duration::from_millis(200), rx_b.recv())
      .await
      .expect("should not time out")
      .expect("B's subscriber should receive its own change");
   assert!(
      change_b
         .new_values
         .expect("capture_values defaults to true")
         .iter()
         .filter_map(|v| v.as_text())
         .any(|s| s == "RealOther"),
      "B's first notification must be the real change, not the buffered one"
   );
   assert!(
      timeout(Duration::from_millis(100), rx_b.recv())
         .await
         .is_err(),
      "B must not receive a second, phantom notification"
   );
}

#[tokio::test]
async fn main_write_reports_main_schema() {
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

   let change = timeout(Duration::from_millis(100), rx.recv())
      .await
      .expect("should not time out")
      .expect("should receive a change");
   assert_eq!(change.schema, "main");
}

#[tokio::test]
async fn attached_primary_key_uses_owning_schema() {
   let temp_a = tempfile::NamedTempFile::new().unwrap();
   let temp_b = tempfile::NamedTempFile::new().unwrap();
   // A's `id` is column 0.
   let db_a = create_attachable_db(
      &temp_a,
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
   )
   .await;
   // B's `id` is column 1 - a different position than A's, so decoding B's
   // write with A's (or no) TableInfo would extract the wrong column, or none
   // at all, as the primary key.
   let db_b = create_attachable_db(
      &temp_b,
      "CREATE TABLE users (label TEXT, id INTEGER PRIMARY KEY)",
   )
   .await;

   let observable_a =
      ObservableSqliteDatabase::new(db_a, ObserverConfig::new().with_tables(["users"]));
   let observable_b =
      ObservableSqliteDatabase::new(db_b.clone(), ObserverConfig::new().with_tables(["users"]));
   register_as_observed(&db_b, &observable_b);

   let mut rx_b = observable_b.subscribe(["users"]);

   let specs = vec![AttachedSpec {
      database: db_b,
      schema_name: "other".to_string(),
      mode: AttachedMode::ReadWrite,
   }];

   let mut writer = observable_a
      .acquire_writer_with_attached(specs)
      .await
      .unwrap();
   sqlx::query("BEGIN").execute(&mut *writer).await.unwrap();
   sqlx::query("INSERT INTO other.users (label, id) VALUES ('widget', 42)")
      .execute(&mut *writer)
      .await
      .unwrap();
   sqlx::query("COMMIT").execute(&mut *writer).await.unwrap();
   writer.detach_all().await.unwrap();

   let change = timeout(Duration::from_millis(200), rx_b.recv())
      .await
      .expect("should not time out")
      .expect("B's subscriber should receive the change");
   assert_eq!(change.schema, "other");
   assert_eq!(
      change.primary_key.len(),
      1,
      "B's own TableInfo must have been queried - an empty primary_key means \
       ensure_table_info was skipped for the attached database"
   );
   assert_eq!(
      change.primary_key[0].as_integer(),
      Some(42),
      "must extract B's own id column (position 1), not whatever column A's \
       schema would put at the same index"
   );
}

/// Table info must be warmed before the single write permit is taken - see
/// `ObservableSqliteDatabase::acquire_writer`'s body for why.
///
/// The two orderings are indistinguishable without a third party, so the probe
/// here is an independent writer. With the read pool pinned to one connection
/// and this test holding it, the warming task can't proceed: under the correct
/// ordering it hasn't taken the write permit yet and the probe gets it
/// immediately, while under the reverse ordering it sits on the permit awaiting a
/// reader nobody will release and the probe blocks until sqlx's acquire timeout.
///
/// **Does not catch a post-permit re-check regression.** If a second
/// `ensure_table_info()` call were added after the write permit is acquired (see
/// `acquire_writer`'s body for why that was investigated and rejected), this
/// test would still pass: the pre-permit warm above blocks on the held reader
/// first, so the permit is never taken and the rejected re-check is never
/// reached. That regression needs a dedicated test with a table that never
/// resolves in the schema, not this one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn table_info_warming_does_not_hold_the_write_permit() {
   let temp = tempfile::NamedTempFile::new().unwrap();
   let db = SqliteDatabase::connect(
      temp.path().to_str().unwrap(),
      Some(SqliteDatabaseConfig {
         max_read_connections: 1,
         ..Default::default()
      }),
   )
   .await
   .unwrap();

   let mut writer = db.acquire_writer().await.unwrap();
   sqlx::query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
      .execute(&mut *writer)
      .await
      .unwrap();
   drop(writer);

   // Fresh observable: "users" is observed but its TableInfo has never been
   // queried, so the next acquire_writer() has to warm it.
   let observable = ObservableSqliteDatabase::new(
      Arc::clone(&db),
      ObserverConfig::new().with_tables(["users"]),
   );

   // Take the only read connection and keep it.
   let held_reader = db.read_pool().unwrap().acquire().await.unwrap();

   let warming = tokio::spawn({
      let observable = observable.clone();
      async move { observable.acquire_writer().await.map(|_| ()) }
   });

   // Let the warming task reach its await on the read pool.
   tokio::time::sleep(Duration::from_millis(100)).await;

   let probe = timeout(Duration::from_millis(500), db.acquire_writer()).await;
   assert!(
      probe.is_ok(),
      "an unrelated writer must still be able to take the write permit while \
       another task is warming table info; the permit is being held across a \
       read-pool await"
   );
   drop(probe);

   // Release the reader so the warming task can finish, and confirm it does.
   drop(held_reader);
   timeout(Duration::from_secs(5), warming)
      .await
      .expect("warming task should finish once a read connection frees up")
      .expect("warming task should not panic")
      .expect("warming task should acquire its writer");
}

// ============================================================================
// Broker-map collision guards (thread 10) - `acquire_writer_with_attached`
// used to seed the broker map with a blind `HashMap::insert`, so a spec
// aliased "main" would silently replace this database's own broker in the
// map, and two specs sharing an alias would silently collide. Neither was
// exploitable in practice only because `sqlx_sqlite_conn_mgr::validate_attached_specs`
// rejects both cases anyway, one layer away, after the map was already built -
// these tests pin that the rejection now happens up front, in this crate, as
// its own clear error, rather than relying on that other layer alone.
// ============================================================================

/// A `main`-aliased spec must be rejected before the broker map is built, and
/// as `validate_attached_specs`'s own `InvalidSchemaName` - not as some opaque
/// failure surfacing later out of `ATTACH` itself (which would report "database
/// main is already in use" once conn-mgr's own, independent validation pass
/// runs). The load-bearing half of this test is the assertion below: this
/// database's own broker must still work normally afterward - the rejection
/// must not have registered any hooks against a partially-built map.
#[tokio::test]
async fn acquire_writer_with_attached_rejects_main_aliased_spec_and_leaves_own_broker_intact() {
   let temp_a = tempfile::NamedTempFile::new().unwrap();
   let temp_b = tempfile::NamedTempFile::new().unwrap();
   let db_a = create_attachable_db(
      &temp_a,
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
   )
   .await;
   let db_b = create_attachable_db(
      &temp_b,
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
   )
   .await;

   let observable_a =
      ObservableSqliteDatabase::new(db_a, ObserverConfig::new().with_tables(["users"]));
   let mut rx_a = observable_a.subscribe(["users"]);

   let specs = vec![AttachedSpec {
      database: db_b,
      schema_name: "main".to_string(),
      mode: AttachedMode::ReadWrite,
   }];

   let result = observable_a.acquire_writer_with_attached(specs).await;
   assert!(
      matches!(
         result,
         Err(sqlx_sqlite_observer::Error::ConnMgr(
            sqlx_sqlite_conn_mgr::Error::InvalidSchemaName(_)
         ))
      ),
      "a main-aliased spec should surface as conn-mgr's own InvalidSchemaName, \
       not an opaque ATTACH failure three steps later, got {:?}",
      result.err()
   );

   // This database's own broker must still be reachable and working: a plain
   // (non-attached) writer must still register hooks and publish normally.
   let mut writer = observable_a
      .acquire_writer()
      .await
      .expect("acquire writer after rejected spec");
   sqlx::query("INSERT INTO users (name) VALUES ('StillWorks')")
      .execute(&mut *writer)
      .await
      .unwrap();
   drop(writer);

   let change = timeout(Duration::from_millis(200), rx_a.recv())
      .await
      .expect("should not time out")
      .expect("main's own broker must still be reachable after the rejected spec");
   assert_eq!(change.table, "users");
}

/// Same guard, for two specs sharing one alias rather than one spec aliased
/// `main`. Compared case-insensitively by `validate_attached_specs`, matching
/// SQLite's own schema namespace - this test uses an exact match rather than
/// an `"x"`/`"X"` pair since the case-insensitive comparison itself is already
/// pinned in `sqlx-sqlite-conn-mgr`'s own tests; this one is about this
/// crate's broker map staying untouched afterward.
#[tokio::test]
async fn acquire_writer_with_attached_rejects_duplicate_alias_and_leaves_own_broker_intact() {
   let temp_a = tempfile::NamedTempFile::new().unwrap();
   let temp_b = tempfile::NamedTempFile::new().unwrap();
   let temp_c = tempfile::NamedTempFile::new().unwrap();
   let db_a = create_attachable_db(
      &temp_a,
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
   )
   .await;
   let db_b = create_attachable_db(
      &temp_b,
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
   )
   .await;
   let db_c = create_attachable_db(
      &temp_c,
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
   )
   .await;

   let observable_a =
      ObservableSqliteDatabase::new(db_a, ObserverConfig::new().with_tables(["users"]));
   let mut rx_a = observable_a.subscribe(["users"]);

   let specs = vec![
      AttachedSpec {
         database: db_b,
         schema_name: "dup".to_string(),
         mode: AttachedMode::ReadWrite,
      },
      AttachedSpec {
         database: db_c,
         schema_name: "dup".to_string(),
         mode: AttachedMode::ReadWrite,
      },
   ];

   let result = observable_a.acquire_writer_with_attached(specs).await;
   assert!(
      matches!(
         result,
         Err(sqlx_sqlite_observer::Error::ConnMgr(
            sqlx_sqlite_conn_mgr::Error::DuplicateSchemaName(_)
         ))
      ),
      "two specs sharing an alias should surface as conn-mgr's own \
       DuplicateSchemaName, not a collision inside this crate's own broker map, \
       got {:?}",
      result.err()
   );

   let mut writer = observable_a
      .acquire_writer()
      .await
      .expect("acquire writer after rejected specs");
   sqlx::query("INSERT INTO users (name) VALUES ('StillWorksToo')")
      .execute(&mut *writer)
      .await
      .unwrap();
   drop(writer);

   let change = timeout(Duration::from_millis(200), rx_a.recv())
      .await
      .expect("should not time out")
      .expect("main's own broker must still be reachable after the rejected specs");
   assert_eq!(change.table, "users");
}
