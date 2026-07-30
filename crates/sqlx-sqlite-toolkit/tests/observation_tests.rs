//! Regression tests for `DatabaseWrapper::enable_observation()`.
//!
//! These specifically cover issue #54: re-calling `enable_observation()` (surfaced
//! to Tauri callers as `observe()`) must not destroy the existing broadcast broker,
//! or every subscriber created before the re-call silently stops receiving events.

#![cfg(feature = "observer")]

use std::time::Duration;

use sqlx_sqlite_observer::ObserverConfig;
use sqlx_sqlite_toolkit::DatabaseWrapper;
use tempfile::TempDir;
use tokio::time::timeout;

async fn create_test_db() -> (DatabaseWrapper, TempDir) {
   let temp_dir = TempDir::new().expect("Failed to create temp directory");
   let db_path = temp_dir.path().join("test.db");
   let wrapper = DatabaseWrapper::connect(&db_path, None)
      .await
      .expect("Failed to connect to test database");

   wrapper
      .execute(
         "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)".into(),
         vec![],
      )
      .await
      .expect("create users table");
   wrapper
      .execute(
         "CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)".into(),
         vec![],
      )
      .await
      .expect("create posts table");

   (wrapper, temp_dir)
}

#[tokio::test]
async fn test_first_enable_observation_applies_requested_config() {
   let (mut wrapper, _temp) = create_test_db().await;

   wrapper.enable_observation(
      ObserverConfig::new()
         .with_tables(["users"])
         .with_channel_capacity(8)
         .with_capture_values(false),
   );

   let broker = wrapper.observable().unwrap().broker();
   assert_eq!(broker.channel_capacity(), 8);
   assert!(!broker.capture_values());
   assert!(wrapper.is_observing());
}

/// This is the exact regression scenario from issue #54: a subscriber created
/// before a second `enable_observation()` call (with a *different* table set)
/// must keep receiving events published after that second call, instead of
/// seeing its `broadcast::Receiver` closed because the broker was replaced.
#[tokio::test]
async fn test_reenable_observation_preserves_existing_subscriber_across_new_tables() {
   let (mut wrapper, _temp) = create_test_db().await;

   wrapper.enable_observation(ObserverConfig::new().with_tables(["users"]));

   // A subscriber that only asked to observe "users" tables, created before the
   // second (additive) enable_observation() call below.
   let mut rx = wrapper.observable().unwrap().subscribe(["users"]);

   // Second observe() call, with a completely different table set. Under the old
   // (destructive) behavior this would drop the broker and close `rx`.
   wrapper.enable_observation(ObserverConfig::new().with_tables(["posts"]));

   wrapper
      .execute("INSERT INTO users (name) VALUES ('Alice')".into(), vec![])
      .await
      .expect("insert into users");

   let change = timeout(Duration::from_millis(200), rx.recv())
      .await
      .expect("subscriber should not have been closed by the second enable_observation() call")
      .expect("should receive a change, not RecvError::Closed");

   assert_eq!(change.table, "users");

   // The second call's tables were merged in, not swapped in.
   let mut observed = wrapper.observable().unwrap().observed_tables();
   observed.sort();
   assert_eq!(observed, vec!["posts".to_string(), "users".to_string()]);
}

/// Config-conflict rule: `channel_capacity` and `capture_values` are fixed by the
/// first `enable_observation()` call. A later call requesting different values for
/// either is ignored for those two fields (only the tables are merged in) - this is
/// the direct-Rust-caller contract for this crate. The Tauri plugin's `observe()`
/// command builds a stricter contract on top of it (rejecting the conflicting
/// request outright), but that's enforced one layer up in `src/commands.rs` of the
/// `tauri-plugin-sqlite` crate, not here.
#[tokio::test]
async fn test_reenable_observation_ignores_conflicting_config() {
   let (mut wrapper, _temp) = create_test_db().await;

   wrapper.enable_observation(
      ObserverConfig::new()
         .with_tables(["users"])
         .with_channel_capacity(4)
         .with_capture_values(false),
   );

   wrapper.enable_observation(
      ObserverConfig::new()
         .with_tables(["users"])
         .with_channel_capacity(999)
         .with_capture_values(true),
   );

   let broker = wrapper.observable().unwrap().broker();
   assert_eq!(
      broker.channel_capacity(),
      4,
      "channel_capacity should stay at the first call's value"
   );
   assert!(
      !broker.capture_values(),
      "capture_values should stay at the first call's value"
   );

   // Confirm the ignored capture_values request is reflected in actual behavior,
   // not just the getter: old/new values should still be absent from change events.
   let mut rx = wrapper.observable().unwrap().subscribe(["users"]);
   wrapper
      .execute("INSERT INTO users (name) VALUES ('Bob')".into(), vec![])
      .await
      .expect("insert into users");

   let change = timeout(Duration::from_millis(200), rx.recv())
      .await
      .expect("should not time out")
      .expect("should receive a change");

   assert!(
      change.new_values.is_none(),
      "capture_values=true from the second call should have been ignored"
   );
}
