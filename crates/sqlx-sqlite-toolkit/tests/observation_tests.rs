//! Regression tests for `DatabaseWrapper`'s observation API.
//!
//! Covers issue #54: re-calling `enable_observation()` (surfaced to Tauri callers
//! as `observe()`) must not destroy the existing broadcast broker, or every
//! subscriber created before the re-call silently stops receiving events.
//!
//! Also covers issue #53: observation is a property of the underlying database,
//! not of any one `DatabaseWrapper` value. A clone of a wrapper, and a completely
//! independent `DatabaseWrapper::connect()` call to the same path, must observe
//! through the exact same broker as the handle that enabled it - and `:memory:`
//! databases, which never share a `SqliteDatabase` with anything, must stay
//! independent of each other precisely because of that. Also covers #53's
//! abandoned-transaction buffer leak: a writer dropped mid-transaction without
//! an explicit commit or rollback must not have its buffered changes resurface
//! on the next transaction's commit.

#![cfg(feature = "observer")]

use std::sync::Arc;
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
   let (wrapper, _temp) = create_test_db().await;

   wrapper.enable_observation(
      ObserverConfig::new()
         .with_tables(["users"])
         .with_channel_capacity(8)
         .with_capture_values(false),
   );

   let observable = wrapper.observable().unwrap();
   let broker = observable.broker();
   assert_eq!(broker.channel_capacity(), 8);
   assert!(!broker.capture_values());
   assert!(wrapper.is_observing());

   // The requested tables are part of "applies requested config" too, and this is
   // the only test that pins them on the *create* path - every other one either
   // subscribes (which registers the table itself) or goes through the merge path.
   // Sorted because `observed_tables()` collects from a `HashSet`.
   let mut observed = observable.observed_tables();
   observed.sort();
   assert_eq!(observed, vec!["users".to_string()]);
}

/// This is the exact regression scenario from issue #54: a subscriber created
/// before a second `enable_observation()` call (with a *different* table set)
/// must keep receiving events published after that second call, instead of
/// seeing its `broadcast::Receiver` closed because the broker was replaced.
#[tokio::test]
async fn test_reenable_observation_preserves_existing_subscriber_across_new_tables() {
   let (wrapper, _temp) = create_test_db().await;

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
   let (wrapper, _temp) = create_test_db().await;

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

   let observable = wrapper.observable().unwrap();
   let broker = observable.broker();
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

/// The issue's exact scenario: a clone of a wrapper shares the same underlying
/// `SqliteDatabase` as the original, so enabling and subscribing through the
/// clone must see writes made through the original.
#[tokio::test]
async fn observation_is_shared_across_clones() {
   let (original, _temp) = create_test_db().await;
   let clone = original.clone();

   clone.enable_observation(ObserverConfig::new().with_tables(["users"]));
   let mut rx = clone.observable().unwrap().subscribe(["users"]);

   original
      .execute("INSERT INTO users (name) VALUES ('Alice')".into(), vec![])
      .await
      .expect("insert into users via original");

   let change = timeout(Duration::from_millis(200), rx.recv())
      .await
      .expect("should not time out")
      .expect("should receive a change");
   assert_eq!(change.table, "users");
}

/// What per-clone sharing alone would not fix: two entirely separate
/// `DatabaseWrapper::connect()` calls to the same path resolve to the same
/// underlying `SqliteDatabase` (the path registry in `sqlx-sqlite-conn-mgr`
/// guarantees this), so they must share observation state too, even though
/// neither is a clone of the other.
#[tokio::test]
async fn observation_is_shared_across_independent_connects() {
   let temp_dir = TempDir::new().expect("Failed to create temp directory");
   let db_path = temp_dir.path().join("test.db");

   let a = DatabaseWrapper::connect(&db_path, None)
      .await
      .expect("connect handle A");
   a.execute(
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)".into(),
      vec![],
   )
   .await
   .expect("create users table");

   let b = DatabaseWrapper::connect(&db_path, None)
      .await
      .expect("connect handle B");

   a.enable_observation(ObserverConfig::new().with_tables(["users"]));
   let mut rx = a.observable().unwrap().subscribe(["users"]);

   b.execute("INSERT INTO users (name) VALUES ('Bob')".into(), vec![])
      .await
      .expect("insert into users via handle B");

   let change = timeout(Duration::from_millis(200), rx.recv())
      .await
      .expect("should not time out")
      .expect("should receive a change");
   assert_eq!(change.table, "users");
}

/// `:memory:` databases are deliberately excluded from the path registry (they
/// all share the literal path `":memory:"`, so registry sharing would be wrong),
/// which means two separate connects never resolve to the same `SqliteDatabase`
/// and therefore never share an observer slot either. Enabling observation on
/// one must not make the other report as observing.
#[tokio::test]
async fn memory_databases_do_not_share_observation() {
   let db1 = DatabaseWrapper::connect(std::path::Path::new(":memory:"), None)
      .await
      .expect("connect first :memory: database");
   let db2 = DatabaseWrapper::connect(std::path::Path::new(":memory:"), None)
      .await
      .expect("connect second :memory: database");

   db1.enable_observation(ObserverConfig::new().with_tables(["users"]));

   assert!(db1.is_observing());
   assert!(
      !db2.is_observing(),
      ":memory: databases must not share observation state"
   );
}

/// The inverse of `observation_is_shared_across_independent_connects`:
/// `disable_observation()` clears the shared slot, so calling it from one
/// independently-connected handle must stop observation for every handle to the
/// same database - including the one that originally enabled it. This is the
/// widened blast radius documented on `disable_observation`'s rustdoc.
#[tokio::test]
async fn disable_observation_affects_all_handles() {
   let temp_dir = TempDir::new().expect("Failed to create temp directory");
   let db_path = temp_dir.path().join("test.db");

   let a = DatabaseWrapper::connect(&db_path, None)
      .await
      .expect("connect handle A");
   a.execute(
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)".into(),
      vec![],
   )
   .await
   .expect("create users table");

   let b = DatabaseWrapper::connect(&db_path, None)
      .await
      .expect("connect handle B");

   a.enable_observation(ObserverConfig::new().with_tables(["users"]));
   let mut rx = a.observable().unwrap().subscribe(["users"]);

   // Disable via the independently-connected handle, not the one that enabled it.
   b.disable_observation();

   assert!(!a.is_observing(), "handle A must see the disable too");
   assert!(!b.is_observing());

   a.execute("INSERT INTO users (name) VALUES ('Carol')".into(), vec![])
      .await
      .expect("insert into users after disable_observation()");

   // Disabling drops the broker (no other Arc keeps it alive once the slot is
   // cleared), which closes the broadcast channel out from under `rx` - so the
   // no-notification outcome can surface either as a timeout or as an
   // immediate `RecvError` on the now-closed channel. Either is a pass; only an
   // actual change arriving is a failure.
   match timeout(Duration::from_millis(100), rx.recv()).await {
      Err(_) => {}     // timed out waiting - no notification arrived
      Ok(Err(_)) => {} // channel closed/lagged - no notification arrived
      Ok(Ok(change)) => panic!(
         "no notification should arrive after disable_observation() from any handle, got {change:?}"
      ),
   }
}

/// The intentional bypass survives the refactor: `acquire_regular_writer()`
/// must still skip observation entirely, even now that observation is
/// database-wide rather than per-handle.
#[tokio::test]
async fn regular_writer_bypasses_observation() {
   let (wrapper, _temp) = create_test_db().await;

   wrapper.enable_observation(ObserverConfig::new().with_tables(["users"]));
   let mut rx = wrapper.observable().unwrap().subscribe(["users"]);

   {
      let mut writer = wrapper
         .acquire_regular_writer()
         .await
         .expect("acquire regular writer");
      sqlx::query("INSERT INTO users (name) VALUES ('Dave')")
         .execute(&mut *writer)
         .await
         .expect("insert via regular writer");
   }

   // Asserting behavior, not log output: this workspace has no log-capture
   // harness, so the only observable evidence of the bypass is the absence of a
   // notification for a write that unquestionably happened.
   let outcome = timeout(Duration::from_millis(100), rx.recv()).await;
   assert!(
      outcome.is_err(),
      "acquire_regular_writer() must not publish any change notification"
   );

   // Confirm the write actually landed - the point is that it bypassed
   // observation, not that it silently failed.
   let rows = wrapper
      .fetch_all("SELECT name FROM users WHERE name = 'Dave'".into(), vec![])
      .execute()
      .await
      .expect("select after regular writer insert");
   assert_eq!(rows.len(), 1);
}

/// The race this fix closes: many independent handles to the same database
/// calling `enable_observation()` concurrently must converge on a single
/// broker, not each build their own and let the last one silently win - which
/// would orphan whichever subscriber was registered against a broker that got
/// replaced. `ObserverSlot::get_or_init` makes the check-and-create atomic
/// under the slot's own lock specifically to rule this out.
///
/// Requires a multi-thread runtime: `enable_observation()` has no `.await` in
/// it, so on the default current-thread runtime, cooperative scheduling can
/// never preempt one task's call mid-body to let another one interleave -
/// every task would run its entire `enable_observation()` to completion
/// before the next one starts, and this test would pass even against a naive
/// (non-atomic) get-then-set implementation. See
/// `sqlx_sqlite_conn_mgr::observer_slot`'s tests for a deterministic version
/// of this same race, using real OS threads and a `Barrier`.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_enable_observation_converges_on_one_broker() {
   let temp_dir = TempDir::new().expect("Failed to create temp directory");
   let db_path = temp_dir.path().join("test.db");

   let seed = DatabaseWrapper::connect(&db_path, None)
      .await
      .expect("connect seed handle");
   seed
      .execute(
         "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)".into(),
         vec![],
      )
      .await
      .expect("create users table");

   // Each task connects independently (not a clone) and races to be the first
   // to enable observation. A naive get()-then-set() implementation would let
   // several of these each observe an empty slot and build their own broker.
   let mut tasks = Vec::new();
   for _ in 0..16 {
      let db_path = db_path.clone();
      tasks.push(tokio::spawn(async move {
         let wrapper = DatabaseWrapper::connect(&db_path, None)
            .await
            .expect("connect racing handle");
         wrapper.enable_observation(ObserverConfig::new().with_tables(["users"]));
         wrapper
            .observable()
            .expect("observation should be enabled after enable_observation()")
            .broker()
            .clone()
      }));
   }

   let mut brokers = Vec::new();
   for task in tasks {
      brokers.push(task.await.expect("racing task should not panic"));
   }

   let first_ptr = std::sync::Arc::as_ptr(&brokers[0]);
   for broker in &brokers[1..] {
      assert!(
         std::ptr::eq(std::sync::Arc::as_ptr(broker), first_ptr),
         "every concurrent enable_observation() call must converge on the same broker"
      );
   }

   // A subscriber registered against the very first broker instance handed back
   // must still receive notifications once the race settles - if some later
   // caller had silently replaced the broker instead of reusing it, this
   // receiver would be listening to a broker no writer publishes through
   // anymore.
   let mut rx = brokers[0].subscribe();
   seed
      .execute("INSERT INTO users (name) VALUES ('Eve')".into(), vec![])
      .await
      .expect("insert into users after the race settles");

   let change = timeout(Duration::from_millis(200), rx.recv())
      .await
      .expect("should not time out")
      .expect("should receive a change");
   assert_eq!(change.table, "users");
}

/// A writer dropped mid-transaction - BEGIN plus a write, then dropped with no
/// COMMIT or ROLLBACK ever sent - must not have its buffered change resurface
/// on the *next* transaction's commit. Without `ObservableWriteGuard::drop`
/// discarding the buffer, the abandoned INSERT below would still be sitting in
/// the broker's buffer when the real transaction commits, and `on_commit`'s
/// `mem::take` would publish it right alongside (or instead of) the real change.
///
/// Uses `acquire_writer()` directly (not `execute_transaction()` or
/// `begin_interruptible_transaction()`) because both of those already have
/// their own higher-level auto-rollback-on-drop safety nets that issue a real
/// `ROLLBACK` before the writer itself drops - which would exercise that
/// existing mechanism, not the gap this test is for: a writer dropped with
/// hooks still registered and no commit or rollback statement ever sent.
#[tokio::test]
async fn abandoned_transaction_does_not_leak_into_next_commit() {
   let (wrapper, _temp) = create_test_db().await;

   wrapper.enable_observation(ObserverConfig::new().with_tables(["users"]));
   let mut rx = wrapper.observable().unwrap().subscribe(["users"]);

   {
      let mut writer = wrapper.acquire_writer().await.expect("acquire writer");
      sqlx::query("BEGIN")
         .execute(&mut *writer)
         .await
         .expect("begin");
      sqlx::query("INSERT INTO users (name) VALUES ('Abandoned')")
         .execute(&mut *writer)
         .await
         .expect("insert (never committed)");
      // Dropped here with no COMMIT or ROLLBACK ever sent.
   }

   wrapper
      .execute("INSERT INTO users (name) VALUES ('Real')".into(), vec![])
      .await
      .expect("insert into users");

   let change = timeout(Duration::from_millis(200), rx.recv())
      .await
      .expect("should not time out")
      .expect("should receive a change");

   assert_eq!(change.table, "users");
   let new_values = change.new_values.expect("capture_values defaults to true");
   assert!(
      new_values
         .iter()
         .filter_map(|v| v.as_text())
         .any(|s| s == "Real"),
      "expected the committed row's own values, got {new_values:?} - a leaked \
       abandoned-transaction event would carry 'Abandoned' instead"
   );

   // No second notification - specifically not the abandoned transaction's
   // insert straggling in as a phantom change alongside the real one.
   let no_more = timeout(Duration::from_millis(100), rx.recv()).await;
   assert!(
      no_more.is_err(),
      "the abandoned transaction's buffered insert must not surface as a \
       second notification"
   );
}

/// Regression test for a strong reference cycle the initial database-wide
/// observation implementation introduced: the observer slot used to hold
/// `Arc<ObservableSqliteDatabase>`, whose own `db` field is an `Arc` back to the
/// very `SqliteDatabase` that owns the slot. That kept the database alive forever
/// once observed, even after every external handle was dropped without calling
/// `close()` - defeating the registry's `Weak` reference and the free-on-drop
/// contract documented on `SqliteDatabase::close`.
///
/// The conn-mgr path registry isn't reachable from this crate to assert against
/// its `Weak` directly, so this asserts the same thing one layer down:
/// downgrading the wrapper's own `Arc<SqliteDatabase>` and confirming it does not
/// upgrade once the last strong reference is dropped. While the slot still stored
/// the whole observable rather than just its broker, this `upgrade()` succeeded.
#[tokio::test]
async fn dropping_wrapper_without_close_frees_database_even_when_observed() {
   let (wrapper, _temp) = create_test_db().await;

   wrapper.enable_observation(ObserverConfig::new().with_tables(["users"]));
   assert!(wrapper.is_observing());

   let weak = Arc::downgrade(wrapper.inner_for_testing());

   // No explicit close() - the whole point is what happens on a bare drop.
   drop(wrapper);

   assert!(
      weak.upgrade().is_none(),
      "the SqliteDatabase must be freed once the last external handle drops, \
       even with observation enabled; a successful upgrade here means the \
       observer slot still holds a strong reference back to this database"
   );
}

/// Pins the known, deliberately deferred broker-identity limitation: a writer
/// binds its hooks to whichever broker was in the slot at acquisition and keeps
/// them for its whole lifetime, so a `disable_observation()` +
/// `enable_observation()` cycle during its open transaction leaves it publishing
/// to the pre-cycle broker. Subscribers created before the cycle still receive
/// the commit; one created after it does not. See
/// `sqlx_sqlite_observer::ObservableSqliteDatabase::acquire_writer`'s doc for the
/// mechanics and the deferred fix.
#[tokio::test]
#[ignore = "pins a known, deliberately deferred limitation - see \
            sqlx_sqlite_observer::ObservableSqliteDatabase::acquire_writer's doc. This test \
            asserts today's buggy behavior (after_result.is_err()), so un-ignoring alone would \
            turn it red once the fix lands: invert the assertion to expect a successful recv() \
            first, then remove #[ignore]. Follow-up issue not yet filed"]
async fn disable_enable_cycle_during_open_writer_strands_new_subscribers_on_new_broker() {
   let (wrapper, _temp) = create_test_db().await;

   wrapper.enable_observation(ObserverConfig::new().with_tables(["users"]));

   // Created *before* the cycle - expected to still receive the commit below,
   // since its Arc keeps the pre-cycle broker alive regardless of what the slot
   // points to afterward.
   let mut rx_before = wrapper.observable().unwrap().subscribe(["users"]);

   // Stands in for "some other caller's interruptible transaction is still
   // open": acquire a writer and begin, but don't commit yet. Its hooks bind to
   // whatever broker is in the slot right now - the pre-cycle one.
   let mut writer = wrapper.acquire_writer().await.expect("acquire writer");
   sqlx::query("BEGIN")
      .execute(&mut *writer)
      .await
      .expect("begin");
   sqlx::query("INSERT INTO users (name) VALUES ('MidCycle')")
      .execute(&mut *writer)
      .await
      .expect("insert while writer is open");

   // The disable/enable cycle, while `writer`'s transaction is still open - the
   // plugin-level equivalent of `unobserve()` followed by `observe()`.
   wrapper.disable_observation();
   wrapper.enable_observation(ObserverConfig::new().with_tables(["users"]));
   assert!(
      wrapper.is_observing(),
      "is_observing() reports success throughout - that's exactly the problem: \
       nothing observable here signals the broker-identity mismatch this test \
       exists to pin"
   );

   // Created *after* the cycle - subscribes against the new broker the slot now
   // holds, which `writer`'s already-registered hooks are not bound to.
   let mut rx_after = wrapper.observable().unwrap().subscribe(["users"]);

   sqlx::query("COMMIT")
      .execute(&mut *writer)
      .await
      .expect("commit");
   drop(writer);

   let change = timeout(Duration::from_millis(200), rx_before.recv())
      .await
      .expect("should not time out")
      .expect("pre-cycle subscriber should still receive the commit");
   assert_eq!(change.table, "users");

   // The bug: a subscriber created after the cycle never sees a commit from a
   // writer whose hooks were already bound to the old broker before the cycle
   // ran. Once the deferred fix lands, this should become a successful
   // `rx_after.recv()` carrying the "MidCycle" row instead of a timeout - at
   // which point remove the #[ignore] above.
   let after_result = timeout(Duration::from_millis(200), rx_after.recv()).await;
   assert!(
      after_result.is_err(),
      "a subscriber created after the disable/enable cycle unexpectedly received \
       the in-flight writer's commit - if this is failing, the deferred fix may \
       already be in place, in which case update this test and remove the \
       #[ignore] above rather than leaving it pinned to the old (buggy) behavior"
   );
}
