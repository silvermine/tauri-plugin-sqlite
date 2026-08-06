//! End-to-end coverage of observed writes into an attached database, through
//! the public toolkit API rather than one layer down at
//! `ObservableSqliteDatabase::acquire_writer_with_attached`.
//!
//! Two things are pinned here that nothing else pins:
//!
//! 1. **Routing.** A change made under an `ATTACH` alias reaches the *owning*
//!    database's subscribers carrying that alias in `TableChange::schema`,
//!    while the same transaction's `main` write reaches this database's own
//!    subscribers as `"main"`.
//! 2. **Alias release.** The attached alias is detached when the transaction
//!    finishes (see `builders::detach_after`'s doc for what stranding it costs).
//!    The second transaction below is the assertion for that; without it, a
//!    `detach_all()` call silently becoming a no-op passes the whole suite.

#![cfg(feature = "observer")]

use std::sync::Arc;
use std::time::Duration;

use sqlx_sqlite_conn_mgr::{AttachedMode, AttachedSpec};
use sqlx_sqlite_observer::ObserverConfig;
use sqlx_sqlite_toolkit::DatabaseWrapper;
use tempfile::TempDir;
use tokio::time::timeout;

/// How long to wait for a published change before calling it lost. Generous
/// because the publish happens on the commit hook, not the caller's task.
const RECV_TIMEOUT: Duration = Duration::from_millis(500);

#[tokio::test]
async fn observed_attached_transaction_routes_by_owner_and_releases_the_alias() {
   let temp = TempDir::new().unwrap();

   let main = DatabaseWrapper::connect(&temp.path().join("main.db"), None)
      .await
      .expect("connect main");
   let other = DatabaseWrapper::connect(&temp.path().join("other.db"), None)
      .await
      .expect("connect other");

   main
      .execute(
         "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)".into(),
         vec![],
      )
      .await
      .expect("create users");
   other
      .execute(
         "CREATE TABLE logs (id INTEGER PRIMARY KEY, msg TEXT)".into(),
         vec![],
      )
      .await
      .expect("create logs");

   // Both databases must observe: a ReadWrite attachment only contributes a
   // broker to the hook map if it has observation enabled of its own, so
   // without this the write to `other.logs` lands unobserved.
   main.enable_observation(ObserverConfig::new().with_tables(["users"]));
   other.enable_observation(ObserverConfig::new().with_tables(["logs"]));

   let mut rx_main = main.observable().unwrap().subscribe(["users"]);
   let mut rx_other = other.observable().unwrap().subscribe(["logs"]);

   // ReadWrite, not ReadOnly: `acquire_writer_with_attached` deliberately leaves a
   // ReadOnly attachment's broker out of the map, so a ReadOnly spec here would
   // make the `other` write unobserved and fail against correct code.
   let make_spec = || AttachedSpec {
      database: Arc::clone(other.inner()),
      schema_name: "other".to_string(),
      mode: AttachedMode::ReadWrite,
   };

   main
      .execute_transaction(vec![
         ("INSERT INTO users (name) VALUES ('Zed')", vec![]),
         ("INSERT INTO other.logs (msg) VALUES ('hello')", vec![]),
      ])
      .attach(vec![make_spec()])
      .execute()
      .await
      .expect("attached observed transaction should commit");

   let main_change = timeout(RECV_TIMEOUT, rx_main.recv())
      .await
      .expect("main's own change should be published")
      .expect("main subscriber should still be live");
   assert_eq!(main_change.schema, "main");
   assert_eq!(main_change.table, "users");

   let other_change = timeout(RECV_TIMEOUT, rx_other.recv())
      .await
      .expect("the attached database's change should reach its own subscribers")
      .expect("other subscriber should still be live");
   assert_eq!(other_change.schema, "other");
   assert_eq!(other_change.table, "logs");

   // Reuses the same alias on the same (single) write connection. Fails with
   // "database other is already in use" if the first transaction didn't
   // detach.
   main
      .execute_transaction(vec![("INSERT INTO users (name) VALUES ('Yan')", vec![])])
      .attach(vec![make_spec()])
      .execute()
      .await
      .expect("second attached transaction must be able to reuse the alias");
}

/// Pins the defect this file's suite otherwise missed: every test above enables
/// observation on *both* main and the attached database, so nothing exercised
/// the case where main's own observation is off but an attached `ReadWrite`
/// database's own observation is on. Before the fix,
/// `DatabaseWrapper::acquire_writer_with_attached` only took the observable
/// path when *main's* broker existed, so an unobserved main sent this straight
/// to `AttachedWriterGuard::Regular` - no hooks registered at all - and
/// `other`'s own subscribers silently got nothing, even though the README
/// promises routing based only on the *attached* database's own state.
///
/// Verified failing against the pre-fix code (a `TempDir`-based reproduction of
/// this exact scenario against the code at commit `8ea4d00`, run in an isolated
/// worktree): `rx_other.recv()` timed out with `Elapsed(())`.
#[tokio::test]
async fn main_unobserved_attached_observed_still_notifies_attached_subscriber() {
   let temp = TempDir::new().unwrap();

   let main = DatabaseWrapper::connect(&temp.path().join("main.db"), None)
      .await
      .expect("connect main");
   let other = DatabaseWrapper::connect(&temp.path().join("other.db"), None)
      .await
      .expect("connect other");

   main
      .execute(
         "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)".into(),
         vec![],
      )
      .await
      .expect("create users");
   other
      .execute(
         "CREATE TABLE logs (id INTEGER PRIMARY KEY, msg TEXT)".into(),
         vec![],
      )
      .await
      .expect("create logs");

   // main is deliberately left unobserved - this is the one variable this test
   // changes relative to `observed_attached_transaction_routes_by_owner_and_releases_the_alias`
   // above.
   other.enable_observation(ObserverConfig::new().with_tables(["logs"]));

   let mut rx_other = other.observable().unwrap().subscribe(["logs"]);

   let spec = AttachedSpec {
      database: Arc::clone(other.inner()),
      schema_name: "other".to_string(),
      mode: AttachedMode::ReadWrite,
   };

   main
      .execute_transaction(vec![(
         "INSERT INTO other.logs (msg) VALUES ('hello')",
         vec![],
      )])
      .attach(vec![spec])
      .execute()
      .await
      .expect("an attached transaction through an unobserved main should still commit");

   let change = timeout(RECV_TIMEOUT, rx_other.recv())
      .await
      .expect("other's subscriber should receive the change even though main is unobserved")
      .expect("other subscriber should still be live");
   assert_eq!(change.schema, "other");
   assert_eq!(change.table, "logs");
}

/// The reverse-direction guard the test above doesn't cover, and which is what
/// catches an over-broad fix to the defect above: it would be a mistake to fix
/// "main unobserved, attached observed" by routing every write to whichever
/// broker exists, since that could misattribute an attached write to main's
/// broker whenever the table names happen to collide. Here, main's *own*
/// observed-table set deliberately includes "logs" - the exact name of the
/// table being written through the attached alias - and `other` is left
/// unobserved. A correct fix still drops the change (per
/// `acquire_writer_with_attached`'s documented "nowhere for its changes to go"
/// rule for an unobserved `ReadWrite` attachment); an over-broad fix that
/// matched by table name alone would incorrectly deliver it to main's "logs"
/// subscriber.
#[tokio::test]
async fn main_observed_with_colliding_table_name_does_not_receive_unobserved_attached_write() {
   let temp = TempDir::new().unwrap();

   let main = DatabaseWrapper::connect(&temp.path().join("main.db"), None)
      .await
      .expect("connect main");
   let other = DatabaseWrapper::connect(&temp.path().join("other.db"), None)
      .await
      .expect("connect other");

   // Both databases happen to have a "logs" table - main's own is unrelated to
   // the attached one, but shares the name on purpose.
   main
      .execute(
         "CREATE TABLE logs (id INTEGER PRIMARY KEY, msg TEXT)".into(),
         vec![],
      )
      .await
      .expect("create main.logs");
   other
      .execute(
         "CREATE TABLE logs (id INTEGER PRIMARY KEY, msg TEXT)".into(),
         vec![],
      )
      .await
      .expect("create other.logs");

   // main observes "logs" - the same name as the table written through the
   // attached alias below. other is left unobserved entirely.
   main.enable_observation(ObserverConfig::new().with_tables(["logs"]));
   let mut rx_main = main.observable().unwrap().subscribe(["logs"]);

   let spec = AttachedSpec {
      database: Arc::clone(other.inner()),
      schema_name: "other".to_string(),
      mode: AttachedMode::ReadWrite,
   };

   main
      .execute_transaction(vec![(
         "INSERT INTO other.logs (msg) VALUES ('should not surface anywhere')",
         vec![],
      )])
      .attach(vec![spec])
      .execute()
      .await
      .expect("an attached transaction into an unobserved database should still commit");

   let result = timeout(Duration::from_millis(200), rx_main.recv()).await;
   assert!(
      result.is_err(),
      "a write into an unobserved attached database's 'logs' table must not be \
       misattributed to main's own 'logs' subscriber just because the names \
       collide - it must be dropped entirely, not misrouted"
   );
}

/// The alias-release guard from the first test in this file, replayed against
/// this file's new "main unobserved" scenario: a regression that stops
/// detaching after a main-unobserved, attached-observed transaction should
/// fail the *second* transaction's `ATTACH` with "database other is already in
/// use", not silently pass because nothing ever reused the alias.
#[tokio::test]
async fn main_unobserved_attached_observed_alias_is_still_released() {
   let temp = TempDir::new().unwrap();

   let main = DatabaseWrapper::connect(&temp.path().join("main.db"), None)
      .await
      .expect("connect main");
   let other = DatabaseWrapper::connect(&temp.path().join("other.db"), None)
      .await
      .expect("connect other");

   main
      .execute(
         "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)".into(),
         vec![],
      )
      .await
      .expect("create users");
   other
      .execute(
         "CREATE TABLE logs (id INTEGER PRIMARY KEY, msg TEXT)".into(),
         vec![],
      )
      .await
      .expect("create logs");

   other.enable_observation(ObserverConfig::new().with_tables(["logs"]));

   let make_spec = || AttachedSpec {
      database: Arc::clone(other.inner()),
      schema_name: "other".to_string(),
      mode: AttachedMode::ReadWrite,
   };

   main
      .execute_transaction(vec![(
         "INSERT INTO other.logs (msg) VALUES ('first')",
         vec![],
      )])
      .attach(vec![make_spec()])
      .execute()
      .await
      .expect("first attached transaction should commit");

   // Reuses the same alias on the same (single) write connection. Fails with
   // "database other is already in use" if the first transaction's detach
   // regressed on this (main-unobserved) path.
   main
      .execute_transaction(vec![(
         "INSERT INTO other.logs (msg) VALUES ('second')",
         vec![],
      )])
      .attach(vec![make_spec()])
      .execute()
      .await
      .expect("second attached transaction must be able to reuse the alias");
}
