//! An attached-database query that fails must still release its `ATTACH` alias.
//!
//! A stranded alias wedges every later attach of that name on the same pooled
//! connection - see `builders::detach_after`'s doc for the mechanism. Each test
//! below fails an attached operation, then proves the alias was released by
//! reusing it immediately.
//!
//! The read tests pin their call sites: nothing else detaches a read connection.
//! The write tests assert the outcome rather than one call site, because the
//! write pool's `after_release` hook also detaches whatever a dropped guard left
//! behind.

use std::sync::Arc;

use sqlx::ConnectOptions;
use sqlx_sqlite_conn_mgr::{AttachedMode, AttachedSpec, SqliteDatabaseConfig};
use sqlx_sqlite_toolkit::{DatabaseWrapper, KeysetColumn};
use tempfile::TempDir;

/// Returns (main, other, tempdir). `main` has `users`, `other` has `logs`.
///
/// `main`'s read pool is pinned to a single connection: the write pool is already
/// `max_connections(1)`, so a stranded write alias is always hit again on the next
/// attempt, but with the default read pool of 6 a stranded *read* alias is usually
/// dodged by landing on a different connection - which would make the read tests
/// pass whether or not the detach happened.
async fn two_databases() -> (DatabaseWrapper, DatabaseWrapper, TempDir) {
   let temp = TempDir::new().expect("temp dir");

   let single_reader = SqliteDatabaseConfig {
      max_read_connections: 1,
      ..Default::default()
   };

   let main = DatabaseWrapper::connect(&temp.path().join("main.db"), Some(single_reader))
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

   (main, other, temp)
}

#[tokio::test]
async fn failed_attached_write_still_releases_the_alias() {
   let (main, other, _temp) = two_databases().await;

   let make_spec = || {
      vec![AttachedSpec {
         database: Arc::clone(other.inner()),
         schema_name: "other".to_string(),
         mode: AttachedMode::ReadWrite,
      }]
   };

   // ATTACH succeeds, the statement itself fails. This is the shape that
   // stranded the alias: the error return skipped the detach.
   let err = main
      .execute(
         "INSERT INTO other.no_such_table (msg) VALUES ('x')".into(),
         vec![],
      )
      .attach(make_spec())
      .await
      .expect_err("write to a nonexistent table should fail");
   assert!(
      err.to_string().contains("no such table"),
      "expected the statement's own error, got: {err}"
   );

   // Same alias, same (single) write connection, valid statement.
   main
      .execute(
         "INSERT INTO other.logs (msg) VALUES ('after failure')".into(),
         vec![],
      )
      .attach(make_spec())
      .await
      .expect("the alias must be reusable after a failed attached write");
}

#[tokio::test]
async fn failed_attached_read_still_releases_the_alias() {
   let (main, other, _temp) = two_databases().await;

   let make_spec = || {
      vec![AttachedSpec {
         database: Arc::clone(other.inner()),
         schema_name: "other".to_string(),
         mode: AttachedMode::ReadOnly,
      }]
   };

   let err = main
      .fetch_all("SELECT * FROM other.no_such_table".into(), vec![])
      .attach(make_spec())
      .await
      .expect_err("read from a nonexistent table should fail");
   assert!(
      err.to_string().contains("no such table"),
      "expected the statement's own error, got: {err}"
   );

   main
      .fetch_all("SELECT * FROM other.logs".into(), vec![])
      .attach(make_spec())
      .await
      .expect("the alias must be reusable after a failed attached read");
}

/// `fetch_one` and `fetch_page` route through the same helper as the two above,
/// but "shares a helper" is exactly the kind of assumption that stops being true
/// during a refactor, and each builder wires the guarded region up itself.
#[tokio::test]
async fn failed_attached_fetch_one_and_fetch_page_still_release_the_alias() {
   let (main, other, _temp) = two_databases().await;

   let make_spec = || {
      vec![AttachedSpec {
         database: Arc::clone(other.inner()),
         schema_name: "other".to_string(),
         mode: AttachedMode::ReadOnly,
      }]
   };

   main
      .fetch_one("SELECT * FROM other.no_such_table".into(), vec![])
      .attach(make_spec())
      .await
      .expect_err("fetch_one on a nonexistent table should fail");
   main
      .fetch_one("SELECT * FROM other.logs LIMIT 1".into(), vec![])
      .attach(make_spec())
      .await
      .expect("the alias must be reusable after a failed attached fetch_one");

   let keyset = vec![KeysetColumn::asc("id")];
   main
      .fetch_page(
         "SELECT * FROM other.no_such_table".into(),
         vec![],
         keyset.clone(),
         10,
      )
      .attach(make_spec())
      .await
      .expect_err("fetch_page on a nonexistent table should fail");
   main
      .fetch_page("SELECT * FROM other.logs".into(), vec![], keyset, 10)
      .attach(make_spec())
      .await
      .expect("the alias must be reusable after a failed attached fetch_page");
}

/// The transaction and interruptible-transaction builders detach on their own
/// error paths too, but "shares the alias-release contract with the builders
/// above" is exactly the kind of assumption a refactor can quietly break -
/// each of these three tests below names the `detach_if_attached()` call site it
/// exercises.
#[tokio::test]
async fn failed_attached_transaction_statement_still_releases_the_alias() {
   let (main, other, _temp) = two_databases().await;

   let make_spec = || {
      vec![AttachedSpec {
         database: Arc::clone(other.inner()),
         schema_name: "other".to_string(),
         mode: AttachedMode::ReadWrite,
      }]
   };

   // The transaction's first statement succeeds; its second fails on a
   // nonexistent table, so the whole transaction rolls back - including the
   // first statement's insert. Exercises `detach_if_attached()` in the
   // `Err(e)` arm of `TransactionExecutionBuilder::execute`.
   let err = main
      .execute_transaction(vec![
         ("INSERT INTO other.logs (msg) VALUES ('ok')", vec![]),
         ("INSERT INTO other.no_such_table (msg) VALUES ('x')", vec![]),
      ])
      .attach(make_spec())
      .execute()
      .await
      .expect_err("the second statement's nonexistent table should fail the whole transaction");
   assert!(
      err.to_string().contains("no such table"),
      "expected the statement's own error, got: {err}"
   );

   // Same alias, same (single) write connection, valid statement.
   main
      .execute(
         "INSERT INTO other.logs (msg) VALUES ('after failure')".into(),
         vec![],
      )
      .attach(make_spec())
      .await
      .expect("the alias must be reusable after a failed attached transaction");

   // Only the post-failure row is present - proving the rollback actually
   // happened, not just that the alias survived.
   let rows = other
      .fetch_all("SELECT msg FROM logs".into(), vec![])
      .await
      .expect("logs should be readable");
   assert_eq!(
      rows.len(),
      1,
      "the rolled-back 'ok' row must not be present"
   );
   assert_eq!(
      rows[0]["msg"].as_str(),
      Some("after failure"),
      "only the row inserted after the failure should remain"
   );
}

#[tokio::test]
async fn failed_attached_interruptible_statement_still_releases_the_alias() {
   let (main, other, _temp) = two_databases().await;

   let make_spec = || {
      vec![AttachedSpec {
         database: Arc::clone(other.inner()),
         schema_name: "other".to_string(),
         mode: AttachedMode::ReadWrite,
      }]
   };

   // `InterruptibleTransaction` deliberately doesn't derive `Debug` (it holds
   // a live write guard), so `expect_err` won't compile here - match on the
   // `Result` directly instead.
   match main
      .begin_interruptible_transaction()
      .attach(make_spec())
      .execute(vec![(
         "INSERT INTO other.no_such_table (msg) VALUES ('x')",
         vec![],
      )])
      .await
   {
      Ok(_) => panic!("expected the initial statement's nonexistent table to fail"),
      Err(err) => assert!(
         err.to_string().contains("no such table"),
         "expected the statement's own error, got: {err}"
      ),
   }

   // Unlike the transaction builder above, this doesn't pin code in
   // `InterruptibleTransactionBuilder::execute` itself: the failing initial
   // statement fails inside `continue_with`, which returns via `?` before the
   // builder ever detaches, so it's `ActiveInterruptibleTransaction`'s own
   // `Drop` impl that rolls back and detaches - in a task spawned onto the
   // runtime. That task holds the single write permit until its `DETACH`
   // finishes, so this reuse attempt genuinely blocks on it for a moment
   // (measured 1-2ms) rather than racing it - deterministic, just not
   // instant.
   main
      .execute(
         "INSERT INTO other.logs (msg) VALUES ('after failure')".into(),
         vec![],
      )
      .attach(make_spec())
      .await
      .expect("the alias must be reusable after a failed interruptible transaction");
}

#[tokio::test]
async fn failed_begin_immediate_still_releases_the_alias() {
   let (main, other, temp) = two_databases().await;

   let make_spec = || {
      vec![AttachedSpec {
         database: Arc::clone(other.inner()),
         schema_name: "other".to_string(),
         mode: AttachedMode::ReadWrite,
      }]
   };

   // A raw connection, opened outside `SqliteDatabase`'s registry, is required
   // to hog the lock: `SqliteDatabase::connect` returns the same registry-shared
   // instance for a given path, so a second `DatabaseWrapper::connect("main.db")`
   // would share main's single-connection write pool rather than contend with
   // it. This locks "main" itself - the busy database this test is named for -
   // not the attached "other".
   let mut hog = sqlx::sqlite::SqliteConnectOptions::new()
      .filename(temp.path().join("main.db"))
      .connect()
      .await
      .expect("open a raw connection to main.db");
   sqlx::query("BEGIN IMMEDIATE")
      .execute(&mut hog)
      .await
      .expect("lock main via the raw connection");
   sqlx::query("INSERT INTO users (name) VALUES ('hog')")
      .execute(&mut hog)
      .await
      .expect("write on the raw connection");

   // The pooled writer's own BEGIN IMMEDIATE now contends with the raw
   // connection's still-open write transaction on the same file and
   // deterministically ends in SQLITE_BUSY - but only after sqlx exhausts its
   // default 5-second busy_timeout retrying internally, which
   // `SqliteDatabaseConfig` exposes no knob to shorten. That wait, not
   // anything wrong with the test, is why this test takes about 5 seconds.
   // Exercises `detach_if_attached()` in the `if let Err(err) = writer.begin_immediate()`
   // arm of `TransactionExecutionBuilder::execute`.
   let err = main
      .execute_transaction(vec![("INSERT INTO main.users (name) VALUES ('a')", vec![])])
      .attach(make_spec())
      .execute()
      .await
      .expect_err("BEGIN IMMEDIATE should fail while the raw connection holds the write lock");
   assert!(
      err.to_string().contains("database is locked"),
      "expected a busy/locked error, got: {err}"
   );

   // Release the hog's lock and reuse the alias.
   sqlx::query("ROLLBACK")
      .execute(&mut hog)
      .await
      .expect("release the raw connection's lock");
   drop(hog);

   main
      .execute(
         "INSERT INTO other.logs (msg) VALUES ('after failure')".into(),
         vec![],
      )
      .attach(make_spec())
      .await
      .expect("the alias must be reusable after a failed BEGIN IMMEDIATE");
}

/// Gives `main` a table whose foreign key is only checked at `COMMIT`, so
/// inserting an orphan row succeeds and the transaction's `COMMIT` is the
/// statement that fails - leaving the transaction open.
async fn create_deferred_foreign_key(main: &DatabaseWrapper) {
   main
      .execute(
         "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER \
          REFERENCES users(id) DEFERRABLE INITIALLY DEFERRED)"
            .into(),
         vec![],
      )
      .await
      .expect("create orders");
}

/// Asserts that `other.logs` holds exactly the row written after the failure,
/// i.e. that the failed transaction's own insert into the attached database was
/// rolled back rather than left behind or committed.
async fn assert_only_the_post_failure_row(other: &DatabaseWrapper) {
   let rows = other
      .fetch_all("SELECT msg FROM logs".into(), vec![])
      .await
      .expect("logs should be readable");
   assert_eq!(
      rows.len(),
      1,
      "the failed transaction's row must not be present"
   );
   assert_eq!(
      rows[0]["msg"].as_str(),
      Some("after failure"),
      "only the row inserted after the failure should remain"
   );
}

#[tokio::test]
async fn failed_interruptible_commit_still_releases_the_alias() {
   let (main, other, _temp) = two_databases().await;
   create_deferred_foreign_key(&main).await;

   let make_spec = || {
      vec![AttachedSpec {
         database: Arc::clone(other.inner()),
         schema_name: "other".to_string(),
         mode: AttachedMode::ReadWrite,
      }]
   };

   // `InterruptibleTransaction` doesn't derive `Debug`, so match rather than
   // `expect` - see `failed_attached_interruptible_statement_still_releases_the_alias`.
   let tx = match main
      .begin_interruptible_transaction()
      .attach(make_spec())
      .execute(vec![
         (
            "INSERT INTO other.logs (msg) VALUES ('uncommitted')",
            vec![],
         ),
         ("INSERT INTO orders (user_id) VALUES (999)", vec![]),
      ])
      .await
   {
      Ok(tx) => tx,
      Err(err) => panic!("the initial statements should succeed, got: {err}"),
   };

   // Both statements succeeded; the orphan `user_id` only fails the deferred
   // foreign key check at COMMIT, and SQLite leaves the transaction open after
   // that. `ActiveInterruptibleTransaction::commit` has already taken the
   // writer out of the transaction by then, so the transaction's own `Drop`
   // has nothing left to roll back or detach - the write pool's
   // `after_release` hook does both.
   let err = tx
      .commit()
      .await
      .expect_err("the deferred foreign key violation should fail COMMIT");
   assert!(
      err.to_string().contains("FOREIGN KEY constraint failed"),
      "expected the COMMIT error itself, got: {err}"
   );

   main
      .execute(
         "INSERT INTO other.logs (msg) VALUES ('after failure')".into(),
         vec![],
      )
      .attach(make_spec())
      .await
      .expect("the alias must be reusable after a failed interruptible COMMIT");

   assert_only_the_post_failure_row(&other).await;
}
