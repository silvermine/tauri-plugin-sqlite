use std::path::Path;
use std::sync::Arc;

use serde_json::json;
use sqlx_sqlite_toolkit::DatabaseWrapper;

/// These tests verify the correct behavior of `DatabaseWrapper::connect` with an
/// in-memory database.
#[tokio::test]
async fn connect_memory_runs_ddl_and_dml() {
   let db = DatabaseWrapper::connect(Path::new(":memory:"), None)
      .await
      .expect("Failed to connect to in-memory database");

   db.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)".into(),
      vec![],
   )
   .execute()
   .await
   .expect("CREATE TABLE should succeed");

   let result = db
      .execute(
         "INSERT INTO t (name) VALUES ($1)".into(),
         vec![json!("Alice")],
      )
      .execute()
      .await
      .expect("INSERT should succeed");

   assert_eq!((result.rows_affected, result.last_insert_id), (1, 1));

   // Read back on the same write connection via an interruptible transaction.
   let mut tx = db
      .begin_interruptible_transaction()
      .execute(vec![(
         "INSERT INTO t (name) VALUES (?)",
         vec![json!("Bob")],
      )])
      .await
      .expect("transaction should start");

   let rows = tx
      .read("SELECT name FROM t ORDER BY id".into(), vec![])
      .await
      .expect("SELECT within transaction should succeed");

   assert_eq!(rows.len(), 2);
   tx.commit().await.expect("commit should succeed");
}

#[tokio::test]
async fn connect_memory_instances_are_independent() {
   let db1 = DatabaseWrapper::connect(Path::new(":memory:"), None)
      .await
      .expect("Failed to connect first in-memory database");
   let db2 = DatabaseWrapper::connect(Path::new(":memory:"), None)
      .await
      .expect("Failed to connect second in-memory database");

   assert!(
      !Arc::ptr_eq(db1.inner(), db2.inner()),
      ":memory: databases should not share the same SqliteDatabase instance"
   );

   db1.execute("CREATE TABLE test (id INTEGER)".into(), vec![])
      .execute()
      .await
      .expect("CREATE TABLE on first database should succeed");

   let result = db2
      .fetch_all("SELECT * FROM test".into(), vec![])
      .execute()
      .await;

   assert!(
      result.is_err(),
      "Second :memory: database should not see tables from the first"
   );
}
