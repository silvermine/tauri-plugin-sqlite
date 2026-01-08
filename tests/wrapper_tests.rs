use serde_json::{Value as JsonValue, json};
use tauri_plugin_sqlite::DatabaseWrapper;
use tempfile::TempDir;

async fn create_test_db() -> (DatabaseWrapper, TempDir) {
   let temp_dir = TempDir::new().expect("Failed to create temp directory");
   let db_path = temp_dir.path().join("test.db");
   let wrapper = DatabaseWrapper::connect_with_path(&db_path, None)
      .await
      .expect("Failed to connect to test database");

   (wrapper, temp_dir)
}

#[tokio::test]
async fn test_execute_and_write_result() {
   let (db, _temp) = create_test_db().await;

   // DDL returns 0 rows affected
   let result = db
      .execute(
         "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)".into(),
         vec![],
      )
      .await
      .unwrap();

   assert_eq!(result.rows_affected, 0);

   // INSERT returns rows_affected and last_insert_id
   let result = db
      .execute(
         "INSERT INTO t (name) VALUES ($1)".into(),
         vec![json!("Alice")],
      )
      .await
      .unwrap();

   assert_eq!((result.rows_affected, result.last_insert_id), (1, 1));

   let result = db
      .execute(
         "INSERT INTO t (name) VALUES ($1)".into(),
         vec![json!("Bob")],
      )
      .await
      .unwrap();

   assert_eq!((result.rows_affected, result.last_insert_id), (1, 2));

   // UPDATE affects multiple rows
   let result = db
      .execute("UPDATE t SET name = 'X' WHERE id > 0".into(), vec![])
      .await
      .unwrap();

   assert_eq!(result.rows_affected, 2);

   db.remove().await.unwrap();
}

#[tokio::test]
async fn test_fetch_all() {
   let (db, _temp) = create_test_db().await;
   db.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, active INT)".into(),
      vec![],
   )
   .await
   .unwrap();

   // Empty table returns empty vec
   assert!(
      db.fetch_all("SELECT * FROM t".into(), vec![])
         .await
         .unwrap()
         .is_empty()
   );

   // Insert test data
   db.execute(
      "INSERT INTO t (name, active) VALUES ($1,$2), ($3,$4), ($5,$6)".into(),
      vec![
         json!("Alice"),
         json!(1),
         json!("Bob"),
         json!(0),
         json!("Charlie"),
         json!(1),
      ],
   )
   .await
   .unwrap();

   // Fetch all rows
   let rows = db
      .fetch_all("SELECT * FROM t ORDER BY id".into(), vec![])
      .await
      .unwrap();

   assert_eq!(rows.len(), 3);
   assert_eq!(rows[0].get("name"), Some(&json!("Alice")));

   // Fetch with parameter filter
   let rows = db
      .fetch_all(
         "SELECT name FROM t WHERE active = $1".into(),
         vec![json!(1)],
      )
      .await
      .unwrap();

   assert_eq!(rows.len(), 2);

   db.remove().await.unwrap();
}

#[tokio::test]
async fn test_fetch_one() {
   let (db, _temp) = create_test_db().await;
   db.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)".into(),
      vec![],
   )
   .await
   .unwrap();

   // No results returns None
   assert!(
      db.fetch_one("SELECT * FROM t WHERE id = $1".into(), vec![json!(999)])
         .await
         .unwrap()
         .is_none()
   );

   db.execute(
      "INSERT INTO t (name) VALUES ($1), ($2)".into(),
      vec![json!("Alice"), json!("Bob")],
   )
   .await
   .unwrap();

   // Single result returns Some
   let row = db
      .fetch_one("SELECT * FROM t WHERE id = $1".into(), vec![json!(1)])
      .await
      .unwrap()
      .unwrap();

   assert_eq!(row.get("name"), Some(&json!("Alice")));

   // Multiple results returns error
   let err = db
      .fetch_one("SELECT * FROM t".into(), vec![])
      .await
      .unwrap_err();

   assert!(err.to_string().contains("2 rows"));

   db.remove().await.unwrap();
}

#[tokio::test]
async fn test_transactions() {
   let (db, _temp) = create_test_db().await;
   db.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER NOT NULL)".into(),
      vec![],
   )
   .await
   .unwrap();

   db.execute(
      "INSERT INTO t (id, val) VALUES (1, 100), (2, 50)".into(),
      vec![],
   )
   .await
   .unwrap();

   // Successful transaction commits
   let results = db
      .execute_transaction(vec![
         ("UPDATE t SET val = val - 30 WHERE id = 1".into(), vec![]),
         ("UPDATE t SET val = val + 30 WHERE id = 2".into(), vec![]),
      ])
      .await
      .unwrap();

   assert_eq!(results.len(), 2);

   let rows = db
      .fetch_all("SELECT val FROM t ORDER BY id".into(), vec![])
      .await
      .unwrap();

   assert_eq!(rows[0].get("val"), Some(&json!(70)));
   assert_eq!(rows[1].get("val"), Some(&json!(80)));

   // Failed transaction rolls back (NULL violates NOT NULL)
   let err = db
      .execute_transaction(vec![
         ("UPDATE t SET val = 999 WHERE id = 1".into(), vec![]),
         ("INSERT INTO t (id, val) VALUES (3, NULL)".into(), vec![]),
      ])
      .await;

   assert!(err.is_err());

   // Verify rollback: id=1 should still be 70
   let row = db
      .fetch_one("SELECT val FROM t WHERE id = 1".into(), vec![])
      .await
      .unwrap()
      .unwrap();

   assert_eq!(row.get("val"), Some(&json!(70)));

   db.remove().await.unwrap();
}

#[tokio::test]
async fn test_type_binding_and_decoding() {
   let (db, _temp) = create_test_db().await;
   db.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, txt TEXT, num REAL, big INTEGER, flag BOOLEAN, data BLOB)".into(),
      vec![],
   )
   .await
   .unwrap();

   let large_int: i64 = 9_007_199_254_740_992; // 2^53

   // Insert with various types including NULL
   db.execute(
      "INSERT INTO t (txt) VALUES ($1)".into(),
      vec![JsonValue::Null],
   )
   .await
   .unwrap();

   db.execute(
      "INSERT INTO t (txt, num) VALUES ($1, $2)".into(),
      vec![json!("hello"), json!(1.23456)],
   )
   .await
   .unwrap();

   db.execute(
      "INSERT INTO t (big) VALUES ($1)".into(),
      vec![json!(large_int)],
   )
   .await
   .unwrap();

   // Boolean
   db.execute("INSERT INTO t (flag) VALUES (TRUE)".into(), vec![])
      .await
      .unwrap();

   // BLOB ("Hello" in hex)
   db.execute("INSERT INTO t (data) VALUES (X'48656C6C6F')".into(), vec![])
      .await
      .unwrap();

   let rows = db
      .fetch_all("SELECT * FROM t ORDER BY id".into(), vec![])
      .await
      .unwrap();

   // NULL decoding
   assert_eq!(rows[0].get("txt"), Some(&JsonValue::Null));

   // Float decoding (with tolerance)
   let num = rows[1].get("num").unwrap().as_f64().unwrap();
   assert!((num - 1.23456).abs() < 0.0001);

   // Large integer precision
   assert_eq!(rows[2].get("big"), Some(&json!(large_int)));

   // Boolean stored as integer
   assert_eq!(rows[3].get("flag"), Some(&json!(1)));

   // BLOB as base64
   assert_eq!(rows[4].get("data").unwrap().as_str(), Some("SGVsbG8="));

   db.remove().await.unwrap();
}

#[tokio::test]
async fn test_column_order_preserved() {
   let (db, _temp) = create_test_db().await;
   db.execute("CREATE TABLE t (z TEXT, a TEXT, m TEXT)".into(), vec![])
      .await
      .unwrap();

   db.execute(
      "INSERT INTO t VALUES ($1, $2, $3)".into(),
      vec![json!("z"), json!("a"), json!("m")],
   )
   .await
   .unwrap();

   let rows = db
      .fetch_all("SELECT z, a, m FROM t".into(), vec![])
      .await
      .unwrap();

   let keys: Vec<&String> = rows[0].keys().collect();
   assert_eq!(keys, vec!["z", "a", "m"]);

   db.remove().await.unwrap();
}

#[tokio::test]
async fn test_close() {
   let (db, _temp) = create_test_db().await;
   db.execute("CREATE TABLE t (id INTEGER)".into(), vec![])
      .await
      .unwrap();

   db.close().await.expect("close should succeed");
}
