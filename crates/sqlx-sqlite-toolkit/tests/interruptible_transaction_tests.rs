use serde_json::json;
use sqlx_sqlite_toolkit::{DatabaseWrapper, Statement};
use tempfile::TempDir;

async fn create_test_db(name: &str) -> (DatabaseWrapper, TempDir) {
   let temp_dir = TempDir::new().expect("Failed to create temp directory");
   let db_path = temp_dir.path().join(name);
   let wrapper = DatabaseWrapper::connect(&db_path, None)
      .await
      .expect("Failed to connect to test database");

   (wrapper, temp_dir)
}

#[tokio::test]
async fn test_interruptible_transaction_with_attached_cross_database_insert() {
   let (main_db, _temp_main) = create_test_db("main.db").await;
   let (attached_db, _temp_attached) = create_test_db("attached.db").await;

   main_db
      .execute(
         "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)".into(),
         vec![],
      )
      .await
      .unwrap();

   attached_db
      .execute(
         "CREATE TABLE archive (id INTEGER PRIMARY KEY, user_name TEXT)".into(),
         vec![],
      )
      .await
      .unwrap();

   attached_db
      .execute(
         "INSERT INTO archive (user_name) VALUES ($1)".into(),
         vec![json!("ArchivedUser")],
      )
      .await
      .unwrap();

   let attached_spec = sqlx_sqlite_conn_mgr::AttachedSpec {
      database: std::sync::Arc::clone(attached_db.inner_for_testing()),
      schema_name: "archive".to_string(),
      mode: sqlx_sqlite_conn_mgr::AttachedMode::ReadOnly,
   };

   let results = main_db
      .execute_transaction(vec![(
         "INSERT INTO users (name) SELECT user_name FROM archive.archive",
         vec![],
      )])
      .attach(vec![attached_spec])
      .await
      .unwrap();

   assert_eq!(results.len(), 1);
   assert_eq!(results[0].rows_affected, 1);

   let rows = main_db
      .fetch_all("SELECT * FROM users".into(), vec![])
      .await
      .unwrap();

   assert_eq!(rows.len(), 1);
   assert_eq!(rows[0].get("name"), Some(&json!("ArchivedUser")));

   main_db.remove().await.unwrap();
   attached_db.remove().await.unwrap();
}

#[tokio::test]
async fn test_basic_interruptible_transaction() {
   let (db, _temp) = create_test_db("test.db").await;

   db.execute(
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)".into(),
      vec![],
   )
   .await
   .unwrap();

   let mut tx = db
      .begin_interruptible_transaction()
      .execute(vec![(
         "INSERT INTO users (name) VALUES (?)",
         vec![json!("Alice")],
      )])
      .await
      .unwrap();

   let results = tx
      .continue_with(vec![Statement {
         query: "INSERT INTO users (name) VALUES (?)".to_string(),
         values: vec![json!("Bob")],
      }])
      .await
      .unwrap();

   assert_eq!(results.len(), 1);
   assert_eq!(results[0].rows_affected, 1);

   let rows = tx
      .read("SELECT name FROM users ORDER BY id".to_string(), vec![])
      .await
      .unwrap();
   assert_eq!(rows.len(), 2);
   assert_eq!(rows[0].get("name"), Some(&json!("Alice")));
   assert_eq!(rows[1].get("name"), Some(&json!("Bob")));

   tx.commit().await.unwrap();

   let committed_rows = db
      .fetch_all("SELECT * FROM users ORDER BY id".into(), vec![])
      .await
      .unwrap();

   assert_eq!(committed_rows.len(), 2);

   db.remove().await.unwrap();
}

#[tokio::test]
async fn test_interruptible_transaction_with_attached() {
   let (main_db, _temp_main) = create_test_db("main.db").await;
   let (attached_db, _temp_attached) = create_test_db("attached.db").await;

   main_db
      .execute(
         "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)".into(),
         vec![],
      )
      .await
      .unwrap();

   attached_db
      .execute(
         "CREATE TABLE archive (id INTEGER PRIMARY KEY, user_name TEXT)".into(),
         vec![],
      )
      .await
      .unwrap();

   attached_db
      .execute(
         "INSERT INTO archive (user_name) VALUES (?)".to_string(),
         vec![json!("ArchivedUser")],
      )
      .await
      .unwrap();

   let attached_spec = sqlx_sqlite_conn_mgr::AttachedSpec {
      database: std::sync::Arc::clone(attached_db.inner_for_testing()),
      schema_name: "archive".to_string(),
      mode: sqlx_sqlite_conn_mgr::AttachedMode::ReadOnly,
   };

   let mut tx = main_db
      .begin_interruptible_transaction()
      .attach(vec![attached_spec])
      .execute(vec![(
         "INSERT INTO users (name) SELECT user_name FROM archive.archive",
         vec![],
      )])
      .await
      .unwrap();

   let users = tx
      .read("SELECT name FROM users".to_string(), vec![])
      .await
      .unwrap();
   assert_eq!(users.len(), 1);
   assert_eq!(users[0].get("name"), Some(&json!("ArchivedUser")));

   tx.commit().await.unwrap();

   let rows = main_db
      .fetch_all("SELECT * FROM users".into(), vec![])
      .await
      .unwrap();

   assert_eq!(rows.len(), 1);
   assert_eq!(rows[0].get("name"), Some(&json!("ArchivedUser")));

   main_db.remove().await.unwrap();
   attached_db.remove().await.unwrap();
}

#[tokio::test]
async fn test_interruptible_transaction_rollback() {
   let (db, _temp) = create_test_db("test.db").await;

   db.execute(
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)".into(),
      vec![],
   )
   .await
   .unwrap();

   let tx = db
      .begin_interruptible_transaction()
      .execute(vec![(
         "INSERT INTO users (name) VALUES (?)",
         vec![json!("Alice")],
      )])
      .await
      .unwrap();

   tx.rollback().await.unwrap();

   let rows = db
      .fetch_all("SELECT * FROM users".into(), vec![])
      .await
      .unwrap();

   assert_eq!(rows.len(), 0);

   db.remove().await.unwrap();
}

#[tokio::test]
async fn test_interruptible_transaction_auto_rollback() {
   let (db, _temp) = create_test_db("test.db").await;

   db.execute(
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)".into(),
      vec![],
   )
   .await
   .unwrap();

   {
      let _tx = db
         .begin_interruptible_transaction()
         .execute(vec![(
            "INSERT INTO users (name) VALUES (?)",
            vec![json!("Alice")],
         )])
         .await
         .unwrap();
      // Transaction dropped without commit - should auto-rollback
   }

   let rows = db
      .fetch_all("SELECT * FROM users".into(), vec![])
      .await
      .unwrap();

   assert_eq!(rows.len(), 0);

   db.remove().await.unwrap();
}

#[tokio::test]
async fn test_attached_database_readwrite_transaction() {
   let (main_db, _temp_main) = create_test_db("main.db").await;
   let (attached_db, _temp_attached) = create_test_db("attached.db").await;

   main_db
      .execute(
         "CREATE TABLE orders (id INTEGER PRIMARY KEY, total REAL)".into(),
         vec![],
      )
      .await
      .unwrap();

   attached_db
      .execute(
         "CREATE TABLE stats (order_count INTEGER DEFAULT 0)".into(),
         vec![],
      )
      .await
      .unwrap();

   attached_db
      .execute("INSERT INTO stats (order_count) VALUES (0)".into(), vec![])
      .await
      .unwrap();

   let attached_spec = sqlx_sqlite_conn_mgr::AttachedSpec {
      database: std::sync::Arc::clone(attached_db.inner_for_testing()),
      schema_name: "stats".to_string(),
      mode: sqlx_sqlite_conn_mgr::AttachedMode::ReadWrite,
   };

   let results = main_db
      .execute_transaction(vec![
         ("INSERT INTO orders (total) VALUES ($1)", vec![json!(99.99)]),
         (
            "UPDATE stats.stats SET order_count = order_count + 1",
            vec![],
         ),
      ])
      .attach(vec![attached_spec])
      .await
      .unwrap();

   assert_eq!(results.len(), 2);
   assert_eq!(results[0].rows_affected, 1);
   assert_eq!(results[1].rows_affected, 1);

   let stats = attached_db
      .fetch_one("SELECT order_count FROM stats".into(), vec![])
      .await
      .unwrap()
      .unwrap();

   assert_eq!(stats.get("order_count"), Some(&json!(1)));

   main_db.remove().await.unwrap();
   attached_db.remove().await.unwrap();
}

#[tokio::test]
async fn test_simple_execute_transaction() {
   let (db, _temp) = create_test_db("test.db").await;

   db.execute(
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)".into(),
      vec![],
   )
   .await
   .unwrap();

   let results = db
      .execute_transaction(vec![
         ("INSERT INTO users (name) VALUES (?)", vec![json!("Alice")]),
         ("INSERT INTO users (name) VALUES (?)", vec![json!("Bob")]),
      ])
      .await
      .unwrap();

   assert_eq!(results.len(), 2);
   assert_eq!(results[0].rows_affected, 1);
   assert_eq!(results[1].rows_affected, 1);

   let rows = db
      .fetch_all("SELECT * FROM users ORDER BY id".into(), vec![])
      .await
      .unwrap();
   assert_eq!(rows.len(), 2);
   assert_eq!(rows[0].get("name"), Some(&json!("Alice")));
   assert_eq!(rows[1].get("name"), Some(&json!("Bob")));

   db.remove().await.unwrap();
}

#[tokio::test]
async fn test_execute_transaction_rollback_on_failure() {
   let (db, _temp) = create_test_db("test.db").await;

   db.execute(
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)".into(),
      vec![],
   )
   .await
   .unwrap();

   // Second statement should fail (NULL in NOT NULL column)
   let result = db
      .execute_transaction(vec![
         ("INSERT INTO users (name) VALUES (?)", vec![json!("Alice")]),
         ("INSERT INTO users (name) VALUES (?)", vec![json!(null)]),
      ])
      .await;

   assert!(result.is_err());

   // First insert should be rolled back
   let rows = db
      .fetch_all("SELECT * FROM users".into(), vec![])
      .await
      .unwrap();
   assert_eq!(rows.len(), 0);

   db.remove().await.unwrap();
}
