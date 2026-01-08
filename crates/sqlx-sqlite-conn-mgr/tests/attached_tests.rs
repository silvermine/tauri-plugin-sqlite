//! Integration tests for attached database functionality

use sqlx_sqlite_conn_mgr::{
   AttachedMode, AttachedSpec, Error, SqliteDatabase, acquire_reader_with_attached,
   acquire_writer_with_attached,
};
use std::sync::Arc;

#[tokio::test]
async fn test_attach_readonly() {
   let main_path = std::env::current_dir().unwrap().join("test_attach_main.db");
   let orders_path = std::env::current_dir()
      .unwrap()
      .join("test_attach_orders.db");

   // Create main database with users table
   let main_db = SqliteDatabase::connect(&main_path, None).await.unwrap();
   let mut writer = main_db.acquire_writer().await.unwrap();
   sqlx::query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
      .execute(&mut *writer)
      .await
      .unwrap();

   sqlx::query("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
      .execute(&mut *writer)
      .await
      .unwrap();

   drop(writer);

   // Create orders database with orders table
   let orders_db = SqliteDatabase::connect(&orders_path, None).await.unwrap();
   let mut writer = orders_db.acquire_writer().await.unwrap();
   sqlx::query("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL)")
      .execute(&mut *writer)
      .await
      .unwrap();

   sqlx::query("INSERT INTO orders (id, user_id, total) VALUES (1, 1, 99.99), (2, 2, 49.99)")
      .execute(&mut *writer)
      .await
      .unwrap();

   drop(writer);

   // Attach orders database for read-only access
   let specs = vec![AttachedSpec {
      database: Arc::clone(&orders_db),
      schema_name: "orders".to_string(),
      mode: AttachedMode::ReadOnly,
   }];

   let mut conn = acquire_reader_with_attached(&main_db, specs).await.unwrap();

   // Cross-database query
   let rows: Vec<(String, f64)> = sqlx::query_as(
      "SELECT u.name, o.total FROM users u JOIN orders.orders o ON u.id = o.user_id ORDER BY u.name",
   )
   .fetch_all(&mut *conn)
   .await
   .unwrap();

   assert_eq!(rows.len(), 2);
   assert_eq!(rows[0].0, "Alice");
   assert_eq!(rows[0].1, 99.99);
   assert_eq!(rows[1].0, "Bob");
   assert_eq!(rows[1].1, 49.99);

   // Explicit cleanup
   conn.detach_all().await.unwrap();

   main_db.remove().await.unwrap();
   orders_db.remove().await.unwrap();
}

#[tokio::test]
async fn test_attach_readwrite_transaction() {
   let main_path = std::env::current_dir()
      .unwrap()
      .join("test_attach_rw_main.db");

   let stats_path = std::env::current_dir()
      .unwrap()
      .join("test_attach_rw_stats.db");

   // Create main database
   let main_db = SqliteDatabase::connect(&main_path, None).await.unwrap();
   let mut writer = main_db.acquire_writer().await.unwrap();
   sqlx::query("CREATE TABLE orders (id INTEGER PRIMARY KEY, total REAL)")
      .execute(&mut *writer)
      .await
      .unwrap();

   drop(writer);

   // Create stats database
   let stats_db = SqliteDatabase::connect(&stats_path, None).await.unwrap();
   let mut writer = stats_db.acquire_writer().await.unwrap();
   sqlx::query("CREATE TABLE order_stats (total_orders INTEGER, total_revenue REAL)")
      .execute(&mut *writer)
      .await
      .unwrap();

   sqlx::query("INSERT INTO order_stats (total_orders, total_revenue) VALUES (0, 0.0)")
      .execute(&mut *writer)
      .await
      .unwrap();

   drop(writer);

   // Attach stats database for read-write access
   let specs = vec![AttachedSpec {
      database: Arc::clone(&stats_db),
      schema_name: "stats".to_string(),
      mode: AttachedMode::ReadWrite,
   }];

   let mut guard = acquire_writer_with_attached(&main_db, specs).await.unwrap();

   // Begin transaction and update both databases
   sqlx::query("BEGIN IMMEDIATE")
      .execute(&mut *guard)
      .await
      .unwrap();

   sqlx::query("INSERT INTO orders (id, total) VALUES (1, 99.99)")
      .execute(&mut *guard)
      .await
      .unwrap();

   sqlx::query("UPDATE stats.order_stats SET total_orders = total_orders + 1, total_revenue = total_revenue + 99.99")
      .execute(&mut *guard)
      .await
      .unwrap();

   sqlx::query("COMMIT").execute(&mut *guard).await.unwrap();

   // Explicit cleanup
   guard.detach_all().await.unwrap();

   // Verify both databases were updated
   let (order_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM orders")
      .fetch_one(main_db.read_pool().unwrap())
      .await
      .unwrap();

   assert_eq!(order_count, 1);

   let (total_orders, total_revenue): (i64, f64) =
      sqlx::query_as("SELECT total_orders, total_revenue FROM order_stats")
         .fetch_one(stats_db.read_pool().unwrap())
         .await
         .unwrap();

   assert_eq!(total_orders, 1);
   assert_eq!(total_revenue, 99.99);

   main_db.remove().await.unwrap();
   stats_db.remove().await.unwrap();
}

#[tokio::test]
async fn test_attach_multiple_databases() {
   let main_path = std::env::current_dir()
      .unwrap()
      .join("test_attach_multi_main.db");

   let db1_path = std::env::current_dir()
      .unwrap()
      .join("test_attach_multi_db1.db");

   let db2_path = std::env::current_dir()
      .unwrap()
      .join("test_attach_multi_db2.db");

   // Create three databases
   let main_db = SqliteDatabase::connect(&main_path, None).await.unwrap();
   let mut writer = main_db.acquire_writer().await.unwrap();
   sqlx::query("CREATE TABLE main_table (id INTEGER, value TEXT)")
      .execute(&mut *writer)
      .await
      .unwrap();

   sqlx::query("INSERT INTO main_table VALUES (1, 'main')")
      .execute(&mut *writer)
      .await
      .unwrap();

   drop(writer);

   let db1 = SqliteDatabase::connect(&db1_path, None).await.unwrap();
   let mut writer = db1.acquire_writer().await.unwrap();
   sqlx::query("CREATE TABLE db1_table (id INTEGER, value TEXT)")
      .execute(&mut *writer)
      .await
      .unwrap();

   sqlx::query("INSERT INTO db1_table VALUES (2, 'db1')")
      .execute(&mut *writer)
      .await
      .unwrap();

   drop(writer);

   let db2 = SqliteDatabase::connect(&db2_path, None).await.unwrap();
   let mut writer = db2.acquire_writer().await.unwrap();
   sqlx::query("CREATE TABLE db2_table (id INTEGER, value TEXT)")
      .execute(&mut *writer)
      .await
      .unwrap();

   sqlx::query("INSERT INTO db2_table VALUES (3, 'db2')")
      .execute(&mut *writer)
      .await
      .unwrap();

   drop(writer);

   // Attach both databases
   let specs = vec![
      AttachedSpec {
         database: Arc::clone(&db1),
         schema_name: "attached1".to_string(),
         mode: AttachedMode::ReadOnly,
      },
      AttachedSpec {
         database: Arc::clone(&db2),
         schema_name: "attached2".to_string(),
         mode: AttachedMode::ReadOnly,
      },
   ];

   let mut conn = acquire_reader_with_attached(&main_db, specs).await.unwrap();

   // Query across all three databases
   let rows: Vec<(i64, String)> = sqlx::query_as(
      "SELECT id, value FROM main_table
       UNION ALL
       SELECT id, value FROM attached1.db1_table
       UNION ALL
       SELECT id, value FROM attached2.db2_table
       ORDER BY id",
   )
   .fetch_all(&mut *conn)
   .await
   .unwrap();

   assert_eq!(rows.len(), 3);
   assert_eq!(rows[0], (1, "main".to_string()));
   assert_eq!(rows[1], (2, "db1".to_string()));
   assert_eq!(rows[2], (3, "db2".to_string()));

   conn.detach_all().await.unwrap();

   main_db.remove().await.unwrap();
   db1.remove().await.unwrap();
   db2.remove().await.unwrap();
}

#[tokio::test]
async fn test_attach_invalid_schema_name() {
   let main_path = std::env::current_dir()
      .unwrap()
      .join("test_attach_invalid_schema.db");

   let other_path = std::env::current_dir()
      .unwrap()
      .join("test_attach_invalid_other.db");

   let main_db = SqliteDatabase::connect(&main_path, None).await.unwrap();
   let other_db = SqliteDatabase::connect(&other_path, None).await.unwrap();

   // Try invalid schema names
   let invalid_names = vec![
      "123invalid",   // starts with digit
      "invalid-name", // contains hyphen
      "invalid name", // contains space
      "invalid;drop", // contains semicolon
      "",             // empty string
   ];

   for invalid_name in invalid_names {
      let specs = vec![AttachedSpec {
         database: Arc::clone(&other_db),
         schema_name: invalid_name.to_string(),
         mode: AttachedMode::ReadOnly,
      }];

      let result = acquire_reader_with_attached(&main_db, specs).await;
      assert!(
         result.is_err(),
         "Schema name '{}' should be rejected",
         invalid_name
      );
      assert!(matches!(result.unwrap_err(), Error::InvalidSchemaName(_)));
   }

   main_db.remove().await.unwrap();
   other_db.remove().await.unwrap();
}

#[tokio::test]
async fn test_attach_duplicate_database() {
   let main_path = std::env::current_dir()
      .unwrap()
      .join("test_attach_dup_main.db");

   let other_path = std::env::current_dir()
      .unwrap()
      .join("test_attach_dup_other.db");

   let main_db = SqliteDatabase::connect(&main_path, None).await.unwrap();
   let other_db = SqliteDatabase::connect(&other_path, None).await.unwrap();

   // Try to attach same database twice - should fail for writers (requires write lock)
   let specs = vec![
      AttachedSpec {
         database: Arc::clone(&other_db),
         schema_name: "alias1".to_string(),
         mode: AttachedMode::ReadWrite,
      },
      AttachedSpec {
         database: Arc::clone(&other_db),
         schema_name: "alias2".to_string(),
         mode: AttachedMode::ReadWrite,
      },
   ];

   let result = acquire_writer_with_attached(&main_db, specs).await;
   assert!(result.is_err());
   assert!(matches!(
      result.unwrap_err(),
      Error::DuplicateAttachedDatabase(_)
   ));

   main_db.remove().await.unwrap();
   other_db.remove().await.unwrap();
}

#[tokio::test]
async fn test_attach_readonly_allows_reads_only() {
   let main_path = std::env::current_dir()
      .unwrap()
      .join("test_attach_ro_read_main.db");

   let other_path = std::env::current_dir()
      .unwrap()
      .join("test_attach_ro_read_other.db");

   let main_db = SqliteDatabase::connect(&main_path, None).await.unwrap();

   let other_db = SqliteDatabase::connect(&other_path, None).await.unwrap();
   let mut writer = other_db.acquire_writer().await.unwrap();
   sqlx::query("CREATE TABLE test (id INTEGER)")
      .execute(&mut *writer)
      .await
      .unwrap();

   sqlx::query("INSERT INTO test VALUES (42)")
      .execute(&mut *writer)
      .await
      .unwrap();

   drop(writer);

   // Attach as read-only
   let specs = vec![AttachedSpec {
      database: Arc::clone(&other_db),
      schema_name: "readonly_db".to_string(),
      mode: AttachedMode::ReadOnly,
   }];

   let mut conn = acquire_reader_with_attached(&main_db, specs).await.unwrap();

   // Reading from read-only attached database should work
   let (value,): (i64,) = sqlx::query_as("SELECT id FROM readonly_db.test")
      .fetch_one(&mut *conn)
      .await
      .unwrap();

   assert_eq!(value, 42);

   conn.detach_all().await.unwrap();

   main_db.remove().await.unwrap();
   other_db.remove().await.unwrap();
}

#[tokio::test]
async fn test_attach_cannot_attach_readwrite_to_reader() {
   let main_path = std::env::current_dir()
      .unwrap()
      .join("test_attach_rw_reader_main.db");

   let other_path = std::env::current_dir()
      .unwrap()
      .join("test_attach_rw_reader_other.db");

   let main_db = SqliteDatabase::connect(&main_path, None).await.unwrap();
   let other_db = SqliteDatabase::connect(&other_path, None).await.unwrap();

   // Try to attach in read-write mode to a reader connection
   let specs = vec![AttachedSpec {
      database: Arc::clone(&other_db),
      schema_name: "other".to_string(),
      mode: AttachedMode::ReadWrite,
   }];

   let result = acquire_reader_with_attached(&main_db, specs).await;
   assert!(result.is_err());
   assert!(matches!(
      result.unwrap_err(),
      Error::CannotAttachReadWriteToReader
   ));

   main_db.remove().await.unwrap();
   other_db.remove().await.unwrap();
}

#[tokio::test]
async fn test_attach_lock_ordering_prevents_deadlock() {
   let main_path = std::env::current_dir()
      .unwrap()
      .join("test_lock_order_main.db");

   let db1_path = std::env::current_dir()
      .unwrap()
      .join("test_lock_order_db1.db");

   let db2_path = std::env::current_dir()
      .unwrap()
      .join("test_lock_order_db2.db");

   let main_db = SqliteDatabase::connect(&main_path, None).await.unwrap();
   let db1 = SqliteDatabase::connect(&db1_path, None).await.unwrap();
   let db2 = SqliteDatabase::connect(&db2_path, None).await.unwrap();

   // Try to attach in different orders - should work due to alphabetical sorting
   let specs_order1 = vec![
      AttachedSpec {
         database: Arc::clone(&db2),
         schema_name: "db2_alias".to_string(),
         mode: AttachedMode::ReadWrite,
      },
      AttachedSpec {
         database: Arc::clone(&db1),
         schema_name: "db1_alias".to_string(),
         mode: AttachedMode::ReadWrite,
      },
   ];

   let specs_order2 = vec![
      AttachedSpec {
         database: Arc::clone(&db1),
         schema_name: "db1_alias".to_string(),
         mode: AttachedMode::ReadWrite,
      },
      AttachedSpec {
         database: Arc::clone(&db2),
         schema_name: "db2_alias".to_string(),
         mode: AttachedMode::ReadWrite,
      },
   ];

   // Both orderings should work without deadlock due to consistent lock ordering
   let guard1 = acquire_writer_with_attached(&main_db, specs_order1)
      .await
      .unwrap();

   guard1.detach_all().await.unwrap();

   let guard2 = acquire_writer_with_attached(&main_db, specs_order2)
      .await
      .unwrap();

   guard2.detach_all().await.unwrap();

   main_db.remove().await.unwrap();
   db1.remove().await.unwrap();
   db2.remove().await.unwrap();
}
