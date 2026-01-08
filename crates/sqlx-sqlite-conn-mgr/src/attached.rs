//! Attached database support for cross-database queries

use crate::Result;
use crate::database::SqliteDatabase;
use crate::error::Error;
use crate::write_guard::WriteGuard;
use sqlx::Sqlite;
use sqlx::pool::PoolConnection;
use sqlx::sqlite::SqliteConnection;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

/// Specification for attaching a database to a connection
#[derive(Clone)]
pub struct AttachedSpec {
   /// The database to attach
   pub database: Arc<SqliteDatabase>,
   /// Schema name to use for the attached database (e.g., "other", "logs")
   pub schema_name: String,
   /// Whether to attach as read-only or read-write
   pub mode: AttachedMode,
}

/// Mode for attaching a database
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttachedMode {
   /// Attach database as read-only
   ReadOnly,
   /// Attach database as read-write (requires acquiring the database's writer)
   ReadWrite,
}

/// Guard holding a read connection with attached database(s)
///
/// **Important**: Call `detach_all()` before dropping to properly clean up attached database(s).
/// Without explicit cleanup, attached databases persist on the pooled connection until
/// it's eventually closed. Derefs to `SqliteConnection` for executing queries.
#[must_use = "if unused, the attached connection and locks are immediately dropped"]
#[derive(Debug)]
pub struct AttachedReadConnection {
   conn: PoolConnection<Sqlite>,
   /// Write locks for attached databases in ReadWrite mode.
   /// These are never read directly but must be held for their entire lifetime
   /// to prevent other operations from writing to attached databases.
   /// Locks are automatically released when this guard is dropped.
   #[allow(dead_code)]
   held_writers: Vec<WriteGuard>,
   /// Schema names of attached databases, retained for debugging utility.
   #[allow(dead_code)]
   schema_names: Vec<String>,
}

impl AttachedReadConnection {
   pub(crate) fn new(
      conn: PoolConnection<Sqlite>,
      held_writers: Vec<WriteGuard>,
      schema_names: Vec<String>,
   ) -> Self {
      Self {
         conn,
         held_writers,
         schema_names,
      }
   }

   /// Explicitly detach all attached databases.
   ///
   /// This method should be called before dropping the connection to ensure
   /// attached databases are properly cleaned up. Without calling this,
   /// attached databases may persist when the connection is returned to the pool.
   pub async fn detach_all(mut self) -> Result<()> {
      for schema_name in &self.schema_names {
         let detach_sql = format!("DETACH DATABASE {}", schema_name);
         sqlx::query(&detach_sql).execute(&mut *self.conn).await?;
      }
      Ok(())
   }
}

impl Deref for AttachedReadConnection {
   type Target = SqliteConnection;

   fn deref(&self) -> &Self::Target {
      &self.conn
   }
}

impl DerefMut for AttachedReadConnection {
   fn deref_mut(&mut self) -> &mut Self::Target {
      &mut self.conn
   }
}

impl Drop for AttachedReadConnection {
   fn drop(&mut self) {
      // Cannot reliably execute async DETACH in synchronous Drop.
      // Call detach_all() before dropping to ensure cleanup.
      // Otherwise, databases remain attached until connection is eventually closed.
      // Note: held_writers are also dropped here, releasing write locks.
   }
}

/// Guard holding a write connection with attached database(s)
///
/// **Important**: Call `detach_all()` before dropping to properly clean up attached databases.
/// Without explicit cleanup, attached databases persist on the pooled connection until
/// it's eventually closed. Derefs to `SqliteConnection` for executing queries.
#[must_use = "if unused, the write guard and locks are immediately dropped"]
#[derive(Debug)]
pub struct AttachedWriteGuard {
   writer: WriteGuard,
   /// Write locks for attached databases in ReadWrite mode.
   /// These are never read directly but must be held for their entire lifetime
   /// to prevent other operations from writing to attached databases.
   /// Locks are automatically released when this guard is dropped.
   #[allow(dead_code)]
   held_writers: Vec<WriteGuard>,
   /// Schema names of attached databases, retained for debugging utility.
   #[allow(dead_code)]
   schema_names: Vec<String>,
}

impl AttachedWriteGuard {
   pub(crate) fn new(
      writer: WriteGuard,
      held_writers: Vec<WriteGuard>,
      schema_names: Vec<String>,
   ) -> Self {
      Self {
         writer,
         held_writers,
         schema_names,
      }
   }

   /// Explicitly detach all attached databases.
   ///
   /// This method should be called before dropping the connection to ensure
   /// attached databases are properly cleaned up. Without calling this,
   /// attached databases may persist when the connection is returned to the pool.
   pub async fn detach_all(mut self) -> Result<()> {
      for schema_name in &self.schema_names {
         let detach_sql = format!("DETACH DATABASE {}", schema_name);
         sqlx::query(&detach_sql).execute(&mut *self.writer).await?;
      }
      Ok(())
   }
}

impl Deref for AttachedWriteGuard {
   type Target = SqliteConnection;

   fn deref(&self) -> &Self::Target {
      &self.writer
   }
}

impl DerefMut for AttachedWriteGuard {
   fn deref_mut(&mut self) -> &mut Self::Target {
      &mut self.writer
   }
}

impl Drop for AttachedWriteGuard {
   fn drop(&mut self) {
      // Cannot reliably execute async DETACH in synchronous Drop.
      // Call detach_all() before dropping to ensure cleanup.
      // Otherwise, databases remain attached until connection is eventually closed.
      // Note: held_writers are also dropped here, releasing write locks.
   }
}

/// Validates that a schema name is a valid SQLite identifier
///
/// A valid schema name:
/// - Must not be empty
/// - Must contain only ASCII alphanumeric characters and underscores
/// - Must not start with a digit
///
/// This prevents SQL injection by ensuring the schema name can only be used
/// as an identifier and cannot:
/// - Terminate statements (;)
/// - Start comments (--)
/// - Break out of string context (')
/// - Execute any SQL operations
fn is_valid_schema_name(name: &str) -> bool {
   !name.is_empty()
      && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
      && !name.chars().next().unwrap().is_ascii_digit()
}

/// Acquire a read connection with attached database(s)
///
/// This function:
/// 1. Acquires a read connection from the main database's read pool
/// 2. For each attached spec:
///    - Validates the attached mode (read-only connections cannot attach read-write)
///    - Executes ATTACH DATABASE statement
/// 3. Returns an `AttachedReadConnection` guard that auto-detaches on drop
///
/// # Arguments
///
/// * `main_db` - The main database to acquire a connection from
/// * `specs` - Specifications for databases to attach
///
/// # Errors
///
/// Returns an error if:
/// - The main database is closed
/// - Cannot acquire a read connection
/// - Attempting to attach read-write to a read connection
/// - ATTACH DATABASE fails
pub async fn acquire_reader_with_attached(
   main_db: &SqliteDatabase,
   mut specs: Vec<AttachedSpec>,
) -> Result<AttachedReadConnection> {
   // Acquire read connection from main database
   let mut conn = main_db.read_pool()?.acquire().await?;

   // Sort specs by database path to prevent deadlocks when multiple callers
   // attach the same databases in different orders.
   // This matches the sorting in acquire_writer_with_attached (by path)
   // to maintain consistent global ordering and prevent deadlocks.
   specs.sort_by(|a, b| a.database.path_str().cmp(&b.database.path_str()));

   // Check for duplicate database paths (same as in acquire_writer_with_attached)
   // SQLite doesn't allow attaching the same database file multiple times,
   // and this likely indicates a programming error
   use std::collections::HashSet;
   let mut seen_paths = HashSet::new();
   for spec in &specs {
      let path = spec.database.path_str();
      if !seen_paths.insert(path.clone()) {
         return Err(Error::DuplicateAttachedDatabase(path));
      }
   }

   let mut schema_names = Vec::new();

   for spec in specs {
      // Validate schema name to prevent SQL injection
      if !is_valid_schema_name(&spec.schema_name) {
         return Err(Error::InvalidSchemaName(spec.schema_name.clone()));
      }

      // Read connections can only attach as read-only
      if spec.mode == AttachedMode::ReadWrite {
         return Err(Error::CannotAttachReadWriteToReader);
      }

      // Execute ATTACH DATABASE
      // Schema name is validated above to contain only safe identifier characters
      let path = spec.database.path_str();
      let escaped_path = path.replace("'", "''");
      let attach_sql = format!("ATTACH DATABASE '{}' AS {}", escaped_path, spec.schema_name);
      sqlx::query(&attach_sql).execute(&mut *conn).await?;

      schema_names.push(spec.schema_name);
   }

   Ok(AttachedReadConnection::new(conn, Vec::new(), schema_names))
}

/// Acquire a write connection with attached database(s)
///
/// This function:
/// 1. Acquires the write connection from the main database
/// 2. For each attached spec:
///    - If read-write mode: acquires the attached database's writer first
///    - Executes ATTACH DATABASE statement
/// 3. Returns an `AttachedWriteGuard` that auto-detaches on drop
///
/// Acquiring attached database writers first ensures proper locking order and
/// prevents other operations from writing to those databases while attached.
///
/// # Arguments
///
/// * `main_db` - The main database to acquire a writer from
/// * `specs` - Specifications for databases to attach
///
/// # Errors
///
/// Returns an error if:
/// - The main database is closed
/// - Cannot acquire the main writer
/// - Cannot acquire an attached database's writer (for read-write mode)
/// - ATTACH DATABASE fails
pub async fn acquire_writer_with_attached(
   main_db: &SqliteDatabase,
   specs: Vec<AttachedSpec>,
) -> Result<AttachedWriteGuard> {
   // Validate schema names first
   for spec in &specs {
      if !is_valid_schema_name(&spec.schema_name) {
         return Err(Error::InvalidSchemaName(spec.schema_name.clone()));
      }
   }

   // CRITICAL: To prevent deadlocks, we must acquire locks in a consistent global order.
   // Example deadlock without global ordering:
   //   Thread 1: main=A, attach B → acquires A, then B
   //   Thread 2: main=B, attach A → acquires B, then A
   // Solution: Sort ALL databases (main + read-write attached) by path before acquiring locks.

   let main_path = main_db.path_str();

   // Collect all databases that need write locks with their paths
   let mut db_entries: Vec<(String, &SqliteDatabase)> = vec![(main_path.clone(), main_db)];

   for spec in &specs {
      if spec.mode == AttachedMode::ReadWrite {
         db_entries.push((spec.database.path_str(), &*spec.database));
      }
   }

   // Check for duplicates (can happen via: main db in specs, same file attached
   // multiple times, or programmatic/config-driven attachment with duplicate paths)
   // This prevents deadlock from trying to acquire the same writer twice
   use std::collections::HashSet;
   let mut seen_paths = HashSet::new();
   for (path, _) in &db_entries {
      if !seen_paths.insert(path.as_str()) {
         return Err(Error::DuplicateAttachedDatabase(path.clone()));
      }
   }

   // Sort by path for consistent global ordering
   db_entries.sort_by(|a, b| a.0.cmp(&b.0));

   // Find main database index in sorted order
   let main_writer_idx = db_entries
      .iter()
      .position(|(path, _)| path == &main_path)
      .expect("main database must be in the list");

   // Acquire all write locks in sorted order
   let mut all_writers = Vec::new();
   for (_, db) in &db_entries {
      all_writers.push(db.acquire_writer().await?);
   }

   // Extract the main writer, keep others as held locks
   let mut writer = all_writers.remove(main_writer_idx);
   let held_writers = all_writers;

   // Execute ATTACH commands
   let mut schema_names = Vec::new();

   for spec in specs {
      let path = spec.database.path_str();
      let escaped_path = path.replace("'", "''");
      let attach_sql = format!("ATTACH DATABASE '{}' AS {}", escaped_path, spec.schema_name);
      sqlx::query(&attach_sql).execute(&mut *writer).await?;

      schema_names.push(spec.schema_name);
   }

   Ok(AttachedWriteGuard::new(writer, held_writers, schema_names))
}

#[cfg(test)]
mod tests {
   use super::*;
   use crate::SqliteDatabase;
   use sqlx::Row;
   use std::sync::Arc;
   use tempfile::TempDir;

   async fn create_test_db(name: &str, temp_dir: &TempDir) -> Arc<SqliteDatabase> {
      let path = temp_dir.path().join(name);
      let db = SqliteDatabase::connect(&path, None).await.unwrap();

      // Create a test table
      let mut writer = db.acquire_writer().await.unwrap();
      sqlx::query(&format!(
         "CREATE TABLE IF NOT EXISTS {} (id INTEGER PRIMARY KEY, value TEXT)",
         name.replace(".db", "")
      ))
      .execute(&mut *writer)
      .await
      .unwrap();

      // Insert test data
      sqlx::query(&format!(
         "INSERT INTO {} (value) VALUES ('test_data')",
         name.replace(".db", "")
      ))
      .execute(&mut *writer)
      .await
      .unwrap();

      db
   }

   #[tokio::test]
   async fn test_attach_readonly_to_reader() {
      let temp_dir = TempDir::new().unwrap();
      let main_db = create_test_db("main.db", &temp_dir).await;
      let other_db = create_test_db("other.db", &temp_dir).await;

      let specs = vec![AttachedSpec {
         database: other_db.clone(),
         schema_name: "other".to_string(),
         mode: AttachedMode::ReadOnly,
      }];

      let mut conn = acquire_reader_with_attached(&main_db, specs).await.unwrap();

      // Query from attached database
      let row = sqlx::query("SELECT value FROM other.other LIMIT 1")
         .fetch_one(&mut *conn)
         .await
         .unwrap();

      let value: String = row.get(0);
      assert_eq!(value, "test_data");
   }

   #[tokio::test]
   async fn test_attach_readonly_to_writer() {
      let temp_dir = TempDir::new().unwrap();
      let main_db = create_test_db("main.db", &temp_dir).await;
      let other_db = create_test_db("other.db", &temp_dir).await;

      let specs = vec![AttachedSpec {
         database: other_db.clone(),
         schema_name: "other".to_string(),
         mode: AttachedMode::ReadOnly,
      }];

      let mut conn = acquire_writer_with_attached(&main_db, specs).await.unwrap();

      // Query from attached database
      let row = sqlx::query("SELECT value FROM other.other LIMIT 1")
         .fetch_one(&mut *conn)
         .await
         .unwrap();

      let value: String = row.get(0);
      assert_eq!(value, "test_data");
   }

   #[tokio::test]
   async fn test_attach_readwrite_to_writer() {
      let temp_dir = TempDir::new().unwrap();
      let main_db = create_test_db("main.db", &temp_dir).await;
      let other_db = create_test_db("other.db", &temp_dir).await;

      let specs = vec![AttachedSpec {
         database: other_db.clone(),
         schema_name: "other".to_string(),
         mode: AttachedMode::ReadWrite,
      }];

      let mut conn = acquire_writer_with_attached(&main_db, specs).await.unwrap();

      // Write to attached database
      sqlx::query("INSERT INTO other.other (value) VALUES ('new_data')")
         .execute(&mut *conn)
         .await
         .unwrap();

      // Read back the data
      let row = sqlx::query("SELECT value FROM other.other WHERE value = 'new_data'")
         .fetch_one(&mut *conn)
         .await
         .unwrap();

      let value: String = row.get(0);
      assert_eq!(value, "new_data");
   }

   #[tokio::test]
   async fn test_attach_readwrite_to_reader_fails() {
      let temp_dir = TempDir::new().unwrap();
      let main_db = create_test_db("main.db", &temp_dir).await;
      let other_db = create_test_db("other.db", &temp_dir).await;

      let specs = vec![AttachedSpec {
         database: other_db.clone(),
         schema_name: "other".to_string(),
         mode: AttachedMode::ReadWrite,
      }];

      let result = acquire_reader_with_attached(&main_db, specs).await;
      assert!(result.is_err());
      assert!(matches!(
         result.unwrap_err(),
         Error::CannotAttachReadWriteToReader
      ));
   }

   #[tokio::test]
   async fn test_attach_multiple_databases() {
      let temp_dir = TempDir::new().unwrap();
      let main_db = create_test_db("main.db", &temp_dir).await;
      let db1 = create_test_db("db1.db", &temp_dir).await;
      let db2 = create_test_db("db2.db", &temp_dir).await;

      let specs = vec![
         AttachedSpec {
            database: db1.clone(),
            schema_name: "db1".to_string(),
            mode: AttachedMode::ReadOnly,
         },
         AttachedSpec {
            database: db2.clone(),
            schema_name: "db2".to_string(),
            mode: AttachedMode::ReadOnly,
         },
      ];

      let mut conn = acquire_reader_with_attached(&main_db, specs).await.unwrap();

      // Query from both attached databases
      let row1 = sqlx::query("SELECT value FROM db1.db1 LIMIT 1")
         .fetch_one(&mut *conn)
         .await
         .unwrap();

      let value1: String = row1.get(0);
      assert_eq!(value1, "test_data");

      let row2 = sqlx::query("SELECT value FROM db2.db2 LIMIT 1")
         .fetch_one(&mut *conn)
         .await
         .unwrap();

      let value2: String = row2.get(0);
      assert_eq!(value2, "test_data");
   }

   #[tokio::test]
   async fn test_attached_database_in_readwrite_mode_holds_writer_lock() {
      let temp_dir = TempDir::new().unwrap();
      let main_db = create_test_db("main.db", &temp_dir).await;
      let other_db = create_test_db("other.db", &temp_dir).await;

      let specs = vec![AttachedSpec {
         database: other_db.clone(),
         schema_name: "other".to_string(),
         mode: AttachedMode::ReadWrite,
      }];

      // Acquire writer with attached database (holds other_db's writer)
      let _guard = acquire_writer_with_attached(&main_db, specs).await.unwrap();

      // Try to acquire other_db's writer directly - should block/timeout
      let acquire_result = tokio::time::timeout(
         std::time::Duration::from_millis(100),
         other_db.acquire_writer(),
      )
      .await;

      // Should timeout because the writer is already held by attached connection
      assert!(
         acquire_result.is_err(),
         "Expected timeout acquiring writer that's already held"
      );
   }

   #[tokio::test]
   async fn test_locks_released_on_drop() {
      let temp_dir = TempDir::new().unwrap();
      let main_db = create_test_db("main.db", &temp_dir).await;
      let other_db = create_test_db("other.db", &temp_dir).await;

      let specs = vec![AttachedSpec {
         database: other_db.clone(),
         schema_name: "other".to_string(),
         mode: AttachedMode::ReadWrite,
      }];

      // Acquire and drop
      {
         let _ = acquire_writer_with_attached(&main_db, specs).await.unwrap();
         // Dropped at end of scope
      }

      // Should now be able to acquire other_db's writer
      let writer = other_db.acquire_writer().await;
      assert!(
         writer.is_ok(),
         "Writer should be available after attached connection dropped"
      );
   }

   #[tokio::test]
   async fn test_cross_database_join_query() {
      let temp_dir = TempDir::new().unwrap();

      // Create main database with users
      let main_db = SqliteDatabase::connect(temp_dir.path().join("main.db"), None)
         .await
         .unwrap();

      let mut writer = main_db.acquire_writer().await.unwrap();
      sqlx::query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
         .execute(&mut *writer)
         .await
         .unwrap();

      sqlx::query("INSERT INTO users (id, name) VALUES (1, 'Alice')")
         .execute(&mut *writer)
         .await
         .unwrap();

      drop(writer);

      // Create orders database
      let orders_db = SqliteDatabase::connect(temp_dir.path().join("orders.db"), None)
         .await
         .unwrap();

      let mut writer = orders_db.acquire_writer().await.unwrap();
      sqlx::query("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL)")
         .execute(&mut *writer)
         .await
         .unwrap();

      sqlx::query("INSERT INTO orders (id, user_id, total) VALUES (100, 1, 99.99)")
         .execute(&mut *writer)
         .await
         .unwrap();

      drop(writer);

      // Attach orders database and perform cross-database join
      let specs = vec![AttachedSpec {
         database: orders_db,
         schema_name: "orders".to_string(),
         mode: AttachedMode::ReadOnly,
      }];

      let mut conn = acquire_reader_with_attached(&main_db, specs).await.unwrap();

      let row = sqlx::query(
         "SELECT u.name, o.total FROM main.users u JOIN orders.orders o ON u.id = o.user_id",
      )
      .fetch_one(&mut *conn)
      .await
      .unwrap();

      let name: String = row.get(0);
      let total: f64 = row.get(1);
      assert_eq!(name, "Alice");
      assert_eq!(total, 99.99);
   }

   #[tokio::test]
   async fn test_sorting_attached_databases_prevents_deadlock() {
      let temp_dir = TempDir::new().unwrap();
      let main_db = create_test_db("main.db", &temp_dir).await;
      let db_a = create_test_db("a.db", &temp_dir).await;
      let db_z = create_test_db("z.db", &temp_dir).await;

      // Specs in reverse alphabetical order
      let specs = vec![
         AttachedSpec {
            database: db_z.clone(),
            schema_name: "z".to_string(),
            mode: AttachedMode::ReadWrite,
         },
         AttachedSpec {
            database: db_a.clone(),
            schema_name: "a".to_string(),
            mode: AttachedMode::ReadWrite,
         },
      ];

      // Should succeed and acquire in sorted order (a, z) to prevent deadlock
      let result = acquire_writer_with_attached(&main_db, specs).await;
      assert!(
         result.is_ok(),
         "Attachment should succeed with sorted acquisition order"
      );
   }

   #[tokio::test]
   async fn test_attaching_same_databases_in_different_order_concurrently_no_deadlock() {
      // This test verifies the fix for the deadlock scenario:
      // Thread 1: main=A, attach B
      // Thread 2: main=B, attach A
      // Without global lock ordering, these could deadlock.
      // With the fix, both acquire locks in the same order (A, B), preventing deadlock.

      let temp_dir = TempDir::new().unwrap();
      let db_a = create_test_db("a.db", &temp_dir).await;
      let db_b = create_test_db("b.db", &temp_dir).await;

      let db_a_clone = db_a.clone();
      let db_b_clone = db_b.clone();

      let task1 = tokio::spawn(async move {
         // Main=A, attach B → will acquire in order: A, B
         let specs = vec![AttachedSpec {
            database: db_b_clone,
            schema_name: "b_schema".to_string(),
            mode: AttachedMode::ReadWrite,
         }];
         let guard = acquire_writer_with_attached(&db_a_clone, specs).await?;
         // Drop immediately to release locks
         drop(guard);
         Ok::<_, crate::Error>(())
      });

      let task2 = tokio::spawn(async move {
         // Main=B, attach A → will also acquire in order: A, B (sorted)
         let specs = vec![AttachedSpec {
            database: db_a,
            schema_name: "a_schema".to_string(),
            mode: AttachedMode::ReadWrite,
         }];
         let guard = acquire_writer_with_attached(&db_b, specs).await?;
         drop(guard);
         Ok::<_, crate::Error>(())
      });

      // Use a timeout to ensure we don't deadlock
      let timeout_duration = std::time::Duration::from_secs(5);
      let result =
         tokio::time::timeout(timeout_duration, async { tokio::try_join!(task1, task2) }).await;

      // Should complete within timeout (no deadlock)
      assert!(
         result.is_ok(),
         "Should complete without deadlock within {} seconds",
         timeout_duration.as_secs()
      );

      // Both tasks should succeed (they run sequentially due to lock ordering)
      let (res1, res2) = result.unwrap().unwrap();
      assert!(res1.is_ok() && res2.is_ok(), "Both tasks should succeed");
   }

   #[tokio::test]
   async fn test_invalid_schema_names_rejected() {
      let temp_dir = TempDir::new().unwrap();
      let main_db = create_test_db("main.db", &temp_dir).await;
      let other_db = create_test_db("other.db", &temp_dir).await;

      // Test various invalid schema names
      let invalid_names = vec![
         "",                        // Empty
         "123invalid",              // Starts with digit
         "schema-name",             // Contains hyphen
         "schema name",             // Contains space
         "schema;DROP TABLE users", // SQL injection attempt
         "schema'--",               // SQL injection attempt
         "schema/*comment*/",       // Contains special chars
      ];

      for invalid_name in invalid_names {
         let specs = vec![AttachedSpec {
            database: other_db.clone(),
            schema_name: invalid_name.to_string(),
            mode: AttachedMode::ReadOnly,
         }];

         let result = acquire_reader_with_attached(&main_db, specs).await;
         assert!(
            matches!(result, Err(Error::InvalidSchemaName(_))),
            "Expected InvalidSchemaName error for '{}'",
            invalid_name
         );
      }
   }

   #[tokio::test]
   async fn test_duplicate_attached_database_rejected() {
      let temp_dir = TempDir::new().unwrap();
      let main_db = create_test_db("main.db", &temp_dir).await;
      let other_db = create_test_db("other.db", &temp_dir).await;

      // Attempt to attach other_db twice
      let specs = vec![
         AttachedSpec {
            database: other_db.clone(),
            schema_name: "other1".to_string(),
            mode: AttachedMode::ReadWrite,
         },
         AttachedSpec {
            database: other_db.clone(),
            schema_name: "other2".to_string(),
            mode: AttachedMode::ReadWrite,
         },
      ];

      let result = acquire_writer_with_attached(&main_db, specs).await;
      assert!(
         matches!(result, Err(Error::DuplicateAttachedDatabase(_))),
         "Should reject duplicate attached database"
      );
   }

   #[tokio::test]
   async fn test_main_db_in_attached_list_rejected() {
      let temp_dir = TempDir::new().unwrap();
      let main_db = create_test_db("main.db", &temp_dir).await;

      // Attempt to attach main_db to itself
      let specs = vec![AttachedSpec {
         database: main_db.clone(),
         schema_name: "main_copy".to_string(),
         mode: AttachedMode::ReadWrite,
      }];

      let result = acquire_writer_with_attached(&main_db, specs).await;
      assert!(
         matches!(result, Err(Error::DuplicateAttachedDatabase(_))),
         "Should reject attaching main database to itself"
      );
   }

   #[tokio::test]
   async fn test_path_with_single_quotes() {
      let temp_dir = TempDir::new().unwrap();

      // Create a subdirectory with a single quote in the name
      let quoted_dir = temp_dir.path().join("user's_data");
      std::fs::create_dir(&quoted_dir).unwrap();

      let main_db = SqliteDatabase::connect(temp_dir.path().join("main.db"), None)
         .await
         .unwrap();

      // Create database in path with single quote
      let other_path = quoted_dir.join("other.db");
      let other_db = SqliteDatabase::connect(&other_path, None).await.unwrap();

      // Attach database with path containing single quote - should succeed
      let specs = vec![AttachedSpec {
         database: other_db,
         schema_name: "other".to_string(),
         mode: AttachedMode::ReadOnly,
      }];

      let result = acquire_reader_with_attached(&main_db, specs).await;
      assert!(
         result.is_ok(),
         "Should attach database with single quote in path"
      );
   }
}
