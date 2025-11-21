//! SQLite database with connection pooling and optional write access

use crate::Result;
use crate::config::SqliteDatabaseConfig;
use crate::error::Error;
use crate::registry::{get_or_open_database, is_memory_database, uncache_database};
use crate::write_guard::WriteGuard;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{ConnectOptions, Pool, Sqlite};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::error;

/// SQLite database with connection pooling for concurrent reads and optional exclusive writes.
///
/// Once the database is opened it can be used for read-only operations by calling `read_pool()`.
/// Write operations are available by calling `acquire_writer()` which lazily initializes WAL mode
/// on first use.
///
/// # Example
///
/// ```no_run
/// use sqlx_sqlite_conn_mgr::SqliteDatabase;
/// use std::sync::Arc;
///
/// # async fn example() -> Result<(), sqlx_sqlite_conn_mgr::Error> {
/// let db = SqliteDatabase::connect("test.db", None).await?;
///
/// // Use read_pool for SELECT queries (concurrent reads)
/// let rows = sqlx::query("SELECT * FROM users")
///     .fetch_all(db.read_pool()?)
///     .await?;
///
/// // Optionally acquire writer for INSERT/UPDATE/DELETE (exclusive)
/// let mut writer = db.acquire_writer().await?;
/// sqlx::query("INSERT INTO users (name) VALUES (?)")
///     .bind("Alice")
///     .execute(&mut *writer)
///     .await?;
///
/// db.close().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct SqliteDatabase {
   /// Pool of read-only connections (defaults to max_connections=6) for concurrent reads
   read_pool: Pool<Sqlite>,

   /// Single read-write connection pool (max_connections=1) for serialized writes
   write_conn: Pool<Sqlite>,

   /// Tracks if WAL mode has been initialized (set on first write)
   wal_initialized: AtomicBool,

   /// Marks database as closed to prevent further operations
   closed: AtomicBool,

   /// Path to database file (used for cleanup and registry lookups)
   path: PathBuf,
}

impl SqliteDatabase {
   /// Connect to a SQLite database
   ///
   /// If the database is already connected, returns the existing connection.
   /// Multiple calls with the same path will return the same database instance.
   ///
   /// The database is created if it doesn't exist. WAL mode is enabled when
   /// `acquire_writer()` is first called.
   ///
   /// # Arguments
   ///
   /// * `path` - Path to the SQLite database file (will be created if missing)
   /// * `custom_config` - Optional custom configuration for connection pools.
   ///   Pass `None` to use defaults (6 max read connections, 30 second idle timeout).
   ///   Specify a custom configuration when the defaults don't meet your requirements.
   ///
   /// # Examples
   ///
   /// ```no_run
   /// use sqlx_sqlite_conn_mgr::SqliteDatabase;
   /// use std::sync::Arc;
   ///
   /// # async fn example() -> Result<(), sqlx_sqlite_conn_mgr::Error> {
   /// // Connect with default configuration (recommended for most use cases)
   /// let db = SqliteDatabase::connect("test.db", None).await?;
   /// # Ok(())
   /// # }
   /// ```
   ///
   /// ```no_run
   /// use sqlx_sqlite_conn_mgr::{SqliteDatabase, SqliteDatabaseConfig};
   /// use std::time::Duration;
   /// use std::sync::Arc;
   ///
   /// # async fn example() -> Result<(), sqlx_sqlite_conn_mgr::Error> {
   /// // Customize configuration when defaults don't meet your requirements
   /// let custom_config = SqliteDatabaseConfig {
   ///    max_read_connections: 10,
   ///    idle_timeout: Duration::from_secs(60),
   /// };
   /// let db = SqliteDatabase::connect("test.db", Some(custom_config)).await?;
   /// # Ok(())
   /// # }
   /// ```
   pub async fn connect(
      path: impl AsRef<Path>,
      custom_config: Option<SqliteDatabaseConfig>,
   ) -> Result<Arc<Self>> {
      let config = custom_config.unwrap_or_default();
      let path = path.as_ref();

      // Validate path is not empty
      if path.as_os_str().is_empty() {
         return Err(crate::error::Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Database path cannot be empty",
         )));
      }

      let path = path.to_path_buf();

      get_or_open_database(&path, || async {
         // Check if database file exists
         let db_exists = path.exists();

         // If database doesn't exist and not :memory:, create it with a temporary connection
         // We don't keep this connection - WAL mode will be set later in acquire_writer()
         //
         // Why do we need to manually create the database file? We could just let the connection
         // create it if it doesn't exist, using `create_if_missing(true)`, right? Not if we called
         // connect and then our very first query was a read-only query, like `PRAGMA user_version;`,
         // for example. That would fail because the read pool connections are read-only and cannot
         // create the file
         if !db_exists && !is_memory_database(&path) {
            let create_options = SqliteConnectOptions::new()
               .filename(&path)
               .create_if_missing(true)
               .read_only(false);

            // Create database file with a temporary connection
            let conn = create_options.connect().await?;
            drop(conn); // Close immediately after creating the file
         }

         // Create read pool with read-only connections
         let read_options = SqliteConnectOptions::new().filename(&path).read_only(true);

         let read_pool = SqlitePoolOptions::new()
            .max_connections(config.max_read_connections)
            .min_connections(0)
            .idle_timeout(Some(config.idle_timeout))
            .connect_with(read_options)
            .await?;

         // Create write pool with a single read-write connection
         let write_options = SqliteConnectOptions::new().filename(&path).read_only(false);

         let write_conn = SqlitePoolOptions::new()
            .max_connections(1)
            .min_connections(0)
            .idle_timeout(Some(config.idle_timeout))
            .connect_with(write_options)
            .await?;

         Ok(Self {
            read_pool,
            write_conn,
            wal_initialized: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            path: path.clone(),
         })
      })
      .await
   }

   /// Get a reference to the connection pool for executing read queries
   ///
   /// Use this for concurrent read operations. Multiple readers can access
   /// the pool simultaneously.
   ///
   /// # Example
   ///
   /// ```no_run
   /// use sqlx_sqlite_conn_mgr::SqliteDatabase;
   /// use sqlx::query;
   /// use std::sync::Arc;
   ///
   /// # async fn example() -> Result<(), sqlx_sqlite_conn_mgr::Error> {
   /// let db = SqliteDatabase::connect("test.db", None).await?;
   /// let result = query("SELECT * FROM users")
   ///     .fetch_all(db.read_pool()?)
   ///     .await?;
   /// # Ok(())
   /// # }
   /// ```
   pub fn read_pool(&self) -> Result<&Pool<Sqlite>> {
      if self.closed.load(Ordering::SeqCst) {
         return Err(Error::DatabaseClosed);
      }
      Ok(&self.read_pool)
   }

   /// Acquire exclusive write access to the database
   ///
   /// This method returns a `WriteGuard` that provides exclusive access to
   /// the single write connection. Only one writer can exist at a time.
   ///
   /// On the first call, this method will enable WAL mode on the database.
   /// Subsequent calls reuse the same write connection.
   ///
   /// # Example
   ///
   /// ```no_run
   /// use sqlx_sqlite_conn_mgr::SqliteDatabase;
   /// use sqlx::query;
   /// use std::sync::Arc;
   ///
   /// # async fn example() -> Result<(), sqlx_sqlite_conn_mgr::Error> {
   /// let db = SqliteDatabase::connect("test.db", None).await?;
   /// let mut writer = db.acquire_writer().await?;
   /// query("INSERT INTO users (name) VALUES (?)")
   ///     .bind("Alice")
   ///     .execute(&mut *writer)
   ///     .await?;
   /// # Ok(())
   /// # }
   /// ```
   pub async fn acquire_writer(&self) -> Result<WriteGuard> {
      if self.closed.load(Ordering::SeqCst) {
         return Err(Error::DatabaseClosed);
      }

      // Acquire connection from pool (max=1 ensures exclusive access)
      let mut conn = self.write_conn.acquire().await?;

      // Initialize WAL mode on first use (idempotent and safe)
      if !self.wal_initialized.load(Ordering::SeqCst) {
         sqlx::query("PRAGMA journal_mode = WAL")
            .execute(&mut *conn)
            .await?;

         // https://www.sqlite.org/wal.html#performance_considerations
         sqlx::query("PRAGMA synchronous = NORMAL")
            .execute(&mut *conn)
            .await?;

         self.wal_initialized.store(true, Ordering::SeqCst);
      }

      // Return WriteGuard wrapping the pool connection
      Ok(WriteGuard::new(conn))
   }

   /// Close the database and clean up resources
   ///
   /// This closes all connections in the pool and removes the database from the cache.
   /// After calling close, any operations on this database will return `Error::DatabaseClosed`.
   ///
   /// Note: Takes `Arc<Self>` to consume ownership, preventing use-after-close at compile time.
   /// The registry stores `Weak` references, so when this Arc is dropped, the database is freed.
   ///
   /// # Example
   ///
   /// ```no_run
   /// use sqlx_sqlite_conn_mgr::SqliteDatabase;
   ///
   /// # async fn example() -> Result<(), sqlx_sqlite_conn_mgr::Error> {
   /// let db = SqliteDatabase::connect("test.db", None).await?;
   /// // ... use database ...
   /// db.close().await?;
   /// # Ok(())
   /// # }
   /// ```
   pub async fn close(self: Arc<Self>) -> Result<()> {
      // Mark as closed
      self.closed.store(true, Ordering::SeqCst);

      // Remove from registry
      if let Err(e) = uncache_database(&self.path).await {
         error!("Failed to remove database from cache: {}", e);
      }

      // This will await all readers to be returned
      self.read_pool.close().await;

      // Checkpoint WAL before closing the write connection to flush changes and truncate WAL file
      // Only attempt if WAL was initialized (write connection was used)
      if self.wal_initialized.load(Ordering::SeqCst) {
         if let Ok(mut conn) = self.write_conn.acquire().await {
            let _ = sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
               .execute(&mut *conn)
               .await;
         }
      }

      self.write_conn.close().await;

      Ok(())
   }

   /// Close the database and delete all database files
   ///
   /// This closes all connections and then deletes the database file,
   /// WAL file, and SHM file from disk. Use with caution!
   ///
   /// Note: Takes `Arc<Self>` to consume ownership, preventing use-after-close at compile time.
   /// The registry stores `Weak` references, so when this Arc is dropped, the database is freed.
   ///
   /// # Example
   ///
   /// ```no_run
   /// use sqlx_sqlite_conn_mgr::SqliteDatabase;
   ///
   /// # async fn example() -> Result<(), sqlx_sqlite_conn_mgr::Error> {
   /// let db = SqliteDatabase::connect("temp.db", None).await?;
   /// // ... use database ...
   /// db.remove().await?;
   /// # Ok(())
   /// # }
   /// ```
   pub async fn remove(self: Arc<Self>) -> Result<()> {
      // Clone path before closing (since close consumes self)
      let path = self.path.clone();

      // Close all connections and clean up
      self.close().await?;

      // Remove main database file - propagate errors (file should exist)
      std::fs::remove_file(&path).map_err(Error::Io)?;

      // Remove WAL and SHM files - ignore "not found" but propagate other errors
      // (these files may not exist if WAL was never initialized)
      let wal_path = path.with_extension("db-wal");
      if let Err(e) = std::fs::remove_file(&wal_path) {
         if e.kind() != std::io::ErrorKind::NotFound {
            return Err(Error::Io(e));
         }
      }

      let shm_path = path.with_extension("db-shm");
      if let Err(e) = std::fs::remove_file(&shm_path) {
         if e.kind() != std::io::ErrorKind::NotFound {
            return Err(Error::Io(e));
         }
      }

      Ok(())
   }
}

#[cfg(test)]
mod tests {
   use super::*;

   #[tokio::test]
   async fn test_concurrent_reads() {
      let db = SqliteDatabase::connect("for_readonly_tests.db", None)
         .await
         .unwrap();

      let start = std::time::Instant::now();
      let mut handles = vec![];

      // Spawn 3 concurrent read tasks (proves read pool allows parallelism)
      for _ in 0..3 {
         let db = Arc::clone(&db);
         handles.push(tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM numbers")
               .fetch_one(db.read_pool().unwrap())
               .await
               .unwrap();

            assert_eq!(count, 12);
         }));
      }

      for handle in handles {
         handle.await.unwrap();
      }

      // Assert parallel execution: 3 tasks × 10ms delay
      // Sequential would take 30ms+, parallel should take ~10-15ms
      assert!(
         start.elapsed().as_millis() < 15,
         "Parallel reads took {}ms (expected <15ms, would be 30ms+ if sequential)",
         start.elapsed().as_millis()
      );
   }

   #[tokio::test]
   async fn test_database_closed_error() {
      use std::fs;

      // Create a test database (file will be created if it doesn't exist)
      let test_path = std::env::current_dir().unwrap().join("test_close_error.db");
      let db = SqliteDatabase::connect(&test_path, None)
         .await
         .expect("Failed to connect to test database");

      // Clone db so we can use it after close
      let db_ref = Arc::clone(&db);
      db.close().await.unwrap();

      // Try to use read_pool after close - should error
      let read_result = db_ref.read_pool();
      assert!(read_result.is_err());
      assert!(matches!(read_result.unwrap_err(), Error::DatabaseClosed));

      // Try to acquire writer after close - should error
      let writer_result = db_ref.acquire_writer().await;
      assert!(writer_result.is_err());
      assert!(matches!(writer_result.unwrap_err(), Error::DatabaseClosed));

      let _ = fs::remove_file(&test_path);
      let _ = fs::remove_file(test_path.with_extension("db-wal"));
      let _ = fs::remove_file(test_path.with_extension("db-shm"));
   }

   #[tokio::test]
   async fn test_memory_databases_never_cached() {
      // :memory: databases should never be cached - each connection is independent
      let db1 = SqliteDatabase::connect(":memory:", None).await.unwrap();
      let db2 = SqliteDatabase::connect(":memory:", None).await.unwrap();

      // Should be different Arc instances (not cached)
      assert!(
         !Arc::ptr_eq(&db1, &db2),
         ":memory: databases should not be cached, each connect should create new instance"
      );

      // Create table in first database
      let mut writer1 = db1.acquire_writer().await.unwrap();
      sqlx::query("CREATE TABLE test (id INTEGER)")
         .execute(&mut *writer1)
         .await
         .unwrap();

      drop(writer1);

      // Second database should NOT have the table (independent instances)
      let result = sqlx::query("SELECT * FROM test")
         .fetch_optional(db2.read_pool().unwrap())
         .await;

      assert!(
         result.is_err(),
         "Second :memory: database should not have table from first"
      );

      drop(db1);
      drop(db2);
   }

   #[tokio::test]
   async fn test_wal_checkpoint_on_close() {
      use std::fs;

      let test_path = std::env::current_dir()
         .unwrap()
         .join("test_wal_checkpoint.db");

      let db = SqliteDatabase::connect(&test_path, None).await.unwrap();

      // Perform write to initialize WAL mode
      let mut writer = db.acquire_writer().await.unwrap();
      sqlx::query("CREATE TABLE test (id INTEGER, value TEXT)")
         .execute(&mut *writer)
         .await
         .unwrap();

      sqlx::query("INSERT INTO test (id, value) VALUES (1, 'test')")
         .execute(&mut *writer)
         .await
         .unwrap();

      drop(writer);

      // WAL file should exist with data
      let wal_path = test_path.with_extension("db-wal");
      assert!(wal_path.exists(), "WAL file should exist after write");

      // Close database (should checkpoint WAL)
      db.close().await.unwrap();

      // WAL file should be either 0 bytes or not exist
      if wal_path.exists() {
         let wal_size = fs::metadata(&wal_path).unwrap().len();
         assert_eq!(wal_size, 0, "WAL file should be 0 bytes after checkpoint");
      }

      let _ = fs::remove_file(&test_path);
      let _ = fs::remove_file(wal_path);
      let _ = fs::remove_file(test_path.with_extension("db-shm"));
   }

   #[tokio::test]
   async fn test_remove() {
      let test_path = std::env::current_dir()
         .unwrap()
         .join("test_close_remove.db");

      let db = SqliteDatabase::connect(&test_path, None).await.unwrap();

      // Perform write to create WAL and SHM files
      let mut writer = db.acquire_writer().await.unwrap();
      sqlx::query("CREATE TABLE test (id INTEGER)")
         .execute(&mut *writer)
         .await
         .unwrap();

      drop(writer);

      assert!(test_path.exists(), "Database file should exist");

      let wal_path = test_path.with_extension("db-wal");
      let shm_path = test_path.with_extension("db-shm");

      db.remove().await.unwrap();

      // All files should be removed
      assert!(!test_path.exists(), "Database file should be removed");
      assert!(!wal_path.exists(), "WAL file should be removed");
      assert!(!shm_path.exists(), "SHM file should be removed");
   }

   #[tokio::test]
   async fn test_custom_config() {
      use std::time::Duration;

      let test_path = std::env::current_dir()
         .unwrap()
         .join("test_custom_config.db");

      let custom_config = SqliteDatabaseConfig {
         max_read_connections: 10,
         idle_timeout: Duration::from_secs(60),
      };

      // Verify custom config is accepted and connection works
      let db = SqliteDatabase::connect(&test_path, Some(custom_config))
         .await
         .unwrap();

      db.remove().await.unwrap();
   }

   #[tokio::test]
   async fn test_wal_mode_initialization() {
      let test_path = std::env::current_dir().unwrap().join("test_wal_mode.db");
      let db = SqliteDatabase::connect(&test_path, None).await.unwrap();

      // Before first write, acquire writer which should initialize WAL
      let mut writer = db.acquire_writer().await.unwrap();

      // Check journal mode
      let (mode,): (String,) = sqlx::query_as("PRAGMA journal_mode")
         .fetch_one(&mut *writer)
         .await
         .unwrap();

      assert_eq!(
         mode.to_lowercase(),
         "wal",
         "Journal mode should be WAL after first acquire_writer"
      );

      // Check sync setting
      let (sync,): (i32,) = sqlx::query_as("PRAGMA synchronous")
         .fetch_one(&mut *writer)
         .await
         .unwrap();

      assert_eq!(
         sync, 1,
         "Sync mode should be NORMAL after first acquire_writer"
      );

      drop(writer);

      db.remove().await.unwrap();
   }

   #[tokio::test]
   async fn test_db_instance_caching() {
      let test_path = std::env::current_dir().unwrap().join("test_caching.db");

      // Connect twice to same path
      let db1 = SqliteDatabase::connect(&test_path, None).await.unwrap();
      let db2 = SqliteDatabase::connect(&test_path, None).await.unwrap();

      // Should be same Arc instance (cached)
      assert!(
         Arc::ptr_eq(&db1, &db2),
         "Same path should return cached instance"
      );

      drop(db1);
      db2.remove().await.unwrap();
   }

   #[tokio::test]
   async fn test_write_serialization() {
      use std::time::{Duration, Instant};

      let test_path = std::env::current_dir()
         .unwrap()
         .join("test_write_serial.db");

      let db = SqliteDatabase::connect(&test_path, None).await.unwrap();

      let mut writer = db.acquire_writer().await.unwrap();
      sqlx::query("CREATE TABLE counter (id INTEGER PRIMARY KEY, value INTEGER)")
         .execute(&mut *writer)
         .await
         .unwrap();

      sqlx::query("INSERT INTO counter (id, value) VALUES (1, 0)")
         .execute(&mut *writer)
         .await
         .unwrap();

      drop(writer);

      // Spawn 3 concurrent write tasks (proves single-connection write pool serializes)
      let start = Instant::now();
      let mut handles = vec![];

      for _ in 0..3 {
         let db_clone = Arc::clone(&db);
         handles.push(tokio::spawn(async move {
            let mut writer = db_clone.acquire_writer().await.unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
            sqlx::query("UPDATE counter SET value = value + 1 WHERE id = 1")
               .execute(&mut *writer)
               .await
               .unwrap();
         }));
      }

      for handle in handles {
         handle.await.unwrap();
      }

      let (value,): (i64,) = sqlx::query_as("SELECT value FROM counter WHERE id = 1")
         .fetch_one(db.read_pool().unwrap())
         .await
         .unwrap();

      assert_eq!(value, 3, "All 3 writes should have been serialized");

      // Should take at least 30ms (3 tasks × 10ms) proving writes are serialized
      assert!(
         start.elapsed().as_millis() >= 25,
         "Serialized writes took {}ms (expected ≥25ms, would be ~10ms if concurrent)",
         start.elapsed().as_millis()
      );

      db.remove().await.unwrap();
   }

   #[tokio::test]
   async fn test_concurrent_reads_and_writes() {
      use std::fs;

      let test_path = std::env::current_dir().unwrap().join("test_read_write.db");
      let _ = fs::remove_file(&test_path);
      let _ = fs::remove_file(test_path.with_extension("db-wal"));
      let _ = fs::remove_file(test_path.with_extension("db-shm"));

      let db = SqliteDatabase::connect(&test_path, None).await.unwrap();

      let mut writer = db.acquire_writer().await.unwrap();
      sqlx::query("CREATE TABLE data (id INTEGER PRIMARY KEY, value INTEGER)")
         .execute(&mut *writer)
         .await
         .unwrap();

      drop(writer);

      let mut handles = vec![];

      // 2 concurrent readers (proves WAL allows reads during writes)
      for _ in 0..2 {
         let db_clone = Arc::clone(&db);
         handles.push(tokio::spawn(async move {
            let rows: Vec<(i64,)> = sqlx::query_as("SELECT COUNT(*) FROM data")
               .fetch_all(db_clone.read_pool().unwrap())
               .await
               .unwrap();

            assert!(rows.len() > 0);
         }));
      }

      // 2 concurrent writers
      for i in 1..=2 {
         let db_clone = Arc::clone(&db);
         handles.push(tokio::spawn(async move {
            let mut writer = db_clone.acquire_writer().await.unwrap();
            sqlx::query("INSERT INTO data (id, value) VALUES (?, ?)")
               .bind(i)
               .bind(i * 10)
               .execute(&mut *writer)
               .await
               .unwrap();
         }));
      }

      for handle in handles {
         handle.await.unwrap();
      }

      // Verify both writes completed
      let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM data")
         .fetch_one(db.read_pool().unwrap())
         .await
         .unwrap();

      assert_eq!(count.0, 2);

      db.remove().await.unwrap();
   }
}
