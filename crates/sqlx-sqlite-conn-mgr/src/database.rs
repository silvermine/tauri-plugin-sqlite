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

/// Analysis limit for PRAGMA optimize on close.
/// SQLite recommends 100-1000 for older versions; 3.46.0+ handles automatically.
/// See: https://www.sqlite.org/lang_analyze.html#recommended_usage_pattern
const OPTIMIZE_ANALYSIS_LIMIT: u32 = 400;

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
   /// use std::sync::Arc;
   ///
   /// # async fn example() -> Result<(), sqlx_sqlite_conn_mgr::Error> {
   /// // Customize configuration when defaults don't meet your requirements
   /// let custom_config = SqliteDatabaseConfig {
   ///    max_read_connections: 10,
   ///    idle_timeout_secs: 60,
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
         let read_options = SqliteConnectOptions::new()
            .filename(&path)
            .read_only(true)
            .optimize_on_close(true, OPTIMIZE_ANALYSIS_LIMIT);

         let read_pool = SqlitePoolOptions::new()
            .max_connections(config.max_read_connections)
            .min_connections(0)
            .idle_timeout(Some(std::time::Duration::from_secs(
               config.idle_timeout_secs,
            )))
            .connect_with(read_options)
            .await?;

         // Create write pool with a single read-write connection
         let write_options = SqliteConnectOptions::new()
            .filename(&path)
            .read_only(false)
            .optimize_on_close(true, OPTIMIZE_ANALYSIS_LIMIT);

         let write_conn = SqlitePoolOptions::new()
            .max_connections(1)
            .min_connections(0)
            .idle_timeout(Some(std::time::Duration::from_secs(
               config.idle_timeout_secs,
            )))
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

      // Initialize WAL mode on first use (atomic check-and-set)
      if self
         .wal_initialized
         .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
         .is_ok()
      {
         sqlx::query("PRAGMA journal_mode = WAL")
            .execute(&mut *conn)
            .await?;

         // https://www.sqlite.org/wal.html#performance_considerations
         sqlx::query("PRAGMA synchronous = NORMAL")
            .execute(&mut *conn)
            .await?;
      }

      // Return WriteGuard wrapping the pool connection
      Ok(WriteGuard::new(conn))
   }

   /// Run database migrations using the provided migrator
   ///
   /// This method runs all pending migrations from the provided `Migrator`.
   /// Migrations are executed using the write connection to ensure exclusive access.
   /// WAL mode is enabled automatically before running migrations.
   ///
   /// SQLx tracks applied migrations in a `_sqlx_migrations` table, so calling
   /// this method multiple times is safe - already-applied migrations are skipped.
   ///
   /// # Arguments
   ///
   /// * `migrator` - A reference to a `Migrator` containing the migrations to run.
   ///   Typically created using `sqlx::migrate!()` macro.
   ///
   /// # Example
   ///
   /// ```ignore
   /// use sqlx_sqlite_conn_mgr::SqliteDatabase;
   ///
   /// // sqlx::migrate! is evaluated at compile time
   /// static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");
   ///
   /// let db = SqliteDatabase::connect("test.db", None).await?;
   /// db.run_migrations(&MIGRATOR).await?;
   /// ```
   pub async fn run_migrations(&self, migrator: &sqlx::migrate::Migrator) -> Result<()> {
      // Ensure WAL mode is initialized via acquire_writer
      // (WriteGuard dropped immediately, returning connection to pool)
      {
         let _writer = self.acquire_writer().await?;
      }

      // Migrator acquires its own connection from the write pool
      migrator.run(&self.write_conn).await?;

      Ok(())
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
      if self.wal_initialized.load(Ordering::SeqCst)
         && let Ok(mut conn) = self.write_conn.acquire().await
      {
         let _ = sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&mut *conn)
            .await;
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
      if let Err(e) = std::fs::remove_file(&wal_path)
         && e.kind() != std::io::ErrorKind::NotFound
      {
         return Err(Error::Io(e));
      }

      let shm_path = path.with_extension("db-shm");
      if let Err(e) = std::fs::remove_file(&shm_path)
         && e.kind() != std::io::ErrorKind::NotFound
      {
         return Err(Error::Io(e));
      }

      Ok(())
   }
}
