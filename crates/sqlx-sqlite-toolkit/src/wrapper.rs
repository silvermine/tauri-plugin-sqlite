use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx::sqlite::SqliteConnection;
use sqlx_sqlite_conn_mgr::{SqliteDatabase, SqliteDatabaseConfig, WriteGuard};
#[cfg(feature = "observer")]
use tracing::warn;

#[cfg(feature = "observer")]
use sqlx_sqlite_observer::{
   ObservableSqliteDatabase, ObservableWriteGuard, ObservationBroker, ObserverConfig,
};

use crate::Error;

/// Result returned from write operations (e.g. INSERT, UPDATE, DELETE).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteQueryResult {
   /// The number of rows affected by the write operation.
   pub rows_affected: u64,
   /// The last inserted row ID (SQLite ROWID).
   ///
   /// Only set for INSERT operations on tables with a ROWID.
   /// Tables created with `WITHOUT ROWID` will not set this value (returns 0).
   pub last_insert_id: i64,
}

/// Unified writer guard that routes through observer when enabled.
///
/// Derefs to `SqliteConnection` so it can be used with `sqlx::query().execute()`.
pub enum WriterGuard {
   /// Regular writer from the connection manager.
   Regular(WriteGuard),
   /// Observable writer that tracks changes via SQLite hooks.
   #[cfg(feature = "observer")]
   Observable(ObservableWriteGuard),
}

impl Deref for WriterGuard {
   type Target = SqliteConnection;

   fn deref(&self) -> &Self::Target {
      match self {
         WriterGuard::Regular(w) => w,
         #[cfg(feature = "observer")]
         WriterGuard::Observable(w) => w,
      }
   }
}

impl DerefMut for WriterGuard {
   fn deref_mut(&mut self) -> &mut Self::Target {
      match self {
         WriterGuard::Regular(w) => &mut *w,
         #[cfg(feature = "observer")]
         WriterGuard::Observable(w) => &mut *w,
      }
   }
}

/// Unified attached-writer guard that routes through the observer when enabled.
///
/// Mirrors [`WriterGuard`]'s Regular/Observable split, but for the
/// attached-database acquisition path (see
/// [`DatabaseWrapper::acquire_writer_with_attached`]).
///
/// Derefs to `SqliteConnection` so it can be used with `sqlx::query().execute()`.
///
/// **Important**: call [`detach_all`](Self::detach_all) before dropping. Neither
/// inner guard detaches in `Drop`, so a dropped guard leaves the alias on the
/// pooled write connection until that connection is eventually closed - and the
/// write pool holds a single connection, so every later attach of that alias
/// fails until then. `#[must_use]` catches the discarded-guard case but cannot
/// carry this hazard in its message, which is why it's stated here.
#[must_use = "if unused, the write guard and locks are immediately dropped"]
pub enum AttachedWriterGuard {
   /// Plain attached writer from the connection manager - unobserved.
   Regular(sqlx_sqlite_conn_mgr::AttachedWriteGuard),
   /// Attached writer that tracks changes via SQLite hooks, routed per-schema
   /// to whichever database owns each affected table.
   #[cfg(feature = "observer")]
   Observable(ObservableWriteGuard),
}

impl Deref for AttachedWriterGuard {
   type Target = SqliteConnection;

   fn deref(&self) -> &Self::Target {
      match self {
         AttachedWriterGuard::Regular(w) => w,
         #[cfg(feature = "observer")]
         AttachedWriterGuard::Observable(w) => w,
      }
   }
}

impl DerefMut for AttachedWriterGuard {
   fn deref_mut(&mut self) -> &mut Self::Target {
      match self {
         AttachedWriterGuard::Regular(w) => &mut *w,
         #[cfg(feature = "observer")]
         AttachedWriterGuard::Observable(w) => &mut *w,
      }
   }
}

impl AttachedWriterGuard {
   /// Detach all attached databases from this writer.
   ///
   /// See `sqlx_sqlite_conn_mgr::AttachedWriteGuard::detach_all` /
   /// `ObservableWriteGuard::detach_all` for what this does on each side;
   /// both are safe to call only after an explicit commit or rollback has
   /// already run.
   pub async fn detach_all(self) -> Result<(), Error> {
      match self {
         AttachedWriterGuard::Regular(w) => Ok(w.detach_all().await?),
         #[cfg(feature = "observer")]
         AttachedWriterGuard::Observable(w) => Ok(w.detach_all().await?),
      }
   }
}

/// Wrapper around SqliteDatabase that provides a high-level API for database operations.
///
/// This struct is the main entry point for interacting with SQLite databases through
/// the toolkit. It wraps the connection manager's `SqliteDatabase` and provides
/// builder-pattern APIs for queries, transactions, and write operations.
///
/// When the `observer` feature is enabled, the wrapper can also manage an
/// `ObservableSqliteDatabase` for change notification support. Observation state
/// itself lives on the shared `SqliteDatabase` (see [`enable_observation`'s
/// doc](Self::enable_observation)), not on this struct, so `#[derive(Clone)]`
/// cloning only `inner` is exactly what makes every clone of a `DatabaseWrapper`,
/// and every independent `connect()` call to the same path, observe through the
/// same broker.
#[derive(Clone)]
pub struct DatabaseWrapper {
   inner: Arc<SqliteDatabase>,
}

impl DatabaseWrapper {
   /// Get the inner Arc<SqliteDatabase> for advanced usage
   ///
   /// This is useful when you need to create `AttachedSpec` instances for cross-database
   /// operations with interruptible transactions.
   pub fn inner(&self) -> &Arc<SqliteDatabase> {
      &self.inner
   }

   #[doc(hidden)]
   pub fn inner_for_testing(&self) -> &Arc<SqliteDatabase> {
      &self.inner
   }

   /// Acquire a writer guard.
   ///
   /// When observation is enabled, returns an observable writer that tracks
   /// changes via SQLite hooks. Otherwise, returns a regular writer.
   ///
   /// **Known limitation:** the broker read from the slot here is snapshotted
   /// into the returned guard for its whole lifetime, so a
   /// [`disable_observation`] + [`enable_observation`] cycle during an open
   /// transaction leaves it publishing to the previous broker - silently, as
   /// far as any status check is concerned. See
   /// `sqlx_sqlite_observer::ObservableSqliteDatabase::acquire_writer`'s doc
   /// for the mechanics and [`disable_observation`] for the reachable trigger.
   ///
   /// [`disable_observation`]: Self::disable_observation
   /// [`enable_observation`]: Self::enable_observation
   pub async fn acquire_writer(&self) -> Result<WriterGuard, Error> {
      #[cfg(feature = "observer")]
      {
         // Read the broker out of the slot and drop the guard immediately -
         // `get()` already does this internally - before doing anything async,
         // so no lock guard is ever held across an `.await` here. The slot
         // holds `Arc<ObservationBroker>`, not `Arc<ObservableSqliteDatabase>`
         // (see `ObservableSqliteDatabase::from_broker`'s doc), so the handle
         // is rebuilt here rather than read out directly.
         let broker = self.inner.observer_slot().get::<ObservationBroker>();
         if let Some(broker) = broker {
            let observable = ObservableSqliteDatabase::from_broker(Arc::clone(&self.inner), broker);
            let writer = observable.acquire_writer().await.map_err(Error::Observer)?;
            return Ok(WriterGuard::Observable(writer));
         }
      }

      Ok(WriterGuard::Regular(self.inner.acquire_writer().await?))
   }

   /// Acquire a regular (non-observable) writer connection.
   ///
   /// This always bypasses the observer, even when observation is enabled.
   /// Useful when you need a writer for operations that should not trigger
   /// change notifications (e.g., internal bookkeeping).
   ///
   /// **This is an intentional, documented bypass, not a hole to close.**
   /// Callers who reach for this method are opting out of observation for this
   /// one writer; every other writer obtained via [`acquire_writer`] on this
   /// same (now database-wide) observation state still gets tracked normally.
   /// When observation is enabled, calling this logs a `tracing::warn!` as a
   /// development-time aid - it compiles out entirely in release builds, since
   /// this workspace pins `tracing` with `release_max_level_off`, so don't rely
   /// on it surfacing in a shipped app. Reads never need an equivalent bypass or
   /// warning: [`fetch_all`](Self::fetch_all), [`fetch_one`](Self::fetch_one),
   /// and [`fetch_page`](Self::fetch_page) all go through the read pool, which
   /// is opened `read_only(true)` and therefore can never write, observed or not.
   ///
   /// [`acquire_writer`]: Self::acquire_writer
   pub async fn acquire_regular_writer(&self) -> Result<WriteGuard, Error> {
      // Checked via the slot directly rather than `is_observing()`, which goes
      // through `observable()` and builds a whole handle only to drop it - the
      // `warn!` compiles out in release but the condition does not.
      #[cfg(feature = "observer")]
      if self
         .inner
         .observer_slot()
         .get::<ObservationBroker>()
         .is_some()
      {
         warn!(
            "acquire_regular_writer() called while observation is enabled on this \
             database; writes through this guard will not be tracked or published \
             to subscribers. This is intentional if you meant to bypass \
             observation - otherwise use acquire_writer() instead."
         );
      }

      Ok(self.inner.acquire_writer().await?)
   }

   /// Acquire a writer guard with one or more databases attached.
   ///
   /// When observation is enabled, routes through the observer so that writes
   /// into attached databases are tracked too - each change publishes to the
   /// broker of whichever database *owns* the affected table, not necessarily
   /// this one. See
   /// `sqlx_sqlite_observer::acquire_writer_with_attached_brokers`
   /// for the exact routing rule. When observation is not enabled, this falls
   /// back to the plain conn-mgr attached-writer acquisition, identical to
   /// calling `sqlx_sqlite_conn_mgr::acquire_writer_with_attached` directly.
   ///
   /// **Observation is checked on both sides, not just this database.** The
   /// observable path is taken when this database is observed *or* any
   /// `ReadWrite` spec's database is, since an attachment observed on its own
   /// still needs hooks registered for its subscribers to hear writes made
   /// through this call. Only when neither side is observed does this fall back
   /// to the plain conn-mgr call, which keeps an entirely unobserved caller
   /// from paying `lock_handle()` and FFI hook registration for nothing, and
   /// from newly requiring `SQLITE_ENABLE_PREUPDATE_HOOK` on a build that never
   /// asked for observation. [`acquire_writer_with_attached_brokers`] takes a
   /// free function's `Option` broker rather than being a method because this
   /// call has no `Self` to invoke when its own observation is off.
   ///
   /// This gate and that function's own map build are not atomic: it re-reads
   /// every slot. If observation is disabled on all sides in between, the
   /// observable path is taken with an empty map, which it handles by skipping
   /// hook registration - so the race is inert rather than contradicting the
   /// rationale above.
   ///
   /// [`acquire_writer_with_attached_brokers`]: sqlx_sqlite_observer::acquire_writer_with_attached_brokers
   pub async fn acquire_writer_with_attached(
      &self,
      specs: Vec<sqlx_sqlite_conn_mgr::AttachedSpec>,
   ) -> Result<AttachedWriterGuard, Error> {
      #[cfg(feature = "observer")]
      {
         // Same immediate clone-and-drop, and same slot-holds-a-broker
         // rebuild, as acquire_writer() - see its comments.
         let main_broker = self.inner.observer_slot().get::<ObservationBroker>();

         // Whether any ReadWrite spec's own database is observed, independent of
         // this database's `main_broker` above - see this method's doc for why
         // either side alone is enough to take the observable path.
         let any_readwrite_attachment_observed = specs.iter().any(|spec| {
            spec.mode == sqlx_sqlite_conn_mgr::AttachedMode::ReadWrite
               && spec
                  .database
                  .observer_slot()
                  .get::<ObservationBroker>()
                  .is_some()
         });

         if main_broker.is_some() || any_readwrite_attachment_observed {
            let guard = sqlx_sqlite_observer::acquire_writer_with_attached_brokers(
               &self.inner,
               main_broker,
               specs,
            )
            .await?;
            return Ok(AttachedWriterGuard::Observable(guard));
         }
      }

      Ok(AttachedWriterGuard::Regular(
         sqlx_sqlite_conn_mgr::acquire_writer_with_attached(&self.inner, specs).await?,
      ))
   }

   /// Begin an interruptible transaction that can be paused and resumed.
   ///
   /// Returns a builder that allows attaching databases before executing the transaction.
   /// Unlike `execute_transaction()`, this allows reading uncommitted data mid-transaction.
   ///
   /// # Examples
   ///
   /// ```no_run
   /// # async fn example(db: &sqlx_sqlite_toolkit::DatabaseWrapper) -> Result<(), sqlx_sqlite_toolkit::Error> {
   /// use serde_json::json;
   ///
   /// let mut tx = db.begin_interruptible_transaction()
   ///     .execute(vec![
   ///         ("INSERT INTO users (name) VALUES (?)", vec![json!("Alice")]),
   ///     ]).await?;
   ///
   /// // Read uncommitted data within the transaction
   /// let rows = tx.read("SELECT count(*) as n FROM users".into(), vec![]).await?;
   ///
   /// tx.commit().await?;
   /// # Ok(())
   /// # }
   /// ```
   pub fn begin_interruptible_transaction(&self) -> InterruptibleTransactionBuilder {
      InterruptibleTransactionBuilder::new(self.clone())
   }

   /// Connect to a SQLite database with an absolute path.
   ///
   /// This is the core connection method. It connects to the database at the given
   /// absolute path with optional configuration.
   ///
   /// Note: `SqliteDatabase::connect()` caches instances in a global registry.
   /// Multiple calls with the same path return the same underlying database,
   /// so this wrapper is lightweight - the actual connection pools are shared.
   ///
   /// # Examples
   ///
   /// ```no_run
   /// # async fn example() -> Result<(), sqlx_sqlite_toolkit::Error> {
   /// use sqlx_sqlite_toolkit::DatabaseWrapper;
   /// use std::path::Path;
   ///
   /// let db = DatabaseWrapper::connect(Path::new("/tmp/my.db"), None).await?;
   /// # Ok(())
   /// # }
   /// ```
   pub async fn connect(
      abs_path: &std::path::Path,
      custom_config: Option<SqliteDatabaseConfig>,
   ) -> Result<Self, Error> {
      let db = SqliteDatabase::connect(abs_path, custom_config).await?;

      Ok(Self { inner: db })
   }

   /// Create a builder for write queries (INSERT/UPDATE/DELETE).
   ///
   /// Returns a builder that can optionally attach databases before executing.
   ///
   /// # Examples
   ///
   /// ```no_run
   /// # async fn example(db: &sqlx_sqlite_toolkit::DatabaseWrapper) -> Result<(), sqlx_sqlite_toolkit::Error> {
   /// use serde_json::json;
   ///
   /// let result = db.execute(
   ///     "INSERT INTO users (name, age) VALUES (?, ?)".into(),
   ///     vec![json!("Alice"), json!(30)],
   /// ).execute().await?;
   ///
   /// println!("Inserted row {}", result.last_insert_id);
   /// # Ok(())
   /// # }
   /// ```
   pub fn execute(&self, query: String, values: Vec<JsonValue>) -> crate::builders::ExecuteBuilder {
      crate::builders::ExecuteBuilder::new(self.clone(), query, values)
   }

   /// Execute multiple statements atomically within a transaction.
   ///
   /// Returns a builder that allows attaching databases before executing the transaction.
   /// All statements either succeed together or fail together.
   ///
   /// Use this when you have a batch of writes and don't need to read data mid-transaction.
   /// For transactions requiring reads of uncommitted data, use `begin_interruptible_transaction()`.
   ///
   /// # Examples
   ///
   /// ```no_run
   /// # async fn example(db: &sqlx_sqlite_toolkit::DatabaseWrapper) -> Result<(), sqlx_sqlite_toolkit::Error> {
   /// use serde_json::json;
   ///
   /// let results = db.execute_transaction(vec![
   ///     ("INSERT INTO users (name) VALUES (?)", vec![json!("Alice")]),
   ///     ("INSERT INTO users (name) VALUES (?)", vec![json!("Bob")]),
   /// ]).execute().await?;
   ///
   /// println!("Inserted {} rows total", results.len());
   /// # Ok(())
   /// # }
   /// ```
   pub fn execute_transaction(
      &self,
      statements: Vec<(&str, Vec<JsonValue>)>,
   ) -> TransactionExecutionBuilder {
      TransactionExecutionBuilder::new(self.clone(), statements)
   }

   /// Create a builder for SELECT queries returning multiple rows.
   ///
   /// Returns a builder that can optionally attach databases before executing.
   ///
   /// # Examples
   ///
   /// ```no_run
   /// # async fn example(db: &sqlx_sqlite_toolkit::DatabaseWrapper) -> Result<(), sqlx_sqlite_toolkit::Error> {
   /// let rows = db.fetch_all(
   ///     "SELECT name, age FROM users WHERE age > ?".into(),
   ///     vec![serde_json::json!(21)],
   /// ).execute().await?;
   ///
   /// for row in &rows {
   ///     println!("{}: {}", row["name"], row["age"]);
   /// }
   /// # Ok(())
   /// # }
   /// ```
   pub fn fetch_all(
      &self,
      query: String,
      values: Vec<JsonValue>,
   ) -> crate::builders::FetchAllBuilder {
      crate::builders::FetchAllBuilder::new(Arc::clone(&self.inner), query, values)
   }

   /// Create a builder for paginated SELECT queries using keyset (cursor-based) pagination.
   ///
   /// Returns a builder that supports `.after(cursor)` for forward pagination,
   /// `.before(cursor)` for backward pagination, and `.attach(specs)` for
   /// cross-database queries.
   ///
   /// The base query must not contain ORDER BY or LIMIT clauses — the builder
   /// appends these automatically based on the keyset definition.
   ///
   /// # Examples
   ///
   /// ```no_run
   /// # async fn example(db: &sqlx_sqlite_toolkit::DatabaseWrapper) -> Result<(), sqlx_sqlite_toolkit::Error> {
   /// use sqlx_sqlite_toolkit::pagination::KeysetColumn;
   ///
   /// let keyset = vec![
   ///    KeysetColumn::asc("category"),
   ///    KeysetColumn::desc("score"),
   ///    KeysetColumn::asc("id"),
   /// ];
   ///
   /// // First page
   /// let page = db.fetch_page(
   ///    "SELECT * FROM posts".into(),
   ///    vec![],
   ///    keyset.clone(),
   ///    25,
   /// ).await?;
   ///
   /// // Next page (forward)
   /// if let Some(cursor) = page.next_cursor {
   ///    let next = db.fetch_page(
   ///       "SELECT * FROM posts".into(),
   ///       vec![],
   ///       keyset.clone(),
   ///       25,
   ///    ).after(cursor).await?;
   ///
   ///    // Previous page (backward)
   ///    if let Some(prev_cursor) = next.next_cursor {
   ///       let prev = db.fetch_page(
   ///          "SELECT * FROM posts".into(),
   ///          vec![],
   ///          keyset,
   ///          25,
   ///       ).before(prev_cursor).await?;
   ///    }
   /// }
   /// # Ok(())
   /// # }
   /// ```
   pub fn fetch_page(
      &self,
      query: String,
      values: Vec<JsonValue>,
      keyset: Vec<crate::pagination::KeysetColumn>,
      page_size: usize,
   ) -> crate::builders::FetchPageBuilder {
      crate::builders::FetchPageBuilder::new(
         Arc::clone(&self.inner),
         query,
         values,
         keyset,
         page_size,
      )
   }

   /// Create a builder for SELECT queries returning zero or one row.
   ///
   /// Returns a builder that can optionally attach databases before executing.
   /// Returns an error if the query returns more than one row.
   ///
   /// # Examples
   ///
   /// ```no_run
   /// # async fn example(db: &sqlx_sqlite_toolkit::DatabaseWrapper) -> Result<(), sqlx_sqlite_toolkit::Error> {
   /// let user = db.fetch_one(
   ///     "SELECT name FROM users WHERE id = ?".into(),
   ///     vec![serde_json::json!(1)],
   /// ).execute().await?;
   ///
   /// match user {
   ///     Some(row) => println!("Found: {}", row["name"]),
   ///     None => println!("Not found"),
   /// }
   /// # Ok(())
   /// # }
   /// ```
   pub fn fetch_one(
      &self,
      query: String,
      values: Vec<JsonValue>,
   ) -> crate::builders::FetchOneBuilder {
      crate::builders::FetchOneBuilder::new(Arc::clone(&self.inner), query, values)
   }

   /// Run database migrations
   ///
   /// Runs all pending migrations from the provided migrator.
   /// SQLx tracks applied migrations, so this is safe to call multiple times.
   ///
   /// Migrations run through `self.inner` directly, never through the observer,
   /// even when observation is enabled - schema changes are not row changes and
   /// have no `TableChange` representation, so there's nothing for a subscriber
   /// to receive here regardless.
   pub async fn run_migrations(
      &self,
      migrator: &sqlx_sqlite_conn_mgr::Migrator,
   ) -> Result<(), Error> {
      self.inner.run_migrations(migrator).await?;
      Ok(())
   }

   /// Close the database connection.
   ///
   /// Checkpoints the WAL and closes all connection pools.
   /// If observation is enabled, it is disabled first to unregister SQLite hooks
   /// and allow the write connection to close cleanly - which, per
   /// [`disable_observation`](Self::disable_observation), affects every handle
   /// to this database.
   pub async fn close(self) -> Result<(), Error> {
      #[cfg(feature = "observer")]
      self.disable_observation();

      self.inner.close().await?;
      Ok(())
   }

   /// Close the database connection and remove all database files.
   ///
   /// Removes the main database file, WAL, and SHM files.
   /// If observation is enabled, it is disabled first to unregister SQLite hooks
   /// and allow the write connection to close cleanly. Same database-wide caveat
   /// as [`close`](Self::close).
   pub async fn remove(self) -> Result<(), Error> {
      #[cfg(feature = "observer")]
      self.disable_observation();

      self.inner.remove().await?;
      Ok(())
   }

   /// Enable observation on this database for the specified tables.
   ///
   /// After calling this, write operations will be tracked and subscribers
   /// can receive change notifications.
   ///
   /// **Database-wide, not per-handle (issue #53):** observation state lives on
   /// the shared `SqliteDatabase` behind `self.inner`, not on this
   /// `DatabaseWrapper` value. Every clone of this wrapper and every independent
   /// `DatabaseWrapper::connect()` call that resolves to the same underlying file
   /// observes through the same broker - there is no such thing as "my own"
   /// observation separate from anyone else's handle to this database. `:memory:`
   /// databases are the one exception: each `connect()` call gets its own
   /// `SqliteDatabase` (they're excluded from the path registry), so they're
   /// independently observed by construction, not because of anything special
   /// here.
   ///
   /// **Additive, not destructive (issue #54):** if observation is already
   /// enabled - by this handle, a clone of it, or a completely independent
   /// connection to the same database - the existing broker is reused rather
   /// than replaced. The requested tables are unioned into its observed-table
   /// set, and any subscribers created before this call keep receiving
   /// notifications uninterrupted. This is what allows independent callers (e.g.
   /// multiple windows observing the same database) to call `enable_observation`
   /// without tearing down each other's subscriptions. The check for an existing
   /// broker, the creation of a new one, and - on the reuse path - the merge of
   /// the requested tables into the existing broker's observed set all happen
   /// under the database's observer slot's single write lock, so two callers
   /// racing to be first can't each build their own broker and have one
   /// silently overwrite (and orphan the subscribers of) the other, and a
   /// concurrent [`disable_observation`](Self::disable_observation) can't land
   /// in the middle of the merge and have this call's tables register against
   /// a broker the slot no longer points to. The lock is released before this
   /// returns, though, so a `disable_observation()` immediately afterward still
   /// tears down what this call just set up.
   ///
   /// `config.channel_capacity` and `config.capture_values` can only take effect on
   /// the *first* call that enables observation for this database. Both are baked
   /// into the broadcast channel/broker at creation time and cannot be changed
   /// without recreating the broker — which would drop existing subscribers, the
   /// exact problem this method now avoids. If a later call requests different
   /// values, they are ignored (a warning is logged) and only the tables are merged
   /// in — this method stays infallible on purpose. The Tauri plugin layer's
   /// `observe()` command rejects a conflicting request outright before it ever
   /// reaches here, so in practice this fallback only matters for direct Rust
   /// callers of this crate. Those callers should not rely on the warning to
   /// notice: all four crates in this workspace pin `tracing` with
   /// `release_max_level_off`, which compiles the `warn!` below out entirely
   /// whenever `debug_assertions` are disabled — not merely when the build
   /// profile happens to be named `release`. The only reliable way to learn the values actually in effect
   /// is to read them back afterward via `broker().channel_capacity()` /
   /// `.capture_values()`. Call [`disable_observation`](Self::disable_observation)
   /// first if you need to change these values, accepting that existing
   /// subscribers will be dropped - and, per the database-wide note above, dropped
   /// for every handle to this database, not just this one.
   ///
   /// Requires the `observer` feature.
   #[cfg(feature = "observer")]
   pub fn enable_observation(&self, config: ObserverConfig) {
      let requested_channel_capacity = config.channel_capacity;
      let requested_capture_values = config.capture_values;
      let requested_tables = config.tables.clone();
      let inner = Arc::clone(&self.inner);

      // `get_or_init_with` rather than `get_or_init` so the merge below runs under
      // the same write lock that decided "reuse, don't create" - see its doc.
      // Doing the merge after that lock released would leave a window for a
      // concurrent `disable_observation()` to clear the slot, orphaning this
      // call's `observe_tables()` on a broker nothing points to.
      let result = self.inner.observer_slot().get_or_init_with(
         || {
            // `ObservableSqliteDatabase::new` stays the single place an
            // `ObserverConfig` becomes a broker, but only the broker goes in the
            // slot - see `ObservableSqliteDatabase::from_broker`'s doc for why
            // storing the whole handle would form a reference cycle.
            let observable = ObservableSqliteDatabase::new(inner, config);
            Arc::clone(observable.broker())
         },
         |broker| {
            // Merge path: a broker already existed (this handle's own prior
            // call, a clone's, or an entirely independent connection's) and
            // get_or_init_with left it in place rather than replacing it.
            if requested_channel_capacity != broker.channel_capacity()
               || requested_capture_values != broker.capture_values()
            {
               warn!(
                  requested_channel_capacity = requested_channel_capacity,
                  active_channel_capacity = broker.channel_capacity(),
                  requested_capture_values = requested_capture_values,
                  active_capture_values = broker.capture_values(),
                  "enable_observation() called with different channel_capacity/capture_values \
                   while observation is already active; keeping the original values since \
                   recreating the broadcast channel would drop existing subscribers. Only the \
                   requested tables were merged in."
               );
            }

            if !requested_tables.is_empty() {
               broker.observe_tables(requested_tables.iter().map(String::as_str));
            }
         },
      );

      if result.is_none() {
         // The slot holds a value of some other type - a programming error
         // elsewhere in this process, since this method is the slot's only
         // writer. Nothing safe to do here but warn and leave observation
         // exactly as it was; see `ObserverSlot::get_or_init`'s doc for why
         // this can't happen from repeated calls to this method alone.
         warn!(
            "enable_observation: observer slot for this database holds a value \
             of an unexpected type; leaving observation state untouched"
         );
      }
   }

   /// Disable observation on this database.
   ///
   /// Clears the database's observer slot and stops tracking changes.
   /// Existing subscribers will stop receiving notifications.
   ///
   /// **Affects every handle to this database (issue #53).** Observation is
   /// database-wide (see [`enable_observation`](Self::enable_observation)), so
   /// this tears it down for clones and independent `connect()` callers alike,
   /// including ones this call has no way to know about. Nothing here counts
   /// how many callers still want observation; a caller that needs that must
   /// coordinate above this crate.
   ///
   /// **The coordination that exists above this crate does not cover you.** The
   /// `tauri-plugin-sqlite` layer reference-counts observation per *webview
   /// label* (issue #54), and a Rust caller holding a `DatabaseWrapper` registers
   /// nothing there. So the plugin's `unobserve()` (or a window being destroyed)
   /// can drive that count to zero and call this method on the database you are
   /// observing, ending your subscription without you having called anything.
   /// Symmetrically, calling this yourself leaves those registrations non-zero
   /// while the slot is empty: the plugin's `subscribe()` then fails with
   /// `OBSERVATION_NOT_ENABLED`, and the next `observe()` builds a fresh broker
   /// that other windows' existing subscriptions are not bound to. A Rust
   /// consumer that must not be torn down needs its own database file - the
   /// broker is keyed by canonical path, so registering the same file under a
   /// different plugin key still shares it.
   ///
   /// **Known limitation: this clears the slot, but never reaches a writer
   /// that already has a broker bound.** Calling this and then
   /// [`enable_observation`](Self::enable_observation) while such a writer's
   /// transaction is open leaves it publishing to the broker it bound at
   /// acquisition, so a subscriber created after the cycle misses that commit
   /// while pre-existing ones still receive it - with `is_observing()`, the new
   /// `subscribe()`, and the commit all reporting success. The reachable
   /// trigger: the last window's `unobserve()` runs, then a new caller's
   /// `observe()`, while another caller's interruptible transaction is still
   /// open. See
   /// `sqlx_sqlite_observer::ObservableSqliteDatabase::acquire_writer`'s doc
   /// for the mechanics and the deferred fix.
   ///
   /// Requires the `observer` feature.
   #[cfg(feature = "observer")]
   pub fn disable_observation(&self) {
      self.inner.observer_slot().clear();
   }

   /// Get an owned handle to the observable database, if observation is enabled.
   ///
   /// Returns `None` if observation has not been enabled via `enable_observation()`.
   ///
   /// Returns an owned `ObservableSqliteDatabase` rather than a reference, since a
   /// reference into the observer slot can't escape the slot's internal lock
   /// guard. The slot itself only holds the broker (see
   /// `ObservableSqliteDatabase::from_broker`'s doc for why), so the handle is
   /// rebuilt from `self.inner` plus that broker - two refcount bumps, not a deep
   /// copy - semantically identical to holding a reference for as long as you need
   /// one.
   ///
   /// Requires the `observer` feature.
   #[cfg(feature = "observer")]
   pub fn observable(&self) -> Option<ObservableSqliteDatabase> {
      self
         .inner
         .observer_slot()
         .get::<ObservationBroker>()
         .map(|broker| ObservableSqliteDatabase::from_broker(Arc::clone(&self.inner), broker))
   }

   /// Returns true if observation is currently enabled on this database.
   ///
   /// Deliberately defined in terms of [`observable`](Self::observable) rather
   /// than the slot's own `is_set()` (which only checks that *something* is
   /// there, not that it downcasts to the `ObservationBroker` this layer
   /// stores). On the slot type-mismatch case documented on
   /// `ObserverSlot::get`, `is_set()` would report `true` while `observable()`
   /// returns `None` - a predicate built on `is_set()` would then claim
   /// observation is on while every acquisition path silently took its
   /// unobserved branch. Defining it this way keeps "is observing" and
   /// "`observable()` returns `Some`" in agreement, which is what makes this
   /// usable as an external invariant: the `tauri-plugin-sqlite` layer's
   /// lock-order tests assert it against its own observer registrations. The
   /// acquisition paths in this file - [`acquire_writer`](Self::acquire_writer)
   /// and [`acquire_regular_writer`](Self::acquire_regular_writer) - read the
   /// slot directly instead, since they need the broker itself (or just a
   /// boolean) without building a handle only to drop it.
   ///
   /// Requires the `observer` feature.
   #[cfg(feature = "observer")]
   pub fn is_observing(&self) -> bool {
      self.observable().is_some()
   }
}

/// Builder for interruptible transactions with optional attached databases
pub struct InterruptibleTransactionBuilder {
   db: DatabaseWrapper,
   attached: Vec<sqlx_sqlite_conn_mgr::AttachedSpec>,
}

impl InterruptibleTransactionBuilder {
   fn new(db: DatabaseWrapper) -> Self {
      Self {
         db,
         attached: Vec::new(),
      }
   }

   /// Attach databases for cross-database operations
   pub fn attach(mut self, specs: Vec<sqlx_sqlite_conn_mgr::AttachedSpec>) -> Self {
      self.attached = specs;
      self
   }

   /// Execute the transaction with initial statements
   ///
   /// Returns an `InterruptibleTransaction` that can be continued, read from, committed, or rolled back.
   pub async fn execute(
      self,
      initial_statements: Vec<(&str, Vec<JsonValue>)>,
   ) -> Result<InterruptibleTransaction, Error> {
      use crate::transactions::{ActiveInterruptibleTransaction, TransactionWriter};

      // Acquire appropriate writer based on whether databases are attached
      let mut writer = if self.attached.is_empty() {
         let guard = self.db.acquire_writer().await?;
         TransactionWriter::from(guard)
      } else {
         let guard = self.db.acquire_writer_with_attached(self.attached).await?;
         TransactionWriter::from(guard)
      };

      // Begin transaction. A failure here (a busy database, say) is the one early
      // return not covered by `ActiveInterruptibleTransaction`'s Drop, since the
      // writer hasn't been handed over yet - so detach explicitly, or the alias
      // strands on the pooled write connection (see `builders::detach_after`).
      if let Err(err) = writer.begin_immediate().await {
         if let Err(detach_err) = writer.detach_if_attached().await {
            tracing::error!(
               "detach_all failed after BEGIN IMMEDIATE failed: {}",
               detach_err
            );
         }
         return Err(err);
      }

      // Create active transaction and execute initial statements
      let mut active_tx = ActiveInterruptibleTransaction::new(
         "direct_rust_api".to_string(),
         uuid::Uuid::new_v4().to_string(),
         writer,
      );

      active_tx.continue_with(initial_statements).await?;

      Ok(InterruptibleTransaction { inner: active_tx })
   }
}

/// An active interruptible transaction that can be continued, read from, committed, or rolled back
///
/// This transaction holds a write lock on the database and will automatically rollback
/// if dropped without an explicit commit.
#[must_use = "if unused, the transaction is immediately rolled back"]
pub struct InterruptibleTransaction {
   inner: crate::transactions::ActiveInterruptibleTransaction,
}

impl InterruptibleTransaction {
   /// Continue transaction with additional statements
   ///
   /// Returns write results for each statement executed.
   pub async fn continue_with(
      &mut self,
      statements: Vec<crate::transactions::Statement>,
   ) -> Result<Vec<WriteQueryResult>, Error> {
      self.inner.continue_with(statements).await
   }

   /// Execute a read query within this transaction
   ///
   /// This allows reading uncommitted changes made within the transaction.
   pub async fn read(
      &mut self,
      query: String,
      values: Vec<JsonValue>,
   ) -> Result<Vec<indexmap::IndexMap<String, JsonValue>>, Error> {
      self.inner.read(query, values).await
   }

   /// Commit this transaction
   ///
   /// Consumes the transaction, making all changes permanent.
   pub async fn commit(self) -> Result<(), Error> {
      self.inner.commit().await
   }

   /// Rollback this transaction
   ///
   /// Consumes the transaction, discarding all changes.
   pub async fn rollback(self) -> Result<(), Error> {
      self.inner.rollback().await
   }
}

/// Builder for regular atomic transactions
pub struct TransactionExecutionBuilder {
   db: DatabaseWrapper,
   statements: Vec<(String, Vec<JsonValue>)>,
   attached: Vec<sqlx_sqlite_conn_mgr::AttachedSpec>,
}

impl TransactionExecutionBuilder {
   fn new(db: DatabaseWrapper, statements: Vec<(&str, Vec<JsonValue>)>) -> Self {
      Self {
         db,
         statements: statements
            .into_iter()
            .map(|(query, values)| (query.to_string(), values))
            .collect(),
         attached: Vec::new(),
      }
   }

   /// Attach databases for cross-database operations
   pub fn attach(mut self, specs: Vec<sqlx_sqlite_conn_mgr::AttachedSpec>) -> Self {
      self.attached = specs;
      self
   }

   /// Execute the transaction atomically
   ///
   /// All statements execute within a single transaction. If any statement fails,
   /// all changes are rolled back automatically.
   pub async fn execute(self) -> Result<Vec<WriteQueryResult>, Error> {
      use crate::transactions::TransactionWriter;

      // Acquire appropriate writer based on whether databases are attached
      let mut writer = if self.attached.is_empty() {
         let guard = self.db.acquire_writer().await?;
         TransactionWriter::from(guard)
      } else {
         let guard = self.db.acquire_writer_with_attached(self.attached).await?;
         TransactionWriter::from(guard)
      };

      // Begin transaction. Same reasoning as the commit/rollback arms below: this
      // early return has to detach too, or the alias strands on the single write
      // connection.
      if let Err(err) = writer.begin_immediate().await {
         if let Err(detach_err) = writer.detach_if_attached().await {
            tracing::error!(
               "detach_all failed after BEGIN IMMEDIATE failed: {}",
               detach_err
            );
         }
         return Err(err);
      }

      // Execute all statements
      let exec_result = async {
         let mut results = Vec::new();
         for (query, values) in self.statements {
            let mut q = sqlx::query(sqlx::AssertSqlSafe(query));
            for value in values {
               q = bind_value(q, value);
            }
            let exec_result = writer.execute_query(q).await?;
            results.push(WriteQueryResult {
               rows_affected: exec_result.rows_affected(),
               last_insert_id: exec_result.last_insert_rowid(),
            });
         }
         Ok::<Vec<WriteQueryResult>, Error>(results)
      }
      .await;

      // Commit or rollback
      match exec_result {
         Ok(results) => {
            writer.commit().await?;
            writer.detach_if_attached().await?;
            Ok(results)
         }
         Err(e) => {
            writer.rollback().await?;
            if let Err(detach_err) = writer.detach_if_attached().await {
               tracing::error!("detach_all failed after rollback: {}", detach_err);
            }
            Err(e)
         }
      }
   }
}

impl std::future::IntoFuture for TransactionExecutionBuilder {
   type Output = Result<Vec<WriteQueryResult>, Error>;
   type IntoFuture = std::pin::Pin<Box<dyn std::future::Future<Output = Self::Output> + Send>>;

   fn into_future(self) -> Self::IntoFuture {
      Box::pin(self.execute())
   }
}

/// Helper function to bind a JSON value to a SQLx query
pub fn bind_value<'a>(
   query: sqlx::query::Query<'a, sqlx::Sqlite, sqlx::sqlite::SqliteArguments>,
   value: JsonValue,
) -> sqlx::query::Query<'a, sqlx::Sqlite, sqlx::sqlite::SqliteArguments> {
   if value.is_null() {
      query.bind(None::<JsonValue>)
   } else if value.is_string() {
      query.bind(value.as_str().unwrap().to_owned())
   } else if let Some(number) = value.as_number() {
      // Preserve integer precision by binding as i64 when possible
      if let Some(int_val) = number.as_i64() {
         query.bind(int_val)
      } else if let Some(uint_val) = number.as_u64() {
         // Try to fit u64 into i64 (SQLite's INTEGER type)
         if uint_val <= i64::MAX as u64 {
            query.bind(uint_val as i64)
         } else {
            // Value too large for i64, use f64 (will lose precision)
            query.bind(uint_val as f64)
         }
      } else {
         // Not an integer, bind as f64
         query.bind(number.as_f64().unwrap_or_default())
      }
   } else {
      query.bind(value)
   }
}
