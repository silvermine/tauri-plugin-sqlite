//! Integration with sqlx-sqlite-conn-mgr crate.
//!
//! This module provides observation capabilities for databases managed by
//! `sqlx-sqlite-conn-mgr`. Enable with the `conn-mgr` feature.
//!
//! Uses SQLite's native hooks for transaction-safe change tracking. Changes
//! are buffered during transactions and only published after commit.
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use sqlx_sqlite_conn_mgr::SqliteDatabase;
//! use sqlx_sqlite_observer::{ObservableSqliteDatabase, ObserverConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!    let db = SqliteDatabase::connect("mydb.db", None).await?;
//!    let config = ObserverConfig::new().with_tables(["users", "posts"]);
//!    let observable = ObservableSqliteDatabase::new(db, config);
//!
//!    let mut rx = observable.subscribe(["users"]);
//!
//!    // Use observable writer for tracked changes
//!    let mut writer = observable.acquire_writer().await?;
//!    sqlx::query("BEGIN").execute(&mut *writer).await?;
//!    sqlx::query("INSERT INTO users (name) VALUES (?)")
//!       .bind("Alice")
//!       .execute(&mut *writer)
//!       .await?;
//!
//!    sqlx::query("COMMIT").execute(&mut *writer).await?;
//!    // Changes publish on commit!
//!
//!    // Read pool works as normal (no observation needed for reads)
//!    let rows = sqlx::query("SELECT * FROM users")
//!       .fetch_all(observable.read_pool()?)
//!       .await?;
//!
//!    Ok(())
//! }
//! ```

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use libsqlite3_sys::sqlite3;
use sqlx::sqlite::SqliteConnection;
use sqlx::{Pool, Sqlite};
use sqlx_sqlite_conn_mgr::{
   AttachedMode, AttachedSpec, AttachedWriteGuard, SqliteDatabase, WriteGuard,
};
use tokio::sync::broadcast;
use tracing::{debug, trace, warn};

use crate::Result;
use crate::broker::ObservationBroker;
use crate::change::TableChange;
use crate::config::ObserverConfig;
use crate::hooks;
use crate::schema::query_table_info;
use crate::stream::TableChangeStream;

/// Wrapper around `SqliteDatabase` that provides change observation.
///
/// This type integrates with `sqlx-sqlite-conn-mgr` to observe changes made
/// through the write connection while leaving read operations unaffected.
/// Uses SQLite's native hooks for transaction-safe notifications.
pub struct ObservableSqliteDatabase {
   db: Arc<SqliteDatabase>,
   broker: Arc<ObservationBroker>,
}

impl ObservableSqliteDatabase {
   /// Create a new observable database wrapper.
   ///
   /// # Arguments
   ///
   /// * `db` - The `SqliteDatabase` instance to observe
   /// * `config` - Observer configuration specifying which tables to track
   pub fn new(db: Arc<SqliteDatabase>, config: ObserverConfig) -> Self {
      let broker = ObservationBroker::new(config.channel_capacity, config.capture_values);

      if !config.tables.is_empty() {
         broker.observe_tables(config.tables.iter().map(String::as_str));
      }

      Self { db, broker }
   }

   /// Rebuilds an observable handle from a broker already stored in `db`'s
   /// [`ObserverSlot`](sqlx_sqlite_conn_mgr::ObserverSlot).
   ///
   /// The slot holds `Arc<ObservationBroker>`, not `Arc<Self>` - storing `Self`
   /// there would put `db`'s own `Arc<SqliteDatabase>` field back into the slot
   /// of the very database it came from, a strong reference cycle that keeps
   /// the database alive forever (it's what the registry's `Weak` reference is
   /// meant to prevent). Every internal read site that finds a broker already
   /// in the slot uses this constructor to hand callers back the same
   /// `ObservableSqliteDatabase` API without recreating that cycle. Unlike
   /// [`new`](Self::new), this never applies an [`ObserverConfig`] - the
   /// broker already carries whatever configuration it was created with.
   pub fn from_broker(db: Arc<SqliteDatabase>, broker: Arc<ObservationBroker>) -> Self {
      Self { db, broker }
   }

   /// Subscribe to change notifications.
   ///
   /// Returns a broadcast receiver that will receive `TableChange` events
   /// when observable tables are modified and transactions commit.
   pub fn subscribe<I, S>(&self, tables: I) -> broadcast::Receiver<TableChange>
   where
      I: IntoIterator<Item = S>,
      S: Into<String>,
   {
      let tables: Vec<String> = tables.into_iter().map(Into::into).collect();
      if !tables.is_empty() {
         self
            .broker
            .observe_tables(tables.iter().map(String::as_str));
      }
      self.broker.subscribe()
   }

   /// Subscribe and get a `Stream` for easier async iteration.
   pub fn subscribe_stream<I, S>(&self, tables: I) -> TableChangeStream
   where
      I: IntoIterator<Item = S>,
      S: Into<String>,
   {
      use crate::stream::TableChangeStreamExt;
      let tables: Vec<String> = tables.into_iter().map(Into::into).collect();
      // Register tables for observation (uses references, avoids clone)
      if !tables.is_empty() {
         self
            .broker
            .observe_tables(tables.iter().map(String::as_str));
      }
      let rx = self.broker.subscribe();
      let stream = rx.into_stream();
      if tables.is_empty() {
         stream
      } else {
         stream.filter_tables(tables)
      }
   }

   /// Get a reference to the read-only connection pool.
   ///
   /// Read operations don't need observation since they don't modify data.
   /// However, this pool is also used internally to query table schema
   /// information (primary key columns, WITHOUT ROWID status) when tables
   /// are first observed.
   pub fn read_pool(&self) -> sqlx_sqlite_conn_mgr::Result<&Pool<Sqlite>> {
      self.db.read_pool()
   }

   /// Acquire an observable write guard.
   ///
   /// The returned `ObservableWriteGuard` has observation hooks registered.
   /// Changes are published to subscribers when transactions commit.
   ///
   /// On first acquisition for each table, queries the schema to determine
   /// primary key columns and WITHOUT ROWID status.
   ///
   /// Nothing is attached on this path, so SQLite only ever reports the
   /// `"main"` schema for writes made through it - see
   /// [`acquire_writer_with_attached`](Self::acquire_writer_with_attached) for
   /// the multi-schema case.
   ///
   /// Warming happens before the writer is acquired, so the single write permit
   /// is never held while awaiting a read-pool connection.
   ///
   /// **Known limitation: the broker this guard's hooks bind to is fixed for
   /// the guard's whole lifetime.** It is `self.broker`, snapshotted when
   /// `Self` was built. If a `disable_observation()` + `enable_observation()`
   /// cycle runs on the same database while this guard's transaction is still
   /// open, the observer slot ends up holding a new broker while these hooks
   /// stay bound to the old one. The commit still reaches subscribers that
   /// existed before the cycle (the hook context's `Arc` keeps that broker
   /// alive), but a subscriber created after it subscribes against the new
   /// broker and never sees this commit. Nothing reports a failure:
   /// `is_observing()`, the new `subscribe()`, and the commit all succeed. The
   /// reachable trigger is an `unobserve()`/`observe()` pair running while
   /// another caller's interruptible transaction is open. Fixing it means
   /// keeping the previous broker reachable for as long as a writer is bound
   /// to it; deferred to a follow-up issue (not yet filed).
   pub async fn acquire_writer(&self) -> Result<ObservableWriteGuard> {
      // Warm before taking the write permit, never after. `ensure_table_info()`
      // awaits a *read*-pool connection, so warming under the permit lets a full
      // read pool and a pending writer wait on each other: tasks holding all six
      // default read connections and then wanting the writer can't get it, and
      // this task can't get a reader, until sqlx's acquire timeout breaks it. A
      // query-then-write pattern reaches this, and on every acquisition, since an
      // observed table missing from the schema is never cached.
      //
      // The trade: a table added to the observed set (via `subscribe`/
      // `subscribe_stream`) after this warm-up goes unwarmed for this
      // transaction - empty `primary_key`, meaningless `rowid` if `WITHOUT
      // ROWID`. That window is wider than the wait for the permit: the
      // preupdate hook reads the observed set live at fire time, not from a
      // snapshot taken here or in `register_hooks`, so a `subscribe()` landing
      // mid-transaction still delivers a change with an empty `primary_key`.
      // The window closes at end-of-transaction, not at permit acquisition.
      //
      // Do not "shrink" it by re-checking `ensure_table_info()` after the
      // permit is acquired. That re-check is only cheap when its work list is
      // empty, and the list never empties for an observed name that doesn't
      // resolve in the schema: `query_table_info` returns `Ok(None)` and the
      // warn-only branch never calls `set_table_info`, so the name stays
      // queued forever. The re-check would then await a read-pool connection
      // *while holding the write permit* on every acquisition for such a
      // database, reintroducing this exact deadlock (verified: it deadlocks).
      // A `try_acquire` variant avoids the deadlock but leaves the window open
      // anyway, per the paragraph above, so it buys nothing.
      self.ensure_table_info().await?;

      let writer = self
         .db
         .acquire_writer()
         .await
         .map_err(crate::error::Error::ConnMgr)?;

      let mut observable = ObservableWriteGuard {
         writer: Some(InnerWriter::Regular(writer)),
         hooks_registered: false,
         raw_db: None,
         brokers: HashMap::new(),
      };

      let mut brokers = HashMap::with_capacity(1);
      brokers.insert("main".to_string(), Arc::clone(&self.broker));
      observable.register_hooks(brokers).await?;
      Ok(observable)
   }

   /// Acquire an observable write guard with one or more databases attached.
   ///
   /// Each change is published to the broker of the database that **owns**
   /// the affected table: a write to `other.users` (where `other` is some
   /// attached database's schema alias) notifies that database's own
   /// subscribers, while a write to `main.users` notifies this database's -
   /// provided this database's own observation is enabled at all; see below.
   ///
   /// Only attached databases in [`AttachedMode::ReadWrite`] that themselves
   /// have observation enabled contribute a broker:
   /// - **`ReadOnly` attachments are skipped because their write permit isn't
   ///   held here - not because they can't be written through.** `ReadOnly`
   ///   describes which locks are taken, not an enforced restriction: databases
   ///   are attached as a plain quoted path, so SQLite is never asked to reject
   ///   writes to them, and such a write lands *and* goes unobserved. Enforcing
   ///   it would mean a `file:...?mode=ro` URI - a behavior change in
   ///   `sqlx-sqlite-conn-mgr`'s `ATTACH` construction, left as follow-up work.
   ///   The skip matters for more than tidiness: every broker in the hook map
   ///   must belong to a database whose write permit *this guard* holds for
   ///   its whole lifetime - that invariant is what makes the commit/rollback
   ///   fan-out safe, since it guarantees no other, independent writer can be
   ///   committing or rolling back that same connection concurrently with
   ///   this guard's hooks. Adding a `ReadOnly` attachment's broker to the map
   ///   would violate that and risks corrupting an unrelated connection's
   ///   buffer. Do not "fix" the skip by removing it.
   /// - A `ReadWrite` attachment with no observation enabled has nowhere for
   ///   its changes to go. It's left out of the broker map rather than routed
   ///   to `self`'s broker, which is what makes the preupdate callback drop
   ///   those changes instead of misattributing them to this database.
   ///
   /// **This database's own observation is independent of the attachments'.**
   /// Reaching this method means `self.broker` exists, so `"main"` always gets
   /// a map entry and this database's own changes are always buffered and
   /// published to it - to no effect if nothing ever subscribed, but the
   /// per-row buffering and `TableInfo` warming still happen. An attachment's
   /// observation lives on *its* database's own slot, discovered below, and
   /// neither side implies the other. For the case this type cannot express -
   /// this database unobserved, an attached `ReadWrite` database observed on
   /// its own - see [`acquire_writer_with_attached_brokers`], which takes the
   /// main broker as an `Option` precisely because `Self` cannot be built
   /// without one.
   ///
   /// Each participating database's `TableInfo` cache is warmed from its own
   /// read pool, not just `self`'s. An attached table's `TableInfo` is only
   /// ever populated by that database's own writer acquisitions, which may
   /// never have happened before it is attached here; without this, its
   /// changes would carry an empty `primary_key` and a meaningless `rowid`
   /// for a `WITHOUT ROWID` table.
   ///
   /// Warming happens before the writer is acquired, so the single write
   /// permit is not held while waiting on a read-pool connection - see
   /// [`acquire_writer`](Self::acquire_writer)'s body for the deadlock that
   /// ordering avoids.
   pub async fn acquire_writer_with_attached(
      &self,
      specs: Vec<AttachedSpec>,
   ) -> Result<ObservableWriteGuard> {
      // `self.broker` always exists, so `Some` always. Only the free function's
      // other caller passes `None`; see its doc.
      acquire_writer_with_attached_brokers(&self.db, Some(Arc::clone(&self.broker)), specs).await
   }

   /// Ensures TableInfo is set for all observed tables.
   ///
   /// Uses the read pool to query schema information, respecting conn-mgr's
   /// requirement that all connections be acquired through it.
   async fn ensure_table_info(&self) -> Result<()> {
      let observed = self.broker.get_observed_tables();

      // Collect tables that need schema info
      let tables_to_query: Vec<String> = observed
         .into_iter()
         .filter(|table| self.broker.get_table_info(table).is_none())
         .collect();

      if tables_to_query.is_empty() {
         return Ok(());
      }

      // Use read pool to query schema
      let pool = self.db.read_pool().map_err(crate::error::Error::ConnMgr)?;
      let mut conn = pool.acquire().await.map_err(crate::error::Error::Sqlx)?;

      for table in tables_to_query {
         match query_table_info(&mut conn, &table).await {
            Ok(Some(info)) => {
               debug!(table = %table, pk_columns = ?info.pk_columns, without_rowid = info.without_rowid, "Queried table info");
               self.broker.set_table_info(&table, info);
            }
            Ok(None) => {
               warn!(table = %table, "Table not found in schema");
            }
            Err(e) => {
               warn!(table = %table, error = %e, "Failed to query table info");
            }
         }
      }

      Ok(())
   }

   /// Get the underlying `SqliteDatabase`.
   pub fn inner(&self) -> &Arc<SqliteDatabase> {
      &self.db
   }

   /// Get the list of currently observed tables.
   pub fn observed_tables(&self) -> Vec<String> {
      self.broker.get_observed_tables()
   }

   /// Returns a reference to the underlying observation broker.
   pub fn broker(&self) -> &Arc<ObservationBroker> {
      &self.broker
   }
}

/// Acquire an observable write guard with one or more databases attached, without
/// requiring `main_db`'s own observation to be enabled.
///
/// A free function rather than a method because `ObservableSqliteDatabase`
/// cannot be constructed without a broker, which is exactly the case this
/// exists for: `main_db` unobserved, with only an attached `ReadWrite`
/// database observed on its own. The method form seeded `"main"`
/// unconditionally, so that combination could not be represented at all and
/// an attached database's subscribers silently received nothing.
///
/// `main_broker` is added to the broker map - and `main_db` to the set whose
/// `TableInfo` is warmed - only when `Some`. With `None`, `main_db`'s writes
/// still happen but are neither buffered nor published, exactly as on a plain
/// [`acquire_writer`](ObservableSqliteDatabase::acquire_writer) against an
/// unobserved database. `ObservableSqliteDatabase::acquire_writer_with_attached`
/// always passes `Some`; `sqlx_sqlite_toolkit::DatabaseWrapper::acquire_writer_with_attached`
/// is what passes `None`, reading the slot directly rather than through a
/// handle it may not be able to build.
///
/// The `ReadOnly`-skip and unobserved-`ReadWrite`-drop rules are unchanged from
/// [`acquire_writer_with_attached`]; only `main_db`'s own treatment differs.
///
/// # Preconditions
///
/// When `main_broker` is `Some`, it must be `main_db`'s own broker - what
/// `main_db.observer_slot().get::<ObservationBroker>()` returned when the caller
/// read it - not another database's and not one left over from an earlier
/// observation cycle. Passing another database's broker would publish
/// `main_db`'s changes to that database's subscribers, the misattribution the
/// `ReadOnly`-skip and `ReadWrite`-drop rules exist to prevent, and would break
/// the write-permit invariant, since nothing here acquires a writer on whatever
/// database that broker belongs to.
///
/// Deliberately unchecked: the slot is legitimately mutable between the
/// caller's read and this call, so "does the slot hold this exact `Arc` now"
/// has no stable answer to assert on. The precondition is on what the caller
/// read, not on what the slot holds now.
///
/// [`acquire_writer_with_attached`]: ObservableSqliteDatabase::acquire_writer_with_attached
pub async fn acquire_writer_with_attached_brokers(
   main_db: &Arc<SqliteDatabase>,
   main_broker: Option<Arc<ObservationBroker>>,
   specs: Vec<AttachedSpec>,
) -> Result<ObservableWriteGuard> {
   // Validate before the broker map is built, so "validation precedes map
   // construction" is a property of this control flow rather than an accident of
   // the ATTACH further down happening to reject the same input later.
   // `validate_attached_specs` rejects `main`/`temp` (case-insensitively) and
   // any two specs sharing an alias. conn-mgr re-validates internally - it has
   // to, being callable directly - and that second pass is an idempotent no-op
   // here, not a redundant check to delete from either side.
   sqlx_sqlite_conn_mgr::validate_attached_specs(&specs).map_err(crate::error::Error::ConnMgr)?;

   // Build the broker map, and collect every observable whose `TableInfo`
   // cache needs warming, before `specs` is consumed by the conn-mgr call
   // below. `AttachedSpec` only needs to be read here, not cloned - the
   // whole `Vec` is handed off afterward.
   let mut brokers: HashMap<String, Arc<ObservationBroker>> = HashMap::new();
   let mut participating: Vec<ObservableSqliteDatabase> = Vec::new();

   if let Some(broker) = main_broker {
      brokers.insert("main".to_string(), Arc::clone(&broker));
      participating.push(ObservableSqliteDatabase::from_broker(
         Arc::clone(main_db),
         broker,
      ));
   }

   for spec in &specs {
      // See `acquire_writer_with_attached`'s doc for why this skip is
      // load-bearing rather than a mere filter: nothing asks SQLite to
      // enforce ReadOnly on an attached schema, so this is about which write
      // permits this guard actually holds, not about which writes are
      // possible.
      if spec.mode != AttachedMode::ReadWrite {
         continue;
      }

      if let Some(broker) = spec.database.observer_slot().get::<ObservationBroker>() {
         // A handle is needed for both halves here - the broker for the map,
         // and something to call `ensure_table_info()` on, which reads the
         // attached database's own read pool - so it's rebuilt from the
         // broker rather than read out whole. See `from_broker`'s doc.
         let observable =
            ObservableSqliteDatabase::from_broker(Arc::clone(&spec.database), broker);

         // Fail loud rather than let `insert` silently overwrite. The
         // validation above already rules this out, so it should never fire;
         // it's an independent guard so that a regression there, or in the
         // ATTACH ordering, breaks here instead of resurfacing as one broker's
         // changes misattributed to another's subscribers.
         if brokers
            .insert(spec.schema_name.clone(), Arc::clone(&observable.broker))
            .is_some()
         {
            return Err(crate::error::Error::BrokerAliasCollision(
               spec.schema_name.clone(),
            ));
         }
         participating.push(observable);
      }
   }

   // Populates each participating database's own TableInfo cache - see
   // `acquire_writer_with_attached`'s doc for why an empty primary_key, not a
   // wrong-column decode, is what's actually at stake here.
   for observable in &participating {
      observable.ensure_table_info().await?;
   }

   let writer = sqlx_sqlite_conn_mgr::acquire_writer_with_attached(main_db, specs)
      .await
      .map_err(crate::error::Error::ConnMgr)?;

   let mut observable = ObservableWriteGuard {
      writer: Some(InnerWriter::Attached(writer)),
      hooks_registered: false,
      raw_db: None,
      brokers: HashMap::new(),
   };

   // `brokers` can be empty here even though a caller's own gate found at least
   // one side observed: `DatabaseWrapper::acquire_writer_with_attached` reads
   // `main_db`'s slot (and each `ReadWrite` spec's) to decide whether to take
   // this path at all, but that read and this one are not atomic with each
   // other, so observation disabled on every side in between leaves nothing in
   // the map. Skipping registration in that case is behaviorally identical to
   // registering hooks against an empty map - there is nothing to publish to
   // either way - but avoids paying for `lock_handle()` and FFI hook
   // registration for nothing, and avoids newly requiring
   // `SQLITE_ENABLE_PREUPDATE_HOOK` on a call that, had the race not happened,
   // would have taken the plain, unobserved path instead. `hooks_registered`
   // stays `false` (its constructed default above), so `Drop` correctly does
   // no cleanup.
   if !brokers.is_empty()
      && let Err(err) = observable.register_hooks(brokers).await
   {
      // register_hooks failed before touching any of observable's state
      // (see its own body), so the writer is untouched and still has the
      // ATTACH(es) live on it. Detach before propagating: AttachedWriteGuard's
      // own Drop deliberately doesn't detach (see its doc), and the write
      // pool is max_connections(1), so leaving the alias attached here
      // would strand it on the pooled connection - every later acquisition
      // that reuses the same alias would then fail at ATTACH with
      // "database is already in use", permanently.
      if let Err(detach_err) = observable.detach_all().await {
         warn!(
            "failed to detach after register_hooks failed ({err}); the \
             write connection may be stuck with a stale ATTACH: {detach_err}"
         );
      }
      return Err(err);
   }
   Ok(observable)
}

impl Clone for ObservableSqliteDatabase {
   fn clone(&self) -> Self {
      Self {
         db: Arc::clone(&self.db),
         broker: Arc::clone(&self.broker),
      }
   }
}

/// Either kind of writer an `ObservableWriteGuard` may wrap.
///
/// Both `WriteGuard` and `AttachedWriteGuard` already `Deref`/`DerefMut` to
/// `SqliteConnection`, so giving this enum the same impls (matching each
/// variant to its inner guard) lets `ObservableWriteGuard` stay agnostic to
/// which one it holds everywhere except construction.
enum InnerWriter {
   Regular(WriteGuard),
   Attached(AttachedWriteGuard),
}

impl Deref for InnerWriter {
   type Target = SqliteConnection;

   fn deref(&self) -> &Self::Target {
      match self {
         InnerWriter::Regular(w) => w,
         InnerWriter::Attached(w) => w,
      }
   }
}

impl DerefMut for InnerWriter {
   fn deref_mut(&mut self) -> &mut Self::Target {
      match self {
         InnerWriter::Regular(w) => w,
         InnerWriter::Attached(w) => w,
      }
   }
}

/// The plain (non-observing) guard handed back by
/// [`ObservableWriteGuard::into_inner`].
///
/// Which variant comes back mirrors how the guard was acquired -
/// `Regular` from [`ObservableSqliteDatabase::acquire_writer`], `Attached`
/// from [`ObservableSqliteDatabase::acquire_writer_with_attached`].
///
/// `#[must_use]` like the guard it came out of, so `guard.into_inner();` as a
/// bare statement still warns: every hazard the inner guards warn about survives
/// the unwrapping, including a stranded `ATTACH` for the `Attached` variant.
#[must_use = "if unused, the write guard and locks are immediately dropped"]
pub enum UnobservedWriter {
   Regular(WriteGuard),
   Attached(AttachedWriteGuard),
}

/// RAII guard for observable write access to the database.
///
/// Wraps either a `WriteGuard` or an `AttachedWriteGuard` from
/// `sqlx-sqlite-conn-mgr` and adds change tracking via SQLite hooks. Changes
/// are published to subscribers when transactions commit.
#[must_use = "if unused, the write lock is immediately released"]
pub struct ObservableWriteGuard {
   writer: Option<InnerWriter>,
   hooks_registered: bool,
   /// Raw sqlite3 pointer, cached during register_hooks so we can
   /// call unregister_hooks synchronously in Drop without needing
   /// the async lock_handle.
   raw_db: Option<*mut sqlite3>,
   /// Brokers hooks were registered with, keyed by schema alias. Retained
   /// (rather than discarded once `hooks::register_hooks` has its own copy)
   /// so `Drop` can discard each one's buffered-but-uncommitted events if
   /// this guard is dropped without an explicit commit or rollback - see
   /// `Drop`'s impl for why that's safe to do unconditionally. Empty until
   /// `register_hooks` populates it.
   brokers: HashMap<String, Arc<ObservationBroker>>,
}

// SAFETY: The raw_db pointer is only used for hook registration/unregistration
// and is always accessed from the same logical owner. The underlying sqlite3
// connection is already Send via sqlx's PoolConnection.
unsafe impl Send for ObservableWriteGuard {}

impl ObservableWriteGuard {
   /// Registers SQLite observation hooks on this writer.
   async fn register_hooks(
      &mut self,
      brokers: HashMap<String, Arc<ObservationBroker>>,
   ) -> Result<()> {
      if self.hooks_registered {
         return Ok(());
      }

      debug!("Registering SQLite observation hooks on WriteGuard");

      let writer = self.writer.as_mut().expect("writer already taken");

      // Get raw SQLite handle
      let mut handle = writer
         .lock_handle()
         .await
         .map_err(|e| crate::Error::Database(format!("Failed to lock connection handle: {}", e)))?;

      let db: *mut sqlite3 = handle.as_raw_handle().as_ptr();

      unsafe {
         hooks::register_hooks(db, brokers.clone())?;
      }

      // Cache the raw pointer so Drop can call unregister_hooks synchronously.
      // SAFETY: The pointer remains valid for the lifetime of the writer,
      // which we own via self.writer.
      self.raw_db = Some(db);
      self.hooks_registered = true;
      self.brokers = brokers;
      Ok(())
   }

   /// Discards every broker's buffered-but-uncommitted events.
   ///
   /// Safe to call unconditionally, regardless of whether a commit or
   /// rollback already ran: `on_commit` drains the buffer via `mem::take`
   /// before publishing, and an explicit `ROLLBACK`'s own rollback_hook
   /// already clears it too, so calling this afterward always finds nothing
   /// left to discard. The only case where it does something is the one it
   /// exists for: hooks torn down - by [`Drop`](Self), [`into_inner`], or
   /// [`detach_all`] - with no commit or rollback ever having run, which
   /// would otherwise let this transaction's buffered events resurface as
   /// phantom changes on the *next* transaction's commit.
   ///
   /// [`into_inner`]: Self::into_inner
   /// [`detach_all`]: Self::detach_all
   fn flush_all_brokers(&self) {
      for broker in self.brokers.values() {
         broker.on_rollback();
      }
   }

   /// Consumes this wrapper and returns the underlying write guard.
   ///
   /// Hooks are unregistered before returning the guard, so it can be
   /// safely used without observation. Also flushes every broker's buffer
   /// (see [`flush_all_brokers`](Self::flush_all_brokers)) - safe to call
   /// whether or not a commit/rollback already ran, and necessary if this is
   /// called mid-transaction, with no commit or rollback yet sent.
   pub fn into_inner(mut self) -> UnobservedWriter {
      // Unregister hooks before returning the writer to prevent
      // use-after-free if the broker is dropped before the connection is reused.
      if self.hooks_registered
         && let Some(db) = self.raw_db
      {
         unsafe {
            crate::hooks::unregister_hooks(db);
         }
         trace!("Hooks unregistered before returning inner writer");
         self.flush_all_brokers();
      }
      self.hooks_registered = false;
      self.raw_db = None;
      match self.writer.take().expect("writer already taken") {
         InnerWriter::Regular(w) => UnobservedWriter::Regular(w),
         InnerWriter::Attached(w) => UnobservedWriter::Attached(w),
      }
   }

   /// Unregisters hooks and detaches any databases attached to this writer.
   ///
   /// If this guard wraps a plain (non-attached) writer, there is nothing to
   /// detach - this reduces to hook unregistration, safe to call regardless
   /// of which kind of writer this guard wraps. Also flushes every broker's
   /// buffer (see [`flush_all_brokers`](Self::flush_all_brokers)) - safe to
   /// call whether or not a commit/rollback already ran.
   pub async fn detach_all(mut self) -> Result<()> {
      if self.hooks_registered
         && let Some(db) = self.raw_db
      {
         unsafe {
            crate::hooks::unregister_hooks(db);
         }
         trace!("Hooks unregistered before detach_all");
         self.flush_all_brokers();
      }
      self.hooks_registered = false;
      self.raw_db = None;

      match self.writer.take().expect("writer already taken") {
         InnerWriter::Regular(_) => Ok(()),
         InnerWriter::Attached(w) => w.detach_all().await.map_err(crate::error::Error::ConnMgr),
      }
   }
}

impl Drop for ObservableWriteGuard {
   fn drop(&mut self) {
      if self.hooks_registered
         && let Some(db) = self.raw_db
      {
         // SAFETY: db was obtained from lock_handle during register_hooks and
         // remains valid because we still own the writer (self.writer). The
         // writer has not been taken (into_inner/detach_all clear
         // hooks_registered before taking it).
         unsafe {
            hooks::unregister_hooks(db);
         }
         trace!("ObservableWriteGuard dropped, hooks unregistered");
         self.flush_all_brokers();
      }
   }
}

impl Deref for ObservableWriteGuard {
   type Target = SqliteConnection;

   fn deref(&self) -> &Self::Target {
      self.writer.as_ref().expect("writer already taken")
   }
}

impl DerefMut for ObservableWriteGuard {
   fn deref_mut(&mut self) -> &mut Self::Target {
      self.writer.as_mut().expect("writer already taken")
   }
}
