//! SQLite plugin commands
//!
//! This module implements the Tauri command handlers that the frontend calls.
//! Each command manages database connections through the DbInstances state.
//!
//! # Lock order: `DbInstances` before `ObserverRegistrations`
//!
//! Any command that touches both the `db_instances` map (which owns each
//! database's observer/broker via `DatabaseWrapper`) and `observer_regs` (the
//! per-webview refcount tracking who is observing each database) must acquire
//! `db_instances.inner`'s write lock first, then perform its `observer_regs`
//! register/release call while still holding it - not release the db lock and
//! re-acquire `observer_regs` afterward. `observe()`, `unobserve()`,
//! `remove()`'s `remove_inner` helper, `close_database_inner`,
//! `close_all_wrappers`, and the window-`Destroyed` cleanup handler all follow
//! this order today.
//!
//! ## What the compiler checks, and what it doesn't
//!
//! This is more than a documented convention, but less than a proof. Every
//! mutating method on `ObserverRegistrations` (`register`, `release`,
//! `release_all_for_label`, `clear_for_db`, `clear_all`) takes a
//! [`DbInstancesGuard`](crate::subscriptions::DbInstancesGuard) as its first
//! parameter - a witness that the caller holds a `DbInstances` write guard at
//! the point of the call. So *co-holding* the two locks is a compile-time
//! obligation: a call site with no db guard in scope fails to compile. That is
//! the shape that was reintroduced four separate times while the rule lived
//! only in prose here.
//!
//! Two things the type system structurally cannot see:
//!
//! - **Acquisition order.** A witness proves a guard exists, not that it was
//!   taken before `observer_regs`'s lock.
//! - **Drop and reacquire mid-sequence.** Releasing the db guard partway
//!   through and immediately taking a fresh one satisfies the witness (a real
//!   guard is a real guard) and passes both deterministic tests, while
//!   reopening the exact race this rule exists to close.
//!
//! Those shapes are covered by tests only, and unevenly.
//! `tests::test_observe_holds_db_lock_across_register` and
//! `tests::test_unobserve_holds_db_lock_across_release` in `src/lib.rs` are
//! deterministic and authoritative for "a guard is held at the call site", one
//! per side. `tests::test_concurrent_observe_and_unobserve_keep_broker_and_registrations_in_sync`
//! is the *only* guard for the drop-and-reacquire shape, and it is
//! probabilistic - its detection rate for a regressed `observe()` side is low
//! (see that test's doc). Treat a green CI run as evidence rather than a
//! guarantee here, and read that doc before changing it.
//!
//! Without a single consistent order held across the whole
//! enable+register/release+disable sequence, two commands running
//! concurrently for the same database (e.g. window B's `observe()` racing
//! window A's `unobserve()`) can interleave such that `is_observing()` and
//! "has any registered observers" disagree - e.g. B registers into a broker
//! that A's concurrent `unobserve()` just destroyed after a stale refcount
//! read. The tests named above are the regression guards.
//!
//! ## `ActiveSubscriptions` is a separate pair, with no fixed order
//!
//! The rule above governs the `db_instances` / `observer_regs` pair only. A
//! third store, `active_subs`, is locked in *both* orders relative to the db
//! lock: `unobserve()` and the window-`Destroyed` handler in `src/lib.rs` call
//! `active_subs.remove_for_db()` while still holding the db guard, whereas
//! `remove()`, `close_database_inner` and `close_all_loaded_databases` take and
//! release `active_subs` *before* acquiring it. `subscribe()` is in both
//! orderings at once: it calls `active_subs.count_for_db()` before acquiring
//! the db lock, then `active_subs.insert()` while still holding it. Do not
//! "fix" one side to match the other by moving a call across a db-lock
//! acquisition without re-reading the reason below.
//!
//! That inconsistency cannot deadlock, and the reason is checkable rather than
//! a matter of inspection: no `ActiveSubscriptions` method ever acquires
//! another lock while holding its own. Everything it does under that lock is a
//! map removal, a `String` comparison, and `AbortHandle::abort()` - which
//! schedules task shutdown on the runtime instead of running the aborted
//! future's destructor inline. So the "wrong" order never closes a cycle: one
//! side waits on `active_subs` while holding the db lock, but nothing ever
//! waits on the db lock while holding `active_subs`. That property follows from
//! what `ActiveSubscription` stores - see its doc comment - not from the call
//! order here, so a change to that struct is what could break it.

use futures::StreamExt;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx_sqlite_conn_mgr::SqliteDatabaseConfig;
use sqlx_sqlite_toolkit::{
   ActiveInterruptibleTransaction, ActiveInterruptibleTransactions, ActiveRegularTransactions,
   DatabaseWrapper, Statement, TransactionWriter, WriteQueryResult,
};
use std::sync::Arc;
use tauri::ipc::Channel;
use tauri::{AppHandle, Runtime, State};
use tracing::debug;
use uuid::Uuid;

use crate::{
   DbInstances, Error, MigrationEvent, MigrationStates, Result,
   subscriptions::{
      ActiveSubscriptions, ObserverConfigParams, ObserverRegistrations, TableChangePayload,
      event_to_payload,
   },
};
use crate::{close_all_loaded_databases, close_database, connect_to_database};

/// Token representing an active interruptible transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionToken {
   pub db_key: String,
   pub transaction_id: String,
}

/// Actions that can be taken on an interruptible transaction
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum TransactionAction {
   Continue { statements: Vec<Statement> },
   Commit,
   Rollback,
}

/// Serializable attached database specification for TypeScript interface
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AttachedDatabaseSpec {
   /// Key of the database to attach (must be loaded via `load()` first)
   pub database_key: String,
   /// Schema name to use for the attached database in queries
   pub schema_name: String,
   /// Access mode: "readOnly" or "readWrite"
   pub mode: AttachedDatabaseMode,
}

/// Access mode for attached databases
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AttachedDatabaseMode {
   ReadOnly,
   ReadWrite,
}

/// Convert serializable specs to internal specs by resolving database references
fn resolve_attached_specs(
   specs: Vec<AttachedDatabaseSpec>,
   db_instances: &std::collections::HashMap<String, DatabaseWrapper>,
) -> Result<Vec<sqlx_sqlite_conn_mgr::AttachedSpec>> {
   let mut resolved = Vec::new();

   for spec in specs {
      let wrapper = db_instances
         .get(&spec.database_key)
         .ok_or_else(|| Error::DatabaseNotLoaded(spec.database_key.clone()))?;

      let mode = match spec.mode {
         AttachedDatabaseMode::ReadOnly => sqlx_sqlite_conn_mgr::AttachedMode::ReadOnly,
         AttachedDatabaseMode::ReadWrite => sqlx_sqlite_conn_mgr::AttachedMode::ReadWrite,
      };

      resolved.push(sqlx_sqlite_conn_mgr::AttachedSpec {
         database: Arc::clone(wrapper.inner()),
         schema_name: spec.schema_name,
         mode,
      });
   }

   Ok(resolved)
}

/// Load/connect to a database and store it in plugin state.
///
/// If the database is already loaded, returns the existing connection.
/// Otherwise, creates a new connection with optional custom configuration.
///
/// `db_key` must be a registration key from
/// [`Builder::register_database`] / [`SetupRegistrar::register_database`].
/// Unregistered keys are rejected with `PATH_NOT_REGISTERED`.
///
/// # Migration Timing
///
/// If migrations are registered for this database, this function waits for them
/// to complete before proceeding. The migration task (spawned at plugin setup)
/// already called `SqliteDatabase::connect()`, which cached the database instance.
/// When we call `connect()` here, we get the **same cached instance** from the
/// registry - so we're not creating duplicate connections.
#[tauri::command]
pub async fn load<R: Runtime>(
   app: AppHandle<R>,
   db_key: String,
   custom_config: Option<SqliteDatabaseConfig>,
) -> Result<String> {
   let response = connect_to_database(&app, &db_key, custom_config).await?;
   Ok(response.path.to_string_lossy().into_owned())
}

/// Execute a write query (INSERT, UPDATE, DELETE, etc.)
#[tauri::command]
pub async fn execute(
   db_instances: State<'_, DbInstances>,
   db_key: String,
   query: String,
   values: Vec<JsonValue>,
   attached: Option<Vec<AttachedDatabaseSpec>>,
) -> Result<(u64, i64)> {
   let instances = db_instances.inner.read().await;

   let wrapper = instances
      .get(&db_key)
      .ok_or_else(|| Error::DatabaseNotLoaded(db_key.clone()))?;

   let mut builder = wrapper.execute(query, values);

   if let Some(specs) = attached {
      let resolved_specs = resolve_attached_specs(specs, &instances)?;
      builder = builder.attach(resolved_specs);
   }

   let result = builder.execute().await?;

   Ok((result.rows_affected, result.last_insert_id))
}

/// Execute multiple write statements atomically within a transaction
#[tauri::command]
pub async fn execute_transaction(
   db_instances: State<'_, DbInstances>,
   regular_txs: State<'_, ActiveRegularTransactions>,
   db_key: String,
   statements: Vec<Statement>,
   attached: Option<Vec<AttachedDatabaseSpec>>,
) -> Result<Vec<WriteQueryResult>> {
   let instances = db_instances.inner.read().await;

   let wrapper = instances
      .get(&db_key)
      .ok_or_else(|| Error::DatabaseNotLoaded(db_key.clone()))?;

   // Convert Statement structs to tuples for wrapper
   let stmt_tuples: Vec<(String, Vec<JsonValue>)> = statements
      .into_iter()
      .map(|s| (s.query, s.values))
      .collect();

   // Generate unique id for tracking this transaction
   let tx_id = Uuid::new_v4().to_string();

   // Resolve attached specs if provided
   let resolved_specs = if let Some(specs) = attached {
      Some(resolve_attached_specs(specs, &instances)?)
   } else {
      None
   };

   // Spawn transaction execution with join handle tracked for cleanup on close
   let wrapper_clone = wrapper.clone();
   let tx_id_clone = tx_id.clone();
   let regular_txs_clone = regular_txs.inner().clone();
   let (result_tx, result_rx) = tokio::sync::oneshot::channel();

   let handle = tokio::spawn(async move {
      // Convert String to &str for execute_transaction
      let stmt_refs: Vec<(&str, Vec<JsonValue>)> = stmt_tuples
         .iter()
         .map(|(query, values)| (query.as_str(), values.clone()))
         .collect();

      let mut builder = wrapper_clone.execute_transaction(stmt_refs);

      if let Some(specs) = resolved_specs {
         builder = builder.attach(specs);
      }

      let result = builder.execute().await;

      // Remove from tracking when complete (even if result is Err)
      regular_txs_clone.remove(&tx_id_clone).await;
      let _ = result_tx.send(result);
   });

   regular_txs
      .insert(db_key.clone(), tx_id.clone(), handle)
      .await;

   match result_rx.await {
      Ok(result) => Ok(result?),
      Err(_) => {
         if let Some(handle) = regular_txs.inner().take_handle(&tx_id).await {
            match handle.await {
               Ok(()) => Err(Error::Other("Transaction completed without result".into())),
               Err(e) if e.is_cancelled() => Err(Error::Toolkit(
                  sqlx_sqlite_toolkit::Error::TransactionCancelled(db_key),
               )),
               Err(e) => Err(Error::Other(format!("Transaction task panicked: {e}"))),
            }
         } else {
            Err(Error::Toolkit(
               sqlx_sqlite_toolkit::Error::TransactionCancelled(db_key),
            ))
         }
      }
   }
}

/// Execute a SELECT query returning all matching rows.
///
/// Returns the entire result set in a single response. For large or unbounded queries,
/// prefer `fetch_page` with keyset pagination to keep memory usage bounded.
#[tauri::command]
pub async fn fetch_all(
   db_instances: State<'_, DbInstances>,
   db_key: String,
   query: String,
   values: Vec<JsonValue>,
   attached: Option<Vec<AttachedDatabaseSpec>>,
) -> Result<Vec<IndexMap<String, JsonValue>>> {
   let instances = db_instances.inner.read().await;

   let wrapper = instances
      .get(&db_key)
      .ok_or_else(|| Error::DatabaseNotLoaded(db_key.clone()))?;

   let mut builder = wrapper.fetch_all(query, values);

   if let Some(specs) = attached {
      let resolved_specs = resolve_attached_specs(specs, &instances)?;
      builder = builder.attach(resolved_specs);
   }

   let result = builder.execute().await?;

   Ok(result)
}

/// Execute a SELECT query expecting zero or one result
#[tauri::command]
pub async fn fetch_one(
   db_instances: State<'_, DbInstances>,
   db_key: String,
   query: String,
   values: Vec<JsonValue>,
   attached: Option<Vec<AttachedDatabaseSpec>>,
) -> Result<Option<IndexMap<String, JsonValue>>> {
   let instances = db_instances.inner.read().await;

   let wrapper = instances
      .get(&db_key)
      .ok_or_else(|| Error::DatabaseNotLoaded(db_key.clone()))?;

   let mut builder = wrapper.fetch_one(query, values);

   if let Some(specs) = attached {
      let resolved_specs = resolve_attached_specs(specs, &instances)?;
      builder = builder.attach(resolved_specs);
   }

   let result = builder.execute().await?;

   Ok(result)
}

/// Execute a paginated SELECT query using keyset (cursor-based) pagination
#[allow(clippy::too_many_arguments)]
#[tauri::command]
pub async fn fetch_page(
   db_instances: State<'_, DbInstances>,
   db_key: String,
   query: String,
   values: Vec<JsonValue>,
   keyset: Vec<sqlx_sqlite_toolkit::KeysetColumn>,
   page_size: usize,
   after: Option<Vec<JsonValue>>,
   before: Option<Vec<JsonValue>>,
   attached: Option<Vec<AttachedDatabaseSpec>>,
) -> Result<sqlx_sqlite_toolkit::KeysetPage> {
   if after.is_some() && before.is_some() {
      return Err(Error::Toolkit(
         sqlx_sqlite_toolkit::Error::ConflictingCursors,
      ));
   }

   let instances = db_instances.inner.read().await;

   let wrapper = instances
      .get(&db_key)
      .ok_or_else(|| Error::DatabaseNotLoaded(db_key.clone()))?;

   let mut builder = wrapper.fetch_page(query, values, keyset, page_size);

   if let Some(cursor_values) = after {
      builder = builder.after(cursor_values);
   } else if let Some(cursor_values) = before {
      builder = builder.before(cursor_values);
   }

   if let Some(specs) = attached {
      let resolved_specs = resolve_attached_specs(specs, &instances)?;
      builder = builder.attach(resolved_specs);
   }

   let result = builder.execute().await?;

   Ok(result)
}

/// Close the loaded instance for a registered database key.
///
/// Returns `true` if the database was loaded and successfully closed.
/// Returns `false` if the database was not loaded (nothing to close).
/// Returns `Err` if transaction cleanup or pool close fails (database file
/// may not be safe to delete or recreate).
/// Active subscriptions for this key are aborted, and in-flight transactions
/// are cleaned up (interruptible transactions rolled back; regular transaction
/// tasks aborted and awaited) before the connection pool is closed.
#[tauri::command]
pub async fn close(
   db_instances: State<'_, DbInstances>,
   active_subs: State<'_, ActiveSubscriptions>,
   observer_regs: State<'_, ObserverRegistrations>,
   interruptible_txs: State<'_, ActiveInterruptibleTransactions>,
   regular_txs: State<'_, ActiveRegularTransactions>,
   db_key: String,
) -> Result<bool> {
   close_database(
      &db_key,
      &db_instances,
      &active_subs,
      &observer_regs,
      &interruptible_txs,
      &regular_txs,
   )
   .await
}

/// Close all database connections.
///
/// All active subscriptions are aborted and in-flight transactions are cleaned
/// up (interruptible transactions rolled back; regular transaction tasks
/// aborted and awaited) before connection pools are closed.
/// Returns `Err` if transaction cleanup or any pool close fails.
#[tauri::command]
pub async fn close_all(
   db_instances: State<'_, DbInstances>,
   active_subs: State<'_, ActiveSubscriptions>,
   observer_regs: State<'_, ObserverRegistrations>,
   interruptible_txs: State<'_, ActiveInterruptibleTransactions>,
   regular_txs: State<'_, ActiveRegularTransactions>,
) -> Result<()> {
   close_all_loaded_databases(
      &db_instances,
      &active_subs,
      &observer_regs,
      &interruptible_txs,
      &regular_txs,
   )
   .await
}

/// Close database connection and remove all database files
///
/// Returns `true` if the database was loaded and successfully removed.
/// Returns `false` if the database was not loaded (nothing to remove).
/// Returns `Err` if transaction cleanup or file removal fails, or if the
/// whole operation doesn't finish within `CLOSE_TIMEOUT` (see `remove_inner`
/// for why that bound exists here and not just on `close()`/`close_all()`).
/// Active subscriptions for this key are aborted, and in-flight transactions
/// are cleaned up (interruptible transactions rolled back; regular
/// transaction tasks aborted and awaited) before the connection pool is
/// closed and the database's files are deleted.
#[tauri::command]
pub async fn remove(
   db_instances: State<'_, DbInstances>,
   active_subs: State<'_, ActiveSubscriptions>,
   observer_regs: State<'_, ObserverRegistrations>,
   interruptible_txs: State<'_, ActiveInterruptibleTransactions>,
   regular_txs: State<'_, ActiveRegularTransactions>,
   db_key: String,
) -> Result<bool> {
   let remove_result = tokio::time::timeout(
      crate::CLOSE_TIMEOUT,
      remove_inner(
         &db_instances,
         &active_subs,
         &observer_regs,
         &interruptible_txs,
         &regular_txs,
         &db_key,
      ),
   )
   .await;

   match remove_result {
      Ok(result) => result,
      Err(_) => Err(Error::Other(format!(
         "database remove timed out after {} seconds",
         crate::CLOSE_TIMEOUT.as_secs()
      ))),
   }
}

/// Abort in-flight transactions and subscriptions for `db_key`, then remove
/// its wrapper and delete its files - attempting the removal even if
/// transaction cleanup failed, mirroring `close_database_inner`'s
/// best-effort teardown in `src/lib.rs`.
///
/// Transaction cleanup must run first. `begin_interruptible_transaction` checks
/// the write connection out for the transaction's whole lifetime, and an
/// abandoned one is only reaped lazily, so without this call
/// `wrapper.remove()`'s `Pool::close()` - which has no timeout of its own -
/// waits on that connection indefinitely.
///
/// Lock order: db_instances write lock, then observer_regs (see the module doc).
/// One guard covers the map removal, the registration clear, and
/// `wrapper.remove()` itself, so nothing can interleave between them. That last
/// part matters because `wrapper.remove()` unlinks the `.db`/`-wal`/`-shm`
/// files: release the guard any earlier and a concurrent `connect_to_database()`
/// can hand back a live handle to this database - the wrapper being torn down,
/// or a freshly connected one at the same path - which the unlink then deletes
/// the files out from under.
///
/// That isn't free. `db_instances`'s write lock covers every loaded database, so
/// holding it across pool teardown and file I/O stalls unrelated databases; the
/// `CLOSE_TIMEOUT` in `remove()` bounds that rather than leaving it open-ended.
async fn remove_inner(
   db_instances: &DbInstances,
   active_subs: &ActiveSubscriptions,
   observer_regs: &ObserverRegistrations,
   interruptible_txs: &ActiveInterruptibleTransactions,
   regular_txs: &ActiveRegularTransactions,
   db_key: &str,
) -> Result<bool> {
   let mut last_error = None;

   active_subs.remove_for_db(db_key).await;

   if let Err(err) =
      sqlx_sqlite_toolkit::cleanup_transactions_for_db(db_key, interruptible_txs, regular_txs).await
   {
      last_error = Some(err.into());
   }

   let mut instances = db_instances.write().await;
   let wrapper = instances.remove(db_key);
   observer_regs.clear_for_db(&mut instances, db_key).await;

   let was_loaded = wrapper.is_some();
   if let Some(wrapper) = wrapper
      && let Err(err) = wrapper.remove().await
   {
      last_error = Some(err.into());
   }

   match last_error {
      Some(err) => Err(err),
      None => Ok(was_loaded),
   }
}

/// Get cached migration events for a database.
///
/// Returns all migration events that have been emitted for the specified database.
/// This allows the frontend to retrieve events even if they were missed due to timing.
///
/// Returns an empty array if no migrations are registered for this database.
#[tauri::command]
pub async fn get_migration_events(
   migration_states: State<'_, MigrationStates>,
   db_key: String,
) -> Result<Vec<MigrationEvent>> {
   let states = migration_states.0.read().await;

   match states.get(&db_key) {
      Some(state) => Ok(state.events.clone()),
      None => Ok(Vec::new()),
   }
}

/// Begin an interruptible transaction and return a token.
///
/// This begins a transaction, executes the initial statements, and returns a token
/// that can be used to continue, commit, or rollback the transaction.
/// The writer connection is held for the entire transaction duration.
#[tauri::command]
pub async fn begin_interruptible_transaction(
   db_instances: State<'_, DbInstances>,
   active_txs: State<'_, ActiveInterruptibleTransactions>,
   db_key: String,
   initial_statements: Vec<Statement>,
   attached: Option<Vec<AttachedDatabaseSpec>>,
) -> Result<TransactionToken> {
   let instances = db_instances.inner.read().await;

   let wrapper = instances
      .get(&db_key)
      .ok_or_else(|| Error::DatabaseNotLoaded(db_key.clone()))?;

   // Generate unique transaction ID
   let transaction_id = Uuid::new_v4().to_string();

   // Acquire appropriate writer based on whether databases are attached
   let mut writer = if let Some(specs) = attached {
      let resolved_specs = resolve_attached_specs(specs, &instances)?;
      let guard =
         sqlx_sqlite_conn_mgr::acquire_writer_with_attached(wrapper.inner(), resolved_specs)
            .await?;
      TransactionWriter::Attached(guard)
   } else {
      TransactionWriter::from(wrapper.acquire_writer().await?)
   };

   // Begin transaction
   writer.begin_immediate().await?;

   // Execute initial statements
   let mut active_tx =
      ActiveInterruptibleTransaction::new(db_key.clone(), transaction_id.clone(), writer);

   active_tx.continue_with(initial_statements).await?;

   // Store transaction state
   active_txs.insert(db_key.clone(), active_tx).await?;

   Ok(TransactionToken {
      db_key,
      transaction_id,
   })
}

/// Continue, commit, or rollback an interruptible transaction.
///
/// Returns a new token if continuing with more statements, or None if committed/rolled back.
#[tauri::command]
pub async fn transaction_continue(
   active_txs: State<'_, ActiveInterruptibleTransactions>,
   token: TransactionToken,
   action: TransactionAction,
) -> Result<Option<TransactionToken>> {
   match action {
      TransactionAction::Continue { statements } => {
         // Remove transaction to get mutable access
         let mut tx = active_txs
            .remove(&token.db_key, &token.transaction_id)
            .await?;

         // Execute statements on the transaction
         match tx.continue_with(statements).await {
            Ok(_results) => {
               // Re-insert transaction - if this fails, tx is dropped and auto-rolled back
               match active_txs.insert(token.db_key.clone(), tx).await {
                  Ok(()) => Ok(Some(token)),
                  Err(e) => {
                     // Transaction lost but will auto-rollback via Drop
                     Err(e.into())
                  }
               }
            }
            Err(e) => {
               // Execution failed, explicitly rollback before returning error
               let _ = tx.rollback().await;
               Err(e.into())
            }
         }
      }

      TransactionAction::Commit => {
         // Remove transaction and commit
         let tx = active_txs
            .remove(&token.db_key, &token.transaction_id)
            .await?;

         tx.commit().await?;
         Ok(None)
      }

      TransactionAction::Rollback => {
         // Remove transaction and rollback
         let tx = active_txs
            .remove(&token.db_key, &token.transaction_id)
            .await?;

         tx.rollback().await?;
         Ok(None)
      }
   }
}

/// Read from database within an interruptible transaction to see uncommitted writes.
///
/// This executes a SELECT query on the same connection as the transaction,
/// allowing you to see uncommitted data.
#[tauri::command]
pub async fn transaction_read(
   active_txs: State<'_, ActiveInterruptibleTransactions>,
   token: TransactionToken,
   query: String,
   values: Vec<JsonValue>,
) -> Result<Vec<IndexMap<String, JsonValue>>> {
   // Remove transaction to get mutable access
   let mut tx = active_txs
      .remove(&token.db_key, &token.transaction_id)
      .await?;

   // Execute read on the transaction
   match tx.read(query, values).await {
      Ok(results) => {
         // Re-insert transaction - if this fails, tx is dropped and auto-rolled back
         match active_txs.insert(token.db_key.clone(), tx).await {
            Ok(()) => Ok(results),
            Err(e) => {
               // Transaction lost but will auto-rollback via Drop
               Err(e.into())
            }
         }
      }
      Err(e) => {
         // Read failed, explicitly rollback before returning error
         let _ = tx.rollback().await;
         Err(e.into())
      }
   }
}

/// Enable observation on a database for change notifications.
///
/// Must be called before `subscribe()`. Configures the observer with the
/// specified tables and options.
///
/// Observation is additive and reference-counted per webview: calling this again
/// (from the same or a different window) merges the requested tables into the
/// existing broker rather than replacing it, so subscriptions already active in
/// any window - including this one - keep receiving notifications uninterrupted.
/// See issue #54.
///
/// `channelCapacity` and `captureValues` can only be set by the *first* window to
/// enable observation for a given database; a later call explicitly requesting
/// different values for either is rejected with `OBSERVATION_CONFIG_CONFLICT`
/// (see `sqlx_sqlite_toolkit::DatabaseWrapper::enable_observation` for the
/// underlying rule and why it can't be changed without dropping existing
/// subscribers). Omit the conflicting field(s) to keep using the active value,
/// or have every window call `unobserve()` first if the value must change.
///
/// `MAX_OBSERVED_TABLES` only bounds the size of a single `observe()` call's
/// request, not the accumulated set of tables observed on a database overall -
/// see the Resource Limits section of the README for why that's intentionally
/// left unbounded for now rather than fixed the wrong way.
///
/// Call `unobserve()` to release this window's registration. The underlying
/// broker and its subscriptions are only torn down once every window that
/// called `observe()` for this database has released its registration.
#[tauri::command]
pub async fn observe<R: Runtime>(
   db_instances: State<'_, DbInstances>,
   observer_regs: State<'_, ObserverRegistrations>,
   webview: tauri::Webview<R>,
   db_key: String,
   tables: Vec<String>,
   config: Option<ObserverConfigParams>,
) -> Result<()> {
   const MAX_OBSERVED_TABLES: usize = 100;
   const MAX_CHANNEL_CAPACITY: usize = 10_000;

   if tables.is_empty() || tables.len() > MAX_OBSERVED_TABLES {
      return Err(Error::InvalidConfig(format!(
         "tables count must be between 1 and {MAX_OBSERVED_TABLES}, got {}",
         tables.len()
      )));
   }

   // Lock order: db_instances write lock, then observer_regs - matched exactly
   // in unobserve() below, and held across the *entire* enable+register
   // sequence. Without a single lock spanning both state stores, a concurrent
   // unobserve() from another window could see this window's registration land
   // after it already decided the refcount had hit zero and torn the broker
   // down - registering into a broker that no longer exists, while observe()
   // had already returned Ok(()) to the frontend.
   let mut instances = db_instances.write().await;

   let wrapper = instances
      .get_mut(&db_key)
      .ok_or_else(|| Error::DatabaseNotLoaded(db_key.clone()))?;

   // Seed channelCapacity/captureValues from the live broker (if one exists)
   // rather than ObserverConfig's hardcoded defaults. Otherwise a
   // caller that supplies no `config` at all still produces a fully-populated
   // ObserverConfig with the crate's defaults (256 / true), which would look
   // like a genuinely conflicting request - and spuriously warn - the moment
   // another window's broker is already using different values.
   let seeded = wrapper.observable().map(|existing| {
      (
         existing.broker().channel_capacity(),
         existing.broker().capture_values(),
      )
   });

   let mut observer_config = sqlx_sqlite_observer::ObserverConfig::new().with_tables(tables);
   if let Some((capacity, capture)) = seeded {
      observer_config = observer_config
         .with_channel_capacity(capacity)
         .with_capture_values(capture);
   }

   if let Some(params) = config {
      if let Some(capacity) = params.channel_capacity {
         if capacity == 0 || capacity > MAX_CHANNEL_CAPACITY {
            return Err(Error::InvalidConfig(format!(
               "channel_capacity must be between 1 and {MAX_CHANNEL_CAPACITY}, got {capacity}"
            )));
         }
         observer_config = observer_config.with_channel_capacity(capacity);
      }
      if let Some(capture) = params.capture_values {
         observer_config = observer_config.with_capture_values(capture);
      }
   }

   // Reject rather than silently ignore an explicit request that conflicts with
   // the live broker's already-fixed channelCapacity/captureValues - both are
   // baked into the broadcast channel at creation time and can't change without
   // dropping every existing subscriber (see `enable_observation`'s doc for why).
   // Comparing the final `observer_config` against `seeded` is exactly "an
   // explicit param differs from the live broker": `observer_config` only
   // diverges from `seeded` above when a `params` field explicitly overrode it,
   // since a caller that omits a field never moves it off the seeded value.
   if let Some((capacity, capture)) = seeded {
      if observer_config.channel_capacity != capacity {
         return Err(Error::ObservationConfigConflict(format!(
            "observe() requested channelCapacity {} for database {db_key}, but \
             observation is already active with channelCapacity {capacity}; this value \
             is fixed by the first window to enable observation and can't be changed \
             without dropping every existing subscriber. Omit channelCapacity to keep \
             using the active value, or have every window call unobserve() first if it \
             must change.",
            observer_config.channel_capacity
         )));
      }
      if observer_config.capture_values != capture {
         return Err(Error::ObservationConfigConflict(format!(
            "observe() requested captureValues {} for database {db_key}, but observation \
             is already active with captureValues {capture}; this value is fixed by the \
             first window to enable observation and can't be changed without dropping \
             every existing subscriber. Omit captureValues to keep using the active \
             value, or have every window call unobserve() first if it must change.",
            observer_config.capture_values
         )));
      }
   }

   // Additive: reuses the existing broker (if any) rather than tearing it down,
   // so subscriptions belonging to other windows are never disturbed here.
   wrapper.enable_observation(observer_config);

   // Track this window as an observer of db_key so unobserve() knows whether
   // it's safe to tear the broker down (see ObserverRegistrations docs).
   // Registered while still holding `instances` - see the lock-order comment
   // above.
   let observer_count = observer_regs
      .register(&mut instances, &db_key, webview.label())
      .await;
   debug!(
      "observe: database {} now has {} registered observer(s)",
      db_key, observer_count
   );

   Ok(())
}

/// Subscribe to change notifications for specific tables.
///
/// Returns a subscription ID that can be used to unsubscribe later.
/// Change events are streamed to the frontend via Tauri Channel.
///
/// The calling window must have called `observe()` for `db_key` itself first;
/// this returns `OBSERVATION_NOT_ENABLED` otherwise, even if some other
/// window's registration currently has a broker active for this database (see
/// issue #54 - piggybacking on another window's registration used to let a
/// subscription outlive its own observer, then be silently aborted the moment
/// that other window called `unobserve()`). One caveat survives: webview
/// labels persist across a reload and registrations aren't cleared on one, so
/// this proves "this webview label called `observe()` at some point", not
/// "this specific page load did".
#[tauri::command]
pub async fn subscribe<R: Runtime>(
   db_instances: State<'_, DbInstances>,
   active_subs: State<'_, ActiveSubscriptions>,
   observer_regs: State<'_, ObserverRegistrations>,
   webview: tauri::Webview<R>,
   db_key: String,
   tables: Vec<String>,
   on_event: Channel<TableChangePayload>,
) -> Result<String> {
   const MAX_SUBSCRIPTIONS_PER_DATABASE: usize = 100;

   let sub_count = active_subs.count_for_db(&db_key).await;
   if sub_count >= MAX_SUBSCRIPTIONS_PER_DATABASE {
      return Err(Error::TooManySubscriptions(MAX_SUBSCRIPTIONS_PER_DATABASE));
   }

   let instances = db_instances.inner.read().await;

   let wrapper = instances
      .get(&db_key)
      .ok_or_else(|| Error::DatabaseNotLoaded(db_key.clone()))?;

   let observable = wrapper
      .observable()
      .ok_or_else(|| Error::ObservationNotEnabled(db_key.clone()))?;

   // This webview must be one of db_key's registered observers itself, not
   // merely subscribing while *some* window's broker happens to exist (see the
   // doc comment above and issue #54). Checked while still holding
   // `instances`'s read lock, alongside the `observable()` check above:
   // `unobserve()` needs the write lock to release a registration and tear the
   // broker down, so holding this read lock across the check and
   // `subscribe_stream()` below is what prevents a concurrent `unobserve()`
   // from doing so in between - the same lock order documented at the top of
   // this file.
   if !observer_regs.is_registered(&db_key, webview.label()).await {
      return Err(Error::ObservationNotEnabled(db_key.clone()));
   }

   // Create subscription stream
   let mut stream = observable.subscribe_stream(tables);

   // Generate unique subscription ID
   let subscription_id = Uuid::new_v4().to_string();

   // Spawn task to forward stream events to the Tauri Channel
   let sub_id = subscription_id.clone();
   let db_key_clone = db_key.clone();
   let active_subs_for_task = active_subs.inner().clone();

   // Ready signal so the task can't start forwarding (and, at the end, reaping
   // its own entry) until this function has actually inserted that entry into
   // `active_subs` below - otherwise, on a multi-thread runtime, a task whose
   // stream/channel ends immediately could run to completion and call
   // `remove()` on another worker thread *before* `insert()` below has run,
   // leaving a since-finished subscription registered forever with no task to
   // ever reap it - the exact leak the reap at the end of the task prevents.
   let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();

   let handle = tokio::spawn(async move {
      // See the ready_tx/ready_rx comment above: wait until this subscription
      // is registered in `active_subs` before doing anything observable.
      let _ = ready_rx.await;

      while let Some(event) = stream.next().await {
         let payload = event_to_payload(event);
         if on_event.send(payload).is_err() {
            // Channel closed (frontend disconnected)
            debug!("Subscription {} channel closed, stopping", sub_id);
            break;
         }
      }

      debug!("Subscription {} for db {} ended", sub_id, &db_key_clone);

      // Reap this subscription's own entry now that its forwarding loop has
      // ended. This does NOT cover a same-webview reload: `on_event.send()`
      // resolves to `Channel::send`, which ends in `webview.eval(...)`, and in
      // a default build (`tracing` isn't a default Tauri feature) that's
      // `tauri-runtime-wry`'s `send_user_message` variant - it returns `Ok` as
      // soon as the message is queued to the app-global event-loop proxy, and
      // the handler is a no-op if the webview it's addressed to is gone. So
      // The loop above only breaks when the upstream broker/stream ends or the
      // event loop tears down: `send()` returns `Ok` for a reloaded webview
      // (still alive, its JS callback just isn't listening) and for a destroyed
      // one alike, so neither triggers this. Reaping here stops a finished
      // subscription from leaving an entry that counts against
      // MAX_SUBSCRIPTIONS_PER_DATABASE forever, which would eventually fail
      // every new subscribe() with TOO_MANY_SUBSCRIPTIONS while nothing is
      // receiving events. Safe to race with unsubscribe() - `remove()` and
      // aborting an already-finished `AbortHandle` are both no-ops.
      active_subs_for_task.remove(&sub_id).await;
   });

   // Track subscription, then release the task to start forwarding/reaping.
   active_subs
      .insert(subscription_id.clone(), db_key, handle.abort_handle())
      .await;
   let _ = ready_tx.send(());

   Ok(subscription_id)
}

/// Unsubscribe from change notifications.
///
/// Returns `true` if the subscription was found and removed.
#[tauri::command]
pub async fn unsubscribe(
   active_subs: State<'_, ActiveSubscriptions>,
   subscription_id: String,
) -> Result<bool> {
   Ok(active_subs.remove(&subscription_id).await)
}

/// Release this window's observation registration for a database.
///
/// Observation is reference-counted per webview (see `observe()` docs and issue
/// #54): if other windows are still observing this database, this call only
/// removes this window's own registration and returns - the broker and every
/// other window's subscriptions are left untouched. Only when the *last*
/// registered window calls `unobserve()` are changes actually stopped, tracking
/// disabled, and all subscriptions for this database aborted.
///
/// Calling this from a window that never called `observe()` for `db_key` is a
/// no-op (beyond validating that `db_key` itself is loaded) - it does not tear
/// down observation that other windows are legitimately still using.
#[tauri::command]
pub async fn unobserve<R: Runtime>(
   db_instances: State<'_, DbInstances>,
   active_subs: State<'_, ActiveSubscriptions>,
   observer_regs: State<'_, ObserverRegistrations>,
   webview: tauri::Webview<R>,
   db_key: String,
) -> Result<()> {
   // Lock order matches observe(): db_instances write lock first, then
   // observer_regs, held across the entire release+disable sequence.
   // Acquiring the db lock unconditionally, before knowing the refcount,
   // also fixes an inconsistency a caller could otherwise observe: this used
   // to only validate `db_key` is loaded on the "last observer" path, so an
   // unloaded/unregistered `db_key` combined with a non-zero remaining count
   // would silently succeed instead of erroring like every other command does.
   let mut instances = db_instances.write().await;

   if !instances.contains_key(&db_key) {
      return Err(Error::DatabaseNotLoaded(db_key.clone()));
   }

   // `release()` takes `&mut instances` as a witness that we're still holding
   // the db lock (see `ObserverRegistrations`'s lock-order doc) - it doesn't
   // read through it, so this doesn't conflict with re-borrowing `instances`
   // below to fetch the wrapper. We can't hold that wrapper borrow across this
   // call instead, because the borrow checker won't allow a live `&mut
   // DatabaseWrapper` (from `instances.get_mut()`) at the same time as this
   // `&mut instances` - which is exactly the kind of thing the witness
   // parameter is meant to force into the open rather than paper over.
   let remaining = match observer_regs
      .release(&mut instances, &db_key, webview.label())
      .await
   {
      Some(remaining) => remaining,
      None => {
         // This webview was never registered as an observer of db_key -
         // nothing to release, and nothing to tear down.
         return Ok(());
      }
   };

   if remaining > 0 {
      debug!(
         "unobserve: {} observer(s) remain for database {}, leaving broker active",
         remaining, db_key
      );
      return Ok(());
   }

   // Last observer released - fully tear down: abort subscriptions and disable
   // the crate-level observer/broker. This `get_mut()` is unreachable-by
   // -construction: `instances` was never dropped since the `contains_key`
   // check above, so nothing could have removed the entry in between. The `?`
   // is still here rather than `.expect()`/`.unwrap()` purely as a defensive
   // fallback in case a future refactor breaks that invariant - it should
   // never actually fire.
   active_subs.remove_for_db(&db_key).await;
   let wrapper = instances
      .get_mut(&db_key)
      .ok_or_else(|| Error::DatabaseNotLoaded(db_key.clone()))?;
   wrapper.disable_observation();
   Ok(())
}
