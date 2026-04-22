use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use serde::Serialize;
use sqlx_sqlite_conn_mgr::Migrator;
use tauri::{Emitter, Manager, RunEvent, Runtime, plugin::Builder as PluginBuilder};
use tokio::sync::{Notify, RwLock};
use tracing::{debug, error, info, trace, warn};

mod commands;
mod error;
mod resolve;
mod subscriptions;

pub use error::{Error, Result};
pub use sqlx_sqlite_conn_mgr::{
   AttachedMode, AttachedSpec, Migrator as SqliteMigrator, SqliteDatabaseConfig,
};
pub use sqlx_sqlite_toolkit::{
   ActiveInterruptibleTransactions, ActiveRegularTransactions, DatabaseWrapper,
   InterruptibleTransaction, InterruptibleTransactionBuilder, Statement,
   TransactionExecutionBuilder, WriteQueryResult,
};

/// Default maximum number of concurrently loaded databases.
const DEFAULT_MAX_DATABASES: usize = 50;

/// Tracks cleanup progress during app exit: 0 = not started, 1 = running, 2 = complete.
static CLEANUP_STATE: AtomicU8 = AtomicU8::new(0);

/// Guarantees `CLEANUP_STATE` reaches `2` and `app_handle.exit(..)` fires even if the
/// cleanup task panics. Without this, a panic would leave the state at `1` and subsequent
/// user exit attempts would call `prevent_exit()` indefinitely.
///
/// The exit code carried through is whatever the triggering `ExitRequested` carried —
/// `None` (user-initiated close) becomes `0`, `Some(n)` (programmatic
/// `app_handle.exit(n)`) is preserved so application-level exit codes survive the
/// cleanup detour.
struct ExitGuard<R: Runtime> {
   app_handle: tauri::AppHandle<R>,
   exit_code: i32,
}

impl<R: Runtime> Drop for ExitGuard<R> {
   fn drop(&mut self) {
      CLEANUP_STATE.store(2, Ordering::SeqCst);
      self.app_handle.exit(self.exit_code);
   }
}

/// Database instances managed by the plugin.
///
/// This struct maintains a thread-safe map of database paths to their corresponding
/// connection wrappers, with a configurable upper limit on how many databases can be
/// loaded simultaneously.
#[derive(Clone)]
pub struct DbInstances {
   pub(crate) inner: Arc<RwLock<HashMap<String, DatabaseWrapper>>>,
   pub(crate) max: usize,
}

impl Default for DbInstances {
   fn default() -> Self {
      Self {
         inner: Arc::new(RwLock::new(HashMap::new())),
         max: DEFAULT_MAX_DATABASES,
      }
   }
}

impl DbInstances {
   /// Create a new instance with the given maximum database count.
   pub fn new(max: usize) -> Self {
      Self {
         inner: Arc::new(RwLock::new(HashMap::new())),
         max,
      }
   }
}

/// Migration status for a database.
#[derive(Debug, Clone)]
pub enum MigrationStatus {
   /// Migrations are pending (not yet started)
   Pending,
   /// Migrations are currently running
   Running,
   /// Migrations completed successfully
   Complete,
   /// Migrations failed with an error
   Failed(String),
}

/// Tracks migration state for a single database with notification support.
pub struct MigrationState {
   pub(crate) status: MigrationStatus,
   pub(crate) notify: Arc<Notify>,
   pub(crate) events: Vec<MigrationEvent>,
}

impl MigrationState {
   fn new() -> Self {
      Self {
         status: MigrationStatus::Pending,
         notify: Arc::new(Notify::new()),
         events: Vec::new(),
      }
   }

   fn update_status(&mut self, status: MigrationStatus) {
      self.status = status;
      self.notify.notify_waiters();
   }

   fn cache_event(&mut self, event: MigrationEvent) {
      self.events.push(event);
   }
}

/// Tracks migration state for all databases.
#[derive(Default)]
pub struct MigrationStates(pub RwLock<HashMap<String, MigrationState>>);

/// Event payload emitted during migration operations.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrationEvent {
   /// Database path (relative, as registered)
   pub db_path: String,
   /// Status: "running", "completed", "failed"
   pub status: String,
   /// Total number of migrations defined in the migrator (on "completed"), not just newly applied
   #[serde(skip_serializing_if = "Option::is_none")]
   pub migration_count: Option<usize>,
   /// Error message (on "failed")
   #[serde(skip_serializing_if = "Option::is_none")]
   pub error: Option<String>,
}

/// Builder for the SQLite plugin.
///
/// Use this to configure the plugin and build the plugin instance.
///
/// # Example
///
/// ```ignore
/// // Note: This example uses `ignore` instead of `no_run` because
/// // tauri::generate_context!() requires tauri.conf.json at compile time,
/// // which cannot be provided in doc test environments.
/// use tauri_plugin_sqlite::Builder;
///
/// # fn main() {
/// // Basic setup (no migrations):
/// tauri::Builder::default()
///     .plugin(Builder::new().build())
///     .run(tauri::generate_context!())
///     .expect("error while running tauri application");
/// # }
/// ```
///
/// # Example with migrations
///
/// ```ignore
/// // Note: This example uses `ignore` instead of `no_run` because
/// // tauri::generate_context!() requires tauri.conf.json at compile time,
/// // which cannot be provided in doc test environments.
/// use tauri_plugin_sqlite::Builder;
///
/// # fn main() {
/// // Setup with migrations:
/// tauri::Builder::default()
///     .plugin(
///         Builder::new()
///             .add_migrations("main.db", sqlx::migrate!("./migrations/main"))
///             .add_migrations("cache.db", sqlx::migrate!("./migrations/cache"))
///             .build()
///     )
///     .run(tauri::generate_context!())
///     .expect("error while running tauri application");
/// # }
/// ```
#[derive(Debug, Default)]
pub struct Builder {
   /// Migrations registered per database path
   migrations: HashMap<String, Arc<Migrator>>,
   /// Timeout for interruptible transactions. Defaults to 5 minutes.
   transaction_timeout: Option<std::time::Duration>,
   /// Maximum number of concurrently loaded databases. Defaults to 50.
   max_databases: Option<usize>,
}

impl Builder {
   /// Create a new builder instance.
   pub fn new() -> Self {
      Self {
         migrations: HashMap::new(),
         transaction_timeout: None,
         max_databases: None,
      }
   }

   /// Register migrations for a database path.
   ///
   /// Migrations will be run automatically at plugin initialization.
   /// Multiple databases can have their own migrations.
   ///
   /// # Arguments
   ///
   /// * `path` - Database path (relative to app config directory)
   /// * `migrator` - Migrator instance, typically from `sqlx::migrate!()`
   ///
   /// # Example
   ///
   /// ```no_run
   /// use tauri_plugin_sqlite::Builder;
   ///
   /// # fn example() {
   /// Builder::new()
   ///     .add_migrations("main.db", sqlx::migrate!("./doc-test-fixtures/migrations"))
   ///     .build::<tauri::Wry>();
   /// # }
   /// ```
   pub fn add_migrations(mut self, path: &str, migrator: Migrator) -> Self {
      self.migrations.insert(path.to_string(), Arc::new(migrator));
      self
   }

   /// Set the timeout for interruptible transactions.
   ///
   /// If an interruptible transaction exceeds this duration, it will be automatically
   /// rolled back on the next access attempt. Defaults to 5 minutes.
   ///
   /// Returns `Err(Error::InvalidConfig)` if `timeout` is zero.
   pub fn transaction_timeout(mut self, timeout: std::time::Duration) -> Result<Self> {
      if timeout.is_zero() {
         return Err(Error::InvalidConfig(
            "transaction_timeout must be greater than zero".to_string(),
         ));
      }
      self.transaction_timeout = Some(timeout);
      Ok(self)
   }

   /// Set the maximum number of databases that can be loaded simultaneously.
   ///
   /// Prevents unbounded memory growth from connection pool proliferation.
   /// Defaults to 50.
   ///
   /// Returns `Err(Error::InvalidConfig)` if `max` is zero.
   pub fn max_databases(mut self, max: usize) -> Result<Self> {
      if max == 0 {
         return Err(Error::InvalidConfig(
            "max_databases must be greater than zero".to_string(),
         ));
      }
      self.max_databases = Some(max);
      Ok(self)
   }

   /// Build the plugin with command registration and state management.
   pub fn build<R: Runtime>(self) -> tauri::plugin::TauriPlugin<R> {
      let migrations = Arc::new(self.migrations);
      let transaction_timeout = self.transaction_timeout;
      let max_databases = self.max_databases;

      PluginBuilder::<R>::new("sqlite")
         .invoke_handler(tauri::generate_handler![
            commands::load,
            commands::execute,
            commands::execute_transaction,
            commands::begin_interruptible_transaction,
            commands::transaction_continue,
            commands::transaction_read,
            commands::fetch_all,
            commands::fetch_one,
            commands::fetch_page,
            commands::close,
            commands::close_all,
            commands::remove,
            commands::get_migration_events,
            commands::observe,
            commands::subscribe,
            commands::unsubscribe,
            commands::unobserve,
         ])
         .setup(move |app, _api| {
            app.manage(match max_databases {
               Some(max) => DbInstances::new(max),
               None => DbInstances::default(),
            });
            app.manage(MigrationStates::default());
            app.manage(match transaction_timeout {
               Some(timeout) => ActiveInterruptibleTransactions::new(timeout),
               None => ActiveInterruptibleTransactions::default(),
            });
            app.manage(ActiveRegularTransactions::default());
            app.manage(subscriptions::ActiveSubscriptions::default());

            // Initialize migration states as Pending for all registered databases
            let migration_states = app.state::<MigrationStates>();
            {
               let mut states = migration_states.0.blocking_write();
               for path in migrations.keys() {
                  states.insert(path.clone(), MigrationState::new());
               }
            }

            // Spawn parallel migration tasks for each registered database
            if !migrations.is_empty() {
               info!("Starting migrations for {} database(s)", migrations.len());

               for (path, migrator) in migrations.iter() {
                  let app_handle = app.clone();
                  let path = path.clone();
                  let migrator = Arc::clone(migrator);

                  tauri::async_runtime::spawn(async move {
                     run_migrations_for_database(app_handle, path, migrator).await;
                  });
               }
            }

            debug!("SQLite plugin initialized");
            Ok(())
         })
         .on_event(|app, event| {
            match event {
               RunEvent::ExitRequested { api, code, .. } => {
                  // Claim cleanup ownership once. Three possible CLEANUP_STATE values:
                  //   0 → claim it, run cleanup
                  //   1 → cleanup already in progress (another invocation won the
                  //       race). Keep exit prevented while it finishes.
                  //   2 → cleanup already complete; this ExitRequested is the
                  //       re-exit fired by ExitGuard. Let it through unchanged.
                  //
                  // We deliberately do not skip programmatic exits (code.is_some()).
                  // A user-space app_handle.exit(N) — fatal-error handler, updater,
                  // Ctrl+C handler — would otherwise tear down plugin state with
                  // interruptible transactions still live in the map, and the
                  // captured-runtime Drop path on the toolkit side still relies on
                  // the runtime being up when it spawns the rollback. Running
                  // cleanup here is the clean path.
                  match CLEANUP_STATE.compare_exchange(
                     0,
                     1,
                     Ordering::SeqCst,
                     Ordering::SeqCst,
                  ) {
                     Ok(_) => {}
                     Err(2) => return,
                     Err(_) => {
                        api.prevent_exit();
                        debug!("Exit requested while database cleanup is in progress");
                        return;
                     }
                  }

                  let exit_code = code.unwrap_or(0);
                  info!(
                     "App exit requested (code={}) - cleaning up transactions and databases",
                     exit_code
                  );

                  // Prevent immediate exit so we can close connections and checkpoint WAL
                  api.prevent_exit();

                  let app_handle = app.clone();

                  let instances_clone = app.state::<DbInstances>().inner().clone();
                  let interruptible_txs_clone = app.state::<ActiveInterruptibleTransactions>().inner().clone();
                  let regular_txs_clone = app.state::<ActiveRegularTransactions>().inner().clone();
                  let active_subs_clone = app.state::<subscriptions::ActiveSubscriptions>().inner().clone();

                  // Run cleanup on the async runtime (without blocking the event loop),
                  // then trigger a programmatic exit when done. ExitGuard ensures
                  // CLEANUP_STATE reaches 2 and exit() fires even on panic.
                  tauri::async_runtime::spawn(async move {
                     let _guard = ExitGuard { app_handle, exit_code };

                     // Scope block: drops the RwLock write guard (from instances_clone)
                     // before _guard fires exit(), whose RunEvent::Exit handler calls
                     // try_read() on the same lock.
                     {
                        let timeout_result = tokio::time::timeout(
                           std::time::Duration::from_secs(5),
                           async {
                              // First, abort all subscriptions and transactions
                              debug!("Aborting active subscriptions and transactions");
                              active_subs_clone.abort_all().await;
                              sqlx_sqlite_toolkit::cleanup_all_transactions(&interruptible_txs_clone, &regular_txs_clone).await;

                              // Close databases (each wrapper's close() disables its own
                              // observer at the crate level, unregistering SQLite hooks)
                              let mut guard = instances_clone.inner.write().await;
                              let wrappers: Vec<DatabaseWrapper> =
                                 guard.drain().map(|(_, v)| v).collect();

                              // Close databases in parallel
                              let mut set = tokio::task::JoinSet::new();
                              for wrapper in wrappers {
                                 set.spawn(async move { wrapper.close().await });
                              }

                              while let Some(result) = set.join_next().await {
                                 match result {
                                    Ok(Err(e)) => warn!("Error closing database: {:?}", e),
                                    Err(e) => warn!("Database close task panicked: {:?}", e),
                                    Ok(Ok(())) => {}
                                 }
                              }
                           },
                        )
                        .await;

                        if timeout_result.is_err() {
                           warn!("Database cleanup timed out after 5 seconds");
                        } else {
                           debug!("Database cleanup complete");
                        }
                     }
                  });
               }
               RunEvent::Exit => {
                  // ExitRequested should have already closed all databases
                  // This is just a safety check
                  let instances = app.state::<DbInstances>();
                  match instances.inner.try_read() {
                     Ok(guard) => {
                        if !guard.is_empty() {
                           warn!(
                              "Exit event fired with {} database(s) still open - cleanup may have been skipped",
                              guard.len()
                           );
                        } else {
                           debug!("Exit event: all databases already closed");
                        }
                     }
                     Err(_) => {
                        warn!("Exit event: could not check database state (lock held - cleanup may still be in progress)");
                     }
                  }
               }
               _ => {
                  // Other events don't require action
               }
            }
         })
         .build()
   }
}

/// Initializes the plugin with default configuration.
pub fn init<R: Runtime>() -> tauri::plugin::TauriPlugin<R> {
   Builder::new().build()
}

/// Run migrations for a single database and emit events.
///
/// This function is spawned as a task for each database with registered migrations.
/// It runs during plugin setup, before the frontend calls `load`.
///
/// # Timing & Caching
///
/// 1. Plugin setup spawns this task (async, non-blocking)
/// 2. This task connects via `SqliteDatabase::connect()`, which caches the instance
/// 3. When frontend later calls `load`, it awaits migration completion first
/// 4. Then `load` calls `connect()` again, which returns the **same cached instance**
///
/// The `DatabaseWrapper` created here is temporary and dropped after migrations complete,
/// but the underlying `SqliteDatabase` (with its connection pools) remains cached in the
/// global registry and is reused when `load` creates its own wrapper.
async fn run_migrations_for_database<R: Runtime>(
   app: tauri::AppHandle<R>,
   path: String,
   migrator: Arc<Migrator>,
) {
   let migration_states = app.state::<MigrationStates>();

   // Update state to Running
   {
      let mut states = migration_states.0.write().await;
      if let Some(state) = states.get_mut(&path) {
         state.update_status(MigrationStatus::Running);
      }
   }

   // Emit running event
   emit_migration_event(&app, &path, "running", None, None);

   // Resolve absolute path and connect
   let abs_path = match resolve_migration_path(&path, &app) {
      Ok(p) => p,
      Err(e) => {
         let error_msg = e.to_string();
         error!(
            "Failed to resolve migration path for {}: {}",
            path, error_msg
         );

         let mut states = migration_states.0.write().await;
         if let Some(state) = states.get_mut(&path) {
            state.update_status(MigrationStatus::Failed(error_msg.clone()));
         }

         emit_migration_event(&app, &path, "failed", None, Some(error_msg));
         return;
      }
   };

   // Connect to database
   let db = match DatabaseWrapper::connect(&abs_path, None).await {
      Ok(wrapper) => wrapper,
      Err(e) => {
         let error_msg = e.to_string();
         error!("Failed to connect for migrations {}: {}", path, error_msg);

         let mut states = migration_states.0.write().await;
         if let Some(state) = states.get_mut(&path) {
            state.update_status(MigrationStatus::Failed(error_msg.clone()));
         }

         emit_migration_event(&app, &path, "failed", None, Some(error_msg));
         return;
      }
   };

   // Run migrations
   // Note: SQLx's migrator.run() doesn't provide per-migration callbacks,
   // so we can only report start and finish. For detailed per-migration events,
   // we would need to iterate migrations manually.
   trace!("Running migrations for {}", path);

   match db.run_migrations(&migrator).await {
      Ok(()) => {
         info!("Migrations completed successfully for {}", path);

         let mut states = migration_states.0.write().await;
         if let Some(state) = states.get_mut(&path) {
            state.update_status(MigrationStatus::Complete);
         }

         let migration_count = migrator.iter().count();
         emit_migration_event(&app, &path, "completed", Some(migration_count), None);
      }
      Err(e) => {
         let error_msg = e.to_string();
         error!("Migration failed for {}: {}", path, error_msg);

         let mut states = migration_states.0.write().await;
         if let Some(state) = states.get_mut(&path) {
            state.update_status(MigrationStatus::Failed(error_msg.clone()));
         }

         emit_migration_event(&app, &path, "failed", None, Some(error_msg));
      }
   }
}

/// Emit a migration event to the frontend and cache it.
fn emit_migration_event<R: Runtime>(
   app: &tauri::AppHandle<R>,
   db_path: &str,
   status: &str,
   migration_count: Option<usize>,
   error: Option<String>,
) {
   let event = MigrationEvent {
      db_path: db_path.to_string(),
      status: status.to_string(),
      migration_count,
      error,
   };

   // Cache event in migration state
   let migration_states = app.state::<MigrationStates>();
   if let Ok(mut states) = migration_states.0.try_write()
      && let Some(state) = states.get_mut(db_path)
   {
      state.cache_event(event.clone());
   }

   if let Err(e) = app.emit("sqlite:migration", &event) {
      warn!("Failed to emit migration event: {}", e);
   }
}

/// Resolve database path for migrations.
///
/// Delegates to `resolve::resolve_database_path` to ensure consistent path validation
/// across all entry points.
fn resolve_migration_path<R: Runtime>(
   path: &str,
   app: &tauri::AppHandle<R>,
) -> Result<std::path::PathBuf> {
   crate::resolve::resolve_database_path(path, app)
}

#[cfg(test)]
mod tests {
   use super::*;

   #[test]
   fn test_max_databases_rejects_zero() {
      let err = Builder::new().max_databases(0).unwrap_err();
      assert!(matches!(err, Error::InvalidConfig(_)));
   }

   #[test]
   fn test_max_databases_accepts_positive() {
      let builder = Builder::new().max_databases(1).unwrap();
      assert_eq!(builder.max_databases, Some(1));
   }

   #[test]
   fn test_transaction_timeout_rejects_zero() {
      let err = Builder::new()
         .transaction_timeout(std::time::Duration::ZERO)
         .unwrap_err();
      assert!(matches!(err, Error::InvalidConfig(_)));
   }

   #[test]
   fn test_transaction_timeout_accepts_positive() {
      let builder = Builder::new()
         .transaction_timeout(std::time::Duration::from_secs(1))
         .unwrap();
      assert_eq!(
         builder.transaction_timeout,
         Some(std::time::Duration::from_secs(1))
      );
   }
}
