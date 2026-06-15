use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use serde::Serialize;
use sqlx_sqlite_conn_mgr::Migrator;
use tauri::{AppHandle, Emitter, Manager, RunEvent, Runtime, plugin::Builder as PluginBuilder};
use tokio::sync::{Notify, RwLock};
use tracing::{debug, error, info, trace, warn};

mod commands;
mod error;
mod subscriptions;
mod validate;

pub use error::{Error, Result};
pub use sqlx_sqlite_conn_mgr::{
   AttachedMode, AttachedSpec, Migrator as SqliteMigrator, SqliteDatabaseConfig,
};
pub use sqlx_sqlite_toolkit::{
   ActiveInterruptibleTransactions, ActiveRegularTransactions, DatabaseWrapper,
   InterruptibleTransaction, InterruptibleTransactionBuilder, Statement,
   TransactionExecutionBuilder, WriteQueryResult,
};

use crate::subscriptions::ActiveSubscriptions;

/// Default maximum number of concurrently loaded databases.
const DEFAULT_MAX_DATABASES: usize = 50;

/// Upper bound on how long close/cleanup may run before returning a timeout error.
const CLOSE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

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
///
/// The string key is the registered database key.
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

/// Tracks the paths of all registered databases.
/// The String value of the key is the database identifier, not the path.
/// For example, the value of the key `MAIN` would be something like
/// `/var/lib/myapp/main.db`.
///
/// This key value is what will be used by the caller to interact with the database.
/// For example, when calling `load()` or `execute()`, the caller will pass the key value
/// to identify the database to which they want to connect.
#[derive(Clone, Default)]
pub struct RegisteredDatabases {
   pub(crate) database_path_by_key: Arc<HashMap<String, PathBuf>>,
}

/// Contains the information required for registering a database.
///
/// When initializing or setting up the plugin, the caller will pass the path to the database
/// file and the migrator to use for the database.
///
/// This information is then stored in the `RegisteredDatabases` struct, which is used to
/// track the paths of all registered databases.
///
/// The `migrator` is not held by the app state, but rather is only used after
/// initialization to run the migrations for the database.
#[derive(Debug, Clone)]
struct DatabaseInfo {
   path: PathBuf,
   migrator: Option<Arc<Migrator>>,
}

fn validated_database_info(
   path: impl Into<PathBuf>,
   migrator: Option<Migrator>,
) -> Result<DatabaseInfo> {
   let path = path.into();
   Ok(DatabaseInfo {
      path: validate::validate_database_path(&path)?,
      migrator: migrator.map(Arc::new),
   })
}

/// Ensure each registration key maps to a distinct database path.
fn ensure_distinct_database_paths(
   database_info_by_key: &HashMap<String, DatabaseInfo>,
) -> Result<()> {
   let mut path_to_key = HashMap::new();

   for (key, info) in database_info_by_key {
      if let Some(existing_key) = path_to_key.insert(info.path.clone(), key.as_str()) {
         return Err(Error::InvalidConfig(format!(
            "database keys {existing_key} and {key} both register the same path: {}",
            info.path.display()
         )));
      }
   }

   Ok(())
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
/// The String value of the key is the database identifier, not the path.
#[derive(Default)]
pub struct MigrationStates(pub RwLock<HashMap<String, MigrationState>>);

/// Event payload emitted during migration operations.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrationEvent {
   /// Database key, meant to be human readable (such as `MAIN`).
   /// This is what is to be used by the client to interact with the database.
   pub db_key: String,
   /// Database path, the absolute path to the database file, such as
   /// `/var/lib/myapp/main.db`.
   pub db_path: PathBuf,
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
/// # Database registration
///
/// Every database must be **registered** with a stable key and filesystem path (or
/// in-memory URI) before it can be opened. The frontend and Rust callers open databases
/// by **key** via `load()` / [`Connection::connect`]. Paths are validated and
/// canonicalized at registration time.
///
/// Because legitimate paths usually depend on runtime values (for example
/// `app.path().app_data_dir()`), registration normally happens in the [`Builder::on_setup`]
/// hook. Static paths can be registered up front with [`Builder::register_database`].
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
/// // Basic setup (no databases registered yet — register them in `on_setup`):
/// tauri::Builder::default()
///     .plugin(Builder::new().build().expect("failed to build sqlite plugin"))
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
/// use tauri::Manager;
///
/// # fn main() {
/// // Resolve the database path from the app instance and register it with migrations.
/// // The frontend then calls `Database.load("MAIN")` with the registration key.
/// tauri::Builder::default()
///     .plugin(
///         Builder::new()
///             .on_setup(|app, reg| {
///                 let db = app.path().app_data_dir()?.join("main.db");
///                 reg.register_database(
///                     "MAIN",
///                     db,
///                     Some(sqlx::migrate!("./migrations/main")),
///                 )?;
///                 Ok(())
///             })
///             .build()
///             .expect("failed to build sqlite plugin")
///     )
///     .run(tauri::generate_context!())
///     .expect("error while running tauri application");
/// # }
/// ```
///
/// Collects database registrations from the [`Builder::on_setup`] hook.
///
/// Passed to the `on_setup` closure during plugin setup, where the `app` instance is
/// available. Use it to register values that can only be computed at runtime (for example,
/// paths derived from `app.path().app_data_dir()`).
#[derive(Default)]
pub struct SetupRegistrar {
   database_info_by_key: HashMap<String, DatabaseInfo>,
}

impl SetupRegistrar {
   /// Register a database path, optionally with migrations. See [`Builder::register_database`].
   ///
   /// This invocation is to be used when the database path is known at runtime (such as
   /// a path dependent on `app.path().app_data_dir()`).
   ///
   /// For a path that is known at compile time, use [`Builder::register_database`]
   /// instead.
   ///
   /// The `key` is the identifier for the database. It is used to identify the database
   /// when calling `load()` or `execute()`.
   ///
   /// The `path` is the absolute filesystem path or in-memory URI. It is validated and
   /// canonicalized at registration time.
   ///
   /// Returns `Err` if the path fails validation (relative, traversal, or canonicalization).
   ///
   /// The `migrator` runs automatically at plugin initialization when provided.
   ///
   /// If the same key is registered more than once, the last registration will override
   /// all previous ones.
   ///
   /// Distinct keys must map to distinct database paths. Duplicate paths are rejected
   /// when the plugin initializes (see [`Builder::build`]).
   pub fn register_database(
      &mut self,
      key: &str,
      path: impl Into<PathBuf>,
      migrator: Option<Migrator>,
   ) -> Result<()> {
      self
         .database_info_by_key
         .insert(key.to_string(), validated_database_info(path, migrator)?);
      Ok(())
   }
}

/// Closure type for the deferred [`Builder::on_setup`] hook.
type OnSetupHook<R> = Box<dyn FnOnce(&AppHandle<R>, &mut SetupRegistrar) -> Result<()> + Send>;

pub struct Builder<R: Runtime> {
   /// Migrations registered per database path, keyed by the database key.
   database_info_by_key: HashMap<String, DatabaseInfo>,
   /// Timeout for interruptible transactions. Defaults to 5 minutes.
   transaction_timeout: Option<std::time::Duration>,
   /// Maximum number of concurrently loaded databases. Defaults to 50.
   max_databases: Option<usize>,
   /// Deferred hook run during plugin setup with the app handle. Lets callers register
   /// paths/migrations computed from `app`. Returning `Err` aborts app startup.
   on_setup: Option<OnSetupHook<R>>,
}

impl<R: Runtime> Default for Builder<R> {
   fn default() -> Self {
      Self::new()
   }
}

impl<R: Runtime> std::fmt::Debug for Builder<R> {
   fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
      f.debug_struct("Builder")
         .field("database_info_by_key", &self.database_info_by_key)
         .field("transaction_timeout", &self.transaction_timeout)
         .field("max_databases", &self.max_databases)
         .field("on_setup", &self.on_setup.is_some())
         .finish()
   }
}

impl<R: Runtime> Builder<R> {
   /// Create a new builder instance.
   pub fn new() -> Self {
      Self {
         database_info_by_key: HashMap::new(),
         transaction_timeout: None,
         max_databases: None,
         on_setup: None,
      }
   }

   /// Register a database by key and path, optionally with migrations.
   ///
   /// Pass `None` for `migrator` when the database has no migrations. Migrations run
   /// automatically at plugin initialization when provided.
   ///
   /// Use this when the path is known at compile time. For paths derived from the `app`
   /// instance (for example `app.path().app_data_dir()`), use [`on_setup`](Self::on_setup)
   /// and [`SetupRegistrar::register_database`] instead.
   ///
   /// The frontend must call `load()` with the registration **key**.
   ///
   /// # Example
   ///
   /// ```no_run
   /// use tauri_plugin_sqlite::Builder;
   /// use std::path::PathBuf;
   ///
   /// const MAIN_DB_KEY: &str = "MAIN";
   ///
   /// # fn example() -> tauri_plugin_sqlite::Result<()> {
   /// Builder::<tauri::Wry>::new()
   ///     .register_database(
   ///         MAIN_DB_KEY,
   ///         PathBuf::from("/var/lib/myapp/main.db"),
   ///         Some(sqlx::migrate!("./doc-test-fixtures/migrations")),
   ///     )?
   ///     .build()?;
   /// # Ok(())
   /// # }
   /// ```
   ///
   /// If the same key is registered more than once, the last registration will override
   /// all previous ones.
   ///
   /// Distinct keys must map to distinct database paths. If two distinct keys register
   /// the same path, plugin initialization returns [`Error::InvalidConfig`]. Registrations
   /// from [`on_setup`](Self::on_setup) are validated when the merged map is initialized.
   pub fn register_database(
      mut self,
      key: &str,
      path: impl Into<PathBuf>,
      migrator: Option<Migrator>,
   ) -> Result<Self> {
      self
         .database_info_by_key
         .insert(key.to_string(), validated_database_info(path, migrator)?);

      Ok(self)
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

   /// Register a hook that runs during plugin setup, once the `app` instance exists.
   ///
   /// This is the primary way to register database paths, because the legitimate absolute
   /// paths usually depend on runtime values — for example paths derived from
   /// `app.path().app_data_dir()`. The closure receives the app handle and a
   /// [`SetupRegistrar`] on which you call [`register_database`](SetupRegistrar::register_database).
   ///
   /// Entries registered here are merged with those registered statically via
   /// [`register_database`](Self::register_database); a later registration for the same
   /// key overrides an earlier one.
   ///
   /// Returning `Err` from the hook aborts app startup (fail-fast).
   ///
   /// # Example
   ///
   /// ```no_run
   /// use tauri_plugin_sqlite::Builder;
   /// use tauri::Manager;
   ///
   /// const MAIN_DB_KEY: &str = "MAIN";
   ///
   /// # fn example() -> tauri_plugin_sqlite::Result<()> {
   /// Builder::<tauri::Wry>::new()
   ///     .on_setup(|app, reg| {
   ///         let dir = app.path().app_data_dir().map_err(|e| tauri_plugin_sqlite::Error::InvalidConfig(e.to_string()))?;
   ///         let db = dir.join("main.db");
   ///         reg.register_database(
   ///            MAIN_DB_KEY,
   ///            db,
   ///            Some(sqlx::migrate!("./doc-test-fixtures/migrations"))
   ///         )?;
   ///         Ok(())
   ///     })
   ///     .build()?;
   /// # Ok(())
   /// # }
   /// ```
   pub fn on_setup(
      mut self,
      f: impl FnOnce(&AppHandle<R>, &mut SetupRegistrar) -> Result<()> + Send + 'static,
   ) -> Self {
      self.on_setup = Some(Box::new(f));
      self
   }

   /// Build the plugin with command registration and state management.
   ///
   /// Duplicate paths across distinct registration keys are rejected during plugin
   /// initialization (the setup hook), after [`on_setup`](Self::on_setup) registrations
   /// are merged with static ones.
   pub fn build(self) -> Result<tauri::plugin::TauriPlugin<R>> {
      let database_info_by_key = self.database_info_by_key;
      let transaction_timeout = self.transaction_timeout;
      let max_databases = self.max_databases;
      let on_setup = self.on_setup;

      Ok(PluginBuilder::<R>::new("sqlite")
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

            // Run the deferred setup hook (if any), merge with static registrations.
            // Paths are validated and canonicalized at registration time. Hook errors
            // abort startup (fail-fast).
            let mut database_info_by_key = database_info_by_key;
            if let Some(on_setup_action) = on_setup {
               let mut registrar = SetupRegistrar::default();
               on_setup_action(app, &mut registrar)?;
               database_info_by_key.extend(registrar.database_info_by_key);
            }

            ensure_distinct_database_paths(&database_info_by_key)?;

            app.manage(RegisteredDatabases {
               database_path_by_key: Arc::new(database_info_by_key.iter().map(|(key, info)| (key.clone(), info.path.clone())).collect()),
            });

            let migration_states = app.state::<MigrationStates>();
            {
               let mut states = migration_states.0.blocking_write();
               // Only track migration state for databases that have a migrator.
               // Keys without migrations are omitted so `await_migrations` returns
               // immediately instead of waiting on a Pending state that never runs.
               for (key, info) in &database_info_by_key {
                  if info.migrator.is_some() {
                     states.insert(key.clone(), MigrationState::new());
                  }
               }
            }

            for (key, info) in &database_info_by_key {
               if let Some(migrator) = &info.migrator {
                  info!("Starting migrations for database {}", key);

                  let key = key.clone();
                  let migrator = migrator.clone();
                  let path = info.path.clone();
                  let app_handle = app.clone();
                  tauri::async_runtime::spawn(async move {
                     run_migrations_for_database(app_handle, &key, &path, &migrator).await;
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
                           CLOSE_TIMEOUT,
                           async {
                              debug!("Aborting active subscriptions and transactions");
                              active_subs_clone.abort_all().await;
                              if let Err(e) = sqlx_sqlite_toolkit::cleanup_all_transactions(
                                 &interruptible_txs_clone,
                                 &regular_txs_clone,
                              )
                              .await
                              {
                                 warn!("Transaction cleanup failed during exit: {e}");
                              }

                              if let Err(e) =
                                 close_all_wrappers(&instances_clone).await
                              {
                                 warn!("Error closing databases during exit: {e:?}");
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
         .build())
   }
}

/// Initializes the plugin with default configuration.
pub fn init<R: Runtime>() -> tauri::plugin::TauriPlugin<R> {
   Builder::<R>::new()
      .build()
      .expect("failed to build sqlite plugin")
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
   key: &str,
   path: &Path,
   migrator: &Arc<Migrator>,
) {
   let migration_states = app.state::<MigrationStates>();

   // Update state to Running
   {
      let mut states = migration_states.0.write().await;
      if let Some(state) = states.get_mut(key) {
         state.update_status(MigrationStatus::Running);
      }
   }

   // Emit running event
   emit_migration_event(&app, key, path, "running", None, None);

   // Resolve absolute path and connect
   let abs_path = match resolve_database_path(key, &app) {
      Ok(p) => p,
      Err(e) => {
         let error_msg = e.to_string();
         error!(
            "Failed to resolve migration path for {}: {}",
            key, error_msg
         );

         let mut states = migration_states.0.write().await;
         if let Some(state) = states.get_mut(key) {
            state.update_status(MigrationStatus::Failed(error_msg.clone()));
         }

         emit_migration_event(&app, key, path, "failed", None, Some(error_msg));
         return;
      }
   };

   // Connect to database
   let db = match DatabaseWrapper::connect(&abs_path, None).await {
      Ok(wrapper) => wrapper,
      Err(e) => {
         let error_msg = e.to_string();
         error!("Failed to connect for migrations {}: {}", key, error_msg);

         let mut states = migration_states.0.write().await;
         if let Some(state) = states.get_mut(key) {
            state.update_status(MigrationStatus::Failed(error_msg.clone()));
         }

         emit_migration_event(&app, key, path, "failed", None, Some(error_msg));
         return;
      }
   };

   // Run migrations
   // Note: SQLx's migrator.run() doesn't provide per-migration callbacks,
   // so we can only report start and finish. For detailed per-migration events,
   // we would need to iterate migrations manually.
   trace!("Running migrations for {}", key);

   match db.run_migrations(migrator).await {
      Ok(()) => {
         info!("Migrations completed successfully for {}", key);

         let mut states = migration_states.0.write().await;
         if let Some(state) = states.get_mut(key) {
            state.update_status(MigrationStatus::Complete);
         }

         let migration_count = migrator.iter().count();
         emit_migration_event(&app, key, path, "completed", Some(migration_count), None);
      }
      Err(e) => {
         let error_msg = e.to_string();
         error!("Migration failed for {}: {}", key, error_msg);

         let mut states = migration_states.0.write().await;
         if let Some(state) = states.get_mut(key) {
            state.update_status(MigrationStatus::Failed(error_msg.clone()));
         }

         emit_migration_event(&app, key, path, "failed", None, Some(error_msg));
      }
   }
}

/// Emit a migration event to the frontend and cache it.
fn emit_migration_event<R: Runtime>(
   app: &tauri::AppHandle<R>,
   db_key: &str,
   db_path: &Path,
   status: &str,
   migration_count: Option<usize>,
   error: Option<String>,
) {
   let event = MigrationEvent {
      db_key: db_key.to_string(),
      db_path: db_path.to_path_buf(),
      status: status.to_string(),
      migration_count,
      error,
   };

   // Cache event in migration state
   let migration_states = app.state::<MigrationStates>();
   if let Ok(mut states) = migration_states.0.try_write()
      && let Some(state) = states.get_mut(db_key)
   {
      state.cache_event(event.clone());
   }

   if let Err(e) = app.emit("sqlite:migration", &event) {
      warn!("Failed to emit migration event: {}", e);
   }
}

/// Connect to a registered database by its registration key.
///
/// Opens the database through the same path as the frontend `load` IPC command
/// ([`connect_to_database`]): awaits migrations, enforces max-database limits, and
/// stores the wrapper in [`DbInstances`]. Returns a [`DatabaseWrapper`] for direct
/// toolkit use.
///
/// The `database_key` must match a key registered via
/// [`Builder::register_database`] or [`SetupRegistrar::register_database`].
///
/// # Why use a key?
///
/// Database paths are usually resolved once during plugin setup — for example
/// `app.path().app_data_dir()?.join("main.db")` in [`Builder::on_setup`]. Without
/// registration keys, every call site would repeat that path discovery or keep its own
/// `PathBuf`. Registration stores the key-to-path mapping once; `connect` reuses the key
/// so callers do not supply a filesystem path on every open.
///
/// On mobile, path discovery is not a cheap string join. Resolvers such as
/// [tauri-plugin-fs-resolver](https://github.com/silvermine/tauri-plugin-fs-resolver)
/// call platform-native APIs so paths match OS sandbox rules. On Android that means a
/// JNI call into Kotlin `Context` (e.g. `getFilesDir()`) on each resolve — noticeably
/// more expensive than a local HashMap lookup, and a different kind of boundary than
/// TypeScript-to-Rust IPC (in-process JNI vs webview bridge). Register the resolved
/// `PathBuf` once in `on_setup`; every later `connect(database_key)` only looks up that
/// key in [`RegisteredDatabases`] — no repeat native or JNI work.
///
/// For webview/frontend access, use `Database.load(dbKey)` instead.
///
/// # Example
///
/// ```ignore
/// use tauri::{Manager, Runtime};
/// use tauri_plugin_sqlite::Connection;
///
/// // During setup (on_setup):
/// // reg.register_database("MAIN", app.path().app_data_dir()?.join("main.db"), None);
///
/// async fn read_users<R: Runtime>(app: tauri::AppHandle<R>) -> tauri_plugin_sqlite::Result<()> {
///     let db = app.connect("MAIN").await?;
///     let rows = db.fetch_all("SELECT * FROM users".into(), vec![]).execute().await?;
///     Ok(())
/// }
/// ```
pub trait Connection<R: Runtime> {
   /// Connect with default pool configuration.
   fn connect(&self, database_key: &str) -> impl Future<Output = Result<DatabaseWrapper>> + Send;

   /// Connect with custom [`SqliteDatabaseConfig`] (pool sizes, idle timeout).
   fn connect_with_config(
      &self,
      database_key: &str,
      config: SqliteDatabaseConfig,
   ) -> impl Future<Output = Result<DatabaseWrapper>> + Send;

   /// Close the loaded instance for a registered database key.
   ///
   /// Returns `true` if the database was loaded and successfully closed.
   /// Returns `false` if the database was not loaded (nothing to close).
   /// Returns `Err` if transaction cleanup or pool close fails (database file
   /// may not be safe to delete or recreate).
   ///
   /// On success (`Ok(true)`), connections are closed and WAL is truncated via
   /// `wal_checkpoint(TRUNCATE)`. The main `.db` file is safe to delete or recreate.
   /// `-wal` / `-shm` sidecar files may remain as empty artifacts and are harmless.
   ///
   /// If close returns `Err`, the database file may still be locked — do not delete it.
   ///
   /// Close is bounded by a 5-second timeout; hung pool teardown returns an error
   /// rather than blocking indefinitely.
   ///
   /// Active subscriptions for this key are aborted, and in-flight transactions
   /// are cleaned up (interruptible transactions rolled back; regular transaction
   /// tasks aborted and awaited) before the connection pool is closed.
   fn close(&self, database_key: &str) -> impl Future<Output = Result<bool>> + Send;
}

/// Delegates to [`connect_to_database`]: same open path as the `load` IPC command.
impl<R: Runtime> Connection<R> for AppHandle<R> {
   async fn connect(&self, database_key: &str) -> Result<DatabaseWrapper> {
      let response = connect_to_database(self, database_key, None).await?;
      Ok(response.wrapper)
   }

   async fn connect_with_config(
      &self,
      database_key: &str,
      config: SqliteDatabaseConfig,
   ) -> Result<DatabaseWrapper> {
      let response = connect_to_database(self, database_key, Some(config)).await?;
      Ok(response.wrapper)
   }

   async fn close(&self, database_key: &str) -> Result<bool> {
      let instances = self
         .try_state::<DbInstances>()
         .ok_or(Error::MissingState("DbInstances".into()))?;
      let subs = self
         .try_state::<ActiveSubscriptions>()
         .ok_or(Error::MissingState("ActiveSubscriptions".into()))?;
      let interruptible_txs =
         self
            .try_state::<ActiveInterruptibleTransactions>()
            .ok_or(Error::MissingState(
               "ActiveInterruptibleTransactions".into(),
            ))?;
      let regular_txs = self
         .try_state::<ActiveRegularTransactions>()
         .ok_or(Error::MissingState("ActiveRegularTransactions".into()))?;
      close_database(
         database_key,
         &instances,
         &subs,
         &interruptible_txs,
         &regular_txs,
      )
      .await
   }
}

struct ConnectionResponse {
   path: PathBuf,
   wrapper: DatabaseWrapper,
}

async fn connect_to_database<R: Runtime>(
   app: &AppHandle<R>,
   db_key: &str,
   custom_config: Option<SqliteDatabaseConfig>,
) -> Result<ConnectionResponse> {
   let migration_states = app.state::<MigrationStates>();
   let db_instances = app.state::<DbInstances>();

   // Wait for migrations to complete if registered for this database
   await_migrations(&migration_states, db_key).await?;

   let path = resolve_database_path(db_key, app)?;

   let instances = db_instances.inner.read().await;

   // Return cached if db was already loaded
   if let Some(wrapper) = instances.get(db_key) {
      return Ok(ConnectionResponse {
         path,
         wrapper: wrapper.clone(),
      });
   }

   drop(instances); // Release read lock before acquiring write lock

   let mut instances = db_instances.inner.write().await;

   // Check database count limit before creating a new connection.
   // This check is before entry() to avoid borrow conflicts, and the write lock
   // prevents races between the len() check and the insert.
   if !instances.contains_key(db_key) && instances.len() >= db_instances.max {
      return Err(Error::TooManyDatabases(db_instances.max));
   }

   // Use entry API to atomically check and insert, avoiding race conditions
   // where two callers could both create wrappers
   use std::collections::hash_map::Entry;
   match instances.entry(db_key.to_string()) {
      Entry::Occupied(entry) => {
         // Another caller won the race and inserted while we waited for write lock
         Ok(ConnectionResponse {
            path,
            wrapper: entry.get().clone(),
         })
      }
      Entry::Vacant(entry) => {
         // We won the race, create and insert the wrapper
         let wrapper = DatabaseWrapper::connect(&path, custom_config).await?;
         entry.insert(wrapper.clone());
         Ok(ConnectionResponse { path, wrapper })
      }
   }
}

/// Wait for migrations to complete for a database, if any are registered.
///
/// Returns Ok(()) if:
/// - No migrations are registered for this database
/// - Migrations completed successfully
///
/// Returns Err if migrations failed.
async fn await_migrations(migration_states: &MigrationStates, db_key: &str) -> Result<()> {
   loop {
      // Get notify handle before checking status
      let notify = {
         match migration_states.0.read().await.get(db_key) {
            // No migrations registered for this database
            None => return Ok(()),

            Some(state) => match &state.status {
               // Migrations completed successfully
               MigrationStatus::Complete => return Ok(()),

               // Migrations failed - return the error
               MigrationStatus::Failed(error) => {
                  return Err(Error::Migration(sqlx::migrate::MigrateError::Source(
                     error.clone().into(),
                  )));
               }

               // Migrations still pending or running - wait for notification
               MigrationStatus::Pending | MigrationStatus::Running => state.notify.clone(),
            },
         }
      };

      // Wait for migration state change
      notify.notified().await;
   }
}

/// Close a loaded database by key, aborting subscriptions and in-flight transactions first.
///
/// Attempts full cleanup even when transaction teardown fails. Returns `Ok(true)` when
/// the database was loaded and fully closed, `Ok(false)` when it was not loaded,
/// or `Err` when cleanup or pool close fails.
///
/// On success, WAL is truncated and the main `.db` file is safe to delete or recreate.
pub(crate) async fn close_database(
   db_key: &str,
   db_instances: &DbInstances,
   active_subs: &ActiveSubscriptions,
   interruptible_txs: &ActiveInterruptibleTransactions,
   regular_txs: &ActiveRegularTransactions,
) -> Result<bool> {
   let db_key = db_key.to_string();
   let close_result = tokio::time::timeout(
      CLOSE_TIMEOUT,
      close_database_inner(
         &db_key,
         db_instances,
         active_subs,
         interruptible_txs,
         regular_txs,
      ),
   )
   .await;

   match close_result {
      Ok(result) => result,
      Err(_) => Err(Error::Other(format!(
         "database close timed out after {} seconds",
         CLOSE_TIMEOUT.as_secs()
      ))),
   }
}

/// Close a loaded database, attempting full cleanup even when transaction teardown fails.
async fn close_database_inner(
   db_key: &str,
   db_instances: &DbInstances,
   active_subs: &ActiveSubscriptions,
   interruptible_txs: &ActiveInterruptibleTransactions,
   regular_txs: &ActiveRegularTransactions,
) -> Result<bool> {
   let mut last_error = None;

   active_subs.remove_for_db(db_key).await;

   if let Err(err) =
      sqlx_sqlite_toolkit::cleanup_transactions_for_db(db_key, interruptible_txs, regular_txs).await
   {
      last_error = Some(err.into());
   }

   let mut instances = db_instances.inner.write().await;
   let wrapper = instances.remove(db_key);
   drop(instances);

   let was_loaded = wrapper.is_some();
   if let Some(wrapper) = wrapper
      && let Err(err) = wrapper.close().await
   {
      last_error = Some(err.into());
   }

   match last_error {
      Some(err) => Err(err),
      None => Ok(was_loaded),
   }
}

async fn close_all_wrappers(db_instances: &DbInstances) -> Result<()> {
   let mut instances = db_instances.inner.write().await;
   let wrappers: Vec<DatabaseWrapper> = instances.drain().map(|(_, v)| v).collect();
   drop(instances);

   let mut last_error: Option<Error> = None;
   for wrapper in wrappers {
      if let Err(e) = wrapper.close().await {
         last_error = Some(e.into());
      }
   }

   match last_error {
      Some(e) => Err(e),
      None => Ok(()),
   }
}

/// Close all loaded database instances after aborting subscriptions and
/// cleaning up in-flight transactions.
pub(crate) async fn close_all_loaded_databases(
   db_instances: &DbInstances,
   active_subs: &ActiveSubscriptions,
   interruptible_txs: &ActiveInterruptibleTransactions,
   regular_txs: &ActiveRegularTransactions,
) -> Result<()> {
   let close_result = tokio::time::timeout(
      CLOSE_TIMEOUT,
      close_all_loaded_databases_inner(db_instances, active_subs, interruptible_txs, regular_txs),
   )
   .await;

   match close_result {
      Ok(result) => result,
      Err(_) => Err(Error::Other(format!(
         "database close timed out after {} seconds",
         CLOSE_TIMEOUT.as_secs()
      ))),
   }
}

async fn close_all_loaded_databases_inner(
   db_instances: &DbInstances,
   active_subs: &ActiveSubscriptions,
   interruptible_txs: &ActiveInterruptibleTransactions,
   regular_txs: &ActiveRegularTransactions,
) -> Result<()> {
   let mut last_error = None;

   active_subs.abort_all().await;

   if let Err(err) =
      sqlx_sqlite_toolkit::cleanup_all_transactions(interruptible_txs, regular_txs).await
   {
      last_error = Some(err.into());
   }

   if let Err(err) = close_all_wrappers(db_instances).await {
      last_error = Some(err);
   }

   match last_error {
      Some(err) => Err(err),
      None => Ok(()),
   }
}

/// Resolve a registered database path by key.
///
/// The `db_key` must match a key registered via
/// [`crate::Builder::register_database`] / [`crate::SetupRegistrar::register_database`].
///
/// Returns `Err(Error::PathNotRegistered)` if the key is not registered.
fn resolve_database_path<R: Runtime>(db_key: &str, app: &AppHandle<R>) -> Result<PathBuf> {
   let registered_databases = app.state::<RegisteredDatabases>();

   if let Some(path) = registered_databases.database_path_by_key.get(db_key) {
      return Ok(path.clone());
   }

   Err(Error::PathNotRegistered(db_key.to_string()))
}

#[cfg(test)]
mod tests {
   use super::*;
   use crate::commands;
   use std::collections::HashMap;
   use tauri::plugin::Plugin;
   use tauri::test::{MockRuntime, mock_app, mock_builder, mock_context, noop_assets};
   use uuid::Uuid;

   /// Build and initialize the plugin for a single registered database.
   ///
   /// Must run **outside** a Tokio runtime context (or on a `spawn_blocking` thread).
   /// Plugin `.setup()` calls `tokio::sync::RwLock::blocking_write()`, which panics
   /// if invoked while the current thread is already driving async tasks (e.g. inside
   /// `#[tokio::test]`). Integration tests below use `#[test]` + `block_on` and run
   /// initialization via `spawn_blocking` before awaiting commands.
   fn init_app_with_registered_db_at_path(
      key: &str,
      db_path: PathBuf,
   ) -> (tauri::App<MockRuntime>, PathBuf) {
      let mut plugin = Builder::<MockRuntime>::new()
         .register_database(key, &db_path, None)
         .unwrap()
         .build()
         .unwrap();
      let app = mock_app();
      plugin
         .initialize(app.handle(), serde_json::Value::default())
         .expect("plugin init should succeed");
      (app, db_path)
   }

   fn init_app_with_main_and_other(
      main_path: PathBuf,
      other_path: PathBuf,
   ) -> tauri::App<MockRuntime> {
      let mut plugin = Builder::<MockRuntime>::new()
         .register_database("MAIN", &main_path, None)
         .unwrap()
         .register_database("OTHER", &other_path, None)
         .unwrap()
         .build()
         .unwrap();
      let app = mock_app();
      plugin
         .initialize(app.handle(), serde_json::Value::default())
         .expect("plugin init should succeed");
      app
   }

   async fn load_and_create_test_table(app: &tauri::App<MockRuntime>, db_key: &str) {
      connect_to_database(app.handle(), db_key, None)
         .await
         .expect("connect should succeed");

      commands::execute(
         app.state::<DbInstances>(),
         db_key.to_string(),
         "CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)".to_string(),
         vec![],
         None,
      )
      .await
      .expect("create table should succeed");
   }

   /// Holds a writer mid-transaction so `close` must abort with a checked-out connection.
   async fn spawn_tracked_mid_write_regular_transaction(
      app: &tauri::App<MockRuntime>,
   ) -> tokio::sync::oneshot::Receiver<()> {
      use sqlx_sqlite_toolkit::TransactionWriter;

      let wrapper = {
         let instances = app.state::<DbInstances>().inner().inner.read().await;
         instances
            .get("MAIN")
            .expect("MAIN should be loaded")
            .clone()
      };
      let tx_id = Uuid::new_v4().to_string();
      let regular_txs = app.state::<ActiveRegularTransactions>().inner().clone();
      let (started_tx, started_rx) = tokio::sync::oneshot::channel();
      let tx_id_for_task = tx_id.clone();

      let handle = tokio::spawn(async move {
         let guard = wrapper.acquire_writer().await.expect("acquire writer");
         let mut writer = TransactionWriter::from(guard);
         writer
            .begin_immediate()
            .await
            .expect("begin immediate should succeed");
         writer
            .execute_query(
               sqlx::query("INSERT INTO test (val) VALUES (?)").bind("should-not-commit"),
            )
            .await
            .expect("insert should succeed");
         started_tx.send(()).ok();
         tokio::time::sleep(std::time::Duration::from_secs(60)).await;
         regular_txs.remove(&tx_id_for_task).await;
      });

      app.state::<ActiveRegularTransactions>()
         .inner()
         .insert("MAIN".into(), tx_id, handle)
         .await;

      started_rx
   }

   fn builder_with_duplicate_paths(temp_dir: &tempfile::TempDir) -> Builder<MockRuntime> {
      let path = validate::validate_database_path(temp_dir.path().join("main.db")).unwrap();

      Builder::<MockRuntime>::new()
         .register_database("MAIN", &path, None)
         .unwrap()
         .register_database("BACKUP", &path, None)
         .unwrap()
   }

   fn mock_app_with_registrations(
      database_path_by_key: HashMap<String, PathBuf>,
   ) -> tauri::App<MockRuntime> {
      let app = tauri::test::mock_app();
      app.manage(RegisteredDatabases {
         database_path_by_key: Arc::new(database_path_by_key),
      });
      app.manage(DbInstances::default());
      app.manage(MigrationStates::default());
      app
   }

   #[tokio::test]
   async fn test_connect_to_database_registered_key() {
      let temp_dir = tempfile::tempdir().unwrap();
      let db_path = validate::validate_database_path(temp_dir.path().join("main.db")).unwrap();
      let mut registrations = HashMap::new();
      registrations.insert("MAIN".to_string(), db_path.clone());

      let app = mock_app_with_registrations(registrations);
      let response = connect_to_database(app.handle(), "MAIN", None)
         .await
         .unwrap();

      assert_eq!(response.path, db_path);
   }

   #[tokio::test]
   async fn test_connect_unregistered_key_returns_path_not_registered() {
      let app = mock_app_with_registrations(HashMap::new());
      let err = match connect_to_database(app.handle(), "MAIN", None).await {
         Err(error) => error,
         Ok(_) => panic!("expected unregistered key to fail"),
      };

      assert!(matches!(err, Error::PathNotRegistered(_)));
      assert_eq!(err.to_string(), "database key not registered: MAIN");
   }

   #[test]
   fn test_register_migrate_and_connect_by_key() {
      let temp_dir = tempfile::tempdir().unwrap();
      let main_db_path = validate::validate_database_path(temp_dir.path().join("main.db")).unwrap();
      let static_db_path =
         validate::validate_database_path(temp_dir.path().join("static.db")).unwrap();
      let main_path_for_setup = main_db_path.clone();

      let mut plugin = Builder::<MockRuntime>::new()
         .register_database("STATIC", static_db_path.clone(), None)
         .unwrap()
         .on_setup(move |_app, reg| {
            reg.register_database(
               "MAIN",
               &main_path_for_setup,
               Some(sqlx::migrate!("./doc-test-fixtures/migrations")),
            )?;
            Ok(())
         })
         .build()
         .unwrap();

      let app = mock_app();
      plugin
         .initialize(app.handle(), serde_json::Value::default())
         .expect("plugin setup should succeed");

      assert_eq!(
         resolve_database_path("STATIC", app.handle()).unwrap(),
         static_db_path
      );
      assert_eq!(
         resolve_database_path("MAIN", app.handle()).unwrap(),
         main_db_path
      );

      tauri::async_runtime::block_on(async {
         let wrapper = app.handle().connect("MAIN").await.unwrap();

         let rows = wrapper
            .fetch_all(
               "SELECT name FROM sqlite_master WHERE type = 'table' AND name = '_doc_test_dummy'"
                  .into(),
               vec![],
            )
            .await
            .unwrap();
         assert_eq!(rows.len(), 1);
         assert_eq!(
            rows[0].get("name").and_then(|value| value.as_str()),
            Some("_doc_test_dummy")
         );

         let migration_states = app.state::<MigrationStates>();
         let states = migration_states.0.read().await;
         let main_state = states.get("MAIN").expect("MAIN migration state");
         assert!(matches!(main_state.status, MigrationStatus::Complete));

         let config_wrapper = app
            .handle()
            .connect_with_config("MAIN", SqliteDatabaseConfig::default())
            .await
            .unwrap();
         let cached_rows = config_wrapper
            .fetch_all(
               "SELECT name FROM sqlite_master WHERE type = 'table' AND name = '_doc_test_dummy'"
                  .into(),
               vec![],
            )
            .await
            .unwrap();
         assert_eq!(cached_rows.len(), 1);
      });
   }

   #[test]
   fn test_migrate_before_connect_waits_for_complete() {
      let temp_dir = tempfile::tempdir().unwrap();
      let db_path = validate::validate_database_path(temp_dir.path().join("main.db")).unwrap();
      let migrations_dir = tempfile::tempdir().unwrap();
      std::fs::write(
         migrations_dir.path().join("20240101000000_dummy.sql"),
         "CREATE TABLE IF NOT EXISTS _migrate_before_connect (id INTEGER PRIMARY KEY);",
      )
      .unwrap();
      let migrator = tauri::async_runtime::block_on(Migrator::new(migrations_dir.path())).unwrap();

      let mut plugin = Builder::<MockRuntime>::new()
         .register_database("MAIN", db_path, Some(migrator))
         .unwrap()
         .build()
         .unwrap();

      let app = mock_app();
      plugin
         .initialize(app.handle(), serde_json::Value::default())
         .expect("plugin setup should succeed");

      tauri::async_runtime::block_on(async {
         let app_handle = app.handle().clone();
         let migration_states = app.state::<MigrationStates>();
         let connect_task = tokio::spawn(async move { app_handle.connect("MAIN").await });

         let mut connect_waited = false;
         while !connect_task.is_finished() {
            if matches!(
               migration_states
                  .0
                  .read()
                  .await
                  .get("MAIN")
                  .map(|state| state.status.clone()),
               Some(MigrationStatus::Pending | MigrationStatus::Running)
            ) {
               connect_waited = true;
            }
            tokio::task::yield_now().await;
         }

         if !connect_waited {
            migration_states
               .0
               .write()
               .await
               .insert("AWAIT_GATE".to_string(), MigrationState::new());
            let gate_app = app.handle().clone();
            let gate = tokio::spawn(async move {
               await_migrations(&gate_app.state::<MigrationStates>(), "AWAIT_GATE").await
            });
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            assert!(
               !gate.is_finished(),
               "await_migrations should block until Complete"
            );
            migration_states
               .0
               .write()
               .await
               .get_mut("AWAIT_GATE")
               .unwrap()
               .update_status(MigrationStatus::Complete);
            gate.await.unwrap().unwrap();
         }

         let wrapper = connect_task.await.unwrap().unwrap();
         assert!(matches!(
            migration_states.0.read().await.get("MAIN").unwrap().status,
            MigrationStatus::Complete
         ));
         let rows = wrapper
            .fetch_all(
               "SELECT name FROM sqlite_master WHERE type = 'table' AND name = '_migrate_before_connect'"
                  .into(),
               vec![],
            )
            .await
            .unwrap();
         assert_eq!(rows.len(), 1);
      });
   }

   #[test]
   fn test_register_migrate_and_connect_by_key_returns_error_if_point_to_same_path() {
      let temp_dir = tempfile::tempdir().unwrap();
      let main_db_path = validate::validate_database_path(temp_dir.path().join("main.db")).unwrap();
      let main_path_for_setup = main_db_path.clone();

      let mut plugin = Builder::<MockRuntime>::new()
         .register_database("MAIN", main_db_path.clone(), None)
         .unwrap()
         .on_setup(move |_app, reg| {
            reg.register_database(
               "MAIN_2",
               &main_path_for_setup,
               Some(sqlx::migrate!("./doc-test-fixtures/migrations")),
            )?;
            Ok(())
         })
         .build()
         .unwrap();

      let app = mock_app();
      let err_msg = plugin
         .initialize(app.handle(), serde_json::Value::default())
         .unwrap_err()
         .to_string();
      assert!(err_msg.contains("MAIN") && err_msg.contains("MAIN_2"));
   }

   #[test]
   fn test_register_database_last_registration_wins() {
      let temp_dir = tempfile::tempdir().unwrap();
      let first_path = validate::validate_database_path(temp_dir.path().join("first.db")).unwrap();
      let second_path =
         validate::validate_database_path(temp_dir.path().join("second.db")).unwrap();

      let mut database_path_by_key = HashMap::new();
      database_path_by_key.insert("MAIN".to_string(), first_path);
      database_path_by_key.insert("MAIN".to_string(), second_path.clone());

      let app = mock_app_with_registrations(database_path_by_key);
      let path = resolve_database_path("MAIN", app.handle()).unwrap();

      assert_eq!(path, second_path);
   }

   #[test]
   fn test_setup_rejects_duplicate_paths_for_distinct_keys() {
      let temp_dir = tempfile::tempdir().unwrap();
      let mut plugin = builder_with_duplicate_paths(&temp_dir).build().unwrap();
      let app = mock_app();

      let err = plugin
         .initialize(app.handle(), serde_json::Value::default())
         .unwrap_err();

      let err_msg = err.to_string();
      assert!(err_msg.contains("MAIN"));
      assert!(err_msg.contains("BACKUP"));
      assert!(err_msg.contains("invalid configuration"));
   }

   #[test]
   fn test_app_build_rejects_duplicate_paths_for_distinct_keys() {
      let temp_dir = tempfile::tempdir().unwrap();
      let plugin = builder_with_duplicate_paths(&temp_dir).build().unwrap();

      let err = mock_builder()
         .plugin(plugin)
         .build(mock_context(noop_assets()))
         .unwrap_err();

      match err {
         tauri::Error::PluginInitialization(name, message) => {
            assert_eq!(name, "sqlite");
            assert!(message.contains("MAIN"));
            assert!(message.contains("BACKUP"));
            assert!(message.contains("invalid configuration"));
         }
         other => panic!("expected PluginInitialization, got {other:?}"),
      }
   }

   #[test]
   fn test_max_databases_rejects_zero() {
      let err = Builder::<MockRuntime>::new().max_databases(0).unwrap_err();
      assert!(matches!(err, Error::InvalidConfig(_)));
   }

   #[test]
   fn test_max_databases_accepts_positive() {
      let builder = Builder::<MockRuntime>::new().max_databases(1).unwrap();
      assert_eq!(builder.max_databases, Some(1));
   }

   #[test]
   fn test_transaction_timeout_rejects_zero() {
      let err = Builder::<MockRuntime>::new()
         .transaction_timeout(std::time::Duration::ZERO)
         .unwrap_err();
      assert!(matches!(err, Error::InvalidConfig(_)));
   }

   #[test]
   fn test_transaction_timeout_accepts_positive() {
      let builder = Builder::<MockRuntime>::new()
         .transaction_timeout(std::time::Duration::from_secs(1))
         .unwrap();
      assert_eq!(
         builder.transaction_timeout,
         Some(std::time::Duration::from_secs(1))
      );
   }

   #[test]
   fn test_connect_without_migrator_does_not_wait_on_migration_state() {
      let temp_dir = tempfile::tempdir().unwrap();
      let db_path = validate::validate_database_path(temp_dir.path().join("main.db")).unwrap();
      let key = "MAIN".to_string();

      tauri::async_runtime::block_on(async {
         // `plugin.initialize()` must not run on the runtime worker thread — see
         // `init_app_with_registered_db_at_path` for why we use `spawn_blocking`.
         let (app, path) =
            tokio::task::spawn_blocking(move || init_app_with_registered_db_at_path(&key, db_path))
               .await
               .expect("plugin init task should succeed");

         let migration_states = app.state::<MigrationStates>();
         assert!(
            !migration_states.0.read().await.contains_key("MAIN"),
            "databases without a migrator should not have migration state"
         );

         let response = connect_to_database(app.handle(), "MAIN", None)
            .await
            .expect("connect should not block on migration state");
         assert_eq!(response.path, path);
      });
   }

   #[test]
   fn test_close_rolls_back_interruptible_transaction() {
      let temp_dir = tempfile::tempdir().unwrap();
      let db_path = validate::validate_database_path(temp_dir.path().join("main.db")).unwrap();
      let key = "MAIN".to_string();

      tauri::async_runtime::block_on(async {
         let (app, _) =
            tokio::task::spawn_blocking(move || init_app_with_registered_db_at_path(&key, db_path))
               .await
               .expect("plugin init task should succeed");

         load_and_create_test_table(&app, "MAIN").await;

         commands::begin_interruptible_transaction(
            app.state::<DbInstances>(),
            app.state::<ActiveInterruptibleTransactions>(),
            "MAIN".to_string(),
            vec![Statement {
               query: "INSERT INTO test (val) VALUES (?)".to_string(),
               values: vec![serde_json::json!("uncommitted")],
            }],
            None,
         )
         .await
         .expect("begin interruptible transaction should succeed");

         let closed = commands::close(
            app.state::<DbInstances>(),
            app.state::<ActiveSubscriptions>(),
            app.state::<ActiveInterruptibleTransactions>(),
            app.state::<ActiveRegularTransactions>(),
            "MAIN".to_string(),
         )
         .await
         .expect("close should succeed");
         assert!(closed);

         assert!(
            app.state::<DbInstances>()
               .inner()
               .inner
               .read()
               .await
               .get("MAIN")
               .is_none(),
            "close should remove the loaded instance"
         );

         let response = connect_to_database(app.handle(), "MAIN", None)
            .await
            .expect("reload after close should succeed");
         let rows = response
            .wrapper
            .fetch_all("SELECT val FROM test".into(), vec![])
            .await
            .expect("fetch after close should succeed");

         assert!(
            rows.is_empty(),
            "close should roll back interruptible transaction before closing"
         );
      });
   }

   #[test]
   fn test_close_aborts_in_flight_regular_transaction() {
      let temp_dir = tempfile::tempdir().unwrap();
      let db_path = validate::validate_database_path(temp_dir.path().join("main.db")).unwrap();
      let key = "MAIN".to_string();

      tauri::async_runtime::block_on(async {
         let (app, path) =
            tokio::task::spawn_blocking(move || init_app_with_registered_db_at_path(&key, db_path))
               .await
               .expect("plugin init task should succeed");

         load_and_create_test_table(&app, "MAIN").await;

         let started_rx = spawn_tracked_mid_write_regular_transaction(&app).await;
         started_rx
            .await
            .expect("regular transaction should hold writer mid-flight");

         let closed = commands::close(
            app.state::<DbInstances>(),
            app.state::<ActiveSubscriptions>(),
            app.state::<ActiveInterruptibleTransactions>(),
            app.state::<ActiveRegularTransactions>(),
            "MAIN".to_string(),
         )
         .await
         .expect("close should succeed");
         assert!(closed);

         assert!(
            app.state::<DbInstances>()
               .inner()
               .inner
               .read()
               .await
               .get("MAIN")
               .is_none(),
            "close should remove the loaded instance"
         );

         let response = connect_to_database(app.handle(), "MAIN", None)
            .await
            .expect("reload after close should succeed");
         let rows = response
            .wrapper
            .fetch_all("SELECT val FROM test".into(), vec![])
            .await
            .expect("fetch after close should succeed");

         assert!(
            rows.is_empty(),
            "close should abort mid-write regular transaction before closing"
         );

         std::fs::remove_file(&path).expect("database file should be safe to delete after close");
      });
   }

   #[test]
   fn test_close_all_cleans_up_transactions() {
      let temp_dir = tempfile::tempdir().unwrap();
      let main_path = validate::validate_database_path(temp_dir.path().join("main.db")).unwrap();
      let other_path = validate::validate_database_path(temp_dir.path().join("other.db")).unwrap();

      let main_path_for_delete = main_path.clone();

      tauri::async_runtime::block_on(async {
         let app = tokio::task::spawn_blocking(move || {
            init_app_with_main_and_other(main_path, other_path)
         })
         .await
         .expect("plugin init task should succeed");

         load_and_create_test_table(&app, "MAIN").await;
         load_and_create_test_table(&app, "OTHER").await;

         commands::begin_interruptible_transaction(
            app.state::<DbInstances>(),
            app.state::<ActiveInterruptibleTransactions>(),
            "MAIN".to_string(),
            vec![Statement {
               query: "INSERT INTO test (val) VALUES (?)".to_string(),
               values: vec![serde_json::json!("uncommitted")],
            }],
            None,
         )
         .await
         .expect("begin interruptible transaction should succeed");

         commands::begin_interruptible_transaction(
            app.state::<DbInstances>(),
            app.state::<ActiveInterruptibleTransactions>(),
            "OTHER".to_string(),
            vec![Statement {
               query: "INSERT INTO test (val) VALUES (?)".to_string(),
               values: vec![serde_json::json!("other-uncommitted")],
            }],
            None,
         )
         .await
         .expect("begin OTHER interruptible transaction should succeed");

         commands::close_all(
            app.state::<DbInstances>(),
            app.state::<ActiveSubscriptions>(),
            app.state::<ActiveInterruptibleTransactions>(),
            app.state::<ActiveRegularTransactions>(),
         )
         .await
         .expect("close_all should succeed");

         assert!(
            app.state::<DbInstances>()
               .inner()
               .inner
               .read()
               .await
               .is_empty(),
            "close_all should remove all loaded instances"
         );

         let response = connect_to_database(app.handle(), "MAIN", None)
            .await
            .expect("reload after close_all should succeed");
         let rows = response
            .wrapper
            .fetch_all("SELECT val FROM test".into(), vec![])
            .await
            .expect("fetch after close_all should succeed");

         assert!(
            rows.is_empty(),
            "close_all should roll back interruptible transactions before closing"
         );

         std::fs::remove_file(&main_path_for_delete)
            .expect("database file should be safe to delete after close_all");
      });
   }

   #[test]
   fn test_close_only_aborts_transactions_for_target_database() {
      let temp_dir = tempfile::tempdir().unwrap();
      let main_path = validate::validate_database_path(temp_dir.path().join("main.db")).unwrap();
      let other_path = validate::validate_database_path(temp_dir.path().join("other.db")).unwrap();

      tauri::async_runtime::block_on(async {
         let app = tokio::task::spawn_blocking(move || {
            init_app_with_main_and_other(main_path, other_path)
         })
         .await
         .expect("plugin init task should succeed");

         load_and_create_test_table(&app, "MAIN").await;
         load_and_create_test_table(&app, "OTHER").await;

         let main_token = commands::begin_interruptible_transaction(
            app.state::<DbInstances>(),
            app.state::<ActiveInterruptibleTransactions>(),
            "MAIN".to_string(),
            vec![Statement {
               query: "INSERT INTO test (val) VALUES (?)".to_string(),
               values: vec![serde_json::json!("main-uncommitted")],
            }],
            None,
         )
         .await
         .expect("begin MAIN interruptible transaction should succeed");

         let other_token = commands::begin_interruptible_transaction(
            app.state::<DbInstances>(),
            app.state::<ActiveInterruptibleTransactions>(),
            "OTHER".to_string(),
            vec![Statement {
               query: "INSERT INTO test (val) VALUES (?)".to_string(),
               values: vec![serde_json::json!("other-uncommitted")],
            }],
            None,
         )
         .await
         .expect("begin OTHER interruptible transaction should succeed");

         let closed = commands::close(
            app.state::<DbInstances>(),
            app.state::<ActiveSubscriptions>(),
            app.state::<ActiveInterruptibleTransactions>(),
            app.state::<ActiveRegularTransactions>(),
            "MAIN".to_string(),
         )
         .await
         .expect("close should succeed");
         assert!(closed);

         assert!(
            app.state::<DbInstances>()
               .inner()
               .inner
               .read()
               .await
               .get("MAIN")
               .is_none()
         );
         assert!(
            app.state::<DbInstances>()
               .inner()
               .inner
               .read()
               .await
               .get("OTHER")
               .is_some()
         );

         let err = commands::transaction_continue(
            app.state::<ActiveInterruptibleTransactions>(),
            main_token,
            commands::TransactionAction::Commit,
         )
         .await
         .expect_err("MAIN transaction should have been aborted on close");
         assert!(matches!(
            err,
            Error::Toolkit(sqlx_sqlite_toolkit::Error::NoActiveTransaction(_))
         ));

         let continued = commands::transaction_continue(
            app.state::<ActiveInterruptibleTransactions>(),
            other_token,
            commands::TransactionAction::Rollback,
         )
         .await
         .expect("OTHER transaction should still be active");
         assert!(continued.is_none());
      });
   }

   #[test]
   fn test_execute_transaction_returns_cancelled_when_database_closed() {
      let temp_dir = tempfile::tempdir().unwrap();
      let db_path = validate::validate_database_path(temp_dir.path().join("main.db")).unwrap();
      let key = "MAIN".to_string();

      tauri::async_runtime::block_on(async {
         let (app, _) =
            tokio::task::spawn_blocking(move || init_app_with_registered_db_at_path(&key, db_path))
               .await
               .expect("plugin init task should succeed");

         load_and_create_test_table(&app, "MAIN").await;

         let started_rx = spawn_tracked_mid_write_regular_transaction(&app).await;
         started_rx
            .await
            .expect("regular transaction should hold writer mid-flight");

         let app_for_exec = app.handle().clone();
         let exec_task = tokio::spawn(async move {
            commands::execute_transaction(
               app_for_exec.state::<DbInstances>(),
               app_for_exec.state::<ActiveRegularTransactions>(),
               "MAIN".to_string(),
               vec![Statement {
                  query: "INSERT INTO test (val) VALUES (?)".to_string(),
                  values: vec![serde_json::json!("blocked")],
               }],
               None,
            )
            .await
         });

         tokio::time::sleep(std::time::Duration::from_millis(50)).await;

         commands::close(
            app.state::<DbInstances>(),
            app.state::<ActiveSubscriptions>(),
            app.state::<ActiveInterruptibleTransactions>(),
            app.state::<ActiveRegularTransactions>(),
            "MAIN".to_string(),
         )
         .await
         .expect("close should succeed");

         let err = exec_task.await.unwrap().expect_err("execute should fail");
         assert!(matches!(
            err,
            Error::Toolkit(sqlx_sqlite_toolkit::Error::TransactionCancelled(_))
         ));
         assert!(err.to_string().contains("transaction cancelled"));
      });
   }
}
