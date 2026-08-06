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

use crate::subscriptions::{ActiveSubscriptions, ObserverRegistrations};

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
            app.manage(subscriptions::ObserverRegistrations::default());

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
                  let observer_regs_clone = app.state::<subscriptions::ObserverRegistrations>().inner().clone();

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

                              // observer_regs_clone is cleared inside close_all_wrappers,
                              // under the same db lock used to drain the instances map.
                              if let Err(e) =
                                 close_all_wrappers(&instances_clone, &observer_regs_clone).await
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
               // A window closing without ever calling unobserve() would otherwise
               // leak its observer registration(s) forever, keeping affected
               // brokers alive with no one left listening (the "phantom
               // registration" gap from the #54 review). The webview label
               // matches the window label for a `WebviewWindow` (the shape this
               // whole design targets); a window hosting multiple independent
               // webviews with distinct labels would need each webview's own
               // Destroyed/close event, which this single window-level hook does
               // not cover. Labels are also reusable across a window's lifetime:
               // a window later recreated with the same static label silently
               // "inherits" any registration left behind here - harmless for a
               // fixed label, but worth knowing for dynamically labeled windows.
               RunEvent::WindowEvent {
                  label,
                  event: tauri::WindowEvent::Destroyed,
                  ..
               } => {
                  let observer_regs = app.state::<subscriptions::ObserverRegistrations>().inner().clone();
                  let active_subs = app.state::<subscriptions::ActiveSubscriptions>().inner().clone();
                  let db_instances = app.state::<DbInstances>().inner().clone();
                  let label = label.clone();

                  tauri::async_runtime::spawn(async move {
                     // Lock order: db_instances write lock, then
                     // observer_regs (same order as observe()/unobserve()
                     // and close_database_inner - see the module doc in
                     // src/commands.rs). Held across the whole
                     // release+disable sequence: a concurrent observe() must
                     // not be able to register into a broker in the window
                     // between "registrations released" and "broker actually
                     // disabled" below.
                     let mut instances = db_instances.write().await;
                     let newly_unobserved = observer_regs
                        .release_all_for_label(&mut instances, &label)
                        .await;
                     if newly_unobserved.is_empty() {
                        return;
                     }

                     debug!(
                        "Window '{}' destroyed - tearing down observation for {} database(s) with no remaining observers",
                        label,
                        newly_unobserved.len()
                     );

                     for db_key in newly_unobserved {
                        active_subs.remove_for_db(&db_key).await;
                        if let Some(wrapper) = instances.get_mut(&db_key) {
                           wrapper.disable_observation();
                        }
                     }
                  });
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
      let observer_regs = self
         .try_state::<ObserverRegistrations>()
         .ok_or(Error::MissingState("ObserverRegistrations".into()))?;
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
         &observer_regs,
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
   observer_regs: &ObserverRegistrations,
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
         observer_regs,
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
   observer_regs: &ObserverRegistrations,
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

   // Lock order: db_instances write lock, then observer_regs - same order as
   // observe()/unobserve() (see the module doc in src/commands.rs), and for the
   // same reason: clearing registrations while STILL holding the db lock (not
   // before, and not after dropping it) prevents a concurrent observe() from
   // registering into - and enabling observation on - this wrapper in the
   // window between "registrations cleared" and "wrapper actually removed
   // below". Without this, that race would leave a phantom registration for a
   // wrapper that's about to be destroyed, i.e. reintroducing the exact
   // problem this whole feature exists to prevent, for the close() path.
   let mut instances = db_instances.write().await;
   let wrapper = instances.remove(db_key);
   // Observation is torn down unconditionally on a full close, regardless of how
   // many windows had registered via observe() - a closed database has no live
   // broker for anyone to observe.
   observer_regs.clear_for_db(&mut instances, db_key).await;
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

/// Drains and closes every loaded database, clearing their observer
/// registrations under the same db lock (see `close_database_inner` for why
/// `observer_regs` must be touched while still holding `db_instances`'s lock,
/// not before or after).
async fn close_all_wrappers(
   db_instances: &DbInstances,
   observer_regs: &ObserverRegistrations,
) -> Result<()> {
   let mut instances = db_instances.write().await;
   let wrappers: Vec<DatabaseWrapper> = instances.drain().map(|(_, v)| v).collect();
   observer_regs.clear_all(&mut instances).await;
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
   observer_regs: &ObserverRegistrations,
   interruptible_txs: &ActiveInterruptibleTransactions,
   regular_txs: &ActiveRegularTransactions,
) -> Result<()> {
   let close_result = tokio::time::timeout(
      CLOSE_TIMEOUT,
      close_all_loaded_databases_inner(
         db_instances,
         active_subs,
         observer_regs,
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

async fn close_all_loaded_databases_inner(
   db_instances: &DbInstances,
   active_subs: &ActiveSubscriptions,
   observer_regs: &ObserverRegistrations,
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

   // observer_regs is cleared inside close_all_wrappers, under the same db
   // lock used to drain the instances map - see its doc comment.
   if let Err(err) = close_all_wrappers(db_instances, observer_regs).await {
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
   use crate::subscriptions::ObserverConfigParams;
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
            app.state::<ObserverRegistrations>(),
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
            app.state::<ObserverRegistrations>(),
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
            app.state::<ObserverRegistrations>(),
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
            app.state::<ObserverRegistrations>(),
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

   /// Regression test for the app-wide freeze fixed alongside `remove()`
   /// holding `db_instances`'s write lock across `wrapper.remove()`: an
   /// abandoned interruptible transaction on the database being removed must
   /// not stall operations on a *different*, unrelated loaded database, and
   /// `remove()` itself must not hang forever waiting on a connection it is
   /// itself holding.
   ///
   /// Before the fix, `wrapper.remove()`'s `Pool::close()` (no timeout of its
   /// own) waited forever for the write connection an abandoned interruptible
   /// transaction had checked out - `ActiveInterruptibleTransactions` only
   /// reaps an abandoned transaction lazily, so nothing else was ever going
   /// to release it. Because `db_instances`'s write lock is a single lock
   /// shared by every loaded database, not one per key, that indefinite wait
   /// blocked *every* database, not just the one being removed.
   ///
   /// Both assertions below are bounded well under `CLOSE_TIMEOUT` (5s) so
   /// this test fails fast, rather than hanging the test binary, if either
   /// half of the fix (transaction cleanup before teardown, or the
   /// `CLOSE_TIMEOUT` wrap around the whole operation) regresses.
   ///
   /// What this does *not* prove: with the fix in place, `remove()`'s
   /// critical section is fast enough (cleanup happens before the db lock is
   /// even taken) that this test cannot reliably force the OTHER-database
   /// query to land *inside* the brief window `remove()` still holds the
   /// lock. That's fine for detecting a regression - in a reverted build the
   /// lock is held for seconds, not microseconds, so the bounded query below
   /// would time out regardless of exact scheduling - but it means a passing
   /// run here doesn't demonstrate true concurrent interleaving on the fixed
   /// code path, only that neither operation is ever left waiting past its
   /// bound.
   #[test]
   fn test_remove_with_abandoned_transaction_does_not_stall_other_database() {
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

         // Begin an interruptible transaction on MAIN and never continue,
         // commit, or roll it back. This checks the write connection out of
         // MAIN's pool for good unless something reclaims it - here,
         // `remove()`'s own cleanup.
         commands::begin_interruptible_transaction(
            app.state::<DbInstances>(),
            app.state::<ActiveInterruptibleTransactions>(),
            "MAIN".to_string(),
            vec![Statement {
               query: "INSERT INTO test (val) VALUES (?)".to_string(),
               values: vec![serde_json::json!("abandoned")],
            }],
            None,
         )
         .await
         .expect("begin MAIN interruptible transaction should succeed");

         let app_for_remove = app.handle().clone();
         let remove_task = tokio::spawn(async move {
            commands::remove(
               app_for_remove.state::<DbInstances>(),
               app_for_remove.state::<ActiveSubscriptions>(),
               app_for_remove.state::<ObserverRegistrations>(),
               app_for_remove.state::<ActiveInterruptibleTransactions>(),
               app_for_remove.state::<ActiveRegularTransactions>(),
               "MAIN".to_string(),
            )
            .await
         });

         // Give the spawned remove() a chance to be scheduled and start
         // running before we race it with the OTHER-database query below -
         // mirrors the same dance in
         // `test_execute_transaction_returns_cancelled_when_database_closed`.
         tokio::time::sleep(std::time::Duration::from_millis(50)).await;

         let other_result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            commands::execute(
               app.state::<DbInstances>(),
               "OTHER".to_string(),
               "INSERT INTO test (val) VALUES ('unaffected')".to_string(),
               vec![],
               None,
            ),
         )
         .await;

         assert!(
            other_result.is_ok(),
            "a query on an unrelated database must not be blocked by remove() cleaning up \
             an abandoned transaction on a different database"
         );
         other_result
            .unwrap()
            .expect("OTHER query should succeed while MAIN is being removed");

         let removed = tokio::time::timeout(std::time::Duration::from_secs(2), remove_task)
            .await
            .expect(
               "remove() must not hang forever on an abandoned transaction - it should finish \
                well within its own CLOSE_TIMEOUT",
            )
            .expect("remove task should not panic")
            .expect("remove() should succeed once the abandoned transaction is cleaned up");
         assert!(removed, "MAIN was loaded and should have been removed");
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
            app.state::<ObserverRegistrations>(),
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

   /// Returns whether observation is currently enabled for `db_key`.
   async fn is_observing(app: &tauri::App<MockRuntime>, db_key: &str) -> bool {
      app.state::<DbInstances>()
         .inner()
         .inner
         .read()
         .await
         .get(db_key)
         .expect("database should be loaded")
         .is_observing()
   }

   /// Polls `db_instances`'s write lock via `try_write()` until some other
   /// task is holding it (i.e. our own `try_write()` starts failing), instead
   /// of assuming a fixed sleep gave a spawned task enough time to reach the
   /// point of contention.
   ///
   /// Used by the deterministic lock-order tests below: without this, a fixed
   /// sleep followed by a single timed acquisition attempt is a CI flake risk.
   /// If task scheduling is slow enough that the spawned command hasn't taken
   /// the db lock yet by the time the sleep ends, the acquisition attempt
   /// succeeds (there's nothing contending it yet) and the test fails for a
   /// reason that has nothing to do with the invariant it's guarding.
   /// Polling for the *first sign* of contention, with a
   /// generous overall budget, turns "was the task scheduled fast enough"
   /// from a pass/fail race into something we simply wait out.
   async fn wait_until_db_instances_write_contended(
      app: &tauri::App<MockRuntime>,
      budget: std::time::Duration,
   ) {
      let deadline = std::time::Instant::now() + budget;
      loop {
         if app
            .state::<DbInstances>()
            .inner()
            .inner
            .try_write()
            .is_err()
         {
            return;
         }
         assert!(
            std::time::Instant::now() < deadline,
            "db_instances write lock was never observed to be contended within {budget:?} - \
             the command under test may not have started, or may not be taking the lock at all"
         );
         tokio::time::sleep(std::time::Duration::from_millis(5)).await;
      }
   }

   /// Regression test for issue #54: re-calling `observe()` (from the same or a
   /// different window) must not tear down subscriptions that were already active.
   /// A subscriber created before a second `observe()` call - with a *different*
   /// table set - must still receive events published after that second call.
   #[test]
   fn test_observe_reenable_with_different_tables_preserves_existing_subscriber() {
      let temp_dir = tempfile::tempdir().unwrap();
      let db_path = validate::validate_database_path(temp_dir.path().join("main.db")).unwrap();
      let key = "MAIN".to_string();

      tauri::async_runtime::block_on(async {
         let (app, _) =
            tokio::task::spawn_blocking(move || init_app_with_registered_db_at_path(&key, db_path))
               .await
               .expect("plugin init task should succeed");

         load_and_create_test_table(&app, "MAIN").await;
         commands::execute(
            app.state::<DbInstances>(),
            "MAIN".to_string(),
            "CREATE TABLE other (id INTEGER PRIMARY KEY, val TEXT)".to_string(),
            vec![],
            None,
         )
         .await
         .expect("create other table should succeed");

         let webview = tauri::WebviewWindowBuilder::new(&app, "window-a", Default::default())
            .build()
            .expect("webview window should build");

         commands::observe(
            app.state::<DbInstances>(),
            app.state::<ObserverRegistrations>(),
            webview.as_ref().clone(),
            "MAIN".to_string(),
            vec!["test".to_string()],
            None,
         )
         .await
         .expect("first observe should succeed");

         let (tx, rx) = std::sync::mpsc::channel::<serde_json::Value>();
         let channel = tauri::ipc::Channel::new(move |body| {
            let value: serde_json::Value = body
               .deserialize()
               .expect("payload should deserialize as JSON");
            tx.send(value).ok();
            Ok(())
         });

         commands::subscribe(
            app.state::<DbInstances>(),
            app.state::<ActiveSubscriptions>(),
            app.state::<ObserverRegistrations>(),
            webview.as_ref().clone(),
            "MAIN".to_string(),
            vec!["test".to_string()],
            channel,
         )
         .await
         .expect("subscribe should succeed");

         // Re-observe with a DIFFERENT table set. Under the old (destructive)
         // behavior, this would drop the broker and silently end the
         // subscription created above.
         commands::observe(
            app.state::<DbInstances>(),
            app.state::<ObserverRegistrations>(),
            webview.as_ref().clone(),
            "MAIN".to_string(),
            vec!["other".to_string()],
            None,
         )
         .await
         .expect("second observe should succeed");

         commands::execute(
            app.state::<DbInstances>(),
            "MAIN".to_string(),
            "INSERT INTO test (val) VALUES ('hello')".to_string(),
            vec![],
            None,
         )
         .await
         .expect("insert should succeed");

         let received = tokio::task::spawn_blocking(move || {
            rx.recv_timeout(std::time::Duration::from_millis(500))
         })
         .await
         .expect("blocking recv task should not panic");

         let payload =
            received.expect("subscriber should still receive events after the second observe()");
         assert_eq!(payload["event"], "change");
         assert_eq!(payload["data"]["table"], "test");
      });
   }

   /// `subscribe()` forwards `tables` into the same shared broker's
   /// `observe_tables()` that `observe()` does (see `MAX_OBSERVED_TABLES`'s doc
   /// comment in `src/commands.rs`), so it must be bounded the same way -
   /// otherwise a single `subscribe()` call could grow the observed set past
   /// the limit `observe()` enforces. Unlike `observe()`, an empty `tables` is
   /// valid on `subscribe()` (it means "no filter"), so only the upper bound
   /// is exercised here.
   #[test]
   fn test_subscribe_rejects_too_many_tables() {
      let temp_dir = tempfile::tempdir().unwrap();
      let db_path = validate::validate_database_path(temp_dir.path().join("main.db")).unwrap();
      let key = "MAIN".to_string();

      tauri::async_runtime::block_on(async {
         let (app, _) =
            tokio::task::spawn_blocking(move || init_app_with_registered_db_at_path(&key, db_path))
               .await
               .expect("plugin init task should succeed");

         load_and_create_test_table(&app, "MAIN").await;

         let webview = tauri::WebviewWindowBuilder::new(&app, "window-a", Default::default())
            .build()
            .expect("webview window should build");

         commands::observe(
            app.state::<DbInstances>(),
            app.state::<ObserverRegistrations>(),
            webview.as_ref().clone(),
            "MAIN".to_string(),
            vec!["test".to_string()],
            None,
         )
         .await
         .expect("observe should succeed");

         // One past MAX_OBSERVED_TABLES (100).
         let too_many_tables: Vec<String> = (0..101).map(|i| format!("table_{i}")).collect();
         let channel = tauri::ipc::Channel::new(|_body| Ok(()));

         let err = commands::subscribe(
            app.state::<DbInstances>(),
            app.state::<ActiveSubscriptions>(),
            app.state::<ObserverRegistrations>(),
            webview.as_ref().clone(),
            "MAIN".to_string(),
            too_many_tables,
            channel,
         )
         .await
         .expect_err("subscribe should reject a request over MAX_OBSERVED_TABLES");

         assert!(matches!(err, Error::InvalidConfig(_)));
      });
   }

   /// Refcount teardown boundary: the broker stays live while at least one
   /// window is registered as an observer, and is only torn down once the last
   /// registered window releases via `unobserve()`. A non-final `unobserve()`
   /// must leave every other window's subscriptions running untouched (#54);
   /// only the final `unobserve()` - the one that drops the refcount to zero -
   /// aborts them.
   #[test]
   fn test_unobserve_refcount_teardown_boundary() {
      let temp_dir = tempfile::tempdir().unwrap();
      let db_path = validate::validate_database_path(temp_dir.path().join("main.db")).unwrap();
      let key = "MAIN".to_string();

      tauri::async_runtime::block_on(async {
         let (app, _) =
            tokio::task::spawn_blocking(move || init_app_with_registered_db_at_path(&key, db_path))
               .await
               .expect("plugin init task should succeed");

         load_and_create_test_table(&app, "MAIN").await;

         let webview_a = tauri::WebviewWindowBuilder::new(&app, "window-a", Default::default())
            .build()
            .expect("webview window A should build");
         let webview_b = tauri::WebviewWindowBuilder::new(&app, "window-b", Default::default())
            .build()
            .expect("webview window B should build");

         commands::observe(
            app.state::<DbInstances>(),
            app.state::<ObserverRegistrations>(),
            webview_a.as_ref().clone(),
            "MAIN".to_string(),
            vec!["test".to_string()],
            None,
         )
         .await
         .expect("window A observe should succeed");

         commands::observe(
            app.state::<DbInstances>(),
            app.state::<ObserverRegistrations>(),
            webview_b.as_ref().clone(),
            "MAIN".to_string(),
            vec!["test".to_string()],
            None,
         )
         .await
         .expect("window B observe should succeed");

         assert!(
            is_observing(&app, "MAIN").await,
            "broker should be live with two registered observers"
         );

         // Window A subscribes before releasing its observer registration, so
         // window A's own (non-final) unobserve() below can be checked for not
         // aborting a subscription that isn't its own to tear down.
         let (tx, rx) = std::sync::mpsc::channel::<serde_json::Value>();
         let channel = tauri::ipc::Channel::new(move |body| {
            let value: serde_json::Value = body
               .deserialize()
               .expect("payload should deserialize as JSON");
            tx.send(value).ok();
            Ok(())
         });

         commands::subscribe(
            app.state::<DbInstances>(),
            app.state::<ActiveSubscriptions>(),
            app.state::<ObserverRegistrations>(),
            webview_a.as_ref().clone(),
            "MAIN".to_string(),
            vec!["test".to_string()],
            channel,
         )
         .await
         .expect("subscribe on window A should succeed");

         commands::unobserve(
            app.state::<DbInstances>(),
            app.state::<ActiveSubscriptions>(),
            app.state::<ObserverRegistrations>(),
            webview_a.as_ref().clone(),
            "MAIN".to_string(),
         )
         .await
         .expect("window A unobserve should succeed");

         assert!(
            is_observing(&app, "MAIN").await,
            "broker should stay live while window B is still registered (rc=1)"
         );

         // Timing-free: catches a non-final unobserve() aborting subscriptions
         // it doesn't own (moving/duplicating the `remove_for_db` call above
         // the `remaining > 0` early return - see #54).
         assert_eq!(
            app.state::<ActiveSubscriptions>()
               .count_for_db("MAIN")
               .await,
            1,
            "a non-final unobserve() must not abort another window's subscription (#54)"
         );

         // Real round-trip: the only thing proving events still actually flow
         // to the surviving subscription after window A's unobserve().
         commands::execute(
            app.state::<DbInstances>(),
            "MAIN".to_string(),
            "INSERT INTO test (val) VALUES ('hello')".to_string(),
            vec![],
            None,
         )
         .await
         .expect("insert should succeed");

         let received =
            tokio::task::spawn_blocking(move || rx.recv_timeout(std::time::Duration::from_secs(2)))
               .await
               .expect("blocking recv task should not panic");

         let payload = received
            .expect("subscription should still receive events after a non-final unobserve()");
         assert_eq!(payload["event"], "change");
         assert_eq!(payload["data"]["table"], "test");

         commands::unobserve(
            app.state::<DbInstances>(),
            app.state::<ActiveSubscriptions>(),
            app.state::<ObserverRegistrations>(),
            webview_b.as_ref().clone(),
            "MAIN".to_string(),
         )
         .await
         .expect("window B unobserve should succeed");

         assert!(
            !is_observing(&app, "MAIN").await,
            "broker should be torn down once the last observer releases (rc=0)"
         );

         // Catches deleting the `remove_for_db` call entirely: without it,
         // nothing asserts the final unobserve() aborts subscriptions. Not
         // deterministic, though: subscribe()'s own forwarding task self-reaps
         // its `active_subs` entry once its stream ends (see the reap comment
         // in `commands::subscribe`), and tearing down the broker here also
         // ends that stream - so the self-reaper can independently drive this
         // count to 0 even with `remove_for_db` deleted, racing the assertion
         // below. Measured detection of that mutation: 9 of 10 runs.
         assert_eq!(
            app.state::<ActiveSubscriptions>()
               .count_for_db("MAIN")
               .await,
            0,
            "the last unobserve() must abort remaining subscriptions"
         );
      });
   }

   /// A second window's explicit `captureValues` request that conflicts with
   /// the value already active for a database must be rejected with
   /// `OBSERVATION_CONFIG_CONFLICT`, without mutating the live broker or
   /// recording that window's registration - the check must return before
   /// `register()` runs.
   #[test]
   fn test_observe_conflicting_capture_values_rejected() {
      let temp_dir = tempfile::tempdir().unwrap();
      let db_path = validate::validate_database_path(temp_dir.path().join("main.db")).unwrap();
      let key = "MAIN".to_string();

      tauri::async_runtime::block_on(async {
         let (app, _) =
            tokio::task::spawn_blocking(move || init_app_with_registered_db_at_path(&key, db_path))
               .await
               .expect("plugin init task should succeed");

         load_and_create_test_table(&app, "MAIN").await;

         let webview_a = tauri::WebviewWindowBuilder::new(&app, "window-a", Default::default())
            .build()
            .expect("webview window A should build");
         let webview_b = tauri::WebviewWindowBuilder::new(&app, "window-b", Default::default())
            .build()
            .expect("webview window B should build");

         commands::observe(
            app.state::<DbInstances>(),
            app.state::<ObserverRegistrations>(),
            webview_a.as_ref().clone(),
            "MAIN".to_string(),
            vec!["test".to_string()],
            None,
         )
         .await
         .expect("window A observe with default config should succeed");

         let err = commands::observe(
            app.state::<DbInstances>(),
            app.state::<ObserverRegistrations>(),
            webview_b.as_ref().clone(),
            "MAIN".to_string(),
            vec!["test".to_string()],
            Some(ObserverConfigParams {
               channel_capacity: None,
               capture_values: Some(false),
            }),
         )
         .await
         .expect_err("conflicting captureValues should be rejected");

         assert!(matches!(err, Error::ObservationConfigConflict(_)));

         let (capacity, capture) = {
            let instances = app.state::<DbInstances>().inner().inner.read().await;
            let observable = instances
               .get("MAIN")
               .expect("MAIN should be loaded")
               .observable()
               .expect("observation should be enabled");
            (
               observable.broker().channel_capacity(),
               observable.broker().capture_values(),
            )
         };
         assert_eq!(
            capacity, 256,
            "the live broker's channelCapacity must be unchanged by a rejected observe()"
         );
         assert!(
            capture,
            "the live broker's captureValues must be unchanged by a rejected observe()"
         );

         assert!(
            !app
               .state::<ObserverRegistrations>()
               .is_registered("MAIN", webview_b.label())
               .await,
            "window B's registration must not be recorded when its observe() is rejected"
         );
      });
   }

   /// Same as `test_observe_conflicting_capture_values_rejected`, but for a
   /// conflicting `channelCapacity` request.
   #[test]
   fn test_observe_conflicting_channel_capacity_rejected() {
      let temp_dir = tempfile::tempdir().unwrap();
      let db_path = validate::validate_database_path(temp_dir.path().join("main.db")).unwrap();
      let key = "MAIN".to_string();

      tauri::async_runtime::block_on(async {
         let (app, _) =
            tokio::task::spawn_blocking(move || init_app_with_registered_db_at_path(&key, db_path))
               .await
               .expect("plugin init task should succeed");

         load_and_create_test_table(&app, "MAIN").await;

         let webview_a = tauri::WebviewWindowBuilder::new(&app, "window-a", Default::default())
            .build()
            .expect("webview window A should build");
         let webview_b = tauri::WebviewWindowBuilder::new(&app, "window-b", Default::default())
            .build()
            .expect("webview window B should build");

         commands::observe(
            app.state::<DbInstances>(),
            app.state::<ObserverRegistrations>(),
            webview_a.as_ref().clone(),
            "MAIN".to_string(),
            vec!["test".to_string()],
            None,
         )
         .await
         .expect("window A observe with default config should succeed");

         let err = commands::observe(
            app.state::<DbInstances>(),
            app.state::<ObserverRegistrations>(),
            webview_b.as_ref().clone(),
            "MAIN".to_string(),
            vec!["test".to_string()],
            Some(ObserverConfigParams {
               channel_capacity: Some(512),
               capture_values: None,
            }),
         )
         .await
         .expect_err("conflicting channelCapacity should be rejected");

         assert!(matches!(err, Error::ObservationConfigConflict(_)));

         let (capacity, capture) = {
            let instances = app.state::<DbInstances>().inner().inner.read().await;
            let observable = instances
               .get("MAIN")
               .expect("MAIN should be loaded")
               .observable()
               .expect("observation should be enabled");
            (
               observable.broker().channel_capacity(),
               observable.broker().capture_values(),
            )
         };
         assert_eq!(
            capacity, 256,
            "the live broker's channelCapacity must be unchanged by a rejected observe()"
         );
         assert!(
            capture,
            "the live broker's captureValues must be unchanged by a rejected observe()"
         );

         assert!(
            !app
               .state::<ObserverRegistrations>()
               .is_registered("MAIN", webview_b.label())
               .await,
            "window B's registration must not be recorded when its observe() is rejected"
         );
      });
   }

   /// An explicit config that matches the live broker's already-active values
   /// exactly is not a conflict and must succeed.
   #[test]
   fn test_observe_identical_explicit_config_succeeds() {
      let temp_dir = tempfile::tempdir().unwrap();
      let db_path = validate::validate_database_path(temp_dir.path().join("main.db")).unwrap();
      let key = "MAIN".to_string();

      tauri::async_runtime::block_on(async {
         let (app, _) =
            tokio::task::spawn_blocking(move || init_app_with_registered_db_at_path(&key, db_path))
               .await
               .expect("plugin init task should succeed");

         load_and_create_test_table(&app, "MAIN").await;

         let webview_a = tauri::WebviewWindowBuilder::new(&app, "window-a", Default::default())
            .build()
            .expect("webview window A should build");
         let webview_b = tauri::WebviewWindowBuilder::new(&app, "window-b", Default::default())
            .build()
            .expect("webview window B should build");

         commands::observe(
            app.state::<DbInstances>(),
            app.state::<ObserverRegistrations>(),
            webview_a.as_ref().clone(),
            "MAIN".to_string(),
            vec!["test".to_string()],
            None,
         )
         .await
         .expect("window A observe with default config should succeed");

         commands::observe(
            app.state::<DbInstances>(),
            app.state::<ObserverRegistrations>(),
            webview_b.as_ref().clone(),
            "MAIN".to_string(),
            vec!["test".to_string()],
            Some(ObserverConfigParams {
               channel_capacity: Some(256),
               capture_values: Some(true),
            }),
         )
         .await
         .expect("explicit config identical to the live broker's values should succeed");

         assert!(
            app.state::<ObserverRegistrations>()
               .is_registered("MAIN", webview_b.label())
               .await,
            "window B's registration must be recorded once its observe() succeeds"
         );
      });
   }

   /// Guards the `seeded` block in `observe()`: once window A has enabled
   /// observation with a non-default explicit config, a second window calling
   /// `observe()` with `config: None` must succeed - its defaults must never be
   /// compared against the live broker's (already non-default) values as if
   /// they were an explicit, conflicting request.
   #[test]
   fn test_observe_omitted_config_after_explicit_config_succeeds() {
      let temp_dir = tempfile::tempdir().unwrap();
      let db_path = validate::validate_database_path(temp_dir.path().join("main.db")).unwrap();
      let key = "MAIN".to_string();

      tauri::async_runtime::block_on(async {
         let (app, _) =
            tokio::task::spawn_blocking(move || init_app_with_registered_db_at_path(&key, db_path))
               .await
               .expect("plugin init task should succeed");

         load_and_create_test_table(&app, "MAIN").await;

         let webview_a = tauri::WebviewWindowBuilder::new(&app, "window-a", Default::default())
            .build()
            .expect("webview window A should build");
         let webview_b = tauri::WebviewWindowBuilder::new(&app, "window-b", Default::default())
            .build()
            .expect("webview window B should build");

         commands::observe(
            app.state::<DbInstances>(),
            app.state::<ObserverRegistrations>(),
            webview_a.as_ref().clone(),
            "MAIN".to_string(),
            vec!["test".to_string()],
            Some(ObserverConfigParams {
               channel_capacity: Some(512),
               capture_values: Some(false),
            }),
         )
         .await
         .expect("window A observe with explicit non-default config should succeed");

         commands::observe(
            app.state::<DbInstances>(),
            app.state::<ObserverRegistrations>(),
            webview_b.as_ref().clone(),
            "MAIN".to_string(),
            vec!["test".to_string()],
            None,
         )
         .await
         .expect("window B observe with config: None should succeed");

         assert!(
            app.state::<ObserverRegistrations>()
               .is_registered("MAIN", webview_b.label())
               .await,
            "window B's registration must be recorded once its observe() succeeds"
         );
      });
   }

   /// `subscribe()` must reject a window that never called `observe()` for
   /// `db_key` itself with `OBSERVATION_NOT_ENABLED`, even while another
   /// window's registration keeps a broker active for that database (#54),
   /// and must not disturb that other window's already-established
   /// subscription.
   #[test]
   fn test_subscribe_without_observe_rejected() {
      let temp_dir = tempfile::tempdir().unwrap();
      let db_path = validate::validate_database_path(temp_dir.path().join("main.db")).unwrap();
      let key = "MAIN".to_string();

      tauri::async_runtime::block_on(async {
         let (app, _) =
            tokio::task::spawn_blocking(move || init_app_with_registered_db_at_path(&key, db_path))
               .await
               .expect("plugin init task should succeed");

         load_and_create_test_table(&app, "MAIN").await;

         let webview_a = tauri::WebviewWindowBuilder::new(&app, "window-a", Default::default())
            .build()
            .expect("webview window A should build");
         let webview_b = tauri::WebviewWindowBuilder::new(&app, "window-b", Default::default())
            .build()
            .expect("webview window B should build");

         commands::observe(
            app.state::<DbInstances>(),
            app.state::<ObserverRegistrations>(),
            webview_a.as_ref().clone(),
            "MAIN".to_string(),
            vec!["test".to_string()],
            None,
         )
         .await
         .expect("window A observe should succeed");

         let (tx, rx) = std::sync::mpsc::channel::<serde_json::Value>();
         let channel = tauri::ipc::Channel::new(move |body| {
            let value: serde_json::Value = body
               .deserialize()
               .expect("payload should deserialize as JSON");
            tx.send(value).ok();
            Ok(())
         });

         commands::subscribe(
            app.state::<DbInstances>(),
            app.state::<ActiveSubscriptions>(),
            app.state::<ObserverRegistrations>(),
            webview_a.as_ref().clone(),
            "MAIN".to_string(),
            vec!["test".to_string()],
            channel,
         )
         .await
         .expect("subscribe on window A, which observed, should succeed");

         // Window B never called observe() - the broker is only live because
         // of window A's registration.
         let (tx_b, _rx_b) = std::sync::mpsc::channel::<serde_json::Value>();
         let channel_b = tauri::ipc::Channel::new(move |body| {
            let value: serde_json::Value = body
               .deserialize()
               .expect("payload should deserialize as JSON");
            tx_b.send(value).ok();
            Ok(())
         });

         let err = commands::subscribe(
            app.state::<DbInstances>(),
            app.state::<ActiveSubscriptions>(),
            app.state::<ObserverRegistrations>(),
            webview_b.as_ref().clone(),
            "MAIN".to_string(),
            vec!["test".to_string()],
            channel_b,
         )
         .await
         .expect_err("subscribe from a window that never observed should be rejected");

         assert!(matches!(err, Error::ObservationNotEnabled(_)));

         assert_eq!(
            app.state::<ActiveSubscriptions>()
               .count_for_db("MAIN")
               .await,
            1,
            "window A's subscription must be unaffected by window B's rejected subscribe()"
         );

         commands::execute(
            app.state::<DbInstances>(),
            "MAIN".to_string(),
            "INSERT INTO test (val) VALUES ('hello')".to_string(),
            vec![],
            None,
         )
         .await
         .expect("insert should succeed");

         let received =
            tokio::task::spawn_blocking(move || rx.recv_timeout(std::time::Duration::from_secs(2)))
               .await
               .expect("blocking recv task should not panic");

         let payload = received
            .expect("window A's subscription should still receive events after window B's rejected subscribe()");
         assert_eq!(payload["event"], "change");
         assert_eq!(payload["data"]["table"], "test");
      });
   }

   /// Lock-order regression guard: `observe()`/`unobserve()` must hold a single lock
   /// (db_instances, then observer_regs) across their whole enable/register or
   /// release/disable sequence, or a concurrent pair on different webviews can
   /// interleave such that `is_observing()` and "has registrations" disagree -
   /// e.g. a window's `observe()` registers into a broker a concurrent
   /// `unobserve()` from another window just destroyed.
   ///
   /// This is a probabilistic guard, not a proof, and its detection rate is
   /// asymmetric: a regressed `unobserve()` side fails reliably, but a regressed
   /// `observe()` side has been measured as low as 1 in 10 runs. `ITERATIONS` is
   /// 2000 because more attempts are the only lever available against a window
   /// this narrow, not because a higher count is known to detect more - that has
   /// not been demonstrated. At ~0.14s the attempts are cheap either way.
   /// Re-measure before lowering it, and do not rely on this test alone.
   ///
   /// Do not delete this test as redundant. It is the *only* guard for a
   /// distinct regression the other two structurally cannot see: releasing the
   /// db guard mid-sequence and immediately reacquiring a fresh one before
   /// `register()`/`release()`. That shape compiles (a real guard satisfies the
   /// `DbInstancesGuard` witness) and passes both deterministic tests 3/3 (a
   /// guard genuinely *is* held at the call), yet still reopens the race - a
   /// concurrent `unobserve()` can take the db lock in the drop/reacquire
   /// window, see the refcount reach zero, and tear the broker down before
   /// `observe()` reacquires and registers. Mutation testing measured this
   /// test failing 5/5 against that shape while both deterministic guards
   /// passed 3/3.
   #[test]
   fn test_concurrent_observe_and_unobserve_keep_broker_and_registrations_in_sync() {
      let temp_dir = tempfile::tempdir().unwrap();
      let db_path = validate::validate_database_path(temp_dir.path().join("main.db")).unwrap();
      let key = "MAIN".to_string();

      tauri::async_runtime::block_on(async {
         let (app, _) =
            tokio::task::spawn_blocking(move || init_app_with_registered_db_at_path(&key, db_path))
               .await
               .expect("plugin init task should succeed");

         load_and_create_test_table(&app, "MAIN").await;

         let webview_a = tauri::WebviewWindowBuilder::new(&app, "window-a", Default::default())
            .build()
            .expect("webview window A should build");
         let webview_b = tauri::WebviewWindowBuilder::new(&app, "window-b", Default::default())
            .build()
            .expect("webview window B should build");

         const ITERATIONS: usize = 2000;

         for i in 0..ITERATIONS {
            // Baseline for this iteration: only window A registered, broker
            // live. Force both released first (no-ops if already released)
            // so each iteration starts from a known state regardless of how
            // the previous iteration's race resolved.
            let _ = commands::unobserve(
               app.state::<DbInstances>(),
               app.state::<ActiveSubscriptions>(),
               app.state::<ObserverRegistrations>(),
               webview_a.as_ref().clone(),
               "MAIN".to_string(),
            )
            .await;
            let _ = commands::unobserve(
               app.state::<DbInstances>(),
               app.state::<ActiveSubscriptions>(),
               app.state::<ObserverRegistrations>(),
               webview_b.as_ref().clone(),
               "MAIN".to_string(),
            )
            .await;
            commands::observe(
               app.state::<DbInstances>(),
               app.state::<ObserverRegistrations>(),
               webview_a.as_ref().clone(),
               "MAIN".to_string(),
               vec!["test".to_string()],
               None,
            )
            .await
            .expect("baseline observe for window A should succeed");

            // Race: window B observes while window A unobserves, concurrently,
            // on separate spawned tasks so the multi-thread runtime can
            // actually run them in parallel rather than just interleaving at
            // await points on one thread.
            let app_for_observe = app.handle().clone();
            let webview_b_for_task = webview_b.as_ref().clone();
            let observe_task = tokio::spawn(async move {
               commands::observe(
                  app_for_observe.state::<DbInstances>(),
                  app_for_observe.state::<ObserverRegistrations>(),
                  webview_b_for_task,
                  "MAIN".to_string(),
                  vec!["test".to_string()],
                  None,
               )
               .await
            });

            let app_for_unobserve = app.handle().clone();
            let webview_a_for_task = webview_a.as_ref().clone();
            let unobserve_task = tokio::spawn(async move {
               commands::unobserve(
                  app_for_unobserve.state::<DbInstances>(),
                  app_for_unobserve.state::<ActiveSubscriptions>(),
                  app_for_unobserve.state::<ObserverRegistrations>(),
                  webview_a_for_task,
                  "MAIN".to_string(),
               )
               .await
            });

            observe_task
               .await
               .expect("observe task should not panic")
               .expect("observe should succeed");
            unobserve_task
               .await
               .expect("unobserve task should not panic")
               .expect("unobserve should succeed");

            let has_registrations = app
               .state::<ObserverRegistrations>()
               .count_for_db("MAIN")
               .await
               > 0;
            let observing = is_observing(&app, "MAIN").await;

            assert_eq!(
               observing, has_registrations,
               "iteration {i}: is_observing() ({observing}) and has-registrations \
                ({has_registrations}) must always agree"
            );
         }
      });
   }

   /// Deterministic lock-order guard, observe side.
   ///
   /// Forces the exact contention the lock-order invariant depends on instead
   /// of relying on scheduling luck: the test itself holds `observer_regs`'s
   /// lock (via the test-only `lock_for_test()` accessor), so `observe()`'s
   /// `register()` call is guaranteed to block. If `observe()` is correctly
   /// holding the `db_instances` lock across that whole sequence, its lock
   /// guard is still alive while blocked - so this test's own attempt to
   /// acquire that same lock (with a short timeout) MUST fail. A regressed
   /// `observe()` that drops the db lock before calling `register()` would let
   /// this acquisition succeed instead.
   #[test]
   fn test_observe_holds_db_lock_across_register() {
      let temp_dir = tempfile::tempdir().unwrap();
      let db_path = validate::validate_database_path(temp_dir.path().join("main.db")).unwrap();
      let key = "MAIN".to_string();

      tauri::async_runtime::block_on(async {
         let (app, _) =
            tokio::task::spawn_blocking(move || init_app_with_registered_db_at_path(&key, db_path))
               .await
               .expect("plugin init task should succeed");

         load_and_create_test_table(&app, "MAIN").await;

         let webview = tauri::WebviewWindowBuilder::new(&app, "window-a", Default::default())
            .build()
            .expect("webview window should build");

         // Hold observer_regs' lock ourselves so observe()'s register() call
         // is guaranteed to block on it.
         let observer_regs_state = app.state::<ObserverRegistrations>();
         let regs_guard = observer_regs_state.lock_for_test().await;

         let app_for_observe = app.handle().clone();
         let webview_for_task = webview.as_ref().clone();
         let observe_task = tokio::spawn(async move {
            commands::observe(
               app_for_observe.state::<DbInstances>(),
               app_for_observe.state::<ObserverRegistrations>(),
               webview_for_task,
               "MAIN".to_string(),
               vec!["test".to_string()],
               None,
            )
            .await
         });

         // Wait for the spawned task to actually reach the point of
         // contention (acquire the db lock, call enable_observation, reach
         // register(), and block on the regs lock we're holding), rather than
         // assuming a fixed sleep was long enough.
         wait_until_db_instances_write_contended(&app, std::time::Duration::from_secs(2)).await;

         let db_lock_attempt = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            app.state::<DbInstances>().inner().inner.write(),
         )
         .await;

         assert!(
            db_lock_attempt.is_err(),
            "observe() must still be holding the db_instances lock while blocked on \
             observer_regs.register() - if this acquisition succeeded, observe() had \
             already dropped the db lock before registering (the exact regression this \
             test guards against)"
         );

         // Release the regs lock so observe() can finish, then clean up.
         drop(regs_guard);

         observe_task
            .await
            .expect("observe task should not panic")
            .expect("observe should succeed");
      });
   }

   /// Deterministic lock-order guard, unobserve side.
   ///
   /// Same technique as `test_observe_holds_db_lock_across_register`, but for
   /// `unobserve()`'s db-lock-then-release() ordering.
   #[test]
   fn test_unobserve_holds_db_lock_across_release() {
      let temp_dir = tempfile::tempdir().unwrap();
      let db_path = validate::validate_database_path(temp_dir.path().join("main.db")).unwrap();
      let key = "MAIN".to_string();

      tauri::async_runtime::block_on(async {
         let (app, _) =
            tokio::task::spawn_blocking(move || init_app_with_registered_db_at_path(&key, db_path))
               .await
               .expect("plugin init task should succeed");

         load_and_create_test_table(&app, "MAIN").await;

         let webview = tauri::WebviewWindowBuilder::new(&app, "window-a", Default::default())
            .build()
            .expect("webview window should build");

         commands::observe(
            app.state::<DbInstances>(),
            app.state::<ObserverRegistrations>(),
            webview.as_ref().clone(),
            "MAIN".to_string(),
            vec!["test".to_string()],
            None,
         )
         .await
         .expect("baseline observe should succeed");

         // Hold observer_regs' lock ourselves so unobserve()'s release() call
         // is guaranteed to block on it.
         let observer_regs_state = app.state::<ObserverRegistrations>();
         let regs_guard = observer_regs_state.lock_for_test().await;

         let app_for_unobserve = app.handle().clone();
         let webview_for_task = webview.as_ref().clone();
         let unobserve_task = tokio::spawn(async move {
            commands::unobserve(
               app_for_unobserve.state::<DbInstances>(),
               app_for_unobserve.state::<ActiveSubscriptions>(),
               app_for_unobserve.state::<ObserverRegistrations>(),
               webview_for_task,
               "MAIN".to_string(),
            )
            .await
         });

         wait_until_db_instances_write_contended(&app, std::time::Duration::from_secs(2)).await;

         let db_lock_attempt = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            app.state::<DbInstances>().inner().inner.write(),
         )
         .await;

         assert!(
            db_lock_attempt.is_err(),
            "unobserve() must still be holding the db_instances lock while blocked on \
             observer_regs.release() - if this acquisition succeeded, unobserve() had \
             already released the db lock before calling release() (the exact \
             regression this test guards against)"
         );

         drop(regs_guard);

         unobserve_task
            .await
            .expect("unobserve task should not panic")
            .expect("unobserve should succeed");
      });
   }

   /// Closing a database fully clears its observer registrations (via
   /// `ObserverRegistrations::clear_for_db`, wired through
   /// `close_database_inner`), so a fresh `observe()` after a `close()`+reload
   /// cycle restarts the refcount at 1 rather than inheriting stale entries
   /// from before the close.
   #[test]
   fn test_observe_after_close_restarts_refcount() {
      let temp_dir = tempfile::tempdir().unwrap();
      let db_path = validate::validate_database_path(temp_dir.path().join("main.db")).unwrap();
      let key = "MAIN".to_string();

      tauri::async_runtime::block_on(async {
         let (app, _) =
            tokio::task::spawn_blocking(move || init_app_with_registered_db_at_path(&key, db_path))
               .await
               .expect("plugin init task should succeed");

         load_and_create_test_table(&app, "MAIN").await;

         let webview = tauri::WebviewWindowBuilder::new(&app, "window-a", Default::default())
            .build()
            .expect("webview window should build");

         commands::observe(
            app.state::<DbInstances>(),
            app.state::<ObserverRegistrations>(),
            webview.as_ref().clone(),
            "MAIN".to_string(),
            vec!["test".to_string()],
            None,
         )
         .await
         .expect("observe should succeed");

         assert_eq!(
            app.state::<ObserverRegistrations>()
               .count_for_db("MAIN")
               .await,
            1
         );

         commands::close(
            app.state::<DbInstances>(),
            app.state::<ActiveSubscriptions>(),
            app.state::<ObserverRegistrations>(),
            app.state::<ActiveInterruptibleTransactions>(),
            app.state::<ActiveRegularTransactions>(),
            "MAIN".to_string(),
         )
         .await
         .expect("close should succeed");

         assert_eq!(
            app.state::<ObserverRegistrations>()
               .count_for_db("MAIN")
               .await,
            0,
            "close() should clear stale observer registrations, not just the crate-level broker"
         );

         // Reload and recreate the table (close() dropped the connection pool;
         // the underlying file-backed table itself still exists on disk, but
         // a fresh wrapper needs to be loaded before observe() will find it).
         connect_to_database(app.handle(), "MAIN", None)
            .await
            .expect("reconnect after close should succeed");

         commands::observe(
            app.state::<DbInstances>(),
            app.state::<ObserverRegistrations>(),
            webview.as_ref().clone(),
            "MAIN".to_string(),
            vec!["test".to_string()],
            None,
         )
         .await
         .expect("observe after reload should succeed");

         assert_eq!(
            app.state::<ObserverRegistrations>()
               .count_for_db("MAIN")
               .await,
            1,
            "refcount should restart at 1, not inherit anything from before the close"
         );
         assert!(is_observing(&app, "MAIN").await);
      });
   }

   /// A window calling `observe()` twice (e.g. to add more tables) still only
   /// holds ONE registration, so a single `unobserve()` call from that same
   /// window fully tears the broker down - the refcount
   /// tracks distinct webviews, not the number of `observe()` calls made.
   #[test]
   fn test_same_window_double_observe_then_single_unobserve_tears_down() {
      let temp_dir = tempfile::tempdir().unwrap();
      let db_path = validate::validate_database_path(temp_dir.path().join("main.db")).unwrap();
      let key = "MAIN".to_string();

      tauri::async_runtime::block_on(async {
         let (app, _) =
            tokio::task::spawn_blocking(move || init_app_with_registered_db_at_path(&key, db_path))
               .await
               .expect("plugin init task should succeed");

         load_and_create_test_table(&app, "MAIN").await;
         commands::execute(
            app.state::<DbInstances>(),
            "MAIN".to_string(),
            "CREATE TABLE other (id INTEGER PRIMARY KEY, val TEXT)".to_string(),
            vec![],
            None,
         )
         .await
         .expect("create other table should succeed");

         let webview = tauri::WebviewWindowBuilder::new(&app, "window-a", Default::default())
            .build()
            .expect("webview window should build");

         commands::observe(
            app.state::<DbInstances>(),
            app.state::<ObserverRegistrations>(),
            webview.as_ref().clone(),
            "MAIN".to_string(),
            vec!["test".to_string()],
            None,
         )
         .await
         .expect("first observe should succeed");

         // Same window, second call, different tables - must not inflate the
         // refcount for this window.
         commands::observe(
            app.state::<DbInstances>(),
            app.state::<ObserverRegistrations>(),
            webview.as_ref().clone(),
            "MAIN".to_string(),
            vec!["other".to_string()],
            None,
         )
         .await
         .expect("second observe should succeed");

         assert_eq!(
            app.state::<ObserverRegistrations>()
               .count_for_db("MAIN")
               .await,
            1,
            "same window calling observe() twice must still be a single registration"
         );

         commands::unobserve(
            app.state::<DbInstances>(),
            app.state::<ActiveSubscriptions>(),
            app.state::<ObserverRegistrations>(),
            webview.as_ref().clone(),
            "MAIN".to_string(),
         )
         .await
         .expect("unobserve should succeed");

         assert!(
            !is_observing(&app, "MAIN").await,
            "a single unobserve() from the only registered window must fully tear down the broker"
         );
      });
   }

   /// Probabilistic regression guard for the close()-vs-observe() variant of
   /// the lock-order invariant: `close_database_inner` must clear `observer_regs`
   /// while still holding the `db_instances` write lock used to remove the
   /// wrapper (not before acquiring it), or a concurrent `observe()` from
   /// another window can register into - and enable observation on - a
   /// wrapper that's about to be removed, leaving a phantom registration
   /// behind after `close()` completes with no wrapper left for it to refer
   /// to. `close()` always removes the wrapper by the time it returns, so the
   /// database is deterministically unloaded after each iteration; what's
   /// racy is only whether a registration is left behind.
   #[test]
   fn test_concurrent_observe_and_close_do_not_leak_registrations() {
      let temp_dir = tempfile::tempdir().unwrap();
      let db_path = validate::validate_database_path(temp_dir.path().join("main.db")).unwrap();
      let key = "MAIN".to_string();

      tauri::async_runtime::block_on(async {
         let (app, _) =
            tokio::task::spawn_blocking(move || init_app_with_registered_db_at_path(&key, db_path))
               .await
               .expect("plugin init task should succeed");

         load_and_create_test_table(&app, "MAIN").await;

         let webview_a = tauri::WebviewWindowBuilder::new(&app, "window-a", Default::default())
            .build()
            .expect("webview window A should build");
         let webview_b = tauri::WebviewWindowBuilder::new(&app, "window-b", Default::default())
            .build()
            .expect("webview window B should build");

         const ITERATIONS: usize = 200;

         for i in 0..ITERATIONS {
            // Baseline for this iteration: MAIN loaded, window A observing.
            connect_to_database(app.handle(), "MAIN", None)
               .await
               .expect("reconnect should succeed");
            commands::observe(
               app.state::<DbInstances>(),
               app.state::<ObserverRegistrations>(),
               webview_a.as_ref().clone(),
               "MAIN".to_string(),
               vec!["test".to_string()],
               None,
            )
            .await
            .expect("baseline observe for window A should succeed");

            // Race: window B observes while the database is closed, concurrently.
            let app_for_observe = app.handle().clone();
            let webview_b_for_task = webview_b.as_ref().clone();
            let observe_task = tokio::spawn(async move {
               commands::observe(
                  app_for_observe.state::<DbInstances>(),
                  app_for_observe.state::<ObserverRegistrations>(),
                  webview_b_for_task,
                  "MAIN".to_string(),
                  vec!["test".to_string()],
                  None,
               )
               .await
            });

            let app_for_close = app.handle().clone();
            let close_task = tokio::spawn(async move {
               commands::close(
                  app_for_close.state::<DbInstances>(),
                  app_for_close.state::<ActiveSubscriptions>(),
                  app_for_close.state::<ObserverRegistrations>(),
                  app_for_close.state::<ActiveInterruptibleTransactions>(),
                  app_for_close.state::<ActiveRegularTransactions>(),
                  "MAIN".to_string(),
               )
               .await
            });

            // observe() may legitimately fail with DATABASE_NOT_LOADED if
            // close() won the race for the db lock first - that's fine, not
            // what this test is guarding against.
            let _ = observe_task.await.expect("observe task should not panic");
            close_task
               .await
               .expect("close task should not panic")
               .expect("close should succeed");

            // close() always removes the wrapper by the time it returns, so
            // the database is deterministically unloaded here - use is_some()
            // directly rather than the is_observing() helper, which assumes a
            // loaded database and would itself panic.
            assert!(
               app.state::<DbInstances>()
                  .inner()
                  .inner
                  .read()
                  .await
                  .get("MAIN")
                  .is_none(),
               "iteration {i}: close() should always leave the database unloaded"
            );
            assert_eq!(
               app.state::<ObserverRegistrations>()
                  .count_for_db("MAIN")
                  .await,
               0,
               "iteration {i}: no registration should survive close() with no wrapper left to refer to"
            );
         }
      });
   }

   /// Pins the invariant that once a subscription's forwarding task ends (its
   /// channel closed), it reaps its own entry from `ActiveSubscriptions` -
   /// exercising the `oneshot` ready-gate + self-removal logic rather than
   /// only reasoning about it. The channel below models the event loop being
   /// gone entirely (every `send` fails), not a same-webview reload - see the
   /// reap comment in `commands::subscribe` for why a reload can't actually
   /// trigger this path. Without this reap, letting the event loop/broker tear
   /// down would leave a dead entry behind forever, and eventually every *new*
   /// subscribe() call would hit `TOO_MANY_SUBSCRIPTIONS` even though nothing
   /// is actually subscribed anymore.
   #[test]
   fn test_subscribe_reaps_itself_after_channel_closes() {
      let temp_dir = tempfile::tempdir().unwrap();
      let db_path = validate::validate_database_path(temp_dir.path().join("main.db")).unwrap();
      let key = "MAIN".to_string();

      tauri::async_runtime::block_on(async {
         let (app, _) =
            tokio::task::spawn_blocking(move || init_app_with_registered_db_at_path(&key, db_path))
               .await
               .expect("plugin init task should succeed");

         load_and_create_test_table(&app, "MAIN").await;

         let webview = tauri::WebviewWindowBuilder::new(&app, "window-a", Default::default())
            .build()
            .expect("webview window should build");

         commands::observe(
            app.state::<DbInstances>(),
            app.state::<ObserverRegistrations>(),
            webview.as_ref().clone(),
            "MAIN".to_string(),
            vec!["test".to_string()],
            None,
         )
         .await
         .expect("observe should succeed");

         // A channel whose "send" always fails, simulating the event loop
         // being gone entirely (not a reload/navigation - a live webview's
         // `Channel::send` still reports `Ok` even when nothing is listening
         // on the JS side; see the reap comment in `commands::subscribe`). The
         // forwarding task's `on_event.send(...).is_err()` check will be true
         // on the first change event, causing it to break out of its loop and
         // reap itself.
         let channel = tauri::ipc::Channel::new(|_body| {
            Err(std::io::Error::other("simulated closed channel").into())
         });

         commands::subscribe(
            app.state::<DbInstances>(),
            app.state::<ActiveSubscriptions>(),
            app.state::<ObserverRegistrations>(),
            webview.as_ref().clone(),
            "MAIN".to_string(),
            vec!["test".to_string()],
            channel,
         )
         .await
         .expect("subscribe should succeed");

         assert_eq!(
            app.state::<ActiveSubscriptions>()
               .count_for_db("MAIN")
               .await,
            1,
            "subscription should be registered immediately after subscribe()"
         );

         commands::execute(
            app.state::<DbInstances>(),
            "MAIN".to_string(),
            "INSERT INTO test (val) VALUES ('trigger')".to_string(),
            vec![],
            None,
         )
         .await
         .expect("insert should succeed");

         // Give the forwarding task a chance to receive the change, fail to
         // send it through the closed channel, and reap itself.
         let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
         loop {
            if app
               .state::<ActiveSubscriptions>()
               .count_for_db("MAIN")
               .await
               == 0
            {
               break;
            }
            assert!(
               std::time::Instant::now() < deadline,
               "forwarding task never reaped its own entry after its channel closed"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
         }
      });
   }
}
