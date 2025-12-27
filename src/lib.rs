use std::collections::HashMap;
use std::sync::Arc;

use serde::Serialize;
use sqlx_sqlite_conn_mgr::Migrator;
use tauri::{Emitter, Manager, RunEvent, Runtime, plugin::Builder as PluginBuilder};
use tokio::sync::{Notify, RwLock};
use tracing::{debug, error, info, trace, warn};

mod commands;
mod decode;
mod error;
mod transactions;
mod wrapper;

pub use error::{Error, Result};
pub use sqlx_sqlite_conn_mgr::Migrator as SqliteMigrator;
pub use transactions::{ActiveInterruptibleTransactions, ActiveRegularTransactions};
pub use wrapper::{DatabaseWrapper, WriteQueryResult};

/// Database instances managed by the plugin.
///
/// This struct maintains a thread-safe map of database paths to their corresponding
/// connection wrappers.
#[derive(Clone, Default)]
pub struct DbInstances(pub Arc<RwLock<HashMap<String, DatabaseWrapper>>>);

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
/// use tauri_plugin_sqlite::Builder;
///
/// // Basic setup (no migrations):
/// tauri::Builder::default()
///     .plugin(Builder::new().build())
///     .run(tauri::generate_context!())
///     .expect("error while running tauri application");
/// ```
///
/// # Example with migrations
///
/// ```ignore
/// use tauri_plugin_sqlite::Builder;
///
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
/// ```
#[derive(Default)]
pub struct Builder {
   /// Migrations registered per database path
   migrations: HashMap<String, Arc<Migrator>>,
}

impl Builder {
   /// Create a new builder instance.
   pub fn new() -> Self {
      Self {
         migrations: HashMap::new(),
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
   /// ```ignore
   /// Builder::new()
   ///     .add_migrations("main.db", sqlx::migrate!("./migrations"))
   ///     .build()
   /// ```
   pub fn add_migrations(mut self, path: &str, migrator: Migrator) -> Self {
      self.migrations.insert(path.to_string(), Arc::new(migrator));
      self
   }

   /// Build the plugin with command registration and state management.
   pub fn build<R: Runtime>(self) -> tauri::plugin::TauriPlugin<R> {
      let migrations = Arc::new(self.migrations);

      PluginBuilder::<R>::new("sqlite")
         .invoke_handler(tauri::generate_handler![
            commands::load,
            commands::execute,
            commands::execute_transaction,
            commands::execute_interruptible_transaction,
            commands::transaction_continue,
            commands::transaction_read,
            commands::fetch_all,
            commands::fetch_one,
            commands::close,
            commands::close_all,
            commands::remove,
            commands::get_migration_events,
         ])
         .setup(move |app, _api| {
            app.manage(DbInstances::default());
            app.manage(MigrationStates::default());
            app.manage(ActiveInterruptibleTransactions::default());
            app.manage(ActiveRegularTransactions::default());

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

                  tokio::spawn(async move {
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
                  info!("App exit requested (code: {:?}) - cleaning up transactions and databases", code);

                  // Prevent immediate exit so we can close connections and checkpoint WAL
                  api.prevent_exit();

                  let app_handle = app.clone();

                  let handle = match tokio::runtime::Handle::try_current() {
                     Ok(h) => h,
                     Err(_) => {
                        warn!("No tokio runtime available for cleanup");
                        app_handle.exit(code.unwrap_or(0));
                        return;
                     }
                  };

                  let instances_clone = app.state::<DbInstances>().inner().clone();
                  let interruptible_txs_clone = app.state::<ActiveInterruptibleTransactions>().inner().clone();
                  let regular_txs_clone = app.state::<ActiveRegularTransactions>().inner().clone();

                  // Spawn a blocking thread to abort transactions and close databases
                  // (block_in_place panics on current_thread runtime)
                  let cleanup_result = std::thread::spawn(move || {
                     handle.block_on(async {
                        // First, abort all active transactions
                        debug!("Aborting active transactions");
                        transactions::cleanup_all_transactions(&interruptible_txs_clone, &regular_txs_clone).await;

                        // Then close databases
                        let mut guard = instances_clone.0.write().await;
                        let wrappers: Vec<DatabaseWrapper> =
                           guard.drain().map(|(_, v)| v).collect();

                        // Close databases in parallel with timeout
                        let mut set = tokio::task::JoinSet::new();
                        for wrapper in wrappers {
                           set.spawn(async move { wrapper.close().await });
                        }

                        let timeout_result = tokio::time::timeout(
                           std::time::Duration::from_secs(5),
                           async {
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
                     })
                  })
                  .join();

                  if let Err(e) = cleanup_result {
                     error!("Database cleanup thread panicked: {:?}", e);
                  }

                  app_handle.exit(code.unwrap_or(0));
               }
               RunEvent::Exit => {
                  // ExitRequested should have already closed all databases
                  // This is just a safety check
                  let instances = app.state::<DbInstances>();
                  match instances.0.try_read() {
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
   let db = match DatabaseWrapper::connect_with_path(&abs_path, None).await {
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

/// Resolve database path for migrations (similar to wrapper but accessible at init).
fn resolve_migration_path<R: Runtime>(
   path: &str,
   app: &tauri::AppHandle<R>,
) -> Result<std::path::PathBuf> {
   let app_path = app
      .path()
      .app_config_dir()
      .map_err(|_| Error::InvalidPath("No app config path found".to_string()))?;

   std::fs::create_dir_all(&app_path).map_err(Error::Io)?;

   Ok(app_path.join(path))
}
