use std::collections::HashMap;

use tauri::{Manager, RunEvent, Runtime, plugin::Builder as PluginBuilder};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

mod commands;
mod decode;
mod error;
mod wrapper;

pub use error::{Error, Result};
pub use wrapper::{DatabaseWrapper, WriteQueryResult};

/// Database instances managed by the plugin.
///
/// This struct maintains a thread-safe map of database paths to their corresponding
/// connection wrappers.
#[derive(Default)]
pub struct DbInstances(pub RwLock<HashMap<String, DatabaseWrapper>>);

/// Builder for the SQLite plugin.
///
/// Use this to configure the plugin and build the plugin instance.
///
/// # Example
///
/// ```ignore
/// use tauri_plugin_sqlite::Builder;
///
/// // In your Tauri app setup:
/// tauri::Builder::default()
///     .plugin(Builder::new().build())
///     .run(tauri::generate_context!())
///     .expect("error while running tauri application");
/// ```
#[derive(Default)]
pub struct Builder;

impl Builder {
   /// Create a new builder instance.
   pub fn new() -> Self {
      Self
   }

   /// Build the plugin with command registration and state management.
   pub fn build<R: Runtime>(self) -> tauri::plugin::TauriPlugin<R> {
      PluginBuilder::<R>::new("sqlite")
         .invoke_handler(tauri::generate_handler![
            commands::load,
            commands::execute,
            commands::execute_transaction,
            commands::fetch_all,
            commands::fetch_one,
            commands::close,
            commands::close_all,
            commands::remove,
         ])
         .setup(|app, _api| {
            app.manage(DbInstances::default());
            debug!("SQLite plugin initialized");
            // Future PR: Possibly handle migrations here
            Ok(())
         })
         .on_event(|app, event| {
            match event {
               RunEvent::ExitRequested { api, code, .. } => {
                  info!("App exit requested (code: {:?}) - closing databases before exit", code);

                  // Prevent immediate exit so we can close connections and checkpoint WAL
                  api.prevent_exit();

                  let instances = app.state::<DbInstances>();
                  let app_handle = app.clone();

                  tokio::task::block_in_place(|| {
                     tokio::runtime::Handle::current().block_on(async {
                        let mut instances = instances.0.write().await;
                        let wrappers: Vec<DatabaseWrapper> = instances.drain().map(|(_, v)| v).collect();

                        for wrapper in wrappers {
                           if let Err(e) = wrapper.close().await {
                              warn!("Error closing database during exit: {:?}", e);
                           }
                        }

                        debug!("All databases closed, calling exit()...");
                     })
                  });

                  app_handle.exit(code.unwrap_or(0));
               }
               RunEvent::Exit => {
                  // ExitRequested should have already closed all databases
                  // This is just a safety check
                  let instances = app.state::<DbInstances>();
                  if let Ok(instances) = instances.0.try_read() {
                     if !instances.is_empty() {
                        warn!("Exit event fired with {} database(s) still open - cleanup may have been skipped", instances.len());
                     } else {
                        debug!("Exit event: all databases already closed");
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
