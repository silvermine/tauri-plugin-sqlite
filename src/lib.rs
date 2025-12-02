use std::collections::HashMap;

use tauri::{Manager, Runtime, plugin::Builder as PluginBuilder};
use tokio::sync::RwLock;

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
            Ok(())
         })
         .build()
   }
}

/// Initializes the plugin with default configuration.
pub fn init<R: Runtime>() -> tauri::plugin::TauriPlugin<R> {
   Builder::new().build()
}
