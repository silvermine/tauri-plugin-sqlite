use tauri::{Runtime, plugin::TauriPlugin};

mod commands;
mod error;

pub use error::{Error, Result};

/// Initializes the plugin.
pub fn init<R: Runtime>() -> TauriPlugin<R> {
   tauri::plugin::Builder::new("sqlite")
      .invoke_handler(tauri::generate_handler![commands::hello])
      .build()
}
