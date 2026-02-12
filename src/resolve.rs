use std::fs::create_dir_all;
use std::path::PathBuf;

use sqlx_sqlite_conn_mgr::SqliteDatabaseConfig;
use sqlx_sqlite_toolkit::DatabaseWrapper;
use tauri::{AppHandle, Manager, Runtime};

use crate::Error;

/// Connect to a SQLite database via the connection manager, resolving
/// the path relative to the app config directory.
///
/// This is the Tauri-specific connection method that resolves relative paths
/// before delegating to the toolkit's `DatabaseWrapper::connect()`.
pub async fn connect<R: Runtime>(
   path: &str,
   app: &AppHandle<R>,
   custom_config: Option<SqliteDatabaseConfig>,
) -> Result<DatabaseWrapper, Error> {
   let abs_path = resolve_database_path(path, app)?;
   Ok(DatabaseWrapper::connect(&abs_path, custom_config).await?)
}

/// Resolve database file path relative to app config directory.
///
/// Paths are joined to `app_config_dir()` (e.g., `Library/Application Support/${bundleIdentifier}` on iOS).
/// Special paths like `:memory:` are passed through unchanged.
pub fn resolve_database_path<R: Runtime>(path: &str, app: &AppHandle<R>) -> Result<PathBuf, Error> {
   let app_path = app
      .path()
      .app_config_dir()
      .map_err(|_| Error::InvalidPath("No app config path found".to_string()))?;

   create_dir_all(&app_path)?;

   // Join the relative path to the app config directory
   Ok(app_path.join(path))
}
