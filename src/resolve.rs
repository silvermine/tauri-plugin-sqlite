use std::fs::create_dir_all;
use std::path::{Component, Path, PathBuf};

use sqlx_sqlite_conn_mgr::{SqliteDatabaseConfig, is_memory_database};
use sqlx_sqlite_toolkit::DatabaseWrapper;
use tauri::{AppHandle, Manager, Runtime};

use crate::{AllowedPaths, Error};

/// Connect to a SQLite database via the connection manager.
///
/// This is the Tauri-specific connection method that validates the path against the
/// registered allowlist (see [`crate::Builder::register_database`]) before delegating to the
/// toolkit's `DatabaseWrapper::connect()`. Only registered absolute paths and in-memory
/// databases are accepted.
pub async fn connect<R: Runtime>(
   path: &str,
   app: &AppHandle<R>,
   custom_config: Option<SqliteDatabaseConfig>,
) -> Result<DatabaseWrapper, Error> {
   let abs_path = resolve_database_path(path, app)?;
   Ok(DatabaseWrapper::connect(&abs_path, custom_config).await?)
}

/// Resolve a database file path.
///
/// A path is accepted only if it is either an in-memory database (`:memory:` and friends,
/// passed through unchanged) or an absolute path that exactly matches an entry registered
/// via [`crate::Builder::register_database`] / [`crate::SetupRegistrar::register_database`].
///
/// Returns `Err(Error::PathTraversal)` if the path contains `..` segments or null bytes.
/// Returns `Err(Error::InvalidPath)` for relative or malformed paths.
/// Returns `Err(Error::PathNotRegistered)` if the path is not on the registered allowlist.
pub fn resolve_database_path<R: Runtime>(path: &str, app: &AppHandle<R>) -> Result<PathBuf, Error> {
   let allowed = app.state::<AllowedPaths>();
   validate_and_resolve(path, &allowed.absolute_database_paths)
}

/// Canonicalize a database key string for registration (allowlist / migrations).
///
/// In-memory paths pass through unchanged. On-disk paths are canonicalized with parent
/// directories created as needed.
pub(crate) fn canonicalize_database_key(key: &str) -> Result<String, Error> {
   if is_memory_path(key) {
      return Ok(key.to_string());
   }

   canonicalize_database_path(Path::new(key), true).map(|p| p.to_string_lossy().into_owned())
}

/// Canonicalize a database file path that may not exist yet.
///
/// When `create_parent` is `true`, missing parent directories are created before
/// canonicalizing (plugin setup and confirmed allowlist matches at load time). When `false`,
/// only existing path components are canonicalized — no filesystem side effects.
pub(crate) fn canonicalize_database_path(
   path: &Path,
   create_parent: bool,
) -> Result<PathBuf, Error> {
   if path.exists() {
      return path
         .canonicalize()
         .map_err(|e| Error::InvalidPath(format!("cannot canonicalize path: {e}")));
   }

   let parent = path
      .parent()
      .ok_or_else(|| Error::InvalidPath("path has no parent".to_string()))?;

   let file_name = path
      .file_name()
      .ok_or_else(|| Error::InvalidPath("path has no file name".to_string()))?;

   if create_parent {
      create_dir_all(parent)?;
   }

   parent
      .canonicalize()
      .map(|p| p.join(file_name))
      .map_err(|e| Error::InvalidPath(format!("cannot canonicalize path: {e}")))
}

/// Validate a user-supplied path and resolve it.
///
/// In-memory database paths are passed through unchanged. Every other path must be an
/// absolute path whose canonical form exactly matches a registered allowlist entry
/// (`allowed_files`); `..` segments and null bytes are always rejected.
fn validate_and_resolve(path: &str, allowed_files: &[PathBuf]) -> Result<PathBuf, Error> {
   // Pass through in-memory database paths unchanged — they don't touch the filesystem.
   // Matches the same patterns as `is_memory_database` in sqlx-sqlite-conn-mgr.
   if is_memory_path(path) {
      return Ok(PathBuf::from(path));
   }

   // Reject null bytes — these can truncate paths in C-level filesystem calls
   if path.contains('\0') {
      return Err(Error::PathTraversal("path contains null byte".to_string()));
   }

   let candidate = Path::new(path);

   if !candidate.is_absolute() {
      return Err(Error::InvalidPath(
         "database path must be absolute".to_string(),
      ));
   }

   // Reject parent directory components outright so a registered path cannot be used as a
   // springboard to reach a sibling/parent location via `..`.
   for component in candidate.components() {
      if matches!(component, Component::ParentDir) {
         return Err(Error::PathTraversal(
            "parent directory references are not allowed".to_string(),
         ));
      }
   }

   // Canonicalize without creating directories so unregistered paths cannot mkdir over IPC.
   let canonical = match canonicalize_database_path(candidate, false) {
      Ok(canonical) => canonical,
      Err(_) => {
         return Err(Error::PathNotRegistered(format!(
            "absolute path is not covered by the allowlist: {candidate:?}"
         )));
      }
   };

   if !allowed_files.contains(&canonical) {
      return Err(Error::PathNotRegistered(format!(
         "absolute path is not covered by the allowlist: {canonical:?}"
      )));
   }

   // Path is allowed — create parent directories if needed, then return the canonical path
   // so the opened file matches the value that passed validation.
   canonicalize_database_path(candidate, true)
}

/// Check if a path string represents an in-memory SQLite database.
///
/// Matches the same patterns as `is_memory_database` in `sqlx-sqlite-conn-mgr`:
/// `:memory:`, `file::memory:*` URIs, and `mode=memory` query parameters.
pub(crate) fn is_memory_path(path: &str) -> bool {
   is_memory_database(Path::new(path))
}

#[cfg(test)]
mod tests {
   use super::*;
   use std::fs;
   use std::sync::atomic::{AtomicU64, Ordering};

   /// Empty allowlist — nothing registered.
   const NO_FILES: &[PathBuf] = &[];

   /// Helper that creates a unique temporary directory for testing.
   fn make_temp_dir() -> PathBuf {
      static COUNTER: AtomicU64 = AtomicU64::new(0);
      let n = COUNTER.fetch_add(1, Ordering::Relaxed);
      let dir =
         std::env::temp_dir().join(format!("tauri_sqlite_test_{}_{}", std::process::id(), n));
      fs::create_dir_all(&dir).unwrap();
      dir
   }

   #[test]
   fn test_memory_passthrough() {
      assert_eq!(
         validate_and_resolve(":memory:", NO_FILES).unwrap(),
         PathBuf::from(":memory:"),
      );
   }

   #[test]
   fn test_file_memory_uri_passthrough() {
      assert_eq!(
         validate_and_resolve("file::memory:?cache=shared", NO_FILES).unwrap(),
         PathBuf::from("file::memory:?cache=shared"),
      );
   }

   #[test]
   fn test_mode_memory_passthrough() {
      assert_eq!(
         validate_and_resolve("file:test?mode=memory", NO_FILES).unwrap(),
         PathBuf::from("file:test?mode=memory"),
      );
   }

   #[test]
   fn test_mode_memory_substring_in_value_is_not_treated_as_memory() {
      let result = validate_and_resolve("file:/home/user/real.db?x=mode=memory", NO_FILES);
      assert!(
         result.is_err(),
         "substring mode=memory must not bypass the allowlist"
      );
   }

   #[test]
   fn test_accepts_registered_absolute_path() {
      let dir = make_temp_dir();
      let canonical_dir = dir.canonicalize().unwrap();
      let abs = dir.join("exact.db");
      let abs_str = abs.to_str().unwrap();

      let files = [canonical_dir.join("exact.db")];
      let result = validate_and_resolve(abs_str, &files).unwrap();
      assert_eq!(result, canonical_dir.join("exact.db"));
   }

   #[test]
   fn test_rejects_unregistered_absolute_path() {
      let err = validate_and_resolve("/etc/passwd", NO_FILES).unwrap_err();
      assert!(matches!(err, Error::PathNotRegistered(_)));
   }

   #[test]
   fn test_rejects_unregistered_path_without_creating_parent() {
      let base = make_temp_dir();
      let unregistered = base.join("nested").join("not-allowed.db");
      let unregistered_str = unregistered.to_str().unwrap().to_string();

      let err = validate_and_resolve(&unregistered_str, NO_FILES).unwrap_err();
      assert!(matches!(err, Error::PathNotRegistered(_)));
      assert!(
         !base.join("nested").exists(),
         "rejected load must not create parent directories"
      );
   }

   #[test]
   fn test_rejects_unregistered_path_with_existing_parent_without_creating_child() {
      let dir = make_temp_dir();
      let unregistered = dir.join("not-allowed.db");
      let unregistered_str = unregistered.to_str().unwrap().to_string();

      let err = validate_and_resolve(&unregistered_str, NO_FILES).unwrap_err();
      assert!(matches!(err, Error::PathNotRegistered(_)));
      assert!(!unregistered.exists());
   }

   #[test]
   fn test_rejects_relative_path() {
      let err = validate_and_resolve("relative.db", NO_FILES).unwrap_err();
      assert!(matches!(err, Error::InvalidPath(_)));
   }

   #[test]
   fn test_rejects_absolute_path_with_parent_traversal() {
      let dir = make_temp_dir();
      let abs_str = format!("{}/../escape.db", dir.to_str().unwrap());

      let err = validate_and_resolve(&abs_str, NO_FILES).unwrap_err();
      assert!(matches!(err, Error::PathTraversal(_)));
   }

   #[test]
   fn test_rejects_absolute_path_with_embedded_traversal() {
      let dir = make_temp_dir();
      let abs_str = format!("{}/sub/../../escape.db", dir.to_str().unwrap());

      let err = validate_and_resolve(&abs_str, NO_FILES).unwrap_err();
      assert!(matches!(err, Error::PathTraversal(_)));
   }

   #[test]
   fn test_rejects_null_byte() {
      let err = validate_and_resolve("path\0evil", NO_FILES).unwrap_err();
      assert!(matches!(err, Error::PathTraversal(_)));
   }

   #[test]
   fn test_setup_and_resolver_canonicalize_agree() {
      let dir = make_temp_dir();
      let db = dir.join("agree.db");

      let from_setup = canonicalize_database_path(&db, true).unwrap();
      let from_resolver =
         validate_and_resolve(db.to_str().unwrap(), std::slice::from_ref(&from_setup)).unwrap();
      assert_eq!(from_setup, from_resolver);
   }

   #[test]
   fn test_canonicalize_database_key_memory_passthrough() {
      assert_eq!(canonicalize_database_key(":memory:").unwrap(), ":memory:");
   }
}
