use std::fs;
use std::path::{Component, Path, PathBuf};

use sqlx_sqlite_conn_mgr::{canonicalize_database_path, is_memory_database};

use crate::{Error, Result};

/// Validate and normalize a database path at registration time.
///
/// In-memory databases (`:memory:`, `file::memory:*`, and `file:` URIs with an exact
/// `mode=memory` query parameter) are returned unchanged. File paths must not contain
/// null bytes or `..` components, must be absolute, and are canonicalized for consistent
/// lookups (symlink-safe when the path or its parent exists).
pub fn validate_database_path(path: impl AsRef<Path>) -> Result<PathBuf> {
   let path = path.as_ref();

   if is_memory_database(path) {
      return Ok(path.to_path_buf());
   }

   let path_str = path.to_str().ok_or_else(|| {
      Error::InvalidPath(format!(
         "database path is not valid UTF-8: {}",
         path.display()
      ))
   })?;

   if path_str.contains('\0') {
      return Err(Error::PathTraversal(format!(
         "database path contains null byte: {path_str}"
      )));
   }

   if path
      .components()
      .any(|component| matches!(component, Component::ParentDir))
   {
      return Err(Error::PathTraversal(format!(
         "database path contains parent traversal: {path_str}"
      )));
   }

   if !path.is_absolute() {
      return Err(Error::InvalidPath(format!(
         "database path must be absolute: {path_str}"
      )));
   }

   if let Some(parent) = path.parent()
      && !parent.as_os_str().is_empty()
   {
      fs::create_dir_all(parent).map_err(|error| {
         Error::InvalidPath(format!(
            "failed to create parent directory for database path {path_str}: {error}"
         ))
      })?;
   }

   canonicalize_database_path(path).map_err(|error| {
      Error::InvalidPath(format!(
         "failed to canonicalize database path {path_str}: {error}"
      ))
   })
}

#[cfg(test)]
mod tests {
   use super::*;
   use std::fs;
   use std::sync::atomic::{AtomicU64, Ordering};

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
         validate_database_path(":memory:").unwrap(),
         PathBuf::from(":memory:"),
      );
   }

   #[test]
   fn test_file_memory_uri_passthrough() {
      assert_eq!(
         validate_database_path("file::memory:?cache=shared").unwrap(),
         PathBuf::from("file::memory:?cache=shared"),
      );
   }

   #[test]
   fn test_mode_memory_passthrough() {
      assert_eq!(
         validate_database_path("file:test?mode=memory").unwrap(),
         PathBuf::from("file:test?mode=memory"),
      );
   }

   #[test]
   fn test_mode_memory_substring_in_value_is_not_treated_as_memory() {
      let err = validate_database_path("file:/home/user/real.db?x=mode=memory").unwrap_err();
      assert!(matches!(err, Error::InvalidPath(_)));
   }

   #[test]
   fn test_creates_missing_parent_directory() {
      static COUNTER: AtomicU64 = AtomicU64::new(0);
      let n = COUNTER.fetch_add(1, Ordering::Relaxed);
      let base = std::env::temp_dir().join(format!(
         "tauri_sqlite_test_missing_parent_{}_{}",
         std::process::id(),
         n
      ));
      let db_path = base.join("nested").join("main.db");

      let result = validate_database_path(&db_path).unwrap();

      assert!(base.join("nested").is_dir());
      assert_eq!(
         result,
         base.canonicalize().unwrap().join("nested").join("main.db")
      );

      fs::remove_dir_all(&base).unwrap();
   }

   #[test]
   fn test_accepts_absolute_path() {
      let dir = make_temp_dir();
      let abs = dir.join("exact.db");

      let result = validate_database_path(&abs).unwrap();
      assert_eq!(result, dir.canonicalize().unwrap().join("exact.db"));
   }

   #[test]
   fn test_rejects_relative_path() {
      let err = validate_database_path("relative.db").unwrap_err();
      assert!(matches!(err, Error::InvalidPath(_)));
   }

   #[test]
   fn test_rejects_absolute_path_with_parent_traversal() {
      let dir = make_temp_dir();
      let abs_str = format!("{}/../escape.db", dir.to_str().unwrap());

      let err = validate_database_path(&abs_str).unwrap_err();
      assert!(matches!(err, Error::PathTraversal(_)));
   }

   #[test]
   fn test_rejects_absolute_path_with_embedded_traversal() {
      let dir = make_temp_dir();
      let abs_str = format!("{}/sub/../../escape.db", dir.to_str().unwrap());

      let err = validate_database_path(&abs_str).unwrap_err();
      assert!(matches!(err, Error::PathTraversal(_)));
   }

   #[test]
   fn test_rejects_null_byte() {
      let err = validate_database_path("path\0evil").unwrap_err();
      assert!(matches!(err, Error::PathTraversal(_)));
   }
}
