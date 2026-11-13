//! Compile checks for the public paths available to applications using the plugin.

use std::sync::Arc;

use tauri_plugin_sqlite::sqlx_sqlite_conn_mgr::{self, AttachedReadConnection, SqliteDatabase};
use tauri_plugin_sqlite::sqlx_sqlite_observer::{
   ObservableSqliteDatabase, ObservableWriteGuard, ObserverConfig, TableChange,
};
use tauri_plugin_sqlite::sqlx_sqlite_toolkit::{
   self, KeysetColumn, KeysetPage, SortDirection, WriterGuard,
};
use tauri_plugin_sqlite::{DatabaseWrapper, Error, SqliteMigrator};

#[test]
fn existing_exports_match_inner_crate_types() {
   let _: fn(&DatabaseWrapper) -> &Arc<SqliteDatabase> =
      sqlx_sqlite_toolkit::DatabaseWrapper::inner;
   let _: fn(sqlx_sqlite_conn_mgr::Migrator) -> SqliteMigrator = std::convert::identity;
   let _: fn(sqlx_sqlite_toolkit::Error) -> Error = Error::Toolkit;
   let _: fn(sqlx_sqlite_conn_mgr::Error) -> Error = Error::from;
}

#[test]
fn observer_features_are_available_through_the_plugin() {
   let _: fn(Arc<SqliteDatabase>, ObserverConfig) -> ObservableSqliteDatabase =
      ObservableSqliteDatabase::new;
   let _: fn(ObservableWriteGuard) -> WriterGuard = WriterGuard::Observable;
}

#[test]
fn advanced_api_types_are_accessible() {
   // These types appear in public method signatures but have no root convenience exports.
   let _ = std::any::TypeId::of::<AttachedReadConnection>();
   let _ = std::any::TypeId::of::<TableChange>();
   let _ = std::any::TypeId::of::<KeysetPage>();
   let _: KeysetColumn = KeysetColumn {
      name: "id".into(),
      direction: SortDirection::Asc,
   };
}
