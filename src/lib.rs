use std::collections::HashMap;
use std::error::Error as StdError;
use std::future::Future;

use serde::Deserialize;
use sqlx::migrate::{Migration as SqlxMigration, MigrationType};
use tauri::{Runtime, plugin::Builder as PluginBuilder};
use tokio::sync::RwLock;

use futures_core::future::BoxFuture;

mod commands;
mod decode;
mod error;
mod wrapper;

pub use error::{Error, Result};
pub use wrapper::{DatabaseWrapper, WriteQueryResult};

type BoxDynError = Box<dyn StdError + Send + Sync>;

/// Database instances managed by the plugin.
///
/// This struct maintains a thread-safe map of database paths to their corresponding
/// connection wrappers.
#[derive(Default)]
pub struct DbInstances(pub RwLock<HashMap<String, DatabaseWrapper>>);

/// Plugin configuration.
///
/// Defines databases to preload during plugin initialization.
#[derive(Default, Clone, Deserialize)]
pub struct PluginConfig {
   /// List of database paths to load on plugin initialization
   #[serde(default)]
   #[allow(dead_code)] // Will be used in future PR
   preload: Vec<String>,
}

/// Represents a database migration.
#[derive(Debug)]
pub struct Migration {
   /// The version number of this migration
   pub version: i64,
   /// A description of what this migration does
   pub description: &'static str,
   /// SQL statements to execute for this migration
   pub sql: Vec<&'static str>,
   /// Whether this is an "up" or "down" migration
   pub kind: MigrationKind,
}

/// The kind of migration (up or down).
#[derive(Debug)]
pub enum MigrationKind {
   /// Apply migration (forward)
   Up,
   /// Revert migration (backward)
   Down,
}

impl From<MigrationKind> for MigrationType {
   fn from(kind: MigrationKind) -> Self {
      match kind {
         MigrationKind::Up | MigrationKind::Down => MigrationType::Simple,
      }
   }
}

/// Internal collection of migrations for a database.
#[derive(Debug)]
struct MigrationList(Vec<Migration>);

impl sqlx::migrate::MigrationSource<'static> for MigrationList {
   fn resolve(self) -> BoxFuture<'static, std::result::Result<Vec<SqlxMigration>, BoxDynError>> {
      Box::pin(async move {
         let mut migrations = Vec::new();
         for migration in self.0 {
            if matches!(migration.kind, MigrationKind::Up) {
               let sql = migration.sql.join(";\n");
               migrations.push(SqlxMigration::new(
                  migration.version,
                  migration.description.into(),
                  migration.kind.into(),
                  sql.into(),
                  false,
               ));
            }
         }
         Ok(migrations)
      })
   }
}

/// Helper function to run async commands in both async and sync contexts.
///
/// This handles the case where we're already in a Tokio runtime (use `block_in_place`)
/// or need to create one (use Tauri's async runtime).
#[allow(dead_code)] // Will be used in a future PR
fn run_async_command<F: Future>(cmd: F) -> F::Output {
   if tokio::runtime::Handle::try_current().is_ok() {
      tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(cmd))
   } else {
      tauri::async_runtime::block_on(cmd)
   }
}

/// Builder for the SQLite plugin.
///
/// Use this to configure migrations and build the plugin instance.
///
/// # Example
///
/// ```rust,ignore
/// use tauri_plugin_sqlite::{Builder, Migration, MigrationKind};
///
/// // In your Tauri app setup:
/// tauri::Builder::default()
///     .plugin(
///         Builder::new()
///             .add_migrations("mydb.db", vec![
///                 Migration {
///                     version: 1,
///                     description: "create users table",
///                     sql: vec!["CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"],
///                     kind: MigrationKind::Up,
///                 }
///             ])
///             .build()
///     )
///     .run(tauri::generate_context!())
///     .expect("error while running tauri application");
/// ```
#[derive(Default)]
pub struct Builder {
   migrations: Option<HashMap<String, MigrationList>>,
}

impl Builder {
   /// Create a new builder instance.
   pub fn new() -> Self {
      Self::default()
   }

   /// Add migrations for a specific database.
   ///
   /// # Arguments
   ///
   /// * `db_path` - The database file path (relative to app config directory)
   /// * `migrations` - Vector of migrations to apply
   #[must_use]
   pub fn add_migrations(mut self, db_path: &str, migrations: Vec<Migration>) -> Self {
      self
         .migrations
         .get_or_insert_with(HashMap::new)
         .insert(db_path.to_string(), MigrationList(migrations));
      self
   }

   /// Build the plugin with the configured migrations.
   ///
   /// This will be fully implemented in a future PR.
   pub fn build<R: Runtime>(self) -> tauri::plugin::TauriPlugin<R, Option<PluginConfig>> {
      // Future PR: Full implementation with setup, preload, and cleanup hooks
      PluginBuilder::<R, Option<PluginConfig>>::new("sqlite")
         .setup(|_app, _api| {
            // Future PR: Database preloading and migration setup
            Ok(())
         })
         .build()
   }
}

/// Initializes the plugin with default configuration.
///
/// For custom configuration and migrations, use `Builder` instead.
pub fn init<R: Runtime>() -> tauri::plugin::TauriPlugin<R, Option<PluginConfig>> {
   Builder::new().build()
}
