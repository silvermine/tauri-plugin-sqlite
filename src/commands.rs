//! SQLite plugin commands
//!
//! This module implements the Tauri command handlers that the frontend calls.
//! Each command manages database connections through the DbInstances state.

use indexmap::IndexMap;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use sqlx_sqlite_conn_mgr::SqliteDatabaseConfig;
use tauri::{AppHandle, Runtime, State};

use crate::{
   DbInstances, Error, MigrationEvent, MigrationStates, MigrationStatus, Result, WriteQueryResult,
   wrapper::DatabaseWrapper,
};

/// Statement in a transaction with query and bind values
#[derive(Debug, Deserialize)]
pub struct Statement {
   query: String,
   values: Vec<JsonValue>,
}

/// Load/connect to a database and store it in plugin state.
///
/// If the database is already loaded, returns the existing connection.
/// Otherwise, creates a new connection with optional custom configuration.
///
/// # Migration Timing
///
/// If migrations are registered for this database, this function waits for them
/// to complete before proceeding. The migration task (spawned at plugin setup)
/// already called `SqliteDatabase::connect()`, which cached the database instance.
/// When we call `connect()` here, we get the **same cached instance** from the
/// registry - so we're not creating duplicate connections.
#[tauri::command]
pub async fn load<R: Runtime>(
   app: AppHandle<R>,
   db_instances: State<'_, DbInstances>,
   migration_states: State<'_, MigrationStates>,
   db: String,
   custom_config: Option<SqliteDatabaseConfig>,
) -> Result<String> {
   // Wait for migrations to complete if registered for this database
   await_migrations(&migration_states, &db).await?;

   let instances = db_instances.0.read().await;

   // Return cached if db was already loaded
   if instances.contains_key(&db) {
      return Ok(db);
   }

   drop(instances); // Release read lock before acquiring write lock

   let mut instances = db_instances.0.write().await;

   // Use entry API to atomically check and insert, avoiding race conditions
   // where two callers could both create wrappers
   use std::collections::hash_map::Entry;
   match instances.entry(db.clone()) {
      Entry::Occupied(_) => {
         // Another caller won the race and inserted while we waited for write lock
         Ok(db)
      }
      Entry::Vacant(entry) => {
         // We won the race, create and insert the wrapper
         let wrapper = DatabaseWrapper::connect(&db, &app, custom_config).await?;
         entry.insert(wrapper);
         Ok(db)
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
async fn await_migrations(migration_states: &State<'_, MigrationStates>, db: &str) -> Result<()> {
   loop {
      // Get notify handle before checking status
      let notify = {
         let states = migration_states.0.read().await;
         match states.get(db) {
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

/// Execute a write query (INSERT, UPDATE, DELETE, etc.)
#[tauri::command]
pub async fn execute(
   db_instances: State<'_, DbInstances>,
   db: String,
   query: String,
   values: Vec<JsonValue>,
) -> Result<(u64, i64)> {
   let instances = db_instances.0.read().await;

   let wrapper = instances
      .get(&db)
      .ok_or_else(|| Error::DatabaseNotLoaded(db.clone()))?;

   let result = wrapper.execute(query, values).await?;

   Ok((result.rows_affected, result.last_insert_id))
}

/// Execute multiple write statements atomically within a transaction
#[tauri::command]
pub async fn execute_transaction(
   db_instances: State<'_, DbInstances>,
   db: String,
   statements: Vec<Statement>,
) -> Result<Vec<WriteQueryResult>> {
   let instances = db_instances.0.read().await;

   let wrapper = instances
      .get(&db)
      .ok_or_else(|| Error::DatabaseNotLoaded(db.clone()))?;

   // Convert Statement structs to tuples for wrapper
   let stmt_tuples: Vec<(String, Vec<JsonValue>)> = statements
      .into_iter()
      .map(|s| (s.query, s.values))
      .collect();

   let results = wrapper.execute_transaction(stmt_tuples).await?;

   Ok(results)
}

/// Execute a SELECT query returning all matching rows
#[tauri::command]
pub async fn fetch_all(
   db_instances: State<'_, DbInstances>,
   db: String,
   query: String,
   values: Vec<JsonValue>,
) -> Result<Vec<IndexMap<String, JsonValue>>> {
   let instances = db_instances.0.read().await;

   let wrapper = instances
      .get(&db)
      .ok_or_else(|| Error::DatabaseNotLoaded(db.clone()))?;

   let rows = wrapper.fetch_all(query, values).await?;

   Ok(rows)
}

/// Execute a SELECT query expecting zero or one result
#[tauri::command]
pub async fn fetch_one(
   db_instances: State<'_, DbInstances>,
   db: String,
   query: String,
   values: Vec<JsonValue>,
) -> Result<Option<IndexMap<String, JsonValue>>> {
   let instances = db_instances.0.read().await;

   let wrapper = instances
      .get(&db)
      .ok_or_else(|| Error::DatabaseNotLoaded(db.clone()))?;

   let row = wrapper.fetch_one(query, values).await?;

   Ok(row)
}

/// Close a specific database connection
///
/// Returns `true` if the database was loaded and successfully closed.
/// Returns `false` if the database was not loaded (nothing to close).
#[tauri::command]
pub async fn close(db_instances: State<'_, DbInstances>, db: String) -> Result<bool> {
   let mut instances = db_instances.0.write().await;

   if let Some(wrapper) = instances.remove(&db) {
      wrapper.close().await?;
      Ok(true)
   } else {
      Ok(false) // Database wasn't loaded
   }
}

/// Close all database connections
#[tauri::command]
pub async fn close_all(db_instances: State<'_, DbInstances>) -> Result<()> {
   let mut instances = db_instances.0.write().await;

   // Collect all wrappers to close
   let wrappers: Vec<DatabaseWrapper> = instances.drain().map(|(_, v)| v).collect();

   // Close each connection
   for wrapper in wrappers {
      wrapper.close().await?;
   }

   Ok(())
}

/// Close database connection and remove all database files
///
/// Returns `true` if the database was loaded and successfully removed.
/// Returns `false` if the database was not loaded (nothing to remove).
#[tauri::command]
pub async fn remove(db_instances: State<'_, DbInstances>, db: String) -> Result<bool> {
   let mut instances = db_instances.0.write().await;

   if let Some(wrapper) = instances.remove(&db) {
      wrapper.remove().await?;
      Ok(true)
   } else {
      Ok(false) // Database wasn't loaded
   }
}

/// Get cached migration events for a database.
///
/// Returns all migration events that have been emitted for the specified database.
/// This allows the frontend to retrieve events even if they were missed due to timing.
///
/// Returns an empty array if no migrations are registered for this database.
#[tauri::command]
pub async fn get_migration_events(
   migration_states: State<'_, MigrationStates>,
   db: String,
) -> Result<Vec<MigrationEvent>> {
   let states = migration_states.0.read().await;

   match states.get(&db) {
      Some(state) => Ok(state.events.clone()),
      None => Ok(Vec::new()),
   }
}
