//! SQLite plugin commands
//!
//! This module implements the Tauri command handlers that the frontend calls.
//! Each command manages database connections through the DbInstances state.

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx_sqlite_conn_mgr::SqliteDatabaseConfig;
use std::sync::Arc;
use tauri::{AppHandle, Runtime, State};
use uuid::Uuid;

use crate::{
   DbInstances, Error, MigrationEvent, MigrationStates, MigrationStatus, Result, WriteQueryResult,
   transactions::{
      ActiveInterruptibleTransaction, ActiveInterruptibleTransactions, ActiveRegularTransactions,
      Statement,
   },
   wrapper::DatabaseWrapper,
};

/// Token representing an active interruptible transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionToken {
   pub db_path: String,
   pub transaction_id: String,
}

/// Actions that can be taken on an interruptible transaction
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum TransactionAction {
   Continue { statements: Vec<Statement> },
   Commit,
   Rollback,
}

/// Serializable attached database specification for TypeScript interface
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AttachedDatabaseSpec {
   /// Path to the database to attach (must be loaded via `load()` first)
   pub database_path: String,
   /// Schema name to use for the attached database in queries
   pub schema_name: String,
   /// Access mode: "readOnly" or "readWrite"
   pub mode: AttachedDatabaseMode,
}

/// Access mode for attached databases
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AttachedDatabaseMode {
   ReadOnly,
   ReadWrite,
}

/// Convert serializable specs to internal specs by resolving database references
fn resolve_attached_specs(
   specs: Vec<AttachedDatabaseSpec>,
   db_instances: &std::collections::HashMap<String, DatabaseWrapper>,
) -> Result<Vec<sqlx_sqlite_conn_mgr::AttachedSpec>> {
   let mut resolved = Vec::new();

   for spec in specs {
      let wrapper = db_instances
         .get(&spec.database_path)
         .ok_or_else(|| Error::DatabaseNotLoaded(spec.database_path.clone()))?;

      let mode = match spec.mode {
         AttachedDatabaseMode::ReadOnly => sqlx_sqlite_conn_mgr::AttachedMode::ReadOnly,
         AttachedDatabaseMode::ReadWrite => sqlx_sqlite_conn_mgr::AttachedMode::ReadWrite,
      };

      resolved.push(sqlx_sqlite_conn_mgr::AttachedSpec {
         database: Arc::clone(wrapper.inner()),
         schema_name: spec.schema_name,
         mode,
      });
   }

   Ok(resolved)
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
   attached: Option<Vec<AttachedDatabaseSpec>>,
) -> Result<(u64, i64)> {
   let instances = db_instances.0.read().await;

   let wrapper = instances
      .get(&db)
      .ok_or_else(|| Error::DatabaseNotLoaded(db.clone()))?;

   let mut builder = wrapper.execute(query, values);

   if let Some(specs) = attached {
      let resolved_specs = resolve_attached_specs(specs, &instances)?;
      builder = builder.attach(resolved_specs);
   }

   let result = builder.execute().await?;

   Ok((result.rows_affected, result.last_insert_id))
}

/// Execute multiple write statements atomically within a transaction
#[tauri::command]
pub async fn execute_transaction(
   db_instances: State<'_, DbInstances>,
   regular_txs: State<'_, ActiveRegularTransactions>,
   db: String,
   statements: Vec<Statement>,
   attached: Option<Vec<AttachedDatabaseSpec>>,
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

   // Generate unique key for tracking this transaction
   let tx_key = format!("{}:{}", db, Uuid::new_v4());

   // Resolve attached specs if provided
   let resolved_specs = if let Some(specs) = attached {
      Some(resolve_attached_specs(specs, &instances)?)
   } else {
      None
   };

   // Spawn transaction execution with abort handle for cleanup on exit
   let wrapper_clone = wrapper.clone();
   let tx_key_clone = tx_key.clone();
   let regular_txs_clone = regular_txs.inner().clone();

   let handle = tokio::spawn(async move {
      let mut builder = wrapper_clone.execute_transaction(stmt_tuples);

      if let Some(specs) = resolved_specs {
         builder = builder.attach(specs);
      }

      let result = builder.execute().await;

      // Remove from tracking when complete (even if result is Err)
      regular_txs_clone.remove(&tx_key_clone).await;

      result
   });

   // Track abort handle for cleanup on app exit
   regular_txs
      .insert(tx_key.clone(), handle.abort_handle())
      .await;

   // Wait for transaction to complete
   match handle.await {
      Ok(result) => result,
      Err(e) => {
         // Task panicked or was aborted - ensure cleanup
         regular_txs.remove(&tx_key).await;

         if e.is_cancelled() {
            Err(Error::Other("Transaction aborted due to app exit".into()))
         } else {
            Err(Error::Other(format!("Transaction task panicked: {}", e)))
         }
      }
   }
}

/// Execute a SELECT query returning all matching rows
#[tauri::command]
pub async fn fetch_all(
   db_instances: State<'_, DbInstances>,
   db: String,
   query: String,
   values: Vec<JsonValue>,
   attached: Option<Vec<AttachedDatabaseSpec>>,
) -> Result<Vec<IndexMap<String, JsonValue>>> {
   let instances = db_instances.0.read().await;

   let wrapper = instances
      .get(&db)
      .ok_or_else(|| Error::DatabaseNotLoaded(db.clone()))?;

   let mut builder = wrapper.fetch_all(query, values);

   if let Some(specs) = attached {
      let resolved_specs = resolve_attached_specs(specs, &instances)?;
      builder = builder.attach(resolved_specs);
   }

   let result = builder.execute().await?;

   Ok(result)
}

/// Execute a SELECT query expecting zero or one result
#[tauri::command]
pub async fn fetch_one(
   db_instances: State<'_, DbInstances>,
   db: String,
   query: String,
   values: Vec<JsonValue>,
   attached: Option<Vec<AttachedDatabaseSpec>>,
) -> Result<Option<IndexMap<String, JsonValue>>> {
   let instances = db_instances.0.read().await;

   let wrapper = instances
      .get(&db)
      .ok_or_else(|| Error::DatabaseNotLoaded(db.clone()))?;

   let mut builder = wrapper.fetch_one(query, values);

   if let Some(specs) = attached {
      let resolved_specs = resolve_attached_specs(specs, &instances)?;
      builder = builder.attach(resolved_specs);
   }

   let result = builder.execute().await?;

   Ok(result)
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

   // Close each connection, continuing on errors to ensure all get closed
   let mut last_error = None;
   for wrapper in wrappers {
      if let Err(e) = wrapper.close().await {
         last_error = Some(e);
      }
   }

   match last_error {
      Some(e) => Err(e),
      None => Ok(()),
   }
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

/// Execute initial statements in an interruptible transaction and return a token.
///
/// This begins a transaction, executes the initial statements, and returns a token
/// that can be used to continue, commit, or rollback the transaction.
/// The writer connection is held for the entire transaction duration.
#[tauri::command]
pub async fn execute_interruptible_transaction(
   db_instances: State<'_, DbInstances>,
   active_txs: State<'_, ActiveInterruptibleTransactions>,
   db: String,
   initial_statements: Vec<Statement>,
) -> Result<TransactionToken> {
   let instances = db_instances.0.read().await;

   let wrapper = instances
      .get(&db)
      .ok_or_else(|| Error::DatabaseNotLoaded(db.clone()))?;

   // Generate unique transaction ID
   let transaction_id = Uuid::new_v4().to_string();

   // Acquire writer for the entire transaction
   let mut writer = wrapper.acquire_writer().await?;

   // Begin transaction
   sqlx::query("BEGIN IMMEDIATE").execute(&mut *writer).await?;

   // Execute initial statements
   for statement in initial_statements {
      let mut q = sqlx::query(&statement.query);
      for value in statement.values {
         q = crate::wrapper::bind_value(q, value);
      }
      q.execute(&mut *writer).await?;
   }

   // Store transaction state
   let tx = ActiveInterruptibleTransaction::new(db.clone(), transaction_id.clone(), writer);

   active_txs.insert(db.clone(), tx).await?;

   Ok(TransactionToken {
      db_path: db,
      transaction_id,
   })
}

/// Continue, commit, or rollback an interruptible transaction.
///
/// Returns a new token if continuing with more statements, or None if committed/rolled back.
#[tauri::command]
pub async fn transaction_continue(
   active_txs: State<'_, ActiveInterruptibleTransactions>,
   token: TransactionToken,
   action: TransactionAction,
) -> Result<Option<TransactionToken>> {
   match action {
      TransactionAction::Continue { statements } => {
         // Remove transaction to get mutable access
         let mut tx = active_txs
            .remove(&token.db_path, &token.transaction_id)
            .await?;

         // Execute statements on the transaction
         match tx.execute_statements(statements).await {
            Ok(()) => {
               // Re-insert transaction - if this fails, tx is dropped and auto-rolled back
               match active_txs.insert(token.db_path.clone(), tx).await {
                  Ok(()) => Ok(Some(token)),
                  Err(e) => {
                     // Transaction lost but will auto-rollback via Drop
                     Err(e)
                  }
               }
            }
            Err(e) => {
               // Execution failed, explicitly rollback before returning error
               let _ = tx.rollback().await;
               Err(e)
            }
         }
      }

      TransactionAction::Commit => {
         // Remove transaction and commit
         let tx = active_txs
            .remove(&token.db_path, &token.transaction_id)
            .await?;

         tx.commit().await?;
         Ok(None)
      }

      TransactionAction::Rollback => {
         // Remove transaction and rollback
         let tx = active_txs
            .remove(&token.db_path, &token.transaction_id)
            .await?;

         tx.rollback().await?;
         Ok(None)
      }
   }
}

/// Read from database within an interruptible transaction to see uncommitted writes.
///
/// This executes a SELECT query on the same connection as the transaction,
/// allowing you to see uncommitted data.
#[tauri::command]
pub async fn transaction_read(
   active_txs: State<'_, ActiveInterruptibleTransactions>,
   token: TransactionToken,
   query: String,
   values: Vec<JsonValue>,
) -> Result<Vec<IndexMap<String, JsonValue>>> {
   // Remove transaction to get mutable access
   let mut tx = active_txs
      .remove(&token.db_path, &token.transaction_id)
      .await?;

   // Execute read on the transaction
   match tx.read(query, values).await {
      Ok(results) => {
         // Re-insert transaction - if this fails, tx is dropped and auto-rolled back
         match active_txs.insert(token.db_path.clone(), tx).await {
            Ok(()) => Ok(results),
            Err(e) => {
               // Transaction lost but will auto-rollback via Drop
               Err(e)
            }
         }
      }
      Err(e) => {
         // Read failed, explicitly rollback before returning error
         let _ = tx.rollback().await;
         Err(e)
      }
   }
}
