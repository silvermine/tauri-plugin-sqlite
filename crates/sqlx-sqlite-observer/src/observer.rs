//! SQLite observer with transaction-safe change notifications.
//!
//! Uses SQLite's native hooks for change detection.

use std::sync::Arc;

use sqlx::SqlitePool;
use tokio::sync::broadcast;
use tracing::{debug, warn};

use crate::Result;
use crate::broker::ObservationBroker;
use crate::change::TableChange;
use crate::config::ObserverConfig;
use crate::connection::ObservableConnection;
use crate::error::Error;
use crate::schema::query_table_info;

/// SQLite database observer with transaction-safe change notifications.
///
/// Uses SQLite's native preupdate_hook, commit_hook, and rollback_hook for
/// change detection. Changes are buffered during transactions and only
/// published to subscribers once committed and readable. Rolled-back transactions
/// produce no notifications.
///
/// # SQLite Version Requirements
///
/// Requires SQLite library compiled with `SQLITE_ENABLE_PREUPDATE_HOOK`.
pub struct SqliteObserver {
   pool: SqlitePool,
   broker: Arc<ObservationBroker>,
   config: ObserverConfig,
}

impl SqliteObserver {
   /// Creates a new observer for the given connection pool.
   ///
   /// Tables specified in the config will be automatically observed.
   pub fn new(pool: SqlitePool, config: ObserverConfig) -> Self {
      let broker = ObservationBroker::new(config.channel_capacity, config.capture_values);

      if !config.tables.is_empty() {
         broker.observe_tables(config.tables.iter().map(String::as_str));
      }

      Self {
         pool,
         broker,
         config,
      }
   }

   /// Subscribes to change notifications for the specified tables.
   ///
   /// If additional tables are provided, they will be added to the observed set.
   /// Returns a broadcast receiver that will receive `TableChange` events
   /// once a committed transaction's changes are readable.
   pub fn subscribe<I, S>(&self, tables: I) -> broadcast::Receiver<TableChange>
   where
      I: IntoIterator<Item = S>,
      S: Into<String>,
   {
      let tables: Vec<String> = tables.into_iter().map(Into::into).collect();
      if !tables.is_empty() {
         self
            .broker
            .observe_tables(tables.iter().map(String::as_str));
      }
      self.broker.subscribe()
   }

   /// Subscribes to change notifications as a Stream.
   ///
   /// Returns a `TableChangeStream` that implements `futures::Stream`.
   /// If tables are specified, the stream will only yield changes for those tables.
   pub fn subscribe_stream<I, S>(&self, tables: I) -> crate::stream::TableChangeStream
   where
      I: IntoIterator<Item = S>,
      S: Into<String>,
   {
      use crate::stream::TableChangeStreamExt;
      let tables: Vec<String> = tables.into_iter().map(Into::into).collect();
      // Register tables for observation (uses references, avoids clone)
      if !tables.is_empty() {
         self
            .broker
            .observe_tables(tables.iter().map(String::as_str));
      }
      let rx = self.broker.subscribe();
      let stream = rx.into_stream();
      if tables.is_empty() {
         stream
      } else {
         stream.filter_tables(tables)
      }
   }

   /// Acquires a connection from the pool with observation hooks registered.
   ///
   /// The returned connection will track changes to observed tables.
   ///
   /// On first acquisition for each table, queries the schema to determine
   /// primary key columns and WITHOUT ROWID status.
   pub async fn acquire(&self) -> Result<ObservableConnection> {
      let conn = self.pool.acquire().await.map_err(|_| Error::PoolAcquire)?;
      let mut observable = ObservableConnection::new(conn, Arc::clone(&self.broker));

      // Query table info for any observed tables that don't have it yet
      self.ensure_table_info(&mut observable).await?;

      observable.register_hooks().await?;
      debug!("Acquired observable connection with hooks registered");
      Ok(observable)
   }

   /// Ensures TableInfo is set for all observed tables.
   async fn ensure_table_info(&self, conn: &mut ObservableConnection) -> Result<()> {
      let observed = self.broker.get_observed_tables();

      for table in observed {
         if self.broker.get_table_info(&table).is_none() {
            match query_table_info(conn, &table).await {
               Ok(Some(info)) => {
                  debug!(table = %table, pk_columns = ?info.pk_columns, without_rowid = info.without_rowid, "Queried table info");
                  self.broker.set_table_info(&table, info);
               }
               Ok(None) => {
                  warn!(table = %table, "Table not found in schema");
               }
               Err(e) => {
                  warn!(table = %table, error = %e, "Failed to query table info");
               }
            }
         }
      }

      Ok(())
   }

   /// Acquires a connection and registers additional tables for observation.
   ///
   /// The specified tables are added to the observed set before acquiring.
   pub async fn acquire_and_observe(&self, tables: &[&str]) -> Result<ObservableConnection> {
      self.broker.observe_tables(tables.iter().copied());
      self.acquire().await
   }

   /// Returns a reference to the underlying connection pool.
   pub fn pool(&self) -> &SqlitePool {
      &self.pool
   }

   /// Returns a reference to the observer configuration.
   pub fn config(&self) -> &ObserverConfig {
      &self.config
   }

   /// Returns a list of tables currently being observed.
   pub fn observed_tables(&self) -> Vec<String> {
      self.broker.get_observed_tables()
   }

   /// Returns a reference to the underlying observation broker.
   pub fn broker(&self) -> &Arc<ObservationBroker> {
      &self.broker
   }
}

impl Clone for SqliteObserver {
   fn clone(&self) -> Self {
      Self {
         pool: self.pool.clone(),
         broker: Arc::clone(&self.broker),
         config: self.config.clone(),
      }
   }
}
