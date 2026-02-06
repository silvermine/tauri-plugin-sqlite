//! Transaction-aware observation broker for buffering and publishing changes.
//!
//! This module provides transaction-safe change notifications. Changes are buffered
//! during transactions (explicit and implicit) and only published after successful
//! commit. Rolled-back transactions produce no notifications.
//!
//! # Data Flow
//!
//! ```text
//! ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
//! │ preupdate_hook  │────►│  broker.buffer  │     │   subscribers   │
//! │ (captures data) │     │  (Vec<Event>)   │     │                 │
//! └─────────────────┘     └────────┬────────┘     └─────────────────┘
//!                                  │                       ▲
//!                     ┌────────────┼────────────┐          │
//!                     │            │            │          │
//!               ┌─────▼────┐  ┌────▼─────┐      │          │
//!               │  COMMIT  │  │ ROLLBACK │      │          │
//!               └─────┬────┘  └────┬─────┘      │          │
//!                     │            │            │          │
//!                     ▼            ▼            │          │
//!               on_commit()   on_rollback()     │          │
//!                     │            │            │          │
//!                     │       buffer.clear()    │          │
//!                     │       (discard)         │          │
//!                     │                         │          │
//!                     └─────────────────────────┴──────────┘
//!                             change_tx.send()
//!                             (publish)
//! ```
//!
//! Changes captured by the preupdate hook are buffered until the transaction
//! (explicit or implicit) completes. On commit, buffered changes are published
//! to subscribers. On rollback, they are discarded without notification.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::{Mutex, RwLock};
use tokio::sync::broadcast;
use tracing::{debug, error, trace};

use crate::change::{ChangeOperation, ColumnValue, TableChange, TableInfo};
use crate::hooks::{PreUpdateEvent, SqliteValue};

/// Transaction-aware observation broker.
///
/// Buffers preupdate events during transactions and publishes them to
/// subscribers only after successful commit. Rolled-back transactions
/// have their buffered changes discarded.
pub struct ObservationBroker {
   buffer: Mutex<Vec<PreUpdateEvent>>,
   change_tx: broadcast::Sender<TableChange>,
   observed_tables: RwLock<HashSet<String>>,
   table_info: RwLock<HashMap<String, TableInfo>>,
   capture_values: bool,
}

impl ObservationBroker {
   /// Creates a new broker with the specified broadcast channel capacity.
   pub fn new(channel_capacity: usize, capture_values: bool) -> Arc<Self> {
      let (change_tx, _) = broadcast::channel(channel_capacity);
      Arc::new(Self {
         buffer: Mutex::new(Vec::new()),
         change_tx,
         observed_tables: RwLock::new(HashSet::new()),
         table_info: RwLock::new(HashMap::new()),
         capture_values,
      })
   }

   /// Checks if a table is being observed.
   pub fn is_table_observed(&self, table: &str) -> bool {
      self.observed_tables.read().contains(table)
   }

   /// Registers a table for observation with its schema information.
   ///
   /// Only changes to observed tables will be buffered and published.
   /// The `TableInfo` is required to correctly extract primary key values
   /// and determine whether the rowid is meaningful for the table.
   pub fn observe_table(&self, table: &str, info: TableInfo) {
      trace!(
         table = %table,
         pk_columns = ?info.pk_columns,
         without_rowid = info.without_rowid,
         "Observing table with schema info"
      );
      self.observed_tables.write().insert(table.to_string());
      self.table_info.write().insert(table.to_string(), info);
   }

   /// Gets the schema information for an observed table.
   pub fn get_table_info(&self, table: &str) -> Option<TableInfo> {
      self.table_info.read().get(table).cloned()
   }

   /// Returns a list of all observed tables.
   pub fn get_observed_tables(&self) -> Vec<String> {
      self.observed_tables.read().iter().cloned().collect()
   }

   /// Called by preupdate_hook - buffers the event for later processing.
   ///
   /// Events are held in the buffer until either `on_commit()` (publish)
   /// or `on_rollback()` (discard) is called.
   pub fn on_preupdate(&self, event: PreUpdateEvent) {
      trace!(
          table = %event.table,
          operation = ?event.operation,
          "Buffering preupdate event"
      );
      self.buffer.lock().push(event);
   }

   /// Called by commit_hook - flushes buffered events to subscribers.
   ///
   /// Converts all buffered `PreUpdateEvent`s to `TableChange`s and sends
   /// them through the broadcast channel. The buffer is cleared afterward.
   pub fn on_commit(&self) {
      let events: Vec<PreUpdateEvent> = {
         let mut buffer = self.buffer.lock();
         std::mem::take(&mut *buffer)
      };

      if events.is_empty() {
         return;
      }

      debug!(count = events.len(), "Flushing buffered changes on commit");

      for event in events {
         match self.event_to_change(event) {
            Ok(table_change) => {
               let _ = self.change_tx.send(table_change);
            }
            Err(e) => {
               error!(error = %e, "Failed to convert event to change");
            }
         }
      }
   }

   /// Called by rollback_hook - discards all buffered events.
   ///
   /// Clears the buffer without publishing any changes to subscribers.
   pub fn on_rollback(&self) {
      let count = {
         let mut buffer = self.buffer.lock();
         let count = buffer.len();
         buffer.clear();
         count
      };

      if count > 0 {
         debug!(count, "Discarding buffered changes on rollback");
      }
   }

   /// Subscribes to change notifications.
   ///
   /// Returns a broadcast receiver that will receive `TableChange` events
   /// after transactions commit.
   pub fn subscribe(&self) -> broadcast::Receiver<TableChange> {
      self.change_tx.subscribe()
   }

   /// Converts a PreUpdateEvent to a TableChange for broadcast.
   fn event_to_change(&self, event: PreUpdateEvent) -> crate::Result<TableChange> {
      let table_info = self.table_info.read().get(&event.table).cloned();

      // For WITHOUT ROWID tables, the rowid from preupdate hook is not meaningful
      let rowid = match &table_info {
         Some(info) if info.without_rowid => None,
         _ => match event.operation {
            ChangeOperation::Insert => Some(event.new_rowid),
            ChangeOperation::Delete => Some(event.old_rowid),
            ChangeOperation::Update => Some(event.new_rowid),
         },
      };

      // Extract primary key values from the appropriate column values
      let primary_key = self.extract_primary_key(&event, table_info.as_ref())?;

      let (old_values, new_values) = if self.capture_values {
         (
            event.old_values.map(Self::values_to_vec),
            event.new_values.map(Self::values_to_vec),
         )
      } else {
         (None, None)
      };

      Ok(TableChange {
         table: event.table,
         operation: Some(event.operation),
         rowid,
         primary_key,
         old_values,
         new_values,
         timestamp: Instant::now(),
      })
   }

   /// Extracts primary key values from the event based on table schema.
   ///
   /// Returns an error if the schema has drifted (e.g., table was altered)
   /// and PK column indices are out of bounds.
   fn extract_primary_key(
      &self,
      event: &PreUpdateEvent,
      table_info: Option<&TableInfo>,
   ) -> crate::Result<Vec<ColumnValue>> {
      let Some(info) = table_info else {
         return Ok(Vec::new());
      };

      if info.pk_columns.is_empty() {
         return Ok(Vec::new());
      }

      // For DELETE, use old values; for INSERT/UPDATE, use new values
      let values = match event.operation {
         ChangeOperation::Delete => event.old_values.as_ref(),
         ChangeOperation::Insert | ChangeOperation::Update => event.new_values.as_ref(),
      };

      let Some(values) = values else {
         return Ok(Vec::new());
      };

      // Extract values at the PK column indices, erroring if any index is out of bounds
      let mut pk_values = Vec::with_capacity(info.pk_columns.len());
      for &idx in &info.pk_columns {
         match values.get(idx) {
            Some(v) => pk_values.push(v.clone().into()),
            None => {
               return Err(crate::Error::SchemaMismatch {
                  table: event.table.clone(),
                  expected: info.pk_columns.len(),
                  actual: values.len(),
               });
            }
         }
      }
      Ok(pk_values)
   }

   /// Converts SqliteValue vec to ColumnValue vec for TableChange.
   fn values_to_vec(values: Vec<SqliteValue>) -> Vec<crate::change::ColumnValue> {
      values.into_iter().map(|v| v.into()).collect()
   }
}

impl std::fmt::Debug for ObservationBroker {
   fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
      f.debug_struct("ObservationBroker")
         .field("buffer_len", &self.buffer.lock().len())
         .field("observed_tables", &self.observed_tables.read().len())
         .finish()
   }
}
