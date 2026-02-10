//! Observer integration for the Tauri plugin.
//!
//! This module provides the bridge between the sqlx-sqlite-observer crate and
//! Tauri's IPC layer, converting observer types to serializable payloads and
//! managing active subscription state.

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::debug;

use sqlx_sqlite_observer::{ChangeOperation, ColumnValue, TableChange, TableChangeEvent};

/// Serializable column value for IPC transport.
///
/// Maps observer's `ColumnValue` to a tagged enum that can be sent to the frontend.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", content = "value")]
#[serde(rename_all = "camelCase")]
pub enum ColumnValuePayload {
   Null,
   Integer(i64),
   Real(f64),
   Text(String),
   Blob(String), // base64-encoded
}

impl From<&ColumnValue> for ColumnValuePayload {
   fn from(value: &ColumnValue) -> Self {
      match value {
         ColumnValue::Null => ColumnValuePayload::Null,
         ColumnValue::Integer(i) => ColumnValuePayload::Integer(*i),
         ColumnValue::Real(r) => ColumnValuePayload::Real(*r),
         ColumnValue::Text(s) => ColumnValuePayload::Text(s.clone()),
         ColumnValue::Blob(b) => {
            use base64::Engine;
            ColumnValuePayload::Blob(base64::engine::general_purpose::STANDARD.encode(b))
         }
      }
   }
}

/// Serializable change data for a single table change.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TableChangeData {
   pub table: String,
   pub operation: Option<String>,
   pub rowid: Option<i64>,
   pub primary_key: Vec<ColumnValuePayload>,
   #[serde(skip_serializing_if = "Option::is_none")]
   pub old_values: Option<Vec<ColumnValuePayload>>,
   #[serde(skip_serializing_if = "Option::is_none")]
   pub new_values: Option<Vec<ColumnValuePayload>>,
}

/// Serializable event payload sent to the frontend via Tauri Channel.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event", content = "data")]
#[serde(rename_all = "camelCase")]
pub enum TableChangePayload {
   Change(TableChangeData),
   Lagged { count: u64 },
}

/// Convert an observer `TableChangeEvent` to a serializable payload.
pub fn event_to_payload(event: TableChangeEvent) -> TableChangePayload {
   match event {
      TableChangeEvent::Change(change) => TableChangePayload::Change(change_to_data(&change)),
      TableChangeEvent::Lagged(count) => TableChangePayload::Lagged { count },
   }
}

/// Convert an observer `TableChange` to serializable data.
fn change_to_data(change: &TableChange) -> TableChangeData {
   TableChangeData {
      table: change.table.clone(),
      operation: change.operation.map(|op| match op {
         ChangeOperation::Insert => "insert".to_string(),
         ChangeOperation::Update => "update".to_string(),
         ChangeOperation::Delete => "delete".to_string(),
      }),
      rowid: change.rowid,
      primary_key: change
         .primary_key
         .iter()
         .map(ColumnValuePayload::from)
         .collect(),
      old_values: change
         .old_values
         .as_ref()
         .map(|vals| vals.iter().map(ColumnValuePayload::from).collect()),
      new_values: change
         .new_values
         .as_ref()
         .map(|vals| vals.iter().map(ColumnValuePayload::from).collect()),
   }
}

/// Observer config params from the frontend.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObserverConfigParams {
   /// Capacity of the broadcast channel. Default: 256.
   pub channel_capacity: Option<usize>,
   /// Whether to capture column values in change notifications. Default: true.
   pub capture_values: Option<bool>,
}

/// Tracks an active subscription's abort handle.
struct ActiveSubscription {
   /// Abort handle for the subscription forwarding task.
   abort_handle: tokio::task::AbortHandle,
   /// Database path this subscription is for.
   db_path: String,
}

/// Global state tracking all active observer subscriptions.
#[derive(Clone, Default)]
pub struct ActiveSubscriptions(Arc<RwLock<HashMap<String, ActiveSubscription>>>);

impl ActiveSubscriptions {
   /// Insert a new subscription.
   pub async fn insert(&self, id: String, db_path: String, abort_handle: tokio::task::AbortHandle) {
      let mut subs = self.0.write().await;
      subs.insert(
         id,
         ActiveSubscription {
            abort_handle,
            db_path,
         },
      );
   }

   /// Remove and abort a subscription. Returns true if found.
   pub async fn remove(&self, id: &str) -> bool {
      let mut subs = self.0.write().await;
      if let Some(sub) = subs.remove(id) {
         sub.abort_handle.abort();
         true
      } else {
         false
      }
   }

   /// Remove and abort all subscriptions for a specific database.
   pub async fn remove_for_db(&self, db_path: &str) {
      let mut subs = self.0.write().await;
      let keys_to_remove: Vec<String> = subs
         .iter()
         .filter(|(_, sub)| sub.db_path == db_path)
         .map(|(k, _)| k.clone())
         .collect();

      for key in keys_to_remove {
         if let Some(sub) = subs.remove(&key) {
            sub.abort_handle.abort();
         }
      }
   }

   /// Abort all subscriptions (for cleanup on app exit).
   pub async fn abort_all(&self) {
      let mut subs = self.0.write().await;
      debug!("Aborting {} active subscription(s)", subs.len());
      for (_, sub) in subs.drain() {
         sub.abort_handle.abort();
      }
   }
}
