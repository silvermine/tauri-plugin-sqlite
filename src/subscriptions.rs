//! Observer integration for the Tauri plugin.
//!
//! This module provides the bridge between the sqlx-sqlite-observer crate and
//! Tauri's IPC layer, converting observer types to serializable payloads and
//! managing active subscription state.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sqlx_sqlite_toolkit::DatabaseWrapper;
use tokio::sync::{RwLock, RwLockWriteGuard};
use tracing::debug;

use sqlx_sqlite_observer::{ChangeOperation, ColumnValue, TableChange, TableChangeEvent};

/// A held write guard on `DbInstances`'s inner map, passed to every mutating
/// `ObserverRegistrations` method as proof the caller already holds it.
///
/// This is a **witness, not a resource**: these methods never read or write
/// through it, only require its existence for the duration of the call. See
/// `ObserverRegistrations`'s doc comment and the module doc in
/// `src/commands.rs` for why - do not "simplify" a call site by fetching a
/// value out of it.
///
/// The inner guard is intentionally private: that makes
/// [`DbInstances::write`](crate::DbInstances::write) the only way to construct
/// one, so "no db guard held at all" - the shape that actually regressed four
/// times - is a compile error at every call site. Were the field public, or
/// were this a plain type alias for
/// `RwLockWriteGuard<'_, HashMap<String, DatabaseWrapper>>`, *any* write guard
/// on *any* `HashMap<String, DatabaseWrapper>` would satisfy the parameter.
///
/// What the token does *not* prove is which `DbInstances` the guard came from.
/// `DbInstances` is `pub`, implements `Default`, and has a `pub fn new`, so
/// `DbInstances::default().write().await` yields a perfectly valid witness over
/// an empty throwaway map - this module's own tests rely on exactly that (see
/// `tests::dummy_db_lock`). So it means "the caller holds *a* `DbInstances`
/// write guard"; that it is *this app's* rests on Tauri managing one instance
/// per type, not on the type system. Nor can it see acquisition order, or a
/// guard dropped and reacquired mid-sequence - see the module doc in
/// `src/commands.rs` for which tests cover those, and how well.
pub struct DbInstancesGuard<'a>(RwLockWriteGuard<'a, HashMap<String, DatabaseWrapper>>);

impl std::ops::Deref for DbInstancesGuard<'_> {
   type Target = HashMap<String, DatabaseWrapper>;

   fn deref(&self) -> &Self::Target {
      &self.0
   }
}

impl std::ops::DerefMut for DbInstancesGuard<'_> {
   fn deref_mut(&mut self) -> &mut Self::Target {
      &mut self.0
   }
}

impl crate::DbInstances {
   /// Acquires this `DbInstances`'s write lock, wrapped as the
   /// [`DbInstancesGuard`] witness that every mutating `ObserverRegistrations`
   /// method requires. This is the *only* way to construct that witness - see
   /// its doc comment for what that does and does not prove.
   ///
   /// Defined here, alongside `DbInstancesGuard`, rather than next to
   /// `DbInstances`'s own definition - that's what lets it (and only it)
   /// reach `DbInstancesGuard`'s private field.
   pub(crate) async fn write(&self) -> DbInstancesGuard<'_> {
      DbInstancesGuard(self.inner.write().await)
   }
}

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
///
/// # These field types are load-bearing for lock safety
///
/// `active_subs` is locked in both orders relative to `db_instances`'s lock
/// (see the "`ActiveSubscriptions` is a separate pair" section of the module
/// doc in `src/commands.rs`). That is deadlock-free only because nothing
/// reachable from this struct can reach back for another lock: an
/// `AbortHandle` schedules cancellation on the runtime rather than running
/// destructors inline, and a `String` does nothing at all. Adding a field
/// holding a `DatabaseWrapper`, a `DbInstances`, an `AppHandle`, or anything
/// whose `Drop` touches plugin state would let dropping an entry inside
/// `ActiveSubscriptions`'s own lock reach for the db lock - turning that
/// inconsistent order into a real lock cycle.
struct ActiveSubscription {
   /// Abort handle for the subscription forwarding task.
   abort_handle: tokio::task::AbortHandle,
   /// Database key this subscription is for.
   db_key: String,
}

/// Global state tracking all active observer subscriptions.
#[derive(Clone, Default)]
pub struct ActiveSubscriptions(Arc<RwLock<HashMap<String, ActiveSubscription>>>);

impl ActiveSubscriptions {
   /// Insert a new subscription.
   pub async fn insert(&self, id: String, db_key: String, abort_handle: tokio::task::AbortHandle) {
      let mut subs = self.0.write().await;
      subs.insert(
         id,
         ActiveSubscription {
            abort_handle,
            db_key,
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
   pub async fn remove_for_db(&self, db_key: &str) {
      let mut subs = self.0.write().await;
      let keys_to_remove: Vec<String> = subs
         .iter()
         .filter(|(_, sub)| sub.db_key == db_key)
         .map(|(k, _)| k.clone())
         .collect();

      for key in keys_to_remove {
         if let Some(sub) = subs.remove(&key) {
            sub.abort_handle.abort();
         }
      }
   }

   /// Count active subscriptions for a specific database.
   pub async fn count_for_db(&self, db_key: &str) -> usize {
      let subs = self.0.read().await;
      subs.values().filter(|sub| sub.db_key == db_key).count()
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

/// Tracks which webview windows currently hold an active `observe()` registration
/// for each database, keyed by database key and webview label.
///
/// Observation is additive and reference-counted: multiple windows can call
/// `observe()` on the same database independently, and the underlying broker
/// (and its subscribers) is only torn down once every window that registered has
/// released its registration via `unobserve()`. See issue #54 — previously,
/// re-calling `observe()` unconditionally destroyed the existing broker, silently
/// terminating every other window's subscriptions.
///
/// The webview label is used as the observer identity because it is the only
/// caller-scoped handle already available to Tauri commands without adding a new
/// argument to the JS-facing API.
///
/// # Lock order: `DbInstances` first
///
/// Every mutating method here (`register`, `release`, `release_all_for_label`,
/// `clear_for_db`, `clear_all`) takes a [`DbInstancesGuard`] as its first
/// parameter - a witness proving the caller already holds `DbInstances`'s write
/// lock.
///
/// The defect this prevents: mutating `observer_regs` without holding the
/// `db_instances` lock lets a concurrent `observe()` register into - or a
/// concurrent teardown destroy - a broker the other side doesn't know is being
/// touched, leaving the refcount and the broker's actual state disagreeing.
/// That shape was reintroduced four separate times while the rule existed only
/// as prose in this comment. The witness makes a call site holding *no* db
/// guard fail to compile instead of merely being wrong. It cannot see
/// acquisition order, or a guard dropped and reacquired mid-sequence; those are
/// covered by tests only, one of them probabilistically - see the module doc in
/// `src/commands.rs`.
#[derive(Clone, Default)]
pub struct ObserverRegistrations(Arc<RwLock<HashMap<String, HashSet<String>>>>);

impl ObserverRegistrations {
   /// Acquires and holds this registry's internal write lock.
   ///
   /// Test-only. Lets a test hold the `observer_regs` lock itself and then
   /// assert that a concurrent `observe()`/`unobserve()` call blocks trying to
   /// acquire it - which can only happen if that call is still holding the
   /// `db_instances` lock at the point it reaches its `register()`/`release()`
   /// call. This proves the lock-order invariant deterministically, rather than
   /// relying on probabilistic scheduling to expose a violation.
   #[cfg(test)]
   pub async fn lock_for_test(
      &self,
   ) -> tokio::sync::RwLockWriteGuard<'_, HashMap<String, HashSet<String>>> {
      self.0.write().await
   }

   /// Registers `webview_label` as an observer of `db_key`.
   ///
   /// Idempotent: registering the same label for the same database more than once
   /// (e.g. a window calling `observe()` again to add more tables) does not
   /// increase the refcount. Returns the number of distinct observing webviews for
   /// this database after registering.
   ///
   /// # Granularity is per webview, not per caller
   ///
   /// Because the identity is the webview label, that idempotency is not limited
   /// to one logical caller. Two independent frontend modules in the *same*
   /// window that each call `observe()` collapse into a single registration, so
   /// whichever of them calls `unobserve()` first drives the refcount to zero and
   /// triggers the full teardown - aborting the other module's subscriptions. A
   /// window therefore needs a single owner of the `observe()`/`unobserve()`
   /// pair; "reference-counted" does not make `unobserve()` locally safe within a
   /// window. `subscribe()`'s registration check does not help here either, since
   /// both modules share the label and both pass it.
   ///
   /// Note that scoping teardown by webview label would not fix this: both
   /// modules share the label, so a label-scoped abort covers the identical set,
   /// and teardown drops the broker regardless. Fixing it properly needs a
   /// per-caller registration token or a frontend-side refcount, which is a
   /// public API change.
   ///
   /// `_db_guard` is a witness that the caller already holds `DbInstances`'s
   /// write lock for the duration of this call - see the lock-order doc above.
   /// It is never read through.
   pub async fn register(
      &self,
      _db_guard: &mut DbInstancesGuard<'_>,
      db_key: &str,
      webview_label: &str,
   ) -> usize {
      let mut regs = self.0.write().await;
      let labels = regs.entry(db_key.to_string()).or_default();
      labels.insert(webview_label.to_string());
      labels.len()
   }

   /// Releases `webview_label`'s observation registration for `db_key`.
   ///
   /// Returns `None` if `webview_label` was never registered as an observer of
   /// `db_key`, which the caller must treat as "nothing to do" rather than
   /// as "the last observer just left". Collapsing both to a plain `0` would
   /// mean a window calling `unobserve()` without ever having called
   /// `observe()` triggers a full broker teardown - destroying a broker other
   /// windows are still legitimately using.
   ///
   /// Otherwise returns `Some(remaining)`, the number of distinct observing
   /// webviews left registered for this database. `Some(0)` means this was the
   /// last registered observer and the caller should tear down the broker (via
   /// `DatabaseWrapper::disable_observation`) and any remaining subscriptions.
   ///
   /// `_db_guard` is a witness - see [`register`](Self::register).
   pub async fn release(
      &self,
      _db_guard: &mut DbInstancesGuard<'_>,
      db_key: &str,
      webview_label: &str,
   ) -> Option<usize> {
      let mut regs = self.0.write().await;
      let labels = regs.get_mut(db_key)?;

      if !labels.remove(webview_label) {
         return None;
      }

      if labels.is_empty() {
         regs.remove(db_key);
         Some(0)
      } else {
         Some(labels.len())
      }
   }

   /// Releases every registration held by `webview_label`, across all databases.
   ///
   /// Used when a webview window is destroyed without having explicitly called
   /// `unobserve()` first - otherwise its registration(s) would leak forever,
   /// keeping affected brokers alive indefinitely even though nothing is
   /// listening anymore (the "phantom registration" gap noted in the #54
   /// review). Returns the database keys that reached zero remaining observers
   /// as a result, so the caller can tear down their brokers.
   ///
   /// Note: webview labels are reusable across a window's lifetime. A window
   /// recreated later with the same static label silently "inherits" whatever
   /// registration state is left for that label - harmless for a fixed label
   /// like a single main window, but something to be aware of for dynamically
   /// labeled windows (e.g. `doc-{id}`), where a fresh window with a *new*
   /// label won't be affected by a previous window's leaked registration.
   ///
   /// `_db_guard` is a witness - see [`register`](Self::register).
   pub async fn release_all_for_label(
      &self,
      _db_guard: &mut DbInstancesGuard<'_>,
      webview_label: &str,
   ) -> Vec<String> {
      let mut regs = self.0.write().await;
      let mut newly_empty = Vec::new();

      regs.retain(|db_key, labels| {
         if !labels.remove(webview_label) {
            return true;
         }

         if labels.is_empty() {
            newly_empty.push(db_key.clone());
            false
         } else {
            true
         }
      });

      newly_empty
   }

   /// Returns the number of distinct webviews currently registered as observers
   /// of `db_key`. Returns `0` if there are none (or the database has never
   /// been observed).
   ///
   /// Test-only: production call sites use the counts already returned by
   /// [`register`](Self::register)/[`release`](Self::release) directly, but
   /// tests need a way to inspect the current count without mutating it (e.g.
   /// to assert an invariant against `DatabaseWrapper::is_observing()` from
   /// outside a `register`/`release` call).
   #[cfg(test)]
   pub async fn count_for_db(&self, db_key: &str) -> usize {
      let regs = self.0.read().await;
      regs.get(db_key).map_or(0, HashSet::len)
   }

   /// Returns whether `webview_label` is currently registered as an observer of
   /// `db_key`.
   ///
   /// Used by `subscribe()` to enforce that the calling webview called
   /// `observe()` for `db_key` itself, rather than merely riding along on some
   /// other window's registration while a broker happens to exist (see
   /// `subscribe()`'s doc comment in `src/commands.rs` and issue #54).
   ///
   /// Read-only, and deliberately takes no [`DbInstancesGuard`] witness - unlike
   /// `register`/`release`/etc., which mutate this registry and need the
   /// witness to enforce a lock-acquisition order relative to `db_instances`. A
   /// reader has no ordering to prove by itself; it's `subscribe()`'s job to
   /// hold `db_instances`'s read lock across both this check and
   /// `subscribe_stream()` to make the pair race-free against a concurrent
   /// `unobserve()`.
   pub(crate) async fn is_registered(&self, db_key: &str, webview_label: &str) -> bool {
      let regs = self.0.read().await;
      regs
         .get(db_key)
         .is_some_and(|labels| labels.contains(webview_label))
   }

   /// Clears all observer registrations for a single database.
   ///
   /// Used when a database is fully closed or removed (`close()`/`remove()`),
   /// which tears down observation unconditionally regardless of how many
   /// windows had registered. Without this, stale registrations would survive a
   /// close/reload cycle and understate how many *new* observers are needed
   /// before the broker on the freshly (re)loaded database is torn down again.
   ///
   /// `_db_guard` is a witness - see [`register`](Self::register).
   pub async fn clear_for_db(&self, _db_guard: &mut DbInstancesGuard<'_>, db_key: &str) {
      let mut regs = self.0.write().await;
      regs.remove(db_key);
   }

   /// Clears all observer registrations for every database (app exit / close_all).
   ///
   /// `_db_guard` is a witness - see [`register`](Self::register).
   pub async fn clear_all(&self, _db_guard: &mut DbInstancesGuard<'_>) {
      let mut regs = self.0.write().await;
      debug!(
         "Clearing observer registrations for {} database(s)",
         regs.len()
      );
      regs.clear();
   }
}

#[cfg(test)]
mod tests {
   use super::*;

   /// Test-only: an empty, never-populated `DbInstances`, used purely to
   /// obtain real [`DbInstancesGuard`] witnesses (via its own `write()`) for
   /// exercising `ObserverRegistrations`'s mutating methods in isolation.
   /// Going through `DbInstances::write()` - rather than building a throwaway
   /// `RwLock<HashMap<...>>` directly - is required now that
   /// `DbInstancesGuard`'s field is private: there is no
   /// other way to construct one, which is the point.
   fn dummy_db_lock() -> crate::DbInstances {
      crate::DbInstances::default()
   }

   #[tokio::test]
   async fn test_register_is_additive_across_distinct_labels() {
      let regs = ObserverRegistrations::default();
      let db_lock = dummy_db_lock();

      assert_eq!(
         regs
            .register(&mut db_lock.write().await, "MAIN", "window-a")
            .await,
         1
      );
      assert_eq!(
         regs
            .register(&mut db_lock.write().await, "MAIN", "window-b")
            .await,
         2
      );
   }

   #[tokio::test]
   async fn test_register_same_label_twice_is_idempotent() {
      let regs = ObserverRegistrations::default();
      let db_lock = dummy_db_lock();

      assert_eq!(
         regs
            .register(&mut db_lock.write().await, "MAIN", "window-a")
            .await,
         1
      );
      // Same window calling observe() again (e.g. to add more tables) must not
      // inflate the refcount.
      assert_eq!(
         regs
            .register(&mut db_lock.write().await, "MAIN", "window-a")
            .await,
         1
      );
   }

   #[tokio::test]
   async fn test_release_keeps_broker_live_until_last_observer_leaves() {
      let regs = ObserverRegistrations::default();
      let db_lock = dummy_db_lock();

      regs
         .register(&mut db_lock.write().await, "MAIN", "window-a")
         .await;
      regs
         .register(&mut db_lock.write().await, "MAIN", "window-b")
         .await;

      // One of two observers releases: broker must stay live (non-zero remaining).
      assert_eq!(
         regs
            .release(&mut db_lock.write().await, "MAIN", "window-b")
            .await,
         Some(1)
      );

      // The last observer releases: broker should now be torn down.
      assert_eq!(
         regs
            .release(&mut db_lock.write().await, "MAIN", "window-a")
            .await,
         Some(0)
      );
   }

   #[tokio::test]
   async fn test_release_unknown_label_or_db_is_a_noop() {
      let regs = ObserverRegistrations::default();
      let db_lock = dummy_db_lock();

      // Releasing from a db_key with no registrations at all is "never
      // registered", not "last observer released" - `None`, not `Some(0)`.
      assert_eq!(
         regs
            .release(&mut db_lock.write().await, "MAIN", "window-a")
            .await,
         None
      );

      regs
         .register(&mut db_lock.write().await, "MAIN", "window-a")
         .await;
      // Releasing a label that was never registered, for a db_key that DOES
      // have other registrations, is also a no-op: window-a's registration is
      // left untouched.
      assert_eq!(
         regs
            .release(&mut db_lock.write().await, "MAIN", "window-unknown")
            .await,
         None
      );
      assert_eq!(regs.count_for_db("MAIN").await, 1);
   }

   #[tokio::test]
   async fn test_release_all_for_label_reports_only_databases_that_reached_zero() {
      let regs = ObserverRegistrations::default();
      let db_lock = dummy_db_lock();

      regs
         .register(&mut db_lock.write().await, "MAIN", "window-a")
         .await;
      regs
         .register(&mut db_lock.write().await, "MAIN", "window-b")
         .await;
      regs
         .register(&mut db_lock.write().await, "OTHER", "window-a")
         .await;

      let mut newly_empty = regs
         .release_all_for_label(&mut db_lock.write().await, "window-a")
         .await;
      newly_empty.sort();

      // MAIN still has window-b, so it must not be reported as newly empty.
      // OTHER had only window-a, so it must be.
      assert_eq!(newly_empty, vec!["OTHER".to_string()]);
      assert_eq!(regs.count_for_db("MAIN").await, 1);
      assert_eq!(regs.count_for_db("OTHER").await, 0);
   }

   #[tokio::test]
   async fn test_release_all_for_label_is_a_noop_for_unregistered_label() {
      let regs = ObserverRegistrations::default();
      let db_lock = dummy_db_lock();

      regs
         .register(&mut db_lock.write().await, "MAIN", "window-a")
         .await;

      assert!(
         regs
            .release_all_for_label(&mut db_lock.write().await, "window-unknown")
            .await
            .is_empty()
      );
      assert_eq!(regs.count_for_db("MAIN").await, 1);
   }

   #[tokio::test]
   async fn test_count_for_db_reflects_distinct_observers() {
      let regs = ObserverRegistrations::default();
      let db_lock = dummy_db_lock();

      assert_eq!(regs.count_for_db("MAIN").await, 0);

      regs
         .register(&mut db_lock.write().await, "MAIN", "window-a")
         .await;
      regs
         .register(&mut db_lock.write().await, "MAIN", "window-a")
         .await; // idempotent
      regs
         .register(&mut db_lock.write().await, "MAIN", "window-b")
         .await;

      assert_eq!(regs.count_for_db("MAIN").await, 2);
   }

   #[tokio::test]
   async fn test_clear_for_db_only_clears_target_database() {
      let regs = ObserverRegistrations::default();
      let db_lock = dummy_db_lock();

      regs
         .register(&mut db_lock.write().await, "MAIN", "window-a")
         .await;
      regs
         .register(&mut db_lock.write().await, "OTHER", "window-a")
         .await;

      regs.clear_for_db(&mut db_lock.write().await, "MAIN").await;

      // MAIN was cleared, so a fresh single registration starts back at 1.
      assert_eq!(
         regs
            .register(&mut db_lock.write().await, "MAIN", "window-a")
            .await,
         1
      );
      // OTHER was untouched, so releasing its only observer reaches zero.
      assert_eq!(
         regs
            .release(&mut db_lock.write().await, "OTHER", "window-a")
            .await,
         Some(0)
      );
   }

   #[tokio::test]
   async fn test_clear_all_clears_every_database() {
      let regs = ObserverRegistrations::default();
      let db_lock = dummy_db_lock();

      regs
         .register(&mut db_lock.write().await, "MAIN", "window-a")
         .await;
      regs
         .register(&mut db_lock.write().await, "OTHER", "window-a")
         .await;

      regs.clear_all(&mut db_lock.write().await).await;

      assert_eq!(
         regs
            .register(&mut db_lock.write().await, "MAIN", "window-a")
            .await,
         1
      );
      assert_eq!(
         regs
            .register(&mut db_lock.write().await, "OTHER", "window-a")
            .await,
         1
      );
   }
}
