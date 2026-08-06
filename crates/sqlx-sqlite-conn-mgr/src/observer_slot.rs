//! Opaque, database-scoped slot for a higher layer's observation state.
//!
//! `sqlx-sqlite-conn-mgr` is the lowest crate in the observation stack and cannot
//! name the observer crate's types without an upward dependency, so this slot is
//! type-erased over `Arc<dyn Any + Send + Sync>`. Higher layers (currently
//! `sqlx-sqlite-toolkit`'s `DatabaseWrapper`, which stores an
//! `Arc<ObservationBroker>`) put a value here and downcast on read.
//!
//! Hanging the slot off [`SqliteDatabase`](crate::SqliteDatabase) rather than a
//! side registry means every handle that shares the same `Arc<SqliteDatabase>` -
//! clones of a wrapper and independent `connect()` calls to the same path alike -
//! shares one observation state (see issue #53). `:memory:` databases are excluded
//! from the path registry (`registry.rs`) and therefore never share a
//! `SqliteDatabase`, so they get independent observation for free with no special
//! casing here.

use std::any::Any;
use std::sync::Arc;

use parking_lot::RwLock;
use tracing::warn;

/// Database-scoped slot holding at most one type-erased value.
///
/// `sqlx-sqlite-conn-mgr` never interprets the contents - it only stores and
/// hands back the `Arc` a higher layer gave it. The only way to *populate* the
/// slot is [`get_or_init`](Self::get_or_init) or
/// [`get_or_init_with`](Self::get_or_init_with), both of which reuse whatever is
/// already there instead of overwriting it, so nothing can replace a stored
/// value with one of another type ([`clear`](Self::clear) can only empty the
/// slot). A caller can still populate two different `T`s across a clear, which
/// remains a programming error rather than a supported use case; see
/// [`get`](Self::get) for the never-panicking behavior when it happens.
#[derive(Default)]
pub struct ObserverSlot(RwLock<Option<Arc<dyn Any + Send + Sync>>>);

impl ObserverSlot {
   /// Empties the slot.
   pub fn clear(&self) {
      *self.0.write() = None;
   }

   /// Returns whether the slot currently holds a value, regardless of its type.
   pub fn is_set(&self) -> bool {
      self.0.read().is_some()
   }

   /// Returns the slot's value downcast to `T`, or `None` if the slot is empty.
   ///
   /// Clones the `Arc` out and drops the internal lock guard before returning,
   /// so no guard is ever observable to the caller or held across an `.await`.
   ///
   /// If the slot holds a value that is not a `T` - a programming error, since
   /// this slot is meant to hold one concrete type for its whole lifetime -
   /// this returns `None` rather than a wrong-typed value, after a
   /// `tracing::warn!`. It never panics, in debug or release.
   pub fn get<T: Any + Send + Sync>(&self) -> Option<Arc<T>> {
      let value = self.0.read().clone()?;
      match value.downcast::<T>() {
         Ok(typed) => Some(typed),
         Err(_) => {
            warn!(
               "ObserverSlot::get() requested a type that does not match the value \
                already stored in the slot; returning None instead of a wrong-typed \
                value. This indicates a programming error - the slot should only ever \
                hold one concrete type."
            );
            None
         }
      }
   }

   /// Atomically returns the existing value downcast to `T`, or creates one via
   /// `init` and stores it, if the slot is empty. The returned `bool` is `true`
   /// when `init` ran (a new value was created), `false` when an existing value
   /// was reused.
   ///
   /// The whole check-and-create happens under the slot's write lock, so two
   /// concurrent callers can never both observe an empty slot and each build and
   /// store their own value - the second caller always sees the first's value
   /// instead of silently overwriting it (and, with it, any subscribers already
   /// registered against the value it replaced).
   ///
   /// `init` runs synchronously while the write lock is held. It must not touch
   /// this slot (no reentrancy - the lock is not reentrant), block, or panic,
   /// since any of the three stalls or fails every other reader/writer of this
   /// database's observation state. A panic is not merely theoretical: the
   /// toolkit's `enable_observation()` builds an `ObservationBroker` in `init`,
   /// and that constructor asserts a non-zero channel capacity, so a direct
   /// Rust caller passing `ObserverConfig::with_channel_capacity(0)` unwinds
   /// out of an otherwise infallible call. The slot survives it intact: this is
   /// a `parking_lot::RwLock`, which does not poison, so the lock is released
   /// on unwind and the slot is simply left empty - as if the call never
   /// happened - rather than permanently unusable.
   ///
   /// Same downcast-mismatch behavior as [`get`](Self::get): if the slot already
   /// holds a value of some other type, this returns `None` rather than a
   /// wrong-typed value or a second, competing value of type `T`.
   ///
   /// A thin wrapper around [`get_or_init_with`](Self::get_or_init_with), which
   /// takes a callback for the reuse case as well.
   pub fn get_or_init<T, F>(&self, init: F) -> Option<(Arc<T>, bool)>
   where
      T: Any + Send + Sync,
      F: FnOnce() -> Arc<T>,
   {
      self.get_or_init_with(init, |_| {})
   }

   /// Same as [`get_or_init`](Self::get_or_init), but runs `on_existing` when an
   /// existing value is reused, still under the write lock that decided "reuse,
   /// don't create".
   ///
   /// That closes a window a caller can't close itself: acting on the existing
   /// value after this returns (merging new entries into a broker already stored
   /// here, say) leaves room for a concurrent [`clear`](Self::clear) in between,
   /// so the follow-up mutates a value the slot no longer holds.
   ///
   /// `on_existing` carries the same restrictions as `init` - no reentrancy, no
   /// blocking, no panicking. Exactly one of the two runs per call.
   pub fn get_or_init_with<T, F, G>(&self, init: F, on_existing: G) -> Option<(Arc<T>, bool)>
   where
      T: Any + Send + Sync,
      F: FnOnce() -> Arc<T>,
      G: FnOnce(&Arc<T>),
   {
      let mut guard = self.0.write();

      if let Some(existing) = guard.as_ref() {
         return match Arc::clone(existing).downcast::<T>() {
            Ok(typed) => {
               on_existing(&typed);
               Some((typed, false))
            }
            Err(_) => {
               warn!(
                  "ObserverSlot::get_or_init_with() requested a type that does not \
                   match the value already stored in the slot; returning None instead \
                   of a wrong-typed value. This indicates a programming error - the \
                   slot should only ever hold one concrete type. (Reached through \
                   get_or_init() if that is what the caller used.)"
               );
               None
            }
         };
      }

      let created = init();
      *guard = Some(Arc::clone(&created) as Arc<dyn Any + Send + Sync>);
      Some((created, true))
   }
}

// `dyn Any` doesn't implement `Debug`, so this can't be derived - `SqliteDatabase`
// derives `Debug` and needs this field to cooperate. Only report whether the slot
// is occupied, since the contents are opaque to this crate anyway.
impl std::fmt::Debug for ObserverSlot {
   fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
      f.debug_struct("ObserverSlot")
         .field("is_set", &self.is_set())
         .finish()
   }
}

#[cfg(test)]
mod tests {
   use super::*;

   #[test]
   fn empty_slot_reports_unset_and_none() {
      let slot = ObserverSlot::default();
      assert!(!slot.is_set());
      assert!(slot.get::<u32>().is_none());
   }

   #[test]
   fn get_or_init_then_get_round_trips() {
      let slot = ObserverSlot::default();
      slot.get_or_init(|| Arc::new(42_u32));
      assert!(slot.is_set());
      assert_eq!(*slot.get::<u32>().unwrap(), 42);
   }

   #[test]
   fn clear_empties_the_slot() {
      let slot = ObserverSlot::default();
      slot.get_or_init(|| Arc::new(42_u32));
      slot.clear();
      assert!(!slot.is_set());
      assert!(slot.get::<u32>().is_none());
   }

   #[test]
   fn get_with_wrong_type_returns_none_not_a_wrong_typed_value() {
      let slot = ObserverSlot::default();
      slot.get_or_init(|| Arc::new(42_u32));
      assert!(slot.get::<String>().is_none());
      // The original value is untouched by a mismatched read.
      assert_eq!(*slot.get::<u32>().unwrap(), 42);
   }

   #[test]
   fn get_or_init_creates_on_first_call_and_reuses_on_second() {
      let slot = ObserverSlot::default();

      let (first, created) = slot.get_or_init(|| Arc::new(1_u32)).unwrap();
      assert!(created);
      assert_eq!(*first, 1);

      let (second, created) = slot.get_or_init(|| Arc::new(999_u32)).unwrap();
      assert!(!created, "second call should reuse the existing value");
      assert_eq!(*second, 1, "second call must not replace the first value");
      assert!(Arc::ptr_eq(&first, &second));
   }

   #[test]
   fn get_or_init_with_wrong_type_returns_none() {
      let slot = ObserverSlot::default();
      slot.get_or_init(|| Arc::new(42_u32));
      assert!(slot.get_or_init(|| Arc::new(String::from("x"))).is_none());
   }

   #[test]
   fn get_or_init_with_runs_on_existing_only_on_reuse() {
      let slot = ObserverSlot::default();

      let (first, created) = slot
         .get_or_init_with(
            || Arc::new(1_u32),
            |_| panic!("on_existing must not run when init runs"),
         )
         .unwrap();
      assert!(created);
      assert_eq!(*first, 1);

      let mut seen: Option<u32> = None;
      let (second, created) = slot
         .get_or_init_with(
            || panic!("init must not run when the slot is already populated"),
            |existing| seen = Some(**existing),
         )
         .unwrap();
      assert!(!created);
      assert_eq!(*second, 1);
      assert_eq!(
         seen,
         Some(1),
         "on_existing should observe the existing value"
      );
   }

   /// Mirrors `get_or_init_holds_the_lock_for_the_whole_init_closure` below, but
   /// for `on_existing` - which running under the write lock is the whole reason
   /// `get_or_init_with` exists.
   #[test]
   fn get_or_init_with_holds_the_lock_for_the_whole_on_existing_closure() {
      use std::sync::{Barrier, mpsc};
      use std::thread;
      use std::time::Duration;

      let slot = Arc::new(ObserverSlot::default());
      slot.get_or_init(|| Arc::new(1_u32));

      let entered_barrier = Arc::new(Barrier::new(2));
      let release_barrier = Arc::new(Barrier::new(2));

      let a_slot = Arc::clone(&slot);
      let a_entered = Arc::clone(&entered_barrier);
      let a_release = Arc::clone(&release_barrier);
      let a_handle = thread::spawn(move || {
         a_slot
            .get_or_init_with::<u32, _, _>(
               || panic!("slot is already seeded; init must not run"),
               |_existing| {
                  // Signals the main thread that we're now inside
                  // `on_existing` - i.e. the write lock is held - then blocks
                  // until told to finish.
                  a_entered.wait();
                  a_release.wait();
               },
            )
            .expect("get_or_init_with should return Some for the seeded slot")
      });

      // Blocks until thread A is confirmed to be inside its on_existing
      // closure, holding the write lock.
      entered_barrier.wait();

      let (b_done_tx, b_done_rx) = mpsc::channel();
      let b_slot = Arc::clone(&slot);
      let b_handle = thread::spawn(move || {
         // clear() also takes the write lock, so it should block on A too.
         b_slot.clear();
         let _ = b_done_tx.send(());
      });

      // Absence of a message within this timeout is the proof that B is still
      // blocked, exactly as in the init-closure test below.
      let still_blocked = b_done_rx.recv_timeout(Duration::from_millis(50));
      assert!(
         still_blocked.is_err(),
         "thread B's clear() must still be blocked on the write lock while \
          thread A's on_existing closure is running"
      );

      // Lets thread A's on_existing closure finish, releasing the write lock.
      release_barrier.wait();

      a_handle.join().expect("thread A should not panic");
      b_done_rx
         .recv_timeout(Duration::from_secs(1))
         .expect("thread B should complete once the write lock is released");
      b_handle.join().expect("thread B should not panic");

      assert!(
         !slot.is_set(),
         "clear() must have run (and emptied the slot) only after \
          on_existing finished"
      );
   }

   /// Deterministic version of the race
   /// `concurrent_enable_observation_converges_on_one_broker` (in
   /// `sqlx-sqlite-toolkit`'s `observation_tests.rs`) demonstrates
   /// probabilistically under a multi-thread tokio runtime. Proves the write
   /// lock is held for the *entire* `init` closure, not just the
   /// check-and-store around it, by making a second thread's `get_or_init()`
   /// call observably block until the first thread's closure returns.
   #[test]
   fn get_or_init_holds_the_lock_for_the_whole_init_closure() {
      use std::sync::{Barrier, mpsc};
      use std::thread;
      use std::time::Duration;

      let slot = Arc::new(ObserverSlot::default());
      let entered_barrier = Arc::new(Barrier::new(2));
      let release_barrier = Arc::new(Barrier::new(2));

      let a_slot = Arc::clone(&slot);
      let a_entered = Arc::clone(&entered_barrier);
      let a_release = Arc::clone(&release_barrier);
      let a_handle = thread::spawn(move || {
         a_slot
            .get_or_init(|| {
               // Signals the main thread that we're now inside `init` - i.e.
               // the write lock is held - then blocks until told to finish.
               a_entered.wait();
               a_release.wait();
               Arc::new(1_u32)
            })
            .expect("get_or_init should return Some for a freshly-typed slot")
      });

      // Blocks until thread A is confirmed to be inside its init closure,
      // holding the write lock.
      entered_barrier.wait();

      let (b_done_tx, b_done_rx) = mpsc::channel();
      let b_slot = Arc::clone(&slot);
      let b_handle = thread::spawn(move || {
         let result = b_slot.get_or_init(|| Arc::new(999_u32));
         let _ = b_done_tx.send(result);
      });

      // Thread B's get_or_init() call must not be able to complete - or even
      // decide whether to create or reuse - while A's init closure is still
      // running under the write lock. A non-atomic implementation (drop the
      // lock before calling init, or check-then-set without holding it
      // throughout) would let B race ahead here instead of blocking, so the
      // absence of a message within this timeout is the proof, not merely a
      // wait: a several-orders-of-magnitude-longer stall than an uncontended
      // get_or_init needs is only possible if B is genuinely blocked on the
      // lock A is still holding.
      let still_blocked = b_done_rx.recv_timeout(Duration::from_millis(50));
      assert!(
         still_blocked.is_err(),
         "thread B's get_or_init() must still be blocked on the write lock \
          while thread A's init closure is running"
      );

      // Lets thread A's init closure finish, releasing the write lock.
      release_barrier.wait();

      let (a_value, a_created) = a_handle.join().expect("thread A should not panic");
      assert!(a_created, "thread A should have created the value");

      let (b_value, b_created) = b_done_rx
         .recv_timeout(Duration::from_secs(1))
         .expect("thread B should complete once the write lock is released")
         .expect("get_or_init should return Some for a freshly-typed slot");
      b_handle.join().expect("thread B should not panic");

      assert!(
         !b_created,
         "thread B must reuse the value thread A created, not build its own"
      );
      assert!(
         Arc::ptr_eq(&a_value, &b_value),
         "both threads must converge on the exact same Arc"
      );
   }
}
