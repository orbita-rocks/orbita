//! An object store that lives in a `BTreeMap`.
//!
//! The format's correctness rests on compare-and-swap, and a test that cannot
//! make two writers race cannot show that the rules hold. This store makes the
//! race cheap: the whole commit protocol runs against it in microseconds, and
//! the entity tags behave the way a real store's do.
//!
//! It is behind a feature flag because nothing in production should link it.

use async_trait::async_trait;
use bytes::Bytes;
use orbita_objectstore::{ETag, ObjectError, ObjectMeta, ObjectResult, ObjectStore, Precondition};
use orbita_runtime::Clock;
use std::collections::BTreeMap;
use std::fmt;
use std::ops::Range;
use std::sync::{Arc, Mutex};

/// A source of write times for the store to stamp on objects.
///
/// It is a captured closure rather than a `dyn Clock` because [`Clock`] is not
/// object safe (its `sleep` returns an `impl Future`), and rather than a
/// generic parameter because that would ripple `MemoryStore<C>` through every
/// test and helper that names the type. `None` here is the whole point: a
/// store built without a clock cannot report a write time, which is exactly
/// the case the sweep must refuse to act on.
type ClockFn = Arc<dyn Fn() -> u64 + Send + Sync>;

#[derive(Default)]
pub struct MemoryStore {
    state: Mutex<State>,
    /// The runtime clock this store stamps writes from, if any. Held outside
    /// the locked state because it never changes after construction.
    clock: Option<ClockFn>,
}

impl fmt::Debug for MemoryStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryStore")
            .field("state", &self.state)
            .field("clock", &self.clock.as_ref().map(|_| "set"))
            .finish()
    }
}

#[derive(Debug, Default)]
struct State {
    /// Each object is its bytes, its current tag, and the write time the store
    /// stamped, which is `None` unless the store was built with a clock.
    objects: BTreeMap<String, (Bytes, ETag, Option<u64>)>,
    /// Entity tags are never reused, including for an object that was deleted
    /// and written again, so a compare-and-swap cannot succeed against a
    /// version that no longer means what the holder thinks.
    next_tag: u64,
    /// Every range served, so a test can assert about what a reader did not
    /// read. The format's claims about cost are claims about requests, and a
    /// test that cannot see the requests cannot check them.
    ranges: Vec<(String, Range<u64>)>,
}

impl State {
    fn tag(&mut self) -> ETag {
        self.next_tag += 1;
        ETag(format!("etag-{}", self.next_tag))
    }
}

impl MemoryStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A store that stamps every write with `clock`'s current time.
    ///
    /// The simulator hands its own [`Clock`] here so the write times objects
    /// carry advance with virtual time, which is what lets a simulated orphan
    /// sweep reason about a grace period deterministically instead of racing
    /// the wall clock. A store built with [`new`](Self::new) has no clock and
    /// reports `None`, standing in for a backend that cannot report a time.
    #[must_use]
    pub fn with_clock<C: Clock>(clock: C) -> Self {
        Self {
            state: Mutex::default(),
            clock: Some(Arc::new(move || clock.now_millis())),
        }
    }

    /// The write time the configured clock would stamp right now, or `None`
    /// when no clock was configured.
    fn stamp(&self) -> Option<u64> {
        self.clock.as_ref().map(|clock| clock())
    }

    /// Every key currently held, for a test that wants to assert about orphans.
    #[must_use]
    pub fn keys(&self) -> Vec<String> {
        self.lock().objects.keys().cloned().collect()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().objects.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lock().objects.is_empty()
    }

    /// Every range this store has served, in order.
    #[must_use]
    pub fn ranges_served(&self) -> Vec<(String, Range<u64>)> {
        self.lock().ranges.clone()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[async_trait]
impl ObjectStore for MemoryStore {
    async fn put(&self, key: &str, data: Bytes) -> ObjectResult<ETag> {
        let stamp = self.stamp();
        let mut state = self.lock();
        let tag = state.tag();
        state
            .objects
            .insert(key.to_string(), (data, tag.clone(), stamp));
        Ok(tag)
    }

    async fn put_if(
        &self,
        key: &str,
        data: Bytes,
        precondition: Precondition,
    ) -> ObjectResult<ETag> {
        let stamp = self.stamp();
        let mut state = self.lock();
        let current = state.objects.get(key).map(|(_, tag, _)| tag.clone());
        let holds = match (&precondition, &current) {
            (Precondition::NotExists, None) => true,
            (Precondition::Match(expected), Some(actual)) => expected == actual,
            _ => false,
        };
        if !holds {
            return Err(ObjectError::PreconditionFailed(key.to_string()));
        }
        let tag = state.tag();
        state
            .objects
            .insert(key.to_string(), (data, tag.clone(), stamp));
        Ok(tag)
    }

    async fn get(&self, key: &str) -> ObjectResult<(Bytes, ETag)> {
        self.lock()
            .objects
            .get(key)
            .map(|(data, tag, _)| (data.clone(), tag.clone()))
            .ok_or_else(|| ObjectError::NotFound(key.to_string()))
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> ObjectResult<Bytes> {
        let mut state = self.lock();
        state.ranges.push((key.to_string(), range.clone()));
        let (data, _, _) = state
            .objects
            .get(key)
            .ok_or_else(|| ObjectError::NotFound(key.to_string()))?;
        let size = data.len() as u64;
        if range.start >= size || range.end > size || range.start > range.end {
            return Err(ObjectError::Other(format!(
                "range {}..{} is not satisfiable for a {size} byte object",
                range.start, range.end
            )));
        }
        Ok(data.slice(range.start as usize..range.end as usize))
    }

    async fn head(&self, key: &str) -> ObjectResult<ObjectMeta> {
        let state = self.lock();
        let (data, tag, last_modified) = state
            .objects
            .get(key)
            .ok_or_else(|| ObjectError::NotFound(key.to_string()))?;
        Ok(ObjectMeta {
            key: key.to_string(),
            size: data.len() as u64,
            etag: tag.clone(),
            last_modified: *last_modified,
        })
    }

    async fn list(&self, prefix: &str) -> ObjectResult<Vec<ObjectMeta>> {
        let state = self.lock();
        Ok(state
            .objects
            .range(prefix.to_string()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, (data, tag, last_modified))| ObjectMeta {
                key: key.clone(),
                size: data.len() as u64,
                etag: tag.clone(),
                last_modified: *last_modified,
            })
            .collect())
    }

    async fn delete(&self, key: &str) -> ObjectResult<()> {
        self.lock().objects.remove(key);
        Ok(())
    }
}

/// A hand-driven clock, so a test can advance write time without pulling in the
/// simulator. It reports the same `now_millis` every call until moved forward,
/// which is all the store's stamping needs.
#[cfg(test)]
#[derive(Clone)]
struct ManualClock {
    millis: Arc<std::sync::atomic::AtomicU64>,
}

#[cfg(test)]
impl ManualClock {
    fn at(millis: u64) -> Self {
        Self {
            millis: Arc::new(std::sync::atomic::AtomicU64::new(millis)),
        }
    }

    fn set(&self, millis: u64) {
        self.millis
            .store(millis, std::sync::atomic::Ordering::SeqCst);
    }
}

#[cfg(test)]
impl Clock for ManualClock {
    fn now_millis(&self) -> u64 {
        self.millis.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn monotonic_nanos(&self) -> u64 {
        self.now_millis().saturating_mul(1_000_000)
    }

    async fn sleep(&self, _duration: std::time::Duration) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_conditional_write_needs_the_current_tag() {
        let store = MemoryStore::new();
        let first = store
            .put_if("k", Bytes::from_static(b"a"), Precondition::NotExists)
            .await
            .expect("nothing there yet");
        assert!(
            store
                .put_if("k", Bytes::from_static(b"b"), Precondition::NotExists)
                .await
                .is_err(),
            "the object exists now"
        );

        let second = store
            .put_if(
                "k",
                Bytes::from_static(b"b"),
                Precondition::Match(first.clone()),
            )
            .await
            .expect("the tag is current");
        assert_ne!(second, first, "a write always moves the tag");
        assert!(
            store
                .put_if("k", Bytes::from_static(b"c"), Precondition::Match(first))
                .await
                .is_err(),
            "a stale tag loses"
        );
    }

    #[tokio::test]
    async fn a_range_request_returns_exactly_the_range() {
        let store = MemoryStore::new();
        store
            .put("k", Bytes::from_static(b"0123456789"))
            .await
            .unwrap();
        assert_eq!(
            store.get_range("k", 2..5).await.unwrap(),
            Bytes::from_static(b"234")
        );
        assert!(store.get_range("k", 8..20).await.is_err(), "past the end");
    }

    #[tokio::test]
    async fn a_listing_is_confined_to_its_prefix_and_sorted() {
        let store = MemoryStore::new();
        for key in ["p/b", "p/a", "q/a"] {
            store.put(key, Bytes::new()).await.unwrap();
        }
        let listed: Vec<String> = store
            .list("p/")
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.key)
            .collect();
        assert_eq!(listed, vec!["p/a".to_string(), "p/b".to_string()]);
    }

    #[tokio::test]
    async fn a_clocked_store_stamps_the_write_time_the_listing_reports() {
        let clock = ManualClock::at(1_000);
        let store = MemoryStore::with_clock(clock.clone());
        store.put("p/a", Bytes::from_static(b"x")).await.unwrap();

        // A later write is stamped with the clock's later reading, so the times
        // objects carry track virtual time rather than an insertion counter.
        clock.set(5_000);
        store.put("p/b", Bytes::from_static(b"y")).await.unwrap();

        let listed = store.list("p/").await.unwrap();
        let times: Vec<Option<u64>> = listed.iter().map(|m| m.last_modified).collect();
        assert_eq!(times, vec![Some(1_000), Some(5_000)]);
        // The same stamp is reachable through head, which is where a caller
        // that already knows the key looks.
        assert_eq!(store.head("p/a").await.unwrap().last_modified, Some(1_000));
    }

    #[tokio::test]
    async fn a_store_without_a_clock_cannot_report_a_write_time() {
        // This stands in for a backend that cannot report a time: the sweep is
        // required to refuse to act on such an object rather than read the
        // absence as "very old".
        let store = MemoryStore::new();
        store.put("p/a", Bytes::from_static(b"x")).await.unwrap();
        assert_eq!(store.head("p/a").await.unwrap().last_modified, None);
        assert_eq!(store.list("p/").await.unwrap()[0].last_modified, None);
    }
}
