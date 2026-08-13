//! A cache for values read back out of segments.
//!
//! [ADR 0006](../../../docs/adr/0006-partitions-are-an-index-over-immutable-objects.md)
//! decided that values are cached rather than resident: "the hot set stays in
//! memory; a read that misses fetches the record from its segment. This is what
//! lets a partition hold more than a node's memory." This is the caching half,
//! which went unbuilt long enough that every read of a flushed key was an
//! object-store round trip and no read on a benchmarked cluster ever returned
//! faster than 14.4ms.
//!
//! # Why this needs no invalidation
//!
//! The cache is keyed on the object a record lives in and its offset within
//! that object, and segments are immutable. Those bytes cannot change, so a
//! cached entry cannot go stale and nothing has to be told when a write
//! happens.
//!
//! What makes that safe is the order of the read path rather than anything
//! here: [`crate::partition::Partition`] consults its memtable first and only
//! reaches a segment when the newest version of a key is already flushed. A
//! write puts the key back in the memtable, which shadows whatever this holds
//! until the flush that publishes a new segment at a new offset.
//!
//! Keying on the *resolved object path* is load-bearing and the reason this
//! does not key on `Loc`, which is what the index actually stores. A `Loc`
//! names a position in the partition's current segment list, and that list is
//! replaced wholesale by compaction. Cache on it and a compaction quietly
//! starts serving whatever moved into that slot.
//!
//! # What it holds
//!
//! Entries are `Stored`, the decoded record, not the visible `Record`. A
//! tombstone and an expired record are both entries that exist and are not
//! visible, and the visibility rule is applied by the caller against the clock
//! at read time. Caching the visible form instead would freeze a TTL decision
//! taken when the entry was first read and serve expired keys forever.
//!
//! # Eviction
//!
//! Least recently used, against a byte budget shared by every partition on the
//! node. ADR 0006 says "a memory budget per partition"; a node holding
//! thousands of partitions would then have thousands of budgets and no bound
//! at all, so the budget is per cache and the cache is per node.
//!
//! Compaction needs no help from the caller. The segments it retires are
//! never read again, so their records are exactly the ones recency is about to
//! evict, and correctness never depended on removing them: the bytes at an
//! offset in a segment that still exists are the same bytes whether or not the
//! partition still lists it. An explicit sweep was written and taken out
//! again, because this cache is shared and a partition cannot name the dead
//! objects without also naming another partition's live ones — a split child
//! reads segments under its parent's prefix (ADR 0009).
//!
//! Recency is a counter rather than a clock. The engine takes a `Runtime` but
//! reads no time of its own here, so the simulator sees the same eviction
//! order on every run of a seed. That ADR called eviction "the most likely
//! place for the design to go wrong, and it is where the simulator should be
//! pointed once the basics work", which is easier to honour if the policy does
//! not depend on wall time.

use crate::partition::Stored;

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

/// What one entry is estimated to cost beyond the value it holds.
///
/// Two map entries, an `Arc<str>` clone, the record's own fields, and the
/// allocator's share of each. Deliberately an estimate: the budget exists to
/// stop a node running out of memory, and a number that is roughly right and
/// costs nothing to maintain serves that better than a precise one that
/// requires walking the structure.
const ENTRY_OVERHEAD_BYTES: u64 = 96;

/// What a cache reports about itself, for metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CacheStats {
    /// Reads served without going to the object store.
    pub hits: u64,
    /// Reads that had to fetch.
    pub misses: u64,
    /// Estimated bytes held.
    pub bytes: u64,
    /// Records held.
    pub entries: u64,
    /// Entries dropped to stay inside the budget.
    pub evictions: u64,
}

impl CacheStats {
    /// Hit rate over the cache's lifetime, or `None` before the first read.
    ///
    /// The number an operator actually looks at. A cache that is too small for
    /// the working set still reports plenty of hits in absolute terms, and
    /// only the ratio says so.
    #[must_use]
    pub fn hit_rate(&self) -> Option<f64> {
        let total = self.hits + self.misses;
        (total > 0).then(|| self.hits as f64 / total as f64)
    }
}

/// One record's identity: the object it lives in, and where in that object.
type Offset = u64;

struct Entry {
    stored: Stored,
    /// The access this entry was last touched at, so the ordering index can be
    /// corrected without scanning it.
    tick: u64,
    cost: u64,
}

struct Inner {
    /// Object key, then offset within it. Nested rather than keyed on a pair
    /// so a lookup can borrow the object key instead of allocating one, and so
    /// that dropping a whole segment is one removal rather than a scan.
    entries: HashMap<Arc<str>, BTreeMap<Offset, Entry>>,
    /// Access order. The lowest tick is the next eviction.
    order: BTreeMap<u64, (Arc<str>, Offset)>,
    tick: u64,
    bytes: u64,
    capacity_bytes: u64,
    hits: u64,
    misses: u64,
    evictions: u64,
}

/// A bounded, shared cache of records read out of segments.
///
/// Cloneable and shared by every partition on a node; the budget is the
/// cache's, not the partition's.
pub struct ValueCache {
    inner: Mutex<Inner>,
}

impl ValueCache {
    /// A cache holding roughly `capacity_bytes` of records.
    ///
    /// A capacity of zero is a working cache that immediately evicts
    /// everything, which is how a caller turns caching off without the read
    /// path growing a branch for it.
    #[must_use]
    pub fn new(capacity_bytes: u64) -> Self {
        Self {
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                order: BTreeMap::new(),
                tick: 0,
                bytes: 0,
                capacity_bytes,
                hits: 0,
                misses: 0,
                evictions: 0,
            }),
        }
    }

    /// The record at `offset` in `object`, if it is held.
    ///
    /// Counts the hit or the miss, so a caller must not consult this
    /// speculatively: the hit rate is the signal that says whether the budget
    /// is large enough, and a probe that was never going to fetch would
    /// understate it.
    pub(crate) fn get(&self, object: &str, offset: Offset) -> Option<Stored> {
        let mut inner = self.inner.lock().expect("value cache poisoned");
        inner.tick += 1;
        let tick = inner.tick;

        let Some((previous, stored)) = inner.entries.get_mut(object).and_then(|by_offset| {
            by_offset.get_mut(&offset).map(|entry| {
                let previous = entry.tick;
                entry.tick = tick;
                (previous, entry.stored.clone())
            })
        }) else {
            inner.misses += 1;
            return None;
        };

        // Move it to the most recent end. The removal is what stops the
        // ordering index growing a stale entry per access.
        if let Some(key) = inner.order.remove(&previous) {
            inner.order.insert(tick, key);
        }
        inner.hits += 1;
        Some(stored)
    }

    /// Holds `stored`, evicting least recently used entries to stay in budget.
    ///
    /// Replacing an existing entry is allowed and is not a correctness
    /// question: the bytes at an offset in an immutable object are the same
    /// bytes whoever read them.
    pub(crate) fn insert(&self, object: &str, offset: Offset, stored: Stored) {
        let cost = ENTRY_OVERHEAD_BYTES + stored.value.len() as u64;
        let mut inner = self.inner.lock().expect("value cache poisoned");

        // An entry larger than the whole budget is not worth evicting the
        // cache to hold, and holding it would leave the cache permanently over
        // budget with one record in it.
        if cost > inner.capacity_bytes {
            return;
        }

        inner.tick += 1;
        let tick = inner.tick;
        let key: Arc<str> = match inner.entries.get_key_value(object) {
            Some((existing, _)) => Arc::clone(existing),
            None => Arc::from(object),
        };

        let by_offset = inner.entries.entry(Arc::clone(&key)).or_default();
        if let Some(old) = by_offset.insert(offset, Entry { stored, tick, cost }) {
            inner.bytes -= old.cost;
            inner.order.remove(&old.tick);
        }
        inner.bytes += cost;
        inner.order.insert(tick, (key, offset));

        inner.evict_to_budget();
    }

    /// What the cache is holding and how well it is serving.
    #[must_use]
    pub fn stats(&self) -> CacheStats {
        let inner = self.inner.lock().expect("value cache poisoned");
        CacheStats {
            hits: inner.hits,
            misses: inner.misses,
            bytes: inner.bytes,
            entries: inner.order.len() as u64,
            evictions: inner.evictions,
        }
    }
}

impl std::fmt::Debug for ValueCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValueCache")
            .field("stats", &self.stats())
            .finish()
    }
}

impl Inner {
    /// Drops least recently used entries until the budget is met.
    fn evict_to_budget(&mut self) {
        while self.bytes > self.capacity_bytes {
            let Some((tick, (object, offset))) = self.order.pop_first() else {
                // Nothing left to evict. Reaching this with bytes outstanding
                // would mean the accounting had drifted from the contents, so
                // trust the contents.
                debug_assert_eq!(self.bytes, 0, "byte accounting outlived the entries");
                self.bytes = 0;
                return;
            };
            let Some(by_offset) = self.entries.get_mut(&object) else {
                continue;
            };
            // Only drop the entry the ordering index actually named. A newer
            // access reinserts under a higher tick, and evicting on a stale
            // pointer would throw away the fresher record.
            if by_offset
                .get(&offset)
                .is_some_and(|entry| entry.tick == tick)
            {
                if let Some(entry) = by_offset.remove(&offset) {
                    self.bytes -= entry.cost;
                    self.evictions += 1;
                }
            }
            if by_offset.is_empty() {
                self.entries.remove(&object);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::Bytes;
    use orbita_core::Version;

    fn stored(value: &'static str) -> Stored {
        Stored {
            version: Version(1),
            expires_at_millis: None,
            deleted: false,
            value: Bytes::from_static(value.as_bytes()),
        }
    }

    fn big_enough_for(entries: u64, value_len: u64) -> u64 {
        entries * (ENTRY_OVERHEAD_BYTES + value_len)
    }

    #[test]
    fn a_record_read_once_is_served_from_memory_the_next_time() {
        let cache = ValueCache::new(big_enough_for(4, 8));
        assert_eq!(cache.get("seg-a", 0), None, "nothing is held yet");

        cache.insert("seg-a", 0, stored("value"));

        assert_eq!(
            cache.get("seg-a", 0).map(|s| s.value),
            Some(Bytes::from_static(b"value"))
        );
        let stats = cache.stats();
        assert_eq!((stats.hits, stats.misses), (1, 1));
    }

    #[test]
    fn the_same_offset_in_two_segments_is_two_records() {
        // The bug this forbids is the whole reason the key is the resolved
        // object path. Offsets repeat across segments constantly — every
        // segment has a record near its start — so a key that is only an
        // offset would serve one segment's record for another's.
        let cache = ValueCache::new(big_enough_for(4, 8));
        cache.insert("seg-a", 64, stored("from a"));
        cache.insert("seg-b", 64, stored("from b"));

        assert_eq!(
            cache.get("seg-a", 64).map(|s| s.value),
            Some(Bytes::from_static(b"from a"))
        );
        assert_eq!(
            cache.get("seg-b", 64).map(|s| s.value),
            Some(Bytes::from_static(b"from b"))
        );
    }

    #[test]
    fn a_tombstone_is_cached_as_an_entry_that_exists_and_is_not_visible() {
        // Caching the visible record instead of the stored one would turn a
        // cached tombstone into a cached absence, and a conditional write
        // needs to tell "deleted" from "never existed".
        let cache = ValueCache::new(big_enough_for(2, 0));
        cache.insert(
            "seg-a",
            0,
            Stored {
                version: Version(9),
                expires_at_millis: None,
                deleted: true,
                value: Bytes::new(),
            },
        );

        let held = cache.get("seg-a", 0).expect("the tombstone is held");
        assert!(held.deleted, "it is still a tombstone");
        assert_eq!(held.visible_at(0), None, "and still invisible");
    }

    #[test]
    fn expiry_is_decided_when_the_entry_is_read_not_when_it_was_cached() {
        // A TTL frozen at insert would outlive its deadline in the cache and
        // keep answering, which is the failure that makes caching a decoded
        // record dangerous if it is the *visible* record.
        let cache = ValueCache::new(big_enough_for(2, 8));
        cache.insert(
            "seg-a",
            0,
            Stored {
                version: Version(1),
                expires_at_millis: Some(5_000),
                deleted: false,
                value: Bytes::from_static(b"soon"),
            },
        );

        let held = cache.get("seg-a", 0).expect("held");
        assert!(held.visible_at(4_999).is_some(), "before the deadline");
        assert!(
            held.visible_at(5_000).is_none(),
            "the deadline is inclusive and the cache does not change that"
        );
    }

    #[test]
    fn the_least_recently_used_record_is_the_one_that_goes() {
        let cache = ValueCache::new(big_enough_for(2, 1));
        cache.insert("seg", 0, stored("a"));
        cache.insert("seg", 1, stored("b"));

        // Touch the older one so the younger becomes the eviction candidate.
        assert!(cache.get("seg", 0).is_some());
        cache.insert("seg", 2, stored("c"));

        assert!(cache.get("seg", 0).is_some(), "recently used, so kept");
        assert!(
            cache.get("seg", 1).is_none(),
            "least recently used, so gone"
        );
        assert!(cache.get("seg", 2).is_some(), "just inserted");
        assert_eq!(cache.stats().evictions, 1);
    }

    #[test]
    fn a_hit_moves_a_record_without_growing_the_ordering_index() {
        // The ordering index is corrected on every hit rather than appended
        // to. If it were appended to, a read-heavy workload would grow it
        // without bound while the cache itself stayed inside its budget.
        let cache = ValueCache::new(big_enough_for(2, 1));
        cache.insert("seg", 0, stored("a"));
        for _ in 0..50 {
            assert!(cache.get("seg", 0).is_some());
        }
        assert_eq!(cache.stats().entries, 1, "one record, one ordering entry");
    }

    #[test]
    fn the_cache_never_holds_more_than_its_budget() {
        let cache = ValueCache::new(big_enough_for(3, 4));
        for offset in 0..200 {
            cache.insert("seg", offset, stored("data"));
            assert!(
                cache.stats().bytes <= big_enough_for(3, 4),
                "over budget at offset {offset}"
            );
        }
        assert_eq!(cache.stats().entries, 3);
    }

    #[test]
    fn a_record_too_large_for_the_budget_is_not_cached_at_all() {
        // Holding it would evict everything else and leave the cache with one
        // record it is over budget for, which is worse than not caching it.
        let cache = ValueCache::new(ENTRY_OVERHEAD_BYTES + 4);
        cache.insert("seg", 0, stored("kept"));
        cache.insert("seg", 1, stored("far too long for this budget"));

        assert!(cache.get("seg", 0).is_some(), "the small record survives");
        assert!(cache.get("seg", 1).is_none(), "the large one was refused");
    }

    #[test]
    fn a_zero_budget_caches_nothing_without_the_read_path_knowing() {
        let cache = ValueCache::new(0);
        cache.insert("seg", 0, stored("a"));
        assert_eq!(cache.get("seg", 0), None);
        assert_eq!(cache.stats().bytes, 0);
    }

    #[test]
    fn the_hit_rate_is_none_until_something_has_been_read() {
        assert_eq!(ValueCache::new(1024).stats().hit_rate(), None);

        let cache = ValueCache::new(big_enough_for(2, 1));
        cache.insert("seg", 0, stored("a"));
        assert!(cache.get("seg", 0).is_some());
        assert!(cache.get("seg", 1).is_none());
        assert_eq!(cache.stats().hit_rate(), Some(0.5));
    }
}
