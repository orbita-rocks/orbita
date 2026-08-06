//! Per-keyspace admission: storage caps and request rate limits.
//!
//! A keyspace configures three quotas — `max_storage_bytes`,
//! `max_reads_per_second`, `max_writes_per_second` — and until now nobody read
//! them. This module is where a worker turns them into refusals.
//!
//! # Why the state is per keyspace and never shared
//!
//! Quota pressure has to stay inside the keyspace that caused it. A neighbour
//! measuring its own latency while a noisy tenant is throttled must not see the
//! throttle at all, so the mechanism cannot take a lock or a queue that spans
//! keyspaces. Every keyspace therefore gets its own [`KeyspaceLimits`] behind
//! an `Arc`, resolved through a read-mostly registry. The registry lock is held
//! only long enough to hand back that `Arc`; all the rate arithmetic runs under
//! a per-keyspace lock, so one tenant being throttled never serializes another
//! tenant's traffic.
//!
//! # Why a hand-rolled token bucket
//!
//! A token bucket is a handful of integers and one lock that is never held
//! across an await, which is exactly the isolation property above. Pulling in a
//! `tower` limit layer or the `governor` crate would buy a shared middleware
//! stack and, in `governor`'s case, its own global clock — both of which cut
//! against keeping each keyspace's throttle independent and drivable from the
//! runtime's clock under deterministic simulation.

use orbita_core::KeyspaceId;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

const NANOS_PER_SEC: u128 = 1_000_000_000;

/// Milli-tokens per whole token. Tokens are tracked scaled by a thousand so
/// that a refill smaller than one token — the common case between two requests
/// a few milliseconds apart — still accumulates rather than rounding to zero.
const MILLI: u64 = 1_000;

/// Which counter a request is charged against.
///
/// Reads and writes are counted separately because they are configured
/// separately: a keyspace can allow many cheap reads and few expensive writes,
/// and one bucket could not express that.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Direction {
    Read,
    Write,
}

/// A refill-on-read token bucket.
///
/// Time is passed in rather than read from a clock so the bucket is pure and
/// testable, and so the worker can feed it the runtime's monotonic clock, which
/// is what makes throttling reproducible under simulation.
struct Bucket {
    /// Tokens added per second. This is the configured rate.
    rate_per_sec: u32,
    /// The ceiling, in milli-tokens. A full second of rate, so a burst up to
    /// the configured rate is allowed after an idle period and the steady
    /// state settles to the rate itself.
    capacity_milli: u64,
    /// Available milli-tokens.
    tokens_milli: u64,
    /// The monotonic nanosecond reading the level was last brought current to.
    last_nanos: u64,
}

impl Bucket {
    fn new(rate_per_sec: u32, now_nanos: u64) -> Self {
        let capacity_milli = u64::from(rate_per_sec).max(1) * MILLI;
        Self {
            rate_per_sec,
            capacity_milli,
            // Starts full, so a keyspace that has just been configured is not
            // punished for traffic that arrived before the bucket existed.
            tokens_milli: capacity_milli,
            last_nanos: now_nanos,
        }
    }

    /// Refills for the elapsed time and takes one token, reporting whether one
    /// was there to take.
    fn try_take(&mut self, now_nanos: u64) -> bool {
        let elapsed = u128::from(now_nanos.saturating_sub(self.last_nanos));
        let refill = (elapsed * u128::from(self.rate_per_sec) * u128::from(MILLI) / NANOS_PER_SEC)
            .min(u128::from(u64::MAX)) as u64;
        // The clock is advanced only once a whole milli-token has accrued.
        // Advancing it on every call would discard the sub-milli remainder of
        // each short interval, and a keyspace polled faster than its rate would
        // then never refill at all. Leaving it put lets the elapsed time keep
        // growing until it is worth a token.
        if refill > 0 {
            self.tokens_milli = self
                .tokens_milli
                .saturating_add(refill)
                .min(self.capacity_milli);
            self.last_nanos = now_nanos;
        }
        if self.tokens_milli >= MILLI {
            self.tokens_milli -= MILLI;
            true
        } else {
            false
        }
    }
}

/// One direction's limiter for one keyspace.
///
/// The configured rate is remembered so a quota change from the admin surface
/// rebuilds the bucket rather than silently keeping the old rate.
struct DirectionSlot {
    configured: Option<u32>,
    bucket: Option<Bucket>,
}

impl DirectionSlot {
    const fn empty() -> Self {
        Self {
            configured: None,
            bucket: None,
        }
    }

    /// Admits a request under `rate`, rebuilding the bucket if the rate moved.
    /// An unset rate is unlimited and always admits.
    fn admit(&mut self, rate: Option<u32>, now_nanos: u64) -> bool {
        match rate {
            None => {
                self.configured = None;
                self.bucket = None;
                true
            }
            Some(rate) => {
                if self.configured != Some(rate) || self.bucket.is_none() {
                    self.configured = Some(rate);
                    self.bucket = Some(Bucket::new(rate, now_nanos));
                }
                self.bucket
                    .as_mut()
                    .expect("bucket set above")
                    .try_take(now_nanos)
            }
        }
    }
}

/// The worker's most recent view of how much storage a keyspace holds.
///
/// It is the sum over the partitions this worker owns for the keyspace, sampled
/// at most one interval ago. See [`KeyspaceLimits::storage_sample`].
struct StorageSample {
    bytes: u64,
    at_nanos: u64,
    taken: bool,
}

/// Everything one keyspace is throttled by, isolated from every other
/// keyspace's state.
struct KeyspaceLimits {
    read: Mutex<DirectionSlot>,
    write: Mutex<DirectionSlot>,
    storage: Mutex<StorageSample>,
}

impl KeyspaceLimits {
    fn new() -> Self {
        Self {
            read: Mutex::new(DirectionSlot::empty()),
            write: Mutex::new(DirectionSlot::empty()),
            storage: Mutex::new(StorageSample {
                bytes: 0,
                at_nanos: 0,
                taken: false,
            }),
        }
    }

    fn admit(&self, direction: Direction, rate: Option<u32>, now_nanos: u64) -> bool {
        let slot = match direction {
            Direction::Read => &self.read,
            Direction::Write => &self.write,
        };
        slot.lock()
            .expect("keyspace rate slot poisoned")
            .admit(rate, now_nanos)
    }

    /// The cached storage figure if it is fresher than `freshness_nanos`, or
    /// `None` when the caller has to remeasure.
    fn storage_sample(&self, now_nanos: u64, freshness_nanos: u64) -> Option<u64> {
        let sample = self
            .storage
            .lock()
            .expect("keyspace storage sample poisoned");
        (sample.taken && now_nanos.saturating_sub(sample.at_nanos) < freshness_nanos)
            .then_some(sample.bytes)
    }

    fn record_storage(&self, bytes: u64, now_nanos: u64) {
        let mut sample = self
            .storage
            .lock()
            .expect("keyspace storage sample poisoned");
        sample.bytes = bytes;
        sample.at_nanos = now_nanos;
        sample.taken = true;
    }
}

/// The per-keyspace admission registry a worker holds.
///
/// Lookups take the read lock and clone one `Arc`; a keyspace seen for the
/// first time takes the write lock once to insert it. No throttling work
/// happens under this lock, which is what keeps one keyspace's limiter off
/// another keyspace's path.
pub(crate) struct Admission {
    keyspaces: RwLock<HashMap<KeyspaceId, Arc<KeyspaceLimits>>>,
}

impl Admission {
    pub(crate) fn new() -> Self {
        Self {
            keyspaces: RwLock::new(HashMap::new()),
        }
    }

    fn limits(&self, keyspace: KeyspaceId) -> Arc<KeyspaceLimits> {
        if let Some(limits) = self
            .keyspaces
            .read()
            .expect("admission registry poisoned")
            .get(&keyspace)
        {
            return Arc::clone(limits);
        }
        Arc::clone(
            self.keyspaces
                .write()
                .expect("admission registry poisoned")
                .entry(keyspace)
                .or_insert_with(|| Arc::new(KeyspaceLimits::new())),
        )
    }

    /// Charges one request against `keyspace`'s `direction` limiter, reporting
    /// whether it may proceed. An unset rate always proceeds.
    pub(crate) fn admit_rate(
        &self,
        keyspace: KeyspaceId,
        direction: Direction,
        rate: Option<u32>,
        now_nanos: u64,
    ) -> bool {
        self.limits(keyspace).admit(direction, rate, now_nanos)
    }

    /// The cached storage figure for `keyspace`, or `None` when it is stale
    /// enough that the caller should remeasure and [`Self::record_storage`].
    pub(crate) fn storage_sample(
        &self,
        keyspace: KeyspaceId,
        now_nanos: u64,
        freshness_nanos: u64,
    ) -> Option<u64> {
        self.limits(keyspace)
            .storage_sample(now_nanos, freshness_nanos)
    }

    pub(crate) fn record_storage(&self, keyspace: KeyspaceId, bytes: u64, now_nanos: u64) {
        self.limits(keyspace).record_storage(bytes, now_nanos);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND: u64 = 1_000_000_000;

    fn ks(id: u64) -> KeyspaceId {
        KeyspaceId(id)
    }

    #[test]
    fn a_bucket_admits_up_to_its_rate_then_refuses_until_it_refills() {
        let admission = Admission::new();
        // Rate of two per second means a full bucket of two, then nothing more
        // until time advances.
        assert!(admission.admit_rate(ks(1), Direction::Write, Some(2), 0));
        assert!(admission.admit_rate(ks(1), Direction::Write, Some(2), 0));
        assert!(
            !admission.admit_rate(ks(1), Direction::Write, Some(2), 0),
            "a third write in the same instant is over the per-second rate"
        );
        // Half a second later one token has refilled.
        assert!(admission.admit_rate(ks(1), Direction::Write, Some(2), SECOND / 2));
        assert!(!admission.admit_rate(ks(1), Direction::Write, Some(2), SECOND / 2));
    }

    #[test]
    fn an_unset_rate_admits_without_limit() {
        let admission = Admission::new();
        for _ in 0..1_000 {
            assert!(admission.admit_rate(ks(1), Direction::Read, None, 0));
        }
    }

    #[test]
    fn reads_and_writes_are_throttled_independently() {
        let admission = Admission::new();
        // Exhaust the write budget entirely.
        assert!(admission.admit_rate(ks(1), Direction::Write, Some(1), 0));
        assert!(!admission.admit_rate(ks(1), Direction::Write, Some(1), 0));
        // The read budget is untouched: a separate bucket, separately
        // configured.
        assert!(
            admission.admit_rate(ks(1), Direction::Read, Some(1), 0),
            "exhausting writes must not spend the read budget"
        );
    }

    #[test]
    fn throttling_one_keyspace_leaves_a_neighbour_at_full_budget() {
        let admission = Admission::new();
        let noisy = ks(1);
        let quiet = ks(2);
        // Saturate the noisy tenant.
        assert!(admission.admit_rate(noisy, Direction::Write, Some(1), 0));
        assert!(!admission.admit_rate(noisy, Direction::Write, Some(1), 0));
        assert!(!admission.admit_rate(noisy, Direction::Write, Some(1), 0));
        // The quiet neighbour, under the same cap, is completely unaffected:
        // its bucket is a different allocation reached without touching the
        // noisy tenant's lock.
        assert!(
            admission.admit_rate(quiet, Direction::Write, Some(1), 0),
            "a neighbour under its cap must not feel a throttled tenant"
        );
    }

    #[test]
    fn a_stale_storage_sample_is_reported_as_absent() {
        let admission = Admission::new();
        admission.record_storage(ks(1), 4_096, 0);
        assert_eq!(admission.storage_sample(ks(1), 500, SECOND), Some(4_096));
        assert_eq!(
            admission.storage_sample(ks(1), 2 * SECOND, SECOND),
            None,
            "a sample older than the freshness window forces a remeasure"
        );
    }

    #[test]
    fn raising_a_rate_rebuilds_the_bucket_so_the_new_ceiling_takes_effect() {
        let admission = Admission::new();
        assert!(admission.admit_rate(ks(1), Direction::Write, Some(1), 0));
        assert!(!admission.admit_rate(ks(1), Direction::Write, Some(1), 0));
        // Reconfigured to a higher rate: the fresh bucket starts full again.
        assert!(admission.admit_rate(ks(1), Direction::Write, Some(5), 0));
    }
}
