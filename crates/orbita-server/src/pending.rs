//! The owner's in-flight writes.
//!
//! [ADR 0003](../../../docs/adr/0003-conditions-evaluate-against-pending-writes.md)
//! makes this the overlay a condition is evaluated against: a write exists in
//! the log before it exists in RocksDB, and a second write to the same key
//! arriving in that window has to see the first one or two compare-and-swaps
//! against the same version could both succeed.
//!
//! The same overlay serves reads, because ADR 0001 acknowledges a write to the
//! client before applying it, so between those two moments the storage engine
//! is not the whole truth about a key.
//!
//! # Two states, and why the first one is deliberately pessimistic
//!
//! A write is *in flight* from the moment it is submitted to the log until the
//! log tells us which Lamport it got, and *resolved* from then until the
//! storage engine has applied it. Only a resolved write has a version, because
//! the Lamport is what the version is, and the log hands that back at the end
//! of the round trip rather than the start.
//!
//! While a key has an in-flight write, every conditional write against it
//! fails. That is safe rather than merely convenient: a client can only hold a
//! version it was told about, it is only told about a version once the write
//! was acknowledged, and an acknowledged write is resolved. So a
//! compare-and-swap that arrives while the key is uncertain is necessarily
//! comparing against a version the in-flight write is about to supersede, and
//! failing it is the answer the client would have got a moment later anyway.

use bytes::Bytes;
use orbita_core::{Lamport, Record, Version};

use std::collections::{BTreeMap, HashMap};

/// What a pending write leaves behind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PendingRecord {
    Put(Record),
    /// A tombstone. The key reads as absent, which is what makes
    /// `IfNotPresent` usable for a lock that was just released.
    Delete,
}

impl PendingRecord {
    fn version(&self, lamport: Lamport) -> Version {
        match self {
            PendingRecord::Put(record) => record.version,
            PendingRecord::Delete => Version(lamport.get()),
        }
    }
}

/// Identifies one submitted write so its resolution lands on the right key.
#[derive(Debug, Clone)]
pub(crate) struct Ticket {
    key: Bytes,
}

#[derive(Debug, Default)]
struct KeyPending {
    inflight: usize,
    /// Keyed by Lamport rather than kept in submission order, because the log
    /// decides the order that matters and two writes submitted a moment apart
    /// can resolve in either order.
    resolved: BTreeMap<Lamport, PendingRecord>,
}

/// One partition's in-flight and unapplied writes.
#[derive(Debug, Default)]
pub(crate) struct PendingSet {
    keys: HashMap<Bytes, KeyPending>,
    resolved: usize,
}

/// What the overlay knows about one key.
#[derive(Debug, Clone)]
pub(crate) struct Overlay {
    /// The newest resolved write for the key, if any.
    pub top: Option<(Lamport, PendingRecord)>,
    /// True while a write to this key has been submitted and has not come
    /// back, which makes the key's next version unknowable.
    pub uncertain: bool,
}

impl PendingSet {
    /// Records that a write has been submitted to the log.
    pub(crate) fn reserve(&mut self, key: &Bytes) -> Ticket {
        self.keys.entry(key.clone()).or_default().inflight += 1;
        Ticket { key: key.clone() }
    }

    /// Records the Lamport and the record the log gave the write.
    pub(crate) fn resolve(&mut self, ticket: &Ticket, lamport: Lamport, record: PendingRecord) {
        if let Some(entry) = self.keys.get_mut(&ticket.key) {
            entry.inflight = entry.inflight.saturating_sub(1);
            entry.resolved.insert(lamport, record);
            self.resolved += 1;
        }
    }

    /// Drops a write that never made it to the log. Nothing is left behind,
    /// because a write that failed was never acknowledged and must not be
    /// visible to anyone.
    pub(crate) fn abandon(&mut self, ticket: &Ticket) {
        if let Some(entry) = self.keys.get_mut(&ticket.key) {
            entry.inflight = entry.inflight.saturating_sub(1);
            self.prune(&ticket.key);
        }
    }

    /// Drops a write the storage engine has now applied.
    ///
    /// Called after the apply, never before, so that a reader either sees the
    /// overlay or sees the applied record and never falls between the two.
    pub(crate) fn applied(&mut self, key: &Bytes, lamport: Lamport) {
        if let Some(entry) = self.keys.get_mut(key) {
            if entry.resolved.remove(&lamport).is_some() {
                self.resolved -= 1;
            }
        }
        self.prune(key);
    }

    pub(crate) fn overlay(&self, key: &[u8]) -> Overlay {
        match self.keys.get(key) {
            None => Overlay {
                top: None,
                uncertain: false,
            },
            Some(entry) => Overlay {
                top: entry
                    .resolved
                    .iter()
                    .next_back()
                    .map(|(l, r)| (*l, r.clone())),
                uncertain: entry.inflight > 0,
            },
        }
    }

    /// How many acknowledged writes are waiting to be applied. A scan waits
    /// for this to reach zero rather than merging the overlay into a page.
    pub(crate) fn unapplied(&self) -> usize {
        self.resolved
    }

    fn prune(&mut self, key: &[u8]) {
        if self
            .keys
            .get(key)
            .is_some_and(|e| e.inflight == 0 && e.resolved.is_empty())
        {
            self.keys.remove(key);
        }
    }
}

/// The key's value as the owner sees it: the storage engine's answer, overlaid
/// with anything newer that is committed but not yet applied.
///
/// Comparing versions rather than trusting the overlay is what makes this
/// correct when a write resolves out of submission order: the log's order is
/// the only order that decides which write wins.
pub(crate) fn visible(
    committed: Option<Record>,
    overlay: &Overlay,
    now_millis: u64,
) -> Option<Record> {
    let Some((lamport, record)) = &overlay.top else {
        return committed;
    };
    let committed_version = committed.as_ref().map_or(Version::ZERO, |r| r.version);
    if record.version(*lamport) <= committed_version {
        return committed;
    }
    match record {
        PendingRecord::Delete => None,
        PendingRecord::Put(record) if record.is_expired_at(now_millis) => None,
        PendingRecord::Put(record) => Some(record.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> Bytes {
        Bytes::from_static(b"lock/leader")
    }

    fn record(version: u64, expires: Option<u64>) -> Record {
        Record {
            value: Bytes::from_static(b"v"),
            version: Version(version),
            expires_at_millis: expires,
        }
    }

    #[test]
    fn a_submitted_write_makes_the_key_uncertain_until_it_resolves() {
        let mut pending = PendingSet::default();
        let ticket = pending.reserve(&key());
        assert!(pending.overlay(&key()).uncertain);

        pending.resolve(&ticket, Lamport(5), PendingRecord::Put(record(5, None)));
        assert!(
            !pending.overlay(&key()).uncertain,
            "once the log has answered, the version is known"
        );
    }

    #[test]
    fn a_failed_write_leaves_nothing_behind() {
        let mut pending = PendingSet::default();
        let ticket = pending.reserve(&key());
        pending.abandon(&ticket);

        let overlay = pending.overlay(&key());
        assert!(!overlay.uncertain);
        assert!(
            overlay.top.is_none(),
            "an unacknowledged write is invisible"
        );
        assert_eq!(pending.unapplied(), 0);
    }

    #[test]
    fn the_newest_lamport_wins_regardless_of_the_order_writes_resolve_in() {
        // Two writes to one key can come back from the log in either order,
        // and the log's order is the one that decides the final value.
        let mut pending = PendingSet::default();
        let first = pending.reserve(&key());
        let second = pending.reserve(&key());
        pending.resolve(&second, Lamport(9), PendingRecord::Put(record(9, None)));
        pending.resolve(&first, Lamport(8), PendingRecord::Put(record(8, None)));

        let overlay = pending.overlay(&key());
        assert_eq!(overlay.top.unwrap().0, Lamport(9));
    }

    #[test]
    fn an_applied_write_stops_being_an_overlay() {
        let mut pending = PendingSet::default();
        let ticket = pending.reserve(&key());
        pending.resolve(&ticket, Lamport(3), PendingRecord::Put(record(3, None)));
        assert_eq!(pending.unapplied(), 1);

        pending.applied(&key(), Lamport(3));
        assert_eq!(pending.unapplied(), 0);
        assert!(pending.overlay(&key()).top.is_none());
    }

    #[test]
    fn an_overlay_older_than_storage_is_ignored() {
        // Storage has already moved past this write, so the overlay entry is a
        // straggler and must not drag the key backwards.
        let mut pending = PendingSet::default();
        let ticket = pending.reserve(&key());
        pending.resolve(&ticket, Lamport(2), PendingRecord::Put(record(2, None)));

        let seen = visible(Some(record(7, None)), &pending.overlay(&key()), 0);
        assert_eq!(seen.unwrap().version, Version(7));
    }

    #[test]
    fn a_pending_delete_hides_a_committed_value() {
        let mut pending = PendingSet::default();
        let ticket = pending.reserve(&key());
        pending.resolve(&ticket, Lamport(4), PendingRecord::Delete);

        assert!(visible(Some(record(1, None)), &pending.overlay(&key()), 0).is_none());
    }

    #[test]
    fn a_pending_write_that_already_expired_is_invisible() {
        let mut pending = PendingSet::default();
        let ticket = pending.reserve(&key());
        pending.resolve(
            &ticket,
            Lamport(4),
            PendingRecord::Put(record(4, Some(100))),
        );

        assert!(visible(None, &pending.overlay(&key()), 100).is_none());
        assert!(visible(None, &pending.overlay(&key()), 99).is_some());
    }
}
