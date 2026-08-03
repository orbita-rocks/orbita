//! Read leases and per-key invalidation.
//!
//! This is the bookkeeping behind
//! [ADR 0001](../../../docs/adr/0001-linearizable-reads-from-replicas.md). A
//! replica serves a read locally only when it holds a live lease, has missed no
//! invalidation, and has not been told this particular key is changing.
//! Everything else forwards to the owner, because being wrong in the
//! conservative direction costs one hop and being wrong in the other direction
//! breaks the guarantee the product is sold on.
//!
//! The two sides measure the lease from different instants on purpose. The
//! owner counts a lease as live until `sent + duration`; the replica counts its
//! own as expired at `received + duration - margin`. Since the replica received
//! the grant after the owner sent it, the replica always gives up first, and
//! that ordering is what makes the scheme safe without synchronised clocks.
//!
//! Both sides use monotonic time. Wall time can jump backwards across an NTP
//! correction, which would either extend a lease past its expiry or expire one
//! early, and the first of those is a correctness bug.
//!
//! # What is not wired up yet
//!
//! An invalidation rides on the WAL entry that replicates the write, so the
//! replica side of this is fed by inbound replication. `orbita_wal::WalService`
//! has no hook for that today, so a replica in this build never receives an
//! invalidation, never holds a lease, and therefore forwards every read. That
//! is the safe end of the design rather than a partial one, and the missing
//! piece is a callback on the WAL's append path.

// The replica half of this module has no caller yet, because the invalidation
// that drives it arrives on WAL replication and `orbita_wal::WalService` has
// no hook to hand it over. The logic and its tests are here so that wiring
// that hook is a small change rather than a design exercise.
#![allow(dead_code)]

use bytes::Bytes;
use orbita_core::{Lamport, NodeId};

use std::collections::HashMap;
use std::time::Duration;

/// How much earlier than the owner a replica gives up its lease.
///
/// It only has to cover the difference in rate between two monotonic clocks
/// over one lease duration, so it is small. It is not a network delay budget;
/// the network delay is already accounted for by the two sides measuring from
/// different instants.
pub const DEFAULT_LEASE_MARGIN: Duration = Duration::from_millis(50);

/// The default lease duration, from ADR 0001.
///
/// Too short and heartbeat traffic climbs and replicas flap out of the read
/// set; too long and a partition stalls writes for that long when a replica
/// goes quiet. ADR 0001 says start here, measure, and expose it.
pub const DEFAULT_LEASE_DURATION: Duration = Duration::from_millis(500);

/// A replica's view of its own read lease and the invalidations it has been
/// told about.
#[derive(Debug, Default)]
pub(crate) struct ReplicaReadState {
    /// Monotonic nanoseconds. Zero means no lease, which is the state a
    /// replica starts in and returns to whenever anything is unclear.
    lease_until_nanos: u64,
    /// The Lamport the next invalidation must carry. Anything else means one
    /// went missing.
    next_expected: Lamport,
    gap: bool,
    invalid: HashMap<Bytes, Lamport>,
}

impl ReplicaReadState {
    /// Starts from what this node has already applied, so the first
    /// invalidation after a restart is checked against a real position rather
    /// than against zero.
    pub(crate) fn new(applied_through: Lamport) -> Self {
        Self {
            next_expected: applied_through.next(),
            ..Default::default()
        }
    }

    /// Records a lease the owner granted, measured from now so that the
    /// replica's copy expires before the owner's.
    pub(crate) fn grant(&mut self, now_nanos: u64, duration: Duration, margin: Duration) {
        let held = duration.saturating_sub(margin);
        self.lease_until_nanos = now_nanos.saturating_add(held.as_nanos() as u64);
    }

    /// Gives up the lease. A replica does this the moment it cannot account
    /// for the invalidation stream, and an owner can do it by simply not
    /// renewing.
    pub(crate) fn drop_lease(&mut self) {
        self.lease_until_nanos = 0;
    }

    pub(crate) fn holds_lease(&self, now_nanos: u64) -> bool {
        now_nanos < self.lease_until_nanos
    }

    /// Marks a key unreadable, which the replica does before it makes the
    /// entry durable and before it acknowledges anything.
    ///
    /// An out-of-order or missing Lamport is a gap. A gap cannot be closed by
    /// guessing, so the lease goes and this replica stops serving until it has
    /// caught up.
    pub(crate) fn invalidate(&mut self, key: Bytes, lamport: Lamport) {
        if lamport < self.next_expected {
            // A retransmission of something already seen. Harmless.
            return;
        }
        if lamport > self.next_expected {
            self.gap = true;
            self.drop_lease();
        }
        self.next_expected = lamport.next();
        let slot = self.invalid.entry(key).or_insert(lamport);
        if lamport > *slot {
            *slot = lamport;
        }
    }

    /// Clears a key once the entry that invalidated it has been applied.
    pub(crate) fn applied(&mut self, key: &[u8], lamport: Lamport) {
        if self.invalid.get(key).is_some_and(|held| *held <= lamport) {
            self.invalid.remove(key);
        }
    }

    /// Declares the replica caught up to `through`, which is the only way a
    /// gap is closed.
    pub(crate) fn caught_up(&mut self, through: Lamport) {
        if through >= self.next_expected {
            self.next_expected = through.next();
        }
        self.gap = false;
        self.invalid.retain(|_, lamport| *lamport > through);
    }

    /// Whether this replica may answer a read for `key` from its own storage.
    pub(crate) fn may_serve(&self, now_nanos: u64, key: &[u8]) -> bool {
        self.holds_lease(now_nanos) && !self.gap && !self.invalid.contains_key(key)
    }

    /// How many keys are currently unreadable. Bounded by replication lag
    /// rather than by the keyspace, so its growth is a signal worth a metric.
    pub(crate) fn invalid_len(&self) -> usize {
        self.invalid.len()
    }
}

/// The owner's record of which replicas may still be serving reads.
///
/// A write may only be acknowledged once every lease holder has confirmed the
/// invalidation or its lease has run out, which is the coherence quorum ADR
/// 0001 keeps separate from the durability quorum.
#[derive(Debug, Default)]
pub(crate) struct LeaseTable {
    /// Monotonic nanoseconds at `sent + duration`, which is later than the
    /// replica's own view of the same lease.
    granted: HashMap<NodeId, u64>,
}

impl LeaseTable {
    pub(crate) fn grant(&mut self, node: NodeId, now_nanos: u64, duration: Duration) {
        self.granted
            .insert(node, now_nanos.saturating_add(duration.as_nanos() as u64));
    }

    /// Stops renewing a replica, which is how an owner takes a slow node out
    /// of the read set instead of waiting on it forever.
    pub(crate) fn revoke(&mut self, node: NodeId) {
        self.granted.remove(&node);
    }

    /// The replicas a write must hear from before it can be acknowledged.
    pub(crate) fn holders(&self, now_nanos: u64) -> Vec<NodeId> {
        let mut nodes: Vec<NodeId> = self
            .granted
            .iter()
            .filter(|(_, until)| now_nanos < **until)
            .map(|(node, _)| *node)
            .collect();
        // Sorted so that a caller's behaviour does not depend on hash order,
        // which the simulator would otherwise see as nondeterminism.
        nodes.sort_unstable();
        nodes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND: u64 = 1_000_000_000;

    fn key(name: &'static str) -> Bytes {
        Bytes::from_static(name.as_bytes())
    }

    #[test]
    fn a_replica_without_a_lease_serves_nothing() {
        let state = ReplicaReadState::new(Lamport::ZERO);
        assert!(!state.may_serve(0, b"k"));
    }

    #[test]
    fn a_replica_gives_up_its_lease_before_the_owner_does() {
        let mut replica = ReplicaReadState::new(Lamport::ZERO);
        let mut owner = LeaseTable::default();
        let duration = Duration::from_millis(500);

        // The owner sends at t=0 and the replica receives at t=10ms, which is
        // the ordering that makes the margin do its job.
        owner.grant(NodeId(2), 0, duration);
        replica.grant(10 * SECOND / 1000, duration, DEFAULT_LEASE_MARGIN);

        let replica_expiry = 10 * SECOND / 1000 + 450 * SECOND / 1000;
        assert!(!replica.holds_lease(replica_expiry));
        assert!(
            owner.holders(replica_expiry).contains(&NodeId(2)),
            "the owner must still believe the lease is live after the replica has given it up"
        );
    }

    #[test]
    fn a_lease_holder_serves_a_key_nobody_is_writing() {
        let mut state = ReplicaReadState::new(Lamport::ZERO);
        state.grant(0, Duration::from_millis(500), DEFAULT_LEASE_MARGIN);
        state.invalidate(key("busy"), Lamport(1));

        assert!(!state.may_serve(0, b"busy"), "the key being written");
        assert!(
            state.may_serve(0, b"quiet"),
            "a read of an untouched key must not be held up by write load elsewhere in the range"
        );
    }

    #[test]
    fn a_key_becomes_readable_again_once_its_entry_is_applied() {
        let mut state = ReplicaReadState::new(Lamport::ZERO);
        state.grant(0, Duration::from_millis(500), DEFAULT_LEASE_MARGIN);
        state.invalidate(key("k"), Lamport(1));
        state.applied(b"k", Lamport(1));

        assert!(state.may_serve(0, b"k"));
        assert_eq!(state.invalid_len(), 0);
    }

    #[test]
    fn a_missed_invalidation_stops_the_replica_serving_anything() {
        // Lamports are one sequence per partition precisely so that a missing
        // message shows up as a gap rather than as silence.
        let mut state = ReplicaReadState::new(Lamport::ZERO);
        state.grant(0, Duration::from_millis(500), DEFAULT_LEASE_MARGIN);
        state.invalidate(key("a"), Lamport(1));
        state.invalidate(key("c"), Lamport(3));

        assert!(
            !state.may_serve(0, b"quiet"),
            "a gap poisons the whole range"
        );
        assert!(!state.holds_lease(0), "and takes the lease with it");
    }

    #[test]
    fn catching_up_closes_a_gap() {
        let mut state = ReplicaReadState::new(Lamport::ZERO);
        state.invalidate(key("a"), Lamport(1));
        state.invalidate(key("c"), Lamport(3));
        state.caught_up(Lamport(3));
        state.grant(0, Duration::from_millis(500), DEFAULT_LEASE_MARGIN);

        assert!(state.may_serve(0, b"a"));
        assert!(state.may_serve(0, b"c"));
    }

    #[test]
    fn a_retransmitted_invalidation_is_not_mistaken_for_a_gap() {
        let mut state = ReplicaReadState::new(Lamport::ZERO);
        state.grant(0, Duration::from_millis(500), DEFAULT_LEASE_MARGIN);
        state.invalidate(key("a"), Lamport(1));
        state.invalidate(key("b"), Lamport(2));
        state.invalidate(key("a"), Lamport(1));

        assert!(state.holds_lease(0), "a duplicate must not drop the lease");
    }

    #[test]
    fn an_expired_lease_leaves_the_read_set() {
        let mut table = LeaseTable::default();
        table.grant(NodeId(2), 0, Duration::from_millis(500));
        table.grant(NodeId(3), 0, Duration::from_millis(500));

        assert_eq!(table.holders(0).len(), 2);
        assert!(
            table.holders(SECOND).is_empty(),
            "a write must not wait on a lease that has run out"
        );
    }

    #[test]
    fn revoking_a_lease_removes_a_replica_from_the_read_set_immediately() {
        let mut table = LeaseTable::default();
        table.grant(NodeId(2), 0, Duration::from_millis(500));
        table.revoke(NodeId(2));
        assert!(table.holders(0).is_empty());
    }
}
