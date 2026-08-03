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
//! # How the two halves are fed
//!
//! The replica half is fed by inbound replication through
//! `orbita_wal::ReplicaObserver`, which the server registers in
//! [`crate::replication`]: an entry arriving marks its key invalid before the
//! replica acknowledges it, and applying the entry clears the mark. The owner
//! half is fed by the lease heartbeat in [`crate::host`], which renews leases
//! on a timer and is what a write's coherence wait consults.

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
    /// Counts every change to the invalid set.
    ///
    /// Reading takes two steps, checking this state and then reading storage,
    /// and an invalidation that lands between them would otherwise go unseen:
    /// the reader checked before the mark was set and answered from a record
    /// it had already read. Worse, an invalidation that is applied in that
    /// window leaves no mark to find on a second look. Comparing this before
    /// and after the storage read closes both, at the cost of forwarding a
    /// read that raced an unrelated write in the same partition. That window
    /// is one storage read rather than one replication round trip, which is
    /// what keeps the objection in ADR 0001 to a partition-wide check from
    /// applying here.
    generation: u64,
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
        self.generation += 1;
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
            self.generation += 1;
        }
    }

    /// Takes a lease the owner offered, or refuses it.
    ///
    /// A lease is refused unless this replica has already been told about
    /// every write up to `through`, which is where the owner's log was when it
    /// sent the offer. Without that check a replica could take a lease while
    /// an invalidation it never received is outstanding, and then serve the
    /// value that invalidation was about. A refusal costs read capacity until
    /// the replica catches up; accepting wrongly costs the guarantee.
    ///
    /// A zero duration is how an owner probes a replica it has stopped
    /// trusting: it drops any lease and is always answered.
    pub(crate) fn accept_grant(
        &mut self,
        now_nanos: u64,
        through: Lamport,
        duration: Duration,
        margin: Duration,
    ) -> bool {
        if duration.is_zero() {
            self.drop_lease();
            return true;
        }
        if self.next_expected <= through {
            self.drop_lease();
            return false;
        }
        // Being past the owner's own log is also what closes a gap. The log
        // has no holes, so a replica that holds everything the owner had when
        // it sent the offer is not missing an entry below it either, whatever
        // an earlier out-of-order message made this think.
        self.gap = false;
        self.grant(now_nanos, duration, margin);
        true
    }

    /// A newer owner said history ends at `above`, so everything this replica
    /// was told about beyond that is never going to arrive.
    ///
    /// The lease goes with it. The truncation means ownership moved, and a
    /// lease from the deposed owner is exactly what the new owner is waiting
    /// out before it accepts a write.
    pub(crate) fn truncated(&mut self, above: Lamport) {
        self.generation += 1;
        self.drop_lease();
        self.invalid.retain(|_, lamport| *lamport <= above);
        self.next_expected = above.next();
        self.gap = false;
    }

    /// Whether this replica may answer a read for `key` from its own storage,
    /// and the generation the answer is only good for.
    ///
    /// The caller reads storage and then calls [`ReplicaReadState::still_serving`]
    /// with what it got back. Answering from a record read before an
    /// invalidation arrived is exactly the stale read the whole design is
    /// against, and checking once cannot see it.
    pub(crate) fn may_serve(&self, now_nanos: u64, key: &[u8]) -> Option<u64> {
        (self.holds_lease(now_nanos) && !self.gap && !self.invalid.contains_key(key))
            .then_some(self.generation)
    }

    /// Whether a record read at `generation` is still one this replica may
    /// hand out.
    pub(crate) fn still_serving(&self, now_nanos: u64, key: &[u8], generation: u64) -> bool {
        self.generation == generation && self.may_serve(now_nanos, key).is_some()
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

    /// The replicas a write must hear from, with when each one's lease runs
    /// out on the owner's clock.
    ///
    /// The expiry is what bounds the wait. An owner that cannot get an
    /// acknowledgement out of a lease holder waits until that lease has
    /// certainly lapsed and then stops counting the replica, which is how one
    /// sick node costs a partition one lease duration rather than its write
    /// path.
    ///
    /// Sorted so that a caller's behaviour does not depend on hash order,
    /// which the simulator would otherwise see as nondeterminism.
    pub(crate) fn holders_with_expiry(&self, now_nanos: u64) -> Vec<(NodeId, u64)> {
        let mut held: Vec<(NodeId, u64)> = self
            .granted
            .iter()
            .filter(|(_, until)| now_nanos < **until)
            .map(|(node, until)| (*node, *until))
            .collect();
        held.sort_unstable();
        held
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
        assert!(state.may_serve(0, b"k").is_none());
    }

    #[test]
    fn a_read_is_refused_when_an_invalidation_landed_while_it_was_reading() {
        // The window this closes is real: the check says the key is readable,
        // the storage read starts, the owner's write arrives and is
        // acknowledged to its client, and the read then answers with the value
        // that write replaced.
        let mut state = ReplicaReadState::new(Lamport::ZERO);
        state.grant(0, Duration::from_millis(500), DEFAULT_LEASE_MARGIN);
        let taken = state.may_serve(0, b"k").expect("readable to start with");

        state.invalidate(key("k"), Lamport(1));
        assert!(!state.still_serving(0, b"k", taken));
    }

    #[test]
    fn a_read_is_refused_when_the_key_was_invalidated_and_applied_mid_read() {
        // The nastier half of the same window: by the time the reader looks
        // again there is no mark left to find, because the entry that set it
        // has already been applied.
        let mut state = ReplicaReadState::new(Lamport::ZERO);
        state.grant(0, Duration::from_millis(500), DEFAULT_LEASE_MARGIN);
        let taken = state.may_serve(0, b"k").expect("readable to start with");

        state.invalidate(key("k"), Lamport(1));
        state.applied(b"k", Lamport(1));
        assert!(
            state.may_serve(0, b"k").is_some(),
            "the key is readable again, but not from a record read before the write"
        );
        assert!(!state.still_serving(0, b"k", taken));
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
            owner
                .holders_with_expiry(replica_expiry)
                .iter()
                .any(|(node, _)| *node == NodeId(2)),
            "the owner must still believe the lease is live after the replica has given it up"
        );
    }

    #[test]
    fn a_lease_holder_serves_a_key_nobody_is_writing() {
        let mut state = ReplicaReadState::new(Lamport::ZERO);
        state.grant(0, Duration::from_millis(500), DEFAULT_LEASE_MARGIN);
        state.invalidate(key("busy"), Lamport(1));

        assert!(
            state.may_serve(0, b"busy").is_none(),
            "the key being written"
        );
        assert!(
            state.may_serve(0, b"quiet").is_some(),
            "a read of an untouched key must not be held up by write load elsewhere in the range"
        );
    }

    #[test]
    fn a_key_becomes_readable_again_once_its_entry_is_applied() {
        let mut state = ReplicaReadState::new(Lamport::ZERO);
        state.grant(0, Duration::from_millis(500), DEFAULT_LEASE_MARGIN);
        state.invalidate(key("k"), Lamport(1));
        state.applied(b"k", Lamport(1));

        assert!(state.may_serve(0, b"k").is_some());
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
            state.may_serve(0, b"quiet").is_none(),
            "a gap poisons the whole range"
        );
        assert!(!state.holds_lease(0), "and takes the lease with it");
    }

    #[test]
    fn a_gap_closes_when_the_owner_offers_a_lease_from_behind_where_this_replica_is() {
        // The log has no holes, so holding everything the owner had when it
        // made the offer proves nothing below it went missing either.
        let mut state = ReplicaReadState::new(Lamport::ZERO);
        state.invalidate(key("a"), Lamport(1));
        state.invalidate(key("c"), Lamport(3));
        state.applied(b"a", Lamport(1));
        state.applied(b"c", Lamport(3));

        assert!(state.accept_grant(
            0,
            Lamport(3),
            Duration::from_millis(500),
            DEFAULT_LEASE_MARGIN
        ));
        assert!(state.may_serve(0, b"a").is_some());
        assert!(state.may_serve(0, b"c").is_some());
    }

    #[test]
    fn a_replica_refuses_a_lease_it_is_not_caught_up_enough_to_hold() {
        let mut state = ReplicaReadState::new(Lamport::ZERO);
        state.invalidate(key("a"), Lamport(1));

        assert!(
            !state.accept_grant(
                0,
                Lamport(4),
                Duration::from_millis(500),
                DEFAULT_LEASE_MARGIN
            ),
            "the owner is past what this replica has been told about"
        );
        assert!(!state.holds_lease(0));
    }

    #[test]
    fn a_probe_takes_no_lease_and_is_always_answered() {
        let mut state = ReplicaReadState::new(Lamport::ZERO);
        state.grant(0, Duration::from_millis(500), DEFAULT_LEASE_MARGIN);
        assert!(state.accept_grant(0, Lamport(9), Duration::ZERO, DEFAULT_LEASE_MARGIN));
        assert!(
            !state.holds_lease(0),
            "a probe is how an owner makes a replica give up a lease it cannot confirm"
        );
    }

    #[test]
    fn a_truncation_drops_the_lease_and_the_marks_that_will_never_arrive() {
        let mut state = ReplicaReadState::new(Lamport::ZERO);
        state.grant(0, Duration::from_millis(500), DEFAULT_LEASE_MARGIN);
        state.invalidate(key("kept"), Lamport(1));
        state.invalidate(key("dropped"), Lamport(2));

        state.truncated(Lamport(1));
        assert!(
            !state.holds_lease(0),
            "a new owner's history change ends the lease"
        );
        assert_eq!(
            state.invalid_len(),
            1,
            "only what survived truncation stays marked"
        );

        state.applied(b"kept", Lamport(1));
        assert_eq!(state.invalid_len(), 0);
    }

    #[test]
    fn an_expired_lease_is_not_reported_as_a_holder() {
        let mut table = LeaseTable::default();
        table.grant(NodeId(2), 0, Duration::from_millis(500));
        assert_eq!(table.holders_with_expiry(0).len(), 1);
        assert!(table.holders_with_expiry(SECOND).is_empty());
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

        assert_eq!(table.holders_with_expiry(0).len(), 2);
        assert!(
            table.holders_with_expiry(SECOND).is_empty(),
            "a write must not wait on a lease that has run out"
        );
    }

    #[test]
    fn revoking_a_lease_removes_a_replica_from_the_read_set_immediately() {
        let mut table = LeaseTable::default();
        table.grant(NodeId(2), 0, Duration::from_millis(500));
        table.revoke(NodeId(2));
        assert!(table.holders_with_expiry(0).is_empty());
    }
}
