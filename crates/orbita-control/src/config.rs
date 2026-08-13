//! The numbers the control plane's timing depends on, and the reasoning for
//! the defaults.
//!
//! The requirement is that writes to a partition are unavailable for less than
//! ten seconds after its owner dies. Every interval here is a piece of that
//! budget, so they are stated together rather than scattered across the code
//! that uses them.

use std::time::Duration;

/// Timing and placement policy for a leader group node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlConfig {
    /// How often a node sends its status report.
    pub heartbeat_interval: Duration,

    /// Silence after which a node is marked suspect. Suspect changes nothing
    /// operationally; it exists so an operator sees trouble before the cluster
    /// acts on it.
    pub suspect_after: Duration,

    /// Silence after which a node is marked dead and its partitions are
    /// failed over.
    pub dead_after: Duration,

    /// How often the leader re-evaluates health, placement, and failover.
    pub sweep_interval: Duration,

    /// How long a read lease an owner grants its replicas lasts, per
    /// [ADR 0001](../../../docs/adr/0001-linearizable-reads-from-replicas.md).
    /// This is not an independent knob: a promoted owner has to wait it out
    /// before accepting writes, so it is part of the failover budget.
    pub lease_duration: Duration,

    /// Slack added to the lease wait before a promotion, covering clock rate
    /// differences between the deposed owner and the leader group.
    pub lease_margin: Duration,

    /// How many copies of a partition the cluster tries to keep, counting the
    /// owner. Three gives the two-of-three durability quorum the WAL assumes.
    pub replication_factor: usize,

    /// Size at which a partition becomes a candidate for an automatic split.
    /// The worker-prepared split protocol executes on demand through the Admin
    /// API; triggering one from this threshold without an operator in the loop
    /// is the automatic-split work tracked by issue #39. Kept here so the number
    /// and its reasoning live with the rest of the control-plane timing.
    pub split_threshold_bytes: u64,

    /// How many replicas a partition keeps beyond its owner.
    ///
    /// Separate from `replication_factor`, which sizes the durability
    /// contract. This sizes the set that holds a copy and can therefore serve
    /// reads. Raising it buys read capacity; it does not change how many
    /// acknowledgements a write waits for, because a peer past the durability
    /// quorum follows the stream without being able to hold a write up. See
    /// ADR 0013.
    ///
    /// Defaults to `replication_factor - 1`, which is exactly the behaviour
    /// before the two were separable.
    pub read_replica_target: usize,

    /// Desired control-plane voters, independent from worker count.
    pub voter_target: usize,

    /// Whether this cluster has crossed into automatically managed membership.
    pub voter_management_enabled: bool,

    /// Continuous dead time before a voter may be replaced.
    pub voter_replacement_after: Duration,

    /// Whether the leader moves ownership to even it out across the cluster.
    ///
    /// One owner serialises a partition, and the owner does strictly more work
    /// per write than a replica: it admits the request, assigns the version,
    /// updates the index, evaluates conditions, coordinates the quorum, and
    /// runs flush and compaction. A node owning every partition of a keyspace
    /// therefore carries measurably more load than one that only replicates it
    /// — about 50% more CPU on the three-node cluster measured for issue #160.
    ///
    /// Splitting places children well, but placement at split time cannot fix
    /// skew that appears later: a failover moves every partition of a dead
    /// owner onto one survivor and nothing moves them back.
    pub ownership_rebalancing_enabled: bool,

    /// How much owner-count skew is tolerated before ownership is moved.
    ///
    /// Expressed as a difference in partitions owned, between the busiest and
    /// least busy eligible node. A move costs the partition a lease drain, so
    /// this is deliberately not zero: a cluster whose owner counts cannot
    /// divide evenly must not trade availability forever chasing a balance
    /// that does not exist. Two is the smallest threshold that is stable when
    /// partitions do not divide evenly by nodes, because a one-apart
    /// distribution is already the best achievable.
    pub ownership_skew_threshold: usize,

    /// How long after moving a partition's ownership before it may move again.
    ///
    /// Rebalancing competes with failover, drains, splits, and merges, all of
    /// which also move ownership. Without a cooldown the balancer would
    /// immediately undo a placement one of those made deliberately, and two
    /// mechanisms fighting over the same partition is worse than either
    /// imbalance they are arguing about.
    pub ownership_rebalance_cooldown: Duration,
}

impl Default for ControlConfig {
    fn default() -> Self {
        Self::for_failover_budget(Duration::from_secs(10))
    }
}

impl ControlConfig {
    /// The default timings, and why they are what they are.
    ///
    /// Detection dominates the budget, so it is where the argument lives. A
    /// heartbeat every 250ms and a death declaration at 3s means twelve
    /// consecutive missed reports before anything happens. The false positive
    /// this defends against is a node that is alive but stalled, and in a Rust
    /// process with no garbage collector the realistic stalls are a slow fsync
    /// or a starved scheduler. Those last hundreds of milliseconds, not three
    /// seconds. A single unlucky packet cannot trigger a failover, and a
    /// sustained stall should.
    ///
    /// The rest of the budget, against a 10 second target:
    ///
    /// | Stage | Worst case |
    /// |---|---|
    /// | Detection | 3s silence plus one 250ms sweep |
    /// | Commit the fencing epoch bump | one consensus round trip |
    /// | Drain the deposed owner's read leases | 550ms |
    /// | Commit the new owner and tell it | one consensus round trip |
    ///
    /// That is under four seconds with the two consensus round trips costed
    /// generously, which leaves room for a leader group election to happen at
    /// the same time as the worker failure. If that headroom ever disappears,
    /// `dead_after` is the knob, and shortening it trades failover speed for
    /// false positives rather than for correctness.
    ///
    /// The lease duration is 500ms, which is where ADR 0001 says to start.
    /// Shortening it makes failover faster and heartbeat traffic heavier;
    /// lengthening it does the reverse. It cannot be changed without
    /// re-costing this table, which is the whole reason the two live in one
    /// struct.
    #[must_use]
    pub fn for_failover_budget(budget: Duration) -> Self {
        let mut config = Self {
            heartbeat_interval: Duration::from_millis(250),
            suspect_after: Duration::from_secs(1),
            dead_after: Duration::from_secs(3),
            sweep_interval: Duration::from_millis(250),
            lease_duration: Duration::from_millis(500),
            lease_margin: Duration::from_millis(50),
            replication_factor: 3,
            read_replica_target: 2,
            split_threshold_bytes: 512 * 1024 * 1024,
            voter_target: 3,
            voter_management_enabled: false,
            voter_replacement_after: Duration::from_secs(5 * 60),
            ownership_rebalancing_enabled: true,
            ownership_skew_threshold: 2,
            // Comfortably longer than a failover takes end to end, so the
            // balancer never reacts to a partition that is still settling
            // from one.
            ownership_rebalance_cooldown: Duration::from_secs(30),
        };
        // A caller asking for a tighter budget than the defaults fit gets the
        // detection window scaled down rather than a silent overrun, because
        // the alternative is a configuration that reports a target it cannot
        // meet.
        if budget < Duration::from_secs(10) {
            let scale = budget.as_millis().max(1) as f64 / 10_000.0;
            config.heartbeat_interval = scale_duration(config.heartbeat_interval, scale);
            config.suspect_after = scale_duration(config.suspect_after, scale);
            config.dead_after = scale_duration(config.dead_after, scale);
            config.sweep_interval = scale_duration(config.sweep_interval, scale);
            config.lease_duration = scale_duration(config.lease_duration, scale);
            config.lease_margin = scale_duration(config.lease_margin, scale);
        }
        config
    }

    /// How long a promoted owner must wait after the fencing entry commits
    /// before it may accept writes.
    ///
    /// ADR 0001 lets a replica serve a read locally while it holds a live
    /// lease from the owner. The deposed owner may have renewed a lease an
    /// instant before it died, so a lease can still be live for
    /// `lease_duration` after the last moment that owner was running. Waiting
    /// that out from the fence, which is strictly later than the last thing
    /// the deposed owner did, means no replica can serve a pre-failover value
    /// after the new owner starts accepting writes.
    #[must_use]
    pub fn lease_drain(&self) -> Duration {
        self.lease_duration + self.lease_margin
    }

    /// The worst-case time from an owner's last heartbeat to the new owner
    /// being able to accept writes, excluding consensus round trips.
    ///
    /// Exposed so a test can assert the budget rather than restating it.
    #[must_use]
    pub fn failover_budget(&self) -> Duration {
        self.dead_after + self.sweep_interval + self.lease_drain() + self.sweep_interval
    }

    /// The worst-case time from the last fault healing to the cluster having
    /// finished reacting to it: every partition owned by a live worker, every
    /// surviving worker routing on the resulting map, every drain complete,
    /// and no sweep still proposing anything.
    ///
    /// [`ControlConfig::failover_budget`] answers "when can writes resume",
    /// which is the guarantee an operator was sold. This answers "when has the
    /// control plane stopped moving", which is the one a liveness check needs,
    /// and it is strictly longer because restoring redundancy and telling
    /// everybody about it both happen after writes are already back.
    ///
    /// Every term is a stage of the recovery, costed at its worst case:
    ///
    /// | Stage | Worst case |
    /// |---|---|
    /// | Detect the failure and promote a replacement | `failover_budget()` |
    /// | Commit the completed lease drain, then promote on the next sweep | one `sweep_interval` |
    /// | Every survivor reports at or past the fence's map version | two `heartbeat_interval` |
    /// | Repair the replica set the failure left short | one `sweep_interval` |
    /// | Each worker fetches the map, then reports it | two `heartbeat_interval` |
    /// | The same round trip for the repair's map bump | two `heartbeat_interval` |
    /// | The same round trip for a drain acknowledgement | two `heartbeat_interval` |
    /// | Two sweeps in which nothing further is proposed | two `sweep_interval` |
    ///
    /// A map round trip costs two heartbeats because that is what it costs in
    /// production: a worker learns of a new map on one poll and reports that
    /// it is routing on it on the next. Four of them are counted because the
    /// stages are serial in the worst case, even though they usually overlap.
    ///
    /// The second and third rows are not in `failover_budget`, and the gap is
    /// deliberate rather than an oversight in either. Committing the completed
    /// drain and waiting for post-fence evidence are what stop a promotion
    /// from being repeated after a leader change or decided on a report that
    /// predates the fence. They lengthen the path to a new owner without
    /// lengthening the window in which writes are unavailable for a reason
    /// anyone would call a failover, so the two numbers measure different
    /// things on purpose.
    ///
    /// The last row is not slack. It is how a controller loop that has
    /// finished is told apart from one that is still retrying every interval,
    /// and without it a cluster flapping forever between two decisions would
    /// pass by being observed at the right moment.
    ///
    /// At the default timings this is 7.05 seconds, against a 4.05 second
    /// failover budget. The margin is measured rather than argued: across
    /// every seeded control-plane schedule, including the aggressive ones
    /// currently parked against issue #76, the slowest cluster to converge
    /// takes 4.5 seconds and the median takes 0.5. Excluding those two the
    /// slowest is 3.0 seconds, so the margin is quoted from the schedules that
    /// stress it hardest rather than from the ones that flatter it.
    ///
    /// That leaves room for a stage landing badly against a sweep without
    /// leaving enough to hide a stage that has stopped happening altogether.
    #[must_use]
    pub fn convergence_bound(&self) -> Duration {
        self.failover_budget() + 4 * self.sweep_interval + 8 * self.heartbeat_interval
    }

    /// Sets how many replicas a partition keeps for read serving.
    ///
    /// Refused below `replication_factor - 1`, because that many are needed for
    /// durability and this knob may only add reach, never take copies away.
    pub fn with_read_replica_target(mut self, target: usize) -> orbita_core::Result<Self> {
        let floor = self.replication_factor.saturating_sub(1);
        if target < floor {
            return Err(orbita_core::Error::InvalidArgument(format!(
                "a read replica target of {target} is below the {floor} replicas durability needs"
            )));
        }
        self.read_replica_target = target;
        Ok(self)
    }

    /// Sets the supported odd voter target.
    pub fn with_voter_target(mut self, target: usize) -> orbita_core::Result<Self> {
        if !matches!(target, 3 | 5) {
            return Err(orbita_core::Error::InvalidArgument(format!(
                "voter target must be 3 or 5, got {target}"
            )));
        }
        self.voter_target = target;
        Ok(self)
    }

    /// Enables leader-driven voter repair after combined-node bootstrap.
    #[must_use]
    pub fn with_voter_management(mut self) -> Self {
        self.voter_management_enabled = true;
        self
    }
}

fn scale_duration(d: Duration, scale: f64) -> Duration {
    Duration::from_millis(((d.as_millis() as f64 * scale) as u64).max(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_timings_fit_the_ten_second_failover_target() {
        let config = ControlConfig::default();
        assert!(
            config.failover_budget() < Duration::from_secs(10),
            "the detection and drain stages alone must leave room for consensus, got {:?}",
            config.failover_budget()
        );
    }

    #[test]
    fn a_node_is_suspect_before_it_is_dead() {
        let config = ControlConfig::default();
        assert!(config.suspect_after < config.dead_after);
        assert!(config.heartbeat_interval < config.suspect_after);
    }

    #[test]
    fn a_tighter_budget_scales_detection_down_rather_than_overrunning() {
        let config = ControlConfig::for_failover_budget(Duration::from_secs(2));
        assert!(config.failover_budget() < Duration::from_secs(2));
        assert!(config.heartbeat_interval < config.suspect_after);
    }

    #[test]
    fn convergence_is_bounded_after_writes_have_already_resumed() {
        // Restoring redundancy and publishing the result both happen after the
        // new owner is accepting writes, so a convergence bound that did not
        // exceed the failover budget would be asserting the wrong thing.
        let config = ControlConfig::default();
        assert!(config.convergence_bound() > config.failover_budget());
        assert_eq!(config.convergence_bound(), Duration::from_millis(7_050));
    }

    #[test]
    fn the_convergence_bound_scales_with_the_timings_it_is_derived_from() {
        // A caller that tightens the failover budget must not be left with a
        // liveness bound stated in someone else's milliseconds.
        let tight = ControlConfig::for_failover_budget(Duration::from_secs(2));
        assert!(tight.convergence_bound() < ControlConfig::default().convergence_bound());
        assert!(tight.convergence_bound() > tight.failover_budget());
    }

    #[test]
    fn the_lease_drain_exceeds_the_lease_it_waits_out() {
        // A promoted owner that waits exactly one lease duration is racing the
        // deposed owner's clock. The margin is what removes the race.
        let config = ControlConfig::default();
        assert!(config.lease_drain() > config.lease_duration);
    }
}
