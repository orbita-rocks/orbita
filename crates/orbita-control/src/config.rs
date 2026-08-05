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

    /// Reserved size threshold for worker-prepared splits. Split execution is
    /// disabled until that protocol is implemented.
    pub split_threshold_bytes: u64,
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
            split_threshold_bytes: 512 * 1024 * 1024,
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
    /// failover budget. The margin is measured rather than argued: across the
    /// seeded control-plane batches the slowest cluster to converge takes 4.5
    /// seconds, and the median takes 0.5. That leaves room for a stage landing
    /// badly against a sweep without leaving enough to hide a stage that has
    /// stopped happening altogether.
    #[must_use]
    pub fn convergence_bound(&self) -> Duration {
        self.failover_budget() + 4 * self.sweep_interval + 8 * self.heartbeat_interval
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
