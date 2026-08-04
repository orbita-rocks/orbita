//! What "ready" means, held in one place so it cannot mean three things.
//!
//! Readiness used to be inferred: the process answers on the client port,
//! therefore it is ready. That answer arrives before the node has registered
//! with the leader group, before its write-ahead log is recovered, and before
//! its partitions are open, which is exactly the window in which a rolling
//! upgrade must not advance. The gate makes the real conditions explicit and
//! lets each subsystem mark its own as it completes startup.
//!
//! The state is a [`tokio::sync::watch`] channel rather than a set of atomics
//! so that it is consumable programmatically as well as over the wire: a
//! subscriber can await the node becoming ready, which is what the SIGTERM
//! partition handoff (issue #26) needs from a peer and what a test needs from
//! a harness. The gRPC surface in [`crate::service`] is only one reader of it.

use tokio::sync::watch;

/// One thing that must have happened before this node may call itself ready.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReadinessCondition {
    /// The node has registered with the leader group at least once, so the
    /// control plane knows it exists and it holds a map the group published.
    ///
    /// Met immediately on a node configured without a leader group, because a
    /// single-node cluster answers to nobody and would otherwise never be
    /// ready. It is not cleared when a later heartbeat fails: a control plane
    /// outage must not become a data plane outage, and a node that has joined
    /// keeps serving on its last good map.
    ControlPlaneJoined,
    /// Every write-ahead log this node holds has been replayed to its trusted
    /// end. Opening a partition is what recovers its log, so this is met when
    /// the initial open of every held partition completes.
    WalRecovered,
    /// The set of open partitions matches the current map and each one is
    /// serving. Cleared again if a later map change hands this node a
    /// partition it cannot open, because a node refusing requests for a
    /// partition it was given is not ready no matter how it got there.
    PartitionsCaughtUp,
    /// The node has not begun a planned shutdown. Clearing this makes the
    /// readiness probe fail before ownership starts moving away.
    AcceptingOwnership,
}

impl ReadinessCondition {
    /// Every condition, in the order reports list them.
    pub const ALL: [Self; 4] = [
        Self::ControlPlaneJoined,
        Self::WalRecovered,
        Self::PartitionsCaughtUp,
        Self::AcceptingOwnership,
    ];

    /// A stable machine-readable name, which is what crosses the wire and what
    /// a probe failure log greps for.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::ControlPlaneJoined => "control-plane-joined",
            Self::WalRecovered => "wal-recovered",
            Self::PartitionsCaughtUp => "partitions-caught-up",
            Self::AcceptingOwnership => "accepting-ownership",
        }
    }
}

impl std::fmt::Display for ReadinessCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// A snapshot of which conditions hold.
///
/// `Copy`, so a reader takes it out of the watch channel and reasons about a
/// moment rather than about state that moves under it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadinessState {
    joined: bool,
    recovered: bool,
    caught_up: bool,
    accepting_ownership: bool,
}

impl ReadinessState {
    /// Whether every condition holds.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.joined && self.recovered && self.caught_up && self.accepting_ownership
    }

    /// Whether one condition holds.
    #[must_use]
    pub fn is_met(&self, condition: ReadinessCondition) -> bool {
        match condition {
            ReadinessCondition::ControlPlaneJoined => self.joined,
            ReadinessCondition::WalRecovered => self.recovered,
            ReadinessCondition::PartitionsCaughtUp => self.caught_up,
            ReadinessCondition::AcceptingOwnership => self.accepting_ownership,
        }
    }

    /// The conditions that do not hold yet, in reporting order.
    #[must_use]
    pub fn unmet(&self) -> Vec<ReadinessCondition> {
        ReadinessCondition::ALL
            .into_iter()
            .filter(|condition| !self.is_met(*condition))
            .collect()
    }

    fn set(&mut self, condition: ReadinessCondition, met: bool) {
        match condition {
            ReadinessCondition::ControlPlaneJoined => self.joined = met,
            ReadinessCondition::WalRecovered => self.recovered = met,
            ReadinessCondition::PartitionsCaughtUp => self.caught_up = met,
            ReadinessCondition::AcceptingOwnership => self.accepting_ownership = met,
        }
    }
}

/// The gate subsystems mark as they finish starting up, and everything else
/// asks.
///
/// A fresh gate reports nothing met, so a node is unready until proven
/// otherwise rather than the reverse.
#[derive(Debug)]
pub struct ReadinessGate {
    state: watch::Sender<ReadinessState>,
}

impl Default for ReadinessGate {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadinessGate {
    /// A gate with every condition unmet.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: watch::Sender::new(ReadinessState::default()),
        }
    }

    /// Records that a condition now holds.
    pub fn mark(&self, condition: ReadinessCondition) {
        self.set(condition, true);
    }

    /// Records that a condition no longer holds.
    pub fn clear(&self, condition: ReadinessCondition) {
        self.set(condition, false);
    }

    fn set(&self, condition: ReadinessCondition, met: bool) {
        // Only a real change notifies subscribers, so a heartbeat that marks
        // the same condition every interval does not look like churn.
        self.state.send_if_modified(|state| {
            if state.is_met(condition) == met {
                return false;
            }
            state.set(condition, met);
            true
        });
    }

    /// The state right now.
    #[must_use]
    pub fn state(&self) -> ReadinessState {
        *self.state.borrow()
    }

    /// Whether every condition holds right now.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.state().is_ready()
    }

    /// A receiver that sees every state change, which is how something waits
    /// for readiness rather than polling for it.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<ReadinessState> {
        self.state.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_gate_reports_every_condition_unmet() {
        let gate = ReadinessGate::new();
        assert_eq!(gate.state().unmet(), ReadinessCondition::ALL.to_vec());
    }

    #[test]
    fn a_fresh_gate_is_not_ready() {
        assert!(!ReadinessGate::new().is_ready());
    }

    #[test]
    fn one_condition_alone_does_not_make_the_gate_ready() {
        let gate = ReadinessGate::new();
        gate.mark(ReadinessCondition::WalRecovered);
        assert!(!gate.is_ready());
    }

    #[test]
    fn marking_every_condition_makes_the_gate_ready() {
        let gate = ReadinessGate::new();
        for condition in ReadinessCondition::ALL {
            gate.mark(condition);
        }
        assert!(gate.is_ready());
    }

    #[test]
    fn clearing_a_condition_makes_a_ready_gate_unready_again() {
        let gate = ReadinessGate::new();
        for condition in ReadinessCondition::ALL {
            gate.mark(condition);
        }
        gate.clear(ReadinessCondition::PartitionsCaughtUp);
        assert_eq!(
            gate.state().unmet(),
            vec![ReadinessCondition::PartitionsCaughtUp]
        );
    }

    #[tokio::test]
    async fn a_subscriber_sees_the_gate_become_ready() {
        let gate = ReadinessGate::new();
        let mut watched = gate.subscribe();
        for condition in ReadinessCondition::ALL {
            gate.mark(condition);
        }
        watched
            .wait_for(ReadinessState::is_ready)
            .await
            .expect("the gate outlives the wait");
    }

    #[test]
    fn marking_a_condition_that_already_holds_does_not_notify_subscribers() {
        let gate = ReadinessGate::new();
        gate.mark(ReadinessCondition::WalRecovered);
        let watched = gate.subscribe();
        gate.mark(ReadinessCondition::WalRecovered);
        assert!(
            !watched.has_changed().expect("the gate is still alive"),
            "a repeated mark is not a change"
        );
    }

    #[test]
    fn every_condition_has_a_distinct_wire_name() {
        let names: std::collections::HashSet<&str> = ReadinessCondition::ALL
            .into_iter()
            .map(ReadinessCondition::name)
            .collect();
        assert_eq!(names.len(), ReadinessCondition::ALL.len());
    }
}
