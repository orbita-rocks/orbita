//! The leader group's decision loop.
//!
//! Everything that proposes a command goes through here, and everything that
//! observes the cluster is stored here. The split between the two is the
//! design: the state machine in [`crate::state`] holds facts that every member
//! agrees on, and the controller holds the leader's own observations, which
//! are not agreed on and must not be.
//!
//! A monotonic clock reading from one node means nothing on another, so
//! replicating heartbeat arrival times would be meaningless as well as
//! expensive. The controller keeps them in memory. A leader that has just
//! taken over therefore starts with no observations and treats every node as
//! freshly heard from, which delays a failover it would otherwise have started
//! immediately. That is the conservative direction: the cost is up to one
//! detection window of extra downtime after a leader change, and the thing it
//! buys is that a new leader can never fence a node on the strength of a clock
//! reading it did not take.

use crate::command::ControlCommand;
use crate::config::ControlConfig;
use crate::consensus::{ConsensusLog, LogIndex, MembershipChange, RaftMember};
use crate::membership::{NodeHealth, NodeRole, NodeStatus};
use crate::model::{hash_secret, Credential, Keyspace, KeyspaceConfig, Permission};
use crate::state::{ClusterState, NodeRecord, PartitionPhase};
use crate::version::{binary_speaks, ClusterVersion, CompatibilityRefusal};

use bytes::Bytes;
use orbita_core::{
    Epoch, Error, Lamport, MapVersion, NodeId, PartitionId, PartitionInfo, PartitionMap, Result,
};
use orbita_runtime::{Clock, Runtime, Transport};

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// What the leader has heard from one node, and when.
#[derive(Debug, Clone)]
struct Observation {
    heard_at_nanos: u64,
    status: NodeStatus,
}

/// What a fresh cluster should be created with.
#[derive(Debug, Clone)]
pub struct BootstrapSpec {
    /// The keyspace to create. A cluster with no keyspace has nowhere to put
    /// anything, so `orbita dev` would otherwise need a second command before
    /// it was usable.
    pub keyspace: String,
    pub config: KeyspaceConfig,
    /// Leader group members to admit, as id and address.
    pub leaders: Vec<(NodeId, String)>,
    /// Workers to admit before the first partition is placed, so that the
    /// keyspace is born with an owner rather than born unavailable.
    ///
    /// Admitted ready, which is what makes that sentence true: ownership goes
    /// only to a ready node, so admitting these as not-ready would create the
    /// keyspace with no owner and leave it unservable until a heartbeat and a
    /// placement sweep had both happened. Whoever writes this list is starting
    /// these nodes in the same breath, and any of them that turns out not to
    /// be ready says so on its first heartbeat, a fraction of a second later.
    pub workers: Vec<(NodeId, String)>,
}

/// A node as an operator sees it.
#[derive(Debug, Clone)]
pub struct NodeView {
    pub record: NodeRecord,
    /// How long since the leader last heard from it, in milliseconds. `None`
    /// when this leader has never heard from it at all.
    pub silent_for_millis: Option<u64>,
    /// The map version this node last said it was routing on. `None` when this
    /// leader has never heard from it.
    ///
    /// This is the leader's own evidence that a decision it published actually
    /// landed, which is what makes "the cluster has caught up" answerable
    /// rather than a matter of waiting long enough and assuming.
    pub reported_map_version: Option<MapVersion>,
    pub is_control_leader: bool,
    /// What the indexes of every partition this node reported cost in memory.
    /// Summed here rather than in the CLI because the leader group holds the
    /// per-partition reports, including for partitions the node holds as a
    /// replica, which never appear against it in the partition table.
    ///
    /// `None` when any part of the answer is missing: a node this leader has
    /// never heard from, or one whose report predates index measurement. A
    /// partial sum is worse than no sum, because it looks like a small
    /// number rather than like a missing one.
    pub index_memory_bytes: Option<u64>,
}

/// A partition as an operator sees it, with the observations the map does not
/// carry.
#[derive(Debug, Clone)]
pub struct PartitionView {
    pub info: PartitionInfo,
    pub phase: PartitionPhase,
    /// The owner's committed prefix: the highest Lamport a durability quorum
    /// confirmed under its epoch.
    ///
    /// Read from the owner's committed prefix rather than its durable
    /// position, because the two differ and only one of them is safe to show.
    /// This is the field issue #87 raised: it used to be filled in from
    /// `orbita_wal::PartitionLog::durable_lamport`, which is one disk, under
    /// a name that promises a quorum. Between the two sit entries whose
    /// `commit` returned `Unavailable`.
    /// A draining owner's `quiesce` truncates the writes it holds alone —
    /// writes whose clients were told they failed — which drops its durable
    /// position. Sourcing this column from that number made a planned
    /// shutdown render as a partition going backwards, which is the reading
    /// of "lost data" and is not what happened. The committed prefix only
    /// ever rises.
    ///
    /// `None` when no owner has reported: an unowned partition, or one
    /// [`PartitionPhase::Fenced`] is holding while its replicas report past
    /// the fence. Lamport zero is a position a partition can genuinely be at,
    /// and a fenced partition mid-failover is not at it.
    pub committed_lamport: Option<Lamport>,
    /// What the owner reported this partition holds. `None` for the same
    /// reason as [`Self::committed_lamport`]: a fenced partition full of data
    /// has an unknown size, not an empty one, and that difference decides
    /// whether an operator thinks losing it is cheap.
    pub size_bytes: Option<u64>,
    /// What the owner's index for this partition costs in memory. A single
    /// partition's index has to fit on its owner, so this is read against one
    /// machine rather than against the cluster.
    ///
    /// `None` when nobody has said: an unowned partition, an owner that has
    /// not reported yet, or an owner running a binary that does not measure
    /// it. Distinct from `Some(0)`, which is a partition whose index really
    /// is empty.
    pub index_bytes: Option<u64>,
    /// Each replica's applied and durable positions. Both are here because
    /// the gap between them is log a replica holds and has not applied, and
    /// the gap from the owner is how far behind it would be if promoted.
    pub replica_progress: Vec<ReplicaProgressView>,
}

/// One replica's position on one partition, as an operator sees it.
///
/// Both positions are optional because a replica the leader group has not
/// heard from has not told anybody where it is. #79 named that state
/// `Unestablished` on the owner's side and made the point precisely: an
/// absent answer read as a healthy one hides a cliff, and an absent answer
/// read as lamport zero invents a maximally-behind replica that may in fact
/// be perfectly current.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaProgressView {
    pub node: NodeId,
    pub applied_lamport: Option<Lamport>,
    pub durable_lamport: Option<Lamport>,
}

/// Everything `DescribeCluster` answers with.
#[derive(Debug, Clone)]
pub struct ClusterView {
    pub nodes: Vec<NodeView>,
    pub partitions: Vec<PartitionView>,
    /// The active cluster version, so `cluster describe` answers "did the
    /// finalize land" without a second command.
    pub cluster_version: ClusterVersion,
}

/// What `finalize-upgrade` committed: where the cluster was and where it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinalizedUpgrade {
    pub previous: ClusterVersion,
    pub active: ClusterVersion,
}

/// The replicated control plane's decision on one status report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistrationOutcome {
    Accepted(MapVersion),
    Incompatible(CompatibilityRefusal),
}

struct Inner {
    state: ClusterState,
    applied: LogIndex,
    /// The outcome of applying each recent entry, so the task that proposed it
    /// learns what its command did even when another task did the applying.
    results: BTreeMap<LogIndex, Result<()>>,
    observations: BTreeMap<NodeId, Observation>,
    /// Reports from returned incarnations: a member id heartbeating under a
    /// durable identity that differs from the one committed membership binds
    /// to that id (ADR 0014). Kept apart from `observations` on purpose. The
    /// old incarnation's silence is what proves its seat is permanently lost,
    /// and folding the new incarnation's heartbeats into the ordinary map
    /// would revive the seat and suppress the very repair that reseats it.
    /// Leader-local, like every observation.
    returnees: BTreeMap<NodeId, Observation>,
    /// Durable split-preparation acknowledgements: for each parent being split,
    /// the holders that have reported their child storage is prepared. This is
    /// an observation, not replicated state — a worker re-reports it whenever it
    /// is asked, so a new leader re-collects it rather than inheriting it — and
    /// the driver turns it into the replicated `MarkSplitPrepared` entries the
    /// completion depends on. It is the real durable ack the review demanded in
    /// place of a mere map-version observation.
    prepared_splits: BTreeMap<(PartitionId, PartitionId, PartitionId), BTreeSet<NodeId>>,
    prepared_merges: Vec<(crate::MergeGeneration, BTreeSet<NodeId>)>,
    /// When this leader first saw each partition in the fenced phase, on its
    /// own monotonic clock. Not replicated, because the lease wait it drives
    /// is measured from an instant only this node observed.
    fenced_since: BTreeMap<PartitionId, u64>,
    /// When this controller started observing. A node it has never heard from
    /// is timed from here rather than from the beginning of time.
    observing_since: u64,
    /// When this leader last moved ownership to even out load, on its own
    /// monotonic clock. Rebalancing is rate limited against this rather than
    /// per partition: each move costs one partition a lease drain, so what has
    /// to be bounded is how often the cluster gives up availability for
    /// balance, not how often any single partition does.
    ///
    /// Not replicated, and cleared on a leadership change, for the same reason
    /// `fenced_since` is: it times an interval only this node observed. A new
    /// leader waiting one extra cooldown before its first move is the safe
    /// direction to be wrong in.
    last_rebalance_at: Option<u64>,
    /// Leadership changes invalidate every local failure-detection deadline.
    was_leader: bool,
}

/// The Lamport a node must already hold before it may be handed `info`.
///
/// The owner's committed prefix — the highest Lamport a durability quorum
/// confirmed — not its raw durable position. The two differ by exactly the tail
/// the owner wrote to its own disk and could not replicate, whose clients were
/// told the write failed. The WAL is not allowed to ship that tail, so
/// requiring a replica to reach the owner's durable position asks for a Lamport
/// no receiver can attain without inventing writes the cluster never promised;
/// requiring the committed prefix is always satisfiable, because every replica
/// in the durability quorum already holds it.
///
/// Falls back to the durable position only when the owner reported no committed
/// prefix, which is a pre-V5 binary mid-rollout. There the older, incidental
/// safety still holds: `quiesce` is what lowers that durable number to the
/// committed prefix, and it does so for exactly the owners that cannot yet
/// report the prefix directly.
fn required_prefix(inner: &Inner, info: &PartitionInfo) -> Lamport {
    let owner_progress = info
        .owner
        .and_then(|owner| inner.observations.get(&owner))
        .and_then(|observation| observation.status.progress(info.id));
    owner_progress
        .and_then(|progress| progress.committed_lamport)
        .or_else(|| owner_progress.map(|progress| progress.durable_lamport))
        .unwrap_or(Lamport::ZERO)
}

/// Whether `candidate` may be promoted to own `info` without losing a write.
///
/// Every path that fences a live owner and promotes a replica goes through
/// here, and it is one function because the cost of the paths disagreeing is
/// silent data loss. With a replication factor of three a write is
/// acknowledged once two holders have it, so one replica can legitimately be
/// behind while writes keep succeeding. Promoting that replica makes its
/// shorter log authoritative and drops writes the cluster already promised.
///
/// A candidate this leader has heard nothing about fails, and must: the whole
/// point is evidence, and absence of a report is absence of evidence. That
/// makes a rebalance decline rather than gamble, which is the right direction
/// for work nobody asked for.
fn caught_up_receiver(
    inner: &Inner,
    info: &PartitionInfo,
    candidate: NodeId,
    required: Lamport,
) -> bool {
    inner.state.is_eligible_owner(candidate)
        && inner
            .observations
            .get(&candidate)
            .and_then(|observation| observation.status.progress(info.id))
            .is_some_and(|progress| progress.durable_lamport >= required)
}

/// One ownership move the balancer wants to make.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Rebalance {
    partition: PartitionId,
    /// Recorded for the sake of tests and logs. The transfer reads the owner
    /// from the map again, because it may have changed since the plan.
    #[allow(dead_code)]
    from: NodeId,
    to: NodeId,
}

/// The leader group's decision loop and the API around it.
///
/// Cheap to clone; every clone shares one state machine and one log, so the
/// admin service, the peer service, and the sweep task can each hold one.
pub struct Controller<R: Runtime, L: ConsensusLog> {
    runtime: R,
    log: Arc<L>,
    config: ControlConfig,
    inner: Arc<Mutex<Inner>>,
}

// How many replicas beside the owner a partition should keep, given what
// was configured, what durability needs, and how large the cluster is.
//
// The ceiling exists because read capacity is bought out of write
// throughput: every holder is an invalidation a write waits on before it
// is acknowledged. Capping holders at half the placeable cluster keeps a
// write off a majority of the nodes and leaves half the cluster free of
// the obligation entirely. Holders are the replicas plus the owner, so the
// cap on replicas is one below half.
//
// It is applied here rather than in the config builder because it depends
// on the size of the cluster now, not its size when someone wrote the
// number down. A cluster that grows relaxes its own ceiling on the next
// pass, which is what makes the knob safe to set optimistically.
//
// The floor beats the ceiling. This knob may add reach and may never take
// copies away, so a cluster too small to satisfy both keeps its durability
// and exceeds the cap.
fn read_replica_want(target: usize, floor: usize, placeable: usize) -> usize {
    target.min((placeable / 2).saturating_sub(1)).max(floor)
}

impl<R: Runtime, L: ConsensusLog> Clone for Controller<R, L> {
    fn clone(&self) -> Self {
        Self {
            runtime: self.runtime.clone(),
            log: Arc::clone(&self.log),
            config: self.config.clone(),
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<R: Runtime, L: ConsensusLog> Controller<R, L> {
    #[must_use]
    pub fn new(runtime: R, log: Arc<L>, config: ControlConfig) -> Self {
        let observing_since = runtime.clock().monotonic_nanos();
        Self {
            runtime,
            log,
            config,
            inner: Arc::new(Mutex::new(Inner {
                state: ClusterState::new(),
                applied: 0,
                results: BTreeMap::new(),
                observations: BTreeMap::new(),
                returnees: BTreeMap::new(),
                prepared_splits: BTreeMap::new(),
                prepared_merges: Vec::new(),
                fenced_since: BTreeMap::new(),
                observing_since,
                last_rebalance_at: None,
                was_leader: false,
            })),
        }
    }

    #[must_use]
    pub fn config(&self) -> &ControlConfig {
        &self.config
    }

    #[must_use]
    pub fn node(&self) -> NodeId {
        self.runtime.transport().local_node()
    }

    /// Whether this node is actually in the leader group's configuration.
    ///
    /// Every combined node hosts a Raft state machine, but only the ones in
    /// the configuration are replicated to. A node outside it holds a dormant
    /// log that nothing will ever advance, so anything that waits for that log
    /// to catch up, or reads state out of it, waits forever or reads nothing.
    /// This is what tells the two apart.
    ///
    /// Answered from this node's own membership, which every combined node
    /// opens from the durable bootstrap certificate, so a node can tell it has
    /// been left out without having to ask the leader.
    pub async fn is_active_member(&self) -> bool {
        let local = self.node();
        self.log.voters().await.contains(&local) || self.log.learners().await.contains(&local)
    }

    /// Whether this controller may expose leader authority right now.
    ///
    /// This is stricter than the Raft role: it becomes true only after the
    /// inherited committed prefix is locally applied and quorum authority is
    /// reconfirmed.
    pub async fn log_is_leader(&self) -> bool {
        self.ensure_leader_ready().await.is_ok()
    }

    /// Who to redirect to when this node may not.
    pub async fn log_leader(&self) -> Option<NodeId> {
        self.log.leader().await
    }

    /// Replays the log into the state machine.
    ///
    /// Called on start and by follower sweeps. Leader-facing operations use
    /// [`Controller::ensure_leader_ready`] instead because recovery alone does
    /// not prove the node still has quorum authority after applying.
    pub async fn recover(&self) -> Result<()> {
        let committed = self.log.commit_index().await;
        self.apply_through(committed).await
    }

    /// Establishes that this controller is authoritative for a leader-facing
    /// operation.
    ///
    /// The first barrier identifies the committed prefix inherited at
    /// election. Applying it closes the stale-state window, and the second
    /// barrier proves leadership survived that apply. If more commands became
    /// committed in between, the loop catches those up before authority is
    /// exposed.
    pub async fn ensure_leader_ready(&self) -> Result<()> {
        let mut target = self.log.leader_barrier().await?;
        loop {
            self.apply_through(target).await?;
            let confirmed = self.log.leader_barrier().await?;
            if confirmed == target {
                return Ok(());
            }
            target = confirmed;
        }
    }

    /// Proposes a command and returns what applying it decided.
    ///
    /// The error from a rejected command comes back through here rather than
    /// from the propose, because a rejection is a decision the whole cluster
    /// agreed on and not a failure to reach anybody.
    pub async fn submit(&self, command: ControlCommand) -> Result<()> {
        self.ensure_leader_ready().await?;
        self.inner
            .lock()
            .await
            .state
            .ensure_command_permitted(&command)?;
        let index = self.log.propose(command).await?;
        self.apply_through(index).await?;
        self.outcome_at(index).await
    }

    async fn apply_through(&self, target: LogIndex) -> Result<()> {
        let mut inner = self.inner.lock().await;
        if inner.applied < target {
            let entries = self.log.subscribe(inner.applied).await?;
            for entry in entries {
                let outcome = inner.state.apply(&entry.command);
                inner.applied = entry.index;
                inner.results.insert(entry.index, outcome);
            }
        }
        // Results are only interesting to whoever proposed the entry. A
        // proposer whose future was dropped never collects, so old ones are
        // swept rather than kept forever.
        let floor = inner.applied.saturating_sub(1024);
        inner.results.retain(|index, _| *index > floor);
        Ok(())
    }

    async fn outcome_at(&self, target: LogIndex) -> Result<()> {
        if let Some(outcome) = self.inner.lock().await.results.remove(&target) {
            return outcome;
        }

        // A submitter can be descheduled after commit while another task
        // applies enough entries to sweep its cached result. The control log
        // is not compacted, so replay recovers the exact deterministic outcome
        // instead of treating a missing rejection as success.
        let mut state = ClusterState::new();
        for entry in self.log.subscribe(0).await? {
            let outcome = state.apply(&entry.command);
            if entry.index == target {
                return outcome;
            }
        }
        Err(Error::Internal(format!(
            "committed control outcome at index {target} is unavailable"
        )))
    }

    /// A copy of the routing table.
    pub async fn partition_map(&self) -> PartitionMap {
        self.inner.lock().await.state.map().clone()
    }

    /// The routing table, but only if it has moved since `have`.
    ///
    /// A worker polls this on a timer, and the map is the largest thing the
    /// control plane sends. Comparing versions first means a healthy cluster's
    /// steady state costs a few bytes per poll instead of the whole map.
    pub async fn partition_map_if_newer(&self, have: MapVersion) -> Option<PartitionMap> {
        let inner = self.inner.lock().await;
        if inner.state.map_version() > have {
            Some(inner.state.map().clone())
        } else {
            None
        }
    }

    pub async fn map_version(&self) -> MapVersion {
        self.inner.lock().await.state.map_version()
    }

    /// The committed control-command index known to this consensus member.
    pub async fn commit_index(&self) -> LogIndex {
        self.log.commit_index().await
    }

    /// Applies local commits and proves this state machine reached an index a
    /// leader reported as committed.
    ///
    /// This is the readiness seam rather than a Raft-specific lag check. It
    /// establishes both halves that matter after restart: the local consensus
    /// log contains the leader's decisions, and the controller has applied
    /// them before it can participate in another rollout quorum.
    pub async fn catch_up_through(&self, authority: LogIndex) -> Result<bool> {
        self.recover().await?;
        let local_commit = self.log.commit_index().await;
        let applied = self.inner.lock().await.applied;
        Ok(local_commit >= authority && applied >= authority)
    }

    /// The active cluster version.
    pub async fn cluster_version(&self) -> ClusterVersion {
        self.inner.lock().await.state.cluster_version()
    }

    /// Advances the active cluster version to the newest one every live node
    /// can speak, which is what `orbita cluster finalize-upgrade` runs.
    ///
    /// Dead nodes do not vote: they are the nodes a failover has already
    /// written off, and refusing to finalize on their account would make a
    /// dead node's last act pinning the cluster to an old version forever. A
    /// suspect node does vote, because it may only be slow, and finalizing
    /// past a node that comes back is exactly the mixed-version state the
    /// window exists to avoid.
    pub async fn finalize_upgrade(&self) -> Result<FinalizedUpgrade> {
        let (previous, target) = {
            let inner = self.inner.lock().await;
            let previous = inner.state.cluster_version();
            let live: Vec<&NodeRecord> = inner
                .state
                .nodes()
                .filter(|n| n.health != NodeHealth::Dead)
                .collect();
            if live.is_empty() {
                return Err(Error::InvalidArgument(
                    "no live node has registered, so there is nothing to check the upgrade against"
                        .into(),
                ));
            }

            // The newest version everyone can reach. Taking the minimum of
            // the maxima is what makes a half-upgraded cluster answer "not
            // yet" instead of advancing past the stragglers.
            let target = live
                .iter()
                .map(|n| n.speaks.max)
                .min()
                .unwrap_or(ClusterVersion::ZERO);

            if target <= previous {
                let laggards: Vec<String> = live
                    .iter()
                    .filter(|n| n.speaks.max <= previous)
                    .map(|n| format!("node {} speaks {}", n.id, n.speaks))
                    .collect();
                return Err(Error::InvalidArgument(format!(
                    "the cluster is already at version {previous} and no newer version is \
                     speakable by every live node: {}",
                    laggards.join(", ")
                )));
            }

            // A node whose window starts above the target has skipped past
            // it, which the one-version window does not allow.
            let skipped: Vec<String> = live
                .iter()
                .filter(|n| !n.speaks.contains(target))
                .map(|n| format!("node {} speaks {}", n.id, n.speaks))
                .collect();
            if !skipped.is_empty() {
                return Err(Error::InvalidArgument(format!(
                    "version {target} is not speakable by every live node ({}); upgrades move \
                     one version at a time, so roll those nodes to a binary that speaks {target} \
                     first",
                    skipped.join(", ")
                )));
            }
            (previous, target)
        };

        self.submit(ControlCommand::SetClusterVersion {
            version: target,
            expect: previous,
        })
        .await?;
        tracing::info!(%previous, active = %target, "finalized the cluster upgrade");
        Ok(FinalizedUpgrade {
            previous,
            active: target,
        })
    }

    /// Every node the cluster knows, and where peers reach it.
    ///
    /// Addresses are what each node reported about itself, so a node that
    /// moves corrects this on its next heartbeat without an operator having to
    /// notice. Workers poll it to fill in their peer directory, since the
    /// partition map names owners by id and nothing else says how to dial one.
    pub async fn node_addresses(&self) -> Vec<(NodeId, String)> {
        self.inner
            .lock()
            .await
            .state
            .nodes()
            .filter(|record| !record.address.is_empty())
            .map(|record| (record.id, record.address.clone()))
            .collect()
    }

    /// A snapshot of the agreed state, for callers that need more than the
    /// map.
    pub async fn snapshot(&self) -> ClusterState {
        self.inner.lock().await.state.clone()
    }

    /// Records a node's self-report and admits it to the cluster if it is new.
    ///
    /// Returns the current map version so the reporting node learns in the
    /// same round trip whether the map it is routing on is stale.
    pub async fn record_status(
        &self,
        node: NodeId,
        status: NodeStatus,
    ) -> Result<RegistrationOutcome> {
        let now = self.runtime.clock().monotonic_nanos();
        // ADR 0014: a member id reporting under a different durable identity
        // is a returned incarnation of a lost disk, not the member restarting.
        // Its heartbeats must not refresh the old incarnation's observation or
        // revive its committed health, or the silence that proves the seat is
        // permanently lost would never accumulate and repair would never run.
        // The report is remembered separately so repair can reseat the node.
        // Empty identities stay on the ordinary path: membership written
        // before identity records cannot tell incarnations apart, and
        // guessing here would strand a merely-restarted node.
        let member_identity = self.log.member_identity(node).await;
        if let Some(member_identity) = member_identity {
            if !member_identity.is_empty()
                && !status.node_identity.is_empty()
                && member_identity != status.node_identity
            {
                let mut inner = self.inner.lock().await;
                let old_reporting = inner.observations.get(&node).is_some_and(|observation| {
                    now.saturating_sub(observation.heard_at_nanos)
                        < self.config.suspect_after.as_nanos() as u64
                });
                if old_reporting {
                    // Both incarnations are alive, which no failure produces.
                    // Two processes are sharing one node id, and any automated
                    // choice between them is a guess. Keep the returnee report
                    // for diagnosis, change nothing, and say so.
                    tracing::warn!(
                        %node,
                        member_identity,
                        reported_identity = status.node_identity,
                        "two incarnations of one node id are reporting; refusing to arbitrate"
                    );
                }
                inner.returnees.insert(
                    node,
                    Observation {
                        heard_at_nanos: now,
                        status,
                    },
                );
                let version = inner.state.map_version();
                return Ok(RegistrationOutcome::Accepted(version));
            }
            // The identities agree again, so any returnee record is stale:
            // either the repair completed and membership now names the new
            // incarnation, or the original member is back after all.
            self.inner.lock().await.returnees.remove(&node);
        }
        let (known, mut refusal, lifecycle_enabled) = {
            let mut inner = self.inner.lock().await;
            let known = inner.state.node(node).cloned();
            if let Some(record) = &known {
                if record.role != status.role {
                    return Err(Error::InvalidArgument(format!(
                        "node {node} is registered as {:?} and cannot report as {:?}; node ids \
                         are stable across roles",
                        record.role, status.role
                    )));
                }
            }
            let refusal = inner.state.compatibility_refusal(status.speaks);
            let lifecycle_enabled = inner.state.lifecycle_enabled();
            if known.is_some() || refusal.is_none() {
                inner.observations.insert(
                    node,
                    Observation {
                        heard_at_nanos: now,
                        status: status.clone(),
                    },
                );
            }
            (known, refusal, lifecycle_enabled)
        };
        let known_member = known.is_some();
        if !known_member {
            if let Some(refusal) = refusal {
                return Ok(RegistrationOutcome::Incompatible(refusal));
            }
        }
        // A changed speakable range is a re-registration too: it is what a
        // rolling update looks like from here, and finalize-upgrade decides
        // from the stored range.
        let needs_registration = known.as_ref().is_none_or(|record| {
            record.address != status.address
                || record.role != status.role
                || record.speaks != status.speaks
                || (lifecycle_enabled
                    && (record.ready != status.ready || record.draining != status.draining))
        });
        let needs_revival = known
            .as_ref()
            .is_some_and(|record| record.health != NodeHealth::Healthy);

        if needs_registration && refusal.is_none() {
            if let Err(error) = self
                .submit(ControlCommand::RegisterNode {
                    node,
                    role: status.role,
                    address: status.address.clone(),
                    speaks: status.speaks,
                    // Both claims are withheld until the active version
                    // carries them, and it is the encoding that requires it: a
                    // `RegisterNode` with either bool set encodes under a tag
                    // the previous binary truncates its log at. So the state
                    // machine cannot learn a node's readiness during the
                    // upgrade window, and `lifecycle_enabled` is what stops it
                    // reading that silence as a node that said no.
                    ready: lifecycle_enabled && status.ready,
                    draining: lifecycle_enabled && status.draining,
                })
                .await
            {
                // Finalization can race the proposal. The state machine is the
                // authority, and this read turns its refusal back into the
                // same structured answer as the fast path above.
                let mut inner = self.inner.lock().await;
                refusal = inner.state.compatibility_refusal(status.speaks);
                if refusal.is_none() {
                    return Err(error);
                }
                if !known_member {
                    inner.observations.remove(&node);
                }
            }
        }
        if needs_revival {
            // A node that came back has to be marked healthy through the log,
            // or it stays out of every placement decision forever.
            self.submit(ControlCommand::SetHealth {
                node,
                health: NodeHealth::Healthy,
            })
            .await?;
        }

        match refusal {
            Some(refusal) => Ok(RegistrationOutcome::Incompatible(refusal)),
            None => Ok(RegistrationOutcome::Accepted(self.map_version().await)),
        }
    }

    /// Creates the first keyspace of a fresh cluster, with its single
    /// unbounded partition and an owner.
    ///
    /// Returns false when the cluster already has state, so this is safe to
    /// call on every start. That matters more than its size suggests: it is
    /// the path a single-command development cluster takes, and it is the one
    /// place where "no state at all" has to turn into "a valid map".
    pub async fn bootstrap(&self, spec: &BootstrapSpec) -> Result<bool> {
        self.recover().await?;
        if !self.snapshot().await.is_fresh() {
            return Ok(false);
        }

        // A fresh cluster's active version is the bootstrapping binary's own.
        // Set first, so there is no committed state in which the cluster has
        // nodes and a keyspace but no version to speak. The nodes named in
        // the spec are admitted with this binary's window; any that run a
        // different binary correct the record on their first heartbeat.
        self.submit(ControlCommand::SetClusterVersion {
            version: binary_speaks().max,
            expect: ClusterVersion::ZERO,
        })
        .await?;

        for (node, address) in &spec.leaders {
            self.submit(ControlCommand::RegisterNode {
                node: *node,
                role: NodeRole::Leader,
                address: address.clone(),
                speaks: binary_speaks(),
                ready: true,
                draining: false,
            })
            .await?;
        }
        for (node, address) in &spec.workers {
            self.submit(ControlCommand::RegisterNode {
                node: *node,
                role: NodeRole::Worker,
                address: address.clone(),
                speaks: binary_speaks(),
                // See `BootstrapSpec::workers`: not-ready here means the
                // keyspace created below is born with no owner.
                ready: true,
                draining: false,
            })
            .await?;
        }

        self.create_keyspace(&spec.keyspace, spec.config.clone())
            .await?;
        Ok(true)
    }

    pub async fn create_keyspace(&self, name: &str, config: KeyspaceConfig) -> Result<Keyspace> {
        let command = {
            let inner = self.inner.lock().await;
            let candidates = inner.state.placement_candidates();
            let (owner, replicas) = split_placement(&candidates, self.config.replication_factor);
            ControlCommand::CreateKeyspace {
                id: inner.state.next_keyspace_id(),
                name: name.to_string(),
                config,
                created_at_millis: self.runtime.clock().now_millis(),
                first_partition: inner.state.next_partition_id(),
                owner,
                replicas,
            }
        };
        self.submit(command).await?;
        self.snapshot()
            .await
            .keyspace_by_name(name)
            .cloned()
            .ok_or(Error::KeyspaceNotFound)
    }

    pub async fn update_keyspace(&self, name: &str, config: KeyspaceConfig) -> Result<Keyspace> {
        let id = self
            .snapshot()
            .await
            .keyspace_by_name(name)
            .ok_or(Error::KeyspaceNotFound)?
            .id;
        self.submit(ControlCommand::UpdateKeyspace { id, config })
            .await?;
        self.snapshot()
            .await
            .keyspace_by_name(name)
            .cloned()
            .ok_or(Error::KeyspaceNotFound)
    }

    pub async fn delete_keyspace(&self, name: &str) -> Result<()> {
        let id = self
            .snapshot()
            .await
            .keyspace_by_name(name)
            .ok_or(Error::KeyspaceNotFound)?
            .id;
        self.submit(ControlCommand::DeleteKeyspace { id }).await
    }

    pub async fn list_keyspaces(&self) -> Vec<Keyspace> {
        self.snapshot().await.keyspaces().cloned().collect()
    }

    /// Issues a credential and returns its id and its secret.
    ///
    /// The secret is shown once and never stored, only its hash. It is drawn
    /// from the operating system rather than from `orbita_runtime::Rng`, whose
    /// documentation says in as many words that it must never be used for
    /// credentials. That is the one place in this crate that does not go
    /// through the runtime seam, and it is safe to except because nothing
    /// about a simulated run depends on which secret was issued.
    ///
    /// The credential id is derived from `op_id`, the caller's stable name for
    /// this invocation, rather than freshly randomised on each apply. That is
    /// what makes a forwarded creation retry-safe: the peer transport resends
    /// after an ambiguous connection loss even when the leader already applied
    /// the first copy, and a fresh random id per attempt would let that resend
    /// commit a second credential and orphan the first one's one-time secret.
    /// A derived id makes the resend land on the same id, where the replicated
    /// duplicate check refuses it — so the operation commits exactly once even
    /// across a leader change, because the check reads replicated state rather
    /// than a leader's memory. The refusal comes back as `AlreadyExists`; the
    /// secret cannot be shown a second time, so the honest answer to a replay
    /// is to say so rather than to invent a new credential.
    pub async fn create_credential(
        &self,
        keyspaces: Vec<String>,
        permissions: Vec<Permission>,
        description: String,
        expires_at_millis: Option<u64>,
        op_id: u128,
    ) -> Result<(String, String)> {
        let id = format!("cred-{op_id:032x}");
        let secret = random_hex(32)?;
        let credential = Credential {
            id: id.clone(),
            secret_hash: hash_secret(&secret),
            keyspaces,
            permissions,
            description,
            created_at_millis: self.runtime.clock().now_millis(),
            expires_at_millis,
        };
        self.submit(ControlCommand::CreateCredential {
            credential: Box::new(credential),
        })
        .await?;
        Ok((id, secret))
    }

    pub async fn revoke_credential(&self, id: &str) -> Result<()> {
        self.submit(ControlCommand::RevokeCredential { id: id.to_string() })
            .await
    }

    /// The current wall-clock time in Unix milliseconds, from the runtime
    /// rather than the host, so credential expiry is checked against the same
    /// clock a simulated run drives.
    #[must_use]
    pub fn now_millis(&self) -> u64 {
        self.runtime.clock().now_millis()
    }

    /// A copy of every live credential, for a worker to enforce against.
    ///
    /// This is what closes the gap the [`Controller::authenticate`] comment
    /// describes: a worker cannot call `authenticate` per request without
    /// putting the control plane on the data path, so it pulls this snapshot on
    /// the same timer as its map and runs the same rule locally through
    /// [`crate::CredentialSnapshot`]. It carries the secret hashes, never the
    /// secrets, so it is no more sensitive than the replicated log already is.
    pub async fn credential_snapshot(&self) -> crate::CredentialSnapshot {
        let inner = self.inner.lock().await;
        crate::CredentialSnapshot::new(inner.state.credentials().cloned().collect())
    }

    /// Checks a credential against a keyspace and an operation.
    ///
    /// This is the leader group's copy of the check. The worker enforces the
    /// same rule against its cached view, because the alternative is a control
    /// plane round trip on every request, which would put the control plane on
    /// the data path and make a control plane outage a data plane outage.
    pub async fn authenticate(
        &self,
        id: &str,
        secret: &str,
        keyspace: &str,
        permission: Permission,
    ) -> Result<()> {
        let now = self.runtime.clock().now_millis();
        let inner = self.inner.lock().await;
        let credential = inner.state.credential(id).ok_or(Error::Unauthenticated)?;
        if credential.secret_hash != hash_secret(secret) {
            return Err(Error::Unauthenticated);
        }
        if !credential.allows(keyspace, permission, now) {
            return Err(Error::PermissionDenied);
        }
        Ok(())
    }

    /// Splits a partition at `at`, driving the worker-prepared protocol to
    /// completion.
    ///
    /// It opens the split, then waits — through the sweep, which proposes the
    /// preparation acknowledgements and the completion — until the parent has
    /// retired and both children are in the map. The parent never retires until
    /// every holder has *durably* prepared child storage and reported it, which
    /// is the prepare-before-retire ordering ADR 0009 and PR #53 require. A
    /// caller with no `at` gets a boundary derived from the range, which is a
    /// poor split point kept only so a keyless request does something.
    pub async fn split_partition(
        &self,
        partition: PartitionId,
        at: Option<Bytes>,
    ) -> Result<(PartitionId, PartitionId)> {
        let (lower, upper, begin) = {
            let inner = self.inner.lock().await;
            let info = inner
                .state
                .map()
                .partition(partition)
                .ok_or_else(|| Error::InvalidArgument(format!("no partition {partition}")))?
                .clone();
            let at = match at {
                Some(at) => at,
                None => suggested_split_key(&info).ok_or_else(|| {
                    Error::InvalidArgument(
                        "no split key was given and none could be derived from the range".into(),
                    )
                })?,
            };
            let lower = inner.state.next_partition_id();
            let upper = lower.next();
            (
                lower,
                upper,
                ControlCommand::BeginSplit {
                    parent: partition,
                    at,
                    lower,
                    upper,
                    expect_epoch: info.epoch,
                },
            )
        };
        self.submit(begin).await?;

        let deadline = self.runtime.clock().monotonic_nanos()
            + (self.config.convergence_bound() + Duration::from_secs(5)).as_nanos() as u64;
        loop {
            self.advance_pending_splits().await?;
            {
                let inner = self.inner.lock().await;
                let map = inner.state.map();
                if map.partition(lower).is_some() && map.partition(upper).is_some() {
                    return Ok((lower, upper));
                }
                if !inner.state.is_splitting(partition) {
                    return Err(Error::Unavailable(format!(
                        "the split of partition {partition} was abandoned before it completed; \
                         re-read the map and retry"
                    )));
                }
            }
            if self.runtime.clock().monotonic_nanos() >= deadline {
                return Err(Error::Unavailable(format!(
                    "the split of partition {partition} did not complete: not every holder \
                     durably prepared child storage in time"
                )));
            }
            self.runtime.clock().sleep(self.config.sweep_interval).await;
        }
    }

    /// Every active split whose parent this node holds, including work it has
    /// already prepared.
    ///
    /// A worker polls this, prepares durable child storage for each, and calls
    /// [`Controller::record_split_prepared`]. The boolean distinguishes work
    /// remaining from lifecycle: an acknowledged intent stays visible until a
    /// replicated CompleteSplit or AbortSplit removes it, so workers keep the
    /// parent's gates closed without needlessly preparing it again. The map
    /// version is captured under the same lock so a worker can reject a
    /// lifecycle observation made before or after its routing snapshot.
    pub async fn active_split_intents_for(
        &self,
        node: NodeId,
    ) -> (MapVersion, Vec<(crate::SplitIntent, bool)>) {
        let mut inner = self.inner.lock().await;
        let map_version = inner.state.map_version();
        let state_intents = inner.state.split_intents();
        let active: BTreeSet<_> = state_intents
            .iter()
            .map(|intent| (intent.parent, intent.lower, intent.upper))
            .collect();
        inner
            .prepared_splits
            .retain(|generation, _| active.contains(generation));
        let intents = state_intents
            .into_iter()
            .filter(|intent| intent.required.contains(&node))
            .map(|intent| {
                let prepared = intent.prepared.contains(&node)
                    || inner
                        .prepared_splits
                        .get(&(intent.parent, intent.lower, intent.upper))
                        .is_some_and(|set| set.contains(&node));
                (intent, prepared)
            })
            .collect();
        (map_version, intents)
    }

    /// Records that `node` has durably prepared its child storage for the split
    /// of `parent`.
    ///
    /// This is the real acknowledgement the completion waits on: a worker calls
    /// it only after both child manifests are on the object store, so the
    /// controller's `MarkSplitPrepared` reflects storage that exists, not a map
    /// version a node happened to observe.
    pub async fn record_split_prepared(
        &self,
        node: NodeId,
        parent: PartitionId,
        lower: PartitionId,
        upper: PartitionId,
    ) -> bool {
        let mut inner = self.inner.lock().await;
        let active = inner.state.split_intents().into_iter().any(|intent| {
            intent.parent == parent
                && intent.lower == lower
                && intent.upper == upper
                && intent.required.contains(&node)
        });
        if !active {
            return false;
        }
        inner
            .prepared_splits
            .entry((parent, lower, upper))
            .or_default()
            .insert(node);
        true
    }

    /// Turns durable preparation acknowledgements into the replicated entries a
    /// split needs: `MarkSplitPrepared` for each acknowledged holder, then
    /// `CompleteSplit` once the state machine records every one.
    ///
    /// Tolerant of racing itself and a fence landing mid-flight: every refusal a
    /// lost race produces is swallowed, because the next pass re-derives the
    /// same intent from committed state and in-memory acks.
    async fn advance_pending_splits(&self) -> Result<()> {
        let (prepares, completes) = {
            let inner = self.inner.lock().await;
            let mut prepares = Vec::new();
            let mut completes = Vec::new();
            for intent in inner.state.split_intents() {
                let generation = (intent.parent, intent.lower, intent.upper);
                let acked = inner.prepared_splits.get(&generation);
                for node in &intent.required {
                    if intent.prepared.contains(node) {
                        continue;
                    }
                    if acked.is_some_and(|set| set.contains(node)) {
                        prepares.push(ControlCommand::MarkSplitPrepared {
                            parent: intent.parent,
                            node: *node,
                            expect_epoch: intent.epoch,
                        });
                    }
                }
                let all_prepared = intent
                    .required
                    .iter()
                    .all(|node| intent.prepared.contains(node));
                if all_prepared {
                    completes.push(ControlCommand::CompleteSplit {
                        parent: intent.parent,
                        expect_epoch: intent.epoch,
                    });
                }
            }
            (prepares, completes)
        };

        for command in prepares {
            match self.submit(command).await {
                Ok(()) | Err(Error::StaleEpoch { .. } | Error::InvalidArgument(_)) => {}
                Err(e) => return Err(e),
            }
        }
        for command in completes {
            let parent = match &command {
                ControlCommand::CompleteSplit { parent, .. } => *parent,
                _ => continue,
            };
            match self.submit(command).await {
                Ok(()) => {
                    tracing::info!(%parent, "retired a split parent; its children own its range");
                    crate::metrics::record_split(crate::metrics::Outcome::Committed);
                    self.inner
                        .lock()
                        .await
                        .prepared_splits
                        .retain(|(held_parent, _, _), _| *held_parent != parent);
                }
                Err(
                    Error::StaleEpoch { .. } | Error::InvalidArgument(_) | Error::Unavailable(_),
                ) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Merges two compatible adjacent partitions through durable worker
    /// preparation, returning the new partition id after the atomic map swap.
    pub async fn merge_partitions(
        &self,
        lower: PartitionId,
        upper: PartitionId,
    ) -> Result<PartitionId> {
        self.ensure_leader_ready().await?;
        self.colocate_merge_parents(lower, upper).await?;
        let generation = {
            let inner = self.inner.lock().await;
            // Check the protocol before validating the requested ids so an
            // unavailable feature has one honest public refusal and cannot be
            // probed into doing any merge work during the rollback window.
            inner.state.ensure_merge_permitted()?;
            let lower_info = inner
                .state
                .map()
                .partition(lower)
                .ok_or_else(|| Error::InvalidArgument(format!("no partition {lower}")))?;
            let upper_info = inner
                .state
                .map()
                .partition(upper)
                .ok_or_else(|| Error::InvalidArgument(format!("no partition {upper}")))?;
            let range = orbita_core::KeyRange::merge(&lower_info.range, &upper_info.range)
                .ok_or_else(|| {
                    Error::InvalidArgument(
                        "merge partitions must be adjacent and in lower/upper order".into(),
                    )
                })?;
            crate::MergeGeneration {
                lower,
                upper,
                lower_epoch: lower_info.epoch,
                upper_epoch: upper_info.epoch,
                merged: inner.state.next_partition_id(),
                boundary: Bytes::copy_from_slice(upper_info.range.start()),
                range,
            }
        };
        self.submit(ControlCommand::BeginMerge {
            generation: generation.clone(),
        })
        .await?;

        let deadline = self.runtime.clock().monotonic_nanos()
            + (self.config.convergence_bound() + Duration::from_secs(5)).as_nanos() as u64;
        loop {
            self.advance_pending_merges().await?;
            {
                let inner = self.inner.lock().await;
                if inner.state.map().partition(generation.merged).is_some() {
                    return Ok(generation.merged);
                }
                if !inner.state.is_merging(lower) && !inner.state.is_merging(upper) {
                    return Err(Error::Unavailable(
                        "the merge was abandoned before completion; re-read the map and retry"
                            .into(),
                    ));
                }
            }
            if self.runtime.clock().monotonic_nanos() >= deadline {
                return Err(Error::Unavailable(
                    "the merge did not complete: not every holder durably prepared storage in time"
                        .into(),
                ));
            }
            self.runtime.clock().sleep(self.config.sweep_interval).await;
        }
    }

    /// Moves one merge parent onto the other's owner, if they differ.
    ///
    /// A merge needs both parents on one holder set, because the cut it makes
    /// is a single in-process operation over two local WALs. Splits now spread
    /// their children, so the two halves of one split — the pair an operator is
    /// most likely to want merged back — normally have different owners. ADR
    /// 0010 anticipated exactly this and reserved the answer: "a later protocol
    /// can move one parent first if cross-owner merge is needed". This is that
    /// move, done here rather than left to the operator.
    ///
    /// It costs nothing in data. Every holder already has the partition built
    /// from the shared object store, and a promoted replica reopens the same
    /// WAL directory it was already replicating into, so the transfer is a
    /// lease change rather than a copy. What it does cost is the deposed
    /// owner's lease drain, which is the same wait a failover pays and is
    /// bounded by `lease_drain`.
    ///
    /// The merge is validated as far as possible before anything moves, so a
    /// request that could never merge does not leave a partition relocated
    /// behind it. A failure after the transfer is still possible — the map can
    /// change underneath — and leaves the pair co-located but unmerged, which
    /// the operator can retry.
    async fn colocate_merge_parents(&self, lower: PartitionId, upper: PartitionId) -> Result<()> {
        let (mover, target) = {
            let inner = self.inner.lock().await;
            inner.state.ensure_merge_permitted()?;
            let lower_info = inner
                .state
                .map()
                .partition(lower)
                .ok_or_else(|| Error::InvalidArgument(format!("no partition {lower}")))?;
            let upper_info = inner
                .state
                .map()
                .partition(upper)
                .ok_or_else(|| Error::InvalidArgument(format!("no partition {upper}")))?;
            if orbita_core::KeyRange::merge(&lower_info.range, &upper_info.range).is_none() {
                return Err(Error::InvalidArgument(
                    "merge partitions must be adjacent and in lower/upper order".into(),
                ));
            }
            let (Some(lower_owner), Some(upper_owner)) = (lower_info.owner, upper_info.owner)
            else {
                return Err(Error::Unavailable(
                    "both merge parents must be owned before they can be merged".into(),
                ));
            };
            if lower_owner == upper_owner {
                return Ok(());
            }
            // Refused here rather than after the transfer, so a pair that was
            // never going to merge does not pay a relocation to find out.
            for id in [lower, upper] {
                if inner.state.is_splitting(id) || inner.state.is_merging(id) {
                    return Err(Error::InvalidArgument(format!(
                        "a split or merge is already active on partition {id}"
                    )));
                }
            }

            // `BeginMerge` requires one complete holder set, not merely a
            // shared owner, so a pair whose holders only partly overlap can
            // never merge however ownership is arranged. Checking before
            // moving matters because the move is not free: relocating first
            // and discovering the mismatch afterwards costs the parent a
            // lease-drain outage and leaves it relocated for a merge that was
            // always going to be refused.
            let holders = |info: &PartitionInfo| -> BTreeSet<NodeId> {
                info.owner
                    .into_iter()
                    .chain(info.replicas.clone())
                    .collect()
            };
            if holders(lower_info) != holders(upper_info) {
                return Err(Error::InvalidArgument(format!(
                    "partitions {lower} and {upper} are held by different sets of nodes, \
                     so no ownership move can make them mergeable"
                )));
            }

            // Whichever parent can move is the one that moves. A holder can
            // only be promoted where it already replicates, so the target has
            // to be a replica of the partition it is taking over, and it has
            // to be caught up: this fences a live owner, and promoting a
            // replica short of the committed prefix loses acknowledged writes
            // whatever the reason for the promotion.
            if upper_info.replicas.contains(&lower_owner)
                && caught_up_receiver(
                    &inner,
                    upper_info,
                    lower_owner,
                    required_prefix(&inner, upper_info),
                )
            {
                (upper, lower_owner)
            } else if lower_info.replicas.contains(&upper_owner)
                && caught_up_receiver(
                    &inner,
                    lower_info,
                    upper_owner,
                    required_prefix(&inner, lower_info),
                )
            {
                (lower, upper_owner)
            } else {
                return Err(Error::Unavailable(format!(
                    "neither partition {lower} nor {upper} has an owner caught up enough \
                     to take the other; retry once replication has settled"
                )));
            }
        };
        self.transfer_ownership(mover, target).await
    }

    /// Active merges held by one worker, observed with the routing map version.
    pub async fn active_merge_intents_for(
        &self,
        node: NodeId,
    ) -> (MapVersion, Vec<(crate::MergeIntent, bool)>) {
        let mut inner = self.inner.lock().await;
        let map_version = inner.state.map_version();
        let active = inner.state.merge_intents();
        inner
            .prepared_merges
            .retain(|(generation, _)| active.iter().any(|intent| intent.generation == *generation));
        let intents = active
            .into_iter()
            .filter(|intent| intent.required.contains(&node))
            .map(|intent| {
                let prepared = intent.prepared.contains(&node)
                    || inner
                        .prepared_merges
                        .iter()
                        .find(|(generation, _)| generation == &intent.generation)
                        .is_some_and(|(_, nodes)| nodes.contains(&node));
                (intent, prepared)
            })
            .collect();
        (map_version, intents)
    }

    pub async fn record_merge_prepared(
        &self,
        node: NodeId,
        generation: crate::MergeGeneration,
    ) -> bool {
        let mut inner = self.inner.lock().await;
        let active = inner
            .state
            .merge_intents()
            .into_iter()
            .any(|intent| intent.generation == generation && intent.required.contains(&node));
        if !active {
            return false;
        }
        match inner
            .prepared_merges
            .iter_mut()
            .find(|(held, _)| held == &generation)
        {
            Some((_, nodes)) => {
                nodes.insert(node);
            }
            None => inner
                .prepared_merges
                .push((generation, BTreeSet::from([node]))),
        }
        true
    }

    async fn advance_pending_merges(&self) -> Result<()> {
        let (prepares, completes) = {
            let inner = self.inner.lock().await;
            let mut prepares = Vec::new();
            let mut completes = Vec::new();
            for intent in inner.state.merge_intents() {
                let acked = inner
                    .prepared_merges
                    .iter()
                    .find(|(generation, _)| generation == &intent.generation)
                    .map(|(_, nodes)| nodes);
                for node in &intent.required {
                    if !intent.prepared.contains(node)
                        && acked.is_some_and(|set| set.contains(node))
                    {
                        prepares.push(ControlCommand::MarkMergePrepared {
                            generation: intent.generation.clone(),
                            node: *node,
                        });
                    }
                }
                if intent
                    .required
                    .iter()
                    .all(|node| intent.prepared.contains(node))
                {
                    completes.push(ControlCommand::CompleteMerge {
                        generation: intent.generation,
                    });
                }
            }
            (prepares, completes)
        };
        for command in prepares {
            match self.submit(command).await {
                Ok(()) | Err(Error::StaleEpoch { .. } | Error::InvalidArgument(_)) => {}
                Err(error) => return Err(error),
            }
        }
        for command in completes {
            let generation = match &command {
                ControlCommand::CompleteMerge { generation } => generation.clone(),
                _ => continue,
            };
            match self.submit(command).await {
                Ok(()) => {
                    crate::metrics::record_merge(crate::metrics::Outcome::Committed);
                    self.inner
                        .lock()
                        .await
                        .prepared_merges
                        .retain(|(held, _)| held != &generation);
                }
                Err(
                    Error::StaleEpoch { .. } | Error::InvalidArgument(_) | Error::Unavailable(_),
                ) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// Hands a partition to one of its replicas, deliberately.
    ///
    /// This is the same two-step sequence as a failover, for the same reason:
    /// the deposed owner has read leases out, and they have to be gone before
    /// the new owner accepts a write. A planned transfer could shorten the
    /// wait by having the old owner revoke its leases explicitly, and that is
    /// worth doing once the worker side exists. Until then it pays the same
    /// drain a crash does.
    pub async fn transfer_ownership(&self, partition: PartitionId, to: NodeId) -> Result<()> {
        let info = {
            let inner = self.inner.lock().await;
            inner
                .state
                .map()
                .partition(partition)
                .ok_or_else(|| Error::InvalidArgument(format!("no partition {partition}")))?
                .clone()
        };
        if !info.replicas.contains(&to) {
            return Err(Error::InvalidArgument(format!(
                "node {to} does not hold partition {partition}, so it would have no data to serve"
            )));
        }
        let deposed = info.owner;
        self.submit(ControlCommand::FencePartition {
            partition,
            expect_epoch: info.epoch,
        })
        .await?;

        self.runtime.clock().sleep(self.config.lease_drain()).await;
        let fenced_epoch = self.epoch_of(partition).await?;
        let mut replicas: Vec<NodeId> =
            info.replicas.iter().copied().filter(|r| *r != to).collect();
        if let Some(old) = deposed {
            if old != to && !replicas.contains(&old) {
                replicas.push(old);
            }
        }
        // Canonically ordered. Nothing reads a replica list positionally, but
        // `complete_merge` compares two of them with `Vec` equality, so a pair
        // that differs only in the order a transfer happened to leave behind
        // would be refused a merge it should be allowed.
        replicas.sort_unstable();
        self.submit(ControlCommand::AssignOwner {
            partition,
            owner: to,
            replicas,
            expect_epoch: fenced_epoch,
        })
        .await
    }

    /// Transfers every partition owned by a draining worker to a ready,
    /// caught-up replica. It never expands a replica set or rebalances ranges.
    ///
    /// "Caught up" is measured against the owner's committed prefix, not its
    /// durable position: the receiver must hold every write a durability
    /// quorum acknowledged, which is the no-lost-write floor a handoff has to
    /// keep, and nothing more. The owner's durable tail may run ahead of that
    /// with entries it could not replicate, and demanding a receiver reach
    /// them would be demanding the impossible — the WAL never ships past the
    /// committed prefix. #79's `quiesce` already truncates the draining owner
    /// to that prefix, which used to make the comparison safe by coincidence;
    /// reading the prefix here makes it safe by construction instead.
    pub async fn drain_node(&self, node: NodeId) -> Result<bool> {
        let commands = {
            let inner = self.inner.lock().await;
            if !inner.state.lifecycle_enabled() {
                return Err(Error::InvalidArgument(
                    "planned handoff requires finalizing the active cluster version".into(),
                ));
            }
            let record = inner
                .state
                .node(node)
                .ok_or_else(|| Error::InvalidArgument(format!("unknown node {node}")))?;
            if record.role != NodeRole::Worker {
                return Err(Error::InvalidArgument(format!(
                    "node {node} is not a worker and cannot hand off partitions"
                )));
            }
            if !record.draining {
                return Err(Error::InvalidArgument(format!(
                    "node {node} has not entered draining state"
                )));
            }

            let mut commands = Vec::new();
            for info in inner
                .state
                .map()
                .partitions()
                .filter(|partition| partition.owner == Some(node))
            {
                // #79's `quiesce` truncates the draining owner's tail back to
                // the committed prefix before a handoff, so here the two
                // numbers coincide at the moment of comparison and the drain
                // would be safe by that coincidence. Comparing the committed
                // prefix directly, in [`required_prefix`], makes it safe by
                // construction instead, whether or not quiesce has run — the
                // #87 invariant that the receiver test names a ceiling a
                // catch-up can actually reach.
                let required = required_prefix(&inner, info);
                let target = info
                    .replicas
                    .iter()
                    .copied()
                    .find(|candidate| caught_up_receiver(&inner, info, *candidate, required));
                let Some(target) = target else {
                    return Err(Error::Unavailable(format!(
                        "partition {} has no eligible ready replica caught up through {required}",
                        info.id
                    )));
                };
                commands.push(ControlCommand::TransferOwnership {
                    partition: info.id,
                    from: node,
                    to: target,
                    replicas: info
                        .replicas
                        .iter()
                        .copied()
                        .filter(|replica| *replica != target)
                        .collect(),
                    expect_epoch: info.epoch,
                });
            }
            commands
        };

        if commands.is_empty() {
            let inner = self.inner.lock().await;
            let receivers_ready =
                inner
                    .state
                    .handoffs_from(node)
                    .into_iter()
                    .flatten()
                    .all(|(_, checkpoint)| {
                        inner
                            .observations
                            .get(&checkpoint.receiver)
                            .is_some_and(|observation| {
                                observation.status.ready
                                    && observation.status.map_version >= checkpoint.map_version
                            })
                    });
            if !receivers_ready {
                return Ok(false);
            }
            return Ok(true);
        }
        for command in commands {
            self.submit(command).await?;
        }
        Ok(false)
    }

    async fn epoch_of(&self, partition: PartitionId) -> Result<Epoch> {
        self.inner
            .lock()
            .await
            .state
            .map()
            .partition(partition)
            .map(|p| p.epoch)
            .ok_or_else(|| Error::InvalidArgument(format!("no partition {partition}")))
    }

    /// Everything `DescribeCluster` answers with.
    pub async fn view(&self) -> ClusterView {
        let control_leader = self.log.leader().await;
        let now = self.runtime.clock().monotonic_nanos();
        let inner = self.inner.lock().await;

        let nodes = inner
            .state
            .nodes()
            .map(|record| NodeView {
                record: record.clone(),
                silent_for_millis: inner
                    .observations
                    .get(&record.id)
                    .map(|obs| (now.saturating_sub(obs.heard_at_nanos)) / 1_000_000),
                reported_map_version: inner
                    .observations
                    .get(&record.id)
                    .map(|obs| obs.status.map_version),
                is_control_leader: control_leader == Some(record.id),
                // `sum` over an iterator of Options is None if any element
                // is, which is exactly the wanted arithmetic: one silent
                // partition makes the node's total unknown rather than low.
                // A node with no partitions sums to Some(0), which is a real
                // answer and what every leader-group member looks like.
                index_memory_bytes: inner
                    .observations
                    .get(&record.id)
                    .and_then(|obs| obs.status.partitions.iter().map(|p| p.index_bytes).sum()),
            })
            .collect();

        let partitions = inner
            .state
            .map()
            .partitions()
            .map(|info| {
                let owner_progress = info
                    .owner
                    .and_then(|o| inner.observations.get(&o))
                    .and_then(|obs| obs.status.progress(info.id));
                PartitionView {
                    info: info.clone(),
                    phase: inner
                        .state
                        .phase(info.id)
                        .unwrap_or(PartitionPhase::Unowned),
                    // All three follow the owner's report, so all three are
                    // absent together when there is no owner to have made
                    // one. #53 made that state common rather than fleeting:
                    // a fenced partition stays unowned until every surviving
                    // replica has reported past the fence.
                    committed_lamport: owner_progress.and_then(|p| p.committed_lamport),
                    size_bytes: owner_progress.map(|p| p.size_bytes),
                    index_bytes: owner_progress.and_then(|p| p.index_bytes),
                    replica_progress: info
                        .replicas
                        .iter()
                        .map(|r| {
                            let progress = inner
                                .observations
                                .get(r)
                                .and_then(|obs| obs.status.progress(info.id));
                            ReplicaProgressView {
                                node: *r,
                                applied_lamport: progress.map(|p| p.applied_lamport),
                                durable_lamport: progress.map(|p| p.durable_lamport),
                            }
                        })
                        .collect(),
                }
            })
            .collect();

        ClusterView {
            nodes,
            partitions,
            cluster_version: inner.state.cluster_version(),
        }
    }

    /// One pass of the decision loop.
    ///
    /// Exposed rather than hidden inside [`Controller::run`] so that a
    /// simulation can drive it a step at a time and assert what changed
    /// between steps, which is how the failover ordering is actually verified.
    pub async fn tick(&self) -> Result<()> {
        // Followers must apply decisions while they are followers. Otherwise a
        // newly elected leader starts its first sweep from the state it held
        // when it last proposed a command, which can predate an ownership
        // fence by an arbitrary amount.
        self.recover().await?;
        let is_leader = self.log.is_leader().await;
        let now = self.runtime.clock().monotonic_nanos();
        {
            let mut inner = self.inner.lock().await;
            if !is_leader {
                inner.was_leader = false;
                return Ok(());
            }
            if !inner.was_leader {
                // Heartbeat times and lease-drain instants are observations
                // made by one leader. Carrying them across an election can
                // fence a healthy worker immediately or promote before the
                // new leader has waited out the old owner's leases.
                inner.observations.clear();
                inner.prepared_splits.clear();
                inner.prepared_merges.clear();
                inner.fenced_since.clear();
                // Timed from a clock this node did not have. Starting the
                // cooldown fresh makes a new leader wait before its first move
                // instead of inheriting a deadline it cannot vouch for.
                inner.last_rebalance_at = Some(now);
                inner.observing_since = now;
                inner.was_leader = true;
            }
        }
        self.refresh_health().await?;
        self.repair_voter_set().await?;
        self.fence_dead_owners().await?;
        self.promote_drained_partitions().await?;
        self.advance_pending_splits().await?;
        self.advance_pending_merges().await?;
        self.place_unowned_partitions().await?;
        self.repair_replica_sets().await?;
        // Last, and deliberately so. Everything above restores a property the
        // cluster is supposed to have; this one improves a property it merely
        // prefers. A sweep that has already spent its budget fencing a dead
        // owner should not then spend more moving a live one for balance.
        self.rebalance_ownership().await?;
        Ok(())
    }

    /// Moves one partition's ownership to even out how much owner-side work
    /// each node carries.
    ///
    /// Ownership is not bookkeeping. The owner admits the request, assigns the
    /// version, updates the index, evaluates conditions, coordinates the
    /// quorum, and runs flush and compaction; a replica appends to its WAL and
    /// fsyncs. On the three-node cluster measured for issue #160 a node owning
    /// every partition of a keyspace carried about 50% more CPU than a node
    /// that only replicated it, and spreading ownership flattened that to
    /// within 5%. So an uneven owner count is uneven load, and evening it out
    /// is real capacity rather than tidiness.
    ///
    /// Placement at split time cannot do this job on its own. It only sees the
    /// moment a partition is created, and the skew that matters arrives later:
    /// a failover moves every partition of a dead owner onto one survivor and
    /// nothing moves them back, and a node that joins an already balanced
    /// cluster is never given anything.
    ///
    /// Deliberately slow and deliberately reluctant. One partition per
    /// cooldown, cluster wide, and only when the imbalance is worth a lease
    /// drain. A balancer that moves ownership often enough to be noticed is
    /// worse than the imbalance it is correcting, because every move makes one
    /// partition briefly unavailable and competes with failover, drains,
    /// splits, and merges for the same partitions.
    ///
    /// Ownership can only move to a node that already holds a copy, so this
    /// redistributes owner-side work across a partition's existing holders. It
    /// cannot widen the set of nodes a keyspace lives on; that needs data to
    /// move and is a different mechanism.
    async fn rebalance_ownership(&self) -> Result<()> {
        if !self.config.ownership_rebalancing_enabled {
            return Ok(());
        }
        let now = self.runtime.clock().monotonic_nanos();
        let cooldown = self.config.ownership_rebalance_cooldown.as_nanos() as u64;

        let plan = {
            let inner = self.inner.lock().await;
            if let Some(last) = inner.last_rebalance_at {
                if now.saturating_sub(last) < cooldown {
                    return Ok(());
                }
            }
            Self::rebalance_plan(&inner, self.config.ownership_skew_threshold)
        };

        let Some(Rebalance { partition, to, .. }) = plan else {
            return Ok(());
        };

        // Recorded before the move, not after. `transfer_ownership` waits out a
        // lease drain, so recording afterwards would let a failure partway
        // through retry immediately and turn one refused move into a loop of
        // fences.
        self.inner.lock().await.last_rebalance_at = Some(now);

        match self.transfer_ownership(partition, to).await {
            // The map moved under the plan: something with a better claim on
            // this partition got there first. Nothing to repair, and the next
            // sweep re-derives from whatever it left behind.
            Ok(()) | Err(Error::StaleEpoch { .. } | Error::InvalidArgument(_)) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Chooses the one ownership move that most reduces owner-count skew, or
    /// nothing when no move is worth its cost.
    ///
    /// Split out from the sweep so it can be tested without a cluster: it is a
    /// pure decision over a snapshot, and the parts worth protecting are which
    /// move it picks and when it declines to pick one.
    fn rebalance_plan(inner: &Inner, threshold: usize) -> Option<Rebalance> {
        let eligible: Vec<NodeId> = inner
            .state
            .nodes()
            .filter(|node| inner.state.is_eligible_owner(node.id))
            .map(|node| node.id)
            .collect();
        if eligible.len() < 2 {
            return None;
        }

        let owned = |node: NodeId| -> usize {
            inner
                .state
                .map()
                .partitions()
                .filter(|info| info.owner == Some(node))
                .count()
        };
        // Ties break on node id so the same snapshot always yields the same
        // move. Two leaders disagreeing here would be harmless — only one
        // proposes — but a decision that wanders makes a flapping balancer
        // impossible to tell from a correct one in a log.
        let mut counts: Vec<(usize, NodeId)> = eligible.iter().map(|n| (owned(*n), *n)).collect();
        counts.sort_unstable();

        let (fewest, _) = *counts.first()?;
        let (most, busiest) = *counts.last()?;
        if most.saturating_sub(fewest) < threshold.max(1) {
            return None;
        }

        // Every partition the busiest node owns that could move, cheapest
        // first by partition id for a stable choice.
        let mut movable: Vec<&PartitionInfo> = inner
            .state
            .map()
            .partitions()
            .filter(|info| info.owner == Some(busiest))
            .filter(|info| inner.state.phase(info.id) == Some(PartitionPhase::Serving))
            // A transfer aborts a split or merge in flight on the partition,
            // which throws away work the cluster already paid for to correct
            // an imbalance that will still be there afterwards.
            .filter(|info| !inner.state.is_splitting(info.id) && !inner.state.is_merging(info.id))
            .collect();
        movable.sort_unstable_by_key(|info| info.id);

        // Take the emptiest node that can actually receive one of them. A node
        // can only be promoted where it already holds a copy, so the emptiest
        // node overall is often not a legal target for anything the busiest
        // node owns.
        for (count, target) in counts {
            if target == busiest {
                break;
            }
            // Moving into a node that is already within one of the busiest
            // does not reduce the spread, it just moves the peak.
            if most.saturating_sub(count) < threshold.max(1) {
                break;
            }
            // Balance is not worth a lost write. Promoting a replica that has
            // not reached the owner's committed prefix makes its shorter log
            // authoritative, and with a replication factor of three one
            // replica can lag while writes keep succeeding through the other
            // two. A candidate this leader cannot prove is caught up is simply
            // not moved to: rebalancing is optional work, so declining costs
            // nothing but a little skew.
            if let Some(info) = movable
                .iter()
                .find(|info| {
                    info.replicas.contains(&target)
                        && caught_up_receiver(inner, info, target, required_prefix(inner, info))
                })
                .copied()
            {
                return Some(Rebalance {
                    partition: info.id,
                    from: busiest,
                    to: target,
                });
            }
        }
        None
    }

    async fn repair_voter_set(&self) -> Result<()> {
        if !self.config.voter_management_enabled || !self.log.manages_membership() {
            return Ok(());
        }
        let voters = self.log.voters().await;
        let learners = self.log.learners().await;
        let now = self.runtime.clock().monotonic_nanos();
        let replacement_after = self.config.voter_replacement_after.as_nanos() as u64;
        let (permanently_dead, candidates, surplus, returned) = {
            let inner = self.inner.lock().await;
            let permanently_dead: Vec<NodeId> = voters
                .iter()
                .copied()
                .filter(|voter| {
                    inner
                        .state
                        .node(*voter)
                        .is_some_and(|node| node.health == NodeHealth::Dead)
                        && now.saturating_sub(
                            inner
                                .observations
                                .get(voter)
                                .map_or(inner.observing_since, |observation| {
                                    observation.heard_at_nanos
                                }),
                        ) >= replacement_after
                })
                .collect();
            let voter_domains: BTreeSet<&str> = voters
                .iter()
                .filter_map(|voter| inner.observations.get(voter))
                .map(|observation| observation.status.failure_domain.as_str())
                .filter(|domain| !domain.is_empty())
                .collect();
            let mut candidates: Vec<(bool, usize, String, NodeId, String)> = inner
                .state
                .placement_candidates()
                .into_iter()
                .filter(|node| !voters.contains(node))
                .filter_map(|node| {
                    let status = &inner.observations.get(&node)?.status;
                    status.voter_eligible.then_some((
                        status.failure_domain.is_empty()
                            || voter_domains.contains(status.failure_domain.as_str()),
                        inner.state.map().held_by(node).count(),
                        status.node_identity.clone(),
                        node,
                        status.address.clone(),
                    ))
                })
                .collect();
            candidates.sort_unstable();
            let mut ranked_voters: Vec<(usize, String, NodeId, String)> = voters
                .iter()
                .filter_map(|node| {
                    let status = &inner.observations.get(node)?.status;
                    Some((
                        inner.state.map().held_by(*node).count(),
                        status.node_identity.clone(),
                        *node,
                        status.failure_domain.clone(),
                    ))
                })
                .collect();
            ranked_voters.sort_unstable();
            let mut retained = BTreeSet::new();
            let mut domains = BTreeSet::new();
            for (_, _, node, domain) in &ranked_voters {
                if retained.len() == self.config.voter_target {
                    break;
                }
                if !domain.is_empty() && domains.insert(domain.as_str()) {
                    retained.insert(*node);
                }
            }
            for (_, _, node, _) in &ranked_voters {
                if retained.len() == self.config.voter_target {
                    break;
                }
                retained.insert(*node);
            }
            let local = self.runtime.transport().local_node();
            let mut surplus: Vec<_> = voters
                .iter()
                .copied()
                .filter(|node| !retained.contains(node))
                .filter(|node| {
                    inner
                        .observations
                        .get(node)
                        .is_some_and(|observation| observation.status.voter_eligible)
                        && inner
                            .state
                            .node(*node)
                            .is_some_and(|record| record.health == NodeHealth::Healthy)
                })
                .collect();
            surplus.sort_unstable_by_key(|node| (*node == local, *node));
            // ADR 0014: a permanently dead voter whose id is heartbeating
            // under a new durable identity is a returned incarnation waiting
            // to be reseated. Raft cannot hold two incarnations of one id, so
            // add-first is impossible for it; it becomes eligible for
            // remove-first below, and only below, when nothing else is wrong.
            let returned: Vec<NodeId> = permanently_dead
                .iter()
                .copied()
                .filter(|voter| {
                    inner.returnees.get(voter).is_some_and(|observation| {
                        observation.status.voter_eligible
                            && observation.status.ready
                            && now.saturating_sub(observation.heard_at_nanos)
                                < self.config.suspect_after.as_nanos() as u64
                    })
                })
                .filter(|voter| {
                    // The window at a reduced quorum is accepted only when
                    // every other voter is provably fine. Degraded but stable
                    // beats an automated step that can lose the cluster.
                    voters.iter().filter(|other| *other != voter).all(|other| {
                        inner
                            .state
                            .node(*other)
                            .is_some_and(|record| record.health == NodeHealth::Healthy)
                    })
                })
                .collect();
            (permanently_dead, candidates, surplus, returned)
        };

        let needs_voter = voters.len() < self.config.voter_target
            || (!permanently_dead.is_empty() && voters.len() == self.config.voter_target);
        if needs_voter {
            let Some((_, _, node_identity, candidate, address)) = candidates.first().cloned()
            else {
                // No spare node can absorb the seat add-first. If the dead
                // voter's own returned incarnation is waiting, vacate the
                // seat for it; the next sweeps re-admit it as a learner
                // through the ordinary candidate path and promote it once
                // caught up (ADR 0014).
                if let Some(remove) = returned.first().copied() {
                    self.log
                        .change_membership(MembershipChange::Remove(remove))
                        .await?;
                    self.inner.lock().await.returnees.remove(&remove);
                    tracing::info!(
                        voter = %remove,
                        "vacated a dead voter seat for its returned incarnation"
                    );
                }
                return Ok(());
            };
            if !learners.contains(&candidate) {
                self.log
                    .change_membership(MembershipChange::AddLearnerMember(RaftMember {
                        node: candidate,
                        address,
                        node_identity,
                    }))
                    .await?;
                tracing::info!(%candidate, "added a voter replacement as a Raft learner");
            } else if self.log.learner_caught_up(candidate).await {
                self.log
                    .change_membership(MembershipChange::Promote(candidate))
                    .await?;
                tracing::info!(%candidate, "promoted a caught-up Raft learner to voter");
            }
            return Ok(());
        }

        if voters.len() > self.config.voter_target {
            if let Some(remove) = permanently_dead
                .first()
                .copied()
                .or_else(|| surplus.first().copied())
            {
                self.log
                    .change_membership(MembershipChange::Remove(remove))
                    .await?;
                tracing::info!(voter = %remove, target = self.config.voter_target, "removed a surplus Raft voter");
            }
        }
        Ok(())
    }

    /// Runs the decision loop until the process ends.
    pub async fn run(&self) {
        loop {
            if let Err(e) = self.tick().await {
                tracing::warn!(error = %e, "control sweep failed; retrying next interval");
            }
            self.runtime.clock().sleep(self.config.sweep_interval).await;
        }
    }

    async fn refresh_health(&self) -> Result<()> {
        let now = self.runtime.clock().monotonic_nanos();
        let changes: Vec<(NodeId, NodeHealth)> = {
            let inner = self.inner.lock().await;
            inner
                .state
                .nodes()
                .filter_map(|record| {
                    let silence = now.saturating_sub(
                        inner
                            .observations
                            .get(&record.id)
                            .map_or(inner.observing_since, |obs| obs.heard_at_nanos),
                    );
                    let health = classify(silence, &self.config);
                    (health != record.health).then_some((record.id, health))
                })
                .collect()
        };

        for (node, health) in changes {
            tracing::info!(%node, ?health, "node health changed");
            self.submit(ControlCommand::SetHealth { node, health })
                .await?;
        }
        Ok(())
    }

    async fn fence_dead_owners(&self) -> Result<()> {
        let doomed: Vec<(PartitionId, Epoch)> = {
            let inner = self.inner.lock().await;
            inner
                .state
                .map()
                .partitions()
                .filter(|info| {
                    info.owner.is_some_and(|owner| {
                        inner
                            .state
                            .node(owner)
                            .is_some_and(|n| n.health == NodeHealth::Dead)
                    })
                })
                .map(|info| (info.id, info.epoch))
                .collect()
        };

        for (partition, expect_epoch) in doomed {
            // The epoch bump and the loss of the owner are one entry, and it
            // commits strictly before anything is promoted. That ordering is
            // what stops a deposed owner that never noticed from writing at
            // its old epoch.
            match self
                .submit(ControlCommand::FencePartition {
                    partition,
                    expect_epoch,
                })
                .await
            {
                Ok(()) => {
                    tracing::info!(%partition, "fenced a dead owner");
                    let committed_at = self.runtime.clock().monotonic_nanos();
                    self.inner
                        .lock()
                        .await
                        .fenced_since
                        .insert(partition, committed_at);
                }
                // Another sweep or another leader got there first. The
                // partition is fenced either way, which is all this pass
                // wanted.
                Err(Error::StaleEpoch { .. } | Error::InvalidArgument(_)) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    async fn promote_drained_partitions(&self) -> Result<()> {
        let now = self.runtime.clock().monotonic_nanos();
        let drain = self.config.lease_drain().as_nanos() as u64;

        let (completed_drains, ready): (Vec<ControlCommand>, Vec<ControlCommand>) = {
            let mut inner = self.inner.lock().await;
            let fenced: Vec<PartitionInfo> = inner
                .state
                .map()
                .partitions()
                .filter(|info| {
                    info.owner.is_none()
                        && matches!(
                            inner.state.phase(info.id),
                            Some(PartitionPhase::Fenced { .. })
                        )
                })
                .cloned()
                .collect();

            let mut completed_drains = Vec::new();
            let mut ready = Vec::new();
            for info in fenced {
                let Some(PartitionPhase::Fenced {
                    map_version,
                    drain_complete,
                    ..
                }) = inner.state.phase(info.id)
                else {
                    continue;
                };
                // A leader that inherited this partition mid-failover has no
                // record of when the fence happened, so it starts the wait
                // now. Waiting longer than necessary costs availability;
                // waiting less could let a replica serve a pre-failover value.
                if !drain_complete {
                    let since = *inner.fenced_since.entry(info.id).or_insert(now);
                    if now.saturating_sub(since) < drain {
                        continue;
                    }
                    // Tag 17 belongs to protocol 0.1. Before finalization the
                    // previous binary must still be able to read every entry.
                    if inner.state.cluster_version() >= ClusterVersion::new(0, 1) {
                        completed_drains.push(ControlCommand::CompleteFenceDrain {
                            partition: info.id,
                            expect_epoch: info.epoch,
                        });
                        continue;
                    }
                }
                let Some(owner) = best_candidate(&inner, &info, map_version) else {
                    // No replica has reported its position yet. Promoting one
                    // blind could pick a node that is behind and lose an
                    // acknowledged write, so the partition stays unavailable
                    // until somebody reports.
                    tracing::warn!(
                        partition = %info.id,
                        "no eligible replica has reported a durable position; not promoting"
                    );
                    continue;
                };
                ready.push(ControlCommand::AssignOwner {
                    partition: info.id,
                    owner,
                    replicas: info
                        .replicas
                        .iter()
                        .copied()
                        .filter(|r| *r != owner)
                        .collect(),
                    expect_epoch: info.epoch,
                });
            }
            (completed_drains, ready)
        };

        for command in completed_drains {
            let partition = match &command {
                ControlCommand::CompleteFenceDrain { partition, .. } => *partition,
                _ => continue,
            };
            match self.submit(command).await {
                Ok(()) => {
                    self.inner.lock().await.fenced_since.remove(&partition);
                }
                Err(Error::StaleEpoch { .. } | Error::InvalidArgument(_)) => {}
                Err(e) => return Err(e),
            }
        }

        for command in ready {
            let (partition, owner) = match &command {
                ControlCommand::AssignOwner {
                    partition, owner, ..
                } => (*partition, *owner),
                _ => continue,
            };
            match self.submit(command).await {
                Ok(()) => {
                    tracing::info!(%partition, %owner, "promoted a replica to owner");
                    self.inner.lock().await.fenced_since.remove(&partition);
                }
                Err(Error::StaleEpoch { .. }) => {}
                Err(error @ Error::InvalidArgument(_)) => {
                    tracing::warn!(
                        %partition,
                        %owner,
                        %error,
                        "an eligible failover promotion was refused"
                    );
                    return Err(error);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    async fn place_unowned_partitions(&self) -> Result<()> {
        let unowned: Vec<PartitionId> = {
            let inner = self.inner.lock().await;
            inner
                .state
                .map()
                .partitions()
                .filter(|info| {
                    info.owner.is_none()
                        && inner.state.phase(info.id) == Some(PartitionPhase::Unowned)
                })
                .map(|info| info.id)
                .collect()
        };

        for partition in unowned {
            // Placement is recomputed per partition so that a cluster coming
            // up with several unowned partitions spreads them instead of
            // stacking them all on whoever happened to be least loaded first.
            //
            // A partition that never had an owner never granted a read lease,
            // so there is nothing to drain and no reason to make a new cluster
            // wait before it can accept writes.
            let command = {
                let inner = self.inner.lock().await;
                let Some(info) = inner.state.map().partition(partition) else {
                    continue;
                };
                let candidates = inner.state.placement_candidates();
                let (owner, replicas) =
                    split_placement(&candidates, self.config.replication_factor);
                let Some(owner) = owner else { continue };
                ControlCommand::AssignOwner {
                    partition,
                    owner,
                    replicas,
                    expect_epoch: info.epoch,
                }
            };
            match self.submit(command).await {
                Ok(()) | Err(Error::StaleEpoch { .. } | Error::InvalidArgument(_)) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    async fn repair_replica_sets(&self) -> Result<()> {
        // The read-serving set, not the durability quorum. A peer past the
        // quorum is not counted by the WAL, so widening this costs nothing in
        // durability. It is not free: every holder is an invalidation a write
        // waits on before it is acknowledged, which is the coherence quorum
        // ADR 0001 keeps separate from durability. Read capacity is therefore
        // bought out of write throughput, which is why there is a ceiling.
        let floor = self.config.replication_factor.saturating_sub(1);
        let commands: Vec<ControlCommand> = {
            let inner = self.inner.lock().await;
            let candidates = inner.state.placement_candidates();
            let want = read_replica_want(self.config.read_replica_target, floor, candidates.len());
            inner
                .state
                .map()
                .partitions()
                .filter(|info| inner.state.phase(info.id) == Some(PartitionPhase::Serving))
                .filter_map(|info| {
                    let owner = info.owner?;
                    let mut next: Vec<NodeId> = info
                        .replicas
                        .iter()
                        .copied()
                        .filter(|r| {
                            inner
                                .state
                                .node(*r)
                                .is_some_and(|n| n.health != NodeHealth::Dead)
                        })
                        .collect();
                    for candidate in &candidates {
                        if next.len() >= want {
                            break;
                        }
                        if *candidate != owner && !next.contains(candidate) {
                            next.push(*candidate);
                        }
                    }
                    (next != info.replicas).then(|| ControlCommand::SetReplicas {
                        partition: info.id,
                        replicas: next,
                        expect_epoch: info.epoch,
                    })
                })
                .collect()
        };

        for command in commands {
            match self.submit(command).await {
                Ok(()) | Err(Error::StaleEpoch { .. } | Error::InvalidArgument(_)) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

/// The most caught-up replica eligible for new ownership, or `None` until
/// every surviving replica has reported its durable position after the fence.
///
/// Highest durable Lamport wins, because that is what bounds the writes the
/// cluster has acknowledged: the WAL acknowledges at two of three, so any
/// acknowledged entry is on at least one surviving node, and promoting the
/// furthest survivor cannot lose one. A missing report, or one generated from
/// a pre-fence map, is an unknown upper bound rather than evidence that the
/// replica is behind. Ties break on node id so the choice is reproducible from
/// a seed.
///
/// The candidate list includes the node the fence deposed, because
/// `fence_partition` demotes it into `replicas` rather than dropping it. That
/// is what makes the sentence above true rather than nearly true: an owner is
/// a member of every durability quorum it counted, so it holds the whole
/// committed prefix, and excluding it left a promotion able to pick a
/// survivor that is genuinely behind an acknowledged write whenever the
/// replica set had already shrunk. Nothing here treats it as special; it is
/// judged on the position it reports, so a deposed owner that came back with
/// a shorter log loses to a replica that did not. See
/// [ADR 0008](../../../docs/adr/0008-a-fenced-owner-stays-a-replica.md).
fn best_candidate(
    inner: &Inner,
    info: &PartitionInfo,
    fence_map_version: MapVersion,
) -> Option<NodeId> {
    let mut durable_required = None;
    for replica in &info.replicas {
        // Replica membership cannot outlive its node record because ForgetNode
        // refuses held partitions. Fail closed if historical or corrupt state
        // ever violates that invariant: an unknown replica may hold the tail.
        let node = inner.state.node(*replica)?;
        if node.health == NodeHealth::Dead {
            continue;
        }
        let observation = inner.observations.get(replica)?;
        if observation.status.map_version < fence_map_version {
            return None;
        }
        let durable = observation.status.progress(info.id)?.durable_lamport;
        durable_required =
            Some(durable_required.map_or(durable, |seen: Lamport| seen.max(durable)));
    }
    let durable_required = durable_required?;

    info.replicas
        .iter()
        .filter(|replica| inner.state.new_ownership_eligibility(**replica).is_ok())
        .filter(|replica| {
            inner
                .observations
                .get(replica)
                .and_then(|obs| obs.status.progress(info.id))
                .is_some_and(|progress| progress.durable_lamport == durable_required)
        })
        .min()
        .copied()
}

/// A boundary key derived from the range alone, used only when the caller did
/// not supply one.
///
/// A poor split point on purpose: it exists so a keyless manual split does
/// something rather than failing. A good boundary comes from the owner, the
/// only node that knows how the keys are distributed inside the range.
fn suggested_split_key(info: &PartitionInfo) -> Option<Bytes> {
    let start = info.range.start();
    match info.range.end() {
        None => {
            let mut key = start.to_vec();
            key.push(0);
            Some(Bytes::from(key))
        }
        Some(end) => {
            let mut key = start.to_vec();
            key.push(0);
            (key.as_slice() < end).then(|| Bytes::from(key))
        }
    }
}

/// Splits an ordered candidate list into an owner and a replica set.
fn split_placement(
    candidates: &[NodeId],
    replication_factor: usize,
) -> (Option<NodeId>, Vec<NodeId>) {
    let Some((owner, rest)) = candidates.split_first() else {
        return (None, Vec::new());
    };
    let replicas = rest
        .iter()
        .copied()
        .take(replication_factor.saturating_sub(1))
        .collect();
    (Some(*owner), replicas)
}

fn classify(silence_nanos: u64, config: &ControlConfig) -> NodeHealth {
    if silence_nanos >= config.dead_after.as_nanos() as u64 {
        NodeHealth::Dead
    } else if silence_nanos >= config.suspect_after.as_nanos() as u64 {
        NodeHealth::Suspect
    } else {
        NodeHealth::Healthy
    }
}

fn random_hex(bytes: usize) -> Result<String> {
    let mut buf = vec![0u8; bytes];
    getrandom::getrandom(&mut buf)
        .map_err(|e| Error::Internal(format!("no operating system entropy: {e}")))?;
    Ok(buf.iter().map(|b| format!("{b:02x}")).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SingleNodeLog;
    use orbita_core::KeyspaceId;
    use orbita_sim::Simulation;

    #[test]
    fn silence_moves_a_node_through_suspect_before_dead() {
        let config = ControlConfig::default();
        let nanos = |ms: u64| ms * 1_000_000;
        assert_eq!(classify(nanos(0), &config), NodeHealth::Healthy);
        assert_eq!(classify(nanos(999), &config), NodeHealth::Healthy);
        assert_eq!(classify(nanos(1_000), &config), NodeHealth::Suspect);
        assert_eq!(classify(nanos(2_999), &config), NodeHealth::Suspect);
        assert_eq!(classify(nanos(3_000), &config), NodeHealth::Dead);
    }

    #[test]
    fn placement_gives_the_first_candidate_the_partition_and_the_rest_replicate_it() {
        let candidates = vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)];
        let (owner, replicas) = split_placement(&candidates, 3);
        assert_eq!(owner, Some(NodeId(1)));
        assert_eq!(replicas, vec![NodeId(2), NodeId(3)]);
    }

    #[test]
    fn placement_on_an_empty_cluster_yields_no_owner_rather_than_panicking() {
        let (owner, replicas) = split_placement(&[], 3);
        assert_eq!(owner, None);
        assert!(replicas.is_empty());
    }

    #[test]
    fn a_single_node_cluster_places_an_owner_with_no_replicas() {
        let (owner, replicas) = split_placement(&[NodeId(1)], 3);
        assert_eq!(owner, Some(NodeId(1)));
        assert!(replicas.is_empty());
    }

    #[test]
    fn a_rejected_proposal_keeps_its_outcome_after_result_cleanup() {
        let sim = Simulation::new(1);
        let runtime = sim.add_node(NodeId(1));
        let opening = runtime.clone();
        let log = sim
            .block_on(async move { SingleNodeLog::open(&opening).await })
            .unwrap();
        let controller = Controller::new(runtime, Arc::clone(&log), ControlConfig::default());
        let rejected = ControlCommand::FencePartition {
            partition: PartitionId(99),
            expect_epoch: Epoch(1),
        };
        let proposing = Arc::clone(&log);
        let command = rejected.clone();
        let target = sim
            .block_on(async move { proposing.propose(command).await })
            .expect("commit rejected command");
        for _ in 0..1025 {
            let proposing = Arc::clone(&log);
            let command = rejected.clone();
            sim.block_on(async move { proposing.propose(command).await })
                .expect("commit command past cleanup window");
        }
        let recovering = controller.clone();
        sim.block_on(async move { recovering.recover().await })
            .expect("apply committed commands");

        let outcome = sim.block_on(async move { controller.outcome_at(target).await });

        assert!(matches!(outcome, Err(Error::InvalidArgument(_))));
    }

    /// A leader with three registered workers and one keyspace.
    #[allow(clippy::type_complexity)]
    fn merged_cluster(
        seed: u64,
    ) -> (
        Simulation,
        Controller<orbita_sim::SimRuntime, SingleNodeLog<orbita_sim::SimRuntime>>,
    ) {
        let sim = Simulation::new(seed);
        let runtime = sim.add_node(NodeId(1));
        let opening = runtime.clone();
        let log = sim
            .block_on(async move { SingleNodeLog::open(&opening).await })
            .unwrap();
        let controller = Controller::new(runtime, log, ControlConfig::default());

        let setup = controller.clone();
        sim.block_on(async move {
            for id in 1..=3 {
                setup
                    .submit(ControlCommand::RegisterNode {
                        node: NodeId(id),
                        role: NodeRole::Worker,
                        address: format!("10.0.0.{id}:7000"),
                        speaks: crate::version::binary_speaks(),
                        ready: true,
                        draining: false,
                    })
                    .await
                    .unwrap();
            }
            setup
                .submit(ControlCommand::SetClusterVersion {
                    version: crate::version::binary_speaks().max,
                    expect: ClusterVersion::ZERO,
                })
                .await
                .unwrap();
            setup
                .submit(ControlCommand::CreateKeyspace {
                    id: KeyspaceId(1),
                    name: "default".into(),
                    config: Default::default(),
                    created_at_millis: 1,
                    first_partition: PartitionId(1),
                    owner: Some(NodeId(1)),
                    replicas: vec![NodeId(2), NodeId(3)],
                })
                .await
                .unwrap();
        });
        (sim, controller)
    }

    /// Splits the keyspace's single partition into `ids.len() + 1` partitions.
    async fn split_into(
        controller: &Controller<orbita_sim::SimRuntime, SingleNodeLog<orbita_sim::SimRuntime>>,
        boundaries: &[(&'static [u8], u64, u64)],
    ) {
        for (at, lower, upper) in boundaries {
            let parent = {
                let inner = controller.inner.lock().await;
                // The partition whose half-open range covers the boundary.
                let found = inner
                    .state
                    .map()
                    .partitions()
                    .find(|p| p.range.start() <= *at && p.range.end().is_none_or(|end| *at < end))
                    .cloned();
                found
            };
            let parent = parent.expect("a partition covering the boundary");
            controller
                .submit(ControlCommand::BeginSplit {
                    parent: parent.id,
                    at: Bytes::from_static(at),
                    lower: PartitionId(*lower),
                    upper: PartitionId(*upper),
                    expect_epoch: parent.epoch,
                })
                .await
                .unwrap();
            let mut holders = vec![parent.owner.unwrap()];
            holders.extend(parent.replicas.iter().copied());
            for node in holders {
                controller
                    .submit(ControlCommand::MarkSplitPrepared {
                        parent: parent.id,
                        node,
                        expect_epoch: parent.epoch,
                    })
                    .await
                    .unwrap();
            }
            controller
                .submit(ControlCommand::CompleteSplit {
                    parent: parent.id,
                    expect_epoch: parent.epoch,
                })
                .await
                .unwrap();
        }
    }

    /// Has every node report that it holds every partition, fully caught up.
    ///
    /// Ownership cannot move to a node the leader cannot prove is caught up,
    /// so a fixture with no heartbeats has no legal destinations and every
    /// placement decision correctly declines. Reporting progress is what makes
    /// these clusters look like clusters rather than like maps.
    async fn all_caught_up(
        controller: &Controller<orbita_sim::SimRuntime, SingleNodeLog<orbita_sim::SimRuntime>>,
        lamport: u64,
    ) {
        let partitions: Vec<PartitionId> = {
            let inner = controller.inner.lock().await;
            let out = inner.state.map().partitions().map(|p| p.id).collect();
            out
        };
        for id in 1..=3u64 {
            let mut status = NodeStatus::joining(NodeRole::Worker, format!("10.0.0.{id}:7000"));
            status.ready = true;
            status.speaks = crate::version::binary_speaks();
            status.partitions = partitions
                .iter()
                .map(|partition| crate::membership::PartitionProgress {
                    partition: *partition,
                    durable_lamport: Lamport(lamport),
                    applied_lamport: Lamport(lamport),
                    size_bytes: 0,
                    index_bytes: None,
                    committed_lamport: Some(Lamport(lamport)),
                })
                .collect();
            controller
                .record_status(NodeId(id), status)
                .await
                .expect("a registered worker may report");
        }
    }

    /// Forces every partition onto one owner, which is the shape a failover
    /// leaves behind and the shape the balancer exists to undo.
    async fn pile_onto(
        controller: &Controller<orbita_sim::SimRuntime, SingleNodeLog<orbita_sim::SimRuntime>>,
        owner: NodeId,
        replicas: Vec<NodeId>,
    ) {
        let ids: Vec<PartitionId> = {
            let inner = controller.inner.lock().await;
            let out = inner.state.map().partitions().map(|p| p.id).collect();
            out
        };
        for id in ids {
            let epoch = controller.epoch_of(id).await.unwrap();
            controller
                .submit(ControlCommand::FencePartition {
                    partition: id,
                    expect_epoch: epoch,
                })
                .await
                .unwrap();
            let fenced = controller.epoch_of(id).await.unwrap();
            controller
                .submit(ControlCommand::AssignOwner {
                    partition: id,
                    owner,
                    replicas: replicas.clone(),
                    expect_epoch: fenced,
                })
                .await
                .unwrap();
        }
    }

    type TestController = Controller<orbita_sim::SimRuntime, SingleNodeLog<orbita_sim::SimRuntime>>;

    async fn owner_counts(controller: &TestController) -> BTreeMap<NodeId, usize> {
        let inner = controller.inner.lock().await;
        let mut counts = BTreeMap::new();
        for info in inner.state.map().partitions() {
            if let Some(owner) = info.owner {
                *counts.entry(owner).or_insert(0) += 1;
            }
        }
        counts
    }

    #[test]
    fn every_partition_on_one_owner_plans_a_move_off_it() {
        // The shape a failover leaves: one survivor holding everything its
        // dead peer used to own, and nothing that moves them back. The owner
        // does strictly more work per write than a replica, so this is a real
        // load imbalance rather than untidy bookkeeping. See issue #160.
        let (sim, controller) = merged_cluster(300);
        sim.block_on(async move {
            split_into(
                &controller,
                &[(b"m", 10, 11), (b"f", 12, 13), (b"t", 14, 15)],
            )
            .await;
            pile_onto(&controller, NodeId(1), vec![NodeId(2), NodeId(3)]).await;
            all_caught_up(&controller, 100).await;
            assert_eq!(
                owner_counts(&controller).await,
                BTreeMap::from([(NodeId(1), 4)]),
                "the fixture should have piled all four onto node 1"
            );

            let inner = controller.inner.lock().await;
            let plan = TestController::rebalance_plan(&inner, 2).expect("a move");
            assert_eq!(plan.from, NodeId(1));
            assert_ne!(plan.to, NodeId(1));
            assert!(
                inner
                    .state
                    .map()
                    .partition(plan.partition)
                    .unwrap()
                    .replicas
                    .contains(&plan.to),
                "ownership can only move where a copy already is"
            );
        });
    }

    #[test]
    fn an_evenly_owned_cluster_plans_no_move() {
        // Every move costs one partition a lease drain. A balancer that keeps
        // moving a cluster that is already as even as it can be spends
        // availability forever and buys nothing.
        let (sim, controller) = merged_cluster(301);
        sim.block_on(async move {
            split_into(
                &controller,
                &[(b"m", 10, 11), (b"f", 12, 13), (b"t", 14, 15)],
            )
            .await;

            let counts = owner_counts(&controller).await;
            assert_eq!(
                counts.values().sum::<usize>(),
                4,
                "the fixture should have four owned partitions, got {counts:?}"
            );
            assert_eq!(counts.len(), 3, "spread across every node, got {counts:?}");
            assert!(
                counts.values().max().unwrap() - counts.values().min().unwrap() <= 1,
                "a spread split should already be even, got {counts:?}"
            );

            let inner = controller.inner.lock().await;
            assert_eq!(TestController::rebalance_plan(&inner, 2), None);
        });
    }

    #[test]
    fn rebalancing_converges_and_then_stops() {
        // The property that separates a balancer from a flapper: applying its
        // own advice repeatedly has to reach a state where it has no advice,
        // rather than trading the same partition back and forth.
        let (sim, controller) = merged_cluster(302);
        sim.block_on(async move {
            split_into(
                &controller,
                &[(b"m", 10, 11), (b"f", 12, 13), (b"t", 14, 15)],
            )
            .await;
            pile_onto(&controller, NodeId(1), vec![NodeId(2), NodeId(3)]).await;
            all_caught_up(&controller, 100).await;

            let mut moves = 0;
            loop {
                let plan = {
                    let inner = controller.inner.lock().await;
                    TestController::rebalance_plan(&inner, 2)
                };
                let Some(plan) = plan else { break };
                moves += 1;
                assert!(moves <= 8, "did not converge in {moves} moves");
                controller
                    .transfer_ownership(plan.partition, plan.to)
                    .await
                    .unwrap();
            }

            let counts = owner_counts(&controller).await;
            let spread = counts.values().max().unwrap() - counts.values().min().unwrap();
            assert!(
                spread < 2,
                "stopped while still skewed by {spread}: {counts:?}"
            );
            assert!(
                moves > 0,
                "started skewed, so it should have moved something"
            );
        });
    }

    #[test]
    fn the_cooldown_allows_only_one_move_per_window() {
        // Each move makes one partition briefly unavailable, so the rate is
        // the safety property. A badly skewed cluster must not be corrected by
        // fencing every partition at once, which would be a self-inflicted
        // outage in the name of balance.
        let (sim, controller) = merged_cluster(304);
        sim.block_on(async move {
            split_into(
                &controller,
                &[(b"m", 10, 11), (b"f", 12, 13), (b"t", 14, 15)],
            )
            .await;
            pile_onto(&controller, NodeId(1), vec![NodeId(2), NodeId(3)]).await;
            all_caught_up(&controller, 100).await;

            controller.rebalance_ownership().await.unwrap();
            let after_one = owner_counts(&controller).await;
            assert_eq!(
                after_one.get(&NodeId(1)).copied().unwrap_or(0),
                3,
                "exactly one partition should have left node 1, got {after_one:?}"
            );

            // Still skewed, and it still declines, because the window has not
            // elapsed.
            for _ in 0..3 {
                controller.rebalance_ownership().await.unwrap();
            }
            assert_eq!(
                owner_counts(&controller).await,
                after_one,
                "the cooldown did not hold a second move"
            );
        });
    }

    /// A log whose membership can be set, so a node can be placed inside or
    /// outside the configuration without standing up a Raft group.
    #[derive(Clone)]
    struct MembershipLog {
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
    }

    impl ConsensusLog for MembershipLog {
        async fn propose(&self, _command: ControlCommand) -> Result<LogIndex> {
            Ok(0)
        }
        async fn commit_index(&self) -> LogIndex {
            0
        }
        async fn subscribe(&self, _after: LogIndex) -> Result<Vec<crate::LogEntry>> {
            Ok(Vec::new())
        }
        async fn leader_barrier(&self) -> Result<LogIndex> {
            Ok(0)
        }
        async fn is_leader(&self) -> bool {
            true
        }
        async fn leader(&self) -> Option<NodeId> {
            None
        }
        async fn voters(&self) -> Vec<NodeId> {
            self.voters.clone()
        }
        async fn learners(&self) -> Vec<NodeId> {
            self.learners.clone()
        }
    }

    fn membership_controller(
        sim: &Simulation,
        local: NodeId,
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
    ) -> Controller<orbita_sim::SimRuntime, MembershipLog> {
        let runtime = sim.add_node(local);
        Controller::new(
            runtime,
            Arc::new(MembershipLog { voters, learners }),
            ControlConfig::default(),
        )
    }

    #[test]
    fn the_read_replica_target_widens_the_holder_set_without_touching_durability() {
        // The control-plane half of ADR 0013. Raising the read target adds
        // holders, and holders are what may serve a read; it must not change
        // the replication factor, which is what sizes the durability contract.
        // If the two moved together, read capacity could only be bought with
        // write latency.
        let config = ControlConfig::default()
            .with_read_replica_target(4)
            .expect("above the durability floor");
        assert_eq!(config.replication_factor, 3, "durability is untouched");
        assert_eq!(config.read_replica_target, 4);

        // And it refuses to take copies away, which is the direction that
        // would quietly weaken durability rather than widen reach.
        let refused = ControlConfig::default().with_read_replica_target(1);
        assert!(
            refused.is_err(),
            "a target below the durability floor must be refused, got {refused:?}"
        );
    }

    #[test]
    fn no_partition_is_held_by_more_than_half_the_cluster() {
        // Every holder is an invalidation a write waits on, so an operator who
        // sets this high is spending write throughput without being told. The
        // cap keeps a write off a majority of the nodes and leaves half the
        // cluster free of the obligation. Holders are replicas plus the owner,
        // so the replica cap is one below half.
        const FLOOR: usize = 2;

        assert_eq!(
            read_replica_want(9, FLOOR, 10),
            4,
            "a target past the ceiling is clamped to it"
        );
        assert_eq!(
            read_replica_want(4, FLOOR, 10),
            4,
            "a target exactly at the ceiling is left alone"
        );
        assert_eq!(
            read_replica_want(4, FLOOR, 20),
            4,
            "a target under the ceiling is not inflated to it"
        );

        // The property the number is chosen for, stated directly.
        for placeable in [8usize, 10, 16, 40] {
            let holders = read_replica_want(usize::MAX, FLOOR, placeable) + 1;
            assert!(
                holders * 2 <= placeable,
                "{holders} holders out of {placeable} nodes is more than half"
            );
        }
    }

    #[test]
    fn durability_outranks_the_read_ceiling_on_a_small_cluster() {
        // The ceiling bounds what reads may take. It may not take copies away,
        // because the replicas underneath it are what the write quorum is made
        // of. On a cluster too small to satisfy both, the floor wins and the
        // holder set exceeds half deliberately.
        const FLOOR: usize = 2;

        assert_eq!(
            read_replica_want(4, FLOOR, 5),
            FLOOR,
            "a five node cluster cannot honour the ceiling without dropping below the floor"
        );
        assert_eq!(
            read_replica_want(4, FLOOR, 3),
            FLOOR,
            "nor can a three node one"
        );
        assert_eq!(
            read_replica_want(4, FLOOR, 0),
            FLOOR,
            "an empty candidate set must not shrink the durability contract"
        );
    }

    #[test]
    fn a_node_outside_the_configuration_knows_it_is_not_a_member() {
        // Every combined node hosts a Raft state machine, but only the ones in
        // the configuration are replicated to. A node outside it holds a log
        // nothing will ever advance, so waiting for that log to catch up waits
        // forever — and waiting is what keeps it out, because the voter set is
        // filled from nodes eligible to own partitions and eligibility needs
        // readiness. Telling the two apart is what breaks that cycle. See
        // issue #167.
        let sim = Simulation::new(400);

        let voter = membership_controller(&sim, NodeId(1), vec![NodeId(1), NodeId(2)], Vec::new());
        assert!(
            sim.block_on(async move { voter.is_active_member().await }),
            "a voter is replicated to and must still be held to catching up"
        );

        let learner =
            membership_controller(&sim, NodeId(3), vec![NodeId(1), NodeId(2)], vec![NodeId(3)]);
        assert!(
            sim.block_on(async move { learner.is_active_member().await }),
            "a learner is replicated to so it can be promoted, so it counts too"
        );

        let surplus =
            membership_controller(&sim, NodeId(4), vec![NodeId(1), NodeId(2)], Vec::new());
        assert!(
            !sim.block_on(async move { surplus.is_active_member().await }),
            "a node the configuration leaves out can never catch up and must not be asked to"
        );
    }

    #[test]
    fn a_lagging_replica_is_never_promoted_for_balance() {
        // The one way an optional optimisation can lose data. A write is
        // acknowledged once a durability quorum holds it, so with three
        // holders one replica can sit behind while writes keep succeeding
        // through the other two. Fencing the owner and promoting that replica
        // makes its shorter log authoritative, and the writes only it is
        // missing were already promised to clients. Balance is never worth
        // that, so a candidate the leader cannot prove is caught up is not a
        // candidate.
        let (sim, controller) = merged_cluster(305);
        sim.block_on(async move {
            split_into(
                &controller,
                &[(b"m", 10, 11), (b"f", 12, 13), (b"t", 14, 15)],
            )
            .await;
            pile_onto(&controller, NodeId(1), vec![NodeId(2), NodeId(3)]).await;

            // Hand one partition to node 2, so node 3 is the emptiest node and
            // therefore the balancer's first choice. Without that the tie
            // break on node id would pick node 2 anyway and the lag would
            // never be what decided anything.
            let handed = {
                let inner = controller.inner.lock().await;
                let id = inner.state.map().partitions().next().unwrap().id;
                id
            };
            let epoch = controller.epoch_of(handed).await.unwrap();
            controller
                .submit(ControlCommand::FencePartition {
                    partition: handed,
                    expect_epoch: epoch,
                })
                .await
                .unwrap();
            let fenced = controller.epoch_of(handed).await.unwrap();
            controller
                .submit(ControlCommand::AssignOwner {
                    partition: handed,
                    owner: NodeId(2),
                    replicas: vec![NodeId(1), NodeId(3)],
                    expect_epoch: fenced,
                })
                .await
                .unwrap();
            all_caught_up(&controller, 100).await;

            // Node 3 falls behind the committed prefix. Node 2 stays current.
            let partitions: Vec<PartitionId> = {
                let inner = controller.inner.lock().await;
                let out = inner.state.map().partitions().map(|p| p.id).collect();
                out
            };
            let mut lagging = NodeStatus::joining(NodeRole::Worker, "10.0.0.3:7000");
            lagging.ready = true;
            lagging.speaks = crate::version::binary_speaks();
            lagging.partitions = partitions
                .iter()
                .map(|partition| crate::membership::PartitionProgress {
                    partition: *partition,
                    durable_lamport: Lamport(40),
                    applied_lamport: Lamport(40),
                    size_bytes: 0,
                    index_bytes: None,
                    committed_lamport: Some(Lamport(40)),
                })
                .collect();
            controller.record_status(NodeId(3), lagging).await.unwrap();

            let inner = controller.inner.lock().await;
            let plan =
                TestController::rebalance_plan(&inner, 2).expect("node 2 can still take one");
            assert_ne!(
                plan.to,
                NodeId(3),
                "promoted a replica that is 60 Lamports behind the committed prefix"
            );
        });
    }

    #[test]
    fn balance_waits_rather_than_promoting_when_nobody_is_proven_caught_up() {
        // The same rule with no safe destination left. Declining is the whole
        // behaviour: the skew stays, which costs a little throughput, and no
        // acknowledged write is at risk, which is the trade the balancer is
        // allowed to make.
        let (sim, controller) = merged_cluster(306);
        sim.block_on(async move {
            split_into(
                &controller,
                &[(b"m", 10, 11), (b"f", 12, 13), (b"t", 14, 15)],
            )
            .await;
            pile_onto(&controller, NodeId(1), vec![NodeId(2), NodeId(3)]).await;
            // Only the owner reports. Nothing is known about either replica,
            // and absence of a report is absence of evidence.
            let partitions: Vec<PartitionId> = {
                let inner = controller.inner.lock().await;
                let out = inner.state.map().partitions().map(|p| p.id).collect();
                out
            };
            let mut owner = NodeStatus::joining(NodeRole::Worker, "10.0.0.1:7000");
            owner.ready = true;
            owner.speaks = crate::version::binary_speaks();
            owner.partitions = partitions
                .iter()
                .map(|partition| crate::membership::PartitionProgress {
                    partition: *partition,
                    durable_lamport: Lamport(100),
                    applied_lamport: Lamport(100),
                    size_bytes: 0,
                    index_bytes: None,
                    committed_lamport: Some(Lamport(100)),
                })
                .collect();
            controller.record_status(NodeId(1), owner).await.unwrap();

            let inner = controller.inner.lock().await;
            assert_eq!(
                TestController::rebalance_plan(&inner, 2),
                None,
                "moved ownership to a node it had heard nothing from"
            );
        });
    }

    #[test]
    fn a_pair_held_by_different_nodes_is_refused_before_anything_moves() {
        // `BeginMerge` needs one complete holder set, not merely one owner, so
        // a partly overlapping pair can never merge however ownership is
        // arranged. Relocating first and finding out afterwards would cost the
        // parent a lease-drain outage and leave it moved for a merge that was
        // always going to be refused.
        let (sim, controller) = merged_cluster(307);
        sim.block_on(async move {
            split_into(&controller, &[(b"m", 10, 11)]).await;
            all_caught_up(&controller, 100).await;

            // Narrow the upper child's holders so the two overlap without
            // matching.
            let upper = {
                let inner = controller.inner.lock().await;
                let found = inner
                    .state
                    .map()
                    .partition(PartitionId(11))
                    .unwrap()
                    .clone();
                found
            };
            let keep: Vec<NodeId> = upper.replicas.iter().copied().take(1).collect();
            controller
                .submit(ControlCommand::SetReplicas {
                    partition: PartitionId(11),
                    replicas: keep,
                    expect_epoch: upper.epoch,
                })
                .await
                .unwrap();

            let before: Vec<(PartitionId, Option<NodeId>, Epoch)> = {
                let inner = controller.inner.lock().await;
                let out = inner
                    .state
                    .map()
                    .partitions()
                    .map(|p| (p.id, p.owner, p.epoch))
                    .collect();
                out
            };

            let refused = controller
                .colocate_merge_parents(PartitionId(10), PartitionId(11))
                .await;
            assert!(
                matches!(refused, Err(Error::InvalidArgument(_))),
                "expected a refusal, got {refused:?}"
            );

            let after: Vec<(PartitionId, Option<NodeId>, Epoch)> = {
                let inner = controller.inner.lock().await;
                let out = inner
                    .state
                    .map()
                    .partitions()
                    .map(|p| (p.id, p.owner, p.epoch))
                    .collect();
                out
            };
            assert_eq!(
                before, after,
                "a refused merge moved ownership anyway, paying a drain for nothing"
            );
        });
    }

    #[test]
    fn a_partition_mid_split_is_left_alone_by_the_balancer() {
        // A transfer aborts a split in flight, throwing away preparation the
        // cluster already paid for, to correct an imbalance that is still
        // there afterwards.
        let (sim, controller) = merged_cluster(303);
        sim.block_on(async move {
            split_into(&controller, &[(b"m", 10, 11)]).await;
            pile_onto(&controller, NodeId(1), vec![NodeId(2), NodeId(3)]).await;
            all_caught_up(&controller, 100).await;

            // Open a split on one of them and leave it open.
            let victim = {
                let inner = controller.inner.lock().await;
                let found = inner.state.map().partitions().next().unwrap().clone();
                found
            };
            controller
                .submit(ControlCommand::BeginSplit {
                    parent: victim.id,
                    at: Bytes::from_static(b"c"),
                    lower: PartitionId(20),
                    upper: PartitionId(21),
                    expect_epoch: victim.epoch,
                })
                .await
                .unwrap();

            let inner = controller.inner.lock().await;
            if let Some(plan) = TestController::rebalance_plan(&inner, 2) {
                assert_ne!(
                    plan.partition, victim.id,
                    "the balancer picked the partition that is mid-split"
                );
            }
        });
    }

    #[test]
    fn merging_two_split_children_moves_one_onto_the_others_owner_first() {
        // A split now spreads its children, so the two halves of one split are
        // the common case of an adjacent pair with different owners — and the
        // pair an operator is most likely to want merged back. Merge still
        // needs one holder set, because the cut it makes is a single
        // in-process operation over two local WALs. ADR 0010 reserved this
        // answer: "a later protocol can move one parent first if cross-owner
        // merge is needed." The operator should not have to know that.
        let (sim, controller) = merged_cluster(200);

        let driving = controller.clone();
        sim.block_on(async move {
            let parent = {
                let inner = driving.inner.lock().await;
                let first = inner.state.map().partitions().next().unwrap().clone();
                first
            };
            driving
                .submit(ControlCommand::BeginSplit {
                    parent: parent.id,
                    at: Bytes::from_static(b"m"),
                    lower: PartitionId(10),
                    upper: PartitionId(11),
                    expect_epoch: parent.epoch,
                })
                .await
                .unwrap();
            let mut holders = vec![parent.owner.unwrap()];
            holders.extend(parent.replicas.iter().copied());
            for node in holders {
                driving
                    .submit(ControlCommand::MarkSplitPrepared {
                        parent: parent.id,
                        node,
                        expect_epoch: parent.epoch,
                    })
                    .await
                    .unwrap();
            }
            driving
                .submit(ControlCommand::CompleteSplit {
                    parent: parent.id,
                    expect_epoch: parent.epoch,
                })
                .await
                .unwrap();

            {
                let inner = driving.inner.lock().await;
                let lower = inner.state.map().partition(PartitionId(10)).unwrap();
                let upper = inner.state.map().partition(PartitionId(11)).unwrap();
                assert_ne!(
                    lower.owner, upper.owner,
                    "the split did not spread, so this test proves nothing"
                );
            }
            all_caught_up(&driving, 100).await;

            driving
                .colocate_merge_parents(PartitionId(10), PartitionId(11))
                .await
                .expect("siblings always share a holder that can own both");

            let inner = driving.inner.lock().await;
            let lower = inner.state.map().partition(PartitionId(10)).unwrap();
            let upper = inner.state.map().partition(PartitionId(11)).unwrap();
            assert_eq!(lower.owner, upper.owner, "the pair is not co-located");
            assert_eq!(
                lower.replicas, upper.replicas,
                "complete_merge compares replica lists with Vec equality"
            );
        });
    }

    #[test]
    fn co_locating_a_pair_that_shares_no_holder_refuses_instead_of_copying() {
        // Ownership can only move where the data already is: a promoted holder
        // reopens the WAL directory it was already replicating into. A pair
        // with no common holder would need a copy, which is a different and
        // much slower operation than the one the operator asked for, so it is
        // refused rather than silently performed.
        let (sim, controller) = merged_cluster(201);

        let driving = controller.clone();
        sim.block_on(async move {
            let parent = {
                let inner = driving.inner.lock().await;
                let first = inner.state.map().partitions().next().unwrap().clone();
                first
            };
            driving
                .submit(ControlCommand::BeginSplit {
                    parent: parent.id,
                    at: Bytes::from_static(b"m"),
                    lower: PartitionId(10),
                    upper: PartitionId(11),
                    expect_epoch: parent.epoch,
                })
                .await
                .unwrap();
            let mut holders = vec![parent.owner.unwrap()];
            holders.extend(parent.replicas.iter().copied());
            for node in holders {
                driving
                    .submit(ControlCommand::MarkSplitPrepared {
                        parent: parent.id,
                        node,
                        expect_epoch: parent.epoch,
                    })
                    .await
                    .unwrap();
            }
            driving
                .submit(ControlCommand::CompleteSplit {
                    parent: parent.id,
                    expect_epoch: parent.epoch,
                })
                .await
                .unwrap();

            // Drive the pair to disjoint holder sets, which a split never
            // produces on its own.
            for (id, owner, replicas) in [
                (PartitionId(10), NodeId(1), vec![NodeId(2)]),
                (PartitionId(11), NodeId(3), Vec::new()),
            ] {
                let epoch = driving.epoch_of(id).await.unwrap();
                driving
                    .submit(ControlCommand::FencePartition {
                        partition: id,
                        expect_epoch: epoch,
                    })
                    .await
                    .unwrap();
                let fenced = driving.epoch_of(id).await.unwrap();
                driving
                    .submit(ControlCommand::AssignOwner {
                        partition: id,
                        owner,
                        replicas,
                        expect_epoch: fenced,
                    })
                    .await
                    .unwrap();
            }

            let refused = driving
                .colocate_merge_parents(PartitionId(10), PartitionId(11))
                .await;

            assert!(
                matches!(refused, Err(Error::InvalidArgument(_))),
                "expected a refusal, got {refused:?}"
            );
        });
    }

    #[test]
    fn a_merge_refused_before_a_version_is_agreed_never_reaches_the_log() {
        let sim = Simulation::new(125);
        let runtime = sim.add_node(NodeId(1));
        let opening = runtime.clone();
        let log = sim
            .block_on(async move { SingleNodeLog::open(&opening).await })
            .unwrap();
        let controller = Controller::new(runtime, Arc::clone(&log), ControlConfig::default());
        // Deliberately not initialised. Merge is part of 0.1 now (ADR 0012), so
        // the protocol gate no longer refuses it on a 0.1 cluster; what it
        // still refuses is a cluster that has agreed no vocabulary at all, and
        // that refusal must cost the log nothing.
        let before = sim.block_on({
            let log = Arc::clone(&log);
            async move { log.commit_index().await }
        });

        let merging = controller.clone();
        let refused = sim.block_on(async move {
            merging
                .submit(ControlCommand::BeginMerge {
                    generation: crate::MergeGeneration {
                        lower: PartitionId(1),
                        upper: PartitionId(2),
                        lower_epoch: Epoch(1),
                        upper_epoch: Epoch(1),
                        merged: PartitionId(3),
                        boundary: Bytes::from_static(b"m"),
                        range: orbita_core::KeyRange::unbounded(),
                    },
                })
                .await
        });
        let after = sim.block_on(async move { log.commit_index().await });

        assert!(matches!(refused, Err(Error::Unavailable(_))));
        assert_eq!(after, before, "a refused merge reached the log");
    }
}
