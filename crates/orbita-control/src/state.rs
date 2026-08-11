//! The replicated state machine.
//!
//! A pure function from a sequence of [`ControlCommand`]s to a cluster's
//! metadata. It reads no clock, draws no random numbers, and performs no I/O,
//! so two members that have applied the same prefix of the log hold byte
//! identical state. That is what makes it safe to put a real consensus
//! implementation underneath it later without touching anything here.
//!
//! # Where the failover ordering is enforced
//!
//! It is enforced here rather than in the code that drives a failover.
//! [`ControlCommand::AssignOwner`] is rejected for a partition that still has
//! an owner, and failover removes one only through
//! [`ControlCommand::FencePartition`], which bumps the epoch as it does so. A
//! caller cannot promote before fencing even by accident. A planned
//! [`ControlCommand::TransferOwnership`] is the narrow exception: the old
//! owner has quiesced writes and leases first, so ownership and the epoch move
//! in one entry without an unavailable interval.
//!
//! What that ordering does *not* say is that a fenced node is finished. It
//! says a node cannot own a partition at an epoch it has already been fenced
//! out of, which the epoch bump enforces on its own. A fenced owner therefore
//! stays in the replica set and can be promoted again at the new epoch, on the
//! same reported evidence as any other copy; see
//! [ADR 0008](../../../docs/adr/0008-a-fenced-owner-stays-a-replica.md) for
//! why that is the safe direction rather than the risky one.

use crate::command::{ControlCommand, MergeGeneration};
use crate::membership::{NodeHealth, NodeRole};
use crate::model::{Credential, Keyspace, KeyspaceConfig};
use crate::version::{
    lifecycle_protocol_active, ClusterVersion, CompatibilityRefusal, VersionRange, PROTOCOL_0_1,
};

use orbita_core::{
    Epoch, Error, KeyRange, KeyspaceId, KeyspaceName, MapVersion, NodeId, PartitionId,
    PartitionInfo, PartitionMap, Result,
};

use std::collections::{BTreeMap, BTreeSet};

/// A split that has begun but not finished.
///
/// While one of these exists the parent keeps its whole range and keeps
/// serving; this only records the intent and tracks which holders have
/// prepared storage for the children. The parent's map entry is retired only
/// once `prepared` covers `required`, which is the prepare-before-retire
/// ordering [PR #53](https://github.com/anomalyco/orbita/pull/53) closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingSplit {
    /// The boundary key. Kept rather than the derived ranges so the completion
    /// entry recomputes them against the parent as it stands then, which is
    /// the same range unless a merge landed in between — and a merge cannot,
    /// because a pending split blocks the parent's replica set from moving.
    pub at: bytes::Bytes,
    pub lower: PartitionId,
    pub upper: PartitionId,
    /// The parent's epoch when the split began. A failover bumps it, which is
    /// what makes every later entry in this split fail closed rather than act
    /// on a parent the cluster has already moved out from under the split.
    pub epoch: Epoch,
    /// The owner and replicas that held the parent when the split began. Every
    /// one must prepare, because any of them can be the child owner or a
    /// replica a later failover promotes.
    pub required: Vec<NodeId>,
    /// Which of `required` have acknowledged preparation.
    pub prepared: BTreeSet<NodeId>,
}

/// A split in flight, as the controller drives it and a worker prepares
/// against it.
///
/// This is the public projection of a [`PendingSplit`] joined to the parent's
/// map entry, so a worker learns which children to build, at which epoch, over
/// what range, without seeing the leader group's private bookkeeping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitIntent {
    pub parent: PartitionId,
    pub keyspace: KeyspaceId,
    /// The boundary key the parent's range splits at.
    pub at: bytes::Bytes,
    pub lower: PartitionId,
    pub upper: PartitionId,
    /// The parent's epoch when the split began; every split entry is checked
    /// against it, so a failover that bumps it aborts the split.
    pub epoch: Epoch,
    /// The epoch the children take, one above the parent's, which fences a
    /// write the old owner had in flight.
    pub child_epoch: Epoch,
    /// The owner and replicas that must each prepare child storage.
    pub required: Vec<NodeId>,
    /// Which of `required` have durably acknowledged preparation.
    pub prepared: Vec<NodeId>,
    /// The parent's current range, so a worker can derive each child's half.
    pub parent_range: KeyRange,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingMerge {
    generation: MergeGeneration,
    required: Vec<NodeId>,
    prepared: BTreeSet<NodeId>,
}

/// A dual-parent merge in flight, projected for workers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeIntent {
    pub generation: MergeGeneration,
    pub keyspace: KeyspaceId,
    pub required: Vec<NodeId>,
    pub prepared: Vec<NodeId>,
}

/// Where a partition is in its ownership lifecycle.
///
/// The distinction between `Unowned` and `Fenced` is the read lease. A
/// partition that has never had an owner has never granted a lease, so a new
/// owner can start immediately. A partition whose owner was fenced may have
/// live leases out at replicas, and per
/// [ADR 0001](../../../docs/adr/0001-linearizable-reads-from-replicas.md) the
/// new owner must not accept a write until those are gone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionPhase {
    /// Created without an owner, because the cluster had no worker to give it
    /// to. Reads and writes are unavailable and no stale value can be served.
    Unowned,
    /// Has an owner.
    Serving,
    /// The owner was fenced. Waiting out its read leases before promoting.
    Fenced {
        /// Who was fenced. Recorded for operators and for the log; the
        /// promotion rule does not read it, because the fence also demotes
        /// this node into `replicas` and it is judged there on the same
        /// reported evidence as every other copy. See
        /// [ADR 0008](../../../docs/adr/0008-a-fenced-owner-stays-a-replica.md).
        deposed: NodeId,
        /// The map version produced by the fence. Replica reports at or beyond
        /// this version have observed this partition's new epoch.
        map_version: MapVersion,
        /// Replicated so a new leader does not repeat a wait its predecessor
        /// already completed.
        drain_complete: bool,
    },
}

/// A node as the leader group records it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRecord {
    pub id: NodeId,
    pub role: NodeRole,
    pub address: String,
    pub health: NodeHealth,
    /// The cluster versions this node's binary asserted it can speak, from
    /// its registration. What `finalize-upgrade` checks a target against.
    pub speaks: VersionRange,
    /// Replicated because ownership decisions must survive a control leader
    /// change without turning process liveness into readiness.
    pub ready: bool,
    /// Draining workers keep their current ownership until each transfer
    /// commits, but are excluded from every new placement.
    pub draining: bool,
}

/// The acknowledgement a planned transfer still needs from its receiver.
///
/// This is replicated with ownership so a control leader change cannot lose
/// the narrower completion condition and fall back to waiting on the whole
/// cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HandoffCheckpoint {
    pub receiver: NodeId,
    pub map_version: MapVersion,
}

/// Everything the leader group knows, as of some prefix of the log.
#[derive(Debug, Clone, Default)]
pub struct ClusterState {
    map: PartitionMap,
    keyspaces: BTreeMap<KeyspaceId, Keyspace>,
    phases: BTreeMap<PartitionId, PartitionPhase>,
    nodes: BTreeMap<NodeId, NodeRecord>,
    /// Splits in flight, keyed by the parent being divided. A parent has at
    /// most one, because a second split is refused while the first is open.
    pending_splits: BTreeMap<PartitionId, PendingSplit>,
    /// Merges keyed by their lower parent; lookups check both parents because
    /// either one changing invalidates the generation.
    pending_merges: BTreeMap<PartitionId, PendingMerge>,
    handoffs: BTreeMap<NodeId, BTreeMap<PartitionId, HandoffCheckpoint>>,
    credentials: BTreeMap<String, Credential>,
    next_keyspace_id: u64,
    next_partition_id: u64,
    /// The active cluster version. Bootstrap sets it to the bootstrapping
    /// binary's own version in the same breath as the first keyspace. A state
    /// recovered from a 0.0 cluster legitimately holds
    /// `ClusterVersion::ZERO`, so ZERO cannot be read as "bootstrap never
    /// ran"; `version_initialized` and `is_fresh` answer that question.
    version: ClusterVersion,
    version_initialized: bool,
}

impl ClusterState {
    #[must_use]
    pub fn new() -> Self {
        Self {
            next_keyspace_id: 1,
            next_partition_id: 1,
            ..Self::default()
        }
    }

    /// The routing table, which is the only part of this that leaves the
    /// leader group.
    #[must_use]
    pub fn map(&self) -> &PartitionMap {
        &self.map
    }

    pub fn keyspaces(&self) -> impl Iterator<Item = &Keyspace> {
        self.keyspaces.values()
    }

    #[must_use]
    pub fn keyspace_by_name(&self, name: &str) -> Option<&Keyspace> {
        self.keyspaces.values().find(|k| k.name.as_str() == name)
    }

    pub fn nodes(&self) -> impl Iterator<Item = &NodeRecord> {
        self.nodes.values()
    }

    #[must_use]
    pub fn node(&self, id: NodeId) -> Option<&NodeRecord> {
        self.nodes.get(&id)
    }

    pub(crate) fn handoffs_from(
        &self,
        node: NodeId,
    ) -> Option<&BTreeMap<PartitionId, HandoffCheckpoint>> {
        self.handoffs.get(&node)
    }

    #[must_use]
    pub fn credential(&self, id: &str) -> Option<&Credential> {
        self.credentials.get(id)
    }

    pub fn credentials(&self) -> impl Iterator<Item = &Credential> {
        self.credentials.values()
    }

    #[must_use]
    pub fn phase(&self, partition: PartitionId) -> Option<PartitionPhase> {
        self.phases.get(&partition).copied()
    }

    /// Whether `partition` is the parent of a split that has begun and not
    /// finished. A child id must never appear in the map while its parent is
    /// still pending, which is the invariant the split unit tests read.
    #[must_use]
    pub fn is_splitting(&self, partition: PartitionId) -> bool {
        self.pending_splits.contains_key(&partition)
    }

    #[must_use]
    pub fn is_merging(&self, partition: PartitionId) -> bool {
        self.pending_merges
            .values()
            .any(|merge| merge.generation.lower == partition || merge.generation.upper == partition)
    }

    #[must_use]
    pub fn merge_intents(&self) -> Vec<MergeIntent> {
        self.pending_merges
            .values()
            .filter_map(|merge| {
                let lower = self.map.partition(merge.generation.lower)?;
                Some(MergeIntent {
                    generation: merge.generation.clone(),
                    keyspace: lower.keyspace,
                    required: merge.required.clone(),
                    prepared: merge.prepared.iter().copied().collect(),
                })
            })
            .collect()
    }

    /// Every split in flight, in the shape the controller drives and a worker
    /// prepares against. Carries the parent's current range so a worker can
    /// derive each child's half without a second lookup.
    #[must_use]
    pub fn split_intents(&self) -> Vec<SplitIntent> {
        self.pending_splits
            .iter()
            .filter_map(|(parent, split)| {
                let info = self.map.partition(*parent)?;
                Some(SplitIntent {
                    parent: *parent,
                    keyspace: info.keyspace,
                    at: split.at.clone(),
                    lower: split.lower,
                    upper: split.upper,
                    epoch: split.epoch,
                    child_epoch: split.epoch.next(),
                    required: split.required.clone(),
                    prepared: split.prepared.iter().copied().collect(),
                    parent_range: info.range.clone(),
                })
            })
            .collect()
    }

    /// True when nothing has ever been created, which is the condition
    /// bootstrap tests for.
    #[must_use]
    pub fn is_fresh(&self) -> bool {
        self.keyspaces.is_empty() && self.map.is_empty()
    }

    /// The next unused keyspace id, for a proposer filling in a command.
    #[must_use]
    pub fn next_keyspace_id(&self) -> KeyspaceId {
        KeyspaceId(self.next_keyspace_id)
    }

    /// The next unused partition id.
    ///
    /// Ids are never reused, including across splits and deletions, so a
    /// request naming a partition that no longer exists is always detectable
    /// rather than being silently answered by whatever took its place.
    #[must_use]
    pub fn next_partition_id(&self) -> PartitionId {
        PartitionId(self.next_partition_id)
    }

    /// Healthy workers, ordered by how little they are already carrying.
    ///
    /// Deterministic, because a placement decision made during apply would
    /// otherwise depend on iteration order and diverge between members. Ties
    /// break on node id for the same reason.
    #[must_use]
    pub fn placement_candidates(&self) -> Vec<NodeId> {
        let mut candidates: Vec<(usize, NodeId)> = self
            .nodes
            .values()
            .filter(|n| self.new_ownership_eligibility(n.id).is_ok())
            .map(|n| (self.map.held_by(n.id).count(), n.id))
            .collect();
        candidates.sort_unstable();
        candidates.into_iter().map(|(_, id)| id).collect()
    }

    /// Whether the replicated membership state permits new ownership.
    ///
    /// Every proposer uses this same predicate before emitting a command, and
    /// apply checks it again because membership may change before the command
    /// commits.
    #[must_use]
    pub(crate) fn is_eligible_owner(&self, node: NodeId) -> bool {
        self.new_ownership_eligibility(node).is_ok()
    }

    /// Whether the finalized cluster protocol carries worker lifecycle state,
    /// and so whether a node without a `ready` claim is being told apart from
    /// one that is genuinely not ready.
    ///
    /// Gated on the active cluster version for the same determinism reason as
    /// `fenced_owner_stays_a_replica` below, and it has to be, since
    /// this decides how `AssignOwner` and every other ownership command
    /// applies. A member that answered this from its own binary version would
    /// accept a committed entry its peers reject.
    ///
    /// The gate reads `>= 0.1` rather than "the cluster is on my version",
    /// which is what it used to read and what issue #105 was. Equality holds
    /// for a leader whose binary is exactly the finalized one and fails for
    /// every other binary in the n-1 window, so a cluster still on the old
    /// version put its old-binary leader on the new rule and its new-binary
    /// workers on the old one. The worker then suppressed the `ready` claim it
    /// knew the protocol could not carry, the leader read that suppression as
    /// "not ready", and the node sat in membership owning nothing for as long
    /// as the cluster stayed un-finalized. Reading the version the way every
    /// other gate reads it puts both binaries on the same rule: before 0.1 no
    /// lifecycle claim is expected of anyone, after it every node makes one.
    ///
    /// Not asking is the only available answer below 0.1, rather than a
    /// lenient one. The claim cannot be on the log for the leader to read: a
    /// `RegisterNode` carrying it encodes under a tag the previous binary
    /// truncates its log at, so recording it during the upgrade window would
    /// trade the rollback window for the placement. Below 0.1 the leader
    /// genuinely cannot tell "not ready" from "could not say", and treating
    /// silence as consent is right because that is the pre-0.1 behaviour the
    /// window promises: no readiness gate, no planned handoff, ordinary
    /// failover.
    #[must_use]
    pub fn lifecycle_enabled(&self) -> bool {
        self.version_initialized && lifecycle_protocol_active(self.version)
    }

    /// Whether the finalized cluster protocol keeps a fenced owner in the
    /// replica set, per
    /// [ADR 0008](../../../docs/adr/0008-a-fenced-owner-stays-a-replica.md).
    ///
    /// This is gated on the active cluster version for a determinism reason,
    /// not a feature-flag one. Every controller replays the same committed
    /// `FencePartition` entry, and the map stays a replicated state machine
    /// only if every member applies that one entry identically. The two
    /// binaries in a rolling upgrade do not: a pre-0.1 binary drops the
    /// deposed owner, a 0.1 binary keeps it. Deciding on the active cluster
    /// version rather than the running binary is what makes the decision the
    /// same on every member. Inside the n-1 window the active version is still
    /// the old one, so both binaries drop the owner and agree; the new rule
    /// only switches on after `finalize-upgrade`, by which point every member
    /// runs a binary that keeps it. Without this gate one committed entry
    /// would produce divergent maps, and an old binary elected during the
    /// fenced interval could serve and propose from the divergent state.
    fn fenced_owner_stays_a_replica(&self) -> bool {
        self.version_initialized && self.version >= PROTOCOL_0_1
    }

    /// Applies one command, returning the same result on every member.
    ///
    /// An error here is a decision, not a transport failure: the command was
    /// committed and it did not take effect, and every member agrees that it
    /// did not. That is why the errors are `orbita_core::Error` values a
    /// client can be told about rather than a separate internal type.
    pub fn apply(&mut self, command: &ControlCommand) -> Result<()> {
        self.ensure_command_permitted(command)?;
        match command {
            ControlCommand::RegisterNode {
                node,
                role,
                address,
                speaks,
                ready,
                draining,
            } => self.register_node(*node, *role, address, *speaks, *ready, *draining),
            ControlCommand::SetHealth { node, health } => self.set_health(*node, *health),
            ControlCommand::ForgetNode { node } => self.forget_node(*node),
            ControlCommand::CreateKeyspace { .. } => self.create_keyspace(command),
            ControlCommand::UpdateKeyspace { id, config } => self.update_keyspace(*id, config),
            ControlCommand::DeleteKeyspace { id } => self.delete_keyspace(*id),
            ControlCommand::CreateCredential { credential } => {
                self.create_credential(credential.as_ref())
            }
            ControlCommand::RevokeCredential { id } => self.revoke_credential(id),
            ControlCommand::FencePartition {
                partition,
                expect_epoch,
            } => self.fence_partition(*partition, *expect_epoch),
            ControlCommand::CompleteFenceDrain {
                partition,
                expect_epoch,
            } => self.complete_fence_drain(*partition, *expect_epoch),
            ControlCommand::AssignOwner {
                partition,
                owner,
                replicas,
                expect_epoch,
            } => self.assign_owner(*partition, *owner, replicas, *expect_epoch),
            ControlCommand::TransferOwnership {
                partition,
                from,
                to,
                replicas,
                expect_epoch,
            } => self.transfer_ownership(*partition, *from, *to, replicas, *expect_epoch),
            ControlCommand::SetReplicas {
                partition,
                replicas,
                expect_epoch,
            } => self.set_replicas(*partition, replicas, *expect_epoch),
            ControlCommand::SplitPartition {
                parent,
                at,
                lower,
                upper,
                expect_epoch,
            } => self.split_partition(*parent, at.clone(), *lower, *upper, *expect_epoch),
            ControlCommand::BeginSplit {
                parent,
                at,
                lower,
                upper,
                expect_epoch,
            } => self.begin_split(*parent, at.clone(), *lower, *upper, *expect_epoch),
            ControlCommand::MarkSplitPrepared {
                parent,
                node,
                expect_epoch,
            } => self.mark_split_prepared(*parent, *node, *expect_epoch),
            ControlCommand::CompleteSplit {
                parent,
                expect_epoch,
            } => self.complete_split(*parent, *expect_epoch),
            ControlCommand::AbortSplit {
                parent,
                expect_epoch,
            } => self.abort_split(*parent, *expect_epoch),
            ControlCommand::BeginMerge { generation } => self.begin_merge(generation.clone()),
            ControlCommand::MarkMergePrepared { generation, node } => {
                self.mark_merge_prepared(generation, *node)
            }
            ControlCommand::CompleteMerge { generation } => self.complete_merge(generation),
            ControlCommand::AbortMerge { generation } => self.abort_merge(generation),
            ControlCommand::SetClusterVersion { version, expect } => {
                self.set_cluster_version(*version, *expect)
            }
        }
    }

    pub(crate) fn ensure_command_permitted(&self, command: &ControlCommand) -> Result<()> {
        if matches!(command, ControlCommand::SplitPartition { .. })
            && self.version_initialized
            && self.version >= PROTOCOL_0_1
        {
            // The old one-entry split. Still applied on replay of a historical
            // log, but never accepted as a new proposal once the safe protocol
            // is speakable, because it retires the parent before any child has
            // storage. The worker-prepared protocol below is the replacement.
            return Err(Error::Unavailable(
                "the single-entry partition split is superseded by the worker-prepared protocol; \
                 use BeginSplit"
                    .into(),
            ));
        }
        // The protocol-0.1 commands. Refused below the active version that
        // introduced them so a historical log never carries a tag a pre-0.1
        // binary cannot decode, and a rolling upgrade never sees one member
        // apply an entry another cannot. `finalize-upgrade` is what turns them
        // on, by which point every member speaks 0.1.
        let is_protocol_0_1 = matches!(
            command,
            ControlCommand::CompleteFenceDrain { .. }
                | ControlCommand::BeginSplit { .. }
                | ControlCommand::MarkSplitPrepared { .. }
                | ControlCommand::CompleteSplit { .. }
                | ControlCommand::AbortSplit { .. }
        );
        if is_protocol_0_1 && (!self.version_initialized || self.version < PROTOCOL_0_1) {
            return Err(Error::Unavailable(
                "the worker-prepared split and replicated fence-drain require active cluster \
                 protocol 0.1"
                    .into(),
            ));
        }
        let is_protocol_0_2 = matches!(
            command,
            ControlCommand::BeginMerge { .. }
                | ControlCommand::MarkMergePrepared { .. }
                | ControlCommand::CompleteMerge { .. }
                | ControlCommand::AbortMerge { .. }
        );
        if is_protocol_0_2 {
            self.ensure_merge_permitted()?;
        }
        Ok(())
    }

    pub(crate) fn ensure_merge_permitted(&self) -> Result<()> {
        if !self.version_initialized || self.version < PROTOCOL_0_1 {
            return Err(Error::Unavailable(
                "partition merge requires an initialized cluster protocol of at least 0.1".into(),
            ));
        }
        Ok(())
    }

    fn bump_map_version(&mut self) {
        let next = self.map.version().next();
        self.map.set_version(next);
    }

    fn register_node(
        &mut self,
        node: NodeId,
        role: NodeRole,
        address: &str,
        speaks: VersionRange,
        ready: bool,
        draining: bool,
    ) -> Result<()> {
        if let Some(refusal) = self.compatibility_refusal(speaks) {
            return Err(Error::InvalidArgument(refusal.to_string()));
        }
        let entry = self.nodes.entry(node).or_insert_with(|| NodeRecord {
            id: node,
            role,
            address: address.to_string(),
            // A node that has just told us it exists has, by that fact, been
            // heard from.
            health: NodeHealth::Healthy,
            speaks,
            ready,
            draining,
        });
        entry.role = role;
        entry.address = address.to_string();
        entry.speaks = speaks;
        entry.ready = ready;
        entry.draining = draining;
        if !draining {
            // A node id may drain more than once over its lifetime. Its next
            // healthy registration starts a new handoff set rather than
            // inheriting acknowledgements from the previous process.
            self.handoffs.remove(&node);
        }
        Ok(())
    }

    /// Why a node cannot join the active cluster, if it cannot.
    ///
    /// A genuinely fresh state has no active version yet. Once any cluster
    /// state exists, including a recovered v0.0 cluster, version zero is a
    /// real active version and only a binary that speaks it may register.
    #[must_use]
    pub fn compatibility_refusal(&self, speaks: VersionRange) -> Option<CompatibilityRefusal> {
        let uninitialized = self.is_fresh() && !self.version_initialized;
        (!uninitialized && !speaks.contains(self.version)).then_some(CompatibilityRefusal {
            speaks,
            active: self.version,
        })
    }

    /// Checks the one eligibility rule used by every new ownership path.
    pub(crate) fn new_ownership_eligibility(&self, node: NodeId) -> Result<()> {
        let record = self
            .nodes
            .get(&node)
            .ok_or_else(|| Error::InvalidArgument(format!("unknown node {node}")))?;
        if record.role != NodeRole::Worker {
            return Err(Error::InvalidArgument(format!(
                "node {node} cannot receive ownership because it is not a worker"
            )));
        }
        if record.health != NodeHealth::Healthy {
            return Err(Error::InvalidArgument(format!(
                "node {node} cannot receive ownership while it is {:?}",
                record.health
            )));
        }
        if let Some(refusal) = self.compatibility_refusal(record.speaks) {
            return Err(Error::InvalidArgument(format!(
                "node {node} cannot receive ownership: {refusal}"
            )));
        }
        if self.lifecycle_enabled() && (!record.ready || record.draining) {
            return Err(Error::InvalidArgument(format!(
                "node {node} cannot receive ownership unless it is ready and not draining"
            )));
        }
        Ok(())
    }

    /// Advances the active cluster version.
    ///
    /// Backwards is refused here rather than left to the proposer, because
    /// after finalization nodes write formats the old version cannot read;
    /// a committed step backwards would be an instruction to corrupt.
    fn set_cluster_version(
        &mut self,
        version: ClusterVersion,
        expect: ClusterVersion,
    ) -> Result<()> {
        if self.version != expect {
            return Err(Error::InvalidArgument(format!(
                "the cluster version is {}, not {expect}; re-read and retry",
                self.version
            )));
        }
        if version <= self.version && self.version != ClusterVersion::ZERO {
            return Err(Error::InvalidArgument(format!(
                "the cluster version can only advance; it is {} and {version} is not newer",
                self.version
            )));
        }
        self.version = version;
        self.version_initialized = true;
        Ok(())
    }

    fn set_health(&mut self, node: NodeId, health: NodeHealth) -> Result<()> {
        let record = self
            .nodes
            .get_mut(&node)
            .ok_or_else(|| Error::InvalidArgument(format!("unknown node {node}")))?;
        record.health = health;
        Ok(())
    }

    fn forget_node(&mut self, node: NodeId) -> Result<()> {
        if self.map.held_by(node).next().is_some() {
            return Err(Error::InvalidArgument(format!(
                "node {node} still holds partitions"
            )));
        }
        self.nodes.remove(&node);
        Ok(())
    }

    /// Takes the whole command rather than eight arguments, because a
    /// positional list that long is one transposition away from creating a
    /// keyspace owned by its own partition id.
    fn create_keyspace(&mut self, command: &ControlCommand) -> Result<()> {
        let ControlCommand::CreateKeyspace {
            id,
            name,
            config,
            created_at_millis,
            first_partition,
            owner,
            replicas,
        } = command
        else {
            return Err(Error::Internal("not a create keyspace command".into()));
        };
        let (id, created_at_millis, first_partition, owner) =
            (*id, *created_at_millis, *first_partition, *owner);
        let name = KeyspaceName::new(name).map_err(|e| Error::InvalidArgument(e.to_string()))?;
        if let Some(owner) = owner {
            self.new_ownership_eligibility(owner)?;
        }
        for replica in replicas {
            self.new_ownership_eligibility(*replica)?;
        }
        if self.keyspaces.values().any(|k| k.name == name) {
            return Err(Error::KeyspaceAlreadyExists);
        }
        if self.keyspaces.contains_key(&id) || self.phases.contains_key(&first_partition) {
            // Two leaders proposed with the same allocation. The one that
            // committed first wins and this one has to be retried with fresh
            // ids rather than silently overwriting.
            return Err(Error::InvalidArgument(
                "keyspace or partition id already in use".into(),
            ));
        }

        self.keyspaces.insert(
            id,
            Keyspace {
                id,
                name,
                config: config.clone(),
                created_at_millis,
            },
        );
        self.next_keyspace_id = self.next_keyspace_id.max(id.get() + 1);

        let keyspace = &self.keyspaces[&id];
        self.map.insert_keyspace(keyspace.info());

        // The whole point of this command: a keyspace is born covered. One
        // unbounded partition owns every key in it from the first instant it
        // exists, so `check_coverage` holds after every entry rather than
        // after some of them.
        self.map.insert_partition(PartitionInfo {
            id: first_partition,
            keyspace: id,
            range: KeyRange::unbounded(),
            owner,
            epoch: Epoch(1),
            replicas: replicas.clone(),
        });
        self.phases.insert(
            first_partition,
            if owner.is_some() {
                PartitionPhase::Serving
            } else {
                PartitionPhase::Unowned
            },
        );
        self.next_partition_id = self.next_partition_id.max(first_partition.get() + 1);
        self.bump_map_version();
        Ok(())
    }

    fn update_keyspace(&mut self, id: KeyspaceId, config: &KeyspaceConfig) -> Result<()> {
        let keyspace = self.keyspaces.get_mut(&id).ok_or(Error::KeyspaceNotFound)?;
        keyspace.config = config.clone();
        let info = keyspace.info();
        self.map.insert_keyspace(info);
        self.bump_map_version();
        Ok(())
    }

    fn delete_keyspace(&mut self, id: KeyspaceId) -> Result<()> {
        if self.keyspaces.remove(&id).is_none() {
            return Err(Error::KeyspaceNotFound);
        }
        let doomed: Vec<PartitionId> = self
            .map
            .partitions()
            .filter(|p| p.keyspace == id)
            .map(|p| p.id)
            .collect();
        for partition in &doomed {
            self.phases.remove(partition);
            self.pending_splits.remove(partition);
        }
        self.pending_merges.retain(|_, merge| {
            !doomed.contains(&merge.generation.lower) && !doomed.contains(&merge.generation.upper)
        });
        // `PartitionMap` can add a keyspace but not remove one, so the map is
        // rebuilt from the tables this crate owns. See the note in the crate
        // documentation about the contract gap.
        self.rebuild_map_without(id);
        self.bump_map_version();
        Ok(())
    }

    fn rebuild_map_without(&mut self, dropped: KeyspaceId) {
        let kept: Vec<PartitionInfo> = self
            .map
            .partitions()
            .filter(|p| p.keyspace != dropped)
            .cloned()
            .collect();
        let mut rebuilt = PartitionMap::new(self.map.version());
        for keyspace in self.keyspaces.values() {
            rebuilt.insert_keyspace(keyspace.info());
        }
        for partition in kept {
            rebuilt.insert_partition(partition);
        }
        self.map = rebuilt;
    }

    fn create_credential(&mut self, credential: &Credential) -> Result<()> {
        if self.credentials.contains_key(&credential.id) {
            return Err(Error::AlreadyExists);
        }
        if credential.keyspaces.is_empty() {
            return Err(Error::InvalidArgument(
                "a credential scoped to no keyspace can never be used".into(),
            ));
        }
        self.credentials
            .insert(credential.id.clone(), credential.clone());
        Ok(())
    }

    fn revoke_credential(&mut self, id: &str) -> Result<()> {
        if self.credentials.remove(id).is_none() {
            return Err(Error::NotFound);
        }
        Ok(())
    }

    fn partition_or_err(&self, partition: PartitionId) -> Result<&PartitionInfo> {
        self.map
            .partition(partition)
            .ok_or_else(|| Error::InvalidArgument(format!("no partition {partition}")))
    }

    fn check_epoch(&self, partition: PartitionId, expect: Epoch) -> Result<PartitionInfo> {
        let info = self.partition_or_err(partition)?;
        if info.epoch != expect {
            return Err(Error::StaleEpoch {
                partition,
                got: expect,
                current: info.epoch,
            });
        }
        Ok(info.clone())
    }

    fn replace_partition(&mut self, info: PartitionInfo) {
        // Removing first is not optional: a partition whose range start moved
        // is a different key in the map, and inserting alone would leave the
        // old entry behind as an overlap.
        let start = info.range.start().to_vec();
        self.map.remove_partition(info.keyspace, &start);
        self.map.insert_partition(info);
    }

    fn fence_partition(&mut self, partition: PartitionId, expect_epoch: Epoch) -> Result<()> {
        let mut info = self.check_epoch(partition, expect_epoch)?;
        let Some(deposed) = info.owner else {
            return Err(Error::InvalidArgument(format!(
                "partition {partition} has no owner to fence"
            )));
        };
        self.abort_merge_for_parent_change(partition);
        // The epoch bump and the ownership removal are the same entry. There
        // is no committed state in which the old owner has been removed but
        // its epoch still stands, which is the state a returning owner could
        // have written at.
        info.owner = None;
        info.epoch = info.epoch.next();
        // The deposed owner is demoted into the replica set rather than
        // dropped out of the map, per
        // [ADR 0008](../../../docs/adr/0008-a-fenced-owner-stays-a-replica.md).
        // It is a member of every durability quorum it ever counted, so its
        // disk holds every write this partition has acknowledged; taking it
        // out of `replicas` threw away the record of the one copy the cluster
        // is certain about. Keeping it there is what lets the promotion rule
        // below see it, and what stops a partition whose other copies are gone
        // from being fenced with nothing named on it at all (issue #76).
        //
        // This does not un-fence anything. The epoch above this line is what
        // stops the deposed owner writing, and it is still bumped in the same
        // entry; a replica is a node that takes appends from an owner, not a
        // node that may serve as one.
        //
        // The demotion is gated on the active cluster version because it is a
        // change to how a committed entry is applied, and every member must
        // apply the same entry the same way or the map diverges. Until
        // `finalize-upgrade` moves the active version to 0.1 the fence drops
        // the deposed owner on every member, old binary and new alike; see
        // `fenced_owner_stays_a_replica` for why that is what keeps the
        // rolling upgrade window deterministic.
        if self.fenced_owner_stays_a_replica() && !info.replicas.contains(&deposed) {
            info.replicas.push(deposed);
        }
        // A fence moves the epoch and the ownership out from under any split in
        // progress, so the split can no longer be completed against them.
        // Dropping it here is the cleanup; the epoch bump alone already makes
        // every remaining split entry fail closed, so this only stops a dead
        // pending record from lingering.
        self.pending_splits.remove(&partition);
        self.replace_partition(info);
        self.bump_map_version();
        self.phases.insert(
            partition,
            PartitionPhase::Fenced {
                deposed,
                map_version: self.map.version(),
                drain_complete: false,
            },
        );
        Ok(())
    }

    fn complete_fence_drain(&mut self, partition: PartitionId, expect_epoch: Epoch) -> Result<()> {
        self.check_epoch(partition, expect_epoch)?;
        let Some(PartitionPhase::Fenced { drain_complete, .. }) = self.phases.get_mut(&partition)
        else {
            return Err(Error::InvalidArgument(format!(
                "partition {partition} is not waiting on fenced-owner leases"
            )));
        };
        *drain_complete = true;
        Ok(())
    }

    fn assign_owner(
        &mut self,
        partition: PartitionId,
        owner: NodeId,
        replicas: &[NodeId],
        expect_epoch: Epoch,
    ) -> Result<()> {
        let mut info = self.check_epoch(partition, expect_epoch)?;
        if info.owner.is_some() {
            // This is the split brain guard. Promoting on top of a live owner
            // is refused outright, so the only path to a new owner runs
            // through a fence, and a fence always bumps the epoch.
            return Err(Error::InvalidArgument(format!(
                "partition {partition} already has an owner; fence it first"
            )));
        }
        if replicas.contains(&owner) {
            return Err(Error::InvalidArgument(
                "the owner must not also be listed as a replica".into(),
            ));
        }
        self.new_ownership_eligibility(owner)?;
        for replica in replicas {
            if !info.replicas.contains(replica) {
                self.new_ownership_eligibility(*replica)?;
            }
        }
        info.owner = Some(owner);
        info.replicas = replicas.to_vec();
        self.replace_partition(info);
        self.phases.insert(partition, PartitionPhase::Serving);
        self.bump_map_version();
        Ok(())
    }

    fn transfer_ownership(
        &mut self,
        partition: PartitionId,
        from: NodeId,
        to: NodeId,
        replicas: &[NodeId],
        expect_epoch: Epoch,
    ) -> Result<()> {
        let mut info = self.check_epoch(partition, expect_epoch)?;
        if info.owner != Some(from) {
            return Err(Error::InvalidArgument(format!(
                "node {from} is not the owner of partition {partition}"
            )));
        }
        if !info.replicas.contains(&to) {
            return Err(Error::InvalidArgument(format!(
                "node {to} is not a replica of partition {partition}"
            )));
        }
        self.new_ownership_eligibility(to)?;
        if replicas.contains(&to) {
            return Err(Error::InvalidArgument(
                "the owner must not also be listed as a replica".into(),
            ));
        }
        self.abort_merge_for_parent_change(partition);

        info.owner = Some(to);
        info.epoch = info.epoch.next();
        info.replicas = replicas.to_vec();
        self.replace_partition(info);
        self.phases.insert(partition, PartitionPhase::Serving);
        self.bump_map_version();
        self.handoffs.entry(from).or_default().insert(
            partition,
            HandoffCheckpoint {
                receiver: to,
                map_version: self.map.version(),
            },
        );
        Ok(())
    }

    fn set_replicas(
        &mut self,
        partition: PartitionId,
        replicas: &[NodeId],
        expect_epoch: Epoch,
    ) -> Result<()> {
        let mut info = self.check_epoch(partition, expect_epoch)?;
        // A pending split snapshotted the current holders as the set that must
        // prepare its children. If a required replica dies, repair wants to
        // replace it — but that replacement can never be one of the holders the
        // split is still waiting on, so a split held open against a dead
        // replica would wedge forever. A replica-set change therefore *aborts*
        // the split rather than being blocked by it. The parent still covers
        // its whole range, so nothing is lost; the split is simply reopened
        // once the set has settled. This is the P1-review's P2.
        //
        // Abandoning it here carries the same epoch bump `abort_split` does,
        // and for the same reason: a parent that was quiesced for the split has
        // handed Lamports back, and it cannot reissue them at an epoch its
        // replicas already hold. Every path that drops a pending split bumps
        // the parent's epoch — this one, `abort_split`, and `fence_partition` —
        // so a holder can treat "my split is gone" and "my epoch moved" as one
        // event. Conditioned on there having *been* a split, so an ordinary
        // replica placement still costs no reopen.
        let abandoned_split = self.pending_splits.remove(&partition).is_some();
        let abandoned_merge = self.abort_merge_for_parent_change(partition);
        if abandoned_split || abandoned_merge {
            info.epoch = info.epoch.next();
        }
        if info.owner.is_some_and(|o| replicas.contains(&o)) {
            return Err(Error::InvalidArgument(
                "the owner must not also be listed as a replica".into(),
            ));
        }
        for replica in replicas {
            if !info.replicas.contains(replica) {
                self.new_ownership_eligibility(*replica)?;
            }
        }
        info.replicas = replicas.to_vec();
        self.replace_partition(info);
        self.bump_map_version();
        Ok(())
    }

    fn split_partition(
        &mut self,
        parent: PartitionId,
        at: bytes::Bytes,
        lower: PartitionId,
        upper: PartitionId,
        expect_epoch: Epoch,
    ) -> Result<()> {
        let info = self.check_epoch(parent, expect_epoch)?;
        if self.phases.contains_key(&lower) || self.phases.contains_key(&upper) {
            return Err(Error::InvalidArgument(
                "a child partition id is already in use".into(),
            ));
        }
        let (low_range, high_range) = info.range.split_at(at).ok_or_else(|| {
            Error::InvalidArgument(format!(
                "the split key is not inside partition {parent}'s range"
            ))
        })?;

        // Both children start one epoch above the parent so that a write the
        // old owner had in flight against the parent is fenced by the split
        // itself, rather than landing in whichever child happens to contain
        // its key.
        let epoch = info.epoch.next();
        let start = info.range.start().to_vec();
        self.map.remove_partition(info.keyspace, &start);
        for (id, range) in [(lower, low_range), (upper, high_range)] {
            self.map.insert_partition(PartitionInfo {
                id,
                keyspace: info.keyspace,
                range,
                owner: info.owner,
                epoch,
                replicas: info.replicas.clone(),
            });
            self.phases.insert(
                id,
                if info.owner.is_some() {
                    PartitionPhase::Serving
                } else {
                    PartitionPhase::Unowned
                },
            );
        }
        self.phases.remove(&parent);
        self.next_partition_id = self
            .next_partition_id
            .max(lower.get() + 1)
            .max(upper.get() + 1);
        self.bump_map_version();
        Ok(())
    }

    /// Opens a worker-prepared split without moving any key.
    ///
    /// The parent stays in the map with its whole range and keeps serving. All
    /// this does is record the intent and snapshot the holders that must
    /// prepare. The map version bumps so those holders notice, which is the
    /// only signal a worker gets to start building child storage while the
    /// partition table still says the parent owns everything.
    fn begin_split(
        &mut self,
        parent: PartitionId,
        at: bytes::Bytes,
        lower: PartitionId,
        upper: PartitionId,
        expect_epoch: Epoch,
    ) -> Result<()> {
        let info = self.check_epoch(parent, expect_epoch)?;
        if self.pending_splits.contains_key(&parent) {
            return Err(Error::InvalidArgument(format!(
                "partition {parent} is already splitting"
            )));
        }
        if self.is_merging(parent) {
            return Err(Error::InvalidArgument(format!(
                "partition {parent} is already merging"
            )));
        }
        // A partition with no owner has no node holding its data, so there is
        // nobody to prepare the children. It becomes splittable the moment it
        // is placed; until then the honest answer is that it is unavailable.
        let Some(owner) = info.owner else {
            return Err(Error::InvalidArgument(format!(
                "partition {parent} has no owner to prepare child storage; place it first"
            )));
        };
        if lower == upper || self.child_id_in_use(lower) || self.child_id_in_use(upper) {
            return Err(Error::InvalidArgument(
                "a child partition id is already in use".into(),
            ));
        }
        // Validate the boundary against the parent's range without mutating
        // anything. The clone is cheap and keeps `at` for the pending record,
        // which the completion entry re-splits against the range as it stands
        // then rather than trusting ranges computed a whole protocol ago.
        info.range.clone().split_at(at.clone()).ok_or_else(|| {
            Error::InvalidArgument(format!(
                "the split key is not inside partition {parent}'s range"
            ))
        })?;

        let mut required = vec![owner];
        required.extend(info.replicas.iter().copied());
        // Reserve the child ids so a concurrent keyspace creation cannot take
        // one before the split completes, the same way every other id is never
        // reused.
        self.next_partition_id = self
            .next_partition_id
            .max(lower.get() + 1)
            .max(upper.get() + 1);
        // Bump the map version so holders notice the split has begun even
        // though the partition table is unchanged; that is the only signal a
        // worker gets to start preparing child storage.
        self.bump_map_version();
        self.pending_splits.insert(
            parent,
            PendingSplit {
                at,
                lower,
                upper,
                epoch: info.epoch,
                required,
                prepared: BTreeSet::new(),
            },
        );
        Ok(())
    }

    /// Whether `id` is spoken for as a live partition or as another split's
    /// pending child.
    fn child_id_in_use(&self, id: PartitionId) -> bool {
        self.phases.contains_key(&id)
            || self
                .pending_splits
                .values()
                .any(|split| split.lower == id || split.upper == id)
            || self
                .pending_merges
                .values()
                .any(|merge| merge.generation.merged == id)
    }

    /// Records that one holder has prepared storage for a pending split's
    /// children.
    fn mark_split_prepared(
        &mut self,
        parent: PartitionId,
        node: NodeId,
        expect_epoch: Epoch,
    ) -> Result<()> {
        self.check_epoch(parent, expect_epoch)?;
        let split = self.pending_splits.get_mut(&parent).ok_or_else(|| {
            Error::InvalidArgument(format!("partition {parent} is not splitting"))
        })?;
        if !split.required.contains(&node) {
            return Err(Error::InvalidArgument(format!(
                "node {node} does not hold partition {parent} and cannot prepare its split"
            )));
        }
        split.prepared.insert(node);
        Ok(())
    }

    /// Retires the parent and installs both children, but only once every
    /// holder has prepared.
    ///
    /// This is the entry the whole protocol exists to gate. Refusing it until
    /// `prepared` covers `required` is what guarantees no child ever appears in
    /// the map before a node has storage for it, and doing the swap in a single
    /// entry is what keeps coverage unbroken across the retirement.
    fn complete_split(&mut self, parent: PartitionId, expect_epoch: Epoch) -> Result<()> {
        let info = self.check_epoch(parent, expect_epoch)?;
        let split = self.pending_splits.get(&parent).ok_or_else(|| {
            Error::InvalidArgument(format!("partition {parent} is not splitting"))
        })?;
        let unprepared: Vec<NodeId> = split
            .required
            .iter()
            .copied()
            .filter(|node| !split.prepared.contains(node))
            .collect();
        if !unprepared.is_empty() {
            // Unavailable rather than InvalidArgument: the request is well
            // formed and will succeed once the named holders prepare, which is
            // a wait, not a mistake.
            return Err(Error::Unavailable(format!(
                "partition {parent} cannot retire until these holders prepare child storage: \
                 {unprepared:?}"
            )));
        }
        let (at, lower, upper) = (split.at.clone(), split.lower, split.upper);
        let (low_range, high_range) = info.range.clone().split_at(at).ok_or_else(|| {
            // The range was validated at begin and the pending split blocks the
            // replica set from moving, so a parent whose range no longer holds
            // the key is an internal inconsistency rather than a caller error.
            Error::Internal(format!(
                "the boundary of the split on partition {parent} left its range"
            ))
        })?;

        // Both children start one epoch above the parent so a write the old
        // owner had in flight against the parent is fenced by the split rather
        // than landing in whichever child holds its key. The keys keep their
        // versions: a split does not merge two sequences, so no key's Lamport
        // moves and none is reissued, which is what ADR 0002 requires.
        let epoch = info.epoch.next();
        let start = info.range.start().to_vec();
        self.pending_splits.remove(&parent);
        self.map.remove_partition(info.keyspace, &start);
        for (id, range) in [(lower, low_range), (upper, high_range)] {
            self.map.insert_partition(PartitionInfo {
                id,
                keyspace: info.keyspace,
                range,
                owner: info.owner,
                epoch,
                replicas: info.replicas.clone(),
            });
            self.phases.insert(
                id,
                if info.owner.is_some() {
                    PartitionPhase::Serving
                } else {
                    PartitionPhase::Unowned
                },
            );
        }
        self.phases.remove(&parent);
        self.next_partition_id = self
            .next_partition_id
            .max(lower.get() + 1)
            .max(upper.get() + 1);
        self.bump_map_version();
        Ok(())
    }

    /// Abandons a pending split, leaving the parent covering its whole range at
    /// a new epoch.
    ///
    /// The epoch bump is the point of this entry, not bookkeeping around it,
    /// and it is in the same entry as the abandonment for the same reason
    /// [`ClusterState::fence_partition`] puts one there: there must be no
    /// committed state in which the split is off and the parent's epoch still
    /// stands.
    ///
    /// The owner quiesced its log to prepare the children, which hands back
    /// every Lamport above the committed prefix. Those Lamports can still be on
    /// a replica — the append landed and the reply was lost — and the reopened
    /// parent resumes assigning from the prefix, so it is about to issue them
    /// again with different bytes under them. A replica gives its tail up for a
    /// strictly higher epoch and nothing else; at an unchanged epoch it would
    /// skip the replacement as a retransmission without comparing bytes,
    /// acknowledge it, and go on holding the original. That is two values at
    /// one version, a divergent replica read, and the replacement lost if that
    /// replica is later promoted. The bump is what lets the reopening owner
    /// fence its replicas back to the horizon it resumes from.
    ///
    /// Ownership is untouched, unlike a fence: the parent was never in doubt,
    /// only its split was. The holder reopens itself at the new epoch through
    /// the ordinary reconcile path.
    fn abort_split(&mut self, parent: PartitionId, expect_epoch: Epoch) -> Result<()> {
        let mut info = self.check_epoch(parent, expect_epoch)?;
        if self.pending_splits.remove(&parent).is_none() {
            return Err(Error::InvalidArgument(format!(
                "partition {parent} is not splitting"
            )));
        }
        info.epoch = info.epoch.next();
        self.replace_partition(info);
        self.bump_map_version();
        Ok(())
    }

    fn begin_merge(&mut self, generation: MergeGeneration) -> Result<()> {
        if generation.lower == generation.upper
            || generation.lower == generation.merged
            || generation.upper == generation.merged
        {
            return Err(Error::InvalidArgument(
                "merge parents and child must have distinct ids".into(),
            ));
        }
        let lower = self.check_epoch(generation.lower, generation.lower_epoch)?;
        let upper = self.check_epoch(generation.upper, generation.upper_epoch)?;
        if lower.keyspace != upper.keyspace {
            return Err(Error::InvalidArgument(
                "merge parents must belong to one keyspace".into(),
            ));
        }
        let range = KeyRange::merge(&lower.range, &upper.range).ok_or_else(|| {
            Error::InvalidArgument("merge parents must be adjacent and in lower/upper order".into())
        })?;
        if range != generation.range
            || lower.range.end() != Some(generation.boundary.as_ref())
            || upper.range.start() != generation.boundary.as_ref()
        {
            return Err(Error::InvalidArgument(
                "merge generation does not match the parents' boundary and range".into(),
            ));
        }
        if lower.owner.is_none()
            || lower.owner != upper.owner
            || lower.replicas != upper.replicas
            || self.phase(lower.id) != Some(PartitionPhase::Serving)
            || self.phase(upper.id) != Some(PartitionPhase::Serving)
        {
            return Err(Error::InvalidArgument(
                "merge parents must be serving on the same owner and replica set".into(),
            ));
        }
        if self.is_splitting(lower.id)
            || self.is_splitting(upper.id)
            || self.is_merging(lower.id)
            || self.is_merging(upper.id)
        {
            return Err(Error::InvalidArgument(
                "a split or merge is already active on one parent".into(),
            ));
        }
        if self.child_id_in_use(generation.merged) {
            return Err(Error::InvalidArgument(
                "the merged partition id is already in use".into(),
            ));
        }

        let mut required = vec![lower.owner.expect("checked")];
        required.extend(lower.replicas.iter().copied());
        self.next_partition_id = self.next_partition_id.max(generation.merged.get() + 1);
        self.pending_merges.insert(
            generation.lower,
            PendingMerge {
                generation,
                required,
                prepared: BTreeSet::new(),
            },
        );
        self.bump_map_version();
        Ok(())
    }

    fn pending_merge(&self, generation: &MergeGeneration) -> Result<&PendingMerge> {
        self.pending_merges
            .get(&generation.lower)
            .filter(|pending| pending.generation == *generation)
            .ok_or_else(|| Error::InvalidArgument("merge generation is not active".into()))
    }

    fn mark_merge_prepared(&mut self, generation: &MergeGeneration, node: NodeId) -> Result<()> {
        self.check_epoch(generation.lower, generation.lower_epoch)?;
        self.check_epoch(generation.upper, generation.upper_epoch)?;
        let pending = self
            .pending_merges
            .get_mut(&generation.lower)
            .filter(|pending| pending.generation == *generation)
            .ok_or_else(|| Error::InvalidArgument("merge generation is not active".into()))?;
        if !pending.required.contains(&node) {
            return Err(Error::InvalidArgument(format!(
                "node {node} is not a required holder for this merge"
            )));
        }
        pending.prepared.insert(node);
        Ok(())
    }

    fn complete_merge(&mut self, generation: &MergeGeneration) -> Result<()> {
        let lower = self.check_epoch(generation.lower, generation.lower_epoch)?;
        let upper = self.check_epoch(generation.upper, generation.upper_epoch)?;
        let pending = self.pending_merge(generation)?;
        let unprepared: Vec<_> = pending
            .required
            .iter()
            .copied()
            .filter(|node| !pending.prepared.contains(node))
            .collect();
        if !unprepared.is_empty() {
            return Err(Error::Unavailable(format!(
                "merge cannot complete until these holders prepare storage: {unprepared:?}"
            )));
        }
        let range = KeyRange::merge(&lower.range, &upper.range)
            .ok_or_else(|| Error::Internal("active merge parents are no longer adjacent".into()))?;
        if range != generation.range
            || lower.owner != upper.owner
            || lower.replicas != upper.replicas
        {
            return Err(Error::Unavailable(
                "merge parents changed after preparation began".into(),
            ));
        }

        self.pending_merges.remove(&generation.lower);
        self.map
            .remove_partition(lower.keyspace, lower.range.start());
        self.map
            .remove_partition(upper.keyspace, upper.range.start());
        let epoch = Epoch(lower.epoch.get().max(upper.epoch.get())).next();
        self.map.insert_partition(PartitionInfo {
            id: generation.merged,
            keyspace: lower.keyspace,
            range,
            owner: lower.owner,
            epoch,
            replicas: lower.replicas,
        });
        self.phases.remove(&lower.id);
        self.phases.remove(&upper.id);
        self.phases
            .insert(generation.merged, PartitionPhase::Serving);
        self.bump_map_version();
        Ok(())
    }

    fn abort_merge(&mut self, generation: &MergeGeneration) -> Result<()> {
        self.pending_merge(generation)?;
        let mut lower = self.check_epoch(generation.lower, generation.lower_epoch)?;
        let mut upper = self.check_epoch(generation.upper, generation.upper_epoch)?;
        self.pending_merges.remove(&generation.lower);
        lower.epoch = lower.epoch.next();
        upper.epoch = upper.epoch.next();
        self.replace_partition(lower);
        self.replace_partition(upper);
        self.bump_map_version();
        Ok(())
    }

    /// Drops a merge whose holder or owner is changing and raises the other
    /// parent's epoch. The caller raises `partition` as part of its own command
    /// (fence, transfer, or replica repair), so doing only the counterpart here
    /// makes abandonment and both epoch changes one replicated state-machine
    /// application without double-bumping the changed side.
    fn abort_merge_for_parent_change(&mut self, partition: PartitionId) -> bool {
        let Some((key, generation)) = self.pending_merges.iter().find_map(|(key, pending)| {
            (pending.generation.lower == partition || pending.generation.upper == partition)
                .then(|| (*key, pending.generation.clone()))
        }) else {
            return false;
        };
        self.pending_merges.remove(&key);
        let other = if generation.lower == partition {
            generation.upper
        } else {
            generation.lower
        };
        if let Some(mut info) = self.map.partition(other).cloned() {
            info.epoch = info.epoch.next();
            self.replace_partition(info);
        }
        true
    }

    /// The map version, exposed so a caller can tell whether anything moved.
    #[must_use]
    pub fn map_version(&self) -> MapVersion {
        self.map.version()
    }

    /// The active cluster version: what every node has agreed to speak, and
    /// the thing `finalize-upgrade` advances.
    #[must_use]
    pub fn cluster_version(&self) -> ClusterVersion {
        self.version
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::KeyspaceConfig;
    use bytes::Bytes;

    fn worker(id: u64) -> ControlCommand {
        ControlCommand::RegisterNode {
            node: NodeId(id),
            role: NodeRole::Worker,
            address: format!("10.0.0.{id}:7000"),
            // Most state tests exercise routing rather than compatibility and
            // move the active version explicitly. Keep their fixture eligible
            // across that range; compatibility tests below register exact
            // production windows with `register_with`.
            speaks: VersionRange::new(ClusterVersion::ZERO, crate::version::binary_speaks().max),
            ready: true,
            draining: false,
        }
    }

    fn set_version(state: &mut ClusterState, version: ClusterVersion) {
        state
            .apply(&ControlCommand::SetClusterVersion {
                version,
                expect: ClusterVersion::ZERO,
            })
            .unwrap();
    }

    /// Registers a worker whose only interesting property is the range it
    /// speaks. It claims readiness, because these are compatibility tests and
    /// a node held out of placement for being unready would prove the version
    /// rule by accident.
    fn register_with(state: &mut ClusterState, id: u64, speaks: VersionRange) -> Result<()> {
        state.apply(&ControlCommand::RegisterNode {
            node: NodeId(id),
            role: NodeRole::Worker,
            address: format!("10.0.0.{id}:7000"),
            speaks,
            ready: true,
            draining: false,
        })
    }

    /// A cluster with three workers and one keyspace covered by one partition.
    fn bootstrapped() -> ClusterState {
        let mut state = ClusterState::new();
        for id in 1..=3 {
            state.apply(&worker(id)).unwrap();
        }
        let candidates = state.placement_candidates();
        state
            .apply(&ControlCommand::CreateKeyspace {
                id: state.next_keyspace_id(),
                name: "default".into(),
                config: KeyspaceConfig::default(),
                created_at_millis: 1,
                first_partition: state.next_partition_id(),
                owner: Some(candidates[0]),
                replicas: candidates[1..3].to_vec(),
            })
            .unwrap();
        state
    }

    #[test]
    fn a_new_keyspace_is_covered_by_one_unbounded_partition_from_the_first_instant() {
        let state = bootstrapped();
        assert_eq!(state.map().check_coverage(), Ok(()));
        assert_eq!(state.map().len(), 1);

        let ks = state.keyspace_by_name("default").unwrap().id;
        let partition = state.map().lookup(ks, b"anything").expect("covered");
        assert!(partition.owner.is_some(), "and it has an owner");
        assert_eq!(partition.epoch, Epoch(1));
        assert_eq!(partition.replicas.len(), 2);
    }

    #[test]
    fn a_keyspace_created_with_no_workers_is_still_fully_covered() {
        // Unavailable is a state the system knows how to describe. A hole in
        // the map is not.
        let mut state = ClusterState::new();
        state
            .apply(&ControlCommand::CreateKeyspace {
                id: KeyspaceId(1),
                name: "default".into(),
                config: KeyspaceConfig::default(),
                created_at_millis: 1,
                first_partition: PartitionId(1),
                owner: None,
                replicas: vec![],
            })
            .unwrap();

        assert_eq!(state.map().check_coverage(), Ok(()));
        assert_eq!(state.phase(PartitionId(1)), Some(PartitionPhase::Unowned));
    }

    #[test]
    fn a_duplicate_keyspace_name_is_rejected() {
        let mut state = bootstrapped();
        let err = state.apply(&ControlCommand::CreateKeyspace {
            id: state.next_keyspace_id(),
            name: "default".into(),
            config: KeyspaceConfig::default(),
            created_at_millis: 2,
            first_partition: state.next_partition_id(),
            owner: None,
            replicas: vec![],
        });
        assert_eq!(err, Err(Error::KeyspaceAlreadyExists));
    }

    #[test]
    fn an_owner_cannot_be_replaced_without_fencing_the_old_one_first() {
        let mut state = bootstrapped();
        let partition = state.map().partitions().next().unwrap().clone();

        let err = state.apply(&ControlCommand::AssignOwner {
            partition: partition.id,
            owner: partition.replicas[0],
            replicas: vec![],
            expect_epoch: partition.epoch,
        });
        assert!(
            err.is_err(),
            "promoting on top of a live owner is a split brain and must not apply"
        );
    }

    #[test]
    fn fencing_bumps_the_epoch_and_leaves_the_partition_ownerless() {
        let mut state = bootstrapped();
        let before = state.map().partitions().next().unwrap().clone();

        state
            .apply(&ControlCommand::FencePartition {
                partition: before.id,
                expect_epoch: before.epoch,
            })
            .unwrap();

        let after = state.map().partition(before.id).unwrap();
        assert_eq!(after.epoch, before.epoch.next());
        assert_eq!(after.owner, None);
        assert_eq!(
            state.phase(before.id),
            Some(PartitionPhase::Fenced {
                deposed: before.owner.unwrap(),
                map_version: state.map().version(),
                drain_complete: false,
            })
        );
    }

    #[test]
    fn fencing_demotes_the_deposed_owner_into_the_replica_set() {
        // The map has to keep naming the node that certainly holds the data.
        // It is a member of every durability quorum it counted, so dropping it
        // threw away the record of the one copy the cluster is sure about,
        // which is what left issue #76's partitions unrecoverable. Under the
        // finalized 0.1 protocol, which is the world this behaviour ships in.
        let mut state = bootstrapped();
        set_version(&mut state, PROTOCOL_0_1);
        let before = state.map().partitions().next().unwrap().clone();
        let deposed = before.owner.unwrap();

        state
            .apply(&ControlCommand::FencePartition {
                partition: before.id,
                expect_epoch: before.epoch,
            })
            .unwrap();

        let after = state.map().partition(before.id).unwrap();
        assert!(after.replicas.contains(&deposed));
        for replica in &before.replicas {
            assert!(after.replicas.contains(replica), "and nothing else moves");
        }
    }

    #[test]
    fn an_unfinalized_cluster_still_drops_the_deposed_owner_when_it_fences() {
        // The determinism guarantee across a rolling upgrade. The demotion is
        // a change to how a committed `FencePartition` entry is applied, so it
        // must not take effect until the active cluster version has finalized
        // to the protocol that introduced it. Inside the n-1 window the active
        // version is still the old one, and every member — old binary and new
        // — has to apply the entry the old way or the map diverges from a
        // single committed entry. Here the version is left below 0.1, so the
        // fence drops the deposed owner exactly as a pre-fix binary does.
        let mut state = bootstrapped();
        set_version(&mut state, ClusterVersion::ZERO);
        let before = state.map().partitions().next().unwrap().clone();
        let deposed = before.owner.unwrap();

        state
            .apply(&ControlCommand::FencePartition {
                partition: before.id,
                expect_epoch: before.epoch,
            })
            .unwrap();

        let after = state.map().partition(before.id).unwrap();
        assert!(
            !after.replicas.contains(&deposed),
            "an unfinalized cluster applies the pre-0.1 fence, which drops the deposed owner"
        );
        assert_eq!(
            after.replicas, before.replicas,
            "and the replica set is otherwise untouched"
        );
    }

    #[test]
    fn a_fence_never_leaves_a_partition_with_nothing_named_on_it() {
        // The route issue #76 was reported through: both replicas are retired
        // while the owner is still serving, so the fence lands on an empty
        // set. With an empty set the owner was acknowledging writes on its own
        // disk alone, which makes it the only node that can be promoted
        // without losing them.
        let mut state = bootstrapped();
        set_version(&mut state, PROTOCOL_0_1);
        let before = state.map().partitions().next().unwrap().clone();
        let deposed = before.owner.unwrap();
        state
            .apply(&ControlCommand::SetReplicas {
                partition: before.id,
                replicas: vec![],
                expect_epoch: before.epoch,
            })
            .unwrap();

        state
            .apply(&ControlCommand::FencePartition {
                partition: before.id,
                expect_epoch: before.epoch,
            })
            .unwrap();

        assert_eq!(
            state.map().partition(before.id).unwrap().replicas,
            vec![deposed]
        );
    }

    #[test]
    fn a_deposed_owner_can_be_given_the_partition_back_at_the_epoch_that_fenced_it() {
        // Promotion is not blocked by identity. What stops the old
        // incarnation writing is the epoch, and the epoch has already moved,
        // so the same node owning the partition again is a strictly later
        // incarnation than the one that was fenced. Under the finalized 0.1
        // protocol the fence leaves it in the replica set, so this exercises
        // giving the partition back to a node that is standing there as a
        // candidate.
        let mut state = bootstrapped();
        set_version(&mut state, PROTOCOL_0_1);
        let before = state.map().partitions().next().unwrap().clone();
        let deposed = before.owner.unwrap();
        state
            .apply(&ControlCommand::FencePartition {
                partition: before.id,
                expect_epoch: before.epoch,
            })
            .unwrap();
        let fenced_epoch = state.map().partition(before.id).unwrap().epoch;

        state
            .apply(&ControlCommand::AssignOwner {
                partition: before.id,
                owner: deposed,
                replicas: before.replicas.clone(),
                expect_epoch: fenced_epoch,
            })
            .unwrap();

        let after = state.map().partition(before.id).unwrap();
        assert_eq!(after.owner, Some(deposed));
        assert!(after.epoch > before.epoch);
        assert!(!after.replicas.contains(&deposed));
        assert_eq!(state.phase(before.id), Some(PartitionPhase::Serving));
    }

    #[test]
    fn a_completed_fence_drain_is_replicated_for_the_next_leader() {
        let mut state = bootstrapped();
        set_version(&mut state, ClusterVersion::new(0, 1));
        let before = state.map().partitions().next().unwrap().clone();
        state
            .apply(&ControlCommand::FencePartition {
                partition: before.id,
                expect_epoch: before.epoch,
            })
            .unwrap();
        let fenced_epoch = state.map().partition(before.id).unwrap().epoch;

        state
            .apply(&ControlCommand::CompleteFenceDrain {
                partition: before.id,
                expect_epoch: fenced_epoch,
            })
            .unwrap();

        assert!(matches!(
            state.phase(before.id),
            Some(PartitionPhase::Fenced {
                drain_complete: true,
                ..
            })
        ));
    }

    #[test]
    fn a_promotion_at_the_pre_fence_epoch_is_rejected() {
        // This is the deposed owner's view of the world arriving late. It must
        // not be able to reinstate itself.
        let mut state = bootstrapped();
        let before = state.map().partitions().next().unwrap().clone();
        state
            .apply(&ControlCommand::FencePartition {
                partition: before.id,
                expect_epoch: before.epoch,
            })
            .unwrap();

        let err = state.apply(&ControlCommand::AssignOwner {
            partition: before.id,
            owner: before.owner.unwrap(),
            replicas: vec![],
            expect_epoch: before.epoch,
        });
        assert!(matches!(err, Err(Error::StaleEpoch { .. })));
    }

    #[test]
    fn fencing_twice_fails_the_second_time_rather_than_bumping_again() {
        // Two sweeps can both notice the same dead owner. A second bump would
        // strand the promotion the first sweep already has in flight.
        let mut state = bootstrapped();
        let before = state.map().partitions().next().unwrap().clone();
        state
            .apply(&ControlCommand::FencePartition {
                partition: before.id,
                expect_epoch: before.epoch,
            })
            .unwrap();
        let err = state.apply(&ControlCommand::FencePartition {
            partition: before.id,
            expect_epoch: before.epoch,
        });
        assert!(matches!(err, Err(Error::StaleEpoch { .. })));
    }

    #[test]
    fn a_full_failover_ends_with_the_promoted_replica_owning_the_range() {
        let mut state = bootstrapped();
        let before = state.map().partitions().next().unwrap().clone();
        let promoted = before.replicas[0];

        state
            .apply(&ControlCommand::FencePartition {
                partition: before.id,
                expect_epoch: before.epoch,
            })
            .unwrap();
        let fenced_epoch = state.map().partition(before.id).unwrap().epoch;
        state
            .apply(&ControlCommand::AssignOwner {
                partition: before.id,
                owner: promoted,
                replicas: vec![before.replicas[1]],
                expect_epoch: fenced_epoch,
            })
            .unwrap();

        let after = state.map().partition(before.id).unwrap();
        assert_eq!(after.owner, Some(promoted));
        assert!(after.epoch > before.epoch, "the epoch fences the old owner");
        assert!(!after.replicas.contains(&promoted));
        assert_eq!(state.map().check_coverage(), Ok(()));
    }

    #[test]
    fn a_historical_split_leaves_coverage_intact_and_every_key_with_one_owner() {
        let mut state = bootstrapped();
        set_version(&mut state, ClusterVersion::ZERO);
        let parent = state.map().partitions().next().unwrap().clone();
        let ks = parent.keyspace;

        state
            .apply(&ControlCommand::SplitPartition {
                parent: parent.id,
                at: Bytes::from_static(b"m"),
                lower: PartitionId(10),
                upper: PartitionId(11),
                expect_epoch: parent.epoch,
            })
            .unwrap();

        assert_eq!(state.map().check_coverage(), Ok(()));
        assert_eq!(state.map().lookup(ks, b"a").unwrap().id, PartitionId(10));
        assert_eq!(state.map().lookup(ks, b"m").unwrap().id, PartitionId(11));
        assert_eq!(state.map().lookup(ks, b"zzz").unwrap().id, PartitionId(11));
        assert!(state.map().partition(parent.id).is_none(), "parent is gone");
    }

    #[test]
    fn an_active_cluster_rejects_a_split_without_changing_the_map() {
        let mut state = bootstrapped();
        set_version(&mut state, PROTOCOL_0_1);
        let before = state.map().clone();
        let parent = before.partitions().next().unwrap();

        let result = state.apply(&ControlCommand::SplitPartition {
            parent: parent.id,
            at: Bytes::from_static(b"m"),
            lower: PartitionId(10),
            upper: PartitionId(11),
            expect_epoch: parent.epoch,
        });

        assert!(matches!(result, Err(Error::Unavailable(_))));
        assert_eq!(state.map(), &before);
    }

    /// Everyone that holds a splitting parent, owner first.
    fn holders(info: &PartitionInfo) -> Vec<NodeId> {
        let mut all = vec![info.owner.expect("an owner")];
        all.extend(info.replicas.iter().copied());
        all
    }

    #[test]
    fn a_begin_split_keeps_the_parent_serving_its_whole_range() {
        // The heart of the fix: opening a split changes no key's owner. The
        // parent still covers everything and neither child is in the map, so
        // there is no instant where a key is owned by a partition that has no
        // storage behind it.
        let mut state = bootstrapped();
        set_version(&mut state, PROTOCOL_0_1);
        let parent = state.map().partitions().next().unwrap().clone();
        let ks = parent.keyspace;

        state
            .apply(&ControlCommand::BeginSplit {
                parent: parent.id,
                at: Bytes::from_static(b"m"),
                lower: PartitionId(10),
                upper: PartitionId(11),
                expect_epoch: parent.epoch,
            })
            .unwrap();

        assert_eq!(state.map().check_coverage(), Ok(()));
        assert!(state.is_splitting(parent.id));
        assert_eq!(state.map().lookup(ks, b"a").unwrap().id, parent.id);
        assert_eq!(state.map().lookup(ks, b"zzz").unwrap().id, parent.id);
        assert!(state.map().partition(PartitionId(10)).is_none());
        assert!(state.map().partition(PartitionId(11)).is_none());
        // The parent's epoch has not moved, so a write in flight against it is
        // still accepted while the children are being prepared.
        assert_eq!(
            state.map().partition(parent.id).unwrap().epoch,
            parent.epoch
        );
    }

    #[test]
    fn a_split_retires_the_parent_only_after_every_holder_prepares_child_storage() {
        let mut state = bootstrapped();
        set_version(&mut state, PROTOCOL_0_1);
        let parent = state.map().partitions().next().unwrap().clone();
        let ks = parent.keyspace;
        let holders = holders(&parent);

        state
            .apply(&ControlCommand::BeginSplit {
                parent: parent.id,
                at: Bytes::from_static(b"m"),
                lower: PartitionId(10),
                upper: PartitionId(11),
                expect_epoch: parent.epoch,
            })
            .unwrap();

        // Completing before anyone has prepared is refused, and the map is
        // left exactly as it was.
        let before = state.map().clone();
        assert!(matches!(
            state.apply(&ControlCommand::CompleteSplit {
                parent: parent.id,
                expect_epoch: parent.epoch,
            }),
            Err(Error::Unavailable(_))
        ));
        assert_eq!(state.map(), &before);

        // Preparing every holder but the last still does not let it complete.
        for holder in &holders[..holders.len() - 1] {
            state
                .apply(&ControlCommand::MarkSplitPrepared {
                    parent: parent.id,
                    node: *holder,
                    expect_epoch: parent.epoch,
                })
                .unwrap();
        }
        assert!(matches!(
            state.apply(&ControlCommand::CompleteSplit {
                parent: parent.id,
                expect_epoch: parent.epoch,
            }),
            Err(Error::Unavailable(_))
        ));
        assert!(
            state.map().partition(parent.id).is_some(),
            "the parent must not retire while a holder is unprepared"
        );

        // The last holder prepares, and only now does the parent retire.
        state
            .apply(&ControlCommand::MarkSplitPrepared {
                parent: parent.id,
                node: *holders.last().unwrap(),
                expect_epoch: parent.epoch,
            })
            .unwrap();
        state
            .apply(&ControlCommand::CompleteSplit {
                parent: parent.id,
                expect_epoch: parent.epoch,
            })
            .unwrap();

        assert_eq!(state.map().check_coverage(), Ok(()));
        assert!(
            state.map().partition(parent.id).is_none(),
            "the parent is gone once its children own its range"
        );
        assert!(!state.is_splitting(parent.id));
        assert_eq!(state.map().lookup(ks, b"a").unwrap().id, PartitionId(10));
        assert_eq!(state.map().lookup(ks, b"m").unwrap().id, PartitionId(11));
        assert_eq!(state.map().lookup(ks, b"zzz").unwrap().id, PartitionId(11));
    }

    #[test]
    fn a_completed_split_puts_both_children_one_epoch_above_the_parent() {
        // The Lamport constraint from ADR 0002 sits on this: the children keep
        // the parent's data and continue its sequence, and the epoch bump
        // fences a write the old owner had in flight against the parent rather
        // than letting it land in whichever child holds its key. No key's
        // version is reissued or moved backwards by the split.
        let mut state = bootstrapped();
        set_version(&mut state, PROTOCOL_0_1);
        let parent = state.map().partitions().next().unwrap().clone();
        drive_split_to_completion(&mut state, &parent, b"m", PartitionId(10), PartitionId(11));

        for child in [PartitionId(10), PartitionId(11)] {
            assert_eq!(
                state.map().partition(child).unwrap().epoch,
                parent.epoch.next()
            );
            assert_eq!(state.map().partition(child).unwrap().owner, parent.owner);
        }
    }

    #[test]
    fn merge_commands_need_an_initialized_protocol_and_nothing_more() {
        // Merge used to require 0.2 so a 0.1 voter that could not decode tags
        // 22 through 25 stayed rollback-safe. Nothing was ever released, so
        // there is no such voter: 0.1 is still being defined rather than kept
        // compatible with, and merge is part of it. See ADR 0012.
        //
        // What is still refused is a cluster that has not agreed a version at
        // all, because a command whose vocabulary nobody has agreed to is the
        // one case the gate was ever protecting against.
        let mut state = bootstrapped();
        set_version(&mut state, PROTOCOL_0_1);
        let parent = state.map().partitions().next().unwrap().clone();
        drive_split_to_completion(&mut state, &parent, b"m", PartitionId(10), PartitionId(11));
        let lower = state.map().partition(PartitionId(10)).unwrap().clone();
        let upper = state.map().partition(PartitionId(11)).unwrap().clone();

        for command in [
            ControlCommand::MarkMergePrepared {
                generation: merge_generation(&lower, &upper, 12),
                node: lower.owner.unwrap(),
            },
            ControlCommand::CompleteMerge {
                generation: merge_generation(&lower, &upper, 12),
            },
            ControlCommand::AbortMerge {
                generation: merge_generation(&lower, &upper, 12),
            },
            ControlCommand::BeginMerge {
                generation: merge_generation(&lower, &upper, 12),
            },
        ] {
            state
                .ensure_command_permitted(&command)
                .unwrap_or_else(|e| panic!("active protocol 0.1 refused {command:?}: {e}"));
        }

        state
            .apply(&ControlCommand::BeginMerge {
                generation: merge_generation(&lower, &upper, 12),
            })
            .expect("protocol 0.1 enables merge");
    }

    fn merge_generation(
        lower: &PartitionInfo,
        upper: &PartitionInfo,
        merged: u64,
    ) -> crate::command::MergeGeneration {
        crate::command::MergeGeneration {
            lower: lower.id,
            upper: upper.id,
            lower_epoch: lower.epoch,
            upper_epoch: upper.epoch,
            merged: PartitionId(merged),
            boundary: Bytes::from_static(b"m"),
            range: KeyRange::unbounded(),
        }
    }

    #[test]
    fn a_merge_prepares_before_atomically_replacing_both_adjacent_parents() {
        let mut state = bootstrapped();
        set_version(&mut state, PROTOCOL_0_1);
        let parent = state.map().partitions().next().unwrap().clone();
        drive_split_to_completion(&mut state, &parent, b"m", PartitionId(10), PartitionId(11));
        let lower = state.map().partition(PartitionId(10)).unwrap().clone();
        let upper = state.map().partition(PartitionId(11)).unwrap().clone();
        let generation = merge_generation(&lower, &upper, 12);
        let before: Vec<_> = state.map().partitions().cloned().collect();

        state
            .apply(&ControlCommand::BeginMerge {
                generation: generation.clone(),
            })
            .unwrap();
        assert_eq!(state.map().check_coverage(), Ok(()));
        assert_eq!(
            state.map().partitions().cloned().collect::<Vec<_>>(),
            before,
            "beginning a merge moves no key"
        );
        assert!(state.is_merging(lower.id));
        assert!(state.is_merging(upper.id));

        let holders = holders(&lower);
        for holder in &holders[..holders.len() - 1] {
            state
                .apply(&ControlCommand::MarkMergePrepared {
                    generation: generation.clone(),
                    node: *holder,
                })
                .unwrap();
        }
        assert!(matches!(
            state.apply(&ControlCommand::CompleteMerge {
                generation: generation.clone(),
            }),
            Err(Error::Unavailable(_))
        ));
        assert_eq!(
            state.map().partitions().cloned().collect::<Vec<_>>(),
            before
        );

        state
            .apply(&ControlCommand::MarkMergePrepared {
                generation: generation.clone(),
                node: *holders.last().unwrap(),
            })
            .unwrap();
        state
            .apply(&ControlCommand::CompleteMerge {
                generation: generation.clone(),
            })
            .unwrap();

        assert_eq!(state.map().check_coverage(), Ok(()));
        assert!(state.map().partition(lower.id).is_none());
        assert!(state.map().partition(upper.id).is_none());
        let merged = state.map().partition(generation.merged).unwrap();
        assert_eq!(merged.range, KeyRange::unbounded());
        assert_eq!(
            state.map().lookup(parent.keyspace, b"a").unwrap().id,
            merged.id
        );
        assert_eq!(
            state.map().lookup(parent.keyspace, b"z").unwrap().id,
            merged.id
        );
    }

    #[test]
    fn a_delayed_merge_ack_cannot_apply_to_a_retried_generation() {
        let mut state = bootstrapped();
        set_version(&mut state, PROTOCOL_0_1);
        let parent = state.map().partitions().next().unwrap().clone();
        drive_split_to_completion(&mut state, &parent, b"m", PartitionId(10), PartitionId(11));
        let lower = state.map().partition(PartitionId(10)).unwrap().clone();
        let upper = state.map().partition(PartitionId(11)).unwrap().clone();
        let stale = merge_generation(&lower, &upper, 12);
        state
            .apply(&ControlCommand::BeginMerge {
                generation: stale.clone(),
            })
            .unwrap();
        state
            .apply(&ControlCommand::AbortMerge {
                generation: stale.clone(),
            })
            .unwrap();

        let lower = state.map().partition(lower.id).unwrap().clone();
        let upper = state.map().partition(upper.id).unwrap().clone();
        let current = merge_generation(&lower, &upper, 13);
        state
            .apply(&ControlCommand::BeginMerge {
                generation: current,
            })
            .unwrap();
        assert!(state
            .apply(&ControlCommand::MarkMergePrepared {
                generation: stale,
                node: lower.owner.unwrap(),
            })
            .is_err());
    }

    #[test]
    fn split_and_merge_decisions_are_mutually_exclusive() {
        let mut state = bootstrapped();
        set_version(&mut state, PROTOCOL_0_1);
        let parent = state.map().partitions().next().unwrap().clone();
        drive_split_to_completion(&mut state, &parent, b"m", PartitionId(10), PartitionId(11));
        let lower = state.map().partition(PartitionId(10)).unwrap().clone();
        let upper = state.map().partition(PartitionId(11)).unwrap().clone();
        state
            .apply(&ControlCommand::BeginMerge {
                generation: merge_generation(&lower, &upper, 12),
            })
            .unwrap();

        assert!(state
            .apply(&ControlCommand::BeginSplit {
                parent: lower.id,
                at: Bytes::from_static(b"g"),
                lower: PartitionId(13),
                upper: PartitionId(14),
                expect_epoch: lower.epoch,
            })
            .is_err());
    }

    #[test]
    fn owner_failure_mid_merge_aborts_and_raises_both_parent_epochs() {
        let mut state = bootstrapped();
        set_version(&mut state, PROTOCOL_0_1);
        let parent = state.map().partitions().next().unwrap().clone();
        drive_split_to_completion(&mut state, &parent, b"m", PartitionId(10), PartitionId(11));
        let lower = state.map().partition(PartitionId(10)).unwrap().clone();
        let upper = state.map().partition(PartitionId(11)).unwrap().clone();
        state
            .apply(&ControlCommand::BeginMerge {
                generation: merge_generation(&lower, &upper, 12),
            })
            .unwrap();

        state
            .apply(&ControlCommand::FencePartition {
                partition: lower.id,
                expect_epoch: lower.epoch,
            })
            .unwrap();

        assert!(!state.is_merging(lower.id));
        assert!(!state.is_merging(upper.id));
        assert_eq!(
            state.map().partition(lower.id).unwrap().epoch,
            lower.epoch.next(),
            "the failed owner is fenced"
        );
        assert_eq!(
            state.map().partition(upper.id).unwrap().epoch,
            upper.epoch.next(),
            "the other quiesced WAL is recreated under a truncating epoch too"
        );
        assert_eq!(state.map().check_coverage(), Ok(()));
    }

    #[test]
    fn the_worker_prepared_split_needs_protocol_0_1_so_old_logs_still_replay() {
        // A 0.0 cluster, and every mid-rollout member behaving as one, refuses
        // the new commands. That is what keeps a historical log free of tags a
        // pre-0.1 binary cannot decode and keeps a rolling upgrade applying
        // every committed entry identically on both binaries.
        let mut state = bootstrapped();
        set_version(&mut state, ClusterVersion::ZERO);
        let parent = state.map().partitions().next().unwrap().clone();
        let before = state.map().clone();

        assert!(matches!(
            state.apply(&ControlCommand::BeginSplit {
                parent: parent.id,
                at: Bytes::from_static(b"m"),
                lower: PartitionId(10),
                upper: PartitionId(11),
                expect_epoch: parent.epoch,
            }),
            Err(Error::Unavailable(_))
        ));
        assert!(!state.is_splitting(parent.id));
        assert_eq!(state.map(), &before);
    }

    #[test]
    fn a_split_cannot_reuse_a_partition_id_that_is_already_live() {
        let mut state = bootstrapped();
        set_version(&mut state, PROTOCOL_0_1);
        let parent = state.map().partitions().next().unwrap().clone();
        let refused = state.apply(&ControlCommand::BeginSplit {
            parent: parent.id,
            at: Bytes::from_static(b"m"),
            lower: parent.id,
            upper: PartitionId(11),
            expect_epoch: parent.epoch,
        });
        assert!(matches!(refused, Err(Error::InvalidArgument(_))));
        assert!(!state.is_splitting(parent.id));
    }

    #[test]
    fn a_begin_split_with_a_key_outside_the_range_is_refused() {
        let mut state = bootstrapped();
        set_version(&mut state, PROTOCOL_0_1);
        let parent = state.map().partitions().next().unwrap().clone();
        drive_split_to_completion(&mut state, &parent, b"m", PartitionId(10), PartitionId(11));
        let lower = state.map().partition(PartitionId(10)).unwrap().clone();

        // "z" is above the lower child's upper bound of "m".
        let refused = state.apply(&ControlCommand::BeginSplit {
            parent: lower.id,
            at: Bytes::from_static(b"z"),
            lower: PartitionId(12),
            upper: PartitionId(13),
            expect_epoch: lower.epoch,
        });
        assert!(matches!(refused, Err(Error::InvalidArgument(_))));
        assert_eq!(state.map().check_coverage(), Ok(()));
    }

    #[test]
    fn a_second_split_of_a_partition_already_splitting_is_refused() {
        let mut state = bootstrapped();
        set_version(&mut state, PROTOCOL_0_1);
        let parent = state.map().partitions().next().unwrap().clone();
        state
            .apply(&ControlCommand::BeginSplit {
                parent: parent.id,
                at: Bytes::from_static(b"m"),
                lower: PartitionId(10),
                upper: PartitionId(11),
                expect_epoch: parent.epoch,
            })
            .unwrap();
        let refused = state.apply(&ControlCommand::BeginSplit {
            parent: parent.id,
            at: Bytes::from_static(b"n"),
            lower: PartitionId(12),
            upper: PartitionId(13),
            expect_epoch: parent.epoch,
        });
        assert!(matches!(refused, Err(Error::InvalidArgument(_))));
    }

    #[test]
    fn a_split_of_an_unowned_partition_is_refused_because_nobody_can_prepare() {
        let mut state = ClusterState::new();
        state
            .apply(&ControlCommand::SetClusterVersion {
                version: PROTOCOL_0_1,
                expect: ClusterVersion::ZERO,
            })
            .unwrap();
        state
            .apply(&ControlCommand::CreateKeyspace {
                id: KeyspaceId(1),
                name: "default".into(),
                config: KeyspaceConfig::default(),
                created_at_millis: 1,
                first_partition: PartitionId(1),
                owner: None,
                replicas: vec![],
            })
            .unwrap();

        let refused = state.apply(&ControlCommand::BeginSplit {
            parent: PartitionId(1),
            at: Bytes::from_static(b"m"),
            lower: PartitionId(10),
            upper: PartitionId(11),
            expect_epoch: Epoch(1),
        });
        assert!(matches!(refused, Err(Error::InvalidArgument(_))));
    }

    #[test]
    fn fencing_a_splitting_parent_abandons_the_split() {
        // A failover has to be free to fence a partition mid-split, and doing
        // so must leave no way to complete the split against the old epoch.
        let mut state = bootstrapped();
        set_version(&mut state, PROTOCOL_0_1);
        let parent = state.map().partitions().next().unwrap().clone();
        state
            .apply(&ControlCommand::BeginSplit {
                parent: parent.id,
                at: Bytes::from_static(b"m"),
                lower: PartitionId(10),
                upper: PartitionId(11),
                expect_epoch: parent.epoch,
            })
            .unwrap();

        state
            .apply(&ControlCommand::FencePartition {
                partition: parent.id,
                expect_epoch: parent.epoch,
            })
            .unwrap();

        assert!(
            !state.is_splitting(parent.id),
            "the fence dropped the split"
        );
        // The old epoch is gone, so both remaining split entries fail closed.
        assert!(matches!(
            state.apply(&ControlCommand::MarkSplitPrepared {
                parent: parent.id,
                node: parent.owner.unwrap(),
                expect_epoch: parent.epoch,
            }),
            Err(Error::StaleEpoch { .. })
        ));
        assert!(matches!(
            state.apply(&ControlCommand::CompleteSplit {
                parent: parent.id,
                expect_epoch: parent.epoch,
            }),
            Err(Error::StaleEpoch { .. })
        ));
        assert_eq!(state.map().check_coverage(), Ok(()));
    }

    #[test]
    fn an_aborted_split_leaves_the_parent_covering_its_range_at_a_new_epoch() {
        let mut state = bootstrapped();
        set_version(&mut state, PROTOCOL_0_1);
        let parent = state.map().partitions().next().unwrap().clone();
        state
            .apply(&ControlCommand::BeginSplit {
                parent: parent.id,
                at: Bytes::from_static(b"m"),
                lower: PartitionId(10),
                upper: PartitionId(11),
                expect_epoch: parent.epoch,
            })
            .unwrap();
        state
            .apply(&ControlCommand::AbortSplit {
                parent: parent.id,
                expect_epoch: parent.epoch,
            })
            .unwrap();

        assert!(!state.is_splitting(parent.id));
        let after = state.map().partition(parent.id).unwrap().clone();
        // Everything about the parent is as it was except the epoch. Nothing
        // moved, nothing was handed to anyone else, and the range is whole.
        assert_eq!(after.range, parent.range);
        assert_eq!(after.owner, parent.owner);
        assert_eq!(after.replicas, parent.replicas);
        assert_eq!(state.map().check_coverage(), Ok(()));
        assert_eq!(
            after.epoch,
            parent.epoch.next(),
            "the owner quiesced its log for this split and gave Lamports back; it may only \
             reissue them under an epoch that lets it truncate its replicas first"
        );
        // The child ids were reserved and are not reused, so a fresh split
        // takes new numbers rather than the abandoned ones.
        assert!(state.next_partition_id().get() > 11);
    }

    #[test]
    fn every_way_of_dropping_a_pending_split_raises_the_parents_epoch() {
        // The invariant a reopening holder relies on: "my split is gone" and
        // "my epoch moved" are one event, whichever entry did the dropping. A
        // path that abandoned a split at an unchanged epoch would let the
        // parent resume assigning Lamports its quiesce handed back, under an
        // epoch no replica will truncate for.
        for drop_it in [
            &(|parent: &PartitionInfo| ControlCommand::AbortSplit {
                parent: parent.id,
                expect_epoch: parent.epoch,
            }) as &dyn Fn(&PartitionInfo) -> ControlCommand,
            &|parent: &PartitionInfo| ControlCommand::SetReplicas {
                partition: parent.id,
                replicas: vec![parent.replicas[0]],
                expect_epoch: parent.epoch,
            },
            &|parent: &PartitionInfo| ControlCommand::FencePartition {
                partition: parent.id,
                expect_epoch: parent.epoch,
            },
        ] {
            let mut state = bootstrapped();
            set_version(&mut state, PROTOCOL_0_1);
            let parent = state.map().partitions().next().unwrap().clone();
            state
                .apply(&ControlCommand::BeginSplit {
                    parent: parent.id,
                    at: Bytes::from_static(b"m"),
                    lower: PartitionId(10),
                    upper: PartitionId(11),
                    expect_epoch: parent.epoch,
                })
                .unwrap();
            let command = drop_it(&parent);
            state.apply(&command).unwrap();
            assert!(!state.is_splitting(parent.id), "{command:?} dropped it");
            assert_eq!(
                state.map().partition(parent.id).unwrap().epoch,
                parent.epoch.next(),
                "{command:?} dropped a pending split without raising the parent's epoch"
            );
        }
    }

    #[test]
    fn a_replica_set_change_aborts_a_pending_split_rather_than_wedging_it() {
        // The P2 from the split review: if a required replica dies after the
        // split began, it can never be marked prepared. Repair replaces it with
        // a SetReplicas, and that has to be able to proceed — so it aborts the
        // split rather than being blocked by it. The parent keeps its whole
        // range, ready for the split to be reopened once the set settles.
        let mut state = bootstrapped();
        set_version(&mut state, PROTOCOL_0_1);
        let parent = state.map().partitions().next().unwrap().clone();
        let new_replicas = vec![parent.replicas[0]];
        state
            .apply(&ControlCommand::BeginSplit {
                parent: parent.id,
                at: Bytes::from_static(b"m"),
                lower: PartitionId(10),
                upper: PartitionId(11),
                expect_epoch: parent.epoch,
            })
            .unwrap();

        state
            .apply(&ControlCommand::SetReplicas {
                partition: parent.id,
                replicas: new_replicas.clone(),
                expect_epoch: parent.epoch,
            })
            .unwrap();

        assert!(
            !state.is_splitting(parent.id),
            "the replica-set change aborts the split instead of wedging on a dead holder"
        );
        assert_eq!(
            state.map().partition(parent.id).unwrap().replicas,
            new_replicas
        );
        assert_eq!(state.map().check_coverage(), Ok(()));
    }

    /// Runs a split all the way through: open it, prepare every holder, retire
    /// the parent. Used by the tests that care about the result rather than the
    /// ordering that produced it.
    fn drive_split_to_completion(
        state: &mut ClusterState,
        parent: &PartitionInfo,
        at: &'static [u8],
        lower: PartitionId,
        upper: PartitionId,
    ) {
        state
            .apply(&ControlCommand::BeginSplit {
                parent: parent.id,
                at: Bytes::from_static(at),
                lower,
                upper,
                expect_epoch: parent.epoch,
            })
            .unwrap();
        for holder in holders(parent) {
            state
                .apply(&ControlCommand::MarkSplitPrepared {
                    parent: parent.id,
                    node: holder,
                    expect_epoch: parent.epoch,
                })
                .unwrap();
        }
        state
            .apply(&ControlCommand::CompleteSplit {
                parent: parent.id,
                expect_epoch: parent.epoch,
            })
            .unwrap();
    }

    #[test]
    fn a_split_fences_writes_that_were_in_flight_against_the_parent() {
        let mut state = bootstrapped();
        let parent = state.map().partitions().next().unwrap().clone();
        state
            .apply(&ControlCommand::SplitPartition {
                parent: parent.id,
                at: Bytes::from_static(b"m"),
                lower: PartitionId(10),
                upper: PartitionId(11),
                expect_epoch: parent.epoch,
            })
            .unwrap();

        for id in [PartitionId(10), PartitionId(11)] {
            assert!(state.map().partition(id).unwrap().epoch > parent.epoch);
        }
    }

    #[test]
    fn a_split_key_outside_the_range_is_rejected() {
        let mut state = bootstrapped();
        let parent = state.map().partitions().next().unwrap().clone();
        state
            .apply(&ControlCommand::SplitPartition {
                parent: parent.id,
                at: Bytes::from_static(b"m"),
                lower: PartitionId(10),
                upper: PartitionId(11),
                expect_epoch: parent.epoch,
            })
            .unwrap();

        let lower = state.map().partition(PartitionId(10)).unwrap().clone();
        let err = state.apply(&ControlCommand::SplitPartition {
            parent: lower.id,
            at: Bytes::from_static(b"z"),
            lower: PartitionId(12),
            upper: PartitionId(13),
            expect_epoch: lower.epoch,
        });
        assert!(err.is_err());
        assert_eq!(state.map().check_coverage(), Ok(()));
    }

    #[test]
    fn deleting_a_keyspace_removes_its_partitions_and_leaves_the_rest_covered() {
        let mut state = bootstrapped();
        let candidates = state.placement_candidates();
        state
            .apply(&ControlCommand::CreateKeyspace {
                id: state.next_keyspace_id(),
                name: "second".into(),
                config: KeyspaceConfig::default(),
                created_at_millis: 2,
                first_partition: state.next_partition_id(),
                owner: Some(candidates[0]),
                replicas: vec![],
            })
            .unwrap();
        let doomed = state.keyspace_by_name("second").unwrap().id;

        state
            .apply(&ControlCommand::DeleteKeyspace { id: doomed })
            .unwrap();

        assert_eq!(state.map().check_coverage(), Ok(()));
        assert!(state.keyspace_by_name("second").is_none());
        assert_eq!(state.map().lookup(doomed, b"a"), None);
        assert!(state.keyspace_by_name("default").is_some());
    }

    #[test]
    fn partition_ids_are_never_reused_after_a_split() {
        let mut state = bootstrapped();
        let parent = state.map().partitions().next().unwrap().clone();
        let next_before = state.next_partition_id();
        state
            .apply(&ControlCommand::SplitPartition {
                parent: parent.id,
                at: Bytes::from_static(b"m"),
                lower: next_before,
                upper: next_before.next(),
                expect_epoch: parent.epoch,
            })
            .unwrap();
        assert!(state.next_partition_id() > next_before.next());
    }

    #[test]
    fn placement_prefers_the_least_loaded_worker() {
        let state = bootstrapped();
        // Node 1 owns the only partition and nodes 2 and 3 replicate it, so
        // all three hold it and the tie breaks on id.
        assert_eq!(
            state.placement_candidates(),
            vec![NodeId(1), NodeId(2), NodeId(3)]
        );
    }

    #[test]
    fn an_unhealthy_worker_is_not_a_placement_candidate() {
        let mut state = bootstrapped();
        state
            .apply(&ControlCommand::SetHealth {
                node: NodeId(2),
                health: NodeHealth::Dead,
            })
            .unwrap();
        assert!(!state.placement_candidates().contains(&NodeId(2)));
    }

    #[test]
    fn a_draining_worker_refuses_new_ownership_assignments() {
        let mut state = bootstrapped();
        set_version(&mut state, crate::version::binary_version());
        state
            .apply(&ControlCommand::FencePartition {
                partition: PartitionId(1),
                expect_epoch: Epoch(1),
            })
            .unwrap();
        state
            .apply(&ControlCommand::RegisterNode {
                node: NodeId(2),
                role: NodeRole::Worker,
                address: "10.0.0.2:7000".into(),
                speaks: crate::version::binary_speaks(),
                ready: true,
                draining: true,
            })
            .unwrap();

        let result = state.apply(&ControlCommand::AssignOwner {
            partition: PartitionId(1),
            owner: NodeId(2),
            replicas: vec![NodeId(3)],
            expect_epoch: Epoch(2),
        });
        assert!(result.is_err());
    }

    /// The active version of a cluster one finalization ahead of this binary.
    ///
    /// A member reaches this state legitimately: `finalize-upgrade` counts
    /// only live nodes, so a member that was down or partitioned during the
    /// finalize comes back on the old binary and replays entries committed at
    /// the newer active version. It must apply them the way its peers did.
    fn one_finalization_ahead() -> ClusterVersion {
        let own = crate::version::binary_version();
        ClusterVersion::new(own.major, own.minor + 1)
    }

    /// Registers a worker that made no lifecycle claim, which is what the
    /// leader records for a worker whose protocol cannot carry one.
    fn register_without_a_lifecycle_claim(state: &mut ClusterState, id: u64, speaks: VersionRange) {
        state
            .apply(&ControlCommand::RegisterNode {
                node: NodeId(id),
                role: NodeRole::Worker,
                address: format!("10.0.0.{id}:7000"),
                speaks,
                ready: false,
                draining: false,
            })
            .unwrap();
    }

    #[test]
    fn the_lifecycle_gate_reads_the_finalized_cluster_version_not_the_running_binary() {
        // Every member of the leader group answers this question while
        // applying the same committed entry, and during an upgrade they do not
        // all run the same binary. So the answer has to come out of the
        // replicated state alone. Reading the running binary — which is what
        // issue #105 was — makes it depend on which process asked.
        let mut before = bootstrapped();
        set_version(&mut before, ClusterVersion::ZERO);
        assert!(
            !before.lifecycle_enabled(),
            "no lifecycle protocol exists below 0.1, whatever binary is asking"
        );

        let mut at = bootstrapped();
        set_version(&mut at, PROTOCOL_0_1);
        assert!(at.lifecycle_enabled(), "0.1 is the version that carries it");

        let mut ahead = bootstrapped();
        set_version(&mut ahead, one_finalization_ahead());
        assert!(
            ahead.lifecycle_enabled(),
            "a cluster past 0.1 still carries lifecycle state, and a member \
             whose binary is not the finalized one has to agree"
        );

        assert!(
            !ClusterState::new().lifecycle_enabled(),
            "a state machine that has never been given a version has no \
             finalized protocol to speak"
        );
    }

    #[test]
    fn a_worker_that_makes_no_lifecycle_claim_can_still_own_before_the_lifecycle_version() {
        // The #105 shape, from the leader's side. A worker on a newer binary
        // knows the active protocol cannot carry `ready`, so it does not
        // claim it. Withholding placement from that worker leaves an operator
        // who grew the cluster mid-upgrade with a node that joined, reports
        // healthy, and silently owns nothing.
        let mut state = bootstrapped();
        set_version(&mut state, ClusterVersion::ZERO);
        register_without_a_lifecycle_claim(&mut state, 4, crate::version::speaks_for(PROTOCOL_0_1));

        assert!(
            state.placement_candidates().contains(&NodeId(4)),
            "before 0.1 no node makes a lifecycle claim, so the absence of \
             one cannot be read as unreadiness"
        );
        assert!(state.new_ownership_eligibility(NodeId(4)).is_ok());
    }

    #[test]
    fn a_worker_that_makes_no_lifecycle_claim_cannot_own_once_the_lifecycle_version_is_active() {
        let mut state = bootstrapped();
        set_version(&mut state, PROTOCOL_0_1);
        register_without_a_lifecycle_claim(&mut state, 4, crate::version::binary_speaks());

        assert!(
            !state.placement_candidates().contains(&NodeId(4)),
            "once the protocol carries readiness, a node that does not claim \
             it is genuinely not ready"
        );
    }

    #[test]
    fn ownership_eligibility_is_the_same_on_a_binary_that_predates_the_active_version() {
        // The determinism guarantee stated as an outcome rather than as a
        // predicate: this process is running the 0.1 binary, and it must
        // refuse the same assignment a binary from the finalized version
        // would refuse. Deciding from `binary_version()` made it accept.
        let ahead = one_finalization_ahead();
        let mut state = bootstrapped();
        set_version(&mut state, ahead);
        // Node 4 runs the binary the cluster finalized on, so it is admitted;
        // this process is the member that has not been upgraded yet.
        register_without_a_lifecycle_claim(
            &mut state,
            4,
            VersionRange::new(crate::version::binary_version(), ahead),
        );
        state
            .apply(&ControlCommand::FencePartition {
                partition: PartitionId(1),
                expect_epoch: Epoch(1),
            })
            .unwrap();

        let assigned = state.apply(&ControlCommand::AssignOwner {
            partition: PartitionId(1),
            owner: NodeId(4),
            replicas: vec![],
            expect_epoch: Epoch(2),
        });
        assert!(
            assigned.is_err(),
            "a member behind the finalized version must apply the finalized \
             rule, or one committed entry produces two different maps"
        );
    }

    #[test]
    fn a_node_that_still_holds_a_partition_cannot_be_forgotten() {
        let mut state = bootstrapped();
        assert!(state
            .apply(&ControlCommand::ForgetNode { node: NodeId(1) })
            .is_err());
    }

    #[test]
    fn the_map_version_advances_on_every_routing_change() {
        let mut state = bootstrapped();
        let before = state.map_version();
        let parent = state.map().partitions().next().unwrap().clone();
        state
            .apply(&ControlCommand::FencePartition {
                partition: parent.id,
                expect_epoch: parent.epoch,
            })
            .unwrap();
        assert!(
            state.map_version() > before,
            "a worker decides what to keep by comparing versions"
        );
    }

    #[test]
    fn a_fresh_cluster_takes_its_first_version_without_needing_one_to_advance_from() {
        let mut state = ClusterState::new();
        assert_eq!(state.cluster_version(), ClusterVersion::ZERO);
        state
            .apply(&ControlCommand::SetClusterVersion {
                version: ClusterVersion::new(0, 1),
                expect: ClusterVersion::ZERO,
            })
            .unwrap();
        assert_eq!(state.cluster_version(), ClusterVersion::new(0, 1));
    }

    #[test]
    fn the_cluster_version_only_advances() {
        let mut state = ClusterState::new();
        state
            .apply(&ControlCommand::SetClusterVersion {
                version: ClusterVersion::new(0, 2),
                expect: ClusterVersion::ZERO,
            })
            .unwrap();

        // Backwards would tell nodes to write a format the version they came
        // from cannot read, so the state machine refuses rather than trusting
        // every proposer to know that.
        let back = state.apply(&ControlCommand::SetClusterVersion {
            version: ClusterVersion::new(0, 1),
            expect: ClusterVersion::new(0, 2),
        });
        assert!(back.is_err());
        assert_eq!(state.cluster_version(), ClusterVersion::new(0, 2));
    }

    #[test]
    fn a_version_advance_against_a_stale_expectation_is_refused() {
        // Two operators finalizing at once: the second proposal names the
        // version the first one already replaced, and must fail rather than
        // advance a second time.
        let mut state = ClusterState::new();
        state
            .apply(&ControlCommand::SetClusterVersion {
                version: ClusterVersion::new(0, 2),
                expect: ClusterVersion::ZERO,
            })
            .unwrap();
        let raced = state.apply(&ControlCommand::SetClusterVersion {
            version: ClusterVersion::new(0, 3),
            expect: ClusterVersion::ZERO,
        });
        assert!(raced.is_err());
        assert_eq!(state.cluster_version(), ClusterVersion::new(0, 2));
    }

    #[test]
    fn re_registering_updates_the_versions_a_node_can_speak() {
        // This is what a rolling update looks like to the state machine: the
        // same node comes back asserting a newer window.
        let active = ClusterVersion::new(0, 9);
        let mut state = ClusterState::new();
        set_version(&mut state, active);
        register_with(
            &mut state,
            1,
            VersionRange::new(ClusterVersion::new(0, 8), active),
        )
        .unwrap();
        let upgraded = VersionRange::new(active, ClusterVersion::new(0, 10));
        register_with(&mut state, 1, upgraded).unwrap();
        assert_eq!(state.node(NodeId(1)).unwrap().speaks, upgraded);
    }

    #[test]
    fn an_exact_active_version_is_admitted() {
        let active = ClusterVersion::new(0, 4);
        let mut state = ClusterState::new();
        set_version(&mut state, active);

        assert_eq!(
            register_with(&mut state, 1, VersionRange::exactly(active)),
            Ok(())
        );
    }

    #[test]
    fn an_n_minus_one_binary_whose_range_reaches_active_is_admitted() {
        let active = ClusterVersion::new(0, 4);
        let mut state = ClusterState::new();
        set_version(&mut state, active);

        assert_eq!(
            register_with(
                &mut state,
                1,
                VersionRange::new(ClusterVersion::new(0, 3), active),
            ),
            Ok(())
        );
    }

    #[test]
    fn a_too_old_node_is_refused_without_becoming_a_member() {
        let mut state = ClusterState::new();
        set_version(&mut state, ClusterVersion::new(0, 4));

        let result = register_with(
            &mut state,
            1,
            VersionRange::new(ClusterVersion::new(0, 2), ClusterVersion::new(0, 3)),
        );

        assert!(
            matches!(result, Err(Error::InvalidArgument(reason)) if reason.contains("0.2..0.3") && reason.contains("0.4"))
        );
        assert!(state.node(NodeId(1)).is_none());
    }

    #[test]
    fn a_too_new_node_with_no_overlap_is_refused() {
        let mut state = ClusterState::new();
        set_version(&mut state, ClusterVersion::new(0, 4));

        let result = register_with(
            &mut state,
            1,
            VersionRange::new(ClusterVersion::new(0, 5), ClusterVersion::new(0, 6)),
        );

        assert!(
            matches!(result, Err(Error::InvalidArgument(reason)) if reason.contains("0.5..0.6") && reason.contains("0.4"))
        );
    }

    #[test]
    fn an_uninitialized_cluster_can_admit_the_node_that_will_bootstrap_it() {
        let mut state = ClusterState::new();
        let speaks = VersionRange::exactly(ClusterVersion::new(9, 9));

        assert_eq!(register_with(&mut state, 1, speaks), Ok(()));
    }

    #[test]
    fn a_legacy_registration_speaks_exactly_zero() {
        let mut state = ClusterState::new();
        state
            .apply(&ControlCommand::CreateKeyspace {
                id: KeyspaceId(1),
                name: "legacy".into(),
                config: KeyspaceConfig::default(),
                created_at_millis: 1,
                first_partition: PartitionId(1),
                owner: None,
                replicas: vec![],
            })
            .unwrap();

        assert_eq!(
            register_with(&mut state, 1, VersionRange::exactly(ClusterVersion::ZERO),),
            Ok(())
        );
        assert!(register_with(
            &mut state,
            2,
            VersionRange::exactly(ClusterVersion::new(0, 1)),
        )
        .is_err());
    }

    #[test]
    fn an_incompatible_registered_node_keeps_existing_ownership_but_gets_no_new_ownership() {
        let old = ClusterVersion::new(0, 3);
        let active = ClusterVersion::new(0, 4);
        let mut state = ClusterState::new();
        set_version(&mut state, old);
        register_with(
            &mut state,
            1,
            VersionRange::new(ClusterVersion::new(0, 2), old),
        )
        .unwrap();
        state
            .apply(&ControlCommand::CreateKeyspace {
                id: KeyspaceId(1),
                name: "default".into(),
                config: KeyspaceConfig::default(),
                created_at_millis: 1,
                first_partition: PartitionId(1),
                owner: Some(NodeId(1)),
                replicas: vec![],
            })
            .unwrap();
        state
            .apply(&ControlCommand::SetClusterVersion {
                version: active,
                expect: old,
            })
            .unwrap();

        assert_eq!(
            state.map().partition(PartitionId(1)).unwrap().owner,
            Some(NodeId(1))
        );
        assert!(!state.placement_candidates().contains(&NodeId(1)));

        state
            .apply(&ControlCommand::FencePartition {
                partition: PartitionId(1),
                expect_epoch: Epoch(1),
            })
            .unwrap();
        let result = state.apply(&ControlCommand::AssignOwner {
            partition: PartitionId(1),
            owner: NodeId(1),
            replicas: vec![],
            expect_epoch: Epoch(2),
        });
        assert!(
            matches!(result, Err(Error::InvalidArgument(reason)) if reason.contains("cannot receive ownership"))
        );
    }

    #[test]
    fn a_credential_scoped_to_nothing_is_rejected() {
        let mut state = ClusterState::new();
        let err = state.apply(&ControlCommand::CreateCredential {
            credential: Box::new(Credential {
                id: "c".into(),
                secret_hash: crate::model::hash_secret("s"),
                keyspaces: vec![],
                permissions: vec![],
                description: String::new(),
                created_at_millis: 0,
                expires_at_millis: None,
            }),
        });
        assert!(err.is_err());
    }
}
