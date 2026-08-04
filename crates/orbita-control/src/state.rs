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
//! an owner, and the only thing that removes an owner is
//! [`ControlCommand::FencePartition`], which bumps the epoch as it does so. A
//! caller cannot promote before fencing even by accident, because the state
//! machine will not apply the entry. Putting the rule in the driver would make
//! it a convention; putting it here makes it an invariant.

use crate::command::ControlCommand;
use crate::membership::{NodeHealth, NodeRole};
use crate::model::{Credential, Keyspace, KeyspaceConfig};
use crate::version::{ClusterVersion, CompatibilityRefusal, VersionRange};

use orbita_core::{
    Epoch, Error, KeyRange, KeyspaceId, KeyspaceName, MapVersion, NodeId, PartitionId,
    PartitionInfo, PartitionMap, Result,
};

use std::collections::BTreeMap;

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
    Fenced { deposed: NodeId },
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
}

/// Everything the leader group knows, as of some prefix of the log.
#[derive(Debug, Clone, Default)]
pub struct ClusterState {
    map: PartitionMap,
    keyspaces: BTreeMap<KeyspaceId, Keyspace>,
    phases: BTreeMap<PartitionId, PartitionPhase>,
    nodes: BTreeMap<NodeId, NodeRecord>,
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

    /// Applies one command, returning the same result on every member.
    ///
    /// An error here is a decision, not a transport failure: the command was
    /// committed and it did not take effect, and every member agrees that it
    /// did not. That is why the errors are `orbita_core::Error` values a
    /// client can be told about rather than a separate internal type.
    pub fn apply(&mut self, command: &ControlCommand) -> Result<()> {
        match command {
            ControlCommand::RegisterNode {
                node,
                role,
                address,
                speaks,
            } => self.register_node(*node, *role, address, *speaks),
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
            ControlCommand::AssignOwner {
                partition,
                owner,
                replicas,
                expect_epoch,
            } => self.assign_owner(*partition, *owner, replicas, *expect_epoch),
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
            ControlCommand::SetClusterVersion { version, expect } => {
                self.set_cluster_version(*version, *expect)
            }
        }
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
        });
        entry.role = role;
        entry.address = address.to_string();
        entry.speaks = speaks;
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
        for partition in doomed {
            self.phases.remove(&partition);
        }
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
        // The epoch bump and the ownership removal are the same entry. There
        // is no committed state in which the old owner has been removed but
        // its epoch still stands, which is the state a returning owner could
        // have written at.
        info.owner = None;
        info.epoch = info.epoch.next();
        info.replicas.retain(|r| *r != deposed);
        self.replace_partition(info);
        self.phases
            .insert(partition, PartitionPhase::Fenced { deposed });
        self.bump_map_version();
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

    fn set_replicas(
        &mut self,
        partition: PartitionId,
        replicas: &[NodeId],
        expect_epoch: Epoch,
    ) -> Result<()> {
        let mut info = self.check_epoch(partition, expect_epoch)?;
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
            speaks: crate::version::binary_speaks(),
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

    fn register_with(state: &mut ClusterState, id: u64, speaks: VersionRange) -> Result<()> {
        state.apply(&ControlCommand::RegisterNode {
            node: NodeId(id),
            role: NodeRole::Worker,
            address: format!("10.0.0.{id}:7000"),
            speaks,
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
                deposed: before.owner.unwrap()
            })
        );
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
    fn a_split_leaves_coverage_intact_and_every_key_with_one_owner() {
        let mut state = bootstrapped();
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
