//! The alphabet of the replicated log.
//!
//! Everything the leader group decides is one of these, and the state machine
//! in [`crate::state`] is a pure function of the sequence of them. Two rules
//! shape the list.
//!
//! **Nothing here reads a clock or a random number.** Applying a command has
//! to produce the same state on every member, and a member that consulted its
//! own clock during apply would diverge from one that applied the same entry a
//! second later. Where a time is genuinely part of the decision, such as a
//! keyspace's creation timestamp, the proposing leader reads its clock once
//! and puts the value in the command.
//!
//! **Nothing here is an observation.** Heartbeat arrival times and replica
//! progress are not replicated, because a monotonic clock reading from one
//! node means nothing on another and because a log entry every 250ms per node
//! would dwarf everything else in the log. The leader keeps observations in
//! memory and replicates only the conclusions it draws from them, which is
//! what [`ControlCommand::SetHealth`] is. A leader that has just taken over
//! has no observations, waits to collect some, and then re-derives health.

use crate::codec::{CodecError, CodecResult, Reader, Writer};
use crate::membership::{NodeHealth, NodeRole};
use crate::model::{Credential, KeyspaceConfig};
use crate::version::{ClusterVersion, VersionRange};

use bytes::Bytes;
use orbita_core::{Epoch, KeyRange, KeyspaceId, NodeId, PartitionId};

// Kept as decode-only legacy. v0.0.1 persisted `RegisterNode` entries under
// this tag with nothing after the address, and recovery treats an entry it
// cannot decode as the end of the trustworthy log and truncates there.
// Reusing the tag with a longer payload would make upgrading a v0.0.1
// control-plane node silently discard its committed state.
const TAG_REGISTER_NODE: u8 = 1;
const TAG_SET_HEALTH: u8 = 2;
const TAG_FORGET_NODE: u8 = 3;
const TAG_CREATE_KEYSPACE: u8 = 4;
const TAG_UPDATE_KEYSPACE: u8 = 5;
const TAG_DELETE_KEYSPACE: u8 = 6;
const TAG_CREATE_CREDENTIAL: u8 = 7;
const TAG_REVOKE_CREDENTIAL: u8 = 8;
const TAG_FENCE_PARTITION: u8 = 9;
const TAG_ASSIGN_OWNER: u8 = 10;
const TAG_SET_REPLICAS: u8 = 11;
const TAG_SPLIT_PARTITION: u8 = 12;
const TAG_SET_CLUSTER_VERSION: u8 = 13;
// `RegisterNode` with the speakable version range appended. New entries are
// written with this tag; the old tag stays readable for the release window.
const TAG_REGISTER_NODE_V2: u8 = 14;
// Registration with readiness and draining state. The older tags stay decode
// only so an upgraded control member can replay either predecessor.
const TAG_REGISTER_NODE_V3: u8 = 15;
const TAG_TRANSFER_OWNERSHIP: u8 = 16;
const TAG_COMPLETE_FENCE_DRAIN: u8 = 17;
// The worker-prepared split protocol, tags 18 through 21. These belong to
// protocol 0.1: an active cluster below it never proposes one, so a historical
// log never contains a tag a pre-0.1 binary cannot decode. See
// `ControlCommand::BeginSplit` for why the operation is four entries and not
// one.
const TAG_BEGIN_SPLIT: u8 = 18;
const TAG_MARK_SPLIT_PREPARED: u8 = 19;
const TAG_COMPLETE_SPLIT: u8 = 20;
const TAG_ABORT_SPLIT: u8 = 21;
// The dual-parent worker-prepared merge protocol. Like split, these tags are
// emitted only after the active cluster protocol has been finalized to a
// version whose binaries all understand them.
const TAG_BEGIN_MERGE: u8 = 22;
const TAG_MARK_MERGE_PREPARED: u8 = 23;
const TAG_COMPLETE_MERGE: u8 = 24;
const TAG_ABORT_MERGE: u8 = 25;

/// The complete identity of one merge attempt.
///
/// Parent ids alone are not enough: an acknowledgement can arrive after an
/// abort and retry. Epochs, child id, boundary, and combined range make that
/// delayed report fail closed even when the same adjacent pair is retried.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeGeneration {
    pub lower: PartitionId,
    pub upper: PartitionId,
    pub lower_epoch: Epoch,
    pub upper_epoch: Epoch,
    pub merged: PartitionId,
    pub boundary: Bytes,
    pub range: KeyRange,
}

impl MergeGeneration {
    pub(crate) fn encode(&self, w: &mut Writer) {
        w.u64(self.lower.get())
            .u64(self.upper.get())
            .u64(self.lower_epoch.get())
            .u64(self.upper_epoch.get())
            .u64(self.merged.get())
            .bytes(&self.boundary)
            .bytes(self.range.start())
            .opt_bytes(self.range.end());
    }

    pub(crate) fn decode(r: &mut Reader<'_>) -> CodecResult<Self> {
        let lower = PartitionId(r.u64()?);
        let upper = PartitionId(r.u64()?);
        let lower_epoch = Epoch(r.u64()?);
        let upper_epoch = Epoch(r.u64()?);
        let merged = PartitionId(r.u64()?);
        let boundary = r.bytes()?;
        let start = r.bytes()?;
        let end = r.opt_bytes()?;
        let range = KeyRange::new(start, end).ok_or(CodecError::OutOfRange("merge range"))?;
        Ok(Self {
            lower,
            upper,
            lower_epoch,
            upper_epoch,
            merged,
            boundary,
            range,
        })
    }
}

/// One decision, committed once and applied everywhere.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlCommand {
    /// Admits a node to the cluster, or updates the address of one already in
    /// it. Idempotent, because a restarting node re-registers on every start
    /// and must not need to know whether it succeeded last time.
    RegisterNode {
        node: NodeId,
        role: NodeRole,
        address: String,
        /// The cluster versions this node's binary can speak, asserted by the
        /// node itself. Recorded so that `finalize-upgrade` can check every
        /// node against a target version without asking anybody at that
        /// moment.
        speaks: VersionRange,
        ready: bool,
        draining: bool,
    },

    /// Records the leader group's conclusion about a node.
    SetHealth {
        node: NodeId,
        health: NodeHealth,
    },

    /// Removes a node entirely. Only legal once it holds no partitions, so
    /// that forgetting a node can never be what creates a hole in the map.
    ForgetNode {
        node: NodeId,
    },

    /// Creates a keyspace and, in the same entry, the single unbounded
    /// partition that covers it.
    ///
    /// They are one command because a keyspace with no partition is a
    /// keyspace whose keys belong to nobody. Splitting this into two entries
    /// would leave a window, however short, in which `check_coverage` fails,
    /// and there is no reason to have that window exist at all.
    CreateKeyspace {
        id: KeyspaceId,
        name: String,
        config: KeyspaceConfig,
        created_at_millis: u64,
        /// Allocated by the proposer so that applying is deterministic.
        first_partition: PartitionId,
        /// `None` when the cluster has no worker to give it to yet. The
        /// partition still exists and still covers the range, so the map stays
        /// complete; it is unavailable rather than absent, and the leader
        /// assigns an owner as soon as one appears.
        owner: Option<NodeId>,
        replicas: Vec<NodeId>,
    },

    UpdateKeyspace {
        id: KeyspaceId,
        config: KeyspaceConfig,
    },

    /// Destroys a keyspace and every partition in it.
    DeleteKeyspace {
        id: KeyspaceId,
    },

    CreateCredential {
        credential: Box<Credential>,
    },

    RevokeCredential {
        id: String,
    },

    /// Fences the current owner by bumping the epoch and leaving the partition
    /// ownerless.
    ///
    /// `expect_epoch` makes this safe to retry and safe to race: a second
    /// proposal for a partition somebody else already fenced fails on the
    /// epoch check instead of bumping again and stranding the promotion that
    /// was already in flight.
    FencePartition {
        partition: PartitionId,
        expect_epoch: Epoch,
    },

    /// Records that the leader waited out every lease from the fenced owner.
    /// Once committed, a later leader need not repeat that completed wait.
    CompleteFenceDrain {
        partition: PartitionId,
        expect_epoch: Epoch,
    },

    /// Gives an ownerless partition an owner.
    ///
    /// The state machine refuses this for a partition that still has one,
    /// which is what forces the fence to commit first during failover. The
    /// planned handoff has its own command after the old owner quiesces.
    AssignOwner {
        partition: PartitionId,
        owner: NodeId,
        replicas: Vec<NodeId>,
        expect_epoch: Epoch,
    },

    /// Moves ownership in one committed map change and bumps the epoch.
    /// Reserved for a planned handoff after the old owner has quiesced writes.
    TransferOwnership {
        partition: PartitionId,
        from: NodeId,
        to: NodeId,
        replicas: Vec<NodeId>,
        expect_epoch: Epoch,
    },

    /// Changes the replica set without touching ownership, which is how a
    /// partition regains a third copy after losing one.
    SetReplicas {
        partition: PartitionId,
        replicas: Vec<NodeId>,
        expect_epoch: Epoch,
    },

    /// Replaces one partition with two at a boundary key, in a single entry.
    ///
    /// The unsafe original. Retained decode-only for replay: a historical log
    /// from before the worker-prepared protocol still contains these, and the
    /// state machine must replay them to reconstruct the map a 0.0 cluster
    /// committed. It is refused for any new proposal at protocol 0.1 and above,
    /// because it retires the parent in the same entry that names the children
    /// as owners — before any worker has storage for them, which is the window
    /// [PR #53](https://github.com/anomalyco/orbita/pull/53) closed. The
    /// four-entry protocol below replaces it.
    SplitPartition {
        parent: PartitionId,
        at: Bytes,
        lower: PartitionId,
        upper: PartitionId,
        expect_epoch: Epoch,
    },

    /// Opens a split without touching the partition table.
    ///
    /// The parent keeps its range and keeps serving; all this records is that a
    /// split is in progress, which two children it will produce, and which
    /// nodes hold the parent and must therefore prepare storage for those
    /// children. The map version bumps so those holders notice and start
    /// preparing, but no key changes owner. This is the first of four entries
    /// because the safety property is an *ordering*: the parent must not retire
    /// until every holder has prepared, and an ordering cannot be expressed in
    /// one atomic entry. See [`CompleteSplit`](Self::CompleteSplit).
    BeginSplit {
        parent: PartitionId,
        at: Bytes,
        lower: PartitionId,
        upper: PartitionId,
        expect_epoch: Epoch,
    },

    /// Records that one holder has prepared storage for the pending split's
    /// children.
    ///
    /// `expect_epoch` is the parent's epoch, unchanged since the split began. A
    /// failover fences the parent and bumps it, which makes this fail closed
    /// rather than acknowledge preparation against a split the cluster has
    /// already abandoned.
    MarkSplitPrepared {
        parent: PartitionId,
        node: NodeId,
        expect_epoch: Epoch,
    },

    /// Retires the parent and installs both children, in one entry.
    ///
    /// Refused until every holder recorded by [`BeginSplit`](Self::BeginSplit)
    /// has a matching [`MarkSplitPrepared`](Self::MarkSplitPrepared). Once it
    /// applies the map goes from covered-by-the-parent to covered-by-the-two-
    /// children with nothing in between, exactly as the old one-entry split
    /// did — the difference is only that it cannot reach this point until the
    /// children have somewhere to live.
    CompleteSplit {
        parent: PartitionId,
        expect_epoch: Epoch,
    },

    /// Abandons a pending split, leaving the parent exactly as it was.
    ///
    /// The escape hatch for a split that cannot finish: a holder that will
    /// never prepare, or an operator who changed their mind. Nothing about the
    /// map moved during preparation, so abandoning it is a version bump and a
    /// forgotten intent.
    AbortSplit {
        parent: PartitionId,
        expect_epoch: Epoch,
    },

    /// Freezes two adjacent parents while leaving both in the routing map.
    BeginMerge {
        generation: MergeGeneration,
    },

    /// Records durable merged-child preparation by one exact holder.
    MarkMergePrepared {
        generation: MergeGeneration,
        node: NodeId,
    },

    /// Atomically retires both parents and installs their prepared child.
    CompleteMerge {
        generation: MergeGeneration,
    },

    /// Abandons a merge and raises both parent epochs before gates reopen.
    AbortMerge {
        generation: MergeGeneration,
    },

    /// Advances the cluster's active protocol version, which is what
    /// `orbita cluster finalize-upgrade` commits.
    ///
    /// `expect` makes it safe to race, the same way `expect_epoch` does for a
    /// fence: two operators finalizing at once produce one advance and one
    /// clear refusal rather than a double bump.
    SetClusterVersion {
        version: ClusterVersion,
        expect: ClusterVersion,
    },
}

impl ControlCommand {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut w = Writer::new();
        match self {
            ControlCommand::RegisterNode {
                node,
                role,
                address,
                speaks,
                ready,
                draining,
            } => {
                let tag = if *ready || *draining {
                    TAG_REGISTER_NODE_V3
                } else {
                    TAG_REGISTER_NODE_V2
                };
                w.u8(tag).u64(node.get()).u8(role_tag(*role)).str(address);
                speaks.encode(&mut w);
                if tag == TAG_REGISTER_NODE_V3 {
                    w.u8(u8::from(*ready)).u8(u8::from(*draining));
                }
            }
            ControlCommand::SetHealth { node, health } => {
                w.u8(TAG_SET_HEALTH).u64(node.get()).u8(health_tag(*health));
            }
            ControlCommand::ForgetNode { node } => {
                w.u8(TAG_FORGET_NODE).u64(node.get());
            }
            ControlCommand::CreateKeyspace {
                id,
                name,
                config,
                created_at_millis,
                first_partition,
                owner,
                replicas,
            } => {
                w.u8(TAG_CREATE_KEYSPACE).u64(id.get()).str(name);
                config.encode(&mut w);
                w.u64(*created_at_millis)
                    .u64(first_partition.get())
                    .opt_u64(owner.map(NodeId::get))
                    .seq(replicas, |w, n| {
                        w.u64(n.get());
                    });
            }
            ControlCommand::UpdateKeyspace { id, config } => {
                w.u8(TAG_UPDATE_KEYSPACE).u64(id.get());
                config.encode(&mut w);
            }
            ControlCommand::DeleteKeyspace { id } => {
                w.u8(TAG_DELETE_KEYSPACE).u64(id.get());
            }
            ControlCommand::CreateCredential { credential } => {
                w.u8(TAG_CREATE_CREDENTIAL);
                credential.encode(&mut w);
            }
            ControlCommand::RevokeCredential { id } => {
                w.u8(TAG_REVOKE_CREDENTIAL).str(id);
            }
            ControlCommand::FencePartition {
                partition,
                expect_epoch,
            } => {
                w.u8(TAG_FENCE_PARTITION)
                    .u64(partition.get())
                    .u64(expect_epoch.get());
            }
            ControlCommand::CompleteFenceDrain {
                partition,
                expect_epoch,
            } => {
                w.u8(TAG_COMPLETE_FENCE_DRAIN)
                    .u64(partition.get())
                    .u64(expect_epoch.get());
            }
            ControlCommand::AssignOwner {
                partition,
                owner,
                replicas,
                expect_epoch,
            } => {
                w.u8(TAG_ASSIGN_OWNER)
                    .u64(partition.get())
                    .u64(owner.get())
                    .seq(replicas, |w, n| {
                        w.u64(n.get());
                    })
                    .u64(expect_epoch.get());
            }
            ControlCommand::TransferOwnership {
                partition,
                from,
                to,
                replicas,
                expect_epoch,
            } => {
                w.u8(TAG_TRANSFER_OWNERSHIP)
                    .u64(partition.get())
                    .u64(from.get())
                    .u64(to.get())
                    .seq(replicas, |w, n| {
                        w.u64(n.get());
                    })
                    .u64(expect_epoch.get());
            }
            ControlCommand::SetReplicas {
                partition,
                replicas,
                expect_epoch,
            } => {
                w.u8(TAG_SET_REPLICAS)
                    .u64(partition.get())
                    .seq(replicas, |w, n| {
                        w.u64(n.get());
                    })
                    .u64(expect_epoch.get());
            }
            ControlCommand::SplitPartition {
                parent,
                at,
                lower,
                upper,
                expect_epoch,
            } => {
                w.u8(TAG_SPLIT_PARTITION)
                    .u64(parent.get())
                    .bytes(at)
                    .u64(lower.get())
                    .u64(upper.get())
                    .u64(expect_epoch.get());
            }
            ControlCommand::BeginSplit {
                parent,
                at,
                lower,
                upper,
                expect_epoch,
            } => {
                w.u8(TAG_BEGIN_SPLIT)
                    .u64(parent.get())
                    .bytes(at)
                    .u64(lower.get())
                    .u64(upper.get())
                    .u64(expect_epoch.get());
            }
            ControlCommand::MarkSplitPrepared {
                parent,
                node,
                expect_epoch,
            } => {
                w.u8(TAG_MARK_SPLIT_PREPARED)
                    .u64(parent.get())
                    .u64(node.get())
                    .u64(expect_epoch.get());
            }
            ControlCommand::CompleteSplit {
                parent,
                expect_epoch,
            } => {
                w.u8(TAG_COMPLETE_SPLIT)
                    .u64(parent.get())
                    .u64(expect_epoch.get());
            }
            ControlCommand::AbortSplit {
                parent,
                expect_epoch,
            } => {
                w.u8(TAG_ABORT_SPLIT)
                    .u64(parent.get())
                    .u64(expect_epoch.get());
            }
            ControlCommand::BeginMerge { generation } => {
                w.u8(TAG_BEGIN_MERGE);
                generation.encode(&mut w);
            }
            ControlCommand::MarkMergePrepared { generation, node } => {
                w.u8(TAG_MARK_MERGE_PREPARED);
                generation.encode(&mut w);
                w.u64(node.get());
            }
            ControlCommand::CompleteMerge { generation } => {
                w.u8(TAG_COMPLETE_MERGE);
                generation.encode(&mut w);
            }
            ControlCommand::AbortMerge { generation } => {
                w.u8(TAG_ABORT_MERGE);
                generation.encode(&mut w);
            }
            ControlCommand::SetClusterVersion { version, expect } => {
                w.u8(TAG_SET_CLUSTER_VERSION);
                version.encode(&mut w);
                expect.encode(&mut w);
            }
        }
        w.finish()
    }

    pub fn decode(buf: &[u8]) -> CodecResult<Self> {
        let mut r = Reader::new(buf);
        let command = match r.u8()? {
            // A v0.0.1 entry. It carries no speakable range because the
            // concept postdates it; `exactly(ZERO)` is the honest default,
            // and the node's next heartbeat corrects the record, since a
            // changed range triggers re-registration.
            TAG_REGISTER_NODE => ControlCommand::RegisterNode {
                node: NodeId(r.u64()?),
                role: role_from_tag(r.u8()?)?,
                address: r.string()?,
                speaks: VersionRange::exactly(ClusterVersion::ZERO),
                ready: false,
                draining: false,
            },
            TAG_SET_HEALTH => ControlCommand::SetHealth {
                node: NodeId(r.u64()?),
                health: health_from_tag(r.u8()?)?,
            },
            TAG_FORGET_NODE => ControlCommand::ForgetNode {
                node: NodeId(r.u64()?),
            },
            TAG_CREATE_KEYSPACE => {
                let id = KeyspaceId(r.u64()?);
                let name = r.string()?;
                let config = KeyspaceConfig::decode(&mut r)?;
                ControlCommand::CreateKeyspace {
                    id,
                    name,
                    config,
                    created_at_millis: r.u64()?,
                    first_partition: PartitionId(r.u64()?),
                    owner: r.opt_u64()?.map(NodeId),
                    replicas: r.seq(|r| Ok(NodeId(r.u64()?)))?,
                }
            }
            TAG_UPDATE_KEYSPACE => {
                let id = KeyspaceId(r.u64()?);
                ControlCommand::UpdateKeyspace {
                    id,
                    config: KeyspaceConfig::decode(&mut r)?,
                }
            }
            TAG_DELETE_KEYSPACE => ControlCommand::DeleteKeyspace {
                id: KeyspaceId(r.u64()?),
            },
            TAG_CREATE_CREDENTIAL => ControlCommand::CreateCredential {
                credential: Box::new(Credential::decode(&mut r)?),
            },
            TAG_REVOKE_CREDENTIAL => ControlCommand::RevokeCredential { id: r.string()? },
            TAG_FENCE_PARTITION => ControlCommand::FencePartition {
                partition: PartitionId(r.u64()?),
                expect_epoch: Epoch(r.u64()?),
            },
            TAG_COMPLETE_FENCE_DRAIN => ControlCommand::CompleteFenceDrain {
                partition: PartitionId(r.u64()?),
                expect_epoch: Epoch(r.u64()?),
            },
            TAG_ASSIGN_OWNER => ControlCommand::AssignOwner {
                partition: PartitionId(r.u64()?),
                owner: NodeId(r.u64()?),
                replicas: r.seq(|r| Ok(NodeId(r.u64()?)))?,
                expect_epoch: Epoch(r.u64()?),
            },
            TAG_SET_REPLICAS => ControlCommand::SetReplicas {
                partition: PartitionId(r.u64()?),
                replicas: r.seq(|r| Ok(NodeId(r.u64()?)))?,
                expect_epoch: Epoch(r.u64()?),
            },
            TAG_SPLIT_PARTITION => ControlCommand::SplitPartition {
                parent: PartitionId(r.u64()?),
                at: r.bytes()?,
                lower: PartitionId(r.u64()?),
                upper: PartitionId(r.u64()?),
                expect_epoch: Epoch(r.u64()?),
            },
            TAG_BEGIN_SPLIT => ControlCommand::BeginSplit {
                parent: PartitionId(r.u64()?),
                at: r.bytes()?,
                lower: PartitionId(r.u64()?),
                upper: PartitionId(r.u64()?),
                expect_epoch: Epoch(r.u64()?),
            },
            TAG_MARK_SPLIT_PREPARED => ControlCommand::MarkSplitPrepared {
                parent: PartitionId(r.u64()?),
                node: NodeId(r.u64()?),
                expect_epoch: Epoch(r.u64()?),
            },
            TAG_COMPLETE_SPLIT => ControlCommand::CompleteSplit {
                parent: PartitionId(r.u64()?),
                expect_epoch: Epoch(r.u64()?),
            },
            TAG_ABORT_SPLIT => ControlCommand::AbortSplit {
                parent: PartitionId(r.u64()?),
                expect_epoch: Epoch(r.u64()?),
            },
            TAG_BEGIN_MERGE => ControlCommand::BeginMerge {
                generation: MergeGeneration::decode(&mut r)?,
            },
            TAG_MARK_MERGE_PREPARED => ControlCommand::MarkMergePrepared {
                generation: MergeGeneration::decode(&mut r)?,
                node: NodeId(r.u64()?),
            },
            TAG_COMPLETE_MERGE => ControlCommand::CompleteMerge {
                generation: MergeGeneration::decode(&mut r)?,
            },
            TAG_ABORT_MERGE => ControlCommand::AbortMerge {
                generation: MergeGeneration::decode(&mut r)?,
            },
            TAG_SET_CLUSTER_VERSION => ControlCommand::SetClusterVersion {
                version: ClusterVersion::decode(&mut r)?,
                expect: ClusterVersion::decode(&mut r)?,
            },
            TAG_REGISTER_NODE_V2 => ControlCommand::RegisterNode {
                node: NodeId(r.u64()?),
                role: role_from_tag(r.u8()?)?,
                address: r.string()?,
                speaks: VersionRange::decode(&mut r)?,
                ready: false,
                draining: false,
            },
            TAG_REGISTER_NODE_V3 => ControlCommand::RegisterNode {
                node: NodeId(r.u64()?),
                role: role_from_tag(r.u8()?)?,
                address: r.string()?,
                speaks: VersionRange::decode(&mut r)?,
                ready: r.u8()? != 0,
                draining: r.u8()? != 0,
            },
            TAG_TRANSFER_OWNERSHIP => ControlCommand::TransferOwnership {
                partition: PartitionId(r.u64()?),
                from: NodeId(r.u64()?),
                to: NodeId(r.u64()?),
                replicas: r.seq(|r| Ok(NodeId(r.u64()?)))?,
                expect_epoch: Epoch(r.u64()?),
            },
            tag => {
                return Err(CodecError::UnknownTag {
                    what: "control command",
                    tag: u64::from(tag),
                })
            }
        };
        r.done()?;
        Ok(command)
    }
}

fn role_tag(role: NodeRole) -> u8 {
    match role {
        NodeRole::Leader => 1,
        NodeRole::Worker => 2,
    }
}

fn role_from_tag(tag: u8) -> CodecResult<NodeRole> {
    match tag {
        1 => Ok(NodeRole::Leader),
        2 => Ok(NodeRole::Worker),
        other => Err(CodecError::UnknownTag {
            what: "node role",
            tag: u64::from(other),
        }),
    }
}

fn health_tag(health: NodeHealth) -> u8 {
    match health {
        NodeHealth::Healthy => 1,
        NodeHealth::Suspect => 2,
        NodeHealth::Dead => 3,
    }
}

fn health_from_tag(tag: u8) -> CodecResult<NodeHealth> {
    match tag {
        1 => Ok(NodeHealth::Healthy),
        2 => Ok(NodeHealth::Suspect),
        3 => Ok(NodeHealth::Dead),
        other => Err(CodecError::UnknownTag {
            what: "node health",
            tag: u64::from(other),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{hash_secret, Permission};

    fn every_command() -> Vec<ControlCommand> {
        vec![
            ControlCommand::RegisterNode {
                node: NodeId(1),
                role: NodeRole::Worker,
                address: "10.0.0.1:7000".into(),
                speaks: VersionRange::new(ClusterVersion::new(0, 1), ClusterVersion::new(0, 2)),
                ready: true,
                draining: false,
            },
            ControlCommand::SetHealth {
                node: NodeId(1),
                health: NodeHealth::Suspect,
            },
            ControlCommand::ForgetNode { node: NodeId(4) },
            ControlCommand::CreateKeyspace {
                id: KeyspaceId(1),
                name: "catalog".into(),
                config: KeyspaceConfig {
                    default_ttl_millis: Some(1),
                    ..KeyspaceConfig::default()
                },
                created_at_millis: 5,
                first_partition: PartitionId(1),
                owner: Some(NodeId(2)),
                replicas: vec![NodeId(3), NodeId(4)],
            },
            ControlCommand::UpdateKeyspace {
                id: KeyspaceId(1),
                config: KeyspaceConfig::default(),
            },
            ControlCommand::DeleteKeyspace { id: KeyspaceId(1) },
            ControlCommand::CreateCredential {
                credential: Box::new(Credential {
                    id: "c".into(),
                    secret_hash: hash_secret("s"),
                    keyspaces: vec!["catalog".into()],
                    permissions: vec![Permission::Read],
                    description: String::new(),
                    created_at_millis: 0,
                    expires_at_millis: None,
                }),
            },
            ControlCommand::RevokeCredential { id: "c".into() },
            ControlCommand::FencePartition {
                partition: PartitionId(1),
                expect_epoch: Epoch(3),
            },
            ControlCommand::CompleteFenceDrain {
                partition: PartitionId(1),
                expect_epoch: Epoch(4),
            },
            ControlCommand::AssignOwner {
                partition: PartitionId(1),
                owner: NodeId(2),
                replicas: vec![NodeId(3), NodeId(4)],
                expect_epoch: Epoch(4),
            },
            ControlCommand::TransferOwnership {
                partition: PartitionId(1),
                from: NodeId(2),
                to: NodeId(3),
                replicas: vec![NodeId(4)],
                expect_epoch: Epoch(4),
            },
            ControlCommand::SetReplicas {
                partition: PartitionId(1),
                replicas: vec![NodeId(3)],
                expect_epoch: Epoch(4),
            },
            ControlCommand::SplitPartition {
                parent: PartitionId(1),
                at: Bytes::from_static(b"m"),
                lower: PartitionId(2),
                upper: PartitionId(3),
                expect_epoch: Epoch(4),
            },
            ControlCommand::BeginSplit {
                parent: PartitionId(1),
                at: Bytes::from_static(b"m"),
                lower: PartitionId(2),
                upper: PartitionId(3),
                expect_epoch: Epoch(4),
            },
            ControlCommand::MarkSplitPrepared {
                parent: PartitionId(1),
                node: NodeId(2),
                expect_epoch: Epoch(4),
            },
            ControlCommand::CompleteSplit {
                parent: PartitionId(1),
                expect_epoch: Epoch(4),
            },
            ControlCommand::AbortSplit {
                parent: PartitionId(1),
                expect_epoch: Epoch(4),
            },
            ControlCommand::BeginMerge {
                generation: merge_generation(),
            },
            ControlCommand::MarkMergePrepared {
                generation: merge_generation(),
                node: NodeId(2),
            },
            ControlCommand::CompleteMerge {
                generation: merge_generation(),
            },
            ControlCommand::AbortMerge {
                generation: merge_generation(),
            },
            ControlCommand::SetClusterVersion {
                version: ClusterVersion::new(0, 2),
                expect: ClusterVersion::new(0, 1),
            },
        ]
    }

    fn merge_generation() -> MergeGeneration {
        MergeGeneration {
            lower: PartitionId(1),
            upper: PartitionId(2),
            lower_epoch: Epoch(4),
            upper_epoch: Epoch(5),
            merged: PartitionId(3),
            boundary: Bytes::from_static(b"m"),
            range: KeyRange::unbounded(),
        }
    }

    #[test]
    fn every_command_round_trips() {
        for command in every_command() {
            let encoded = command.encode();
            assert_eq!(
                ControlCommand::decode(&encoded),
                Ok(command.clone()),
                "{command:?}"
            );
        }
    }

    #[test]
    fn a_register_node_entry_written_by_v0_0_1_still_decodes() {
        // v0.0.1 clusters hold these in their control logs, and recovery
        // truncates the log at the first entry it cannot decode. This is the
        // old shape byte for byte: tag 1, node, role, address, nothing else.
        // If this test breaks, upgrading a v0.0.1 control-plane node destroys
        // its committed state.
        let legacy = Writer::new()
            .u8(1)
            .u64(7)
            .u8(2)
            .str("10.0.0.7:7000")
            .finish();
        assert_eq!(
            ControlCommand::decode(&legacy),
            Ok(ControlCommand::RegisterNode {
                node: NodeId(7),
                role: NodeRole::Worker,
                address: "10.0.0.7:7000".into(),
                speaks: VersionRange::exactly(ClusterVersion::ZERO),
                ready: false,
                draining: false,
            })
        );
    }

    #[test]
    fn a_command_written_by_a_newer_binary_is_rejected_rather_than_misread() {
        // A log entry this binary does not understand has to stop recovery
        // loudly. Applying a partial decode would silently diverge this
        // member's state machine from the others.
        let encoded = Bytes::from_static(&[200, 0, 0, 0]);
        assert!(matches!(
            ControlCommand::decode(&encoded),
            Err(CodecError::UnknownTag { .. })
        ));
    }

    #[test]
    fn a_truncated_command_is_rejected() {
        let encoded = ControlCommand::SplitPartition {
            parent: PartitionId(1),
            at: Bytes::from_static(b"m"),
            lower: PartitionId(2),
            upper: PartitionId(3),
            expect_epoch: Epoch(4),
        }
        .encode();
        for cut in 1..encoded.len() {
            assert!(
                ControlCommand::decode(&encoded[..cut]).is_err(),
                "a command cut at {cut} must not decode"
            );
        }
    }
}
