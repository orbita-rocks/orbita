//! What a node reports about itself, and what the leader group concludes from
//! the reports.
//!
//! The split matters. [`NodeStatus`] is a fact the node asserts: its role, its
//! address, and how far it has got on every partition it holds. [`NodeHealth`]
//! is a judgement the leader group makes from the absence of those facts, and
//! no node ever reports its own health. A node that could tell you it was dead
//! would not be.

use crate::codec::{CodecResult, Reader, Writer};
use crate::version::VersionRange;

use orbita_core::{Lamport, MapVersion, PartitionId};

/// Which half of the cluster a node is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NodeRole {
    /// A member of the leader group. Holds metadata, owns no partitions.
    Leader,
    /// Owns and replicates partitions, and serves the data path.
    Worker,
}

/// The leader group's opinion of a node, derived from heartbeat arrival.
///
/// `Suspect` exists so that a node which is merely slow is visible to an
/// operator before anything is done about it. Failover only acts on `Dead`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NodeHealth {
    Healthy,
    Suspect,
    Dead,
}

/// How far one node has got on one partition.
///
/// `durable_lamport` is the promotion input. It is what the node has on stable
/// storage, not what it has applied to its storage engine, because the promise is that no
/// acknowledged write is lost and an acknowledgement is paid for by durability
/// rather than by application.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionProgress {
    pub partition: PartitionId,
    pub durable_lamport: Lamport,
    pub applied_lamport: Lamport,
    pub size_bytes: u64,
}

impl PartitionProgress {
    pub(crate) fn encode(&self, w: &mut Writer) {
        w.u64(self.partition.get())
            .u64(self.durable_lamport.get())
            .u64(self.applied_lamport.get())
            .u64(self.size_bytes);
    }

    pub(crate) fn decode(r: &mut Reader<'_>) -> CodecResult<Self> {
        Ok(Self {
            partition: PartitionId(r.u64()?),
            durable_lamport: Lamport(r.u64()?),
            applied_lamport: Lamport(r.u64()?),
            size_bytes: r.u64()?,
        })
    }
}

/// One heartbeat's worth of self-report from a node.
///
/// Carrying the whole partition list on every heartbeat rather than a delta
/// keeps the leader group's view a function of the last message alone, so a
/// dropped heartbeat costs freshness and never leaves the two sides
/// disagreeing about what was already sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeStatus {
    pub role: NodeRole,
    /// Where peers reach this node. Reported rather than configured centrally
    /// so a node that moves does not need an operator to notice.
    pub address: String,
    /// The map version this node is routing on, which tells the leader group
    /// whether its published map has actually landed.
    pub map_version: MapVersion,
    /// The cluster versions this node's binary can speak. Reported on every
    /// heartbeat rather than once, so that a rolling update in which the
    /// binary changed under the same node id corrects the record without an
    /// operator noticing anything.
    pub speaks: VersionRange,
    /// Whether the node has recovered and caught up every partition it holds.
    /// This is reported readiness, not process liveness.
    pub ready: bool,
    /// A draining node remains healthy and serves its current ownership, but
    /// must not receive another assignment.
    pub draining: bool,
    pub partitions: Vec<PartitionProgress>,
}

impl NodeStatus {
    /// A report from a node that holds nothing yet, which is what a worker
    /// sends on its first heartbeat.
    #[must_use]
    pub fn joining(role: NodeRole, address: impl Into<String>) -> Self {
        Self {
            role,
            address: address.into(),
            map_version: MapVersion::default(),
            speaks: crate::version::binary_speaks(),
            ready: false,
            draining: false,
            partitions: Vec::new(),
        }
    }

    #[must_use]
    pub fn progress(&self, partition: PartitionId) -> Option<PartitionProgress> {
        self.partitions
            .iter()
            .copied()
            .find(|p| p.partition == partition)
    }

    pub(crate) fn encode(&self, w: &mut Writer) {
        self.encode_head(w);
        self.speaks.encode(w);
        w.u8(u8::from(self.ready)).u8(u8::from(self.draining));
        w.seq(&self.partitions, |w, p| p.encode(w));
    }

    /// The first version-aware encoding, which predates readiness reporting.
    pub(crate) fn encode_v2(&self, w: &mut Writer) {
        self.encode_head(w);
        self.speaks.encode(w);
        w.seq(&self.partitions, |w, p| p.encode(w));
    }

    /// The v0.0.1 encoding, which has no speakable range.
    ///
    /// Sent only when the leader answering turned out to be a v0.0.1 binary,
    /// so a mid-rollout heartbeat lands instead of being undecodable. Delete
    /// this when the compatibility window moves past 0.0.
    pub(crate) fn encode_legacy(&self, w: &mut Writer) {
        self.encode_head(w);
        w.seq(&self.partitions, |w, p| p.encode(w));
    }

    fn encode_head(&self, w: &mut Writer) {
        w.u8(match self.role {
            NodeRole::Leader => 0,
            NodeRole::Worker => 1,
        })
        .str(&self.address)
        .u64(self.map_version.get());
    }

    pub(crate) fn decode(r: &mut Reader<'_>) -> CodecResult<Self> {
        let (role, address, map_version) = Self::decode_head(r)?;
        Ok(Self {
            role,
            address,
            map_version,
            speaks: VersionRange::decode(r)?,
            ready: r.u8()? != 0,
            draining: r.u8()? != 0,
            partitions: r.seq(PartitionProgress::decode)?,
        })
    }

    /// Decodes the first version-aware encoding. Absence is not evidence of
    /// readiness, so an old node is never selected as a planned handoff target.
    pub(crate) fn decode_v2(r: &mut Reader<'_>) -> CodecResult<Self> {
        let (role, address, map_version) = Self::decode_head(r)?;
        Ok(Self {
            role,
            address,
            map_version,
            speaks: VersionRange::decode(r)?,
            ready: false,
            draining: false,
            partitions: r.seq(PartitionProgress::decode)?,
        })
    }

    /// Decodes the v0.0.1 encoding.
    ///
    /// The range defaults to "speaks nothing but 0.0", which is what a binary
    /// old enough to send this shape actually speaks. Delete alongside
    /// [`NodeStatus::encode_legacy`].
    pub(crate) fn decode_legacy(r: &mut Reader<'_>) -> CodecResult<Self> {
        let (role, address, map_version) = Self::decode_head(r)?;
        Ok(Self {
            role,
            address,
            map_version,
            speaks: VersionRange::exactly(crate::version::ClusterVersion::ZERO),
            ready: false,
            draining: false,
            partitions: r.seq(PartitionProgress::decode)?,
        })
    }

    fn decode_head(r: &mut Reader<'_>) -> CodecResult<(NodeRole, String, MapVersion)> {
        let role = match r.u8()? {
            0 => NodeRole::Leader,
            1 => NodeRole::Worker,
            tag => {
                return Err(crate::codec::CodecError::UnknownTag {
                    what: "node role",
                    tag: u64::from(tag),
                })
            }
        };
        Ok((role, r.string()?, MapVersion(r.u64()?)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_status_report_round_trips() {
        let status = NodeStatus {
            role: NodeRole::Worker,
            address: "10.0.0.4:7000".into(),
            map_version: MapVersion(12),
            speaks: crate::version::VersionRange::new(
                crate::version::ClusterVersion::new(0, 1),
                crate::version::ClusterVersion::new(0, 2),
            ),
            ready: true,
            draining: false,
            partitions: vec![PartitionProgress {
                partition: PartitionId(3),
                durable_lamport: Lamport(90),
                applied_lamport: Lamport(88),
                size_bytes: 4096,
            }],
        };

        let mut w = Writer::new();
        status.encode(&mut w);
        let encoded = w.finish();

        let mut r = Reader::new(&encoded);
        assert_eq!(NodeStatus::decode(&mut r).unwrap(), status);
        assert_eq!(r.done(), Ok(()));
    }

    #[test]
    fn an_unknown_role_is_rejected_rather_than_guessed() {
        let encoded = Writer::new().u8(9).finish();
        let mut r = Reader::new(&encoded);
        assert!(NodeStatus::decode(&mut r).is_err());
    }
}
