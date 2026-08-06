//! The worker-to-leader-group protocol.
//!
//! Two methods: fetch the partition map, and report a node's own status. That
//! is the whole data plane facing surface of the control plane, and keeping it
//! that small is deliberate. Everything an operator does goes through the
//! `Admin` gRPC service instead, so the path a worker depends on stays
//! something you can hold in your head.
//!
//! Everything is little endian, framed by [`crate::codec`].

use crate::codec::{CodecError, CodecResult, Reader, Writer};
use crate::membership::NodeStatus;
use crate::model::Credential;
use crate::version::{ClusterVersion, CompatibilityRefusal, VersionRange};

use bytes::Bytes;
use orbita_core::{
    Epoch, KeyRange, KeyspaceId, KeyspaceInfo, KeyspaceName, MapVersion, NodeId, PartitionId,
    PartitionInfo, PartitionMap,
};

pub const METHOD_FETCH_MAP: u16 = 1;
/// The v0.0.1 status report: no speakable range in the request, no cluster
/// version in the reply. Served for one release window so that a rolling
/// update between 0.0 and the first version-aware release does not black out
/// heartbeats across the boundary; delete when the window moves past 0.0.
pub const METHOD_REPORT_STATUS: u16 = 2;
pub const METHOD_FETCH_NODES: u16 = 3;
/// The status report carrying the speakable range, answered with the active
/// cluster version. A client that gets "unknown control method" back falls
/// back to [`METHOD_REPORT_STATUS`], which is how a new worker heartbeats an
/// old leader mid-rollout.
pub const METHOD_REPORT_STATUS_V2: u16 = 4;
/// Reads the leader's committed control-command index. A restarting voter uses
/// this as catch-up authority before it reports Ready.
pub const METHOD_FETCH_COMMIT_INDEX: u16 = 5;
/// Status reporting with readiness and draining state.
pub const METHOD_REPORT_STATUS_V3: u16 = 6;
/// A worker asks the control leader to hand off every partition it owns.
pub const METHOD_DRAIN_NODE: u16 = 7;
/// Status reporting that carries what each partition's index costs in memory.
/// A separate method rather than a wider V3 payload because the progress list
/// is fixed-width per entry: a leader decoding the old shape would read the
/// new field as the next partition id.
pub const METHOD_REPORT_STATUS_V4: u16 = 8;
/// Status reporting that carries the owner's committed prefix beside its
/// durable position.
///
/// A separate method for the same reason V4 was: the progress list decodes a
/// fixed sequence of fields per entry, so a V4 leader reading a sixth field
/// would take it for the next partition's id. That is the rule for anything
/// added per partition, and it is why a new rung is cheaper than it looks —
/// the fallback below already knows how to lose a field and keep the
/// heartbeat.
pub const METHOD_REPORT_STATUS_V5: u16 = 9;
/// Fetches every live credential, secret hashes and all, so a worker can
/// enforce authentication against a cached copy rather than a control-plane
/// round trip per request. A worker polls this on the same timer as its map.
pub const METHOD_FETCH_CREDENTIALS: u16 = 10;

const STATUS_MAP: u8 = 0;
const STATUS_ACCEPTED: u8 = 1;
const STATUS_NOT_LEADER: u8 = 2;
const STATUS_ERROR: u8 = 3;
const STATUS_NODES: u8 = 4;
const STATUS_COMMIT_INDEX: u8 = 5;
const STATUS_UNAVAILABLE: u8 = 6;
const STATUS_INCOMPATIBLE: u8 = 7;
const STATUS_DRAIN_PROGRESS: u8 = 8;
const STATUS_CREDENTIALS: u8 = 9;

/// Asks for the map, saying what the caller already has.
///
/// The version comparison happens on the leader so that a worker polling every
/// second in a quiet cluster pays a few bytes rather than the whole map.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FetchMapRequest {
    pub have: MapVersion,
}

impl FetchMapRequest {
    pub(crate) fn encode(&self) -> Bytes {
        Writer::new().u64(self.have.get()).finish()
    }

    pub(crate) fn decode(buf: &[u8]) -> CodecResult<Self> {
        let mut r = Reader::new(buf);
        let have = MapVersion(r.u64()?);
        r.done()?;
        Ok(Self { have })
    }
}

/// One heartbeat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReportStatusRequest {
    pub node: NodeId,
    pub status: NodeStatus,
}

impl ReportStatusRequest {
    pub(crate) fn encode(&self) -> Bytes {
        let mut w = Writer::new();
        w.u64(self.node.get());
        self.status.encode(&mut w);
        w.finish()
    }

    pub(crate) fn decode(buf: &[u8]) -> CodecResult<Self> {
        let mut r = Reader::new(buf);
        let node = NodeId(r.u64()?);
        let status = NodeStatus::decode(&mut r)?;
        r.done()?;
        Ok(Self { node, status })
    }

    pub(crate) fn encode_v4(&self) -> Bytes {
        let mut w = Writer::new();
        w.u64(self.node.get());
        self.status.encode_v4(&mut w);
        w.finish()
    }

    pub(crate) fn decode_v4(buf: &[u8]) -> CodecResult<Self> {
        let mut r = Reader::new(buf);
        let node = NodeId(r.u64()?);
        let status = NodeStatus::decode_v4(&mut r)?;
        r.done()?;
        Ok(Self { node, status })
    }

    pub(crate) fn encode_v3(&self) -> Bytes {
        let mut w = Writer::new();
        w.u64(self.node.get());
        self.status.encode_v3(&mut w);
        w.finish()
    }

    pub(crate) fn decode_v3(buf: &[u8]) -> CodecResult<Self> {
        let mut r = Reader::new(buf);
        let node = NodeId(r.u64()?);
        let status = NodeStatus::decode_v3(&mut r)?;
        r.done()?;
        Ok(Self { node, status })
    }

    pub(crate) fn encode_v2(&self) -> Bytes {
        let mut w = Writer::new();
        w.u64(self.node.get());
        self.status.encode_v2(&mut w);
        w.finish()
    }

    pub(crate) fn decode_v2(buf: &[u8]) -> CodecResult<Self> {
        let mut r = Reader::new(buf);
        let node = NodeId(r.u64()?);
        let status = NodeStatus::decode_v2(&mut r)?;
        r.done()?;
        Ok(Self { node, status })
    }

    /// The v0.0.1 payload shape, for [`super::wire::METHOD_REPORT_STATUS`].
    pub(crate) fn encode_legacy(&self) -> Bytes {
        let mut w = Writer::new();
        w.u64(self.node.get());
        self.status.encode_legacy(&mut w);
        w.finish()
    }

    pub(crate) fn decode_legacy(buf: &[u8]) -> CodecResult<Self> {
        let mut r = Reader::new(buf);
        let node = NodeId(r.u64()?);
        let status = NodeStatus::decode_legacy(&mut r)?;
        r.done()?;
        Ok(Self { node, status })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DrainNodeRequest {
    pub node: NodeId,
}

impl DrainNodeRequest {
    pub(crate) fn encode(self) -> Bytes {
        Writer::new().u64(self.node.get()).finish()
    }

    pub(crate) fn decode(buf: &[u8]) -> CodecResult<Self> {
        let mut r = Reader::new(buf);
        let request = Self {
            node: NodeId(r.u64()?),
        };
        r.done()?;
        Ok(request)
    }
}

/// The one response shape both methods share.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ControlResponse {
    /// `None` means the caller's version was already current.
    Map(Option<PartitionMap>),
    Accepted {
        map_version: MapVersion,
        /// The active cluster version, carried on every heartbeat reply so a
        /// node learns which version to speak in the same round trip that
        /// keeps it out of the failure detector.
        ///
        /// Optional because it rides at the end of the message: a v0.0.1
        /// leader's reply simply ends after the map version, and a reply to a
        /// v0.0.1 worker must end there too or the worker rejects it as
        /// trailing bytes. `None` means "the other side predates versions".
        cluster_version: Option<ClusterVersion>,
    },
    /// Carries the leader so the caller retries in one hop rather than
    /// sweeping the whole group.
    NotLeader {
        leader: Option<NodeId>,
    },
    /// Every node the cluster knows, and where peers reach it.
    ///
    /// A worker needs this because the partition map names owners and replicas
    /// by id and says nothing about how to dial them. Each node reports its
    /// own address on every heartbeat, so the leader group already holds the
    /// answer, and handing it back is what lets an operator configure the
    /// leader group and nothing else.
    Nodes(Vec<(NodeId, String)>),
    /// A drain request was accepted. `complete` becomes true only after every
    /// receiver in this node's handoff set has reported its transfer version.
    DrainProgress {
        complete: bool,
        map_version: MapVersion,
    },
    /// Every live credential, secret hashes and all.
    ///
    /// This is the one control-plane answer that carries secret material, and
    /// it carries only the hashes the replicated log already holds, never a
    /// secret. A worker caches it to enforce authentication locally.
    Credentials(Vec<Credential>),
    /// The leader's committed control-command index.
    CommitIndex(crate::LogIndex),
    /// This member cannot establish leader authority right now. Unlike a
    /// command refusal, callers may try another member or retry later.
    Unavailable(String),
    Incompatible(CompatibilityRefusal),
    Error(String),
}

impl ControlResponse {
    pub(crate) fn encode(&self) -> Bytes {
        let mut w = Writer::new();
        match self {
            ControlResponse::Map(map) => {
                w.u8(STATUS_MAP);
                match map {
                    Some(map) => {
                        w.u8(1);
                        encode_map(&mut w, map);
                    }
                    None => {
                        w.u8(0);
                    }
                }
            }
            ControlResponse::Accepted {
                map_version,
                cluster_version,
            } => {
                w.u8(STATUS_ACCEPTED).u64(map_version.get());
                if let Some(cluster_version) = cluster_version {
                    cluster_version.encode(&mut w);
                }
            }
            ControlResponse::NotLeader { leader } => {
                w.u8(STATUS_NOT_LEADER)
                    .opt_u64(leader.map(orbita_core::NodeId::get));
            }
            ControlResponse::Nodes(nodes) => {
                w.u8(STATUS_NODES);
                w.seq(nodes, |w, (node, address)| {
                    w.u64(node.get()).str(address);
                });
            }
            ControlResponse::DrainProgress {
                complete,
                map_version,
            } => {
                w.u8(STATUS_DRAIN_PROGRESS)
                    .u8(u8::from(*complete))
                    .u64(map_version.get());
            }
            ControlResponse::Incompatible(refusal) => {
                w.u8(STATUS_INCOMPATIBLE);
                refusal.speaks.encode(&mut w);
                refusal.active.encode(&mut w);
            }
            ControlResponse::Credentials(credentials) => {
                w.u8(STATUS_CREDENTIALS);
                w.seq(credentials, |w, credential| credential.encode(w));
            }
            ControlResponse::CommitIndex(index) => {
                w.u8(STATUS_COMMIT_INDEX).u64(*index);
            }
            ControlResponse::Error(message) => {
                w.u8(STATUS_ERROR).str(message);
            }
            ControlResponse::Unavailable(message) => {
                w.u8(STATUS_UNAVAILABLE).str(message);
            }
        }
        w.finish()
    }

    pub(crate) fn decode(buf: &[u8]) -> CodecResult<Self> {
        let mut r = Reader::new(buf);
        let response = match r.u8()? {
            STATUS_MAP => {
                if r.u8()? == 0 {
                    ControlResponse::Map(None)
                } else {
                    ControlResponse::Map(Some(decode_map(&mut r)?))
                }
            }
            STATUS_ACCEPTED => ControlResponse::Accepted {
                map_version: MapVersion(r.u64()?),
                // The version is the last field, so a reply from a v0.0.1
                // leader is one that ends here. Absent is an answer, not an
                // error; the caller treats it as "no version learned".
                cluster_version: if r.has_more() {
                    Some(ClusterVersion::decode(&mut r)?)
                } else {
                    None
                },
            },
            STATUS_NOT_LEADER => ControlResponse::NotLeader {
                leader: r.opt_u64()?.map(NodeId),
            },
            STATUS_NODES => ControlResponse::Nodes(r.seq(|r| Ok((NodeId(r.u64()?), r.string()?)))?),
            STATUS_DRAIN_PROGRESS => ControlResponse::DrainProgress {
                complete: r.u8()? != 0,
                map_version: MapVersion(r.u64()?),
            },
            STATUS_INCOMPATIBLE => ControlResponse::Incompatible(CompatibilityRefusal {
                speaks: VersionRange::decode(&mut r)?,
                active: ClusterVersion::decode(&mut r)?,
            }),
            STATUS_CREDENTIALS => ControlResponse::Credentials(r.seq(|r| Credential::decode(r))?),
            STATUS_COMMIT_INDEX => ControlResponse::CommitIndex(r.u64()?),
            STATUS_ERROR => ControlResponse::Error(r.string()?),
            STATUS_UNAVAILABLE => ControlResponse::Unavailable(r.string()?),
            tag => {
                return Err(CodecError::UnknownTag {
                    what: "control response",
                    tag: u64::from(tag),
                })
            }
        };
        r.done()?;
        Ok(response)
    }
}

/// Encodes a map for the wire.
///
/// Keyspaces are recovered from the partitions rather than written separately,
/// because `PartitionMap` can be asked for a keyspace by id but cannot be
/// asked to list them. Every keyspace this crate creates is born with a
/// partition and never loses its last one, so the set of keyspaces named by
/// partitions is the complete set. See the crate documentation for the
/// contract gap this works around.
fn encode_map(w: &mut Writer, map: &PartitionMap) {
    let partitions: Vec<&PartitionInfo> = map.partitions().collect();

    let mut keyspace_ids: Vec<KeyspaceId> = partitions.iter().map(|p| p.keyspace).collect();
    keyspace_ids.sort_unstable();
    keyspace_ids.dedup();
    let keyspaces: Vec<&KeyspaceInfo> = keyspace_ids
        .iter()
        .filter_map(|id| map.keyspace(*id))
        .collect();

    w.u64(map.version().get());
    w.seq(&keyspaces, |w, info| {
        w.u64(info.id.get())
            .str(info.name.as_str())
            .opt_u64(info.default_ttl_millis)
            .opt_u64(info.max_value_bytes)
            .opt_u64(info.max_storage_bytes)
            .opt_u32(info.max_reads_per_second)
            .opt_u32(info.max_writes_per_second);
    });
    w.seq(&partitions, |w, info| {
        w.u64(info.id.get())
            .u64(info.keyspace.get())
            .bytes(info.range.start())
            .opt_bytes(info.range.end())
            .opt_u64(info.owner.map(orbita_core::NodeId::get))
            .u64(info.epoch.get())
            .seq(&info.replicas, |w, node| {
                w.u64(node.get());
            });
    });
}

fn decode_map(r: &mut Reader<'_>) -> CodecResult<PartitionMap> {
    let mut map = PartitionMap::new(MapVersion(r.u64()?));

    let keyspaces = r.seq(|r| {
        let id = KeyspaceId(r.u64()?);
        let name =
            KeyspaceName::new(r.string()?).map_err(|_| CodecError::OutOfRange("keyspace name"))?;
        Ok(KeyspaceInfo {
            id,
            name,
            default_ttl_millis: r.opt_u64()?,
            max_value_bytes: r.opt_u64()?,
            max_storage_bytes: r.opt_u64()?,
            max_reads_per_second: r.opt_u32()?,
            max_writes_per_second: r.opt_u32()?,
        })
    })?;
    for info in keyspaces {
        map.insert_keyspace(info);
    }

    let partitions = r.seq(|r| {
        let id = PartitionId(r.u64()?);
        let keyspace = KeyspaceId(r.u64()?);
        let start = r.bytes()?;
        let end = r.opt_bytes()?;
        let range = KeyRange::new(start, end).ok_or(CodecError::OutOfRange("partition range"))?;
        Ok(PartitionInfo {
            id,
            keyspace,
            range,
            owner: r.opt_u64()?.map(NodeId),
            epoch: Epoch(r.u64()?),
            replicas: r.seq(|r| Ok(NodeId(r.u64()?)))?,
        })
    })?;
    for info in partitions {
        map.insert_partition(info);
    }

    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::{NodeRole, PartitionProgress};
    use orbita_core::Lamport;

    fn map() -> PartitionMap {
        let mut map = PartitionMap::new(MapVersion(42));
        map.insert_keyspace(KeyspaceInfo {
            id: KeyspaceId(1),
            name: KeyspaceName::new("catalog").unwrap(),
            default_ttl_millis: Some(1_000),
            max_value_bytes: None,
            max_storage_bytes: Some(1 << 30),
            max_reads_per_second: Some(100),
            max_writes_per_second: None,
        });
        map.insert_partition(PartitionInfo {
            id: PartitionId(1),
            keyspace: KeyspaceId(1),
            range: KeyRange::new(Bytes::new(), Some(Bytes::from_static(b"m"))).unwrap(),
            owner: Some(NodeId(1)),
            epoch: Epoch(3),
            replicas: vec![NodeId(2), NodeId(3)],
        });
        map.insert_partition(PartitionInfo {
            id: PartitionId(2),
            keyspace: KeyspaceId(1),
            range: KeyRange::new(Bytes::from_static(b"m"), None).unwrap(),
            owner: None,
            epoch: Epoch(4),
            replicas: vec![],
        });
        map
    }

    #[test]
    fn a_partition_map_survives_the_wire_unchanged() {
        let original = map();
        let response = ControlResponse::Map(Some(original.clone()));
        let decoded = ControlResponse::decode(&response.encode()).unwrap();
        let ControlResponse::Map(Some(round_tripped)) = decoded else {
            panic!("expected a map");
        };

        assert_eq!(round_tripped, original);
        assert_eq!(round_tripped.check_coverage(), Ok(()));
        assert_eq!(
            round_tripped.lookup(KeyspaceId(1), b"zz").unwrap().owner,
            None,
            "an unowned partition stays unowned rather than becoming node zero"
        );
    }

    #[test]
    fn every_response_round_trips() {
        for response in [
            ControlResponse::Map(None),
            ControlResponse::Accepted {
                map_version: MapVersion(9),
                cluster_version: Some(ClusterVersion::new(0, 2)),
            },
            // What a v0.0.1 leader sends, and what a leader answering a
            // v0.0.1 worker must send.
            ControlResponse::Accepted {
                map_version: MapVersion(9),
                cluster_version: None,
            },
            ControlResponse::NotLeader {
                leader: Some(NodeId(2)),
            },
            ControlResponse::NotLeader { leader: None },
            ControlResponse::DrainProgress {
                complete: false,
                map_version: MapVersion(10),
            },
            ControlResponse::Incompatible(CompatibilityRefusal {
                speaks: VersionRange::new(ClusterVersion::new(0, 3), ClusterVersion::new(0, 4)),
                active: ClusterVersion::new(0, 2),
            }),
            ControlResponse::Unavailable("catching up".into()),
            ControlResponse::Error("no".into()),
            ControlResponse::CommitIndex(42),
            ControlResponse::Credentials(vec![Credential {
                id: "cred-1".into(),
                secret_hash: crate::model::hash_secret("s3cret"),
                keyspaces: vec!["catalog".into()],
                permissions: vec![crate::model::Permission::Read],
                description: "a worker's cached copy".into(),
                created_at_millis: 7,
                expires_at_millis: Some(99),
            }]),
        ] {
            assert_eq!(
                ControlResponse::decode(&response.encode()),
                Ok(response.clone())
            );
        }
    }

    #[test]
    fn drain_progress_round_trips_distinct_from_a_refusal() {
        for complete in [false, true] {
            let progress = ControlResponse::DrainProgress {
                complete,
                map_version: MapVersion(12),
            };
            assert_eq!(ControlResponse::decode(&progress.encode()), Ok(progress));
        }

        let progress = ControlResponse::DrainProgress {
            complete: false,
            map_version: MapVersion(12),
        };
        let refusal = ControlResponse::Error("node is not draining".into());
        assert_ne!(progress.encode()[0], refusal.encode()[0]);
        assert_eq!(ControlResponse::decode(&refusal.encode()), Ok(refusal));
    }

    #[test]
    fn a_status_report_round_trips() {
        let request = ReportStatusRequest {
            node: NodeId(7),
            status: NodeStatus {
                role: NodeRole::Worker,
                address: "10.0.0.7:7000".into(),
                map_version: MapVersion(3),
                speaks: crate::version::binary_speaks(),
                ready: true,
                draining: false,
                partitions: vec![PartitionProgress {
                    partition: PartitionId(1),
                    durable_lamport: Lamport(10),
                    applied_lamport: Lamport(9),
                    size_bytes: 1024,
                    index_bytes: Some(256),
                    committed_lamport: Some(Lamport(9)),
                }],
            },
        };
        assert_eq!(
            ReportStatusRequest::decode(&request.encode()),
            Ok(request.clone())
        );
    }

    #[test]
    fn a_status_report_carries_index_memory_only_on_the_newest_method() {
        // The progress list is fixed width per entry, so a leader decoding
        // the older shape would read index memory as the next partition id.
        // The two methods exist to keep that from being possible.
        let request = ReportStatusRequest {
            node: NodeId(7),
            status: NodeStatus {
                role: NodeRole::Worker,
                address: "10.0.0.7:7000".into(),
                map_version: MapVersion(3),
                speaks: crate::version::binary_speaks(),
                ready: true,
                draining: false,
                partitions: vec![PartitionProgress {
                    partition: PartitionId(1),
                    durable_lamport: Lamport(10),
                    applied_lamport: Lamport(9),
                    size_bytes: 1024,
                    index_bytes: Some(256),
                    committed_lamport: Some(Lamport(9)),
                }],
            },
        };

        assert_ne!(request.encode(), request.encode_v3());
        let through_v3 = ReportStatusRequest::decode_v3(&request.encode_v3()).unwrap();
        assert_eq!(
            through_v3.status.partitions[0].index_bytes, None,
            "a report that could not carry the measurement did not carry a zero either"
        );
        assert_eq!(through_v3.status.partitions[0].size_bytes, 1024);
        assert_eq!(
            ReportStatusRequest::decode(&request.encode())
                .unwrap()
                .status
                .partitions[0]
                .index_bytes,
            Some(256)
        );
    }

    #[test]
    fn a_v0_0_1_status_report_still_decodes_through_the_legacy_method() {
        // The legacy encoding is byte for byte what v0.0.1 sends on
        // METHOD_REPORT_STATUS. If this breaks, an old worker heartbeating a
        // new leader mid-rollout drops out of the failure detector.
        let request = ReportStatusRequest {
            node: NodeId(7),
            status: NodeStatus {
                role: NodeRole::Worker,
                address: "10.0.0.7:7000".into(),
                map_version: MapVersion(3),
                speaks: crate::version::VersionRange::exactly(crate::version::ClusterVersion::ZERO),
                ready: false,
                draining: false,
                partitions: vec![PartitionProgress {
                    partition: PartitionId(1),
                    durable_lamport: Lamport(10),
                    applied_lamport: Lamport(9),
                    size_bytes: 1024,
                    // A v0.0.1 report cannot carry either of these, so the
                    // only value that round trips through the legacy shape is
                    // the one meaning nobody said.
                    index_bytes: None,
                    committed_lamport: None,
                }],
            },
        };
        assert_eq!(
            ReportStatusRequest::decode_legacy(&request.encode_legacy()),
            Ok(request)
        );
    }

    #[test]
    fn a_fetch_request_round_trips() {
        let request = FetchMapRequest {
            have: MapVersion(11),
        };
        assert_eq!(FetchMapRequest::decode(&request.encode()), Ok(request));
    }

    #[test]
    fn a_truncated_response_is_rejected_rather_than_partially_believed() {
        let encoded = ControlResponse::Map(Some(map())).encode();
        for cut in 1..encoded.len() {
            assert!(
                ControlResponse::decode(&encoded[..cut]).is_err(),
                "a response cut at {cut} must not decode"
            );
        }
    }
}
