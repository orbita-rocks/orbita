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

use bytes::Bytes;
use orbita_core::{
    Epoch, KeyRange, KeyspaceId, KeyspaceInfo, KeyspaceName, MapVersion, NodeId, PartitionId,
    PartitionInfo, PartitionMap,
};

pub const METHOD_FETCH_MAP: u16 = 1;
pub const METHOD_REPORT_STATUS: u16 = 2;
pub const METHOD_FETCH_NODES: u16 = 3;

const STATUS_MAP: u8 = 0;
const STATUS_ACCEPTED: u8 = 1;
const STATUS_NOT_LEADER: u8 = 2;
const STATUS_ERROR: u8 = 3;
const STATUS_NODES: u8 = 4;

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
}

/// The one response shape both methods share.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ControlResponse {
    /// `None` means the caller's version was already current.
    Map(Option<PartitionMap>),
    Accepted {
        map_version: MapVersion,
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
            ControlResponse::Accepted { map_version } => {
                w.u8(STATUS_ACCEPTED).u64(map_version.get());
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
            ControlResponse::Error(message) => {
                w.u8(STATUS_ERROR).str(message);
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
            },
            STATUS_NOT_LEADER => ControlResponse::NotLeader {
                leader: r.opt_u64()?.map(NodeId),
            },
            STATUS_NODES => ControlResponse::Nodes(r.seq(|r| Ok((NodeId(r.u64()?), r.string()?)))?),
            STATUS_ERROR => ControlResponse::Error(r.string()?),
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
            },
            ControlResponse::NotLeader {
                leader: Some(NodeId(2)),
            },
            ControlResponse::NotLeader { leader: None },
            ControlResponse::Error("no".into()),
        ] {
            assert_eq!(
                ControlResponse::decode(&response.encode()),
                Ok(response.clone())
            );
        }
    }

    #[test]
    fn a_status_report_round_trips() {
        let request = ReportStatusRequest {
            node: NodeId(7),
            status: NodeStatus {
                role: NodeRole::Worker,
                address: "10.0.0.7:7000".into(),
                map_version: MapVersion(3),
                partitions: vec![PartitionProgress {
                    partition: PartitionId(1),
                    durable_lamport: Lamport(10),
                    applied_lamport: Lamport(9),
                    size_bytes: 1024,
                }],
            },
        };
        assert_eq!(
            ReportStatusRequest::decode(&request.encode()),
            Ok(request.clone())
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
