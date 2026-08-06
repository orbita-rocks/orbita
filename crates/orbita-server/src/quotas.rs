//! Quota and rate-limit enforcement, driven through a real node.
//!
//! The bucket arithmetic is unit-tested in [`crate::quota`]; this is the other
//! half of the promise — that a configured keyspace quota actually turns a
//! request into a `ResourceExhausted`, that storage and rate refusals stay
//! distinguishable, and that a throttled tenant does not reach across into a
//! neighbour. It runs under the simulator so virtual time stands still between
//! requests unless a test advances it, which is what makes a rate assertion
//! deterministic.

use crate::map_source::{BoxedMapSource, StaticMapSource};
use crate::node::{DataLayout, Node};

use orbita_core::{
    Epoch, Error, KeyRange, KeyspaceId, KeyspaceInfo, KeyspaceName, MapVersion, NodeId,
    PartitionId, PartitionInfo, PartitionMap,
};
use orbita_format::testing::MemoryStore;
use orbita_proto::v1::{GetRequest, SetRequest};
use orbita_sim::{SimRuntime, Simulation};

use std::sync::Arc;

/// A keyspace configured with whatever quotas a test cares about, plus a single
/// unbounded partition this node owns.
struct KeyspaceSpec {
    id: u64,
    name: &'static str,
    max_storage_bytes: Option<u64>,
    max_reads_per_second: Option<u32>,
    max_writes_per_second: Option<u32>,
}

fn map(specs: &[KeyspaceSpec]) -> PartitionMap {
    let mut map = PartitionMap::new(MapVersion(1));
    for spec in specs {
        let keyspace = KeyspaceId(spec.id);
        map.insert_keyspace(KeyspaceInfo {
            id: keyspace,
            name: KeyspaceName::new(spec.name).unwrap(),
            default_ttl_millis: None,
            max_value_bytes: None,
            max_storage_bytes: spec.max_storage_bytes,
            max_reads_per_second: spec.max_reads_per_second,
            max_writes_per_second: spec.max_writes_per_second,
        });
        map.insert_partition(PartitionInfo {
            id: PartitionId(spec.id),
            keyspace,
            range: KeyRange::unbounded(),
            owner: Some(NodeId(1)),
            epoch: Epoch(1),
            replicas: Vec::new(),
        });
    }
    assert_eq!(
        map.check_coverage(),
        Ok(()),
        "the fixture must be coverable"
    );
    map
}

fn start(sim: &Simulation, specs: &[KeyspaceSpec]) -> Arc<Node<SimRuntime>> {
    let runtime = sim.add_node(NodeId(1));
    let layout = DataLayout {
        store: Arc::new(MemoryStore::new()),
        wal_root: "wal".to_string(),
        wal_segment_bytes: crate::DEFAULT_WAL_SEGMENT_BYTES,
    };
    let source = BoxedMapSource::new(StaticMapSource::new(map(specs)));
    sim.block_on(async move {
        Node::start(
            runtime,
            NodeId(1),
            layout,
            source,
            crate::DEFAULT_LEASE_DURATION,
            Arc::new(crate::ReadinessGate::new()),
        )
        .await
        .expect("the node starts")
    })
}

fn set(keyspace: &str, key: &str, value: &[u8]) -> SetRequest {
    SetRequest {
        keyspace: keyspace.to_string(),
        key: key.as_bytes().to_vec(),
        value: value.to_vec(),
        ttl_millis: None,
        condition: None,
    }
}

fn get(keyspace: &str, key: &str) -> GetRequest {
    GetRequest {
        keyspace: keyspace.to_string(),
        key: key.as_bytes().to_vec(),
    }
}

#[test]
fn a_write_that_would_exceed_the_storage_cap_is_refused_as_storage() {
    let sim = Simulation::new(1);
    let node = start(
        &sim,
        &[KeyspaceSpec {
            id: 1,
            name: "default",
            max_storage_bytes: Some(4),
            max_reads_per_second: None,
            max_writes_per_second: None,
        }],
    );

    let refused = {
        let node = Arc::clone(&node);
        sim.block_on(async move { node.set(set("default", "k", b"12345"), false).await })
    }
    .expect_err("a five-byte value cannot fit under a four-byte cap");
    match refused {
        Error::QuotaExceeded(message) => assert!(
            message.contains("storage"),
            "a storage refusal has to be tellable from a rate one, got: {message}"
        ),
        other => panic!("expected a quota refusal, got {other:?}"),
    }
}

#[test]
fn a_write_under_the_storage_cap_is_admitted() {
    let sim = Simulation::new(1);
    let node = start(
        &sim,
        &[KeyspaceSpec {
            id: 1,
            name: "default",
            max_storage_bytes: Some(1 << 20),
            max_reads_per_second: None,
            max_writes_per_second: None,
        }],
    );

    let written = {
        let node = Arc::clone(&node);
        sim.block_on(async move { node.set(set("default", "k", b"small"), false).await })
    }
    .expect("a small write fits under a generous cap");
    assert!(written.applied);
}

#[test]
fn exceeding_the_write_rate_is_refused_as_a_write_rate() {
    let sim = Simulation::new(1);
    let node = start(
        &sim,
        &[KeyspaceSpec {
            id: 1,
            name: "default",
            max_storage_bytes: None,
            max_reads_per_second: None,
            max_writes_per_second: Some(1),
        }],
    );

    // Virtual time does not advance between these, so the one-per-second bucket
    // starts full, admits one, and refuses the next.
    let first = {
        let node = Arc::clone(&node);
        sim.block_on(async move { node.set(set("default", "a", b"v"), false).await })
    };
    assert!(first.expect("the first write is within budget").applied);

    let second = {
        let node = Arc::clone(&node);
        sim.block_on(async move { node.set(set("default", "b", b"v"), false).await })
    }
    .expect_err("the second write in the same instant is over the write rate");
    match second {
        Error::QuotaExceeded(message) => assert!(
            message.contains("write rate"),
            "a write-rate refusal names the write rate, got: {message}"
        ),
        other => panic!("expected a quota refusal, got {other:?}"),
    }
}

#[test]
fn exceeding_the_read_rate_is_refused_independently_of_writes() {
    let sim = Simulation::new(1);
    let node = start(
        &sim,
        &[KeyspaceSpec {
            id: 1,
            name: "default",
            max_storage_bytes: None,
            max_reads_per_second: Some(1),
            // A generous write rate proves the read budget is its own counter.
            max_writes_per_second: Some(1_000),
        }],
    );

    // A write does not spend the read budget.
    {
        let node = Arc::clone(&node);
        sim.block_on(async move { node.set(set("default", "a", b"v"), false).await })
    }
    .expect("a write is not charged against reads");

    let first = {
        let node = Arc::clone(&node);
        sim.block_on(async move { node.get(get("default", "a"), false).await })
    };
    assert!(first.expect("the first read is within budget").found);

    let second = {
        let node = Arc::clone(&node);
        sim.block_on(async move { node.get(get("default", "a"), false).await })
    }
    .expect_err("the second read in the same instant is over the read rate");
    match second {
        Error::QuotaExceeded(message) => assert!(
            message.contains("read rate"),
            "a read-rate refusal names the read rate, got: {message}"
        ),
        other => panic!("expected a quota refusal, got {other:?}"),
    }
}

#[test]
fn a_throttled_neighbour_does_not_stop_a_keyspace_under_its_cap() {
    let sim = Simulation::new(1);
    // Two keyspaces on the one node: a noisy tenant capped at one write per
    // second, and a quiet neighbour with no cap at all.
    let node = start(
        &sim,
        &[
            KeyspaceSpec {
                id: 1,
                name: "noisy",
                max_storage_bytes: None,
                max_reads_per_second: None,
                max_writes_per_second: Some(1),
            },
            KeyspaceSpec {
                id: 2,
                name: "quiet",
                max_storage_bytes: None,
                max_reads_per_second: None,
                max_writes_per_second: None,
            },
        ],
    );

    // Saturate the noisy tenant: the first write lands, the second is refused.
    {
        let node = Arc::clone(&node);
        sim.block_on(async move { node.set(set("noisy", "a", b"v"), false).await })
    }
    .expect("the noisy tenant's first write is within budget");
    {
        let node = Arc::clone(&node);
        sim.block_on(async move { node.set(set("noisy", "b", b"v"), false).await })
    }
    .expect_err("the noisy tenant is now throttled");

    // The neighbour, reached without touching the noisy tenant's limiter, is
    // completely unaffected.
    let quiet = {
        let node = Arc::clone(&node);
        sim.block_on(async move { node.set(set("quiet", "a", b"v"), false).await })
    }
    .expect("a keyspace under its cap must not feel a throttled neighbour");
    assert!(quiet.applied);
}
