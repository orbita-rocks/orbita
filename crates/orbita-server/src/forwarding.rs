//! Two nodes, one keyspace, and a client that never learns there are two.
//!
//! A worker that does not own a key forwards the client's own request to the
//! node that does. That path only exists when there is more than one node, so
//! it is exercised here under the simulator's transport rather than over real
//! sockets: the routing decision, the proxy envelope, and the refusal to make
//! a second hop are all above the transport, and the simulator is where a
//! dropped or reordered message can be injected later.

use crate::map_source::{BoxedMapSource, StaticMapSource};
use crate::node::{DataLayout, Node};

use orbita_core::{
    Epoch, Error, KeyRange, KeyspaceId, KeyspaceInfo, KeyspaceName, MapVersion, NodeId,
    PartitionId, PartitionInfo, PartitionMap,
};
use orbita_format::testing::MemoryStore;
use orbita_proto::v1::{GetRequest, ListRequest, SetRequest};
use orbita_runtime::Runtime;
use orbita_sim::{SimRuntime, Simulation};

use std::sync::Arc;

const KEYSPACE: &str = "default";
const BOUNDARY: &[u8] = b"m";

/// One keyspace split in two at `m`, with a different owner on each side.
fn split_map() -> PartitionMap {
    let keyspace = KeyspaceId(1);
    let mut map = PartitionMap::new(MapVersion(1));
    map.insert_keyspace(KeyspaceInfo {
        id: keyspace,
        name: KeyspaceName::new(KEYSPACE).unwrap(),
        default_ttl_millis: None,
        max_value_bytes: None,
        max_storage_bytes: None,
        max_reads_per_second: None,
        max_writes_per_second: None,
    });
    map.insert_partition(PartitionInfo {
        id: PartitionId(1),
        keyspace,
        range: KeyRange::new(
            bytes::Bytes::new(),
            Some(bytes::Bytes::from_static(BOUNDARY)),
        )
        .unwrap(),
        owner: Some(NodeId(1)),
        epoch: Epoch(1),
        replicas: Vec::new(),
    });
    map.insert_partition(PartitionInfo {
        id: PartitionId(2),
        keyspace,
        range: KeyRange::new(bytes::Bytes::from_static(BOUNDARY), None).unwrap(),
        owner: Some(NodeId(2)),
        epoch: Epoch(1),
        replicas: Vec::new(),
    });
    assert_eq!(
        map.check_coverage(),
        Ok(()),
        "the fixture must cover the keyspace"
    );
    map
}

fn start(sim: &Simulation, node: NodeId) -> Arc<Node<SimRuntime>> {
    let runtime = sim.add_node(node);
    // Each node persists into its own in-memory store, so a run touches no
    // real filesystem and stays deterministic.
    let layout = DataLayout {
        store: Arc::new(MemoryStore::new()),
        wal_root: "wal".to_string(),
        wal_segment_bytes: crate::DEFAULT_WAL_SEGMENT_BYTES,
    };
    let source = BoxedMapSource::new(StaticMapSource::new(split_map()));
    sim.block_on(async move {
        // Authentication off: this suite exercises forwarding, not the
        // credential gate, so admission lets every request straight through to
        // the routing it is testing.
        let authenticator = Arc::new(crate::auth::Authenticator::new(
            false,
            None,
            std::time::Duration::from_secs(86_400),
            runtime.clock().clone(),
        ));
        Node::start(
            runtime,
            node,
            layout,
            source,
            Vec::new(),
            crate::DEFAULT_LEASE_DURATION,
            Arc::new(crate::ReadinessGate::new()),
            authenticator,
        )
        .await
        .expect("the node starts")
    })
}

fn set(key: &str, value: &str) -> SetRequest {
    SetRequest {
        keyspace: KEYSPACE.to_string(),
        key: key.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
        ttl_millis: None,
        condition: None,
    }
}

fn get(key: &str) -> GetRequest {
    GetRequest {
        keyspace: KEYSPACE.to_string(),
        key: key.as_bytes().to_vec(),
    }
}

#[test]
fn a_request_for_a_key_this_node_does_not_own_is_served_by_the_one_that_does() {
    let sim = Simulation::new(1);
    let first = start(&sim, NodeId(1));
    let second = start(&sim, NodeId(2));

    // "zebra" is past the boundary, so node one owns none of it and has to
    // forward. The client is talking to node one throughout.
    let written = {
        let node = Arc::clone(&first);
        sim.block_on(async move { node.set(set("zebra", "striped"), false, None).await })
    }
    .expect("a forwarded write succeeds");
    assert!(written.applied);

    let read = {
        let node = Arc::clone(&first);
        sim.block_on(async move { node.get(get("zebra"), false, None).await })
    }
    .expect("a forwarded read succeeds");
    assert!(read.found);
    assert_eq!(read.value, b"striped");
    assert_eq!(read.version, written.version);

    // And the owner has it locally, which is what proves the write went to the
    // right place rather than being served from the wrong one.
    let direct = {
        let node = Arc::clone(&second);
        sim.block_on(async move { node.get(get("zebra"), false, None).await })
    }
    .expect("the owner has the key");
    assert_eq!(direct.value, b"striped");

    drop(first);
    drop(second);
}

#[test]
fn each_node_serves_the_half_of_the_keyspace_it_owns() {
    let sim = Simulation::new(2);
    let first = start(&sim, NodeId(1));
    let _second = start(&sim, NodeId(2));

    for key in ["apple", "zebra"] {
        let node = Arc::clone(&first);
        let key = key.to_string();
        let written = sim
            .block_on(async move { node.set(set(&key, "v"), false, None).await })
            .expect("a client can write anywhere in the keyspace through any node");
        assert!(written.applied);
    }

    // A scan starting before the boundary pages into the other partition, and
    // the cursor is what carries it there.
    let mut seen = Vec::new();
    let mut cursor = Vec::new();
    for _ in 0..5 {
        let node = Arc::clone(&first);
        let request = ListRequest {
            keyspace: KEYSPACE.to_string(),
            prefix: Vec::new(),
            cursor: cursor.clone(),
            limit: 10,
            include_values: false,
        };
        let page = sim
            .block_on(async move { node.list(request, false, None).await })
            .expect("a page");
        seen.extend(page.entries.iter().map(|e| e.key.clone()));
        cursor = page.next_cursor;
        if cursor.is_empty() {
            break;
        }
    }

    assert_eq!(
        seen,
        vec![b"apple".to_vec(), b"zebra".to_vec()],
        "a scan spanning two partitions visits every key once, in order"
    );
}

#[test]
fn a_forwarded_request_is_never_forwarded_again() {
    // A stale map anywhere in the cluster would otherwise turn one request
    // into a loop between two nodes that each think the other owns the key.
    let sim = Simulation::new(3);
    let first = start(&sim, NodeId(1));

    let node = Arc::clone(&first);
    let refused = sim
        .block_on(async move { node.get(get("zebra"), true, None).await })
        .expect_err("a node that was forwarded a key it does not own must refuse");

    assert!(
        matches!(refused, Error::NotOwner { owner: Some(owner), .. } if owner == NodeId(2)),
        "the refusal names the current owner so the origin can repair its map, got {refused}"
    );
}
