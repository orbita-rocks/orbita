//! No acknowledged write is lost when a partition splits under contention.
//!
//! This is the invariant the split's data-plane half was held on until it could
//! be proven: a client hammering the parent through the split must find every
//! write it saw acknowledged readable afterward, from exactly one child. The
//! danger is a write that is committed and acknowledged after the children have
//! captured their horizon but before the parent retires — a write in the
//! parent's log that no child inherits and no client knows failed. WAL fencing
//! is partition-id scoped, so a child (a new id) cannot fence such a write; the
//! parent has to *drain* it, not fence it, which is why the owner quiesces
//! before it prepares.
//!
//! The cluster is two nodes over one shared object store, the way #77 taught
//! this class of test has to be: a per-node store hides exactly the bug that
//! only appears when a child reopens over segments a different node published.
//! A spawned writer races the split; the assertion reads back every write it was
//! told was applied.

use crate::map_source::{BoxedMapSource, StaticMapSource};
use crate::node::{DataLayout, Node};

use orbita_control::WireSplitIntent;
use orbita_core::{
    Epoch, KeyRange, KeyspaceId, KeyspaceInfo, KeyspaceName, MapVersion, NodeId, PartitionId,
    PartitionInfo, PartitionMap,
};
use orbita_format::testing::MemoryStore;
use orbita_proto::v1::{GetRequest, SetRequest};
use orbita_runtime::{Clock, Runtime};
use orbita_sim::{harness, Failure, SimRuntime, Simulation};

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const KEYSPACE: &str = "default";
const PARENT: PartitionId = PartitionId(1);
const LOWER: PartitionId = PartitionId(2);
const UPPER: PartitionId = PartitionId(3);
const OWNER: NodeId = NodeId(1);
const REPLICA: NodeId = NodeId(2);
/// The split boundary. Keys below it land in [`LOWER`], keys at or above it in
/// [`UPPER`].
const BOUNDARY: &[u8] = b"m";

fn keyspace_info() -> KeyspaceInfo {
    KeyspaceInfo {
        id: KeyspaceId(1),
        name: KeyspaceName::new(KEYSPACE).expect("a literal name is valid"),
        default_ttl_millis: None,
        max_value_bytes: None,
        max_storage_bytes: None,
        max_reads_per_second: None,
        max_writes_per_second: None,
    }
}

/// The map before the split: one partition, owned and replicated.
fn parent_map() -> PartitionMap {
    let mut map = PartitionMap::new(MapVersion(1));
    map.insert_keyspace(keyspace_info());
    map.insert_partition(PartitionInfo {
        id: PARENT,
        keyspace: KeyspaceId(1),
        range: KeyRange::unbounded(),
        owner: Some(OWNER),
        epoch: Epoch(1),
        replicas: vec![REPLICA],
    });
    map
}

/// The map after the split: the parent retired, two children at the next epoch,
/// same owner and replica. This is what `CompleteSplit` produces, and swapping
/// the source to it stands in for that committed entry, whose atomicity and
/// prepare-gating are proven in `orbita-control`.
fn children_map() -> PartitionMap {
    let (low, high) = KeyRange::unbounded()
        .split_at(bytes::Bytes::from_static(BOUNDARY))
        .expect("the boundary is inside the range");
    let mut map = PartitionMap::new(MapVersion(3));
    map.insert_keyspace(keyspace_info());
    for (id, range) in [(LOWER, low), (UPPER, high)] {
        map.insert_partition(PartitionInfo {
            id,
            keyspace: KeyspaceId(1),
            range,
            owner: Some(OWNER),
            epoch: Epoch(2),
            replicas: vec![REPLICA],
        });
    }
    map
}

fn start_node(
    sim: &Simulation,
    node: NodeId,
    source: &StaticMapSource,
    store: Arc<MemoryStore>,
) -> Arc<Node<SimRuntime>> {
    let runtime = sim.add_node(node);
    let layout = DataLayout {
        store,
        wal_root: format!("wal-{}", node.get()),
        wal_segment_bytes: crate::DEFAULT_WAL_SEGMENT_BYTES,
    };
    let source = BoxedMapSource::new(source.clone());
    let gate = Arc::new(crate::ReadinessGate::new());
    sim.block_on(async move {
        let authenticator = Arc::new(crate::auth::Authenticator::new(
            false,
            None,
            Duration::from_secs(86_400),
            runtime.clock().clone(),
        ));
        Node::start(
            runtime,
            node,
            layout,
            source,
            Duration::from_millis(150),
            gate,
            authenticator,
        )
        .await
        .expect("the node starts")
    })
}

fn poll(sim: &Simulation, nodes: &[&Arc<Node<SimRuntime>>]) {
    let nodes: Vec<Arc<Node<SimRuntime>>> = nodes.iter().map(|n| Arc::clone(n)).collect();
    sim.block_on(async move {
        for _ in 0..2 {
            for node in &nodes {
                let _ = node.refresh_map().await;
            }
        }
    });
    // A bounded advance rather than run-until-idle, because a concurrent writer
    // keeps the world busy — it never goes idle until it is told to stop — and
    // this is called mid-scenario while the writer is live.
    sim.run_for(Duration::from_millis(100));
}

/// The key written at index `i`, alternating sides of the split boundary so a
/// split has data to divide.
fn key_at(i: u64) -> Vec<u8> {
    let side = if i % 2 == 0 { "a" } else { "n" };
    format!("{side}{i:07}").into_bytes()
}

/// A distinct value per index, so a wrong value is a distinguishable failure
/// from a missing one.
fn value_at(i: u64) -> Vec<u8> {
    format!("v-{i:07}").into_bytes()
}

/// One write the client made, and whether it was told the write applied.
struct Wrote {
    index: u64,
    acknowledged: bool,
}

/// Spawns a client that writes distinct keys through `node` until told to stop,
/// recording which ones it was told were applied.
fn spawn_writer(
    sim: &Simulation,
    node: &Arc<Node<SimRuntime>>,
    stop: &Arc<AtomicBool>,
    log: &Arc<Mutex<Vec<Wrote>>>,
) {
    let node = Arc::clone(node);
    let stop = Arc::clone(stop);
    let log = Arc::clone(log);
    let clock = sim.runtime(OWNER).clock().clone();
    let next = Arc::new(AtomicU64::new(0));
    sim.spawn(async move {
        while !stop.load(Ordering::Acquire) {
            let index = next.fetch_add(1, Ordering::Relaxed);
            let acknowledged = node
                .set(
                    SetRequest {
                        keyspace: KEYSPACE.to_string(),
                        key: key_at(index),
                        value: value_at(index),
                        ttl_millis: None,
                        condition: None,
                    },
                    false,
                    None,
                )
                .await
                .map(|response| response.applied)
                .unwrap_or(false);
            log.lock().expect("write log poisoned").push(Wrote {
                index,
                acknowledged,
            });
            // Yield so virtual time advances and the split can interleave. A
            // relaxed cadence keeps the whole scenario inside the simulator's
            // step budget while still landing writes on both sides of the split.
            clock.sleep(Duration::from_millis(40)).await;
        }
    });
}

/// Reads a key through `node`, which routes it to whichever partition owns it.
fn read(sim: &Simulation, node: &Arc<Node<SimRuntime>>, key: Vec<u8>) -> Option<Vec<u8>> {
    let node = Arc::clone(node);
    sim.block_on(async move {
        node.get(
            GetRequest {
                keyspace: KEYSPACE.to_string(),
                key,
            },
            false,
            None,
        )
        .await
        .ok()
        .filter(|response| response.found)
        .map(|response| response.value)
    })
}

/// The split's intent, as the leader would hand it to a worker.
fn intent() -> WireSplitIntent {
    WireSplitIntent {
        parent: PARENT,
        at: bytes::Bytes::from_static(BOUNDARY),
        lower: LOWER,
        upper: UPPER,
    }
}

#[test]
fn no_acknowledged_write_is_lost_across_a_split_under_contention() {
    // Reproduce a failure with:
    //   ORBITA_SIM_SEED=<seed> cargo test -p orbita-server \
    //     no_acknowledged_write_is_lost_across_a_split_under_contention
    harness::check_seeds(
        "split::no_acknowledged_write_is_lost_across_a_split_under_contention",
        24,
        run,
    );
}

#[test]
fn without_the_quiesce_gate_a_racing_write_is_lost() {
    // The proof the gate is load-bearing. Prepare the children — capturing the
    // horizon — then reopen admission, which is exactly what preparing without
    // quiescing would leave: a parent still taking writes above the horizon the
    // children inherit. A write the client is then told applied is gone once the
    // children publish, because it is in the parent's abandoned log and no child
    // references it. This asserts that loss, so a change that made the gate stop
    // mattering would fail here rather than pass silently.
    let sim = Simulation::new(1);
    let store = Arc::new(MemoryStore::new());
    let source = StaticMapSource::new(parent_map());
    let owner = start_node(&sim, OWNER, &source, Arc::clone(&store));
    let replica = start_node(&sim, REPLICA, &source, Arc::clone(&store));
    poll(&sim, &[&owner, &replica]);

    // Some data to divide, all below the horizon.
    for i in 0..4u64 {
        write_one(&sim, &owner, i);
    }
    sim.run_until_idle();

    // Prepare the children WITHOUT quiescing — the parent keeps admitting writes
    // and its log keeps taking them. The horizon is captured here.
    let children = {
        let (low, high) = KeyRange::unbounded()
            .split_at(bytes::Bytes::from_static(BOUNDARY))
            .unwrap();
        [
            orbita_storage::ChildSpec {
                id: LOWER,
                epoch: Epoch(2),
                range: low,
            },
            orbita_storage::ChildSpec {
                id: UPPER,
                epoch: Epoch(2),
                range: high,
            },
        ]
    };
    sim.block_on({
        let owner = Arc::clone(&owner);
        async move {
            owner
                .host(PARENT)
                .await
                .expect("the owner holds the parent")
                .prepare_children_without_quiescing(&children)
                .await
                .expect("children prepared");
        }
    });

    // Acknowledge one more write against the still-live parent, above the
    // horizon the children captured.
    let gap = 999u64;
    assert!(
        write_one(&sim, &owner, gap),
        "the un-quiesced parent still acknowledges the write"
    );
    sim.run_until_idle();

    // Retire the parent and install the children.
    source.set(children_map());
    poll(&sim, &[&owner, &replica]);

    assert!(
        read(&sim, &owner, key_at(gap)).is_none(),
        "without the quiesce gate a write acknowledged above the horizon is lost across the \
         split; the gate is what makes it safe"
    );
    // The writes below the horizon are still there, so the loss is specifically
    // the ungated tail and not the whole partition.
    for i in 0..4u64 {
        assert_eq!(read(&sim, &owner, key_at(i)), Some(value_at(i)), "key {i}");
    }
}

/// Writes one indexed key through `node`, returning whether it was acknowledged.
fn write_one(sim: &Simulation, node: &Arc<Node<SimRuntime>>, index: u64) -> bool {
    let node = Arc::clone(node);
    sim.block_on(async move {
        node.set(
            SetRequest {
                keyspace: KEYSPACE.to_string(),
                key: key_at(index),
                value: value_at(index),
                ttl_millis: None,
                condition: None,
            },
            false,
            None,
        )
        .await
        .map(|response| response.applied)
        .unwrap_or(false)
    })
}

#[allow(clippy::too_many_lines)]
fn run(seed: u64) -> Result<(), Failure> {
    let sim = Simulation::new(seed);
    let sim = &sim;
    let store = Arc::new(MemoryStore::new());
    let source = StaticMapSource::new(parent_map());
    let owner = start_node(sim, OWNER, &source, Arc::clone(&store));
    let replica = start_node(sim, REPLICA, &source, Arc::clone(&store));
    poll(sim, &[&owner, &replica]);

    let stop = Arc::new(AtomicBool::new(false));
    let log = Arc::new(Mutex::new(Vec::new()));
    spawn_writer(sim, &owner, &stop, &log);

    // Let a run of writes land on the live parent before the split opens.
    sim.run_for(Duration::from_millis(200));
    if log.lock().expect("write log poisoned").is_empty() {
        return Err(sim.failure("no write landed before the split, so this seed proves nothing"));
    }

    // The worker half of the split, running concurrently with the writer:
    // quiesce the parent and prepare both children over its segments. This is
    // the real path the control loop takes, and it returns the parent only once
    // both child manifests are durable.
    let prepared = {
        let owner = Arc::clone(&owner);
        sim.block_on(async move { owner.prepare_pending_splits(&[intent()]).await })
    };
    if prepared != vec![PARENT] {
        return Err(sim.failure(format!(
            "the owner did not report the split prepared: {prepared:?}"
        )));
    }

    // Retire the parent and install the children, then let both nodes reconcile
    // onto them. This is the committed CompleteSplit, whose atomicity is proven
    // in orbita-control; here it is a source swap.
    source.set(children_map());
    poll(sim, &[&owner, &replica]);

    // A second wave of writes, now against the children.
    sim.run_for(Duration::from_millis(200));
    stop.store(true, Ordering::Release);
    sim.run_until_idle();

    // Every write the client was told applied must be readable, with its exact
    // value, from exactly one child. A write that was refused — including one
    // the quiesce turned away — is allowed to be absent.
    let writes = log.lock().expect("write log poisoned");
    let acknowledged = writes.iter().filter(|w| w.acknowledged).count();
    if acknowledged == 0 {
        return Err(sim.failure("no write was acknowledged, so nothing was proven"));
    }
    for wrote in writes.iter().filter(|w| w.acknowledged) {
        let key = key_at(wrote.index);
        match read(sim, &owner, key.clone()) {
            Some(value) if value == value_at(wrote.index) => {}
            Some(other) => {
                return Err(sim.failure(format!(
                    "key {:?} read back {:?}, not the acknowledged {:?}",
                    String::from_utf8_lossy(&key),
                    other,
                    value_at(wrote.index)
                )))
            }
            None => {
                return Err(sim.failure(format!(
                    "an acknowledged write to {:?} was lost across the split",
                    String::from_utf8_lossy(&key)
                )))
            }
        }
    }

    // And each key sits under exactly the child that owns its half of the
    // range, which is the map-level exactly-one-owner property.
    let map = owner.map();
    for wrote in writes.iter().filter(|w| w.acknowledged) {
        let key = key_at(wrote.index);
        let expected = if key.as_slice() < BOUNDARY {
            LOWER
        } else {
            UPPER
        };
        match map.lookup(KeyspaceId(1), &key) {
            Some(info) if info.id == expected => {}
            other => {
                return Err(sim.failure(format!(
                    "key {:?} routes to {:?}, not the child {expected} that owns its range",
                    String::from_utf8_lossy(&key),
                    other.map(|i| i.id)
                )))
            }
        }
    }
    Ok(())
}
