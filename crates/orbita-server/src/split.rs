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

use orbita_control::{
    MergeGeneration, MergeIntentSnapshot, SplitIntentSnapshot, WireMergeIntent, WireSplitIntent,
};
use orbita_core::{
    Epoch, KeyRange, KeyspaceId, KeyspaceInfo, KeyspaceName, Lamport, MapVersion, NodeId,
    PartitionId, PartitionInfo, PartitionMap,
};
use orbita_format::testing::MemoryStore;
use orbita_proto::v1::{GetRequest, ListRequest, SetRequest};
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

/// The map with the parent still whole, at `epoch`, owned by `owner` with
/// `replicas` behind it.
///
/// An abort raises the parent's epoch without moving it — see
/// `ControlState::abort_split` — and a later failover moves it. Both are a
/// committed entry in production; here they are a source swap.
fn parent_map_at(
    version: MapVersion,
    epoch: Epoch,
    owner: NodeId,
    replicas: Vec<NodeId>,
) -> PartitionMap {
    let mut map = PartitionMap::new(version);
    map.insert_keyspace(keyspace_info());
    map.insert_partition(PartitionInfo {
        id: PARENT,
        keyspace: KeyspaceId(1),
        range: KeyRange::unbounded(),
        owner: Some(owner),
        epoch,
        replicas,
    });
    map
}

/// The map after the split: the parent retired, two children at the next epoch.
/// Swapping the source to it stands in for that committed entry, whose
/// atomicity and prepare-gating are proven in `orbita-control`.
///
/// Both children are placed on one owner here. `CompleteSplit` spreads them
/// when the holder set is large enough to spread across, so this is the
/// single-holder case — and it is also the shape the merge tests below need,
/// because a merge cuts both parents in one process. `Controller` relocates a
/// parent to reach it when a split did spread.
fn children_map() -> PartitionMap {
    children_map_owned(OWNER, OWNER)
}

/// The map after a split that spread its children, which is what
/// `CompleteSplit` produces whenever the holder set has room to spread. Each
/// child is owned by one holder and replicated by the other, so the pair is
/// served by two nodes instead of one. That is the whole point of splitting —
/// one owner serialises each partition, so adding partitions without adding
/// owners adds no write throughput. See issue #160.
fn spread_children_map() -> PartitionMap {
    children_map_owned(OWNER, REPLICA)
}

fn children_map_owned(lower_owner: NodeId, upper_owner: NodeId) -> PartitionMap {
    let (low, high) = KeyRange::unbounded()
        .split_at(bytes::Bytes::from_static(BOUNDARY))
        .expect("the boundary is inside the range");
    let mut map = PartitionMap::new(MapVersion(3));
    map.insert_keyspace(keyspace_info());
    for (id, range, owner) in [(LOWER, low, lower_owner), (UPPER, high, upper_owner)] {
        let other = if owner == OWNER { REPLICA } else { OWNER };
        map.insert_partition(PartitionInfo {
            id,
            keyspace: KeyspaceId(1),
            range,
            owner: Some(owner),
            epoch: Epoch(2),
            // The holder set is the parent's either way; only the role
            // assignment inside it changes. `complete_split` builds the child
            // rows the same way, and the owner is never also a replica.
            replicas: vec![other],
        });
    }
    map
}

fn merge_generation() -> MergeGeneration {
    MergeGeneration {
        lower: LOWER,
        upper: UPPER,
        lower_epoch: Epoch(2),
        upper_epoch: Epoch(2),
        merged: PartitionId(4),
        boundary: bytes::Bytes::from_static(BOUNDARY),
        range: KeyRange::unbounded(),
    }
}

fn merge_snapshot(prepared_by_this_node: bool) -> MergeIntentSnapshot {
    MergeIntentSnapshot {
        map_version: MapVersion(3),
        intents: vec![WireMergeIntent {
            generation: merge_generation(),
            prepared_by_this_node,
        }],
    }
}

fn start_merge_node(
    sim: &Simulation,
    node: NodeId,
    source: &StaticMapSource,
    store: Arc<MemoryStore>,
) -> Arc<Node<SimRuntime>> {
    start_node_with_snapshots(
        sim,
        node,
        source,
        store,
        Some(SplitIntentSnapshot {
            map_version: MapVersion(3),
            intents: Vec::new(),
        }),
        Some(MergeIntentSnapshot {
            map_version: MapVersion(3),
            intents: Vec::new(),
        }),
    )
}

fn merged_map() -> PartitionMap {
    merged_map_owned(OWNER, vec![REPLICA])
}

fn merged_map_owned(owner: NodeId, replicas: Vec<NodeId>) -> PartitionMap {
    let mut map = PartitionMap::new(MapVersion(5));
    map.insert_keyspace(keyspace_info());
    map.insert_partition(PartitionInfo {
        id: PartitionId(4),
        keyspace: KeyspaceId(1),
        range: KeyRange::unbounded(),
        owner: Some(owner),
        epoch: Epoch(3),
        replicas,
    });
    map
}

fn start_node(
    sim: &Simulation,
    node: NodeId,
    source: &StaticMapSource,
    store: Arc<MemoryStore>,
) -> Arc<Node<SimRuntime>> {
    start_node_with_intents(sim, node, source, store, Vec::new())
}

fn start_node_with_intents(
    sim: &Simulation,
    node: NodeId,
    source: &StaticMapSource,
    store: Arc<MemoryStore>,
    intents: Vec<WireSplitIntent>,
) -> Arc<Node<SimRuntime>> {
    start_node_with_snapshot(
        sim,
        node,
        source,
        store,
        Some(SplitIntentSnapshot {
            map_version: MapVersion(1),
            intents,
        }),
    )
}

fn start_node_with_snapshot(
    sim: &Simulation,
    node: NodeId,
    source: &StaticMapSource,
    store: Arc<MemoryStore>,
    snapshot: Option<SplitIntentSnapshot>,
) -> Arc<Node<SimRuntime>> {
    start_node_with_snapshots(sim, node, source, store, snapshot, None)
}

fn start_node_with_snapshots(
    sim: &Simulation,
    node: NodeId,
    source: &StaticMapSource,
    store: Arc<MemoryStore>,
    split_snapshot: Option<SplitIntentSnapshot>,
    merge_snapshot: Option<MergeIntentSnapshot>,
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
            split_snapshot,
            merge_snapshot,
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
        prepared_by_this_node: false,
    }
}

fn generation() -> (PartitionId, PartitionId, PartitionId) {
    (PARENT, LOWER, UPPER)
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

#[test]
fn the_flush_loop_does_not_compact_a_split_parent_while_it_is_pending() {
    // The ADR 0009 freeze at the node level. A pending split leaves the parent
    // owned and open; the background flush loop keeps visiting every owned host,
    // and a flush pass advances the counter that eventually compacts — which
    // deletes the segments the children now reference. The freeze takes the
    // parent out of the flush loop for the split's duration. Here the loop runs
    // well past the compaction trigger while the split is held pending, and the
    // children still read every pre-split key afterward.
    let sim = Simulation::new(1);
    let store = Arc::new(MemoryStore::new());
    let source = StaticMapSource::new(parent_map());
    let owner = start_node(&sim, OWNER, &source, Arc::clone(&store));
    let replica = start_node(&sim, REPLICA, &source, Arc::clone(&store));
    poll(&sim, &[&owner, &replica]);

    // Two segments to divide.
    for i in 0..2u64 {
        write_one(&sim, &owner, i);
    }
    flush_owned(&sim, &owner);
    for i in 2..4u64 {
        write_one(&sim, &owner, i);
    }
    flush_owned(&sim, &owner);

    // Begin the split through the real worker path: freeze and publish.
    let prepared = {
        let owner = Arc::clone(&owner);
        sim.block_on(async move { owner.prepare_pending_splits(&[intent()]).await })
    };
    assert_eq!(prepared, vec![generation()]);
    let mut acknowledged = intent();
    acknowledged.prepared_by_this_node = true;
    for _ in 0..3 {
        let owner = Arc::clone(&owner);
        let acknowledged = acknowledged.clone();
        assert!(
            sim.block_on(async move { owner.prepare_pending_splits(&[acknowledged]).await })
                .is_empty(),
            "an acknowledged holder does not prepare or report the split again"
        );
    }
    assert!(
        sim.block_on({
            let owner = Arc::clone(&owner);
            async move { owner.host(PARENT).await.unwrap().is_maintenance_frozen() }
        }),
        "a pending split freezes the parent's maintenance"
    );

    // Run the flush loop far past the compaction trigger. A frozen parent is
    // skipped, so its segments are never merged and deleted.
    for _ in 0..200 {
        flush_owned(&sim, &owner);
    }

    // Complete the split and read every pre-split key from the children.
    source.set(children_map());
    poll(&sim, &[&owner, &replica]);
    for i in 0..4u64 {
        assert_eq!(
            read(&sim, &owner, key_at(i)),
            Some(value_at(i)),
            "pre-split key {i} is still readable from its child after a long pending split"
        );
    }
}

#[test]
fn a_restarted_acknowledged_owner_is_gated_before_it_can_serve() {
    let sim = Simulation::new(17);
    let store = Arc::new(MemoryStore::new());
    let source = StaticMapSource::new(parent_map());
    let owner = start_node(&sim, OWNER, &source, Arc::clone(&store));
    let replica = start_node(&sim, REPLICA, &source, Arc::clone(&store));
    poll(&sim, &[&owner, &replica]);
    for i in 0..4 {
        assert!(write_one(&sim, &owner, i));
    }
    let prepared = {
        let owner = Arc::clone(&owner);
        sim.block_on(async move { owner.prepare_pending_splits(&[intent()]).await })
    };
    assert_eq!(prepared, vec![generation()]);

    drop(owner);
    let mut acknowledged = intent();
    acknowledged.prepared_by_this_node = true;
    let restarted = start_node_with_intents(
        &sim,
        OWNER,
        &source,
        Arc::clone(&store),
        vec![acknowledged.clone()],
    );
    assert!(
        !write_one(&sim, &restarted, 999),
        "startup restores the active split gate before the first write can arrive"
    );
    let host = sim.block_on({
        let restarted = Arc::clone(&restarted);
        async move { restarted.host(PARENT).await.unwrap() }
    });
    assert!(!host.is_admitting_writes());
    assert!(!host.is_admitting_leases());
    assert!(host.is_maintenance_frozen());

    for _ in 0..200 {
        flush_owned(&sim, &restarted);
    }
    source.set(children_map());
    poll(&sim, &[&restarted, &replica]);
    for i in 0..4 {
        assert_eq!(read(&sim, &restarted, key_at(i)), Some(value_at(i)));
    }
    assert!(read(&sim, &restarted, key_at(999)).is_none());
}

#[test]
fn startup_keeps_owners_closed_until_the_map_matches_the_intent_snapshot() {
    let sim = Simulation::new(43);
    let source = StaticMapSource::new(parent_map());
    let owner = start_node_with_snapshot(
        &sim,
        OWNER,
        &source,
        Arc::new(MemoryStore::new()),
        Some(SplitIntentSnapshot {
            map_version: MapVersion(2),
            intents: Vec::new(),
        }),
    );

    assert!(!write_one(&sim, &owner, 1));
    assert!(!sim.block_on({
        let owner = Arc::clone(&owner);
        async move { owner.host(PARENT).await.unwrap().is_admitting_writes() }
    }));
}

#[test]
fn split_preparation_revokes_parent_read_leases_before_acknowledging() {
    let sim = Simulation::new(23);
    let store = Arc::new(MemoryStore::new());
    let source = StaticMapSource::new(parent_map());
    let owner = start_node(&sim, OWNER, &source, Arc::clone(&store));
    let replica = start_node(&sim, REPLICA, &source, Arc::clone(&store));
    poll(&sim, &[&owner, &replica]);
    assert!(write_one(&sim, &owner, 0));
    sim.block_on({
        let owner = Arc::clone(&owner);
        async move { owner.renew_leases().await }
    });
    let replica_host = sim.block_on({
        let replica = Arc::clone(&replica);
        async move { replica.host(PARENT).await.unwrap() }
    });
    assert!(
        replica_host.might_serve(&key_at(0)),
        "the replica holds a parent lease"
    );

    let prepared = sim.block_on({
        let owner = Arc::clone(&owner);
        async move { owner.prepare_pending_splits(&[intent()]).await }
    });
    assert_eq!(prepared, vec![generation()]);
    assert!(
        !replica_host.might_serve(&key_at(0)),
        "the durable prepared ack follows explicit parent-lease revocation"
    );
    sim.block_on({
        let owner = Arc::clone(&owner);
        async move { owner.renew_leases().await }
    });
    assert!(
        !replica_host.might_serve(&key_at(0)),
        "heartbeat cannot reissue a lease while the split remains active"
    );

    sim.block_on({
        let owner = Arc::clone(&owner);
        async move { owner.prepare_pending_splits(&[]).await }
    });
    let owner_host = sim.block_on({
        let owner = Arc::clone(&owner);
        async move { owner.host(PARENT).await.unwrap() }
    });
    assert!(
        owner_host.is_admitting_leases(),
        "abort reopens lease issuance"
    );
    sim.block_on({
        let owner = Arc::clone(&owner);
        async move { owner.renew_leases().await }
    });
    assert!(
        replica_host.might_serve(&key_at(0)),
        "the live parent may grant read leases again after abort"
    );
    assert!(
        write_one(&sim, &owner, 999),
        "abort replaces the irreversibly quiesced WAL before reopening writes"
    );
}

#[test]
fn an_aborted_split_reopens_the_parents_maintenance() {
    // The freeze must lift when a split is abandoned — a required holder that
    // never acks — so the parent resumes flushing, compacting, and sweeping.
    let sim = Simulation::new(1);
    let store = Arc::new(MemoryStore::new());
    let source = StaticMapSource::new(parent_map());
    let owner = start_node(&sim, OWNER, &source, Arc::clone(&store));
    let replica = start_node(&sim, REPLICA, &source, Arc::clone(&store));
    poll(&sim, &[&owner, &replica]);
    for i in 0..2u64 {
        write_one(&sim, &owner, i);
    }
    flush_owned(&sim, &owner);

    // Open the split, then abandon it: the next intent fetch returns nothing.
    let owner2 = Arc::clone(&owner);
    sim.block_on(async move { owner2.prepare_pending_splits(&[intent()]).await });
    assert!(sim.block_on({
        let owner = Arc::clone(&owner);
        async move { owner.host(PARENT).await.unwrap().is_maintenance_frozen() }
    }));

    let owner3 = Arc::clone(&owner);
    sim.block_on(async move { owner3.prepare_pending_splits(&[]).await });
    assert!(
        !sim.block_on({
            let owner = Arc::clone(&owner);
            async move { owner.host(PARENT).await.unwrap().is_maintenance_frozen() }
        }),
        "an abandoned split reopens the parent's maintenance"
    );
    assert!(sim.block_on({
        let owner = Arc::clone(&owner);
        async move { owner.host(PARENT).await.unwrap().is_admitting_leases() }
    }));
}

#[test]
fn an_empty_intent_snapshot_cannot_abort_a_split_against_an_older_map() {
    let sim = Simulation::new(41);
    let store = Arc::new(MemoryStore::new());
    let source = StaticMapSource::new(parent_map());
    let owner = start_node(&sim, OWNER, &source, Arc::clone(&store));
    let _replica = start_node(&sim, REPLICA, &source, store);
    assert_eq!(
        sim.block_on({
            let owner = Arc::clone(&owner);
            async move { owner.prepare_pending_splits(&[intent()]).await }
        }),
        vec![generation()]
    );

    let newer_empty = SplitIntentSnapshot {
        map_version: MapVersion(2),
        intents: Vec::new(),
    };
    assert!(sim
        .block_on({
            let owner = Arc::clone(&owner);
            async move { owner.prepare_split_snapshot(&newer_empty).await }
        })
        .is_empty());
    assert!(
        !sim.block_on({
            let owner = Arc::clone(&owner);
            async move { owner.host(PARENT).await.unwrap().is_admitting_writes() }
        }),
        "completion or abort observed after a stale map must leave the parent gated"
    );

    let matching_empty = SplitIntentSnapshot {
        map_version: MapVersion(1),
        intents: Vec::new(),
    };
    sim.block_on({
        let owner = Arc::clone(&owner);
        async move { owner.prepare_split_snapshot(&matching_empty).await }
    });
    assert!(write_one(&sim, &owner, 999));
}

/// The key whose write is stranded on the replica: the append lands, the reply
/// is lost, and the owner reports the write failed.
const GHOST: u64 = 500;
/// The key written after the abort, which takes the Lamport the ghost had.
const REPLACEMENT: u64 = 501;
/// The write after that, whose committed watermark is what releases the
/// reissued entry into a replica's storage.
const FOLLOW_ON: u64 = 502;

/// Writes `GHOST` with the replica's replies discarded, so it ends up durable on
/// the replica and reported as failed to the client.
///
/// This is the state the whole abort hazard rests on, and it needs an
/// asymmetric fault to produce: a symmetric partition leaves the replica
/// holding nothing, and then there is no tail for a reopened owner to collide
/// with. Returns whether the fault produced the state it was after.
fn strand_a_tail_on_the_replica(sim: &Simulation, owner: &Arc<Node<SimRuntime>>) -> bool {
    sim.partition_one_way(REPLICA, OWNER);
    let refused = !write_one(sim, owner, GHOST);
    sim.heal_one_way(REPLICA, OWNER);
    sim.run_until_idle();
    refused
}

/// Every acknowledged write `node` can be asked for, by index.
fn readable(sim: &Simulation, node: &Arc<Node<SimRuntime>>, indexes: &[u64]) -> Vec<u64> {
    indexes
        .iter()
        .copied()
        .filter(|i| read(sim, node, key_at(*i)) == Some(value_at(*i)))
        .collect()
}

#[test]
fn an_aborted_split_does_not_reissue_a_version_its_replica_still_holds() {
    // Reproduce a failure with:
    //   ORBITA_SIM_SEED=<seed> cargo test -p orbita-server \
    //     an_aborted_split_does_not_reissue_a_version_its_replica_still_holds
    harness::check_seeds(
        "split::an_aborted_split_does_not_reissue_a_version_its_replica_still_holds",
        24,
        aborted_split_run,
    );
}

/// The abort path's write-loss scenario, driven through the real reconcile.
///
/// Quiescing for a split hands back every Lamport above the committed prefix,
/// and one of them can be sitting on the replica because its acknowledgement
/// was lost rather than because the write failed. The reopened parent resumes
/// from the prefix and issues that Lamport again with different bytes under it.
/// A replica skips an entry at or below its durable position without comparing
/// bytes, so unless something truncates it first it acknowledges the
/// replacement and keeps the original — and the divergence only becomes visible
/// once that replica is promoted, which is what this runs to the end.
fn aborted_split_run(seed: u64) -> Result<(), Failure> {
    let sim = Simulation::new(seed);
    let sim = &sim;
    let store = Arc::new(MemoryStore::new());
    let source = StaticMapSource::new(parent_map());
    let owner = start_node(sim, OWNER, &source, Arc::clone(&store));
    let replica = start_node(sim, REPLICA, &source, Arc::clone(&store));
    poll(sim, &[&owner, &replica]);

    let survivors: Vec<u64> = (0..4).collect();
    for i in &survivors {
        if !write_one(sim, &owner, *i) {
            return Err(sim.failure(format!("the healthy write {i} was refused")));
        }
    }
    if !strand_a_tail_on_the_replica(sim, &owner) {
        return Err(sim.failure(
            "the owner acknowledged a write whose reply never came back, so this seed never \
             stranded the tail the scenario is about",
        ));
    }

    // The worker half of the split: quiesce and prepare. The quiesce is what
    // hands the stranded Lamport back.
    let prepared = {
        let owner = Arc::clone(&owner);
        sim.block_on(async move { owner.prepare_pending_splits(&[intent()]).await })
    };
    if prepared != vec![generation()] {
        return Err(sim.failure(format!(
            "the owner did not report the split prepared: {prepared:?}"
        )));
    }

    // The leader abandons the split. `AbortSplit` raises the parent's epoch in
    // the same committed entry, which is what lets the reopening owner truncate
    // its replica before it admits a write.
    source.set(parent_map_at(MapVersion(2), Epoch(2), OWNER, vec![REPLICA]));
    // The replica reconciles onto the new epoch first, so it is listening when
    // the owner reopens. The order is the point of the next assertion, not a
    // convenience: it is what makes "the fence landed before any write was
    // admitted" a fact this test can observe rather than a race it might miss.
    poll(sim, &[&replica]);
    poll(sim, &[&owner]);
    let intents = SplitIntentSnapshot {
        map_version: MapVersion(2),
        intents: Vec::new(),
    };
    {
        let owner = Arc::clone(&owner);
        sim.block_on(async move { owner.prepare_split_snapshot(&intents).await });
    }

    // Before a single write is admitted, the replica's log already ends at the
    // committed prefix. Waiting for the first append to carry the news would
    // leave a window in which the replica holds an entry under a version the
    // owner is about to mean something else by, and a promotion inside that
    // window serves it.
    let prefix = Lamport(survivors.len() as u64);
    let replica_holds = sim.block_on({
        let replica = Arc::clone(&replica);
        async move { replica.host(PARENT).await.unwrap().durable_lamport().await }
    });
    if replica_holds != prefix {
        return Err(sim.failure(format!(
            "the reopened parent admits writes with its replica still holding {replica_holds} \
             rather than the committed prefix {prefix}: the surrendered tail was not fenced \
             before write admission"
        )));
    }

    // The reissue: this write takes exactly the Lamport the ghost had.
    if !write_one(sim, &owner, REPLACEMENT) {
        return Err(sim.failure(
            "the reopened parent refused the write that reissues the surrendered Lamport",
        ));
    }
    sim.run_until_idle();

    // Fail the owner over to the replica. Whatever the replica holds is now
    // history, which is the only way a divergent tail becomes a wrong answer to
    // a client rather than a curiosity on a disk.
    source.set(parent_map_at(MapVersion(3), Epoch(3), REPLICA, vec![OWNER]));
    poll(sim, &[&owner, &replica]);
    sim.run_until_idle();

    let found = readable(sim, &replica, &[REPLACEMENT]);
    if found != vec![REPLACEMENT] {
        return Err(sim.failure(
            "the promoted replica cannot read the write that reissued a surrendered Lamport: it \
             skipped the reissue as a retransmission of the entry it was already holding",
        ));
    }
    if !readable(sim, &replica, &[GHOST]).is_empty() {
        return Err(sim.failure(
            "the promoted replica served the surrendered entry, whose client was told the write \
             failed",
        ));
    }
    let kept = readable(sim, &replica, &survivors);
    if kept != survivors {
        return Err(sim.failure(format!(
            "the fence cut committed history as well as the surrendered tail: {kept:?} of \
             {survivors:?} survived"
        )));
    }
    Ok(())
}

#[test]
fn a_replica_applies_the_reissued_entry_and_not_the_one_the_parent_gave_up() {
    // The same abort, with the replica behind on the map — the likelier order,
    // since the owner reopens the moment it sees the abort and the replica gets
    // there on its own poll. Its host is therefore not rebuilt, so the entry the
    // owner surrendered is still sitting in the queue this replica holds
    // between "durable here" and "the owner says it was acknowledged".
    //
    // The log truncation alone does not reach that queue. Both copies of the
    // Lamport would end up in it, applies run in order, and storage ignores a
    // mutation at or below its committed Lamport — so the surrendered entry
    // would apply and the reissue would be silently dropped, leaving this
    // replica holding the value the cluster threw away and missing the one it
    // kept. Only visible after a promotion, which is where this ends.
    let sim = Simulation::new(11);
    let store = Arc::new(MemoryStore::new());
    let source = StaticMapSource::new(parent_map());
    let owner = start_node(&sim, OWNER, &source, Arc::clone(&store));
    let replica = start_node(&sim, REPLICA, &source, Arc::clone(&store));
    poll(&sim, &[&owner, &replica]);
    let survivors: Vec<u64> = (0..4).collect();
    for i in &survivors {
        assert!(write_one(&sim, &owner, *i));
    }
    assert!(strand_a_tail_on_the_replica(&sim, &owner));

    let prepared = {
        let owner = Arc::clone(&owner);
        sim.block_on(async move { owner.prepare_pending_splits(&[intent()]).await })
    };
    assert_eq!(prepared, vec![generation()]);

    // Only the owner learns of the abort. The replica is still serving the
    // partition at the epoch it was opened under.
    source.set(parent_map_at(MapVersion(2), Epoch(2), OWNER, vec![REPLICA]));
    poll(&sim, &[&owner]);
    assert!(
        write_one(&sim, &owner, REPLACEMENT),
        "the reopened parent reissues the surrendered Lamport"
    );
    // One more, so the replica hears a committed watermark that covers the
    // reissue and releases it into storage. Without this the queue is still
    // holding both copies when the promotion rebuilds the host, and the
    // promotion replays the log instead — which hides the bug rather than
    // fixing it.
    assert!(write_one(&sim, &owner, FOLLOW_ON));
    sim.run_until_idle();

    // Now the replica catches up and is promoted, so what it holds becomes the
    // partition's history.
    source.set(parent_map_at(MapVersion(3), Epoch(3), REPLICA, vec![OWNER]));
    poll(&sim, &[&owner, &replica]);
    sim.run_until_idle();

    assert_eq!(
        readable(&sim, &replica, &[REPLACEMENT]),
        vec![REPLACEMENT],
        "the promoted replica lost the acknowledged write that reissued the surrendered Lamport"
    );
    assert!(
        readable(&sim, &replica, &[GHOST]).is_empty(),
        "the promoted replica served the surrendered entry, whose client was told it failed"
    );
    assert_eq!(
        readable(&sim, &replica, &[FOLLOW_ON]),
        vec![FOLLOW_ON],
        "and the write behind it"
    );
    assert_eq!(
        readable(&sim, &replica, &survivors),
        survivors,
        "and committed history survived the cut"
    );
}

#[test]
fn a_parent_that_surrendered_lamports_stays_closed_until_its_abort_raises_the_epoch() {
    // The fail-closed half. The control plane bumps the epoch on every path
    // that drops a pending split, but a holder cannot verify that from here, so
    // it checks the one thing it can: it gave Lamports back, and reopening at
    // the epoch it gave them back under would issue them again with nothing to
    // stop a replica treating the reissue as a retransmission. Staying closed
    // costs one partition's availability; reopening costs a version.
    let sim = Simulation::new(7);
    let store = Arc::new(MemoryStore::new());
    let source = StaticMapSource::new(parent_map());
    let owner = start_node(&sim, OWNER, &source, Arc::clone(&store));
    let replica = start_node(&sim, REPLICA, &source, Arc::clone(&store));
    poll(&sim, &[&owner, &replica]);
    assert!(write_one(&sim, &owner, 0));
    assert!(strand_a_tail_on_the_replica(&sim, &owner));

    let prepared = {
        let owner = Arc::clone(&owner);
        sim.block_on(async move { owner.prepare_pending_splits(&[intent()]).await })
    };
    assert_eq!(prepared, vec![generation()]);

    // The split goes away but the epoch does not move, which is the state no
    // committed abort produces and every stale or buggy one would.
    {
        let owner = Arc::clone(&owner);
        sim.block_on(async move { owner.prepare_pending_splits(&[]).await });
    }
    assert!(
        !sim.block_on({
            let owner = Arc::clone(&owner);
            async move { owner.host(PARENT).await.unwrap().is_admitting_writes() }
        }),
        "a parent that handed Lamports back must not reopen at the epoch it handed them back \
         under"
    );
    assert!(!write_one(&sim, &owner, REPLACEMENT));

    // With the epoch the real abort carries, it reopens and serves.
    source.set(parent_map_at(MapVersion(2), Epoch(2), OWNER, vec![REPLICA]));
    poll(&sim, &[&owner, &replica]);
    assert!(
        write_one(&sim, &owner, REPLACEMENT),
        "the abort's epoch bump is what reopens the parent"
    );
}

/// Runs one flush pass over every partition `node` owns.
fn flush_owned(sim: &Simulation, node: &Arc<Node<SimRuntime>>) {
    let node = Arc::clone(node);
    sim.block_on(async move { node.flush_owned().await });
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

fn run(seed: u64) -> Result<(), Failure> {
    run_split(seed, children_map())
}

/// The same contention scenario against a split that spread its children.
fn run_spread(seed: u64) -> Result<(), Failure> {
    run_split(seed, spread_children_map())
}

/// Writes through the parent's owner while the split runs underneath, then
/// proves every acknowledged write survived it and routes to the child that
/// owns its half.
///
/// `after` is the map the split completes into, which is the only difference
/// between the co-located and spread cases. Spreading means half the keys are
/// no longer owned by the node the client is talking to, so the same writes now
/// have to survive a proxy hop as well as the split.
#[allow(clippy::too_many_lines)]
fn run_split(seed: u64, after: PartitionMap) -> Result<(), Failure> {
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
    if prepared != vec![generation()] {
        return Err(sim.failure(format!(
            "the owner did not report the split prepared: {prepared:?}"
        )));
    }

    // Everything written from here on meets the children rather than the
    // parent, which is the half of the scenario the placement changes.
    let after_split = log.lock().expect("write log poisoned").len();

    // Retire the parent and install the children, then let both nodes reconcile
    // onto them. This is the committed CompleteSplit, whose atomicity is proven
    // in orbita-control; here it is a source swap.
    source.set(after);
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
    // Both children have to have taken an acknowledged write after the split
    // installed, or the scenario never reached the placement it exists to
    // test. Under a spread map one of these two lands on a partition this node
    // does not own, so losing it silently would turn the whole check green
    // while proving nothing about spreading.
    for (child, side) in [(LOWER, false), (UPPER, true)] {
        let landed = writes
            .iter()
            .skip(after_split)
            .filter(|w| w.acknowledged)
            .any(|w| (key_at(w.index).as_slice() >= BOUNDARY) == side);
        if !landed {
            return Err(sim.failure(format!(
                "no write was acknowledged against {child} after the split, \
                 so this seed did not exercise the post-split placement"
            )));
        }
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

#[test]
fn no_acknowledged_write_is_lost_when_a_split_spreads_its_children() {
    // A split only buys write throughput if its children land on different
    // owners, because one owner serialises each partition. That placement is
    // new, and it changes what the write path has to survive: half the keys
    // stop being owned by the node the client is talking to at the instant the
    // children install, so an in-flight write can find its partition moved to a
    // peer mid-flight. This asserts the same guarantee as the co-located case
    // — nothing acknowledged is lost, everything routes to the child that owns
    // its half — against that harder placement. See issue #160.
    //
    // Reproduce a failure with:
    //   ORBITA_SIM_SEED=<seed> cargo test -p orbita-server \
    //     no_acknowledged_write_is_lost_when_a_split_spreads_its_children
    harness::check_seeds(
        "split::no_acknowledged_write_is_lost_when_a_split_spreads_its_children",
        24,
        run_spread,
    );
}

#[test]
fn no_acknowledged_write_is_lost_across_a_merge_under_contention() {
    // Replay with:
    //   ORBITA_SIM_SEED=<seed> cargo test -p orbita-server \
    //     no_acknowledged_write_is_lost_across_a_merge_under_contention
    harness::check_seeds(
        "split::no_acknowledged_write_is_lost_across_a_merge_under_contention",
        24,
        run_merge,
    );
}

fn run_merge(seed: u64) -> Result<(), Failure> {
    let sim = Simulation::new(seed);
    let store = Arc::new(MemoryStore::new());
    let source = StaticMapSource::new(children_map());
    let owner = start_merge_node(&sim, OWNER, &source, Arc::clone(&store));
    let replica = start_merge_node(&sim, REPLICA, &source, Arc::clone(&store));
    poll(&sim, &[&owner, &replica]);

    let stop = Arc::new(AtomicBool::new(false));
    let log = Arc::new(Mutex::new(Vec::new()));
    spawn_writer(&sim, &owner, &stop, &log);
    sim.run_for(Duration::from_millis(200));

    let split_snapshot = SplitIntentSnapshot {
        map_version: MapVersion(3),
        intents: Vec::new(),
    };
    let owner_prepared = {
        let owner = Arc::clone(&owner);
        let splits = split_snapshot.clone();
        let merges = merge_snapshot(false);
        sim.block_on(async move { owner.prepare_transition_snapshots(&splits, &merges).await.1 })
    };
    if owner_prepared != vec![merge_generation()] {
        return Err(sim.failure(format!(
            "the owner did not durably prepare the merge: {owner_prepared:?}"
        )));
    }
    let replica_prepared = {
        let replica = Arc::clone(&replica);
        let splits = split_snapshot;
        let merges = merge_snapshot(false);
        sim.block_on(async move {
            replica
                .prepare_transition_snapshots(&splits, &merges)
                .await
                .1
        })
    };
    if replica_prepared != vec![merge_generation()] {
        return Err(sim.failure("the replica acknowledged before the merged snapshot was servable"));
    }

    source.set(merged_map());
    poll(&sim, &[&owner, &replica]);
    sim.run_for(Duration::from_millis(200));
    stop.store(true, Ordering::Release);
    sim.run_until_idle();

    let writes = log.lock().expect("write log poisoned");
    if !writes.iter().any(|write| write.acknowledged) {
        return Err(sim.failure("no write was acknowledged, so the merge proved nothing"));
    }
    for wrote in writes.iter().filter(|write| write.acknowledged) {
        let key = key_at(wrote.index);
        match read(&sim, &owner, key.clone()) {
            Some(value) if value == value_at(wrote.index) => {}
            other => {
                return Err(sim.failure(format!(
                    "acknowledged merge-racing key {:?} read back {other:?}",
                    String::from_utf8_lossy(&key)
                )))
            }
        }
    }
    Ok(())
}

#[test]
fn one_merge_holder_acked_and_one_pending_keeps_both_parents_closed() {
    let sim = Simulation::new(125);
    let store = Arc::new(MemoryStore::new());
    let source = StaticMapSource::new(children_map());
    let owner = start_merge_node(&sim, OWNER, &source, store);
    let splits = SplitIntentSnapshot {
        map_version: MapVersion(3),
        intents: Vec::new(),
    };
    let merges = merge_snapshot(false);
    let preparing = Arc::clone(&owner);
    let prepared = sim.block_on(async move {
        preparing
            .prepare_transition_snapshots(&splits, &merges)
            .await
            .1
    });
    assert_eq!(prepared, vec![merge_generation()]);
    for parent in [LOWER, UPPER] {
        let owner = Arc::clone(&owner);
        assert!(sim.block_on(async move {
            let host = owner.host(parent).await.unwrap();
            !host.is_admitting_writes()
                && !host.is_admitting_leases()
                && host.is_maintenance_frozen()
        }));
    }
    assert!(read(&sim, &owner, b"a-stale-route".to_vec()).is_none());
}

#[test]
fn a_stale_parent_route_refuses_list_after_the_merged_child_accepts_a_write() {
    let sim = Simulation::new(128);
    let store = Arc::new(MemoryStore::new());
    let stale_source = StaticMapSource::new(children_map());
    let stale = start_merge_node(&sim, OWNER, &stale_source, Arc::clone(&store));
    let child_source = StaticMapSource::new(children_map());
    let child = start_merge_node(&sim, REPLICA, &child_source, Arc::clone(&store));
    poll(&sim, &[&stale, &child]);

    let old = Arc::clone(&stale);
    sim.block_on(async move {
        old.set(
            SetRequest {
                keyspace: KEYSPACE.into(),
                key: b"a-before-merge".to_vec(),
                value: b"old".to_vec(),
                ttl_millis: None,
                condition: None,
            },
            false,
            None,
        )
        .await
        .expect("the parent accepts the old row")
    });
    let splits = SplitIntentSnapshot {
        map_version: MapVersion(3),
        intents: Vec::new(),
    };
    let preparing = Arc::clone(&stale);
    sim.block_on(async move {
        preparing
            .prepare_transition_snapshots(&splits, &merge_snapshot(false))
            .await
    });

    child_source.set(merged_map_owned(REPLICA, Vec::new()));
    poll(&sim, &[&child]);
    let writing = Arc::clone(&child);
    sim.block_on(async move {
        writing
            .set(
                SetRequest {
                    keyspace: KEYSPACE.into(),
                    key: b"z-after-merge".to_vec(),
                    value: b"new".to_vec(),
                    ttl_millis: None,
                    condition: None,
                },
                false,
                None,
            )
            .await
            .expect("the merged child accepts a newer row")
    });

    let listing = Arc::clone(&stale);
    let refused = sim.block_on(async move {
        listing
            .list(
                ListRequest {
                    keyspace: KEYSPACE.into(),
                    prefix: Vec::new(),
                    cursor: Vec::new(),
                    limit: 10,
                    include_values: true,
                },
                false,
                None,
            )
            .await
    });
    assert!(
        matches!(refused, Err(orbita_core::Error::Unavailable(_))),
        "the stale parent returned rows after the child moved ahead: {refused:?}"
    );
}

#[test]
fn a_same_epoch_merge_parent_restart_restores_both_gates_closed() {
    let sim = Simulation::new(126);
    let store = Arc::new(MemoryStore::new());
    let source = StaticMapSource::new(children_map());
    let splits = Some(SplitIntentSnapshot {
        map_version: MapVersion(3),
        intents: Vec::new(),
    });
    let owner = start_node_with_snapshots(
        &sim,
        OWNER,
        &source,
        store,
        splits,
        Some(merge_snapshot(false)),
    );
    for parent in [LOWER, UPPER] {
        let owner = Arc::clone(&owner);
        assert!(sim.block_on(async move {
            let host = owner.host(parent).await.unwrap();
            !host.is_admitting_writes() && !host.is_admitting_leases()
        }));
    }
}

#[test]
fn owner_failure_after_merge_preparation_resumes_from_the_durable_child() {
    let sim = Simulation::new(127);
    let store = Arc::new(MemoryStore::new());
    let source = StaticMapSource::new(children_map());
    let owner = start_merge_node(&sim, OWNER, &source, Arc::clone(&store));
    let replica = start_merge_node(&sim, REPLICA, &source, store);
    poll(&sim, &[&owner, &replica]);
    let wrote = Arc::clone(&owner);
    let write = sim.block_on(async move {
        wrote
            .set(
                SetRequest {
                    keyspace: KEYSPACE.into(),
                    key: b"a-before-owner-failure".to_vec(),
                    value: b"durable".to_vec(),
                    ttl_millis: None,
                    condition: None,
                },
                false,
                None,
            )
            .await
    });
    assert!(write.is_ok(), "the pre-failure write lands: {write:?}");
    let splits = SplitIntentSnapshot {
        map_version: MapVersion(3),
        intents: Vec::new(),
    };
    let merges = merge_snapshot(false);
    let preparing = Arc::clone(&owner);
    assert_eq!(
        sim.block_on(async move {
            preparing
                .prepare_transition_snapshots(&splits, &merges)
                .await
                .1
        }),
        vec![merge_generation()]
    );

    sim.crash(OWNER);
    drop(owner);
    let splits = SplitIntentSnapshot {
        map_version: MapVersion(3),
        intents: Vec::new(),
    };
    let merges = merge_snapshot(false);
    let preparing = Arc::clone(&replica);
    assert_eq!(
        sim.block_on(async move {
            preparing
                .prepare_transition_snapshots(&splits, &merges)
                .await
                .1
        }),
        vec![merge_generation()],
        "the survivor verifies the owner's durable child rather than wedging"
    );

    source.set(merged_map_owned(REPLICA, vec![OWNER]));
    poll(&sim, &[&replica]);
    assert_eq!(
        read(&sim, &replica, b"a-before-owner-failure".to_vec()),
        Some(b"durable".to_vec())
    );
}
