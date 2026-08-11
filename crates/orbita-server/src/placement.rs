//! What happens to a partition when its replicas are placed after it opened.
//!
//! A partition is born owned and unreplicated. The control plane places its
//! replicas afterwards, and it does that with `SetReplicas`, which bumps the
//! map version and deliberately leaves the epoch where it is, because placing
//! a replica is not a change of ownership.
//!
//! That leaves a window nothing else in the system covers. The owner already
//! has the partition open with the peer list it was born with, so it has to
//! notice a change that does not look like one, and the replica holds none of
//! the history the partition already has, so somebody has to send it. Neither
//! is exercised by a test that starts a cluster already at its final shape, and
//! both fail silently: writes keep being acknowledged, on one copy, while the
//! map promises more.
//!
//! The same module covers the other half of the same question: what a
//! catch-up is allowed to carry. A catch-up that is not driving a write may
//! only move a replica up to the owner's committed prefix, because everything
//! above that is a write whose client was told `Unavailable`, and copying it
//! onto a second node is what turns a reported failure into a value a promoted
//! owner will serve.
//!
//! This runs the cases under the deterministic simulator so the ordering is
//! fixed rather than lucky.

use crate::map_source::{BoxedMapSource, StaticMapSource};
use crate::node::{DataLayout, Node};

use orbita_core::{
    Epoch, KeyRange, KeyspaceId, KeyspaceInfo, KeyspaceName, Lamport, MapVersion, NodeId,
    PartitionId, PartitionInfo, PartitionMap,
};
use orbita_format::testing::MemoryStore;
use orbita_proto::v1::{GetRequest, SetRequest};
use orbita_runtime::{Clock, Runtime};
use orbita_sim::{harness, SimRuntime, Simulation};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const KEYSPACE: &str = "default";
const PARTITION: PartitionId = PartitionId(1);

const OWNER: NodeId = NodeId(1);
const FIRST: NodeId = NodeId(2);
const SECOND: NodeId = NodeId(3);

/// How many writes land before the replica exists. More than one so that a
/// backfill has to carry a run of entries rather than a single edge case.
const WRITES: u64 = 5;

/// The map as the control plane first publishes it: owned, unreplicated.
fn unplaced() -> PartitionMap {
    map_with(MapVersion(1), OWNER, Epoch(1), Vec::new())
}

/// The same partition after the control plane placed a replica on it. Same
/// epoch, higher version: placement is not an ownership change.
fn placed() -> PartitionMap {
    map_with(MapVersion(2), OWNER, Epoch(1), vec![FIRST])
}

/// The same again with the full replica set, which is the shape that lets a
/// single unreachable peer hide behind a reachable one.
fn placed_on_both() -> PartitionMap {
    map_with(MapVersion(2), OWNER, Epoch(1), vec![FIRST, SECOND])
}

fn map_with(
    version: MapVersion,
    owner: NodeId,
    epoch: Epoch,
    replicas: Vec<NodeId>,
) -> PartitionMap {
    let keyspace = KeyspaceId(1);
    let mut map = PartitionMap::new(version);
    map.insert_keyspace(KeyspaceInfo {
        id: keyspace,
        name: KeyspaceName::new(KEYSPACE).expect("a literal name is valid"),
        default_ttl_millis: None,
        max_value_bytes: None,
        max_storage_bytes: None,
        max_reads_per_second: None,
        max_writes_per_second: None,
    });
    map.insert_partition(PartitionInfo {
        id: PARTITION,
        keyspace,
        range: KeyRange::unbounded(),
        owner: Some(owner),
        epoch,
        replicas,
    });
    map
}

fn start_node(sim: &Simulation, node: NodeId, source: &StaticMapSource) -> Arc<Node<SimRuntime>> {
    start_node_reporting(sim, node, source, &Arc::new(crate::ReadinessGate::new()))
}

/// Starts a node whose storage lives in a store the caller controls, so a
/// scenario can give every node the same bucket. Production runs one object
/// store for the whole cluster; a per-node store hides every bug that only
/// appears when a promoted node reopens over a manifest a different node
/// published.
fn start_node_on_store(
    sim: &Simulation,
    node: NodeId,
    source: &StaticMapSource,
    store: Arc<MemoryStore>,
) -> Arc<Node<SimRuntime>> {
    let runtime = sim.add_node(node);
    let layout = DataLayout {
        store,
        wal_root: "wal".to_string(),
        wal_segment_bytes: crate::DEFAULT_WAL_SEGMENT_BYTES,
    };
    let source = BoxedMapSource::new(source.clone());
    let gate = Arc::new(crate::ReadinessGate::new());
    sim.block_on(async move {
        // Authentication off: placement is about ownership and catch-up, so
        // admission lets every request through to the routing under test.
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
            None,
            None,
            Duration::from_millis(150),
            gate,
            authenticator,
        )
        .await
        .expect("the node starts")
    })
}

/// The same, with the readiness gate the node reports through handed in, so a
/// scenario can check the operator-facing answer rather than only the
/// in-process one.
fn start_node_reporting(
    sim: &Simulation,
    node: NodeId,
    source: &StaticMapSource,
    gate: &Arc<crate::ReadinessGate>,
) -> Arc<Node<SimRuntime>> {
    let runtime = sim.add_node(node);
    let layout = DataLayout {
        store: Arc::new(MemoryStore::new()),
        wal_root: "wal".to_string(),
        // The default, so nothing here rolls a segment: these scenarios are
        // about what a catch-up may carry, and the retention cliff is
        // `retention.rs`'s subject.
        wal_segment_bytes: crate::DEFAULT_WAL_SEGMENT_BYTES,
    };
    let source = BoxedMapSource::new(source.clone());
    let gate = Arc::clone(gate);
    sim.block_on(async move {
        // Authentication off: placement is about ownership and catch-up, so
        // admission lets every request through to the routing under test.
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
            None,
            None,
            Duration::from_millis(150),
            gate,
            authenticator,
        )
        .await
        .expect("the node starts")
    })
}

/// How far a node says it has durably logged for the partition under test.
/// `None` when it does not hold the partition at all, which is a different
/// failure from holding it and being empty.
async fn durable(node: Arc<Node<SimRuntime>>) -> Option<Lamport> {
    node.progress()
        .await
        .into_iter()
        .find(|progress| progress.partition == PARTITION)
        .map(|progress| progress.durable_lamport)
}

/// Writes `count` keys named `prefix0..` and reports which ones the node said
/// it had applied. A write that fails is not an error here: several of these
/// scenarios exist precisely to produce one.
fn write_keys(
    sim: &Simulation,
    node: &Arc<Node<SimRuntime>>,
    prefix: &str,
    count: u64,
) -> Vec<bool> {
    let node = Arc::clone(node);
    let prefix = prefix.to_string();
    sim.block_on(async move {
        let mut outcomes = Vec::with_capacity(count as usize);
        for i in 0..count {
            let outcome = node
                .set(
                    SetRequest {
                        keyspace: KEYSPACE.to_string(),
                        key: format!("{prefix}{i}").into_bytes(),
                        value: i.to_be_bytes().to_vec(),
                        ttl_millis: None,
                        condition: None,
                    },
                    false,
                    None,
                )
                .await;
            outcomes.push(matches!(outcome, Ok(response) if response.applied));
        }
        outcomes
    })
}

/// Whether a key is visible from this node, asking it directly rather than
/// through the routing table so the answer is about this node's own state.
fn is_visible(sim: &Simulation, node: &Arc<Node<SimRuntime>>, key: &str) -> bool {
    let node = Arc::clone(node);
    let key = key.to_string();
    sim.block_on(async move {
        node.get(
            GetRequest {
                keyspace: KEYSPACE.to_string(),
                key: key.into_bytes(),
            },
            false,
            None,
        )
        .await
        .map(|response| response.found)
        .unwrap_or(false)
    })
}

/// Polls every node's map source the way the control loop does.
///
/// Twice, because both nodes poll on the same timer in production and either
/// can win, and because the second pass is what proves a failed catch-up is
/// retried rather than dropped.
fn poll_maps(sim: &Simulation, nodes: &[&Arc<Node<SimRuntime>>], passes: usize) {
    let nodes: Vec<Arc<Node<SimRuntime>>> = nodes.iter().map(|n| Arc::clone(n)).collect();
    sim.block_on(async move {
        for _ in 0..passes {
            for node in &nodes {
                let _ = node.refresh_map().await;
            }
        }
    });
    sim.run_until_idle();
}

#[test]
fn a_replica_placed_after_the_last_write_is_caught_up_without_another_write() {
    harness::check_seeds(
        "placement::a_replica_placed_after_the_last_write_is_caught_up_without_another_write",
        20,
        |seed| {
            let sim = Simulation::new(seed);
            let source = StaticMapSource::new(unplaced());
            let owner = start_node(&sim, NodeId(1), &source);
            let replica = start_node(&sim, NodeId(2), &source);

            // Every write here is acknowledged on one copy, because at this
            // point that is honestly all the map asks for.
            let writing = Arc::clone(&owner);
            sim.block_on(async move {
                for i in 0..WRITES {
                    writing
                        .set(
                            SetRequest {
                                keyspace: KEYSPACE.to_string(),
                                key: format!("k{i}").into_bytes(),
                                value: i.to_be_bytes().to_vec(),
                                ttl_millis: None,
                                condition: None,
                            },
                            false,
                            None,
                        )
                        .await
                        .expect("the owner accepts a write");
                }
            });
            sim.run_until_idle();

            let owner_at = sim
                .block_on(durable(Arc::clone(&owner)))
                .expect("the owner holds the partition it was given");
            if owner_at == Lamport::ZERO {
                return Err(sim.failure("no write reached the owner's log, so this proves nothing"));
            }

            // The placement lands. Nothing writes again: this is the case where
            // the partition goes quiet the moment its replica appears, which is
            // also the case a draining owner is in, since it has already closed
            // write admission.
            source.set(placed());
            let refreshing = (Arc::clone(&replica), Arc::clone(&owner));
            sim.block_on(async move {
                let (replica, owner) = refreshing;
                // Both poll on the same timer in production and either can win.
                // Refreshing twice is what a second poll is, and the second one
                // is what proves a failed catch-up is retried rather than lost.
                for _ in 0..2 {
                    replica.refresh_map().await.expect("the replica reconciles");
                    owner.refresh_map().await.expect("the owner reconciles");
                }
            });
            sim.run_until_idle();

            let replica_at = sim.block_on(durable(Arc::clone(&replica)));
            let owner_after = sim.block_on(durable(Arc::clone(&owner)));
            drop(owner);
            drop(replica);

            match replica_at {
                None => Err(sim.failure(
                    "the replica never opened the partition it was placed on".to_string(),
                )),
                Some(at) if at < owner_at => Err(sim.failure(format!(
                    "the replica sits at {at} while the owner holds {owner_at}, so an \
                     acknowledged write is still on one copy",
                ))),
                Some(_) => {
                    if owner_after != Some(owner_at) {
                        return Err(sim.failure(format!(
                            "the owner moved from {owner_at} to {owner_after:?} without a write",
                        )));
                    }
                    Ok(())
                }
            }
        },
    );
}

#[test]
fn a_placed_replica_that_is_already_current_is_not_walked_backwards() {
    harness::check_seeds(
        "placement::a_placed_replica_that_is_already_current_is_not_walked_backwards",
        20,
        |seed| {
            let sim = Simulation::new(seed);
            // Born at its final shape, so the replica is fed by the ordinary
            // append path and a drain pass has nothing to do: nothing is
            // uncommitted to give up and nothing is missing to send. Doing it
            // anyway must be a no-op rather than a truncation.
            let source = StaticMapSource::new(placed());
            let owner = start_node(&sim, NodeId(1), &source);
            let replica = start_node(&sim, NodeId(2), &source);

            let writing = Arc::clone(&owner);
            sim.block_on(async move {
                for i in 0..WRITES {
                    writing
                        .set(
                            SetRequest {
                                keyspace: KEYSPACE.to_string(),
                                key: format!("k{i}").into_bytes(),
                                value: i.to_be_bytes().to_vec(),
                                ttl_millis: None,
                                condition: None,
                            },
                            false,
                            None,
                        )
                        .await
                        .expect("the owner accepts a write");
                }
            });
            sim.run_until_idle();

            let before = sim.block_on(durable(Arc::clone(&replica)));
            let owner_before = sim.block_on(durable(Arc::clone(&owner)));
            let syncing = Arc::clone(&owner);
            sim.block_on(async move { syncing.prepare_handoff().await });
            sim.run_until_idle();
            let after = sim.block_on(durable(Arc::clone(&replica)));
            let owner_after = sim.block_on(durable(Arc::clone(&owner)));
            drop(owner);
            drop(replica);

            if after < before {
                return Err(sim.failure(format!(
                    "a catch-up push moved a current replica from {before:?} back to {after:?}",
                )));
            }
            if owner_after != owner_before {
                return Err(sim.failure(format!(
                    "quiescing gave up {owner_before:?} down to {owner_after:?} on an owner \
                     whose every write was acknowledged",
                )));
            }
            Ok(())
        },
    );
}

#[test]
fn a_replica_unreachable_when_it_was_placed_is_still_caught_up_after_the_link_heals() {
    harness::check_seeds(
        "placement::a_replica_unreachable_when_it_was_placed_is_still_caught_up_after_the_link_heals",
        20,
        |seed| {
            let sim = Simulation::new(seed);
            let source = StaticMapSource::new(unplaced());
            let owner = start_node(&sim, OWNER, &source);
            let replica = start_node(&sim, FIRST, &source);

            let wrote = write_keys(&sim, &owner, "k", WRITES);
            sim.run_until_idle();
            if wrote.iter().any(|applied| !applied) {
                return Err(sim.failure("an unreplicated owner refused a write"));
            }
            let owner_at = sim
                .block_on(durable(Arc::clone(&owner)))
                .expect("the owner holds the partition it was given");

            // The placement lands while the peer is unreachable, so every
            // catch-up call fails. What matters is not that this pass fails —
            // it has to — but that the node remembers it did. The map does not
            // move again, so nothing else will ever raise the question.
            sim.partition(OWNER, FIRST);
            source.set(placed());
            // The replica polls first, so it is open and serving replication
            // before the owner tries to reach it. The only reason a catch-up
            // fails here is the partition.
            poll_maps(&sim, &[&replica, &owner], 2);

            sim.heal_all();
            // As many passes as the control loop would make in a few seconds.
            // If synchronization is tracked per replica, the first of these
            // finishes the job; if it is inferred from the placement having
            // changed, none of them ever tries again.
            poll_maps(&sim, &[&owner, &replica], 3);

            let replica_at = sim.block_on(durable(Arc::clone(&replica)));
            drop(owner);
            drop(replica);

            match replica_at {
                Some(at) if at >= owner_at => Ok(()),
                other => Err(sim.failure(format!(
                    "the replica sits at {other:?} while the owner holds {owner_at}: a catch-up \
                     that failed while the peer was unreachable was never retried, so the \
                     partition advertises a copy that does not exist",
                ))),
            }
        },
    );
}

#[test]
fn a_placement_stays_pending_while_one_of_two_new_replicas_is_still_behind() {
    harness::check_seeds(
        "placement::a_placement_stays_pending_while_one_of_two_new_replicas_is_still_behind",
        20,
        |seed| {
            let sim = Simulation::new(seed);
            let source = StaticMapSource::new(unplaced());
            let owner = start_node(&sim, OWNER, &source);
            let first = start_node(&sim, FIRST, &source);
            let second = start_node(&sim, SECOND, &source);

            let wrote = write_keys(&sim, &owner, "k", WRITES);
            sim.run_until_idle();
            if wrote.iter().any(|applied| !applied) {
                return Err(sim.failure("an unreplicated owner refused a write"));
            }
            let owner_at = sim
                .block_on(durable(Arc::clone(&owner)))
                .expect("the owner holds the partition it was given");

            // One peer reachable, one not. A pass that calls this a success
            // because somebody answered leaves the partition on two real
            // copies while the map promises three, and never looks again.
            sim.partition(OWNER, SECOND);
            source.set(placed_on_both());
            // Both replicas open the partition before the owner reaches for
            // them, so the reachable one really does answer and the pass really
            // is half done rather than wholly failed.
            poll_maps(&sim, &[&first, &second, &owner], 2);

            sim.heal_all();
            poll_maps(&sim, &[&first, &second, &owner], 3);

            let first_at = sim.block_on(durable(Arc::clone(&first)));
            let second_at = sim.block_on(durable(Arc::clone(&second)));
            drop(owner);
            drop(first);
            drop(second);

            if first_at.is_none_or(|at| at < owner_at) {
                return Err(sim.failure(format!(
                    "the reachable replica sits at {first_at:?}, below the owner's {owner_at}",
                )));
            }
            if second_at.is_none_or(|at| at < owner_at) {
                return Err(sim.failure(format!(
                    "the replica that was unreachable sits at {second_at:?} while the owner \
                     holds {owner_at}: one peer answering made the pass look complete",
                )));
            }
            Ok(())
        },
    );
}

#[test]
fn a_catch_up_never_carries_a_write_the_client_was_told_had_failed() {
    harness::check_seeds(
        "placement::a_catch_up_never_carries_a_write_the_client_was_told_had_failed",
        20,
        |seed| {
            let sim = Simulation::new(seed);
            // Born at its final shape, so the writes below are acknowledged
            // at two of three and the committed prefix is unambiguous.
            let source = StaticMapSource::new(placed_on_both());
            let owner = start_node(&sim, OWNER, &source);
            let first = start_node(&sim, FIRST, &source);
            let second = start_node(&sim, SECOND, &source);
            poll_maps(&sim, &[&owner, &first, &second], 1);

            // One replica misses the whole run. Two of three is still met, so
            // every one of these is acknowledged and the committed prefix is
            // where the owner ends up. Having a genuinely lagging replica is
            // what gives the catch-up below something real to backfill, so the
            // ceiling it has to respect is tested on a pass that is doing work
            // rather than on one that had nothing to do anyway.
            sim.partition(OWNER, SECOND);
            let wrote = write_keys(&sim, &owner, "kept", WRITES);
            sim.run_until_idle();
            if wrote.iter().any(|applied| !applied) {
                return Err(sim.failure("a partition with one live replica refused a write"));
            }
            let committed = sim
                .block_on(durable(Arc::clone(&owner)))
                .expect("the owner holds the partition");

            // Now the owner is alone. These writes reach its own disk and
            // nowhere else, and their clients are told `Unavailable`.
            sim.partition(OWNER, FIRST);
            let ghosts = write_keys(&sim, &owner, "ghost", 2);
            sim.run_until_idle();
            if ghosts.iter().any(|applied| *applied) {
                return Err(sim.failure(
                    "a write with no second copy was acknowledged, so this proves nothing",
                ));
            }

            // Connectivity returns and the catch-up the reconcile pass owes
            // runs. It must carry the lagging replica up to the committed
            // prefix and stop there, on both peers.
            sim.heal_all();
            let catching_up = Arc::clone(&owner);
            sim.block_on(async move { catching_up.catch_up_owned().await });
            sim.run_until_idle();

            let first_at = sim.block_on(durable(Arc::clone(&first)));
            let second_at = sim.block_on(durable(Arc::clone(&second)));
            drop(owner);
            drop(first);
            drop(second);

            for (node, at) in [(FIRST, first_at), (SECOND, second_at)] {
                match at {
                    Some(at) if at > committed => {
                        return Err(sim.failure(format!(
                            "replica {node} was carried to {at}, past the committed prefix \
                             {committed}: a catch-up copied a write whose client was told it \
                             had failed, which is all it takes for a promotion to serve it",
                        )));
                    }
                    Some(at) if at < committed => {
                        return Err(sim.failure(format!(
                            "replica {node} sits at {at}, below the committed prefix {committed}",
                        )));
                    }
                    None => {
                        return Err(
                            sim.failure(format!("replica {node} does not hold the partition"))
                        );
                    }
                    Some(_) => {}
                }
            }
            Ok(())
        },
    );
}

#[test]
fn a_write_that_failed_on_the_old_owner_is_not_visible_after_the_handoff() {
    harness::check_seeds(
        "placement::a_write_that_failed_on_the_old_owner_is_not_visible_after_the_handoff",
        20,
        |seed| {
            let sim = Simulation::new(seed);
            let source = StaticMapSource::new(placed_on_both());
            let owner = start_node(&sim, OWNER, &source);
            let first = start_node(&sim, FIRST, &source);
            let second = start_node(&sim, SECOND, &source);
            poll_maps(&sim, &[&owner, &first, &second], 1);

            // One replica lags the whole run, so the catch-up below has real
            // work and the ghost travels alongside it if the horizon is wrong.
            sim.partition(OWNER, SECOND);
            let wrote = write_keys(&sim, &owner, "kept", WRITES);
            sim.run_until_idle();
            if wrote.iter().any(|applied| !applied) {
                return Err(sim.failure("a partition with one live replica refused a write"));
            }

            sim.partition(OWNER, FIRST);
            let ghosts = write_keys(&sim, &owner, "ghost", 1);
            sim.run_until_idle();
            if ghosts[0] {
                return Err(sim.failure(
                    "a write with no second copy was acknowledged, so this proves nothing",
                ));
            }

            sim.heal_all();
            let catching_up = Arc::clone(&owner);
            sim.block_on(async move { catching_up.catch_up_owned().await });
            sim.run_until_idle();

            // The handoff: a replica takes the partition at a higher epoch,
            // which is what promotion looks like from a worker's side. Whatever
            // that node holds in its log becomes history, so anything a
            // catch-up put there is now a value clients can read.
            source.set(map_with(
                MapVersion(3),
                FIRST,
                Epoch(2),
                vec![OWNER, SECOND],
            ));
            poll_maps(&sim, &[&first, &second, &owner], 2);

            let ghost_visible = is_visible(&sim, &first, "ghost0");
            let kept_visible: Vec<bool> = (0..WRITES)
                .map(|i| is_visible(&sim, &first, &format!("kept{i}")))
                .collect();
            drop(owner);
            drop(first);
            drop(second);

            if ghost_visible {
                return Err(sim.failure(
                    "the new owner serves a write the old owner told its client had failed",
                ));
            }
            if kept_visible.iter().any(|found| !found) {
                return Err(sim.failure(format!(
                    "an acknowledged write did not survive the handoff: {kept_visible:?}",
                )));
            }
            Ok(())
        },
    );
}

#[test]
fn a_write_acknowledged_while_a_handoff_is_in_flight_survives_it() {
    harness::check_seeds(
        "placement::a_write_acknowledged_while_a_handoff_is_in_flight_survives_it",
        400,
        |seed| {
            let sim = Simulation::new(seed);
            let source = StaticMapSource::new(placed_on_both());
            // One bucket, as production runs it, so the promoted node reopens
            // over the manifest the old owner published.
            let store = Arc::new(MemoryStore::new());
            let owner = start_node_on_store(&sim, OWNER, &source, Arc::clone(&store));
            let first = start_node_on_store(&sim, FIRST, &source, Arc::clone(&store));
            let second = start_node_on_store(&sim, SECOND, &source, Arc::clone(&store));
            poll_maps(&sim, &[&owner, &first, &second], 1);

            // FIRST is cut off for the first stretch and a flusher runs, which
            // is the load the issue calls for: it keeps batches in flight to a
            // reachable replica long enough that the drain below can start
            // truncating the log while one of their acknowledgements is still
            // on the wire. That race is the bug.
            sim.partition(OWNER, FIRST);

            // A writer and a flusher run against the owner concurrently with
            // the drain, rather than being quiesced before it. The hazard is a
            // write that is acknowledged to its client by a batch whose reply
            // lands while the drain is giving up the uncommitted tail: without
            // the fix the drain drops it from the log and the acknowledgement
            // is handed out anyway, so the new owner never sees it.
            let acked: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
            let stop = Arc::new(AtomicBool::new(false));

            let writer_runtime = sim.runtime(OWNER);
            let writing = Arc::clone(&owner);
            let acked_writer = Arc::clone(&acked);
            let stop_writer = Arc::clone(&stop);
            sim.spawn(async move {
                let mut i = 0u64;
                while !stop_writer.load(Ordering::Relaxed) {
                    let key = format!("drain-{i}");
                    let outcome = writing
                        .set(
                            SetRequest {
                                keyspace: KEYSPACE.to_string(),
                                key: key.clone().into_bytes(),
                                value: i.to_be_bytes().to_vec(),
                                ttl_millis: None,
                                condition: None,
                            },
                            false,
                            None,
                        )
                        .await;
                    if matches!(outcome, Ok(response) if response.applied) {
                        acked_writer.lock().unwrap().push(key);
                    }
                    i += 1;
                    writer_runtime
                        .clock()
                        .sleep(Duration::from_micros(200))
                        .await;
                }
            });

            let flusher_runtime = sim.runtime(OWNER);
            let flushing = Arc::clone(&owner);
            let stop_flusher = Arc::clone(&stop);
            sim.spawn(async move {
                while !stop_flusher.load(Ordering::Relaxed) {
                    flushing.flush_owned().await;
                    flusher_runtime
                        .clock()
                        .sleep(Duration::from_micros(500))
                        .await;
                }
            });

            // Let the write/flush pipeline get going, with the map still at its
            // starting shape.
            sim.run_for(Duration::from_millis(5));

            // The link heals, so the owner can carry FIRST up. With the log
            // checkpointed by the flusher, that catch-up closes the gap out of
            // the bucket rather than out of any log.
            sim.heal_all();

            // Flag the writer to stop and quiesce the owner directly, rather
            // than through `Server::drain`. This is deliberate, and it is the
            // seam this regression has to use: `Server::drain` holds
            // `Node::begin_draining` across `prepare_handoff` (see
            // `crate::Server::drain`), and `begin_draining` closes admission and
            // then awaits `writes.write()`, which every `set` holds a read
            // permit against for the whole of `host.write().await` — commit
            // included. So the guard does not return until every admitted write
            // has resolved, and the quiesce it fronts meets an empty in-flight
            // set. The #77 loss is precisely a batch whose acknowledgement lands
            // *during* the truncate inside quiesce, so routing this write
            // through the real guard would drain the very batch the race needs
            // and the bug would vanish (confirmed: through `begin_draining` this
            // seed passes on the pre-fix WAL). Calling `prepare_handoff` while a
            // write is still in flight is the only faithful way to open that
            // window deterministically. The companion
            // `a_planned_drain_closes_write_admission_before_it_quiesces`
            // traverses the guard end to end to prove the composed drain path
            // never exposes an acknowledged write to the truncate.
            //
            // After the quiesce, confirm FIRST is caught up to the owner exactly
            // as the control plane requires before it will hand a partition
            // over. Only then is a lost write a reopen bug rather than the
            // promotion of a replica that never had it.
            stop.store(true, Ordering::Relaxed);
            let syncing = Arc::clone(&owner);
            sim.block_on(async move { syncing.prepare_handoff().await });
            sim.run_until_idle();
            let owner_at = sim.block_on(durable(Arc::clone(&owner)));
            let first_at = sim.block_on(durable(Arc::clone(&first)));
            if first_at < owner_at {
                return Err(sim.failure(format!(
                    "FIRST is at {first_at:?} while the owner holds {owner_at:?}, so this seed \
                     promotes a replica the control plane would have refused",
                )));
            }

            // The handoff: a caught-up replica takes the partition at a higher
            // epoch and reopens as owner, then republishes its manifest.
            source.set(map_with(
                MapVersion(3),
                FIRST,
                Epoch(2),
                vec![OWNER, SECOND],
            ));
            poll_maps(&sim, &[&first, &second, &owner], 2);
            let republishing = Arc::clone(&first);
            sim.block_on(async move { republishing.flush_owned().await });
            sim.run_until_idle();

            // Every write the client was told succeeded has to be readable from
            // the new owner. A write the drain dropped must never have been
            // acknowledged, so it is never in this list; the bug was a write
            // that was both dropped and acknowledged.
            let acked = std::mem::take(&mut *acked.lock().unwrap());
            let missing: Vec<String> = acked
                .iter()
                .filter(|key| !is_visible(&sim, &first, key))
                .cloned()
                .collect();
            drop(owner);
            drop(first);
            drop(second);

            if !missing.is_empty() {
                return Err(sim.failure(format!(
                    "{} acknowledged write(s) went missing across an in-flight handoff, \
                     e.g. {:?}",
                    missing.len(),
                    &missing[..missing.len().min(5)],
                )));
            }
            Ok(())
        },
    );
}

#[test]
fn a_planned_drain_closes_write_admission_before_it_quiesces() {
    // The companion to `a_write_acknowledged_while_a_handoff_is_in_flight_...`.
    // That one isolates the WAL window the fix closes by quiescing directly
    // while a write is in flight. This one traverses the real drain the way
    // `Server::drain` does — `Node::begin_draining` held across
    // `prepare_handoff` — to prove the composed production path never hands an
    // acknowledged write to the truncate in the first place.
    //
    // It passes with and without the WAL fix, and that is the point rather than
    // a gap: `begin_draining` closes admission and awaits `writes.write()`,
    // which every `set` holds a read permit against through commit, so by the
    // time the quiesce runs there is no in-flight batch whose acknowledgement
    // could race it. The property under guard is the drain *ordering* — that
    // admission is closed and drained before the log is cut — so a future
    // refactor that quiesced before draining admission would reopen the #77
    // window and, on a WAL that had also regressed, drop the write this asserts
    // survives.
    harness::check_seeds(
        "placement::a_planned_drain_closes_write_admission_before_it_quiesces",
        400,
        |seed| {
            let sim = Simulation::new(seed);
            let source = StaticMapSource::new(placed_on_both());
            // One bucket, as production runs it, so the promoted node reopens
            // over the manifest the old owner published.
            let store = Arc::new(MemoryStore::new());
            let owner = start_node_on_store(&sim, OWNER, &source, Arc::clone(&store));
            let first = start_node_on_store(&sim, FIRST, &source, Arc::clone(&store));
            let second = start_node_on_store(&sim, SECOND, &source, Arc::clone(&store));
            poll_maps(&sim, &[&owner, &first, &second], 1);

            // Same load the in-flight scenario uses: FIRST is cut off while a
            // writer and a flusher keep batches moving against the owner, so
            // there is genuinely a write in flight when the drain begins. The
            // difference is only how the drain is entered below.
            sim.partition(OWNER, FIRST);

            let acked: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
            let stop = Arc::new(AtomicBool::new(false));

            let writer_runtime = sim.runtime(OWNER);
            let writing = Arc::clone(&owner);
            let acked_writer = Arc::clone(&acked);
            let stop_writer = Arc::clone(&stop);
            sim.spawn(async move {
                let mut i = 0u64;
                while !stop_writer.load(Ordering::Relaxed) {
                    let key = format!("drain-{i}");
                    let outcome = writing
                        .set(
                            SetRequest {
                                keyspace: KEYSPACE.to_string(),
                                key: key.clone().into_bytes(),
                                value: i.to_be_bytes().to_vec(),
                                ttl_millis: None,
                                condition: None,
                            },
                            false,
                            None,
                        )
                        .await;
                    if matches!(outcome, Ok(response) if response.applied) {
                        acked_writer.lock().unwrap().push(key);
                    }
                    i += 1;
                    writer_runtime
                        .clock()
                        .sleep(Duration::from_micros(200))
                        .await;
                }
            });

            let flusher_runtime = sim.runtime(OWNER);
            let flushing = Arc::clone(&owner);
            let stop_flusher = Arc::clone(&stop);
            sim.spawn(async move {
                while !stop_flusher.load(Ordering::Relaxed) {
                    flushing.flush_owned().await;
                    flusher_runtime
                        .clock()
                        .sleep(Duration::from_micros(500))
                        .await;
                }
            });

            sim.run_for(Duration::from_millis(5));
            sim.heal_all();

            // The real drain entry, in the order `Server::drain` uses it:
            // `begin_draining` closes write admission and does not return until
            // every admitted write has resolved, and the guard it returns is
            // held across `prepare_handoff` so no write can be admitted while
            // the log is being cut. The writer keeps running into this call on
            // purpose, so the guard has an in-flight write to drain rather than
            // a quiet log; it is stopped only once the guard owns the node.
            let draining = Arc::clone(&owner);
            sim.block_on(async move {
                let _writes = draining.begin_draining().await;
                draining.prepare_handoff().await;
            });
            stop.store(true, Ordering::Relaxed);
            sim.run_until_idle();

            let owner_at = sim.block_on(durable(Arc::clone(&owner)));
            let first_at = sim.block_on(durable(Arc::clone(&first)));
            if first_at < owner_at {
                return Err(sim.failure(format!(
                    "FIRST is at {first_at:?} while the owner holds {owner_at:?}, so this seed \
                     promotes a replica the control plane would have refused",
                )));
            }

            // The handoff: a caught-up replica takes the partition at a higher
            // epoch, reopens as owner, and republishes its manifest.
            source.set(map_with(
                MapVersion(3),
                FIRST,
                Epoch(2),
                vec![OWNER, SECOND],
            ));
            poll_maps(&sim, &[&first, &second, &owner], 2);
            let republishing = Arc::clone(&first);
            sim.block_on(async move { republishing.flush_owned().await });
            sim.run_until_idle();

            let acked = std::mem::take(&mut *acked.lock().unwrap());
            let missing: Vec<String> = acked
                .iter()
                .filter(|key| !is_visible(&sim, &first, key))
                .cloned()
                .collect();
            drop(owner);
            drop(first);
            drop(second);

            if !missing.is_empty() {
                return Err(sim.failure(format!(
                    "{} acknowledged write(s) went missing across a drain that closed write \
                     admission first, e.g. {:?}",
                    missing.len(),
                    &missing[..missing.len().min(5)],
                )));
            }
            Ok(())
        },
    );
}

#[test]
fn a_quiesced_owner_advertises_only_what_a_replica_can_be_carried_to() {
    harness::check_seeds(
        "placement::a_quiesced_owner_advertises_only_what_a_replica_can_be_carried_to",
        20,
        |seed| {
            let sim = Simulation::new(seed);
            let source = StaticMapSource::new(placed_on_both());
            let owner = start_node(&sim, OWNER, &source);
            let first = start_node(&sim, FIRST, &source);
            let second = start_node(&sim, SECOND, &source);
            poll_maps(&sim, &[&owner, &first, &second], 1);

            sim.partition(OWNER, SECOND);
            let wrote = write_keys(&sim, &owner, "kept", WRITES);
            sim.run_until_idle();
            if wrote.iter().any(|applied| !applied) {
                return Err(sim.failure("a partition with one live replica refused a write"));
            }

            sim.partition(OWNER, FIRST);
            let ghosts = write_keys(&sim, &owner, "ghost", 2);
            sim.run_until_idle();
            if ghosts.iter().any(|applied| *applied) {
                return Err(sim.failure(
                    "a write with no second copy was acknowledged, so this proves nothing",
                ));
            }
            sim.heal_all();

            // What a drain does on every pass, and the guard on the fix rather
            // than a reproduction of the bug. The control plane hands a
            // partition to a replica that has reached the owner's advertised
            // position and to no other, so bounding the catch-up at the
            // committed prefix without also giving the tail up would leave the
            // owner advertising a number no replica is allowed to reach, and
            // the drain would burn its whole budget instead of losing a write.
            // Both halves have to land together.
            let draining = Arc::clone(&owner);
            sim.block_on(async move { draining.prepare_handoff().await });
            sim.run_until_idle();

            let owner_at = sim.block_on(durable(Arc::clone(&owner)));
            let first_at = sim.block_on(durable(Arc::clone(&first)));
            let second_at = sim.block_on(durable(Arc::clone(&second)));
            drop(owner);
            drop(first);
            drop(second);

            if first_at != owner_at || second_at != owner_at {
                return Err(sim.failure(format!(
                    "a quiesced owner advertises {owner_at:?} while its replicas hold \
                     {first_at:?} and {second_at:?}, so the control plane can never find a \
                     caught-up handoff target",
                )));
            }
            Ok(())
        },
    );
}

#[test]
fn a_drain_does_not_report_a_durability_problem_it_created_itself() {
    harness::check_seeds(
        "placement::a_drain_does_not_report_a_durability_problem_it_created_itself",
        20,
        |seed| {
            let sim = Simulation::new(seed);
            let source = StaticMapSource::new(placed_on_both());
            let gate = Arc::new(crate::ReadinessGate::new());
            let owner = start_node_reporting(&sim, OWNER, &source, &gate);
            let first = start_node(&sim, FIRST, &source);
            let second = start_node(&sim, SECOND, &source);
            poll_maps(&sim, &[&owner, &first, &second], 1);

            let wrote = write_keys(&sim, &owner, "kept", WRITES);
            sim.run_until_idle();
            if wrote.iter().any(|applied| !applied) {
                return Err(sim.failure("a healthy three-copy partition refused a write"));
            }

            // A tail that reached this disk and nowhere else. Giving it up is
            // what a drain does, and it is the only thing in the system that
            // makes an owner's log shorter.
            sim.partition(OWNER, FIRST);
            sim.partition(OWNER, SECOND);
            let ghosts = write_keys(&sim, &owner, "ghost", 2);
            sim.run_until_idle();
            if ghosts.iter().any(|applied| *applied) {
                return Err(sim.failure(
                    "a write with no second copy was acknowledged, so this proves nothing",
                ));
            }
            sim.heal_all();

            let healthy_before = gate
                .state()
                .is_met(crate::readiness::ReadinessCondition::ReplicasRecoverable);
            if !healthy_before {
                return Err(sim.failure(
                    "the owner reported a durability problem before draining, so this seed \
                     cannot tell the drain's effect apart from the starting state",
                ));
            }

            let draining = Arc::clone(&owner);
            sim.block_on(async move { draining.prepare_handoff().await });
            sim.run_until_idle();
            // The heartbeat is what publishes the verdict, and a real drain has
            // already stopped it. Driving it anyway is the stronger check: even
            // if it did run, the answer has to be that nothing is stranded.
            let reporting = Arc::clone(&owner);
            sim.block_on(async move { reporting.renew_leases().await });
            sim.run_until_idle();

            let healthy_after = gate
                .state()
                .is_met(crate::readiness::ReadinessCondition::ReplicasRecoverable);
            let reporting = Arc::clone(&owner);
            let stranded = sim.block_on(async move { reporting.replicas_beyond_retention().await });
            drop(owner);
            drop(first);
            drop(second);

            if !stranded.is_empty() {
                return Err(sim.failure(format!(
                    "giving up the uncommitted tail stranded a replica that was following: \
                     {stranded:?}",
                )));
            }
            if !healthy_after {
                return Err(sim.failure(
                    "a drain cleared the replicas-recoverable condition, so a planned shutdown \
                     raises a durability alarm about a replica it did not harm",
                ));
            }
            Ok(())
        },
    );
}
