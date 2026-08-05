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
//! This runs the case under the deterministic simulator so the ordering is
//! fixed rather than lucky.

use crate::map_source::{BoxedMapSource, StaticMapSource};
use crate::node::{DataLayout, Node};

use orbita_core::{
    Epoch, KeyRange, KeyspaceId, KeyspaceInfo, KeyspaceName, Lamport, MapVersion, NodeId,
    PartitionId, PartitionInfo, PartitionMap,
};
use orbita_format::testing::MemoryStore;
use orbita_proto::v1::SetRequest;
use orbita_sim::{harness, SimRuntime, Simulation};

use std::sync::Arc;
use std::time::Duration;

const KEYSPACE: &str = "default";
const PARTITION: PartitionId = PartitionId(1);

/// How many writes land before the replica exists. More than one so that a
/// backfill has to carry a run of entries rather than a single edge case.
const WRITES: u64 = 5;

/// The map as the control plane first publishes it: owned, unreplicated.
fn unplaced() -> PartitionMap {
    map_with(MapVersion(1), Vec::new())
}

/// The same partition after the control plane placed a replica on it. Same
/// epoch, higher version: placement is not an ownership change.
fn placed() -> PartitionMap {
    map_with(MapVersion(2), vec![NodeId(2)])
}

fn map_with(version: MapVersion, replicas: Vec<NodeId>) -> PartitionMap {
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
        owner: Some(NodeId(1)),
        epoch: Epoch(1),
        replicas,
    });
    map
}

fn start_node(sim: &Simulation, node: NodeId, source: &StaticMapSource) -> Arc<Node<SimRuntime>> {
    let runtime = sim.add_node(node);
    let layout = DataLayout {
        store: Arc::new(MemoryStore::new()),
        wal_root: "wal".to_string(),
    };
    let source = BoxedMapSource::new(source.clone());
    sim.block_on(async move {
        Node::start(
            runtime,
            node,
            layout,
            source,
            Duration::from_millis(150),
            Arc::new(crate::ReadinessGate::new()),
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
            // append path and the catch-up push has nothing to do. Pushing
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
                        )
                        .await
                        .expect("the owner accepts a write");
                }
            });
            sim.run_until_idle();

            let before = sim.block_on(durable(Arc::clone(&replica)));
            let syncing = Arc::clone(&owner);
            sim.block_on(async move { syncing.sync_replicas().await });
            sim.run_until_idle();
            let after = sim.block_on(durable(Arc::clone(&replica)));
            drop(owner);
            drop(replica);

            if after < before {
                return Err(sim.failure(format!(
                    "a catch-up push moved a current replica from {before:?} back to {after:?}",
                )));
            }
            Ok(())
        },
    );
}
