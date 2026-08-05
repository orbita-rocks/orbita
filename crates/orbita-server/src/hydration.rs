//! What hydration is allowed to conclude from a manifest, checked under the
//! deterministic simulator.
//!
//! Rebuilding a partition from object storage reads two things out of the same
//! manifest: where the partition's data ends, and which epoch published it. The
//! second is easy to drop on the floor, and dropping it is not a cosmetic
//! omission. A replica hydrates precisely when it is behind, which is precisely
//! the state a node is in after it has missed messages, which is precisely how
//! a node misses a fence. So the moment a replica most needs to know that the
//! sender has been deposed is the moment it has just read the proof and thrown
//! it away.
//!
//! The second scenario here is about the log's own bytes rather than the
//! manifest. `docs/UPGRADES.md` promises that a rollback before finalization
//! costs nothing, because nothing has written a new format. The write-ahead log
//! framing has no way to skip a record it does not understand: an older reader
//! stops at the first unknown kind and truncates everything after it. So a
//! hydration marker written into the log would take the whole tail with it on
//! rollback and reopen the node at position zero with its storage already at
//! the manifest horizon, where it would reissue Lamports the segments hold and
//! have them silently dropped on apply. That is an acknowledged write lost
//! during a supported operation, so the horizon is derived at every open rather
//! than written down, and this asserts it stays that way.
//!
//! Both run under seeds because the object store, the disk and the peer
//! transport are all simulated, and the interleaving of a download against
//! inbound replication is exactly the sort of thing a single fixed ordering
//! makes look safe.

use crate::host::{HostSpec, LeasePolicy, PartitionHost, PartitionPaths, WriteOp};
use crate::replication::ReplicaBridge;

use bytes::Bytes;
use orbita_core::{
    Epoch, Error, KeyRange, KeyspaceId, Lamport, NodeId, PartitionId, WriteCondition,
};
use orbita_format::testing::MemoryStore;
use orbita_format::PartitionPath;
use orbita_runtime::{Disk, File, OpenOptions, Runtime, ServiceId, Transport};
use orbita_sim::{harness, NetworkFaults, SimConfig, SimRuntime, Simulation};
use orbita_storage::{Mutation, Partition};
use orbita_wal::{Hydration, Wal, WalConfig, WalService};

use std::sync::Arc;

const PARTITION: PartitionId = PartitionId(1);
const OWNER: NodeId = NodeId(1);
const REPLICA: NodeId = NodeId(2);

/// The seeds a pull request runs. The nightly batch and a targeted widening
/// both come from `ORBITA_SIM_SEEDS`.
const SEEDS: u64 = 32;

/// How many times the deposed owner is allowed to try. Under injected faults a
/// single attempt can be swallowed by the network, and a scenario that read
/// that as a fence would pass for the wrong reason.
const ATTEMPTS: usize = 8;

fn path() -> PartitionPath {
    PartitionPath::new("", KeyspaceId(1), PARTITION)
}

/// Publishes a manifest as an owner at `epoch` would, holding one key per
/// Lamport so which source answered a later read is visible rather than
/// inferred.
fn publish(
    sim: &Simulation,
    runtime: SimRuntime,
    store: Arc<MemoryStore>,
    epoch: Epoch,
    lamports: &[u64],
) {
    let lamports = lamports.to_vec();
    sim.block_on(async move {
        let partition = Partition::open(runtime, store, path(), epoch, KeyRange::unbounded())
            .await
            .expect("the successor opens over the same objects");
        for lamport in lamports {
            partition
                .apply(&Mutation::put(
                    Lamport(lamport),
                    Bytes::from(format!("k{lamport}")),
                    Bytes::from_static(b"value"),
                    None,
                ))
                .await
                .expect("applying above the horizon");
        }
        partition.flush().await.expect("publishing the manifest");
    });
}

/// A replica of the partition, wired the way a `Node` wires one: a host, a
/// bridge that observes and hydrates, and a `WalService` on the transport.
fn start_replica(
    sim: &Simulation,
    runtime: SimRuntime,
    store: Arc<MemoryStore>,
    epoch: Epoch,
) -> (
    Arc<PartitionHost<SimRuntime>>,
    Arc<crate::replication::Applies>,
) {
    let paths = PartitionPaths {
        store,
        path: path(),
        wal_dir: "wal/replica".to_string(),
    };
    sim.block_on(async move {
        let (bridge, applies) = ReplicaBridge::start(&runtime);
        let host = PartitionHost::open_replica(
            runtime.clone(),
            HostSpec {
                id: PARTITION,
                epoch,
                range: KeyRange::unbounded(),
                lease: LeasePolicy::default(),
            },
            &paths,
        )
        .await
        .expect("the replica opens");

        bridge.register(&host);
        let service = WalService::new();
        service.register(host.log());
        service.observe(Arc::clone(&bridge) as Arc<dyn orbita_wal::ReplicaObserver>);
        service.hydrate_with(Arc::clone(&bridge) as Arc<dyn orbita_wal::PartitionHydrator>);
        runtime.transport().register(ServiceId::Wal, service);
        (host, applies)
    })
}

#[test]
fn a_deposed_owner_cannot_borrow_a_replicas_hydration_to_reach_quorum() {
    harness::check_seeds(
        "hydration::a_deposed_owner_cannot_borrow_a_replicas_hydration_to_reach_quorum",
        SEEDS,
        |seed| {
            // A hostile network, because this is a safety claim and it has to
            // hold while messages drop, duplicate and arrive out of order.
            // That is also what makes the deposed owner retry and what makes
            // the replica answer the same batch twice, which is where the
            // first version of this fix turned out to be wrong: a refusal that
            // lived only in one call held for exactly one batch.
            //
            // The disk is left honest. Disk faults have their own suite, and
            // here they only stop the scenario reaching the question.
            let sim = Simulation::with_config(SimConfig {
                network: NetworkFaults::chaotic(),
                ..SimConfig::new(seed)
            });
            let owner_runtime = sim.add_node(OWNER);
            let replica_runtime = sim.add_node(REPLICA);
            let store = Arc::new(MemoryStore::new());

            // The partition as it stood when everyone agreed: one owner at
            // epoch 1, a manifest it published, and a replica that has it.
            publish(
                &sim,
                owner_runtime.clone(),
                Arc::clone(&store),
                Epoch(1),
                &[1, 2],
            );
            let (replica, _applies) =
                start_replica(&sim, replica_runtime.clone(), Arc::clone(&store), Epoch(1));

            // The owner loses its replica and keeps taking writes. Nothing
            // local disproves its grant, so it keeps acknowledging on one
            // copy, and its log runs ahead of the replica's by more than a
            // batch. Its replica set is empty for that stretch, so nothing is
            // ever in flight to the replica and nothing is waiting to be
            // retransmitted: this owner is ahead in the only way that matters,
            // which is that it cannot close the distance itself.
            let deposed = sim.block_on({
                let runtime = owner_runtime.clone();
                async move {
                    Wal::open(
                        runtime,
                        WalConfig::new(PARTITION, "wal/deposed", Epoch(1)).with_hydration(
                            Hydration {
                                epoch: Epoch(1),
                                through: Lamport(2),
                            },
                        ),
                    )
                    .await
                    .expect("the deposed owner still opens; nothing local disproves its grant")
                }
            });
            for i in 0..4 {
                let deposed = Arc::clone(&deposed);
                sim.block_on(async move {
                    deposed
                        .commit(orbita_wal::WalOp::Put {
                            key: Bytes::from(format!("alone{i}")),
                            value: Bytes::from_static(b"one copy only"),
                            expires_at_millis: None,
                        })
                        .await
                        .expect("an unreplicated owner acknowledges its own write")
                });
            }

            // The failover the replica does not hear about. A new owner at
            // epoch 2 takes the partition, rebuilds it from the same objects,
            // writes, and publishes. Nothing tells this replica, which is the
            // whole point: it is behind, and being behind is how a node misses
            // a fence.
            publish(
                &sim,
                sim.add_node(NodeId(3)),
                Arc::clone(&store),
                Epoch(2),
                &[3, 4, 5, 6],
            );
            sim.run_until_idle();

            // The control plane also placed the replica back on this partition,
            // which the deposed owner hears and the replica does not. From here
            // its every batch leaves a hole in the replica's log, which is what
            // sends the replica to the bucket on the deposed owner's behalf.
            deposed.set_replicas(&[REPLICA]);

            // Retried, because a dropped message must not be allowed to look
            // like a fence. Every attempt is another chance for the replica to
            // acknowledge, which is the outcome this scenario exists to
            // forbid.
            let mut refused_by_the_manifest = false;
            for i in 0..ATTEMPTS {
                let deposed = Arc::clone(&deposed);
                let committed = sim.block_on(async move {
                    deposed
                        .commit(orbita_wal::WalOp::Put {
                            key: Bytes::from(format!("stale{i}")),
                            value: Bytes::from_static(b"written under a dead epoch"),
                            expires_at_millis: None,
                        })
                        .await
                });
                sim.run_until_idle();
                match committed {
                    Ok(lamport) => {
                        return Err(sim.failure(format!(
                            "the deposed owner reached quorum for {lamport}: the replica \
                             hydrated from an epoch-2 manifest and acknowledged an epoch-1 write \
                             anyway, which the real owner will later truncate",
                        )))
                    }
                    Err(Error::StaleEpoch {
                        current: Epoch(2), ..
                    }) => refused_by_the_manifest = true,
                    Err(Error::StaleEpoch { current, .. }) => {
                        return Err(sim.failure(format!(
                            "the write was refused at epoch {current} rather than the epoch 2 \
                             the manifest names",
                        )))
                    }
                    // The message never landed, so this attempt says nothing
                    // about the epoch. Safe, and not the answer being looked
                    // for; the next attempt gets another go.
                    Err(_) => {}
                }
            }
            if !refused_by_the_manifest {
                return Err(sim.failure(format!(
                    "none of {ATTEMPTS} attempts reached the replica, so this seed neither \
                     proves nor disproves the fence",
                )));
            }

            // The rebuild is kept even though the acknowledgement was refused.
            // Downloading a manifest is correct no matter who asked for it, and
            // throwing it away would make the next legitimate owner pay for the
            // download twice.
            let held = sim.block_on({
                let replica = Arc::clone(&replica);
                async move {
                    (
                        replica.log().durable_lamport().await,
                        replica.get(b"k6").await.expect("a local read"),
                        replica.get(b"stale0").await.expect("a local read"),
                    )
                }
            });
            drop(replica);
            drop(deposed);

            if held.0 < Lamport(6) {
                return Err(sim.failure(format!(
                    "the replica sits at {} rather than at the manifest horizon 6, so the \
                     refusal cost it the rebuild it had already paid for",
                    held.0
                )));
            }
            if held.1.is_none() {
                return Err(sim.failure(
                    "the replica refused the write and also failed to keep what it downloaded",
                ));
            }
            if held.2.is_some() {
                return Err(sim.failure(
                    "the deposed owner's write is readable on the replica despite being refused",
                ));
            }
            Ok(())
        },
    );
}

#[test]
fn the_owner_the_manifest_names_closes_the_same_gap_the_deposed_one_could_not() {
    // The refusal has to be about the epoch and not about hydration, or the
    // fix would have turned a working feature off. Same shape, same manifest,
    // an owner at the epoch the manifest names.
    harness::check_seeds(
        "hydration::the_owner_the_manifest_names_closes_the_same_gap_the_deposed_one_could_not",
        SEEDS,
        |seed| {
            let sim = Simulation::new(seed);
            let owner_runtime = sim.add_node(OWNER);
            let replica_runtime = sim.add_node(REPLICA);
            let store = Arc::new(MemoryStore::new());

            publish(
                &sim,
                owner_runtime.clone(),
                Arc::clone(&store),
                Epoch(1),
                &[1, 2],
            );
            let (replica, _applies) =
                start_replica(&sim, replica_runtime.clone(), Arc::clone(&store), Epoch(1));

            sim.partition(OWNER, REPLICA);
            publish(
                &sim,
                sim.add_node(NodeId(3)),
                Arc::clone(&store),
                Epoch(2),
                &[3, 4, 5, 6],
            );
            sim.heal(OWNER, REPLICA);
            sim.run_until_idle();

            let current = sim.block_on({
                let runtime = owner_runtime.clone();
                async move {
                    Wal::open(
                        runtime,
                        WalConfig::new(PARTITION, "wal/current", Epoch(2))
                            .with_replicas(vec![REPLICA])
                            .with_hydration(Hydration {
                                epoch: Epoch(2),
                                through: Lamport(6),
                            }),
                    )
                    .await
                    .expect("the current owner opens")
                }
            });

            let committed = sim.block_on({
                let current = Arc::clone(&current);
                async move {
                    current
                        .commit(orbita_wal::WalOp::Put {
                            key: Bytes::from_static(b"live"),
                            value: Bytes::from_static(b"written under the live epoch"),
                            expires_at_millis: None,
                        })
                        .await
                }
            });
            sim.run_until_idle();
            drop(replica);
            drop(current);

            match committed {
                Ok(lamport) if lamport > Lamport(6) => Ok(()),
                Ok(lamport) => Err(sim.failure(format!(
                    "the owner reused {lamport}, which the published segments already hold",
                ))),
                Err(error) => Err(sim.failure(format!(
                    "the live owner was refused as {error}, so hydration stopped closing gaps",
                ))),
            }
        },
    );
}

#[test]
fn a_hydrated_node_leaves_a_log_the_previous_binary_still_recovers() {
    harness::check_seeds(
        "hydration::a_hydrated_node_leaves_a_log_the_previous_binary_still_recovers",
        SEEDS,
        |seed| {
            let sim = Simulation::new(seed);
            let runtime = sim.add_node(OWNER);
            let store = Arc::new(MemoryStore::new());

            // A partition in the bucket and nothing on this node's disk: the
            // replacement worker ADR 0006 exists to make cheap, and the only
            // case where the log has to start above zero.
            publish(
                &sim,
                sim.add_node(NodeId(3)),
                Arc::clone(&store),
                Epoch(1),
                &[1, 2, 3, 4],
            );

            let paths = PartitionPaths {
                store: Arc::clone(&store) as Arc<dyn orbita_objectstore::ObjectStore>,
                path: path(),
                wal_dir: "wal/worker".to_string(),
            };
            let host = sim.block_on({
                let runtime = runtime.clone();
                async move {
                    PartitionHost::open_owner(
                        runtime,
                        HostSpec {
                            id: PARTITION,
                            epoch: Epoch(1),
                            range: KeyRange::unbounded(),
                            lease: LeasePolicy::default(),
                        },
                        &paths,
                        Vec::new(),
                    )
                    .await
                    .expect("the replacement worker opens")
                }
            });

            // Writes above the horizon. These are acknowledged, so losing them
            // to a rollback is the failure this scenario is looking for.
            let writes = 1 + sim.random_below(4);
            let mut acknowledged = Vec::new();
            for i in 0..writes {
                let writing = Arc::clone(&host);
                let ack = sim.block_on(async move {
                    writing
                        .write(
                            Bytes::from(format!("above{i}")),
                            WriteOp::Put {
                                value: Bytes::from_static(b"value"),
                                ttl_millis: None,
                            },
                            WriteCondition::None,
                        )
                        .await
                        .expect("a solo owner acknowledges its own write")
                });
                if let Some(version) = ack.version {
                    acknowledged.push(Lamport(version.get()));
                }
            }
            // A flush and its checkpoint, because a checkpoint is the record
            // hydration most obviously interacts with: it is the one that
            // names a position the file may not physically hold.
            let flushing = Arc::clone(&host);
            sim.block_on(async move { flushing.flush().await.expect("the flush succeeds") });
            sim.run_until_idle();
            drop(host);

            // Now roll the binary back. The previous reader knows record kinds
            // one through three and stops dead at anything else, so this is
            // what its recovery would make of the bytes this node left.
            let names = sim.block_on({
                let runtime = runtime.clone();
                async move { runtime.disk().list("wal/worker").await.expect("listing") }
            });
            let mut recovered = Lamport::ZERO;
            for name in &names {
                // `Disk::list` is implemented twice and the two do not agree on
                // whether they return bare names or full paths, which
                // `orbita_wal::log` documents at length. Accept either.
                let name = if name.contains('/') {
                    name.clone()
                } else {
                    format!("wal/worker/{name}")
                };
                let bytes = sim.block_on({
                    let runtime = runtime.clone();
                    let name = name.clone();
                    async move {
                        let file = runtime
                            .disk()
                            .open(&name, OpenOptions::default())
                            .await
                            .expect("opening a segment");
                        let size = file.size().await.expect("sizing a segment");
                        file.read_at(0, size as usize)
                            .await
                            .expect("reading a segment")
                    }
                });
                match previous_binary_scan(&bytes) {
                    Ok(durable) => recovered = recovered.max(durable),
                    Err(offset) => {
                        return Err(sim.failure(format!(
                            "the previous binary stops at offset {offset} of {name}, so a \
                             rollback truncates this log and reopens the node below its \
                             storage horizon",
                        )))
                    }
                }
            }

            for lamport in &acknowledged {
                if *lamport > recovered {
                    return Err(sim.failure(format!(
                        "the acknowledged write at {lamport} is not recoverable by the previous \
                         binary, which only reaches {recovered}",
                    )));
                }
            }
            Ok(())
        },
    );
}

/// The format-version-1 reader, reduced to what a rollback depends on: parse
/// the segment header, decode frames in order, and stop dead at the first
/// record kind it was never taught. Returns the durable position it recovers,
/// or the offset it gave up at.
///
/// Written out by hand rather than reusing the log's own scanner, so that
/// teaching the current binary a new record kind cannot quietly teach this one
/// the same thing and make the scenario pass by forgetting the question.
fn previous_binary_scan(bytes: &[u8]) -> Result<Lamport, usize> {
    const HEADER: usize = 16;
    const FRAME_HEADER: usize = 8;
    const KIND_ENTRY: u8 = 1;
    const KINDS_IT_KNOWS: u8 = 3;

    if bytes.len() < HEADER || &bytes[..4] != b"OWAL" {
        return Err(0);
    }
    let mut durable = Lamport::ZERO;
    let mut pos = HEADER;
    while pos + FRAME_HEADER <= bytes.len() {
        let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().expect("four bytes")) as usize;
        let body = pos + FRAME_HEADER;
        if body + len > bytes.len() {
            // A torn tail, which is a crash rather than a version problem and
            // is what both binaries do about one.
            break;
        }
        let kind = bytes[body];
        if kind > KINDS_IT_KNOWS {
            return Err(pos);
        }
        if kind == KIND_ENTRY {
            // Lamport is the first field after the kind byte.
            let at = u64::from_le_bytes(
                bytes[body + 1..body + 9]
                    .try_into()
                    .expect("eight bytes follow the kind"),
            );
            durable = durable.max(Lamport(at));
        }
        pos = body + len;
    }
    Ok(durable)
}
