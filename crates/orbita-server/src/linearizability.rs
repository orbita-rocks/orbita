//! The read and write paths, checked rather than argued about.
//!
//! ADR 0001 acknowledges a write before applying it to storage, and ADR 0003
//! evaluates conditions against an overlay of writes that are in that window.
//! Both are cheap to describe and easy to get subtly wrong, and neither has
//! natural coverage from a test that does one thing at a time. So these run a
//! partition under the deterministic simulator, record what every client saw,
//! and hand the history to the linearizability checker.
//!
//! One owner is covered: the overlay, the ordering of Lamports against the
//! order writes are applied in, and the acknowledge-before-apply window.
//!
//! The read path is covered too, with a replica actually serving reads. That
//! is the case ADR 0001 exists for and the one it warns is the kind that fails
//! silently, so it is checked here rather than unit tested and trusted: writes
//! go to the owner, reads go to a node that only replicates the partition, and
//! the checker is handed everything both clients saw.

use crate::host::{HostSpec, LeasePolicy, PartitionHost, PartitionPaths, WriteOp};
use crate::map_source::{BoxedMapSource, StaticMapSource};
use crate::node::{DataLayout, Node};

use bytes::Bytes;
use orbita_core::{
    Epoch, KeyRange, KeyspaceId, KeyspaceInfo, KeyspaceName, MapVersion, NodeId, PartitionId,
    PartitionInfo, PartitionMap, Record, Version, WriteCondition,
};
use orbita_proto::v1::{GetRequest, SetRequest};
use orbita_runtime::{Clock, Runtime};
use orbita_sim::lin::{check, Recorder, Register, RegisterOp, RegisterRet};
use orbita_sim::{harness, SimRuntime, Simulation};

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// How many states the checker may explore before giving up. Concurrency
/// rather than length is what makes the search expensive, so this is generous
/// for the handful of clients these scenarios run.
const SEARCH_BUDGET: u64 = 2_000_000;

const KEY: &[u8] = b"register";

/// A RocksDB directory unique to one simulation run.
///
/// The storage engine does its own I/O below the runtime seam, so it needs a
/// real path even under simulation. That limit is stated in the simulator's
/// own crate docs; what is simulated here is everything above it.
struct StoragePath(std::path::PathBuf);

impl StoragePath {
    fn new(label: &str, seed: u64) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "orbita-lin-{}-{label}-{seed}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::remove_dir_all(&path).ok();
        Self(path)
    }

    fn as_str(&self) -> String {
        self.0.to_string_lossy().into_owned()
    }
}

impl Drop for StoragePath {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).ok();
    }
}

fn open(
    sim: &Simulation,
    runtime: SimRuntime,
    storage: &StoragePath,
) -> Arc<PartitionHost<SimRuntime>> {
    let paths = PartitionPaths {
        storage_path: storage.as_str(),
        wal_dir: "wal/p1".to_string(),
    };
    sim.block_on(async move {
        PartitionHost::open_owner(
            runtime,
            HostSpec {
                id: PartitionId(1),
                epoch: Epoch(1),
                range: KeyRange::unbounded(),
                lease: LeasePolicy::default(),
            },
            &paths,
            Vec::new(),
        )
        .await
        .expect("the partition opens")
    })
}

fn encode(value: u64) -> Bytes {
    Bytes::copy_from_slice(&value.to_be_bytes())
}

fn decode(record: &Record) -> u64 {
    u64::from_be_bytes(
        record
            .value
            .as_ref()
            .try_into()
            .expect("an eight byte value"),
    )
}

#[test]
fn a_read_never_misses_an_acknowledged_write() {
    harness::check_seeds(
        "linearizability::a_read_never_misses_an_acknowledged_write",
        20,
        |seed| {
            let storage = StoragePath::new("register", seed);
            let sim = Simulation::new(seed);
            let runtime = sim.add_node(NodeId(1));
            let host = open(&sim, runtime.clone(), &storage);
            let recorder: Recorder<RegisterOp, RegisterRet> = Recorder::new();

            for client in 0..4u64 {
                let host = Arc::clone(&host);
                let recorder = recorder.clone();
                let clock = runtime.clock().clone();
                sim.spawn(async move {
                    for step in 0..4u64 {
                        // Every client writes a value only it produces, so a
                        // read that returns one names the write it came from.
                        let value = client * 100 + step + 1;
                        if step % 2 == 0 {
                            let call = recorder.invoke(
                                client,
                                RegisterOp::Write(value),
                                clock.monotonic_nanos(),
                            );
                            let written = host
                                .write(
                                    Bytes::from_static(KEY),
                                    WriteOp::Put {
                                        value: encode(value),
                                        ttl_millis: None,
                                    },
                                    WriteCondition::None,
                                )
                                .await;
                            match written {
                                Ok(ack) if ack.applied => {
                                    recorder.complete(call, RegisterRet::Acked);
                                }
                                // A write that failed may or may not have
                                // happened, which is exactly what the checker
                                // treats an unknown outcome as.
                                _ => recorder.abandon(call),
                            }
                        } else {
                            let call =
                                recorder.invoke(client, RegisterOp::Read, clock.monotonic_nanos());
                            match host.get(KEY).await {
                                Ok(found) => recorder
                                    .complete(call, RegisterRet::Value(found.as_ref().map(decode))),
                                Err(_) => recorder.abandon(call),
                            }
                        }
                    }
                });
            }

            sim.run_until_idle();
            let history = recorder.history();
            drop(host);

            if history.len() < 16 {
                return Err(sim.failure(format!(
                    "only {} of 16 operations were recorded",
                    history.len()
                )));
            }
            check(&Register, &history, SEARCH_BUDGET)
                .map(|_| ())
                .map_err(|violation| sim.failure(violation.to_string()))
        },
    );
}

#[test]
fn at_most_one_compare_and_swap_against_a_version_ever_succeeds() {
    harness::check_seeds(
        "linearizability::at_most_one_compare_and_swap_against_a_version_ever_succeeds",
        20,
        |seed| {
            let storage = StoragePath::new("cas", seed);
            let sim = Simulation::new(seed);
            let runtime = sim.add_node(NodeId(1));
            let host = open(&sim, runtime.clone(), &storage);

            let initial = {
                let host = Arc::clone(&host);
                sim.block_on(async move {
                    host.write(
                        Bytes::from_static(KEY),
                        WriteOp::Put {
                            value: encode(0),
                            ttl_millis: None,
                        },
                        WriteCondition::None,
                    )
                    .await
                    .expect("the first write commits")
                    .version
                    .expect("an applied write has a version")
                })
            };

            let winners = Arc::new(AtomicU64::new(0));
            for contender in 1..=6u64 {
                let host = Arc::clone(&host);
                let winners = Arc::clone(&winners);
                sim.spawn(async move {
                    let outcome = host
                        .write(
                            Bytes::from_static(KEY),
                            WriteOp::Put {
                                value: encode(contender),
                                ttl_millis: None,
                            },
                            WriteCondition::IfVersion(initial),
                        )
                        .await;
                    if outcome.is_ok_and(|ack| ack.applied) {
                        winners.fetch_add(1, Ordering::SeqCst);
                    }
                });
            }

            sim.run_until_idle();
            let won = winners.load(Ordering::SeqCst);

            let final_value = {
                let host = Arc::clone(&host);
                sim.block_on(async move { host.get(KEY).await.expect("the read succeeds") })
            };
            let version = final_value.as_ref().map(|r| r.version);
            drop(host);

            if won != 1 {
                return Err(sim.failure(format!(
                    "{won} of six compare-and-swaps against version {initial} succeeded"
                )));
            }
            // The one that won is the one whose value is there, so a swap that
            // was told it lost cannot have written anything.
            if version.is_some_and(|v: Version| v <= initial) {
                return Err(sim.failure(format!(
                    "the winning swap left version {version:?}, which did not move past {initial}"
                )));
            }
            Ok(())
        },
    );
}

/// One partition owned by node one and replicated by node two.
fn owner_and_replica_map() -> PartitionMap {
    let keyspace = KeyspaceId(1);
    let mut map = PartitionMap::new(MapVersion(1));
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
        id: PartitionId(1),
        keyspace,
        range: KeyRange::unbounded(),
        owner: Some(NodeId(1)),
        epoch: Epoch(1),
        replicas: vec![NodeId(2)],
    });
    map
}

const KEYSPACE: &str = "default";

/// How many times the owner renews leases before the test stops.
///
/// A fixed count rather than a loop, because a timer that never stops would
/// keep the simulated world from ever going idle and the run from ever ending.
const RENEWALS: u32 = 40;

fn start_node(
    sim: &Simulation,
    storage: &StoragePath,
    node: NodeId,
    lease: Duration,
) -> Arc<Node<SimRuntime>> {
    let runtime = sim.add_node(node);
    let layout = DataLayout {
        storage_root: storage.0.join(format!("n{}", node.get())),
        wal_root: "wal".to_string(),
    };
    std::fs::create_dir_all(&layout.storage_root).expect("a storage directory");
    let source = BoxedMapSource::new(StaticMapSource::new(owner_and_replica_map()));
    sim.block_on(async move {
        Node::start(
            runtime,
            node,
            layout,
            source,
            lease,
            Arc::new(crate::ReadinessGate::new()),
        )
        .await
        .expect("the node starts")
    })
}

#[test]
fn a_replica_serving_reads_never_answers_with_a_value_a_write_has_replaced() {
    harness::check_seeds(
        "linearizability::a_replica_serving_reads_never_answers_with_a_value_a_write_has_replaced",
        20,
        |seed| {
            let storage = StoragePath::new("replica-reads", seed);
            let sim = Simulation::new(seed);
            // Shorter than the production default so that the owner's
            // heartbeat, which is what releases the last write of a burst,
            // lands inside a test that runs for a few hundred milliseconds of
            // virtual time.
            let lease = Duration::from_millis(150);
            let owner = start_node(&sim, &storage, NodeId(1), lease);
            let replica = start_node(&sim, &storage, NodeId(2), lease);
            let recorder: Recorder<RegisterOp, RegisterRet> = Recorder::new();
            let clock = sim.add_node(NodeId(1)).clock().clone();

            // The owner's heartbeat, which is what puts the replica in the
            // read set at all. In production this is a loop the server owns.
            {
                let owner = Arc::clone(&owner);
                let clock = clock.clone();
                sim.spawn(async move {
                    for _ in 0..RENEWALS {
                        owner.renew_leases().await;
                        clock.sleep(lease / 3).await;
                    }
                });
            }

            for client in 0..3u64 {
                let owner = Arc::clone(&owner);
                let recorder = recorder.clone();
                let clock = clock.clone();
                sim.spawn(async move {
                    for step in 0..4u64 {
                        let value = client * 100 + step + 1;
                        let call = recorder.invoke(
                            client,
                            RegisterOp::Write(value),
                            clock.monotonic_nanos(),
                        );
                        let written = owner
                            .set(
                                SetRequest {
                                    keyspace: KEYSPACE.to_string(),
                                    key: KEY.to_vec(),
                                    value: value.to_be_bytes().to_vec(),
                                    ttl_millis: None,
                                    condition: None,
                                },
                                false,
                            )
                            .await;
                        match written {
                            Ok(response) if response.applied => {
                                recorder.complete(call, RegisterRet::Acked);
                            }
                            _ => recorder.abandon(call),
                        }
                        // Spaced out, because a key being written continuously
                        // is one a replica is not allowed to serve, and a test
                        // where that is always true would only ever exercise
                        // forwarding.
                        clock.sleep(Duration::from_millis(10)).await;
                    }
                });
            }

            // Readers talk only to the replica. Every answer it gives is one
            // the checker has to be able to place in a legal order, which is
            // the whole of what ADR 0001 promises.
            let served = Arc::new(AtomicU64::new(0));
            for reader in 10..12u64 {
                let replica = Arc::clone(&replica);
                let recorder = recorder.clone();
                let clock = clock.clone();
                let served = Arc::clone(&served);
                sim.spawn(async move {
                    for _ in 0..24u64 {
                        let before = replica.replica_reads();
                        let call =
                            recorder.invoke(reader, RegisterOp::Read, clock.monotonic_nanos());
                        let found = replica
                            .get(
                                GetRequest {
                                    keyspace: KEYSPACE.to_string(),
                                    key: KEY.to_vec(),
                                },
                                false,
                            )
                            .await;
                        match found {
                            Ok(response) => {
                                let value = response.found.then(|| {
                                    u64::from_be_bytes(
                                        response
                                            .value
                                            .as_slice()
                                            .try_into()
                                            .expect("an eight byte value"),
                                    )
                                });
                                recorder.complete(call, RegisterRet::Value(value));
                                if replica.replica_reads() > before {
                                    served.fetch_add(1, Ordering::SeqCst);
                                }
                            }
                            Err(_) => recorder.abandon(call),
                        }
                        // Slower than the writers on purpose. A read that
                        // lands before the owner's first heartbeat has nothing
                        // to serve under, so a reader that finished inside one
                        // heartbeat would only ever exercise forwarding.
                        clock.sleep(Duration::from_millis(5)).await;
                    }
                });
            }

            sim.run_until_idle();
            let history = recorder.history();
            let locally = served.load(Ordering::SeqCst);
            drop(owner);
            drop(replica);

            if locally == 0 {
                return Err(sim.failure(
                    "the replica forwarded every read, so this seed checked the owner twice"
                        .to_string(),
                ));
            }
            check(&Register, &history, SEARCH_BUDGET)
                .map(|_| ())
                .map_err(|violation| sim.failure(violation.to_string()))
        },
    );
}
