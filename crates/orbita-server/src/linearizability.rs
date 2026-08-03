//! The read and write paths, checked rather than argued about.
//!
//! ADR 0001 acknowledges a write before applying it to storage, and ADR 0003
//! evaluates conditions against an overlay of writes that are in that window.
//! Both are cheap to describe and easy to get subtly wrong, and neither has
//! natural coverage from a test that does one thing at a time. So these run a
//! partition under the deterministic simulator, record what every client saw,
//! and hand the history to the linearizability checker.
//!
//! What is covered is one owner: the overlay, the ordering of Lamports against
//! the order writes are applied in, and the acknowledge-before-apply window.
//! Replication and failover are not, because a replica cannot yet receive an
//! invalidation. See the lease module.

use crate::host::{PartitionHost, PartitionPaths, WriteOp};

use bytes::Bytes;
use orbita_core::{Epoch, KeyRange, NodeId, PartitionId, Record, Version, WriteCondition};
use orbita_runtime::{Clock, Runtime};
use orbita_sim::lin::{check, Recorder, Register, RegisterOp, RegisterRet};
use orbita_sim::{harness, SimRuntime, Simulation};

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

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
            PartitionId(1),
            Epoch(1),
            KeyRange::unbounded(),
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
