//! Whether concurrent writes share an fsync, checked rather than assumed.
//!
//! An fsync costs the same whether it carries one write or a hundred. On a
//! laptop's NVMe that is a fraction of a millisecond and the difference hides;
//! on network-attached cloud storage it is milliseconds, and a log that syncs
//! once per write is pinned to roughly `1 / fsync_latency` writes per second no
//! matter how many clients are asking. Measured on EBS at 2.77ms per sync, that
//! ceiling is about 361 writes per second, which is what issue #147 is.
//!
//! [`Wal::flush`] already takes the whole pending queue, so the batching is
//! emergent: it depends on how many writers are queued behind the flush lock
//! when a flush wins it. That makes it exactly the sort of property that reads
//! as obviously working and can be entirely absent, so it is counted here.
//!
//! Writers use distinct keys on purpose. Same-key writers would serialise on
//! the conditional-write machinery and this would measure that instead.

use crate::host::{HostSpec, LeasePolicy, PartitionHost, PartitionPaths, WriteOp};
use crate::replication::ReplicaBridge;

use bytes::Bytes;
use orbita_core::{Epoch, KeyRange, KeyspaceId, NodeId, PartitionId, WriteCondition};
use orbita_format::testing::MemoryStore;
use orbita_format::PartitionPath;
use orbita_runtime::{Runtime, ServiceId, Transport};
use orbita_sim::{harness, SimRuntime, Simulation};
use orbita_wal::WalService;

use orbita_storage::ValueCache;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Concurrent writers. Enough that a batching log has a clear opportunity to
/// coalesce and an unbatched one is unambiguously distinguishable.
const WRITERS: u64 = 32;

/// Sequential writes per writer, so the loop is closed rather than one-shot.
const ROUNDS: u64 = 8;

fn partition_paths() -> PartitionPaths {
    PartitionPaths {
        value_cache: Arc::new(ValueCache::new(1 << 20)),
        store: Arc::new(MemoryStore::new()),
        path: PartitionPath::new("", KeyspaceId(1), PartitionId(1)),
        wal_dir: "wal/p1".to_string(),
        wal_segment_bytes: crate::DEFAULT_WAL_SEGMENT_BYTES,
        durability_acks: 1,
    }
}

fn paths_in(store: Arc<MemoryStore>, wal_dir: &str) -> PartitionPaths {
    PartitionPaths {
        value_cache: Arc::new(ValueCache::new(1 << 20)),
        store,
        path: PartitionPath::new("", KeyspaceId(1), PartitionId(1)),
        wal_dir: wal_dir.to_string(),
        wal_segment_bytes: crate::DEFAULT_WAL_SEGMENT_BYTES,
        durability_acks: 1,
    }
}

/// Brings up a replica that actually serves the append protocol, so a batch
/// that would leave a hole is refused by something rather than accepted by a
/// stub.
fn start_replica(
    sim: &Simulation,
    runtime: SimRuntime,
    store: Arc<MemoryStore>,
) -> Arc<PartitionHost<SimRuntime>> {
    let paths = paths_in(store, "wal/replica");
    sim.block_on(async move {
        let (bridge, _applies) = ReplicaBridge::start(&runtime);
        let host = PartitionHost::open_replica(
            runtime.clone(),
            HostSpec {
                id: PartitionId(1),
                epoch: Epoch(1),
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
        host
    })
}

fn put(host: &Arc<PartitionHost<SimRuntime>>, key: &'static [u8]) -> impl Future<Output = ()> {
    let host = Arc::clone(host);
    async move {
        let _ = host
            .write(
                Bytes::from_static(key),
                WriteOp::Put {
                    value: Bytes::from_static(b"v"),
                    ttl_millis: None,
                },
                WriteCondition::None,
            )
            .await;
    }
}

fn open(sim: &Simulation, runtime: SimRuntime) -> Arc<PartitionHost<SimRuntime>> {
    let paths = partition_paths();
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

/// The minimum entries per fsync worth calling batching.
///
/// Deliberately far below what the write path actually achieves, which is 16
/// with these writers, because the exact ratio depends on the interleaving a
/// seed produces. Two is enough to tell "batching" from "not batching" — the
/// regression this guards is the collapse to 1.31, not a change from 16 to 12.
const MIN_BATCH: u64 = 2;

/// Polls a future once without consuming it.
fn poll_once_in_place<F: Future>(future: &mut Pin<Box<F>>) -> bool {
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    future.as_mut().poll(&mut cx).is_ready()
}

/// Drives a future to its first suspension and then abandons it, which is what
/// a dropped request does to the work it had in flight.
fn poll_once_then_drop<F: Future>(mut future: Pin<Box<F>>) {
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    let _ = future.as_mut().poll(&mut cx);
    drop(future);
}

#[test]
fn a_cancelled_write_does_not_strand_the_flusher() {
    harness::check_seeds(
        "group_commit::a_cancelled_write_does_not_strand_the_flusher",
        20,
        |seed| {
            let sim = Simulation::new(seed);
            let runtime = sim.add_node(NodeId(1));
            let host = open(&sim, runtime.clone());

            // A write driven far enough to claim the flusher turn and then
            // dropped, which is what a disconnected client or an expired
            // deadline does to the commit it was waiting on.
            {
                let host = Arc::clone(&host);
                poll_once_then_drop(Box::pin(host.write(
                    Bytes::from_static(b"abandoned"),
                    WriteOp::Put {
                        value: Bytes::from_static(b"v"),
                        ttl_millis: None,
                    },
                    WriteCondition::None,
                )));
            }
            sim.run_until_idle();

            // Whether that write landed is not the question — it was abandoned,
            // so either answer is honest. The question is whether the partition
            // still takes writes, or whether the turn went with it.
            let finished = Arc::new(AtomicBool::new(false));
            {
                let host = Arc::clone(&host);
                let finished = Arc::clone(&finished);
                sim.spawn(async move {
                    let _ = host
                        .write(
                            Bytes::from_static(b"after"),
                            WriteOp::Put {
                                value: Bytes::from_static(b"v"),
                                ttl_millis: None,
                            },
                            WriteCondition::None,
                        )
                        .await;
                    finished.store(true, Ordering::SeqCst);
                });
            }
            sim.run_until_idle();

            // Checked as a flag rather than by awaiting the write, because the
            // failure is that it never resolves. Awaiting it would hang the
            // suite instead of reporting.
            if !finished.load(Ordering::SeqCst) {
                return Err(sim.failure(
                    "a write issued after a cancelled one never resolved: the \
                     cancelled commit took the flusher turn with it and every \
                     later writer is parked behind a flush that will never run"
                        .to_owned(),
                ));
            }
            drop(host);
            Ok(())
        },
    );
}

#[test]
fn concurrent_writes_share_an_fsync() {
    harness::check_seeds(
        "group_commit::concurrent_writes_share_an_fsync",
        20,
        |seed| {
            let sim = Simulation::new(seed);
            let runtime = sim.add_node(NodeId(1));
            let host = open(&sim, runtime.clone());

            for writer in 0..WRITERS {
                let host = Arc::clone(&host);
                sim.spawn(async move {
                    // Closed loop, like a real client: the next write is not
                    // issued until the last one is acknowledged. One-shot writers
                    // all arrive at once and would batch perfectly no matter how
                    // the log behaved, which would make this test prove nothing.
                    for step in 0..ROUNDS {
                        let key = Bytes::from(format!("key-{writer}-{step}"));
                        let _ = host
                            .write(
                                key,
                                WriteOp::Put {
                                    value: Bytes::from_static(b"v"),
                                    ttl_millis: None,
                                },
                                WriteCondition::None,
                            )
                            .await;
                    }
                });
            }

            sim.run_until_idle();

            let (flushes, entries) = host.flush_stats().expect("an owner has a log");
            drop(host);

            if entries < WRITERS * ROUNDS {
                return Err(sim.failure(format!(
                    "only {entries} of {} writes reached the log",
                    WRITERS * ROUNDS
                )));
            }

            // One fsync per write is the failure this exists to catch: on storage
            // where a sync costs milliseconds it pins throughput to the disk's
            // round-trip rate no matter how many clients are asking.
            if flushes * MIN_BATCH > entries {
                return Err(sim.failure(format!(
                    "{entries} writes cost {flushes} fsyncs, or {:.2} entries per sync: \
                 concurrent writers are not sharing a flush, so every write pays a \
                 full disk round trip on its own (issue #147)",
                    entries as f64 / flushes as f64
                )));
            }
            Ok(())
        },
    );
}

#[test]
fn a_cancelled_write_does_not_wedge_replication() {
    harness::check_seeds(
        "group_commit::a_cancelled_write_does_not_wedge_replication",
        20,
        |seed| {
            let sim = Simulation::new(seed);
            let owner_runtime = sim.add_node(NodeId(1));
            let replica_runtime = sim.add_node(NodeId(2));
            let store = Arc::new(MemoryStore::new());

            let _replica = start_replica(&sim, replica_runtime, Arc::clone(&store));

            let paths = paths_in(Arc::clone(&store), "wal/owner");
            let host = sim.block_on({
                let runtime = owner_runtime.clone();
                async move {
                    PartitionHost::open_owner(
                        runtime,
                        HostSpec {
                            id: PartitionId(1),
                            epoch: Epoch(1),
                            range: KeyRange::unbounded(),
                            lease: LeasePolicy::default(),
                        },
                        &paths,
                        vec![NodeId(2)],
                    )
                    .await
                    .expect("the owner opens")
                }
            });

            // A write before the cancellation, to prove replication works.
            let before = sim.block_on({
                let host = Arc::clone(&host);
                async move {
                    host.write(
                        Bytes::from_static(b"before"),
                        WriteOp::Put {
                            value: Bytes::from_static(b"v"),
                            ttl_millis: None,
                        },
                        WriteCondition::None,
                    )
                    .await
                }
            });
            sim.run_until_idle();

            // Abandon a write part way through its flush.
            poll_once_then_drop(Box::pin(put(&host, b"abandoned")));
            sim.run_until_idle();

            // And one after, which is the question.
            let after = sim.block_on({
                let host = Arc::clone(&host);
                async move {
                    host.write(
                        Bytes::from_static(b"after"),
                        WriteOp::Put {
                            value: Bytes::from_static(b"v"),
                            ttl_millis: None,
                        },
                        WriteCondition::None,
                    )
                    .await
                }
            });
            sim.run_until_idle();

            // Three more, because the failure this guards is not a blip: the
            // hole a lost batch leaves is in the Lamport sequence, so every
            // later batch replicates with a `prev_lamport` the replica cannot
            // satisfy and the refusal never clears on its own.
            let mut later = Vec::new();
            for n in 0..3 {
                let r = sim.block_on({
                    let host = Arc::clone(&host);
                    async move {
                        host.write(
                            Bytes::from(format!("later-{n}")),
                            WriteOp::Put {
                                value: Bytes::from_static(b"v"),
                                ttl_millis: None,
                            },
                            WriteCondition::None,
                        )
                        .await
                    }
                });
                sim.run_until_idle();
                later.push(r.is_ok());
            }

            if before.is_err() {
                return Err(sim.failure(
                    "the write before the cancellation did not replicate, so this \
                     scenario proves nothing about the one after it"
                        .to_owned(),
                ));
            }
            if after.is_err() || later.iter().any(|ok| !ok) {
                return Err(sim.failure(format!(
                    "a cancelled write wedged replication: the next write returned \
                     {after:?} and the three after that returned {later:?}. Its batch \
                     went with the dropped future, and the Lamports it had already \
                     been issued are now a permanent hole no later batch can \
                     replicate across (issue #150)"
                )));
            }
            drop(host);
            Ok(())
        },
    );
}

#[test]
fn a_cancelled_write_does_not_leave_the_owner_behind_its_replica() {
    harness::check_seeds(
        "group_commit::a_cancelled_write_does_not_leave_the_owner_behind_its_replica",
        20,
        |seed| {
            let sim = Simulation::new(seed);
            let owner_runtime = sim.add_node(NodeId(1));
            let replica_runtime = sim.add_node(NodeId(2));
            let store = Arc::new(MemoryStore::new());
            let _replica = start_replica(&sim, replica_runtime, Arc::clone(&store));

            let paths = paths_in(Arc::clone(&store), "wal/owner");
            let host = sim.block_on({
                let runtime = owner_runtime.clone();
                async move {
                    PartitionHost::open_owner(
                        runtime,
                        HostSpec {
                            id: PartitionId(1),
                            epoch: Epoch(1),
                            range: KeyRange::unbounded(),
                            lease: LeasePolicy::default(),
                        },
                        &paths,
                        vec![NodeId(2)],
                    )
                    .await
                    .expect("the owner opens")
                }
            });

            let key = Bytes::from_static(b"k");
            let _ = sim.block_on({
                let host = Arc::clone(&host);
                let key = key.clone();
                async move {
                    host.write(
                        key,
                        WriteOp::Put {
                            value: Bytes::from_static(b"v1"),
                            ttl_millis: None,
                        },
                        WriteCondition::None,
                    )
                    .await
                }
            });
            sim.run_until_idle();

            // A second write, abandoned in the window that matters: after its
            // entry is committed to the log and before the request has resolved
            // the overlay or handed the mutation to the applier.
            {
                let mut write = Box::pin({
                    let host = Arc::clone(&host);
                    let key = key.clone();
                    async move {
                        let _ = host
                            .write(
                                key,
                                WriteOp::Put {
                                    value: Bytes::from_static(b"v2"),
                                    ttl_millis: None,
                                },
                                WriteCondition::None,
                            )
                            .await;
                    }
                });
                let mut completed = false;
                for _ in 0..20 {
                    // Checked before polling, not after. Once the log has the
                    // entry, the next poll is the one that resolves the overlay
                    // and hands the mutation to the applier — which is exactly
                    // the step this is trying to skip.
                    if host.committed_prefix().map(|l| l.get()).unwrap_or(0) >= 2 {
                        break;
                    }
                    if poll_once_in_place(&mut write) {
                        completed = true;
                        break;
                    }
                    sim.run_until_idle();
                }
                drop(write);
                assert!(!completed, "the write finished, so nothing was cancelled");
            }
            sim.run_until_idle();

            let seen = sim.block_on({
                let host = Arc::clone(&host);
                let key = key.clone();
                async move { host.get(&key).await }
            });
            let value = seen.ok().flatten().map(|r| r.value);
            // The heartbeat is how an owner learns where its replica's log
            // ends, so it is what moves the committed prefix after the fact.
            for _ in 0..3 {
                sim.block_on({
                    let host = Arc::clone(&host);
                    async move { host.renew_leases().await }
                });
                sim.run_until_idle();
            }
            let committed = host.committed_prefix().map(|l| l.get()).unwrap_or(0);
            if committed < 2 {
                return Err(sim.failure(
                    "the abandoned write never reached the committed prefix, so this \
                     scenario never got to the state it is about"
                        .to_owned(),
                ));
            }

            // The log says the entry is durable at quorum. If the owner is
            // still serving the value underneath it, then it never applied its
            // own committed entry: a restart would replay it and change the
            // answer, and a replica promoted now would serve the newer value
            // while this node served the older one.
            if value.as_deref() != Some(&b"v2"[..]) {
                return Err(sim.failure(format!(
                    "the owner committed lamport {committed} and still serves {value:?}: \
                     cancelling the request skipped the overlay resolution and the apply, \
                     so the owner is behind its own log"
                )));
            }
            drop(host);
            Ok(())
        },
    );
}
