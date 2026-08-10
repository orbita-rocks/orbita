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

use bytes::Bytes;
use orbita_core::{Epoch, KeyRange, KeyspaceId, NodeId, PartitionId, WriteCondition};
use orbita_format::testing::MemoryStore;
use orbita_format::PartitionPath;
use orbita_sim::{harness, SimRuntime, Simulation};

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
        store: Arc::new(MemoryStore::new()),
        path: PartitionPath::new("", KeyspaceId(1), PartitionId(1)),
        wal_dir: "wal/p1".to_string(),
        wal_segment_bytes: crate::DEFAULT_WAL_SEGMENT_BYTES,
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
