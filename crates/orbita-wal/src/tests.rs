//! What the durability claim actually rests on.

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use orbita_core::{Epoch, Error, Lamport, NodeId, PartitionId, Result};
use orbita_runtime::{Rng, Runtime, ServiceId, Transport};

use crate::format::{self, LogRecord, WalEntry, WalOp};
use crate::log::{PartitionLog, TruncationReason, DEFAULT_SEGMENT_TARGET_BYTES};
use crate::owner::{Wal, WalConfig};
use crate::replica::WalService;
use crate::testkit::{block_on, yield_now, Faults, MemDisk, MemNetwork, TestRuntime};
use crate::wire::{AppendRequest, WalResponse};

const PARTITION: PartitionId = PartitionId(7);
const DIR: &str = "wal";
const OWNER: NodeId = NodeId(1);
const PEER_A: NodeId = NodeId(2);
const PEER_B: NodeId = NodeId(3);

fn segment(seq: u64) -> String {
    format!("{DIR}/{seq:012}.wal")
}

fn put(lamport: u64, epoch: u64, key: &str) -> WalEntry {
    WalEntry {
        lamport: Lamport(lamport),
        epoch: Epoch(epoch),
        partition: PARTITION,
        op: WalOp::Put {
            key: Bytes::copy_from_slice(key.as_bytes()),
            value: Bytes::from_static(b"value"),
            expires_at_millis: None,
        },
    }
}

fn op(key: &str) -> WalOp {
    WalOp::Put {
        key: Bytes::copy_from_slice(key.as_bytes()),
        value: Bytes::from_static(b"value"),
        expires_at_millis: None,
    }
}

// -------------------------------------------------------------------------
// The log on its own.
// -------------------------------------------------------------------------

/// Writes `count` entries, one per fsync, and hands back the bytes on disk.
async fn build_log(runtime: &TestRuntime, count: u64) -> Vec<u8> {
    let log = PartitionLog::open(
        runtime.clone(),
        DIR,
        PARTITION,
        DEFAULT_SEGMENT_TARGET_BYTES,
    )
    .await
    .expect("open");
    for i in 1..=count {
        let entry = put(i, 1, &format!("key-{i}"));
        let frame = format::encode(&LogRecord::Entry(entry.clone()));
        log.append_frames(&[frame], entry.lamport)
            .await
            .expect("append");
    }
    runtime.mem_disk().contents(&segment(1)).expect("segment")
}

/// The offset each entry's frame ends at, given the same entries `build_log`
/// writes.
fn frame_ends(count: u64) -> Vec<usize> {
    let mut ends = Vec::new();
    let mut offset = format::SEGMENT_HEADER_BYTES;
    for i in 1..=count {
        offset += format::encode(&LogRecord::Entry(put(i, 1, &format!("key-{i}")))).len();
        ends.push(offset);
    }
    ends
}

#[test]
fn a_crash_at_any_byte_offset_leaves_a_recoverable_log() {
    let base = TestRuntime::solo(1);
    let bytes = block_on(&base, build_log(&base, 6));
    let ends = frame_ends(6);

    for cut in 0..=bytes.len() {
        let disk = MemDisk::new();
        disk.write_raw(&segment(1), bytes[..cut].to_vec());
        let runtime = base.peer(disk.clone(), MemNetwork::new().node(OWNER));

        let log = block_on(
            &runtime,
            PartitionLog::open(
                runtime.clone(),
                DIR,
                PARTITION,
                DEFAULT_SEGMENT_TARGET_BYTES,
            ),
        )
        .expect("a torn log must still open");

        let expected = ends.iter().filter(|end| **end <= cut).count();
        let recovery = log.recovery();
        assert_eq!(
            recovery.entries.len(),
            expected,
            "a crash after {cut} bytes must keep exactly the {expected} whole entries"
        );
        assert_eq!(
            recovery.durable_lamport,
            Lamport(expected as u64),
            "durable Lamport after a crash at {cut} bytes"
        );
        for (i, entry) in recovery.entries.iter().enumerate() {
            assert_eq!(*entry, put(i as u64 + 1, 1, &format!("key-{}", i + 1)));
        }

        // A cut that lands on a record boundary, or right after the segment
        // header, leaves nothing torn behind.
        let clean = cut == format::SEGMENT_HEADER_BYTES || ends.contains(&cut);
        assert_eq!(
            recovery.truncated.is_some(),
            !clean,
            "a cut at {cut} should report truncation only when it lands mid record"
        );

        // The torn bytes are gone, not merely ignored, so the next append
        // cannot land after a hole.
        if cut >= format::SEGMENT_HEADER_BYTES {
            let on_disk = disk.contents(&segment(1)).expect("segment");
            let kept = ends.iter().filter(|end| **end <= cut).next_back();
            assert_eq!(
                on_disk.len(),
                *kept.unwrap_or(&format::SEGMENT_HEADER_BYTES),
                "the log must be physically truncated to the last whole entry"
            );
        }
    }
}

#[test]
fn corrupt_bytes_mid_log_are_reported_as_truncation_and_never_returned_as_data() {
    let base = TestRuntime::solo(2);
    let bytes = block_on(&base, build_log(&base, 4));
    let ends = frame_ends(4);

    for damaged in 0..bytes.len() {
        let mut copy = bytes.clone();
        copy[damaged] ^= 0b0010_0000;

        let disk = MemDisk::new();
        disk.write_raw(&segment(1), copy);
        let runtime = base.peer(disk, MemNetwork::new().node(OWNER));
        let log = block_on(
            &runtime,
            PartitionLog::open(
                runtime.clone(),
                DIR,
                PARTITION,
                DEFAULT_SEGMENT_TARGET_BYTES,
            ),
        )
        .expect("a corrupt log must open, reporting what it dropped");

        let survivors = ends.iter().filter(|end| **end <= damaged).count();
        let recovery = log.recovery();
        assert_eq!(
            recovery.entries.len(),
            survivors,
            "damage at byte {damaged} must drop that entry and everything after it"
        );
        assert!(
            recovery.truncated.is_some(),
            "damage at byte {damaged} must be reported, not silently swallowed"
        );
    }
}

#[test]
fn a_foreign_or_torn_segment_header_yields_an_empty_log_rather_than_garbage() {
    let base = TestRuntime::solo(3);
    let disk = MemDisk::new();
    disk.write_raw(&segment(1), b"this is not a wal segment".to_vec());
    let runtime = base.peer(disk, MemNetwork::new().node(OWNER));

    let log = block_on(
        &runtime,
        PartitionLog::open(
            runtime.clone(),
            DIR,
            PARTITION,
            DEFAULT_SEGMENT_TARGET_BYTES,
        ),
    )
    .expect("open");

    assert!(log.recovery().entries.is_empty());
    assert_eq!(
        log.recovery().truncated.map(|t| t.reason),
        Some(TruncationReason::Malformed)
    );
}

#[test]
fn segments_roll_over_and_a_checkpoint_removes_the_ones_fully_applied() {
    let base = TestRuntime::solo(4);
    block_on(&base, async {
        let log = PartitionLog::open(base.clone(), DIR, PARTITION, 64)
            .await
            .expect("open");
        for i in 1..=8 {
            let entry = put(i, 1, &format!("key-{i}"));
            let frame = format::encode(&LogRecord::Entry(entry.clone()));
            log.append_frames(&[frame], entry.lamport).await.unwrap();
        }
        assert!(
            base.mem_disk().paths().len() > 1,
            "the log must roll over rather than growing one file forever"
        );

        log.checkpoint(Lamport(6)).await.expect("checkpoint");
        assert!(
            base.mem_disk().paths().len() < 8,
            "applied segments must be removed"
        );

        // Reopening proves the checkpoint is durable and that replay starts
        // after it rather than from the beginning.
        let reopened = PartitionLog::open(base.clone(), DIR, PARTITION, 64)
            .await
            .expect("reopen");
        let recovery = reopened.recovery();
        assert_eq!(recovery.applied_through, Lamport(6));
        assert_eq!(recovery.durable_lamport, Lamport(8));
        assert_eq!(
            recovery
                .entries
                .iter()
                .map(|e| e.lamport)
                .collect::<Vec<_>>(),
            vec![Lamport(7), Lamport(8)],
            "only unapplied entries need replaying"
        );
    });
}

#[test]
fn a_log_written_by_two_owners_at_once_is_cut_where_the_lamports_go_backwards() {
    let base = TestRuntime::solo(5);
    let bytes = block_on(&base, build_log(&base, 3));
    let mut forged = bytes.clone();
    forged.extend_from_slice(&format::encode(&LogRecord::Entry(put(2, 9, "rewritten"))));

    let disk = MemDisk::new();
    disk.write_raw(&segment(1), forged);
    let runtime = base.peer(disk, MemNetwork::new().node(OWNER));
    let log = block_on(
        &runtime,
        PartitionLog::open(
            runtime.clone(),
            DIR,
            PARTITION,
            DEFAULT_SEGMENT_TARGET_BYTES,
        ),
    )
    .expect("open");

    assert_eq!(log.recovery().durable_lamport, Lamport(3));
    assert_eq!(
        log.recovery().truncated.map(|t| t.reason),
        Some(TruncationReason::LamportOutOfOrder)
    );
}

// -------------------------------------------------------------------------
// A three node partition.
// -------------------------------------------------------------------------

struct Peer {
    runtime: TestRuntime,
    service: WalService<TestRuntime>,
}

impl Peer {
    async fn durable(&self) -> Lamport {
        self.service
            .log(PARTITION)
            .expect("registered")
            .durable_lamport()
            .await
    }

    async fn epoch(&self) -> Epoch {
        self.service
            .log(PARTITION)
            .expect("registered")
            .epoch()
            .await
    }
}

struct Cluster {
    net: MemNetwork,
    owner: Arc<Wal<TestRuntime>>,
    owner_peer: Peer,
    peers: Vec<Peer>,
}

async fn peer(base: &TestRuntime, net: &MemNetwork, id: NodeId) -> Peer {
    let runtime = base.peer(MemDisk::new(), net.node(id));
    let log = PartitionLog::open(
        runtime.clone(),
        DIR,
        PARTITION,
        DEFAULT_SEGMENT_TARGET_BYTES,
    )
    .await
    .expect("open replica log");
    let service = WalService::new();
    service.register(log);
    runtime
        .transport()
        .register(ServiceId::Wal, service.clone());
    Peer { runtime, service }
}

async fn cluster(base: &TestRuntime) -> Cluster {
    let net = MemNetwork::new();
    let owner_runtime = base.peer(MemDisk::new(), net.node(OWNER));
    let owner = Wal::open(
        owner_runtime.clone(),
        WalConfig::new(PARTITION, DIR, Epoch(1)).with_replicas(vec![PEER_A, PEER_B]),
    )
    .await
    .expect("open owner");

    // The owner serves its own log too, so that after a failover the node can
    // take appends from whoever replaced it without reopening anything.
    let service = WalService::new();
    service.register(owner.log());
    owner_runtime
        .transport()
        .register(ServiceId::Wal, service.clone());

    Cluster {
        owner,
        owner_peer: Peer {
            runtime: owner_runtime,
            service,
        },
        peers: vec![
            peer(base, &net, PEER_A).await,
            peer(base, &net, PEER_B).await,
        ],
        net,
    }
}

#[test]
fn a_commit_is_acknowledged_once_two_of_three_hold_it() {
    let base = TestRuntime::solo(10);
    block_on(&base, async {
        let c = cluster(&base).await;
        assert_eq!(c.owner.commit(op("a")).await.unwrap(), Lamport(1));
        assert_eq!(c.owner.durable_lamport(), Lamport(1));
        assert_eq!(c.owner.committed_lamport(), Lamport(1));
        for peer in &c.peers {
            assert_eq!(peer.durable().await, Lamport(1));
        }
    });
}

#[test]
fn losing_one_of_three_replicas_loses_no_acknowledged_write() {
    let base = TestRuntime::solo(11);
    block_on(&base, async {
        let c = cluster(&base).await;
        c.net.isolate(PEER_B);

        for i in 1..=5 {
            let lamport = c
                .owner
                .commit(op(&format!("k{i}")))
                .await
                .expect("a single lost replica must not stop the write path");
            assert_eq!(lamport, Lamport(i));
        }

        assert_eq!(
            c.peers[0].durable().await,
            Lamport(5),
            "the surviving replica holds every acknowledged write"
        );
        assert_eq!(c.peers[1].durable().await, Lamport::ZERO);

        // If the owner now dies, the surviving replica can be promoted with
        // nothing missing.
        let survivor = PartitionLog::open(
            c.peers[0].runtime.clone(),
            DIR,
            PARTITION,
            DEFAULT_SEGMENT_TARGET_BYTES,
        )
        .await
        .expect("reopen");
        assert_eq!(survivor.recovery().durable_lamport, Lamport(5));
    });
}

#[test]
fn losing_two_replicas_fails_the_write_rather_than_acknowledging_it() {
    let base = TestRuntime::solo(12);
    block_on(&base, async {
        let c = cluster(&base).await;
        c.net.isolate(PEER_A);
        c.net.isolate(PEER_B);

        let result = c.owner.commit(op("lonely")).await;
        assert!(
            matches!(result, Err(Error::Unavailable(_))),
            "a write that reached one copy is not a write, got {result:?}"
        );
        assert_eq!(
            c.owner.committed_lamport(),
            Lamport::ZERO,
            "nothing may be reported as committed"
        );

        // Availability comes back with the peers, and the entry that failed is
        // still on this node's disk, so a retry does not duplicate it.
        c.net.heal(PEER_A);
        assert_eq!(c.owner.commit(op("again")).await.unwrap(), Lamport(2));
        assert_eq!(c.peers[0].durable().await, Lamport(2));
    });
}

#[test]
fn an_append_from_a_fenced_owner_is_rejected_however_well_formed_it_is() {
    let base = TestRuntime::solo(13);
    block_on(&base, async {
        let c = cluster(&base).await;
        c.owner.commit(op("first")).await.unwrap();

        // The control plane promoted someone else, and told the replicas.
        let promoted = Wal::open(
            base.peer(MemDisk::new(), c.net.node(NodeId(9))),
            WalConfig::new(PARTITION, DIR, Epoch(1)).with_replicas(vec![PEER_A, PEER_B]),
        )
        .await
        .unwrap();
        promoted.promote(Epoch(2)).await.unwrap();

        for peer in &c.peers {
            assert_eq!(peer.epoch().await, Epoch(2), "the fence must be durable");
        }

        let result = c.owner.commit(op("second")).await;
        assert!(
            matches!(result, Err(Error::StaleEpoch { .. })),
            "a deposed owner must be refused, got {result:?}"
        );
        assert!(
            matches!(
                c.owner.commit(op("third")).await,
                Err(Error::StaleEpoch { .. })
            ),
            "and must stay refused rather than retrying its way back in"
        );
    });
}

#[test]
fn a_replica_that_restarts_still_refuses_the_owner_it_already_fenced() {
    let base = TestRuntime::solo(14);
    block_on(&base, async {
        let c = cluster(&base).await;

        let promoted = Wal::open(
            base.peer(MemDisk::new(), c.net.node(NodeId(9))),
            WalConfig::new(PARTITION, DIR, Epoch(1)).with_replicas(vec![PEER_A]),
        )
        .await
        .unwrap();
        promoted.promote(Epoch(4)).await.unwrap();

        // Reopening from the same bytes is what a restart is.
        let restarted = PartitionLog::open(
            c.peers[0].runtime.clone(),
            DIR,
            PARTITION,
            DEFAULT_SEGMENT_TARGET_BYTES,
        )
        .await
        .unwrap();
        assert_eq!(
            restarted.epoch().await,
            Epoch(4),
            "a fence that a restart forgets is not a fence"
        );

        let service = WalService::new();
        service.register(restarted);
        c.peers[0]
            .runtime
            .transport()
            .register(ServiceId::Wal, service.clone());

        // Well formed in every respect except the epoch it carries.
        let stale = AppendRequest {
            partition: PARTITION,
            epoch: Epoch(1),
            prev_lamport: Lamport::ZERO,
            committed: Lamport::ZERO,
            entries: vec![{
                let entry = put(1, 1, "sneaky");
                let frame = format::encode(&LogRecord::Entry(entry.clone()));
                (entry, frame)
            }],
        };
        let bytes = c
            .owner_peer
            .runtime
            .transport()
            .call(
                PEER_A,
                orbita_runtime::PeerCall {
                    service: ServiceId::Wal,
                    method: crate::wire::METHOD_APPEND,
                    payload: stale.encode(),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            WalResponse::decode(&bytes).unwrap(),
            WalResponse::StaleEpoch { current: Epoch(4) }
        );
    });
}

#[test]
fn a_replica_that_missed_a_batch_is_caught_up_rather_than_left_with_a_hole() {
    let base = TestRuntime::solo(15);
    block_on(&base, async {
        let c = cluster(&base).await;
        c.net.isolate(PEER_B);
        for i in 1..=3 {
            c.owner.commit(op(&format!("k{i}"))).await.unwrap();
        }
        assert_eq!(c.peers[1].durable().await, Lamport::ZERO);

        c.net.heal(PEER_B);
        c.owner.commit(op("k4")).await.unwrap();

        // The fast replica wins the race, so the backfill of the slow one
        // finishes after the commit has already been acknowledged.
        for _ in 0..8 {
            yield_now().await;
        }

        assert_eq!(
            c.peers[1].durable().await,
            Lamport(4),
            "a replica behind by a batch is backfilled, not left with a hole"
        );
        let recovered = PartitionLog::open(
            c.peers[1].runtime.clone(),
            DIR,
            PARTITION,
            DEFAULT_SEGMENT_TARGET_BYTES,
        )
        .await
        .unwrap();
        assert_eq!(
            recovered
                .recovery()
                .entries
                .iter()
                .map(|e| e.lamport.get())
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
    });
}

#[test]
fn a_promoted_owner_truncates_a_divergent_tail_that_was_never_acknowledged() {
    let base = TestRuntime::solo(16);
    block_on(&base, async {
        let c = cluster(&base).await;
        for i in 1..=3 {
            c.owner.commit(op(&format!("k{i}"))).await.unwrap();
        }

        // The owner writes two more entries that reach nobody else, so they
        // were never acknowledged to a client.
        c.net.isolate(PEER_A);
        c.net.isolate(PEER_B);
        assert!(c.owner.commit(op("orphan-4")).await.is_err());
        assert!(c.owner.commit(op("orphan-5")).await.is_err());
        assert_eq!(c.owner.durable_lamport(), Lamport(5));
        c.net.heal(PEER_A);
        c.net.heal(PEER_B);

        // The control plane promotes the most caught-up survivor, which holds
        // three entries, and the old owner becomes one of its replicas.
        c.peers[0].service.unregister(PARTITION);
        let promoted = Wal::open(
            c.peers[0].runtime.clone(),
            WalConfig::new(PARTITION, DIR, Epoch(1)).with_replicas(vec![OWNER, PEER_B]),
        )
        .await
        .unwrap();
        c.peers[0].service.register(promoted.log());
        assert_eq!(promoted.durable_lamport(), Lamport(3));

        promoted.promote(Epoch(2)).await.unwrap();
        assert_eq!(
            c.owner_peer.durable().await,
            Lamport(3),
            "the old owner's unacknowledged tail is discarded, not merged"
        );

        assert_eq!(promoted.commit(op("k4")).await.unwrap(), Lamport(4));
        assert_eq!(c.owner_peer.durable().await, Lamport(4));

        let reopened = PartitionLog::open(
            c.owner_peer.runtime.clone(),
            DIR,
            PARTITION,
            DEFAULT_SEGMENT_TARGET_BYTES,
        )
        .await
        .unwrap();
        assert_eq!(
            reopened
                .recovery()
                .entries
                .iter()
                .map(|e| e.lamport.get())
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4],
            "the discarded entries must not come back on restart"
        );
    });
}

#[test]
fn a_peer_that_was_unreachable_at_promotion_is_fenced_by_its_first_append() {
    let base = TestRuntime::solo(17);
    block_on(&base, async {
        let c = cluster(&base).await;
        for i in 1..=2 {
            c.owner.commit(op(&format!("k{i}"))).await.unwrap();
        }
        c.net.isolate(PEER_A);
        c.net.isolate(PEER_B);
        assert!(c.owner.commit(op("orphan")).await.is_err());

        c.peers[0].service.unregister(PARTITION);
        let promoted = Wal::open(
            c.peers[0].runtime.clone(),
            WalConfig::new(PARTITION, DIR, Epoch(1)).with_replicas(vec![OWNER, PEER_B]),
        )
        .await
        .unwrap();
        c.peers[0].service.register(promoted.log());
        c.net.heal(PEER_A);

        // The old owner is still unreachable when the fence goes out.
        promoted.promote(Epoch(2)).await.unwrap();
        c.net.heal(OWNER);
        c.net.heal(PEER_B);

        assert_eq!(promoted.commit(op("k3")).await.unwrap(), Lamport(3));
        assert_eq!(
            c.owner_peer.durable().await,
            Lamport(3),
            "the first append at the new epoch cuts the divergent tail"
        );
        assert_eq!(c.owner_peer.epoch().await, Epoch(2));
    });
}

#[test]
fn concurrent_commits_share_one_fsync() {
    let base = TestRuntime::solo(18);
    let disk = block_on(&base, async {
        let c = cluster(&base).await;
        let owner_disk = c.owner_peer.runtime.mem_disk();
        let results: Arc<Mutex<Vec<Result<Lamport>>>> = Arc::new(Mutex::new(Vec::new()));

        for i in 0..8 {
            let wal = Arc::clone(&c.owner);
            let results = Arc::clone(&results);
            base.spawn(async move {
                let result = wal.commit(op(&format!("k{i}"))).await;
                results.lock().unwrap().push(result);
            });
        }

        for _ in 0..10_000 {
            if results.lock().unwrap().len() == 8 {
                break;
            }
            yield_now().await;
        }

        let results = results.lock().unwrap();
        assert_eq!(results.len(), 8, "every commit must finish");
        assert!(results.iter().all(Result::is_ok), "{results:?}");
        assert_eq!(c.owner.committed_lamport(), Lamport(8));
        owner_disk
    });

    assert!(
        disk.syncs() < 8,
        "eight concurrent commits paid for {} fsyncs; batching is what the 5ms target rests on",
        disk.syncs()
    );
}

#[test]
fn a_failed_fsync_stops_the_owner_rather_than_guessing() {
    let base = TestRuntime::solo(19);
    block_on(&base, async {
        let c = cluster(&base).await;
        c.owner_peer.runtime.mem_disk().set_faults(Faults {
            fail_next_sync: true,
            ..Faults::default()
        });

        let result = c.owner.commit(op("doomed")).await;
        assert!(matches!(result, Err(Error::Internal(_))), "got {result:?}");
        assert!(
            c.owner.commit(op("after")).await.is_err(),
            "a log whose durability is unknown must not keep taking writes"
        );
    });
}

#[test]
fn a_torn_local_append_is_dropped_and_the_owner_reopens_cleanly() {
    let base = TestRuntime::solo(20);
    block_on(&base, async {
        let c = cluster(&base).await;
        c.owner.commit(op("kept")).await.unwrap();

        c.owner_peer.runtime.mem_disk().set_faults(Faults {
            tear_next_append_at: Some(3),
            ..Faults::default()
        });
        assert!(c.owner.commit(op("torn")).await.is_err());

        let reopened = PartitionLog::open(
            c.owner_peer.runtime.clone(),
            DIR,
            PARTITION,
            DEFAULT_SEGMENT_TARGET_BYTES,
        )
        .await
        .unwrap();
        assert_eq!(reopened.recovery().durable_lamport, Lamport(1));
        assert_eq!(
            reopened.recovery().truncated.map(|t| t.reason),
            Some(TruncationReason::TornTail)
        );
    });
}

#[test]
fn the_control_plane_can_ask_a_peer_how_far_it_has_logged() {
    let base = TestRuntime::solo(21);
    block_on(&base, async {
        let c = cluster(&base).await;
        c.owner.commit(op("a")).await.unwrap();
        assert_eq!(
            c.owner.replica_status(PEER_A).await.unwrap(),
            (Lamport(1), Epoch(1))
        );
    });
}

#[test]
fn a_randomised_schedule_of_commits_and_lost_peers_never_loses_an_acknowledged_write() {
    for seed in 0..12u64 {
        let base = TestRuntime::solo(seed);
        block_on(&base, async {
            let c = cluster(&base).await;
            let mut acknowledged: Vec<Lamport> = Vec::new();

            for step in 0..40 {
                match base.rng().below(4) {
                    0 => {
                        let target = if base.rng().chance(1, 2) {
                            PEER_A
                        } else {
                            PEER_B
                        };
                        c.net.isolate(target);
                    }
                    1 => {
                        c.net.heal(PEER_A);
                        c.net.heal(PEER_B);
                    }
                    2 => {
                        c.net.slow(if base.rng().chance(1, 2) {
                            PEER_A
                        } else {
                            PEER_B
                        });
                    }
                    _ => {}
                }

                if let Ok(lamport) = c.owner.commit(op(&format!("k{step}"))).await {
                    acknowledged.push(lamport);
                }
            }

            c.net.heal(PEER_A);
            c.net.heal(PEER_B);

            // Two of three is the whole contract: every write the owner
            // acknowledged is on this node and at least one other.
            for lamport in acknowledged {
                let mut holders = 0;
                if c.owner_peer.durable().await >= lamport {
                    holders += 1;
                }
                for peer in &c.peers {
                    if peer.durable().await >= lamport {
                        holders += 1;
                    }
                }
                assert!(
                    holders >= 2,
                    "seed {seed}: {lamport} was acknowledged but only {holders} nodes hold it"
                );
            }
        });
    }
}
