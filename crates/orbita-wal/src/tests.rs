//! What the durability claim actually rests on.

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use orbita_core::{Epoch, Error, Lamport, NodeId, PartitionId, Result};
use orbita_runtime::{Rng, Runtime, ServiceId, Transport};

use crate::format::{self, LogRecord, WalEntry, WalOp};
use crate::log::{CatchUp, PartitionLog, TruncationReason, DEFAULT_SEGMENT_TARGET_BYTES};
use crate::owner::{BeyondRetention, ReplicaCatchUp, Wal, WalConfig};
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
fn a_catch_up_beyond_the_checkpoint_is_reported_apart_from_being_already_caught_up() {
    // These two used to be one answer, and conflating them is how an
    // unrecoverable replica reads like a healthy one. Only the first needs
    // hydration; the second needs nothing at all. See issue #63.
    let base = TestRuntime::solo(6);
    block_on(&base, async {
        let log = PartitionLog::open(base.clone(), DIR, PARTITION, 64)
            .await
            .expect("open");
        for i in 1..=8 {
            let entry = put(i, 1, &format!("key-{i}"));
            let frame = format::encode(&LogRecord::Entry(entry.clone()));
            log.append_frames(&[frame], entry.lamport).await.unwrap();
        }
        log.checkpoint(Lamport(6)).await.expect("checkpoint");

        let retained = match log.entries_after(Lamport::ZERO).await.expect("scan") {
            CatchUp::BeyondRetention { retained_from } => {
                retained_from.expect("segments survive the checkpoint")
            }
            other => panic!("a replica holding nothing must not be told it is fine: {other:?}"),
        };
        assert!(
            retained > Lamport(1),
            "the reported horizon has to be above where the replica is or it diagnoses nothing"
        );

        assert_eq!(
            log.entries_after(Lamport(8)).await.expect("scan"),
            CatchUp::UpToDate,
            "a replica level with the owner is not a replica that fell off a cliff"
        );

        let CatchUp::Entries(entries) = log
            .entries_after(Lamport(retained.get() - 1))
            .await
            .expect("scan")
        else {
            panic!("the retained range is still servable");
        };
        assert_eq!(entries.first().map(|e| e.lamport), Some(retained));
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
fn a_catch_up_stops_at_the_committed_prefix_rather_than_the_local_tail() {
    let base = TestRuntime::solo(112);
    block_on(&base, async {
        let c = cluster(&base).await;
        // Acknowledged at two of three, so this is the committed prefix.
        c.owner.commit(op("kept")).await.unwrap();
        assert_eq!(c.owner.committed_lamport(), Lamport(1));

        // Reached this node's disk and nowhere else. The client was told
        // `Unavailable`, and the local durable position now runs ahead of what
        // anybody was promised.
        c.net.isolate(PEER_A);
        c.net.isolate(PEER_B);
        assert!(c.owner.commit(op("ghost")).await.is_err());
        assert_eq!(c.owner.durable_lamport(), Lamport(2));
        assert_eq!(c.owner.committed_lamport(), Lamport(1));

        c.net.heal(PEER_A);
        c.net.heal(PEER_B);
        let caught_up = c.owner.catch_up_replicas().await.unwrap();
        assert_eq!(caught_up.horizon, Lamport(1), "the horizon is the prefix");
        assert!(caught_up.is_complete());

        // The failed write stays on one copy. Putting it on a second is all it
        // takes for the next promotion to replay it into storage and serve it.
        for peer in &c.peers {
            assert_eq!(
                peer.durable().await,
                Lamport(1),
                "a catch-up carried a write whose client was told it had failed"
            );
        }
    });
}

#[test]
fn a_catch_up_names_the_replica_that_did_not_answer_rather_than_calling_the_pass_done() {
    let base = TestRuntime::solo(113);
    block_on(&base, async {
        let c = cluster(&base).await;
        // One peer misses the write, so two of three is still met and the
        // entry is acknowledged while one advertised copy holds nothing.
        c.net.isolate(PEER_B);
        c.owner.commit(op("first")).await.unwrap();
        assert_eq!(c.owner.committed_lamport(), Lamport(1));

        // A pass judged on whether any call returned would call this finished,
        // and the partition would go on advertising a copy that is empty.
        let caught_up = c.owner.catch_up_replicas().await.unwrap();
        assert_eq!(caught_up.caught_up, vec![PEER_A]);
        assert_eq!(caught_up.behind, vec![PEER_B]);
        assert!(!caught_up.is_complete());
        assert_eq!(c.owner.replicas_behind(), vec![PEER_B]);

        c.net.heal(PEER_B);
        let caught_up = c.owner.catch_up_replicas().await.unwrap();
        assert!(caught_up.is_complete(), "the retry finishes the job");
        assert!(c.owner.replicas_behind().is_empty());
        assert_eq!(c.peers[1].durable().await, Lamport(1));
    });
}

#[test]
fn quiescing_gives_up_the_tail_no_client_was_told_about_and_keeps_the_rest() {
    let base = TestRuntime::solo(114);
    block_on(&base, async {
        let c = cluster(&base).await;
        c.owner.commit(op("kept")).await.unwrap();

        c.net.isolate(PEER_A);
        c.net.isolate(PEER_B);
        assert!(c.owner.commit(op("ghost")).await.is_err());
        assert_eq!(c.owner.durable_lamport(), Lamport(2));

        // A draining owner has to advertise a position a replica may be
        // carried to, and a catch-up may not go past the committed prefix. The
        // only honest way to meet in the middle is to give up the entries this
        // node holds alone and already reported as failed.
        assert_eq!(c.owner.quiesce().await.unwrap(), Lamport(1));
        assert_eq!(c.owner.durable_lamport(), Lamport(1));
        assert_eq!(c.owner.committed_lamport(), Lamport(1));
        assert_eq!(
            c.owner_peer.durable().await,
            Lamport(1),
            "the entry is gone from the log, not merely from the watermark"
        );

        // One way. A Lamport the log has given back must never be reissued, or
        // two writes would share a version.
        assert!(matches!(
            c.owner.commit(op("after")).await,
            Err(Error::Unavailable(_))
        ));

        // And it stays a no-op afterwards, because a drain calls it on every
        // pass rather than once.
        assert_eq!(c.owner.quiesce().await.unwrap(), Lamport(1));
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
fn an_owner_that_has_replicated_nothing_yet_calls_its_replicas_unestablished_not_healthy() {
    // The bug this protects against: an owner opens with an empty view of its
    // replicas, and an empty view read as "nobody is stranded" is a healthy
    // answer nobody earned. After a restart or a promotion, an idle partition
    // would report that forever while a replica sat unable to serve. See the
    // #75 review of issue #63.
    let base = TestRuntime::solo(19);
    block_on(&base, async {
        let c = cluster(&base).await;
        assert_eq!(
            c.owner.catch_up_status(),
            vec![
                (PEER_A, ReplicaCatchUp::Unestablished),
                (PEER_B, ReplicaCatchUp::Unestablished),
            ],
            "a replica this owner has not heard from is not a replica it has cleared"
        );
        assert!(c.owner.beyond_retention().is_empty());
    });
}

#[test]
fn a_reopened_owner_relearns_a_stranded_replica_without_replicating_anything() {
    let base = TestRuntime::solo(20);
    block_on(&base, async {
        // A small segment target so a handful of commits roll several
        // segments, which is what makes a checkpoint drop history.
        let net = MemNetwork::new();
        let owner_runtime = base.peer(MemDisk::new(), net.node(OWNER));
        let mut config =
            WalConfig::new(PARTITION, DIR, Epoch(1)).with_replicas(vec![PEER_A, PEER_B]);
        config.segment_target_bytes = 256;
        let owner = Wal::open(owner_runtime.clone(), config.clone())
            .await
            .expect("open owner");
        let peers = vec![
            peer(&base, &net, PEER_A).await,
            peer(&base, &net, PEER_B).await,
        ];

        // PEER_B misses everything from here, and the owner checkpoints past
        // what it missed.
        net.isolate(PEER_B);
        for i in 1..=12 {
            owner.commit(op(&format!("k{i}"))).await.unwrap();
        }
        owner.checkpoint(Lamport(12)).await.expect("checkpoint");
        let horizon = owner.log().retained_from().await;
        assert!(
            horizon > Lamport(1),
            "the checkpoint has to have dropped history for this to test anything"
        );
        assert_eq!(peers[1].durable().await, Lamport::ZERO);
        drop(owner);
        net.heal(PEER_B);

        // The owner restarts and writes nothing. Its own log still says where
        // history starts, so hearing where a replica is once is enough.
        let reopened = Wal::open(owner_runtime, config).await.expect("reopen");
        assert_eq!(
            reopened.catch_up_status(),
            vec![
                (PEER_A, ReplicaCatchUp::Unestablished),
                (PEER_B, ReplicaCatchUp::Unestablished),
            ]
        );

        for peer in &peers {
            let node = peer.runtime.transport().local_node();
            reopened
                .note_replica_position(node, peer.durable().await)
                .await;
        }
        assert_eq!(
            reopened.beyond_retention(),
            vec![BeyondRetention {
                node: PEER_B,
                replica_durable: Lamport::ZERO,
                retained_from: Some(horizon),
            }],
            "a restart must not lose the fact that a replica cannot be caught up"
        );
        assert_eq!(
            reopened.catch_up_status()[0],
            (
                PEER_A,
                ReplicaCatchUp::Following {
                    through: Lamport(12)
                }
            ),
            "the replica that kept up is cleared by the same report, and the report says where"
        );
    });
}

#[test]
fn quiescing_cannot_strand_a_replica_that_was_following() {
    // The two halves of this branch meet here. Quiesce is the only operation
    // in the crate that makes a log shorter, and a replica is stranded when
    // the entry it needs next is older than where the log's history starts. If
    // giving up a tail could move that horizon, a draining owner would strand
    // its own replicas and clear `replicas-recoverable` on its way out, which
    // is a durability alarm raised by the shutdown rather than by anything
    // wrong. It cannot, because a tail is the newest entries and the horizon
    // is about the oldest, but that is a load-bearing argument and it should
    // fail loudly if it ever stops being true.
    let base = TestRuntime::solo(21);
    block_on(&base, async {
        let net = MemNetwork::new();
        let owner_runtime = base.peer(MemDisk::new(), net.node(OWNER));
        let mut config = WalConfig::new(PARTITION, DIR, Epoch(1)).with_replicas(vec![PEER_A]);
        config.segment_target_bytes = 256;
        let owner = Wal::open(owner_runtime, config).await.expect("open owner");
        // Bound rather than dropped: a peer owns the handler it registered, so
        // letting it fall out of scope would take the replica off the network.
        let _peers = [peer(&base, &net, PEER_A).await];

        for i in 1..=12 {
            owner.commit(op(&format!("k{i}"))).await.unwrap();
        }
        // Checkpointed, so history no longer starts at the beginning and the
        // horizon is a real number rather than a trivial one.
        owner.checkpoint(Lamport(12)).await.expect("checkpoint");
        let horizon = owner.log().retained_from().await;
        assert!(horizon > Lamport(1), "the checkpoint dropped no history");
        owner.note_replica_position(PEER_A, Lamport(12)).await;
        assert!(owner.beyond_retention().is_empty());

        // A tail that reached this disk and nowhere else, which is exactly
        // what a drain has to give up.
        net.isolate(PEER_A);
        assert!(owner.commit(op("ghost")).await.is_err());
        assert!(owner.durable_lamport() > owner.committed_lamport());
        net.heal(PEER_A);

        assert_eq!(owner.quiesce().await.unwrap(), Lamport(12));
        assert!(
            owner.log().retained_from().await <= horizon,
            "giving up a tail moved where history starts, which can strand a replica"
        );

        owner.note_replica_position(PEER_A, Lamport(12)).await;
        assert_eq!(
            owner.catch_up_status(),
            vec![(
                PEER_A,
                ReplicaCatchUp::Following {
                    through: Lamport(12)
                }
            )],
            "a replica that was following is still following a quiesced owner"
        );
        assert!(
            owner.beyond_retention().is_empty(),
            "a draining owner reported its own replica stranded"
        );
        assert!(
            owner.replicas_behind().is_empty(),
            "a replica holding the committed prefix is not outstanding catch-up work"
        );
    });
}

#[test]
fn a_stranded_replica_is_reported_rather_than_retried_forever() {
    // The two models this branch merged answer different questions about the
    // same replica, and this is where they have to agree. A replica past the
    // retention cliff is short of the committed prefix, so a pass judged only
    // on distance would call it outstanding work and the owner would reconcile
    // on every poll for the rest of its life without ever helping it. It
    // belongs to the other channel.
    let base = TestRuntime::solo(22);
    block_on(&base, async {
        let net = MemNetwork::new();
        let owner_runtime = base.peer(MemDisk::new(), net.node(OWNER));
        let mut config =
            WalConfig::new(PARTITION, DIR, Epoch(1)).with_replicas(vec![PEER_A, PEER_B]);
        config.segment_target_bytes = 256;
        let owner = Wal::open(owner_runtime, config).await.expect("open owner");
        let peers = [
            peer(&base, &net, PEER_A).await,
            peer(&base, &net, PEER_B).await,
        ];

        net.isolate(PEER_B);
        for i in 1..=12 {
            owner.commit(op(&format!("k{i}"))).await.unwrap();
        }
        owner.checkpoint(Lamport(12)).await.expect("checkpoint");
        assert_eq!(peers[1].durable().await, Lamport::ZERO);
        net.heal(PEER_B);

        let pass = owner.catch_up_replicas().await.unwrap();
        assert_eq!(pass.horizon, Lamport(12));
        assert_eq!(pass.caught_up, vec![PEER_A]);
        assert_eq!(pass.stranded, vec![PEER_B]);
        assert!(
            pass.behind.is_empty(),
            "a replica no retry can help is not outstanding work"
        );
        assert!(
            pass.is_complete(),
            "the pass did everything it could, so the caller must settle"
        );

        // And it is loud in the channel that exists for it.
        assert!(owner.replicas_behind().is_empty());
        assert_eq!(
            owner.beyond_retention().first().map(|one| one.node),
            Some(PEER_B),
            "the fault has to leave through the retention report"
        );
    });
}

#[test]
fn the_tracked_retention_horizon_is_the_one_a_catch_up_scan_finds() {
    // Two ways of answering "where does my history start" that must not drift:
    // the scan is authoritative and the tracked value is what the heartbeat
    // path can afford to ask on every renewal.
    let base = TestRuntime::solo(21);
    block_on(&base, async {
        let log = PartitionLog::open(base.clone(), DIR, PARTITION, 64)
            .await
            .expect("open");
        assert_eq!(
            log.retained_from().await,
            Lamport(1),
            "an empty log's history starts at the first Lamport nobody has written yet"
        );

        for i in 1..=8 {
            let entry = put(i, 1, &format!("key-{i}"));
            let frame = format::encode(&LogRecord::Entry(entry.clone()));
            log.append_frames(&[frame], entry.lamport).await.unwrap();
        }
        assert_eq!(log.retained_from().await, Lamport(1));

        log.checkpoint(Lamport(6)).await.expect("checkpoint");
        let scanned = match log.entries_after(Lamport::ZERO).await.expect("scan") {
            CatchUp::BeyondRetention { retained_from } => retained_from.expect("entries survive"),
            other => panic!("the checkpoint dropped history: {other:?}"),
        };
        assert_eq!(log.retained_from().await, scanned);
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
