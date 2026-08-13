//! What the durability claim actually rests on.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use orbita_core::{Epoch, Error, Lamport, NodeId, PartitionId, Result};
use orbita_runtime::{
    Clock, PeerCall, PeerHandler, Rng, Runtime, ServiceId, Transport, TransportResult,
};

use crate::format::{self, LogRecord, WalEntry, WalOp};
use crate::log::{CatchUp, PartitionLog, TruncationReason, DEFAULT_SEGMENT_TARGET_BYTES};
use crate::owner::{BeyondRetention, ReplicaCatchUp, ReplicationLag, Wal, WalConfig};
use crate::replica::{Hydration, WalService};
use crate::testkit::{block_on, yield_now, Faults, MemDisk, MemNetwork, TestRuntime};
use crate::wire::{AppendRequest, WalResponse};

const PARTITION: PartitionId = PartitionId(7);
const DIR: &str = "wal";
const OWNER: NodeId = NodeId(1);
const PEER_A: NodeId = NodeId(2);
const PEER_B: NodeId = NodeId(3);
const PEER_C: NodeId = NodeId(4);
const PEER_D: NodeId = NodeId(5);

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
// Hydration: a log whose history starts at a manifest horizon.
// -------------------------------------------------------------------------

#[test]
fn a_hydrated_log_resumes_at_the_manifest_horizon_rather_than_at_the_start() {
    // The claim ADR 0006 makes operationally: a node that downloaded the
    // partition holds every write below the horizon, so its log resumes there
    // and the next entry follows the horizon rather than the empty file.
    let base = TestRuntime::solo(40);
    block_on(&base, async {
        let log = PartitionLog::open(base.clone(), DIR, PARTITION, DEFAULT_SEGMENT_TARGET_BYTES)
            .await
            .expect("open");
        assert_eq!(log.durable_lamport().await, Lamport::ZERO);

        assert!(log.hydrate(Lamport(500)).await);
        assert_eq!(log.durable_lamport().await, Lamport(500));
        assert_eq!(log.applied_through().await, Lamport(500));
        assert_eq!(log.hydrated_through().await, Lamport(500));

        let entry = put(501, 1, "after");
        log.append_frames(
            &[format::encode(&LogRecord::Entry(entry.clone()))],
            entry.lamport,
        )
        .await
        .expect("append");
        assert_eq!(log.durable_lamport().await, Lamport(501));
    });
}

#[test]
fn a_hydrated_horizon_is_re_derived_at_every_open_rather_than_written_down() {
    // The rollback promise in `docs/UPGRADES.md` is why this is not a record.
    // Reopening the log finds a file that knows nothing about the horizon, and
    // the caller supplies it again from the manifest it has already read. The
    // tail written above the horizon survives untouched, which is the part a
    // persisted marker got wrong.
    let base = TestRuntime::solo(40);
    block_on(&base, async {
        let log = PartitionLog::open(base.clone(), DIR, PARTITION, DEFAULT_SEGMENT_TARGET_BYTES)
            .await
            .expect("open");
        log.hydrate(Lamport(500)).await;
        let entry = put(501, 1, "after");
        log.append_frames(
            &[format::encode(&LogRecord::Entry(entry.clone()))],
            entry.lamport,
        )
        .await
        .expect("append");

        let reopened =
            PartitionLog::open(base.clone(), DIR, PARTITION, DEFAULT_SEGMENT_TARGET_BYTES)
                .await
                .expect("reopen");
        assert_eq!(
            reopened
                .recovery()
                .entries
                .iter()
                .map(|e| e.lamport)
                .collect::<Vec<_>>(),
            vec![Lamport(501)],
            "the tail above the horizon is in the file and is recovered from it"
        );

        assert!(
            !reopened.hydrate(Lamport(500)).await,
            "the file already stands above the horizon, so re-supplying it changes nothing"
        );
        assert_eq!(reopened.durable_lamport().await, Lamport(501));
    });
}

#[test]
fn a_previous_binary_reads_back_every_log_a_hydrated_node_writes() {
    // The rollback promise, checked rather than argued about. Before
    // finalization an operator may roll a worker back to the binary it was
    // upgraded from, and `docs/UPGRADES.md` says that costs nothing because
    // nothing has written a new format. This framing has no way to skip a
    // record it does not know: an older reader stops at the first unknown kind
    // and truncates everything after it, so a hydration marker at the head of
    // the log would take the whole tail with it and reopen the node at
    // position zero while its storage sat at the manifest horizon. It would
    // then reissue Lamports the segments already hold and have them dropped on
    // apply, which is an acknowledged write lost during a supported rollback.
    let base = TestRuntime::solo(91);
    block_on(&base, async {
        let log = PartitionLog::open(base.clone(), DIR, PARTITION, DEFAULT_SEGMENT_TARGET_BYTES)
            .await
            .expect("open");
        log.hydrate(Lamport(100)).await;
        log.record_fence(Epoch(2)).await.expect("fence");
        for i in 101..=103 {
            let entry = put(i, 2, &format!("key-{i}"));
            log.append_frames(
                &[format::encode(&LogRecord::Entry(entry.clone()))],
                entry.lamport,
            )
            .await
            .unwrap();
        }
        log.checkpoint(Lamport(102)).await.expect("checkpoint");

        let bytes = base.mem_disk().contents(&segment(1)).expect("segment");
        let read = previous_binary_scan(&bytes);
        assert!(
            read.whole_file,
            "a previous binary must reach the end of every log this one writes"
        );
        assert_eq!(
            read.durable,
            Lamport(103),
            "and must recover the tail written above the horizon"
        );
    });
}

/// What the format-version-1 reader does with a segment: parse the header,
/// decode frames, and stop dead at the first record kind it does not know.
///
/// This is the previous binary's recovery, reduced to the part that matters
/// for a rollback. It is written out by hand rather than reusing `scan_segment`
/// so that adding a record kind cannot quietly teach it to accept one.
struct PreviousBinary {
    durable: Lamport,
    whole_file: bool,
}

fn previous_binary_scan(bytes: &[u8]) -> PreviousBinary {
    const KINDS_IT_KNOWS: u8 = 3;
    let mut read = PreviousBinary {
        durable: Lamport::ZERO,
        whole_file: false,
    };
    if format::parse_segment_header(bytes).is_err() {
        return read;
    }
    let mut pos = format::SEGMENT_HEADER_BYTES;
    while pos < bytes.len() {
        // Length and checksum first, then the body, whose first byte names the
        // record kind.
        if bytes[pos + format::FRAME_HEADER_BYTES] > KINDS_IT_KNOWS {
            return read;
        }
        match format::decode(&bytes[pos..]) {
            Ok((record, used)) => {
                if let Some(lamport) = record.lamport() {
                    read.durable = lamport;
                }
                pos += used;
            }
            Err(_) => return read,
        }
    }
    read.whole_file = true;
    read
}

#[test]
fn hydrating_below_where_the_log_already_stands_is_a_no_op() {
    let base = TestRuntime::solo(41);
    block_on(&base, async {
        let log = PartitionLog::open(base.clone(), DIR, PARTITION, DEFAULT_SEGMENT_TARGET_BYTES)
            .await
            .expect("open");
        for i in 1..=4 {
            let entry = put(i, 1, &format!("key-{i}"));
            log.append_frames(
                &[format::encode(&LogRecord::Entry(entry.clone()))],
                entry.lamport,
            )
            .await
            .unwrap();
        }

        assert!(!log.hydrate(Lamport(2)).await);
        assert_eq!(log.durable_lamport().await, Lamport(4));
        assert_eq!(
            log.applied_through().await,
            Lamport::ZERO,
            "a stale horizon must not mark unapplied entries applied"
        );
    });
}

#[test]
fn a_cut_below_the_hydrated_horizon_cannot_disown_the_downloaded_partition() {
    // A fence names where the log's history ends. It cannot name where the
    // partition's data ends, because the segments below the horizon were
    // published by a fenced owner for writes that were already acknowledged.
    let base = TestRuntime::solo(43);
    block_on(&base, async {
        let log = PartitionLog::open(base.clone(), DIR, PARTITION, DEFAULT_SEGMENT_TARGET_BYTES)
            .await
            .expect("open");
        log.hydrate(Lamport(100)).await;
        let entry = put(101, 1, "tail");
        log.append_frames(
            &[format::encode(&LogRecord::Entry(entry.clone()))],
            entry.lamport,
        )
        .await
        .unwrap();

        log.truncate_above(Lamport(50)).await.expect("truncate");
        assert_eq!(
            log.durable_lamport().await,
            Lamport(100),
            "the log gives up its tail, never the horizon its data was built from"
        );
    });
}

/// A hydrator standing in for a partition whose manifest the bucket holds.
struct PublishedManifest {
    found: Hydration,
    calls: Arc<Mutex<usize>>,
}

impl PublishedManifest {
    fn at(epoch: u64, through: u64) -> Self {
        Self {
            found: Hydration {
                epoch: Epoch(epoch),
                through: Lamport(through),
            },
            calls: Arc::new(Mutex::new(0)),
        }
    }
}

#[async_trait::async_trait]
impl crate::replica::PartitionHydrator for PublishedManifest {
    async fn hydrate(&self, _partition: PartitionId) -> Hydration {
        *self.calls.lock().expect("call count poisoned") += 1;
        self.found
    }
}

/// An append that would leave a hole, from a replica's point of view.
fn append_from(prev: Lamport, lamport: u64) -> AppendRequest {
    append_at(Epoch(1), prev, lamport)
}

fn append_at(epoch: Epoch, prev: Lamport, lamport: u64) -> AppendRequest {
    let entry = WalEntry {
        epoch,
        ..put(lamport, 1, "beyond")
    };
    let frame = format::encode(&LogRecord::Entry(entry.clone()));
    AppendRequest {
        partition: PARTITION,
        epoch,
        prev_lamport: prev,
        committed: prev,
        entries: vec![(entry, frame)],
    }
}

#[test]
fn a_replica_beyond_the_retained_log_reports_a_gap_when_it_cannot_hydrate() {
    // The behaviour before this change, kept as the contrast: with nothing in
    // the bucket to build from, the honest answer is still "I have a hole".
    let base = TestRuntime::solo(44);
    block_on(&base, async {
        let net = MemNetwork::new();
        let peer = peer(&base, &net, PEER_A).await;
        let response = block_on_append(&peer, append_from(Lamport(10), 11)).await;
        assert_eq!(
            response,
            WalResponse::Gap {
                durable_lamport: Lamport::ZERO,
                epoch: Epoch(1),
            }
        );
    });
}

#[test]
fn a_replica_beyond_the_retained_log_hydrates_instead_of_reporting_a_gap() {
    // The payoff: the writes under the hole are in the bucket, so the replica
    // downloads them and takes the batch, instead of asking the owner for a
    // retransmission the owner may have checkpointed away.
    let base = TestRuntime::solo(45);
    block_on(&base, async {
        let net = MemNetwork::new();
        let peer = peer(&base, &net, PEER_A).await;
        let manifest = Arc::new(PublishedManifest::at(1, 10));
        let calls = Arc::clone(&manifest.calls);
        peer.service.hydrate_with(manifest);

        let response = block_on_append(&peer, append_from(Lamport(10), 11)).await;
        assert_eq!(
            response,
            WalResponse::Ok {
                durable_lamport: Lamport(11),
                epoch: Epoch(1),
            },
            "the gap closed from object storage and the batch was accepted"
        );
        assert_eq!(*calls.lock().unwrap(), 1);
        assert_eq!(peer.durable().await, Lamport(11));

        // A batch that follows contiguously does not ask again, because the
        // download only happens when the log cannot take the entries as they
        // are.
        let next = block_on_append(&peer, append_from(Lamport(11), 12)).await;
        assert!(matches!(next, WalResponse::Ok { .. }));
        assert_eq!(*calls.lock().unwrap(), 1);
    });
}

#[test]
fn a_hydration_that_does_not_reach_the_batch_still_reports_the_gap_it_closed() {
    // Honesty under a partial answer: the bucket was behind the batch, so the
    // owner is told where this replica now stands rather than being left to
    // assume the append landed.
    let base = TestRuntime::solo(46);
    block_on(&base, async {
        let net = MemNetwork::new();
        let peer = peer(&base, &net, PEER_A).await;
        peer.service
            .hydrate_with(Arc::new(PublishedManifest::at(1, 5)));

        let response = block_on_append(&peer, append_from(Lamport(10), 11)).await;
        assert_eq!(
            response,
            WalResponse::Gap {
                durable_lamport: Lamport(5),
                epoch: Epoch(1),
            }
        );
    });
}

#[test]
fn an_owner_the_manifest_proves_is_deposed_gets_no_acknowledgement() {
    // The replica never saw the epoch-2 fence, so its own log cannot refuse
    // this batch. Hydration reads a manifest only an epoch-2 owner could have
    // published, and that is proof enough: acknowledging here would let a
    // deposed owner reach quorum for a write the real owner later truncates,
    // which loses an acknowledged write.
    let base = TestRuntime::solo(90);
    block_on(&base, async {
        let net = MemNetwork::new();
        let peer = peer(&base, &net, PEER_A).await;
        peer.service
            .hydrate_with(Arc::new(PublishedManifest::at(2, 10)));

        let response = block_on_append(&peer, append_at(Epoch(1), Lamport(10), 11)).await;
        assert_eq!(
            response,
            WalResponse::StaleEpoch { current: Epoch(2) },
            "the manifest's epoch is what answers, because the log has no fence to answer with"
        );
        assert_eq!(
            peer.durable().await,
            Lamport(10),
            "the rebuild is kept; only the acknowledgement is refused"
        );
        assert_eq!(
            peer.epoch().await,
            Epoch(2),
            "the refusal is written down, or it would hold for exactly one batch: the rebuild \
             closes the gap that sent this node to the bucket, so the next attempt would find a \
             contiguous log and never look again"
        );
    });
}

#[test]
fn a_deposed_owner_refused_once_stays_refused_on_every_retry() {
    // The failure the first version of this fix had, and the reason the fence
    // is recorded rather than merely acted on. Hydration removes the gap, so a
    // refusal that lived only in the call that read the manifest would let the
    // deposed owner's very next batch through.
    let base = TestRuntime::solo(90);
    block_on(&base, async {
        let net = MemNetwork::new();
        let peer = peer(&base, &net, PEER_A).await;
        peer.service
            .hydrate_with(Arc::new(PublishedManifest::at(2, 10)));

        for attempt in 0..3 {
            assert_eq!(
                block_on_append(&peer, append_at(Epoch(1), Lamport(10), 11)).await,
                WalResponse::StaleEpoch { current: Epoch(2) },
                "attempt {attempt} was answered as if the sender were still the owner"
            );
        }
    });
}

#[test]
fn a_hydration_that_moves_nothing_does_not_adopt_a_fence_out_of_band() {
    // The fence is only sound where it is recorded: the horizon was above
    // everything this log held, so there was no tail for a newer owner to cut
    // away. Where the horizon does not move, that guarantee is gone, and the
    // refusal has to stand on its own rather than take the epoch with it. The
    // gap is still there in that case, so the next attempt reads the manifest
    // again and is refused again.
    let base = TestRuntime::solo(94);
    block_on(&base, async {
        let net = MemNetwork::new();
        let peer = peer(&base, &net, PEER_A).await;
        peer.service
            .hydrate_with(Arc::new(PublishedManifest::at(2, 0)));

        assert_eq!(
            block_on_append(&peer, append_at(Epoch(1), Lamport(10), 11)).await,
            WalResponse::StaleEpoch { current: Epoch(2) },
        );
        assert_eq!(
            peer.epoch().await,
            Epoch(1),
            "no rebuild happened, so nothing here proves this log has no divergent tail"
        );
        assert_eq!(
            block_on_append(&peer, append_at(Epoch(1), Lamport(10), 11)).await,
            WalResponse::StaleEpoch { current: Epoch(2) },
            "and the gap is still there, so the manifest is consulted again"
        );
    });
}

#[test]
fn the_owner_the_manifest_names_is_served_after_the_deposed_one_is_refused() {
    // The other half of the same rule. A manifest at epoch 2 refuses epoch 1
    // and says nothing against epoch 2, so the real owner closes the same gap
    // and is acknowledged.
    let base = TestRuntime::solo(90);
    block_on(&base, async {
        let net = MemNetwork::new();
        let peer = peer(&base, &net, PEER_A).await;
        peer.service
            .hydrate_with(Arc::new(PublishedManifest::at(2, 10)));

        assert!(matches!(
            block_on_append(&peer, append_at(Epoch(1), Lamport(10), 11)).await,
            WalResponse::StaleEpoch { .. }
        ));
        assert_eq!(
            block_on_append(&peer, append_at(Epoch(2), Lamport(10), 11)).await,
            WalResponse::Ok {
                durable_lamport: Lamport(11),
                epoch: Epoch(2),
            }
        );
    });
}

#[test]
fn a_manifest_behind_the_sender_does_not_fence_the_sender() {
    // An owner at epoch 3 whose predecessor published the last manifest is the
    // ordinary case, and reading that manifest must not turn a live owner into
    // a deposed one.
    let base = TestRuntime::solo(90);
    block_on(&base, async {
        let net = MemNetwork::new();
        let peer = peer(&base, &net, PEER_A).await;
        peer.service
            .hydrate_with(Arc::new(PublishedManifest::at(2, 10)));

        assert_eq!(
            block_on_append(&peer, append_at(Epoch(3), Lamport(10), 11)).await,
            WalResponse::Ok {
                durable_lamport: Lamport(11),
                epoch: Epoch(3),
            }
        );
    });
}

#[test]
fn quiescing_a_hydrated_owner_gives_up_its_tail_without_giving_up_its_horizon() {
    // Where hydration meets the drain path. Quiescing drops the writes that
    // reached this node's disk alone, and it does that by cutting the log,
    // which is the one operation that lowers a durable position. Hydration
    // puts a floor under that cut at the manifest horizon.
    //
    // The two cannot disagree, and the reason is worth stating rather than
    // trusting: a manifest only ever covers writes that were applied, an apply
    // only ever happens after the acknowledgement, so the horizon is at or
    // below the committed prefix by construction. The floor is therefore never
    // the thing that stops a quiesce, and the tail above the prefix still
    // goes.
    let base = TestRuntime::solo(95);
    block_on(&base, async {
        let net = MemNetwork::new();
        let runtime = base.peer(MemDisk::new(), net.node(OWNER));
        // A replica that is registered but never reachable, so every write
        // this owner takes reaches its own disk and no second copy.
        let wal = Wal::open(
            runtime,
            WalConfig::new(PARTITION, DIR, Epoch(2))
                .with_replicas(vec![PEER_A])
                .with_hydration(Hydration {
                    epoch: Epoch(2),
                    through: Lamport(10),
                }),
        )
        .await
        .expect("open");
        assert_eq!(wal.durable_lamport(), Lamport(10));
        assert_eq!(wal.committed_lamport(), Lamport(10));

        for i in 0..3 {
            let _unavailable = wal.commit(op(&format!("one copy {i}"))).await;
        }
        assert!(
            wal.durable_lamport() > Lamport(10),
            "the writes reached this node's own log"
        );

        let after = wal.quiesce().await.expect("quiesce");
        assert_eq!(
            after,
            Lamport(10),
            "the tail no client was told about goes, down to the committed prefix, which \
             here is exactly the horizon this owner was built to"
        );
        assert_eq!(wal.log().hydrated_through().await, Lamport(10));
    });
}

#[test]
fn a_worker_granted_a_superseded_epoch_refuses_to_open_as_owner() {
    // The same evidence at the other end of its life. A replacement worker has
    // no fence record of its own, so without the manifest a stale grant would
    // be indistinguishable from a fresh one and this node would start writing
    // under a dead epoch.
    let base = TestRuntime::solo(92);
    block_on(&base, async {
        let net = MemNetwork::new();
        let runtime = base.peer(MemDisk::new(), net.node(OWNER));
        let opened = Wal::open(
            runtime,
            WalConfig::new(PARTITION, DIR, Epoch(1)).with_hydration(Hydration {
                epoch: Epoch(2),
                through: Lamport(10),
            }),
        )
        .await;
        assert!(
            matches!(
                opened.err(),
                Some(Error::StaleEpoch {
                    got: Epoch(1),
                    current: Epoch(2),
                    ..
                })
            ),
            "a grant the bucket disproves is refused rather than taken"
        );
    });
}

#[test]
fn an_owner_at_the_epoch_that_published_the_manifest_opens_above_its_horizon() {
    let base = TestRuntime::solo(93);
    block_on(&base, async {
        let net = MemNetwork::new();
        let runtime = base.peer(MemDisk::new(), net.node(OWNER));
        let wal = Wal::open(
            runtime,
            WalConfig::new(PARTITION, DIR, Epoch(2)).with_hydration(Hydration {
                epoch: Epoch(2),
                through: Lamport(10),
            }),
        )
        .await
        .expect("the grant matches the manifest");
        assert_eq!(
            wal.durable_lamport(),
            Lamport(10),
            "the sequence resumes above the versions the segments already hold"
        );
    });
}

async fn block_on_append(peer: &Peer, request: AppendRequest) -> WalResponse {
    let bytes = orbita_runtime::PeerHandler::handle(
        &peer.service,
        OWNER,
        orbita_runtime::PeerCall {
            service: ServiceId::Wal,
            method: crate::wire::METHOD_APPEND,
            payload: request.encode(),
        },
    )
    .await
    .expect("the service always answers");
    WalResponse::decode(&bytes).expect("decodable")
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

/// A replica that refuses the first append and opens the partition because of
/// it, then serves normally.
///
/// Event driven rather than timed. The window this reproduces is a race, and a
/// race staged with a timer is at the mercy of what the deterministic runtime
/// decides to run first — a timed version of this test passed against the
/// unfixed code, because the executor advanced to the timer before the owner's
/// first attempt and the replica was warm by the time anything asked it.
///
/// Keying the warm-up to the refusal itself removes the ordering question: the
/// first append is always refused, and the replica is always ready for the
/// second. Nothing but a retry can get a write through this.
#[derive(Clone)]
struct WarmsOnRefusal {
    inner: WalService<TestRuntime>,
    runtime: TestRuntime,
    refused: Arc<Mutex<bool>>,
}

impl PeerHandler for WarmsOnRefusal {
    async fn handle(&self, from: NodeId, call: PeerCall) -> TransportResult<Bytes> {
        let first = {
            let mut refused = self.refused.lock().expect("refusal flag poisoned");
            let first = !*refused;
            *refused = true;
            first
        };
        if first {
            // What a replica that has not reconciled the map yet says, and
            // then, belatedly, does.
            let log = PartitionLog::open(
                self.runtime.clone(),
                DIR,
                PARTITION,
                DEFAULT_SEGMENT_TARGET_BYTES,
            )
            .await
            .expect("open replica log");
            self.inner.register(log);
            return Ok(WalResponse::Error(format!("partition {PARTITION} not held here")).encode());
        }
        self.inner.handle(from, call).await
    }
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
fn a_follower_beyond_the_quorum_cannot_hold_up_a_write() {
    // The property the whole read-scaling argument rests on. A peer added so
    // it can serve reads receives every append, so its view stays current, but
    // it is not counted toward durability. If it were, buying read capacity
    // would mean buying write latency and enlarging the set of nodes any one
    // of which can stall a write, which is the opposite trade.
    let base = TestRuntime::solo(41);
    block_on(&base, async {
        let net = MemNetwork::new();
        let owner_runtime = base.peer(MemDisk::new(), net.node(OWNER));
        let owner = Wal::open(
            owner_runtime.clone(),
            WalConfig::new(PARTITION, DIR, Epoch(1))
                // Four peers, and still the two-of-three durability contract:
                // this node plus one acknowledgement.
                .with_replicas(vec![PEER_A, PEER_B, PEER_C, PEER_D])
                .with_durability_acks(1),
        )
        .await
        .expect("open owner");
        let service = WalService::new();
        service.register(owner.log());
        owner_runtime
            .transport()
            .register(ServiceId::Wal, service.clone());
        let peers = [
            peer(&base, &net, PEER_A).await,
            peer(&base, &net, PEER_B).await,
            peer(&base, &net, PEER_C).await,
            peer(&base, &net, PEER_D).await,
        ];

        // Everything past the quorum goes away. Under a quorum sized from the
        // peer list this would need three acknowledgements and would now fail.
        net.isolate(PEER_B);
        net.isolate(PEER_C);
        net.isolate(PEER_D);

        for i in 1..=5 {
            let lamport = owner
                .commit(op(&format!("k{i}")))
                .await
                .expect("followers are not counted, so losing them changes nothing");
            assert_eq!(lamport, Lamport(i));
        }

        assert_eq!(
            peers[0].durable().await,
            Lamport(5),
            "the peer that answered holds every acknowledged write"
        );
        for absent in &peers[1..] {
            assert_eq!(absent.durable().await, Lamport::ZERO);
        }
    });
}

#[test]
fn a_write_still_needs_a_second_copy_however_many_followers_are_listening() {
    // The other half of the same rule, and the one that would make this
    // dangerous if it were wrong. Followers do not count toward durability, so
    // they must not be able to satisfy it either by standing in for the peer
    // that is gone. Losing every peer fails the write, exactly as it does with
    // two.
    let base = TestRuntime::solo(43);
    block_on(&base, async {
        let net = MemNetwork::new();
        let owner_runtime = base.peer(MemDisk::new(), net.node(OWNER));
        let owner = Wal::open(
            owner_runtime.clone(),
            WalConfig::new(PARTITION, DIR, Epoch(1))
                .with_replicas(vec![PEER_A, PEER_B, PEER_C, PEER_D])
                .with_durability_acks(1),
        )
        .await
        .expect("open owner");
        let service = WalService::new();
        service.register(owner.log());
        owner_runtime
            .transport()
            .register(ServiceId::Wal, service.clone());
        let _peers = [
            peer(&base, &net, PEER_A).await,
            peer(&base, &net, PEER_B).await,
            peer(&base, &net, PEER_C).await,
            peer(&base, &net, PEER_D).await,
        ];

        for id in [PEER_A, PEER_B, PEER_C, PEER_D] {
            net.isolate(id);
        }

        let refused = owner.commit(op("k1")).await;
        assert!(
            refused.is_err(),
            "a write with no second copy must fail however many peers were listening"
        );
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
            matches!(result, Err(Error::Indeterminate(_))),
            "a write that reached one copy is not a write, got {result:?}"
        );
        // Indeterminate rather than Unavailable, because the entry is fsynced
        // here before replication is attempted. The client must not replay it:
        // this log still holds it and the next open will apply it. See #187.
        assert!(
            !result.as_ref().unwrap_err().is_retryable(),
            "a write that may already be durable must not be advertised as safe to replay"
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

/// The key a peer holds at `lamport`, or `None` if its log does not reach
/// there. Reads the log rather than a watermark, because the whole question
/// here is whose *bytes* sit under a version.
async fn key_at(peer: &Peer, lamport: Lamport) -> Option<String> {
    let log = peer.service.log(PARTITION).expect("registered");
    let entry = log
        .entries_after(Lamport(lamport.get() - 1))
        .await
        .ok()
        .and_then(|found| match found {
            CatchUp::Entries(entries) => entries.into_iter().find(|e| e.lamport == lamport),
            _ => None,
        })?;
    match entry.op {
        WalOp::Put { key, .. } => Some(String::from_utf8_lossy(&key).into_owned()),
        WalOp::Delete { key, .. } => Some(String::from_utf8_lossy(&key).into_owned()),
    }
}

/// Leaves a replica holding an entry the owner then gives back: the append
/// lands, the reply is lost, the owner reports the write failed, and quiescing
/// drops it locally. Returns the Lamport that now means different things on the
/// two nodes' disks.
async fn strand_a_tail_on_the_replicas(c: &Cluster) -> Lamport {
    c.owner.commit(op("kept")).await.expect("a healthy write");
    for peer in [PEER_A, PEER_B] {
        c.net.swallow_replies(peer);
    }
    assert!(
        c.owner.commit(op("ghost")).await.is_err(),
        "with no reply coming back the owner cannot call this write durable"
    );
    for peer in [PEER_A, PEER_B] {
        c.net.deliver_replies(peer);
    }
    let ghost = c.owner.durable_lamport();
    assert_eq!(ghost, Lamport(2));
    for peer in &c.peers {
        assert_eq!(
            key_at(peer, ghost).await.as_deref(),
            Some("ghost"),
            "the append landed; only the answer was lost"
        );
    }

    // The split's preparation. The owner gives up everything above the
    // committed prefix, which is where it will start assigning again.
    assert_eq!(c.owner.quiesce().await.unwrap(), Lamport(1));
    assert_eq!(c.owner.surrendered_lamport(), ghost);
    ghost
}

#[test]
fn a_reopened_owner_replaces_a_surrendered_lamport_on_its_replicas() {
    // The abort path's hazard, end to end in one crate. A replica holds an
    // entry at Lamport 2 that the owner has handed back, and the reopened owner
    // is about to issue Lamport 2 again with different bytes. Reopening above
    // the epoch the log records fences the replicas first, so the version means
    // one thing everywhere afterwards.
    let base = TestRuntime::solo(115);
    block_on(&base, async {
        let c = cluster(&base).await;
        let reissued = strand_a_tail_on_the_replicas(&c).await;

        // The abort raised the parent's epoch, so the owner reopens above what
        // its own log records. That is what lets it truncate its replicas.
        let reopened = Wal::open(
            c.owner_peer.runtime.clone(),
            WalConfig::new(PARTITION, DIR, Epoch(2)).with_replicas(vec![PEER_A, PEER_B]),
        )
        .await
        .expect("reopen at the abort's epoch");
        for peer in &c.peers {
            assert_eq!(
                peer.durable().await,
                Lamport(1),
                "fenced back to the prefix"
            );
            assert_eq!(peer.epoch().await, Epoch(2), "and the fence is durable");
        }

        assert_eq!(reopened.commit(op("replacement")).await.unwrap(), reissued);
        for peer in &c.peers {
            assert_eq!(
                key_at(peer, reissued).await.as_deref(),
                Some("replacement"),
                "a replica that skipped the reissued Lamport as a retransmission would still \
                 hold the surrendered entry, and would serve it after a promotion"
            );
        }
    });
}

#[test]
fn reopening_at_the_surrendered_epoch_would_leave_two_values_at_one_version() {
    // The proof the epoch bump is load-bearing rather than decorative. Reopen
    // at the epoch the log already records — which is what an abort that left
    // the parent's epoch alone produces — and nothing tells the replicas their
    // tail is dead. A replica skips an entry at or below its durable position
    // without comparing bytes, so it acknowledges the replacement and goes on
    // holding the original. This asserts that divergence, so a change that made
    // the bump stop mattering fails here rather than passing silently.
    let base = TestRuntime::solo(116);
    block_on(&base, async {
        let c = cluster(&base).await;
        let reissued = strand_a_tail_on_the_replicas(&c).await;

        let reopened = Wal::open(
            c.owner_peer.runtime.clone(),
            WalConfig::new(PARTITION, DIR, Epoch(1)).with_replicas(vec![PEER_A, PEER_B]),
        )
        .await
        .expect("the log permits a same-epoch reopen; only the caller knows better");
        assert_eq!(
            reopened.commit(op("replacement")).await.unwrap(),
            reissued,
            "the owner is told the reissued write reached a quorum"
        );
        for peer in &c.peers {
            assert_eq!(
                key_at(peer, reissued).await.as_deref(),
                Some("ghost"),
                "and the replica still holds the surrendered bytes under that version"
            );
        }
        // Which is exactly why the reopening caller has to ask first.
        assert_eq!(c.owner.surrendered_lamport(), reissued);
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
fn replication_lag_measures_the_worst_replica_that_trails_the_committed_prefix() {
    // The WAL replication-lag signal. A healthy owner reports nothing behind;
    // once a replica is cut off while writes keep committing on the others, the
    // committed prefix advances past it and the lag is exactly that gap.
    let base = TestRuntime::solo(31);
    block_on(&base, async {
        let net = MemNetwork::new();
        let owner_runtime = base.peer(MemDisk::new(), net.node(OWNER));
        let config = WalConfig::new(PARTITION, DIR, Epoch(1)).with_replicas(vec![PEER_A, PEER_B]);
        let owner = Wal::open(owner_runtime, config).await.expect("open owner");
        // Bound rather than dropped: a peer owns the handler it registered.
        let _peers = [
            peer(&base, &net, PEER_A).await,
            peer(&base, &net, PEER_B).await,
        ];

        for i in 1..=5 {
            owner.commit(op(&format!("k{i}"))).await.unwrap();
        }
        assert_eq!(owner.committed_lamport(), Lamport(5));
        assert_eq!(
            owner.replication_lag(),
            ReplicationLag::default(),
            "with every replica caught up nothing is behind"
        );

        // One replica goes away. A majority still stores each write, so the
        // committed prefix climbs to 9 while the isolated replica stays at 5.
        net.isolate(PEER_B);
        for i in 6..=9 {
            owner.commit(op(&format!("k{i}"))).await.unwrap();
        }
        assert_eq!(owner.committed_lamport(), Lamport(9));

        let lag = owner.replication_lag();
        assert_eq!(
            lag.replicas_behind, 1,
            "exactly the isolated replica trails the committed prefix"
        );
        assert_eq!(
            lag.max_lamports, 4,
            "and it trails by the four writes committed without it"
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
fn a_stranded_replica_that_hydrates_clears_itself_by_taking_the_next_batch() {
    // Hydration is the thing that turns `Stranded` back into `Following`, and
    // it does so through the record that already exists rather than through a
    // second one. Nothing on the hydration path touches `catch_up`, `acked` or
    // `replicas_behind`: the replica rebuilds from the bucket, the next batch
    // stops leaving a hole, and the acknowledgement it sends is what moves it.
    //
    // That is worth pinning here rather than only end to end, because the
    // failure it guards against is silent. A hydration path that kept its own
    // notion of who is caught up would contradict this one, and the owner would
    // report a replica unrecoverable through `replicas-recoverable` while the
    // replica was serving reads.
    let base = TestRuntime::solo(23);
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
        net.heal(PEER_B);

        // The cliff, reached first and with no hydrator wired, or the rest of
        // this proves nothing about coming back from it.
        let pass = owner.catch_up_replicas().await.unwrap();
        assert_eq!(pass.stranded, vec![PEER_B]);
        assert_eq!(
            owner.beyond_retention().first().map(|one| one.node),
            Some(PEER_B)
        );

        // The bucket has what the log gave up. This is what the replica's
        // storage engine finds when the next batch turns out to leave a hole.
        peers[1]
            .service
            .hydrate_with(Arc::new(PublishedManifest::at(1, 12)));

        owner.commit(op("k13")).await.expect("the next write");

        assert_eq!(peers[1].durable().await, Lamport(13));
        assert!(
            owner.beyond_retention().is_empty(),
            "the owner has nothing left to report, so `replicas-recoverable` clears on its own"
        );
        assert!(
            owner.replicas_behind().is_empty(),
            "and it is not outstanding work either: the replica confirmed the prefix itself"
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

#[test]
fn a_write_waits_for_a_replica_that_is_still_opening_the_partition() {
    // Issue #168. Nothing makes a replica open a partition before its owner
    // starts taking writes for it: the owner admits writes as soon as its own
    // host is open, and each holder reconciles the map on its own poll. So for
    // a partition that has just appeared — a split child, a new placement,
    // every partition on a cluster that just started — the first thing that
    // ever contacts the replica is the first client write, and it arrives
    // before the replica has registered the partition.
    //
    // Failing there is the wrong answer twice over. The quorum was reachable a
    // moment later, and the error is raised after the entry is already on the
    // owner's disk, so the client cannot safely retry it.
    let base = TestRuntime::solo(77);
    block_on(&base, async {
        let net = MemNetwork::new();
        let owner_runtime = base.peer(MemDisk::new(), net.node(OWNER));
        let owner = Wal::open(
            owner_runtime.clone(),
            WalConfig::new(PARTITION, DIR, Epoch(1))
                .with_replicas(vec![PEER_A])
                .with_durability_acks(1),
        )
        .await
        .expect("open owner");
        let service = WalService::new();
        service.register(owner.log());
        owner_runtime
            .transport()
            .register(ServiceId::Wal, service.clone());

        let replica_runtime = base.peer(MemDisk::new(), net.node(PEER_A));
        replica_runtime.transport().register(
            ServiceId::Wal,
            WarmsOnRefusal {
                inner: WalService::new(),
                runtime: replica_runtime.clone(),
                refused: Arc::new(Mutex::new(false)),
            },
        );

        owner
            .commit(op("k1"))
            .await
            .expect("a replica that was still opening must not fail the write");
    });
}

#[test]
fn a_write_still_fails_quickly_when_a_replica_that_was_following_goes_away() {
    // The other side of the rule, and the reason the retry keys on whether a
    // replica has ever answered rather than on a plain attempt count. A peer
    // that was following and has now gone is an outage, not a partition coming
    // up, and holding every write for the whole retry budget would turn one
    // dead node into a stalled write path.
    let base = TestRuntime::solo(78);
    block_on(&base, async {
        let net = MemNetwork::new();
        let owner_runtime = base.peer(MemDisk::new(), net.node(OWNER));
        let owner = Wal::open(
            owner_runtime.clone(),
            WalConfig::new(PARTITION, DIR, Epoch(1))
                .with_replicas(vec![PEER_A])
                .with_durability_acks(1),
        )
        .await
        .expect("open owner");
        let service = WalService::new();
        service.register(owner.log());
        owner_runtime
            .transport()
            .register(ServiceId::Wal, service.clone());
        let _peer = peer(&base, &net, PEER_A).await;

        // Establish the replica, so it is no longer a partition that has never
        // answered.
        owner.commit(op("k1")).await.expect("the first write lands");

        net.isolate(PEER_A);
        let started = base.clock().monotonic_nanos();
        let refused = owner.commit(op("k2")).await;
        let waited = Duration::from_nanos(base.clock().monotonic_nanos() - started);

        assert!(refused.is_err(), "an outage still fails the write");
        // A fixed bound rather than the budget constant, so shrinking the
        // budget cannot make this pass by moving the goalposts. Well under the
        // two seconds an unestablished replica is given.
        assert!(
            waited < Duration::from_millis(500),
            "a replica that was following and went away must not be waited out \
             for the warm-up budget, waited {waited:?}"
        );
    });
}
