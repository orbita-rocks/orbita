//! Property tests for the write-ahead log's byte decoders.
//!
//! The round-trip and flipped-byte tests next to each decoder prove it reads
//! the frames we wrote and catches the corruptions we imagined. They say
//! nothing about the frames we did not imagine, and a WAL segment or a
//! replication message is bytes off a disk or a socket: a torn append, a lying
//! disk, a peer at a version this build does not implement. Two properties
//! cover the rest.
//!
//! * **Round-trip identity.** Encoding any well-formed record or message and
//!   decoding the result returns the value. For an [`AppendRequest`] this
//!   includes the exact per-entry frames, because a replica writes the owner's
//!   bytes through unchanged and a re-encode would break the end-to-end
//!   checksum.
//! * **Arbitrary bytes never panic.** Any byte string handed to a decoder
//!   returns `Ok` or `Err`, never a panic. With `#![forbid(unsafe_code)]` a
//!   decoder bug can at worst crash the process, so a decoder proven not to
//!   panic is one that cannot be crashed by what a peer or a disk hands it.
//!
//! The WAL has no checked-in golden vector the way the partition format does,
//! so the never-panic corpus is built here from freshly encoded frames and
//! messages, then mutated (exact, one bit flipped, truncated). A
//! plausible-but-broken frame reaches decode paths that pure noise, stopped at
//! the first length or checksum check, never does.
//!
//! A bit flip alone stops at the frame CRC, though, so the pure-mutation
//! properties prove the CRC rejects damage but never run `decode_body` on a
//! corrupt-but-checksum-valid frame. The checksum-repaired properties do: they
//! flip a byte in a frame body and then recompute the frame CRC over
//! `length ++ body`, so the frame is corrupt in its meaning yet valid in its
//! checksum. That is the input a decoder cannot tell from a real frame, and it
//! is what exercises the record decoder past the gate.

use bytes::Bytes;
use orbita_core::{Epoch, Lamport, PartitionId};
use proptest::prelude::*;

use crate::format::{self, segment_header, LogRecord, WalEntry, WalOp, FRAME_HEADER_BYTES};
use crate::wire::{AppendRequest, FenceRequest, StatusRequest, WalResponse};

// ----- value strategies ---------------------------------------------------

fn arb_bytes(max: usize) -> impl Strategy<Value = Bytes> {
    proptest::collection::vec(any::<u8>(), 0..max).prop_map(Bytes::from)
}

fn arb_op() -> impl Strategy<Value = WalOp> {
    prop_oneof![
        (
            arb_bytes(48),
            arb_bytes(64),
            proptest::option::of(any::<u64>())
        )
            .prop_map(|(key, value, expires_at_millis)| WalOp::Put {
                key,
                value,
                expires_at_millis,
            }),
        (arb_bytes(48), proptest::option::of(any::<u64>())).prop_map(
            |(key, tombstone_expires_at_millis)| WalOp::Delete {
                key,
                tombstone_expires_at_millis,
            }
        ),
    ]
}

fn arb_entry() -> impl Strategy<Value = WalEntry> {
    (any::<u64>(), any::<u64>(), any::<u64>(), arb_op()).prop_map(
        |(lamport, epoch, partition, op)| WalEntry {
            lamport: Lamport(lamport),
            epoch: Epoch(epoch),
            partition: PartitionId(partition),
            op,
        },
    )
}

fn arb_record() -> impl Strategy<Value = LogRecord> {
    prop_oneof![
        arb_entry().prop_map(LogRecord::Entry),
        any::<u64>().prop_map(|l| LogRecord::Checkpoint {
            applied_through: Lamport(l),
        }),
        any::<u64>().prop_map(|e| LogRecord::Fence { epoch: Epoch(e) }),
    ]
}

fn arb_append() -> impl Strategy<Value = AppendRequest> {
    (
        any::<u64>(),
        any::<u64>(),
        any::<u64>(),
        any::<u64>(),
        proptest::collection::vec(arb_entry(), 0..6),
    )
        .prop_map(|(partition, epoch, prev, committed, entries)| {
            let entries = entries
                .into_iter()
                .map(|entry| {
                    let frame = format::encode(&LogRecord::Entry(entry.clone()));
                    (entry, frame)
                })
                .collect();
            AppendRequest {
                partition: PartitionId(partition),
                epoch: Epoch(epoch),
                prev_lamport: Lamport(prev),
                committed: Lamport(committed),
                entries,
            }
        })
}

fn arb_response() -> impl Strategy<Value = WalResponse> {
    prop_oneof![
        (any::<u64>(), any::<u64>()).prop_map(|(l, e)| WalResponse::Ok {
            durable_lamport: Lamport(l),
            epoch: Epoch(e),
        }),
        any::<u64>().prop_map(|e| WalResponse::StaleEpoch { current: Epoch(e) }),
        (any::<u64>(), any::<u64>()).prop_map(|(l, e)| WalResponse::Gap {
            durable_lamport: Lamport(l),
            epoch: Epoch(e),
        }),
        // Any UTF-8 string round-trips; the message is read back with from_utf8.
        proptest::string::string_regex("[ -~]{0,64}")
            .expect("a valid regex")
            .prop_map(WalResponse::Error),
    ]
}

// ----- never-panic input strategies ---------------------------------------

/// Arbitrary bytes, augmented with a corpus of plausible-but-broken inputs:
/// each seed exactly, with one byte flipped, and truncated at an arbitrary
/// point. The seeds are what carry a decode past its first length or checksum
/// check, which is where the paths worth testing are.
fn decoder_input(corpus: Vec<Vec<u8>>) -> impl Strategy<Value = Vec<u8>> {
    let exact = proptest::sample::select(corpus.clone());
    let flipped = (
        proptest::sample::select(corpus.clone()),
        any::<proptest::sample::Index>(),
        any::<u8>(),
    )
        .prop_map(|(mut seed, index, xor)| {
            if !seed.is_empty() {
                let at = index.index(seed.len());
                seed[at] ^= xor.max(1);
            }
            seed
        });
    let truncated = (
        proptest::sample::select(corpus),
        any::<proptest::sample::Index>(),
    )
        .prop_map(|(seed, index)| {
            let cut = index.index(seed.len() + 1);
            seed[..cut].to_vec()
        });
    prop_oneof![
        proptest::collection::vec(any::<u8>(), 0..1024),
        exact,
        flipped,
        truncated,
    ]
}

/// Recomputes a WAL frame's CRC over `length ++ body`, so a frame whose body
/// was mutated in place still passes [`format::decode`]'s checksum and reaches
/// `decode_body`. The frame is `len(u32) | crc(u32) | body`, and the checksum
/// covers the length bytes followed by the body.
fn repair_frame(frame: &mut [u8]) {
    if frame.len() < FRAME_HEADER_BYTES {
        return;
    }
    let len = u32::from_le_bytes(frame[0..4].try_into().expect("four bytes")) as usize;
    // The length names where the body ends; clamp it so a mutated length still
    // points inside the buffer, then checksum exactly that span.
    let end = (FRAME_HEADER_BYTES + len).min(frame.len());
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&frame[0..4]);
    hasher.update(&frame[FRAME_HEADER_BYTES..end]);
    frame[4..8].copy_from_slice(&hasher.finalize().to_le_bytes());
}

/// A frame whose body is corrupted but whose CRC is repaired, so it clears the
/// checksum and reaches `decode_body`. A flip in the body leaves the length
/// intact, which is what keeps the repaired checksum the one the decoder reads.
fn checksum_repaired_frame() -> impl Strategy<Value = Vec<u8>> {
    (
        proptest::sample::select(frame_corpus()),
        any::<proptest::sample::Index>(),
        any::<u8>(),
    )
        .prop_map(|(mut seed, index, xor)| {
            if seed.len() > FRAME_HEADER_BYTES {
                let at = FRAME_HEADER_BYTES + index.index(seed.len() - FRAME_HEADER_BYTES);
                seed[at] ^= xor.max(1);
            }
            repair_frame(&mut seed);
            seed
        })
}

/// A single-entry append whose one frame is corrupted in its body and then has
/// its CRC repaired, so [`AppendRequest::decode`] clears the frame checksum and
/// runs the record decode behind it rather than rejecting at the CRC.
fn checksum_repaired_append() -> impl Strategy<Value = Vec<u8>> {
    (any::<proptest::sample::Index>(), any::<u8>()).prop_map(|(index, xor)| {
        let entry = sample_entry();
        let frame = format::encode(&LogRecord::Entry(entry.clone()));
        let request = AppendRequest {
            partition: PartitionId(1),
            epoch: Epoch(2),
            prev_lamport: Lamport(4),
            committed: Lamport(3),
            entries: vec![(entry, frame)],
        };
        let mut bytes = request.encode().to_vec();
        // The fixed header is partition, epoch, prev, committed (four u64s) and
        // a u32 count, so the one frame begins at byte 36.
        let frame_start = 4 * 8 + 4;
        if bytes.len() > frame_start + FRAME_HEADER_BYTES {
            let body = bytes.len() - frame_start - FRAME_HEADER_BYTES;
            let at = frame_start + FRAME_HEADER_BYTES + index.index(body);
            bytes[at] ^= xor.max(1);
            repair_frame(&mut bytes[frame_start..]);
        }
        bytes
    })
}

fn sample_entry() -> WalEntry {
    WalEntry {
        lamport: Lamport(5),
        epoch: Epoch(2),
        partition: PartitionId(7),
        op: WalOp::Put {
            key: Bytes::from_static(b"key"),
            value: Bytes::from_static(b"value"),
            expires_at_millis: Some(1_700_000_000_000),
        },
    }
}

fn frame_corpus() -> Vec<Vec<u8>> {
    vec![
        format::encode(&LogRecord::Entry(sample_entry())).to_vec(),
        format::encode(&LogRecord::Entry(WalEntry {
            op: WalOp::Delete {
                key: Bytes::from_static(b"gone"),
                tombstone_expires_at_millis: None,
            },
            ..sample_entry()
        }))
        .to_vec(),
        format::encode(&LogRecord::Checkpoint {
            applied_through: Lamport(9),
        })
        .to_vec(),
        format::encode(&LogRecord::Fence { epoch: Epoch(4) }).to_vec(),
    ]
}

fn header_corpus() -> Vec<Vec<u8>> {
    vec![
        segment_header(PartitionId(7)).to_vec(),
        segment_header(PartitionId(0)).to_vec(),
    ]
}

fn append_corpus() -> Vec<Vec<u8>> {
    let one = AppendRequest {
        partition: PartitionId(1),
        epoch: Epoch(2),
        prev_lamport: Lamport(4),
        committed: Lamport(3),
        entries: vec![{
            let e = sample_entry();
            let f = format::encode(&LogRecord::Entry(e.clone()));
            (e, f)
        }],
    };
    vec![one.encode().to_vec()]
}

fn response_corpus() -> Vec<Vec<u8>> {
    vec![
        WalResponse::Ok {
            durable_lamport: Lamport(9),
            epoch: Epoch(1),
        }
        .encode()
        .to_vec(),
        WalResponse::StaleEpoch { current: Epoch(7) }
            .encode()
            .to_vec(),
        WalResponse::Gap {
            durable_lamport: Lamport(3),
            epoch: Epoch(1),
        }
        .encode()
        .to_vec(),
        WalResponse::Error("nope".to_string()).encode().to_vec(),
    ]
}

fn small_corpus() -> Vec<Vec<u8>> {
    // Fence and status are three and one fixed-width fields respectively; a
    // single well-formed message is enough to seed a mutation strategy.
    vec![
        FenceRequest {
            partition: PartitionId(1),
            epoch: Epoch(2),
            truncate_above: Lamport(3),
        }
        .encode()
        .to_vec(),
        StatusRequest {
            partition: PartitionId(1),
        }
        .encode()
        .to_vec(),
    ]
}

// ----- frame decoder ------------------------------------------------------
// Decoder: crates/orbita-wal/src/format.rs:226 (format::decode)

proptest! {
    #[test]
    fn a_frame_round_trips_through_encode_and_decode(record in arb_record()) {
        let frame = format::encode(&record);
        let (decoded, used) = format::decode(&frame).expect("a well-formed frame decodes");
        prop_assert_eq!(decoded, record);
        prop_assert_eq!(used, frame.len(), "the whole frame is consumed");
    }

    #[test]
    fn decoding_arbitrary_bytes_as_a_frame_never_panics(
        bytes in decoder_input(frame_corpus())
    ) {
        let _ = format::decode(&bytes);
    }

    #[test]
    fn decoding_a_checksum_valid_corrupt_frame_never_panics(
        bytes in checksum_repaired_frame()
    ) {
        // The body is corrupt but the frame CRC agrees, so this reaches
        // `decode_body` past the checksum, which the flipped-seed property
        // cannot: its stale CRC fails first.
        let _ = format::decode(&bytes);
    }
}

// ----- segment header decoder ---------------------------------------------
// Decoder: crates/orbita-wal/src/format.rs:136 (format::parse_segment_header)

proptest! {
    #[test]
    fn a_segment_header_round_trips(partition in any::<u64>()) {
        let header = segment_header(PartitionId(partition));
        let decoded = format::parse_segment_header(&header).expect("a written header parses");
        prop_assert_eq!(decoded, PartitionId(partition));
    }

    #[test]
    fn parsing_arbitrary_bytes_as_a_segment_header_never_panics(
        bytes in decoder_input(header_corpus())
    ) {
        let _ = format::parse_segment_header(&bytes);
    }
}

// ----- AppendRequest decoder ----------------------------------------------
// Decoder: crates/orbita-wal/src/wire.rs:73 (AppendRequest::decode)

proptest! {
    #[test]
    fn an_append_round_trips_with_its_frames_intact(request in arb_append()) {
        let decoded = AppendRequest::decode(&request.encode()).expect("a built append decodes");
        prop_assert_eq!(decoded, request);
    }

    #[test]
    fn decoding_arbitrary_bytes_as_an_append_never_panics(
        bytes in decoder_input(append_corpus())
    ) {
        let _ = AppendRequest::decode(&Bytes::from(bytes));
    }

    #[test]
    fn decoding_a_checksum_valid_corrupt_append_never_panics(
        bytes in checksum_repaired_append()
    ) {
        // The framed entry's CRC agrees over its corrupt body, so decode gets
        // past the per-frame checksum into the entry decode and the trailing
        // structural checks rather than stopping at the CRC.
        let _ = AppendRequest::decode(&Bytes::from(bytes));
    }
}

// ----- FenceRequest / StatusRequest decoders ------------------------------
// Decoders: wire.rs:127 (FenceRequest::decode), wire.rs:151 (StatusRequest).

proptest! {
    #[test]
    fn a_fence_round_trips(
        partition in any::<u64>(),
        epoch in any::<u64>(),
        truncate_above in any::<u64>(),
    ) {
        let request = FenceRequest {
            partition: PartitionId(partition),
            epoch: Epoch(epoch),
            truncate_above: Lamport(truncate_above),
        };
        let decoded = FenceRequest::decode(&request.encode()).expect("a built fence decodes");
        prop_assert_eq!(decoded, request);
    }

    #[test]
    fn a_status_round_trips(partition in any::<u64>()) {
        let request = StatusRequest {
            partition: PartitionId(partition),
        };
        let decoded = StatusRequest::decode(&request.encode()).expect("a built status decodes");
        prop_assert_eq!(decoded, request);
    }

    #[test]
    fn decoding_arbitrary_bytes_as_a_fence_or_status_never_panics(
        bytes in decoder_input(small_corpus())
    ) {
        let buf = Bytes::from(bytes);
        let _ = FenceRequest::decode(&buf);
        let _ = StatusRequest::decode(&buf);
    }
}

// ----- WalResponse decoder ------------------------------------------------
// Decoder: crates/orbita-wal/src/wire.rs:213 (WalResponse::decode)

proptest! {
    #[test]
    fn a_response_round_trips(response in arb_response()) {
        let decoded = WalResponse::decode(&response.encode()).expect("a built response decodes");
        prop_assert_eq!(decoded, response);
    }

    #[test]
    fn decoding_arbitrary_bytes_as_a_response_never_panics(
        bytes in decoder_input(response_corpus())
    ) {
        let _ = WalResponse::decode(&Bytes::from(bytes));
    }
}
