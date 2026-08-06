//! Property tests for the partition-v1 byte decoders.
//!
//! The golden vectors and the hand-flipped-byte tests prove the decoders read
//! the bytes we meant to write and catch the corruptions we thought of. They
//! say nothing about the bytes we did not think of, and those are exactly the
//! bytes a decoder meets in the wild: a truncated download, a half-overwritten
//! object, a bucket something else wrote to. Two properties close that gap.
//!
//! * **Round-trip identity.** Encoding any well-formed value and decoding the
//!   result returns the value. This is the guarantee the format exists to make.
//! * **Arbitrary bytes never panic.** Feeding a decoder any byte string returns
//!   `Ok` or `Err`, never a panic. `#![forbid(unsafe_code)]` bounds a decoder
//!   bug to a crash rather than memory unsafety, so a proven no-panic decoder is
//!   a decoder that cannot take the process down on bad input.
//!
//! The never-panic generators are seeded with the checked-in golden vectors and
//! with freshly encoded values, then mutated (exact, one bit flipped,
//! truncated). Pure random bytes rarely get past the first length or magic
//! check; a corrupted-but-plausible object is where the interesting paths live,
//! and the golden vectors are the most plausible objects there are.
//!
//! A bit flip on its own does not go far, though: every checksum in this format
//! catches it at the first gate, so the pure-mutation properties prove the gate
//! rejects damage but never exercise the parsing behind it. A panic hiding past
//! a checksum would go unseen. The checksum-repaired properties close that: they
//! mutate a valid input and then recompute every enclosing checksum, so the
//! bytes are corrupt in their meaning yet valid in their integrity fields. That
//! is exactly what a decoder cannot tell apart from a genuine object, and it is
//! the input that reaches the offset arithmetic, the entry loop, and the record
//! body — the code the pure-mutation properties leave untouched.

use std::path::{Path, PathBuf};

use bytes::Bytes;
use orbita_core::{Epoch, KeyRange, KeyspaceId, Lamport, PartitionId};
use orbita_format::record::{ExternalValue, RecordValue};
use orbita_format::segment::{
    BuiltSegment, Segment, SegmentBuilder, SegmentFooter, SegmentHeader, SegmentIndex, FOOTER_LEN,
    HEADER_LEN,
};
use orbita_format::{Manifest, SegmentEntry, SegmentRecord};
use proptest::prelude::*;

const KEYSPACE: KeyspaceId = KeyspaceId(1);
const PARTITION: PartitionId = PartitionId(7);
const EPOCH: Epoch = Epoch(6);

const GOLDEN_SEGMENT: &str = "tests/golden/partition-v1.oseg";
const GOLDEN_MANIFEST: &str = "tests/golden/partition-v1.manifest.json";

fn golden(relative: &str) -> Vec<u8> {
    let path: PathBuf = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative);
    std::fs::read(&path).unwrap_or_else(|e| panic!("reading golden vector {relative}: {e}"))
}

// ----- value strategies ---------------------------------------------------

fn arb_bytes(max: usize) -> impl Strategy<Value = Bytes> {
    proptest::collection::vec(any::<u8>(), 0..max).prop_map(Bytes::from)
}

/// Printable ASCII, which is a subset of UTF-8, so an external name always
/// survives the decoder's `from_utf8` check and the round-trip stays about the
/// framing rather than about string validity.
fn arb_name() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[ -~]{0,48}").expect("a valid regex")
}

fn arb_value() -> impl Strategy<Value = RecordValue> {
    prop_oneof![
        Just(RecordValue::Tombstone),
        arb_bytes(64).prop_map(RecordValue::Inline),
        (arb_name(), any::<u64>(), any::<u32>()).prop_map(|(name, length, crc32c)| {
            RecordValue::External(ExternalValue {
                name,
                length,
                crc32c,
            })
        }),
    ]
}

fn arb_record() -> impl Strategy<Value = SegmentRecord> {
    (
        arb_bytes(48),
        any::<u64>(),
        proptest::option::of(any::<u64>()),
        arb_value(),
    )
        .prop_map(|(key, lamport, expires_at_millis, value)| SegmentRecord {
            key,
            lamport: Lamport(lamport),
            expires_at_millis,
            value,
        })
}

/// A built, well-formed segment together with the records it was built from.
///
/// Keys come out of a `BTreeMap`, so they are unique and ascending, which is
/// exactly what the builder demands. A non-empty map guarantees a non-empty
/// segment, which the builder also demands.
fn arb_segment() -> impl Strategy<Value = (Vec<SegmentRecord>, BuiltSegment)> {
    proptest::collection::btree_map(
        arb_bytes(16),
        (
            any::<u64>(),
            proptest::option::of(any::<u64>()),
            arb_value(),
        ),
        1..8,
    )
    .prop_map(|map| {
        let records: Vec<SegmentRecord> = map
            .into_iter()
            .map(|(key, (lamport, expires_at_millis, value))| SegmentRecord {
                key,
                lamport: Lamport(lamport),
                expires_at_millis,
                value,
            })
            .collect();
        let mut builder = SegmentBuilder::new(KEYSPACE, PARTITION, EPOCH);
        for record in &records {
            builder.push(record).expect("keys ascend out of a BTreeMap");
        }
        let built = builder.finish().expect("the map is non-empty");
        (records, built)
    })
}

/// A valid manifest, assembled from actually-built segments so that every
/// invariant the decoder enforces holds by construction: real names, real
/// sizes, keys inside the range, and a horizon at or above every segment.
fn arb_manifest() -> impl Strategy<Value = Manifest> {
    proptest::collection::vec(arb_segment().prop_map(|(_, built)| built), 0..4).prop_map(|builts| {
        let committed = builts
            .iter()
            .map(|b| b.max_lamport.get())
            .max()
            .unwrap_or(0);
        let segments: Vec<SegmentEntry> = builts
            .iter()
            .enumerate()
            .map(|(i, built)| {
                let name = format!("segments/{:016x}-{:016x}.oseg", EPOCH.get(), i);
                SegmentEntry::of(name, built)
            })
            .collect();
        Manifest {
            keyspace_id: KEYSPACE,
            partition_id: PARTITION,
            epoch: EPOCH,
            committed_lamport: Lamport(committed),
            // Unbounded contains every key, so no generated segment can fall
            // outside it.
            range: KeyRange::unbounded(),
            segments,
        }
    })
}

// ----- never-panic input strategies ---------------------------------------

/// Arbitrary bytes, augmented with a corpus of plausible-but-broken inputs:
/// each seed exactly, with one byte flipped, and truncated at an arbitrary
/// point. Pure noise almost never clears the first magic or length check, so
/// the seeds are what actually exercise the decode paths past it.
fn decoder_input(corpus: Vec<Vec<u8>>) -> impl Strategy<Value = Vec<u8>> {
    let exact = proptest::sample::select(corpus.clone());
    let flipped = (
        proptest::sample::select(corpus.clone()),
        any::<prop::sample::Index>(),
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
        any::<prop::sample::Index>(),
    )
        .prop_map(|(seed, index)| {
            let cut = index.index(seed.len() + 1);
            seed[..cut].to_vec()
        });
    prop_oneof![
        proptest::collection::vec(any::<u8>(), 0..2048),
        exact,
        flipped,
        truncated,
    ]
}

fn record_corpus() -> Vec<Vec<u8>> {
    let mut corpus = vec![
        SegmentRecord {
            key: Bytes::from_static(b"alpha"),
            lamport: Lamport(1),
            expires_at_millis: None,
            value: RecordValue::Inline(Bytes::from_static(b"first")),
        }
        .encode(),
        SegmentRecord {
            key: Bytes::from_static(b"charlie"),
            lamport: Lamport(3),
            expires_at_millis: Some(1_700_086_400_000),
            value: RecordValue::Tombstone,
        }
        .encode(),
        SegmentRecord {
            key: Bytes::from_static(b"delta"),
            lamport: Lamport(4),
            expires_at_millis: None,
            value: RecordValue::External(ExternalValue {
                name: "values/x.oval".to_string(),
                length: 13,
                crc32c: 0xdead_beef,
            }),
        }
        .encode(),
    ];
    // Every record in the golden segment is also a record on its own; seed
    // with the whole object so a decoder reading it as one framed record still
    // never panics.
    corpus.push(golden(GOLDEN_SEGMENT));
    corpus
}

fn segment_corpus() -> Vec<Vec<u8>> {
    vec![golden(GOLDEN_SEGMENT)]
}

fn manifest_corpus() -> Vec<Vec<u8>> {
    let golden_manifest = golden(GOLDEN_MANIFEST);
    let empty = Manifest::empty(KEYSPACE, PARTITION, EPOCH, KeyRange::unbounded())
        .encode()
        .to_vec();
    vec![golden_manifest, empty]
}

// ----- checksum-repaired input strategies ---------------------------------
//
// The pure-mutation strategies above stop at the first checksum. These start
// from a valid input, corrupt it, and then repair the checksums the corruption
// invalidated, so the bytes clear the integrity gate and the parsing logic
// behind it is what actually runs.

/// Recomputes a framed record's leading CRC over `length ++ body`, so a body
/// mutated in place still passes [`SegmentRecord::decode`]'s checksum and lands
/// in `decode_body`. The frame is `crc32c(u32) | length(u32) | body`, and the
/// checksum covers everything from the length onwards.
fn repair_record_frame(frame: &mut [u8]) {
    if frame.len() < 8 {
        return;
    }
    let length = u32::from_le_bytes(frame[4..8].try_into().expect("four bytes")) as usize;
    // The framed length decides where the record ends; clamp it so a mutated
    // length still names a slice that exists, then checksum exactly that slice.
    let total = (8 + length).min(frame.len());
    let checksum = crc32c::crc32c(&frame[4..total]);
    frame[..4].copy_from_slice(&checksum.to_le_bytes());
}

/// A framed record whose body is corrupted but whose CRC is repaired, so it
/// flows past the checksum into `decode_body`. Seeded from the same corpus as
/// the pure-mutation property; a flip in the body leaves the framing length
/// intact, which is what keeps the repaired checksum reachable.
fn checksum_repaired_record() -> impl Strategy<Value = Vec<u8>> {
    (
        proptest::sample::select(record_corpus()),
        any::<prop::sample::Index>(),
        any::<u8>(),
    )
        .prop_map(|(mut seed, index, xor)| {
            if seed.len() > 8 {
                let at = 8 + index.index(seed.len() - 8);
                seed[at] ^= xor.max(1);
            }
            repair_record_frame(&mut seed);
            seed
        })
}

/// A segment footer with a valid magic, version, and self-checksum, carrying
/// arbitrary offset, length, and lamport fields. Built by overwriting the
/// mutable words of a real footer and repairing the footer's own CRC, so the
/// bytes clear [`SegmentFooter::decode`]'s integrity gate and the offsets reach
/// `index_range`, where an unchecked add would wrap.
#[allow(clippy::too_many_arguments)]
fn checksum_valid_footer(
    index_offset: u64,
    index_length: u64,
    record_count: u64,
    min_lamport: u64,
    max_lamport: u64,
    data_crc32c: u32,
    index_crc32c: u32,
) -> Vec<u8> {
    // The last FOOTER_LEN bytes of the golden segment are a valid footer; reuse
    // its magic and version rather than restating the private constants here.
    let segment = golden(GOLDEN_SEGMENT);
    let mut footer = segment[segment.len() - FOOTER_LEN as usize..].to_vec();
    footer[0..8].copy_from_slice(&index_offset.to_le_bytes());
    footer[8..16].copy_from_slice(&index_length.to_le_bytes());
    footer[16..24].copy_from_slice(&record_count.to_le_bytes());
    footer[24..32].copy_from_slice(&min_lamport.to_le_bytes());
    footer[32..40].copy_from_slice(&max_lamport.to_le_bytes());
    footer[40..44].copy_from_slice(&data_crc32c.to_le_bytes());
    footer[44..48].copy_from_slice(&index_crc32c.to_le_bytes());
    // 48..52 is reserved and must stay zero; 56..64 keep the golden version and
    // magic. The footer's own CRC covers bytes 0..52.
    let checksum = crc32c::crc32c(&footer[..52]);
    footer[52..56].copy_from_slice(&checksum.to_le_bytes());
    footer
}

/// A built segment with one byte flipped somewhere in its data or index
/// section and every enclosing checksum repaired: the containing record frame
/// when the flip lands in the data section, the data-section and index-section
/// CRCs in the footer, and the footer's own CRC. The result clears every
/// integrity gate in [`Segment::decode`] and exercises the structural checks
/// past them on bytes no encoder would ever produce.
fn checksum_repaired_segment() -> impl Strategy<Value = Vec<u8>> {
    (arb_segment(), any::<prop::sample::Index>(), any::<u8>()).prop_map(
        |((_, built), index, xor)| {
            let mut bytes = built.bytes.to_vec();
            let footer_at = bytes.len() - FOOTER_LEN as usize;
            let index_offset =
                u64::from_le_bytes(bytes[footer_at..footer_at + 8].try_into().expect("eight"))
                    as usize;
            let index_length = u64::from_le_bytes(
                bytes[footer_at + 8..footer_at + 16]
                    .try_into()
                    .expect("eight"),
            ) as usize;

            // Flip a byte anywhere in the data or index sections, which together
            // span the header end up to the footer.
            let region = footer_at - HEADER_LEN as usize;
            if region > 0 {
                let at = HEADER_LEN as usize + index.index(region);
                bytes[at] ^= xor.max(1);

                // A flip in the data section breaks the record frame it lands
                // in; repair that frame so the corruption reaches the record
                // decoder rather than dying at its checksum.
                if at < index_offset {
                    for entry in built.index.entries() {
                        let start = entry.offset as usize;
                        let end = start + entry.record_length as usize;
                        if (start..end).contains(&at) {
                            repair_record_frame(&mut bytes[start..end]);
                            break;
                        }
                    }
                }
            }

            // Repair the two section checksums and then the footer's own.
            let data_crc = crc32c::crc32c(&bytes[HEADER_LEN as usize..index_offset]);
            let index_crc = crc32c::crc32c(&bytes[index_offset..index_offset + index_length]);
            bytes[footer_at + 40..footer_at + 44].copy_from_slice(&data_crc.to_le_bytes());
            bytes[footer_at + 44..footer_at + 48].copy_from_slice(&index_crc.to_le_bytes());
            let checksum = crc32c::crc32c(&bytes[footer_at..footer_at + 52]);
            bytes[footer_at + 52..footer_at + 56].copy_from_slice(&checksum.to_le_bytes());
            bytes
        },
    )
}

// ----- SegmentRecord ------------------------------------------------------
// Decoder: crates/orbita-format/src/record.rs:166 (SegmentRecord::decode)

proptest! {
    #[test]
    fn a_record_round_trips_through_encode_and_decode(record in arb_record()) {
        let encoded = record.encode();
        let (decoded, consumed) =
            SegmentRecord::decode(&encoded).expect("a well-formed record decodes");
        prop_assert_eq!(decoded, record);
        prop_assert_eq!(consumed, encoded.len(), "the whole record is consumed");
    }

    #[test]
    fn decoding_arbitrary_bytes_as_a_record_never_panics(
        bytes in decoder_input(record_corpus())
    ) {
        // The return value is deliberately discarded: the property is that this
        // call returns at all rather than panicking.
        let _ = SegmentRecord::decode(&bytes);
    }

    #[test]
    fn decoding_a_checksum_valid_corrupt_record_never_panics(
        bytes in checksum_repaired_record()
    ) {
        // The body is corrupt but the frame CRC agrees, so this reaches
        // `decode_body` past the checksum. That is the path the pure-mutation
        // property cannot reach, because its flip fails the CRC first.
        let _ = SegmentRecord::decode(&bytes);
    }
}

// ----- Segment ------------------------------------------------------------
// Decoder: crates/orbita-format/src/segment.rs:490 (Segment::decode)

proptest! {
    #[test]
    fn a_segment_round_trips_to_the_records_it_was_built_from(
        (records, built) in arb_segment()
    ) {
        let segment = Segment::decode(&built.bytes).expect("a built segment decodes");
        prop_assert_eq!(segment.records(), records.as_slice());
        prop_assert_eq!(segment.footer.record_count, records.len() as u64);
    }

    #[test]
    fn decoding_arbitrary_bytes_as_a_segment_never_panics(
        bytes in decoder_input(segment_corpus())
    ) {
        let _ = Segment::decode(&bytes);
    }

    #[test]
    fn decoding_a_checksum_valid_corrupt_segment_never_panics(
        bytes in checksum_repaired_segment()
    ) {
        // Every checksum in the object agrees, so decode clears the data and
        // index CRCs and the per-record frame CRCs and lands on the structural
        // checks: index-versus-data agreement, ascending keys, offset math.
        // Those run only on integrity-valid bytes, which a bit flip never is.
        let _ = Segment::decode(&bytes);
    }
}

// ----- SegmentHeader / SegmentFooter / SegmentIndex -----------------------
// Decoders: segment.rs:67, segment.rs:141, segment.rs:261. These are the
// pieces Segment::decode is built from, and a reader that fetched only a tail
// range calls the footer decoder directly, so each is exercised on its own.

proptest! {
    #[test]
    fn decoding_arbitrary_bytes_as_a_segment_header_never_panics(
        bytes in decoder_input(segment_corpus())
    ) {
        let _ = SegmentHeader::decode(&bytes);
    }

    #[test]
    fn decoding_arbitrary_bytes_as_a_segment_footer_never_panics(
        bytes in proptest::collection::vec(any::<u8>(), 0..128),
        object_bytes in any::<u64>(),
    ) {
        if let Ok(footer) = SegmentFooter::decode(&bytes) {
            // The offsets came off untrusted bytes; computing where the index
            // sits must reject rather than wrap, at any claimed object size.
            let _ = footer.index_range(object_bytes);
        }
    }

    #[test]
    fn decoding_a_checksum_valid_footer_never_panics(
        index_offset in any::<u64>(),
        index_length in any::<u64>(),
        record_count in any::<u64>(),
        min_lamport in any::<u64>(),
        max_lamport in any::<u64>(),
        data_crc32c in any::<u32>(),
        index_crc32c in any::<u32>(),
        object_bytes in any::<u64>(),
    ) {
        // A footer whose magic, version, and self-checksum all agree, so decode
        // gets past its integrity gate to the min/max-lamport check and, on
        // success, to `index_range`. The random-bytes property above almost
        // never assembles a valid checksum, so it rarely reaches here at all.
        let bytes = checksum_valid_footer(
            index_offset,
            index_length,
            record_count,
            min_lamport,
            max_lamport,
            data_crc32c,
            index_crc32c,
        );
        if let Ok(footer) = SegmentFooter::decode(&bytes) {
            let _ = footer.index_range(object_bytes);
        }
    }

    #[test]
    fn decoding_arbitrary_bytes_as_a_segment_index_never_panics(
        bytes in proptest::collection::vec(any::<u8>(), 0..512),
        index_offset in any::<u64>(),
        index_length in any::<u64>(),
        record_count in any::<u64>(),
        min_lamport in any::<u64>(),
        max_lamport in any::<u64>(),
        data_crc32c in any::<u32>(),
        index_crc32c in any::<u32>(),
    ) {
        let footer = SegmentFooter {
            index_offset,
            index_length,
            record_count,
            min_lamport: Lamport(min_lamport),
            max_lamport: Lamport(max_lamport),
            data_crc32c,
            index_crc32c,
        };
        let _ = SegmentIndex::decode(&bytes, &footer);
    }

    #[test]
    fn decoding_a_checksum_valid_segment_index_never_panics(
        bytes in proptest::collection::vec(any::<u8>(), 0..512),
        index_offset in any::<u64>(),
        index_length in any::<u64>(),
        record_count in any::<u64>(),
        min_lamport in any::<u64>(),
        max_lamport in any::<u64>(),
        data_crc32c in any::<u32>(),
    ) {
        // Derive `index_crc32c` from the generated bytes so decode agrees at the
        // checksum and proceeds into the entry loop: the key-length reads, the
        // ascending-key check, and the count-versus-footer check. The property
        // with an independently generated checksum returns at the first
        // comparison and never runs any of that.
        let index_crc32c = crc32c::crc32c(&bytes);
        let footer = SegmentFooter {
            index_offset,
            index_length,
            record_count,
            min_lamport: Lamport(min_lamport),
            max_lamport: Lamport(max_lamport),
            data_crc32c,
            index_crc32c,
        };
        let _ = SegmentIndex::decode(&bytes, &footer);
    }
}

// ----- Manifest -----------------------------------------------------------
// Decoder: crates/orbita-format/src/manifest.rs:154 (Manifest::decode)

proptest! {
    #[test]
    fn a_manifest_round_trips_through_its_own_encoding(manifest in arb_manifest()) {
        let decoded = Manifest::decode(&manifest.encode()).expect("a built manifest decodes");
        prop_assert_eq!(decoded, manifest);
    }

    #[test]
    fn decoding_arbitrary_bytes_as_a_manifest_never_panics(
        bytes in decoder_input(manifest_corpus())
    ) {
        let _ = Manifest::decode(&bytes);
    }
}
