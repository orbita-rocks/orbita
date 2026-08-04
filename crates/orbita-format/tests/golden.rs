//! Golden test vectors: a tiny valid segment and manifest, checked in as
//! fixture bytes.
//!
//! Offset tables in prose are checked by whoever reads carefully. Fixture bytes
//! are checked by every implementation on every run, and they give a third
//! party something to conform to rather than something to interpret. The
//! specification asked for these once there was an implementation to produce
//! them; this is that.
//!
//! To regenerate after a deliberate format change:
//!
//! ```text
//! ORBITA_BLESS_GOLDEN=1 cargo test -p orbita-format --test golden
//! ```
//!
//! Regenerating is not a way to make a failure go away. Version 1's bytes never
//! change once it ships, so a diff here after that point means the change is a
//! new version number rather than an edit to this one.

use bytes::Bytes;
use orbita_core::{Epoch, KeyRange, KeyspaceId, Lamport, PartitionId};
use orbita_format::manifest::SegmentEntry;
use orbita_format::record::{ExternalValue, RecordValue};
use orbita_format::segment::{BuiltSegment, Segment, SegmentBuilder};
use orbita_format::{Manifest, PartitionPath, SegmentRecord};

use std::path::{Path, PathBuf};

const KEYSPACE: KeyspaceId = KeyspaceId(1);
const PARTITION: PartitionId = PartitionId(7);
const EPOCH: Epoch = Epoch(6);

const SEGMENT_FIXTURE: &str = "tests/golden/partition-v1.oseg";
const MANIFEST_FIXTURE: &str = "tests/golden/partition-v1.manifest.json";

fn key(k: &str) -> Bytes {
    Bytes::copy_from_slice(k.as_bytes())
}

/// One of every legal record shape, in key order.
///
/// A vector that only held plain inline values would let an implementation pass
/// while getting tombstones, expiry, or external values wrong, and those are the
/// three an external reader is most likely to get wrong.
fn records() -> Vec<SegmentRecord> {
    vec![
        SegmentRecord {
            key: key("alpha"),
            lamport: Lamport(1),
            expires_at_millis: None,
            value: RecordValue::Inline(key("first")),
        },
        SegmentRecord {
            key: key("bravo"),
            lamport: Lamport(2),
            expires_at_millis: Some(1_700_000_000_000),
            value: RecordValue::Inline(key("second")),
        },
        SegmentRecord {
            key: key("charlie"),
            lamport: Lamport(3),
            expires_at_millis: Some(1_700_086_400_000),
            value: RecordValue::Tombstone,
        },
        SegmentRecord {
            key: key("delta"),
            lamport: Lamport(4),
            expires_at_millis: None,
            value: RecordValue::External(ExternalValue {
                name: "values/0000000000000006-0000000000000001.oval".to_string(),
                length: b"a large value".len() as u64,
                crc32c: crc32c::crc32c(b"a large value"),
            }),
        },
    ]
}

fn segment() -> BuiltSegment {
    let mut builder = SegmentBuilder::new(KEYSPACE, PARTITION, EPOCH);
    for record in &records() {
        builder.push(record).expect("the fixture is in key order");
    }
    builder.finish().expect("the fixture is not empty")
}

fn manifest(segment: &BuiltSegment) -> Manifest {
    Manifest {
        keyspace_id: KEYSPACE,
        partition_id: PARTITION,
        epoch: EPOCH,
        committed_lamport: Lamport(9),
        range: KeyRange::new(Bytes::new(), Some(key("m"))).expect("a valid range"),
        segments: vec![SegmentEntry::of(
            "segments/0000000000000006-0000000000000000.oseg".to_string(),
            segment,
        )],
    }
}

fn fixture(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(relative)
}

/// Compares against the checked-in bytes, or rewrites them when blessed.
fn check(relative: &str, produced: &[u8]) {
    let path = fixture(relative);
    if std::env::var_os("ORBITA_BLESS_GOLDEN").is_some() {
        std::fs::write(&path, produced).expect("writing the fixture");
        return;
    }

    let expected = std::fs::read(&path).unwrap_or_else(|e| {
        panic!(
            "{relative} is missing ({e}); regenerate with ORBITA_BLESS_GOLDEN=1 if that is intended"
        )
    });
    assert_eq!(
        produced,
        expected.as_slice(),
        "{relative} no longer matches what this build produces. \
         Version 1's bytes do not change; a deliberate change is a new version."
    );
}

#[test]
fn the_segment_vector_is_unchanged() {
    check(SEGMENT_FIXTURE, &segment().bytes);
}

#[test]
fn the_manifest_vector_is_unchanged() {
    check(MANIFEST_FIXTURE, &manifest(&segment()).encode());
}

#[test]
fn the_checked_in_segment_decodes_to_the_records_it_was_built_from() {
    // The direction that matters for a third party: bytes on disk, read back
    // without the builder that produced them.
    check(SEGMENT_FIXTURE, &segment().bytes);
    let bytes = std::fs::read(fixture(SEGMENT_FIXTURE)).expect("the fixture");
    let segment = Segment::decode(&bytes).expect("the fixture is valid");

    assert_eq!(segment.header.keyspace_id, KEYSPACE);
    assert_eq!(segment.header.partition_id, PARTITION);
    assert_eq!(segment.records(), records());
    assert_eq!(segment.footer.record_count, 4);
    assert_eq!(segment.footer.min_lamport, Lamport(1));
    assert_eq!(segment.footer.max_lamport, Lamport(4));
    assert_eq!(segment.index.len(), 4);
    assert!(segment.index.lookup(b"charlie").is_some());
}

#[test]
fn the_checked_in_manifest_decodes_to_a_manifest_that_names_the_segment() {
    check(MANIFEST_FIXTURE, &manifest(&segment()).encode());
    let bytes = std::fs::read(fixture(MANIFEST_FIXTURE)).expect("the fixture");
    let manifest = Manifest::decode(&bytes).expect("the fixture is valid");
    let entry = &manifest.segments[0];

    assert_eq!(manifest.epoch, EPOCH);
    assert_eq!(manifest.committed_lamport, Lamport(9));
    assert_eq!(entry.min_key, key("alpha"));
    assert_eq!(entry.max_key, key("delta"));
    assert_eq!(
        entry.bytes,
        std::fs::read(fixture(SEGMENT_FIXTURE)).unwrap().len() as u64,
        "the manifest's size is what a reader uses to find the footer"
    );

    let path = PartitionPath::new("orbita", KEYSPACE, PARTITION);
    assert_eq!(
        path.object(&entry.name),
        "orbita/keyspaces/0000000000000001/partitions/0000000000000007/\
         segments/0000000000000006-0000000000000000.oseg"
    );
}
