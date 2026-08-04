//! Finding the objects nobody will ever read.
//!
//! Two things make objects unreferenced. A commit drops one, as when compaction
//! replaces a segment. Or a commit writes one and never reaches its manifest
//! swap, which is what an interrupted or abandoned commit leaves behind. The
//! second is why the sweep is required rather than an optimisation: without it
//! a partition accumulates objects forever.
//!
//! # This module finds them; it does not delete them
//!
//! Deleting an unreferenced object is safe only after a grace period, and the
//! grace period must exceed both the longest read and the longest commit. A
//! reader part way through a snapshot taken against an earlier manifest will
//! request objects that manifest named, and steps 1 and 2 of a commit write
//! objects that nothing references until step 6, so a sweeper that considered
//! only read duration would delete objects an in-flight commit is about to
//! publish.
//!
//! Judging either clock needs to know when an object was written, and
//! [`ObjectMeta`](orbita_objectstore::ObjectMeta) does not carry that. Until it
//! does, this module answers the question it can answer from the objects
//! themselves, and the caller supplies the timing.

use crate::manifest::Manifest;
use crate::paths::{self, PartitionPath};
use crate::record::{RecordValue, SegmentRecord};

use orbita_objectstore::ObjectMeta;
use std::collections::BTreeSet;

/// Everything the current manifest reaches, by name relative to the partition
/// directory.
///
/// Segments come from the manifest itself. Value objects do not: they are
/// reached through the records inside live segments, so a caller has to walk
/// those segments before it knows which values are still referenced. Sweeping
/// values off the manifest alone would delete every large value in the
/// partition.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Referenced {
    names: BTreeSet<String>,
}

impl Referenced {
    #[must_use]
    pub fn of(manifest: &Manifest) -> Self {
        Self {
            names: manifest.segments.iter().map(|s| s.name.clone()).collect(),
        }
    }

    /// Adds the external values one live segment's records point at.
    pub fn add_records(&mut self, records: &[SegmentRecord]) {
        for record in records {
            if let RecordValue::External(external) = &record.value {
                self.names.insert(external.name.clone());
            }
        }
    }

    #[must_use]
    pub fn contains(&self, relative_name: &str) -> bool {
        self.names.contains(relative_name)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.names.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }
}

/// The objects in `listing` that `referenced` does not reach, as full store
/// keys.
///
/// Only objects this format writes are considered. Anything else under the
/// prefix belongs to somebody else and a sweeper has no business deleting it,
/// which is the reason object names are parsed strictly rather than by
/// stripping a suffix.
#[must_use]
pub fn unreferenced(
    referenced: &Referenced,
    path: &PartitionPath,
    listing: &[ObjectMeta],
) -> Vec<String> {
    listing
        .iter()
        .filter(|object| {
            path.relative(&object.key).is_some_and(|relative| {
                paths::parse_object_name(relative).is_some() && !referenced.contains(relative)
            })
        })
        .map(|object| object.key.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::SegmentEntry;
    use crate::record::ExternalValue;

    use bytes::Bytes;
    use orbita_core::{Epoch, KeyRange, KeyspaceId, Lamport, PartitionId};
    use orbita_objectstore::ETag;

    fn path() -> PartitionPath {
        PartitionPath::new("orbita", KeyspaceId(1), PartitionId(7))
    }

    fn object(relative: &str) -> ObjectMeta {
        ObjectMeta {
            key: path().object(relative),
            size: 1024,
            etag: ETag("etag".to_string()),
        }
    }

    fn manifest(segments: Vec<String>) -> Manifest {
        Manifest {
            keyspace_id: KeyspaceId(1),
            partition_id: PartitionId(7),
            epoch: Epoch(1),
            committed_lamport: Lamport(10),
            range: KeyRange::unbounded(),
            segments: segments
                .into_iter()
                .map(|name| SegmentEntry {
                    name,
                    bytes: 1024,
                    record_count: 1,
                    min_key: Bytes::from_static(b"a"),
                    max_key: Bytes::from_static(b"z"),
                    min_lamport: Lamport(1),
                    max_lamport: Lamport(10),
                })
                .collect(),
        }
    }

    #[test]
    fn a_segment_the_manifest_dropped_is_unreferenced() {
        let live = paths::segment_name(Epoch(1), 1);
        let dropped = paths::segment_name(Epoch(1), 0);
        let referenced = Referenced::of(&manifest(vec![live.clone()]));

        assert_eq!(
            unreferenced(&referenced, &path(), &[object(&live), object(&dropped)]),
            vec![path().object(&dropped)]
        );
    }

    #[test]
    fn a_value_reached_through_a_live_segment_is_not_swept() {
        let segment = paths::segment_name(Epoch(1), 0);
        let live_value = paths::value_name(Epoch(1), 1);
        let orphan_value = paths::value_name(Epoch(1), 2);

        let mut referenced = Referenced::of(&manifest(vec![segment.clone()]));
        referenced.add_records(&[SegmentRecord {
            key: Bytes::from_static(b"a"),
            lamport: Lamport(1),
            expires_at_millis: None,
            value: RecordValue::External(ExternalValue {
                name: live_value.clone(),
                length: 1,
                crc32c: 0,
            }),
        }]);

        assert_eq!(
            unreferenced(
                &referenced,
                &path(),
                &[object(&segment), object(&live_value), object(&orphan_value)]
            ),
            vec![path().object(&orphan_value)],
            "values are reached through records, not through the manifest"
        );
    }

    #[test]
    fn objects_this_format_did_not_write_are_left_alone() {
        let referenced = Referenced::of(&manifest(vec![]));
        let listing = [
            object("manifest.json"),
            object("segments/notes.txt"),
            ObjectMeta {
                key: "somewhere/else.oseg".to_string(),
                size: 1,
                etag: ETag("etag".to_string()),
            },
        ];
        assert!(unreferenced(&referenced, &path(), &listing).is_empty());
    }
}
