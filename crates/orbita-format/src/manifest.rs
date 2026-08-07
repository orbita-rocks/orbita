//! The manifest: a partition's only mutable object, and its atomic pointer.
//!
//! It is JSON because it is read once per commit rather than once per key,
//! because it is the first thing a reader has to parse, and because needing a
//! binary parser to find the data would be a poor start for a format meant to
//! be read by other tools.
//!
//! Nothing outside the manifest is authoritative. An object that exists and is
//! not named here is not part of the partition, whether it is left over from an
//! interrupted commit or from a compaction whose cleanup has not run.

use crate::error::{FormatError, Result};
use crate::paths::SEGMENTS_DIR;
use crate::segment::{BuiltSegment, FORMAT_VERSION};

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use bytes::Bytes;
use orbita_core::{Epoch, KeyRange, KeyspaceId, Lamport, PartitionId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// One live segment, as the manifest describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentEntry {
    /// The object's name, relative to the partition directory it lives under
    /// (which is [`Self::source`]'s directory when the segment is shared, and
    /// this manifest's own otherwise).
    pub name: String,
    /// The partition whose directory the object physically lives under, when
    /// this is a shared reference produced by a split. `None` for a segment
    /// this partition wrote itself, which is resolved relative to its own
    /// directory exactly as before. See
    /// [ADR 0009](../../../docs/adr/0009-a-split-shares-the-parents-segments.md):
    /// a child references the parent's immutable segments in place rather than
    /// copying them.
    pub source: Option<PartitionId>,
    /// The object's exact size. A reader without suffix range requests uses
    /// this to compute the footer's absolute offset. Always the real object
    /// size, including for a shared reference, because the reader reads the
    /// whole object's footer.
    pub bytes: u64,
    /// The number of records in the object. For a shared reference this is the
    /// full object's count, not the subset in this child's range, because the
    /// reader validates it against the object's own footer before filtering.
    pub record_count: u64,
    /// The smallest and largest keys actually present, both inclusive. Not the
    /// range the segment was written for. For a shared reference these are
    /// clamped to the keys present within this partition's range, so pruning
    /// and range validation see only what this partition serves.
    pub min_key: Bytes,
    pub max_key: Bytes,
    pub min_lamport: Lamport,
    pub max_lamport: Lamport,
}

impl SegmentEntry {
    /// The entry for a segment that has just been built, under `name`. Written
    /// by this partition, so it carries no cross-partition source.
    #[must_use]
    pub fn of(name: String, built: &BuiltSegment) -> Self {
        Self {
            name,
            source: None,
            bytes: built.bytes.len() as u64,
            record_count: built.record_count,
            min_key: built.min_key.clone(),
            max_key: built.max_key.clone(),
            min_lamport: built.min_lamport,
            max_lamport: built.max_lamport,
        }
    }

    /// Whether this segment could hold `key`, judged on its key bounds alone.
    ///
    /// This is the pruning a reader does when it has not built a full index.
    /// Segments overlap, so a `true` here means "fetch and look", not "found".
    #[must_use]
    pub fn may_hold(&self, key: &[u8]) -> bool {
        key >= &self.min_key[..] && key <= &self.max_key[..]
    }

    /// The byte range holding the footer, for a reader that cannot make a
    /// suffix request.
    ///
    /// Saturating rather than panicking on an entry too small to hold a footer.
    /// A validated manifest cannot produce one, but these fields are public, and
    /// an empty range that the store refuses is a better answer than a panic in
    /// a caller that hand-built an entry.
    #[must_use]
    pub fn footer_range(&self) -> std::ops::Range<u64> {
        self.bytes.saturating_sub(crate::segment::FOOTER_LEN)..self.bytes
    }
}

/// A partition's manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    pub keyspace_id: KeyspaceId,
    pub partition_id: PartitionId,
    /// The ownership epoch of the writer that produced this manifest.
    pub epoch: Epoch,
    /// The flush horizon: every log entry at or below it is reflected in the
    /// listed segments, and nothing above it is.
    pub committed_lamport: Lamport,
    pub range: KeyRange,
    pub segments: Vec<SegmentEntry>,
}

impl Manifest {
    /// The manifest a partition starts life with, before its first flush.
    #[must_use]
    pub fn empty(
        keyspace_id: KeyspaceId,
        partition_id: PartitionId,
        epoch: Epoch,
        range: KeyRange,
    ) -> Self {
        Self {
            keyspace_id,
            partition_id,
            epoch,
            committed_lamport: Lamport::ZERO,
            range,
            segments: Vec::new(),
        }
    }

    /// Every segment that could hold `key`, newest first by Lamport.
    ///
    /// Overlap is normal, so this can return more than one. Ordering by
    /// `max_lamport` is a heuristic for looking in the likeliest place first
    /// and is not on its own an answer to which record wins; that is decided
    /// per record, by [`crate::snapshot`].
    #[must_use]
    pub fn candidates(&self, key: &[u8]) -> Vec<&SegmentEntry> {
        let mut found: Vec<&SegmentEntry> =
            self.segments.iter().filter(|s| s.may_hold(key)).collect();
        found.sort_by(|a, b| b.max_lamport.cmp(&a.max_lamport));
        found
    }

    #[must_use]
    pub fn total_bytes(&self) -> u64 {
        self.segments.iter().map(|s| s.bytes).sum()
    }

    #[must_use]
    pub fn record_count(&self) -> u64 {
        self.segments.iter().map(|s| s.record_count).sum()
    }

    /// Renders the manifest.
    ///
    /// Pretty-printed on purpose. This object is read once per commit and is
    /// the first thing a person looks at when a partition misbehaves, so the
    /// bytes saved by compacting it would be paid for in legibility.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut json = serde_json::to_vec_pretty(&Wire::from(self))
            .expect("a manifest is always serialisable");
        json.push(b'\n');
        Bytes::from(json)
    }

    /// Parses and validates a manifest.
    ///
    /// Validation is not optional politeness. A reader that accepts a manifest
    /// whose segment bounds sit outside the partition's range, or whose
    /// `committed_lamport` is below a segment's own, has accepted a description
    /// of a partition that cannot exist, and everything it does afterwards is
    /// guesswork.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let wire: Wire = serde_json::from_slice(bytes).map_err(|e| FormatError::Malformed {
            what: "manifest",
            detail: e.to_string(),
        })?;
        wire.validate()
    }
}

/// The JSON shape, kept separate from the validated type so that nothing
/// constructs a [`Manifest`] by deserialising into it directly.
///
/// Unknown fields are refused rather than skipped. The compatibility rules say
/// there is no extension mechanism and that a version number is where a change
/// is argued about, so a field this build does not know is a manifest from a
/// format this build does not implement, whatever its `format_version` claims.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Wire {
    format_version: u64,
    keyspace_id: u64,
    partition_id: u64,
    epoch: u64,
    committed_lamport: u64,
    range: WireRange,
    segments: Vec<WireSegment>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRange {
    /// Empty means unbounded below, and it is the only way to say that.
    start: String,
    /// `null` means unbounded above. An empty string is invalid rather than
    /// meaning unbounded.
    end: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireSegment {
    name: String,
    /// The source partition a shared segment lives under. Absent for a
    /// self-written segment, and skipped on serialization when absent, so a
    /// manifest with no shared segments is byte-identical to one written
    /// before this field existed. That is what keeps historical manifests
    /// replaying unchanged without moving the format version.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_partition_id: Option<u64>,
    bytes: u64,
    record_count: u64,
    min_key: String,
    max_key: String,
    min_lamport: u64,
    max_lamport: u64,
}

impl From<&Manifest> for Wire {
    fn from(m: &Manifest) -> Self {
        Self {
            format_version: u64::from(FORMAT_VERSION),
            keyspace_id: m.keyspace_id.get(),
            partition_id: m.partition_id.get(),
            epoch: m.epoch.get(),
            committed_lamport: m.committed_lamport.get(),
            range: WireRange {
                start: BASE64.encode(m.range.start()),
                end: m.range.end().map(|e| BASE64.encode(e)),
            },
            segments: m
                .segments
                .iter()
                .map(|s| WireSegment {
                    name: s.name.clone(),
                    source_partition_id: s.source.map(PartitionId::get),
                    bytes: s.bytes,
                    record_count: s.record_count,
                    min_key: BASE64.encode(&s.min_key),
                    max_key: BASE64.encode(&s.max_key),
                    min_lamport: s.min_lamport.get(),
                    max_lamport: s.max_lamport.get(),
                })
                .collect(),
        }
    }
}

impl Wire {
    fn validate(self) -> Result<Manifest> {
        if self.format_version != u64::from(FORMAT_VERSION) {
            return Err(FormatError::UnsupportedVersion {
                found: self.format_version,
                supported: u64::from(FORMAT_VERSION),
            });
        }

        let start = decode_bytes("range.start", &self.range.start)?;
        let end = match &self.range.end {
            None => None,
            Some(text) => {
                let end = decode_bytes("range.end", text)?;
                if end.is_empty() {
                    return Err(malformed(
                        "an empty range.end is invalid; unbounded above is written as null",
                    ));
                }
                Some(end)
            }
        };
        let range = KeyRange::new(start, end)
            .ok_or_else(|| malformed("range.end must be strictly above range.start"))?;

        let mut names = BTreeSet::new();
        let mut segments = Vec::with_capacity(self.segments.len());
        for segment in self.segments {
            // Identity is (source, name): one object is fully qualified by which
            // partition's directory it lives under plus its relative name, so a
            // shared reference and a self-written segment that happened to share
            // a relative name are still distinct objects.
            if !names.insert((segment.source_partition_id, segment.name.clone())) {
                return Err(malformed(&format!(
                    "segment {} is listed twice",
                    segment.name
                )));
            }
            // Not merely under segments/, but a name this format could have
            // produced. A manifest naming something else describes a partition
            // that cannot exist, and the sweep, which parses names strictly,
            // would not recognise the reference as one.
            if !segment.name.starts_with(&format!("{SEGMENTS_DIR}/"))
                || crate::paths::parse_object_name(&segment.name).is_none()
            {
                return Err(malformed(&format!(
                    "segment name {} is not a name this format produces",
                    segment.name
                )));
            }
            if segment.record_count == 0 {
                return Err(malformed(&format!(
                    "segment {} claims no records, which cannot be written",
                    segment.name
                )));
            }
            if segment.bytes < crate::segment::HEADER_LEN + crate::segment::FOOTER_LEN {
                return Err(malformed(&format!(
                    "segment {} is too small to hold a header and a footer",
                    segment.name
                )));
            }

            let min_key = decode_bytes("min_key", &segment.min_key)?;
            let max_key = decode_bytes("max_key", &segment.max_key)?;
            if min_key > max_key {
                return Err(malformed(&format!(
                    "segment {} has min_key above max_key",
                    segment.name
                )));
            }
            // Both bounds are inclusive and both name keys that are actually
            // present, so both must sit inside the partition's half-open range.
            if !range.contains(&min_key) || !range.contains(&max_key) {
                return Err(malformed(&format!(
                    "segment {} holds keys outside the partition's range",
                    segment.name
                )));
            }
            if segment.min_lamport > segment.max_lamport {
                return Err(malformed(&format!(
                    "segment {} has min_lamport above max_lamport",
                    segment.name
                )));
            }
            // The flush horizon says everything at or below it is in the
            // segments. A segment holding something above it contradicts that,
            // and a recovering node would replay a write it already has.
            if segment.max_lamport > self.committed_lamport {
                return Err(malformed(&format!(
                    "segment {} holds lamport {} above committed_lamport {}",
                    segment.name, segment.max_lamport, self.committed_lamport
                )));
            }

            // A shared reference must name another partition, never this one:
            // a segment under our own prefix is resolved relatively, and a
            // "source" pointing at ourselves would be a second, ambiguous way
            // to say the same thing.
            let source = match segment.source_partition_id {
                None => None,
                Some(id) if id == self.partition_id => {
                    return Err(malformed(&format!(
                        "segment {} names its own partition as its source",
                        segment.name
                    )))
                }
                Some(id) => Some(PartitionId(id)),
            };

            segments.push(SegmentEntry {
                name: segment.name,
                source,
                bytes: segment.bytes,
                record_count: segment.record_count,
                min_key,
                max_key,
                min_lamport: Lamport(segment.min_lamport),
                max_lamport: Lamport(segment.max_lamport),
            });
        }

        Ok(Manifest {
            keyspace_id: KeyspaceId(self.keyspace_id),
            partition_id: PartitionId(self.partition_id),
            epoch: Epoch(self.epoch),
            committed_lamport: Lamport(self.committed_lamport),
            range,
            segments,
        })
    }
}

fn decode_bytes(field: &str, text: &str) -> Result<Bytes> {
    BASE64
        .decode(text)
        .map(Bytes::from)
        .map_err(|e| malformed(&format!("{field} is not standard base64: {e}")))
}

fn malformed(detail: &str) -> FormatError {
    FormatError::Malformed {
        what: "manifest",
        detail: detail.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The worked example from the specification, which this crate is obliged
    /// to accept exactly as written.
    const SPEC_EXAMPLE: &str = r#"{
      "format_version": 1,
      "keyspace_id": 1,
      "partition_id": 7,
      "epoch": 6,
      "committed_lamport": 4000,
      "range": { "start": "", "end": "bQ==" },
      "segments": [
        {
          "name": "segments/0000000000000005-0000000000000011.oseg",
          "bytes": 1048576,
          "record_count": 1200,
          "min_key": "YQ==",
          "max_key": "bA==",
          "min_lamport": 1,
          "max_lamport": 2500
        },
        {
          "name": "segments/0000000000000006-0000000000000000.oseg",
          "bytes": 262144,
          "record_count": 300,
          "min_key": "Yw==",
          "max_key": "aw==",
          "min_lamport": 2501,
          "max_lamport": 4000
        }
      ]
    }"#;

    fn example() -> Manifest {
        Manifest::decode(SPEC_EXAMPLE.as_bytes()).expect("the specification's own example")
    }

    #[test]
    fn the_specifications_example_decodes_to_what_its_prose_claims() {
        let manifest = example();
        assert_eq!(manifest.keyspace_id, KeyspaceId(1));
        assert_eq!(manifest.partition_id, PartitionId(7));
        assert_eq!(manifest.epoch, Epoch(6));
        assert_eq!(manifest.committed_lamport, Lamport(4000));
        assert_eq!(manifest.range.start(), b"", "unbounded below");
        assert_eq!(manifest.range.end(), Some(&b"m"[..]));
        assert_eq!(manifest.segments[0].min_key, Bytes::from_static(b"a"));
        assert_eq!(manifest.segments[0].max_key, Bytes::from_static(b"l"));
        assert_eq!(manifest.segments[1].min_key, Bytes::from_static(b"c"));
        assert_eq!(manifest.segments[1].max_key, Bytes::from_static(b"k"));
    }

    #[test]
    fn a_manifest_round_trips_through_its_own_encoding() {
        let manifest = example();
        assert_eq!(Manifest::decode(&manifest.encode()).unwrap(), manifest);
    }

    #[test]
    fn an_unbounded_range_survives_the_round_trip_as_null() {
        let manifest = Manifest::empty(
            KeyspaceId(1),
            PartitionId(7),
            Epoch(1),
            KeyRange::unbounded(),
        );
        let json = String::from_utf8(manifest.encode().to_vec()).unwrap();
        assert!(json.contains("\"end\": null"), "{json}");
        assert_eq!(Manifest::decode(json.as_bytes()).unwrap(), manifest);
    }

    #[test]
    fn a_version_this_build_does_not_implement_is_rejected() {
        let json = SPEC_EXAMPLE.replace("\"format_version\": 1", "\"format_version\": 2");
        assert!(matches!(
            Manifest::decode(json.as_bytes()),
            Err(FormatError::UnsupportedVersion { found: 2, .. })
        ));
    }

    #[test]
    fn identifiers_above_two_to_the_fifty_third_survive() {
        // A parser that narrows to a double loses these, which is the failure
        // the specification calls out by name.
        let json = SPEC_EXAMPLE.replace(
            "\"committed_lamport\": 4000",
            "\"committed_lamport\": 9007199254740993",
        );
        assert_eq!(
            Manifest::decode(json.as_bytes()).unwrap().committed_lamport,
            Lamport(9_007_199_254_740_993)
        );
    }

    #[test]
    fn an_empty_end_is_invalid_rather_than_unbounded() {
        let json = SPEC_EXAMPLE.replace("\"end\": \"bQ==\"", "\"end\": \"\"");
        assert!(matches!(
            Manifest::decode(json.as_bytes()),
            Err(FormatError::Malformed { .. })
        ));
    }

    #[test]
    fn a_segment_holding_keys_outside_the_partition_is_rejected() {
        // "z" is above the partition's exclusive end of "m". A reader taught to
        // accept this would prune on bounds that cannot be true.
        let json = SPEC_EXAMPLE.replace("\"max_key\": \"bA==\"", "\"max_key\": \"eg==\"");
        assert!(matches!(
            Manifest::decode(json.as_bytes()),
            Err(FormatError::Malformed { .. })
        ));
    }

    #[test]
    fn a_segment_above_the_flush_horizon_is_rejected() {
        let json = SPEC_EXAMPLE.replace("\"max_lamport\": 4000", "\"max_lamport\": 4001");
        assert!(matches!(
            Manifest::decode(json.as_bytes()),
            Err(FormatError::Malformed { .. })
        ));
    }

    #[test]
    fn a_horizon_above_every_segment_is_allowed() {
        // Entries in the gap were flushed and left no record, which happens
        // when every write in it was superseded or expired.
        let json =
            SPEC_EXAMPLE.replace("\"committed_lamport\": 4000", "\"committed_lamport\": 9000");
        assert_eq!(
            Manifest::decode(json.as_bytes()).unwrap().committed_lamport,
            Lamport(9000)
        );
    }

    #[test]
    fn a_segment_name_this_format_could_not_have_produced_is_rejected() {
        for name in [
            "segments/notes.txt",
            "segments/5-11.oseg",
            "values/0000000000000005-0000000000000011.oval",
        ] {
            let json =
                SPEC_EXAMPLE.replace("segments/0000000000000005-0000000000000011.oseg", name);
            assert!(
                matches!(
                    Manifest::decode(json.as_bytes()),
                    Err(FormatError::Malformed { .. })
                ),
                "{name} was accepted"
            );
        }
    }

    #[test]
    fn an_unknown_field_is_rejected_rather_than_skipped() {
        // There is no extension mechanism, on purpose. A field this build does
        // not know came from a format it does not implement, and skipping it is
        // how a reader silently returns something other than what was stored.
        let json = SPEC_EXAMPLE.replace(
            "\"committed_lamport\": 4000",
            "\"committed_lamport\": 4000,\n      \"compression\": \"zstd\"",
        );
        assert!(matches!(
            Manifest::decode(json.as_bytes()),
            Err(FormatError::Malformed { .. })
        ));
    }

    #[test]
    fn a_self_written_manifest_does_not_emit_the_source_field() {
        // The additive-and-invisible property that keeps replay compatible: a
        // manifest with no shared segments must serialize exactly as it did
        // before the field existed, so historical bytes and freshly written
        // self-owned bytes are indistinguishable.
        let json = String::from_utf8(example().encode().to_vec()).unwrap();
        assert!(
            !json.contains("source_partition_id"),
            "a self-owned manifest must not carry the shared-segment field: {json}"
        );
    }

    #[test]
    fn a_shared_segment_reference_round_trips_and_names_its_source() {
        // A child's manifest referencing the parent's segment in place, per ADR
        // 0009. It carries the source partition, decodes back to it, and the
        // encoding is the only place the new field appears.
        let mut manifest = example();
        manifest.partition_id = PartitionId(20);
        manifest.segments[0].source = Some(PartitionId(7));
        let encoded = manifest.encode();
        assert!(String::from_utf8(encoded.to_vec())
            .unwrap()
            .contains("\"source_partition_id\": 7"));
        let decoded = Manifest::decode(&encoded).unwrap();
        assert_eq!(decoded, manifest);
        assert_eq!(decoded.segments[0].source, Some(PartitionId(7)));
        assert_eq!(
            decoded.segments[1].source, None,
            "the other stays self-owned"
        );
    }

    #[test]
    fn a_segment_that_names_its_own_partition_as_source_is_rejected() {
        // A segment under a partition's own directory is resolved relatively; a
        // source pointing back at that same partition is a second, ambiguous
        // way to say the same thing, and refused rather than tolerated.
        let json = SPEC_EXAMPLE.replace(
            "\"name\": \"segments/0000000000000005-0000000000000011.oseg\",",
            "\"name\": \"segments/0000000000000005-0000000000000011.oseg\",\n          \
             \"source_partition_id\": 7,",
        );
        assert!(matches!(
            Manifest::decode(json.as_bytes()),
            Err(FormatError::Malformed { .. })
        ));
    }

    #[test]
    fn an_old_manifest_without_the_source_field_decodes_unchanged() {
        // The replay guarantee stated as a test: the specification's own
        // example predates the field, and must decode to segments that are all
        // self-owned.
        let manifest = example();
        assert!(manifest.segments.iter().all(|s| s.source.is_none()));
    }

    #[test]
    fn a_duplicated_segment_name_is_rejected() {
        let json = SPEC_EXAMPLE.replace(
            "segments/0000000000000006-0000000000000000.oseg",
            "segments/0000000000000005-0000000000000011.oseg",
        );
        assert!(matches!(
            Manifest::decode(json.as_bytes()),
            Err(FormatError::Malformed { .. })
        ));
    }

    #[test]
    fn candidates_prune_on_key_bounds_and_keep_the_overlap() {
        let manifest = example();
        // "b" is inside the first segment's bounds only.
        assert_eq!(manifest.candidates(b"b").len(), 1);
        // "d" is inside both, which is the normal case rather than a defect.
        assert_eq!(manifest.candidates(b"d").len(), 2);
        assert_eq!(
            manifest.candidates(b"d")[0].max_lamport,
            Lamport(4000),
            "the likeliest place to look comes first"
        );
        assert!(manifest.candidates(b"zz").is_empty());
    }
}
