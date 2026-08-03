//! Segments: the immutable, sorted objects a partition's data actually lives
//! in.
//!
//! ```text
//! +--------------------+
//! | header    32 bytes |
//! +--------------------+
//! | data section       |   records, ascending by key
//! +--------------------+
//! | key index section  |   one entry per record
//! +--------------------+
//! | footer    64 bytes |
//! +--------------------+
//! ```
//!
//! The footer is last and fixed in size so a reader can fetch it with one
//! range request against the tail of the object. From the footer it finds the
//! index, and from the index it finds any single record: three requests to
//! read one key out of a segment it has never seen, and one per key after
//! that.
//!
//! The index carries keys and offsets but no values, which is what lets a node
//! rebuild its in-memory index, or a tool list a keyspace, without reading a
//! single value.

use crate::error::{FormatError, Result};
use crate::record::{Cursor, SegmentRecord};

use bytes::Bytes;
use orbita_core::{Epoch, KeyspaceId, Lamport, PartitionId};

pub const FORMAT_VERSION: u16 = 1;
pub const HEADER_LEN: u64 = 32;
pub const FOOTER_LEN: u64 = 64;

const HEADER_MAGIC: &[u8; 6] = b"ORBSEG";
const FOOTER_MAGIC: &[u8; 6] = b"ORBEND";
/// The part of the footer its own checksum covers.
const FOOTER_CHECKSUMMED: usize = 52;

/// What a segment says about itself.
///
/// The identifiers repeat what the object's name already carries, which is
/// deliberate: an object copied out of its path, which is what happens the
/// moment somebody investigates an incident, still describes itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentHeader {
    pub keyspace_id: KeyspaceId,
    pub partition_id: PartitionId,
    /// The writer's epoch, truncated to its low 32 bits. Diagnostics only; the
    /// object's name carries the whole thing.
    pub epoch_low: u32,
}

impl SegmentHeader {
    fn encode(&self) -> [u8; HEADER_LEN as usize] {
        let mut out = [0u8; HEADER_LEN as usize];
        out[..6].copy_from_slice(HEADER_MAGIC);
        out[6..8].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        // flags at 8..10 and reserved at 10..12 are zero.
        out[12..20].copy_from_slice(&self.keyspace_id.get().to_le_bytes());
        out[20..28].copy_from_slice(&self.partition_id.get().to_le_bytes());
        out[28..32].copy_from_slice(&self.epoch_low.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let bytes = bytes
            .get(..HEADER_LEN as usize)
            .ok_or(FormatError::Truncated {
                what: "segment header",
                needed: HEADER_LEN,
                found: bytes.len() as u64,
            })?;
        if &bytes[..6] != HEADER_MAGIC {
            return Err(FormatError::BadMagic {
                what: "segment header",
            });
        }
        check_version(u16::from_le_bytes(
            bytes[6..8].try_into().expect("two bytes"),
        ))?;

        let flags = u16::from_le_bytes(bytes[8..10].try_into().expect("two bytes"));
        let reserved = u16::from_le_bytes(bytes[10..12].try_into().expect("two bytes"));
        if flags != 0 || reserved != 0 {
            return Err(FormatError::Malformed {
                what: "segment header",
                detail: format!("flags {flags:#06x} and reserved {reserved:#06x} must be zero"),
            });
        }

        Ok(Self {
            keyspace_id: KeyspaceId(u64::from_le_bytes(
                bytes[12..20].try_into().expect("eight bytes"),
            )),
            partition_id: PartitionId(u64::from_le_bytes(
                bytes[20..28].try_into().expect("eight bytes"),
            )),
            epoch_low: u32::from_le_bytes(bytes[28..32].try_into().expect("four bytes")),
        })
    }
}

/// The fixed-size tail that lets a reader find its way in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentFooter {
    pub index_offset: u64,
    pub index_length: u64,
    pub record_count: u64,
    pub min_lamport: Lamport,
    pub max_lamport: Lamport,
    pub data_crc32c: u32,
    pub index_crc32c: u32,
}

impl SegmentFooter {
    fn encode(&self) -> [u8; FOOTER_LEN as usize] {
        let mut out = [0u8; FOOTER_LEN as usize];
        out[0..8].copy_from_slice(&self.index_offset.to_le_bytes());
        out[8..16].copy_from_slice(&self.index_length.to_le_bytes());
        out[16..24].copy_from_slice(&self.record_count.to_le_bytes());
        out[24..32].copy_from_slice(&self.min_lamport.get().to_le_bytes());
        out[32..40].copy_from_slice(&self.max_lamport.get().to_le_bytes());
        out[40..44].copy_from_slice(&self.data_crc32c.to_le_bytes());
        out[44..48].copy_from_slice(&self.index_crc32c.to_le_bytes());
        // 48..52 is reserved and stays zero.
        let checksum = crc32c::crc32c(&out[..FOOTER_CHECKSUMMED]);
        out[52..56].copy_from_slice(&checksum.to_le_bytes());
        out[56..58].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        out[58..64].copy_from_slice(FOOTER_MAGIC);
        out
    }

    /// Decodes the last [`FOOTER_LEN`] bytes of a segment.
    ///
    /// The magic and the version are checked before any offset in the footer is
    /// interpreted, which is why the version appears here as well as in the
    /// header: a reader that has fetched only the tail can reject a format it
    /// does not understand without acting on numbers it cannot read.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != FOOTER_LEN as usize {
            return Err(FormatError::Truncated {
                what: "segment footer",
                needed: FOOTER_LEN,
                found: bytes.len() as u64,
            });
        }
        if &bytes[58..64] != FOOTER_MAGIC {
            return Err(FormatError::BadMagic {
                what: "segment footer",
            });
        }
        check_version(u16::from_le_bytes(
            bytes[56..58].try_into().expect("two bytes"),
        ))?;

        let stored = u32::from_le_bytes(bytes[52..56].try_into().expect("four bytes"));
        let computed = crc32c::crc32c(&bytes[..FOOTER_CHECKSUMMED]);
        if stored != computed {
            return Err(FormatError::ChecksumMismatch {
                what: "segment footer",
                stored,
                computed,
            });
        }

        let reserved = u32::from_le_bytes(bytes[48..52].try_into().expect("four bytes"));
        if reserved != 0 {
            return Err(FormatError::Malformed {
                what: "segment footer",
                detail: format!("reserved word {reserved:#010x} must be zero"),
            });
        }

        let u64_at = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().expect("eight"));
        let footer = Self {
            index_offset: u64_at(0),
            index_length: u64_at(8),
            record_count: u64_at(16),
            min_lamport: Lamport(u64_at(24)),
            max_lamport: Lamport(u64_at(32)),
            data_crc32c: u32::from_le_bytes(bytes[40..44].try_into().expect("four bytes")),
            index_crc32c: u32::from_le_bytes(bytes[44..48].try_into().expect("four bytes")),
        };
        if footer.min_lamport > footer.max_lamport {
            return Err(FormatError::Malformed {
                what: "segment footer",
                detail: format!(
                    "min_lamport {} is above max_lamport {}",
                    footer.min_lamport, footer.max_lamport
                ),
            });
        }
        Ok(footer)
    }

    /// Where the index section sits, given the object's size.
    pub fn index_range(&self, object_bytes: u64) -> Result<std::ops::Range<u64>> {
        let end = self
            .index_offset
            .checked_add(self.index_length)
            .ok_or_else(|| FormatError::Malformed {
                what: "segment footer",
                detail: "the index runs past the end of the address space".to_string(),
            })?;
        if self.index_offset < HEADER_LEN || end + FOOTER_LEN > object_bytes {
            return Err(FormatError::Malformed {
                what: "segment footer",
                detail: format!(
                    "an index at {}..{end} does not fit in a {object_bytes} byte object",
                    self.index_offset
                ),
            });
        }
        Ok(self.index_offset..end)
    }
}

/// Where one record sits, and under which key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexEntry {
    pub key: Bytes,
    /// From the start of the object, not from the start of the data section.
    pub offset: u64,
    /// The whole record, including its checksum and length prefix.
    pub record_length: u32,
}

impl IndexEntry {
    fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&(self.key.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.key);
        out.extend_from_slice(&self.offset.to_le_bytes());
        out.extend_from_slice(&self.record_length.to_le_bytes());
    }

    /// The range to range-request in order to read this record.
    #[must_use]
    pub fn range(&self) -> std::ops::Range<u64> {
        self.offset..self.offset + u64::from(self.record_length)
    }
}

/// A segment's key index, in the same order as the data section.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SegmentIndex {
    entries: Vec<IndexEntry>,
}

impl SegmentIndex {
    /// Decodes the index section, checking it against the footer first.
    pub fn decode(bytes: &[u8], footer: &SegmentFooter) -> Result<Self> {
        let computed = crc32c::crc32c(bytes);
        if computed != footer.index_crc32c {
            return Err(FormatError::ChecksumMismatch {
                what: "segment key index",
                stored: footer.index_crc32c,
                computed,
            });
        }

        let mut cursor = Cursor::new(bytes);
        let mut entries: Vec<IndexEntry> = Vec::new();
        while !cursor.is_empty() {
            let key = cursor.bytes32().map_err(|_| FormatError::Malformed {
                what: "segment key index",
                detail: "an entry runs past the end of the section".to_string(),
            })?;
            let entry = IndexEntry {
                key,
                offset: cursor.u64()?,
                record_length: cursor.u32()?,
            };
            if let Some(previous) = entries.last() {
                if entry.key <= previous.key {
                    return Err(FormatError::Malformed {
                        what: "segment key index",
                        detail: "keys are not strictly ascending".to_string(),
                    });
                }
            }
            entries.push(entry);
        }

        if entries.len() as u64 != footer.record_count {
            return Err(FormatError::Malformed {
                what: "segment key index",
                detail: format!(
                    "{} entries for {} records",
                    entries.len(),
                    footer.record_count
                ),
            });
        }
        Ok(Self { entries })
    }

    #[must_use]
    pub fn entries(&self) -> &[IndexEntry] {
        &self.entries
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Finds a key. Keys ascend, so this is a binary search rather than a scan.
    #[must_use]
    pub fn lookup(&self, key: &[u8]) -> Option<&IndexEntry> {
        self.entries
            .binary_search_by(|entry| entry.key.as_ref().cmp(key))
            .ok()
            .map(|at| &self.entries[at])
    }
}

/// A segment that has been built but not yet written anywhere.
///
/// It carries what the manifest entry needs, so that publishing a segment
/// never means computing those numbers a second time from a different source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuiltSegment {
    pub bytes: Bytes,
    pub record_count: u64,
    pub min_key: Bytes,
    pub max_key: Bytes,
    pub min_lamport: Lamport,
    pub max_lamport: Lamport,
}

/// Writes a segment, one record at a time, in key order.
pub struct SegmentBuilder {
    body: Vec<u8>,
    index: Vec<IndexEntry>,
    min_key: Option<Bytes>,
    max_key: Bytes,
    min_lamport: Lamport,
    max_lamport: Lamport,
}

impl SegmentBuilder {
    #[must_use]
    pub fn new(keyspace_id: KeyspaceId, partition_id: PartitionId, epoch: Epoch) -> Self {
        let header = SegmentHeader {
            keyspace_id,
            partition_id,
            epoch_low: epoch.get() as u32,
        };
        Self {
            body: header.encode().to_vec(),
            index: Vec::new(),
            min_key: None,
            max_key: Bytes::new(),
            min_lamport: Lamport(u64::MAX),
            max_lamport: Lamport::ZERO,
        }
    }

    /// Appends a record.
    ///
    /// Keys must ascend and no key may appear twice. Producing a segment always
    /// means writing out a sorted map or merging sorted runs, so this costs a
    /// writer nothing and it lets a reader stop looking once it has found a key.
    pub fn push(&mut self, record: &SegmentRecord) -> Result<()> {
        if let Some(previous) = self.index.last() {
            if record.key <= previous.key {
                return Err(FormatError::Malformed {
                    what: "segment",
                    detail: format!(
                        "key {:?} does not sort after {:?}",
                        record.key, previous.key
                    ),
                });
            }
        }

        let offset = self.body.len() as u64;
        let encoded = record.encode();
        let record_length = u32::try_from(encoded.len()).map_err(|_| FormatError::Malformed {
            what: "segment",
            detail: "a record longer than 4 GiB cannot be indexed".to_string(),
        })?;
        self.body.extend_from_slice(&encoded);

        self.index.push(IndexEntry {
            key: record.key.clone(),
            offset,
            record_length,
        });
        self.min_key.get_or_insert_with(|| record.key.clone());
        self.max_key = record.key.clone();
        self.min_lamport = self.min_lamport.min(record.lamport);
        self.max_lamport = self.max_lamport.max(record.lamport);
        Ok(())
    }

    #[must_use]
    pub fn record_count(&self) -> u64 {
        self.index.len() as u64
    }

    /// Finishes the segment.
    ///
    /// An empty segment is refused rather than produced. Its manifest entry
    /// would have to claim key bounds it does not have, and a reader that
    /// pruned on those bounds would be pruning on fiction.
    pub fn finish(mut self) -> Result<BuiltSegment> {
        let Some(min_key) = self.min_key.clone() else {
            return Err(FormatError::Malformed {
                what: "segment",
                detail: "a segment with no records has no key bounds to publish".to_string(),
            });
        };

        let data_crc32c = crc32c::crc32c(&self.body[HEADER_LEN as usize..]);
        let index_offset = self.body.len() as u64;

        let mut index = Vec::new();
        for entry in &self.index {
            entry.encode(&mut index);
        }
        let index_crc32c = crc32c::crc32c(&index);
        let index_length = index.len() as u64;
        self.body.extend_from_slice(&index);

        let footer = SegmentFooter {
            index_offset,
            index_length,
            record_count: self.index.len() as u64,
            min_lamport: self.min_lamport,
            max_lamport: self.max_lamport,
            data_crc32c,
            index_crc32c,
        };
        self.body.extend_from_slice(&footer.encode());

        Ok(BuiltSegment {
            bytes: Bytes::from(self.body),
            record_count: footer.record_count,
            min_key,
            max_key: self.max_key,
            min_lamport: self.min_lamport,
            max_lamport: self.max_lamport,
        })
    }
}

/// A whole segment, held in memory.
///
/// This is the scrub path and the path a small segment takes. The reader that
/// matters for serving traffic fetches the footer and the index and leaves the
/// data section alone, which is [`crate::snapshot`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Segment {
    pub header: SegmentHeader,
    pub footer: SegmentFooter,
    pub index: SegmentIndex,
    records: Vec<SegmentRecord>,
}

impl Segment {
    /// Decodes and fully verifies a segment.
    ///
    /// Every checksum in the object is checked, including the two section
    /// checksums, which is what makes this the scrub. A reader that finds a
    /// mismatch fails rather than skipping: a segment is written once and
    /// completely, so damage to one is real damage.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let object_bytes = bytes.len() as u64;
        if object_bytes < HEADER_LEN + FOOTER_LEN {
            return Err(FormatError::Truncated {
                what: "segment",
                needed: HEADER_LEN + FOOTER_LEN,
                found: object_bytes,
            });
        }
        let header = SegmentHeader::decode(bytes)?;
        let footer = SegmentFooter::decode(&bytes[(object_bytes - FOOTER_LEN) as usize..])?;
        let index_range = footer.index_range(object_bytes)?;

        let data = &bytes[HEADER_LEN as usize..index_range.start as usize];
        let computed = crc32c::crc32c(data);
        if computed != footer.data_crc32c {
            return Err(FormatError::ChecksumMismatch {
                what: "segment data section",
                stored: footer.data_crc32c,
                computed,
            });
        }
        let index = SegmentIndex::decode(
            &bytes[index_range.start as usize..index_range.end as usize],
            &footer,
        )?;

        let mut records = Vec::with_capacity(index.len());
        let mut at = 0usize;
        while at < data.len() {
            let (record, consumed) = SegmentRecord::decode(&data[at..])?;
            // The index entry and the record it points at must agree about
            // where the record ends, because a reader that trusts the index
            // alone range-requests exactly that many bytes.
            match index.entries().get(records.len()) {
                Some(entry) if entry.record_length as usize == consumed => {}
                Some(entry) => {
                    return Err(FormatError::Malformed {
                        what: "segment key index",
                        detail: format!(
                            "entry claims a {} byte record where the data section holds {consumed}",
                            entry.record_length
                        ),
                    })
                }
                None => {
                    return Err(FormatError::Malformed {
                        what: "segment",
                        detail: "the data section holds more records than the index".to_string(),
                    })
                }
            }
            records.push(record);
            at += consumed;
        }
        if records.len() != index.len() {
            return Err(FormatError::Malformed {
                what: "segment",
                detail: format!(
                    "{} records in the data section for {} index entries",
                    records.len(),
                    index.len()
                ),
            });
        }
        let mut offset = HEADER_LEN;
        for (record, entry) in records.iter().zip(index.entries()) {
            if record.key != entry.key || entry.offset != offset {
                return Err(FormatError::Malformed {
                    what: "segment key index",
                    detail: "an entry does not point at the record it names".to_string(),
                });
            }
            offset += u64::from(entry.record_length);
        }

        Ok(Self {
            header,
            footer,
            index,
            records,
        })
    }

    #[must_use]
    pub fn records(&self) -> &[SegmentRecord] {
        &self.records
    }
}

fn check_version(found: u16) -> Result<()> {
    if found == FORMAT_VERSION {
        Ok(())
    } else {
        Err(FormatError::UnsupportedVersion {
            found: u64::from(found),
            supported: u64::from(FORMAT_VERSION),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::RecordValue;

    fn record(key: &str, lamport: u64) -> SegmentRecord {
        SegmentRecord {
            key: Bytes::copy_from_slice(key.as_bytes()),
            lamport: Lamport(lamport),
            expires_at_millis: None,
            value: RecordValue::Inline(Bytes::copy_from_slice(key.as_bytes())),
        }
    }

    fn built() -> BuiltSegment {
        let mut builder = SegmentBuilder::new(KeyspaceId(1), PartitionId(7), Epoch(6));
        for (key, lamport) in [("a", 10), ("b", 30), ("c", 20)] {
            builder.push(&record(key, lamport)).expect("ascending");
        }
        builder.finish().expect("non-empty")
    }

    #[test]
    fn a_built_segment_decodes_to_what_went_in() {
        let built = built();
        let segment = Segment::decode(&built.bytes).expect("valid");

        assert_eq!(segment.header.keyspace_id, KeyspaceId(1));
        assert_eq!(segment.header.partition_id, PartitionId(7));
        assert_eq!(segment.header.epoch_low, 6);
        assert_eq!(segment.records().len(), 3);
        assert_eq!(segment.footer.record_count, 3);
        assert_eq!(segment.footer.min_lamport, Lamport(10));
        assert_eq!(segment.footer.max_lamport, Lamport(30));
        assert_eq!(built.min_key, Bytes::from_static(b"a"));
        assert_eq!(built.max_key, Bytes::from_static(b"c"));
    }

    #[test]
    fn the_index_locates_a_record_without_reading_the_data_section() {
        let built = built();
        let segment = Segment::decode(&built.bytes).expect("valid");
        let entry = segment.index.lookup(b"b").expect("present");

        let range = entry.range();
        let (record, _) =
            SegmentRecord::decode(&built.bytes[range.start as usize..range.end as usize])
                .expect("the range holds exactly one record");
        assert_eq!(record, self::record("b", 30));

        assert!(segment.index.lookup(b"zz").is_none());
    }

    #[test]
    fn a_footer_can_be_read_from_the_tail_alone() {
        let built = built();
        let tail = &built.bytes[built.bytes.len() - FOOTER_LEN as usize..];
        let footer = SegmentFooter::decode(tail).expect("valid");

        let index_range = footer.index_range(built.bytes.len() as u64).expect("fits");
        let index = SegmentIndex::decode(
            &built.bytes[index_range.start as usize..index_range.end as usize],
            &footer,
        )
        .expect("valid");
        assert_eq!(index.len(), 3, "three requests, no data section");
    }

    #[test]
    fn keys_must_ascend_and_may_not_repeat() {
        let mut builder = SegmentBuilder::new(KeyspaceId(1), PartitionId(7), Epoch(6));
        builder.push(&record("b", 1)).expect("first");
        assert!(builder.push(&record("a", 2)).is_err(), "out of order");
        assert!(builder.push(&record("b", 2)).is_err(), "repeated");
    }

    #[test]
    fn an_empty_segment_is_refused() {
        let builder = SegmentBuilder::new(KeyspaceId(1), PartitionId(7), Epoch(6));
        assert!(builder.finish().is_err());
    }

    #[test]
    fn a_flipped_bit_below_the_header_is_caught() {
        // Everything from the data section onwards is covered by a checksum.
        // The header's identifiers are not, deliberately: they repeat what the
        // object's name already says and exist so that an object copied out of
        // its path still describes itself. Its magic and version are checked
        // directly, which the tests below cover.
        let built = built();
        for byte in HEADER_LEN as usize..built.bytes.len() {
            let mut damaged = built.bytes.to_vec();
            damaged[byte] ^= 0x01;
            assert!(
                Segment::decode(&damaged).is_err(),
                "byte {byte} went undetected"
            );
        }
    }

    #[test]
    fn a_truncated_object_is_rejected_rather_than_read_short() {
        let built = built();
        for length in [0, 1, 32, built.bytes.len() - 1] {
            assert!(
                Segment::decode(&built.bytes[..length]).is_err(),
                "{length} bytes decoded"
            );
        }
    }

    #[test]
    fn an_unknown_version_is_rejected_from_the_tail_before_any_offset_is_used() {
        let built = built();
        let mut damaged = built.bytes.to_vec();
        let at = damaged.len() - 8;
        damaged[at..at + 2].copy_from_slice(&2u16.to_le_bytes());

        let tail = &damaged[damaged.len() - FOOTER_LEN as usize..];
        assert!(matches!(
            SegmentFooter::decode(tail),
            Err(FormatError::UnsupportedVersion { found: 2, .. })
        ));
    }

    #[test]
    fn an_index_that_disagrees_with_the_record_it_points_at_is_rejected() {
        let built = built();
        let segment = Segment::decode(&built.bytes).expect("valid");
        let footer = &segment.footer;

        // Rewrite the first entry's record_length, then repair the index
        // checksum so that the disagreement rather than the checksum is what
        // this test exercises.
        let mut damaged = built.bytes.to_vec();
        let index_start = footer.index_offset as usize;
        let first = index_start + 4 + segment.index.entries()[0].key.len() + 8;
        damaged[first..first + 4].copy_from_slice(&9999u32.to_le_bytes());

        let index_end = index_start + footer.index_length as usize;
        let repaired = crc32c::crc32c(&damaged[index_start..index_end]);
        let footer_at = damaged.len() - FOOTER_LEN as usize;
        damaged[footer_at + 44..footer_at + 48].copy_from_slice(&repaired.to_le_bytes());
        let checksum = crc32c::crc32c(&damaged[footer_at..footer_at + FOOTER_CHECKSUMMED]);
        damaged[footer_at + 52..footer_at + 56].copy_from_slice(&checksum.to_le_bytes());

        assert!(matches!(
            Segment::decode(&damaged),
            Err(FormatError::Malformed { .. })
        ));
    }
}
