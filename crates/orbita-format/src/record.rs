//! One record in a segment's data section.
//!
//! A record is the whole story of one key at one Lamport: whether it exists,
//! when it stops existing, and where its bytes are. Deletes are records rather
//! than absences because a conditional write has to tell a key that never
//! existed apart from one deleted at a known version.

use crate::error::{FormatError, Result};

use bytes::Bytes;
use orbita_core::{Lamport, Record, Version};

/// The `flags` byte, counting from the least significant bit.
pub mod flags {
    /// The key is deleted and there is no value.
    pub const TOMBSTONE: u8 = 1 << 0;
    /// The record carries `expires_at_millis`.
    pub const EXPIRY: u8 = 1 << 1;
    /// The value is stored in its own object.
    pub const EXTERNAL: u8 = 1 << 2;
    /// Reserved for the transaction work: marks a record as an uncommitted
    /// intent. In partition-v1 it must be zero, and a reader treats it exactly
    /// as it treats the generic reserved bits. It is named so the bit exists
    /// before the format's first release freezes the bytes.
    pub const INTENT: u8 = 1 << 3;
    /// Everything a writer sets to zero and a reader rejects, which includes
    /// [`INTENT`] until a future version gives it meaning.
    pub const RESERVED: u8 = !(TOMBSTONE | EXPIRY | EXTERNAL);
}

/// A value that lives in its own object.
///
/// The length and checksum live here rather than in the object, so the object
/// stays exactly the bytes a caller stored and can be handed to something that
/// knows nothing about this format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalValue {
    /// The object's name, relative to the partition directory.
    pub name: String,
    /// The object's size, which a reader checks before trusting it.
    pub length: u64,
    pub crc32c: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordValue {
    /// The key is deleted. A tombstone has no value, which is why the illegal
    /// combination of a tombstone with an external value cannot be built here.
    Tombstone,
    Inline(Bytes),
    External(ExternalValue),
}

/// One key at one Lamport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentRecord {
    pub key: Bytes,
    /// The Lamport this record was written at, which is also the key's version.
    pub lamport: Lamport,
    /// Absolute expiry in Unix milliseconds, UTC.
    pub expires_at_millis: Option<u64>,
    pub value: RecordValue,
}

impl SegmentRecord {
    #[must_use]
    pub fn is_tombstone(&self) -> bool {
        matches!(self.value, RecordValue::Tombstone)
    }

    /// Whether this record is expired at `now_millis`.
    ///
    /// The deadline is inclusive: a record is gone at the instant it names,
    /// which is what the specification says and what makes a TTL usable as a
    /// lease.
    #[must_use]
    pub fn is_expired_at(&self, now_millis: u64) -> bool {
        self.expires_at_millis.is_some_and(|e| e <= now_millis)
    }

    /// Whether a reader should report this key as present.
    ///
    /// Tombstones and expired records are absent keys, not present ones
    /// carrying a special value. A reader that gets this backwards reports
    /// deleted data as live.
    #[must_use]
    pub fn is_visible_at(&self, now_millis: u64) -> bool {
        !self.is_tombstone() && !self.is_expired_at(now_millis)
    }

    /// The record as the API's [`Record`], given the value bytes.
    ///
    /// The caller supplies the bytes because an external value costs a fetch,
    /// and whether to pay for it is not this type's decision.
    #[must_use]
    pub fn to_record(&self, value: Bytes) -> Record {
        Record {
            value,
            version: Version(self.lamport.get()),
            expires_at_millis: self.expires_at_millis,
        }
    }

    fn flags(&self) -> u8 {
        let mut bits = 0;
        if self.is_tombstone() {
            bits |= flags::TOMBSTONE;
        }
        if self.expires_at_millis.is_some() {
            bits |= flags::EXPIRY;
        }
        if matches!(self.value, RecordValue::External(_)) {
            bits |= flags::EXTERNAL;
        }
        bits
    }

    /// Appends the record's body, meaning everything the framing's `length`
    /// counts.
    pub(crate) fn encode_body(&self, out: &mut Vec<u8>) {
        out.push(self.flags());
        out.extend_from_slice(&self.lamport.get().to_le_bytes());
        // The reserved commit timestamp. Zero in partition-v1; the field
        // exists so the transaction work does not need a new format version
        // for its bytes.
        out.extend_from_slice(&0u64.to_le_bytes());
        out.extend_from_slice(&u32_len(self.key.len()).to_le_bytes());
        out.extend_from_slice(&self.key);
        if let Some(expiry) = self.expires_at_millis {
            out.extend_from_slice(&expiry.to_le_bytes());
        }
        match &self.value {
            RecordValue::Tombstone => {}
            RecordValue::Inline(value) => {
                out.extend_from_slice(&u32_len(value.len()).to_le_bytes());
                out.extend_from_slice(value);
            }
            RecordValue::External(external) => {
                out.extend_from_slice(&u32_len(external.name.len()).to_le_bytes());
                out.extend_from_slice(external.name.as_bytes());
                out.extend_from_slice(&external.length.to_le_bytes());
                out.extend_from_slice(&external.crc32c.to_le_bytes());
            }
        }
    }

    /// The whole record, framed with its checksum and length.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut framed = vec![0u8; 4];
        let mut body = Vec::new();
        self.encode_body(&mut body);
        framed.extend_from_slice(&u32_len(body.len()).to_le_bytes());
        framed.extend_from_slice(&body);
        let checksum = crc32c::crc32c(&framed[4..]);
        framed[..4].copy_from_slice(&checksum.to_le_bytes());
        framed
    }

    /// Decodes a framed record, verifying its checksum first.
    ///
    /// `bytes` may be exactly one record or the rest of the data section; the
    /// length in the framing decides where the record ends. That length is
    /// covered by the checksum, so a corrupted one cannot send this past what
    /// was verified.
    pub fn decode(bytes: &[u8]) -> Result<(SegmentRecord, usize)> {
        let header = bytes.get(..8).ok_or(FormatError::Truncated {
            what: "record framing",
            needed: 8,
            found: bytes.len() as u64,
        })?;
        let stored = u32::from_le_bytes(header[..4].try_into().expect("four bytes"));
        let length = u32::from_le_bytes(header[4..8].try_into().expect("four bytes")) as usize;

        let total = 8 + length;
        if bytes.len() < total {
            return Err(FormatError::Truncated {
                what: "record body",
                needed: total as u64,
                found: bytes.len() as u64,
            });
        }
        let computed = crc32c::crc32c(&bytes[4..total]);
        if computed != stored {
            return Err(FormatError::ChecksumMismatch {
                what: "record",
                stored,
                computed,
            });
        }

        Ok((decode_body(&bytes[8..total])?, total))
    }
}

/// Decodes a record body, which the framing has already verified.
///
/// Every field is checked against the body's own length rather than against
/// what the framing claimed, because the specification requires rejecting a
/// record whose `length` disagrees with the fields it contains rather than
/// trusting either one.
fn decode_body(body: &[u8]) -> Result<SegmentRecord> {
    let mut cursor = Cursor::new(body);
    let bits = cursor.u8()?;

    if bits & flags::RESERVED != 0 {
        return Err(FormatError::Malformed {
            what: "record",
            detail: format!("reserved flag bits set: {bits:#010b}"),
        });
    }
    let tombstone = bits & flags::TOMBSTONE != 0;
    let external = bits & flags::EXTERNAL != 0;
    if tombstone && external {
        return Err(FormatError::Malformed {
            what: "record",
            detail: "a tombstone has no value to store externally".to_string(),
        });
    }

    let lamport = Lamport(cursor.u64()?);
    let commit_timestamp = cursor.u64()?;
    if commit_timestamp != 0 {
        return Err(FormatError::Malformed {
            what: "record",
            detail: format!("the reserved commit timestamp must be zero, found {commit_timestamp}"),
        });
    }
    let key = cursor.bytes32()?;
    let expires_at_millis = if bits & flags::EXPIRY != 0 {
        Some(cursor.u64()?)
    } else {
        None
    };

    let value = if tombstone {
        RecordValue::Tombstone
    } else if external {
        let name = cursor.bytes32()?;
        let name = String::from_utf8(name.to_vec()).map_err(|_| FormatError::Malformed {
            what: "record",
            detail: "an external value's object name is not UTF-8".to_string(),
        })?;
        RecordValue::External(ExternalValue {
            name,
            length: cursor.u64()?,
            crc32c: cursor.u32()?,
        })
    } else {
        RecordValue::Inline(cursor.bytes32()?)
    };

    if !cursor.is_empty() {
        return Err(FormatError::Malformed {
            what: "record",
            detail: format!("{} trailing bytes after the value", cursor.remaining()),
        });
    }

    Ok(SegmentRecord {
        key,
        lamport,
        expires_at_millis,
        value,
    })
}

/// How far into a framed record the `lamport` sits: past the four-byte
/// checksum, the four-byte length, and the one-byte flags.
pub const LAMPORT_OFFSET: u64 = 9;

/// Reads a record's Lamport out of the first [`LAMPORT_OFFSET`] + 8 bytes.
///
/// This exists so that deciding which of two segments holds the newer record
/// for a key costs a few bytes rather than a whole record. It cannot verify
/// anything, because the checksum covers bytes this has not read, so the
/// answer is only ever used to choose a record that is then fetched and
/// verified in full.
pub fn peek_lamport(head: &[u8]) -> Result<Lamport> {
    let at = LAMPORT_OFFSET as usize;
    let bytes = head.get(at..at + 8).ok_or(FormatError::Truncated {
        what: "record head",
        needed: LAMPORT_OFFSET + 8,
        found: head.len() as u64,
    })?;
    Ok(Lamport(u64::from_le_bytes(
        bytes.try_into().expect("eight bytes"),
    )))
}

/// Reads fields out of a body, refusing to run past its end.
pub(crate) struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.at == self.bytes.len()
    }

    pub(crate) fn remaining(&self) -> usize {
        self.bytes.len() - self.at
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.at.checked_add(n).ok_or(FormatError::Malformed {
            what: "record",
            detail: "a length overflows the address space".to_string(),
        })?;
        let slice = self.bytes.get(self.at..end).ok_or(FormatError::Truncated {
            what: "record body",
            needed: end as u64,
            found: self.bytes.len() as u64,
        })?;
        self.at = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("four bytes"),
        ))
    }

    pub(crate) fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("eight bytes"),
        ))
    }

    /// A 32-bit length followed by that many bytes.
    pub(crate) fn bytes32(&mut self) -> Result<Bytes> {
        let length = self.u32()? as usize;
        Ok(Bytes::copy_from_slice(self.take(length)?))
    }
}

/// Every length in this format is 32-bit, so anything longer cannot be
/// represented whatever a cluster is configured to allow.
///
/// Callers validate against the operational limits long before this, and a
/// panic here would mean the format's own bound was breached, which is a bug
/// rather than bad input.
fn u32_len(len: usize) -> u32 {
    u32::try_from(len).expect("a field longer than 4 GiB cannot be represented in this format")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(k: &str) -> Bytes {
        Bytes::copy_from_slice(k.as_bytes())
    }

    fn inline() -> SegmentRecord {
        SegmentRecord {
            key: key("a"),
            lamport: Lamport(47),
            expires_at_millis: None,
            value: RecordValue::Inline(Bytes::from_static(b"value")),
        }
    }

    fn round_trip(record: &SegmentRecord) {
        let encoded = record.encode();
        let (decoded, consumed) = SegmentRecord::decode(&encoded).expect("decodes");
        assert_eq!(&decoded, record);
        assert_eq!(consumed, encoded.len(), "the whole record was consumed");
    }

    #[test]
    fn every_legal_shape_round_trips() {
        round_trip(&inline());
        round_trip(&SegmentRecord {
            expires_at_millis: Some(1_700_000_000_000),
            ..inline()
        });
        round_trip(&SegmentRecord {
            value: RecordValue::Tombstone,
            expires_at_millis: Some(1_700_000_000_000),
            ..inline()
        });
        round_trip(&SegmentRecord {
            value: RecordValue::External(ExternalValue {
                name: "values/0000000000000006-0000000000000000.oval".to_string(),
                length: 10 * 1024 * 1024,
                crc32c: 0xdead_beef,
            }),
            expires_at_millis: Some(1_700_000_000_000),
            ..inline()
        });
        round_trip(&SegmentRecord {
            key: Bytes::new(),
            value: RecordValue::Inline(Bytes::new()),
            ..inline()
        });
    }

    #[test]
    fn a_record_decodes_from_a_longer_buffer_and_reports_where_it_ended() {
        let mut buffer = inline().encode();
        let first = buffer.len();
        buffer.extend_from_slice(&inline().encode());

        let (_, consumed) = SegmentRecord::decode(&buffer).expect("decodes");
        assert_eq!(consumed, first, "the length in the framing ends the record");
    }

    #[test]
    fn a_flipped_bit_anywhere_in_a_record_is_caught() {
        let encoded = inline().encode();
        for byte in 4..encoded.len() {
            let mut damaged = encoded.clone();
            damaged[byte] ^= 0x01;
            let outcome = SegmentRecord::decode(&damaged);
            assert!(
                matches!(
                    outcome,
                    Err(FormatError::ChecksumMismatch { .. } | FormatError::Truncated { .. })
                ),
                "byte {byte} went undetected: {outcome:?}"
            );
        }
    }

    #[test]
    fn a_corrupted_length_cannot_send_a_reader_past_what_was_verified() {
        // The length is inside the checksum's coverage precisely so that this
        // is a checksum failure rather than a read of whatever follows. The
        // buffer is padded so that the corrupted length stays in bounds, which
        // is the case where a reader without that coverage would happily decode
        // whatever came after the record.
        let mut encoded = inline().encode();
        encoded.resize(1024, 0);
        encoded[4] = 0xff;
        assert!(matches!(
            SegmentRecord::decode(&encoded),
            Err(FormatError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn a_reserved_flag_bit_is_rejected_rather_than_ignored() {
        let mut body = Vec::new();
        inline().encode_body(&mut body);
        body[0] |= 0b1_0000;
        assert!(matches!(
            decode_body(&body),
            Err(FormatError::Malformed { .. })
        ));
    }

    #[test]
    fn the_reserved_intent_flag_is_rejected_in_partition_v1() {
        // The bit is named for the transaction work but carries no meaning
        // yet, so a reader treats it exactly as a generic reserved bit.
        let mut body = Vec::new();
        inline().encode_body(&mut body);
        body[0] |= flags::INTENT;
        assert!(matches!(
            decode_body(&body),
            Err(FormatError::Malformed { .. })
        ));
    }

    #[test]
    fn the_reserved_commit_timestamp_is_written_as_zero() {
        let mut body = Vec::new();
        inline().encode_body(&mut body);
        // flags (1) + lamport (8), then the reserved eight bytes.
        assert_eq!(&body[9..17], &[0u8; 8]);
    }

    #[test]
    fn a_nonzero_reserved_commit_timestamp_is_rejected() {
        let mut body = Vec::new();
        inline().encode_body(&mut body);
        body[9..17].copy_from_slice(&1u64.to_le_bytes());
        assert!(matches!(
            decode_body(&body),
            Err(FormatError::Malformed { .. })
        ));
    }

    #[test]
    fn a_tombstone_carrying_an_external_value_is_rejected() {
        let mut body = Vec::new();
        SegmentRecord {
            value: RecordValue::External(ExternalValue {
                name: "values/x.oval".to_string(),
                length: 1,
                crc32c: 0,
            }),
            ..inline()
        }
        .encode_body(&mut body);
        body[0] |= flags::TOMBSTONE;
        assert!(matches!(
            decode_body(&body),
            Err(FormatError::Malformed { .. })
        ));
    }

    #[test]
    fn a_length_that_disagrees_with_the_fields_is_rejected() {
        let mut body = Vec::new();
        inline().encode_body(&mut body);
        body.push(0);
        assert!(
            matches!(decode_body(&body), Err(FormatError::Malformed { .. })),
            "trailing bytes mean the length and the fields disagree"
        );

        body.truncate(body.len() - 3);
        assert!(matches!(
            decode_body(&body),
            Err(FormatError::Truncated { .. })
        ));
    }

    #[test]
    fn expiry_is_inclusive_and_a_tombstone_is_never_visible() {
        let record = SegmentRecord {
            expires_at_millis: Some(100),
            ..inline()
        };
        assert!(record.is_visible_at(99));
        assert!(!record.is_visible_at(100), "gone at the instant it names");

        let tombstone = SegmentRecord {
            value: RecordValue::Tombstone,
            expires_at_millis: Some(100),
            ..inline()
        };
        assert!(!tombstone.is_visible_at(0));
    }
}
