//! How a record is laid out on disk.
//!
//! The layout is fixed-width up front and variable-width only at the tail, so
//! the compaction filter can read a record's expiry without allocating and
//! without knowing anything about the value. That path runs over every record
//! in the partition on every compaction, which is the reason the encoding
//! looks like this rather than like a general-purpose serialization format.

use bytes::Bytes;
use orbita_core::{Error, Record, Result, Version};

/// The leading byte of every stored value.
///
/// It exists so that a future layout change can be rolled out without a
/// migration pass: an old record still decodes, and a new one is written the
/// new way.
const FORMAT_V1: u8 = 1;

const FLAG_DELETED: u8 = 1 << 0;
const FLAG_HAS_EXPIRY: u8 = 1 << 1;

/// Format byte, flags byte, and the version.
const HEADER_LEN: usize = 1 + 1 + 8;
const EXPIRY_LEN: usize = 8;

/// One value as it sits in RocksDB.
///
/// This is distinct from [`Record`] because a tombstone is a stored entry with
/// no visible record, and because the read path has to reason about entries
/// that exist but are not visible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Stored {
    pub version: Version,
    pub expires_at_millis: Option<u64>,
    /// True for a tombstone, meaning the key was deleted at `version`.
    pub deleted: bool,
    pub value: Bytes,
}

impl Stored {
    /// Whether the entry is past its deadline, which for a live record means
    /// it is invisible and for a tombstone means it is reclaimable.
    pub fn is_expired_at(&self, now_millis: u64) -> bool {
        self.expires_at_millis.is_some_and(|e| now_millis >= e)
    }

    /// The record a reader should see, or `None` if the key is absent as far
    /// as the API is concerned.
    pub fn visible_at(&self, now_millis: u64) -> Option<Record> {
        if self.deleted || self.is_expired_at(now_millis) {
            return None;
        }
        Some(Record {
            value: self.value.clone(),
            version: self.version,
            expires_at_millis: self.expires_at_millis,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut flags = 0u8;
        if self.deleted {
            flags |= FLAG_DELETED;
        }
        if self.expires_at_millis.is_some() {
            flags |= FLAG_HAS_EXPIRY;
        }

        let mut out = Vec::with_capacity(HEADER_LEN + EXPIRY_LEN + self.value.len());
        out.push(FORMAT_V1);
        out.push(flags);
        out.extend_from_slice(&self.version.get().to_be_bytes());
        if let Some(expiry) = self.expires_at_millis {
            out.extend_from_slice(&expiry.to_be_bytes());
        }
        out.extend_from_slice(&self.value);
        out
    }

    pub fn decode(raw: &[u8]) -> Result<Self> {
        let (version, expires_at_millis, deleted, body) = decode_header(raw)
            .ok_or_else(|| Error::Internal("corrupt record in partition".to_string()))?;
        Ok(Self {
            version,
            expires_at_millis,
            deleted,
            value: Bytes::copy_from_slice(body),
        })
    }
}

/// Reads only the parts of a record the compaction filter needs.
///
/// This is separate from [`Stored::decode`] so that reclamation never copies a
/// value it is about to throw away.
pub(crate) fn expiry_of(raw: &[u8]) -> Option<Option<u64>> {
    decode_header(raw).map(|(_, expiry, _, _)| expiry)
}

type Header<'a> = (Version, Option<u64>, bool, &'a [u8]);

fn decode_header(raw: &[u8]) -> Option<Header<'_>> {
    if raw.len() < HEADER_LEN || raw[0] != FORMAT_V1 {
        return None;
    }
    let flags = raw[1];
    let version = Version(u64::from_be_bytes(raw[2..10].try_into().ok()?));

    let mut offset = HEADER_LEN;
    let expires_at_millis = if flags & FLAG_HAS_EXPIRY != 0 {
        if raw.len() < HEADER_LEN + EXPIRY_LEN {
            return None;
        }
        let expiry = u64::from_be_bytes(raw[offset..offset + EXPIRY_LEN].try_into().ok()?);
        offset += EXPIRY_LEN;
        Some(expiry)
    } else {
        None
    };

    Some((
        version,
        expires_at_millis,
        flags & FLAG_DELETED != 0,
        &raw[offset..],
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored(deleted: bool, expiry: Option<u64>) -> Stored {
        Stored {
            version: Version(9),
            expires_at_millis: expiry,
            deleted,
            value: Bytes::from_static(b"payload"),
        }
    }

    #[test]
    fn a_record_round_trips_through_its_encoding() {
        for entry in [
            stored(false, None),
            stored(false, Some(1234)),
            stored(true, Some(u64::MAX)),
        ] {
            assert_eq!(Stored::decode(&entry.encode()).unwrap(), entry);
        }
    }

    #[test]
    fn an_empty_value_is_distinct_from_a_tombstone() {
        let empty = Stored {
            value: Bytes::new(),
            ..stored(false, None)
        };
        let decoded = Stored::decode(&empty.encode()).unwrap();
        assert!(!decoded.deleted);
        assert!(decoded.visible_at(0).is_some(), "an empty value is a value");
    }

    #[test]
    fn a_tombstone_is_never_visible() {
        assert!(stored(true, Some(u64::MAX)).visible_at(0).is_none());
    }

    #[test]
    fn expiry_is_inclusive_of_the_deadline() {
        let entry = stored(false, Some(100));
        assert!(entry.visible_at(99).is_some());
        assert!(
            entry.visible_at(100).is_none(),
            "a key is gone at its expiry instant"
        );
    }

    #[test]
    fn expiry_is_readable_without_decoding_the_value() {
        let encoded = stored(false, Some(77)).encode();
        assert_eq!(expiry_of(&encoded), Some(Some(77)));
        assert_eq!(expiry_of(&stored(false, None).encode()), Some(None));
    }

    #[test]
    fn truncated_and_unknown_records_fail_to_decode() {
        assert!(Stored::decode(&[]).is_err(), "empty");
        assert!(Stored::decode(&[0; 32]).is_err(), "unknown format byte");

        let mut short = stored(false, Some(5)).encode();
        short.truncate(HEADER_LEN + 2);
        assert!(Stored::decode(&short).is_err(), "truncated expiry");
        assert_eq!(expiry_of(&short), None);
    }
}
