//! Scan cursors.
//!
//! See the crate docs for why a cursor names a key rather than a position.
//! This module is only the encoding; the resumption rules live on the scan
//! path where the partition range is known.

use bytes::Bytes;
use orbita_core::{Error, Result};

const FORMAT_V1: u8 = 1;
const HEADER_LEN: usize = 1 + 8;

/// Where the next page of a scan resumes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Cursor {
    /// The last key the previous page returned. The next page starts strictly
    /// after it.
    pub after_key: Bytes,
    pub prefix_hash: u64,
}

impl Cursor {
    pub fn new(after_key: Bytes, prefix: &[u8]) -> Self {
        Self {
            after_key,
            prefix_hash: hash_prefix(prefix),
        }
    }

    pub fn encode(&self) -> Bytes {
        let mut out = Vec::with_capacity(HEADER_LEN + self.after_key.len());
        out.push(FORMAT_V1);
        out.extend_from_slice(&self.prefix_hash.to_be_bytes());
        out.extend_from_slice(&self.after_key);
        Bytes::from(out)
    }

    /// Decodes a cursor and checks it belongs to this scan.
    ///
    /// Replaying a cursor against a different prefix would return a page that
    /// looks plausible and is wrong, so it is rejected rather than tolerated.
    /// The hash is not a security boundary; it catches caller mistakes.
    pub fn decode(raw: &[u8], prefix: &[u8]) -> Result<Self> {
        if raw.len() < HEADER_LEN || raw[0] != FORMAT_V1 {
            return Err(Error::InvalidArgument("malformed scan cursor".to_string()));
        }
        let prefix_hash = u64::from_be_bytes(
            raw[1..HEADER_LEN]
                .try_into()
                .map_err(|_| Error::InvalidArgument("malformed scan cursor".to_string()))?,
        );
        if prefix_hash != hash_prefix(prefix) {
            return Err(Error::InvalidArgument(
                "scan cursor was issued for a different prefix".to_string(),
            ));
        }
        Ok(Self {
            after_key: Bytes::copy_from_slice(&raw[HEADER_LEN..]),
            prefix_hash,
        })
    }
}

/// FNV-1a, chosen because it is four lines and this is a typo check rather
/// than a defence against a motivated caller.
fn hash_prefix(prefix: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in prefix {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cursor_round_trips_through_its_encoding() {
        let cursor = Cursor::new(Bytes::from_static(b"users/42"), b"users/");
        let decoded = Cursor::decode(&cursor.encode(), b"users/").unwrap();
        assert_eq!(decoded, cursor);
    }

    #[test]
    fn an_empty_key_is_a_valid_resume_point() {
        let cursor = Cursor::new(Bytes::new(), b"");
        assert_eq!(Cursor::decode(&cursor.encode(), b"").unwrap(), cursor);
    }

    #[test]
    fn a_cursor_from_another_prefix_is_rejected() {
        let cursor = Cursor::new(Bytes::from_static(b"users/42"), b"users/");
        assert!(matches!(
            Cursor::decode(&cursor.encode(), b"locks/"),
            Err(Error::InvalidArgument(_))
        ));
    }

    #[test]
    fn a_garbled_cursor_is_rejected_rather_than_guessed_at() {
        assert!(Cursor::decode(b"", b"").is_err());
        assert!(
            Cursor::decode(&[0u8; 16], b"").is_err(),
            "wrong format byte"
        );
    }
}
