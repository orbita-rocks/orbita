//! What a stored value is, and what conditions a write can carry.

use crate::ids::Version;
use bytes::Bytes;

/// A stored value with the metadata that travels with it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub value: Bytes,
    pub version: Version,
    /// Absolute expiry in Unix milliseconds, or `None` for no TTL.
    ///
    /// TTLs are stored as absolute instants rather than durations so that
    /// replication lag, WAL replay, and partition splits cannot quietly extend
    /// a key's life.
    pub expires_at_millis: Option<u64>,
}

impl Record {
    #[must_use]
    pub fn is_expired_at(&self, now_millis: u64) -> bool {
        self.expires_at_millis.is_some_and(|e| now_millis >= e)
    }
}

/// The precondition a write must satisfy to be applied.
///
/// These are what make Orbita usable for locks, leases, and catalog pointers,
/// so they are part of the core write path rather than an extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WriteCondition {
    /// Apply unconditionally.
    #[default]
    None,
    /// Apply only if the key does not currently exist. An expired key counts
    /// as absent.
    IfNotPresent,
    /// Apply only if the key exists at exactly this version.
    IfVersion(Version),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(expires: Option<u64>) -> Record {
        Record {
            value: Bytes::from_static(b"v"),
            version: Version(1),
            expires_at_millis: expires,
        }
    }

    #[test]
    fn expiry_is_inclusive_of_the_deadline() {
        let r = record(Some(100));
        assert!(!r.is_expired_at(99));
        assert!(r.is_expired_at(100), "a key is gone at its expiry instant");
        assert!(r.is_expired_at(101));
    }

    #[test]
    fn no_ttl_never_expires() {
        assert!(!record(None).is_expired_at(u64::MAX));
    }
}
