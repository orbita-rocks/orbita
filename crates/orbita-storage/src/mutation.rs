//! The description of a committed write that a replica replays.
//!
//! This type lives here rather than in `orbita-wal` so that the storage engine
//! has no dependency on the log format. The WAL decodes an entry and hands the
//! partition a `Mutation`; the partition neither knows nor cares what the
//! entry looked like on the wire. See the crate docs.
//!
//! There is no version field. Under ADR 0002 a key's version is the Lamport at
//! which it was last written, so the Lamport the entry already carries is the
//! version. Storing both would put two numbers where one will do and invite
//! every future reader to work out which one is authoritative.
//!
//! The absolute expiry, by contrast, has to be carried. The owner resolved it
//! against its own clock when it accepted the write, and a replica that
//! recomputed it would extend the key's life by however far behind it is.

use bytes::Bytes;
use orbita_core::{Lamport, Version};

/// The version a write at `lamport` produces.
///
/// This is the whole of ADR 0002 in one line, and it is a function rather than
/// an inline cast so that the places relying on the equivalence are findable.
#[must_use]
pub(crate) fn version_at(lamport: Lamport) -> Version {
    Version(lamport.get())
}

/// One committed write, in the form a replica can replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mutation {
    /// The partition's Lamport for this write. It orders mutations, it is the
    /// key's new version, and it is what makes replay idempotent, since a
    /// partition ignores anything at or below what it has already committed.
    pub lamport: Lamport,
    pub key: Bytes,
    pub op: MutationOp,
}

/// What a mutation does to its key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MutationOp {
    Put {
        value: Bytes,
        /// Absolute expiry in Unix milliseconds, already resolved by the
        /// owner so that replication lag cannot extend a key's life.
        expires_at_millis: Option<u64>,
    },
    Delete {
        /// When the tombstone itself becomes reclaimable.
        tombstone_expires_at_millis: u64,
    },
}

impl Mutation {
    #[must_use]
    pub fn put(lamport: Lamport, key: Bytes, value: Bytes, expires_at_millis: Option<u64>) -> Self {
        Self {
            lamport,
            key,
            op: MutationOp::Put {
                value,
                expires_at_millis,
            },
        }
    }

    #[must_use]
    pub fn delete(lamport: Lamport, key: Bytes, tombstone_expires_at_millis: u64) -> Self {
        Self {
            lamport,
            key,
            op: MutationOp::Delete {
                tombstone_expires_at_millis,
            },
        }
    }

    /// The version this mutation leaves the key at, which is what the owner
    /// reports to the client once the write is acknowledged.
    #[must_use]
    pub fn version(&self) -> Version {
        version_at(self.lamport)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_mutations_version_is_its_lamport() {
        let m = Mutation::put(Lamport(9_001), Bytes::from_static(b"k"), Bytes::new(), None);
        assert_eq!(m.version(), Version(9_001));
    }
}
