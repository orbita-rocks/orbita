//! Identifiers and logical counters.
//!
//! These are newtypes rather than bare integers because mixing up a partition
//! id and a node id is exactly the sort of bug that survives review and then
//! corrupts a partition map at 3am.

use std::fmt;

macro_rules! counter_newtype {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
        pub struct $name(pub u64);

        impl $name {
            pub const ZERO: Self = Self(0);

            /// Returns the next value in sequence.
            #[must_use]
            pub const fn next(self) -> Self {
                Self(self.0 + 1)
            }

            #[must_use]
            pub const fn get(self) -> u64 {
                self.0
            }
        }

        impl From<u64> for $name {
            fn from(v: u64) -> Self {
                Self(v)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

counter_newtype! {
    /// Identifies a node in the cluster, whether it runs as a leader group
    /// member or as a worker.
    NodeId
}

counter_newtype! {
    /// Identifies a partition, meaning one contiguous key range within one
    /// keyspace. Partition ids are never reused, including after a split or a
    /// merge, so a stale request naming an old partition is always detectable.
    PartitionId
}

counter_newtype! {
    /// Internal keyspace identifier. Stable across renames.
    KeyspaceId
}

counter_newtype! {
    /// The version of a single key's value. Every successful write to a key
    /// increments it, and conditional writes compare against it.
    Version
}

counter_newtype! {
    /// A partition's Lamport timestamp. The owner advances it on every commit,
    /// and replicas report how far they have applied so a read can tell
    /// whether a local replica is caught up enough to serve it.
    Lamport
}

counter_newtype! {
    /// Ownership epoch for a partition. The leader group bumps it on every
    /// ownership change, which is what fences a deposed owner: a write
    /// carrying a stale epoch is rejected by the replicas.
    Epoch
}

/// A user-facing keyspace name.
///
/// Names are restricted because they appear in object storage paths, metrics
/// labels, and credentials. Keeping the character set boring avoids a class of
/// escaping bugs later.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeyspaceName(String);

impl KeyspaceName {
    pub const MAX_LEN: usize = 64;

    /// Validates and wraps a keyspace name.
    ///
    /// A name must be 1 to 64 characters of ASCII lowercase alphanumerics,
    /// `-`, or `_`, and must start with a letter.
    pub fn new(name: impl Into<String>) -> Result<Self, InvalidKeyspaceName> {
        let name = name.into();
        if name.is_empty() || name.len() > Self::MAX_LEN {
            return Err(InvalidKeyspaceName(name));
        }
        let starts_ok = name.chars().next().is_some_and(|c| c.is_ascii_lowercase());
        let body_ok = name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_');
        if starts_ok && body_ok {
            Ok(Self(name))
        } else {
            Err(InvalidKeyspaceName(name))
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for KeyspaceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("invalid keyspace name: {0:?}")]
pub struct InvalidKeyspaceName(pub String);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_increment() {
        assert_eq!(Version::ZERO.next(), Version(1));
        assert_eq!(Epoch(7).next().get(), 8);
    }

    #[test]
    fn keyspace_names_are_validated() {
        assert!(KeyspaceName::new("catalog").is_ok());
        assert!(KeyspaceName::new("tenant-1_locks").is_ok());

        assert!(KeyspaceName::new("").is_err(), "empty");
        assert!(KeyspaceName::new("1leading").is_err(), "leading digit");
        assert!(KeyspaceName::new("Upper").is_err(), "uppercase");
        assert!(KeyspaceName::new("has space").is_err(), "space");
        assert!(KeyspaceName::new("a/b").is_err(), "path separator");
        assert!(KeyspaceName::new("x".repeat(65)).is_err(), "too long");
    }
}
