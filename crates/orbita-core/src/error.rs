//! The error type that crosses crate boundaries.
//!
//! Each variant maps to exactly one gRPC status code at the edge, and the
//! mapping lives with the server. Adding a variant here means deciding what a
//! client should do about it, which is why the list is short on purpose.

use crate::ids::{Epoch, NodeId, PartitionId, Version};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("key not found")]
    NotFound,

    #[error("key already exists")]
    AlreadyExists,

    #[error("version mismatch: expected {expected}, found {actual:?}")]
    VersionMismatch {
        expected: Version,
        /// `None` when the key does not exist at all.
        actual: Option<Version>,
    },

    #[error("keyspace not found")]
    KeyspaceNotFound,

    #[error("keyspace already exists")]
    KeyspaceAlreadyExists,

    /// This node does not own the partition. Carries the current owner so the
    /// receiving node can forward rather than making the client re-resolve.
    #[error("not the owner of partition {partition}; owner is {owner:?}")]
    NotOwner {
        partition: PartitionId,
        owner: Option<NodeId>,
    },

    /// The request carried an ownership epoch that has since been superseded,
    /// which is how a deposed owner learns it has been fenced.
    #[error("stale epoch {got} for partition {partition}, current is {current}")]
    StaleEpoch {
        partition: PartitionId,
        got: Epoch,
        current: Epoch,
    },

    #[error("{what} exceeds limit: {size} > {limit} bytes")]
    TooLarge {
        what: &'static str,
        size: usize,
        limit: usize,
    },

    #[error("quota exceeded: {0}")]
    QuotaExceeded(String),

    #[error("unauthenticated")]
    Unauthenticated,

    #[error("permission denied")]
    PermissionDenied,

    /// Retryable: the partition has no owner right now, typically mid-failover.
    #[error("partition unavailable: {0}")]
    Unavailable(String),

    /// This node is not the leader of the control plane.
    ///
    /// Distinct from [`Error::Unavailable`] because the caller should retry
    /// somewhere specific rather than back off: a client that cannot tell
    /// "wrong node" from "node down" retries the same dead path.
    #[error("not the leader; leader is {leader:?}")]
    NotLeader { leader: Option<NodeId> },

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("internal error: {0}")]
    Internal(String),
}

impl Error {
    /// Whether a client can safely retry the same request.
    ///
    /// Every write is idempotent under retry because conditional writes carry
    /// an expected version, so this is about whether retrying could ever
    /// succeed, not about side effects.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Error::Unavailable(_)
                | Error::NotOwner { .. }
                | Error::StaleEpoch { .. }
                | Error::NotLeader { .. }
        )
    }
}
