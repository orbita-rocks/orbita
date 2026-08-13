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
    ///
    /// Only for failures raised *before* anything was written. A failure after
    /// the entry is durable locally is [`Error::Indeterminate`], because the
    /// two need opposite handling and one code covering both leaves a client
    /// unable to tell them apart.
    #[error("partition unavailable: {0}")]
    Unavailable(String),

    /// The write may or may not have taken effect, and nobody can say which.
    ///
    /// Raised when an owner cannot reach a durability quorum. By then the
    /// entry is already fsynced to the owner's own log, so it is not a write
    /// that did not happen: it is one whose outcome is unknown, and it may
    /// still land later — the next open replays the log above the flush
    /// horizon.
    ///
    /// **Not retryable, and that is the whole point of it existing.** A client
    /// that replays it can have its retry applied alongside the entry the log
    /// was always going to replay. For an unconditional put the two agree; for
    /// a conditional one they do not, and the retry is refused for a condition
    /// the client's own first attempt made false. A lock taken with IF NOT
    /// PRESENT is then reported as lost by the client that holds it, which is
    /// the wedge use case failing quietly.
    ///
    /// What a caller should do instead is read the key back and decide from
    /// what it finds. That is the only thing that distinguishes the two
    /// outcomes, and no error code can do it for them.
    #[error("write outcome unknown: {0}")]
    Indeterminate(String),

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
    /// True only when the request is known not to have taken effect, so a
    /// replay cannot double-apply anything. That is narrower than "retrying
    /// might succeed": [`Error::Indeterminate`] might well succeed on a retry
    /// and is still false here, because the first attempt may have landed and
    /// the client has no way to know.
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
