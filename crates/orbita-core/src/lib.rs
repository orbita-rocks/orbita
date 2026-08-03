//! Shared vocabulary for Orbita.
//!
//! Every other crate depends on this one, and this one depends on almost
//! nothing. It holds the identifiers, key ranges, versions, limits, and errors
//! that cross crate boundaries, so that two crates never invent competing
//! spellings of the same idea.
//!
//! This is a contract crate. Changes here ripple through the whole workspace,
//! so they go through the contract owner rather than being made in passing.

#![forbid(unsafe_code)]

mod error;
mod ids;
mod limits;
mod map;
mod range;
mod record;

pub use error::{Error, Result};
pub use ids::{Epoch, KeyspaceId, KeyspaceName, Lamport, NodeId, PartitionId, Version};
pub use limits::{
    MAX_KEY_BYTES, MAX_LIST_BYTES, MAX_LIST_LIMIT, MAX_VALUE_BYTES, MESSAGE_OVERHEAD_BYTES,
};
pub use map::{CoverageError, KeyspaceInfo, MapVersion, PartitionInfo, PartitionMap};
pub use range::KeyRange;
pub use record::{Record, WriteCondition};
