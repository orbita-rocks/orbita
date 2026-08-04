//! The Orbita partition format, version 1.
//!
//! Everything Orbita writes to object storage for one partition, and everything
//! needed to read it back. The bytes are specified in
//! [`docs/format/partition-v1.md`](../../../docs/format/partition-v1.md), and
//! that document rather than this crate is the definition: the point of owning
//! a format is that something other than Orbita can read it, and this crate is
//! one implementation of the specification rather than its meaning.
//!
//! Per [ADR 0006](../../../docs/adr/0006-partitions-are-an-index-over-immutable-objects.md),
//! a partition is a memory-resident index over immutable objects. That shape is
//! what keeps the format small: a lookup never consults more than one segment,
//! so there are no bloom filters, no block cache, and no level structure, and
//! compaction exists only to reclaim space.
//!
//! # The pieces
//!
//! - [`manifest`] is the partition's only mutable object and its atomic
//!   pointer. Nothing outside it is authoritative.
//! - [`segment`] is the immutable, sorted object the data lives in, carrying a
//!   key index alongside it so that rebuilding an in-memory index never reads a
//!   value.
//! - [`record`] is one key at one Lamport, including the tombstones that let a
//!   conditional write tell a key that never existed apart from one deleted at
//!   a known version.
//! - [`commit`] publishes objects by swapping the manifest under a
//!   compare-and-swap, and fences a writer whose epoch has been superseded.
//! - [`snapshot`] is the reader: manifest, footers, key indexes, and then one
//!   range request per key.
//! - [`compact`] merges segments, and [`sweep`] finds the objects that leaves
//!   behind.
//!
//! # What this crate is not
//!
//! It does not hold recent writes, replay a log, decide when to flush, or know
//! that a cluster exists. It reads and writes the objects it is told to, so
//! that every persisted byte goes through [`ObjectStore`](orbita_objectstore::ObjectStore)
//! and the deterministic simulation can inject a fault anywhere in it.

#![forbid(unsafe_code)]

pub mod commit;
pub mod compact;
mod error;
pub mod manifest;
pub mod paths;
pub mod record;
pub mod segment;
pub mod snapshot;
pub mod sweep;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub use commit::{load_manifest, CommitPlan, PartitionWriter};
pub use error::{FormatError, Result};
pub use manifest::{Manifest, SegmentEntry};
pub use paths::PartitionPath;
pub use record::{ExternalValue, RecordValue, SegmentRecord};
pub use segment::{
    BuiltSegment, Segment, SegmentBuilder, SegmentFooter, SegmentHeader, SegmentIndex, FOOTER_LEN,
    FORMAT_VERSION, HEADER_LEN,
};
pub use snapshot::{KeyLocation, Snapshot};
