//! Single-partition storage engine.
//!
//! This crate owns one partition and everything that happens inside it:
//! enforcing conditional writes, filtering expired keys, serving prefix
//! scans, and moving data between the mutable table and the immutable
//! segments `orbita-format` defines, per
//! [ADR 0006](../../../docs/adr/0006-partitions-are-an-index-over-immutable-objects.md).
//! Every persisted byte goes through `orbita_objectstore::ObjectStore`, which
//! is what lets the deterministic simulation stand in a faultable store and
//! what makes a worker cheap to replace. The crate knows nothing about
//! replication, ownership, or the cluster.
//!
//! Work briefs: `docs/plan/01-storage.md` and `docs/plan/07-format.md`.
//!
//! # A key's version is the Lamport it was written at
//!
//! Per [ADR 0002](../../../docs/adr/0002-key-versions-are-partition-lamports.md),
//! a key's version is the partition Lamport at which that key was last
//! written. Versions are therefore unique within a partition, always
//! increasing, never reused, and sparse from any one key's point of view: a
//! key can go from version 47 to 112 to 900 as other keys are written in
//! between. That sparseness is not a defect, because a version is an opaque
//! token to compare against rather than a count of anything.
//!
//! An earlier version of this crate gave each key its own counter, and that is
//! superseded. The short reason is that per-key counters restart once a
//! tombstone is reclaimed, so a stale compare-and-swap could succeed against a
//! different value that happens to sit at the same number, and that a single
//! sequence per partition is what lets a replica notice it has missed an
//! invalidation. The ADR has the argument in full.
//!
//! A version is only meaningful inside its partition. Comparing versions
//! across partitions means nothing, and since compare-and-swap is single-key
//! and a key lives in exactly one partition, it never needs to happen.
//!
//! # The caller assigns the Lamport
//!
//! [`Partition::put`] and [`Partition::delete`] take the Lamport to write at
//! rather than allocating one. Under
//! [ADR 0001](../../../docs/adr/0001-linearizable-reads-from-replicas.md) the
//! owner assigns a Lamport, appends to its own log, and replicates before the
//! storage engine ever sees the write, so by the time storage is involved the
//! number is already fixed and already on the wire. A partition that allocated its own
//! would produce a second number that has to agree with the first, and the two
//! nodes replaying that write would have nothing to agree on.
//!
//! The partition still records the highest Lamport it has committed, both
//! because that is what a replica reports about how caught up it is and
//! because it is the only place a reused or rewound Lamport can be caught.
//! Local writes and replayed mutations advance the same counter, so a promoted
//! replica keeps writing where the log left off. A write that fails its
//! condition commits nothing and does not consume the Lamport.
//!
//! Assigning outside the partition means the caller must assign under the same
//! serialization that submits the write. The owner's single write path does
//! exactly that. A Lamport that is not ahead of what is committed is rejected,
//! because a duplicate version is invisible once it is on disk.
//!
//! # Deletes are explicit tombstones
//!
//! Deleting a key writes a record marked deleted, carrying the version the
//! delete produced, instead of erasing the key. Conditional writes need to
//! tell "this key never existed" apart from "this key was deleted at version
//! 112", because a client doing compare-and-swap on a lease has to know
//! whether it lost a race or is looking at a fresh key. An absence cannot
//! answer that; a tombstone can.
//!
//! The cost is space, and the answer to that cost is the mechanism TTL already
//! needs: a tombstone is stored with an absolute expiry a day out, and the
//! compaction that reclaims expired records reclaims it too. A delete costs
//! one record for a day and then nothing.
//!
//! # Cursors name a key, not a position
//!
//! A scan cursor is opaque to clients, but inside it is the last key the
//! previous page returned, plus a hash of the prefix it was issued for. It
//! deliberately does not contain an offset, a snapshot identifier, or a
//! partition id.
//!
//! That matters because a partition can split between two pages of the same
//! scan. An offset into a partition means nothing once that partition becomes
//! two, and a snapshot identifier is worse: it is valid on the wrong node and
//! quietly returns the wrong range. A key position survives, because dividing
//! a range does not reorder keys. Whichever partition now covers that key can
//! resume the scan from it, and the two halves between them still cover every
//! key exactly once. A backup tool that pages through a keyspace during a
//! split gets a complete answer, which is the whole point.
//!
//! For the same reason a cursor pointing below this partition's range is
//! clamped to the range start rather than rejected, and one pointing at or
//! above the range end yields an empty final page. Both are states a caller
//! lands in right after a split, and neither can skip a key: the keys below
//! the range belong to the sibling partition, which the caller reaches through
//! the partition map.
//!
//! # Relationship to the write-ahead log
//!
//! This crate does not depend on `orbita-wal`. It defines [`Mutation`], the
//! smallest description of a committed write that a replica needs in order to
//! reproduce it, and the WAL crate constructs one when it decodes an entry.
//! Wiring the two together is a later, deliberate step that belongs to the
//! server crate. Keeping the dependency out means neither crate waits on the
//! other, and the storage engine stays testable without a log.
//!
//! A `Mutation` carries no version, because the Lamport a log entry already
//! has is the version.

#![forbid(unsafe_code)]

mod cursor;
mod mutation;
mod partition;

#[cfg(test)]
mod model_test;
#[cfg(test)]
mod testing;

pub use mutation::{Mutation, MutationOp};
pub use partition::{Partition, ScanEntry, ScanPage, WriteOutcome, TOMBSTONE_RETENTION_MILLIS};
