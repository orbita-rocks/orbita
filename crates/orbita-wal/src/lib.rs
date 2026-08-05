//! Write-ahead log and replication.
//!
//! A write is durable when its log entry is on stable storage at two of three
//! replicas. This crate owns the log format, the replication protocol between
//! an owner and its replicas, recovery from a torn tail, and the epoch check
//! that fences a deposed owner.
//!
//! This is where Orbita's durability claim actually lives, so it is also where
//! the simulator will spend most of its fault injection budget.
//!
//! Work brief: `docs/plan/02-wal.md`.
//!
//! # Shape
//!
//! [`Wal`] is the owner's side: it assigns Lamports, batches, fsyncs, and
//! replicates. [`WalService`] is the replica's side, registered against
//! `ServiceId::Wal`, serving every partition this node replicates.
//! [`PartitionLog`] is the file underneath both, and is what a node keeps when
//! it changes role.
//!
//! # Three decisions
//!
//! ## Pipelining: yes, but no holes
//!
//! Entry N+1 may be written and sent before N is acknowledged. Without that,
//! throughput is capped at one round trip per write, and the 5ms target has no
//! headroom left once the fsync is paid for.
//!
//! What is not allowed is a hole. Every batch names the Lamport it must
//! follow, and a replica that is not exactly there refuses the batch and says
//! how far it has got, so the owner backfills before continuing. A replica's
//! log is therefore always a prefix of the owner's, never a prefix with gaps
//! in it.
//!
//! That choice pays for itself twice. Recovery only ever has to find the end
//! of a contiguous run rather than reassemble a sparse one, and an
//! acknowledgement becomes a watermark rather than a set: a reply saying "I
//! have N" proves the replica also has everything below N, so a later
//! acknowledgement rescues an earlier one whose reply was lost.
//!
//! ## Checksum scope: per entry
//!
//! Each entry carries its own CRC32 over its own bytes, including its length
//! field. A batch-wide checksum would be cheaper, but it makes the unit of
//! damage the batch: one bad byte would discard every entry that happened to
//! share an fsync with it, and batches are largest exactly when the system is
//! busiest. Per entry, a crash midway through writing a batch keeps the
//! entries that made it and drops only the torn one.
//!
//! CRC32 is not a defence against a determined attacker. It is a defence
//! against a torn write and a lying disk, which are the failures that actually
//! happen here.
//!
//! ## Replica divergence: the newer epoch wins, and the extra entries go
//!
//! A promoted owner can find a peer holding entries the owner does not have.
//! Those entries were never acknowledged to any client, and here is why. An
//! acknowledgement requires the entry on two of three nodes. The control plane
//! promotes the most caught-up survivor, so if any surviving node held an
//! acknowledged entry, the promoted node holds it too. Anything above the
//! promoted owner's watermark therefore existed on at most the old owner and
//! one peer that is behind, which is not two of three, which means no client
//! was ever told it was written.
//!
//! So the peer's extra entries are discarded rather than merged. That happens
//! in two places: the fence message sent on promotion, and, for a peer that
//! was unreachable at the time, the first append it receives at the new epoch,
//! which truncates above the point the new owner says its history ends. There
//! is no path where a divergent entry survives and is later replayed.
//!
//! # Failure stance
//!
//! An owner that is fenced, or whose own disk fails a write or an fsync, stops
//! being an owner: every commit after that point fails until the node reopens
//! the log and recovers it. Losing contact with both replicas is not fatal in
//! the same way, because the ordered protocol means a later acknowledgement
//! subsumes an earlier one. Those commits fail rather than block, and the
//! caller is told the write was unavailable rather than that it succeeded.
//!
//! One known limit: `commit` is not cancellation safe. The committer that
//! wins the race to flush is the one driving the batch, so dropping its future
//! mid-flush leaves the others in that batch waiting. Nothing on disk is
//! damaged and no write is falsely acknowledged, but the fix is a dedicated
//! flusher task per partition, which is a change worth making once there is a
//! simulator to prove it did not introduce a stall.

#![forbid(unsafe_code)]

mod format;
mod log;
mod owner;
mod replica;
mod wire;

#[cfg(test)]
mod testkit;

#[cfg(test)]
mod tests;

pub use format::{WalEntry, WalOp};
pub use log::{
    CatchUp, PartitionLog, RecoveryState, Truncation, TruncationReason,
    DEFAULT_SEGMENT_TARGET_BYTES,
};
pub use owner::{BeyondRetention, Wal, WalConfig};
pub use replica::{ReplicaObserver, WalService};
pub use wire::{METHOD_APPEND, METHOD_FENCE, METHOD_STATUS};
