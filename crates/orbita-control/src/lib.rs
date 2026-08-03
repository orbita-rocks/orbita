//! The leader group.
//!
//! The nodes that hold the cluster's authoritative metadata: the partition
//! map, worker membership, keyspace definitions and quotas, and ownership
//! epochs. It detects dead workers, fences and replaces partition owners, and
//! decides when to split.
//!
//! Nothing here is on the data path. Workers cache what they need and keep
//! serving reads while the leader group is unavailable, which is deliberate: a
//! control plane outage must not become a data plane outage. The shape that
//! makes it true is that [`ControlClient`] is only ever called on a timer, and
//! what it returns is a value the caller keeps.
//!
//! Work brief: `docs/plan/03-control.md`.
//!
//! # Shape
//!
//! [`ClusterState`] is the replicated state machine: a pure function from a
//! sequence of [`ControlCommand`]s to the cluster's metadata. [`Controller`]
//! proposes commands, applies them, and runs the sweep that turns missed
//! heartbeats into failovers. [`ConsensusLog`] is the seam consensus lives
//! behind, [`SingleNodeLog`] is the implementation that ships today, and
//! [`ControlClient`] is how everything outside this crate reaches it.
//!
//! # What is here and what is not
//!
//! Consensus is behind a trait with a single-node implementation underneath
//! it. That is staging, and [`consensus`] documents it as such along with
//! exactly how `openraft` slots in without changing anything above the trait.
//! A single-node control plane is not a production configuration and this
//! crate does not pretend it is.
//!
//! Partition merge is not implemented. It is the highest-risk requirement in
//! the project, `docs/plan/README.md` flags it as the first thing to cut, and
//! a half-built merge would be worse than none. The constraint it has to meet
//! is written down in
//! [ADR 0002](../../../docs/adr/0002-key-versions-are-partition-lamports.md):
//! a merged partition's Lamport sequence has to exceed everything either side
//! ever issued, or a client's held version stops matching through no write of
//! its own.
//!
//! # The failover ordering
//!
//! This is the sequence the ten second target and the no-lost-write guarantee
//! both depend on:
//!
//! 1. Heartbeats stop. The owner goes suspect, then dead. Replicas keep
//!    serving reads throughout.
//! 2. One entry marks the partition ownerless and bumps its epoch. The bump is
//!    what fences the old owner, so it commits before anything else happens.
//! 3. The new owner waits out the deposed owner's read leases.
//! 4. One entry names the most caught-up replica as owner.
//!
//! Steps two and four cannot be reordered or fused, and that is enforced in
//! [`ClusterState`] rather than in the code that drives a failover:
//! `AssignOwner` is rejected for a partition that still has an owner, and the
//! only thing that removes an owner is `FencePartition`, which bumps the epoch
//! in the same entry. A caller cannot get the order wrong even by trying.
//!
//! Step three is why [`ControlConfig`] keeps the lease duration next to the
//! failure detection thresholds. Per
//! [ADR 0001](../../../docs/adr/0001-linearizable-reads-from-replicas.md), a
//! replica may serve a read locally while it holds a live lease from the
//! owner, and the deposed owner may have renewed one an instant before it
//! died. Promoting immediately would let a replica answer with a pre-failover
//! value after the new owner had already accepted a write. So the promotion
//! waits `lease_duration + lease_margin` measured from the fence, which is
//! strictly later than anything the deposed owner did.
//!
//! # Where quotas are enforced
//!
//! The leader group decides them and the worker enforces them. Quotas travel
//! in the partition map, so a worker enforcing a limit is reading a value it
//! already holds rather than asking anybody. The cost is staleness bounded by
//! the worker's refresh interval: for a few seconds after an operator lowers a
//! limit, a tenant may exceed the new one. That is the right trade, because
//! the alternative puts a control plane round trip on every request and
//! therefore puts the control plane on the data path.
//!
//! # Gaps found in the contract crates
//!
//! Two, both worked around here rather than changed, per the rule in
//! `docs/plan/README.md`:
//!
//! - `PartitionMap` can insert a keyspace but not remove one, so deleting a
//!   keyspace rebuilds the map from the tables this crate owns.
//! - `PartitionMap` has no iterator over its keyspaces, so encoding one for
//!   the wire recovers the keyspace set from the partitions. That is only
//!   correct because every keyspace here is born with a partition and never
//!   loses its last one.

#![forbid(unsafe_code)]

mod admin;
mod client;
mod codec;
mod command;
mod config;
pub mod consensus;
mod controller;
mod membership;
mod model;
mod service;
mod state;
mod wire;

pub use admin::AdminService;
pub use client::{ControlClient, LocalControlClient};
pub use codec::CodecError;
pub use command::ControlCommand;
pub use config::ControlConfig;
pub use consensus::{ConsensusLog, LogEntry, LogIndex, SingleNodeLog};
pub use controller::{BootstrapSpec, ClusterView, Controller, NodeView, PartitionView};
pub use membership::{NodeHealth, NodeRole, NodeStatus, PartitionProgress};
pub use model::{hash_secret, Credential, Keyspace, KeyspaceConfig, Permission};
pub use service::ControlService;
pub use state::{ClusterState, NodeRecord, PartitionPhase};
pub use wire::{METHOD_FETCH_MAP, METHOD_REPORT_STATUS};
