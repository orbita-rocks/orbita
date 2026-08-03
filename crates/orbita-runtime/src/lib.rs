//! The seams Orbita runs on.
//!
//! Every interaction with the outside world that could differ between two runs
//! of the same code goes through a trait defined here: time, disk, peer
//! messaging, randomness, and task spawning. Production code gets the Tokio
//! implementation and the deterministic test harness gets a simulated one, and
//! neither the storage engine nor the control plane knows the difference.
//!
//! This exists because deterministic simulation cannot be retrofitted. If any
//! subsystem reaches for `SystemTime::now()` or `tokio::spawn` directly, the
//! simulation stops being reproducible and the correctness claim that Orbita
//! is built on stops being true. The traits are the enforcement mechanism.
//!
//! # Static versus dynamic dispatch
//!
//! These traits use `impl Future` return types, so they are not
//! dyn-compatible, and code that uses them is generic over `R: Runtime`. That
//! is deliberate: these sit on the hot path, and a boxed future per disk read
//! is a cost we would rather not pay for a choice that is fixed at compile
//! time anyway. Where pluggability matters more than nanoseconds, such as
//! object storage, we use dynamic dispatch instead.
//!
//! This is a contract crate. Changes here ripple through the whole workspace.

#![forbid(unsafe_code)]

mod clock;
mod combinator;
mod disk;
mod rng;
mod transport;

#[cfg(feature = "tokio-runtime")]
pub mod tokio_runtime;

pub use clock::Clock;
pub use combinator::{join_all, quorum, select, timeout, Either, Elapsed};
pub use disk::{Disk, DiskError, DiskResult, File, OpenOptions};
pub use rng::{Rng, SeededRng};
pub use transport::{PeerCall, PeerHandler, ServiceId, Transport, TransportError, TransportResult};

// Re-exported because every signature in this crate's traits mentions it, and
// implementers should not have to depend on orbita-core just to spell one.
pub use orbita_core::NodeId;

use std::future::Future;

/// The bundle of seams a node runs on.
///
/// Subsystems take a single `R: Runtime` parameter rather than four separate
/// ones, which keeps signatures readable and makes swapping in the simulator a
/// one-type change at the top of a test.
pub trait Runtime: Clone + Send + Sync + 'static {
    type Clock: Clock;
    type Disk: Disk;
    type Transport: Transport;
    type Rng: Rng;

    fn clock(&self) -> &Self::Clock;
    fn disk(&self) -> &Self::Disk;
    fn transport(&self) -> &Self::Transport;

    /// The seeded source of randomness.
    ///
    /// Anything that picks a jitter interval, a split point sample, or a peer
    /// to prefer must draw from here, or replaying a failing seed will not
    /// reproduce the failure.
    fn rng(&self) -> &Self::Rng;

    /// Spawns a background task.
    ///
    /// Under simulation the scheduler runs tasks in a deterministic order, so
    /// a bug that depends on an unlucky interleaving is reproducible from its
    /// seed.
    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static;
}
