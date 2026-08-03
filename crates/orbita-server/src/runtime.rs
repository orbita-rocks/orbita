//! The runtime a production node uses.
//!
//! `orbita_runtime::tokio_runtime` deliberately stops short of a `Runtime`
//! implementation, because the transport half of it is peer-to-peer messaging
//! and that belongs to the worker. This is the assembly of the two: the
//! operating system's clock, disk, and randomness, plus this crate's peer
//! transport.
//!
//! This is also the only place in the crate that reaches for a real clock or
//! spawns a real task. Everything above it goes through the trait, which is
//! what lets the same code run under the simulator.

use crate::transport::PeerTransport;

use orbita_core::NodeId;
use orbita_runtime::tokio_runtime::{TokioClock, TokioDisk};
use orbita_runtime::{Clock, Runtime, SeededRng};

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

/// What a worker runs on in production.
#[derive(Clone)]
pub struct ServerRuntime {
    clock: TokioClock,
    disk: TokioDisk,
    transport: PeerTransport,
    rng: Arc<SeededRng>,
}

impl ServerRuntime {
    /// Builds the runtime for one node, rooted at its data directory.
    ///
    /// The seed is explicit so that a production incident can be replayed with
    /// the same jitter choices the node made. A node given no seed derives one
    /// from its identity and start time, which is unpredictable enough for
    /// jitter and is never used for anything that needs to be secret.
    #[must_use]
    pub fn new(node: NodeId, data_dir: impl Into<PathBuf>, seed: Option<u64>) -> Self {
        let clock = TokioClock::new();
        let seed = seed.unwrap_or_else(|| clock.now_millis() ^ (node.get().wrapping_mul(0x9E37)));
        Self {
            disk: TokioDisk::new(data_dir),
            transport: PeerTransport::new(node),
            rng: Arc::new(SeededRng::new(seed)),
            clock,
        }
    }
}

impl Runtime for ServerRuntime {
    type Clock = TokioClock;
    type Disk = TokioDisk;
    type Transport = PeerTransport;
    type Rng = Arc<SeededRng>;

    fn clock(&self) -> &Self::Clock {
        &self.clock
    }

    fn disk(&self) -> &Self::Disk {
        &self.disk
    }

    fn transport(&self) -> &Self::Transport {
        &self.transport
    }

    fn rng(&self) -> &Self::Rng {
        &self.rng
    }

    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(future);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orbita_runtime::{Disk, OpenOptions, Rng};

    #[tokio::test]
    async fn the_disk_is_rooted_at_the_nodes_data_directory() {
        let root = std::env::temp_dir().join(format!("orbita-runtime-test-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let runtime = ServerRuntime::new(NodeId(1), &root, Some(7));

        runtime
            .disk()
            .open("wal/000000000001.wal", OpenOptions::create())
            .await
            .expect("a path inside the root opens");
        assert!(
            runtime
                .disk()
                .open("../escape", OpenOptions::create())
                .await
                .is_err(),
            "a node must not be able to write outside its own data directory"
        );

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn a_pinned_seed_reproduces_the_same_choices() {
        let a = ServerRuntime::new(NodeId(1), ".", Some(42));
        let b = ServerRuntime::new(NodeId(1), ".", Some(42));
        assert_eq!(a.rng().next_u64(), b.rng().next_u64());
    }
}
