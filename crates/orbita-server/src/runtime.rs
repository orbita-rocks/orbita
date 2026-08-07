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
    spawn_handle: Option<tokio::runtime::Handle>,
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
            spawn_handle: None,
        }
    }

    /// Routes tasks spawned through this clone onto the reserved control
    /// executor while preserving the same clock, disk, transport, and RNG.
    #[must_use]
    pub(crate) fn for_control(&self, handle: tokio::runtime::Handle) -> Self {
        let mut runtime = self.clone();
        runtime.spawn_handle = Some(handle);
        runtime
    }

    /// Replaces the peer call timeout, which has to happen before anything is
    /// registered on the transport because it builds a new one.
    #[must_use]
    pub fn with_peer_call_timeout(mut self, node: NodeId, timeout: std::time::Duration) -> Self {
        self.transport = PeerTransport::with_timeout(node, timeout);
        self
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
        if let Some(handle) = &self.spawn_handle {
            handle.spawn(future);
        } else {
            tokio::spawn(future);
        }
    }
}

/// One executor thread reserved for Raft ticks and controller work.
pub(crate) struct ControlExecutor {
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
    handle: tokio::runtime::Handle,
}

impl ControlExecutor {
    pub(crate) fn start() -> orbita_core::Result<Self> {
        let (handle_tx, handle_rx) = std::sync::mpsc::sync_channel(1);
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let thread = std::thread::Builder::new()
            .name("orbita-control".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("building the reserved control runtime");
                handle_tx
                    .send(runtime.handle().clone())
                    .expect("server receives the control runtime handle");
                runtime.block_on(async {
                    let _ = stopped.await;
                });
            })
            .map_err(|error| {
                orbita_core::Error::Internal(format!(
                    "starting the reserved control executor: {error}"
                ))
            })?;
        let handle = handle_rx.recv().map_err(|error| {
            orbita_core::Error::Internal(format!("starting the control executor: {error}"))
        })?;
        Ok(Self {
            stop: Some(stop),
            thread: Some(thread),
            handle,
        })
    }

    pub(crate) fn handle(&self) -> tokio::runtime::Handle {
        self.handle.clone()
    }
}

impl Drop for ControlExecutor {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
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

    #[test]
    fn saturated_worker_execution_cannot_starve_control_ticks() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        let worker = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let executor = ControlExecutor::start().unwrap();
        let runtime = ServerRuntime::new(NodeId(1), ".", Some(1)).for_control(executor.handle());
        let ticks = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&ticks);
        runtime.spawn(async move {
            for _ in 0..5 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                observed.fetch_add(1, Ordering::Release);
            }
        });

        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        worker.spawn(async move {
            while !worker_stop.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }
        });
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert_eq!(
            ticks.load(Ordering::Acquire),
            5,
            "the control executor has its own thread and bounded timers"
        );
        stop.store(true, Ordering::Release);
        drop(worker);
        drop(executor);
    }
}
