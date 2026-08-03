//! The worker node.
//!
//! Serves the client gRPC API, routes each request to the partition that owns
//! the key, proxies to the owning node when that is not this one, and
//! implements the linearizable read path: a replica serves a read locally only
//! under a live lease with no gap in its invalidation stream, and forwards it
//! to the owner otherwise.
//!
//! This crate also owns the production peer transport and the `Runtime`
//! implementation that production binaries use, which is the piece
//! `orbita_runtime::tokio_runtime` deliberately leaves out.
//!
//! Work brief: `docs/plan/04-server.md`.
//!
//! # Starting one
//!
//! ```no_run
//! # async fn run() -> orbita_core::Result<()> {
//! use orbita_server::{Server, ServerConfig};
//!
//! let server = Server::start(ServerConfig::single_node("data")).await?;
//! println!("serving on {}", server.local_addr());
//! server.shutdown().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # What is built and what is not
//!
//! A single node serves the whole `Kv` API end to end: conditional writes,
//! TTLs, prefix scans with pagination, and the size limits, over real gRPC.
//! Routing and proxying to a partition owner are built and are exercised
//! through the runtime's transport seam.
//!
//! Two pieces are deliberately unfinished rather than half-done, and both are
//! called out where they live. Dialling a remote peer is missing from
//! [`PeerTransport`], so a production multi-node cluster cannot forward yet
//! even though everything above the transport can. And a replica never
//! receives an invalidation, because `orbita_wal::WalService` has no hook to
//! deliver one, so a replica holds no lease and forwards every read. Both fail
//! in the safe direction: a request is refused or forwarded, never answered
//! from state that might be stale.

#![forbid(unsafe_code)]

mod config;
#[cfg(test)]
mod forwarding;
mod host;
mod lease;
#[cfg(test)]
mod linearizability;
mod map_source;
mod node;
mod pending;
mod proxy;
mod runtime;
mod service;
mod status;
mod transport;
mod validate;

pub use config::{ServerConfig, DEFAULT_KEYSPACE};
pub use lease::{DEFAULT_LEASE_DURATION, DEFAULT_LEASE_MARGIN};
pub use map_source::{single_node_map, BoxedMapSource, MapSource, StaticMapSource};
pub use runtime::ServerRuntime;
pub use status::to_status;
pub use transport::PeerTransport;

use crate::node::{DataLayout, Node};
use crate::service::KvService;

use orbita_core::{Error, Result};
use orbita_proto::v1::kv_server::KvServer;

use std::net::SocketAddr;
use std::sync::Arc;

/// A running worker.
///
/// Holding one means the node is serving. Dropping one without calling
/// [`Server::shutdown`] leaves the listener running until the process exits,
/// which is fine for a binary and not what a test wants.
pub struct Server {
    local_addr: SocketAddr,
    node: Arc<Node<ServerRuntime>>,
    shutdown: tokio::sync::oneshot::Sender<()>,
    serving: tokio::task::JoinHandle<()>,
}

impl Server {
    /// Opens this node's partitions and starts serving clients.
    ///
    /// Returns once the socket is bound and every partition the map says this
    /// node holds is open and recovered, so a caller that gets a `Server` back
    /// can send it a request immediately.
    pub async fn start(config: ServerConfig) -> Result<Self> {
        let storage_root = config.data_dir.join("storage");
        std::fs::create_dir_all(&storage_root)
            .map_err(|e| Error::Internal(format!("creating {}: {e}", storage_root.display())))?;

        let runtime = ServerRuntime::new(config.node_id, &config.data_dir, config.rng_seed);
        let layout = DataLayout {
            storage_root,
            wal_root: "wal".to_string(),
        };
        let node = Node::start(runtime, config.node_id, layout, config.map_source).await?;

        let listener = tokio::net::TcpListener::bind(config.listen_addr)
            .await
            .map_err(|e| Error::Internal(format!("binding {}: {e}", config.listen_addr)))?;
        let local_addr = listener
            .local_addr()
            .map_err(|e| Error::Internal(format!("reading the bound address: {e}")))?;
        let incoming = tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
            .map_err(|e| Error::Internal(format!("serving on {local_addr}: {e}")))?;

        let (shutdown, stop) = tokio::sync::oneshot::channel();
        let service = KvServer::new(KvService::new(Arc::clone(&node)));
        let serving = tokio::spawn(async move {
            let served = tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async {
                    // A dropped sender means the `Server` handle went away, so
                    // stopping is the right answer to that too.
                    let _ = stop.await;
                })
                .await;
            if let Err(error) = served {
                tracing::error!(%error, "the client listener stopped");
            }
        });

        tracing::info!(node = config.node_id.get(), %local_addr, "orbita worker is serving");
        Ok(Self {
            local_addr,
            node,
            shutdown,
            serving,
        })
    }

    /// The address clients connect to, which is what was actually bound rather
    /// than what was asked for.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Stops serving and waits for in-flight requests to finish.
    pub async fn shutdown(self) -> Result<()> {
        let _ = self.shutdown.send(());
        self.serving
            .await
            .map_err(|e| Error::Internal(format!("the client listener panicked: {e}")))
    }

    /// Serves until something else stops the process.
    pub async fn wait(self) -> Result<()> {
        self.serving
            .await
            .map_err(|e| Error::Internal(format!("the client listener panicked: {e}")))
    }

    /// Refetches the partition map now rather than waiting for a misrouted
    /// request to trigger the repair. This is what an admin command calls.
    pub async fn refresh_map(&self) -> Result<()> {
        self.node.refresh_map().await
    }
}
