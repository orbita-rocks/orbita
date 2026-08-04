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
//! # The two listeners
//!
//! A node binds two ports. Clients speak gRPC on one, because that is where
//! compatibility is a promise to strangers. Peers speak the private framing in
//! [`crate::frame`] on the other, per ADR 0004, so that an operator can keep
//! peer traffic on a private network and expose only the client port.
//!
//! # What is built and what is not
//!
//! A cluster works end to end: writes replicate over real sockets, replicas
//! serve reads under a lease with per-key invalidation, the map comes from the
//! control plane, and losing an owner promotes a replica without losing an
//! acknowledged write. `tests/multi_node.rs` is that claim as a test.
//!
//! A node joined to a leader group is told only where that group is. It
//! reports its own peer address on its heartbeat and reads the other nodes'
//! back on the same timer, so peer addresses are discovered rather than
//! configured. [`ServerConfig::peers`] still seeds the directory, which is
//! what a node that starts before the control plane needs.
//!
//! What is not built is a snapshot. A replica that falls further behind than
//! its owner's log still holds cannot be caught up, and says so rather than
//! pretending; the owner logs it and the partition runs on the copies it has.
//!
//! Owners periodically publish applied writes as partition-v1 segments. A WAL
//! checkpoint follows only after the manifest compare-and-swap succeeds, so a
//! failed or deposed writer always retains the log range recovery still needs.
//! Until hydration lands in issue #17, a replica that misses beyond the
//! retained WAL cannot catch up from the manifest and stays unavailable. WAL
//! truncation is live now; snapshot recovery is deliberately not implied.

#![forbid(unsafe_code)]

mod config;
mod control;
#[cfg(test)]
mod forwarding;
mod frame;
mod fs_store;
mod host;
mod lease;
#[cfg(test)]
mod linearizability;
mod map_source;
mod node;
mod pending;
mod proxy;
mod readiness;
mod replication;
mod runtime;
mod service;
mod status;
mod transport;
mod validate;

pub use config::{
    ServerConfig, DEFAULT_CONTROL_POLL_INTERVAL, DEFAULT_FLUSH_INTERVAL, DEFAULT_KEYSPACE,
};
pub use control::{ControlMapSource, PeerDirectorySync, StatusReporter};
pub use lease::{DEFAULT_LEASE_DURATION, DEFAULT_LEASE_MARGIN};
pub use map_source::{single_node_map, BoxedMapSource, MapSource, StaticMapSource};
pub use readiness::{ReadinessCondition, ReadinessGate, ReadinessState};
pub use runtime::ServerRuntime;
pub use status::to_status;
pub use transport::{PeerListener, PeerTransport, DEFAULT_PEER_CALL_TIMEOUT};

use crate::node::{DataLayout, Node};
use crate::service::{HealthService, KvService};

use orbita_control::ControlClient;
use orbita_core::{Error, Result};
use orbita_objectstore::s3::S3Store;
use orbita_objectstore::ObjectStore;
use orbita_proto::v1::health_server::HealthServer;
use orbita_proto::v1::kv_server::KvServer;
use orbita_runtime::Runtime;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

/// A running worker.
///
/// Holding one means the node is serving. Dropping one without calling
/// [`Server::shutdown`] leaves the listener running until the process exits,
/// which is fine for a binary and not what a test wants.
pub struct Server {
    local_addr: SocketAddr,
    peer_addr: SocketAddr,
    node: Arc<Node<ServerRuntime>>,
    readiness: Arc<ReadinessGate>,
    transport: PeerTransport,
    peers: PeerListener,
    heartbeat: tokio::task::JoinHandle<()>,
    flusher: tokio::task::JoinHandle<()>,
    /// Present only for a node joined to a leader group.
    reporting: Option<tokio::task::JoinHandle<()>>,
    /// The reporting loop's handle, kept so the server can answer which
    /// cluster version is active. Present only alongside `reporting`.
    reporter: Option<StatusReporter<ServerRuntime>>,
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
        let runtime = ServerRuntime::new(config.node_id, &config.data_dir, config.rng_seed)
            .with_peer_call_timeout(config.node_id, config.peer_call_timeout);
        for (node, address) in &config.peers {
            runtime.transport().set_peer(*node, address.clone());
        }
        let store: Arc<dyn ObjectStore> = match config.object_store {
            Some(object_store) => Arc::new(
                S3Store::connect(object_store)
                    .map_err(|e| Error::Internal(format!("configuring object storage: {e}")))?,
            ),
            None => {
                let storage_root = config.data_dir.join("storage");
                std::fs::create_dir_all(&storage_root).map_err(|e| {
                    Error::Internal(format!("creating {}: {e}", storage_root.display()))
                })?;
                Arc::new(fs_store::FsStore::new(storage_root))
            }
        };
        let layout = DataLayout {
            store,
            wal_root: "wal".to_string(),
        };

        // A node in a real cluster takes its map from the leader group, and
        // the same client carries the heartbeat that failover watches for.
        let control = (!config.leader_group.is_empty())
            .then(|| ControlClient::new(runtime.clone(), config.leader_group.clone()));
        let map_source = match &control {
            Some(client) => BoxedMapSource::new(ControlMapSource::new(client.clone())),
            None => config.map_source,
        };

        let readiness = Arc::new(ReadinessGate::new());
        // A node with no leader group answers to nobody, so the join condition
        // is met by construction rather than left to hang readiness forever.
        // A joined node's condition is marked by the control loop below, on
        // its first report the leader group accepts.
        if control.is_none() {
            readiness.mark(ReadinessCondition::ControlPlaneJoined);
        }

        let node = Node::start(
            runtime.clone(),
            config.node_id,
            layout,
            map_source,
            config.lease_duration,
            Arc::clone(&readiness),
        )
        .await?;

        // Peers are served only once every handler is registered, so a peer
        // that connects the instant the port opens cannot be told that a
        // service this node does serve is missing.
        let peers = runtime
            .transport()
            .listen(config.peer_listen_addr)
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "binding peer port {}: {e}",
                    config.peer_listen_addr
                ))
            })?;
        let peer_addr = peers.local_addr();

        // The lease heartbeat is a production loop rather than something the
        // node starts for itself, so that a simulated run drives it a step at
        // a time and never has a timer keeping the world from going idle.
        let heartbeat = tokio::spawn(Self::renew_leases_loop(
            Arc::downgrade(&node),
            node.lease_interval(),
        ));
        let flusher = tokio::spawn(Self::flush_loop(
            Arc::downgrade(&node),
            config.flush_interval,
        ));

        let mut reporter = None;
        let reporting = control.map(|client| {
            let status = StatusReporter::new(client.clone(), config.node_id, peer_addr.to_string());
            // The server keeps a handle so version-dependent behaviour can
            // ask which cluster version is active without joining the loop.
            reporter = Some(status.clone());
            let directory =
                PeerDirectorySync::new(client, runtime.transport().clone(), config.node_id);
            tokio::spawn(Self::control_loop(
                Arc::downgrade(&node),
                status,
                directory,
                config.control_poll_interval,
                Arc::clone(&readiness),
            ))
        });

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
        let health = HealthServer::new(HealthService::new(Arc::clone(&readiness)));
        let serving = tokio::spawn(async move {
            let served = tonic::transport::Server::builder()
                .add_service(service)
                .add_service(health)
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

        tracing::info!(node = config.node_id.get(), %local_addr, %peer_addr, "orbita worker is serving");
        Ok(Self {
            local_addr,
            peer_addr,
            node,
            readiness,
            transport: runtime.transport().clone(),
            peers,
            heartbeat,
            flusher,
            reporting,
            reporter,
            shutdown,
            serving,
        })
    }

    /// The active cluster version this node last learned from the leader
    /// group, which is the version its behaviour gates on. `None` for a node
    /// with no leader group, or one whose first heartbeat has not landed.
    #[must_use]
    pub fn active_cluster_version(&self) -> Option<orbita_control::ClusterVersion> {
        self.reporter
            .as_ref()
            .and_then(StatusReporter::active_cluster_version)
    }

    /// The address clients connect to, which is what was actually bound rather
    /// than what was asked for.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// The address peers connect to, which is what this node advertises to the
    /// rest of the cluster.
    #[must_use]
    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    /// Stops serving and waits for in-flight requests to finish.
    pub async fn shutdown(self) -> Result<()> {
        let _ = self.shutdown.send(());
        self.heartbeat.abort();
        self.flusher.abort();
        if let Some(reporting) = &self.reporting {
            reporting.abort();
        }
        self.peers.shutdown().await;
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

    /// Renews the read leases this node's partitions have out, forever.
    ///
    /// A weak reference, so a node that is dropped stops heartbeating rather
    /// than keeping itself alive through its own background task.
    async fn renew_leases_loop(node: std::sync::Weak<Node<ServerRuntime>>, interval: Duration) {
        loop {
            let Some(live) = node.upgrade() else {
                return;
            };
            live.renew_leases().await;
            drop(live);
            tokio::time::sleep(interval).await;
        }
    }

    /// Publishes every owned partition on the configured cadence.
    async fn flush_loop(node: std::sync::Weak<Node<ServerRuntime>>, interval: Duration) {
        loop {
            tokio::time::sleep(interval).await;
            let Some(live) = node.upgrade() else {
                return;
            };
            live.flush_owned().await;
        }
    }

    /// Reports this node's progress to the leader group and refetches the map,
    /// forever.
    ///
    /// The two are one loop because they are two halves of the same
    /// conversation: the report is what keeps this node out of the failure
    /// detector, and the fetch is how it learns that a partition moved to or
    /// from it without a client request having to discover it first.
    async fn control_loop(
        node: std::sync::Weak<Node<ServerRuntime>>,
        reporter: StatusReporter<ServerRuntime>,
        directory: PeerDirectorySync<ServerRuntime>,
        interval: Duration,
        readiness: Arc<ReadinessGate>,
    ) {
        loop {
            let Some(live) = node.upgrade() else {
                return;
            };
            let version = live.map().version();
            let progress = live.progress().await;
            // Reported before the directory is read, so that this node's own
            // address is in the answer the other nodes get on their next poll.
            //
            // The first accepted report is the join for readiness purposes: it
            // is the moment the leader group knows this node's address and
            // progress. A later report failing does not clear the condition,
            // because a control plane outage must not unready every worker at
            // once; see `ReadinessCondition::ControlPlaneJoined`.
            if reporter.report(version, progress).await {
                readiness.mark(ReadinessCondition::ControlPlaneJoined);
            }
            directory.refresh().await;
            if let Err(error) = live.refresh_map().await {
                tracing::debug!(%error, "could not refresh the partition map");
            }
            drop(live);
            tokio::time::sleep(interval).await;
        }
    }

    /// Refetches the partition map now rather than waiting for a misrouted
    /// request to trigger the repair. This is what an admin command calls.
    pub async fn refresh_map(&self) -> Result<()> {
        self.node.refresh_map().await
    }

    /// Tells this node where a peer is, for a peer whose address was not known
    /// when the node started.
    pub fn add_peer(&self, node: orbita_core::NodeId, address: impl Into<String>) {
        self.transport.set_peer(node, address);
    }

    /// How many reads this node has answered from a partition it replicates
    /// rather than owns, which is the read path's whole reason to exist.
    #[must_use]
    pub fn replica_reads(&self) -> u64 {
        self.node.replica_reads()
    }

    /// The readiness gate this node reports from.
    ///
    /// Shared rather than snapshotted so a caller can subscribe and await a
    /// state instead of polling the HTTP surface. This is the handle the
    /// SIGTERM partition handoff (issue #26) consumes: a peer deciding whether
    /// to take partitions from a draining node asks this, not the probe.
    #[must_use]
    pub fn readiness(&self) -> Arc<ReadinessGate> {
        Arc::clone(&self.readiness)
    }
}
