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
//! pretending; the partition runs on the copies it has.
//!
//! Owners periodically publish applied writes as partition-v1 segments. A WAL
//! checkpoint follows only after the manifest compare-and-swap succeeds, so a
//! failed or deposed writer always retains the log range recovery still needs.
//! Until hydration lands in issue #17, a replica that misses beyond the
//! retained WAL cannot catch up from the manifest and stays unavailable. WAL
//! truncation is live now; snapshot recovery is deliberately not implied.
//!
//! That is a sharp edge rather than a quiet one. The owner names the replica,
//! where it stopped, and the oldest entry it still holds, through
//! [`Server::replicas_beyond_retention`], and that drives the
//! `replicas-recoverable` readiness condition so the answer leaves the process
//! and a rolling update stops at a partition permanently short a copy. The
//! owner learns where each replica's log ends from the lease heartbeat, so a
//! restarted or promoted owner reaches the same verdict without writing
//! anything. `src/retention.rs` runs the whole cliff under the simulator and is
//! also the tracking test for #17.

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
#[cfg(test)]
mod retention;
mod runtime;
mod s3_store;
mod service;
mod status;
mod transport;
mod validate;

pub use config::{
    S3StorageConfig, ServerConfig, DEFAULT_CONTROL_POLL_INTERVAL, DEFAULT_FLUSH_INTERVAL,
    DEFAULT_KEYSPACE, DEFAULT_WAL_SEGMENT_BYTES,
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

use orbita_control::{
    AdminService, BootstrapSpec, ConsensusLog, ControlClient, ControlConfig, ControlService,
    Controller, KeyspaceConfig, RaftLog,
};
use orbita_core::{Error, Result};
use orbita_objectstore::s3::{S3Config, S3Store};
use orbita_objectstore::ObjectStore;
use orbita_proto::v1::health_server::HealthServer;
use orbita_proto::v1::kv_server::KvServer;
use orbita_runtime::{Clock, Runtime, ServiceId, Transport};

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
    raft: Option<Arc<RaftLog>>,
    control: Option<tokio::task::JoinHandle<()>>,
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
        let mut raft = None;
        let mut controller = None;
        if config.leader_member {
            let log = RaftLog::open(&runtime, &config.leader_group).await?;
            let control =
                Controller::new(runtime.clone(), Arc::clone(&log), ControlConfig::default());
            runtime
                .transport()
                .register(ServiceId::Control, ControlService::new(control.clone()));
            raft = Some(log);
            controller = Some(control);
        }

        // A leader must be reachable before an election can finish. Workers
        // defer binding until their partition handlers are registered below.
        let mut peer_listener = if config.leader_member {
            Some(
                runtime
                    .transport()
                    .listen(config.peer_listen_addr)
                    .await
                    .map_err(|e| {
                        Error::Internal(format!(
                            "binding peer port {}: {e}",
                            config.peer_listen_addr
                        ))
                    })?,
            )
        } else {
            None
        };

        if let (Some(log), Some(control)) = (&raft, &controller) {
            let local_address = config.peer_advertise_addr.clone().unwrap_or_else(|| {
                // A leader listener is already bound above, so ephemeral-port
                // tests can advertise the address the kernel selected.
                peer_listener
                    .as_ref()
                    .expect("leader peer listener is bound")
                    .local_addr()
                    .to_string()
            });
            start_control_plane(
                &runtime,
                log,
                control,
                &config.leader_group,
                &config.peers,
                &local_address,
            )
            .await?;
        }
        let store: Arc<dyn ObjectStore> = match config.object_store {
            Some(object_store) => match object_store.credentials.clone() {
                Some(credentials) => Arc::new(
                    S3Store::connect(S3Config {
                        endpoint: object_store.endpoint,
                        bucket: object_store.bucket,
                        region: object_store.region,
                        credentials,
                        force_path_style: object_store.force_path_style,
                    })
                    .map_err(|e| Error::Internal(format!("configuring object storage: {e}")))?,
                ),
                None => Arc::new(
                    s3_store::RefreshingS3Store::connect(object_store)
                        .await
                        .map_err(|e| Error::Internal(format!("configuring object storage: {e}")))?,
                ),
            },
            None => {
                // The local adapter keeps `orbita dev` self-contained.
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
            wal_segment_bytes: config.wal_segment_bytes,
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
        readiness.mark(ReadinessCondition::AcceptingOwnership);
        // A node with no leader group answers to nobody, so the join condition
        // is met by construction rather than left to hang readiness forever.
        // A joined node's condition is marked by the control loop below, on
        // its first report the leader group accepts.
        if control.is_none() {
            readiness.mark(ReadinessCondition::ControlPlaneJoined);
            readiness.mark(ReadinessCondition::ClusterVersionCompatible);
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
        let peers = match peer_listener.take() {
            Some(listener) => listener,
            None => runtime
                .transport()
                .listen(config.peer_listen_addr)
                .await
                .map_err(|e| {
                    Error::Internal(format!(
                        "binding peer port {}: {e}",
                        config.peer_listen_addr
                    ))
                })?,
        };
        let peer_addr = peers.local_addr();
        let peer_advertise_addr = config
            .peer_advertise_addr
            .clone()
            .unwrap_or_else(|| peer_addr.to_string());

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
            let role = if config.leader_member {
                orbita_control::NodeRole::Leader
            } else {
                orbita_control::NodeRole::Worker
            };
            let status = StatusReporter::new_with_role(
                client.clone(),
                config.node_id,
                role,
                peer_advertise_addr,
            );
            // The server keeps a handle so version-dependent behaviour can
            // ask which cluster version is active without joining the loop.
            reporter = Some(status.clone());
            let directory =
                PeerDirectorySync::new(client.clone(), runtime.transport().clone(), config.node_id);
            let leader_controller = controller.clone();
            tokio::spawn(Self::control_loop(
                Arc::downgrade(&node),
                status,
                directory,
                client,
                leader_controller,
                config.control_poll_interval,
                Arc::clone(&readiness),
            ))
        });
        let control_task = controller.as_ref().map(|controller| {
            let controller = controller.clone();
            tokio::spawn(async move { controller.run().await })
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
        let admin = controller.map(AdminService::new);
        let serving = tokio::spawn(async move {
            let shutdown = async {
                // A dropped sender means the `Server` handle went away, so
                // stopping is the right answer to that too.
                let _ = stop.await;
            };
            let served = match admin {
                Some(admin) => {
                    tonic::transport::Server::builder()
                        .add_service(service)
                        .add_service(health)
                        .add_service(admin.into_server())
                        .serve_with_incoming_shutdown(incoming, shutdown)
                        .await
                }
                None => {
                    tonic::transport::Server::builder()
                        .add_service(service)
                        .add_service(health)
                        .serve_with_incoming_shutdown(incoming, shutdown)
                        .await
                }
            };
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
            raft,
            control: control_task,
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
        if let Some(control) = &self.control {
            control.abort();
        }
        if let Some(raft) = &self.raft {
            raft.shutdown();
        }
        self.peers.shutdown().await;
        self.serving
            .await
            .map_err(|e| Error::Internal(format!("the client listener panicked: {e}")))
    }

    /// Serves until something else stops the process.
    pub async fn wait(&mut self) -> Result<()> {
        (&mut self.serving)
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
        client: ControlClient<ServerRuntime>,
        leader_controller: Option<Controller<ServerRuntime, RaftLog>>,
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
            let reported = match reporter
                .report(version, progress, readiness.is_ready(), false)
                .await
            {
                Ok(orbita_control::StatusReportResponse::Accepted { .. }) => {
                    readiness.mark(ReadinessCondition::ClusterVersionCompatible);
                    true
                }
                Ok(orbita_control::StatusReportResponse::Incompatible(_)) => {
                    readiness.clear(ReadinessCondition::ClusterVersionCompatible);
                    readiness.clear(ReadinessCondition::ControlPlaneJoined);
                    false
                }
                // A heartbeat outage does not clear a completed join or make
                // the data plane unavailable on its own.
                Err(error) => {
                    tracing::debug!(%error, "reporting status to the leader group failed");
                    false
                }
            };
            match &leader_controller {
                Some(controller) => {
                    let caught_up = match client.fetch_commit_index().await {
                        Ok(authority) => controller
                            .catch_up_through(authority)
                            .await
                            .unwrap_or(false),
                        Err(_) => false,
                    };
                    update_control_readiness(&readiness, reported, Some(caught_up));
                }
                None => update_control_readiness(&readiness, reported, None),
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

    /// Replicas of this node's partitions that have fallen further behind than
    /// its log still reaches.
    ///
    /// Empty is healthy. Anything else names a replica that is out of the read
    /// set and out of the durability quorum and that will not come back on its
    /// own: WAL truncation is live and hydration from object storage is not
    /// (issue #17), so the entries it needs exist only as segments nothing can
    /// yet turn back into a caught-up replica. This is the answer an operator
    /// asks for rather than greps for.
    #[must_use]
    pub async fn replicas_beyond_retention(
        &self,
    ) -> Vec<(orbita_core::PartitionId, orbita_wal::BeyondRetention)> {
        self.node.replicas_beyond_retention().await
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

    /// Stops write admission, reports draining, and waits until the control
    /// plane has committed every ownership handoff before stopping listeners.
    ///
    /// The timeout is deliberately external policy. Kubernetes gives the pod
    /// a grace period, while the process uses a slightly smaller drain budget
    /// so a failure is logged before the orchestrator sends SIGKILL.
    pub async fn drain(self, timeout: Duration) -> Result<()> {
        if self
            .reporter
            .as_ref()
            .is_none_or(|reporter| !reporter.can_handoff())
        {
            return self.shutdown().await;
        }

        self.readiness.clear(ReadinessCondition::AcceptingOwnership);
        self.heartbeat.abort();
        self.flusher.abort();
        if let Some(reporting) = &self.reporting {
            reporting.abort();
        }
        let reporter = self.reporter.clone().expect("checked above");
        let node = Arc::clone(&self.node);
        let interval = node.lease_interval();
        let last_failure = Arc::new(std::sync::Mutex::new(Some(
            "waiting for in-flight writes to finish".to_string(),
        )));
        let failure = Arc::clone(&last_failure);
        let drain = async move {
            let _writes = node.begin_draining().await;
            *failure.lock().expect("drain failure lock poisoned") =
                Some("waiting for the control plane to accept draining state".into());
            loop {
                let reported = reporter
                    .report(node.map().version(), node.progress().await, false, true)
                    .await;
                if matches!(
                    reported,
                    Ok(orbita_control::StatusReportResponse::Accepted { .. })
                ) {
                    break;
                }
                tokio::time::sleep(interval).await;
            }

            // No lease can outlive this point because renewal stopped before
            // write admission closed. Keep reporting the draining state while
            // waiting so a delayed executor cannot turn a planned transfer
            // into failover by letting the failure detector fence this node.
            let leases_expire_at = tokio::time::Instant::now() + node.lease_drain();
            while tokio::time::Instant::now() < leases_expire_at {
                tokio::time::sleep_until(
                    leases_expire_at.min(tokio::time::Instant::now() + interval),
                )
                .await;
                let _ = reporter
                    .report(node.map().version(), node.progress().await, false, true)
                    .await;
            }
            loop {
                let _ = reporter
                    .report(node.map().version(), node.progress().await, false, true)
                    .await;
                match reporter.drain_node().await {
                    Ok(true) => {
                        node.refresh_map().await?;
                        if node.owned_partition_count() == 0 {
                            return Ok::<(), Error>(());
                        }
                    }
                    Ok(false) => {
                        *failure.lock().expect("drain failure lock poisoned") = Some(
                            "waiting for receiving owners to acknowledge their handoff maps".into(),
                        );
                    }
                    Err(error) => {
                        *failure.lock().expect("drain failure lock poisoned") =
                            Some(error.to_string());
                        tracing::warn!(%error, "partition handoff is not complete; retrying");
                    }
                }
                tokio::time::sleep(interval).await;
            }
        };

        tokio::time::timeout(timeout, drain).await.map_err(|_| {
            let detail = last_failure
                .lock()
                .expect("drain failure lock poisoned")
                .clone()
                .unwrap_or_else(|| "the control plane did not accept a drain pass".into());
            Error::Unavailable(format!(
                "node drain timed out after {} seconds; remaining ownership was not handed off: {detail}",
                timeout.as_secs(),
            ))
        })??;

        let _ = self.shutdown.send(());
        if let Some(reporting) = &self.reporting {
            reporting.abort();
        }
        if let Some(control) = &self.control {
            control.abort();
        }
        if let Some(raft) = &self.raft {
            raft.shutdown();
        }
        self.peers.shutdown().await;
        self.serving
            .await
            .map_err(|e| Error::Internal(format!("the client listener panicked: {e}")))
    }
}

async fn start_control_plane(
    runtime: &ServerRuntime,
    log: &Arc<RaftLog>,
    controller: &Controller<ServerRuntime, RaftLog>,
    voters: &[orbita_core::NodeId],
    peers: &[(orbita_core::NodeId, String)],
    local_address: &str,
) -> Result<()> {
    let client = ControlClient::new(runtime.clone(), voters.to_vec());
    controller.recover().await?;
    let deadline = runtime.clock().monotonic_nanos() + Duration::from_secs(30).as_nanos() as u64;
    loop {
        controller.recover().await?;
        let fresh = controller.snapshot().await.is_fresh();
        if !fresh {
            if let Ok(authority) = client.fetch_commit_index().await {
                if controller.catch_up_through(authority).await? {
                    return Ok(());
                }
            }
        }
        if fresh && log.is_leader().await {
            let leaders = peers
                .iter()
                .filter(|(node, _)| voters.contains(node))
                .cloned()
                .collect();
            controller
                .bootstrap(&BootstrapSpec {
                    keyspace: DEFAULT_KEYSPACE.to_string(),
                    config: KeyspaceConfig::default(),
                    leaders,
                    workers: Vec::new(),
                })
                .await?;
            tracing::info!(address = local_address, "bootstrapped the leader group");
            return Ok(());
        }
        if runtime.clock().monotonic_nanos() >= deadline {
            return Err(Error::Unavailable(format!(
                "leader group {:?} did not elect a leader within 30 seconds",
                voters
            )));
        }
        runtime.clock().sleep(Duration::from_millis(100)).await;
    }
}

fn update_control_readiness(
    readiness: &ReadinessGate,
    reported: bool,
    leader_caught_up: Option<bool>,
) {
    match leader_caught_up {
        Some(true)
            if reported
                || readiness
                    .state()
                    .is_met(ReadinessCondition::ControlPlaneJoined) =>
        {
            readiness.mark(ReadinessCondition::ControlPlaneJoined);
        }
        Some(_) => readiness.clear(ReadinessCondition::ControlPlaneJoined),
        None if reported => readiness.mark(ReadinessCondition::ControlPlaneJoined),
        None => {}
    }
}

#[cfg(test)]
mod leader_readiness_tests {
    use super::*;

    #[test]
    fn an_accepted_leader_report_does_not_make_a_lagging_voter_ready() {
        // Everything this function does not touch already holds, so that what
        // is left unmet is what it decided. Written as "all but one" rather
        // than a list, because a list goes stale the next time a condition is
        // added and fails a test about something else.
        let readiness = ReadinessGate::new();
        for condition in ReadinessCondition::ALL {
            if condition != ReadinessCondition::ControlPlaneJoined {
                readiness.mark(condition);
            }
        }

        update_control_readiness(&readiness, true, Some(false));

        assert_eq!(
            readiness.state().unmet(),
            vec![ReadinessCondition::ControlPlaneJoined]
        );
    }

    #[test]
    fn a_voter_becomes_ready_after_applying_through_the_leader_authority() {
        // Everything this function does not touch already holds, so that what
        // is left unmet is what it decided. Written as "all but one" rather
        // than a list, because a list goes stale the next time a condition is
        // added and fails a test about something else.
        let readiness = ReadinessGate::new();
        for condition in ReadinessCondition::ALL {
            if condition != ReadinessCondition::ControlPlaneJoined {
                readiness.mark(condition);
            }
        }

        update_control_readiness(&readiness, true, Some(true));

        assert!(readiness.is_ready());
    }

    #[test]
    fn a_ready_voter_turns_unready_when_it_falls_behind() {
        let readiness = ReadinessGate::new();
        for condition in ReadinessCondition::ALL {
            readiness.mark(condition);
        }

        update_control_readiness(&readiness, true, Some(false));

        assert!(!readiness.is_ready());
    }
}
