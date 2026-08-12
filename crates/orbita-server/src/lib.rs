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
//! Owners periodically publish applied writes as partition-v1 segments. A WAL
//! checkpoint follows only after the manifest compare-and-swap succeeds, so a
//! failed or deposed writer always retains the log range recovery still needs.
//!
//! A node that has to take on a partition builds it from that manifest rather
//! than from a peer, which is the operational payoff ADR 0006 was adopted for:
//! replacing a worker is a download, and it costs the replacement rather than
//! taxing a healthy node. Hydration happens when a partition is opened, and
//! again in place when a replica turns out to have fallen further behind than
//! its owner's retained log. Either way the log takes the horizon it was built
//! to as where its history starts, so replication resumes above it and WAL
//! recovery replays only the tail the manifest does not cover. That horizon is
//! re-derived from the manifest at every open rather than written into the log,
//! which is what keeps a pre-finalization rollback free.
//!
//! The manifest's epoch travels with its horizon, because a manifest is
//! published by a fenced compare-and-swap and so is evidence about ownership. A
//! replica that hydrates on behalf of an owner the manifest outranks refuses
//! the append rather than acknowledging a write the real owner will truncate.
//!
//! Hydration is what turns the retention cliff from a dead end into a slow
//! path. A replica past the owner's retained log is still named, with where it
//! stopped and the oldest entry the owner still holds, through
//! [`Server::replicas_beyond_retention`], and that still drives the
//! `replicas-recoverable` readiness condition; what changed is that the
//! condition now clears on its own, because the replica rebuilds from the
//! bucket and the next append it acknowledges moves it back to following.
//! `src/retention.rs` runs the whole cliff under the simulator and asserts the
//! recovery rather than the dead end.
//!
//! What remains unavailable is the narrow case where both are exhausted: a
//! replica beyond the retained log whose partition has never been flushed, or
//! whose manifest is itself behind the gap. That is reported rather than
//! papered over, and the partition runs on the copies it has.

#![forbid(unsafe_code)]

mod auth;
mod aws;
mod bootstrap;
mod config;
mod control;
#[cfg(test)]
mod durability;
#[cfg(test)]
mod forwarding;
mod frame;
mod fs_store;
#[cfg(test)]
mod group_commit;
mod host;
#[cfg(test)]
mod hydration;
mod lease;
#[cfg(test)]
mod linearizability;
mod map_source;
mod metrics;
mod node;
mod pending;
#[cfg(test)]
mod placement;
mod proxy;
mod quota;
#[cfg(test)]
mod quotas;
mod readiness;
mod replication;
#[cfg(test)]
mod retention;
mod runtime;
mod service;
#[cfg(test)]
mod split;
mod status;
mod transport;
mod validate;

pub use aws::{AssumeRoleConfig, DEFAULT_SESSION_DURATION_SECONDS};
pub use config::{
    S3CredentialSource, S3StorageConfig, ServerConfig, DEFAULT_CONTROL_POLL_INTERVAL,
    DEFAULT_FLUSH_INTERVAL, DEFAULT_KEYSPACE, DEFAULT_SWEEP_INTERVAL, DEFAULT_WAL_SEGMENT_BYTES,
};
pub use control::{ControlMapSource, PeerDirectorySync, StatusReporter};
pub use lease::{DEFAULT_LEASE_DURATION, DEFAULT_LEASE_MARGIN};
pub use map_source::{single_node_map, BoxedMapSource, MapSource, StaticMapSource};
pub use node::{max_admin_message_bytes, max_transport_message_bytes, message_bytes_ceiling};
pub use orbita_storage::{DEFAULT_SWEEP_GRACE_MILLIS, DEFAULT_SWEEP_SKEW_MILLIS};
pub use readiness::{ReadinessCondition, ReadinessGate, ReadinessState};
pub use runtime::ServerRuntime;
pub use status::to_status;
pub use transport::{PeerListener, PeerTransport, DEFAULT_PEER_CALL_TIMEOUT};

use crate::node::{DataLayout, Node};
use crate::service::{HealthService, KvService};
use bootstrap::BootstrapOutcome;

use orbita_control::{
    AdminService, BootstrapSpec, ConsensusLog, ControlClient, ControlConfig, ControlService,
    Controller, KeyspaceConfig, RaftLog, RaftMember,
};
use orbita_core::{Error, Result};
use orbita_objectstore::s3::{HttpTransport, HyperTransport, NowMillis, S3Config, S3Store};
use orbita_objectstore::ObjectStore;
use orbita_proto::v1::health_server::HealthServer;
use orbita_proto::v1::kv_server::KvServer;
use orbita_runtime::{Clock, Runtime, ServiceId, Transport};

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

/// A running node.
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
    compactor: tokio::task::JoinHandle<()>,
    sweeper: tokio::task::JoinHandle<()>,
    /// Present only for a node joined to a leader group.
    reporting: Option<tokio::task::JoinHandle<()>>,
    /// The reporting loop's handle, kept so the server can answer which
    /// cluster version is active. Present only alongside `reporting`.
    reporter: Option<StatusReporter<ServerRuntime>>,
    raft: Option<Arc<RaftLog>>,
    control: Option<tokio::task::JoinHandle<()>>,
    _control_executor: Option<runtime::ControlExecutor>,
    shutdown: tokio::sync::oneshot::Sender<()>,
    serving: tokio::task::JoinHandle<()>,
}

impl Server {
    /// Opens this node's partitions and starts serving clients.
    ///
    /// Returns once the socket is bound and every partition the map says this
    /// node holds is open and recovered, so a caller that gets a `Server` back
    /// can send it a request immediately.
    pub async fn start(mut config: ServerConfig) -> Result<Self> {
        let runtime = ServerRuntime::new(config.node_id, &config.data_dir, config.rng_seed)
            .with_peer_call_timeout(config.node_id, config.peer_call_timeout);
        // The config root secret, hashed once here so the plaintext never
        // travels past this boundary. It is overlaid onto both the data-plane
        // cache and the admin surface so it bootstraps a cluster whose auth is
        // on before any credential exists in the log. It is never persisted.
        let root_credential = config
            .root_credential
            .as_deref()
            .map(orbita_control::root_secret_hash);
        // How long the credential cache may go unrefreshed before it stops
        // being trusted. Tied to the poll cadence rather than a bare wall-clock
        // constant so that raising `control_poll_interval` cannot make the
        // bound smaller than a single poll and trip on ordinary jitter: it is a
        // budget of missed polls. At the default 250ms poll that is 10s, which
        // bounds how long a credential revoked during a control-plane outage
        // can keep authorizing. See `auth::Authenticator`.
        const CREDENTIAL_CACHE_STALENESS_POLLS: u32 = 40;
        let credential_cache_max_staleness =
            config.control_poll_interval * CREDENTIAL_CACHE_STALENESS_POLLS;
        // The credential cache the client edge enforces against. Empty until
        // the control loop's first fetch, apart from the root overlay above;
        // harmless while auth is off, and safe while it is on, since an empty
        // cache refuses rather than admits.
        let authenticator = Arc::new(auth::Authenticator::new(
            config.require_auth,
            root_credential,
            credential_cache_max_staleness,
            runtime.clock().clone(),
        ));
        let store: Arc<dyn ObjectStore> = match config.object_store.clone() {
            Some(object_store) => Arc::new(
                connect_object_store(&runtime, object_store)
                    .map_err(|e| Error::Internal(format!("configuring object storage: {e}")))?,
            ),
            None if config.automatic_cluster => {
                return Err(Error::InvalidArgument(
                    "combined clustered nodes require a shared object store for safe bootstrap"
                        .into(),
                ));
            }
            None => {
                let storage_root = config.data_dir.join("storage");
                std::fs::create_dir_all(&storage_root).map_err(|e| {
                    Error::Internal(format!("creating {}: {e}", storage_root.display()))
                })?;
                Arc::new(fs_store::FsStore::new(storage_root))
            }
        };
        let mut node_identity = String::new();
        let mut recovered_membership = None;
        if config.automatic_cluster {
            let address = config.peer_advertise_addr.as_deref().ok_or_else(|| {
                Error::InvalidArgument(
                    "combined clustered nodes need a peer advertise address".into(),
                )
            })?;
            let outcome = bootstrap::bootstrap(
                Arc::clone(&store),
                &config.data_dir,
                &config.cluster_name,
                config.node_id,
                address,
                &config.failure_domain,
                config.voter_eligible,
            )
            .await?;
            let certificate = match outcome {
                BootstrapOutcome::Joined(certificate) => certificate,
                BootstrapOutcome::IdentityMismatch(diagnostic) => {
                    return serve_identity_mismatch(config.listen_addr, &diagnostic).await;
                }
            };
            tracing::info!(cluster_identity = %certificate.cluster_identity, "joined the durable cluster identity");
            node_identity = certificate.node_identity.clone();
            let bootstrap: Vec<_> = certificate
                .voters
                .into_iter()
                .map(|voter| RaftMember {
                    node: voter.node,
                    address: voter.address,
                    node_identity: voter.node_identity,
                })
                .collect();
            let membership = RaftLog::recover_membership(&runtime, &bootstrap).await?;
            if let Some(member) = membership
                .voters
                .iter()
                .chain(&membership.learners)
                .find(|member| member.node == config.node_id)
            {
                if !member.node_identity.is_empty() && member.node_identity != node_identity {
                    let diagnostic = format!(
                        "node id {} is committed for durable identity {}, not {}",
                        config.node_id, member.node_identity, node_identity
                    );
                    return serve_identity_mismatch(config.listen_addr, &diagnostic).await;
                }
            }
            config.peers = membership
                .voters
                .iter()
                .chain(&membership.learners)
                .filter(|member| !member.address.is_empty())
                .map(|member| (member.node, member.address.clone()))
                .collect();
            config.leader_group = membership.voters.iter().map(|member| member.node).collect();
            recovered_membership = Some(membership);
            // Every combined node hosts a dormant Raft state machine. A worker
            // has to be reachable as a learner before the current voters can
            // catch it up and promote it.
            config.leader_member = true;
            config.leader_owns_partitions = true;
        }
        for (node, address) in &config.peers {
            runtime.transport().set_peer(*node, address.clone());
        }
        let control_executor = config
            .leader_member
            .then(runtime::ControlExecutor::start)
            .transpose()?;
        if let Some(executor) = &control_executor {
            for service in [ServiceId::Raft, ServiceId::Control] {
                runtime
                    .transport()
                    .route_service(service, executor.handle());
            }
        }
        let control_runtime = control_executor.as_ref().map_or_else(
            || runtime.clone(),
            |executor| runtime.for_control(executor.handle()),
        );
        let mut raft = None;
        let mut controller = None;
        if config.leader_member {
            let log = match recovered_membership.take() {
                Some(membership) => RaftLog::open_membership(&control_runtime, membership).await?,
                None => RaftLog::open(&control_runtime, &config.leader_group).await?,
            };
            let mut control_config =
                ControlConfig::default().with_voter_target(config.voter_target)?;
            if config.automatic_cluster {
                control_config = control_config.with_voter_management();
            }
            let control =
                Controller::new(control_runtime.clone(), Arc::clone(&log), control_config);
            control_runtime.transport().register(
                ServiceId::Control,
                ControlService::new(control.clone()).require_auth(config.require_auth),
            );
            raft = Some(log);
            controller = Some(control);
        }

        // A leader must be reachable before an election can finish. Workers
        // defer binding until their partition handlers are registered below.
        let mut peer_listener = if config.leader_member {
            Some(
                listen_peer_transport(&runtime, control_executor.as_ref(), config.peer_listen_addr)
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
            if !config.automatic_cluster || config.leader_group.contains(&config.node_id) {
                start_control_plane(&runtime, log, control, &config, &local_address).await?;
            }
        }
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
        readiness.mark(ReadinessCondition::ClusterIdentityMatched);
        readiness.mark(ReadinessCondition::AcceptingOwnership);
        // Auth-policy agreement starts met and is cleared only on a positive
        // disagreement with the leader group, so a cluster that cannot advertise
        // its policy (an older leader, or no leader group at all) is never
        // wedged unready. The control loop below re-evaluates it each poll.
        readiness.mark(ReadinessCondition::AuthPolicyAgreed);
        // A node with no leader group answers to nobody, so the join condition
        // is met by construction rather than left to hang readiness forever.
        // A joined node's condition is marked by the control loop below, on
        // its first report the leader group accepts.
        if control.is_none() {
            readiness.mark(ReadinessCondition::ControlPlaneJoined);
            readiness.mark(ReadinessCondition::ClusterVersionCompatible);
        }

        // Split lifecycle state is fetched before hosts enter routing. A
        // restarted parent must never be published with write, maintenance, or
        // lease gates open while its children already capture an older horizon.
        //
        // Failing start on an error is deliberate: booting blind to a split we
        // cannot rule out is what the gates exist to prevent. The one refusal
        // that is not blindness is a leader that predates the method at all,
        // and `fetch_split_intents` already answers that with the empty set,
        // so an old-binary leader does not crash-loop a new worker.
        let initial_split_intents = match &control {
            Some(client) => Some(client.fetch_split_intents(config.node_id).await?),
            None => None,
        };
        let initial_merge_intents = match &control {
            Some(client) => Some(client.fetch_merge_intents(config.node_id).await?),
            None => None,
        };

        let node = Node::start(
            runtime.clone(),
            config.node_id,
            layout,
            map_source,
            initial_split_intents,
            initial_merge_intents,
            config.lease_duration,
            config.control_poll_interval,
            Arc::clone(&readiness),
            Arc::clone(&authenticator),
        )
        .await?;

        // Peers are served only once every handler is registered, so a peer
        // that connects the instant the port opens cannot be told that a
        // service this node does serve is missing.
        let peers = match peer_listener.take() {
            Some(listener) => listener,
            None => {
                listen_peer_transport(&runtime, control_executor.as_ref(), config.peer_listen_addr)
                    .await
                    .map_err(|e| {
                        Error::Internal(format!(
                            "binding peer port {}: {e}",
                            config.peer_listen_addr
                        ))
                    })?
            }
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
        // Compaction rides its own cadence rather than the flush loop, and no
        // longer runs from inside a client write at all. It rewrites the whole
        // partition under the write lock, so letting a request trigger it put a
        // stall proportional to the partition on whichever write happened to
        // cross the threshold (issue #143).
        let compactor = tokio::spawn(Self::compact_loop(
            Arc::downgrade(&node),
            config.compact_interval,
        ));
        // The orphan sweep rides its own slow cadence rather than the flush
        // loop, because it lists a whole partition prefix and so must run far
        // less often than a flush. It is off unless an operator turns it on:
        // it deletes from the bucket, so it does not start reclaiming on its
        // own. When disabled the task exists but returns at once, so the field
        // stays a plain handle and objects simply accumulate until an operator
        // opts in — the recoverable failure, not the destructive one.
        let sweeper = tokio::spawn(Self::sweep_loop(
            Arc::downgrade(&node),
            config.sweep_enabled,
            config.sweep_interval,
            config.sweep_grace_millis,
            config.sweep_skew_millis,
            config.sweep_dry_run,
        ));

        // Kept before the reporting loop takes ownership of the client, so
        // that an admin call this node cannot answer has somewhere to go.
        let admin_forward = control.clone();

        let mut reporter = None;
        let reporting = control.map(|client| {
            // The role reported here is the one the control plane admits
            // owners by, so a leader group member that also serves partitions
            // has to report as a worker or it would never be given one. See
            // `ServerConfig::leader_owns_partitions`.
            let role = if config.leader_member && !config.leader_owns_partitions {
                orbita_control::NodeRole::Leader
            } else {
                orbita_control::NodeRole::Worker
            };
            let status = StatusReporter::new_with_role(
                client.clone(),
                config.node_id,
                role,
                peer_advertise_addr,
            )
            .with_voter_attributes(
                config.voter_eligible,
                config.failure_domain.clone(),
                node_identity.clone(),
            );
            // The server keeps a handle so version-dependent behaviour can
            // ask which cluster version is active without joining the loop.
            reporter = Some(status.clone());
            let directory =
                PeerDirectorySync::new(client.clone(), runtime.transport().clone(), config.node_id);
            let leader_controller = controller.clone();
            // Keep the credential cache warm on its own timer, but only when
            // authentication is on: a cluster with it off never reads the cache,
            // and skipping the fetch keeps a worker from calling a method an
            // older leader may not serve. Separate from the map loop because it
            // has a different failure story: a fetch that fails leaves the last
            // good set in place rather than touching readiness or routing, so a
            // control plane outage never fails every request closed. A leader
            // member reads its own committed state; a worker pulls it over the
            // wire.
            if config.require_auth {
                tokio::spawn(Self::refresh_credentials_loop(
                    Arc::downgrade(&authenticator),
                    client.clone(),
                    leader_controller.clone(),
                    config.control_poll_interval,
                ));
            }
            // Its own loop, spawned whether or not this node requires auth,
            // because the shape it guards against is a node that requires *no*
            // auth while the cluster does: that node has to learn the cluster's
            // policy and pull itself from rotation, which it cannot do if the
            // check only ran when auth was already on locally.
            tokio::spawn(Self::auth_policy_loop(
                client.clone(),
                config.require_auth,
                config.control_poll_interval,
                Arc::clone(&readiness),
            ));
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
            match &control_executor {
                Some(executor) => executor
                    .handle()
                    .spawn(async move { controller.run().await }),
                None => tokio::spawn(async move { controller.run().await }),
            }
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
        // Both message limits come from the same ceiling `GetLimits` publishes,
        // so the transport accepts exactly what the store tells a client it may
        // send. Tonic's default decode limit is 4 MiB, below the advertised
        // ceiling, so a maximum list page or a value at the largest keyspace
        // cap would be refused by the transport before the handler saw it.
        let message_limit = node::max_transport_message_bytes();
        let service = KvServer::new(KvService::new(Arc::clone(&node)))
            .max_decoding_message_size(message_limit)
            .max_encoding_message_size(message_limit);
        let health = HealthServer::new(HealthService::new(Arc::clone(&readiness)));
        // Every node that knows where the control plane is serves the whole
        // admin surface, whether or not it holds one. A member that is not the
        // current Raft leader, and a worker that hosts no controller at all,
        // both forward: an operator cannot be expected to know which node is
        // leader, since finding that out is what `cluster describe` is for.
        // Only a node with no control plane at all serves no admin surface,
        // and that shape exists only in tests over a static map.
        //
        // Whichever shape it takes, enforcement and the root identity are
        // configured on it the same way: authorization happens on the node the
        // operator dialled, before any forward, so a forwarding worker enforces
        // too. See `AdminService::authorize`.
        let admin = match (controller, admin_forward) {
            (Some(controller), Some(client)) => {
                Some(AdminService::new(controller).with_forwarding(client))
            }
            (Some(controller), None) => Some(AdminService::new(controller)),
            (None, Some(client)) => Some(AdminService::forwarding(client)),
            (None, None) => None,
        }
        .map(|admin| {
            admin
                .require_auth(config.require_auth)
                .root_credential(root_credential)
        });
        let serving = tokio::spawn(async move {
            let shutdown = async {
                // A dropped sender means the `Server` handle went away, so
                // stopping is the right answer to that too.
                let _ = stop.await;
            };
            // The Admin surface is sized apart from the KV ceiling: a cluster
            // description scales with the number of partitions, not with any
            // value or list limit, so the KV ceiling would refuse a valid
            // description of a few hundred partitions. See
            // `max_admin_message_bytes`.
            let admin_limit = node::max_admin_message_bytes();
            let served = match admin {
                Some(admin) => {
                    let admin = admin
                        .into_server()
                        .max_decoding_message_size(admin_limit)
                        .max_encoding_message_size(admin_limit);
                    tonic::transport::Server::builder()
                        .add_service(service)
                        .add_service(health)
                        .add_service(admin)
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

        let role = if config.leader_member {
            "leader"
        } else {
            "worker"
        };
        tracing::info!(node = config.node_id.get(), role, %local_addr, %peer_addr, "node listeners are serving");
        Ok(Self {
            local_addr,
            peer_addr,
            node,
            readiness,
            transport: runtime.transport().clone(),
            peers,
            heartbeat,
            flusher,
            compactor,
            sweeper,
            reporting,
            reporter,
            raft,
            control: control_task,
            _control_executor: control_executor,
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
        self.compactor.abort();
        self.sweeper.abort();
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

    /// Compacts every owned partition that has earned it, on its own cadence.
    ///
    /// A weak reference, so a dropped node stops compacting rather than pinning
    /// itself alive, matching the flush and sweep loops.
    async fn compact_loop(node: std::sync::Weak<Node<ServerRuntime>>, interval: Duration) {
        loop {
            tokio::time::sleep(interval).await;
            let Some(live) = node.upgrade() else {
                return;
            };
            live.compact_owned().await;
        }
    }

    /// Sweeps orphaned objects from this node's owned partitions, forever.
    ///
    /// The operator-tunable backstop against leaked objects: a failed compaction
    /// or an abandoned commit strands objects the manifest no longer references,
    /// and nothing else ever reclaims them. It runs on its own slow cadence
    /// rather than inside the flush loop because it lists a whole partition
    /// prefix per pass. The grace period and skew — and whether this only
    /// reports rather than deletes — come from configuration so an operator can
    /// look before the sweep acts on a real bucket. A weak reference, so a
    /// dropped node stops sweeping rather than pinning itself alive.
    async fn sweep_loop(
        node: std::sync::Weak<Node<ServerRuntime>>,
        enabled: bool,
        interval: Duration,
        grace_millis: u64,
        skew_millis: u64,
        dry_run: bool,
    ) {
        if !enabled {
            // Disabled is the default. Nothing is reclaimed until an operator
            // opts in, which is the safe direction for a loop that deletes.
            return;
        }
        if dry_run {
            tracing::info!(
                "orphan sweep is in dry-run mode: it will report candidates, not delete"
            );
        }
        loop {
            tokio::time::sleep(interval).await;
            let Some(live) = node.upgrade() else {
                return;
            };
            live.sweep_owned(grace_millis, skew_millis, dry_run).await;
        }
    }

    /// Keeps the client edge's credential cache in step with the control plane,
    /// forever.
    ///
    /// This is the worker half of the enforcement the `authenticate` comment in
    /// `orbita_control` describes: rather than a control-plane round trip per
    /// request, the worker pulls the whole credential set on a timer and checks
    /// against it locally. Refreshing the whole set is what makes revocation
    /// take effect on the next request rather than the next connection. A weak
    /// reference to the authenticator, so a dropped node stops refreshing rather
    /// than pinning itself alive. A leader member reads its own committed state
    /// with no transport in the way; a worker fetches it over the wire, and a
    /// fetch that fails leaves the last good set in place.
    async fn refresh_credentials_loop(
        authenticator: std::sync::Weak<auth::Authenticator<<ServerRuntime as Runtime>::Clock>>,
        client: ControlClient<ServerRuntime>,
        leader_controller: Option<Controller<ServerRuntime, RaftLog>>,
        interval: Duration,
    ) {
        loop {
            let Some(authenticator) = authenticator.upgrade() else {
                return;
            };
            match &leader_controller {
                Some(controller) => {
                    authenticator.refresh(controller.credential_snapshot().await);
                }
                None => match client.fetch_credentials().await {
                    Ok(credentials) => {
                        authenticator.refresh(orbita_control::CredentialSnapshot::new(credentials));
                    }
                    Err(error) => {
                        tracing::debug!(%error, "could not refresh the credential cache");
                    }
                },
            }
            drop(authenticator);
            tokio::time::sleep(interval).await;
        }
    }

    /// Keeps this node's readiness honest about the cluster's auth policy,
    /// forever.
    ///
    /// `require_auth` is this node's own setting. Each tick asks the leader
    /// group whether the cluster requires auth and, when it gets an answer,
    /// reconciles [`ReadinessCondition::AuthPolicyAgreed`]: agreement keeps the
    /// node ready, disagreement pulls it from the load balancer. This is what
    /// closes the rolling-update window in which an auth-disabled node would go
    /// on serving unauthenticated requests while its peers reject them. A
    /// leader that cannot report a policy — one predating the method — leaves
    /// the condition untouched, which is the documented residual: the gate only
    /// protects a cluster whose control plane can advertise the policy.
    async fn auth_policy_loop(
        client: ControlClient<ServerRuntime>,
        require_auth: bool,
        interval: Duration,
        readiness: Arc<ReadinessGate>,
    ) {
        loop {
            match client.fetch_auth_policy().await {
                Ok(cluster_requires_auth) => {
                    update_auth_policy_readiness(&readiness, require_auth, cluster_requires_auth);
                }
                Err(error) => {
                    tracing::debug!(%error, "could not learn the cluster's auth policy");
                }
            }
            tokio::time::sleep(interval).await;
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
        let mut convergence_logged = false;
        loop {
            let Some(live) = node.upgrade() else {
                return;
            };
            let node_id = live.id();
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
            // Prepare durable child storage for any split this node holds the
            // parent of, and acknowledge the ones it finished. This is the
            // worker half of the ADR 0009 split: the owner quiesces the parent
            // and publishes both child manifests over its segments, then this
            // reports the ack the leader's completion waits on. Run even when
            // there are no intents, so a partition quiesced for a split the
            // leader later abandoned has its writes reopened.
            match (
                client.fetch_split_intents(node_id).await,
                client.fetch_merge_intents(node_id).await,
            ) {
                (Ok(splits), Ok(merges)) => {
                    let (prepared_splits, prepared_merges) =
                        live.prepare_transition_snapshots(&splits, &merges).await;
                    for (parent, lower, upper) in prepared_splits {
                        if let Err(error) = client
                            .report_split_prepared(node_id, parent, lower, upper)
                            .await
                        {
                            tracing::debug!(
                                %error,
                                partition = parent.get(),
                                "could not report split preparation to the leader group"
                            );
                        }
                    }
                    for generation in prepared_merges {
                        if let Err(error) = client.report_merge_prepared(node_id, generation).await
                        {
                            tracing::debug!(%error, "could not report merge preparation");
                        }
                    }
                }
                (Err(error), _) | (_, Err(error)) => {
                    tracing::debug!(%error, "could not fetch partition transition intents")
                }
            }
            if !convergence_logged && readiness.is_ready() {
                let map = live.map();
                let held_partitions = map.held_by(node_id).count();
                match &leader_controller {
                    Some(controller) => {
                        let leader = controller.log_leader().await.map_or(0, |id| id.get());
                        let raft_role = if leader == node_id.get() {
                            "leader"
                        } else {
                            "follower"
                        };
                        tracing::info!(
                            node = node_id.get(),
                            leader,
                            raft_role,
                            map_version = map.version().get(),
                            held_partitions,
                            "leader member converged with the group and is ready"
                        );
                    }
                    None => {
                        tracing::info!(
                            node = node_id.get(),
                            map_version = map.version().get(),
                            held_partitions,
                            "worker joined the leader group and is ready"
                        );
                    }
                }
                convergence_logged = true;
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
        self.compactor.abort();
        self.sweeper.abort();
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
                // Refetched every pass rather than only after a handoff is
                // committed. Draining aborted the control loop, which is the
                // only thing that otherwise refreshes the map, so a placement
                // the leader group committed a moment before SIGTERM would
                // never reach this node: it would keep an empty peer list, no
                // replica would ever catch up, and the control plane would
                // refuse the handoff for as long as the budget allowed.
                if let Err(error) = node.refresh_map().await {
                    tracing::debug!(%error, "could not refresh the partition map while draining");
                }
                // And with write admission closed, an append is never going to
                // carry the log to a replica that is behind, so the owner
                // gives up the tail no client was told about and pushes what
                // remains. This is what makes the handoff possible rather than
                // merely permitted.
                node.prepare_handoff().await;
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

/// Builds the S3 store this node reads and writes objects through.
///
/// One transport is shared by the store and by every credential provider, so a
/// credential refresh reuses the connection pool the node already has, and one
/// clock feeds both signing and expiry. Both come from the runtime rather than
/// from `SystemTime` and a fresh hyper client, which is what will let a
/// simulated run drive credential expiry and metadata-service failures.
fn connect_object_store(
    runtime: &ServerRuntime,
    config: S3StorageConfig,
) -> orbita_objectstore::ObjectResult<S3Store> {
    let transport: Arc<dyn HttpTransport> = Arc::new(HyperTransport::new());
    let now_millis: NowMillis = {
        let clock = runtime.clock().clone();
        Arc::new(move || clock.now_millis())
    };
    let credentials = aws::credentials_provider(
        runtime.clock(),
        transport.clone(),
        now_millis.clone(),
        &config,
    )?;

    S3Store::new(
        S3Config {
            endpoint: config.endpoint,
            bucket: config.bucket,
            region: config.region,
            // Replaced immediately below. The store's own field is the static
            // path, and this node always goes through a provider so that the
            // static and refreshable cases share one code path.
            credentials: orbita_objectstore::s3::Credentials {
                access_key_id: String::new(),
                secret_access_key: String::new(),
                session_token: None,
            },
            force_path_style: config.force_path_style,
        },
        transport,
        now_millis,
    )
    .map(|store| store.with_credentials_provider(credentials))
}

async fn listen_peer_transport(
    runtime: &ServerRuntime,
    control_executor: Option<&runtime::ControlExecutor>,
    address: SocketAddr,
) -> std::io::Result<PeerListener> {
    match control_executor {
        Some(executor) => {
            runtime
                .transport()
                .listen_on(address, executor.handle())
                .await
        }
        None => runtime.transport().listen(address).await,
    }
}

async fn serve_identity_mismatch(addr: SocketAddr, diagnostic: &str) -> Result<Server> {
    let readiness = Arc::new(ReadinessGate::new());
    for condition in ReadinessCondition::ALL {
        if condition != ReadinessCondition::ClusterIdentityMatched {
            readiness.mark(condition);
        }
    }
    let listener = tokio::net::TcpListener::bind(addr).await.map_err(|error| {
        Error::Internal(format!("binding diagnostic health port {addr}: {error}"))
    })?;
    let local_addr = listener
        .local_addr()
        .map_err(|error| Error::Internal(format!("reading diagnostic health address: {error}")))?;
    let incoming = tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
        .map_err(|error| {
            Error::Internal(format!(
                "serving diagnostic health on {local_addr}: {error}"
            ))
        })?;
    tracing::error!(%diagnostic, %local_addr, "cluster identity mismatch; serving health only");
    let health = HealthServer::new(HealthService::new(readiness));
    let stopped = tonic::transport::Server::builder()
        .add_service(health)
        .serve_with_incoming(incoming)
        .await;
    Err(Error::Internal(match stopped {
        Ok(()) => "diagnostic health listener stopped".into(),
        Err(error) => format!("diagnostic health listener stopped: {error}"),
    }))
}

async fn start_control_plane(
    runtime: &ServerRuntime,
    log: &Arc<RaftLog>,
    controller: &Controller<ServerRuntime, RaftLog>,
    config: &ServerConfig,
    local_address: &str,
) -> Result<()> {
    let voters = &config.leader_group;
    let client = ControlClient::new(runtime.clone(), voters.clone());
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
            let members: Vec<_> = config
                .peers
                .iter()
                .filter(|(node, _)| voters.contains(node))
                .cloned()
                .collect();
            // A voter that also owns partitions is admitted as a worker,
            // because that is the role the control plane hands ownership to.
            // Written into the bootstrap rather than left to the first
            // heartbeat to correct, so `cluster describe` never shows a role
            // that was true for a quarter of a second.
            let (leaders, workers) = if config.leader_owns_partitions {
                (Vec::new(), members)
            } else {
                (members, Vec::new())
            };
            // The first keyspace is born with the cluster, because a bootstrap
            // that leaves no keyspace behind means the first write needs an
            // admin call that the operator did not know to make.
            let mut wanted = config.keyspaces.iter();
            let first = wanted.next().map_or_else(
                || DEFAULT_KEYSPACE.to_string(),
                |name| name.as_str().to_string(),
            );
            controller
                .bootstrap(&BootstrapSpec {
                    keyspace: first,
                    config: KeyspaceConfig::default(),
                    leaders,
                    workers,
                })
                .await?;
            for name in wanted {
                controller
                    .create_keyspace(name.as_str(), KeyspaceConfig::default())
                    .await?;
            }
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

/// Reconciles the auth-policy readiness condition against the cluster's policy.
///
/// `local` is this node's own `require_auth`; `cluster` is what the leader
/// group advertises. They must match for this node to be ready: a node that
/// requires no auth while the cluster does is a bypass hole and is pulled from
/// rotation, and the reverse — auth on while the cluster is off — is failed
/// too, because a disagreement in either direction is a misconfiguration an
/// operator needs surfaced rather than silently served. Agreement marks the
/// condition; only a positive disagreement clears it, so a cluster that cannot
/// report a policy at all never reaches this code and stays ready.
fn update_auth_policy_readiness(readiness: &ReadinessGate, local: bool, cluster: bool) {
    if local == cluster {
        readiness.mark(ReadinessCondition::AuthPolicyAgreed);
    } else {
        tracing::warn!(
            local_require_auth = local,
            cluster_require_auth = cluster,
            "this node's require_auth disagrees with the cluster's; failing readiness so a \
             half-rolled auth change cannot serve a bypass behind the load balancer"
        );
        readiness.clear(ReadinessCondition::AuthPolicyAgreed);
    }
}

#[cfg(test)]
mod leader_readiness_tests {
    use super::*;

    #[tokio::test]
    async fn an_identity_mismatch_binds_health_and_reports_only_that_condition() {
        let address = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap()
        };
        let serving = tokio::spawn(async move {
            serve_identity_mismatch(address, "wrong cluster identity").await
        });
        let endpoint = format!("http://{address}");
        let mut health = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match orbita_proto::v1::health_client::HealthClient::connect(endpoint.clone()).await
                {
                    Ok(client) => break client,
                    Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
                }
            }
        })
        .await
        .expect("diagnostic health listener binds");

        let response = health
            .check_readiness(orbita_proto::v1::CheckReadinessRequest {})
            .await
            .unwrap()
            .into_inner();
        assert!(!response.ready);
        let unmet: Vec<_> = response
            .conditions
            .into_iter()
            .filter(|condition| !condition.met)
            .map(|condition| condition.name)
            .collect();
        assert_eq!(unmet, ["cluster-identity-matched"]);

        serving.abort();
    }

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

    /// Marks everything but auth-policy agreement, so a test of that condition
    /// reads off exactly what the reconciler decided.
    fn all_but_auth_policy() -> ReadinessGate {
        let readiness = ReadinessGate::new();
        for condition in ReadinessCondition::ALL {
            if condition != ReadinessCondition::AuthPolicyAgreed {
                readiness.mark(condition);
            }
        }
        readiness
    }

    #[test]
    fn an_auth_disabled_node_in_an_auth_on_cluster_is_not_ready() {
        // The bypass shape: no auth locally, auth required cluster-wide. This
        // node must be pulled from the load balancer.
        let readiness = all_but_auth_policy();
        update_auth_policy_readiness(&readiness, false, true);
        assert_eq!(
            readiness.state().unmet(),
            vec![ReadinessCondition::AuthPolicyAgreed]
        );
        assert!(!readiness.is_ready());
    }

    #[test]
    fn an_auth_on_node_disagreeing_with_an_auth_off_cluster_is_not_ready() {
        // The safe-but-misconfigured direction is failed too: disagreement in
        // either direction is surfaced rather than served.
        let readiness = all_but_auth_policy();
        update_auth_policy_readiness(&readiness, true, false);
        assert!(!readiness.is_ready());
    }

    #[test]
    fn agreement_on_auth_policy_makes_the_node_ready() {
        for policy in [false, true] {
            let readiness = all_but_auth_policy();
            update_auth_policy_readiness(&readiness, policy, policy);
            assert!(readiness.is_ready(), "agreement on require_auth={policy}");
        }
    }

    #[test]
    fn learning_agreement_clears_an_earlier_disagreement() {
        // A node that briefly disagreed and then a later poll agrees — for
        // instance after its own config caught up — becomes ready again.
        let readiness = all_but_auth_policy();
        update_auth_policy_readiness(&readiness, false, true);
        assert!(!readiness.is_ready());
        update_auth_policy_readiness(&readiness, true, true);
        assert!(readiness.is_ready());
    }
}
