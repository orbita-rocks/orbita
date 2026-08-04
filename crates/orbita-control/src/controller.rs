//! The leader group's decision loop.
//!
//! Everything that proposes a command goes through here, and everything that
//! observes the cluster is stored here. The split between the two is the
//! design: the state machine in [`crate::state`] holds facts that every member
//! agrees on, and the controller holds the leader's own observations, which
//! are not agreed on and must not be.
//!
//! A monotonic clock reading from one node means nothing on another, so
//! replicating heartbeat arrival times would be meaningless as well as
//! expensive. The controller keeps them in memory. A leader that has just
//! taken over therefore starts with no observations and treats every node as
//! freshly heard from, which delays a failover it would otherwise have started
//! immediately. That is the conservative direction: the cost is up to one
//! detection window of extra downtime after a leader change, and the thing it
//! buys is that a new leader can never fence a node on the strength of a clock
//! reading it did not take.

use crate::command::ControlCommand;
use crate::config::ControlConfig;
use crate::consensus::{ConsensusLog, LogIndex};
use crate::membership::{NodeHealth, NodeRole, NodeStatus};
use crate::model::{hash_secret, Credential, Keyspace, KeyspaceConfig, Permission};
use crate::state::{ClusterState, NodeRecord, PartitionPhase};
use crate::version::{binary_speaks, ClusterVersion, CompatibilityRefusal};

use bytes::Bytes;
use orbita_core::{
    Epoch, Error, Lamport, MapVersion, NodeId, PartitionId, PartitionInfo, PartitionMap, Result,
};
use orbita_runtime::{Clock, Runtime, Transport};

use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// What the leader has heard from one node, and when.
#[derive(Debug, Clone)]
struct Observation {
    heard_at_nanos: u64,
    status: NodeStatus,
}

/// What a fresh cluster should be created with.
#[derive(Debug, Clone)]
pub struct BootstrapSpec {
    /// The keyspace to create. A cluster with no keyspace has nowhere to put
    /// anything, so `orbita dev` would otherwise need a second command before
    /// it was usable.
    pub keyspace: String,
    pub config: KeyspaceConfig,
    /// Leader group members to admit, as id and address.
    pub leaders: Vec<(NodeId, String)>,
    /// Workers to admit before the first partition is placed, so that the
    /// keyspace is born with an owner rather than born unavailable.
    pub workers: Vec<(NodeId, String)>,
}

impl BootstrapSpec {
    /// A single-node development cluster: one process that is both the leader
    /// group and the only worker.
    #[must_use]
    pub fn dev(node: NodeId, address: impl Into<String>) -> Self {
        let address = address.into();
        Self {
            keyspace: "default".into(),
            config: KeyspaceConfig::default(),
            leaders: vec![(node, address.clone())],
            workers: vec![(node, address)],
        }
    }
}

/// A node as an operator sees it.
#[derive(Debug, Clone)]
pub struct NodeView {
    pub record: NodeRecord,
    /// How long since the leader last heard from it, in milliseconds. `None`
    /// when this leader has never heard from it at all.
    pub silent_for_millis: Option<u64>,
    pub is_control_leader: bool,
}

/// A partition as an operator sees it, with the observations the map does not
/// carry.
#[derive(Debug, Clone)]
pub struct PartitionView {
    pub info: PartitionInfo,
    pub phase: PartitionPhase,
    /// The owner's reported durable Lamport, which is the partition's
    /// committed position.
    pub committed_lamport: Lamport,
    pub size_bytes: u64,
    pub replica_progress: Vec<(NodeId, Lamport)>,
}

/// Everything `DescribeCluster` answers with.
#[derive(Debug, Clone)]
pub struct ClusterView {
    pub nodes: Vec<NodeView>,
    pub partitions: Vec<PartitionView>,
    /// The active cluster version, so `cluster describe` answers "did the
    /// finalize land" without a second command.
    pub cluster_version: ClusterVersion,
}

/// What `finalize-upgrade` committed: where the cluster was and where it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinalizedUpgrade {
    pub previous: ClusterVersion,
    pub active: ClusterVersion,
}

/// The replicated control plane's decision on one status report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistrationOutcome {
    Accepted(MapVersion),
    Incompatible(CompatibilityRefusal),
}

struct Inner {
    state: ClusterState,
    applied: LogIndex,
    /// The outcome of applying each recent entry, so the task that proposed it
    /// learns what its command did even when another task did the applying.
    results: BTreeMap<LogIndex, Result<()>>,
    observations: BTreeMap<NodeId, Observation>,
    /// When this leader first saw each partition in the fenced phase, on its
    /// own monotonic clock. Not replicated, because the lease wait it drives
    /// is measured from an instant only this node observed.
    fenced_since: BTreeMap<PartitionId, u64>,
    /// When this controller started observing. A node it has never heard from
    /// is timed from here rather than from the beginning of time.
    observing_since: u64,
}

/// The leader group's decision loop and the API around it.
///
/// Cheap to clone; every clone shares one state machine and one log, so the
/// admin service, the peer service, and the sweep task can each hold one.
pub struct Controller<R: Runtime, L: ConsensusLog> {
    runtime: R,
    log: Arc<L>,
    config: ControlConfig,
    inner: Arc<Mutex<Inner>>,
}

impl<R: Runtime, L: ConsensusLog> Clone for Controller<R, L> {
    fn clone(&self) -> Self {
        Self {
            runtime: self.runtime.clone(),
            log: Arc::clone(&self.log),
            config: self.config.clone(),
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<R: Runtime, L: ConsensusLog> Controller<R, L> {
    #[must_use]
    pub fn new(runtime: R, log: Arc<L>, config: ControlConfig) -> Self {
        let observing_since = runtime.clock().monotonic_nanos();
        Self {
            runtime,
            log,
            config,
            inner: Arc::new(Mutex::new(Inner {
                state: ClusterState::new(),
                applied: 0,
                results: BTreeMap::new(),
                observations: BTreeMap::new(),
                fenced_since: BTreeMap::new(),
                observing_since,
            })),
        }
    }

    #[must_use]
    pub fn config(&self) -> &ControlConfig {
        &self.config
    }

    #[must_use]
    pub fn node(&self) -> NodeId {
        self.runtime.transport().local_node()
    }

    /// Whether this node may take decisions right now.
    pub async fn log_is_leader(&self) -> bool {
        self.log.is_leader().await
    }

    /// Who to redirect to when this node may not.
    pub async fn log_leader(&self) -> Option<NodeId> {
        self.log.leader().await
    }

    /// Replays the log into the state machine.
    ///
    /// Called once on start. Everything else applies as it proposes, so this
    /// is the only place a member catches up on decisions it did not make.
    pub async fn recover(&self) -> Result<()> {
        let committed = self.log.commit_index().await;
        self.apply_through(committed).await.map(|_| ())
    }

    /// Proposes a command and returns what applying it decided.
    ///
    /// The error from a rejected command comes back through here rather than
    /// from the propose, because a rejection is a decision the whole cluster
    /// agreed on and not a failure to reach anybody.
    pub async fn submit(&self, command: ControlCommand) -> Result<()> {
        let index = self.log.propose(command).await?;
        self.apply_through(index).await
    }

    async fn apply_through(&self, target: LogIndex) -> Result<()> {
        let mut inner = self.inner.lock().await;
        if inner.applied < target {
            let entries = self.log.subscribe(inner.applied).await?;
            for entry in entries {
                let outcome = inner.state.apply(&entry.command);
                inner.applied = entry.index;
                inner.results.insert(entry.index, outcome);
            }
        }
        let result = inner.results.remove(&target).unwrap_or(Ok(()));
        // Results are only interesting to whoever proposed the entry. A
        // proposer whose future was dropped never collects, so old ones are
        // swept rather than kept forever.
        let floor = inner.applied.saturating_sub(1024);
        inner.results.retain(|index, _| *index > floor);
        result
    }

    /// A copy of the routing table.
    pub async fn partition_map(&self) -> PartitionMap {
        self.inner.lock().await.state.map().clone()
    }

    /// The routing table, but only if it has moved since `have`.
    ///
    /// A worker polls this on a timer, and the map is the largest thing the
    /// control plane sends. Comparing versions first means a healthy cluster's
    /// steady state costs a few bytes per poll instead of the whole map.
    pub async fn partition_map_if_newer(&self, have: MapVersion) -> Option<PartitionMap> {
        let inner = self.inner.lock().await;
        if inner.state.map_version() > have {
            Some(inner.state.map().clone())
        } else {
            None
        }
    }

    pub async fn map_version(&self) -> MapVersion {
        self.inner.lock().await.state.map_version()
    }

    /// The active cluster version.
    pub async fn cluster_version(&self) -> ClusterVersion {
        self.inner.lock().await.state.cluster_version()
    }

    /// Advances the active cluster version to the newest one every live node
    /// can speak, which is what `orbita cluster finalize-upgrade` runs.
    ///
    /// Dead nodes do not vote: they are the nodes a failover has already
    /// written off, and refusing to finalize on their account would make a
    /// dead node's last act pinning the cluster to an old version forever. A
    /// suspect node does vote, because it may only be slow, and finalizing
    /// past a node that comes back is exactly the mixed-version state the
    /// window exists to avoid.
    pub async fn finalize_upgrade(&self) -> Result<FinalizedUpgrade> {
        let (previous, target) = {
            let inner = self.inner.lock().await;
            let previous = inner.state.cluster_version();
            let live: Vec<&NodeRecord> = inner
                .state
                .nodes()
                .filter(|n| n.health != NodeHealth::Dead)
                .collect();
            if live.is_empty() {
                return Err(Error::InvalidArgument(
                    "no live node has registered, so there is nothing to check the upgrade against"
                        .into(),
                ));
            }

            // The newest version everyone can reach. Taking the minimum of
            // the maxima is what makes a half-upgraded cluster answer "not
            // yet" instead of advancing past the stragglers.
            let target = live
                .iter()
                .map(|n| n.speaks.max)
                .min()
                .unwrap_or(ClusterVersion::ZERO);

            if target <= previous {
                let laggards: Vec<String> = live
                    .iter()
                    .filter(|n| n.speaks.max <= previous)
                    .map(|n| format!("node {} speaks {}", n.id, n.speaks))
                    .collect();
                return Err(Error::InvalidArgument(format!(
                    "the cluster is already at version {previous} and no newer version is \
                     speakable by every live node: {}",
                    laggards.join(", ")
                )));
            }

            // A node whose window starts above the target has skipped past
            // it, which the one-version window does not allow.
            let skipped: Vec<String> = live
                .iter()
                .filter(|n| !n.speaks.contains(target))
                .map(|n| format!("node {} speaks {}", n.id, n.speaks))
                .collect();
            if !skipped.is_empty() {
                return Err(Error::InvalidArgument(format!(
                    "version {target} is not speakable by every live node ({}); upgrades move \
                     one version at a time, so roll those nodes to a binary that speaks {target} \
                     first",
                    skipped.join(", ")
                )));
            }
            (previous, target)
        };

        self.submit(ControlCommand::SetClusterVersion {
            version: target,
            expect: previous,
        })
        .await?;
        tracing::info!(%previous, active = %target, "finalized the cluster upgrade");
        Ok(FinalizedUpgrade {
            previous,
            active: target,
        })
    }

    /// Every node the cluster knows, and where peers reach it.
    ///
    /// Addresses are what each node reported about itself, so a node that
    /// moves corrects this on its next heartbeat without an operator having to
    /// notice. Workers poll it to fill in their peer directory, since the
    /// partition map names owners by id and nothing else says how to dial one.
    pub async fn node_addresses(&self) -> Vec<(NodeId, String)> {
        self.inner
            .lock()
            .await
            .state
            .nodes()
            .filter(|record| !record.address.is_empty())
            .map(|record| (record.id, record.address.clone()))
            .collect()
    }

    /// A snapshot of the agreed state, for callers that need more than the
    /// map.
    pub async fn snapshot(&self) -> ClusterState {
        self.inner.lock().await.state.clone()
    }

    /// Records a node's self-report and admits it to the cluster if it is new.
    ///
    /// Returns the current map version so the reporting node learns in the
    /// same round trip whether the map it is routing on is stale.
    pub async fn record_status(
        &self,
        node: NodeId,
        status: NodeStatus,
    ) -> Result<RegistrationOutcome> {
        let now = self.runtime.clock().monotonic_nanos();
        let (known, mut refusal) = {
            let mut inner = self.inner.lock().await;
            let known = inner.state.node(node).cloned();
            let refusal = inner.state.compatibility_refusal(status.speaks);
            if known.is_some() || refusal.is_none() {
                inner.observations.insert(
                    node,
                    Observation {
                        heard_at_nanos: now,
                        status: status.clone(),
                    },
                );
            }
            (known, refusal)
        };
        let known_member = known.is_some();
        if !known_member {
            if let Some(refusal) = refusal {
                return Ok(RegistrationOutcome::Incompatible(refusal));
            }
        }
        // A changed speakable range is a re-registration too: it is what a
        // rolling update looks like from here, and finalize-upgrade decides
        // from the stored range.
        let needs_registration = known.as_ref().is_none_or(|record| {
            record.address != status.address
                || record.role != status.role
                || record.speaks != status.speaks
        });
        let needs_revival = known
            .as_ref()
            .is_some_and(|record| record.health != NodeHealth::Healthy);

        if needs_registration && refusal.is_none() {
            if let Err(error) = self
                .submit(ControlCommand::RegisterNode {
                    node,
                    role: status.role,
                    address: status.address.clone(),
                    speaks: status.speaks,
                })
                .await
            {
                // Finalization can race the proposal. The state machine is the
                // authority, and this read turns its refusal back into the
                // same structured answer as the fast path above.
                let mut inner = self.inner.lock().await;
                refusal = inner.state.compatibility_refusal(status.speaks);
                if refusal.is_none() {
                    return Err(error);
                }
                if !known_member {
                    inner.observations.remove(&node);
                }
            }
        }
        if needs_revival {
            // A node that came back has to be marked healthy through the log,
            // or it stays out of every placement decision forever.
            self.submit(ControlCommand::SetHealth {
                node,
                health: NodeHealth::Healthy,
            })
            .await?;
        }

        match refusal {
            Some(refusal) => Ok(RegistrationOutcome::Incompatible(refusal)),
            None => Ok(RegistrationOutcome::Accepted(self.map_version().await)),
        }
    }

    /// Creates the first keyspace of a fresh cluster, with its single
    /// unbounded partition and an owner.
    ///
    /// Returns false when the cluster already has state, so this is safe to
    /// call on every start. That matters more than its size suggests: it is
    /// the path a single-command development cluster takes, and it is the one
    /// place where "no state at all" has to turn into "a valid map".
    pub async fn bootstrap(&self, spec: &BootstrapSpec) -> Result<bool> {
        self.recover().await?;
        if !self.snapshot().await.is_fresh() {
            return Ok(false);
        }

        // A fresh cluster's active version is the bootstrapping binary's own.
        // Set first, so there is no committed state in which the cluster has
        // nodes and a keyspace but no version to speak. The nodes named in
        // the spec are admitted with this binary's window; any that run a
        // different binary correct the record on their first heartbeat.
        self.submit(ControlCommand::SetClusterVersion {
            version: binary_speaks().max,
            expect: ClusterVersion::ZERO,
        })
        .await?;

        for (node, address) in &spec.leaders {
            self.submit(ControlCommand::RegisterNode {
                node: *node,
                role: NodeRole::Leader,
                address: address.clone(),
                speaks: binary_speaks(),
            })
            .await?;
        }
        for (node, address) in &spec.workers {
            self.submit(ControlCommand::RegisterNode {
                node: *node,
                role: NodeRole::Worker,
                address: address.clone(),
                speaks: binary_speaks(),
            })
            .await?;
        }

        self.create_keyspace(&spec.keyspace, spec.config.clone())
            .await?;
        Ok(true)
    }

    pub async fn create_keyspace(&self, name: &str, config: KeyspaceConfig) -> Result<Keyspace> {
        let command = {
            let inner = self.inner.lock().await;
            let candidates = inner.state.placement_candidates();
            let (owner, replicas) = split_placement(&candidates, self.config.replication_factor);
            ControlCommand::CreateKeyspace {
                id: inner.state.next_keyspace_id(),
                name: name.to_string(),
                config,
                created_at_millis: self.runtime.clock().now_millis(),
                first_partition: inner.state.next_partition_id(),
                owner,
                replicas,
            }
        };
        self.submit(command).await?;
        self.snapshot()
            .await
            .keyspace_by_name(name)
            .cloned()
            .ok_or(Error::KeyspaceNotFound)
    }

    pub async fn update_keyspace(&self, name: &str, config: KeyspaceConfig) -> Result<Keyspace> {
        let id = self
            .snapshot()
            .await
            .keyspace_by_name(name)
            .ok_or(Error::KeyspaceNotFound)?
            .id;
        self.submit(ControlCommand::UpdateKeyspace { id, config })
            .await?;
        self.snapshot()
            .await
            .keyspace_by_name(name)
            .cloned()
            .ok_or(Error::KeyspaceNotFound)
    }

    pub async fn delete_keyspace(&self, name: &str) -> Result<()> {
        let id = self
            .snapshot()
            .await
            .keyspace_by_name(name)
            .ok_or(Error::KeyspaceNotFound)?
            .id;
        self.submit(ControlCommand::DeleteKeyspace { id }).await
    }

    pub async fn list_keyspaces(&self) -> Vec<Keyspace> {
        self.snapshot().await.keyspaces().cloned().collect()
    }

    /// Issues a credential and returns its id and its secret.
    ///
    /// The secret is shown once and never stored, only its hash. It is drawn
    /// from the operating system rather than from `orbita_runtime::Rng`, whose
    /// documentation says in as many words that it must never be used for
    /// credentials. That is the one place in this crate that does not go
    /// through the runtime seam, and it is safe to except because nothing
    /// about a simulated run depends on which secret was issued.
    pub async fn create_credential(
        &self,
        keyspaces: Vec<String>,
        permissions: Vec<Permission>,
        description: String,
        expires_at_millis: Option<u64>,
    ) -> Result<(String, String)> {
        let id = format!("cred-{}", random_hex(8)?);
        let secret = random_hex(32)?;
        let credential = Credential {
            id: id.clone(),
            secret_hash: hash_secret(&secret),
            keyspaces,
            permissions,
            description,
            created_at_millis: self.runtime.clock().now_millis(),
            expires_at_millis,
        };
        self.submit(ControlCommand::CreateCredential {
            credential: Box::new(credential),
        })
        .await?;
        Ok((id, secret))
    }

    pub async fn revoke_credential(&self, id: &str) -> Result<()> {
        self.submit(ControlCommand::RevokeCredential { id: id.to_string() })
            .await
    }

    /// Checks a credential against a keyspace and an operation.
    ///
    /// This is the leader group's copy of the check. The worker enforces the
    /// same rule against its cached view, because the alternative is a control
    /// plane round trip on every request, which would put the control plane on
    /// the data path and make a control plane outage a data plane outage.
    pub async fn authenticate(
        &self,
        id: &str,
        secret: &str,
        keyspace: &str,
        permission: Permission,
    ) -> Result<()> {
        let now = self.runtime.clock().now_millis();
        let inner = self.inner.lock().await;
        let credential = inner.state.credential(id).ok_or(Error::Unauthenticated)?;
        if credential.secret_hash != hash_secret(secret) {
            return Err(Error::Unauthenticated);
        }
        if !credential.allows(keyspace, permission, now) {
            return Err(Error::PermissionDenied);
        }
        Ok(())
    }

    /// Splits a partition at `at`, or at the midpoint the owner suggests when
    /// `at` is `None`.
    ///
    /// The map change is a single committed entry, so there is no instant at
    /// which a key in the parent's range is unowned or doubly owned. Both
    /// children start at the parent's epoch plus one, which fences any write
    /// the owner had in flight against the parent.
    ///
    /// What is not here yet is the data side of a split: the owner has to
    /// quiesce, flush, and report a boundary before the children can accept
    /// writes independently. The metadata operation is the part that has to be
    /// atomic, and it is; the handshake is a worker protocol.
    pub async fn split_partition(
        &self,
        partition: PartitionId,
        at: Option<Bytes>,
    ) -> Result<(PartitionId, PartitionId)> {
        let (command, lower, upper) = {
            let inner = self.inner.lock().await;
            let info = inner
                .state
                .map()
                .partition(partition)
                .ok_or_else(|| Error::InvalidArgument(format!("no partition {partition}")))?
                .clone();
            let at = match at {
                Some(at) => at,
                None => suggested_split_key(&info).ok_or_else(|| {
                    Error::InvalidArgument(
                        "no split key was given and none could be derived from the range".into(),
                    )
                })?,
            };
            let lower = inner.state.next_partition_id();
            let upper = lower.next();
            (
                ControlCommand::SplitPartition {
                    parent: partition,
                    at,
                    lower,
                    upper,
                    expect_epoch: info.epoch,
                },
                lower,
                upper,
            )
        };
        self.submit(command).await?;
        Ok((lower, upper))
    }

    /// Hands a partition to one of its replicas, deliberately.
    ///
    /// This is the same two-step sequence as a failover, for the same reason:
    /// the deposed owner has read leases out, and they have to be gone before
    /// the new owner accepts a write. A planned transfer could shorten the
    /// wait by having the old owner revoke its leases explicitly, and that is
    /// worth doing once the worker side exists. Until then it pays the same
    /// drain a crash does.
    pub async fn transfer_ownership(&self, partition: PartitionId, to: NodeId) -> Result<()> {
        let info = {
            let inner = self.inner.lock().await;
            inner
                .state
                .map()
                .partition(partition)
                .ok_or_else(|| Error::InvalidArgument(format!("no partition {partition}")))?
                .clone()
        };
        if !info.replicas.contains(&to) {
            return Err(Error::InvalidArgument(format!(
                "node {to} does not hold partition {partition}, so it would have no data to serve"
            )));
        }
        let deposed = info.owner;
        self.submit(ControlCommand::FencePartition {
            partition,
            expect_epoch: info.epoch,
        })
        .await?;

        self.runtime.clock().sleep(self.config.lease_drain()).await;

        let fenced_epoch = self.epoch_of(partition).await?;
        let mut replicas: Vec<NodeId> =
            info.replicas.iter().copied().filter(|r| *r != to).collect();
        if let Some(old) = deposed {
            if old != to && !replicas.contains(&old) {
                replicas.push(old);
            }
        }
        self.submit(ControlCommand::AssignOwner {
            partition,
            owner: to,
            replicas,
            expect_epoch: fenced_epoch,
        })
        .await
    }

    async fn epoch_of(&self, partition: PartitionId) -> Result<Epoch> {
        self.inner
            .lock()
            .await
            .state
            .map()
            .partition(partition)
            .map(|p| p.epoch)
            .ok_or_else(|| Error::InvalidArgument(format!("no partition {partition}")))
    }

    /// Partitions that have grown past the split threshold.
    ///
    /// Reported rather than acted on. A split needs a boundary key, and the
    /// only thing that can choose a good one is the owner, which is the only
    /// node that knows how the keys are distributed inside the range. A
    /// midpoint chosen from the range bounds alone would routinely produce two
    /// lopsided halves and a second split immediately after.
    pub async fn split_candidates(&self) -> Vec<PartitionId> {
        let inner = self.inner.lock().await;
        let mut out = Vec::new();
        for info in inner.state.map().partitions() {
            let size = info
                .owner
                .and_then(|o| inner.observations.get(&o))
                .and_then(|obs| obs.status.progress(info.id))
                .map_or(0, |p| p.size_bytes);
            if size >= self.config.split_threshold_bytes {
                out.push(info.id);
            }
        }
        out
    }

    /// Everything `DescribeCluster` answers with.
    pub async fn view(&self) -> ClusterView {
        let control_leader = self.log.leader().await;
        let now = self.runtime.clock().monotonic_nanos();
        let inner = self.inner.lock().await;

        let nodes = inner
            .state
            .nodes()
            .map(|record| NodeView {
                record: record.clone(),
                silent_for_millis: inner
                    .observations
                    .get(&record.id)
                    .map(|obs| (now.saturating_sub(obs.heard_at_nanos)) / 1_000_000),
                is_control_leader: control_leader == Some(record.id),
            })
            .collect();

        let partitions = inner
            .state
            .map()
            .partitions()
            .map(|info| {
                let owner_progress = info
                    .owner
                    .and_then(|o| inner.observations.get(&o))
                    .and_then(|obs| obs.status.progress(info.id));
                PartitionView {
                    info: info.clone(),
                    phase: inner
                        .state
                        .phase(info.id)
                        .unwrap_or(PartitionPhase::Unowned),
                    committed_lamport: owner_progress.map_or(Lamport::ZERO, |p| p.durable_lamport),
                    size_bytes: owner_progress.map_or(0, |p| p.size_bytes),
                    replica_progress: info
                        .replicas
                        .iter()
                        .map(|r| {
                            let applied = inner
                                .observations
                                .get(r)
                                .and_then(|obs| obs.status.progress(info.id))
                                .map_or(Lamport::ZERO, |p| p.applied_lamport);
                            (*r, applied)
                        })
                        .collect(),
                }
            })
            .collect();

        ClusterView {
            nodes,
            partitions,
            cluster_version: inner.state.cluster_version(),
        }
    }

    /// One pass of the decision loop.
    ///
    /// Exposed rather than hidden inside [`Controller::run`] so that a
    /// simulation can drive it a step at a time and assert what changed
    /// between steps, which is how the failover ordering is actually verified.
    pub async fn tick(&self) -> Result<()> {
        if !self.log.is_leader().await {
            return Ok(());
        }
        self.refresh_health().await?;
        self.fence_dead_owners().await?;
        self.promote_drained_partitions().await?;
        self.place_unowned_partitions().await?;
        self.repair_replica_sets().await?;
        Ok(())
    }

    /// Runs the decision loop until the process ends.
    pub async fn run(&self) {
        loop {
            if let Err(e) = self.tick().await {
                tracing::warn!(error = %e, "control sweep failed; retrying next interval");
            }
            self.runtime.clock().sleep(self.config.sweep_interval).await;
        }
    }

    async fn refresh_health(&self) -> Result<()> {
        let now = self.runtime.clock().monotonic_nanos();
        let changes: Vec<(NodeId, NodeHealth)> = {
            let inner = self.inner.lock().await;
            inner
                .state
                .nodes()
                .filter_map(|record| {
                    let silence = now.saturating_sub(
                        inner
                            .observations
                            .get(&record.id)
                            .map_or(inner.observing_since, |obs| obs.heard_at_nanos),
                    );
                    let health = classify(silence, &self.config);
                    (health != record.health).then_some((record.id, health))
                })
                .collect()
        };

        for (node, health) in changes {
            tracing::info!(%node, ?health, "node health changed");
            self.submit(ControlCommand::SetHealth { node, health })
                .await?;
        }
        Ok(())
    }

    async fn fence_dead_owners(&self) -> Result<()> {
        let doomed: Vec<(PartitionId, Epoch)> = {
            let inner = self.inner.lock().await;
            inner
                .state
                .map()
                .partitions()
                .filter(|info| {
                    info.owner.is_some_and(|owner| {
                        inner
                            .state
                            .node(owner)
                            .is_some_and(|n| n.health == NodeHealth::Dead)
                    })
                })
                .map(|info| (info.id, info.epoch))
                .collect()
        };

        let now = self.runtime.clock().monotonic_nanos();
        for (partition, expect_epoch) in doomed {
            // The epoch bump and the loss of the owner are one entry, and it
            // commits strictly before anything is promoted. That ordering is
            // what stops a deposed owner that never noticed from writing at
            // its old epoch.
            match self
                .submit(ControlCommand::FencePartition {
                    partition,
                    expect_epoch,
                })
                .await
            {
                Ok(()) => {
                    tracing::info!(%partition, "fenced a dead owner");
                    self.inner.lock().await.fenced_since.insert(partition, now);
                }
                // Another sweep or another leader got there first. The
                // partition is fenced either way, which is all this pass
                // wanted.
                Err(Error::StaleEpoch { .. } | Error::InvalidArgument(_)) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    async fn promote_drained_partitions(&self) -> Result<()> {
        let now = self.runtime.clock().monotonic_nanos();
        let drain = self.config.lease_drain().as_nanos() as u64;

        let ready: Vec<ControlCommand> = {
            let mut inner = self.inner.lock().await;
            let fenced: Vec<PartitionInfo> = inner
                .state
                .map()
                .partitions()
                .filter(|info| {
                    info.owner.is_none()
                        && matches!(
                            inner.state.phase(info.id),
                            Some(PartitionPhase::Fenced { .. })
                        )
                })
                .cloned()
                .collect();

            let mut commands = Vec::new();
            for info in fenced {
                // A leader that inherited this partition mid-failover has no
                // record of when the fence happened, so it starts the wait
                // now. Waiting longer than necessary costs availability;
                // waiting less could let a replica serve a pre-failover value.
                let since = *inner.fenced_since.entry(info.id).or_insert(now);
                if now.saturating_sub(since) < drain {
                    continue;
                }
                let Some(owner) = best_candidate(&inner, &info) else {
                    // No replica has reported its position yet. Promoting one
                    // blind could pick a node that is behind and lose an
                    // acknowledged write, so the partition stays unavailable
                    // until somebody reports.
                    tracing::warn!(
                        partition = %info.id,
                        "no eligible replica has reported a durable position; not promoting"
                    );
                    continue;
                };
                commands.push(ControlCommand::AssignOwner {
                    partition: info.id,
                    owner,
                    replicas: info
                        .replicas
                        .iter()
                        .copied()
                        .filter(|r| *r != owner)
                        .collect(),
                    expect_epoch: info.epoch,
                });
            }
            commands
        };

        for command in ready {
            let (partition, owner) = match &command {
                ControlCommand::AssignOwner {
                    partition, owner, ..
                } => (*partition, *owner),
                _ => continue,
            };
            match self.submit(command).await {
                Ok(()) => {
                    tracing::info!(%partition, %owner, "promoted a replica to owner");
                    self.inner.lock().await.fenced_since.remove(&partition);
                }
                Err(Error::StaleEpoch { .. }) => {}
                Err(e @ Error::InvalidArgument(_)) => {
                    tracing::warn!(%partition, %owner, error = %e, "selected owner was refused");
                    return Err(e);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    async fn place_unowned_partitions(&self) -> Result<()> {
        let unowned: Vec<PartitionId> = {
            let inner = self.inner.lock().await;
            inner
                .state
                .map()
                .partitions()
                .filter(|info| {
                    info.owner.is_none()
                        && inner.state.phase(info.id) == Some(PartitionPhase::Unowned)
                })
                .map(|info| info.id)
                .collect()
        };

        for partition in unowned {
            // Placement is recomputed per partition so that a cluster coming
            // up with several unowned partitions spreads them instead of
            // stacking them all on whoever happened to be least loaded first.
            //
            // A partition that never had an owner never granted a read lease,
            // so there is nothing to drain and no reason to make a new cluster
            // wait before it can accept writes.
            let command = {
                let inner = self.inner.lock().await;
                let Some(info) = inner.state.map().partition(partition) else {
                    continue;
                };
                let candidates = inner.state.placement_candidates();
                let (owner, replicas) =
                    split_placement(&candidates, self.config.replication_factor);
                let Some(owner) = owner else { continue };
                ControlCommand::AssignOwner {
                    partition,
                    owner,
                    replicas,
                    expect_epoch: info.epoch,
                }
            };
            match self.submit(command).await {
                Ok(()) | Err(Error::StaleEpoch { .. } | Error::InvalidArgument(_)) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    async fn repair_replica_sets(&self) -> Result<()> {
        let want = self.config.replication_factor.saturating_sub(1);
        let commands: Vec<ControlCommand> = {
            let inner = self.inner.lock().await;
            let candidates = inner.state.placement_candidates();
            inner
                .state
                .map()
                .partitions()
                .filter(|info| inner.state.phase(info.id) == Some(PartitionPhase::Serving))
                .filter_map(|info| {
                    let owner = info.owner?;
                    let mut next: Vec<NodeId> = info
                        .replicas
                        .iter()
                        .copied()
                        .filter(|r| {
                            inner
                                .state
                                .node(*r)
                                .is_some_and(|n| n.health != NodeHealth::Dead)
                        })
                        .collect();
                    for candidate in &candidates {
                        if next.len() >= want {
                            break;
                        }
                        if *candidate != owner && !next.contains(candidate) {
                            next.push(*candidate);
                        }
                    }
                    (next != info.replicas).then(|| ControlCommand::SetReplicas {
                        partition: info.id,
                        replicas: next,
                        expect_epoch: info.epoch,
                    })
                })
                .collect()
        };

        for command in commands {
            match self.submit(command).await {
                Ok(()) | Err(Error::StaleEpoch { .. } | Error::InvalidArgument(_)) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

/// The most caught-up replica eligible for new ownership, or `None` if none
/// has reported.
///
/// Highest durable Lamport wins, because that is what bounds the writes the
/// cluster has acknowledged: the WAL acknowledges at two of three, so any
/// acknowledged entry is on at least one surviving node, and promoting the
/// furthest survivor cannot lose one. Ties break on node id so the choice is
/// reproducible from a seed.
fn best_candidate(inner: &Inner, info: &PartitionInfo) -> Option<NodeId> {
    let mut best: Option<(Lamport, NodeId)> = None;
    for replica in &info.replicas {
        if inner.state.new_ownership_eligibility(*replica).is_err() {
            continue;
        }
        let Some(progress) = inner
            .observations
            .get(replica)
            .and_then(|obs| obs.status.progress(info.id))
        else {
            continue;
        };
        let candidate = (progress.durable_lamport, *replica);
        let wins = match best {
            None => true,
            Some((lamport, node)) => {
                candidate.0 > lamport || (candidate.0 == lamport && candidate.1 < node)
            }
        };
        if wins {
            best = Some(candidate);
        }
    }
    best.map(|(_, node)| node)
}

/// Splits an ordered candidate list into an owner and a replica set.
fn split_placement(
    candidates: &[NodeId],
    replication_factor: usize,
) -> (Option<NodeId>, Vec<NodeId>) {
    let Some((owner, rest)) = candidates.split_first() else {
        return (None, Vec::new());
    };
    let replicas = rest
        .iter()
        .copied()
        .take(replication_factor.saturating_sub(1))
        .collect();
    (Some(*owner), replicas)
}

fn classify(silence_nanos: u64, config: &ControlConfig) -> NodeHealth {
    if silence_nanos >= config.dead_after.as_nanos() as u64 {
        NodeHealth::Dead
    } else if silence_nanos >= config.suspect_after.as_nanos() as u64 {
        NodeHealth::Suspect
    } else {
        NodeHealth::Healthy
    }
}

/// A boundary key derived from the range alone, used only when the caller did
/// not supply one.
///
/// This is a poor split point and it is meant to be: it exists so a manual
/// split with no key does something rather than failing, and the doc comment
/// on [`Controller::split_candidates`] explains why a good one has to come
/// from the owner.
fn suggested_split_key(info: &PartitionInfo) -> Option<Bytes> {
    let start = info.range.start();
    match info.range.end() {
        // An unbounded range has no midpoint to compute, so the split point is
        // one byte past the start, which at least produces two legal ranges.
        None => {
            let mut key = start.to_vec();
            key.push(0);
            Some(Bytes::from(key))
        }
        Some(end) => {
            let mut key = start.to_vec();
            key.push(0);
            (key.as_slice() < end).then(|| Bytes::from(key))
        }
    }
}

fn random_hex(bytes: usize) -> Result<String> {
    let mut buf = vec![0u8; bytes];
    getrandom::getrandom(&mut buf)
        .map_err(|e| Error::Internal(format!("no operating system entropy: {e}")))?;
    Ok(buf.iter().map(|b| format!("{b:02x}")).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn silence_moves_a_node_through_suspect_before_dead() {
        let config = ControlConfig::default();
        let nanos = |ms: u64| ms * 1_000_000;
        assert_eq!(classify(nanos(0), &config), NodeHealth::Healthy);
        assert_eq!(classify(nanos(999), &config), NodeHealth::Healthy);
        assert_eq!(classify(nanos(1_000), &config), NodeHealth::Suspect);
        assert_eq!(classify(nanos(2_999), &config), NodeHealth::Suspect);
        assert_eq!(classify(nanos(3_000), &config), NodeHealth::Dead);
    }

    #[test]
    fn placement_gives_the_first_candidate_the_partition_and_the_rest_replicate_it() {
        let candidates = vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)];
        let (owner, replicas) = split_placement(&candidates, 3);
        assert_eq!(owner, Some(NodeId(1)));
        assert_eq!(replicas, vec![NodeId(2), NodeId(3)]);
    }

    #[test]
    fn placement_on_an_empty_cluster_yields_no_owner_rather_than_panicking() {
        let (owner, replicas) = split_placement(&[], 3);
        assert_eq!(owner, None);
        assert!(replicas.is_empty());
    }

    #[test]
    fn a_single_node_cluster_places_an_owner_with_no_replicas() {
        let (owner, replicas) = split_placement(&[NodeId(1)], 3);
        assert_eq!(owner, Some(NodeId(1)));
        assert!(replicas.is_empty());
    }
}
