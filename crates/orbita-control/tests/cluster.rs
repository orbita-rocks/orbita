//! The control plane under deterministic simulation.
//!
//! These are the tests the work brief's "done when" list is written against.
//! They run a real leader group node, real worker heartbeats, and the real
//! sweep loop against virtual time, so a failover here takes the same number
//! of virtual seconds it would take in production and none of the real ones.
//!
//! The strongest assertions read the replicated log rather than the resulting
//! state. Checking that a partition ended up with a new owner proves very
//! little; checking that the entry which bumped the epoch commits before the
//! entry which named the new owner proves the thing that actually stops a
//! split brain.
//!
//! Every scenario that breaks something ends with [`Cluster::converged`],
//! which stops the world from breaking any further and then requires the
//! cluster to finish reacting within `ControlConfig::convergence_bound`. That
//! is what makes each fault schedule here a liveness test as well as a safety
//! one, and it is worth the line: every defect this project has found in
//! ownership movement so far left the cluster safe and stuck rather than
//! wrong.
//!
//! Convergence is checked in both of the senses that matter. Ownership has to
//! land somewhere live, and the data has to follow it: [`Replication`] gives
//! the harness a committed position per partition so "every surviving replica
//! has caught up" is something the check reads rather than something it
//! assumes. Without it the condition would be satisfied by every node sitting
//! at the same Lamport forever, which is the shape review caught this file in
//! once already.

use bytes::Bytes;
use orbita_control::{
    binary_speaks, BootstrapSpec, ClusterState, ClusterVersion, ConsensusLog, ControlClient,
    ControlCommand, ControlConfig, ControlService, Controller, KeyspaceConfig, NodeHealth,
    NodeRole, NodeStatus, PartitionProgress, RegistrationOutcome, SingleNodeLog,
    StatusReportResponse, VersionRange,
};
use orbita_core::{
    Epoch, Error, KeyspaceId, Lamport, MapVersion, NodeId, PartitionId, PartitionMap,
};
use orbita_proto::v1::admin_server::Admin as _;
use orbita_runtime::{Clock, PeerCall, Runtime, ServiceId, Transport};
use orbita_sim::{
    check_seeds, converge_within, expect_converged, DiskFaults, DiskPolicy, Failure, NetworkFaults,
    SimConfig, SimRuntime, Simulation, Unmet,
};

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const LEADER: NodeId = NodeId(1);
const WORKERS: [NodeId; 3] = [NodeId(2), NodeId(3), NodeId(4)];

type Log = SingleNodeLog<SimRuntime>;
type Ctl = Controller<SimRuntime, Log>;

/// Where the data actually is, as the harness models it.
///
/// This exists because a convergence check that only watches map versions
/// certifies a property it never looks at. An owner accepts writes and its
/// replicas trail it, and without something standing in for that every node
/// sits at the same Lamport forever and "every surviving replica has caught
/// up" is true by arithmetic rather than by replication.
///
/// The model is deliberately thin. An owner commits one write per heartbeat,
/// a replica adopts the owner's committed position when it next reports and
/// can reach it, and a position never moves backwards, which is what lets a
/// scenario pin a replica ahead of its owner to choose who gets promoted.
///
/// What it is not is a WAL. It says nothing about how the bytes travel or how
/// long they take, only about whether the cluster ever re-establishes the flow
/// after a fault, which is the only part of replication a control-plane
/// liveness check is entitled to an opinion on.
#[derive(Default)]
struct Replication {
    /// The committed position of each partition, advanced by whoever owns it.
    committed: HashMap<PartitionId, u64>,
    /// Where each node has got to on each partition it holds.
    positions: HashMap<(NodeId, PartitionId), u64>,
    /// Set once the convergence window opens. Replicas cannot catch up with a
    /// target that keeps moving, so quiescence has to include the workload:
    /// against a live writer the honest condition would be "within one
    /// heartbeat of the owner", which measures throughput rather than
    /// liveness.
    ///
    /// A draining owner stops writing for the same reason and without needing
    /// this flag, since the handoff it is waiting for requires a replica to
    /// reach its exact position. That is not a convenience: it is what the
    /// crate docs mean by a planned shutdown quiescing writes before it
    /// drains, and a drain against a live writer would never find a receiver.
    frozen: bool,
    /// Nodes that have stopped following their owner. What a replica that
    /// cannot reach its owner looks like from here, and the only way to hold
    /// one behind on purpose now that replication moves it forward.
    held_back: BTreeSet<NodeId>,
}

impl Replication {
    /// One heartbeat's worth of progress for `node` on the partitions it
    /// holds, and what it should now report.
    fn advance(
        &mut self,
        node: NodeId,
        held: &[(PartitionId, bool, bool)],
        writing: bool,
    ) -> HashMap<PartitionId, u64> {
        let mut reported = HashMap::new();
        for (partition, is_owner, reachable) in held {
            let committed = self.committed.get(partition).copied().unwrap_or(0);
            let mut position = self
                .positions
                .get(&(node, *partition))
                .copied()
                .unwrap_or(0)
                // Never backwards. A replica a scenario put ahead of its owner
                // stays ahead, which is how the promotion tests choose the
                // node they mean to have promoted.
                .max(committed);
            if *is_owner {
                // The owner defines the committed tail rather than tracking
                // it, because it is the node accepting writes. A promoted
                // replica therefore carries its own position up with it, which
                // is what gives the nodes behind it something to catch up to.
                if writing && !self.frozen {
                    position += 1;
                }
                self.committed.insert(*partition, position);
            } else if self.held_back.contains(&node) || !reachable {
                // Not following, either because a scenario said so or because
                // it could not reach its owner this round. Whatever it had
                // already reached is still durable, so it keeps that and
                // reports it, which is what makes it a lagging copy rather
                // than an absent one.
                position = self
                    .positions
                    .get(&(node, *partition))
                    .copied()
                    .unwrap_or(0);
            }
            self.positions.insert((node, *partition), position);
            reported.insert(*partition, position);
        }
        reported
    }

    /// Puts a node at a position of the scenario's choosing on every partition
    /// it holds, and moves the partition's committed tail with it when the
    /// node owns it.
    fn pin(&mut self, node: NodeId, held: &[PartitionId], lamport: u64) {
        for partition in held {
            self.positions.insert((node, *partition), lamport);
        }
    }
}

type Progress = Arc<Mutex<Replication>>;

struct Cluster {
    sim: Simulation,
    controller: Ctl,
    log: Arc<Log>,
    progress: Progress,
    readiness: Arc<Mutex<HashMap<NodeId, bool>>>,
    draining: Arc<Mutex<HashMap<NodeId, bool>>>,
    /// Whether a node is still sending heartbeats. A node that has gone quiet
    /// without dying is the case promotion evidence has to survive, since its
    /// last report predates the fence it is being judged against.
    reporting: Arc<Mutex<HashMap<NodeId, bool>>>,
    /// What each worker claims its index costs, per partition it holds. Set
    /// by the tests so that the describe surface can be checked against a
    /// known answer rather than against whatever the storage layer produced.
    ///
    /// `None` is what a worker looks like when its report came through a
    /// status method that cannot carry the measurement, which is every worker
    /// running an older binary for the length of a rolling upgrade.
    index_bytes: Arc<Mutex<HashMap<NodeId, Option<u64>>>>,
    /// The committed prefix each worker reports, where `None` is a node whose
    /// status method has no room for one.
    committed: Arc<Mutex<HashMap<NodeId, Option<u64>>>>,
    /// Nodes an operator asked to hand their partitions off. A drain is not
    /// something the sweep starts on its own, so nothing can be said about one
    /// finishing unless the harness remembers that it was asked for.
    drains_requested: Arc<Mutex<BTreeSet<NodeId>>>,
    /// The control log index the convergence check last saw, and when it last
    /// moved. A sweep that has finished and a sweep that re-proposes the same
    /// command every interval are indistinguishable in a single sample.
    log_progress: Arc<Mutex<(u64, u64)>>,
    wiring: Wiring,
}

/// The seam a replica reaches its owner through, so a severed link stops
/// replication rather than being invisible to it.
#[derive(Clone)]
struct ReplicationEndpoint;

impl orbita_runtime::PeerHandler for ReplicationEndpoint {
    async fn handle(
        &self,
        _from: NodeId,
        _call: PeerCall,
    ) -> Result<Bytes, orbita_runtime::TransportError> {
        Ok(Bytes::new())
    }
}

/// How a worker's heartbeat reaches the leader group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Wiring {
    /// Straight into the controller. Cheap, and blind to network faults.
    Direct,
    /// Over the simulated transport, so a dropped heartbeat is a dropped
    /// heartbeat rather than a call that cannot fail.
    Networked,
}

impl Cluster {
    /// A leader group of one and three workers, bootstrapped with one
    /// keyspace, with heartbeats and the sweep loop running.
    fn start(seed: u64) -> Self {
        Self::start_with(SimConfig::new(seed))
    }

    /// The same cluster, in whatever world the caller wants. Faults are the
    /// reason a scenario is worth running across many seeds rather than one.
    fn start_with(config: SimConfig) -> Self {
        Self::start_wired(config, Wiring::Direct)
    }

    /// The same cluster with heartbeats crossing the simulated network rather
    /// than calling the controller in process.
    ///
    /// Every other scenario here calls `record_status` directly, which is
    /// enough for what they assert and means the network faults this simulator
    /// can inject never reach the control plane at all. A liveness claim made
    /// only against a perfect network is worth much less than one made against
    /// a network that drops and reorders, so one scenario pays for the wiring.
    fn start_networked(config: SimConfig) -> Self {
        Self::start_wired(config, Wiring::Networked)
    }

    fn start_wired(config: SimConfig, wiring: Wiring) -> Self {
        let sim = Simulation::with_config(config);
        let leader = sim.add_node(LEADER);
        for worker in WORKERS {
            sim.add_node(worker);
        }

        let opening = leader.clone();
        let log = sim.block_on(async move { Log::open(&opening).await.expect("open control log") });

        let controller =
            Controller::new(leader.clone(), Arc::clone(&log), ControlConfig::default());
        let spec = BootstrapSpec {
            keyspace: "default".into(),
            config: KeyspaceConfig::default(),
            leaders: vec![(LEADER, "10.0.0.1:7000".into())],
            workers: WORKERS
                .iter()
                .map(|w| (*w, format!("10.0.0.{w}:7000")))
                .collect(),
        };

        let bootstrapping = controller.clone();
        let created = sim.block_on(async move { bootstrapping.bootstrap(&spec).await });
        assert_eq!(created, Ok(true), "a fresh cluster bootstraps");

        let cluster = Self {
            sim,
            controller,
            log,
            progress: Arc::new(Mutex::new(Replication::default())),
            readiness: Arc::new(Mutex::new(
                WORKERS.into_iter().map(|node| (node, true)).collect(),
            )),
            draining: Arc::new(Mutex::new(
                WORKERS.into_iter().map(|node| (node, false)).collect(),
            )),
            reporting: Arc::new(Mutex::new(
                WORKERS.into_iter().map(|node| (node, true)).collect(),
            )),
            index_bytes: Arc::new(Mutex::new(HashMap::new())),
            committed: Arc::new(Mutex::new(HashMap::new())),
            drains_requested: Arc::new(Mutex::new(BTreeSet::new())),
            log_progress: Arc::new(Mutex::new((0, 0))),
            wiring,
        };
        cluster.spawn_sweep(&leader);
        if wiring == Wiring::Networked {
            leader.transport().register(
                ServiceId::Control,
                ControlService::new(cluster.controller.clone()),
            );
            // What a replica calls to follow its owner. It carries no payload
            // because the position lives in the harness's model; what is being
            // simulated is whether the two nodes can talk at all, which is the
            // only part of replication a control-plane liveness check cares
            // about.
            for worker in WORKERS {
                cluster
                    .sim
                    .runtime(worker)
                    .transport()
                    .register(ServiceId::Wal, ReplicationEndpoint);
            }
        }
        for worker in WORKERS {
            cluster.spawn_heartbeats(worker);
        }
        // Let the first round of heartbeats land so that the controller has
        // observations before anything interesting happens.
        cluster.sim.run_for(Duration::from_secs(1));
        cluster
    }

    fn spawn_sweep(&self, leader: &SimRuntime) {
        let controller = self.controller.clone();
        let clock = leader.clock().clone();
        let interval = controller.config().sweep_interval;
        leader.spawn(async move {
            loop {
                let _ = controller.tick().await;
                clock.sleep(interval).await;
            }
        });
    }

    fn spawn_heartbeats(&self, node: NodeId) {
        let runtime = self.sim.runtime(node);
        let controller = self.controller.clone();
        let clock = runtime.clock().clone();
        let interval = controller.config().heartbeat_interval;
        let progress = Arc::clone(&self.progress);
        let readiness = Arc::clone(&self.readiness);
        let draining = Arc::clone(&self.draining);
        // Built once outside the loop so a worker remembers which member
        // answered last, the way the real refresh loop does.
        let client = ControlClient::new(runtime.clone(), vec![LEADER]);
        let transport = runtime.transport().clone();
        let wiring = self.wiring;
        // A worker that cannot reach the leader group keeps routing on the map
        // it already holds, so the loop has to survive a failed fetch rather
        // than forget where it was.
        let mut held = PartitionMap::default();
        // Readiness only travels on the wire shape the finalized protocol
        // enables, so a worker cannot report it until a reply has told it the
        // active cluster version matches its own binary. `ControlConnection`
        // in orbita-server does exactly this, and getting it wrong here left
        // every worker permanently ineligible to own anything, which is how
        // the convergence check earned its keep before it had shipped.
        let mut lifecycle = false;
        let reporting = Arc::clone(&self.reporting);
        let index_bytes = Arc::clone(&self.index_bytes);
        let committed = Arc::clone(&self.committed);

        runtime.spawn(async move {
            loop {
                // A node told to stop reporting is alive and quiet, not
                // gone. Replication pauses with the heartbeat rather than
                // continuing behind it, because this loop is the whole of
                // what the harness models a working node as doing, and
                // letting data flow through a node the leader cannot hear
                // from would claim a fidelity the model does not have.
                let should_report = *reporting
                    .lock()
                    .expect("reporting lock poisoned")
                    .get(&node)
                    .unwrap_or(&false);
                if should_report {
                    match wiring {
                        Wiring::Direct => held = controller.partition_map().await,
                        Wiring::Networked => {
                            if let Ok(Some(fresher)) =
                                client.fetch_map_if_newer(held.version()).await
                            {
                                held = fresher;
                            }
                        }
                    }
                    // Writes land and replicas follow, one heartbeat at a time. A
                    // node that is down is not running this loop, so it stops
                    // following, which is what leaves it behind to catch up on.
                    //
                    // Under the networked wiring a replica has to actually reach
                    // its owner to follow it, so a partition between the two
                    // leaves the replica behind the way a real one would. Without
                    // the probe the model would replicate through a severed link
                    // and the check would never see a lagging copy at all.
                    let mut holdings: Vec<(PartitionId, bool, bool)> = Vec::new();
                    for info in held.held_by(node) {
                        let owns = info.owner == Some(node);
                        let reachable = match (wiring, info.owner) {
                            (Wiring::Networked, Some(owner)) if !owns => transport
                                .call(
                                    owner,
                                    PeerCall {
                                        service: ServiceId::Wal,
                                        method: 1,
                                        payload: Bytes::new(),
                                    },
                                )
                                .await
                                .is_ok(),
                            _ => true,
                        };
                        holdings.push((info.id, owns, reachable));
                    }
                    let is_draining = *draining
                        .lock()
                        .expect("draining lock poisoned")
                        .get(&node)
                        .unwrap_or(&false);
                    let positions = progress.lock().expect("progress lock poisoned").advance(
                        node,
                        &holdings,
                        !is_draining,
                    );
                    let index = *index_bytes
                        .lock()
                        .expect("index bytes lock poisoned")
                        .get(&node)
                        .unwrap_or(&Some(0));
                    // Three states, not two: absent from the map is the
                    // healthy default, `Some(x)` pins the prefix somewhere
                    // behind the durable position, and `None` is a node whose
                    // status method has no room to report one at all.
                    let reported_committed = committed
                        .lock()
                        .expect("committed lock poisoned")
                        .get(&node)
                        .copied();
                    let partitions: Vec<PartitionProgress> = holdings
                        .iter()
                        .map(|(partition, _, _)| {
                            let lamport = Lamport(positions.get(partition).copied().unwrap_or(0));
                            PartitionProgress {
                                partition: *partition,
                                durable_lamport: lamport,
                                applied_lamport: lamport,
                                size_bytes: 0,
                                index_bytes: index,
                                // A healthy owner's committed prefix sits at
                                // its durable position; the tests that care
                                // about the gap, or about a node too old to
                                // report one, set it explicitly.
                                committed_lamport: match reported_committed {
                                    None => Some(lamport),
                                    Some(set) => set.map(Lamport),
                                },
                            }
                        })
                        .collect();
                    let status = NodeStatus {
                        role: NodeRole::Worker,
                        address: format!("10.0.0.{node}:7000"),
                        map_version: held.version(),
                        speaks: orbita_control::binary_speaks(),
                        ready: *readiness
                            .lock()
                            .expect("readiness lock poisoned")
                            .get(&node)
                            .unwrap_or(&false),
                        draining: *draining
                            .lock()
                            .expect("draining lock poisoned")
                            .get(&node)
                            .unwrap_or(&false),
                        partitions,
                    };
                    match wiring {
                        Wiring::Direct => {
                            let _ = controller.record_status(node, status).await;
                        }
                        Wiring::Networked => {
                            let sent = if lifecycle {
                                client.report_status_with_lifecycle(node, status).await
                            } else {
                                client.report_status_for_version(node, status).await
                            };
                            if let Ok(StatusReportResponse::Accepted {
                                cluster_version: Some(active),
                                ..
                            }) = sent
                            {
                                lifecycle = active == orbita_control::binary_version();
                            }
                        }
                    }
                }
                clock.sleep(interval).await;
            }
        });
    }

    /// Puts a node at a chosen data position, so a promotion can be checked
    /// against a known answer rather than against whichever node happened to
    /// report first.
    ///
    /// A pin is a starting point rather than a fixture. Replication carries
    /// the node forward from here as soon as it holds a partition whose owner
    /// is further ahead, which is the behaviour the convergence check is there
    /// to require.
    fn set_progress(&self, node: NodeId, lamport: u64) {
        let map = self.map();
        let held: Vec<PartitionId> = map.held_by(node).map(|info| info.id).collect();
        self.progress
            .lock()
            .expect("progress lock poisoned")
            .pin(node, &held, lamport);
    }

    /// Stops a replica following its owner, or lets it follow again.
    ///
    /// A replica that cannot reach its owner looks exactly like this from the
    /// leader group: still alive, still heartbeating, still reporting a
    /// position, and that position no longer moving.
    fn set_following(&self, node: NodeId, following: bool) {
        let mut replication = self.progress.lock().expect("progress lock poisoned");
        if following {
            replication.held_back.remove(&node);
        } else {
            replication.held_back.insert(node);
        }
    }

    fn set_index_bytes(&self, node: NodeId, bytes: u64) {
        self.index_bytes
            .lock()
            .expect("index bytes lock poisoned")
            .insert(node, Some(bytes));
    }

    /// Makes a worker report the way one whose heartbeat went through a
    /// status method without an index field does.
    fn set_committed(&self, node: NodeId, lamport: Option<u64>) {
        self.committed
            .lock()
            .expect("committed lock poisoned")
            .insert(node, lamport);
    }

    fn set_index_unreported(&self, node: NodeId) {
        self.index_bytes
            .lock()
            .expect("index bytes lock poisoned")
            .insert(node, None);
    }

    fn set_ready(&self, node: NodeId, ready: bool) {
        self.readiness
            .lock()
            .expect("readiness lock poisoned")
            .insert(node, ready);
    }

    fn set_draining(&self, node: NodeId, draining: bool) {
        self.draining
            .lock()
            .expect("draining lock poisoned")
            .insert(node, draining);
    }

    fn set_reporting(&self, node: NodeId, reporting: bool) {
        self.reporting
            .lock()
            .expect("reporting lock poisoned")
            .insert(node, reporting);
    }

    fn map(&self) -> PartitionMap {
        let controller = self.controller.clone();
        self.sim
            .block_on(async move { controller.partition_map().await })
    }

    fn only_partition(&self) -> PartitionId {
        let map = self.map();
        let mut ids = map.partitions().map(|p| p.id);
        let id = ids.next().expect("a bootstrapped cluster has a partition");
        assert!(ids.next().is_none(), "expected exactly one partition");
        id
    }

    fn owner_of(&self, partition: PartitionId) -> Option<NodeId> {
        self.map().partition(partition).and_then(|p| p.owner)
    }

    fn epoch_of(&self, partition: PartitionId) -> Epoch {
        self.map()
            .partition(partition)
            .map_or(Epoch::ZERO, |p| p.epoch)
    }

    /// Everything committed so far, in order.
    fn entries(&self) -> Vec<ControlCommand> {
        let log = Arc::clone(&self.log);
        self.sim.block_on(async move {
            log.subscribe(0)
                .await
                .expect("reading the log")
                .into_iter()
                .map(|entry| entry.command)
                .collect()
        })
    }

    /// Reports a heartbeat for `node` claiming it can speak `speaks`, which
    /// is what a node whose binary was replaced under it does. Applied
    /// synchronously, with no virtual time advanced, so the harness's own
    /// heartbeat loops cannot overwrite it before an assertion runs.
    fn report_speaks(&self, node: NodeId, role: NodeRole, speaks: VersionRange) {
        let controller = self.controller.clone();
        self.sim
            .block_on(async move {
                controller
                    .record_status(
                        node,
                        NodeStatus {
                            role,
                            address: format!("10.0.0.{node}:7000"),
                            map_version: MapVersion::default(),
                            speaks,
                            ready: true,
                            draining: false,
                            partitions: vec![],
                        },
                    )
                    .await
            })
            .expect("recording a status report");
    }

    /// Asks a node to hand its partitions off, remembering that it was asked.
    ///
    /// The real caller is a shutting-down worker's own loop, which keeps
    /// calling until it is told the drain finished or its grace period runs
    /// out. Recording the request is what lets the convergence check stand in
    /// for that loop and require the drain to actually end.
    fn request_drain(&self, node: NodeId) -> Result<bool, Error> {
        self.drains_requested
            .lock()
            .expect("drain lock poisoned")
            .insert(node);
        let controller = self.controller.clone();
        self.sim
            .block_on(async move { controller.drain_node(node).await })
    }

    /// The conditions that together mean the cluster has finished reacting.
    ///
    /// Each is something an operator would check by hand after an incident,
    /// and each is the absence of a state review has already caught this
    /// cluster able to sit in forever: a fenced partition nobody was promoted
    /// for, a replica set still naming a node the leader gave up on, a worker
    /// routing on a map two versions old, a drain waiting on an
    /// acknowledgement that is never coming.
    ///
    /// Catch-up is checked in both of the senses that matter, and they are not
    /// the same thing. `ROUTING` is control-plane catch-up: has this worker
    /// seen the decision the leader published. `REPLICATED` is data catch-up:
    /// has this replica actually reached the position its owner has committed.
    /// A check that only asked the first would certify the second without ever
    /// looking at it, which is worse than not checking at all, because it
    /// makes the gap look covered.
    fn unmet(&self) -> Vec<Unmet> {
        const NOTICED: &str = "the leader has had time to notice the last fault";
        const OWNED: &str = "every partition has an owner";
        const LIVE_OWNER: &str = "every owner is a node the leader believes is alive";
        const NO_DEAD_REPLICAS: &str = "no replica set names a node the leader gave up on";
        const REDUNDANT: &str = "every partition holds as many replicas as the cluster can give it";
        const ROUTING: &str = "every surviving worker is routing on the current map";
        const REPLICATED: &str = "every surviving replica has caught up with its owner";
        const DRAINED: &str = "every requested drain has completed";
        const SETTLED: &str = "the sweep has stopped proposing commands";

        let controller = self.controller.clone();
        let (state, view, index) = self.sim.block_on(async move {
            (
                controller.snapshot().await,
                controller.view().await,
                controller.commit_index().await,
            )
        });

        let mut unmet = Vec::new();
        let config = self.controller.config();
        let now = self.sim.now_nanos();
        let map_version = state.map().version();
        let candidates = state.placement_candidates();
        let want_replicas = config.replication_factor.saturating_sub(1);

        // A cluster that has not been told anything is wrong yet satisfies
        // every condition below, so without this the check would cheerfully
        // certify the gap between a node dying and the leader finding out.
        // The last fault may have been a node going quiet, and silence is only
        // evidence after `dead_after` plus the sweep that acts on it.
        if let Some(at) = self.sim.last_fault_nanos() {
            let notice = config.dead_after + config.sweep_interval;
            let since = now.saturating_sub(at);
            if since < notice.as_nanos() as u64 {
                unmet.push(Unmet::new(
                    NOTICED,
                    format!(
                        "the last fault landed {}ms ago and the leader needs {notice:?} to \
                         classify a node that stopped talking",
                        since / 1_000_000
                    ),
                ));
            }
        }

        for info in state.map().partitions() {
            let Some(owner) = info.owner else {
                unmet.push(Unmet::new(
                    OWNED,
                    format!(
                        "partition {} is {:?} with no owner",
                        info.id,
                        state.phase(info.id)
                    ),
                ));
                continue;
            };
            match state.node(owner) {
                Some(record) if record.health == NodeHealth::Healthy => {}
                found => unmet.push(Unmet::new(
                    LIVE_OWNER,
                    format!(
                        "partition {} is owned by {owner}, which the leader records as {:?}",
                        info.id,
                        found.map(|record| record.health)
                    ),
                )),
            }
            for replica in &info.replicas {
                if state.node(*replica).map(|record| record.health) == Some(NodeHealth::Dead) {
                    unmet.push(Unmet::new(
                        NO_DEAD_REPLICAS,
                        format!(
                            "partition {} still lists dead node {replica} among its replicas",
                            info.id
                        ),
                    ));
                }
            }
            // Redundancy is measured against what the cluster can currently
            // offer rather than against the replication factor, because a
            // three-node cluster that lost a node cannot reach three copies
            // and is not defective for failing to.
            let available = candidates
                .iter()
                .filter(|candidate| **candidate != owner)
                .count();
            let reachable = want_replicas.min(available);
            if info.replicas.len() < reachable {
                unmet.push(Unmet::new(
                    REDUNDANT,
                    format!(
                        "partition {} holds {} replicas with {reachable} placeable",
                        info.id,
                        info.replicas.len()
                    ),
                ));
            }
        }

        for node in &view.nodes {
            if node.record.role != NodeRole::Worker || node.record.health != NodeHealth::Healthy {
                continue;
            }
            if node
                .reported_map_version
                .is_none_or(|seen| seen < map_version)
            {
                unmet.push(Unmet::new(
                    ROUTING,
                    format!(
                        "healthy worker {} last reported map version {:?}, not {map_version}",
                        node.record.id, node.reported_map_version
                    ),
                ));
            }
        }

        // Data catch-up, read from the leader's own observations: the owner's
        // committed position is what the cluster has acknowledged, and a
        // replica short of it is a copy that would lose writes if it were
        // promoted. A node the leader has given up on is excluded, since a
        // dead replica is not evidence of anything and never catches up.
        //
        // This asserts that a replica behind its owner catches up, and it used
        // to carry a caveat: #75 pinned a replica that falls past the owner's
        // retained log as `Stranded`, with no catch-up path at all, which
        // would have made this condition false rather than slow. #82 closed
        // that. Hydration rebuilds such a replica from the bucket, and
        // `retention::PAST_THE_HORIZON` moved from `FailsLoudly` to
        // `Recovers` to say so. The state that could have contradicted this
        // condition now recovers, so the caveat is gone rather than merely
        // unreachable.
        //
        // What remains is a boundary rather than a disagreement. `Stranded` is
        // still terminal for WAL catch-up, per the table on
        // `orbita_wal::ReplicaCatchUp`: a retry cannot help, and the rescue
        // comes from the object store one layer up. This condition is
        // indifferent to which route was taken, because it reads positions
        // rather than mechanisms, and both routes end with the replica at the
        // owner's committed position.
        //
        // The owner's own verdict is still the better authority and is still
        // not reachable from here, for the reason #75 gave: the controller has
        // every number it needs except the retention horizon, and carrying it
        // wants a worker-to-leader status method belonging with #36.
        //
        // The concern #87 raised is closed: `PartitionView::committed_lamport`
        // is now the owner's quorum-replicated prefix rather than its own
        // disk's position, so this condition asks replicas to reach entries a
        // catch-up is actually allowed to ship them. Both are absent rather
        // than zero when nobody has reported, and absence is not convergence.
        for partition in &view.partitions {
            let Some(committed) = partition.committed_lamport else {
                unmet.push(Unmet::new(
                    REPLICATED,
                    format!(
                        "partition {} has no owner reporting a committed position, so \
                         nothing can be said about its replicas",
                        partition.info.id
                    ),
                ));
                continue;
            };
            for progress in &partition.replica_progress {
                let replica = progress.node;
                let surviving = state
                    .node(replica)
                    .is_some_and(|record| record.health != NodeHealth::Dead);
                if !surviving {
                    continue;
                }
                let Some(position) = progress.durable_lamport else {
                    unmet.push(Unmet::new(
                        REPLICATED,
                        format!(
                            "replica {replica} of partition {} has not reported a \
                             position, which is not evidence that it has one",
                            partition.info.id
                        ),
                    ));
                    continue;
                };
                if position < committed {
                    unmet.push(Unmet::new(
                        REPLICATED,
                        format!(
                            "replica {replica} of partition {} is at {position}, behind the \
                             committed position {committed} its owner {:?} reports",
                            partition.info.id, partition.info.owner
                        ),
                    ));
                }
            }
        }

        let requested: Vec<NodeId> = self
            .drains_requested
            .lock()
            .expect("drain lock poisoned")
            .iter()
            .copied()
            .collect();
        for node in requested {
            let controller = self.controller.clone();
            let outcome = self
                .sim
                .block_on(async move { controller.drain_node(node).await });
            if outcome != Ok(true) {
                unmet.push(Unmet::new(
                    DRAINED,
                    format!("node {node} was asked to drain and still answers {outcome:?}"),
                ));
            }
        }

        // Read after the drain retries above, so a drain still committing
        // transfers keeps the log visibly moving instead of being mistaken for
        // a cluster at rest.
        let quiet_for = 2 * config.sweep_interval;
        let mut progress = self
            .log_progress
            .lock()
            .expect("log progress lock poisoned");
        if progress.0 != index {
            *progress = (index, now);
        }
        let quiet = now.saturating_sub(progress.1);
        if quiet < quiet_for.as_nanos() as u64 {
            unmet.push(Unmet::new(
                SETTLED,
                format!(
                    "the control log last moved to index {index} {}ms ago, inside the {quiet_for:?} \
                     a loop retrying every sweep would take to give itself away",
                    quiet / 1_000_000
                ),
            ));
        }

        unmet
    }

    /// Requires the cluster to converge once nothing is breaking it any more.
    ///
    /// Called at the end of a scenario rather than as a scenario of its own,
    /// so every fault schedule already in this file becomes a liveness test
    /// for the cost of one line.
    ///
    /// # Errors
    ///
    /// Names the conditions still unmet at the bound, with the seed that
    /// reproduces the run.
    fn converged(&self) -> Result<(), Failure> {
        let config = self.controller.config().clone();
        // The workload stops with the faults. `converge_within` freezes the
        // one because a cluster still being torn at owes nobody a finished
        // recovery, and this freezes the other for the same reason: a replica
        // cannot reach a target that moves every heartbeat, so against a live
        // writer the strongest true statement would be "within one heartbeat
        // of the owner", which measures throughput rather than liveness.
        self.progress.lock().expect("progress lock poisoned").frozen = true;
        converge_within(
            &self.sim,
            config.convergence_bound(),
            // The conditions can only change when a sweep or a heartbeat runs,
            // so examining them more often than the faster of the two buys
            // nothing but simulator steps.
            config.sweep_interval.min(config.heartbeat_interval),
            || self.unmet(),
        )
    }

    /// What the leader group currently believes about a node.
    fn health_of(&self, node: NodeId) -> Option<NodeHealth> {
        let controller = self.controller.clone();
        self.sim
            .block_on(async move { controller.snapshot().await.node(node).map(|n| n.health) })
    }

    /// The position the leader believes a replica has reached, which is what
    /// the convergence check reads.
    fn replica_position(&self, partition: PartitionId, replica: NodeId) -> Lamport {
        let controller = self.controller.clone();
        let view = self.sim.block_on(async move { controller.view().await });
        view.partitions
            .iter()
            .find(|p| p.info.id == partition)
            .and_then(|p| {
                p.replica_progress
                    .iter()
                    .find(|progress| progress.node == replica)
                    .and_then(|progress| progress.durable_lamport)
            })
            .unwrap_or(Lamport::ZERO)
    }

    /// The position the partition's owner has committed to.
    fn committed_position(&self, partition: PartitionId) -> Lamport {
        let controller = self.controller.clone();
        let view = self.sim.block_on(async move { controller.view().await });
        view.partitions
            .iter()
            .find(|p| p.info.id == partition)
            .and_then(|p| p.committed_lamport)
            .unwrap_or(Lamport::ZERO)
    }

    /// Runs until `ready` holds or `limit` of virtual time has passed.
    fn run_until(&self, limit: Duration, ready: impl Fn(&Self) -> bool) -> bool {
        let deadline = self.sim.now_nanos() + limit.as_nanos() as u64;
        while self.sim.now_nanos() < deadline {
            if ready(self) {
                return true;
            }
            self.sim.run_for(Duration::from_millis(50));
        }
        ready(self)
    }
}

/// Replays the log into a fresh state machine, checking coverage after every
/// single entry.
///
/// This is what "no key is unowned at any instant" means when the map only
/// changes at commit points: if coverage holds after every committed entry,
/// there is no instant at which it did not hold, because there is no state
/// between two entries.
fn coverage_holds_through_every_entry(entries: &[ControlCommand]) -> Result<(), String> {
    let mut state = ClusterState::new();
    for (position, command) in entries.iter().enumerate() {
        let _ = state.apply(command);
        state
            .map()
            .check_coverage()
            .map_err(|e| format!("coverage broke after entry {position} ({command:?}): {e}"))?;
    }
    Ok(())
}

/// Replays the log and, after every committed entry, requires that every probe
/// key resolves to exactly one owned partition and that no split child appears
/// in the map while its parent is still there — the prepare-before-retire
/// invariant, read as a continuous scan through the split.
fn every_key_has_one_owner_through_the_split(
    entries: &[ControlCommand],
    keyspace: KeyspaceId,
    parent: PartitionId,
    lower: PartitionId,
    upper: PartitionId,
) -> Result<(), String> {
    let probes: [&[u8]; 6] = [b"", b"a", b"l", b"m", b"n", b"zzz"];
    let mut state = ClusterState::new();
    for (position, command) in entries.iter().enumerate() {
        let _ = state.apply(command);
        let map = state.map();
        map.check_coverage()
            .map_err(|e| format!("coverage broke after entry {position} ({command:?}): {e}"))?;
        let parent_present = map.partition(parent).is_some();
        for child in [lower, upper] {
            if map.partition(child).is_some() && parent_present {
                return Err(format!(
                    "child {child} and parent {parent} were both in the map after entry \
                     {position} ({command:?}); a child appeared before the parent retired"
                ));
            }
        }
        let keyspace_live =
            parent_present || map.partition(lower).is_some() || map.partition(upper).is_some();
        if !keyspace_live {
            continue;
        }
        for key in probes {
            match map.lookup(keyspace, key) {
                Some(info) if info.owner.is_some() => {}
                Some(info) => {
                    return Err(format!(
                        "key {key:?} resolved to unowned partition {} after entry {position}",
                        info.id
                    ))
                }
                None => {
                    return Err(format!(
                        "key {key:?} resolved to no partition after entry {position} ({command:?})"
                    ))
                }
            }
        }
    }
    Ok(())
}

impl Cluster {
    /// Spawns a task that stands in for the workers' durable child-storage
    /// preparation: it polls the intents each worker must prepare and records a
    /// durable acknowledgement for it. The actual byte-level preparation is
    /// proven in `orbita-storage`; here the ack is what the controller's
    /// completion waits on, exactly as it does in production.
    fn spawn_split_preparers(&self) {
        for node in WORKERS {
            let controller = self.controller.clone();
            let clock = self.sim.runtime(LEADER).clock().clone();
            self.sim.runtime(LEADER).spawn(async move {
                loop {
                    let (_, intents) = controller.active_split_intents_for(node).await;
                    for (intent, prepared) in intents {
                        if prepared {
                            continue;
                        }
                        controller
                            .record_split_prepared(node, intent.parent, intent.lower, intent.upper)
                            .await;
                    }
                    clock.sleep(Duration::from_millis(50)).await;
                }
            });
        }
    }
}

#[test]
fn an_acknowledged_holder_still_sees_a_multi_holder_split_as_active() {
    let cluster = Cluster::start(31);
    let parent = cluster.only_partition();
    let info = cluster.map().partition(parent).unwrap().clone();
    let owner = info.owner.unwrap();
    let waiting = info.replicas[0];
    let outcome = Arc::new(std::sync::Mutex::new(None));
    let recording = Arc::clone(&outcome);
    let controller = cluster.controller.clone();
    cluster.sim.spawn(async move {
        *recording.lock().expect("split outcome poisoned") = Some(
            controller
                .split_partition(parent, Some(Bytes::from_static(b"m")))
                .await,
        );
    });
    cluster.sim.run_for(Duration::from_millis(100));

    let (_, first) = cluster.sim.block_on({
        let controller = cluster.controller.clone();
        async move { controller.active_split_intents_for(owner).await }
    });
    assert_eq!(first.len(), 1);
    assert!(!first[0].1);
    cluster.sim.block_on({
        let controller = cluster.controller.clone();
        let intent = first[0].0.clone();
        async move {
            controller
                .record_split_prepared(owner, parent, intent.lower, intent.upper)
                .await
        }
    });

    for _ in 0..3 {
        let (_, owner_active) = cluster.sim.block_on({
            let controller = cluster.controller.clone();
            async move { controller.active_split_intents_for(owner).await }
        });
        assert_eq!(owner_active.len(), 1);
        assert!(
            owner_active[0].1,
            "the owner has no work left but the split is active"
        );
        let (_, waiting_active) = cluster.sim.block_on({
            let controller = cluster.controller.clone();
            async move { controller.active_split_intents_for(waiting).await }
        });
        assert_eq!(waiting_active.len(), 1);
        assert!(
            !waiting_active[0].1,
            "another holder still owes preparation"
        );
    }
    assert!(
        outcome.lock().expect("split outcome poisoned").is_none(),
        "the split cannot complete while another required holder remains unacknowledged"
    );
}

#[test]
fn an_aborted_generations_acknowledgement_does_not_prepare_its_retry() {
    let cluster = Cluster::start(37);
    let parent = cluster.only_partition();
    let info = cluster.map().partition(parent).unwrap().clone();
    let owner = info.owner.unwrap();
    let begin = move |lower, upper, expect_epoch| ControlCommand::BeginSplit {
        parent,
        at: Bytes::from_static(b"m"),
        lower,
        upper,
        expect_epoch,
    };

    cluster
        .sim
        .block_on({
            let controller = cluster.controller.clone();
            async move {
                controller
                    .submit(begin(PartitionId(10), PartitionId(11), info.epoch))
                    .await
            }
        })
        .unwrap();
    assert!(cluster.sim.block_on({
        let controller = cluster.controller.clone();
        async move {
            controller
                .record_split_prepared(owner, parent, PartitionId(10), PartitionId(11))
                .await
        }
    }));
    assert!(
        cluster.sim.block_on({
            let controller = cluster.controller.clone();
            async move {
                controller
                    .record_split_prepared(owner, parent, PartitionId(10), PartitionId(11))
                    .await
            }
        }),
        "retries of the active generation remain idempotent"
    );
    cluster
        .sim
        .block_on({
            let controller = cluster.controller.clone();
            async move {
                controller
                    .submit(ControlCommand::AbortSplit {
                        parent,
                        expect_epoch: info.epoch,
                    })
                    .await
            }
        })
        .unwrap();
    // The abort raised the parent's epoch, because a parent that quiesced for
    // the split gave Lamports back and may not reissue them under an epoch its
    // replicas still hold. A retry therefore re-reads the map rather than
    // reusing the epoch it opened the first attempt against.
    let after_abort = cluster.map().partition(parent).unwrap().epoch;
    assert_eq!(after_abort, info.epoch.next());
    cluster
        .sim
        .block_on({
            let controller = cluster.controller.clone();
            async move {
                controller
                    .submit(begin(PartitionId(12), PartitionId(13), after_abort))
                    .await
            }
        })
        .unwrap();

    assert!(
        !cluster.sim.block_on({
            let controller = cluster.controller.clone();
            async move {
                controller
                    .record_split_prepared(owner, parent, PartitionId(10), PartitionId(11))
                    .await
            }
        }),
        "a delayed report for the abandoned children is rejected"
    );
    let (_, active) = cluster.sim.block_on({
        let controller = cluster.controller.clone();
        async move { controller.active_split_intents_for(owner).await }
    });
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].0.lower, PartitionId(12));
    assert!(
        !active[0].1,
        "the new child ids define a fresh preparation generation"
    );
}

#[test]
fn a_manual_split_retires_the_parent_only_after_every_holder_durably_prepares() {
    // The end-to-end control-plane proof, driven by real preparation acks
    // rather than a map-version observation. Every pre-split key stays owned by
    // exactly one partition throughout, no child appears before the parent
    // retires, and the children advance the epoch. Reproduce a failure with
    //   ORBITA_SIM_SEED=<seed> cargo test -p orbita-control --test cluster \
    //     a_manual_split_retires_the_parent_only_after_every_holder_durably_prepares
    check_seeds(
        "a_manual_split_retires_the_parent_only_after_every_holder_durably_prepares",
        16,
        |seed| {
            let cluster = Cluster::start(seed);
            cluster.spawn_split_preparers();
            let parent = cluster.only_partition();
            let keyspace = cluster
                .map()
                .partition(parent)
                .expect("the partition")
                .keyspace;
            let parent_epoch = cluster.epoch_of(parent);

            let controller = cluster.controller.clone();
            let split = cluster.sim.block_on(async move {
                controller
                    .split_partition(parent, Some(Bytes::from_static(b"m")))
                    .await
            });
            let (lower, upper) = match split {
                Ok(children) => children,
                Err(reason) => {
                    return Err(cluster
                        .sim
                        .failure(format!("the manual split did not complete: {reason:?}")))
                }
            };

            if let Err(reason) = every_key_has_one_owner_through_the_split(
                &cluster.entries(),
                keyspace,
                parent,
                lower,
                upper,
            ) {
                return Err(cluster.sim.failure(reason));
            }

            let map = cluster.map();
            for (key, expected) in [
                (&b"a"[..], lower),
                (b"l", lower),
                (b"m", upper),
                (b"zz", upper),
            ] {
                match map.lookup(keyspace, key) {
                    Some(info) if info.id == expected => {}
                    other => {
                        return Err(cluster.sim.failure(format!(
                            "key {key:?} resolved to {:?}, expected {expected}",
                            other.map(|i| i.id)
                        )))
                    }
                }
            }
            if map.partition(parent).is_some() {
                return Err(cluster.sim.failure("the parent outlived the split"));
            }
            for child in [lower, upper] {
                if map.partition(child).expect("a live child").epoch <= parent_epoch {
                    return Err(cluster.sim.failure(format!(
                        "child {child}'s epoch did not advance past the parent"
                    )));
                }
            }
            Ok(())
        },
    );
}

#[test]
fn a_split_aborts_rather_than_wedging_when_a_required_holder_dies() {
    // The P2 fix, end to end: a required replica dies after BeginSplit, so it
    // can never prepare. Repair replaces it with a SetReplicas, which aborts the
    // split, and the parent is left whole and re-splittable rather than stuck.
    let cluster = Cluster::start(1);
    // Only the owner ever prepares; the replicas never do, so a split that
    // needed all three would hang without the abort.
    let owner = {
        let controller = cluster.controller.clone();
        let node = cluster
            .map()
            .partition(cluster.only_partition())
            .unwrap()
            .owner
            .unwrap();
        let clock = cluster.sim.runtime(LEADER).clock().clone();
        cluster.sim.runtime(LEADER).spawn(async move {
            loop {
                let (_, intents) = controller.active_split_intents_for(node).await;
                for (intent, prepared) in intents {
                    if prepared {
                        continue;
                    }
                    controller
                        .record_split_prepared(node, intent.parent, intent.lower, intent.upper)
                        .await;
                }
                clock.sleep(Duration::from_millis(50)).await;
            }
        });
        node
    };
    let partition = cluster.only_partition();
    let before = cluster.map().partition(partition).unwrap().clone();
    let doomed = *before.replicas.iter().find(|r| **r != owner).unwrap();

    // Open the split, then kill a required replica.
    let controller = cluster.controller.clone();
    cluster
        .sim
        .block_on(async move {
            controller
                .submit(ControlCommand::BeginSplit {
                    parent: partition,
                    at: Bytes::from_static(b"m"),
                    lower: PartitionId(100),
                    upper: PartitionId(101),
                    expect_epoch: before.epoch,
                })
                .await
        })
        .expect("the split opens");
    assert!(cluster.sim.block_on({
        let controller = cluster.controller.clone();
        async move { controller.snapshot().await.is_splitting(partition) }
    }));

    cluster.sim.crash(doomed);
    // Repair notices the dead replica and replaces it, which aborts the split.
    assert!(cluster.run_until(Duration::from_secs(10), |c| {
        !c.sim.block_on({
            let controller = c.controller.clone();
            async move { controller.snapshot().await.is_splitting(partition) }
        })
    }));
    // The parent is intact and coverage never broke.
    assert!(cluster.map().partition(partition).is_some());
    assert_eq!(cluster.map().check_coverage(), Ok(()));
}

#[test]
fn bootstrap_produces_a_single_unbounded_partition_with_an_owner() {
    let cluster = Cluster::start(1);
    let map = cluster.map();

    assert_eq!(map.check_coverage(), Ok(()));
    assert_eq!(map.len(), 1, "a fresh keyspace starts as one partition");

    let partition = map.partitions().next().unwrap();
    assert!(
        partition.range.start().is_empty(),
        "starts at the beginning"
    );
    assert_eq!(partition.range.end(), None, "and is unbounded");
    assert!(partition.owner.is_some(), "and somebody owns it");
    assert_eq!(partition.epoch, Epoch(1));
}

#[test]
fn describe_reports_the_index_memory_each_node_holds() {
    let cluster = Cluster::start(3);
    for (offset, worker) in WORKERS.iter().enumerate() {
        cluster.set_index_bytes(*worker, 1_000 * (offset as u64 + 1));
    }
    cluster.sim.run_for(Duration::from_secs(2));

    let controller = cluster.controller.clone();
    let view = cluster.sim.block_on(async move { controller.view().await });

    let partitions_held = |node: NodeId| cluster.map().held_by(node).count() as u64;
    for (offset, worker) in WORKERS.iter().enumerate() {
        let node = view
            .nodes
            .iter()
            .find(|n| n.record.id == *worker)
            .expect("every worker is in the description");
        assert_eq!(
            node.index_memory_bytes,
            Some(1_000 * (offset as u64 + 1) * partitions_held(*worker)),
            "a node's index memory is the sum over every partition it holds, \
             replicas included, since those are resident too"
        );
    }
}

#[test]
fn a_worker_that_never_reported_index_memory_stays_unknown_rather_than_empty() {
    // The mixed-version case. A worker whose heartbeat arrived through a
    // status method without the field said nothing about its index, and the
    // description has to keep saying nothing. Reporting zero would tell an
    // operator watching for memory exhaustion that a full index is empty,
    // which is the one direction of error this number cannot afford.
    let cluster = Cluster::start(11);
    let silent = WORKERS[0];
    cluster.set_index_unreported(silent);
    for worker in &WORKERS[1..] {
        cluster.set_index_bytes(*worker, 1_024);
    }
    cluster.sim.run_for(Duration::from_secs(2));

    let controller = cluster.controller.clone();
    let view = cluster.sim.block_on(async move { controller.view().await });

    let node = view
        .nodes
        .iter()
        .find(|n| n.record.id == silent)
        .expect("the worker is still in the description");
    assert_eq!(
        node.index_memory_bytes, None,
        "a node that did not measure its index has no total, not a zero one"
    );

    for worker in &WORKERS[1..] {
        let node = view
            .nodes
            .iter()
            .find(|n| n.record.id == *worker)
            .expect("every worker is in the description");
        assert!(
            node.index_memory_bytes.is_some(),
            "one silent node does not make its neighbours unknown"
        );
    }

    // Every partition the silent worker owns is unknown too, and every
    // partition owned by a reporting worker still has its number.
    for partition in &view.partitions {
        match partition.info.owner {
            Some(owner) if owner == silent => assert_eq!(partition.index_bytes, None),
            Some(_) => assert!(partition.index_bytes.is_some()),
            None => assert_eq!(
                partition.index_bytes, None,
                "an unowned partition has nobody to have measured it"
            ),
        }
    }
}

#[test]
fn one_unmeasured_partition_makes_a_nodes_whole_index_total_unknown() {
    // A partial sum is the failure mode a plain zero substitution turns into
    // once it is added up: it looks like a small number rather than a
    // missing one, and small is the answer that says there is headroom.
    let cluster = Cluster::start(12);
    let worker = WORKERS[0];
    cluster.set_index_bytes(worker, 4_096);
    cluster.sim.run_for(Duration::from_secs(2));

    let controller = cluster.controller.clone();
    let before = cluster
        .sim
        .block_on(async move { controller.view().await })
        .nodes
        .iter()
        .find(|n| n.record.id == worker)
        .expect("the worker is described")
        .index_memory_bytes;
    assert!(before.is_some(), "a reporting worker has a total");

    cluster.set_index_unreported(worker);
    cluster.sim.run_for(Duration::from_secs(2));

    let controller = cluster.controller.clone();
    let after = cluster
        .sim
        .block_on(async move { controller.view().await })
        .nodes
        .iter()
        .find(|n| n.record.id == worker)
        .expect("the worker is described")
        .index_memory_bytes;
    assert_eq!(after, None);
}

#[test]
fn describe_reports_partition_size_and_index_memory_from_the_owner() {
    let cluster = Cluster::start(4);
    let partition = cluster.only_partition();
    let owner = cluster.owner_of(partition).expect("an owner");
    cluster.set_index_bytes(owner, 4_096);
    cluster.sim.run_for(Duration::from_secs(2));

    let controller = cluster.controller.clone();
    let view = cluster.sim.block_on(async move { controller.view().await });
    let described = view
        .partitions
        .iter()
        .find(|p| p.info.id == partition)
        .expect("the partition is described");

    assert_eq!(
        described.index_bytes,
        Some(4_096),
        "the owner's report is what a partition's index costs, because the \
         owner is the node that has to fit it"
    );
}

#[test]
fn an_empty_index_is_described_as_zero_and_not_as_a_missing_measurement() {
    // The other half of keeping unknown and empty apart. A worker that has
    // measured its index and found it empty is a real answer, and folding it
    // into "unknown" would trade one wrong reading for another.
    let cluster = Cluster::start(13);
    let partition = cluster.only_partition();
    let owner = cluster.owner_of(partition).expect("an owner");
    cluster.set_index_bytes(owner, 0);
    cluster.sim.run_for(Duration::from_secs(2));

    let controller = cluster.controller.clone();
    let view = cluster.sim.block_on(async move { controller.view().await });
    let described = view
        .partitions
        .iter()
        .find(|p| p.info.id == partition)
        .expect("the partition is described");

    assert_eq!(described.index_bytes, Some(0));
}

#[test]
fn describe_reports_a_replicas_durable_position_beside_what_it_applied() {
    let cluster = Cluster::start(5);
    let partition = cluster.only_partition();
    for worker in WORKERS {
        cluster.set_progress(worker, 42);
    }
    cluster.sim.run_for(Duration::from_secs(2));

    let controller = cluster.controller.clone();
    let view = cluster.sim.block_on(async move { controller.view().await });
    let described = view
        .partitions
        .iter()
        .find(|p| p.info.id == partition)
        .expect("the partition is described");

    assert!(
        !described.replica_progress.is_empty(),
        "a bootstrapped partition has replicas"
    );
    for replica in &described.replica_progress {
        // At or past where the scenario put it, because #78's replication
        // model keeps a writing owner moving and the replicas following.
        // What is protected here is that the durable position is reported in
        // its own right rather than inferred from what was applied.
        let durable = replica
            .durable_lamport
            .expect("a following replica reports a durable position");
        assert!(durable >= Lamport(42), "{durable:?}");
        assert_eq!(replica.applied_lamport, Some(durable));
    }
}

#[test]
fn bootstrapping_a_cluster_that_already_has_state_changes_nothing() {
    let cluster = Cluster::start(2);
    let before = cluster.map();

    let controller = cluster.controller.clone();
    let spec = BootstrapSpec {
        keyspace: "default".to_string(),
        config: KeyspaceConfig::default(),
        leaders: Vec::new(),
        workers: vec![(LEADER, "10.0.0.1:7000".to_string())],
    };
    let created = cluster
        .sim
        .block_on(async move { controller.bootstrap(&spec).await });

    assert_eq!(
        created,
        Ok(false),
        "bootstrap is safe to call on every start"
    );
    assert_eq!(cluster.map(), before);
}

#[test]
fn a_killed_owner_is_replaced_within_the_failover_budget() {
    check_seeds(
        "a_killed_owner_is_replaced_within_the_failover_budget",
        32,
        |seed| {
            let cluster = Cluster::start(seed);
            let partition = cluster.only_partition();
            let deposed = cluster.owner_of(partition).expect("an owner");

            cluster.sim.crash(deposed);
            let crashed_at = cluster.sim.now_nanos();

            let replaced = cluster.run_until(Duration::from_secs(10), |c| {
                c.owner_of(partition).is_some_and(|owner| owner != deposed)
            });
            if !replaced {
                return Err(cluster.sim.failure(format!(
                    "partition {partition} still had no new owner ten seconds after {deposed} died"
                )));
            }

            let took = Duration::from_nanos(cluster.sim.now_nanos() - crashed_at);
            if took >= Duration::from_secs(10) {
                return Err(cluster.sim.failure(format!(
                    "failover took {took:?}, over the ten second target"
                )));
            }
            if cluster.map().check_coverage() != Ok(()) {
                return Err(cluster.sim.failure("the map lost coverage during failover"));
            }
            cluster.converged()
        },
    );
}

#[test]
fn the_epoch_is_bumped_before_a_replica_is_promoted() {
    check_seeds(
        "the_epoch_is_bumped_before_a_replica_is_promoted",
        32,
        |seed| {
            let cluster = Cluster::start(seed);
            let partition = cluster.only_partition();
            let deposed = cluster.owner_of(partition).expect("an owner");

            cluster.sim.crash(deposed);
            cluster.run_until(Duration::from_secs(10), |c| {
                c.owner_of(partition).is_some_and(|owner| owner != deposed)
            });

            let entries = cluster.entries();
            let fenced = entries.iter().position(|command| {
                matches!(command, ControlCommand::FencePartition { partition: p, .. } if *p == partition)
            });
            let promoted = entries.iter().position(|command| {
                matches!(command, ControlCommand::AssignOwner { partition: p, owner, .. } if *p == partition && *owner != deposed)
            });

            match (fenced, promoted) {
                (Some(fenced), Some(promoted)) if fenced < promoted => cluster.converged(),
                (fenced, promoted) => Err(cluster.sim.failure(format!(
                    "the fence must commit before the promotion, got fence at {fenced:?} and promotion at {promoted:?}"
                ))),
            }
        },
    );
}

#[test]
fn a_deposed_owner_cannot_commit_at_its_old_epoch() {
    check_seeds(
        "a_deposed_owner_cannot_commit_at_its_old_epoch",
        16,
        |seed| {
            let cluster = Cluster::start(seed);
            let partition = cluster.only_partition();
            let deposed = cluster.owner_of(partition).expect("an owner");
            let stale_epoch = cluster.epoch_of(partition);

            cluster.sim.crash(deposed);
            cluster.run_until(Duration::from_secs(10), |c| {
                c.owner_of(partition).is_some_and(|owner| owner != deposed)
            });

            if cluster.epoch_of(partition) <= stale_epoch {
                return Err(cluster.sim.failure(
                    "the epoch did not advance, so the old owner's writes would still be accepted",
                ));
            }

            // The old owner comes back believing it is still in charge. The state
            // machine has to refuse it at its old epoch, which is the same check a
            // replica makes against a stale WAL append.
            let controller = cluster.controller.clone();
            let refused = cluster.sim.block_on(async move {
                controller
                    .submit(ControlCommand::AssignOwner {
                        partition,
                        owner: deposed,
                        replicas: vec![],
                        expect_epoch: stale_epoch,
                    })
                    .await
            });
            if refused.is_ok() {
                return Err(cluster
                    .sim
                    .failure("a deposed owner reinstated itself at its old epoch"));
            }
            cluster.converged()
        },
    );
}

#[test]
fn the_most_caught_up_replica_is_the_one_promoted() {
    check_seeds(
        "the_most_caught_up_replica_is_the_one_promoted",
        16,
        |seed| {
            let cluster = Cluster::start(seed);
            let partition = cluster.only_partition();
            let deposed = cluster.owner_of(partition).expect("an owner");
            let replicas: Vec<NodeId> = cluster
                .map()
                .partition(partition)
                .expect("the partition")
                .replicas
                .clone();
            if replicas.len() < 2 {
                return Err(cluster
                    .sim
                    .failure("expected two replicas to choose between"));
            }

            // Deliberately give the higher node id the lower position, so that
            // picking by id rather than by progress would fail this.
            cluster.set_progress(replicas[0], 10);
            cluster.set_progress(replicas[1], 900);
            cluster.sim.run_for(Duration::from_secs(1));

            cluster.sim.crash(deposed);
            cluster.run_until(Duration::from_secs(10), |c| {
                c.owner_of(partition).is_some_and(|owner| owner != deposed)
            });

            let promoted = cluster.owner_of(partition);
            if promoted != Some(replicas[1]) {
                return Err(cluster.sim.failure(format!(
                    "promoted {promoted:?}, but {} was further ahead",
                    replicas[1]
                )));
            }
            cluster.converged()
        },
    );
}

#[test]
fn an_unready_most_durable_replica_blocks_promotion_until_a_ready_copy_catches_up() {
    check_seeds(
        "an_unready_most_durable_replica_blocks_promotion_until_a_ready_copy_catches_up",
        16,
        |seed| {
            let cluster = Cluster::start(seed);
            let partition = cluster.only_partition();
            let before = cluster.map().partition(partition).unwrap().clone();
            let deposed = before.owner.expect("an owner");
            let [ready, unready] = before.replicas.as_slice() else {
                return Err(cluster
                    .sim
                    .failure("expected two replicas to choose between"));
            };

            cluster.set_progress(*ready, 10);
            cluster.set_progress(*unready, 900);
            cluster.set_ready(*unready, false);
            cluster.sim.run_for(Duration::from_secs(1));
            cluster.sim.crash(deposed);

            cluster.sim.run_for(Duration::from_secs(10));
            if cluster.owner_of(partition).is_some() {
                return Err(cluster
                    .sim
                    .failure("a ready replica behind the acknowledged tail was promoted"));
            }

            cluster.set_progress(*ready, 900);
            let replaced = cluster.run_until(Duration::from_secs(10), |c| {
                c.owner_of(partition) == Some(*ready)
            });
            if !replaced {
                return Err(cluster.sim.failure(
                    "the ready replica was not promoted after reaching the durable tail",
                ));
            }
            if cluster.owner_of(partition) != Some(*ready) {
                return Err(cluster.sim.failure(format!(
                    "promoted {:?}, but ready replica {ready} was the only eligible candidate",
                    cluster.owner_of(partition)
                )));
            }
            cluster.converged()
        },
    );
}

/// The other half of ADR 0008, and the half that keeps it honest.
///
/// The fence leaves the deposed owner in the replica set, which makes it a
/// candidate. It does not make it a claimant. A node whose disk came back
/// shorter than it went away, from a torn tail or a replaced volume, reports
/// a lower position and loses to a copy that did not, exactly like any other
/// replica. If this ever starts passing by
/// promoting the deposed owner, the promotion rule has quietly become an
/// identity check and the no-lost-write argument no longer holds.
#[test]
fn a_deposed_owner_that_came_back_short_loses_to_a_replica_that_did_not() {
    check_seeds(
        "a_deposed_owner_that_came_back_short_loses_to_a_replica_that_did_not",
        16,
        |seed| {
            let cluster = Cluster::start(seed);
            let partition = cluster.only_partition();
            let before = cluster.map().partition(partition).unwrap().clone();
            let deposed = before.owner.expect("an owner");
            let [behind, ahead] = before.replicas.as_slice() else {
                return Err(cluster
                    .sim
                    .failure("expected two replicas to choose between"));
            };

            // One replica is far past anything the returning owner will
            // report. Both are held unready so that nothing is promoted while
            // the owner is away, which is what leaves all three copies in the
            // running for a single decision.
            cluster.set_progress(*ahead, 900);
            cluster.set_ready(*behind, false);
            cluster.set_ready(*ahead, false);
            cluster.sim.run_for(Duration::from_secs(1));

            cluster.sim.crash(deposed);
            let fenced = cluster.run_until(Duration::from_secs(10), |c| {
                c.map()
                    .partition(partition)
                    .is_some_and(|p| p.owner.is_none())
            });
            if !fenced {
                return Err(cluster.sim.failure("a dead owner is fenced"));
            }
            if !cluster
                .map()
                .partition(partition)
                .is_some_and(|p| p.replicas.contains(&deposed))
            {
                return Err(cluster
                    .sim
                    .failure("the fence should have left the deposed owner in the replica set"));
            }

            cluster.sim.restart(deposed, DiskPolicy::Intact);
            cluster.spawn_heartbeats(deposed);
            cluster.set_ready(*ahead, true);
            let replaced =
                cluster.run_until(Duration::from_secs(10), |c| c.owner_of(partition).is_some());
            if !replaced {
                return Err(cluster.sim.failure("nothing was promoted at all"));
            }
            if cluster.owner_of(partition) != Some(*ahead) {
                return Err(cluster.sim.failure(format!(
                    "promoted {:?}, but replica {ahead} reported the furthest durable position",
                    cluster.owner_of(partition)
                )));
            }
            cluster.set_ready(*behind, true);
            cluster.converged()
        },
    );
}

#[test]
fn an_incompatible_most_durable_replica_blocks_promotion_until_a_compatible_copy_catches_up() {
    check_seeds(
        "an_incompatible_most_durable_replica_blocks_promotion_until_a_compatible_copy_catches_up",
        32,
        |seed| {
            let cluster = Cluster::start(seed);
            let partition = cluster.only_partition();
            let before = cluster.map().partition(partition).unwrap().clone();
            let deposed = before.owner.expect("an owner");
            let compatible = before.replicas[0];
            let incompatible = before.replicas[1];

            cluster.report_speaks(compatible, NodeRole::Worker, upgraded_speaks());
            // A 2-of-3 acknowledgement permits the failed owner and this
            // incompatible replica to hold the acknowledged tail at 900 while
            // the remaining compatible replica is still at 10.
            cluster.set_progress(compatible, 10);
            cluster.set_progress(incompatible, 900);

            let previous = binary_speaks().max;
            let controller = cluster.controller.clone();
            cluster
                .sim
                .block_on(async move {
                    controller
                        .submit(ControlCommand::SetClusterVersion {
                            version: upgraded_speaks().max,
                            expect: previous,
                        })
                        .await
                })
                .expect("advancing the test cluster version");

            cluster.sim.crash(deposed);
            cluster.sim.run_for(Duration::from_secs(10));
            if cluster.owner_of(partition).is_some() {
                return Err(cluster
                    .sim
                    .failure("a compatible replica behind the acknowledged tail was promoted"));
            }

            cluster.set_progress(compatible, 900);
            let promoted = cluster.run_until(Duration::from_secs(10), |c| {
                c.owner_of(partition) == Some(compatible)
            });
            if !promoted {
                return Err(cluster.sim.failure(
                    "the compatible replica was not promoted after reaching the durable tail",
                ));
            }
            let owner = cluster.owner_of(partition);
            if owner != Some(compatible) {
                return Err(cluster.sim.failure(format!(
                    "promoted {owner:?}; expected caught-up compatible node {compatible} instead of incompatible node {incompatible}"
                )));
            }
            cluster.converged()
        },
    );
}

#[test]
fn a_promotion_waits_out_the_deposed_owners_read_leases() {
    check_seeds(
        "a_promotion_waits_out_the_deposed_owners_read_leases",
        16,
        |seed| {
            let cluster = Cluster::start(seed);
            let partition = cluster.only_partition();
            let deposed = cluster.owner_of(partition).expect("an owner");
            let drain = cluster.controller.config().lease_drain();

            cluster.sim.crash(deposed);

            // Find the instant the fence took effect, then the instant a new
            // owner appeared, and check the gap.
            let fenced_at = {
                let found = cluster.run_until(Duration::from_secs(10), |c| {
                    c.map()
                        .partition(partition)
                        .is_some_and(|p| p.owner.is_none())
                });
                if !found {
                    return Err(cluster.sim.failure("the dead owner was never fenced"));
                }
                cluster.sim.now_nanos()
            };

            let promoted =
                cluster.run_until(Duration::from_secs(10), |c| c.owner_of(partition).is_some());
            if !promoted {
                return Err(cluster.sim.failure("no replica was ever promoted"));
            }
            let gap = Duration::from_nanos(cluster.sim.now_nanos() - fenced_at);

            // The observed gap is measured from when the test noticed the
            // fence, which is at or after the fence itself, so a correct
            // implementation can show a gap slightly under the drain. What
            // must not happen is a promotion effectively immediately.
            if gap + Duration::from_millis(25) < drain {
                return Err(cluster.sim.failure(format!(
                    "promoted {gap:?} after the fence, which does not wait out a {drain:?} lease"
                )));
            }
            cluster.converged()
        },
    );
}

#[test]
fn a_fenced_partition_waits_for_every_survivor_to_observe_the_fence() {
    let cluster = Cluster::start(1);
    let partition = cluster.only_partition();
    let before = cluster.map().partition(partition).unwrap().clone();
    let deposed = before.owner.expect("an owner");
    let silent = before.replicas[0];

    cluster.sim.crash(deposed);
    cluster.sim.run_for(Duration::from_millis(2_750));
    cluster.set_reporting(silent, false);
    assert!(cluster.run_until(Duration::from_secs(1), |c| {
        c.owner_of(partition).is_none()
    }));

    let config = cluster.controller.config();
    cluster
        .sim
        .run_for(config.lease_drain() + config.sweep_interval.saturating_mul(2));
    assert_eq!(
        cluster.owner_of(partition),
        None,
        "a survivor whose last report predates the fence may hold the durable tail"
    );

    cluster.set_reporting(silent, true);
    assert!(cluster.run_until(Duration::from_secs(2), |c| {
        c.owner_of(partition).is_some()
    }));
    // Waiting for evidence is only safe if the wait ends. This is the fault
    // schedule that most directly produces the state review keeps finding, so
    // it is the one most worth requiring to settle.
    expect_converged(
        "a_fenced_partition_waits_for_every_survivor_to_observe_the_fence",
        cluster.converged(),
    );
}

#[test]
fn unrelated_map_changes_do_not_invalidate_post_fence_reports() {
    let cluster = Cluster::start(2);
    let partition = cluster.only_partition();
    let before = cluster.map().partition(partition).unwrap().clone();
    let deposed = before.owner.expect("an owner");

    cluster.sim.crash(deposed);
    assert!(cluster.run_until(Duration::from_secs(5), |c| {
        c.owner_of(partition).is_none()
    }));
    cluster
        .sim
        .run_for(cluster.controller.config().heartbeat_interval);
    for replica in &before.replicas {
        cluster.set_reporting(*replica, false);
    }

    let fence_version = cluster.map().version();
    let controller = cluster.controller.clone();
    cluster
        .sim
        .block_on(async move {
            controller
                .create_keyspace("second", KeyspaceConfig::default())
                .await
        })
        .expect("create an unrelated keyspace");
    assert!(cluster.map().version() > fence_version);

    assert!(cluster.run_until(Duration::from_secs(2), |c| {
        c.owner_of(partition).is_some()
    }));

    // The replicas were silenced to prove the fence evidence survives churn,
    // and the promoted owner is one of them. Letting them report again is what
    // makes convergence a fair question: a cluster whose only owner has been
    // told to stop talking is not stuck, it is being held.
    for replica in &before.replicas {
        cluster.set_reporting(*replica, true);
    }
    expect_converged(
        "unrelated_map_changes_do_not_invalidate_post_fence_reports",
        cluster.converged(),
    );
}

#[test]
fn a_new_control_leader_does_not_repeat_a_completed_lease_drain() {
    let cluster = Cluster::start(3);
    let partition = cluster.only_partition();
    let before = cluster.map().partition(partition).unwrap().clone();
    let deposed = before.owner.expect("an owner");

    cluster.sim.crash(deposed);
    assert!(cluster.run_until(Duration::from_secs(5), |c| {
        c.entries().iter().any(|command| {
            matches!(
                command,
                ControlCommand::CompleteFenceDrain { partition: id, .. } if *id == partition
            )
        })
    }));
    assert_eq!(cluster.owner_of(partition), None);

    cluster.sim.crash(LEADER);
    let restarted = cluster.sim.restart(LEADER, DiskPolicy::Intact);
    let clock = restarted.clock().clone();
    let promoted = cluster.sim.block_on(async move {
        let log = Log::open(&restarted).await?;
        let controller = Controller::new(restarted, log, ControlConfig::default());
        controller.recover().await?;
        controller.tick().await?;
        // A leader that has just taken over holds no observations, so its
        // first sweep treats every node as freshly heard from, the deposed
        // owner included: since ADR 0008 the fence leaves that node in the
        // replica set, and a candidate nobody has heard from is an unknown
        // upper bound rather than a node that is behind. Letting its silence
        // age out is what a real successor sees, and it is deliberately not
        // the thing under test. What is under test is that the promotion
        // below costs no second `lease_drain` on top of it, because the
        // completed drain was replicated.
        clock.sleep(controller.config().dead_after).await;
        let map = controller.partition_map().await;
        for replica in before.replicas {
            controller
                .record_status(
                    replica,
                    NodeStatus {
                        role: NodeRole::Worker,
                        address: format!("10.0.0.{replica}:7000"),
                        map_version: map.version(),
                        speaks: binary_speaks(),
                        ready: true,
                        draining: false,
                        partitions: vec![PartitionProgress {
                            partition,
                            durable_lamport: Lamport(1),
                            applied_lamport: Lamport(1),
                            size_bytes: 0,
                            // A node speaking the current status method that
                            // has measured an empty index. This scenario is
                            // about promotion, so the honest neutral value is
                            // a measurement, not the absence of one.
                            index_bytes: Some(0),
                            committed_lamport: Some(Lamport(1)),
                        }],
                    },
                )
                .await?;
        }
        controller.tick().await?;
        Ok::<_, Error>(
            controller
                .partition_map()
                .await
                .partition(partition)
                .and_then(|info| info.owner),
        )
    });

    assert!(matches!(promoted, Ok(Some(_))));
}

#[test]
fn reads_are_never_interrupted_by_a_failover_because_coverage_never_breaks() {
    check_seeds(
        "reads_are_never_interrupted_by_a_failover_because_coverage_never_breaks",
        32,
        |seed| {
            let cluster = Cluster::start(seed);
            let partition = cluster.only_partition();
            let deposed = cluster.owner_of(partition).expect("an owner");

            cluster.sim.crash(deposed);
            cluster.run_until(Duration::from_secs(10), |c| {
                c.owner_of(partition).is_some_and(|owner| owner != deposed)
            });

            coverage_holds_through_every_entry(&cluster.entries())
                .map_err(|reason| cluster.sim.failure(reason))?;
            cluster.converged()
        },
    );
}

#[test]
fn a_planned_drain_moves_ownership_in_one_epoch_bumping_commit() {
    check_seeds(
        "a_planned_drain_moves_ownership_in_one_epoch_bumping_commit",
        16,
        |seed| {
            let cluster = Cluster::start(seed);
            let partition = cluster.only_partition();
            let before = cluster.map().partition(partition).unwrap().clone();
            let owner = before.owner.unwrap();
            for node in WORKERS {
                cluster.set_progress(node, 20);
            }
            cluster.set_draining(owner, true);
            cluster.sim.run_for(Duration::from_secs(1));

            let drained = cluster.request_drain(owner);
            if drained != Ok(false) {
                return Err(cluster
                    .sim
                    .failure(format!("the drain did not transfer ownership: {drained:?}")));
            }
            let after = cluster.map().partition(partition).unwrap().clone();
            if after.owner == Some(owner) || after.epoch != before.epoch.next() {
                return Err(cluster.sim.failure(format!(
                    "handoff ended at owner {:?}, epoch {}, from owner {owner}, epoch {}",
                    after.owner, after.epoch, before.epoch
                )));
            }
            let last = cluster.entries().into_iter().last();
            if !matches!(last, Some(ControlCommand::TransferOwnership { from, .. }) if from == owner)
            {
                return Err(cluster
                    .sim
                    .failure("the handoff was not one atomic transfer command"));
            }
            cluster.converged()
        },
    );
}

#[test]
fn a_drain_hands_off_at_the_committed_prefix_not_the_owners_durable_tail() {
    // The #87 invariant, made explicit. An owner can hold entries on its own
    // disk that no quorum acknowledged — writes whose clients were told they
    // failed — so its durable position runs ahead of its committed prefix. The
    // WAL is never allowed to ship that tail, so a receiver can reach the
    // prefix but not the durable position. A drain comparing the durable
    // position would ask for the impossible and refuse a handoff that is in
    // fact safe. #79's `quiesce` hid this by truncating the owner's tail to
    // the prefix before the comparison, so the two coincided; this asserts the
    // drain is correct without leaning on that, by putting every receiver at
    // the committed prefix and below the owner's durable position and
    // requiring the handoff to happen anyway.
    check_seeds(
        "a_drain_hands_off_at_the_committed_prefix_not_the_owners_durable_tail",
        16,
        |seed| {
            let cluster = Cluster::start(seed);
            let partition = cluster.only_partition();
            let before = cluster.map().partition(partition).unwrap().clone();
            let owner = before.owner.unwrap();

            // The owner's disk is ahead of its quorum: durable 20, committed
            // 10. Every replica sits at the committed prefix and is held there,
            // standing in for a quorum that acknowledged through 10 and no
            // further. Under the old comparison none of them is caught up
            // "through 20" and the drain refuses; under the committed-prefix
            // comparison they are caught up through 10 and one is chosen.
            cluster.set_progress(owner, 20);
            cluster.set_committed(owner, Some(10));
            for replica in &before.replicas {
                cluster.set_progress(*replica, 10);
                cluster.set_following(*replica, false);
            }
            cluster.set_draining(owner, true);
            cluster.sim.run_for(Duration::from_secs(1));

            let drained = cluster.request_drain(owner);
            if drained != Ok(false) {
                return Err(cluster.sim.failure(format!(
                    "an owner ahead of its quorum could not hand off to a replica at \
                     the committed prefix: {drained:?}"
                )));
            }
            let after = cluster.map().partition(partition).unwrap().clone();
            match after.owner {
                Some(new_owner) if new_owner != owner && before.replicas.contains(&new_owner) => {}
                other => {
                    return Err(cluster.sim.failure(format!(
                        "handoff landed on {other:?}, not a replica sitting at the \
                         committed prefix below the owner's durable tail"
                    )));
                }
            }
            Ok(())
        },
    );
}

#[test]
fn a_not_ready_replica_is_never_a_planned_handoff_target() {
    check_seeds(
        "a_not_ready_replica_is_never_a_planned_handoff_target",
        16,
        |seed| {
            let cluster = Cluster::start(seed);
            let partition = cluster.only_partition();
            let before = cluster.map().partition(partition).unwrap().clone();
            let owner = before.owner.unwrap();
            for replica in &before.replicas {
                cluster.set_ready(*replica, false);
            }
            cluster.set_draining(owner, true);
            cluster.sim.run_for(Duration::from_secs(1));

            let result = cluster.request_drain(owner);
            if !matches!(result, Err(orbita_core::Error::Unavailable(_))) {
                return Err(cluster
                    .sim
                    .failure(format!("an unready receiver was not refused: {result:?}")));
            }
            if cluster.owner_of(partition) != Some(owner) {
                return Err(cluster
                    .sim
                    .failure("ownership changed despite there being no ready receiver"));
            }

            // A refusal has to be the cluster waiting rather than the cluster
            // giving up: once a replica reports ready, the same drain must
            // finish on its own. That is what the convergence check requires
            // now that the request is on record.
            for replica in &before.replicas {
                cluster.set_ready(*replica, true);
            }
            cluster.converged()
        },
    );
}

#[test]
fn a_drain_waits_only_for_its_receivers_not_an_unrelated_slow_partition() {
    check_seeds(
        "a_drain_waits_only_for_its_receivers_not_an_unrelated_slow_partition",
        16,
        |seed| {
            let cluster = Cluster::start(seed);
            let draining_partition = cluster.only_partition();
            let draining = cluster.owner_of(draining_partition).expect("an owner");
            let receiver = cluster
                .map()
                .partition(draining_partition)
                .unwrap()
                .replicas[0];
            let unrelated_owner = WORKERS
                .into_iter()
                .find(|node| *node != draining && *node != receiver)
                .expect("a third worker");

            let controller = cluster.controller.clone();
            cluster
                .sim
                .block_on(async move {
                    let state = controller.snapshot().await;
                    controller
                        .submit(ControlCommand::CreateKeyspace {
                            id: state.next_keyspace_id(),
                            name: "unrelated".into(),
                            config: KeyspaceConfig::default(),
                            created_at_millis: 2,
                            first_partition: state.next_partition_id(),
                            owner: Some(unrelated_owner),
                            replicas: vec![draining, receiver],
                        })
                        .await
                })
                .expect("creating an unrelated partition");

            for node in WORKERS {
                cluster.set_progress(node, 20);
            }
            cluster.set_ready(unrelated_owner, false);
            cluster.set_draining(draining, true);
            cluster.sim.run_for(Duration::from_secs(1));

            let first = cluster.request_drain(draining);
            if first != Ok(false) {
                return Err(cluster
                    .sim
                    .failure(format!("the drain did not commit its handoff: {first:?}")));
            }
            cluster.sim.run_for(Duration::from_secs(1));

            let controller = cluster.controller.clone();
            let complete = cluster
                .sim
                .block_on(async move { controller.drain_node(draining).await });
            if complete != Ok(true) {
                return Err(cluster.sim.failure(format!(
                    "the unrelated unready owner blocked drain completion: {complete:?}"
                )));
            }
            cluster.converged()
        },
    );
}

#[test]
fn a_control_leader_restart_keeps_the_unacknowledged_handoff_set() {
    let cluster = Cluster::start(14);
    let partition = cluster.only_partition();
    let draining = cluster.owner_of(partition).expect("an owner");
    for node in WORKERS {
        cluster.set_progress(node, 20);
    }
    cluster.set_draining(draining, true);
    cluster.sim.run_for(Duration::from_secs(1));

    let controller = cluster.controller.clone();
    let first = cluster
        .sim
        .block_on(async move { controller.drain_node(draining).await });
    assert_eq!(first, Ok(false), "the ownership transfer commits");

    cluster.sim.crash(LEADER);
    let restarted = cluster.sim.restart(LEADER, DiskPolicy::Intact);
    let recovered = cluster.sim.block_on(async move {
        let log = Log::open(&restarted).await?;
        let controller = Controller::new(restarted, log, ControlConfig::default());
        controller.recover().await?;
        controller.drain_node(draining).await
    });

    assert_eq!(
        recovered,
        Ok(false),
        "the new control leader has no receiver report and must not forget the handoff"
    );
}

#[test]
fn a_split_is_refused_until_child_storage_can_be_prepared() {
    let cluster = Cluster::start(1);
    let parent = cluster.only_partition();
    let before = cluster.map();
    let epoch = before.partition(parent).expect("the parent").epoch;
    let log = Arc::clone(&cluster.log);
    cluster
        .sim
        .block_on(async move {
            log.propose(ControlCommand::SplitPartition {
                parent,
                at: bytes::Bytes::from_static(b"m"),
                lower: PartitionId(100),
                upper: PartitionId(101),
                expect_epoch: epoch,
            })
            .await
        })
        .expect("commit a split without the controller proposal path");
    let controller = cluster.controller.clone();
    cluster
        .sim
        .block_on(async move { controller.recover().await })
        .expect("apply the committed refusal");

    assert_eq!(cluster.map(), before);
}

#[test]
fn the_partition_map_survives_a_full_restart() {
    check_seeds("the_partition_map_survives_a_full_restart", 16, |seed| {
        let cluster = Cluster::start(seed);
        // Move the map somewhere non-trivial first, so that surviving means
        // more than "the bootstrap ran again".
        let controller = cluster.controller.clone();
        let created = cluster.sim.block_on(async move {
            controller
                .create_keyspace("second", KeyspaceConfig::default())
                .await
        });
        if created.is_err() {
            return Err(cluster
                .sim
                .failure(format!("keyspace creation failed: {created:?}")));
        }
        cluster.sim.run_for(Duration::from_secs(2));
        let before = cluster.map();

        // The whole leader group goes down and comes back with its disk.
        cluster.sim.crash(LEADER);
        let restarted = cluster.sim.restart(LEADER, DiskPolicy::Intact);

        let opening = restarted.clone();
        let recovered = cluster.sim.block_on(async move {
            let log = Log::open(&opening).await?;
            let controller = Controller::new(opening, log, ControlConfig::default());
            controller.recover().await?;
            Ok::<PartitionMap, orbita_core::Error>(controller.partition_map().await)
        });

        match recovered {
            Ok(after) if after == before => Ok(()),
            Ok(after) => Err(cluster.sim.failure(format!(
                "the map changed across a restart: {} partitions became {}",
                before.len(),
                after.len()
            ))),
            Err(e) => Err(cluster.sim.failure(format!("recovery failed: {e}"))),
        }
    });
}

#[test]
fn a_restart_replays_the_map_rather_than_bootstrapping_a_second_one() {
    let cluster = Cluster::start(3);
    let before = cluster.map();

    cluster.sim.crash(LEADER);
    let restarted = cluster.sim.restart(LEADER, DiskPolicy::Intact);
    let spec = BootstrapSpec {
        keyspace: "default".into(),
        config: KeyspaceConfig::default(),
        leaders: vec![(LEADER, "10.0.0.1:7000".into())],
        workers: WORKERS
            .iter()
            .map(|w| (*w, format!("10.0.0.{w}:7000")))
            .collect(),
    };

    let after = cluster.sim.block_on(async move {
        let log = Log::open(&restarted).await.unwrap();
        let controller = Controller::new(restarted, log, ControlConfig::default());
        let created = controller.bootstrap(&spec).await.unwrap();
        assert!(
            !created,
            "a restarted node must not create a second keyspace"
        );
        controller.partition_map().await
    });

    assert_eq!(after, before);
    assert_eq!(after.check_coverage(), Ok(()));
}

#[test]
fn a_worker_that_comes_back_is_healthy_again_and_can_hold_partitions() {
    let cluster = Cluster::start(4);
    let partition = cluster.only_partition();
    let deposed = cluster.owner_of(partition).expect("an owner");

    cluster.sim.crash(deposed);
    cluster.run_until(Duration::from_secs(10), |c| {
        c.owner_of(partition).is_some_and(|owner| owner != deposed)
    });

    cluster.sim.restart(deposed, DiskPolicy::Intact);
    cluster.spawn_heartbeats(deposed);
    let recovered = cluster.run_until(Duration::from_secs(5), |c| {
        c.map()
            .partition(partition)
            .is_some_and(|p| p.holds(deposed))
    });

    assert!(
        recovered,
        "a worker that returns should be given work again rather than staying dead forever"
    );
    assert_eq!(cluster.map().check_coverage(), Ok(()));
    expect_converged(
        "a_worker_that_comes_back_is_healthy_again_and_can_hold_partitions",
        cluster.converged(),
    );
}

#[test]
fn keyspaces_created_after_bootstrap_are_covered_and_owned() {
    let cluster = Cluster::start(5);
    let controller = cluster.controller.clone();
    let keyspace = cluster
        .sim
        .block_on(async move {
            controller
                .create_keyspace("catalog", KeyspaceConfig::default())
                .await
        })
        .expect("creating a keyspace");

    let map = cluster.map();
    assert_eq!(map.check_coverage(), Ok(()));
    let partition = map.lookup(keyspace.id, b"anything").expect("covered");
    assert!(partition.owner.is_some());
}

#[test]
fn a_credential_round_trips_through_the_replicated_log() {
    let cluster = Cluster::start(6);
    let controller = cluster.controller.clone();

    let (id, secret) = cluster
        .sim
        .block_on({
            let controller = controller.clone();
            async move {
                controller
                    .create_credential(
                        vec!["default".into()],
                        vec![orbita_control::Permission::Read],
                        "a reader".into(),
                        None,
                        0x1234_5678_9abc_def0,
                    )
                    .await
            }
        })
        .expect("creating a credential");

    let checks = cluster.sim.block_on(async move {
        (
            controller
                .authenticate(&id, &secret, "default", orbita_control::Permission::Read)
                .await,
            controller
                .authenticate(&id, "wrong", "default", orbita_control::Permission::Read)
                .await,
            controller
                .authenticate(&id, &secret, "default", orbita_control::Permission::Write)
                .await,
        )
    });

    assert_eq!(checks.0, Ok(()));
    assert!(checks.1.is_err(), "a wrong secret is rejected");
    assert!(checks.2.is_err(), "a read credential cannot write");
}

/// The window an upgraded binary one minor ahead of this one would report.
fn upgraded_speaks() -> VersionRange {
    let own = binary_speaks().max;
    VersionRange::new(own, ClusterVersion::new(own.major, own.minor + 1))
}

#[test]
fn a_fresh_cluster_starts_at_the_bootstrapping_binarys_version() {
    let cluster = Cluster::start(20);
    let controller = cluster.controller.clone();
    let version = cluster
        .sim
        .block_on(async move { controller.cluster_version().await });
    assert_eq!(version, binary_speaks().max);

    // The log entry matters independently of the value: bootstrap must have
    // committed the version rather than left the state machine on its
    // default, or a member replaying the log could not agree on it.
    let set_at_bootstrap = cluster.entries().into_iter().any(|command| {
        command
            == ControlCommand::SetClusterVersion {
                version: binary_speaks().max,
                expect: ClusterVersion::ZERO,
            }
    });
    assert!(
        set_at_bootstrap,
        "bootstrap did not commit a cluster version"
    );
}

#[test]
fn finalizing_with_nothing_newer_to_speak_is_refused_with_a_reason() {
    // Every node is on the same binary as the cluster version, so there is
    // nothing to finalize, and the operator should be told that rather than
    // shown a silent no-op that leaves them wondering if it worked.
    let cluster = Cluster::start(21);
    let controller = cluster.controller.clone();
    let refused = cluster
        .sim
        .block_on(async move { controller.finalize_upgrade().await });
    let Err(orbita_core::Error::InvalidArgument(reason)) = refused else {
        panic!("expected a refusal, got {refused:?}");
    };
    assert!(reason.contains("already at"), "{reason}");
}

#[test]
fn finalizing_advances_once_every_live_node_reports_the_new_window() {
    let cluster = Cluster::start(22);
    let before = binary_speaks().max;

    cluster.report_speaks(LEADER, NodeRole::Leader, upgraded_speaks());
    for worker in WORKERS {
        cluster.report_speaks(worker, NodeRole::Worker, upgraded_speaks());
    }

    let controller = cluster.controller.clone();
    let finalized = cluster
        .sim
        .block_on(async move { controller.finalize_upgrade().await })
        .expect("every node can speak the new version");
    assert_eq!(finalized.previous, before);
    assert_eq!(finalized.active, upgraded_speaks().max);

    // The advance is a committed log entry, not leader-local state: a member
    // that replays the log must land on the same version.
    let advances = cluster
        .entries()
        .into_iter()
        .filter(|command| {
            matches!(
                command,
                ControlCommand::SetClusterVersion { version, .. }
                    if *version == upgraded_speaks().max
            )
        })
        .count();
    assert_eq!(advances, 1);
}

#[test]
fn a_node_that_cannot_speak_the_new_version_blocks_the_finalize_by_name() {
    let cluster = Cluster::start(23);
    let before = binary_speaks().max;

    cluster.report_speaks(LEADER, NodeRole::Leader, upgraded_speaks());
    // Workers 2 and 3 are upgraded; worker 4 is still on the old binary.
    cluster.report_speaks(WORKERS[0], NodeRole::Worker, upgraded_speaks());
    cluster.report_speaks(WORKERS[1], NodeRole::Worker, upgraded_speaks());

    let controller = cluster.controller.clone();
    let refused = cluster
        .sim
        .block_on(async move { controller.finalize_upgrade().await });
    let Err(orbita_core::Error::InvalidArgument(reason)) = refused else {
        panic!("expected a refusal, got {refused:?}");
    };
    assert!(
        reason.contains(&format!("node {}", WORKERS[2])),
        "the error must name the node holding the upgrade back: {reason}"
    );

    let controller = cluster.controller.clone();
    let version = cluster
        .sim
        .block_on(async move { controller.cluster_version().await });
    assert_eq!(version, before, "nothing may be committed on a refusal");
}

#[test]
fn a_dead_node_does_not_pin_the_cluster_to_the_old_version() {
    // A node that failover already wrote off must not get a vote, or losing a
    // node during an upgrade would leave the cluster unable to ever finalize.
    let cluster = Cluster::start(24);
    let doomed = WORKERS[2];
    cluster.sim.crash(doomed);
    let died = cluster.run_until(Duration::from_secs(10), |c| {
        let controller = c.controller.clone();
        c.sim.block_on(async move {
            controller
                .snapshot()
                .await
                .node(doomed)
                .is_some_and(|n| n.health == orbita_control::NodeHealth::Dead)
        })
    });
    assert!(died, "the crashed worker was never declared dead");

    cluster.report_speaks(LEADER, NodeRole::Leader, upgraded_speaks());
    cluster.report_speaks(WORKERS[0], NodeRole::Worker, upgraded_speaks());
    cluster.report_speaks(WORKERS[1], NodeRole::Worker, upgraded_speaks());

    let controller = cluster.controller.clone();
    let finalized = cluster
        .sim
        .block_on(async move { controller.finalize_upgrade().await })
        .expect("a dead node must not block the finalize");
    assert_eq!(finalized.active, upgraded_speaks().max);
    expect_converged(
        "a_dead_node_does_not_pin_the_cluster_to_the_old_version",
        cluster.converged(),
    );
}

#[test]
fn an_incompatible_existing_owner_stays_live_while_its_heartbeats_continue() {
    check_seeds(
        "an_incompatible_existing_owner_stays_live_while_its_heartbeats_continue",
        32,
        |seed| {
            let cluster = Cluster::start(seed);
            let partition = cluster.only_partition();
            let owner = cluster.owner_of(partition).expect("an owner");
            let previous = binary_speaks().max;
            let active = ClusterVersion::new(previous.major, previous.minor + 1);
            let controller = cluster.controller.clone();
            cluster
                .sim
                .block_on(async move {
                    controller
                        .submit(ControlCommand::SetClusterVersion {
                            version: active,
                            expect: previous,
                        })
                        .await
                })
                .expect("advancing the test cluster version");

            cluster.set_progress(owner, 77);
            let dead_after = cluster.controller.config().dead_after;
            cluster.sim.run_for(dead_after * 3);

            let controller = cluster.controller.clone();
            let health = cluster.sim.block_on(async move {
                controller
                    .snapshot()
                    .await
                    .node(owner)
                    .map(|record| record.health)
            });
            if health != Some(orbita_control::NodeHealth::Healthy) {
                return Err(cluster.sim.failure(format!(
                    "heartbeating incompatible owner {owner} aged to {health:?}"
                )));
            }
            if cluster.owner_of(partition) != Some(owner) {
                return Err(cluster
                    .sim
                    .failure("a live incompatible owner lost its existing ownership"));
            }
            let controller = cluster.controller.clone();
            let progress = cluster.sim.block_on(async move {
                controller
                    .view()
                    .await
                    .partitions
                    .into_iter()
                    .find(|view| view.info.id == partition)
                    .map(|view| view.committed_lamport)
            });
            // At or past where the test put it, rather than exactly there: an
            // owner that is still live is still committing writes, so pinning
            // the equality would be asserting that it had stopped. What is
            // being protected is that the leader keeps recording an
            // incompatible owner's position instead of dropping it. The
            // doubled Option is the field's own plus this lookup's.
            if progress
                .flatten()
                .is_none_or(|committed| committed < Lamport(77))
            {
                return Err(cluster.sim.failure(format!(
                    "incompatible owner's heartbeat progress was not retained: {progress:?}"
                )));
            }
            cluster.converged()
        },
    );
}

#[test]
fn incompatible_joins_are_rejected_under_deterministic_simulation() {
    check_seeds(
        "incompatible_joins_are_rejected_under_deterministic_simulation",
        32,
        |seed| {
            let cluster = Cluster::start(seed);
            let active = binary_speaks().max;
            let too_new = VersionRange::new(
                ClusterVersion::new(active.major, active.minor + 1),
                ClusterVersion::new(active.major, active.minor + 2),
            );
            let controller = cluster.controller.clone();
            let outcome = cluster.sim.block_on(async move {
                controller
                    .record_status(
                        NodeId(9),
                        NodeStatus {
                            role: NodeRole::Worker,
                            address: "10.0.0.9:7000".into(),
                            map_version: MapVersion::default(),
                            speaks: too_new,
                            ready: false,
                            draining: false,
                            partitions: vec![],
                        },
                    )
                    .await
            });
            match outcome {
                Ok(RegistrationOutcome::Incompatible(refusal))
                    if refusal.speaks == too_new && refusal.active == active => {}
                other => {
                    return Err(cluster
                        .sim
                        .failure(format!("incompatible join was not refused: {other:?}")))
                }
            }
            let controller = cluster.controller.clone();
            let admitted = cluster
                .sim
                .block_on(async move { controller.snapshot().await.node(NodeId(9)).is_some() });
            if admitted {
                return Err(cluster.sim.failure("the refused node became a member"));
            }
            cluster.converged()
        },
    );
}

#[test]
fn rolling_upgrade_ranges_are_accepted_under_deterministic_simulation() {
    check_seeds(
        "rolling_upgrade_ranges_are_accepted_under_deterministic_simulation",
        32,
        |seed| {
            let cluster = Cluster::start(seed);
            let active = binary_speaks().max;
            let rolling =
                VersionRange::new(active, ClusterVersion::new(active.major, active.minor + 1));
            let controller = cluster.controller.clone();
            let outcome = cluster.sim.block_on(async move {
                controller
                    .record_status(
                        NodeId(9),
                        NodeStatus {
                            role: NodeRole::Worker,
                            address: "10.0.0.9:7000".into(),
                            map_version: MapVersion::default(),
                            speaks: rolling,
                            ready: false,
                            draining: false,
                            partitions: vec![],
                        },
                    )
                    .await
            });
            if !matches!(outcome, Ok(RegistrationOutcome::Accepted(_))) {
                return Err(cluster
                    .sim
                    .failure(format!("rolling upgrade join was refused: {outcome:?}")));
            }
            // Node 9 was admitted and will never heartbeat again, so this also
            // pins that a member which joins and immediately vanishes is aged
            // out rather than left holding the cluster short of quiescence.
            cluster.converged()
        },
    );
}

/// Ran ignored until issue #76 was fixed, because it reached that defect on 2
/// of the first 20000 seeds, 4422 the lowest, by killing both replicas of an
/// already fenced partition and then bringing the deposed owner back. That was
/// a third independent route into the same stuck state, and it is the reason
/// to keep this scenario's schedule rather than trim it: seed 4422 is now a
/// regression test for the fence keeping the deposed owner in the replica set.
#[test]
fn a_cluster_converges_after_an_arbitrary_sequence_of_worker_failures() {
    // The scenarios above each break one thing at a chosen moment, which is
    // what makes their safety assertions readable and what makes them a thin
    // sample of the states a cluster reaches. This one lets the seed decide
    // what fails and when, asserts nothing about the route, and requires only
    // that the cluster be finished when the failures stop. Liveness is the one
    // property that can be checked without knowing what happened.
    check_seeds(
        "a_cluster_converges_after_an_arbitrary_sequence_of_worker_failures",
        64,
        |seed| {
            let cluster = Cluster::start(seed);
            let mut down: BTreeSet<NodeId> = BTreeSet::new();

            for _ in 0..6 {
                let picked = WORKERS[cluster.sim.random_below(WORKERS.len() as u64) as usize];
                if down.contains(&picked) {
                    cluster.sim.restart(picked, DiskPolicy::Intact);
                    cluster.spawn_heartbeats(picked);
                    down.remove(&picked);
                } else if down.len() + 1 < WORKERS.len() {
                    // One worker always stays up. A cluster with nothing left
                    // to own a partition is unavailable by arithmetic rather
                    // than by defect, and asserting against that would be
                    // asserting that three minus three is more than zero.
                    cluster.sim.crash(picked);
                    down.insert(picked);
                }
                // Sometimes inside the detection window and sometimes well
                // past it, so the schedule covers failures that overlap a
                // failover already in flight as well as ones that do not.
                cluster
                    .sim
                    .run_for(Duration::from_millis(200 + cluster.sim.random_below(5_000)));
            }

            if cluster.map().check_coverage() != Ok(()) {
                return Err(cluster.sim.failure("the map lost coverage"));
            }
            cluster.converged()
        },
    );
}

/// A world where heartbeats have to survive the network to arrive.
///
/// The drop rate is high enough that twelve consecutive losses, the silence
/// `dead_after` acts on, is a real event across a batch rather than a
/// theoretical one, which is what makes the scenario capable of producing a
/// spurious failover. The budget then stops a run from spending all of its
/// virtual time in detection and never reaching the state worth checking.
fn hostile_network(seed: u64) -> SimConfig {
    SimConfig {
        network: NetworkFaults {
            drop_permille: 300,
            duplicate_permille: 50,
            slow_permille: 100,
            ..NetworkFaults::none()
        },
        fault_warmup: Duration::from_secs(2),
        fault_budget: 400,
        ..SimConfig::new(seed)
    }
}

/// Ran ignored until issue #76 was fixed, because it reached that defect on 15
/// of the first 20000 seeds, 130 being the lowest. The invariant was never
/// relaxed to get it green: the check was right and the cluster was wrong.
///
/// Those seed numbers move whenever the traffic this scenario generates
/// changes, since a seed names an interleaving rather than a state. The
/// deterministic reproductions below are the ones to work from; these are here
/// to say how often the defect was reachable, not to be replayed.
#[test]
fn a_cluster_converges_after_the_network_stops_eating_heartbeats() {
    let faults = Arc::new(AtomicU64::new(0));
    let counter = Arc::clone(&faults);
    check_seeds(
        "a_cluster_converges_after_the_network_stops_eating_heartbeats",
        64,
        move |seed| {
            let cluster = Cluster::start_networked(hostile_network(seed));
            let stranded = WORKERS[(cluster.sim.random_below(WORKERS.len() as u64)) as usize];

            // One-way, so the leader group stops hearing from a worker that is
            // still perfectly happy and still answering everyone else. That
            // asymmetry is what produces a failover nobody needed, and
            // recovering from an unnecessary failover is as much a liveness
            // obligation as recovering from a real one.
            cluster.sim.partition_one_way(stranded, LEADER);
            cluster
                .sim
                .run_for(Duration::from_millis(500 + cluster.sim.random_below(6_000)));

            if cluster.map().check_coverage() != Ok(()) {
                return Err(cluster.sim.failure("the map lost coverage"));
            }
            Arc::clone(&counter).fetch_add(cluster.sim.faults_injected(), Ordering::Relaxed);
            cluster.converged()
        },
    );

    assert!(
        faults.load(Ordering::Relaxed) > 0,
        "no network fault was injected across the whole batch, so this proved nothing"
    );
}

/// A partition whose replica set is emptied before its owner dies is never
/// promoted and never placed, and no worker coming back changes that.
///
/// Found by the convergence check on the networked batch above and reduced to
/// this, which needs no seed luck. The sequence is entirely ordinary:
///
/// 1. Both replicas go silent long enough to be declared dead, and
///    `repair_replica_sets` takes each one out. The replica set is now empty.
/// 2. The owner then dies, and `fence_partition` removes it too, leaving the
///    partition `Fenced` with nothing named on it.
/// 3. Every worker comes back healthy, ready, and eligible.
///
/// The partition used to stay unavailable forever, because each of the three
/// sweep stages declined it in turn: `promote_drained_partitions` read
/// candidates out of an empty `replicas`, `place_unowned_partitions` only
/// looks at `Unowned` partitions, and `repair_replica_sets` only looks at
/// `Serving` ones. Safe, and stuck.
///
/// What unsticks it is the fence demoting the deposed owner into the replica
/// set instead of dropping it, per ADR 0008, so a fence can no longer leave a
/// partition with nothing named on it. Here that node is also the only node
/// that ever held the data: with an empty replica set `Wal::replicate` needs
/// no acknowledgement at all, so the owner was committing writes on its own
/// disk alone and promoting anything else would lose them.
#[test]
fn a_partition_fenced_with_an_empty_replica_set_is_placed_again() {
    let cluster = Cluster::start(41);
    let partition = cluster.only_partition();
    let info = cluster.map().partition(partition).unwrap().clone();
    let owner = info.owner.expect("an owner");

    for replica in &info.replicas {
        cluster.sim.crash(*replica);
    }
    let emptied = cluster.run_until(Duration::from_secs(10), |c| {
        c.map()
            .partition(partition)
            .is_some_and(|p| p.replicas.is_empty())
    });
    assert!(emptied, "the sweep should retire replicas it believes dead");

    cluster.sim.crash(owner);
    let fenced = cluster.run_until(Duration::from_secs(10), |c| {
        c.map()
            .partition(partition)
            .is_some_and(|p| p.owner.is_none())
    });
    assert!(fenced, "a dead owner is fenced");

    // Everything comes back. There is now a full complement of healthy, ready,
    // eligible workers and one partition that nobody owns.
    for node in WORKERS {
        cluster.sim.restart(node, DiskPolicy::Intact);
        cluster.spawn_heartbeats(node);
    }
    expect_converged(
        "a_partition_fenced_with_an_empty_replica_set_is_placed_again",
        cluster.converged(),
    );
}

#[test]
fn a_replica_that_fell_behind_catches_up_once_it_can_follow_again() {
    // The failover scenarios all check that ownership lands somewhere. This
    // one checks the half that ownership does not cover: a copy that stopped
    // following has to be brought back to the owner's position, or the cluster
    // is one node's disk away from losing writes it has already acknowledged
    // while every ownership assertion in the file still passes.
    check_seeds(
        "a_replica_that_fell_behind_catches_up_once_it_can_follow_again",
        32,
        |seed| {
            let cluster = Cluster::start(seed);
            let partition = cluster.only_partition();
            let stalled = cluster.map().partition(partition).unwrap().replicas[0];

            cluster.set_following(stalled, false);
            cluster
                .sim
                .run_for(Duration::from_millis(500 + cluster.sim.random_below(4_000)));

            let owner = cluster.owner_of(partition).expect("an owner");
            let behind = cluster.replica_position(partition, stalled);
            let ahead = cluster.committed_position(partition);
            if behind >= ahead {
                return Err(cluster.sim.failure(format!(
                    "replica {stalled} was supposed to fall behind owner {owner}, but sits at \
                     {behind} against {ahead}"
                )));
            }

            cluster.set_following(stalled, true);
            cluster.converged()
        },
    );
}

/// The same defect as the test above, reached from the other side, and the
/// shape that says most clearly what the fix had to decide.
///
/// Here the replica set is full when the owner is fenced, and then both
/// remaining replicas die. The deposed owner comes back healthy and ready,
/// holding the data, and it is the only node that could serve the partition.
/// It used to be the one node the fence took out of `replicas`, so
/// `best_candidate` could not see it, and no other sweep stage looks at a
/// `Fenced` partition at all.
///
/// A cluster with a live, ready, caught-up copy of a partition and no way to
/// route to it was the clearest statement of the question in #76: whether a
/// fenced owner may be promoted again. ADR 0008 answers yes, at the epoch the
/// fence produced and on the position the node reports, and this is the case
/// that answer exists for.
///
/// The replicas are made unready before the owner dies, which is doing real
/// work rather than decorating the setup. Without it the leader promotes one
/// of them out of a report that landed in the same millisecond as the fence,
/// even though the test has since crashed it, and the scenario turns into a
/// second failover from an owner that never served a request. That state is
/// genuinely unrecoverable, because the promoted node may have accepted writes
/// for all the leader can tell and so nothing older than it may be promoted,
/// and it is not the state this test is about. Unready keeps the replicas in
/// the replica set, where `repair_replica_sets` leaves them, while taking them
/// out of the running.
#[test]
fn a_fenced_owner_that_returns_can_take_its_partition_back() {
    let cluster = Cluster::start(47);
    let partition = cluster.only_partition();
    let info = cluster.map().partition(partition).unwrap().clone();
    let deposed = info.owner.expect("an owner");

    for replica in &info.replicas {
        cluster.set_ready(*replica, false);
    }
    cluster.sim.run_for(Duration::from_secs(1));

    cluster.sim.crash(deposed);
    let fenced = cluster.run_until(Duration::from_secs(10), |c| {
        c.map()
            .partition(partition)
            .is_some_and(|p| p.owner.is_none())
    });
    assert!(fenced, "a dead owner is fenced");
    let after_fence = cluster.map().partition(partition).unwrap().replicas.clone();
    assert!(
        info.replicas.iter().all(|r| after_fence.contains(r)),
        "the replica set is still full at the fence, which is what makes this \
         the other route into #76 rather than the emptied one above"
    );

    // Everything that was left on the partition goes away, so the deposed
    // owner is the only copy the cluster has.
    for replica in &info.replicas {
        cluster.sim.crash(*replica);
    }
    let stranded = cluster.run_until(Duration::from_secs(10), |c| {
        c.map().partition(partition).is_some_and(|p| {
            p.replicas
                .iter()
                .all(|r| c.health_of(*r) == Some(NodeHealth::Dead))
        })
    });
    assert!(stranded, "the surviving replicas are declared dead");

    cluster.sim.restart(deposed, DiskPolicy::Intact);
    cluster.spawn_heartbeats(deposed);
    expect_converged(
        "a_fenced_owner_that_returns_can_take_its_partition_back",
        cluster.converged(),
    );
    assert_eq!(
        cluster.owner_of(partition),
        Some(deposed),
        "the only node holding the partition is the one that got it back"
    );
}

/// The test of the liveness test.
///
/// A convergence check that cannot fail is worse than no check at all,
/// because it reads like a guarantee. This parks the cluster in the exact
/// state review found it able to sit in forever, fenced and safe with nothing
/// eligible to promote, and requires the check to say so, naming the condition
/// rather than only reporting that something did not happen.
#[test]
fn a_cluster_that_cannot_promote_fails_the_convergence_check_by_name() {
    let cluster = Cluster::start(31);
    let partition = cluster.only_partition();
    let info = cluster.map().partition(partition).unwrap().clone();
    let deposed = info.owner.expect("an owner");

    for replica in &info.replicas {
        cluster.set_ready(*replica, false);
    }
    cluster.sim.run_for(Duration::from_secs(1));
    cluster.sim.crash(deposed);

    let failure = cluster
        .converged()
        .expect_err("a cluster with nothing left to promote has not converged");
    assert!(
        failure.reason.contains("every partition has an owner"),
        "the failure must name the condition that broke: {}",
        failure.reason
    );
    assert!(
        failure.reason.contains(&format!("partition {partition}")),
        "the failure must name the partition that is stuck: {}",
        failure.reason
    );
    assert!(
        failure.reason.contains("the last fault landed at"),
        "the failure must say what it is measuring recovery from: {}",
        failure.reason
    );
}

/// The same test for the half of the invariant that is about data rather than
/// ownership.
///
/// Review caught this check certifying replica catch-up while only ever
/// looking at map versions, and a scenario existed in which a surviving
/// replica sat at Lamport 10 against an owner at 900 and convergence still
/// passed. This is what stops that from being reintroduced quietly: if the
/// condition is ever weakened back into a map-version check, this test starts
/// passing when it should fail.
#[test]
fn a_replica_stuck_behind_its_owner_fails_the_convergence_check_by_name() {
    let cluster = Cluster::start(43);
    let partition = cluster.only_partition();
    let stalled = cluster.map().partition(partition).unwrap().replicas[0];

    // Alive, heartbeating, in the replica set, routing on the current map, and
    // not following. Every other condition holds; only the data is behind.
    cluster.set_following(stalled, false);
    cluster.sim.run_for(Duration::from_secs(2));

    let failure = cluster
        .converged()
        .expect_err("a replica short of its owner has not converged");
    assert!(
        failure
            .reason
            .contains("every surviving replica has caught up with its owner"),
        "the failure must name the condition that broke: {}",
        failure.reason
    );
    assert!(
        failure.reason.contains(&format!("replica {stalled}")),
        "the failure must name the replica that is behind: {}",
        failure.reason
    );
}

/// Kept separate from the scenarios so a failure points at the harness rather
/// than at the system under test.
#[test]
fn the_harness_reports_a_failure_with_the_seed_that_produced_it() {
    let failure = Failure {
        seed: 7,
        reason: "example".into(),
        trace: Simulation::new(7).trace(),
    };
    assert!(failure.to_string().contains("seed 7"));
}

/// A world where the control log's disk misbehaves after the cluster has
/// formed.
///
/// Bootstrap runs before the warmup expires, so the cluster exists; everything
/// after it has to cope with proposals that fail.
fn flaky_disk(seed: u64) -> SimConfig {
    SimConfig {
        disk: DiskFaults {
            // High enough that a failover's handful of appends is very likely
            // to hit one. The budget then stops the run from spending all of
            // its virtual time retrying and never reaching the state worth
            // checking.
            write_failure_permille: 500,
            partial_write_permille: 200,
            torn_tail_on_crash: true,
            ..DiskFaults::none()
        },
        fault_warmup: Duration::from_secs(2),
        fault_budget: 20,
        ..SimConfig::new(seed)
    }
}

#[test]
fn a_failover_still_completes_when_the_control_log_keeps_failing_writes() {
    // Counted across the batch rather than per seed. A single failover makes
    // only a couple of appends, so an individual seed can legitimately escape
    // without a fault; a whole batch escaping means the scenario is not
    // testing what its name says.
    let faults = Arc::new(AtomicU64::new(0));
    let counter = Arc::clone(&faults);
    check_seeds(
        "a_failover_still_completes_when_the_control_log_keeps_failing_writes",
        32,
        move |seed| {
            let faults = Arc::clone(&counter);
            let cluster = Cluster::start_with(flaky_disk(seed));
            let partition = cluster.only_partition();
            let deposed = cluster.owner_of(partition).expect("an owner");
            cluster.sim.run_for(Duration::from_secs(3));

            cluster.sim.crash(deposed);
            // Generous, because every failed proposal costs a whole sweep
            // interval. What is being checked is that the sweep converges at
            // all rather than wedging on a half-applied failover.
            let replaced = cluster.run_until(Duration::from_secs(60), |c| {
                c.owner_of(partition).is_some_and(|owner| owner != deposed)
            });
            if !replaced {
                return Err(cluster
                    .sim
                    .failure("the failover never converged on a flaky disk"));
            }
            faults.fetch_add(cluster.sim.faults_injected(), Ordering::Relaxed);
            coverage_holds_through_every_entry(&cluster.entries())
                .map_err(|reason| cluster.sim.failure(reason))?;
            cluster.converged()
        },
    );

    assert!(
        faults.load(Ordering::Relaxed) > 0,
        "no disk fault was injected across the whole batch, so this proved nothing"
    );
}

#[test]
fn a_control_log_that_survived_failed_writes_still_replays_in_full() {
    check_seeds(
        "a_control_log_that_survived_failed_writes_still_replays_in_full",
        32,
        |seed| {
            let cluster = Cluster::start_with(flaky_disk(seed));
            cluster.sim.run_for(Duration::from_secs(3));

            let controller = cluster.controller.clone();
            let _ = cluster.sim.block_on(async move {
                controller
                    .create_keyspace("second", KeyspaceConfig::default())
                    .await
            });
            cluster.sim.run_for(Duration::from_secs(3));
            let before = cluster.map();

            // A partial write must not bury the entries that follow it. If the
            // rollback in the log is wrong, recovery stops early and the map
            // comes back short.
            let reopened = cluster.sim.runtime(LEADER);
            let after = cluster.sim.block_on(async move {
                let log = Log::open(&reopened).await?;
                let controller = Controller::new(reopened, log, ControlConfig::default());
                controller.recover().await?;
                Ok::<PartitionMap, orbita_core::Error>(controller.partition_map().await)
            });

            match after {
                Ok(after) if after == before => cluster.converged(),
                Ok(after) => Err(cluster.sim.failure(format!(
                    "replay lost entries: {} partitions at version {} became {} at version {}",
                    before.len(),
                    before.version(),
                    after.len(),
                    after.version()
                ))),
                Err(e) => Err(cluster.sim.failure(format!("replay failed: {e}"))),
            }
        },
    );
}

#[test]
fn a_worker_reaches_the_leader_group_over_the_transport() {
    let cluster = Cluster::start(11);
    let leader = cluster.sim.runtime(LEADER);
    leader.transport().register(
        ServiceId::Control,
        ControlService::new(cluster.controller.clone()),
    );

    let worker = cluster.sim.runtime(WORKERS[0]);
    let client = ControlClient::new(worker, vec![LEADER]);
    let expected = cluster.map();

    let fetched = cluster.sim.block_on({
        let client = client.clone();
        async move { client.fetch_map().await }
    });
    assert_eq!(fetched, Ok(expected.clone()));

    // The version check is what keeps a polling worker cheap, so it has to
    // actually answer "nothing changed" rather than resending the map.
    let unchanged = cluster.sim.block_on({
        let client = client.clone();
        async move { client.fetch_map_if_newer(expected.version()).await }
    });
    assert_eq!(unchanged, Ok(None));

    let reported = cluster.sim.block_on({
        let client = client.clone();
        async move {
            client
                .report_status(
                    WORKERS[0],
                    NodeStatus {
                        role: NodeRole::Worker,
                        address: "10.0.0.2:7000".into(),
                        map_version: MapVersion::default(),
                        speaks: orbita_control::binary_speaks(),
                        ready: true,
                        draining: false,
                        partitions: vec![],
                    },
                )
                .await
        }
    });
    assert_eq!(reported, Ok(()));
}

#[test]
fn a_node_learns_the_leader_groups_auth_policy_over_the_transport() {
    // The signal the readiness gate leans on: a node must be able to ask the
    // leader group whether the cluster requires auth, so it can catch a
    // half-rolled `require_auth` change instead of trusting its own config in
    // isolation.
    for require_auth in [false, true] {
        let cluster = Cluster::start(21);
        let leader = cluster.sim.runtime(LEADER);
        leader.transport().register(
            ServiceId::Control,
            ControlService::new(cluster.controller.clone()).require_auth(require_auth),
        );

        let worker = cluster.sim.runtime(WORKERS[0]);
        let client = ControlClient::new(worker, vec![LEADER]);
        let learned = cluster
            .sim
            .block_on(async move { client.fetch_auth_policy().await });
        assert_eq!(
            learned,
            Ok(require_auth),
            "a node reads back exactly the policy the leader advertises"
        );
    }
}

#[test]
fn the_drain_protocol_distinguishes_progress_from_refusal() {
    let cluster = Cluster::start(13);
    let partition = cluster.only_partition();
    let owner = cluster.owner_of(partition).expect("an owner");
    for node in WORKERS {
        cluster.set_progress(node, 20);
    }
    cluster.set_draining(owner, true);
    cluster.sim.run_for(Duration::from_secs(1));

    let leader = cluster.sim.runtime(LEADER);
    leader.transport().register(
        ServiceId::Control,
        ControlService::new(cluster.controller.clone()),
    );
    let client = ControlClient::new(cluster.sim.runtime(owner), vec![LEADER]);

    let progress = cluster.sim.block_on({
        let client = client.clone();
        async move { client.drain_node(owner).await }
    });
    assert_eq!(
        progress,
        Ok(false),
        "a committed transfer awaiting receiver acknowledgement is progress"
    );

    let refusal = cluster
        .sim
        .block_on(async move { client.drain_node(NodeId(99)).await });
    assert!(
        matches!(refusal, Err(orbita_core::Error::Internal(ref reason)) if reason.contains("unknown node")),
        "an actual refusal must remain an error, got {refusal:?}"
    );
}

#[test]
fn a_worker_cannot_overwrite_a_registered_leader_identity() {
    let cluster = Cluster::start(12);
    let controller = cluster.controller.clone();
    let result = cluster.sim.block_on(async move {
        controller
            .record_status(
                LEADER,
                NodeStatus::joining(NodeRole::Worker, "10.0.0.99:7000"),
            )
            .await
    });
    assert!(
        matches!(result, Err(Error::InvalidArgument(ref message)) if message.contains("cannot report as")),
        "a cross-role registration must be rejected, got {result:?}"
    );
    let controller = cluster.controller.clone();
    let state = cluster
        .sim
        .block_on(async move { controller.snapshot().await });
    let record = state.node(LEADER).cloned().expect("leader remains");
    assert_eq!(record.role, NodeRole::Leader);
}

#[test]
fn catch_up_requires_the_local_log_and_controller_to_reach_the_authority() {
    let cluster = Cluster::start(13);
    let current = cluster.controller.clone();
    let authority = cluster
        .sim
        .block_on(async move { current.commit_index().await });

    let caught_up = cluster.controller.clone();
    assert_eq!(
        cluster
            .sim
            .block_on(async move { caught_up.catch_up_through(authority).await }),
        Ok(true)
    );

    let lagging = cluster.controller.clone();
    assert_eq!(
        cluster
            .sim
            .block_on(async move { lagging.catch_up_through(u64::MAX).await }),
        Ok(false)
    );
}

#[test]
fn a_client_with_no_reachable_leader_reports_unavailable_rather_than_hanging() {
    let cluster = Cluster::start(12);
    let worker = cluster.sim.runtime(WORKERS[0]);
    // Nobody registered a control handler, so every call fails at the peer.
    let client = ControlClient::new(worker, vec![LEADER]);

    let fetched = cluster
        .sim
        .block_on(async move { client.fetch_map().await });

    assert!(
        matches!(fetched, Err(orbita_core::Error::Unavailable(_))),
        "a worker must be able to tell an outage from a refusal, got {fetched:?}"
    );
}

/// A consensus log that counts how often the controller asks who the leader
/// is.
///
/// `Controller::view` asks exactly once, which makes this an honest counter of
/// how many observation snapshots one admin call took. That number is the
/// thing the describe endpoint got wrong: it is meant to be a constant, and it
/// was one per keyspace.
struct CountingLog {
    inner: Arc<SingleNodeLog<SimRuntime>>,
    views: Arc<AtomicU64>,
}

impl ConsensusLog for CountingLog {
    async fn propose(&self, command: ControlCommand) -> Result<orbita_control::LogIndex, Error> {
        self.inner.propose(command).await
    }

    async fn commit_index(&self) -> orbita_control::LogIndex {
        self.inner.commit_index().await
    }

    async fn subscribe(
        &self,
        after: orbita_control::LogIndex,
    ) -> Result<Vec<orbita_control::LogEntry>, Error> {
        self.inner.subscribe(after).await
    }

    async fn leader_barrier(&self) -> Result<orbita_control::LogIndex, Error> {
        self.inner.leader_barrier().await
    }

    async fn is_leader(&self) -> bool {
        self.inner.is_leader().await
    }

    async fn leader(&self) -> Option<NodeId> {
        self.views.fetch_add(1, Ordering::SeqCst);
        self.inner.leader().await
    }
}

/// The describe endpoint, with a counter on the snapshots it takes.
fn counted_admin(
    seed: u64,
    keyspaces: usize,
) -> (
    Simulation,
    orbita_control::AdminService<SimRuntime, CountingLog>,
    Arc<AtomicU64>,
) {
    let sim = Simulation::with_config(SimConfig::new(seed));
    let leader = sim.add_node(LEADER);
    for worker in WORKERS {
        sim.add_node(worker);
    }

    let opening = leader.clone();
    let single = sim.block_on(async move { SingleNodeLog::open(&opening).await.expect("open") });
    let views = Arc::new(AtomicU64::new(0));
    let log = Arc::new(CountingLog {
        inner: single,
        views: Arc::clone(&views),
    });

    let controller = Controller::new(leader, Arc::clone(&log), ControlConfig::default());
    let spec = BootstrapSpec {
        keyspace: "default".into(),
        config: KeyspaceConfig::default(),
        leaders: vec![(LEADER, "10.0.0.1:7000".into())],
        workers: WORKERS
            .iter()
            .map(|w| (*w, format!("10.0.0.{w}:7000")))
            .collect(),
    };
    let bootstrapping = controller.clone();
    assert_eq!(
        sim.block_on(async move { bootstrapping.bootstrap(&spec).await }),
        Ok(true)
    );

    for extra in 1..keyspaces {
        let controller = controller.clone();
        let name = format!("tenant-{extra}");
        sim.block_on(async move {
            controller
                .create_keyspace(&name, KeyspaceConfig::default())
                .await
                .expect("creating a keyspace");
        });
    }

    let admin = orbita_control::AdminService::new(controller);
    (sim, admin, views)
}

#[test]
fn describe_takes_one_observation_snapshot_no_matter_how_many_keyspaces() {
    // The defect this pins: describe built each keyspace row from its own
    // freshly rebuilt view, so the work grew with keyspaces times partitions
    // and, worse, one response could carry rows sampled at different
    // instants. The snapshot count has to be flat in the keyspace count.
    let snapshots = |keyspaces: usize| {
        let (sim, admin, views) = counted_admin(21, keyspaces);
        views.store(0, Ordering::SeqCst);
        sim.block_on(async move {
            admin
                .describe_cluster(tonic::Request::new(
                    orbita_proto::v1::DescribeClusterRequest::default(),
                ))
                .await
                .expect("describe answers")
        });
        views.load(Ordering::SeqCst)
    };

    let one = snapshots(1);
    let eight = snapshots(8);
    assert_eq!(
        one, eight,
        "describing eight keyspaces took {eight} snapshots against {one} for a \
         single keyspace; the endpoint is rebuilding the cluster view per row"
    );
}

#[test]
fn a_describe_response_agrees_with_itself_about_what_each_keyspace_stores() {
    // The consistency half of the same defect. Keyspace totals and the
    // partition rows beside them have to come from one observation, or an
    // operator reading a describe is comparing two different moments and
    // cannot tell that they are.
    let (sim, admin, _) = counted_admin(22, 4);
    let described = sim
        .block_on(async move {
            admin
                .describe_cluster(tonic::Request::new(
                    orbita_proto::v1::DescribeClusterRequest::default(),
                ))
                .await
        })
        .expect("describe answers")
        .into_inner();

    assert_eq!(described.keyspaces.len(), 4, "every keyspace is described");
    for keyspace in &described.keyspaces {
        let mine: Vec<_> = described
            .partitions
            .iter()
            .filter(|p| p.keyspace_id == keyspace.id)
            .collect();
        assert_eq!(
            keyspace.partition_count as usize,
            mine.len(),
            "keyspace {} counts partitions the same response does not list",
            keyspace.name
        );
        assert_eq!(
            keyspace.stored_bytes,
            mine.iter().filter_map(|p| p.size_bytes).sum::<u64>(),
            "keyspace {} totals bytes the partitions beside it do not add up to",
            keyspace.name
        );
        assert_eq!(
            keyspace.partitions_without_size as usize,
            mine.iter().filter(|p| p.size_bytes.is_none()).count(),
            "keyspace {} disagrees with the same response about how many of \
             its partitions had nobody reporting a size",
            keyspace.name
        );
    }
}

#[test]
fn a_fenced_partition_reports_unknown_progress_rather_than_zeroes() {
    // #53 made a partition sit fenced and unowned for as long as it takes
    // every surviving replica to report past the fence, which is a state an
    // operator now meets routinely during a failover. There is no owner to
    // have reported size, position, or index, so none of those are known.
    //
    // Zero is the wrong answer for all three in the same way it was wrong for
    // an unreported index: a partition holding a gigabyte reads as empty, and
    // a replica set mid-failover reads as perfectly caught up, at exactly the
    // moment somebody is deciding whether losing this partition is cheap.
    let cluster = Cluster::start(31);
    let partition = cluster.only_partition();
    let deposed = cluster.owner_of(partition).expect("an owner");
    cluster.set_index_bytes(deposed, 8_192);
    cluster.sim.run_for(Duration::from_secs(2));

    let controller = cluster.controller.clone();
    let before = cluster.sim.block_on(async move { controller.view().await });
    let described = before
        .partitions
        .iter()
        .find(|p| p.info.id == partition)
        .expect("the partition is described");
    assert_eq!(
        described.index_bytes,
        Some(8_192),
        "a served partition reports what its owner measured"
    );
    assert!(described.size_bytes.is_some());
    assert!(described.committed_lamport.is_some());

    // Take the owner away. Nobody is left to report on this partition.
    cluster.sim.crash(deposed);
    assert!(cluster.run_until(Duration::from_secs(5), |c| {
        c.owner_of(partition).is_none()
    }));

    let controller = cluster.controller.clone();
    let view = cluster.sim.block_on(async move { controller.view().await });
    let described = view
        .partitions
        .iter()
        .find(|p| p.info.id == partition)
        .expect("a fenced partition is still described");

    assert_eq!(described.info.owner, None, "the fence removed the owner");
    assert_eq!(
        described.size_bytes, None,
        "a partition with no owner reporting has an unknown size, not an \
         empty one"
    );
    assert_eq!(
        described.committed_lamport, None,
        "and an unknown committed position, not lamport zero"
    );
    assert_eq!(described.index_bytes, None);
}

#[test]
fn a_replica_that_has_not_reported_is_unknown_rather_than_at_lamport_zero() {
    // #79 named this state `Unestablished` and made the case for it on the
    // owner's side: an absent answer read as a healthy one hides a cliff. The
    // describe surface had the mirror-image bug. Reading silence as lamport
    // zero invents a maximally-behind replica out of a node that may be
    // perfectly current, and during a failover that is the number somebody
    // decides a promotion against.
    //
    // A leader that has just taken over is the reachable version of this: the
    // map names replicas it has never heard a word from.
    let cluster = Cluster::start(45);
    let partition = cluster.only_partition();
    for worker in WORKERS {
        cluster.set_progress(worker, 42);
    }
    assert!(cluster.run_until(Duration::from_secs(5), |c| {
        c.map()
            .partition(partition)
            .is_some_and(|info| !info.replicas.is_empty())
    }));

    cluster.sim.crash(LEADER);
    let restarted = cluster.sim.restart(LEADER, DiskPolicy::Intact);
    let view = cluster.sim.block_on(async move {
        let log = Log::open(&restarted).await?;
        let controller = Controller::new(restarted, log, ControlConfig::default());
        controller.recover().await?;
        Ok::<_, Error>(controller.view().await)
    });
    let view = view.expect("the new leader describes the cluster");

    let described = view
        .partitions
        .iter()
        .find(|p| p.info.id == partition)
        .expect("the partition is described");
    assert!(
        !described.replica_progress.is_empty(),
        "the map still names replicas for the new leader to be silent about"
    );
    for replica in &described.replica_progress {
        assert_eq!(
            replica.durable_lamport, None,
            "a replica this leader has not heard from has no position, not \
             position zero"
        );
        assert_eq!(replica.applied_lamport, None);
    }
    assert_eq!(
        described.committed_lamport, None,
        "and no owner has reported a committed prefix to it either"
    );
}

#[test]
fn a_partitions_committed_position_comes_from_the_prefix_a_quorum_confirmed() {
    // #79 introduced a committed prefix distinct from a node's durable
    // position, and gave a draining owner `quiesce`, which truncates the
    // writes it holds alone and so makes its durable position go *down*.
    // Those writes were never acknowledged to anyone, so nothing was lost,
    // but a describe sourcing the partition's committed column from the
    // durable position rendered a planned shutdown as a partition moving
    // backwards. Only the prefix is safe here, and it only ever rises.
    let cluster = Cluster::start(43);
    let partition = cluster.only_partition();
    let owner = cluster.owner_of(partition).expect("an owner");
    cluster.set_progress(owner, 100);
    cluster.set_committed(owner, Some(90));
    cluster.sim.run_for(Duration::from_secs(2));

    let controller = cluster.controller.clone();
    let view = cluster.sim.block_on(async move { controller.view().await });
    let described = view
        .partitions
        .iter()
        .find(|p| p.info.id == partition)
        .expect("the partition is described");
    assert_eq!(
        described.committed_lamport,
        Some(Lamport(90)),
        "the committed column is the quorum-confirmed prefix, not the \
         owner's durable position"
    );

    // Quiesce: the durable position drops to meet the prefix. The prefix
    // does not move, so neither does what an operator is shown.
    cluster.set_progress(owner, 90);
    cluster.sim.run_for(Duration::from_secs(2));

    let controller = cluster.controller.clone();
    let view = cluster.sim.block_on(async move { controller.view().await });
    let described = view
        .partitions
        .iter()
        .find(|p| p.info.id == partition)
        .expect("the partition is described");
    assert_eq!(
        described.committed_lamport,
        Some(Lamport(90)),
        "a quiescing owner must not render as though it lost data"
    );
}

#[test]
fn an_owner_that_cannot_report_a_committed_prefix_leaves_it_unknown() {
    // What a worker one version behind looks like: its report came through a
    // status method with no room for the prefix. Borrowing its durable
    // position would put a number that can go backwards under a name that
    // promises it cannot.
    let cluster = Cluster::start(44);
    let partition = cluster.only_partition();
    let owner = cluster.owner_of(partition).expect("an owner");
    cluster.set_progress(owner, 100);
    cluster.set_committed(owner, None);
    cluster.sim.run_for(Duration::from_secs(2));

    let controller = cluster.controller.clone();
    let view = cluster.sim.block_on(async move { controller.view().await });
    let described = view
        .partitions
        .iter()
        .find(|p| p.info.id == partition)
        .expect("the partition is described");
    assert_eq!(described.committed_lamport, None);
}

/// Wraps an admin message in a request, optionally carrying a bearer secret.
fn admin_request<T>(message: T, secret: Option<&str>) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    if let Some(secret) = secret {
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {secret}")
                .parse()
                .expect("a bearer token is a valid header value"),
        );
    }
    request
}

/// A leader's admin surface with authentication on and a root configured.
///
/// Nothing has been created through the credential log, so the only identity
/// that can pass is the config overlay. This is the bootstrap position.
fn rooted_admin(
    seed: u64,
    root: Option<&str>,
) -> (Cluster, orbita_control::AdminService<SimRuntime, Log>) {
    let cluster = Cluster::start(seed);
    let admin = orbita_control::AdminService::new(cluster.controller.clone())
        .require_auth(true)
        .root_credential(root.map(orbita_control::root_secret_hash));
    (cluster, admin)
}

#[test]
fn a_configured_root_bootstraps_admin_before_any_credential_exists() {
    // The chicken-and-egg: with auth on, creating the first credential needs a
    // credential, and none exists in the log yet. The config root breaks it.
    let (cluster, admin) = rooted_admin(90, Some("root-secret"));

    let created = cluster
        .sim
        .block_on({
            let admin = admin.clone();
            async move {
                admin
                    .create_credential(admin_request(
                        orbita_proto::v1::CreateCredentialRequest {
                            keyspaces: vec!["default".into()],
                            permissions: vec![orbita_proto::v1::Permission::Write as i32],
                            description: "the first real credential".into(),
                            expires_at_millis: None,
                        },
                        Some("root-secret"),
                    ))
                    .await
            }
        })
        .expect("the root creates the first credential")
        .into_inner();

    // The credential the root just minted is a tenant credential: it may write
    // its own keyspace on the data plane, but it must NOT administer the
    // cluster. Deriving admin from a per-keyspace write is the cross-tenant
    // escalation this boundary exists to prevent, so the admin surface refuses
    // it with PermissionDenied even though it is a real, unexpired credential.
    let denied = cluster.sim.block_on({
        let admin = admin.clone();
        let secret = created.secret.clone();
        async move {
            admin
                .list_keyspaces(admin_request(
                    orbita_proto::v1::ListKeyspacesRequest::default(),
                    Some(&secret),
                ))
                .await
        }
    });
    assert_eq!(
        denied
            .expect_err("a tenant credential cannot administer the cluster")
            .code(),
        tonic::Code::PermissionDenied,
        "a per-keyspace write must not confer cluster administration"
    );

    // Only the root administers, and it keeps working after minting tenants.
    let listed = cluster.sim.block_on(async move {
        admin
            .list_keyspaces(admin_request(
                orbita_proto::v1::ListKeyspacesRequest::default(),
                Some("root-secret"),
            ))
            .await
    });
    assert!(listed.is_ok(), "the root administers the cluster");
}

#[test]
fn a_wrong_root_secret_is_unauthenticated_at_the_admin_surface() {
    let (cluster, admin) = rooted_admin(91, Some("root-secret"));
    let denied = cluster.sim.block_on(async move {
        admin
            .list_keyspaces(admin_request(
                orbita_proto::v1::ListKeyspacesRequest::default(),
                Some("not-the-root"),
            ))
            .await
    });
    assert_eq!(
        denied.expect_err("a wrong root secret is refused").code(),
        tonic::Code::Unauthenticated,
        "a wrong secret is unauthenticated, the same code as the rest of the path"
    );
}

#[test]
fn without_a_configured_root_admin_is_refused_before_any_credential_exists() {
    // The overlay exists only when configured. With auth on, no root, and an
    // empty credential log, admin has nothing to authorize against.
    let (cluster, admin) = rooted_admin(92, None);
    let denied = cluster.sim.block_on(async move {
        admin
            .list_keyspaces(admin_request(
                orbita_proto::v1::ListKeyspacesRequest::default(),
                Some("root-secret"),
            ))
            .await
    });
    assert_eq!(
        denied
            .expect_err("no root means no bootstrap identity")
            .code(),
        tonic::Code::Unauthenticated,
    );
}

#[test]
fn auth_disabled_admin_passes_through_regardless_of_root() {
    // Auth off: the root is irrelevant and a request with no header is served.
    let cluster = Cluster::start(93);
    let admin = orbita_control::AdminService::new(cluster.controller.clone());
    let listed = cluster.sim.block_on(async move {
        admin
            .list_keyspaces(admin_request(
                orbita_proto::v1::ListKeyspacesRequest::default(),
                None,
            ))
            .await
    });
    assert!(
        listed.is_ok(),
        "with auth off every request passes, root or no root"
    );
}
