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

use orbita_control::{
    binary_speaks, BootstrapSpec, ClusterState, ClusterVersion, ConsensusLog, ControlClient,
    ControlCommand, ControlConfig, ControlService, Controller, KeyspaceConfig, NodeHealth,
    NodeRole, NodeStatus, PartitionProgress, RegistrationOutcome, SingleNodeLog,
    StatusReportResponse, VersionRange,
};
use orbita_core::{Epoch, Error, Lamport, MapVersion, NodeId, PartitionId, PartitionMap};
use orbita_runtime::{Clock, Runtime, ServiceId, Transport};
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

/// How far each worker claims to have got. The test sets these so that
/// promotion can be checked against a known answer rather than against
/// whichever node happened to be first.
type Progress = Arc<Mutex<HashMap<NodeId, u64>>>;

struct Cluster {
    sim: Simulation,
    controller: Ctl,
    log: Arc<Log>,
    progress: Progress,
    readiness: Arc<Mutex<HashMap<NodeId, bool>>>,
    draining: Arc<Mutex<HashMap<NodeId, bool>>>,
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
            progress: Arc::new(Mutex::new(HashMap::new())),
            readiness: Arc::new(Mutex::new(
                WORKERS.into_iter().map(|node| (node, true)).collect(),
            )),
            draining: Arc::new(Mutex::new(
                WORKERS.into_iter().map(|node| (node, false)).collect(),
            )),
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

        runtime.spawn(async move {
            loop {
                match wiring {
                    Wiring::Direct => held = controller.partition_map().await,
                    Wiring::Networked => {
                        if let Ok(Some(fresher)) = client.fetch_map_if_newer(held.version()).await {
                            held = fresher;
                        }
                    }
                }
                let lamport = Lamport(
                    *progress
                        .lock()
                        .expect("progress lock poisoned")
                        .get(&node)
                        .unwrap_or(&0),
                );
                let partitions: Vec<PartitionProgress> = held
                    .held_by(node)
                    .map(|info| PartitionProgress {
                        partition: info.id,
                        durable_lamport: lamport,
                        applied_lamport: lamport,
                        size_bytes: 0,
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
                clock.sleep(interval).await;
            }
        });
    }

    fn set_progress(&self, node: NodeId, lamport: u64) {
        self.progress
            .lock()
            .expect("progress lock poisoned")
            .insert(node, lamport);
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
    /// Deliberately not here: anything comparing Lamport positions between
    /// replicas. This harness makes those numbers up, so a condition written
    /// against them would be testing the harness. What the control plane
    /// genuinely knows about catch-up is the map version each node reports it
    /// is routing on, and that is what is used instead.
    fn unmet(&self) -> Vec<Unmet> {
        const NOTICED: &str = "the leader has had time to notice the last fault";
        const OWNED: &str = "every partition has an owner";
        const LIVE_OWNER: &str = "every owner is a node the leader believes is alive";
        const NO_DEAD_REPLICAS: &str = "no replica set names a node the leader gave up on";
        const REDUNDANT: &str = "every partition holds as many replicas as the cluster can give it";
        const CAUGHT_UP: &str = "every surviving worker is routing on the current map";
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
                    CAUGHT_UP,
                    format!(
                        "healthy worker {} last reported map version {:?}, not {map_version}",
                        node.record.id, node.reported_map_version
                    ),
                ));
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
fn bootstrapping_a_cluster_that_already_has_state_changes_nothing() {
    let cluster = Cluster::start(2);
    let before = cluster.map();

    let controller = cluster.controller.clone();
    let spec = BootstrapSpec::dev(LEADER, "10.0.0.1:7000");
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
            if gap + Duration::from_millis(100) < drain {
                return Err(cluster.sim.failure(format!(
                    "promoted {gap:?} after the fence, which does not wait out a {drain:?} lease"
                )));
            }
            cluster.converged()
        },
    );
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
fn a_split_leaves_every_key_owned_at_every_committed_instant() {
    check_seeds(
        "a_split_leaves_every_key_owned_at_every_committed_instant",
        16,
        |seed| {
            let cluster = Cluster::start(seed);
            let parent = cluster.only_partition();
            let keyspace = cluster
                .map()
                .partition(parent)
                .expect("the partition")
                .keyspace;

            let controller = cluster.controller.clone();
            let split = cluster.sim.block_on(async move {
                controller
                    .split_partition(parent, Some(bytes::Bytes::from_static(b"m")))
                    .await
            });
            let Ok((lower, upper)) = split else {
                return Err(cluster.sim.failure(format!("the split failed: {split:?}")));
            };
            cluster.sim.run_for(Duration::from_secs(1));

            if let Err(reason) = coverage_holds_through_every_entry(&cluster.entries()) {
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
                return Err(cluster
                    .sim
                    .failure("the parent partition outlived the split"));
            }
            cluster.converged()
        },
    );
}

#[test]
fn the_partition_map_survives_a_full_restart() {
    check_seeds("the_partition_map_survives_a_full_restart", 16, |seed| {
        let cluster = Cluster::start(seed);
        let partition = cluster.only_partition();

        // Move the map somewhere non-trivial first, so that surviving means
        // more than "the bootstrap ran again".
        let controller = cluster.controller.clone();
        let split = cluster.sim.block_on(async move {
            controller
                .split_partition(partition, Some(bytes::Bytes::from_static(b"m")))
                .await
        });
        if split.is_err() {
            return Err(cluster.sim.failure(format!("the split failed: {split:?}")));
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
            if progress != Some(Lamport(77)) {
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

/// Ignored because it reaches the defect in issue #76 on 11 of the first
/// 20000 seeds, 2807 being the lowest, and a scenario that goes red on the
/// nightly batch is a scenario people learn to ignore for real. The invariant
/// is not relaxed to get it green: the check is right and the cluster is
/// wrong. Remove the ignore with the fix.
#[test]
#[ignore = "finds the known liveness defect tracked by issue #76"]
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
/// The partition stays unavailable forever, because each of the three sweep
/// stages declines it in turn: `promote_drained_partitions` reads candidates
/// out of an empty `replicas`, `place_unowned_partitions` only looks at
/// `Unowned` partitions, and `repair_replica_sets` only looks at `Serving`
/// ones. Safe, and stuck.
///
/// Ignored rather than deleted: it is the tracking case for issue #76, and
/// removing the ignore is how the fix proves itself. Fixing it is a decision
/// about whether a fenced owner may be promoted again, which belongs in its
/// own change rather than smuggled into a test harness.
#[test]
#[ignore = "known liveness defect, tracked by issue #76"]
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
            let partition = cluster.only_partition();
            cluster.sim.run_for(Duration::from_secs(3));

            let controller = cluster.controller.clone();
            let _ = cluster.sim.block_on(async move {
                controller
                    .split_partition(partition, Some(bytes::Bytes::from_static(b"m")))
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
