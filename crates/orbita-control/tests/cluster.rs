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

use orbita_control::{
    binary_speaks, BootstrapSpec, ClusterState, ClusterVersion, ConsensusLog, ControlClient,
    ControlCommand, ControlConfig, ControlService, Controller, KeyspaceConfig, NodeRole,
    NodeStatus, PartitionProgress, RegistrationOutcome, SingleNodeLog, VersionRange,
};
use orbita_core::{Epoch, Error, Lamport, MapVersion, NodeId, PartitionId, PartitionMap};
use orbita_proto::v1::admin_server::Admin as _;
use orbita_runtime::{Clock, Runtime, ServiceId, Transport};
use orbita_sim::{check_seeds, DiskFaults, DiskPolicy, Failure, SimConfig, SimRuntime, Simulation};

use std::collections::HashMap;
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
    reporting: Arc<Mutex<HashMap<NodeId, bool>>>,
    /// What each worker claims its index costs, per partition it holds. Set
    /// by the tests so that the describe surface can be checked against a
    /// known answer rather than against whatever the storage layer produced.
    ///
    /// `None` is what a worker looks like when its report came through a
    /// status method that cannot carry the measurement, which is every worker
    /// running an older binary for the length of a rolling upgrade.
    index_bytes: Arc<Mutex<HashMap<NodeId, Option<u64>>>>,
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
            reporting: Arc::new(Mutex::new(
                WORKERS.into_iter().map(|node| (node, true)).collect(),
            )),
            index_bytes: Arc::new(Mutex::new(HashMap::new())),
        };
        cluster.spawn_sweep(&leader);
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
        let reporting = Arc::clone(&self.reporting);
        let index_bytes = Arc::clone(&self.index_bytes);

        runtime.spawn(async move {
            loop {
                let should_report = *reporting
                    .lock()
                    .expect("reporting lock poisoned")
                    .get(&node)
                    .unwrap_or(&false);
                if should_report {
                    let map = controller.partition_map().await;
                    let lamport = Lamport(
                        *progress
                            .lock()
                            .expect("progress lock poisoned")
                            .get(&node)
                            .unwrap_or(&0),
                    );
                    let index = *index_bytes
                        .lock()
                        .expect("index bytes lock poisoned")
                        .get(&node)
                        .unwrap_or(&Some(0));
                    let partitions: Vec<PartitionProgress> = map
                        .held_by(node)
                        .map(|info| PartitionProgress {
                            partition: info.id,
                            durable_lamport: lamport,
                            applied_lamport: lamport,
                            size_bytes: 0,
                            index_bytes: index,
                        })
                        .collect();
                    let status = NodeStatus {
                        role: NodeRole::Worker,
                        address: format!("10.0.0.{node}:7000"),
                        map_version: map.version(),
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
                    let _ = controller.record_status(node, status).await;
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

    fn set_index_bytes(&self, node: NodeId, bytes: u64) {
        self.index_bytes
            .lock()
            .expect("index bytes lock poisoned")
            .insert(node, Some(bytes));
    }

    /// Makes a worker report the way one whose heartbeat went through a
    /// status method without an index field does.
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
        assert_eq!(
            replica.durable_lamport,
            Lamport(42),
            "WAL lag is measured from the durable position, so it has to be \
             reported and not inferred from what was applied"
        );
        assert_eq!(replica.applied_lamport, Lamport(42));
    }
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
            Ok(())
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
                (Some(fenced), Some(promoted)) if fenced < promoted => Ok(()),
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
            Ok(())
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
            Ok(())
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
            Ok(())
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
            Ok(())
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
            Ok(())
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
    let promoted = cluster.sim.block_on(async move {
        let log = Log::open(&restarted).await?;
        let controller = Controller::new(restarted, log, ControlConfig::default());
        controller.recover().await?;
        controller.tick().await?;
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
                .map_err(|reason| cluster.sim.failure(reason))
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

            let controller = cluster.controller.clone();
            let drained = cluster
                .sim
                .block_on(async move { controller.drain_node(owner).await });
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

            let controller = cluster.controller.clone();
            let result = cluster
                .sim
                .block_on(async move { controller.drain_node(owner).await });
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
            Ok(())
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

            let controller = cluster.controller.clone();
            let first = cluster
                .sim
                .block_on(async move { controller.drain_node(draining).await });
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
            Ok(())
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
            if progress != Some(Some(Lamport(77))) {
                return Err(cluster.sim.failure(format!(
                    "incompatible owner's heartbeat progress was not retained: {progress:?}"
                )));
            }
            Ok(())
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
            Ok(())
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
            Ok(())
        },
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
                .map_err(|reason| cluster.sim.failure(reason))
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
                Ok(after) if after == before => Ok(()),
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
