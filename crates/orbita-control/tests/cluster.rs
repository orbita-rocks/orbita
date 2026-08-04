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
    NodeStatus, PartitionProgress, SingleNodeLog, VersionRange,
};
use orbita_core::{Epoch, Lamport, MapVersion, NodeId, PartitionId, PartitionMap};
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

        runtime.spawn(async move {
            loop {
                let map = controller.partition_map().await;
                let lamport = Lamport(
                    *progress
                        .lock()
                        .expect("progress lock poisoned")
                        .get(&node)
                        .unwrap_or(&0),
                );
                let partitions: Vec<PartitionProgress> = map
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
                    map_version: map.version(),
                    speaks: orbita_control::binary_speaks(),
                    partitions,
                };
                let _ = controller.record_status(node, status).await;
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
            Ok(())
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
                .map_err(|reason| cluster.sim.failure(reason))
        },
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
            Ok(())
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
                        partitions: vec![],
                    },
                )
                .await
        }
    });
    assert_eq!(reported, Ok(()));
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
