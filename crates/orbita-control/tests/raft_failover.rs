//! Ownership failover through a real Raft group and real worker WALs.
//!
//! Pinned replay:
//! `ORBITA_SIM_SEED=7 cargo test -p orbita-control --test raft_failover a_promoted_owner_fences_partitioned_old_owner_writes_and_restores_liveness -- --exact --nocapture`.

use orbita_control::{
    BootstrapSpec, ClusterState, ConsensusLog, ControlCommand, ControlConfig, Controller,
    KeyspaceConfig, RaftLog,
};
use orbita_core::{Epoch, Error, Lamport, NodeId, PartitionId};
use orbita_runtime::{Runtime, ServiceId, Transport};
use orbita_sim::{check_seeds, Failure, SimRuntime, Simulation};
use orbita_wal::{PartitionLog, Wal, WalConfig, WalOp, WalService, DEFAULT_SEGMENT_TARGET_BYTES};

use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const CONTROL: [NodeId; 3] = [NodeId(1), NodeId(2), NodeId(3)];
const WORKERS: [NodeId; 3] = [NodeId(11), NodeId(12), NodeId(13)];
const ELECTION_GRACE: Duration = Duration::from_secs(10);
const REPLICATION_GRACE: Duration = Duration::from_secs(2);
const WAL_DIR: &str = "failover/wal";

type Ctl = Controller<SimRuntime, RaftLog>;

struct Group {
    sim: Simulation,
    logs: Vec<Arc<RaftLog>>,
    controllers: Vec<Ctl>,
    partition: PartitionId,
}

impl Group {
    fn start(seed: u64) -> Self {
        let sim = Simulation::new(seed);
        for node in CONTROL.into_iter().chain(WORKERS) {
            sim.add_node(node);
        }

        let logs: Vec<_> = CONTROL
            .iter()
            .map(|node| {
                let runtime = sim.runtime(*node);
                sim.block_on(async move {
                    RaftLog::open(&runtime, &CONTROL)
                        .await
                        .expect("open raft log")
                })
            })
            .collect();
        sim.run_for(ELECTION_GRACE);
        let leader = exactly_one_leader(&sim, &logs, &CONTROL).expect("initial election");

        let controllers: Vec<_> = CONTROL
            .iter()
            .zip(&logs)
            .map(|(node, log)| {
                Controller::new(
                    sim.runtime(*node),
                    Arc::clone(log),
                    ControlConfig::default(),
                )
            })
            .collect();
        let leader_controller = controllers[index_of(leader)].clone();
        let spec = BootstrapSpec {
            keyspace: "default".into(),
            config: KeyspaceConfig::default(),
            leaders: CONTROL
                .iter()
                .map(|node| (*node, format!("10.0.0.{node}:7000")))
                .collect(),
            workers: WORKERS
                .iter()
                .map(|node| (*node, format!("10.0.0.{node}:7000")))
                .collect(),
        };
        sim.block_on(async move { leader_controller.bootstrap(&spec).await })
            .expect("bootstrap through raft");
        sim.run_for(REPLICATION_GRACE);
        for controller in &controllers {
            let controller = controller.clone();
            sim.block_on(async move { controller.recover().await })
                .expect("apply bootstrap");
        }

        let controller = controllers[index_of(leader)].clone();
        let map = sim.block_on(async move { controller.partition_map().await });
        let partition = map.partitions().next().expect("bootstrap partition").id;
        let group = Self {
            sim,
            logs,
            controllers,
            partition,
        };
        group.assert_invariants().expect("bootstrap invariants");
        group
    }

    fn controller(&self, node: NodeId) -> Ctl {
        self.controllers[index_of(node)].clone()
    }

    fn leader(&self, members: &[NodeId]) -> Result<NodeId, Failure> {
        exactly_one_leader(&self.sim, &self.logs, members)
    }

    fn elect(&self, members: &[NodeId]) -> Result<NodeId, Failure> {
        self.sim.run_for(ELECTION_GRACE);
        self.leader(members)
    }

    fn recover(&self, node: NodeId) -> Result<(), Failure> {
        let controller = self.controller(node);
        self.sim
            .block_on(async move { controller.recover().await })
            .map_err(|error| {
                self.sim
                    .failure(format!("controller {node} did not catch up: {error}"))
            })
    }

    fn owner_epoch(&self, node: NodeId) -> (Option<NodeId>, Epoch) {
        let controller = self.controller(node);
        let partition = self.partition;
        let map = self
            .sim
            .block_on(async move { controller.partition_map().await });
        let info = map.partition(partition).expect("partition remains present");
        (info.owner, info.epoch)
    }

    fn commands(&self, node: NodeId) -> Vec<ControlCommand> {
        let log = Arc::clone(&self.logs[index_of(node)]);
        self.sim
            .block_on(async move { log.subscribe(0).await })
            .expect("read committed raft entries")
            .into_iter()
            .map(|entry| entry.command)
            .collect()
    }

    fn assert_invariants(&self) -> Result<(), Failure> {
        let histories: Vec<_> = CONTROL
            .iter()
            .filter(|node| self.sim.is_up(**node))
            .map(|node| (*node, self.commands(*node)))
            .collect();
        for (node, commands) in &histories {
            check_history(commands).map_err(|reason| {
                self.sim.failure(format!(
                    "control node {node} broke an ownership invariant: {reason}"
                ))
            })?;
        }
        for (left_node, left) in &histories {
            for (right_node, right) in &histories {
                let shared = left.len().min(right.len());
                if left[..shared] != right[..shared] {
                    return Err(self.sim.failure(format!(
                        "committed histories diverged between {left_node} and {right_node}"
                    )));
                }
            }
        }
        Ok(())
    }
}

fn index_of(node: NodeId) -> usize {
    CONTROL
        .iter()
        .position(|candidate| *candidate == node)
        .expect("control group member")
}

fn exactly_one_leader(
    sim: &Simulation,
    logs: &[Arc<RaftLog>],
    members: &[NodeId],
) -> Result<NodeId, Failure> {
    let leaders: Vec<_> = members
        .iter()
        .filter(|node| sim.is_up(**node))
        .filter_map(|node| {
            let log = Arc::clone(&logs[index_of(*node)]);
            sim.block_on(async move { log.is_leader().await })
                .then_some(*node)
        })
        .collect();
    let [leader] = leaders[..] else {
        return Err(sim.failure(format!(
            "expected one leader among {members:?}, found {leaders:?}"
        )));
    };
    Ok(leader)
}

fn check_history(commands: &[ControlCommand]) -> Result<(), String> {
    let mut state = ClusterState::new();
    let mut epochs = BTreeMap::new();
    let mut owners = BTreeMap::new();
    for (index, command) in commands.iter().enumerate() {
        let _ = state.apply(command);
        state
            .map()
            .check_coverage()
            .map_err(|error| format!("coverage failed after entry {index}: {error}"))?;
        for info in state.map().partitions() {
            if let Some(previous) = epochs.insert(info.id, info.epoch) {
                if info.epoch < previous {
                    return Err(format!(
                        "partition {} regressed from epoch {previous} to {}",
                        info.id, info.epoch
                    ));
                }
            }
            if let Some(owner) = info.owner {
                let key = (info.id, info.epoch);
                if let Some(previous) = owners.insert(key, owner) {
                    if previous != owner {
                        return Err(format!(
                            "partition {} had owners {previous} and {owner} in epoch {}",
                            info.id, info.epoch
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

fn fence(partition: PartitionId, epoch: Epoch) -> ControlCommand {
    ControlCommand::FencePartition {
        partition,
        expect_epoch: epoch,
    }
}

fn put(key: &'static [u8]) -> WalOp {
    WalOp::Put {
        key: Bytes::from_static(key),
        value: Bytes::from_static(b"value"),
        expires_at_millis: None,
    }
}

#[test]
fn crashing_before_epoch_replication_does_not_create_a_phantom_fence() {
    check_seeds(
        "crashing_before_epoch_replication_does_not_create_a_phantom_fence",
        16,
        |seed| {
            let group = Group::start(seed);
            let old_leader = group.leader(&CONTROL)?;
            let survivors: Vec<_> = CONTROL
                .iter()
                .copied()
                .filter(|node| *node != old_leader)
                .collect();
            for survivor in &survivors {
                group.sim.partition(old_leader, *survivor);
            }

            let outcome = Arc::new(Mutex::new(None));
            let sink = Arc::clone(&outcome);
            let controller = group.controller(old_leader);
            let command = fence(group.partition, Epoch(1));
            group.sim.spawn(async move {
                *sink.lock().expect("proposal outcome poisoned") =
                    Some(controller.submit(command).await);
            });
            group.sim.run_for(Duration::from_millis(500));
            group.sim.crash(old_leader);
            group.sim.heal_all();

            let leader = group.elect(&survivors)?;
            group.recover(leader)?;
            if group.owner_epoch(leader) != (Some(WORKERS[0]), Epoch(1)) {
                return Err(group
                    .sim
                    .failure("an epoch command that never reached a quorum took effect"));
            }
            let controller = group.controller(leader);
            let partition = group.partition;
            group
                .sim
                .block_on(async move { controller.submit(fence(partition, Epoch(1))).await })
                .map_err(|error| {
                    group
                        .sim
                        .failure(format!("new leader could not fence: {error}"))
                })?;
            group.assert_invariants()?;
            if group.owner_epoch(leader) != (None, Epoch(2)) {
                return Err(group.sim.failure("the quorum fence did not take effect"));
            }
            Ok(())
        },
    );
}

#[test]
fn a_quorum_committed_epoch_survives_a_crash_before_controller_visibility() {
    check_seeds(
        "a_quorum_committed_epoch_survives_a_crash_before_controller_visibility",
        16,
        |seed| {
            let group = Group::start(seed);
            let old_leader = group.leader(&CONTROL)?;
            let log = Arc::clone(&group.logs[index_of(old_leader)]);
            let partition = group.partition;
            group
                .sim
                .block_on(async move { log.propose(fence(partition, Epoch(1))).await })
                .map_err(|error| group.sim.failure(format!("epoch did not commit: {error}")))?;

            // The command went through the log directly, so no controller has
            // made it visible in its state machine before the crash.
            if group.owner_epoch(old_leader) != (Some(WORKERS[0]), Epoch(1)) {
                return Err(group
                    .sim
                    .failure("the old controller applied an unobserved command"));
            }
            group.sim.crash(old_leader);
            let survivors: Vec<_> = CONTROL
                .iter()
                .copied()
                .filter(|node| *node != old_leader)
                .collect();
            let leader = group.elect(&survivors)?;
            group.recover(leader)?;
            group.assert_invariants()?;
            if group.owner_epoch(leader) != (None, Epoch(2)) {
                return Err(group
                    .sim
                    .failure("the committed epoch disappeared with its proposing leader"));
            }
            Ok(())
        },
    );
}

#[test]
fn a_majority_election_wins_an_inflight_epoch_race_and_fences_the_stale_leader() {
    check_seeds(
        "a_majority_election_wins_an_inflight_epoch_race_and_fences_the_stale_leader",
        32,
        |seed| {
            let group = Group::start(seed);
            let old_leader = group.leader(&CONTROL)?;
            let majority: Vec<_> = CONTROL
                .iter()
                .copied()
                .filter(|node| *node != old_leader)
                .collect();
            for member in &majority {
                group.sim.partition(old_leader, *member);
            }

            let stale_result = Arc::new(Mutex::new(None));
            let sink = Arc::clone(&stale_result);
            let stale = group.controller(old_leader);
            let partition = group.partition;
            group.sim.spawn(async move {
                *sink.lock().expect("stale result poisoned") =
                    Some(stale.submit(fence(partition, Epoch(1))).await);
            });
            group.sim.run_for(Duration::from_millis(500));

            let new_leader = group.elect(&majority)?;
            group.recover(new_leader)?;
            let controller = group.controller(new_leader);
            let partition = group.partition;
            group
                .sim
                .block_on(async move { controller.submit(fence(partition, Epoch(1))).await })
                .map_err(|error| group.sim.failure(format!("majority fence failed: {error}")))?;
            group.sim.heal_all();
            group.sim.run_for(REPLICATION_GRACE);

            let stale = stale_result.lock().expect("stale result poisoned").clone();
            if !matches!(stale, Some(Err(Error::NotLeader { .. }))) {
                return Err(group.sim.failure(format!(
                    "the stale control leader did not step down cleanly: {stale:?}"
                )));
            }
            let commands = group.commands(new_leader);
            let fences = commands
                .iter()
                .filter(|command| {
                    matches!(command, ControlCommand::FencePartition { partition, .. } if *partition == group.partition)
                })
                .count();
            if fences != 1 {
                return Err(group.sim.failure(format!(
                    "the election race committed {fences} epoch fences instead of one"
                )));
            }

            let stale_controller = group.controller(old_leader);
            let stale_attempt = group.sim.block_on(async move {
                stale_controller
                    .submit(ControlCommand::AssignOwner {
                        partition,
                        owner: WORKERS[0],
                        replicas: vec![WORKERS[1], WORKERS[2]],
                        expect_epoch: Epoch(1),
                    })
                    .await
            });
            if !matches!(stale_attempt, Err(Error::NotLeader { .. })) {
                return Err(group.sim.failure(format!(
                    "a stale control leader's command was not refused: {stale_attempt:?}"
                )));
            }
            group.assert_invariants()?;
            Ok(())
        },
    );
}

#[test]
fn a_new_leader_starts_a_fresh_worker_failure_detection_window() {
    check_seeds(
        "a_new_leader_starts_a_fresh_worker_failure_detection_window",
        16,
        |seed| {
            let group = Group::start(seed);
            let old_leader = group.leader(&CONTROL)?;
            let majority: Vec<_> = CONTROL
                .iter()
                .copied()
                .filter(|node| *node != old_leader)
                .collect();
            for member in &majority {
                group.sim.partition(old_leader, *member);
            }

            let new_leader = group.elect(&majority)?;
            let controller = group.controller(new_leader);
            group
                .sim
                .block_on(async move { controller.tick().await })
                .map_err(|error| {
                    group
                        .sim
                        .failure(format!("first leader tick failed: {error}"))
                })?;
            if group.owner_epoch(new_leader) != (Some(WORKERS[0]), Epoch(1)) {
                return Err(group.sim.failure(
                    "a new leader fenced a worker before observing a full failure window",
                ));
            }
            group.assert_invariants()
        },
    );
}

#[test]
fn a_promoted_owner_fences_partitioned_old_owner_writes_and_restores_liveness() {
    // Seed 7 is kept as a stable replay in the module documentation.
    check_seeds(
        "a_promoted_owner_fences_partitioned_old_owner_writes_and_restores_liveness",
        32,
        |seed| {
            let group = Group::start(seed);
            let control_leader = group.leader(&CONTROL)?;
            let partition = group.partition;

            let owner_runtime = group.sim.runtime(WORKERS[0]);
            let old_owner = group
                .sim
                .block_on({
                    let runtime = owner_runtime.clone();
                    async move {
                        Wal::open(
                            runtime,
                            WalConfig::new(partition, WAL_DIR, Epoch(1))
                                .with_replicas(vec![WORKERS[1], WORKERS[2]]),
                        )
                        .await
                    }
                })
                .map_err(|error| group.sim.failure(format!("open old owner WAL: {error}")))?;
            let old_service = WalService::new();
            old_service.register(old_owner.log());
            owner_runtime
                .transport()
                .register(ServiceId::Wal, old_service);

            let mut services = BTreeMap::new();
            for worker in [WORKERS[1], WORKERS[2]] {
                let runtime = group.sim.runtime(worker);
                let log = group
                    .sim
                    .block_on({
                        let runtime = runtime.clone();
                        async move {
                            PartitionLog::open(
                                runtime,
                                WAL_DIR,
                                partition,
                                DEFAULT_SEGMENT_TARGET_BYTES,
                            )
                            .await
                        }
                    })
                    .map_err(|error| {
                        group.sim.failure(format!("open replica {worker}: {error}"))
                    })?;
                let service = WalService::new();
                service.register(log);
                runtime
                    .transport()
                    .register(ServiceId::Wal, service.clone());
                services.insert(worker, service);
            }

            group
                .sim
                .block_on({
                    let owner = Arc::clone(&old_owner);
                    async move { owner.commit(put(b"before")).await }
                })
                .map_err(|error| {
                    group
                        .sim
                        .failure(format!("initial WAL write failed: {error}"))
                })?;

            let controller = group.controller(control_leader);
            group
                .sim
                .block_on(async move { controller.transfer_ownership(partition, WORKERS[1]).await })
                .map_err(|error| {
                    group
                        .sim
                        .failure(format!("ownership transfer failed: {error}"))
                })?;
            group.sim.run_for(REPLICATION_GRACE);
            group.assert_invariants()?;
            let (owner, epoch) = group.owner_epoch(control_leader);
            if owner != Some(WORKERS[1]) || epoch != Epoch(2) {
                return Err(group.sim.failure(format!(
                    "control state promoted {owner:?} at {epoch}, expected {} at epoch 2",
                    WORKERS[1]
                )));
            }

            services[&WORKERS[1]].unregister(partition);
            let promoted = group
                .sim
                .block_on({
                    let runtime = group.sim.runtime(WORKERS[1]);
                    async move {
                        Wal::open(
                            runtime,
                            WalConfig::new(partition, WAL_DIR, Epoch(1))
                                .with_replicas(vec![WORKERS[2], WORKERS[0]]),
                        )
                        .await
                    }
                })
                .map_err(|error| group.sim.failure(format!("open promoted WAL: {error}")))?;
            group
                .sim
                .block_on({
                    let promoted = Arc::clone(&promoted);
                    async move { promoted.promote(epoch).await }
                })
                .map_err(|error| group.sim.failure(format!("apply WAL fence: {error}")))?;

            for node in CONTROL {
                group.sim.partition(WORKERS[0], node);
            }
            let stale_write = group.sim.block_on({
                let owner = Arc::clone(&old_owner);
                async move { owner.commit(put(b"stale")).await }
            });
            if !matches!(stale_write, Err(Error::StaleEpoch { .. })) {
                return Err(group.sim.failure(format!(
                    "the stale partition owner acknowledged a write: {stale_write:?}"
                )));
            }
            if old_owner.committed_lamport() != Lamport(1) {
                return Err(group
                    .sim
                    .failure("the stale write advanced the acknowledged WAL watermark"));
            }

            group.sim.heal_all();
            let live = group.sim.block_on({
                let promoted = Arc::clone(&promoted);
                async move { promoted.commit(put(b"after")).await }
            });
            if live != Ok(Lamport(2)) {
                return Err(group.sim.failure(format!(
                    "writes did not recover after quorum connectivity returned: {live:?}"
                )));
            }
            group.sim.run_for(Duration::from_secs(1));
            let replica_epoch = group.sim.block_on({
                let log = services[&WORKERS[2]].log(partition).expect("replica log");
                async move { (log.durable_lamport().await, log.epoch().await) }
            });
            if replica_epoch != (Lamport(2), Epoch(2)) {
                return Err(group.sim.failure(format!(
                    "the surviving replica ended at {replica_epoch:?}, not Lamport 2 epoch 2"
                )));
            }
            Ok(())
        },
    );
}
