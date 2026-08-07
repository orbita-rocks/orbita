//! A Raft leader group under deterministic simulation.
//!
//! These scenarios are the reason `RaftLog` routes every piece of its IO
//! through `orbita_runtime`: elections and replication run in virtual time on
//! the simulated network, so a failing seed replays exactly. The assertions
//! follow the same rule as `tests/cluster.rs`: read the log, not just the
//! outcome, because "every node holds the same entries at the same indices"
//! is the claim the trait actually makes.

use orbita_control::{
    BootstrapSpec, ConsensusLog, ControlCommand, ControlConfig, Controller, KeyspaceConfig,
    MembershipChange, NodeRole, NodeStatus, RaftLog,
};
use orbita_core::{Error, MapVersion, NodeId};
use orbita_sim::{check_seeds, Failure, Simulation};

use std::sync::Arc;
use std::time::Duration;

const NODES: [NodeId; 3] = [NodeId(1), NodeId(2), NodeId(3)];

/// Long enough for an election under the default one-to-two-second timeout,
/// with room for a retry if the first round splits.
const ELECTION_GRACE: Duration = Duration::from_secs(10);

/// Long enough for a proposal to replicate and for every follower to apply.
const REPLICATION_GRACE: Duration = Duration::from_secs(2);

fn register(id: u64) -> ControlCommand {
    ControlCommand::RegisterNode {
        node: NodeId(id),
        role: NodeRole::Worker,
        address: format!("10.0.0.{id}:7000"),
        speaks: orbita_control::binary_speaks(),
        ready: true,
        draining: false,
    }
}

/// Starts a three-node group and returns each node's log handle.
fn start(sim: &Simulation) -> Vec<Arc<RaftLog>> {
    for node in NODES {
        sim.add_node(node);
    }
    NODES
        .iter()
        .map(|node| {
            let runtime = sim.runtime(*node);
            sim.block_on(async move {
                RaftLog::open(&runtime, &NODES)
                    .await
                    .expect("open raft log")
            })
        })
        .collect()
}

/// The nodes that currently believe they are the leader.
fn leaders(sim: &Simulation, logs: &[Arc<RaftLog>]) -> Vec<NodeId> {
    NODES
        .iter()
        .zip(logs)
        .filter_map(|(node, log)| {
            let log = Arc::clone(log);
            sim.block_on(async move { log.is_leader().await })
                .then_some(*node)
        })
        .collect()
}

/// Elects a leader and returns it, or explains why there is not exactly one.
fn elect(sim: &Simulation, logs: &[Arc<RaftLog>]) -> Result<NodeId, Failure> {
    sim.run_for(ELECTION_GRACE);
    let elected = leaders(sim, logs);
    let [leader] = elected[..] else {
        return Err(sim.failure(format!("expected exactly one leader, found {elected:?}")));
    };
    for (node, log) in NODES.iter().zip(logs) {
        let log = Arc::clone(log);
        let seen = sim.block_on(async move { log.leader().await });
        if seen != Some(leader) {
            return Err(sim.failure(format!(
                "node {node} believes the leader is {seen:?}, not {leader}"
            )));
        }
    }
    Ok(leader)
}

#[test]
fn three_nodes_elect_exactly_one_leader() {
    check_seeds("three_nodes_elect_exactly_one_leader", 10, |seed| {
        let sim = Simulation::new(seed);
        let logs = start(&sim);
        elect(&sim, &logs).map(|_| ())
    });
}

#[test]
fn a_command_proposed_on_the_leader_reaches_every_node() {
    check_seeds(
        "a_command_proposed_on_the_leader_reaches_every_node",
        10,
        |seed| {
            let sim = Simulation::new(seed);
            let logs = start(&sim);
            let leader = elect(&sim, &logs)?;
            let leader_log = Arc::clone(
                &logs[NODES
                    .iter()
                    .position(|n| *n == leader)
                    .expect("leader is a member")],
            );

            for id in 1..=3 {
                let log = Arc::clone(&leader_log);
                let index = sim
                    .block_on(async move { log.propose(register(id)).await })
                    .map_err(|e| sim.failure(format!("proposal {id} failed: {e}")))?;
                if index != id {
                    return Err(sim.failure(format!("proposal {id} committed at index {index}")));
                }
            }

            sim.run_for(REPLICATION_GRACE);
            for (node, log) in NODES.iter().zip(&logs) {
                let log = Arc::clone(log);
                let (commit, entries) =
                    sim.block_on(async move { (log.commit_index().await, log.subscribe(0).await) });
                let entries =
                    entries.map_err(|e| sim.failure(format!("subscribe on {node}: {e}")))?;
                if commit != 3 {
                    return Err(sim.failure(format!("node {node} commit index is {commit}, not 3")));
                }
                for (offset, entry) in entries.iter().enumerate() {
                    let id = offset as u64 + 1;
                    if entry.index != id || entry.command != register(id) {
                        return Err(sim.failure(format!(
                            "node {node} disagrees about entry {id}: {entry:?}"
                        )));
                    }
                }
            }
            Ok(())
        },
    );
}

#[test]
fn a_follower_redirects_proposals_to_the_leader() {
    check_seeds("a_follower_redirects_proposals_to_the_leader", 10, |seed| {
        let sim = Simulation::new(seed);
        let logs = start(&sim);
        let leader = elect(&sim, &logs)?;
        let follower = NODES
            .iter()
            .position(|n| *n != leader)
            .expect("two followers exist");

        let log = Arc::clone(&logs[follower]);
        let refused = sim.block_on(async move { log.propose(register(1)).await });
        match refused {
            Err(Error::NotLeader { leader: hint }) if hint == Some(leader) => Ok(()),
            other => Err(sim.failure(format!(
                "a follower accepted a proposal or misdirected it: {other:?}"
            ))),
        }
    });
}

#[test]
fn losing_the_leader_elects_another_and_replication_continues() {
    check_seeds(
        "losing_the_leader_elects_another_and_replication_continues",
        10,
        |seed| {
            let sim = Simulation::new(seed);
            let logs = start(&sim);
            let first = elect(&sim, &logs)?;

            let log = Arc::clone(
                &logs[NODES
                    .iter()
                    .position(|n| *n == first)
                    .expect("leader is a member")],
            );
            sim.block_on(async move { log.propose(register(1)).await })
                .map_err(|e| sim.failure(format!("first proposal failed: {e}")))?;
            sim.run_for(REPLICATION_GRACE);

            sim.crash(first);
            sim.run_for(ELECTION_GRACE);

            let survivors: Vec<_> = NODES
                .iter()
                .zip(&logs)
                .filter(|(node, _)| **node != first)
                .collect();
            let elected: Vec<NodeId> = survivors
                .iter()
                .filter_map(|(node, log)| {
                    let log = Arc::clone(log);
                    sim.block_on(async move { log.is_leader().await })
                        .then_some(**node)
                })
                .collect();
            let [second] = elected[..] else {
                return Err(sim.failure(format!(
                    "expected one leader among the survivors, found {elected:?}"
                )));
            };

            let log = Arc::clone(
                survivors
                    .iter()
                    .find(|(node, _)| **node == second)
                    .expect("second leader is a survivor")
                    .1,
            );
            let index = sim
                .block_on(async move { log.propose(register(2)).await })
                .map_err(|e| sim.failure(format!("post-failover proposal failed: {e}")))?;
            if index != 2 {
                return Err(sim.failure(format!(
                    "the entry committed before the crash was lost: register(2) landed at {index}"
                )));
            }

            sim.run_for(REPLICATION_GRACE);
            for (node, log) in survivors {
                let log = Arc::clone(log);
                let entries = sim
                    .block_on(async move { log.subscribe(0).await })
                    .map_err(|e| sim.failure(format!("subscribe on {node}: {e}")))?;
                let commands: Vec<_> = entries.iter().map(|e| e.command.clone()).collect();
                if commands != vec![register(1), register(2)] {
                    return Err(sim.failure(format!("node {node} holds {commands:?}")));
                }
            }
            Ok(())
        },
    );
}

/// The claim the whole design rests on: `raft-rs` draws its election jitter
/// from a thread rng, and the driver's overwrite of it is what keeps a run
/// replayable. If this fails, that overwrite has a gap.
#[test]
fn an_election_replays_identically_from_its_seed() {
    let run = |seed| {
        let sim = Simulation::new(seed);
        let logs = start(&sim);
        sim.run_for(ELECTION_GRACE);
        (leaders(&sim, &logs), format!("{}", sim.trace()))
    };
    let (first_leaders, first_trace) = run(7);
    let (second_leaders, second_trace) = run(7);
    assert_eq!(first_leaders, second_leaders);
    assert_eq!(
        first_trace, second_trace,
        "two runs of one seed must produce byte-identical traces"
    );
}

#[test]
fn a_one_node_group_elects_itself_and_survives_a_restart() {
    check_seeds(
        "a_one_node_group_elects_itself_and_survives_a_restart",
        10,
        |seed| {
            let sim = Simulation::new(seed);
            let node = NodeId(1);
            let runtime = sim.add_node(node);
            let voters = [node];

            let opening = runtime.clone();
            let log = sim.block_on(async move {
                RaftLog::open(&opening, &voters)
                    .await
                    .expect("open raft log")
            });
            sim.run_for(ELECTION_GRACE);

            let handle = Arc::clone(&log);
            if !sim.block_on(async move { handle.is_leader().await }) {
                return Err(sim.failure("a one-node group did not elect itself"));
            }
            for id in 1..=3 {
                let handle = Arc::clone(&log);
                sim.block_on(async move { handle.propose(register(id)).await })
                    .map_err(|e| sim.failure(format!("proposal {id} failed: {e}")))?;
            }

            // Stop the first incarnation before starting the second, so exactly
            // one driver speaks for the node.
            log.shutdown();
            sim.run_for(Duration::from_secs(1));

            let reopening = sim.runtime(node);
            let reopened = sim.block_on(async move {
                RaftLog::open(&reopening, &voters)
                    .await
                    .expect("reopen raft log")
            });
            sim.run_for(ELECTION_GRACE);

            let handle = Arc::clone(&reopened);
            let (commit, entries) = sim
                .block_on(async move { (handle.commit_index().await, handle.subscribe(0).await) });
            let entries =
                entries.map_err(|e| sim.failure(format!("subscribe after restart: {e}")))?;
            if commit != 3 {
                return Err(sim.failure(format!(
                    "restart lost committed entries: commit index is {commit}, not 3"
                )));
            }
            let commands: Vec<_> = entries.iter().map(|e| e.command.clone()).collect();
            if commands != vec![register(1), register(2), register(3)] {
                return Err(sim.failure(format!("restart replayed {commands:?}")));
            }
            Ok(())
        },
    );
}

#[test]
fn target_five_expands_three_to_five_one_safe_transition_at_a_time() {
    check_seeds(
        "target_five_expands_three_to_five_one_safe_transition_at_a_time",
        10,
        |seed| {
            let sim = Simulation::new(seed);
            let all = [NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5)];
            for node in all {
                sim.add_node(node);
            }
            let logs: Vec<_> = all
                .iter()
                .map(|node| {
                    let runtime = sim.runtime(*node);
                    sim.block_on(async move {
                        RaftLog::open(&runtime, &NODES)
                            .await
                            .expect("open voter or dormant learner")
                    })
                })
                .collect();
            sim.run_for(ELECTION_GRACE);
            let elected: Vec<_> = NODES
                .iter()
                .zip(&logs[..3])
                .filter_map(|(node, log)| {
                    let log = Arc::clone(log);
                    sim.block_on(async move { log.is_leader().await })
                        .then_some(*node)
                })
                .collect();
            let [leader] = elected[..] else {
                return Err(sim.failure(format!("expected one leader, got {elected:?}")));
            };
            for (node, log) in all[3..].iter().zip(&logs[3..]) {
                let log = Arc::clone(log);
                if sim.block_on(async move { log.is_leader().await }) {
                    return Err(sim.failure(format!(
                        "dormant learner {node} campaigned before admission"
                    )));
                }
            }
            let leader_log =
                Arc::clone(&logs[all.iter().position(|node| *node == leader).expect("member")]);

            for joining in [NodeId(4), NodeId(5)] {
                let log = Arc::clone(&leader_log);
                sim.block_on(async move {
                    log.change_membership(MembershipChange::AddLearner(joining))
                        .await
                })
                .map_err(|error| sim.failure(format!("add learner {joining}: {error}")))?;
                sim.run_for(REPLICATION_GRACE);
                let log = Arc::clone(&leader_log);
                if !sim.block_on(async move { log.learner_caught_up(joining).await }) {
                    return Err(sim.failure(format!("learner {joining} never caught up")));
                }
                let log = Arc::clone(&leader_log);
                sim.block_on(async move {
                    log.change_membership(MembershipChange::Promote(joining))
                        .await
                })
                .map_err(|error| sim.failure(format!("promote {joining}: {error}")))?;
                let log = Arc::clone(&leader_log);
                let voters = sim.block_on(async move { log.voters().await });
                let expected = if joining == NodeId(4) { 4 } else { 5 };
                if voters.len() != expected || !voters.contains(&joining) {
                    return Err(
                        sim.failure(format!("promotion of {joining} produced voters {voters:?}"))
                    );
                }
            }
            Ok(())
        },
    );
}

#[test]
fn a_permanently_lost_voter_is_replaced_add_first_with_an_eligible_node() {
    check_seeds(
        "a_permanently_lost_voter_is_replaced_add_first_with_an_eligible_node",
        10,
        |seed| {
            let sim = Simulation::new(seed);
            let all = [NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5)];
            for node in NODES {
                sim.add_node(node);
            }
            let mut logs: Vec<_> = NODES
                .iter()
                .map(|node| {
                    let runtime = sim.runtime(*node);
                    sim.block_on(async move { RaftLog::open(&runtime, &NODES).await.unwrap() })
                })
                .collect();
            sim.run_for(ELECTION_GRACE);
            let leader = NODES
                .iter()
                .copied()
                .find(|node| {
                    let log = Arc::clone(&logs[all.iter().position(|item| item == node).unwrap()]);
                    sim.block_on(async move { log.is_leader().await })
                })
                .ok_or_else(|| sim.failure("the initial voter set elected no leader"))?;
            let leader_index = all.iter().position(|node| *node == leader).unwrap();
            let config = ControlConfig {
                suspect_after: Duration::from_secs(60),
                dead_after: Duration::from_secs(120),
                voter_management_enabled: true,
                ..ControlConfig::default()
            };
            let controller = Controller::new(
                sim.runtime(leader),
                Arc::clone(&logs[leader_index]),
                config.clone(),
            );
            let bootstrapping = controller.clone();
            sim.block_on(async move {
                bootstrapping
                    .bootstrap(&BootstrapSpec {
                        keyspace: "default".into(),
                        config: KeyspaceConfig::default(),
                        leaders: Vec::new(),
                        workers: NODES
                            .iter()
                            .map(|node| (*node, format!("10.0.0.{node}:7000")))
                            .collect(),
                    })
                    .await
            })
            .map_err(|error| sim.failure(format!("bootstrap failed: {error}")))?;

            for node in all {
                let reporting = controller.clone();
                sim.block_on(async move {
                    reporting
                        .record_status(
                            node,
                            NodeStatus {
                                role: NodeRole::Worker,
                                address: format!("10.0.0.{node}:7000"),
                                map_version: MapVersion::default(),
                                speaks: orbita_control::binary_speaks(),
                                ready: true,
                                draining: false,
                                voter_eligible: node != NodeId(4),
                                failure_domain: format!("zone-{node}"),
                                node_identity: format!("identity-{node}"),
                                partitions: Vec::new(),
                            },
                        )
                        .await
                })
                .map_err(|error| sim.failure(format!("status for {node} failed: {error}")))?;
            }
            let observing = controller.clone();
            sim.block_on(async move { observing.tick().await })
                .map_err(|error| sim.failure(format!("initial voter sweep failed: {error}")))?;

            let lost = NODES.iter().copied().find(|node| *node != leader).unwrap();
            sim.crash(lost);
            sim.run_for(config.voter_replacement_after);

            for node in [NodeId(4), NodeId(5)] {
                let runtime = sim.add_node(node);
                logs.push(
                    sim.block_on(async move { RaftLog::open(&runtime, &NODES).await.unwrap() }),
                );
            }

            // Refresh every surviving observation so only the crashed voter has
            // been continuously dead for the replacement window.
            for node in all.into_iter().filter(|node| *node != lost) {
                let reporting = controller.clone();
                sim.block_on(async move {
                    reporting
                        .record_status(
                            node,
                            NodeStatus {
                                role: NodeRole::Worker,
                                address: format!("10.0.0.{node}:7000"),
                                map_version: MapVersion::default(),
                                speaks: orbita_control::binary_speaks(),
                                ready: true,
                                draining: false,
                                voter_eligible: node != NodeId(4),
                                failure_domain: format!("zone-{node}"),
                                node_identity: format!("identity-{node}"),
                                partitions: Vec::new(),
                            },
                        )
                        .await
                })
                .map_err(|error| sim.failure(format!("refresh for {node} failed: {error}")))?;
            }

            let repairing = controller.clone();
            sim.block_on(async move { repairing.tick().await })
                .map_err(|error| sim.failure(format!("adding learner failed: {error}")))?;
            let log = Arc::clone(&logs[leader_index]);
            let after_add = sim.block_on(async move { (log.voters().await, log.learners().await) });
            if after_add.0.len() != 3 || after_add.1 != vec![NodeId(5)] {
                return Err(sim.failure(format!(
                    "replacement did not add only eligible node 5 first: {after_add:?}"
                )));
            }

            sim.run_for(ELECTION_GRACE);
            for node in all.into_iter().filter(|node| *node != lost) {
                let reporting = controller.clone();
                sim.block_on(async move {
                    reporting
                        .record_status(
                            node,
                            NodeStatus {
                                role: NodeRole::Worker,
                                address: format!("10.0.0.{node}:7000"),
                                map_version: MapVersion::default(),
                                speaks: orbita_control::binary_speaks(),
                                ready: true,
                                draining: false,
                                voter_eligible: node != NodeId(4),
                                failure_domain: format!("zone-{node}"),
                                node_identity: format!("identity-{node}"),
                                partitions: Vec::new(),
                            },
                        )
                        .await
                })
                .map_err(|error| {
                    sim.failure(format!("catch-up heartbeat for {node} failed: {error}"))
                })?;
            }
            sim.run_for(ELECTION_GRACE);
            let mut transitions = Vec::new();
            for stage in ["promotion", "removal"] {
                let repairing = controller.clone();
                sim.block_on(async move { repairing.tick().await })
                    .map_err(|error| sim.failure(format!("{stage} failed: {error}")))?;
                let log = Arc::clone(&logs[leader_index]);
                transitions
                    .push(sim.block_on(async move { (log.voters().await, log.learners().await) }));
            }
            let log = Arc::clone(&logs[leader_index]);
            let voters = sim.block_on(async move { log.voters().await });
            if voters.len() != 3 || voters.contains(&lost) || !voters.contains(&NodeId(5)) {
                return Err(sim.failure(format!(
                    "replacement did not finish at three voters: {voters:?}; transitions: {transitions:?}"
                )));
            }
            Ok(())
        },
    );
}
