//! What happens to a replica that falls past its owner's retained log.
//!
//! WAL truncation is live. Hydrating a partition from the segments an owner
//! published is not, and lands in issue #17. Between those two facts sits a
//! cliff: a replica that misses more than the owner's retained log asks for
//! entries that no longer exist anywhere a log can produce them, and there is
//! no path back for it in this release. Issue #16's review accepted that as an
//! explicit merge tradeoff.
//!
//! An accepted tradeoff that nothing exercises is a tradeoff that drifts. This
//! runs the cliff under the deterministic simulator and pins what it looks
//! like from outside: the owner names the failure and fails the
//! `replicas-recoverable` readiness condition, the replica does not serve a
//! read from its own state, and its log stops where it stopped rather than
//! resuming above the hole.
//!
//! It also pins the two ways that answer could quietly become useless. An
//! owner that restarts and writes nothing has to reach the same verdict, or a
//! promotion turns a stranded partition back into a healthy-looking one; and
//! the verdict has to leave the process, or it is the log line this replaced.
//!
//! # Deleting the old expectation is required rather than optional
//!
//! [`PAST_THE_HORIZON`] says which of two outcomes the cluster produces. It is
//! [`PastTheHorizon::FailsLoudly`] today. When issue #17 lands it becomes
//! [`PastTheHorizon::Recovers`], and the arm below it spells out what
//! hydration has to make true. Hydration that ships without moving it fails
//! this test, because a replica that recovers does not satisfy the assertions
//! that say it cannot, so the flip cannot be forgotten and then discovered by
//! an operator.

use crate::map_source::{BoxedMapSource, StaticMapSource};
use crate::node::{DataLayout, Node};
use crate::readiness::ReadinessCondition;

use orbita_core::{
    Epoch, KeyRange, KeyspaceId, KeyspaceInfo, KeyspaceName, Lamport, MapVersion, NodeId,
    PartitionId, PartitionInfo, PartitionMap,
};
use orbita_format::testing::MemoryStore;
use orbita_proto::v1::{GetRequest, SetRequest};
use orbita_runtime::{Clock, Runtime};
use orbita_sim::{harness, DiskPolicy, Failure, SimRuntime, Simulation};

use std::sync::Arc;
use std::time::Duration;

/// What the cluster does with a replica that has fallen past the owner's
/// retained write-ahead log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PastTheHorizon {
    /// Today. The owner records the replica as unrecoverable and says so, the
    /// replica leaves the read set, and nothing invents the entries it is
    /// missing.
    FailsLoudly,
    /// Issue #17. The owner's published segments are turned back into a
    /// caught-up replica, and the cliff becomes a slow path rather than a
    /// dead end.
    ///
    /// Never constructed until then, which is exactly what makes it the thing
    /// hydration has to change. The allow goes when the constant does.
    #[allow(dead_code)]
    Recovers,
}

/// The outcome this build produces. See the module docs: issue #17 moves this.
const PAST_THE_HORIZON: PastTheHorizon = PastTheHorizon::FailsLoudly;

const KEYSPACE: &str = "default";
const PARTITION: PartitionId = PartitionId(1);

const OWNER: NodeId = NodeId(1);
/// The replica that stays reachable, so the partition keeps its quorum and the
/// owner keeps committing while the other one is cut off. Without it the
/// scenario would test an unavailable partition rather than a stranded copy.
const HEALTHY: NodeId = NodeId(2);
/// The replica the scenario strands.
const STRANDED: NodeId = NodeId(3);

/// Small enough that a few dozen writes roll several segments.
///
/// Segment size is the retention granularity: a checkpoint drops whole
/// segments, so this is what decides how far a replica may lag and still be
/// caught up. Shrinking it here is how the scenario reaches in a few writes
/// the state a production cluster reaches after 64 MiB of them.
const WAL_SEGMENT_BYTES: u64 = 4 * 1024;

/// Big enough that a handful of writes fill a segment, small enough that the
/// whole run stays cheap across a batch of seeds.
const VALUE_BYTES: usize = 512;

/// Writes the stranded node replicates before it is cut off.
const BEFORE: u64 = 4;

/// Writes made while it is cut off. Enough to roll several segments, so that a
/// checkpoint leaves the oldest retained entry well above where the stranded
/// node stopped.
const DURING: u64 = 48;

/// Writes made after the link is healed, which is what makes the owner try to
/// catch the stranded node up and discover it cannot.
const AFTER: u64 = 8;

/// How many lease renewals the owner's heartbeat performs before it stops.
///
/// A fixed count rather than a loop, because a timer that never stops would
/// keep the simulated world from going idle and the run from ending.
const RENEWALS: u32 = 400;

fn cluster_map() -> PartitionMap {
    let keyspace = KeyspaceId(1);
    let mut map = PartitionMap::new(MapVersion(1));
    map.insert_keyspace(KeyspaceInfo {
        id: keyspace,
        name: KeyspaceName::new(KEYSPACE).expect("a literal name is valid"),
        default_ttl_millis: None,
        max_value_bytes: None,
        max_storage_bytes: None,
        max_reads_per_second: None,
        max_writes_per_second: None,
    });
    map.insert_partition(PartitionInfo {
        id: PARTITION,
        keyspace,
        range: KeyRange::unbounded(),
        owner: Some(OWNER),
        epoch: Epoch(1),
        replicas: vec![HEALTHY, STRANDED],
    });
    map
}

/// Where one node persists.
///
/// An in-memory store per node, the way each node owns its own bucket prefix
/// or data directory in production. Held by the scenario rather than made
/// inside `start_node`, because a restart has to come back to the same objects.
fn layout() -> DataLayout {
    DataLayout {
        store: Arc::new(MemoryStore::new()),
        wal_root: "wal".to_string(),
        wal_segment_bytes: WAL_SEGMENT_BYTES,
    }
}

/// One worker, with the readiness gate it reports through.
///
/// The gate is what makes this scenario able to check the operator-facing
/// answer rather than only the in-process one: it is the same gate
/// `Health.CheckReadiness` renders and the same one a rolling update stops on.
struct Worker {
    node: Arc<Node<SimRuntime>>,
    gate: Arc<crate::ReadinessGate>,
}

fn start_node(
    sim: &Simulation,
    runtime: SimRuntime,
    node: NodeId,
    lease: Duration,
    layout: DataLayout,
) -> Worker {
    let source = BoxedMapSource::new(StaticMapSource::new(cluster_map()));
    let gate = Arc::new(crate::ReadinessGate::new());
    let started = {
        let gate = Arc::clone(&gate);
        sim.block_on(async move {
            Node::start(runtime, node, layout, source, lease, gate)
                .await
                .expect("the node starts")
        })
    };
    Worker {
        node: started,
        gate,
    }
}

fn key(n: u64) -> String {
    format!("k{n:04}")
}

/// The value written at `n`, padded so that a few dozen writes roll segments.
fn value(n: u64) -> Vec<u8> {
    let mut value = n.to_be_bytes().to_vec();
    value.resize(VALUE_BYTES, b'.');
    value
}

/// Writes `keys` through the owner, returning how many it acknowledged.
fn write_all(
    sim: &Simulation,
    owner: &Arc<Node<SimRuntime>>,
    keys: impl Iterator<Item = u64>,
) -> u64 {
    let mut applied = 0;
    for n in keys {
        let owner = Arc::clone(owner);
        let request = SetRequest {
            keyspace: KEYSPACE.to_string(),
            key: key(n).into_bytes(),
            value: value(n),
            ttl_millis: None,
            condition: None,
        };
        let acknowledged = sim.block_on(async move {
            matches!(owner.set(request, false).await, Ok(response) if response.applied)
        });
        applied += u64::from(acknowledged);
    }
    applied
}

/// Reads a key through `node`, which either answers it or forwards it to the
/// owner. A client cannot tell those apart, which is the point of asking here
/// rather than inside the host.
fn read(sim: &Simulation, node: &Arc<Node<SimRuntime>>, n: u64) -> Option<Vec<u8>> {
    let node = Arc::clone(node);
    let request = GetRequest {
        keyspace: KEYSPACE.to_string(),
        key: key(n).into_bytes(),
    };
    sim.block_on(async move {
        match node.get(request, false).await {
            Ok(response) if response.found => Some(response.value),
            _ => None,
        }
    })
}

/// How far a node says it has durably logged this partition.
///
/// This is the number the control plane's heartbeat carries and compares when
/// it picks a replacement owner, so asking for it is asking what the cluster
/// itself believes rather than reaching into the host.
fn durable(sim: &Simulation, node: &Arc<Node<SimRuntime>>) -> Lamport {
    let node = Arc::clone(node);
    sim.block_on(async move {
        node.progress()
            .await
            .into_iter()
            .find(|progress| progress.partition == PARTITION)
            .expect("every node in this scenario holds the partition")
            .durable_lamport
    })
}

#[test]
fn a_replica_past_the_retention_horizon_is_reported_and_serves_nothing_until_hydration_lands() {
    harness::check_seeds(
        "retention::a_replica_past_the_retention_horizon_is_reported_and_serves_nothing_until_hydration_lands",
        20,
        scenario,
    );
}

#[allow(clippy::too_many_lines)]
fn scenario(seed: u64) -> Result<(), Failure> {
    let sim = Simulation::new(seed);
    // Shorter than the production default so a run covers many heartbeats in a
    // few hundred milliseconds of virtual time.
    let lease = Duration::from_millis(150);
    let owner_layout = layout();
    let mut owning = start_node(
        &sim,
        sim.add_node(OWNER),
        OWNER,
        lease,
        owner_layout.clone(),
    );
    let healthy = start_node(&sim, sim.add_node(HEALTHY), HEALTHY, lease, layout()).node;
    let stranded = start_node(&sim, sim.add_node(STRANDED), STRANDED, lease, layout()).node;
    let clock = sim.runtime(OWNER).clock().clone();

    // The owner's lease heartbeat, which is what puts a replica in the read
    // set at all, and what carries every replica's log position back. In
    // production this is a loop the server owns. It reads the owner out of a
    // cell so that restarting the owner below replaces what it heartbeats
    // rather than keeping the dead incarnation alive.
    let heartbeating: Arc<std::sync::Mutex<Option<Arc<Node<SimRuntime>>>>> =
        Arc::new(std::sync::Mutex::new(Some(Arc::clone(&owning.node))));
    {
        let heartbeating = Arc::clone(&heartbeating);
        let clock = clock.clone();
        sim.spawn(async move {
            for _ in 0..RENEWALS {
                let current = heartbeating
                    .lock()
                    .expect("heartbeat cell poisoned")
                    .clone();
                if let Some(node) = current {
                    node.renew_leases().await;
                }
                clock.sleep(lease / 3).await;
            }
        });
    }
    let owner = Arc::clone(&owning.node);

    // A healthy replica first, so that "it stopped serving" means something.
    if write_all(&sim, &owner, 1..=BEFORE) != BEFORE {
        return Err(sim.failure("the cluster never became writable".to_string()));
    }
    for _ in 0..8 {
        sim.run_for(lease);
        for n in 1..=BEFORE {
            read(&sim, &stranded, n);
        }
        if stranded.replica_reads() > 0 {
            break;
        }
    }
    let served_while_healthy = stranded.replica_reads();
    if served_while_healthy == 0 {
        return Err(sim.failure(
            "the stranded node never served a read while it was caught up, so this seed \
             proved nothing about it stopping"
                .to_string(),
        ));
    }
    if !owning
        .gate
        .state()
        .is_met(ReadinessCondition::ReplicasRecoverable)
    {
        return Err(sim.failure(
            "the owner reported a durability problem before there was one, so this seed cannot \
             tell the report apart from the default"
                .to_string(),
        ));
    }
    let stopped_at = durable(&sim, &stranded);

    // Cut it off and write past the horizon. The other replica keeps the
    // quorum, so the owner keeps committing and keeps checkpointing.
    sim.partition(OWNER, STRANDED);
    if write_all(&sim, &owner, BEFORE + 1..=BEFORE + DURING) != DURING {
        return Err(sim
            .failure("the partition stopped accepting writes with a quorum still up".to_string()));
    }
    // The periodic flush: publish the segments, then checkpoint the log behind
    // them. This is what drops the entries the stranded node still needs.
    {
        let flushing = Arc::clone(&owner);
        sim.block_on(async move { flushing.flush_owned().await });
    }

    sim.heal(OWNER, STRANDED);
    if write_all(&sim, &owner, BEFORE + DURING + 1..=BEFORE + DURING + AFTER) != AFTER {
        return Err(sim.failure("the owner stopped committing after the link healed".to_string()));
    }
    // Long enough for many heartbeats and many catch-up attempts to run, so
    // that an outcome that only holds for an instant is not mistaken for one
    // that holds.
    sim.run_for(lease * 8);

    let newest = BEFORE + DURING + AFTER;
    let fallen = {
        let owner = Arc::clone(&owner);
        sim.block_on(async move { owner.replicas_beyond_retention().await })
    };
    let reported_before_restart = owning
        .gate
        .state()
        .is_met(ReadinessCondition::ReplicasRecoverable);
    let healthy_durable = durable(&sim, &healthy);
    let stranded_durable = durable(&sim, &stranded);
    let stranded_served = stranded.replica_reads();
    // Every key ever acknowledged, asked of the stranded node. It may forward
    // these and it may not answer them, and it may not answer them wrongly.
    let answers: Vec<(u64, Option<Vec<u8>>)> = (1..=newest)
        .map(|n| (n, read(&sim, &stranded, n)))
        .collect();
    let served_by_reading = stranded.replica_reads() - stranded_served;

    let outcome = match PAST_THE_HORIZON {
        PastTheHorizon::FailsLoudly => {
            // The owner names it, rather than leaving it to a log line. A
            // diagnosis is the replica's position and the owner's oldest
            // retained entry side by side: the gap is everything between.
            let [(partition, beyond)] = fallen.as_slice() else {
                return Err(sim.failure(format!(
                    "the owner reported {} replicas beyond its retention horizon, expected \
                     exactly one: {fallen:?}",
                    fallen.len()
                )));
            };
            if *partition != PARTITION || beyond.node != STRANDED {
                return Err(sim.failure(format!(
                    "the owner blamed the wrong copy: {partition} {beyond:?}"
                )));
            }
            if beyond.replica_durable != stopped_at {
                return Err(sim.failure(format!(
                    "the owner reported the stranded node at {}, but it stopped at {stopped_at}",
                    beyond.replica_durable
                )));
            }
            match beyond.retained_from {
                Some(oldest) if oldest > stopped_at.next() => {}
                other => {
                    return Err(sim.failure(format!(
                        "the owner reported its oldest retained entry as {other:?}, which is not \
                         above the {} the stranded node needs next",
                        stopped_at.next()
                    )))
                }
            }

            // It does not rejoin. A log with a hole cannot be replayed, so the
            // stranded node's durable position stays exactly where it stopped
            // rather than resuming above the entries it never received.
            if stranded_durable != stopped_at {
                return Err(sim.failure(format!(
                    "the stranded node moved from {stopped_at} to {stranded_durable} without \
                     receiving the entries in between"
                )));
            }

            // It does not serve. Every read it was asked went to the owner,
            // however many leases were offered in the meantime.
            if served_by_reading != 0 {
                return Err(sim.failure(format!(
                    "the stranded node answered {served_by_reading} reads from its own state \
                     while missing everything above {stopped_at}"
                )));
            }
            Ok(())
        }
        PastTheHorizon::Recovers => {
            // Issue #17. Hydration turns the published segments back into a
            // caught-up replica, so the owner has nothing left to report, the
            // stranded node's log reaches the owner's, and it serves reads
            // again. Delete the arm above when this becomes the answer.
            if !fallen.is_empty() {
                return Err(sim.failure(format!(
                    "hydration is available and the owner still reports {fallen:?}"
                )));
            }
            if stranded_durable < healthy_durable {
                return Err(sim.failure(format!(
                    "the hydrated node reached {stranded_durable}, behind the {healthy_durable} \
                     the replica that never fell off holds"
                )));
            }
            if served_by_reading == 0 {
                return Err(sim.failure(
                    "the hydrated node forwarded every read, so it never rejoined the read set"
                        .to_string(),
                ));
            }
            Ok(())
        }
    };
    outcome?;

    // True under both outcomes, and the one that matters most: whatever the
    // stranded node did with those reads, it never answered one wrongly. A
    // replica that rejoined with a hole would answer some of these with the
    // absence of a key the cluster acknowledged.
    for (n, answer) in answers {
        let Some(answer) = answer else {
            return Err(sim.failure(format!(
                "reading {} through the stranded node lost an acknowledged write",
                key(n)
            )));
        };
        if answer != value(n) {
            return Err(sim.failure(format!(
                "reading {} through the stranded node returned a value no write produced",
                key(n)
            )));
        }
    }

    // The healthy replica is untouched by any of this: one copy falling off
    // must not take the partition's remaining redundancy with it.
    if !fallen.iter().all(|(_, beyond)| beyond.node != HEALTHY) {
        return Err(sim.failure(format!(
            "the replica that never lost the link was reported too: {fallen:?}"
        )));
    }
    if healthy_durable < Lamport(newest) {
        return Err(sim.failure(format!(
            "the replica that kept its link only reached {healthy_durable} of {newest}, so the \
             partition lost more than the one copy"
        )));
    }

    // The verdict has to leave the process, or it is the log line this set out
    // to replace with something an operator can ask for. This is the gate
    // `Health.CheckReadiness` renders and the gate a rolling update stops on.
    let stranded_is_a_readiness_problem =
        PAST_THE_HORIZON == PastTheHorizon::FailsLoudly && !fallen.is_empty();
    if reported_before_restart == stranded_is_a_readiness_problem {
        return Err(sim.failure(format!(
            "the owner's readiness said replicas-recoverable was {reported_before_restart} with \
             {} replicas beyond its horizon",
            fallen.len()
        )));
    }

    // An owner that restarts and writes nothing must reach the same verdict.
    // Detection used to depend on a later append being refused, so a restarted
    // owner of an idle partition reported the healthy answer indefinitely
    // while the replica still could not serve. See the #75 review of #63.
    drop(owner);
    *heartbeating.lock().expect("heartbeat cell poisoned") = None;
    drop(owning.node);
    sim.crash(OWNER);
    owning = start_node(
        &sim,
        sim.restart(OWNER, DiskPolicy::Intact),
        OWNER,
        lease,
        owner_layout,
    );
    *heartbeating.lock().expect("heartbeat cell poisoned") = Some(Arc::clone(&owning.node));
    // Heartbeats only. Nothing is written, which is the whole point.
    sim.run_for(lease * 8);

    let after_restart = {
        let owner = Arc::clone(&owning.node);
        sim.block_on(async move { owner.replicas_beyond_retention().await })
    };
    let ready_after_restart = owning
        .gate
        .state()
        .is_met(ReadinessCondition::ReplicasRecoverable);
    let outcome = match PAST_THE_HORIZON {
        PastTheHorizon::FailsLoudly => {
            if after_restart != fallen {
                return Err(sim.failure(format!(
                    "before the restart the owner reported {fallen:?} and after it, having \
                     written nothing, it reported {after_restart:?}"
                )));
            }
            if ready_after_restart {
                return Err(sim.failure(
                    "a restarted owner called itself recoverable while it reported a replica it \
                     cannot catch up"
                        .to_string(),
                ));
            }
            Ok(())
        }
        PastTheHorizon::Recovers => {
            // Issue #17: hydration has run by now, so a restart finds nothing
            // to report and nothing holding readiness down.
            if !after_restart.is_empty() || !ready_after_restart {
                return Err(sim.failure(format!(
                    "hydration is available and a restarted owner still reports {after_restart:?}"
                )));
            }
            Ok(())
        }
    };
    outcome?;

    drop(owning.node);
    drop(healthy);
    drop(stranded);
    Ok(())
}
