//! What happens to a replica that falls past its owner's retained log.
//!
//! WAL truncation is live, and so, since issue #17, is hydration. Between
//! those two facts used to sit a cliff: a replica that missed more than the
//! owner's retained log asked for entries no log could produce, and there was
//! no path back for it. Issue #16's review accepted that as an explicit merge
//! tradeoff, and this scenario existed to pin what the dead end looked like
//! from outside.
//!
//! It now pins the recovery instead. The cliff is still reached — the owner
//! checkpoints away the entries the stranded node needs, and this asserts that
//! before healing the link, because a scenario that cannot tell "recovered
//! from the cliff" from "never fell off it" proves nothing. What changed is
//! what happens next: the stranded node rebuilds the partition from the
//! segments the owner published, the first append it then acknowledges moves
//! it back to following, the owner has nothing left to report, and the
//! `replicas-recoverable` readiness condition clears on its own.
//!
//! It also still pins the two ways that answer could quietly become useless.
//! An owner that restarts and writes nothing has to reach the same verdict, or
//! a promotion turns the partition's health into an accident of timing; and
//! the verdict has to leave the process, or it is the log line this replaced.
//!
//! # The bucket is the cluster's, not the node's
//!
//! Every worker is configured with one object store and `PartitionPath`
//! carries no node component, so a partition's segments and manifest live at
//! one place every node can read. That is the premise of ADR 0006 and the
//! reason replacing a worker is a download. This scenario models it that way;
//! a store per node would make hydration impossible by construction and would
//! leave this test passing for a reason that has nothing to do with what it
//! claims.
//!
//! # The old expectation is kept as a contrast, not as the answer
//!
//! [`PAST_THE_HORIZON`] says which of two outcomes the cluster produces, and
//! issue #17 moved it from [`PastTheHorizon::FailsLoudly`] to
//! [`PastTheHorizon::Recovers`]. The losing arm stays because it is still what
//! the cluster does whenever hydration has nothing to offer — a partition that
//! has never been flushed, or a manifest that is itself behind the gap — and
//! because a flip with the other side deleted is a flip nobody can read.

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
    /// Before issue #17. The owner records the replica as unrecoverable and
    /// says so, the replica leaves the read set, and nothing invents the
    /// entries it is missing.
    ///
    /// Kept as the contrast rather than deleted: it is the outcome this
    /// scenario pinned before hydration existed, and it is what the cluster
    /// still does whenever hydration has nothing to offer — a partition that
    /// has never been flushed, or a manifest that is itself behind the gap.
    #[allow(dead_code)]
    FailsLoudly,
    /// Today, since issue #17. The owner's published segments are turned back
    /// into a caught-up replica, and the cliff is a slow path rather than a
    /// dead end.
    Recovers,
}

/// The outcome this build produces. Moved by issue #17; see the module docs.
const PAST_THE_HORIZON: PastTheHorizon = PastTheHorizon::Recovers;

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
/// The object store is the cluster's, not the node's. Every worker is
/// configured with one bucket and `PartitionPath` carries no node component,
/// so a partition's segments and manifest live at one place that every node
/// can read. That is the whole premise of ADR 0006 and of hydration: replacing
/// a worker is a download because the objects are already somewhere the
/// replacement can reach.
///
/// The write-ahead log is the node's own, and stays so — each node has its own
/// simulated disk under `wal_root`.
fn layout(store: &Arc<MemoryStore>) -> DataLayout {
    DataLayout {
        store: Arc::clone(store) as Arc<dyn orbita_objectstore::ObjectStore>,
        wal_root: "wal".to_string(),
        wal_segment_bytes: WAL_SEGMENT_BYTES,
        durability_acks: 1,
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
            // Authentication off: retention is about expiry and compaction, not
            // the credential gate, so admission passes every request through.
            let authenticator = Arc::new(crate::auth::Authenticator::new(
                false,
                None,
                std::time::Duration::from_secs(86_400),
                runtime.clock().clone(),
            ));
            Node::start(
                runtime,
                node,
                layout,
                source,
                None,
                None,
                lease,
                crate::DEFAULT_CONTROL_POLL_INTERVAL,
                gate,
                authenticator,
            )
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
            matches!(owner.set(request, false, None).await, Ok(response) if response.applied)
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
        match node.get(request, false, None).await {
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
    let bucket = Arc::new(MemoryStore::new());
    let owner_layout = layout(&bucket);
    let mut owning = start_node(
        &sim,
        sim.add_node(OWNER),
        OWNER,
        lease,
        owner_layout.clone(),
    );
    let healthy = start_node(&sim, sim.add_node(HEALTHY), HEALTHY, lease, layout(&bucket)).node;
    let stranded = start_node(
        &sim,
        sim.add_node(STRANDED),
        STRANDED,
        lease,
        layout(&bucket),
    )
    .node;
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

    // The cliff, established rather than assumed. Under `Recovers` the
    // stranded node ends up caught up either way, so without this the scenario
    // could not tell "hydration rescued a replica that had fallen past the
    // log" from "the checkpoint never dropped what it needed and an ordinary
    // catch-up would have carried it". That distinction is the entire subject.
    let retained_from = {
        let owner = Arc::clone(&owner);
        sim.block_on(async move { owner.retained_from(PARTITION).await })
    };
    match retained_from {
        Some(oldest) if oldest > stopped_at.next() => {}
        other => {
            return Err(sim.failure(format!(
                "the owner's oldest retained entry is {other:?}, which is not above the {} the \
                 stranded node needs next, so it never fell past the horizon and this seed \
                 proves nothing about recovering from it",
                stopped_at.next()
            )))
        }
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
