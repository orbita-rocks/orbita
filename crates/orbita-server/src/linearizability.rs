//! The read and write paths, checked rather than argued about.
//!
//! ADR 0001 acknowledges a write before applying it to storage, and ADR 0003
//! evaluates conditions against an overlay of writes that are in that window.
//! Both are cheap to describe and easy to get subtly wrong, and neither has
//! natural coverage from a test that does one thing at a time. So these run a
//! partition under the deterministic simulator, record what every client saw,
//! and hand the history to the linearizability checker.
//!
//! One owner is covered: the overlay, the ordering of Lamports against the
//! order writes are applied in, and the acknowledge-before-apply window.
//!
//! What a losing conditional write is *told* is covered here for the same
//! reason. A key with a write still in flight has no version yet, so the
//! answer a loser gets depends on where in that window it landed, and the
//! window only exists when two writers are genuinely concurrent. A single
//! threaded test cannot reach it and an end-to-end one reaches it by luck.
//!
//! The read path is covered too, with a replica actually serving reads. That
//! is the case ADR 0001 exists for and the one it warns is the kind that fails
//! silently, so it is checked here rather than unit tested and trusted: writes
//! go to the owner, reads go to a node that only replicates the partition, and
//! the checker is handed everything both clients saw.

use crate::host::{HostSpec, LeasePolicy, PartitionHost, PartitionPaths, WriteOp};
use crate::map_source::{BoxedMapSource, StaticMapSource};
use crate::node::{DataLayout, Node};

use bytes::Bytes;
use orbita_core::{
    Epoch, KeyRange, KeyspaceId, KeyspaceInfo, KeyspaceName, MapVersion, NodeId, PartitionId,
    PartitionInfo, PartitionMap, Record, Version, WriteCondition,
};
use orbita_format::testing::MemoryStore;
use orbita_format::PartitionPath;
use orbita_proto::v1::{GetRequest, SetRequest};
use orbita_runtime::{Clock, Runtime};
use orbita_sim::lin::{check, Recorder, Register, RegisterOp, RegisterRet};
use orbita_sim::{harness, DiskFaults, SimConfig, SimRuntime, Simulation};

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// How many states the checker may explore before giving up. Concurrency
/// rather than length is what makes the search expensive, so this is generous
/// for the handful of clients these scenarios run.
const SEARCH_BUDGET: u64 = 2_000_000;

const KEY: &[u8] = b"register";

/// Where a simulated partition persists.
///
/// An in-memory store per node, so a run touches no real filesystem and stays
/// deterministic. These scenarios are about the read and write paths rather
/// than about durability, so a store that never fails is the right one: a
/// flush failing here would be noise. The store failing on purpose is
/// [`crate::durability`], which drives the real `S3Store` over the
/// simulator's fault-injecting transport.
fn partition_paths() -> PartitionPaths {
    PartitionPaths {
        store: Arc::new(MemoryStore::new()),
        path: PartitionPath::new("", KeyspaceId(1), PartitionId(1)),
        wal_dir: "wal/p1".to_string(),
        wal_segment_bytes: crate::DEFAULT_WAL_SEGMENT_BYTES,
        durability_acks: 1,
    }
}

fn open(sim: &Simulation, runtime: SimRuntime) -> Arc<PartitionHost<SimRuntime>> {
    open_with_replicas(sim, runtime, Vec::new())
}

/// Opens the owner with peers it must reach before a write resolves.
///
/// Separate from [`open`] because a replica set changes what "in flight"
/// means: with no replicas a write is durable the moment the local disk
/// answers, and the window a conditional write can be caught inside is a few
/// microseconds wide. Replication is what makes that window wide enough to
/// hold open on purpose.
fn open_with_replicas(
    sim: &Simulation,
    runtime: SimRuntime,
    replicas: Vec<NodeId>,
) -> Arc<PartitionHost<SimRuntime>> {
    let paths = partition_paths();
    sim.block_on(async move {
        PartitionHost::open_owner(
            runtime,
            HostSpec {
                id: PartitionId(1),
                epoch: Epoch(1),
                range: KeyRange::unbounded(),
                lease: LeasePolicy::default(),
            },
            &paths,
            replicas,
        )
        .await
        .expect("the partition opens")
    })
}

fn encode(value: u64) -> Bytes {
    Bytes::copy_from_slice(&value.to_be_bytes())
}

fn decode(record: &Record) -> u64 {
    u64::from_be_bytes(
        record
            .value
            .as_ref()
            .try_into()
            .expect("an eight byte value"),
    )
}

#[test]
fn a_read_never_misses_an_acknowledged_write() {
    harness::check_seeds(
        "linearizability::a_read_never_misses_an_acknowledged_write",
        20,
        |seed| {
            let sim = Simulation::new(seed);
            let runtime = sim.add_node(NodeId(1));
            let host = open(&sim, runtime.clone());
            let recorder: Recorder<RegisterOp, RegisterRet> = Recorder::new();

            for client in 0..4u64 {
                let host = Arc::clone(&host);
                let recorder = recorder.clone();
                let clock = runtime.clock().clone();
                sim.spawn(async move {
                    for step in 0..4u64 {
                        // Every client writes a value only it produces, so a
                        // read that returns one names the write it came from.
                        let value = client * 100 + step + 1;
                        if step % 2 == 0 {
                            let call = recorder.invoke(
                                client,
                                RegisterOp::Write(value),
                                clock.monotonic_nanos(),
                            );
                            let written = host
                                .write(
                                    Bytes::from_static(KEY),
                                    WriteOp::Put {
                                        value: encode(value),
                                        ttl_millis: None,
                                    },
                                    WriteCondition::None,
                                )
                                .await;
                            match written {
                                Ok(ack) if ack.applied => {
                                    recorder.complete(call, RegisterRet::Acked);
                                }
                                // A write that failed may or may not have
                                // happened, which is exactly what the checker
                                // treats an unknown outcome as.
                                _ => recorder.abandon(call),
                            }
                        } else {
                            let call =
                                recorder.invoke(client, RegisterOp::Read, clock.monotonic_nanos());
                            match host.get(KEY).await {
                                Ok(found) => recorder
                                    .complete(call, RegisterRet::Value(found.as_ref().map(decode))),
                                Err(_) => recorder.abandon(call),
                            }
                        }
                    }
                });
            }

            sim.run_until_idle();
            let history = recorder.history();
            drop(host);

            if history.len() < 16 {
                return Err(sim.failure(format!(
                    "only {} of 16 operations were recorded",
                    history.len()
                )));
            }
            check(&Register, &history, SEARCH_BUDGET)
                .map(|_| ())
                .map_err(|violation| sim.failure(violation.to_string()))
        },
    );
}

#[test]
fn at_most_one_compare_and_swap_against_a_version_ever_succeeds() {
    harness::check_seeds(
        "linearizability::at_most_one_compare_and_swap_against_a_version_ever_succeeds",
        20,
        |seed| {
            let sim = Simulation::new(seed);
            let runtime = sim.add_node(NodeId(1));
            let host = open(&sim, runtime.clone());

            let initial = {
                let host = Arc::clone(&host);
                sim.block_on(async move {
                    host.write(
                        Bytes::from_static(KEY),
                        WriteOp::Put {
                            value: encode(0),
                            ttl_millis: None,
                        },
                        WriteCondition::None,
                    )
                    .await
                    .expect("the first write commits")
                    .version
                    .expect("an applied write has a version")
                })
            };

            let winners = Arc::new(AtomicU64::new(0));
            for contender in 1..=6u64 {
                let host = Arc::clone(&host);
                let winners = Arc::clone(&winners);
                sim.spawn(async move {
                    let outcome = host
                        .write(
                            Bytes::from_static(KEY),
                            WriteOp::Put {
                                value: encode(contender),
                                ttl_millis: None,
                            },
                            WriteCondition::IfVersion(initial),
                        )
                        .await;
                    if outcome.is_ok_and(|ack| ack.applied) {
                        winners.fetch_add(1, Ordering::SeqCst);
                    }
                });
            }

            sim.run_until_idle();
            let won = winners.load(Ordering::SeqCst);

            let final_value = {
                let host = Arc::clone(&host);
                sim.block_on(async move { host.get(KEY).await.expect("the read succeeds") })
            };
            let version = final_value.as_ref().map(|r| r.version);
            drop(host);

            if won != 1 {
                return Err(sim.failure(format!(
                    "{won} of six compare-and-swaps against version {initial} succeeded"
                )));
            }
            // The one that won is the one whose value is there, so a swap that
            // was told it lost cannot have written anything.
            if version.is_some_and(|v: Version| v <= initial) {
                return Err(sim.failure(format!(
                    "the winning swap left version {version:?}, which did not move past {initial}"
                )));
            }
            Ok(())
        },
    );
}

#[test]
fn a_contended_if_not_present_loser_is_told_the_version_that_beat_it() {
    harness::check_seeds(
        "linearizability::a_contended_if_not_present_loser_is_told_the_version_that_beat_it",
        20,
        |seed| {
            let sim = Simulation::new(seed);
            let runtime = sim.add_node(NodeId(1));
            let host = open(&sim, runtime.clone());

            // Every contender races the same acquisition, and they start
            // together, so the losers land inside the winner's in-flight
            // window rather than after it. That window is the whole point:
            // a loser that arrives after the winner has been acknowledged is
            // the easy case, and it already worked.
            let acks = Arc::new(std::sync::Mutex::new(Vec::new()));
            for contender in 1..=4u64 {
                let host = Arc::clone(&host);
                let acks = Arc::clone(&acks);
                sim.spawn(async move {
                    let ack = host
                        .write(
                            Bytes::from_static(KEY),
                            WriteOp::Put {
                                value: encode(contender),
                                ttl_millis: None,
                            },
                            WriteCondition::IfNotPresent,
                        )
                        .await;
                    if let Ok(ack) = ack {
                        acks.lock().expect("results poisoned").push(ack);
                    }
                });
            }

            sim.run_until_idle();
            let acks = acks.lock().expect("results poisoned").clone();
            drop(host);

            if acks.len() != 4 {
                return Err(sim.failure(format!(
                    "only {} of four acquisitions were answered",
                    acks.len()
                )));
            }
            let winners: Vec<_> = acks.iter().filter(|ack| ack.applied).collect();
            if winners.len() != 1 {
                return Err(sim.failure(format!(
                    "{} of four if_not_present acquisitions won the lock",
                    winners.len()
                )));
            }
            let held = winners[0]
                .version
                .expect("an applied write reports its version");

            for loser in acks.iter().filter(|ack| !ack.applied) {
                if loser.current_version != Some(held) {
                    return Err(sim.failure(format!(
                        "a loser was told current_version {:?}, not the winning version {held:?}; \
                         it cannot name the holder without a second read",
                        loser.current_version
                    )));
                }
            }
            Ok(())
        },
    );
}

/// How long the contenders in the scenario below may take, in virtual time.
///
/// Well under the five seconds a conditional write may spend waiting for an
/// in-flight write to the same key, and far above the microseconds a simulated
/// append takes. This is the whole assertion of that scenario, because the
/// difference between being released by the failure and being released by the
/// timeout is invisible in the values: both end up answering, and only one of
/// them answers this decade.
const PROMPT: Duration = Duration::from_secs(1);

#[test]
fn a_contender_waiting_on_an_acquisition_that_fails_is_released_by_the_failure() {
    harness::check_seeds(
        "linearizability::a_contender_waiting_on_an_acquisition_that_fails_is_released_by_the_failure",
        24,
        |seed| {
            let mut config = SimConfig::new(seed);
            // A disk that refuses every append, so the leading acquisition is
            // always the one that never lands. A contender parked on it is
            // waiting for something that will not happen, and the only thing
            // that can free it is the failure itself.
            config.disk = DiskFaults {
                write_failure_permille: 1000,
                ..DiskFaults::none()
            };
            // Opening the partition is setup rather than the thing under test,
            // and a run whose disk refused to let it open would explore
            // nothing.
            config.fault_warmup = Duration::from_millis(50);
            let sim = Simulation::with_config(config);
            let runtime = sim.add_node(NodeId(1));
            let host = open(&sim, runtime.clone());

            // Sleeping rather than running the world forward: a freshly opened
            // partition is idle, and a world with nothing scheduled does not
            // advance its clock, so `run_for` would return without ever
            // clearing the warm-up and the run would meet a healthy disk.
            let clock = runtime.clock().clone();
            sim.block_on(async move { clock.sleep(Duration::from_millis(60)).await });
            let started = sim.now_nanos();

            let answers = Arc::new(std::sync::Mutex::new(Vec::new()));
            for contender in 1..=4u64 {
                let host = Arc::clone(&host);
                let answers = Arc::clone(&answers);
                sim.spawn(async move {
                    let answer = host
                        .write(
                            Bytes::from_static(KEY),
                            WriteOp::Put {
                                value: encode(contender),
                                ttl_millis: None,
                            },
                            WriteCondition::IfNotPresent,
                        )
                        .await;
                    answers.lock().expect("results poisoned").push(answer);
                });
            }

            sim.run_until_idle();
            let elapsed = Duration::from_nanos(sim.now_nanos().saturating_sub(started));
            let faulted = sim.faults_injected();
            let answers = answers.lock().expect("results poisoned").clone();
            sim.stop_injecting_faults();
            drop(host);

            if faulted == 0 {
                return Err(sim.failure(
                    "no append was refused, so this seed raced a healthy disk".to_string(),
                ));
            }
            if answers.len() != 4 {
                return Err(sim.failure(format!(
                    "only {} of four acquisitions were answered",
                    answers.len()
                )));
            }
            if elapsed > PROMPT {
                return Err(sim.failure(format!(
                    "four contended acquisitions took {elapsed:?}, so at least one waited out its \
                     budget rather than being released when the write it parked on failed"
                )));
            }

            // A write the log refused was never acknowledged, so nobody may be
            // told it took the lock and nobody may be told they lost to it.
            for ack in answers.iter().filter_map(|answer| answer.as_ref().ok()) {
                if ack.applied {
                    return Err(sim.failure(
                        "an acquisition the log refused was reported as applied".to_string(),
                    ));
                }
                if ack.current_version.is_some() {
                    return Err(sim.failure(format!(
                        "a contender was told it lost to version {:?}, which no write committed",
                        ack.current_version
                    )));
                }
            }
            Ok(())
        },
    );
}

/// How long a conditional write may spend waiting for an in-flight write to
/// the same key before it gives up. Mirrors `host::CONDITION_SETTLE_TIMEOUT`,
/// which is private and deliberately so: the scenario below is about what a
/// client is told when that budget expires, and it has to be able to say when
/// expiry happened without the constant being part of anyone's API.
const SETTLE_BUDGET: Duration = Duration::from_secs(5);

/// How long the replicas in the scenario below take to give up on a call.
///
/// Chosen well above [`SETTLE_BUDGET`] because that is the case the scenario
/// exists for, and because real deployments can produce it: a peer call
/// timeout is configuration, and a local disk write has no timeout at all. So
/// the settle budget is not an upper bound on how long the write being waited
/// for may stay in flight, and a run that assumed otherwise would be testing a
/// world that cannot happen rather than the one that can.
const UNANSWERING_PEER_TIMEOUT: Duration = Duration::from_secs(60);

#[test]
fn a_conditional_write_that_cannot_settle_in_time_is_undecided_rather_than_lost() {
    harness::check_seeds(
        "linearizability::a_conditional_write_that_cannot_settle_in_time_is_undecided_rather_than_lost",
        20,
        |seed| {
            let mut config = SimConfig::new(seed);
            // Long enough that the leading write is still in flight when the
            // contender's budget runs out. This is the whole scenario: the
            // owner is asked to decide a condition it has no answer for, and
            // is still going to have no answer for well after it has to reply.
            config.call_timeout = UNANSWERING_PEER_TIMEOUT;
            let sim = Simulation::with_config(config);
            let runtime = sim.add_node(NodeId(1));
            let replicas = vec![NodeId(2), NodeId(3)];
            for replica in &replicas {
                sim.add_node(*replica);
                // Silent rather than down. A replica that refuses the
                // connection answers immediately and the leading write
                // resolves; one that is simply unreachable leaves it in
                // flight, which is the state the condition cannot be decided
                // against.
                sim.partition(NodeId(1), *replica);
            }
            let host = open_with_replicas(&sim, runtime.clone(), replicas);

            // Unconditional, so it never waits on anything itself, and it is
            // the write everyone else ends up parked on.
            let leader = Arc::clone(&host);
            sim.spawn(async move {
                let _ = leader
                    .write(
                        Bytes::from_static(KEY),
                        WriteOp::Put {
                            value: encode(1),
                            ttl_millis: None,
                        },
                        WriteCondition::None,
                    )
                    .await;
            });

            let answer = Arc::new(std::sync::Mutex::new(None));
            let sink = Arc::clone(&answer);
            let contender = Arc::clone(&host);
            let clock = runtime.clock().clone();
            sim.spawn(async move {
                // Sleeping inside the task rather than running the world
                // forward outside it. The simulated clock jumps to the next
                // scheduled timer, and the only timer the leading write leaves
                // behind is its own peer-call deadline a minute out, so a
                // `run_for` here would land past the very window it is trying
                // to arrive inside. A sleep puts a timer on the calendar at
                // the moment worth stopping at: well past a local append,
                // nowhere near the settle budget.
                clock.sleep(Duration::from_millis(100)).await;
                let started = clock.monotonic_nanos();
                let result = contender
                    .write(
                        Bytes::from_static(KEY),
                        WriteOp::Put {
                            value: encode(2),
                            ttl_millis: None,
                        },
                        WriteCondition::IfNotPresent,
                    )
                    .await;
                *sink.lock().expect("result poisoned") =
                    Some((result, clock.monotonic_nanos().saturating_sub(started)));
            });

            sim.run_until_idle();
            let answer = answer.lock().expect("result poisoned").take();
            drop(host);

            let Some((result, waited)) = answer else {
                return Err(sim.failure(
                    "the contender never got an answer at all".to_string(),
                ));
            };
            let waited = Duration::from_nanos(waited);

            match result {
                // The condition was never decided, so nothing may claim it
                // was. `applied: false` here would be a verdict built out of
                // state read from outside the window the answer lives in: the
                // write it is parked on may still fail, in which case this
                // caller never lost, or land on a version this response does
                // not name.
                Ok(ack) => Err(sim.failure(format!(
                    "an undecided conditional write was answered applied={} \
                     current_version={:?}, which is a verdict the owner never reached",
                    ack.applied, ack.current_version
                ))),
                Err(error) if !error.is_retryable() => Err(sim.failure(format!(
                    "an undecided conditional write failed {error}, which a client cannot \
                     retry; uncertainty has to be retryable or the caller is stuck"
                ))),
                Err(_) if waited < SETTLE_BUDGET => Err(sim.failure(format!(
                    "the contender gave up after {waited:?}, short of its {SETTLE_BUDGET:?} \
                     budget, so it never actually waited the window out"
                ))),
                Err(_) if waited >= UNANSWERING_PEER_TIMEOUT => Err(sim.failure(format!(
                    "the contender answered after {waited:?}, which is when the leading write \
                     finally failed, so this seed proved nothing about the budget"
                ))),
                Err(_) => Ok(()),
            }
        },
    );
}

/// One partition owned by node one and replicated by node two.
fn owner_and_replica_map() -> PartitionMap {
    map_with_replicas(vec![NodeId(2)])
}

fn map_with_replicas(replicas: Vec<NodeId>) -> PartitionMap {
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
        id: PartitionId(1),
        keyspace,
        range: KeyRange::unbounded(),
        owner: Some(NodeId(1)),
        epoch: Epoch(1),
        replicas,
    });
    map
}

const KEYSPACE: &str = "default";

/// How many times the owner renews leases before the test stops.
///
/// A fixed count rather than a loop, because a timer that never stops would
/// keep the simulated world from ever going idle and the run from ever ending.
const RENEWALS: u32 = 40;

fn start_node(sim: &Simulation, node: NodeId, lease: Duration) -> Arc<Node<SimRuntime>> {
    start_node_with_map(sim, node, lease, owner_and_replica_map())
}

fn start_node_with_map(
    sim: &Simulation,
    node: NodeId,
    lease: Duration,
    map: PartitionMap,
) -> Arc<Node<SimRuntime>> {
    let runtime = sim.add_node(node);
    // Each node gets its own store, the way each node owns its own bucket
    // prefix or data directory in production.
    let layout = DataLayout {
        store: Arc::new(MemoryStore::new()),
        wal_root: "wal".to_string(),
        wal_segment_bytes: crate::DEFAULT_WAL_SEGMENT_BYTES,
        durability_acks: 1,
    };
    let source = BoxedMapSource::new(StaticMapSource::new(map));
    sim.block_on(async move {
        // Authentication off: this suite drives the read path directly, so
        // admission passes every request through to it.
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
            Arc::new(crate::ReadinessGate::new()),
            authenticator,
        )
        .await
        .expect("the node starts")
    })
}

#[test]
fn a_same_epoch_owner_restart_waits_out_leases_its_previous_process_granted() {
    let sim = Simulation::new(29);
    let lease = Duration::from_millis(150);
    let map = map_with_replicas(vec![NodeId(2), NodeId(3)]);
    let owner = start_node_with_map(&sim, NodeId(1), lease, map.clone());
    let stale = start_node_with_map(&sim, NodeId(2), lease, map.clone());
    let _quorum = start_node_with_map(&sim, NodeId(3), lease, map.clone());

    let first = sim.block_on({
        let owner = Arc::clone(&owner);
        async move {
            owner
                .set(
                    SetRequest {
                        keyspace: KEYSPACE.to_string(),
                        key: KEY.to_vec(),
                        value: 1u64.to_be_bytes().to_vec(),
                        ttl_millis: None,
                        condition: None,
                    },
                    false,
                    None,
                )
                .await
        }
    });
    assert!(first.is_ok());
    sim.block_on({
        let owner = Arc::clone(&owner);
        async move { owner.renew_leases().await }
    });
    let served_before = stale.replica_reads();
    let old = sim
        .block_on({
            let stale = Arc::clone(&stale);
            async move {
                stale
                    .get(
                        GetRequest {
                            keyspace: KEYSPACE.to_string(),
                            key: KEY.to_vec(),
                        },
                        false,
                        None,
                    )
                    .await
            }
        })
        .unwrap();
    assert_eq!(old.value, 1u64.to_be_bytes());
    assert!(
        stale.replica_reads() > served_before,
        "the replica holds a live lease"
    );

    sim.partition(NodeId(1), NodeId(2));
    drop(owner);
    let before_restart = sim.runtime(NodeId(1)).clock().monotonic_nanos();
    let restarted = start_node_with_map(&sim, NodeId(1), lease, map_with_replicas(vec![NodeId(3)]));
    let after_restart = sim.runtime(NodeId(1)).clock().monotonic_nanos();
    assert!(
        after_restart.saturating_sub(before_restart) >= lease.as_nanos() as u64,
        "the restarted owner must wait out every lease its previous process may have granted"
    );
    let outcome = Arc::new(Mutex::new(None));
    sim.spawn({
        let restarted = Arc::clone(&restarted);
        let outcome = Arc::clone(&outcome);
        async move {
            let result = restarted
                .set(
                    SetRequest {
                        keyspace: KEYSPACE.to_string(),
                        key: KEY.to_vec(),
                        value: 2u64.to_be_bytes().to_vec(),
                        ttl_millis: None,
                        condition: None,
                    },
                    false,
                    None,
                )
                .await;
            *outcome.lock().expect("write outcome poisoned") = Some(result);
        }
    });

    sim.run_until_idle();
    assert!(
        outcome
            .lock()
            .expect("write outcome poisoned")
            .as_ref()
            .is_some_and(|result| result.is_ok()),
        "the write may complete once the possible old lease has certainly expired"
    );
}

#[test]
fn a_replica_serving_reads_never_answers_with_a_value_a_write_has_replaced() {
    harness::check_seeds(
        "linearizability::a_replica_serving_reads_never_answers_with_a_value_a_write_has_replaced",
        20,
        |seed| {
            let sim = Simulation::new(seed);
            // Shorter than the production default so that the owner's
            // heartbeat, which is what releases the last write of a burst,
            // lands inside a test that runs for a few hundred milliseconds of
            // virtual time.
            let lease = Duration::from_millis(150);
            let owner = start_node(&sim, NodeId(1), lease);
            let replica = start_node(&sim, NodeId(2), lease);
            let recorder: Recorder<RegisterOp, RegisterRet> = Recorder::new();
            let clock = sim.add_node(NodeId(1)).clock().clone();

            // The owner's heartbeat, which is what puts the replica in the
            // read set at all. In production this is a loop the server owns.
            {
                let owner = Arc::clone(&owner);
                let clock = clock.clone();
                sim.spawn(async move {
                    for _ in 0..RENEWALS {
                        owner.renew_leases().await;
                        clock.sleep(lease / 3).await;
                    }
                });
            }

            for client in 0..3u64 {
                let owner = Arc::clone(&owner);
                let recorder = recorder.clone();
                let clock = clock.clone();
                sim.spawn(async move {
                    for step in 0..4u64 {
                        let value = client * 100 + step + 1;
                        let call = recorder.invoke(
                            client,
                            RegisterOp::Write(value),
                            clock.monotonic_nanos(),
                        );
                        let written = owner
                            .set(
                                SetRequest {
                                    keyspace: KEYSPACE.to_string(),
                                    key: KEY.to_vec(),
                                    value: value.to_be_bytes().to_vec(),
                                    ttl_millis: None,
                                    condition: None,
                                },
                                false,
                                None,
                            )
                            .await;
                        match written {
                            Ok(response) if response.applied => {
                                recorder.complete(call, RegisterRet::Acked);
                            }
                            _ => recorder.abandon(call),
                        }
                        // Spaced out, because a key being written continuously
                        // is one a replica is not allowed to serve, and a test
                        // where that is always true would only ever exercise
                        // forwarding.
                        clock.sleep(Duration::from_millis(10)).await;
                    }
                });
            }

            // Readers talk only to the replica. Every answer it gives is one
            // the checker has to be able to place in a legal order, which is
            // the whole of what ADR 0001 promises.
            let served = Arc::new(AtomicU64::new(0));
            for reader in 10..12u64 {
                let replica = Arc::clone(&replica);
                let recorder = recorder.clone();
                let clock = clock.clone();
                let served = Arc::clone(&served);
                sim.spawn(async move {
                    for _ in 0..24u64 {
                        let before = replica.replica_reads();
                        let call =
                            recorder.invoke(reader, RegisterOp::Read, clock.monotonic_nanos());
                        let found = replica
                            .get(
                                GetRequest {
                                    keyspace: KEYSPACE.to_string(),
                                    key: KEY.to_vec(),
                                },
                                false,
                                None,
                            )
                            .await;
                        match found {
                            Ok(response) => {
                                let value = response.found.then(|| {
                                    u64::from_be_bytes(
                                        response
                                            .value
                                            .as_slice()
                                            .try_into()
                                            .expect("an eight byte value"),
                                    )
                                });
                                recorder.complete(call, RegisterRet::Value(value));
                                if replica.replica_reads() > before {
                                    served.fetch_add(1, Ordering::SeqCst);
                                }
                            }
                            Err(_) => recorder.abandon(call),
                        }
                        // Slower than the writers on purpose. A read that
                        // lands before the owner's first heartbeat has nothing
                        // to serve under, so a reader that finished inside one
                        // heartbeat would only ever exercise forwarding.
                        clock.sleep(Duration::from_millis(5)).await;
                    }
                });
            }

            sim.run_until_idle();
            let history = recorder.history();
            let locally = served.load(Ordering::SeqCst);
            drop(owner);
            drop(replica);

            if locally == 0 {
                return Err(sim.failure(
                    "the replica forwarded every read, so this seed checked the owner twice"
                        .to_string(),
                ));
            }
            check(&Register, &history, SEARCH_BUDGET)
                .map(|_| ())
                .map_err(|violation| sim.failure(violation.to_string()))
        },
    );
}
