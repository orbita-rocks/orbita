//! What survives when the object store fails during a flush.
//!
//! [ADR 0006](../../../docs/adr/0006-partitions-are-an-index-over-immutable-objects.md)
//! makes three claims that are cheap to state and expensive to be wrong about:
//! the durability boundary is the successful manifest swap, a failed
//! publication leaves the WAL replay range intact, and a stale writer's
//! segments are never reachable from the current manifest. Each is an
//! ordering, and an ordering is exactly the kind of thing that keeps holding
//! in every unit test and stops holding the first time a real bucket returns
//! a 503 halfway through.
//!
//! So these scenarios break the store on purpose, at named points, and then
//! ask two questions. The first is direct: is the bucket and the log in the
//! state the ordering promises? The second is the one that matters to a
//! client: restart the partition on whatever survived and hand every client
//! answer, before and after, to the linearizability checker. A run that loses
//! an acknowledged write shows up there whether or not anybody thought to
//! assert about it.
//!
//! The faults arrive through `orbita_sim::SimBucket`, which is an
//! `HttpTransport` rather than an `ObjectStore`, so the store being faulted is
//! the `S3Store` production runs rather than a stand-in.
//!
//! # What pins the checkpoint-after-publication ordering
//!
//! [`flush_and_checkpoint`](crate::host) publishes the manifest and only then
//! moves the WAL checkpoint. Swapping those two, so the log is checkpointed at
//! its durable position before the segment is published, passes every unit
//! test in the workspace and fails
//! [`a_segment_upload_that_fails_mid_flush_leaves_the_manifest_and_the_replay_range_alone`],
//! [`a_manifest_swap_whose_response_was_lost_still_leaves_the_log_replayable`],
//! and
//! [`a_crash_between_upload_and_publication_loses_nothing_that_was_acknowledged`]:
//! the log forgets entries the manifest never took responsibility for, and the
//! restarted partition answers a read with a value an acknowledged write had
//! already replaced.

use crate::host::{HostSpec, LeasePolicy, PartitionHost, PartitionPaths, WriteOp};

use bytes::Bytes;
use orbita_core::{
    Epoch, KeyRange, KeyspaceId, Lamport, NodeId, PartitionId, Record, WriteCondition,
};
use orbita_format::paths::MANIFEST_NAME;
use orbita_format::{Manifest, PartitionPath};
use orbita_runtime::{Clock, Runtime};
use orbita_sim::lin::{check, Recorder, Register, RegisterOp, RegisterRet};
use orbita_sim::{
    harness, DiskPolicy, SimBucket, SimConfig, SimRuntime, Simulation, StoreFault, StoreFaults,
};

use orbita_storage::ValueCache;
use std::sync::Arc;
use std::time::Duration;

/// How many states the checker may explore. These histories are short and
/// almost serial, so this is generous.
const SEARCH_BUDGET: u64 = 2_000_000;

const KEY: &[u8] = b"register";
const BUCKET: &str = "orbita";

/// Where the partition lives in the bucket. Every host in this module opens
/// this one path, because the interesting scenarios are the ones where two
/// incarnations of a partition reach the same objects.
fn partition_path() -> PartitionPath {
    PartitionPath::new("", KeyspaceId(1), PartitionId(1))
}

fn paths(bucket: &Arc<SimBucket>, wal_dir: &str) -> PartitionPaths {
    PartitionPaths {
        value_cache: Arc::new(ValueCache::new(1 << 20)),
        store: bucket.store(),
        path: partition_path(),
        wal_dir: wal_dir.to_string(),
        wal_segment_bytes: crate::DEFAULT_WAL_SEGMENT_BYTES,
        durability_acks: 1,
    }
}

fn open(
    sim: &Simulation,
    runtime: SimRuntime,
    bucket: &Arc<SimBucket>,
    wal_dir: &str,
    epoch: Epoch,
) -> Arc<PartitionHost<SimRuntime>> {
    let paths = paths(bucket, wal_dir);
    sim.block_on(async move {
        PartitionHost::open_owner(
            runtime,
            HostSpec {
                id: PartitionId(1),
                epoch,
                range: KeyRange::unbounded(),
                lease: LeasePolicy::default(),
            },
            &paths,
            Vec::new(),
        )
        .await
        .expect("the partition opens")
    })
}

/// The manifest the bucket currently holds, read without going through the
/// store, so an assertion about durability cannot itself be faulted.
fn published(bucket: &Arc<SimBucket>) -> Option<Manifest> {
    let key = partition_path().object(MANIFEST_NAME);
    bucket
        .object(&key)
        .map(|bytes| Manifest::decode(&bytes).expect("a published manifest decodes"))
}

/// The flush horizon the bucket stands behind. Nothing at or below this needs
/// the log; everything above it does.
fn horizon(bucket: &Arc<SimBucket>) -> Lamport {
    published(bucket).map_or(Lamport::ZERO, |manifest| manifest.committed_lamport)
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

/// Writes `value` and records what the client was told.
///
/// A write that failed is abandoned rather than recorded, because the client
/// does not know whether it took effect and the checker is entitled to place
/// it anywhere or nowhere.
fn write(
    sim: &Simulation,
    host: &Arc<PartitionHost<SimRuntime>>,
    recorder: &Recorder<RegisterOp, RegisterRet>,
    client: u64,
    value: u64,
) {
    let host = Arc::clone(host);
    let recorder = recorder.clone();
    let at = sim.now_nanos();
    sim.block_on(async move {
        let call = recorder.invoke(client, RegisterOp::Write(value), at);
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
            Ok(ack) if ack.applied => recorder.complete(call, RegisterRet::Acked),
            _ => recorder.abandon(call),
        }
    });
}

/// Writes without putting the call in the history.
///
/// Used for the one write a deposed owner issues after it has been replaced.
/// It really did acknowledge that write, and the acknowledgement is not one
/// the cluster made: routing sends clients to whoever the partition map names,
/// and fencing a process that has not noticed it lost is the control plane's
/// problem rather than the format's. Feeding it to the register checker would
/// be asserting that split brain is linearizable, which it is not and is not
/// what this scenario is about.
fn write_off_the_record(sim: &Simulation, host: &Arc<PartitionHost<SimRuntime>>, value: u64) {
    let host = Arc::clone(host);
    sim.block_on(async move {
        let _ = host
            .write(
                Bytes::from_static(KEY),
                WriteOp::Put {
                    value: encode(value),
                    ttl_millis: None,
                },
                WriteCondition::None,
            )
            .await;
    });
}

/// Reads the register and records the answer.
fn read(
    sim: &Simulation,
    host: &Arc<PartitionHost<SimRuntime>>,
    recorder: &Recorder<RegisterOp, RegisterRet>,
    client: u64,
) {
    let host = Arc::clone(host);
    let recorder = recorder.clone();
    let at = sim.now_nanos();
    sim.block_on(async move {
        let call = recorder.invoke(client, RegisterOp::Read, at);
        match host.get(KEY).await {
            Ok(found) => recorder.complete(call, RegisterRet::Value(found.as_ref().map(decode))),
            Err(_) => recorder.abandon(call),
        }
    });
}

/// Hands the recorded history to the checker.
fn linearizable(
    sim: &Simulation,
    recorder: &Recorder<RegisterOp, RegisterRet>,
) -> Result<(), orbita_sim::Failure> {
    check(&Register, &recorder.history(), SEARCH_BUDGET)
        .map(|_| ())
        .map_err(|violation| sim.failure(violation.to_string()))
}

/// The WAL checkpoint, which is where replay starts on the next open.
fn replay_starts_at(sim: &Simulation, host: &Arc<PartitionHost<SimRuntime>>) -> Lamport {
    let log = host.log();
    sim.block_on(async move { log.applied_through().await })
}

/// The keys the bucket gained since `before`.
fn new_keys(bucket: &Arc<SimBucket>, before: &[String]) -> Vec<String> {
    bucket
        .keys()
        .into_iter()
        .filter(|key| !before.contains(key))
        .collect()
}

#[test]
fn a_segment_upload_that_fails_mid_flush_leaves_the_manifest_and_the_replay_range_alone() {
    let sim = Simulation::new(1);
    let node = NodeId(1);
    let runtime = sim.add_node(node);
    let bucket = sim.bucket(BUCKET);
    let host = open(&sim, runtime, &bucket, "wal/p1", Epoch(1));
    let recorder: Recorder<RegisterOp, RegisterRet> = Recorder::new();

    // One good flush first, so there is a horizon to be left alone rather than
    // an empty bucket that trivially cannot move.
    write(&sim, &host, &recorder, 0, 1);
    sim.block_on({
        let host = Arc::clone(&host);
        async move { host.flush().await.expect("the first flush publishes") }
    });
    let settled = horizon(&bucket);
    assert!(settled > Lamport::ZERO, "the first flush published nothing");

    // A second acknowledged write, and then a store that refuses the segment.
    write(&sim, &host, &recorder, 0, 2);
    bucket.inject_once("PUT", "segments/", StoreFault::Status(503));
    let outcome = sim.block_on({
        let host = Arc::clone(&host);
        async move { host.flush().await }
    });
    assert!(
        outcome.is_err(),
        "a segment the store refused cannot be reported as flushed"
    );
    assert_eq!(
        bucket.armed(),
        0,
        "the refusal never reached a segment upload, so this run tested a healthy store"
    );

    // Invariant one: publication is the boundary, and nothing published.
    assert_eq!(
        horizon(&bucket),
        settled,
        "the manifest moved for a segment that was never written"
    );
    // Invariant two: the log still holds everything the manifest does not.
    assert!(
        replay_starts_at(&sim, &host) <= settled,
        "the log checkpointed past a horizon the bucket does not stand behind"
    );

    // And what survives is still explainable: crash, restart on the same disk
    // and the same bucket, and read the register back.
    sim.crash(node);
    drop(host);
    let runtime = sim.restart(node, DiskPolicy::Intact);
    let recovered = open(&sim, runtime, &bucket, "wal/p1", Epoch(1));
    read(&sim, &recovered, &recorder, 1);
    drop(recovered);

    harness::expect_converged(
        "durability::a_segment_upload_that_fails_mid_flush_leaves_the_manifest_and_the_replay_range_alone",
        linearizable(&sim, &recorder),
    );
}

#[test]
fn a_manifest_swap_whose_response_was_lost_still_leaves_the_log_replayable() {
    // The writer issued a conditional write, the bucket applied it, and the
    // answer never came back. The store did publish, so the data is durable;
    // the writer does not know that, and the rule it has to follow is that a
    // publication it was not told succeeded buys it nothing.
    let sim = Simulation::new(2);
    let node = NodeId(1);
    let runtime = sim.add_node(node);
    let bucket = sim.bucket(BUCKET);
    let host = open(&sim, runtime, &bucket, "wal/p1", Epoch(1));
    let recorder: Recorder<RegisterOp, RegisterRet> = Recorder::new();

    write(&sim, &host, &recorder, 0, 1);
    sim.block_on({
        let host = Arc::clone(&host);
        async move { host.flush().await.expect("the first flush publishes") }
    });
    let settled = horizon(&bucket);
    let checkpoint_before = replay_starts_at(&sim, &host);

    write(&sim, &host, &recorder, 0, 2);
    bucket.inject_once("PUT", MANIFEST_NAME, StoreFault::ResponseLost);
    let outcome = sim.block_on({
        let host = Arc::clone(&host);
        async move { host.flush().await }
    });
    assert!(
        outcome.is_err(),
        "a swap whose answer was lost must be reported as a failure"
    );
    assert_eq!(
        bucket.armed(),
        0,
        "the lost response never reached the manifest write it was armed against"
    );

    // Invariant one, from the other side: the swap succeeded, so the bucket is
    // where the data is, whatever the writer believes.
    assert!(
        horizon(&bucket) > settled,
        "the conditional write applied, so the published horizon must have moved"
    );
    // Invariant two: the writer was told the publication failed, so it must not
    // have narrowed the range the log will replay.
    assert_eq!(
        replay_starts_at(&sim, &host),
        checkpoint_before,
        "the log checkpointed on a publication the writer was told had failed"
    );

    sim.crash(node);
    drop(host);
    let runtime = sim.restart(node, DiskPolicy::Intact);
    let recovered = open(&sim, runtime, &bucket, "wal/p1", Epoch(1));
    read(&sim, &recovered, &recorder, 1);

    // The retry has to compose with what it does not know it published: the
    // segment from the lost swap is either named by the manifest or superseded
    // by one that covers the same writes, and either way the horizon only goes
    // up.
    let before_retry = horizon(&bucket);
    sim.block_on({
        let host = Arc::clone(&recovered);
        async move { host.flush().await.expect("the retry publishes") }
    });
    assert!(
        horizon(&bucket) >= before_retry,
        "a horizon went backwards, which makes the log's replay range a guess"
    );
    read(&sim, &recovered, &recorder, 1);
    drop(recovered);

    harness::expect_converged(
        "durability::a_manifest_swap_whose_response_was_lost_still_leaves_the_log_replayable",
        linearizable(&sim, &recorder),
    );
}

#[test]
fn a_crash_between_upload_and_publication_loses_nothing_that_was_acknowledged() {
    // The window ADR 0006 is argued on: the segment is in the bucket, the
    // manifest still points somewhere else, and the process is gone. Nothing
    // in memory can be consulted about what happened, so the answer has to be
    // reconstructible from the bucket and the log alone.
    let sim = Simulation::new(3);
    let node = NodeId(1);
    let runtime = sim.add_node(node);
    let bucket = sim.bucket(BUCKET);
    let host = open(&sim, runtime.clone(), &bucket, "wal/p1", Epoch(1));
    let recorder: Recorder<RegisterOp, RegisterRet> = Recorder::new();

    write(&sim, &host, &recorder, 0, 1);
    sim.block_on({
        let host = Arc::clone(&host);
        async move { host.flush().await.expect("the first flush publishes") }
    });
    let settled = horizon(&bucket);
    let keys_before = bucket.keys();

    write(&sim, &host, &recorder, 0, 2);
    bucket.inject_once("PUT", MANIFEST_NAME, StoreFault::CrashNode(node));
    // Spawned on the node, so the crash takes the flush with it rather than
    // letting it observe an error and tidy up. A process does not get to.
    runtime.spawn({
        let host = Arc::clone(&host);
        async move {
            let _ = host.flush().await;
        }
    });
    sim.run_until_idle();
    assert!(!sim.is_up(node), "the injected crash never landed");

    // The segment reached the bucket and the manifest did not name it.
    let stranded = new_keys(&bucket, &keys_before);
    assert!(
        !stranded.is_empty(),
        "the crash landed before the upload, so this run tested the wrong window"
    );
    assert_eq!(
        horizon(&bucket),
        settled,
        "the manifest moved without a swap ever completing"
    );
    let named = published(&bucket)
        .expect("a manifest was published earlier")
        .segments
        .iter()
        .map(|entry| partition_path().object(&entry.name))
        .collect::<Vec<_>>();
    for key in &stranded {
        assert!(
            !named.contains(key),
            "{key} was stranded by a crash and the manifest names it anyway"
        );
    }

    drop(host);
    let runtime = sim.restart(node, DiskPolicy::Intact);
    let recovered = open(&sim, runtime, &bucket, "wal/p1", Epoch(1));
    // Invariant two, checked where it is observable: the write above the
    // horizon has to come back out of the log.
    assert!(
        replay_starts_at(&sim, &recovered) <= settled,
        "the restarted log will not replay the writes the manifest never took"
    );
    read(&sim, &recovered, &recorder, 1);
    drop(recovered);

    harness::expect_converged(
        "durability::a_crash_between_upload_and_publication_loses_nothing_that_was_acknowledged",
        linearizable(&sim, &recorder),
    );
}

#[test]
fn a_stale_writers_segments_are_never_reachable_from_the_current_manifest() {
    // Two owners, both alive, both convinced they hold the partition. The
    // deposed one has a current entity tag, so nothing about the conditional
    // write itself stops it from erasing its replacement's work; the epoch
    // carried in the manifest is what does.
    let sim = Simulation::new(4);
    let bucket = sim.bucket(BUCKET);
    let recorder: Recorder<RegisterOp, RegisterRet> = Recorder::new();

    let deposed = open(&sim, sim.add_node(NodeId(1)), &bucket, "wal/one", Epoch(1));
    write(&sim, &deposed, &recorder, 0, 1);
    sim.block_on({
        let host = Arc::clone(&deposed);
        async move { host.flush().await.expect("the first owner publishes") }
    });

    // The replacement opens the same partition at a higher epoch, hydrates
    // from the manifest, and publishes its own writes.
    let replacement = open(&sim, sim.add_node(NodeId(2)), &bucket, "wal/two", Epoch(2));
    write(&sim, &replacement, &recorder, 1, 2);
    sim.block_on({
        let host = Arc::clone(&replacement);
        async move { host.flush().await.expect("the replacement publishes") }
    });
    let current = published(&bucket).expect("the replacement published");
    assert_eq!(current.epoch, Epoch(2));
    let replacement_segments: Vec<String> = current
        .segments
        .iter()
        .map(|entry| entry.name.clone())
        .collect();
    let keys_before = bucket.keys();

    // The deposed owner, which has heard nothing, writes and flushes. Its
    // segment reaches the bucket; its manifest must not.
    write_off_the_record(&sim, &deposed, 3);
    let outcome = sim.block_on({
        let host = Arc::clone(&deposed);
        async move { host.flush().await }
    });
    assert!(
        outcome.is_err(),
        "a deposed owner published a manifest, which erases its replacement"
    );

    let after = published(&bucket).expect("a manifest is still there");
    assert_eq!(after.epoch, Epoch(2), "the deposed owner's epoch took over");
    let stale = new_keys(&bucket, &keys_before);
    assert!(
        !stale.is_empty(),
        "the deposed owner never uploaded, so this run tested the wrong thing"
    );
    for key in &stale {
        let name = partition_path()
            .relative(key)
            .expect("every object sits under the partition");
        assert!(
            !after.segments.iter().any(|entry| entry.name == name),
            "{name} was written by a deposed owner and the current manifest names it"
        );
    }
    assert_eq!(
        after
            .segments
            .iter()
            .map(|entry| entry.name.clone())
            .collect::<Vec<_>>(),
        replacement_segments,
        "the replacement's segments changed under it"
    );

    // A replacement worker with no local state hydrates from the bucket alone,
    // which is the case that turns "unreachable from the manifest" into
    // "unreachable at all".
    drop(deposed);
    drop(replacement);
    let fresh = open(
        &sim,
        sim.add_node(NodeId(3)),
        &bucket,
        "wal/three",
        Epoch(3),
    );
    read(&sim, &fresh, &recorder, 2);
    drop(fresh);

    harness::expect_converged(
        "durability::a_stale_writers_segments_are_never_reachable_from_the_current_manifest",
        linearizable(&sim, &recorder),
    );
}

/// How many writes a seeded run issues before it stops breaking the store.
const BATCH_WRITES: u64 = 12;

#[test]
fn a_partition_flushing_through_a_failing_store_never_loses_an_acknowledged_write() {
    // The named scenarios above cover the three failures anybody thought of.
    // This one covers the ones nobody did: every store call is a candidate for
    // a lost request, a lost response, or a throttle, and the seed decides
    // which. What has to hold is the same thing, so the assertion is the
    // checker rather than a list.
    harness::check_seeds(
        "durability::a_partition_flushing_through_a_failing_store_never_loses_an_acknowledged_write",
        24,
        |seed| {
            let mut config = SimConfig::new(seed);
            // Well above `StoreFaults::chaotic`, because a run makes only a
            // few dozen store calls and this scenario is worthless on a seed
            // that met a healthy bucket. The guard at the end refuses such a
            // seed rather than passing quietly, and these rates are what keep
            // the guard from being the usual outcome.
            config.store = StoreFaults {
                request_lost_permille: 60,
                response_lost_permille: 60,
                server_error_permille: 60,
                slow_permille: 20,
                ..StoreFaults::none()
            };
            // Opening the partition is setup rather than the thing under test,
            // and a run whose store refused to let it open would explore
            // nothing. The writes below start well after this.
            config.fault_warmup = Duration::from_millis(50);
            let sim = Simulation::with_config(config);

            let node = NodeId(1);
            let runtime = sim.add_node(node);
            let bucket = sim.bucket(BUCKET);
            let host = open(&sim, runtime.clone(), &bucket, "wal/p1", Epoch(1));
            let recorder: Recorder<RegisterOp, RegisterRet> = Recorder::new();

            sim.run_for(Duration::from_millis(60));
            // One named failure per seed on top of whatever the rates
            // produce. Sampling alone leaves a small fraction of seeds
            // meeting a healthy bucket, and a nightly batch of fifty thousand
            // turns "a small fraction" into "several every night that tested
            // nothing".
            let (method, target, fault) = match sim.random_below(6) {
                0 => ("PUT", "segments/", StoreFault::RequestLost),
                1 => ("PUT", "segments/", StoreFault::Status(503)),
                2 => ("PUT", MANIFEST_NAME, StoreFault::ResponseLost),
                3 => ("PUT", MANIFEST_NAME, StoreFault::RequestLost),
                4 => ("PUT", MANIFEST_NAME, StoreFault::Status(500)),
                _ => ("GET", MANIFEST_NAME, StoreFault::Status(503)),
            };
            // Armed early enough that several flushes remain to match it, so
            // the check at the end of the loop is about the system rather
            // than about the scenario running out of room.
            let arm_at = sim.random_below(BATCH_WRITES / 3);

            for step in 0..BATCH_WRITES {
                if step == arm_at {
                    bucket.inject_once(method, target, fault.clone());
                }
                write(&sim, &host, &recorder, 0, step + 1);
                read(&sim, &host, &recorder, 0);
                // Every write gets a flush attempt, so most of them meet a
                // store that is failing. A flush that fails is not an error
                // here: the log still holds the write, which is the claim.
                sim.block_on({
                    let host = Arc::clone(&host);
                    async move {
                        let _ = host.flush().await;
                    }
                });
                let clock = runtime.clock().clone();
                sim.block_on(async move { clock.sleep(Duration::from_millis(5)).await });
            }

            // The arming above makes a fault-free seed vanishingly unlikely
            // rather than impossible: a run whose sampled faults happened to
            // kill every segment upload never reaches the manifest write it
            // armed. That seed still tested a failing store, which is what
            // this asks. A seed that met a healthy one tested nothing.
            if sim.last_fault_nanos().is_none() {
                return Err(sim.failure(format!(
                    "no fault landed, so this seed tested a healthy bucket \
                     ({method} {target} was armed at write {arm_at})"
                )));
            }

            // Whatever the bucket holds, the log must not have checkpointed
            // past it, or a restart would replay from a position nothing
            // stands behind.
            let published_at = horizon(&bucket);
            if replay_starts_at(&sim, &host) > published_at {
                return Err(sim.failure(format!(
                    "the log checkpointed through {} with the bucket only at {published_at}",
                    replay_starts_at(&sim, &host)
                )));
            }

            // Recovery is only meaningful once the world stops being torn at.
            sim.stop_injecting_faults();
            sim.crash(node);
            drop(host);
            let runtime = sim.restart(node, DiskPolicy::Intact);
            let recovered = open(&sim, runtime, &bucket, "wal/p1", Epoch(1));
            read(&sim, &recovered, &recorder, 1);
            drop(recovered);

            linearizable(&sim, &recorder)
        },
    );
}
