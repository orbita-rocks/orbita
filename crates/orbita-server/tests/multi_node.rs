//! Three worker nodes, one leader group, real sockets, and a dead owner.
//!
//! Everything above the transport has been exercised under the simulator,
//! which is the right place to explore unlucky interleavings and the wrong
//! place to find out that a length prefix is written in the wrong byte order.
//! So this test uses the real peer transport over loopback, the real control
//! plane, and the generated gRPC client, and it kills a node.
//!
//! # What it is asserting
//!
//! The product's claim is that reads scale to replicas without giving up
//! linearizability, and that losing an owner loses no acknowledged write. Both
//! are checked here rather than argued:
//!
//! - a replica answers reads out of its own storage, counted rather than
//!   assumed, because a forwarded read and a locally served one look identical
//!   to a client;
//! - a reader running throughout never sees a value go backwards, which is the
//!   observable form of the guarantee;
//! - every write acknowledged before the owner died is readable after another
//!   node has taken over.
//!
//! # Why it is in-process
//!
//! Three processes would be more faithful and would make the test depend on a
//! built binary, a working directory, and a way to notice a child that died
//! for the wrong reason. Everything this is checking lives above the process
//! boundary and below the client API, and the sockets here are real, so the
//! part that would gain fidelity is the part already covered by packaging.

use orbita_control::{
    binary_speaks, BootstrapSpec, ClusterVersion, ControlCommand, ControlConfig, ControlService,
    Controller, KeyspaceConfig, SingleNodeLog,
};
use orbita_core::{NodeId, PartitionMap};
use orbita_proto::v1::kv_client::KvClient;
use orbita_proto::v1::{GetRequest, SetRequest};
use orbita_runtime::{Runtime, ServiceId, Transport};
use orbita_server::{Server, ServerConfig, ServerRuntime, DEFAULT_KEYSPACE};

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tonic::transport::Channel;

/// The leader group is one node in this test, because what is under test is
/// the worker side of failover rather than consensus.
const LEADER: NodeId = NodeId(10);
const WORKERS: [NodeId; 3] = [NodeId(1), NodeId(2), NodeId(3)];

/// A tight failover budget so the test runs in seconds rather than tens of
/// them. Every interval scales together, which is the point of expressing it
/// as a budget rather than as four numbers.
const BUDGET: Duration = Duration::from_secs(2);

/// A directory nothing else is using, removed when the test ends.
struct DataDir(PathBuf);

impl DataDir {
    fn new(name: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "orbita-multi-node-{}-{name}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::remove_dir_all(&path).ok();
        std::fs::create_dir_all(&path).expect("a temp directory");
        Self(path)
    }
}

impl Drop for DataDir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).ok();
    }
}

/// The leader group, as one node with its own peer listener.
struct LeaderGroup {
    controller: Controller<ServerRuntime, SingleNodeLog<ServerRuntime>>,
    address: String,
    _dir: DataDir,
}

async fn start_leader_group(config: ControlConfig) -> LeaderGroup {
    let dir = DataDir::new("leader");
    let runtime = ServerRuntime::new(LEADER, &dir.0, Some(1));
    let log = SingleNodeLog::open(&runtime)
        .await
        .expect("the consensus log opens");
    let controller = Controller::new(runtime.clone(), log, config);

    runtime
        .transport()
        .register(ServiceId::Control, ControlService::new(controller.clone()));
    let listener = runtime
        .transport()
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .expect("the leader group binds a peer port");
    let address = listener.local_addr().to_string();
    // Held by the accept loop for the life of the test; the directory guard
    // outliving it is what matters for cleanup.
    std::mem::forget(listener);

    controller
        .bootstrap(&BootstrapSpec {
            keyspace: DEFAULT_KEYSPACE.to_string(),
            config: KeyspaceConfig::default(),
            leaders: vec![(LEADER, address.clone())],
            // Registered before they start, so the keyspace is born with an
            // owner rather than born unavailable. The addresses are corrected
            // by the first heartbeat from each node.
            workers: WORKERS.iter().map(|n| (*n, String::new())).collect(),
        })
        .await
        .expect("the cluster bootstraps");

    let sweeping = controller.clone();
    tokio::spawn(async move { sweeping.run().await });

    LeaderGroup {
        controller,
        address,
        _dir: dir,
    }
}

/// One worker, with the directory it writes to.
struct Worker {
    id: NodeId,
    server: Option<Server>,
    client: KvClient<Channel>,
    _dir: DataDir,
}

async fn start_worker(id: NodeId, leader: &str, lease: Duration) -> Worker {
    let dir = DataDir::new(&format!("worker{}", id.get()));
    let config = ServerConfig::single_node(&dir.0)
        .with_node_id(id)
        .on_ephemeral_port()
        .with_peers(vec![(LEADER, leader.to_string())])
        .with_leader_group(vec![LEADER])
        .with_control_poll_interval(Duration::from_millis(50))
        .with_lease_duration(lease);

    let server = Server::start(config).await.expect("a worker starts");
    let client = KvClient::connect(format!("http://{}", server.local_addr()))
        .await
        .expect("a client connects");
    Worker {
        id,
        server: Some(server),
        client,
        _dir: dir,
    }
}

impl Worker {
    fn server(&self) -> &Server {
        self.server.as_ref().expect("this worker is still running")
    }

    /// Stops this worker the way a crash would: no drain, no goodbye.
    async fn kill(&mut self) {
        let server = self.server.take().expect("this worker is still running");
        server.shutdown().await.expect("the listener stops");
    }

    async fn drain(&mut self, timeout: Duration) -> orbita_core::Result<()> {
        let server = self.server.take().expect("this worker is still running");
        server.drain(timeout).await
    }
}

fn set(key: &str, value: &str) -> SetRequest {
    SetRequest {
        keyspace: DEFAULT_KEYSPACE.to_string(),
        key: key.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
        ttl_millis: None,
        condition: None,
    }
}

fn get(key: &str) -> GetRequest {
    GetRequest {
        keyspace: DEFAULT_KEYSPACE.to_string(),
        key: key.as_bytes().to_vec(),
    }
}

/// Waits for something to become true, or gives up loudly.
async fn until(what: &str, limit: Duration, mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + limit;
    while Instant::now() < deadline {
        if ready() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("gave up waiting for {what} after {limit:?}");
}

/// The owner and replicas of the cluster's only partition.
fn placement(map: &PartitionMap) -> (Option<NodeId>, Vec<NodeId>) {
    let info = map.partitions().next().expect("one partition exists");
    (info.owner, info.replicas.clone())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cluster_survives_losing_the_owner_without_losing_an_acknowledged_write() {
    let control = ControlConfig::for_failover_budget(BUDGET);
    let lease = control.lease_duration;
    let group = start_leader_group(control.clone()).await;

    let mut workers = Vec::new();
    for id in WORKERS {
        workers.push(start_worker(id, &group.address, lease).await);
    }
    // Nothing tells these nodes where each other are. Each reports its own
    // peer address to the leader group on its heartbeat and reads the rest
    // back on the same timer, which is the only reason a write below can reach
    // a replica at all. A deployment configures the leader group and no more,
    // and this is the test that says so.

    // The leader group places the replicas once it has heard from everyone.
    let seen = group.controller.clone();
    let placed = Arc::new(std::sync::Mutex::new((None, Vec::new())));
    let watch = Arc::clone(&placed);
    tokio::spawn(async move {
        loop {
            let map = seen.partition_map().await;
            *watch.lock().unwrap() = placement(&map);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    until(
        "the partition to get a full replica set",
        BUDGET * 4,
        || placed.lock().unwrap().1.len() == 2,
    )
    .await;

    let (owner, replicas) = placed.lock().unwrap().clone();
    let owner = owner.expect("a placed partition has an owner");
    let replica = replicas[0];
    let survivor = replicas[1];

    // Writes go through the node that owns the partition; reads go to a node
    // that only replicates it, which is the path this whole design exists for.
    let owner_index = workers
        .iter()
        .position(|w| w.id == owner)
        .expect("the owner");
    let replica_index = workers
        .iter()
        .position(|w| w.id == replica)
        .expect("a replica");

    // A write needs the owner to be able to dial its replicas, which it can
    // only do once the leader group has told it where they are. That is one
    // control poll after the replicas were placed, so the first write waits
    // for it rather than assuming it has already happened.
    {
        let mut warmup = workers[owner_index].client.clone();
        let deadline = Instant::now() + BUDGET * 6;
        loop {
            let outcome = warmup.set(set("warmup", "ready")).await;
            let applied = matches!(&outcome, Ok(response) if response.get_ref().applied);
            if applied {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the cluster never became writable: {outcome:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    let acknowledged = {
        let mut written = Vec::new();
        for i in 0..20u32 {
            let response = workers[owner_index]
                .client
                .set(set(&format!("k{i}"), &format!("v{i}")))
                .await
                .expect("a write to the owner succeeds")
                .into_inner();
            assert!(response.applied, "write {i} was refused");
            written.push((format!("k{i}"), format!("v{i}")));
        }
        written
    };

    // A replica takes a read lease from the owner's heartbeat, so give it one
    // heartbeat before asking it to serve.
    tokio::time::sleep(lease).await;
    for (key, value) in &acknowledged {
        let found = workers[replica_index]
            .client
            .get(get(key))
            .await
            .expect("a read from a replica succeeds")
            .into_inner();
        assert!(found.found, "{key} was missing from the replica");
        assert_eq!(String::from_utf8_lossy(&found.value), *value);
    }
    let deadline = Instant::now() + BUDGET * 3;
    while workers[replica_index].server().replica_reads() == 0 && Instant::now() < deadline {
        let _ = workers[replica_index]
            .client
            .get(get(&acknowledged[0].0))
            .await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        workers[replica_index].server().replica_reads() > 0,
        "every read forwarded to the owner, so replicas are not serving reads at all"
    );

    // A reader that runs across the failover, and a value that only ever goes
    // up. A read that sees it go down is the guarantee breaking, and no amount
    // of unavailability is.
    let highest_seen = Arc::new(AtomicU64::new(0));
    let reads_after_failover = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicU64::new(0));
    let mut reader = workers[replica_index].client.clone();
    let watching = Arc::clone(&highest_seen);
    let counting = Arc::clone(&reads_after_failover);
    let stopping = Arc::clone(&stop);
    let reading = tokio::spawn(async move {
        let mut violations = Vec::new();
        while stopping.load(Ordering::Relaxed) == 0 {
            if let Ok(response) = reader.get(get("counter")).await {
                let response = response.into_inner();
                if response.found {
                    let seen: u64 = String::from_utf8_lossy(&response.value)
                        .parse()
                        .unwrap_or(0);
                    let best = watching.load(Ordering::SeqCst);
                    if seen < best {
                        violations.push(format!("read {seen} after having read {best}"));
                    } else {
                        watching.store(seen, Ordering::SeqCst);
                    }
                    counting.fetch_add(1, Ordering::Relaxed);
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        violations
    });

    let committed = Arc::new(AtomicU64::new(0));
    let mut writer = workers[owner_index].client.clone();
    let writing = Arc::clone(&committed);
    let stopping = Arc::clone(&stop);
    let counting = tokio::spawn(async move {
        let mut next = 1u64;
        while stopping.load(Ordering::Relaxed) == 0 {
            // The owner dying stops the writer rather than making it retry
            // elsewhere, because what is being checked is that nothing it was
            // told succeeded is lost.
            let Ok(response) = writer.set(set("counter", &next.to_string())).await else {
                break;
            };
            if !response.into_inner().applied {
                break;
            }
            writing.store(next, Ordering::SeqCst);
            next += 1;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });

    until("the writer to commit something", BUDGET, || {
        committed.load(Ordering::SeqCst) > 0
    })
    .await;

    // Kill the owner.
    workers[owner_index].kill().await;
    let acknowledged_before_the_kill = committed.load(Ordering::SeqCst);
    let _ = counting.await;

    until(
        "the partition to be handed to a survivor",
        BUDGET * 6,
        || matches!(placed.lock().unwrap().0, Some(new) if new != owner),
    )
    .await;
    let promoted = placed.lock().unwrap().0.expect("a new owner");
    assert!(
        promoted == replica || promoted == survivor,
        "the partition went to a node that was not holding a copy of it"
    );

    // Reads have to work again once the new owner is serving, whichever
    // surviving node the client happens to be talking to.
    let mut client = workers
        .iter()
        .find(|w| w.id == survivor)
        .expect("the other survivor")
        .client
        .clone();
    let mut last = None;
    let mut refused = None;
    for _ in 0..200 {
        match client.get(get("counter")).await {
            Ok(response) => {
                last = Some(response.into_inner());
                break;
            }
            Err(status) => {
                refused = Some(status);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    let after = last.unwrap_or_else(|| {
        panic!("reads never came back after the failover; the last refusal was {refused:?}")
    });
    assert!(after.found, "the counter went missing across the failover");

    // No acknowledged write is lost. The counter is the interesting one,
    // because it was being written at the moment the owner died.
    let survived: u64 = String::from_utf8_lossy(&after.value).parse().unwrap_or(0);
    assert!(
        survived >= acknowledged_before_the_kill,
        "the cluster lost an acknowledged write: it said {acknowledged_before_the_kill} was \
         committed and now holds {survived}"
    );

    for (key, value) in &acknowledged {
        let found = client
            .get(get(key))
            .await
            .expect("a read after the failover succeeds")
            .into_inner();
        assert!(found.found, "{key} did not survive the failover");
        assert_eq!(
            String::from_utf8_lossy(&found.value),
            *value,
            "{key} came back changed"
        );
    }

    stop.store(1, Ordering::Relaxed);
    let violations = reading.await.expect("the reader finishes");
    assert!(
        violations.is_empty(),
        "a read went backwards across the failover: {violations:?}"
    );
    assert!(
        reads_after_failover.load(Ordering::Relaxed) > 0,
        "the reader never saw the counter at all, so it proved nothing"
    );

    for worker in &mut workers {
        if worker.server.is_some() {
            worker.kill().await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_joined_worker_reports_ready_once_the_leader_group_has_heard_from_it() {
    let control = ControlConfig::for_failover_budget(BUDGET);
    let lease = control.lease_duration;
    let group = start_leader_group(control).await;

    let mut worker = start_worker(WORKERS[0], &group.address, lease).await;

    // Recovery and partition open complete inside `Server::start`, but the
    // join lands only when the control loop's first heartbeat is accepted, so
    // readiness is awaited rather than asserted. The subscription is the same
    // handle the SIGTERM handoff will consume, which is why this waits on the
    // gate instead of polling the RPC.
    let gate = worker.server().readiness();
    let mut watched = gate.subscribe();
    tokio::time::timeout(
        BUDGET * 4,
        watched.wait_for(orbita_server::ReadinessState::is_ready),
    )
    .await
    .expect("the worker becomes ready within the budget")
    .expect("the gate outlives the wait");

    // The wire agrees with the gate.
    let mut health = orbita_proto::v1::health_client::HealthClient::connect(format!(
        "http://{}",
        worker.server().local_addr()
    ))
    .await
    .expect("a health client connects");
    let response = health
        .check_readiness(orbita_proto::v1::CheckReadinessRequest {})
        .await
        .expect("readiness is answered")
        .into_inner();
    assert!(response.ready, "unmet: {:?}", response.conditions);

    worker.kill().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_planned_shutdown_hands_off_acknowledged_writes_and_retires_the_old_owner() {
    let control = ControlConfig::for_failover_budget(BUDGET);
    let lease = control.lease_duration;
    let group = start_leader_group(control).await;
    let mut workers = Vec::new();
    for id in WORKERS {
        workers.push(start_worker(id, &group.address, lease).await);
    }

    let seen = group.controller.clone();
    let placed = Arc::new(std::sync::Mutex::new((None, Vec::new())));
    let watch = Arc::clone(&placed);
    tokio::spawn(async move {
        loop {
            *watch.lock().unwrap() = placement(&seen.partition_map().await);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    until("the partition to be replicated", BUDGET * 4, || {
        placed.lock().unwrap().1.len() == 2
    })
    .await;
    let old_owner = placed.lock().unwrap().0.unwrap();
    let old_index = workers
        .iter()
        .position(|worker| worker.id == old_owner)
        .unwrap();

    let deadline = Instant::now() + BUDGET * 4;
    loop {
        let outcome = workers[old_index]
            .client
            .set(set("drain-warmup", "ready"))
            .await;
        if matches!(&outcome, Ok(response) if response.get_ref().applied) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the cluster never became writable"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let mut writer = workers[old_index].client.clone();
    let writes = tokio::spawn(async move {
        let mut acknowledged = Vec::new();
        for i in 0..50u64 {
            let key = format!("drain-{i}");
            match writer.set(set(&key, &i.to_string())).await {
                Ok(response) if response.get_ref().applied => acknowledged.push((key, i)),
                Ok(_) | Err(_) => break,
            }
        }
        acknowledged
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    workers[old_index]
        .drain(BUDGET * 6)
        .await
        .expect("the planned handoff completes");
    let acknowledged = writes.await.expect("the writer task finishes");
    assert!(
        !acknowledged.is_empty(),
        "the concurrent writer proved nothing"
    );

    let map = group.controller.partition_map().await;
    let (new_owner, _) = placement(&map);
    let new_owner = new_owner.expect("the partition remains owned");
    assert_ne!(new_owner, old_owner);
    let new_index = workers
        .iter()
        .position(|worker| worker.id == new_owner)
        .unwrap();
    for (key, value) in acknowledged {
        let response = workers[new_index]
            .client
            .get(get(&key))
            .await
            .expect("an acknowledged write is readable from the new owner")
            .into_inner();
        assert_eq!(String::from_utf8_lossy(&response.value), value.to_string());
    }

    assert!(
        workers[old_index].client.get(get("drain-0")).await.is_err(),
        "the stale owner endpoint must stop serving after transfer"
    );
    for worker in &mut workers {
        if worker.server.is_some() {
            worker.kill().await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_planned_shutdown_completes_when_the_replicas_were_placed_after_the_last_write() {
    // The load-sensitive version of this is
    // `a_planned_shutdown_hands_off_acknowledged_writes_and_retires_the_old_owner`,
    // which only reaches this state when the scheduler is busy enough that the
    // owner's map poll loses a race with the drain. Here the ordering is built
    // rather than waited for: one worker starts alone, so the partition is born
    // owned and unreplicated, every write lands while that is still true, and
    // the replicas are placed afterwards with nothing left to write.
    //
    // That is the shape a SIGTERM arriving shortly after a scale-up has, and it
    // is the one where a handoff has to be made possible rather than merely
    // permitted: the control plane will not give a partition to a replica that
    // has not caught up, write admission is already closed, and an append is
    // the only thing that ever moves a replica forward.
    let control = ControlConfig::for_failover_budget(BUDGET);
    let lease = control.lease_duration;
    let group = start_leader_group(control).await;

    let mut workers = vec![start_worker(WORKERS[0], &group.address, lease).await];
    let seen = group.controller.clone();
    let placed = Arc::new(std::sync::Mutex::new((None, Vec::new())));
    let watch = Arc::clone(&placed);
    tokio::spawn(async move {
        loop {
            *watch.lock().unwrap() = placement(&seen.partition_map().await);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    // Twice the budget the other tests give this, because they start from a
    // cluster that is already at its final shape and this one is waiting out a
    // cold bootstrap. The number that matters to what is under test is the
    // drain budget below, which is the same as everywhere else.
    const SETUP: Duration = Duration::from_secs(16);
    until("the only worker to be given the partition", SETUP, || {
        let held = placed.lock().unwrap();
        held.0 == Some(WORKERS[0]) && held.1.is_empty()
    })
    .await;

    // Acknowledged while the map honestly promises a single copy.
    let mut acknowledged = Vec::new();
    let deadline = Instant::now() + SETUP;
    while acknowledged.len() < 10 {
        let key = format!("early-{}", acknowledged.len());
        let value = acknowledged.len().to_string();
        match workers[0].client.set(set(&key, &value)).await {
            Ok(response) if response.get_ref().applied => acknowledged.push((key, value)),
            _ => assert!(
                Instant::now() < deadline,
                "the single-node cluster never became writable"
            ),
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // The scale-up. Nothing writes from here on, so the only thing that can
    // put these entries on a second node is the owner deciding to send them.
    for id in &WORKERS[1..] {
        workers.push(start_worker(*id, &group.address, lease).await);
    }
    until("the scaled-up partition to be replicated", SETUP, || {
        placed.lock().unwrap().1.len() == 2
    })
    .await;

    workers[0]
        .drain(BUDGET * 6)
        .await
        .expect("the planned handoff completes");

    let (new_owner, _) = placement(&group.controller.partition_map().await);
    let new_owner = new_owner.expect("the partition remains owned");
    assert_ne!(new_owner, WORKERS[0], "the drained node is still the owner");
    let new_index = workers
        .iter()
        .position(|worker| worker.id == new_owner)
        .unwrap();
    for (key, value) in acknowledged {
        let response = workers[new_index]
            .client
            .get(get(&key))
            .await
            .expect("an acknowledged write is readable from the new owner")
            .into_inner();
        assert!(response.found, "{key} did not survive the handoff");
        assert_eq!(String::from_utf8_lossy(&response.value), value);
    }

    for worker in &mut workers {
        if worker.server.is_some() {
            worker.kill().await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_drain_timeout_is_reported_as_failure() {
    let control = ControlConfig::for_failover_budget(BUDGET);
    let lease = control.lease_duration;
    let group = start_leader_group(control).await;
    let mut worker = start_worker(WORKERS[0], &group.address, lease).await;

    let deadline = Instant::now() + BUDGET * 3;
    while !worker.server().readiness().is_ready() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        worker.server().readiness().is_ready(),
        "worker never became ready"
    );

    let error = worker
        .drain(Duration::from_millis(10))
        .await
        .expect_err("a ten millisecond budget cannot drain a live lease");
    assert!(error.to_string().contains("drain timed out"), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_incompatible_rejoin_stays_running_and_reports_version_readiness() {
    let control = ControlConfig::for_failover_budget(BUDGET);
    let lease = control.lease_duration;
    let group = start_leader_group(control).await;
    let previous = binary_speaks().max;
    let active = ClusterVersion::new(previous.major, previous.minor + 1);
    group
        .controller
        .submit(ControlCommand::SetClusterVersion {
            version: active,
            expect: previous,
        })
        .await
        .expect("the test advances beyond this binary's range");

    let mut worker = start_worker(WORKERS[0], &group.address, lease).await;
    tokio::time::timeout(BUDGET, async {
        loop {
            if worker.server().active_cluster_version() == Some(active) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the worker receives the structured refusal");

    let readiness = worker.server().readiness().state();
    assert!(
        !readiness.is_ready(),
        "an incompatible node must not be Ready"
    );
    assert!(!readiness.is_met(orbita_server::ReadinessCondition::ClusterVersionCompatible));
    assert!(!readiness.is_met(orbita_server::ReadinessCondition::ControlPlaneJoined));

    // The health endpoint is still reachable, which is what keeps diagnostics
    // available while Kubernetes holds the rollout at this pod.
    let mut health = orbita_proto::v1::health_client::HealthClient::connect(format!(
        "http://{}",
        worker.server().local_addr()
    ))
    .await
    .expect("the incompatible worker stays alive");
    let response = health
        .check_readiness(orbita_proto::v1::CheckReadinessRequest {})
        .await
        .expect("the running worker answers readiness")
        .into_inner();
    assert!(response
        .conditions
        .iter()
        .any(|condition| condition.name == "cluster-version-compatible" && !condition.met));

    worker.kill().await;
}
