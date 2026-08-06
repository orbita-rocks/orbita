//! Determinism is easy to hold when nothing goes wrong. These run a cluster
//! with every fault switched on, which is the only version of the claim worth
//! making.

use orbita_core::NodeId;
use orbita_objectstore::{ObjectStore, Precondition};
use orbita_runtime::{
    Clock, Disk, File, OpenOptions, PeerCall, PeerHandler, Runtime, ServiceId, Transport,
    TransportError,
};
use orbita_sim::{SimConfig, SimRuntime, Simulation};

use bytes::Bytes;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Appends what it is sent to its own log and publishes it to a shared
/// bucket, so a run exercises the disk, the network, and the object store at
/// once.
#[derive(Clone, Default)]
struct Logger {
    /// Absent for a node that only has to answer, which keeps the warmup test
    /// from having to stand up a disk it does not use.
    rt: Option<SimRuntime>,
    /// Absent for the same reason. Present, it makes every delivered message
    /// cost a conditional write, which is where the store's own draws enter
    /// the run.
    store: Option<Arc<dyn ObjectStore>>,
    applied: Arc<AtomicU64>,
}

impl PeerHandler for Logger {
    fn handle(
        &self,
        _from: NodeId,
        call: PeerCall,
    ) -> impl Future<Output = Result<Bytes, TransportError>> + Send {
        let applied = self.applied.clone();
        let rt = self.rt.clone();
        let store = self.store.clone();
        async move {
            if let Some(rt) = rt {
                let file = rt
                    .disk()
                    .open("chaos.log", OpenOptions::create())
                    .await
                    .map_err(|e| TransportError::Remote(e.to_string()))?;
                // Both of these can fail under a hostile disk, and the point
                // is that the run stays reproducible either way.
                let _ = file.append(call.payload.clone()).await;
                let _ = file.sync().await;
            }
            if let Some(store) = store {
                // A read then a conditional write, which is the shape of a
                // manifest swap and the shape a lost response is worst for.
                let held = store.get("chaos/current").await.ok();
                let precondition = match &held {
                    Some((_, etag)) => Precondition::Match(etag.clone()),
                    None => Precondition::NotExists,
                };
                let _ = store
                    .put_if("chaos/current", call.payload, precondition)
                    .await;
            }
            applied.fetch_add(1, Ordering::Relaxed);
            Ok(Bytes::new())
        }
    }
}

/// Three nodes gossiping at each other through a network that is trying to
/// stop them.
fn chaotic_run(config: SimConfig) -> (String, u64) {
    let sim = Simulation::with_config(config);
    let applied = Arc::new(AtomicU64::new(0));
    // One bucket for all three, because that is what a bucket is, and because
    // three writers racing one key is where a store fault does the most
    // damage.
    let bucket = sim.bucket("chaos");

    for id in 1..=3u64 {
        let rt = sim.add_node(NodeId(id));
        rt.transport().register(
            ServiceId::Wal,
            Logger {
                rt: Some(rt.clone()),
                store: Some(bucket.store()),
                applied: applied.clone(),
            },
        );
    }

    for id in 1..=3u64 {
        let rt = sim.runtime(NodeId(id));
        rt.clone().spawn(async move {
            for round in 0..8u64 {
                rt.clock().sleep(Duration::from_millis(1)).await;
                for peer in 1..=3u64 {
                    if peer == id {
                        continue;
                    }
                    let _ = rt
                        .transport()
                        .call(
                            NodeId(peer),
                            PeerCall {
                                service: ServiceId::Wal,
                                method: 1,
                                payload: Bytes::from(format!("{id}:{round}")),
                            },
                        )
                        .await;
                }
            }
        });
    }

    // Partition one node off in one direction partway through, then heal it,
    // so the run covers the asymmetric case as well as the random faults.
    sim.run_for(Duration::from_millis(3));
    sim.partition_one_way(NodeId(3), NodeId(1));
    sim.run_for(Duration::from_millis(3));
    sim.heal_all();
    sim.run_until_idle();

    (sim.trace().to_string(), sim.faults_injected())
}

#[test]
fn determinism_holds_with_every_fault_turned_on() {
    for seed in [1, 7, 64, 999] {
        let (first, first_faults) = chaotic_run(SimConfig::chaotic(seed));
        let (second, second_faults) = chaotic_run(SimConfig::chaotic(seed));
        assert_eq!(
            first, second,
            "seed {seed} diverged once faults were involved"
        );
        assert_eq!(first_faults, second_faults);
        assert!(
            first_faults > 0,
            "seed {seed} injected nothing, so this proves nothing"
        );
        // Named specifically, because the object store draws from the same
        // stream as everything else and is the newest thing in it. A seed
        // that faulted only the disk would leave that untested and still pass
        // the check above.
        assert!(
            first.contains("store fault"),
            "seed {seed} never faulted the object store"
        );
    }
}

#[test]
fn the_fault_budget_bounds_how_much_can_go_wrong() {
    let mut config = SimConfig::chaotic(1);
    config.fault_budget = 5;
    let (_, injected) = chaotic_run(config);

    assert!(
        injected <= 5,
        "the budget is what stops a run from spending all of its time recovering, and it \
         let {injected} faults through"
    );
}

#[test]
fn a_budget_of_zero_leaves_a_run_untouched() {
    let mut config = SimConfig::chaotic(1);
    config.fault_budget = 0;
    let (_, injected) = chaotic_run(config);
    assert_eq!(injected, 0);
}

#[test]
fn faults_hold_off_until_the_warmup_has_passed() {
    let mut config = SimConfig::new(1);
    config.network.drop_permille = 1000;
    config.fault_warmup = Duration::from_millis(50);

    let sim = Simulation::with_config(config);
    let a = sim.add_node(NodeId(1));
    let b = sim.add_node(NodeId(2));
    b.transport().register(ServiceId::Wal, Logger::default());

    // A cluster needs a moment to form before the world starts breaking, and
    // this is the knob that gives it one.
    let transport = a.transport().clone();
    let early = sim.block_on(async move {
        transport
            .call(
                NodeId(2),
                PeerCall {
                    service: ServiceId::Raft,
                    method: 1,
                    payload: Bytes::new(),
                },
            )
            .await
    });
    assert_eq!(
        early,
        Err(TransportError::NoHandler(ServiceId::Raft)),
        "the request reached the peer, so nothing was dropped during warmup"
    );
    assert_eq!(sim.faults_injected(), 0);
}
