//! What the simulated network does to a peer call.

use orbita_core::NodeId;
use orbita_runtime::{PeerCall, PeerHandler, Runtime, ServiceId, Transport, TransportError};

use bytes::Bytes;
use orbita_sim::{SimConfig, Simulation};

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Echoes the payload and remembers that it ran, so a test can tell the
/// difference between a request that never arrived and a reply that never came
/// back. That distinction is the whole reason asymmetric partitions matter.
#[derive(Clone, Default)]
struct Echo {
    calls: Arc<AtomicU64>,
    arrivals: Arc<Mutex<Vec<u16>>>,
}

impl PeerHandler for Echo {
    fn handle(
        &self,
        _from: NodeId,
        call: PeerCall,
    ) -> impl Future<Output = Result<Bytes, TransportError>> + Send {
        let calls = self.calls.clone();
        let arrivals = self.arrivals.clone();
        async move {
            calls.fetch_add(1, Ordering::Relaxed);
            arrivals.lock().unwrap().push(call.method);
            Ok(call.payload)
        }
    }
}

fn ping(method: u16) -> PeerCall {
    PeerCall {
        service: ServiceId::Wal,
        method,
        payload: Bytes::from_static(b"ping"),
    }
}

/// Two nodes, the second answering WAL calls.
fn pair(config: SimConfig) -> (Simulation, Echo) {
    let sim = Simulation::with_config(config);
    let a = sim.add_node(NodeId(1));
    let b = sim.add_node(NodeId(2));
    let echo = Echo::default();
    b.transport().register(ServiceId::Wal, echo.clone());
    drop(a);
    (sim, echo)
}

#[test]
fn a_call_reaches_the_peer_and_the_reply_comes_back() {
    let (sim, echo) = pair(SimConfig::new(1));
    let transport = sim.runtime(NodeId(1)).transport().clone();

    let reply = sim.block_on(async move { transport.call(NodeId(2), ping(1)).await });

    assert_eq!(reply.unwrap(), Bytes::from_static(b"ping"));
    assert_eq!(echo.calls.load(Ordering::Relaxed), 1);
}

#[test]
fn a_call_takes_time_to_arrive() {
    let (sim, _) = pair(SimConfig::new(1));
    let transport = sim.runtime(NodeId(1)).transport().clone();

    sim.block_on(async move { transport.call(NodeId(2), ping(1)).await.unwrap() });

    assert!(
        sim.now_nanos() > 0,
        "a peer call that costs no virtual time hides every latency-dependent bug"
    );
}

#[test]
fn a_call_to_a_node_that_does_not_exist_fails_immediately() {
    let (sim, _) = pair(SimConfig::new(1));
    let transport = sim.runtime(NodeId(1)).transport().clone();

    let reply = sim.block_on(async move { transport.call(NodeId(99), ping(1)).await });

    assert_eq!(reply, Err(TransportError::UnknownPeer(NodeId(99))));
    assert_eq!(
        sim.now_nanos(),
        0,
        "an unknown peer must not cost a timeout"
    );
}

#[test]
fn a_call_for_an_unregistered_service_reports_no_handler() {
    let (sim, _) = pair(SimConfig::new(1));
    let transport = sim.runtime(NodeId(1)).transport().clone();

    let reply = sim.block_on(async move {
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

    assert_eq!(reply, Err(TransportError::NoHandler(ServiceId::Raft)));
}

#[test]
fn a_symmetric_partition_stops_traffic_in_both_directions() {
    let (sim, echo) = pair(SimConfig::new(1));
    sim.partition(NodeId(1), NodeId(2));
    let transport = sim.runtime(NodeId(1)).transport().clone();

    let reply = sim.block_on(async move { transport.call(NodeId(2), ping(1)).await });

    assert_eq!(reply, Err(TransportError::Timeout(NodeId(2))));
    assert_eq!(
        echo.calls.load(Ordering::Relaxed),
        0,
        "the request should never have arrived"
    );
}

#[test]
fn an_asymmetric_partition_applies_the_request_and_loses_the_reply() {
    // The case the brief singles out: the peer commits the write and the
    // caller times out. A system that treats a timeout as "it did not happen"
    // is wrong here, and no symmetric partition would ever show it.
    let (sim, echo) = pair(SimConfig::new(1));
    sim.partition_one_way(NodeId(2), NodeId(1));
    let transport = sim.runtime(NodeId(1)).transport().clone();

    let reply = sim.block_on(async move { transport.call(NodeId(2), ping(1)).await });

    assert_eq!(reply, Err(TransportError::Timeout(NodeId(2))));
    assert_eq!(
        echo.calls.load(Ordering::Relaxed),
        1,
        "the request must have been applied even though the caller heard nothing"
    );
}

#[test]
fn healing_a_partition_lets_traffic_through_again() {
    let (sim, echo) = pair(SimConfig::new(1));
    sim.partition(NodeId(1), NodeId(2));
    let transport = sim.runtime(NodeId(1)).transport().clone();
    let first = transport.clone();
    assert!(sim
        .block_on(async move { first.call(NodeId(2), ping(1)).await })
        .is_err());

    sim.heal(NodeId(1), NodeId(2));
    let reply = sim.block_on(async move { transport.call(NodeId(2), ping(2)).await });

    assert!(reply.is_ok());
    assert_eq!(echo.calls.load(Ordering::Relaxed), 1);
}

#[test]
fn a_dropped_message_becomes_a_timeout() {
    let mut config = SimConfig::new(1);
    config.network.drop_permille = 1000;
    let (sim, echo) = pair(config);
    let transport = sim.runtime(NodeId(1)).transport().clone();

    let reply = sim.block_on(async move { transport.call(NodeId(2), ping(1)).await });

    assert_eq!(reply, Err(TransportError::Timeout(NodeId(2))));
    assert_eq!(echo.calls.load(Ordering::Relaxed), 0);
}

#[test]
fn a_duplicated_message_runs_the_handler_twice_and_answers_once() {
    let mut config = SimConfig::new(1);
    config.network.duplicate_permille = 1000;
    let (sim, echo) = pair(config);
    let transport = sim.runtime(NodeId(1)).transport().clone();

    let reply = sim.block_on(async move { transport.call(NodeId(2), ping(1)).await });
    // The second copy is still in flight when the caller returns.
    sim.run_until_idle();

    assert!(reply.is_ok(), "the caller sees one answer");
    assert_eq!(
        echo.calls.load(Ordering::Relaxed),
        2,
        "a duplicate that the handler never sees is not a duplicate"
    );
}

#[test]
fn messages_can_arrive_in_a_different_order_than_they_were_sent() {
    let mut reordered = false;
    for seed in 1..=30 {
        let mut config = SimConfig::new(seed);
        config.network.slow_permille = 400;
        let (sim, echo) = pair(config);

        for method in 1..=4u16 {
            let transport = sim.runtime(NodeId(1)).transport().clone();
            sim.spawn(async move {
                let _ = transport.call(NodeId(2), ping(method)).await;
            });
        }
        sim.run_until_idle();

        let arrivals = echo.arrivals.lock().unwrap().clone();
        assert_eq!(arrivals.len(), 4);
        if arrivals != vec![1, 2, 3, 4] {
            reordered = true;
            break;
        }
    }
    assert!(
        reordered,
        "no seed reordered anything, so the network is delivering in send order"
    );
}

#[test]
fn a_call_to_a_crashed_node_is_refused_rather_than_answered() {
    let (sim, _) = pair(SimConfig::new(1));
    sim.crash(NodeId(2));
    let transport = sim.runtime(NodeId(1)).transport().clone();

    let reply = sim.block_on(async move { transport.call(NodeId(2), ping(1)).await });

    assert_eq!(reply, Err(TransportError::Unreachable(NodeId(2))));
}

#[test]
fn a_restarted_node_answers_again_once_it_registers() {
    let (sim, _) = pair(SimConfig::new(1));
    sim.crash(NodeId(2));
    let back = sim.restart(NodeId(2), orbita_sim::DiskPolicy::Intact);

    let transport = sim.runtime(NodeId(1)).transport().clone();
    let first = transport.clone();
    assert_eq!(
        sim.block_on(async move { first.call(NodeId(2), ping(1)).await }),
        Err(TransportError::NoHandler(ServiceId::Wal)),
        "a restarted process has not registered its handlers yet"
    );

    back.transport().register(ServiceId::Wal, Echo::default());
    assert!(sim
        .block_on(async move { transport.call(NodeId(2), ping(1)).await })
        .is_ok());
}
