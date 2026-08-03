//! Crashing, restarting, and what does and does not come back.

use orbita_core::NodeId;
use orbita_runtime::{Clock, Runtime};
use orbita_sim::{DiskPolicy, Simulation};

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// A node ticking once a millisecond, which is the shape of every heartbeat
/// loop in the system.
fn heartbeat(sim: &Simulation, node: NodeId) -> Arc<AtomicU64> {
    let rt = sim.runtime(node);
    let ticks = Arc::new(AtomicU64::new(0));
    let counter = ticks.clone();
    rt.clone().spawn(async move {
        loop {
            rt.clock().sleep(Duration::from_millis(1)).await;
            counter.fetch_add(1, Ordering::Relaxed);
        }
    });
    ticks
}

#[test]
fn a_crash_stops_the_work_a_node_had_in_flight() {
    let sim = Simulation::new(1);
    sim.add_node(NodeId(1));
    let ticks = heartbeat(&sim, NodeId(1));

    sim.run_for(Duration::from_millis(10));
    let before = ticks.load(Ordering::Relaxed);
    assert!(before > 0, "the heartbeat never started");

    sim.crash(NodeId(1));
    sim.run_for(Duration::from_millis(10));

    assert_eq!(
        ticks.load(Ordering::Relaxed),
        before,
        "a crashed node kept running, so a crash is not abrupt enough to be one"
    );
}

#[test]
fn a_crash_with_nothing_else_running_leaves_the_world_idle() {
    let sim = Simulation::new(1);
    sim.add_node(NodeId(1));
    heartbeat(&sim, NodeId(1));

    sim.run_for(Duration::from_millis(5));
    sim.crash(NodeId(1));

    // Nothing is runnable and no timer belongs to anyone, so the driver has to
    // report that rather than spinning.
    assert!(
        !sim.step(),
        "a timer outlived the task that was waiting on it"
    );
}

#[test]
fn a_restarted_node_runs_again_once_its_work_is_respawned() {
    let sim = Simulation::new(1);
    sim.add_node(NodeId(1));
    let first = heartbeat(&sim, NodeId(1));
    sim.run_for(Duration::from_millis(5));
    sim.crash(NodeId(1));

    // A restarted process starts its background work from scratch, and this
    // reflects that rather than pretending the old task resumes.
    sim.restart(NodeId(1), DiskPolicy::Intact);
    let second = heartbeat(&sim, NodeId(1));
    sim.run_for(Duration::from_millis(5));

    assert!(second.load(Ordering::Relaxed) > 0);
    assert!(
        first.load(Ordering::Relaxed) > 0,
        "the pre-crash heartbeat should still show what it managed before dying"
    );
}

#[test]
fn a_node_is_down_between_crash_and_restart() {
    let sim = Simulation::new(1);
    sim.add_node(NodeId(1));
    assert!(sim.is_up(NodeId(1)));

    sim.crash(NodeId(1));
    assert!(!sim.is_up(NodeId(1)));

    sim.restart(NodeId(1), DiskPolicy::Intact);
    assert!(sim.is_up(NodeId(1)));
}

#[test]
fn crashing_one_node_leaves_the_others_running() {
    let sim = Simulation::new(1);
    sim.add_node(NodeId(1));
    sim.add_node(NodeId(2));
    let one = heartbeat(&sim, NodeId(1));
    let two = heartbeat(&sim, NodeId(2));

    sim.run_for(Duration::from_millis(5));
    sim.crash(NodeId(1));
    let one_at_crash = one.load(Ordering::Relaxed);
    let two_at_crash = two.load(Ordering::Relaxed);
    sim.run_for(Duration::from_millis(5));

    assert_eq!(one.load(Ordering::Relaxed), one_at_crash);
    assert!(two.load(Ordering::Relaxed) > two_at_crash);
}
