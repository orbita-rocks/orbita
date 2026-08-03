//! The promise that makes a simulation failure worth reporting: the seed that
//! failed in CI fails the same way on a laptop, from one command.

use orbita_core::NodeId;
use orbita_runtime::{Clock, Runtime};
use orbita_sim::{check_seeds, Failure, Simulation};

use std::sync::{Arc, Mutex};
use std::time::Duration;

/// A scenario that fails only on one seed, standing in for a real bug that
/// only a particular interleaving reaches.
fn scenario(seed: u64) -> Result<(), Failure> {
    let sim = Simulation::new(seed);
    let order: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));

    for id in 1..=3u64 {
        let node = sim.add_node(NodeId(id));
        let order = order.clone();
        node.clone().spawn(async move {
            node.clock().sleep(Duration::from_micros(id * 10)).await;
            order.lock().unwrap().push(id);
            node.yield_now().await;
            order.lock().unwrap().push(id * 10);
        });
    }
    sim.run_until_idle();

    let observed = order.lock().unwrap().clone();
    if seed == 3 {
        return Err(sim.failure(format!("the deliberate failure, order was {observed:?}")));
    }
    Ok(())
}

#[test]
fn a_failing_seed_is_reported_with_the_command_that_replays_it() {
    let panicked = std::panic::catch_unwind(|| {
        check_seeds(
            "a_failing_seed_is_reported_with_the_command_that_replays_it",
            5,
            scenario,
        );
    })
    .expect_err("the scenario fails on seed 3, so the harness must fail too");

    let message = panicked
        .downcast_ref::<String>()
        .expect("the harness reports a formatted message");

    assert!(message.contains("seed 3 failed"), "{message}");
    assert!(message.contains("ORBITA_SIM_SEED=3"), "{message}");
    assert!(
        message.contains("cargo test -p orbita-sim"),
        "the report has to be a command someone can paste: {message}"
    );
    assert!(
        message.contains("poll task="),
        "the report has to carry the tail of the trace: {message}"
    );
}

#[test]
fn a_seed_that_passed_still_passes_when_it_is_run_alone() {
    for seed in [1, 2, 4, 5] {
        assert!(
            scenario(seed).is_ok(),
            "seed {seed} is not stable in isolation"
        );
    }
}

#[test]
fn replaying_a_failing_seed_reproduces_the_same_trace() {
    let first = scenario(3).unwrap_err();
    let second = scenario(3).unwrap_err();

    assert_eq!(first.reason, second.reason);
    assert_eq!(
        first.trace.to_string(),
        second.trace.to_string(),
        "a failure that does not replay byte for byte is a bug in the simulator itself"
    );
    assert_eq!(first.trace.dropped(), 0, "the trace was truncated");
}
