//! The properties everything else in the crate depends on: one seed, one
//! interleaving, and a clock that only moves when nothing can run.

use orbita_core::NodeId;
use orbita_runtime::{Clock, Runtime};
use orbita_sim::Simulation;

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// A workload with enough concurrency that the order tasks finish in is a
/// scheduling decision rather than a foregone conclusion.
fn interleaving_for(seed: u64) -> (Vec<u64>, String) {
    let sim = Simulation::new(seed);
    let log: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));

    for id in 1..=4u64 {
        let node = sim.add_node(NodeId(id));
        let log = log.clone();
        node.clone().spawn(async move {
            for round in 0..5u64 {
                node.clock().sleep(Duration::from_millis(id % 3 + 1)).await;
                log.lock().unwrap().push(id * 100 + round);
                node.yield_now().await;
            }
        });
    }

    sim.run_until_idle();
    let order = log.lock().unwrap().clone();
    (order, sim.trace().to_string())
}

#[test]
fn the_same_seed_produces_the_same_interleaving() {
    for seed in [1, 2, 3, 99, 12345] {
        let (first_order, first_trace) = interleaving_for(seed);
        let (second_order, second_trace) = interleaving_for(seed);
        assert_eq!(
            first_order, second_order,
            "seed {seed} produced two different orderings"
        );
        assert_eq!(
            first_trace, second_trace,
            "seed {seed} produced two different traces"
        );
    }
}

#[test]
fn different_seeds_explore_different_interleavings() {
    let traces: Vec<String> = (1..=8).map(|seed| interleaving_for(seed).1).collect();
    let distinct: std::collections::HashSet<&String> = traces.iter().collect();
    assert!(
        distinct.len() > 1,
        "every seed produced the same trace, so the seed is not reaching the scheduler"
    );
}

#[test]
fn sleeping_advances_the_clock_instead_of_waiting() {
    let sim = Simulation::new(1);
    let node = sim.add_node(NodeId(1));
    let clock = node.clock().clone();

    let started = std::time::Instant::now();
    sim.block_on(async move {
        clock.sleep(Duration::from_secs(3600)).await;
    });

    assert_eq!(sim.now_nanos(), 3600 * 1_000_000_000);
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "an hour of virtual time must not cost an hour of real time"
    );
}

#[test]
fn time_only_moves_when_every_task_is_blocked() {
    let sim = Simulation::new(1);
    let node = sim.add_node(NodeId(1));
    let observed: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));

    // Two tasks that only yield. Nothing here waits on time, so the clock must
    // still read zero when they are done.
    for _ in 0..2 {
        let node = node.clone();
        let observed = observed.clone();
        node.clone().spawn(async move {
            for _ in 0..10 {
                observed
                    .lock()
                    .unwrap()
                    .push(node.clock().monotonic_nanos());
                node.yield_now().await;
            }
        });
    }

    sim.run_until_idle();
    assert!(
        observed.lock().unwrap().iter().all(|&t| t == 0),
        "time moved while a task was still runnable"
    );
}

#[test]
fn wall_time_and_monotonic_time_advance_together_from_a_fixed_origin() {
    let sim = Simulation::new(1);
    let node = sim.add_node(NodeId(1));
    let clock = node.clock().clone();

    let origin = clock.now_millis();
    assert_eq!(
        origin,
        sim.config().wall_clock_origin_millis,
        "a run that embeds the real date is not reproducible"
    );

    let after = sim.block_on(async move {
        clock.sleep(Duration::from_millis(250)).await;
        clock.now_millis()
    });
    assert_eq!(after, origin + 250);
}

/// A future carried out of the task that first polled it.
///
/// A wrapper rather than a bare pinned box because handing an awaitable out of
/// an `async` block is normally a mistake, and here it is the whole point.
struct Handover(std::pin::Pin<Box<dyn Future<Output = ()> + Send>>);

#[test]
fn a_pending_future_polled_by_one_task_is_woken_by_the_task_it_moves_to() {
    // The shape this protects is `orbita_wal`'s: an owner polls a call to each
    // replica, stops as soon as the quorum answers, and hands the calls still
    // in flight to background tasks so a lagging replica is not abandoned. The
    // moved future was already polled once, so the timer behind it was
    // registered against a task that has since finished.
    //
    // The simulator used to keep that first waker. The timer fired, the wake
    // went to a task that no longer existed, and the moved future was never
    // polled again, which surfaced as the world going idle with a write still
    // outstanding. Polling with a fresh waker is something a future has to
    // tolerate, so the fix is here rather than in the caller.
    let sim = Simulation::new(1);
    let node = sim.add_node(NodeId(1));
    let clock = node.clock().clone();

    let handed_over = sim.block_on(async move {
        let mut sleeping: std::pin::Pin<Box<dyn Future<Output = ()> + Send>> =
            Box::pin(async move { clock.sleep(Duration::from_millis(50)).await });
        let first =
            std::future::poll_fn(|cx| std::task::Poll::Ready(sleeping.as_mut().poll(cx))).await;
        assert!(
            first.is_pending(),
            "the future has to still be waiting for this to prove anything"
        );
        Handover(sleeping)
    });

    let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
    {
        let finished = Arc::clone(&finished);
        sim.spawn(async move {
            handed_over.0.await;
            finished.store(true, std::sync::atomic::Ordering::SeqCst);
        });
    }
    sim.run_until_idle();

    assert!(
        finished.load(std::sync::atomic::Ordering::SeqCst),
        "the moved future was never woken, so the world went idle with work outstanding"
    );
}

#[test]
fn a_spawned_task_runs_without_being_awaited() {
    let sim = Simulation::new(4);
    let node = sim.add_node(NodeId(1));
    let ran = Arc::new(Mutex::new(false));

    let flag = ran.clone();
    node.spawn(async move {
        *flag.lock().unwrap() = true;
    });

    sim.run_until_idle();
    assert!(*ran.lock().unwrap());
}
