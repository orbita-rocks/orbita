//! The driver: the object a test holds, and the only thing that makes virtual
//! time move.
//!
//! Nothing in this crate runs on its own. A test spawns work, then asks the
//! `Simulation` to step, and every step is a decision recorded in the trace.
//! That is what separates a simulator from a test that merely uses fake time:
//! there is no ambient concurrency to be surprised by.

use crate::config::SimConfig;
use crate::harness::Failure;
use crate::objectstore::SimBucket;
use crate::runtime::SimRuntime;
use crate::trace::Trace;
use crate::world::{BucketState, SimCore};

use orbita_core::NodeId;
use orbita_runtime::Rng;

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// What a restarting node finds where its data used to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskPolicy {
    /// The machine came back with its disk. Everything durable at the moment
    /// of the crash is still there, and everything that was only in the page
    /// cache is not.
    Intact,
    /// The machine was replaced. This is the common cloud case and it is the
    /// one that finds bugs in recovery paths that quietly assume local state
    /// survives.
    Lost,
}

/// One simulated world, driven by one thread.
pub struct Simulation {
    core: Arc<SimCore>,
}

impl Simulation {
    /// A world with real latencies and no faults.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self::with_config(SimConfig::new(seed))
    }

    #[must_use]
    pub fn with_config(config: SimConfig) -> Self {
        Self {
            core: SimCore::new(config),
        }
    }

    #[must_use]
    pub fn seed(&self) -> u64 {
        self.core.config.seed
    }

    #[must_use]
    pub fn config(&self) -> &SimConfig {
        &self.core.config
    }

    /// Brings a node into existence and hands back its runtime.
    ///
    /// The runtime can be dropped and asked for again later with `runtime`,
    /// since the node lives in the world rather than in the handle.
    pub fn add_node(&self, node: NodeId) -> SimRuntime {
        {
            let mut state = self.core.state();
            let entry = state.nodes.entry(node).or_default();
            entry.up = true;
            state.record(format!("node {node} started"));
        }
        SimRuntime::new(self.core.clone(), node)
    }

    /// A fresh handle to a node that already exists.
    #[must_use]
    pub fn runtime(&self, node: NodeId) -> SimRuntime {
        SimRuntime::new(self.core.clone(), node)
    }

    #[must_use]
    pub fn is_up(&self, node: NodeId) -> bool {
        self.core.state().is_up(node)
    }

    /// Kills a node where it stands.
    ///
    /// In-flight tasks are dropped rather than unwound, handlers stop
    /// answering, and everything written but not durable is lost. If torn
    /// tails are enabled the last record may survive in pieces, which is what
    /// a real crash mid-append leaves behind.
    pub fn crash(&self, node: NodeId) {
        self.core.crash_node(node);
    }

    /// A bucket in the simulated object store, addressed by name.
    ///
    /// Two calls with one name hand back handles onto the same objects,
    /// because a bucket is shared infrastructure and the interesting failures
    /// are the ones where two writers reach it at once. Nothing here dials
    /// anything: the handle is an [`orbita_objectstore::s3::HttpTransport`],
    /// so the S3 store under test is the same code production runs.
    pub fn bucket(&self, name: &str) -> Arc<SimBucket> {
        {
            let mut state = self.core.state();
            if !state.buckets.contains_key(name) {
                state
                    .buckets
                    .insert(name.to_string(), BucketState::default());
                state.record(format!("bucket {name} created"));
            }
        }
        Arc::new(SimBucket::new(self.core.clone(), name.to_string()))
    }

    /// Brings a node back, with or without its data.
    ///
    /// The caller gets a fresh runtime and is expected to re-register handlers
    /// and re-spawn background work, because a restarted process does exactly
    /// that.
    pub fn restart(&self, node: NodeId, disk: DiskPolicy) -> SimRuntime {
        {
            let mut state = self.core.state();
            let entry = state.nodes.entry(node).or_default();
            entry.up = true;
            if disk == DiskPolicy::Lost {
                entry.files.clear();
                entry.disk_generation += 1;
            }
            state.record(format!("node {node} restarted with disk {disk:?}"));
        }
        SimRuntime::new(self.core.clone(), node)
    }

    /// Blocks traffic in one direction only.
    ///
    /// This is the primitive because it is the interesting case: `a` can send
    /// to `b` and hear nothing back, so `a` believes `b` is dead while `b`
    /// sees a live peer issuing commands.
    pub fn partition_one_way(&self, from: NodeId, to: NodeId) {
        let mut state = self.core.state();
        state.last_fault_nanos = Some(state.now);
        state.blocked.insert((from, to));
        state.record(format!("link {from}->{to} down"));
    }

    /// Blocks traffic in both directions.
    pub fn partition(&self, a: NodeId, b: NodeId) {
        self.partition_one_way(a, b);
        self.partition_one_way(b, a);
    }

    pub fn heal_one_way(&self, from: NodeId, to: NodeId) {
        let mut state = self.core.state();
        if state.blocked.remove(&(from, to)) {
            state.record(format!("link {from}->{to} up"));
        }
    }

    pub fn heal(&self, a: NodeId, b: NodeId) {
        self.heal_one_way(a, b);
        self.heal_one_way(b, a);
    }

    /// Restores every link. Used after a partition test has made its point.
    pub fn heal_all(&self) {
        let mut state = self.core.state();
        state.blocked.clear();
        state.record("all links up");
    }

    /// Stops the world from breaking any further.
    ///
    /// Distinct from exhausting the budget: a run that spent its budget still
    /// had faults on offer, whereas this says the scenario is finished
    /// injecting them. Everything after this point is recovery, which is the
    /// only window in which a liveness claim can be made at all. A cluster
    /// still being torn at is under no obligation to have finished anything.
    ///
    /// A crashed node stays crashed. Convergence has to hold with the
    /// survivors it actually has, not with the ones it wishes it had.
    pub fn stop_injecting_faults(&self) {
        let mut state = self.core.state();
        if !state.faults_frozen {
            state.faults_frozen = true;
            state.record("fault injection stopped");
        }
    }

    /// When the last fault was injected, in virtual nanoseconds.
    ///
    /// `None` for a run in which nothing ever went wrong, which is worth being
    /// able to say out loud: a convergence check that passes on a run with no
    /// faults in it has not proved much.
    #[must_use]
    pub fn last_fault_nanos(&self) -> Option<u64> {
        self.core.state().last_fault_nanos
    }

    #[must_use]
    pub fn now_nanos(&self) -> u64 {
        self.core.now()
    }

    /// How much of the fault budget the run has spent. A scenario that finds
    /// nothing and turns out to have injected nothing was not testing what its
    /// author thought it was.
    #[must_use]
    pub fn faults_injected(&self) -> u64 {
        self.core.state().faults_used
    }

    /// A draw from the application stream, for a scenario that needs to decide
    /// when to crash something. Using this rather than a private generator
    /// keeps the scenario reproducible from the seed.
    pub fn random_below(&self, n: u64) -> u64 {
        self.core.app_rng().below(n)
    }

    /// Spawns work that belongs to the test rather than to any node, so a
    /// crash does not take it with it.
    pub fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.core.spawn_task(None, Box::pin(future));
    }

    /// Runs one scheduling decision. Returns false when nothing is runnable
    /// and no timer is pending.
    pub fn step(&self) -> bool {
        self.core.step()
    }

    /// Runs until the world has nothing left to do.
    pub fn run_until_idle(&self) {
        while self.core.step() {
            self.guard_progress();
        }
    }

    /// Runs until at least `duration` of virtual time has passed, or until the
    /// world goes idle.
    pub fn run_for(&self, duration: Duration) {
        let deadline = self.core.now() + duration.as_nanos() as u64;
        while self.core.now() < deadline {
            if !self.core.step() {
                return;
            }
            self.guard_progress();
        }
    }

    /// Drives a future to completion and returns its output.
    ///
    /// Panics if the world goes idle first, because a test that awaits
    /// something nothing will ever complete is a bug in the test or in the
    /// system, and hanging is a worse way to learn that than failing.
    pub fn block_on<F, T>(&self, future: F) -> T
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let result: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
        let sink = result.clone();
        self.spawn(async move {
            let value = future.await;
            *sink.lock().expect("result lock poisoned") = Some(value);
        });
        loop {
            if let Some(value) = result.lock().expect("result lock poisoned").take() {
                return value;
            }
            if !self.core.step() {
                panic!(
                    "seed {}: the simulation went idle with the future still pending.\n{}",
                    self.seed(),
                    self.trace().tail(40)
                );
            }
            self.guard_progress();
        }
    }

    /// A snapshot of everything recorded so far. Two runs of one seed must
    /// produce equal traces, and that assertion is the crate's own test.
    #[must_use]
    pub fn trace(&self) -> Trace {
        self.core.state().trace.clone()
    }

    /// Packages a failure with the trace that produced it, ready for the seed
    /// harness to report.
    #[must_use]
    pub fn failure(&self, reason: impl Into<String>) -> Failure {
        Failure {
            seed: self.seed(),
            reason: reason.into(),
            trace: self.trace(),
        }
    }

    fn guard_progress(&self) {
        let state = self.core.state();
        assert!(
            state.steps <= self.core.config.max_steps,
            "seed {}: the simulation ran {} steps without finishing, which means it is stuck",
            self.core.config.seed,
            state.steps
        );
    }
}

impl Drop for Simulation {
    fn drop(&mut self) {
        // Tasks hold runtimes, runtimes hold the world, and the world holds
        // tasks. Breaking that cycle by hand is the price of a scheduler that
        // owns its futures.
        self.core.clear();
    }
}
