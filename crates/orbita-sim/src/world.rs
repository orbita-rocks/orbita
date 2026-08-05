//! The single piece of shared state every simulated node reaches into.
//!
//! Everything that could differ between two runs lives behind one mutex: the
//! clock, the ready queue, the timer wheel, each node's files, and the
//! network. Concentrating it here is what makes the ordering auditable. If
//! scheduling state were spread across the disk and the network modules, a
//! determinism bug would be a search problem rather than a reading problem.
//!
//! The mutex is not there for concurrency. Exactly one thread ever drives a
//! simulation. It is there because `orbita_runtime::Runtime` requires `Send +
//! Sync`, which rules out `Rc<RefCell<_>>`, and because `unsafe` is forbidden
//! there is no third option. Nothing ever contends on it.

use crate::config::SimConfig;
use crate::trace::Trace;

use orbita_core::NodeId;
use orbita_runtime::{PeerCall, Rng, SeededRng, TransportError};

/// See the note on the disk alias: the contract crate does not re-export this
/// one either.
pub(crate) type TransportResult<T> = Result<T, TransportError>;

use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::task::{Wake, Waker};

pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct TaskId(pub u64);

/// An inbound peer handler, erased so the network can store handlers for
/// services it knows nothing about.
///
/// `orbita_runtime::PeerHandler` returns `impl Future`, which is not
/// dyn-compatible, so the simulator boxes the future at this boundary. That
/// cost is paid once per delivered message and only under simulation.
pub(crate) trait DynHandler: Send + Sync + 'static {
    fn handle_dyn(&self, from: NodeId, call: PeerCall) -> BoxFuture<'_, TransportResult<Bytes>>;
}

impl<H: orbita_runtime::PeerHandler> DynHandler for H {
    fn handle_dyn(&self, from: NodeId, call: PeerCall) -> BoxFuture<'_, TransportResult<Bytes>> {
        Box::pin(self.handle(from, call))
    }
}

/// One file as the simulator sees it.
///
/// `visible` is what a reader sees now; `durable` is what would survive a
/// power cut. Keeping them apart is the whole point, because a system that
/// cannot tell the difference is a system that loses acknowledged writes.
#[derive(Debug, Default, Clone)]
pub(crate) struct FileState {
    pub visible: Vec<u8>,
    pub durable: Vec<u8>,
}

#[derive(Debug, Default)]
pub(crate) struct NodeState {
    pub up: bool,
    /// Bumped when a node restarts with a fresh disk, so file handles held
    /// from before the restart fail rather than silently addressing new data.
    pub disk_generation: u64,
    pub files: BTreeMap<String, FileState>,
}

pub(crate) struct TaskSlot {
    /// The node the task belongs to, if any. Crashing a node drops its tasks,
    /// which is what makes a crash abrupt rather than graceful.
    pub node: Option<NodeId>,
    pub future: Option<BoxFuture<'static, ()>>,
}

pub(crate) struct SimState {
    pub now: u64,
    pub steps: u64,
    next_task: u64,
    next_seq: u64,
    pub tasks: BTreeMap<TaskId, TaskSlot>,
    pub ready: BTreeSet<TaskId>,
    /// Keyed by deadline then insertion order, so two timers that expire in
    /// the same nanosecond still fire in a fixed order.
    pub timers: BTreeMap<(u64, u64), Waker>,
    pub nodes: BTreeMap<NodeId, NodeState>,
    /// Directed links that are down. `(a, b)` present means a message from `a`
    /// to `b` is discarded, while `b` to `a` may still flow. Asymmetric
    /// reachability is the case that finds bugs symmetric partitions do not,
    /// so it is the primitive and the symmetric case is built from it.
    pub blocked: BTreeSet<(NodeId, NodeId)>,
    pub handlers: BTreeMap<(NodeId, u16), Arc<dyn DynHandler>>,
    pub trace: Trace,
    pub faults_used: u64,
    /// The virtual instant the last fault was injected, if any. What a
    /// convergence check measures its bound from, since "recovered" is only
    /// meaningful relative to the last thing that broke.
    pub last_fault_nanos: Option<u64>,
    /// Set once a scenario has stopped breaking the world on purpose. Distinct
    /// from an exhausted budget, which is a run that ran out of faults rather
    /// than one that decided it was done.
    pub faults_frozen: bool,
}

impl SimState {
    fn new(config: &SimConfig) -> Self {
        Self {
            now: 0,
            steps: 0,
            next_task: 0,
            next_seq: 0,
            tasks: BTreeMap::new(),
            ready: BTreeSet::new(),
            timers: BTreeMap::new(),
            nodes: BTreeMap::new(),
            blocked: BTreeSet::new(),
            handlers: BTreeMap::new(),
            trace: Trace::new(config.seed, config.trace_limit),
            faults_used: 0,
            last_fault_nanos: None,
            faults_frozen: false,
        }
    }

    pub fn seq(&mut self) -> u64 {
        self.next_seq += 1;
        self.next_seq
    }

    pub fn record(&mut self, event: impl Into<String>) {
        let now = self.now;
        self.trace.record(now, event);
    }

    pub fn is_up(&self, node: NodeId) -> bool {
        self.nodes.get(&node).is_some_and(|n| n.up)
    }

    pub fn link_open(&self, from: NodeId, to: NodeId) -> bool {
        !self.blocked.contains(&(from, to))
    }
}

/// The simulated world, shared by every node in it.
pub(crate) struct SimCore {
    pub config: SimConfig,
    state: Mutex<SimState>,
    /// Three independent streams from one seed, so that turning fault
    /// injection on does not shift the sequence of numbers the application
    /// draws. Without the split, changing a fault rate changes every random
    /// decision downstream and a "narrow the repro" workflow stops working.
    sched_rng: SeededRng,
    fault_rng: SeededRng,
    app_rng: SeededRng,
}

impl SimCore {
    pub fn new(config: SimConfig) -> Arc<Self> {
        let seed = config.seed;
        let state = Mutex::new(SimState::new(&config));
        Arc::new(Self {
            config,
            state,
            sched_rng: SeededRng::new(mix(seed, 0x5348_4544)),
            fault_rng: SeededRng::new(mix(seed, 0x4641_554c)),
            app_rng: SeededRng::new(mix(seed, 0x4150_5000)),
        })
    }

    /// The state lock. Held only across bookkeeping, never across a poll of a
    /// user future, or the world would deadlock on itself.
    pub fn state(&self) -> MutexGuard<'_, SimState> {
        self.state.lock().expect("simulation state lock poisoned")
    }

    pub fn now(&self) -> u64 {
        self.state().now
    }

    pub fn app_rng(&self) -> &SeededRng {
        &self.app_rng
    }

    /// Draws from the environment's stream. Anything that is part of a fault,
    /// including how much of a partial write survived, comes from here so the
    /// application's own draws stay put when fault rates change.
    pub fn fault_below(&self, n: u64) -> u64 {
        self.fault_rng.below(n)
    }

    /// Draws a fault decision against the budget.
    ///
    /// Every fault site goes through here so that the budget and the warm-up
    /// are enforced in one place rather than remembered at each call site.
    pub fn roll_fault(&self, state: &mut SimState, permille: u64) -> bool {
        if permille == 0 || state.faults_frozen {
            return false;
        }
        if state.now < self.config.fault_warmup.as_nanos() as u64 {
            return false;
        }
        if state.faults_used >= self.config.fault_budget {
            return false;
        }
        if self.fault_rng.chance(permille, 1000) {
            state.faults_used += 1;
            state.last_fault_nanos = Some(state.now);
            true
        } else {
            false
        }
    }

    /// A latency in `[min, max]`, drawn from the fault stream because it is
    /// part of the environment rather than part of the application.
    pub fn latency(&self, min: std::time::Duration, max: std::time::Duration) -> u64 {
        let min = min.as_nanos() as u64;
        let max = max.as_nanos() as u64;
        if max <= min {
            min
        } else {
            min + self.fault_rng.below(max - min + 1)
        }
    }

    pub fn spawn_task(
        self: &Arc<Self>,
        node: Option<NodeId>,
        future: BoxFuture<'static, ()>,
    ) -> TaskId {
        let mut state = self.state();
        state.next_task += 1;
        let id = TaskId(state.next_task);
        state.tasks.insert(
            id,
            TaskSlot {
                node,
                future: Some(future),
            },
        );
        state.ready.insert(id);
        match node {
            Some(n) => state.record(format!("spawn task={} node={n}", id.0)),
            None => state.record(format!("spawn task={} node=-", id.0)),
        }
        id
    }

    /// Runs one scheduling step. Returns false when nothing is left to do.
    ///
    /// The order in which runnable tasks are polled is drawn from the seed, so
    /// the same seed produces the same interleaving on every machine, and a
    /// different seed explores a different one. Time only moves when nothing
    /// is runnable, which is what makes an hour of lease expiry cost a
    /// microsecond of real time.
    pub fn step(self: &Arc<Self>) -> bool {
        let picked = {
            let mut state = self.state();
            state.steps += 1;
            if state.ready.is_empty() {
                match self.advance_time(&mut state) {
                    Some(wakers) => {
                        drop(state);
                        for waker in wakers {
                            waker.wake();
                        }
                        return true;
                    }
                    None => return false,
                }
            }
            let index = self.sched_rng.below(state.ready.len() as u64) as usize;
            let id = *state
                .ready
                .iter()
                .nth(index)
                .expect("index drawn from the set's own length");
            state.ready.remove(&id);
            state
                .tasks
                .get_mut(&id)
                .and_then(|slot| slot.future.take())
                .map(|future| (id, future))
        };

        let Some((id, mut future)) = picked else {
            // The task was killed by a crash between being woken and being
            // polled, which is a normal race in this design.
            return true;
        };

        {
            let mut state = self.state();
            state.record(format!("poll task={}", id.0));
        }

        let waker = Waker::from(Arc::new(TaskWaker {
            core: Arc::downgrade(self),
            task: id,
        }));
        let mut cx = std::task::Context::from_waker(&waker);
        let finished = future.as_mut().poll(&mut cx).is_ready();

        // The future is put back, or dropped, only after the poll returns, and
        // the drop happens outside the lock because a future's destructor can
        // reach back into the world.
        let mut orphan = None;
        {
            let mut state = self.state();
            if finished {
                state.tasks.remove(&id);
                state.record(format!("done task={}", id.0));
            } else if let Some(slot) = state.tasks.get_mut(&id) {
                slot.future = Some(future);
            } else {
                orphan = Some(future);
            }
        }
        drop(orphan);
        true
    }

    /// Moves the clock to the next deadline and returns the wakers that fire.
    ///
    /// Returns `None` when no timer is pending, which means the simulation is
    /// either finished or deadlocked. The caller cannot tell those apart and
    /// does not need to.
    fn advance_time(&self, state: &mut SimState) -> Option<Vec<Waker>> {
        let (&(deadline, _), _) = state.timers.iter().next()?;
        state.now = state.now.max(deadline);
        let expired: Vec<(u64, u64)> = state
            .timers
            .range(..=(deadline, u64::MAX))
            .map(|(k, _)| *k)
            .collect();
        let mut wakers = Vec::with_capacity(expired.len());
        for key in expired {
            if let Some(waker) = state.timers.remove(&key) {
                wakers.push(waker);
            }
        }
        state.record(format!("clock advanced, {} timers fire", wakers.len()));
        Some(wakers)
    }

    /// Drops every task belonging to a node. Used by crash, where the point is
    /// that in-flight work simply stops rather than unwinding.
    pub fn kill_node_tasks(&self, node: NodeId) {
        let mut orphans = Vec::new();
        {
            let mut state = self.state();
            let doomed: Vec<TaskId> = state
                .tasks
                .iter()
                .filter(|(_, slot)| slot.node == Some(node))
                .map(|(id, _)| *id)
                .collect();
            for id in doomed {
                if let Some(mut slot) = state.tasks.remove(&id) {
                    state.ready.remove(&id);
                    orphans.extend(slot.future.take());
                }
            }
        }
        drop(orphans);
    }

    /// Releases every task so the reference cycle from state to future back to
    /// state does not leak. Called when the driving `Simulation` is dropped.
    pub fn clear(&self) {
        let mut orphans = Vec::new();
        let handlers;
        {
            let mut state = self.state();
            let ids: Vec<TaskId> = state.tasks.keys().copied().collect();
            for id in ids {
                if let Some(mut slot) = state.tasks.remove(&id) {
                    orphans.extend(slot.future.take());
                }
            }
            state.ready.clear();
            state.timers.clear();
            // Taken rather than cleared: a handler's destructor can reach
            // back into the world too, for example by dropping the last
            // sender of a channel whose receiver's waker is a task waker, so
            // it must run outside the lock like the futures do.
            handlers = std::mem::take(&mut state.handlers);
        }
        drop(orphans);
        drop(handlers);
    }
}

struct TaskWaker {
    core: Weak<SimCore>,
    task: TaskId,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let Some(core) = self.core.upgrade() else {
            return;
        };
        let mut state = core.state();
        if state.tasks.contains_key(&self.task) {
            state.ready.insert(self.task);
        }
    }
}

/// Derives an independent stream from the run's seed.
fn mix(seed: u64, salt: u64) -> u64 {
    let z = seed.wrapping_add(salt).wrapping_mul(0xD6E8_FEB8_6659_FD93);
    z ^ (z >> 32)
}

/// Sleeps until a virtual deadline.
///
/// Every wait in the simulator, whether a `Clock::sleep`, a disk latency, or a
/// message in flight, funnels into this future, so there is exactly one place
/// where virtual time can be waited on.
pub(crate) struct SimSleep {
    core: Arc<SimCore>,
    deadline: u64,
    key: Option<(u64, u64)>,
}

impl SimSleep {
    pub fn new(core: Arc<SimCore>, delay_nanos: u64) -> Self {
        let deadline = core.now() + delay_nanos;
        Self {
            core,
            deadline,
            key: None,
        }
    }
}

impl Future for SimSleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        let me = self.get_mut();
        let mut state = me.core.state();
        if state.now >= me.deadline {
            if let Some(key) = me.key.take() {
                state.timers.remove(&key);
            }
            return std::task::Poll::Ready(());
        }
        // The waker is refreshed on every poll, not only on the first. A
        // future can be polled by one task, left pending, and then moved into
        // another: `orbita_wal` does exactly that when a batch reaches its
        // quorum before every replica has answered and the remaining calls are
        // handed to background tasks. Keeping the first waker meant the timer
        // fired against a task that had already finished, the wake was
        // discarded, and the moved future was never polled again, which the
        // driver reported as the world going idle with work outstanding.
        let key = match me.key {
            Some(key) => key,
            None => {
                let seq = state.seq();
                (me.deadline, seq)
            }
        };
        state.timers.insert(key, cx.waker().clone());
        me.key = Some(key);
        std::task::Poll::Pending
    }
}

impl Drop for SimSleep {
    fn drop(&mut self) {
        // A sleep abandoned by a select or killed by a crash must not leave a
        // timer behind, or the clock would advance to a deadline nobody is
        // waiting for and the run would never look idle.
        if let Some(key) = self.key.take() {
            self.core.state().timers.remove(&key);
        }
    }
}

/// Yields to the scheduler, giving other runnable tasks a chance to interleave
/// at this point without any virtual time passing.
pub(crate) struct SimYield {
    yielded: bool,
}

impl SimYield {
    pub fn new() -> Self {
        Self { yielded: false }
    }
}

impl Future for SimYield {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        let me = self.get_mut();
        if me.yielded {
            std::task::Poll::Ready(())
        } else {
            me.yielded = true;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    }
}
