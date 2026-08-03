//! A test-only stand-in for the simulator.
//!
//! `orbita-sim` is being built alongside this crate, so these are the smallest
//! implementations of the runtime seams that let the WAL's failure modes be
//! tested at all: an in-memory disk that can fail, tear, and corrupt writes, an
//! in-memory network that can drop nodes, and a single-threaded executor whose
//! task order is fixed. When the simulator lands, this module goes and the
//! tests point at it instead.
//!
//! Nothing here is exported. It exists so the tests in `tests.rs` can be
//! written today rather than after another crate is finished.

// A harness is a set of knobs, and not every test pulls every knob.
#![allow(dead_code)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use bytes::Bytes;
use orbita_core::NodeId;
use orbita_runtime::{
    Clock, Disk, DiskError, File, OpenOptions, PeerCall, PeerHandler, Runtime, SeededRng,
    ServiceId, Transport, TransportError,
};

// -------------------------------------------------------------------------
// A deterministic executor.
// -------------------------------------------------------------------------

struct Task {
    future: Mutex<Option<Pin<Box<dyn Future<Output = ()> + Send>>>>,
    queue: Arc<Mutex<VecDeque<Arc<Task>>>>,
}

impl Wake for Task {
    fn wake(self: Arc<Self>) {
        let queue = Arc::clone(&self.queue);
        queue.lock().expect("queue poisoned").push_back(self);
    }
}

struct MainWaker(AtomicBool);

impl Wake for MainWaker {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// Runs `future` to completion, driving spawned tasks in between polls.
///
/// One thread and one task at a time, so a failing interleaving is the same
/// interleaving on the next run.
pub(crate) fn block_on<F: Future>(runtime: &TestRuntime, future: F) -> F::Output {
    let queue = Arc::clone(&runtime.queue);
    let flag = Arc::new(MainWaker(AtomicBool::new(true)));
    let waker = Waker::from(Arc::clone(&flag));
    let mut cx = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);

    loop {
        if flag.0.swap(false, Ordering::SeqCst) {
            if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
                return value;
            }
        }

        let ready: Vec<Arc<Task>> = {
            let mut queue = queue.lock().expect("queue poisoned");
            queue.drain(..).collect()
        };

        if ready.is_empty() && !flag.0.load(Ordering::SeqCst) {
            panic!("nothing is runnable and the main future is not done: the test deadlocked");
        }

        for task in ready {
            let mut slot = task.future.lock().expect("task poisoned");
            let Some(mut fut) = slot.take() else {
                continue;
            };
            let task_waker = Waker::from(Arc::clone(&task));
            let mut task_cx = Context::from_waker(&task_waker);
            if fut.as_mut().poll(&mut task_cx).is_pending() {
                *slot = Some(fut);
            }
        }
    }
}

/// Hands control back to the executor once, so another task can run.
pub(crate) async fn yield_now() {
    let mut yielded = false;
    std::future::poll_fn(move |cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

// -------------------------------------------------------------------------
// The runtime.
// -------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct TestRuntime {
    clock: TestClock,
    disk: MemDisk,
    transport: MemTransport,
    rng: Arc<SeededRng>,
    queue: Arc<Mutex<VecDeque<Arc<Task>>>>,
}

impl TestRuntime {
    pub(crate) fn new(disk: MemDisk, transport: MemTransport, seed: u64) -> Self {
        Self {
            clock: TestClock::default(),
            disk,
            transport,
            rng: Arc::new(SeededRng::new(seed)),
            queue: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// A single node with no peers, for tests about the log itself.
    pub(crate) fn solo(seed: u64) -> Self {
        Self::new(MemDisk::new(), MemNetwork::new().node(NodeId(1)), seed)
    }

    pub(crate) fn mem_disk(&self) -> MemDisk {
        self.disk.clone()
    }

    /// Another node's runtime, sharing this one's executor so that one
    /// `block_on` drives the whole cluster.
    pub(crate) fn peer(&self, disk: MemDisk, transport: MemTransport) -> Self {
        Self {
            clock: self.clock.clone(),
            disk,
            transport,
            rng: Arc::clone(&self.rng),
            queue: Arc::clone(&self.queue),
        }
    }
}

impl Runtime for TestRuntime {
    type Clock = TestClock;
    type Disk = MemDisk;
    type Transport = MemTransport;
    type Rng = SeededRng;

    fn clock(&self) -> &Self::Clock {
        &self.clock
    }

    fn disk(&self) -> &Self::Disk {
        &self.disk
    }

    fn transport(&self) -> &Self::Transport {
        &self.transport
    }

    fn rng(&self) -> &Self::Rng {
        &self.rng
    }

    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let task = Arc::new(Task {
            future: Mutex::new(Some(Box::pin(future))),
            queue: Arc::clone(&self.queue),
        });
        self.queue.lock().expect("queue poisoned").push_back(task);
    }
}

#[derive(Clone, Default)]
pub(crate) struct TestClock {
    millis: Arc<AtomicU64>,
}

impl Clock for TestClock {
    fn now_millis(&self) -> u64 {
        self.millis.load(Ordering::SeqCst)
    }

    fn monotonic_nanos(&self) -> u64 {
        self.millis.load(Ordering::SeqCst) * 1_000_000
    }

    async fn sleep(&self, duration: Duration) {
        self.millis
            .fetch_add(duration.as_millis() as u64, Ordering::SeqCst);
        yield_now().await;
    }
}

// -------------------------------------------------------------------------
// An in-memory disk that can misbehave.
// -------------------------------------------------------------------------

/// What the next write should do instead of working.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Faults {
    /// Fail the next append outright, writing nothing.
    pub fail_next_append: bool,
    /// Write only this many bytes of the next append, which is what a crash
    /// partway through one looks like.
    pub tear_next_append_at: Option<usize>,
    /// Fail the next fsync, leaving the caller unable to say what is durable.
    pub fail_next_sync: bool,
}

#[derive(Default)]
struct DiskInner {
    files: HashMap<String, Arc<Mutex<Vec<u8>>>>,
    faults: Faults,
    syncs: u64,
}

#[derive(Clone, Default)]
pub(crate) struct MemDisk {
    inner: Arc<Mutex<DiskInner>>,
}

impl MemDisk {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn set_faults(&self, faults: Faults) {
        self.inner.lock().expect("disk poisoned").faults = faults;
    }

    pub(crate) fn syncs(&self) -> u64 {
        self.inner.lock().expect("disk poisoned").syncs
    }

    pub(crate) fn contents(&self, path: &str) -> Option<Vec<u8>> {
        let inner = self.inner.lock().expect("disk poisoned");
        inner
            .files
            .get(path)
            .map(|f| f.lock().expect("file poisoned").clone())
    }

    pub(crate) fn write_raw(&self, path: &str, bytes: Vec<u8>) {
        let mut inner = self.inner.lock().expect("disk poisoned");
        inner
            .files
            .insert(path.to_string(), Arc::new(Mutex::new(bytes)));
    }

    pub(crate) fn paths(&self) -> Vec<String> {
        let inner = self.inner.lock().expect("disk poisoned");
        let mut out: Vec<String> = inner.files.keys().cloned().collect();
        out.sort();
        out
    }

    /// A byte-for-byte copy, which is how a test takes the disk state at the
    /// instant of a simulated crash.
    pub(crate) fn snapshot(&self) -> MemDisk {
        let inner = self.inner.lock().expect("disk poisoned");
        let files = inner
            .files
            .iter()
            .map(|(path, data)| {
                (
                    path.clone(),
                    Arc::new(Mutex::new(data.lock().expect("file poisoned").clone())),
                )
            })
            .collect();
        MemDisk {
            inner: Arc::new(Mutex::new(DiskInner {
                files,
                faults: Faults::default(),
                syncs: 0,
            })),
        }
    }
}

impl Disk for MemDisk {
    type File = MemFile;

    async fn open(&self, path: &str, options: OpenOptions) -> Result<Self::File, DiskError> {
        let mut inner = self.inner.lock().expect("disk poisoned");
        let existing = inner.files.get(path).cloned();
        let data = match existing {
            Some(data) if !options.truncate => data,
            Some(data) => {
                data.lock().expect("file poisoned").clear();
                data
            }
            None if options.create => {
                let data = Arc::new(Mutex::new(Vec::new()));
                inner.files.insert(path.to_string(), Arc::clone(&data));
                data
            }
            None => return Err(DiskError::NotFound(path.to_string())),
        };
        Ok(MemFile {
            data,
            disk: Arc::clone(&self.inner),
        })
    }

    async fn remove(&self, path: &str) -> Result<(), DiskError> {
        let mut inner = self.inner.lock().expect("disk poisoned");
        match inner.files.remove(path) {
            Some(_) => Ok(()),
            None => Err(DiskError::NotFound(path.to_string())),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, DiskError> {
        let inner = self.inner.lock().expect("disk poisoned");
        let prefix = format!("{prefix}/");
        let mut out: Vec<String> = inner
            .files
            .keys()
            .filter_map(|path| path.strip_prefix(&prefix))
            .filter(|rest| !rest.contains('/'))
            .map(ToString::to_string)
            .collect();
        out.sort();
        Ok(out)
    }
}

pub(crate) struct MemFile {
    data: Arc<Mutex<Vec<u8>>>,
    disk: Arc<Mutex<DiskInner>>,
}

impl File for MemFile {
    async fn append(&self, bytes: Bytes) -> Result<u64, DiskError> {
        let (fail, tear) = {
            let mut inner = self.disk.lock().expect("disk poisoned");
            let fail = std::mem::take(&mut inner.faults.fail_next_append);
            let tear = inner.faults.tear_next_append_at.take();
            (fail, tear)
        };
        if fail {
            return Err(DiskError::Io("injected write failure".into()));
        }

        let mut data = self.data.lock().expect("file poisoned");
        let offset = data.len() as u64;
        let end = tear.map_or(bytes.len(), |n| n.min(bytes.len()));
        data.extend_from_slice(&bytes[..end]);
        if tear.is_some() {
            return Err(DiskError::Io("injected torn write".into()));
        }
        Ok(offset)
    }

    async fn sync(&self) -> Result<(), DiskError> {
        let fail = {
            let mut inner = self.disk.lock().expect("disk poisoned");
            inner.syncs += 1;
            std::mem::take(&mut inner.faults.fail_next_sync)
        };
        // A real fsync is slow enough that other writers pile up behind it,
        // and that pile is what group commit exists to serve. Yielding here is
        // what makes that visible to the tests.
        yield_now().await;
        if fail {
            return Err(DiskError::Io("injected sync failure".into()));
        }
        Ok(())
    }

    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes, DiskError> {
        let data = self.data.lock().expect("file poisoned");
        let start = offset as usize;
        let end = start
            .checked_add(len)
            .ok_or(DiskError::OutOfBounds { offset, len })?;
        if end > data.len() {
            return Err(DiskError::OutOfBounds { offset, len });
        }
        Ok(Bytes::copy_from_slice(&data[start..end]))
    }

    async fn truncate(&self, offset: u64) -> Result<(), DiskError> {
        let mut data = self.data.lock().expect("file poisoned");
        data.truncate(offset as usize);
        Ok(())
    }

    async fn size(&self) -> Result<u64, DiskError> {
        Ok(self.data.lock().expect("file poisoned").len() as u64)
    }
}

// -------------------------------------------------------------------------
// An in-memory network that can lose nodes.
// -------------------------------------------------------------------------

trait ErasedHandler: Send + Sync {
    fn handle(
        &self,
        from: NodeId,
        call: PeerCall,
    ) -> Pin<Box<dyn Future<Output = Result<Bytes, TransportError>> + Send + '_>>;
}

struct Erased<H: PeerHandler>(H);

impl<H: PeerHandler> ErasedHandler for Erased<H> {
    fn handle(
        &self,
        from: NodeId,
        call: PeerCall,
    ) -> Pin<Box<dyn Future<Output = Result<Bytes, TransportError>> + Send + '_>> {
        Box::pin(self.0.handle(from, call))
    }
}

#[derive(Default)]
struct NetworkInner {
    handlers: HashMap<(NodeId, ServiceId), Arc<dyn ErasedHandler>>,
    down: HashSet<NodeId>,
    slow: HashSet<NodeId>,
    calls: u64,
}

#[derive(Clone, Default)]
pub(crate) struct MemNetwork {
    inner: Arc<Mutex<NetworkInner>>,
}

impl MemNetwork {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn node(&self, id: NodeId) -> MemTransport {
        MemTransport {
            network: Arc::clone(&self.inner),
            local: id,
        }
    }

    /// Makes every call to this node fail, which is both a crashed node and
    /// the far side of a partition.
    pub(crate) fn isolate(&self, id: NodeId) {
        self.inner.lock().expect("network poisoned").down.insert(id);
    }

    pub(crate) fn heal(&self, id: NodeId) {
        self.inner
            .lock()
            .expect("network poisoned")
            .down
            .remove(&id);
    }

    /// Delays this node's replies, so the other replica answers first and the
    /// two-of-three path is exercised rather than the everyone-answered one.
    pub(crate) fn slow(&self, id: NodeId) {
        self.inner.lock().expect("network poisoned").slow.insert(id);
    }

    pub(crate) fn calls(&self) -> u64 {
        self.inner.lock().expect("network poisoned").calls
    }
}

#[derive(Clone)]
pub(crate) struct MemTransport {
    network: Arc<Mutex<NetworkInner>>,
    local: NodeId,
}

impl Transport for MemTransport {
    async fn call(&self, to: NodeId, call: PeerCall) -> Result<Bytes, TransportError> {
        let (handler, down, slow) = {
            let mut inner = self.network.lock().expect("network poisoned");
            inner.calls += 1;
            (
                inner.handlers.get(&(to, call.service)).cloned(),
                inner.down.contains(&to),
                inner.slow.contains(&to),
            )
        };
        if down {
            return Err(TransportError::Unreachable(to));
        }
        if slow {
            for _ in 0..4 {
                yield_now().await;
            }
        }
        match handler {
            Some(handler) => handler.handle(self.local, call).await,
            None => Err(TransportError::NoHandler(call.service)),
        }
    }

    fn register(&self, service: ServiceId, handler: impl PeerHandler) {
        self.network
            .lock()
            .expect("network poisoned")
            .handlers
            .insert((self.local, service), Arc::new(Erased(handler)));
    }

    fn local_node(&self) -> NodeId {
        self.local
    }
}
