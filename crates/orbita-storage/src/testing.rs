//! A runtime and an object store for the crate's own tests.
//!
//! The storage engine reads the clock on every operation, so testing TTL means
//! controlling time rather than sleeping through it. Persistence goes through
//! `orbita_objectstore::ObjectStore`, so the tests run against the in-memory
//! store the format crate ships for exactly this purpose, and a "restart" is
//! reopening the partition over the same store. Everything else here is a
//! stub, because one partition touches no disk and never talks to a peer.

use crate::partition::WriteOutcome;

use bytes::Bytes;
use orbita_core::{
    Epoch, KeyRange, KeyspaceId, Lamport, NodeId, PartitionId, Version, WriteCondition,
};
use orbita_format::testing::MemoryStore;
use orbita_format::PartitionPath;
use orbita_objectstore::{ETag, ObjectMeta, ObjectResult, ObjectStore, Precondition};
use orbita_runtime::{
    Clock, Disk, DiskError, File, OpenOptions, PeerCall, PeerHandler, Rng, Runtime, SeededRng,
    ServiceId, Transport, TransportError,
};
use std::future::Future;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Wall time a test sets by hand.
///
/// Expiry is checked against this, so a test can stand exactly on a deadline
/// rather than sleeping past it and hoping.
#[derive(Debug, Clone, Default)]
pub(crate) struct ManualClock {
    millis: Arc<AtomicU64>,
}

impl ManualClock {
    pub fn set_millis(&self, millis: u64) {
        self.millis.store(millis, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_millis(&self) -> u64 {
        self.millis.load(Ordering::SeqCst)
    }

    fn monotonic_nanos(&self) -> u64 {
        self.millis.load(Ordering::SeqCst).saturating_mul(1_000_000)
    }

    fn sleep(&self, _duration: Duration) -> impl Future<Output = ()> + Send {
        std::future::ready(())
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct NoDisk;

#[derive(Debug, Clone)]
pub(crate) struct NoFile;

impl Disk for NoDisk {
    type File = NoFile;

    async fn open(&self, path: &str, _options: OpenOptions) -> Result<Self::File, DiskError> {
        Err(unreachable_seam(path))
    }

    async fn remove(&self, path: &str) -> Result<(), DiskError> {
        Err(unreachable_seam(path))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, DiskError> {
        Err(unreachable_seam(prefix))
    }
}

impl File for NoFile {
    async fn append(&self, _data: Bytes) -> Result<u64, DiskError> {
        Err(unreachable_seam(""))
    }

    async fn sync(&self) -> Result<(), DiskError> {
        Err(unreachable_seam(""))
    }

    async fn read_at(&self, _offset: u64, _len: usize) -> Result<Bytes, DiskError> {
        Err(unreachable_seam(""))
    }

    async fn truncate(&self, _offset: u64) -> Result<(), DiskError> {
        Err(unreachable_seam(""))
    }

    async fn size(&self) -> Result<u64, DiskError> {
        Err(unreachable_seam(""))
    }
}

fn unreachable_seam(what: &str) -> DiskError {
    DiskError::Io(format!(
        "the storage engine does not use the disk seam ({what})"
    ))
}

#[derive(Debug, Clone, Default)]
pub(crate) struct NoTransport;

impl Transport for NoTransport {
    async fn call(&self, to: NodeId, _call: PeerCall) -> Result<Bytes, TransportError> {
        Err(TransportError::UnknownPeer(to))
    }

    fn register(&self, _service: ServiceId, _handler: impl PeerHandler) {}

    fn local_node(&self) -> NodeId {
        NodeId(1)
    }
}

/// A seeded generator that survives the runtime being cloned, so every clone
/// draws from the same reproducible sequence.
#[derive(Debug, Clone)]
pub(crate) struct SharedRng(Arc<SeededRng>);

impl Rng for SharedRng {
    fn next_u64(&self) -> u64 {
        self.0.next_u64()
    }
}

#[derive(Clone)]
pub(crate) struct TestRuntime {
    clock: ManualClock,
    disk: NoDisk,
    transport: NoTransport,
    rng: SharedRng,
}

impl Runtime for TestRuntime {
    type Clock = ManualClock;
    type Disk = NoDisk;
    type Transport = NoTransport;
    type Rng = SharedRng;

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
        tokio::spawn(future);
    }
}

/// A partition plus everything needed to reopen it over the same objects,
/// which is what a restart is under this engine.
pub(crate) struct TempPartition {
    inner: crate::Partition<TestRuntime>,
    store: Arc<MemoryStore>,
    runtime: TestRuntime,
    range: KeyRange,
}

impl TempPartition {
    /// Opens another incarnation over the same objects at a different epoch,
    /// which is what a failover looks like from the object store's side.
    pub(crate) async fn successor(&self, epoch: Epoch) -> crate::Partition<TestRuntime> {
        crate::Partition::open(
            self.runtime.clone(),
            self.store.clone(),
            partition_path(),
            epoch,
            self.range.clone(),
        )
        .await
        .expect("opening a successor over the same store")
    }
}

impl Deref for TempPartition {
    type Target = crate::Partition<TestRuntime>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Drops the partition and opens a fresh one over the same store, losing
/// everything a real restart would lose.
pub(crate) async fn reopened(partition: TempPartition) -> TempPartition {
    let TempPartition {
        inner,
        store,
        runtime,
        range,
    } = partition;
    drop(inner);
    let inner = crate::Partition::open(
        runtime.clone(),
        store.clone(),
        partition_path(),
        Epoch(1),
        range.clone(),
    )
    .await
    .expect("reopening over the same store");
    TempPartition {
        inner,
        store,
        runtime,
        range,
    }
}

/// A partition plus the handle that moves its clock.
pub(crate) async fn partition_with_clock() -> (TempPartition, ManualClock) {
    open_partition(KeyRange::unbounded(), ManualClock::default()).await
}

/// Two incarnations over one object namespace, standing in for an owner and a
/// replica that must observe publication without publishing itself.
pub(crate) async fn partition_pair_with_clock() -> (TempPartition, TempPartition, ManualClock) {
    partition_pair_at_epochs(Epoch(1), Epoch(1)).await
}

/// The same pair, opened under different ownership epochs.
///
/// A failover is exactly this: the same objects, a new writer, a higher epoch.
/// Tests that care what a manifest says about ownership need to be able to
/// build one that a lower-epoch reader will meet.
pub(crate) async fn partition_pair_at_epochs(
    first_epoch: Epoch,
    second_epoch: Epoch,
) -> (TempPartition, TempPartition, ManualClock) {
    let clock = ManualClock::default();
    let store = Arc::new(MemoryStore::new());
    let runtime = TestRuntime {
        clock: clock.clone(),
        disk: NoDisk,
        transport: NoTransport,
        rng: SharedRng(Arc::new(SeededRng::new(0))),
    };
    let range = KeyRange::unbounded();
    let first = crate::Partition::open(
        runtime.clone(),
        store.clone(),
        partition_path(),
        first_epoch,
        range.clone(),
    )
    .await
    .expect("opening the owner");
    let second = crate::Partition::open(
        runtime.clone(),
        store.clone(),
        partition_path(),
        second_epoch,
        range.clone(),
    )
    .await
    .expect("opening the replica");
    (
        TempPartition {
            inner: first,
            store: store.clone(),
            runtime: runtime.clone(),
            range: range.clone(),
        },
        TempPartition {
            inner: second,
            store,
            runtime,
            range,
        },
        clock,
    )
}

/// A partition wrapped in the Lamport assignment its owner would do.
///
/// Versions are Lamports now, so a test that does not care about specific
/// numbers still has to supply increasing ones. This stands in for the owning
/// worker: it hands out the next Lamport and submits the write inside the same
/// critical section, which is the contract [`crate::Partition::put`] requires
/// and the shape ADR 0001 describes. The Lamport is only consumed when
/// something is actually committed, so a refused condition does not leave a
/// hole in the sequence.
pub(crate) struct Owner {
    partition: TempPartition,
    next: tokio::sync::Mutex<u64>,
}

impl Deref for Owner {
    type Target = crate::Partition<TestRuntime>;

    fn deref(&self) -> &Self::Target {
        &self.partition
    }
}

impl Owner {
    pub async fn put(
        &self,
        key: &[u8],
        value: Bytes,
        ttl: Option<Duration>,
        condition: WriteCondition,
    ) -> orbita_core::Result<WriteOutcome> {
        let mut next = self.next.lock().await;
        let outcome = self
            .partition
            .put(Lamport(*next), key, value, ttl, condition)
            .await?;
        if outcome
            == (WriteOutcome::Applied {
                version: Version(*next),
            })
        {
            *next += 1;
        }
        Ok(outcome)
    }

    pub async fn delete(
        &self,
        key: &[u8],
        condition: WriteCondition,
    ) -> orbita_core::Result<WriteOutcome> {
        let mut next = self.next.lock().await;
        let outcome = self
            .partition
            .delete(Lamport(*next), key, condition)
            .await?;
        if outcome
            == (WriteOutcome::Applied {
                version: Version(*next),
            })
        {
            *next += 1;
        }
        Ok(outcome)
    }

    /// Writes at a Lamport the test chose, for the cases where the exact
    /// number is the point.
    pub async fn put_at(
        &self,
        lamport: Lamport,
        key: &[u8],
        value: Bytes,
        ttl: Option<Duration>,
        condition: WriteCondition,
    ) -> orbita_core::Result<WriteOutcome> {
        let mut next = self.next.lock().await;
        let outcome = self
            .partition
            .put(lamport, key, value, ttl, condition)
            .await?;
        *next = (*next).max(lamport.get() + 1);
        Ok(outcome)
    }

    pub async fn delete_at(
        &self,
        lamport: Lamport,
        key: &[u8],
        condition: WriteCondition,
    ) -> orbita_core::Result<WriteOutcome> {
        let mut next = self.next.lock().await;
        let outcome = self.partition.delete(lamport, key, condition).await?;
        *next = (*next).max(lamport.get() + 1);
        Ok(outcome)
    }
}

/// A fresh partition covering every key, with time stopped at zero.
pub(crate) async fn owner() -> Owner {
    owner_in_range(KeyRange::unbounded()).await
}

pub(crate) async fn owner_in_range(range: KeyRange) -> Owner {
    owner_from(open_partition(range, ManualClock::default()).await.0)
}

pub(crate) async fn owner_with_clock() -> (Owner, ManualClock) {
    let (partition, clock) = open_partition(KeyRange::unbounded(), ManualClock::default()).await;
    (owner_from(partition), clock)
}

/// An owner whose object store stamps write times from the same clock the
/// engine reads, plus that store and clock.
///
/// The default test store reports no write time, standing in for a backend that
/// cannot, which is exactly what the orphan sweep must refuse to act on. This
/// one is the other case: a backend that does stamp times, so a test can drive
/// the sweep's grace period through virtual time within one clock domain, the
/// way a simulated run does.
pub(crate) async fn owner_with_clocked_store() -> (Owner, Arc<MemoryStore>, ManualClock) {
    let clock = ManualClock::default();
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let runtime = TestRuntime {
        clock: clock.clone(),
        disk: NoDisk,
        transport: NoTransport,
        rng: SharedRng(Arc::new(SeededRng::new(0))),
    };
    let range = KeyRange::unbounded();
    let inner = crate::Partition::open(
        runtime.clone(),
        store.clone(),
        partition_path(),
        Epoch(1),
        range.clone(),
    )
    .await
    .expect("opening a partition over a clocked store");
    let partition = TempPartition {
        inner,
        store: store.clone(),
        runtime,
        range,
    };
    (owner_from(partition), store, clock)
}

/// Opens a partition at an arbitrary id, epoch, and range over an existing
/// store, which is how a test inspects a split child that was prepared over the
/// parent's objects: same bucket, new partition directory, no copy.
pub(crate) async fn open_child(
    store: &Arc<MemoryStore>,
    clock: &ManualClock,
    id: PartitionId,
    epoch: Epoch,
    range: KeyRange,
) -> crate::Partition<TestRuntime> {
    let runtime = TestRuntime {
        clock: clock.clone(),
        disk: NoDisk,
        transport: NoTransport,
        rng: SharedRng(Arc::new(SeededRng::new(0))),
    };
    crate::Partition::open(
        runtime,
        store.clone(),
        PartitionPath::new("", KeyspaceId(1), id),
        epoch,
        range,
    )
    .await
    .expect("opening a child over the parent's store")
}

fn owner_from(partition: TempPartition) -> Owner {
    Owner {
        partition,
        // Lamport zero means "nothing committed", so the first write is one.
        next: tokio::sync::Mutex::new(1),
    }
}

fn partition_path() -> PartitionPath {
    PartitionPath::new("", KeyspaceId(1), PartitionId(1))
}

async fn open_partition(range: KeyRange, clock: ManualClock) -> (TempPartition, ManualClock) {
    let store = Arc::new(MemoryStore::new());
    let runtime = TestRuntime {
        clock: clock.clone(),
        disk: NoDisk,
        transport: NoTransport,
        rng: SharedRng(Arc::new(SeededRng::new(0))),
    };
    let inner = crate::Partition::open(
        runtime.clone(),
        store.clone(),
        partition_path(),
        Epoch(1),
        range.clone(),
    )
    .await
    .expect("opening a partition over a fresh store");
    (
        TempPartition {
            inner,
            store,
            runtime,
            range,
        },
        clock,
    )
}

/// An object store that counts what was read, so a test can assert on the cost
/// of an operation rather than only on its result.
///
/// Hydration is the case this exists for. Reading the manifest is one small
/// object; opening a snapshot reads the footer and key index of every segment
/// the manifest names. Those are different enough in cost that "did it rebuild"
/// is a behaviour worth pinning, and it is invisible from the return value --
/// a hydrate that rebuilt and one that did not report the same horizon.
#[derive(Debug)]
pub(crate) struct CountingStore {
    inner: MemoryStore,
    manifest_reads: AtomicUsize,
    segment_reads: AtomicUsize,
}

impl CountingStore {
    pub(crate) fn new() -> Self {
        Self {
            inner: MemoryStore::new(),
            manifest_reads: AtomicUsize::new(0),
            segment_reads: AtomicUsize::new(0),
        }
    }

    /// How many times the manifest object itself has been fetched.
    pub(crate) fn manifest_reads(&self) -> usize {
        self.manifest_reads.load(Ordering::Relaxed)
    }

    /// How many segment reads have happened, whole or ranged. Any of these
    /// means a snapshot was built.
    pub(crate) fn segment_reads(&self) -> usize {
        self.segment_reads.load(Ordering::Relaxed)
    }

    pub(crate) fn reset(&self) {
        self.manifest_reads.store(0, Ordering::Relaxed);
        self.segment_reads.store(0, Ordering::Relaxed);
    }

    fn note(&self, key: &str) {
        if key.ends_with("manifest.json") {
            self.manifest_reads.fetch_add(1, Ordering::Relaxed);
        } else if key.contains("/segments/") {
            self.segment_reads.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[async_trait::async_trait]
impl ObjectStore for CountingStore {
    async fn put(&self, key: &str, data: Bytes) -> ObjectResult<ETag> {
        self.inner.put(key, data).await
    }

    async fn put_if(&self, key: &str, data: Bytes, condition: Precondition) -> ObjectResult<ETag> {
        self.inner.put_if(key, data, condition).await
    }

    async fn get(&self, key: &str) -> ObjectResult<(Bytes, ETag)> {
        self.note(key);
        self.inner.get(key).await
    }

    async fn get_range(&self, key: &str, range: std::ops::Range<u64>) -> ObjectResult<Bytes> {
        self.note(key);
        self.inner.get_range(key, range).await
    }

    async fn head(&self, key: &str) -> ObjectResult<ObjectMeta> {
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str) -> ObjectResult<Vec<ObjectMeta>> {
        self.inner.list(prefix).await
    }

    async fn delete(&self, key: &str) -> ObjectResult<()> {
        self.inner.delete(key).await
    }
}

/// An owner and a second reader over one [`CountingStore`], so a test can
/// assert what hydration actually read.
///
/// Returns the store as well, because the assertion is on its counters rather
/// than on either partition.
pub(crate) async fn counting_partition_pair() -> (
    crate::Partition<TestRuntime>,
    crate::Partition<TestRuntime>,
    Arc<CountingStore>,
) {
    let clock = ManualClock::default();
    let store = Arc::new(CountingStore::new());
    let runtime = TestRuntime {
        clock,
        disk: NoDisk,
        transport: NoTransport,
        rng: SharedRng(Arc::new(SeededRng::new(0))),
    };
    let range = KeyRange::unbounded();
    let owner = crate::Partition::open(
        runtime.clone(),
        store.clone(),
        partition_path(),
        Epoch(1),
        range.clone(),
    )
    .await
    .expect("opening the owner");
    let reader = crate::Partition::open(runtime, store.clone(), partition_path(), Epoch(1), range)
        .await
        .expect("opening the reader");
    (owner, reader, store)
}
