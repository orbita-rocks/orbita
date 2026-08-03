//! A runtime for the crate's own tests.
//!
//! The storage engine reads the clock on every operation, so testing TTL means
//! controlling time rather than sleeping through it. The simulator in
//! `orbita-sim` will eventually provide this, but the storage engine is in the
//! first wave alongside it and cannot wait for it. Everything here is a stub
//! except the clock and the random number generator, because the storage
//! engine touches nothing else: RocksDB does its own file I/O below the disk
//! seam, and one partition never talks to a peer.

use crate::partition::WriteOutcome;

use bytes::Bytes;
use orbita_core::{KeyRange, Lamport, NodeId, Version, WriteCondition};
use orbita_runtime::{
    Clock, Disk, DiskError, File, OpenOptions, PeerCall, PeerHandler, Rng, Runtime, SeededRng,
    ServiceId, Transport, TransportError,
};
use std::future::Future;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
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

/// A partition in a directory that cleans itself up.
pub(crate) struct TempPartition {
    inner: crate::Partition<TestRuntime>,
    dir: PathBuf,
}

impl Deref for TempPartition {
    type Target = crate::Partition<TestRuntime>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Drop for TempPartition {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.dir).ok();
    }
}

/// A partition plus the handle that moves its clock.
pub(crate) async fn partition_with_clock() -> (TempPartition, ManualClock) {
    open_partition(KeyRange::unbounded(), ManualClock::default()).await
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

fn owner_from(partition: TempPartition) -> Owner {
    Owner {
        partition,
        // Lamport zero means "nothing committed", so the first write is one.
        next: tokio::sync::Mutex::new(1),
    }
}

async fn open_partition(range: KeyRange, clock: ManualClock) -> (TempPartition, ManualClock) {
    let dir = temp_dir();
    let runtime = TestRuntime {
        clock: clock.clone(),
        disk: NoDisk,
        transport: NoTransport,
        rng: SharedRng(Arc::new(SeededRng::new(0))),
    };
    let inner = crate::Partition::open(runtime, dir.to_str().unwrap(), range)
        .await
        .expect("opening a partition in a fresh directory");
    (TempPartition { inner, dir }, clock)
}

fn temp_dir() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!("orbita-storage-test-{}-{id}", std::process::id()))
}
