//! One partition, as this node holds it.
//!
//! A host bundles the three things a partition needs on a worker: its storage
//! engine, its log, and the bookkeeping the read and write paths in ADR 0001
//! and ADR 0003 require. Routing decides which host a request belongs to; a
//! host knows nothing about the cluster.
//!
//! # The write path
//!
//! Per [ADR 0003](../../../docs/adr/0003-conditions-evaluate-against-pending-writes.md)
//! the owner evaluates a condition against committed state overlaid with its
//! own pending writes, and holds no lock across replication.
//! `Partition::put` and `Partition::delete` fuse evaluation and commit under
//! one lock, which is the right shape for a single writer and the wrong shape
//! for an owner, so this path reads with `get`, decides against the overlay,
//! and commits with `apply`.
//!
//! The submission lock deserves its own note. It is held across evaluating the
//! condition and across handing the write to the log, and it is released
//! before the log has replicated anything. The reason it has to cover the hand
//! off at all is that the log assigns the Lamport, and the Lamport is both the
//! version and the order in which writes must be applied to storage. If two
//! writes could enter the log in a different order from the one they were
//! evaluated in, the overlay would disagree with the log about which write
//! wins, and the storage engine would silently drop the one that arrived with
//! the lower Lamport second.
//!
//! That is why the future returned by `Wal::commit` is polled once while the
//! lock is held. The first poll is what reserves the Lamport; everything after
//! it is the fsync and the round trip to the replicas, which happens after the
//! lock is gone.

use crate::lease::{LeaseTable, ReplicaReadState};
use crate::pending::{self, PendingRecord, PendingSet};

use bytes::Bytes;
use orbita_core::{
    Epoch, Error, KeyRange, Lamport, PartitionId, Record, Result, Version, WriteCondition,
};
use orbita_runtime::{timeout, Clock, Runtime};
use orbita_storage::{Mutation, Partition, ScanPage, TOMBSTONE_RETENTION_MILLIS};
use orbita_wal::{PartitionLog, Wal, WalConfig, WalEntry, WalOp};

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::Poll;
use std::time::Duration;

/// How long a scan waits for acknowledged writes to reach the storage engine.
///
/// A page is read from RocksDB alone, so a write that has been acknowledged
/// and not yet applied would be missing from it. Waiting is cheap because the
/// queue drains in the time an apply takes, and a scan is already the
/// expensive path.
const SCAN_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// What a write does to its key.
#[derive(Debug, Clone)]
pub(crate) enum WriteOp {
    Put {
        value: Bytes,
        ttl_millis: Option<u64>,
    },
    Delete,
}

/// What the client is told about a write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WriteAck {
    pub applied: bool,
    /// The version the key now sits at, when the write happened.
    pub version: Option<Version>,
    /// The version that was there instead, when the condition did not hold.
    pub current_version: Option<Version>,
    /// Whether the key existed before the call, which `DELETE` reports.
    pub existed: bool,
}

/// Where a partition's files live.
///
/// The two paths are separate because they go through different layers. The
/// log goes through `orbita_runtime::Disk`, so its path is relative to the
/// node's data root and the simulator can fault-inject it. RocksDB does its
/// own I/O underneath that seam, so its path is a real filesystem path.
#[derive(Debug, Clone)]
pub(crate) struct PartitionPaths {
    pub storage_path: String,
    pub wal_dir: String,
}

pub(crate) struct PartitionHost<R: Runtime> {
    id: PartitionId,
    epoch: Epoch,
    runtime: R,
    storage: Arc<Partition<R>>,
    /// Present only when this node owns the partition. A replica holds the
    /// same log through `WalService` instead, and serves no writes.
    wal: Option<Arc<Wal<R>>>,
    log: Arc<PartitionLog<R>>,
    pending: Arc<Mutex<PendingSet>>,
    /// Serialises condition evaluation and Lamport assignment, and nothing
    /// else. See the module docs.
    submit: tokio::sync::Mutex<()>,
    applies: tokio::sync::mpsc::UnboundedSender<tokio::sync::oneshot::Receiver<Option<Mutation>>>,
    drained: Arc<tokio::sync::Notify>,
    read_state: Mutex<ReplicaReadState>,
    #[allow(dead_code)]
    leases: Mutex<LeaseTable>,
}

impl<R: Runtime> PartitionHost<R> {
    /// Opens a partition this node owns: storage, log, and the replay of
    /// anything the log holds that storage has not applied.
    pub(crate) async fn open_owner(
        runtime: R,
        id: PartitionId,
        epoch: Epoch,
        range: KeyRange,
        paths: &PartitionPaths,
        replicas: Vec<orbita_core::NodeId>,
    ) -> Result<Arc<Self>> {
        let storage = Arc::new(Partition::open(runtime.clone(), &paths.storage_path, range).await?);
        let config = WalConfig::new(id, paths.wal_dir.clone(), epoch).with_replicas(replicas);
        let wal = Wal::open(runtime.clone(), config).await?;

        // A restart finds entries that were durable and never applied, because
        // the acknowledgement to the client came before the apply. Replaying
        // them is what makes that ordering safe.
        let recovery = wal.recover();
        for entry in &recovery.entries {
            storage.apply(&mutation_of(entry)).await?;
        }
        if let Some(truncation) = recovery.truncated {
            tracing::warn!(
                partition = id.get(),
                reason = ?truncation.reason,
                offset = truncation.offset,
                "log tail was dropped during recovery"
            );
        }

        let log = wal.log();
        Ok(Self::assemble(runtime, id, epoch, storage, Some(wal), log))
    }

    /// Opens a partition this node replicates but does not own.
    ///
    /// The log is opened here and handed to `WalService` rather than opened by
    /// it, because a node that is later promoted keeps the same log it was
    /// replicating into, and reopening a live log would run recovery a second
    /// time.
    pub(crate) async fn open_replica(
        runtime: R,
        id: PartitionId,
        epoch: Epoch,
        range: KeyRange,
        paths: &PartitionPaths,
    ) -> Result<Arc<Self>> {
        let storage = Arc::new(Partition::open(runtime.clone(), &paths.storage_path, range).await?);
        let log = PartitionLog::open(
            runtime.clone(),
            paths.wal_dir.clone(),
            id,
            orbita_wal::DEFAULT_SEGMENT_TARGET_BYTES,
        )
        .await?;
        for entry in &log.recovery().entries {
            storage.apply(&mutation_of(entry)).await?;
        }

        let host = Self::assemble(runtime, id, epoch, storage, None, log);
        let applied = host.storage.committed_lamport().await?;
        *host.read_state.lock().expect("read state poisoned") = ReplicaReadState::new(applied);
        Ok(host)
    }

    fn assemble(
        runtime: R,
        id: PartitionId,
        epoch: Epoch,
        storage: Arc<Partition<R>>,
        wal: Option<Arc<Wal<R>>>,
        log: Arc<PartitionLog<R>>,
    ) -> Arc<Self> {
        let (applies, queue) = tokio::sync::mpsc::unbounded_channel();
        let pending = Arc::new(Mutex::new(PendingSet::default()));
        let drained = Arc::new(tokio::sync::Notify::new());

        // The applier holds weak references so that dropping a host closes
        // its RocksDB instance there and then. A background task keeping the
        // engine alive would make a restart fail to acquire the lock on files
        // the previous incarnation has already finished with.
        runtime.spawn(apply_loop(
            id,
            Arc::downgrade(&storage),
            Arc::downgrade(&pending),
            Arc::downgrade(&drained),
            queue,
        ));

        Arc::new(Self {
            id,
            epoch,
            runtime,
            storage,
            wal,
            log,
            pending,
            submit: tokio::sync::Mutex::new(()),
            applies,
            drained,
            read_state: Mutex::new(ReplicaReadState::new(Lamport::ZERO)),
            leases: Mutex::new(LeaseTable::default()),
        })
    }

    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn id(&self) -> PartitionId {
        self.id
    }

    #[must_use]
    pub(crate) fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// The log, so a node that changes role keeps the file it already has
    /// rather than reopening and recovering a live log.
    pub(crate) fn log(&self) -> Arc<PartitionLog<R>> {
        Arc::clone(&self.log)
    }

    #[must_use]
    pub(crate) fn is_owner(&self) -> bool {
        self.wal.is_some()
    }

    /// Whether this node may answer a read for `key` without asking the owner.
    ///
    /// The owner always may. A replica may only under a live lease with no gap
    /// in its invalidation stream and no invalidation outstanding for this
    /// key, which is the whole of the ADR 0001 read path.
    pub(crate) fn may_serve(&self, key: &[u8]) -> bool {
        if self.is_owner() {
            return true;
        }
        let now = self.runtime.clock().monotonic_nanos();
        self.read_state
            .lock()
            .expect("read state poisoned")
            .may_serve(now, key)
    }

    /// The key's current value, from storage overlaid with anything committed
    /// that has not been applied yet.
    ///
    /// The overlay is read before storage on purpose. An entry leaves the
    /// overlay only after storage has it, so a reader that looks at the
    /// overlay first either sees the pending record or sees the applied one,
    /// and never falls into the gap between them.
    pub(crate) async fn get(&self, key: &[u8]) -> Result<Option<Record>> {
        let overlay = self
            .pending
            .lock()
            .expect("pending set poisoned")
            .overlay(key);
        let committed = self.storage.get(key).await?;
        Ok(pending::visible(
            committed,
            &overlay,
            self.runtime.clock().now_millis(),
        ))
    }

    /// One page of a prefix scan.
    pub(crate) async fn scan(
        &self,
        prefix: &[u8],
        cursor: Option<&[u8]>,
        limit: u32,
    ) -> Result<ScanPage> {
        self.wait_for_applies().await;
        self.storage.scan(prefix, cursor, limit).await
    }

    /// The highest Lamport this partition has applied, which is what a replica
    /// reports about how caught up it is.
    #[allow(dead_code)]
    pub(crate) async fn committed_lamport(&self) -> Result<Lamport> {
        self.storage.committed_lamport().await
    }

    /// Evaluates the condition, commits through the log, and answers the
    /// client. Applying to storage happens afterwards, on the applier.
    pub(crate) async fn write(
        &self,
        key: Bytes,
        op: WriteOp,
        condition: WriteCondition,
    ) -> Result<WriteAck> {
        let Some(wal) = self.wal.as_ref() else {
            return Err(Error::NotOwner {
                partition: self.id,
                owner: None,
            });
        };

        let guard = self.submit.lock().await;
        let now = self.runtime.clock().now_millis();

        let overlay = self
            .pending
            .lock()
            .expect("pending set poisoned")
            .overlay(&key);
        let committed = self.storage.get(&key).await?;
        let visible = pending::visible(committed, &overlay, now);

        if let Some(failure) = evaluate(condition, visible.as_ref(), overlay.uncertain) {
            return Ok(failure);
        }
        let existed = visible.is_some();

        // Deleting something that is not there commits nothing. Writing a
        // tombstone for a key that never existed would consume a version and
        // tell the client a delete happened, and neither is true.
        if matches!(op, WriteOp::Delete) && !existed {
            return Ok(WriteAck {
                applied: true,
                version: None,
                current_version: None,
                existed: false,
            });
        }

        let wal_op = wal_op_of(&key, &op, now);
        let ticket = self
            .pending
            .lock()
            .expect("pending set poisoned")
            .reserve(&key);

        // Polling once assigns the Lamport under the submission lock. See the
        // module docs for why the order of those assignments is what keeps the
        // overlay and the storage engine agreeing with the log.
        let mut commit = Box::pin(wal.commit(wal_op.clone()));
        let first = poll_once(&mut commit).await;

        let (resolved, applied) = tokio::sync::oneshot::channel();
        // Reserving the apply slot here, in submission order, is what makes
        // the applier apply in Lamport order.
        let queued = self.applies.send(applied).is_ok();
        drop(guard);

        let outcome = match first {
            Some(outcome) => outcome,
            None => commit.await,
        };

        match outcome {
            Ok(lamport) => {
                let record = pending_record_of(&op, lamport, now);
                self.pending
                    .lock()
                    .expect("pending set poisoned")
                    .resolve(&ticket, lamport, record);
                if queued {
                    let _ = resolved.send(Some(mutation_of_op(lamport, &key, &wal_op)));
                }
                Ok(WriteAck {
                    applied: true,
                    version: Some(Version(lamport.get())),
                    current_version: None,
                    existed,
                })
            }
            Err(error) => {
                self.pending
                    .lock()
                    .expect("pending set poisoned")
                    .abandon(&ticket);
                let _ = resolved.send(None);
                Err(error)
            }
        }
    }

    /// Grants a replica a read lease, which the owner does on its heartbeat.
    ///
    /// Unused until there is a heartbeat to carry it. See the lease module.
    #[allow(dead_code)]
    pub(crate) fn grant_lease(&self, node: orbita_core::NodeId, duration: Duration) {
        let now = self.runtime.clock().monotonic_nanos();
        self.leases
            .lock()
            .expect("lease table poisoned")
            .grant(node, now, duration);
    }

    /// The replicas a write must hear from before it is acknowledged, per the
    /// coherence quorum in ADR 0001.
    #[allow(dead_code)]
    pub(crate) fn lease_holders(&self) -> Vec<orbita_core::NodeId> {
        let now = self.runtime.clock().monotonic_nanos();
        self.leases
            .lock()
            .expect("lease table poisoned")
            .holders(now)
    }

    /// Records that the owner has told this replica a key is changing.
    #[allow(dead_code)]
    pub(crate) fn invalidate(&self, key: Bytes, lamport: Lamport) {
        self.read_state
            .lock()
            .expect("read state poisoned")
            .invalidate(key, lamport);
    }

    /// Applies an entry this node received as a replica, and clears the key's
    /// invalidation once storage has it.
    #[allow(dead_code)]
    pub(crate) async fn apply_replicated(&self, entry: &WalEntry) -> Result<()> {
        let mutation = mutation_of(entry);
        self.storage.apply(&mutation).await?;
        self.read_state
            .lock()
            .expect("read state poisoned")
            .applied(&mutation.key, mutation.lamport);
        Ok(())
    }

    async fn wait_for_applies(&self) {
        loop {
            let notified = self.drained.notified();
            let mut notified = std::pin::pin!(notified);
            // Registered before the check so a drain that lands between the
            // two is not missed.
            notified.as_mut().enable();

            if self
                .pending
                .lock()
                .expect("pending set poisoned")
                .unapplied()
                == 0
            {
                return;
            }

            if timeout(self.runtime.clock(), SCAN_DRAIN_TIMEOUT, notified)
                .await
                .is_err()
            {
                tracing::warn!(
                    partition = self.id.get(),
                    "scan gave up waiting for acknowledged writes to be applied"
                );
                return;
            }
        }
    }
}

/// Applies committed writes in the order the log gave them.
///
/// Order is not an optimisation here. The storage engine tracks one committed
/// Lamport per partition and ignores anything at or below it, so an apply that
/// runs out of order does not merely land late, it is discarded.
async fn apply_loop<R: Runtime>(
    partition: PartitionId,
    storage: Weak<Partition<R>>,
    pending: Weak<Mutex<PendingSet>>,
    drained: Weak<tokio::sync::Notify>,
    mut queue: tokio::sync::mpsc::UnboundedReceiver<
        tokio::sync::oneshot::Receiver<Option<Mutation>>,
    >,
) {
    while let Some(slot) = queue.recv().await {
        // An error means the write was dropped before it resolved, and a
        // `None` means it failed. Both leave the log's sequence with a hole
        // where nothing was ever acknowledged, which the storage engine
        // tolerates because it only requires Lamports to increase.
        let Ok(Some(mutation)) = slot.await else {
            continue;
        };
        let (Some(storage), Some(pending)) = (storage.upgrade(), pending.upgrade()) else {
            // The partition closed underneath us. Anything unapplied is still
            // in the log, and recovery replays it.
            return;
        };
        if let Err(error) = storage.apply(&mutation).await {
            tracing::error!(
                partition = partition.get(),
                lamport = mutation.lamport.get(),
                %error,
                "applying an acknowledged write to storage failed"
            );
        }
        pending
            .lock()
            .expect("pending set poisoned")
            .applied(&mutation.key, mutation.lamport);
        if let Some(drained) = drained.upgrade() {
            drained.notify_waiters();
        }
    }
}

/// Polls a future once and reports whether it finished.
///
/// This exists for one reason: `Wal::commit` reserves its Lamport on the first
/// poll, and the caller needs that reservation to happen while it still holds
/// the submission lock. Awaiting the whole future there would hold a lock
/// across replication, which ADR 0003 rules out.
async fn poll_once<F: Future>(future: &mut Pin<Box<F>>) -> Option<F::Output> {
    std::future::poll_fn(|cx| {
        Poll::Ready(match future.as_mut().poll(cx) {
            Poll::Ready(value) => Some(value),
            Poll::Pending => None,
        })
    })
    .await
}

/// Checks a condition, returning the acknowledgement to send if it fails.
fn evaluate(
    condition: WriteCondition,
    visible: Option<&Record>,
    uncertain: bool,
) -> Option<WriteAck> {
    let found = visible.map(|r| r.version);
    let holds = match condition {
        WriteCondition::None => true,
        // A key with a write in flight has no version anyone can hold, so no
        // condition against it can be satisfied. See the pending module.
        _ if uncertain => false,
        WriteCondition::IfNotPresent => visible.is_none(),
        WriteCondition::IfVersion(expected) => found == Some(expected),
    };
    if holds {
        None
    } else {
        Some(WriteAck {
            applied: false,
            version: None,
            current_version: found,
            existed: visible.is_some(),
        })
    }
}

/// Resolves a TTL against the owner's clock at the moment it accepts the
/// write, so replication lag cannot extend a key's life.
fn wal_op_of(key: &Bytes, op: &WriteOp, now_millis: u64) -> WalOp {
    match op {
        WriteOp::Put { value, ttl_millis } => WalOp::Put {
            key: key.clone(),
            value: value.clone(),
            expires_at_millis: ttl_millis.map(|ttl| now_millis.saturating_add(ttl)),
        },
        WriteOp::Delete => WalOp::Delete {
            key: key.clone(),
            tombstone_expires_at_millis: Some(
                now_millis.saturating_add(TOMBSTONE_RETENTION_MILLIS),
            ),
        },
    }
}

fn pending_record_of(op: &WriteOp, lamport: Lamport, now_millis: u64) -> PendingRecord {
    match op {
        WriteOp::Put { value, ttl_millis } => PendingRecord::Put(Record {
            value: value.clone(),
            version: Version(lamport.get()),
            expires_at_millis: ttl_millis.map(|ttl| now_millis.saturating_add(ttl)),
        }),
        WriteOp::Delete => PendingRecord::Delete,
    }
}

fn mutation_of(entry: &WalEntry) -> Mutation {
    mutation_of_op(entry.lamport, key_of(&entry.op), &entry.op)
}

fn key_of(op: &WalOp) -> &Bytes {
    match op {
        WalOp::Put { key, .. } | WalOp::Delete { key, .. } => key,
    }
}

fn mutation_of_op(lamport: Lamport, key: &Bytes, op: &WalOp) -> Mutation {
    match op {
        WalOp::Put {
            value,
            expires_at_millis,
            ..
        } => Mutation::put(lamport, key.clone(), value.clone(), *expires_at_millis),
        WalOp::Delete {
            tombstone_expires_at_millis,
            ..
        } => Mutation::delete(
            lamport,
            key.clone(),
            // An entry with no tombstone deadline reclaims immediately, which
            // reads the same as a reclaimed tombstone: the key is absent.
            tombstone_expires_at_millis.unwrap_or(0),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(version: u64) -> Record {
        Record {
            value: Bytes::from_static(b"v"),
            version: Version(version),
            expires_at_millis: None,
        }
    }

    #[test]
    fn an_unconditional_write_always_proceeds() {
        assert!(evaluate(WriteCondition::None, None, false).is_none());
        assert!(evaluate(WriteCondition::None, Some(&record(3)), true).is_none());
    }

    #[test]
    fn if_not_present_reports_what_it_found() {
        let failure = evaluate(WriteCondition::IfNotPresent, Some(&record(3)), false)
            .expect("the key is there, so the condition fails");
        assert!(!failure.applied);
        assert_eq!(failure.current_version, Some(Version(3)));
    }

    #[test]
    fn a_compare_and_swap_against_the_wrong_version_fails() {
        let failure = evaluate(
            WriteCondition::IfVersion(Version(2)),
            Some(&record(3)),
            false,
        )
        .expect("versions differ");
        assert_eq!(failure.current_version, Some(Version(3)));
        assert!(failure.existed);
    }

    #[test]
    fn a_compare_and_swap_fails_while_the_key_has_a_write_in_flight() {
        // The in-flight write is about to move the version, so the version the
        // caller is holding is already stale even though it matches what is
        // committed. Succeeding here is how two swaps against one version both
        // win.
        assert!(evaluate(
            WriteCondition::IfVersion(Version(3)),
            Some(&record(3)),
            true
        )
        .is_some());
        assert!(evaluate(WriteCondition::IfNotPresent, None, true).is_some());
    }

    #[test]
    fn a_ttl_becomes_an_absolute_deadline_at_the_owners_clock() {
        let op = wal_op_of(
            &Bytes::from_static(b"k"),
            &WriteOp::Put {
                value: Bytes::from_static(b"v"),
                ttl_millis: Some(1_000),
            },
            5_000,
        );
        match op {
            WalOp::Put {
                expires_at_millis, ..
            } => assert_eq!(expires_at_millis, Some(6_000)),
            WalOp::Delete { .. } => panic!("a put became a delete"),
        }
    }

    #[tokio::test]
    async fn polling_once_reports_a_future_that_is_not_done() {
        let mut never = Box::pin(std::future::pending::<()>());
        assert!(poll_once(&mut never).await.is_none());

        let mut ready = Box::pin(std::future::ready(7));
        assert_eq!(poll_once(&mut ready).await, Some(7));
    }
}
