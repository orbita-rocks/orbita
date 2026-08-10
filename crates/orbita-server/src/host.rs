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
//!
//! The overlay carries a third state the ADR did not have to name: a write
//! that has been submitted and has not yet been told its Lamport. Nothing
//! conditional can be decided against a key in that state, so a conditional
//! write that finds one waits for it — outside the submission lock, which is
//! the part that keeps ADR 0003's promise. See
//! [`PartitionHost::await_settled`]. A wait that runs out of budget fails the
//! call `Unavailable` rather than answering from outside the window, because
//! the owner still does not know who won.

use crate::lease::{LeaseTable, ReplicaReadState, DEFAULT_LEASE_DURATION, DEFAULT_LEASE_MARGIN};
use crate::pending::{self, PendingRecord, PendingSet};
use crate::proxy::{self, LeaseGrant, LeaseReply};

use bytes::Bytes;
use orbita_core::{
    Epoch, Error, KeyRange, Lamport, NodeId, PartitionId, Record, Result, Version, WriteCondition,
};
use orbita_format::PartitionPath;
use orbita_objectstore::ObjectStore;
use orbita_runtime::{join_all, timeout, Clock, PeerCall, Runtime, ServiceId, Transport};
use orbita_storage::{
    ChildSpec, Mutation, Partition, ScanBudget, ScanPage, TOMBSTONE_RETENTION_MILLIS,
};
use orbita_wal::{CatchUpPass, Hydration, PartitionLog, Wal, WalConfig, WalEntry, WalOp};

use std::collections::{HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::Poll;
use std::time::Duration;

/// How long a scan waits for acknowledged writes to reach the storage engine.
///
/// A page is read from the storage engine alone, so a write that has been
/// acknowledged and not yet applied would be missing from it. Waiting is cheap
/// because the queue drains in the time an apply takes, and a scan is already
/// the expensive path.
const SCAN_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a conditional write waits for an in-flight write to the same key
/// to come back from the log before giving up on deciding the condition.
///
/// The wait is normally one replication round trip, because that is all it
/// takes for the other write to learn its Lamport. This bound exists for the
/// case where that never happens — a log that has stopped answering — and it
/// is set at the same five seconds a scan waits for the same reason: long
/// enough that a healthy cluster never reaches it, short enough that a client
/// gets an answer rather than a hung call.
///
/// Reaching it means the owner still does not know, and *still does not know*
/// is the answer the client gets: [`Error::Unavailable`], which is retryable.
/// This bound is not an upper bound on the thing being waited for. A peer call
/// timeout is configurable well past five seconds and a local disk write has
/// no timeout at all, so the write parked on here can and does resolve
/// afterwards. Answering `applied: false` from a stale read at expiry would
/// claim the caller lost to a write that may yet fail, and would report a
/// version that may not be the one that ends up winning.
const CONDITION_SETTLE_TIMEOUT: Duration = Duration::from_secs(5);

/// How many keys a replica may hold unreadable before it is worth saying so.
///
/// There is no correct number here. It is set well above what replication lag
/// produces in a healthy cluster, so that crossing it means something is wrong
/// rather than that the cluster is busy.
const INVALID_SET_WARN: usize = 10_000;

/// The lease timings this partition runs on.
///
/// The two numbers travel together because they are one decision: the margin
/// only has to cover the difference in rate between two monotonic clocks over
/// one lease duration, so changing the duration without looking at the margin
/// is how the safety argument in ADR 0001 quietly stops holding.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LeasePolicy {
    pub duration: Duration,
    pub margin: Duration,
}

impl Default for LeasePolicy {
    fn default() -> Self {
        Self {
            duration: DEFAULT_LEASE_DURATION,
            margin: DEFAULT_LEASE_MARGIN,
        }
    }
}

impl LeasePolicy {
    /// How often the owner renews, which has to be well inside the duration or
    /// a replica drops out of the read set between two heartbeats.
    pub(crate) fn heartbeat_interval(&self) -> Duration {
        // Three renewals per lease means two can be lost before a replica
        // stops serving, which keeps a single dropped message from costing
        // read capacity.
        self.duration / 3
    }
}

/// What a partition is, as far as opening one goes.
///
/// The four travel together because both constructors and the assembly step
/// all need the same set, and threading them individually is how a function
/// grows an argument list nobody can read.
#[derive(Debug, Clone)]
pub(crate) struct HostSpec {
    pub id: PartitionId,
    pub epoch: Epoch,
    pub range: KeyRange,
    pub lease: LeasePolicy,
}

/// What a read of a replica came back with.
pub(crate) enum Read {
    Served(Option<Record>),
    /// This node cannot answer for the key after all, so the owner has to.
    MustForward,
}

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

/// Where a partition's data lives.
///
/// The two locations go through different layers on purpose. The log goes
/// through `orbita_runtime::Disk`, so its path is relative to the node's data
/// root and the simulator can fault-inject it. The storage engine persists
/// through `ObjectStore`, which is a second seam the simulator can stand a
/// store into, and in production is either a bucket or the node's own
/// filesystem adapter.
#[derive(Clone)]
pub(crate) struct PartitionPaths {
    pub store: Arc<dyn ObjectStore>,
    pub path: PartitionPath,
    pub wal_dir: String,
    /// The size a log segment reaches before it is rolled. It belongs with the
    /// paths because it is the shape of the log on disk rather than a policy:
    /// a checkpoint drops whole segments, so this is what decides how much
    /// history an owner keeps after one.
    pub wal_segment_bytes: u64,
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
    /// Fires whenever a submitted write learns its Lamport or gives up on
    /// getting one, which is when a key stops being uncertain.
    ///
    /// A conditional write that finds the key it is about to decide on already
    /// in flight has nothing truthful to say about it yet, so it waits here
    /// rather than answering. One partition-wide signal instead of one per
    /// key: waking a handful of waiters that then re-check costs nothing
    /// beside the round trip they were waiting on, and a map of notifiers
    /// keyed by key would have to be reaped.
    settled: Arc<tokio::sync::Notify>,
    applies: tokio::sync::mpsc::UnboundedSender<ApplyWork>,
    drained: Arc<tokio::sync::Notify>,
    read_state: Mutex<ReplicaReadState>,
    leases: Mutex<LeaseTable>,
    /// Until this instant a previous process incarnation may have a lease out
    /// to a replica the current map no longer names.
    leases_uncertain_until_nanos: u64,
    lease: LeasePolicy,
    /// The peers this node replicates to, when it owns the partition.
    ///
    /// Mutable because the control plane places replicas onto a partition that
    /// is already owned and serving, and it does so without bumping the epoch.
    /// See [`PartitionHost::set_replicas`].
    replicas: Mutex<Vec<NodeId>>,
    /// Replicas this owner is willing to grant a lease to. A replica drops out
    /// when a renewal fails, because an owner that keeps granting to a node it
    /// cannot reach would wait out a lease on every write forever.
    grantable: Mutex<HashSet<NodeId>>,
    /// How far the owner has acknowledged to clients, as last heard. Nothing
    /// above this is applied on a replica.
    committed: Mutex<Lamport>,
    /// Entries that are durable here and waiting for that watermark.
    withheld: Mutex<VecDeque<WalEntry>>,
    /// Serialises releasing them, since the storage engine requires Lamport
    /// order and discards anything that arrives out of it.
    applying: tokio::sync::Mutex<()>,
    /// Serializes segment publication with the WAL checkpoint it permits.
    flushing: Arc<tokio::sync::Mutex<()>>,
    /// Whether this owner is still admitting writes. Closed for the duration of
    /// a split so the committed prefix the children inherit is final: no
    /// acknowledged write can land above it after the children are published.
    /// Per-partition rather than the node-wide drain gate, because a split
    /// quiesces one partition while the rest of the node keeps serving. See
    /// [`PartitionHost::quiesce_and_prepare_children`].
    admitting_writes: std::sync::atomic::AtomicBool,
    /// Whether this owner may issue parent read leases. Split preparation
    /// closes this before draining existing grants, so no retired parent can
    /// remain in the read set after its children activate.
    admitting_leases: std::sync::atomic::AtomicBool,
    /// Orders ordinary renewal passes against split lease drainage. Without
    /// this, an already-started positive renewal could arrive after the
    /// split's zero-duration revocation and resurrect the lease.
    lease_renewal: tokio::sync::Mutex<()>,
    /// Held for a write's whole duration as a read guard; a split takes the
    /// write guard to wait for every already-admitted write to resolve before
    /// it settles the log to the committed prefix. This is the same close-then-
    /// drain shape the node-wide handoff drain uses, scoped to one partition.
    /// Held shared by every write in flight and exclusively by a split.
    ///
    /// In an `Arc` so a write can take an owned guard and carry it onto the
    /// task that finishes its commit. A borrowed guard would be released by a
    /// cancelled request while its entry was still landing, which is exactly
    /// the window a split must not capture a horizon in.
    write_barrier: Arc<tokio::sync::RwLock<()>>,
}

impl<R: Runtime> PartitionHost<R> {
    /// Opens a partition this node owns: storage, log, and the replay of
    /// anything the log holds that storage has not applied.
    pub(crate) async fn open_owner(
        runtime: R,
        spec: HostSpec,
        paths: &PartitionPaths,
        replicas: Vec<NodeId>,
    ) -> Result<Arc<Self>> {
        let HostSpec {
            id, epoch, range, ..
        } = spec.clone();
        let storage = Arc::new(
            Partition::open(
                runtime.clone(),
                Arc::clone(&paths.store),
                paths.path.clone(),
                epoch,
                range,
            )
            .await?,
        );
        // What opening the storage engine built out of the bucket. On a node
        // that has held this partition all along it is the last manifest it
        // published; on a replacement it is the whole partition, downloaded
        // rather than copied from a peer. Either way it is where this node's
        // log has to think it starts, and its epoch is the second opinion on
        // whether the grant that brought us here is still current.
        let hydrated = hydration_of(&storage).await;
        let mut config = WalConfig::new(id, paths.wal_dir.clone(), epoch)
            .with_replicas(replicas.clone())
            .with_hydration(hydrated);
        config.segment_target_bytes = paths.wal_segment_bytes;
        let wal = Wal::open(runtime.clone(), config).await?;

        // A restart finds entries that were durable and never applied, because
        // the acknowledgement to the client came before the apply. Replaying
        // them is what makes that ordering safe. Hydration and this compose
        // rather than compete: the manifest covers everything up to its
        // horizon, and the log carries the tail above it.
        let recovery = wal.recover();
        // Entries the manifest already covers are skipped rather than applied.
        // `Partition::apply` ignores them anyway, but reading a value out of a
        // log to have it thrown away is work proportional to the retained log
        // rather than to the tail that matters.
        for entry in recovery
            .entries
            .iter()
            .filter(|e| e.lamport > hydrated.through)
        {
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
        Ok(Self::assemble(
            runtime,
            &spec,
            storage,
            Some(wal),
            log,
            replicas,
        ))
    }

    /// Opens a partition this node replicates but does not own.
    ///
    /// The log is opened here and handed to `WalService` rather than opened by
    /// it, because a node that is later promoted keeps the same log it was
    /// replicating into, and reopening a live log would run recovery a second
    /// time.
    pub(crate) async fn open_replica(
        runtime: R,
        spec: HostSpec,
        paths: &PartitionPaths,
    ) -> Result<Arc<Self>> {
        let id = spec.id;
        let storage = Arc::new(
            Partition::open(
                runtime.clone(),
                Arc::clone(&paths.store),
                paths.path.clone(),
                spec.epoch,
                spec.range.clone(),
            )
            .await?,
        );
        let hydrated = hydration_of(&storage).await;
        let log = PartitionLog::open(
            runtime.clone(),
            paths.wal_dir.clone(),
            id,
            paths.wal_segment_bytes,
        )
        .await?;
        // Adopted before anything is replayed, so this node reports the
        // position its data is actually at. A replica that hydrated from the
        // bucket and still claimed position zero would ask its owner to resend
        // writes the owner has checkpointed away, and would never catch up.
        log.hydrate(hydrated.through).await;
        // Entries the manifest already covers are skipped rather than applied.
        // `Partition::apply` would discard them anyway, but reading a value out
        // of a log to have it thrown away is work proportional to the retained
        // log rather than to the tail that matters.
        for entry in log
            .recovery()
            .entries
            .iter()
            .filter(|e| e.lamport > hydrated.through)
        {
            storage.apply(&mutation_of(entry)).await?;
        }

        let host = Self::assemble(runtime, &spec, storage, None, log, Vec::new());
        // The invalidation stream continues from where the log is, not from
        // where storage is. The two differ whenever entries are durable and
        // not yet applied, and starting from storage would make the next
        // invalidation look like a gap.
        let held = host.log.durable_lamport().await;
        *host.read_state.lock().expect("read state poisoned") = ReplicaReadState::new(held);
        Ok(host)
    }

    fn assemble(
        runtime: R,
        spec: &HostSpec,
        storage: Arc<Partition<R>>,
        wal: Option<Arc<Wal<R>>>,
        log: Arc<PartitionLog<R>>,
        replicas: Vec<NodeId>,
    ) -> Arc<Self> {
        let (applies, queue) = tokio::sync::mpsc::unbounded_channel();
        let pending = Arc::new(Mutex::new(PendingSet::default()));
        let drained = Arc::new(tokio::sync::Notify::new());
        let settled = Arc::new(tokio::sync::Notify::new());
        let flushing = Arc::new(tokio::sync::Mutex::new(()));
        // A same-epoch process restart cannot know which leases its previous
        // incarnation granted, including to a replica removed from the current
        // map. One full duration is the only conservative reconstruction.
        let leases_uncertain_until_nanos = wal.as_ref().map_or(0, |_| {
            runtime
                .clock()
                .monotonic_nanos()
                .saturating_add(spec.lease.duration.as_nanos() as u64)
        });

        // The applier holds weak references so that dropping a host retires
        // its storage engine there and then. A background task keeping the
        // engine alive would let a deposed incarnation keep applying after
        // its replacement has opened the same partition.
        runtime.spawn(apply_loop(
            spec.id,
            ApplyTarget {
                storage: Arc::downgrade(&storage),
                pending: Arc::downgrade(&pending),
                drained: Arc::downgrade(&drained),
                settled: Arc::downgrade(&settled),
                wal: wal.as_ref().map(Arc::downgrade),
                log: Arc::downgrade(&log),
                flushing: Arc::downgrade(&flushing),
                flush_on_trigger: wal.is_some(),
            },
            queue,
        ));

        Arc::new(Self {
            id: spec.id,
            epoch: spec.epoch,
            runtime,
            storage,
            wal,
            log,
            pending,
            submit: tokio::sync::Mutex::new(()),
            settled,
            applies,
            drained,
            read_state: Mutex::new(ReplicaReadState::new(Lamport::ZERO)),
            leases: Mutex::new(LeaseTable::default()),
            leases_uncertain_until_nanos,
            lease: spec.lease,
            grantable: Mutex::new(replicas.iter().copied().collect()),
            replicas: Mutex::new(replicas),
            committed: Mutex::new(Lamport::ZERO),
            withheld: Mutex::new(VecDeque::new()),
            applying: tokio::sync::Mutex::new(()),
            flushing,
            admitting_writes: std::sync::atomic::AtomicBool::new(true),
            admitting_leases: std::sync::atomic::AtomicBool::new(true),
            lease_renewal: tokio::sync::Mutex::new(()),
            write_barrier: Arc::new(tokio::sync::RwLock::new(())),
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

    /// Adopts a replica set the control plane placed after this host opened.
    ///
    /// A partition is born owned and unreplicated and gets its replicas a
    /// moment later, and that placement moves the map version without moving
    /// the epoch, because placement is not a change of ownership. An owner that
    /// only learned its peers at open time would go on acknowledging writes
    /// against an empty peer list, which
    /// [`orbita_wal::Wal::replicate`] treats as a quorum already met — the
    /// write is durable on one copy while the map promises three, and no
    /// replica ever advances far enough to be promoted or to take a read lease.
    ///
    /// Applied in place rather than by reopening the host: a reopen throws away
    /// in-flight writes, and a client cannot tell that apart from a failover it
    /// did nothing to deserve.
    ///
    /// Returns whether anything changed, so the caller can log a real
    /// transition rather than every poll.
    pub(crate) fn set_replicas(&self, replicas: &[NodeId]) -> bool {
        let Some(wal) = self.wal.as_ref() else {
            // A replica does not replicate onwards, so it has no peer list to
            // keep current.
            return false;
        };
        let mut held = self.replicas.lock().expect("replica set poisoned");
        if held.as_slice() == replicas {
            return false;
        }
        *held = replicas.to_vec();
        // A peer that has just been added is grantable until a renewal to it
        // fails; one that has been removed stops being offered new leases. The
        // lease table is deliberately left alone, so a lease already out to a
        // removed peer is still waited out rather than forgotten: the peer can
        // be serving reads under it, and forgetting it is how a stale read
        // happens.
        let mut grantable = self.grantable.lock().expect("grantable set poisoned");
        grantable.retain(|node| replicas.contains(node));
        grantable.extend(replicas.iter().copied());
        drop(grantable);
        drop(held);
        wal.set_replicas(replicas);
        true
    }

    /// Carries any replica that is behind up to the committed prefix.
    ///
    /// Called whenever this node notices an advertised copy that has not
    /// confirmed the prefix, and on every pass of a drain. Both are moments
    /// where a replica can be behind with no write coming to carry it forward,
    /// and where leaving it behind means the partition has fewer real copies
    /// than the map claims. A replica does nothing here: it has no peers of
    /// its own to feed.
    ///
    /// The horizon is deliberately the committed prefix rather than this
    /// node's durable position; see [`orbita_wal::Wal::catch_up_replicas`].
    pub(crate) async fn catch_up_replicas(&self) -> Result<CatchUpPass> {
        match self.wal.as_ref() {
            Some(wal) => wal.catch_up_replicas().await,
            None => Ok(CatchUpPass {
                horizon: Lamport::ZERO,
                caught_up: Vec::new(),
                behind: Vec::new(),
                stranded: Vec::new(),
            }),
        }
    }

    /// The advertised replicas a catch-up still owes a pass.
    ///
    /// This is what keeps a failed catch-up pending. It reads the same
    /// per-replica record that [`PartitionHost::replicas_beyond_retention`]
    /// reads, so the two cannot disagree about a node: one names the work a
    /// retry can still do and the other names the work it cannot. See
    /// [`orbita_wal::Wal::replicas_behind`].
    pub(crate) fn replicas_behind(&self) -> Vec<NodeId> {
        self.wal
            .as_ref()
            .map(|wal| wal.replicas_behind())
            .unwrap_or_default()
    }

    /// Closes this partition's log and drops the tail no client was told
    /// about, so that what it advertises is a position a replica can reach.
    ///
    /// See [`orbita_wal::Wal::quiesce`]. Only meaningful for an owner, and
    /// only correct once write admission is closed.
    pub(crate) async fn quiesce(&self) -> Result<()> {
        match self.wal.as_ref() {
            Some(wal) => wal.quiesce().await.map(|_| ()),
            None => Ok(()),
        }
    }

    /// The highest Lamport this owner's log handed back, or zero.
    ///
    /// Asked before a partition is reopened in place, because a reopen above
    /// this mark reissues versions a replica may still hold different bytes
    /// for, and only a higher epoch makes a replica give that tail up. See
    /// [`orbita_wal::Wal::surrendered_lamport`].
    pub(crate) fn surrendered_lamport(&self) -> Lamport {
        self.wal
            .as_ref()
            .map_or(Lamport::ZERO, |wal| wal.surrendered_lamport())
    }

    /// Quiesces this partition and durably prepares both split children over
    /// its segments, returning the committed prefix the children inherit.
    ///
    /// This is the worker's half of the ADR 0009 split, and the ordering is the
    /// write-loss guard. It closes write admission, waits for every already
    /// admitted write to resolve, then `quiesce`s the log — dropping the
    /// uncommitted tail whose clients were told `Unavailable`, exactly as a
    /// handoff drain does (#79/#87), so the position it settles to is the
    /// committed prefix and never the owner's local durable tail. It waits for
    /// the applier to carry storage up to that prefix, then prepares the
    /// children over it with no copy. Nothing above the returned horizon can be
    /// in the children, and nothing above it can be admitted afterwards, so no
    /// acknowledged write is lost across the split.
    ///
    /// Idempotent and safe to repeat: a re-run re-quiesces a quiesced log for
    /// free and re-publishes child manifests that already exist as a no-op.
    /// Only the owner runs it; a replica has no writes to quiesce and reaches
    /// the same child manifests through the shared bucket.
    pub(crate) async fn quiesce_and_prepare_children(
        &self,
        children: &[ChildSpec],
    ) -> Result<Lamport> {
        if self.wal.is_none() {
            return Err(Error::NotOwner {
                partition: self.id,
                owner: None,
            });
        }
        // Close write and lease admission, then wait out in-flight writes and
        // every lease under which a replica could still serve the parent.
        self.close_split_gates();
        let _barrier = self.write_barrier.write().await;
        self.drain_read_leases().await;
        // Give up the uncommitted tail so the log settles to the committed
        // prefix, then let the applier carry storage up to it before capturing.
        self.quiesce().await?;
        self.wait_for_applies().await;
        self.storage.prepare_child_partitions(children).await
    }

    /// Closes the split gates without doing preparation work.
    ///
    /// A recovered owner is put in this state before it enters the node's host
    /// table. The first authoritative active-intent fetch either keeps it
    /// closed or reopens it, so restart never exposes an unchecked parent.
    pub(crate) fn close_split_gates(&self) {
        self.admitting_writes
            .store(false, std::sync::atomic::Ordering::Release);
        self.admitting_leases
            .store(false, std::sync::atomic::Ordering::Release);
        self.freeze_maintenance();
    }

    /// Prepares child storage over the parent's segments *without* quiescing —
    /// no admission close, no log settle — so a test can show why the quiesce is
    /// load-bearing: a write acknowledged after the horizon this captures lands
    /// in the parent's still-live log, which no child inherits, and is lost when
    /// the parent retires. Only the real
    /// [`quiesce_and_prepare_children`](Self::quiesce_and_prepare_children) is
    /// used in production.
    #[cfg(test)]
    pub(crate) async fn prepare_children_without_quiescing(
        &self,
        children: &[ChildSpec],
    ) -> Result<Lamport> {
        self.storage.prepare_child_partitions(children).await
    }

    /// Freezes this partition's flush, compaction, and sweep because it is a
    /// split parent whose children reference its segments in place (ADR 0009).
    /// Set from the moment the worker sees the split intent, so it covers the
    /// whole window before the children publish and after, not just the
    /// quiesced sub-window. See [`orbita_storage::Partition::freeze_maintenance`].
    pub(crate) fn freeze_maintenance(&self) {
        self.storage.freeze_maintenance();
    }

    /// Whether this partition's maintenance is frozen for an in-progress split.
    pub(crate) fn is_maintenance_frozen(&self) -> bool {
        self.storage.is_maintenance_frozen()
    }

    /// Whether this owner is currently admitting writes. False while a split is
    /// quiescing it.
    pub(crate) fn is_admitting_writes(&self) -> bool {
        self.admitting_writes
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Whether this owner may currently issue read leases.
    pub(crate) fn is_admitting_leases(&self) -> bool {
        self.admitting_leases
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Whether this node might answer a read for `key` without asking the
    /// owner.
    ///
    /// The owner always may. A replica may only under a live lease with no gap
    /// in its invalidation stream and no invalidation outstanding for this
    /// key, which is the ADR 0001 read path. This is the first half of that
    /// decision, and the answer is provisional: [`PartitionHost::read`] checks
    /// again against what it actually read.
    pub(crate) fn might_serve(&self, key: &[u8]) -> bool {
        if self.is_owner() {
            return true;
        }
        let now = self.runtime.clock().monotonic_nanos();
        self.read_state
            .lock()
            .expect("read state poisoned")
            .may_serve(now, key)
            .is_some()
    }

    /// Reads a key, or reports that this node turned out not to be allowed to
    /// answer for it after all.
    ///
    /// A replica decides twice, before and after reading storage, because the
    /// two are not one step. Between them the owner can replicate a write for
    /// this key, be told it is durable, and acknowledge it to its client, and a
    /// reader that checked only beforehand would then answer with the value
    /// that write replaced. That is precisely the stale read ADR 0001 exists to
    /// prevent, and the deterministic simulator's linearizability checker found
    /// it here rather than anyone reasoning it out.
    pub(crate) async fn read(&self, key: &[u8]) -> Result<Read> {
        if self.is_owner() {
            return Ok(Read::Served(self.get(key).await?));
        }
        let now = self.runtime.clock().monotonic_nanos();
        let Some(generation) = self
            .read_state
            .lock()
            .expect("read state poisoned")
            .may_serve(now, key)
        else {
            return Ok(Read::MustForward);
        };

        let record = self.get(key).await?;

        let now = self.runtime.clock().monotonic_nanos();
        if !self
            .read_state
            .lock()
            .expect("read state poisoned")
            .still_serving(now, key, generation)
        {
            return Ok(Read::MustForward);
        }
        Ok(Read::Served(record))
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
        budget: ScanBudget,
    ) -> Result<ScanPage> {
        self.wait_for_applies().await;
        self.storage.scan(prefix, cursor, budget).await
    }

    /// The highest Lamport this partition has applied, which is what a replica
    /// reports about how caught up it is.
    pub(crate) async fn committed_lamport(&self) -> Result<Lamport> {
        self.storage.committed_lamport().await
    }

    /// The committed prefix this owner has established: the highest Lamport a
    /// durability quorum confirmed under its epoch.
    ///
    /// `None` on a replica, which has no quorum of its own to have confirmed
    /// anything. Reported beside the durable position rather than instead of
    /// it because they answer different questions: the durable position is
    /// the promotion input, and this is the watermark the no-lost-write
    /// promise is stated against. Only this one is safe to show an operator
    /// as the partition's position, because `quiesce` lowers the other.
    pub(crate) fn committed_prefix(&self) -> Option<Lamport> {
        self.wal.as_ref().map(|wal| wal.committed_lamport())
    }

    /// The highest Lamport this node has on stable storage for this partition.
    ///
    /// This is the promotion input rather than the applied Lamport, because an
    /// acknowledgement is paid for by durability and not by application, so
    /// this is what bounds the writes the cluster has promised.
    pub(crate) async fn durable_lamport(&self) -> Lamport {
        self.log.durable_lamport().await
    }

    /// Replicas of this partition that have fallen past what this owner's log
    /// still holds.
    ///
    /// Empty on a replica and on a healthy owner. A non-empty answer names a
    /// node that is out of the read set and out of the durability quorum and
    /// that no retry will recover, which is a state rather than a log line
    /// precisely so that something can be asked about it.
    pub(crate) fn replicas_beyond_retention(&self) -> Vec<orbita_wal::BeyondRetention> {
        self.wal
            .as_ref()
            .map_or_else(Vec::new, |wal| wal.beyond_retention())
    }

    /// The oldest Lamport this partition's log still holds.
    ///
    /// The retention floor, opposite `Wal::committed_lamport`'s ceiling: a
    /// replica can be carried from the log exactly when the entry it needs
    /// next is at or above this. Nothing in the running server asks — an owner
    /// reports the floor per stranded replica through
    /// [`PartitionHost::replicas_beyond_retention`], which is the answer an
    /// operator wants. This exists so a scenario can establish that a gap is
    /// genuinely past the log rather than merely large.
    #[cfg(test)]
    pub(crate) async fn retained_from(&self) -> Lamport {
        self.log.retained_from().await
    }

    /// How far this owner's advertised replicas trail its committed prefix.
    ///
    /// The WAL replication-lag signal, per partition. Empty on a replica, which
    /// has no committed prefix of its own to measure against. See
    /// [`orbita_wal::Wal::replication_lag`].
    pub(crate) fn replication_lag(&self) -> orbita_wal::ReplicationLag {
        self.wal
            .as_ref()
            .map(|wal| wal.replication_lag())
            .unwrap_or_default()
    }

    /// How much disk this partition is using, which is what the control plane
    /// compares against the split threshold.
    pub(crate) async fn size_bytes(&self) -> Result<u64> {
        self.storage.size_bytes().await
    }

    /// What this partition's memory-resident index costs on this node.
    ///
    /// Reported to the leader group because the node is the only place the
    /// number exists, and because ADR 0006 makes it the resource that runs
    /// out before disk does.
    pub(crate) async fn index_bytes(&self) -> Result<u64> {
        self.storage.index_bytes().await
    }

    /// Rebuilds this partition's storage from the manifest in the bucket and
    /// reports what that manifest says.
    ///
    /// This is the running-node half of hydration: opening a partition already
    /// reads the manifest, and this is what a replica that has fallen beyond its
    /// owner's retained log calls to catch up without a restart and without
    /// asking a healthy peer for a copy. The epoch comes back with the horizon
    /// because the caller is closing a gap on behalf of somebody claiming to
    /// own this partition, and the manifest is the only thing on this path that
    /// can contradict that claim.
    pub(crate) async fn hydrate(&self) -> Result<Hydration> {
        let found = self.storage.hydrate().await?;
        Ok(Hydration {
            epoch: found.epoch,
            through: found.through,
        })
    }

    /// Publishes every applied write and checkpoints only after the manifest
    /// swap is durable.
    pub(crate) async fn flush(&self) -> Result<()> {
        if !self.is_owner() {
            return Ok(());
        }
        self.wait_for_applies().await;
        flush_and_checkpoint(&self.storage, &self.log, &self.flushing, true).await
    }

    /// Compacts if enough has been flushed since the last one, and does nothing
    /// otherwise.
    ///
    /// Owner-only, because compaction publishes a manifest and only the owner
    /// may. Serialised against the flush path by the same `flushing` mutex: a
    /// compaction republishes the manifest a flush is also trying to swap, and
    /// the two racing is a lost compare-and-swap rather than a correctness
    /// problem, but there is no reason to pay for it.
    pub(crate) async fn compact_if_needed(&self) -> Result<()> {
        if !self.is_owner() {
            return Ok(());
        }
        let _ordered = self.flushing.lock().await;
        self.storage.compact_if_needed().await
    }

    /// Reclaims objects this partition no longer references and is safely done
    /// needing.
    ///
    /// A no-op on a replica: publishing and reclaiming a partition's objects is
    /// the owner's job, and a deposed writer that swept would race its
    /// replacement, exactly as it must not publish a manifest. See
    /// [`orbita_storage::Partition::sweep_orphans`] for the grace-period and
    /// clock-domain contract the deletion decision rests on.
    pub(crate) async fn sweep_orphans(
        &self,
        shared: &std::collections::BTreeSet<String>,
        grace_millis: u64,
        max_skew_millis: u64,
        dry_run: bool,
    ) -> Result<orbita_storage::SweepReport> {
        if !self.is_owner() {
            return Ok(orbita_storage::SweepReport {
                dry_run,
                ..Default::default()
            });
        }
        self.storage
            .sweep_orphans(shared, grace_millis, max_skew_millis, dry_run)
            .await
    }

    /// The segments this host references in place under other partitions'
    /// directories, grouped by the partition each lives under. See ADR 0009 and
    /// [`Node::sweep_owned`].
    pub(crate) async fn shared_segment_sources(
        &self,
    ) -> std::collections::BTreeMap<PartitionId, std::collections::BTreeSet<String>> {
        self.storage.shared_segment_sources().await
    }

    /// Evaluates the condition, commits through the log, and answers the
    /// client. Applying to storage happens afterwards, on the applier.
    ///
    /// A conditional write whose key already has a write in flight cannot be
    /// decided yet, and this waits for that write rather than refusing on the
    /// spot. See [`PartitionHost::await_settled`] for why that is the answer
    /// and not just a nicer error message.
    ///
    /// If that wait runs out the call fails [`Error::Unavailable`] rather than
    /// answering. A conditional write therefore has three outcomes on the
    /// wire, not two: applied, definitively not applied, and *undecided*. The
    /// third is retryable and is the only honest thing to say, because the
    /// owner never learned whether the caller lost. See
    /// [`CONDITION_SETTLE_TIMEOUT`].
    pub(crate) async fn write(
        self: &Arc<Self>,
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

        // A split quiesces this partition by closing admission and draining
        // in-flight writes. Checking before the barrier turns away a write that
        // arrives after the close; holding the barrier read guard for the
        // write's whole duration is what lets the quiesce wait for the ones
        // already admitted. Re-checking after acquiring closes the race where
        // the close landed between the two.
        if !self
            .admitting_writes
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(Error::Unavailable(format!(
                "partition {} is splitting and is not admitting writes",
                self.id
            )));
        }
        let admission = Arc::clone(&self.write_barrier).read_owned().await;
        if !self
            .admitting_writes
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(Error::Unavailable(format!(
                "partition {} is splitting and is not admitting writes",
                self.id
            )));
        }

        // The whole call gets one waiting budget, not one per attempt, so a
        // key under constant conditional traffic cannot hold a caller here
        // indefinitely by staying uncertain.
        //
        // The admission guard above is deliberately held across that wait. A
        // conditional write parked on an in-flight neighbour is an already
        // admitted write, and waiting for those to resolve is exactly what the
        // quiesce is for, so a split drains it rather than racing it. The wait
        // is bounded, so the drain is too.
        let settle_by = self
            .runtime
            .clock()
            .monotonic_nanos()
            .saturating_add(CONDITION_SETTLE_TIMEOUT.as_nanos() as u64);

        let (guard, overlay, visible, now) = loop {
            let guard = self.submit.lock().await;
            let now = self.runtime.clock().now_millis();

            let overlay = self
                .pending
                .lock()
                .expect("pending set poisoned")
                .overlay(&key);
            let committed = self.storage.get(&key).await?;
            let visible = pending::visible(committed, &overlay, now);

            // An unconditional write does not read the key, so nothing about
            // it is uncertain and it never waits.
            if !overlay.uncertain || matches!(condition, WriteCondition::None) {
                break (guard, overlay, visible, now);
            }
            let remaining = Duration::from_nanos(
                settle_by.saturating_sub(self.runtime.clock().monotonic_nanos()),
            );
            if remaining.is_zero() {
                // Undecided, and said as such. `visible` here is state from
                // outside the in-flight window, so evaluating against it would
                // manufacture a definitive `applied: false` out of a question
                // this node never got an answer to — the exact dishonesty the
                // wait exists to remove, on a slower path. The write parked on
                // may still fail, in which case the caller never lost, or land
                // with a version this response would have got wrong.
                drop(guard);
                tracing::warn!(
                    partition = self.id.get(),
                    "a conditional write is undecided: the in-flight write to the same key did \
                     not settle within the budget"
                );
                return Err(Error::Unavailable(format!(
                    "partition {} still has a write in flight on this key after {} seconds, so \
                     the condition is undecided; retry",
                    self.id,
                    CONDITION_SETTLE_TIMEOUT.as_secs()
                )));
            }
            // Released first, and deliberately. Waiting under the submission
            // lock would hold a lock across replication, which is the one
            // thing ADR 0003 forbids; only this caller waits, and every other
            // write to the partition keeps going.
            drop(guard);
            self.await_settled(&key, remaining).await;
        };

        if let Some(failure) = evaluate(condition, visible.as_ref(), overlay.uncertain) {
            // Naming a version is as good as acknowledging the write that
            // produced it, and the version being reported here may still be
            // sitting in the overlay, committed but not yet invalidated on the
            // replicas. So it waits for the same coherence an applied write
            // waits for. Otherwise a client told which version holds the lock
            // could turn around, read a replica, and be told the lock is free.
            let revealing = overlay.top.as_ref().map(|(lamport, _)| *lamport);
            drop(guard);
            if let Some(lamport) = revealing {
                self.await_coherence(wal, lamport).await;
            }
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
        // Owns its log handle rather than borrowing this call's, because the
        // commit outlives the request: it is driven to completion on the task
        // below whether or not the caller is still here.
        // The Lamport is assigned here, under the submission lock, which is
        // what orders the applier. Nothing in this call awaits between taking
        // the lock and handing the work on, so a cancelled request cannot leave
        // a submission half made.
        let lamport = match wal.submit(wal_op.clone()) {
            Ok(lamport) => lamport,
            Err(error) => {
                self.pending
                    .lock()
                    .expect("pending set poisoned")
                    .abandon(&ticket);
                self.settled.notify_waiters();
                return Err(error);
            }
        };

        // Handed to the applier at submission rather than on completion. The
        // applier waits for the commit itself and resolves the overlay and
        // applies to storage whether or not this request is still here, so a
        // dropped request loses its answer and nothing else. Doing that work on
        // the request meant a cancelled one left its entry committed in the log
        // and never applied, and the owner served the value underneath its own
        // committed prefix. Reserving the slot in submission order is what
        // makes the applier apply in Lamport order.
        let _ = self.applies.send(ApplyWork {
            ticket,
            lamport,
            record: pending_record_of(&op, lamport, now),
            mutation: mutation_of_op(lamport, &key, &wal_op),
        });
        drop(guard);

        // Only the waiting is this request's.
        let outcome = wal.wait_for(lamport).await;
        // The write is durable at quorum by here, so its Lamport is in the
        // committed prefix a split would capture: releasing the admission guard
        // now still keeps a concurrent quiesce from capturing a horizon below
        // this write, while not holding it across the coherence wait, which can
        // take a lease interval and would otherwise stall a racing split.
        drop(admission);

        match outcome {
            Ok(lamport) => {
                // Durable is not enough to answer the client. Every replica
                // that could still serve a read has to have the invalidation
                // too, or the client would be told about a value another node
                // is still hiding.
                self.await_coherence(wal, lamport).await;
                Ok(WriteAck {
                    applied: true,
                    version: Some(Version(lamport.get())),
                    current_version: None,
                    existed,
                })
            }
            Err(error) => Err(error),
        }
    }

    /// Renews every replica's read lease, which is the owner's half of the
    /// ADR 0001 read path.
    ///
    /// A lease is recorded before it is sent and dropped only when the replica
    /// says in as many words that it did not take it. A renewal whose reply is
    /// lost may well have been received, and an owner that assumed otherwise
    /// would stop waiting for a replica that is still serving reads.
    ///
    /// A replica whose renewal fails stops being granted anything, and is sent
    /// a zero-length grant instead. That doubles as a probe: it takes no lease,
    /// so an answer to it proves the replica has none, which is what lets the
    /// owner start granting again.
    pub(crate) async fn renew_leases(&self) {
        let Some(wal) = self.wal.as_ref() else {
            return;
        };
        let _renewal = self.lease_renewal.lock().await;
        let replicas = self.replicas.lock().expect("replica set poisoned").clone();
        if replicas.is_empty() {
            return;
        }
        // Where the log stands now. A replica takes the lease only if it is
        // past this, which is what stops it serving a key it has not been told
        // about. Reading it before the calls go out is the conservative order:
        // anything committed after this only makes the offer harder to accept.
        let through = wal.durable_lamport();
        // What clients have been told about, which is what a replica may
        // apply. Read after the durable position so it can never name a
        // Lamport the replica has not been offered.
        let committed = wal.committed_lamport();
        let epoch = self.epoch;

        let granting = self.is_admitting_leases();
        let renewals: Vec<_> = replicas
            .iter()
            .map(|node| self.renew_one(*node, epoch, through, committed, granting))
            .collect();
        join_all(renewals).await;
    }

    async fn renew_one(
        &self,
        node: NodeId,
        epoch: Epoch,
        through: Lamport,
        committed: Lamport,
        admission_open: bool,
    ) {
        let granting = admission_open
            && self
                .grantable
                .lock()
                .expect("grantable set poisoned")
                .contains(&node);
        let duration = if granting {
            self.lease.duration
        } else {
            Duration::ZERO
        };

        let sent = self.runtime.clock().monotonic_nanos();
        if granting {
            // The owner counts the lease from when it sent the offer, which is
            // earlier than the replica counts its own from. That ordering is
            // what makes the replica give up first.
            self.leases
                .lock()
                .expect("lease table poisoned")
                .grant(node, sent, duration);
        }

        let call = PeerCall {
            service: ServiceId::Proxy,
            method: proxy::METHOD_LEASE,
            payload: LeaseGrant {
                partition: self.id,
                epoch,
                through,
                committed,
                duration_millis: duration.as_millis() as u64,
            }
            .encode(),
        };

        let answered = self
            .runtime
            .transport()
            .call(node, call)
            .await
            .map_err(|e| Error::Unavailable(e.to_string()))
            .and_then(|reply| proxy::decode_lease_reply(&reply));

        // Where the replica said its log ends, whatever it decided about the
        // lease. This is the heartbeat's second job and the reason an owner
        // that has replicated nothing since it opened still knows which of its
        // replicas it can catch up. See `Wal::note_replica_position`.
        if let (Ok(reply), Some(wal)) = (&answered, self.wal.as_ref()) {
            if let Some(durable) = reply.durable {
                wal.note_replica_position(node, durable).await;
            }
        }

        match answered {
            Ok(reply) if reply.accepted && granting => {}
            Ok(reply) => {
                // Either the replica refused the lease, or this was a probe
                // and it has confirmed it holds none. Both mean nothing there
                // can serve a stale read, so the wait can stop counting it.
                self.leases
                    .lock()
                    .expect("lease table poisoned")
                    .revoke(node);

                // Putting it back in the read set is a different claim, and it
                // does not follow from the same evidence. Answering proves the
                // replica is reachable; it does not prove it can keep up, and
                // a replica that cannot keep up is exactly the one that will
                // accept a lease and then miss the invalidation for the next
                // write, stalling that write until its lease runs out.
                //
                // ADR 0001 bounds that at "one lease duration, once, and then
                // the replica is out of the read set entirely". Re-admitting on
                // reachability alone breaks the "once": the coherence timeout
                // evicts the replica, the next heartbeat probes it, the probe
                // is answered, it is grantable again within one renewal
                // interval (a third of a lease), it takes a fresh lease, falls
                // behind again, and stalls the next write. That loop is issue
                // #136 -- a measured 1,362 lease-duration stalls in a single
                // benchmark run, where the design says there should be one per
                // replica that goes bad.
                //
                // So re-admit only on evidence of having caught up. The bar is
                // the committed prefix rather than the owner's local log end:
                // committed is what a linearizable read has to reflect, it is
                // reachable while the owner keeps writing, and a replica that
                // holds it is one the next lease will not immediately strand.
                // A replica that stays behind keeps serving durability and
                // keeps receiving replication; it just stops taking reads
                // until it can carry them, which is the trade ADR 0001 makes.
                if readmissible_to_read_set(reply.durable, committed) {
                    self.grantable
                        .lock()
                        .expect("grantable set poisoned")
                        .insert(node);
                }
            }
            Err(error) => {
                tracing::debug!(
                    partition = self.id.get(),
                    node = node.get(),
                    %error,
                    "renewing a read lease failed"
                );
                self.grantable
                    .lock()
                    .expect("grantable set poisoned")
                    .remove(&node);
            }
        }
    }

    /// Revokes every parent read lease and waits out any replica that could not
    /// confirm revocation. The wait derives from the actual grants in the
    /// owner's lease table rather than a guessed sleep.
    async fn drain_read_leases(&self) {
        let Some(wal) = self.wal.as_ref() else {
            return;
        };
        let _renewal = self.lease_renewal.lock().await;
        self.wait_for_possible_restart_leases().await;
        let replicas = self.replicas.lock().expect("replica set poisoned").clone();
        let through = wal.durable_lamport();
        let committed = wal.committed_lamport();
        let renewals: Vec<_> = replicas
            .iter()
            .map(|node| self.renew_one(*node, self.epoch, through, committed, false))
            .collect();
        join_all(renewals).await;

        loop {
            let now = self.runtime.clock().monotonic_nanos();
            let until = self
                .leases
                .lock()
                .expect("lease table poisoned")
                .holders_with_expiry(now)
                .into_iter()
                .map(|(_, until)| until)
                .max();
            let Some(until) = until else { return };
            self.runtime
                .clock()
                .sleep(Duration::from_nanos(until.saturating_sub(now)))
                .await;
        }
    }

    /// Waits for the writes already in flight on `key` to come back from the
    /// log, so a condition against it can be decided on facts.
    ///
    /// A key with an unresolved write has no version yet, because the Lamport
    /// is the version and the log hands it back at the end of the round trip.
    /// Refusing every condition in that window keeps two compare-and-swaps
    /// against one version from both winning, which is the property that
    /// matters, but it answers a losing `if_not_present` with no version to
    /// report — and the same loser arriving a moment later would have been
    /// told exactly which version beat it. A client cannot control whether its
    /// contention was concurrent or sequential, so it should not be able to
    /// see the difference.
    ///
    /// Waiting removes the difference instead of papering over it, and it is
    /// the only mechanism that stays honest in both directions. The in-flight
    /// write either lands, in which case its version is the true answer to
    /// "who holds this", or it fails, in which case it was never acknowledged,
    /// must not be visible to anyone, and the caller is free to win. Guessing
    /// the version the pending write is about to get would answer the first
    /// case and lie about the second.
    ///
    /// Returning is not the same as settling. The budget can expire with the
    /// key still uncertain, and the caller re-checks rather than trusting
    /// this, because expiry is not an answer to the condition — see
    /// [`CONDITION_SETTLE_TIMEOUT`] for what the caller does with it instead.
    ///
    /// This must not be called with the submission lock held. It waits on a
    /// replication round trip, and ADR 0003 exists precisely to keep that out
    /// from under a lock.
    async fn await_settled(&self, key: &Bytes, budget: Duration) {
        let notified = self.settled.notified();
        let mut notified = std::pin::pin!(notified);
        // Registered before the check, so a write that resolves between the
        // two is not missed and the wait cannot park on a key that has already
        // settled.
        notified.as_mut().enable();

        if !self
            .pending
            .lock()
            .expect("pending set poisoned")
            .overlay(key)
            .uncertain
        {
            return;
        }

        // The expiry is not logged here. A budget that runs out on a key that
        // settled a moment earlier is a non-event, and only the caller — which
        // re-reads the overlay — can tell that case from the one worth
        // warning about.
        let _ = timeout(self.runtime.clock(), budget, notified).await;
    }

    /// Waits until no replica that could still be serving reads is missing
    /// this write.
    ///
    /// This is the coherence quorum, which ADR 0001 keeps separate from the
    /// durability quorum. Durability asks whether the write survives and is
    /// satisfied by two of three; coherence asks whether anyone can still hand
    /// out the old value, and that is a question about named lease holders.
    ///
    /// The wait is bounded by the leases themselves. A replica that does not
    /// answer stops being a lease holder when its lease runs out, so the worst
    /// case is one lease duration, once, and then the replica is out of the
    /// read set. A newly opened owner also waits one duration because its prior
    /// process may have granted a lease to a replica the current map no longer
    /// names.
    /// How many fsyncs this partition's log has issued, and how many entries
    /// they carried. `None` for a replica, which has no owning log.
    ///
    /// Exposed so a test can assert the write path actually batches rather
    /// than infer it from throughput, which is what let issue #147 sit
    /// undetected behind a plausible-looking flush loop.
    #[cfg(test)]
    #[must_use]
    pub fn flush_stats(&self) -> Option<(u64, u64)> {
        self.wal.as_ref().map(|wal| wal.flush_stats())
    }

    async fn await_coherence(&self, wal: &Arc<Wal<R>>, lamport: Lamport) {
        self.wait_for_possible_restart_leases().await;
        loop {
            let now = self.runtime.clock().monotonic_nanos();
            let behind: Vec<(NodeId, u64)> = self
                .leases
                .lock()
                .expect("lease table poisoned")
                .holders_with_expiry(now)
                .into_iter()
                .filter(|(node, _)| wal.acked_through(*node) < lamport)
                .collect();
            if behind.is_empty() {
                return;
            }

            let nodes: Vec<NodeId> = behind.iter().map(|(node, _)| *node).collect();
            let until = behind
                .iter()
                .map(|(_, until)| *until)
                .max()
                .expect("the list is not empty");
            let wait = Duration::from_nanos(until.saturating_sub(now));

            match timeout(
                self.runtime.clock(),
                wait,
                wal.wait_until_acked(&nodes, lamport),
            )
            .await
            {
                Ok(Ok(())) => return,
                // The log has given up on this owner. The replicas are being
                // fenced by whoever replaced it, which takes their leases with
                // it, so there is nothing left to wait for.
                Ok(Err(_)) => return,
                Err(_) => {
                    tracing::warn!(
                        partition = self.id.get(),
                        lamport = lamport.get(),
                        "waited out a read lease for a replica that did not acknowledge"
                    );
                    let mut leases = self.leases.lock().expect("lease table poisoned");
                    let mut grantable = self.grantable.lock().expect("grantable set poisoned");
                    for node in nodes {
                        leases.revoke(node);
                        grantable.remove(&node);
                    }
                }
            }
        }
    }

    pub(crate) async fn wait_for_possible_restart_leases(&self) {
        let now = self.runtime.clock().monotonic_nanos();
        if now < self.leases_uncertain_until_nanos {
            self.runtime
                .clock()
                .sleep(Duration::from_nanos(
                    self.leases_uncertain_until_nanos - now,
                ))
                .await;
        }
    }

    /// Takes a read lease this partition's owner offered, or refuses it.
    ///
    /// Refusing is the answer whenever anything is unclear, because a replica
    /// that holds no lease costs one extra hop per read and a replica that
    /// holds one it should not have breaks linearizability.
    ///
    /// The same message carries how far the owner has acknowledged, which is
    /// what releases entries this node is holding back.
    pub(crate) async fn accept_lease(&self, grant: &LeaseGrant) -> LeaseReply {
        // Answered even when the grant is refused, because where this node's
        // log ends is what the owner needs most from a replica it cannot
        // grant to.
        let durable = Some(self.log.durable_lamport().await);
        if self.is_owner() || grant.epoch < self.epoch {
            return LeaseReply {
                accepted: false,
                durable,
            };
        }
        self.commit_through(grant.committed).await;

        let now = self.runtime.clock().monotonic_nanos();
        let accepted = self
            .read_state
            .lock()
            .expect("read state poisoned")
            .accept_grant(
                now,
                grant.through,
                Duration::from_millis(grant.duration_millis),
                self.lease.margin,
            );
        LeaseReply { accepted, durable }
    }

    /// Releases everything the owner has acknowledged to a client.
    ///
    /// A replica makes an entry durable long before the owner decides the
    /// write succeeded, so applying on durability alone would put a write into
    /// this node's storage that no client was ever told about. A read that saw
    /// one, followed by a read that did not, is a history no sequential order
    /// explains, which is what the simulator's linearizability checker
    /// reported before this existed.
    pub(crate) async fn commit_through(&self, committed: Lamport) {
        {
            let mut held = self.committed.lock().expect("committed watermark poisoned");
            if committed <= *held {
                return;
            }
            *held = committed;
        }
        self.release_applies().await;
    }

    /// Applies whatever is now below the watermark, in Lamport order.
    ///
    /// One at a time, because the storage engine ignores a mutation at or
    /// below its committed Lamport, so an apply that runs out of order is
    /// discarded rather than merely late.
    async fn release_applies(&self) {
        let _ordered = self.applying.lock().await;
        loop {
            let committed = *self.committed.lock().expect("committed watermark poisoned");
            let next = {
                let mut waiting = self.withheld.lock().expect("withheld entries poisoned");
                match waiting.front() {
                    Some(entry) if entry.lamport <= committed => {
                        waiting.pop_front().expect("the front is there")
                    }
                    _ => return,
                }
            };

            let mutation = mutation_of(&next);
            if let Err(error) = self.storage.apply(&mutation).await {
                tracing::error!(
                    partition = self.id.get(),
                    lamport = next.lamport.get(),
                    %error,
                    "applying a replicated entry to storage failed"
                );
                continue;
            }
            if let Err(error) = self.storage.reclaim_published_if_needed().await {
                tracing::warn!(
                    partition = self.id.get(),
                    %error,
                    "replica could not reclaim entries covered by the published manifest"
                );
            }
            self.read_state
                .lock()
                .expect("read state poisoned")
                .applied(&mutation.key, mutation.lamport);
        }
    }

    /// Records that the owner has told this replica a key is changing.
    pub(crate) fn invalidate(&self, key: Bytes, lamport: Lamport) {
        let outstanding = {
            let mut state = self.read_state.lock().expect("read state poisoned");
            state.invalidate(key, lamport);
            state.invalid_len()
        };
        // The invalid set is bounded by replication lag rather than by the
        // size of the keyspace, so it stays small when things are healthy.
        // Growth means this replica is falling behind its owner, and it shows
        // up here before it shows up as a read that forwards.
        if outstanding > INVALID_SET_WARN {
            tracing::warn!(
                partition = self.id.get(),
                outstanding,
                "this replica is holding an unusual number of unreadable keys"
            );
        }
    }

    /// A newer owner said this replica's history ends at `above`.
    ///
    /// The withheld queue is cut to match, and that half is load-bearing rather
    /// than tidy. An entry sits withheld from the moment it is durable until
    /// the owner says it was acknowledged, so a tail the log just dropped is
    /// still queued here — waiting on a commit watermark that is never coming.
    /// The new owner resumes from the cut and reissues those Lamports with
    /// different bytes, and both copies would end up in this queue. Applies run
    /// in Lamport order and storage ignores a mutation at or below its
    /// committed Lamport, so the stale copy would apply first and the
    /// replacement would be silently discarded — this replica would serve the
    /// value the cluster threw away.
    pub(crate) fn truncated(&self, above: Lamport) {
        self.read_state
            .lock()
            .expect("read state poisoned")
            .truncated(above);
        self.withheld
            .lock()
            .expect("withheld entries poisoned")
            .retain(|entry| entry.lamport <= above);
    }

    /// Takes an entry this node received as a replica.
    ///
    /// It is queued rather than applied. The key stays unreadable here until
    /// the owner says the write was acknowledged, so a read of it forwards in
    /// the meantime, which is the conservative direction.
    pub(crate) async fn apply_replicated(&self, entry: &WalEntry) -> Result<()> {
        self.withheld
            .lock()
            .expect("withheld entries poisoned")
            .push_back(entry.clone());
        self.release_applies().await;
        Ok(())
    }

    /// How many replicated entries are durable here and not yet released.
    ///
    /// Bounded by how far ahead the owner is of its own acknowledgements,
    /// which is one heartbeat in the healthy case and a useful signal when it
    /// is not.
    #[allow(dead_code)]
    pub(crate) fn withheld_len(&self) -> usize {
        self.withheld
            .lock()
            .expect("withheld entries poisoned")
            .len()
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
struct ApplyTarget<R: Runtime> {
    storage: Weak<Partition<R>>,
    pending: Weak<Mutex<PendingSet>>,
    drained: Weak<tokio::sync::Notify>,
    settled: Weak<tokio::sync::Notify>,
    wal: Option<Weak<Wal<R>>>,
    log: Weak<PartitionLog<R>>,
    flushing: Weak<tokio::sync::Mutex<()>>,
    flush_on_trigger: bool,
}

/// One submitted write, handed to the applier at submission rather than on
/// completion.
///
/// Everything here is known under the submission lock, which is the point: the
/// applier can finish this write without the request that started it. A request
/// that goes away loses its answer and nothing else.
struct ApplyWork {
    ticket: pending::Ticket,
    lamport: Lamport,
    record: PendingRecord,
    mutation: Mutation,
}

async fn apply_loop<R: Runtime>(
    partition: PartitionId,
    target: ApplyTarget<R>,
    mut queue: tokio::sync::mpsc::UnboundedReceiver<ApplyWork>,
) {
    while let Some(work) = queue.recv().await {
        // The applier waits for the commit rather than the request doing it.
        // A request only ever owned the answer; letting it own the resolution
        // too meant a cancelled one left its entry committed in the log and
        // never applied here, so the owner served the value underneath its own
        // committed prefix and a restart changed the answer. Waiting here costs
        // nothing extra: this task already exists per partition and already
        // had to run before the write was visible.
        let committed = match &target.wal {
            Some(wal) => match wal.upgrade() {
                Some(wal) => wal.wait_for(work.lamport).await.is_ok(),
                // The partition closed. Anything unapplied is in the log and
                // recovery replays it.
                None => return,
            },
            // No log of its own, so nothing to wait for.
            None => true,
        };

        let (Some(pending), Some(settled)) = (target.pending.upgrade(), target.settled.upgrade())
        else {
            return;
        };
        if committed {
            pending.lock().expect("pending set poisoned").resolve(
                &work.ticket,
                work.lamport,
                work.record,
            );
        } else {
            // Nothing landed, so the key is back to whatever it was. A waiter
            // parked on this write must be released to win rather than left
            // losing to a write that never happened.
            pending
                .lock()
                .expect("pending set poisoned")
                .abandon(&work.ticket);
        }
        // The key has a version again, so anyone who parked on it can now be
        // told the truth about it.
        settled.notify_waiters();
        if !committed {
            continue;
        }
        let mutation = work.mutation;
        let (Some(storage), Some(pending)) = (target.storage.upgrade(), target.pending.upgrade())
        else {
            // The partition closed underneath us. Anything unapplied is still
            // in the log, and recovery replays it.
            return;
        };
        let applied = match storage.apply(&mutation).await {
            Ok(()) => true,
            Err(error) => {
                tracing::error!(
                    partition = partition.get(),
                    lamport = mutation.lamport.get(),
                    %error,
                    "applying an acknowledged write to storage failed"
                );
                false
            }
        };
        if applied && target.flush_on_trigger {
            if let (Some(log), Some(flushing)) = (target.log.upgrade(), target.flushing.upgrade()) {
                if let Err(error) = flush_and_checkpoint(&storage, &log, &flushing, false).await {
                    tracing::warn!(
                        partition = partition.get(),
                        %error,
                        "size-triggered flush failed; the WAL remains replayable"
                    );
                }
            }
        }
        pending
            .lock()
            .expect("pending set poisoned")
            .applied(&mutation.key, mutation.lamport);
        if let Some(drained) = target.drained.upgrade() {
            drained.notify_waiters();
        }
    }
}

/// Keeps the manifest horizon and WAL checkpoint in their required order.
async fn flush_and_checkpoint<R: Runtime>(
    storage: &Partition<R>,
    log: &PartitionLog<R>,
    flushing: &tokio::sync::Mutex<()>,
    force: bool,
) -> Result<()> {
    let _ordered = flushing.lock().await;
    if force {
        storage.flush().await?;
    } else {
        storage.flush_if_needed().await?;
    }

    let flushed = storage.flushed_lamport().await?;
    // A node may open a manifest ahead of the WAL it retained, which is the
    // normal case for a replacement worker. Hydration is what reconciles the
    // two: opening the partition records the manifest horizon in the log, so
    // `durable_lamport` already accounts for the writes that live only in the
    // segments. The cap stays because a log that was not hydrated, or that was
    // hydrated to a lower horizon, must still not have a checkpoint claim a
    // position it cannot back.
    let checkpoint = flushed.min(log.durable_lamport().await);
    if checkpoint > log.applied_through().await {
        log.checkpoint(checkpoint).await?;
    }
    Ok(())
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

/// Whether a replica that answered a heartbeat without holding a lease may be
/// offered one again.
///
/// Split out from the renewal so the rule can be read and tested on its own,
/// because it is the difference between ADR 0001's "one lease duration, once"
/// and a stall on every heartbeat.
///
/// `durable` is where the replica said its log ends. `None` means a peer too
/// old to report it, which is treated as not knowing rather than as good news,
/// so it stays out of the read set. That is the safe direction: the cost is
/// read capacity from a peer that cannot describe itself, and the alternative
/// is granting a lease on no evidence at all.
fn readmissible_to_read_set(durable: Option<Lamport>, committed: Lamport) -> bool {
    durable.is_some_and(|durable| durable >= committed)
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
        //
        // The write path never reaches this arm any more: it waits the window
        // out, and if the wait expires it fails the call `Unavailable` rather
        // than deciding. The arm stays because the property it protects —
        // never letting two swaps against one version both win — must not
        // depend on a caller upstream remembering to check first.
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

/// What the manifest the storage engine already adopted says, in the shape the
/// log crate speaks.
///
/// The two crates each name this value for themselves rather than sharing a
/// type, for the same reason [`mutation_of`] exists: neither depends on the
/// other, the vocabulary crate is frozen, and this host is the one place they
/// meet. Reading it costs nothing because [`Partition::open`] read the manifest
/// on the way in.
async fn hydration_of<R: Runtime>(storage: &Partition<R>) -> Hydration {
    let found = storage.hydration().await;
    Hydration {
        epoch: found.epoch,
        through: found.through,
    }
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

    use async_trait::async_trait;
    use orbita_core::{KeyspaceId, NodeId};
    use orbita_format::segment::Segment;
    use orbita_format::testing::MemoryStore;
    use orbita_format::{load_manifest, PartitionPath};
    use orbita_objectstore::{ETag, ObjectError, ObjectMeta, ObjectResult, Precondition};
    use orbita_sim::{SimRuntime, Simulation};
    use std::ops::Range;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    fn record(version: u64) -> Record {
        Record {
            value: Bytes::from_static(b"v"),
            version: Version(version),
            expires_at_millis: None,
        }
    }

    struct FaultStore {
        inner: MemoryStore,
        fail_segment: AtomicBool,
        fail_manifest: AtomicBool,
        range_reads: AtomicUsize,
    }

    impl FaultStore {
        fn new() -> Self {
            Self {
                inner: MemoryStore::new(),
                fail_segment: AtomicBool::new(false),
                fail_manifest: AtomicBool::new(false),
                range_reads: AtomicUsize::new(0),
            }
        }

        fn fail_next_segment(&self) {
            self.fail_segment.store(true, Ordering::Release);
        }

        fn fail_next_manifest(&self) {
            self.fail_manifest.store(true, Ordering::Release);
        }
    }

    #[async_trait]
    impl ObjectStore for FaultStore {
        async fn put(&self, key: &str, data: Bytes) -> ObjectResult<ETag> {
            if key.contains("/segments/") && self.fail_segment.swap(false, Ordering::AcqRel) {
                return Err(ObjectError::Transient(
                    "injected segment failure".to_string(),
                ));
            }
            self.inner.put(key, data).await
        }

        async fn put_if(
            &self,
            key: &str,
            data: Bytes,
            precondition: Precondition,
        ) -> ObjectResult<ETag> {
            if key.ends_with("manifest.json") && self.fail_manifest.swap(false, Ordering::AcqRel) {
                return Err(ObjectError::Transient(
                    "injected manifest failure".to_string(),
                ));
            }
            self.inner.put_if(key, data, precondition).await
        }

        async fn get(&self, key: &str) -> ObjectResult<(Bytes, ETag)> {
            self.inner.get(key).await
        }

        async fn get_range(&self, key: &str, range: Range<u64>) -> ObjectResult<Bytes> {
            self.range_reads.fetch_add(1, Ordering::Relaxed);
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

    fn partition_path() -> PartitionPath {
        PartitionPath::new("", KeyspaceId(1), PartitionId(1))
    }

    fn start_host(
        sim: &Simulation,
        runtime: SimRuntime,
        store: Arc<FaultStore>,
    ) -> Arc<PartitionHost<SimRuntime>> {
        let paths = PartitionPaths {
            store,
            path: partition_path(),
            wal_dir: "wal/p1".to_string(),
            wal_segment_bytes: crate::DEFAULT_WAL_SEGMENT_BYTES,
        };
        sim.block_on(async move {
            PartitionHost::open_owner(
                runtime,
                HostSpec {
                    id: PartitionId(1),
                    epoch: Epoch(1),
                    range: KeyRange::unbounded(),
                    lease: LeasePolicy::default(),
                },
                &paths,
                Vec::new(),
            )
            .await
            .expect("the owner opens")
        })
    }

    fn start_replica(
        sim: &Simulation,
        runtime: SimRuntime,
        store: Arc<FaultStore>,
    ) -> Arc<PartitionHost<SimRuntime>> {
        let paths = PartitionPaths {
            store,
            path: partition_path(),
            wal_dir: "wal/replica-p1".to_string(),
            wal_segment_bytes: crate::DEFAULT_WAL_SEGMENT_BYTES,
        };
        sim.block_on(async move {
            PartitionHost::open_replica(
                runtime,
                HostSpec {
                    id: PartitionId(1),
                    epoch: Epoch(1),
                    range: KeyRange::unbounded(),
                    lease: LeasePolicy::default(),
                },
                &paths,
            )
            .await
            .expect("the replica opens")
        })
    }

    fn write_one(sim: &Simulation, host: &Arc<PartitionHost<SimRuntime>>) {
        let host = Arc::clone(host);
        sim.block_on(async move {
            host.write(
                Bytes::from_static(b"key"),
                WriteOp::Put {
                    value: Bytes::from_static(b"value"),
                    ttl_millis: None,
                },
                WriteCondition::None,
            )
            .await
            .expect("the WAL commit succeeds")
        });
    }

    fn publish_horizon(
        sim: &Simulation,
        runtime: SimRuntime,
        store: Arc<FaultStore>,
        horizon: Lamport,
    ) {
        publish_keys(sim, runtime, store, &[(horizon, "published")]);
    }

    /// Publishes a manifest holding these keys at these Lamports, which is what
    /// a node hydrating this partition would find in the bucket.
    fn publish_keys(
        sim: &Simulation,
        runtime: SimRuntime,
        store: Arc<FaultStore>,
        keys: &[(Lamport, &str)],
    ) {
        let keys: Vec<(Lamport, Bytes)> = keys
            .iter()
            .map(|(at, key)| (*at, Bytes::copy_from_slice(key.as_bytes())))
            .collect();
        sim.block_on(async move {
            let partition = Partition::open(
                runtime,
                store as Arc<dyn ObjectStore>,
                partition_path(),
                Epoch(1),
                KeyRange::unbounded(),
            )
            .await
            .unwrap();
            for (at, key) in keys {
                partition
                    .apply(&Mutation::put(at, key, Bytes::from_static(b"value"), None))
                    .await
                    .unwrap();
            }
            partition.flush().await.unwrap();
        });
    }

    #[test]
    fn a_successful_live_flush_publishes_partition_v1_before_checkpointing_the_wal() {
        let sim = Simulation::new(16);
        let store = Arc::new(FaultStore::new());
        let host = start_host(&sim, sim.add_node(NodeId(1)), Arc::clone(&store));
        write_one(&sim, &host);

        let flushing = Arc::clone(&host);
        sim.block_on(async move { flushing.flush().await.expect("the flush succeeds") });

        let (manifest, checkpoint, segment) = sim.block_on({
            let host = Arc::clone(&host);
            let store = Arc::clone(&store);
            async move {
                let manifest = load_manifest(store.as_ref(), &partition_path())
                    .await
                    .expect("the manifest reads")
                    .expect("the manifest was published")
                    .0;
                let (bytes, _) = store
                    .get(&partition_path().object(&manifest.segments[0].name))
                    .await
                    .expect("the manifest's segment exists");
                let segment = Segment::decode(&bytes).expect("production wrote partition-v1");
                (manifest, host.log.applied_through().await, segment)
            }
        });
        assert_eq!(manifest.committed_lamport, Lamport(1));
        assert_eq!(manifest.segments.len(), 1);
        assert_eq!(checkpoint, manifest.committed_lamport);
        assert_eq!(segment.records()[0].key, Bytes::from_static(b"key"));
    }

    #[test]
    fn a_truncation_drops_the_entries_a_replica_was_holding_for_release() {
        // A replica keeps an entry between "durable in my log" and "the owner
        // says it was acknowledged". A truncation cuts the log, and this queue
        // has to go with it: the new owner resumes from the cut and reissues
        // those Lamports, so both copies would sit here, applies run in order,
        // and storage ignores a mutation at or below its committed Lamport. The
        // surrendered entry would win and the reissue would be dropped without
        // a word — this replica serving the value the cluster discarded, and
        // missing the one it kept.
        let sim = Simulation::new(16);
        let store = Arc::new(FaultStore::new());
        let replica = start_replica(&sim, sim.add_node(NodeId(1)), Arc::clone(&store));
        let entry = |lamport: u64, key: &'static str| WalEntry {
            lamport: Lamport(lamport),
            epoch: Epoch(1),
            partition: PartitionId(1),
            op: WalOp::Put {
                key: Bytes::from_static(key.as_bytes()),
                value: Bytes::from_static(b"value"),
                expires_at_millis: None,
            },
        };
        let queue = |entry: WalEntry| {
            let replica = Arc::clone(&replica);
            sim.block_on(async move { replica.apply_replicated(&entry).await.unwrap() });
        };
        let release = |through: u64| {
            let replica = Arc::clone(&replica);
            sim.block_on(async move { replica.commit_through(Lamport(through)).await });
        };
        let holds = |key: &'static str| {
            let replica = Arc::clone(&replica);
            sim.block_on(async move { replica.get(key.as_bytes()).await.unwrap().is_some() })
        };

        queue(entry(1, "committed"));
        queue(entry(2, "surrendered"));
        release(1);
        assert_eq!(
            replica.withheld_len(),
            1,
            "the entry above the owner's watermark is held, not applied"
        );

        // The owner gave Lamport 2 back and reopened above this replica's
        // epoch, so it says history ends at 1.
        replica.truncated(Lamport(1));
        queue(entry(2, "reissued"));
        release(2);

        assert!(holds("reissued"), "the reissued entry must reach storage");
        assert!(
            !holds("surrendered"),
            "a replica that applied the entry its owner handed back is serving a value no client \
             was ever told about, under a version that now means something else"
        );
        assert!(holds("committed"), "and committed history is untouched");
    }

    #[test]
    fn committed_replica_applies_reclaim_the_owners_published_horizon() {
        let sim = Simulation::new(16);
        let runtime = sim.add_node(NodeId(1));
        let store = Arc::new(FaultStore::new());
        let replica = start_replica(&sim, runtime.clone(), Arc::clone(&store));

        let entries: Vec<WalEntry> = (1..=32)
            .map(|lamport| WalEntry {
                lamport: Lamport(lamport),
                epoch: Epoch(1),
                partition: PartitionId(1),
                op: WalOp::Put {
                    key: Bytes::from(format!("k{lamport}")),
                    value: Bytes::from(vec![lamport as u8; orbita_core::MAX_VALUE_BYTES]),
                    expires_at_millis: None,
                },
            })
            .collect();
        for entry in &entries {
            let replica = Arc::clone(&replica);
            let entry = entry.clone();
            sim.block_on(async move { replica.apply_replicated(&entry).await.unwrap() });
        }

        sim.block_on({
            let store = Arc::clone(&store);
            let entries = entries.clone();
            async move {
                let owner = Partition::open(
                    runtime,
                    store as Arc<dyn ObjectStore>,
                    partition_path(),
                    Epoch(1),
                    KeyRange::unbounded(),
                )
                .await
                .unwrap();
                for entry in &entries {
                    owner.apply(&mutation_of(entry)).await.unwrap();
                }
                owner.flush().await.unwrap();
            }
        });

        let replica_to_commit = Arc::clone(&replica);
        sim.block_on(async move { replica_to_commit.commit_through(Lamport(32)).await });
        let replica_to_read = Arc::clone(&replica);
        sim.block_on(async move {
            assert!(replica_to_read.get(b"k32").await.unwrap().is_some());
        });
        assert!(
            store.range_reads.load(Ordering::Relaxed) > 0,
            "the host's production release path replaced the memtable with the published index"
        );
    }

    #[test]
    fn a_failed_segment_upload_keeps_the_write_in_the_wal_and_memtable() {
        let sim = Simulation::new(16);
        let store = Arc::new(FaultStore::new());
        let host = start_host(&sim, sim.add_node(NodeId(1)), Arc::clone(&store));
        write_one(&sim, &host);
        store.fail_next_segment();

        let flushing = Arc::clone(&host);
        assert!(sim.block_on(async move { flushing.flush().await }).is_err());

        let host = Arc::clone(&host);
        let store = Arc::clone(&store);
        sim.block_on(async move {
            assert_eq!(host.log.applied_through().await, Lamport::ZERO);
            assert!(host.get(b"key").await.unwrap().is_some());
            assert!(load_manifest(store.as_ref(), &partition_path())
                .await
                .unwrap()
                .is_none());
        });
    }

    #[test]
    fn a_worker_with_no_log_of_its_own_takes_its_position_from_the_published_manifest() {
        // The replacement-worker case ADR 0006 exists to make cheap: nothing on
        // local disk, a partition in the bucket. Before hydration this node
        // opened at position zero and could only be caught up by a peer
        // resending every write since the beginning of the partition.
        let sim = Simulation::new(16);
        let runtime = sim.add_node(NodeId(1));
        let store = Arc::new(FaultStore::new());
        publish_horizon(&sim, runtime.clone(), Arc::clone(&store), Lamport(9));

        let host = start_host(&sim, runtime, store);
        let flushing = Arc::clone(&host);
        sim.block_on(async move { flushing.flush().await.expect("an empty WAL is valid") });

        let host = Arc::clone(&host);
        let (durable, checkpoint, value) = sim.block_on(async move {
            (
                host.log.durable_lamport().await,
                host.log.applied_through().await,
                host.get(b"published").await.unwrap(),
            )
        });
        assert_eq!(
            durable,
            Lamport(9),
            "the manifest is a stronger durability claim than the log, so the log adopts it"
        );
        assert_eq!(checkpoint, Lamport(9));
        assert!(
            value.is_some(),
            "the partition was rebuilt from the bucket, not copied from a peer"
        );
    }

    #[test]
    fn hydration_never_lowers_a_log_that_is_already_past_the_manifest() {
        // A node whose own log runs ahead of the last published manifest is the
        // steady state of a busy owner. Adopting the horizon there would rewind
        // its position and let it reissue versions it has already handed out.
        let sim = Simulation::new(16);
        let runtime = sim.add_node(NodeId(1));
        let store = Arc::new(FaultStore::new());
        let host = start_host(&sim, runtime.clone(), Arc::clone(&store));
        for _ in 0..3 {
            write_one(&sim, &host);
        }
        let flushing = Arc::clone(&host);
        sim.block_on(async move { flushing.flush().await.expect("the flush succeeds") });
        drop(host);

        // Reopening runs hydration against a manifest at Lamport 3 with a log
        // that also stands at 3, which must leave both alone.
        let reopened = start_host(&sim, runtime, store);
        let durable = sim.block_on({
            let reopened = Arc::clone(&reopened);
            async move { reopened.log.durable_lamport().await }
        });
        assert_eq!(durable, Lamport(3));
    }

    #[test]
    fn a_running_replica_rebuilds_from_the_bucket_without_being_reopened() {
        // What a replica that fell beyond its owner's retained log does about
        // it. Before hydration the only cure was restarting the node, because
        // the manifest was read exactly once, at open.
        let sim = Simulation::new(16);
        let runtime = sim.add_node(NodeId(1));
        let store = Arc::new(FaultStore::new());
        let replica = start_replica(&sim, runtime.clone(), Arc::clone(&store));

        publish_keys(
            &sim,
            runtime,
            Arc::clone(&store),
            &[(Lamport(1), "one"), (Lamport(2), "two")],
        );

        let replica = Arc::clone(&replica);
        sim.block_on(async move {
            assert_eq!(
                replica.hydrate().await.unwrap().through,
                Lamport(2),
                "the horizon it reports is the one the owner may resume from"
            );
            assert!(replica.get(b"one").await.unwrap().is_some());
            assert!(replica.get(b"two").await.unwrap().is_some());
            assert_eq!(
                replica.hydrate().await.unwrap().through,
                Lamport(2),
                "hydrating again is a no-op rather than a second download"
            );
        });
    }

    #[test]
    fn hydration_replays_the_wal_tail_beyond_the_manifest() {
        // The composition the issue turns on. A hydrated partition is current
        // as of its manifest and no further, so the log above that horizon has
        // to be replayed on top of it, and the log below it must not be: those
        // writes are already in the segments, and replaying them would be work
        // proportional to the retained log rather than to the tail.
        let sim = Simulation::new(16);
        let runtime = sim.add_node(NodeId(1));
        let store = Arc::new(FaultStore::new());

        // Five writes in the log, of which the bucket has published the first
        // three under different keys, so which source answered a read is
        // visible rather than inferred.
        let wal = sim.block_on({
            let runtime = runtime.clone();
            async move {
                Wal::open(runtime, WalConfig::new(PartitionId(1), "wal/p1", Epoch(1)))
                    .await
                    .unwrap()
            }
        });
        for key in ["log1", "log2", "log3", "log4", "log5"] {
            let wal = Arc::clone(&wal);
            sim.block_on(async move {
                wal.commit(WalOp::Put {
                    key: Bytes::copy_from_slice(key.as_bytes()),
                    value: Bytes::from_static(b"value"),
                    expires_at_millis: None,
                })
                .await
                .unwrap();
            });
        }
        drop(wal);
        publish_keys(
            &sim,
            runtime.clone(),
            Arc::clone(&store),
            &[
                (Lamport(1), "seg1"),
                (Lamport(2), "seg2"),
                (Lamport(3), "seg3"),
            ],
        );

        let host = start_host(&sim, runtime, store);
        let host = Arc::clone(&host);
        sim.block_on(async move {
            for key in [b"seg1".as_slice(), b"seg2", b"seg3"] {
                assert!(
                    host.get(key).await.unwrap().is_some(),
                    "the manifest supplies everything at or below its horizon"
                );
            }
            for key in [b"log4".as_slice(), b"log5"] {
                assert!(
                    host.get(key).await.unwrap().is_some(),
                    "the log supplies the tail the manifest does not cover"
                );
            }
            for key in [b"log1".as_slice(), b"log2", b"log3"] {
                assert!(
                    host.get(key).await.unwrap().is_none(),
                    "entries at or below the horizon are the manifest's account, not the log's"
                );
            }
            assert_eq!(host.log.durable_lamport().await, Lamport(5));
        });
    }

    #[test]
    fn a_failed_manifest_publication_never_checkpoints_past_the_unpublished_segment() {
        let sim = Simulation::new(16);
        let store = Arc::new(FaultStore::new());
        let host = start_host(&sim, sim.add_node(NodeId(1)), Arc::clone(&store));
        write_one(&sim, &host);
        store.fail_next_manifest();

        let flushing = Arc::clone(&host);
        assert!(sim.block_on(async move { flushing.flush().await }).is_err());

        let host = Arc::clone(&host);
        sim.block_on(async move {
            assert_eq!(host.log.applied_through().await, Lamport::ZERO);
            assert!(host.get(b"key").await.unwrap().is_some());
        });
        assert_eq!(
            store
                .inner
                .keys()
                .iter()
                .filter(|key| key.contains("/segments/"))
                .count(),
            1,
            "the uploaded object is an orphan until a retry publishes another segment"
        );
    }

    #[test]
    fn restart_after_a_failed_publication_replays_and_retries_without_reusing_the_orphan_name() {
        let sim = Simulation::new(16);
        let store = Arc::new(FaultStore::new());
        let runtime = sim.add_node(NodeId(1));
        let host = start_host(&sim, runtime.clone(), Arc::clone(&store));
        write_one(&sim, &host);
        store.fail_next_manifest();
        let flushing = Arc::clone(&host);
        assert!(sim.block_on(async move { flushing.flush().await }).is_err());
        drop(host);

        let restarted = start_host(&sim, runtime, Arc::clone(&store));
        let flushing = Arc::clone(&restarted);
        sim.block_on(async move { flushing.flush().await.expect("the retry succeeds") });

        let manifest = sim.block_on({
            let store = Arc::clone(&store);
            async move {
                load_manifest(store.as_ref(), &partition_path())
                    .await
                    .unwrap()
                    .unwrap()
                    .0
            }
        });
        assert_eq!(manifest.committed_lamport, Lamport(1));
        assert!(manifest.segments[0].name.ends_with("0000000000000001.oseg"));
        assert_eq!(
            store
                .inner
                .keys()
                .iter()
                .filter(|key| key.contains("/segments/"))
                .count(),
            2,
            "the failed attempt's sequence zero object was not overwritten"
        );
    }

    #[test]
    fn a_stale_live_writer_cannot_replace_the_new_epochs_manifest() {
        let sim = Simulation::new(16);
        let runtime = sim.add_node(NodeId(1));
        let store = Arc::new(FaultStore::new());
        let path = partition_path();

        let (stale, replacement) = sim.block_on({
            let runtime = runtime.clone();
            let store = Arc::clone(&store);
            let path = path.clone();
            async move {
                let stale = Partition::open(
                    runtime.clone(),
                    Arc::clone(&store) as Arc<dyn ObjectStore>,
                    path.clone(),
                    Epoch(6),
                    KeyRange::unbounded(),
                )
                .await
                .unwrap();
                let replacement = Partition::open(
                    runtime,
                    store as Arc<dyn ObjectStore>,
                    path,
                    Epoch(7),
                    KeyRange::unbounded(),
                )
                .await
                .unwrap();
                (stale, replacement)
            }
        });

        sim.block_on(async move {
            replacement
                .apply(&Mutation::put(
                    Lamport(9),
                    Bytes::from_static(b"winner"),
                    Bytes::from_static(b"v"),
                    None,
                ))
                .await
                .unwrap();
            replacement.flush().await.unwrap();

            stale
                .apply(&Mutation::put(
                    Lamport(3),
                    Bytes::from_static(b"stale"),
                    Bytes::from_static(b"v"),
                    None,
                ))
                .await
                .unwrap();
            assert!(stale.flush().await.is_err(), "epoch 6 was deposed");
        });

        let manifest = sim.block_on({
            let store = Arc::clone(&store);
            async move {
                load_manifest(store.as_ref(), &path)
                    .await
                    .unwrap()
                    .unwrap()
                    .0
            }
        });
        assert_eq!(manifest.epoch, Epoch(7));
        assert_eq!(manifest.committed_lamport, Lamport(9));
    }

    #[test]
    fn an_unconditional_write_always_proceeds() {
        assert!(evaluate(WriteCondition::None, None, false).is_none());
        assert!(evaluate(WriteCondition::None, Some(&record(3)), true).is_none());
    }

    // The loop this closes: a replica evicted from the read set for missing an
    // invalidation used to be re-admitted for merely answering the next probe,
    // which arrives one renewal interval later -- a third of a lease. It would
    // take a fresh lease, fall behind again, and stall the next write for a
    // full lease duration. ADR 0001 allows that once per replica that goes bad,
    // not once per heartbeat.
    #[test]
    fn a_replica_that_only_answers_does_not_get_back_into_the_read_set() {
        // Behind the committed prefix: reachable, but it cannot carry a read.
        assert!(!readmissible_to_read_set(Some(Lamport(9)), Lamport(10)));
    }

    #[test]
    fn a_replica_that_reached_the_committed_prefix_takes_reads_again() {
        assert!(readmissible_to_read_set(Some(Lamport(10)), Lamport(10)));
        // Past it, because the owner keeps writing while the heartbeat is in
        // flight and the replica may have taken entries beyond the snapshot
        // this renewal was built from.
        assert!(readmissible_to_read_set(Some(Lamport(11)), Lamport(10)));
    }

    #[test]
    fn a_replica_that_cannot_say_where_its_log_ends_stays_out_of_the_read_set() {
        assert!(!readmissible_to_read_set(None, Lamport(10)));
        // Even against an empty log, because the absence is what is unknown,
        // not the position.
        assert!(!readmissible_to_read_set(None, Lamport::ZERO));
    }

    #[test]
    fn an_owner_that_has_committed_nothing_admits_a_replica_at_zero() {
        assert!(readmissible_to_read_set(Some(Lamport::ZERO), Lamport::ZERO));
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
