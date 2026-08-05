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

use crate::lease::{LeaseTable, ReplicaReadState, DEFAULT_LEASE_DURATION, DEFAULT_LEASE_MARGIN};
use crate::pending::{self, PendingRecord, PendingSet};
use crate::proxy::{self, LeaseGrant};

use bytes::Bytes;
use orbita_core::{
    Epoch, Error, KeyRange, Lamport, NodeId, PartitionId, Record, Result, Version, WriteCondition,
};
use orbita_format::PartitionPath;
use orbita_objectstore::ObjectStore;
use orbita_runtime::{join_all, timeout, Clock, PeerCall, Runtime, ServiceId, Transport};
use orbita_storage::{Mutation, Partition, ScanPage, TOMBSTONE_RETENTION_MILLIS};
use orbita_wal::{PartitionLog, Wal, WalConfig, WalEntry, WalOp};

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
    leases: Mutex<LeaseTable>,
    lease: LeasePolicy,
    /// The peers this node replicates to, when it owns the partition.
    replicas: Vec<NodeId>,
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
        let config =
            WalConfig::new(id, paths.wal_dir.clone(), epoch).with_replicas(replicas.clone());
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
        let flushing = Arc::new(tokio::sync::Mutex::new(()));

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
            applies,
            drained,
            read_state: Mutex::new(ReplicaReadState::new(Lamport::ZERO)),
            leases: Mutex::new(LeaseTable::default()),
            lease: spec.lease,
            grantable: Mutex::new(replicas.iter().copied().collect()),
            replicas,
            committed: Mutex::new(Lamport::ZERO),
            withheld: Mutex::new(VecDeque::new()),
            applying: tokio::sync::Mutex::new(()),
            flushing,
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
        limit: u32,
    ) -> Result<ScanPage> {
        self.wait_for_applies().await;
        self.storage.scan(prefix, cursor, limit).await
    }

    /// The highest Lamport this partition has applied, which is what a replica
    /// reports about how caught up it is.
    pub(crate) async fn committed_lamport(&self) -> Result<Lamport> {
        self.storage.committed_lamport().await
    }

    /// The highest Lamport this node has on stable storage for this partition.
    ///
    /// This is the promotion input rather than the applied Lamport, because an
    /// acknowledgement is paid for by durability and not by application, so
    /// this is what bounds the writes the cluster has promised.
    pub(crate) async fn durable_lamport(&self) -> Lamport {
        self.log.durable_lamport().await
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

    /// Publishes every applied write and checkpoints only after the manifest
    /// swap is durable.
    pub(crate) async fn flush(&self) -> Result<()> {
        if !self.is_owner() {
            return Ok(());
        }
        self.wait_for_applies().await;
        flush_and_checkpoint(&self.storage, &self.log, &self.flushing, true).await
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
        if self.replicas.is_empty() {
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

        let renewals: Vec<_> = self
            .replicas
            .iter()
            .map(|node| self.renew_one(*node, epoch, through, committed))
            .collect();
        join_all(renewals).await;
    }

    async fn renew_one(&self, node: NodeId, epoch: Epoch, through: Lamport, committed: Lamport) {
        let granting = self
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

        match answered {
            Ok(true) if granting => {}
            Ok(_) => {
                // Either the replica refused the lease, or this was a probe
                // and it has confirmed it holds none. Both mean nothing there
                // can serve a stale read, so the wait can stop counting it.
                self.leases
                    .lock()
                    .expect("lease table poisoned")
                    .revoke(node);
                self.grantable
                    .lock()
                    .expect("grantable set poisoned")
                    .insert(node);
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
    /// read set.
    async fn await_coherence(&self, wal: &Arc<Wal<R>>, lamport: Lamport) {
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

    /// Takes a read lease this partition's owner offered, or refuses it.
    ///
    /// Refusing is the answer whenever anything is unclear, because a replica
    /// that holds no lease costs one extra hop per read and a replica that
    /// holds one it should not have breaks linearizability.
    ///
    /// The same message carries how far the owner has acknowledged, which is
    /// what releases entries this node is holding back.
    pub(crate) async fn accept_lease(&self, grant: &LeaseGrant) -> bool {
        if self.is_owner() || grant.epoch < self.epoch {
            return false;
        }
        self.commit_through(grant.committed).await;

        let now = self.runtime.clock().monotonic_nanos();
        self.read_state
            .lock()
            .expect("read state poisoned")
            .accept_grant(
                now,
                grant.through,
                Duration::from_millis(grant.duration_millis),
                self.lease.margin,
            )
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
    pub(crate) fn truncated(&self, above: Lamport) {
        self.read_state
            .lock()
            .expect("read state poisoned")
            .truncated(above);
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
    log: Weak<PartitionLog<R>>,
    flushing: Weak<tokio::sync::Mutex<()>>,
    flush_on_trigger: bool,
}

async fn apply_loop<R: Runtime>(
    partition: PartitionId,
    target: ApplyTarget<R>,
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
    // A promoted node may open a manifest ahead of the WAL it retained. The
    // manifest proves that every local entry is durable, but the checkpoint
    // record cannot claim a Lamport this log has never held.
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
            partition
                .apply(&Mutation::put(
                    horizon,
                    Bytes::from_static(b"published"),
                    Bytes::from_static(b"value"),
                    None,
                ))
                .await
                .unwrap();
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
    fn a_fresh_promoted_wal_does_not_checkpoint_beyond_the_manifest_it_opened() {
        let sim = Simulation::new(16);
        let runtime = sim.add_node(NodeId(1));
        let store = Arc::new(FaultStore::new());
        publish_horizon(&sim, runtime.clone(), Arc::clone(&store), Lamport(9));

        let host = start_host(&sim, runtime, store);
        let flushing = Arc::clone(&host);
        sim.block_on(async move { flushing.flush().await.expect("an empty WAL is valid") });

        let host = Arc::clone(&host);
        assert_eq!(
            sim.block_on(async move { host.log.applied_through().await }),
            Lamport::ZERO,
            "a checkpoint never names a Lamport the local WAL did not hold"
        );
    }

    #[test]
    fn a_truncated_promoted_wal_checkpoints_only_its_local_durable_prefix() {
        let sim = Simulation::new(16);
        let runtime = sim.add_node(NodeId(1));
        let store = Arc::new(FaultStore::new());
        let wal = sim.block_on({
            let runtime = runtime.clone();
            async move {
                Wal::open(runtime, WalConfig::new(PartitionId(1), "wal/p1", Epoch(1)))
                    .await
                    .unwrap()
            }
        });
        for key in ["a", "b", "c"] {
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
        publish_horizon(&sim, runtime.clone(), Arc::clone(&store), Lamport(9));

        let host = start_host(&sim, runtime, store);
        let flushing = Arc::clone(&host);
        sim.block_on(async move { flushing.flush().await.expect("the prefix is covered") });

        let host = Arc::clone(&host);
        assert_eq!(
            sim.block_on(async move { host.log.applied_through().await }),
            Lamport(3),
            "the checkpoint is capped at the local WAL's durable end"
        );
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
