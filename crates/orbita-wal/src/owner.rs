//! The owner side: append locally, replicate, acknowledge at two of three.

use std::collections::HashMap;
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::Poll;

use bytes::Bytes;
use orbita_core::{Epoch, Error, Lamport, NodeId, PartitionId, Result};
use orbita_runtime::{PeerCall, Runtime, ServiceId, Transport};

use crate::format::{self, LogRecord, WalEntry, WalOp};
use crate::log::{PartitionLog, RecoveryState, DEFAULT_SEGMENT_TARGET_BYTES};
use crate::wire::{
    AppendRequest, FenceRequest, StatusRequest, WalResponse, METHOD_APPEND, METHOD_FENCE,
    METHOD_STATUS,
};

/// What a node needs to know to own a partition's log.
#[derive(Debug, Clone)]
pub struct WalConfig {
    pub partition: PartitionId,
    /// Directory for this partition's segments, relative to the data root.
    pub dir: String,
    /// The ownership epoch the control plane granted. An append carrying an
    /// older epoch than a replica has seen is rejected, which is what stops a
    /// deposed owner.
    pub epoch: Epoch,
    /// The peers holding the other two copies. This node is the third.
    pub replicas: Vec<NodeId>,
    pub segment_target_bytes: u64,
}

impl WalConfig {
    #[must_use]
    pub fn new(partition: PartitionId, dir: impl Into<String>, epoch: Epoch) -> Self {
        Self {
            partition,
            dir: dir.into(),
            epoch,
            replicas: Vec::new(),
            segment_target_bytes: DEFAULT_SEGMENT_TARGET_BYTES,
        }
    }

    #[must_use]
    pub fn with_replicas(mut self, replicas: Vec<NodeId>) -> Self {
        self.replicas = replicas;
        self
    }
}

struct Pending {
    entry: WalEntry,
    frame: Bytes,
}

/// One group commit: written and fsynced locally, now in flight to the
/// replicas.
struct Batch {
    last: Lamport,
    acked: bool,
}

/// How far one catch-up pass got, replica by replica.
///
/// Reported per node rather than as a single yes or no because a pass that
/// reached one of two replicas has not finished the job: the partition still
/// advertises a copy that does not hold the history. A caller that treated
/// "somebody answered" as success would clear the work and never look again,
/// and on an idle partition nothing else ever raises the question.
#[derive(Debug, Clone)]
pub struct CatchUp {
    /// The committed prefix every replica was carried to. See
    /// [`Wal::committed_lamport`] for why the horizon is that watermark and
    /// not the local durable one.
    pub horizon: Lamport,
    /// Replicas that have confirmed, in a reply, that they hold `horizon`.
    pub caught_up: Vec<NodeId>,
    /// Replicas that have not, whatever the reason.
    pub behind: Vec<NodeId>,
}

impl CatchUp {
    /// Whether every advertised copy now really exists.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.behind.is_empty()
    }
}

struct OwnerState {
    epoch: Epoch,
    next_lamport: Lamport,
    /// How far this node's own log has been fsynced.
    ///
    /// A local number and nothing more. It runs ahead of `replicated` by every
    /// entry that reached this disk and no other, which is exactly the set of
    /// writes whose clients were told `Unavailable`. Treating it as a
    /// replication target is what copies those writes onto a second node.
    durable_local: Lamport,
    /// Assigned a Lamport, not yet written. The next flusher takes the whole
    /// queue, which is where several concurrent writes come to share an fsync.
    pending: Vec<Pending>,
    inflight: Vec<Batch>,
    /// The highest Lamport a replica has confirmed. A prefix, not a set: the
    /// protocol refuses to leave holes, so one acknowledgement covers
    /// everything below it.
    replicated: Lamport,
    /// Everything at or below this failed to reach a replica. A later batch
    /// can still rescue it, since acknowledging entry N+1 proves the replica
    /// holds N.
    failed_through: Lamport,
    /// How far each replica has confirmed, individually.
    ///
    /// The durability quorum does not need this, since two of three is a
    /// count. The coherence quorum in ADR 0001 does: the owner may only
    /// acknowledge a write once every replica still holding a read lease has
    /// the invalidation, and that is a question about named nodes rather than
    /// about how many answered.
    acked: HashMap<NodeId, Lamport>,
    /// Set when this owner has given up its uncommitted tail and will never
    /// assign another Lamport. See [`Wal::quiesce`].
    quiesced: bool,
    /// Set when this node must stop being an owner: it was fenced, or its own
    /// disk stopped telling the truth.
    fatal: Option<Error>,
}

/// A partition's write-ahead log, from the owner's side.
///
/// `commit` resolves when the entry is on stable storage at two of three
/// nodes, counting this one. Everything else here exists to make that true
/// under crashes, fences, and lost peers.
pub struct Wal<R: Runtime> {
    runtime: R,
    log: Arc<PartitionLog<R>>,
    partition: PartitionId,
    /// The peers this owner replicates to, swapped rather than fixed.
    ///
    /// Placement is not an ownership change: the control plane can widen or
    /// narrow a replica set without moving the epoch, and an owner that could
    /// only learn its peers at open time would keep acknowledging writes
    /// against the peer list it was born with. Held as an `Arc<[NodeId]>` so
    /// a replication pass takes one cheap snapshot and never holds the lock
    /// across an await.
    replicas: Mutex<Arc<[NodeId]>>,
    /// Held while a batch is written and fsynced, never while it is in flight
    /// to the replicas. That is what allows pipelining.
    flush: tokio::sync::Mutex<()>,
    state: Mutex<OwnerState>,
    progress: tokio::sync::Notify,
}

impl<R: Runtime> Wal<R> {
    /// Opens the log, recovers it, and takes ownership at the configured
    /// epoch.
    ///
    /// Fails if this node has already accepted a higher epoch, because that
    /// means the control plane's grant is stale and writing under it would be
    /// exactly the split brain the epoch exists to prevent.
    pub async fn open(runtime: R, config: WalConfig) -> Result<Arc<Self>> {
        let log = PartitionLog::open(
            runtime.clone(),
            config.dir.clone(),
            config.partition,
            config.segment_target_bytes,
        )
        .await?;

        let seen = log.epoch().await;
        if config.epoch < seen {
            return Err(Error::StaleEpoch {
                partition: config.partition,
                got: config.epoch,
                current: seen,
            });
        }
        log.record_fence(config.epoch).await?;
        let durable = log.durable_lamport().await;

        Ok(Arc::new(Self {
            runtime,
            log,
            partition: config.partition,
            replicas: Mutex::new(config.replicas.into()),
            flush: tokio::sync::Mutex::new(()),
            state: Mutex::new(OwnerState {
                epoch: config.epoch,
                next_lamport: durable,
                durable_local: durable,
                pending: Vec::new(),
                inflight: Vec::new(),
                replicated: durable,
                failed_through: Lamport::ZERO,
                acked: HashMap::new(),
                quiesced: false,
                fatal: None,
            }),
            progress: tokio::sync::Notify::new(),
        }))
    }

    /// The log this owner writes to, so a node that is demoted to replica can
    /// keep serving from the same file.
    #[must_use]
    pub fn log(&self) -> Arc<PartitionLog<R>> {
        Arc::clone(&self.log)
    }

    #[must_use]
    pub fn partition(&self) -> PartitionId {
        self.partition
    }

    /// The peers this owner currently replicates to.
    #[must_use]
    pub fn replicas(&self) -> Arc<[NodeId]> {
        Arc::clone(&self.replicas.lock().expect("wal replica set poisoned"))
    }

    /// Points this owner at a new replica set without reopening the log.
    ///
    /// The control plane places replicas after a partition is already owned
    /// and serving, and it does that without bumping the epoch because
    /// placement is not a change of ownership. Reopening the partition to pick
    /// the change up would throw away in-flight writes for a reason a client
    /// cannot distinguish from a failover, so the peer list is swapped instead.
    /// Entries the previous list never carried are not lost: the first append
    /// to a peer that is behind comes back as a gap and is backfilled from this
    /// node's own log.
    pub fn set_replicas(&self, replicas: &[NodeId]) {
        let mut held = self.replicas.lock().expect("wal replica set poisoned");
        if held.as_ref() != replicas {
            *held = replicas.into();
        }
    }

    /// How far this node has durably logged, for the control plane's promotion
    /// decision.
    ///
    /// This is a claim about one disk, not about the cluster. It can stand
    /// above [`Wal::committed_lamport`], and everything in that gap is a write
    /// this node holds alone and whose client was told it failed.
    #[must_use]
    pub fn durable_lamport(&self) -> Lamport {
        self.state().durable_local
    }

    /// The committed prefix: the highest Lamport a durability quorum has
    /// confirmed under this owner's epoch.
    ///
    /// This is the one watermark the guarantees are stated against, so it is
    /// worth being exact about what it is and is not.
    ///
    /// It only moves when [`Wal::replicate`] reports that enough replicas
    /// stored a batch, which is the same event that resolves the client's
    /// `commit`. So every Lamport at or below it is on at least two nodes and
    /// may have been reported to a client as applied, which makes it the
    /// no-lost-write floor: a promotion that keeps this prefix keeps every
    /// acknowledged write. And nothing above it has been acknowledged to
    /// anyone, which makes it the no-phantom-write ceiling for any transfer
    /// that is not itself a write in flight.
    ///
    /// It is deliberately not `durable_local`. That watermark counts entries
    /// that reached this node's disk and no other, whose `commit` returned
    /// `Unavailable`. Those entries stay in the log because a later batch's
    /// acknowledgement can still rescue them — a replica that stores entry
    /// N+1 has proved it stores N — but rescuing them is a write's job. A
    /// catch-up that shipped them would put a reported failure onto a second
    /// node with no write behind it, and a promotion turns anything on a
    /// promoted node's disk into history.
    #[must_use]
    pub fn committed_lamport(&self) -> Lamport {
        self.state().replicated
    }

    /// The advertised replicas that are not known to hold the committed
    /// prefix.
    ///
    /// Answered from the per-replica acknowledgements the append path already
    /// records, so a replica counts as caught up only because it said so in a
    /// reply, never because a call to somebody else succeeded.
    ///
    /// The evidence is remembered rather than re-established, which bounds
    /// what this can notice: a replica that acknowledged the prefix and then
    /// lost its disk reports as current until something moves the prefix and
    /// the next append finds the gap. That is the same hole recovery has
    /// always had for a replica whose storage is replaced under it, and
    /// closing it is a question about detecting silent data loss rather than
    /// about placement.
    #[must_use]
    pub fn replicas_behind(&self) -> Vec<NodeId> {
        let replicas = self.replicas();
        let state = self.state();
        replicas
            .iter()
            .copied()
            .filter(|node| {
                state.acked.get(node).copied().unwrap_or(Lamport::ZERO) < state.replicated
            })
            .collect()
    }

    #[must_use]
    pub fn epoch(&self) -> Epoch {
        self.state().epoch
    }

    /// How far one named replica has confirmed it holds.
    ///
    /// This is what the coherence quorum in ADR 0001 is asked, and it is
    /// separate from [`Wal::committed_lamport`] on purpose: durability counts
    /// replicas and coherence names them.
    #[must_use]
    pub fn acked_through(&self, node: NodeId) -> Lamport {
        self.state()
            .acked
            .get(&node)
            .copied()
            .unwrap_or(Lamport::ZERO)
    }

    /// Resolves once every node in `nodes` holds `lamport`.
    ///
    /// This never gives up on its own. The caller bounds it, because the only
    /// sensible bound is the remaining life of the read lease the wait exists
    /// to protect, and this crate does not know about leases.
    pub async fn wait_until_acked(&self, nodes: &[NodeId], lamport: Lamport) -> Result<()> {
        loop {
            let notified = self.progress.notified();
            let mut notified = std::pin::pin!(notified);
            // Registered before the check so a wake that lands between the two
            // is not lost.
            notified.as_mut().enable();

            {
                let state = self.state();
                if let Some(fatal) = &state.fatal {
                    return Err(fatal.clone());
                }
                let behind = nodes
                    .iter()
                    .any(|node| state.acked.get(node).copied().unwrap_or(Lamport::ZERO) < lamport);
                if !behind {
                    return Ok(());
                }
            }

            notified.await;
        }
    }

    /// Records how far a replica has confirmed. An acknowledgement covers
    /// everything below it, because the protocol refuses a batch that would
    /// leave a hole.
    fn record_ack(&self, node: NodeId, through: Lamport) {
        {
            let mut state = self.state();
            let slot = state.acked.entry(node).or_insert(Lamport::ZERO);
            if through > *slot {
                *slot = through;
            }
        }
        self.progress.notify_waiters();
    }

    /// What recovery found when the log was opened.
    ///
    /// Reading the log back is done once, at open, because appending to an
    /// unexamined tail would build durable state on top of a torn write. This
    /// returns that result rather than rescanning, which would report a clean
    /// log and hide the fact that a tail was dropped.
    pub fn recover(&self) -> RecoveryState {
        self.log.recovery().clone()
    }

    /// Appends locally, replicates, and resolves when two of three have it
    /// durably.
    pub async fn commit(self: &Arc<Self>, op: WalOp) -> Result<Lamport> {
        let lamport = {
            let mut state = self.state();
            if let Some(fatal) = &state.fatal {
                return Err(fatal.clone());
            }
            if state.quiesced {
                return Err(Error::Unavailable(format!(
                    "partition {} is quiesced for handoff",
                    self.partition
                )));
            }
            let lamport = state.next_lamport.next();
            state.next_lamport = lamport;
            let entry = WalEntry {
                lamport,
                epoch: state.epoch,
                partition: self.partition,
                op,
            };
            let frame = format::encode(&LogRecord::Entry(entry.clone()));
            state.pending.push(Pending { entry, frame });
            lamport
        };

        self.flush().await;
        self.wait_for(lamport).await
    }

    /// Takes ownership at a higher epoch after the control plane promoted this
    /// node.
    ///
    /// Tells the peers where this node's history ends so a peer holding
    /// entries beyond it drops them. See the crate docs for why that is safe.
    pub async fn promote(&self, epoch: Epoch) -> Result<()> {
        {
            let state = self.state();
            if epoch <= state.epoch {
                return Err(Error::InvalidArgument(format!(
                    "cannot promote to epoch {epoch}, already at {}",
                    state.epoch
                )));
            }
        }
        self.log.record_fence(epoch).await?;
        let durable = self.log.durable_lamport().await;
        {
            let mut state = self.state();
            state.epoch = epoch;
            state.next_lamport = durable;
            state.durable_local = durable;
            state.replicated = durable;
            state.pending.clear();
            state.inflight.clear();
            state.failed_through = Lamport::ZERO;
            // A new epoch means the replicas are about to be told where
            // history ends, so what they confirmed under the old owner says
            // nothing about where they are now.
            state.acked.clear();
            state.fatal = None;
        }

        let request = FenceRequest {
            partition: self.partition,
            epoch,
            truncate_above: durable,
        };
        for node in self.replicas().iter() {
            // A peer we cannot reach is fenced by the first append it sees at
            // the new epoch, so promotion does not wait on it. A peer that
            // says we are already stale is another matter.
            if let Ok(WalResponse::StaleEpoch { current }) =
                self.send(*node, METHOD_FENCE, request.encode()).await
            {
                let error = Error::StaleEpoch {
                    partition: self.partition,
                    got: epoch,
                    current,
                };
                self.set_fatal(error.clone());
                return Err(error);
            }
        }
        Ok(())
    }

    /// Brings every replica up to the committed prefix without waiting for a
    /// new write to carry the entries there.
    ///
    /// Replication is otherwise driven entirely by appends, and an append only
    /// happens when a client writes. Two situations leave replicas behind with
    /// nothing to fix them: a replica placed onto a partition that then goes
    /// idle, and an owner that has closed write admission because it is
    /// draining. The second is the dangerous one — the control plane will only
    /// hand a partition to a replica that has caught up, so an owner that
    /// cannot push has nothing to hand off and drains forever.
    ///
    /// The horizon is [`Wal::committed_lamport`] and not the local durable
    /// position, and that is the whole safety argument here. A write is
    /// carried above the committed prefix by exactly one thing, the `commit`
    /// that is trying to make it committed, and that carrier reports the
    /// outcome to a client. Everything this method could otherwise pick up is
    /// a write whose client was already told `Unavailable`; putting it on a
    /// second node with no client waiting turns a reported failure into a
    /// value the next promoted owner will replay into storage and serve.
    ///
    /// Mechanically this is a zero-entry append at the committed prefix. A
    /// replica that already holds it answers plainly; one that is behind
    /// answers with a gap, which the existing catch-up path fills from this
    /// node's own log, bounded by the same horizon. Nothing new is invented on
    /// the wire.
    ///
    /// Errors only when this owner has been fenced, which is not a retryable
    /// condition. A replica that could not be reached comes back in
    /// [`CatchUp::behind`] so the caller keeps the work pending.
    pub async fn catch_up_replicas(&self) -> Result<CatchUp> {
        let replicas = self.replicas();
        let (epoch, horizon, fatal) = {
            let state = self.state();
            (state.epoch, state.replicated, state.fatal.clone())
        };
        if let Some(fatal) = fatal {
            return Err(fatal);
        }
        let mut result = CatchUp {
            horizon,
            caught_up: Vec::new(),
            behind: Vec::new(),
        };
        if replicas.is_empty() {
            return Ok(result);
        }

        let request = AppendRequest {
            partition: self.partition,
            epoch,
            prev_lamport: horizon,
            committed: horizon,
            entries: Vec::new(),
        };

        for node in replicas.iter() {
            if let Outcome::Stale(current) = self.call_replica(*node, request.clone()).await {
                let error = Error::StaleEpoch {
                    partition: self.partition,
                    got: epoch,
                    current,
                };
                self.set_fatal(error.clone());
                return Err(error);
            }
            // Judged on what the replica said about itself rather than on
            // whether the call returned, because those differ: a reply can
            // arrive from a replica that is still short of the horizon, and a
            // node that answered nothing has proved nothing.
            if self.acked_through(*node) >= horizon {
                result.caught_up.push(*node);
            } else {
                result.behind.push(*node);
            }
        }
        Ok(result)
    }

    /// Stops assigning Lamports and drops the tail no client was told about.
    ///
    /// A draining owner has to advertise a position a replica can be carried
    /// to, or the control plane finds no caught-up handoff target and the
    /// drain burns its whole budget. With write admission closed and every
    /// admitted write resolved, everything above the committed prefix is a
    /// write that reached this disk alone and whose client was told it failed,
    /// and this node is the last one that still knows that. Discarding it here
    /// is what makes the failure final instead of leaving it for a future
    /// promotion to resolve as a success.
    ///
    /// This is one-way. Further commits are refused rather than assigned a
    /// Lamport the log has just given back, because reissuing a version would
    /// break [ADR 0002] and appending above the cut would leave the hole the
    /// replication protocol exists to prevent.
    ///
    /// Returns the durable position afterwards, which is now the committed
    /// prefix.
    ///
    /// [ADR 0002]: https://github.com/orbita-rocks/orbita/blob/develop/docs/adr/0002-key-versions-are-partition-lamports.md
    pub async fn quiesce(&self) -> Result<Lamport> {
        let (committed, durable) = {
            let mut state = self.state();
            if let Some(fatal) = &state.fatal {
                return Err(fatal.clone());
            }
            // Set before the check below, so that a caller that has to retry
            // is not racing new writes on the way back in.
            state.quiesced = true;
            if !state.pending.is_empty() {
                return Err(Error::Internal(format!(
                    "partition {} was quiesced with {} writes still unflushed",
                    self.partition,
                    state.pending.len()
                )));
            }
            (state.replicated, state.durable_local)
        };
        if durable <= committed {
            return Ok(durable);
        }

        self.log.truncate_above(committed).await?;
        let durable = self.log.durable_lamport().await;
        {
            let mut state = self.state();
            state.durable_local = durable;
            state.next_lamport = durable;
            // A batch that is gone from the log cannot be rescued by a reply
            // that is still on the wire. `advance` clamps as well; this keeps
            // the two from disagreeing about what is even in flight.
            state.inflight.retain(|batch| batch.last <= durable);
        }
        self.progress.notify_waiters();
        Ok(durable)
    }

    /// Asks a peer how far it has durably logged, which is what the control
    /// plane compares when choosing who to promote.
    pub async fn replica_status(&self, node: NodeId) -> Result<(Lamport, Epoch)> {
        let payload = StatusRequest {
            partition: self.partition,
        }
        .encode();
        match self.send(node, METHOD_STATUS, payload).await? {
            WalResponse::Ok {
                durable_lamport,
                epoch,
            } => Ok((durable_lamport, epoch)),
            WalResponse::StaleEpoch { current } => Err(Error::StaleEpoch {
                partition: self.partition,
                got: self.epoch(),
                current,
            }),
            WalResponse::Gap { .. } => Err(Error::Internal(
                "a status request cannot leave a gap".to_string(),
            )),
            WalResponse::Error(message) => Err(Error::Unavailable(message)),
        }
    }

    /// Drops the log up to `applied_through`, which the caller must only do
    /// once those entries are applied and their SSTs are durable.
    pub async fn checkpoint(&self, applied_through: Lamport) -> Result<()> {
        self.log.checkpoint(applied_through).await
    }

    fn state(&self) -> MutexGuard<'_, OwnerState> {
        self.state.lock().expect("wal owner state poisoned")
    }

    fn set_fatal(&self, error: Error) {
        {
            let mut state = self.state();
            if state.fatal.is_none() {
                state.fatal = Some(error);
            }
        }
        self.progress.notify_waiters();
    }

    /// Writes and fsyncs whatever is pending, then replicates it.
    ///
    /// Every committer calls this, and all but one of a concurrent group finds
    /// the queue already taken. That is the batching: the cost of an fsync is
    /// paid once for everyone who arrived while it was running.
    async fn flush(self: &Arc<Self>) {
        let guard = self.flush.lock().await;

        let batch = {
            let mut state = self.state();
            if state.fatal.is_some() {
                return;
            }
            std::mem::take(&mut state.pending)
        };
        if batch.is_empty() {
            return;
        }

        let first = batch[0].entry.lamport;
        let last = batch[batch.len() - 1].entry.lamport;
        let frames: Vec<Bytes> = batch.iter().map(|p| p.frame.clone()).collect();

        if let Err(e) = self.log.append_frames(&frames, last).await {
            // A failed local write leaves the log in a state we cannot reason
            // about, so this owner stops rather than guessing.
            self.set_fatal(e);
            return;
        }
        {
            let mut state = self.state();
            state.durable_local = last;
            state.inflight.push(Batch { last, acked: false });
        }

        // Releasing here is the pipelining decision made concrete: the next
        // batch is written while this one is still in flight.
        drop(guard);

        let (epoch, committed) = {
            let state = self.state();
            (state.epoch, state.replicated)
        };
        let request = AppendRequest {
            partition: self.partition,
            epoch,
            prev_lamport: Lamport(first.get() - 1),
            committed,
            entries: batch
                .into_iter()
                .map(|p| (p.entry, p.frame))
                .collect::<Vec<_>>(),
        };

        match self.replicate(request).await {
            Ok(()) => {
                {
                    let mut state = self.state();
                    for entry in &mut state.inflight {
                        if entry.last == last {
                            entry.acked = true;
                        }
                    }
                    advance(&mut state);
                }
                self.progress.notify_waiters();
            }
            Err(error) => {
                if matches!(error, Error::StaleEpoch { .. }) {
                    self.set_fatal(error);
                } else {
                    {
                        let mut state = self.state();
                        if last > state.failed_through {
                            state.failed_through = last;
                        }
                    }
                    self.progress.notify_waiters();
                }
            }
        }
    }

    async fn wait_for(&self, lamport: Lamport) -> Result<Lamport> {
        loop {
            let notified = self.progress.notified();
            let mut notified = std::pin::pin!(notified);
            // Registered before the check so a wake that lands between the two
            // is not lost.
            notified.as_mut().enable();

            {
                let state = self.state();
                if state.replicated >= lamport {
                    return Ok(lamport);
                }
                if let Some(fatal) = &state.fatal {
                    return Err(fatal.clone());
                }
                if lamport <= state.failed_through {
                    return Err(Error::Unavailable(format!(
                        "partition {} could not reach a second copy for {lamport}",
                        self.partition
                    )));
                }
            }

            notified.await;
        }
    }

    /// Resolves once enough replicas hold the batch, which with two replicas
    /// means one of them: this node is the other half of the two.
    ///
    /// A replica that has not answered by then is handed to a background task
    /// rather than dropped. Dropping it would be the cheap thing to do and
    /// would leave a lagging replica lagging forever, because the fast replica
    /// wins every race.
    async fn replicate(self: &Arc<Self>, request: AppendRequest) -> Result<()> {
        let replicas = self.replicas();
        let required = replicas.len().div_ceil(2);
        if required == 0 {
            return Ok(());
        }

        let mut calls: Vec<Pin<Box<dyn Future<Output = Outcome> + Send>>> = replicas
            .iter()
            .map(|node| {
                let this = Arc::clone(self);
                let request = request.clone();
                let node = *node;
                Box::pin(async move { this.call_replica(node, request).await })
                    as Pin<Box<dyn Future<Output = Outcome> + Send>>
            })
            .collect();

        let mut acks = 0usize;
        let mut stale: Option<Epoch> = None;

        let outcome = poll_fn(|cx| {
            let mut i = 0;
            while i < calls.len() {
                match calls[i].as_mut().poll(cx) {
                    Poll::Ready(outcome) => {
                        drop(calls.remove(i));
                        match outcome {
                            Outcome::Acked => acks += 1,
                            Outcome::Stale(current) => stale = Some(current),
                            Outcome::Failed => {}
                        }
                    }
                    Poll::Pending => i += 1,
                }
            }

            if let Some(current) = stale {
                return Poll::Ready(Err(Error::StaleEpoch {
                    partition: request.partition,
                    got: request.epoch,
                    current,
                }));
            }
            if acks >= required {
                return Poll::Ready(Ok(()));
            }
            if calls.is_empty() {
                return Poll::Ready(Err(Error::Unavailable(format!(
                    "partition {} has {acks} of {required} replicas",
                    request.partition
                ))));
            }
            Poll::Pending
        })
        .await;

        let last = request.last_lamport();
        for call in calls {
            let this = Arc::clone(self);
            self.runtime.spawn(async move {
                let outcome = call.await;
                this.record_late_outcome(last, outcome);
            });
        }

        outcome
    }

    /// Folds in a reply that arrived after the batch was already
    /// acknowledged. It can still matter: a straggler that says we are fenced
    /// has to stop us.
    fn record_late_outcome(&self, last: Lamport, outcome: Outcome) {
        match outcome {
            Outcome::Stale(current) => self.set_fatal(Error::StaleEpoch {
                partition: self.partition,
                got: self.epoch(),
                current,
            }),
            Outcome::Acked => {
                {
                    let mut state = self.state();
                    for batch in &mut state.inflight {
                        if batch.last == last {
                            batch.acked = true;
                        }
                    }
                    advance(&mut state);
                }
                self.progress.notify_waiters();
            }
            Outcome::Failed => {}
        }
    }

    async fn call_replica(&self, node: NodeId, request: AppendRequest) -> Outcome {
        match self.send(node, METHOD_APPEND, request.encode()).await {
            Ok(WalResponse::Ok {
                durable_lamport, ..
            }) => {
                self.record_ack(node, durable_lamport);
                Outcome::Acked
            }
            Ok(WalResponse::StaleEpoch { current }) => {
                tracing::warn!(
                    partition = self.partition.get(),
                    current = current.get(),
                    "this node has been fenced by a newer owner"
                );
                Outcome::Stale(current)
            }
            Ok(WalResponse::Gap {
                durable_lamport, ..
            }) => self.catch_up(node, durable_lamport, &request).await,
            Ok(WalResponse::Error(message)) => {
                tracing::warn!(node = node.get(), %message, "replica rejected an append");
                Outcome::Failed
            }
            Err(_) => Outcome::Failed,
        }
    }

    /// Resends everything a lagging replica is missing, from this node's own
    /// log.
    ///
    /// One attempt only. A replica that is still behind after this is behind
    /// by more than the log holds and needs a snapshot, which is the storage
    /// crate's job, not this one's.
    async fn catch_up(&self, node: NodeId, from: Lamport, request: &AppendRequest) -> Outcome {
        let last = request.last_lamport();
        let entries = match self.log.entries_after(from).await {
            Ok(Some(entries)) => entries,
            Ok(None) | Err(_) => {
                tracing::warn!(
                    node = node.get(),
                    from = from.get(),
                    "replica is behind what the log still holds and needs a snapshot"
                );
                return Outcome::Failed;
            }
        };

        let entries: Vec<(WalEntry, Bytes)> = entries
            .into_iter()
            .filter(|e| e.lamport <= last)
            .map(|e| {
                let frame = format::encode(&LogRecord::Entry(e.clone()));
                (e, frame)
            })
            .collect();

        let payload = AppendRequest {
            partition: request.partition,
            epoch: request.epoch,
            prev_lamport: from,
            committed: self.committed_lamport(),
            entries,
        }
        .encode();

        match self.send(node, METHOD_APPEND, payload).await {
            Ok(WalResponse::Ok {
                durable_lamport, ..
            }) => {
                self.record_ack(node, durable_lamport);
                Outcome::Acked
            }
            Ok(WalResponse::StaleEpoch { current }) => Outcome::Stale(current),
            _ => Outcome::Failed,
        }
    }

    async fn send(&self, node: NodeId, method: u16, payload: Bytes) -> Result<WalResponse> {
        let call = PeerCall {
            service: ServiceId::Wal,
            method,
            payload,
        };
        let bytes = self
            .runtime
            .transport()
            .call(node, call)
            .await
            .map_err(|e| Error::Unavailable(e.to_string()))?;
        WalResponse::decode(&bytes)
            .map_err(|e| Error::Internal(format!("undecodable wal response: {e:?}")))
    }
}

enum Outcome {
    Acked,
    Stale(Epoch),
    Failed,
}

/// Moves the acknowledged watermark up.
///
/// An acknowledgement of a batch proves the replica holds every entry below it
/// too, because the protocol refuses a batch that would leave a hole. So a
/// later batch's acknowledgement rescues an earlier one whose own reply was
/// lost.
///
/// Clamped to what this node itself holds. The committed prefix is a claim
/// that a quorum stored the entry, and this node is a member of every quorum
/// it counts, so a watermark above `durable_local` would be a claim about
/// entries this node has given back. Only [`Wal::quiesce`] can lower the
/// durable position, and only after the replies that could arrive late were
/// already for entries no client is waiting on.
fn advance(state: &mut OwnerState) {
    let mut best = state.replicated;
    for batch in &state.inflight {
        if batch.acked && batch.last > best {
            best = batch.last;
        }
    }
    state.replicated = best.min(state.durable_local);
    let released = state.replicated;
    state.inflight.retain(|b| b.last > released);
}
