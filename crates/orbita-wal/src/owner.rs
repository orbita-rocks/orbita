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
use crate::log::{CatchUp, PartitionLog, RecoveryState, DEFAULT_SEGMENT_TARGET_BYTES};
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

/// A replica this owner has proven it cannot catch up from its own log.
///
/// The owner is the only node that can tell this: the replica knows it is
/// missing entries, and only the owner knows whether it still holds them. So
/// the owner records it rather than logging it and moving on, because a
/// warning line is not a state anything can be asked about, and a failure mode
/// nothing can be asked about is one an operator discovers first.
///
/// Until hydration from object storage lands (issue #17) there is no path back
/// for such a replica: the entries are cluster-durable in the published
/// manifest, and nothing turns those objects into a caught-up replica. It
/// stays out of the read set and has to be replaced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BeyondRetention {
    pub node: NodeId,
    /// How far the replica said it had logged when it fell off.
    pub replica_durable: Lamport,
    /// The oldest Lamport this owner still retains, which is the other half of
    /// the diagnosis: the gap is everything between the two. `None` when the
    /// owner's log retains no entries at all.
    pub retained_from: Option<Lamport>,
}

/// What an owner has established about one replica's ability to catch up from
/// its log.
///
/// The three states exist because two of them are not the same thing, and
/// collapsing them is how a cliff hides. An owner that has just opened its log
/// has replicated nothing and heard nothing, so it knows nothing, and an
/// absent answer read as a healthy one is the bug the #75 review of issue #63
/// found: a restarted owner of an idle partition would report every replica
/// fine while one of them could not serve at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaCatchUp {
    /// Neither replicated to nor heard from since this log opened. Not
    /// healthy, not stranded, not an answer to give an operator.
    Unestablished,
    /// Holds a prefix this log can still extend.
    Following,
    /// Needs entries this log no longer holds.
    Stranded(BeyondRetention),
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

struct OwnerState {
    epoch: Epoch,
    next_lamport: Lamport,
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
    /// What this owner has established about each replica's ability to catch
    /// up. Seeded `Unestablished` for every configured replica when the log
    /// opens, so that having heard nothing yet is never mistaken for having
    /// heard something good.
    catch_up: HashMap<NodeId, ReplicaCatchUp>,
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
    replicas: Vec<NodeId>,
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
        let catch_up = config
            .replicas
            .iter()
            .map(|node| (*node, ReplicaCatchUp::Unestablished))
            .collect();

        Ok(Arc::new(Self {
            runtime,
            log,
            partition: config.partition,
            replicas: config.replicas,
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
                catch_up,
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

    /// How far this node has durably logged, for the control plane's promotion
    /// decision.
    #[must_use]
    pub fn durable_lamport(&self) -> Lamport {
        self.state().durable_local
    }

    /// How far a two-of-three acknowledgement has reached. Every Lamport at or
    /// below this was reported to a client as committed.
    #[must_use]
    pub fn committed_lamport(&self) -> Lamport {
        self.state().replicated
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

    /// What this owner has established about each of its replicas, in node
    /// order so that two reports of an unchanged state are the same bytes.
    ///
    /// Every configured replica appears, including the ones nothing is known
    /// about. That is the whole point: a caller that only sees failures cannot
    /// tell "checked and fine" from "never checked".
    #[must_use]
    pub fn catch_up_status(&self) -> Vec<(NodeId, ReplicaCatchUp)> {
        let state = self.state();
        let mut status: Vec<(NodeId, ReplicaCatchUp)> = self
            .replicas
            .iter()
            .map(|node| {
                let known = state
                    .catch_up
                    .get(node)
                    .copied()
                    .unwrap_or(ReplicaCatchUp::Unestablished);
                (*node, known)
            })
            .collect();
        drop(state);
        status.sort_unstable_by_key(|(node, _)| node.get());
        status
    }

    /// The replicas this owner has proven it cannot catch up from its own log.
    ///
    /// Empty means no replica is known to be stranded, which is not the same
    /// as every replica being fine; ask [`Wal::catch_up_status`] for that
    /// distinction. A non-empty answer names a copy that is out of the read
    /// set and out of the durability quorum until something outside this crate
    /// restores it.
    #[must_use]
    pub fn beyond_retention(&self) -> Vec<BeyondRetention> {
        self.catch_up_status()
            .into_iter()
            .filter_map(|(_, status)| match status {
                ReplicaCatchUp::Stranded(fallen) => Some(fallen),
                ReplicaCatchUp::Unestablished | ReplicaCatchUp::Following => None,
            })
            .collect()
    }

    /// Folds in a report of where a replica's log ends.
    ///
    /// Any report will do, and that is what makes an owner that has just
    /// opened its log able to reconstruct the truth without replicating
    /// anything: the lease heartbeat reaches every replica whether or not
    /// there are writes, and one number back is enough when the owner already
    /// knows where its own history starts.
    ///
    /// Cheap enough for a heartbeat. It reads the retention horizon the log
    /// tracks rather than scanning the directory for it, which is why
    /// [`crate::PartitionLog::retained_from`] exists.
    pub async fn note_replica_position(&self, node: NodeId, replica_durable: Lamport) {
        let retained_from = self.log.retained_from().await;
        // The log holds one contiguous run, so a replica can be extended from
        // here exactly when the entry it needs next has not been dropped.
        let status = if replica_durable.next() >= retained_from {
            ReplicaCatchUp::Following
        } else {
            ReplicaCatchUp::Stranded(BeyondRetention {
                node,
                replica_durable,
                // A log that has dropped everything reports no horizon rather
                // than a Lamport it does not hold.
                retained_from: (self.log.durable_lamport().await >= retained_from)
                    .then_some(retained_from),
            })
        };
        self.record_catch_up(node, status);
    }

    /// Records a conclusion about a replica, logging only when it changes.
    ///
    /// Catch-up is judged on every append and every heartbeat, so a line per
    /// judgement would bury the one that matters under thousands that do not.
    fn record_catch_up(&self, node: NodeId, status: ReplicaCatchUp) {
        let changed = {
            let mut state = self.state();
            state.catch_up.insert(node, status) != Some(status)
        };
        if !changed {
            return;
        }
        match status {
            ReplicaCatchUp::Stranded(fallen) => tracing::error!(
                partition = self.partition.get(),
                node = node.get(),
                replica_durable = fallen.replica_durable.get(),
                retained_from = fallen.retained_from.map(Lamport::get),
                "replica has fallen past this owner's retained log and cannot be caught up from \
                 it; it is out of the read set and the durability quorum until it is hydrated \
                 from object storage or replaced"
            ),
            ReplicaCatchUp::Following => tracing::info!(
                partition = self.partition.get(),
                node = node.get(),
                "replica is following this owner's log again"
            ),
            ReplicaCatchUp::Unestablished => {}
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
        // A replica that took a batch holds a prefix this log can extend,
        // whatever put it there, so this is also how a hydrated replica clears
        // itself without hydration needing a second bookkeeping path.
        self.record_catch_up(node, ReplicaCatchUp::Following);
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
            for status in state.catch_up.values_mut() {
                *status = ReplicaCatchUp::Unestablished;
            }
            state.fatal = None;
        }

        let request = FenceRequest {
            partition: self.partition,
            epoch,
            truncate_above: durable,
        };
        for node in &self.replicas {
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
        let required = self.replicas.len().div_ceil(2);
        if required == 0 {
            return Ok(());
        }

        let mut calls: Vec<Pin<Box<dyn Future<Output = Outcome> + Send>>> = self
            .replicas
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
            Ok(CatchUp::Entries(entries)) => entries,
            Ok(CatchUp::BeyondRetention { retained_from }) => {
                self.record_catch_up(
                    node,
                    ReplicaCatchUp::Stranded(BeyondRetention {
                        node,
                        replica_durable: from,
                        retained_from,
                    }),
                );
                return Outcome::Failed;
            }
            // The replica reported a gap and this log has nothing above it,
            // which means the batch this is answering has already been
            // superseded. Nothing to send and nothing wrong.
            Ok(CatchUp::UpToDate) => return Outcome::Failed,
            Err(error) => {
                // Reading this node's own log failed, which says nothing about
                // the replica. Kept distinct from the cliff above so that a bad
                // disk here is not diagnosed as a lost replica there.
                tracing::warn!(
                    node = node.get(),
                    from = from.get(),
                    %error,
                    "reading the log to catch a replica up failed"
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
fn advance(state: &mut OwnerState) {
    let mut best = state.replicated;
    for batch in &state.inflight {
        if batch.acked && batch.last > best {
            best = batch.last;
        }
    }
    state.replicated = best;
    state.inflight.retain(|b| b.last > best);
}
