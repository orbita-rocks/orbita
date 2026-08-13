//! The owner side: append locally, replicate, acknowledge at two of three.

use std::collections::HashMap;
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use orbita_core::{Epoch, Error, Lamport, NodeId, PartitionId, Result};
use orbita_runtime::{join_all, Clock, PeerCall, Runtime, ServiceId, Transport};

use crate::format::{self, LogRecord, WalEntry, WalOp};
use crate::log::{CatchUp, PartitionLog, RecoveryState, DEFAULT_SEGMENT_TARGET_BYTES};
use crate::replica::Hydration;
use crate::wire::{
    AppendRequest, FenceRequest, StatusRequest, WalResponse, METHOD_APPEND, METHOD_FENCE,
    METHOD_STATUS,
};

/// How long an owner keeps retrying a batch while a replica has never
/// answered.
///
/// Sized against the two things that make a replica late to a partition that
/// already has an owner: the control poll that tells it the map moved, a
/// quarter of a second by default, and the cold open that follows, which
/// [ADR 0013](../../../docs/adr/0013-read-serving-is-decoupled-from-the-durability-quorum.md)
/// measured against real S3 at 1.30s for a 114 MiB partition and 2.36s for
/// 309 MiB.
///
/// Two seconds covers the first and most of the second. It is deliberately
/// under the three seconds the control plane waits before declaring a node
/// dead, so a peer that is genuinely gone is reported gone rather than retried
/// past the point anyone is still waiting. A partition large enough to open
/// more slowly than this loses the tail of the window, which costs the first
/// writes and not correctness.
pub const DEFAULT_REPLICATION_RETRY_BUDGET: Duration = Duration::from_secs(2);

/// The first pause after a batch fails, doubling from there.
///
/// Short enough that the common case — a replica a poll interval behind its
/// owner — is recovered in one or two attempts rather than waited out.
const REPLICATION_RETRY_INITIAL: Duration = Duration::from_millis(25);

/// The longest pause between attempts, so the budget is spent on several tries
/// rather than one long sleep that might outlast the window it was waiting for.
const REPLICATION_RETRY_MAX_BACKOFF: Duration = Duration::from_millis(250);

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
    /// The peers this owner ships every append to. Some of them hold a durable
    /// copy the cluster counts on; the rest follow for freshness only. Both
    /// receive the same stream, because a follower that is not sent the writes
    /// cannot stay current, and telling them apart here would put a policy
    /// decision in the replication path.
    pub replicas: Vec<NodeId>,
    /// How many of those peers must acknowledge before a write is durable.
    ///
    /// Separate from the length of the peer list, and that separation is the
    /// point. A quorum sized from the list means every peer added to serve
    /// reads also has to be waited on, so read capacity could only be bought
    /// with write latency and with a larger set of nodes any one of which can
    /// stall a write. Sized independently, a peer beyond the quorum receives
    /// every append, can be promoted, and can be lost without a write noticing.
    ///
    /// One, with this node, is the two-of-three the WAL has always required.
    pub durability_acks: usize,
    /// How long an owner keeps trying to reach a durability quorum for a batch
    /// while some replica has still never answered.
    ///
    /// Covers the window in which a partition exists on its owner and not yet
    /// on its replicas, which is every split child, every new placement, and
    /// every partition on a cluster that has just started. Zero restores the
    /// single attempt every release before this made. See issue #168.
    pub replication_retry_budget: Duration,
    pub segment_target_bytes: u64,
    /// What the manifest this node's storage was built from says, if there was
    /// one.
    ///
    /// It travels in the config rather than being applied by the caller
    /// afterwards because the owner reads its log position exactly once, at
    /// open, to decide where to start assigning Lamports. A hydration applied
    /// after that would be invisible to this owner and it would hand out
    /// versions the segments already contain. The epoch comes with it because
    /// a manifest published above the epoch this node was granted means the
    /// grant is stale and this node must not open as owner at all.
    pub hydrated: Hydration,
}

impl WalConfig {
    #[must_use]
    pub fn new(partition: PartitionId, dir: impl Into<String>, epoch: Epoch) -> Self {
        Self {
            partition,
            dir: dir.into(),
            epoch,
            replicas: Vec::new(),
            // One peer plus this node is the two-of-three the WAL has always
            // required, so the default changes nothing for a cluster that adds
            // no followers.
            durability_acks: 1,
            replication_retry_budget: DEFAULT_REPLICATION_RETRY_BUDGET,
            segment_target_bytes: DEFAULT_SEGMENT_TARGET_BYTES,
            hydrated: Hydration::default(),
        }
    }

    /// Sets how many peer acknowledgements a write requires.
    #[must_use]
    pub fn with_durability_acks(mut self, acks: usize) -> Self {
        self.durability_acks = acks;
        self
    }

    /// Sets how long a batch keeps retrying while a replica has never
    /// answered. Zero is a single attempt.
    #[must_use]
    pub fn with_replication_retry_budget(mut self, budget: Duration) -> Self {
        self.replication_retry_budget = budget;
        self
    }

    #[must_use]
    pub fn with_replicas(mut self, replicas: Vec<NodeId>) -> Self {
        self.replicas = replicas;
        self
    }

    /// Declares what the manifest the storage engine was built from says, so
    /// this owner starts its sequence above the writes the bucket already
    /// holds and refuses to open under a grant that manifest disproves.
    #[must_use]
    pub fn with_hydration(mut self, hydrated: Hydration) -> Self {
        self.hydrated = hydrated;
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

/// How far this owner's advertised replicas trail its committed prefix.
///
/// This is the WAL replication-lag signal the observability requirement asks
/// for, folded to two numbers so it labels a partition rather than a partition
/// crossed with a node: a per-replica label would grow the metric with the
/// cluster twice over, once per partition and again per replica, and the
/// question an operator asks — is this partition's replication keeping up — is
/// answered by the worst replica and the count of laggards, not by naming each
/// one. A replica that is [`ReplicaCatchUp::Stranded`] is not lag: no number of
/// Lamports describes a copy a retry cannot advance, and it is reported through
/// [`Wal::beyond_retention`] instead.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReplicationLag {
    /// Advertised replicas short of the committed prefix, excluding stranded
    /// ones. Zero is the healthy answer.
    pub replicas_behind: u64,
    /// The largest Lamport distance any of those replicas trails the committed
    /// prefix by. Zero when nothing is behind.
    pub max_lamports: u64,
}

/// What an owner has established about one replica: where its log ends, and
/// whether this log can still extend it.
///
/// The three states exist because two of them are not the same thing, and
/// collapsing them is how a cliff hides. An owner that has just opened its log
/// has replicated nothing and heard nothing, so it knows nothing, and an
/// absent answer read as a healthy one is the bug the #75 review of issue #63
/// found: a restarted owner of an idle partition would report every replica
/// fine while one of them could not serve at all.
///
/// This is the only per-replica record in the crate, and both questions asked
/// about a replica's health are read off it. Whether a catch-up still has work
/// to do for a node is [`Wal::replicas_behind`]; whether no catch-up ever can
/// is [`Wal::beyond_retention`]. They partition the same three states, so they
/// cannot contradict each other about one node:
///
/// | State | `replicas_behind` | `beyond_retention` |
/// |---|---|---|
/// | `Unestablished` | yes, if there is history to give | no |
/// | `Following` below the committed prefix | yes | no |
/// | `Following` at or above it | no | no |
/// | `Stranded` | no — a retry cannot help | yes |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaCatchUp {
    /// Neither replicated to nor heard from since this log opened. Not
    /// healthy, not stranded, not an answer to give an operator.
    Unestablished,
    /// Holds a prefix this log can still extend, ending at `through`.
    ///
    /// The position is carried rather than left implicit because "can be
    /// extended" and "has been extended" are different facts and a caller
    /// needs both. Without it, deciding whether a catch-up still owes this
    /// replica a pass would need a second map keyed by the same nodes, and two
    /// maps describing one replica are two answers waiting to disagree.
    Following { through: Lamport },
    /// Needs entries this log no longer holds.
    Stranded(BeyondRetention),
}

impl ReplicaCatchUp {
    /// Whether this is the same kind of answer as `other`, ignoring how far
    /// the replica has got.
    ///
    /// Used to decide whether a conclusion is worth logging. A replica
    /// following along moves its position on every append, and a line per
    /// append would bury the one transition an operator needs to see.
    fn same_kind(self, other: Self) -> bool {
        matches!(
            (self, other),
            (Self::Unestablished, Self::Unestablished)
                | (Self::Following { .. }, Self::Following { .. })
                | (Self::Stranded(_), Self::Stranded(_))
        )
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
///
/// Named for the pass rather than the state, because the state is
/// [`ReplicaCatchUp`] and the log's answer to a backfill request is
/// [`crate::CatchUp`]. This is neither: it is what one round of
/// [`Wal::catch_up_replicas`] achieved, read back off the per-replica record
/// once the calls have landed.
#[derive(Debug, Clone)]
pub struct CatchUpPass {
    /// The committed prefix every replica was carried to. See
    /// [`Wal::committed_lamport`] for why the horizon is that watermark and
    /// not the local durable one.
    pub horizon: Lamport,
    /// Replicas now known to hold `horizon`.
    pub caught_up: Vec<NodeId>,
    /// Replicas short of it that another pass could still carry: unreachable,
    /// or reached and still catching up.
    pub behind: Vec<NodeId>,
    /// Replicas short of it that no pass can carry, because the entries they
    /// need are older than this owner's retained log.
    ///
    /// Kept apart from `behind` because the two need opposite handling. One is
    /// work to retry; the other is a fault to report, and retrying it forever
    /// would pin a node in a permanent retry loop while hiding the reason. It
    /// leaves the crate through [`Wal::beyond_retention`] instead.
    pub stranded: Vec<NodeId>,
}

impl CatchUpPass {
    /// Whether every advertised copy a catch-up could still help now holds the
    /// horizon.
    ///
    /// Deliberately blind to `stranded`. A stranded replica is not incomplete
    /// work, it is finished work with a bad answer, and a caller that kept
    /// retrying on account of it would never settle. Its own signal is the
    /// `replicas-recoverable` readiness condition.
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
    /// Whether a flusher holds the turn right now.
    ///
    /// A flag rather than a lock because a committer that cannot flush must
    /// park on `progress` and stay queued in `pending`, not sit in a lock's
    /// wait list holding an entry nobody can batch. See [`Wal::flush`].
    flushing: bool,
    /// The highest Lamport [`Wal::quiesce`] handed back, or zero if it never
    /// dropped anything.
    ///
    /// Read by whoever reopens this partition, because it is the one fact that
    /// decides whether reopening at the same epoch is safe. Lamports above the
    /// committed prefix can be on a replica — the append landed and the reply
    /// was lost — and a reopened owner starts assigning again from the prefix.
    /// Reopening at the same epoch would therefore issue new bytes under a
    /// version a replica already holds different bytes for, and a replica skips
    /// an entry at or below its durable position without comparing bytes, so it
    /// would acknowledge the replacement and keep the original. A reopen above
    /// this mark needs a higher epoch, which is the only thing that makes a
    /// replica give its tail up. See [`Wal::surrendered_lamport`].
    surrendered: Lamport,
    /// What this owner has established about each replica: where its log ends,
    /// and whether this log can still extend it. Seeded `Unestablished` for
    /// every configured replica when the log opens, so that having heard
    /// nothing yet is never mistaken for having heard something good.
    ///
    /// This is the single per-replica record. Both questions anyone asks about
    /// a replica's health are read off it — [`Wal::replicas_behind`], the work
    /// a catch-up can still do, and [`Wal::beyond_retention`], the work it
    /// cannot — so the two can never disagree about the same node.
    ///
    /// `acked` above is not a second copy of this. It answers a different
    /// question: whether one named node confirmed one named Lamport, which is
    /// what the ADR 0001 coherence quorum waits on. This one answers where a
    /// replica stands relative to the log as a whole.
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
    /// The peers this owner replicates to, swapped rather than fixed.
    ///
    /// Placement is not an ownership change: the control plane can widen or
    /// narrow a replica set without moving the epoch, and an owner that could
    /// only learn its peers at open time would keep acknowledging writes
    /// against the peer list it was born with. Held as an `Arc<[NodeId]>` so
    /// a replication pass takes one cheap snapshot and never holds the lock
    /// across an await.
    replicas: Mutex<Arc<[NodeId]>>,
    /// How many peer acknowledgements make a write durable. Fixed for the life
    /// of the owner: it is a property of the durability contract, not of who
    /// happens to be listening.
    durability_acks: usize,
    /// How long a batch keeps retrying while a replica has never answered.
    /// Fixed for the life of the owner, like the quorum it is trying to reach.
    replication_retry_budget: Duration,
    state: Mutex<OwnerState>,
    progress: tokio::sync::Notify,
    /// How many fsyncs this owner has issued, and how many entries they
    /// carried.
    ///
    /// Throughput on a real disk is entries divided by fsyncs: one fsync costs
    /// the same whether it carries one write or a hundred, so this ratio is
    /// the write path's efficiency expressed as a number. It is counted rather
    /// than inferred because the batching is emergent — it depends on how many
    /// writers happen to be queued when a flush wins the lock — and reasoning
    /// about that from the code has already produced two wrong answers.
    flushes: AtomicU64,
    flushed_entries: AtomicU64,
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

        // The log's own fence and the published manifest are two independent
        // records of who the cluster last agreed owns this partition, and a
        // grant below either of them is stale. The manifest matters most
        // exactly when the log cannot help: a replacement worker has no fence
        // record at all, so without this it would take a superseded grant and
        // start writing under a dead epoch.
        let seen = log.epoch().await.max(config.hydrated.epoch);
        if config.epoch < seen {
            return Err(Error::StaleEpoch {
                partition: config.partition,
                got: config.epoch,
                current: seen,
            });
        }
        log.record_fence(config.epoch).await?;
        // Before the position is read, not after: everything below depends on
        // `durable` being where this node's history actually starts.
        log.hydrate(config.hydrated.through).await;
        let durable = log.durable_lamport().await;
        let catch_up = config
            .replicas
            .iter()
            .map(|node| (*node, ReplicaCatchUp::Unestablished))
            .collect();

        let wal = Arc::new(Self {
            runtime,
            log,
            partition: config.partition,
            replicas: Mutex::new(config.replicas.into()),
            durability_acks: config.durability_acks,
            replication_retry_budget: config.replication_retry_budget,
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
                flushing: false,
                surrendered: Lamport::ZERO,
                catch_up,
                fatal: None,
            }),
            progress: tokio::sync::Notify::new(),
            flushes: AtomicU64::new(0),
            flushed_entries: AtomicU64::new(0),
        });

        // Opening above the epoch this node's own log recorded means this
        // incarnation is claiming the partition from whatever held it before —
        // a promotion, or a split parent reopening after its abort bumped the
        // epoch. Either way a peer can be holding entries above `durable` that
        // this node has no record of, and `durable` is where this owner is
        // about to start assigning Lamports again. Telling the peers where
        // history ends *here*, rather than letting the first append carry the
        // news, is what stops a peer acknowledging a reissued Lamport as a
        // retransmission of the bytes it already has.
        //
        // Skipped on a same-epoch reopen because nothing new is being claimed:
        // the peers already hold this epoch, the cut would be a no-op on the
        // replica side, and a restart should not pay a round trip per replica
        // to learn that.
        if config.epoch > seen {
            wal.fence_peers(config.epoch, durable).await?;
        }
        Ok(wal)
    }

    /// Tells every replica that this owner's history ends at `truncate_above`,
    /// so a peer holding entries beyond it drops them.
    ///
    /// The cut is the position this owner resumes from, and it is deliberately
    /// the same cut the first append at this epoch would have applied — this
    /// only moves it earlier, to before any write can be admitted. See the
    /// crate docs on replica divergence for why discarding is safe rather than
    /// merely convenient.
    ///
    /// Errors only when a peer says this owner is already stale, which is not
    /// a retryable condition and must stop it. A peer that cannot be reached is
    /// left to the first append it sees at this epoch, which truncates it the
    /// same way; blocking ownership on an unreachable peer would trade a
    /// narrow window for an outage.
    async fn fence_peers(&self, epoch: Epoch, truncate_above: Lamport) -> Result<()> {
        let replicas = self.replicas();
        if replicas.is_empty() {
            return Ok(());
        }
        let request = FenceRequest {
            partition: self.partition,
            epoch,
            truncate_above,
        };
        // Concurrently, because these are bounded by a per-call timeout and a
        // dead peer would otherwise add its whole timeout to the latency of
        // every peer behind it in the list.
        let sent = join_all(
            replicas
                .iter()
                .map(|node| self.send(*node, METHOD_FENCE, request.encode()))
                .collect::<Vec<_>>(),
        )
        .await;
        for reply in sent {
            if let Ok(WalResponse::StaleEpoch { current }) = reply {
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

    /// How many fsyncs this owner has issued, and how many entries they
    /// carried.
    ///
    /// The ratio is what decides write throughput against a real disk, where
    /// an fsync costs milliseconds regardless of how much it carries. One
    /// entry per fsync means every write pays a full disk round trip on its
    /// own. See issue #147.
    #[must_use]
    pub fn flush_stats(&self) -> (u64, u64) {
        (
            self.flushes.load(Ordering::Relaxed),
            self.flushed_entries.load(Ordering::Relaxed),
        )
    }

    /// The highest Lamport this owner handed back in [`Wal::quiesce`], or zero
    /// if it never gave anything up.
    ///
    /// Whoever reopens the partition has to ask, because the answer decides
    /// whether the reopen may happen at the same epoch. Everything in
    /// `(committed_lamport, this]` was dropped from this node's log while
    /// possibly sitting on a replica — the append landed and the reply was
    /// lost — and a reopened owner resumes assigning from the committed prefix,
    /// so those Lamports are about to be issued a second time with different
    /// bytes under them. A replica skips an entry at or below its own durable
    /// position without comparing bytes, so at an unchanged epoch it would
    /// acknowledge the replacement and go on holding the original: two values
    /// at one version, a divergent replica read, and the replacement lost if
    /// that replica is later promoted. Only a higher epoch makes a replica give
    /// its tail up, so a reopen above this mark must carry one.
    #[must_use]
    pub fn surrendered_lamport(&self) -> Lamport {
        self.state().surrendered
    }

    /// The advertised replicas a catch-up still owes a pass, meaning those not
    /// known to hold the committed prefix and not proven unreachable from this
    /// log.
    ///
    /// Read off [`Wal::catch_up_status`], which is the crate's only
    /// per-replica record, so this cannot disagree with
    /// [`Wal::beyond_retention`] about a node. The three states divide cleanly:
    ///
    /// - `Unestablished` counts as behind, but only when there is history to
    ///   hand over. Nothing has been heard from the replica, and a pass is
    ///   exactly what establishes something; an empty log has nothing to
    ///   establish and the first write will do it. This is stronger than
    ///   trusting an acknowledgement map, because a promoted or restarted
    ///   owner starts here rather than starting from a number that predates
    ///   its epoch.
    /// - `Following` counts as behind while its position is short of the
    ///   committed prefix. Under load the append path keeps the position
    ///   current, so a healthy partition answers empty without anyone asking.
    /// - `Stranded` never counts. A retry cannot produce entries the log no
    ///   longer holds, and treating it as pending would pin the owner in a
    ///   permanent retry loop instead of reporting the fault. It leaves
    ///   through [`Wal::beyond_retention`] and the `replicas-recoverable`
    ///   readiness condition.
    ///
    /// One limit is inherited rather than fixed here: a position only ever
    /// rises, so a replica that confirmed the prefix and then lost its disk
    /// reads as current until an append finds the gap and turns it into
    /// `Stranded`. Lowering on a heartbeat report would close that and open a
    /// worse one, since a reply that raced an append is stale by exactly the
    /// same shape and would put a healthy cluster into a permanent catch-up
    /// loop.
    #[must_use]
    pub fn replicas_behind(&self) -> Vec<NodeId> {
        let committed = self.committed_lamport();
        self.catch_up_status()
            .into_iter()
            .filter(|(_, status)| match status {
                ReplicaCatchUp::Unestablished => committed > Lamport::ZERO,
                ReplicaCatchUp::Following { through } => *through < committed,
                ReplicaCatchUp::Stranded(_) => false,
            })
            .map(|(node, _)| node)
            .collect()
    }

    /// How far this owner's advertised replicas trail its committed prefix.
    ///
    /// Read off the same per-replica record as [`Wal::replicas_behind`], so the
    /// count here and the nodes named there cannot disagree. An `Unestablished`
    /// replica counts as behind by the full committed prefix once there is
    /// history to hand over, because nothing is known to have reached it; a
    /// `Following` replica counts by exactly what it has not yet confirmed; a
    /// `Stranded` one is not lag and is excluded.
    #[must_use]
    pub fn replication_lag(&self) -> ReplicationLag {
        let committed = self.committed_lamport();
        if committed == Lamport::ZERO {
            return ReplicationLag::default();
        }
        let mut lag = ReplicationLag::default();
        for (_, status) in self.catch_up_status() {
            let through = match status {
                ReplicaCatchUp::Following { through } if through < committed => through,
                ReplicaCatchUp::Unestablished => Lamport::ZERO,
                // Caught up, or stranded and reported elsewhere.
                ReplicaCatchUp::Following { .. } | ReplicaCatchUp::Stranded(_) => continue,
            };
            lag.replicas_behind += 1;
            lag.max_lamports = lag.max_lamports.max(committed.get() - through.get());
        }
        lag
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
    ///
    /// The current peer list is what this iterates, not the recorded one, so a
    /// replica placed onto the partition after the log opened has no entry yet
    /// and reads as `Unestablished`. That is the honest answer and it is what
    /// makes a catch-up owe the new peer a pass; a replica removed from the
    /// set stops being reported without its record having to be hunted down.
    #[must_use]
    pub fn catch_up_status(&self) -> Vec<(NodeId, ReplicaCatchUp)> {
        let replicas = self.replicas();
        let state = self.state();
        let mut status: Vec<(NodeId, ReplicaCatchUp)> = replicas
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
                ReplicaCatchUp::Unestablished | ReplicaCatchUp::Following { .. } => None,
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
    ///
    /// This is also what lets [`Wal::replicas_behind`] work on an owner that
    /// has replicated nothing since it opened. Without it the only evidence a
    /// fresh owner had was an acknowledgement, which needs a write, and an
    /// idle partition never produces one.
    pub async fn note_replica_position(&self, node: NodeId, replica_durable: Lamport) {
        let retained_from = self.log.retained_from().await;
        // The log holds one contiguous run, so a replica can be extended from
        // here exactly when the entry it needs next has not been dropped.
        let status = if replica_durable.next() >= retained_from {
            ReplicaCatchUp::Following {
                through: replica_durable,
            }
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

    /// Records a conclusion about a replica, logging only when the kind of
    /// conclusion changes.
    ///
    /// Catch-up is judged on every append and every heartbeat, so a line per
    /// judgement would bury the one that matters under thousands that do not.
    /// A following replica moves its position constantly and none of those
    /// moves is news; only becoming stranded, or stopping being stranded, is.
    ///
    /// A `Following` position only ever rises. A heartbeat reply and an append
    /// acknowledgement are two samples of the same number taken at different
    /// moments, and the reply can easily be the older one; letting it win
    /// would make a busy partition look permanently behind and put it in a
    /// catch-up loop it does not need. The cost is the inherited limit
    /// documented on [`Wal::replicas_behind`], and the case that actually
    /// matters — a replica that lost entries — is caught by the append path
    /// as `Stranded` rather than by a lowered watermark.
    fn record_catch_up(&self, node: NodeId, status: ReplicaCatchUp) {
        let (changed, status) = {
            let mut state = self.state();
            let held = state.catch_up.get(&node).copied();
            let status = match (held, status) {
                (
                    Some(ReplicaCatchUp::Following { through: held }),
                    ReplicaCatchUp::Following { through },
                ) => ReplicaCatchUp::Following {
                    through: through.max(held),
                },
                (_, status) => status,
            };
            state.catch_up.insert(node, status);
            (!held.is_some_and(|held| held.same_kind(status)), status)
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
            ReplicaCatchUp::Following { through } => tracing::info!(
                partition = self.partition.get(),
                node = node.get(),
                through = through.get(),
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
        // itself without hydration needing a second bookkeeping path. The
        // position it confirmed goes in with it, which is what a catch-up
        // reads back to decide whether it still owes this node a pass.
        self.record_catch_up(node, ReplicaCatchUp::Following { through });
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
        let lamport = self.submit(op)?;
        self.wait_for(lamport).await
    }

    /// Assigns a Lamport, queues the entry, and makes sure a flusher is
    /// running, without waiting for any of it.
    ///
    /// Split from [`Wal::commit`] so a caller can learn the Lamport while it
    /// still holds whatever lock orders submissions, and hand the rest of the
    /// work to something that outlives the request. Nothing here awaits, so a
    /// caller cannot be cancelled part way through submitting.
    pub fn submit(self: &Arc<Self>, op: WalOp) -> Result<Lamport> {
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

        // Deliberately not awaited. The flush runs on a task of its own so that
        // dropping a caller cancels only the waiting, never the writing. A
        // cancelled flush used to take its batch down with it, and because
        // those Lamports were already issued, the log kept a permanent hole
        // that every later batch then failed to replicate across. See #150.
        self.ensure_flusher();
        Ok(lamport)
    }

    /// Starts a flusher unless one is already running.
    ///
    /// The turn is claimed here, synchronously, rather than inside the spawned
    /// task. Claiming it in the task would let a second committer see no
    /// flusher and spawn another one before the first had started.
    fn ensure_flusher(self: &Arc<Self>) {
        {
            let mut state = self.state();
            if state.flushing || state.fatal.is_some() {
                return;
            }
            state.flushing = true;
        }
        let wal = Arc::clone(self);
        self.runtime.spawn(async move {
            wal.flush_loop().await;
        });
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

        self.fence_peers(epoch, durable).await
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
    /// [`CatchUpPass::behind`] so the caller keeps the work pending, and one
    /// the log can no longer reach comes back in [`CatchUpPass::stranded`] so
    /// the caller stops trying and reports it instead.
    pub async fn catch_up_replicas(&self) -> Result<CatchUpPass> {
        let replicas = self.replicas();
        let (epoch, horizon, fatal) = {
            let state = self.state();
            (state.epoch, state.replicated, state.fatal.clone())
        };
        if let Some(fatal) = fatal {
            return Err(fatal);
        }
        let mut result = CatchUpPass {
            horizon,
            caught_up: Vec::new(),
            behind: Vec::new(),
            stranded: Vec::new(),
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
        }

        // Read back off the per-replica record rather than from the outcomes,
        // because the call returning and the replica having arrived are
        // different facts: a reply can come from a node that is still short of
        // the horizon, a node that answered nothing has proved nothing, and a
        // backfill that hit the retention cliff answers the same way as a lost
        // packet while meaning something a retry cannot fix. The record knows
        // the difference; the outcomes do not.
        for (node, status) in self.catch_up_status() {
            match status {
                ReplicaCatchUp::Following { through } if through >= horizon => {
                    result.caught_up.push(node);
                }
                ReplicaCatchUp::Stranded(_) => result.stranded.push(node),
                ReplicaCatchUp::Following { .. } | ReplicaCatchUp::Unestablished => {
                    result.behind.push(node);
                }
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
    /// It cannot strand a replica, which is worth stating because this is the
    /// one operation in the crate that makes a log shorter. A replica is
    /// stranded when the entry it needs next is older than
    /// [`crate::PartitionLog::retained_from`], and cutting a tail can only
    /// lower that horizon, never raise it — the entries being dropped are the
    /// newest ones, not the oldest. So a replica judged `Following` before a
    /// quiesce is still `Following` after it, and a draining owner cannot
    /// clear the `replicas-recoverable` readiness condition by giving up its
    /// own tail.
    ///
    /// [ADR 0002]: https://github.com/orbita-rocks/orbita/blob/develop/docs/adr/0002-key-versions-are-partition-lamports.md
    pub async fn quiesce(&self) -> Result<Lamport> {
        let (committed, dropped_from) = {
            let mut state = self.state();
            if let Some(fatal) = &state.fatal {
                return Err(fatal.clone());
            }
            // Set before the check below, so that a caller that has to retry
            // is not racing new writes on the way back in. It also freezes the
            // committed prefix: from here [`advance`] refuses to raise it, so a
            // batch acknowledged during the truncate below cannot report an
            // entry committed that this call is about to drop. Without that,
            // the acknowledgement racing the truncate is exactly how a planned
            // handoff loses an acknowledged write (issue #77).
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
        if dropped_from <= committed {
            return Ok(dropped_from);
        }

        self.log.truncate_above(committed).await?;
        let durable = self.log.durable_lamport().await;
        {
            let mut state = self.state();
            state.durable_local = durable;
            state.next_lamport = durable;
            // Everything the truncation dropped sat above the committed prefix,
            // so no client was told it succeeded — and with the prefix frozen,
            // none can be. Marking the dropped span failed is what turns the
            // still-pending `commit` of a batch that was in flight when the
            // drain began into the `Unavailable` its client must get, rather
            // than leaving it to wait forever on a prefix that will never reach
            // it. The floor is the committed prefix, so a kept entry is never
            // failed by this.
            state.failed_through = state.failed_through.max(dropped_from);
            // Recorded so that whoever reopens this partition can see that
            // Lamports were handed back and refuse to reissue them under an
            // epoch the replicas still hold. See [`Wal::surrendered_lamport`].
            state.surrendered = state.surrendered.max(dropped_from);
            // A batch that is gone from the log cannot be rescued by a reply
            // that is still on the wire.
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

    /// Drains the pending queue, one fsync per pass, until nothing is waiting.
    ///
    /// Runs on its own task, started by [`Wal::ensure_flusher`], and holds the
    /// flusher turn for as long as it runs. Committers only ever wait for their
    /// own Lamport, so no request drives this and no request can cancel it.
    ///
    /// Draining in a loop is what produces the batching. Everything that
    /// arrives while one pass is fsyncing is still queued when the next pass
    /// takes the queue, so a group of concurrent writers shares one sync. The
    /// alternative — each committer flushing for itself — collapses under
    /// closed-loop clients, which is what clients are: every acknowledgement
    /// releases one writer, which arrives alone and syncs alone. Measured, that
    /// equilibrium is 1.31 entries per sync, and it is issue #147.
    async fn flush_loop(self: Arc<Self>) {
        // The turn was claimed by `ensure_flusher`. This gives it back however
        // this task ends, so a panic cannot wedge the partition.
        let mut turn = FlushTurn::new(&self);
        loop {
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
            self.flush_batch(batch).await;
            // Nothing arrived while that pass ran, so there is no work left to
            // hand on. Released here rather than by the guard so that the check
            // and the release are one critical section: releasing first would
            // let a committer see a free turn, spawn a flusher, and race this
            // one for the same queue.
            let idle = {
                let mut state = self.state();
                if state.pending.is_empty() {
                    state.flushing = false;
                    turn.disarm();
                    true
                } else {
                    false
                }
            };
            if idle {
                self.progress.notify_waiters();
                return;
            }
        }
    }

    /// Writes and fsyncs one batch, then hands its replication to a task of
    /// its own.
    ///
    /// The hand-off is what keeps the disk and the network from taking turns.
    /// Awaiting replication here would mean the next batch could not start its
    /// fsync until the previous one had reached a quorum, so every cycle would
    /// cost a sync plus a network round trip and `pending` would grow for a
    /// whole peer timeout whenever a replica was slow. The flusher goes back
    /// for the next batch as soon as this one is on the local disk.
    async fn flush_batch(self: &Arc<Self>, batch: Vec<Pending>) {
        let first = batch[0].entry.lamport;
        let last = batch[batch.len() - 1].entry.lamport;
        let frames: Vec<Bytes> = batch.iter().map(|p| p.frame.clone()).collect();

        // Counted here rather than around the append, because this is the
        // point where the batch size is decided and one fsync is committed to.
        self.flushes.fetch_add(1, Ordering::Relaxed);
        self.flushed_entries
            .fetch_add(frames.len() as u64, Ordering::Relaxed);

        if let Err(e) = self.log.append_frames(&frames, last).await {
            // A failed local write leaves the log in a state we cannot reason
            // about, so this owner stops rather than guessing. The flusher's
            // guard gives the turn back on the way out, so the committers
            // parked on `progress` are woken to see the fatal rather than
            // stranded behind a flush that is never coming.
            self.set_fatal(e);
            return;
        }
        {
            let mut state = self.state();
            state.durable_local = last;
            state.inflight.push(Batch { last, acked: false });
        }
        // Wakes the committers this batch carried as soon as it is on the disk,
        // rather than making them wait out the replication below.
        self.progress.notify_waiters();

        // Replication runs on its own task so the flusher can take the next
        // batch straight back to the disk. Ordering across batches is still the
        // replica's to enforce: it refuses an append that would leave a hole,
        // which is the same guarantee that held when two flushes could overlap
        // here before.
        let wal = Arc::clone(self);
        self.runtime.spawn(async move {
            wal.replicate_batch(batch, first, last).await;
        });
    }

    /// Ships one batch to the replicas and folds the answer into the committed
    /// prefix.
    async fn replicate_batch(self: Arc<Self>, batch: Vec<Pending>, first: Lamport, last: Lamport) {
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

    /// Resolves once `lamport` is committed, or fails once it is known not to
    /// be.
    ///
    /// Public because the applier waits on it independently of the request that
    /// submitted the write. That is what lets a cancelled request lose only its
    /// answer: the entry is still committed, resolved and applied by whoever is
    /// waiting on it here.
    pub async fn wait_for(&self, lamport: Lamport) -> Result<Lamport> {
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
                    // Indeterminate, not Unavailable. The entry is already
                    // fsynced to this node's log by the time replication is
                    // attempted, so this is not a write that did not happen —
                    // it is one nobody can speak for, and the next open will
                    // replay it. See issue #187.
                    return Err(Error::Indeterminate(format!(
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
    /// Ships a batch to the replicas, retrying while a replica that has never
    /// answered might still be opening the partition.
    ///
    /// Nothing makes a replica open a partition before its owner starts taking
    /// writes for it. The owner admits writes as soon as its own host is open,
    /// and each holder reconciles the map on its own poll, so for a partition
    /// that has just appeared — a split child, a new placement, or every
    /// partition on a cluster that has just started — the first thing that ever
    /// contacts a replica is the first client write. It arrives before the
    /// replica has registered the partition and is refused, and the write fails
    /// with a quorum it could have reached a moment later. See issue #168.
    ///
    /// Retrying is safe because an append is idempotent by construction: a
    /// replica skips entries at or below what it already holds, and a
    /// per-partition gate keeps two appends from interleaving. Re-sending to a
    /// peer that already acknowledged costs a round trip and changes nothing.
    ///
    /// **It retries only while some replica has never been heard from.** A
    /// replica that is `Unestablished` has not answered since this log opened,
    /// which is what a warming replica looks like; one that was following and
    /// has now gone is an outage, and an outage should fail quickly rather than
    /// hold every write for the whole budget. That distinction is why this uses
    /// the catch-up state rather than simply counting attempts.
    async fn replicate(self: &Arc<Self>, request: AppendRequest) -> Result<()> {
        let mut backoff = REPLICATION_RETRY_INITIAL;
        let mut spent = Duration::ZERO;
        loop {
            let outcome = self.replicate_once(request.clone()).await;
            let Err(error) = outcome else {
                return outcome;
            };
            // A fenced owner is not warming up. It is done.
            if matches!(error, Error::StaleEpoch { .. }) {
                return Err(error);
            }
            if spent >= self.replication_retry_budget || !self.any_replica_unestablished() {
                return Err(error);
            }
            let pause = backoff.min(self.replication_retry_budget - spent);
            self.runtime.clock().sleep(pause).await;
            spent += pause;
            backoff = (backoff * 2).min(REPLICATION_RETRY_MAX_BACKOFF);
        }
    }

    /// Whether any replica has yet to answer anything since this log opened.
    ///
    /// The signal that separates a partition still coming up from one whose
    /// peers have gone. Deliberately "any" rather than "the ones this batch
    /// needed": the quorum is a count, not a named set, so a batch that fell
    /// short cannot say which replica would have completed it.
    fn any_replica_unestablished(&self) -> bool {
        let state = self.state();
        self.replicas().iter().any(|node| {
            !matches!(
                state.catch_up.get(node),
                Some(ReplicaCatchUp::Following { .. }) | Some(ReplicaCatchUp::Stranded(_))
            )
        })
    }

    async fn replicate_once(self: &Arc<Self>, request: AppendRequest) -> Result<()> {
        let replicas = self.replicas();
        // Sized from the durability requirement, not from how many peers happen
        // to be listening. Peers beyond the quorum are followers: they get the
        // append so their view stays current, and they cannot hold a write up
        // by being slow or gone. Capped by the list because a quorum larger
        // than the peers that exist could never be met.
        let required = self.durability_acks.min(replicas.len());
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
    /// One attempt only. A replica that is still behind after this is behind by
    /// more than the log holds, and could not close the distance from object
    /// storage either, so there is nothing this node can send it.
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
///
/// Clamped to what this node itself holds. The committed prefix is a claim
/// that a quorum stored the entry, and this node is a member of every quorum
/// it counts, so a watermark above `durable_local` would be a claim about
/// entries this node has given back. Only [`Wal::quiesce`] can lower the
/// durable position, and only after the replies that could arrive late were
/// already for entries no client is waiting on.
/// Holds the flusher turn and gives it back however the flush ends.
///
/// The turn is a flag rather than a lock, and a flag does not unwind. A commit
/// future dropped part way through its fsync — a disconnected client, an
/// expired deadline, an aborted task — would otherwise leave the turn held by a
/// flusher that no longer exists. Every later committer would queue in
/// `pending`, park on `progress` behind a flush that is never coming, and wait
/// forever; `quiesce` could not finish either, so the partition would stay
/// unavailable until the process restarted. The lock this replaced released on
/// unwind for free. This is that property put back on purpose.
struct FlushTurn<'a, R: Runtime> {
    wal: &'a Wal<R>,
    held: bool,
}

impl<'a, R: Runtime> FlushTurn<'a, R> {
    fn new(wal: &'a Wal<R>) -> Self {
        Self { wal, held: true }
    }

    /// Gives up responsibility for the turn, because the caller has just
    /// cleared it itself.
    ///
    /// The path that completes a batch releases the turn under the same lock
    /// that publishes `durable_local`, and that pairing is load bearing: see
    /// [`Wal::flush`]. Dropping this guard afterwards must not clear a flag a
    /// later flusher has since taken.
    fn disarm(&mut self) {
        self.held = false;
    }
}

impl<R: Runtime> Drop for FlushTurn<'_, R> {
    fn drop(&mut self) {
        if !self.held {
            return;
        }
        // Deliberately not `Wal::state`, which panics on a poisoned lock. This
        // can run while a panic is already unwinding, and panicking there
        // aborts the process. A poisoned owner is already finished; failing to
        // clear a flag on it changes nothing.
        if let Ok(mut state) = self.wal.state.lock() {
            state.flushing = false;
        }
        self.wal.progress.notify_waiters();
    }
}

fn advance(state: &mut OwnerState) {
    // A quiescing owner has already chosen the position it is truncating the
    // log down to and frozen its committed prefix there. A batch whose
    // acknowledgement lands during the truncate must not raise the prefix past
    // that floor: [`Wal::quiesce`] drops everything above it, so reporting one
    // of those entries committed here would hand a client an acknowledgement
    // for a write the log no longer holds and the next owner never sees. That
    // is the acknowledged write that goes missing across a planned handoff in
    // issue #77. The drop the drain performs is the source of truth once
    // quiescing has begun; the late reply is stale by exactly the shape the
    // freeze exists to reject.
    if state.quiesced {
        return;
    }
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
