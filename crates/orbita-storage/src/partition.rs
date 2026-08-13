//! One partition: a mutable table of recent writes over immutable segments.
//!
//! Per [ADR 0006](../../../docs/adr/0006-partitions-are-an-index-over-immutable-objects.md)
//! a partition is a memory-resident index over immutable objects in object
//! storage, plus a sorted table of writes that have been acknowledged but not
//! yet flushed into a segment. A read consults the table first and the index
//! second; a write goes into the table and becomes durable through the
//! replicated log, not through anything this crate does.
//!
//! # Flushing and the two durability levels
//!
//! Flushing turns the mutable table into a segment and swaps the manifest, on
//! a size trigger and on explicit request. The manifest's `committed_lamport`
//! is the flush horizon: a restart rebuilds the index from the manifest and
//! replays the log above that horizon, which [`Partition::apply`]'s
//! idempotence makes safe to overdo. Time-based flushing is the caller's to
//! schedule, because this crate has nowhere to run a timer: it takes a
//! `Runtime` but deliberately owns no background tasks, so that everything it
//! does happens inside a call a test can drive.
//!
//! # What compaction is for
//!
//! A lookup never consults more than one segment, so the only reason to merge
//! is to reclaim space: expired records, aged tombstones, and shadowed
//! versions. It runs after enough full, size-triggered flushes amortise a
//! rewrite, on a bounded timer cadence, and on explicit request. Timer flushes
//! do not compact based on segment count, because tiny durability segments
//! should not repeatedly rewrite a whole partition. Compaction republishes
//! through the same manifest swap as a flush, so one that dies half way costs
//! objects rather than data.
//! The objects it replaces are deleted once the new manifest is committed;
//! anything a crash strands is left for the orphan sweep,
//! [`Partition::sweep_orphans`], which reclaims what an interrupted compaction
//! or an abandoned commit leaves behind. A cache for values read back out of
//! segments is still deliberate scope for later.

use crate::cache::ValueCache;
use crate::cursor::Cursor;
use crate::mutation::{version_at, Mutation, MutationOp};

use bytes::Bytes;
use orbita_core::{
    Epoch, Error, KeyRange, Lamport, PartitionId, Record, Result, Version, WriteCondition,
    MAX_KEY_BYTES, MAX_LIST_BYTES, MAX_LIST_LIMIT, MAX_VALUE_BYTES,
};
use orbita_format::segment::{Segment, SegmentBuilder};
use orbita_format::{
    compact, load_manifest, CommitPlan, ExternalValue, FormatError, PartitionPath, PartitionWriter,
    RecordValue, SegmentEntry, SegmentRecord, Snapshot, SweepReport,
};
use orbita_objectstore::{ObjectError, ObjectStore};
use orbita_runtime::{Clock, Runtime};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

/// How long a tombstone survives before compaction may reclaim it.
///
/// A tombstone exists so a conditional write can distinguish "this key never
/// existed" from "this key was deleted". That distinction only helps a caller
/// still in the middle of a retry loop, and a day is far longer than any such
/// loop while still being short enough that a delete-heavy workload does not
/// grow without bound.
pub const TOMBSTONE_RETENTION_MILLIS: u64 = 24 * 60 * 60 * 1000;

/// How large the mutable table may grow before a write triggers a flush.
///
/// The number bounds recovery work as much as memory: everything above the
/// flush horizon has to be replayed from the log on a restart. Eight
/// mebibytes keeps both small while staying far above any single record, so a
/// flush amortises over many writes rather than chasing each one.
const FLUSH_TRIGGER_BYTES: u64 = 8 * 1024 * 1024;

/// How many full, size-triggered flushes amortise a compaction check.
///
/// Reads never pay for segment count, so this is purely about space: every
/// overwrite strands a shadowed record until a merge reclaims it.
///
/// A check after four flushes keeps reclaimable bytes from accumulating far
/// beyond one bounded pass. A check may find that every new record is live, in
/// which case rewriting it would reclaim nothing and the debt is discharged
/// without touching object storage.
const COMPACT_TRIGGER_FULL_FLUSHES: usize = COMPACTION_INPUT_FLUSHES as usize;

/// How many timer passes may elapse before small segments are compacted.
///
/// This is deliberately larger than the old segment-count trigger: sparse
/// partitions should not rewrite themselves every few minutes, but even one
/// expired timer segment must eventually be reclaimed.
const COMPACT_TRIGGER_TIMER_FLUSHES: usize = 120;

/// The bookkeeping cost charged to the flush trigger per entry, on top of the
/// key and value bytes. An estimate is all a trigger needs.
const ENTRY_OVERHEAD_BYTES: u64 = 64;

/// How many flushes' worth of segment one compaction pass merges.
///
/// Named separately so the trigger cadence and per-pass budget describe the
/// same unit. A pass may read less because it selects only segments with bytes
/// to reclaim, and leaves its trigger armed when more candidates remain.
const COMPACTION_INPUT_FLUSHES: u64 = 4;

/// How many bytes of segment a single compaction pass will read and rewrite.
///
/// Four flushes' worth. Compaction used to merge the whole partition, which
/// made its cost grow with the partition rather than with what had accumulated
/// since the last pass: measured, a 140 MiB partition stalled writes for 805ms
/// and dropped throughput to 47 ops/s, and it only got worse as the partition
/// grew. Bounding the input keeps that cost flat.
///
/// Larger reclaims more per pass and stalls longer; smaller does the opposite
/// and leaves more segments live, which costs nothing on the read path because
/// an exact index consults exactly one segment however many there are. Four
/// flushes is small enough that a pass is short and large enough that a pass is
/// worth taking the lock for. See issue #143.
const COMPACTION_INPUT_BYTES: u64 = COMPACTION_INPUT_FLUSHES * FLUSH_TRIGGER_BYTES;

/// How long the orphan sweep leaves an unreferenced object alone before it is a
/// deletion candidate.
///
/// This bound is the whole safety of the sweep, and the number is chosen to
/// exceed both quantities it has to clear rather than the smaller of them. An
/// object becomes unreferenced the instant a manifest swap drops it, but a
/// reader that opened a snapshot against the *previous* manifest is still
/// entitled to fetch it, and an in-flight commit writes objects nothing
/// references until its own manifest swap makes them live. So the grace period
/// has to outlast the longest read a client can hold open *and* the longest
/// commit a writer can be part way through. An hour is far above either on this
/// engine — a scan drains in seconds and a commit is a handful of conditional
/// writes — while still bounding how long a genuinely stranded object lingers.
/// It is a default rather than a law: a deployment whose reads or commits run
/// longer raises it, which is why the sweep takes it as an argument.
pub const DEFAULT_SWEEP_GRACE_MILLIS: u64 = 60 * 60 * 1000;

/// How much the orphan sweep widens the grace period to cover the backend's own
/// worst-case internal clock skew.
///
/// The sweep judges an object's age by comparing its write time to a reference
/// time, both read from the object store's clock domain. But even one backend
/// is a distributed system: S3 stamps two objects, or answers a "now", from
/// servers whose clocks need not agree to the millisecond. This allowance is
/// added on top of the grace period so an object has to clear the grace *and*
/// the skew before it is touched, so that two backend clocks disagreeing can
/// never be mistaken for elapsed time. Five minutes is generous for a
/// well-run store and costs only a slightly later reclaim.
pub const DEFAULT_SWEEP_SKEW_MILLIS: u64 = 5 * 60 * 1000;

/// What one index entry costs beyond its key bytes: the key handle, the
/// location record, and this entry's share of the B-tree node holding them.
///
/// An estimate rather than a measurement, because the only exact answer comes
/// from allocator internals and the number exists so an operator can watch a
/// resource, not so anything can be reconciled against it.
const INDEX_ENTRY_OVERHEAD_BYTES: u64 = 64;

/// What a conditional write did.
///
/// A failed condition is not an error at this layer. The caller needs the
/// version that was actually present in order to retry, and threading that
/// through an error type makes every call site pattern match on an error
/// anyway. The edge turns this into a gRPC status with
/// [`WriteOutcome::into_result`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOutcome {
    /// The key now sits at this version.
    ///
    /// Normally this is the Lamport the caller supplied. It is lower when the
    /// write was a no-op, which happens when a delete finds a key that is
    /// already deleted. A caller that sees a lower version knows nothing was
    /// committed, so there is nothing to replicate and the Lamport it reserved
    /// is free to use again.
    Applied { version: Version },
    /// The write was not committed. `found` is the version visible at the
    /// moment the condition was evaluated, and `None` means the key was
    /// absent, expired, or deleted.
    ConditionFailed {
        condition: WriteCondition,
        found: Option<Version>,
    },
}

impl WriteOutcome {
    /// Collapses the outcome into the error the API surface reports.
    pub fn into_result(self) -> Result<Version> {
        match self {
            WriteOutcome::Applied { version } => Ok(version),
            WriteOutcome::ConditionFailed {
                condition: WriteCondition::IfNotPresent,
                ..
            } => Err(Error::AlreadyExists),
            WriteOutcome::ConditionFailed {
                condition: WriteCondition::IfVersion(expected),
                found,
            } => Err(Error::VersionMismatch {
                expected,
                actual: found,
            }),
            WriteOutcome::ConditionFailed {
                condition: WriteCondition::None,
                ..
            } => Err(Error::Internal(
                "an unconditional write reported a condition failure".to_string(),
            )),
        }
    }
}

/// A child partition to prepare during a split.
///
/// The id and epoch are the ones the control plane allocated for the child, and
/// the range is the child's half of the parent's range. See
/// [`Partition::prepare_child_partitions`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildSpec {
    pub id: PartitionId,
    pub epoch: Epoch,
    pub range: KeyRange,
}

/// One key and its record, as returned by a scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanEntry {
    pub key: Bytes,
    pub record: Record,
}

/// One page of a scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanPage {
    pub entries: Vec<ScanEntry>,
    /// Absent when the scan reached the end of the range, which is the only
    /// signal a caller needs to stop paging.
    pub cursor: Option<Bytes>,
}

/// How much of a range a single [`Partition::scan`] page may return.
///
/// A page stops at whichever bound it reaches first: the entry count the caller
/// asked for, or the byte budget that keeps the encoded response under the
/// transport ceiling. The byte budget exists because the count alone lets a
/// caller demand a multi-gigabyte page — a thousand maximum-size values is ten
/// gigabytes — that the store would assemble in full and then be unable to send.
/// Bounding the scan itself means the store never materialises more than one
/// page's worth, and the caller pages the rest through the returned cursor.
#[derive(Debug, Clone, Copy)]
pub struct ScanBudget {
    /// Maximum entries, `1..=MAX_LIST_LIMIT`.
    pub max_entries: u32,
    /// Maximum bytes of returned payload: keys always, and values when
    /// `include_values`. This mirrors [`orbita_core::MAX_LIST_BYTES`]; the
    /// per-entry protobuf framing is left to
    /// [`orbita_core::MESSAGE_OVERHEAD_BYTES`] of headroom, so a full page still
    /// fits under the advertised message ceiling.
    pub max_bytes: u64,
    /// Whether values count toward `max_bytes`. It mirrors whether the caller
    /// will put them on the wire: a keys-only page must not be truncated early
    /// for values it is not going to send.
    pub include_values: bool,
}

impl ScanBudget {
    /// A page bounded only by an entry count, using the default list byte
    /// budget and returning values. This is the shape a plain `LIST` uses and
    /// the one tests reach for when the byte bound is not what they exercise.
    pub fn of_entries(max_entries: u32) -> Self {
        Self {
            max_entries,
            max_bytes: MAX_LIST_BYTES,
            include_values: true,
        }
    }
}

/// One entry as the partition holds it, in the mutable table or a segment.
///
/// This is distinct from [`Record`] because a tombstone is a stored entry with
/// no visible record, and because the read path has to reason about entries
/// that exist but are not visible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Stored {
    pub version: Version,
    pub expires_at_millis: Option<u64>,
    /// True for a tombstone, meaning the key was deleted at `version`.
    pub deleted: bool,
    pub value: Bytes,
}

impl Stored {
    /// Whether the entry is past its deadline, which for a live record means
    /// it is invisible and for a tombstone means it is reclaimable.
    pub fn is_expired_at(&self, now_millis: u64) -> bool {
        self.expires_at_millis.is_some_and(|e| now_millis >= e)
    }

    /// The record a reader should see, or `None` if the key is absent as far
    /// as the API is concerned.
    pub fn visible_at(&self, now_millis: u64) -> Option<Record> {
        if self.deleted || self.is_expired_at(now_millis) {
            return None;
        }
        Some(Record {
            value: self.value.clone(),
            version: self.version,
            expires_at_millis: self.expires_at_millis,
        })
    }
}

/// What a published manifest says about a partition.
///
/// The horizon and the epoch travel together on purpose. A caller that reads a
/// manifest in order to rebuild a partition has, in the same read, learned who
/// the cluster last agreed owns it, and separating the two invites the caller
/// to use the data and discard the ownership evidence. That is exactly the
/// mistake that lets a deposed owner collect an acknowledgement from a replica
/// which hydrated on its behalf: the replica proved the sender was deposed and
/// then answered it anyway.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Hydration {
    /// The ownership epoch of the writer that published the manifest. Nobody
    /// below this epoch can still be the owner.
    pub epoch: Epoch,
    /// Everything at or below this Lamport is in the published segments.
    pub through: Lamport,
}

/// Where a flushed key's winning record sits.
#[derive(Debug, Clone, Copy)]
struct Loc {
    /// A position in [`State::segments`], which moves wholesale on compaction
    /// and only ever grows on flush, so a held position never dangles.
    segment: usize,
    offset: u64,
    record_length: u32,
}

/// Whether a record decoded out of a read-ahead window is the one the index
/// currently points at, and so worth spending cache budget on.
///
/// Segment as well as offset. Offsets repeat across segments — every segment
/// holds a record just after its header — so a check on offset alone admits a
/// superseded copy whenever its winner happens to sit at the same place in
/// another segment. Nothing would serve the dead copy, because the cache is
/// keyed by object and no index entry resolves to it, but it would hold budget
/// a live record needs, which is a cache that is too small wearing a disguise.
fn is_winning_record(
    index: &BTreeMap<Bytes, Loc>,
    key: &[u8],
    segment: usize,
    offset: u64,
) -> bool {
    index
        .get(key)
        .is_some_and(|held| held.segment == segment && held.offset == offset)
}

/// How many index entries one miss may look at while planning its window.
///
/// The walk skips entries belonging to other segments, and on a segment whose
/// keys have mostly been superseded there may be very few of its own left.
/// Without a cap the walk would then run to the end of the partition's index
/// looking for neighbours that are not there, so a random read over an old
/// segment would cost a scan of every later key — while holding the read lock,
/// and once per miss.
///
/// Generous enough that a segment which is still mostly current never reaches
/// it: at the default budget and the record sizes measured, a full window is
/// about 250 entries. Reaching this cap means the segment is sparse here, and
/// giving up is the right answer rather than a compromise, because read-ahead
/// over a mostly-superseded segment would fetch bytes nothing is going to ask
/// for.
const MAX_READ_AHEAD_SCAN: usize = 1024;

/// Plans the byte range a miss should fetch.
///
/// Free of the partition so it can be tested against a synthetic index: the
/// interesting cases are a sparse segment and a dense one, and building either
/// through the write path would say more about flushing than about this.
fn plan_read_ahead(index: &BTreeMap<Bytes, Loc>, loc: Loc, key: &[u8], budget: u64) -> ReadWindow {
    let start = loc.offset;
    let mut end = start + u64::from(loc.record_length);
    let mut followers = 0usize;
    let mut scanned = 0usize;
    if budget == 0 {
        return ReadWindow {
            start,
            end,
            followers,
            scanned,
        };
    }
    for (_, next) in index.range::<[u8], _>((Bound::Excluded(key), Bound::Unbounded)) {
        if scanned >= MAX_READ_AHEAD_SCAN {
            break;
        }
        scanned += 1;
        if next.segment != loc.segment || next.offset < end {
            continue;
        }
        let candidate = next.offset + u64::from(next.record_length);
        if candidate.saturating_sub(start) > budget {
            break;
        }
        end = candidate;
        followers += 1;
    }
    ReadWindow {
        start,
        end,
        followers,
        scanned,
    }
}

/// The byte range a single miss fetches.
struct ReadWindow {
    start: u64,
    end: u64,
    /// Index entries the plan looked at, including ones it skipped.
    ///
    /// Carried because the bound on that walk is otherwise invisible: the cap
    /// changes no window this returns — a sparse segment plans the same one
    /// record either way — so the only way to hold it, or to notice it
    /// binding, is to measure the work rather than the answer.
    scanned: usize,
    /// Records beyond the one that was asked for. Zero means the window is
    /// exactly one record and the strict length check still applies.
    followers: usize,
}

/// Everything that changes together under the partition's one lock.
struct State {
    /// Acknowledged writes not yet in a segment. Holds at most one entry per
    /// key, because a later write at the same key shadows the earlier one
    /// completely and the earlier one is already durable in the log.
    memtable: BTreeMap<Bytes, Stored>,
    /// The flush trigger's estimate of the table's cost.
    memtable_bytes: u64,
    /// The highest Lamport committed, across local writes and replayed
    /// mutations alike. Volatile: it is rebuilt on restart from the manifest
    /// horizon plus the log replay the host performs.
    committed: Lamport,
    /// The manifest's `committed_lamport`, meaning everything at or below it
    /// is in the segments and nothing above it is.
    flushed: Lamport,
    /// The ownership epoch of the writer that published the manifest behind
    /// `flushed`.
    ///
    /// Held because a manifest is evidence about who owns this partition, not
    /// only about where its data ends. Only an owner that won the
    /// compare-and-swap at that epoch could have published it, so anything
    /// claiming to own the partition at a lower epoch has been deposed. The
    /// hydration path is the one place that reads a manifest without already
    /// knowing the current epoch, and it is the one place that has to say so.
    flushed_epoch: Epoch,
    /// The published segments, in manifest order.
    segments: Vec<SegmentEntry>,
    /// Every flushed key and where its winning record lives.
    index: BTreeMap<Bytes, Loc>,
    /// What [`State::index`] is estimated to cost in memory, maintained
    /// alongside it rather than walked on demand. Every caller asking is a
    /// heartbeat, and a per-key walk once a second is a bill that grows with
    /// the partition for a number nobody needs to the byte.
    index_bytes: u64,
    /// Timer flushes can be tiny, so only full memtables pay toward a rewrite.
    full_flushes_since_compaction: usize,
    /// Timer passes since the last merge of a non-empty partition.
    timer_flushes_since_compaction: usize,
    /// Original segment names the current bounded expiry sweep has not read.
    ///
    /// Size-triggered compaction can identify stale segments from the index,
    /// but expiry requires reading records. Names make the sweep cohort stable
    /// while flushes and earlier bounded outputs append new segments between
    /// passes; neither can extend the sweep or be mistaken for examined input.
    compaction_sweep_pending: Vec<(Option<PartitionId>, String)>,
    /// Timer credits the current sweep covered when its cohort was captured.
    /// Flushes between passes add credits above this snapshot and must survive
    /// cohort completion because their segments were not swept.
    compaction_sweep_timer_debt: usize,
    /// Memtable size at the last replica manifest refresh attempt.
    reclaim_attempted_at_bytes: u64,
}

/// A single partition of one keyspace.
///
/// The partition is told which key range it owns and rejects anything outside
/// it. It does not know who owns it, which epoch is current, or that other
/// partitions exist; the epoch it takes at open is stamped into object names
/// and manifests so a deposed writer cannot collide with its replacement.
pub struct Partition<R: Runtime> {
    runtime: R,
    store: Arc<dyn ObjectStore>,
    path: PartitionPath,
    range: KeyRange,
    writer: PartitionWriter<dyn ObjectStore>,
    /// Records read back out of segments, shared with every other partition
    /// on the node so the budget bounds the node rather than the partition.
    ///
    /// Deliberately not inside `state`: a hit only reads, but keeping recency
    /// means mutating, and doing that under the partition's `RwLock` would
    /// make every cache hit take the write lock and serialise the read path
    /// behind it. Its own lock is held for a map lookup and nothing else.
    cache: Arc<ValueCache>,
    /// How many bytes a miss may fetch, counting from the record that missed.
    ///
    /// Zero fetches exactly the record asked for, which is what every release
    /// before this did.
    read_ahead_bytes: u64,
    /// One lock rather than a lock per key is deliberate: a partition already
    /// has exactly one writer in production, because the owning worker
    /// serializes writes before they reach here, so striping would buy
    /// contention we do not have in exchange for a class of bugs we would
    /// rather not reason about. Reads share it.
    state: tokio::sync::RwLock<State>,
    /// True while this partition is a split parent whose children reference its
    /// segments in place, per
    /// [ADR 0009](../../../docs/adr/0009-a-split-shares-the-parents-segments.md).
    /// It freezes flush, compaction, and sweep for the whole split, because
    /// compaction deletes the self-written segments it replaces (see
    /// [`Partition::compact`]) and those are exactly the objects the children
    /// now point at — deleting one dangles a child's reference and loses the
    /// data. Checked under [`Partition::state`], and set before the child
    /// manifests are published, so any maintenance that acquires the lock after
    /// the children exist sees it and stands down, while one that ran before
    /// touched only segments no child had yet referenced.
    maintenance_frozen: std::sync::atomic::AtomicBool,
    /// How many bytes of segment one compaction pass reads and rewrites.
    ///
    /// A field rather than the constant directly so a test can set a bound it
    /// can actually reach. Exercising the bounded path against the production
    /// value would mean writing tens of megabytes per test, and a bound that is
    /// only ever tested at "merges everything" is not tested at all.
    compaction_input_bytes: std::sync::atomic::AtomicU64,
}

impl<R: Runtime> Partition<R> {
    /// Opens the partition, rebuilding its index from the manifest if one has
    /// been published.
    ///
    /// The caller replays its log above [`Partition::committed_lamport`]
    /// afterwards; nothing here reads a log. The runtime is taken even though
    /// only its clock is used, so that adding background work later does not
    /// change every caller's signature.
    pub async fn open(
        runtime: R,
        store: Arc<dyn ObjectStore>,
        path: PartitionPath,
        epoch: Epoch,
        range: KeyRange,
    ) -> Result<Self> {
        let writer = PartitionWriter::open(Arc::clone(&store), path.clone(), epoch)
            .await
            .map_err(format_error)?;

        let mut state = State {
            memtable: BTreeMap::new(),
            memtable_bytes: 0,
            committed: Lamport::ZERO,
            flushed: Lamport::ZERO,
            flushed_epoch: Epoch::ZERO,
            segments: Vec::new(),
            index: BTreeMap::new(),
            index_bytes: 0,
            full_flushes_since_compaction: 0,
            timer_flushes_since_compaction: 0,
            compaction_sweep_pending: Vec::new(),
            compaction_sweep_timer_debt: 0,
            reclaim_attempted_at_bytes: 0,
        };
        if let Some(snapshot) = Snapshot::open(Arc::clone(&store), path.clone())
            .await
            .map_err(format_error)?
        {
            adopt(&mut state, &snapshot);
        }

        Ok(Self {
            runtime,
            store,
            path,
            range,
            writer,
            // Off until a caller supplies one. A zero-budget cache is a
            // working cache that holds nothing, so the read path has no branch
            // for whether caching is configured.
            cache: Arc::new(ValueCache::new(0)),
            read_ahead_bytes: 0,
            state: tokio::sync::RwLock::new(state),
            maintenance_frozen: std::sync::atomic::AtomicBool::new(false),
            compaction_input_bytes: std::sync::atomic::AtomicU64::new(COMPACTION_INPUT_BYTES),
        })
    }

    /// Serves reads of flushed keys out of `cache` instead of the object
    /// store, sharing it with every other partition given the same one.
    ///
    /// Additive rather than an argument to [`Partition::open`] because a
    /// partition without a cache is correct, only slow, and every existing
    /// caller should keep working without deciding about memory it does not
    /// own. The one that does own it — the node — passes one in.
    ///
    /// Sharing is the point. A per-partition budget on a node holding
    /// thousands of partitions is thousands of budgets and no bound at all,
    /// and it would give a cold tenant the same memory as the hot one paying
    /// for the node.
    #[must_use]
    pub fn with_value_cache(mut self, cache: Arc<ValueCache>) -> Self {
        self.cache = cache;
        self
    }

    /// Fetches up to `bytes` per miss instead of one record.
    ///
    /// Separate from the cache because the two answer different questions. The
    /// cache decides what a second read of a key costs; this decides what the
    /// first read of its neighbours costs, and the two are worth turning on and
    /// measuring independently.
    #[must_use]
    pub fn with_read_ahead(mut self, bytes: u64) -> Self {
        self.read_ahead_bytes = bytes;
        self
    }

    /// Freezes flush, compaction, and sweep because this partition is a split
    /// parent whose children now reference its segments in place (ADR 0009).
    ///
    /// Set before the child manifests are published. Because every maintenance
    /// path re-checks it under [`Partition::state`], and publication also holds
    /// that lock, a maintenance pass either ran before the children existed
    /// (touching only unshared segments) or sees the freeze and stands down;
    /// there is no interleaving in which it deletes a segment a child points at.
    pub fn freeze_maintenance(&self) {
        self.maintenance_frozen
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Lifts the freeze, for a split that was abandoned. A completed split
    /// retires this partition instead.
    pub fn resume_maintenance(&self) {
        self.maintenance_frozen
            .store(false, std::sync::atomic::Ordering::Release);
    }

    /// Whether maintenance is frozen for an in-progress split.
    #[must_use]
    pub fn is_maintenance_frozen(&self) -> bool {
        self.maintenance_frozen
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// The key range this partition owns.
    #[must_use]
    pub fn range(&self) -> &KeyRange {
        &self.range
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Record>> {
        self.check_key(key)?;
        let now = self.now_millis();
        let state = self.state.read().await;
        Ok(self
            .load(&state, key)
            .await?
            .and_then(|s| s.visible_at(now)))
    }

    /// Writes a value at `lamport`, subject to `condition`.
    ///
    /// The key's new version is `lamport`, per ADR 0002. The caller supplies it
    /// rather than the partition allocating one because the owner has to know
    /// the Lamport before it replicates, and the version has to be the same
    /// number on all three nodes. See the crate docs for why assignment lives
    /// with the caller.
    ///
    /// The returned outcome carries either the new version or the version that
    /// was actually present, because a caller that lost a compare-and-swap
    /// race retries against what it found.
    pub async fn put(
        &self,
        lamport: Lamport,
        key: &[u8],
        value: Bytes,
        ttl: Option<Duration>,
        condition: WriteCondition,
    ) -> Result<WriteOutcome> {
        self.check_key(key)?;
        if value.len() > MAX_VALUE_BYTES {
            return Err(Error::TooLarge {
                what: "value",
                size: value.len(),
                limit: MAX_VALUE_BYTES,
            });
        }

        let now = self.now_millis();
        let mut state = self.state.write().await;
        check_lamport(&state, lamport)?;

        let existing = self.load(&state, key).await?;
        if let Some(failure) = evaluate(condition, existing.as_ref(), now) {
            return Ok(failure);
        }

        let entry = Stored {
            version: version_at(lamport),
            expires_at_millis: ttl.map(|d| absolute_expiry(now, d)),
            deleted: false,
            value,
        };
        self.commit(&mut state, lamport, key, entry, true).await?;
        Ok(WriteOutcome::Applied {
            version: version_at(lamport),
        })
    }

    /// Deletes a key at `lamport`, subject to `condition`.
    ///
    /// Deleting an absent key succeeds and writes a tombstone, so that a retry
    /// of a delete that already happened is not reported as a failure. See the
    /// crate docs for why the tombstone is explicit.
    pub async fn delete(
        &self,
        lamport: Lamport,
        key: &[u8],
        condition: WriteCondition,
    ) -> Result<WriteOutcome> {
        self.check_key(key)?;

        let now = self.now_millis();
        let mut state = self.state.write().await;
        check_lamport(&state, lamport)?;

        let existing = self.load(&state, key).await?;
        if let Some(failure) = evaluate(condition, existing.as_ref(), now) {
            return Ok(failure);
        }

        // Deleting something already deleted commits nothing, so the Lamport
        // is not consumed. Moving the version here would invalidate the token
        // held by the caller that is retrying, and the retry would never
        // converge.
        if let Some(entry) = existing.as_ref() {
            if entry.deleted && !entry.is_expired_at(now) {
                return Ok(WriteOutcome::Applied {
                    version: entry.version,
                });
            }
        }

        let entry = Stored {
            version: version_at(lamport),
            expires_at_millis: Some(now.saturating_add(TOMBSTONE_RETENTION_MILLIS)),
            deleted: true,
            value: Bytes::new(),
        };
        self.commit(&mut state, lamport, key, entry, true).await?;
        Ok(WriteOutcome::Applied {
            version: version_at(lamport),
        })
    }

    /// Returns one page of keys under `prefix`, in key order.
    ///
    /// The page is collected under the partition's read lock, so it is a
    /// consistent view of the partition even while writes wait. Consistency
    /// does not extend across pages, which the product requirements state
    /// outright.
    pub async fn scan(
        &self,
        prefix: &[u8],
        cursor: Option<&[u8]>,
        budget: ScanBudget,
    ) -> Result<ScanPage> {
        let limit = budget.max_entries;
        if limit == 0 || limit > MAX_LIST_LIMIT {
            return Err(Error::InvalidArgument(format!(
                "scan limit must be between 1 and {MAX_LIST_LIMIT}"
            )));
        }
        if prefix.len() > MAX_KEY_BYTES {
            return Err(Error::TooLarge {
                what: "prefix",
                size: prefix.len(),
                limit: MAX_KEY_BYTES,
            });
        }

        let cursor = cursor.map(|raw| Cursor::decode(raw, prefix)).transpose()?;
        let Some(start) = self.scan_start(prefix, cursor.as_ref()) else {
            return Ok(ScanPage {
                entries: Vec::new(),
                cursor: None,
            });
        };

        let now = self.now_millis();
        // The read lock is held across the store fetches below, so a slow
        // store read stalls writers for the length of a page. Immaterial for
        // the filesystem and in-memory stores that exist today; worth a
        // bounded fetch or a lock drop per entry when a remote bucket lands.
        let state = self.state.read().await;
        let from = (Bound::Included(start.as_slice()), Bound::Unbounded);
        let mut table = state.memtable.range::<[u8], _>(from).peekable();
        let mut flushed = state.index.range::<[u8], _>(from).peekable();

        let mut entries = Vec::new();
        let mut used_bytes = 0u64;
        let mut exhausted = true;
        loop {
            // The next key is the smaller of the two heads, and the mutable
            // table shadows the index outright: its entry for a key is always
            // the newer one, because a flushed record can only be overtaken by
            // a later write and a later write sits in the table.
            let from_table = match (table.peek(), flushed.peek()) {
                (None, None) => break,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (Some((held, _)), Some((published, _))) => held <= published,
            };
            let (key, stored) = if from_table {
                let (key, entry) = table.next().expect("peeked");
                if flushed.peek().is_some_and(|(shadowed, _)| *shadowed == key) {
                    flushed.next();
                }
                (key.clone(), entry.clone())
            } else {
                let (key, loc) = flushed.next().expect("peeked");
                (key.clone(), self.fetch(&state, *loc, key).await?)
            };
            if !key.starts_with(prefix) || !self.range.contains(&key) {
                break;
            }
            // Expired and deleted keys are skipped rather than counted, so a
            // page is short only at the end of the range. A caller that saw
            // them would have to filter them itself, and would learn about
            // keys it is not allowed to see.
            let Some(record) = stored.visible_at(now) else {
                continue;
            };
            if entries.len() == limit as usize {
                exhausted = false;
                break;
            }
            // The byte budget bounds a page that the count alone would let grow
            // past what the transport can carry. The first entry is always
            // returned, however large, so a single value over the budget pages
            // rather than wedging the scan; every entry after it must fit. The
            // entry is left unconsumed for the next page — the cursor resumes
            // strictly after the last entry returned, which sits before it.
            let entry_bytes = key.len() as u64
                + if budget.include_values {
                    record.value.len() as u64
                } else {
                    0
                };
            if !entries.is_empty() && used_bytes + entry_bytes > budget.max_bytes {
                exhausted = false;
                break;
            }
            used_bytes += entry_bytes;
            entries.push(ScanEntry { key, record });
        }

        let next = if exhausted {
            None
        } else {
            entries
                .last()
                .map(|e| Cursor::new(e.key.clone(), prefix).encode())
        };
        Ok(ScanPage {
            entries,
            cursor: next,
        })
    }

    /// Replays a mutation the owner already committed.
    ///
    /// Applying the same mutation twice leaves the same state, because the
    /// partition records the highest Lamport it has committed and ignores
    /// anything at or below it. That is what makes log replay after a crash
    /// safe, including replay of entries the manifest already covers, and it
    /// assumes the log delivers mutations in Lamport order, which the log
    /// guarantees.
    ///
    /// Unlike [`Partition::put`], a Lamport that has already been committed is
    /// silently ignored rather than rejected. A replayed log entry is expected
    /// and routine; an owner assigning a stale Lamport to a fresh write is a
    /// bug, and the two should not report the same way.
    ///
    /// The absolute expiry comes from the mutation rather than being recomputed
    /// here. If a replica read its own clock, two nodes would disagree about
    /// when a lock expires.
    pub async fn apply(&self, mutation: &Mutation) -> Result<()> {
        self.check_key(&mutation.key)?;
        if let MutationOp::Put { value, .. } = &mutation.op {
            if value.len() > MAX_VALUE_BYTES {
                return Err(Error::TooLarge {
                    what: "value",
                    size: value.len(),
                    limit: MAX_VALUE_BYTES,
                });
            }
        }

        let mut state = self.state.write().await;
        if mutation.lamport <= state.committed {
            return Ok(());
        }

        let entry = match &mutation.op {
            MutationOp::Put {
                value,
                expires_at_millis,
            } => Stored {
                version: mutation.version(),
                expires_at_millis: *expires_at_millis,
                deleted: false,
                value: value.clone(),
            },
            MutationOp::Delete {
                tombstone_expires_at_millis,
            } => Stored {
                version: mutation.version(),
                expires_at_millis: Some(*tombstone_expires_at_millis),
                deleted: true,
                value: Bytes::new(),
            },
        };
        // Replicas must not publish this partition's manifest. The host calls
        // `flush_if_needed` only for the current owner after this apply lands.
        self.commit(&mut state, mutation.lamport, &mutation.key, entry, false)
            .await
    }

    /// The highest Lamport this partition has committed.
    ///
    /// On a replica this is the applied Lamport that ADR 0001's read path
    /// reports to the owner. On an owner it is the last version handed out.
    /// One number serves both because local writes and replayed mutations
    /// advance the same sequence.
    pub async fn committed_lamport(&self) -> Result<Lamport> {
        Ok(self.state.read().await.committed)
    }

    /// The highest Lamport made durable by a published manifest.
    ///
    /// A WAL may checkpoint through this boundary, and never through
    /// [`Partition::committed_lamport`], because acknowledged writes above it
    /// still exist only in the replicated log.
    pub async fn flushed_lamport(&self) -> Result<Lamport> {
        Ok(self.state.read().await.flushed)
    }

    /// The partition's size, which is what the leader splits on.
    ///
    /// This is the published segments plus an estimate of the mutable table,
    /// not an exact figure. A split threshold does not need one, and shadowed
    /// records inflate it only until a compaction runs.
    pub async fn size_bytes(&self) -> Result<u64> {
        let state = self.state.read().await;
        Ok(state.memtable_bytes + state.segments.iter().map(|s| s.bytes).sum::<u64>())
    }

    /// What this partition's memory-resident index is estimated to cost.
    ///
    /// ADR 0006 keeps the index resident whether or not the values are, which
    /// makes memory the resource a worker exhausts first and makes this the
    /// number an operator watches. It excludes the mutable table, whose cost
    /// is already charged to [`Partition::size_bytes`], so adding the two
    /// double counts nothing.
    pub async fn index_bytes(&self) -> Result<u64> {
        Ok(self.state.read().await.index_bytes)
    }

    /// Flushes the mutable table into a segment and publishes it.
    ///
    /// A no-op when the table is empty. This is the explicit trigger ADR 0006
    /// names; the size trigger calls the same path from inside a write.
    ///
    /// A no-op too while a split has frozen maintenance: the flush counter it
    /// would advance is what eventually triggers compaction, and compaction
    /// deletes the very segments the children now reference. The check is under
    /// the state lock so it cannot race the publication of those child
    /// manifests. See [`Partition::freeze_maintenance`].
    pub async fn flush(&self) -> Result<()> {
        let mut state = self.state.write().await;
        if self.is_maintenance_frozen() {
            return Ok(());
        }
        if !state.segments.is_empty() {
            state.timer_flushes_since_compaction += 1;
        }
        // Compaction is no longer spilled out of a flush. Both triggers are
        // counters now, and [`Partition::compact_if_needed`] is the one place
        // that acts on them, so a flush costs a flush whoever asked for it.
        self.flush_locked(&mut state, false).await
    }

    /// Flushes only when the mutable table has crossed the size trigger.
    ///
    /// Ownership is deliberately the caller's decision. Replicas apply the
    /// same mutations but only the current owner may publish a manifest.
    ///
    /// Frozen during a split: a size-triggered flush can spill into compaction
    /// the same way a timer flush can, so it stands down too. A split closes
    /// write admission before it retires the parent, so the table it declines
    /// to flush here is bounded by the brief pre-quiesce window.
    pub async fn flush_if_needed(&self) -> Result<()> {
        let mut state = self.state.write().await;
        if self.is_maintenance_frozen() {
            return Ok(());
        }
        if state.memtable_bytes < FLUSH_TRIGGER_BYTES {
            return Ok(());
        }
        self.flush_locked(&mut state, true).await
    }

    /// Durably prepares child storage for a split, copying no segment bytes.
    ///
    /// This is the data-plane half of the worker-prepared split from ADR 0009.
    /// It flushes the parent so every committed record is in a segment, then
    /// publishes a manifest for each child that references the parent's
    /// segments *in place* — each child names exactly the parent segments that
    /// hold a key in the child's range, as cross-partition references, with
    /// bounds clamped to the keys the child actually owns. No object is copied;
    /// the children and the parent share the same immutable segment objects
    /// until a later compaction rewrites them under a child's own directory.
    ///
    /// Every child begins at the parent's flush horizon, which is the returned
    /// value, so a child's Lamport sequence continues the parent's and no key's
    /// version regresses (ADR 0002). The caller must have quiesced the parent
    /// first, so that horizon is the parent's final committed position and no
    /// write can land above it after the children are published.
    ///
    /// Idempotent: a child whose manifest is already published at or above its
    /// intended epoch is left untouched, so a retried preparation — which a
    /// worker will do whenever a report is lost — publishes nothing new.
    pub async fn prepare_child_partitions(&self, children: &[ChildSpec]) -> Result<Lamport> {
        let mut state = self.state.write().await;
        // The memtable is not shareable: only segments can be referenced, so
        // everything committed has to be flushed into one before a child can
        // point at it.
        self.flush_locked(&mut state, false).await?;
        let horizon = state.flushed;
        for child in children {
            self.prepare_one_child(&state, child, horizon).await?;
        }
        Ok(horizon)
    }

    /// Durably prepares one merged child over two adjacent parents without
    /// copying segment bytes.
    ///
    /// Both parents must already be quiesced and maintenance-frozen by the
    /// caller. Their remaining memtables are flushed, their physical segment
    /// sources are flattened into one manifest, and the child horizon is the
    /// greater committed horizon. Existing record versions are untouched, so
    /// equal Lamports on different keys remain valid while the first new child
    /// write is allocated strictly above both source sequences (ADR 0002).
    pub async fn prepare_merged_partition(
        &self,
        upper: &Self,
        child: &ChildSpec,
    ) -> Result<Lamport> {
        let combined = KeyRange::merge(&self.range, &upper.range).ok_or_else(|| {
            Error::InvalidArgument("merge sources must be adjacent and in lower/upper order".into())
        })?;
        if combined != child.range || self.path.keyspace_id() != upper.path.keyspace_id() {
            return Err(Error::InvalidArgument(
                "merged child range and keyspace must exactly match both sources".into(),
            ));
        }

        // Callers always pass lower then upper, so taking the locks in range
        // order gives every merge one lock order and avoids a dual-parent
        // deadlock. Both maintenance freezes are already set, so no compaction
        // can invalidate either state while these guards are held.
        let mut lower_state = self.state.write().await;
        self.flush_locked(&mut lower_state, false).await?;
        let mut upper_state = upper.state.write().await;
        upper.flush_locked(&mut upper_state, false).await?;
        let horizon = lower_state.flushed.max(upper_state.flushed);

        let mut by_object: BTreeMap<(PartitionId, String), SegmentEntry> = BTreeMap::new();
        for (partition, state) in [(self, &*lower_state), (upper, &*upper_state)] {
            for mut entry in state.segments.iter().cloned() {
                let source = entry
                    .source
                    .unwrap_or_else(|| partition.path.partition_id());
                entry.source = Some(source);
                by_object
                    .entry((source, entry.name.clone()))
                    .and_modify(|existing| {
                        if entry.min_key < existing.min_key {
                            existing.min_key = entry.min_key.clone();
                        }
                        if entry.max_key > existing.max_key {
                            existing.max_key = entry.max_key.clone();
                        }
                    })
                    .or_insert(entry);
            }
        }
        let mut segments: Vec<_> = by_object.into_values().collect();
        segments.sort_by(|a, b| a.source.cmp(&b.source).then(a.name.cmp(&b.name)));

        let child_path = self.path.for_partition(child.id);
        if let Some((existing, _)) = load_manifest(self.store.as_ref(), &child_path)
            .await
            .map_err(format_error)?
        {
            if existing.epoch >= child.epoch {
                return Ok(existing.committed_lamport);
            }
        }
        let writer = PartitionWriter::open(Arc::clone(&self.store), child_path, child.epoch)
            .await
            .map_err(format_error)?;
        writer
            .commit(|_| CommitPlan {
                committed_lamport: horizon,
                range: child.range.clone(),
                segments: segments.clone(),
            })
            .await
            .map_err(format_error)?;
        Ok(horizon)
    }

    /// Publishes one child's manifest over the parent's segments.
    async fn prepare_one_child(
        &self,
        state: &State,
        child: &ChildSpec,
        horizon: Lamport,
    ) -> Result<()> {
        // The parent segments that hold at least one key in the child's range,
        // with the min and max keys the child actually owns in each. The parent
        // index already resolved which segment holds each key's winning record,
        // so referencing exactly those segments gives the child every live
        // record in its range and nothing shadowed.
        let mut refs: BTreeMap<usize, (Bytes, Bytes)> = BTreeMap::new();
        for (key, loc) in &state.index {
            if !child.range.contains(key) {
                continue;
            }
            refs.entry(loc.segment)
                .and_modify(|(min, max)| {
                    if key < min {
                        *min = key.clone();
                    }
                    if key > max {
                        *max = key.clone();
                    }
                })
                .or_insert_with(|| (key.clone(), key.clone()));
        }

        let mut child_segments: Vec<SegmentEntry> = refs
            .into_iter()
            .map(|(position, (min_key, max_key))| {
                let parent = &state.segments[position];
                // Point at where the object physically lives. Usually that is
                // this partition, but if the parent's own entry is itself a
                // shared reference (this partition was a child of a prior
                // split), follow it to the true owner so references never chain.
                let source = parent.source.unwrap_or_else(|| self.path.partition_id());
                SegmentEntry {
                    name: parent.name.clone(),
                    source: Some(source),
                    bytes: parent.bytes,
                    // The full object's record count and lamports, not the
                    // child's subset: the reader validates these against the
                    // object's own footer before it filters to the range.
                    record_count: parent.record_count,
                    min_key,
                    max_key,
                    min_lamport: parent.min_lamport,
                    max_lamport: parent.max_lamport,
                }
            })
            .collect();
        // A stable order so a re-prepared child encodes identical bytes.
        child_segments.sort_by(|a, b| a.name.cmp(&b.name).then(a.source.cmp(&b.source)));

        let child_path = self.path.for_partition(child.id);
        // Already prepared? A manifest at or above the child's epoch means an
        // earlier attempt published it, and re-publishing would only risk a
        // lost race with the child's own writer once it is live.
        if let Some((existing, _)) = load_manifest(self.store.as_ref(), &child_path)
            .await
            .map_err(format_error)?
        {
            if existing.epoch >= child.epoch {
                return Ok(());
            }
        }

        let writer = PartitionWriter::open(Arc::clone(&self.store), child_path, child.epoch)
            .await
            .map_err(format_error)?;
        writer
            .commit(|_| CommitPlan {
                committed_lamport: horizon,
                range: child.range.clone(),
                segments: child_segments.clone(),
            })
            .await
            .map_err(format_error)?;
        Ok(())
    }

    /// Reclaims replica memtable entries covered by a manifest the owner has
    /// already published.
    ///
    /// This never writes the manifest. If publication has not advanced, every
    /// entry stays in memory and the next attempt waits for another
    /// flush-sized interval of growth.
    pub async fn reclaim_published_if_needed(&self) -> Result<()> {
        {
            let mut state = self.state.write().await;
            let retry_at = state
                .reclaim_attempted_at_bytes
                .saturating_add(FLUSH_TRIGGER_BYTES);
            if state.memtable_bytes < FLUSH_TRIGGER_BYTES || state.memtable_bytes < retry_at {
                return Ok(());
            }
            state.reclaim_attempted_at_bytes = state.memtable_bytes;
        }

        let Some(snapshot) = Snapshot::open(Arc::clone(&self.store), self.path.clone())
            .await
            .map_err(format_error)?
        else {
            return Ok(());
        };
        let mut state = self.state.write().await;
        if snapshot.committed_lamport() <= state.flushed {
            return Ok(());
        }
        adopt(&mut state, &snapshot);
        Ok(())
    }

    /// Rebuilds this partition from whatever manifest the bucket currently
    /// publishes, and reports the horizon it is now current through.
    ///
    /// This is the payoff ADR 0006 promised: a worker that has to take on a
    /// partition it holds nothing for downloads the index rather than taxing a
    /// healthy peer for a copy. Building the index reads footers and key
    /// indexes only, so the cost is proportional to key count rather than to
    /// bytes, and the caller then replays its log above the returned horizon to
    /// pick up the tail the manifest does not cover.
    ///
    /// Hydrating is idempotent and safe to abandon half way. Nothing here
    /// mutates the partition until the whole snapshot has been read, so a
    /// download that fails leaves the previous state intact and the next
    /// attempt starts over; a manifest that is not ahead of what this
    /// partition already holds is a no-op rather than a rebuild. Writes that
    /// arrived above the horizon while the download ran stay in the mutable
    /// table, because the horizon is exactly the line the segments cover.
    ///
    /// Returns a horizon of `Lamport::ZERO` for a partition that has never
    /// flushed, which is a partition with nothing to download rather than a
    /// missing one.
    ///
    /// The epoch travels back with the horizon because the manifest is
    /// evidence about ownership as well as about data, and a caller that
    /// hydrated in order to serve somebody claiming to own the partition has
    /// to be able to check that claim against it. See [`Hydration`].
    pub async fn hydrate(&self) -> Result<Hydration> {
        // The manifest first, on its own. It is one small object, where opening
        // a snapshot reads the footer and key index of every segment the
        // manifest names -- bytes proportional to the partition's keys.
        //
        // That distinction is the whole point of doing this in two steps. A
        // replica with a gap the bucket cannot close asks for a rebuild on
        // *every* append it refuses, and the answer is almost always that
        // nothing moved. Paying a full index rebuild to discover that turns one
        // behind replica into a loop that saturates the node and the object
        // store, and it does not stop when the writes do: measured at issue
        // \#141 as three workers and MinIO all burning CPU on a completely idle
        // cluster, with reads of the bucket and no writes to it.
        //
        // So the cheap read decides whether the expensive one is worth doing.
        let Some((manifest, _)) = load_manifest(self.store.as_ref(), &self.path)
            .await
            .map_err(format_error)?
        else {
            // Never flushed, so there is nothing to download.
            return Ok(self.hydration().await);
        };

        {
            let mut state = self.state.write().await;
            if manifest.committed_lamport <= state.flushed {
                // Nothing to adopt but the epoch. A manifest can be republished
                // by a newer owner without the horizon moving, most obviously by
                // a compaction, and that manifest is no less proof of who owns
                // this partition than one that added records. Taking it here
                // rather than after a rebuild is what makes the common case one
                // small GET.
                state.flushed_epoch = state.flushed_epoch.max(manifest.epoch);
                return Ok(Hydration {
                    epoch: state.flushed_epoch,
                    through: state.flushed,
                });
            }
        }

        // The manifest is genuinely ahead, so the rebuild is worth its cost.
        // Re-read rather than building from the manifest just fetched: between
        // the two reads the bucket can move again, and a snapshot is the thing
        // that resolves segments and their shared sources consistently.
        let Some(snapshot) = Snapshot::open(Arc::clone(&self.store), self.path.clone())
            .await
            .map_err(format_error)?
        else {
            return Ok(self.hydration().await);
        };
        let mut state = self.state.write().await;
        if snapshot.committed_lamport() <= state.flushed {
            state.flushed_epoch = state.flushed_epoch.max(snapshot.manifest().epoch);
        } else {
            adopt(&mut state, &snapshot);
        }
        Ok(Hydration {
            epoch: state.flushed_epoch,
            through: state.flushed,
        })
    }

    /// What the last manifest this partition adopted says, without going back
    /// to the bucket.
    ///
    /// [`Partition::open`] already reads the manifest, so a caller opening a
    /// partition and then its log does not need a second round trip to learn
    /// where its history starts or who published it.
    pub async fn hydration(&self) -> Hydration {
        let state = self.state.read().await;
        Hydration {
            epoch: state.flushed_epoch,
            through: state.flushed,
        }
    }

    /// Runs one bounded sweep, which physically reclaims expired records,
    /// aged tombstones, and shadowed versions from the selected segments.
    ///
    /// Reclamation otherwise waits for the full-flush trigger, and the
    /// product promises "eventually" rather than a bound. This exists so an
    /// operator, or a test, can ask for the sweep now. The mutable table is
    /// flushed first so the merge sees everything.
    ///
    /// A no-op while a split has frozen maintenance, because compaction is the
    /// one operation that deletes a shared segment out from under a child. The
    /// check is under the state lock and the freeze is set before the child
    /// manifests are published, so a compaction can never run against a segment
    /// a child already references. See [`Partition::freeze_maintenance`].
    pub async fn compact(&self) -> Result<()> {
        let mut state = self.state.write().await;
        if self.is_maintenance_frozen() {
            return Ok(());
        }
        self.flush_locked(&mut state, false).await?;
        self.compact_locked(&mut state, true).await.map(|_| ())
    }

    /// Reclaims objects this partition no longer references and is safely done
    /// needing: the segments an interrupted compaction replaced and the objects
    /// an abandoned commit wrote but never published.
    ///
    /// Compaction deletes the objects it replaces itself, so this is the
    /// backstop for the cases where that delete never happened — a crash, or a
    /// store that refused the delete after the manifest swap already succeeded.
    /// Without it, every such object accumulates in the bucket forever.
    ///
    /// An object is deleted only when it is both unreferenced *and* provably
    /// older than `grace_millis` (widened by `max_skew_millis`) in the object
    /// store's own clock domain — never the host clock's. That is what keeps the
    /// sweep from deleting an object a slow reader still holds a reference to or
    /// an in-flight commit is about to publish; see [`DEFAULT_SWEEP_GRACE_MILLIS`]
    /// for why the bound is configured rather than assumed. An object whose
    /// backend write time is unknown is refused rather than guessed old.
    ///
    /// With `dry_run`, nothing is deleted and the returned report names what a
    /// real run would remove, so a first run against a real bucket can be looked
    /// at before it acts.
    ///
    /// This is safe to call only on the current owner. Publishing and reclaiming
    /// a partition's objects is the owner's job — a deposed writer that swept
    /// would race its replacement — which is why it lives beside [`flush`] and
    /// [`compact`] and is left to the host to invoke only for a partition it
    /// owns, exactly as flush publication is.
    ///
    /// [`flush`]: Partition::flush
    /// [`compact`]: Partition::compact
    /// `shared` names the objects under this partition's directory that other
    /// live partitions still reference in place, per ADR 0009 — a split child
    /// referencing this partition's segments. The sweep protects every one of
    /// them as if this partition's own manifest named it, so it can never
    /// delete a segment a sibling is serving from. The caller assembles the set
    /// from the sibling manifests it can see; an incomplete set is a deleted
    /// segment, so a caller that cannot be sure must pass nothing and skip.
    pub async fn sweep_orphans(
        &self,
        shared: &std::collections::BTreeSet<String>,
        grace_millis: u64,
        max_skew_millis: u64,
        dry_run: bool,
    ) -> Result<SweepReport> {
        // Frozen during a split. A split parent's children reference its
        // segments, and the `shared` set a single node can assemble may not
        // name a child owned only by another node, so the safe answer while the
        // split is in flight is to sweep nothing here at all. Compaction is also
        // frozen, so the parent's manifest is not dropping segments the sweep
        // would then chase. See [`Partition::freeze_maintenance`].
        if self.is_maintenance_frozen() {
            return Ok(SweepReport {
                dry_run,
                ..Default::default()
            });
        }
        orbita_format::sweep_partition_protecting(
            self.store.as_ref(),
            &self.path,
            shared,
            grace_millis,
            max_skew_millis,
            dry_run,
        )
        .await
        .map_err(format_error)
    }

    /// The segments this partition references in place under other partitions'
    /// directories, grouped by the partition each lives under.
    ///
    /// A node hands this to the orphan sweep so that when it sweeps a partition
    /// P, it knows which of P's objects a sibling still needs and must not
    /// delete. See [`Partition::sweep_orphans`] and ADR 0009.
    pub async fn shared_segment_sources(&self) -> BTreeMap<PartitionId, BTreeSet<String>> {
        let mut out: BTreeMap<PartitionId, BTreeSet<String>> = BTreeMap::new();
        for entry in &self.state.read().await.segments {
            if let Some(source) = entry.source {
                out.entry(source).or_default().insert(entry.name.clone());
            }
        }
        out
    }

    fn now_millis(&self) -> u64 {
        self.runtime.clock().now_millis()
    }

    fn check_key(&self, key: &[u8]) -> Result<()> {
        if key.len() > MAX_KEY_BYTES {
            return Err(Error::TooLarge {
                what: "key",
                size: key.len(),
                limit: MAX_KEY_BYTES,
            });
        }
        if !self.range.contains(key) {
            return Err(Error::InvalidArgument(
                "key is outside this partition's range".to_string(),
            ));
        }
        Ok(())
    }

    /// The entry stored under `key`, wherever it lives, without regard to
    /// visibility.
    async fn load(&self, state: &State, key: &[u8]) -> Result<Option<Stored>> {
        if let Some(entry) = state.memtable.get(key) {
            return Ok(Some(entry.clone()));
        }
        let Some(loc) = state.index.get(key) else {
            return Ok(None);
        };
        self.fetch(state, *loc, key).await.map(Some)
    }

    /// Reads one record out of its segment and verifies it is the one the
    /// The byte range one miss should fetch, and how many extra records it
    /// covers.
    ///
    /// Bounded by the index rather than by the segment's layout. Records in a
    /// segment are sorted by key and laid out in that order, so walking the
    /// index forward from the key that missed gives the records that physically
    /// follow it. Ending on a record boundary the index named means the window
    /// can never run past the data section into the key index, which decoding
    /// would read as records and would be nonsense.
    ///
    /// Entries belonging to other segments are skipped rather than ending the
    /// walk: they interleave in key order without interrupting this segment's
    /// bytes, so stopping at the first one would usually read ahead by nothing
    /// at all on a partition with more than one segment.
    fn read_ahead(&self, state: &State, loc: Loc, key: &[u8]) -> ReadWindow {
        plan_read_ahead(&state.index, loc, key, self.read_ahead_bytes)
    }

    /// Caches the records that came back behind the one that was asked for.
    ///
    /// Every insert is checked against the index before it is kept. A segment
    /// holds shadowed versions as well as winning ones, and the bytes between
    /// two indexed records may belong to a version some later segment replaced.
    /// Caching those would be harmless — they are keyed by the offset they
    /// really live at, and no index entry points there, so nothing could read
    /// them back — but they would hold budget that live records need.
    ///
    /// Decode failures end the walk rather than failing the read. The record
    /// the caller asked for has already been decoded and verified; anything
    /// after it is opportunistic, and a partial trailing record at the end of
    /// the window is expected rather than exceptional.
    fn warm_followers(
        &self,
        state: &State,
        segment: usize,
        object: &str,
        start: u64,
        mut rest: &[u8],
        mut offset_in_window: usize,
    ) {
        while !rest.is_empty() {
            let Ok((record, consumed)) = SegmentRecord::decode(rest) else {
                return;
            };
            if consumed == 0 {
                return;
            }
            let offset = start + offset_in_window as u64;
            let winning = is_winning_record(&state.index, record.key.as_ref(), segment, offset);
            if winning {
                if let RecordValue::Inline(value) = &record.value {
                    self.cache.insert(
                        object,
                        offset,
                        Stored {
                            version: Version(record.lamport.get()),
                            expires_at_millis: record.expires_at_millis,
                            deleted: record.is_tombstone(),
                            value: value.clone(),
                        },
                    );
                } else if record.is_tombstone() {
                    self.cache.insert(
                        object,
                        offset,
                        Stored {
                            version: Version(record.lamport.get()),
                            expires_at_millis: record.expires_at_millis,
                            deleted: true,
                            value: Bytes::new(),
                        },
                    );
                }
                // An external value lives in its own object (ADR 0007), so
                // warming it would be a second round trip per neighbour and
                // defeat the point. It is left for its own read to fetch.
            }
            rest = &rest[consumed..];
            offset_in_window += consumed;
        }
    }

    /// index promised.
    async fn fetch(&self, state: &State, loc: Loc, key: &[u8]) -> Result<Stored> {
        // A shared segment (a split child's cross-partition reference) lives
        // under the source partition's directory, so resolve the object there
        // rather than under this partition's own prefix. See ADR 0009.
        let entry = &state.segments[loc.segment];
        let name = &entry.name;
        let object = self.path.resolve_segment(entry);

        // The resolved object and the offset within it, never the `Loc`: a
        // `Loc` names a slot in the current segment list and compaction
        // replaces that list wholesale. Segments themselves are immutable, so
        // a hit here needs no proof of freshness beyond having been read from
        // this object at this offset once before.
        if let Some(cached) = self.cache.get(&object, loc.offset) {
            return Ok(cached);
        }

        // One round trip, however many records it comes back with. A miss is
        // dominated by the round trip rather than by the bytes — measured at
        // about 14ms against S3 for a 1 KiB record — so fetching only the
        // record that was asked for spends the expensive part of the operation
        // on the cheapest possible result.
        let window = self.read_ahead(state, loc, key);
        if window.scanned >= MAX_READ_AHEAD_SCAN {
            // Read-ahead gave up rather than kept walking. It means this
            // segment holds few of the keys that follow, which is what a
            // segment looks like when later flushes have superseded most of
            // it, so it is a hint that compaction is behind rather than a
            // problem with the read. Debug because a partition in that state
            // says it on every miss.
            tracing::debug!(
                segment = %entry.name,
                scanned = window.scanned,
                "read-ahead reached its scan bound and planned a short window"
            );
        }
        let raw = self
            .store
            .get_range(&object, window.start..window.end)
            .await
            .map_err(store_error)?;

        // The asked-for record sits at the front of the window by construction,
        // so it decodes first and the neighbours behind it are a side effect.
        let (record, consumed) = SegmentRecord::decode(&raw).map_err(format_error)?;
        if record.key != key {
            return Err(Error::Internal(format!(
                "{name} holds a different record than the index claims for this key"
            )));
        }
        if window.followers > 0 {
            self.warm_followers(
                state,
                loc.segment,
                &object,
                window.start,
                &raw[consumed..],
                consumed,
            );
        } else if consumed != raw.len() {
            // Without read-ahead the range is exactly one record, so anything
            // left over means the index and the segment disagree about how long
            // it is. With read-ahead there is deliberately more.
            return Err(Error::Internal(format!(
                "{name} holds a different record than the index claims for this key"
            )));
        }

        let deleted = record.is_tombstone();
        let value = match record.value {
            RecordValue::Tombstone => Bytes::new(),
            RecordValue::Inline(value) => value,
            // Read-side only for now: this engine writes every value inline
            // (see `segment_record_of`), so this arm serves segments written
            // by other implementations of the format. ADR 0007's write path,
            // where large values spill to their own objects, is future work.
            RecordValue::External(external) => {
                // The value object lives beside its segment, under the source
                // partition for a shared reference.
                let value_key = match entry.source {
                    None => self.path.object(&external.name),
                    Some(source) => self.path.for_partition(source).object(&external.name),
                };
                let (bytes, _) = self.store.get(&value_key).await.map_err(store_error)?;
                if bytes.len() as u64 != external.length
                    || crc32c::crc32c(&bytes) != external.crc32c
                {
                    return Err(Error::Internal(format!(
                        "external value {} does not match its record",
                        external.name
                    )));
                }
                bytes
            }
        };
        let stored = Stored {
            version: Version(record.lamport.get()),
            expires_at_millis: record.expires_at_millis,
            deleted,
            value,
        };
        // Held after decoding rather than as raw bytes, so a hit skips the
        // decode and the checksum as well as the round trip. An external value
        // is cached in the same entry, which is what makes the second read of
        // an ADR 0007 record cost nothing rather than one round trip instead
        // of two.
        self.cache.insert(&object, loc.offset, stored.clone());
        Ok(stored)
    }

    /// Commits one entry to the mutable table and advances the Lamport, then
    /// flushes if the table has crossed the trigger.
    async fn commit(
        &self,
        state: &mut State,
        lamport: Lamport,
        key: &[u8],
        entry: Stored,
        flush_on_trigger: bool,
    ) -> Result<()> {
        let key = Bytes::copy_from_slice(key);
        let added = entry_cost(&key, &entry);
        if let Some(replaced) = state.memtable.insert(key.clone(), entry) {
            state.memtable_bytes = state
                .memtable_bytes
                .saturating_sub(entry_cost(&key, &replaced));
        }
        state.memtable_bytes += added;
        state.committed = lamport;

        // A failed flush must not fail the write that tripped it. By this
        // point the write is applied here and durable in the log, and the
        // caller has been promised exactly that; reporting an error would
        // tell a client its durable write failed. The writes stay in the
        // table and the next trigger retries, which does mean a store that
        // stays down grows the table without bound. That is the honest
        // trade: the alternative is refusing durable writes because space
        // reclamation is behind, and refusal is the worse lie.
        if flush_on_trigger && state.memtable_bytes >= FLUSH_TRIGGER_BYTES {
            if let Err(error) = self.flush_locked(state, true).await {
                tracing::warn!(
                    %error,
                    memtable_bytes = state.memtable_bytes,
                    "flush failed; writes stay in the mutable table until the next trigger"
                );
            }
        }
        Ok(())
    }

    async fn flush_locked(&self, state: &mut State, full_flush: bool) -> Result<()> {
        if state.memtable.is_empty() {
            return Ok(());
        }

        let mut builder = SegmentBuilder::new(
            self.path.keyspace_id(),
            self.path.partition_id(),
            self.writer.epoch(),
        );
        for (key, entry) in &state.memtable {
            builder
                .push(&segment_record_of(key, entry))
                .map_err(format_error)?;
        }
        let built = builder.finish().map_err(format_error)?;
        let published = self
            .writer
            .put_segment(&built)
            .await
            .map_err(format_error)?;

        let mut segments = state.segments.clone();
        segments.push(published);
        let committed = state.committed;
        let range = self.range.clone();
        let manifest = self
            .writer
            .commit(|_| CommitPlan {
                committed_lamport: committed,
                range: range.clone(),
                segments: segments.clone(),
            })
            .await
            .map_err(format_error)?;

        // Everything in the new segment wins over anything flushed before it,
        // because every record here carries a Lamport above the old horizon.
        let position = manifest.segments.len() - 1;
        for entry in built.index.entries() {
            let replaced = state.index.insert(
                entry.key.clone(),
                Loc {
                    segment: position,
                    offset: entry.offset,
                    record_length: entry.record_length,
                },
            );
            // A key already in the index moved to a newer segment rather than
            // arriving, and the entry costs the same either way.
            if replaced.is_none() {
                state.index_bytes += index_entry_cost(&entry.key);
            }
        }
        state.flushed_epoch = state.flushed_epoch.max(manifest.epoch);
        state.segments = manifest.segments;
        state.memtable.clear();
        state.memtable_bytes = 0;
        state.flushed = committed;
        if full_flush {
            state.full_flushes_since_compaction += 1;
        }

        // Compaction used to run here, from inside whichever client write
        // crossed the flush trigger. It rewrites the whole partition, so that
        // put a stall proportional to the partition's size on one unlucky
        // request: measured at issue \#143 as p99 spikes to 1.26s and
        // throughput swinging between 47 and 2406 ops/s at a fixed
        // concurrency, purely on where the trigger happened to land.
        //
        // The counter still moves here, because a flush is what makes
        // compaction worth doing. Deciding to act on it belongs to
        // [`Partition::compact_if_needed`], which the host calls on its own
        // cadence.
        Ok(())
    }

    /// Compacts if enough has been flushed since the last one to make it worth
    /// the work, and does nothing otherwise.
    ///
    /// The counterpart to [`Partition::flush_if_needed`]: one pass, no timer,
    /// no task. The host owns the cadence, which is what keeps a simulated run
    /// able to drive maintenance a step at a time rather than racing a timer
    /// this crate started for itself.
    ///
    /// This does not make compaction cheap, only unscheduled by a client. The
    /// merge still takes the partition's write lock for its duration, so a
    /// large partition still stalls writes while it runs -- it just no longer
    /// happens inside a request. Bounding the work per merge is issue \#143 and
    /// is what actually shortens the stall.
    pub async fn compact_if_needed(&self) -> Result<()> {
        let mut state = self.state.write().await;
        if self.is_maintenance_frozen() {
            return Ok(());
        }
        let full_due = state.full_flushes_since_compaction >= COMPACT_TRIGGER_FULL_FLUSHES;
        let timer_due = state.timer_flushes_since_compaction >= COMPACT_TRIGGER_TIMER_FLUSHES;
        let sweep_pending = !state.compaction_sweep_pending.is_empty();
        if !full_due && !timer_due && !sweep_pending {
            return Ok(());
        }
        let sweep = timer_due || sweep_pending;
        let more = self.compact_locked(&mut state, sweep).await?;
        if more {
            if timer_due {
                state.timer_flushes_since_compaction = state
                    .timer_flushes_since_compaction
                    .max(COMPACT_TRIGGER_TIMER_FLUSHES);
            } else if !sweep {
                state.full_flushes_since_compaction = state
                    .full_flushes_since_compaction
                    .max(COMPACT_TRIGGER_FULL_FLUSHES);
            }
        }
        Ok(())
    }

    /// Runs one bounded unit. A sweep examines every segment in turn for
    /// expiry; an ordinary pass reads only segments the index proves contain
    /// records shadowed by retained segments.
    async fn compact_locked(&self, state: &mut State, sweep: bool) -> Result<bool> {
        if state.segments.is_empty() {
            state.full_flushes_since_compaction = 0;
            state.timer_flushes_since_compaction = 0;
            state.compaction_sweep_pending.clear();
            state.compaction_sweep_timer_debt = 0;
            return Ok(false);
        }
        let now = self.now_millis();

        if sweep && state.compaction_sweep_pending.is_empty() {
            state.compaction_sweep_timer_debt = state.timer_flushes_since_compaction;
            state.compaction_sweep_pending = state
                .segments
                .iter()
                .map(|entry| (entry.source, entry.name.clone()))
                .collect();
        }

        let mut winner_counts = vec![0u64; state.segments.len()];
        for loc in state.index.values() {
            winner_counts[loc.segment] += 1;
        }
        let reclaimable: Vec<usize> = state
            .segments
            .iter()
            .enumerate()
            .filter_map(|(position, entry)| {
                (entry.source.is_some() || winner_counts[position] < entry.record_count)
                    .then_some(position)
            })
            .collect();
        let candidates: Vec<usize> = if sweep {
            state
                .segments
                .iter()
                .enumerate()
                .filter_map(|(position, entry)| {
                    state
                        .compaction_sweep_pending
                        .iter()
                        .any(|(source, name)| *source == entry.source && name == &entry.name)
                        .then_some(position)
                })
                .collect()
        } else {
            reclaimable
        };
        let candidate_count = candidates.len();

        // The first object is allowed to exceed the target because historical
        // and shared segments can already be oversized. Every later admission
        // respects it, so a pass never combines that object with more input.
        let mut selected = Vec::new();
        let mut input_bytes = 0u64;
        for position in candidates {
            let bytes = state.segments[position].bytes;
            if !selected.is_empty() && input_bytes + bytes > self.compaction_input_bytes() {
                break;
            }
            input_bytes += bytes;
            selected.push(position);
        }
        let more = selected.len() < candidate_count;
        if selected.is_empty() {
            state.full_flushes_since_compaction = 0;
            if sweep {
                state.timer_flushes_since_compaction = 0;
                state.compaction_sweep_pending.clear();
                state.compaction_sweep_timer_debt = 0;
            }
            return Ok(false);
        }

        let mut selected_positions = vec![false; state.segments.len()];
        for &position in &selected {
            selected_positions[position] = true;
        }
        let retained: Vec<SegmentEntry> = state
            .segments
            .iter()
            .enumerate()
            .filter(|(position, _)| !selected_positions[*position])
            .map(|(_, entry)| entry.clone())
            .collect();

        let mut inputs = Vec::with_capacity(selected.len());
        let mut external_sources: BTreeMap<(Bytes, Lamport), PartitionId> = BTreeMap::new();
        let mut relocated_values: BTreeMap<(PartitionId, String), ExternalValue> = BTreeMap::new();
        for &position in &selected {
            let entry = &state.segments[position];
            // A shared segment is read from the source partition's directory.
            // Compaction merges it into a new self-written segment, which is
            // how a child eventually stops sharing the parent's objects.
            let (bytes, _) = self
                .store
                .get(&self.path.resolve_segment(entry))
                .await
                .map_err(store_error)?;
            let segment = Segment::decode(&bytes).map_err(format_error)?;
            let records = segment.records().to_vec();
            if let Some(source) = entry.source {
                for record in &records {
                    if matches!(record.value, RecordValue::External(_)) {
                        external_sources.insert((record.key.clone(), record.lamport), source);
                    }
                }
            }
            inputs.push(records);
        }
        // Keeps anything whose removal could uncover an older record in a
        // segment this pass is leaving behind, and otherwise keeps every
        // unexpired tombstone so a retrying deleter still gets an answer. With
        // nothing retained it is the whole-partition rule.
        let mut merged = compact::merge_bounded(&inputs, now, &retained).map_err(format_error)?;
        // Let the format merge inspect every selected record first, including
        // duplicate losing Lamports that indicate corruption. Only then use
        // the exact index to discard a selected winner superseded by a segment
        // this pass retains.
        merged.retain(|record| {
            state
                .index
                .get(&record.key)
                .is_some_and(|loc| selected_positions[loc.segment])
        });
        // A shared segment physically holds keys on both sides of a split
        // boundary, so a child compacting one must keep only the keys it owns;
        // writing the rest would put keys outside its range into its own
        // segment, which the manifest would rightly refuse. For a partition
        // that shares nothing this is a no-op, since its records are all in
        // range already. See ADR 0009.
        merged.retain(|record| self.range.contains(&record.key));
        // Relocate only shared external values that survived validation,
        // winner selection, expiry, and range filtering. A shared parent may
        // hold large values for both children, and copying irrelevant values
        // would make a segment-byte-bounded pass perform unbounded blob I/O.
        for record in &mut merged {
            let RecordValue::External(external) = &record.value else {
                continue;
            };
            let Some(source) = external_sources
                .get(&(record.key.clone(), record.lamport))
                .copied()
            else {
                continue;
            };
            let source_key = (source, external.name.clone());
            let relocated = match relocated_values.get(&source_key) {
                Some(relocated) => relocated.clone(),
                None => {
                    let value_key = self.path.for_partition(source).object(&external.name);
                    let (value, _) = self.store.get(&value_key).await.map_err(store_error)?;
                    if value.len() as u64 != external.length
                        || crc32c::crc32c(&value) != external.crc32c
                    {
                        return Err(Error::Internal(format!(
                            "external value {} does not match its record",
                            external.name
                        )));
                    }
                    let relocated = self.writer.put_value(value).await.map_err(format_error)?;
                    relocated_values.insert(source_key, relocated.clone());
                    relocated
                }
            };
            record.value = RecordValue::External(relocated);
        }

        // Only self-written segments are ours to delete after the swap. A
        // shared reference's object lives under another partition's directory
        // and may still be referenced by a sibling child, so compaction drops
        // the reference from this manifest but never deletes the object; the
        // cross-partition-aware orphan sweep reclaims it once no live manifest
        // names it. See ADR 0009.
        let selected_identities: Vec<(Option<PartitionId>, String)> = selected
            .iter()
            .map(|&position| {
                let entry = &state.segments[position];
                (entry.source, entry.name.clone())
            })
            .collect();
        let replaced: Vec<String> = selected
            .iter()
            .map(|&position| &state.segments[position])
            .filter(|e| e.source.is_none())
            .map(|e| e.name.clone())
            .collect();
        let output_position = retained.len();
        let (segments, index) = if merged.is_empty() {
            (Vec::new(), BTreeMap::new())
        } else {
            let mut builder = SegmentBuilder::new(
                self.path.keyspace_id(),
                self.path.partition_id(),
                self.writer.epoch(),
            );
            for record in &merged {
                builder.push(record).map_err(format_error)?;
            }
            let built = builder.finish().map_err(format_error)?;
            let published = self
                .writer
                .put_segment(&built)
                .await
                .map_err(format_error)?;

            let mut index = BTreeMap::new();
            for entry in built.index.entries() {
                index.insert(
                    entry.key.clone(),
                    Loc {
                        segment: output_position,
                        offset: entry.offset,
                        record_length: entry.record_length,
                    },
                );
            }
            (vec![published], index)
        };

        // A compacted output is new work, not the oldest input again. Putting
        // it after retained entries prevents the next bounded pass from
        // feeding the previous output straight back into itself.
        let mut plan_segments = retained;
        plan_segments.extend(segments);

        // Rebuilt from the old index rather than from the segments, because
        // reading the retained ones back is the cost this change exists to
        // avoid. A key whose winner was in the merged set is relocated to the
        // new segment, or dropped if the merge reclaimed it; every other key
        // keeps its record and only moves position.
        let mut remapped: BTreeMap<Bytes, Loc> = BTreeMap::new();
        let mut retained_positions = vec![None; state.segments.len()];
        let mut next_position = 0usize;
        for (position, was_selected) in selected_positions.iter().enumerate() {
            if !was_selected {
                retained_positions[position] = Some(next_position);
                next_position += 1;
            }
        }
        for (key, loc) in &state.index {
            if selected_positions[loc.segment] {
                if let Some(found) = index.get(key) {
                    remapped.insert(key.clone(), *found);
                }
            } else {
                remapped.insert(
                    key.clone(),
                    Loc {
                        segment: retained_positions[loc.segment]
                            .expect("an unselected segment has a retained position"),
                        offset: loc.offset,
                        record_length: loc.record_length,
                    },
                );
            }
        }
        let index = remapped;

        // Compaction republishes the same logical state, so the horizon must
        // not move: the log above it still has to replay after a crash.
        let flushed = state.flushed;
        let range = self.range.clone();
        let committed_segments = plan_segments.clone();
        let manifest = self
            .writer
            .commit(|_| CommitPlan {
                committed_lamport: flushed,
                range: range.clone(),
                segments: committed_segments.clone(),
            })
            .await
            .map_err(format_error)?;
        state.flushed_epoch = state.flushed_epoch.max(manifest.epoch);
        state.segments = manifest.segments;
        state.index = index;
        state.index_bytes = index_cost(&state.index);
        if !sweep {
            state.full_flushes_since_compaction = 0;
        }
        if sweep {
            state
                .compaction_sweep_pending
                .retain(|identity| !selected_identities.contains(identity));
            if state.compaction_sweep_pending.is_empty() {
                state.timer_flushes_since_compaction = state
                    .timer_flushes_since_compaction
                    .saturating_sub(state.compaction_sweep_timer_debt);
                state.compaction_sweep_timer_debt = 0;
            }
        }

        // These deletes are why compaction is frozen during a split. The
        // replaced objects are unreferenced by *this* manifest the moment it
        // swapped, but since ADR 0009 a split child can reference this
        // partition's self-written segments in place, so "unreferenced here"
        // is no longer "unreferenced anywhere". Deleting one a child points at
        // would dangle that reference and lose acknowledged data. `compact` is
        // a no-op while [`Partition::maintenance_frozen`] is set, and the
        // freeze is established before any child manifest is published, so this
        // loop only ever runs when no child references what it deletes. Outside
        // a split this is still the only writer, so the deletes are safe.
        // Failures here strand objects for the cross-partition orphan sweep to
        // reclaim, rather than failing a compaction that already happened.
        for name in replaced {
            let _ = self.store.delete(&self.path.object(&name)).await;
        }
        Ok(more)
    }

    fn compaction_input_bytes(&self) -> u64 {
        self.compaction_input_bytes
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Shrinks the compaction unit so a test can reach the bounded path
    /// without writing tens of megabytes.
    #[cfg(test)]
    pub(crate) fn set_compaction_input_bytes(&self, bytes: u64) {
        self.compaction_input_bytes
            .store(bytes, std::sync::atomic::Ordering::Relaxed);
    }

    /// Where the merged scan should start, or `None` if this page is
    /// certainly empty.
    fn scan_start(&self, prefix: &[u8], cursor: Option<&Cursor>) -> Option<Vec<u8>> {
        let mut start = prefix.to_vec().max(self.range.start().to_vec());
        if let Some(cursor) = cursor {
            // Strictly after the last key returned, which is what makes a
            // multi-page scan visit each key exactly once.
            let mut after = cursor.after_key.to_vec();
            after.push(0);
            start = start.max(after);
        }
        if self.range.end().is_some_and(|end| start.as_slice() >= end) {
            return None;
        }
        Some(start)
    }

    #[cfg(test)]
    pub(crate) async fn stored_entry(&self, key: &[u8]) -> Result<Option<Stored>> {
        let state = self.state.read().await;
        self.load(&state, key).await
    }

    #[cfg(test)]
    pub(crate) async fn segment_count(&self) -> usize {
        self.state.read().await.segments.len()
    }

    #[cfg(test)]
    pub(crate) async fn segment_entries(&self) -> Vec<SegmentEntry> {
        self.state.read().await.segments.clone()
    }

    #[cfg(test)]
    pub(crate) async fn arm_full_compaction(&self) {
        self.state.write().await.full_flushes_since_compaction = COMPACT_TRIGGER_FULL_FLUSHES;
    }

    #[cfg(test)]
    pub(crate) async fn arm_timer_compaction(&self) {
        self.state.write().await.timer_flushes_since_compaction = COMPACT_TRIGGER_TIMER_FLUSHES;
    }

    #[cfg(test)]
    pub(crate) async fn timer_compaction_is_due(&self) -> bool {
        self.state.read().await.timer_flushes_since_compaction >= COMPACT_TRIGGER_TIMER_FLUSHES
    }

    #[cfg(test)]
    pub(crate) async fn timer_flushes_since_compaction(&self) -> usize {
        self.state.read().await.timer_flushes_since_compaction
    }

    #[cfg(test)]
    pub(crate) async fn memtable_bytes(&self) -> u64 {
        self.state.read().await.memtable_bytes
    }
}

/// Rejects a Lamport that would break the sequence versions are drawn from.
///
/// Versions are only unique and monotonic if the Lamports are, and once a
/// duplicate is in a segment nothing downstream can tell it happened. Callers
/// must assign the Lamport under the same serialization that submits the
/// write, which is what the owner's single write path does.
fn check_lamport(state: &State, lamport: Lamport) -> Result<()> {
    if lamport <= state.committed {
        return Err(Error::InvalidArgument(format!(
            "lamport {lamport} is not ahead of the committed lamport {}",
            state.committed
        )));
    }
    Ok(())
}

/// What one index entry is estimated to hold in memory.
fn index_entry_cost(key: &Bytes) -> u64 {
    key.len() as u64 + INDEX_ENTRY_OVERHEAD_BYTES
}

/// The whole index's estimated cost, for the paths that replace it wholesale
/// and are already walking every entry.
fn index_cost(index: &BTreeMap<Bytes, Loc>) -> u64 {
    index.keys().map(index_entry_cost).sum()
}

/// Adopts a published manifest as this partition's flushed state.
///
/// One function rather than three copies because opening a partition,
/// reclaiming a replica's memtable, and hydrating from a bucket are the same
/// operation seen from different distances, and the invariant they share is
/// easy to break independently: the index, the segment list, and the horizon
/// have to move together, or a location will name a segment the partition no
/// longer lists.
///
/// The committed Lamport only ever rises. A partition holding acknowledged
/// writes above the horizon must not forget them by adopting an older
/// manifest's idea of where the sequence is, because the next write would
/// reuse a version that has already been handed out.
fn adopt<S: ObjectStore + ?Sized>(state: &mut State, snapshot: &Snapshot<S>) {
    let horizon = snapshot.committed_lamport();
    // An epoch only ever rises, because a manifest is published by a
    // compare-and-swap the control plane fenced. Taking the maximum rather
    // than the manifest's value outright means a partition that has seen
    // evidence of a newer owner cannot be talked back into forgetting it.
    state.flushed_epoch = state.flushed_epoch.max(snapshot.manifest().epoch);
    state.index = snapshot
        .locations()
        .map(|(key, at)| {
            (
                key.clone(),
                Loc {
                    segment: at.segment,
                    offset: at.offset,
                    record_length: at.record_length,
                },
            )
        })
        .collect();
    // What the index costs in memory is a property of the index, so it is
    // recomputed wherever the index is replaced rather than left for each
    // caller to remember. ADR 0006 makes this the resource a worker runs out
    // of first, and a hydrated partition reporting the cost of the index it
    // just discarded would be the stalest possible answer about it.
    state.index_bytes = index_cost(&state.index);
    state.segments = snapshot.manifest().segments.clone();
    state.compaction_sweep_pending.clear();
    state.compaction_sweep_timer_debt = 0;
    state.flushed = horizon;
    if state.committed < horizon {
        state.committed = horizon;
    }
    // Everything at or below the horizon is in a segment now, so holding it in
    // memory a second time buys nothing.
    state
        .memtable
        .retain(|_, entry| Lamport(entry.version.get()) > horizon);
    state.memtable_bytes = state
        .memtable
        .iter()
        .map(|(key, entry)| entry_cost(key, entry))
        .sum();
    state.reclaim_attempted_at_bytes = state.memtable_bytes;
}

/// What one entry charges against the flush trigger.
fn entry_cost(key: &Bytes, entry: &Stored) -> u64 {
    record_footprint(key.len(), entry.value.len())
}

/// The bytes a single stored record adds to [`Partition::size_bytes`]: its key,
/// its value, and the fixed per-entry bookkeeping the size figure charges on
/// top of them.
///
/// Exposed so a caller reserving space before a write — admission checking a
/// storage cap — charges the same footprint storage will actually add, rather
/// than the value alone. Counting only the value would let a zero-byte cap
/// admit a keyed write whose real cost is the key plus this framing, so the two
/// must not drift; keeping the estimate here is what keeps them in step.
#[must_use]
pub fn record_footprint(key_len: usize, value_len: usize) -> u64 {
    key_len as u64 + value_len as u64 + ENTRY_OVERHEAD_BYTES
}

fn segment_record_of(key: &Bytes, entry: &Stored) -> SegmentRecord {
    SegmentRecord {
        key: key.clone(),
        lamport: Lamport(entry.version.get()),
        expires_at_millis: entry.expires_at_millis,
        value: if entry.deleted {
            RecordValue::Tombstone
        } else {
            RecordValue::Inline(entry.value.clone())
        },
    }
}

fn absolute_expiry(now_millis: u64, ttl: Duration) -> u64 {
    now_millis.saturating_add(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX))
}

fn format_error(e: FormatError) -> Error {
    Error::Internal(format!("partition format: {e}"))
}

fn store_error(e: ObjectError) -> Error {
    Error::Internal(format!("object store: {e}"))
}

/// Checks a write condition, returning the failure to report if it does not
/// hold.
///
/// An expired or deleted key counts as absent everywhere, which is what makes
/// `IfNotPresent` usable as a lock acquisition with a TTL.
fn evaluate(
    condition: WriteCondition,
    existing: Option<&Stored>,
    now_millis: u64,
) -> Option<WriteOutcome> {
    let visible = existing.and_then(|e| e.visible_at(now_millis));
    let found = visible.as_ref().map(|r| r.version);
    let holds = match condition {
        WriteCondition::None => true,
        WriteCondition::IfNotPresent => visible.is_none(),
        WriteCondition::IfVersion(expected) => found == Some(expected),
    };
    if holds {
        None
    } else {
        Some(WriteOutcome::ConditionFailed { condition, found })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{
        open_child, owner, owner_in_range, owner_with_clock, owner_with_clocked_store,
        partition_pair_at_epochs, partition_pair_with_clock, partition_with_clock, reopened,
    };
    use orbita_core::KeyspaceId;
    use orbita_format::PartitionPath;

    fn bytes(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    /// The 26 single-letter keys, so a split at "m" leaves data on both sides.
    fn alphabet() -> Vec<String> {
        (b'a'..=b'z').map(|c| (c as char).to_string()).collect()
    }

    async fn write_alphabet(owner: &crate::testing::Owner) {
        for k in alphabet() {
            owner
                .put(
                    k.as_bytes(),
                    bytes(&format!("v-{k}")),
                    None,
                    WriteCondition::None,
                )
                .await
                .expect("write a letter");
        }
    }

    /// Writes `keys`, then flushes, producing one segment.
    async fn write_and_flush(owner: &crate::testing::Owner, keys: &[String]) {
        for k in keys {
            owner
                .put(
                    k.as_bytes(),
                    bytes(&format!("v-{k}")),
                    None,
                    WriteCondition::None,
                )
                .await
                .expect("write");
        }
        owner.flush().await.expect("flush");
    }

    #[tokio::test]
    async fn repeated_full_flush_checks_do_not_rewrite_fully_live_segments() {
        let owner = owner().await;
        for cycle in 0..3 {
            for segment in 0..COMPACTION_INPUT_FLUSHES {
                write_and_flush(&owner, &[format!("cycle-{cycle}-segment-{segment}")]).await;
            }
            let before = owner.segment_entries().await;
            owner.arm_full_compaction().await;

            owner.compact_if_needed().await.expect("compact check");

            assert_eq!(
                owner.segment_entries().await,
                before,
                "compaction exists to reclaim stale records, not combine fully live segments"
            );
        }
    }

    #[tokio::test]
    async fn one_bounded_pass_does_not_clear_unprocessed_compaction_debt() {
        let owner = owner().await;
        for version in 0..=COMPACTION_INPUT_FLUSHES {
            owner
                .put(
                    b"key",
                    bytes(&format!("v{version}")),
                    None,
                    WriteCondition::None,
                )
                .await
                .expect("overwrite");
            owner.flush().await.expect("flush");
        }
        owner.set_compaction_input_bytes(1);
        owner.arm_full_compaction().await;

        for expected in (1..=COMPACTION_INPUT_FLUSHES as usize).rev() {
            owner.compact_if_needed().await.expect("bounded compaction");
            assert_eq!(owner.segment_count().await, expected);
        }

        assert_eq!(
            owner
                .get(b"key")
                .await
                .expect("read")
                .map(|record| record.value),
            Some(bytes(&format!("v{COMPACTION_INPUT_FLUSHES}")))
        );
    }

    #[tokio::test]
    async fn a_bounded_timer_sweep_does_not_chase_its_own_outputs() {
        let owner = owner().await;
        for segment in 0..6 {
            write_and_flush(&owner, &[format!("unique-{segment}")]).await;
        }
        let entries = owner.segment_entries().await;
        owner.set_compaction_input_bytes(entries[0].bytes + entries[1].bytes);
        owner.arm_timer_compaction().await;

        owner.compact_if_needed().await.expect("first timer pass");
        write_and_flush(&owner, &["arrived-mid-sweep".to_owned()]).await;
        let late_segment = owner
            .segment_entries()
            .await
            .last()
            .expect("late segment")
            .name
            .clone();

        let mut passes = 1;
        while owner.timer_compaction_is_due().await {
            owner.compact_if_needed().await.expect("timer compaction");
            passes += 1;
            assert!(passes <= 3, "the sweep started consuming its own outputs");
        }

        assert_eq!(passes, 3, "six inputs should drain as three bounded pairs");
        let after = owner.segment_entries().await;
        assert!(after.iter().any(|entry| entry.name == late_segment));
        assert_eq!(
            owner.timer_flushes_since_compaction().await,
            1,
            "the flush arriving mid-sweep must remain as future timer debt"
        );
        owner
            .compact_if_needed()
            .await
            .expect("idle compaction check");
        assert_eq!(owner.segment_entries().await, after);
    }

    #[tokio::test]
    async fn a_bounded_compaction_loses_nothing_it_did_not_merge() {
        // The risk in merging part of a partition is the bookkeeping: the
        // records that were not merged keep their segments, and those segments
        // move position. An index that is not remapped points at the wrong
        // object and reads the wrong bytes, or nothing.
        let owner = owner().await;
        let groups: Vec<Vec<String>> = alphabet().chunks(5).map(<[String]>::to_vec).collect();
        for group in &groups {
            write_and_flush(&owner, group).await;
        }

        owner.set_compaction_input_bytes(1);
        // Repeatedly, because each pass moves every retained segment again and
        // a remap that is wrong by one only shows up once it has been applied
        // more than once.
        for _ in 0..4 {
            owner.compact().await.expect("compact");
        }

        for k in alphabet() {
            let got = owner.get(k.as_bytes()).await.expect("read");
            assert_eq!(
                got.map(|r| r.value),
                Some(bytes(&format!("v-{k}"))),
                "{k} did not survive a bounded compaction"
            );
        }
    }

    #[tokio::test]
    async fn a_bounded_compaction_keeps_the_newest_record_for_an_overwritten_key() {
        // An overwrite leaves the old record in an older segment and the new
        // one in a newer segment. A pass that merges the old segment must not
        // let the value it resurrects win, and the index must still point at
        // the newer record afterwards.
        let owner = owner().await;
        write_and_flush(&owner, &["k".to_owned()]).await;
        for round in 1..=4u32 {
            owner
                .put(
                    b"k",
                    bytes(&format!("v{round}")),
                    None,
                    WriteCondition::None,
                )
                .await
                .expect("overwrite");
            owner.flush().await.expect("flush");
        }

        owner.set_compaction_input_bytes(1);
        owner.compact().await.expect("compact");

        let got = owner.get(b"k").await.expect("read");
        assert_eq!(
            got.map(|r| r.value),
            Some(bytes("v4")),
            "the newest write must survive a pass that merged the oldest segments"
        );
    }

    fn child_prefix(id: u64) -> String {
        PartitionPath::new("", KeyspaceId(1), PartitionId(id))
            .prefix()
            .to_string()
    }

    #[tokio::test]
    async fn a_prepared_child_serves_the_parents_keys_in_its_range_without_copying_a_segment() {
        // The proof the split review demanded: every pre-split key is readable
        // from exactly one child afterwards, and no segment byte was copied to
        // make that true. A child that had been created empty (the data-loss
        // bug) would fail every read here.
        let (parent, store, clock) = owner_with_clocked_store().await;
        clock.set_millis(1);
        write_alphabet(&parent).await;

        let (low, high) = parent
            .range()
            .clone()
            .split_at(bytes("m"))
            .expect("the boundary is inside the range");
        let horizon = parent
            .prepare_child_partitions(&[
                ChildSpec {
                    id: PartitionId(2),
                    epoch: Epoch(2),
                    range: low.clone(),
                },
                ChildSpec {
                    id: PartitionId(3),
                    epoch: Epoch(2),
                    range: high.clone(),
                },
            ])
            .await
            .expect("preparing both children");
        assert_eq!(
            horizon,
            parent.committed_lamport().await.unwrap(),
            "children begin at the parent's committed position"
        );

        // No copy: a child's directory holds a manifest and nothing else, and
        // every segment object still lives under the parent's prefix.
        let keys = store.keys();
        for id in [2, 3] {
            let prefix = child_prefix(id);
            assert!(
                !keys
                    .iter()
                    .any(|k| k.starts_with(&prefix) && k.ends_with(".oseg")),
                "child {id} must reference the parent's segments, not copies of them"
            );
            assert!(
                keys.iter().any(|k| *k == format!("{prefix}manifest.json")),
                "child {id} must have a published manifest"
            );
        }
        assert!(
            keys.iter()
                .any(|k| k.starts_with(&child_prefix(1)) && k.ends_with(".oseg")),
            "the parent's segment objects are the ones being shared"
        );

        // Every key is served by exactly one child: the owning child returns
        // it, the other rejects it as out of range.
        let child2 = open_child(&store, &clock, PartitionId(2), Epoch(2), low).await;
        let child3 = open_child(&store, &clock, PartitionId(3), Epoch(2), high).await;
        for k in alphabet() {
            let (owner, other) = if k.as_str() < "m" {
                (&child2, &child3)
            } else {
                (&child3, &child2)
            };
            let record = owner
                .get(k.as_bytes())
                .await
                .expect("the owning child answers")
                .unwrap_or_else(|| panic!("key {k} lost across the split"));
            assert_eq!(record.value, bytes(&format!("v-{k}")));
            assert!(
                other.get(k.as_bytes()).await.is_err(),
                "the non-owning child rejects {k} as outside its range"
            );
        }
    }

    #[tokio::test]
    async fn a_prepared_child_preserves_each_keys_version() {
        // ADR 0002: a key's version is its Lamport, and a split must not move
        // it. The value read from the child carries the same version the parent
        // wrote it at.
        let (parent, store, clock) = owner_with_clocked_store().await;
        clock.set_millis(1);
        write_alphabet(&parent).await;
        let want: Vec<(String, Version)> = {
            let mut out = Vec::new();
            for k in alphabet() {
                let v = parent.get(k.as_bytes()).await.unwrap().unwrap().version;
                out.push((k, v));
            }
            out
        };

        let (low, high) = parent.range().clone().split_at(bytes("m")).unwrap();
        parent
            .prepare_child_partitions(&[
                ChildSpec {
                    id: PartitionId(2),
                    epoch: Epoch(2),
                    range: low.clone(),
                },
                ChildSpec {
                    id: PartitionId(3),
                    epoch: Epoch(2),
                    range: high.clone(),
                },
            ])
            .await
            .unwrap();
        let child2 = open_child(&store, &clock, PartitionId(2), Epoch(2), low).await;
        let child3 = open_child(&store, &clock, PartitionId(3), Epoch(2), high).await;

        for (k, version) in want {
            let child = if k.as_str() < "m" { &child2 } else { &child3 };
            assert_eq!(
                child.get(k.as_bytes()).await.unwrap().unwrap().version,
                version,
                "key {k} kept its version across the split"
            );
        }
    }

    #[tokio::test]
    async fn a_merge_preserves_equal_source_versions_and_allocates_above_both_horizons() {
        // ADR 0002 deliberately permits two keys inherited from independent
        // parents to carry the same version. Merge must preserve both tokens,
        // then continue strictly above the greatest source horizon.
        let (parent, store, clock) = owner_with_clocked_store().await;
        clock.set_millis(1);
        parent
            .put(b"a", bytes("parent-low"), None, WriteCondition::None)
            .await
            .unwrap();
        parent
            .put(b"z", bytes("parent-high"), None, WriteCondition::None)
            .await
            .unwrap();
        let (low_range, high_range) = parent.range().clone().split_at(bytes("m")).unwrap();
        parent
            .prepare_child_partitions(&[
                ChildSpec {
                    id: PartitionId(2),
                    epoch: Epoch(2),
                    range: low_range.clone(),
                },
                ChildSpec {
                    id: PartitionId(3),
                    epoch: Epoch(2),
                    range: high_range.clone(),
                },
            ])
            .await
            .unwrap();
        let low = open_child(&store, &clock, PartitionId(2), Epoch(2), low_range).await;
        let high = open_child(&store, &clock, PartitionId(3), Epoch(2), high_range).await;

        low.put(
            Lamport(3),
            b"b",
            bytes("equal-low"),
            None,
            WriteCondition::None,
        )
        .await
        .unwrap();
        high.put(
            Lamport(3),
            b"y",
            bytes("equal-high"),
            None,
            WriteCondition::None,
        )
        .await
        .unwrap();
        low.flush().await.unwrap();
        high.flush().await.unwrap();

        let merged = ChildSpec {
            id: PartitionId(4),
            epoch: Epoch(3),
            range: KeyRange::unbounded(),
        };
        let horizon = low
            .prepare_merged_partition(&high, &merged)
            .await
            .expect("prepare a shared-segment merge");
        assert_eq!(horizon, Lamport(3));

        let merged = open_child(
            &store,
            &clock,
            PartitionId(4),
            Epoch(3),
            KeyRange::unbounded(),
        )
        .await;
        assert_eq!(merged.get(b"b").await.unwrap().unwrap().version, Version(3));
        assert_eq!(merged.get(b"y").await.unwrap().unwrap().version, Version(3));
        assert_eq!(merged.get(b"a").await.unwrap().unwrap().version, Version(1));
        assert_eq!(merged.get(b"z").await.unwrap().unwrap().version, Version(2));
        let shared = merged.shared_segment_sources().await;
        assert!(shared.contains_key(&PartitionId(1)));
        assert!(shared.contains_key(&PartitionId(2)));
        assert!(shared.contains_key(&PartitionId(3)));
        assert!(
            segment_objects(&store, 4).is_empty(),
            "merge preparation copies no source segment bytes"
        );
        clock.set_millis(10_000);
        parent
            .sweep_orphans(&shared[&PartitionId(1)], 1_000, 0, false)
            .await
            .unwrap();
        low.sweep_orphans(&shared[&PartitionId(2)], 1_000, 0, false)
            .await
            .unwrap();
        high.sweep_orphans(&shared[&PartitionId(3)], 1_000, 0, false)
            .await
            .unwrap();
        assert_eq!(
            merged.get(b"b").await.unwrap().unwrap().value,
            bytes("equal-low")
        );
        assert_eq!(
            merged.get(b"y").await.unwrap().unwrap().value,
            bytes("equal-high")
        );

        merged.compact().await.unwrap();
        assert!(
            merged.shared_segment_sources().await.is_empty(),
            "merged compaction materializes both source sets under the child"
        );
        assert!(!segment_objects(&store, 4).is_empty());
        merged
            .put(
                Lamport(4),
                b"n",
                bytes("first-after-merge"),
                None,
                WriteCondition::None,
            )
            .await
            .expect("the first merged write is above both source horizons");
        assert_eq!(merged.get(b"n").await.unwrap().unwrap().version, Version(4));
    }

    #[tokio::test]
    async fn merged_compaction_relocates_external_values_from_both_parents() {
        let (_unused, store, clock) = owner_with_clocked_store().await;
        clock.set_millis(1);
        let (low_range, high_range) = KeyRange::unbounded().split_at(bytes("m")).unwrap();
        let low = open_child(&store, &clock, PartitionId(2), Epoch(2), low_range.clone()).await;
        let high = open_child(&store, &clock, PartitionId(3), Epoch(2), high_range.clone()).await;
        let low_value = Bytes::from(vec![0x2a; 4096]);
        let high_value = Bytes::from(vec![0x3b; 4096]);
        let mut source_value_keys = Vec::new();
        for (partition, id, key, value, range) in [
            (
                &low,
                PartitionId(2),
                bytes("a"),
                low_value.clone(),
                low_range,
            ),
            (
                &high,
                PartitionId(3),
                bytes("z"),
                high_value.clone(),
                high_range,
            ),
        ] {
            let external = partition.writer.put_value(value).await.unwrap();
            source_value_keys.push(partition.path.object(&external.name));
            let mut builder = SegmentBuilder::new(KeyspaceId(1), id, Epoch(2));
            builder
                .push(&SegmentRecord {
                    key,
                    lamport: Lamport(1),
                    expires_at_millis: None,
                    value: RecordValue::External(external),
                })
                .unwrap();
            let segment = partition
                .writer
                .put_segment(&builder.finish().unwrap())
                .await
                .unwrap();
            partition
                .writer
                .commit(|_| CommitPlan {
                    committed_lamport: Lamport(1),
                    range: range.clone(),
                    segments: vec![segment.clone()],
                })
                .await
                .unwrap();
            partition.hydrate().await.unwrap();
        }

        low.prepare_merged_partition(
            &high,
            &ChildSpec {
                id: PartitionId(4),
                epoch: Epoch(3),
                range: KeyRange::unbounded(),
            },
        )
        .await
        .unwrap();
        let merged = open_child(
            &store,
            &clock,
            PartitionId(4),
            Epoch(3),
            KeyRange::unbounded(),
        )
        .await;
        assert_eq!(merged.get(b"a").await.unwrap().unwrap().value, low_value);
        assert_eq!(merged.get(b"z").await.unwrap().unwrap().value, high_value);

        let shared = merged.shared_segment_sources().await;
        clock.set_millis(10_000);
        low.sweep_orphans(&shared[&PartitionId(2)], 1_000, 0, false)
            .await
            .unwrap();
        high.sweep_orphans(&shared[&PartitionId(3)], 1_000, 0, false)
            .await
            .unwrap();
        assert!(source_value_keys
            .iter()
            .all(|key| store.keys().contains(key)));

        merged.compact().await.unwrap();
        let child_values: Vec<_> = store
            .keys()
            .into_iter()
            .filter(|key| key.starts_with(&child_prefix(4)) && key.ends_with(".oval"))
            .collect();
        assert_eq!(child_values.len(), 2);
        assert!(merged.shared_segment_sources().await.is_empty());
        assert_eq!(merged.get(b"a").await.unwrap().unwrap().value, low_value);
        assert_eq!(merged.get(b"z").await.unwrap().unwrap().value, high_value);

        for (partition, range) in [(&low, low.range().clone()), (&high, high.range().clone())] {
            partition
                .writer
                .commit(|_| CommitPlan {
                    committed_lamport: Lamport(1),
                    range: range.clone(),
                    segments: Vec::new(),
                })
                .await
                .unwrap();
        }
        clock.set_millis(12_000);
        for partition in [&low, &high] {
            partition
                .sweep_orphans(&BTreeSet::new(), 1_000, 0, false)
                .await
                .unwrap();
        }
        assert!(
            source_value_keys
                .iter()
                .all(|key| !store.keys().contains(key)),
            "source values collect only after merged compaction drops both references"
        );
        assert!(child_values.iter().all(|key| store.keys().contains(key)));
    }

    #[tokio::test]
    async fn without_dual_quiesce_a_racing_parent_write_is_absent_from_the_merge() {
        let (_unused, store, clock) = owner_with_clocked_store().await;
        let (low_range, high_range) = KeyRange::unbounded().split_at(bytes("m")).unwrap();
        let low = open_child(&store, &clock, PartitionId(2), Epoch(2), low_range).await;
        let high = open_child(&store, &clock, PartitionId(3), Epoch(2), high_range).await;
        low.put(
            Lamport(1),
            b"a",
            bytes("before"),
            None,
            WriteCondition::None,
        )
        .await
        .unwrap();
        high.put(
            Lamport(1),
            b"z",
            bytes("before"),
            None,
            WriteCondition::None,
        )
        .await
        .unwrap();
        low.prepare_merged_partition(
            &high,
            &ChildSpec {
                id: PartitionId(4),
                epoch: Epoch(3),
                range: KeyRange::unbounded(),
            },
        )
        .await
        .unwrap();

        // This is the write both admission gates prevent. It lands after M's
        // horizon and has nowhere to go once the parents retire.
        low.put(
            Lamport(2),
            b"b-raced",
            bytes("acknowledged-too-late"),
            None,
            WriteCondition::None,
        )
        .await
        .unwrap();
        let merged = open_child(
            &store,
            &clock,
            PartitionId(4),
            Epoch(3),
            KeyRange::unbounded(),
        )
        .await;
        assert_eq!(
            merged.get(b"b-raced").await.unwrap(),
            None,
            "the negative control must demonstrate why both WALs quiesce before preparation"
        );
    }

    /// The relative names of the segment objects physically under one
    /// partition's directory, for a test that watches compaction delete them.
    fn segment_objects(store: &orbita_format::testing::MemoryStore, id: u64) -> Vec<String> {
        let prefix = child_prefix(id);
        store
            .keys()
            .into_iter()
            .filter(|k| k.starts_with(&prefix) && k.ends_with(".oseg"))
            .collect()
    }

    #[tokio::test]
    async fn a_frozen_split_parent_does_not_compact_away_a_segment_a_child_references() {
        // The ADR 0009 freeze, proven at the layer where the loss happens. A
        // split parent's children reference its self-written segments in place;
        // compaction deletes exactly those segments when it merges them, which
        // would dangle the children's references and lose acknowledged data.
        // Freezing maintenance for the split's duration is what prevents it.
        let (parent, store, clock) = owner_with_clocked_store().await;
        clock.set_millis(1);
        // Two segments, so compaction has something to merge and delete.
        for k in ["a", "c", "e"] {
            parent
                .put(
                    k.as_bytes(),
                    bytes(&format!("v-{k}")),
                    None,
                    WriteCondition::None,
                )
                .await
                .unwrap();
        }
        parent.flush().await.unwrap();
        for k in ["n", "p", "r"] {
            parent
                .put(
                    k.as_bytes(),
                    bytes(&format!("v-{k}")),
                    None,
                    WriteCondition::None,
                )
                .await
                .unwrap();
        }
        parent.flush().await.unwrap();

        let (low, high) = parent.range().clone().split_at(bytes("m")).unwrap();
        parent
            .prepare_child_partitions(&[
                ChildSpec {
                    id: PartitionId(2),
                    epoch: Epoch(2),
                    range: low.clone(),
                },
                ChildSpec {
                    id: PartitionId(3),
                    epoch: Epoch(2),
                    range: high.clone(),
                },
            ])
            .await
            .unwrap();
        let shared = segment_objects(&store, 1);
        assert!(
            shared.len() >= 2,
            "the parent has the segments the children now reference"
        );

        // Frozen: compaction is a no-op, so every shared segment survives.
        parent.freeze_maintenance();
        parent.compact().await.unwrap();
        assert_eq!(
            segment_objects(&store, 1),
            shared,
            "a frozen split parent must not compact away a segment a child references"
        );
        let child_low = open_child(&store, &clock, PartitionId(2), Epoch(2), low).await;
        for k in ["a", "c", "e"] {
            assert_eq!(
                child_low.get(k.as_bytes()).await.unwrap().unwrap().value,
                bytes(&format!("v-{k}")),
                "the child still reads {k} from the shared segment"
            );
        }

        // Lift the freeze — the abort path — and the same compaction now deletes
        // those segments, which is exactly the loss the freeze prevented.
        parent.resume_maintenance();
        parent.compact().await.unwrap();
        let after = segment_objects(&store, 1);
        assert!(
            after.iter().all(|name| !shared.contains(name)),
            "unfrozen, compaction deletes the segments the children referenced: {after:?}"
        );
    }

    #[tokio::test]
    async fn a_child_stops_sharing_once_it_compacts() {
        // Sharing is self-healing: when a child compacts, it merges the shared
        // segments into one it writes under its own directory and drops the
        // cross-partition references, so it can be read with no parent objects
        // in play.
        let (parent, store, clock) = owner_with_clocked_store().await;
        clock.set_millis(1);
        write_alphabet(&parent).await;
        let (low, high) = parent.range().clone().split_at(bytes("m")).unwrap();
        parent
            .prepare_child_partitions(&[
                ChildSpec {
                    id: PartitionId(2),
                    epoch: Epoch(2),
                    range: low.clone(),
                },
                ChildSpec {
                    id: PartitionId(3),
                    epoch: Epoch(2),
                    range: high.clone(),
                },
            ])
            .await
            .unwrap();

        let child2 = open_child(&store, &clock, PartitionId(2), Epoch(2), low.clone()).await;
        child2
            .compact()
            .await
            .expect("the child compacts its shared segments");

        // After compaction the child has its own segment and reads still hold.
        let prefix = child_prefix(2);
        assert!(
            store
                .keys()
                .iter()
                .any(|k| k.starts_with(&prefix) && k.ends_with(".oseg")),
            "compaction wrote a segment under the child's own directory"
        );
        let reopened = open_child(&store, &clock, PartitionId(2), Epoch(2), low).await;
        for k in alphabet().into_iter().filter(|k| k.as_str() < "m") {
            assert_eq!(
                reopened.get(k.as_bytes()).await.unwrap().unwrap().value,
                bytes(&format!("v-{k}")),
                "the compacted child still serves {k}"
            );
        }
    }

    #[tokio::test]
    async fn child_compaction_relocates_an_external_value_out_of_a_shared_parent_segment() {
        let (parent, store, clock) = owner_with_clocked_store().await;
        clock.set_millis(1);
        let value = Bytes::from(vec![0x5a; 4096]);
        let source_external = parent.writer.put_value(value.clone()).await.unwrap();
        let source_value_key = parent.path.object(&source_external.name);
        let mut builder = SegmentBuilder::new(KeyspaceId(1), PartitionId(1), Epoch(1));
        builder
            .push(&SegmentRecord {
                key: bytes("a"),
                lamport: Lamport(1),
                expires_at_millis: None,
                value: RecordValue::External(source_external),
            })
            .unwrap();
        let built = builder.finish().unwrap();
        let segment = parent.writer.put_segment(&built).await.unwrap();
        let range = KeyRange::unbounded();
        parent
            .writer
            .commit(|_| CommitPlan {
                committed_lamport: Lamport(1),
                range: range.clone(),
                segments: vec![segment.clone()],
            })
            .await
            .unwrap();
        parent.hydrate().await.unwrap();

        let (low, high) = parent.range().clone().split_at(bytes("m")).unwrap();
        parent
            .prepare_child_partitions(&[
                ChildSpec {
                    id: PartitionId(2),
                    epoch: Epoch(2),
                    range: low.clone(),
                },
                ChildSpec {
                    id: PartitionId(3),
                    epoch: Epoch(2),
                    range: high,
                },
            ])
            .await
            .unwrap();
        let child = open_child(&store, &clock, PartitionId(2), Epoch(2), low.clone()).await;
        assert_eq!(child.get(b"a").await.unwrap().unwrap().value, value);

        let shared = child
            .shared_segment_sources()
            .await
            .remove(&PartitionId(1))
            .unwrap();
        clock.set_millis(10_000);
        parent
            .sweep_orphans(&shared, 1_000, 0, false)
            .await
            .unwrap();
        assert!(
            store.keys().contains(&source_value_key),
            "the source external value stays live while a child shares its segment"
        );

        child.compact().await.unwrap();
        let reopened = open_child(&store, &clock, PartitionId(2), Epoch(2), low).await;
        assert_eq!(reopened.get(b"a").await.unwrap().unwrap().value, value);
        let child_values: Vec<_> = store
            .keys()
            .into_iter()
            .filter(|key| key.starts_with(&child_prefix(2)) && key.ends_with(".oval"))
            .collect();
        assert_eq!(
            child_values.len(),
            1,
            "compaction materialized one child-owned value"
        );
        reopened
            .sweep_orphans(&BTreeSet::new(), 1_000, 0, false)
            .await
            .unwrap();
        assert!(
            store.keys().contains(&child_values[0]),
            "the child segment keeps its relocated external value live"
        );

        let range = KeyRange::unbounded();
        parent
            .writer
            .commit(|_| CommitPlan {
                committed_lamport: Lamport(1),
                range: range.clone(),
                segments: Vec::new(),
            })
            .await
            .unwrap();
        clock.set_millis(12_000);
        parent
            .sweep_orphans(&BTreeSet::new(), 1_000, 0, false)
            .await
            .unwrap();
        assert!(
            !store.keys().contains(&source_value_key),
            "the source value is collectible after no manifest references its segment"
        );
        assert!(store.keys().contains(&child_values[0]));
    }

    #[tokio::test]
    async fn a_written_value_reads_back_at_the_lamport_it_was_written_at() {
        let p = owner().await;
        let outcome = p
            .put_at(Lamport(47), b"k", bytes("v"), None, WriteCondition::None)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            WriteOutcome::Applied {
                version: Version(47)
            }
        );

        let record = p.get(b"k").await.unwrap().unwrap();
        assert_eq!(record.value, bytes("v"));
        assert_eq!(record.version, Version(47));
    }

    #[tokio::test]
    async fn a_missing_key_reads_as_absent_rather_than_erroring() {
        let p = owner().await;
        assert_eq!(p.get(b"nope").await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_keys_version_is_sparse_because_other_keys_advance_the_sequence() {
        let p = owner().await;
        p.put_at(Lamport(47), b"a", bytes("v"), None, WriteCondition::None)
            .await
            .unwrap();
        for lamport in [100, 105, 111] {
            p.put_at(
                Lamport(lamport),
                b"b",
                bytes("v"),
                None,
                WriteCondition::None,
            )
            .await
            .unwrap();
        }
        p.put_at(Lamport(112), b"a", bytes("v2"), None, WriteCondition::None)
            .await
            .unwrap();

        assert_eq!(p.get(b"a").await.unwrap().unwrap().version, Version(112));
        assert_eq!(
            p.get(b"b").await.unwrap().unwrap().version,
            Version(111),
            "a version names when a key was written, not how often"
        );
    }

    #[tokio::test]
    async fn writing_another_key_leaves_a_held_version_valid() {
        // The worry that motivated per-key counters, and the reason ADR 0002
        // says it does not hold: a compare-and-swap token survives unrelated
        // traffic.
        let p = owner().await;
        p.put_at(Lamport(1), b"lock", bytes("v"), None, WriteCondition::None)
            .await
            .unwrap();
        for lamport in 2..20 {
            p.put_at(
                Lamport(lamport),
                b"noise",
                bytes("v"),
                None,
                WriteCondition::None,
            )
            .await
            .unwrap();
        }

        let outcome = p
            .put_at(
                Lamport(20),
                b"lock",
                bytes("v2"),
                None,
                WriteCondition::IfVersion(Version(1)),
            )
            .await
            .unwrap();
        assert_eq!(
            outcome,
            WriteOutcome::Applied {
                version: Version(20)
            },
            "no unrelated write may invalidate a held version"
        );
    }

    #[tokio::test]
    async fn versions_are_unique_across_every_key_in_the_partition() {
        let p = owner().await;
        for i in 0..50u64 {
            p.put(
                format!("k{}", i % 7).as_bytes(),
                bytes("v"),
                None,
                WriteCondition::None,
            )
            .await
            .unwrap();
        }

        let page = p
            .scan(b"k", None, ScanBudget::of_entries(100))
            .await
            .unwrap();
        let mut versions: Vec<Version> = page.entries.iter().map(|e| e.record.version).collect();
        let count = versions.len();
        versions.sort_unstable();
        versions.dedup();
        assert_eq!(versions.len(), count, "no two live keys share a version");
    }

    #[tokio::test]
    async fn a_lamport_that_does_not_advance_the_sequence_is_rejected() {
        let p = owner().await;
        p.put_at(Lamport(10), b"k", bytes("v"), None, WriteCondition::None)
            .await
            .unwrap();

        for stale in [Lamport(10), Lamport(9), Lamport::ZERO] {
            assert!(
                matches!(
                    p.put_at(stale, b"k", bytes("v2"), None, WriteCondition::None)
                        .await,
                    Err(Error::InvalidArgument(_))
                ),
                "reusing a version would break every comparison downstream"
            );
            assert!(matches!(
                p.delete_at(stale, b"k", WriteCondition::None).await,
                Err(Error::InvalidArgument(_))
            ));
        }
        assert_eq!(p.get(b"k").await.unwrap().unwrap().value, bytes("v"));
    }

    #[tokio::test]
    async fn the_committed_lamport_follows_the_last_write() {
        let p = owner().await;
        assert_eq!(p.committed_lamport().await.unwrap(), Lamport::ZERO);

        p.put_at(Lamport(5), b"k", bytes("v"), None, WriteCondition::None)
            .await
            .unwrap();
        assert_eq!(p.committed_lamport().await.unwrap(), Lamport(5));

        p.delete_at(Lamport(9), b"k", WriteCondition::None)
            .await
            .unwrap();
        assert_eq!(p.committed_lamport().await.unwrap(), Lamport(9));
    }

    #[tokio::test]
    async fn a_failed_condition_does_not_consume_the_lamport() {
        let p = owner().await;
        p.put_at(Lamport(1), b"k", bytes("v"), None, WriteCondition::None)
            .await
            .unwrap();

        let refused = p
            .put_at(
                Lamport(2),
                b"k",
                bytes("v2"),
                None,
                WriteCondition::IfNotPresent,
            )
            .await
            .unwrap();
        assert!(matches!(refused, WriteOutcome::ConditionFailed { .. }));
        assert_eq!(
            p.committed_lamport().await.unwrap(),
            Lamport(1),
            "nothing was committed, so the owner may reuse the lamport"
        );

        p.put_at(Lamport(2), b"k", bytes("v2"), None, WriteCondition::None)
            .await
            .unwrap();
        assert_eq!(p.get(b"k").await.unwrap().unwrap().version, Version(2));
    }

    #[tokio::test]
    async fn if_not_present_lets_exactly_one_writer_win() {
        let p = owner().await;
        let first = p
            .put(b"lock", bytes("me"), None, WriteCondition::IfNotPresent)
            .await
            .unwrap();
        let second = p
            .put(b"lock", bytes("you"), None, WriteCondition::IfNotPresent)
            .await
            .unwrap();

        assert_eq!(
            first,
            WriteOutcome::Applied {
                version: Version(1)
            }
        );
        assert_eq!(
            second,
            WriteOutcome::ConditionFailed {
                condition: WriteCondition::IfNotPresent,
                found: Some(Version(1)),
            },
            "the loser must learn the winner's version"
        );
        assert_eq!(p.get(b"lock").await.unwrap().unwrap().value, bytes("me"));
    }

    // Genuinely parallel, because the point is the read-modify-write race that
    // a single-threaded runtime would never produce. The Lamport is drawn
    // inside the same critical section that submits the write, which is what
    // the owner's write path does.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_if_not_present_writes_produce_one_winner() {
        let p = Arc::new(owner().await);

        let mut handles = Vec::new();
        for i in 0..8u8 {
            let p = Arc::clone(&p);
            handles.push(tokio::task::spawn(async move {
                p.put(
                    b"lock",
                    Bytes::from(vec![i]),
                    None,
                    WriteCondition::IfNotPresent,
                )
                .await
                .unwrap()
            }));
        }

        let mut winners = 0;
        for handle in handles {
            match handle.await.unwrap() {
                WriteOutcome::Applied { version } => {
                    winners += 1;
                    assert_eq!(version, Version(1));
                }
                WriteOutcome::ConditionFailed { found, .. } => {
                    assert_eq!(found, Some(Version(1)), "losers report the winner");
                }
            }
        }
        assert_eq!(winners, 1, "exactly one writer may take the lock");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_compare_and_swap_loop_loses_no_updates() {
        let p = Arc::new(owner().await);
        p.put(b"counter", Bytes::from(vec![0]), None, WriteCondition::None)
            .await
            .unwrap();

        let mut handles = Vec::new();
        for _ in 0..4 {
            let p = Arc::clone(&p);
            handles.push(tokio::task::spawn(async move {
                for _ in 0..25 {
                    loop {
                        let current = p.get(b"counter").await.unwrap().unwrap();
                        let next = Bytes::from(vec![current.value[0].wrapping_add(1)]);
                        let outcome = p
                            .put(
                                b"counter",
                                next,
                                None,
                                WriteCondition::IfVersion(current.version),
                            )
                            .await
                            .unwrap();
                        if matches!(outcome, WriteOutcome::Applied { .. }) {
                            break;
                        }
                    }
                }
            }));
        }
        for handle in handles {
            handle.await.unwrap();
        }

        let final_record = p.get(b"counter").await.unwrap().unwrap();
        assert_eq!(
            final_record.version,
            Version(101),
            "one committed lamport per increment, plus the initial write"
        );
        assert_eq!(final_record.value[0], 100, "no increment was lost");
    }

    #[tokio::test]
    async fn a_failed_version_check_reports_the_version_actually_found() {
        let p = owner().await;
        p.put_at(Lamport(3), b"k", bytes("v"), None, WriteCondition::None)
            .await
            .unwrap();
        p.put_at(Lamport(8), b"k", bytes("v2"), None, WriteCondition::None)
            .await
            .unwrap();

        let outcome = p
            .put_at(
                Lamport(9),
                b"k",
                bytes("v3"),
                None,
                WriteCondition::IfVersion(Version(3)),
            )
            .await
            .unwrap();
        assert_eq!(
            outcome,
            WriteOutcome::ConditionFailed {
                condition: WriteCondition::IfVersion(Version(3)),
                found: Some(Version(8)),
            }
        );
        assert!(matches!(
            outcome.into_result(),
            Err(Error::VersionMismatch {
                expected: Version(3),
                actual: Some(Version(8))
            })
        ));
    }

    #[tokio::test]
    async fn a_version_check_against_a_missing_key_reports_no_version() {
        let p = owner().await;
        let outcome = p
            .put(
                b"k",
                bytes("v"),
                None,
                WriteCondition::IfVersion(Version(1)),
            )
            .await
            .unwrap();
        assert_eq!(
            outcome,
            WriteOutcome::ConditionFailed {
                condition: WriteCondition::IfVersion(Version(1)),
                found: None,
            }
        );
    }

    #[tokio::test]
    async fn a_deleted_key_is_absent_but_its_tombstone_keeps_the_delete_version() {
        let p = owner().await;
        p.put_at(Lamport(4), b"k", bytes("v"), None, WriteCondition::None)
            .await
            .unwrap();
        let deleted = p
            .delete_at(Lamport(6), b"k", WriteCondition::None)
            .await
            .unwrap();

        assert_eq!(
            deleted,
            WriteOutcome::Applied {
                version: Version(6)
            }
        );
        assert_eq!(p.get(b"k").await.unwrap(), None, "invisible to readers");
        assert_eq!(
            p.stored_entry(b"k").await.unwrap().unwrap().version,
            Version(6),
            "the tombstone still knows when the delete happened"
        );
    }

    #[tokio::test]
    async fn writing_after_a_delete_takes_a_later_version_than_the_delete() {
        let p = owner().await;
        p.put_at(Lamport(4), b"k", bytes("v"), None, WriteCondition::None)
            .await
            .unwrap();
        p.delete_at(Lamport(6), b"k", WriteCondition::None)
            .await
            .unwrap();
        let outcome = p
            .put_at(
                Lamport(7),
                b"k",
                bytes("v2"),
                None,
                WriteCondition::IfNotPresent,
            )
            .await
            .unwrap();

        assert_eq!(
            outcome,
            WriteOutcome::Applied {
                version: Version(7)
            },
            "a deleted key is absent, and its version keeps moving forward"
        );
    }

    #[tokio::test]
    async fn repeating_a_delete_does_not_move_the_version() {
        let p = owner().await;
        p.put_at(Lamport(1), b"k", bytes("v"), None, WriteCondition::None)
            .await
            .unwrap();
        let first = p
            .delete_at(Lamport(2), b"k", WriteCondition::None)
            .await
            .unwrap();
        let second = p
            .delete_at(Lamport(3), b"k", WriteCondition::None)
            .await
            .unwrap();

        assert_eq!(first, second, "a retried delete must converge");
        assert_eq!(
            p.committed_lamport().await.unwrap(),
            Lamport(2),
            "a delete that changes nothing commits nothing"
        );
    }

    #[tokio::test]
    async fn deleting_a_key_that_never_existed_succeeds() {
        let p = owner().await;
        let outcome = p
            .delete_at(Lamport(3), b"ghost", WriteCondition::None)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            WriteOutcome::Applied {
                version: Version(3)
            }
        );
        assert_eq!(p.get(b"ghost").await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_conditional_delete_reports_the_version_it_found() {
        let p = owner().await;
        p.put_at(Lamport(2), b"k", bytes("v"), None, WriteCondition::None)
            .await
            .unwrap();
        let outcome = p
            .delete_at(Lamport(3), b"k", WriteCondition::IfVersion(Version(7)))
            .await
            .unwrap();
        assert_eq!(
            outcome,
            WriteOutcome::ConditionFailed {
                condition: WriteCondition::IfVersion(Version(7)),
                found: Some(Version(2)),
            }
        );
        assert!(p.get(b"k").await.unwrap().is_some(), "nothing was removed");
    }

    #[tokio::test]
    async fn a_recreated_key_cannot_be_swapped_with_its_pre_deletion_version() {
        // The ABA that per-key counters had. Under those, the recreated key
        // would sit at version 1 again and this compare-and-swap would
        // succeed against a value the caller has never seen.
        let (p, clock) = owner_with_clock().await;
        clock.set_millis(0);
        p.put_at(
            Lamport(1),
            b"lease",
            bytes("first"),
            None,
            WriteCondition::None,
        )
        .await
        .unwrap();
        let held = p.get(b"lease").await.unwrap().unwrap().version;
        p.delete_at(Lamport(2), b"lease", WriteCondition::None)
            .await
            .unwrap();

        // Wait out tombstone retention and let compaction take the evidence.
        clock.set_millis(TOMBSTONE_RETENTION_MILLIS);
        p.compact().await.unwrap();
        assert_eq!(
            p.stored_entry(b"lease").await.unwrap(),
            None,
            "the key is physically gone, which is the setup for the ABA"
        );

        p.put_at(
            Lamport(3),
            b"lease",
            bytes("second"),
            None,
            WriteCondition::None,
        )
        .await
        .unwrap();

        let outcome = p
            .put_at(
                Lamport(4),
                b"lease",
                bytes("stolen"),
                None,
                WriteCondition::IfVersion(held),
            )
            .await
            .unwrap();
        assert_eq!(
            outcome,
            WriteOutcome::ConditionFailed {
                condition: WriteCondition::IfVersion(held),
                found: Some(Version(3)),
            },
            "a version from before the delete may never match again"
        );
        assert_eq!(
            p.get(b"lease").await.unwrap().unwrap().value,
            bytes("second")
        );
    }

    #[tokio::test]
    async fn a_key_expires_in_the_millisecond_its_deadline_arrives() {
        let (p, clock) = owner_with_clock().await;
        clock.set_millis(1_000);
        p.put(
            b"session",
            bytes("v"),
            Some(Duration::from_millis(50)),
            WriteCondition::None,
        )
        .await
        .unwrap();

        clock.set_millis(1_049);
        assert!(p.get(b"session").await.unwrap().is_some());

        clock.set_millis(1_050);
        assert_eq!(
            p.get(b"session").await.unwrap(),
            None,
            "the key is gone at the deadline, not after it"
        );
    }

    #[tokio::test]
    async fn an_expired_key_disappears_from_scans_too() {
        let (p, clock) = owner_with_clock().await;
        clock.set_millis(0);
        p.put(b"a", bytes("v"), None, WriteCondition::None)
            .await
            .unwrap();
        p.put(
            b"b",
            bytes("v"),
            Some(Duration::from_millis(10)),
            WriteCondition::None,
        )
        .await
        .unwrap();

        clock.set_millis(10);
        let page = p.scan(b"", None, ScanBudget::of_entries(10)).await.unwrap();
        assert_eq!(
            page.entries
                .iter()
                .map(|e| e.key.clone())
                .collect::<Vec<_>>(),
            vec![bytes("a")]
        );
    }

    #[tokio::test]
    async fn an_expired_key_can_be_taken_by_if_not_present() {
        let (p, clock) = owner_with_clock().await;
        clock.set_millis(0);
        p.put_at(
            Lamport(1),
            b"lease",
            bytes("holder-1"),
            Some(Duration::from_millis(5)),
            WriteCondition::IfNotPresent,
        )
        .await
        .unwrap();

        clock.set_millis(5);
        let outcome = p
            .put_at(
                Lamport(2),
                b"lease",
                bytes("holder-2"),
                None,
                WriteCondition::IfNotPresent,
            )
            .await
            .unwrap();
        assert_eq!(
            outcome,
            WriteOutcome::Applied {
                version: Version(2)
            },
            "an expired lease is free to take, at the lamport that took it"
        );
    }

    #[tokio::test]
    async fn compaction_reclaims_expired_records() {
        let (p, clock) = owner_with_clock().await;
        clock.set_millis(0);
        p.put(
            b"tmp",
            bytes("v"),
            Some(Duration::from_millis(1)),
            WriteCondition::None,
        )
        .await
        .unwrap();
        p.put(b"keep", bytes("v"), None, WriteCondition::None)
            .await
            .unwrap();

        clock.set_millis(1);
        assert!(
            p.stored_entry(b"tmp").await.unwrap().is_some(),
            "still stored before the sweep"
        );

        p.compact().await.unwrap();
        assert_eq!(
            p.stored_entry(b"tmp").await.unwrap(),
            None,
            "expired records are physically gone after compaction"
        );
        assert!(p.stored_entry(b"keep").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn compaction_reclaims_tombstones_once_they_age_out() {
        let (p, clock) = owner_with_clock().await;
        clock.set_millis(0);
        p.put(b"k", bytes("v"), None, WriteCondition::None)
            .await
            .unwrap();
        p.delete(b"k", WriteCondition::None).await.unwrap();

        p.compact().await.unwrap();
        assert!(
            p.stored_entry(b"k").await.unwrap().is_some(),
            "a fresh tombstone still answers questions about the delete"
        );

        clock.set_millis(TOMBSTONE_RETENTION_MILLIS);
        p.compact().await.unwrap();
        assert_eq!(p.stored_entry(b"k").await.unwrap(), None);
    }

    #[tokio::test]
    async fn compaction_never_reclaims_the_committed_lamport() {
        // Reclaiming a key must not free its version for reuse, or the
        // partition would hand out a version it has already used.
        let (p, clock) = owner_with_clock().await;
        clock.set_millis(0);
        p.put_at(Lamport(12), b"k", bytes("v"), None, WriteCondition::None)
            .await
            .unwrap();
        p.delete_at(Lamport(13), b"k", WriteCondition::None)
            .await
            .unwrap();

        clock.set_millis(TOMBSTONE_RETENTION_MILLIS * 2);
        p.compact().await.unwrap();

        assert_eq!(
            p.stored_entry(b"k").await.unwrap(),
            None,
            "the data is gone"
        );
        assert_eq!(p.committed_lamport().await.unwrap(), Lamport(13));
        assert!(
            matches!(
                p.put_at(Lamport(13), b"k", bytes("v"), None, WriteCondition::None)
                    .await,
                Err(Error::InvalidArgument(_))
            ),
            "a reclaimed key must not free its version for reuse"
        );
    }

    #[tokio::test]
    async fn a_scan_returns_only_keys_under_the_prefix_in_order() {
        let p = owner().await;
        for key in ["a/1", "a/2", "b/1", "a/3"] {
            p.put(key.as_bytes(), bytes("v"), None, WriteCondition::None)
                .await
                .unwrap();
        }

        let page = p
            .scan(b"a/", None, ScanBudget::of_entries(10))
            .await
            .unwrap();
        assert_eq!(
            page.entries
                .iter()
                .map(|e| e.key.clone())
                .collect::<Vec<_>>(),
            vec![bytes("a/1"), bytes("a/2"), bytes("a/3")]
        );
        assert_eq!(page.cursor, None, "the range was exhausted");
    }

    #[tokio::test]
    async fn a_cursor_walks_every_key_exactly_once() {
        let p = owner().await;
        let keys: Vec<String> = (0..25).map(|i| format!("k{i:03}")).collect();
        for key in &keys {
            p.put(key.as_bytes(), bytes("v"), None, WriteCondition::None)
                .await
                .unwrap();
        }

        let mut seen = Vec::new();
        let mut cursor: Option<Bytes> = None;
        loop {
            let page = p
                .scan(b"k", cursor.as_deref(), ScanBudget::of_entries(4))
                .await
                .unwrap();
            seen.extend(page.entries.iter().map(|e| e.key.clone()));
            match page.cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }

        let expected: Vec<Bytes> = keys.iter().map(|k| bytes(k)).collect();
        assert_eq!(seen, expected);
    }

    #[tokio::test]
    async fn a_cursor_skips_nothing_when_keys_change_between_pages() {
        let p = owner().await;
        for i in 0..20 {
            p.put(
                format!("k{i:03}").as_bytes(),
                bytes("v"),
                None,
                WriteCondition::None,
            )
            .await
            .unwrap();
        }

        let first = p.scan(b"k", None, ScanBudget::of_entries(5)).await.unwrap();
        assert_eq!(first.entries.len(), 5);

        // Rewrite a key already returned, delete one not yet reached, and add
        // one after the cursor. Only the last two may show up.
        p.put(b"k000", bytes("v2"), None, WriteCondition::None)
            .await
            .unwrap();
        p.delete(b"k010", WriteCondition::None).await.unwrap();
        p.put(b"k0055", bytes("v"), None, WriteCondition::None)
            .await
            .unwrap();

        let mut seen: Vec<Bytes> = first.entries.iter().map(|e| e.key.clone()).collect();
        let mut cursor = first.cursor;
        while let Some(c) = cursor {
            let page = p
                .scan(b"k", Some(&c), ScanBudget::of_entries(5))
                .await
                .unwrap();
            seen.extend(page.entries.iter().map(|e| e.key.clone()));
            cursor = page.cursor;
        }

        assert!(
            seen.windows(2).all(|w| w[0] < w[1]),
            "no duplicates, in order"
        );
        assert!(seen.contains(&bytes("k0055")), "a key added ahead is seen");
        assert!(!seen.contains(&bytes("k010")), "a key deleted ahead is not");
        assert_eq!(seen.len(), 20, "19 survivors plus the inserted key");
    }

    #[tokio::test]
    async fn a_page_is_a_consistent_view_of_the_keys_it_returns() {
        let p = owner().await;
        for i in 0..10 {
            p.put(
                format!("k{i}").as_bytes(),
                bytes("original"),
                None,
                WriteCondition::None,
            )
            .await
            .unwrap();
        }

        let page = p
            .scan(b"k", None, ScanBudget::of_entries(10))
            .await
            .unwrap();
        p.put(b"k5", bytes("changed"), None, WriteCondition::None)
            .await
            .unwrap();

        assert!(
            page.entries
                .iter()
                .all(|e| e.record.value == bytes("original")),
            "a page already returned does not change under the caller"
        );
    }

    #[tokio::test]
    async fn a_cursor_from_below_the_range_resumes_at_the_range_start() {
        // This is the state a caller lands in when a partition splits between
        // pages and its cursor points into the sibling half.
        let range = KeyRange::new(Bytes::from_static(b"m"), None).unwrap();
        let p = owner_in_range(range).await;
        for key in ["m1", "m2", "n1"] {
            p.put(key.as_bytes(), bytes("v"), None, WriteCondition::None)
                .await
                .unwrap();
        }

        let stale = Cursor::new(bytes("a"), b"").encode();
        let page = p
            .scan(b"", Some(&stale), ScanBudget::of_entries(10))
            .await
            .unwrap();
        assert_eq!(
            page.entries
                .iter()
                .map(|e| e.key.clone())
                .collect::<Vec<_>>(),
            vec![bytes("m1"), bytes("m2"), bytes("n1")],
            "nothing this partition owns may be skipped"
        );
    }

    #[tokio::test]
    async fn a_cursor_past_the_range_end_yields_an_empty_final_page() {
        let range = KeyRange::new(Bytes::new(), Some(Bytes::from_static(b"m"))).unwrap();
        let p = owner_in_range(range).await;
        p.put(b"a", bytes("v"), None, WriteCondition::None)
            .await
            .unwrap();

        let stale = Cursor::new(bytes("z"), b"").encode();
        let page = p
            .scan(b"", Some(&stale), ScanBudget::of_entries(10))
            .await
            .unwrap();
        assert!(page.entries.is_empty());
        assert_eq!(page.cursor, None, "there is nothing left to page through");
    }

    #[tokio::test]
    async fn a_scan_stops_at_the_partition_boundary() {
        let range = KeyRange::new(Bytes::new(), Some(Bytes::from_static(b"m"))).unwrap();
        let p = owner_in_range(range).await;
        p.put(b"a", bytes("v"), None, WriteCondition::None)
            .await
            .unwrap();
        assert!(
            p.put(b"z", bytes("v"), None, WriteCondition::None)
                .await
                .is_err(),
            "a key outside the range is not ours to write"
        );

        let page = p.scan(b"", None, ScanBudget::of_entries(10)).await.unwrap();
        assert_eq!(page.entries.len(), 1);
    }

    #[tokio::test]
    async fn a_scan_page_stops_at_the_byte_budget_and_resumes_after_the_last_entry() {
        let p = owner().await;
        // Four values of a kilobyte each. A budget under two of them must cut
        // the page after the first, and the count bound is left slack so it is
        // plainly the bytes that stopped it.
        let value = bytes(&"v".repeat(1024));
        for i in 0..4 {
            p.put(
                format!("k{i}").as_bytes(),
                value.clone(),
                None,
                WriteCondition::None,
            )
            .await
            .unwrap();
        }
        let budget = ScanBudget {
            max_entries: 10,
            max_bytes: 1500,
            include_values: true,
        };
        let first = p.scan(b"k", None, budget).await.unwrap();
        assert_eq!(
            first.entries.len(),
            1,
            "a value past the remaining budget starts the next page instead"
        );
        let cursor = first.cursor.expect("a truncated page carries a cursor");

        // The remainder pages out in full, in order, nothing dropped for the
        // entry the first page stopped before.
        let mut seen: Vec<_> = first.entries.iter().map(|e| e.key.clone()).collect();
        let mut cursor = Some(cursor);
        while let Some(c) = cursor.take() {
            let page = p.scan(b"k", Some(&c), budget).await.unwrap();
            assert!(!page.entries.is_empty(), "a cursor must make progress");
            seen.extend(page.entries.iter().map(|e| e.key.clone()));
            cursor = page.cursor;
        }
        let expected: Vec<Bytes> = (0..4).map(|i| Bytes::from(format!("k{i}"))).collect();
        assert_eq!(seen, expected, "every key returned once, in order");
    }

    #[tokio::test]
    async fn a_scan_returns_a_first_entry_larger_than_the_whole_budget() {
        let p = owner().await;
        let value = bytes(&"v".repeat(4096));
        p.put(b"big", value, None, WriteCondition::None)
            .await
            .unwrap();
        // A budget smaller than the one value must still return it: a hard cap
        // below a single value would make the scan unable to move past the key.
        let page = p
            .scan(
                b"",
                None,
                ScanBudget {
                    max_entries: 10,
                    max_bytes: 8,
                    include_values: true,
                },
            )
            .await
            .unwrap();
        assert_eq!(page.entries.len(), 1);
        assert!(page.cursor.is_none(), "the only key is the last one");
    }

    #[tokio::test]
    async fn a_keys_only_scan_does_not_count_values_against_the_budget() {
        let p = owner().await;
        let value = bytes(&"v".repeat(4096));
        for i in 0..4 {
            p.put(
                format!("k{i}").as_bytes(),
                value.clone(),
                None,
                WriteCondition::None,
            )
            .await
            .unwrap();
        }
        // Small keys, large values. With values excluded the whole set fits a
        // budget far below one value, so a keys-only page is not truncated for
        // bytes it will never send.
        let page = p
            .scan(
                b"k",
                None,
                ScanBudget {
                    max_entries: 10,
                    max_bytes: 64,
                    include_values: false,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            page.entries.len(),
            4,
            "keys alone stay well under the budget"
        );
        assert!(page.cursor.is_none());
    }

    #[tokio::test]
    async fn a_scan_limit_outside_the_allowed_range_is_rejected() {
        let p = owner().await;
        assert!(matches!(
            p.scan(b"", None, ScanBudget::of_entries(0)).await,
            Err(Error::InvalidArgument(_))
        ));
        assert!(matches!(
            p.scan(b"", None, ScanBudget::of_entries(MAX_LIST_LIMIT + 1))
                .await,
            Err(Error::InvalidArgument(_))
        ));
    }

    #[tokio::test]
    async fn an_oversized_key_is_rejected_before_the_engine_sees_it() {
        let p = owner().await;
        let key = vec![b'k'; MAX_KEY_BYTES + 1];
        assert!(matches!(
            p.put(&key, bytes("v"), None, WriteCondition::None).await,
            Err(Error::TooLarge { what: "key", .. })
        ));
        assert!(matches!(
            p.get(&key).await,
            Err(Error::TooLarge { what: "key", .. })
        ));
        assert!(matches!(
            p.delete(&key, WriteCondition::None).await,
            Err(Error::TooLarge { what: "key", .. })
        ));

        // Nothing reached storage, so the largest legal key is untouched and
        // no lamport was burned.
        let legal = vec![b'k'; MAX_KEY_BYTES];
        assert_eq!(p.get(&legal).await.unwrap(), None);
        assert_eq!(p.committed_lamport().await.unwrap(), Lamport::ZERO);
    }

    #[tokio::test]
    async fn an_oversized_value_is_rejected_before_the_engine_sees_it() {
        let p = owner().await;
        let value = Bytes::from(vec![0u8; MAX_VALUE_BYTES + 1]);
        assert!(matches!(
            p.put(b"k", value, None, WriteCondition::None).await,
            Err(Error::TooLarge { what: "value", .. })
        ));
        assert_eq!(p.get(b"k").await.unwrap(), None);
        assert_eq!(p.committed_lamport().await.unwrap(), Lamport::ZERO);
    }

    #[tokio::test]
    async fn applying_a_mutation_twice_is_the_same_as_applying_it_once() {
        let (p, _clock) = partition_with_clock().await;
        let mutation = Mutation::put(Lamport(1), bytes("k"), bytes("v"), None);

        p.apply(&mutation).await.unwrap();
        let after_first = p.get(b"k").await.unwrap();
        p.apply(&mutation).await.unwrap();

        assert_eq!(p.get(b"k").await.unwrap(), after_first);
        assert_eq!(p.committed_lamport().await.unwrap(), Lamport(1));
    }

    #[tokio::test]
    async fn an_applied_mutation_takes_its_version_from_its_lamport() {
        let (p, _clock) = partition_with_clock().await;
        p.apply(&Mutation::put(Lamport(512), bytes("k"), bytes("v"), None))
            .await
            .unwrap();

        assert_eq!(
            p.get(b"k").await.unwrap().unwrap().version,
            Version(512),
            "the log carries one number and it is the version"
        );
    }

    #[tokio::test]
    async fn replaying_a_span_of_mutations_reaches_the_same_state() {
        let (p, _clock) = partition_with_clock().await;
        let mutations: Vec<Mutation> = (1..=4u64)
            .map(|i| {
                Mutation::put(
                    Lamport(i * 10),
                    bytes("k"),
                    Bytes::from(vec![i as u8]),
                    None,
                )
            })
            .collect();

        for m in &mutations {
            p.apply(m).await.unwrap();
        }
        let settled = p.get(b"k").await.unwrap();

        for m in &mutations {
            p.apply(m).await.unwrap();
        }
        assert_eq!(
            p.get(b"k").await.unwrap(),
            settled,
            "a replay from the start of the log must not rewind the partition"
        );
        assert_eq!(p.committed_lamport().await.unwrap(), Lamport(40));
    }

    #[tokio::test]
    async fn an_applied_delete_hides_the_key_and_keeps_its_version() {
        let (p, _clock) = partition_with_clock().await;
        p.apply(&Mutation::put(Lamport(1), bytes("k"), bytes("v"), None))
            .await
            .unwrap();
        p.apply(&Mutation::delete(Lamport(2), bytes("k"), u64::MAX))
            .await
            .unwrap();

        assert_eq!(p.get(b"k").await.unwrap(), None);
        assert_eq!(
            p.stored_entry(b"k").await.unwrap().unwrap().version,
            Version(2)
        );
    }

    #[tokio::test]
    async fn an_applied_expiry_is_the_owners_deadline_not_the_replicas() {
        let (p, clock) = partition_with_clock().await;
        clock.set_millis(9_000);
        // Decided on the owner at time 0 with a 100ms TTL. A replica that
        // recomputed it would extend the key's life by nine seconds.
        p.apply(&Mutation::put(
            Lamport(1),
            bytes("k"),
            bytes("v"),
            Some(100),
        ))
        .await
        .unwrap();

        assert_eq!(p.get(b"k").await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_local_write_and_a_replayed_one_share_the_same_sequence() {
        // A promoted replica keeps writing where the log left off, so the two
        // paths must not each keep their own idea of the committed Lamport.
        let p = owner().await;
        p.apply(&Mutation::put(Lamport(30), bytes("k"), bytes("v"), None))
            .await
            .unwrap();

        assert!(
            matches!(
                p.put_at(Lamport(30), b"k", bytes("v2"), None, WriteCondition::None)
                    .await,
                Err(Error::InvalidArgument(_))
            ),
            "a local write may not reuse a lamport the log already spent"
        );
        p.put_at(Lamport(31), b"k", bytes("v2"), None, WriteCondition::None)
            .await
            .unwrap();
        assert_eq!(p.get(b"k").await.unwrap().unwrap().version, Version(31));
    }

    #[tokio::test]
    async fn size_accounting_grows_with_the_data_written() {
        let p = owner().await;
        let empty = p.size_bytes().await.unwrap();
        for i in 0..200 {
            p.put(
                format!("k{i:04}").as_bytes(),
                Bytes::from(vec![7u8; 1024]),
                None,
                WriteCondition::None,
            )
            .await
            .unwrap();
        }
        assert!(
            p.size_bytes().await.unwrap() > empty + 100 * 1024,
            "the leader splits on this number, so it has to move"
        );
    }

    #[tokio::test]
    async fn index_memory_grows_with_the_keys_flushed_and_survives_a_reopen() {
        // ADR 0006 makes the index resident whether or not the values are, so
        // this is what a worker runs out of first. An operator watching it
        // needs it to move with the key count and to be the same number after
        // a restart, since a restart rebuilds the index from the manifest.
        let (p, _clock) = partition_with_clock().await;
        assert_eq!(p.index_bytes().await.unwrap(), 0);

        for i in 0..200u32 {
            p.apply(&Mutation::put(
                Lamport(u64::from(i) + 1),
                Bytes::from(format!("k{i:04}")),
                bytes("v"),
                None,
            ))
            .await
            .unwrap();
        }
        assert_eq!(
            p.index_bytes().await.unwrap(),
            0,
            "an unflushed write is in the mutable table, not the index"
        );

        p.flush().await.unwrap();
        let flushed = p.index_bytes().await.unwrap();
        assert!(flushed >= 200 * 5, "200 five-byte keys at least: {flushed}");

        let p = reopened(p).await;
        assert_eq!(
            p.index_bytes().await.unwrap(),
            flushed,
            "a rebuilt index costs what the one it replaced did"
        );
    }

    #[tokio::test]
    async fn index_memory_does_not_double_count_a_key_written_twice() {
        // Every flush inserts into the index, and a key that moves to a newer
        // segment is one entry rather than two. Counting it twice would grow
        // the number without bound under an overwrite workload.
        let (p, _clock) = partition_with_clock().await;
        p.apply(&Mutation::put(Lamport(1), bytes("k"), bytes("v"), None))
            .await
            .unwrap();
        p.flush().await.unwrap();
        let once = p.index_bytes().await.unwrap();

        p.apply(&Mutation::put(Lamport(2), bytes("k"), bytes("v2"), None))
            .await
            .unwrap();
        p.flush().await.unwrap();

        assert_eq!(p.index_bytes().await.unwrap(), once);
    }

    #[tokio::test]
    async fn a_flush_publishes_and_a_reopen_reads_it_back() {
        let (p, _clock) = partition_with_clock().await;
        p.apply(&Mutation::put(Lamport(3), bytes("a"), bytes("one"), None))
            .await
            .unwrap();
        p.apply(&Mutation::put(Lamport(5), bytes("b"), bytes("two"), None))
            .await
            .unwrap();
        p.flush().await.unwrap();

        let p = reopened(p).await;
        assert_eq!(p.get(b"a").await.unwrap().unwrap().value, bytes("one"));
        assert_eq!(p.get(b"b").await.unwrap().unwrap().version, Version(5));
        assert_eq!(
            p.committed_lamport().await.unwrap(),
            Lamport(5),
            "the manifest horizon is where replay starts, so it has to hold"
        );
    }

    #[tokio::test]
    async fn writes_above_the_flush_horizon_are_the_logs_to_restore() {
        // The division of labour a restart depends on: the manifest carries
        // everything at or below its horizon, and the caller replays its log
        // above it. Storage alone forgetting an unflushed write is correct,
        // and replaying it back is idempotent.
        let (p, _clock) = partition_with_clock().await;
        let flushed = Mutation::put(Lamport(1), bytes("a"), bytes("v"), None);
        let unflushed = Mutation::put(Lamport(2), bytes("b"), bytes("v"), None);
        p.apply(&flushed).await.unwrap();
        p.flush().await.unwrap();
        p.apply(&unflushed).await.unwrap();

        let p = reopened(p).await;
        assert!(p.get(b"a").await.unwrap().is_some());
        assert_eq!(
            p.get(b"b").await.unwrap(),
            None,
            "an unflushed write is the log's to bring back"
        );
        assert_eq!(p.committed_lamport().await.unwrap(), Lamport(1));

        p.apply(&flushed).await.unwrap();
        p.apply(&unflushed).await.unwrap();
        assert!(p.get(b"b").await.unwrap().is_some());
        assert_eq!(p.committed_lamport().await.unwrap(), Lamport(2));
    }

    #[tokio::test]
    async fn reads_and_scans_span_the_memtable_and_the_segments() {
        let (p, _clock) = partition_with_clock().await;
        p.apply(&Mutation::put(Lamport(1), bytes("a"), bytes("old"), None))
            .await
            .unwrap();
        p.apply(&Mutation::put(
            Lamport(2),
            bytes("b"),
            bytes("flushed"),
            None,
        ))
        .await
        .unwrap();
        p.flush().await.unwrap();
        // One key overwritten in the memtable, one new one beside it.
        p.apply(&Mutation::put(Lamport(3), bytes("a"), bytes("new"), None))
            .await
            .unwrap();
        p.apply(&Mutation::put(
            Lamport(4),
            bytes("c"),
            bytes("recent"),
            None,
        ))
        .await
        .unwrap();

        assert_eq!(
            p.get(b"a").await.unwrap().unwrap().value,
            bytes("new"),
            "the memtable shadows the segment"
        );
        let page = p.scan(b"", None, ScanBudget::of_entries(10)).await.unwrap();
        let keys: Vec<Bytes> = page.entries.iter().map(|e| e.key.clone()).collect();
        assert_eq!(keys, vec![bytes("a"), bytes("b"), bytes("c")]);
        assert_eq!(page.entries[0].record.value, bytes("new"));
    }

    #[tokio::test]
    async fn compaction_folds_the_segments_into_one() {
        let (p, _clock) = partition_with_clock().await;
        for (lamport, key) in [(1u64, "a"), (2, "b")] {
            p.apply(&Mutation::put(
                Lamport(lamport),
                bytes(key),
                bytes("v"),
                None,
            ))
            .await
            .unwrap();
            p.flush().await.unwrap();
        }
        assert_eq!(p.segment_count().await, 2);

        p.compact().await.unwrap();
        assert_eq!(p.segment_count().await, 1);
        assert!(p.get(b"a").await.unwrap().is_some());
        assert!(p.get(b"b").await.unwrap().is_some());

        let p = reopened(p).await;
        assert!(
            p.get(b"a").await.unwrap().is_some() && p.get(b"b").await.unwrap().is_some(),
            "the compacted manifest is the one a reopen finds"
        );
    }

    #[tokio::test]
    async fn a_deleted_key_flushes_as_a_tombstone_not_an_absence() {
        // The distinction a conditional write needs survives the flush: a
        // reopened partition can still tell "deleted at version 2" from
        // "never existed".
        let (p, _clock) = partition_with_clock().await;
        p.apply(&Mutation::put(Lamport(1), bytes("k"), bytes("v"), None))
            .await
            .unwrap();
        p.apply(&Mutation::delete(Lamport(2), bytes("k"), u64::MAX))
            .await
            .unwrap();
        p.flush().await.unwrap();

        let p = reopened(p).await;
        assert_eq!(p.get(b"k").await.unwrap(), None);
        let stored = p.stored_entry(b"k").await.unwrap().unwrap();
        assert!(stored.deleted);
        assert_eq!(stored.version, Version(2));
    }

    #[tokio::test]
    async fn tiny_timed_flushes_do_not_trigger_full_partition_compaction() {
        let (p, _clock) = partition_with_clock().await;
        for lamport in 1..=COMPACT_TRIGGER_FULL_FLUSHES as u64 {
            p.apply(&Mutation::put(
                Lamport(lamport),
                bytes(&format!("k{lamport:02}")),
                bytes("v"),
                None,
            ))
            .await
            .unwrap();
            p.flush().await.unwrap();
        }

        assert_eq!(
            p.segment_count().await,
            COMPACT_TRIGGER_FULL_FLUSHES,
            "time-based durability must not turn sixteen tiny writes into a full rewrite"
        );
    }

    #[tokio::test]
    async fn timer_flushes_eventually_reclaim_an_expired_sparse_segment() {
        let (p, clock) = partition_with_clock().await;
        clock.set_millis(0);
        p.apply(&Mutation::put(
            Lamport(1),
            bytes("expired"),
            bytes("value"),
            Some(1),
        ))
        .await
        .unwrap();
        p.flush().await.unwrap();
        clock.set_millis(1);
        assert!(p.stored_entry(b"expired").await.unwrap().is_some());

        for _ in 0..COMPACT_TRIGGER_TIMER_FLUSHES {
            p.flush().await.unwrap();
        }
        // The flushes arm the trigger; the host's maintenance pass is what acts
        // on it. Compaction no longer spills out of a flush, so a test that
        // only flushed would be asserting the coupling this change removed
        // rather than the reclamation it is about.
        assert!(
            p.stored_entry(b"expired").await.unwrap().is_some(),
            "flushing alone must not compact, or a client write would pay for it"
        );
        p.compact_if_needed().await.unwrap();

        assert_eq!(
            p.stored_entry(b"expired").await.unwrap(),
            None,
            "timer-only partitions still receive eventual physical reclamation"
        );
    }

    #[tokio::test]
    async fn the_orphan_sweep_reclaims_a_stranded_object_once_it_ages_out() {
        use crate::testing::owner_with_clocked_store;
        use orbita_core::{KeyspaceId, PartitionId};
        use orbita_format::paths::segment_name;

        let (p, store, clock) = owner_with_clocked_store().await;
        let path = PartitionPath::new("", KeyspaceId(1), PartitionId(1));

        // An object an abandoned commit could leave: a segment name this format
        // parses, written under the partition prefix, that no manifest names.
        let orphan = path.object(&segment_name(Epoch(1), 999));
        clock.set_millis(1_000);
        store
            .put(&orphan, Bytes::from_static(b"stranded"))
            .await
            .unwrap();

        // Publish a manifest at the same instant. The manifest that leaves the
        // orphan unreferenced has only just been published, so nothing is safe
        // to delete yet.
        clock.set_millis(1_000);
        p.put(b"a", bytes("v"), None, WriteCondition::None)
            .await
            .unwrap();
        p.flush().await.unwrap();

        let report = p
            .sweep_orphans(&BTreeSet::new(), 5_000, 0, false)
            .await
            .unwrap();
        assert!(
            report.deleted.is_empty(),
            "the dropping manifest has not settled yet"
        );
        assert!(
            store.keys().contains(&orphan),
            "and so the orphan is still on the store"
        );

        // A later flush republishes the manifest; then time advances past the
        // grace period, so the manifest that dropped the orphan has settled and
        // the orphan is old in its own right.
        clock.set_millis(10_000);
        p.put(b"b", bytes("v"), None, WriteCondition::None)
            .await
            .unwrap();
        p.flush().await.unwrap();
        clock.set_millis(20_000);

        // A dry run first: it names the orphan without removing it.
        let preview = p
            .sweep_orphans(&BTreeSet::new(), 5_000, 0, true)
            .await
            .unwrap();
        assert_eq!(preview.deleted, vec![orphan.clone()]);
        assert!(store.keys().contains(&orphan), "a dry run touches nothing");

        // Then for real.
        let swept = p
            .sweep_orphans(&BTreeSet::new(), 5_000, 0, false)
            .await
            .unwrap();
        assert_eq!(swept.deleted, vec![orphan.clone()]);
        assert!(
            !store.keys().contains(&orphan),
            "the aged orphan is reclaimed"
        );
        // The live keys and their segments are untouched.
        assert!(p.get(b"a").await.unwrap().is_some());
        assert!(p.get(b"b").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn a_replica_reclaims_only_entries_covered_by_the_owners_manifest() {
        let (owner, replica, _clock) = partition_pair_with_clock().await;
        for lamport in 1..=32 {
            let mutation = Mutation::put(
                Lamport(lamport),
                bytes(&format!("k{lamport}")),
                Bytes::from(vec![lamport as u8; MAX_VALUE_BYTES]),
                None,
            );
            owner.apply(&mutation).await.unwrap();
            replica.apply(&mutation).await.unwrap();
        }
        assert!(replica.memtable_bytes().await >= FLUSH_TRIGGER_BYTES);

        owner.flush().await.unwrap();
        replica
            .apply(&Mutation::put(
                Lamport(33),
                bytes("above"),
                Bytes::from(vec![33; MAX_VALUE_BYTES]),
                None,
            ))
            .await
            .unwrap();
        assert!(
            replica.stored_entry(b"above").await.unwrap().is_some(),
            "the entry above the horizon starts in the memtable"
        );
        replica.reclaim_published_if_needed().await.unwrap();

        assert!(replica.memtable_bytes().await < FLUSH_TRIGGER_BYTES);
        assert_eq!(
            replica.get(b"k32").await.unwrap().unwrap().value.len(),
            MAX_VALUE_BYTES,
            "reclamation replaces the memtable with the published index"
        );
        assert_eq!(
            replica
                .stored_entry(b"above")
                .await
                .unwrap()
                .unwrap()
                .version,
            Version(33),
            "an entry above the manifest horizon remains in the memtable"
        );
    }

    #[tokio::test]
    async fn hydrating_builds_the_whole_partition_from_the_published_manifest() {
        // The replacement-worker case: a partition that holds nothing takes on
        // one that has been flushed, and gets it from the bucket rather than
        // from a peer.
        let (owner, fresh, _clock) = partition_pair_with_clock().await;
        for lamport in 1..=4 {
            owner
                .apply(&Mutation::put(
                    Lamport(lamport),
                    bytes(&format!("k{lamport}")),
                    bytes("value"),
                    None,
                ))
                .await
                .unwrap();
        }
        owner.flush().await.unwrap();

        assert_eq!(fresh.get(b"k1").await.unwrap(), None, "nothing yet");
        assert_eq!(
            fresh.hydrate().await.unwrap(),
            Hydration {
                epoch: Epoch(1),
                through: Lamport(4)
            }
        );

        for lamport in 1..=4 {
            let key = format!("k{lamport}");
            assert_eq!(
                fresh.get(key.as_bytes()).await.unwrap().unwrap().version,
                Version(lamport),
                "every key the manifest names is readable after hydration"
            );
        }
        assert_eq!(fresh.committed_lamport().await.unwrap(), Lamport(4));
        assert_eq!(fresh.flushed_lamport().await.unwrap(), Lamport(4));
    }

    #[tokio::test]
    async fn hydrating_twice_changes_nothing_the_second_time() {
        // Idempotence is what makes hydration safe to retry after a failure,
        // and safe to call on a path that cannot tell whether it is needed.
        let (owner, fresh, _clock) = partition_pair_with_clock().await;
        owner
            .apply(&Mutation::put(Lamport(7), bytes("k"), bytes("v"), None))
            .await
            .unwrap();
        owner.flush().await.unwrap();

        let at_seven = Hydration {
            epoch: Epoch(1),
            through: Lamport(7),
        };
        assert_eq!(fresh.hydrate().await.unwrap(), at_seven);
        assert_eq!(fresh.hydrate().await.unwrap(), at_seven);
        assert_eq!(fresh.get(b"k").await.unwrap().unwrap().version, Version(7));
        assert_eq!(fresh.segment_count().await, 1, "no second copy was adopted");
    }

    #[tokio::test]
    async fn hydrating_a_partition_that_was_never_flushed_reports_nothing_to_download() {
        let (fresh, _clock) = partition_with_clock().await;
        assert_eq!(fresh.hydrate().await.unwrap(), Hydration::default());
    }

    #[tokio::test]
    async fn hydrating_keeps_writes_that_arrived_above_the_manifest_horizon() {
        // The horizon is exactly the line the segments cover, so a write above
        // it is the caller's to keep. Dropping it would lose an acknowledged
        // write, which is the one thing this system promises never to do.
        let (owner, replica, _clock) = partition_pair_with_clock().await;
        owner
            .apply(&Mutation::put(Lamport(1), bytes("old"), bytes("v"), None))
            .await
            .unwrap();
        owner.flush().await.unwrap();

        replica
            .apply(&Mutation::put(Lamport(9), bytes("new"), bytes("v"), None))
            .await
            .unwrap();
        assert_eq!(replica.hydrate().await.unwrap().through, Lamport(1));

        assert!(replica.get(b"old").await.unwrap().is_some());
        assert_eq!(
            replica.get(b"new").await.unwrap().unwrap().version,
            Version(9),
            "the write above the horizon survives the rebuild"
        );
        assert_eq!(
            replica.committed_lamport().await.unwrap(),
            Lamport(9),
            "adopting an older manifest must not rewind the version sequence"
        );
    }

    #[tokio::test]
    async fn hydrating_reports_the_epoch_of_the_writer_that_published_the_manifest() {
        // The manifest is evidence about who owns this partition, not only
        // about where its data ends. A node that rebuilds from a manifest an
        // epoch-2 owner published has learned that anybody still claiming
        // epoch 1 has been deposed, and it can only act on that if the epoch
        // travels back with the horizon.
        let (owner, behind, _clock) = partition_pair_at_epochs(Epoch(2), Epoch(1)).await;
        owner
            .apply(&Mutation::put(Lamport(4), bytes("k"), bytes("v"), None))
            .await
            .unwrap();
        owner.flush().await.unwrap();

        assert_eq!(
            behind.hydrate().await.unwrap(),
            Hydration {
                epoch: Epoch(2),
                through: Lamport(4)
            }
        );
    }

    #[tokio::test]
    async fn a_republished_manifest_carries_its_epoch_even_when_the_horizon_stands_still() {
        // A compaction republishes the same horizon under the new owner's
        // epoch. A reader that only looked at the horizon would decide there
        // was nothing to learn and keep serving the deposed owner.
        let (owner, behind, _clock) = partition_pair_at_epochs(Epoch(1), Epoch(1)).await;
        owner
            .apply(&Mutation::put(Lamport(4), bytes("k"), bytes("v"), None))
            .await
            .unwrap();
        owner.flush().await.unwrap();
        assert_eq!(behind.hydrate().await.unwrap().epoch, Epoch(1));

        let successor = behind.successor(Epoch(5)).await;
        successor.compact().await.unwrap();

        let found = behind.hydrate().await.unwrap();
        assert_eq!(found.through, Lamport(4), "the horizon did not move");
        assert_eq!(
            found.epoch,
            Epoch(5),
            "and the ownership evidence did, which is the half that matters here"
        );
    }

    #[tokio::test]
    async fn an_adopted_epoch_never_goes_backwards() {
        // Epochs only rise, because a manifest reaches the bucket through a
        // fenced compare-and-swap. A partition that has seen evidence of a
        // newer owner must not be talked back into forgetting it.
        let (owner, behind, _clock) = partition_pair_at_epochs(Epoch(7), Epoch(1)).await;
        owner
            .apply(&Mutation::put(Lamport(1), bytes("k"), bytes("v"), None))
            .await
            .unwrap();
        owner.flush().await.unwrap();
        assert_eq!(behind.hydrate().await.unwrap().epoch, Epoch(7));
        assert_eq!(
            behind.hydrate().await.unwrap().epoch,
            Epoch(7),
            "a second read of the same manifest cannot lower it"
        );
        assert_eq!(behind.hydration().await.epoch, Epoch(7));
    }

    #[tokio::test]
    async fn hydrating_against_a_manifest_that_is_behind_leaves_the_partition_alone() {
        // A node whose own state is ahead of the bucket, which is the steady
        // state of any owner between flushes.
        let (owner, _replica, _clock) = partition_pair_with_clock().await;
        owner
            .apply(&Mutation::put(Lamport(1), bytes("a"), bytes("v"), None))
            .await
            .unwrap();
        owner.flush().await.unwrap();
        owner
            .apply(&Mutation::put(Lamport(2), bytes("b"), bytes("v"), None))
            .await
            .unwrap();

        assert_eq!(owner.hydrate().await.unwrap().through, Lamport(1));
        assert_eq!(owner.committed_lamport().await.unwrap(), Lamport(2));
        assert!(
            owner.get(b"b").await.unwrap().is_some(),
            "the unflushed write is still there"
        );
    }

    // The loop behind issue #141. A replica with a gap the bucket cannot close
    // asks for a rebuild on every append it refuses, and the answer is almost
    // always that nothing moved. Rebuilding to discover that reads the footer
    // and key index of every segment, so one behind replica saturates itself
    // and the object store, and it does not stop when the writes do.
    //
    // Asserting on reads rather than on the horizon, because a hydrate that
    // rebuilt and one that did not return the same value. The cost is the
    // behaviour.
    #[tokio::test]
    async fn hydrating_against_an_unchanged_manifest_does_not_rebuild_the_index() {
        let (owner, replica, store) = crate::testing::counting_partition_pair().await;
        owner
            .apply(&Mutation::put(Lamport(1), bytes("a"), bytes("v"), None))
            .await
            .unwrap();
        owner.flush().await.unwrap();

        // First hydrate: the manifest is ahead of a partition that has adopted
        // nothing, so a rebuild is the correct, expensive answer.
        store.reset();
        let first = replica.hydrate().await.unwrap();
        assert_eq!(first.through, Lamport(1));
        assert!(
            store.segment_reads() > 0,
            "a manifest genuinely ahead has to be read into the index"
        );

        // Second and third, with nothing republished. This is the state a
        // gapped replica is in on every refused append.
        store.reset();
        for _ in 0..2 {
            assert_eq!(
                replica.hydrate().await.unwrap(),
                first,
                "the horizon is unchanged, which is why the cost is invisible \
                 from the return value"
            );
        }
        assert_eq!(
            store.segment_reads(),
            0,
            "an unchanged manifest must not cost an index rebuild"
        );
        assert_eq!(
            store.manifest_reads(),
            2,
            "one small read each, which is what decides the expensive one is \
             not worth doing"
        );
    }

    #[tokio::test]
    async fn a_flushed_key_read_twice_only_reaches_the_object_store_once() {
        // The whole point of the cache, and the thing two EKS runs measured
        // the absence of: before this, every read of a flushed key was an
        // object-store round trip, and no read on a benchmarked cluster ever
        // came back faster than 14.4ms.
        //
        // Asserted on reads rather than on latency or on the value, because a
        // cached read and an uncached one return the same bytes. The cost is
        // the behaviour.
        let (owner, _replica, store) = crate::testing::counting_partition_pair().await;
        let owner = owner.with_value_cache(Arc::new(ValueCache::new(1 << 20)));
        owner
            .apply(&Mutation::put(Lamport(1), bytes("a"), bytes("v"), None))
            .await
            .unwrap();
        // Flushed, so the memtable no longer shadows the segment. Until it is,
        // a read never reaches the object store and proves nothing.
        owner.flush().await.unwrap();

        store.reset();
        assert_eq!(
            owner.get(b"a").await.unwrap().map(|r| r.value),
            Some(bytes("v"))
        );
        assert!(
            store.segment_reads() > 0,
            "the first read of a flushed key has to fetch it"
        );

        store.reset();
        for _ in 0..8 {
            assert_eq!(
                owner.get(b"a").await.unwrap().map(|r| r.value),
                Some(bytes("v")),
                "a cached read answers with the same record"
            );
        }
        assert_eq!(
            store.segment_reads(),
            0,
            "every read after the first must be served from memory"
        );
    }

    #[tokio::test]
    async fn a_write_is_not_answered_from_a_cached_copy_of_what_it_replaced() {
        // The safety property that makes an uninvalidated cache sound. Nothing
        // tells the cache a write happened, so what protects a reader is the
        // order of the read path: a write lands in the memtable, the memtable
        // is consulted first, and the stale entry underneath is simply never
        // reached.
        let (owner, _replica, store) = crate::testing::counting_partition_pair().await;
        let owner = owner.with_value_cache(Arc::new(ValueCache::new(1 << 20)));
        owner
            .apply(&Mutation::put(Lamport(1), bytes("a"), bytes("old"), None))
            .await
            .unwrap();
        owner.flush().await.unwrap();

        // Warm the cache with the flushed value, so there is something stale
        // to serve if the ordering is ever wrong.
        assert_eq!(
            owner.get(b"a").await.unwrap().map(|r| r.value),
            Some(bytes("old"))
        );

        owner
            .apply(&Mutation::put(Lamport(2), bytes("a"), bytes("new"), None))
            .await
            .unwrap();
        assert_eq!(
            owner.get(b"a").await.unwrap().map(|r| r.value),
            Some(bytes("new")),
            "the memtable shadows the cached record"
        );

        // And after the flush publishes the new record at a new offset, the
        // old cached entry is unreachable rather than merely shadowed.
        owner.flush().await.unwrap();
        store.reset();
        assert_eq!(
            owner.get(b"a").await.unwrap().map(|r| r.value),
            Some(bytes("new")),
            "the new record is read from its own offset"
        );
    }

    #[tokio::test]
    async fn a_partition_without_a_cache_still_serves_reads() {
        // The default. A cache is memory the node owns and decides about, so a
        // partition opened without one has to keep working rather than fail
        // closed or quietly hold an unbounded one.
        let (owner, _replica, store) = crate::testing::counting_partition_pair().await;
        owner
            .apply(&Mutation::put(Lamport(1), bytes("a"), bytes("v"), None))
            .await
            .unwrap();
        owner.flush().await.unwrap();

        store.reset();
        for _ in 0..3 {
            assert_eq!(
                owner.get(b"a").await.unwrap().map(|r| r.value),
                Some(bytes("v"))
            );
        }
        assert!(
            store.segment_reads() >= 3,
            "with no cache every read fetches, which is what it did before"
        );
    }

    #[tokio::test]
    async fn one_miss_warms_the_records_next_to_it() {
        // The point of read-ahead. A miss costs a round trip whatever it brings
        // back — about 14ms against S3 — so bringing back one record spends the
        // expensive part of the operation on the cheapest possible result.
        // Segment records are sorted by key and laid out in that order, so the
        // neighbours are already in the bytes the range request has to cross.
        let (owner, _replica, store) = crate::testing::counting_partition_pair().await;
        let owner = owner
            .with_value_cache(Arc::new(ValueCache::new(1 << 20)))
            .with_read_ahead(64 * 1024);
        for i in 0..16u32 {
            owner
                .apply(&Mutation::put(
                    Lamport(u64::from(i) + 1),
                    bytes(&format!("k{i:03}")),
                    bytes("value"),
                    None,
                ))
                .await
                .unwrap();
        }
        owner.flush().await.unwrap();

        store.reset();
        assert!(owner.get(b"k000").await.unwrap().is_some());
        let first = store.segment_reads();
        assert!(first > 0, "the first read has to fetch");

        // Every other key was in the bytes that fetch already crossed.
        store.reset();
        for i in 1..16u32 {
            assert!(
                owner
                    .get(format!("k{i:03}").as_bytes())
                    .await
                    .unwrap()
                    .is_some(),
                "k{i:03} should have been warmed by the miss on k000"
            );
        }
        assert_eq!(
            store.segment_reads(),
            0,
            "fifteen neighbours came back with the first miss and none of them fetched"
        );
    }

    #[tokio::test]
    async fn read_ahead_does_not_serve_a_neighbour_a_later_write_replaced() {
        // The risk read-ahead introduces. A segment holds shadowed versions as
        // well as winning ones, so the bytes behind a record may be a version
        // some later flush replaced. Warming them blindly would put a stale
        // record in the cache under an offset nothing indexes — harmless on its
        // own — but the read path must still answer with the winning version.
        let (owner, _replica, store) = crate::testing::counting_partition_pair().await;
        let owner = owner
            .with_value_cache(Arc::new(ValueCache::new(1 << 20)))
            .with_read_ahead(64 * 1024);
        for i in 0..8u32 {
            owner
                .apply(&Mutation::put(
                    Lamport(u64::from(i) + 1),
                    bytes(&format!("k{i:03}")),
                    bytes("old"),
                    None,
                ))
                .await
                .unwrap();
        }
        owner.flush().await.unwrap();

        // A second segment, shadowing one of the keys in the first.
        owner
            .apply(&Mutation::put(
                Lamport(100),
                bytes("k004"),
                bytes("new"),
                None,
            ))
            .await
            .unwrap();
        owner.flush().await.unwrap();

        store.reset();
        // Missing on k000 drags the whole first segment's data through,
        // including the superseded copy of k004.
        assert!(owner.get(b"k000").await.unwrap().is_some());

        assert_eq!(
            owner.get(b"k004").await.unwrap().map(|r| r.value),
            Some(bytes("new")),
            "the winning version, not the one read-ahead happened to cross"
        );
        assert_eq!(
            owner.get(b"k003").await.unwrap().map(|r| r.value),
            Some(bytes("old")),
            "a neighbour nothing replaced is still correct"
        );
    }

    #[tokio::test]
    async fn read_ahead_off_fetches_exactly_one_record() {
        // The default, and the behaviour every release before this had. Worth a
        // test because the strict length check that catches an index disagreeing
        // with its segment only applies when the window is one record, and it
        // would be easy to lose it while making room for a window that is not.
        let (owner, _replica, store) = crate::testing::counting_partition_pair().await;
        let owner = owner.with_value_cache(Arc::new(ValueCache::new(1 << 20)));
        for i in 0..8u32 {
            owner
                .apply(&Mutation::put(
                    Lamport(u64::from(i) + 1),
                    bytes(&format!("k{i:03}")),
                    bytes("value"),
                    None,
                ))
                .await
                .unwrap();
        }
        owner.flush().await.unwrap();

        store.reset();
        assert!(owner.get(b"k000").await.unwrap().is_some());
        store.reset();
        assert!(owner.get(b"k001").await.unwrap().is_some());
        assert!(
            store.segment_reads() > 0,
            "with read-ahead off a neighbour is still a fetch"
        );
    }

    fn loc(segment: usize, offset: u64, len: u32) -> Loc {
        Loc {
            segment,
            offset,
            record_length: len,
        }
    }

    #[test]
    fn planning_a_window_bounds_the_index_it_walks() {
        // The cost this bounds is a scan, not a fetch, so the window it
        // returns is the same either way: a sparse segment plans one record
        // whether the walk stopped early or ran to the end of the partition.
        // Only the work differs, so only the work can be asserted.
        //
        // Without the bound, one cold read of an old segment costs a pass over
        // every later key in the partition, under the read lock, once per miss.
        let mut index: BTreeMap<Bytes, Loc> = BTreeMap::new();
        index.insert(bytes("k00000"), loc(0, 0, 64));
        for i in 0..(MAX_READ_AHEAD_SCAN * 4) {
            index.insert(bytes(&format!("k{:05}", i + 1)), loc(1, i as u64 * 64, 64));
        }

        let window = plan_read_ahead(&index, loc(0, 0, 64), b"k00000", 1 << 20);

        assert!(
            window.scanned <= MAX_READ_AHEAD_SCAN,
            "walked {} entries of a {} entry index",
            window.scanned,
            index.len()
        );
        assert_eq!(
            window.followers, 0,
            "there is nothing else in this segment to warm"
        );
        assert_eq!(window.end, 64, "so the window is the one record asked for");
    }

    #[test]
    fn planning_a_window_still_reaches_across_records_another_segment_interleaves() {
        // The reason the walk skips rather than stops. Two segments whose keys
        // alternate are the ordinary result of a flush, and a plan that ended
        // at the first foreign key would read ahead by nothing at all.
        let mut index: BTreeMap<Bytes, Loc> = BTreeMap::new();
        for i in 0..16u64 {
            let (segment, offset) = if i % 2 == 0 {
                (0, i / 2 * 64)
            } else {
                (1, i / 2 * 64)
            };
            index.insert(bytes(&format!("k{i:05}")), loc(segment, offset, 64));
        }

        let window = plan_read_ahead(&index, loc(0, 0, 64), b"k00000", 1 << 20);

        assert_eq!(
            window.followers, 7,
            "every later record in this segment, none of the other's"
        );
        assert_eq!(window.end, 8 * 64);
    }

    #[test]
    fn planning_a_window_stops_at_the_budget() {
        let mut index: BTreeMap<Bytes, Loc> = BTreeMap::new();
        for i in 0..64u64 {
            index.insert(bytes(&format!("k{i:05}")), loc(0, i * 64, 64));
        }

        let window = plan_read_ahead(&index, loc(0, 0, 64), b"k00000", 4 * 64);

        assert_eq!(window.end - window.start, 4 * 64);
        assert_eq!(window.followers, 3);
    }

    #[test]
    fn a_record_a_later_segment_superseded_is_not_worth_cache_budget() {
        // Offsets repeat across segments, because every segment holds a record
        // just after its header. So the copy of a key sitting at offset 32 of
        // an old segment and the winning copy at offset 32 of a newer one are
        // different records at the same number, and only one of them is worth
        // holding.
        //
        // Admitting the dead one serves nothing — the cache is keyed by object
        // and no index entry resolves to it — but it spends budget a live
        // record needs, which is a cache that is too small wearing a disguise.
        let mut index: BTreeMap<Bytes, Loc> = BTreeMap::new();
        index.insert(bytes("rewritten"), loc(1, 32, 64));
        index.insert(bytes("untouched"), loc(0, 96, 64));

        assert!(
            !is_winning_record(&index, b"rewritten", 0, 32),
            "the superseded copy shares an offset with its winner and is still dead"
        );
        assert!(
            is_winning_record(&index, b"rewritten", 1, 32),
            "the winning copy is admitted"
        );
        assert!(
            is_winning_record(&index, b"untouched", 0, 96),
            "a key no later segment touched is admitted from where it lives"
        );
        assert!(
            !is_winning_record(&index, b"untouched", 0, 32),
            "and not from an offset the index does not name"
        );
        assert!(
            !is_winning_record(&index, b"absent", 0, 32),
            "a key compaction dropped entirely is not worth holding"
        );
    }
}
