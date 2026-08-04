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
//! versions. It runs when the segment count crosses a threshold and on
//! explicit request, and it republishes through the same manifest swap as a
//! flush, so a compaction that dies half way costs objects rather than data.
//! The objects it replaces are deleted once the new manifest is committed;
//! anything a crash strands is left for a sweep, which does not exist yet and
//! is deliberate scope for later, as is a cache for values read back out of
//! segments.

use crate::cursor::Cursor;
use crate::mutation::{version_at, Mutation, MutationOp};

use bytes::Bytes;
use orbita_core::{
    Epoch, Error, KeyRange, Lamport, Record, Result, Version, WriteCondition, MAX_KEY_BYTES,
    MAX_LIST_LIMIT, MAX_VALUE_BYTES,
};
use orbita_format::segment::{Segment, SegmentBuilder};
use orbita_format::{
    compact, CommitPlan, FormatError, PartitionPath, PartitionWriter, RecordValue, SegmentEntry,
    SegmentRecord, Snapshot,
};
use orbita_objectstore::{ObjectError, ObjectStore};
use orbita_runtime::{Clock, Runtime};
use std::collections::BTreeMap;
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

/// How many segments may accumulate before a flush triggers a compaction.
///
/// Reads never pay for segment count, so this is purely about space: every
/// overwrite strands a shadowed record until a merge reclaims it. Sixteen
/// flush-sized segments bound that waste at roughly the cost of one merge per
/// sixteen flushes.
const COMPACT_TRIGGER_SEGMENTS: usize = 16;

/// The bookkeeping cost charged to the flush trigger per entry, on top of the
/// key and value bytes. An estimate is all a trigger needs.
const ENTRY_OVERHEAD_BYTES: u64 = 64;

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

/// Where a flushed key's winning record sits.
#[derive(Debug, Clone, Copy)]
struct Loc {
    /// A position in [`State::segments`], which moves wholesale on compaction
    /// and only ever grows on flush, so a held position never dangles.
    segment: usize,
    offset: u64,
    record_length: u32,
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
    /// The published segments, in manifest order.
    segments: Vec<SegmentEntry>,
    /// Every flushed key and where its winning record lives.
    index: BTreeMap<Bytes, Loc>,
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
    /// One lock rather than a lock per key is deliberate: a partition already
    /// has exactly one writer in production, because the owning worker
    /// serializes writes before they reach here, so striping would buy
    /// contention we do not have in exchange for a class of bugs we would
    /// rather not reason about. Reads share it.
    state: tokio::sync::RwLock<State>,
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
            segments: Vec::new(),
            index: BTreeMap::new(),
        };
        if let Some(snapshot) = Snapshot::open(Arc::clone(&store), path.clone())
            .await
            .map_err(format_error)?
        {
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
            state.flushed = snapshot.committed_lamport();
            state.committed = state.flushed;
            state.segments = snapshot.manifest().segments.clone();
        }

        Ok(Self {
            runtime,
            store,
            path,
            range,
            writer,
            state: tokio::sync::RwLock::new(state),
        })
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
        self.commit(&mut state, lamport, key, entry).await?;
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
        self.commit(&mut state, lamport, key, entry).await?;
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
    pub async fn scan(&self, prefix: &[u8], cursor: Option<&[u8]>, limit: u32) -> Result<ScanPage> {
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
        self.commit(&mut state, mutation.lamport, &mutation.key, entry)
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

    /// The partition's size, which is what the leader splits on.
    ///
    /// This is the published segments plus an estimate of the mutable table,
    /// not an exact figure. A split threshold does not need one, and shadowed
    /// records inflate it only until a compaction runs.
    pub async fn size_bytes(&self) -> Result<u64> {
        let state = self.state.read().await;
        Ok(state.memtable_bytes + state.segments.iter().map(|s| s.bytes).sum::<u64>())
    }

    /// Flushes the mutable table into a segment and publishes it.
    ///
    /// A no-op when the table is empty. This is the explicit trigger ADR 0006
    /// names; the size trigger calls the same path from inside a write.
    pub async fn flush(&self) -> Result<()> {
        let mut state = self.state.write().await;
        self.flush_locked(&mut state).await
    }

    /// Merges every segment into one, which is what physically reclaims
    /// expired records, aged tombstones, and shadowed versions.
    ///
    /// Reclamation otherwise waits for the segment-count trigger, and the
    /// product promises "eventually" rather than a bound. This exists so an
    /// operator, or a test, can ask for the sweep now. The mutable table is
    /// flushed first so the merge sees everything.
    pub async fn compact(&self) -> Result<()> {
        let mut state = self.state.write().await;
        self.flush_locked(&mut state).await?;
        self.compact_locked(&mut state).await
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
    /// index promised.
    async fn fetch(&self, state: &State, loc: Loc, key: &[u8]) -> Result<Stored> {
        let name = &state.segments[loc.segment].name;
        let raw = self
            .store
            .get_range(
                &self.path.object(name),
                loc.offset..loc.offset + u64::from(loc.record_length),
            )
            .await
            .map_err(store_error)?;
        let (record, consumed) = SegmentRecord::decode(&raw).map_err(format_error)?;
        if consumed != raw.len() || record.key != key {
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
                let (bytes, _) = self
                    .store
                    .get(&self.path.object(&external.name))
                    .await
                    .map_err(store_error)?;
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
        Ok(Stored {
            version: Version(record.lamport.get()),
            expires_at_millis: record.expires_at_millis,
            deleted,
            value,
        })
    }

    /// Commits one entry to the mutable table and advances the Lamport, then
    /// flushes if the table has crossed the trigger.
    async fn commit(
        &self,
        state: &mut State,
        lamport: Lamport,
        key: &[u8],
        entry: Stored,
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
        if state.memtable_bytes >= FLUSH_TRIGGER_BYTES {
            if let Err(error) = self.flush_locked(state).await {
                tracing::warn!(
                    %error,
                    memtable_bytes = state.memtable_bytes,
                    "flush failed; writes stay in the mutable table until the next trigger"
                );
            }
        }
        Ok(())
    }

    async fn flush_locked(&self, state: &mut State) -> Result<()> {
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
            state.index.insert(
                entry.key.clone(),
                Loc {
                    segment: position,
                    offset: entry.offset,
                    record_length: entry.record_length,
                },
            );
        }
        state.segments = manifest.segments;
        state.memtable.clear();
        state.memtable_bytes = 0;
        state.flushed = committed;

        // The flush's own promise, meaning the horizon advance, has already
        // held by here; compaction is space reclamation on top of it. A
        // failed compaction therefore does not fail the flush: the segments
        // stay as they are and the next flush crosses the threshold again.
        if state.segments.len() >= COMPACT_TRIGGER_SEGMENTS {
            if let Err(error) = self.compact_locked(state).await {
                tracing::warn!(
                    %error,
                    segments = state.segments.len(),
                    "compaction failed; the segments stand until the next trigger"
                );
            }
        }
        Ok(())
    }

    async fn compact_locked(&self, state: &mut State) -> Result<()> {
        if state.segments.is_empty() {
            return Ok(());
        }
        let now = self.now_millis();

        // This materializes the whole partition in memory and runs under the
        // write lock, possibly from inside the client write that crossed the
        // flush trigger. With the current constants that is a bounded but
        // real stall, and it grows with partition size until the split
        // threshold caps it. The crate's no-background-tasks posture is
        // deliberate, so a streaming merge, or handing the schedule to the
        // host, is the known follow-up rather than an accident.

        let mut inputs = Vec::with_capacity(state.segments.len());
        for entry in &state.segments {
            let (bytes, _) = self
                .store
                .get(&self.path.object(&entry.name))
                .await
                .map_err(store_error)?;
            let segment = Segment::decode(&bytes).map_err(format_error)?;
            inputs.push(segment.records().to_vec());
        }
        // A whole-partition merge, whose tombstone rule is time-based: an
        // unexpired tombstone still answers a retrying deleter, so it
        // survives until its retention passes and the expiry rule reclaims
        // it on schedule.
        let merged = compact::merge_all(&inputs, now).map_err(format_error)?;

        let replaced: Vec<String> = state.segments.iter().map(|e| e.name.clone()).collect();
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
                        segment: 0,
                        offset: entry.offset,
                        record_length: entry.record_length,
                    },
                );
            }
            (vec![published], index)
        };

        // Compaction republishes the same logical state, so the horizon must
        // not move: the log above it still has to replay after a crash.
        let flushed = state.flushed;
        let range = self.range.clone();
        let plan_segments = segments.clone();
        let manifest = self
            .writer
            .commit(|_| CommitPlan {
                committed_lamport: flushed,
                range: range.clone(),
                segments: plan_segments.clone(),
            })
            .await
            .map_err(format_error)?;
        state.segments = manifest.segments;
        state.index = index;

        // The replaced objects are unreferenced the moment the manifest
        // swapped, and this is the only writer, so deleting them now is safe.
        // An external reader hydrating from the old manifest fails its read
        // and retries against the new one, which the format documents as the
        // deal. Failures here strand objects for a future sweep rather than
        // failing the compaction that already happened.
        for name in replaced {
            let _ = self.store.delete(&self.path.object(&name)).await;
        }
        Ok(())
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

/// What one entry charges against the flush trigger.
fn entry_cost(key: &Bytes, entry: &Stored) -> u64 {
    key.len() as u64 + entry.value.len() as u64 + ENTRY_OVERHEAD_BYTES
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
    use crate::testing::{owner, owner_in_range, owner_with_clock, partition_with_clock, reopened};

    fn bytes(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
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

        let page = p.scan(b"k", None, 100).await.unwrap();
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
        let page = p.scan(b"", None, 10).await.unwrap();
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

        let page = p.scan(b"a/", None, 10).await.unwrap();
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
            let page = p.scan(b"k", cursor.as_deref(), 4).await.unwrap();
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

        let first = p.scan(b"k", None, 5).await.unwrap();
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
            let page = p.scan(b"k", Some(&c), 5).await.unwrap();
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

        let page = p.scan(b"k", None, 10).await.unwrap();
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
        let page = p.scan(b"", Some(&stale), 10).await.unwrap();
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
        let page = p.scan(b"", Some(&stale), 10).await.unwrap();
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

        let page = p.scan(b"", None, 10).await.unwrap();
        assert_eq!(page.entries.len(), 1);
    }

    #[tokio::test]
    async fn a_scan_limit_outside_the_allowed_range_is_rejected() {
        let p = owner().await;
        assert!(matches!(
            p.scan(b"", None, 0).await,
            Err(Error::InvalidArgument(_))
        ));
        assert!(matches!(
            p.scan(b"", None, MAX_LIST_LIMIT + 1).await,
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
        let page = p.scan(b"", None, 10).await.unwrap();
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
}
