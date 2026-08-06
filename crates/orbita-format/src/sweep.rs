//! Finding the objects nobody will ever read.
//!
//! Two things make objects unreferenced. A commit drops one, as when compaction
//! replaces a segment. Or a commit writes one and never reaches its manifest
//! swap, which is what an interrupted or abandoned commit leaves behind. The
//! second is why the sweep is required rather than an optimisation: without it
//! a partition accumulates objects forever.
//!
//! # Finding them is pure; deleting them respects a grace period
//!
//! [`Referenced`] and [`unreferenced`] answer only the question the objects
//! themselves can answer: which of them the current manifest no longer reaches.
//! That answer is not enough to delete on. Deleting an unreferenced object is
//! safe only after a grace period, and the grace period must exceed both the
//! longest read and the longest commit. A reader part way through a snapshot
//! taken against an earlier manifest will request objects that manifest named,
//! and steps 1 and 2 of a commit write objects that nothing references until
//! step 6, so a sweeper that considered only read duration would delete objects
//! an in-flight commit is about to publish.
//!
//! [`sweep_partition`] is the wired sweep that #100 adds on top of those pure
//! functions: it lists a partition, subtracts the referenced set, and deletes
//! only what is both unreferenced *and* provably old enough. The grace period
//! is a *configured* bound the caller passes in rather than a constant guessed
//! here, because "longer than the longest read and the longest commit" is a
//! property of a deployment, not of this format.
//!
//! Judging age needs to know when an object was written, which
//! [`ObjectMeta::last_modified`](orbita_objectstore::ObjectMeta::last_modified)
//! carries. That write time is in the *backend's* clock domain, not the host
//! clock's, and it is an `Option`. The sweep must not compare it against
//! `orbita_runtime::Clock::now_millis()`; it judges age with
//! [`ObjectMeta::is_safely_older_than`](orbita_objectstore::ObjectMeta::is_safely_older_than),
//! which takes a reference "now" from the same backend domain, folds in a skew
//! allowance, and treats a `None` write time as "do not touch". A backend that
//! cannot report a write time reports `None` rather than a zero, because zero
//! reads as the epoch, and treating an unknown time as ancient deletes live
//! data. See the field's own contract.
//!
//! # Grace runs from when an object became garbage, not from when it was born
//!
//! An object's *creation* time is the wrong thing to age against. A segment
//! that was live for months and was dropped a second ago by a fresh compaction
//! is ancient by creation time, but a reader that opened a snapshot against the
//! manifest just replaced can still be part way through fetching it. The clock
//! the grace has to outlast starts when the *dropping* manifest was published —
//! the moment the object stopped being reachable — because that is when the
//! last reader that can still want it began to drain.
//!
//! [`sweep_partition`] enforces that with two independent waits, and an object
//! must clear both. The manifest that no longer references the object must
//! itself have been in effect longer than the grace period: while it has not,
//! a reader on an earlier manifest may still hold the object. And the object
//! must be older than the grace period in its own right: an abandoned commit
//! writes an object before it swaps the manifest that would name it, so a very
//! recent object may be one an in-flight commit is about to make live. The
//! grace period exceeds the longest read *and* the longest commit precisely so
//! one bound covers both.
//!
//! # Reading a "now" in the backend's clock domain
//!
//! Judging how long the manifest has been settled needs a *current* time in the
//! backend's domain, and the object store exposes no clock: the only times it
//! reports are the ones it stamped on writes. So the sweep writes a tiny marker
//! and reads back the time the store stamped it, then removes it. The marker is
//! never a partition object and nothing a reader consults will list it, so its
//! brief existence changes nothing. A store that cannot stamp a time reports
//! `None`, and the sweep then does nothing rather than guess an age.

use crate::commit::load_manifest;
use crate::error::Result;
use crate::manifest::Manifest;
use crate::paths::{self, PartitionPath};
use crate::record::{RecordValue, SegmentRecord};
use crate::segment::Segment;

use bytes::Bytes;
use orbita_objectstore::{BackendTime, ObjectMeta, ObjectStore};
use std::collections::{BTreeSet, HashMap};

/// The relative name of the marker the sweep writes to read the object store's
/// clock. It is neither a segment nor a value name, so [`paths::parse_object_name`]
/// rejects it and the sweep never treats it as a candidate, and it sits directly
/// under the partition prefix so a listing of that prefix finds it.
const CLOCK_PROBE_NAME: &str = "sweep-clock-probe";

/// Everything the current manifest reaches, by name relative to the partition
/// directory.
///
/// Segments come from the manifest itself. Value objects do not: they are
/// reached through the records inside live segments, so a caller has to walk
/// those segments before it knows which values are still referenced. Sweeping
/// values off the manifest alone would delete every large value in the
/// partition.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Referenced {
    names: BTreeSet<String>,
}

impl Referenced {
    #[must_use]
    pub fn of(manifest: &Manifest) -> Self {
        Self {
            names: manifest.segments.iter().map(|s| s.name.clone()).collect(),
        }
    }

    /// Adds the external values one live segment's records point at.
    pub fn add_records(&mut self, records: &[SegmentRecord]) {
        for record in records {
            if let RecordValue::External(external) = &record.value {
                self.names.insert(external.name.clone());
            }
        }
    }

    #[must_use]
    pub fn contains(&self, relative_name: &str) -> bool {
        self.names.contains(relative_name)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.names.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }
}

/// The objects in `listing` that `referenced` does not reach, as full store
/// keys.
///
/// Only objects this format writes are considered. Anything else under the
/// prefix belongs to somebody else and a sweeper has no business deleting it,
/// which is the reason object names are parsed strictly rather than by
/// stripping a suffix.
#[must_use]
pub fn unreferenced(
    referenced: &Referenced,
    path: &PartitionPath,
    listing: &[ObjectMeta],
) -> Vec<String> {
    listing
        .iter()
        .filter(|object| {
            path.relative(&object.key).is_some_and(|relative| {
                paths::parse_object_name(relative).is_some() && !referenced.contains(relative)
            })
        })
        .map(|object| object.key.clone())
        .collect()
}

/// What one sweep of a partition did, or would have done.
///
/// Reported rather than logged-and-forgotten so the caller can surface it: an
/// operator running the sweep for the first time wants to see the set before it
/// trusts the sweep with a real bucket, which is what [`dry_run`](Self::dry_run)
/// is for.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepReport {
    /// True when nothing was deleted and [`deleted`](Self::deleted) lists what a
    /// real run would have removed. This is the "look before it acts" mode.
    pub dry_run: bool,
    /// The objects deleted, or, under `dry_run`, the objects a real run would
    /// delete: unreferenced and provably older than the grace period in the
    /// backend's clock domain.
    pub deleted: Vec<String>,
    /// Unreferenced objects left in place because they are not yet safely older
    /// than the grace period, or carry no write time to judge at all. Held back
    /// is the safe direction: a young orphan is collected on a later pass, but a
    /// premature delete cannot be undone.
    pub retained: Vec<String>,
}

/// Deletes the objects a partition no longer references and is safely done
/// needing, and leaves everything else alone.
///
/// This is the whole point of the module: without it, every object a failed
/// compaction or an abandoned commit strands accumulates forever. It composes
/// the pure [`Referenced`]/[`unreferenced`] pair with two age gates, both built
/// on [`ObjectMeta::is_safely_older_than`]. An unreferenced object is deleted
/// only when both hold:
///
/// - **The dropping manifest has settled.** The current manifest — the one that
///   does not reference the object — must itself have been in effect for longer
///   than the grace period. Until it has, a reader that opened a snapshot
///   against an earlier manifest may still fetch the object. This is what makes
///   a months-old segment just dropped by a fresh compaction *wait* rather than
///   be deleted the instant it is orphaned.
/// - **The object itself is old enough.** An abandoned commit writes an object
///   before it swaps the manifest that would name it, so a very recent object
///   may be one an in-flight commit is about to make live. The object must be
///   older than the grace period in its own right to rule that out.
///
/// `grace_millis` is a configured bound, not a constant: it must exceed both the
/// longest read and the longest commit a deployment allows, which this crate has
/// no way to know. `max_skew_millis` widens it by the backend's worst-case
/// internal clock skew. Both flow straight into
/// [`ObjectMeta::is_safely_older_than`], so a sweep that goes through here cannot
/// mix clock domains or forget skew.
///
/// With `dry_run` set, no candidate is deleted and the report names what a real
/// run would remove. That is the first-run affordance: a real bucket is looked
/// at before it is acted on. (The clock probe below is still written and removed
/// in dry-run, because a candidate list is meaningless without a current time to
/// judge the manifest against; it is never a partition object and deletes
/// nothing a reader can see.)
///
/// # Fail-closed corners
///
/// - A partition with no manifest has published nothing, so there is no
///   referenced set to subtract against. The sweep does nothing rather than
///   guess.
/// - When the store cannot stamp a write time — so the clock probe or the
///   manifest reports `None` — there is no way to establish how long the
///   manifest has been settled, so every candidate is retained.
/// - An object whose own write time is `None` is never a candidate, because
///   [`ObjectMeta::is_safely_older_than`] refuses it.
pub async fn sweep_partition<S: ObjectStore + ?Sized>(
    store: &S,
    path: &PartitionPath,
    grace_millis: u64,
    max_skew_millis: u64,
    dry_run: bool,
) -> Result<SweepReport> {
    // A time in the object store's own clock domain, read now. The grace runs
    // from when the dropping manifest was published, and measuring how long ago
    // that was needs a current time, not the manifest's own stamp. See the
    // module docs.
    let reference_now = probe_backend_now(store, path).await?;

    let Some((manifest, _)) = load_manifest(store, path).await? else {
        // Nothing published: no referenced set to subtract against.
        return Ok(SweepReport {
            dry_run,
            ..Default::default()
        });
    };

    let mut referenced = Referenced::of(&manifest);
    let listing = store.list(path.prefix()).await?;

    let manifest_key = path.manifest();
    let manifest_meta = listing.iter().find(|object| object.key == manifest_key);

    // External values are reached through the records inside live segments, not
    // through the manifest, so a value in use would look unreferenced from the
    // manifest alone. Walk the live segments to protect them — but only when the
    // partition actually holds value objects, so the common case (this engine
    // writes every value inline) never pays to read a segment it has no orphan
    // value to protect.
    let values_prefix = path.values_prefix();
    let holds_values = listing
        .iter()
        .any(|object| object.key.starts_with(&values_prefix));
    if holds_values {
        for entry in &manifest.segments {
            let (bytes, _) = store.get(&path.object(&entry.name)).await?;
            referenced.add_records(Segment::decode(&bytes)?.records());
        }
    }

    let candidates = unreferenced(&referenced, path, &listing);

    // Fail closed if the backend cannot give a current time or a manifest time:
    // without both, "the manifest has been settled for a grace period" cannot be
    // established, so nothing is safe to delete.
    let (Some(reference_now), Some(manifest_meta)) = (reference_now, manifest_meta) else {
        return Ok(SweepReport {
            dry_run,
            deleted: Vec::new(),
            retained: candidates,
        });
    };

    // The manifest that dropped these objects must have been in effect longer
    // than the grace period, or a reader on an earlier manifest could still hold
    // one of them. This is a property of the manifest, not of any one object, so
    // it is evaluated once: while it does not hold, no candidate is safe.
    let manifest_settled =
        manifest_meta.is_safely_older_than(reference_now, grace_millis, max_skew_millis);

    let meta_by_key: HashMap<&str, &ObjectMeta> = listing
        .iter()
        .map(|object| (object.key.as_str(), object))
        .collect();

    let mut report = SweepReport {
        dry_run,
        deleted: Vec::new(),
        retained: Vec::new(),
    };
    for key in candidates {
        // Both waits: the manifest has settled (covers a reader on the prior
        // manifest) and the object itself is old enough (covers an in-flight
        // commit that wrote it and has not yet swapped the manifest to name it).
        let old_enough = manifest_settled
            && meta_by_key.get(key.as_str()).is_some_and(|object| {
                object.is_safely_older_than(reference_now, grace_millis, max_skew_millis)
            });
        if old_enough {
            if !dry_run {
                store.delete(&key).await?;
            }
            report.deleted.push(key);
        } else {
            report.retained.push(key);
        }
    }
    Ok(report)
}

/// Reads a timestamp in the object store's own clock domain.
///
/// The [`ObjectStore`] trait exposes no "what time is it"; the only times it
/// reports are the ones it stamped on writes. So the sweep writes a marker and
/// reads the time back off it, then removes it. The marker
/// ([`CLOCK_PROBE_NAME`]) is never a partition object and nothing a reader
/// consults will list it, so its brief existence changes nothing a reader can
/// see. A store that cannot stamp a time reports `None`, which the caller treats
/// as "do not act".
async fn probe_backend_now<S: ObjectStore + ?Sized>(
    store: &S,
    path: &PartitionPath,
) -> Result<Option<BackendTime>> {
    let key = path.object(CLOCK_PROBE_NAME);
    store
        .put(&key, Bytes::from_static(b"orbita-sweep-clock-probe"))
        .await?;
    let meta = store.head(&key).await?;
    // Best effort: a probe left behind is harmless — nothing references it and
    // it is never a deletion candidate — and failing the sweep because cleanup
    // failed would be the worse outcome.
    let _ = store.delete(&key).await;
    Ok(meta.last_modified)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::{CommitPlan, PartitionWriter};
    use crate::manifest::SegmentEntry;
    use crate::record::ExternalValue;
    use crate::segment::{BuiltSegment, SegmentBuilder};
    use crate::testing::MemoryStore;

    use bytes::Bytes;
    use orbita_core::{Epoch, KeyRange, KeyspaceId, Lamport, PartitionId};
    use orbita_objectstore::{BackendTime, ETag};
    use orbita_runtime::Clock;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    fn path() -> PartitionPath {
        PartitionPath::new("orbita", KeyspaceId(1), PartitionId(7))
    }

    fn object(relative: &str) -> ObjectMeta {
        ObjectMeta {
            key: path().object(relative),
            size: 1024,
            etag: ETag("etag".to_string()),
            last_modified: Some(BackendTime(1_000)),
        }
    }

    fn manifest(segments: Vec<String>) -> Manifest {
        Manifest {
            keyspace_id: KeyspaceId(1),
            partition_id: PartitionId(7),
            epoch: Epoch(1),
            committed_lamport: Lamport(10),
            range: KeyRange::unbounded(),
            segments: segments
                .into_iter()
                .map(|name| SegmentEntry {
                    name,
                    bytes: 1024,
                    record_count: 1,
                    min_key: Bytes::from_static(b"a"),
                    max_key: Bytes::from_static(b"z"),
                    min_lamport: Lamport(1),
                    max_lamport: Lamport(10),
                })
                .collect(),
        }
    }

    #[test]
    fn a_segment_the_manifest_dropped_is_unreferenced() {
        let live = paths::segment_name(Epoch(1), 1);
        let dropped = paths::segment_name(Epoch(1), 0);
        let referenced = Referenced::of(&manifest(vec![live.clone()]));

        assert_eq!(
            unreferenced(&referenced, &path(), &[object(&live), object(&dropped)]),
            vec![path().object(&dropped)]
        );
    }

    #[test]
    fn a_value_reached_through_a_live_segment_is_not_swept() {
        let segment = paths::segment_name(Epoch(1), 0);
        let live_value = paths::value_name(Epoch(1), 1);
        let orphan_value = paths::value_name(Epoch(1), 2);

        let mut referenced = Referenced::of(&manifest(vec![segment.clone()]));
        referenced.add_records(&[SegmentRecord {
            key: Bytes::from_static(b"a"),
            lamport: Lamport(1),
            expires_at_millis: None,
            value: RecordValue::External(ExternalValue {
                name: live_value.clone(),
                length: 1,
                crc32c: 0,
            }),
        }]);

        assert_eq!(
            unreferenced(
                &referenced,
                &path(),
                &[object(&segment), object(&live_value), object(&orphan_value)]
            ),
            vec![path().object(&orphan_value)],
            "values are reached through records, not through the manifest"
        );
    }

    #[test]
    fn objects_this_format_did_not_write_are_left_alone() {
        let referenced = Referenced::of(&manifest(vec![]));
        let listing = [
            object("manifest.json"),
            object("segments/notes.txt"),
            ObjectMeta {
                key: "somewhere/else.oseg".to_string(),
                size: 1,
                etag: ETag("etag".to_string()),
                last_modified: Some(BackendTime(1_000)),
            },
        ];
        assert!(unreferenced(&referenced, &path(), &listing).is_empty());
    }

    /// A clock a test drives by hand, so the write times objects carry and the
    /// grace period judged against them advance together under one domain. This
    /// stands in for the backend's own clock, the way the simulated store does.
    #[derive(Clone, Default)]
    struct HandClock {
        millis: Arc<AtomicU64>,
    }

    impl HandClock {
        fn set(&self, millis: u64) {
            self.millis.store(millis, Ordering::SeqCst);
        }
    }

    impl Clock for HandClock {
        fn now_millis(&self) -> u64 {
            self.millis.load(Ordering::SeqCst)
        }

        fn monotonic_nanos(&self) -> u64 {
            self.now_millis().saturating_mul(1_000_000)
        }

        async fn sleep(&self, _duration: std::time::Duration) {}
    }

    fn built_segment(key: &str, lamport: u64) -> BuiltSegment {
        let mut builder = SegmentBuilder::new(KeyspaceId(1), PartitionId(7), Epoch(1));
        builder
            .push(&SegmentRecord {
                key: Bytes::copy_from_slice(key.as_bytes()),
                lamport: Lamport(lamport),
                expires_at_millis: None,
                value: RecordValue::Inline(Bytes::from_static(b"v")),
            })
            .expect("one record is sorted");
        builder.finish().expect("non-empty")
    }

    fn plan(segments: Vec<SegmentEntry>) -> CommitPlan {
        CommitPlan {
            committed_lamport: segments
                .iter()
                .map(|s| s.max_lamport)
                .max()
                .unwrap_or(Lamport::ZERO),
            range: KeyRange::unbounded(),
            segments,
        }
    }

    async fn writer(store: &Arc<MemoryStore>) -> PartitionWriter<MemoryStore> {
        PartitionWriter::open(store.clone(), path(), Epoch(1))
            .await
            .expect("opens")
    }

    #[tokio::test]
    async fn an_object_dropped_by_a_fresh_manifest_is_kept_until_the_manifest_settles() {
        let clock = HandClock::default();
        clock.set(1_000);
        let store = Arc::new(MemoryStore::with_clock(clock.clone()));
        let w = writer(&store).await;

        // A stranded object, a live segment, and the manifest that names only
        // the live one, all at the same instant. The sweep probes the clock at
        // that same instant, so the dropping manifest has been in effect for no
        // time at all, let alone a grace period: a reader on an earlier manifest
        // could still hold the orphan, so it must not go yet.
        let orphan = w.put_segment(&built_segment("a", 1)).await.unwrap();
        let live = w.put_segment(&built_segment("b", 2)).await.unwrap();
        w.commit(|_| plan(vec![live.clone()])).await.unwrap();

        let report = sweep_partition(store.as_ref(), &path(), 5_000, 0, false)
            .await
            .unwrap();

        assert!(
            report.deleted.is_empty(),
            "the dropping manifest has not settled"
        );
        assert_eq!(report.retained, vec![path().object(&orphan.name)]);
        assert!(
            store.keys().contains(&path().object(&orphan.name)),
            "the orphan is still on the store"
        );
    }

    #[tokio::test]
    async fn an_unreferenced_object_past_the_grace_period_is_deleted() {
        let clock = HandClock::default();
        clock.set(1_000);
        let store = Arc::new(MemoryStore::with_clock(clock.clone()));
        let w = writer(&store).await;

        let orphan = w.put_segment(&built_segment("a", 1)).await.unwrap();
        let live = w.put_segment(&built_segment("b", 2)).await.unwrap();
        w.commit(|_| plan(vec![live.clone()])).await.unwrap();

        // The manifest that dropped the orphan has now been in effect longer
        // than the grace period, and the orphan is old in its own right, so it
        // is finally safe to delete.
        clock.set(10_000);
        let report = sweep_partition(store.as_ref(), &path(), 5_000, 0, false)
            .await
            .unwrap();

        assert_eq!(report.deleted, vec![path().object(&orphan.name)]);
        assert!(report.retained.is_empty());
        let keys = store.keys();
        assert!(
            !keys.contains(&path().object(&orphan.name)),
            "the aged orphan is gone"
        );
        assert!(
            keys.contains(&path().object(&live.name)),
            "a referenced segment is never swept"
        );
        assert!(keys.contains(&path().manifest()));
    }

    #[tokio::test]
    async fn a_months_old_object_just_orphaned_waits_a_full_grace_after_the_manifest() {
        // The P1 case: an object that is ancient by creation time but was only
        // just dropped by a fresh manifest must not be deleted until a full
        // grace interval has elapsed since that manifest, because a reader on the
        // prior manifest can still be fetching it. Grace runs from the drop, not
        // from creation.
        let clock = HandClock::default();
        let store = Arc::new(MemoryStore::with_clock(clock.clone()));
        let w = writer(&store).await;

        // Written and published "months" ago.
        clock.set(1_000);
        let old = w.put_segment(&built_segment("a", 1)).await.unwrap();
        w.commit(|_| plan(vec![old.clone()])).await.unwrap();

        // Much later, a fresh compaction replaces it and drops it from the
        // manifest. The old object is now unreferenced but ancient by creation.
        let months_later = 1_000_000_000;
        clock.set(months_later);
        let replacement = w.put_segment(&built_segment("a", 2)).await.unwrap();
        w.commit(|_| plan(vec![replacement.clone()])).await.unwrap();

        // Immediately after the dropping manifest: the object is ancient, but
        // the manifest has not settled, so it must survive.
        let report = sweep_partition(store.as_ref(), &path(), 5_000, 0, false)
            .await
            .unwrap();
        assert!(
            report.deleted.is_empty(),
            "a months-old object just orphaned is not deleted while the manifest is fresh"
        );
        assert_eq!(report.retained, vec![path().object(&old.name)]);
        assert!(store.keys().contains(&path().object(&old.name)));

        // Once the dropping manifest has been in effect for a full grace period,
        // any reader on the prior manifest has drained, and the object goes.
        clock.set(months_later + 5_000);
        let report = sweep_partition(store.as_ref(), &path(), 5_000, 0, false)
            .await
            .unwrap();
        assert_eq!(report.deleted, vec![path().object(&old.name)]);
        assert!(!store.keys().contains(&path().object(&old.name)));
        assert!(store.keys().contains(&path().object(&replacement.name)));
    }

    #[tokio::test]
    async fn a_referenced_object_is_never_a_candidate_however_old() {
        let clock = HandClock::default();
        clock.set(1_000);
        let store = Arc::new(MemoryStore::with_clock(clock.clone()));
        let w = writer(&store).await;

        let live = w.put_segment(&built_segment("a", 1)).await.unwrap();
        w.commit(|_| plan(vec![live.clone()])).await.unwrap();

        // A zero grace and a far-future reference now would delete anything
        // eligible; the live segment is simply not eligible.
        clock.set(1_000_000);
        let report = sweep_partition(store.as_ref(), &path(), 0, 0, false)
            .await
            .unwrap();

        assert!(report.deleted.is_empty());
        assert!(report.retained.is_empty());
        assert!(store.keys().contains(&path().object(&live.name)));
    }

    #[tokio::test]
    async fn an_object_without_a_write_time_is_never_deleted() {
        // A store with no clock reports `None` for every write time, standing in
        // for a backend that cannot report one. With no reference now in the
        // backend's domain, the sweep must refuse to act rather than read the
        // absence as "ancient".
        let store = Arc::new(MemoryStore::new());
        let w = writer(&store).await;

        let orphan = w.put_segment(&built_segment("a", 1)).await.unwrap();
        let live = w.put_segment(&built_segment("b", 2)).await.unwrap();
        w.commit(|_| plan(vec![live.clone()])).await.unwrap();

        let report = sweep_partition(store.as_ref(), &path(), 0, 0, false)
            .await
            .unwrap();

        assert!(report.deleted.is_empty(), "a None time is fail-closed");
        assert_eq!(report.retained, vec![path().object(&orphan.name)]);
        assert!(store.keys().contains(&path().object(&orphan.name)));
    }

    #[tokio::test]
    async fn a_dry_run_reports_the_candidates_and_deletes_nothing() {
        let clock = HandClock::default();
        clock.set(1_000);
        let store = Arc::new(MemoryStore::with_clock(clock.clone()));
        let w = writer(&store).await;

        let orphan = w.put_segment(&built_segment("a", 1)).await.unwrap();
        let live = w.put_segment(&built_segment("b", 2)).await.unwrap();
        w.commit(|_| plan(vec![live.clone()])).await.unwrap();
        // Advance past the grace period so the orphan is a genuine candidate the
        // dry run can report.
        clock.set(10_000);

        let report = sweep_partition(store.as_ref(), &path(), 5_000, 0, true)
            .await
            .unwrap();

        assert!(report.dry_run);
        assert_eq!(
            report.deleted,
            vec![path().object(&orphan.name)],
            "a dry run names what it would remove"
        );
        assert!(
            store.keys().contains(&path().object(&orphan.name)),
            "but the bucket is untouched"
        );
    }

    #[tokio::test]
    async fn a_value_reached_through_a_live_segment_survives_the_sweep() {
        // External values are reached through records, not the manifest, so the
        // sweep has to walk live segments before it can tell a used value from a
        // stranded one. This engine writes values inline today; this protects
        // the format's future external-value path from a sweep that trusts the
        // manifest alone.
        let clock = HandClock::default();
        clock.set(1_000);
        let store = Arc::new(MemoryStore::with_clock(clock.clone()));
        let w = writer(&store).await;

        let live_value = w.put_value(Bytes::from_static(b"keep")).await.unwrap();
        let orphan_value = w.put_value(Bytes::from_static(b"junk")).await.unwrap();
        let mut builder = SegmentBuilder::new(KeyspaceId(1), PartitionId(7), Epoch(1));
        builder
            .push(&SegmentRecord {
                key: Bytes::from_static(b"a"),
                lamport: Lamport(1),
                expires_at_millis: None,
                value: RecordValue::External(live_value.clone()),
            })
            .unwrap();
        let live = w.put_segment(&builder.finish().unwrap()).await.unwrap();
        w.commit(|_| plan(vec![live.clone()])).await.unwrap();
        // Past the grace period, so the stranded value is a candidate.
        clock.set(10_000);

        let report = sweep_partition(store.as_ref(), &path(), 5_000, 0, false)
            .await
            .unwrap();

        assert_eq!(
            report.deleted,
            vec![path().object(&orphan_value.name)],
            "only the value nothing points at is swept"
        );
        let keys = store.keys();
        assert!(
            keys.contains(&path().object(&live_value.name)),
            "the value a live record references is kept"
        );
        assert!(keys.contains(&path().object(&live.name)));
    }
}
