//! Publishing objects, and the manifest swap that makes them real.
//!
//! A commit publishes segments and value objects that are already written. It
//! is the manifest swap alone that makes them part of the partition, so writing
//! an object is always safe to repeat and always safe to abandon.
//!
//! The conditional write is the only ordering primitive the format needs, which
//! is why [`ObjectStore`] requires compare-and-swap. A backend without it
//! cannot host this format safely.

use crate::error::{FormatError, Result};
use crate::manifest::{Manifest, SegmentEntry};
use crate::paths::{self, PartitionPath};
use crate::record::ExternalValue;
use crate::segment::BuiltSegment;

use bytes::Bytes;
use orbita_core::{Epoch, KeyRange, Lamport};
use orbita_objectstore::{ETag, ObjectError, ObjectStore, Precondition};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// How many lost races a commit will absorb before it gives up.
///
/// A partition has one writer, so losing repeatedly means something else is
/// writing this manifest. Looping forever would turn that into a hang, and a
/// hang is the hardest failure to diagnose from the outside.
const MAX_COMMIT_ATTEMPTS: u32 = 32;

/// What a commit is publishing.
///
/// The writer supplies the segments and the horizon; it does not supply the
/// epoch. That is the point: a writer stamps its own ownership epoch and no
/// other value, and making the epoch unreachable from here is a cheaper
/// guarantee than a rule somebody has to remember.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitPlan {
    pub committed_lamport: Lamport,
    pub range: KeyRange,
    pub segments: Vec<SegmentEntry>,
}

/// Reads a partition's manifest, keeping the entity tag a commit needs.
///
/// `Ok(None)` means the partition has no manifest yet, which is a state rather
/// than an error: a partition that has never flushed has nothing to point at.
pub async fn load_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    path: &PartitionPath,
) -> Result<Option<(Manifest, ETag)>> {
    match store.get(&path.manifest()).await {
        Ok((bytes, etag)) => Ok(Some((Manifest::decode(&bytes)?, etag))),
        Err(ObjectError::NotFound(_)) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// The one writer of one partition.
pub struct PartitionWriter<S: ObjectStore + ?Sized> {
    store: Arc<S>,
    path: PartitionPath,
    epoch: Epoch,
    next_sequence: AtomicU64,
}

impl<S: ObjectStore + ?Sized> PartitionWriter<S> {
    /// Takes up writing for `epoch`, establishing the next sequence first.
    ///
    /// The sequence comes off a listing rather than out of memory, because a
    /// crash between writing an object and recording that fact leaves the
    /// object on the store and the memory gone. Reusing that name is the one
    /// unrecoverable failure this format has, so the listing is not an
    /// optimisation to skip on a restart that "knows" where it was.
    pub async fn open(store: Arc<S>, path: PartitionPath, epoch: Epoch) -> Result<Self> {
        let mut highest: Option<u64> = None;
        for prefix in [path.segments_prefix(), path.values_prefix()] {
            for object in store.list(&prefix).await? {
                let Some(relative) = path.relative(&object.key) else {
                    continue;
                };
                // Only this writer's own epoch matters. A higher sequence under
                // somebody else's epoch cannot collide with a name this writer
                // will produce, which is the reason names carry an epoch.
                if let Some((found, sequence)) = paths::parse_object_name(relative) {
                    if found == epoch {
                        highest = Some(highest.map_or(sequence, |h: u64| h.max(sequence)));
                    }
                }
            }
        }

        Ok(Self {
            store,
            path,
            epoch,
            next_sequence: AtomicU64::new(highest.map_or(0, |h| h + 1)),
        })
    }

    #[must_use]
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    #[must_use]
    pub fn path(&self) -> &PartitionPath {
        &self.path
    }

    fn take_sequence(&self) -> u64 {
        self.next_sequence.fetch_add(1, Ordering::Relaxed)
    }

    /// Writes a value into its own object and returns the reference a record
    /// carries.
    ///
    /// The object holds the value bytes and nothing else: no header, no
    /// framing. Its length and checksum live in the referencing record, so the
    /// object stays exactly what a caller stored and can be fetched and used
    /// directly by something that has never heard of this format.
    pub async fn put_value(&self, value: Bytes) -> Result<ExternalValue> {
        let name = paths::value_name(self.epoch, self.take_sequence());
        let external = ExternalValue {
            length: value.len() as u64,
            crc32c: crc32c::crc32c(&value),
            name,
        };
        self.store
            .put(&self.path.object(&external.name), value)
            .await?;
        Ok(external)
    }

    /// Writes a segment and returns the entry that would publish it.
    ///
    /// Writing is not publishing. Until a manifest names it, this object is not
    /// part of the partition and the sweep is entitled to collect it.
    pub async fn put_segment(&self, built: &BuiltSegment) -> Result<SegmentEntry> {
        let name = paths::segment_name(self.epoch, self.take_sequence());
        self.store
            .put(&self.path.object(&name), built.bytes.clone())
            .await?;
        Ok(SegmentEntry::of(name, built))
    }

    /// Swaps in a new manifest, retrying if it loses the race.
    ///
    /// `plan` is called with the manifest currently in the store, or `None` if
    /// there is none, and is called again on each retry so that a writer that
    /// lost a race builds against what it lost to rather than against what it
    /// read the first time.
    ///
    /// Stops with [`FormatError::Deposed`] if the manifest carries an epoch
    /// above this writer's. That check happens before the write rather than
    /// only on the retry path, which is the difference between fencing a
    /// deposed owner and letting it silently erase its replacement's work: its
    /// entity tag is current, so its conditional write would succeed.
    pub async fn commit<F>(&self, mut plan: F) -> Result<Manifest>
    where
        F: FnMut(Option<&Manifest>) -> CommitPlan,
    {
        for _ in 0..MAX_COMMIT_ATTEMPTS {
            let current = load_manifest(self.store.as_ref(), &self.path).await?;

            if let Some((manifest, _)) = &current {
                if manifest.epoch > self.epoch {
                    return Err(FormatError::Deposed {
                        own: self.epoch,
                        found: manifest.epoch,
                    });
                }
            }

            let plan = plan(current.as_ref().map(|(m, _)| m));
            let manifest = Manifest {
                keyspace_id: self.path.keyspace_id(),
                partition_id: self.path.partition_id(),
                epoch: self.epoch,
                committed_lamport: plan.committed_lamport,
                range: plan.range,
                segments: plan.segments,
            };
            // Encoding and decoding here costs one pass over a small object and
            // catches a writer publishing a manifest no reader would accept,
            // which is worth far more than the pass costs.
            let encoded = manifest.encode();
            Manifest::decode(&encoded)?;

            let precondition = match &current {
                Some((_, etag)) => Precondition::Match(etag.clone()),
                None => Precondition::NotExists,
            };
            match self
                .store
                .put_if(&self.path.manifest(), encoded, precondition)
                .await
            {
                Ok(_) => return Ok(manifest),
                // Somebody committed between the read and the write. Go back and
                // read again; the epoch check above decides whether that retry
                // is legitimate or whether this writer is finished.
                Err(ObjectError::PreconditionFailed(_)) => continue,
                Err(e) => return Err(e.into()),
            }
        }

        Err(FormatError::Contended {
            attempts: MAX_COMMIT_ATTEMPTS,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{RecordValue, SegmentRecord};
    use crate::segment::SegmentBuilder;
    use crate::testing::MemoryStore;

    use orbita_core::{KeyspaceId, PartitionId};

    fn path() -> PartitionPath {
        PartitionPath::new("orbita", KeyspaceId(1), PartitionId(7))
    }

    fn segment(key: &str, lamport: u64) -> BuiltSegment {
        let mut builder = SegmentBuilder::new(KeyspaceId(1), PartitionId(7), Epoch(1));
        builder
            .push(&SegmentRecord {
                key: Bytes::copy_from_slice(key.as_bytes()),
                lamport: Lamport(lamport),
                expires_at_millis: None,
                value: RecordValue::Inline(Bytes::from_static(b"v")),
            })
            .unwrap();
        builder.finish().unwrap()
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

    async fn writer<S: ObjectStore>(store: &Arc<S>, epoch: Epoch) -> PartitionWriter<S> {
        PartitionWriter::open(store.clone(), path(), epoch)
            .await
            .expect("opens")
    }

    #[tokio::test]
    async fn the_first_commit_creates_the_manifest() {
        let store = Arc::new(MemoryStore::new());
        let writer = writer(&store, Epoch(1)).await;

        let entry = writer.put_segment(&segment("a", 5)).await.unwrap();
        let manifest = writer.commit(|_| plan(vec![entry.clone()])).await.unwrap();

        assert_eq!(manifest.epoch, Epoch(1));
        assert_eq!(manifest.committed_lamport, Lamport(5));
        let (stored, _) = load_manifest(store.as_ref(), &path())
            .await
            .unwrap()
            .expect("committed");
        assert_eq!(stored, manifest);
    }

    #[tokio::test]
    async fn a_writer_stamps_its_own_epoch_and_never_the_one_it_read() {
        let store = Arc::new(MemoryStore::new());
        writer(&store, Epoch(1))
            .await
            .commit(|_| plan(vec![]))
            .await
            .unwrap();

        let successor = writer(&store, Epoch(2)).await;
        let manifest = successor.commit(|_| plan(vec![])).await.unwrap();
        assert_eq!(manifest.epoch, Epoch(2));
    }

    #[tokio::test]
    async fn a_deposed_owner_cannot_erase_its_replacements_work() {
        // The failure the epoch check exists for. The old owner's entity tag is
        // current, so without the check its conditional write succeeds and the
        // replacement's segments vanish with no error raised anywhere.
        let store = Arc::new(MemoryStore::new());
        let deposed = writer(&store, Epoch(6)).await;
        deposed.commit(|_| plan(vec![])).await.unwrap();

        let replacement = writer(&store, Epoch(7)).await;
        let entry = replacement.put_segment(&segment("a", 9)).await.unwrap();
        replacement
            .commit(|_| plan(vec![entry.clone()]))
            .await
            .unwrap();

        let stale = deposed.put_segment(&segment("b", 3)).await.unwrap();
        let outcome = deposed.commit(|_| plan(vec![stale.clone()])).await;
        assert!(
            matches!(
                outcome,
                Err(FormatError::Deposed {
                    own: Epoch(6),
                    found: Epoch(7)
                })
            ),
            "{outcome:?}"
        );

        let (stored, _) = load_manifest(store.as_ref(), &path())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.epoch, Epoch(7));
        assert_eq!(stored.segments, vec![entry], "the replacement still stands");
    }

    #[tokio::test]
    async fn a_lost_race_rebuilds_against_what_it_lost_to() {
        // A writer reads the manifest, somebody else commits, and the first
        // writer's conditional write then fails. The retry has to build against
        // what it lost to; building against what it read the first time would
        // drop the other commit.
        let plain = Arc::new(MemoryStore::new());
        let bootstrap = writer(&plain, Epoch(1)).await;
        let theirs = bootstrap.put_segment(&segment("a", 4)).await.unwrap();
        bootstrap.commit(|_| plan(vec![])).await.unwrap();

        let interloper = Manifest {
            keyspace_id: KeyspaceId(1),
            partition_id: PartitionId(7),
            epoch: Epoch(1),
            committed_lamport: Lamport(4),
            range: KeyRange::unbounded(),
            segments: vec![theirs.clone()],
        };
        let store = Arc::new(RacingStore::new(plain, interloper.encode()));

        let racer = writer(&store, Epoch(1)).await;
        let mine = racer.put_segment(&segment("b", 8)).await.unwrap();
        let mut seen = Vec::new();
        let manifest = racer
            .commit(|current| {
                seen.push(current.map_or(0, |m| m.segments.len()));
                let mut segments = current.map(|m| m.segments.clone()).unwrap_or_default();
                segments.push(mine.clone());
                plan(segments)
            })
            .await
            .unwrap();

        assert_eq!(seen, vec![0, 1], "the retry saw the interloper's manifest");
        assert_eq!(
            manifest.segments,
            vec![theirs, mine],
            "neither commit was dropped"
        );
    }

    /// A store that lets one commit slip in between a writer's read and its
    /// conditional write, which is the interleaving the precondition exists to
    /// catch and the only one a single-threaded test cannot otherwise produce.
    struct RacingStore {
        inner: Arc<MemoryStore>,
        interloper: std::sync::Mutex<Option<Bytes>>,
    }

    impl RacingStore {
        fn new(inner: Arc<MemoryStore>, interloper: Bytes) -> Self {
            Self {
                inner,
                interloper: std::sync::Mutex::new(Some(interloper)),
            }
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for RacingStore {
        async fn put(&self, key: &str, data: Bytes) -> orbita_objectstore::ObjectResult<ETag> {
            self.inner.put(key, data).await
        }

        async fn put_if(
            &self,
            key: &str,
            data: Bytes,
            precondition: Precondition,
        ) -> orbita_objectstore::ObjectResult<ETag> {
            let first = self
                .interloper
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take();
            if let Some(bytes) = first {
                self.inner.put(key, bytes).await?;
            }
            self.inner.put_if(key, data, precondition).await
        }

        async fn get(&self, key: &str) -> orbita_objectstore::ObjectResult<(Bytes, ETag)> {
            self.inner.get(key).await
        }

        async fn get_range(
            &self,
            key: &str,
            range: std::ops::Range<u64>,
        ) -> orbita_objectstore::ObjectResult<Bytes> {
            self.inner.get_range(key, range).await
        }

        async fn head(
            &self,
            key: &str,
        ) -> orbita_objectstore::ObjectResult<orbita_objectstore::ObjectMeta> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
        ) -> orbita_objectstore::ObjectResult<Vec<orbita_objectstore::ObjectMeta>> {
            self.inner.list(prefix).await
        }

        async fn delete(&self, key: &str) -> orbita_objectstore::ObjectResult<()> {
            self.inner.delete(key).await
        }
    }

    #[tokio::test]
    async fn a_restarted_writer_takes_its_sequence_from_the_store() {
        let store = Arc::new(MemoryStore::new());
        let before = writer(&store, Epoch(3)).await;
        let first = before.put_segment(&segment("a", 1)).await.unwrap();
        // Never committed, so it is an orphan. The name is still used.
        assert_eq!(first.name, paths::segment_name(Epoch(3), 0));

        let after = writer(&store, Epoch(3)).await;
        let second = after.put_segment(&segment("a", 1)).await.unwrap();
        assert_eq!(
            second.name,
            paths::segment_name(Epoch(3), 1),
            "a name is never reused, even by a writer that forgot it used one"
        );
    }

    #[tokio::test]
    async fn a_new_epoch_starts_its_own_sequence() {
        let store = Arc::new(MemoryStore::new());
        writer(&store, Epoch(3))
            .await
            .put_segment(&segment("a", 1))
            .await
            .unwrap();

        let successor = writer(&store, Epoch(4)).await;
        let entry = successor.put_segment(&segment("a", 1)).await.unwrap();
        assert_eq!(entry.name, paths::segment_name(Epoch(4), 0));
    }

    #[tokio::test]
    async fn segments_and_values_share_one_sequence() {
        // They share a sequence because they share the guarantee: a name is
        // never reused. Two counters would be two chances to get it wrong.
        let store = Arc::new(MemoryStore::new());
        let writer = writer(&store, Epoch(1)).await;
        let value = writer.put_value(Bytes::from_static(b"big")).await.unwrap();
        let segment = writer.put_segment(&segment("a", 1)).await.unwrap();

        assert_eq!(value.name, paths::value_name(Epoch(1), 0));
        assert_eq!(segment.name, paths::segment_name(Epoch(1), 1));
        assert_eq!(value.length, 3);
        assert_eq!(value.crc32c, crc32c::crc32c(b"big"));
    }
}
