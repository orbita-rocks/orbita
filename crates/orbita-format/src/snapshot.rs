//! Reading a partition out of a bucket.
//!
//! This is the case the format exists to support: something that is not Orbita,
//! holding nothing but read access to the objects, reconstructing exactly the
//! partition's state as of the manifest's `committed_lamport`.
//!
//! Building the index reads footers and key indexes and never touches a data
//! section, so hydrating a partition costs bytes proportional to its keys
//! rather than to its values.
//!
//! What a snapshot shows is the flushed state and nothing later. Writes that
//! are durable in the write-ahead log but not yet flushed are not here, which
//! is the difference between the two durability levels a client can ask about.

use crate::error::{FormatError, Result};
use crate::manifest::Manifest;
use crate::paths::PartitionPath;
use crate::record::{self, RecordValue, SegmentRecord};
use crate::segment::{SegmentFooter, SegmentIndex};

use bytes::Bytes;
use orbita_core::{Lamport, PartitionId, Record};
use orbita_objectstore::ObjectStore;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Where the winning record for a key lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Location {
    segment: usize,
    offset: u64,
    record_length: u32,
    /// Filled in only when two segments both held the key and the Lamport had
    /// to be read to decide between them.
    lamport: Option<Lamport>,
}

/// Where a key's winning record sits, as a [`Snapshot`] resolved it.
///
/// This exists so a storage engine can seed its own in-memory index from a
/// snapshot and then maintain it incrementally across flushes, rather than
/// paying a full rebuild every time the manifest moves. The `segment` is a
/// position in the snapshot's manifest, which is only meaningful against that
/// same manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyLocation {
    pub segment: usize,
    pub offset: u64,
    pub record_length: u32,
}

impl Location {
    fn range(&self) -> std::ops::Range<u64> {
        self.offset..self.offset + u64::from(self.record_length)
    }
}

/// A partition as of one manifest.
pub struct Snapshot<S: ObjectStore + ?Sized> {
    store: Arc<S>,
    path: PartitionPath,
    manifest: Manifest,
    index: BTreeMap<Bytes, Location>,
}

impl<S: ObjectStore + ?Sized> Snapshot<S> {
    /// Reads the manifest and builds the key index.
    ///
    /// Returns `Ok(None)` if the partition has no manifest, which is a
    /// partition that has never flushed rather than a missing one.
    pub async fn open(store: Arc<S>, path: PartitionPath) -> Result<Option<Self>> {
        let Some((manifest, _)) = crate::commit::load_manifest(store.as_ref(), &path).await? else {
            return Ok(None);
        };
        Self::of(store, path, manifest).await.map(Some)
    }

    /// Builds the index for a manifest already in hand.
    ///
    /// The manifest has to agree with the directory it was found in. A manifest
    /// copied into the wrong partition's directory, which is an operator error
    /// rather than an exotic one, would otherwise be served as that partition's
    /// contents, and the identifiers are in the manifest precisely so that the
    /// mistake is detectable.
    pub async fn of(store: Arc<S>, path: PartitionPath, manifest: Manifest) -> Result<Self> {
        if manifest.keyspace_id != path.keyspace_id()
            || manifest.partition_id != path.partition_id()
        {
            return Err(FormatError::Corrupt(format!(
                "a manifest for keyspace {} partition {} is stored under {}",
                manifest.keyspace_id,
                manifest.partition_id,
                path.prefix()
            )));
        }

        let mut snapshot = Self {
            store,
            path,
            manifest,
            index: BTreeMap::new(),
        };
        snapshot.build().await?;
        Ok(snapshot)
    }

    async fn build(&mut self) -> Result<()> {
        for (position, entry) in self.manifest.segments.iter().enumerate() {
            // A shared segment (a split's cross-partition reference) lives under
            // the source partition's directory, not this one's. See ADR 0009.
            let key = self.path.resolve_segment(entry);

            let tail = self
                .store
                .get_range(&key, entry.footer_range())
                .await
                .map_err(FormatError::Store)?;
            let footer = SegmentFooter::decode(&tail)?;
            if footer.record_count != entry.record_count {
                return Err(FormatError::Corrupt(format!(
                    "{} holds {} records where the manifest claims {}",
                    entry.name, footer.record_count, entry.record_count
                )));
            }

            let index_range = footer.index_range(entry.bytes)?;
            let raw = self
                .store
                .get_range(&key, index_range)
                .await
                .map_err(FormatError::Store)?;
            let index = SegmentIndex::decode(&raw, &footer)?;

            for candidate in index.entries() {
                // A shared segment physically holds keys on both sides of the
                // split boundary; a child indexes only the keys in its own
                // range, so it serves exactly what it owns. For a self-written
                // segment every key is already in range, so this is a no-op.
                if !self.manifest.range.contains(&candidate.key) {
                    continue;
                }
                let mut located = Location {
                    segment: position,
                    offset: candidate.offset,
                    record_length: candidate.record_length,
                    lamport: None,
                };
                if let Some(existing) = self.index.get(&candidate.key).copied() {
                    located = self.winner(existing, located).await?;
                }
                self.index.insert(candidate.key.clone(), located);
            }
        }
        Ok(())
    }

    /// Decides which of two records for one key is the live one.
    ///
    /// The higher Lamport wins. Where the two segments' Lamport spans do not
    /// overlap the manifest already answers this, and where they do the
    /// Lamports are read from the records themselves: nine bytes in, eight
    /// bytes long, no value involved.
    ///
    /// # Tie detection here is partial, and deliberately so
    ///
    /// Two records for one key at one Lamport is a corrupt partition rather
    /// than a tie to break, and this reports it when the tie involves the
    /// record that would have won. It does not report a tie between two losers:
    /// with segments at Lamports 5, 9, and 5 for one key, the 9 is the right
    /// answer and this returns it without ever comparing the two 5s.
    ///
    /// Catching that would mean fetching a Lamport for every candidate rather
    /// than only for the ones the manifest cannot separate, which is a request
    /// per duplicate key on the hydration path in exchange for finding
    /// corruption that does not change what any read returns.
    /// [`crate::compact::merge`] has every record in hand already and checks
    /// exhaustively, so a scrub is where this is found.
    async fn winner(&self, existing: Location, candidate: Location) -> Result<Location> {
        let a = &self.manifest.segments[existing.segment];
        let b = &self.manifest.segments[candidate.segment];
        if a.max_lamport < b.min_lamport {
            return Ok(candidate);
        }
        if b.max_lamport < a.min_lamport {
            return Ok(existing);
        }

        let existing_lamport = self.lamport_at(existing).await?;
        let candidate_lamport = self.lamport_at(candidate).await?;
        if existing_lamport == candidate_lamport {
            return Err(FormatError::Corrupt(format!(
                "{} and {} both hold a record at lamport {existing_lamport} for one key",
                a.name, b.name
            )));
        }
        Ok(if candidate_lamport > existing_lamport {
            Location {
                lamport: Some(candidate_lamport),
                ..candidate
            }
        } else {
            Location {
                lamport: Some(existing_lamport),
                ..existing
            }
        })
    }

    async fn lamport_at(&self, located: Location) -> Result<Lamport> {
        if let Some(lamport) = located.lamport {
            return Ok(lamport);
        }
        let key = self
            .path
            .resolve_segment(&self.manifest.segments[located.segment]);
        let head = self
            .store
            .get_range(
                &key,
                located.offset..located.offset + record::LAMPORT_OFFSET + 8,
            )
            .await
            .map_err(FormatError::Store)?;
        record::peek_lamport(&head)
    }

    #[must_use]
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// The flush horizon. A node recovering this partition replays log entries
    /// above it and skips everything at or below it.
    #[must_use]
    pub fn committed_lamport(&self) -> Lamport {
        self.manifest.committed_lamport
    }

    /// How many keys the index holds, counting tombstones and expired records,
    /// which are absent to a reader but still occupy the index.
    #[must_use]
    pub fn len(&self) -> usize {
        self.index.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Every key in the index, ascending, without regard to visibility.
    pub fn keys(&self) -> impl Iterator<Item = &Bytes> {
        self.index.keys()
    }

    /// Every key and where its winning record lives, ascending.
    ///
    /// See [`KeyLocation`] for what the positions mean and why this is public.
    pub fn locations(&self) -> impl Iterator<Item = (&Bytes, KeyLocation)> {
        self.index.iter().map(|(key, located)| {
            (
                key,
                KeyLocation {
                    segment: located.segment,
                    offset: located.offset,
                    record_length: located.record_length,
                },
            )
        })
    }

    /// The record for a key, or `None` if no segment holds one.
    ///
    /// This is the raw record, including tombstones and expired records. Most
    /// callers want [`Snapshot::get`], which drops both.
    pub async fn record(&self, key: &[u8]) -> Result<Option<SegmentRecord>> {
        let Some(located) = self.index.get(key).copied() else {
            return Ok(None);
        };
        let entry = &self.manifest.segments[located.segment];
        let name = &entry.name;
        // A shared segment lives under the source partition's directory.
        let raw = self
            .store
            .get_range(&self.path.resolve_segment(entry), located.range())
            .await
            .map_err(FormatError::Store)?;
        let (record, consumed) = SegmentRecord::decode(&raw)?;
        if consumed != raw.len() {
            return Err(FormatError::Corrupt(format!(
                "{name} holds a {consumed} byte record where its index claims {}",
                raw.len()
            )));
        }
        if record.key != key {
            return Err(FormatError::Corrupt(format!(
                "{name} holds a record for a different key than its index claims"
            )));
        }
        Ok(Some(record))
    }

    /// What a client would read for this key at `now_millis`.
    ///
    /// Tombstones and expired records are absent keys rather than present ones
    /// with special values, which is the step an external reader is most likely
    /// to get wrong and the one that would report deleted data as live.
    pub async fn get(&self, key: &[u8], now_millis: u64) -> Result<Option<Record>> {
        let Some(record) = self.record(key).await? else {
            return Ok(None);
        };
        if !record.is_visible_at(now_millis) {
            return Ok(None);
        }
        // An external value written by a shared segment's owner lives under the
        // source partition's directory, so resolve it the same way the record
        // was resolved.
        let source = self
            .index
            .get(key)
            .and_then(|located| self.manifest.segments.get(located.segment))
            .and_then(|entry| entry.source);
        let value = self.value(&record, source).await?;
        Ok(Some(record.to_record(value)))
    }

    /// The value bytes for a record, fetching an external value if that is
    /// where they live and checking it against the record's own length and
    /// checksum.
    ///
    /// `source` is the partition whose directory the value object lives under,
    /// which differs from this snapshot's own partition only for a value
    /// reached through a shared (split) segment. `None` resolves relative to
    /// this partition, which is correct for every value it wrote itself.
    pub async fn value(
        &self,
        record: &SegmentRecord,
        source: Option<PartitionId>,
    ) -> Result<Bytes> {
        match &record.value {
            RecordValue::Tombstone => Ok(Bytes::new()),
            RecordValue::Inline(value) => Ok(value.clone()),
            RecordValue::External(external) => {
                let owner = match source {
                    None => self.path.object(&external.name),
                    Some(source) => self.path.for_partition(source).object(&external.name),
                };
                let (bytes, _) = self.store.get(&owner).await.map_err(FormatError::Store)?;
                if bytes.len() as u64 != external.length {
                    return Err(FormatError::Corrupt(format!(
                        "{} is {} bytes where its record claims {}",
                        external.name,
                        bytes.len(),
                        external.length
                    )));
                }
                let computed = crc32c::crc32c(&bytes);
                if computed != external.crc32c {
                    return Err(FormatError::ChecksumMismatch {
                        what: "external value",
                        stored: external.crc32c,
                        computed,
                    });
                }
                Ok(bytes)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::{CommitPlan, PartitionWriter};
    use crate::manifest::SegmentEntry;
    use crate::record::ExternalValue;
    use crate::segment::{BuiltSegment, SegmentBuilder};
    use crate::testing::MemoryStore;

    use orbita_core::{Epoch, KeyRange, KeyspaceId, PartitionId, Version};

    const KEYSPACE: KeyspaceId = KeyspaceId(1);
    const PARTITION: PartitionId = PartitionId(7);

    fn path() -> PartitionPath {
        PartitionPath::new("orbita", KEYSPACE, PARTITION)
    }

    fn key(k: &str) -> Bytes {
        Bytes::copy_from_slice(k.as_bytes())
    }

    fn put(k: &str, lamport: u64, value: &str) -> SegmentRecord {
        SegmentRecord {
            key: key(k),
            lamport: Lamport(lamport),
            expires_at_millis: None,
            value: RecordValue::Inline(key(value)),
        }
    }

    fn build(records: &[SegmentRecord]) -> BuiltSegment {
        let mut builder = SegmentBuilder::new(KEYSPACE, PARTITION, Epoch(1));
        for record in records {
            builder.push(record).expect("sorted");
        }
        builder.finish().expect("non-empty")
    }

    /// Commits a sequence of flushes, each one segment, in order.
    async fn partition(flushes: &[Vec<SegmentRecord>]) -> Arc<MemoryStore> {
        let store = Arc::new(MemoryStore::new());
        let writer = PartitionWriter::open(store.clone(), path(), Epoch(1))
            .await
            .expect("opens");

        let mut published: Vec<SegmentEntry> = Vec::new();
        for records in flushes {
            let entry = writer.put_segment(&build(records)).await.expect("written");
            published.push(entry);
            let segments = published.clone();
            writer
                .commit(|_| CommitPlan {
                    committed_lamport: segments
                        .iter()
                        .map(|s| s.max_lamport)
                        .max()
                        .unwrap_or(Lamport::ZERO),
                    range: KeyRange::unbounded(),
                    segments: segments.clone(),
                })
                .await
                .expect("committed");
        }
        store
    }

    async fn snapshot(store: Arc<MemoryStore>) -> Snapshot<MemoryStore> {
        Snapshot::open(store, path())
            .await
            .expect("readable")
            .expect("a manifest")
    }

    #[tokio::test]
    async fn a_partition_with_no_manifest_reads_as_absent_rather_than_broken() {
        let store = Arc::new(MemoryStore::new());
        assert!(Snapshot::open(store, path()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_snapshot_reads_what_was_flushed() {
        let store = partition(&[vec![put("a", 1, "one"), put("b", 2, "two")]]).await;
        let snapshot = snapshot(store).await;

        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot.committed_lamport(), Lamport(2));

        let record = snapshot.get(b"a", 0).await.unwrap().expect("present");
        assert_eq!(record.value, key("one"));
        assert_eq!(record.version, Version(1));
        assert_eq!(snapshot.get(b"zz", 0).await.unwrap(), None);
    }

    #[tokio::test]
    async fn overlapping_segments_resolve_by_lamport() {
        // Two flushes both holding "b", which is the normal case rather than a
        // defect: recent writes are scattered across the keyspace.
        let store = partition(&[
            vec![put("a", 1, "old"), put("b", 2, "old")],
            vec![put("b", 9, "new"), put("c", 10, "new")],
        ])
        .await;
        let snapshot = snapshot(store).await;

        assert_eq!(snapshot.len(), 3);
        assert_eq!(
            snapshot.get(b"b", 0).await.unwrap().unwrap().value,
            key("new"),
            "the higher lamport wins"
        );
        assert_eq!(
            snapshot.get(b"a", 0).await.unwrap().unwrap().value,
            key("old")
        );
    }

    #[tokio::test]
    async fn an_older_flush_committed_after_a_newer_one_still_loses() {
        // The segments are published in the opposite order to their Lamports,
        // which a reader that trusted manifest order would get wrong.
        let store = partition(&[
            vec![put("b", 9, "new")],
            vec![put("a", 1, "old"), put("b", 2, "old")],
        ])
        .await;
        let snapshot = snapshot(store).await;
        assert_eq!(
            snapshot.get(b"b", 0).await.unwrap().unwrap().value,
            key("new")
        );
    }

    #[tokio::test]
    async fn overlapping_lamport_spans_are_resolved_from_the_records() {
        // Spans that interleave, so the manifest cannot decide and the Lamports
        // have to be read: [1, 10] and [5, 8], with "b" in both.
        let store = partition(&[
            vec![put("a", 1, "old"), put("b", 4, "old"), put("z", 10, "old")],
            vec![put("b", 5, "new"), put("m", 8, "new")],
        ])
        .await;
        let snapshot = snapshot(store).await;
        assert_eq!(
            snapshot.get(b"b", 0).await.unwrap().unwrap().value,
            key("new")
        );
    }

    #[tokio::test]
    async fn a_tombstone_is_an_absent_key_not_a_present_one() {
        let store = partition(&[
            vec![put("a", 1, "value")],
            vec![SegmentRecord {
                key: key("a"),
                lamport: Lamport(5),
                expires_at_millis: Some(86_400_000),
                value: RecordValue::Tombstone,
            }],
        ])
        .await;
        let snapshot = snapshot(store).await;

        assert_eq!(snapshot.get(b"a", 0).await.unwrap(), None);
        assert!(
            snapshot.record(b"a").await.unwrap().unwrap().is_tombstone(),
            "the tombstone is still there for anything that needs the version"
        );
        assert_eq!(snapshot.len(), 1, "it occupies the index");
    }

    #[tokio::test]
    async fn an_expired_record_is_absent_from_the_instant_it_expires() {
        let store = partition(&[vec![SegmentRecord {
            key: key("a"),
            lamport: Lamport(1),
            expires_at_millis: Some(100),
            value: RecordValue::Inline(key("v")),
        }]])
        .await;
        let snapshot = snapshot(store).await;

        assert!(snapshot.get(b"a", 99).await.unwrap().is_some());
        assert_eq!(snapshot.get(b"a", 100).await.unwrap(), None);
    }

    #[tokio::test]
    async fn an_external_value_is_fetched_and_checked_against_its_record() {
        let store = Arc::new(MemoryStore::new());
        let writer = PartitionWriter::open(store.clone(), path(), Epoch(1))
            .await
            .unwrap();

        let big = Bytes::from(vec![7u8; 4096]);
        let external = writer.put_value(big.clone()).await.unwrap();
        let entry = writer
            .put_segment(&build(&[SegmentRecord {
                key: key("a"),
                lamport: Lamport(3),
                expires_at_millis: None,
                value: RecordValue::External(external.clone()),
            }]))
            .await
            .unwrap();
        writer
            .commit(|_| CommitPlan {
                committed_lamport: Lamport(3),
                range: KeyRange::unbounded(),
                segments: vec![entry.clone()],
            })
            .await
            .unwrap();

        let snapshot = snapshot(store.clone()).await;
        assert_eq!(snapshot.get(b"a", 0).await.unwrap().unwrap().value, big);

        // The object holds the value and nothing else, which is what lets a
        // tool that has never heard of this format use it directly.
        let (raw, _) = store.get(&path().object(&external.name)).await.unwrap();
        assert_eq!(raw, big);
    }

    #[tokio::test]
    async fn a_damaged_external_value_is_caught_by_the_referencing_record() {
        let store = Arc::new(MemoryStore::new());
        let writer = PartitionWriter::open(store.clone(), path(), Epoch(1))
            .await
            .unwrap();
        let external = writer
            .put_value(Bytes::from_static(b"value"))
            .await
            .unwrap();
        let entry = writer
            .put_segment(&build(&[SegmentRecord {
                key: key("a"),
                lamport: Lamport(3),
                expires_at_millis: None,
                value: RecordValue::External(external.clone()),
            }]))
            .await
            .unwrap();
        writer
            .commit(|_| CommitPlan {
                committed_lamport: Lamport(3),
                range: KeyRange::unbounded(),
                segments: vec![entry.clone()],
            })
            .await
            .unwrap();

        store
            .put(&path().object(&external.name), Bytes::from_static(b"valve"))
            .await
            .unwrap();

        let snapshot = snapshot(store).await;
        assert!(matches!(
            snapshot.get(b"a", 0).await,
            Err(FormatError::ChecksumMismatch { .. })
        ));
    }

    #[tokio::test]
    async fn a_manifest_found_under_the_wrong_partition_is_refused() {
        let store = partition(&[vec![put("a", 1, "v")]]).await;
        let elsewhere = PartitionPath::new("orbita", KEYSPACE, PartitionId(8));
        let (manifest, _) = crate::commit::load_manifest(store.as_ref(), &path())
            .await
            .unwrap()
            .unwrap();
        store
            .put(&elsewhere.manifest(), manifest.encode())
            .await
            .unwrap();

        let outcome = Snapshot::open(store, elsewhere).await.err();
        assert!(
            matches!(outcome, Some(FormatError::Corrupt(_))),
            "{outcome:?}"
        );
    }

    #[tokio::test]
    async fn a_manifest_naming_a_segment_that_is_not_there_fails_the_read() {
        let store = partition(&[vec![put("a", 1, "v")]]).await;
        let snapshot = snapshot(store.clone()).await;
        let name = snapshot.manifest().segments[0].name.clone();
        store.delete(&path().object(&name)).await.unwrap();

        assert!(
            Snapshot::open(store, path()).await.is_err(),
            "a missing object is a failure rather than a partition with fewer keys"
        );
    }

    #[tokio::test]
    async fn two_records_for_one_key_at_one_lamport_is_corruption() {
        let store = partition(&[
            vec![put("a", 1, "one"), put("b", 5, "one")],
            vec![put("b", 5, "two"), put("c", 6, "two")],
        ])
        .await;
        let outcome = Snapshot::open(store, path()).await.err();
        assert!(
            matches!(outcome, Some(FormatError::Corrupt(_))),
            "a tie is not something to break: {outcome:?}"
        );
    }

    #[tokio::test]
    async fn an_unreferenced_segment_is_not_part_of_the_partition() {
        let store = partition(&[vec![put("a", 1, "v")]]).await;
        let writer = PartitionWriter::open(store.clone(), path(), Epoch(1))
            .await
            .unwrap();
        // Written but never committed, which is what an abandoned commit leaves.
        writer
            .put_segment(&build(&[put("b", 2, "v")]))
            .await
            .unwrap();

        let snapshot = snapshot(store).await;
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot.get(b"b", 0).await.unwrap(), None);
    }

    #[tokio::test]
    async fn building_the_index_never_reads_a_data_section() {
        // The claim that makes hydration cheap: a node rebuilding its index, or
        // a tool listing a keyspace, reads footers and key indexes and nothing
        // else. Two requests per segment, neither of them touching a value.
        let store = partition(&[vec![put("a", 1, "value"), put("b", 2, "value")]]).await;
        let snapshot = snapshot(store.clone()).await;
        assert_eq!(snapshot.keys().count(), 2);

        let entry = &snapshot.manifest().segments[0];
        let object = path().object(&entry.name);
        let served = store.ranges_served();
        let footer = SegmentFooter::decode(
            &store
                .get_range(&object, entry.footer_range())
                .await
                .unwrap(),
        )
        .unwrap();

        assert_eq!(served.len(), 2, "one footer and one index: {served:?}");
        for (key, range) in served {
            assert_eq!(key, object);
            assert!(
                range.start >= footer.index_offset,
                "{range:?} reaches into the data section"
            );
        }
    }

    #[tokio::test]
    async fn an_external_value_reference_survives_a_snapshot_that_never_reads_it() {
        let store = Arc::new(MemoryStore::new());
        let writer = PartitionWriter::open(store.clone(), path(), Epoch(1))
            .await
            .unwrap();
        let external = writer.put_value(Bytes::from(vec![1u8; 16])).await.unwrap();
        let entry = writer
            .put_segment(&build(&[SegmentRecord {
                key: key("a"),
                lamport: Lamport(1),
                expires_at_millis: None,
                value: RecordValue::External(external),
            }]))
            .await
            .unwrap();
        writer
            .commit(|_| CommitPlan {
                committed_lamport: Lamport(1),
                range: KeyRange::unbounded(),
                segments: vec![entry.clone()],
            })
            .await
            .unwrap();

        let snapshot = snapshot(store).await;
        let record = snapshot.record(b"a").await.unwrap().unwrap();
        assert!(matches!(
            record.value,
            RecordValue::External(ExternalValue { .. })
        ));
    }
}
