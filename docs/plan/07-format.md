# 07: Partition format (`orbita-format`)

Owns the bytes Orbita writes to object storage, and everything needed to read
them back. The specification is [`docs/format/partition-v1.md`](../format/partition-v1.md);
this crate is one implementation of it rather than its meaning, which is the
distinction that makes the format open.

Added after briefs 01 to 06, because [ADR 0006](../adr/0006-partitions-are-an-index-over-immutable-objects.md)
replaced RocksDB with a format this project owns, and a format nobody can
implement against is a single-implementation format with extra steps.

## Scope

- Segment encoding and decoding: header, records, key index, footer, and every
  checksum in between.
- The manifest, including validating a manifest a stranger wrote rather than
  trusting one this build produced.
- Object naming, and recovering a writer's next sequence from a listing.
- The commit protocol, meaning the compare-and-swap on `manifest.json` and the
  epoch check that fences a deposed writer before it writes.
- The reader: manifest, footers, key indexes, then one range request per key.
- The compaction merge rules, and finding unreferenced objects.
- Golden test vectors, checked in as bytes.

## Out of scope

- Recent writes. This crate has no memtable and does not decide when to flush.
- The write-ahead log, replication, ownership, and the partition map.
- Deleting anything. [`sweep`](../../crates/orbita-format/src/sweep.rs) finds
  unreferenced objects; the grace period that decides when one may go is the
  caller's, and needs object creation times that
  `orbita_objectstore::ObjectMeta` does not yet carry.

## What is left

`orbita-storage` still runs on RocksDB. Moving it onto this crate is the next
step and a larger one, because it is where the parts this brief excludes live:
a memtable, a flush policy, an index that spans both, recovery from the log
above `committed_lamport`, and a compaction schedule. Doing it in the same
change as the format would have meant reviewing the bytes and the engine
together, and the bytes are the part another implementation has to agree with.

## Done when

- A segment round-trips, and a flipped bit anywhere below the header is caught.
  The header's identifiers are outside every checksum on purpose: they repeat
  what the object's name says, so that an object copied out of its path still
  describes itself.
- The specification's own manifest example decodes to what its prose claims.
- A deposed writer at epoch 6 cannot replace a manifest at epoch 7, and a
  writer that lost a compare-and-swap rebuilds against what it lost to.
- A reader resolves overlapping segments by Lamport, and reports tombstones and
  expired records as absent keys rather than present ones.
- Building the index reads footers and key indexes and no data section.
- The golden vectors match byte for byte.
