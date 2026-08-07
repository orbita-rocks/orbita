# 0009: A split shares the parent's segments in place

Status: Accepted, 2026-08-08.

Extends [ADR 0006](0006-partitions-are-an-index-over-immutable-objects.md). It
does not supersede it: partitions are still a memory-resident index over
immutable objects. This adds that the objects may be shared by more than one
partition within a keyspace.

## Context

A range partition splits when it grows too large for one owner to index in
memory. Splitting divides one partition's key range into two children, each a
new partition with its own id.

The obvious implementation copies data: each child gets its own segment objects
holding its half of the keys. That is correct and it is what most LSM stores
do. It is also expensive in exactly the situation that triggers a split. A
partition splits *because* it is large, so a copy-on-split rewrites the whole
partition — hundreds of megabytes to gigabytes of object storage — at the
moment the system is already under pressure, doubling the stored bytes until
compaction reclaims the originals. It also takes long enough that the split
either blocks writes for its duration or races them.

[ADR 0006](0006-partitions-are-an-index-over-immutable-objects.md) already gives
us a cheaper primitive. A partition is an *index* over immutable segment
objects; the index is memory-resident and cheap to build, the objects are the
expensive part and they never change. A split does not need new objects — it
needs two new indexes over the objects that already exist. The parent's
segments already hold the children's data, correctly sorted, correctly
versioned. All a child needs is to reference them.

The blocker was purely structural. A manifest names its segments *relative to
its own partition directory* (`crates/orbita-format/src/paths.rs`), so a segment
object lives under exactly one partition's prefix and only that partition's
manifest could name it. A child, having a new partition id, had no way to point
at the parent's objects.

## Decision

**On a split, the children reference the parent's immutable segment objects in
place. No segment bytes are copied.**

Concretely:

- A manifest segment entry may optionally carry the **source partition** the
  object lives under. An entry a partition wrote itself carries no source and is
  resolved relative to that partition's own directory, exactly as before. An
  entry a split produced carries the parent's partition id, and the reader
  resolves the object under the *parent's* directory. This is an additive,
  optional field: a manifest with no cross-partition entries is byte-identical
  to one written before this change, so historical manifests replay unchanged
  and the format version does not move.

- A segment object therefore becomes **shareable across partitions within a
  keyspace**. The same object may be named by the parent's manifest (before the
  split) and by one or both children's manifests (after it).

- **Liveness of a shared object is by reference from any live manifest, not by
  prefix.** An object under partition P's directory is garbage only when *no*
  live partition in the keyspace — P or any other — still names it. The orphan
  sweep, which until now judged an object solely against the manifest of the
  partition whose prefix it sat under, must union references across the
  keyspace before it deletes anything, and fail closed when it cannot establish
  that union.

- A child's index is **restricted to the child's key range** even though a
  shared segment physically holds keys on both sides of the split boundary. The
  reader filters a shared segment's entries to the child's range as it builds
  the index, so a child serves only the keys it owns.

- Sharing is temporary and self-healing. When a child later compacts, it merges
  the shared segments into a new segment it writes under its *own* directory and
  drops the cross-partition references. Once no live manifest names a parent
  segment, the sweep reclaims it under the cross-partition liveness rule above.

## Consequences

**The orphan sweep becomes cross-partition, and this is the dangerous part.**
Issue #100's sweep deletes objects under a partition's prefix that the
partition's own manifest no longer references, after a grace period. With shared
segments, an object under the parent's prefix may still be referenced by a
child's manifest under a different prefix. A sweep that judged liveness from one
prefix's manifest would delete a segment a child is serving from — data loss
through the GC path. So liveness is now keyspace-wide: before deleting an object
under any prefix, the sweep confirms no live partition in the keyspace
references it, and it fails closed (retains) when it cannot load the manifests
it would need to be sure. Additionally, a partition being split freezes its own
flush, compaction, and sweep for the duration of the split, so it cannot drop or
delete a segment its about-to-exist children already reference.

**ADR 0002 is preserved.** A child continues the parent's Lamport sequence; it
inherits the parent's committed horizon and allocates above it. No key's version
is reissued or moved backward by a split, because the records themselves are the
same objects with the same Lamports.

**ADR 0006 is extended, not contradicted.** Partitions remain an index over
immutable objects. The one sentence in ADR 0006's model that this changes is the
implicit assumption that each object belongs to exactly one index; now an
immutable object may back more than one index, which is if anything more true to
the "immutable objects" premise than copy-on-split would be.

**A retired parent's directory is not eagerly reclaimed.** After a split
completes the parent partition is gone from the map, so no node holds it and the
per-owner sweep never visits its directory. Its shared segments stay alive by
reference from the children; its manifest object and anything the children have
since compacted away leak until a keyspace-level reclamation pass exists. That
pass is deferred: leaking a manifest object is cheap and safe, and building it
before the cross-partition liveness rule is trustworthy would be building the
dangerous half first.

## Alternatives considered

**Copy-on-split.** Correct and simple, and rejected for cost: it rewrites the
whole partition at the worst possible moment and doubles its stored bytes until
compaction catches up. The whole point of ADR 0006's index-over-objects model is
that this copy is unnecessary.

**Absolute (bucket-root-relative) segment names in every manifest.** This would
make any object referenceable from anywhere without a source field, but it
throws away the property that "everything a partition owns sits under its
prefix", which the sweep and every operator's mental model rely on. Keeping
names relative and adding an *optional* source for the exceptional case keeps the
common case exactly as it was.

**Reusing the parent's partition id for one child.** Ids are never reused in
Orbita (`crates/orbita-control/src/state.rs`), because a request naming a
partition that no longer exists must be detectable rather than silently answered
by whatever took its place. A split producing two genuinely new ids keeps that
invariant.
</content>
</invoke>
