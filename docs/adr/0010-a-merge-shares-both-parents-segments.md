# 0010: A merge shares both parents' segments in place

Status: Accepted, 2026-08-08.

Extends [ADR 0009](0009-a-split-shares-the-parents-segments.md) from one source
partition to two. It also fixes how the version rule in
[ADR 0002](0002-key-versions-are-partition-lamports.md) applies when two
independent Lamport sequences become one.

## Context

A merge replaces adjacent partitions A and B with one partition M. Copying
both partitions would make a routine scaling operation rewrite all of their
data. ADR 0009 already rejected that cost for splits and made immutable segment
objects shareable across partition manifests.

The harder part is not storage. A and B accept writes independently, issue
read leases independently, and allocate Lamports independently. They can both
have issued Lamport 40, including to two live keys. Retiring either parent
before both final prefixes are durable in M loses an acknowledged write. Starting
M from either horizon can reissue a version from the other side.

## Decision

A merge is a worker-prepared lifecycle transition over two parents.

- The parents must be distinct, exactly adjacent, in one keyspace, serving on
  the same owner and replica set, and free of another split or merge. Requiring
  one holder set gives the operation one place to make the dual-parent cut. A
  later protocol can move one parent first if cross-owner merge is needed.
- One generation names both parent ids and epochs, the merged child id, the
  boundary, and the combined range. Every preparation acknowledgement carries
  that complete identity. An acknowledgement delayed from an abort cannot
  prepare a retry.
- Both parents close write and lease admission and freeze flush, compaction,
  and sweep before either WAL is quiesced. Both read-lease sets drain, including
  the conservative interval reconstructed after a same-epoch restart. Both
  WALs then settle to their committed prefixes.
- M keeps every existing record version. Its committed horizon is
  `max(A committed, B committed)`, and its first write is strictly above that
  value. Two inherited keys may have the same version. This is the correction
  in ADR 0002, not a collision to rewrite.
- M's manifest references immutable segments from both parents in place.
  References are flattened to the physical source partition and deduplicated
  by `(source partition, object name)`, because a prior split can leave both
  parents referring to the same object. No segment bytes are copied.
- Completion is one replicated map change that removes A and B and installs M.
  It is refused until every holder has reported M durable and servable.
- Aborting raises both parent epochs in the same replicated entry that removes
  the intent. A holder recreates each irreversibly quiesced WAL and fences its
  replicas before reopening admission. A fence, transfer, or replica-set change
  on either parent aborts the merge and raises the other parent's epoch too.
- Shared objects stay live while any manifest references them. M's compaction
  rewrites both source sets under M, relocates external values with their
  source provenance, and drops the shared references. Source objects become
  collectible only after the last reference disappears.

## Consequences

The merge costs no object copy on its critical path. Its unavailable window is
the dual lease drain, WAL settle, and manifest publication. That cost is
deliberate: allowing either parent to keep accepting writes would create a
tail the merged child cannot inherit safely.

The common placement requirement narrows which adjacent pair an operator can
merge. I considered coordinating two owners, but that needs a distributed cut
between data-plane writers and adds another failure protocol before it adds a
new user-visible capability. Placement can make a pair compatible first, which
is the smaller safe design.

Retired parent ids are never reused. A stale route therefore cannot resolve to
M by accident. Parents remain gated until the worker observes a coherent map
and lifecycle snapshot, then reconciliation removes them. A stale request is
refused rather than served from retired data.

## Alternatives considered

**Rewrite every record and assign globally unique versions.** Rejected because
it invalidates every compare-and-swap token clients hold. ADR 0002 explicitly
permits equal versions on distinct inherited keys.

**Choose the lower parent's Lamport sequence.** Rejected because B may have
issued a greater number. M would then move backward and eventually reissue B's
version.

**Copy both parents into a new segment.** Correct, but it rewrites the entire
pair while both are frozen. The manifest already has the source-reference
primitive, so paying that cost would discard the reason ADR 0009 exists.
