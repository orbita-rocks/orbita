# 0007: Large values are their own objects

Status: Accepted, 2026-08-03.

Extends [ADR 0006](0006-partitions-are-an-index-over-immutable-objects.md).
Raises the value size ceiling from 256KB to 10MB, per keyspace.

## Context

A value size of 10MB is a requirement, and 256KB was chosen when the storage
engine was an LSM. Under an LSM the objection was write amplification: leveled
compaction rewrites a value through every level, measured at 16 to 25 times, so
a 10MB value costs a couple of hundred megabytes of rewriting over its life.
The usual fix is key-value separation, meaning the LSM holds a pointer and the
value lives elsewhere, which is what RocksDB's BlobDB does.

ADR 0006 removed the LSM. Segments are immutable and an exact in-memory index
means a lookup never consults more than one of them, so compaction exists only
to reclaim space. That takes most of the objection away on its own, and it
makes the separation trivial rather than a bolted-on feature, because
everything is already an object.

What remains is that 10MB is large relative to almost every other number in the
system: a write-ahead log entry, a network round trip, a cache entry, and a
list page were all sized when a value could not exceed 256KB.

## Decision

**A value above a threshold is stored as its own object.** The index holds a
reference rather than an offset into a shared segment. Small values continue to
be packed into segments, because an object per key would turn one request into
thousands and object storage charges per request.

**A large value is written to object storage before it is committed, and the
log entry carries only its reference.** So the write path for a large value is:
put the object, then run the ordinary replicated commit with a reference-sized
entry. Consequences worth stating plainly:

- The log stays small. Replication does not ship 10MB to three nodes, a batch
  of large writes does not produce an enormous fsync, and the existing segment
  and frame sizes stay sane.
- Durability is stronger, not weaker. The bytes are in object storage before
  anything is acknowledged, so a large value never exists only in memory.
- The latency is different in kind. A large write costs an object storage PUT,
  which is tens of milliseconds, rather than a WAL round trip measured in
  single digits. The requirements state this as its own number rather than
  pretending one figure covers both.
- A crash between the put and the commit leaves an unreferenced object. The
  manifest knows which objects are live, so collecting orphans is a sweep, and
  it has to exist rather than being assumed.

**Large values are not cached like small ones.** A 10MB value admitted to the
hot set evicts thousands of small ones, which is a bad trade for a store whose
read path is built on the small ones being resident. Large values get their own
bounded budget, and the default for that budget is small.

**The ceiling is per keyspace and it is not the default.** 10MB is available to
a keyspace that asks for it. The default stays far below, because a tenant
storing 10MB values should be making that choice rather than discovering it.

## Consequences

**The gRPC message limit becomes a client-visible problem, and it is the
sharpest edge here.** Tonic defaults to 4MB for decoding, and so does every
generated client in every language. Nothing in the workspace configures it
today. Raising the server limit is one line; the problem is that a client must
raise its own too, and a client that does not gets a confusing failure about
message size rather than anything about Orbita. This directly weakens the dumb
client property that the Python end-to-end suite was written to verify.

Two things follow. The server configures its limits from the largest configured
keyspace ceiling. And the limits become discoverable over the API rather than
living only in Rust source, which the end-to-end suite already flagged as a gap
for a different reason.

**List pages must be bounded by bytes, not by entries.** A thousand entries at
10MB each is a ten gigabyte response. The count limit stays as a second bound,
but the byte bound is the one that matters now.

**The pending overlay cannot hold values.** ADR 0003 has the owner keep each
in-flight write's resulting record in memory so conditions can be evaluated
against it, and it currently stores and clones a whole record. At 10MB that is
a memory fault waiting for a burst of concurrent writes. For large values the
overlay holds the reference, which is possible only because the object is
already written by the time the overlay is populated.

**Splits get a third trigger.** A partition of a thousand 10MB values is ten
gigabytes with a trivial index, which the byte threshold catches. A partition
of a million small values has a large index, which ADR 0006 already added a
trigger for. Neither catches a partition that is fine on both counts and slow
for another reason, but that is load-based splitting and it remains out of
scope.

**Object storage request cost becomes visible.** One object per large value
means one GET per cold read of one. That is the right trade against packing
them into shared segments and re-reading megabytes to serve one key, and it is
a real line on a bill that small values do not produce.

## Alternatives considered

**Pack large values into segments like everything else.** Uniform, and it makes
a cold read of one key fetch a segment sized for many, or forces range requests
and an offset index that is most of what separation gives you anyway.

**Replicate large values through the log like small ones.** No new write path
and no orphan sweep. It ships 10MB to three nodes, produces fsyncs sized for
the largest value rather than the typical one, and puts the value in memory on
three machines at once.

**A streaming write RPC** so a large value never appears as one gRPC message,
which would keep every client's default limits working untouched. This is the
most attractive rejected option, and it is rejected for now because it adds an
API shape that clients must understand, which is a worse dumb client regression
than a documented limit. Worth revisiting if the message limit proves to be a
recurring support problem.

**Store large values outside Orbita entirely,** with the store holding a
pointer the application resolves. Cheapest by far, and it pushes consistency
between the pointer and the blob onto every application that does it.
