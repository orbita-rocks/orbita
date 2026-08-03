# 0006: Partitions are a memory-resident index over immutable objects

Status: Accepted, 2026-08-03.

Replaces RocksDB as the storage engine. Supersedes the storage half of the
write path in `docs/REQUIREMENTS.md`, which said compacted SSTs are uploaded to
object storage. The replicated write-ahead log is unchanged.

## Context

The storage engine has been RocksDB, with its SSTs pushed to object storage.
That works and it has two problems worth solving.

The first is that RocksDB's format is a single-implementation format. It is not
proprietary, it is Apache and BSD licensed, but nobody writes an independent
reader for it, it is coupled to RocksDB's own versioning, and it assumes
low-latency local disk. Data at rest in an Orbita cluster is therefore
reachable only through Orbita.

The second is that it caps our correctness claim. RocksDB performs its own file
I/O beneath `orbita_runtime::Disk`, so deterministic simulation cannot inject
faults inside it and treats it as a trusted component. The honest version of the
claim has been "the distributed layer is verified under simulation", which is
weaker than what the project is being sold on.

The lesson worth taking from Iceberg is not that everything belongs in object
storage. It is that specifying the at-rest format and the commit protocol is
what lets anything else read your data. That is separable from write latency,
and Iceberg says nothing about ingest speed.

## Decision

A partition is a memory-resident index over immutable objects in object
storage, plus a mutable table of recent writes.

### The pieces

The bytes are specified in [docs/format](../format/), which is the artifact
that makes any of this worth doing. A format nobody outside this repository can
implement against is a single-implementation format with extra steps.

**Segments** are immutable objects holding a sorted run of records. They are
the unit of storage, they are never modified after they are written, and their
format is specified and versioned independently of any implementation.

**The index** maps every key in the partition to the segment and offset holding
its current record. It is fully memory-resident, and that single property is
what keeps the format simple: an exact in-memory index needs no bloom filters,
no block cache, and no level structure, which is most of what an LSM is for.

**The mutable table** holds writes that have been acknowledged but not yet
written into a segment. It is a sorted map, and it is what a read consults
first.

**The manifest** names the live segments and the log position they cover. It is
the atomic pointer, swapped by a compare-and-swap on the object store, which
`ObjectStore::put_if` already provides.

**Values are cached, not resident.** The hot set stays in memory; a read that
misses fetches the record from its segment. This is what lets a partition hold
more than a node's memory.

### The write path

Unchanged through the log, which is the point. A write goes to the replicated
write-ahead log, is acknowledged once it is durable on two of three replicas,
and is applied to the mutable table. The no-lost-write guarantee and the
existing latency target survive intact because none of that touches object
storage.

Flushing turns the mutable table into a new segment and swaps the manifest. It
happens on a size or time trigger, and on explicit request. So there are two
durability levels a client can ask about: replicated, which is the default and
survives losing a node, and flushed, which survives losing the cluster. A
client that needs the second asks for it and waits.

### Compaction

Merging sorted runs to reclaim records that newer segments supersede. There is
no level structure and no size-tiered policy, because with an exact in-memory
index there is nothing to gain from one: a lookup never consults more than one
segment, so the only reason to merge is to reclaim space.

## Consequences

**Capacity scales by adding workers, and hot capacity is the cluster's
memory.** Partitions split and distribute, so the index distributes with them
and nothing about it caps the cluster. What it does cap is one partition: an
index has to fit on the node that owns it, which becomes one of the triggers
for a split alongside size.

The residual effect is a sizing one rather than a limit. Memory holds both
indexes and cached values, so a keyspace of small values yields less byte
capacity per gigabyte of memory than the value size alone implies. That belongs
in the operator documentation as guidance on choosing machines, not in the
requirements as a ceiling.

**Reads have two latencies now, and the requirements need both.** A hot read is
a memory lookup and should be well inside the current target. A cold read pays
one object storage round trip, which is tens of milliseconds on standard object
storage and single-digit milliseconds on the express tiers. Publishing one
number for both would be a lie in whichever direction it is wrong.

**Deterministic simulation becomes complete.** With no RocksDB, every byte the
system persists goes through `Disk` or `ObjectStore`, both of which the
simulator implements and injects faults into. The claim becomes "the system is
verified under simulation" rather than "the distributed layer is", and the
caveat in `docs/plan/05-sim.md` can be deleted rather than explained.

**Large values stop being a problem.** A value is bytes in a segment rather than
something compaction rewrites through every level, so raising the size limit
becomes a memory and network question rather than a write amplification one.

**The data becomes readable by other things.** A specified format plus a
specified manifest means a reader in any language can open a snapshot from
object storage without going through Orbita. That value is real and it arrives
later than the engineering does, because it depends on somebody writing that
reader.

**Eviction is new, and it is the cost of the choice.** A fully memory-resident
partition would need none. Choosing a hot set means choosing a cache policy, a
memory budget per partition, and a way to behave sanely when the budget is
exceeded. This is the most likely place for the design to go wrong, and it is
where the simulator should be pointed once the basics work.

**Recovery loads an index, not a dataset.** A restarting node reads the manifest
and enough of the segments to rebuild its index, then serves. That is
proportional to key count rather than to bytes, which keeps the readiness gate
from growing with the data.

**RocksDB is removed.** It appears in one crate and one file today, and
`Partition` is about ten methods, so the replacement is contained. Nothing in
the write-ahead log, the control plane, the routing layer, or the read path
knows the engine exists.

## Alternatives considered

**Keep RocksDB.** It works, it is battle-tested, and it costs the open format
and the complete simulation claim. Battle-tested matters, and this is the one
alternative that deserves more respect than it is getting here.

**Fully memory-resident partitions, no faulting.** Simpler than what we chose:
no eviction, no cold read path, no second latency number. It caps capacity at
roughly a terabyte across a small cluster, which is still far past etcd, and it
was rejected because the capacity headroom was judged worth the cache.

**Adopt SlateDB.** An object-storage-native LSM in Rust, Apache-2.0, with
production users. The right answer if data must exceed memory by a wide margin,
and unnecessary here: an LSM's structure exists to answer lookups without an
exact index, and we have one.

**Object-storage-primary writes,** acknowledging only once durable in object
storage. Genuinely simple and it puts a floor of tens of milliseconds under
every write, which ends the coordination use case the product is positioned
around.
