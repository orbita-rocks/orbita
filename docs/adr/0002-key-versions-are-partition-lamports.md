# 0002: Key versions are partition Lamports

Status: Accepted, 2026-08-03.

Supersedes the version allocation decision recorded in the `orbita-storage`
module documentation, which gave each key its own counter starting at 1.

## Context

Every value carries a version, and clients compare against it to do
compare-and-swap. That is the primitive locks, leases, and catalog pointers are
built from, so it is the primitive the wedge use case depends on.

The storage engine was first built with a per-key counter: a key's first write
is version 1, its second is version 2, and so on. The reasoning given was that
deriving versions from the partition's Lamport timestamp would make a client's
expected version move whenever an unrelated key was written, so a
compare-and-swap retry loop would spin under load through no fault of the
caller.

That reasoning does not survive inspection. If a key's version is the Lamport
at which that key was last written, then writing some other key advances the
partition counter but does not touch the record stored under this key. The
version a client is holding stays valid until someone writes the key the client
cares about, which is exactly the semantics compare-and-swap needs. There is no
spinning.

Meanwhile the per-key counter has a real defect that we found by asking a
different question. Versions restart at 1 once a key's tombstone is reclaimed,
which happens a day after deletion. So a client that read key K at version 1,
paused for longer than that, and came back can successfully compare-and-swap
against a completely different K that also happens to sit at version 1. That is
an ABA problem, and it lands squarely on locks and leases, which are the things
we are telling people to build with this.

[ADR 0001](0001-linearizable-reads-from-replicas.md) then gave a second, larger
reason. Replicas learn about writes through per-key invalidation messages, and
a replica has to be able to tell when it has missed one. A single monotonic
sequence per partition makes a missed message visible as a gap. Per-key
counters cannot do this, because learning that "abc" moved to version 2 says
nothing about whether an invalidation for "xyz" went missing.

## Decision

A key's version is the partition Lamport at which that key was last written.

Versions are therefore monotonically increasing and never reissued. They are
also sparse from any single key's point of view: a key might go from version 47
to version 112 to version 900 as other keys are written in between.

See the correction below on what "unique" does and does not mean here.

Sparseness is not a problem because a version is an opaque token for
comparison, not a count of anything a user cares about. etcd, which is the
system Orbita is positioned against, does the same thing: its `mod_revision` is
a cluster-wide revision rather than a per-key counter, and that is what people
compare against in transactions.

## Correction, 2026-08-03

The first version of this record claimed versions are "unique within a
partition". That is too strong, and it was caught while implementing the change
rather than after merge shipped, which is the good outcome.

A merge breaks it. Two partitions that split from a common parent at Lamport
500 each hold live records with versions below 500, assigned independently
after the split. Merge them and the merged partition legitimately contains two
different keys sitting at version 300, one inherited from each side. Nothing is
wrong; the two halves simply used the same numbers for different keys while
they were separate.

The invariant that actually matters is narrower: **a version is never
reissued**, meaning no key ever sees a version it has already had, and the
partition never hands out a number it has handed out before. That is what
compare-and-swap needs, since a comparison is always against one key. It is
also what the gap detection in
[ADR 0001](0001-linearizable-reads-from-replicas.md) needs, since that reads
the ongoing invalidation sequence rather than the versions sitting in stored
records.

The alternative was to rewrite every record's version during a merge so that
uniqueness held. That invalidates every version token every client is holding,
turning a routine background operation into a cluster-wide surprise for
application code. Not worth it for a property nothing depends on.

So: monotonic and never reissued, and two distinct keys in a merged partition
may share a number. A merge must still produce a forward sequence that exceeds
everything either side had issued.

## Consequences

**The ABA disappears.** A version that was valid once and then reclaimed can
never reappear, so a stale compare-and-swap fails rather than silently
succeeding against a different key.

**Missed invalidations are detectable,** which ADR 0001 depends on.

**WAL entries no longer need a separate version field,** since the Lamport they
already carry is the version. The log gets slightly smaller and one class of
disagreement between two counters stops being possible.

**Version numbers are not a write count.** Anyone who wants to know how many
times a key has been written needs to track it themselves. No use case we have
needs this.

**A version is only meaningful within its partition.** Comparing versions
across partitions is meaningless, and the API should not encourage it. Since
compare-and-swap is single-key and a key lives in exactly one partition, this
never comes up in practice, but it belongs in the documentation.

**Splits and merges must preserve the Lamport ordering,** because versions now
inherit whatever that ordering does. A merge in particular has to produce a
Lamport sequence that does not go backwards for any key, or a client's held
version could become invalid through no write of its own. This is a constraint
on `docs/plan/03-control.md`, and the merge design has to state how it is met.

## Alternatives considered

**Per-key counters,** which is what was built first. Rejected for the ABA and
for being unable to support gap detection.

**Per-key counters plus a separate Lamport on every record,** so both are
available. This works and it stores two numbers where one will do, then invites
every future reader to wonder which one is authoritative. The one number does
both jobs.

**A globally unique version, such as a UUID or a hybrid logical clock
timestamp.** Unique, and it gives up ordering within a partition, which is what
gap detection needs.
