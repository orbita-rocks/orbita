# 0003: Conditions evaluate against pending writes

Status: Accepted, 2026-08-03.

Extends [ADR 0001](0001-linearizable-reads-from-replicas.md), which described
the write path without saying how a second write to the same key behaves while
the first is still in flight.

## Context

ADR 0001 put the client acknowledgement before the apply to RocksDB, because
what makes a stale read impossible is the invalidation, not the apply. That
ordering is right, and it opens a window that the record did not address.

The owner's sequence for a write is: evaluate the condition against local
state, assign a Lamport, append to the log, replicate, acknowledge the client,
then apply to RocksDB. Between the evaluation and the apply, the write exists
in the log but not in the storage engine. A second write to the same key
arriving in that window would evaluate its condition against a RocksDB that
does not yet know about the first one, and would therefore reach a conclusion
that is simply wrong. Two compare-and-swap operations against the same version
could both succeed.

The obvious fix is to serialise: hold the key's write lock from evaluation
through apply. It is correct and it costs more than it looks. The lock is now
held across a replication round trip, so a partition can commit one write per
round trip. At a 2ms round trip that is roughly 500 writes per second per
partition. A large keyspace has enough partitions to absorb that, but a small
deployment does not, and a fresh cluster starts with exactly one partition. The
system would be slowest in the configuration a new user tries first.

## Decision

The owner evaluates conditions against committed state overlaid with its
pending writes, and never holds a lock across replication.

The owner already keeps a set of in-flight keys, because ADR 0001 needs it to
know which invalidations are outstanding. That set is extended to carry the
record each pending write will produce, not just the key and Lamport. A
condition is then evaluated against the pending record if there is one, and
against RocksDB otherwise.

**This is safe because the log has no holes.** The write-ahead log guarantees
that a replica's log is a prefix of the owner's, never a prefix with gaps, so
entry N+1 can only be durable if entry N is. Evaluating N+1 against N's effect
can therefore never be contradicted later: if N+1 survives, N survived too. The
no-holes property was chosen to keep recovery simple, and it turns out to be
what makes pipelined condition evaluation correct. That is worth noticing,
because if anyone later argues for allowing holes to improve throughput, this
is a second thing that would break.

**The storage engine's `put` and `delete` are demoted to single-node and test
use.** They fuse evaluation and commit under one lock, which is the right shape
for a caller that is the only writer and the wrong shape for the owner. The
owner reads through `get`, decides against its own overlay, and commits through
`apply`. No new storage API is needed, which is the main reason to prefer this
over teaching the engine about pending state.

## Consequences

**Write throughput is no longer capped by the replication round trip,** which
was the point. Pipelining is limited by the log and the network rather than by
condition evaluation.

**The owner holds values in memory for in-flight writes,** not just keys. The
set is bounded by how many writes are in flight, so it is small when things are
healthy and its growth is a useful signal when they are not. It is worth a
metric.

**The pending set is owner-local and must not be rebuilt after a failover.** A
promoted owner starts with an empty one, which is correct: anything the old
owner had pending was either durable, in which case the log replays it, or not,
in which case it was never acknowledged and must not be resurrected.

**Condition evaluation now has two sources and can disagree with itself if the
overlay is wrong.** This is a correctness-critical path with no natural test
coverage from single-node testing, so it needs the simulator: concurrent
conditional writes to one key, under failover, checked for the property that at
most one compare-and-swap against a given version ever succeeds.

## Alternatives considered

**Hold the write lock across replication.** Simple, obviously correct, and caps
a partition at one write per round trip. Rejected because it makes a
single-partition cluster, which is what everyone starts with, the worst case.

**Evaluate optimistically and abort on conflict.** Let both writes proceed and
detect the conflict at apply time. That means acknowledging a write that later
turns out to have failed its condition, which is not something we can take
back.

**Teach the storage engine about pending writes,** so `put` stays usable by the
owner. The engine would need to know about replication to know when a pending
write resolves, which is precisely the knowledge the crate is designed not to
have.
