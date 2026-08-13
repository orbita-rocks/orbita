# 0013: Read serving is decoupled from the durability quorum

Status: Accepted, 2026-08-13.

Proposed 2026-08-12 and revised twice before acceptance: once to correct a wrong
claim about the coherence quorum and say what followers cost the write path, and
once to record that the missing value cache outranks every other precondition
here. Both corrections came from measurement rather than review, which is the
argument for measuring a decision before freezing it.

Extends [ADR 0001](0001-linearizable-reads-from-replicas.md), whose read
protocol this keeps unchanged and whose population it widens. Narrows the
placement assumption in
[ADR 0006](0006-partitions-are-an-index-over-immutable-objects.md), which says
partitions split and distribute so the index distributes with them. They do not,
and this is why.

## Context

A keyspace lives on exactly `replication_factor` nodes and always will. Measured
on a six-node cluster running one keyspace split eight ways:

```
partitions HELD / node: {1: 8, 2: 8, 3: 8, 4: 0, 5: 0, 6: 0}
```

Three ceilings follow, and they are one ceiling wearing three hats:

- **Read capacity.** Only a holder may serve a read, so reads cap at `RF` nodes.
- **Write capacity.** Only a holder may own, so writes cap at `RF` nodes.
- **Dataset size.** The index is memory-resident and every holder holds the
  index for every partition of the keyspace, so a keyspace is bounded by one
  node's memory. At a measured 5–8% index-to-data ratio, a terabyte needs
  50–80 GiB of RAM on every holder.

The cause is that the map's replica list does three jobs at once: it is the WAL
durability quorum, it is the set allowed to serve reads, and it is the set
eligible to own. Splitting distributes ownership within that set — which is what
issue #160 delivered — but the set itself never widens, because it is defined by
the durability quorum and a quorum has no reason to grow.

ADR 0006 assumed the opposite: "partitions split and distribute, so the index
distributes with them." The index distributes across partitions and not across
nodes, so the mitigation that ADR relies on is not delivered.

The workload makes this sharper rather than softer. Orbita is multi-tenant, so
there will always be far more partitions than nodes, most of them cold. The
operating problem is not "spread work evenly" but "move a hot tenant onto
hardware that can serve it", which needs many small partitions and cheap
migration — neither of which helps while every partition of a keyspace is
pinned to the same three nodes.

## Decision

**Serving a read and holding a durable copy become separate obligations.**

- The durability quorum stays exactly as it is: `replication_factor` holders,
  a 2-of-3 WAL quorum, unchanged in size, membership rules, and promotion
  criteria. Nothing about how a write is made durable changes.
- Any node may additionally become a **follower** of a partition: it opens the
  index from the object store, subscribes to the owner's invalidation stream,
  and serves reads under a lease. A follower carries no durability obligation
  and is counted by no durability quorum. It **is** counted by the coherence
  quorum, and an earlier draft of this record claimed otherwise. That claim was
  wrong, it was load-bearing, and "What unbounded followers cost the write
  path" below is what replaces it.

**Consensus tracks ownership, and little else about placement.** Ownership is a
lease with a short life, relinquished after idleness and taken by whoever writes
next. A partition with no writer has no owner, and a partition with no owner is
immutable, which is the cheapest thing a distributed system can offer.

**A follower is unbounded in number and in lifetime. It is not best-effort about
correctness.** The distinction is the whole of this ADR and it is stated in full
below.

**Cache validity keys to the segment and manifest version, never to the
ownership epoch.** A change of owner changes who may append. It changes no
bytes, so it must not invalidate a single cached index anywhere.

## What a follower may and may not answer

ADR 0001 already settled the hard part and this changes none of it. Its
invalidation is per key, for a reason it states plainly: a partition-wide
comparison makes a replica behind for every key in the range as soon as anything
in the range is written, so "the read fan-out disappears exactly when the
cluster is busy enough to need it". Per-key invalidation is what makes a
follower useful under sustained write load rather than useless.

A follower answers a read only when it can prove that key is current: it holds a
live lease and has received every invalidation the stream owes it. The stream
carries Lamports and a follower knows the Lamport the next invalidation must
carry, so a gap is detectable rather than silent.

When it cannot prove that, it has exactly three permitted behaviours, and
serving is not among them:

1. **Forward to the owner.** Always correct, costs one in-cluster hop, and is
   the default.
2. **Wait for the stream to catch up, within a deadline, then serve.** Correct
   because the proof arrives before the answer does. Bounded so that a stalled
   stream cannot hold a client.
3. **Refuse.** Correct, cheapest, and an acceptable first implementation.

A follower that has lost stream continuity refuses for the whole partition until
it re-establishes, because a missed invalidation names no key and therefore
impeaches every key. Recovery is a re-read from the object store, which is why
the two freshness paths below both exist.

**The failure mode this creates is a load one, and it is named here so that it
is designed for rather than discovered.** Forwarding is correct and
anti-scaling: it concentrates reads on the owner exactly when the cluster is
busy. Per-key invalidation is what keeps that rare, and a follower that cannot
keep up should stop being a follower rather than forward every read through the
owner it is already failing to track.

## Two ways to stay current

A follower has two paths to freshness with different costs, and the cheap one
failing degrades to the expensive one rather than to incorrectness.

- **Incremental, from the WAL.** The owner's stream carries individual writes.
  A follower applies them to its cached view. This is low latency and
  proportional to the write rate.
- **Complete, from the object store.** Re-read the manifest and rebuild. This is
  proportional to the partition, is always available, and needs nothing from the
  owner. It is the recovery path for a lost stream, a cold start, and a follower
  that has fallen too far behind to catch up incrementally.

Following the WAL for freshness and holding the WAL for durability are then
different obligations over the same stream. A follower may lag, drop, or vanish
and nothing durable depends on it. A quorum member may not. That asymmetry is
what lets followers be unbounded while the quorum stays small and fixed.

Compaction is a third signal and must not be confused with the second. An
invalidation says a key's current record moved. Compaction says a segment a
follower has cached is no longer referenced at all. They arrive differently and
a follower has to tell them apart, or it will hold a segment that is correct and
unreachable, or discard one that is still live.

## What unbounded followers cost the write path

The owner enumerates its lease holders today and blocks on them. `LeaseTable`
is a `HashMap<NodeId, u64>` of grants (`crates/orbita-server/src/lease.rs`),
`holders_with_expiry` returns the set a write must hear from, and
`await_coherence` (`crates/orbita-server/src/host.rs`) runs on the
acknowledgement path: a write is acknowledged once every live lease holder has
confirmed the invalidation or its lease has lapsed.

So a follower that holds a lease is enumerated, and every write pays for it.
Reads scale with follower count and writes anti-scale with it, in fan-out per
write and in tail latency, which becomes the slowest follower's. A follower
that is merely slow costs each write one lease duration.

This is not a gap in the implementation that a better protocol closes. A cached
copy can be proven current in exactly two ways, and there is no third:

- **Invalidate.** The writer tells the cache before it acknowledges. Reads then
  cost no round trip, and the writer must know every reader.
- **Validate.** The reader asks at read time. Writes then cost nothing in
  readers, and every read pays a round trip to whatever holds the truth.

ADR 0001 chose invalidation and bought linearizable reads with no round trip in
them. The bill for that choice is denominated in readers, and this record was
written as though the bill would not arrive.

turbopuffer is the same system built on the other choice, and its published
numbers are what the trade costs: any query node may serve any namespace, no
node is enumerated, and a strongly consistent query pays one object-store round
trip to check the commit point — p50 14ms warm, against under 10ms when the
caller opts into eventual consistency. Unbounded readers, and a floor set by
the validation round trip.

Asking for unbounded followers *and* zero-round-trip linearizable reads is
asking for a coherent cache that nobody pays coherence for. This record has to
pick, and it picks both — explicitly, in two tiers.

## The decision this forces: readers come in two tiers

**A bounded, enumerated tier.** Lease holders, counted by the coherence quorum,
serving reads with no round trip. Its size is a configured number rather than
the replication factor, which is the part of this ADR that survives intact and
which `read_replica_target` already implements. Writes pay for this tier, so
its size is a write-throughput decision, and it must have a ceiling.

**An unbounded, unenumerated tier.** Readers that hold no lease, are counted by
nothing, and validate per read against the owner before answering. Writes pay
nothing for them and they may come and go freely. They pay one in-cluster hop
per read, which is the same hop as forwarding except that the value never
crosses it twice and the owner never touches storage to answer.

**The validating tier's protocol is not designed here, and nothing implements
it.** What this record fixes is that the tier exists, that it is the answer to
unbounded readers, and that it is where a reader goes when it will not be
counted by a write. The wire format of a validation, how a reader learns the
Lamport to validate against, and how validations batch are all open, and the
bounded tier is useful without any of it. Accepting this record commits to the
shape and not to a protocol.

The tier a node is in is a provisioning decision, not a durability one, and a
node may move between tiers without any write noticing. That is what makes
enlisting compute cheap, which was the point of this record; what changes is
the admission that the cheap tier is the one with a round trip in it.

## Fencing an owner with unbounded followers

The unbounded tier needs no fencing at all. It holds no lease and proves
nothing on its own: it validates against the owner per read, and an owner that
has been fenced cannot answer a validation. Readers that cache nothing they are
allowed to trust are readers a fence can ignore.

The bounded tier is enumerated, so a fence *could* ask it to acknowledge, and
should not have to. Leases are time-bounded and a fence already waits out
`lease_duration + lease_margin` before a promoted owner accepts a write, which
is the mechanism that keeps a fence's cost independent of how many readers are
listening even though the owner could count them.

This is why the leases should be short. A short lease bounds the fence wait, and
the fence wait is the availability cost of every ownership change — and in a
model where ownership moves on idleness, ownership changes are ordinary rather
than exceptional. The revoke-and-confirm path stays available for the quorum,
where the membership is known and small.

## Consequences

**Read capacity stops being bounded by the replication factor.** Zero-round-trip
read capacity becomes bounded by the configured lease-holder count, which is a
provisioning decision rather than a durability one — but it is still a bound,
and it is paid for out of write throughput. Read capacity that tolerates one
in-cluster validation hop is bounded by nothing.

**Write throughput now has a term in it that reads control.** Every lease
holder is a confirmation a write waits on. That term did not exist while the
holder set was the replication factor, because the replication factor is not a
tuning knob operators reach for under read load. It is now, so the
lease-holder count needs a ceiling and a metric before it needs a default.

**Index memory distributes.** A node holds indexes for the partitions it owns or
follows, not for every partition of every keyspace it replicates. This is the
mitigation ADR 0006 assumed it had.

**Write capacity distributes with ownership**, because ownership stops being
confined to the durability quorum. It remains one owner per partition; what
changes is that the owners can be anywhere.

**Cold partitions become nearly free** — no owner, no lease, no resident index.
This is the property that makes a multi-tenant store with far more partitions
than nodes affordable.

**A cold follower costs a full index load.** Measured against real S3: 2.36s for
a 309 MiB partition, 1.30s for 114 MiB. Sub-linear because the cost is
`2 × segment_count` sequential ranged GETs rather than bytes. Two preconditions
below attack this directly.

**Ownership handoff costs an index load on the new owner**, the same 2.4s. In a
model where ownership moves on idleness, that is a latency cliff on the first
write after an idle period. Mitigations worth choosing between deliberately:
keep the index warm after relinquishing, load it lazily, or make the idle
timeout long enough that the cliff is rare.

**The control plane's job gets smaller and its data gets larger.** It tracks
ownership leases and ranges, not durability membership for read purposes. But
a multi-tenant cluster has far more partitions, and that lands on the one
ceiling `docs/REQUIREMENTS.md` admits is uncharacterised.

## Preconditions

Three measured costs are load-bearing enough that this decision should not be
implemented before they are addressed, because all three would otherwise be
blamed on it.

**There is no value cache, and it dominates everything this record claims.**
ADR 0006 decided that values are cached rather than resident. The caching half
was never built: `Partition::load` checks the memtable and on a miss goes
straight to `Partition::fetch`, which issues one `get_range` per record. Every
read of a flushed key is therefore one object-store round trip, and the holder
set cannot change how many round trips a read costs.

Measured twice, on EKS, against real S3:

| | reads/s | p50 | peak pod CPU |
|---|---|---|---|
| 2026-08-12, 3 holders → 5 | 9,131 → 9,001 | 24.7ms → 26.1ms | 110.3s → 65.6s |
| 2026-08-13, 3 holders → 5 | 9,374 → 9,602 | 25.6ms → 25.2ms | 92.0s → 47.1s |

The second run measured one keyspace under both conditions rather than two
keyspaces under one each. Both agree: throughput does not move, and load
distribution transforms. Peak pod CPU fell 41% and then 49%, total CPU across
five pods fell from 175.2s to 142.9s, and forwarding disappeared.

So the mechanism in this record works and is worth having. It buys no read
capacity until a read can be served without going to the object store. No read
in any run of either day returned faster than 14.4ms, at any thread count or
holder count, with the servers about 19% utilised. That floor is the round
trip, and it is what a reader has to stop paying before "read capacity follows
the holder set" can be true.

This is the precondition that outranks the two below. They are latency cliffs
on a cold path; this one sets the steady-state cost of every read.

**Heartbeat reporting is O(partitions), unconditionally.** `Node::progress`
walks every host on every heartbeat and reports five fields each, with no
filtering to what changed. At a 250ms interval a node holding four thousand
partitions ships four thousand records four times a second, and the leader does
that work per node forever, whether or not any partition is active. Cold
partitions are free everywhere except here. Reporting only what moved, with a
periodic full reconciliation, is the fix.

**A cold open is `2 × segment_count` serial round trips.** `Snapshot::build`
loops the manifest's segments and issues two ranged GETs each — footer, then key
index — sequentially, with no concurrency anywhere in the crate. That is what
makes a cold open latency-bound rather than bandwidth-bound, and it is what a
follower pays on every cold start. Fetching them concurrently is a contained
change with no format impact. Addressed: `Snapshot::build` now fetches segments
concurrently.

Beyond those, a materialised index — one object per partition, rebuilt
periodically, read alongside the few segments written since — would take a cold
open from `2N` requests to a small constant. That is a format change and a
separate decision, and the numbers above are what should decide it.

## Alternatives considered

**Raise the replication factor.** Scales reads linearly and costs nothing to
build, since `repair_replica_sets` would widen existing keyspaces onto new nodes
automatically. It does not scale writes at all: every holder writes every
record, so per-node write volume is `data_rate × RF / holders`, which is
`data_rate` for any RF. It also makes the index memory problem worse rather than
better, since more holders each hold the whole keyspace's index. Useful, and not
this.

**Give different partitions different holder sets.** Distributes reads, writes,
and index memory using only placement, with the durability model untouched. It
is strictly smaller than this ADR and delivers much of it. It was rejected as
the primary answer because it leaves read capacity for any single partition
pinned at `RF`, which is the wrong shape for a multi-tenant store where one hot
tenant is one partition. It remains a good intermediate step.

**Move the unflushed tail to the object store, removing the quorum entirely.**
The most complete answer: no holders, no quorum, any node serves anything. It
trades write acknowledgement latency for it — a conditional PUT rather than a
local fsync and a peer round trip — and the last EKS run concluded the write
ceiling was entirely fsync, so the trade may be smaller than it appears. Not
decided here. This ADR is deliberately compatible with it: if durability moves
to the object store later, followers do not change, because they were never
part of the quorum.

**Let followers serve stale reads under a staleness bound.** Rejected. It is a
different consistency model wearing the same API, and the value of the current
one is that a client never has to ask which it got. If bounded staleness is ever
wanted it should be a property a caller requests explicitly, not a thing that
happens to reads when the cluster is busy.

turbopuffer's numbers are the argument for that shape rather than against it.
Its strongly consistent query pays the validation round trip at p50 14ms and
its eventually consistent one skips it at under 10ms — the same engine, the
same data, and a caller who chose. What is being rejected here is not the
weaker model; it is the weaker model arriving unannounced.
