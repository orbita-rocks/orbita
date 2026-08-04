# Orbita

> Orbita is a strongly consistent, multitenant distributed key-value store. It
> stores partitions as immutable objects behind a memory-resident index, keeps
> the objects on object storage, and is built under deterministic simulation
> from day one.

This document is the product requirements for Orbita v1. It fixes the scope,
the guarantees, and the acceptance criteria. Sequencing and detailed design
belong to the engineering plan, not this document.

## Why this exists

Building distributed systems requires a coordination substrate: somewhere to
put locks, leases, epochs, catalogs, and control-plane state, with guarantees
you can actually trust. The incumbent in this role is etcd, and etcd has known
ceilings. It has a practical storage limit of around 8GB, a single Raft group
that every operation flows through, and no real multitenancy, so every team
that needs coordination ends up running its own cluster. The more capable
alternative, FoundationDB, brings full transactions but also brings an
operational model that is heavy if all you needed was consistent KV.

Orbita sits between them. It scales past etcd by splitting the keyspace into
partitions as it grows, it is multitenant so one cluster serves many teams and
systems, and it stays a KV store rather than growing into a database. The
headline claim is correctness you can verify: the entire system runs under
deterministic simulation testing, and we publish the fault-injection results.
For the people this is built for, infrastructure engineers deciding what to
bet a platform on, that evidence is the product.

## Positioning

The pitch: **the coordination substrate you can verify.** A strongly
consistent, multitenant KV store for people building distributed systems,
built under deterministic simulation from day one, with published correctness
results. It starts as one partition on your laptop and splits as it grows,
past where etcd stops.

- **Primary persona.** Infrastructure engineers building their own platforms:
  control planes, schedulers, catalogs, metadata services. They need a
  substrate to embed in an architecture, they read correctness reports before
  adopting, and they feel etcd's limits directly.
- **Positioned against etcd.** Same role, without the 8GB ceiling, the single
  Raft group, or the single-tenant assumption.
- **Positioned respectfully against FoundationDB.** FDB is more capable today
  if you need multi-key transactions, and v1 concedes that openly. The
  concession is now dated rather than permanent: strict-serializability
  transactions are a committed post-v1 direction (see Transactions below), and
  the long-term contrast with FDB becomes published correctness evidence
  rather than scope.
- **Not a cache.** Redis and Valkey trade durability and consistency for
  latency. Orbita makes the opposite trade. We do not compete on raw latency
  and should not invite that comparison.

## Use cases

1. **Coordination substrate.** Distributed locks, leader election, service
   registries, fencing tokens, epoch counters. This is the wedge use case.
   Version CAS, IF NOT PRESENT, and linearizable reads are the primitives
   these are built from.
2. **Metadata and catalog backends.** The atomic pointer-swap pattern:
   Iceberg-style catalogs, manifest stores, control-plane state. Modest data
   volume, read-heavy, and correctness-critical, which is exactly the shape
   Orbita optimizes for.
3. **Read-heavy application state with TTLs.** Session stores, feature flags,
   config serving, token stores. Served by replica reads and lazy expiry.

The known gap for the wedge use case is watch/subscribe, which is a v1
non-goal. It is first on the roadmap, and the PRD says so out loud because the
target persona will notice either way.

## Functional requirements

### API

The API is a small gRPC surface, deliberately simple enough that clients stay
dumb (see Protocol below).

- **SET(keyspace, key, value, options)** writes a value. Options: TTL, and a
  condition (IF NOT PRESENT, or IF version equals X).
- **GET(keyspace, key)** returns the value and its current version.
- **DELETE(keyspace, key, options)** removes a key, optionally conditional on
  version.
- **LIST(keyspace, prefix, cursor, limit)** returns an ordered page of keys
  and values matching a prefix, plus a continuation cursor.

Every value carries a version, which is the partition Lamport at which the key
was last written. Versions increase and are never reissued, so a
compare-and-swap cannot succeed against a key that was deleted and recreated
under an old version. See [ADR 0002](adr/0002-key-versions-are-partition-lamports.md),
including its correction on what uniqueness does and does not mean across a
merge.

Conditional writes (CAS on version, plus IF NOT PRESENT) are required, not
optional, because they are what make Orbita usable for locks, elections, and
catalog pointers. Writes for a key already serialize through one owner, so the
incremental cost is low and the payoff is the entire coordination use case.

### LIST semantics

Each page of a LIST is a consistent snapshot of a single partition's range. A
multi-page scan across partitions is not a point-in-time snapshot. I
considered snapshot-consistent full scans, but that pulls MVCC machinery into
v1 for a guarantee the target use cases rarely need, so we ship the honest,
cheap version and document it clearly. The eventual fix is already designed:
once snapshot timestamps land as the first stage of the transaction work (see
Transactions), a scan can carry one and a multi-page scan becomes
point-in-time. v1 ships without it.

### TTL

TTLs are stored as absolute expiry timestamps so replication and partition
splits cannot skew them. Expired keys are never visible to GET or LIST, and
are physically reclaimed by a background compaction sweep. The guarantee is
"never visible after expiry, reclaimed eventually." There is no bounded
reclamation window in v1.

### Consistency

All reads are linearizable, and replicas serve them so that read capacity grows
with the cluster rather than bottlenecking on one node per range.

A replica serves a read locally only while it holds a live read lease from the
partition owner, has not missed an invalidation, and has not been told that
this particular key is being written. Otherwise it forwards to the owner. The
owner invalidates individual keys by riding on the WAL replication it already
performs, so keeping replicas coherent costs no extra round trip in the healthy
case.

[ADR 0001](adr/0001-linearizable-reads-from-replicas.md) has the design and the
reasoning, including why a simpler per-partition watermark was rejected. The
short version is that a partition-wide freshness check makes a replica behind
for every key whenever any key is being written, so read fan-out collapses
under exactly the load it exists to absorb.

### Multitenancy

Tenancy is expressed as keyspaces. Each keyspace is an isolated, independently
partitioned key namespace with:

- **Credentials.** Clients authenticate and are scoped to keyspaces. This is
  table stakes for anything exposed on a network.
- **Quotas and rate limits.** Per-keyspace caps on storage and request rate,
  so one tenant cannot starve the others. This is the lesson every multitenant
  store learns eventually; we build it in from the start.
- **Config.** Per-keyspace defaults such as default TTL and max value size.

### Security

The first version of this document specified credentials and quotas and said
nothing about transport security or granular authorization, which had the
priorities inverted for a system asking to hold control-plane state. This
section fixes the requirements; ROADMAP.md schedules them, currently at
v0.4.0.

- **Encryption in transit, everywhere.** TLS on the client surface, and
  mutually authenticated connections between peers, completing what
  [ADR 0004](adr/0004-peer-traffic-uses-private-framing.md) starts.
  Certificate rotation must not require a restart, because coordination
  infrastructure is exactly the thing you cannot bounce casually.
- **Authorization finer than the keyspace.** Credentials grantable per key
  prefix within a keyspace, read or write. A substrate shared by teams needs
  the range-scoped permissions etcd users already expect, and arguably needs
  them more, since multitenancy is the pitch.
- **Audit logging.** Administrative and credential operations produce an
  audit record. The target buyer's security review asks for this by name.

Encryption at rest stays out of scope here: object storage backends provide
it, and the deployment docs should say how to turn it on rather than Orbita
reimplementing it.

## Architecture requirements

This section fixes the architectural commitments the product depends on. The
mechanics (wire formats, exact split protocol, timestamp propagation) belong
to the design doc.

### Topology

A cluster is a leader group plus workers, all running the same binary in
different roles.

- **The leader group** is 3 or more nodes running Raft. It owns the partition
  map, worker membership, keyspace metadata, split and merge decisions, and
  failover. I considered object-store CAS election and an external
  coordinator, but a Raft group is the battle-tested pattern here and gives
  fast failover. We adopt an existing Rust Raft implementation (openraft or
  raft-rs) rather than building our own; Orbita owns the storage and network
  traits underneath it, which is exactly the seam deterministic simulation
  needs anyway.
- **Workers** own partitions. Each partition has one owning worker that
  serializes all writes, plus two full replicas.

### Partitioning

The keyspace of each keyspace is range-partitioned. A cluster starts with a
single partition per keyspace and splits partitions at a boundary key as they
grow. Range partitioning is what keeps LIST cheap and makes splits metadata
operations rather than rehashing events.

- Splits are size-triggered.
- Merges of small adjacent partitions are in scope for v1. Merge is the
  trickiest coordination case in the system for the least immediate payoff,
  so it is flagged here as the highest-risk requirement; the engineering plan
  should sequence it accordingly.

### Write path

A write goes to the partition owner, which replicates a WAL entry to the two
replicas and acknowledges at 2-of-3. Replicas apply entries asynchronously to
their own in-memory table.

This hybrid (replicated WAL for latency, object storage for bulk durability)
is a deliberate middle path. Object storage alone puts a 50 to 100ms floor
under writes; local disk alone makes losing a machine a data-loss event. The
2-of-3 WAL gives single-digit-millisecond writes that survive the loss of any
one worker with zero data loss, and object-storage-resident data keeps workers
cheap to replace and rebalance.

There are therefore two durability levels a client can ask about. A write is
**replicated** by default, meaning it survives losing a worker, and that is
what an acknowledgement means. A client that needs the write to survive losing
the whole cluster asks for it to be **flushed** and waits for the segment to
reach object storage. Flushing also happens on its own, on a size and time
trigger; asking only means waiting for it.

### Storage format

Partitions are a memory-resident index over immutable objects in object
storage, described in
[ADR 0006](adr/0006-partitions-are-an-index-over-immutable-objects.md). The
format is our own and it is specified, because a single-implementation format
means data at rest is reachable only through Orbita, and because an engine that
does its own I/O puts a ceiling on what deterministic simulation can verify.

Object storage access goes through a pluggable storage trait. v1 ships and
supports the S3-compatible API only (AWS S3, MinIO, R2); other backends are
the community's to add through the trait.

### Failure handling

When a partition owner dies, the leader group detects it via missed
heartbeats, fences the old owner with an epoch bump, and promotes the most
caught-up replica to owner. The targets: writes to the affected partition are
unavailable for less than 10 seconds, reads continue from caught-up replicas
throughout, and no acknowledged write is ever lost. Read availability during
failover matters because it is the read-optimized promise under failure, not
just in steady state.

## Protocol and clients

Orbita speaks a simple gRPC protocol designed so clients can be dumb. A client
connects to any worker, typically through one load-balanced endpoint. Every
worker caches the partition map and forwards misdirected requests to the right
owner internally, costing at most one intra-cluster hop.

The dumb path stays fully supported: generated stubs against the load-balanced
endpoint remain a complete way to use every non-transactional operation,
because ease of adoption is still what an open-source project lives or dies
on. The original position stopped there, since a real client library per
language is a forever cost. Transactions changed the math. An interactive
transaction cannot pay a forwarding hop per read, and conflict retries belong
in a library rather than in every caller, so we now also ship an official
smart client:

- **A Rust core library**, generic over `orbita_runtime` like every other
  crate, so the deterministic simulator can drive the client too and
  map-staleness races and mid-commit failovers become seeded, replayable
  tests.
- **A partition map cache** discovered through the API the same way limits
  are, and repaired by the standard loop: a misrouted request is answered with
  the right owner, the client patches its cache and retries. Misrouting is
  never a correctness problem because epoch fencing means a deposed owner can
  only refuse, so the cache is purely a latency optimization.
- **A retry-closure transaction API** in the FoundationDB style, so the
  conflict-retry state machine lives in the library once.
- **Python bindings next**, with the end-to-end suite doubling as the
  conformance suite for every binding after it.

Third parties will write clients we do not, so `docs/CLIENTS.md` specifies
what a client must implement: the routing and repair loop, the error taxonomy
and which outcomes rerun a transaction, and the read-your-own-writes overlay
semantics, including how buffered writes merge into a scan. It is written
alongside the official client rather than after it, because a spec nobody has
implemented against is prose, not a contract.

## Transactions

The first version of this document treated multi-key transactions as
permanently out of scope. That position is reversed: ACID transactions are a
committed direction, currently scheduled at v0.3.0 in ROADMAP.md, after the
first release and after the testing investment that makes checking them
possible. This section records the decisions so everything built earlier has
them in mind, not to schedule the work; sequencing lives in ROADMAP.md.

The guarantee is strict serializability or nothing. I considered causal
transactions over vector clocks, which avoid central timestamping and handle
splits and merges gracefully, but they deliver parallel snapshot isolation,
and its anomalies, meaning two observers seeing commits in different orders,
are exactly what the coordination use cases exist to prevent. A transaction
guarantee weaker than the KV store's linearizability would undercut the one
claim the product makes.

The design that fits the architecture:

- **Timestamps come from an oracle in the leader group**, allocated in blocks
  committed through Raft so that Raft stays off the per-transaction hot path,
  with the new leader skipping to the end of the last reserved block on
  failover. I rejected clock-based schemes (HLCs with uncertainty windows)
  because their correctness rests on a skew bound holding in production, which
  the simulator can exercise but never discharge, and an assumption-dependent
  guarantee lands on the headline claim.
- **MVCC rides the immutable-object format.** Old segments already contain old
  versions and a manifest is already a snapshot; what MVCC adds is retention
  governed by a cluster-wide low-watermark, the minimum active snapshot
  timestamp, with a configurable window so a forgotten reader cannot stall
  reclamation forever. Snapshot reads older than the window fail cleanly.
- **Validation happens at partition owners.** Optimistic concurrency: reads at
  a snapshot timestamp, writes buffered in the client, and a commit validated
  by the owners involved, with intents logged through the existing 2-of-3 WAL
  so they survive failover, and epoch fencing preventing a deposed owner from
  validating anything. The coordinator is an owner, never the client, so a
  client that dies mid-transaction leaves nothing to clean up.
- **Single-key writes never touch the oracle.** They keep today's latency and
  availability. The partition Lamport advances past any commit timestamp it
  observes, Lamport's own rule, so versions stay totally ordered within a
  partition and the CAS contract is unchanged.

The staging, each stage independently shippable and independently useful:
snapshot timestamps on reads first, which fixes the LIST limitation; then
single-partition multi-key batches, which need no oracle at all because the
owner already serializes; then cross-partition snapshot isolation; then
read-set validation, which upgrades it to strict serializability.

What v1 owes this direction is one thing, and it has a deadline: the
partition-v1 record encoding reserves a commit timestamp field and an intent
flag before the format freezes at the first release. Everything else defers;
frozen bytes do not.

## Scale envelope

Two different things get called scale, and this section used to mix them. What
the architecture bounds is a property of Orbita and belongs in the pitch. What
v1 is tested to is a statement about our effort so far, and it moves every time
someone runs a bigger experiment. Only the first is a ceiling.

| Dimension | What bounds it | v1 target |
|---|---|---|
| Workers | Nothing in the design. Three is the floor, because a 2-of-3 WAL quorum needs three. | 3 and up |
| Partitions | Leader group metadata throughput, which is the real ceiling on cluster size | not characterized yet |
| Hot data | The sum of memory across the workers | grows by adding workers |
| Total data | Object storage, meaning effectively nothing | unbounded in practice, and independent of worker count |
| Read throughput | Replica count, since replicas serve reads | ~100k reads/sec |
| Write throughput | Partition count, since one owner serializes each partition | ~10k writes/sec |
| Max key size | Fixed | 10KB |
| Max value size | Per-keyspace configuration | 10MB, default far lower |

There is no architectural cap on workers, and claiming a small one gave away
the product's main argument for free. The thing that does grow with cluster
size is the partition map: every worker caches all of it, and every worker
heartbeats its progress on every partition it holds to the leader group. So the
leader group's capacity to carry metadata is what bounds a cluster, and it is
bounded in partitions rather than in machines.

That distinction is the whole etcd contrast, so it is worth being exact about.
etcd puts every operation through one Raft group, which is why its ceiling
arrives at around 8GB of data. Orbita puts only metadata changes through one,
and reads and writes never touch it at all. A cluster gets big enough to
strain the leader group only when it has enough partitions to hold far more
data than etcd can, and the fix when that day comes is a sharded control plane
rather than a rewrite.

The throughput numbers are targets rather than measurements, and they are
cluster-wide figures for a cluster of the size we currently test. Neither is a
ceiling. Read capacity grows with replicas per
[ADR 0001](adr/0001-linearizable-reads-from-replicas.md), and write capacity
grows with partitions, which split as they grow. Publishing a measured curve
of both against worker count is a release deliverable, and it replaces these
rows when it exists.

The partition ceiling is genuinely unknown, which is worth saying rather than
guessing at. The simulator has no partition-count knob today, so nothing has
established where the leader group starts to strain. Finding that number is the
experiment that turns the argument above from a design claim into a published
one, and it should happen before we make the scaling claim in marketing.

Hot and total are separate numbers because
[ADR 0006](adr/0006-partitions-are-an-index-over-immutable-objects.md) keeps
values in memory and their data in object storage. What a cluster holds hot is
what its workers can hold in memory; what it holds at all is a question about
object storage, and the answer there is effectively no limit. Both scale by
adding workers, since partitions split and distribute.

Two sizing notes for whoever is choosing machine types. A partition's index is
resident whether or not its values are, so memory serves two purposes at once,
and a keyspace of small values gets less byte capacity per gigabyte of memory
than the value size alone suggests. And a single partition's index has to fit
on its owner, which is one of the things that triggers a split.

Value size is a per-keyspace setting rather than one global number. Large
values cost memory in the cache and time on the wire, so a tenant that needs
them raises its own limit and accepts different performance, instead of every
keyspace paying for one tenant's blobs.

A value above a threshold is stored as its own object and written before it is
committed, so the log carries a reference rather than megabytes. See
[ADR 0007](adr/0007-large-values-are-their-own-objects.md). One consequence
reaches clients: gRPC implementations default to a 4MB message limit, so a
keyspace configured above that requires clients to raise their own limit. The
API publishes the limits so a client can discover them rather than learning
them by exceeding one.

## Operations

- **One static binary.** The same `orbita` binary runs as leader or worker by
  configuration. A laptop cluster is one command. This is a deliberate
  adoption lever, not a convenience.
- **Distribution.** A Docker image built and published on every release,
  Kubernetes manifests and a Helm chart, and a Docker Compose quickstart that
  stands up a leader group, workers, and MinIO for local evaluation.
- **Admin surface.** An admin gRPC API with a CLI wrapping it: keyspace CRUD,
  credential management, partition map inspection, and manual split, merge,
  and rebalance triggers.
- **Observability.** OpenTelemetry traces, logs, and metrics throughout,
  including per-keyspace and per-partition latency, WAL replication lag, split
  and merge activity, and quota consumption.
- **Backup and point-in-time restore.** The storage design in
  [ADR 0006](adr/0006-partitions-are-an-index-over-immutable-objects.md) makes
  this cheap on purpose: immutable segments and manifests mean a backup is a
  manifest retention policy and a restore is pointing at an old manifest. The
  requirement is that an operator can restore a keyspace to a named point in
  time, and that the retention window is theirs to configure. Backup is the
  first checkbox on any production-adoption review, and a design that gets it
  nearly free should say so out loud.
- **An offline format reader.** A standalone tool that reads a partition from
  a bucket with no cluster running. The format is specified so that data at
  rest is reachable without Orbita, and this tool is what makes that promise
  checkable rather than aspirational; it is also the recovery path of last
  resort.

## Engineering posture

- **Language.** Rust. Storage originally sat on the `rocksdb` crate; ADR 0006
  replaced that with a partition format this project owns, so the engine has
  no storage dependency to trust beyond the object store.
- **Deterministic simulation testing from day one.** All network, disk, and
  clock access sits behind swappable interfaces so the whole system runs
  single-threaded in a seeded, fault-injecting simulation. This is expensive
  up front and famously impossible to retrofit, and it is the foundation of
  the product's headline claim. Correctness evidence is not a launch
  deliverable bolted on at the end; it is the development methodology.
- **License.** Apache-2.0. Maximally adoptable, standard for this class of
  infrastructure. We accept the hosted-service risk that comes with it.

## Acceptance criteria

v1 does not ship until these are measured, not estimated:

1. p99 GET latency at or under 2ms in-cluster at the target read throughput,
   for keys in the memory-resident hot set. A read that misses and has to fetch
   from object storage is reported separately, because publishing one number
   for both would be wrong in whichever direction it landed.
2. p99 SET latency at or under 5ms at the target write throughput, for values
   at the default size limit. This is the replicated acknowledgement; an
   explicit flush to object storage is a different operation with its own
   number.
3. Linearizability verified under fault injection (partitions, crashes,
   restarts, clock skew) in the deterministic simulator.
4. Owner failover completes in under 10 seconds with zero acknowledged writes
   lost and reads served throughout.
5. A public correctness report covering the DST and fault-injection results,
   published as part of launch. A third-party Jepsen analysis is the follow-on
   goal once the system stabilizes.

## Non-goals for v1

These are out of scope on purpose, and each is a fence we draw now so scope
cannot creep past it:

- **Multi-key transactions, in v1.** Single-key atomicity and version CAS
  only. This is no longer a permanent fence; the direction and its constraints
  are fixed in Transactions above, and v1's only obligation to it is the
  format reservation recorded there.
- **Watch/subscribe streams.** No change notifications. This is the known gap
  for the coordination use case and the first roadmap item.
- **Secondary indexes and queries.** Key lookup and prefix scan only. Orbita
  is a KV substrate, not a database, and staying one is a feature.
- **Geo-replication.** Single-region clusters only. This keeps the
  clock-sync and uncertainty-interval design space out of v1 entirely.

## Roadmap

Sequencing lives in ROADMAP.md, which maps this work to releases. The
priority order, for when the two disagree: watch/subscribe streams first,
then the transaction ladder from Transactions above with the official client
library and `docs/CLIENTS.md` alongside, then a third-party Jepsen analysis,
then additional object storage backends via the storage trait (GCS, Azure),
then a multi-region story.

Earlier versions of this document kept multi-key transactions off this list
and conceded them to FoundationDB flatly. That concession is withdrawn
deliberately, not drifted into. The deciding argument was that the pieces
Orbita already has, meaning owner serialization, epoch fencing, a replicated
WAL that can carry intents, and a simulator that can drive the whole protocol,
are most of a verifiable transaction system, and published correctness
evidence for transactions is scarcer in the market than transactions
themselves.
