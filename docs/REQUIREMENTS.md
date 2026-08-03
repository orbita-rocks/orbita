# Orbita

> Orbita is a strongly consistent, multitenant distributed key-value store. It
> uses RocksDB for storage, keeps compacted data on object storage, and is
> built under deterministic simulation from day one.

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
- **Positioned respectfully against FoundationDB.** FDB is more capable if you
  need multi-key transactions, and heavier if you don't. We concede the
  transaction gap openly rather than pretending it away.
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
cheap version and document it clearly.

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
their own local RocksDB. Compacted SSTs are uploaded to object storage.

This hybrid (replicated WAL for latency, object storage for bulk durability)
is a deliberate middle path. Object storage alone puts a 50 to 100ms floor
under writes; local disk alone makes losing a machine a data-loss event. The
2-of-3 WAL gives single-digit-millisecond writes that survive the loss of any
one worker with zero data loss, and the S3-resident SSTs keep workers cheap to
replace and rebalance.

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

I considered smart clients that route directly to partition owners, which is
the higher-performance pattern, but it means maintaining a real client library
per language forever. A protocol simple enough to use from generated gRPC
stubs is worth the hop, especially for an open-source project that lives or
dies on ease of adoption.

## Scale envelope

v1 targets the small-cluster sweet spot. These numbers bound what we design
and test for, and they become the limits we document.

| Dimension | v1 target |
|---|---|
| Workers | 3 to 15 |
| Logical data | up to ~10TB |
| Cluster read throughput | ~100k reads/sec |
| Cluster write throughput | ~10k writes/sec |
| Max key size | 10KB |
| Max value size | 256KB |

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

## Engineering posture

- **Language.** Rust, using the mature `rocksdb` crate.
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

1. p99 GET latency at or under 2ms in-cluster at the target read throughput.
2. p99 SET latency at or under 5ms at the target write throughput.
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

- **Multi-key transactions.** Single-key atomicity and version CAS only.
  Cross-key transactions mean MVCC and a transaction protocol, which is a
  different product tier.
- **Watch/subscribe streams.** No change notifications. This is the known gap
  for the coordination use case and the first roadmap item.
- **Secondary indexes and queries.** Key lookup and prefix scan only. Orbita
  is a KV substrate, not a database, and staying one is a feature.
- **Geo-replication.** Single-region clusters only. This keeps the
  clock-sync and uncertainty-interval design space out of v1 entirely.

## Roadmap (post-v1, in rough priority order)

1. Watch/subscribe streams.
2. Third-party Jepsen analysis.
3. Additional object storage backends via the storage trait (GCS, Azure).
4. Multi-region story.
5. Multi-key transactions, if the substrate positioning ever demands it.
