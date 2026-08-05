# 04: Worker and client API (`orbita-server`)

The node clients actually talk to. It serves the gRPC API, routes each request
to the right partition, and implements the linearizable read path.

## Scope

- The `Kv` service from `orbita-proto`, including the mapping from
  `orbita_core::Error` to gRPC status codes. Keep that mapping in one place;
  scattered it drifts.
- Routing. A worker holds a cached partition map, finds the partition owning a
  key, and either serves it or forwards to the owner over `Transport` with
  `ServiceId::Proxy`. Clients are dumb by design: they connect to any worker
  and never learn about partitions.
- Handling a stale cached map: a forwarded request that arrives at a node which
  is no longer the owner must be re-forwarded or rejected with `NotOwner`
  carrying the current owner, and never silently served from stale state.
- The linearizable read path. A replica caught up to the partition's committed
  Lamport serves the read locally; otherwise it forwards to the owner. This is
  what makes reads scale across replicas without weakening the guarantee, and
  the caught-up check is the part to get exactly right.
- The write path: validate, check the condition, commit through the WAL, apply
  to storage, advance the Lamport.
- The production `Transport`, meaning gRPC peer-to-peer, and with it the
  `Runtime` implementation production binaries use. This is the missing piece
  in `orbita_runtime::tokio_runtime`.
- Authentication, credential checking, and per-keyspace quota enforcement at
  the edge.
- OpenTelemetry traces, metrics, and logs, labelled per keyspace and per
  partition.

## Out of scope

- Deciding ownership, which is brief 03. You consume the map.
- Log and storage internals, which are briefs 01 and 02.

## The read path, in detail

This was the open question in the first draft of this brief, and it has since
been settled. Read [ADR 0001](../adr/0001-linearizable-reads-from-replicas.md)
in full before writing any of this crate; it is the design, not background
reading.

The summary. A replica serves a read locally only when all three of these hold:
it has a live read lease from the owner, it has no gap in the invalidation
stream, and the key is not in its invalid set. Anything else forwards to the
owner. The owner invalidates keys by riding on WAL replication, and
acknowledges a write to the client only once every lease-holding replica has
acknowledged the invalidation or had its lease expire.

You own the read path, the lease bookkeeping on both sides, and the invalid
set. The WAL crate owns the message that carries the invalidation, so agree the
wire format with whoever holds that crate rather than inventing a second
channel.

## The write path

Read [ADR 0003](../adr/0003-conditions-evaluate-against-pending-writes.md),
which is also binding. The owner evaluates a write's condition against
committed state overlaid with its own pending writes, and never holds a lock
across replication. The pending set is the same one the read path uses for
invalidation, carrying the resulting record as well as the key.

Note what this means for the storage engine: `Partition::put` and
`Partition::delete` fuse evaluation and commit, so the owner does not use them.
It reads with `get`, decides against its overlay, and commits with `apply`.

Do not argue that this is safe. Have the simulator's linearizability checker
demonstrate it under partitions and failovers, which is the entire reason that
crate exists.

### WAL retention before hydration

Live flushes checkpoint an owner's local WAL only after the partition manifest
is published. This bounds owner disk use, but it lands before hydration in
issue #17. A replica that falls behind the retained WAL asks for entries the
owner has already removed, receives no catch-up range, and has no snapshot path
in this release. It remains unavailable rather than inventing state.

That is an explicit merge tradeoff for issue #16, not a recovery guarantee. The
manifest makes the removed entries cluster-durable, but only hydration can turn
those objects back into a caught-up replica. Until #17 lands, operators must
repair or replace such a replica after hydration exists; there is no safe
fallback to an incomplete WAL.

The cliff is a state rather than a log line. The owner records the replica, the
Lamport it stopped at, and the oldest Lamport the owner still retains, and
reports all three through `Server::replicas_beyond_retention`. That drives the
`replicas-recoverable` readiness condition, so the answer leaves the process
through `Health.CheckReadiness` and a rolling update stops at a node that owns
a partition permanently short a copy.

The evidence comes from two places, and the second is what makes the first
trustworthy. An append refused with `Gap` proves it, and the lease heartbeat
carries every replica's log position back on a cadence that does not depend on
there being any writes. Without the second, an owner that restarts or is
promoted starts with an empty view, and an empty view read as a healthy one is
how a partition sits stranded and silent. So the owner's view names every
replica, including the ones it has established nothing about; see
`orbita_wal::ReplicaCatchUp`.

`crates/orbita-server/src/retention.rs` drives the whole thing under the
simulator and pins what it looks like from outside: the owner names it and
fails readiness, the replica leaves the read set rather than serving from its
own state, its log stops where it stopped instead of resuming above the hole,
and an owner that restarts and writes nothing reaches the same verdict. That
scenario is also the tracking test for #17: it asserts one of two named
outcomes, and hydration has to move which one.

How far a replica may lag before it falls off is the write-ahead log's segment
size, since a checkpoint drops whole segments. That is `wal_segment_bytes` on
`ServerConfig`, defaulting to 64 MiB.

## Decisions to make and write down

- **Proxy hop cost.** Does a forwarded request open a new stream per call, or
  is there a pooled multiplexed connection per peer? At 100k reads a second
  this matters.
- **Partition map staleness.** How is the cache refreshed: pushed by the leader
  group, pulled on a timer, or repaired lazily on a `NotOwner`? Cheapest is
  lazy repair, and it makes the first request after a failover pay the cost.
- **Backpressure.** What happens when a partition owner is saturated? Queueing
  forever turns a slow partition into a cluster-wide outage.

## Done when

- A client using generated stubs and no hand-written library can do everything
  in the API, connecting to any node.
- A read never returns a value older than an acknowledged write, verified by
  the simulator's linearizability checker under partitions and failovers.
- A request for a key whose partition just moved succeeds without the client
  noticing anything beyond latency.
- Credentials are enforced: a token scoped to keyspace A cannot touch keyspace
  B, and this is tested rather than assumed.
- Quota exhaustion returns `QuotaExceeded` and does not degrade other
  keyspaces.
- p99 latencies meet the requirements targets on a three node cluster.
