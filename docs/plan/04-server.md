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

### WAL retention and hydration

Live flushes checkpoint an owner's local WAL only after the partition manifest
is published, which bounds owner disk use. What that used to cost was recovery:
a replica behind the retained WAL asked for entries the owner had already
removed, got no catch-up range, and stayed unavailable rather than inventing
state.

Hydration is the answer, and it is what makes the retention bound safe rather
than merely cheap. A node builds the partition from the current manifest and
takes that horizon as where its own log starts, so it resumes replication above
the horizon instead of at the start. It happens when a partition is opened,
which is the replacement-worker case, and in place when an inbound batch turns
out to leave a hole, which is the fell-behind case and does not need a restart.

The horizon is derived at every open rather than written into the log, and that
is a compatibility decision rather than an implementation detail. The log's
framing cannot skip a record it does not understand: an older reader stops at
the first unknown kind and truncates the rest, so a marker record would make
the pre-finalization rollback `docs/UPGRADES.md` promises lose acknowledged
writes. Nothing is lost by deriving it, because the manifest is read before the
log is opened on both the owner and replica paths.

Hydration also carries the manifest's epoch, not only its horizon. A manifest
reaches the bucket through the owner's fenced compare-and-swap, so it is proof
about who owns the partition. A replica that hydrates on behalf of a sender the
manifest outranks refuses the append instead of acknowledging it, and a worker
whose ownership grant the manifest outranks refuses to open as owner at all.

Two limits remain, and neither is a fallback to an incomplete WAL. A partition
that has never been flushed has nothing to hydrate from, and a manifest that is
itself behind the gap closes only part of it. Both are reported as the gap they
are.

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
