# 03: Control plane (`orbita-control`)

The leader group: three or more nodes running Raft that hold the cluster's
authoritative metadata and make every decision about who owns what.

This is the highest-risk brief in the project. It is also the one where being
slow and careful is cheapest, because a bug here corrupts the partition map and
a corrupt partition map loses data that the WAL faithfully preserved.

## Scope

- Raft, via `openraft` or `raft-rs`. Do not write one. Wire its storage and
  network through `orbita_runtime` so the simulator can drive it; that seam is
  the reason to adopt rather than build.
- The replicated state machine: the partition map, worker membership, keyspace
  definitions and quotas, and credentials.
- Worker health from heartbeats, with the healthy, suspect, and dead states the
  admin API exposes.
- Failover: on losing an owner, pick the replica with the highest durable
  Lamport, bump the epoch, and publish the new owner. The epoch bump is what
  fences the old owner, so it must be committed to Raft before the new owner is
  told it may accept writes. Getting that order wrong is a split brain.
- Split: choose a partition and a boundary key, then drive the state machine
  through it so that no key is ever owned by nobody or by two owners at once.
- Merge: the same, for two adjacent partitions.
- Replica placement, and rebalancing when workers join or leave.
- Serving the `Admin` gRPC API from `orbita-proto`.

## Out of scope

- The data path. Nothing here sits between a client and a key. Workers cache
  the partition map and keep serving reads while the leader group is electing;
  a control plane outage must not become a data plane outage, and the design
  should make that obviously true rather than incidentally true.
- Moving actual bytes during a rebalance. You decide and instruct; the worker
  moves data.

## The failover ordering, in detail

This is the sequence the 10 second target and the no-lost-write guarantee both
depend on, so it is worth stating rather than leaving to inference:

1. Heartbeats from the owner stop. Mark it suspect, keep serving reads from
   replicas.
2. After the failure threshold, commit a Raft entry marking it dead and
   bumping the partition's epoch.
3. Only once that entry is committed, choose the replica with the highest
   durable Lamport and commit it as the new owner.
4. Tell the new owner. It may now accept writes at the new epoch.
5. The old owner, if it comes back, learns its epoch is stale on its first
   append and steps down without having accepted anything.

## Decisions to make and write down

- **Failure detection threshold.** Too eager and a garbage collection pause
  triggers a failover; too slow and the 10 second target is missed. Pick
  numbers, justify them against the target, and make them configurable.
- **Split point selection.** Midpoint by size needs a RocksDB size estimate,
  which is approximate. Decide how approximate is acceptable and what happens
  when a split produces two lopsided halves.
- **Merge safety.** The hard case: both partitions must stop accepting writes,
  agree on a final Lamport, and hand off to one owner, without a window where a
  key is unowned. Write the state machine down before writing code, and expect
  this to take longer than the rest of the brief combined.

  A merge also has to preserve Lamport ordering, because a key's version is now
  its Lamport, per
  [ADR 0002](../adr/0002-key-versions-are-partition-lamports.md). If a merge
  lets any key's version go backwards, a client's held version silently stops
  matching and its compare-and-swap fails for no reason it can see. State how
  the merged sequence is derived and why it cannot regress.

- **Lease revocation on ownership change.** Read replicas serve under leases
  from the owner, per
  [ADR 0001](../adr/0001-linearizable-reads-from-replicas.md). A promoted owner
  must not accept writes until the deposed owner's leases are revoked or
  expired, or a replica could still serve a pre-failover value. Lease duration
  is therefore part of the failover budget, not an independent knob, and the
  two have to be chosen together.
- **Quota enforcement point.** The leader knows the quota, the worker sees the
  traffic. Decide where the check happens and how stale the worker's view may
  be.

## Done when

- A killed owner is replaced within 10 seconds, with reads never interrupted
  and no acknowledged write lost.
- A deposed owner that returns cannot commit anything at its old epoch, even if
  it never noticed it was deposed.
- A split leaves every key owned by exactly one partition, with no key
  unreachable at any instant during the operation. Test by reading continuously
  through the split.
- A merge does the same, including when a node fails midway through it.
- Killing a leader group member does not interrupt the data path at all.
- The partition map survives a full leader group restart.
- Every one of the above holds under simulation across thousands of seeds, not
  just in a happy-path integration test.
