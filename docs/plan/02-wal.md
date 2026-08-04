# 02: Write-ahead log and replication (`orbita-wal`)

Orbita's durability claim is that an acknowledged write survives the loss of
any one worker. This crate is where that claim is either true or false.

A write is acknowledged when its entry is on stable storage at two of three
replicas. That is the whole contract, and everything here serves it.

## Scope

- The log format: framing, checksums, and segment rollover. Recovery must be
  able to tell a complete entry from a torn tail, because a crash mid-append is
  the expected case, not the exotic one.
- Append and fsync through `orbita_runtime::Disk`. Never touch `std::fs`.
- The replication protocol between an owner and its two replicas, over
  `Transport` with `ServiceId::Wal`. Define your own message types and
  encoding.
- Acknowledgement at two of three, including the owner's own copy.
- Batching. Several concurrent writes should share one fsync, since fsync is
  the write path's dominant cost and the 5ms target assumes amortisation.
- Recovery: replay from the last checkpoint, discard a torn tail, and report
  the highest durable Lamport.
- The epoch check. An append carrying an epoch older than the replica's current
  one must be rejected. This is what fences a deposed owner, and it is the
  single most important safety property in the crate.
- Truncation once entries are applied and their SSTs are durable.

## Out of scope

- Deciding who the owner is. You are told, via an epoch from the control plane.
- Promotion and failover, which is brief 03. You provide what it needs: how far
  each replica has durably logged.
- Applying entries to the storage engine, which is brief 01.

## Interface sketch

```rust
pub struct WalEntry {
    pub lamport: Lamport,
    pub epoch: Epoch,
    pub partition: PartitionId,
    pub op: WalOp,   // Put { key, value, expires_at } | Delete { key }
}

pub struct Wal<R: Runtime> { /* ... */ }

impl<R: Runtime> Wal<R> {
    pub async fn open(runtime: R, path: &str, partition: PartitionId) -> Result<Self>;

    /// Appends locally, replicates, and resolves when two of three have it
    /// durably. Errors if this node's epoch is stale.
    pub async fn commit(&self, op: WalOp) -> Result<Lamport>;

    pub async fn recover(&self) -> Result<RecoveryState>;

    /// How far this node has durably logged, for the control plane's
    /// promotion decision.
    pub fn durable_lamport(&self) -> Lamport;
}
```

## Decisions to make and write down

- **Pipelining.** Does the owner allow entry N+1 to be in flight before N is
  acknowledged? It is necessary for throughput and it complicates recovery,
  since a replica may hold a gap. Decide, and make recovery handle it.
- **Checksum scope.** Per entry, or per batch? Per entry costs more and lets
  recovery salvage more of a damaged log.
- **Replica divergence.** When a promoted owner finds a replica holding an
  entry it does not have, what happens? Under two-of-three that entry was never
  acknowledged, so truncating it is safe, but the code must say so explicitly
  rather than implying it.

## Done when

- A crash at any point during an append leaves the log recoverable, with every
  acknowledged write present and any un-acknowledged tail cleanly discarded.
  Test this by injecting a fault at every byte offset, not at a few.
- Losing any one of three replicas loses no acknowledged write.
- Losing two replicas fails writes rather than acknowledging them. Availability
  is not worth a lost write here.
- An append from a fenced owner is rejected even if it is otherwise well
  formed.
- Corrupt bytes mid-log are detected and reported as truncation at that offset,
  never returned as data.
- Under simulation, a randomised schedule of concurrent commits, crashes, and
  partitions never produces a state where an acknowledged write is missing.
