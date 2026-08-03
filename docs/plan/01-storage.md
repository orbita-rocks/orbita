# 01: Storage engine (`orbita-storage`)

Owns one partition's RocksDB instance and everything that happens inside it.
This is single-node code with no notion of replication, ownership, or the
cluster, which makes it the best place to start: it is fully testable on its
own and every other crate depends on its semantics being right.

## Scope

- Add the `rocksdb` crate and own all RocksDB configuration. Column families,
  compaction settings, block cache, and write batching are yours to choose.
- Key and value encoding. Values carry a version and an optional absolute
  expiry, per `orbita_core::Record`.
- The write path: `put`, `delete`, and conditional forms of both honouring
  `orbita_core::WriteCondition`. A conditional write that fails must report the
  version it actually found, because the caller uses that to retry.
- The read path: `get`, and prefix scan with a cursor for `LIST`.
- TTL: expired keys are invisible to reads and scans immediately, and are
  physically reclaimed by a RocksDB compaction filter. Absolute deadlines only.
- Applying a WAL entry. The WAL crate produces entries; you consume them
  idempotently, so replaying the same entry twice leaves the same state.
- A snapshot for one page of a scan, so a `LIST` page is a consistent view even
  while writes continue.
- Size accounting for the partition, since the leader splits on size.

## Out of scope

- Replication, epochs, ownership, and the partition map. You are given a
  partition; you do not know who owns it.
- The WAL format itself, which is brief 02.
- Object storage. SST upload and hydration land later; keep the RocksDB
  configuration free of assumptions that would prevent it.
- Multi-key atomicity beyond what a single RocksDB write batch gives you.

## Interface sketch

Not binding, but this is the shape callers expect. Take a `Runtime` parameter
even where you do not need it yet, so the signature does not change later.

```rust
pub struct Partition<R: Runtime> { /* ... */ }

impl<R: Runtime> Partition<R> {
    pub async fn open(runtime: R, path: &str, range: KeyRange) -> Result<Self>;

    pub async fn get(&self, key: &[u8]) -> Result<Option<Record>>;

    /// Returns the new version, or the condition failure with the version
    /// actually present.
    pub async fn put(
        &self,
        key: &[u8],
        value: Bytes,
        ttl: Option<Duration>,
        condition: WriteCondition,
    ) -> Result<WriteOutcome>;

    pub async fn delete(&self, key: &[u8], condition: WriteCondition) -> Result<WriteOutcome>;

    pub async fn scan(&self, prefix: &[u8], cursor: Option<&[u8]>, limit: u32)
        -> Result<ScanPage>;

    pub async fn apply(&self, entry: &WalEntry) -> Result<()>;

    pub async fn size_bytes(&self) -> Result<u64>;
}
```

## Decisions to make and write down

- **Version allocation.** Settled by
  [ADR 0002](../adr/0002-key-versions-are-partition-lamports.md): a key's
  version is the partition Lamport at which it was last written. This brief
  originally left it open and the crate was first built with per-key counters,
  which the ADR supersedes and explains.
- **Delete representation.** A RocksDB tombstone, or an explicit tombstone
  record with a version? The second is needed if a conditional write must
  distinguish "never existed" from "deleted at version 4", and it costs space.
- **Cursor encoding.** Opaque to clients, but it must survive a partition split
  gracefully: a cursor issued before a split should not silently skip keys.
  Getting this wrong is a data-loss-shaped bug in a backup tool.

## Done when

- Conditional writes are correct under concurrent access: two `IfNotPresent`
  writes to the same key, one wins, one reports the other's version.
- An expired key is invisible to `get` and to `scan` in the same millisecond it
  expires, and is gone from disk after a compaction.
- A scan cursor walks a range exactly once with no duplicates and no gaps,
  including when values are written and deleted between pages.
- `apply` is idempotent: replaying a WAL entry twice is indistinguishable from
  applying it once.
- Oversized keys and values are rejected with `Error::TooLarge` before touching
  RocksDB.
- A property test generates random operation sequences and compares against a
  `BTreeMap` model. This is the test that will actually find the bugs.
