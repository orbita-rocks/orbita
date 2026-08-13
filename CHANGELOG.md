# Changelog

Notable changes to Orbita, newest first. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the versions are
[semantic](https://semver.org/spec/v2.0.0.html).

Before 1.0 the minor version is the compatibility unit. A minor bump can change
the peer protocol or a persisted format, and a patch bump cannot. That rule is
what the rollback window in [docs/UPGRADES.md](docs/UPGRADES.md) rests on, so a
change that breaks it is a bug in the release, not a judgement call.

Entries under Unreleased are generated from the conventional commit history by
`git-cliff` when a release is cut, and edited by hand when a change deserves a
sentence a commit subject cannot carry.

## [Unreleased]

### Added

- **A cache for values read out of segments.**
  [ADR 0006](docs/adr/0006-partitions-are-an-index-over-immutable-objects.md)
  decided that values are cached rather than resident and the caching half was
  never built, so every read of a flushed key was an object-store round trip.
  Measured on EKS against real S3, turning it on takes read throughput from
  9,166 to 52,925 reads/s and p50 from 25.6ms to 3.6ms, and the fastest read on
  that cluster goes from 14.9ms to 0.31ms — the round trip leaving the read
  path. Node-scoped budget through `ORBITA_VALUE_CACHE_BYTES`, 256 MiB by
  default, zero to turn it off. Reported through
  `orbita.value_cache.{bytes,hits,misses,evictions}`.
- **Read-ahead on a cache miss.** A miss costs a round trip whatever it brings
  back, and segment records are sorted and contiguous, so one miss now fetches a
  window and warms the neighbours it had to cross anyway. Cold misses to warm a
  5,000-record working set fall from about 5,170 per node to about 94.
  `ORBITA_READ_AHEAD_BYTES`, 256 KiB by default.
- **Read serving decoupled from the durability quorum.**
  [ADR 0013](docs/adr/0013-read-serving-is-decoupled-from-the-durability-quorum.md).
  A keyspace is no longer confined to `replication_factor` nodes:
  `ORBITA_READ_REPLICA_TARGET` sizes the read-serving set independently, capped
  at half the placeable cluster because every holder is an invalidation a write
  waits on. Measured at +13% to +39% read throughput going from three holders to
  five, for less total CPU, because forwarding disappears rather than moving.
  `orbita.partition.lease_holders` reports what a write is paying for.

### Performance

- The p99 GET target in `docs/REQUIREMENTS.md` is met and measured for the first
  time: 1.97ms at 14,847 reads/s on one four-vCPU pod, for a working set held in
  memory, which is the population that criterion names. It could not be measured
  at all before the value cache existed, because there was no hot set to measure
  it against. Above that rate latency grows with concurrency in the ordinary way,
  so the number to quote is "under 2ms at about 15k reads/s per node" rather than
  "under 2ms".

### Known issues

Disclosed rather than fixed. Each has an issue.

- **A new partition's replication path starts cold** (#168). An owner admits
  writes the moment it opens, and nothing requires its replicas to have opened
  the same partition yet, so the first writes can fail with
  `could not reach a second copy` until the replicas catch up. There is no retry
  in the owner's replication path today. It affects fresh clusters as well as
  split children — a 2,000 record load against a just-ready cluster lost 294
  records to it — and the failure is genuinely ambiguous, so it must not be
  retried blindly by a client.
- **Adjacent partition merges ship present but disabled** (#125, ADR 0012). The
  code is in this release and the vocabulary sits behind cluster protocol 0.2,
  so nothing exercises it until a `finalize-upgrade` to 0.2. That is deliberate:
  the merge commands cannot be spoken safely inside a 0.1 window.
- **The first-Raft-upgrade procedure has never been run on a real Kubernetes
  cluster** (#61). It is covered by tests and by the simulator, and it has not
  been rehearsed against a live rollout.
- **A keyspace's index memory does not distribute** (#170). Read capacity now
  grows past the replication factor; index memory and single-partition write
  capacity do not. A keyspace is still bounded by one node's memory for its
  index.
- **Heartbeat reporting is O(partitions) on every interval** (#177), whatever
  changed. It is invisible at the partition counts tested here and is the one
  place a cold partition is not free.
- **A node that loses its data directory may not rejoin** (#184), refusing with
  a cluster identity mismatch and serving health only. Seen once, on a cluster
  scaled up and down repeatedly, and not yet reproduced deliberately.
- **`cluster describe --output json` reports `owner` as null** next to a correct
  `owner_node_id` (#161). Cosmetic, in a field a script may read.

## [0.0.1] - 2026-08-04

The first numbered release, so there is a fixed point to build on and to
compare against.

### Added

- A working single node. `orbita dev` serves reads, writes, deletes, and scans
  end to end. See [docs/QUICKSTART.md](docs/QUICKSTART.md) for what works
  today and what does not.
- A working cluster shape. Multi-node clusters start, bind both listeners, and
  report healthy, exercised from outside Rust through the CLI, Docker Compose,
  and Kubernetes manifests. Worker registration with the leader group is the
  outstanding piece, so cross-node serving does not work yet.
- The partition storage format specified in
  [ADR 0006](docs/adr/0006-partitions-are-an-index-over-immutable-objects.md),
  an open format in place of
  RocksDB, with its limits documented rather than implied.
- A release process. Releases are cut from tags on `main`, prerelease artifacts
  are built from every push to `develop`, and the workspace version drives the
  image tag, the chart's `appVersion`, and the pinned manifests from one place.
  See [docs/RELEASING.md](docs/RELEASING.md).
- The build and test tasks run through moon, so the path a laptop takes is the
  same one CI takes.
