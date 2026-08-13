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

## [0.1.0] - 2026-08-13

### Added

- Build Orbita from requirements through a working single node ([4f1c050](https://github.com/orbita-rocks/orbita/commit/4f1c05055d1f32d0651a83290c52a047d6466830))
- Make Orbita a working cluster, and test it from outside Rust ([7d809c6](https://github.com/orbita-rocks/orbita/commit/7d809c6629b757635947e74a88aa675aec3456c5))
- Replace RocksDB with an open format, and publish the limits ([c3a3cdb](https://github.com/orbita-rocks/orbita/commit/c3a3cdb95fe8aa5ae92ea78f701d41abac31a45a))
- Implement the partition format specified in ADR 0006 (#8) ([fabaa3f](https://github.com/orbita-rocks/orbita/commit/fabaa3fcc6f2c4825f95c145596deabb3b029a7b))
- Implement the S3-compatible ObjectStore (#15) ([7bfc916](https://github.com/orbita-rocks/orbita/commit/7bfc916e61b8e93289fc4183987cefb8757a0669))
- Make readiness assert join, WAL recovery, and partition catch-up (#25) ([b2a6a29](https://github.com/orbita-rocks/orbita/commit/b2a6a2998d4972e6e5e85e8cc157492a08fd787d))
- Move the storage engine onto the partition format ([49f2bbc](https://github.com/orbita-rocks/orbita/commit/49f2bbc24a3323d991f757c8d2e57d699475a391))
- Address the storage-migration review findings ([fbb5b8a](https://github.com/orbita-rocks/orbita/commit/fbb5b8acb74b71909b099a7ed4bf19f159370bfb))
- Replace RocksDB with an open format, and publish the limits ([bd950a1](https://github.com/orbita-rocks/orbita/commit/bd950a15485318b4ae09656b810fb49b1e8d623b))
- Reserve commit timestamp and intent flag in record encoding (#19) ([5ea6b2b](https://github.com/orbita-rocks/orbita/commit/5ea6b2b6b02e7fc36883f64690933e83f4851707))
- Add cluster version to replicated state and finalize-upgrade (#24) ([3186e6d](https://github.com/orbita-rocks/orbita/commit/3186e6d188d558ef45259fead9df35229191866b))
- Hand off partitions on SIGTERM (#26) ([659e532](https://github.com/orbita-rocks/orbita/commit/659e53270233cb4da46bc1dd8e4d084cb2140bee))
- Slot Raft under the ConsensusLog trait (#21) ([617c3a9](https://github.com/orbita-rocks/orbita/commit/617c3a9b078e4285ed925cffc23cebfb833e7e71))
- Publish live segment manifests (#16) ([d9e9d11](https://github.com/orbita-rocks/orbita/commit/d9e9d11763be386b007c2904bd5ebc23ccf3c353))
- Enforce the compatibility window (#27) ([cbf8652](https://github.com/orbita-rocks/orbita/commit/cbf86521288d58183817a2f685714530d89f48d7))
- Wire fixed leader peers (#52) ([af7b83b](https://github.com/orbita-rocks/orbita/commit/af7b83bbe368b0faf75dc79da4d03dcc305c71ff))
- Hydrate a partition from a bucket (#82) ([e0b4094](https://github.com/orbita-rocks/orbita/commit/e0b4094d9ab5adbe108b63410007f2fbd6f3a8c8))
- Source S3 credentials from IAM role assumption and instance profiles (#80) ([3228499](https://github.com/orbita-rocks/orbita/commit/3228499ea068a11d93d51cf1b08950434d4e6f1a))
- Surface resource consumption in cluster describe (#81) ([9eecd14](https://github.com/orbita-rocks/orbita/commit/9eecd14867ccfbbf8ab7ac3d1b65df271480850b))
- Drive the object store through fault injection (#92) ([605d48d](https://github.com/orbita-rocks/orbita/commit/605d48d6af92c517d15b8df30950c407a99d7bfc))
- Stand up disposable EKS + S3 test clusters (#116) ([edefbfd](https://github.com/orbita-rocks/orbita/commit/edefbfd48000fcfd9aca5a52e81156fa9bc61400))
- Enforce credentials at the boundary with a config root credential (#108) ([0adba07](https://github.com/orbita-rocks/orbita/commit/0adba073c9b4e0c939cb8666ebbd11bc6427fb24))
- Enforce per-keyspace storage quotas and request rate limits (#109) ([f9163e9](https://github.com/orbita-rocks/orbita/commit/f9163e9beae4e6828cda2e901d66297d8095e947))
- Carry object write time on ObjectMeta for the orphan sweep (#112) ([d2053fd](https://github.com/orbita-rocks/orbita/commit/d2053fd7c57575e6e504fc41ed47f7866f473c6c))
- Install an OpenTelemetry exporter and emit the required signals (#110) ([9579fe0](https://github.com/orbita-rocks/orbita/commit/9579fe02841287059f94ab6969d1034dc4a0875d))
- Run the orphan sweep (#120) ([b466fc5](https://github.com/orbita-rocks/orbita/commit/b466fc5699b0b3da041318b615a4c90add3068a5))
- Interactive REPL over the same parser and renderer (#122) ([baca394](https://github.com/orbita-rocks/orbita/commit/baca39422222d134048b0de1085dfb361f43a996))
- Partition split with worker-prepared shared-segment children (ADR 0009) (#121) ([065cc4e](https://github.com/orbita-rocks/orbita/commit/065cc4e1f96ecf66962d913e6fd39ec9928877e2))
- Combine node roles and automate voters (#137) ([9e4d03b](https://github.com/orbita-rocks/orbita/commit/9e4d03ba56212fad93687e476bb5cc5eda23adc7))
- Support safe adjacent partition merges (#135) ([5e31fb8](https://github.com/orbita-rocks/orbita/commit/5e31fb832d1dff3dbf2890cf4836243d8282c611))
- Reach an authenticated OTLP collector over TLS (#162) ([ecdad8d](https://github.com/orbita-rocks/orbita/commit/ecdad8d42702465d1fb1be5be90df2f18a6fccb1))
- Distribute partition ownership across the cluster (#163) ([951521b](https://github.com/orbita-rocks/orbita/commit/951521b8e646a13a8eeec2fbfe3c848baedf38b8))
- Report what adopting a partition cost (#174) ([72e22ce](https://github.com/orbita-rocks/orbita/commit/72e22ce90877da6f8f2db7d0e0c7057b81079b82))
- Size the durability quorum independently of the read holder set (#176) ([1d87e96](https://github.com/orbita-rocks/orbita/commit/1d87e96b85cf57f10d7b16b1f5b2e9b84a52108d))
- Cache values read back out of segments (#179) ([c854f17](https://github.com/orbita-rocks/orbita/commit/c854f172c51c8a8999c814e77e392034a23c662f))
- Give every node a value cache and make it observable (#180) ([1e960f6](https://github.com/orbita-rocks/orbita/commit/1e960f67349bf59733c8c9d8eec967c74e68b20c))
- Fetch a window of records per miss, not one (#181) ([00aaf44](https://github.com/orbita-rocks/orbita/commit/00aaf44601cea3b11a39fd5fb88ad896c08a8353))


### Documentation

- Add the release roadmap and commit to a transactions direction (#9) ([b5a5945](https://github.com/orbita-rocks/orbita/commit/b5a594528c73744d97d7bad4ff44c8d174278038))
- Add operator scaling signals to v0.1.0 and v0.2.0 ([63cb769](https://github.com/orbita-rocks/orbita/commit/63cb769cba451d1248d7802a566ac4086e6ba2bb))
- Make partition splitting automatic, not an operator signal ([4d35ae7](https://github.com/orbita-rocks/orbita/commit/4d35ae7f01fafc9d5548b05a84208aa33efde940))
- Add AGENTS.md guidance for coding agents ([1ebfb03](https://github.com/orbita-rocks/orbita/commit/1ebfb03695c4ce20f4b2a01b018dffb1208256a1))
- Pin the draft window to v0.1.0 and record the gating constraint ([b4b2959](https://github.com/orbita-rocks/orbita/commit/b4b2959dc2e7e76637d1fb5413339f0f689755f0))
- Require IAM role assumption and instance-profile credentials for S3 ([9e5a628](https://github.com/orbita-rocks/orbita/commit/9e5a628bfe4c92501c8cb83ff5e72092cbaf4ce2))
- Stop describing RocksDB as the storage engine (#89) ([7510124](https://github.com/orbita-rocks/orbita/commit/75101244048cdf38bd93666d3e3e279ad29e558a))
- Split v0.3.0, schedule encryption at rest, restore merge to v0.1.0 ([b061cfe](https://github.com/orbita-rocks/orbita/commit/b061cfe02be315f6e3489f60a6dc5ee558d49b3b))
- Map where the read and write paths meet (#140) ([ad0f7f9](https://github.com/orbita-rocks/orbita/commit/ad0f7f9ac18cb41b9bbcf9b7ca09408b7e5a6a53))
- Correct what write capacity grows with (#173) ([b4a5c28](https://github.com/orbita-rocks/orbita/commit/b4a5c2892c46a0e6f1f989db07959ea167991078))
- Accept 0013, read serving is decoupled from the durability quorum (#175) ([020bbbc](https://github.com/orbita-rocks/orbita/commit/020bbbcca3e75ffde9acdcbddcb152f7df1dc78b))
- Write the v0.1.0 disclosures the commit history cannot carry (#185) ([52bf0b7](https://github.com/orbita-rocks/orbita/commit/52bf0b78b2e0682b4d302319a2825b5557c43d74))


### Fixed

- Keep credentials and signed requests out of debug logs (#15) ([442f0c5](https://github.com/orbita-rocks/orbita/commit/442f0c54369094b3a74de004f069943e08aa62de))
- Record reconcile outcome under the hosts lock and scope the rejoin claim ([56381ea](https://github.com/orbita-rocks/orbita/commit/56381eababc0b47108cee1b11347058713679c3e))
- Keep reconcile readiness test storage-agnostic ([3bb7275](https://github.com/orbita-rocks/orbita/commit/3bb727567e3d40db26b85be6789d0205996020c7))
- Keep the v0.0.1 control log and heartbeat encodings readable (#24) ([0b138ec](https://github.com/orbita-rocks/orbita/commit/0b138ec1ef48d9980117d893050a0043099db89d))
- Include finalize-upgrade in the Admin surface (#24) ([aed6b19](https://github.com/orbita-rocks/orbita/commit/aed6b19d759ee14581405afdc6f1c5610024fabf))
- Stop the raft driver on an undecodable committed entry ([9895491](https://github.com/orbita-rocks/orbita/commit/9895491b9467f93553f7d900648534c3cd9e5eff))
- Preserve handoff liveness and progress (#26) ([af8c09e](https://github.com/orbita-rocks/orbita/commit/af8c09ea4bd98750252f0b80a3d4ef2cb074fae9))
- Bound live flush side effects (#16) ([338c13c](https://github.com/orbita-rocks/orbita/commit/338c13c56d4ffd262729b4130720e9509b5c2c14))
- Preserve ownership across compatibility refusals (#27) ([864c076](https://github.com/orbita-rocks/orbita/commit/864c0762abdb6d59592c363ccceb6ac3f6e83c00))
- Preserve fencing across Raft failover (#23) ([c51a51b](https://github.com/orbita-rocks/orbita/commit/c51a51ba7571ad4ab003a604012bffd0bcbd0802))
- Gate leader authority on applied state (#23) ([dd2384d](https://github.com/orbita-rocks/orbita/commit/dd2384db84627dfcf580edc11b4b046c254bd429))
- Reconcile failover fencing with leader wiring ([f50d81e](https://github.com/orbita-rocks/orbita/commit/f50d81ea41071d678ca6c1a0a5a33d94e8edc519))
- Preserve durable failover across compatibility checks ([362655a](https://github.com/orbita-rocks/orbita/commit/362655adbc08f76b0731368fd47f5c3ef116e517))
- Integrate live manifests with cluster startup ([511b3c1](https://github.com/orbita-rocks/orbita/commit/511b3c14d46683a535858032b13f0319cb6e4c1f))
- Preserve rollback-safe handoff after develop merge ([d782e55](https://github.com/orbita-rocks/orbita/commit/d782e55f5cd3687b33b27fe29739f2848bce1664))
- Prevent unsafe partition transitions (#53) ([3b969a3](https://github.com/orbita-rocks/orbita/commit/3b969a3de5aed46b8233948ca76ff0fd698c05a6))
- Pin the WAL-truncation-before-hydration failure mode (#75) ([fde16a1](https://github.com/orbita-rocks/orbita/commit/fde16a19809448fe99eb704f9cf576f6ea186e75))
- Stop reusing a connection MinIO closed after a losing CAS (#85) (#86) ([bc47286](https://github.com/orbita-rocks/orbita/commit/bc472862578e4cb49fd51b6398ed65616b7e86f5))
- Let a draining owner catch its replicas up before handing off (#79) ([9f61985](https://github.com/orbita-rocks/orbita/commit/9f6198582aee14e2a29440bf682f8178dd1ee80f))
- Let a fenced owner recover its partition (#93) ([046d4a0](https://github.com/orbita-rocks/orbita/commit/046d4a02b134e81288cdc4ccbe0186777ea4b548))
- Give orbita dev a control plane and forward Admin to the leader (#94) ([eb6492d](https://github.com/orbita-rocks/orbita/commit/eb6492d80dfbadcbcf909b7a56681bb56cc2803b))
- Stop a drain from dropping a write it just acknowledged (#107) ([3f4dd15](https://github.com/orbita-rocks/orbita/commit/3f4dd15f994fcbb13b82584dd007d03fd64130ef))
- Size gRPC message limits from the keyspace ceiling (#111) ([b0a8098](https://github.com/orbita-rocks/orbita/commit/b0a809835137796c6d2fd2fd275fb57bc9bfcdb1))
- Drain against the committed prefix, not the durable tail (#117) ([b0ef3f6](https://github.com/orbita-rocks/orbita/commit/b0ef3f68f3f484d39e8f30b549adc0ff0a2c29dd))
- Give a contended if_not_present loser the winner's version (#123) ([a21f2d4](https://github.com/orbita-rocks/orbita/commit/a21f2d4cfe8e8ddfd5559afbdef0369576743f8b))
- Resolve new-worker placement against an old leader (#124) ([f14c19c](https://github.com/orbita-rocks/orbita/commit/f14c19c068fd2318e20f42b409fdf9646720fd66))
- Re-admit a replica to the read set only once it has caught up (#138) ([77b3312](https://github.com/orbita-rocks/orbita/commit/77b331220177e32a29c8c4f6b49ec649c482851e))
- Read the manifest before rebuilding the index (#145) ([8b0f9e4](https://github.com/orbita-rocks/orbita/commit/8b0f9e4d7686cf97ec3f8d6214a18dead067f7c1))
- Take compaction off the client write path (#146) ([d671ad0](https://github.com/orbita-rocks/orbita/commit/d671ad02645fdad2c3594de1d2d553a31c27ae33))
- Stop the smoke test passing against a local cluster (#148) ([e5aad98](https://github.com/orbita-rocks/orbita/commit/e5aad98b8f9ee7518b194c49c28d3eb6c6289acd))
- Let concurrent writes share an fsync (#149) ([af0816a](https://github.com/orbita-rocks/orbita/commit/af0816a9bf3e3a8471e6ffbff21f79f7de71b588))
- Keep bounded compaction debt moving ([01bacda](https://github.com/orbita-rocks/orbita/commit/01bacdae8c3c215370b95e153b4ad42a824428a0))
- Flush on a task no request can cancel (#151) ([e95f2b5](https://github.com/orbita-rocks/orbita/commit/e95f2b537cb87654b529584d276230091aa31a33))
- Make git-cliff actually run ([f7c7c66](https://github.com/orbita-rocks/orbita/commit/f7c7c66b43e65ed7ae1c56984d3b03467fbb663d))
- Stop failing writes a split has already finished with (#164) ([c829e60](https://github.com/orbita-rocks/orbita/commit/c829e601e30c24dff3545ab6d41252440212066b))
- Let a node outside the leader group become ready (#169) ([a27e7b7](https://github.com/orbita-rocks/orbita/commit/a27e7b7c318c023b79d5efe0e3028c5f53c34b27))
- Refuse to recover from a truncated control log (#172) ([5f8061b](https://github.com/orbita-rocks/orbita/commit/5f8061b997cedd6dbd2aa8e3dfa70e36ac2fb1e0))
- Bound the read-ahead scan and check segment identity (#183) ([255621c](https://github.com/orbita-rocks/orbita/commit/255621cd97cbea978f2dbb066899cd7258803bca))
- Keep trying a quorum while a replica has never answered (#186) ([9790f0d](https://github.com/orbita-rocks/orbita/commit/9790f0d85d5a739a3fa3819232ac68cc280b6275))
- Stop telling clients a quorum failure is safe to replay (#190) ([1f54ec8](https://github.com/orbita-rocks/orbita/commit/1f54ec869c7cc53404dcbc942df4919d5dade667))


### Performance

- Merge a bounded slice of a partition rather than all of it ([a52688d](https://github.com/orbita-rocks/orbita/commit/a52688d9c84cfc8c54860febeb76238000c0cf44))
- Finish a commit on the applier instead of a task per write (#157) ([be04f9b](https://github.com/orbita-rocks/orbita/commit/be04f9bd01d05fc1e3bdd6a7b619f1874db815c7))

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

### Fixed

- **The first writes to a new partition no longer fail while its replicas are
  still opening it** (#168). An owner admits writes the moment it opens, and
  nothing requires its replicas to have opened the same partition yet, so for a
  split child, a new placement, or every partition on a cluster that has just
  started, the first client write was the first thing to contact a replica —
  and there was no retry anywhere on that path. A 2,000 record load against a
  just-ready cluster lost 294 records to it. An owner now keeps trying while
  some replica has never answered, and still fails fast when a replica that was
  following goes away, because an outage is not a partition coming up.

### Changed

- **A write that could not reach a durability quorum is no longer reported as
  retryable** (#187). It now returns a distinct error and the gRPC status
  `UNKNOWN` rather than `UNAVAILABLE`, which most client stacks retry by
  default.

  The failure is raised after the entry is already durable on the owner, so the
  write may still take effect — the next open replays the log above the flush
  horizon. Replaying it is what turns one legal late landing into two writes.
  For a conditional write the damage is plainer: a lock taken with IF NOT
  PRESENT, refused, and retried is reported as lost to the client that holds
  it, which is the wedge use case failing quietly.

  **What a client should do instead is read the key back and decide from what
  it finds.** Nothing else can tell the two outcomes apart.

  A node running an older build decodes the new peer error code through its
  unknown-code path and reports it as internal. The wording is wrong and the
  advice is right: internal is not retryable either, so a half-upgraded cluster
  cannot tell a client to replay a write that may already have landed.

### Known issues

Disclosed rather than fixed. Each has an issue.

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
- **A write refused for want of a quorum may still land later** (#188), because
  the entry is durable on the owner before replication is attempted and the next
  open replays it. This is a documented outcome rather than a defect: the client
  is told the result is unknown, and a refused write landing later is a legal
  history, which the simulator's linearizability checker confirms. It is listed
  here because it surprises people, not because it is wrong.
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
