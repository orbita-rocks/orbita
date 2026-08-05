# 05: Deterministic simulation (`orbita-sim`)

The harness that makes Orbita's headline claim checkable. It implements every
seam in `orbita-runtime` against virtual time, an in-memory network, and a
fault-injecting disk, then runs whole clusters inside one thread driven by a
seeded generator.

This starts in the first wave even though nothing depends on it yet. Every
other brief's interesting tests are written against it, and a simulator built
after the fact tends to be a simulator the system cannot actually run under.

## Scope

- `SimRuntime`, implementing `Runtime`, `Clock`, `Disk`, `Transport`, and
  `Rng`.
- A deterministic scheduler. Tasks run one at a time in an order derived from
  the seed. Same seed, same interleaving, every time, on every machine.
- Virtual time. `sleep` advances a clock rather than waiting, so a test can
  cover an hour of lease expiry in a millisecond, and time only moves when
  every runnable task is blocked.
- Network fault injection: drop, delay, duplicate, reorder, and partition,
  including asymmetric partitions where A reaches B but B does not reach A.
  That case finds bugs symmetric partitions do not.
- Disk fault injection: write failures, partial writes, torn tails, corruption
  on read, and fsync that reports success without persisting. The last one is
  what real disks do and what most systems get wrong.
- Node lifecycle: crash a node, restart it with its disk intact, or restart it
  with its disk gone, since replacing a machine is the common cloud case.
- A linearizability checker. Record the history of client operations with their
  invocation and completion instants, then verify that some sequential ordering
  explains it. Wing and Gong or Knossos style search is fine; it need only
  handle the small histories a test generates.
- A test harness that runs N seeds, and reports the seed and a replayable trace
  on failure. A failure that cannot be replayed from its seed is a bug in this
  crate.

## Out of scope

- Performance measurement. Virtual time says nothing about real latency.

## Storage under simulation

The section that used to sit here described a limit: RocksDB did its own file
I/O beneath `orbita_runtime::Disk`, so storage was a trusted component with
faults injected at its API boundary. Per
[ADR 0006](../adr/0006-partitions-are-an-index-over-immutable-objects.md) the
storage engine now persists exclusively through
`orbita_objectstore::ObjectStore`, and per this brief's own instruction the
limit was deleted rather than reworded.

That seam now has faults injected through it, and the crate is not what does
the injecting. `orbita_sim::SimBucket` implements
`orbita_objectstore::s3::HttpTransport`, so a simulated partition persists
through the same `S3Store` production runs: the signing, the mapping of a
status onto the error taxonomy, the conditional-write headers, and the
paginated listing are all under test rather than stubbed out. That is why this
crate depends on `orbita-objectstore` with only its `s3` feature, which is the
split that feature was cut for.

The faults are the ones object storage actually produces: a request that never
arrived, a response that was lost after the store applied the request, a
throttle, and a crash placed exactly between two store calls. The second is
the one that earns the design. A conditional write whose answer was lost and
one that never landed are indistinguishable to the writer and differ by
whether the partition's data is durable, so the manifest swap being the
durability boundary is a claim with a failure mode rather than a definition.

The scenarios live in `orbita-server`, in `src/durability.rs`, because that is
where the ordering they pin lives.

## Decisions to make and write down

- **Scheduler shape.** Cooperative single-threaded with an explicit ready
  queue, or a custom `Future` executor with seeded poll ordering? The second is
  more faithful and harder.
- **Trace format.** What is recorded, and can a failing run be replayed from
  the trace alone or only from the seed plus the same binary? Seed-only replay
  is simpler and breaks whenever the code changes, which is often exactly when
  you want to replay.
- **Fault budget.** Injecting faults on every operation finds bugs but explores
  a shallow state space, since the system spends all its time recovering. Decide
  how faults are scheduled and make it tunable per test.

## Done when

- The same seed produces a byte-identical trace across runs and across
  machines.
- A deliberately introduced bug, such as acknowledging a write at one of three
  replicas instead of two, is caught by the linearizability checker within a
  modest number of seeds. This is the test of the tester, and it should be a
  permanent part of the suite rather than a one-off check.
- A failing seed reported by CI reproduces locally with one command.
- The suite runs a batch of seeds per pull request in minutes, and a much
  longer batch nightly.
