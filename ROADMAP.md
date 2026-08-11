# Roadmap

This document maps the remaining work to the releases that will carry it. It
exists because the requirements say what v1 is and the work briefs say how to
build it, but neither says what ships when, and without that ordering every
conversation about scope starts from scratch.

Versions follow the scheme in [docs/UPGRADES.md](docs/UPGRADES.md): before 1.0
the minor version is the compatibility unit, breaking changes to protocols and
formats land in minors, and patch releases mix freely. The release mechanics,
meaning the workspace version as the one source of truth and the tag-driven
pipeline, are in [docs/RELEASING.md](docs/RELEASING.md) (PR #7).

This roadmap deliberately stops short of planning 1.0. The 1.0 claim is the
acceptance criteria in [docs/REQUIREMENTS.md](docs/REQUIREMENTS.md) holding
with published evidence, and there is no point scheduling that claim until the
system is further along and the first measurements exist.

## Where the system stands

More is built than the version number suggests. The KV surface (GET, SET,
DELETE, LIST with cursors, CAS and IF NOT PRESENT, TTLs), keyspaces with
credentials and quotas, the 2-of-3 replicated WAL, owner failover with epoch
fencing, replica reads behind leases per ADR 0001, the admin API and CLI, the
deterministic simulator, and a Python end-to-end suite all exist and pass.
Partition split works end to end under ADR 0009. The data plane shares the
parent's immutable segments in place with no copy: a child indexes the parent's
objects restricted to its range, so every pre-split key is readable from exactly
one child, and the orphan sweep is cross-partition so it never reclaims a segment
a child still references. A worker fetches the split intent over the control
wire, quiesces the parent's writes — draining to the committed prefix so no
acknowledged write can land above the horizon the children inherit — prepares
both children, and reports the durable acknowledgement the control plane's
completion waits on. Simulation proves no acknowledged write is lost across a
split under contention. Automatic size-triggered splitting (issue #39) and merge
remain unimplemented.

One thing is deliberately staged rather than missing by accident: the control
plane runs its replicated state machine over a single-node consensus log, with
the `ConsensusLog` trait as the seam where Raft goes.

Storage used to be the other one, and is not any more. Partitions run on the
open format specified in
[docs/format/partition-v1.md](docs/format/partition-v1.md) over
`orbita_objectstore::ObjectStore`, with an S3-compatible implementation behind
the trait and a filesystem one for a node with no bucket. RocksDB is gone, per
[ADR 0006](docs/adr/0006-partitions-are-an-index-over-immutable-objects.md).

## v0.1.0: the system, complete enough to mean it

The first release carries everything short of the measurement work. I
considered cutting a release from what exists now and spreading the rest
across several minors, but each of those releases would have shipped with a
caveat that undercuts the pitch: a control plane that dies with one node,
upgrades that cannot run unattended, and, when this was written, a storage
format that was specified but not implemented. Since nobody is waiting on a
tag, the first release should be the one that does not need apologizing for.

The work groups into four themes, and they are roughly the order to build in.

### Release and build mechanics

Both are open PRs and land first, because everything after them wants CI and a
repeatable release path.

- The release pipeline (PR #7): version from the workspace `Cargo.toml`,
  binaries for four targets, a signed multi-arch image, the chart, and a GitHub
  release cut by a tag.
- The moon build (PR #5), so what CI runs and what a laptop runs are the same
  definition.

### The open format and object storage

This makes the storage story true. The requirements claim partitions are an
index over immutable objects and that the format is specified and open, and
the product's storage pitch, meaning workers cheap to replace and data
reachable without Orbita, stays unearned for as long as any of that is only
written down. The engine half is written now: partitions run on partition-v1
over `orbita_objectstore::ObjectStore`, an S3-compatible store implements the
trait with the conditional writes manifest swaps depend on, and a worker is
replaced by hydrating from a bucket rather than copying from a peer. What
follows is the rest of the theme.

- Golden test vectors checked in as fixture bytes, per the format README's
  planned section, so a third party has something to conform to.
- A reserved commit timestamp field and intent flag in the record encoding,
  per the Transactions section of the requirements. The transaction work
  itself is post-1.0, but the bytes freeze with this release, and this
  reservation is the only part of that direction with a deadline.
- musl static builds, which were a project of their own while the build needed
  a C++ toolchain for the storage engine. That toolchain left with RocksDB, so
  what is between here and a static binary is build configuration.
- Resource visibility in `orbita cluster describe`: memory held by the
  partition indexes per node, per-partition size, WAL lag, and quota
  saturation. This lives in the format theme because the format work created
  the need: retiring RocksDB made the index memory-resident, so memory is now
  the resource that runs out first, and shipping that change with no way to
  watch it is exactly the kind of caveat this release exists to avoid.
  The thresholds that turn these numbers into scaling decisions come later;
  the raw signals cannot.

Folding this into the first release also dissolves a conflict the earlier
draft of this roadmap had to flag: the format spec says its draft window closes
when the first release ships, and now the first release is the one that writes
the bytes. The freeze and the implementation arrive together.

### The leader group becomes a group

The `ConsensusLog` seam was built so that Raft slots in without touching the
state machine above it; this is where it slots in. A control plane that does
not survive losing its node is not something to hand an infrastructure
engineer whose whole job is judging failure modes.

- Raft under `ConsensusLog` via an existing implementation, openraft or
  raft-rs, with its storage and network wired through `orbita_runtime` so the
  simulator can drive it.
- Leader peer wiring through the CLI and peer listener, so a three-node leader
  group is a configuration rather than a diagram.
- Leader group failover under fault injection in the simulator, since the
  failover ordering argument (epoch commit before promotion) now has to hold
  across a real election.

### Upgrades that keep their promise

These are the gaps [docs/UPGRADES.md](docs/UPGRADES.md) lists under "what does
not work yet." The chart is already written for the process; the server does
not hold up its end, and until it does, every rollout has to be babysat.

- The cluster version in the control plane's replicated state, nodes gating
  behavior on it, and `orbita cluster finalize-upgrade`.
- Readiness that means ready: rejoined the leader group, WAL recovered,
  partitions open and caught up, not merely answering on the client port.
- Partition handoff on SIGTERM, so a rolling restart is a drain rather than a
  string of failovers.
- The n-1 compatibility window enforced rather than documented: a node that
  cannot speak the active cluster version says so instead of guessing.

### The scope corrections

Two rounds of review have grown this release rather than shrunk it. The first
found the multitenancy enforcement gap (the wave 4 issues). The second, a
planning pass on 2026-08-07, put two cut items back:

- **Partition merge returns.** It was the scheduled cut, and the cut is
  reversed: a range-partitioned store that can split and never merge treats
  fragmentation as permanent, and the requirements call merge in scope for v1.
  It is still the riskiest coordination case in the system, so it sequences
  strictly after split re-enablement (#97) and carries the version-uniqueness
  correction from ADR 0002 as its acceptance test.
- **#84, unified node roles and automated Raft voter management**, is pulled
  in from the unscheduled list. It touches leader-group membership, so it
  waits for the handoff path to settle (#77) before it starts.

The cost is said out loud: the release gets longer, and if the date starts to
matter these two are the cut line back to the previous plan.

Upgrade review found that merge adds replicated command tags a 0.1 voter cannot
decode, and an earlier draft of ADR 0012 therefore put the operation behind
cluster protocol 0.2. That was withdrawn before it merged. The voter it
protected does not exist: nothing has been released, so 0.1 is still being
defined rather than kept compatible with, and merge tags 22 through 25 are part
of its alphabet. Merge is an enabled v0.1.0 feature and needs no finalization.

### What v0.1.0 can still cut

#84 is the remaining cut item. The upgrade theme is the next candidate, since
attended-only rollouts are a warning label rather than a wrong claim. The
format and Raft themes are not cuttable; they are the difference between the
pitch being true and not.

## v0.2.0: testing and correctness

The theme is that the test system grows up before the feature surface grows
again. v0.1.0 ships a system whose correctness argument rests on the
deterministic simulator, and v0.3.0 pulls a transaction protocol and a
streaming surface into that system. Between them is the release where the
instruments get built, because verifying serializable histories and long-lived
subscriptions demands more from the harness than verifying per-key
linearizability does, and building the checker after the protocol is how the
protocol ships unchecked.

This section is deliberately vague. The work needs a plan of its own, and
writing that plan is the first deliverable of the release. The shape of it:

- Improved harnesses: more of the system drivable by the simulator, more
  fault types, longer and larger schedules, and the knobs the scale envelope
  says are missing, partition count first.
- A regression discipline: every bug found, in the simulator or in the field,
  becomes a seeded regression test that runs forever.
- Tests at scale: real clusters, sustained load, and the measured latency and
  throughput curves that replace the target rows in the requirements table.
- Backup and point-in-time restore, plus an offline format reader. Both fall
  out of ADR 0006 nearly for free: immutable segments and manifests mean PITR
  is retaining old manifests and restore is pointing at one, and a tool that
  dumps a bucket with no cluster running is what makes the open-format
  promise checkable rather than aspirational. They live in this release
  because restore tooling doubles as test infrastructure.
- The operator kit: curated dashboards, alert rules with thresholds tied to
  the SLOs the acceptance criteria define (WAL lag, failover duration, quota
  saturation), and runbooks for the incidents every operator hits. It lands
  here because the tests-at-scale work needs the same instrumentation to be
  trustworthy. The kit also owns the scaling signals: the thresholds that
  tell an operator when to add a worker or grow the leader group, surfaced
  through `cluster describe` on top of the raw numbers v0.1.0 exposes. They
  land here rather than with the numbers because a credible threshold comes
  from the measured envelope the tests-at-scale work produces, and a
  threshold invented before the measurements exist is a guess with a UI.
  Partition splitting is deliberately not on that list: the split and merge
  machinery already exists and the decision needs no capacity the cluster
  does not have, so the same threshold that would have paged an operator
  triggers the split automatically instead. The operator signals are the
  ones that require hardware to show up.
- Correctness evidence as a stream, not a one-shot report: nightly seeded
  simulation runs published continuously, with the launch report assembled
  from them. A report ages; a public record of interleavings explored and
  violations found compounds, and it is the form of the claim a skeptical
  infrastructure engineer can keep checking.
- The watch/subscribe design doc. v0.3.0 builds watch, and nothing scheduled
  designing it; writing the design here means the feature release starts
  building rather than deciding, and the harness work can grow the knobs a
  long-lived subscription will need to be tested with.
- gRPC health checking and server reflection on the client surface. Small,
  and the operator tooling this release ships will want a standard health
  protocol to point at anyway.
- Whatever the measurements say to fix. This bullet is load-bearing; the
  honest outcome of a first serious testing pass is a list of regressions.

Once this exists, planning 1.0 becomes a conversation about numbers rather
than intentions, and this roadmap gets its next revision.

## v0.3.0: watch and the easy half of the ladder

An earlier revision of this roadmap put watch, the whole transaction ladder,
the smart client, and the etcd importer in one release, and admitted in the
same breath that it would probably split along the ladder's stages. The
2026-08-07 planning pass performed that split up front rather than under
pressure, which is when splits produce the wrong halves.

The theme is closing the gaps the target persona hits first, with the work
that needs no timestamp oracle. Both protocol additions are deliberately
sequenced after v0.2.0 so they land on a harness that can check them.

1. **Watch/subscribe streams.** The known gap for the coordination use case,
   and the first thing the target persona asks about. Built from the design
   doc v0.2.0 writes.
2. **Coordination recipes.** A small library of the wedge patterns, meaning
   locks, leader election, fencing tokens, and epoch counters, run under the
   deterministic simulator like everything else. The requirements say these
   are built from CAS and linearizable reads, which today means every user
   hand-rolls the same retry loops and some get lease renewal or fencing
   subtly wrong and blame Orbita. A distributed lock with published
   fault-injection results is a claim nobody else in this market makes, and
   it is cheap because the harness exists. Recipes ship with watch because
   watch is what makes them efficient.
3. **Snapshot timestamps on reads**, the first rung of the transaction
   ladder, which fixes the multi-page LIST limitation on its own.
4. **Single-partition multi-key batches**, the second rung, which need no
   oracle at all because the owner already serializes.

## v0.4.0: transactions and the adoption path

The hard half of the ladder, and the pieces that only pay off once it exists.

1. **Cross-partition snapshot isolation, then read-set validation**, which
   upgrades it to strict serializability, per the Transactions section of the
   requirements.
2. **The official smart client**: the Rust core library and the
   client-authoring doc (`docs/CLIENTS.md`), landing alongside the
   cross-partition work because interactive transactions are what justify a
   real client library, and a routing cache alone did not.
3. **The etcd migration path.** An importer from an etcd snapshot into a
   keyspace, and a migration guide mapping etcd concepts to Orbita's,
   meaning revisions to versions, leases to TTLs, and watches to watch. The
   positioning makes "I have an etcd cluster today" the modal prospect, and
   the docs currently have no answer to their first question. It lands here
   rather than with watch because the guide's answer to "how do I migrate my
   transactional workloads" should exist before we invite the migration.

## v0.5.0: security

The theme is being trustworthy on a hostile network, which is a precondition
for the production-adoption claim rather than a feature. The requirements
cover credentials and quotas but were silent on transport security and
granular authorization until this roadmap forced the question; the Security
section of the requirements now specifies what this release delivers.

- TLS on the client surface and mutual authentication between peers,
  completing what ADR 0004's private framing starts, with certificate
  rotation that does not require a restart.
- Authorization finer than the keyspace: credentials grantable per key
  prefix, since a coordination substrate shared by teams needs the same
  range-scoped permissions etcd users already expect.
- An audit log of administrative and credential operations, because the
  target buyer's security review will ask for it by name.
- Encryption at rest: envelope encryption of the WAL and the object segments
  under one KMS-backed cluster key. This reverses a fence the requirements
  drew, and the reversal is recorded there; the short version is that "the
  object storage backend provides it" was never true of the WAL, which lives
  on local worker disks and holds every acknowledged write that has not yet
  flushed.

Multitenancy without transport security and granular authorization is a demo,
not a product, and this release is scheduled before any 1.0 conversation for
exactly that reason.

## Later

In priority order, from the requirements:

1. **A third-party Jepsen analysis**, once the system is stable enough that
   the report would be about Orbita rather than about churn, and ideally
   covering the transaction protocol from v0.3.0 rather than stopping short
   of it.
2. **Additional object storage backends** (GCS, Azure) through the trait,
   likely community-contributed.
3. **A Kubernetes operator** for day-2 automation. The v0.1.0 upgrade work
   makes rollouts safe and the v0.2.0 kit makes them observable; an operator
   makes them boring, and it should wait until the procedures it automates
   have stopped changing.
4. **An etcd API shim, as a labeled experiment.** A kine-style facade that
   lets existing etcd clients run against Orbita would be the single biggest
   adoption lever if it works, and a tar pit of etcd semantics, meaning
   compaction revisions and watch guarantees, if approached casually. It gets
   a research spike with permission to fail, not a promise.
5. **A multi-region story**, which was fenced out of v1 to keep clock
   uncertainty out of the design space.

Multi-key transactions used to be on this list, and before that they were off
every list on purpose, conceded to FoundationDB. The requirements record why
that reversed and what the guarantee has to be; the short version is that
Orbita already owns most of what a verifiable transaction system needs, and
evidence-backed transactions are rarer than transactions. Scheduling them at
v0.3.0 is what v0.2.0's investment in the harness is for.
