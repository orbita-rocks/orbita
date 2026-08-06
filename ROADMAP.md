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
Partition split runs the worker-prepared protocol: an operator-driven split
opens as intent, waits for every holder of the parent to ready storage for the
children, and only then retires the parent, so no key is ever owned by a
partition that has nowhere to put it. Triggering that split automatically from
size is still to come, and merge remains unimplemented.

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

### What v0.1.0 can still cut

Merge is the scheduled cut if the timeline slips, per the work briefs: it is
the riskiest coordination case in the system for the least immediate payoff.
The upgrade theme is the second candidate, since attended-only rollouts are a
warning label rather than a wrong claim. The format and Raft themes are not
cuttable; they are the difference between the pitch being true and not.

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
- Whatever the measurements say to fix. This bullet is load-bearing; the
  honest outcome of a first serious testing pass is a list of regressions.

Once this exists, planning 1.0 becomes a conversation about numbers rather
than intentions, and this roadmap gets its next revision.

## v0.3.0: watch, transactions, and the adoption path

The theme is closing the gaps the target persona actually hits, in the order
they ask about them. The first two are protocol additions, which is what a
minor version is for under the compatibility scheme, and both are
deliberately sequenced after v0.2.0 so they land on a harness that can check
them.

1. **Watch/subscribe streams.** The known gap for the coordination use case,
   and the first thing the target persona asks about.
2. **The transaction ladder**, per the Transactions section of the
   requirements: snapshot timestamps on reads, which fixes the multi-page
   LIST limitation on its own; single-partition multi-key batches;
   cross-partition snapshot isolation; then strict serializability. The
   official smart client and the client-authoring doc (`docs/CLIENTS.md`)
   land alongside, since interactive transactions are what justify a real
   client library.
3. **Coordination recipes.** A small library of the wedge patterns, meaning
   locks, leader election, fencing tokens, and epoch counters, run under the
   deterministic simulator like everything else. The requirements say these
   are built from CAS and linearizable reads, which today means every user
   hand-rolls the same retry loops and some get lease renewal or fencing
   subtly wrong and blame Orbita. A distributed lock with published
   fault-injection results is a claim nobody else in this market makes, and
   it is cheap because the harness exists. Recipes ship with watch because
   watch is what makes them efficient.
4. **The etcd migration path.** An importer from an etcd snapshot into a
   keyspace, and a migration guide mapping etcd concepts to Orbita's,
   meaning revisions to versions, leases to TTLs, and watches to watch. The
   positioning makes "I have an etcd cluster today" the modal prospect, and
   the docs currently have no answer to their first question.

The ladder's stages are individually shippable, so if this release needs to
split, it splits along them: watch, recipes, and the early stages first, the
cross-partition work in a minor of its own, shifting the numbers below.

## v0.4.0: security

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
