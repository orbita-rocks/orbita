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
fencing, split and merge, replica reads behind leases per ADR 0001, the admin
API and CLI, the deterministic simulator, and a Python end-to-end suite all
exist and pass.

Two things are deliberately staged rather than missing by accident. The control
plane runs its replicated state machine over a single-node consensus log, with
the `ConsensusLog` trait as the seam where Raft goes. And storage still runs on
RocksDB while [docs/format/partition-v1.md](docs/format/partition-v1.md)
specifies the open format that replaces it; the object storage trait exists but
nothing implements it yet.

## v0.1.0: the system, complete enough to mean it

The first release carries everything short of the measurement work. I
considered cutting a release from what exists today and spreading the rest
across several minors, but each of those releases would have shipped with a
caveat that undercuts the pitch: a control plane that dies with one node, a
storage format that is specified but not implemented, upgrades that cannot run
unattended. Since nobody is waiting on a tag, the first release should be the
one that does not need apologizing for.

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
index over immutable objects and that the format is specified and open. Today
that is a specification and a trait with no implementation behind either, and
the product's storage pitch, meaning workers cheap to replace and data
reachable without Orbita, is unearned until this lands.

- An S3-compatible implementation of the object storage trait (AWS S3, MinIO,
  R2), with conditional writes for manifest swaps.
- Segment write and manifest publication in the partition-v1 format, and
  hydration of a partition from a bucket, which is what makes replacing a
  worker a download rather than a peer-to-peer copy.
- Retiring RocksDB in favor of the memory-resident index over immutable
  objects per ADR 0006.
- Golden test vectors checked in as fixture bytes, per the format README's
  planned section, so a third party has something to conform to.
- A reserved commit timestamp field and intent flag in the record encoding,
  per the Transactions section of the requirements. The transaction work
  itself is post-1.0, but the bytes freeze with this release, and this
  reservation is the only part of that direction with a deadline.
- musl static builds become worth revisiting once the C++ toolchain dependency
  goes with RocksDB.

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
- The leader peer wiring the CLI already stubs out (`TODO(leader-peers)` and
  the peer listener in `orbita-cli`), so a three-node leader group is a
  configuration rather than a diagram.
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

## v0.2.0: the evidence

The theme is measurement. The acceptance criteria in
[docs/REQUIREMENTS.md](docs/REQUIREMENTS.md) all say measured, not estimated,
and this release is where the measuring happens. It follows everything else
because every earlier piece of work changes what would be measured.

- A partition-count knob in the simulator, and the experiment that finds where
  the leader group strains. The scale envelope calls this the number that turns
  the scaling argument from a design claim into a published one.
- The measured latency and throughput curves against worker count, replacing
  the target rows in the requirements table.
- The public correctness report: DST coverage, fault-injection results, and the
  linearizability verification under partitions, crashes, restarts, and clock
  skew.
- Whatever the measurements say to fix. This bullet is load-bearing; the
  honest outcome of a first benchmarking pass is a list of regressions.

Once this exists, planning 1.0 becomes a conversation about numbers rather
than intentions, and this roadmap gets its next revision.

## Later

In priority order, from the requirements:

1. **Watch/subscribe streams.** The known gap for the coordination use case,
   and the first thing the target persona asks about.
2. **The transaction ladder**, per the Transactions section of the
   requirements: snapshot timestamps on reads, which fixes the multi-page
   LIST limitation on its own; single-partition multi-key batches;
   cross-partition snapshot isolation; then strict serializability. The
   official smart client and the client-authoring doc (`docs/CLIENTS.md`)
   land alongside, since interactive transactions are what justify a real
   client library.
3. **A third-party Jepsen analysis**, once the system is stable enough that
   the report would be about Orbita rather than about churn.
4. **Additional object storage backends** (GCS, Azure) through the trait,
   likely community-contributed.
5. **A multi-region story**, which was fenced out of v1 to keep clock
   uncertainty out of the design space.

Multi-key transactions used to be off this list on purpose, conceded to
FoundationDB. The requirements now record why that reversed and what the
guarantee has to be; the short version is that Orbita already owns most of
what a verifiable transaction system needs, and evidence-backed transactions
are rarer than transactions.
