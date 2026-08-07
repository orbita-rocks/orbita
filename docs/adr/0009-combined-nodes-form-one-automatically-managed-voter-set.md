# 0009: Combined nodes form one automatically managed voter set

Status: Accepted, 2026-08-07.

Supersedes the separate leader and worker topology in `docs/REQUIREMENTS.md`,
`docs/plan/03-control.md`, `docs/plan/04-server.md`, and
`docs/plan/06-ops.md`. It also supersedes the fixed-voter bootstrap described
in `orbita-cli`'s node module. It does not change ADR 0005's upgrade window or
ADR 0008's partition-owner fencing rules.

## Context

Orbita runs one binary in two clustered roles today. Three leader processes
hold control metadata in Raft and three workers provide the minimum three-copy
data path. A resilient minimum therefore takes six processes even though the
leader processes are mostly idle, and adding or replacing a voter means editing
the fixed `ORBITA_LEADER_PEERS` list on every node.

The roles did protect Raft from worker load, but process separation is not the
only way to reserve execution capacity. It also confused three different ideas:
the elected Raft leader, the nodes currently entitled to vote, and the nodes
serving data. They are responsibilities rather than process types.

Removing the static voter list creates two hard problems which have to be
solved together. A fresh group needs one cluster identity before it can run
Raft, and a running group needs to change voters through Raft rather than by
rewriting local configuration. A timeout followed by "the smallest node id
wins" solves neither. Two partitions can each wait out the timeout and create
two durable clusters with the same operator-facing name.

The shared object store is already part of every clustered Orbita deployment.
It provides conditional create through `ObjectStore::put_if`, and unlike a
node-to-node vote it remains one serialization point when the node network is
partitioned. I considered requiring an external discovery or coordination
service, but that adds another system to run in order to start this one. I also
considered deriving an identity from the first three nodes to answer, but two
disjoint groups of three can derive two answers. The object store is the
existing primitive that closes that hole.

## Decision

### One node process

Every clustered process is a node and every compatible, ready node may own or
replicate partitions. A node may additionally be a Raft voter. One voter is the
elected Raft leader at a time. Neither voter status nor leadership removes the
node from the worker data path.

The voter target is independent from node count. It is three by default and may
be set to five. No other value is accepted. A fourth node in a three-voter
cluster adds worker capacity and remains a Raft learner. A target increase from
three to five promotes two learners through separate membership changes.

### Durable identities

Each data directory holds a create-once node identity document containing a
format version, a random 128-bit node identity, the configured numeric node id,
and the cluster identity once one is known. The random identity distinguishes a
replaced disk from a restarted process even when an orchestrator reuses its
ordinal. A configured numeric id that disagrees with the durable document is a
startup error.

The shared object store holds one create-once identity at
`clusters/<cluster-name>/identity`. A fresh node proposes a random 128-bit
cluster identity with conditional create, then reads the winning value. It
persists that value and fsyncs it before issuing a bootstrap acknowledgement or
serving traffic. A node whose durable cluster identity differs from the object
store value stays running and unready; it never joins and never overwrites
either identity.

The cluster name is only the namespace used to find the identity. The random
identity is the authority. Reusing a name against a different bucket is a new
cluster; attaching an old data directory to it is refused because the durable
identity differs.

### Discovery is not membership

`cluster.seeds` contains peer endpoints or service-DNS names. It answers where
a node may find peers and carries no promise that any discovered node votes.
Nodes exchange the durable node identity, numeric id, advertised peer address,
failure domain, eligibility, binary version range, and cluster identity over an
additive versioned control method. Once Raft is established, the committed node
registry becomes the peer directory; seeds remain only a way for a new or
fully-restarting node to find that registry.

`ORBITA_LEADER_PEERS` remains readable for one rolling-upgrade window. On a
volume containing the old `control/raft-voters` file, its ids and addresses are
interpreted as discovery seeds and as the already-established voter set. They
are not written into a fresh combined cluster. The old `leader` and `worker`
role values likewise start the combined process during that window, with a
warning, so mixed old and new binaries can run before finalization.

### Bootstrap certificate

Fresh nodes publish signed-by-possession acknowledgements under
`clusters/<cluster-name>/bootstrap/claims/<node-identity>`. A claim contains the
cluster identity, numeric node id, peer address, failure domain, eligibility,
and a nonce, and is written only after the node has durably persisted the
cluster identity. Conditional create makes one durable identity issue at most
one claim.

Once at least three eligible claims name the winning cluster identity, every
node deterministically ranks them by failure-domain spread and then node
identity. The first three form a proposed bootstrap certificate. The
certificate contains the exact claims, cluster identity, protocol version, and
initial voter set, and is installed with conditional create at
`clusters/<cluster-name>/bootstrap/certificate`. All nodes read the winner and
only its three voters initialize Raft. A certificate naming fewer than three
eligible durable identities is invalid and startup remains unready.

The certificate is immutable. Nodes that arrive later learn the established
identity and voter set from it, start as workers and Raft learners, and register
through the elected leader. A full restart reads the same certificate and the
same local identities before opening Raft.

This prevents dual bootstrap for two reasons. All candidates first converge on
one object-store identity, and conditional create permits one bootstrap
certificate for that identity. A partition that can reach peers but not the
object store cannot create or read the certificate and therefore cannot start a
fresh Raft group. A partition that can reach the object store reads the same
certificate as the other side. Once any node has a Raft log or a durable cluster
identity it is never fresh again, so loss of quorum cannot send it back through
bootstrap. The protocol fails closed when the object store itself is not
linearizable for conditional writes; such a backend is already outside
`ObjectStore::put_if`'s contract and cannot safely hold Orbita manifests either.

### Voter placement and repair

Every node reports a failure domain, defaulting to an empty domain, and whether
it is eligible to vote. The target is placed by maximizing distinct non-empty
failure domains, then minimizing existing worker ownership, then ordering by
durable node identity. Missing domains are allowed because a three-node laptop
or Compose cluster still has to form, but they rank behind a candidate that
improves spread.

Only the current Raft leader runs the membership manager. It reads the actual
Raft configuration, the committed node registry, and leader-local health
observations. It submits at most one membership operation at a time and waits
until the resulting configuration is committed and applied before considering
another. Worker registration and partition placement continue while a
membership operation is active.

A healthy eligible learner is promoted when the voter count is below target.
Target growth from three to five therefore takes two committed transitions, not
one replacement of local configuration. Surplus voters are removed only after
the lower target has been committed as cluster policy and never below a quorum
that can commit the removal.

Raft configuration changes use `ConfChangeV2` joint consensus. Adding a voter
first adds and catches it up as a learner, then enters and leaves a joint
configuration that contains both the old and new quorums. Replacing a voter is
the same add-first sequence followed by removal of the old voter. There is no
application-level command that pretends membership changed; the Raft log and
its applied `ConfState` are the authority.

Worker health and voter replacement use different thresholds. The existing
three-second dead threshold may fence a partition owner to meet the data-path
failover budget. A voter is eligible for permanent replacement only after five
minutes of continuous `Dead`, never merely `Suspect`, and the timer resets on
any accepted report. A new control leader starts every replacement timer over
because the observations are leader-local. This hysteresis avoids membership
churn during a pause, rollout, or temporary partition while still repairing a
permanently lost voter. A removed voter that returns remains a worker and
learner unless a later repair chooses it again.

No side of a minority partition can replace voters. The membership manager can
run only after a leader barrier backed by the current quorum, and joint
consensus requires both old and new majorities while a transition is active.

### Reserved control execution

Production starts a dedicated Tokio runtime for Raft ticks, Raft persistence,
control peer handling, controller sweeps, and status heartbeats. Worker gRPC,
WAL replication, storage flushes, compaction, and client forwarding stay on the
worker runtime. The runtimes share the peer transport and disk handles, but not
executor worker threads or task queues. The control runtime has at least one
thread even when the worker runtime is saturated.

The simulator represents the same reservation with a control-ready queue that
is serviced at every bounded scheduling interval. Saturating worker tasks can
change interleavings but cannot postpone a due Raft tick past one heartbeat
interval. This is a liveness reservation, not a priority that changes protocol
ordering.

### Restart and migration

Restart opens identity before listeners, bootstrap, Raft, or storage. A node
with a certificate but no local Raft log rejoins as a learner. A node with a
Raft log must recover the cluster identity and applied configuration that log
names; disagreement is unready rather than a crash loop, following ADR 0005.

During the first rolling upgrade, old leader volumes retain their fixed voter
file and old worker volumes retain their worker data. New binaries recognize
both, persist the cluster identity beside them, and run the combined data path.
The three old voters remain the voter set until all nodes are upgraded and the
cluster version is finalized. Automatic promotion, removal, and use of the new
bootstrap wire methods are cluster-version-gated, because an old binary cannot
apply a new Raft configuration safely. Before finalization, replacement follows
the old static procedure. After finalization, `ORBITA_LEADER_PEERS` is ignored
for membership and may be removed from configuration.

A version mismatch never exits solely for being a mismatch. The process binds
its health surface, reports `cluster-version-compatible` or
`cluster-identity-matched` as unmet, and remains available for diagnosis and
rollback, preserving ADR 0005.

## Consequences

The resilient minimum is three Orbita processes instead of six. Every one
serves clients and data, while three also vote.

Adding worker capacity no longer edits existing nodes. A new node needs the
cluster name, shared object store, and seed DNS. It learns the committed peer
directory after first contact and does not enlarge quorum unless the voter
target requires it.

The shared object store is now on the clustered bootstrap path, not only the
data durability path. A fresh cluster cannot form while it is unavailable. An
established cluster does not consult it to elect or replace a Raft leader, so an
object-store outage after bootstrap does not redefine membership.

Five-minute voter repair is intentionally slower than partition-owner
failover. Voter replacement is a topology decision whose false positive costs a
joint-consensus transition; partition fencing is a data availability decision
already protected by epochs and leases. Giving the two one threshold would
optimize one of them in the wrong direction.

Combined nodes need explicit resource sizing. A node may now hold partition
indexes and a Raft log, and Kubernetes requests must leave the dedicated
control thread runnable. The chart spreads all nodes, while the membership
manager uses reported failure domains to spread the voter subset.

The old `leader` and `worker` names remain visible in upgrade documentation for
one compatibility window. In steady-state documentation, "leader" means the
currently elected Raft leader, "voter" means a node in Raft membership, and
"worker" means the data-path capability every node has.

## Alternatives considered

**Smallest node id bootstraps after a timeout.** Simple and unsafe. Two network
partitions can each choose a smallest id and create two durable clusters.

**Three peer acknowledgements without an external conditional write.** Better,
and still unsafe with six fresh nodes split three and three. Durable promises
stop one node signing twice; they do not force two disjoint quorums to overlap.

**Static bootstrap voters, dynamic voters afterwards.** Safe, but preserves the
operator-managed fixed map at the moment it is hardest to recover from a
mistake. Discovery seeds would still secretly be membership.

**An external coordinator such as etcd or Kubernetes Leases.** It provides the
needed uniqueness, and makes Orbita depend on another coordination substrate to
start. The object store already has the conditional primitive and is already a
cluster dependency.

**Every worker votes.** It removes a membership manager and makes quorum cost
and election disruption grow with worker capacity. Five additional storage
nodes should not turn a three-voter metadata group into an eight-member Raft
group.

**Replace a dead voter in one step.** Raft can encode it, but the replacement
has no committed prefix yet. Add as learner, catch up, then use joint consensus
so the cluster never counts a vote that cannot recover its state.
