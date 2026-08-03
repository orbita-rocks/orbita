# 0005: Upgrades follow Kubernetes rollouts

Status: Accepted, 2026-08-03.

Supersedes the version skew behaviour described in `orbita-cli`'s node module,
which refuses to start on a version mismatch.

## Context

Kubernetes is the primary place Orbita will run, so the upgrade process should
be the one Kubernetes already performs rather than a second process layered on
top of it. A StatefulSet rolling update replaces pods one at a time in reverse
ordinal order, and it will not move to the next pod until the current one
reports Ready. An operator already knows `kubectl rollout status`,
`kubectl rollout undo`, and the `partition` field for staging a canary.
Inventing a parallel mechanism means teaching them a second one and keeping the
two honest with each other.

The behaviour we have today does not survive contact with that. A node refuses
to start when its version does not match the leader group's, which under a
rolling update means the first upgraded pod exits, enters CrashLoopBackOff, and
stalls the rollout with the cluster half upgraded. Refusing feels like the
conservative choice and is in fact the worst available outcome: the operator's
only escape is rolling back from a state the software chose to get stuck in.

Underneath that is a modelling error. A binary version and a protocol version
are not the same thing, and Orbita has several protocols that change at
different rates: the client gRPC API, the peer framing, the write-ahead log
format, the storage record encoding, and the control plane's replicated state.
Comparing binary versions collapses all of them into one answer that is
simultaneously too strict and too loose.

## Decision

### A cluster version, separate from the binary version

The control plane's replicated state holds the cluster's active protocol
version. Every binary declares the range of cluster versions it can speak, and
the supported window is the active version and the one before it. That is the
same n-1 skew rule Kubernetes uses between its own control plane and kubelets,
which means the constraint is already familiar to the audience.

A node joining a cluster reads the active version. Inside the window it starts
and speaks the active version, whatever its own binary version is. Outside the
window it starts, reports itself not Ready, and says why in its logs and its
status. It does not exit.

Not exiting is the whole point. A pod that is running and not Ready stops the
rollout at exactly one pod, keeps its diagnostics reachable, and leaves
`kubectl rollout undo` working normally. A pod that exits takes its own logs
away in a restart loop and gives the operator less to work with.

### Readiness means rejoined, not merely listening

The rollout's safety depends entirely on what Ready means, because that is what
gates the move to the next pod. So Ready means this node has registered with
the leader group, recovered its write-ahead log, opened every partition the map
says it holds, and caught up enough to serve them. It does not mean the socket
is bound.

Getting this wrong is the standard way stateful systems are broken on
Kubernetes: pods report Ready as soon as they are listening, the rollout
marches through all three replicas faster than the data can re-replicate, and
quorum is lost while every probe stays green.

Liveness is the opposite and must be conservative, because failing it kills the
pod. It checks only that the process is responsive, and never anything about
cluster state. A liveness probe that depends on reaching peers converts a
network partition into every pod being killed at once.

Slow starts belong to a startup probe rather than to a generous liveness
threshold, since recovering a large log legitimately takes time and that is not
the same as being wedged.

### A planned shutdown transfers ownership, it does not fail over

On SIGTERM a node hands its partitions to a caught-up replica and waits for the
handoff before exiting, within the pod's termination grace period. Raft leaders
step down the same way.

This matters more under Kubernetes than anywhere else. A rolling restart of
three workers is an ordinary weekly event, and if each restart went through
failure detection and promotion, every routine deploy would spend three
separate windows of up to ten seconds with writes unavailable. Failover is the
answer to a node dying unexpectedly. A deploy is not unexpected, and paying the
unexpected-failure cost for a planned event is a choice, not a necessity.

### Finalization is explicit, and it is the one-way door

When every node is upgraded, an operator runs `orbita cluster finalize-upgrade`,
which bumps the active cluster version after checking that every registered node
supports it. Only then do nodes begin writing new formats or using new
behaviour.

Before finalization, downgrade is `kubectl rollout undo` and nothing else,
because nothing new has been written. After finalization, downgrade is not
supported and the command says so before it proceeds.

Finalization is deliberately not automatic. Automatic finalization would remove
the rollback window precisely when an operator is most likely to want it, which
is a few minutes after a rollout completes and something looks wrong.

### Persisted formats carry their own versions

Each on-disk format keeps its own version independent of the cluster version:
the write-ahead log segment header and the storage record format byte already
do. A node reads every format version its window permits and writes the version
the active cluster version dictates. This is what makes the pre-finalization
rollback window real rather than aspirational.

## Consequences

**Rolling upgrades work with no Orbita-specific procedure.** The upgrade is
`kubectl set image` or a Helm upgrade, watched with `kubectl rollout status`,
followed by one finalize command.

**Staged rollouts come for free** through the StatefulSet `partition` field.
Canary one pod, verify, lower the partition, then finalize.

**The chart has to carry its weight.** Correct probes, a PodDisruptionBudget
that preserves quorum, a termination grace period longer than a handoff takes,
and a preStop hook if SIGTERM handling alone proves insufficient. A chart that
ships the wrong probe semantics silently undoes everything above.

**Mixed-version operation is a supported state, not an incident.** It has to be
tested that way, which means the simulator should run clusters with nodes at
two adjacent cluster versions rather than assuming uniformity.

**We carry compatibility code for one version window.** That is a real cost and
it is bounded: code for supporting version n-1 is deleted when the window moves
past it, and the window is one version wide by rule.

**ADR 0004 needs a correction.** It says peer framing has no compatibility
guarantee. The accurate statement is that peer framing is compatible within a
cluster version window, which is what makes any of this possible. Without that,
a rolling upgrade could not work at all.

## Alternatives considered

**Refuse to start on skew,** which is what we do now. Safe in isolation and
wedges a rollout into CrashLoopBackOff, leaving the cluster half upgraded with
no forward path.

**Start and hope the formats are close enough.** No check at all. This appears
to work and then corrupts something under load, which is the failure mode worth
the most effort to avoid.

**Automatic finalization once every node reports the new version.** One less
step, and it silently closes the rollback window at the exact moment an
operator would most want it open.

**Support downgrade after finalization** by writing formats old versions can
read, or by dual-writing through a transition. This is a permanent tax on every
format change, paid forever, for a capability nobody has asked for yet. Worth
revisiting when there is a production user with a real rollback requirement.

**An Orbita-specific upgrade orchestrator.** More control, and it competes with
the thing the operator is already using and trusts.
