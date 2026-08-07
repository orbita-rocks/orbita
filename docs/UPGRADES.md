# Upgrades

There is no Orbita upgrade procedure. On Kubernetes the upgrade is a
StatefulSet rolling update, which you already know how to drive, plus one
command at the end. The reasoning is in
`docs/adr/0005-upgrades-follow-kubernetes-rollouts.md`.

## What does not work yet

Read this first. The chart is written for the process below; the server is not
finished.

- The cluster version, compatibility enforcement, and `orbita cluster
  finalize-upgrade` exist. The control plane holds the version in its
  replicated state and refuses a registration whose speakable range does not
  include it. The refused node stays running, reports
  `cluster-version-compatible` and `control-plane-joined` as unmet readiness
  conditions, and logs both its range and the active version. No behaviour
  actually changes with the version yet because no format has two versions to
  choose between. Planned worker handoff is the first version-gated behavior:
  workers continue using the previous status protocol and ordinary failover
  until the operator finalizes the new cluster version.
Readiness is otherwise real: a node reports Ready only once it has recovered
its write-ahead log and opened and caught up the partitions the map says it
holds, and it turns unready again if a map change hands it a partition it
cannot open. A leader voter also has to apply through the commit index reported
by the current Raft leader. Seeing a leader is not enough.

A worker also reports `replicas-recoverable` as unmet when it owns a partition
whose replica has fallen further behind than its write-ahead log still reaches.
WAL truncation is live and hydration from object storage is not, so that
replica cannot be recovered and the partition is permanently short a copy. The
rollout stops there on purpose: continuing would take out another copy of a
partition that has already lost one. Replace the replica, or wait for issue
\#17. `orbita cluster describe` shows how far each replica has applied, and the
owner logs which one it is and the two Lamports that bracket the gap.

Stage a rollout with the `partition` field and check the cluster between steps
when the blast radius warrants it. After finalization, a worker drains its
partitions on SIGTERM; before finalization it exits through ordinary failover so
the rollback-compatible control protocol remains on the replicated log.

## What a version means

Two of them, and keeping them apart is the whole idea.

The **binary version** is what is in the image tag. It follows semver and it is
what `orbita --version` prints.

The **cluster version** is the protocol and format version every node in a
cluster has agreed to speak. It lives in the control plane's replicated state
and changes only when an operator finalizes an upgrade. A node speaks the
cluster version, not its own binary version, which is what lets nodes of
different binary versions work together at all.

A binary supports the active cluster version and the one before it. That is the
same n-1 window Kubernetes uses between its control plane and kubelets, and it
means one upgrade at a time: going from 0.3 to 0.5 requires stopping at 0.4.
Skipping is not supported and the node will tell you rather than guess.

Before 1.0 the minor version is the compatibility unit, because that is where
breaking changes go while the formats are still moving. Patch releases mix
freely.

Persisted formats carry their own version independently: the write-ahead log
segment header and the storage record format byte both do already. A node reads
any format version its window allows and writes the version the active cluster
version calls for. That is what makes the rollback window below real rather
than a hope.

## The first upgrade to Raft leaders

This section is a one-time transition for a release whose leader pods do not
run Raft. Skip it when the installed release already has a working leader
quorum.

A normal StatefulSet rolling update cannot cross this boundary. It replaces
one pod and waits for that pod to become Ready before replacing the next. The
first new pod is one Raft-capable voter beside two old processes that cannot
vote, so it cannot reach two-of-three and can never become Ready.
`podManagementPolicy: Parallel` does not change rolling replacement order; it
only changes initial creation and scaling.

Render the one-time transition by setting:

```
helm upgrade orbita deploy/helm/orbita --namespace orbita \
  --set image.tag=0.1.0 \
  --set leader.firstRaftUpgrade=true
```

This changes the leader StatefulSet to `OnDelete`. It does not replace any pod
on its own. Replace ordinals 1 and 2 together, then wait for both new processes
to form a quorum and pass the catch-up readiness gate:

```
kubectl --namespace orbita delete pod orbita-leader-1 orbita-leader-2
kubectl --namespace orbita wait --for=condition=Ready \
  pod/orbita-leader-1 pod/orbita-leader-2 --timeout=10m
```

Do not continue unless both are Ready. Once they are, replace the remaining old
voter and wait for its local Raft log and controller to catch up:

```
kubectl --namespace orbita delete pod orbita-leader-0
kubectl --namespace orbita wait --for=condition=Ready \
  pod/orbita-leader-0 --timeout=10m
```

Run Helm once more with `leader.firstRaftUpgrade=false`. The pod template is
already current, so this restores `RollingUpdate` without another restart.
Every later upgrade follows the ordinary procedure below.

This exception replaces two leader pods together because no one-at-a-time
sequence can create the first quorum. The worker data plane remains available
on its last map while the leader group forms. It is not a general permission to
restart two Raft voters together after this transition.

## Migrating to combined nodes

The first release implementing ADR 0009 reads the old `ORBITA_LEADER_PEERS`
configuration and the durable `control/raft-voters` file for one compatibility
window. Old leader processes remain the three voters and old workers remain
data-only while old and new binaries coexist. A new binary started with
`--role leader` or `--role worker` preserves that old role and logs that the
spelling is transitional; it does not automatically change Raft membership
during the rollout.

Roll the old leader and worker StatefulSets under their existing names and
volumes first. Do not switch to the combined chart shape in the same Helm
operation, because Kubernetes cannot automatically reattach six old claims to
three new pod names. Once every process runs the new binary, finalize the
cluster version, drain the old workers, and move the three voter volumes to the
combined node StatefulSet. The voter volumes already contain the authoritative
Raft membership and cluster identity, so the object-store bootstrap certificate
is migration metadata rather than permission to form another group.

After finalization, new combined nodes use the shared object store and cluster
name to discover the durable identity and voter certificate. They join as
workers and learners. `ORBITA_LEADER_PEERS` may then be removed; changing it no
longer changes membership. A volume whose persisted cluster identity disagrees
with the bucket starts unready and names `cluster-identity-matched` rather than
exiting, which keeps rollback diagnostics available per ADR 0005.

The elected leader, voters, and workers are deliberately separate terms after
this migration. There is one elected leader, three or five voters, and every
node is a worker even when it is also one of those voters.

## The rollout

Upgrade the image. With Helm:

```
helm upgrade orbita deploy/helm/orbita --namespace orbita --set image.tag=0.4.0
```

Or directly:

```
kubectl --namespace orbita set image statefulset/orbita-worker orbita=ghcr.io/orbita-rocks/orbita:0.4.0
```

Watch it:

```
kubectl --namespace orbita rollout status statefulset/orbita-worker
```

Pods are replaced one at a time in reverse ordinal order, and the rollout does
not move to the next pod until the current one reports Ready. That is the whole
safety mechanism, which is why readiness asserts recovery and catch-up rather
than just a listening socket, and why the rejoin gap above is a gap worth
closing rather than a footnote.

Upgrade the leader group and the workers separately. There are two StatefulSets
and they are two rollouts.

## Canary one pod first

The `partition` field holds the rollout at an ordinal. Set it to one below the
highest, so only the last pod is replaced:

```
kubectl --namespace orbita patch statefulset orbita-worker \
  -p '{"spec":{"updateStrategy":{"rollingUpdate":{"partition":2}}}}'
```

Then change the image. Only `orbita-worker-2` is replaced. Check it:

```
orbita cluster describe
kubectl --namespace orbita logs orbita-worker-2
```

When you are satisfied, lower the partition to 0 and the rest follow.

## Rolling back

Before finalization, a rollback is the ordinary Kubernetes one, because nothing
has written a new format yet:

```
kubectl --namespace orbita rollout undo statefulset/orbita-worker
```

One caveat for the transition off 0.0 specifically. The first version-aware
release writes control log entries the 0.0 binary cannot read, and 0.0's
recovery truncates its log at the first entry it cannot decode. So once a
control-plane node has run the new binary, rolling that node's binary back to
0.0 discards whatever the new binary committed. The upgraded binary reads
everything 0.0 wrote, so the forward direction is safe; it is the return to
0.0 that is not, and this is a one-time cost of the version machinery not
existing yet when 0.0 shipped.

A node that cannot speak the cluster's active version starts, reports itself
not Ready, and says why in its logs. It does not exit.
That is deliberate: a pod that is running and not Ready stops the rollout at
exactly one pod and leaves its diagnostics reachable, where a pod that exits
takes its logs away in a restart loop. The refusal includes the node's
speakable range and the active cluster version, so the stopped rollout
identifies which side needs changing.

The registration check is part of the replicated state machine, not a worker
preflight. A refused worker is not added to membership and cannot receive
ownership. If finalization makes a previously registered node incompatible,
the node keeps serving partitions it already holds. This preserves the data
plane during a control-plane transition. It is excluded from placement and
cannot be promoted or added as a new replica until it reports a compatible
range again.

## Growing a cluster mid-upgrade

Adding a worker while the rollout is half done works, and the new worker is
placed. A node inside the window speaks the active cluster version rather than
its own binary version, so a newer worker in a cluster that has not finalized
yet is a node of the active version in every respect the leader group reasons
about: the same status protocol, the same on-disk formats, and ordinary
failover rather than a planned handoff on shutdown. There is nothing about it
an un-upgraded leader has to understand and cannot.

The readiness a worker reports is the exception, and it is not a gap in what
the leader knows so much as one in what it is allowed to write down. That claim
travels in a registration shape the previous binary cannot decode, so it stays
off the replicated log until you finalize, which is what keeps the rollback
below real. Before finalization no node makes the claim and no node is judged
on it; after finalization every node makes it and every node is judged on it.
Both binaries decide which of those they are in from the active cluster
version, not from their own, so they never disagree about a node they are both
looking at.

The practical consequence is that placement during the upgrade window ignores
readiness and uses health, role, and version compatibility, which is the
pre-0.1 rule. A worker that is up but still recovering can therefore be given a
partition during the window; it opens it when it can, the same as it would have
before any of this existed. Finalize when the rollout is done and the stricter
rule comes back.

The v0.0.1 heartbeat remains a deliberate special case. Its registration has
no version field, so the control plane treats it as speaking exactly version
0.0. It is accepted only while 0.0 is active. A newer worker can still fall
back to the legacy heartbeat when talking to a v0.0.1 leader, but it reports
Ready only if its own range includes 0.0. This keeps the first rolling upgrade
working without turning a missing field into an unlimited compatibility claim.

## Finalizing

When every node is upgraded and you are happy, bump the cluster's active
version:

```
orbita cluster finalize-upgrade
```

Only after that do nodes start writing new formats or using new behaviour.
Until then the rollback above is available and cheap. After it, rolling back is
not supported, and the command says so before it proceeds.

Finalization is not automatic on purpose. Doing it automatically would close
the rollback window at the exact moment an operator is most likely to want it,
which is a few minutes after a rollout finishes and something looks off.

The command checks before it commits: if any live node cannot speak the new
version, nothing changes and the error names the nodes holding it back. A
node the cluster has declared dead does not get a vote, because a lost node's
last act should not be pinning the cluster to an old version.

### If you need to go back after finalizing

You cannot, and the honest answer is to plan as though that is true. Restoring
from a backup taken before the upgrade is the only path, and backup tooling is
itself not built yet, so today the answer is to not finalize until you are sure.

Supporting downgrade after finalization would mean writing formats that older
versions can still read, or writing both formats through a transition. That is
a cost paid on every format change forever, and it is not worth paying before
somebody is running this in production with a real rollback requirement.

## Upgrading outside Kubernetes

The shape is the same, done by hand. Nothing about Orbita requires Kubernetes;
it is just where the tooling does the tedious part for you.

One node at a time, and wait for each one to rejoin before starting the next.
The reason is the same reason the rolling update waits for Ready: replacing a
second node while the first is still catching up can take a partition below the
replicas it needs.

Never restart two members of a three member leader group at once. That loses
quorum, and the cluster stops accepting metadata changes until one comes back.
Check the group is otherwise healthy before touching a member.

With Compose, one service at a time:

```
docker compose up -d --no-deps orbita-worker-2
```

Then confirm it is back before moving on:

```
orbita cluster describe
```

Finalize once every node is upgraded, the same as above.

## When a rollout goes wrong

**The rollout stops at one pod.** This is the mechanism working. Kubernetes is
refusing to replace another pod because the current one never became Ready. Look
at that pod before doing anything else:

```
kubectl --namespace orbita describe pod orbita-worker-2
kubectl --namespace orbita logs orbita-worker-2
```

Do not force the rollout past it. A stuck rollout is one pod down; a forced one
can be all of them.

**A pod is in CrashLoopBackOff.** A version mismatch is not supposed to cause
this, since a node outside the window is meant to run and report not Ready. If
you see a crash loop, look for a configuration error or an unreadable data
directory rather than a version problem.

**The cluster is half upgraded and you want out.** Before finalizing, roll back:
that is what the window is for, and nothing new has been written.

**Quorum is lost in the leader group.** Stop restarting things. The data path
keeps serving reads from workers that already have their partition map, so the
damage is bounded while you work out which members are down and bring them back.
Restarting more members to try to fix it is how a recoverable situation becomes
a restore.

## Probes, and why they are shaped this way

Three probes, three different jobs. Getting these wrong is the usual way a
stateful system is broken on Kubernetes.

- Readiness gates the rollout and the client Service. It runs `orbita cluster
  ready`, which exits non-zero and names the unmet conditions until the node
  has recovered its write-ahead log, opened and caught up its partitions,
  registered with the leader group where it has one, and confirmed that its
  binary can speak the active cluster version. A leader-group node also waits
  for its durable Raft state and applies through the current leader's commit
  index before it becomes Ready.
- Liveness kills the pod when it fails, so it runs `orbita cluster ping` and
  checks only that the process responds. It never checks cluster state and
  never checks whether peers are reachable, because a liveness probe that
  depended on peers would turn a network partition into every pod being killed
  at once.
- Startup covers a slow start with the readiness command and its own generous
  budget. Recovering a large write-ahead log or waiting for the leader group
  takes time, and that is not the same as being wedged, so the startup budget
  is generous where the liveness threshold is not.

The thresholds are under `probes` in the chart's values.
