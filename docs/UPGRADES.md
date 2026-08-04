# Upgrades

There is no Orbita upgrade procedure. On Kubernetes the upgrade is a
StatefulSet rolling update, which you already know how to drive, plus one
command at the end. The reasoning is in
`docs/adr/0005-upgrades-follow-kubernetes-rollouts.md`.

## What does not work yet

Read this first. The chart is written for the process below; the server is not
finished.

- The cluster version and `orbita cluster finalize-upgrade` exist. The control
  plane holds the version in its replicated state, every node reports the
  range of versions its binary can speak on every heartbeat and reads the
  active version back, and finalizing advances the version only after checking
  every live node can speak the new one. What does not exist yet is the
  enforcement around it: a node outside the window says so in its logs but
  still joins, and no behaviour actually changes with the version because no
  format has two versions to choose between yet. Mixed-version operation is
  therefore still not something you should rely on today.
- A node does not hand its partitions off on SIGTERM. The termination grace
  periods in the chart are sized for a handoff that the server does not perform
  yet, so today a restart is a failover.
- The `orbita` binary does not yet hand the leader group configuration to the
  server. A node started by the CLI runs without a control client, and the
  `control-plane-joined` readiness condition is met by construction on such a
  node rather than by an accepted registration. The server asserts the join
  whenever it is configured with a leader group, which is how the in-process
  cluster tests run it; the CLI wiring is the missing piece.

Readiness is otherwise real: a node reports Ready only once it has recovered
its write-ahead log and opened and caught up the partitions the map says it
holds, and it turns unready again if a map change hands it a partition it
cannot open. What a deployed cluster does not get yet is the rejoin half,
per the gap above.

The practical consequence of the gaps that remain: stage a rollout with the
`partition` field and check the cluster between steps when the blast radius
warrants it, because each pod replacement is still a failover and the rollout
does not wait for a rejoin it cannot see.

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

A node that cannot speak the cluster's active version is meant to start,
report itself not Ready, and say why in its logs. It is not meant to exit.
That is deliberate: a pod that is running and not Ready stops the rollout at
exactly one pod and leaves its diagnostics reachable, where a pod that exits
takes its logs away in a restart loop. Today the node starts and says why in
its logs; readiness does not reflect it yet.

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
  has recovered its write-ahead log, opened and caught up its partitions, and
  registered with the leader group where it has one. Until the CLI wires the
  leader group through (see the gaps at the top), that last condition is met
  by construction on a deployed node.
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
