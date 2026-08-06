# 0008: A fenced owner stays a replica

Status: Accepted, 2026-08-05.

Records the failover decision issue #76 asked for and no record covered.
Nothing is superseded. [ADR 0001](0001-linearizable-reads-from-replicas.md)
requires a promoted owner to wait out the deposed owner's read leases before it
accepts a write; that is unchanged, and this decision leans on it.

## Context

Failover today is three committed steps. The leader group notices an owner has
stopped talking, commits a fence that removes the owner and bumps the epoch in
one entry, waits out the read leases the old owner granted, and then promotes
the survivor that reports the highest durable position. The epoch bump and the
loss of ownership being one entry is what makes it impossible to promote before
fencing, and that ordering is not in question here.

The fence did one more thing: it took the deposed owner out of `replicas`. That
produced a state the cluster cannot leave. A fenced partition with no live,
eligible member of `replicas` is never owned again, whatever else the cluster
has available, because no sweep stage owns that state.
`promote_drained_partitions` draws candidates only out of `info.replicas`,
`place_unowned_partitions` only looks at `Unowned`, and `repair_replica_sets`
only looks at `Serving`. Reads and writes to the partition are unavailable for
the life of the cluster, and there is no operator path out: the only
ownership-moving admin call is `TransferOwnership`, which requires the target to
already be in `replicas`.

Three routes reach it, none of them exotic. Both replicas can be declared dead
and retired by `repair_replica_sets` before the owner dies, so the fence lands
on an already-empty set. The set can be full at the fence and its members die
afterwards, leaving the deposed owner as the only copy and the only node the
fence excluded. Or the leader can be wrong about all of it: a one-way network
fault makes a perfectly healthy worker look dead, and the failover it triggers
strands a partition nobody needed to move.

Underneath the three routes is one question, which is what makes this worth a
record: what does a replica set name? It has named live copies. A node the
leader believes is dead is dropped from it, which throws away the record of
where the data actually is at exactly the moment that record is worth the most.
The deposed owner is the sharpest case, because it is the node the cluster is
most certain about and the one the fence deleted.

## Decision

**The fence demotes the deposed owner into the replica set instead of removing
it.** It is then a promotion candidate like any other, judged on the position it
reports.

That is the whole change. `fence_partition` still clears `owner` and still
bumps the epoch in the same entry. `AssignOwner` still refuses a partition that
has an owner. The lease drain still runs before any promotion. The promotion
rule in `best_candidate` is untouched: highest reported durable position wins, a
candidate must have reported at or beyond the map version the fence produced,
and ties break on node id.

### Gated on cluster-version finalization

The demotion is conditional on the active cluster version being at or above
0.1, the version that introduces it, and takes effect only once
`orbita cluster finalize-upgrade` has committed that version. Until then the
fence drops the deposed owner exactly as it did before, on every member.

This gate is a correctness requirement, not a rollout convenience. The map is a
replicated state machine: every controller replays the same committed
`FencePartition` entry, and the guarantee that they all hold identical state
depends on every member applying that one entry the same way. The two binaries
present during a rolling upgrade do not agree about this entry on their own — a
pre-0.1 binary drops the deposed owner, a 0.1 binary keeps it — so deciding on
the running binary would let a single committed entry produce divergent maps,
and an old binary elected during the fenced interval could then serve and
propose from state no other member holds. Per `docs/UPGRADES.md` the active
cluster version is the one thing every member agrees on regardless of its
binary, and it moves only at finalization, after every member that cannot speak
the new version has been refused. Gating on it makes the whole n-1 window apply
the old rule and switches to the new rule only once every member runs a binary
that agrees. This follows the pattern the state machine already uses for
`CompleteFenceDrain`, which is gated on the same version for the same reason.

### Why this cannot lose an acknowledged write

The guarantee at stake is the one in `docs/REQUIREMENTS.md`: owner failover
loses zero acknowledged writes. The argument is four steps.

1. **An owner holds everything it has acknowledged.** A write is acknowledged
   when `Wal::committed_lamport` reaches it, and `advance` clamps that watermark
   to the owner's own `durable_local`. An owner is a member of every durability
   quorum it counts, so its disk is a superset of the committed prefix by
   construction, not by luck.
2. **So the deposed owner is the best copy the cluster has, not a risky one.**
   At the instant of the fence no surviving node holds an acknowledged entry the
   deposed owner does not. Anything the deposed owner has and others do not is
   above the committed prefix, which is entries whose `commit` returned
   `Unavailable` and which no client was told had landed.
3. **It is still promoted on evidence, not on identity.** Nothing special-cases
   it. A deposed owner that comes back with a shorter log, from a torn tail or a
   replaced disk, reports a lower position and loses to a replica that did not.
   A deposed owner that has not reported since the fence is not promoted at all.
4. **The fence still fences.** The epoch bump is untouched, so a re-promoted
   owner serves at a strictly higher epoch than the incarnation that was fenced.
   `orbita_wal`'s replica side refuses an append below the epoch it holds, which
   is what stops the old incarnation's in-flight writes whether or not the node
   that gets promoted next happens to be the same machine.

The direction of the risk is worth stating plainly, because the framing in #76
had it the other way round. Excluding the deposed owner was not the safe
default. It was a hole. `best_candidate` skips replicas the leader believes are
dead and promotes the furthest live one, so once `repair_replica_sets` has
retired a replica, an entry acknowledged at the owner and that retired replica
sits on no live candidate, and a promotion commits it away. The deposed owner is
the node that closes that gap, and it was the one node not allowed to.

The emptied-replica-set case makes it sharpest. `Wal::replicate` computes
`required = replicas.len().div_ceil(2)`, so a partition whose replica set has
been emptied acknowledges writes on the owner's disk alone. There the deposed
owner is not merely the best candidate. It is the only candidate that is not
data loss, and promoting anything else would be a lost-write bug dressed as a
recovery.

## Consequences

**The map names a copy that may be down.** A fenced partition's replica set can
list a node the leader records as dead. That is the right reading of what a
replica set is during a failover, meaning who is known to hold this, and the
sweep already tolerates it: `repair_replica_sets` retires the node once the
partition is `Serving` again. What repair means by a replica set is unchanged,
so this does not become a general redefinition by the back door.

**A spurious failover can hand the partition back to the node it took it from.**
That costs one epoch bump and one lease drain, and it lands the partition on the
node that needs no catch-up. It cannot loop, because promotion requires the node
to be healthy and eligible, and a node that keeps disappearing keeps failing
that test.

**A re-promoted owner resurrects its own uncommitted tail.** Entries between the
committed prefix and its durable position had `commit` return `Unavailable`, and
promoting the node turns them into history. This is not new: the `orbita_wal`
crate documentation already says that whatever a promoted owner has on its disk
becomes history. What changes is the size of the window, because a deposed
owner's tail can be longer than a replica's. This is the acceptable side of the
trade. `Unavailable` is an ambiguous answer and clients are told to check rather
than assume; a lost acknowledged write is not ambiguous, and it is the thing the
product is sold on. Narrowing the window means persisting the committed prefix
so a recovered owner can truncate to it, which is worth doing and is not this
change.

**A partition whose every copy is gone stays unavailable.** Nothing here rescues
a partition that has lost all three disks, and nothing should.

**The recovery is inactive during a rolling upgrade to 0.1.** Until the cluster
version finalizes to 0.1 the fence still drops the deposed owner, so the stuck
state issue #76 describes remains reachable in the upgrade window. That is the
accepted price of applying one committed entry identically on every member. A
cluster mid-upgrade is a cluster an operator is already tending, the window is
bounded by the finalize step, and a divergent map is a worse failure than a
partition that recovers a finalize later. A cluster that bootstraps fresh on
0.1 finalizes to 0.1 as it starts, so the gate only defers the behaviour for an
actual old-to-new upgrade, never for a new install.

**`Controller::transfer_ownership` now re-adds the deposed owner redundantly.**
It computes its replica list from pre-fence state, so it is correct either way,
and it is left alone rather than churned.

## Alternatives considered

**Promote out of `PartitionPhase::Fenced { deposed }` directly,** leaving the
map untouched and special-casing the deposed node in the promotion rule.
Rejected because the leader does not have the evidence. A worker builds its
status report from `PartitionMap::held_by` and reconciles its open partitions
from the same place, so a node the fence removed from the map closes the
partition and reports nothing further about it. Promoting it would therefore
mean promoting on identity rather than on a reported position, which is exactly
what the rest of `best_candidate` refuses to do, and the missing evidence is
missing precisely in the crash-and-return case this is meant to fix. Getting it
back would mean workers reporting partitions the map does not name, which is a
larger change to what a status report is, for a worse answer than putting the
node back in the map.

**Stop `repair_replica_sets` emptying a replica set,** making it name copies
rather than live copies everywhere. Rejected as the fix for this, and not
because the underlying idea is wrong. It is the same idea this record acts on.
It changes what the map means in every path that reads it at once, including
placement, redundancy accounting, and the durability quorum, and it still does
not close the case: the route through a full replica set at the fence reaches
the stuck state with nothing for repair to have declined to do. This decision
takes the insight and applies it to the one node whose copy is certain.

**Place a fenced partition with no candidate as if it were `Unowned`.** Rejected
outright. It is available and empty, which is losing every acknowledged write in
the partition and reporting it as a recovery. Unavailable is a state the system
can describe honestly; silently empty is not.

**Leave it to an operator.** That was the status quo and it is not an option:
there is no admin command that edits a replica set, so the only path out of the
state was hand-editing replicated control state.
