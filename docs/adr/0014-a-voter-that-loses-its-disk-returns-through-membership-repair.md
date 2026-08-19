# 0014: A voter that loses its disk returns through membership repair

Status: Accepted, 2026-08-19.

Extends ADR 0011. It supersedes two of that record's specific behaviors: the
treatment of a certified numeric id arriving with a fresh disk, and the rule
that repair never removes a dead voter while the voter set is at target. The
bootstrap protocol, the certificate's immutability, and every other placement
and repair rule in ADR 0011 stand unchanged.

## Context

A bootstrap voter that loses its data directory can never rejoin the cluster
(issue #193). The node mints a fresh durable node identity on its empty disk,
reads the immutable bootstrap certificate, finds its numeric id bound to the
identity the old disk held, and refuses to start anything but a health
listener. It refuses forever, and each restart mints another identity, so it
can never converge.

The refusal itself is correct in isolation. A voter's durable Raft state is
the record of its promises: the term it voted in and who it voted for. A node
that opens Raft under a voter id without that state can vote twice in a term
it has already voted in, and two leaders in one term is the failure Raft
exists to prevent. The certificate check is the only thing standing between an
amnesiac disk and that outcome, so it cannot simply be deleted.

The problem is that everything downstream turns a correct refusal into a
permanent one. The refused node never heartbeats, so the leader cannot see
it. The membership manager cannot choose it as a replacement because its id is
already in the voter set. Removal of a dead voter happens only when the voter
set exceeds target, and a three-node cluster has no spare node to trigger
that, so the dead seat is never vacated. The cluster runs degraded at two of
three voters indefinitely, and the only recovery we have exercised is a full
reset that destroys the cluster.

This shape is not a corner case. Kubernetes reuses StatefulSet ordinals, the
chart derives the numeric node id from the ordinal, and a lost persistent
volume with a reused ordinal is an ordinary failure in the exact environment
the deployment docs recommend. Three nodes is the floor the documentation
advertises, and these clusters are meant to operate hands-free. A common
failure with no automated recovery path on the minimum topology fails both
promises at once.

ADR 0011 already states the intended behavior in one sentence: a node with a
certificate but no local Raft log rejoins as a learner. The code does not
deliver that sentence when the certificate names the node's id. This record
decides how to deliver it without giving up the amnesia protection.

## Decision

### Displacement is a distinct outcome

Bootstrap distinguishes three cases where it previously saw two. A data
directory whose durable cluster identity disagrees with the object store is
still refused outright; that disk belongs to another cluster and no automated
action on it is safe. A node whose identity matches its certificate entry
joins as before. The new case is a node whose identity document was created
this boot, whose disk holds no Raft log, and whose numeric id the certificate
binds to a different identity. That node is displaced, not mismatched: the
seat it once held now belongs to a dead incarnation of itself.

Displacement is detected mechanically, from evidence on disk, with no
configuration and no operator signal. That is deliberate. Any knob that asks
an operator to confirm "yes, the disk is really gone" reintroduces the manual
step this record exists to remove, and any heuristic softer than "the
identity file was created this boot and there is no Raft log" starts guessing
about disks that may hold promises.

### A displaced node is a worker with a pending claim

A displaced node starts the full worker data path, heartbeats to the leader
under its new identity, and reports one dedicated unmet readiness condition
so an operator watching `cluster describe` can see the state and its
progress. It does not open Raft.

Not opening Raft is the load-bearing rule. Even a node that considers itself
a learner answers vote requests, and the rest of the group still believes the
contested id is a voter, so any Raft participation under that id risks the
double vote. The displaced node therefore stays out of consensus entirely
until the group has, through committed configuration changes, released the
old incarnation's seat.

### After first commit, membership is the identity authority

The bootstrap certificate remains immutable and remains the birth record: it
is how a fresh cluster forms exactly once and how late arrivals learn the
established identity. But it stops being the last word on who holds a seat.
Once a Raft log exists, the committed membership, which records node id,
address, and durable node identity for every member, is the living authority,
and the certificate check applies only to nodes with no committed history to
consult. This is the same principle ADR 0011 applies to discovery seeds:
bootstrap inputs stop being membership the moment Raft can speak for itself.

### The leader treats an identity change as a new incarnation

Status reports carry the durable node identity. When a report arrives for a
known node id with an identity that differs from the one committed membership
records for that id, the leader does not revive the old record. The old
incarnation's health keeps decaying toward permanently dead, its replacement
timer keeps running, and the new incarnation is tracked as a pending
returnee. Without this rule the fresh incarnation's heartbeats would mark the
dead voter healthy and suppress the very repair that would seat it.

One ambiguity is refused rather than resolved. If both incarnations of an id
are reporting at once, that is not a failed disk, it is a configuration error
such as two processes sharing one id. The membership manager stops, surfaces
a diagnostic, and waits. Every automated choice in that situation is a guess,
and this is the one branch where a human is genuinely required.

### A displaced voter is reseated by remove-first repair

ADR 0011 rejected replacing a dead voter in one step and required add-first
replacement. That rule assumed the replacement is a different node. A
returned incarnation reuses its numeric id, and Raft cannot hold two
incarnations of one id, so add-first is not conservative here, it is
impossible. For this case only, repair runs remove-first: remove the dead
voter through joint consensus, then add the returned incarnation as a learner
under its new identity, catch it up, and promote it. Each step is one
committed configuration change, submitted one at a time, exactly as every
other repair operates. Add-first remains the rule whenever the replacement is
a different node.

Remove-first opens a window at two voters, where quorum is two and the group
tolerates no further failure. That window is accepted, with guards. The
remove step fires only after the old incarnation has been continuously dead
for the full five-minute replacement threshold, only while every remaining
voter is healthy, and only when no other membership change is in flight. The
honest comparison is not "three voters versus two"; the cluster is already
effectively at two of three, and the choice is between a guarded window
minutes long and a degradation with no end.

### The rendezvous rides the heartbeat

The displaced node needs to learn when the group has reseated it, and the
channel already exists. The heartbeat response tells the node how committed
membership currently regards its id. When membership binds the node's id to
its own identity as a learner, the node opens Raft, catches up, and is
promoted by the ordinary sweep. No new discovery protocol, no object-store
polling, and no ordering problem: the leader commits the learner add first,
and the node acts only on what the leader reports back.

## Consequences

Recovery from a lost voter disk is automatic end to end on the minimum
topology. The sequence from volume loss to a restored three-voter set is
bounded by the heartbeat cadence, the five-minute replacement threshold, and
learner catch-up, with no operator action at any point. The displaced state
is visible in readiness output throughout, so hands-free does not mean
invisible.

The five-minute threshold doubles as the flap filter. A transient volume
detach comes back with the same identity and simply resumes; nothing in this
record triggers on it. Seat surgery requires a genuinely new incarnation and
a genuinely silent old one, sustained for the full threshold.

A repeated failure mid-repair is resumable. If the disk is lost again, the
newest incarnation becomes the pending returnee and the sequence re-runs;
one-at-a-time membership changes make every interruption a clean state to
continue from.

The certified-id refusal test changes its expectation from mismatch to
displacement, and the displaced path needs a simulation scenario that wipes a
voter's disk, restarts it, and proves the group returns to three healthy
voters with no double vote. The scenario must fail without this change.
Bootstrap's wait loop moves onto the runtime clock so the simulator can drive
it at all.

The two-voter window is a real cost and is the deliberate price of the
three-node floor. Clusters of four or more nodes will usually never enter it,
because a spare node absorbs the seat add-first and the returned node simply
remains a worker.

## Alternatives considered

**Keep refusing, document a manual recovery.** Safe and simple, and it makes
the advertised minimum topology one lost volume away from a permanently
degraded cluster that only a full reset can fix. Hands-free operation was the
requirement; this fails it by construction.

**An operator command that rewrites the certificate.** Recovery by editing
the birth record. It breaks the certificate's immutability, which is what
makes dual bootstrap impossible, and it adds the operator step this record
exists to remove. Rejected on both grounds.

**Let the amnesiac rejoin as a voter directly.** The double vote. A node
without its durable Raft state must never be counted in a quorum that
believes it remembers its promises.

**Add-first with the same id.** Raft membership cannot contain two
incarnations of one id, so there is nothing to add until the old seat is
removed. The rejected one-step replacement in ADR 0011 was a choice; this is
an impossibility.

**Mint a new numeric id for the returned node.** It preserves add-first by
making the returnee look like a fresh node. But the id comes from
configuration, derived from the StatefulSet ordinal, and ADR 0011 makes a
configured id that disagrees with the durable document a startup error.
Shifting ids under a stable ordinal moves the churn into configuration and
into every operator's mental model of which pod is which. The identity
document already exists to tell incarnations apart; using it is cheaper than
renumbering the fleet.
