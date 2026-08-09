# 0011: Merge commands require cluster protocol 0.2

Status: Accepted, 2026-08-09.

Applies the rollout contract in
[ADR 0005](0005-upgrades-follow-kubernetes-rollouts.md) to the replicated merge
vocabulary introduced by ADR 0010.

## Context

A merge is four durable control decisions: begin, record each prepared holder,
complete, or abort. They use command tags 22 through 25. The protocol 0.1
binary only decodes tags through 21.

Calling the operation a protocol 0.1 feature because it was planned for the
0.1.0 milestone would let a new leader append a tag an old voter cannot apply
or recover. Both binaries would claim to speak the same active protocol while
disagreeing about its persisted alphabet. That would make the documented
mixed-version and pre-finalization rollback window false.

## Decision

Merge commands belong to cluster protocol 0.2. A controller refuses all four
before log proposal while the active protocol is below 0.2. The binary decodes
the commands for replay, but no live operation may emit them until
`orbita cluster finalize-upgrade` proves every live node speaks 0.2.

The 0.2 binary speaks 0.1 and 0.2. It can therefore roll through an active 0.1
cluster without changing behavior. Finalization is the explicit point that
enables merge and ends rollback to a 0.1 binary.

## Consequences

The persisted vocabulary and the compatibility claim agree. An old voter is
never asked to decode a merge entry, and a new voter can replay entries written
after finalization.

Merge cannot ship as an enabled operation in a 0.1.0 cluster. The implementation
may be present in the binary during the rollout, but the feature becomes usable
only at protocol 0.2. Keeping the v0.1.0 milestone would require redesigning
merge without new replicated commands, not relabeling those commands as 0.1.
