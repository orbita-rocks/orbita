# 0012: Merge commands belong to cluster protocol 0.1

Status: Accepted, 2026-08-11.

Places the replicated merge vocabulary introduced by
[ADR 0010](0010-a-merge-shares-both-parents-segments.md) inside protocol 0.1
rather than deferring it to 0.2 under the rollout contract in
[ADR 0005](0005-upgrades-follow-kubernetes-rollouts.md).

## Context

A merge is four durable control decisions: begin, record each prepared holder,
complete, or abort. They use command tags 22 through 25.

An earlier draft of this ADR assigned them to protocol 0.2. The reasoning was
that a binary speaking only 0.1 decodes tags through 21, and recovery treats an
unknown tag as the end of the trustworthy log and truncates there. A new leader
appending tag 22 to a log an old voter must recover would therefore not merely
fail that command, it would destroy the tail. Deferring merge to 0.2 kept the
mixed-version and rollback window honest.

That reasoning is sound and its premise is not. It protects a 0.1 voter that
cannot decode merge tags. No such voter exists and none ever has:

- There is no `v0.1.0` tag and no GitHub release. Nothing has shipped.
- 0.1 is therefore still being defined, not preserved. Naming the alphabet of a
  protocol before anything speaks it is not a compatibility break, it is the
  ordinary act of deciding what the protocol is.
- The only artifacts carrying 0.1 are `0.1.0-dev` prereleases, which are
  explicitly not a compatibility promise.

Deferring merge to 0.2 to protect a population of zero costs a real thing: the
partition lifecycle stays half-built, with split landed and merge unavailable,
through a release that could have carried both.

## Decision

Merge command tags 22 through 25 are part of cluster protocol 0.1. The 0.1
alphabet runs through tag 25.

The control plane still refuses a merge on a cluster that has agreed no version
at all, because a command whose vocabulary nobody has accepted is the case the
gate was always genuinely protecting against. It no longer requires
`orbita cluster finalize-upgrade`, because there is no earlier protocol to
finalize away from.

## Consequences

A `0.1.0-dev` prerelease built before this ADR cannot share a cluster with one
built after it. The older binary decodes through tag 21 and would truncate its
log at the first merge command. This is acceptable precisely because
prereleases carry no promise, but it means a mixed prerelease cluster spanning
this change is not a supported configuration and must be replaced rather than
rolled.

The version-gating machinery stays. `speaks_for` still widens the window for a
non-zero minor, the active-version comparison still governs the lifecycle gate,
and `finalize-upgrade` still exists. What changes is that nothing exercises the
two-version path until a real 0.2 arrives, so the first genuine protocol change
will be the first live test of it. That is a real cost and the reason to keep
the mechanism rather than delete it along with its only current user.

This supersedes the draft of ADR 0012 that assigned merge to 0.2. That draft was
never merged, so no accepted decision is being revised.
