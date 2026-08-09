# Architecture decision records

These record decisions that were argued about, where the reasoning matters as
much as the outcome. The requirements document says what Orbita does. These say
why it does it that way, and what we gave up.

A record earns its place when someone will later look at the code, think "that
is a strange way to do it," and be right to ask. If a decision is obvious from
the code or was never contested, it does not need one.

Records are immutable once accepted. When a decision changes, write a new
record that supersedes the old one and leave the old one in place with a link
forward. The history of a wrong turn is worth more than a tidy directory.

| Record | Title | Status |
|---|---|---|
| [0001](0001-linearizable-reads-from-replicas.md) | Linearizable reads from replicas | Accepted |
| [0002](0002-key-versions-are-partition-lamports.md) | Key versions are partition Lamports | Accepted, corrected |
| [0003](0003-conditions-evaluate-against-pending-writes.md) | Conditions evaluate against pending writes | Accepted |
| [0004](0004-peer-traffic-uses-private-framing.md) | Peer traffic uses private framing, not gRPC | Accepted, corrected by 0005 |
| [0005](0005-upgrades-follow-kubernetes-rollouts.md) | Upgrades follow Kubernetes rollouts | Accepted |
| [0006](0006-partitions-are-an-index-over-immutable-objects.md) | Partitions are a memory-resident index over immutable objects | Accepted |
| [0007](0007-large-values-are-their-own-objects.md) | Large values are their own objects | Accepted |
| [0008](0008-a-fenced-owner-stays-a-replica.md) | A fenced owner stays a replica | Accepted |
| [0009](0009-a-split-shares-the-parents-segments.md) | A split shares the parent's segments in place | Accepted |
| [0010](0010-a-merge-shares-both-parents-segments.md) | A merge shares both parents' segments in place | Accepted |
| [0011](0011-merge-commands-require-cluster-protocol-0-2.md) | Merge commands require cluster protocol 0.2 | Accepted |
