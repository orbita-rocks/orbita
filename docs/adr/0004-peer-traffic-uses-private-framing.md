# 0004: Peer traffic uses private framing, not gRPC

Status: Accepted, 2026-08-03.

Overrides `docs/plan/04-server.md`, which asked for a "production gRPC peer
Transport". That was written before the protocol definitions existed and does
not survive contact with them.

## Context

Nodes talk to each other for three things: WAL replication, control plane
traffic, and proxying a client request to a partition owner. All of it goes
through `orbita_runtime::Transport`, which carries an opaque payload tagged
with a `ServiceId`, so each subsystem defines and encodes its own messages.

The WAL and the control plane both did exactly that, independently, and both
hand-rolled a compact binary encoding. The WAL's is deliberate: a replicated
entry travels in the same framing it has on disk, so the replica writes the
owner's bytes through unchanged and the checksum is verified end to end rather
than recomputed after a decode.

Making the transport gRPC would mean adding a peer service to `/proto` that
wraps those payloads in protobuf messages, which raises the question of what
that buys. The answer is not much. Peer traffic is internal, it is versioned
with the binary rather than with a published contract, no external client will
ever speak it, and the payloads are already framed.

There is also a concrete cost. The WAL's end-to-end checksum property depends
on the bytes arriving as they were written. Wrapping them in a protobuf field
preserves that, but every future contributor now has two framings to reason
about, and the temptation to "just decode it properly" at the boundary is a bug
waiting for someone in a hurry.

## Decision

Peer traffic uses a private length-prefixed framing over its own TCP listener,
separate from the client gRPC port. The wire format is:

```
frame     length u32 | service u16 | method u16 | request_id u64 | payload
response  length u32 | request_id u64 | status u8 | payload
```

The `request_id` is what allows more than one call to be in flight on one
connection, which matters because the alternative is a connection per call and
a partition owner talks to its replicas constantly.

The client API stays gRPC. That is the boundary where compatibility is a
promise to strangers, and it is where the cost of a schema language is worth
paying.

## Consequences

**Two listeners per node,** one for clients and one for peers. They should be
separately configurable, because an operator will want the peer port on a
private network and the client port exposed, and because a peer port reachable
from the internet is a hole.

**The peer protocol has no compatibility guarantee across arbitrary versions.**

Corrected by [ADR 0005](0005-upgrades-follow-kubernetes-rollouts.md), which was
written a few hours later. The original text here said nodes of different
versions must not talk to each other at all, and pointed at the CLI's
refuse-on-skew check as the enforcement. That does not survive a Kubernetes
rolling update, where old and new nodes necessarily coexist and a node that
refuses to start stalls the rollout.

The accurate statement is that peer framing is compatible within a cluster
version window, one version wide. Nodes speak the cluster's active protocol
version rather than their own binary version, which is what makes a rolling
upgrade possible at all.

**Debugging is worse than gRPC would be.** No reflection, no `grpcurl`. The
answer is that peer traffic carries a request id, so tracing can correlate a
call across two nodes, and that is worth building early rather than after the
first confusing incident.

**No dependency on protobuf for internal traffic,** which keeps the WAL's
end-to-end checksum property intact and keeps one framing in the codebase
rather than two.

## Alternatives considered

**A gRPC peer service wrapping opaque bytes.** Uniform with the client API and
buys nothing, because the payload stays opaque either way. It costs an extra
encode and decode on the hottest internal path and adds a second framing to
reason about.

**Fully modelling peer messages in protobuf,** so they are inspectable. This is
the version that actually earns something, and it costs the WAL's end-to-end
checksum, forces every message change through the contract crate, and slows
down exactly the crates that iterate most. Worth revisiting if peer protocol
debugging becomes a recurring problem.

**Reusing the client gRPC port for peer traffic.** One less listener, and it
means a node cannot firewall peer traffic separately from client traffic.
