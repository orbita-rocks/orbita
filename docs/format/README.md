# The Orbita partition format

This directory specifies what Orbita writes to object storage. It is a
specification rather than a description of the implementation: the point of
owning the format is that something other than Orbita can read it, and that is
only true if the document is precise enough to implement against.

| Version | Status | Document |
|---|---|---|
| 1 | Draft | [partition-v1.md](partition-v1.md) |

Draft means the bytes may still change. It stops being a draft when the first
release ships, after which the rules below apply.

## Why this exists

Two reasons, both in
[ADR 0006](../adr/0006-partitions-are-an-index-over-immutable-objects.md).

Data written in a single-implementation format is reachable only through the
implementation that wrote it. A specified format means a snapshot in a bucket
can be read by a tool that knows nothing about Orbita, which matters for
analytics, for bulk loading, for disaster recovery, and for anyone deciding
whether to depend on this.

An engine that performs its own I/O also caps what deterministic simulation can
verify. Everything described here goes through interfaces the simulator
implements, so faults can be injected anywhere in it.

## Compatibility rules

Once a version ships, its bytes never change. A change to the layout is a new
version number.

A reader must reject a version it does not know rather than guessing. Formats
that tolerate unknown versions produce silent misreads, which are worse than a
refusal.

A writer emits one version at a time, chosen by the cluster's active version,
per [ADR 0005](../adr/0005-upgrades-follow-kubernetes-rollouts.md). A node reads
every version its compatibility window permits and writes only the active one.
That is what makes the rollback window in an upgrade real: nothing writes the
new format until an operator finalizes.

Unknown fields are not skippable and there is no extension mechanism. This is a
deliberate limit. Extensibility in a storage format tends to become a way to
ship a change without deciding whether it is a breaking one, and the version
number is a better place for that argument.

## Conventions used in the specification

- All integers are little endian and unsigned unless stated otherwise.
- Offsets and lengths are in bytes, measured from the start of the object.
- Keys sort by unsigned byte-wise comparison, which is what
  `orbita_core::KeyRange` uses and what partition boundaries mean.
- Checksums are CRC32C, the Castagnoli polynomial, chosen because it has
  hardware support on every architecture this runs on.
- A checksum covers the bytes that follow it, including any length prefix that
  describes them, so a corrupted length cannot direct a reader past the end of
  what was verified.
