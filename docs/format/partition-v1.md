# Orbita partition format, version 1

Status: Draft. The bytes may still change until v0.1.0 ships; earlier
pre-releases do not close the window.

This specifies everything Orbita writes to object storage for one partition. It
is written to be implementable by someone who has never read the Orbita source,
because that is the only way to know whether the format is actually open.

Read [README.md](README.md) first for the conventions, which are not repeated
here.

## Key ranges

A partition owns a half-open range of keys, `[start, end)`. A key is in the
range when it is greater than or equal to `start` and strictly less than `end`.

An empty `start` means unbounded below, and it is the only way to express that,
since the empty key sorts before everything. An absent `end`, written as JSON
`null`, means unbounded above. There is no other way to express unbounded, and
an `end` of empty is invalid rather than meaning unbounded.

Keys compare by unsigned byte-wise ordering. A shorter key that is a prefix of
a longer one sorts before it.

## What a partition looks like in a bucket

```
<root>/keyspaces/<keyspace_id>/partitions/<partition_id>/
    manifest.json
    segments/<epoch>-<sequence>.oseg
    values/<epoch>-<sequence>.oval
```

`keyspace_id`, `partition_id`, `epoch`, and `sequence` are unsigned 64-bit
integers written as 16 lowercase hexadecimal digits, zero padded. Padding is
what makes a listing sort in the same order as the numbers, which matters
because a writer recovering its place walks a listing.

### A segment object may be named by more than one partition

Almost everything a partition names sits under that partition's own directory,
and the manifest holds names relative to it. There is one exception, and a
reader that has not implemented it will fail on any partition that came from a
split.

When a partition splits, each child references the parent's segment objects
where they already sit rather than copying them. A partition splits because it
is large, so copying would rewrite the whole thing at the moment the system can
least afford it, and would double the stored bytes until compaction caught up.
The objects are immutable and already sorted and versioned, so a child needs a
new index over them rather than new bytes. See
[ADR 0009](../adr/0009-a-split-shares-the-parents-segments.md).

A child records this by putting the parent's partition id on the segment entry,
in `source_partition_id`, which the manifest section below defines. The object
is not moved, copied, or renamed. It stays at the key it has always had, under
the parent's directory, and the parent's manifest may still name it too.

Sharing stays inside one keyspace. A shared object's key differs from one under
the referencing partition's own directory only in the `<partition_id>`
component of the path, so a reader resolves it by substituting that component
and nothing else.

Sharing is temporary. When a child compacts, it merges the shared segments into
a segment under its own directory and drops the references, and the object
becomes collectable once no partition names it. The compaction and deletion
section below says what that costs and what it forbids.

A child's first manifest is published by whoever performs the split, before the
child has an owner of its own, and is stamped with the epoch the child was
allocated rather than the publisher's. Everything after that is the ordinary
commit protocol.

### Object names are never reused

A name identifies one object for the life of the partition. Writing different
bytes to a name that was used before is the one unrecoverable failure this
format has, because a manifest that references it may already have been read.

A name is only unique within one partition's directory, which is the most a
writer can enforce, so an object is fully identified by the partition whose
directory it lives under together with its relative name. A manifest may not
list that pair twice. Two entries carrying the same relative name and different
source partitions name two different objects and are both legal.

A writer lists and names objects only under its own directory, and it never
writes a segment or a value object into another partition's. So a shared
reference cannot collide with anything the referencing partition writes, and a
writer recovering its next sequence still only has to walk its own listing.

Two rules make that hold.

Names carry the ownership epoch as well as a sequence, so a deposed owner and
its replacement cannot collide. Both may be writing for a moment during a
failover, and two writers producing different objects that the manifest
arbitrates between is recoverable, where two writers producing the same object
name is not.

A writer establishes its next sequence before writing anything. On taking
ownership, or on restarting while still holding it, it lists `segments/` and
`values/` and takes the next sequence above the highest it finds bearing its
own epoch, or zero if there is none. It may not assume it knows its own
sequence from memory, because a crash between writing an object and recording
that fact leaves the object on the store and the memory gone.

Nothing outside the manifest is authoritative. An object that exists and is not
named by the manifest is not part of the partition, whether it is a leftover
from an interrupted commit or from a compaction whose cleanup has not run.

That says which partition an object belongs to. It does not say the object is
garbage, and reading it that way is now a way to delete live data. An object
under one partition's directory may be named by another partition's manifest, so
what makes it collectable is that no manifest in the keyspace names it. The
compaction and deletion section below states that rule, and it is the one a
sweep has to implement.

## The manifest

`manifest.json` is the partition's only mutable object and its atomic pointer.
It is JSON because it is read once per commit rather than once per key, because
it is the first thing a reader has to parse, and because needing a binary
parser to find the data would be a poor start for a format meant to be read by
other tools.

Byte strings appear as standard base64 with padding, since JSON cannot hold
arbitrary bytes.

**Numbers in the manifest are unsigned 64-bit integers and must be handled as
such.** An implementation whose JSON parser silently narrows to a double loses
precision above 2^53, which includes JavaScript's default number type. Values
are not expected to reach that in practice, and an implementation must not
depend on that expectation.

```json
{
  "format_version": 1,
  "keyspace_id": 1,
  "partition_id": 7,
  "epoch": 6,
  "committed_lamport": 4000,
  "range": { "start": "", "end": "bQ==" },
  "segments": [
    {
      "name": "segments/0000000000000005-0000000000000011.oseg",
      "bytes": 1048576,
      "record_count": 1200,
      "min_key": "YQ==",
      "max_key": "bA==",
      "min_lamport": 1,
      "max_lamport": 2500
    },
    {
      "name": "segments/0000000000000006-0000000000000000.oseg",
      "bytes": 262144,
      "record_count": 300,
      "min_key": "Yw==",
      "max_key": "aw==",
      "min_lamport": 2501,
      "max_lamport": 4000
    }
  ]
}
```

### Fields

| Field | Meaning |
|---|---|
| `format_version` | The version of this specification the manifest and its segments conform to. A reader that does not implement it must reject the partition. |
| `keyspace_id`, `partition_id` | Identifiers, matching the object's path. |
| `epoch` | The ownership epoch of the writer that produced this manifest. |
| `committed_lamport` | The flush horizon. Defined below. |
| `range` | The partition's half-open key range. |
| `segments` | The live segments. A segment not listed here is not part of the partition. |

Each segment entry:

| Field | Meaning |
|---|---|
| `name` | The object's name, relative to the directory it lives under, which is `source_partition_id`'s when that is present and this partition's otherwise. |
| `source_partition_id` | Optional. The partition whose directory holds the object. Absent means this partition's own. Defined below. |
| `bytes` | The object's exact size. A reader without suffix range requests uses this to compute the footer's absolute offset. Always the whole object's size, including for a shared entry. |
| `record_count` | How many records the object holds. Always the whole object's count, including for a shared entry, because a reader checks it against the object's own footer before filtering anything. |
| `min_key`, `max_key` | The smallest and largest keys this entry contributes to this partition, both inclusive. Not the range the segment was written for. For a shared entry these are clamped to this partition's range, so they describe the part of the object this partition serves rather than the whole object. |
| `min_lamport`, `max_lamport` | The smallest and largest `lamport` of any record in the object, both inclusive. These are not clamped for a shared entry, so an entry's Lamport span may reach outside what this partition serves. |

Every other rule about an entry holds whether or not it is shared. The name must
still be one this format produces, `min_key` and `max_key` must still sit inside
the partition's range, and `max_lamport` must still be at or below
`committed_lamport`. A reader must reject a manifest that breaks any of them.

### source_partition_id names a shared segment

`source_partition_id` is an unsigned 64-bit integer and it is optional. A writer
omits it entirely for a segment the partition wrote itself, rather than writing
`null` or the partition's own id, and a reader must treat its absence as the
whole meaning it has: the object is under this partition's directory and is
resolved exactly as it was before this field existed.

When it is present, the object is under that partition's directory instead. Both
partitions are in the same keyspace, so a reader builds the key by taking its own
partition directory and replacing the `<partition_id>` component.

Two values are illegal and a reader must reject the manifest rather than
interpret them. A `source_partition_id` equal to the manifest's own
`partition_id` is refused, because a segment under a partition's own directory
is already named relatively and a source pointing back at that partition would
be a second, ambiguous way to say the same thing. And the same
`source_partition_id` and `name` pair must not appear twice, since that names one
object twice.

References do not chain. A `source_partition_id` names the partition whose
directory actually holds the bytes, not an intermediate partition that also
referenced it. When a child of a split is itself split, its children carry the
original owner's id, so a reader resolves an object in one step and never has to
follow a manifest it has not been asked to read.

Here is a child of the partition above, holding the lower half of its range and
serving it out of the parent's two segments without a byte having been copied.

```json
{
  "format_version": 1,
  "keyspace_id": 1,
  "partition_id": 20,
  "epoch": 9,
  "committed_lamport": 4000,
  "range": { "start": "", "end": "Zg==" },
  "segments": [
    {
      "name": "segments/0000000000000005-0000000000000011.oseg",
      "source_partition_id": 7,
      "bytes": 1048576,
      "record_count": 1200,
      "min_key": "YQ==",
      "max_key": "ZQ==",
      "min_lamport": 1,
      "max_lamport": 2500
    },
    {
      "name": "segments/0000000000000006-0000000000000000.oseg",
      "source_partition_id": 7,
      "bytes": 262144,
      "record_count": 300,
      "min_key": "Yw==",
      "max_key": "ZA==",
      "min_lamport": 2501,
      "max_lamport": 4000
    }
  ]
}
```

The first entry resolves to
`<root>/keyspaces/0000000000000001/partitions/0000000000000007/segments/0000000000000005-0000000000000011.oseg`,
which is the same object the parent's own manifest names. Under partition 20's
own directory there is no `segments/` at all until it flushes or compacts, and a
reader that resolved the name there would get a missing object rather than a
wrong answer.

The asymmetry in that entry is deliberate and a writer has to get it right.
`bytes` and `record_count` describe the whole object, so the footer sits where
the entry says it does and a reader's check of the entry against the footer
still means something. The key bounds describe the child, so pruning and the
manifest's own range validation see only what the child serves. The Lamport
bounds describe the whole object, which is the conservative direction: a span
wider than the truth only makes a reader read the actual Lamports out of two
candidate records more often, and reading them is what decides the winner
anyway.

`committed_lamport` is inherited from the parent, because a child continues the
parent's Lamport sequence and allocates above it. No key's version moves.

### An old reader cannot read a manifest with shared segments

This field arrived after version 1 was first written and the version number did
not move, so it is worth being exact about who can read what.

A manifest that names no shared segment does not carry the field at all, so it
is byte for byte what a writer would have produced before the field existed.
Any partition whose manifest names no shared segment is therefore readable by
any implementation of version 1, old or new, and every manifest written before
this change is readable now. That covers a split's parent as well, since a
parent's own manifest never carries the field, and it covers a child again once
it has compacted its way out of sharing.

A manifest that does name a shared segment is a different matter. This format
has no extension mechanism and unknown fields are refused rather than skipped,
which is the rule that stops a reader silently returning something other than
what was stored. So an implementation of version 1 that predates this field will
reject a child's manifest outright. That is the correct outcome and it is also a
real limitation: such a reader cannot read a partition produced by a split, and
it will say so rather than guess.

Version 1 is a draft, and this is the window a draft exists for. Amending it
here is the cheaper answer than a version 2, because a new version number would
make every manifest unreadable to older implementations rather than only the
ones that use the new field. Once the first release ships, that door closes and
a change of this shape becomes a new version number.

### committed_lamport is the flush horizon

Every write-ahead log entry at or below `committed_lamport` is reflected in the
listed segments. Nothing above it is.

That has two consequences worth stating, because getting either backwards
produces silent data loss.

`committed_lamport` is greater than or equal to every segment's `max_lamport`.
It may be strictly greater, which means entries in that gap were flushed and
left no record, as happens when every write in the gap was superseded or
expired before the flush.

It is not the point the log had been acknowledged to. A cluster acknowledges
writes to clients well ahead of flushing them, which is the difference between
the two durability levels a client can ask about. A reader of the bucket alone
sees the flushed state and nothing later, and that is exactly what this field
names.

A node recovering a partition replays log entries above `committed_lamport` and
skips everything at or below it.

### epoch, and what a writer may stamp

A writer stamps its own ownership epoch and no other value. It never copies the
epoch it read, and never raises its own to match.

A manifest is never replaced by one carrying a lower epoch.

A split's first manifest for a child is the one case where the writer is not the
partition's owner, because the child has no owner yet. It stamps the epoch the
child was allocated, which is the epoch the child's first real owner will hold,
and the rule above then governs everything after it. It is still one epoch, held
by one partition, and it is still never the publisher's own.

## Segments

A segment is immutable. Once written it is never modified, only referenced and
eventually deleted.

**Segments may overlap in key range, and normally do.** A flush turns the
current in-memory table into one segment, and recent writes are scattered
across the keyspace rather than confined to a contiguous slice, so a fresh
segment overlaps the ones already there by construction. Requiring disjoint
ranges would make every flush rewrite the whole partition.

Overlap is not a problem to resolve at read time because Orbita keeps an exact
in-memory index, so a lookup knows which segment holds a key without consulting
the others. A reader that has not built such an index prunes candidates using
each entry's `min_key` and `max_key`, then resolves any remaining ambiguity by
`lamport` as the reading algorithm describes.

```
+--------------------+
| header    32 bytes |
+--------------------+
| data section       |   records, ascending by key
+--------------------+
| key index section  |   one entry per record
+--------------------+
| footer    64 bytes |
+--------------------+
```

The layout puts the footer last and gives it a fixed size so a reader can
retrieve it with one range request against the end of the object, using `bytes`
from the manifest if it cannot make a suffix request. From the footer it can
locate the index, and from the index it can locate any single record. That is
three requests to read one key out of a segment it has never seen, and one
request per key after that.

### Header

| Offset | Size | Field |
|---|---|---|
| 0 | 6 | magic, ASCII `ORBSEG` |
| 6 | 2 | `format_version`, 1 |
| 8 | 2 | `flags`, currently zero |
| 10 | 2 | reserved, zero |
| 12 | 8 | `keyspace_id` |
| 20 | 8 | `partition_id` |
| 28 | 4 | `epoch` truncated to its low 32 bits, for diagnostics only |

The identifiers repeat what the object's name already says. That is deliberate:
an object copied out of its path, which is what happens the moment somebody
investigates an incident, still describes itself.

### Records

Each record in the data section is:

| Size | Field |
|---|---|
| 4 | `crc32c` over everything that follows, through the end of `body` |
| 4 | `length`, bytes in `body` |
| `length` | `body` |

`body` is:

| Size | Field |
|---|---|
| 1 | `flags` |
| 8 | `lamport`, which is also the record's version |
| 8 | `commit_timestamp`, reserved; must be written as zero |
| 4 | `key_length` |
| `key_length` | `key` |
| 8 | `expires_at_millis`, present only if `flags` bit 1 is set |
| varies | value, described below |

`commit_timestamp` is reserved for the transaction work described in the
Transactions section of [REQUIREMENTS.md](../REQUIREMENTS.md), which needs its
bytes to exist before v0.1.0 freezes them. In partition-v1 a writer must write
it as zero and a reader must reject a non-zero value rather than interpret it,
the same rule the reserved flag bits follow. Readers must not assign it any
meaning; a future version will.

That rejection has a consequence the transaction work inherits: a v1 reader
refuses a non-zero value as a malformed record, not as a version it does not
implement. Whatever eventually writes this field for real must gate the change
on every reader understanding it, by cluster version or capability, rather
than relying on the format version to sort readers from writers.

`expires_at_millis` is milliseconds since the Unix epoch, UTC. A record is
expired when that value is less than or equal to the reader's current time on
the same scale.

`flags` bits, counting from the least significant:

| Bit | Meaning |
|---|---|
| 0 | tombstone; the key is deleted and there is no value |
| 1 | the record carries `expires_at_millis` |
| 2 | the value is stored in its own object |
| 3 | intent; reserved for the transaction work, must be zero |
| 4-7 | reserved, must be zero |

Bit 3 is named rather than generic for the same reason `commit_timestamp`
exists: the Transactions direction claims it before v0.1.0 freezes
this version's bytes. In partition-v1 it must be zero like every other reserved
bit, and a reader treats it exactly as it treats them.

Legal combinations:

| Combination | Legal | Note |
|---|---|---|
| tombstone with expiry | yes | This is the normal case. A tombstone is retained for a bounded time and then reclaimed, and the expiry is when that becomes allowed. |
| tombstone with external value | no | A tombstone has no value to store. |
| expiry with external value | yes | Nothing about a large value prevents it expiring. |
| any reserved bit set | no | |

A reader must reject a record with an illegal combination or a non-zero
reserved bit, rather than ignoring the bit it does not understand. Skipping
what a writer meant is how a reader silently returns something other than what
was stored.

A tombstone carries no value. Deletes are recorded rather than being an absence
because a conditional write has to tell a key that never existed apart from one
that was deleted at a known version.

An inline value is:

| Size | Field |
|---|---|
| 4 | `value_length` |
| `value_length` | `value` |

An external value, when bit 2 is set, is:

| Size | Field |
|---|---|
| 4 | `name_length` |
| `name_length` | object name, relative to the directory of the segment that holds this record |
| 8 | `value_length`, the size of that object |
| 4 | `crc32c` of the object's contents |

The name is relative rather than absolute for the same reason a segment's is,
and it is resolved the same way. For a record in a segment the partition wrote
itself that is the partition's own directory. For a record in a shared segment
it is the source partition's, because the value object was written beside the
segment by whoever wrote both.

Value objects hold the value bytes and nothing else. No header, no framing. A
reader that wants one can fetch it and use it directly, and the integrity data
lives in the referencing record instead, so the object stays exactly what a
caller stored.

Keys ascend through the data section and no key appears twice in one segment.
Producing a segment always means writing out a sorted map or merging sorted
runs, so this costs nothing to guarantee and it lets a reader stop looking once
it has found a key.

The lengths in a record are redundant with each other: `length` must equal the
size of the fields that follow it. A reader must reject a record whose `length`
disagrees with the fields it contains, rather than trusting either one.

### Key index section

One entry per record, in the same order:

| Size | Field |
|---|---|
| 4 | `key_length` |
| `key_length` | `key` |
| 8 | `offset` of the record from the start of the object |
| 4 | `record_length`, total bytes of the record including its 8-byte checksum and length prefix |

A reader must reject an index whose `record_length` disagrees with the
`length` in the record it points at.

The index exists so that rebuilding the in-memory index does not require
reading values. A node recovering a partition, or an external tool listing a
keyspace, reads the header, the footer, and the index, and never touches the
data section at all.

### Footer

Fixed at 64 bytes so a reader can fetch it without knowing the object's layout.

| Offset from end | Size | Field |
|---|---|---|
| -64 | 8 | `index_offset` |
| -56 | 8 | `index_length` |
| -48 | 8 | `record_count` |
| -40 | 8 | `min_lamport` |
| -32 | 8 | `max_lamport` |
| -24 | 4 | `crc32c` of the data section |
| -20 | 4 | `crc32c` of the key index section |
| -16 | 4 | reserved, zero |
| -12 | 4 | `crc32c` of the preceding 52 bytes of this footer |
| -8 | 2 | `format_version`, 1 |
| -6 | 6 | magic, ASCII `ORBEND` |

The version appears in both the header and the footer so that a reader which
has only fetched the tail can reject a format it does not understand before
interpreting any of the offsets in it.

## Committing

A commit publishes segments and value objects that are already written. It is
the manifest swap alone that makes them part of the partition.

1. Write any value objects.
2. Write the segment objects.
3. Read `manifest.json` and keep its entity tag.
4. **If that manifest carries an epoch greater than this writer's own, stop.**
   This writer has been deposed and the objects it just wrote are orphans for
   the sweep to collect. It must not proceed to step 5, and it must not raise
   its own epoch to match.
5. Build the new manifest, stamping this writer's own epoch. Never the epoch
   that was read, and never a higher one.
6. Conditionally write `manifest.json`, requiring the entity tag from step 3 to
   be unchanged. A partition with no manifest yet requires that the object not
   exist.
7. If the condition fails, another writer committed between steps 3 and 6. Go
   back to step 3. The check at step 4 is what decides whether the retry is
   legitimate or whether this writer is finished.

Step 4 is the one that matters, and it is easy to get wrong in a way that
passes review. Consider a deposed owner at epoch 6 that begins a commit after
its replacement at epoch 7 has already committed. It reads a current entity tag
and a manifest at epoch 7. Without step 4 it would build a manifest, and a rule
that only says "never write a lower epoch" would be satisfied if it copied or
raised to 7. Its conditional write would then succeed, because its entity tag
is current, and it would replace the replacement's manifest with one listing
its own stale segments. Every write the new owner had committed would vanish,
with no rule violated and no error raised anywhere. The epoch check has to
happen before the write, not only on the retry path, and the writer's own epoch
has to be the only value it is willing to stamp.

Steps 1 and 2 are safe to repeat and safe to abandon. An object written by a
commit that never reached step 6 is unreferenced, and unreferenced objects are
not part of the partition.

Steps 1 and 2 cover the objects this commit produces. A manifest may also name
shared segments, which are already on the store under another partition's
directory and which this writer neither writes nor may write. It names them and
nothing else changes about the protocol.

The conditional write is the only ordering primitive this format needs, and it
is why `ObjectStore` requires compare-and-swap. A backend without it cannot
host this format safely, and should not pretend to.

## Reading a snapshot

An implementation that only reads, which is the case this format exists to
support, does the following.

1. Fetch `manifest.json`. Reject any `format_version` it does not implement.
2. For each segment entry, work out the object's key. When the entry carries a
   `source_partition_id`, the object is under that partition's directory in the
   same keyspace. When it does not, the object is under this partition's own
   directory. Every request for that segment, now and later, goes to the key
   this step produced.
3. For each segment, fetch the footer, verify its own checksum, and reject a
   `format_version` or magic it does not recognise. Fetch the key index and
   verify it against the footer's index checksum.
4. Discard index entries whose key falls outside the manifest's `range`. A
   shared segment physically holds keys on both sides of the boundary the split
   drew, and only the ones inside this partition's range are this partition's to
   serve. For a segment the partition wrote itself this discards nothing, since
   both its bounds are inside the range and so is everything between them.
5. Build a map from key to the record holding it. Segments may overlap, so more
   than one may hold a key; the record with the higher `lamport` wins. Two
   records for one key with the same `lamport` is a corrupt partition, not a
   tie to break.
6. Drop tombstones. Drop records whose `expires_at_millis` is at or before the
   current time. Both are absent keys, not present ones with special values.
7. To read a value, range-request the record at its offset and length, verify
   its checksum, and decode it. If the value is external, resolve its name under
   the same directory as the segment that holds the record, which for a shared
   entry means the source partition's rather than this one's. Fetch that object
   and check it against the length and checksum in the record.

A reader pruning candidates without building a full index uses each segment
entry's `min_key` and `max_key` to skip segments that cannot hold the key, then
applies step 5 to whatever remains. A shared entry's bounds are already clamped
to this partition's range, so such a reader gets step 4 without doing anything.

Step 7 is the one worth stating separately, because a value object's name is
relative like a segment's and there is nothing in the name to say where it
lives. A record inside a shared segment was written by the source partition and
names a value object beside it, under the source partition's directory. A reader
that resolved it under its own would request an object that is not there. The
rule is that a value is resolved wherever the segment that referenced it was
resolved.

The section checksums in the footer cover the whole data and index sections.
Verifying the data section means reading all of it, so a point read verifies
only the record it fetched, using that record's own checksum. The section
checksums are for a scrub or a full rebuild, where the bytes are being read
anyway.

A reader that follows this sees exactly the partition's state as of the
manifest's `committed_lamport`. It will not see writes that are durable in the
write-ahead log but not yet flushed, which is the difference between the two
durability levels a client can ask about.

## Compaction and deletion

Compaction merges segments and reclaims space. Correctness rules:

- Merging preserves, for each key, the record with the highest `lamport`.
- A tombstone may be dropped only when no retained segment holds an older
  record for its key, since dropping it early resurrects the value.
- An expired record may be dropped at any time.
- The result is published by the ordinary commit above.

### Compacting a shared segment

A compaction may take a shared segment as input, and doing so is how a child
stops sharing. Three extra rules apply, and each one is a way to lose or corrupt
data if it is skipped.

The output holds only keys inside this partition's range. A shared segment holds
keys on both sides of the split boundary, and writing the far side into a
segment under this partition's directory would produce a manifest naming keys
outside its own range, which a reader must reject.

Any external value the merged records reference is copied into this partition's
`values/` directory and the record is rewritten to name the copy. The output
segment is written under this partition's directory and carries no
`source_partition_id`, so from that moment its records resolve relative to this
partition, and a name still pointing at the source partition's value object
would resolve to nothing. This copy is the only one a split ever pays for, and
it is bounded by the values the child actually keeps rather than by the
partition's size. A writer that reads a value to copy it checks it against the
length and checksum in the referencing record first, so a corrupt value is
caught before it is duplicated.

The compaction drops the shared entries from this manifest and deletes nothing.
The objects are under another partition's directory and may still be named by
the partition that wrote them or by a sibling child. Reclaiming them is the
sweep's job, under the rule below.

### Dropping a tombstone erases a distinction

A tombstone exists so that a conditional write can tell a key that never
existed apart from one deleted at a known version. Once it is dropped, that
distinction is gone: a compare-and-swap against the delete's version stops
succeeding and starts failing, through no action of the client holding it.

Every design that reclaims tombstones has this horizon, and the honest thing is
to bound it rather than pretend it is not there. Orbita retains a tombstone for
a configured duration recorded as the record's own expiry, so the horizon is
visible in the data rather than implied by a policy elsewhere. A client whose
held version is older than that duration must expect it to be refused. What
bounds the duration is an operational question, and it is
[ADR 0002](../adr/0002-key-versions-are-partition-lamports.md) territory
rather than the format's.

### An object is live while any partition names it

Before shared segments existed, an object under a partition's directory was
garbage exactly when that partition's own manifest stopped naming it. That is no
longer sufficient grounds to delete anything, and a sweep that still works that
way will delete a segment a child is reading from. This is the most dangerous
consequence of sharing, because it destroys data through the path that is
supposed to be housekeeping, and it does it quietly.

An object under partition P's directory is referenced when any of the following
holds. It is a candidate for deletion only when none of them does.

- **P's own manifest names it** as a segment entry carrying no
  `source_partition_id`.
- **Another partition in the same keyspace names it**, as a segment entry whose
  `source_partition_id` is P and whose `name` is this object's.
- **A record inside a segment either of those rules protects** points at it as
  an external value.

The third rule is the one that is easy to miss, because a value object appears
in no manifest at all. It is reached only through a record inside a segment, so
establishing that a value is unreferenced means reading the live segments that
resolve to P's directory, including the ones another partition shares from it. A
sweeper that judged values from manifests alone would delete every large value
in the partition.

The union is over the keyspace, not over one directory, so a sweeper has to know
every live partition in the keyspace before it can delete anything under any of
them. That is a harder thing to be sure of than reading one manifest, and the
rule that makes it safe is that uncertainty means retention. A sweeper that
cannot enumerate the keyspace's live partitions, cannot load one of their
manifests, or is looking at a partition that has no manifest at all must delete
nothing. Retaining an object that turned out to be garbage costs storage until
the next pass. Deleting one that turned out to be live cannot be undone.

Two situations follow from that and are worth naming, because an implementation
that has not thought about them will get them wrong in the unsafe direction.

A partition that is being split must not sweep. Its children's references are
decided before their manifests exist, so for the length of the split there is a
window in which an object is spoken for and nothing on the store says so.

A partition retired by a split has no manifest, so there is no set of live
references to subtract against and nothing sweeps its directory. Its segments
stay alive on the children's references, which is the point. Its manifest object
and anything the children have since compacted past are left behind until some
keyspace-wide pass reclaims them, and no such pass is specified here. Leaking a
manifest object is cheap. Getting the liveness union wrong is not, and building
the reclamation before the union is trustworthy would be building the dangerous
half first.

### The deletion grace period

Objects that nothing names, by the rule above, may eventually be deleted, but
not immediately, and the clock that governs the wait starts in different places
for the two ways an object becomes unreferenced.

**An object dropped by a commit,** such as a segment that compaction replaced,
becomes unreferenced when the manifest that stopped naming it was written. The
clock starts there, not at the object's creation. A segment may be live for
months before a compaction drops it, and deleting it on an age threshold would
remove objects that are still referenced. For a shared object, more than one
manifest can be the one that drops it, and the clock starts at the last of them.
A reader is still draining on whichever partition let go of it most recently.

**An object that was never referenced,** left by a commit that failed or was
abandoned, can only be judged by its own age, since nothing records when it was
written other than the store's own metadata. The clock starts at creation.

The grace period must exceed both of the following:

- **The longest read.** A reader may be part way through a snapshot taken
  against an earlier manifest, and deleting an object it is about to request
  turns a successful read into a failure.
- **The longest commit.** Steps 1 and 2 of a commit write objects that nothing
  references until step 6. A large compaction can spend a long time in between.
  A sweeper that only considered read duration would delete objects an in-flight
  commit is about to publish, producing a manifest that references objects that
  no longer exist, which is worse than either failure it was trying to avoid.

Because an interrupted commit also leaves unreferenced objects, the sweep that
collects them is required rather than an optimisation. Without it a partition
accumulates objects nobody will ever read.

## Integrity

Every checksum is CRC32C.

Within record framing and the key index, a checksum precedes the bytes it
covers and covers the length that describes them, so a corrupted length cannot
send a reader past what was verified. The footer is the exception: its own
checksum covers the 52 bytes before it, and the two section checksums it holds
cover bytes elsewhere in the object. That is forced by the footer being at a
known offset from the end, which is what lets a reader find it at all.

CRC32C detects the failures that actually occur here, which are torn writes,
truncation, and bit rot. It is not a defence against someone who can modify the
bucket. Anyone with write access to the objects can produce a consistent
manifest describing whatever they like, and no checksum in a format defends
against that. Access control does.

A reader that finds a checksum mismatch must fail rather than skip. Unlike the
write-ahead log, where a torn tail is an expected outcome of a crash and is
truncated, a segment is written once and completely. Damage to one is real
damage, and continuing past it would silently return a partition that is
missing data.

## Field widths are format bounds, not operational limits

The lengths in this format are 32-bit, so a key or a value cannot exceed 4 GiB
whatever a cluster is configured to allow. Operational limits are far lower,
are set per keyspace, and are published over the API rather than living here.
See [ADR 0007](../adr/0007-large-values-are-their-own-objects.md). An
implementation validates against the field widths; it learns what a particular
cluster will accept by asking that cluster.
