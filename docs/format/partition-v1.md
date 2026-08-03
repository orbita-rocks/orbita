# Orbita partition format, version 1

Status: Draft. The bytes may still change until the first release.

This specifies everything Orbita writes to object storage for one partition. It
is written to be implementable by someone who has never read the Orbita source,
because that is the only way to know whether the format is actually open.

Read [README.md](README.md) first for the conventions, which are not repeated
here.

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
because recovery walks a listing.

Names contain an epoch as well as a sequence so that a deposed owner and its
replacement cannot pick the same name. Both may be writing for a moment during
a failover, and two writers producing different objects that the manifest
arbitrates between is recoverable, where two writers producing the same object
name is not.

Nothing outside the manifest is authoritative. An object that exists and is not
named by the manifest is not part of the partition, whether it is a leftover
from an interrupted commit or from a compaction whose cleanup has not run.

## The manifest

`manifest.json` is the partition's only mutable object and its atomic pointer.
It is JSON because it is read once per commit rather than once per key, because
it is the first thing a reader has to parse, and because needing a binary
parser to find the data would be a poor start for a format meant to be read by
other tools.

Byte strings appear as standard base64 with padding, since JSON cannot hold
arbitrary bytes. An absent `end` means the partition's range is unbounded
above.

```json
{
  "format_version": 1,
  "keyspace_id": 1,
  "partition_id": 7,
  "epoch": 6,
  "committed_lamport": 4821,
  "range": { "start": "", "end": "bQ==" },
  "segments": [
    {
      "name": "segments/0000000000000006-0000000000000012.oseg",
      "bytes": 1048576,
      "record_count": 1200,
      "min_key": "YQ==",
      "max_key": "eg==",
      "min_lamport": 1,
      "max_lamport": 4000
    }
  ]
}
```

`committed_lamport` is how far the write-ahead log had been acknowledged when
this manifest was written. A reader learns from it exactly which point in the
partition's history this snapshot represents. A node recovering from it learns
which log entries it still has to replay.

`epoch` is the ownership epoch of the writer. A manifest is never replaced by
one carrying a lower epoch, which stops a deposed owner from overwriting its
replacement's work even if it wins a race on the object store.

Segments are listed in ascending `min_key` order and their key ranges do not
overlap. A merge is what produces that property and compaction is what
maintains it, so a reader may binary search the list rather than examining
every entry.

## Segments

A segment is immutable. Once written it is never modified, only referenced and
eventually deleted.

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
retrieve it with one range request against the end of the object without
knowing anything else. From the footer it can locate the index, and from the
index it can locate any single record. That is three requests to read one key
out of a segment it has never seen, and one request per key after that.

### Header

| Offset | Size | Field |
|---|---|---|
| 0 | 6 | magic, ASCII `ORBSEG` |
| 6 | 2 | `format_version`, 1 |
| 8 | 2 | `flags`, currently zero |
| 10 | 2 | reserved, zero |
| 12 | 8 | `keyspace_id` |
| 20 | 8 | `partition_id` |
| 28 | 4 | `epoch` truncated to 32 bits, for diagnostics only |

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
| 4 | `key_length` |
| `key_length` | `key` |
| 8 | `expires_at_millis`, present only if `flags` bit 1 is set |
| varies | value, described below |

`flags` bits, counting from the least significant:

| Bit | Meaning |
|---|---|
| 0 | tombstone; the key is deleted and there is no value |
| 1 | the record carries `expires_at_millis` |
| 2 | the value is stored in its own object |
| 3-7 | reserved, must be zero |

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
| `name_length` | object name, relative to the partition directory |
| 8 | `value_length`, the size of that object |
| 4 | `crc32c` of the object's contents |

Value objects hold the value bytes and nothing else. No header, no framing. A
reader that wants one can fetch it and use it directly, and the integrity data
lives in the referencing record instead, so the object stays exactly what a
caller stored.

Keys ascend through the data section and no key appears twice in one segment.
Producing a segment always means writing out a sorted map or merging sorted
runs, so this costs nothing to guarantee and it lets a reader stop looking once
it has found a key.

### Key index section

One entry per record, in the same order:

| Size | Field |
|---|---|
| 4 | `key_length` |
| `key_length` | `key` |
| 8 | `offset` of the record from the start of the object |
| 4 | `record_length`, total bytes including the record's own header |

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
4. Build the new manifest, carrying an `epoch` at or above the one just read.
5. Conditionally write `manifest.json`, requiring the entity tag to be
   unchanged. A partition with no manifest yet requires that the object not
   exist.
6. If the condition fails, another writer committed first. Re-read, decide
   whether the work still applies, and retry. If the manifest now carries a
   higher epoch, this node has been deposed and must stop rather than retry.

Steps 1 and 2 are safe to repeat and safe to abandon. An object written by a
commit that never reached step 5 is unreferenced, and unreferenced objects are
not part of the partition.

The conditional write is the only ordering primitive this format needs, and it
is why `ObjectStore` requires compare-and-swap. A backend without it cannot
host this format safely, and should not pretend to.

## Reading a snapshot

An implementation that only reads, which is the case this format exists to
support, does the following.

1. Fetch `manifest.json`. Reject any `format_version` it does not implement.
2. For each segment, fetch the footer, verify its checksum, and fetch the key
   index. Verify the index checksum.
3. Build a map from key to the record holding it. Where more than one segment
   holds a key, the one with the higher `lamport` wins. Compaction normally
   leaves at most one, but a reader must not depend on that.
4. Drop tombstones. Drop records whose `expires_at_millis` is at or before the
   current time. Both are absent keys, not present ones with special values.
5. To read a value, range-request the record at its offset and length, verify
   its checksum, and decode it. If the value is external, fetch that object and
   check it against the length and checksum in the record.

A reader that follows this sees exactly the partition's state as of the
manifest's `committed_lamport`. It will not see anything written after that,
including writes that are durable in the write-ahead log but not yet flushed,
which is the difference between the two durability levels a client can ask
about.

## Compaction and deletion

Compaction merges segments and reclaims space. Correctness rules:

- Merging preserves, for each key, the record with the highest `lamport`.
- A tombstone may be dropped only when no retained segment holds an older
  record for its key, since dropping it early resurrects the value.
- An expired record may be dropped at any time.
- The result is published by the ordinary commit above.

Objects that the new manifest no longer names may be deleted, but not
immediately. A reader may be part way through a snapshot taken against the
previous manifest, and deleting an object it is about to request turns a
successful read into a failure. Implementations keep unreferenced objects for a
grace period that exceeds the longest read they intend to support.

Because an interrupted commit also leaves unreferenced objects, the sweep that
collects them is required rather than an optimisation. Without it a partition
accumulates objects nobody will ever read.

## Integrity

Every checksum is CRC32C and every one covers the length that describes the
bytes it protects, so a corrupted length cannot send a reader past what was
verified.

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
