# Write-path integration

This change takes four optimizations from the September 14 write-path spike:

- Unconditional SET skips the old-value fetch under admission. DELETE and
  conditional writes retain their existing state/overlay checks.
- WAL batches are encoded once and shared across replica calls and retries.
- Quota serving-node counts are published with each routing snapshot.
- Flush uploads publish snapshots outside the storage state lock. A separate
  bounded task coalesces size-triggered requests so uploads do not park the
  ordered applier. Manifest publishers remain serialized; newer applied
  versions survive snapshot installation, and cancellation preserves the
  readable table. Compaction still holds the state lock.

The local-sync/replication overlap was removed after review reproduced a
same-epoch restart hazard: a replica could retain unsynced owner bytes, then
acknowledge different replacement bytes as a duplicate Lamport. Local fsync
again precedes sending that batch. Simply fencing on every open would not fix
it: existing peers ignore same-epoch fences, and delayed or unreachable peers
require a recovery protocol that prevents stale traffic from reintroducing
those tails. Safe overlap is deferred rather than changing that protocol here.
Quiesce still waits for the local flusher before truncating, and both normal
and catch-up acknowledgements must cover the original batch's last Lamport.

No early-invalidation receipt RPC or receipt bookkeeping is included. Client
completion still requires both durable quorum and the existing read-coherence
condition. The client API, peer wire format and disk format are unchanged.
[Issue #197](https://github.com/orbita-rocks/orbita/issues/197) tracks an append
exchange that carries both invalidation and durable acknowledgements over the
existing peer connection.

## Prior measurement

The September 14 experiment used three native nodes and a client sharing one
Mac, MinIO in Docker, disabled value cache/read-ahead, and release executables.
Three trials per variant rotated execution order. The final stage had 2,048
keys, with 1 KiB or 16 KiB values; larger scenarios crossed the 8 MiB flush
trigger. These are local comparisons, not cloud capacity estimates.

The measured receipt-disabled variant retained receiver-side bookkeeping and
local-sync/replication overlap. This PR now includes neither. The figures below
are historical experiment results, not estimates of the four-change PR's
speedup. The revised combination has not been benchmarked.
Each value below is a median across three successful trials.

| Overwrite workload | Baseline writes/s | Receipt RPCs disabled writes/s | Baseline p99 ms | Receipt RPCs disabled p99 ms |
|---|---:|---:|---:|---:|
| 1 KiB, concurrency 8 | 247 | 334 | 58.69 | 41.07 |
| 1 KiB, concurrency 32 | 714 | 1,277 | 68.77 | 40.03 |
| 16 KiB, concurrency 32 | 537 | 862 | 146.49 | 61.58 |
| 16 KiB, concurrency 64 | 975 | 1,566 | 153.77 | 81.16 |

Across all eight overwrite cases, that variant improved median throughput by
19–79%. Single-client 1 KiB insert throughput was 87 writes/s at baseline,
75 with all six experiments, and 95 with receipt RPCs disabled. The full
six-change spike did better on several large-value cases, so early receipts
remain worth investigating separately rather than a universal improvement.

All measured operations in the nine complete trials reported zero errors. One
additional baseline attempt failed with a quorum error and 205 operation
errors; its root cause remains unresolved and its successful retry is used in
the latency summary. The failed attempt was preserved separately. These runs
do not isolate an effect size for each remaining change.

Source revisions: baseline `bfa1bf81783d0dd8e07bd066c60457f19d15a971`, full spike
`d44db0b6e62c9f7378d7b86b1a47ca0ace003840`, benchmark clients
`40ab5c3e736d4468edddffb6cda01f08738fa3c4`. Raw results, binary hashes, the RPC
ablation patch and reproduction scripts remain in the benchmark workspace at
`orbita-benchmark/results/write-spike-20260914/`.
