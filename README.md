<p align="center">
  <img src="docs/assets/orbita-logo.svg" alt="Orbita" width="360">
</p>

<p align="center">
  <em>One coordination backbone. From laptop to cloud scale.</em>
</p>

<p align="center">
  <a href="https://github.com/orbita-rocks/orbita/actions/workflows/ci.yml?query=branch%3Adevelop"><img src="https://github.com/orbita-rocks/orbita/actions/workflows/ci.yml/badge.svg?branch=develop" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License: Apache-2.0"></a>
  <img src="https://img.shields.io/badge/rust-1.85%2B-orange.svg" alt="Rust 1.85+">
</p>

Orbita is a strongly consistent store for locks, leases, catalogs, epochs, and
control-plane state. Start with one process. Grow into one shared, multitenant
system without replacing the direct KV model or running a cluster for every
team.

The amount of coordination data is usually small. Its consequences are not.
Orbita is built for the state that tells a larger system who owns what, what is
current, and what may happen next.

[Website](https://orbita.rocks) · [Quickstart](docs/QUICKSTART.md) ·
[Requirements](docs/REQUIREMENTS.md) · [Roadmap](ROADMAP.md)

## Status

Orbita is under active development and is not production-ready. The wire
protocol, storage format, and operating model may still change before the first
release.

Single-node and multi-node clusters work end to end today. The current system
has a Raft-backed control plane, replicated writes, lease-based replica reads,
owner failover, object-backed storage and hydration, keyspace credentials and
quotas, rolling-upgrade gates, and an Admin service on every node.

The notable holes are just as important:

- Partition split and merge return `Unimplemented`. Every keyspace currently
  remains one range partition, so horizontal partition growth is architecture,
  not demonstrated behavior yet.
- Native TLS, peer mTLS, prefix authorization, and audit logging are planned
  for v0.4. Do not expose the peer port to an untrusted network.
- Watch streams, multi-key transactions, backup and restore, and the offline
  format reader are not implemented.
- The project has not published production-scale latency, throughput, or
  correctness results. The simulator is the development method today; broader
  evidence is the next phase of the roadmap.

[The quickstart](docs/QUICKSTART.md) keeps the detailed list of what works and
what does not.

## Start with one process

From a checkout:

```bash
cargo install --path crates/orbita-cli
orbita dev
```

That starts a complete development cluster on `127.0.0.1:7100`: one process
running a one-voter control plane and a worker, with a `default` keyspace and
state under `.orbita/dev`. It deliberately has no object store or replication,
so it is for learning and local development rather than durable deployment.

Use one-shot commands from another terminal:

```bash
orbita set default greeting hello
orbita get default greeting
orbita list default
orbita cluster describe
```

Or keep one connection open in the interactive shell:

```console
$ orbita repl
orbita interactive session against http://127.0.0.1:7100. Type :help for session commands, :quit to leave.
orbita> :use default
keyspace set to default
orbita:default> set greeting "hello world"
ok, version 1
orbita:default> get greeting
hello world
version 1
orbita:default> list
greeting
orbita:default> :quit
```

The prompt shows the current keyspace. Use `:use <keyspace>` to select one,
`:format json` to change output, `:help` for session commands, and `:quit` or
Ctrl-D to leave. Ctrl-C cancels the current input line.

The REPL uses the same parser and renderer as one-shot commands. `get default
greeting` at the prompt and `orbita get default greeting` in a shell do the
same thing. It provides line editing and history for the current session;
persistent history and command completion are not implemented yet.

For scripts, keep using one-shot commands and `--output json`. `get` prints the
value first so it pipes, exits 2 when a key is absent, and exits 3 when a
conditional write does not apply.

## A focused coordination surface

Orbita stays small on purpose:

- `GET`, `SET`, `DELETE`, and ordered prefix `LIST`, with bounded pages and
  opaque cursors.
- Compare-and-swap by version and `IF NOT PRESENT`, which are the primitives
  behind locks, leader election, fencing, and atomic catalog pointers.
- Absolute TTL deadlines. Expired keys disappear from reads at the deadline,
  independent of replication delay or failover.
- Keyspaces as tenant boundaries, with scoped credentials, read and write
  permissions, storage quotas, rate limits, default TTLs, and value limits.
- Linearizable reads from replicas while their owner-issued lease and per-key
  invalidation state remain valid. A replica forwards when it cannot prove a
  local answer is current.

The current hard value limit is 256 KiB. A `LIST` page is individually
consistent, but a scan across pages is not a point-in-time snapshot.

## Why the system can grow

Orbita separates the fast coordination path from bulk durability:

1. The partition owner serializes a write and replicates its WAL entry. A fully
   placed partition acknowledges after the owner and one of two replicas have
   made the entry durable.
2. Workers flush immutable partition segments and atomically publish manifests
   to S3-compatible object storage outside the normal acknowledgement path.
3. A new or replaced worker hydrates its index from the bucket, then catches up
   from the WAL instead of copying the full dataset from a busy peer.
4. Range partitions are designed to distribute indexes and hot data across
   workers while the small Raft voter group carries metadata rather than every
   read and write.

The WAL keeps writes fast. Object storage makes workers replaceable. Range
partitions are the path to horizontal capacity. The first two are implemented;
automatic split execution is the remaining step that makes the third true in a
running cluster.

The storage engine uses Orbita's documented
[`partition-v1`](docs/format/partition-v1.md) format over a pluggable object
store. S3-compatible storage covers AWS S3, MinIO, and R2. A filesystem backend
keeps local development self-contained.

## Correctness you can inspect

Orbita is built so a failure can become evidence instead of a story about what
probably happened. Network, disk, clock, and object-store access sit behind
deterministic runtime seams. A failing simulation emits its seed, and that seed
replays the same schedule.

```bash
moon run orbita-sim:sim
ORBITA_SIM_SEED=<seed> moon run orbita-sim:sim
```

The simulator injects dropped, duplicated, delayed, and reordered messages;
torn writes and lying `fsync`; crashes and restarts; and object-store failures.
It checks linearizability and convergence across failover, leases, WAL
replication, flush, hydration, and retention scenarios.

The reasoning is public too:

- [Requirements](docs/REQUIREMENTS.md) fix the scope and guarantees.
- [Architecture decisions](docs/adr/README.md) record the tradeoffs and are
  immutable once accepted.
- [Work briefs](docs/plan/README.md) divide ownership across the workspace.
- [`partition-v1`](docs/format/partition-v1.md) specifies the bytes at rest.

This is a method and a commitment, not a completed proof. Continuous simulation
results, real-cluster measurements, and a public correctness report are v0.2
work.

## Run a cluster

The Compose stack runs three Raft voters, three workers, and MinIO:

```bash
docker compose up --build -d
docker compose --profile smoke run --rm smoke
```

The Helm chart runs the same shape on Kubernetes:

```bash
helm install orbita deploy/helm/orbita --namespace orbita --create-namespace
kubectl --namespace orbita port-forward svc/orbita 7100:7100
orbita cluster ready
```

Every node uses two listeners. Port 7100 serves the public client and Admin
gRPC APIs. Port 7101 carries unauthenticated private peer framing for Raft, WAL
replication, and forwarding. Keep 7101 on a private network.

Set `ORBITA_REQUIRE_AUTH=true` and `ORBITA_ROOT_CREDENTIAL` to enforce
credentials at the client boundary. Authentication is off by default, and the
current listeners are plaintext h2c.

[The quickstart](docs/QUICKSTART.md) covers laptop, Compose, and Kubernetes
paths. [Testing on EKS](docs/TESTING-ON-EKS.md) adds a disposable real-AWS path
with S3 and IRSA.

## Operations

One `orbita` executable runs every node role and every client command.

- `orbita cluster describe` shows node health, ownership, replica progress,
  storage, index memory, and quota configuration. Add `--output json` for a
  stable scripting interface.
- `orbita cluster ready` checks registration, recovery, partition catch-up,
  Raft progress, version compatibility, and auth-policy agreement.
- `orbita cluster finalize-upgrade` closes the rollback window after every live
  node can speak the next cluster version.
- `ORBITA_OTLP_ENDPOINT` enables OTLP/gRPC export for traces and metrics.
  Structured logs remain on stderr.

[Upgrades](docs/UPGRADES.md) documents the n-1 compatibility window, readiness,
draining, finalization, and rollback boundary.

## Documentation

| Document | What it covers |
|---|---|
| [Quickstart](docs/QUICKSTART.md) | Three ways to run Orbita, current gaps, configuration, and incident commands |
| [Requirements](docs/REQUIREMENTS.md) | Product scope, guarantees, scale envelope, and acceptance criteria |
| [Roadmap](ROADMAP.md) | What ships in each pre-1.0 release |
| [Architecture decisions](docs/adr/README.md) | Decisions, corrections, and rejected alternatives |
| [Storage format](docs/format/partition-v1.md) | The open partition format and its invariants |
| [Build](docs/BUILD.md) | Toolchain, Moon tasks, tests, and simulation |
| [Upgrades](docs/UPGRADES.md) | Rolling upgrades and the cluster-version contract |
| [Releasing](docs/RELEASING.md) | Versioning, artifacts, and the tag-driven release pipeline |

## Contributing

Start with [CONTRIBUTING.md](CONTRIBUTING.md). The full local gate mirrors CI:

```bash
moon run :fmt :lint :test --query "language=rust"
```

Behavior changes that touch replication, ownership, failover, splits, or reads
should include a simulation seed or scenario that fails without the change.
Participation is governed by the [Code of Conduct](CODE_OF_CONDUCT.md).

Report security issues through [SECURITY.md](SECURITY.md), not a public issue.

## License

Orbita is licensed under the [Apache License 2.0](LICENSE).
