<p align="center">
  <img src="docs/assets/orbita-logo.svg" alt="Orbita" width="360">
</p>

<p align="center">
  <em>Strongly consistent distributed KV with no storage ceiling.</em>
</p>

<p align="center">
  <a href="https://github.com/orbita-rocks/orbita/actions/workflows/ci.yml?query=branch%3Adevelop"><img src="https://github.com/orbita-rocks/orbita/actions/workflows/ci.yml/badge.svg?branch=develop" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License: Apache-2.0"></a>
  <img src="https://img.shields.io/badge/rust-1.85%2B-orange.svg" alt="Rust 1.85+">
</p>

Orbita is where you put the locks, leases, epochs, catalogs, and control-plane
state that a platform coordinates on. It is multitenant, so one cluster serves
every team that needs it rather than one cluster each, and it starts as a
single partition on your laptop and splits as it grows, past where etcd stops.

## Why this exists

Every platform needs a coordination substrate, and the usual answer has known
ceilings. etcd has a practical storage limit around 8GB, a single Raft group
that every operation flows through, and no real multitenancy, so each team that
needs coordination ends up running its own cluster. FoundationDB is more
capable, and it brings an operational model that is heavy if all you needed was
consistent KV.

Orbita sits between them. It range-partitions the keyspace and splits as data
grows, it is multitenant so one cluster serves many teams, and it stays a KV
store rather than growing into a database.

The part that matters for adoption is the evidence. The whole system runs under
deterministic simulation with injected faults, seeded so any failure reproduces
exactly, and we publish the results. If you are deciding what to bet a platform
on, that is the thing worth reading.

## Status

Orbita is being built in the open and is not ready for production. Nothing here
is stable yet, including the on-disk format and the wire protocol.

A single node serves reads, writes, deletes, and scans end to end today. A
multi-node cluster starts and reports healthy, but workers do not yet register
with the leader group, and the admin service is not implemented server-side.
[docs/QUICKSTART.md](docs/QUICKSTART.md) keeps the current list of what works
and what does not, and is the honest one to read before you spend an hour on
this.

## Try it

```bash
cargo install --path crates/orbita-cli
orbita dev
```

That is a whole cluster: one node that is its own leader group and its own
worker, on `127.0.0.1:7100`, with a `default` keyspace created at startup. From
another terminal:

```bash
orbita set default greeting hello
orbita get default greeting
```

`get` prints the value on the first line so it pipes, and exits 2 when the key
is absent and 3 when a conditional write did not apply, so a script can branch
without parsing anything.

There is a Docker Compose stack and a Helm chart for a real cluster shape.
[docs/QUICKSTART.md](docs/QUICKSTART.md) walks all three paths.

## What you get

- Linearizable reads, served from replicas rather than funneled through one
  node per range, so read capacity grows with the cluster.
- Conditional writes, meaning compare-and-swap on version and IF NOT PRESENT.
  These are what make locks, leader election, and catalog pointer swaps
  possible, so they are required rather than optional.
- Keyspaces as tenants, each independently partitioned, with its own
  credentials, quotas, and defaults.
- TTLs stored as absolute expiry timestamps, so replication and partition
  splits cannot skew them.
- A replicated WAL for write latency and object storage for bulk durability.
  Writes acknowledge at 2-of-3 and survive losing a worker; a client that needs
  to survive losing the cluster asks for the write to be flushed.
- One static binary that runs as leader or worker by configuration, an S3
  compatible object store behind a pluggable trait, and OpenTelemetry traces,
  logs, and metrics throughout.

Watch and subscribe is the known gap, and it is first on the roadmap.
Multi-key transactions are not planned; if you need them, FoundationDB is the
better tool and we would rather say so than pretend otherwise.

## Documentation

| Document | What it covers |
|---|---|
| [Quickstart](docs/QUICKSTART.md) | Three ways to get a cluster, the two ports, configuration, and what to run during an incident |
| [Requirements](docs/REQUIREMENTS.md) | Scope, guarantees, the scale envelope, and acceptance criteria for v1 |
| [Architecture decisions](docs/adr/README.md) | The decisions that were argued about, and what we gave up |
| [Upgrades](docs/UPGRADES.md) | The rolling upgrade procedure, and which parts the server does not support yet |
| [Work briefs](docs/plan/README.md) | How the build is split across crates, and what each one owns |

## Contributing

Start with [CONTRIBUTING.md](CONTRIBUTING.md). The short version is that
`cargo fmt`, `cargo clippy`, and `cargo test` all have to pass, and a change to
behavior wants a simulation seed that fails without it. Participation is
governed by our [Code of Conduct](CODE_OF_CONDUCT.md).

To report a security issue, follow [SECURITY.md](SECURITY.md) rather than
opening a public issue.

## License

Orbita is licensed under the [Apache License 2.0](LICENSE).
