# 06: Binary, CLI, and packaging (`orbita-cli`)

How someone gets from curiosity to a running cluster. This brief is low risk
and high leverage: for an open source project, the first ten minutes decide
whether there is an eleventh.

## Scope

- One `orbita` binary that runs as a leader group member or a worker depending
  on configuration. Not two binaries, not a wrapper script.
- `orbita dev`, which stands up a single-node cluster with sensible defaults
  and no configuration file, for a laptop.
- Configuration from a file with environment variable overrides. Every option
  needs a default that works.
- The admin CLI, wrapping the `Admin` gRPC service: keyspace CRUD, credentials,
  cluster and partition inspection, and manual split, merge, and transfer.
  Anything an operator can do must be scriptable, which follows from the CLI
  being a thin wrapper rather than a second implementation.
- Human-readable output by default and `--output json` for scripts.
- A Docker image built and published on every release.
- A Docker Compose quickstart: a leader group, three workers, and MinIO, in one
  command.
- Kubernetes manifests and a Helm chart. StatefulSets for workers, an operator
  is a later project.
- OpenTelemetry configuration: endpoint, sampling, and resource attributes.

## Out of scope

- The server itself, which is brief 04.
- An operator or a web dashboard. Both are post-v1.

## Decisions to make and write down

- **Bootstrap.** How does a fresh cluster elect its first leader group and
  create the first partition? This is the step every distributed system makes
  awkward, and the one an evaluator hits first. It deserves more design thought
  than its size suggests.
- **Config format.** TOML is the Rust default. YAML is what the Kubernetes
  audience expects. Pick one, support the other only if it is free.
- **Version skew.** What does a worker do when it finds a leader group running
  a different version? Refusing to start is safest, and it makes rolling
  upgrades a design question that has to be answered rather than discovered.

## Done when

- `docker compose up` produces a working cluster, and a documented sequence of
  CLI commands creates a keyspace, writes a key, and reads it back.
- The Helm chart deploys to a fresh cluster with no manual steps.
- `orbita --help` is good enough that the quickstart is a formality.
- Every admin API call has a CLI equivalent.
- The image is built and published by CI on a tag, for both amd64 and arm64.
