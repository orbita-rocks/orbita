# AGENTS.md

Guidance for coding agents working in this repository.

## What this is

Orbita is a strongly consistent, multitenant, range-partitioned distributed KV
store written in Rust. It holds the locks, leases, epochs, catalogs, and
control-plane state a platform coordinates on. It is not production-ready, but
single node and multi node both work end to end: workers register, writes
replicate, and the Admin service is served by every node and forwarded to the
control plane's leader. Partition split and merge are the notable holes.

## Required reading order

1. `docs/REQUIREMENTS.md` — scope and guarantees
2. `docs/adr/` — accepted ADRs are immutable and **override everything else**,
   including this file. Decision changes get a new ADR, never an edit.
3. `docs/plan/` — per-crate work briefs, written so multiple people or agents
   can build in parallel

## Layout

- `crates/` — 11-crate Cargo workspace. Moon project ids equal crate dir names.
  - **Contract crates — do not modify without raising an issue first:**
    `orbita-core`, `orbita-runtime`, `orbita-objectstore`, `orbita-proto`.
    These are frozen vocabulary. All other crate internals are fair game.
  - `orbita-storage`, `orbita-wal`, `orbita-control`, `orbita-server`,
    `orbita-format`, `orbita-sim` (deterministic simulation),
    `orbita-cli` (the `orbita` binary)
- `proto/orbita/v1/` — wire contract (`kv.proto`, `admin.proto`)
- `tests/e2e/` — Python pytest suite driving the real binary over a socket
- `deploy/` — Helm chart + k8s manifests

## Toolchain

- Rust pinned in `rust-toolchain.toml` (rustup picks it up automatically)
- `protoc` must be on PATH (`brew install protobuf`)
- Build orchestrator is **moon**, installed via proto. Never run `proto use`
  (it breaks the Rust install). The moon version is pinned in both
  `.prototools` and `MOONREPO_CLI_VERSION` in `.github/workflows/ci.yml`;
  keep them in sync manually.

## Commands

The full gate (must pass before you're done; mirrors CI):

```
moon run :fmt :lint :test --query "language=rust"
```

Add `--affected` to limit to touched crates (compares against `develop`, the
default branch).

- Build: `moon run :build`
- One crate's tests: `moon run <crate>:test` (≡ `cargo test -p <crate> --all-features`)
- Lint is clippy with `-D warnings`; fmt is `cargo fmt --check` with default config
- E2E: `moon run e2e:test` (builds the binary, makes a venv, generates gRPC
  stubs from `/proto`, runs pytest). Stubs are regenerated every run — never
  commit them.
- Simulation: `moon run orbita-sim:sim` (seeded, release profile).
  Nightly long batch: `moon run orbita-sim:sim-long`.

Gotcha: if you add a dependency between workspace crates in `Cargo.toml`, you
must also add it to that crate's `moon.yml` `dependsOn`. Moon cannot follow
`workspace = true` deps; this only affects `--affected` accuracy.

## Testing conventions

- Test names state the behavior protected: `expiry_is_inclusive_of_the_deadline`,
  not `test_ttl_2`.
- Behavior changes touching replication, ownership, failover, splits, or the
  read path should ship with a simulation seed or scenario that fails without
  the change. A failing sim run prints a replay command; pin with
  `ORBITA_SIM_SEED`, widen with `ORBITA_SIM_SEEDS`.
- E2E tests get one node per test on a free port and temp dir. It is a test
  harness, not a client library. Run a single test with normal pytest
  selection from `tests/e2e/`.

## Code style

- No `unsafe`: every crate sets `#![forbid(unsafe_code)]`. Keep it that way.
- Public items carry doc comments that explain *why*, not what.
- Config files in this repo explain their reasoning in comments; preserve that
  culture when editing them.
- No custom rustfmt/clippy config — defaults plus `-D warnings`.

## Git conventions

- Default branch is `develop`.
- Commits: short subject plus prose body explaining the reasoning or tradeoff.
  Use the `conventional-commit-message` skill in `.agents/skills/`.
- Branch/PR naming: use the `pr-branch-naming` skill in `.agents/skills/`.
- PRs contain one idea each.

## Local dev

- `orbita dev` runs a whole single-node cluster on `127.0.0.1:7100` with a
  `default` keyspace.
- `docker-compose.yml` stands up the full cluster shape with MinIO as the
  S3-compatible object store.
- There is no `.env` file; server config comes from `ORBITA_*` env vars
  (see `docker-compose.yml` for examples).
