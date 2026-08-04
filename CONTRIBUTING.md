# Contributing to Orbita

Thanks for wanting to help. Orbita is a coordination substrate, meaning people
put things in it that their platform cannot lose, so the bar for a change is
higher than the bar for most projects. This document is about how to clear it
without guessing.

Participation is governed by our [Code of Conduct](CODE_OF_CONDUCT.md).

## Before you write code

Read [docs/REQUIREMENTS.md](docs/REQUIREMENTS.md) first. It fixes the scope and
the guarantees, and most disagreements about a change turn out to be
disagreements about those. Then read [docs/adr/](docs/adr/README.md), which
records the decisions that were argued about and what we gave up to make them.
Where an ADR and anything else disagree, the ADR is newer and wins.

For anything beyond a small fix, open an issue before you build it. That is not
process for its own sake. Several people and agents work from
[docs/plan/](docs/plan/README.md) at the same time, and the failure mode that
costs everyone a day is two of them independently improving the same shared
trait.

The contract crates, meaning `orbita-core`, `orbita-runtime`,
`orbita-objectstore`, and `orbita-proto`, define the vocabulary everything else
compiles against. If a change wants to modify one of them, raise it rather than
doing it. Inside any other crate, structure things however you like.

## Setting up

You need a Rust toolchain, which `rust-toolchain.toml` pins so your laptop and
CI cannot drift apart, `protoc` for the protocol definitions, and moon, which
is what runs the build and the test suites.

```bash
brew install protobuf                  # or: apt-get install -y protobuf-compiler
curl -fsSL https://moonrepo.dev/install/proto.sh | bash
proto install moon
moon run :build
```

Do not run `proto use` here. proto reads `rust-toolchain.toml` as a version
file, so it takes the Rust install away from rustup and hands it to proto,
which on a clean machine produces a toolchain with no cargo in it.
[docs/BUILD.md](docs/BUILD.md) has the rest.

## What has to pass

CI runs these, so run them before you push:

```bash
moon run :fmt :lint :test --query "language=rust"
```

Or just the crates your change touches, which is the reason to prefer moon over
cargo locally:

```bash
moon run :fmt :lint :test --query "language=rust" --affected
```

The `-D warnings` on clippy is not decoration, and it lives in the task
definition rather than in CI's environment, so the clippy you run is the clippy
that gates the merge. That used to be a `RUSTFLAGS` that only CI set, which
meant a clean laptop run and a red pull request.

The end-to-end suite drives a running node from Python over a real socket,
using stubs generated from `/proto` by stock `protoc`. It exists to catch the
one thing the Rust tests structurally cannot: a protocol that only works when
both ends were generated together. Run it when you touch the protocol or the
server surface. See [tests/e2e/README.md](tests/e2e/README.md).

```bash
moon run e2e:test
```

That builds the binary, creates the virtualenv, installs the dependencies into
it, and runs pytest, because the task graph says those are what it needs.

## Simulation is how we argue about correctness

The whole system runs single-threaded under a seeded, fault-injecting
simulation. That is the methodology, not a testing phase at the end, and it is
the basis of the claim the project is built on.

```bash
moon run orbita-sim:sim
```

A failing seed prints the command that replays it exactly. Pin a single run
with `ORBITA_SIM_SEED`, and widen a batch with `ORBITA_SIM_SEEDS` when you want
to explore more of the state space than a pull request run does.

If you are changing behavior that a fault could break, meaning anything touching
replication, ownership, failover, splits, or the read path, the change should
come with a scenario or a seed that fails without it. A bug that simulation
could have caught and did not is worth a scenario even if the fix is one line.

## Pull requests

Keep a pull request to one idea. Describe what it changes and why it matters,
and skip the walkthrough of the diff, since the diff is right there. If it
changes a decision, write an ADR rather than editing an accepted one; records
are immutable once accepted, and a new one that supersedes the old is how the
history of a wrong turn stays readable.

Commit messages follow the same shape: a short subject line, then prose that
explains the reasoning and the tradeoff.

## Licensing

Orbita is Apache-2.0. There is no CLA. By opening a pull request you agree that
your contribution is licensed under the same terms, per section 5 of the
[license](LICENSE). If your employer owns your work, make sure you have the
clearance to contribute it before you do.

New contributors are welcome to add themselves to
[CONTRIBUTORS.md](CONTRIBUTORS.md) in the same pull request as their first
change.

## Security

Do not open a public issue for a vulnerability. [SECURITY.md](SECURITY.md) has
the process.
