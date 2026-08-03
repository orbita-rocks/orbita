# Building Orbita

The build runs through [moon](https://moonrepo.dev). You can still use cargo
directly and nothing will stop you, but moon is what CI runs, so it is the
thing that decides whether a change merges.

## Why there is a build tool at all

Orbita is ten crates, a Python end-to-end suite, and a deterministic simulator
that needs a different cargo profile than everything else. Before moon, the
list of commands that make up "did I break anything" lived in a GitHub Actions
file, which meant the only way to run what CI runs was to read the YAML and
retype it. Every time that list drifted, somebody found out on a pull request
instead of on their laptop.

moon fixes that by holding one definition of each task and letting both CI and
a laptop run it. It also knows which crates a change actually touches, so a
change to `orbita-wal` does not have to retest `orbita-cli`.

The other half of this is versions. The Rust version lives in
`.moon/toolchain.yml`, and moon writes it back out to `rust-toolchain.toml`, so
rustup users get the same compiler without knowing moon exists. moon's own
version is pinned in `.prototools`. Neither CI nor a developer picks a version
of anything, which is the point.

## Getting set up

```
proto use
```

That installs the pinned moon. If you do not have proto, one line gets it:

```
curl -fsSL https://moonrepo.dev/install/proto.sh | bash
```

moon installs the Rust toolchain itself on the first run. You still need
`protoc` on the machine, because `orbita-proto` compiles the wire definitions
at build time and that is a system dependency moon does not manage. On a Mac
that is `brew install protobuf`.

## The tasks

Every crate under `crates/` gets `fmt`, `lint`, `test`, and `build` without a
config file of its own. A new crate is a new directory, and that is all.

```
moon run :test                       every crate, and the end-to-end suite
moon run orbita-wal:test             one crate
moon run :lint --query "language=rust"   what the CI check job runs
moon run e2e:test                    the wire contract, from a Python client
moon run orbita-sim:sim              the simulation, in release
```

The one worth knowing about is `--affected`, which is the reason to prefer moon
over cargo locally:

```
moon run :test --affected
```

That runs the tests for the crates your working tree actually changes, plus
everything downstream of them. moon reads the dependency graph out of the
`Cargo.toml` files, so it is the same graph cargo builds from and there is no
second copy to keep current.

## Things that are deliberately not cached

`build` is not cached, because moon does not own `target/` and a cache hit over
a binary somebody deleted is a confusing way to fail. cargo is already
incremental, so there is nothing lost.

`e2e:test` is not cached either. It drives the shipped binary as a subprocess
rather than importing anything, so moon cannot see that a server change should
re-run it. Caching would hide the exact regressions the suite exists to catch.

`sim-long` is not cached because the whole point is to explore new seeds every
time. "Nothing changed" is not a reason to skip it.

## The end-to-end suite

`moon run e2e:test` builds the binary, creates a virtualenv under `tests/e2e`,
installs the test dependencies into it, and runs pytest. The virtualenv is
deliberate: running the suite should not install anything into whichever python
is first on your PATH.

## Denying warnings

`-D warnings` is an argument to the clippy task rather than a `RUSTFLAGS` set
in the CI environment. That way the clippy you run is the clippy that gates the
merge, and a warning does not first appear as a red pull request.
