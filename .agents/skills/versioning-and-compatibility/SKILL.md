---
name: versioning-and-compatibility
description: Decide what version bump a change needs in Orbita, and change anything that carries a version. Use when asked whether a change is breaking, whether it needs a minor or a patch, what version something should be, or when editing the workspace version, the chart appVersion, or a pinned image tag. Triggers on "does this need a minor bump", "is this breaking", "bump the version", "what version", "set the version to", "change the image tag", "why did the release refuse my patch bump".
---

# Versioning and compatibility in Orbita

[docs/UPGRADES.md](../../../docs/UPGRADES.md) is what a version means to a
running cluster. This skill is how to act on it.

## There is one version

It lives in `[workspace.package] version` in the root `Cargo.toml`. These are
all derived from it and must never be edited independently:

- `appVersion` in `deploy/helm/orbita/Chart.yaml`
- the pinned `image:` tags in `deploy/manifests/orbita.yaml`
- the version entries for workspace members in `Cargo.lock`
- the heading in `CHANGELOG.md`
- what `orbita --version` prints

**Never hand-edit any of them, including the one in `Cargo.toml`.** Use the
script:

```bash
scripts/bump-version.sh 0.2.0
```

It edits files and stops, so the diff is reviewable before anything is
committed. The reason it exists is that these live in different files read by
different tools, which is exactly the situation where a manual edit updates
three of them and misses the fourth. The moment the chart and the manifests
disagree, the compatibility window stops meaning anything.

The chart's own `version` is the exception. It tracks the templates, not the
binary, and moves on its own schedule. Bump it by hand when you change
something under `deploy/helm/`, and leave it alone otherwise.

## What `develop` carries

`develop` sits on the next version with a `-dev` suffix, for example
`0.2.0-dev`. It does not change per commit. A build is identified by the commit
sha, which `orbita --version` prints alongside the version
(`0.2.0-dev (abc1234)`).

Do not bump the `-dev` version as part of ordinary work. The post-release
back-merge sets it, once per cycle.

## Which bump

Before 1.0 the **minor version is the compatibility unit**. A binary supports
the active cluster version and the one before it, and that window is what makes
a rollback possible. So:

- **Minor** (`0.1.0` to `0.2.0`) for anything that changes the peer protocol, a
  persisted format, or the wire contract. Also for new features.
- **Patch** (`0.1.0` to `0.1.1`) for fixes that change none of those.

A patch release **cannot** change the peer protocol or a persisted format. This
is not a style preference; the rollback window assumes it cannot, and a patch
that breaks it produces a cluster that cannot roll back, discovered in
production.

In practice, treat a change under any of these as forcing a minor:

- `proto/`
- `crates/orbita-proto/`
- `crates/orbita-wal/`
- `crates/orbita-format/`, when the on-disk format changes rather than the code
  that reads it

The Release PR workflow checks the first three and fails a patch bump that
touches them. That check is a coarse approximation and will sometimes be wrong
in the annoying direction. Cut a minor release rather than arguing with it; the
cost of an unnecessary minor before 1.0 is nothing.

## Marking a change as breaking

Commit messages drive the changelog, so the marker belongs there. Use `!` and a
`BREAKING CHANGE:` footer, per the `conventional-commit-message` skill. A
breaking change is promoted into the changelog regardless of its commit type,
including under `refactor`, because that is the case a reader most needs to
see.

## crates.io

Nothing is published. Every crate carries `publish = false`, and a new crate
needs it too. This is deliberate: publishing would be a semver promise about
`orbita-core`'s types that the project is not ready to make before 1.0. Do not
remove `publish = false` without the user asking for it explicitly.
