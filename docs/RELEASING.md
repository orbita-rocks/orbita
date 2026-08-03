# Releasing Orbita

This document explains how a release is made and why it is shaped the way it
is. [docs/UPGRADES.md](UPGRADES.md) covers what a version means to a running
cluster; this one covers how a version comes to exist.

## The shape of it

There is one version, and it lives in `[workspace.package] version` in the root
`Cargo.toml`. The image tag, the chart's `appVersion`, the pinned images in the
plain manifests, and what `orbita --version` prints are all derived from it.
They live in separate files because the tools that read them are separate,
which is exactly the situation where a hand edit updates three of them and
misses the fourth. `scripts/bump-version.sh` is the reason that cannot happen.

There are two branches. `develop` is where work lands and where prerelease
artifacts come from. `main` only ever contains released code, and a tag on
`main` is what publishes.

|                | `develop`                | `main`                       |
| -------------- | ------------------------ | ---------------------------- |
| Version        | `0.2.0-dev`              | `0.1.0`                      |
| Built on       | every push               | a pushed tag                 |
| Image tags     | `develop`, `sha-abc1234` | `0.1.0`, `0.1`, `latest`     |
| Binaries       | workflow artifacts, 30d  | attached to the release      |
| Chart          | not published            | pushed to GHCR as OCI        |
| GitHub release | none                     | yes                          |

The `-dev` suffix is a real semver prerelease, so `0.2.0-dev` sorts before
`0.2.0` and Cargo is happy with it. It does not change per commit. A commit is
identified by the sha in the image tag and by the sha baked into the binary,
which is why `orbita --version` prints `0.2.0-dev (abc1234)`. Bumping the
version on every merge would produce a version number nobody could reason
about and a lockfile churning on every commit.

## Cutting a release

Run the **Release PR** workflow from the Actions tab with the version you want,
say `0.1.0`. It branches from `develop`, runs the bump script, and opens a pull
request titled `v0.1.0 release` against `main`.

That pull request is the release. Reviewing it is the one point in the process
where somebody looks at everything that has accumulated since the last release
in a single diff, which is worth more than reviewing the version bump it
contains.

Merge it with a merge commit. Not a squash. Squashing rewrites the develop
commits, which means `develop` stops being an ancestor of `main`, and from then
on every release pull request conflicts and every back-merge fights. This is
enforced as a branch protection setting rather than left as a convention,
because it is a mistake somebody will make exactly once and then spend an
afternoon undoing.

Merging publishes nothing. Then tag:

```bash
git switch main && git pull
scripts/tag-release.sh v0.1.0 --push
```

The script refuses to tag a dirty tree, a version the manifests disagree with,
a commit that is not reachable from `main`, or a release with no changelog
section. The push is behind its own flag because that is the half that cannot
be taken back: once the image is pulled and the binary is downloaded, deleting
the tag does not help anybody.

Pushing the tag starts the **Release** workflow, which verifies the tag against
the tree, reruns the full test suite with a hundred thousand simulation seeds,
builds binaries for four targets, builds and signs a multi-architecture image,
publishes the chart if its version moved, and creates the GitHub release. The
publishing jobs sit behind the `release` environment, so there is a second
approval between a pushed tag and a public artifact.

Afterwards the **Post-release** workflow opens a pull request merging `main`
back into `develop` and setting the version to `0.2.0-dev`. It looks like
bookkeeping, and it is, but skipping it is what makes the next release painful:
the bump would exist only on `main`, and the following release pull request
would show it as a revert.

## Patch releases

A fix that cannot wait for the next minor goes on a branch off `main`, not off
`develop`, and the release pull request targets `main` directly. Everything
else is the same. The back-merge still has to happen, and it matters more here
than usual, because the fix does not exist on `develop` until it does.

Before 1.0 the minor version is the compatibility unit. A patch release cannot
change the peer protocol or a persisted format, because the rollback window
described in [UPGRADES.md](UPGRADES.md) assumes it cannot. The Release PR
workflow checks this by looking at whether `proto/`, `crates/orbita-proto/`, or
`crates/orbita-wal/` changed, and fails a patch bump that touches them. That
check is a coarse approximation and will occasionally be wrong in the annoying
direction; cut a minor release rather than arguing with it.

## What each piece is

- `scripts/bump-version.sh` sets the version everywhere it appears. It edits
  files and stops, so it is safe to run locally and read the diff.
- `scripts/roll-changelog.sh` turns the Unreleased section into a released one,
  filling it from the conventional commit history with `git-cliff` first if it
  is installed.
- `scripts/tag-release.sh` checks that a commit is releasable, then tags it.
- `.github/workflows/release-pr.yml` opens the release pull request.
- `.github/workflows/release.yml` is everything a tag does.
- `.github/workflows/prerelease.yml` is everything a push to `develop` does.
- `.github/workflows/post-release.yml` back-merges and opens the next cycle.
- `cliff.toml` decides which commit types reach the changelog. It follows the
  visible and hidden split in the conventional commit skill, with anything
  marked breaking promoted regardless of type.

## Repository settings this assumes

These are not in the repository, and the process quietly stops working without
them.

`main` requires a pull request, requires CI to pass, and allows merge commits
only. `develop` is the default branch, so a pull request opened without
thinking lands in the right place. A tag ruleset restricts who can push `v*`.
There is a `release` environment with a required reviewer, which is the second
gate on publishing.

One secret is worth adding: `RELEASE_TOKEN`, a PAT or app token used to open
the release and back-merge pull requests. The default `GITHUB_TOKEN` cannot
trigger other workflows, so a pull request opened with it arrives with no
checks running on it and nothing to gate the merge. The workflows fall back to
`GITHUB_TOKEN` if the secret is absent, and the symptom of the fallback is a
release pull request with no checks; pushing an empty commit to it wakes CI up.

## Things deliberately not done

Nothing is published to crates.io. Every crate carries `publish = false`. These
are internal libraries, and publishing them would be a semver promise about
`orbita-core`'s types that is not worth making before 1.0. The product is the
binary and the image.

There are no musl builds. The image builds RocksDB from source with a C++
toolchain, and static linking against musl is a project of its own. It becomes
much cheaper if the storage engine stops needing C++, and is worth revisiting
then.
