---
name: cutting-a-release
description: Cut, tag, or troubleshoot an Orbita release. Use when asked to release a version, prepare a release PR, tag a build, ship or publish a version, produce a prerelease, or work out why a release or prerelease pipeline failed. Triggers on "cut a release", "release 0.2.0", "tag main", "ship this", "publish a build", "open a release PR", "the release workflow failed".
argument-hint: [version, e.g. 0.2.0]
---

# Cutting an Orbita release

The full explanation lives in [docs/RELEASING.md](../../../docs/RELEASING.md).
This skill is what to actually do, and what not to do on the user's behalf.

## Before anything else

**Never push a tag without the user explicitly asking for that step.** Pushing
a tag is what publishes. It builds and pushes an image, uploads binaries, and
creates a public GitHub release, and none of that can be taken back once
somebody has pulled it. Deleting the tag afterwards does not help. Creating a
tag locally is fine and reversible; pushing it is not.

The same applies to merging the release pull request. Open it, report it, and
stop.

## The shape of the system

One version, in `[workspace.package] version` in the root `Cargo.toml`.
Everything else is derived from it. `develop` carries the next version with a
`-dev` suffix and builds prerelease artifacts on every push. `main` carries the
last release, and a tag on `main` publishes.

If you need to reason about which bump is allowed, that is the
`versioning-and-compatibility` skill, not this one.

## Cutting a release

### 1. Open the release pull request

Run the `Release PR` workflow with the version. This branches from `develop`,
runs the bump script, and opens a `v0.2.0 release` pull request against `main`.

```bash
gh workflow run release-pr.yml -f version=0.2.0
```

Doing it by hand is the same thing, and is what to fall back to if the workflow
is unavailable:

```bash
git switch develop && git pull
git switch -c release/v0.2.0
scripts/bump-version.sh 0.2.0
git commit -am "chore(release): v0.2.0"
git push -u origin release/v0.2.0
gh pr create --base main --title "v0.2.0 release" --body "..."
```

### 2. The merge is the user's

The pull request must merge with a **merge commit, never a squash**. Squashing
rewrites the develop commits, which stops `develop` from being an ancestor of
`main` and makes every later release conflict in the same handful of files. If
you see the repository configured to allow squash merges on `main`, say so.

### 3. Tag, only when asked

```bash
git switch main && git pull
scripts/tag-release.sh v0.2.0
```

Without `--push` this creates the tag locally and prints the push command. It
refuses a dirty tree, a version the manifests disagree with, a commit that is
not reachable from `main`, and a release with no changelog section. Do not work
around a refusal by tagging manually; each of those checks is there because the
failure it catches is expensive.

Add `--push` only when the user has asked for the release to go out.

### 4. Afterwards

The `Post-release` workflow opens a pull request back-merging `main` into
`develop` and moving the version to the next `-dev`. If it did not run, the
back-merge still has to happen, or the next release pull request shows the
version bump as a revert.

## Prereleases

There is nothing to do. Every push to `develop` publishes
`ghcr.io/orbita-rocks/orbita:develop` and a `sha-` tag, and uploads a Linux
binary as a workflow artifact for 30 days. When pointing somebody at a
prerelease build, quote the `sha-` tag rather than `develop`, because `develop`
will have moved by the time they read it.

## When something fails

| Symptom | Cause |
| --- | --- |
| The tag and the workspace version disagree | Tagging a commit that was never bumped. Tag the release commit on `main`, not the tip of a branch. |
| The tag and the chart `appVersion` disagree | Something was hand-edited instead of going through `bump-version.sh`. |
| `not reachable from origin/main` | Tagging from `develop` or a feature branch. Releases come from `main`. |
| `CHANGELOG.md has no section for X` | `bump-version.sh` did not run, or ran for a different version. |
| `CHANGELOG.md already has a section for X` | The bump ran twice. Reset the changelog rather than deleting one section by hand. |
| The release pull request has no checks | `RELEASE_TOKEN` is unset, so the pull request was opened with `GITHUB_TOKEN`, which cannot trigger workflows. Push an empty commit to wake CI. |
| `cargo fetch --locked` fails in the image build | A crate was added without updating the `Dockerfile` manifest list. See the `adding-a-crate` skill. |
| A patch bump is rejected for touching the protocol | Working as intended. Cut a minor release. |

## Things to leave alone

The version bump and the tag are manual on purpose. Do not add a workflow that
tags automatically on merge, and do not extend the release scripts to push on
their own. The whole design puts the irreversible step behind a deliberate
human action, and automating it away is the one change that defeats the point.
