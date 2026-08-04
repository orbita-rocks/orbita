# Changelog

Notable changes to Orbita, newest first. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the versions are
[semantic](https://semver.org/spec/v2.0.0.html).

Before 1.0 the minor version is the compatibility unit. A minor bump can change
the peer protocol or a persisted format, and a patch bump cannot. That rule is
what the rollback window in [docs/UPGRADES.md](docs/UPGRADES.md) rests on, so a
change that breaks it is a bug in the release, not a judgement call.

Entries under Unreleased are generated from the conventional commit history by
`git-cliff` when a release is cut, and edited by hand when a change deserves a
sentence a commit subject cannot carry.

## [Unreleased]

### Added

- A release process. Releases are cut from tags on `main`, prerelease artifacts
  are built from every push to `develop`, and the workspace version drives the
  image tag, the chart's `appVersion`, and the pinned manifests from one place.
  See [docs/RELEASING.md](docs/RELEASING.md).
