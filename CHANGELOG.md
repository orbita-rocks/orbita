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

## [0.0.1] - 2026-08-04

The first numbered release, so there is a fixed point to build on and to
compare against.

### Added

- A working single node. `orbita dev` serves reads, writes, deletes, and scans
  end to end. See [docs/QUICKSTART.md](docs/QUICKSTART.md) for what works
  today and what does not.
- A working cluster shape. Multi-node clusters start, bind both listeners, and
  report healthy, exercised from outside Rust through the CLI, Docker Compose,
  and Kubernetes manifests. Worker registration with the leader group is the
  outstanding piece, so cross-node serving does not work yet.
- The partition storage format specified in
  [ADR 0006](docs/adr/0006-partitions-are-an-index-over-immutable-objects.md),
  an open format in place of
  RocksDB, with its limits documented rather than implied.
- A release process. Releases are cut from tags on `main`, prerelease artifacts
  are built from every push to `develop`, and the workspace version drives the
  image tag, the chart's `appVersion`, and the pinned manifests from one place.
  See [docs/RELEASING.md](docs/RELEASING.md).
- The build and test tasks run through moon, so the path a laptop takes is the
  same one CI takes.
