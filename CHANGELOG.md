# Changelog

## v0.0.1 (2026-08-04)

This is the first cut of Orbita, versioned so there is a fixed point to build
on and to compare against. Nothing before this had a number, which made it
impossible to talk about what changed between two states of the project.

What is in it:

- A working single node. `orbita dev` serves reads, writes, deletes, and scans
  end to end.
- A working cluster shape. Multi-node clusters start, bind both listeners, and
  report healthy, and the deployment is exercised from outside Rust through
  the CLI, Docker Compose, and Kubernetes manifests. Worker registration with
  the leader group is the outstanding piece, so cross-node serving does not
  work yet.
- An open storage format in place of RocksDB, with the format's limits
  documented rather than implied, and the partition format implemented as
  specified in ADR 0006.
- The build and test tasks run through moon, so every path through the build
  is the same one CI takes.
- The open source boilerplate, meaning the license and contribution scaffolding
  a public repository needs.

The known gaps, meaning the admin service and worker registration, are listed
in [docs/QUICKSTART.md](docs/QUICKSTART.md) so nobody spends an hour
discovering them.
