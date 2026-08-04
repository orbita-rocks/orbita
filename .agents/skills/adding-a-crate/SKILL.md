---
name: adding-a-crate
description: Add a new crate to the Orbita workspace without breaking the image build or the affected-task graph. Use when creating a new crate, splitting an existing one, renaming a crate, or removing one. Triggers on "add a crate", "new crate", "split this into a crate", "extract into its own crate", "cargo fetch fails in Docker", "the image build cannot find a manifest".
---

# Adding a crate to the workspace

Four files know about the set of crates, and only one of them is `Cargo.toml`.
The other three fail in ways that do not point back at the crate you added, so
this is a checklist rather than a discussion.

## The checklist

**1. `Cargo.toml` at the root.** Add the path to `members`. If other crates
will depend on it, also add it to `[workspace.dependencies]` as a `path`
dependency, so siblings can write `orbita-thing.workspace = true`.

**2. The crate's own `Cargo.toml`.** Inherit everything inheritable, and set
`publish = false` like its siblings:

```toml
[package]
name = "orbita-thing"
description = "One sentence on what this crate is for."
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true
repository.workspace = true
authors.workspace = true

publish = false
```

Do not write a literal `version = "0.0.0"` for an internal dependency. It is a
copy of the workspace version that nothing keeps in sync, and it rots on the
first release. Use `orbita-thing.workspace = true`.

**3. `crates/orbita-thing/moon.yml`.** List the internal crates it depends on:

```yaml
# https://moonrepo.dev/docs/config/project
$schema: 'https://moonrepo.dev/schemas/project.json'

# Mirrors this crate's Cargo dependencies. See .moon/tasks/rust.yml for why
# these are written out by hand rather than inferred.
dependsOn:
  - 'orbita-core'
```

This duplicates what `Cargo.toml` says, and
[docs/BUILD.md](../../../docs/BUILD.md) explains why moon cannot infer it: its
Rust plugin only follows dependencies written as `path = "..."` in the crate's
own manifest, and every internal dependency here goes through the workspace.

Skipping this does not break CI, which runs every crate every time. It breaks
`moon run :test --affected`, which will report that a change touches nothing
downstream. That is the one wrong answer that matters, because it looks like a
pass. Also add the new crate to the `dependsOn` of anything that depends on it.

**4. `Dockerfile`.** This is the one that actually breaks the build. The
dependency-caching layer copies crate manifests by name and then creates stub
sources for them, so a new crate needs two edits:

```dockerfile
COPY crates/orbita-thing/Cargo.toml crates/orbita-thing/
```

and its bare name added to the loop below:

```dockerfile
&& for c in core runtime objectstore format thing proto ... ; do \
```

Miss either and `cargo fetch --locked` fails, because the root manifest names a
workspace member whose manifest is not in the image. The error talks about a
missing manifest and not about the crate you added. A binary crate needs a
`src/main.rs` stub as well; `orbita-cli` is the example.

## Verifying

```bash
moon query projects --id orbita-thing     # moon sees it
moon run :fmt :lint :test --query "language=rust"
```

For the Dockerfile, the cheap check is to replay just that layer rather than
building the whole image: copy the root manifests plus exactly the crate
manifests the `Dockerfile` names into an empty directory, create the stub
sources, and run `cargo fetch --locked` there. It fails in seconds if the list
is wrong, instead of several minutes into an image build.

Every push to `develop` builds the image, so a mistake here surfaces on the
integration branch rather than during a release. That is the safety net, not a
substitute for the checklist.

## Removing or renaming

The same four places, in reverse. A stale entry in the `Dockerfile` loop is
harmless, but a stale `COPY` line fails the build with a missing-file error,
and a stale `dependsOn` makes moon fail to resolve the project graph.
