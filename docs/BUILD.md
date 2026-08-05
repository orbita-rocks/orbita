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

moon does not own the compiler version. `rust-toolchain.toml` still does, and
rustup still installs it, because that already worked and everyone who writes
Rust already knows it. moon's own version is pinned in `.prototools`, which is
the only version this added.

## Getting set up

```
proto install moon
```

That installs the pinned moon and nothing else. Do not run `proto use` here.
proto reads `rust-toolchain.toml` as a version file, so `proto use` takes the
Rust install away from rustup and hands it to proto, which on a clean machine
produces a toolchain with no cargo in it.

If you do not have proto, one line gets it:

```
curl -fsSL https://moonrepo.dev/install/proto.sh | bash
```

You need rustup and `protoc` on the machine. rustup reads
`rust-toolchain.toml` and installs the pinned compiler the first time you run
anything. `protoc` is a system dependency moon does not manage, and
`orbita-proto` compiles the wire definitions at build time. On a Mac that is
`brew install protobuf`.

You do not need a Python. The end-to-end suite pins one in
`.moon/toolchains.yml` and moon installs it, so the suite runs against the same
interpreter everywhere rather than whatever `python3` points at on your
machine.

## The tasks

Every crate under `crates/` gets `fmt`, `lint`, `test`, and `build` without a
config file of its own. A new crate is a new directory, plus a `dependsOn` in
its `moon.yml` listing the other crates it uses, which the next section is
about.

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
everything downstream of them.

## The one duplicated thing

Each crate lists its internal dependencies in a `dependsOn` in its `moon.yml`,
which its `Cargo.toml` already says. That is a second copy of the graph, and it
is here because moon will not build the first one.

moon infers dependencies from the language's manifest, but its Rust toolchain
plugin only follows a dependency written as `path = "..."` in the crate's own
manifest ([source](https://github.com/moonrepo/plugins/blob/master/toolchains/rust/src/tier2.rs)).
Every internal dependency here is written `orbita-core.workspace = true`, so
the path lives in the root manifest under `[workspace.dependencies]` and the
plugin skips it. Without `dependsOn`, moon sees ten crates that depend on
nothing.

CI is unaffected, because it runs every crate every time. What breaks is
`--affected`: a graph with no edges means a change to `orbita-core` looks like
it touches nothing downstream, and you get told your change is tested when it
is not. So if you add a crate dependency to a `Cargo.toml`, add it to the
`moon.yml` next to it.

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

`moon run e2e:test` installs the pinned Python, builds the binary, creates a
virtualenv under `tests/e2e`, installs the test dependencies into it, and runs
pytest. The virtualenv is deliberate: running the suite should not install
anything into whichever python is first on your PATH.

moon has plugins that would own the virtualenv and the install too, which would
delete the `setup` task. They are not usable yet, and
[.moon/toolchains.yml](../.moon/toolchains.yml) says why.

## Live object store verification

`moon run orbita-objectstore:test-live-s3` runs the two tests in
`crates/orbita-objectstore/tests/minio.rs` against whatever S3-compatible
endpoint the environment points at. The second of them is the one that matters:
it proves a deposed writer holding a stale ETag loses the manifest swap to the
writer that replaced it, which is the entire fencing story for the object store.

This cannot be proved against a mock. The failure it guards against is a server
that accepts `If-Match` and ignores it, and such a server passes every mocked
test in the crate while losing the race in production. So the tests take their
endpoint from the environment and get pointed at three real servers:

| Backend | Where | When |
| --- | --- | --- |
| MinIO | the `check` job in `ci.yml` | every pull request |
| AWS S3 | `live-object-store.yml` | Mondays, 08:00 UTC, and on demand |
| Cloudflare R2 | `live-object-store.yml` | Mondays, 08:00 UTC, and on demand |

GCS is out of scope on purpose. Its XML interoperability layer ignores these
headers and wants `x-goog-if-generation-match` instead, so supporting it means
a second `ObjectStore` rather than this one with a different endpoint.

### Configuration an operator has to create

These live on the repository. Everything that identifies an account or
authenticates to one is a secret. The region is a variable, because GitHub
masks secret values wherever they appear in a log, and masking a string as
short and as common as `us-east-1` would redact unrelated output and make a
failure harder to read rather than easier.

| Name | Kind | Holds |
| --- | --- | --- |
| `LIVE_S3_REGION` | variable | The region the AWS test bucket lives in. The endpoint is derived from it. |
| `LIVE_S3_BUCKET` | secret | The AWS test bucket name. It must already exist. |
| `LIVE_S3_ACCESS_KEY_ID` | secret | Access key id for the IAM principal below. |
| `LIVE_S3_SECRET_ACCESS_KEY` | secret | Its secret access key. |
| `LIVE_R2_ACCOUNT_ID` | secret | Cloudflare account id. It is in the R2 endpoint hostname, which is why it is not a variable. |
| `LIVE_R2_BUCKET` | secret | The R2 test bucket name. It must already exist. |
| `LIVE_R2_ACCESS_KEY_ID` | secret | R2 API token access key id, scoped to Object Read and Write on that one bucket. |
| `LIVE_R2_SECRET_ACCESS_KEY` | secret | Its secret access key. |

Both buckets should be dedicated to this and nothing else, and both want a
lifecycle rule expiring objects under `orbita-it/` after a day. The tests clean
up after themselves on the happy path, but the run that fails is the run that
leaves an object behind, and that is also the run you least want to be doing
bucket housekeeping during.

The AWS key needs nothing beyond the prefix the tests write to. Every key is
created under `orbita-it/`, so:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListOnlyTheTestPrefix",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::REPLACE-WITH-BUCKET",
      "Condition": { "StringLike": { "s3:prefix": "orbita-it/*" } }
    },
    {
      "Sid": "ObjectsUnderTheTestPrefix",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::REPLACE-WITH-BUCKET/orbita-it/*"
    }
  ]
}
```

`s3:GetObject` covers `HeadObject` and ranged reads as well as whole-object
reads, and conditional PUT needs no permission of its own beyond
`s3:PutObject`. There is no `s3:*` here and there should not be: this key sits
in a CI secret store, and the blast radius of it leaking should be one prefix
in one bucket.

Long-lived keys are the easy path, not the good one. The tests already read an
optional `ORBITA_S3_TEST_SESSION_TOKEN`, so the AWS half can move to GitHub's
OIDC provider and a role with the policy above without anyone touching Rust.
That is worth doing and is not done yet.

### When the configuration is absent

The workflow does not run in a fork at all. A fork has no buckets and no
credentials and never will, so the only thing a scheduled run there produces is
a failure notification for somebody who cannot act on it.

Inside this repository it fails, loudly, naming the missing secret. That looks
like it contradicts the rule the MinIO step in `ci.yml` is built on, that a
durability test which skips itself is worse than no test, but it is the same
rule applied to a different situation. On the pull request path the credentials
are a container we start ourselves, so they cannot go missing, and the right
answer is to never allow a skip. Here they are supplied by a human out of band,
so they can go missing, and a skip would produce exactly the outcome the rule
exists to prevent: a green run standing in for evidence nobody collected.

The practical consequence is that the first Monday after this lands is red until
the secrets exist. That is the alarm doing its job, not a defect.

### Who finds out when it breaks

A scheduled workflow that fails emails whoever last edited the cron line, which
is an accident of git history rather than a decision. So a failure opens an
issue instead, assigned to the code owner of `/crates/orbita-objectstore/`, and
a second consecutive failure comments on the same issue rather than opening
another. If `.github/CODEOWNERS` changes, change the assignee in the workflow
with it.

It is not a page. A failure here does not mean anything is down. It means a
claim we make about a backend may have stopped being true, and answering that
means reproducing it by hand, working out whether the vendor changed or we did,
and then either fixing the store or withdrawing the claim. None of that goes
faster for having woken somebody up.

### What it does not prove

The tests exercise the conditional write semantics. They do not systematically
exercise what a backend does to the TCP connection afterwards, and that has
already bitten us once: MinIO answers a losing conditional PUT with `412` and
then closes the socket without saying `Connection: close`, so hyper pooled a
dead connection and the next unrelated request failed. The transport now drops
the connection on any `409` or `412`, which covers the AWS and R2 versions of
that same event for free.

What is not covered is a backend that hangs up after some other status, a `404`
or a `416` or a `200`, which we still pool. That was measured against MinIO and
assumed of the others. It is a race, so these two tests would catch it only by
luck. If a live run ever fails with `client error (SendRequest)`, that is the
first thing to suspect.

## Denying warnings

`-D warnings` is an argument to the clippy task rather than a `RUSTFLAGS` set
in the CI environment. That way the clippy you run is the clippy that gates the
merge, and a warning does not first appear as a red pull request.
