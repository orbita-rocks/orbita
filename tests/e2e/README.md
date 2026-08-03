# End-to-end tests

These tests drive a running Orbita node from Python, over a real socket, using
gRPC stubs generated from `/proto` by stock `protoc`. They exist to check the
wire contract from outside the Rust build.

## Running them

```
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python -m pytest
```

The first run builds the `orbita` binary if `target/debug/orbita` is missing.
Set `ORBITA_BINARY` to point at a binary you already built.

## What this is, and what it is not

This is a test harness. It is not a Python client library, it is not published,
and nothing outside this directory should import it. If it starts growing
retry logic, connection pooling, or convenience wrappers, that is a signal to
build a real client library on purpose rather than letting one accumulate here
by accident.

The Rust integration tests already cover the same behaviours using tonic's
generated client. That client comes out of the same build as the server, so
both sides share one set of generated types and agree with each other by
construction even if the protocol definition is wrong. These tests exist for
the failure that cannot catch: a protocol that only works when both ends were
generated together. The product also claims a client can be dumb, meaning
usable from generated stubs with no hand-written library, and this suite is the
only thing testing that claim.

The stubs are regenerated on every run and never committed. Regenerating is
part of the test: it proves the checked-in protos still compile under a
toolchain that knows nothing about prost or tonic.

## Layout

- `harness.py` starts and stops the node, and generates the stubs.
- `conftest.py` holds the fixtures. The `kv` fixture is a connected Kv stub
  against a node with an empty data directory.
- `test_cluster.py` is the marker for multi-node tests, which cannot be
  written yet.

Each test gets its own node on a free port over its own temporary data
directory. That costs about a fifth of a second per test and removes every
question about one test seeing another's keys.

## Where a dumb client has to guess

These are the places where writing a naive client meant knowing something the
protocol definitions do not say. Each is small on its own. Together they are
the gap between "you can use generated stubs" and "you can use generated stubs
without reading our Rust".

**The size limits are not in the protos.** Fixed. `GetLimits` now reports the
key, value, and list page limits, along with the maximum gRPC message size a
client should configure its channel to. The harness calls it during startup and
sizes the real connection from the answer, which is what a client library
should do, and the tests take sizes from it rather than hardcoding numbers
copied out of the Rust source.

This one mattered more than it looked. gRPC implementations default to a 4 MB
message limit, so a keyspace configured above that would fail inside a client's
own gRPC stack with an error about message size and nothing about Orbita.

**`Condition.if_not_present` is a bool inside a oneof, and only `true` means
anything.** Setting it to `false` selects the oneof arm, so it is not the same
as sending no condition, but the server writes anyway. A reader could
reasonably expect `if_not_present: false` to mean "apply only if present",
which is a condition the API does not have. An empty message arm, or an enum,
would make the wrong thing unrepresentable.

**`NOT_FOUND` means the keyspace, never the key.** A missing key is a
successful call with `found` set to false. That is the right split, since a
missing key is an ordinary outcome, but nothing in the protos says which
statuses the service uses, so the first thing a client author writes is
probably a mapping of `NOT_FOUND` to "no such key".

**A failed condition is likewise a successful call**, reported by `applied`.
Also right, also undocumented in the protos. Both of these belong in comments
on the RPCs, since the statuses are the entire error contract for someone who
only has the generated stubs.

**`limit: 0` means "server's choice", not "no entries".** Zero is the proto3
default, so every client that leaves the field alone sends it, and what it gets
back is a page size nobody wrote down.

**`ttl_millis: 0` writes a key that can never be read.** The call reports
`applied` and a version, and the key is already expired. A client computing a
TTL as `deadline - now` will land on zero eventually. Whether that should be an
`INVALID_ARGUMENT` is a judgement call, but silently accepting a write that is
gone before it returns is the kind of thing that becomes a support thread.

**There is no way to find out which keyspaces exist.** `ListKeyspaces` is on
the Admin service, which returns `UNIMPLEMENTED`. Today a client has to be told
out of band that the keyspace is called `default`. This is a known gap rather
than a design flaw, and it is listed here because it is a real obstacle now.

**There is no health or readiness check.** Neither `grpc.health.v1.Health` nor
server reflection is registered, so this harness waits for a node by retrying a
real `Get` against a keyspace it has to already know the name of. Every load
balancer and orchestrator wants the standard health service, and every test
harness in every language will otherwise reinvent this loop.

What the protocol gets right, since the list above is one-sided: `optional` is
used where it carries meaning, so a lost compare-and-swap against an absent key
is genuinely distinguishable from one against a different version. The list
cursor is bound to its prefix and says so when it is misused rather than
quietly scanning the wrong range. The oneof arms behave identically from Python
and from Rust. `DeleteResponse` separates `applied` from `existed`, which is
what makes a retried delete safe.

**Does a Python user need a helper library?** Not for correctness. Everything
in the API is reachable and behaves sanely from generated stubs, and the tests
here are the evidence. What they would want is a thin ergonomic layer: a retry
loop around a compare-and-swap, an iterator over `List` that carries the
cursor, and constants for the limits. That is fifty lines of application code,
not a library, and the claim survives. Publishing the limits in the protos, as
constants or as an RPC, and registering the standard health service would close
most of what is left.
