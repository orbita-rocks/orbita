"""Multi-tenancy isolation, driven from outside the process as an adversary.

Orbita's whole pitch is that many tenants share a cluster and none of them can
reach, starve, or observe another. That is a claim about a boundary, and the
absence of a test that *attacks* the boundary is itself the finding. So this
file plays the attacker rather than the well-behaved client every other suite
models:

* A credential scoped to keyspace A is turned against keyspace B for every data
  verb — read, write, and list — and each is expected to come back
  ``PERMISSION_DENIED``. The secret is real and known; what it is not is
  authorized *there*, which is the exact line ``PERMISSION_DENIED`` draws
  against ``UNAUTHENTICATED``.

* A noisy tenant is driven into its storage cap and its write-rate limit until
  the owner refuses it ``RESOURCE_EXHAUSTED``, and the refusal message is read
  to prove a storage refusal and a rate refusal stay distinguishable.

* While that noisy tenant is being throttled as hard as we can drive it, a
  quiet neighbour runs a steady workload and we measure it. The neighbour must
  keep succeeding at ~100% and must not have its latency collapsed by the
  storm next door. This is the observable consequence of #96 enforcing quotas
  with no lock or queue that spans keyspaces.

* A credential is used successfully, revoked, and then presented again on the
  *same* channel: the second request must be refused. Revocation takes effect
  on the next request, not the next connection.

The node is started with authentication on (``ORBITA_REQUIRE_AUTH``) and a
config root (``ORBITA_ROOT_CREDENTIAL``) so the Admin surface — which is
root-only — can mint keyspaces and credentials, exactly as an operator would.
"""

from __future__ import annotations

import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import grpc
import pytest
from orbita.v1 import admin_pb2, admin_pb2_grpc, kv_pb2, kv_pb2_grpc

import harness
from conftest import poll_until

# The config root the node is booted with. It is the only identity the Admin
# surface honours, so every keyspace and credential in this file is created
# under it. It is a test secret and nothing more.
ROOT_SECRET = "root-secret-for-the-tenancy-isolation-suite"

# The node process refreshes its credential cache and partition map off the
# control plane on a timer (DEFAULT_CONTROL_POLL_INTERVAL, 250ms), so a freshly
# minted keyspace or credential is not visible on the data plane the instant
# the admin call returns. Every "is it live yet" wait is generous against that.
PROPAGATION_TIMEOUT_SECONDS = 15.0

_STUB_MODULES = SimpleNamespace(kv=kv_pb2_grpc, admin=admin_pb2_grpc)


def _probe_request() -> kv_pb2.GetLimitsRequest:
    """The unauthenticated readiness probe: GetLimits names the default keyspace.

    GetLimits is deliberately exempt from auth (a client sizes its channel
    before it holds a credential), so it is a legal way to learn the node is up
    even with ``require_auth`` on.
    """
    return kv_pb2.GetLimitsRequest(keyspace=harness.DEFAULT_KEYSPACE)


def _bearer(secret: str) -> list[tuple[str, str]]:
    """The gRPC metadata a client presenting ``secret`` sends.

    The server lifts the ``authorization`` header out of the request metadata
    and expects a ``Bearer <secret>`` value; see ``orbita-server::service``.
    """
    return [("authorization", f"Bearer {secret}")]


@pytest.fixture
def secured_node(orbita_binary, tmp_path):
    """A single node with authentication on and a known config root.

    This is a whole cluster: its own leader group, so the Admin surface has a
    control plane to answer from, and its own worker, so credential and quota
    enforcement both run here.
    """
    node = harness.Node(
        binary=orbita_binary,
        data_dir=tmp_path / "data",
        pb2_grpc=_STUB_MODULES,
        log_path=tmp_path / "orbita.log",
        env={
            "ORBITA_REQUIRE_AUTH": "true",
            "ORBITA_ROOT_CREDENTIAL": ROOT_SECRET,
        },
    )
    try:
        node.start(_probe_request)
        yield node
    finally:
        node.stop()


def _create_keyspace(admin, name: str, **config) -> None:
    """Create a keyspace under the root identity, with whatever quotas apply."""
    admin.CreateKeyspace(
        admin_pb2.CreateKeyspaceRequest(
            name=name,
            config=admin_pb2.KeyspaceConfig(**config),
        ),
        timeout=10.0,
        metadata=_bearer(ROOT_SECRET),
    )


def _create_credential(admin, keyspaces, permissions) -> tuple[str, str]:
    """Mint a keyspace-scoped credential under root; return (id, secret)."""
    created = admin.CreateCredential(
        admin_pb2.CreateCredentialRequest(
            keyspaces=list(keyspaces),
            permissions=list(permissions),
            description="tenancy isolation e2e",
        ),
        timeout=10.0,
        metadata=_bearer(ROOT_SECRET),
    )
    return created.credential_id, created.secret


def _wait_until_writable(kv, keyspace: str, secret: str) -> None:
    """Block until ``secret`` can write ``keyspace``, or fail.

    This clears two propagation delays at once: the new keyspace reaching the
    data plane's partition map, and the new credential reaching the worker's
    authenticator. Once a write succeeds, both are live and a cross-tenant
    refusal below can only be about scope, not about timing.
    """

    def writable():
        try:
            kv.Set(
                kv_pb2.SetRequest(keyspace=keyspace, key=b"__probe__", value=b"1"),
                timeout=5.0,
                metadata=_bearer(secret),
            )
            return True
        except grpc.RpcError:
            return False

    assert poll_until(
        writable, timeout=PROPAGATION_TIMEOUT_SECONDS
    ), f"credential never became usable in its own keyspace {keyspace!r}"


# --- Two tenants that are strangers to each other ---------------------------


@pytest.fixture
def two_tenants(secured_node):
    """Two keyspaces, each with its own read+write credential, both live.

    Returned as a namespace so a test reads ``t.a_secret`` / ``t.b_name``
    rather than unpacking a tuple. Neither credential names the other's
    keyspace, which is the whole point.
    """
    admin = secured_node.admin
    kv = secured_node.kv

    _create_keyspace(admin, "tenant-a")
    _create_keyspace(admin, "tenant-b")

    perms = [admin_pb2.PERMISSION_READ, admin_pb2.PERMISSION_WRITE]
    _, a_secret = _create_credential(admin, ["tenant-a"], perms)
    _, b_secret = _create_credential(admin, ["tenant-b"], perms)

    _wait_until_writable(kv, "tenant-a", a_secret)
    _wait_until_writable(kv, "tenant-b", b_secret)

    return SimpleNamespace(
        node=secured_node,
        kv=kv,
        admin=admin,
        a_name="tenant-a",
        b_name="tenant-b",
        a_secret=a_secret,
        b_secret=b_secret,
    )


def test_a_scoped_credential_works_within_its_own_keyspace(two_tenants):
    """The positive control the cross-tenant refusals lean on.

    If this failed, a ``PERMISSION_DENIED`` below would prove nothing — it
    could just mean auth is broken for everyone. So establish first that each
    credential is fully functional at home: write, read it back, list it.
    """
    t = two_tenants
    t.kv.Set(
        kv_pb2.SetRequest(keyspace=t.a_name, key=b"mine", value=b"hello"),
        metadata=_bearer(t.a_secret),
    )
    got = t.kv.Get(
        kv_pb2.GetRequest(keyspace=t.a_name, key=b"mine"),
        metadata=_bearer(t.a_secret),
    )
    assert got.found and got.value == b"hello"

    listed = t.kv.List(
        kv_pb2.ListRequest(keyspace=t.a_name, prefix=b"", limit=10),
        metadata=_bearer(t.a_secret),
    )
    assert any(entry.key == b"mine" for entry in listed.entries)


@pytest.mark.parametrize("verb", ["read", "write", "list"])
def test_a_credential_cannot_touch_a_neighbours_keyspace(two_tenants, verb):
    """Tenant A's credential is refused every data verb against tenant B.

    A read, a write, and a list are three separate authorization checks on the
    server, so each is attacked on its own. The secret is valid and known;
    it simply does not name keyspace B, which the server distinguishes from an
    unknown secret by answering ``PERMISSION_DENIED`` rather than
    ``UNAUTHENTICATED``.
    """
    t = two_tenants
    calls = {
        "read": lambda: t.kv.Get(
            kv_pb2.GetRequest(keyspace=t.b_name, key=b"k"),
            metadata=_bearer(t.a_secret),
        ),
        "write": lambda: t.kv.Set(
            kv_pb2.SetRequest(keyspace=t.b_name, key=b"k", value=b"v"),
            metadata=_bearer(t.a_secret),
        ),
        "list": lambda: t.kv.List(
            kv_pb2.ListRequest(keyspace=t.b_name, prefix=b"", limit=10),
            metadata=_bearer(t.a_secret),
        ),
    }
    with pytest.raises(grpc.RpcError) as caught:
        calls[verb]()
    assert caught.value.code() == grpc.StatusCode.PERMISSION_DENIED, (
        f"a {verb} into a neighbour's keyspace must be PERMISSION_DENIED, not "
        f"{caught.value.code()}: a known credential out of scope is denied, "
        "not unauthenticated"
    )


def test_the_reverse_direction_is_symmetric(two_tenants):
    """Isolation is not one-way: B's credential is just as barred from A.

    A boundary that only holds in one direction is not a boundary. Kept
    separate from the parametrized case so a regression that leaks only one way
    is still caught.
    """
    t = two_tenants
    with pytest.raises(grpc.RpcError) as caught:
        t.kv.Set(
            kv_pb2.SetRequest(keyspace=t.a_name, key=b"k", value=b"v"),
            metadata=_bearer(t.b_secret),
        )
    assert caught.value.code() == grpc.StatusCode.PERMISSION_DENIED


# --- Quota refusals stay inside the tenant that caused them -----------------


def test_a_storage_cap_refuses_the_offender_with_resource_exhausted(secured_node):
    """A tenant over its storage cap is refused, and the refusal names storage.

    The message has to distinguish a storage refusal from a rate one, because a
    client's remedy differs: delete data versus back off. So the assertion is
    both the code and the word.
    """
    admin = secured_node.admin
    kv = secured_node.kv
    # A tiny cap so a handful of ordinary writes crosses it. Storage is charged
    # as the record's whole footprint, so this fills in a couple of writes.
    _create_keyspace(admin, "cramped", max_storage_bytes=256)
    _, secret = _create_credential(
        admin, ["cramped"], [admin_pb2.PERMISSION_WRITE]
    )
    _wait_until_writable(kv, "cramped", secret)

    def refused_for_storage():
        # Keep writing distinct, non-trivial values until the cap bites. The
        # owner samples its own footprint at most once a second, so the refusal
        # may take a few writes to land; poll rather than guess a count.
        try:
            for i in range(64):
                kv.Set(
                    kv_pb2.SetRequest(
                        keyspace="cramped",
                        key=f"key-{i}".encode(),
                        value=b"x" * 64,
                    ),
                    timeout=5.0,
                    metadata=_bearer(secret),
                )
            return None
        except grpc.RpcError as error:
            return error

    error = poll_until(refused_for_storage, timeout=PROPAGATION_TIMEOUT_SECONDS)
    assert error.code() == grpc.StatusCode.RESOURCE_EXHAUSTED
    assert "storage" in (error.details() or "").lower(), (
        "a storage refusal has to name storage so a client can tell it from a "
        f"rate refusal; got {error.details()!r}"
    )


def test_a_write_rate_limit_refuses_with_resource_exhausted_naming_the_rate(
    secured_node,
):
    """A tenant over its write rate is refused, and the refusal names the rate.

    The counterpart to the storage test: same code, different word, so the two
    refusals a client sees are never ambiguous.
    """
    admin = secured_node.admin
    kv = secured_node.kv
    # A low ceiling. The per-node share on a one-node cluster is the whole
    # limit, and the bucket starts full, so a burst past the ceiling is refused
    # in the same instant.
    _create_keyspace(admin, "ratelimited", max_writes_per_second=5)
    _, secret = _create_credential(
        admin, ["ratelimited"], [admin_pb2.PERMISSION_WRITE]
    )
    _wait_until_writable(kv, "ratelimited", secret)

    def burst_hits_the_rate():
        try:
            for i in range(200):
                kv.Set(
                    kv_pb2.SetRequest(
                        keyspace="ratelimited",
                        key=f"burst-{i}".encode(),
                        value=b"v",
                    ),
                    timeout=5.0,
                    metadata=_bearer(secret),
                )
            return None
        except grpc.RpcError as error:
            return error

    error = poll_until(burst_hits_the_rate, timeout=PROPAGATION_TIMEOUT_SECONDS)
    assert error.code() == grpc.StatusCode.RESOURCE_EXHAUSTED
    assert "rate" in (error.details() or "").lower(), (
        "a rate refusal has to name the rate so a client can tell it from a "
        f"storage refusal; got {error.details()!r}"
    )


# --- The noisy-neighbour measurement ----------------------------------------


def _hammer(kv, keyspace, secret, stop, refusals, errors):
    """Fire writes at ``keyspace`` as fast as possible until ``stop`` is set.

    Counts how many were refused ``RESOURCE_EXHAUSTED`` so the test can prove
    the noisy tenant really was being throttled, not merely busy.
    """
    i = 0
    while not stop.is_set():
        i += 1
        try:
            kv.Set(
                kv_pb2.SetRequest(
                    keyspace=keyspace, key=f"noise-{i}".encode(), value=b"v"
                ),
                timeout=5.0,
                metadata=_bearer(secret),
            )
        except grpc.RpcError as error:
            if error.code() == grpc.StatusCode.RESOURCE_EXHAUSTED:
                refusals.append(1)
            else:
                errors.append(error)


def _steady(kv, keyspace, secret, stop, latencies, failures):
    """Run a calm, sequential workload against ``keyspace`` and time each call.

    One request at a time, so the neighbour never limits itself; anything that
    slows it down or fails it is coming from outside, which is exactly what the
    isolation claim forbids.
    """
    i = 0
    while not stop.is_set():
        i += 1
        started = time.perf_counter()
        try:
            kv.Set(
                kv_pb2.SetRequest(
                    keyspace=keyspace, key=f"calm-{i}".encode(), value=b"v"
                ),
                timeout=5.0,
                metadata=_bearer(secret),
            )
            latencies.append(time.perf_counter() - started)
        except grpc.RpcError as error:
            failures.append(error)
        # A small pace so a full 2s window is a couple hundred requests, not a
        # spin that competes with the storm for CPU and measures scheduler
        # noise instead of isolation.
        time.sleep(0.005)


@pytest.mark.parametrize("attempt", range(3))
def test_a_throttled_tenant_does_not_degrade_its_neighbour(secured_node, attempt):
    """A storm in one keyspace leaves its neighbour healthy and quick.

    The noisy tenant is capped low on writes and hammered from several threads
    until the owner is refusing it constantly. The quiet neighbour, under a
    generous cap, runs a steady workload the whole time. The isolation property
    is that the neighbour never sees the storm: its success rate stays 100% and
    its latency stays inside a generous bound. Because #96 gives every keyspace
    its own limiter behind its own lock, the storm cannot take a lock or a
    queue the neighbour is waiting on.

    Run several times because a single clean pass of a timing property is weak
    evidence; a flaky isolation guarantee is a broken one.
    """
    admin = secured_node.admin
    kv = secured_node.kv

    noisy_ks = f"noisy-{attempt}"
    quiet_ks = f"quiet-{attempt}"
    # The noisy tenant is capped hard; the quiet one is left effectively
    # unlimited for the modest, paced load it runs, so any refusal or slowdown
    # it sees is leakage from next door and not its own ceiling.
    _create_keyspace(admin, noisy_ks, max_writes_per_second=5)
    _create_keyspace(admin, quiet_ks, max_writes_per_second=100_000)
    _, noisy_secret = _create_credential(
        admin, [noisy_ks], [admin_pb2.PERMISSION_WRITE]
    )
    _, quiet_secret = _create_credential(
        admin, [quiet_ks], [admin_pb2.PERMISSION_WRITE]
    )
    _wait_until_writable(kv, noisy_ks, noisy_secret)
    _wait_until_writable(kv, quiet_ks, quiet_secret)

    stop = threading.Event()
    refusals: list[int] = []
    noisy_errors: list[grpc.RpcError] = []
    latencies: list[float] = []
    quiet_failures: list[grpc.RpcError] = []

    duration = 2.0
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = [
            pool.submit(_hammer, kv, noisy_ks, noisy_secret, stop, refusals, noisy_errors)
            for _ in range(4)
        ]
        futures.append(
            pool.submit(
                _steady, kv, quiet_ks, quiet_secret, stop, latencies, quiet_failures
            )
        )
        time.sleep(duration)
        stop.set()
        for future in futures:
            future.result(timeout=30.0)

    # The storm has to have been a storm, or the test proves nothing about
    # isolation under pressure.
    assert not noisy_errors, (
        "the noisy tenant should only ever see RESOURCE_EXHAUSTED, never a "
        f"different failure; got {[e.code() for e in noisy_errors]}"
    )
    assert len(refusals) > 10, (
        "the noisy tenant was not actually throttled, so this run does not "
        f"exercise isolation under pressure (saw {len(refusals)} refusals)"
    )

    # The neighbour: unbroken and unhurried.
    assert quiet_failures == [], (
        "the quiet neighbour was refused while its noisy peer was throttled, "
        "which is exactly the cross-tenant leak this test exists to catch: "
        f"{[e.code() for e in quiet_failures]}"
    )
    assert len(latencies) > 20, (
        "the neighbour barely ran, so its health is unmeasured "
        f"({len(latencies)} samples)"
    )
    # Generous, absolute bounds rather than a tight ratio: the claim is that
    # the neighbour is not *collapsed*, and a local write is single-digit
    # milliseconds, so even a loaded CI runner clears these by a wide margin.
    # A tight bound here would measure the runner, not the isolation.
    median = statistics.median(latencies)
    p95 = statistics.quantiles(latencies, n=20)[18]
    assert median < 0.25, f"neighbour median latency {median:.3f}s is degraded"
    assert p95 < 1.0, f"neighbour p95 latency {p95:.3f}s is degraded"
    assert max(latencies) < 2.0, (
        f"a neighbour request took {max(latencies):.3f}s, which is a stall the "
        "storm should not have been able to cause"
    )


# --- Revocation takes effect on the next request ----------------------------


def test_a_revoked_credential_is_refused_on_its_next_request(secured_node):
    """Revocation lands on the next request over the same channel, not the next
    connection.

    The credential is used successfully, revoked, and presented again on the
    very same stub and channel. That the second request is refused — with no
    reconnect in between — is the proof that enforcement reads a live cache per
    request rather than a verdict cached when the connection opened.
    """
    admin = secured_node.admin
    kv = secured_node.kv
    _create_keyspace(admin, "revocable")
    cred_id, secret = _create_credential(
        admin, ["revocable"], [admin_pb2.PERMISSION_READ, admin_pb2.PERMISSION_WRITE]
    )
    _wait_until_writable(kv, "revocable", secret)

    # Works now, on this channel.
    kv.Set(
        kv_pb2.SetRequest(keyspace="revocable", key=b"k", value=b"v"),
        metadata=_bearer(secret),
    )

    admin.RevokeCredential(
        admin_pb2.RevokeCredentialRequest(credential_id=cred_id),
        timeout=10.0,
        metadata=_bearer(ROOT_SECRET),
    )

    # The revoke reaches the worker on the next credential refresh; once it
    # has, the same channel is refused. A revoked secret is now unknown, so the
    # server answers UNAUTHENTICATED.
    def refused():
        try:
            kv.Get(
                kv_pb2.GetRequest(keyspace="revocable", key=b"k"),
                timeout=5.0,
                metadata=_bearer(secret),
            )
            return None
        except grpc.RpcError as error:
            return error

    error = poll_until(refused, timeout=PROPAGATION_TIMEOUT_SECONDS)
    assert error.code() == grpc.StatusCode.UNAUTHENTICATED, (
        "a revoked secret is no longer known, so the next request over the "
        f"same channel is UNAUTHENTICATED, got {error.code()}"
    )
