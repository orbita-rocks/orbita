"""Credential enforcement, over a real socket, against the real binary.

The only credential test that existed before this file ran the CLI against a
canned gRPC stand-in. The stand-in stood in for exactly the boundary that does
the enforcing, so the one thing left unproven end to end was that a real node,
started with authentication *required*, refuses an unauthenticated, a
wrong-credential, and a revoked-credential client at the wire with the status
codes the contract pins. This file starts that node and drives it as an
ordinary gRPC client would.

The rules under test, read off the merged enforcement code
(`crates/orbita-server/src/auth.rs`, `crates/orbita-control/src/auth.rs`):

* The header is `authorization: Bearer <secret>`.
* Authentication is turned on with `ORBITA_REQUIRE_AUTH=true`, and a bootstrap
  root identity is named with `ORBITA_ROOT_CREDENTIAL=<secret>`. Both are read
  through the same config layer every `orbita` command uses, so `orbita dev`
  honours them with no extra flag.
* A missing, malformed, or unknown secret is `UNAUTHENTICATED`.
* A known secret used outside its keyspace, without the permission, or expired
  is `PERMISSION_DENIED`.
* Admin is root-only: a tenant credential — even a writer — is
  `PERMISSION_DENIED` on any Admin RPC. Only the configured root passes.
* Revocation is bounded-eventual, not literally next-request: the worker
  enforces against a credential cache it refreshes on the control-poll timer
  (250ms by default), so a revoke lands on the next refresh — within about one
  interval — rather than on the next call. The test states that bound and fails
  if a revoke never lands or drifts past it, and it checks the refusal on the
  same open connection so it is the cache being re-read, not a per-connection
  verdict.

# TLS posture (the recorded deployment contract)

The server speaks **plaintext gRPC**. It does not terminate TLS itself, by
design: a node's own listener is h2c, and transport security is expected to be
supplied by a proxy or service mesh in front of it (the peer listener is meant
for a private network for the same reason; see `config.rs`). That decision is
made visible rather than ambient by
`test_the_server_speaks_plaintext_grpc_and_terminates_no_tls`, which proves an
insecure channel is served and — independently of certificate trust — that the
port completes no TLS handshake. The check disables certificate verification on
purpose, so it distinguishes a plaintext server from a TLS one rather than a
trusted certificate from an untrusted one: a TLS server with a self-signed or
private-CA certificate would complete the handshake and flip the test red. If a
future change makes the node terminate TLS on this port, that test fails and
this contract gets revisited on purpose.
"""

from __future__ import annotations

import socket
import ssl
import time
from types import SimpleNamespace

import grpc
import pytest
from orbita.v1 import admin_pb2, admin_pb2_grpc, kv_pb2, kv_pb2_grpc

import harness
from conftest import DEFAULT_KEYSPACE as KS
from conftest import poll_until

# The bootstrap identity the auth-enabled node is configured with. It is the
# only credential that exists before any is minted through Admin, so every test
# that needs to create or revoke a credential authenticates as this.
ROOT_SECRET = "root-bootstrap-secret-for-the-e2e-suite"

# The worker refreshes its credential cache off the control-poll timer, which is
# DEFAULT_CONTROL_POLL_INTERVAL in crates/orbita-server/src/config.rs — 250ms.
# Enforcement, including revocation, is therefore bounded-eventual: a change to
# the credential set lands on the next refresh, within about one interval.
CREDENTIAL_REFRESH_INTERVAL_SECONDS = 0.25

# The window a revocation must land within, stated rather than left open. A
# generous multiple of the refresh interval so a loaded CI runner does not flake,
# but bounded so the test FAILS if a revoke never takes effect or drifts far past
# one interval — the honest contract is "within about one refresh", not "never".
REVOCATION_BOUND_SECONDS = 20 * CREDENTIAL_REFRESH_INTERVAL_SECONDS

# The stub modules the harness Node binds, in the shape it expects. conftest has
# already generated the stubs and put them on the path by the time this imports.
_STUBS = SimpleNamespace(kv=kv_pb2_grpc, admin=admin_pb2_grpc)


def _probe() -> kv_pb2.GetLimitsRequest:
    """Readiness probe and channel-sizing request.

    GetLimits is deliberately unauthenticated on the server, so it doubles as
    the startup probe even when auth is required: it proves the node answers and
    that the default keyspace exists without needing a credential yet.
    """
    return kv_pb2.GetLimitsRequest(keyspace=KS)


def bearer(secret: str) -> list[tuple[str, str]]:
    """The gRPC metadata a client presenting `secret` sends."""
    return [("authorization", f"Bearer {secret}")]


ROOT_METADATA = bearer(ROOT_SECRET)


@pytest.fixture
def auth_node(orbita_binary, tmp_path):
    """A single node started with authentication REQUIRED and a root credential.

    This is the whole point of the file: a real node whose data surface and
    admin surface both enforce credentials, reached over an ordinary insecure
    gRPC channel.
    """
    node = harness.Node(
        binary=orbita_binary,
        data_dir=tmp_path / "data",
        pb2_grpc=_STUBS,
        log_path=tmp_path / "orbita.log",
        env={
            "ORBITA_REQUIRE_AUTH": "true",
            "ORBITA_ROOT_CREDENTIAL": ROOT_SECRET,
        },
    )
    try:
        node.start(_probe)
        yield node
    finally:
        node.stop()


def _create_credential(node, *, keyspaces, permissions, description="e2e") -> str:
    """Mint a credential as root and return its one-time secret."""
    created = node.admin.CreateCredential(
        admin_pb2.CreateCredentialRequest(
            keyspaces=keyspaces,
            permissions=permissions,
            description=description,
        ),
        timeout=10.0,
        metadata=ROOT_METADATA,
    )
    assert created.secret, "a created credential returns its secret once"
    return created.credential_id, created.secret


def _code_of(call) -> grpc.StatusCode:
    with pytest.raises(grpc.RpcError) as caught:
        call()
    return caught.value.code()


# --- Unauthenticated: no credential at all -------------------------------------


def test_an_unauthenticated_kv_request_is_unauthenticated_at_the_wire(auth_node):
    """A data-plane call with no bearer token is refused UNAUTHENTICATED.

    This is the exact boundary the old CLI-against-a-stand-in test could only
    approximate: a real node, auth on, an ordinary Get, no header.
    """
    code = _code_of(
        lambda: auth_node.kv.Get(kv_pb2.GetRequest(keyspace=KS, key=b"k"), timeout=10.0)
    )
    assert code == grpc.StatusCode.UNAUTHENTICATED


def test_an_unauthenticated_admin_request_is_unauthenticated_at_the_wire(auth_node):
    """The admin surface refuses a call with no bearer token, UNAUTHENTICATED."""
    code = _code_of(
        lambda: auth_node.admin.ListKeyspaces(
            admin_pb2.ListKeyspacesRequest(), timeout=10.0
        )
    )
    assert code == grpc.StatusCode.UNAUTHENTICATED


# --- Unauthenticated: a secret that does not resolve ---------------------------


def test_an_unknown_secret_is_unauthenticated_on_kv_and_admin(auth_node):
    """A well-formed bearer token that names no credential is UNAUTHENTICATED.

    "You sent no credential" and "you sent a bad one" are the same failure to a
    caller — both mean authenticate and retry — so an unknown secret is
    UNAUTHENTICATED rather than PERMISSION_DENIED.
    """
    unknown = bearer("this-secret-was-never-issued")
    kv_code = _code_of(
        lambda: auth_node.kv.Get(
            kv_pb2.GetRequest(keyspace=KS, key=b"k"), timeout=10.0, metadata=unknown
        )
    )
    assert kv_code == grpc.StatusCode.UNAUTHENTICATED

    admin_code = _code_of(
        lambda: auth_node.admin.ListKeyspaces(
            admin_pb2.ListKeyspacesRequest(), timeout=10.0, metadata=unknown
        )
    )
    assert admin_code == grpc.StatusCode.UNAUTHENTICATED


def test_a_malformed_authorization_header_is_unauthenticated(auth_node):
    """A header without the `Bearer ` scheme carries no usable secret.

    The server strips exactly `Bearer <secret>`; anything else reads as no
    credential at all, which on an auth-on cluster is UNAUTHENTICATED.
    """
    malformed = [("authorization", "not-a-bearer-token")]
    code = _code_of(
        lambda: auth_node.kv.Get(
            kv_pb2.GetRequest(keyspace=KS, key=b"k"), timeout=10.0, metadata=malformed
        )
    )
    assert code == grpc.StatusCode.UNAUTHENTICATED


# --- PermissionDenied: a known secret out of scope -----------------------------


def test_a_credential_used_on_the_wrong_keyspace_is_permission_denied(auth_node):
    """A keyspace-scoped credential is denied on a keyspace it does not name.

    The credential is real and known, so this is not an authentication failure;
    it is a scope failure, which the contract pins to PERMISSION_DENIED.
    """
    # A second keyspace the credential will deliberately not be scoped to.
    auth_node.admin.CreateKeyspace(
        admin_pb2.CreateKeyspaceRequest(name="vault"),
        timeout=10.0,
        metadata=ROOT_METADATA,
    )
    _, secret = _create_credential(
        auth_node,
        keyspaces=[KS],
        permissions=[admin_pb2.PERMISSION_READ, admin_pb2.PERMISSION_WRITE],
    )

    # It works on its own keyspace once the worker's cache has caught up, which
    # is what proves the credential is genuinely valid and the refusal below is
    # about scope rather than a cache miss.
    def usable_on_its_keyspace():
        try:
            auth_node.kv.Get(
                kv_pb2.GetRequest(keyspace=KS, key=b"k"),
                timeout=5.0,
                metadata=bearer(secret),
            )
            return True
        except grpc.RpcError:
            return None

    assert poll_until(usable_on_its_keyspace)

    code = _code_of(
        lambda: auth_node.kv.Get(
            kv_pb2.GetRequest(keyspace="vault", key=b"k"),
            timeout=10.0,
            metadata=bearer(secret),
        )
    )
    assert code == grpc.StatusCode.PERMISSION_DENIED


def test_a_credential_lacking_the_permission_is_permission_denied(auth_node):
    """A read-only credential is denied a write on its own keyspace.

    Same keyspace, right identity, wrong permission — PERMISSION_DENIED.
    """
    _, secret = _create_credential(
        auth_node, keyspaces=[KS], permissions=[admin_pb2.PERMISSION_READ]
    )

    # The read it *is* allowed proves the credential reached the worker, so the
    # write refusal below is about the missing permission, not a stale cache.
    def read_allowed():
        try:
            auth_node.kv.Get(
                kv_pb2.GetRequest(keyspace=KS, key=b"k"),
                timeout=5.0,
                metadata=bearer(secret),
            )
            return True
        except grpc.RpcError:
            return None

    assert poll_until(read_allowed)

    code = _code_of(
        lambda: auth_node.kv.Set(
            kv_pb2.SetRequest(keyspace=KS, key=b"k", value=b"v"),
            timeout=10.0,
            metadata=bearer(secret),
        )
    )
    assert code == grpc.StatusCode.PERMISSION_DENIED


# --- The positive control ------------------------------------------------------


def test_a_valid_credential_on_its_keyspace_reads_and_writes(auth_node):
    """A credential with the right keyspace and permission is served.

    Without this, every refusal above could be a node that refuses everything,
    and the suite would prove nothing about enforcement.
    """
    _, secret = _create_credential(
        auth_node,
        keyspaces=[KS],
        permissions=[admin_pb2.PERMISSION_READ, admin_pb2.PERMISSION_WRITE],
    )

    def written():
        try:
            return auth_node.kv.Set(
                kv_pb2.SetRequest(keyspace=KS, key=b"auth", value=b"granted"),
                timeout=5.0,
                metadata=bearer(secret),
            )
        except grpc.RpcError:
            return None

    assert poll_until(written), "a valid credential has to be able to write"
    got = auth_node.kv.Get(
        kv_pb2.GetRequest(keyspace=KS, key=b"auth"),
        timeout=10.0,
        metadata=bearer(secret),
    )
    assert got.found and got.value == b"granted"


# --- Revocation is bounded-eventual: it lands within about one refresh --------


def test_a_revoked_credential_stops_working_within_one_refresh_interval(auth_node):
    """A revoked credential is refused within a bounded window, on the same channel.

    The honest contract is bounded-eventual, not literally next-request: the
    worker enforces against a cached credential set it refreshes off the
    control-poll timer (~250ms), so a revoke lands on the next refresh rather
    than on the next call. This test states that bound
    (`REVOCATION_BOUND_SECONDS`) and fails if the revoke never takes effect or
    takes longer than it — an unbounded poll would instead pass no matter how
    long revocation drifted.

    What it still proves about the mechanism: the refusal arrives on the *same*
    stub and channel that just succeeded — no reconnect — so this is the cache
    being re-read, not a fresh per-connection verdict. A revoked secret names no
    credential, so the refusal is UNAUTHENTICATED, and it stays refused.
    """
    credential_id, secret = _create_credential(
        auth_node,
        keyspaces=[KS],
        permissions=[admin_pb2.PERMISSION_READ, admin_pb2.PERMISSION_WRITE],
    )

    def get_with_credential():
        return auth_node.kv.Get(
            kv_pb2.GetRequest(keyspace=KS, key=b"k"),
            timeout=5.0,
            metadata=bearer(secret),
        )

    def usable():
        try:
            get_with_credential()
            return True
        except grpc.RpcError:
            return None

    assert poll_until(usable), "the credential has to work before it is revoked"

    auth_node.admin.RevokeCredential(
        admin_pb2.RevokeCredentialRequest(credential_id=credential_id),
        timeout=10.0,
        metadata=ROOT_METADATA,
    )

    # Poll against a stated deadline rather than an open-ended one: the revoke
    # must land within REVOCATION_BOUND_SECONDS or this fails. Same channel
    # throughout, so a success here would be a per-connection verdict a revoke
    # could not reach; a bounded refusal is the cache having been re-read.
    deadline = time.monotonic() + REVOCATION_BOUND_SECONDS
    refused_code = None
    while time.monotonic() < deadline:
        try:
            get_with_credential()
        except grpc.RpcError as error:
            refused_code = error.code()
            break
        time.sleep(0.02)

    assert refused_code == grpc.StatusCode.UNAUTHENTICATED, (
        "the revoked credential was still accepted "
        f"{REVOCATION_BOUND_SECONDS} seconds after revocation; revocation must "
        "land within about one refresh interval, and a revoked secret names no "
        "credential so the refusal is UNAUTHENTICATED"
    )

    # And it stays refused: revocation is not a one-shot blip that a later
    # refresh could undo. A handful of follow-up calls all fail the same way.
    for _ in range(5):
        with pytest.raises(grpc.RpcError) as caught:
            get_with_credential()
        assert caught.value.code() == grpc.StatusCode.UNAUTHENTICATED


# --- Admin is root-only --------------------------------------------------------


def test_admin_is_root_only_a_tenant_writer_is_denied_but_root_succeeds(auth_node):
    """A tenant write credential cannot administer the cluster.

    Writing to a keyspace does not confer the power to delete keyspaces, mint
    credentials, or revoke others. A tenant credential — even a writer — is
    PERMISSION_DENIED on an Admin RPC; only the configured root passes. This is
    the boundary that used to leak: any single-keyspace writer was once wrongly
    promoted to cluster admin.
    """
    _, tenant_secret = _create_credential(
        auth_node,
        keyspaces=[KS],
        permissions=[admin_pb2.PERMISSION_READ, admin_pb2.PERMISSION_WRITE],
    )

    def tenant_ready():
        try:
            auth_node.kv.Get(
                kv_pb2.GetRequest(keyspace=KS, key=b"k"),
                timeout=5.0,
                metadata=bearer(tenant_secret),
            )
            return True
        except grpc.RpcError:
            return None

    assert poll_until(tenant_ready), "the tenant credential has to be live first"

    denied = _code_of(
        lambda: auth_node.admin.ListKeyspaces(
            admin_pb2.ListKeyspacesRequest(),
            timeout=10.0,
            metadata=bearer(tenant_secret),
        )
    )
    assert denied == grpc.StatusCode.PERMISSION_DENIED

    # Root is the one identity that administers the cluster.
    listed = auth_node.admin.ListKeyspaces(
        admin_pb2.ListKeyspacesRequest(), timeout=10.0, metadata=ROOT_METADATA
    )
    assert KS in [k.name for k in listed.keyspaces]


# --- TLS posture: plaintext-plus-proxy is the recorded contract ----------------


def _completes_a_tls_handshake(host: str, port: int, timeout: float = 10.0) -> bool:
    """Whether the server completes a TLS handshake on this port.

    Certificate trust is deliberately turned OFF: the context does not verify
    the hostname and accepts any certificate. That is what makes this a test of
    the *protocol* rather than of trust — a TLS server presenting a self-signed
    cert, a private-CA cert, or a cert not valid for 127.0.0.1 would all still
    complete the handshake here and return True. Only a server that does not
    speak TLS at all — one sending plaintext HTTP/2 bytes where a ServerHello is
    expected — makes the handshake fail, which surfaces as an ``ssl.SSLError``
    (typically "wrong version number"). So a False return means "this port does
    not terminate TLS", provably, without depending on any certificate being
    trusted.
    """
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    try:
        with socket.create_connection((host, port), timeout=timeout) as raw:
            raw.settimeout(timeout)
            with context.wrap_socket(raw, server_hostname=host):
                # The handshake completed. A TLS server got here regardless of
                # whether its certificate would be trusted.
                return True
    except ssl.SSLError:
        # Bytes that are not a TLS record: the server is not speaking TLS.
        return False
    except (OSError, socket.timeout):
        # A refused or silent connection is not a completed TLS handshake.
        return False


def test_the_server_speaks_plaintext_grpc_and_terminates_no_tls(node):
    """The node serves h2c and completes no TLS handshake on its client port.

    This records the deployment contract in an executable form: transport
    security is a proxy's job, not the node's. The check is independent of
    certificate trust (see `_completes_a_tls_handshake`), so it cannot be
    satisfied by a TLS server with a self-signed or private-CA certificate —
    that server would complete the handshake and flip this test red. If the node
    ever starts terminating TLS on this port, this fails and the contract is
    revisited on purpose. See this module's docstring for why plaintext is the
    design today.
    """
    # Plaintext is served: the running node answered its probe over exactly this
    # kind of insecure channel, and does so again here.
    served = node.kv.GetLimits(kv_pb2.GetLimitsRequest(keyspace=KS), timeout=10.0)
    assert served.max_message_bytes > 0

    # TLS is not terminated: a handshake with verification disabled still fails,
    # which can only mean the server is not speaking TLS at all. A self-signed or
    # private-CA TLS server would instead complete the handshake and fail this.
    assert not _completes_a_tls_handshake("127.0.0.1", node.port), (
        "the node completed a TLS handshake on its client port; the recorded "
        "contract is plaintext h2c with TLS terminated by a proxy in front"
    )
