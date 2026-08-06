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
* Revocation takes effect on the next request against the same connection, not
  the next connection: the worker refreshes its credential cache on a timer
  (the control poll interval, 250ms by default) and reads the fresh set on the
  very next call.

# TLS posture (the recorded deployment contract)

The server speaks **plaintext gRPC**. It does not terminate TLS itself, by
design: a node's own listener is h2c, and transport security is expected to be
supplied by a proxy or service mesh in front of it (the peer listener is meant
for a private network for the same reason; see `config.rs`). That decision is
made visible rather than ambient by
`test_the_server_speaks_plaintext_grpc_and_terminates_no_tls`, which proves an
insecure channel is served and a TLS channel is refused. If a future change
makes the node terminate TLS, that test fails and this contract gets revisited
on purpose.
"""

from __future__ import annotations

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


# --- Revocation takes effect on the next request, not the next connection ------


def test_a_revoked_credential_is_refused_on_the_next_request(auth_node):
    """A revoked credential stops working on the same open channel.

    The credential works, is revoked through Admin as root, and then fails on
    the *same* stub and channel — no reconnect. That is what distinguishes
    next-request enforcement (the worker re-reads a refreshed credential set)
    from next-connection enforcement (a per-connection verdict that a revoke
    could not reach). A revoked secret names no credential, so the refusal is
    UNAUTHENTICATED.
    """
    credential_id, secret = _create_credential(
        auth_node,
        keyspaces=[KS],
        permissions=[admin_pb2.PERMISSION_READ, admin_pb2.PERMISSION_WRITE],
    )

    def usable():
        try:
            auth_node.kv.Get(
                kv_pb2.GetRequest(keyspace=KS, key=b"k"),
                timeout=5.0,
                metadata=bearer(secret),
            )
            return True
        except grpc.RpcError:
            return None

    assert poll_until(usable), "the credential has to work before it is revoked"

    auth_node.admin.RevokeCredential(
        admin_pb2.RevokeCredentialRequest(credential_id=credential_id),
        timeout=10.0,
        metadata=ROOT_METADATA,
    )

    # The worker refreshes its credential cache on the control-poll timer, so
    # the refusal lands within a couple of intervals. Crucially this is the same
    # channel that just succeeded: no new connection is opened between the two.
    def refused():
        try:
            auth_node.kv.Get(
                kv_pb2.GetRequest(keyspace=KS, key=b"k"),
                timeout=5.0,
                metadata=bearer(secret),
            )
            return None
        except grpc.RpcError as error:
            return error.code()

    code = poll_until(refused)
    assert code == grpc.StatusCode.UNAUTHENTICATED, (
        "a revoked credential names no credential, so the next request is "
        "UNAUTHENTICATED"
    )


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


def test_the_server_speaks_plaintext_grpc_and_terminates_no_tls(node):
    """The node serves h2c and refuses a TLS handshake.

    This records the deployment contract in an executable form: transport
    security is a proxy's job, not the node's. The plaintext channel every
    other test uses is served; a TLS channel against the same port cannot
    complete a handshake, because there is no server certificate to complete it
    with. See this module's docstring for why that is by design.
    """
    # Plaintext is served: the running node answered its probe over exactly this
    # kind of channel, and does so again here.
    served = node.kv.GetLimits(kv_pb2.GetLimitsRequest(keyspace=KS), timeout=10.0)
    assert served.max_message_bytes > 0

    # TLS is not: a secure channel to a plaintext listener cannot handshake, so
    # the call fails rather than returning. If the node ever starts terminating
    # TLS, this stops raising and the contract is revisited on purpose.
    tls_channel = grpc.secure_channel(
        f"127.0.0.1:{node.port}", grpc.ssl_channel_credentials()
    )
    try:
        tls_stub = kv_pb2_grpc.KvStub(tls_channel)
        with pytest.raises(grpc.RpcError) as caught:
            tls_stub.GetLimits(kv_pb2.GetLimitsRequest(keyspace=KS), timeout=10.0)
        assert caught.value.code() == grpc.StatusCode.UNAVAILABLE, (
            "a plaintext server cannot complete a TLS handshake, so the client "
            "sees the connection fail"
        )
    finally:
        tls_channel.close()
