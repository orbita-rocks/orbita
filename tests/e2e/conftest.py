"""Fixtures for the end-to-end suite.

Importing this file regenerates the gRPC stubs and puts them on the path, so
test modules can `from orbita.v1 import kv_pb2` exactly as an ordinary user of
the published protos would.
"""

from __future__ import annotations

import sys
import time
from types import SimpleNamespace

import pytest

import harness

harness.generate_stubs()
sys.path.insert(0, str(harness.GENERATED_DIR))

from orbita.v1 import admin_pb2_grpc, kv_pb2, kv_pb2_grpc  # noqa: E402

DEFAULT_KEYSPACE = harness.DEFAULT_KEYSPACE

_STUB_MODULES = SimpleNamespace(kv=kv_pb2_grpc, admin=admin_pb2_grpc)


def _probe_request() -> kv_pb2.GetLimitsRequest:
    """The request used to decide a node is up, and to size the connection.

    It names the default keyspace, because the keyspace is created during
    startup and a node that has not created it yet answers NOT_FOUND rather
    than succeeding. So one call proves the node is answering, proves the
    keyspace exists, and returns the limits the real channel is built from.
    """
    return kv_pb2.GetLimitsRequest(keyspace=DEFAULT_KEYSPACE)


@pytest.fixture(scope="session")
def orbita_binary():
    return harness.build_binary()


@pytest.fixture
def node(orbita_binary, tmp_path):
    """A running single node, on its own port, over its own data directory."""
    node = harness.Node(
        binary=orbita_binary,
        data_dir=tmp_path / "data",
        pb2_grpc=_STUB_MODULES,
        log_path=tmp_path / "orbita.log",
    )
    node.probe = _probe_request
    try:
        node.start(_probe_request)
        yield node
    finally:
        node.stop()


@pytest.fixture
def kv(node):
    """The Kv stub for the running node, which is what most tests want."""
    return node.kv


@pytest.fixture
def limits(node):
    """What this cluster says it accepts.

    Tests take sizes from here rather than hardcoding them, so a test asserts
    that the server agrees with itself rather than that it agrees with a number
    somebody copied out of the Rust source.
    """
    return node.limits


def restart(node) -> None:
    """Stop a node and start it again over the same data directory."""
    node.restart(_probe_request)


def poll_until(predicate, timeout=10.0, interval=0.02):
    """Wait for a condition instead of sleeping a guessed amount of time.

    Returns the truthy value the predicate produced, or raises. Anything
    timing-dependent in this suite goes through here so that a slow machine
    makes the suite slower rather than red.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(interval)
    raise AssertionError(f"condition never became true within {timeout} seconds")
