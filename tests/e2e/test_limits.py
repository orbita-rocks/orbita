"""The limits a client needs before it sends anything.

This is the discovery call, so it matters more from Python than from Rust: a
client in another language has only the generated stub and whatever the server
tells it. A limit that lives in Rust source is a limit this client can learn
only by exceeding it and reading the error, which is a poor way to find out.

The field that earns this RPC is max_message_bytes. gRPC implementations
default to a 4MB message limit, so a keyspace configured above that fails
inside the client's own gRPC stack with an error about message size and nothing
about Orbita. This is how a client knows what to configure.
"""

import grpc
import pytest
from orbita.v1 import kv_pb2 as pb

from conftest import DEFAULT_KEYSPACE as KS


def test_limits_are_reported_without_naming_a_keyspace(kv):
    limits = kv.GetLimits(pb.GetLimitsRequest())

    assert limits.max_key_bytes > 0
    assert limits.max_value_bytes > 0
    assert limits.max_list_entries > 0
    assert limits.max_list_bytes > 0


def test_the_reported_message_size_covers_a_maximum_value(kv):
    # A client that sized its channel to the value limit alone would reject a
    # maximum value once the key, the keyspace name, and framing are added.
    limits = kv.GetLimits(pb.GetLimitsRequest())

    assert limits.max_message_bytes > limits.max_value_bytes
    assert limits.max_message_bytes >= limits.max_list_bytes


def test_limits_can_be_asked_for_one_keyspace(kv):
    # Value size is configured per keyspace, so a client talking to one
    # keyspace should be able to ask about that one rather than reasoning about
    # the cluster maximum.
    limits = kv.GetLimits(pb.GetLimitsRequest(keyspace=KS))

    assert limits.max_key_bytes > 0
    assert limits.max_value_bytes > 0


def test_an_unknown_keyspace_is_an_error_rather_than_a_default(kv):
    # Reporting cluster defaults for a keyspace that does not exist would let a
    # client size itself against a limit that does not apply to it, and find
    # out later.
    with pytest.raises(grpc.RpcError) as caught:
        kv.GetLimits(pb.GetLimitsRequest(keyspace="nosuchkeyspace"))

    assert caught.value.code() == grpc.StatusCode.NOT_FOUND


def test_a_value_at_the_reported_limit_is_accepted(kv):
    # The point of publishing a limit is that writing exactly it works. If this
    # fails, the number is wrong rather than the write being too big.
    limits = kv.GetLimits(pb.GetLimitsRequest(keyspace=KS))
    value = b"x" * limits.max_value_bytes

    written = kv.Set(
        pb.SetRequest(keyspace=KS, key=b"at-the-limit", value=value)
    )
    assert written.applied

    read = kv.Get(pb.GetRequest(keyspace=KS, key=b"at-the-limit"))
    assert read.found
    assert len(read.value) == limits.max_value_bytes


def test_a_value_one_byte_over_the_reported_limit_is_refused(kv):
    limits = kv.GetLimits(pb.GetLimitsRequest(keyspace=KS))
    value = b"x" * (limits.max_value_bytes + 1)

    with pytest.raises(grpc.RpcError) as caught:
        kv.Set(pb.SetRequest(keyspace=KS, key=b"over-the-limit", value=value))

    assert caught.value.code() == grpc.StatusCode.INVALID_ARGUMENT
