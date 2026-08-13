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


def test_a_list_page_stays_under_the_reported_message_size_and_pages_the_rest(kv):
    # A LIST the store assembles has to fit the channel the store advertises.
    # A caller can ask for a thousand entries; with maximum values that is a
    # multi-gigabyte response, and even a handful of them overruns the ceiling.
    # The page has to be bounded by the bytes it will occupy, not just the count
    # asked for, and carry a cursor for the remainder. Without that bound the
    # transport refuses the oversized page and the caller cannot read its data.
    limits = kv.GetLimits(pb.GetLimitsRequest(keyspace=KS))
    ceiling = limits.max_message_bytes
    value = b"v" * limits.max_value_bytes

    # Enough maximum values that returning them all at once would overrun the
    # ceiling several times over, and a caller who asks for far more than that.
    count = 40
    for i in range(count):
        written = kv.Set(
            pb.SetRequest(keyspace=KS, key=f"big/{i:04}".encode(), value=value)
        )
        assert written.applied

    seen = []
    cursor = b""
    pages = 0
    while True:
        page = kv.List(
            pb.ListRequest(
                keyspace=KS,
                prefix=b"big/",
                limit=limits.max_list_entries,
                cursor=cursor,
                include_values=True,
            )
        )
        pages += 1
        assert pages < 100, "pagination is not terminating"
        assert page.ByteSize() <= ceiling, (
            f"a page ({page.ByteSize()} bytes) must fit under the advertised "
            f"ceiling ({ceiling} bytes)"
        )
        seen.extend(entry.key for entry in page.entries)
        cursor = page.next_cursor
        if not cursor:
            break

    assert pages > 1, "the byte bound must have forced more than one page"
    assert seen == [f"big/{i:04}".encode() for i in range(count)], (
        "every key comes back once, in order, across the pages"
    )


def test_a_request_at_the_reported_message_size_reaches_the_handler(kv):
    # The published max_message_bytes is a promise the transport has to keep: a
    # client that sizes its channel to it (the harness does) and sends exactly
    # that many bytes must reach the handler rather than be cut off first. The
    # conftest channel is built to this very number, so this is the assertion
    # that fails when the server advertises a ceiling above what its own
    # transport will accept -- the request never arrives and gRPC reports a
    # message-size error instead of the handler's own answer.
    #
    # The value runs far past the value limit on purpose. That makes the
    # handler refuse the request for size, and that INVALID_ARGUMENT -- rather
    # than a transport error -- is what proves the whole message crossed.
    limits = kv.GetLimits(pb.GetLimitsRequest(keyspace=KS))
    target = limits.max_message_bytes
    key = b"at-the-message-limit"

    # Grow the value until the whole request encodes to exactly the ceiling.
    # One step is not always enough because the value's length varint can widen
    # as it grows, so close the remaining gap until it lands.
    request = pb.SetRequest(keyspace=KS, key=key, value=b"x" * target)
    while request.ByteSize() != target:
        value_len = len(request.value) + (target - request.ByteSize())
        request = pb.SetRequest(keyspace=KS, key=key, value=b"x" * value_len)
    assert request.ByteSize() == target

    with pytest.raises(grpc.RpcError) as caught:
        kv.Set(request)

    assert caught.value.code() == grpc.StatusCode.INVALID_ARGUMENT
