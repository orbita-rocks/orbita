"""The gRPC status codes, which are the whole error contract for a client that
has no library.

A caller in another language sees a code, a message string, and nothing else.
So the split between "this is a status" and "this is a field on a successful
response" is part of the API, and changing it silently would break callers.
"""

import grpc
import pytest
from orbita.v1 import kv_pb2

from conftest import DEFAULT_KEYSPACE as KS

# These are not in the protos. They come from the requirements document and
# from constants inside the server, so a client has to be told them out of
# band. That is a finding, recorded in the README.
MAX_KEY_BYTES = 10 * 1024
MAX_VALUE_BYTES = 256 * 1024


def code_of(call):
    with pytest.raises(grpc.RpcError) as caught:
        call()
    return caught.value.code(), caught.value.details()


def test_a_missing_key_is_a_successful_call_not_a_not_found_status(kv):
    # NOT_FOUND is spent on the keyspace, so a missing key cannot use it. A
    # client has to branch on the `found` field instead.
    response = kv.Get(kv_pb2.GetRequest(keyspace=KS, key=b"absent"))
    assert not response.found


@pytest.mark.parametrize(
    "call_name",
    ["Get", "Set", "Delete", "List"],
)
def test_an_unknown_keyspace_is_not_found_on_every_rpc(kv, call_name):
    requests = {
        "Get": kv_pb2.GetRequest(keyspace="nobody-made-this", key=b"k"),
        "Set": kv_pb2.SetRequest(keyspace="nobody-made-this", key=b"k", value=b"v"),
        "Delete": kv_pb2.DeleteRequest(keyspace="nobody-made-this", key=b"k"),
        "List": kv_pb2.ListRequest(keyspace="nobody-made-this", prefix=b"", limit=10),
    }
    code, details = code_of(lambda: getattr(kv, call_name)(requests[call_name]))
    assert code == grpc.StatusCode.NOT_FOUND
    assert "keyspace" in details, "the message has to say which of the two things is missing"


def test_a_keyspace_name_the_server_will_not_accept_is_invalid_argument(kv):
    code, _ = code_of(lambda: kv.Get(kv_pb2.GetRequest(keyspace="", key=b"k")))
    assert code == grpc.StatusCode.INVALID_ARGUMENT

    code, _ = code_of(lambda: kv.Get(kv_pb2.GetRequest(keyspace="../escape", key=b"k")))
    assert code == grpc.StatusCode.INVALID_ARGUMENT


def test_an_oversized_key_is_invalid_argument(kv):
    code, details = code_of(
        lambda: kv.Set(
            kv_pb2.SetRequest(keyspace=KS, key=b"k" * (MAX_KEY_BYTES + 1), value=b"v")
        )
    )
    assert code == grpc.StatusCode.INVALID_ARGUMENT
    assert str(MAX_KEY_BYTES) in details, (
        "the limit is not in the protos, so the message is the only place a "
        "client can learn it"
    )


def test_an_oversized_value_is_invalid_argument(kv):
    code, details = code_of(
        lambda: kv.Set(
            kv_pb2.SetRequest(keyspace=KS, key=b"k", value=b"v" * (MAX_VALUE_BYTES + 1))
        )
    )
    assert code == grpc.StatusCode.INVALID_ARGUMENT
    assert str(MAX_VALUE_BYTES) in details


def test_a_key_and_value_exactly_at_the_limit_are_accepted(kv):
    response = kv.Set(
        kv_pb2.SetRequest(
            keyspace=KS,
            key=b"k" * MAX_KEY_BYTES,
            value=b"v" * MAX_VALUE_BYTES,
        )
    )
    assert response.applied, "the documented maximum has to be inclusive"


def test_a_failed_condition_is_a_successful_call_that_did_not_apply(kv):
    # A condition that does not hold is an expected outcome of a working
    # program, not an error, so it must not be a status. A client library that
    # raised on FAILED_PRECONDITION here would make every lock acquisition a
    # try/except.
    written = kv.Set(kv_pb2.SetRequest(keyspace=KS, key=b"k", value=b"v"))
    response = kv.Set(
        kv_pb2.SetRequest(
            keyspace=KS,
            key=b"k",
            value=b"other",
            condition=kv_pb2.Condition(if_version=written.version + 99),
        )
    )
    assert response.applied is False


def test_a_cursor_the_server_did_not_issue_is_invalid_argument(kv):
    code, _ = code_of(
        lambda: kv.List(
            kv_pb2.ListRequest(keyspace=KS, prefix=b"item/", limit=10, cursor=b"garbage")
        )
    )
    assert code == grpc.StatusCode.INVALID_ARGUMENT
