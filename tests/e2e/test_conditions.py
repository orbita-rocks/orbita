"""Conditional writes, which is the feature the coordination use case rests on.

The Condition message is a protobuf oneof, and a oneof is exactly the shape
that can be encoded correctly by one language's generated code and read wrongly
by another. These tests set the oneof from Python and check that the server
agreed about which arm was chosen.
"""

import pytest
from orbita.v1 import kv_pb2

from conftest import DEFAULT_KEYSPACE as KS


def set_request(key, value, condition=None):
    return kv_pb2.SetRequest(keyspace=KS, key=key, value=value, condition=condition)


def test_if_not_present_creates_a_key_exactly_once(kv):
    condition = kv_pb2.Condition(if_not_present=True)

    first = kv.Set(set_request(b"lock", b"mine", condition))
    assert first.applied
    assert first.version > 0

    second = kv.Set(set_request(b"lock", b"yours", condition))
    assert not second.applied

    assert kv.Get(kv_pb2.GetRequest(keyspace=KS, key=b"lock")).value == b"mine"


def test_a_lost_if_not_present_reports_the_version_that_is_already_there(kv):
    winner = kv.Set(set_request(b"lock", b"mine", kv_pb2.Condition(if_not_present=True)))

    loser = kv.Set(set_request(b"lock", b"yours", kv_pb2.Condition(if_not_present=True)))
    assert not loser.applied
    assert loser.HasField("current_version")
    assert loser.current_version == winner.version


def test_a_compare_and_swap_against_the_current_version_wins(kv):
    first = kv.Set(set_request(b"pointer", b"a"))

    swapped = kv.Set(
        set_request(b"pointer", b"b", kv_pb2.Condition(if_version=first.version))
    )
    assert swapped.applied
    assert swapped.version > first.version
    assert kv.Get(kv_pb2.GetRequest(keyspace=KS, key=b"pointer")).value == b"b"


def test_a_lost_compare_and_swap_reports_the_winning_version(kv):
    first = kv.Set(set_request(b"pointer", b"a"))
    winner = kv.Set(
        set_request(b"pointer", b"b", kv_pb2.Condition(if_version=first.version))
    )

    loser = kv.Set(
        set_request(b"pointer", b"c", kv_pb2.Condition(if_version=first.version))
    )
    assert not loser.applied
    assert loser.current_version == winner.version, (
        "a loser needs the version that won so it can reread and retry"
    )
    assert kv.Get(kv_pb2.GetRequest(keyspace=KS, key=b"pointer")).value == b"b"


def test_a_compare_and_swap_against_an_absent_key_reports_no_current_version(kv):
    refused = kv.Set(set_request(b"ghost", b"v", kv_pb2.Condition(if_version=1)))
    assert not refused.applied
    assert not refused.HasField("current_version"), (
        "an unset current_version is how a caller learns the key is gone rather "
        "than merely at a different version"
    )
    assert not kv.Get(kv_pb2.GetRequest(keyspace=KS, key=b"ghost")).found


def test_a_condition_message_with_no_arm_set_writes_unconditionally(kv):
    # Python builds an empty Condition() readily, and a caller who has not
    # decided which arm to use will produce one by accident.
    kv.Set(set_request(b"k", b"first"))
    response = kv.Set(set_request(b"k", b"second", kv_pb2.Condition()))
    assert response.applied


def test_if_not_present_set_to_false_still_writes_over_an_existing_key(kv):
    # This documents a sharp edge rather than endorsing it. `if_not_present` is
    # a bool inside a oneof, so False is a set arm and not an absent condition,
    # yet the write applies anyway. See the README for why that matters.
    kv.Set(set_request(b"k", b"first"))
    response = kv.Set(set_request(b"k", b"second", kv_pb2.Condition(if_not_present=False)))
    assert response.applied
    assert kv.Get(kv_pb2.GetRequest(keyspace=KS, key=b"k")).value == b"second"


def test_a_conditional_delete_against_the_current_version_removes_the_key(kv):
    written = kv.Set(set_request(b"lease", b"held"))

    deleted = kv.Delete(
        kv_pb2.DeleteRequest(
            keyspace=KS,
            key=b"lease",
            condition=kv_pb2.Condition(if_version=written.version),
        )
    )
    assert deleted.applied
    assert deleted.existed
    assert not kv.Get(kv_pb2.GetRequest(keyspace=KS, key=b"lease")).found


def test_a_conditional_delete_that_loses_leaves_the_key_alone(kv):
    written = kv.Set(set_request(b"lease", b"held"))

    refused = kv.Delete(
        kv_pb2.DeleteRequest(
            keyspace=KS,
            key=b"lease",
            condition=kv_pb2.Condition(if_version=written.version + 1),
        )
    )
    assert not refused.applied
    assert refused.current_version == written.version
    assert kv.Get(kv_pb2.GetRequest(keyspace=KS, key=b"lease")).found


@pytest.mark.parametrize("arm", ["if_not_present", "if_version"])
def test_the_server_reads_the_same_oneof_arm_python_wrote(kv, arm):
    # If the two sides disagreed about field numbering or arm identity, one of
    # these would behave like the other, so running both is the check.
    written = kv.Set(set_request(b"k", b"v"))
    condition = (
        kv_pb2.Condition(if_not_present=True)
        if arm == "if_not_present"
        else kv_pb2.Condition(if_version=written.version)
    )
    assert condition.WhichOneof("kind") == arm

    response = kv.Set(set_request(b"k", b"next", condition))
    # if_not_present must fail on a key that exists; if_version must succeed
    # against the version that is actually there.
    assert response.applied == (arm == "if_version")
