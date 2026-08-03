"""The whole Kv surface, driven the way a first-time user would drive it.

Nothing here reaches for knowledge that is not in the .proto files, because
the point is to find out where that is not enough.
"""

from orbita.v1 import kv_pb2

from conftest import DEFAULT_KEYSPACE as KS


def test_a_round_trip_returns_the_value_and_the_version_the_write_reported(kv):
    written = kv.Set(kv_pb2.SetRequest(keyspace=KS, key=b"greeting", value=b"hello"))
    assert written.applied
    assert written.version > 0

    read = kv.Get(kv_pb2.GetRequest(keyspace=KS, key=b"greeting"))
    assert read.found
    assert read.value == b"hello"
    assert read.version == written.version


def test_a_get_for_a_key_that_was_never_written_reports_absence_not_an_error(kv):
    read = kv.Get(kv_pb2.GetRequest(keyspace=KS, key=b"never-written"))
    assert not read.found
    assert read.value == b""
    assert read.version == 0
    assert not read.HasField("expires_at_millis")


def test_overwriting_a_key_returns_a_higher_version_and_the_new_value(kv):
    first = kv.Set(kv_pb2.SetRequest(keyspace=KS, key=b"k", value=b"one"))
    second = kv.Set(kv_pb2.SetRequest(keyspace=KS, key=b"k", value=b"two"))
    assert second.version > first.version

    read = kv.Get(kv_pb2.GetRequest(keyspace=KS, key=b"k"))
    assert read.value == b"two"
    assert read.version == second.version


def test_a_delete_removes_the_key_from_get_and_from_list(kv):
    kv.Set(kv_pb2.SetRequest(keyspace=KS, key=b"doomed", value=b"v"))

    deleted = kv.Delete(kv_pb2.DeleteRequest(keyspace=KS, key=b"doomed"))
    assert deleted.applied
    assert deleted.existed

    assert not kv.Get(kv_pb2.GetRequest(keyspace=KS, key=b"doomed")).found
    listed = kv.List(kv_pb2.ListRequest(keyspace=KS, prefix=b"doomed", limit=10))
    assert list(listed.entries) == []


def test_deleting_a_key_that_is_not_there_is_not_an_error(kv):
    # A client that retries a delete after a timeout has to be able to tell the
    # second attempt succeeded, so this cannot be an error.
    response = kv.Delete(kv_pb2.DeleteRequest(keyspace=KS, key=b"never-existed"))
    assert response.applied
    assert not response.existed
    assert not response.HasField("current_version")


def test_a_list_returns_the_keys_under_a_prefix_in_key_order(kv):
    for key in [b"b/2", b"a/1", b"b/1", b"c/1"]:
        kv.Set(kv_pb2.SetRequest(keyspace=KS, key=key, value=b"v"))

    page = kv.List(
        kv_pb2.ListRequest(keyspace=KS, prefix=b"b/", limit=10, include_values=True)
    )
    assert [entry.key for entry in page.entries] == [b"b/1", b"b/2"]
    assert [entry.value for entry in page.entries] == [b"v", b"v"]
    assert page.next_cursor == b""


def test_a_list_can_ask_for_keys_without_their_values(kv):
    kv.Set(kv_pb2.SetRequest(keyspace=KS, key=b"k", value=b"a value nobody wants"))

    page = kv.List(kv_pb2.ListRequest(keyspace=KS, prefix=b"k", limit=10))
    assert page.entries[0].key == b"k"
    assert page.entries[0].value == b""
    # The version still comes back, which is what makes a keys-only scan usable
    # as the first half of a compare-and-swap.
    assert page.entries[0].version > 0


def test_a_list_of_an_empty_prefix_range_is_an_empty_page_not_an_error(kv):
    page = kv.List(kv_pb2.ListRequest(keyspace=KS, prefix=b"nothing-here/", limit=10))
    assert list(page.entries) == []
    assert page.next_cursor == b""
