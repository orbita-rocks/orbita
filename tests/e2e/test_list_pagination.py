"""Paging a prefix with the cursor, which is the only part of the API with
state a client has to carry between calls.

A cursor that dropped or repeated a key would be almost invisible in a small
test, so these use more keys than one page holds and check the whole set.
"""

import grpc
import pytest
from orbita.v1 import kv_pb2

from conftest import DEFAULT_KEYSPACE as KS

TOTAL_KEYS = 120


def fill(kv, total=TOTAL_KEYS):
    keys = [f"item/{i:04}".encode() for i in range(total)]
    for key in keys:
        kv.Set(kv_pb2.SetRequest(keyspace=KS, key=key, value=b"v"))
    # A key outside the prefix, so a scan that ignored the prefix would show up.
    kv.Set(kv_pb2.SetRequest(keyspace=KS, key=b"other", value=b"v"))
    return sorted(keys)


def walk(kv, prefix, limit, include_values=False):
    """Page through a prefix and return every entry, in the order seen."""
    entries = []
    cursor = b""
    pages = 0
    while True:
        page = kv.List(
            kv_pb2.ListRequest(
                keyspace=KS,
                prefix=prefix,
                cursor=cursor,
                limit=limit,
                include_values=include_values,
            )
        )
        pages += 1
        assert pages < 500, "the scan is not terminating"
        entries.extend(page.entries)
        cursor = page.next_cursor
        if cursor == b"":
            return entries, pages


@pytest.mark.parametrize("limit", [1, 7, 50])
def test_a_paged_scan_visits_every_key_exactly_once(kv, limit):
    expected = fill(kv)

    entries, pages = walk(kv, b"item/", limit)
    seen = [entry.key for entry in entries]

    assert len(seen) == len(set(seen)), "no key appears on two pages"
    assert seen == expected, "no duplicates, no gaps, and in key order"
    assert pages > 1, "the limit has to have actually paged"


def test_a_page_holds_no_more_than_the_limit_it_was_asked_for(kv):
    fill(kv, total=30)

    page = kv.List(kv_pb2.ListRequest(keyspace=KS, prefix=b"item/", limit=4))
    assert len(page.entries) == 4
    assert page.next_cursor != b"", "a truncated page has to say there is more"


def test_the_final_page_reports_an_empty_cursor(kv):
    fill(kv, total=5)

    page = kv.List(kv_pb2.ListRequest(keyspace=KS, prefix=b"item/", limit=100))
    assert len(page.entries) == 5
    assert page.next_cursor == b"", "an empty cursor is how a client stops"


def test_paging_carries_values_when_the_scan_asks_for_them(kv):
    for i in range(10):
        kv.Set(kv_pb2.SetRequest(keyspace=KS, key=f"v/{i}".encode(), value=b"payload"))

    entries, _ = walk(kv, b"v/", limit=3, include_values=True)
    assert len(entries) == 10
    assert all(entry.value == b"payload" for entry in entries)


def test_a_cursor_may_only_continue_the_prefix_it_was_issued_for(kv):
    # An opaque cursor invites a client to carry it somewhere it does not
    # belong, and silently returning the wrong range would be much worse than
    # an error.
    fill(kv, total=30)
    for i in range(5):
        kv.Set(kv_pb2.SetRequest(keyspace=KS, key=f"elsewhere/{i}".encode(), value=b"v"))

    cursor = kv.List(kv_pb2.ListRequest(keyspace=KS, prefix=b"item/", limit=4)).next_cursor

    with pytest.raises(grpc.RpcError) as caught:
        kv.List(
            kv_pb2.ListRequest(keyspace=KS, prefix=b"elsewhere/", limit=4, cursor=cursor)
        )
    assert caught.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert "prefix" in caught.value.details()


def test_a_scan_may_change_its_page_size_partway_through(kv):
    # A client that adapts its page size to how fast pages come back is a
    # normal thing to write, so the cursor must not be tied to the limit.
    fill(kv, total=30)

    first = kv.List(kv_pb2.ListRequest(keyspace=KS, prefix=b"item/", limit=4))
    second = kv.List(
        kv_pb2.ListRequest(
            keyspace=KS, prefix=b"item/", limit=2, cursor=first.next_cursor
        )
    )
    assert [entry.key for entry in second.entries] == [b"item/0004", b"item/0005"]


def test_a_limit_of_zero_returns_a_page_rather_than_nothing(kv):
    # Zero is the proto3 default, so any client that leaves the field alone
    # sends it. The server treats it as "your choice of page size", which is
    # useful and is not written down anywhere in the protos.
    fill(kv, total=30)

    page = kv.List(kv_pb2.ListRequest(keyspace=KS, prefix=b"item/", limit=0))
    assert len(page.entries) > 0
