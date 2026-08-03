"""TTL, from the outside, where the only thing visible is what the API returns.

The guarantee in the requirements is "never visible after expiry, reclaimed
eventually", so these tests check visibility and say nothing about storage.
"""

import time

from orbita.v1 import kv_pb2

from conftest import DEFAULT_KEYSPACE as KS, poll_until

# Short enough that the suite stays quick, long enough that a loaded machine
# does not expire the key before the first read.
TTL_MILLIS = 500


def test_a_ttl_comes_back_as_an_absolute_expiry_the_client_can_read(kv):
    before = time.time() * 1000
    kv.Set(
        kv_pb2.SetRequest(
            keyspace=KS, key=b"session/a", value=b"v", ttl_millis=TTL_MILLIS
        )
    )

    read = kv.Get(kv_pb2.GetRequest(keyspace=KS, key=b"session/a"))
    assert read.found
    assert read.HasField("expires_at_millis")
    # The server converts a duration into a deadline immediately, so the
    # deadline it reports has to sit in the window we just created.
    assert before <= read.expires_at_millis <= before + TTL_MILLIS + 5000


def test_a_key_without_a_ttl_reports_no_expiry(kv):
    kv.Set(kv_pb2.SetRequest(keyspace=KS, key=b"forever", value=b"v"))
    read = kv.Get(kv_pb2.GetRequest(keyspace=KS, key=b"forever"))
    assert read.found
    assert not read.HasField("expires_at_millis")


def test_an_expired_key_disappears_from_get_and_from_list(kv):
    kv.Set(
        kv_pb2.SetRequest(
            keyspace=KS, key=b"session/a", value=b"v", ttl_millis=TTL_MILLIS
        )
    )
    kv.Set(kv_pb2.SetRequest(keyspace=KS, key=b"session/b", value=b"v"))

    assert kv.Get(kv_pb2.GetRequest(keyspace=KS, key=b"session/a")).found

    poll_until(
        lambda: not kv.Get(kv_pb2.GetRequest(keyspace=KS, key=b"session/a")).found,
        timeout=15.0,
    )

    page = kv.List(kv_pb2.ListRequest(keyspace=KS, prefix=b"session/", limit=10))
    assert [entry.key for entry in page.entries] == [b"session/b"], (
        "an expired key is invisible to a scan whether or not it has been reclaimed"
    )


def test_a_key_written_with_a_zero_ttl_is_already_gone(kv):
    # Zero is a duration the proto allows and does not describe. A client
    # computing a TTL from arithmetic can land on it, so the behaviour is worth
    # pinning down.
    response = kv.Set(
        kv_pb2.SetRequest(keyspace=KS, key=b"instant", value=b"v", ttl_millis=0)
    )
    assert response.applied
    assert not kv.Get(kv_pb2.GetRequest(keyspace=KS, key=b"instant")).found
