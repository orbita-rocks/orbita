"""Versions behave the way ADR 0002 says they do, as seen from a client.

A version is the partition Lamport at which the key was last written, so from
one key's point of view the sequence is sparse. A client that assumed versions
counted its own writes would be wrong, and this is where that shows up.
"""

from orbita.v1 import kv_pb2

from conftest import DEFAULT_KEYSPACE as KS


def write(kv, key, value=b"v"):
    return kv.Set(kv_pb2.SetRequest(keyspace=KS, key=key, value=value))


def test_writing_other_keys_advances_the_version_a_later_write_receives(kv):
    first = write(kv, b"watched")
    for i in range(5):
        write(kv, f"unrelated/{i}".encode())
    second = write(kv, b"watched")

    assert second.version > first.version + 1, (
        "versions are partition wide, so unrelated writes consume numbers"
    )


def test_an_unrelated_write_does_not_change_the_version_a_key_is_holding(kv):
    written = write(kv, b"watched")
    for i in range(5):
        write(kv, f"unrelated/{i}".encode())

    assert kv.Get(kv_pb2.GetRequest(keyspace=KS, key=b"watched")).version == written.version
    # Which is what makes the version a client is holding still usable for a
    # compare-and-swap under load from other keys.
    swap = kv.Set(
        kv_pb2.SetRequest(
            keyspace=KS,
            key=b"watched",
            value=b"next",
            condition=kv_pb2.Condition(if_version=written.version),
        )
    )
    assert swap.applied


def test_versions_only_ever_move_forward_for_one_key(kv):
    versions = [write(kv, b"k", f"v{i}".encode()).version for i in range(20)]
    assert versions == sorted(set(versions)), "versions are strictly increasing"


def test_a_recreated_key_never_gets_a_version_it_already_had(kv):
    # This is the ABA case ADR 0002 exists to close: a stale compare-and-swap
    # against a key that was deleted and written again must not succeed.
    original = write(kv, b"lease", b"first")
    kv.Delete(kv_pb2.DeleteRequest(keyspace=KS, key=b"lease"))
    recreated = write(kv, b"lease", b"second")

    assert recreated.version > original.version

    stale = kv.Set(
        kv_pb2.SetRequest(
            keyspace=KS,
            key=b"lease",
            value=b"stolen",
            condition=kv_pb2.Condition(if_version=original.version),
        )
    )
    assert not stale.applied
    assert stale.current_version == recreated.version


def test_the_version_a_list_reports_is_the_one_the_write_returned(kv):
    written = {key: write(kv, key).version for key in [b"p/1", b"p/2", b"p/3"]}

    page = kv.List(kv_pb2.ListRequest(keyspace=KS, prefix=b"p/", limit=10))
    assert {entry.key: entry.version for entry in page.entries} == written
