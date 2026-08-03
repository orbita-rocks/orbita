"""Data written before a restart is still there afterwards.

The Rust suite covers this in process. Doing it again from outside is worth the
duplication because here the node is a real process that is really killed, so
anything the server only flushes on a graceful in-process shutdown would show
up.
"""

from orbita.v1 import kv_pb2

from conftest import DEFAULT_KEYSPACE as KS, restart


def test_an_acknowledged_write_survives_a_restart_of_the_process(node):
    written = node.kv.Set(kv_pb2.SetRequest(keyspace=KS, key=b"durable", value=b"yes"))
    assert written.applied

    restart(node)

    read = node.kv.Get(kv_pb2.GetRequest(keyspace=KS, key=b"durable"))
    assert read.found
    assert read.value == b"yes"
    assert read.version == written.version, "a version is not reissued across a restart"


def test_versions_continue_after_a_restart_rather_than_starting_over(node):
    written = node.kv.Set(kv_pb2.SetRequest(keyspace=KS, key=b"k", value=b"before"))

    restart(node)

    after = node.kv.Set(kv_pb2.SetRequest(keyspace=KS, key=b"k", value=b"after"))
    assert after.version > written.version

    # And a compare-and-swap held across the restart still behaves, which is
    # what a lock holder that reconnected would do.
    stale = node.kv.Set(
        kv_pb2.SetRequest(
            keyspace=KS,
            key=b"k",
            value=b"stale",
            condition=kv_pb2.Condition(if_version=written.version),
        )
    )
    assert not stale.applied
    assert stale.current_version == after.version


def test_a_deleted_key_stays_deleted_across_a_restart(node):
    node.kv.Set(kv_pb2.SetRequest(keyspace=KS, key=b"gone", value=b"v"))
    node.kv.Delete(kv_pb2.DeleteRequest(keyspace=KS, key=b"gone"))

    restart(node)

    assert not node.kv.Get(kv_pb2.GetRequest(keyspace=KS, key=b"gone")).found
