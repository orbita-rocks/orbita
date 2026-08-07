"""Manual partition splits through the shipped binary and public gRPC API.

The standard E2E harness intentionally runs one self-contained ``orbita dev``
process per test. It therefore cannot target a follower ingress or hold a split
pending on an unacknowledged second holder. Deterministic multi-node Rust tests
pin those lease and pending-restart invariants; these tests prove the public
surface preserves data, routing, and durability in a real process.
"""

import threading
import time

import grpc
from orbita.v1 import admin_pb2, kv_pb2

from conftest import DEFAULT_KEYSPACE as KS, poll_until, restart


def _partition(node):
    described = node.admin.DescribeCluster(
        admin_pb2.DescribeClusterRequest(keyspace=KS), timeout=10.0
    )
    assert len(described.partitions) == 1
    return described.partitions[0]


def _split(node, boundary=b"m"):
    response = node.admin.SplitPartition(
        admin_pb2.SplitPartitionRequest(
            partition_id=_partition(node).id, split_key=boundary
        ),
        timeout=30.0,
    )

    def converged():
        described = node.admin.DescribeCluster(
            admin_pb2.DescribeClusterRequest(keyspace=KS), timeout=5.0
        )
        return described if len(described.partitions) == 2 else None

    described = poll_until(converged, timeout=20.0)
    assert {p.id for p in described.partitions} == {
        response.lower.id,
        response.upper.id,
    }
    return response


def _walk(node, prefix=b"", limit=7):
    entries = []
    cursor = b""
    for _ in range(500):
        page = node.kv.List(
            kv_pb2.ListRequest(
                keyspace=KS,
                prefix=prefix,
                cursor=cursor,
                limit=limit,
                include_values=True,
            ),
            timeout=10.0,
        )
        entries.extend(page.entries)
        cursor = page.next_cursor
        if not cursor:
            return entries
    raise AssertionError("the cross-partition list did not terminate")


def test_manual_split_preserves_both_ranges_and_lists_each_key_once(node):
    expected = {}
    for side in (b"a", b"z"):
        for index in range(24):
            key = side + f"/{index:03}".encode()
            value = b"value-" + key
            written = node.kv.Set(
                kv_pb2.SetRequest(keyspace=KS, key=key, value=value), timeout=10.0
            )
            assert written.applied
            expected[key] = value

    result = _split(node)
    assert result.lower.end_key == b"m"
    assert result.upper.start_key == b"m"
    for key, value in expected.items():
        got = node.kv.Get(kv_pb2.GetRequest(keyspace=KS, key=key), timeout=10.0)
        assert got.found and got.value == value

    entries = _walk(node)
    seen = [entry.key for entry in entries]
    assert seen == sorted(expected)
    assert len(seen) == len(set(seen)), "routing/list pagination duplicated a key"
    assert {entry.key: entry.value for entry in entries} == expected


def test_every_write_acknowledged_during_a_real_split_remains_readable(node):
    acknowledged = {}
    lock = threading.Lock()
    stop = threading.Event()
    started = threading.Event()

    def write_loop():
        index = 0
        while not stop.is_set():
            side = b"a" if index % 2 == 0 else b"z"
            key = side + f"/race/{index:05}".encode()
            value = b"race-value-" + str(index).encode()
            try:
                reply = node.kv.Set(
                    kv_pb2.SetRequest(keyspace=KS, key=key, value=value), timeout=2.0
                )
                if reply.applied:
                    with lock:
                        acknowledged[key] = value
                        if len(acknowledged) >= 5:
                            started.set()
            except grpc.RpcError:
                pass
            index += 1

    writer = threading.Thread(target=write_loop, daemon=True)
    writer.start()
    assert started.wait(timeout=10.0), "the writer never reached the live parent"
    _split(node)
    time.sleep(0.05)
    stop.set()
    writer.join(timeout=10.0)
    assert not writer.is_alive()

    with lock:
        durable = dict(acknowledged)
    assert durable
    for key, value in durable.items():
        got = node.kv.Get(kv_pb2.GetRequest(keyspace=KS, key=key), timeout=10.0)
        assert got.found and got.value == value


def test_split_children_and_later_writes_survive_a_process_restart(node):
    before = {b"a/restart": b"lower", b"z/restart": b"upper"}
    for key, value in before.items():
        assert node.kv.Set(
            kv_pb2.SetRequest(keyspace=KS, key=key, value=value), timeout=10.0
        ).applied
    _split(node)

    after = {b"b/after": b"new-lower", b"y/after": b"new-upper"}
    for key, value in after.items():
        assert node.kv.Set(
            kv_pb2.SetRequest(keyspace=KS, key=key, value=value), timeout=10.0
        ).applied
    restart(node)

    for key, value in {**before, **after}.items():
        got = node.kv.Get(kv_pb2.GetRequest(keyspace=KS, key=key), timeout=10.0)
        assert got.found and got.value == value
    assert node.kv.Set(
        kv_pb2.SetRequest(keyspace=KS, key=b"z/post-restart", value=b"still-writable"),
        timeout=10.0,
    ).applied


def test_reads_remain_current_through_split_activation_and_a_child_update(node):
    key = b"a/read-continuity"
    old = b"before"
    new = b"after"
    assert node.kv.Set(
        kv_pb2.SetRequest(keyspace=KS, key=key, value=old), timeout=10.0
    ).applied
    stop = threading.Event()
    updated = threading.Event()
    after_update = []

    def read_loop():
        while not stop.is_set():
            began_after_update = updated.is_set()
            try:
                got = node.kv.Get(
                    kv_pb2.GetRequest(keyspace=KS, key=key), timeout=2.0
                )
                assert got.found
                if began_after_update:
                    after_update.append(got.value)
            except grpc.RpcError:
                pass

    reader = threading.Thread(target=read_loop, daemon=True)
    reader.start()
    _split(node)
    assert node.kv.Set(
        kv_pb2.SetRequest(keyspace=KS, key=key, value=new), timeout=10.0
    ).applied
    updated.set()
    poll_until(lambda: len(after_update) >= 10, timeout=10.0)
    stop.set()
    reader.join(timeout=10.0)
    assert after_update
    assert set(after_update) == {new}, "a read begun after the child write saw the parent value"
