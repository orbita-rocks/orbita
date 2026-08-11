"""Manual adjacent-partition merges through the shipped Admin API."""

import threading
import time

import grpc
from orbita.v1 import admin_pb2, kv_pb2

from conftest import DEFAULT_KEYSPACE as KS, poll_until, restart
from test_split import _set_after_routing_converges, _split, _walk


def _merge(node, split):
    response = node.admin.MergePartitions(
        admin_pb2.MergePartitionsRequest(
            lower_partition_id=split.lower.id,
            upper_partition_id=split.upper.id,
        ),
        timeout=30.0,
    )

    def converged():
        described = node.admin.DescribeCluster(
            admin_pb2.DescribeClusterRequest(keyspace=KS), timeout=5.0
        )
        return described.partitions[0] if len(described.partitions) == 1 else None

    merged = poll_until(converged, timeout=20.0)
    assert response.merged.id == merged.id
    return merged


def _set_after_merge_routing_converges(node, key, value):
    def write():
        try:
            response = node.kv.Set(
                kv_pb2.SetRequest(keyspace=KS, key=key, value=value), timeout=2.0
            )
            return response if response.applied else None
        except grpc.RpcError as error:
            if error.code() == grpc.StatusCode.UNAVAILABLE:
                return None
            raise

    return poll_until(write, timeout=20.0)


def _get_after_merge_routing_converges(node, key):
    def read():
        try:
            response = node.kv.Get(
                kv_pb2.GetRequest(keyspace=KS, key=key), timeout=2.0
            )
            return response if response.found else None
        except grpc.RpcError as error:
            if error.code() == grpc.StatusCode.UNAVAILABLE:
                return None
            raise

    return poll_until(read, timeout=20.0)


def test_manual_merge_preserves_both_ranges_versions_and_list_coverage(node):
    expected = {b"a/original": b"low", b"z/original": b"high"}
    for key, value in expected.items():
        assert node.kv.Set(
            kv_pb2.SetRequest(keyspace=KS, key=key, value=value), timeout=10.0
        ).applied

    split = _split(node)
    low = _set_after_routing_converges(node, b"b/equal", b"equal-low")
    high = _set_after_routing_converges(node, b"y/equal", b"equal-high")
    assert low.version == high.version
    expected.update({b"b/equal": b"equal-low", b"y/equal": b"equal-high"})

    merged = _merge(node, split)
    assert merged.start_key == b"" and merged.end_key == b""
    for key, value in expected.items():
        got = _get_after_merge_routing_converges(node, key)
        assert got.found and got.value == value
    entries = _walk(node)
    assert [entry.key for entry in entries] == sorted(expected)
    assert {entry.key: entry.value for entry in entries} == expected

    first = _set_after_merge_routing_converges(node, b"n/after", b"forward")
    assert first.version > low.version


def test_acknowledged_writes_racing_merge_survive_and_restart(node):
    split = _split(node)
    acknowledged = {}
    lock = threading.Lock()
    started = threading.Event()
    stop = threading.Event()

    def write_loop():
        index = 0
        while not stop.is_set():
            side = b"a" if index % 2 == 0 else b"z"
            key = side + f"/merge-race/{index:05}".encode()
            value = b"value-" + str(index).encode()
            try:
                response = node.kv.Set(
                    kv_pb2.SetRequest(keyspace=KS, key=key, value=value), timeout=2.0
                )
                if response.applied:
                    with lock:
                        acknowledged[key] = value
                        if len(acknowledged) >= 5:
                            started.set()
            except grpc.RpcError:
                pass
            index += 1

    writer = threading.Thread(target=write_loop, daemon=True)
    writer.start()
    assert started.wait(timeout=10.0)
    _merge(node, split)
    time.sleep(0.05)
    stop.set()
    writer.join(timeout=10.0)
    assert not writer.is_alive()

    restart(node)
    with lock:
        durable = dict(acknowledged)
    assert durable
    for key, value in durable.items():
        got = node.kv.Get(kv_pb2.GetRequest(keyspace=KS, key=key), timeout=10.0)
        assert got.found and got.value == value
    assert node.kv.Set(
        kv_pb2.SetRequest(keyspace=KS, key=b"post-restart", value=b"writable"),
        timeout=10.0,
    ).applied
