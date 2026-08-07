"""The Admin service, over the wire, against a real single node.

This file replaces a tripwire. It used to assert that every Admin RPC answered
UNIMPLEMENTED, and said in its own docstring that the day someone implemented
the service the test should fail and be rewritten with real coverage. That day
is issue #88, and this is the rewrite.

It still walks the service descriptor rather than a hand-written list, so an
RPC added to the proto is noticed the moment it appears. What changed is what
the walk asserts: an RPC is either served, or refused on purpose with a reason
the refusal itself gives. Nothing here is allowed to be unimplemented by
accident.
"""

import grpc
import pytest
from google.protobuf import message_factory
from orbita.v1 import admin_pb2, kv_pb2

from conftest import DEFAULT_KEYSPACE, poll_until

ADMIN_SERVICE = admin_pb2.DESCRIPTOR.services_by_name["Admin"]
METHODS = [method.name for method in ADMIN_SERVICE.methods]

# Merge remains deliberately unavailable. Split is exercised over the real
# public API in test_split.py.
DELIBERATELY_UNIMPLEMENTED = {"MergePartitions"}


def test_the_proto_still_declares_the_admin_surface_we_expect():
    # If this list shrinks, a failure below is a proto change and not a server
    # change, which is worth being able to tell apart.
    assert set(METHODS) == {
        "CreateKeyspace",
        "UpdateKeyspace",
        "DeleteKeyspace",
        "ListKeyspaces",
        "CreateCredential",
        "RevokeCredential",
        "DescribeCluster",
        "SplitPartition",
        "MergePartitions",
        "TransferOwnership",
        "FinalizeUpgrade",
    }


@pytest.mark.parametrize("method_name", sorted(METHODS))
def test_every_admin_rpc_is_served_by_a_single_node(node, method_name):
    """A dev node is a whole cluster, so it answers the whole admin surface.

    The requests are empty, so most of these are refused on their arguments.
    That is the point: a refusal on the arguments proves the RPC reached the
    control plane, whereas UNIMPLEMENTED proves it never got there.
    """
    method = ADMIN_SERVICE.methods_by_name[method_name]
    request = message_factory.GetMessageClass(method.input_type)()

    try:
        getattr(node.admin, method_name)(request, timeout=10.0)
        code = grpc.StatusCode.OK
        details = ""
    except grpc.RpcError as error:
        code = error.code()
        details = error.details() or ""

    if method_name in DELIBERATELY_UNIMPLEMENTED:
        assert code == grpc.StatusCode.UNIMPLEMENTED
        assert details, f"{method_name} has to say why it is refused"
        return

    assert code != grpc.StatusCode.UNIMPLEMENTED, (
        f"{method_name} is not served. A node started with `orbita dev` is its "
        "own leader group, so every admin RPC has somewhere to go."
    )
    assert code != grpc.StatusCode.UNAVAILABLE, (
        f"{method_name} answered UNAVAILABLE, which is what a node that cannot "
        "find the control leader says. On a single node there is only one."
    )


def test_a_single_node_reports_itself_as_the_control_leader(node):
    """A group of one still has to hold an election before it can decide.

    Asserted rather than assumed, because everything else in this file would
    pass by luck if the controller happened to answer a read from stale state.
    """
    described = node.admin.DescribeCluster(
        admin_pb2.DescribeClusterRequest(), timeout=10.0
    )
    leaders = [n.id for n in described.nodes if n.is_raft_leader]
    assert leaders == [1], f"expected the only node to lead, got {described.nodes}"


def test_the_keyspace_created_at_startup_is_listed(node):
    listed = node.admin.ListKeyspaces(admin_pb2.ListKeyspacesRequest(), timeout=10.0)
    assert DEFAULT_KEYSPACE in [k.name for k in listed.keyspaces]


def test_a_created_keyspace_can_be_written_to_and_read_back(node):
    """The quickstart, as a test.

    Create, write, read. This is the sequence `orbita --help` prints and the
    one that failed on its second line before the admin surface was served.
    """
    created = node.admin.CreateKeyspace(
        admin_pb2.CreateKeyspaceRequest(name="demo"), timeout=10.0
    )
    assert created.name == "demo"
    assert created.partition_count == 1, "a new keyspace is born covered"

    listed = node.admin.ListKeyspaces(admin_pb2.ListKeyspacesRequest(), timeout=10.0)
    assert "demo" in [k.name for k in listed.keyspaces]

    # The node serves from a partition map it refreshes on a timer, so a write
    # arriving immediately after the creation may be the request that repairs
    # the map. Retrying covers the interval; failing forever would not be it.
    def written():
        try:
            return node.kv.Set(
                kv_pb2.SetRequest(keyspace="demo", key=b"greeting", value=b"hello"),
                timeout=5.0,
            )
        except grpc.RpcError:
            return None

    assert poll_until(written), "a keyspace that exists has to be writable"
    got = node.kv.Get(
        kv_pb2.GetRequest(keyspace="demo", key=b"greeting"), timeout=10.0
    )
    assert got.found and got.value == b"hello"


def test_creating_a_keyspace_that_exists_is_already_exists(node):
    with pytest.raises(grpc.RpcError) as caught:
        node.admin.CreateKeyspace(
            admin_pb2.CreateKeyspaceRequest(name=DEFAULT_KEYSPACE), timeout=10.0
        )
    assert caught.value.code() == grpc.StatusCode.ALREADY_EXISTS


def test_an_updated_keyspace_limit_reaches_the_data_plane(node):
    """A config change is only real if a client can see it.

    GetLimits is what a client sizes its channel from, so this checks that the
    admin write travelled all the way to the map the KV surface answers out of.
    """
    node.admin.UpdateKeyspace(
        admin_pb2.UpdateKeyspaceRequest(
            name=DEFAULT_KEYSPACE,
            config=admin_pb2.KeyspaceConfig(max_value_bytes=1024),
        ),
        timeout=10.0,
    )

    def narrowed():
        limits = node.kv.GetLimits(
            kv_pb2.GetLimitsRequest(keyspace=DEFAULT_KEYSPACE), timeout=5.0
        )
        return limits.max_value_bytes == 1024

    assert poll_until(narrowed), "the lowered cap never reached the KV surface"


def test_deleting_a_keyspace_requires_the_name_repeated(node):
    node.admin.CreateKeyspace(
        admin_pb2.CreateKeyspaceRequest(name="doomed"), timeout=10.0
    )

    with pytest.raises(grpc.RpcError) as caught:
        node.admin.DeleteKeyspace(
            admin_pb2.DeleteKeyspaceRequest(name="doomed", confirm_name="typo"),
            timeout=10.0,
        )
    assert caught.value.code() == grpc.StatusCode.INVALID_ARGUMENT

    node.admin.DeleteKeyspace(
        admin_pb2.DeleteKeyspaceRequest(name="doomed", confirm_name="doomed"),
        timeout=10.0,
    )
    listed = node.admin.ListKeyspaces(admin_pb2.ListKeyspacesRequest(), timeout=10.0)
    assert "doomed" not in [k.name for k in listed.keyspaces]


def test_a_credential_is_issued_once_and_can_be_revoked(node):
    created = node.admin.CreateCredential(
        admin_pb2.CreateCredentialRequest(
            keyspaces=[DEFAULT_KEYSPACE],
            permissions=[admin_pb2.PERMISSION_READ],
            description="an end to end test",
        ),
        timeout=10.0,
    )
    assert created.credential_id
    assert created.secret, "the secret is returned once, at creation, or never"

    node.admin.RevokeCredential(
        admin_pb2.RevokeCredentialRequest(credential_id=created.credential_id),
        timeout=10.0,
    )

    with pytest.raises(grpc.RpcError) as caught:
        node.admin.RevokeCredential(
            admin_pb2.RevokeCredentialRequest(credential_id=created.credential_id),
            timeout=10.0,
        )
    assert caught.value.code() == grpc.StatusCode.NOT_FOUND


def test_a_credential_scoped_to_nothing_is_refused(node):
    with pytest.raises(grpc.RpcError) as caught:
        node.admin.CreateCredential(
            admin_pb2.CreateCredentialRequest(permissions=[admin_pb2.PERMISSION_READ]),
            timeout=10.0,
        )
    assert caught.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_describe_cluster_covers_every_partition_of_the_keyspace_it_is_asked_about(node):
    described = node.admin.DescribeCluster(
        admin_pb2.DescribeClusterRequest(keyspace=DEFAULT_KEYSPACE), timeout=10.0
    )
    assert [k.name for k in described.keyspaces] == [DEFAULT_KEYSPACE]
    assert len(described.partitions) == 1
    partition = described.partitions[0]
    assert partition.start_key == b"" and partition.end_key == b""
    assert partition.owner_node_id == 1, (
        "the only node has to own the partition, or a dev cluster serves nothing"
    )


def test_describe_cluster_for_a_keyspace_that_does_not_exist_is_not_found(node):
    with pytest.raises(grpc.RpcError) as caught:
        node.admin.DescribeCluster(
            admin_pb2.DescribeClusterRequest(keyspace="nothing-here"), timeout=10.0
        )
    assert caught.value.code() == grpc.StatusCode.NOT_FOUND


def test_transferring_ownership_to_a_node_that_does_not_exist_is_refused(node):
    """Honest coverage of a path a single node cannot exercise properly.

    A transfer needs somewhere to transfer to, which a one-node cluster does
    not have. What can be checked here is that the request reaches the control
    plane and is refused on its merits rather than dropped.
    """
    described = node.admin.DescribeCluster(
        admin_pb2.DescribeClusterRequest(), timeout=10.0
    )
    partition = described.partitions[0].id

    with pytest.raises(grpc.RpcError) as caught:
        node.admin.TransferOwnership(
            admin_pb2.TransferOwnershipRequest(partition_id=partition, to_node_id=99),
            timeout=10.0,
        )
    assert caught.value.code() in {
        grpc.StatusCode.INVALID_ARGUMENT,
        grpc.StatusCode.NOT_FOUND,
    }


@pytest.mark.parametrize("method_name", sorted(DELIBERATELY_UNIMPLEMENTED))
def test_deliberately_unimplemented_admin_calls_refuse_and_say_why(node, method_name):
    """The remaining unimplemented call refuses with an explanation.

    Kept as its own test rather than folded into the descriptor walk so that
    the list of deliberate refusals is somewhere a reader will find it. When
    it lands, this test is where the deletion goes.
    """
    method = ADMIN_SERVICE.methods_by_name[method_name]
    request = message_factory.GetMessageClass(method.input_type)()

    with pytest.raises(grpc.RpcError) as caught:
        getattr(node.admin, method_name)(request, timeout=10.0)

    assert caught.value.code() == grpc.StatusCode.UNIMPLEMENTED
    assert "not implemented" in (caught.value.details() or "").lower() or "disabled" in (
        caught.value.details() or ""
    ).lower()
