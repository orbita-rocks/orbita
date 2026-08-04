"""The Admin service is declared but not served.

This is here so that the day someone implements it, this test fails and tells
us to write real coverage rather than leaving the admin surface untested. It
walks the service descriptor instead of a hand-written list, so an RPC added to
the proto is covered the moment it appears.
"""

import grpc
import pytest
from google.protobuf import message_factory
from orbita.v1 import admin_pb2

ADMIN_SERVICE = admin_pb2.DESCRIPTOR.services_by_name["Admin"]
METHODS = [method.name for method in ADMIN_SERVICE.methods]


def test_the_proto_still_declares_the_admin_surface_we_expect():
    # If this list shrinks, the failure above is a proto change and not a
    # server change, which is worth being able to tell apart.
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


@pytest.mark.parametrize("method_name", METHODS)
def test_every_admin_rpc_is_still_unimplemented(node, method_name):
    method = ADMIN_SERVICE.methods_by_name[method_name]
    request = message_factory.GetMessageClass(method.input_type)()

    with pytest.raises(grpc.RpcError) as caught:
        getattr(node.admin, method_name)(request, timeout=10.0)

    assert caught.value.code() == grpc.StatusCode.UNIMPLEMENTED, (
        f"{method_name} answered something. The admin surface is live now, so "
        "this suite needs real tests for it."
    )
