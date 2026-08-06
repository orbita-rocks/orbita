//! The Admin surface is sized apart from the KV surface, over the real gRPC
//! transport.
//!
//! A cluster description grows with the number of partitions, each carrying
//! boundary keys up to the key limit; it has nothing to do with any keyspace's
//! value or list size. Sizing the Admin transport from the KV ceiling — as an
//! earlier change did to keep the KV halves in agreement — put a few-megabyte
//! cap on a response that is the store's own truthful answer about itself, so a
//! description of a few hundred partitions came back as `OUT_OF_RANGE`. These
//! tests stand up a stub Admin service that returns a description too large for
//! the KV ceiling and prove the Admin ceiling carries it whole, and that the KV
//! ceiling would have refused it.

use orbita_core::MAX_KEY_BYTES;
use orbita_proto::v1 as pb;
use orbita_proto::v1::admin_client::AdminClient;
use orbita_proto::v1::admin_server::{Admin, AdminServer};

use prost::Message as _;
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status};

/// The number of partitions whose boundary keys alone push the description past
/// the KV ceiling. Two keys of `MAX_KEY_BYTES` each is ~20 KiB per partition;
/// 220 of them is ~4.4 MiB, over the 4 MiB-and-change KV ceiling and well
/// under the Admin one.
const PARTITIONS: u64 = 220;

/// A stub that answers `describe_cluster` and `list_keyspaces` with responses
/// deliberately larger than the KV ceiling, and refuses everything else. It
/// stands in for the real controller so the test exercises the transport
/// sizing, which is the only thing under test here, without a Raft group.
#[derive(Default)]
struct BigAdmin;

fn big_description() -> pb::DescribeClusterResponse {
    let boundary = vec![b'k'; MAX_KEY_BYTES];
    let partitions = (0..PARTITIONS)
        .map(|id| pb::Partition {
            id,
            keyspace_id: 1,
            start_key: boundary.clone(),
            end_key: boundary.clone(),
            owner_node_id: 1,
            epoch: 1,
            committed_lamport: None,
            replicas: Vec::new(),
            size_bytes: None,
            index_bytes: None,
        })
        .collect();
    pb::DescribeClusterResponse {
        nodes: Vec::new(),
        partitions,
        cluster_version: None,
        keyspaces: Vec::new(),
    }
}

fn big_keyspace_list() -> pb::ListKeyspacesResponse {
    // Names are small, so a keyspace list gets its bulk from having many
    // entries rather than large ones; either way it is a valid answer the KV
    // ceiling has no business bounding.
    let keyspaces = (0..2_000u64)
        .map(|i| pb::Keyspace {
            name: format!("keyspace-with-a-reasonably-long-name-{i:08}"),
            ..Default::default()
        })
        .collect();
    pb::ListKeyspacesResponse { keyspaces }
}

#[tonic::async_trait]
impl Admin for BigAdmin {
    async fn describe_cluster(
        &self,
        _request: Request<pb::DescribeClusterRequest>,
    ) -> Result<Response<pb::DescribeClusterResponse>, Status> {
        Ok(Response::new(big_description()))
    }

    async fn list_keyspaces(
        &self,
        _request: Request<pb::ListKeyspacesRequest>,
    ) -> Result<Response<pb::ListKeyspacesResponse>, Status> {
        Ok(Response::new(big_keyspace_list()))
    }

    async fn create_keyspace(
        &self,
        _request: Request<pb::CreateKeyspaceRequest>,
    ) -> Result<Response<pb::Keyspace>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn update_keyspace(
        &self,
        _request: Request<pb::UpdateKeyspaceRequest>,
    ) -> Result<Response<pb::Keyspace>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn delete_keyspace(
        &self,
        _request: Request<pb::DeleteKeyspaceRequest>,
    ) -> Result<Response<pb::DeleteKeyspaceResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn create_credential(
        &self,
        _request: Request<pb::CreateCredentialRequest>,
    ) -> Result<Response<pb::CreateCredentialResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn revoke_credential(
        &self,
        _request: Request<pb::RevokeCredentialRequest>,
    ) -> Result<Response<pb::RevokeCredentialResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn finalize_upgrade(
        &self,
        _request: Request<pb::FinalizeUpgradeRequest>,
    ) -> Result<Response<pb::FinalizeUpgradeResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn split_partition(
        &self,
        _request: Request<pb::SplitPartitionRequest>,
    ) -> Result<Response<pb::SplitPartitionResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn merge_partitions(
        &self,
        _request: Request<pb::MergePartitionsRequest>,
    ) -> Result<Response<pb::MergePartitionsResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn transfer_ownership(
        &self,
        _request: Request<pb::TransferOwnershipRequest>,
    ) -> Result<Response<pb::TransferOwnershipResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }
}

/// Serves `BigAdmin` on an ephemeral port, sized exactly as the real server
/// sizes its Admin surface, and returns the address plus a shutdown handle.
async fn serve_admin() -> (
    String,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind an ephemeral port");
    let addr = listener.local_addr().expect("read the bound address");
    let incoming = tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
        .expect("wrap the listener");

    let admin_limit = orbita_server::max_admin_message_bytes();
    let service = AdminServer::new(BigAdmin)
        .max_decoding_message_size(admin_limit)
        .max_encoding_message_size(admin_limit);

    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stop_rx.await;
            })
            .await
            .expect("the admin server serves");
    });

    (format!("http://{addr}"), stop_tx, handle)
}

#[tokio::test]
async fn a_cluster_description_larger_than_the_kv_ceiling_is_returned_whole() {
    let (endpoint, stop, handle) = serve_admin().await;

    // A client sized the way the real admin client is sized carries the whole
    // description.
    let admin_limit = orbita_server::max_admin_message_bytes();
    let mut client = AdminClient::new(
        Channel::from_shared(endpoint.clone())
            .expect("a valid endpoint")
            .connect()
            .await
            .expect("connect to the admin stub"),
    )
    .max_decoding_message_size(admin_limit)
    .max_encoding_message_size(admin_limit);

    let described = client
        .describe_cluster(pb::DescribeClusterRequest {
            keyspace: String::new(),
        })
        .await
        .expect("an admin-sized channel carries a large description")
        .into_inner();
    assert_eq!(described.partitions.len() as u64, PARTITIONS);

    // The response really is over the KV ceiling: that is the whole point, and
    // is what the earlier change refused.
    let encoded = described.encoded_len();
    let kv_ceiling = orbita_server::max_transport_message_bytes();
    assert!(
        encoded > kv_ceiling,
        "the description ({encoded} bytes) must exceed the KV ceiling ({kv_ceiling} bytes) \
         for this test to prove anything"
    );

    let listed = client
        .list_keyspaces(pb::ListKeyspacesRequest {})
        .await
        .expect("an admin-sized channel carries a large keyspace list")
        .into_inner();
    assert_eq!(listed.keyspaces.len(), 2_000);

    let _ = stop.send(());
    let _ = handle.await;
}

#[tokio::test]
async fn the_kv_ceiling_would_have_refused_the_same_description() {
    let (endpoint, stop, handle) = serve_admin().await;

    // A client capped at the KV ceiling — which is what the regressed code
    // applied to the admin client — cannot decode the description at all. This
    // is the failure the fix removes, pinned so it cannot creep back.
    let kv_ceiling = orbita_server::max_transport_message_bytes();
    let mut client = AdminClient::new(
        Channel::from_shared(endpoint.clone())
            .expect("a valid endpoint")
            .connect()
            .await
            .expect("connect to the admin stub"),
    )
    .max_decoding_message_size(kv_ceiling);

    let refused = client
        .describe_cluster(pb::DescribeClusterRequest {
            keyspace: String::new(),
        })
        .await
        .expect_err("a KV-sized channel is too small for a large description");
    assert_eq!(
        refused.code(),
        tonic::Code::OutOfRange,
        "the KV ceiling refuses the description for its size"
    );

    let _ = stop.send(());
    let _ = handle.await;
}
