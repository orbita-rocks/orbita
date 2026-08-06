//! Fixed leader voters through the production server and TCP transport.

use orbita_core::NodeId;
use orbita_proto::v1::admin_client::AdminClient;
use orbita_proto::v1::{
    CreateKeyspaceRequest, DescribeClusterRequest, ListKeyspacesRequest, SplitPartitionRequest,
};
use orbita_server::{Server, ServerConfig};
use tonic::Code;

use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::time::Duration;

fn unused_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve a loopback port");
    listener.local_addr().expect("read the reserved address")
}

struct DataDir(PathBuf);

impl DataDir {
    fn new(node: u64) -> Self {
        let path =
            std::env::temp_dir().join(format!("orbita-leader-group-{}-{node}", std::process::id()));
        std::fs::remove_dir_all(&path).ok();
        std::fs::create_dir_all(&path).expect("create the node data directory");
        Self(path)
    }
}

impl Drop for DataDir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).ok();
    }
}

fn config(
    node: NodeId,
    dir: &DataDir,
    peer_addr: SocketAddr,
    peers: &[(NodeId, String)],
) -> ServerConfig {
    ServerConfig::single_node(&dir.0)
        .with_node_id(node)
        .with_listen_addr(unused_addr())
        .with_peer_listen_addr(peer_addr)
        .with_peer_advertise_addr(peer_addr.to_string())
        .with_peers(peers.to_vec())
        .with_leader_group(peers.iter().map(|(id, _)| *id).collect())
        .with_leader_member(true)
}

async fn keyspace_names(server: &Server) -> Option<Vec<String>> {
    let mut client = AdminClient::connect(format!("http://{}", server.local_addr()))
        .await
        .ok()?;
    Some(
        client
            .list_keyspaces(ListKeyspacesRequest {})
            .await
            .ok()?
            .into_inner()
            .keyspaces
            .into_iter()
            .map(|keyspace| keyspace.name)
            .collect(),
    )
}

async fn leader_keyspace_names(servers: &[&Server]) -> Vec<String> {
    for server in servers {
        if let Some(names) = keyspace_names(server).await {
            return names;
        }
    }
    panic!("no leader voter served the Admin read")
}

async fn wait_until_ready(server: &Server) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !server.readiness().is_ready() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("leader voter reaches readiness after control catch-up");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_of_three_configured_leaders_form_replicate_and_restart() {
    let dirs = [DataDir::new(1), DataDir::new(2), DataDir::new(3)];
    let peer_addrs = [unused_addr(), unused_addr(), unused_addr()];
    let peers: Vec<_> = peer_addrs
        .iter()
        .enumerate()
        .map(|(offset, address)| (NodeId(offset as u64 + 1), address.to_string()))
        .collect();

    // Node three is deliberately unavailable. Two fixed voters are still a
    // quorum, and no special bootstrap mode should be needed.
    let (one, two) = tokio::join!(
        Server::start(config(NodeId(1), &dirs[0], peer_addrs[0], &peers)),
        Server::start(config(NodeId(2), &dirs[1], peer_addrs[1], &peers)),
    );
    let one = one.expect("the first voter starts");
    let mut two = two.expect("the second voter starts");
    wait_until_ready(&one).await;
    wait_until_ready(&two).await;

    let mut created = false;
    for server in [&one, &two] {
        let mut client = AdminClient::connect(format!("http://{}", server.local_addr()))
            .await
            .expect("connect to a leader voter");
        if client
            .create_keyspace(CreateKeyspaceRequest {
                name: "replicated".to_string(),
                config: None,
            })
            .await
            .is_ok()
        {
            created = true;
            break;
        }
    }
    assert!(created, "one voter must accept the control-plane proposal");

    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(leader_keyspace_names(&[&one, &two])
        .await
        .contains(&"replicated".to_string()));

    // Split is wired to the real worker-prepared protocol now, not stubbed out
    // with Unimplemented. This leader group runs no workers, so the bootstrap
    // partition has no owner to prepare child storage, and the honest refusal
    // is a well-formed request that cannot proceed — InvalidArgument, never
    // Unimplemented. A cluster with a worker splits for real; this asserts the
    // wire reaches the protocol rather than a stub.
    let mut split_reached_the_protocol = false;
    for server in [&one, &two] {
        let mut client = AdminClient::connect(format!("http://{}", server.local_addr()))
            .await
            .expect("connect to a leader voter");
        let Ok(description) = client
            .describe_cluster(DescribeClusterRequest {
                keyspace: String::new(),
            })
            .await
        else {
            continue;
        };
        let partition = description
            .into_inner()
            .partitions
            .into_iter()
            .next()
            .expect("the bootstrap partition");
        let error = client
            .split_partition(SplitPartitionRequest {
                partition_id: partition.id,
                split_key: b"m".to_vec(),
            })
            .await
            .expect_err("a partition with no owner cannot prepare child storage");
        assert_ne!(
            error.code(),
            Code::Unimplemented,
            "split is no longer stubbed; it runs the worker-prepared protocol"
        );
        assert_eq!(error.code(), Code::InvalidArgument);
        split_reached_the_protocol = true;
        break;
    }
    assert!(
        split_reached_the_protocol,
        "the control leader must run the split protocol"
    );

    two.shutdown().await.expect("the second voter stops");
    tokio::time::sleep(Duration::from_millis(100)).await;
    two = Server::start(config(NodeId(2), &dirs[1], peer_addrs[1], &peers))
        .await
        .expect("the second voter restarts from durable identity");
    wait_until_ready(&two).await;
    assert!(leader_keyspace_names(&[&one, &two])
        .await
        .contains(&"replicated".to_string()));

    one.shutdown().await.expect("the first voter stops");
    two.shutdown().await.expect("the second voter stops");
}
