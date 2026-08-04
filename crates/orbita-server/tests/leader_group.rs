//! Fixed leader voters through the production server and TCP transport.

use orbita_core::NodeId;
use orbita_proto::v1::admin_client::AdminClient;
use orbita_proto::v1::{CreateKeyspaceRequest, ListKeyspacesRequest};
use orbita_server::{Server, ServerConfig};

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

async fn keyspace_names(server: &Server) -> Vec<String> {
    let mut client = AdminClient::connect(format!("http://{}", server.local_addr()))
        .await
        .expect("connect to the Admin service");
    client
        .list_keyspaces(ListKeyspacesRequest {})
        .await
        .expect("list keyspaces")
        .into_inner()
        .keyspaces
        .into_iter()
        .map(|keyspace| keyspace.name)
        .collect()
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
    assert!(keyspace_names(&one)
        .await
        .contains(&"replicated".to_string()));
    assert!(keyspace_names(&two)
        .await
        .contains(&"replicated".to_string()));

    two.shutdown().await.expect("the second voter stops");
    tokio::time::sleep(Duration::from_millis(100)).await;
    two = Server::start(config(NodeId(2), &dirs[1], peer_addrs[1], &peers))
        .await
        .expect("the second voter restarts from durable identity");
    assert!(keyspace_names(&two)
        .await
        .contains(&"replicated".to_string()));

    one.shutdown().await.expect("the first voter stops");
    two.shutdown().await.expect("the second voter stops");
}
