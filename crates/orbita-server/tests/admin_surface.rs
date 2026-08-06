//! Who answers an admin call, over the real gRPC surface and the real TCP
//! peer transport.
//!
//! Two claims, both of which were false and both of which an operator meets in
//! their first ten minutes: a single node started the way `orbita dev` starts
//! one serves the whole admin surface itself, and a call sent to a node that
//! is not the current Raft leader still gets answered.

use orbita_core::NodeId;
use orbita_proto::v1::admin_client::AdminClient;
use orbita_proto::v1::{CreateKeyspaceRequest, DescribeClusterRequest, ListKeyspacesRequest};
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
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "orbita-admin-surface-{}-{name}",
            std::process::id()
        ));
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

async fn admin(server: &Server) -> AdminClient<tonic::transport::Channel> {
    AdminClient::connect(format!("http://{}", server.local_addr()))
        .await
        .expect("connect to the node's client port")
}

async fn wait_until_ready(server: &Server) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !server.readiness().is_ready() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the node reaches readiness");
}

/// Exactly what `orbita dev` asks for: one node that is its own leader group
/// and its own worker.
fn dev_shaped(dir: &DataDir) -> ServerConfig {
    let peer = unused_addr();
    ServerConfig::single_node(&dir.0)
        .with_node_id(NodeId(1))
        .with_listen_addr(unused_addr())
        .with_peer_listen_addr(peer)
        .with_peer_advertise_addr(peer.to_string())
        .with_peers(vec![(NodeId(1), peer.to_string())])
        .with_leader_group(vec![NodeId(1)])
        .with_leader_member(true)
        .with_leader_owns_partitions(true)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_single_node_cluster_serves_the_whole_admin_surface_itself() {
    let dir = DataDir::new("single");
    let server = Server::start(dev_shaped(&dir))
        .await
        .expect("a single node cluster starts");
    wait_until_ready(&server).await;

    let mut client = admin(&server).await;

    // A group of one still has to hold an election before anything may be
    // proposed, so this is asserted rather than assumed: without it the two
    // calls below would be testing a controller that got lucky.
    let described = client
        .describe_cluster(DescribeClusterRequest {
            keyspace: String::new(),
        })
        .await
        .expect("describe_cluster answers on a single node")
        .into_inner();
    assert_eq!(
        described
            .nodes
            .iter()
            .filter(|node| node.is_raft_leader)
            .map(|node| node.id)
            .collect::<Vec<_>>(),
        vec![1],
        "the only node has to elect itself, or nothing it is asked can be decided"
    );

    // The keyspace the dev path creates at startup, which is what makes the
    // first write work without an admin call.
    let listed = client
        .list_keyspaces(ListKeyspacesRequest {})
        .await
        .expect("list_keyspaces answers on a single node")
        .into_inner();
    assert!(
        listed.keyspaces.iter().any(|k| k.name == "default"),
        "a dev node starts with its keyspace already created, got {:?}",
        listed.keyspaces
    );

    // And the second line of the quickstart, which used to be the first thing
    // an evaluator saw fail.
    let created = client
        .create_keyspace(CreateKeyspaceRequest {
            name: "demo".to_string(),
            config: None,
        })
        .await
        .expect("create_keyspace answers on a single node")
        .into_inner();
    assert_eq!(created.name, "demo");

    server.shutdown().await.expect("the node stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_admin_call_to_a_voter_that_is_not_the_raft_leader_still_lands() {
    let dirs = [DataDir::new("voter-1"), DataDir::new("voter-2")];
    let peer_addrs = [unused_addr(), unused_addr()];
    let peers: Vec<_> = peer_addrs
        .iter()
        .enumerate()
        .map(|(offset, address)| (NodeId(offset as u64 + 1), address.to_string()))
        .collect();
    let config = |node: usize| {
        ServerConfig::single_node(&dirs[node].0)
            .with_node_id(NodeId(node as u64 + 1))
            .with_listen_addr(unused_addr())
            .with_peer_listen_addr(peer_addrs[node])
            .with_peer_advertise_addr(peer_addrs[node].to_string())
            .with_peers(peers.clone())
            .with_leader_group(peers.iter().map(|(id, _)| *id).collect())
            .with_leader_member(true)
    };

    let (one, two) = tokio::join!(Server::start(config(0)), Server::start(config(1)));
    let one = one.expect("the first voter starts");
    let two = two.expect("the second voter starts");
    wait_until_ready(&one).await;
    wait_until_ready(&two).await;

    // Which node is leader is a race between two elections, so the test finds
    // out rather than assuming, and then deliberately asks the other one.
    let leader = admin(&one)
        .await
        .describe_cluster(DescribeClusterRequest {
            keyspace: String::new(),
        })
        .await
        .expect("a voter answers describe_cluster")
        .into_inner()
        .nodes
        .into_iter()
        .find(|node| node.is_raft_leader)
        .expect("the group elected a leader")
        .id;
    let follower = if leader == 1 { &two } else { &one };

    let mut client = admin(follower).await;
    let created = client
        .create_keyspace(CreateKeyspaceRequest {
            name: "forwarded".to_string(),
            config: None,
        })
        .await
        .expect("a voter that is not the leader has to forward, not refuse")
        .into_inner();
    assert_eq!(created.name, "forwarded");

    // And a read, because the two go down different paths in the controller
    // and `cluster describe` is the call an operator reaches for first.
    let described = client
        .describe_cluster(DescribeClusterRequest {
            keyspace: String::new(),
        })
        .await
        .expect("a voter that is not the leader answers describe_cluster too")
        .into_inner();
    assert!(
        described
            .keyspaces
            .iter()
            .any(|keyspace| keyspace.name == "forwarded"),
        "the forwarded creation has to be visible in the leader's own state"
    );

    one.shutdown().await.expect("the first voter stops");
    two.shutdown().await.expect("the second voter stops");
}
