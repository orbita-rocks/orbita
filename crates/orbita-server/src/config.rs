//! What a worker needs to know before it starts.
//!
//! The default is a single node serving one keyspace out of `./data` on
//! localhost, because a cluster that takes a configuration file to evaluate is
//! a cluster most people do not evaluate.

use crate::lease::DEFAULT_LEASE_DURATION;
use crate::map_source::{single_node_map, BoxedMapSource, StaticMapSource};
use crate::transport::DEFAULT_PEER_CALL_TIMEOUT;

use orbita_core::{KeyspaceName, NodeId};

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

/// The keyspace a server creates when nothing else is configured, so that a
/// first request works without an admin call.
pub const DEFAULT_KEYSPACE: &str = "default";

/// How often a worker talks to the leader group by default.
///
/// This matches the control plane's own heartbeat interval, which is chosen so
/// that twelve reports have to go missing before a node is declared dead.
pub const DEFAULT_CONTROL_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// One worker's configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// This node's identity in the cluster. It appears in the partition map,
    /// so it has to match what the map says about who owns what.
    pub node_id: NodeId,

    /// Where clients connect. Port zero binds an arbitrary free port, which is
    /// what tests use, and [`crate::Server::local_addr`] reports what it got.
    pub listen_addr: SocketAddr,

    /// Where peers connect, which is a separate listener from the client one.
    ///
    /// ADR 0004 keeps the two apart because an operator wants the peer port on
    /// a private network and the client port exposed, and because a peer port
    /// reachable from the internet is a hole.
    pub peer_listen_addr: SocketAddr,

    /// The stable address this node publishes to peers. This differs from the
    /// bind address in containers, where the listener uses a wildcard and the
    /// advertised address is the StatefulSet DNS name.
    pub peer_advertise_addr: Option<String>,

    /// Where this node reaches every other node it might talk to.
    ///
    /// Configuration rather than discovery: finding a peer's address requires
    /// asking something, and everything worth asking is itself reached over
    /// the peer transport.
    pub peers: Vec<(NodeId, String)>,

    /// How long a call to a peer waits before it is treated as failed.
    pub peer_call_timeout: Duration,

    /// The leader group this node belongs to.
    ///
    /// Empty means there is no control plane, which is the single-node and
    /// test case: the map comes from [`ServerConfig::map_source`] and never
    /// changes. Non-empty replaces the map source with one backed by the
    /// leader group and starts the heartbeat that failover depends on.
    pub leader_group: Vec<NodeId>,

    /// Whether this node is one of the fixed voters and therefore hosts the
    /// replicated control plane rather than only consuming it.
    pub leader_member: bool,

    /// How often this node refetches the map and reports its own progress.
    ///
    /// One timer for both because they answer each other: the report says how
    /// far this node has got, and the fetch is how it learns that the answer
    /// moved a partition to or from it. It has to be well inside the control
    /// plane's death declaration or this node is failed over while healthy.
    pub control_poll_interval: Duration,

    /// The root of everything this node writes: the write-ahead log under
    /// `wal/` and the storage engine under `storage/`.
    pub data_dir: PathBuf,

    /// Where the partition map comes from. A single node uses a static map; a
    /// real cluster will use an adapter over the control plane.
    pub map_source: BoxedMapSource,

    /// How long a read lease granted to a replica lasts.
    ///
    /// Too short and heartbeat traffic climbs and replicas flap out of the
    /// read set; too long and a partition stalls writes for that long when a
    /// replica goes quiet. ADR 0001 says start at 500ms and measure.
    pub lease_duration: Duration,

    /// Pins the node's randomness so that a production run can be replayed
    /// with the same jitter decisions. Unset draws one at startup.
    pub rng_seed: Option<u64>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        let node_id = NodeId(1);
        Self {
            node_id,
            listen_addr: "127.0.0.1:7379".parse().expect("a literal address parses"),
            peer_listen_addr: "127.0.0.1:7380".parse().expect("a literal address parses"),
            peer_advertise_addr: None,
            peers: Vec::new(),
            peer_call_timeout: DEFAULT_PEER_CALL_TIMEOUT,
            leader_group: Vec::new(),
            leader_member: false,
            control_poll_interval: DEFAULT_CONTROL_POLL_INTERVAL,
            data_dir: PathBuf::from("data"),
            map_source: BoxedMapSource::new(StaticMapSource::new(single_node_map(
                node_id,
                &[KeyspaceName::new(DEFAULT_KEYSPACE).expect("a literal name is valid")],
            ))),
            lease_duration: DEFAULT_LEASE_DURATION,
            rng_seed: None,
        }
    }
}

impl ServerConfig {
    /// A single-node server with everything under `data_dir` and one keyspace.
    ///
    /// This is the shape of a laptop cluster and of most tests: one node owns
    /// every partition, so nothing is ever forwarded and no control plane has
    /// to exist.
    #[must_use]
    pub fn single_node(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            ..Default::default()
        }
    }

    /// Replaces the keyspaces a single-node server serves.
    ///
    /// Keyspace creation belongs to the admin API and the control plane. Until
    /// that exists, this is how a test or a laptop cluster gets more than one.
    #[must_use]
    pub fn with_keyspaces(mut self, names: &[KeyspaceName]) -> Self {
        self.map_source =
            BoxedMapSource::new(StaticMapSource::new(single_node_map(self.node_id, names)));
        self
    }

    #[must_use]
    pub fn with_listen_addr(mut self, addr: SocketAddr) -> Self {
        self.listen_addr = addr;
        self
    }

    #[must_use]
    pub fn with_node_id(mut self, node_id: NodeId) -> Self {
        self.node_id = node_id;
        self
    }

    #[must_use]
    pub fn with_peer_listen_addr(mut self, addr: SocketAddr) -> Self {
        self.peer_listen_addr = addr;
        self
    }

    #[must_use]
    pub fn with_peer_advertise_addr(mut self, addr: impl Into<String>) -> Self {
        self.peer_advertise_addr = Some(addr.into());
        self
    }

    /// Where this node reaches its peers, as pairs of node id and address.
    #[must_use]
    pub fn with_peers(mut self, peers: Vec<(NodeId, String)>) -> Self {
        self.peers = peers;
        self
    }

    /// Where the partition map comes from, for a node joined to a real
    /// cluster.
    #[must_use]
    pub fn with_map_source(mut self, source: BoxedMapSource) -> Self {
        self.map_source = source;
        self
    }

    /// Joins this node to a leader group, which is what makes its map come
    /// from the control plane and its failures visible to failover.
    #[must_use]
    pub fn with_leader_group(mut self, members: Vec<NodeId>) -> Self {
        self.leader_group = members;
        self
    }

    /// Hosts the fixed leader group on this node.
    #[must_use]
    pub fn with_leader_member(mut self, leader_member: bool) -> Self {
        self.leader_member = leader_member;
        self
    }

    #[must_use]
    pub fn with_control_poll_interval(mut self, interval: Duration) -> Self {
        self.control_poll_interval = interval;
        self
    }

    #[must_use]
    pub fn with_lease_duration(mut self, duration: Duration) -> Self {
        self.lease_duration = duration;
        self
    }

    /// The addresses to bind when the caller wants the operating system to
    /// choose both ports.
    #[must_use]
    pub fn on_ephemeral_port(self) -> Self {
        let ephemeral: SocketAddr = "127.0.0.1:0".parse().expect("a literal address parses");
        self.with_listen_addr(ephemeral)
            .with_peer_listen_addr(ephemeral)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::map_source::MapSource;

    #[tokio::test]
    async fn the_default_server_needs_no_configuration_to_be_useful() {
        let config = ServerConfig::default();
        let map = config.map_source.fetch().await.unwrap();

        assert_eq!(map.check_coverage(), Ok(()));
        let keyspace = map
            .keyspace_by_name(DEFAULT_KEYSPACE)
            .expect("a fresh server serves one keyspace");
        assert_eq!(
            map.lookup(keyspace.id, b"anything").unwrap().owner,
            Some(config.node_id),
            "the only node owns everything"
        );
    }

    #[tokio::test]
    async fn extra_keyspaces_each_get_their_own_partition() {
        let names = [
            KeyspaceName::new("catalog").unwrap(),
            KeyspaceName::new("locks").unwrap(),
        ];
        let config = ServerConfig::single_node("data").with_keyspaces(&names);
        let map = config.map_source.fetch().await.unwrap();

        assert_eq!(map.len(), 2);
        assert_eq!(map.check_coverage(), Ok(()));
    }
}
