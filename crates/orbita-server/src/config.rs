//! What a worker needs to know before it starts.
//!
//! The default is a single node serving one keyspace out of `./data` on
//! localhost, because a cluster that takes a configuration file to evaluate is
//! a cluster most people do not evaluate.

use crate::lease::DEFAULT_LEASE_DURATION;
use crate::map_source::{single_node_map, BoxedMapSource, StaticMapSource};

use orbita_core::{KeyspaceName, NodeId};

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

/// The keyspace a server creates when nothing else is configured, so that a
/// first request works without an admin call.
pub const DEFAULT_KEYSPACE: &str = "default";

/// One worker's configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// This node's identity in the cluster. It appears in the partition map,
    /// so it has to match what the map says about who owns what.
    pub node_id: NodeId,

    /// Where clients connect. Port zero binds an arbitrary free port, which is
    /// what tests use, and [`crate::Server::local_addr`] reports what it got.
    pub listen_addr: SocketAddr,

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

    /// The address to bind when the caller wants the operating system to
    /// choose the port.
    #[must_use]
    pub fn on_ephemeral_port(self) -> Self {
        self.with_listen_addr("127.0.0.1:0".parse().expect("a literal address parses"))
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
