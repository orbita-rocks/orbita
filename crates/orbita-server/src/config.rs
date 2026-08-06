//! What a worker needs to know before it starts.
//!
//! The default is a single node serving one keyspace out of `./data` on
//! localhost, because a cluster that takes a configuration file to evaluate is
//! a cluster most people do not evaluate.

use crate::lease::DEFAULT_LEASE_DURATION;
use crate::map_source::{single_node_map, BoxedMapSource, StaticMapSource};
use crate::transport::DEFAULT_PEER_CALL_TIMEOUT;

use crate::aws::AssumeRoleConfig;

use orbita_core::{KeyspaceName, NodeId};
use orbita_objectstore::s3::{Credentials, S3Config};

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

/// How long acknowledged writes normally wait for cluster-wide durability.
pub const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(30);

/// How large a write-ahead log segment grows before a new one is started.
///
/// This is the granularity truncation works at, and truncation is what bounds
/// an owner's disk. A checkpoint removes whole segments, so the oldest entry
/// an owner still holds is the first entry of its oldest retained segment,
/// which makes this number the size of the window a replica may fall behind by
/// and still be caught up from the log. Until hydration lands in issue #17,
/// falling outside that window is unrecoverable, so shrinking this shrinks how
/// much lag a partition survives.
pub const DEFAULT_WAL_SEGMENT_BYTES: u64 = orbita_wal::DEFAULT_SEGMENT_TARGET_BYTES;

/// Where a node's S3 credentials come from before any role is assumed.
///
/// Every variant but [`S3CredentialSource::Default`] is a *named* source: it
/// is used, and no other is tried. A provider that falls through at request
/// time to whatever is reachable is convenient on a laptop and a liability in
/// production, because a node whose intended source is broken then authenticates
/// as something else and the first anyone hears of it is an audit log full of
/// the wrong principal.
///
/// [`S3CredentialSource::Default`] exists because "no source named" cannot mean
/// "the instance profile" without silently re-pointing every deployment that
/// used to rely on the AWS chain. See `crate::aws` for the full argument; the
/// short version is that on EKS the instance profile is a different principal
/// from the workload role, and falling through to it succeeds.
#[derive(Debug, Clone)]
pub enum S3CredentialSource {
    /// Resolve at startup, in the order the AWS SDKs document, and log which
    /// source was picked. This is what an unset configuration means.
    Default,
    /// A key pair from configuration. The only thing MinIO and R2 offer, and
    /// the wrong answer on AWS unless it is a bootstrap identity that can do
    /// nothing but assume a role.
    Static(Credentials),
    /// A key pair from `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and
    /// optionally `AWS_SESSION_TOKEN`.
    Environment,
    /// EKS IRSA: a projected OIDC token traded for a session through
    /// `sts:AssumeRoleWithWebIdentity`. This is the workload role, which is
    /// *not* the node's instance profile.
    WebIdentity,
    /// An ECS or Fargate task role, or the EKS Pod Identity agent.
    ContainerCredentials,
    /// The EC2 instance profile, read over IMDSv2. Needs no Secret at all,
    /// which is the whole point — but on EKS it is the node role, so it is not
    /// usually what a pod wants.
    InstanceProfile,
}

/// One S3-compatible bucket and the credentials used to reach it.
#[derive(Debug, Clone)]
pub struct S3StorageConfig {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    /// The base identity this node authenticates as.
    pub credentials: S3CredentialSource,
    /// A role to assume on top of the base identity. This is the preferred
    /// AWS deployment: the base is only permitted to assume, and the role
    /// carries the bucket policy, so storage access can be re-scoped without
    /// touching a single instance.
    pub assume_role: Option<AssumeRoleConfig>,
    /// Overrides the instance metadata endpoint. Unset uses the link-local
    /// address; a value here exists for tests and for the container runtimes
    /// that proxy metadata somewhere else.
    pub imds_endpoint: Option<String>,
    /// Overrides the STS endpoint used by both `AssumeRole` and the web
    /// identity exchange. Unset derives a regional endpoint from the region's
    /// partition, which is right everywhere AWS publishes one; this is for
    /// PrivateLink, for a test double, and for a partition that postdates this
    /// release.
    pub sts_endpoint: Option<String>,
    /// What this node's assumed sessions are called in CloudTrail. It is not a
    /// secret, and it is the only thing that tells two nodes apart in an audit
    /// log, so it should carry the node identity.
    pub session_name: Option<String>,
    pub force_path_style: bool,
}

impl S3StorageConfig {
    /// The `host[:port]` the instance metadata service answers on.
    pub(crate) fn imds_authority(&self) -> String {
        self.imds_endpoint
            .clone()
            .unwrap_or_else(|| crate::aws::imds::IMDS_AUTHORITY.to_string())
    }
}

impl From<S3Config> for S3StorageConfig {
    fn from(config: S3Config) -> Self {
        Self {
            endpoint: config.endpoint,
            bucket: config.bucket,
            region: config.region,
            credentials: S3CredentialSource::Static(config.credentials),
            assume_role: None,
            imds_endpoint: None,
            sts_endpoint: None,
            session_name: None,
            force_path_style: config.force_path_style,
        }
    }
}

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

    /// A shared S3-compatible store. `None` keeps the single-node filesystem
    /// adapter used by `orbita dev`; a multi-node deployment supplies this.
    pub object_store: Option<S3StorageConfig>,

    /// How often owners publish their applied writes to object storage.
    pub flush_interval: Duration,

    /// How large a write-ahead log segment grows before it is rolled.
    ///
    /// See [`DEFAULT_WAL_SEGMENT_BYTES`]: this sets how far a replica may lag
    /// before its owner can no longer catch it up from the log.
    pub wal_segment_bytes: u64,

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

    /// Whether clients must present a credential.
    ///
    /// Off by default, because a cluster that refuses its first request before
    /// anyone has issued a credential is a cluster nobody can bootstrap, and
    /// the single-node and test shapes have no control plane to issue one at
    /// all. Turning it on is an explicit operator decision (`ORBITA_REQUIRE_AUTH`)
    /// that the CLI mirrors: with it off, an unauthenticated request is allowed
    /// through rather than rejected, so the server is the one place that decides
    /// whether a credential was required. See [`crate::Server`] and the `auth`
    /// module for the enforcement path.
    pub require_auth: bool,
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
            object_store: None,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            wal_segment_bytes: DEFAULT_WAL_SEGMENT_BYTES,
            map_source: BoxedMapSource::new(StaticMapSource::new(single_node_map(
                node_id,
                &[KeyspaceName::new(DEFAULT_KEYSPACE).expect("a literal name is valid")],
            ))),
            lease_duration: DEFAULT_LEASE_DURATION,
            rng_seed: None,
            require_auth: false,
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

    /// Uses an S3-compatible bucket instead of the single-node filesystem.
    #[must_use]
    pub fn with_object_store(mut self, config: impl Into<S3StorageConfig>) -> Self {
        self.object_store = Some(config.into());
        self
    }

    #[must_use]
    pub fn with_flush_interval(mut self, interval: Duration) -> Self {
        self.flush_interval = interval;
        self
    }

    /// Sets the write-ahead log segment size, and with it how much replication
    /// lag a partition can absorb before a replica falls off the retention
    /// horizon.
    #[must_use]
    pub fn with_wal_segment_bytes(mut self, bytes: u64) -> Self {
        self.wal_segment_bytes = bytes;
        self
    }

    #[must_use]
    pub fn with_lease_duration(mut self, duration: Duration) -> Self {
        self.lease_duration = duration;
        self
    }

    /// Turns credential enforcement on, so every client request must carry a
    /// valid `authorization: Bearer <secret>` header.
    #[must_use]
    pub fn with_require_auth(mut self, require_auth: bool) -> Self {
        self.require_auth = require_auth;
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
