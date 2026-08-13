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

/// How often an owner checks whether a partition has earned a compaction.
///
/// Deliberately slower than a flush, because compaction only becomes worth
/// doing after many flushes and it takes the partition's write lock while it
/// runs. Checking is cheap -- it reads two counters under the lock and returns
/// -- so this cadence sets how promptly a partition that has crossed the
/// threshold gets serviced, not how often work happens.
pub const DEFAULT_COMPACT_INTERVAL: Duration = Duration::from_secs(60);

/// How often an owner sweeps its partitions for orphaned objects.
///
/// The sweep lists a partition's whole prefix, so it is deliberately far rarer
/// than a flush: orphans are produced only by a compaction or commit that
/// failed to clean up after itself, which is rare, and an object stranded for
/// an extra few minutes costs only its own storage. Ten minutes reclaims
/// promptly on a healthy cluster without turning the sweep's listing into a
/// standing tax on the object store.
pub const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(10 * 60);

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

/// How much of a node's memory holds records read back out of segments.
///
/// A flat number rather than a fraction of the machine, because the engine
/// cannot see the machine and a container's limit is not the host's memory.
/// It is deliberately modest: the index is already resident and is the
/// allocation that must not be squeezed, since losing it costs a rebuild from
/// object storage while losing a cached value costs one fetch.
///
/// Large enough to matter, though. The working sets this was measured against
/// were 5.7 MiB and 228 MiB, so this holds either outright, and a node that
/// wants more should be told rather than guessed at.
pub const DEFAULT_VALUE_CACHE_BYTES: u64 = 256 * 1024 * 1024;

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
    /// How many peer acknowledgements a write requires, independent of how many
    /// peers hold the partition.
    ///
    /// One, together with the owner, is the two-of-three the WAL has always
    /// required. Peers beyond that follow the stream for freshness and serve
    /// reads without being able to hold a write up, which is what lets read
    /// capacity grow without buying it in write latency. See ADR 0013.
    pub durability_acks: usize,

    /// How many replicas a partition keeps so they can serve reads. `None`
    /// leaves the control plane at its durability floor. See ADR 0013.
    pub read_replica_target: Option<usize>,

    /// Bytes of records read back out of segments this node will hold in
    /// memory, shared across every partition it hosts.
    ///
    /// ADR 0006 decided values are cached rather than resident, which is what
    /// lets a partition hold more than a node's memory. Without a cache every
    /// read of a flushed key is an object-store round trip, and a measured
    /// cluster never answered one faster than 14.4ms.
    ///
    /// Shared rather than per partition because a node holding thousands of
    /// partitions would otherwise have thousands of budgets and no bound. Zero
    /// turns caching off and restores the previous behaviour exactly.
    pub value_cache_bytes: u64,

    pub leader_member: bool,

    /// Whether a leader group member also owns partitions.
    ///
    /// False in production, because the control plane is deliberately off the
    /// data path: a member that was also compacting a partition would put
    /// storage work in front of an election. `orbita dev` is the one exception
    /// and is documented as one, being "one node that is its own leader group
    /// and its own worker" — and a single-node cluster whose only node cannot
    /// own a partition serves nothing at all.
    ///
    /// It reads as a role to the control plane, which admits only workers as
    /// owners. That check stays where it is: relaxing it for a group of one
    /// would put the rule at the mercy of how many nodes happened to be up.
    pub leader_owns_partitions: bool,

    /// Whether clustered startup discovers its initial voters through the
    /// object-store bootstrap certificate from ADR 0011.
    pub automatic_cluster: bool,

    /// The object-store namespace used to locate the durable cluster identity.
    pub cluster_name: String,

    /// The desired Raft voter count. Only three and five are supported.
    pub voter_target: usize,

    /// Whether this node may be selected for the voter set.
    pub voter_eligible: bool,

    /// The operator-supplied failure domain used to spread voters.
    pub failure_domain: String,

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

    /// How often owners check whether a partition has earned a compaction.
    pub compact_interval: Duration,

    /// Whether the orphan sweep runs at all.
    ///
    /// Off by default, and deliberately so: the sweep deletes objects from the
    /// bucket, and a destructive background loop that turns itself on the moment
    /// a node starts is the kind of default that reclaims the wrong thing on
    /// somebody's production data before they knew it existed. An operator opts
    /// in (`ORBITA_SWEEP_ENABLED=true`) once they have set a grace period they
    /// trust, and is expected to preview with [`sweep_dry_run`] first. Leaving
    /// it off costs only leaked space, which is recoverable; leaving it on by
    /// default could cost data, which is not.
    ///
    /// [`sweep_dry_run`]: ServerConfig::sweep_dry_run
    pub sweep_enabled: bool,

    /// How often an owner runs the orphan sweep over the partitions it holds.
    ///
    /// The sweep reclaims objects a failed compaction or an abandoned commit
    /// stranded; without it they accumulate in the bucket forever. See
    /// [`DEFAULT_SWEEP_INTERVAL`] for why this cadence is far slower than the
    /// flush.
    pub sweep_interval: Duration,

    /// How long the orphan sweep leaves an unreferenced object alone before it
    /// may delete it, in the object store's own clock domain.
    ///
    /// This bound is the safety of the sweep: it has to exceed the longest read
    /// a client can hold open *and* the longest commit a writer can be part way
    /// through, because both hold references to objects that are unreferenced by
    /// the current manifest. See
    /// [`orbita_storage::DEFAULT_SWEEP_GRACE_MILLIS`] for the full argument. A
    /// deployment whose reads or commits run longer than the default raises it.
    pub sweep_grace_millis: u64,

    /// How much the sweep widens the grace period to absorb the object store's
    /// own worst-case internal clock skew.
    ///
    /// Even one backend can stamp two objects from servers whose clocks
    /// disagree, so the sweep requires an object to clear the grace period *and*
    /// this allowance before it is touched. See
    /// [`orbita_storage::DEFAULT_SWEEP_SKEW_MILLIS`].
    pub sweep_skew_millis: u64,

    /// Whether the sweep only reports what it would delete instead of deleting.
    ///
    /// A safety valve for the first runs against a real bucket: with it on, the
    /// sweep logs the objects it *would* reclaim and touches nothing, so an
    /// operator can confirm the grace period is set right before trusting it to
    /// delete. Off by default, because a sweep that never deletes never solves
    /// the problem it exists for — objects accumulating forever.
    pub sweep_dry_run: bool,

    /// How large a write-ahead log segment grows before it is rolled.
    ///
    /// See [`DEFAULT_WAL_SEGMENT_BYTES`]: this sets how far a replica may lag
    /// before its owner can no longer catch it up from the log.
    pub wal_segment_bytes: u64,

    /// Where the partition map comes from. A single node uses a static map; a
    /// real cluster will use an adapter over the control plane.
    pub map_source: BoxedMapSource,

    /// The keyspaces that exist the moment this cluster is first created.
    ///
    /// Kept beside [`ServerConfig::map_source`] rather than folded into it
    /// because the two answer the same question for different cluster shapes,
    /// and a node can now be both: with a control plane the map is fetched and
    /// this is what the very first bootstrap writes into it, while without one
    /// the static map is the whole answer. Both are set by
    /// [`ServerConfig::with_keyspaces`], so they cannot drift apart and leave
    /// `orbita dev --keyspace demo` serving a keyspace called something else.
    pub keyspaces: Vec<KeyspaceName>,

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

    /// A bootstrap root credential, supplied whole as its plaintext secret.
    ///
    /// This exists to resolve the bootstrap chicken-and-egg: with
    /// [`Self::require_auth`] on, the admin surface itself demands a
    /// write-capable credential, but the first credential is created *through*
    /// admin, so a cluster turning auth on has no way to create its first one.
    /// An operator names a root secret here (`ORBITA_ROOT_CREDENTIAL`); the
    /// server hashes it at startup and overlays the hash onto enforcement, so a
    /// request bearing it is authorized as a fully privileged, all-keyspaces,
    /// write-capable, non-expiring identity before any credential exists in the
    /// log, and can then create the first real one.
    ///
    /// It is a config secret with total blast radius. It is never written to
    /// the replicated log and never logged. It is the operator's job to rotate
    /// it and to remove it once real credentials exist: it is a bootstrap key,
    /// not a standing one. `None` — the default — means no root, and a cluster
    /// with auth on and no root must create its first credential while auth is
    /// off.
    pub root_credential: Option<String>,
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
            durability_acks: 1,
            read_replica_target: None,
            value_cache_bytes: DEFAULT_VALUE_CACHE_BYTES,
            leader_member: false,
            leader_owns_partitions: false,
            automatic_cluster: false,
            cluster_name: "orbita".to_string(),
            voter_target: 3,
            voter_eligible: true,
            failure_domain: String::new(),
            control_poll_interval: DEFAULT_CONTROL_POLL_INTERVAL,
            data_dir: PathBuf::from("data"),
            object_store: None,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            compact_interval: DEFAULT_COMPACT_INTERVAL,
            sweep_enabled: false,
            sweep_interval: DEFAULT_SWEEP_INTERVAL,
            sweep_grace_millis: orbita_storage::DEFAULT_SWEEP_GRACE_MILLIS,
            sweep_skew_millis: orbita_storage::DEFAULT_SWEEP_SKEW_MILLIS,
            sweep_dry_run: false,
            wal_segment_bytes: DEFAULT_WAL_SEGMENT_BYTES,
            map_source: BoxedMapSource::new(StaticMapSource::new(single_node_map(
                node_id,
                &[KeyspaceName::new(DEFAULT_KEYSPACE).expect("a literal name is valid")],
            ))),
            keyspaces: vec![KeyspaceName::new(DEFAULT_KEYSPACE).expect("a literal name is valid")],
            lease_duration: DEFAULT_LEASE_DURATION,
            rng_seed: None,
            require_auth: false,
            root_credential: None,
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

    /// Replaces the keyspaces this server starts life with.
    ///
    /// For a node with no control plane that is the whole map, forever. For
    /// one that has a control plane, it is what the first bootstrap creates
    /// and the control plane owns them from then on, so this is a starting
    /// condition rather than a standing configuration: a keyspace created or
    /// deleted through the admin API afterwards is not affected by it.
    #[must_use]
    pub fn with_keyspaces(mut self, names: &[KeyspaceName]) -> Self {
        self.map_source =
            BoxedMapSource::new(StaticMapSource::new(single_node_map(self.node_id, names)));
        self.keyspaces = names.to_vec();
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

    /// Lets this leader group member own partitions too, which is what makes
    /// `orbita dev` a whole cluster in one process.
    #[must_use]
    pub fn with_leader_owns_partitions(mut self, owns: bool) -> Self {
        self.leader_owns_partitions = owns;
        self
    }

    /// Sets how many replicas a partition keeps for serving reads.
    #[must_use]
    pub fn with_read_replica_target(mut self, target: Option<usize>) -> Self {
        self.read_replica_target = target;
        self
    }

    /// Sets how much memory this node holds records read out of segments in.
    ///
    /// Zero is meaningful and supported: it turns caching off and restores the
    /// behaviour of every release before ADR 0006's cache existed, which is
    /// what an operator reaches for when they suspect it.
    #[must_use]
    pub fn with_value_cache_bytes(mut self, bytes: u64) -> Self {
        self.value_cache_bytes = bytes;
        self
    }

    /// Selects combined-node bootstrap. The object store fixes the identity
    /// and initial voter certificate before Raft starts.
    #[must_use]
    pub fn with_automatic_cluster(
        mut self,
        cluster_name: impl Into<String>,
        voter_target: usize,
        voter_eligible: bool,
        failure_domain: impl Into<String>,
    ) -> Self {
        self.automatic_cluster = true;
        self.cluster_name = cluster_name.into();
        self.voter_target = voter_target;
        self.voter_eligible = voter_eligible;
        self.failure_domain = failure_domain.into();
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
    pub fn with_compact_interval(mut self, interval: Duration) -> Self {
        self.compact_interval = interval;
        self
    }

    #[must_use]
    pub fn with_flush_interval(mut self, interval: Duration) -> Self {
        self.flush_interval = interval;
        self
    }

    /// Turns the orphan sweep on. Off by default; see
    /// [`ServerConfig::sweep_enabled`] for why a destructive loop does not start
    /// itself.
    #[must_use]
    pub fn with_sweep_enabled(mut self, enabled: bool) -> Self {
        self.sweep_enabled = enabled;
        self
    }

    /// Sets how often an owner sweeps its partitions for orphaned objects.
    #[must_use]
    pub fn with_sweep_interval(mut self, interval: Duration) -> Self {
        self.sweep_interval = interval;
        self
    }

    /// Sets the orphan sweep's grace period and skew allowance, in the object
    /// store's clock domain. See [`ServerConfig::sweep_grace_millis`].
    #[must_use]
    pub fn with_sweep_bounds(mut self, grace_millis: u64, skew_millis: u64) -> Self {
        self.sweep_grace_millis = grace_millis;
        self.sweep_skew_millis = skew_millis;
        self
    }

    /// Puts the orphan sweep in report-only mode, so it names what it would
    /// delete without deleting. See [`ServerConfig::sweep_dry_run`].
    #[must_use]
    pub fn with_sweep_dry_run(mut self, dry_run: bool) -> Self {
        self.sweep_dry_run = dry_run;
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

    /// Sets the bootstrap root credential, as its plaintext secret.
    ///
    /// See [`ServerConfig::root_credential`]: the secret is hashed at startup
    /// and overlaid onto enforcement so it can bootstrap a cluster whose auth
    /// is on before any credential exists.
    #[must_use]
    pub fn with_root_credential(mut self, root_credential: Option<String>) -> Self {
        self.root_credential = root_credential;
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

    #[test]
    fn the_orphan_sweep_is_off_in_a_default_configuration() {
        // The sweep deletes from the bucket, so a default configuration must not
        // run it: an operator opts in once the grace period is set to something
        // this deployment can stand behind.
        let config = ServerConfig::default();
        assert!(!config.sweep_enabled, "the sweep does not start itself");
        assert!(!config.sweep_dry_run);
        assert_eq!(
            config.sweep_grace_millis,
            orbita_storage::DEFAULT_SWEEP_GRACE_MILLIS
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
