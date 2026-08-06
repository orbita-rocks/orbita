//! Configuration, layered from four sources.
//!
//! # Why TOML and not YAML
//!
//! The file format is TOML. Rust tooling already speaks it, the parser is a
//! dependency this workspace would carry anyway, and its scalar typing is
//! unambiguous, so `listen = "0.0.0.0:7100"` cannot quietly become a sexagesimal
//! number the way it can in YAML 1.1. The Kubernetes audience does write YAML,
//! but they do not hand-write a node configuration file: the Helm chart renders
//! it into a ConfigMap for them, and the values file they actually edit is YAML
//! either way. Supporting both formats would mean two parsers, two sets of
//! examples, and two sets of bug reports for no capability we do not already
//! have, so we support one. Anyone who needs YAML can set environment
//! variables instead, which is the escape hatch that costs nothing because it
//! already exists for containers.
//!
//! # Precedence
//!
//! Later sources win over earlier ones:
//!
//! 1. Built-in defaults. Every option has one, which is what lets `orbita dev`
//!    run with no file and no environment at all.
//! 2. The configuration file, from `--config`, then `ORBITA_CONFIG`, then
//!    `./orbita.toml`, then `/etc/orbita/orbita.toml`. The first one that
//!    exists is used. A path given explicitly must exist; a default path that
//!    does not exist is not an error.
//! 3. Environment variables, listed in [`ENVIRONMENT`]. These exist so a
//!    container image can be configured without a mounted file.
//! 4. Command line flags.
//!
//! The order is flag beats environment beats file beats default. It is that
//! way round because it matches how an operator debugs: the file is the
//! deployed state, the environment is what the orchestrator injected, and the
//! flag is what the human typed just now to test a theory. The most immediate
//! intent wins.
//!
//! Precedence is per option, not per section. Setting `ORBITA_LISTEN` does not
//! discard the rest of the `[node]` table from the file.

use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

/// Every environment variable the tool reads, paired with the option it sets.
///
/// This is a table rather than scattered `std::env::var` calls so that
/// `orbita config env` can print it and the documentation cannot drift from
/// the code.
pub const ENVIRONMENT: &[(&str, &str)] = &[
    ("ORBITA_CONFIG", "path to the configuration file"),
    ("ORBITA_NODE_ID", "node.id"),
    ("ORBITA_NODE_ROLE", "node.role, one of leader or worker"),
    ("ORBITA_LISTEN", "node.listen"),
    ("ORBITA_ADVERTISE", "node.advertise"),
    ("ORBITA_PEER_LISTEN", "node.peer_listen"),
    ("ORBITA_PEER_ADVERTISE", "node.peer_advertise"),
    ("ORBITA_DATA_DIR", "node.data_dir"),
    ("ORBITA_CLUSTER_NAME", "cluster.name"),
    (
        "ORBITA_LEADER_PEERS",
        "cluster.leader_peers, comma separated NODE_ID=ADDR entries",
    ),
    ("ORBITA_ALLOW_VERSION_SKEW", "cluster.allow_version_skew"),
    ("ORBITA_REQUIRE_AUTH", "cluster.require_auth"),
    ("ORBITA_ROOT_CREDENTIAL", "cluster.root_credential"),
    (
        "ORBITA_JOIN_BACKOFF_INITIAL",
        "cluster.join_backoff_initial, a duration such as 250ms",
    ),
    (
        "ORBITA_JOIN_BACKOFF_MAX",
        "cluster.join_backoff_max, a duration such as 10s",
    ),
    (
        "ORBITA_JOIN_TIMEOUT",
        "cluster.join_timeout, a duration, or 0 to retry forever",
    ),
    (
        "ORBITA_DRAIN_TIMEOUT",
        "cluster.drain_timeout, bounded by the orchestrator grace period",
    ),
    ("ORBITA_OBJECT_STORE_ENDPOINT", "object_store.endpoint"),
    ("ORBITA_OBJECT_STORE_BUCKET", "object_store.bucket"),
    ("ORBITA_OBJECT_STORE_REGION", "object_store.region"),
    (
        "ORBITA_OBJECT_STORE_ACCESS_KEY_ID",
        "object_store.access_key_id",
    ),
    (
        "ORBITA_OBJECT_STORE_SECRET_ACCESS_KEY",
        "object_store.secret_access_key",
    ),
    (
        "ORBITA_OBJECT_STORE_CREDENTIAL_SOURCE",
        "object_store.credential_source: default, static, environment, \
         web-identity, container, or instance-profile",
    ),
    ("ORBITA_OBJECT_STORE_ROLE_ARN", "object_store.role_arn"),
    (
        "ORBITA_OBJECT_STORE_ROLE_EXTERNAL_ID",
        "object_store.role_external_id",
    ),
    (
        "ORBITA_OBJECT_STORE_ROLE_SESSION_NAME",
        "object_store.role_session_name",
    ),
    (
        "ORBITA_OBJECT_STORE_STS_ENDPOINT",
        "object_store.sts_endpoint, overriding the partition-derived one",
    ),
    (
        "ORBITA_OBJECT_STORE_FORCE_PATH_STYLE",
        "object_store.force_path_style",
    ),
    ("ORBITA_OTLP_ENDPOINT", "telemetry.otlp_endpoint"),
    ("ORBITA_TRACE_SAMPLE_RATIO", "telemetry.trace_sample_ratio"),
    ("ORBITA_SERVICE_NAME", "telemetry.service_name"),
    ("ORBITA_LOG_LEVEL", "telemetry.log_level"),
    (
        "ORBITA_RESOURCE_ATTRIBUTES",
        "telemetry.resource_attributes, as key=value pairs separated by commas",
    ),
    ("ORBITA_ENDPOINT", "client.endpoint"),
    ("ORBITA_CREDENTIAL", "client.credential"),
];

/// Where the tool looks for a configuration file when it was not told.
///
/// Ordered from most specific to least, so a file next to the working
/// directory beats a system-wide one.
pub const DEFAULT_CONFIG_PATHS: &[&str] = &["orbita.toml", "/etc/orbita/orbita.toml"];

/// The port a node listens on for client and admin gRPC traffic.
///
/// This is the port that gets exposed. It speaks a versioned, published
/// protocol, and it is the only thing a program outside the cluster ever needs
/// to reach.
pub const DEFAULT_PORT: u16 = 7100;

/// The port a node listens on for traffic from other nodes.
///
/// Peer traffic is WAL replication, control plane messages, and proxied client
/// requests, in a private length-prefixed framing rather than gRPC. See
/// `docs/adr/0004-peer-traffic-uses-private-framing.md`. It is a separate
/// listener from the client port so that an operator can put it on a private
/// network, which is where it belongs: the framing carries no authentication
/// of its own, nodes of different versions must not speak it, and a peer port
/// reachable from the internet is a hole rather than a feature.
pub const DEFAULT_PEER_PORT: u16 = 7101;

/// Which half of the cluster this node belongs to.
///
/// The same binary runs both, so this is the only thing that distinguishes a
/// leader group member from a worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
#[clap(rename_all = "lowercase")]
pub enum Role {
    /// Runs Raft, owns the partition map, keyspace metadata, and failover.
    Leader,
    /// Owns partitions and serves client reads and writes.
    Worker,
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Leader => f.write_str("leader"),
            Self::Worker => f.write_str("worker"),
        }
    }
}

/// A configuration with every option resolved.
///
/// Commands take this rather than the partially populated [`Layer`] so that no
/// code past resolution has to decide what an absent option means.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Config {
    pub node: NodeConfig,
    pub cluster: ClusterConfig,
    pub object_store: ObjectStoreConfig,
    pub telemetry: TelemetryConfig,
    pub client: ClientConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct NodeConfig {
    pub id: u64,
    pub role: Role,
    /// Where client and admin gRPC traffic is served.
    pub listen: String,
    /// The address clients should use to reach this one. Inside a
    /// container `listen` is usually `0.0.0.0`, which nothing can dial, so
    /// this defaults to `listen` and has to be set in any real deployment.
    pub advertise: String,
    /// Where peer traffic is served. Bind this to a private interface where
    /// there is one, because nothing outside the cluster should be able to
    /// open a peer connection.
    pub peer_listen: String,
    /// The address other nodes should dial to reach this one. It goes in
    /// `cluster.leader_peers` on every node, so it has to be a name every peer
    /// resolves to the same thing.
    pub peer_advertise: String,
    pub data_dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ClusterConfig {
    pub name: String,
    /// The leader group, as `NODE_ID=ADDR` entries.
    ///
    /// On a leader this is the initial Raft membership, identical on every
    /// leader node. The voter IDs are checked against the durable Raft
    /// identity on restart, so a changed template cannot redefine a cluster.
    /// On a worker it is
    /// the list of leaders to contact in order to register and be told the
    /// partition map. One list rather than two, because they are the same
    /// addresses and an operator keeping two lists in sync will not.
    /// See [`crate::node`] for why.
    pub leader_peers: Vec<String>,
    pub allow_version_skew: bool,
    /// Whether this cluster requires a credential on every client request.
    ///
    /// Off by default so a fresh cluster can be brought up and its first
    /// credential issued; on, every `Kv` and `Admin` call must carry a valid
    /// `authorization: Bearer <secret>` header. This is the server-side switch
    /// the CLI's own credential handling assumes exists.
    pub require_auth: bool,
    /// A bootstrap root credential secret, if the operator configured one.
    ///
    /// This is what resolves the bootstrap chicken-and-egg: with
    /// `require_auth` on, the admin surface itself demands a credential, but
    /// the first credential is created through admin. A root secret named here
    /// is hashed by the server and honored as a fully privileged identity
    /// before any credential exists, so it can create the first real one.
    ///
    /// It is a config secret with total blast radius: it is never serialized
    /// back out (see the skipped field below) and it is the operator's job to
    /// rotate it and remove it once real credentials exist. `None` means no
    /// root, and a cluster with auth on and no root must create its first
    /// credential while auth is off.
    #[serde(skip_serializing)]
    pub root_credential: Option<String>,
    /// How long to wait before the first retry when the leader group is not
    /// reachable yet.
    pub join_backoff_initial_millis: u64,
    /// The ceiling on that wait. Backoff doubles up to here and stops growing,
    /// so a leader group that comes up after ten minutes is still found within
    /// this long of coming up.
    pub join_backoff_max_millis: u64,
    /// How long to keep trying before giving up and exiting. Zero means retry
    /// forever.
    pub join_timeout_millis: u64,
    /// The process-side handoff budget. Kubernetes should allow a little more
    /// than this before SIGKILL so the timeout is reported clearly.
    pub drain_timeout_millis: u64,
}

/// Where a node's S3 credentials come from.
///
/// Every value but [`CredentialSource::Default`] is named rather than
/// discovered, and a named source is used and no other is tried. A provider
/// chain that falls through at request time is convenient on a laptop and a
/// liability in production: a node whose intended source is broken authenticates
/// as whatever else is lying around, and the first anyone hears of it is an
/// audit log full of the wrong principal.
///
/// [`CredentialSource::Default`] is what an unset value means. It resolves the
/// AWS chain's order once, at startup, and logs which source it picked. It
/// exists because mapping "no keys configured" straight to `InstanceProfile`
/// silently re-points every EKS deployment from its workload role to its node
/// role, which succeeds rather than failing and is therefore worse than an
/// outage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CredentialSource {
    /// Resolve at startup: environment, then web identity, then container,
    /// then instance profile. A shared `~/.aws` profile is an error rather
    /// than a step that gets skipped.
    Default,
    /// `access_key_id` and `secret_access_key` from configuration. What MinIO
    /// and R2 need, and the only thing they offer.
    Static,
    /// `AWS_ACCESS_KEY_ID` and friends from the process environment.
    Environment,
    /// EKS IRSA: a projected OIDC token traded for a session on the workload
    /// role. Not the same principal as the node's instance profile.
    WebIdentity,
    /// An ECS or Fargate task role, or the EKS Pod Identity agent.
    Container,
    /// The EC2 instance profile, over IMDSv2. Needs no Secret at all.
    InstanceProfile,
}

impl CredentialSource {
    /// Whether this source reads `access_key_id` and `secret_access_key`.
    ///
    /// One predicate rather than a `==` at each site, so validation and the
    /// server mapping cannot disagree about whether a key is meaningful.
    #[must_use]
    pub fn uses_static_keys(self) -> bool {
        self == Self::Static
    }
}

impl fmt::Display for CredentialSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Default => "default",
            Self::Static => "static",
            Self::Environment => "environment",
            Self::WebIdentity => "web-identity",
            Self::Container => "container",
            Self::InstanceProfile => "instance-profile",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ObjectStoreConfig {
    /// Unset means no object store, which is only viable for a single node on
    /// a laptop. `orbita dev` leaves it unset on purpose.
    pub endpoint: Option<String>,
    pub bucket: String,
    pub region: String,
    /// Resolved rather than configured where it was left out: keys present
    /// means static, keys absent means the instance profile. An operator who
    /// wants to be sure which one they get sets it.
    pub credential_source: CredentialSource,
    pub access_key_id: Option<String>,
    #[serde(skip_serializing)]
    pub secret_access_key: Option<String>,
    /// The role to assume on top of the base credentials, as a full ARN.
    /// Unset means the base credentials talk to S3 directly.
    pub role_arn: Option<String>,
    /// The external id a cross-account role's trust policy requires. Treated
    /// as a secret even though AWS does not call it one.
    #[serde(skip_serializing)]
    pub role_external_id: Option<String>,
    /// What the assumed session is called in CloudTrail. It defaults to the
    /// cluster name and node id, because a session name that is the same on
    /// every node makes an audit log useless.
    pub role_session_name: Option<String>,
    /// Overrides the STS endpoint for role assumption and for the IRSA token
    /// exchange. Unset derives a regional one from the region's partition,
    /// which is correct in every partition AWS publishes; this is for
    /// PrivateLink, for a partition newer than this release, and for a test
    /// double.
    pub sts_endpoint: Option<String>,
    /// MinIO and most S3-compatible stores need path style addressing, and AWS
    /// itself does not. The default suits the quickstart, so an AWS deployment
    /// has to turn it off.
    pub force_path_style: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TelemetryConfig {
    /// Unset disables trace and metric export entirely. Logs still go to
    /// stderr, because a node that cannot reach a collector should still be
    /// debuggable.
    pub otlp_endpoint: Option<String>,
    pub trace_sample_ratio: f64,
    pub service_name: String,
    pub log_level: String,
    pub resource_attributes: BTreeMap<String, String>,
}

/// Where the client side commands point.
///
/// A node ignores this and a client command ignores everything else, but they
/// share one file so that an operator has one thing to configure.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ClientConfig {
    pub endpoint: String,
    #[serde(skip_serializing)]
    pub credential: Option<String>,
}

/// One source of configuration, before it is merged with the others.
///
/// Every field is optional because "not set here" and "set to the default
/// value here" have to stay distinguishable, or a file could not override an
/// environment variable back to a default.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Layer {
    #[serde(default)]
    pub node: NodeLayer,
    #[serde(default)]
    pub cluster: ClusterLayer,
    #[serde(default)]
    pub object_store: ObjectStoreLayer,
    #[serde(default)]
    pub telemetry: TelemetryLayer,
    #[serde(default)]
    pub client: ClientLayer,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeLayer {
    pub id: Option<u64>,
    pub role: Option<Role>,
    pub listen: Option<String>,
    pub advertise: Option<String>,
    pub peer_listen: Option<String>,
    pub peer_advertise: Option<String>,
    pub data_dir: Option<PathBuf>,
}

/// The join durations are strings rather than numbers so that a file says
/// `join_backoff_max = "10s"` and not `10000`. A bare number would have to
/// mean either seconds or milliseconds, and either choice is wrong by a factor
/// of a thousand for somebody.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterLayer {
    pub name: Option<String>,
    pub leader_peers: Option<Vec<String>>,
    pub allow_version_skew: Option<bool>,
    pub require_auth: Option<bool>,
    pub root_credential: Option<String>,
    pub join_backoff_initial: Option<String>,
    pub join_backoff_max: Option<String>,
    pub join_timeout: Option<String>,
    pub drain_timeout: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectStoreLayer {
    pub endpoint: Option<String>,
    pub bucket: Option<String>,
    pub region: Option<String>,
    pub credential_source: Option<CredentialSource>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub role_arn: Option<String>,
    pub role_external_id: Option<String>,
    pub role_session_name: Option<String>,
    pub sts_endpoint: Option<String>,
    pub force_path_style: Option<bool>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelemetryLayer {
    pub otlp_endpoint: Option<String>,
    pub trace_sample_ratio: Option<f64>,
    pub service_name: Option<String>,
    pub log_level: Option<String>,
    pub resource_attributes: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientLayer {
    pub endpoint: Option<String>,
    pub credential: Option<String>,
}

/// Replaces `$target` with `$source` wherever the source has an opinion.
macro_rules! overlay {
    ($target:expr, $source:expr, $($field:ident),+ $(,)?) => {
        $(
            if $source.$field.is_some() {
                $target.$field = $source.$field;
            }
        )+
    };
}

impl Layer {
    /// Folds another layer on top of this one. The other layer wins every
    /// option it sets and leaves the rest alone.
    #[must_use]
    pub fn merge(mut self, other: Self) -> Self {
        overlay!(
            self.node,
            other.node,
            id,
            role,
            listen,
            advertise,
            peer_listen,
            peer_advertise,
            data_dir
        );
        overlay!(
            self.cluster,
            other.cluster,
            name,
            leader_peers,
            allow_version_skew,
            require_auth,
            root_credential,
            join_backoff_initial,
            join_backoff_max,
            join_timeout,
            drain_timeout
        );
        overlay!(
            self.object_store,
            other.object_store,
            endpoint,
            bucket,
            region,
            credential_source,
            access_key_id,
            secret_access_key,
            role_arn,
            role_external_id,
            role_session_name,
            sts_endpoint,
            force_path_style
        );
        overlay!(
            self.telemetry,
            other.telemetry,
            otlp_endpoint,
            trace_sample_ratio,
            service_name,
            log_level,
            resource_attributes
        );
        overlay!(self.client, other.client, endpoint, credential);
        self
    }

    /// Parses a layer out of TOML text.
    ///
    /// Unknown keys are an error rather than a warning, because a typo in a
    /// configuration file otherwise shows up as a mysterious default hours
    /// later.
    pub fn from_toml(text: &str) -> Result<Self> {
        toml::from_str(text)
            .context("the configuration is not valid; check for a misspelled or misplaced key")
    }

    /// Reads a layer from a file.
    pub fn from_file(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("cannot read the configuration file at {}", path.display()))?;
        Self::from_toml(&text)
            .with_context(|| format!("cannot parse the configuration file at {}", path.display()))
    }

    /// Reads a layer from a map of environment variables.
    ///
    /// It takes a map rather than reading the process environment so that the
    /// precedence tests can run in parallel without fighting over global
    /// state.
    pub fn from_env(env: &BTreeMap<String, String>) -> Result<Self> {
        let get = |key: &str| env.get(key).map(String::as_str).filter(|v| !v.is_empty());

        let parse = |key: &str| -> Result<Option<u64>> {
            get(key)
                .map(|v| {
                    v.parse::<u64>()
                        .with_context(|| format!("{key} must be a whole number, got {v:?}"))
                })
                .transpose()
        };
        let flag = |key: &str| -> Result<Option<bool>> {
            get(key)
                .map(|v| match v.to_ascii_lowercase().as_str() {
                    "1" | "true" | "yes" | "on" => Ok(true),
                    "0" | "false" | "no" | "off" => Ok(false),
                    other => bail!("{key} must be true or false, got {other:?}"),
                })
                .transpose()
        };

        let role = get("ORBITA_NODE_ROLE")
            .map(|v| match v.to_ascii_lowercase().as_str() {
                "leader" => Ok(Role::Leader),
                "worker" => Ok(Role::Worker),
                other => bail!("ORBITA_NODE_ROLE must be leader or worker, got {other:?}"),
            })
            .transpose()?;

        let credential_source = get("ORBITA_OBJECT_STORE_CREDENTIAL_SOURCE")
            .map(
                |v| match v.to_ascii_lowercase().replace('_', "-").as_str() {
                    "default" => Ok(CredentialSource::Default),
                    "static" => Ok(CredentialSource::Static),
                    "environment" | "env" => Ok(CredentialSource::Environment),
                    // `irsa` is what the EKS documentation calls this and what
                    // an operator will reach for first.
                    "web-identity" | "irsa" => Ok(CredentialSource::WebIdentity),
                    "container" | "ecs" | "pod-identity" => Ok(CredentialSource::Container),
                    "instance-profile" | "instance" | "imds" => {
                        Ok(CredentialSource::InstanceProfile)
                    }
                    other => bail!(
                        "ORBITA_OBJECT_STORE_CREDENTIAL_SOURCE must be one of default, static, \
                         environment, web-identity, container, or instance-profile, got {other:?}"
                    ),
                },
            )
            .transpose()?;

        let ratio = get("ORBITA_TRACE_SAMPLE_RATIO")
            .map(|v| {
                v.parse::<f64>().with_context(|| {
                    format!("ORBITA_TRACE_SAMPLE_RATIO must be a number, got {v:?}")
                })
            })
            .transpose()?;

        Ok(Self {
            node: NodeLayer {
                id: parse("ORBITA_NODE_ID")?,
                role,
                listen: get("ORBITA_LISTEN").map(str::to_owned),
                advertise: get("ORBITA_ADVERTISE").map(str::to_owned),
                peer_listen: get("ORBITA_PEER_LISTEN").map(str::to_owned),
                peer_advertise: get("ORBITA_PEER_ADVERTISE").map(str::to_owned),
                data_dir: get("ORBITA_DATA_DIR").map(PathBuf::from),
            },
            cluster: ClusterLayer {
                name: get("ORBITA_CLUSTER_NAME").map(str::to_owned),
                leader_peers: get("ORBITA_LEADER_PEERS").map(parse_list),
                allow_version_skew: flag("ORBITA_ALLOW_VERSION_SKEW")?,
                require_auth: flag("ORBITA_REQUIRE_AUTH")?,
                root_credential: get("ORBITA_ROOT_CREDENTIAL").map(str::to_owned),
                join_backoff_initial: get("ORBITA_JOIN_BACKOFF_INITIAL").map(str::to_owned),
                join_backoff_max: get("ORBITA_JOIN_BACKOFF_MAX").map(str::to_owned),
                join_timeout: get("ORBITA_JOIN_TIMEOUT").map(str::to_owned),
                drain_timeout: get("ORBITA_DRAIN_TIMEOUT").map(str::to_owned),
            },
            object_store: ObjectStoreLayer {
                endpoint: get("ORBITA_OBJECT_STORE_ENDPOINT").map(str::to_owned),
                bucket: get("ORBITA_OBJECT_STORE_BUCKET").map(str::to_owned),
                region: get("ORBITA_OBJECT_STORE_REGION").map(str::to_owned),
                credential_source,
                access_key_id: get("ORBITA_OBJECT_STORE_ACCESS_KEY_ID").map(str::to_owned),
                secret_access_key: get("ORBITA_OBJECT_STORE_SECRET_ACCESS_KEY").map(str::to_owned),
                role_arn: get("ORBITA_OBJECT_STORE_ROLE_ARN").map(str::to_owned),
                role_external_id: get("ORBITA_OBJECT_STORE_ROLE_EXTERNAL_ID").map(str::to_owned),
                role_session_name: get("ORBITA_OBJECT_STORE_ROLE_SESSION_NAME").map(str::to_owned),
                sts_endpoint: get("ORBITA_OBJECT_STORE_STS_ENDPOINT").map(str::to_owned),
                force_path_style: flag("ORBITA_OBJECT_STORE_FORCE_PATH_STYLE")?,
            },
            telemetry: TelemetryLayer {
                otlp_endpoint: get("ORBITA_OTLP_ENDPOINT").map(str::to_owned),
                trace_sample_ratio: ratio,
                service_name: get("ORBITA_SERVICE_NAME").map(str::to_owned),
                log_level: get("ORBITA_LOG_LEVEL").map(str::to_owned),
                resource_attributes: get("ORBITA_RESOURCE_ATTRIBUTES")
                    .map(parse_attributes)
                    .transpose()?,
            },
            client: ClientLayer {
                endpoint: get("ORBITA_ENDPOINT").map(str::to_owned),
                credential: get("ORBITA_CREDENTIAL").map(str::to_owned),
            },
        })
    }

    /// Fills in the defaults and checks the result is usable.
    pub fn resolve(self) -> Result<Config> {
        let listen = self
            .node
            .listen
            .unwrap_or_else(|| format!("0.0.0.0:{DEFAULT_PORT}"));
        listen
            .parse::<SocketAddr>()
            .with_context(|| format!("node.listen must be an address and port, got {listen:?}"))?;

        let advertise_was_set = self.node.advertise.is_some();
        let advertise = self.node.advertise.unwrap_or_else(|| listen.clone());

        let peer_listen = self
            .node
            .peer_listen
            .unwrap_or_else(|| format!("0.0.0.0:{DEFAULT_PEER_PORT}"));
        let parsed_peer: SocketAddr = peer_listen.parse().with_context(|| {
            format!("node.peer_listen must be an address and port, got {peer_listen:?}")
        })?;
        // Two listeners cannot share a port, and the failure if they try is a
        // bind error minutes into a rollout rather than a sentence now. Port
        // zero is exempt, because it means "whichever one is free" and two of
        // them are two different ports.
        if parsed_peer.port() != 0
            && parsed_peer == listen.parse::<SocketAddr>().unwrap_or(parsed_peer)
        {
            bail!(
                "node.listen and node.peer_listen are both {listen}, and a node needs two \
                 listeners: one for clients and one for peers"
            );
        }
        let peer_advertise = self.node.peer_advertise.unwrap_or_else(|| {
            // Falling back to the client advertise address with the peer port
            // is the answer that is right in a container, where the host name
            // is the same for both and only the port differs.
            match advertise.rsplit_once(':') {
                Some((host, _)) if advertise_was_set => format!("{host}:{}", parsed_peer.port()),
                _ => peer_listen.clone(),
            }
        });

        let join_backoff_initial_millis = duration_millis(
            "cluster.join_backoff_initial",
            self.cluster.join_backoff_initial,
        )?
        .unwrap_or(250);
        let join_backoff_max_millis =
            duration_millis("cluster.join_backoff_max", self.cluster.join_backoff_max)?
                .unwrap_or(10_000);
        // Five minutes rather than forever. A worker that cannot find the
        // leader group should exit and let the orchestrator restart it, because
        // a crash loop is visible in `kubectl get pods` and a process retrying
        // silently for a day is not.
        let join_timeout_millis =
            duration_millis("cluster.join_timeout", self.cluster.join_timeout)?.unwrap_or(300_000);
        let drain_timeout_millis =
            duration_millis("cluster.drain_timeout", self.cluster.drain_timeout)?
                .unwrap_or(290_000);
        if join_backoff_max_millis < join_backoff_initial_millis {
            bail!(
                "cluster.join_backoff_max is {join_backoff_max_millis} ms, below \
                 cluster.join_backoff_initial at {join_backoff_initial_millis} ms, so the backoff \
                 would shrink rather than grow"
            );
        }

        let trace_sample_ratio = self.telemetry.trace_sample_ratio.unwrap_or(1.0);
        if !(0.0..=1.0).contains(&trace_sample_ratio) {
            bail!("telemetry.trace_sample_ratio must be between 0 and 1, got {trace_sample_ratio}");
        }

        // Left unset, the source follows the keys: keys mean static, and no
        // keys means the node resolves one at startup in the AWS chain's
        // order. It deliberately does *not* mean the instance profile. On EKS
        // the instance profile is the node role and IRSA is the workload role,
        // so defaulting keyless to IMDS would take every existing IRSA
        // deployment and quietly re-point it at a different principal — which
        // succeeds, rather than failing, wherever node IMDS is reachable.
        let has_key = self.object_store.access_key_id.is_some()
            || self.object_store.secret_access_key.is_some();
        let credential_source = self.object_store.credential_source.unwrap_or({
            if has_key {
                CredentialSource::Static
            } else {
                CredentialSource::Default
            }
        });
        // Catching this here rather than at startup means the pod fails with a
        // sentence instead of a 403 from S3 several minutes into a rollout.
        if !credential_source.uses_static_keys() && has_key {
            bail!(
                "object_store.credential_source is {credential_source}, but a static key is also \
                 configured; remove the key or set credential_source to static, because a node \
                 that silently ignores one of them is a node nobody can audit"
            );
        }
        if credential_source == CredentialSource::Static
            && self.object_store.endpoint.is_some()
            && !has_key
        {
            bail!(
                "object_store.credential_source is static, but no access_key_id or \
                 secret_access_key was configured"
            );
        }
        if let Some(sts_endpoint) = &self.object_store.sts_endpoint {
            if !sts_endpoint.starts_with("http://") && !sts_endpoint.starts_with("https://") {
                bail!(
                    "object_store.sts_endpoint must start with http:// or https://, got \
                     {sts_endpoint:?}"
                );
            }
        }

        let endpoint = self
            .client
            .endpoint
            .unwrap_or_else(|| format!("http://127.0.0.1:{DEFAULT_PORT}"));
        if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
            bail!("client.endpoint must start with http:// or https://, got {endpoint:?}");
        }

        Ok(Config {
            node: NodeConfig {
                id: self.node.id.unwrap_or(1),
                role: self.node.role.unwrap_or(Role::Worker),
                listen,
                advertise,
                peer_listen,
                peer_advertise,
                data_dir: self
                    .node
                    .data_dir
                    .unwrap_or_else(|| PathBuf::from(".orbita")),
            },
            cluster: ClusterConfig {
                name: self.cluster.name.unwrap_or_else(|| "orbita".to_owned()),
                leader_peers: self.cluster.leader_peers.unwrap_or_default(),
                allow_version_skew: self.cluster.allow_version_skew.unwrap_or(false),
                require_auth: self.cluster.require_auth.unwrap_or(false),
                root_credential: self.cluster.root_credential,
                join_backoff_initial_millis,
                join_backoff_max_millis,
                join_timeout_millis,
                drain_timeout_millis,
            },
            object_store: ObjectStoreConfig {
                endpoint: self.object_store.endpoint,
                bucket: self
                    .object_store
                    .bucket
                    .unwrap_or_else(|| "orbita".to_owned()),
                region: self
                    .object_store
                    .region
                    .unwrap_or_else(|| "us-east-1".to_owned()),
                credential_source,
                access_key_id: self.object_store.access_key_id,
                secret_access_key: self.object_store.secret_access_key,
                role_arn: self.object_store.role_arn,
                role_external_id: self.object_store.role_external_id,
                role_session_name: self.object_store.role_session_name,
                sts_endpoint: self.object_store.sts_endpoint,
                force_path_style: self.object_store.force_path_style.unwrap_or(true),
            },
            telemetry: TelemetryConfig {
                otlp_endpoint: self.telemetry.otlp_endpoint,
                trace_sample_ratio,
                service_name: self
                    .telemetry
                    .service_name
                    .unwrap_or_else(|| "orbita".to_owned()),
                log_level: self
                    .telemetry
                    .log_level
                    .unwrap_or_else(|| "info".to_owned()),
                resource_attributes: self.telemetry.resource_attributes.unwrap_or_default(),
            },
            client: ClientConfig {
                endpoint,
                credential: self.client.credential,
            },
        })
    }
}

/// Parses a duration such as `500ms`, `30s`, `5m`, `2h`, or `7d` into
/// milliseconds.
///
/// A unit is required. A bare number would have to mean either seconds or
/// milliseconds, and whichever we picked would be wrong by a factor of a
/// thousand for somebody, on a TTL, silently. A bare `0` is the one exception,
/// because zero of anything is zero and an operator writing "no timeout" will
/// write `0`.
pub fn parse_duration_millis(text: &str) -> Result<u64> {
    let text = text.trim();
    if text == "0" {
        return Ok(0);
    }
    let split = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    let (digits, unit) = text.split_at(split);
    if digits.is_empty() {
        bail!("expected a number and a unit, such as 30s, got {text:?}");
    }
    let value: u64 = digits.parse()?;
    let millis = match unit {
        "ms" => value,
        "s" => value * 1_000,
        "m" => value * 60_000,
        "h" => value * 3_600_000,
        "d" => value * 86_400_000,
        "" => bail!("durations need a unit: ms, s, m, h, or d. Got {text:?}"),
        other => bail!("unknown duration unit {other:?}. Use ms, s, m, h, or d"),
    };
    Ok(millis)
}

/// Parses an optional duration option, naming the option when it is wrong.
fn duration_millis(option: &str, value: Option<String>) -> Result<Option<u64>> {
    value
        .map(|v| parse_duration_millis(&v).with_context(|| format!("{option} is not a duration")))
        .transpose()
}

fn parse_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_owned)
        .collect()
}

fn parse_attributes(value: &str) -> Result<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    for pair in value.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let (key, val) = pair.split_once('=').with_context(|| {
            format!("resource attributes must be key=value pairs, got {pair:?}")
        })?;
        out.insert(key.trim().to_owned(), val.trim().to_owned());
    }
    Ok(out)
}

/// Picks the configuration file to read.
///
/// An explicit path that does not exist is an error, because the operator
/// asked for that file and silently ignoring it would start a node with the
/// wrong settings. A default path that does not exist is fine, because most
/// people never write one.
pub fn locate_file(
    explicit: Option<&Path>,
    env: &BTreeMap<String, String>,
) -> Result<Option<PathBuf>> {
    if let Some(path) = explicit {
        if !path.exists() {
            bail!("no configuration file at {}", path.display());
        }
        return Ok(Some(path.to_owned()));
    }
    if let Some(path) = env.get("ORBITA_CONFIG").filter(|v| !v.is_empty()) {
        let path = PathBuf::from(path);
        if !path.exists() {
            bail!(
                "ORBITA_CONFIG points at {}, which does not exist",
                path.display()
            );
        }
        return Ok(Some(path));
    }
    Ok(DEFAULT_CONFIG_PATHS
        .iter()
        .map(PathBuf::from)
        .find(|p| p.exists()))
}

/// Builds the effective configuration from all four sources.
///
/// `flags` is the layer the command line produced, and it is applied last
/// because the person typing it is the most recent authority on what should
/// happen.
pub fn load(
    explicit_file: Option<&Path>,
    env: &BTreeMap<String, String>,
    flags: Layer,
) -> Result<Config> {
    load_with_base(Layer::default(), explicit_file, env, flags)
}

/// Builds the effective configuration on top of a base layer.
///
/// `orbita dev` uses this to supply a whole set of laptop-shaped defaults that
/// still lose to a file, the environment, and flags. Putting them in a base
/// layer rather than in the flags layer is what keeps `orbita dev
/// --config ./mine.toml` doing what it looks like it does.
pub fn load_with_base(
    base: Layer,
    explicit_file: Option<&Path>,
    env: &BTreeMap<String, String>,
    flags: Layer,
) -> Result<Config> {
    let file = match locate_file(explicit_file, env)? {
        Some(path) => Layer::from_file(&path)?,
        None => Layer::default(),
    };
    base.merge(file)
        .merge(Layer::from_env(env)?)
        .merge(flags)
        .resolve()
}

/// The configuration `orbita dev` runs with.
///
/// A single node is its own leader group and its own worker, so there is
/// nothing to bootstrap and no peers to list. The data directory is relative
/// so that deleting it is obvious, and there is no object store because a
/// laptop does not need bulk durability to try the thing out.
///
/// The peer listener still exists and still binds, on the port above the
/// client one, so that the development path exercises the same two-listener
/// shape a real deployment has. Both are on the loopback address, because
/// nothing outside this machine has any business reaching either.
#[must_use]
pub fn dev_defaults(port: u16, data_dir: PathBuf) -> Layer {
    Layer {
        node: NodeLayer {
            id: Some(1),
            role: Some(Role::Leader),
            listen: Some(format!("127.0.0.1:{port}")),
            advertise: Some(format!("127.0.0.1:{port}")),
            peer_listen: Some(format!("127.0.0.1:{}", port.wrapping_add(1))),
            peer_advertise: Some(format!("127.0.0.1:{}", port.wrapping_add(1))),
            data_dir: Some(data_dir),
        },
        cluster: ClusterLayer {
            name: Some("dev".to_owned()),
            leader_peers: Some(Vec::new()),
            allow_version_skew: Some(false),
            ..ClusterLayer::default()
        },
        ..Layer::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn every_option_has_a_default_so_dev_needs_no_configuration() {
        let config = Layer::default().resolve().expect("defaults must resolve");
        assert_eq!(config.node.id, 1);
        assert_eq!(config.node.role, Role::Worker);
        assert_eq!(config.node.listen, "0.0.0.0:7100");
        assert_eq!(config.node.advertise, "0.0.0.0:7100");
        assert_eq!(config.node.peer_listen, "0.0.0.0:7101");
        assert_eq!(config.node.peer_advertise, "0.0.0.0:7101");
        assert_eq!(config.client.endpoint, "http://127.0.0.1:7100");
        assert_eq!(config.telemetry.log_level, "info");
        assert!(config.object_store.endpoint.is_none());
    }

    #[test]
    fn an_object_store_with_no_keys_resolves_its_source_rather_than_assuming_imds() {
        let file =
            Layer::from_toml("[object_store]\nendpoint = \"https://s3.us-east-1.amazonaws.com\"\n")
                .unwrap();
        let config = Layer::default().merge(file).resolve().unwrap();
        assert_eq!(
            config.object_store.credential_source,
            CredentialSource::Default,
            "a keyless AWS deployment must be the default, but it must not be pinned to the \
             instance profile: on EKS that is the node role, not the workload role, and \
             defaulting to it silently changes which principal an existing deployment uses"
        );
    }

    #[test]
    fn every_credential_source_can_be_named_in_the_environment() {
        for (spelling, expected) in [
            ("default", CredentialSource::Default),
            ("static", CredentialSource::Static),
            ("environment", CredentialSource::Environment),
            ("env", CredentialSource::Environment),
            ("web-identity", CredentialSource::WebIdentity),
            ("web_identity", CredentialSource::WebIdentity),
            ("irsa", CredentialSource::WebIdentity),
            ("container", CredentialSource::Container),
            ("ecs", CredentialSource::Container),
            ("pod-identity", CredentialSource::Container),
            ("instance-profile", CredentialSource::InstanceProfile),
            ("imds", CredentialSource::InstanceProfile),
        ] {
            let layer =
                Layer::from_env(&env(&[("ORBITA_OBJECT_STORE_CREDENTIAL_SOURCE", spelling)]))
                    .unwrap_or_else(|error| panic!("{spelling:?} should parse: {error:#}"));
            assert_eq!(
                layer.object_store.credential_source,
                Some(expected),
                "{spelling:?}"
            );
        }
    }

    #[test]
    fn a_static_key_alongside_any_keyless_source_is_refused() {
        // Not just instance-profile: a key next to IRSA is the same
        // unauditable ambiguity, and the chart can now render either.
        for source in [
            "default",
            "environment",
            "web-identity",
            "container",
            "instance-profile",
        ] {
            let file = Layer::from_toml(&format!(
                "[object_store]\nendpoint = \"https://s3.us-east-1.amazonaws.com\"\n\
                 credential_source = \"{source}\"\naccess_key_id = \"a\"\n\
                 secret_access_key = \"s\"\n"
            ))
            .unwrap();
            let error = Layer::default()
                .merge(file)
                .resolve()
                .err()
                .unwrap_or_else(|| panic!("{source} alongside a static key should be refused"));
            assert!(
                format!("{error:#}").contains("credential_source"),
                "{error:#}"
            );
        }
    }

    #[test]
    fn the_sts_endpoint_must_be_a_url() {
        let file = Layer::from_toml(
            "[object_store]\nendpoint = \"https://s3.us-east-1.amazonaws.com\"\n\
             sts_endpoint = \"sts.internal.example.com\"\n",
        )
        .unwrap();
        let error = Layer::default().merge(file).resolve().unwrap_err();
        assert!(format!("{error:#}").contains("sts_endpoint"), "{error:#}");
    }

    #[test]
    fn configured_keys_select_the_static_source() {
        let file = Layer::from_toml(
            "[object_store]\nendpoint = \"http://minio:9000\"\naccess_key_id = \"a\"\n\
             secret_access_key = \"s\"\n",
        )
        .unwrap();
        let config = Layer::default().merge(file).resolve().unwrap();
        assert_eq!(
            config.object_store.credential_source,
            CredentialSource::Static
        );
    }

    #[test]
    fn a_static_key_alongside_the_instance_profile_is_refused() {
        let file = Layer::from_toml(
            "[object_store]\nendpoint = \"https://s3.us-east-1.amazonaws.com\"\n\
             credential_source = \"instance-profile\"\naccess_key_id = \"a\"\n\
             secret_access_key = \"s\"\n",
        )
        .unwrap();
        let error = Layer::default().merge(file).resolve().unwrap_err();
        assert!(
            format!("{error:#}").contains("credential_source"),
            "{error:#}"
        );
    }

    #[test]
    fn the_static_source_without_keys_is_refused() {
        let file = Layer::from_toml(
            "[object_store]\nendpoint = \"http://minio:9000\"\ncredential_source = \"static\"\n",
        )
        .unwrap();
        assert!(Layer::default().merge(file).resolve().is_err());
    }

    #[test]
    fn the_credential_source_can_be_named_in_the_environment() {
        let environment = Layer::from_env(&env(&[(
            "ORBITA_OBJECT_STORE_CREDENTIAL_SOURCE",
            "instance-profile",
        )]))
        .unwrap();
        assert_eq!(
            environment.object_store.credential_source,
            Some(CredentialSource::InstanceProfile)
        );
        assert!(Layer::from_env(&env(&[(
            "ORBITA_OBJECT_STORE_CREDENTIAL_SOURCE",
            "whatever-is-lying-around",
        )]))
        .is_err());
    }

    #[test]
    fn the_role_to_assume_comes_from_the_environment_too() {
        let environment = Layer::from_env(&env(&[
            (
                "ORBITA_OBJECT_STORE_ROLE_ARN",
                "arn:aws:iam::123456789012:role/orbita",
            ),
            ("ORBITA_OBJECT_STORE_ROLE_EXTERNAL_ID", "shared-secret"),
        ]))
        .unwrap();
        assert_eq!(
            environment.object_store.role_arn.as_deref(),
            Some("arn:aws:iam::123456789012:role/orbita")
        );
        assert_eq!(
            environment.object_store.role_external_id.as_deref(),
            Some("shared-secret")
        );
    }

    #[test]
    fn a_serialized_configuration_carries_no_credential_material() {
        let file = Layer::from_toml(
            "[cluster]\nroot_credential = \"the-root-secret\"\n\
             [object_store]\nendpoint = \"http://minio:9000\"\naccess_key_id = \"the-key-id\"\n\
             secret_access_key = \"the-secret\"\nrole_external_id = \"the-external-id\"\n",
        )
        .unwrap();
        let config = Layer::default().merge(file).resolve().unwrap();
        assert_eq!(
            config.cluster.root_credential.as_deref(),
            Some("the-root-secret"),
            "the root secret is still read into the resolved configuration"
        );
        let rendered = serde_json::to_string(&config).expect("serializes");
        assert!(
            !rendered.contains("the-secret")
                && !rendered.contains("the-external-id")
                && !rendered.contains("the-root-secret"),
            "a printed configuration must not be a credential dump: {rendered}"
        );
    }

    #[test]
    fn the_root_credential_can_be_named_in_the_environment() {
        let environment =
            Layer::from_env(&env(&[("ORBITA_ROOT_CREDENTIAL", "the-root-secret")])).unwrap();
        assert_eq!(
            environment.cluster.root_credential.as_deref(),
            Some("the-root-secret")
        );
    }

    #[test]
    fn a_file_value_beats_the_default() {
        let file = Layer::from_toml("[node]\nlisten = \"0.0.0.0:9000\"\n").unwrap();
        let config = Layer::default().merge(file).resolve().unwrap();
        assert_eq!(config.node.listen, "0.0.0.0:9000");
    }

    #[test]
    fn an_environment_variable_beats_the_file() {
        let file = Layer::from_toml("[node]\nlisten = \"0.0.0.0:9000\"\n").unwrap();
        let environment = Layer::from_env(&env(&[("ORBITA_LISTEN", "0.0.0.0:9001")])).unwrap();
        let config = Layer::default()
            .merge(file)
            .merge(environment)
            .resolve()
            .unwrap();
        assert_eq!(config.node.listen, "0.0.0.0:9001");
    }

    #[test]
    fn a_flag_beats_the_environment_and_the_file() {
        let file = Layer::from_toml("[node]\nlisten = \"0.0.0.0:9000\"\n").unwrap();
        let environment = Layer::from_env(&env(&[("ORBITA_LISTEN", "0.0.0.0:9001")])).unwrap();
        let flags = Layer {
            node: NodeLayer {
                listen: Some("0.0.0.0:9002".to_owned()),
                ..NodeLayer::default()
            },
            ..Layer::default()
        };
        let config = Layer::default()
            .merge(file)
            .merge(environment)
            .merge(flags)
            .resolve()
            .unwrap();
        assert_eq!(config.node.listen, "0.0.0.0:9002");
    }

    #[test]
    fn a_higher_layer_overrides_one_option_without_discarding_its_section() {
        let file =
            Layer::from_toml("[node]\nid = 7\nlisten = \"0.0.0.0:9000\"\ndata_dir = \"/data\"\n")
                .unwrap();
        let environment = Layer::from_env(&env(&[("ORBITA_LISTEN", "0.0.0.0:9001")])).unwrap();
        let config = Layer::default()
            .merge(file)
            .merge(environment)
            .resolve()
            .unwrap();
        assert_eq!(config.node.id, 7);
        assert_eq!(config.node.data_dir, PathBuf::from("/data"));
        assert_eq!(config.node.listen, "0.0.0.0:9001");
    }

    #[test]
    fn an_empty_environment_variable_does_not_override_anything() {
        let file = Layer::from_toml("[node]\nlisten = \"0.0.0.0:9000\"\n").unwrap();
        let environment = Layer::from_env(&env(&[("ORBITA_LISTEN", "")])).unwrap();
        let config = Layer::default()
            .merge(file)
            .merge(environment)
            .resolve()
            .unwrap();
        assert_eq!(config.node.listen, "0.0.0.0:9000");
    }

    #[test]
    fn an_unknown_key_in_the_file_is_an_error_rather_than_a_silent_default() {
        let err = Layer::from_toml("[node]\nlistn = \"0.0.0.0:9000\"\n").unwrap_err();
        assert!(format!("{err:#}").contains("misspelled"), "{err:#}");
    }

    #[test]
    fn leader_peers_come_from_the_environment_as_a_comma_separated_list() {
        let environment = Layer::from_env(&env(&[(
            "ORBITA_LEADER_PEERS",
            "1=a:7101, 2=b:7101 ,3=c:7101",
        )]))
        .unwrap();
        let config = Layer::default().merge(environment).resolve().unwrap();
        assert_eq!(
            config.cluster.leader_peers,
            ["1=a:7101", "2=b:7101", "3=c:7101"]
        );
    }

    #[test]
    fn resource_attributes_parse_as_key_value_pairs() {
        let environment = Layer::from_env(&env(&[(
            "ORBITA_RESOURCE_ATTRIBUTES",
            "deployment.environment=prod, region=us-east-1",
        )]))
        .unwrap();
        let config = Layer::default().merge(environment).resolve().unwrap();
        assert_eq!(
            config.telemetry.resource_attributes["deployment.environment"],
            "prod"
        );
        assert_eq!(config.telemetry.resource_attributes["region"], "us-east-1");
    }

    #[test]
    fn resource_attributes_without_an_equals_sign_are_rejected() {
        let err = Layer::from_env(&env(&[("ORBITA_RESOURCE_ATTRIBUTES", "prod")])).unwrap_err();
        assert!(format!("{err:#}").contains("key=value"), "{err:#}");
    }

    #[test]
    fn a_role_the_binary_does_not_have_is_rejected_with_the_valid_ones_named() {
        let err = Layer::from_env(&env(&[("ORBITA_NODE_ROLE", "coordinator")])).unwrap_err();
        assert!(format!("{err:#}").contains("leader or worker"), "{err:#}");
    }

    #[test]
    fn booleans_accept_the_spellings_orchestrators_actually_emit() {
        for spelling in ["1", "true", "TRUE", "yes", "on"] {
            let layer = Layer::from_env(&env(&[("ORBITA_ALLOW_VERSION_SKEW", spelling)])).unwrap();
            assert_eq!(layer.cluster.allow_version_skew, Some(true), "{spelling}");
        }
        for spelling in ["0", "false", "no", "off"] {
            let layer = Layer::from_env(&env(&[("ORBITA_ALLOW_VERSION_SKEW", spelling)])).unwrap();
            assert_eq!(layer.cluster.allow_version_skew, Some(false), "{spelling}");
        }
    }

    #[test]
    fn a_sample_ratio_outside_zero_to_one_is_rejected() {
        let layer = Layer {
            telemetry: TelemetryLayer {
                trace_sample_ratio: Some(1.5),
                ..TelemetryLayer::default()
            },
            ..Layer::default()
        };
        let err = layer.resolve().unwrap_err();
        assert!(format!("{err:#}").contains("between 0 and 1"), "{err:#}");
    }

    #[test]
    fn a_listen_address_without_a_port_is_rejected_at_load_rather_than_at_bind() {
        let layer = Layer {
            node: NodeLayer {
                listen: Some("0.0.0.0".to_owned()),
                ..NodeLayer::default()
            },
            ..Layer::default()
        };
        let err = layer.resolve().unwrap_err();
        assert!(format!("{err:#}").contains("address and port"), "{err:#}");
    }

    #[test]
    fn a_client_endpoint_without_a_scheme_is_rejected() {
        let layer = Layer {
            client: ClientLayer {
                endpoint: Some("127.0.0.1:7100".to_owned()),
                ..ClientLayer::default()
            },
            ..Layer::default()
        };
        let err = layer.resolve().unwrap_err();
        assert!(format!("{err:#}").contains("http://"), "{err:#}");
    }

    #[test]
    fn advertise_falls_back_to_listen_when_it_is_not_set() {
        let layer = Layer {
            node: NodeLayer {
                listen: Some("10.0.0.4:7100".to_owned()),
                ..NodeLayer::default()
            },
            ..Layer::default()
        };
        assert_eq!(layer.resolve().unwrap().node.advertise, "10.0.0.4:7100");
    }

    #[test]
    fn the_client_and_peer_listeners_get_different_ports_by_default() {
        let config = Layer::default().resolve().unwrap();
        assert_ne!(config.node.listen, config.node.peer_listen);
        assert_eq!(DEFAULT_PEER_PORT, DEFAULT_PORT + 1);
    }

    #[test]
    fn both_listeners_on_one_address_is_refused_before_anything_tries_to_bind() {
        let layer = Layer {
            node: NodeLayer {
                listen: Some("0.0.0.0:7100".to_owned()),
                peer_listen: Some("0.0.0.0:7100".to_owned()),
                ..NodeLayer::default()
            },
            ..Layer::default()
        };
        let err = layer.resolve().unwrap_err();
        assert!(format!("{err:#}").contains("two listeners"), "{err:#}");
    }

    #[test]
    fn two_listeners_on_port_zero_are_allowed_because_that_is_two_free_ports() {
        let layer = Layer {
            node: NodeLayer {
                listen: Some("127.0.0.1:0".to_owned()),
                peer_listen: Some("127.0.0.1:0".to_owned()),
                ..NodeLayer::default()
            },
            ..Layer::default()
        };
        assert!(layer.resolve().is_ok());
    }

    #[test]
    fn the_peer_advertise_address_keeps_the_advertise_host_and_takes_the_peer_port() {
        let layer = Layer {
            node: NodeLayer {
                listen: Some("0.0.0.0:7100".to_owned()),
                advertise: Some("worker-1.orbita:7100".to_owned()),
                ..NodeLayer::default()
            },
            ..Layer::default()
        };
        assert_eq!(
            layer.resolve().unwrap().node.peer_advertise,
            "worker-1.orbita:7101"
        );
    }

    #[test]
    fn the_peer_addresses_come_from_the_environment_like_every_other_option() {
        let environment = Layer::from_env(&env(&[
            ("ORBITA_PEER_LISTEN", "10.1.0.4:9101"),
            ("ORBITA_PEER_ADVERTISE", "node-a.internal:9101"),
        ]))
        .unwrap();
        let config = Layer::default().merge(environment).resolve().unwrap();
        assert_eq!(config.node.peer_listen, "10.1.0.4:9101");
        assert_eq!(config.node.peer_advertise, "node-a.internal:9101");
    }

    #[test]
    fn join_durations_are_written_with_a_unit_rather_than_a_bare_number() {
        let file = Layer::from_toml(
            "[cluster]\njoin_backoff_initial = \"500ms\"\njoin_backoff_max = \"1m\"\n\
             join_timeout = \"10m\"\n",
        )
        .unwrap();
        let config = Layer::default().merge(file).resolve().unwrap();
        assert_eq!(config.cluster.join_backoff_initial_millis, 500);
        assert_eq!(config.cluster.join_backoff_max_millis, 60_000);
        assert_eq!(config.cluster.join_timeout_millis, 600_000);
    }

    #[test]
    fn a_zero_join_timeout_is_accepted_and_means_retry_forever() {
        let file = Layer::from_toml("[cluster]\njoin_timeout = \"0\"\n").unwrap();
        assert_eq!(
            Layer::default()
                .merge(file)
                .resolve()
                .unwrap()
                .cluster
                .join_timeout_millis,
            0
        );
    }

    #[test]
    fn a_join_backoff_maximum_below_the_initial_wait_is_refused() {
        let file = Layer::from_toml(
            "[cluster]\njoin_backoff_initial = \"10s\"\njoin_backoff_max = \"1s\"\n",
        )
        .unwrap();
        let err = Layer::default().merge(file).resolve().unwrap_err();
        assert!(
            format!("{err:#}").contains("shrink rather than grow"),
            "{err:#}"
        );
    }

    #[test]
    fn a_join_duration_without_a_unit_names_the_option_it_came_from() {
        let environment = Layer::from_env(&env(&[("ORBITA_JOIN_TIMEOUT", "300")])).unwrap();
        let err = Layer::default().merge(environment).resolve().unwrap_err();
        assert!(
            format!("{err:#}").contains("cluster.join_timeout"),
            "{err:#}"
        );
    }

    #[test]
    fn an_explicit_config_path_that_does_not_exist_is_an_error() {
        let err =
            locate_file(Some(Path::new("/nowhere/orbita.toml")), &BTreeMap::new()).unwrap_err();
        assert!(
            format!("{err:#}").contains("no configuration file"),
            "{err:#}"
        );
    }

    #[test]
    fn a_config_file_read_from_disk_reaches_the_resolved_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("orbita.toml");
        std::fs::write(&path, "[cluster]\nname = \"from-file\"\n").unwrap();
        let config = load(Some(&path), &BTreeMap::new(), Layer::default()).unwrap();
        assert_eq!(config.cluster.name, "from-file");
    }

    #[test]
    fn the_dev_config_needs_no_file_and_forms_its_own_leader_group() {
        let config = dev_defaults(7100, PathBuf::from(".orbita/dev"))
            .resolve()
            .unwrap();
        assert_eq!(config.node.role, Role::Leader);
        assert!(config.cluster.leader_peers.is_empty());
        assert_eq!(config.node.listen, "127.0.0.1:7100");
        // Both listeners are on the loopback address, because nothing outside
        // the laptop has any business reaching either of them.
        assert_eq!(config.node.peer_listen, "127.0.0.1:7101");
    }

    #[test]
    fn the_secret_access_key_never_reaches_serialized_output() {
        let layer = Layer {
            object_store: ObjectStoreLayer {
                secret_access_key: Some("hunter2".to_owned()),
                ..ObjectStoreLayer::default()
            },
            client: ClientLayer {
                credential: Some("also-secret".to_owned()),
                ..ClientLayer::default()
            },
            ..Layer::default()
        };
        let json = serde_json::to_string(&layer.resolve().unwrap()).unwrap();
        assert!(!json.contains("hunter2"), "{json}");
        assert!(!json.contains("also-secret"), "{json}");
    }
}
