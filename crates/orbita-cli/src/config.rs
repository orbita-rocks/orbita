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
    ("ORBITA_DATA_DIR", "node.data_dir"),
    ("ORBITA_CLUSTER_NAME", "cluster.name"),
    (
        "ORBITA_LEADER_PEERS",
        "cluster.leader_peers, comma separated",
    ),
    ("ORBITA_ALLOW_VERSION_SKEW", "cluster.allow_version_skew"),
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

/// The port a node listens on for client, admin, and peer traffic.
///
/// One port carries all three because a second port is a second thing to
/// expose, firewall, and get wrong, and gRPC multiplexes services on one
/// connection anyway.
pub const DEFAULT_PORT: u16 = 7100;

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
    pub listen: String,
    /// The address other nodes should use to reach this one. Inside a
    /// container `listen` is usually `0.0.0.0`, which no peer can dial, so
    /// this defaults to `listen` and has to be set in any real deployment.
    pub advertise: String,
    pub data_dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ClusterConfig {
    pub name: String,
    /// The initial leader group membership, identical on every leader node.
    /// Ignored once the data directory holds a Raft log, so it is safe to
    /// leave in a template forever. See [`crate::node`] for why.
    pub leader_peers: Vec<String>,
    pub allow_version_skew: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ObjectStoreConfig {
    /// Unset means no object store, which is only viable for a single node on
    /// a laptop. `orbita dev` leaves it unset on purpose.
    pub endpoint: Option<String>,
    pub bucket: String,
    pub region: String,
    pub access_key_id: Option<String>,
    #[serde(skip_serializing)]
    pub secret_access_key: Option<String>,
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
    pub data_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterLayer {
    pub name: Option<String>,
    pub leader_peers: Option<Vec<String>>,
    pub allow_version_skew: Option<bool>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectStoreLayer {
    pub endpoint: Option<String>,
    pub bucket: Option<String>,
    pub region: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
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
        overlay!(self.node, other.node, id, role, listen, advertise, data_dir);
        overlay!(
            self.cluster,
            other.cluster,
            name,
            leader_peers,
            allow_version_skew
        );
        overlay!(
            self.object_store,
            other.object_store,
            endpoint,
            bucket,
            region,
            access_key_id,
            secret_access_key,
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
                data_dir: get("ORBITA_DATA_DIR").map(PathBuf::from),
            },
            cluster: ClusterLayer {
                name: get("ORBITA_CLUSTER_NAME").map(str::to_owned),
                leader_peers: get("ORBITA_LEADER_PEERS").map(parse_list),
                allow_version_skew: flag("ORBITA_ALLOW_VERSION_SKEW")?,
            },
            object_store: ObjectStoreLayer {
                endpoint: get("ORBITA_OBJECT_STORE_ENDPOINT").map(str::to_owned),
                bucket: get("ORBITA_OBJECT_STORE_BUCKET").map(str::to_owned),
                region: get("ORBITA_OBJECT_STORE_REGION").map(str::to_owned),
                access_key_id: get("ORBITA_OBJECT_STORE_ACCESS_KEY_ID").map(str::to_owned),
                secret_access_key: get("ORBITA_OBJECT_STORE_SECRET_ACCESS_KEY").map(str::to_owned),
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

        let advertise = self.node.advertise.unwrap_or_else(|| listen.clone());

        let trace_sample_ratio = self.telemetry.trace_sample_ratio.unwrap_or(1.0);
        if !(0.0..=1.0).contains(&trace_sample_ratio) {
            bail!("telemetry.trace_sample_ratio must be between 0 and 1, got {trace_sample_ratio}");
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
                data_dir: self
                    .node
                    .data_dir
                    .unwrap_or_else(|| PathBuf::from(".orbita")),
            },
            cluster: ClusterConfig {
                name: self.cluster.name.unwrap_or_else(|| "orbita".to_owned()),
                leader_peers: self.cluster.leader_peers.unwrap_or_default(),
                allow_version_skew: self.cluster.allow_version_skew.unwrap_or(false),
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
                access_key_id: self.object_store.access_key_id,
                secret_access_key: self.object_store.secret_access_key,
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
#[must_use]
pub fn dev_defaults(port: u16, data_dir: PathBuf) -> Layer {
    Layer {
        node: NodeLayer {
            id: Some(1),
            role: Some(Role::Leader),
            listen: Some(format!("127.0.0.1:{port}")),
            advertise: Some(format!("127.0.0.1:{port}")),
            data_dir: Some(data_dir),
        },
        cluster: ClusterLayer {
            name: Some("dev".to_owned()),
            leader_peers: Some(Vec::new()),
            allow_version_skew: Some(false),
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
        assert_eq!(config.client.endpoint, "http://127.0.0.1:7100");
        assert_eq!(config.telemetry.log_level, "info");
        assert!(config.object_store.endpoint.is_none());
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
        let environment =
            Layer::from_env(&env(&[("ORBITA_LEADER_PEERS", "a:7100, b:7100 ,c:7100")])).unwrap();
        let config = Layer::default().merge(environment).resolve().unwrap();
        assert_eq!(config.cluster.leader_peers, ["a:7100", "b:7100", "c:7100"]);
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
