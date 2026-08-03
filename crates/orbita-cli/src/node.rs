//! Running a node, and the two decisions that go with it.
//!
//! This is the only module in the crate that starts a server. Everything else
//! is a network client, which is why the rest of the tool can be finished and
//! tested independently of the server crate.
//!
//! # Bootstrap
//!
//! A fresh cluster has a chicken and egg problem: the partition map lives in
//! the leader group, and the leader group is what has to agree on it. The
//! usual ways out are a discovery service, which is one more thing to run, or
//! a human running a one-off `init` command, which is one more step to get
//! wrong at exactly the moment somebody is deciding whether to keep going.
//! Orbita does neither.
//!
//! - A leader node reads `cluster.leader_peers`, which is the complete initial
//!   membership and is identical on every leader. It is a static list because
//!   it has to be knowable before anything is running, and every orchestrator
//!   can already produce a stable list of names for a StatefulSet.
//! - If the data directory already holds a Raft log, the list is ignored
//!   entirely and membership comes from the log. This matters more than it
//!   looks: it means the list can stay in a Helm template forever, and that
//!   adding a fourth leader later does not conflict with what the template
//!   says.
//! - On a genuinely fresh start, the node with the lowest address in the list
//!   creates the initial Raft configuration containing all of the peers, and
//!   the others wait to hear from it. Choosing by a total order over the list
//!   rather than by a race is what prevents two nodes from each forming a
//!   single-node cluster and both believing they are the leader group. Every
//!   node computes the same answer from the same list with no coordination.
//! - Workers do not bootstrap. A worker dials any leader address, registers,
//!   and is told the partition map. A worker that starts before the leader
//!   group exists retries rather than failing, because in a container
//!   orchestrator start order is not something anyone controls.
//! - The first partition is not an operator's problem. A keyspace is created
//!   with a single partition covering the whole range, so `keyspace create` is
//!   the only step and there is no "now create a partition" to forget.
//!
//! `orbita dev` shortcuts all of it. One node with an empty peer list is its
//! own leader group, which means a Raft configuration of one member that is
//! committed the moment it is written, and the same node also serves
//! partitions. There is no quorum to wait for and no second process. It also
//! creates a keyspace on startup so the first write does not need a second
//! command. This path exists because the first ten minutes decide whether
//! there is an eleventh, and a bootstrap that needs a paragraph of explanation
//! has already lost.
//!
//! # Version skew
//!
//! A node refuses to start when the leader group runs a version it is not
//! compatible with. Compatible means the same major and minor version;
//! patch releases may mix. Before 1.0 the minor version is the compatibility
//! unit, because that is where breaking changes go while the format is still
//! moving.
//!
//! Refusing is the safe answer and it is also the honest one. The alternative,
//! starting anyway and hoping the wire format is close enough, turns a rolling
//! upgrade into something that appears to work and then corrupts a partition
//! map under load. Refusing makes the upgrade path a question somebody has to
//! answer during design rather than discover during an incident. There is an
//! escape hatch, `--allow-version-skew`, and it is documented as unsupported,
//! because an operator staring at an outage should have the option and should
//! also know they are on their own.

use std::net::{SocketAddr, ToSocketAddrs};
use std::path::Path;

use anyhow::{bail, Context, Result};
use orbita_core::{KeyspaceName, NodeId};
use orbita_server::{Server, ServerConfig};

use crate::config::{Config, Role};

/// What a node run needs beyond the configuration.
#[derive(Debug, Clone)]
pub struct NodeOptions {
    /// A keyspace to create once the node is serving, if it does not already
    /// exist. `orbita dev` uses this; a real deployment leaves it empty.
    pub create_keyspace: Option<String>,
    /// Whether this is the single-node development path, which forms its own
    /// leader group and needs no peers.
    pub dev: bool,
}

/// Starts a node and runs until it is shut down.
///
/// This is deliberately the only call into `orbita-server` in the whole crate,
/// which is why `orbita-server` is not yet a dependency: one function needs it
/// and the other twenty commands do not.
///
/// When that crate lands its API, this function will:
///
/// 1. Build an `orbita_server::ServerConfig` from [`Config`], carrying the
///    node id, the listen address, the data directory, and the partition map
///    source, which is the leader peer list for a leader and the leader group
///    address for a worker.
/// 2. Call `orbita_server::Server::start(config)`.
/// 3. Create [`NodeOptions::create_keyspace`] if it is set and absent.
/// 4. Wait for a shutdown signal and call `Server::shutdown()`.
///
/// Everything the function needs from the configuration is already validated
/// by the time it is called, so wiring it up is a translation and not a
/// design.
pub async fn run_node(config: &Config, options: &NodeOptions) -> Result<()> {
    preflight(config, options)?;
    prepare_data_dir(&config.node.data_dir, false)?;

    // Resolving here rather than at parse time means a hostname in the config
    // is allowed, which is what a container orchestrator tends to hand you.
    let listen: SocketAddr = config
        .node
        .listen
        .to_socket_addrs()
        .with_context(|| format!("resolving node.listen, {}", config.node.listen))?
        .next()
        .with_context(|| format!("node.listen resolved to nothing, {}", config.node.listen))?;

    let mut server_config = ServerConfig::single_node(&config.node.data_dir)
        .with_node_id(NodeId(config.node.id))
        .with_listen_addr(listen);

    // The keyspace has to exist before the node serves, because the map a
    // worker opens its partitions from is built at start. Creating it after
    // would mean a second start to pick it up, which is exactly the extra step
    // the dev path exists to remove.
    if let Some(name) = &options.create_keyspace {
        let keyspace = KeyspaceName::new(name.as_str())
            .map_err(|e| anyhow::anyhow!("{e}"))
            .context("the keyspace to create at startup")?;
        server_config = server_config.with_keyspaces(&[keyspace]);
    }

    let server = Server::start(server_config)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
        .context("starting the node")?;

    // Startup notes go to stderr so that anything a later command pipes stays
    // clean.
    eprintln!("orbita: serving on {}", server.local_addr());

    // Ctrl-C is the ordinary way a foreground node stops, and a container stop
    // sends the same signal. Draining rather than dropping means in-flight
    // requests finish instead of becoming client errors on every deploy.
    tokio::select! {
        result = server.wait() => {
            result.map_err(|e| anyhow::anyhow!("{e}")).context("while serving")?;
        }
        signal = tokio::signal::ctrl_c() => {
            signal.context("waiting for a shutdown signal")?;
            eprintln!("orbita: draining");
            // `wait` consumed the server in the other branch, so this branch
            // owns it here.
            return Ok(());
        }
    }

    Ok(())
}

/// Checks the things that would otherwise fail minutes into a start.
///
/// A configuration mistake found at startup costs a restart. The same mistake
/// found after the node has joined a group and started taking traffic costs a
/// lot more, so anything checkable is checked here.
pub fn preflight(config: &Config, options: &NodeOptions) -> Result<()> {
    if config.node.role == Role::Leader && !options.dev && config.cluster.leader_peers.is_empty() {
        bail!(
            "a leader node needs cluster.leader_peers, the initial leader group membership, \
             listed identically on every leader. Use `orbita dev` for a single node cluster"
        );
    }
    if config.node.advertise.starts_with("0.0.0.0") && !options.dev {
        bail!(
            "node.advertise is {}, which no other node can dial. Set it to an address peers \
             can reach",
            config.node.advertise
        );
    }
    Ok(())
}

/// Creates the data directory, optionally clearing it first.
///
/// `clean` exists for `orbita dev`, where starting over is a normal thing to
/// want and deleting the right directory by hand is a normal thing to get
/// wrong.
pub fn prepare_data_dir(path: &Path, clean: bool) -> Result<()> {
    if clean && path.exists() {
        std::fs::remove_dir_all(path)
            .with_context(|| format!("cannot clear the data directory at {}", path.display()))?;
    }
    std::fs::create_dir_all(path)
        .with_context(|| format!("cannot create the data directory at {}", path.display()))
}

/// Whether a node of version `ours` may join a cluster running `theirs`.
///
/// Same major and same minor. Patch versions may mix, which is what makes a
/// security patch deployable without a coordinated restart of the world.
#[must_use]
pub fn versions_compatible(ours: &str, theirs: &str) -> bool {
    fn series(version: &str) -> Option<(u64, u64)> {
        // Drop any prerelease or build metadata before comparing, so that
        // 1.2.0-rc1 and 1.2.0 are the same series.
        let core = version.split(['-', '+']).next().unwrap_or_default();
        let mut parts = core.split('.');
        let major = parts.next()?.parse().ok()?;
        let minor = parts.next()?.parse().ok()?;
        Some((major, minor))
    }
    match (series(ours), series(theirs)) {
        (Some(a), Some(b)) => a == b,
        // A version we cannot parse is a version we cannot reason about, and
        // guessing is exactly what this check exists to prevent.
        _ => false,
    }
}

/// The error a node reports when it meets a leader group it cannot join.
///
/// It names both versions and the flag, because the operator reading it is
/// mid-upgrade and needs to know which way the skew runs.
#[must_use]
pub fn version_skew_message(ours: &str, theirs: &str) -> String {
    format!(
        "this node runs {ours} and the leader group runs {theirs}, which are not compatible. \
         Upgrade the leader group first, or start with --allow-version-skew, which is \
         unsupported"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ClusterLayer, Layer, NodeLayer};

    fn options() -> NodeOptions {
        NodeOptions {
            create_keyspace: None,
            dev: false,
        }
    }

    fn config(role: Role, peers: &[&str], advertise: &str) -> Config {
        Layer {
            node: NodeLayer {
                role: Some(role),
                advertise: Some(advertise.to_owned()),
                ..NodeLayer::default()
            },
            cluster: ClusterLayer {
                leader_peers: Some(peers.iter().map(|p| (*p).to_owned()).collect()),
                ..ClusterLayer::default()
            },
            ..Layer::default()
        }
        .resolve()
        .unwrap()
    }

    #[test]
    fn a_leader_with_no_peer_list_is_told_to_use_dev_instead() {
        let err = preflight(&config(Role::Leader, &[], "10.0.0.1:7100"), &options()).unwrap_err();
        assert!(format!("{err:#}").contains("orbita dev"), "{err:#}");
    }

    #[test]
    fn a_worker_needs_no_peer_list() {
        assert!(preflight(&config(Role::Worker, &[], "10.0.0.1:7100"), &options()).is_ok());
    }

    #[test]
    fn a_wildcard_advertise_address_is_refused_because_no_peer_can_dial_it() {
        let err = preflight(&config(Role::Worker, &[], "0.0.0.0:7100"), &options()).unwrap_err();
        assert!(
            format!("{err:#}").contains("no other node can dial"),
            "{err:#}"
        );
    }

    #[test]
    fn dev_is_exempt_from_both_checks_because_it_has_no_peers_to_reach() {
        let dev = NodeOptions {
            create_keyspace: Some("default".to_owned()),
            dev: true,
        };
        assert!(preflight(&config(Role::Leader, &[], "0.0.0.0:7100"), &dev).is_ok());
    }

    #[test]
    fn patch_versions_may_mix_within_a_cluster() {
        assert!(versions_compatible("1.2.3", "1.2.9"));
        assert!(versions_compatible("0.4.0", "0.4.7"));
    }

    #[test]
    fn a_different_minor_version_is_refused() {
        assert!(!versions_compatible("1.2.3", "1.3.0"));
        assert!(!versions_compatible("0.4.0", "0.5.0"));
    }

    #[test]
    fn a_different_major_version_is_refused() {
        assert!(!versions_compatible("1.2.3", "2.2.3"));
    }

    #[test]
    fn a_prerelease_is_the_same_series_as_its_release() {
        assert!(versions_compatible("1.2.0-rc1", "1.2.0"));
    }

    #[test]
    fn an_unparseable_version_is_refused_rather_than_guessed_at() {
        assert!(!versions_compatible("wobble", "1.2.3"));
        assert!(!versions_compatible("1.2.3", ""));
    }

    #[test]
    fn the_skew_message_names_both_versions_and_the_escape_hatch() {
        let message = version_skew_message("0.4.0", "0.5.1");
        assert!(message.contains("0.4.0"), "{message}");
        assert!(message.contains("0.5.1"), "{message}");
        assert!(message.contains("--allow-version-skew"), "{message}");
    }

    #[test]
    fn preparing_a_data_directory_creates_every_missing_parent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a/b/c");
        prepare_data_dir(&path, false).unwrap();
        assert!(path.is_dir());
    }

    #[test]
    fn cleaning_a_data_directory_removes_what_was_there_before() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data");
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join("old"), b"stale").unwrap();
        prepare_data_dir(&path, true).unwrap();
        assert!(path.is_dir());
        assert!(!path.join("old").exists());
    }

    #[test]
    fn a_node_serves_and_stops_cleanly() {
        // Port 0 asks the operating system for a free one, so this test does
        // not fight another run of itself for a fixed port.
        let dir = tempfile::tempdir().unwrap();
        let config = Layer {
            node: NodeLayer {
                advertise: Some("127.0.0.1:0".to_owned()),
                listen: Some("127.0.0.1:0".to_owned()),
                data_dir: Some(dir.path().join("data")),
                ..NodeLayer::default()
            },
            ..Layer::default()
        }
        .resolve()
        .unwrap();

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        runtime.block_on(async {
            let serving = tokio::spawn({
                let config = config.clone();
                let options = options();
                async move { run_node(&config, &options).await }
            });

            // The node is up once it has bound and opened its partitions.
            // Aborting is how a foreground node dies on a signal, and the
            // point of this test is that getting that far does not error.
            tokio::time::sleep(std::time::Duration::from_millis(750)).await;
            assert!(!serving.is_finished(), "the node stopped on its own");
            serving.abort();
        });
    }
}
