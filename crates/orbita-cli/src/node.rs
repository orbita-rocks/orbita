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
//!   orchestrator start order is not something anyone controls. The retry is
//!   exponential and capped, and it gives up after `cluster.join_timeout` so
//!   that a worker which will never join shows up as a restarting pod rather
//!   than a process quietly waiting forever.
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
//! This module used to refuse to start when the leader group ran a version it
//! did not match. `docs/adr/0005-upgrades-follow-kubernetes-rollouts.md`
//! supersedes that, and the reasoning is worth keeping because refusing looks
//! like the careful answer. Under a StatefulSet rolling update, the first
//! upgraded pod would exit, land in CrashLoopBackOff, and stall the rollout
//! with the cluster half upgraded and no forward path. A pod that runs and
//! reports itself not Ready stops the rollout at exactly one pod and keeps its
//! logs reachable.
//!
//! The replacement is a cluster version held in the control plane's replicated
//! state, separate from the binary version, with a window of the active
//! version and the one before it. A node outside the window starts, reports
//! not Ready, and says why. It does not exit.
//!
//! None of that exists yet, because there is no cluster version to read.
//! [`versions_compatible`] and [`version_skew_message`] compare binary versions
//! and are kept only so the tests that describe the old rule keep documenting
//! what changed. Nothing calls them. `--allow-version-skew` is likewise inert
//! and will be removed when the cluster version lands, because there will be
//! nothing left for it to override.
//!
//! # Two listeners
//!
//! A node binds a client gRPC listener and a peer listener on separate
//! addresses. `docs/adr/0004-peer-traffic-uses-private-framing.md` is the
//! record of why. The consequence for this module is that both addresses have
//! to be dialable by the people who dial them, and that the peer one is not
//! something to expose: it carries WAL bytes in the framing they have on disk
//! and it belongs on a private network. ADR 0005 corrects one line of ADR
//! 0004: peer framing is compatible within a cluster version window rather
//! than not at all, which is what makes a rolling upgrade possible.

use std::net::{SocketAddr, ToSocketAddrs};
use std::path::Path;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use orbita_core::{KeyspaceName, NodeId};
use orbita_server::{Server, ServerConfig};

use crate::config::{ClusterConfig, Config, Role};

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
/// This is deliberately the only call into `orbita-server` in the whole crate.
/// Every other command is a network client, so a change to the server's API is
/// a change to this one function rather than to twenty.
///
/// The client listener, the data directory, the node id, and the startup
/// keyspace are wired up. The peer listener and the leader peer list are
/// resolved and validated here but not yet handed to the server, because the
/// server does not accept them yet. The `TODO(peer-listener)` below is the
/// single place that changes when it does.
pub async fn run_node(config: &Config, options: &NodeOptions) -> Result<()> {
    preflight(config, options)?;
    prepare_data_dir(&config.node.data_dir, false)?;

    let listen = resolve("node.listen", &config.node.listen)?;
    let peer_listen = resolve("node.peer_listen", &config.node.peer_listen)?;

    // TODO(leader-peers): the leader peer list does not reach the server yet.
    // `ServerConfig::with_peers` wants pairs of node id and address, and
    // `cluster.leader_peers` is addresses alone, because an operator writing a
    // peer list should not also have to keep a node id table in sync with it.
    // Closing that gap is a question for the server's registration path, not
    // for this crate to guess at, so the list is carried and logged here and
    // wired up when there is somewhere to put it.
    if !config.cluster.leader_peers.is_empty() {
        tracing::info!(
            leader_peers = %config.cluster.leader_peers.join(","),
            peer_advertise = %config.node.peer_advertise,
            "the leader group is configured but this build does not yet register with it"
        );
    }

    // A worker whose leader group is not up yet retries instead of failing.
    // Nobody chooses start order in an orchestrator, and a worker that exits
    // because it was scheduled first turns an ordinary rollout into a crash
    // loop that resolves itself only by luck.
    let joining =
        config.node.role == Role::Worker && !options.dev && !config.cluster.leader_peers.is_empty();
    let mut backoff = JoinBackoff::new(&config.cluster);

    let server = loop {
        match Server::start(server_config(config, options, listen, peer_listen)?).await {
            Ok(server) => break server,
            Err(error) if joining => {
                let Some(delay) = backoff.next_delay() else {
                    return Err(anyhow::anyhow!("{error}"))
                        .context(join_give_up_message(&config.cluster));
                };
                tracing::warn!(
                    error = %error,
                    leader_peers = %config.cluster.leader_peers.join(","),
                    retry_in_millis = delay.as_millis() as u64,
                    "cannot join the leader group yet, retrying"
                );
                tokio::time::sleep(delay).await;
            }
            Err(error) => {
                return Err(anyhow::anyhow!("{error}")).context("starting the node");
            }
        }
    };

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

/// Builds the server configuration for one start attempt.
///
/// It is rebuilt per attempt rather than cloned because a `ServerConfig` owns
/// its partition map source, and a retry after a failed start needs a fresh
/// one rather than the one that already failed.
fn server_config(
    config: &Config,
    options: &NodeOptions,
    listen: SocketAddr,
    peer_listen: SocketAddr,
) -> Result<ServerConfig> {
    let mut server_config = ServerConfig::single_node(&config.node.data_dir)
        .with_node_id(NodeId(config.node.id))
        .with_listen_addr(listen)
        .with_peer_listen_addr(peer_listen);

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
    Ok(server_config)
}

/// Turns a configured address into a socket address.
///
/// Resolving here rather than at parse time means a hostname in the config is
/// allowed, which is what a container orchestrator tends to hand you.
fn resolve(option: &str, address: &str) -> Result<SocketAddr> {
    address
        .to_socket_addrs()
        .with_context(|| format!("resolving {option}, {address}"))?
        .next()
        .with_context(|| format!("{option} resolved to nothing, {address}"))
}

/// How long a node waits between attempts to reach the leader group.
///
/// The schedule doubles from the initial delay up to the maximum and stays
/// there, so a group that comes up late is still found within the maximum of
/// coming up, and a group that never comes up is not hammered. There is no
/// jitter: a node goes through `orbita_runtime::Runtime` for anything the
/// simulator has to reproduce, and a startup retry is not worth an unreachable
/// random number generator here.
#[derive(Debug, Clone)]
pub struct JoinBackoff {
    next: Duration,
    max: Duration,
    /// Zero means keep trying forever, which is what an operator who would
    /// rather have a hung pod than a restarting one asks for.
    budget: Duration,
    spent: Duration,
}

impl JoinBackoff {
    #[must_use]
    pub fn new(cluster: &ClusterConfig) -> Self {
        Self {
            next: Duration::from_millis(cluster.join_backoff_initial_millis),
            max: Duration::from_millis(cluster.join_backoff_max_millis),
            budget: Duration::from_millis(cluster.join_timeout_millis),
            spent: Duration::ZERO,
        }
    }

    /// How long to wait before the next attempt, or `None` when the budget is
    /// spent and the node should give up.
    pub fn next_delay(&mut self) -> Option<Duration> {
        let delay = self.next.min(self.max);
        if !self.budget.is_zero() && self.spent + delay > self.budget {
            return None;
        }
        self.spent += delay;
        self.next = self.next.saturating_mul(2).min(self.max);
        Some(delay)
    }
}

/// What a node says when it has waited as long as it was told to.
///
/// It names the addresses it was trying, because the usual cause is a peer
/// address that resolves to nothing or points at the client port.
#[must_use]
pub fn join_give_up_message(cluster: &ClusterConfig) -> String {
    format!(
        "gave up reaching the leader group after {} ms. Tried {}. Check that these are peer \
         addresses rather than client ones, that they resolve, and raise cluster.join_timeout \
         or set it to 0 to keep trying",
        cluster.join_timeout_millis,
        cluster.leader_peers.join(", ")
    )
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
    if !options.dev {
        if let Some(option) = unroutable("node.advertise", &config.node.advertise) {
            bail!(
                "{option} is {}, which nothing can dial. Set it to an address clients can reach",
                config.node.advertise
            );
        }
        // The same check for the peer address, because a peer advertise
        // address nobody can dial is the failure that looks like a healthy
        // node with an empty partition map.
        if let Some(option) = unroutable("node.peer_advertise", &config.node.peer_advertise) {
            bail!(
                "{option} is {}, which no other node can dial. Set it to an address peers can \
                 reach on your private network",
                config.node.peer_advertise
            );
        }
        if config.node.advertise == config.node.peer_advertise {
            bail!(
                "node.advertise and node.peer_advertise are both {}, but clients and peers reach \
                 this node on two different listeners. Give the peer listener its own port",
                config.node.advertise
            );
        }
    }
    Ok(())
}

/// Names the option when an advertise address is one nobody can dial.
///
/// A wildcard bind address means "every interface" to the kernel and nothing
/// at all to a peer that tries to connect to it, and the unspecified IPv6
/// address is the same mistake spelled differently.
fn unroutable<'a>(option: &'a str, address: &str) -> Option<&'a str> {
    let host = address.rsplit_once(':').map_or(address, |(host, _)| host);
    let host = host.trim_start_matches('[').trim_end_matches(']');
    matches!(host, "0.0.0.0" | "::" | "*").then_some(option)
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
///
/// Superseded by ADR 0005 and called by nothing. Compatibility is a property
/// of the cluster version rather than of the binary version, because the client
/// API, the peer framing, the write-ahead log, the storage records, and the
/// replicated control plane state change at different rates and one binary
/// version cannot answer for all five.
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
///
/// Superseded by ADR 0005 along with [`versions_compatible`]. A node outside
/// the supported window reports itself not Ready rather than failing to start,
/// so this text describes an outcome that no longer happens.
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
        layer(role, peers, advertise).resolve().unwrap()
    }

    fn layer(role: Role, peers: &[&str], advertise: &str) -> Layer {
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
    fn a_wildcard_advertise_address_is_refused_because_nothing_can_dial_it() {
        let err = preflight(&config(Role::Worker, &[], "0.0.0.0:7100"), &options()).unwrap_err();
        assert!(format!("{err:#}").contains("nothing can dial"), "{err:#}");
    }

    #[test]
    fn a_wildcard_peer_advertise_address_is_refused_the_same_way_as_the_client_one() {
        let mut layer = layer(Role::Worker, &[], "10.0.0.1:7100");
        layer.node.peer_advertise = Some("0.0.0.0:7101".to_owned());
        let err = preflight(&layer.resolve().unwrap(), &options()).unwrap_err();
        assert!(
            format!("{err:#}").contains("no other node can dial"),
            "{err:#}"
        );
    }

    #[test]
    fn an_unspecified_ipv6_peer_advertise_address_is_refused_too() {
        let mut layer = layer(Role::Worker, &[], "10.0.0.1:7100");
        layer.node.peer_advertise = Some("[::]:7101".to_owned());
        let err = preflight(&layer.resolve().unwrap(), &options()).unwrap_err();
        assert!(
            format!("{err:#}").contains("no other node can dial"),
            "{err:#}"
        );
    }

    #[test]
    fn one_address_for_both_listeners_is_refused_because_a_node_binds_two() {
        let mut layer = layer(Role::Worker, &[], "10.0.0.1:7100");
        layer.node.peer_advertise = Some("10.0.0.1:7100".to_owned());
        let err = preflight(&layer.resolve().unwrap(), &options()).unwrap_err();
        assert!(format!("{err:#}").contains("its own port"), "{err:#}");
    }

    #[test]
    fn the_peer_advertise_address_defaults_to_the_client_host_and_the_peer_port() {
        let config = config(Role::Worker, &[], "worker-1:7100");
        assert_eq!(config.node.peer_advertise, "worker-1:7101");
    }

    fn backoff(initial: u64, max: u64, timeout: u64) -> JoinBackoff {
        let mut cluster = config(Role::Worker, &[], "10.0.0.1:7100").cluster;
        cluster.join_backoff_initial_millis = initial;
        cluster.join_backoff_max_millis = max;
        cluster.join_timeout_millis = timeout;
        JoinBackoff::new(&cluster)
    }

    #[test]
    fn the_join_backoff_doubles_until_it_reaches_the_maximum_and_stays_there() {
        let mut backoff = backoff(100, 400, 0);
        let waits: Vec<u64> = (0..5)
            .map(|_| backoff.next_delay().unwrap().as_millis() as u64)
            .collect();
        assert_eq!(waits, [100, 200, 400, 400, 400]);
    }

    #[test]
    fn a_zero_join_timeout_means_a_worker_never_stops_trying() {
        let mut backoff = backoff(1_000, 1_000, 0);
        for _ in 0..1_000 {
            assert!(backoff.next_delay().is_some());
        }
    }

    #[test]
    fn a_worker_gives_up_once_it_has_waited_as_long_as_it_was_told_to() {
        let mut backoff = backoff(1_000, 1_000, 2_500);
        assert!(backoff.next_delay().is_some());
        assert!(backoff.next_delay().is_some());
        // A third wait would run past the budget, so it is refused rather than
        // truncated: waiting less than the backoff says would be a busy loop.
        assert!(backoff.next_delay().is_none());
    }

    #[test]
    fn giving_up_names_the_addresses_that_were_tried_and_how_to_wait_longer() {
        let cluster = config(Role::Worker, &["leader-1:7101", "leader-2:7101"], "w:7100").cluster;
        let message = join_give_up_message(&cluster);
        assert!(message.contains("leader-1:7101"), "{message}");
        assert!(message.contains("leader-2:7101"), "{message}");
        assert!(message.contains("join_timeout"), "{message}");
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
                // Port 0 for the peer listener too, or two runs of this test
                // would fight over the default peer port.
                peer_listen: Some("127.0.0.1:0".to_owned()),
                peer_advertise: Some("127.0.0.1:1".to_owned()),
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
