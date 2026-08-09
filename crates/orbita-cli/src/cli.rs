//! The command surface.
//!
//! This module is clap types and nothing else, so that the shape of the tool
//! can be read in one file. The help text here is the documentation most
//! people will actually read, so it is written to be sufficient on its own:
//! the quickstart in the README should be a formality for anyone who runs
//! `orbita --help`.
//!
//! Every command that talks to a cluster does so through the same gRPC API a
//! program would use. There is no private channel and no second
//! implementation, which is what makes "anything an operator can do is
//! scriptable" true by construction rather than by discipline.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

use crate::config::{ClientLayer, ClusterLayer, Layer, NodeLayer, Role, TelemetryLayer};
use crate::output::Format;

// The duration parser lives with the configuration because a duration in a
// file and a duration on a flag have to mean the same thing. It is re-exported
// here so that the value parsers below read as one piece.
pub use crate::config::parse_duration_millis;

/// Something went wrong, and the tool has said what.
pub const EXIT_ERROR: i32 = 1;
/// A `get` found no key. This is a separate code from an error because an
/// absent key is a normal answer, and a script checking a lock should not have
/// to parse output to tell the two apart.
pub const EXIT_NOT_FOUND: i32 = 2;
/// A conditional `set` or `delete` did not apply. Losing a compare-and-swap is
/// the expected outcome of an election, not a failure.
pub const EXIT_CONDITION_NOT_MET: i32 = 3;

/// What a command produced: the text to print and the code to exit with.
///
/// The code is carried alongside the output rather than returned by printing
/// so that "the key was absent" and "the condition failed" can be normal
/// results with their own exit codes instead of errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    pub text: String,
    pub code: i32,
}

impl Outcome {
    /// A command that succeeded.
    #[must_use]
    pub fn ok(text: String) -> Self {
        Self { text, code: 0 }
    }

    /// A command that worked but whose answer was no.
    #[must_use]
    pub fn with_code(text: String, code: i32) -> Self {
        Self { text, code }
    }
}

const ABOUT: &str = "A strongly consistent, multitenant key-value store.";

const LONG_ABOUT: &str = "\
A strongly consistent, multitenant key-value store.

One binary does everything. `orbita serve` runs a combined node. Every node
serves worker traffic and an automatically managed subset votes in Raft. Every other command is a
client that talks to a running cluster over the same gRPC API a program would
use, so anything you can do here you can script.

Start here:

  orbita dev &                     a single node cluster on 127.0.0.1:7100
  orbita keyspace create demo
  orbita set demo greeting hello
  orbita get demo greeting

Configuration comes from four places, each beating the one before it: built-in
defaults, a TOML file, environment variables, then flags. Run `orbita config
show` to see what was resolved and `orbita config env` for the variables that
are read.

Exit codes: 0 success, 1 error, 2 the key was not found, 3 a conditional write
was not applied. Code 3 means the cluster decided against you. A conditional
write the cluster could not decide at all is an error, exits 1, and is safe to
retry.";

#[derive(Debug, Parser)]
#[command(
    name = "orbita",
    version = crate::LONG_VERSION,
    about = ABOUT,
    long_about = LONG_ABOUT,
    disable_help_subcommand = true
)]
pub struct Cli {
    #[command(flatten)]
    pub global: GlobalArgs,

    #[command(subcommand)]
    pub command: Command,
}

/// Options that mean the same thing to every command.
///
/// They sit under their own heading so that a subcommand's help shows the
/// flags specific to it first, which is what the reader came for.
#[derive(Debug, Args, Default)]
#[command(next_help_heading = "Global options")]
pub struct GlobalArgs {
    /// Path to the configuration file.
    ///
    /// Without this the tool tries ORBITA_CONFIG, then ./orbita.toml, then
    /// /etc/orbita/orbita.toml, and runs on defaults if none exist. A path
    /// given here must exist, because silently ignoring it would start a node
    /// with settings nobody chose.
    #[arg(short, long, value_name = "PATH", global = true)]
    pub config: Option<PathBuf>,

    /// How to print results. Use json in scripts; the human tables are not a
    /// stable interface and the JSON field names are.
    #[arg(short, long, value_name = "FORMAT", global = true)]
    pub output: Option<Format>,

    /// The cluster to talk to, for every command except serve and dev.
    ///
    /// Any worker will do. A worker that does not own the key forwards the
    /// request internally, so there is nothing to route on the client side.
    #[arg(long, value_name = "URL", global = true)]
    pub endpoint: Option<String>,

    /// The credential secret to authenticate with.
    ///
    /// Prefer ORBITA_CREDENTIAL or the configuration file. A secret on the
    /// command line is visible to every other process on the machine.
    #[arg(long, value_name = "SECRET", global = true)]
    pub credential: Option<String>,

    /// Log verbosity, as a tracing filter such as info or orbita=debug.
    #[arg(long, value_name = "LEVEL", global = true)]
    pub log_level: Option<String>,
}

impl GlobalArgs {
    /// The configuration layer the global flags contribute.
    #[must_use]
    pub fn layer(&self) -> Layer {
        Layer {
            client: ClientLayer {
                endpoint: self.endpoint.clone(),
                credential: self.credential.clone(),
                // The default keyspace is not a global flag: it comes from
                // ORBITA_KEYSPACE, the config file, or the REPL's `:use`, so a
                // one-shot command still names its keyspace on the line.
                keyspace: None,
            },
            telemetry: TelemetryLayer {
                log_level: self.log_level.clone(),
                ..TelemetryLayer::default()
            },
            ..Layer::default()
        }
    }

    #[must_use]
    pub fn format(&self) -> Format {
        self.output.unwrap_or_default()
    }
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run a node.
    #[command(long_about = "\
Run a combined clustered node.

Every node owns or replicates partitions and serves reads and writes. Three or
five eligible nodes additionally vote in Raft, and one voter is elected leader.

A node binds two listeners. --listen carries client and admin gRPC, and is the
one to expose. --peer-listen carries traffic from other nodes in a private
framing, and belongs on a private network: it is compatible only within a
cluster version window, and a peer port reachable from the internet is a hole.

Fresh nodes agree on a durable identity and initial three-voter certificate
through the shared object store. --leader-peers remains only for migration from
the old fixed-role topology.")]
    Serve(ServeArgs),

    /// Run a single node cluster with no configuration at all.
    #[command(long_about = "\
Run a single node cluster on this machine, for trying things out.

The node is its own leader group and its own worker, so there is no quorum to
form and no peer list to write down. State goes under the data directory,
which you can delete when you are done. There is no object store, so this is
not a durable deployment and is not meant to be one.")]
    Dev(DevArgs),

    /// Create, inspect, and change keyspaces.
    #[command(long_about = "\
Keyspaces are the unit of tenancy. Each one is an isolated, independently
partitioned key namespace with its own credentials, quotas, and defaults.

A new keyspace starts as a single partition covering the whole range and
splits as it grows, so there is no partition count to choose up front.")]
    Keyspace {
        #[command(subcommand)]
        command: KeyspaceCommand,
    },

    /// Issue and revoke credentials.
    Credential {
        #[command(subcommand)]
        command: CredentialCommand,
    },

    /// Inspect the cluster.
    Cluster {
        #[command(subcommand)]
        command: ClusterCommand,
    },

    /// Split, merge, and move partitions by hand.
    #[command(long_about = "\
The leader group splits, merges, and rebalances on its own. These commands
exist for the times an operator needs to force the issue, such as splitting
ahead of a load test or draining a node before maintenance.")]
    Partition {
        #[command(subcommand)]
        command: PartitionCommand,
    },

    /// Read a key.
    Get(GetArgs),

    /// Write a key.
    Set(SetArgs),

    /// Remove a key.
    Delete(DeleteArgs),

    /// List keys under a prefix.
    List(ListArgs),

    /// Inspect the resolved configuration.
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },

    /// Open an interactive session against a cluster.
    #[command(long_about = "\
Open one session and type commands into it, instead of paying a fresh process,
runtime, and connection for every line.

This is a loop, not a second tool. Each line is parsed by the same argument
parser and printed by the same renderer as the one-shot `orbita` command, so
anything you can type here you can script, and the reverse. `orbita get demo k`
on the command line and `get demo k` at the prompt do the same thing.

The session remembers a current keyspace and output format so a data command
can leave its keyspace off. Set them with the session commands, which start
with a colon so they can never be confused with a cluster command:

  :use <keyspace>      set the current keyspace (`:use` with no name clears it)
  :format <human|json> switch output format for the rest of the session
  :keyspace            show the current keyspace
  :help                list the session commands
  :quit                leave (Ctrl-D does the same)

A missed `get` and a lost compare-and-swap have no exit code to land in here,
so they print their normal output and then a `[not found]` or `[condition not
met]` note, which is the same answer a script reads from exit code 2 or 3.

serve and dev run a node and block forever, so they are refused here. So is
reading a `set` value from standard input, because the line editor owns stdin;
pass the value as an argument instead.")]
    Repl(ReplArgs),
}

/// Options for the interactive session.
///
/// It is its own args struct, empty today, so that a later flag such as a
/// startup script or a one-shot `--command` has somewhere to land without
/// reshaping the command enum.
#[derive(Debug, Args, Default)]
pub struct ReplArgs {}

#[derive(Debug, Args)]
pub struct ServeArgs {
    /// This node's id, which must be unique in the cluster and stable across
    /// restarts.
    #[arg(long, value_name = "ID")]
    pub node_id: Option<u64>,

    /// The node role. Use node; leader and worker are migration spellings.
    #[arg(long, value_name = "ROLE")]
    pub role: Option<Role>,

    /// The address to bind for client and admin gRPC traffic.
    #[arg(long, value_name = "ADDR")]
    pub listen: Option<String>,

    /// The address clients should dial to reach this one. Required in any
    /// deployment where listen is a wildcard, because nothing can dial
    /// 0.0.0.0.
    #[arg(long, value_name = "ADDR")]
    pub advertise: Option<String>,

    /// The address to bind for traffic from other nodes.
    ///
    /// This is a second listener, not the client port. Bind it to a private
    /// interface: peer traffic carries WAL bytes and control plane messages in
    /// a private framing, and a peer port reachable from the internet is a
    /// hole.
    #[arg(long, value_name = "ADDR")]
    pub peer_listen: Option<String>,

    /// The address other nodes should dial to reach this one, which is what
    /// goes in every node's leader peer list.
    #[arg(long, value_name = "ADDR")]
    pub peer_advertise: Option<String>,

    /// Where the WAL, the local partition objects, and the Raft log live.
    #[arg(long, value_name = "PATH")]
    pub data_dir: Option<PathBuf>,

    /// The leader group, as NODE_ID=ADDR entries.
    ///
    /// Identical on every node. A leader uses the ids as fixed Raft voters and
    /// checks them against durable state on restart. A worker uses the same
    /// entries to contact the group.
    #[arg(long, value_name = "NODE_ID=ADDR", value_delimiter = ',')]
    pub leader_peers: Option<Vec<String>>,

    /// Desired Raft voters, independent from worker count. Must be 3 or 5.
    #[arg(long, value_name = "3|5")]
    pub voter_target: Option<usize>,

    /// Failure domain used to spread voters, such as an availability zone.
    #[arg(long, value_name = "NAME")]
    pub failure_domain: Option<String>,

    /// Keep this node out of automatic voter placement.
    #[arg(long)]
    pub voter_ineligible: bool,

    /// How long to keep trying to reach the leader group before giving up,
    /// such as 5m. Use 0 to retry forever.
    ///
    /// A worker that starts before the leader group retries rather than
    /// failing, because start order in an orchestrator is nobody's choice.
    /// Giving up eventually is deliberate: a crash loop is visible and a
    /// process retrying silently for a day is not.
    #[arg(long, value_name = "DURATION")]
    pub join_timeout: Option<String>,

    /// Start even if the leader group runs an incompatible version.
    ///
    /// Inert. Refusing to start on a version mismatch was replaced by a
    /// cluster version window, under which a node outside the window starts,
    /// reports itself not ready, and says why, because a pod that exits stalls
    /// a rolling update with the cluster half upgraded. Nothing checks a
    /// version yet, so this flag overrides nothing and will be removed.
    #[arg(long)]
    pub allow_version_skew: bool,
}

impl ServeArgs {
    /// The configuration layer the serve flags contribute.
    #[must_use]
    pub fn layer(&self) -> Layer {
        Layer {
            node: NodeLayer {
                id: self.node_id,
                role: self.role,
                listen: self.listen.clone(),
                advertise: self.advertise.clone(),
                peer_listen: self.peer_listen.clone(),
                peer_advertise: self.peer_advertise.clone(),
                data_dir: self.data_dir.clone(),
            },
            cluster: ClusterLayer {
                leader_peers: self.leader_peers.clone(),
                voter_target: self.voter_target,
                voter_eligible: self.voter_ineligible.then_some(false),
                failure_domain: self.failure_domain.clone(),
                allow_version_skew: self.allow_version_skew.then_some(true),
                join_timeout: self.join_timeout.clone(),
                ..ClusterLayer::default()
            },
            ..Layer::default()
        }
    }
}

#[derive(Debug, Args)]
pub struct DevArgs {
    /// The client port, bound to the loopback address only. The peer listener
    /// takes the port above it.
    #[arg(long, default_value_t = crate::config::DEFAULT_PORT, value_name = "PORT")]
    pub port: u16,

    /// Where to keep state. Delete this directory to start over.
    #[arg(long, default_value = ".orbita/dev", value_name = "PATH")]
    pub data_dir: PathBuf,

    /// Remove the data directory before starting, for a guaranteed clean run.
    #[arg(long)]
    pub clean: bool,

    /// A keyspace to create on startup if it does not exist, so that the first
    /// write does not need a second command.
    #[arg(long, default_value = "default", value_name = "NAME")]
    pub keyspace: String,
}

#[derive(Debug, Subcommand)]
pub enum KeyspaceCommand {
    /// Create a keyspace.
    Create {
        /// The name clients will use. Unique within the cluster.
        name: String,

        #[command(flatten)]
        config: KeyspaceConfigArgs,
    },

    /// List every keyspace and what it is using.
    List,

    /// Change a keyspace's limits and defaults.
    ///
    /// Only the options given are changed. Everything else is left as it is.
    Update {
        name: String,

        #[command(flatten)]
        config: KeyspaceConfigArgs,
    },

    /// Delete a keyspace and everything in it.
    #[command(long_about = "\
Delete a keyspace and destroy its data.

The name has to be given twice, once as the argument and once as --confirm.
That is deliberate friction: a script that deletes the wrong keyspace has to
get the same name wrong in two places.")]
    Delete {
        name: String,

        /// The keyspace name again, to confirm.
        #[arg(long, value_name = "NAME")]
        confirm: String,
    },
}

/// The per-keyspace limits, shared by create and update.
#[derive(Debug, Args, Default)]
pub struct KeyspaceConfigArgs {
    /// TTL applied to writes that do not carry their own, such as 30s or 1h.
    /// Unset means keys do not expire by default.
    #[arg(long, value_name = "DURATION", value_parser = parse_duration_millis)]
    pub default_ttl: Option<u64>,

    /// Per-keyspace value size cap. Cannot exceed the cluster maximum.
    #[arg(long, value_name = "BYTES")]
    pub max_value_bytes: Option<u64>,

    /// Storage quota across every partition of this keyspace.
    #[arg(long, value_name = "BYTES")]
    pub max_storage_bytes: Option<u64>,

    /// Read rate cap, so one tenant cannot starve the others.
    #[arg(long, value_name = "N")]
    pub max_reads_per_second: Option<u32>,

    /// Write rate cap.
    #[arg(long, value_name = "N")]
    pub max_writes_per_second: Option<u32>,
}

#[derive(Debug, Subcommand)]
pub enum CredentialCommand {
    /// Issue a credential scoped to one or more keyspaces.
    #[command(long_about = "\
Issue a credential.

The secret is returned once, at creation, and is not stored in a form anyone
can read back. If it is lost, revoke the credential and issue another.")]
    Create {
        /// A keyspace this credential may touch. Repeat for more than one. A
        /// credential scoped to nothing is always a mistake, so at least one
        /// is required.
        #[arg(long, value_name = "NAME", required = true)]
        keyspace: Vec<String>,

        /// What the credential may do. Repeat to grant both.
        #[arg(long, value_name = "PERMISSION", required = true)]
        permission: Vec<PermissionArg>,

        /// A note about who or what this is for, so the list is readable in a
        /// year.
        #[arg(long, default_value = "", value_name = "TEXT")]
        description: String,

        /// Expire the credential this long from now, such as 90d.
        #[arg(long, value_name = "DURATION", value_parser = parse_duration_millis)]
        expires_in: Option<u64>,
    },

    /// Revoke a credential immediately.
    Revoke { credential_id: String },
}

/// What a credential is allowed to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
#[clap(rename_all = "lowercase")]
pub enum PermissionArg {
    Read,
    Write,
}

#[derive(Debug, Subcommand)]
pub enum ClusterCommand {
    /// Show the partition map and node health.
    #[command(long_about = "\
Show every node, its health, and every partition with its owner, epoch, and
replicas.

Each replica is printed with how far it trails the owner's committed lamport.
That number is what decides whether a replica can serve a linearizable read
locally and how far behind it would be if it were promoted, so it is the first
thing to look at during a failover.")]
    Describe {
        /// Only show partitions belonging to this keyspace.
        #[arg(long, value_name = "NAME")]
        keyspace: Option<String>,
    },

    /// Check that a node is answering, for a container liveness check.
    #[command(long_about = "\
Ask a node whether it is up, and exit 0 if it answered.

This is what a liveness probe should run. It is deliberately weaker than
`cluster describe`: any answer at all counts, even an error, because the
question is whether the process is serving rather than whether the cluster is
well. Only a connection that could not be made or a request that timed out
counts as down.

Use `cluster ready` for a readiness probe, `cluster describe` to find out
whether the cluster is healthy, and this to find out whether one process is
alive.")]
    Ping,

    /// Check that a node is ready to serve, for a readiness probe.
    #[command(long_about = "\
Ask a node whether it is ready to serve, and exit 0 only if it is.

Ready is stronger than answering. A node is ready once it has registered with
the leader group, recovered its write-ahead log, and opened and caught up
every partition the map says it holds. Until then this exits non-zero and
names the conditions still outstanding, which is what makes a rolling upgrade
wait for the node instead of outrunning it.

This is what a Kubernetes readiness or startup probe should run. Liveness
should keep running `cluster ping`: readiness depends on the leader group, and
a liveness probe that does would restart healthy pods during a control plane
outage.")]
    Ready,

    /// Advance the cluster version after a rolling upgrade.
    #[command(long_about = "\
Advance the cluster's active version to the newest one every live node can
speak. Run it once every node is upgraded and you are happy with the result.

Until this runs, upgraded nodes keep speaking the old version and a rollback
is the ordinary Kubernetes one, because nothing new has been written. After it
runs, nodes start writing new formats and rolling back is not supported: the
only path backwards is restoring from a backup taken before the upgrade. That
is why finalization is a command and not automatic.

If any live node cannot speak the new version, nothing is committed and the
error names the nodes holding it back.")]
    FinalizeUpgrade,
}

#[derive(Debug, Subcommand)]
pub enum PartitionCommand {
    /// Split a partition in two.
    Split {
        partition_id: u64,

        /// The key to split at. Without it the owner picks a midpoint by size.
        #[arg(long, value_name = "KEY")]
        at: Option<String>,
    },

    /// Merge two adjacent partitions into one.
    Merge {
        /// The lower partition of the pair.
        lower_partition_id: u64,
        /// The upper partition, which must start where the lower one ends.
        upper_partition_id: u64,
    },

    /// Move ownership of a partition to another node.
    #[command(long_about = "\
Hand a partition to a different owner.

The target must already be a replica. Transferring to a node with no data
would leave the partition unavailable until it hydrated, which is a failover,
not a transfer.")]
    Transfer {
        partition_id: u64,

        /// The node to hand it to.
        #[arg(long, value_name = "NODE_ID")]
        to: u64,
    },
}

#[derive(Debug, Args)]
#[command(long_about = "\
Read a key.

The keyspace comes first: `get demo greeting`. It may be left off when a
default keyspace is in effect, from ORBITA_KEYSPACE, `client.keyspace` in the
configuration, or `:use` in the REPL, so that `get greeting` reads from the
current keyspace. Passing both a keyspace and a key always names the keyspace
first, so a two-argument `get demo greeting` means the same thing no matter
what the environment holds.

A miss is not an error. The command prints the key as not found and exits 2, so
a script checking a lock can branch on the code without parsing anything.")]
pub struct GetArgs {
    /// The keyspace to read from, or the key when a default keyspace is set.
    pub keyspace: Option<String>,
    /// The key to read.
    pub key: Option<String>,

    /// Name the keyspace explicitly, ahead of any positional or default.
    ///
    /// The unambiguous form: with it, the positionals are only the key, so
    /// there is never a question of which argument is the keyspace.
    #[arg(short = 'k', long = "keyspace", value_name = "NAME")]
    pub keyspace_flag: Option<String>,
}

#[derive(Debug, Args)]
#[command(long_about = "\
Write a key.

The value can be an argument, a file, or standard input, because a value is
bytes and not every byte survives a shell.

The keyspace comes first: `set demo key value`. It may be left off when a
default keyspace is in effect (ORBITA_KEYSPACE, `client.keyspace`, or the
REPL's `:use`), in which case `set key value` writes to the current keyspace.
Because the value is optional, a two-argument `set a b` is read as keyspace and
key when no default is set, and as key and value when one is.

When the value comes from a file or standard input there is no third argument
to break the tie, so name the keyspace with --keyspace to be unambiguous:
`printf secret | orbita set --keyspace prod locks/leader` writes the piped bytes
to `locks/leader` in `prod`, no matter what default is set. With --keyspace the
positionals are only the key and an optional value.

Conditions are what make this usable for locks and catalog pointers.
--if-not-present takes a lock; --if-version swings a pointer only if nobody
else moved it first. A condition that is not met is not an error: the command
exits 3 and reports the version it found instead.

Exit 3 is a verdict, so it is the one answer a script may act on without
retrying. A conditional write that collides with another write the cluster has
not finished landing is refused as UNAVAILABLE and exits 1 instead, because
nobody yet knows who won; retry it.")]
pub struct SetArgs {
    /// The keyspace to write to, or the key when a default keyspace is set.
    pub keyspace: Option<String>,
    /// The key to write, or the value when a default keyspace is set.
    pub key: Option<String>,
    /// The value. Omit it to read the value from standard input.
    pub value: Option<String>,

    /// Name the keyspace explicitly, ahead of any positional or default.
    ///
    /// The unambiguous form, and the one to use when the value comes from a
    /// file or standard input: with it the positionals are only the key and an
    /// optional value, so `set <keyspace> <key>` can never be misread as
    /// `set <key> <value>`.
    #[arg(short = 'k', long = "keyspace", value_name = "NAME")]
    pub keyspace_flag: Option<String>,

    /// Read the value from this file instead.
    #[arg(long, value_name = "PATH")]
    pub value_file: Option<PathBuf>,

    /// Expire the key this long after the write commits, such as 30s or 1h.
    #[arg(long, value_name = "DURATION", value_parser = parse_duration_millis)]
    pub ttl: Option<u64>,

    /// Write only if the key does not exist. An expired key counts as absent.
    #[arg(long, conflicts_with = "if_version")]
    pub if_not_present: bool,

    /// Write only if the key is at exactly this version.
    #[arg(long, value_name = "VERSION")]
    pub if_version: Option<u64>,
}

#[derive(Debug, Args)]
pub struct DeleteArgs {
    /// The keyspace to delete from, or the key when a default keyspace is set.
    pub keyspace: Option<String>,
    /// The key to delete.
    pub key: Option<String>,

    /// Name the keyspace explicitly, ahead of any positional or default.
    #[arg(short = 'k', long = "keyspace", value_name = "NAME")]
    pub keyspace_flag: Option<String>,

    /// Delete only if the key is at exactly this version.
    #[arg(long, value_name = "VERSION")]
    pub if_version: Option<u64>,
}

#[derive(Debug, Args)]
#[command(long_about = "\
List keys under a prefix, one page at a time.

Each page is a consistent snapshot of a single partition's range. A scan that
spans several pages is not a point-in-time snapshot of the keyspace, and a
caller that needs one has to build it. The cursor is printed rather than
followed automatically so that this stays true and visible.")]
pub struct ListArgs {
    /// The keyspace to scan, or the prefix when a default keyspace is set.
    pub keyspace: Option<String>,
    /// The prefix to match. Empty scans the whole keyspace.
    pub prefix: Option<String>,

    /// Name the keyspace explicitly, ahead of any positional or default.
    ///
    /// With it, the single positional is unambiguously the prefix.
    #[arg(short = 'k', long = "keyspace", value_name = "NAME")]
    pub keyspace_flag: Option<String>,

    /// Continue from the cursor a previous page returned.
    #[arg(long, value_name = "CURSOR")]
    pub cursor: Option<String>,

    /// Maximum entries to return, capped by the server.
    #[arg(long, default_value_t = 100, value_name = "N")]
    pub limit: u32,

    /// Return values as well as keys. Without this the server never reads
    /// values it would only discard.
    #[arg(long)]
    pub values: bool,
}

#[derive(Debug, Subcommand)]
pub enum ConfigCommand {
    /// Print the resolved configuration, with secrets omitted.
    ///
    /// This is the answer to "which of my four configuration sources won".
    Show,

    /// List every environment variable the tool reads.
    Env,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn the_command_surface_is_internally_consistent() {
        // Catches conflicting short flags, duplicate argument names, and the
        // other structural mistakes clap can only find at runtime.
        Cli::command().debug_assert();
    }

    #[test]
    fn durations_require_a_unit_so_a_ttl_cannot_be_wrong_by_a_thousand() {
        assert_eq!(parse_duration_millis("500ms").unwrap(), 500);
        assert_eq!(parse_duration_millis("30s").unwrap(), 30_000);
        assert_eq!(parse_duration_millis("5m").unwrap(), 300_000);
        assert_eq!(parse_duration_millis("2h").unwrap(), 7_200_000);
        assert_eq!(parse_duration_millis("7d").unwrap(), 604_800_000);
        assert!(parse_duration_millis("30").is_err());
        assert!(parse_duration_millis("30 weeks").is_err());
        assert!(parse_duration_millis("s").is_err());
    }

    #[test]
    fn global_flags_are_accepted_after_a_subcommand() {
        let cli = Cli::try_parse_from(["orbita", "keyspace", "list", "--output", "json"]).unwrap();
        assert_eq!(cli.global.format(), Format::Json);
    }

    #[test]
    fn serve_flags_become_a_configuration_layer_that_beats_the_file() {
        let cli = Cli::try_parse_from([
            "orbita",
            "serve",
            "--role",
            "leader",
            "--node-id",
            "3",
            "--leader-peers",
            "1=a:7101,2=b:7101",
        ])
        .unwrap();
        let Command::Serve(args) = cli.command else {
            panic!("expected serve");
        };
        let layer = args.layer();
        assert_eq!(layer.node.role, Some(Role::Leader));
        assert_eq!(layer.node.id, Some(3));
        assert_eq!(
            layer.cluster.leader_peers.as_deref(),
            Some(["1=a:7101".to_owned(), "2=b:7101".to_owned()].as_slice())
        );
    }

    #[test]
    fn an_unset_serve_flag_leaves_the_option_to_the_lower_layers() {
        let cli = Cli::try_parse_from(["orbita", "serve"]).unwrap();
        let Command::Serve(args) = cli.command else {
            panic!("expected serve");
        };
        let layer = args.layer();
        assert_eq!(layer.node.listen, None);
        assert_eq!(layer.cluster.allow_version_skew, None);
    }

    #[test]
    fn the_two_conditions_on_a_write_are_mutually_exclusive() {
        let result = Cli::try_parse_from([
            "orbita",
            "set",
            "demo",
            "k",
            "v",
            "--if-not-present",
            "--if-version",
            "4",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn deleting_a_keyspace_requires_the_name_twice() {
        assert!(Cli::try_parse_from(["orbita", "keyspace", "delete", "demo"]).is_err());
        assert!(
            Cli::try_parse_from(["orbita", "keyspace", "delete", "demo", "--confirm", "demo"])
                .is_ok()
        );
    }

    #[test]
    fn a_credential_scoped_to_nothing_is_rejected_before_it_reaches_the_server() {
        assert!(Cli::try_parse_from(["orbita", "credential", "create"]).is_err());
        assert!(Cli::try_parse_from([
            "orbita",
            "credential",
            "create",
            "--keyspace",
            "demo",
            "--permission",
            "read"
        ])
        .is_ok());
    }

    #[test]
    fn dev_runs_with_no_arguments_at_all() {
        let cli = Cli::try_parse_from(["orbita", "dev"]).unwrap();
        let Command::Dev(args) = cli.command else {
            panic!("expected dev");
        };
        assert_eq!(args.port, crate::config::DEFAULT_PORT);
        assert_eq!(args.keyspace, "default");
    }

    #[test]
    fn the_root_help_names_every_top_level_command() {
        let help = Cli::command().render_long_help().to_string();
        for command in [
            "serve",
            "dev",
            "keyspace",
            "credential",
            "cluster",
            "partition",
            "get",
            "set",
            "delete",
            "list",
            "config",
        ] {
            assert!(help.contains(command), "{command} is missing from the help");
        }
    }

    #[test]
    fn the_root_help_shows_a_worked_example_and_the_exit_codes() {
        let help = Cli::command().render_long_help().to_string();
        assert!(help.contains("orbita keyspace create demo"), "{help}");
        assert!(help.contains("Exit codes"), "{help}");
    }

    #[test]
    fn every_subcommand_has_help_text() {
        fn check(command: &clap::Command, path: &str) {
            for sub in command.get_subcommands() {
                let name = format!("{path} {}", sub.get_name());
                assert!(
                    sub.get_about().is_some(),
                    "{name} has no description, so it is invisible in the parent help"
                );
                check(sub, &name);
            }
        }
        check(&Cli::command(), "orbita");
    }
}
