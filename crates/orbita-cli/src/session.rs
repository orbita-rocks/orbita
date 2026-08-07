//! One resolved session, and the single place a parsed command is dispatched.
//!
//! The one-shot binary and the REPL run the *same* commands, so they share one
//! dispatch function here rather than each growing their own. That is what
//! makes "anything an operator can do is scriptable" true by construction: a
//! REPL line and a shell invocation are the same [`Command`] going through the
//! same [`dispatch`], so there is no second command surface to keep in step.
//!
//! A [`Session`] is what one command needs that outlives it in the REPL: the
//! resolved configuration, one channel dialed once and cloned per command, the
//! current output format, and the current keyspace. The one-shot path builds a
//! session too, uses it once, and drops it, so the two paths cannot diverge.

use anyhow::{bail, Result};
use tonic::transport::Channel;

use crate::cli::{Command, ConfigCommand, Outcome};
use crate::config::{self, Config};
use crate::output::{render, EnvVariable, EnvView, Format};
use crate::{admin, data};

/// Everything a command runs against that a REPL keeps between lines.
pub struct Session {
    /// The resolved configuration. Immutable for the life of the session: the
    /// REPL fixes the endpoint and credential at startup, because changing them
    /// per line would mean re-dialing and defeat holding one channel.
    pub config: Config,
    /// One channel, dialed lazily once and cloned per command. `Channel` clones
    /// share the underlying connection pool, so this is the "one connection
    /// across lines" the REPL wants.
    pub channel: Channel,
    /// The output format. Session state so `:format json` can change it, and an
    /// individual line's `--output` can still override it for that line.
    pub format: Format,
    /// The current keyspace, filled from `client.keyspace` at startup and
    /// changed by the REPL's `:use`. `None` means every data command must name
    /// its keyspace, which is how the tool behaves with nothing configured.
    pub keyspace: Option<String>,
    /// Whether this is an interactive session. It gates the two behaviours that
    /// only make sense outside a REPL: reading a `set` value from standard
    /// input, and running `serve` or `dev`.
    pub interactive: bool,
}

impl Session {
    /// Builds a session from a resolved configuration.
    ///
    /// The channel is dialed lazily, so this does not connect; the first
    /// command does. That keeps `orbita config show` from opening a socket and
    /// lets the REPL start even if the cluster is not up yet.
    pub fn new(config: Config, format: Format, interactive: bool) -> Result<Self> {
        let channel = crate::client::channel(&config)?;
        let keyspace = config.client.keyspace.clone();
        Ok(Self {
            config,
            channel,
            format,
            keyspace,
            interactive,
        })
    }

    /// The keyspace a data command falls back to when its argument is omitted.
    #[must_use]
    pub fn default_keyspace(&self) -> Option<&str> {
        self.keyspace.as_deref()
    }
}

/// Turns one parsed command into what should be printed and the code to exit
/// with. The single dispatch point for both the one-shot binary and the REPL.
///
/// `format` is passed rather than read from the session so a REPL line's
/// `--output` can override the session default for that one line without
/// mutating shared state.
pub async fn dispatch(session: &Session, format: Format, command: Command) -> Result<Outcome> {
    match command {
        // Both block forever running a node. The one-shot path handles them
        // before ever reaching here; in the REPL they are refused, because a
        // command that never returns has no place in a loop that reads the next
        // line. The message points at the shell rather than pretending the REPL
        // could do it.
        Command::Serve(_) | Command::Dev(_) => bail!(
            "serve and dev run a node and block forever, so they are not available in an \
             interactive session. Leave the session and run `orbita serve` or `orbita dev` from \
             your shell"
        ),
        // Nesting a REPL inside a REPL is the sort of thing that reads as a bug
        // even when it works, so it is refused rather than silently entered.
        Command::Repl(_) => bail!("already in an interactive session"),
        Command::Config { command } => config_outcome(&session.config, format, command),
        Command::Keyspace { command } => Ok(Outcome::ok(
            admin::keyspace(session, format, command).await?,
        )),
        Command::Credential { command } => Ok(Outcome::ok(
            admin::credential(session, format, command).await?,
        )),
        Command::Cluster { command } => {
            Ok(Outcome::ok(admin::cluster(session, format, command).await?))
        }
        Command::Partition { command } => Ok(Outcome::ok(
            admin::partition(session, format, command).await?,
        )),
        Command::Get(args) => data::get(session, format, args).await,
        Command::Set(args) => data::set(session, format, args).await,
        Command::Delete(args) => data::delete(session, format, args).await,
        Command::List(args) => data::list(session, format, args).await,
    }
}

/// Renders the `config` subcommands, which read no cluster and so take no
/// channel and no runtime.
///
/// It is a free function rather than an arm inside [`dispatch`] so the one-shot
/// path can call it directly, without wrapping it in a runtime it does not need
/// — the whole reason `orbita config show` stays cheap.
pub fn config_outcome(config: &Config, format: Format, command: ConfigCommand) -> Result<Outcome> {
    match command {
        ConfigCommand::Show => Ok(Outcome::ok(render(format, config)?)),
        ConfigCommand::Env => {
            let view = EnvView {
                variables: config::ENVIRONMENT
                    .iter()
                    .map(|(name, sets)| EnvVariable {
                        name: (*name).to_owned(),
                        sets: (*sets).to_owned(),
                    })
                    .collect(),
            };
            Ok(Outcome::ok(render(format, &view)?))
        }
    }
}
