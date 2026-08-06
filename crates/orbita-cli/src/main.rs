//! The `orbita` binary.
//!
//! One binary runs a node in either role and also wraps the admin and data
//! APIs, so an evaluator downloads one thing and an operator scripts against
//! the same surface the CLI uses.
//!
//! Everything with a decision in it lives in the library next door. This file
//! is dispatch: parse, resolve configuration, run one command, print, exit.
//!
//! Work brief: `docs/plan/06-ops.md`.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::io::Write as _;

use anyhow::{Context, Result};
use clap::Parser;
use orbita_cli::cli::{Cli, Command, DevArgs, Outcome, ServeArgs, EXIT_ERROR};
use orbita_cli::config::{self, Config};
use orbita_cli::node::{self, NodeOptions};
use orbita_cli::output::Format;
use orbita_cli::session::{self, Session};
use orbita_cli::{repl, telemetry};

fn main() {
    let cli = Cli::parse();
    let format = cli.global.format();

    match run(cli) {
        Ok(outcome) => {
            print(&outcome.text);
            std::process::exit(outcome.code);
        }
        Err(error) => {
            // The failure path uses the same format the command would have, so
            // a script parsing JSON does not suddenly get a sentence in
            // English.
            let text = match format {
                Format::Json => serde_json::json!({ "error": format!("{error:#}") }).to_string(),
                Format::Human => format!("orbita: {error:#}"),
            };
            eprintln!("{text}");
            std::process::exit(EXIT_ERROR);
        }
    }
}

/// Prints command output, tolerating a closed pipe.
///
/// `orbita list demo | head` closes the pipe early, and a panic out of the
/// print macros would turn a normal shell idiom into an error.
fn print(text: &str) {
    if text.is_empty() {
        return;
    }
    let mut stdout = std::io::stdout();
    let _ = stdout.write_all(text.as_bytes());
    let _ = stdout.flush();
}

fn run(cli: Cli) -> Result<Outcome> {
    let env: BTreeMap<String, String> = std::env::vars().collect();
    let format = cli.global.format();
    let global = cli.global.layer();
    let config_path = cli.global.config.clone();

    // Node commands feed their flags into the same layering as everything
    // else, so `--listen` beats ORBITA_LISTEN beats the file with no special
    // case anywhere.
    let config = match &cli.command {
        Command::Serve(args) => {
            config::load(config_path.as_deref(), &env, global.merge(args.layer()))?
        }
        Command::Dev(args) => config::load_with_base(
            config::dev_defaults(args.port, args.data_dir.clone()),
            config_path.as_deref(),
            &env,
            global,
        )?,
        _ => config::load(config_path.as_deref(), &env, global)?,
    };

    // A node installs its telemetry inside its own runtime, because the OTLP
    // exporters need one to spawn on; see `node::run_node`. Every other command
    // is a short-lived client, so stderr logging is installed here and now.
    if !matches!(cli.command, Command::Serve(_) | Command::Dev(_)) {
        telemetry::install_logging(&config)?;
    }

    match cli.command {
        // Serve and dev run a node and block forever, so they build their own
        // runtime and never touch the shared dispatch.
        Command::Serve(args) => serve(&config, &args),
        Command::Dev(args) => dev(&config, &args),
        // The config commands read no cluster, so they render here without a
        // runtime. That is the reason `orbita config show` does not start a
        // thread pool it will never use.
        Command::Config { command } => session::config_outcome(&config, format, command),
        // The interactive session owns its own runtime and channel for the life
        // of the loop, so it is entered directly rather than through block_on.
        Command::Repl(_) => repl::run(config, format),
        // Every remaining command is a network call. It goes through the exact
        // dispatch the REPL uses, so there is one command surface and not two.
        // A one-shot session is built, used once, and dropped. The endpoint is
        // named on the failure path, because "connection refused" without an
        // address is the least useful error a CLI can produce.
        command => {
            let endpoint = config.client.endpoint.clone();
            let session = Session::new(config, format, false)?;
            block_on(session::dispatch(&session, format, command))
                .with_context(|| format!("while talking to {endpoint}"))
        }
    }
}

fn serve(config: &Config, args: &ServeArgs) -> Result<Outcome> {
    let options = NodeOptions {
        create_keyspace: None,
        dev: false,
    };
    if args.allow_version_skew {
        // Written to stderr rather than through `tracing`, because the node's
        // subscriber is not installed until `run_node` is inside its runtime.
        eprintln!(
            "orbita: --allow-version-skew does nothing. Compatibility is a cluster version window \
             now, and no node checks a version yet"
        );
    }
    node::preflight(config, &options)?;
    node::prepare_data_dir(&config.node.data_dir, false)?;
    block_on(node::run_node(config, &options))?;
    Ok(Outcome::ok(String::new()))
}

fn dev(config: &Config, args: &DevArgs) -> Result<Outcome> {
    let options = NodeOptions {
        create_keyspace: Some(args.keyspace.clone()),
        dev: true,
    };
    node::prepare_data_dir(&config.node.data_dir, args.clean)?;
    // Startup notes go to stderr so that anything a later command pipes stays
    // clean.
    eprintln!(
        "orbita dev: listening on {}, data in {}, keyspace {}",
        config.node.listen,
        config.node.data_dir.display(),
        args.keyspace
    );
    block_on(node::run_node(config, &options))?;
    Ok(Outcome::ok(String::new()))
}

/// Runs one async operation on a runtime built for the job.
///
/// A CLI does one thing and exits, so it builds a runtime where it needs one
/// rather than wrapping `main` in an attribute. That keeps `orbita config show`
/// from starting a thread pool it will never use.
fn block_on<T>(future: impl std::future::Future<Output = Result<T>>) -> Result<T> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(future)
}
