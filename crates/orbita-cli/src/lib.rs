//! The `orbita` command line tool, as a library.
//!
//! The binary in `main.rs` is a thin shell around this so that the argument
//! parsing, the configuration layering, and the output formatting can be
//! tested without spawning a process. Anything with a decision in it lives
//! here.
//!
//! The layout follows the shape of the tool:
//!
//! - [`cli`] is the whole command surface as clap types, and nothing else.
//! - [`config`] resolves a configuration from defaults, a file, the
//!   environment, and flags, in that order of increasing precedence.
//! - [`output`] turns results into either human text or JSON, in one place, so
//!   that no command invents its own formatting.
//! - [`client`] dials a cluster and attaches the credential.
//! - [`admin`] and [`data`] wrap the `Admin` and `Kv` gRPC services. They are
//!   deliberately thin: the CLI must not become a second implementation of
//!   anything the API already does.
//! - [`session`] holds the resolved config and one channel, and is the single
//!   place a parsed command is dispatched, so the one-shot binary and the REPL
//!   run the same code.
//! - [`repl`] is the interactive loop: session state plus a line editor that
//!   re-enters the same parser and renderer, never a second command surface.
//! - [`node`] is the single seam where this crate starts a server.
//! - [`telemetry`] carries the OpenTelemetry settings from configuration to
//!   whoever installs the exporter.
//!
//! Work brief: `docs/plan/06-ops.md`.

#![forbid(unsafe_code)]

pub mod admin;
pub mod cli;
pub mod client;
pub mod config;
pub mod data;
pub mod node;
pub mod output;
pub mod repl;
pub mod session;
pub mod telemetry;

/// The version string the binary reports and the version a node compares
/// against a leader group when it joins.
///
/// It lives here rather than being read from `CARGO_PKG_VERSION` at each call
/// site so that the compatibility check in [`node`] and the `--version` output
/// can never disagree.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The commit the binary was built from, short.
///
/// `unknown` when the build had no git repository to ask, which is what
/// building from a source tarball looks like.
pub const BUILD_SHA: &str = env!("ORBITA_BUILD_SHA");

/// What `--version` prints, and what a bug report should quote.
///
/// The version on its own is not enough on `develop`, where it stays at the
/// next `-dev` version for as long as the cycle lasts and so identifies a
/// range of commits rather than a build.
pub const LONG_VERSION: &str = env!("ORBITA_LONG_VERSION");
