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
pub mod telemetry;

/// The version string the binary reports and the version a node compares
/// against a leader group when it joins.
///
/// It lives here rather than being read from `CARGO_PKG_VERSION` at each call
/// site so that the compatibility check in [`node`] and the `--version` output
/// can never disagree.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
