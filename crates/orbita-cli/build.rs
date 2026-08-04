//! Bakes the commit into the binary.
//!
//! On `develop` the version is `0.2.0-dev` for weeks at a time, so the version
//! alone cannot answer "which build is this node running", which is the first
//! question in every report about a prerelease. The short sha answers it.
//!
//! A missing or failed `git` is not an error. Building from a source tarball
//! with no repository is a normal thing to do, and it should produce a working
//! binary rather than a build failure about version metadata.

use std::process::Command;

fn git(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8(out.stdout).ok()?.trim().to_string();
    (!text.is_empty()).then_some(text)
}

/// Where git keeps a given file for this checkout, absolute.
fn git_path(name: &str) -> Option<String> {
    git(&["rev-parse", "--path-format=absolute", "--git-path", name])
}

fn main() {
    let sha = git(&["rev-parse", "--short=7", "HEAD"]).unwrap_or_else(|| "unknown".to_string());

    println!("cargo:rustc-env=ORBITA_BUILD_SHA={sha}");

    // Concatenated here rather than in the crate, so that the joined string is
    // a plain `env!` constant and the crate needs no dependency to build one.
    let version = std::env::var("CARGO_PKG_VERSION").unwrap_or_default();
    println!("cargo:rustc-env=ORBITA_LONG_VERSION={version} ({sha})");

    // Without this the sha is whatever it was the first time the crate was
    // compiled, which is worse than not having one. The path is asked for
    // rather than assumed, because in a git worktree `.git` is a file pointing
    // somewhere else and the HEAD that moves is not the one next door.
    if let Some(head) = git_path("HEAD") {
        println!("cargo:rerun-if-changed={head}");
    }
    println!("cargo:rerun-if-changed=build.rs");
}
