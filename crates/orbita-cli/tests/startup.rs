//! The binary starts without panicking.
//!
//! These run the real `orbita` executable, because the regression they guard
//! only shows up in a process with no ambient Tokio runtime: constructing the
//! session's channel out of runtime scope panicked every network command and
//! the REPL with "there is no reactor running" before any real error could be
//! reported. A unit test cannot see it, because the test harness always has a
//! runtime; only spawning the binary does.

#![forbid(unsafe_code)]

use std::process::{Command, Stdio};

/// The compiled binary, provided to integration tests by Cargo.
const ORBITA: &str = env!("CARGO_BIN_EXE_orbita");

#[test]
fn a_network_command_fails_to_connect_without_panicking() {
    // Port 1 on loopback is never listening, so this must reach a connection
    // error — not a panic — to prove the channel was built inside the runtime.
    let output = Command::new(ORBITA)
        .args(["--endpoint", "http://127.0.0.1:1", "get", "demo", "key"])
        .output()
        .expect("the binary should run");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("there is no reactor running"),
        "the startup panic is back: {stderr}"
    );
    assert!(
        !stderr.contains("panicked"),
        "the command panicked: {stderr}"
    );
    assert!(
        !output.status.success(),
        "connecting to a dead port should fail, not succeed: {stderr}"
    );
    assert!(
        stderr.contains("while talking to"),
        "the failure should name the endpoint it could not reach: {stderr}"
    );
}

#[test]
fn the_repl_starts_and_leaves_cleanly_without_panicking() {
    // Closed stdin is immediate EOF, which is the ordinary way to leave, so the
    // session should build its runtime and channel, print its banner, and exit
    // zero — never panic on startup.
    let output = Command::new(ORBITA)
        .arg("repl")
        .stdin(Stdio::null())
        .output()
        .expect("the binary should run");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains("panicked"), "the REPL panicked: {stderr}");
    assert!(
        !stderr.contains("there is no reactor running"),
        "the startup panic is back: {stderr}"
    );
    assert!(
        output.status.success(),
        "leaving the REPL on EOF should exit zero: {stderr}"
    );
}
