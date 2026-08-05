//! Running a scenario across many seeds, and reporting the one that broke.
//!
//! A simulation failure is only useful if it comes back. The contract here is
//! that a failing run prints its seed and the command that replays it, and
//! that running that command reproduces the same failure on any machine. A
//! failure that cannot be replayed from its seed is a bug in this crate, not
//! in the system under test.

use crate::trace::Trace;

/// A scenario that did not hold, with the evidence.
#[derive(Debug, Clone)]
pub struct Failure {
    pub seed: u64,
    pub reason: String,
    pub trace: Trace,
}

impl Failure {
    /// The failure, the command that replays it, and the tail of the trace.
    ///
    /// One function rather than a format each caller assembles, so a scenario
    /// that is a plain `#[test]` rather than a seeded batch reports a failure
    /// the same way and stays as replayable.
    #[must_use]
    pub fn replay_report(&self, test_name: &str) -> String {
        format!(
            "\n{self}\n\nreproduce with:\n    {SEED_VAR}={} cargo test -p {} {test_name} -- --exact{} --nocapture\n\nlast events:\n{}\n",
            self.seed,
            owning_package(),
            ignored_filter(),
            self.trace.tail(60)
        )
    }
}

/// The filter flag a replay needs in order to run at all.
///
/// A scenario that pins a known failure is marked `#[ignore]`, and a command
/// that omits the flag runs zero tests and reports success, which is a worse
/// outcome than printing nothing: it tells someone chasing the bug that it is
/// already fixed. The condition is read from how this run was invoked, since a
/// test that needed the flag to start needs it to start again.
///
/// The flag emitted is `--include-ignored` rather than `--ignored`, because it
/// runs the named test whether or not the ignore is still there. Someone
/// replaying a failure a week later should not have their command break on the
/// commit that removes the attribute.
fn ignored_filter() -> &'static str {
    let invoked_with = |flag: &str| std::env::args().any(|arg| arg == flag);
    if invoked_with("--ignored") || invoked_with("--include-ignored") {
        " --include-ignored"
    } else {
        ""
    }
}

/// The package whose test binary is running.
///
/// Scenarios live in the crates they exercise rather than here, so a command
/// naming this crate would not replay most of them. Cargo puts the running
/// package in the environment; the fallback only applies to a test binary
/// somebody invoked directly, which is also the case where they already know
/// which one they ran.
fn owning_package() -> String {
    std::env::var("CARGO_PKG_NAME").unwrap_or_else(|_| "orbita-sim".into())
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "seed {} failed: {}", self.seed, self.reason)
    }
}

/// The environment variable that pins a run to one seed.
pub const SEED_VAR: &str = "ORBITA_SIM_SEED";

/// The environment variable that overrides how many seeds a scenario runs.
pub const SEED_COUNT_VAR: &str = "ORBITA_SIM_SEEDS";

/// The seeds a scenario should run.
///
/// A pull request runs a modest batch and a nightly job sets
/// `ORBITA_SIM_SEEDS` to a much larger one, so the same test serves both
/// without a second copy of the scenario.
#[must_use]
pub fn seeds(default_count: u64) -> Vec<u64> {
    if let Ok(pinned) = std::env::var(SEED_VAR) {
        if let Ok(seed) = pinned.trim().parse::<u64>() {
            return vec![seed];
        }
    }
    let count = std::env::var(SEED_COUNT_VAR)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(default_count);
    // Seeds start at one and run up, so "seed 37" means the same run today and
    // next year rather than depending on how many seeds a batch happened to
    // have.
    (1..=count).collect()
}

/// Runs `scenario` over a batch of seeds and panics on the first failure with
/// enough detail to replay it.
///
/// `test_name` is the name of the calling test, used to print a command that
/// runs exactly that test at exactly that seed.
pub fn check_seeds<F>(test_name: &str, default_count: u64, scenario: F)
where
    F: Fn(u64) -> Result<(), Failure>,
{
    for seed in seeds(default_count) {
        if let Err(failure) = scenario(seed) {
            panic!("{}", failure.replay_report(test_name));
        }
    }
}

/// Fails a plain `#[test]` with the same evidence a seeded batch would give.
///
/// A scenario that runs one fixed seed is still a scenario, and losing the
/// replay command because it was not written as a batch would be a worse
/// debugging experience for no reason.
pub fn expect_converged(test_name: &str, outcome: Result<(), Failure>) {
    if let Err(failure) = outcome {
        panic!("{}", failure.replay_report(test_name));
    }
}
