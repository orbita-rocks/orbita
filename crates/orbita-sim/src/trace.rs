//! The record of what happened.
//!
//! Two runs of the same seed must produce the same trace, byte for byte. That
//! is the property the whole crate rests on, and comparing formatted lines is
//! the cheapest way to check it that also happens to be readable when a test
//! fails at three in the morning.
//!
//! The trace replays a failure only in combination with the same binary. That
//! is a deliberate limit: a trace rich enough to replay on its own would have
//! to encode every scheduling decision, and it would go stale the moment the
//! code under test changed, which is exactly when a replay is wanted. The seed
//! plus the trace tells you what happened; the seed alone reproduces it.

use std::fmt;

/// An ordered log of simulation events, capped so a pathological run cannot
/// exhaust memory.
#[derive(Debug, Clone)]
pub struct Trace {
    seed: u64,
    entries: Vec<String>,
    limit: usize,
    dropped: usize,
}

impl Trace {
    #[must_use]
    pub fn new(seed: u64, limit: usize) -> Self {
        Self {
            seed,
            entries: Vec::new(),
            limit,
            dropped: 0,
        }
    }

    pub(crate) fn record(&mut self, at_nanos: u64, event: impl Into<String>) {
        if self.entries.len() >= self.limit {
            self.dropped += 1;
            return;
        }
        self.entries
            .push(format!("{:>14} {}", at_nanos, event.into()));
    }

    #[must_use]
    pub fn seed(&self) -> u64 {
        self.seed
    }

    #[must_use]
    pub fn lines(&self) -> &[String] {
        &self.entries
    }

    /// The number of events dropped because the trace hit its limit. A
    /// non-zero value means two traces can agree line for line and still come
    /// from different runs, so determinism assertions should check it.
    #[must_use]
    pub fn dropped(&self) -> usize {
        self.dropped
    }

    /// The last `n` lines, which is usually all anyone wants from a failure.
    #[must_use]
    pub fn tail(&self, n: usize) -> String {
        let start = self.entries.len().saturating_sub(n);
        self.entries[start..].join("\n")
    }
}

impl fmt::Display for Trace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "seed {}", self.seed)?;
        for line in &self.entries {
            writeln!(f, "{line}")?;
        }
        if self.dropped > 0 {
            writeln!(f, "... {} further events not recorded", self.dropped)?;
        }
        Ok(())
    }
}
