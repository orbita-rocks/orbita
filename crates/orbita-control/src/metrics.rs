//! Split and merge activity counters.
//!
//! These read from `opentelemetry::global`, which is a no-op meter until the
//! `orbita` binary installs an exporter, so a leader group in a test or a
//! single-node run records into nothing. The counters live in the control plane
//! because that is the only place a split or a merge is decided; a worker never
//! sees one happen.
//!
//! The activity signal is deliberately a count with an `outcome` label rather
//! than a per-partition series. A split or a merge is a control-plane event, not
//! a property of a partition over time, and an operator watching for it wants to
//! know that one happened and how it resolved, not to carry a series per
//! partition that will only ever see a single increment. The `outcome` label is
//! a fixed, small set, so the metric never grows with the cluster.
//!
//! Split and merge both record `unimplemented` today: the control-plane split
//! protocol exists but its data-plane half (worker child-storage preparation
//! and parent quiescing) does not, so the operator surface fails closed. The
//! counters are wired now so the series exist the day execution does; the label
//! set stays fixed and small, so it never grows with the cluster.

use std::sync::OnceLock;

use opentelemetry::metrics::Counter;
use opentelemetry::{global, KeyValue};

struct Instruments {
    splits: Counter<u64>,
    merges: Counter<u64>,
}

fn instruments() -> &'static Instruments {
    static INSTRUMENTS: OnceLock<Instruments> = OnceLock::new();
    INSTRUMENTS.get_or_init(|| {
        let meter = global::meter("orbita-control");
        Instruments {
            splits: meter
                .u64_counter("orbita.partition.splits")
                .with_description("Partition split attempts, by outcome.")
                .build(),
            merges: meter
                .u64_counter("orbita.partition.merges")
                .with_description("Partition merge attempts, by outcome.")
                .build(),
        }
    })
}

/// How a split or merge attempt resolved. A fixed set, so the label is bounded.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Outcome {
    /// The operation is not yet executable and was refused without changing the
    /// map.
    Unimplemented,
}

impl Outcome {
    fn as_str(self) -> &'static str {
        match self {
            Outcome::Unimplemented => "unimplemented",
        }
    }
}

/// Counts one split attempt.
pub(crate) fn record_split(outcome: Outcome) {
    instruments()
        .splits
        .add(1, &[KeyValue::new("outcome", outcome.as_str())]);
}

/// Counts one merge attempt.
pub(crate) fn record_merge(outcome: Outcome) {
    instruments()
        .merges
        .add(1, &[KeyValue::new("outcome", outcome.as_str())]);
}

#[cfg(test)]
mod tests {
    use super::*;

    // No provider is installed in a unit test, so the global meter is a no-op.
    // This proves the counters build and the record paths run in that state,
    // which is the state a leader group is in until the node process wires an
    // exporter.
    #[test]
    fn split_and_merge_counters_record_without_a_provider() {
        record_split(Outcome::Unimplemented);
        record_merge(Outcome::Unimplemented);
    }
}
