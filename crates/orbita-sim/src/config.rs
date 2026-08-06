//! Knobs for a simulation run.
//!
//! Fault rates live here rather than being hard-coded so a test can say what
//! it is exploring. A test of steady-state correctness wants a quiet network;
//! a test of recovery wants a hostile one. The brief calls this the fault
//! budget: injecting a fault on every operation finds bugs but explores a
//! shallow state space, because the system spends the whole run recovering
//! instead of making progress.

use std::time::Duration;

/// Everything a run needs beyond its seed.
#[derive(Debug, Clone)]
pub struct SimConfig {
    /// The one number a failure is reported with. Everything else about a run
    /// is derived from it.
    pub seed: u64,

    pub network: NetworkFaults,
    pub disk: DiskFaults,
    pub store: StoreFaults,

    /// How long a peer call waits before giving up. A dropped message only
    /// becomes an observable failure when this expires, so it also bounds how
    /// long a run spends on a message that will never arrive.
    pub call_timeout: Duration,

    /// The total number of faults a run may inject, across network and disk.
    /// This is the fault budget: it keeps a run from spending all of its
    /// virtual time in recovery and never reaching the states worth checking.
    pub fault_budget: u64,

    /// Faults are suppressed until this much virtual time has passed, so a
    /// cluster gets to form before the world starts breaking.
    pub fault_warmup: Duration,

    /// Steps the driver will take before declaring the run stuck. A run that
    /// hits this is either livelocked or badly configured, and either way
    /// hanging CI is worse than failing it.
    pub max_steps: u64,

    /// How many trace lines to retain. Traces are compared byte for byte to
    /// prove determinism, and kept small enough to paste into a bug report.
    pub trace_limit: usize,

    /// Where the simulated wall clock starts. Fixed by default, because a
    /// trace that embeds the real date is not reproducible.
    pub wall_clock_origin_millis: u64,
}

impl SimConfig {
    /// A quiet world: real latencies, no faults. The baseline a test starts
    /// from before it decides what to break.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            network: NetworkFaults::none(),
            disk: DiskFaults::none(),
            store: StoreFaults::none(),
            call_timeout: Duration::from_millis(500),
            fault_budget: u64::MAX,
            fault_warmup: Duration::ZERO,
            max_steps: 5_000_000,
            trace_limit: 100_000,
            // 2024-01-01T00:00:00Z. Any fixed instant would do; a recent one
            // keeps TTL arithmetic in a plausible range.
            wall_clock_origin_millis: 1_704_067_200_000,
        }
    }

    /// A hostile world: messages drop, duplicate, and straggle, and the disk
    /// misbehaves in all the ways real disks do.
    #[must_use]
    pub fn chaotic(seed: u64) -> Self {
        Self {
            network: NetworkFaults::chaotic(),
            disk: DiskFaults::chaotic(),
            store: StoreFaults::chaotic(),
            ..Self::new(seed)
        }
    }
}

/// What the network does to a message.
///
/// Rates are per thousand rather than floating point so that a run is
/// reproducible across machines without depending on float formatting.
#[derive(Debug, Clone)]
pub struct NetworkFaults {
    /// Chance in a thousand that a message is thrown away. Applied
    /// independently to the request and to the response, so a call can fail
    /// after the peer has already applied it. That asymmetry is the reason
    /// every peer request has to be idempotent.
    pub drop_permille: u64,

    /// Chance in a thousand that a message is delivered twice.
    pub duplicate_permille: u64,

    /// Chance in a thousand that a message is held far longer than usual.
    /// Combined with ordinary jitter this is what produces reordering.
    pub slow_permille: u64,

    pub min_latency: Duration,
    pub max_latency: Duration,
    pub slow_latency: Duration,
}

impl NetworkFaults {
    #[must_use]
    pub fn none() -> Self {
        Self {
            drop_permille: 0,
            duplicate_permille: 0,
            slow_permille: 0,
            min_latency: Duration::from_micros(200),
            max_latency: Duration::from_millis(2),
            slow_latency: Duration::from_millis(200),
        }
    }

    #[must_use]
    pub fn chaotic() -> Self {
        Self {
            drop_permille: 50,
            duplicate_permille: 30,
            slow_permille: 50,
            ..Self::none()
        }
    }
}

/// What the disk does to the bytes it was given.
#[derive(Debug, Clone)]
pub struct DiskFaults {
    /// Chance in a thousand that an append reports an error. The caller learns
    /// nothing about whether the bytes landed, which is the contract
    /// `DiskError::Io` spells out.
    pub write_failure_permille: u64,

    /// Chance in a thousand that an append writes a prefix of the data and
    /// then reports an error.
    pub partial_write_permille: u64,

    /// Chance in a thousand that a read returns bytes that fail their
    /// checksum. Recovery is supposed to treat this as truncation, not as a
    /// fatal error.
    pub read_corruption_permille: u64,

    /// Chance in a thousand that `sync` returns success without making
    /// anything durable. This is what real disks and virtualised block devices
    /// do with a write cache, and it is the fault most systems get wrong,
    /// because everything looks fine until the machine loses power.
    pub lying_fsync_permille: u64,

    /// Whether a crash leaves a partial record at the end of the file rather
    /// than a clean boundary. A torn tail is the expected outcome of dying
    /// mid-append, so recovery has to handle it on every start.
    pub torn_tail_on_crash: bool,

    pub min_latency: Duration,
    pub max_latency: Duration,
}

impl DiskFaults {
    #[must_use]
    pub fn none() -> Self {
        Self {
            write_failure_permille: 0,
            partial_write_permille: 0,
            read_corruption_permille: 0,
            lying_fsync_permille: 0,
            torn_tail_on_crash: false,
            min_latency: Duration::from_micros(50),
            max_latency: Duration::from_micros(500),
        }
    }

    #[must_use]
    pub fn chaotic() -> Self {
        Self {
            write_failure_permille: 20,
            partial_write_permille: 20,
            read_corruption_permille: 10,
            lying_fsync_permille: 20,
            torn_tail_on_crash: true,
            ..Self::none()
        }
    }
}

/// What the object store does to a request.
///
/// The distinction that earns its keep here is between a request that never
/// arrived and a response that never came back. Both look identical to the
/// caller, and only the second leaves the store changed, so a commit protocol
/// that treats them as the same thing is a commit protocol that either loses
/// a write or publishes one twice. Per ADR 0006 the manifest swap is the
/// durability boundary, and this is what puts that claim under load.
#[derive(Debug, Clone)]
pub struct StoreFaults {
    /// Chance in a thousand that the request never reaches the store, so
    /// nothing was applied and a retry is free.
    pub request_lost_permille: u64,

    /// Chance in a thousand that the store applies the request and the answer
    /// is lost on the way back. This is the one that matters: a conditional
    /// write that succeeded and was reported as a failure is the case a
    /// writer cannot distinguish from one that never happened.
    pub response_lost_permille: u64,

    /// Chance in a thousand that the store answers `503 Slow Down`, which is
    /// what a throttled or overloaded bucket does. Nothing is applied, and the
    /// caller is expected to treat it as retryable.
    pub server_error_permille: u64,

    /// Chance in a thousand that a request is held far longer than usual,
    /// which is what turns a flush into something a lease can expire under.
    pub slow_permille: u64,

    /// Object storage is a network round trip rather than a disk seek, so the
    /// floor here is deliberately an order of magnitude above the disk's. A
    /// simulation that made a manifest swap as cheap as an fsync would never
    /// explore the window a real one leaves open.
    pub min_latency: Duration,
    pub max_latency: Duration,
    pub slow_latency: Duration,
}

impl StoreFaults {
    #[must_use]
    pub fn none() -> Self {
        Self {
            request_lost_permille: 0,
            response_lost_permille: 0,
            server_error_permille: 0,
            slow_permille: 0,
            min_latency: Duration::from_millis(1),
            max_latency: Duration::from_millis(10),
            slow_latency: Duration::from_millis(500),
        }
    }

    #[must_use]
    pub fn chaotic() -> Self {
        Self {
            request_lost_permille: 20,
            response_lost_permille: 20,
            server_error_permille: 20,
            slow_permille: 20,
            ..Self::none()
        }
    }
}
