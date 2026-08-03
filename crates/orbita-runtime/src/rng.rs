//! Seeded randomness.

/// A source of random numbers that a simulation can replay.
///
/// Interior mutability keeps this usable from `&self`, so it can live behind a
/// `Runtime` without forcing every caller to hold a mutable borrow.
pub trait Rng: Send + Sync + 'static {
    fn next_u64(&self) -> u64;

    /// A uniform value in `[0, n)`. Returns 0 when `n` is 0.
    fn below(&self, n: u64) -> u64 {
        if n == 0 {
            0
        } else {
            self.next_u64() % n
        }
    }

    /// True with probability `numerator / denominator`.
    ///
    /// Fault injection reads more clearly with this than with modulo
    /// arithmetic at the call site.
    fn chance(&self, numerator: u64, denominator: u64) -> bool {
        denominator > 0 && self.below(denominator) < numerator
    }
}

/// Sharing one generator across clones of a runtime is common enough that
/// every crate was otherwise writing the same newtype to get it.
impl<R: Rng + ?Sized> Rng for std::sync::Arc<R> {
    fn next_u64(&self) -> u64 {
        (**self).next_u64()
    }
}

/// A seeded SplitMix64 generator.
///
/// This is deliberately not a cryptographic generator and must never be used
/// for credentials. It exists so that a simulation run is identified by a
/// single `u64` seed: report the seed with a failure and anyone can replay the
/// exact interleaving that produced it.
#[derive(Debug)]
pub struct SeededRng {
    state: std::sync::atomic::AtomicU64,
}

impl SeededRng {
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self {
            state: std::sync::atomic::AtomicU64::new(seed),
        }
    }
}

impl Rng for SeededRng {
    fn next_u64(&self) -> u64 {
        use std::sync::atomic::Ordering;
        // SplitMix64. Determinism holds because the simulator runs tasks one
        // at a time; under real concurrency the sequence is still uniform but
        // the interleaving is not reproducible, which is fine in production.
        let z = self
            .state
            .fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed)
            .wrapping_add(0x9E37_79B9_7F4A_7C15);
        let z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        let z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_replays_the_same_sequence() {
        let a = SeededRng::new(42);
        let b = SeededRng::new(42);
        let seq_a: Vec<u64> = (0..16).map(|_| a.next_u64()).collect();
        let seq_b: Vec<u64> = (0..16).map(|_| b.next_u64()).collect();
        assert_eq!(seq_a, seq_b, "a seed must fully determine the run");
    }

    #[test]
    fn different_seeds_diverge() {
        let a = SeededRng::new(1);
        let b = SeededRng::new(2);
        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn below_respects_its_bound() {
        let rng = SeededRng::new(7);
        for _ in 0..1000 {
            assert!(rng.below(10) < 10);
        }
        assert_eq!(rng.below(0), 0, "a zero bound must not divide by zero");
    }

    #[test]
    fn chance_hits_both_extremes() {
        let rng = SeededRng::new(9);
        assert!(!rng.chance(0, 100), "zero probability never fires");
        assert!(rng.chance(100, 100), "certain probability always fires");
    }
}
