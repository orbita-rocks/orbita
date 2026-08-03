//! Time.

use std::future::Future;
use std::time::Duration;

/// The two clocks a node needs, plus sleeping.
///
/// Wall time and monotonic time are kept separate because they fail
/// differently. Wall time can jump backwards across an NTP correction, so it
/// is only safe for TTL expiry, which is a shared absolute deadline. Anything
/// measuring an interval, such as a heartbeat deadline or a lease, must use
/// monotonic time or it will misbehave exactly when the cluster is already
/// having a bad day.
pub trait Clock: Clone + Send + Sync + 'static {
    /// Unix milliseconds. Use for TTL expiry, and for nothing else.
    fn now_millis(&self) -> u64;

    /// Nanoseconds from an arbitrary origin, never decreasing. Use for
    /// timeouts, leases, and heartbeats.
    fn monotonic_nanos(&self) -> u64;

    /// Sleeps for at least `duration`.
    ///
    /// Under simulation this advances virtual time instead of blocking, which
    /// is what lets a test explore an hour of lease expiry in a millisecond.
    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send;
}
