//! The object storage contract.
//!
//! Compacted SSTs and partition manifests live in object storage, which is
//! what keeps a worker cheap to replace: a new replica hydrates from S3 rather
//! than from a peer's disk.
//!
//! # Why this one is dyn-compatible
//!
//! Unlike the runtime seams, this trait boxes its futures via `async_trait`.
//! Every call here is a network round trip measured in milliseconds, so an
//! allocation per call is noise, and in exchange the backend becomes a runtime
//! choice. That is what the requirements mean by a pluggable storage trait:
//! v1 ships and supports S3, and other backends can be added without touching
//! any caller.
//!
//! This is a contract crate. Changes here ripple through the whole workspace.

#![forbid(unsafe_code)]

#[cfg(feature = "s3")]
pub mod s3;

use async_trait::async_trait;
use bytes::Bytes;
use std::ops::Range;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ObjectError {
    #[error("object not found: {0}")]
    NotFound(String),

    /// A conditional write lost the race. The caller must re-read and retry,
    /// which is the whole basis of manifest updates.
    #[error("precondition failed for {0}")]
    PreconditionFailed(String),

    #[error("access denied for {0}")]
    AccessDenied(String),

    /// Worth retrying, meaning a timeout, a throttle, or a 5xx.
    #[error("transient failure: {0}")]
    Transient(String),

    #[error("object store error: {0}")]
    Other(String),
}

impl ObjectError {
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(self, ObjectError::Transient(_))
    }
}

pub type ObjectResult<T> = Result<T, ObjectError>;

/// An opaque version tag, meaning an ETag or a generation number.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ETag(pub String);

/// A point in time reported by an object store backend, in *that backend's own
/// clock domain*.
///
/// # Why this is not a bare `u64`
///
/// S3 stamps `Last-Modified` from its own server clock. The in-memory and
/// simulated stores stamp from `orbita_runtime`'s `Clock`. None of these is the
/// Orbita host clock, and none of them is synchronized with the others: equal
/// *units* (Unix milliseconds) do not imply a shared *now*. If this were a
/// `u64`, a caller could subtract a host `Clock::now_millis()` from an object's
/// write time and read the difference as an age. That age is wrong by however
/// far the two clocks have drifted, and it is wrong in the fatal direction: a
/// host clock running ahead of the backend makes a just-written object look old
/// enough to delete before its manifest is even published.
///
/// Wrapping the millis forces a caller to name the clock domain before it can
/// treat the number as an age. The only supported way to turn a `BackendTime`
/// into an age is to compare it against another `BackendTime` read from the
/// *same* backend; see [`ObjectMeta::is_safely_older_than`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BackendTime(pub u64);

impl BackendTime {
    /// The raw Unix milliseconds. Naming the domain is the caller's job the
    /// moment it unwraps this, which is the whole point of the wrapper.
    #[must_use]
    pub fn millis(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub key: String,
    pub size: u64,
    pub etag: ETag,

    /// When the object was last written, in the object store backend's own
    /// clock domain (see [`BackendTime`]), or `None` when the backend cannot
    /// report a time.
    ///
    /// # This is not the host clock
    ///
    /// The value is whatever the *backend* believes the write time was: the S3
    /// server's clock for the S3 store, the runtime clock for the in-memory and
    /// simulated stores. It is deliberately *not* comparable to the Orbita host
    /// `Clock::now_millis()`. The orphan sweep (#100) must judge an object's age
    /// only against a reference time read from the same backend, and must widen
    /// its grace period by the backend's worst-case internal clock skew, because
    /// even one backend (S3 is itself a distributed system) can stamp two
    /// objects, or answer a "now", from servers whose clocks disagree. Both of
    /// those obligations are encoded in [`ObjectMeta::is_safely_older_than`], so
    /// a sweep that goes through it cannot silently mix clock domains or forget
    /// skew.
    ///
    /// # `None` means refuse to act
    ///
    /// A missing time is not a licence to assume the object is old: an absent
    /// value would otherwise read as the epoch, which is the direction that
    /// deletes live data. Backends that genuinely cannot report a write time say
    /// so with `None` rather than a zero that would be mistaken for "very old",
    /// and [`ObjectMeta::is_safely_older_than`] treats `None` as "do not touch".
    ///
    /// Backends populate it from the listing they already read: the S3 store
    /// wires through `LastModified`, and the in-memory store stamps the runtime
    /// clock at write time so a simulated run can drive the grace period
    /// deterministically within one controllable domain.
    pub last_modified: Option<BackendTime>,
}

impl ObjectMeta {
    /// Whether this object is *provably* old enough to be a deletion candidate,
    /// judged only from times in the backend's own clock domain.
    ///
    /// This is the safety floor the orphan sweep (#100) builds its grace period
    /// on, not the whole sweep policy. It fails closed in three ways, each of
    /// which is a way the naive `now - last_modified >= grace` check deletes
    /// live data:
    ///
    /// - **Unknown time.** A `None` [`last_modified`](Self::last_modified)
    ///   returns `false`. An object whose age cannot be established is never a
    ///   candidate.
    /// - **Domain confusion.** `reference_now` is a [`BackendTime`], so a caller
    ///   cannot pass a host `Clock::now_millis()` without constructing one and
    ///   confronting, in a reviewable line of code, that it must come from the
    ///   *same* backend as the object's write time.
    /// - **Skew.** `max_skew_millis` is added to `min_age_millis` before the
    ///   comparison, so the object must clear the grace period *and* the
    ///   backend's worst-case clock skew. A caller that believes there is no
    ///   skew passes `0` deliberately rather than by omission.
    ///
    /// The subtraction saturates: a `reference_now` earlier than the write time
    /// (a backend whose clock stepped backwards) yields an age of zero, never a
    /// wrapped-around "ancient" age.
    #[must_use]
    pub fn is_safely_older_than(
        &self,
        reference_now: BackendTime,
        min_age_millis: u64,
        max_skew_millis: u64,
    ) -> bool {
        match self.last_modified {
            None => false,
            Some(written) => {
                let age = reference_now.millis().saturating_sub(written.millis());
                age >= min_age_millis.saturating_add(max_skew_millis)
            }
        }
    }
}

/// The precondition on a conditional write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Precondition {
    /// Succeed only if the object does not exist. This is how a manifest is
    /// first created without two writers both believing they made it.
    NotExists,
    /// Succeed only if the object is still at this version. This is the
    /// compare-and-swap that partition manifest updates are built on, and the
    /// reason the backend must support conditional writes at all.
    Match(ETag),
}

#[async_trait]
pub trait ObjectStore: Send + Sync + 'static {
    async fn put(&self, key: &str, data: Bytes) -> ObjectResult<ETag>;

    /// Writes only if `precondition` holds, returning
    /// [`ObjectError::PreconditionFailed`] otherwise.
    ///
    /// A backend that cannot do this atomically must not implement this trait,
    /// because manifest correctness depends on it rather than on locking.
    async fn put_if(
        &self,
        key: &str,
        data: Bytes,
        precondition: Precondition,
    ) -> ObjectResult<ETag>;

    async fn get(&self, key: &str) -> ObjectResult<(Bytes, ETag)>;

    /// Reads part of an object, which is how an SST block is fetched without
    /// pulling the whole file.
    async fn get_range(&self, key: &str, range: Range<u64>) -> ObjectResult<Bytes>;

    async fn head(&self, key: &str) -> ObjectResult<ObjectMeta>;

    async fn list(&self, prefix: &str) -> ObjectResult<Vec<ObjectMeta>>;

    async fn delete(&self, key: &str) -> ObjectResult<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(last_modified: Option<BackendTime>) -> ObjectMeta {
        ObjectMeta {
            key: "p/a".to_string(),
            size: 1,
            etag: ETag("etag".to_string()),
            last_modified,
        }
    }

    #[test]
    fn an_unknown_write_time_is_never_a_deletion_candidate() {
        // The fail-closed case the whole `Option` exists for: an object whose
        // age cannot be established must survive, not be read as "very old".
        let object = meta(None);
        assert!(!object.is_safely_older_than(BackendTime(u64::MAX), 0, 0));
    }

    #[test]
    fn an_object_within_the_skew_window_is_not_yet_old() {
        // Written at 1_000 in the backend's domain, and a backend-domain "now"
        // of 6_000 puts its observed age at 5_000ms. That clears a 4_000ms
        // grace on its own, but not once a 2_000ms skew allowance is added:
        // the gap could be the clocks disagreeing rather than real elapsed
        // time, so the object is not yet safe to delete.
        let object = meta(Some(BackendTime(1_000)));
        assert!(!object.is_safely_older_than(BackendTime(6_000), 4_000, 2_000));
    }

    #[test]
    fn an_object_past_the_grace_plus_skew_is_old() {
        // The same object at a later reference now clears grace and skew both.
        let object = meta(Some(BackendTime(1_000)));
        assert!(object.is_safely_older_than(BackendTime(8_000), 4_000, 2_000));
    }

    #[test]
    fn a_reference_now_before_the_write_time_reads_as_zero_age() {
        // A backend whose clock stepped backwards must not wrap into an ancient
        // age; the object simply is not old yet. Without the saturating
        // subtraction this would underflow to a near-`u64::MAX` age and clear
        // any grace.
        let object = meta(Some(BackendTime(9_000)));
        assert!(!object.is_safely_older_than(BackendTime(1_000), 4_000, 0));
    }
}
