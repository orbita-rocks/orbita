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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub key: String,
    pub size: u64,
    pub etag: ETag,

    /// When the object was last written, as Unix milliseconds on the same
    /// scale as `orbita_runtime`'s `Clock::now_millis`, or `None` when the
    /// backend cannot report a time.
    ///
    /// # `None` means refuse to act
    ///
    /// The orphan sweep may delete an object only after a grace period that
    /// exceeds both the longest read and the longest commit, and it judges
    /// that period from this timestamp. A missing time is therefore not a
    /// licence to assume the object is old: an absent value reads as the
    /// epoch, which is the direction that deletes live data. A sweep that
    /// finds `None` must refuse to act on that object rather than guess, so
    /// backends that genuinely cannot report a write time say so here instead
    /// of reporting a zero that would be mistaken for "very old".
    ///
    /// Backends populate it from the listing they already read: the S3 store
    /// wires through `LastModified`, and the in-memory store stamps the
    /// runtime clock at write time so a simulated run can drive the grace
    /// period deterministically.
    pub last_modified: Option<u64>,
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
