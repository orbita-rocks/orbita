//! Local durable storage.
//!
//! This covers the write-ahead log, which is the file Orbita's durability
//! claim rests on. Per
//! [ADR 0006](../../../docs/adr/0006-partitions-are-an-index-over-immutable-objects.md)
//! the storage engine persists through `orbita_objectstore::ObjectStore`
//! rather than doing file I/O of its own, so every byte Orbita keeps crosses
//! this trait or that one and nothing writes underneath either. That is what
//! puts the whole system inside the simulator's reach rather than just the
//! distributed layer.

use bytes::Bytes;
use std::future::Future;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DiskError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("read past end of file: offset {offset}, length {len}")]
    OutOfBounds { offset: u64, len: usize },

    /// Injected by the simulator, and real on a failing disk. Callers must
    /// treat a failed write as neither applied nor not applied until they read
    /// it back, which is the whole reason this variant is spelled out.
    #[error("i/o error: {0}")]
    Io(String),

    /// The bytes read back did not match their checksum. Recovery must treat
    /// this as truncation at that point, not as a fatal error, because a torn
    /// tail is the expected outcome of a crash mid-append.
    #[error("corrupt data at offset {offset}")]
    Corrupt { offset: u64 },
}

pub type DiskResult<T> = Result<T, DiskError>;

#[derive(Debug, Clone, Copy, Default)]
pub struct OpenOptions {
    pub create: bool,
    pub truncate: bool,
}

impl OpenOptions {
    #[must_use]
    pub fn create() -> Self {
        Self {
            create: true,
            truncate: false,
        }
    }
}

/// A filesystem, scoped to one node's data directory.
pub trait Disk: Clone + Send + Sync + 'static {
    type File: File;

    fn open(
        &self,
        path: &str,
        options: OpenOptions,
    ) -> impl Future<Output = DiskResult<Self::File>> + Send;

    fn remove(&self, path: &str) -> impl Future<Output = DiskResult<()>> + Send;

    /// Lists the entries directly inside a directory.
    ///
    /// `dir` names a directory, not a prefix to match, and the result holds
    /// bare entry names rather than paths: listing `wal/7` yields
    /// `["000001.log"]`, not `["wal/7/000001.log"]`. A directory that does not
    /// exist lists as empty rather than failing, because a node opening a log
    /// for the first time is an ordinary case and not an error.
    ///
    /// Results are sorted. A real filesystem returns entries in whatever order
    /// it likes, and recovery depends on segment order, so sorting here means
    /// no caller has to remember to.
    ///
    /// This is spelled out because the two implementations once disagreed
    /// about it, one matching a prefix and returning full paths and the other
    /// listing a directory and returning bare names. The consequence was that
    /// a log in a nested directory looked empty under simulation, so reopening
    /// one silently started a new log. Nothing failed; the data was just gone.
    /// An ambiguous contract between two implementations of the same trait is
    /// worth more words than it looks like it deserves.
    fn list(&self, dir: &str) -> impl Future<Output = DiskResult<Vec<String>>> + Send;
}

/// An append-only file with explicit durability.
///
/// There is no buffered write without a matching `sync`, because the point of
/// this trait is to make the durability boundary visible in the code that
/// depends on it.
pub trait File: Send + Sync + 'static {
    /// Appends bytes, returning the offset they were written at. The data is
    /// not durable until `sync` returns.
    fn append(&self, data: Bytes) -> impl Future<Output = DiskResult<u64>> + Send;

    /// Makes every prior append durable. A write is only acknowledged to a
    /// peer after this resolves.
    fn sync(&self) -> impl Future<Output = DiskResult<()>> + Send;

    fn read_at(&self, offset: u64, len: usize) -> impl Future<Output = DiskResult<Bytes>> + Send;

    /// Discards everything at or after `offset`, which is how recovery drops a
    /// torn tail.
    fn truncate(&self, offset: u64) -> impl Future<Output = DiskResult<()>> + Send;

    /// The current size in bytes.
    fn size(&self) -> impl Future<Output = DiskResult<u64>> + Send;
}
