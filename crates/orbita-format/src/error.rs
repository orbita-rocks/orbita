//! What can go wrong reading or writing the format.
//!
//! These are deliberately separate from [`orbita_core::Error`], which is the
//! vocabulary the API surface speaks. A reader of a bucket is not making an
//! API call, and telling it "internal error" would throw away the one thing it
//! needs, which is where in the object the bytes stopped making sense.

use orbita_core::Epoch;
use orbita_objectstore::ObjectError;

pub type Result<T> = std::result::Result<T, FormatError>;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FormatError {
    /// A version this build does not implement. Rejecting rather than guessing
    /// is what the compatibility rules require: a reader that tolerates an
    /// unknown version returns something other than what was stored.
    #[error("unsupported format version {found}, this build implements {supported}")]
    UnsupportedVersion { found: u64, supported: u64 },

    #[error("{what} does not start with the expected magic")]
    BadMagic { what: &'static str },

    #[error("{what} is truncated: needs {needed} bytes, has {found}")]
    Truncated {
        what: &'static str,
        needed: u64,
        found: u64,
    },

    #[error("{what} failed its checksum: stored {stored:#010x}, computed {computed:#010x}")]
    ChecksumMismatch {
        what: &'static str,
        stored: u32,
        computed: u32,
    },

    #[error("malformed {what}: {detail}")]
    Malformed { what: &'static str, detail: String },

    /// The partition's objects disagree with each other in a way no reader can
    /// resolve, such as two records for one key at the same Lamport. Distinct
    /// from [`FormatError::Malformed`] because the bytes decoded fine and it is
    /// the partition as a whole that is wrong.
    #[error("corrupt partition: {0}")]
    Corrupt(String),

    /// This writer's epoch has been superseded, so it must stop rather than
    /// commit. See the commit protocol: a deposed owner that writes anyway
    /// erases its replacement's work with no rule violated.
    #[error("writer at epoch {own} is deposed by a manifest at epoch {found}")]
    Deposed { own: Epoch, found: Epoch },

    /// A commit lost its compare-and-swap this many times in a row. A partition
    /// has one writer, so this means something else is writing the manifest.
    #[error("commit gave up after {attempts} lost races on manifest.json")]
    Contended { attempts: u32 },

    #[error(transparent)]
    Store(#[from] ObjectError),
}

impl FormatError {
    /// Whether the caller can usefully try the same thing again.
    ///
    /// Damage to an object is never retryable: a segment is written once and
    /// completely, so a checksum mismatch is real damage rather than a
    /// transient read.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(self, FormatError::Store(e) if e.is_retryable())
    }
}

impl From<FormatError> for orbita_core::Error {
    fn from(e: FormatError) -> Self {
        match &e {
            FormatError::Store(o) if o.is_retryable() => {
                orbita_core::Error::Unavailable(e.to_string())
            }
            _ => orbita_core::Error::Internal(e.to_string()),
        }
    }
}
