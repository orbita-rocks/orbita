//! Hard limits from the product requirements.
//!
//! These are checked at the edge of the system so that no interior code has to
//! defend against an oversized key. They are constants rather than
//! configuration because they bound the design (WAL entry sizing, RPC message
//! limits), not just the policy.

/// Maximum key size, 10 KiB.
pub const MAX_KEY_BYTES: usize = 10 * 1024;

/// Maximum value size, 256 KiB.
pub const MAX_VALUE_BYTES: usize = 256 * 1024;

/// Maximum number of entries a single `LIST` page may return.
pub const MAX_LIST_LIMIT: u32 = 1000;
