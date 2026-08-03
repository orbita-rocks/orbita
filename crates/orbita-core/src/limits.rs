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

/// Soft cap on the bytes a single `LIST` page may return, 4 MiB.
///
/// Soft because a page always returns at least one entry. A hard cap below
/// the size of a single value would make a scan unable to move past that key,
/// which is a worse failure than an oversized page.
///
/// With large values this binds long before [`MAX_LIST_LIMIT`] does: a
/// thousand entries at the maximum value size would be a ten gigabyte
/// response.
pub const MAX_LIST_BYTES: u64 = 4 * 1024 * 1024;

/// Room to allow for everything in a request that is not the value itself,
/// meaning the key, the keyspace name, and protobuf framing.
///
/// This exists so that a client sizing its connection can be told one number
/// rather than being asked to do the arithmetic and getting it slightly wrong.
pub const MESSAGE_OVERHEAD_BYTES: u64 = 64 * 1024;
