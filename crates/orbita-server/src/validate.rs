//! What the edge checks before anything interior sees a request.
//!
//! The limits live in `orbita_core` because they bound the design, not just
//! the policy: a WAL frame is sized against them and so is the RPC message
//! limit. Checking them here means no code past this point has to defend
//! against a 4GB value.

use orbita_core::{
    Error, KeyspaceName, Result, Version, WriteCondition, MAX_KEY_BYTES, MAX_LIST_LIMIT,
    MAX_VALUE_BYTES,
};
use orbita_proto::v1::{condition::Kind, Condition};

/// The page size used when a caller asks for no particular one.
///
/// Small enough that a client that ignores the cursor and pages one screen at
/// a time still works, large enough that a full scan is not a million round
/// trips.
pub(crate) const DEFAULT_LIST_LIMIT: u32 = 100;

pub(crate) fn key(key: &[u8]) -> Result<()> {
    if key.len() > MAX_KEY_BYTES {
        return Err(Error::TooLarge {
            what: "key",
            size: key.len(),
            limit: MAX_KEY_BYTES,
        });
    }
    Ok(())
}

pub(crate) fn value(value: &[u8]) -> Result<()> {
    if value.len() > MAX_VALUE_BYTES {
        return Err(Error::TooLarge {
            what: "value",
            size: value.len(),
            limit: MAX_VALUE_BYTES,
        });
    }
    Ok(())
}

pub(crate) fn prefix(prefix: &[u8]) -> Result<()> {
    if prefix.len() > MAX_KEY_BYTES {
        return Err(Error::TooLarge {
            what: "prefix",
            size: prefix.len(),
            limit: MAX_KEY_BYTES,
        });
    }
    Ok(())
}

/// Clamps a page size rather than rejecting it.
///
/// A limit above the maximum is a caller asking for as much as it can get,
/// which is a reasonable thing to ask and a silly thing to fail.
pub(crate) fn list_limit(limit: u32) -> u32 {
    match limit {
        0 => DEFAULT_LIST_LIMIT,
        n => n.min(MAX_LIST_LIMIT),
    }
}

/// Turns a keyspace name from the wire into one the rest of the system will
/// accept.
///
/// The character set is restricted because these names appear in object
/// storage paths, metrics labels, and credentials, so a rejected name here is
/// an escaping bug that never happens later.
pub(crate) fn keyspace_name(name: &str) -> Result<KeyspaceName> {
    KeyspaceName::new(name).map_err(|e| Error::InvalidArgument(e.to_string()))
}

/// An absent condition means write unconditionally, which is what the protocol
/// says and what most callers want.
pub(crate) fn condition(condition: Option<&Condition>) -> WriteCondition {
    match condition.and_then(|c| c.kind.as_ref()) {
        None => WriteCondition::None,
        Some(Kind::IfNotPresent(true)) => WriteCondition::IfNotPresent,
        // `if_not_present: false` is the default value of a proto3 bool, so it
        // cannot be told apart from an unset field by a client that meant
        // nothing by it. Treating it as unconditional is the reading that
        // cannot surprise anyone.
        Some(Kind::IfNotPresent(false)) => WriteCondition::None,
        Some(Kind::IfVersion(version)) => WriteCondition::IfVersion(Version(*version)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_oversized_key_is_rejected_at_the_edge() {
        assert!(key(&vec![0u8; MAX_KEY_BYTES]).is_ok());
        assert!(matches!(
            key(&vec![0u8; MAX_KEY_BYTES + 1]),
            Err(Error::TooLarge { what: "key", .. })
        ));
    }

    #[test]
    fn an_oversized_value_is_rejected_at_the_edge() {
        assert!(value(&vec![0u8; MAX_VALUE_BYTES]).is_ok());
        assert!(matches!(
            value(&vec![0u8; MAX_VALUE_BYTES + 1]),
            Err(Error::TooLarge { what: "value", .. })
        ));
    }

    #[test]
    fn an_empty_key_is_allowed_because_it_is_where_a_range_starts() {
        assert!(key(b"").is_ok());
    }

    #[test]
    fn a_page_size_is_clamped_rather_than_refused() {
        assert_eq!(list_limit(0), DEFAULT_LIST_LIMIT);
        assert_eq!(list_limit(10), 10);
        assert_eq!(list_limit(u32::MAX), MAX_LIST_LIMIT);
    }

    #[test]
    fn a_keyspace_name_that_could_escape_a_path_is_refused() {
        assert!(keyspace_name("catalog").is_ok());
        assert!(matches!(
            keyspace_name("../etc"),
            Err(Error::InvalidArgument(_))
        ));
    }

    #[test]
    fn an_unset_condition_writes_unconditionally() {
        assert_eq!(condition(None), WriteCondition::None);
        assert_eq!(
            condition(Some(&Condition { kind: None })),
            WriteCondition::None
        );
        assert_eq!(
            condition(Some(&Condition {
                kind: Some(Kind::IfNotPresent(false))
            })),
            WriteCondition::None,
            "a default-valued bool cannot be told from an unset one"
        );
    }

    #[test]
    fn a_version_condition_carries_through() {
        assert_eq!(
            condition(Some(&Condition {
                kind: Some(Kind::IfVersion(42))
            })),
            WriteCondition::IfVersion(Version(42))
        );
    }
}
