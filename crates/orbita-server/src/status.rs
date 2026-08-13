//! The one place `orbita_core::Error` becomes a gRPC status.
//!
//! Every handler funnels through here because a status code is part of the
//! wire contract: a client decides whether to retry from the code alone, and a
//! second copy of this mapping somewhere else would drift the moment either
//! copy was edited.

use orbita_core::Error;
use tonic::{Code, Status};

/// Turns an error into the status a client sees.
///
/// Retryable errors become `UNAVAILABLE` rather than something more precise,
/// because routing is deliberately invisible to clients: a request that landed
/// on the wrong node or arrived mid-failover is the cluster's problem to
/// explain, not the caller's problem to understand.
#[must_use]
pub fn to_status(error: &Error) -> Status {
    let code = match error {
        Error::NotFound => Code::NotFound,
        Error::AlreadyExists | Error::KeyspaceAlreadyExists => Code::AlreadyExists,
        Error::VersionMismatch { .. } => Code::FailedPrecondition,
        Error::KeyspaceNotFound => Code::NotFound,
        Error::NotOwner { .. }
        | Error::StaleEpoch { .. }
        | Error::Unavailable(_)
        | Error::NotLeader { .. } => Code::Unavailable,
        Error::TooLarge { .. } | Error::InvalidArgument(_) => Code::InvalidArgument,
        Error::QuotaExceeded(_) => Code::ResourceExhausted,
        Error::Unauthenticated => Code::Unauthenticated,
        Error::PermissionDenied => Code::PermissionDenied,
        // Unknown rather than Unavailable, and the difference is the point.
        // Most client stacks retry UNAVAILABLE by default, and this is the one
        // failure a client must not replay without looking first.
        Error::Indeterminate(_) => Code::Unknown,
        Error::Internal(_) => Code::Internal,
    };
    Status::new(code, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use orbita_core::{NodeId, PartitionId, Version, MAX_KEY_BYTES};

    #[test]
    fn a_lost_compare_and_swap_is_a_failed_precondition_not_an_error() {
        // A client that lost a race has to be able to tell that apart from a
        // malformed request, because one is retried and the other is a bug.
        let status = to_status(&Error::VersionMismatch {
            expected: Version(7),
            actual: Some(Version(9)),
        });
        assert_eq!(status.code(), Code::FailedPrecondition);
    }

    #[test]
    fn everything_a_client_should_retry_maps_to_unavailable() {
        for error in [
            Error::Unavailable("mid failover".into()),
            Error::NotOwner {
                partition: PartitionId(1),
                owner: Some(NodeId(2)),
            },
            Error::StaleEpoch {
                partition: PartitionId(1),
                got: 1.into(),
                current: 2.into(),
            },
        ] {
            assert!(error.is_retryable(), "{error} should be retryable");
            assert_eq!(to_status(&error).code(), Code::Unavailable, "{error}");
        }
    }

    #[test]
    fn an_oversized_key_is_the_callers_mistake() {
        let status = to_status(&Error::TooLarge {
            what: "key",
            size: MAX_KEY_BYTES + 1,
            limit: MAX_KEY_BYTES,
        });
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("exceeds limit"));
    }

    #[test]
    fn an_unknown_keyspace_is_not_found_rather_than_invalid() {
        assert_eq!(to_status(&Error::KeyspaceNotFound).code(), Code::NotFound);
    }

    #[test]
    fn quota_exhaustion_is_resource_exhausted() {
        assert_eq!(
            to_status(&Error::QuotaExceeded("writes".into())).code(),
            Code::ResourceExhausted
        );
    }
}
