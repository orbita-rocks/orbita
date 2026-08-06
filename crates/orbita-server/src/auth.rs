//! Credential enforcement on the worker's data surface.
//!
//! Every `Kv` RPC funnels through an [`Authenticator`] before it reaches a
//! partition. The rule it applies is the leader group's own, from
//! `orbita_control`: read the `authorization: Bearer <secret>` header, hash the
//! secret, and check it against a cached [`CredentialSnapshot`] for the
//! keyspace and permission the request needs. The cache is refreshed on the
//! same timer as the partition map, so enforcement costs a hash and a scan of a
//! small list rather than a control-plane round trip, and a revoked credential
//! is refused the next time it is presented rather than the next connection.
//!
//! # Authentication off
//!
//! A cluster can run with authentication off, and that is an explicit decision
//! carried in [`crate::ServerConfig::require_auth`] rather than an accident of
//! an empty credential set. When it is off, every request is allowed through
//! and no header is read. The CLI agrees: it sends a bearer token only when it
//! has one and otherwise leaves the header off, so an open cluster stays easy
//! to poke at and the server is the one place that decides whether a credential
//! was required.

use orbita_control::{bearer_secret, CredentialSnapshot, Permission};
use orbita_core::Result;
use orbita_runtime::Clock;

use std::sync::{Arc, Mutex};
use tonic::metadata::MetadataMap;

/// Enforces credentials against a cached snapshot for one node.
///
/// Generic over the clock so credential expiry is judged on the same clock a
/// simulated run drives, rather than the host wall clock.
pub(crate) struct Authenticator<C: Clock> {
    /// Whether a credential is required at all. Off means allow through.
    enabled: bool,
    clock: C,
    /// The last credential set fetched from the control plane. Swapped whole on
    /// each refresh; read on every request.
    snapshot: Arc<Mutex<CredentialSnapshot>>,
}

impl<C: Clock> Authenticator<C> {
    /// Builds an authenticator that starts with no credentials cached.
    ///
    /// An enabled authenticator with an empty cache refuses everything until
    /// its first refresh lands, which is the safe direction to fail: a worker
    /// that has never heard from the control plane cannot vouch for anyone.
    pub(crate) fn new(enabled: bool, clock: C) -> Self {
        Self {
            enabled,
            clock,
            snapshot: Arc::new(Mutex::new(CredentialSnapshot::default())),
        }
    }

    /// Replaces the cached credential set.
    ///
    /// Called from the control loop after each fetch. Swapping the whole set is
    /// what makes revocation take effect on the next request: the next check
    /// reads this set, not a verdict cached per connection.
    pub(crate) fn refresh(&self, snapshot: CredentialSnapshot) {
        *self.snapshot.lock().expect("credential cache poisoned") = snapshot;
    }

    /// Authorizes a `permission` on `keyspace` from a request's metadata.
    ///
    /// A no-op when authentication is off. Otherwise it is the worker's copy of
    /// `Controller::authenticate`: an unknown or missing secret is
    /// `Unauthenticated`, and a known secret out of scope, without the
    /// permission, or expired is `PermissionDenied`.
    pub(crate) fn authorize(
        &self,
        metadata: &MetadataMap,
        keyspace: &str,
        permission: Permission,
    ) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let secret = bearer_secret(header(metadata))?;
        let snapshot = self.snapshot.lock().expect("credential cache poisoned");
        snapshot.authorize(secret, keyspace, permission, self.clock.now_millis())
    }
}

/// The raw `authorization` header value, if the request carried one that is
/// representable as text. A binary or absent header reads as no header, which
/// [`bearer_secret`] turns into `Unauthenticated`.
fn header(metadata: &MetadataMap) -> Option<&str> {
    metadata
        .get("authorization")
        .and_then(|value| value.to_str().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    use orbita_control::{hash_secret, Credential};
    use orbita_core::Error;

    /// A clock frozen at a chosen instant, so expiry is exercised without a
    /// real timer. `Clock` also demands sleep and monotonic time, which these
    /// tests never reach.
    #[derive(Clone)]
    struct FrozenClock(u64);

    impl Clock for FrozenClock {
        fn now_millis(&self) -> u64 {
            self.0
        }

        fn monotonic_nanos(&self) -> u64 {
            0
        }

        async fn sleep(&self, _duration: std::time::Duration) {}
    }

    fn credential(secret: &str) -> Credential {
        Credential {
            id: "cred-1".into(),
            secret_hash: hash_secret(secret),
            keyspaces: vec!["catalog".into()],
            permissions: vec![Permission::Read, Permission::Write],
            description: String::new(),
            created_at_millis: 0,
            expires_at_millis: None,
        }
    }

    fn enabled(secret: &str) -> Authenticator<FrozenClock> {
        let auth = Authenticator::new(true, FrozenClock(0));
        auth.refresh(CredentialSnapshot::new(vec![credential(secret)]));
        auth
    }

    fn bearer(secret: &str) -> MetadataMap {
        let mut metadata = MetadataMap::new();
        metadata.insert(
            "authorization",
            format!("Bearer {secret}")
                .parse()
                .expect("a bearer token is a valid header"),
        );
        metadata
    }

    #[test]
    fn a_disabled_authenticator_allows_a_request_with_no_credential() {
        let auth = Authenticator::new(false, FrozenClock(0));
        let empty = MetadataMap::new();
        assert_eq!(auth.authorize(&empty, "catalog", Permission::Write), Ok(()));
    }

    #[test]
    fn a_valid_credential_passes() {
        let auth = enabled("s3cret");
        assert_eq!(
            auth.authorize(&bearer("s3cret"), "catalog", Permission::Write),
            Ok(())
        );
    }

    #[test]
    fn a_missing_credential_is_unauthenticated() {
        let auth = enabled("s3cret");
        assert_eq!(
            auth.authorize(&MetadataMap::new(), "catalog", Permission::Read),
            Err(Error::Unauthenticated)
        );
    }

    #[test]
    fn a_wrong_secret_is_unauthenticated() {
        let auth = enabled("s3cret");
        assert_eq!(
            auth.authorize(&bearer("nope"), "catalog", Permission::Read),
            Err(Error::Unauthenticated)
        );
    }

    #[test]
    fn a_wrong_keyspace_is_permission_denied() {
        let auth = enabled("s3cret");
        assert_eq!(
            auth.authorize(&bearer("s3cret"), "locks", Permission::Read),
            Err(Error::PermissionDenied)
        );
    }

    #[test]
    fn a_missing_permission_is_permission_denied() {
        let auth = Authenticator::new(true, FrozenClock(0));
        auth.refresh(CredentialSnapshot::new(vec![Credential {
            permissions: vec![Permission::Read],
            ..credential("s3cret")
        }]));
        assert_eq!(
            auth.authorize(&bearer("s3cret"), "catalog", Permission::Write),
            Err(Error::PermissionDenied)
        );
    }

    #[test]
    fn a_revoked_credential_is_refused_on_the_next_request() {
        let auth = enabled("s3cret");
        assert_eq!(
            auth.authorize(&bearer("s3cret"), "catalog", Permission::Read),
            Ok(())
        );
        // A revoke reaches the worker as a refresh with the credential gone.
        auth.refresh(CredentialSnapshot::default());
        assert_eq!(
            auth.authorize(&bearer("s3cret"), "catalog", Permission::Read),
            Err(Error::Unauthenticated)
        );
    }

    #[test]
    fn an_expired_credential_is_permission_denied() {
        let auth = Authenticator::new(true, FrozenClock(1_000));
        auth.refresh(CredentialSnapshot::new(vec![Credential {
            expires_at_millis: Some(1_000),
            ..credential("s3cret")
        }]));
        assert_eq!(
            auth.authorize(&bearer("s3cret"), "catalog", Permission::Read),
            Err(Error::PermissionDenied)
        );
    }

    #[test]
    fn the_refusal_codes_are_the_ones_67_will_assert() {
        use crate::status::to_status;
        use tonic::Code;

        let auth = enabled("s3cret");
        assert_eq!(
            to_status(
                &auth
                    .authorize(&MetadataMap::new(), "catalog", Permission::Read)
                    .unwrap_err()
            )
            .code(),
            Code::Unauthenticated
        );
        assert_eq!(
            to_status(
                &auth
                    .authorize(&bearer("s3cret"), "locks", Permission::Read)
                    .unwrap_err()
            )
            .code(),
            Code::PermissionDenied
        );
    }
}
