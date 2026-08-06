//! Credential enforcement on the worker's data surface.
//!
//! Every `Kv` RPC funnels through an [`Authenticator`] as the first step of the
//! node's admission boundary, before it reaches a partition and before it can
//! spend a rate token or be measured against storage. The rule it applies is
//! the leader group's own, from `orbita_control`: read the
//! `authorization: Bearer <secret>` header, hash the secret, and check it
//! against a cached [`CredentialSnapshot`] for the keyspace and permission the
//! request needs. The cache is refreshed on the same timer as the partition
//! map, so enforcement costs a hash and a scan of a small list rather than a
//! control-plane round trip, and a revoked credential is refused the next time
//! it is presented rather than the next connection.
//!
//! The header is extracted from the gRPC metadata at the transport edge and
//! handed in as a plain `Option<&str>`, so this type — and the [`crate::node`]
//! admission path that drives it — never depends on `tonic`.
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

/// Enforces credentials against a cached snapshot for one node.
///
/// Generic over the clock so credential expiry is judged on the same clock a
/// simulated run drives, rather than the host wall clock.
pub(crate) struct Authenticator<C: Clock> {
    /// Whether a credential is required at all. Off means allow through.
    enabled: bool,
    clock: C,
    /// The config-derived root secret hash, if the operator configured one.
    ///
    /// This is the bootstrap identity. It is applied to every cached snapshot
    /// so it is authorized before the first refresh lands and independently of
    /// what the control plane ships — a worker that has never heard from the
    /// control plane still honors it, because it comes from this node's own
    /// configuration rather than from the log. It is only ever this hash and
    /// only in memory; see [`orbita_control::root_secret_hash`] for why it
    /// exists and its blast radius.
    root: Option<[u8; 32]>,
    /// The last credential set fetched from the control plane. Swapped whole on
    /// each refresh; read on every request.
    snapshot: Arc<Mutex<CredentialSnapshot>>,
}

impl<C: Clock> Authenticator<C> {
    /// Builds an authenticator that starts with only the config root, if any.
    ///
    /// An enabled authenticator with an empty cache refuses everything until
    /// its first refresh lands, which is the safe direction to fail: a worker
    /// that has never heard from the control plane cannot vouch for anyone.
    /// The one exception is the configured root, which is overlaid here so it
    /// works during exactly that window and is what bootstrap depends on.
    pub(crate) fn new(enabled: bool, root: Option<[u8; 32]>, clock: C) -> Self {
        Self {
            enabled,
            clock,
            root,
            snapshot: Arc::new(Mutex::new(CredentialSnapshot::default().with_root(root))),
        }
    }

    /// Replaces the cached credential set.
    ///
    /// Called from the control loop after each fetch. Swapping the whole set is
    /// what makes revocation take effect on the next request: the next check
    /// reads this set, not a verdict cached per connection. The config root is
    /// re-overlaid onto the incoming set so a refresh never drops it; the log
    /// never carries the root, so nothing in a fetch would restore it.
    pub(crate) fn refresh(&self, snapshot: CredentialSnapshot) {
        *self.snapshot.lock().expect("credential cache poisoned") = snapshot.with_root(self.root);
    }

    /// Authorizes a `permission` on `keyspace` from a request's `authorization`
    /// header value.
    ///
    /// `credential` is the raw header the client sent, already lifted out of the
    /// transport metadata by the caller; `None` means the request carried no
    /// usable header at all. A no-op when authentication is off. Otherwise it is
    /// the worker's copy of `Controller::authenticate`: an unknown or missing
    /// secret is `Unauthenticated`, and a known secret out of scope, without the
    /// permission, or expired is `PermissionDenied`.
    pub(crate) fn authorize(
        &self,
        credential: Option<&str>,
        keyspace: &str,
        permission: Permission,
    ) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let secret = bearer_secret(credential)?;
        let snapshot = self.snapshot.lock().expect("credential cache poisoned");
        snapshot.authorize(secret, keyspace, permission, self.clock.now_millis())
    }
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
        let auth = Authenticator::new(true, None, FrozenClock(0));
        auth.refresh(CredentialSnapshot::new(vec![credential(secret)]));
        auth
    }

    /// The raw `authorization` header value a client presenting `secret` would
    /// send, as the transport edge would hand it to [`Authenticator::authorize`].
    fn bearer(secret: &str) -> String {
        format!("Bearer {secret}")
    }

    #[test]
    fn a_disabled_authenticator_allows_a_request_with_no_credential() {
        let auth = Authenticator::new(false, None, FrozenClock(0));
        assert_eq!(auth.authorize(None, "catalog", Permission::Write), Ok(()));
    }

    #[test]
    fn a_valid_credential_passes() {
        let auth = enabled("s3cret");
        assert_eq!(
            auth.authorize(
                Some(bearer("s3cret").as_str()),
                "catalog",
                Permission::Write
            ),
            Ok(())
        );
    }

    #[test]
    fn a_missing_credential_is_unauthenticated() {
        let auth = enabled("s3cret");
        assert_eq!(
            auth.authorize(None, "catalog", Permission::Read),
            Err(Error::Unauthenticated)
        );
    }

    #[test]
    fn a_wrong_secret_is_unauthenticated() {
        let auth = enabled("s3cret");
        assert_eq!(
            auth.authorize(Some(bearer("nope").as_str()), "catalog", Permission::Read),
            Err(Error::Unauthenticated)
        );
    }

    #[test]
    fn a_wrong_keyspace_is_permission_denied() {
        let auth = enabled("s3cret");
        assert_eq!(
            auth.authorize(Some(bearer("s3cret").as_str()), "locks", Permission::Read),
            Err(Error::PermissionDenied)
        );
    }

    #[test]
    fn a_missing_permission_is_permission_denied() {
        let auth = Authenticator::new(true, None, FrozenClock(0));
        auth.refresh(CredentialSnapshot::new(vec![Credential {
            permissions: vec![Permission::Read],
            ..credential("s3cret")
        }]));
        assert_eq!(
            auth.authorize(
                Some(bearer("s3cret").as_str()),
                "catalog",
                Permission::Write
            ),
            Err(Error::PermissionDenied)
        );
    }

    #[test]
    fn a_revoked_credential_is_refused_on_the_next_request() {
        let auth = enabled("s3cret");
        assert_eq!(
            auth.authorize(Some(bearer("s3cret").as_str()), "catalog", Permission::Read),
            Ok(())
        );
        // A revoke reaches the worker as a refresh with the credential gone.
        auth.refresh(CredentialSnapshot::default());
        assert_eq!(
            auth.authorize(Some(bearer("s3cret").as_str()), "catalog", Permission::Read),
            Err(Error::Unauthenticated)
        );
    }

    #[test]
    fn an_expired_credential_is_permission_denied() {
        let auth = Authenticator::new(true, None, FrozenClock(1_000));
        auth.refresh(CredentialSnapshot::new(vec![Credential {
            expires_at_millis: Some(1_000),
            ..credential("s3cret")
        }]));
        assert_eq!(
            auth.authorize(Some(bearer("s3cret").as_str()), "catalog", Permission::Read),
            Err(Error::PermissionDenied)
        );
    }

    #[test]
    fn a_configured_root_is_authorized_before_the_first_refresh() {
        // The bootstrap window on the data plane: auth on, nothing fetched from
        // the control plane yet, so the cache is empty apart from the overlay.
        let root = orbita_control::root_secret_hash("root-secret");
        let auth = Authenticator::new(true, Some(root), FrozenClock(0));
        assert_eq!(
            auth.authorize(
                Some(bearer("root-secret").as_str()),
                "any-keyspace",
                Permission::Write
            ),
            Ok(())
        );
    }

    #[test]
    fn a_refresh_does_not_drop_the_configured_root() {
        let root = orbita_control::root_secret_hash("root-secret");
        let auth = Authenticator::new(true, Some(root), FrozenClock(0));
        // A fetch delivers a set that knows nothing of the root.
        auth.refresh(CredentialSnapshot::new(vec![credential("s3cret")]));
        assert_eq!(
            auth.authorize(
                Some(bearer("root-secret").as_str()),
                "locks",
                Permission::Write
            ),
            Ok(()),
            "the root survives a refresh; the log never carries it to restore"
        );
        assert_eq!(
            auth.authorize(
                Some(bearer("s3cret").as_str()),
                "catalog",
                Permission::Write
            ),
            Ok(()),
            "and the fetched credentials still enforce their own scope"
        );
    }

    #[test]
    fn a_wrong_root_secret_is_unauthenticated_on_the_data_plane() {
        let root = orbita_control::root_secret_hash("root-secret");
        let auth = Authenticator::new(true, Some(root), FrozenClock(0));
        assert_eq!(
            auth.authorize(
                Some(bearer("not-the-root").as_str()),
                "catalog",
                Permission::Read
            ),
            Err(Error::Unauthenticated)
        );
    }

    #[test]
    fn auth_off_ignores_the_root_entirely() {
        let root = orbita_control::root_secret_hash("root-secret");
        let auth = Authenticator::new(false, Some(root), FrozenClock(0));
        assert_eq!(auth.authorize(None, "catalog", Permission::Write), Ok(()));
    }

    #[test]
    fn the_refusal_codes_are_the_ones_67_will_assert() {
        use crate::status::to_status;
        use tonic::Code;

        let auth = enabled("s3cret");
        assert_eq!(
            to_status(
                &auth
                    .authorize(None, "catalog", Permission::Read)
                    .unwrap_err()
            )
            .code(),
            Code::Unauthenticated
        );
        assert_eq!(
            to_status(
                &auth
                    .authorize(Some(bearer("s3cret").as_str()), "locks", Permission::Read)
                    .unwrap_err()
            )
            .code(),
            Code::PermissionDenied
        );
    }
}
