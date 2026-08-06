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
//!
//! # A stale cache fails closed
//!
//! The cache is the whole reason enforcement stays off the control plane's back,
//! but it is also a trust window: a fetch that fails leaves the last good set in
//! place, so a credential revoked during a control-plane outage would keep
//! authorizing for as long as the outage lasted, which contradicts the promise
//! that revocation takes effect on the next request. So the cache carries a
//! maximum staleness. Once a fetched set has gone longer than that without a
//! successful refresh, it is no longer trusted: a request that would have leaned
//! on it is refused [`Error::Unavailable`] — retryable, because the moment the
//! worker reaches the control plane again the set is fresh and the same
//! credential works. The configured root is exempt: it comes from this node's
//! own configuration, not the log, so it cannot be revoked there and staleness
//! cannot make it wrong.

use orbita_control::{bearer_secret, CredentialSnapshot, Permission};
use orbita_core::{Error, Result};
use orbita_runtime::Clock;

use std::sync::{Arc, Mutex};
use std::time::Duration;
use tonic::metadata::MetadataMap;

/// The cached credential set and when it was last refreshed, kept together so a
/// reader judges the set's age against the same lock that swapped it.
struct Cached {
    snapshot: CredentialSnapshot,
    /// When the set now in `snapshot` last came from a successful refresh, on
    /// the authenticator's clock. `None` until the first refresh lands: before
    /// that the set is empty apart from the root overlay, so there is no stale
    /// set to distrust and the empty set refuses on its own.
    refreshed_at_millis: Option<u64>,
}

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
    /// How long a fetched set may go without a successful refresh before it is
    /// no longer trusted. Bounds how long a revoked credential can outlive its
    /// revocation during a control-plane outage. See the module docs.
    max_staleness_millis: u64,
    /// The last credential set fetched from the control plane, and its age.
    /// Swapped whole on each refresh; read on every request.
    cached: Arc<Mutex<Cached>>,
}

impl<C: Clock> Authenticator<C> {
    /// Builds an authenticator that starts with only the config root, if any.
    ///
    /// An enabled authenticator with an empty cache refuses everything until
    /// its first refresh lands, which is the safe direction to fail: a worker
    /// that has never heard from the control plane cannot vouch for anyone.
    /// The one exception is the configured root, which is overlaid here so it
    /// works during exactly that window and is what bootstrap depends on.
    ///
    /// `max_staleness` bounds how long a fetched set is trusted after refreshes
    /// stop succeeding; see the module docs and [`Authenticator::authorize`].
    pub(crate) fn new(
        enabled: bool,
        root: Option<[u8; 32]>,
        max_staleness: Duration,
        clock: C,
    ) -> Self {
        Self {
            enabled,
            clock,
            root,
            max_staleness_millis: max_staleness.as_millis().min(u128::from(u64::MAX)) as u64,
            cached: Arc::new(Mutex::new(Cached {
                snapshot: CredentialSnapshot::default().with_root(root),
                refreshed_at_millis: None,
            })),
        }
    }

    /// Replaces the cached credential set.
    ///
    /// Called from the control loop after each fetch. Swapping the whole set is
    /// what makes revocation take effect on the next request: the next check
    /// reads this set, not a verdict cached per connection. The config root is
    /// re-overlaid onto the incoming set so a refresh never drops it; the log
    /// never carries the root, so nothing in a fetch would restore it. The
    /// refresh time is stamped here so a later request can tell a fresh set
    /// from one the control plane stopped confirming.
    pub(crate) fn refresh(&self, snapshot: CredentialSnapshot) {
        let mut cached = self.cached.lock().expect("credential cache poisoned");
        cached.snapshot = snapshot.with_root(self.root);
        cached.refreshed_at_millis = Some(self.clock.now_millis());
    }

    /// Authorizes a `permission` on `keyspace` from a request's metadata.
    ///
    /// A no-op when authentication is off. Otherwise it is the worker's copy of
    /// `Controller::authenticate`: an unknown or missing secret is
    /// `Unauthenticated`, and a known secret out of scope, without the
    /// permission, or expired is `PermissionDenied`.
    ///
    /// A fetched set that has gone longer than `max_staleness` without a
    /// successful refresh is no longer trusted: a request that would rely on it
    /// is refused [`Error::Unavailable`] rather than authorized against an
    /// arbitrarily old set, so a credential revoked during a control-plane
    /// outage stops working within the bound instead of lasting the outage. The
    /// configured root is checked first and is exempt, because it is not a
    /// fetched credential and cannot be revoked through the log.
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
        let now = self.clock.now_millis();
        let cached = self.cached.lock().expect("credential cache poisoned");
        // The root is config-derived, never revocable through the log, so it
        // authorizes regardless of how stale the fetched set is. Checked
        // against a root-only view so a match here can only be the root.
        if CredentialSnapshot::default()
            .with_root(self.root)
            .authorize(secret, keyspace, permission, now)
            .is_ok()
        {
            return Ok(());
        }
        // Past that, a set the control plane has not confirmed within the bound
        // is not trusted. Fail closed and retryable: the fetched set may name
        // this secret, but it may also have dropped it in a revoke this node
        // has not yet heard, and lasting the outage is exactly the bug.
        if let Some(refreshed_at) = cached.refreshed_at_millis {
            if now.saturating_sub(refreshed_at) > self.max_staleness_millis {
                return Err(Error::Unavailable(
                    "credential cache is stale: the control plane has been unreachable past the \
                     staleness bound, so this node will not authorize from a possibly-revoked set"
                        .into(),
                ));
            }
        }
        cached.snapshot.authorize(secret, keyspace, permission, now)
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

    use std::sync::atomic::{AtomicU64, Ordering};

    /// A generous staleness bound for tests that are not about staleness: the
    /// frozen clock never advances, so the fetched set is always age zero and
    /// this bound is never reached.
    const NEVER_STALE: Duration = Duration::from_secs(3_600);

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

    /// A clock the test moves by hand, so a fetched set can be aged past the
    /// staleness bound without a real timer.
    #[derive(Clone)]
    struct MovableClock(Arc<AtomicU64>);

    impl MovableClock {
        fn new(start: u64) -> Self {
            Self(Arc::new(AtomicU64::new(start)))
        }

        fn advance(&self, millis: u64) {
            self.0.fetch_add(millis, Ordering::SeqCst);
        }
    }

    impl Clock for MovableClock {
        fn now_millis(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
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
        let auth = Authenticator::new(true, None, NEVER_STALE, FrozenClock(0));
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
        let auth = Authenticator::new(false, None, NEVER_STALE, FrozenClock(0));
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
        let auth = Authenticator::new(true, None, NEVER_STALE, FrozenClock(0));
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
        let auth = Authenticator::new(true, None, NEVER_STALE, FrozenClock(1_000));
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
    fn a_configured_root_is_authorized_before_the_first_refresh() {
        // The bootstrap window on the data plane: auth on, nothing fetched from
        // the control plane yet, so the cache is empty apart from the overlay.
        let root = orbita_control::root_secret_hash("root-secret");
        let auth = Authenticator::new(true, Some(root), NEVER_STALE, FrozenClock(0));
        assert_eq!(
            auth.authorize(&bearer("root-secret"), "any-keyspace", Permission::Write),
            Ok(())
        );
    }

    #[test]
    fn a_refresh_does_not_drop_the_configured_root() {
        let root = orbita_control::root_secret_hash("root-secret");
        let auth = Authenticator::new(true, Some(root), NEVER_STALE, FrozenClock(0));
        // A fetch delivers a set that knows nothing of the root.
        auth.refresh(CredentialSnapshot::new(vec![credential("s3cret")]));
        assert_eq!(
            auth.authorize(&bearer("root-secret"), "locks", Permission::Write),
            Ok(()),
            "the root survives a refresh; the log never carries it to restore"
        );
        assert_eq!(
            auth.authorize(&bearer("s3cret"), "catalog", Permission::Write),
            Ok(()),
            "and the fetched credentials still enforce their own scope"
        );
    }

    #[test]
    fn a_wrong_root_secret_is_unauthenticated_on_the_data_plane() {
        let root = orbita_control::root_secret_hash("root-secret");
        let auth = Authenticator::new(true, Some(root), NEVER_STALE, FrozenClock(0));
        assert_eq!(
            auth.authorize(&bearer("not-the-root"), "catalog", Permission::Read),
            Err(Error::Unauthenticated)
        );
    }

    #[test]
    fn auth_off_ignores_the_root_entirely() {
        let root = orbita_control::root_secret_hash("root-secret");
        let auth = Authenticator::new(false, Some(root), NEVER_STALE, FrozenClock(0));
        assert_eq!(
            auth.authorize(&MetadataMap::new(), "catalog", Permission::Write),
            Ok(())
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

    #[test]
    fn a_fetched_credential_stops_authorizing_once_the_cache_is_too_stale() {
        let clock = MovableClock::new(0);
        let auth = Authenticator::new(true, None, Duration::from_millis(1_000), clock.clone());
        auth.refresh(CredentialSnapshot::new(vec![credential("s3cret")]));
        // Fresh: the credential works.
        assert_eq!(
            auth.authorize(&bearer("s3cret"), "catalog", Permission::Read),
            Ok(())
        );
        // Within the bound: still trusted.
        clock.advance(1_000);
        assert_eq!(
            auth.authorize(&bearer("s3cret"), "catalog", Permission::Read),
            Ok(())
        );
        // Past the bound with no successful refresh: the set is no longer
        // trusted, so a request that would rely on it fails closed and
        // retryable rather than authorizing from a possibly-revoked snapshot.
        clock.advance(1);
        assert_eq!(
            auth.authorize(&bearer("s3cret"), "catalog", Permission::Read),
            Err(Error::Unavailable(
                "credential cache is stale: the control plane has been unreachable past the \
                 staleness bound, so this node will not authorize from a possibly-revoked set"
                    .into()
            ))
        );
    }

    #[test]
    fn a_successful_refresh_resets_the_staleness_clock() {
        let clock = MovableClock::new(0);
        let auth = Authenticator::new(true, None, Duration::from_millis(1_000), clock.clone());
        auth.refresh(CredentialSnapshot::new(vec![credential("s3cret")]));
        clock.advance(1_500);
        // Stale now.
        assert!(auth
            .authorize(&bearer("s3cret"), "catalog", Permission::Read)
            .is_err());
        // A refresh at the current time makes it fresh again.
        auth.refresh(CredentialSnapshot::new(vec![credential("s3cret")]));
        assert_eq!(
            auth.authorize(&bearer("s3cret"), "catalog", Permission::Read),
            Ok(())
        );
    }

    #[test]
    fn the_root_still_authorizes_even_when_the_cache_is_stale() {
        // The root is config-derived, not a fetched credential, so staleness of
        // the fetched set cannot make it wrong. It keeps working through an
        // outage that has aged everything else out.
        let root = orbita_control::root_secret_hash("root-secret");
        let clock = MovableClock::new(0);
        let auth = Authenticator::new(
            true,
            Some(root),
            Duration::from_millis(1_000),
            clock.clone(),
        );
        auth.refresh(CredentialSnapshot::new(vec![credential("s3cret")]));
        clock.advance(10_000);
        // The fetched credential is aged out,
        assert!(auth
            .authorize(&bearer("s3cret"), "catalog", Permission::Read)
            .is_err());
        // but the root is not.
        assert_eq!(
            auth.authorize(&bearer("root-secret"), "any-keyspace", Permission::Write),
            Ok(())
        );
    }

    #[test]
    fn a_stale_cache_refuses_retryable_not_as_a_hard_denial() {
        use crate::status::to_status;
        use tonic::Code;

        let clock = MovableClock::new(0);
        let auth = Authenticator::new(true, None, Duration::from_millis(1_000), clock.clone());
        auth.refresh(CredentialSnapshot::new(vec![credential("s3cret")]));
        clock.advance(2_000);
        let status = to_status(
            &auth
                .authorize(&bearer("s3cret"), "catalog", Permission::Read)
                .unwrap_err(),
        );
        assert_eq!(
            status.code(),
            Code::Unavailable,
            "a stale cache is a transient server condition, so the client should retry"
        );
    }
}
