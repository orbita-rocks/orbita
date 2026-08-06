//! The one credential-enforcement rule, shared by the leader group and the
//! workers.
//!
//! [`crate::Controller::authenticate`] is the leader group's copy of this
//! check, keyed by credential id. A worker never has the id: the wire only
//! carries the secret, in an `authorization: Bearer <secret>` header, because
//! that is what every gRPC proxy and log scrubber already treats as sensitive.
//! So the worker's copy is keyed by the secret's hash instead, against a
//! [`CredentialSnapshot`] it refreshes on the same timer as its map. That is
//! what keeps the check off the control plane: enforcing on every request
//! costs a hash and a scan of a cached list, not a round trip, so a control
//! plane outage never becomes a data plane outage.
//!
//! The verdicts are pinned, because a client decides whether to retry from the
//! status code alone and issue #67 asserts on them: an unknown or malformed
//! secret is [`Error::Unauthenticated`], and a known secret that may not do
//! what it asked is [`Error::PermissionDenied`].

use crate::model::{hash_secret, Credential, Permission};

use orbita_core::{Error, Result};

/// A config-supplied secret, hashed the way the rest of the path hashes one.
///
/// This exists to bootstrap authentication. With auth on, the admin surface
/// itself demands a write-capable credential (see [`CredentialSnapshot::authorize_admin`]),
/// so the very first credential cannot be created through the log without one
/// already existing — a chicken-and-egg. An operator resolves it by naming a
/// root secret in configuration; the node hashes it here and overlays it onto
/// every snapshot, so a request bearing it is authorized before any log entry
/// exists and can then create the first real credential.
///
/// Its blast radius is total: a root request satisfies both the data-plane and
/// the admin rule as a fully privileged, all-keyspaces, write-capable,
/// non-expiring identity, with no scoping or expiry to fall back on. It is
/// therefore the operator's job to rotate it and to remove it once real
/// credentials exist; it is a bootstrap key, not a standing one. It is stored
/// only as this hash and only in memory, never written to the replicated log,
/// so a leaked log or snapshot does not hand it out.
#[must_use]
pub fn root_secret_hash(secret: &str) -> [u8; 32] {
    hash_secret(secret)
}

/// The prefix a bearer token carries in the `authorization` header.
const BEARER_PREFIX: &str = "Bearer ";

/// Pulls the secret out of an `authorization` header value.
///
/// `header` is the raw header string, or `None` when the request carried no
/// `authorization` at all. A request with no bearer token is
/// [`Error::Unauthenticated`] rather than a softer error because, on a cluster
/// with authentication on, "you sent no credential" and "you sent a bad one"
/// are the same failure to a caller: both mean authenticate and retry.
///
/// This lives here, transport-agnostic, so the leader group's admin surface
/// and a worker's data surface strip the token exactly the same way.
pub fn bearer_secret(header: Option<&str>) -> Result<&str> {
    header
        .and_then(|value| value.strip_prefix(BEARER_PREFIX))
        .filter(|secret| !secret.is_empty())
        .ok_or(Error::Unauthenticated)
}

/// A point-in-time copy of every live credential a worker enforces against.
///
/// Cheap enough to clone under a lock and hand to a request, and refreshed
/// whole rather than diffed because a credential set is small and a diff would
/// be a second thing to get wrong about revocation. Refreshing whole is also
/// what makes revocation take effect on the next request rather than the next
/// connection: the request reads whatever the latest refresh installed, so a
/// credential dropped from the set is refused the next time it is presented.
#[derive(Debug, Clone, Default)]
pub struct CredentialSnapshot {
    credentials: Vec<Credential>,
    /// The SHA-256 of the config-supplied root secret, if one is configured.
    ///
    /// This is an overlay, not a member of `credentials`: it is derived from a
    /// node's configuration rather than from the replicated log, so it is
    /// present before the first log entry exists and is never persisted. It is
    /// checked ahead of the credential scan so it authorizes on an empty set,
    /// which is what makes it a bootstrap identity. See [`root_secret_hash`].
    root: Option<[u8; 32]>,
}

impl CredentialSnapshot {
    /// Builds a snapshot from the credentials the leader group holds.
    #[must_use]
    pub fn new(credentials: Vec<Credential>) -> Self {
        Self {
            credentials,
            root: None,
        }
    }

    /// Overlays a config-derived root secret hash onto this snapshot.
    ///
    /// Applied at the enforcement boundary from a node's configuration, not
    /// from the log, so the root identity is available before any credential
    /// has been created and never travels through or is stored in the
    /// replicated state. `None` leaves the snapshot with no root, which is the
    /// default and what a cluster without a configured root runs with.
    #[must_use]
    pub fn with_root(mut self, root: Option<[u8; 32]>) -> Self {
        self.root = root;
        self
    }

    /// Whether `secret` is the configured root secret.
    ///
    /// A plain hash comparison, the same one the credential scan uses, so the
    /// root is matched by exactly the hashing the rest of the path uses and a
    /// wrong root secret simply fails to match and falls through to the scan.
    fn is_root(&self, secret: &str) -> bool {
        self.root.is_some_and(|hash| hash == hash_secret(secret))
    }

    /// The credentials in this snapshot, for the wire path that ships them to a
    /// worker.
    #[must_use]
    pub fn credentials(&self) -> &[Credential] {
        &self.credentials
    }

    /// Finds a credential by the hash of its secret.
    ///
    /// The secret is 256 bits of operating system entropy, so a plain SHA-256
    /// comparison is enough and a collision is not a threat model. A linear
    /// scan is fine because the credential set is an operator-sized list, not a
    /// per-tenant one.
    fn by_secret(&self, secret: &str) -> Option<&Credential> {
        let hash = hash_secret(secret);
        self.credentials.iter().find(|c| c.secret_hash == hash)
    }

    /// Authorizes a data-plane request: a `permission` on a `keyspace`.
    ///
    /// This is [`crate::Controller::authenticate`] against a cached view. An
    /// unknown secret is [`Error::Unauthenticated`]; a known secret that is
    /// out of scope, missing the permission, or expired is
    /// [`Error::PermissionDenied`], which is exactly what
    /// [`Credential::allows`] already decides.
    pub fn authorize(
        &self,
        secret: &str,
        keyspace: &str,
        permission: Permission,
        now_millis: u64,
    ) -> Result<()> {
        // The root is fully privileged and non-expiring, so it clears any
        // keyspace and permission. Checked first so it works on an empty set.
        if self.is_root(secret) {
            return Ok(());
        }
        let credential = self.by_secret(secret).ok_or(Error::Unauthenticated)?;
        if credential.allows(keyspace, permission, now_millis) {
            Ok(())
        } else {
            Err(Error::PermissionDenied)
        }
    }

    /// Authorizes a cluster-wide admin call.
    ///
    /// Admin operations are not keyspace-scoped the way a credential is, so
    /// there is no per-keyspace permission to check against. The coarse rule
    /// this iteration settles on: any unexpired credential that carries the
    /// `Write` permission on at least one keyspace may administer the cluster.
    /// A read-only or expired credential is refused. That deliberately treats
    /// "can write somewhere" as "can operate the cluster"; finer-grained admin
    /// roles are future work (issue #67), and the tradeoff is documented rather
    /// than hidden so a stricter rule can replace it without surprising anyone.
    ///
    /// An unknown secret is [`Error::Unauthenticated`]; a known but
    /// insufficient one is [`Error::PermissionDenied`].
    ///
    /// This is where the bootstrap chicken-and-egg lives: with auth on, admin
    /// demands a write-capable credential, but the first credential is created
    /// through admin. A configured root secret (see [`root_secret_hash`] and
    /// [`CredentialSnapshot::with_root`]) breaks the cycle: it satisfies this
    /// rule from configuration, without a log entry, so it can create the
    /// first real credential.
    pub fn authorize_admin(&self, secret: &str, now_millis: u64) -> Result<()> {
        // The root is a write-capable, non-expiring identity, so it may
        // administer the cluster. Checked ahead of the scan so it is the
        // bootstrap identity that creates the first real credential.
        if self.is_root(secret) {
            return Ok(());
        }
        let credential = self.by_secret(secret).ok_or(Error::Unauthenticated)?;
        if credential.is_expired(now_millis) {
            return Err(Error::PermissionDenied);
        }
        if credential.permissions.contains(&Permission::Write) && !credential.keyspaces.is_empty() {
            Ok(())
        } else {
            Err(Error::PermissionDenied)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn credential(secret: &str) -> Credential {
        Credential {
            id: "cred-1".into(),
            secret_hash: hash_secret(secret),
            keyspaces: vec!["catalog".into()],
            permissions: vec![Permission::Read, Permission::Write],
            description: "the catalog service".into(),
            created_at_millis: 0,
            expires_at_millis: None,
        }
    }

    fn snapshot(secret: &str) -> CredentialSnapshot {
        CredentialSnapshot::new(vec![credential(secret)])
    }

    #[test]
    fn a_bearer_token_yields_its_secret() {
        assert_eq!(bearer_secret(Some("Bearer s3cret")), Ok("s3cret"));
    }

    #[test]
    fn a_missing_or_empty_authorization_is_unauthenticated() {
        assert_eq!(bearer_secret(None), Err(Error::Unauthenticated));
        assert_eq!(bearer_secret(Some("")), Err(Error::Unauthenticated));
        assert_eq!(bearer_secret(Some("Bearer ")), Err(Error::Unauthenticated));
        assert_eq!(bearer_secret(Some("s3cret")), Err(Error::Unauthenticated));
    }

    #[test]
    fn a_matching_secret_passes_its_keyspace_and_permission() {
        assert_eq!(
            snapshot("s3cret").authorize("s3cret", "catalog", Permission::Write, 0),
            Ok(())
        );
    }

    #[test]
    fn an_unknown_secret_is_unauthenticated_not_permission_denied() {
        assert_eq!(
            snapshot("s3cret").authorize("wrong", "catalog", Permission::Read, 0),
            Err(Error::Unauthenticated)
        );
    }

    #[test]
    fn a_secret_out_of_its_keyspace_is_permission_denied() {
        assert_eq!(
            snapshot("s3cret").authorize("s3cret", "locks", Permission::Read, 0),
            Err(Error::PermissionDenied)
        );
    }

    #[test]
    fn a_secret_without_the_permission_is_permission_denied() {
        let read_only = Credential {
            permissions: vec![Permission::Read],
            ..credential("s3cret")
        };
        let snapshot = CredentialSnapshot::new(vec![read_only]);
        assert_eq!(
            snapshot.authorize("s3cret", "catalog", Permission::Write, 0),
            Err(Error::PermissionDenied)
        );
    }

    #[test]
    fn revoking_a_credential_refuses_it_on_the_next_check() {
        let snapshot = snapshot("s3cret");
        assert_eq!(
            snapshot.authorize("s3cret", "catalog", Permission::Read, 0),
            Ok(())
        );
        // A revoke is a refresh with the credential gone; the very next check
        // reads the new set rather than any per-connection verdict.
        let revoked = CredentialSnapshot::default();
        assert_eq!(
            revoked.authorize("s3cret", "catalog", Permission::Read, 0),
            Err(Error::Unauthenticated)
        );
    }

    #[test]
    fn an_expired_credential_is_refused_for_data_and_admin() {
        let expiring = Credential {
            expires_at_millis: Some(1_000),
            ..credential("s3cret")
        };
        let snapshot = CredentialSnapshot::new(vec![expiring]);
        assert_eq!(
            snapshot.authorize("s3cret", "catalog", Permission::Read, 1_000),
            Err(Error::PermissionDenied)
        );
        assert_eq!(
            snapshot.authorize_admin("s3cret", 1_000),
            Err(Error::PermissionDenied)
        );
    }

    #[test]
    fn admin_needs_a_write_capable_credential() {
        assert_eq!(snapshot("s3cret").authorize_admin("s3cret", 0), Ok(()));
        assert_eq!(
            snapshot("s3cret").authorize_admin("wrong", 0),
            Err(Error::Unauthenticated)
        );
        let read_only = CredentialSnapshot::new(vec![Credential {
            permissions: vec![Permission::Read],
            ..credential("s3cret")
        }]);
        assert_eq!(
            read_only.authorize_admin("s3cret", 0),
            Err(Error::PermissionDenied)
        );
    }

    #[test]
    fn a_configured_root_authorizes_data_and_admin_on_an_empty_set() {
        // No credentials created through the log yet: only the config overlay.
        let snapshot = CredentialSnapshot::default().with_root(Some(hash_secret("root")));
        assert_eq!(
            snapshot.authorize("root", "any-keyspace", Permission::Write, 0),
            Ok(())
        );
        assert_eq!(snapshot.authorize_admin("root", 0), Ok(()));
    }

    #[test]
    fn a_root_never_expires_and_reaches_every_keyspace() {
        let snapshot = CredentialSnapshot::default().with_root(Some(hash_secret("root")));
        // Far past any plausible expiry, and a keyspace no credential names.
        assert_eq!(
            snapshot.authorize("root", "locks", Permission::Read, u64::MAX),
            Ok(())
        );
        assert_eq!(snapshot.authorize_admin("root", u64::MAX), Ok(()));
    }

    #[test]
    fn a_wrong_root_secret_is_unauthenticated() {
        let snapshot = CredentialSnapshot::default().with_root(Some(hash_secret("root")));
        assert_eq!(
            snapshot.authorize("nope", "catalog", Permission::Read, 0),
            Err(Error::Unauthenticated)
        );
        assert_eq!(
            snapshot.authorize_admin("nope", 0),
            Err(Error::Unauthenticated)
        );
    }

    #[test]
    fn without_a_configured_root_an_empty_set_authorizes_nothing() {
        let snapshot = CredentialSnapshot::default();
        assert_eq!(
            snapshot.authorize("root", "catalog", Permission::Read, 0),
            Err(Error::Unauthenticated)
        );
        assert_eq!(
            snapshot.authorize_admin("root", 0),
            Err(Error::Unauthenticated)
        );
    }

    #[test]
    fn a_configured_root_coexists_with_a_normal_credential() {
        let snapshot = snapshot("s3cret").with_root(Some(hash_secret("root")));
        // The real credential still enforces its own scope,
        assert_eq!(
            snapshot.authorize("s3cret", "catalog", Permission::Write, 0),
            Ok(())
        );
        assert_eq!(
            snapshot.authorize("s3cret", "locks", Permission::Read, 0),
            Err(Error::PermissionDenied)
        );
        // while the root reaches past it.
        assert_eq!(
            snapshot.authorize("root", "locks", Permission::Write, 0),
            Ok(())
        );
    }
}
