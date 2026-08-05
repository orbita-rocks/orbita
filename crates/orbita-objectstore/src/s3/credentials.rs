//! Where the signer gets its keys from.
//!
//! The store used to hold a [`Credentials`] value for its whole life, which is
//! correct for MinIO and R2 — they hand out nothing but static keys — and
//! wrong for AWS, where the credential a node should be using is a session
//! credential that expires within the hour. Holding one forces the caller to
//! rebuild the store, and with it the connection pool, every time it wants a
//! fresh key.
//!
//! So the store asks instead of holds. That is the whole seam: one call, made
//! once per request, that a provider answers however it likes. Static keys are
//! a provider that answers from a field; an instance-profile or assumed-role
//! provider answers from a cache it refreshes on its own schedule. The store
//! stays ignorant of the difference, which is the point — it must not grow a
//! refresh timer, and a provider must not grow an opinion about S3.
//!
//! Resolution is fallible because sourcing a credential can fail (the metadata
//! service is unreachable, the role was deleted) in a way reading a field
//! cannot, and the caller needs that as an error rather than as a 403 twenty
//! milliseconds later.

use crate::ObjectResult;
use async_trait::async_trait;
use std::sync::Arc;

pub use super::sign::Credentials;

/// Supplies the credential a request is signed with.
///
/// Dyn-compatible on purpose: the choice of provider is a deployment decision
/// read out of configuration, not something known at compile time, and one
/// boxed future per S3 round trip is not a cost worth a type parameter
/// threaded through the store.
#[async_trait]
pub trait CredentialsProvider: Send + Sync + 'static {
    /// The credential to sign the next request with.
    ///
    /// Called once per request, so an implementation that talks to the network
    /// must cache; the store deliberately has no idea whether this is a field
    /// read or a round trip, and will not rate-limit it.
    async fn credentials(&self) -> ObjectResult<Credentials>;
}

/// The provider for a key pair that was configured and never changes.
///
/// This is not a fallback or a legacy path. MinIO and R2 offer nothing else,
/// and a credential that cannot expire needs no refresh machinery, so the
/// simplest provider is also the right one for most non-AWS deployments.
pub struct StaticCredentials {
    credentials: Credentials,
}

impl StaticCredentials {
    #[must_use]
    pub fn new(credentials: Credentials) -> Self {
        Self { credentials }
    }

    /// The `Arc` form, which is what [`S3Store`](super::S3Store) takes.
    #[must_use]
    pub fn shared(credentials: Credentials) -> Arc<dyn CredentialsProvider> {
        Arc::new(Self::new(credentials))
    }
}

/// Delegates to [`Credentials`], which redacts the secret and the session
/// token, so a provider caught in a `{:?}` is not a credential leak.
impl std::fmt::Debug for StaticCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticCredentials")
            .field("credentials", &self.credentials)
            .finish()
    }
}

#[async_trait]
impl CredentialsProvider for StaticCredentials {
    async fn credentials(&self) -> ObjectResult<Credentials> {
        Ok(self.credentials.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn credentials() -> Credentials {
        Credentials {
            access_key_id: "AKID".to_string(),
            secret_access_key: "the-secret".to_string(),
            session_token: Some("the-session-token".to_string()),
        }
    }

    #[tokio::test]
    async fn static_credentials_answer_the_same_key_every_time() {
        let provider = StaticCredentials::new(credentials());
        let first = provider.credentials().await.expect("static never fails");
        let second = provider.credentials().await.expect("static never fails");
        assert_eq!(first.access_key_id, second.access_key_id);
        assert_eq!(first.secret_access_key, second.secret_access_key);
    }

    #[test]
    fn a_debug_logged_provider_does_not_print_the_secret() {
        let formatted = format!("{:?}", StaticCredentials::new(credentials()));
        assert!(
            !formatted.contains("the-secret") && !formatted.contains("the-session-token"),
            "secrets leaked into Debug output: {formatted}"
        );
    }
}
