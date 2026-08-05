//! Sourcing AWS credentials without a long-lived key.
//!
//! `docs/REQUIREMENTS.md` says the S3 backend cannot be limited to static
//! keys: on AWS the node has to be able to assume a role, and it has to be
//! able to read the instance profile, because a long-lived access key sitting
//! in a Kubernetes Secret is the thing security-conscious AWS shops prohibit
//! outright. A deployment that cannot run without one fails review before it
//! starts.
//!
//! # Why not the AWS SDK
//!
//! This node signs its own requests. `orbita-objectstore` explains why —
//! the SDK's signer arrives welded to its own HTTP client, retry policy, and
//! clock, and a deterministic simulator cannot have any of those. Credential
//! sourcing has exactly the same property, and worse: a credential provider
//! with its own background refresh timer would make credential expiry an event
//! that happens between the operations a simulated run can observe. So the
//! providers here are built on the same two seams the store already uses,
//! `HttpTransport` for the network and an injected clock for time, and the
//! `aws-config` dependency is gone.
//!
//! # The shape
//!
//! - [`imds::InstanceProfile`] reads a session credential out of the EC2
//!   metadata service over IMDSv2. This is the required floor: no Secret
//!   anywhere.
//! - [`sts::AssumeRole`] exchanges any base credential for a session on a
//!   named role. This is the preferred deployment, and it layers over either
//!   base.
//! - [`refresh::RefreshingCredentials`] caches whatever a source produces and
//!   replaces it before it expires, without stalling traffic and without a
//!   background task.
//! - Static keys are `orbita_objectstore::s3::StaticCredentials` and need
//!   none of this, which is right: MinIO and R2 have nothing else to offer.

pub(crate) mod imds;
pub(crate) mod partition;
pub(crate) mod refresh;
pub(crate) mod sts;
mod timestamp;

#[cfg(test)]
pub(crate) mod testing;

pub use sts::{AssumeRoleConfig, DEFAULT_SESSION_DURATION_SECONDS};

use crate::config::{S3CredentialSource, S3StorageConfig};

use async_trait::async_trait;
use orbita_objectstore::s3::{
    Credentials, CredentialsProvider, HttpTransport, NowMillis, StaticCredentials,
};
use orbita_objectstore::ObjectResult;
use orbita_runtime::Clock;

use std::sync::Arc;

/// A credential and the instant it stops working.
///
/// `expires_at_millis` is `None` for a credential that never expires, which is
/// what makes a static key legal underneath the refresh logic with no special
/// case anywhere.
pub(crate) struct SessionCredentials {
    pub(crate) credentials: Credentials,
    pub(crate) expires_at_millis: Option<u64>,
}

/// Delegates to [`Credentials`], which redacts the secret and the session
/// token. The expiry is not a secret and is the one field worth seeing in a
/// log line about credential refresh.
impl std::fmt::Debug for SessionCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionCredentials")
            .field("credentials", &self.credentials)
            .field("expires_at_millis", &self.expires_at_millis)
            .finish()
    }
}

/// Something that can produce a fresh credential from scratch.
///
/// Separate from `CredentialsProvider` on purpose: a provider answers the
/// question "what should I sign with right now", which is usually a cache
/// read, while a source is the expensive thing behind the cache. Splitting
/// them is what keeps the refresh policy in one place instead of copied into
/// every source.
#[async_trait]
pub(crate) trait SessionSource: Send + Sync + 'static {
    async fn fetch(&self) -> ObjectResult<SessionCredentials>;

    /// A short phrase naming this source for a log message. It must never
    /// contain a credential; a role ARN or an endpoint is fine.
    fn describe(&self) -> String;
}

/// Builds the provider chain one worker signs its S3 requests with.
///
/// Time comes from the runtime's clock and the network from the shared
/// transport, so the whole chain is drivable by a simulated run: injecting an
/// expiry is moving the clock, and injecting an IMDS failure is answering one
/// request differently.
pub(crate) fn credentials_provider<C: Clock>(
    clock: &C,
    transport: Arc<dyn HttpTransport>,
    now_millis: NowMillis,
    config: &S3StorageConfig,
) -> ObjectResult<Arc<dyn CredentialsProvider>> {
    let base: Arc<dyn CredentialsProvider> = match &config.credentials {
        S3CredentialSource::Static(credentials) => StaticCredentials::shared(credentials.clone()),
        S3CredentialSource::InstanceProfile => Arc::new(refresh::RefreshingCredentials::new(
            clock.clone(),
            imds::InstanceProfile::new(transport.clone(), config.imds_authority()),
        )),
    };

    let Some(assume_role) = &config.assume_role else {
        return Ok(base);
    };
    Ok(Arc::new(refresh::RefreshingCredentials::new(
        clock.clone(),
        sts::AssumeRole::new(
            base,
            transport,
            now_millis,
            config.region.clone(),
            assume_role.clone(),
        )?,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aws::testing::ScriptedTransport;
    use orbita_objectstore::s3::{HttpResponse, TransportFailure};
    use orbita_runtime::tokio_runtime::TokioClock;

    fn storage_config(credentials: S3CredentialSource) -> S3StorageConfig {
        S3StorageConfig {
            endpoint: "https://s3.us-east-1.amazonaws.com".to_string(),
            bucket: "orbita".to_string(),
            region: "us-east-1".to_string(),
            credentials,
            assume_role: None,
            imds_endpoint: None,
            force_path_style: false,
        }
    }

    fn ok(body: &str) -> Result<HttpResponse, TransportFailure> {
        Ok(HttpResponse {
            status: 200,
            headers: vec![],
            body: bytes::Bytes::from(body.to_string()),
        })
    }

    fn static_keys() -> S3CredentialSource {
        S3CredentialSource::Static(Credentials {
            access_key_id: "AKIABASE".to_string(),
            secret_access_key: "base-secret".to_string(),
            session_token: None,
        })
    }

    #[tokio::test]
    async fn static_keys_reach_the_signer_without_touching_the_network() {
        let transport = ScriptedTransport::new(vec![]);
        let provider = credentials_provider(
            &TokioClock::new(),
            transport.clone(),
            Arc::new(|| 0),
            &storage_config(static_keys()),
        )
        .expect("valid configuration");

        let credentials = provider.credentials().await.expect("static");
        assert_eq!(credentials.access_key_id, "AKIABASE");
        assert!(transport.requests().is_empty());
    }

    #[tokio::test]
    async fn assume_role_layers_over_static_keys() {
        let mut config = storage_config(static_keys());
        config.assume_role = Some(AssumeRoleConfig::new(
            "arn:aws:iam::123456789012:role/orbita",
            "orbita-node-1",
        ));
        let transport = ScriptedTransport::new(vec![ok(
            "<Credentials><AccessKeyId>ASIAASSUMED</AccessKeyId>\
             <SecretAccessKey>s</SecretAccessKey><SessionToken>t</SessionToken>\
             <Expiration>2099-01-01T00:00:00Z</Expiration></Credentials>",
        )]);

        let provider = credentials_provider(
            &TokioClock::new(),
            transport.clone(),
            Arc::new(|| 1_369_353_600_000),
            &config,
        )
        .expect("valid configuration");

        let credentials = provider.credentials().await.expect("assumed");
        assert_eq!(credentials.access_key_id, "ASIAASSUMED");
        assert_eq!(transport.requests().len(), 1);
    }

    #[tokio::test]
    async fn assume_role_layers_over_the_instance_profile() {
        let mut config = storage_config(S3CredentialSource::InstanceProfile);
        config.assume_role = Some(AssumeRoleConfig::new(
            "arn:aws:iam::123456789012:role/orbita",
            "orbita-node-1",
        ));
        let transport = ScriptedTransport::new(vec![
            ok("the-imds-token"),
            ok("orbita-node-role"),
            ok(
                r#"{"AccessKeyId":"ASIAPROFILE","SecretAccessKey":"s","Token":"t",
                   "Expiration":"2099-01-01T00:00:00Z"}"#,
            ),
            ok("<Credentials><AccessKeyId>ASIAASSUMED</AccessKeyId>\
                <SecretAccessKey>s</SecretAccessKey><SessionToken>t</SessionToken>\
                <Expiration>2099-01-01T00:00:00Z</Expiration></Credentials>"),
        ]);

        let provider = credentials_provider(
            &TokioClock::new(),
            transport.clone(),
            Arc::new(|| 1_369_353_600_000),
            &config,
        )
        .expect("valid configuration");

        let credentials = provider.credentials().await.expect("assumed");
        assert_eq!(
            credentials.access_key_id, "ASIAASSUMED",
            "the assumed session, not the instance profile, is what signs S3 requests"
        );

        let requests = transport.requests();
        let assume = requests.last().expect("an AssumeRole call was made");
        assert!(
            assume
                .header("authorization")
                .expect("signed")
                .contains("Credential=ASIAPROFILE/"),
            "the instance profile credential must sign the exchange: {assume:?}"
        );
    }

    #[tokio::test]
    async fn the_instance_profile_alone_needs_no_secret_and_no_role() {
        let transport = ScriptedTransport::new(vec![
            ok("the-imds-token"),
            ok("orbita-node-role"),
            ok(
                r#"{"AccessKeyId":"ASIAPROFILE","SecretAccessKey":"s","Token":"t",
                   "Expiration":"2099-01-01T00:00:00Z"}"#,
            ),
        ]);
        let provider = credentials_provider(
            &TokioClock::new(),
            transport,
            Arc::new(|| 0),
            &storage_config(S3CredentialSource::InstanceProfile),
        )
        .expect("valid configuration");

        assert_eq!(
            provider.credentials().await.expect("sourced").access_key_id,
            "ASIAPROFILE"
        );
    }
}
