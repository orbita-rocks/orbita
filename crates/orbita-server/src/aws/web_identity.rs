//! Session credentials from `sts:AssumeRoleWithWebIdentity`.
//!
//! This is how a pod on EKS authenticates under IRSA. The EKS pod identity
//! webhook projects a short-lived OIDC service account token into the
//! container and sets `AWS_WEB_IDENTITY_TOKEN_FILE` and `AWS_ROLE_ARN`; the
//! node trades that token for a session on the role.
//!
//! # Why this exists rather than "just use the instance profile"
//!
//! On EKS the instance profile is the *node* role, and the workload role is
//! something else entirely. A pod that falls back to the node role does not
//! fail — it succeeds, as the wrong principal, with whatever the node group
//! happens to be allowed to do. That is a silent privilege change and it is
//! invisible in everything except CloudTrail. So IRSA is a first-class source
//! here, and it is tried before IMDS, exactly as the AWS SDKs order them.
//!
//! # The request is unsigned
//!
//! `AssumeRoleWithWebIdentity` is the one STS call that takes no SigV4
//! signature: the OIDC token *is* the authentication. That is what makes it a
//! bootstrap source rather than something that layers over a base credential
//! the way [`super::sts::AssumeRole`] does.
//!
//! # The token is re-read every time
//!
//! The projected token has a lifetime measured in an hour or so and kubelet
//! rewrites the file in place. Caching its contents would mean presenting an
//! expired assertion after the first rotation, so every fetch reads the file.

use super::sts::{form_encode, session_from_response, split_endpoint, STS_API_VERSION};
use super::{SessionCredentials, SessionSource};

use async_trait::async_trait;
use bytes::Bytes;
use orbita_objectstore::s3::{HttpRequest, HttpTransport, Scheme};
use orbita_objectstore::{ObjectError, ObjectResult};

use std::path::PathBuf;
use std::sync::Arc;

/// Where the EKS pod identity webhook says the projected token lives.
pub(crate) const TOKEN_FILE_VARIABLE: &str = "AWS_WEB_IDENTITY_TOKEN_FILE";
/// The role the webhook says to assume, from the service account annotation.
pub(crate) const ROLE_ARN_VARIABLE: &str = "AWS_ROLE_ARN";
/// The session name the webhook may set. Optional; AWS defaults it.
pub(crate) const SESSION_NAME_VARIABLE: &str = "AWS_ROLE_SESSION_NAME";

/// What IRSA needs to exchange a projected token for a session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebIdentityConfig {
    /// The file the OIDC token is projected into. Re-read on every fetch.
    pub token_file: PathBuf,
    /// The role the token is presented against, as a full ARN.
    pub role_arn: String,
    /// The name the session appears under in CloudTrail.
    pub session_name: String,
    /// Overrides the regional STS endpoint. Same escape hatch as
    /// [`super::AssumeRoleConfig::endpoint`], and it matters for the same
    /// partitions.
    pub endpoint: Option<String>,
    /// How long each session lasts, in seconds.
    pub duration_seconds: u32,
}

/// Exchanges a projected OIDC token for a session on a role.
pub(crate) struct WebIdentity {
    transport: Arc<dyn HttpTransport>,
    authority: String,
    scheme: Scheme,
    config: WebIdentityConfig,
}

impl WebIdentity {
    pub(crate) fn new(
        transport: Arc<dyn HttpTransport>,
        region: &str,
        config: WebIdentityConfig,
    ) -> ObjectResult<Self> {
        if config.role_arn.is_empty() {
            return Err(ObjectError::Other(format!(
                "web identity credentials need a role ARN; set {ROLE_ARN_VARIABLE} or \
                 object_store.role_arn"
            )));
        }
        let endpoint = config
            .endpoint
            .clone()
            .unwrap_or_else(|| super::sts::default_endpoint(region));
        let (scheme, authority) = split_endpoint(&endpoint)?;
        Ok(Self {
            transport,
            authority,
            scheme,
            config,
        })
    }

    /// The projected token, read fresh.
    ///
    /// The read error carries the path but never the contents: the file holds
    /// a bearer assertion good for assuming the role.
    fn token(&self) -> ObjectResult<String> {
        let raw = std::fs::read_to_string(&self.config.token_file).map_err(|error| {
            ObjectError::AccessDenied(format!(
                "reading the web identity token from {}: {error}",
                self.config.token_file.display()
            ))
        })?;
        let token = raw.trim().to_string();
        if token.is_empty() {
            return Err(ObjectError::AccessDenied(format!(
                "the web identity token file {} is empty; the projected service account token \
                 has not been written yet",
                self.config.token_file.display()
            )));
        }
        Ok(token)
    }

    fn body(&self, token: &str) -> Bytes {
        let pairs = [
            ("Action", "AssumeRoleWithWebIdentity"),
            ("Version", STS_API_VERSION),
            ("RoleArn", &self.config.role_arn),
            ("RoleSessionName", &self.config.session_name),
            ("DurationSeconds", &self.config.duration_seconds.to_string()),
            ("WebIdentityToken", token),
        ];
        Bytes::from(
            pairs
                .iter()
                .map(|(key, value)| format!("{key}={}", form_encode(value)))
                .collect::<Vec<_>>()
                .join("&"),
        )
    }
}

#[async_trait]
impl SessionSource for WebIdentity {
    async fn fetch(&self) -> ObjectResult<SessionCredentials> {
        let token = self.token()?;
        let request = HttpRequest {
            method: "POST",
            scheme: self.scheme,
            authority: self.authority.clone(),
            path: "/".to_string(),
            query: vec![],
            headers: vec![(
                "content-type".to_string(),
                "application/x-www-form-urlencoded".to_string(),
            )],
            // Deliberately unsigned: the OIDC assertion is the credential.
            body: self.body(&token),
        };

        let response = self.transport.execute(request).await.map_err(|failure| {
            let message = format!(
                "assuming {} with a web identity: {}",
                self.config.role_arn, failure.message
            );
            if failure.retryable {
                ObjectError::Transient(message)
            } else {
                ObjectError::Other(message)
            }
        })?;

        let text = String::from_utf8_lossy(&response.body);
        session_from_response(
            response.status,
            &text,
            &format!("assuming {} with a web identity", self.config.role_arn),
        )
    }

    fn describe(&self) -> String {
        format!(
            "sts:AssumeRoleWithWebIdentity of {} (IRSA)",
            self.config.role_arn
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aws::testing::ScriptedTransport;
    use orbita_objectstore::s3::{HttpResponse, TransportFailure};

    fn response(status: u16, body: &str) -> Result<HttpResponse, TransportFailure> {
        Ok(HttpResponse {
            status,
            headers: vec![],
            body: Bytes::from(body.to_string()),
        })
    }

    fn success_document() -> String {
        r#"<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult><Credentials>
            <AccessKeyId>ASIAIRSA</AccessKeyId>
            <SecretAccessKey>irsa-secret</SecretAccessKey>
            <SessionToken>irsa-session-token</SessionToken>
            <Expiration>2024-02-29T12:34:56Z</Expiration>
        </Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>"#
            .to_string()
    }

    /// A token file in a fresh temp directory, cleaned up by the OS the way
    /// every other temp file in this workspace's tests is.
    struct TokenFile {
        path: PathBuf,
    }

    impl TokenFile {
        fn with(contents: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "orbita-web-identity-{}-{:?}",
                std::process::id(),
                std::thread::current().id()
            ));
            std::fs::write(&path, contents).expect("writing a temp file");
            Self { path }
        }
    }

    impl Drop for TokenFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    fn config(token_file: PathBuf) -> WebIdentityConfig {
        WebIdentityConfig {
            token_file,
            role_arn: "arn:aws:iam::123456789012:role/orbita-workload".to_string(),
            session_name: "orbita-node-1".to_string(),
            endpoint: None,
            duration_seconds: 3_600,
        }
    }

    #[tokio::test]
    async fn a_projected_token_is_exchanged_for_a_session() {
        let file = TokenFile::with("the-projected-token\n");
        let transport = ScriptedTransport::new(vec![response(200, &success_document())]);
        let session = WebIdentity::new(transport.clone(), "us-east-1", config(file.path.clone()))
            .expect("valid configuration")
            .fetch()
            .await
            .expect("assumed");

        assert_eq!(session.credentials.access_key_id, "ASIAIRSA");
        assert_eq!(session.expires_at_millis, Some(1_709_210_096_000));

        let request = &transport.requests()[0];
        assert_eq!(request.authority, "sts.us-east-1.amazonaws.com");
        let body = String::from_utf8_lossy(&request.body).to_string();
        assert!(body.contains("Action=AssumeRoleWithWebIdentity"), "{body}");
        assert!(
            body.contains("WebIdentityToken=the-projected-token"),
            "the trailing newline kubelet writes must be trimmed: {body}"
        );
    }

    #[tokio::test]
    async fn the_exchange_is_unsigned_because_the_token_is_the_credential() {
        let file = TokenFile::with("the-projected-token");
        let transport = ScriptedTransport::new(vec![response(200, &success_document())]);
        WebIdentity::new(transport.clone(), "us-east-1", config(file.path.clone()))
            .expect("valid configuration")
            .fetch()
            .await
            .expect("assumed");

        assert!(
            transport.requests()[0].header("authorization").is_none(),
            "there is no base credential to sign this with, and STS does not want one"
        );
    }

    #[tokio::test]
    async fn the_token_is_reread_on_every_fetch() {
        let file = TokenFile::with("first-token");
        let transport = ScriptedTransport::new(vec![
            response(200, &success_document()),
            response(200, &success_document()),
        ]);
        let provider = WebIdentity::new(transport.clone(), "us-east-1", config(file.path.clone()))
            .expect("valid configuration");

        provider.fetch().await.expect("first");
        std::fs::write(&file.path, "rotated-token").expect("rotating");
        provider.fetch().await.expect("second");

        let bodies: Vec<String> = transport
            .requests()
            .iter()
            .map(|r| String::from_utf8_lossy(&r.body).to_string())
            .collect();
        assert!(
            bodies[1].contains("WebIdentityToken=rotated-token"),
            "kubelet rewrites this file in place; a cached token is an expired assertion: \
             {bodies:?}"
        );
    }

    #[tokio::test]
    async fn a_missing_token_file_is_reported_without_guessing() {
        let transport = ScriptedTransport::new(vec![]);
        let error = WebIdentity::new(
            transport.clone(),
            "us-east-1",
            config(PathBuf::from("/nonexistent/orbita/token")),
        )
        .expect("valid configuration")
        .fetch()
        .await
        .expect_err("no token, no session");

        assert!(matches!(error, ObjectError::AccessDenied(_)), "{error:?}");
        assert!(
            transport.requests().is_empty(),
            "there is nothing to present to STS"
        );
    }

    #[tokio::test]
    async fn a_rejected_assertion_is_reported_verbatim() {
        let file = TokenFile::with("stale-token");
        let transport = ScriptedTransport::new(vec![response(
            400,
            "<ErrorResponse><Error><Code>ExpiredTokenException</Code>\
             <Message>Token expired</Message></Error></ErrorResponse>",
        )]);
        let error = WebIdentity::new(transport, "us-east-1", config(file.path.clone()))
            .expect("valid configuration")
            .fetch()
            .await
            .expect_err("refused");

        match error {
            ObjectError::AccessDenied(message) => {
                assert!(message.contains("ExpiredTokenException"), "{message}");
                assert!(message.contains("orbita-workload"), "{message}");
            }
            other => panic!("expected AccessDenied, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_debug_logged_web_identity_request_does_not_carry_the_assertion() {
        let file = TokenFile::with("the-projected-token");
        let transport = ScriptedTransport::new(vec![response(200, &success_document())]);
        let session = WebIdentity::new(transport.clone(), "us-east-1", config(file.path.clone()))
            .expect("valid configuration")
            .fetch()
            .await
            .expect("assumed");

        let formatted = format!("{:?} {:?}", transport.requests(), session);
        assert!(
            !formatted.contains("the-projected-token") && !formatted.contains("irsa-secret"),
            "the OIDC assertion is the capability to assume the role, and it leaked: {formatted}"
        );
    }

    #[test]
    fn the_web_identity_endpoint_follows_the_region_partition() {
        let file = TokenFile::with("t");
        let provider = WebIdentity::new(
            ScriptedTransport::new(vec![]),
            "cn-north-1",
            config(file.path.clone()),
        )
        .expect("valid configuration");
        assert_eq!(provider.authority, "sts.cn-north-1.amazonaws.com.cn");
    }
}
