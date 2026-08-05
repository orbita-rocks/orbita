//! Instance-profile credentials from the EC2 instance metadata service.
//!
//! This is the keyless floor: an EC2 instance or an EKS node with an instance
//! profile attached can reach object storage with no Secret in the cluster at
//! all, which is the deployment a security review actually approves.
//!
//! # IMDSv2 only
//!
//! Every request carries a session token obtained with a `PUT`, and no
//! fallback to the unauthenticated v1 protocol is implemented. v1 is readable
//! by anything that can make the instance issue an HTTP request — an SSRF in
//! any process on the box — and a fallback path would mean that turning
//! `HttpTokens: required` on gains nothing, because a helpful client would
//! just try v1 first. An instance configured for v1 only fails here with a
//! message that says so.
//!
//! # Three requests
//!
//! Token, then role name, then the credential. The role name is refetched
//! every time rather than cached separately, because an instance profile can
//! be swapped underneath a running instance and a cached role name would keep
//! a node pointed at the profile it no longer has. Three round trips on the
//! link-local address every fifty-five minutes is not worth optimising.

use super::document::session_from_json;
use super::{SessionCredentials, SessionSource};

use async_trait::async_trait;
use bytes::Bytes;
use orbita_objectstore::s3::{HttpRequest, HttpResponse, HttpTransport, Scheme};
use orbita_objectstore::{ObjectError, ObjectResult};

use std::sync::Arc;

/// The link-local address every EC2 instance answers metadata on.
pub(crate) const IMDS_AUTHORITY: &str = "169.254.169.254";

/// How long the IMDSv2 session token is asked to live.
///
/// It is refetched on every credential refresh anyway, so this only has to
/// outlive three back-to-back requests. Six hours is the AWS maximum and is
/// what the SDKs ask for; asking for less would mean a token that can expire
/// between the `PUT` and the `GET` on a badly stalled instance.
const TOKEN_TTL_SECONDS: u32 = 21_600;

/// Sources credentials from the instance profile attached to this instance.
pub(crate) struct InstanceProfile {
    transport: Arc<dyn HttpTransport>,
    authority: String,
}

impl InstanceProfile {
    /// `authority` is the metadata endpoint, overridable so that a test, or a
    /// simulated run, can point this at something that is not a link-local
    /// address it has no business reaching.
    pub(crate) fn new(transport: Arc<dyn HttpTransport>, authority: impl Into<String>) -> Self {
        Self {
            transport,
            authority: authority.into(),
        }
    }

    async fn execute(&self, request: HttpRequest) -> ObjectResult<HttpResponse> {
        self.transport.execute(request).await.map_err(|failure| {
            if failure.retryable {
                ObjectError::Transient(format!(
                    "the instance metadata service: {}",
                    failure.message
                ))
            } else {
                ObjectError::Other(format!(
                    "the instance metadata service: {}",
                    failure.message
                ))
            }
        })
    }

    fn get(&self, path: &str, token: &str) -> HttpRequest {
        HttpRequest {
            method: "GET",
            // Plain HTTP: the metadata service is link-local, has no
            // certificate, and terminates inside the hypervisor.
            scheme: Scheme::Http,
            authority: self.authority.clone(),
            path: path.to_string(),
            query: vec![],
            headers: vec![("x-aws-ec2-metadata-token".to_string(), token.to_string())],
            body: Bytes::new(),
        }
    }

    async fn token(&self) -> ObjectResult<String> {
        let request = HttpRequest {
            method: "PUT",
            scheme: Scheme::Http,
            authority: self.authority.clone(),
            path: "/latest/api/token".to_string(),
            query: vec![],
            headers: vec![(
                "x-aws-ec2-metadata-token-ttl-seconds".to_string(),
                TOKEN_TTL_SECONDS.to_string(),
            )],
            body: Bytes::new(),
        };
        let response = self.execute(request).await?;
        if response.status != 200 {
            return Err(ObjectError::AccessDenied(format!(
                "the instance metadata service refused an IMDSv2 session token with status {}; \
                 an instance configured for IMDSv1 only cannot supply credentials to Orbita",
                response.status
            )));
        }
        let token = String::from_utf8_lossy(&response.body).trim().to_string();
        if token.is_empty() {
            return Err(ObjectError::Other(
                "the instance metadata service returned an empty IMDSv2 session token".to_string(),
            ));
        }
        Ok(token)
    }

    async fn role_name(&self, token: &str) -> ObjectResult<String> {
        let response = self
            .execute(self.get("/latest/meta-data/iam/security-credentials/", token))
            .await?;
        if response.status == 404 {
            return Err(ObjectError::AccessDenied(
                "this instance has no IAM instance profile attached, so it cannot source \
                 credentials from the instance metadata service"
                    .to_string(),
            ));
        }
        if response.status != 200 {
            return Err(status_error("listing instance profile roles", &response));
        }
        // The listing is one role per line; an instance can only have one
        // profile, so anything past the first line is not ours to interpret.
        String::from_utf8_lossy(&response.body)
            .lines()
            .map(str::trim)
            .find(|line| !line.is_empty())
            .map(str::to_string)
            .ok_or_else(|| {
                ObjectError::AccessDenied(
                    "the instance metadata service listed no instance profile role".to_string(),
                )
            })
    }
}

fn status_error(what: &str, response: &HttpResponse) -> ObjectError {
    let message = format!(
        "the instance metadata service answered status {} while {what}",
        response.status
    );
    match response.status {
        401 | 403 => ObjectError::AccessDenied(message),
        429 | 500..=599 => ObjectError::Transient(message),
        _ => ObjectError::Other(message),
    }
}

#[async_trait]
impl SessionSource for InstanceProfile {
    async fn fetch(&self) -> ObjectResult<SessionCredentials> {
        let token = self.token().await?;
        let role = self.role_name(&token).await?;
        let response = self
            .execute(self.get(
                &format!("/latest/meta-data/iam/security-credentials/{role}"),
                &token,
            ))
            .await?;
        if response.status != 200 {
            return Err(status_error(
                "reading instance profile credentials",
                &response,
            ));
        }

        session_from_json(&response.body, "the instance metadata service")
    }

    fn describe(&self) -> String {
        format!("the instance metadata service at {}", self.authority)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aws::testing::ScriptedTransport;
    use orbita_objectstore::s3::TransportFailure;

    fn ok(body: &str) -> Result<HttpResponse, TransportFailure> {
        Ok(HttpResponse {
            status: 200,
            headers: vec![],
            body: Bytes::from(body.to_string()),
        })
    }

    fn status(status: u16) -> Result<HttpResponse, TransportFailure> {
        Ok(HttpResponse {
            status,
            headers: vec![],
            body: Bytes::new(),
        })
    }

    const CREDENTIAL_DOCUMENT: &str = r#"{
        "Code": "Success",
        "LastUpdated": "2024-02-29T11:00:00Z",
        "Type": "AWS-HMAC",
        "AccessKeyId": "ASIAEXAMPLE",
        "SecretAccessKey": "the-secret",
        "Token": "the-session-token",
        "Expiration": "2024-02-29T12:34:56Z"
    }"#;

    fn happy_path() -> Arc<ScriptedTransport> {
        ScriptedTransport::new(vec![
            ok("the-imds-token"),
            ok("orbita-node-role"),
            ok(CREDENTIAL_DOCUMENT),
        ])
    }

    #[tokio::test]
    async fn instance_profile_credentials_carry_their_expiry() {
        let transport = happy_path();
        let session = InstanceProfile::new(transport, IMDS_AUTHORITY)
            .fetch()
            .await
            .expect("sourced");

        assert_eq!(session.credentials.access_key_id, "ASIAEXAMPLE");
        assert_eq!(
            session.credentials.session_token.as_deref(),
            Some("the-session-token")
        );
        assert_eq!(session.expires_at_millis, Some(1_709_210_096_000));
    }

    #[tokio::test]
    async fn every_metadata_request_carries_an_imdsv2_session_token() {
        let transport = happy_path();
        InstanceProfile::new(transport.clone(), IMDS_AUTHORITY)
            .fetch()
            .await
            .expect("sourced");

        let requests = transport.requests();
        assert_eq!(requests[0].method, "PUT");
        assert_eq!(requests[0].path, "/latest/api/token");
        for request in &requests[1..] {
            assert_eq!(
                request.header("x-aws-ec2-metadata-token"),
                Some("the-imds-token"),
                "an unauthenticated IMDSv1 read must never be issued: {request:?}"
            );
        }
    }

    #[tokio::test]
    async fn the_role_name_is_reread_on_every_refresh() {
        let transport = ScriptedTransport::new(vec![
            ok("token-1"),
            ok("first-role"),
            ok(CREDENTIAL_DOCUMENT),
            ok("token-2"),
            ok("second-role"),
            ok(CREDENTIAL_DOCUMENT),
        ]);
        let provider = InstanceProfile::new(transport.clone(), IMDS_AUTHORITY);
        provider.fetch().await.expect("first");
        provider.fetch().await.expect("second");

        let paths: Vec<String> = transport
            .requests()
            .iter()
            .map(|r| r.path.clone())
            .collect();
        assert!(
            paths.contains(&"/latest/meta-data/iam/security-credentials/first-role".to_string())
                && paths.contains(
                    &"/latest/meta-data/iam/security-credentials/second-role".to_string()
                ),
            "a profile swapped underneath the node must be picked up: {paths:?}"
        );
    }

    #[tokio::test]
    async fn an_imdsv1_only_instance_is_named_rather_than_worked_around() {
        let transport = ScriptedTransport::new(vec![status(403)]);
        let error = InstanceProfile::new(transport, IMDS_AUTHORITY)
            .fetch()
            .await
            .expect_err("no token, no credentials");
        match error {
            ObjectError::AccessDenied(message) => {
                assert!(message.contains("IMDSv1"), "{message}");
            }
            other => panic!("expected AccessDenied, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn an_instance_with_no_profile_says_so() {
        let transport = ScriptedTransport::new(vec![ok("the-imds-token"), status(404)]);
        let error = InstanceProfile::new(transport, IMDS_AUTHORITY)
            .fetch()
            .await
            .expect_err("no profile, no credentials");
        match error {
            ObjectError::AccessDenied(message) => {
                assert!(message.contains("instance profile"), "{message}");
            }
            other => panic!("expected AccessDenied, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn an_unreadable_expiry_is_refused_rather_than_assumed_eternal() {
        let transport = ScriptedTransport::new(vec![
            ok("the-imds-token"),
            ok("orbita-node-role"),
            ok(r#"{"AccessKeyId":"A","SecretAccessKey":"s","Token":"t","Expiration":"soon"}"#),
        ]);
        assert!(
            InstanceProfile::new(transport, IMDS_AUTHORITY)
                .fetch()
                .await
                .is_err(),
            "a credential with an unreadable deadline must not be treated as permanent"
        );
    }

    #[tokio::test]
    async fn a_metadata_service_timeout_is_retryable() {
        let transport = ScriptedTransport::new(vec![Err(TransportFailure {
            retryable: true,
            message: "timed out".to_string(),
        })]);
        let error = InstanceProfile::new(transport, IMDS_AUTHORITY)
            .fetch()
            .await
            .expect_err("no answer");
        assert!(error.is_retryable(), "{error:?}");
    }

    #[tokio::test]
    async fn a_debug_logged_metadata_request_does_not_carry_the_imdsv2_token() {
        let transport = happy_path();
        let provider = InstanceProfile::new(transport.clone(), IMDS_AUTHORITY);
        let session = provider.fetch().await.expect("sourced");

        let formatted = format!("{:?} {:?}", transport.requests(), session);
        assert!(
            !formatted.contains("the-imds-token"),
            "the IMDSv2 token is the capability to read a credential, and it leaked: {formatted}"
        );
        assert!(
            !formatted.contains("the-secret") && !formatted.contains("the-session-token"),
            "a credential leaked into Debug output: {formatted}"
        );
    }
}
