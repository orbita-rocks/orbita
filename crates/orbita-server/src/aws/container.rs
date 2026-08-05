//! Credentials from the container credential provider.
//!
//! This one endpoint covers two deployments that look nothing alike from the
//! outside:
//!
//! - **ECS and Fargate task roles.** The agent sets
//!   `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` and serves the credential from
//!   the task metadata address, `169.254.170.2`.
//! - **EKS Pod Identity.** The agent sets
//!   `AWS_CONTAINER_CREDENTIALS_FULL_URI` to `169.254.170.23` and requires a
//!   bearer token from `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`.
//!
//! Both answer the same JSON document IMDS does, which is why they share
//! [`super::document`].
//!
//! # Why it must come before IMDS
//!
//! In both deployments the *instance* also has a profile, and it is a
//! different principal with different permissions. A task that falls through to
//! the instance profile does not fail; it authenticates as the host. So this
//! source is tried first, exactly as the AWS SDKs order it, and a node that has
//! these variables set never reaches IMDS.
//!
//! # The loopback rule
//!
//! `AWS_CONTAINER_CREDENTIALS_FULL_URI` is an arbitrary URL out of the process
//! environment, and it is presented a bearer token. Anything that can set that
//! variable can therefore aim the token at a host it controls. AWS's own SDKs
//! restrict plain-HTTP full URIs to loopback and the two link-local agent
//! addresses; this does the same, because a credential-fetching client that
//! will POST its bearer token anywhere is an exfiltration primitive.

use super::document::session_from_json;
use super::{SessionCredentials, SessionSource};

use async_trait::async_trait;
use bytes::Bytes;
use orbita_objectstore::s3::{HttpRequest, HttpTransport, Scheme};
use orbita_objectstore::{ObjectError, ObjectResult};

use std::path::PathBuf;
use std::sync::Arc;

/// The ECS task metadata address, which `RELATIVE_URI` is resolved against.
pub(crate) const ECS_AUTHORITY: &str = "169.254.170.2";
/// The EKS Pod Identity agent's address. Named here only so the loopback rule
/// below can allow it.
pub(crate) const EKS_POD_IDENTITY_AUTHORITY: &str = "169.254.170.23";

pub(crate) const RELATIVE_URI_VARIABLE: &str = "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI";
pub(crate) const FULL_URI_VARIABLE: &str = "AWS_CONTAINER_CREDENTIALS_FULL_URI";
pub(crate) const TOKEN_VARIABLE: &str = "AWS_CONTAINER_AUTHORIZATION_TOKEN";
pub(crate) const TOKEN_FILE_VARIABLE: &str = "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE";

/// Where the container credential endpoint is and how to authenticate to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerCredentialsConfig {
    /// The full endpoint URL, already resolved from either the relative or the
    /// full form.
    pub uri: String,
    /// A bearer token supplied inline. EKS Pod Identity uses the file form
    /// instead, because the token rotates.
    pub authorization_token: Option<String>,
    /// A file holding the bearer token, re-read on every fetch.
    pub authorization_token_file: Option<PathBuf>,
}

impl ContainerCredentialsConfig {
    /// The configuration implied by `RELATIVE_URI`, which is the ECS form.
    #[must_use]
    pub fn relative(path: &str) -> Self {
        let path = if path.starts_with('/') {
            path.to_string()
        } else {
            format!("/{path}")
        };
        Self {
            uri: format!("http://{ECS_AUTHORITY}{path}"),
            authorization_token: None,
            authorization_token_file: None,
        }
    }
}

/// Reads a credential from the container credential provider.
pub(crate) struct ContainerCredentials {
    transport: Arc<dyn HttpTransport>,
    scheme: Scheme,
    authority: String,
    path: String,
    config: ContainerCredentialsConfig,
}

impl ContainerCredentials {
    pub(crate) fn new(
        transport: Arc<dyn HttpTransport>,
        config: ContainerCredentialsConfig,
    ) -> ObjectResult<Self> {
        let (scheme, rest) = if let Some(rest) = config.uri.strip_prefix("https://") {
            (Scheme::Https, rest)
        } else if let Some(rest) = config.uri.strip_prefix("http://") {
            (Scheme::Http, rest)
        } else {
            return Err(ObjectError::Other(format!(
                "{FULL_URI_VARIABLE} must start with http:// or https://, got {:?}",
                config.uri
            )));
        };
        let (authority, path) = match rest.find('/') {
            Some(index) => (rest[..index].to_string(), rest[index..].to_string()),
            None => (rest.to_string(), "/".to_string()),
        };
        if authority.is_empty() {
            return Err(ObjectError::Other(format!(
                "{FULL_URI_VARIABLE} has no host: {:?}",
                config.uri
            )));
        }
        if scheme == Scheme::Http && !is_trusted_plaintext_host(&authority) {
            return Err(ObjectError::AccessDenied(format!(
                "{FULL_URI_VARIABLE} points at {authority} over plain HTTP; only loopback, \
                 {ECS_AUTHORITY}, and {EKS_POD_IDENTITY_AUTHORITY} are trusted with a bearer \
                 token in the clear"
            )));
        }
        Ok(Self {
            transport,
            scheme,
            authority,
            path,
            config,
        })
    }

    /// The bearer token, from the file when there is one.
    ///
    /// The file wins over the inline value because that is the form EKS Pod
    /// Identity uses and the one that rotates; a deployment that set both meant
    /// the rotating one.
    fn authorization(&self) -> ObjectResult<Option<String>> {
        if let Some(path) = &self.config.authorization_token_file {
            let raw = std::fs::read_to_string(path).map_err(|error| {
                ObjectError::AccessDenied(format!(
                    "reading the container authorization token from {}: {error}",
                    path.display()
                ))
            })?;
            return Ok(Some(raw.trim().to_string()));
        }
        Ok(self.config.authorization_token.clone())
    }
}

/// Whether a plaintext endpoint is one AWS's own clients will hand a token to.
fn is_trusted_plaintext_host(authority: &str) -> bool {
    let host = host_of(authority);
    host == ECS_AUTHORITY
        || host == EKS_POD_IDENTITY_AUTHORITY
        || host == "localhost"
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// The host out of `host[:port]`.
///
/// An IPv6 literal is bracketed and full of colons, so the port cannot be
/// found by splitting on the last one: `[::1]` would come back as `[::`, which
/// parses as no address at all and would quietly fail the loopback check.
fn host_of(authority: &str) -> &str {
    if let Some(rest) = authority.strip_prefix('[') {
        return rest.split(']').next().unwrap_or(rest);
    }
    authority
        .rsplit_once(':')
        .map_or(authority, |(host, _)| host)
}

#[async_trait]
impl SessionSource for ContainerCredentials {
    async fn fetch(&self) -> ObjectResult<SessionCredentials> {
        let mut headers = vec![("accept".to_string(), "application/json".to_string())];
        if let Some(token) = self.authorization()? {
            headers.push(("authorization".to_string(), token));
        }
        let request = HttpRequest {
            method: "GET",
            scheme: self.scheme,
            authority: self.authority.clone(),
            path: self.path.clone(),
            query: vec![],
            headers,
            body: Bytes::new(),
        };

        let response = self.transport.execute(request).await.map_err(|failure| {
            let message = format!("the container credential provider: {}", failure.message);
            if failure.retryable {
                ObjectError::Transient(message)
            } else {
                ObjectError::Other(message)
            }
        })?;

        if response.status != 200 {
            let message = format!(
                "the container credential provider answered status {}",
                response.status
            );
            return Err(match response.status {
                401 | 403 => ObjectError::AccessDenied(message),
                429 | 500..=599 => ObjectError::Transient(message),
                _ => ObjectError::Other(message),
            });
        }

        session_from_json(&response.body, "the container credential provider")
    }

    fn describe(&self) -> String {
        format!(
            "the container credential provider at {}{}",
            self.authority, self.path
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aws::testing::ScriptedTransport;
    use orbita_objectstore::s3::{HttpResponse, TransportFailure};

    const DOCUMENT: &str = r#"{
        "AccessKeyId": "ASIATASK",
        "SecretAccessKey": "the-task-secret",
        "Token": "the-task-session-token",
        "Expiration": "2024-02-29T12:34:56Z"
    }"#;

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

    #[tokio::test]
    async fn an_ecs_relative_uri_resolves_against_the_task_metadata_address() {
        let transport = ScriptedTransport::new(vec![ok(DOCUMENT)]);
        let session = ContainerCredentials::new(
            transport.clone(),
            ContainerCredentialsConfig::relative("/v2/credentials/abcd-1234"),
        )
        .expect("valid configuration")
        .fetch()
        .await
        .expect("sourced");

        assert_eq!(session.credentials.access_key_id, "ASIATASK");
        let request = &transport.requests()[0];
        assert_eq!(request.authority, ECS_AUTHORITY);
        assert_eq!(request.path, "/v2/credentials/abcd-1234");
        assert!(
            request.header("authorization").is_none(),
            "ECS does not issue a bearer token and sending an empty one is worse than sending none"
        );
    }

    #[tokio::test]
    async fn an_eks_pod_identity_request_carries_the_bearer_token_from_its_file() {
        let path = std::env::temp_dir().join(format!(
            "orbita-container-token-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::write(&path, "the-pod-identity-token\n").expect("writing a temp file");

        let transport = ScriptedTransport::new(vec![ok(DOCUMENT)]);
        ContainerCredentials::new(
            transport.clone(),
            ContainerCredentialsConfig {
                uri: format!("http://{EKS_POD_IDENTITY_AUTHORITY}/v1/credentials"),
                authorization_token: None,
                authorization_token_file: Some(path.clone()),
            },
        )
        .expect("valid configuration")
        .fetch()
        .await
        .expect("sourced");

        assert_eq!(
            transport.requests()[0].header("authorization"),
            Some("the-pod-identity-token"),
            "the trailing newline the agent writes must be trimmed"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn a_plaintext_endpoint_off_loopback_is_refused_rather_than_handed_a_token() {
        let error = ContainerCredentials::new(
            ScriptedTransport::new(vec![]),
            ContainerCredentialsConfig {
                uri: "http://attacker.example.com/creds".to_string(),
                authorization_token: Some("the-token".to_string()),
                authorization_token_file: None,
            },
        )
        .err()
        .expect("a bearer token must not leave the host in the clear");
        assert!(matches!(error, ObjectError::AccessDenied(_)), "{error:?}");
    }

    #[tokio::test]
    async fn loopback_and_the_agent_addresses_are_trusted() {
        for uri in [
            "http://127.0.0.1:8080/creds",
            "http://localhost/creds",
            "http://[::1]/creds",
            "http://[::1]:8080/creds",
            "http://169.254.170.2/v2/credentials/x",
            "http://169.254.170.23/v1/credentials",
            // TLS anywhere is fine: the token is not in the clear.
            "https://credentials.internal.example.com/creds",
        ] {
            assert!(
                ContainerCredentials::new(
                    ScriptedTransport::new(vec![]),
                    ContainerCredentialsConfig {
                        uri: uri.to_string(),
                        authorization_token: Some("t".to_string()),
                        authorization_token_file: None,
                    },
                )
                .is_ok(),
                "{uri} should be trusted"
            );
        }
    }

    #[tokio::test]
    async fn a_refused_credential_read_is_not_retryable_but_a_throttle_is() {
        let denied = ContainerCredentials::new(
            ScriptedTransport::new(vec![status(403)]),
            ContainerCredentialsConfig::relative("/v2/credentials/x"),
        )
        .expect("valid configuration")
        .fetch()
        .await
        .expect_err("refused");
        assert!(!denied.is_retryable(), "{denied:?}");

        let throttled = ContainerCredentials::new(
            ScriptedTransport::new(vec![status(429)]),
            ContainerCredentialsConfig::relative("/v2/credentials/x"),
        )
        .expect("valid configuration")
        .fetch()
        .await
        .expect_err("throttled");
        assert!(throttled.is_retryable(), "{throttled:?}");
    }

    #[tokio::test]
    async fn a_debug_logged_container_request_does_not_carry_the_bearer_token() {
        let transport = ScriptedTransport::new(vec![ok(DOCUMENT)]);
        let session = ContainerCredentials::new(
            transport.clone(),
            ContainerCredentialsConfig {
                uri: "http://169.254.170.23/v1/credentials".to_string(),
                authorization_token: Some("the-pod-identity-token".to_string()),
                authorization_token_file: None,
            },
        )
        .expect("valid configuration")
        .fetch()
        .await
        .expect("sourced");

        let formatted = format!("{:?} {:?}", transport.requests(), session);
        assert!(
            !formatted.contains("the-pod-identity-token")
                && !formatted.contains("the-task-secret")
                && !formatted.contains("the-task-session-token"),
            "a credential leaked into Debug output: {formatted}"
        );
    }
}
