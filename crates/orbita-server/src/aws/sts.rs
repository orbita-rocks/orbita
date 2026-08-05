//! Session credentials from `sts:AssumeRole`.
//!
//! This is the preferred AWS deployment. The node's own identity — an instance
//! profile, or in a pinch a static key — is only permitted to assume a role,
//! and the role is what actually carries the bucket policy. That separation is
//! what lets an operator revoke or re-scope Orbita's storage access without
//! touching the instances, and what makes an external id meaningful in a
//! cross-account setup.
//!
//! # It layers on anything
//!
//! The base is an [`Arc<dyn CredentialsProvider>`], so this works over the
//! instance profile (the intended shape) or over static keys (which is how a
//! deployment that already has a bootstrap key narrows its blast radius).
//! Underneath, the base is usually itself refreshing, so an expiring
//! instance-profile credential and an expiring assumed-role session refresh
//! independently and neither has to know about the other.
//!
//! # Why the query protocol
//!
//! `AssumeRole` is called over the old form-encoded query API rather than the
//! JSON one because it is what SigV4 signs most simply — one POST, one body,
//! no service-specific headers — and because the response is a fixed XML shape
//! that has not changed since 2011. Adding a JSON codec here would buy
//! nothing.

use super::{partition, SessionCredentials, SessionSource};
use crate::aws::timestamp::parse_rfc3339_millis;

use async_trait::async_trait;
use bytes::Bytes;
use orbita_objectstore::s3::{
    sign_request, Credentials, CredentialsProvider, HttpRequest, HttpTransport, NowMillis, Scheme,
};
use orbita_objectstore::{ObjectError, ObjectResult};

use std::sync::Arc;

/// The `AssumeRole` API version this code speaks. It is a constant of the
/// protocol, not a thing to configure.
pub(crate) const STS_API_VERSION: &str = "2011-06-15";

/// How long a session is requested for.
///
/// One hour is the default ceiling for a role whose maximum session duration
/// has not been raised, so asking for it works everywhere; asking for more
/// would fail on an ordinary role with an error most operators would read as a
/// permissions problem.
pub const DEFAULT_SESSION_DURATION_SECONDS: u32 = 3_600;

/// What role to assume and how to identify the session.
#[derive(Debug, Clone)]
pub struct AssumeRoleConfig {
    /// The role to assume, as a full ARN.
    pub role_arn: String,
    /// The name the session appears under in CloudTrail. It is not a secret
    /// and it is the only thing that tells two nodes apart in an audit log, so
    /// it should carry the node identity.
    pub session_name: String,
    /// The shared secret a third party's role trust policy can require. It
    /// exists to stop the confused-deputy problem in cross-account access, so
    /// it is treated as a secret even though AWS does not call it one.
    pub external_id: Option<String>,
    /// How long each session lasts, in seconds. It has to be within the role's
    /// own maximum session duration.
    pub duration_seconds: u32,
    /// Overrides the regional STS endpoint. Set this only for a private
    /// endpoint or a test double; the default is derived from the region.
    pub endpoint: Option<String>,
}

impl AssumeRoleConfig {
    /// A configuration with the defaults filled in, which is everything except
    /// the role itself.
    #[must_use]
    pub fn new(role_arn: impl Into<String>, session_name: impl Into<String>) -> Self {
        Self {
            role_arn: role_arn.into(),
            session_name: session_name.into(),
            external_id: None,
            duration_seconds: DEFAULT_SESSION_DURATION_SECONDS,
            endpoint: None,
        }
    }
}

/// The regional STS endpoint for `region`.
///
/// Regional rather than the global `sts.amazonaws.com` because a global
/// endpoint is a cross-region dependency: a node in `eu-west-1` should not
/// lose its credentials because `us-east-1` is having a day.
///
/// The DNS suffix comes from the region's partition rather than being spelled
/// `amazonaws.com` here, because that name does not resolve in China and does
/// not exist at all in the isolated partitions. See [`super::partition`].
pub(crate) fn default_endpoint(region: &str) -> String {
    format!("https://sts.{region}.{}", partition::dns_suffix(region))
}

/// Exchanges a base credential for a session on an assumed role.
pub(crate) struct AssumeRole {
    base: Arc<dyn CredentialsProvider>,
    transport: Arc<dyn HttpTransport>,
    now_millis: NowMillis,
    region: String,
    authority: String,
    scheme: Scheme,
    config: AssumeRoleConfig,
}

impl AssumeRole {
    pub(crate) fn new(
        base: Arc<dyn CredentialsProvider>,
        transport: Arc<dyn HttpTransport>,
        now_millis: NowMillis,
        region: impl Into<String>,
        config: AssumeRoleConfig,
    ) -> ObjectResult<Self> {
        let region = region.into();
        let endpoint = config
            .endpoint
            .clone()
            .unwrap_or_else(|| default_endpoint(&region));
        let (scheme, authority) = split_endpoint(&endpoint)?;
        if config.role_arn.is_empty() {
            return Err(ObjectError::Other(
                "assuming a role needs a role ARN".to_string(),
            ));
        }
        Ok(Self {
            base,
            transport,
            now_millis,
            region,
            authority,
            scheme,
            config,
        })
    }

    fn body(&self) -> Bytes {
        let mut pairs = vec![
            ("Action", "AssumeRole".to_string()),
            ("Version", STS_API_VERSION.to_string()),
            ("RoleArn", self.config.role_arn.clone()),
            ("RoleSessionName", self.config.session_name.clone()),
            ("DurationSeconds", self.config.duration_seconds.to_string()),
        ];
        if let Some(external_id) = &self.config.external_id {
            pairs.push(("ExternalId", external_id.clone()));
        }
        let encoded = pairs
            .iter()
            .map(|(key, value)| format!("{key}={}", form_encode(value)))
            .collect::<Vec<_>>()
            .join("&");
        Bytes::from(encoded)
    }
}

/// Percent-encodes one form value.
///
/// `application/x-www-form-urlencoded` with the RFC 3986 unreserved set left
/// bare, which is what SigV4 also wants; a role ARN is full of colons and
/// slashes and both have to survive as escapes rather than as separators.
/// Space becomes `%20` and not `+`, because the signature covers the body
/// bytes and STS accepts either.
pub(crate) fn form_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

pub(crate) fn split_endpoint(endpoint: &str) -> ObjectResult<(Scheme, String)> {
    let (scheme, rest) = if let Some(rest) = endpoint.strip_prefix("https://") {
        (Scheme::Https, rest)
    } else if let Some(rest) = endpoint.strip_prefix("http://") {
        (Scheme::Http, rest)
    } else {
        return Err(ObjectError::Other(format!(
            "the STS endpoint {endpoint:?} must start with http:// or https://"
        )));
    };
    let authority = rest.trim_end_matches('/');
    if authority.is_empty() || authority.contains('/') {
        return Err(ObjectError::Other(format!(
            "the STS endpoint {endpoint:?} must be scheme://host[:port] with no path"
        )));
    }
    Ok((scheme, authority.to_string()))
}

/// Pulls `<tag>value</tag>` out of a response without a full parse. The
/// `AssumeRole` response is a fixed, tiny document and its shape has not moved
/// since 2011, which is the same reason `orbita-objectstore` reads S3 error
/// bodies this way.
pub(crate) fn xml_field(body: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = body.find(&open)? + open.len();
    let end = body[start..].find(&close)? + start;
    Some(body[start..end].to_string())
}

/// Turns one STS response into a session credential, or into the error the
/// operator needs to read.
///
/// `AssumeRole` and `AssumeRoleWithWebIdentity` return the same `<Credentials>`
/// element and the same `<Error>` element, so they share this rather than
/// having two copies that can drift on which status codes are retryable.
/// `what` names the operation for the message: it is a role ARN or an audience,
/// never a credential.
pub(crate) fn session_from_response(
    status: u16,
    text: &str,
    what: &str,
) -> ObjectResult<SessionCredentials> {
    if status != 200 {
        // The STS error code and message name the actual problem — a trust
        // policy that does not allow this principal, a missing external
        // id, an expired web identity token — and an operator needs them
        // verbatim. Neither is a secret.
        let detail = match (xml_field(text, "Code"), xml_field(text, "Message")) {
            (Some(code), Some(message)) => format!("{code}: {message}"),
            (Some(code), None) => code,
            _ => format!("status {status}"),
        };
        let message = format!("{what}: {detail}");
        return Err(match status {
            400 | 401 | 403 => ObjectError::AccessDenied(message),
            429 | 500..=599 => ObjectError::Transient(message),
            _ => ObjectError::Other(message),
        });
    }

    let field = |tag: &str| {
        xml_field(text, tag)
            .ok_or_else(|| ObjectError::Other(format!("{what}: the response had no <{tag}>")))
    };
    let expiration = field("Expiration")?;
    let expires_at_millis = parse_rfc3339_millis(&expiration).ok_or_else(|| {
        ObjectError::Other(format!(
            "{what}: the session expiry {expiration:?} is unreadable; refusing to guess how \
             long the session lasts"
        ))
    })?;

    Ok(SessionCredentials {
        credentials: Credentials {
            access_key_id: field("AccessKeyId")?,
            secret_access_key: field("SecretAccessKey")?,
            session_token: Some(field("SessionToken")?),
        },
        expires_at_millis: Some(expires_at_millis),
    })
}

#[async_trait]
impl SessionSource for AssumeRole {
    async fn fetch(&self) -> ObjectResult<SessionCredentials> {
        let base = self.base.credentials().await?;
        let body = self.body();
        let mut request = HttpRequest {
            method: "POST",
            scheme: self.scheme,
            authority: self.authority.clone(),
            path: "/".to_string(),
            query: vec![],
            headers: vec![(
                "content-type".to_string(),
                "application/x-www-form-urlencoded".to_string(),
            )],
            body,
        };
        sign_request(
            &mut request,
            &base,
            &self.region,
            "sts",
            (self.now_millis)(),
        );

        let response = self.transport.execute(request).await.map_err(|failure| {
            let message = format!("assuming {}: {}", self.config.role_arn, failure.message);
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
            &format!("assuming {}", self.config.role_arn),
        )
    }

    fn describe(&self) -> String {
        format!("sts:AssumeRole of {}", self.config.role_arn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aws::testing::ScriptedTransport;
    use orbita_objectstore::s3::{HttpResponse, StaticCredentials, TransportFailure};

    const SIGNING_TIME: u64 = 1_369_353_600_000;

    fn base() -> Arc<dyn CredentialsProvider> {
        StaticCredentials::shared(Credentials {
            access_key_id: "AKIABASE".to_string(),
            secret_access_key: "base-secret".to_string(),
            session_token: None,
        })
    }

    fn response(status: u16, body: &str) -> Result<HttpResponse, TransportFailure> {
        Ok(HttpResponse {
            status,
            headers: vec![],
            body: Bytes::from(body.to_string()),
        })
    }

    fn success_document() -> String {
        r#"<AssumeRoleResponse><AssumeRoleResult><Credentials>
            <AccessKeyId>ASIAASSUMED</AccessKeyId>
            <SecretAccessKey>assumed-secret</SecretAccessKey>
            <SessionToken>assumed-session-token</SessionToken>
            <Expiration>2024-02-29T12:34:56Z</Expiration>
        </Credentials></AssumeRoleResult></AssumeRoleResponse>"#
            .to_string()
    }

    fn provider(transport: Arc<ScriptedTransport>, config: AssumeRoleConfig) -> AssumeRole {
        AssumeRole::new(
            base(),
            transport,
            Arc::new(|| SIGNING_TIME),
            "us-east-1",
            config,
        )
        .expect("valid configuration")
    }

    fn config() -> AssumeRoleConfig {
        AssumeRoleConfig::new("arn:aws:iam::123456789012:role/orbita", "orbita-node-1")
    }

    #[tokio::test]
    async fn an_assumed_session_carries_its_token_and_expiry() {
        let transport = ScriptedTransport::new(vec![response(200, &success_document())]);
        let session = provider(transport, config())
            .fetch()
            .await
            .expect("assumed");

        assert_eq!(session.credentials.access_key_id, "ASIAASSUMED");
        assert_eq!(
            session.credentials.session_token.as_deref(),
            Some("assumed-session-token")
        );
        assert_eq!(session.expires_at_millis, Some(1_709_210_096_000));
    }

    #[tokio::test]
    async fn the_assume_role_call_is_signed_against_sts_with_the_base_credential() {
        let transport = ScriptedTransport::new(vec![response(200, &success_document())]);
        provider(transport.clone(), config())
            .fetch()
            .await
            .expect("assumed");

        let request = &transport.requests()[0];
        let authorization = request.header("authorization").expect("signed");
        assert!(
            authorization.contains("Credential=AKIABASE/") && authorization.contains("/sts/"),
            "the base credential must sign the exchange, scoped to sts: {authorization}"
        );
        assert_eq!(request.authority, "sts.us-east-1.amazonaws.com");
    }

    #[tokio::test]
    async fn the_external_id_is_sent_only_when_it_is_configured() {
        let transport = ScriptedTransport::new(vec![response(200, &success_document())]);
        provider(transport.clone(), config())
            .fetch()
            .await
            .expect("assumed");
        let without = String::from_utf8_lossy(&transport.requests()[0].body).to_string();
        assert!(!without.contains("ExternalId"), "{without}");

        let mut with_external = config();
        with_external.external_id = Some("shared/secret".to_string());
        let transport = ScriptedTransport::new(vec![response(200, &success_document())]);
        provider(transport.clone(), with_external)
            .fetch()
            .await
            .expect("assumed");
        let with = String::from_utf8_lossy(&transport.requests()[0].body).to_string();
        assert!(
            with.contains("ExternalId=shared%2Fsecret"),
            "the external id must be form-encoded, not sent raw: {with}"
        );
    }

    #[tokio::test]
    async fn the_role_arn_survives_form_encoding() {
        let transport = ScriptedTransport::new(vec![response(200, &success_document())]);
        provider(transport.clone(), config())
            .fetch()
            .await
            .expect("assumed");
        let body = String::from_utf8_lossy(&transport.requests()[0].body).to_string();
        assert!(
            body.contains("RoleArn=arn%3Aaws%3Aiam%3A%3A123456789012%3Arole%2Forbita"),
            "an unescaped ARN would end the parameter at its first colon: {body}"
        );
    }

    #[tokio::test]
    async fn a_refused_trust_policy_is_reported_verbatim() {
        let transport = ScriptedTransport::new(vec![response(
            403,
            "<ErrorResponse><Error><Code>AccessDenied</Code>\
             <Message>User is not authorized to perform: sts:AssumeRole</Message>\
             </Error></ErrorResponse>",
        )]);
        let error = provider(transport, config())
            .fetch()
            .await
            .expect_err("refused");
        match error {
            ObjectError::AccessDenied(message) => {
                assert!(message.contains("not authorized"), "{message}");
                assert!(message.contains("role/orbita"), "{message}");
            }
            other => panic!("expected AccessDenied, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_throttled_assume_role_is_retryable() {
        let transport = ScriptedTransport::new(vec![response(
            429,
            "<ErrorResponse><Error><Code>Throttling</Code></Error></ErrorResponse>",
        )]);
        let error = provider(transport, config())
            .fetch()
            .await
            .expect_err("throttled");
        assert!(error.is_retryable(), "{error:?}");
    }

    #[tokio::test]
    async fn assume_role_layers_over_a_failing_base_without_calling_sts() {
        struct NoBase;

        #[async_trait]
        impl CredentialsProvider for NoBase {
            async fn credentials(&self) -> ObjectResult<Credentials> {
                Err(ObjectError::AccessDenied("no instance profile".to_string()))
            }
        }

        let transport = ScriptedTransport::new(vec![]);
        let assume = AssumeRole::new(
            Arc::new(NoBase),
            transport.clone(),
            Arc::new(|| SIGNING_TIME),
            "us-east-1",
            config(),
        )
        .expect("valid configuration");

        assert!(assume.fetch().await.is_err());
        assert!(
            transport.requests().is_empty(),
            "there is nothing to sign an unauthenticated AssumeRole with"
        );
    }

    #[tokio::test]
    async fn a_debug_logged_assume_role_request_does_not_carry_the_signature() {
        let transport = ScriptedTransport::new(vec![response(200, &success_document())]);
        let session = provider(transport.clone(), config())
            .fetch()
            .await
            .expect("assumed");
        let formatted = format!("{:?} {:?}", transport.requests(), session);
        assert!(
            !formatted.contains("Signature=") && !formatted.contains("assumed-secret"),
            "a replayable credential leaked into Debug output: {formatted}"
        );
    }

    #[test]
    fn the_sts_endpoint_is_regional_by_default() {
        assert_eq!(
            default_endpoint("eu-west-1"),
            "https://sts.eu-west-1.amazonaws.com"
        );
    }

    #[test]
    fn the_sts_endpoint_follows_the_region_partition() {
        // `sts.cn-north-1.amazonaws.com` does not resolve. Building it would
        // turn every credential refresh in China into a DNS failure that looks
        // like a network problem and is not one.
        assert_eq!(
            default_endpoint("cn-north-1"),
            "https://sts.cn-north-1.amazonaws.com.cn"
        );
        assert_eq!(
            default_endpoint("us-gov-west-1"),
            "https://sts.us-gov-west-1.amazonaws.com"
        );
        assert_eq!(
            default_endpoint("us-iso-east-1"),
            "https://sts.us-iso-east-1.c2s.ic.gov"
        );
    }

    #[tokio::test]
    async fn a_configured_sts_endpoint_overrides_the_derived_one() {
        let mut config = config();
        config.endpoint = Some("https://vpce-1234.sts.us-east-1.vpce.amazonaws.com".to_string());
        let transport = ScriptedTransport::new(vec![response(200, &success_document())]);
        provider(transport.clone(), config)
            .fetch()
            .await
            .expect("assumed");
        assert_eq!(
            transport.requests()[0].authority,
            "vpce-1234.sts.us-east-1.vpce.amazonaws.com",
            "a PrivateLink or isolated-partition endpoint has to be settable by hand"
        );
    }

    #[test]
    fn a_role_arn_is_required() {
        let mut config = config();
        config.role_arn = String::new();
        assert!(AssumeRole::new(
            base(),
            ScriptedTransport::new(vec![]),
            Arc::new(|| SIGNING_TIME),
            "us-east-1",
            config,
        )
        .is_err());
    }
}
