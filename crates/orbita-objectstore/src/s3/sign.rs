//! AWS Signature Version 4, request signing only.
//!
//! This is written out by hand rather than pulled from an SDK because the
//! store needs exactly one signing mode — headers, single-chunk payload,
//! service `s3` — and the SDK's version of it arrives welded to its own HTTP
//! client, retry policy, and clock, which is precisely what a deterministic
//! simulator cannot allow. The algorithm is fixed and published, and a unit
//! test pins this implementation to AWS's own worked example.
//!
//! Reference: "Authenticating Requests (AWS Signature Version 4)" in the
//! Amazon S3 API documentation.

use super::http::{HttpRequest, STRICT_ENCODE};
use super::time::amz_timestamp;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

/// Static credentials, as S3-compatible stores hand them out.
#[derive(Clone)]
pub struct Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    /// Present when the credentials came from STS; sent as
    /// `x-amz-security-token` and included in the signature.
    pub session_token: Option<String>,
}

/// Manual so a `{:?}` of a config, an error context, or a panic message never
/// writes the live secret to a log. The access key id is an identifier, not a
/// secret, so it stays; it is what an operator needs to tell two credential
/// sets apart.
impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// Signs `request` in place: adds `host`, `x-amz-date`,
/// `x-amz-content-sha256`, the session token if there is one, and the
/// `authorization` header over all of them.
///
/// `now_millis` is Unix wall time. It is a parameter rather than a call to
/// `SystemTime::now()` so that a simulated run signs deterministically; the
/// production constructor supplies the system clock.
pub(crate) fn sign(
    request: &mut HttpRequest,
    credentials: &Credentials,
    region: &str,
    now_millis: u64,
) {
    let (date, stamp) = amz_timestamp(now_millis);
    let payload_hash = hex(&Sha256::digest(&request.body));

    if request.header("host").is_none() {
        request
            .headers
            .push(("host".to_string(), request.authority.clone()));
    }
    request
        .headers
        .push(("x-amz-date".to_string(), stamp.clone()));
    request
        .headers
        .push(("x-amz-content-sha256".to_string(), payload_hash.clone()));
    if let Some(token) = &credentials.session_token {
        request
            .headers
            .push(("x-amz-security-token".to_string(), token.clone()));
    }

    // Canonical headers: lowercase names, trimmed values with internal runs of
    // spaces collapsed, sorted by name. Every header on the request is signed;
    // signing a subset invites a proxy to vary the rest.
    let mut canonical: Vec<(String, String)> = request
        .headers
        .iter()
        .map(|(name, value)| (name.to_ascii_lowercase(), collapse_spaces(value.trim())))
        .collect();
    canonical.sort();
    let signed_headers = canonical
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_headers: String = canonical
        .iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect();

    let canonical_request = format!(
        "{}\n{}\n{}\n{canonical_headers}\n{signed_headers}\n{payload_hash}",
        request.method,
        request.path,
        request.canonical_query(),
    );

    let scope = format!("{date}/{region}/s3/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{stamp}\n{scope}\n{}",
        hex(&Sha256::digest(canonical_request.as_bytes()))
    );

    let mut key = hmac_sha256(
        format!("AWS4{}", credentials.secret_access_key).as_bytes(),
        date.as_bytes(),
    );
    for part in [region, "s3", "aws4_request"] {
        key = hmac_sha256(&key, part.as_bytes());
    }
    let signature = hex(&hmac_sha256(&key, string_to_sign.as_bytes()));

    request.headers.push((
        "authorization".to_string(),
        format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, \
             Signature={signature}",
            credentials.access_key_id
        ),
    ));
}

/// Percent-encodes one path segment the way the canonical URI wants it.
pub(crate) fn encode_path_segment(segment: &str) -> String {
    percent_encoding::percent_encode(segment.as_bytes(), STRICT_ENCODE).to_string()
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    // HMAC accepts keys of any length, so this cannot fail.
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn collapse_spaces(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut last_was_space = false;
    for c in value.chars() {
        if c == ' ' {
            if !last_was_space {
                out.push(' ');
            }
            last_was_space = true;
        } else {
            out.push(c);
            last_was_space = false;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::http::Scheme;
    use bytes::Bytes;

    /// AWS's published example credentials, from the SigV4 test vectors.
    fn example_credentials() -> Credentials {
        Credentials {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRFiCYEXAMPLEKEY".to_string(),
            session_token: None,
        }
    }

    /// The worked "GET Object" example from the Amazon S3 SigV4
    /// documentation: GET /test.txt from examplebucket with a Range header,
    /// signed at 2013-05-24T00:00:00Z in us-east-1. The canonical request and
    /// its hash (7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc439649\
    /// 46972) match the documentation, and the final signature below was
    /// cross-checked against botocore's `S3SigV4Auth` over the identical
    /// request, so a mismatch here is a bug in this file rather than a
    /// disagreement with a server.
    #[test]
    fn signing_matches_the_published_aws_example() {
        let mut request = HttpRequest {
            method: "GET",
            scheme: Scheme::Https,
            authority: "examplebucket.s3.amazonaws.com".to_string(),
            path: "/test.txt".to_string(),
            query: vec![],
            headers: vec![("range".to_string(), "bytes=0-9".to_string())],
            body: Bytes::new(),
        };
        sign(
            &mut request,
            &example_credentials(),
            "us-east-1",
            1_369_353_600_000,
        );

        let authorization = request.header("authorization").expect("signed");
        assert_eq!(
            authorization,
            "AWS4-HMAC-SHA256 \
             Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
             SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
             Signature=51a4f6aa2b6678b8bc64339c22721571d2a069f12f5ec80b1e6d12e50636512a"
        );
    }

    #[test]
    fn a_session_token_is_signed_when_present() {
        let mut credentials = example_credentials();
        credentials.session_token = Some("the-token".to_string());
        let mut request = HttpRequest {
            method: "GET",
            scheme: Scheme::Https,
            authority: "examplebucket.s3.amazonaws.com".to_string(),
            path: "/".to_string(),
            query: vec![],
            headers: vec![],
            body: Bytes::new(),
        };
        sign(&mut request, &credentials, "us-east-1", 1_369_353_600_000);

        assert_eq!(request.header("x-amz-security-token"), Some("the-token"));
        let authorization = request.header("authorization").expect("signed");
        assert!(
            authorization.contains("x-amz-security-token"),
            "the token header must be signed, not merely sent: {authorization}"
        );
    }

    #[test]
    fn a_debug_logged_signed_request_is_not_replayable() {
        let mut credentials = example_credentials();
        credentials.session_token = Some("the-session-token".to_string());
        let mut request = HttpRequest {
            method: "PUT",
            scheme: Scheme::Https,
            authority: "examplebucket.s3.amazonaws.com".to_string(),
            path: "/manifest".to_string(),
            query: vec![],
            headers: vec![],
            body: Bytes::from_static(b"v1"),
        };
        sign(&mut request, &credentials, "us-east-1", 1_369_353_600_000);

        let formatted = format!("{request:?}");
        assert!(
            !formatted.contains("Signature=") && !formatted.contains("the-session-token"),
            "a replayable credential leaked into Debug output: {formatted}"
        );
        assert!(
            formatted.contains("/manifest") && formatted.contains("x-amz-date"),
            "the parts a debugging session needs must survive: {formatted}"
        );
    }

    #[test]
    fn the_plaintext_secret_never_reaches_a_debug_log() {
        let mut credentials = example_credentials();
        credentials.session_token = Some("the-session-token".to_string());
        let formatted = format!("{credentials:?}");
        assert!(
            !formatted.contains("wJalrXUtnFEMI") && !formatted.contains("the-session-token"),
            "secrets leaked into Debug output: {formatted}"
        );
        assert!(
            formatted.contains("AKIAIOSFODNN7EXAMPLE"),
            "the access key id is how an operator tells credentials apart: {formatted}"
        );
    }

    #[test]
    fn path_segments_encode_but_slashes_survive_elsewhere() {
        assert_eq!(encode_path_segment("a b+c"), "a%20b%2Bc");
        assert_eq!(encode_path_segment("plain-key_1.~"), "plain-key_1.~");
    }
}
