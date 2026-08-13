//! The HTTP seam the S3 store runs on.
//!
//! `orbita-runtime` abstracts time, disk, peer messaging, randomness, and task
//! spawning, but it has no seam for outbound HTTP, and inventing a
//! general-purpose one for a single consumer would be a bigger abstraction
//! than the problem deserves. This trait is the narrow version: exactly the
//! request/response shape the S3 protocol needs, dyn-compatible like
//! [`ObjectStore`](crate::ObjectStore) itself, so the deterministic simulator
//! can implement it and inject faults — a dropped response, a stale 500, a
//! conditional write that "succeeds" after its response is lost — without a
//! socket anywhere. If the runtime later grows a real network seam, this trait
//! is the one thing that has to move.

use async_trait::async_trait;
use bytes::Bytes;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC};

/// The characters SigV4 leaves bare: the RFC 3986 unreserved set. Everything
/// else is percent-encoded with uppercase hex, which is also what the wire URL
/// uses so the canonical request and the request line never disagree.
pub(crate) const STRICT_ENCODE: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

/// Whether the request travels in the clear or under TLS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    Http,
    Https,
}

impl Scheme {
    fn as_str(self) -> &'static str {
        match self {
            Scheme::Http => "http",
            Scheme::Https => "https",
        }
    }
}

/// One HTTP request, fully formed and signed.
///
/// The path is already percent-encoded and the query pairs are raw; the store
/// encodes the query exactly once, in [`HttpRequest::canonical_query`], so the
/// string that was signed is byte-for-byte the string on the wire.
#[derive(Clone)]
pub struct HttpRequest {
    pub method: &'static str,
    pub scheme: Scheme,
    /// `host[:port]`, which is both where to connect and the `Host` header.
    pub authority: String,
    /// Begins with `/`, percent-encoded.
    pub path: String,
    /// Raw (unencoded) query pairs.
    pub query: Vec<(String, String)>,
    /// Header names are expected lowercase; the signer lowercases when
    /// canonicalizing, but keeping them lowercase at the source avoids two
    /// spellings of the same header.
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
}

impl HttpRequest {
    /// The query string as signed and as sent, sorted by key with strict
    /// percent-encoding, per the SigV4 canonical form.
    #[must_use]
    pub fn canonical_query(&self) -> String {
        let mut pairs: Vec<(String, String)> = self
            .query
            .iter()
            .map(|(k, v)| {
                (
                    percent_encoding::percent_encode(k.as_bytes(), STRICT_ENCODE).to_string(),
                    percent_encoding::percent_encode(v.as_bytes(), STRICT_ENCODE).to_string(),
                )
            })
            .collect();
        pairs.sort();
        pairs
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&")
    }

    /// The absolute URL to dial.
    #[must_use]
    pub fn url(&self) -> String {
        let query = self.canonical_query();
        if query.is_empty() {
            format!("{}://{}{}", self.scheme.as_str(), self.authority, self.path)
        } else {
            format!(
                "{}://{}{}?{query}",
                self.scheme.as_str(),
                self.authority,
                self.path
            )
        }
    }

    /// Public to match [`HttpResponse::header`]: anything that builds a
    /// request against this seam, such as a credential provider, needs to be
    /// able to read one back.
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// Manual so a debug-logged request is not a replayable one. A signed request
/// carries the `authorization` header and, for STS credentials, the session
/// token; either is enough to reissue the request within its validity window,
/// so both are elided while everything a debugging session actually needs,
/// the method, URL, and remaining headers, stays.
///
/// The IMDSv2 session token is elided for the same reason and is arguably
/// worse to leak: it is not a credential itself, it is the capability to read
/// one out of the instance metadata service.
impl std::fmt::Debug for HttpRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let headers: Vec<(&str, &str)> = self
            .headers
            .iter()
            .map(|(name, value)| {
                if name.eq_ignore_ascii_case("authorization")
                    || name.eq_ignore_ascii_case("x-amz-security-token")
                    || name.eq_ignore_ascii_case("x-aws-ec2-metadata-token")
                {
                    (name.as_str(), "<redacted>")
                } else {
                    (name.as_str(), value.as_str())
                }
            })
            .collect();
        f.debug_struct("HttpRequest")
            .field("method", &self.method)
            .field("scheme", &self.scheme)
            .field("authority", &self.authority)
            .field("path", &self.path)
            .field("query", &self.query)
            .field("headers", &headers)
            .field("body_len", &self.body.len())
            .finish()
    }
}

/// One HTTP response, body fully read.
///
/// Bodies here are segments and manifests, which are sized to be held in
/// memory anyway, so there is nothing to stream.
#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
}

impl HttpResponse {
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// A failure below HTTP: the request never produced a status line.
///
/// A response with a status, even a 500, is not a `TransportFailure`; it is a
/// response, and the store maps it. This type is for the connection that
/// refused, the TLS handshake that failed, and the read that timed out.
#[derive(Debug, Clone)]
pub struct TransportFailure {
    /// Whether retrying could plausibly help. Timeouts and connection resets
    /// are retryable; a malformed URL is not.
    pub retryable: bool,
    pub message: String,
}

/// Sends one request and reads the whole response.
///
/// Implementations do not retry, follow redirects, or interpret status codes.
/// Retry policy belongs to the caller via
/// [`ObjectError::is_retryable`](crate::ObjectError::is_retryable), and
/// keeping the transport single-shot is what makes fault injection precise: a
/// simulated fault happens exactly once, not somewhere inside a retry loop it
/// cannot see.
#[async_trait]
pub trait HttpTransport: Send + Sync + 'static {
    async fn execute(&self, request: HttpRequest) -> Result<HttpResponse, TransportFailure>;
}
