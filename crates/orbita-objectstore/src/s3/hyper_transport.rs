//! The production [`HttpTransport`], on hyper with rustls.
//!
//! This module is the one place in the store that touches real time and real
//! sockets, which is why it is feature-gated away from everything the
//! simulator links. The timeout below uses `tokio::time` directly rather than
//! the runtime's `Clock` because this transport only exists in production
//! builds where the Tokio runtime is the clock; the simulator supplies its own
//! `HttpTransport` and never enters this file.

use super::http::{HttpRequest, HttpResponse, HttpTransport, TransportFailure};
use async_trait::async_trait;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use std::time::Duration;

/// How long one request may take end to end, connect through last body byte.
///
/// Generous because a segment PUT can be tens of megabytes on a slow link;
/// the caller's retry policy, not this timeout, is the liveness mechanism.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

pub struct HyperTransport {
    client: Client<HttpsConnector<HttpConnector>, Full<Bytes>>,
}

impl HyperTransport {
    #[must_use]
    pub fn new() -> Self {
        let connector = HttpsConnectorBuilder::new()
            .with_webpki_roots()
            // MinIO in development listens on plain HTTP; the scheme in the
            // configured endpoint decides, not this transport.
            .https_or_http()
            .enable_http1()
            .build();
        Self {
            client: Client::builder(TokioExecutor::new()).build(connector),
        }
    }
}

impl Default for HyperTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl HttpTransport for HyperTransport {
    async fn execute(&self, request: HttpRequest) -> Result<HttpResponse, TransportFailure> {
        let mut builder = http::Request::builder()
            .method(request.method)
            .uri(request.url());
        for (name, value) in &request.headers {
            // hyper derives Host from the URI; sending it again would
            // duplicate the header on the wire.
            if name.eq_ignore_ascii_case("host") {
                continue;
            }
            builder = builder.header(name, value);
        }
        let outgoing = builder
            .body(Full::new(request.body))
            .map_err(|e| TransportFailure {
                retryable: false,
                message: format!("malformed request: {e}"),
            })?;

        let response = tokio::time::timeout(REQUEST_TIMEOUT, self.client.request(outgoing))
            .await
            .map_err(|_| TransportFailure {
                retryable: true,
                message: format!("request timed out after {REQUEST_TIMEOUT:?}"),
            })?
            .map_err(|e| TransportFailure {
                // Connection-level failures — refused, reset, TLS — are the
                // weather of object storage; callers retry them.
                retryable: true,
                message: format!("request failed: {e}"),
            })?;

        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_string(),
                    String::from_utf8_lossy(value.as_bytes()).to_string(),
                )
            })
            .collect();
        let body = response
            .into_body()
            .collect()
            .await
            .map_err(|e| TransportFailure {
                retryable: true,
                message: format!("reading response body failed: {e}"),
            })?
            .to_bytes();

        Ok(HttpResponse {
            status,
            headers,
            body,
        })
    }
}
