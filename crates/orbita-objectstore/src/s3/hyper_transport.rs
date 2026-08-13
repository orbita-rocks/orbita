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
use hyper_util::client::legacy::connect::{capture_connection, HttpConnector};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use std::time::Duration;

/// How long one request may take end to end, connect through last body byte.
///
/// Generous because a segment PUT can be tens of megabytes on a slow link;
/// the caller's retry policy, not this timeout, is the liveness mechanism.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Whether the server is expected to hang up after answering with `status`,
/// so the connection must be dropped rather than returned to the pool.
///
/// MinIO answers a losing conditional PUT with `412` and then closes the
/// socket, but does not say `Connection: close` in that response. Nothing on
/// the wire tells hyper the connection is finished, so the pool keeps it and
/// hands it to the next call, which writes a request into a socket the server
/// has already torn down and gets back
/// `hyper::Error(IncompleteMessage)` — "connection closed before message
/// completed". Measured against the pinned CI image: every 412 is followed by
/// a close, on both `If-None-Match: *` and `If-Match`, at every body size.
/// `404`, `416`, and every 2xx were measured over the same loop and never
/// close, which is why they are deliberately absent here.
///
/// `409` is Amazon's `ConditionalRequestConflict`. It is not reachable on
/// MinIO so it could not be measured, but it is the same event as a 412 — a
/// conditional write that lost — so it gets the same treatment. The cost of
/// being wrong is one extra TCP handshake on a path that has already lost a
/// race; the cost of omitting it, if AWS behaves like MinIO, is the bug this
/// function exists to close.
///
/// Dropping the connection rather than retrying the failed call is the whole
/// point. `IncompleteMessage` does not prove the request went unprocessed —
/// hyper had already written it — so retrying it would risk reissuing a
/// conditional PUT the server actually applied but never got to acknowledge,
/// which would report a won manifest CAS as a lost one. Refusing to reuse the
/// connection removes the hazard for every verb without ever sending anything
/// twice. hyper already retries the case that *is* provably unsent: a request
/// it can hand back to the caller surfaces as `Canceled`, not as this.
fn server_hangs_up_after(status: u16) -> bool {
    matches!(status, 409 | 412)
}

/// An error plus its source chain, as one line.
///
/// hyper's client error renders as `client error (SendRequest)` and nothing
/// else; the cause — "connection closed before message completed", a refused
/// connect, a TLS failure — lives only in `source()`. Flattening the chain
/// here is what makes a transport failure in a log or a test report say what
/// actually went wrong.
fn describe(error: &(dyn std::error::Error + 'static)) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        source = cause.source();
    }
    message
}

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
        let mut outgoing = builder
            .body(Full::new(request.body))
            .map_err(|e| TransportFailure {
                retryable: false,
                message: format!("malformed request: {e}"),
            })?;

        // Taken before the request is sent so the connection it lands on can
        // be reached once the status is known; see `server_hangs_up_after`.
        let connection = capture_connection(&mut outgoing);

        let response = tokio::time::timeout(REQUEST_TIMEOUT, self.client.request(outgoing))
            .await
            .map_err(|_| TransportFailure {
                retryable: true,
                message: format!("request timed out after {REQUEST_TIMEOUT:?}"),
            })?
            .map_err(|e| TransportFailure {
                // Connection-level failures — refused, reset, TLS — are the
                // weather of object storage; callers retry them. The message
                // keeps the source chain because hyper's own Display for a
                // pool failure is only ever "client error (SendRequest)",
                // which names no cause at all.
                retryable: true,
                message: format!("request failed: {}", describe(&e)),
            })?;

        let status = response.status().as_u16();
        if server_hangs_up_after(status) {
            // Before the body is read, because reading it to completion is
            // what hands the connection back to the pool.
            if let Some(connected) = connection.connection_metadata().as_ref() {
                connected.poison();
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::Scheme;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    /// How the stub should answer a request that carries a conditional header.
    #[derive(Clone, Copy)]
    enum Losing {
        /// MinIO's behaviour: 412, keep-alive promised, socket closed anyway.
        HangUpAfter412,
        /// A server that answers the same 404 twice on one connection, to show
        /// that a status outside the conditional-write family still pools.
        Never404,
    }

    /// Index just past the `\r\n\r\n` that ends a request head.
    fn head_end(buf: &[u8]) -> Option<usize> {
        buf.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4)
    }

    fn content_length(head: &str) -> usize {
        head.lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse().ok())?
            })
            .unwrap_or(0)
    }

    /// Reads one whole request, head and body, leaving anything extra buffered.
    async fn read_request(socket: &mut TcpStream, buffered: &mut Vec<u8>) -> Option<String> {
        loop {
            if let Some(end) = head_end(buffered) {
                let head = String::from_utf8_lossy(&buffered[..end]).to_string();
                let total = end + content_length(&head);
                while buffered.len() < total {
                    let mut chunk = [0u8; 8192];
                    match socket.read(&mut chunk).await {
                        Ok(0) | Err(_) => return None,
                        Ok(n) => buffered.extend_from_slice(&chunk[..n]),
                    }
                }
                buffered.drain(..total);
                return Some(head);
            }
            let mut chunk = [0u8; 8192];
            match socket.read(&mut chunk).await {
                Ok(0) | Err(_) => return None,
                Ok(n) => buffered.extend_from_slice(&chunk[..n]),
            }
        }
    }

    fn response_bytes(status_line: &str, extra: &str, body: &str) -> Vec<u8> {
        format!(
            "HTTP/1.1 {status_line}\r\n{extra}content-length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    /// A stand-in for MinIO that reproduces the poisoning every time instead of
    /// five times in a hundred.
    ///
    /// The determinism comes from *when* it closes. A real server closes as
    /// soon as it has answered, and whether the client notices before it
    /// reuses the socket is a race the client usually wins. This stub waits
    /// for the client's next bytes before hanging up, which pins the race
    /// open: if the transport reuses the connection at all, the request is
    /// already on the wire when the EOF arrives, which is exactly the
    /// `IncompleteMessage` seen in CI.
    ///
    /// Returns the authority to dial and the number of connections accepted,
    /// which is the assertion that matters: reuse shows up as a count of one.
    async fn stub(mode: Losing) -> (String, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let authority = format!("127.0.0.1:{}", listener.local_addr().expect("addr").port());
        let accepted = Arc::new(AtomicUsize::new(0));
        let counter = accepted.clone();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                counter.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    let mut buffered = Vec::new();
                    while let Some(head) = read_request(&mut socket, &mut buffered).await {
                        let lowered = head.to_ascii_lowercase();
                        let conditional = lowered.contains("\r\nif-none-match:")
                            || lowered.contains("\r\nif-match:");
                        if conditional {
                            match mode {
                                Losing::HangUpAfter412 => {
                                    let _ = socket
                                        .write_all(&response_bytes(
                                            "412 Precondition Failed",
                                            "",
                                            "<Error><Code>PreconditionFailed</Code></Error>",
                                        ))
                                        .await;
                                    let mut sink = [0u8; 8192];
                                    let _ = socket.read(&mut sink).await;
                                    return;
                                }
                                Losing::Never404 => {
                                    let _ = socket
                                        .write_all(&response_bytes(
                                            "404 Not Found",
                                            "",
                                            "<Error><Code>NoSuchKey</Code></Error>",
                                        ))
                                        .await;
                                }
                            }
                        } else {
                            let _ = socket
                                .write_all(&response_bytes("200 OK", "etag: \"v1\"\r\n", "body"))
                                .await;
                        }
                    }
                });
            }
        });
        (authority, accepted)
    }

    fn request(authority: &str, headers: Vec<(String, String)>) -> HttpRequest {
        HttpRequest {
            method: if headers.is_empty() { "GET" } else { "PUT" },
            scheme: Scheme::Http,
            authority: authority.to_string(),
            path: "/bucket/manifest".to_string(),
            query: vec![],
            headers,
            body: Bytes::from_static(b"v1"),
        }
    }

    fn conditional(authority: &str) -> HttpRequest {
        request(
            authority,
            vec![("if-none-match".to_string(), "*".to_string())],
        )
    }

    #[tokio::test]
    async fn a_call_after_a_losing_conditional_write_does_not_inherit_the_closed_connection() {
        let (authority, accepted) = stub(Losing::HangUpAfter412).await;
        let transport = HyperTransport::new();

        let lost = transport
            .execute(conditional(&authority))
            .await
            .expect("the losing conditional write is still a response");
        assert_eq!(lost.status, 412);

        // Before the fix this call failed every time with
        // `client error (SendRequest): connection closed before message
        // completed`, because the pool handed back the socket the server had
        // already torn down.
        let next = transport
            .execute(request(&authority, vec![]))
            .await
            .expect("the call after a lost CAS must not fail on the transport");
        assert_eq!(next.status, 200);
        assert_eq!(
            accepted.load(Ordering::SeqCst),
            2,
            "the connection the server hung up on must not be reused"
        );
    }

    #[tokio::test]
    async fn a_404_still_reuses_its_connection() {
        let (authority, accepted) = stub(Losing::Never404).await;
        let transport = HyperTransport::new();

        assert_eq!(
            transport
                .execute(conditional(&authority))
                .await
                .expect("404")
                .status,
            404
        );
        assert_eq!(
            transport
                .execute(request(&authority, vec![]))
                .await
                .expect("follow-up")
                .status,
            200
        );
        // The measured behaviour is that a 404 leaves the connection healthy,
        // so throwing it away would be a cost paid for nothing on every probe
        // for an object that is not there.
        assert_eq!(
            accepted.load(Ordering::SeqCst),
            1,
            "a status outside the conditional-write family must still pool"
        );
    }

    /// The guard on the fix: nothing here may turn a real transport failure
    /// into a quiet success.
    #[tokio::test]
    async fn a_server_that_answers_nothing_is_still_an_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let authority = format!("127.0.0.1:{}", listener.local_addr().expect("addr").port());
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                drop(socket);
            }
        });

        let failure = HyperTransport::new()
            .execute(request(&authority, vec![]))
            .await
            .expect_err("a connection that closes without responding is a failure");
        assert!(failure.retryable);
        // Not asserted against a particular cause: whether the peer's close
        // arrives as an orderly EOF or as an RST depends on the platform, and
        // both are the same finding. What must hold is that it arrives at all.
        assert!(
            failure.message.starts_with("request failed: "),
            "got {:?}",
            failure.message
        );
    }

    #[tokio::test]
    async fn a_refused_connection_is_still_an_error() {
        // Binding and dropping hands back a port nothing is listening on.
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let authority = format!("127.0.0.1:{}", listener.local_addr().expect("addr").port());
        drop(listener);

        let failure = HyperTransport::new()
            .execute(request(&authority, vec![]))
            .await
            .expect_err("nothing is listening");
        assert!(failure.retryable);
        assert!(
            failure.message.contains("connect"),
            "a failure to connect must say so, got {:?}",
            failure.message
        );
    }

    /// The reason a `SendRequest` failure was diagnosable at all: hyper's own
    /// rendering of it names no cause, so the chain has to be flattened.
    #[test]
    fn a_failure_carries_its_causes_into_one_message() {
        #[derive(Debug)]
        struct Layer(&'static str, Option<Box<Layer>>);
        impl std::fmt::Display for Layer {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.0)
            }
        }
        impl std::error::Error for Layer {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                self.1.as_deref().map(|l| l as &dyn std::error::Error)
            }
        }

        let error = Layer(
            "client error (SendRequest)",
            Some(Box::new(Layer(
                "connection closed before message completed",
                None,
            ))),
        );
        assert_eq!(
            describe(&error),
            "client error (SendRequest): connection closed before message completed"
        );
    }
}
