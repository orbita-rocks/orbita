//! The S3-compatible [`ObjectStore`], covering AWS S3, MinIO, and R2.
//!
//! # Conditional writes are the load-bearing part
//!
//! The commit protocol swaps a partition manifest with a compare-and-swap, and
//! this store maps that directly onto conditional PUT:
//!
//! - [`Precondition::NotExists`] becomes `If-None-Match: *`
//! - [`Precondition::Match`] becomes `If-Match: <etag>`
//!
//! Backend support, as of this writing:
//!
//! - **AWS S3**: `If-None-Match: *` on PUT since August 2024 and `If-Match`
//!   since November 2024. Both are native and atomic. A losing write gets
//!   `412 Precondition Failed`, or `409 Conditional Request Conflict` when it
//!   lost to a concurrent conditional write still in flight; both map to
//!   [`ObjectError::PreconditionFailed`] because the remedy is the same:
//!   re-read and retry.
//! - **MinIO**: conditional PUT (`If-None-Match: *` and `If-Match`) landed in
//!   the community server in early 2025. Older releases answer
//!   `501 Not Implemented`, which this store surfaces as [`ObjectError::Other`]
//!   with an explicit message rather than pretending the write was guarded.
//! - **Cloudflare R2**: `PutObject` documents full conditional support
//!   (`If-Match`, `If-None-Match`, and the timestamp forms).
//!
//! A backend that answers 501 (or 200-and-ignores-the-header, which only a
//! test against a real server can catch) cannot host a partition manifest.
//! That is a finding to surface, not to paper over: GCS's XML interoperability
//! layer, for example, ignores these headers and needs its
//! `x-goog-if-generation-match` dialect instead, so it would need its own
//! `ObjectStore` implementation rather than this one with a different
//! endpoint.
//!
//! # Determinism
//!
//! `orbita-runtime` has no seam for outbound HTTP today, so this store defines
//! its own narrow one, [`HttpTransport`], and takes its wall clock (used only
//! for request signing) as an injected function. Production wires in the
//! hyper transport and the system clock; the simulator can wire in anything.
//! The store itself never retries, sleeps, or spawns, so every fault a
//! transport injects surfaces exactly once, as a value.

mod http;
mod list;
mod sign;
mod time;

#[cfg(feature = "hyper-client")]
mod hyper_transport;

pub use http::{HttpRequest, HttpResponse, HttpTransport, Scheme, TransportFailure};
pub use sign::Credentials;

#[cfg(feature = "hyper-client")]
pub use hyper_transport::HyperTransport;

use crate::{ETag, ObjectError, ObjectMeta, ObjectResult, ObjectStore, Precondition};
use async_trait::async_trait;
use bytes::Bytes;
use std::ops::Range;
use std::sync::Arc;

/// Where the bucket lives and how to authenticate to it.
#[derive(Debug, Clone)]
pub struct S3Config {
    /// Scheme, host, and optional port: `https://s3.us-east-1.amazonaws.com`,
    /// `https://<account>.r2.cloudflarestorage.com`, or `http://127.0.0.1:9000`.
    pub endpoint: String,
    pub bucket: String,
    /// The SigV4 signing region. MinIO accepts anything consistent
    /// (conventionally `us-east-1`); R2 uses `auto`.
    pub region: String,
    pub credentials: Credentials,
    /// Address objects as `endpoint/bucket/key` instead of
    /// `bucket.endpoint/key`. Required for MinIO and anything else without
    /// wildcard DNS in front of it; AWS accepts either.
    pub force_path_style: bool,
}

/// The wall clock the signer reads, as Unix milliseconds.
///
/// SigV4 embeds the signing time in every request, so this is the one place
/// the store touches wall time. It is injectable for the same reason the
/// transport is: a deterministic run must sign deterministically.
pub type NowMillis = Arc<dyn Fn() -> u64 + Send + Sync>;

/// An [`ObjectStore`] backed by an S3-compatible service.
pub struct S3Store {
    scheme: Scheme,
    endpoint_authority: String,
    bucket: String,
    region: String,
    credentials: Credentials,
    path_style: bool,
    transport: Arc<dyn HttpTransport>,
    now_millis: NowMillis,
}

impl S3Store {
    /// Builds a store over an explicit transport and clock.
    ///
    /// Fails only if the endpoint URL is malformed; nothing is dialed here.
    pub fn new(
        config: S3Config,
        transport: Arc<dyn HttpTransport>,
        now_millis: NowMillis,
    ) -> ObjectResult<Self> {
        let (scheme, authority) = split_endpoint(&config.endpoint)?;
        if config.bucket.is_empty() {
            return Err(ObjectError::Other("bucket name is empty".to_string()));
        }
        Ok(Self {
            scheme,
            endpoint_authority: authority,
            bucket: config.bucket,
            region: config.region,
            credentials: config.credentials,
            path_style: config.force_path_style,
            transport,
            now_millis,
        })
    }

    /// Builds a store over the production hyper transport and the system
    /// clock.
    #[cfg(feature = "hyper-client")]
    pub fn connect(config: S3Config) -> ObjectResult<Self> {
        let transport = Arc::new(HyperTransport::new());
        let now: NowMillis = Arc::new(|| {
            // Duration since the epoch cannot fail on a clock after 1970; a
            // host with a pre-epoch clock has bigger problems than signing.
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0)
        });
        Self::new(config, transport, now)
    }

    /// The signed request for `method` against `key`, or against the bucket
    /// itself when `key` is `None`.
    fn request(
        &self,
        method: &'static str,
        key: Option<&str>,
        query: Vec<(String, String)>,
        headers: Vec<(String, String)>,
        body: Bytes,
    ) -> HttpRequest {
        let encoded_key = key.map(|k| {
            k.split('/')
                .map(sign::encode_path_segment)
                .collect::<Vec<_>>()
                .join("/")
        });
        let (authority, path) = if self.path_style {
            let path = match &encoded_key {
                Some(k) => format!("/{}/{k}", self.bucket),
                None => format!("/{}", self.bucket),
            };
            (self.endpoint_authority.clone(), path)
        } else {
            let path = match &encoded_key {
                Some(k) => format!("/{k}"),
                None => "/".to_string(),
            };
            (format!("{}.{}", self.bucket, self.endpoint_authority), path)
        };
        let mut request = HttpRequest {
            method,
            scheme: self.scheme,
            authority,
            path,
            query,
            headers,
            body,
        };
        sign::sign(
            &mut request,
            &self.credentials,
            &self.region,
            (self.now_millis)(),
        );
        request
    }

    async fn execute(&self, request: HttpRequest) -> ObjectResult<HttpResponse> {
        self.transport.execute(request).await.map_err(|failure| {
            if failure.retryable {
                ObjectError::Transient(failure.message)
            } else {
                ObjectError::Other(failure.message)
            }
        })
    }

    fn etag_of(&self, key: &str, response: &HttpResponse) -> ObjectResult<ETag> {
        response
            .header("etag")
            .map(|v| ETag(v.to_string()))
            .ok_or_else(|| {
                // An entity tag is not decoration: without one the next
                // conditional write has nothing to compare against, so a
                // backend that omits it cannot participate in the commit
                // protocol.
                ObjectError::Other(format!("no etag in response for {key}"))
            })
    }
}

/// Maps a non-success HTTP status to the contract's error taxonomy.
fn error_for(key: &str, response: &HttpResponse) -> ObjectError {
    let detail = |fallback: &str| {
        let body = String::from_utf8_lossy(&response.body);
        let code = xml_field(&body, "Code");
        let message = xml_field(&body, "Message");
        match (code, message) {
            (Some(c), Some(m)) => format!("{c}: {m}"),
            (Some(c), None) => c,
            _ => fallback.to_string(),
        }
    };
    match response.status {
        404 => ObjectError::NotFound(key.to_string()),
        // 412 is the precondition losing outright. 409 is Amazon's
        // ConditionalRequestConflict: the write lost to another conditional
        // write that was still settling. The caller's move is identical, so
        // the taxonomy does not distinguish them.
        412 | 409 => ObjectError::PreconditionFailed(key.to_string()),
        401 | 403 => ObjectError::AccessDenied(key.to_string()),
        429 | 500 | 502 | 503 | 504 => ObjectError::Transient(format!(
            "{key}: status {}: {}",
            response.status,
            detail("no further detail")
        )),
        501 => ObjectError::Other(format!(
            "{key}: the backend answered 501 Not Implemented, which for a conditional \
             write means it cannot guard the manifest swap; upgrade the backend or use \
             one that supports conditional PUT ({})",
            detail("no further detail")
        )),
        status => ObjectError::Other(format!(
            "{key}: unexpected status {status}: {}",
            detail("no further detail")
        )),
    }
}

/// Pulls `<tag>value</tag>` out of an error document without a full parse;
/// error bodies are tiny and their shape is stable across backends.
fn xml_field(body: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = body.find(&open)? + open.len();
    let end = body[start..].find(&close)? + start;
    Some(body[start..end].to_string())
}

fn split_endpoint(endpoint: &str) -> ObjectResult<(Scheme, String)> {
    let (scheme, rest) = if let Some(rest) = endpoint.strip_prefix("https://") {
        (Scheme::Https, rest)
    } else if let Some(rest) = endpoint.strip_prefix("http://") {
        (Scheme::Http, rest)
    } else {
        return Err(ObjectError::Other(format!(
            "endpoint {endpoint:?} must start with http:// or https://"
        )));
    };
    let authority = rest.trim_end_matches('/');
    if authority.is_empty() || authority.contains('/') {
        return Err(ObjectError::Other(format!(
            "endpoint {endpoint:?} must be scheme://host[:port] with no path"
        )));
    }
    Ok((scheme, authority.to_string()))
}

#[async_trait]
impl ObjectStore for S3Store {
    async fn put(&self, key: &str, data: Bytes) -> ObjectResult<ETag> {
        let request = self.request("PUT", Some(key), vec![], vec![], data);
        let response = self.execute(request).await?;
        if response.status != 200 {
            return Err(error_for(key, &response));
        }
        self.etag_of(key, &response)
    }

    async fn put_if(
        &self,
        key: &str,
        data: Bytes,
        precondition: Precondition,
    ) -> ObjectResult<ETag> {
        let header = match &precondition {
            Precondition::NotExists => ("if-none-match".to_string(), "*".to_string()),
            Precondition::Match(etag) => ("if-match".to_string(), etag.0.clone()),
        };
        let request = self.request("PUT", Some(key), vec![], vec![header], data);
        let response = self.execute(request).await?;
        if response.status != 200 {
            return Err(error_for(key, &response));
        }
        self.etag_of(key, &response)
    }

    async fn get(&self, key: &str) -> ObjectResult<(Bytes, ETag)> {
        let request = self.request("GET", Some(key), vec![], vec![], Bytes::new());
        let response = self.execute(request).await?;
        if response.status != 200 {
            return Err(error_for(key, &response));
        }
        let etag = self.etag_of(key, &response)?;
        Ok((response.body, etag))
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> ObjectResult<Bytes> {
        if range.start > range.end {
            return Err(ObjectError::Other(format!(
                "range {}..{} is inverted",
                range.start, range.end
            )));
        }
        if range.start == range.end {
            // `bytes=n-(n-1)` is not a legal range header, and zero bytes need
            // no round trip anyway.
            return Ok(Bytes::new());
        }
        let header = (
            "range".to_string(),
            format!("bytes={}-{}", range.start, range.end - 1),
        );
        let request = self.request("GET", Some(key), vec![], vec![header], Bytes::new());
        let response = self.execute(request).await?;
        match response.status {
            206 => Ok(response.body),
            // A server that answers a range request with the whole object has
            // ignored the header. Returning the slice would hide that the
            // request cost the full object; the contract's claims about IO are
            // claims about requests.
            200 => Err(ObjectError::Other(format!(
                "{key}: the backend ignored the range request and returned the full object"
            ))),
            416 => Err(ObjectError::Other(format!(
                "{key}: range {}..{} is not satisfiable",
                range.start, range.end
            ))),
            _ => Err(error_for(key, &response)),
        }
    }

    async fn head(&self, key: &str) -> ObjectResult<ObjectMeta> {
        let request = self.request("HEAD", Some(key), vec![], vec![], Bytes::new());
        let response = self.execute(request).await?;
        if response.status != 200 {
            return Err(error_for(key, &response));
        }
        let etag = self.etag_of(key, &response)?;
        let size = response
            .header("content-length")
            .and_then(|v| v.trim().parse::<u64>().ok())
            .ok_or_else(|| {
                ObjectError::Other(format!("no content-length in response for {key}"))
            })?;
        Ok(ObjectMeta {
            key: key.to_string(),
            size,
            etag,
        })
    }

    async fn list(&self, prefix: &str) -> ObjectResult<Vec<ObjectMeta>> {
        let mut objects = Vec::new();
        let mut continuation: Option<String> = None;
        loop {
            let mut query = vec![
                ("list-type".to_string(), "2".to_string()),
                ("prefix".to_string(), prefix.to_string()),
            ];
            if let Some(token) = &continuation {
                query.push(("continuation-token".to_string(), token.clone()));
            }
            let request = self.request("GET", None, query, vec![], Bytes::new());
            let response = self.execute(request).await?;
            if response.status != 200 {
                return Err(error_for(prefix, &response));
            }
            let page = list::parse_list_page(&response.body)?;
            objects.extend(page.objects);
            match page.next_continuation_token {
                Some(token) => continuation = Some(token),
                None => break,
            }
        }
        Ok(objects)
    }

    async fn delete(&self, key: &str) -> ObjectResult<()> {
        let request = self.request("DELETE", Some(key), vec![], vec![], Bytes::new());
        let response = self.execute(request).await?;
        match response.status {
            // S3 answers 204 whether or not the key existed, which matches
            // the contract: delete is idempotent.
            200 | 204 => Ok(()),
            _ => Err(error_for(key, &response)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// A transport that records every request and plays back a script of
    /// responses, which is the same shape a simulated transport will take.
    struct ScriptedTransport {
        requests: Mutex<Vec<HttpRequest>>,
        responses: Mutex<Vec<Result<HttpResponse, TransportFailure>>>,
    }

    impl ScriptedTransport {
        fn new(responses: Vec<Result<HttpResponse, TransportFailure>>) -> Arc<Self> {
            Arc::new(Self {
                requests: Mutex::new(Vec::new()),
                // Stored reversed so playback can pop from the back.
                responses: Mutex::new(responses.into_iter().rev().collect()),
            })
        }

        fn requests(&self) -> Vec<HttpRequest> {
            self.requests.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl HttpTransport for ScriptedTransport {
        async fn execute(&self, request: HttpRequest) -> Result<HttpResponse, TransportFailure> {
            self.requests.lock().unwrap().push(request);
            self.responses
                .lock()
                .unwrap()
                .pop()
                .expect("the test script ran out of responses")
        }
    }

    fn response(status: u16, headers: &[(&str, &str)], body: &str) -> HttpResponse {
        HttpResponse {
            status,
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            body: Bytes::from(body.to_string()),
        }
    }

    fn store(transport: Arc<ScriptedTransport>) -> S3Store {
        store_with(transport, true)
    }

    fn store_with(transport: Arc<ScriptedTransport>, path_style: bool) -> S3Store {
        S3Store::new(
            S3Config {
                endpoint: "http://127.0.0.1:9000".to_string(),
                bucket: "orbita".to_string(),
                region: "us-east-1".to_string(),
                credentials: Credentials {
                    access_key_id: "AKID".to_string(),
                    secret_access_key: "secret".to_string(),
                    session_token: None,
                },
                force_path_style: path_style,
            },
            transport,
            Arc::new(|| 1_369_353_600_000),
        )
        .expect("valid config")
    }

    #[tokio::test]
    async fn put_if_not_exists_sends_if_none_match_star() {
        let transport = ScriptedTransport::new(vec![Ok(response(200, &[("ETag", "\"v1\"")], ""))]);
        let etag = store(transport.clone())
            .put_if(
                "m/current",
                Bytes::from_static(b"x"),
                Precondition::NotExists,
            )
            .await
            .expect("created");
        assert_eq!(etag, ETag("\"v1\"".to_string()));
        let request = &transport.requests()[0];
        assert_eq!(request.header("if-none-match"), Some("*"));
        assert_eq!(request.method, "PUT");
    }

    #[tokio::test]
    async fn put_if_match_sends_the_held_tag_verbatim() {
        let transport = ScriptedTransport::new(vec![Ok(response(200, &[("ETag", "\"v2\"")], ""))]);
        store(transport.clone())
            .put_if(
                "m/current",
                Bytes::from_static(b"x"),
                Precondition::Match(ETag("\"v1\"".to_string())),
            )
            .await
            .expect("swapped");
        assert_eq!(transport.requests()[0].header("if-match"), Some("\"v1\""));
    }

    #[tokio::test]
    async fn a_412_and_a_409_both_mean_the_precondition_lost() {
        for status in [412, 409] {
            let transport = ScriptedTransport::new(vec![Ok(response(status, &[], ""))]);
            let result = store(transport)
                .put_if("k", Bytes::new(), Precondition::NotExists)
                .await;
            assert_eq!(
                result,
                Err(ObjectError::PreconditionFailed("k".to_string())),
                "status {status}"
            );
        }
    }

    #[tokio::test]
    async fn a_501_names_the_missing_conditional_write_support() {
        let transport = ScriptedTransport::new(vec![Ok(response(501, &[], ""))]);
        let result = store(transport)
            .put_if("k", Bytes::new(), Precondition::NotExists)
            .await;
        match result {
            Err(ObjectError::Other(message)) => {
                assert!(message.contains("conditional"), "{message}");
            }
            other => panic!("expected Other, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_missing_object_is_not_found() {
        let transport = ScriptedTransport::new(vec![Ok(response(
            404,
            &[],
            "<Error><Code>NoSuchKey</Code></Error>",
        ))]);
        let result = store(transport).get("gone").await;
        assert_eq!(result, Err(ObjectError::NotFound("gone".to_string())));
    }

    #[tokio::test]
    async fn a_throttle_and_a_5xx_are_transient() {
        for status in [429, 500, 503] {
            let transport = ScriptedTransport::new(vec![Ok(response(status, &[], ""))]);
            let result = store(transport).get("k").await;
            assert!(
                matches!(&result, Err(e) if e.is_retryable()),
                "status {status} gave {result:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_transport_failure_maps_by_its_retryability() {
        let transport = ScriptedTransport::new(vec![Err(TransportFailure {
            retryable: true,
            message: "timed out".to_string(),
        })]);
        let result = store(transport).get("k").await;
        assert_eq!(result, Err(ObjectError::Transient("timed out".to_string())));
    }

    #[tokio::test]
    async fn a_range_read_asks_for_inclusive_bytes_and_wants_a_206() {
        let transport = ScriptedTransport::new(vec![Ok(response(206, &[], "234"))]);
        let bytes = store(transport.clone())
            .get_range("k", 2..5)
            .await
            .expect("partial content");
        assert_eq!(bytes, Bytes::from_static(b"234"));
        assert_eq!(transport.requests()[0].header("range"), Some("bytes=2-4"));
    }

    #[tokio::test]
    async fn an_ignored_range_header_is_an_error_not_a_slice() {
        let transport =
            ScriptedTransport::new(vec![Ok(response(200, &[("ETag", "\"e\"")], "0123456789"))]);
        assert!(store(transport).get_range("k", 2..5).await.is_err());
    }

    #[tokio::test]
    async fn an_empty_range_makes_no_request() {
        let transport = ScriptedTransport::new(vec![]);
        let bytes = store(transport.clone())
            .get_range("k", 5..5)
            .await
            .expect("empty");
        assert!(bytes.is_empty());
        assert!(transport.requests().is_empty());
    }

    #[tokio::test]
    async fn a_listing_follows_its_continuation_token() {
        let first = r#"<ListBucketResult>
            <IsTruncated>true</IsTruncated>
            <NextContinuationToken>tok</NextContinuationToken>
            <Contents><Key>p/a</Key><ETag>"a"</ETag><Size>1</Size></Contents>
        </ListBucketResult>"#;
        let second = r#"<ListBucketResult>
            <IsTruncated>false</IsTruncated>
            <Contents><Key>p/b</Key><ETag>"b"</ETag><Size>2</Size></Contents>
        </ListBucketResult>"#;
        let transport = ScriptedTransport::new(vec![
            Ok(response(200, &[], first)),
            Ok(response(200, &[], second)),
        ]);
        let keys: Vec<String> = store(transport.clone())
            .list("p/")
            .await
            .expect("lists")
            .into_iter()
            .map(|m| m.key)
            .collect();
        assert_eq!(keys, vec!["p/a".to_string(), "p/b".to_string()]);

        let requests = transport.requests();
        assert!(
            requests[1]
                .query
                .contains(&("continuation-token".to_string(), "tok".to_string())),
            "the second page must resume from the token: {:?}",
            requests[1].query
        );
    }

    #[tokio::test]
    async fn path_style_addresses_through_the_bucket_path() {
        let transport = ScriptedTransport::new(vec![Ok(response(200, &[("ETag", "\"e\"")], "v"))]);
        store(transport.clone()).get("dir/key 1").await.expect("ok");
        let request = &transport.requests()[0];
        assert_eq!(request.authority, "127.0.0.1:9000");
        assert_eq!(request.path, "/orbita/dir/key%201");
    }

    #[tokio::test]
    async fn virtual_host_style_addresses_through_the_bucket_host() {
        let transport = ScriptedTransport::new(vec![Ok(response(200, &[("ETag", "\"e\"")], "v"))]);
        store_with(transport.clone(), false)
            .get("dir/key")
            .await
            .expect("ok");
        let request = &transport.requests()[0];
        assert_eq!(request.authority, "orbita.127.0.0.1:9000");
        assert_eq!(request.path, "/dir/key");
    }

    #[tokio::test]
    async fn a_put_without_an_etag_in_the_response_is_an_error() {
        let transport = ScriptedTransport::new(vec![Ok(response(200, &[], ""))]);
        assert!(store(transport).put("k", Bytes::new()).await.is_err());
    }

    #[tokio::test]
    async fn head_reads_size_and_tag_from_headers() {
        let transport = ScriptedTransport::new(vec![Ok(response(
            200,
            &[("ETag", "\"e\""), ("Content-Length", "42")],
            "",
        ))]);
        let meta = store(transport).head("k").await.expect("ok");
        assert_eq!(
            meta,
            ObjectMeta {
                key: "k".to_string(),
                size: 42,
                etag: ETag("\"e\"".to_string()),
            }
        );
    }

    #[test]
    fn an_endpoint_needs_a_scheme_and_no_path() {
        assert!(split_endpoint("127.0.0.1:9000").is_err());
        assert!(split_endpoint("https://host/extra").is_err());
        assert_eq!(
            split_endpoint("https://s3.amazonaws.com/").expect("valid"),
            (Scheme::Https, "s3.amazonaws.com".to_string())
        );
    }
}
