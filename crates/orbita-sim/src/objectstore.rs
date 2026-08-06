//! An object store that lies the way object storage lies.
//!
//! # Why this is a transport and not an `ObjectStore`
//!
//! Standing a fault-injecting [`ObjectStore`] into the seam would have been
//! less code, and it would have tested a store nothing ships. The real store
//! is [`orbita_objectstore::s3::S3Store`], and between a caller and a bucket
//! it does work that can be wrong: it signs, it shapes conditional writes into
//! `If-Match` and `If-None-Match`, it maps a status back onto the contract's
//! error taxonomy, and it walks a paginated XML listing. A simulated
//! `ObjectStore` skips all of that, so a scenario built on one would prove
//! that the commit protocol is safe against a store the product does not have.
//!
//! [`HttpTransport`] is the seam that was cut for this, and its contract is
//! the reason it works: an implementation does not retry, does not follow
//! redirects, and does not interpret statuses, so a fault happens exactly once
//! rather than somewhere inside a retry loop the simulator cannot see. The
//! `s3` and `hyper-client` Cargo features were split so this crate could take
//! the first without the second, which is why a deterministic run links no TLS
//! stack.
//!
//! # The fault that earns the whole module
//!
//! [`StoreFault::ResponseLost`] applies the request and then loses the answer.
//! A writer that issued a conditional PUT and got a transport error cannot
//! tell that from one that never landed, and per ADR 0006 the manifest swap is
//! the durability boundary, so the two states differ by exactly whether the
//! partition's data is durable. Everything else here exists to get a scenario
//! into position to hit that.

use crate::world::{BucketState, SimCore, SimSleep, StoredObject};

use orbita_core::NodeId;
use orbita_objectstore::s3::{
    Credentials, HttpRequest, HttpResponse, HttpTransport, NowMillis, S3Config, S3Store,
    TransportFailure,
};
use orbita_objectstore::ObjectStore;

use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;

/// The endpoint the simulated store answers on.
///
/// Never dialed and never resolved; it exists because SigV4 signs the `Host`
/// header, so the request the transport receives has to have been built
/// against some authority.
const SIM_ENDPOINT: &str = "http://objectstore.sim";

/// How many keys one listing page carries.
///
/// Far below the thousand a real bucket uses, on purpose. Pagination is a loop
/// with a continuation token in the middle of it, and a page size no test ever
/// crosses is a loop no test ever enters.
const LIST_PAGE_KEYS: usize = 4;

/// What the store does to one request instead of serving it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreFault {
    /// The request never arrived, so nothing was applied and a retry costs
    /// nothing but time.
    RequestLost,
    /// The store applied the request and the answer was lost coming back. The
    /// caller sees the same error as [`StoreFault::RequestLost`] and the world
    /// is in the opposite state.
    ResponseLost,
    /// The store answered with this status and applied nothing. Use a 503 for
    /// a throttle the caller should retry, a 500 for a server that broke.
    Status(u16),
    /// The node that issued the request dies with it in flight, and the store
    /// never answers. This is how a crash is placed between two store calls
    /// rather than between two scheduler steps that happen to be near them.
    CrashNode(NodeId),
}

/// One fault a scenario queued by name, waiting for the request it matches.
#[derive(Debug, Clone)]
pub(crate) struct PendingFault {
    method: &'static str,
    key_contains: String,
    fault: StoreFault,
}

/// A handle onto one bucket, and the transport that reaches it.
///
/// Cheap to clone in the sense that matters: the objects live in the world, so
/// two handles onto one name are two views of one bucket rather than two
/// buckets.
pub struct SimBucket {
    core: Arc<SimCore>,
    name: String,
}

impl SimBucket {
    pub(crate) fn new(core: Arc<SimCore>, name: String) -> Self {
        Self { core, name }
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The [`ObjectStore`] a partition persists through, which is the real S3
    /// store wired onto this transport and the simulated clock.
    ///
    /// The clock matters more than it looks: SigV4 embeds the signing time in
    /// every request, so a store on the system clock would sign differently on
    /// two runs of one seed and the trace would stop being reproducible.
    #[must_use]
    pub fn store(self: &Arc<Self>) -> Arc<dyn ObjectStore> {
        let core = self.core.clone();
        let origin = core.config.wall_clock_origin_millis;
        let now: NowMillis = Arc::new(move || origin + core.now() / 1_000_000);
        Arc::new(
            S3Store::new(
                S3Config {
                    endpoint: SIM_ENDPOINT.to_string(),
                    bucket: self.name.clone(),
                    region: "sim".to_string(),
                    credentials: Credentials {
                        access_key_id: "SIMULATED".to_string(),
                        secret_access_key: "simulated".to_string(),
                        session_token: None,
                    },
                    // Path style, so the bucket is in the path rather than in
                    // a hostname this transport would have to parse back out.
                    force_path_style: true,
                },
                self.clone(),
                now,
            )
            .expect("the simulated endpoint is a literal and the bucket name is not empty"),
        )
    }

    /// Every key the bucket holds, sorted.
    ///
    /// A test asserts about orphans with this: an object a crash stranded is
    /// present and unnamed by the manifest, and both halves of that sentence
    /// have to be checkable.
    #[must_use]
    pub fn keys(&self) -> Vec<String> {
        let state = self.core.state();
        state
            .buckets
            .get(&self.name)
            .map(|bucket| bucket.objects.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// One object's bytes, read without going through the transport.
    ///
    /// Deliberately not an `ObjectStore` call: an assertion about what the
    /// bucket holds must not be able to fail because a fault was injected into
    /// the read that checks it.
    #[must_use]
    pub fn object(&self, key: &str) -> Option<Bytes> {
        let state = self.core.state();
        state
            .buckets
            .get(&self.name)?
            .objects
            .get(key)
            .map(|object| object.bytes.clone())
    }

    /// How many requests the bucket has served, faults included. A scenario
    /// that injected nothing because it never reached the store is one whose
    /// author was mistaken about what it tested.
    #[must_use]
    pub fn requests(&self) -> u64 {
        let state = self.core.state();
        state
            .buckets
            .get(&self.name)
            .map(|bucket| bucket.requests)
            .unwrap_or(0)
    }

    /// Arms one fault against the next request whose method matches and whose
    /// key contains `key_contains`.
    ///
    /// Named rather than sampled, because the three failures ADR 0006 is
    /// argued on are specific ones and a scenario that waits for a seed to
    /// produce them is a scenario that mostly does not test them. The sampled
    /// rates in [`crate::StoreFaults`] cover the states nobody thought to name.
    pub fn inject_once(
        &self,
        method: &'static str,
        key_contains: impl Into<String>,
        fault: StoreFault,
    ) {
        let key_contains = key_contains.into();
        let mut state = self.core.state();
        let bucket = state.buckets.entry(self.name.clone()).or_default();
        bucket.injected.push_back(PendingFault {
            method,
            key_contains: key_contains.clone(),
            fault: fault.clone(),
        });
        state.record(format!(
            "store armed bucket={} method={method} match={key_contains} fault={fault:?}",
            self.name
        ));
    }

    /// How many armed faults are still waiting for a request to match.
    ///
    /// A scenario that armed a fault nothing ever matched ran without the
    /// failure it was written around, and passing quietly is the worst
    /// possible outcome for a test whose whole subject is a failure.
    #[must_use]
    pub fn armed(&self) -> usize {
        let state = self.core.state();
        state
            .buckets
            .get(&self.name)
            .map(|bucket| bucket.injected.len())
            .unwrap_or(0)
    }

    /// Charges a request its round trip, which is also what makes every store
    /// call a scheduling point.
    async fn spin(&self, slow: bool) {
        let faults = self.core.config.store.clone();
        let nanos = if slow {
            faults.slow_latency.as_nanos() as u64
        } else {
            self.core.latency(faults.min_latency, faults.max_latency)
        };
        SimSleep::new(self.core.clone(), nanos).await;
    }

    /// Picks the fault this request suffers, if any.
    ///
    /// An armed fault wins over a sampled one and does not spend the budget: a
    /// scenario that asked for a specific failure has already decided the run
    /// is about that failure. It does respect
    /// [`Simulation::stop_injecting_faults`](crate::Simulation::stop_injecting_faults),
    /// because a leftover arming firing during the recovery window would break
    /// the only period in which a liveness claim can be made.
    fn choose_fault(&self, method: &'static str, key: &str) -> Option<StoreFault> {
        let mut state = self.core.state();
        let name = self.name.clone();
        let frozen = state.faults_frozen;
        let bucket = state.buckets.entry(name.clone()).or_default();
        bucket.requests += 1;
        let armed = if frozen {
            None
        } else {
            bucket
                .injected
                .iter()
                .position(|f| f.method == method && key.contains(&f.key_contains))
        };
        if let Some(position) = armed {
            let fired = bucket
                .injected
                .remove(position)
                .expect("the position came from this queue")
                .fault;
            state.last_fault_nanos = Some(state.now);
            state.record(format!(
                "store fault bucket={name} method={method} key={key} kind={fired:?} armed=true"
            ));
            return Some(fired);
        }

        let faults = self.core.config.store.clone();
        // Ordered least to most surprising, so that raising one rate does not
        // shift which stream position the others draw from.
        let sampled = if self
            .core
            .roll_fault(&mut state, faults.request_lost_permille)
        {
            StoreFault::RequestLost
        } else if self
            .core
            .roll_fault(&mut state, faults.server_error_permille)
        {
            StoreFault::Status(503)
        } else if self
            .core
            .roll_fault(&mut state, faults.response_lost_permille)
        {
            StoreFault::ResponseLost
        } else {
            return None;
        };
        state.record(format!(
            "store fault bucket={name} method={method} key={key} kind={sampled:?} armed=false"
        ));
        Some(sampled)
    }

    fn roll_slow(&self) -> bool {
        let mut state = self.core.state();
        let permille = self.core.config.store.slow_permille;
        self.core.roll_fault(&mut state, permille)
    }

    /// Applies one decoded request and builds the answer.
    fn serve(&self, op: &Op) -> HttpResponse {
        let mut state = self.core.state();
        let bucket = state.buckets.entry(self.name.clone()).or_default();
        match op {
            Op::Put {
                key,
                body,
                precondition,
            } => serve_put(bucket, key, body, precondition.as_ref()),
            Op::Get { key, range } => serve_get(bucket, key, *range),
            Op::Head { key } => serve_head(bucket, key),
            Op::Delete { key } => {
                bucket.objects.remove(key);
                // 204 whether or not the key was there, which is what S3 does
                // and what makes delete idempotent.
                response(204, vec![], Bytes::new())
            }
            Op::List { prefix, after } => serve_list(bucket, prefix, after.as_deref()),
        }
    }
}

#[async_trait]
impl HttpTransport for SimBucket {
    async fn execute(&self, request: HttpRequest) -> Result<HttpResponse, TransportFailure> {
        let slow = self.roll_slow();
        self.spin(slow).await;

        let Some(op) = decode(&self.name, &request) else {
            // A request this endpoint cannot parse is a bug in the store, not
            // a fault, so it is reported as a status the store will not
            // mistake for anything retryable.
            return Ok(response(400, vec![], Bytes::new()));
        };

        let key = op.key();
        match self.choose_fault(request.method, key) {
            Some(StoreFault::RequestLost) => Err(TransportFailure {
                retryable: true,
                message: format!("{} {key}: the request never arrived", request.method),
            }),
            Some(StoreFault::Status(status)) => Ok(response(
                status,
                vec![],
                Bytes::from_static(
                    b"<Error><Code>SlowDown</Code><Message>simulated</Message></Error>",
                ),
            )),
            Some(StoreFault::ResponseLost) => {
                self.serve(&op);
                Err(TransportFailure {
                    retryable: true,
                    message: format!("{} {key}: the response was lost", request.method),
                })
            }
            Some(StoreFault::CrashNode(node)) => {
                self.core.crash_node(node);
                // Never answers. The caller's task belongs to the node that
                // just died, so it is dropped rather than left waiting; a
                // scenario that issues this from a task the crash does not own
                // will hang, and the driver reports that as a stuck run.
                std::future::pending().await
            }
            None => Ok(self.serve(&op)),
        }
    }
}

/// What one request is asking for, once the S3 wire form is undone.
enum Op {
    Put {
        key: String,
        body: Bytes,
        precondition: Option<Condition>,
    },
    Get {
        key: String,
        range: Option<(u64, u64)>,
    },
    Head {
        key: String,
    },
    Delete {
        key: String,
    },
    List {
        prefix: String,
        after: Option<String>,
    },
}

impl Op {
    fn key(&self) -> &str {
        match self {
            Op::Put { key, .. } | Op::Get { key, .. } | Op::Head { key } | Op::Delete { key } => {
                key
            }
            Op::List { prefix, .. } => prefix,
        }
    }
}

/// The precondition a conditional PUT carries.
enum Condition {
    NotExists,
    Match(String),
}

/// Turns a signed request back into the operation it means.
///
/// Returns `None` for anything this endpoint does not implement, which is the
/// honest answer: a real bucket would reject it too, and silently guessing
/// would let the store send something no bucket accepts.
fn decode(bucket: &str, request: &HttpRequest) -> Option<Op> {
    let rest = request.path.strip_prefix('/')?.strip_prefix(bucket)?;
    let key = match rest {
        "" | "/" => None,
        rest => Some(percent_decode(rest.strip_prefix('/')?)?),
    };

    match (request.method, key) {
        ("PUT", Some(key)) => {
            let precondition = match (request.header("if-none-match"), request.header("if-match")) {
                (Some("*"), _) => Some(Condition::NotExists),
                (_, Some(etag)) => Some(Condition::Match(etag.to_string())),
                _ => None,
            };
            Some(Op::Put {
                key,
                body: request.body.clone(),
                precondition,
            })
        }
        ("GET", Some(key)) => Some(Op::Get {
            key,
            range: request.header("range").and_then(parse_range),
        }),
        ("HEAD", Some(key)) => Some(Op::Head { key }),
        ("DELETE", Some(key)) => Some(Op::Delete { key }),
        ("GET", None) => {
            let query = |name: &str| {
                request
                    .query
                    .iter()
                    .find(|(k, _)| k == name)
                    .map(|(_, v)| v.clone())
            };
            if query("list-type").as_deref() != Some("2") {
                return None;
            }
            Some(Op::List {
                prefix: query("prefix").unwrap_or_default(),
                after: query("continuation-token"),
            })
        }
        _ => None,
    }
}

fn percent_decode(encoded: &str) -> Option<String> {
    percent_encoding::percent_decode_str(encoded)
        .decode_utf8()
        .ok()
        .map(|decoded| decoded.into_owned())
}

/// Reads `bytes=first-last`, which is the only form the store sends and the
/// only one this answers.
fn parse_range(header: &str) -> Option<(u64, u64)> {
    let (first, last) = header.trim().strip_prefix("bytes=")?.split_once('-')?;
    Some((first.parse().ok()?, last.parse().ok()?))
}

fn serve_put(
    bucket: &mut BucketState,
    key: &str,
    body: &Bytes,
    precondition: Option<&Condition>,
) -> HttpResponse {
    let current = bucket.objects.get(key).map(|object| object.etag.clone());
    let holds = match (precondition, &current) {
        (None, _) => true,
        (Some(Condition::NotExists), None) => true,
        (Some(Condition::Match(expected)), Some(actual)) => expected == actual,
        _ => false,
    };
    if !holds {
        return response(
            412,
            vec![],
            Bytes::from_static(
                b"<Error><Code>PreconditionFailed</Code><Message>simulated</Message></Error>",
            ),
        );
    }
    let etag = bucket.tag();
    bucket.objects.insert(
        key.to_string(),
        StoredObject {
            bytes: body.clone(),
            etag: etag.clone(),
        },
    );
    response(200, vec![("ETag".to_string(), etag)], Bytes::new())
}

fn serve_get(bucket: &BucketState, key: &str, range: Option<(u64, u64)>) -> HttpResponse {
    let Some(object) = bucket.objects.get(key) else {
        return not_found();
    };
    let headers = vec![("ETag".to_string(), object.etag.clone())];
    let Some((first, last)) = range else {
        return response(200, headers, object.bytes.clone());
    };
    let size = object.bytes.len() as u64;
    if first >= size || last < first {
        return response(
            416,
            headers,
            Bytes::from_static(
                b"<Error><Code>InvalidRange</Code><Message>simulated</Message></Error>",
            ),
        );
    }
    // A real server clamps the last byte to the end of the object rather than
    // refusing, and a store that assumed otherwise would break against it.
    let end = last.min(size - 1) + 1;
    response(
        206,
        headers,
        object.bytes.slice(first as usize..end as usize),
    )
}

fn serve_head(bucket: &BucketState, key: &str) -> HttpResponse {
    let Some(object) = bucket.objects.get(key) else {
        return not_found();
    };
    response(
        200,
        vec![
            ("ETag".to_string(), object.etag.clone()),
            ("Content-Length".to_string(), object.bytes.len().to_string()),
        ],
        Bytes::new(),
    )
}

fn serve_list(bucket: &BucketState, prefix: &str, after: Option<&str>) -> HttpResponse {
    let mut keys: Vec<&String> = bucket
        .objects
        .keys()
        .filter(|key| key.starts_with(prefix))
        .filter(|key| after.is_none_or(|token| key.as_str() > token))
        .collect();
    let truncated = keys.len() > LIST_PAGE_KEYS;
    keys.truncate(LIST_PAGE_KEYS);

    let mut body = String::from("<ListBucketResult>");
    body.push_str(&format!("<IsTruncated>{truncated}</IsTruncated>"));
    if truncated {
        if let Some(last) = keys.last() {
            body.push_str(&format!(
                "<NextContinuationToken>{}</NextContinuationToken>",
                escape(last)
            ));
        }
    }
    for key in keys {
        let object = &bucket.objects[key];
        body.push_str(&format!(
            "<Contents><Key>{}</Key><ETag>{}</ETag><Size>{}</Size></Contents>",
            escape(key),
            escape(&object.etag),
            object.bytes.len()
        ));
    }
    body.push_str("</ListBucketResult>");
    response(200, vec![], Bytes::from(body))
}

/// Escapes the characters a `ListBucketResult` must not carry raw.
///
/// Object keys may hold `&`, and the store's parser resolves entity references
/// on purpose, so emitting one unescaped would make this endpoint the only
/// thing in the system that cannot be listed correctly.
fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn not_found() -> HttpResponse {
    response(
        404,
        vec![],
        Bytes::from_static(b"<Error><Code>NoSuchKey</Code><Message>simulated</Message></Error>"),
    )
}

fn response(status: u16, headers: Vec<(String, String)>, body: Bytes) -> HttpResponse {
    HttpResponse {
        status,
        headers,
        body,
    }
}
