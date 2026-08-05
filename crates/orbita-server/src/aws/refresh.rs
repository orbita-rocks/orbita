//! Serving a credential that expires, without a background task.
//!
//! An instance-profile or assumed-role credential lasts an hour or so and then
//! stops working. Fetching one per request would put the metadata service on
//! the critical path of every object read; fetching one at startup would take
//! the node down an hour later. So the credential is cached and replaced
//! before it expires.
//!
//! # Why no timer
//!
//! The obvious shape is a spawned task that sleeps until the credential is
//! nearly due and refreshes it. This does not do that, for two reasons. A
//! spawned task is a lifetime to manage and a thing to leak on shutdown, and,
//! more importantly, under deterministic simulation a background timer makes
//! credential refresh an event that happens *between* the operations a test
//! can see, rather than a consequence of one. Every fetch here is caused by a
//! request, which means a simulated run can place an expiry or an IMDS failure
//! exactly where it wants it.
//!
//! # Why a refresh does not stall traffic
//!
//! Refresh starts [`REFRESH_MARGIN`] before expiry, while the cached
//! credential is still perfectly good. Exactly one caller wins the refresh
//! lock and pays the round trip; every other caller sees the lock taken, finds
//! the cached credential still valid, and signs with it immediately. Requests
//! already in flight are untouched — they were signed before any of this. Only
//! a caller that finds *no* usable credential at all waits, and by then there
//! is nothing to serve it anyway.
//!
//! The same property covers a failing metadata service: if the fetch fails
//! while the cached credential is still valid, the cached one is returned and
//! the failure is logged. A five-minute IMDS outage is survivable; turning it
//! into an immediate write failure is not.

use super::{SessionCredentials, SessionSource};

use async_trait::async_trait;
use orbita_objectstore::s3::{Credentials, CredentialsProvider};
use orbita_objectstore::{ObjectError, ObjectResult};
use orbita_runtime::Clock;

use std::sync::RwLock;
use std::time::Duration;

/// How long before expiry a refresh is attempted.
///
/// AWS reissues instance-profile credentials well before they lapse, and five
/// minutes is long enough that a metadata service having a bad minute, or a
/// throttled `AssumeRole`, has many chances to succeed before anything the
/// node signs stops working.
pub(crate) const REFRESH_MARGIN: Duration = Duration::from_secs(300);

/// How much validity a credential needs left to be worth signing with.
///
/// A request signed with a credential that expires while it is on the wire
/// fails, and object storage requests are not always fast. Thirty seconds is
/// the floor below which serving the cached credential stops being a kindness.
pub(crate) const MINIMUM_VALIDITY: Duration = Duration::from_secs(30);

/// Caches one session credential and replaces it before it expires.
pub(crate) struct RefreshingCredentials<C: Clock, S: SessionSource> {
    clock: C,
    source: S,
    cached: RwLock<Option<SessionCredentials>>,
    /// Held for the duration of one fetch, so a burst of requests arriving at
    /// the refresh boundary produces one call to the metadata service and not
    /// one per request.
    refreshing: tokio::sync::Mutex<()>,
}

impl<C: Clock, S: SessionSource> RefreshingCredentials<C, S> {
    pub(crate) fn new(clock: C, source: S) -> Self {
        Self {
            clock,
            source,
            cached: RwLock::new(None),
            refreshing: tokio::sync::Mutex::new(()),
        }
    }

    /// The cached credential if it has more than `margin` of life left.
    ///
    /// A credential with no expiry is always usable, which is what makes a
    /// static base credential legal underneath this without a special case.
    fn cached_with(&self, now_millis: u64, margin: Duration) -> Option<Credentials> {
        let cached = self
            .cached
            .read()
            .expect("credential cache is not poisoned");
        let session = cached.as_ref()?;
        match session.expires_at_millis {
            None => Some(session.credentials.clone()),
            Some(expiry) if now_millis.saturating_add(margin.as_millis() as u64) < expiry => {
                Some(session.credentials.clone())
            }
            Some(_) => None,
        }
    }

    fn store(&self, session: SessionCredentials) {
        *self
            .cached
            .write()
            .expect("credential cache is not poisoned") = Some(session);
    }
}

#[async_trait]
impl<C: Clock, S: SessionSource> CredentialsProvider for RefreshingCredentials<C, S> {
    async fn credentials(&self) -> ObjectResult<Credentials> {
        // The common case: a credential with plenty of life left, served
        // without touching the refresh lock at all.
        if let Some(credentials) = self.cached_with(self.clock.now_millis(), REFRESH_MARGIN) {
            return Ok(credentials);
        }

        let guard = match self.refreshing.try_lock() {
            Ok(guard) => guard,
            Err(_) => {
                // Somebody else is already fetching. If what we have is still
                // signable, use it rather than queueing behind them.
                if let Some(credentials) =
                    self.cached_with(self.clock.now_millis(), MINIMUM_VALIDITY)
                {
                    return Ok(credentials);
                }
                self.refreshing.lock().await
            }
        };

        // Re-check under the lock: whoever we waited for may have filled the
        // cache, in which case there is nothing left to do.
        if let Some(credentials) = self.cached_with(self.clock.now_millis(), REFRESH_MARGIN) {
            return Ok(credentials);
        }

        match self.source.fetch().await {
            Ok(session) => {
                let credentials = session.credentials.clone();
                self.store(session);
                drop(guard);
                Ok(credentials)
            }
            Err(error) => {
                drop(guard);
                match self.cached_with(self.clock.now_millis(), MINIMUM_VALIDITY) {
                    Some(credentials) => {
                        // Deliberately not an error: the node can still sign,
                        // and there is time for the next request to try again.
                        // The message names the source, never the credential.
                        tracing::warn!(
                            source = %self.source.describe(),
                            error = %error,
                            "refreshing AWS credentials failed; continuing with the cached \
                             credential until it expires"
                        );
                        Ok(credentials)
                    }
                    None => Err(ObjectError::AccessDenied(format!(
                        "no usable AWS credential from {}: {error}",
                        self.source.describe()
                    ))),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    /// A clock whose wall time only moves when a test moves it.
    #[derive(Clone)]
    struct TestClock {
        millis: Arc<AtomicU64>,
    }

    impl TestClock {
        fn at(millis: u64) -> Self {
            Self {
                millis: Arc::new(AtomicU64::new(millis)),
            }
        }

        fn advance(&self, by: Duration) {
            self.millis
                .fetch_add(by.as_millis() as u64, Ordering::SeqCst);
        }
    }

    impl Clock for TestClock {
        fn now_millis(&self) -> u64 {
            self.millis.load(Ordering::SeqCst)
        }

        fn monotonic_nanos(&self) -> u64 {
            self.now_millis() * 1_000_000
        }

        fn sleep(&self, _duration: Duration) -> impl Future<Output = ()> + Send {
            std::future::ready(())
        }
    }

    /// A source that hands out numbered credentials and can be told to fail.
    struct ScriptedSource {
        fetches: AtomicU64,
        lifetime: Duration,
        clock: TestClock,
        fail: Arc<std::sync::atomic::AtomicBool>,
    }

    impl ScriptedSource {
        fn new(clock: TestClock, lifetime: Duration) -> Self {
            Self {
                fetches: AtomicU64::new(0),
                lifetime,
                clock,
                fail: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            }
        }
    }

    #[async_trait]
    impl SessionSource for ScriptedSource {
        async fn fetch(&self) -> ObjectResult<SessionCredentials> {
            if self.fail.load(Ordering::SeqCst) {
                return Err(ObjectError::Transient(
                    "metadata service is down".to_string(),
                ));
            }
            let n = self.fetches.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(SessionCredentials {
                credentials: Credentials {
                    access_key_id: format!("AKID{n}"),
                    secret_access_key: "secret".to_string(),
                    session_token: Some(format!("token{n}")),
                },
                expires_at_millis: Some(self.clock.now_millis() + self.lifetime.as_millis() as u64),
            })
        }

        fn describe(&self) -> String {
            "the test source".to_string()
        }
    }

    fn provider(
        clock: TestClock,
        lifetime: Duration,
    ) -> RefreshingCredentials<TestClock, ScriptedSource> {
        let source = ScriptedSource::new(clock.clone(), lifetime);
        RefreshingCredentials::new(clock, source)
    }

    #[tokio::test]
    async fn the_first_request_sources_a_credential() {
        let clock = TestClock::at(1_000_000);
        let provider = provider(clock, Duration::from_secs(3600));
        let credentials = provider.credentials().await.expect("sourced");
        assert_eq!(credentials.access_key_id, "AKID1");
    }

    #[tokio::test]
    async fn a_valid_credential_is_reused_rather_than_refetched() {
        let clock = TestClock::at(1_000_000);
        let provider = provider(clock.clone(), Duration::from_secs(3600));
        provider.credentials().await.expect("sourced");
        clock.advance(Duration::from_secs(600));
        let credentials = provider.credentials().await.expect("cached");
        assert_eq!(
            credentials.access_key_id, "AKID1",
            "a credential with an hour left must not cost a round trip"
        );
        assert_eq!(provider.source.fetches.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn session_credentials_refresh_before_expiry() {
        let clock = TestClock::at(1_000_000);
        let provider = provider(clock.clone(), Duration::from_secs(3600));
        provider.credentials().await.expect("sourced");

        // Inside the refresh margin but still valid: the credential must have
        // been replaced before it could ever be used past its deadline.
        clock.advance(Duration::from_secs(3600) - REFRESH_MARGIN);
        let credentials = provider.credentials().await.expect("refreshed");
        assert_eq!(credentials.access_key_id, "AKID2");
        assert_eq!(provider.source.fetches.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn an_expired_credential_is_never_handed_out() {
        let clock = TestClock::at(1_000_000);
        let provider = provider(clock.clone(), Duration::from_secs(3600));
        provider.credentials().await.expect("sourced");
        clock.advance(Duration::from_secs(3600));
        provider.source.fail.store(true, Ordering::SeqCst);

        let result = provider.credentials().await;
        assert!(
            matches!(result, Err(ObjectError::AccessDenied(_))),
            "an expired credential must surface as a failure, not be signed with: {result:?}"
        );
    }

    #[tokio::test]
    async fn a_failed_refresh_keeps_serving_the_still_valid_credential() {
        let clock = TestClock::at(1_000_000);
        let provider = provider(clock.clone(), Duration::from_secs(3600));
        provider.credentials().await.expect("sourced");

        clock.advance(Duration::from_secs(3600) - REFRESH_MARGIN);
        provider.source.fail.store(true, Ordering::SeqCst);
        let credentials = provider
            .credentials()
            .await
            .expect("the cached credential is still good");
        assert_eq!(
            credentials.access_key_id, "AKID1",
            "a metadata service outage inside the refresh margin must not fail a write"
        );
    }

    #[tokio::test]
    async fn imds_failure_does_not_drop_in_flight_requests() {
        // "In flight" means a request that resolved its credential before the
        // metadata service broke. Its credential is a value it already holds,
        // so nothing the provider does afterwards can take it away, and the
        // provider must not start refusing while that credential is valid.
        let clock = TestClock::at(1_000_000);
        let provider = provider(clock.clone(), Duration::from_secs(3600));
        let in_flight = provider.credentials().await.expect("sourced");

        provider.source.fail.store(true, Ordering::SeqCst);
        clock.advance(Duration::from_secs(3600) - REFRESH_MARGIN);

        for _ in 0..5 {
            let served = provider.credentials().await.expect("served from cache");
            assert_eq!(served.access_key_id, in_flight.access_key_id);
        }
    }

    #[tokio::test]
    async fn concurrent_requests_at_the_refresh_boundary_cause_one_fetch() {
        let clock = TestClock::at(1_000_000);
        let provider = Arc::new(provider(clock.clone(), Duration::from_secs(3600)));
        provider.credentials().await.expect("sourced");
        clock.advance(Duration::from_secs(3600) - REFRESH_MARGIN);

        let mut tasks = Vec::new();
        for _ in 0..8 {
            let provider = provider.clone();
            tasks.push(tokio::spawn(async move {
                provider.credentials().await.expect("served")
            }));
        }
        for task in tasks {
            task.await.expect("no panic");
        }

        assert_eq!(
            provider.source.fetches.load(Ordering::SeqCst),
            2,
            "a burst at the refresh boundary must not stampede the metadata service"
        );
    }

    #[tokio::test]
    async fn a_credential_without_an_expiry_is_sourced_once_and_kept() {
        struct Eternal;

        #[async_trait]
        impl SessionSource for Eternal {
            async fn fetch(&self) -> ObjectResult<SessionCredentials> {
                Ok(SessionCredentials {
                    credentials: Credentials {
                        access_key_id: "STATIC".to_string(),
                        secret_access_key: "secret".to_string(),
                        session_token: None,
                    },
                    expires_at_millis: None,
                })
            }

            fn describe(&self) -> String {
                "a static key pair".to_string()
            }
        }

        let clock = TestClock::at(0);
        let provider = RefreshingCredentials::new(clock.clone(), Eternal);
        provider.credentials().await.expect("sourced");
        clock.advance(Duration::from_secs(86_400 * 365));
        assert_eq!(
            provider
                .credentials()
                .await
                .expect("still good")
                .access_key_id,
            "STATIC"
        );
    }
}
