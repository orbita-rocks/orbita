//! A scripted [`HttpTransport`] shared by the credential provider tests.
//!
//! It is the same shape a simulated transport will take: a fixed script of
//! answers, and a record of what was asked. Nothing here retries or sleeps, so
//! an injected failure happens exactly once and the test can assert on the
//! request that caused it.

use async_trait::async_trait;
use orbita_objectstore::s3::{HttpRequest, HttpResponse, HttpTransport, TransportFailure};
use std::sync::{Arc, Mutex};

pub(crate) struct ScriptedTransport {
    requests: Mutex<Vec<HttpRequest>>,
    responses: Mutex<Vec<Result<HttpResponse, TransportFailure>>>,
}

impl ScriptedTransport {
    pub(crate) fn new(responses: Vec<Result<HttpResponse, TransportFailure>>) -> Arc<Self> {
        Arc::new(Self {
            requests: Mutex::new(Vec::new()),
            // Stored reversed so playback can pop from the back.
            responses: Mutex::new(responses.into_iter().rev().collect()),
        })
    }

    pub(crate) fn requests(&self) -> Vec<HttpRequest> {
        self.requests.lock().expect("not poisoned").clone()
    }
}

#[async_trait]
impl HttpTransport for ScriptedTransport {
    async fn execute(&self, request: HttpRequest) -> Result<HttpResponse, TransportFailure> {
        self.requests.lock().expect("not poisoned").push(request);
        self.responses
            .lock()
            .expect("not poisoned")
            .pop()
            .expect("the test script ran out of responses")
    }
}
