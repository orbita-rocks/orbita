//! The peer transport a production node runs on.
//!
//! `orbita_runtime::Transport` is a byte-level envelope rather than a stream,
//! so that the simulator can drop, delay, duplicate, and reorder whole
//! messages. This is the production side of that seam.
//!
//! # What is here and what is not
//!
//! Handler registration and local delivery are here, and they are what a
//! single-node cluster needs: a node that owns every partition never calls a
//! peer, and a call it makes to itself should not go through a socket.
//!
//! Dialling a remote peer is not here yet, so a call to another node reports
//! it as unreachable rather than pretending. That is deliberate honesty about
//! an unfinished piece: routing, proxying, and replication are all written
//! against this trait and are exercised under the simulator's transport, so
//! the remaining work is one implementation of `call` rather than anything
//! above it. See the crate docs for what that implementation needs.

use bytes::Bytes;
use orbita_core::NodeId;
use orbita_runtime::{
    PeerCall, PeerHandler, ServiceId, Transport, TransportError, TransportResult,
};

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

/// Peer messaging for a production node.
#[derive(Clone)]
pub struct PeerTransport {
    inner: Arc<Inner>,
}

struct Inner {
    local: NodeId,
    handlers: Mutex<HashMap<ServiceId, Arc<dyn ErasedHandler>>>,
}

impl PeerTransport {
    #[must_use]
    pub fn new(local: NodeId) -> Self {
        Self {
            inner: Arc::new(Inner {
                local,
                handlers: Mutex::new(HashMap::new()),
            }),
        }
    }

    fn handler(&self, service: ServiceId) -> Option<Arc<dyn ErasedHandler>> {
        self.inner
            .handlers
            .lock()
            .expect("peer handler registry poisoned")
            .get(&service)
            .cloned()
    }
}

impl Transport for PeerTransport {
    async fn call(&self, to: NodeId, call: PeerCall) -> TransportResult<Bytes> {
        if to != self.inner.local {
            return Err(TransportError::Unreachable(to));
        }
        // A node talks to itself whenever it holds both ends of a
        // conversation, such as a partition it owns and replicates to nobody.
        // Short-circuiting keeps that from needing a loopback socket.
        let Some(handler) = self.handler(call.service) else {
            return Err(TransportError::NoHandler(call.service));
        };
        handler.handle_boxed(self.inner.local, call).await
    }

    fn register(&self, service: ServiceId, handler: impl PeerHandler) {
        self.inner
            .handlers
            .lock()
            .expect("peer handler registry poisoned")
            .insert(service, Arc::new(Erased(handler)));
    }

    fn local_node(&self) -> NodeId {
        self.inner.local
    }
}

/// The boxed-future adapter every transport with a handler registry needs,
/// because `PeerHandler` returns `impl Future` and so is not dyn-compatible.
trait ErasedHandler: Send + Sync + 'static {
    fn handle_boxed<'a>(
        &'a self,
        from: NodeId,
        call: PeerCall,
    ) -> Pin<Box<dyn Future<Output = TransportResult<Bytes>> + Send + 'a>>;
}

struct Erased<H>(H);

impl<H: PeerHandler> ErasedHandler for Erased<H> {
    fn handle_boxed<'a>(
        &'a self,
        from: NodeId,
        call: PeerCall,
    ) -> Pin<Box<dyn Future<Output = TransportResult<Bytes>> + Send + 'a>> {
        Box::pin(self.0.handle(from, call))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Echo;

    impl PeerHandler for Echo {
        async fn handle(&self, _from: NodeId, call: PeerCall) -> TransportResult<Bytes> {
            Ok(call.payload)
        }
    }

    fn call(payload: &'static [u8]) -> PeerCall {
        PeerCall {
            service: ServiceId::Proxy,
            method: 1,
            payload: Bytes::from_static(payload),
        }
    }

    #[tokio::test]
    async fn a_node_can_call_itself_without_a_socket() {
        let transport = PeerTransport::new(NodeId(1));
        transport.register(ServiceId::Proxy, Echo);
        assert_eq!(
            transport.call(NodeId(1), call(b"hello")).await,
            Ok(Bytes::from_static(b"hello"))
        );
    }

    #[tokio::test]
    async fn a_call_to_a_service_nobody_serves_is_reported_as_such() {
        let transport = PeerTransport::new(NodeId(1));
        assert_eq!(
            transport.call(NodeId(1), call(b"")).await,
            Err(TransportError::NoHandler(ServiceId::Proxy))
        );
    }

    #[tokio::test]
    async fn a_remote_peer_is_reported_unreachable_rather_than_silently_dropped() {
        let transport = PeerTransport::new(NodeId(1));
        transport.register(ServiceId::Proxy, Echo);
        assert_eq!(
            transport.call(NodeId(2), call(b"")).await,
            Err(TransportError::Unreachable(NodeId(2)))
        );
    }
}
