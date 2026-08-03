//! Node-to-node messaging.
//!
//! # Why this is a byte-level envelope
//!
//! The simulator intercepts peer communication here, at the level of whole
//! request and response messages, rather than at the level of a TCP byte
//! stream. Simulating a socket faithfully enough to run gRPC over it is a
//! large amount of work that tests the HTTP/2 stack rather than Orbita, and
//! the failures worth exploring, meaning dropped, delayed, duplicated, and
//! reordered messages plus partitions, are all expressible at this level.
//!
//! Requests carry opaque bytes and a service identifier so that this crate
//! does not need to know the WAL's message types or the control plane's. Each
//! subsystem defines and encodes its own, which is what lets those crates be
//! built independently of each other.
//!
//! Client-facing gRPC does not go through here. That is a real network edge
//! served by tonic; the simulator drives the server through its request types
//! directly instead.

use orbita_core::NodeId;

use bytes::Bytes;
use std::future::Future;

/// Which subsystem a call belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum ServiceId {
    /// WAL replication between a partition owner and its replicas.
    Wal = 1,
    /// Raft traffic within the leader group.
    Raft = 2,
    /// Requests a node proxies to a partition owner on a client's behalf.
    Proxy = 3,
    /// Heartbeats, partition map gossip, and other control plane chatter.
    Control = 4,
}

/// One request to one peer.
#[derive(Debug, Clone)]
pub struct PeerCall {
    pub service: ServiceId,
    /// Method discriminant, defined by the owning subsystem.
    pub method: u16,
    pub payload: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TransportError {
    /// The peer did not answer in time. Note that this says nothing about
    /// whether it processed the request, which is why every peer request must
    /// be idempotent.
    #[error("timed out calling node {0}")]
    Timeout(NodeId),

    #[error("node {0} unreachable")]
    Unreachable(NodeId),

    #[error("node {0} is not known to this cluster")]
    UnknownPeer(NodeId),

    #[error("peer returned an error: {0}")]
    Remote(String),

    #[error("no handler registered for {0:?}")]
    NoHandler(ServiceId),
}

pub type TransportResult<T> = Result<T, TransportError>;

/// Sends requests to peers.
pub trait Transport: Clone + Send + Sync + 'static {
    fn call(
        &self,
        to: NodeId,
        call: PeerCall,
    ) -> impl Future<Output = TransportResult<Bytes>> + Send;

    /// Registers the handler for one service on this node.
    ///
    /// Handlers are registered per service so that the WAL and the control
    /// plane can be developed and tested in isolation.
    fn register(&self, service: ServiceId, handler: impl PeerHandler);

    fn local_node(&self) -> NodeId;
}

/// Handles inbound peer requests for one service.
///
/// Note that this returns `impl Future` and so is not dyn-compatible, which
/// means a transport that keeps handlers in a registry has to erase them
/// behind its own boxed-future adapter. That is a few lines in each transport
/// and it keeps the hot path unboxed for everyone calling through it, which is
/// the trade the rest of this crate makes too.
pub trait PeerHandler: Send + Sync + 'static {
    fn handle(
        &self,
        from: NodeId,
        call: PeerCall,
    ) -> impl Future<Output = TransportResult<Bytes>> + Send;
}
