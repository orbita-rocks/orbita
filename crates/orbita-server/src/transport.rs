//! The peer transport a production node runs on.
//!
//! `orbita_runtime::Transport` is a byte-level envelope rather than a stream,
//! so that the simulator can drop, delay, duplicate, and reorder whole
//! messages. This is the production side of that seam: a private
//! length-prefixed protocol over its own TCP listener, separate from the
//! client gRPC port, per
//! [ADR 0004](../../../docs/adr/0004-peer-traffic-uses-private-framing.md).
//! The frame layout lives in [`crate::frame`].
//!
//! # Why there is a pool rather than a connection per call
//!
//! A partition owner talks to its replicas on every write, so a handshake per
//! call would put a round trip in front of every round trip. One connection
//! per peer is kept open instead, and the request id in the frame is what lets
//! many calls share it: replies come back in whatever order the peer produced
//! them and are matched to their waiters by id rather than by arrival.
//!
//! A connection that fails is dropped from the pool and redialled on the next
//! call. Every waiter on a broken connection is failed rather than left
//! hanging, because a peer request that never resolves is worse than one that
//! reports a failure: every caller here has a retry, and none of them has a
//! way out of waiting forever.
//!
//! # Timeouts
//!
//! Every call is bounded. A timeout says nothing about whether the peer did
//! the work, which is why `orbita_runtime::TransportError::Timeout` documents
//! that peer requests must be idempotent, and both the WAL append path and the
//! proxy path are.
//!
//! # Where the runtime seam is
//!
//! This module is the production implementation of the seam itself, so it is
//! one of the two places in the crate that touch sockets and spawn tasks
//! directly; the other is the gRPC listener in the crate root. Everything
//! above the trait keeps going through the runtime and keeps running unchanged
//! under the simulator.

use crate::frame::{
    frame_length, FrameError, Request, Response, MAX_FRAME_BYTES, STATUS_ERROR, STATUS_NO_HANDLER,
    STATUS_OK,
};

use bytes::Bytes;
use orbita_core::NodeId;
use orbita_runtime::tokio_runtime::TokioClock;
use orbita_runtime::{
    timeout, PeerCall, PeerHandler, ServiceId, Transport, TransportError, TransportResult,
};

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};

/// How long a peer call waits before it is reported as timed out.
///
/// This is a backstop for a peer that has accepted a connection and then
/// stopped answering, which TCP does not report on its own. It is well inside
/// the control plane's three second death declaration, so a caller notices a
/// stalled node before the cluster acts on it.
pub const DEFAULT_PEER_CALL_TIMEOUT: Duration = Duration::from_secs(2);

/// How long dialling a peer may take.
const DIAL_TIMEOUT: Duration = Duration::from_secs(2);

/// Who a handler is told called it.
///
/// The private framing carries no sender identity, because nothing in the
/// protocol reads one: every handler in the workspace authorises by epoch and
/// by partition ownership rather than by who is speaking. Rather than invent
/// an identity a peer could claim falsely, inbound calls are labelled
/// unidentified and the fact is stated here.
const UNIDENTIFIED_PEER: NodeId = NodeId(0);

/// Peer messaging for a production node.
#[derive(Clone)]
pub struct PeerTransport {
    inner: Arc<Inner>,
}

struct Inner {
    local: NodeId,
    clock: TokioClock,
    call_timeout: Duration,
    handlers: Mutex<HashMap<ServiceId, RegisteredHandler>>,
    service_executors: Mutex<HashMap<ServiceId, tokio::runtime::Handle>>,
    /// Where each peer is reached. Configuration rather than discovery,
    /// because discovering a peer's address requires asking something, and the
    /// thing to ask is reached over this transport.
    directory: Mutex<HashMap<NodeId, String>>,
    peers: Mutex<HashMap<NodeId, Arc<PeerSlot>>>,
    next_request_id: AtomicU64,
}

#[derive(Clone)]
struct RegisteredHandler {
    handler: Arc<dyn ErasedHandler>,
    executor: tokio::runtime::Handle,
}

/// One peer's connection, behind its own lock so that dialling a slow peer
/// does not hold up a call to a healthy one.
#[derive(Default)]
struct PeerSlot {
    connection: tokio::sync::Mutex<Option<Arc<Connection>>>,
}

impl PeerTransport {
    #[must_use]
    pub fn new(local: NodeId) -> Self {
        Self::with_timeout(local, DEFAULT_PEER_CALL_TIMEOUT)
    }

    #[must_use]
    pub fn with_timeout(local: NodeId, call_timeout: Duration) -> Self {
        Self {
            inner: Arc::new(Inner {
                local,
                clock: TokioClock::new(),
                call_timeout,
                handlers: Mutex::new(HashMap::new()),
                service_executors: Mutex::new(HashMap::new()),
                directory: Mutex::new(HashMap::new()),
                peers: Mutex::new(HashMap::new()),
                next_request_id: AtomicU64::new(1),
            }),
        }
    }

    /// Tells this node where a peer is.
    ///
    /// Addresses can be replaced while the node is running, because a peer
    /// that moves should not need a restart here. Any existing connection is
    /// dropped so that the next call dials the new address.
    pub fn set_peer(&self, node: NodeId, address: impl Into<String>) {
        let address = address.into();
        let changed = {
            let mut directory = self
                .inner
                .directory
                .lock()
                .expect("peer directory poisoned");
            directory.insert(node, address.clone()).as_deref() != Some(address.as_str())
        };
        if changed {
            self.inner
                .peers
                .lock()
                .expect("peer connections poisoned")
                .remove(&node);
        }
    }

    /// Starts serving inbound peer traffic.
    ///
    /// Returns once the socket is bound, so a caller that holds one of these
    /// can tell its peers where to find it.
    pub async fn listen(&self, addr: SocketAddr) -> std::io::Result<PeerListener> {
        self.listen_on(addr, tokio::runtime::Handle::current())
            .await
    }

    /// Binds and serves the shared peer socket on `executor`.
    ///
    /// The listener and frame reader live there so worker saturation cannot
    /// stop Raft heartbeats at the socket. Individual services are dispatched
    /// to the handle captured at registration, keeping WAL and proxy handlers
    /// on the worker runtime.
    pub async fn listen_on(
        &self,
        addr: SocketAddr,
        executor: tokio::runtime::Handle,
    ) -> std::io::Result<PeerListener> {
        let (bound, listening) = oneshot::channel();
        let transport = self.clone();
        let task = executor.spawn(async move {
            let listener = match TcpListener::bind(addr).await {
                Ok(listener) => listener,
                Err(error) => {
                    let _ = bound.send(Err(error));
                    return;
                }
            };
            let local_addr = match listener.local_addr() {
                Ok(local_addr) => local_addr,
                Err(error) => {
                    let _ = bound.send(Err(error));
                    return;
                }
            };
            let (shutdown, mut stop) = oneshot::channel();
            let (connections, _) = tokio::sync::watch::channel(false);
            let connection_shutdown = connections.clone();
            if bound.send(Ok((local_addr, shutdown, connections))).is_err() {
                return;
            }
            loop {
                tokio::select! {
                    _ = &mut stop => break,
                    accepted = listener.accept() => match accepted {
                        Ok((stream, from)) => {
                            let transport = transport.clone();
                            let stop = connection_shutdown.subscribe();
                            tokio::spawn(async move { serve(transport, stream, from, stop).await });
                        }
                        Err(error) => {
                            tracing::warn!(%error, "accepting a peer connection failed");
                        }
                    },
                }
            }
        });
        let (local_addr, shutdown, connections) = listening.await.map_err(|_| {
            std::io::Error::other("peer listener executor stopped before binding")
        })??;
        tracing::info!(node = self.inner.local.get(), %local_addr, "serving peer traffic");
        Ok(PeerListener {
            local_addr,
            shutdown,
            connections,
            task,
        })
    }

    pub(crate) fn route_service(&self, service: ServiceId, executor: tokio::runtime::Handle) {
        self.inner
            .service_executors
            .lock()
            .expect("peer service executors poisoned")
            .insert(service, executor);
    }

    fn handler(&self, service: ServiceId) -> Option<RegisteredHandler> {
        self.inner
            .handlers
            .lock()
            .expect("peer handler registry poisoned")
            .get(&service)
            .cloned()
    }

    fn address(&self, node: NodeId) -> Option<String> {
        self.inner
            .directory
            .lock()
            .expect("peer directory poisoned")
            .get(&node)
            .cloned()
    }

    fn slot(&self, node: NodeId) -> Arc<PeerSlot> {
        Arc::clone(
            self.inner
                .peers
                .lock()
                .expect("peer connections poisoned")
                .entry(node)
                .or_default(),
        )
    }

    /// A live connection to `node`, dialling if there is not one already.
    ///
    /// `stale` names a connection the caller has just found broken, so that
    /// two callers racing on the same failure do not each throw away the
    /// other's fresh connection.
    async fn connection(
        &self,
        node: NodeId,
        slot: &PeerSlot,
        stale: Option<&Arc<Connection>>,
    ) -> TransportResult<Arc<Connection>> {
        let mut held = slot.connection.lock().await;
        if let Some(existing) = held.as_ref() {
            let superseded = stale.is_some_and(|s| Arc::ptr_eq(s, existing));
            if !existing.is_closed() && !superseded {
                return Ok(Arc::clone(existing));
            }
            *held = None;
        }

        let address = self
            .address(node)
            .ok_or(TransportError::UnknownPeer(node))?;
        let dialled = timeout(
            &self.inner.clock,
            DIAL_TIMEOUT,
            TcpStream::connect(&address),
        )
        .await
        .map_err(|_| TransportError::Timeout(node))?
        .map_err(|error| {
            tracing::debug!(node = node.get(), %address, %error, "dialling a peer failed");
            TransportError::Unreachable(node)
        })?;

        let connection = Connection::start(dialled, node);
        *held = Some(Arc::clone(&connection));
        Ok(connection)
    }

    async fn call_local(&self, call: PeerCall) -> TransportResult<Bytes> {
        // A node talks to itself whenever it holds both ends of a
        // conversation, such as a partition it owns and replicates to nobody.
        // Short-circuiting keeps that from needing a loopback socket.
        let Some(handler) = self.handler(call.service) else {
            return Err(TransportError::NoHandler(call.service));
        };
        let local = self.inner.local;
        let erased = Arc::clone(&handler.handler);
        let (reply, result) = oneshot::channel();
        handler.executor.spawn(async move {
            let _ = reply.send(erased.handle_boxed(local, call).await);
        });
        result
            .await
            .unwrap_or(Err(TransportError::Unreachable(local)))
    }

    async fn call_remote(&self, to: NodeId, call: PeerCall) -> TransportResult<Bytes> {
        let slot = self.slot(to);
        let mut stale: Option<Arc<Connection>> = None;

        // Two attempts, because the common failure is a pooled connection the
        // peer closed while it was idle, and that is indistinguishable from a
        // dead peer until a second dial fails too.
        for attempt in 0..2 {
            let connection = self.connection(to, &slot, stale.as_ref()).await?;
            let request_id = self.inner.next_request_id.fetch_add(1, Ordering::Relaxed);
            let request = Request {
                service: call.service,
                method: call.method,
                request_id,
                payload: call.payload.clone(),
            };

            let waiting = match connection.send(request) {
                Ok(waiting) => waiting,
                Err(()) => {
                    stale = Some(connection);
                    continue;
                }
            };

            match timeout(&self.inner.clock, self.inner.call_timeout, waiting).await {
                Ok(Ok(response)) => return status_of(to, call.service, response),
                // The connection went away while this call was on it. Whether
                // the peer saw the request is unknown, which is what makes
                // idempotence a requirement of every peer request rather than
                // a nicety.
                Ok(Err(_)) => {
                    connection.discard(request_id);
                    if attempt == 1 {
                        return Err(TransportError::Unreachable(to));
                    }
                    stale = Some(connection);
                }
                Err(_) => {
                    connection.discard(request_id);
                    return Err(TransportError::Timeout(to));
                }
            }
        }
        Err(TransportError::Unreachable(to))
    }
}

impl Transport for PeerTransport {
    async fn call(&self, to: NodeId, call: PeerCall) -> TransportResult<Bytes> {
        if to == self.inner.local {
            return self.call_local(call).await;
        }
        self.call_remote(to, call).await
    }

    fn register(&self, service: ServiceId, handler: impl PeerHandler) {
        let executor = self
            .inner
            .service_executors
            .lock()
            .expect("peer service executors poisoned")
            .get(&service)
            .cloned()
            .unwrap_or_else(tokio::runtime::Handle::current);
        self.inner
            .handlers
            .lock()
            .expect("peer handler registry poisoned")
            .insert(
                service,
                RegisteredHandler {
                    handler: Arc::new(Erased(handler)),
                    executor,
                },
            );
    }

    fn local_node(&self) -> NodeId {
        self.inner.local
    }
}

/// A bound peer listener.
///
/// Holding one means the node is reachable by its peers. Dropping it stops the
/// accept loop, and [`PeerListener::shutdown`] waits for it to finish so a
/// test can rebind the port.
pub struct PeerListener {
    local_addr: SocketAddr,
    shutdown: oneshot::Sender<()>,
    connections: tokio::sync::watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl PeerListener {
    /// What was actually bound, which is what a node advertises to its peers.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn shutdown(self) {
        let _ = self.connections.send(true);
        let _ = self.shutdown.send(());
        let _ = self.task.await;
    }
}

/// One pooled connection to one peer.
struct Connection {
    outbound: mpsc::UnboundedSender<Bytes>,
    inflight: Arc<Mutex<Inflight>>,
}

#[derive(Default)]
struct Inflight {
    waiting: HashMap<u64, oneshot::Sender<Response>>,
    closed: bool,
}

impl Inflight {
    /// Fails everything still waiting, which is what turns a broken socket
    /// into an error at each caller rather than a set of futures that never
    /// resolve.
    fn close(&mut self) {
        self.closed = true;
        self.waiting.clear();
    }
}

impl Connection {
    fn start(stream: TcpStream, node: NodeId) -> Arc<Self> {
        // Replication is a stream of small latency-critical messages, so
        // waiting to coalesce them is exactly the wrong trade.
        if let Err(error) = stream.set_nodelay(true) {
            tracing::debug!(node = node.get(), %error, "could not disable Nagle on a peer connection");
        }
        let (mut reader, mut writer) = tokio::io::split(stream);
        let (outbound, mut queue) = mpsc::unbounded_channel::<Bytes>();
        let inflight: Arc<Mutex<Inflight>> = Arc::default();

        let writing = Arc::clone(&inflight);
        tokio::spawn(async move {
            while let Some(frame) = queue.recv().await {
                if let Err(error) = writer.write_all(&frame).await {
                    tracing::debug!(node = node.get(), %error, "writing to a peer failed");
                    break;
                }
            }
            // Closing the write half tells the peer we are done, which is what
            // makes its own read loop exit rather than wait.
            let _ = writer.shutdown().await;
            writing.lock().expect("inflight poisoned").close();
        });

        let reading = Arc::clone(&inflight);
        tokio::spawn(async move {
            loop {
                match read_frame(&mut reader).await {
                    Ok(Some(body)) => match Response::decode(&body) {
                        Ok(response) => {
                            let waiter = reading
                                .lock()
                                .expect("inflight poisoned")
                                .waiting
                                .remove(&response.request_id);
                            // A reply with no waiter is one whose caller timed
                            // out or gave up. Dropping it is the whole of the
                            // handling that needs.
                            if let Some(waiter) = waiter {
                                let _ = waiter.send(response);
                            }
                        }
                        Err(error) => {
                            tracing::warn!(node = node.get(), %error, "undecodable peer response");
                            break;
                        }
                    },
                    Ok(None) => break,
                    Err(error) => {
                        tracing::debug!(node = node.get(), %error, "reading from a peer failed");
                        break;
                    }
                }
            }
            reading.lock().expect("inflight poisoned").close();
        });

        Arc::new(Self { outbound, inflight })
    }

    fn is_closed(&self) -> bool {
        self.inflight.lock().expect("inflight poisoned").closed
    }

    /// Queues a request and hands back what its reply will arrive on.
    ///
    /// The waiter is registered before the frame is queued, so a reply cannot
    /// arrive before there is somewhere to put it.
    fn send(&self, request: Request) -> Result<oneshot::Receiver<Response>, ()> {
        let (tell, hear) = oneshot::channel();
        {
            let mut inflight = self.inflight.lock().expect("inflight poisoned");
            if inflight.closed {
                return Err(());
            }
            inflight.waiting.insert(request.request_id, tell);
        }
        match self.outbound.send(request.encode()) {
            Ok(()) => Ok(hear),
            Err(_) => {
                self.discard(request.request_id);
                Err(())
            }
        }
    }

    /// Forgets a call whose caller is no longer waiting, so a peer that
    /// answers late does not leave an entry behind forever.
    fn discard(&self, request_id: u64) {
        self.inflight
            .lock()
            .expect("inflight poisoned")
            .waiting
            .remove(&request_id);
    }
}

fn status_of(to: NodeId, service: ServiceId, response: Response) -> TransportResult<Bytes> {
    match response.status {
        STATUS_OK => Ok(response.payload),
        STATUS_NO_HANDLER => Err(TransportError::NoHandler(service)),
        STATUS_ERROR => Err(TransportError::Remote(
            String::from_utf8_lossy(&response.payload).into_owned(),
        )),
        other => Err(TransportError::Remote(format!(
            "node {to} answered with an unknown status {other}"
        ))),
    }
}

/// Serves one inbound connection until the peer closes it.
///
/// Each request is handled on its own task and each reply is queued to a
/// single writer, so a slow handler holds up neither the reader nor the
/// replies to calls that finished behind it. The request id is what makes that
/// safe: the caller matches replies by id and never by order.
async fn serve(
    transport: PeerTransport,
    stream: TcpStream,
    from: SocketAddr,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    if let Err(error) = stream.set_nodelay(true) {
        tracing::debug!(%from, %error, "could not disable Nagle on an inbound connection");
    }
    let (mut reader, mut writer) = tokio::io::split(stream);
    let (replies, mut queue) = mpsc::unbounded_channel::<Bytes>();

    let writing = tokio::spawn(async move {
        while let Some(frame) = queue.recv().await {
            if writer.write_all(&frame).await.is_err() {
                break;
            }
        }
        let _ = writer.shutdown().await;
    });

    loop {
        let frame = tokio::select! {
            _ = stop.changed() => break,
            frame = read_frame(&mut reader) => frame,
        };
        let body = match frame {
            Ok(Some(body)) => body,
            Ok(None) => break,
            Err(error) => {
                tracing::debug!(%from, %error, "reading from a peer failed");
                break;
            }
        };
        let request = match Request::decode(&body) {
            Ok(request) => request,
            // A frame this build cannot parse means the peer is speaking a
            // different version of a protocol that has no compatibility
            // guarantee, so the connection is dropped rather than resynced.
            Err(error) => {
                tracing::warn!(%from, %error, "undecodable peer request");
                break;
            }
        };

        let transport = transport.clone();
        let replies = replies.clone();
        let handler = transport.handler(request.service);
        let Some(handler) = handler else {
            let _ = replies.send(
                Response {
                    request_id: request.request_id,
                    status: STATUS_NO_HANDLER,
                    payload: Bytes::new(),
                }
                .encode(),
            );
            continue;
        };
        handler.executor.spawn(async move {
            let request_id = request.request_id;
            let call = PeerCall {
                service: request.service,
                method: request.method,
                payload: request.payload,
            };
            let response = match handler.handler.handle_boxed(UNIDENTIFIED_PEER, call).await {
                Ok(payload) => Response {
                    request_id,
                    status: STATUS_OK,
                    payload,
                },
                Err(error) => Response {
                    request_id,
                    status: STATUS_ERROR,
                    payload: Bytes::from(error.to_string()),
                },
            };
            let _ = replies.send(response.encode());
        });
    }

    drop(replies);
    let _ = writing.await;
}

/// Reads one length-prefixed frame body, or `None` at a clean end of stream.
///
/// `read_exact` is what handles a peer whose write landed in several packets,
/// which is the first thing a real socket does that an in-memory transport
/// never did.
async fn read_frame<R>(reader: &mut R) -> Result<Option<Vec<u8>>, FrameError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut prefix = [0u8; 4];
    if reader.read_exact(&mut prefix).await.is_err() {
        // Nothing had been read of this frame, so the peer either closed
        // cleanly or reset. Both mean there is no more traffic on this
        // connection, and the caller's waiters are failed either way.
        return Ok(None);
    }
    let length = frame_length(prefix)?;
    let mut body = vec![0u8; length];
    match reader.read_exact(&mut body).await {
        Ok(_) => Ok(Some(body)),
        // A frame that started and did not finish is a truncated message
        // rather than a clean close, and treating it as one would silently
        // drop a request.
        Err(_) => Err(FrameError::Truncated),
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

/// The reader allocates the length a peer announced before it has read a byte
/// of the body, so the cap has to fit in the field that carries it.
const _: () = assert!(MAX_FRAME_BYTES <= u32::MAX as usize);

#[cfg(test)]
mod tests {
    use super::*;

    struct Echo;

    impl PeerHandler for Echo {
        async fn handle(&self, _from: NodeId, call: PeerCall) -> TransportResult<Bytes> {
            Ok(call.payload)
        }
    }

    /// A handler that holds method one until method two arrives, so a test can
    /// prove a second call is not stuck behind the first.
    struct Gate {
        open: Arc<tokio::sync::Notify>,
    }

    impl PeerHandler for Gate {
        async fn handle(&self, _from: NodeId, call: PeerCall) -> TransportResult<Bytes> {
            if call.method == 1 {
                self.open.notified().await;
            } else {
                self.open.notify_waiters();
            }
            Ok(call.payload)
        }
    }

    struct Refuse;

    impl PeerHandler for Refuse {
        async fn handle(&self, _from: NodeId, _call: PeerCall) -> TransportResult<Bytes> {
            Err(TransportError::Remote("refused".to_string()))
        }
    }

    struct ExecutionThread;

    impl PeerHandler for ExecutionThread {
        async fn handle(&self, _from: NodeId, _call: PeerCall) -> TransportResult<Bytes> {
            Ok(Bytes::from(
                std::thread::current()
                    .name()
                    .unwrap_or("unnamed")
                    .to_owned(),
            ))
        }
    }

    fn call(payload: &'static [u8]) -> PeerCall {
        PeerCall {
            service: ServiceId::Proxy,
            method: 1,
            payload: Bytes::from_static(payload),
        }
    }

    async fn pair() -> (PeerTransport, PeerTransport, PeerListener) {
        let one = PeerTransport::new(NodeId(1));
        let two = PeerTransport::new(NodeId(2));
        let listener = two.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
        one.set_peer(NodeId(2), listener.local_addr().to_string());
        (one, two, listener)
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
    async fn a_peer_with_no_known_address_is_reported_rather_than_dialled() {
        let transport = PeerTransport::new(NodeId(1));
        assert_eq!(
            transport.call(NodeId(2), call(b"")).await,
            Err(TransportError::UnknownPeer(NodeId(2)))
        );
    }

    #[tokio::test]
    async fn a_call_reaches_a_peer_over_a_real_socket() {
        let (one, two, listener) = pair().await;
        two.register(ServiceId::Proxy, Echo);

        assert_eq!(
            one.call(NodeId(2), call(b"over the wire")).await,
            Ok(Bytes::from_static(b"over the wire"))
        );
        listener.shutdown().await;
    }

    #[tokio::test]
    async fn control_ingress_and_handlers_run_on_the_reserved_executor() {
        let one = PeerTransport::new(NodeId(1));
        let two = PeerTransport::new(NodeId(2));
        let executor = crate::runtime::ControlExecutor::start().unwrap();
        two.route_service(ServiceId::Control, executor.handle());
        two.register(ServiceId::Control, ExecutionThread);
        let listener = two
            .listen_on("127.0.0.1:0".parse().unwrap(), executor.handle())
            .await
            .unwrap();
        one.set_peer(NodeId(2), listener.local_addr().to_string());

        let handled = one
            .call(
                NodeId(2),
                PeerCall {
                    service: ServiceId::Control,
                    method: 1,
                    payload: Bytes::new(),
                },
            )
            .await
            .unwrap();
        assert_eq!(handled, Bytes::from_static(b"orbita-control"));

        listener.shutdown().await;
    }

    #[tokio::test]
    async fn a_payload_larger_than_one_packet_arrives_whole() {
        // The first thing a real socket does that an in-memory transport never
        // did is split a write across several reads.
        let (one, two, listener) = pair().await;
        two.register(ServiceId::Proxy, Echo);

        let payload = Bytes::from(vec![7u8; 3 * 1024 * 1024]);
        let echoed = one
            .call(
                NodeId(2),
                PeerCall {
                    service: ServiceId::Proxy,
                    method: 1,
                    payload: payload.clone(),
                },
            )
            .await
            .expect("a large call succeeds");
        assert_eq!(echoed, payload);
        listener.shutdown().await;
    }

    #[tokio::test]
    async fn a_slow_call_does_not_hold_up_one_behind_it_on_the_same_connection() {
        // This is what the request id buys. Without it the second call would
        // wait for the first, and an owner would replicate at the speed of its
        // slowest in-flight message.
        let (one, two, listener) = pair().await;
        let open = Arc::new(tokio::sync::Notify::new());
        two.register(ServiceId::Proxy, Gate { open });

        let slow = {
            let one = one.clone();
            tokio::spawn(async move {
                one.call(
                    NodeId(2),
                    PeerCall {
                        service: ServiceId::Proxy,
                        method: 1,
                        payload: Bytes::from_static(b"slow"),
                    },
                )
                .await
            })
        };
        // Let the slow call become the only thing on the connection.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let quick = one
            .call(
                NodeId(2),
                PeerCall {
                    service: ServiceId::Proxy,
                    method: 2,
                    payload: Bytes::from_static(b"quick"),
                },
            )
            .await;
        assert_eq!(quick, Ok(Bytes::from_static(b"quick")));
        assert_eq!(
            slow.await.unwrap(),
            Ok(Bytes::from_static(b"slow")),
            "the first call still finishes once it is released"
        );
        listener.shutdown().await;
    }

    #[tokio::test]
    async fn a_handlers_refusal_arrives_as_a_remote_error_not_a_dropped_call() {
        let (one, two, listener) = pair().await;
        two.register(ServiceId::Proxy, Refuse);

        let outcome = one.call(NodeId(2), call(b"")).await;
        match outcome {
            Err(TransportError::Remote(message)) => assert!(
                message.contains("refused"),
                "the peer's reason must survive the hop, got {message}"
            ),
            other => panic!("a refusal must not look like a network failure, got {other:?}"),
        }
        listener.shutdown().await;
    }

    #[tokio::test]
    async fn a_service_the_peer_does_not_serve_is_reported_rather_than_hanging() {
        let (one, _two, listener) = pair().await;
        assert_eq!(
            one.call(NodeId(2), call(b"")).await,
            Err(TransportError::NoHandler(ServiceId::Proxy))
        );
        listener.shutdown().await;
    }

    #[tokio::test]
    async fn a_call_to_a_peer_that_stopped_answering_times_out() {
        // A socket that is accepted and then ignored looks healthy to TCP, so
        // the only thing that ends the call is the call timeout.
        let one = PeerTransport::with_timeout(NodeId(1), Duration::from_millis(100));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _held = listener.accept().await;
            std::future::pending::<()>().await;
        });
        one.set_peer(NodeId(2), addr.to_string());

        assert_eq!(
            one.call(NodeId(2), call(b"")).await,
            Err(TransportError::Timeout(NodeId(2)))
        );
    }

    #[tokio::test]
    async fn a_call_after_the_peer_restarted_redials_rather_than_failing_forever() {
        let one = PeerTransport::new(NodeId(1));
        let two = PeerTransport::new(NodeId(2));
        two.register(ServiceId::Proxy, Echo);

        let first = two.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let addr = first.local_addr();
        one.set_peer(NodeId(2), addr.to_string());
        assert!(one.call(NodeId(2), call(b"before")).await.is_ok());
        first.shutdown().await;

        // Rebinding the same port is the closest a test gets to a peer process
        // restarting underneath a pooled connection.
        let second = two.listen(addr).await.expect("the port is free again");
        assert_eq!(
            one.call(NodeId(2), call(b"after")).await,
            Ok(Bytes::from_static(b"after"))
        );
        second.shutdown().await;
    }

    #[tokio::test]
    async fn a_call_to_a_peer_that_is_not_listening_is_reported_unreachable() {
        let one = PeerTransport::new(NodeId(1));
        // Binding and dropping gives an address nothing is listening on.
        let addr = {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap()
        };
        one.set_peer(NodeId(2), addr.to_string());
        assert_eq!(
            one.call(NodeId(2), call(b"")).await,
            Err(TransportError::Unreachable(NodeId(2)))
        );
    }

    #[tokio::test]
    async fn many_calls_in_flight_at_once_each_get_their_own_answer() {
        let (one, two, listener) = pair().await;
        two.register(ServiceId::Proxy, Echo);

        let mut running = Vec::new();
        for i in 0..64u32 {
            let one = one.clone();
            running.push(tokio::spawn(async move {
                let payload = Bytes::from(i.to_be_bytes().to_vec());
                let echoed = one
                    .call(
                        NodeId(2),
                        PeerCall {
                            service: ServiceId::Proxy,
                            method: 1,
                            payload: payload.clone(),
                        },
                    )
                    .await;
                assert_eq!(echoed, Ok(payload), "reply {i} went to the wrong caller");
            }));
        }
        for task in running {
            task.await.unwrap();
        }
        listener.shutdown().await;
    }
}
