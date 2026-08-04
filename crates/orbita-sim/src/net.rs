//! An in-memory network with a mean streak.
//!
//! A call is modelled as two independent trips: the request out and the
//! response back. Each trip has its own latency and its own chance of being
//! dropped, and each is checked against the partition table at the moment it
//! would arrive rather than when it was sent. That is what makes an asymmetric
//! partition expressible: a request can arrive and be applied while the
//! response is discarded, so the caller times out on a write the peer already
//! committed. Symmetric partitions never produce that state, and it is the one
//! that breaks systems.

use crate::world::{SimCore, SimSleep};

use orbita_core::NodeId;
use orbita_runtime::{PeerCall, PeerHandler, ServiceId, Transport, TransportError};

use crate::world::TransportResult;

use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

/// One node's connection to the simulated network.
#[derive(Clone)]
pub struct SimTransport {
    core: Arc<SimCore>,
    node: NodeId,
}

impl SimTransport {
    pub(crate) fn new(core: Arc<SimCore>, node: NodeId) -> Self {
        Self { core, node }
    }

    fn hop_latency(&self) -> u64 {
        let net = &self.core.config.network;
        let base = self.core.latency(net.min_latency, net.max_latency);
        let slow = {
            let mut state = self.core.state();
            self.core.roll_fault(&mut state, net.slow_permille)
        };
        if slow {
            base + net.slow_latency.as_nanos() as u64
        } else {
            base
        }
    }

    /// Schedules one delivery attempt. A duplicated message is two of these,
    /// each with its own latency, so a duplicate can also arrive out of order.
    fn dispatch(&self, to: NodeId, call: PeerCall, slot: Arc<Slot>, msg: u64, copy: u32) {
        let core = self.core.clone();
        let from = self.node;
        let out = self.hop_latency();
        let back = self.hop_latency();
        let drop_permille = core.config.network.drop_permille;

        let task = Box::pin(async move {
            let service = call.service;
            SimSleep::new(core.clone(), out).await;

            {
                let mut state = core.state();
                if !state.link_open(from, to) {
                    state.record(format!("lost msg={msg}.{copy} reason=partitioned-request"));
                    return;
                }
                if core.roll_fault(&mut state, drop_permille) {
                    state.record(format!("lost msg={msg}.{copy} reason=dropped-request"));
                    return;
                }
                if !state.is_up(to) {
                    state.record(format!("refused msg={msg}.{copy} node={to} reason=down"));
                    drop(state);
                    slot.complete(Err(TransportError::Unreachable(to)));
                    return;
                }
                state.record(format!("recv msg={msg}.{copy} node={to}"));
            }

            let handler = core.state().handlers.get(&(to, service as u16)).cloned();
            let result = match handler {
                Some(handler) => handler.handle_dyn(from, call).await,
                None => Err(TransportError::NoHandler(service)),
            };

            // The reply travels on its own task, belonging to no node. Once a
            // process has put a response on the wire, killing that process
            // does not recall the packet, and modelling it otherwise hides the
            // most important failure there is: a write the peer committed and
            // acknowledged just before it died.
            let wire = core.clone();
            let reply = Box::pin(async move {
                SimSleep::new(wire.clone(), back).await;
                {
                    let mut state = wire.state();
                    if !state.link_open(to, from) {
                        // The peer has already applied the request. The caller
                        // will time out and, if it is correct, retry into an
                        // idempotent handler.
                        state.record(format!("lost msg={msg}.{copy} reason=partitioned-response"));
                        return;
                    }
                    if wire.roll_fault(&mut state, drop_permille) {
                        state.record(format!("lost msg={msg}.{copy} reason=dropped-response"));
                        return;
                    }
                    state.record(format!("reply msg={msg}.{copy} to={from}"));
                }
                slot.complete(result);
            });
            core.spawn_task(None, reply);
        });

        // The request and the handler belong to the receiving node, so
        // crashing it stops an in-flight handler where it stands.
        self.core.spawn_task(Some(to), task);
    }
}

impl Transport for SimTransport {
    fn call(
        &self,
        to: NodeId,
        call: PeerCall,
    ) -> impl Future<Output = TransportResult<Bytes>> + Send {
        let core = self.core.clone();
        let from = self.node;
        let this = self.clone();
        async move {
            let known = {
                let state = core.state();
                state.nodes.contains_key(&to)
            };
            if !known {
                return Err(TransportError::UnknownPeer(to));
            }

            let slot = Arc::new(Slot::default());
            let msg = {
                let mut state = core.state();
                let msg = state.seq();
                state.record(format!(
                    "send msg={msg} from={from} to={to} service={:?} method={} bytes={}",
                    call.service,
                    call.method,
                    call.payload.len()
                ));
                msg
            };

            let duplicate = {
                let mut state = core.state();
                core.roll_fault(&mut state, core.config.network.duplicate_permille)
            };
            this.dispatch(to, call.clone(), slot.clone(), msg, 0);
            if duplicate {
                core.state().record(format!("duplicate msg={msg} to={to}"));
                this.dispatch(to, call, slot.clone(), msg, 1);
            }

            let deadline = core.now() + core.config.call_timeout.as_nanos() as u64;
            Await {
                core: core.clone(),
                slot,
                deadline,
                peer: to,
                timer: None,
            }
            .await
        }
    }

    fn register(&self, service: ServiceId, handler: impl PeerHandler) {
        let replaced;
        {
            let mut state = self.core.state();
            replaced = state
                .handlers
                .insert((self.node, service as u16), Arc::new(handler));
            state.record(format!("register node={} service={service:?}", self.node));
        }
        // A replaced handler is dropped outside the lock, because its
        // destructor can reach back into the world: a restarted node's new
        // handler displaces the old one, whose drop may wake a task.
        drop(replaced);
    }

    fn local_node(&self) -> NodeId {
        self.node
    }
}

/// Where a reply lands, if one ever comes.
#[derive(Default)]
struct Slot {
    inner: Mutex<SlotState>,
}

#[derive(Default)]
struct SlotState {
    response: Option<TransportResult<Bytes>>,
    waker: Option<Waker>,
}

impl Slot {
    /// Fills the slot, ignoring a second fill. A duplicated request produces
    /// two replies and the caller only ever sees the first.
    fn complete(&self, response: TransportResult<Bytes>) {
        let waker = {
            let mut inner = self.inner.lock().expect("slot lock poisoned");
            if inner.response.is_some() {
                return;
            }
            inner.response = Some(response);
            inner.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// Waits for a reply or for the call timeout, whichever comes first.
///
/// This is a hand-written select rather than a combinator because the
/// simulator has no executor library underneath it, and because both arms have
/// to be able to deregister cleanly when the call is abandoned.
struct Await {
    core: Arc<SimCore>,
    slot: Arc<Slot>,
    deadline: u64,
    peer: NodeId,
    timer: Option<(u64, u64)>,
}

impl Future for Await {
    type Output = TransportResult<Bytes>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.get_mut();
        {
            let mut inner = me.slot.inner.lock().expect("slot lock poisoned");
            if let Some(response) = inner.response.take() {
                return Poll::Ready(response);
            }
            inner.waker = Some(cx.waker().clone());
        }

        let mut state = me.core.state();
        if state.now >= me.deadline {
            state.record(format!("timeout peer={}", me.peer));
            return Poll::Ready(Err(TransportError::Timeout(me.peer)));
        }
        if me.timer.is_none() {
            let seq = state.seq();
            let key = (me.deadline, seq);
            state.timers.insert(key, cx.waker().clone());
            me.timer = Some(key);
        }
        Poll::Pending
    }
}

impl Drop for Await {
    fn drop(&mut self) {
        if let Some(key) = self.timer.take() {
            self.core.state().timers.remove(&key);
        }
    }
}
