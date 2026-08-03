//! The replica side of the protocol.
//!
//! One service handles every partition this node replicates, because
//! `Transport` registers one handler per `ServiceId` and a node is a replica
//! for many partitions at once.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::format::WalEntry;
use bytes::Bytes;
use orbita_core::{Lamport, NodeId, PartitionId};
use orbita_runtime::{PeerCall, PeerHandler, Runtime, TransportError};

use crate::log::PartitionLog;
use crate::wire::{
    AppendRequest, FenceRequest, StatusRequest, WalResponse, METHOD_APPEND, METHOD_FENCE,
    METHOD_STATUS,
};

/// Watches replicated entries as they arrive.
///
/// This exists for the read path. A replica may serve a read locally only if
/// it has not been told that the key is changing, and the message that tells
/// it is the replication of the write itself. Without a hook here the server
/// cannot see replicated entries at all, so a replica could neither stop
/// serving a stale value nor apply the write to its storage engine.
///
/// The methods are synchronous because the work behind them is marking an
/// in-memory set, and because they sit between an inbound batch and its
/// acknowledgement. Anything slow here becomes write latency for the whole
/// partition, so an implementation that needs to do real work should queue it
/// and return.
///
/// See ADR 0001 for why invalidation has to happen before the acknowledgement
/// rather than when the entry is applied.
pub trait ReplicaObserver: Send + Sync + 'static {
    /// These keys are about to change.
    ///
    /// Called before the entries are written and always before this node
    /// acknowledges them, so a replica stops answering with the old value
    /// strictly before the owner can tell a client the new one is committed.
    fn invalidating(&self, entries: &[WalEntry]);

    /// These entries are durable on this node.
    ///
    /// The observer applies them to its storage engine and clears the marks it
    /// set in `invalidating` once it has.
    fn durable(&self, entries: &[WalEntry]);

    /// The owner has acknowledged everything up to `through` to its clients.
    ///
    /// An observer that applies replicated entries to a storage engine must
    /// not run ahead of this, or it will hold, and can serve, a write that was
    /// never acknowledged to anybody.
    fn committed(&self, partition: PartitionId, through: Lamport);

    /// Everything above `above` has been discarded, because a newer owner said
    /// history ends there.
    ///
    /// An observer holding marks for those entries has to drop them, or it
    /// will wait forever for writes that are never going to arrive.
    ///
    /// The partition is named because one service handles every partition this
    /// node replicates, and a Lamport on its own does not say which log it
    /// belongs to.
    fn truncated(&self, partition: PartitionId, above: Lamport);
}

/// Serves inbound WAL traffic for every partition this node holds.
///
/// Cheap to clone; all clones share one registry, so the server can hand a
/// clone to the transport and keep one for itself.
pub struct WalService<R: Runtime> {
    logs: Arc<Mutex<HashMap<PartitionId, Arc<PartitionLog<R>>>>>,
    observer: Arc<Mutex<Option<Arc<dyn ReplicaObserver>>>>,
    /// One gate per partition, held across a whole append.
    ///
    /// An owner pipelines: it releases its flush lock before replicating, so
    /// two batches for one partition are in flight at once and arrive here
    /// concurrently. Without this, both would read the same durable Lamport,
    /// and the one that lost the race would be rejected as non contiguous even
    /// though it was perfectly in order. The log is a sequence, so appending
    /// to it is one at a time by nature; the gate only makes that explicit.
    gates: Arc<Mutex<HashMap<PartitionId, Arc<tokio::sync::Mutex<()>>>>>,
}

impl<R: Runtime> Clone for WalService<R> {
    fn clone(&self) -> Self {
        Self {
            logs: Arc::clone(&self.logs),
            observer: Arc::clone(&self.observer),
            gates: Arc::clone(&self.gates),
        }
    }
}

impl<R: Runtime> Default for WalService<R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R: Runtime> WalService<R> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            logs: Arc::new(Mutex::new(HashMap::new())),
            observer: Arc::new(Mutex::new(None)),
            gates: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Registers the observer that watches replicated entries.
    ///
    /// Set this before the transport starts delivering, or the first batch
    /// lands without anything having been invalidated for it.
    pub fn observe(&self, observer: Arc<dyn ReplicaObserver>) {
        *self.observer.lock().expect("wal service observer poisoned") = Some(observer);
    }

    fn observer(&self) -> Option<Arc<dyn ReplicaObserver>> {
        self.observer
            .lock()
            .expect("wal service observer poisoned")
            .clone()
    }

    /// Starts serving replication traffic for a partition.
    ///
    /// The log is passed in rather than opened here because a node that gets
    /// promoted keeps the same log it was replicating into, and reopening it
    /// would run recovery a second time against a log that is already live.
    pub fn register(&self, log: Arc<PartitionLog<R>>) {
        self.logs
            .lock()
            .expect("wal service registry poisoned")
            .insert(log.partition(), log);
    }

    /// Stops serving a partition, which is what a node does when it is about
    /// to take over as its owner.
    pub fn unregister(&self, partition: PartitionId) {
        self.logs
            .lock()
            .expect("wal service registry poisoned")
            .remove(&partition);
        self.gates
            .lock()
            .expect("wal service gate registry poisoned")
            .remove(&partition);
    }

    /// The log this node holds for a partition, for the code that applies
    /// entries to storage.
    #[must_use]
    pub fn log(&self, partition: PartitionId) -> Option<Arc<PartitionLog<R>>> {
        self.logs
            .lock()
            .expect("wal service registry poisoned")
            .get(&partition)
            .cloned()
    }

    fn gate(&self, partition: PartitionId) -> Arc<tokio::sync::Mutex<()>> {
        Arc::clone(
            self.gates
                .lock()
                .expect("wal service gate registry poisoned")
                .entry(partition)
                .or_default(),
        )
    }

    async fn dispatch(&self, call: PeerCall) -> WalResponse {
        match call.method {
            METHOD_APPEND => match AppendRequest::decode(&call.payload) {
                Ok(request) => self.append(request).await,
                Err(e) => WalResponse::Error(format!("undecodable append: {e:?}")),
            },
            METHOD_FENCE => match FenceRequest::decode(&call.payload) {
                Ok(request) => self.fence(request).await,
                Err(e) => WalResponse::Error(format!("undecodable fence: {e:?}")),
            },
            METHOD_STATUS => match StatusRequest::decode(&call.payload) {
                Ok(request) => self.status(request).await,
                Err(e) => WalResponse::Error(format!("undecodable status: {e:?}")),
            },
            other => WalResponse::Error(format!("unknown wal method {other}")),
        }
    }

    async fn append(&self, request: AppendRequest) -> WalResponse {
        let Some(log) = self.log(request.partition) else {
            return WalResponse::Error(format!("partition {} not held here", request.partition));
        };
        // Held for the whole of the decision and the write. See the gate's own
        // documentation for why reading the durable Lamport and appending have
        // to be one step.
        let gate = self.gate(request.partition);
        let _ordered = gate.lock().await;

        let current = log.epoch().await;
        if request.epoch < current {
            // The single most important line in the crate: a deposed owner
            // gets no further writes, however well formed they are.
            tracing::warn!(
                partition = request.partition.get(),
                got = request.epoch.get(),
                current = current.get(),
                "rejecting append from a fenced owner"
            );
            return WalResponse::StaleEpoch { current };
        }

        if request.epoch > current {
            // A newer owner is authoritative about where history ends. See the
            // crate docs on replica divergence for why dropping our tail is
            // safe rather than merely convenient.
            if let Err(e) = log.record_fence(request.epoch).await {
                return WalResponse::Error(e.to_string());
            }
            let held = log.durable_lamport().await;
            if let Err(e) = log.truncate_above(request.prev_lamport).await {
                return WalResponse::Error(e.to_string());
            }
            // Only a truncation that discarded something is one the observer
            // has to hear about. Every replica meets its owner at an epoch
            // above the one its own log records, so the first append of a
            // partition's life takes this path with nothing above the cut, and
            // an observer told about it would throw away state it still needs.
            if held > request.prev_lamport {
                if let Some(observer) = self.observer() {
                    observer.truncated(request.partition, request.prev_lamport);
                }
            }
        }

        let durable = log.durable_lamport().await;
        let epoch = log.epoch().await;
        if request.prev_lamport > durable {
            // Writing this batch would leave a hole, and a log with a hole
            // cannot be replayed.
            return WalResponse::Gap {
                durable_lamport: durable,
                epoch,
            };
        }

        let mut expected = durable.next();
        let mut frames = Vec::with_capacity(request.entries.len());
        let mut accepted = Vec::with_capacity(request.entries.len());
        for (entry, frame) in &request.entries {
            if entry.partition != request.partition {
                return WalResponse::Error("entry belongs to another partition".into());
            }
            // Entries at or below what we hold are a retransmission of bytes
            // we already have, so skipping them makes append idempotent.
            if entry.lamport <= durable {
                continue;
            }
            if entry.lamport != expected {
                return WalResponse::Error(format!(
                    "non contiguous batch: expected {expected}, got {}",
                    entry.lamport
                ));
            }
            expected = expected.next();
            frames.push(frame.clone());
            accepted.push(entry.clone());
        }

        // Invalidating before the write, rather than after, means a crash
        // between the two leaves this node having stopped serving keys it did
        // not end up storing. That is the harmless direction: it forwards
        // reads it could have served. The other order has a window where the
        // entry is durable, the owner acknowledges the client, and this node
        // is still handing out the previous value.
        let observer = self.observer();
        if let Some(observer) = &observer {
            if !accepted.is_empty() {
                observer.invalidating(&accepted);
            }
        }

        let last = if frames.is_empty() {
            durable
        } else {
            Lamport(expected.get() - 1)
        };
        if let Err(e) = log.append_frames(&frames, last).await {
            return WalResponse::Error(e.to_string());
        }

        if let Some(observer) = &observer {
            if !accepted.is_empty() {
                observer.durable(&accepted);
            }
            // After the entries, so an observer that queues them sees the
            // release behind whatever it is releasing.
            observer.committed(request.partition, request.committed);
        }

        WalResponse::Ok {
            durable_lamport: log.durable_lamport().await,
            epoch,
        }
    }

    async fn fence(&self, request: FenceRequest) -> WalResponse {
        let Some(log) = self.log(request.partition) else {
            return WalResponse::Error(format!("partition {} not held here", request.partition));
        };
        // The same gate as an append, because truncating the tail while one is
        // being written would leave the log describing neither history.
        let gate = self.gate(request.partition);
        let _ordered = gate.lock().await;

        let current = log.epoch().await;
        if request.epoch < current {
            return WalResponse::StaleEpoch { current };
        }
        if request.epoch > current {
            if let Err(e) = log.record_fence(request.epoch).await {
                return WalResponse::Error(e.to_string());
            }
            let held = log.durable_lamport().await;
            if let Err(e) = log.truncate_above(request.truncate_above).await {
                return WalResponse::Error(e.to_string());
            }
            if held > request.truncate_above {
                if let Some(observer) = self.observer() {
                    observer.truncated(request.partition, request.truncate_above);
                }
            }
        }
        WalResponse::Ok {
            durable_lamport: log.durable_lamport().await,
            epoch: log.epoch().await,
        }
    }

    async fn status(&self, request: StatusRequest) -> WalResponse {
        match self.log(request.partition) {
            Some(log) => WalResponse::Ok {
                durable_lamport: log.durable_lamport().await,
                epoch: log.epoch().await,
            },
            None => WalResponse::Error(format!("partition {} not held here", request.partition)),
        }
    }
}

impl<R: Runtime> PeerHandler for WalService<R> {
    async fn handle(&self, _from: NodeId, call: PeerCall) -> Result<Bytes, TransportError> {
        // Protocol level refusals travel as a response rather than a transport
        // error, so the owner can tell "you are fenced" from "the network ate
        // it" without parsing a string.
        Ok(self.dispatch(call).await.encode())
    }
}
