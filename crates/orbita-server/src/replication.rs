//! What a replica does with the entries its owner sends it.
//!
//! `orbita_wal::WalService` writes an inbound batch to this node's log and
//! acknowledges it. Everything else a replica has to do with that batch lives
//! here, behind the `ReplicaObserver` hook: marking keys unreadable before the
//! acknowledgement, applying entries to the storage engine afterwards, and
//! throwing marks away when a newer owner says history ended earlier than this
//! node thought.
//!
//! # Why the marks come first and the apply comes later
//!
//! ADR 0001 acknowledges a write to the client once it is durable and once no
//! lease holder can still serve the old value. The invalidation is what makes
//! the second true, so it has to happen before this node acknowledges the
//! batch. The apply does not: a key stays unreadable here until storage has
//! it, so a read arriving in between forwards to the owner rather than
//! answering from a value that is about to be replaced.
//!
//! That is why `invalidating` does its work inline and `durable` queues it. The
//! observer runs between an inbound batch and its acknowledgement, so anything
//! slow in it becomes write latency for the whole partition, and applying to
//! RocksDB is slow.

use crate::host::PartitionHost;

use orbita_core::{Lamport, PartitionId};
use orbita_runtime::Runtime;
use orbita_wal::{ReplicaObserver, WalEntry};

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

/// The observer a node registers with its `WalService`.
///
/// It holds the partitions this node replicates rather than reaching back into
/// the node, because the invalidation half runs synchronously on the
/// replication path and cannot take an asynchronous lock to find a host.
///
/// Everything it reaches is weak, and that is not an optimisation. The
/// transport's handler registry holds the `WalService`, which holds this, so
/// anything strong here outlives the node: a partition's storage engine would
/// stay open after the node closed it, and the apply queue would be closed by
/// whoever tore down the transport rather than by the node that made it.
pub(crate) struct ReplicaBridge<R: Runtime> {
    hosts: Mutex<HashMap<PartitionId, Weak<PartitionHost<R>>>>,
    applies: Weak<Applies>,
}

/// One thing for the applier to do, in the order the owner said it.
enum Work {
    /// These entries are durable here and waiting to be released.
    Durable(Vec<WalEntry>),
    /// The owner has acknowledged this far, so anything held back below it can
    /// be applied.
    Committed(PartitionId, Lamport),
}

/// The queue durable entries wait on to be applied.
///
/// A named type so the node can own it: dropping it is what stops the applier,
/// and that has to happen when the node goes away rather than when the last
/// observer does.
pub(crate) struct Applies(tokio::sync::mpsc::UnboundedSender<Work>);

impl<R: Runtime> ReplicaBridge<R> {
    /// Builds the bridge and starts the task that applies what it receives.
    ///
    /// The task holds a weak reference, so a node that shuts down closes its
    /// storage engines rather than leaving an applier holding them open.
    pub(crate) fn start(runtime: &R) -> (Arc<Self>, Arc<Applies>) {
        let (sender, mut queue) = tokio::sync::mpsc::unbounded_channel::<Work>();
        let applies = Arc::new(Applies(sender));
        let bridge = Arc::new(Self {
            hosts: Mutex::new(HashMap::new()),
            applies: Arc::downgrade(&applies),
        });

        let weak = Arc::downgrade(&bridge);
        runtime.spawn(async move {
            while let Some(work) = queue.recv().await {
                let Some(bridge) = weak.upgrade() else {
                    return;
                };
                match work {
                    Work::Durable(entries) => bridge.apply(&entries).await,
                    Work::Committed(partition, through) => {
                        if let Some(host) = bridge.host(partition) {
                            host.commit_through(through).await;
                        }
                    }
                }
            }
        });
        (bridge, applies)
    }

    /// Starts watching a partition this node replicates.
    pub(crate) fn register(&self, host: &Arc<PartitionHost<R>>) {
        self.hosts
            .lock()
            .expect("replica bridge registry poisoned")
            .insert(host.id(), Arc::downgrade(host));
    }

    /// Stops watching a partition, which is what a node does when it takes
    /// over as owner or hands the partition away.
    pub(crate) fn unregister(&self, partition: PartitionId) {
        self.hosts
            .lock()
            .expect("replica bridge registry poisoned")
            .remove(&partition);
    }

    fn host(&self, partition: PartitionId) -> Option<Arc<PartitionHost<R>>> {
        self.hosts
            .lock()
            .expect("replica bridge registry poisoned")
            .get(&partition)
            .and_then(Weak::upgrade)
    }

    /// Applies a batch in the order the owner assigned, which is the order the
    /// storage engine requires: it ignores anything at or below its committed
    /// Lamport, so an apply that runs out of order is discarded rather than
    /// merely late.
    async fn apply(&self, entries: &[WalEntry]) {
        for entry in entries {
            let Some(host) = self.host(entry.partition) else {
                // The partition moved between the acknowledgement and here.
                // The entry is in the log either way, so recovery replays it
                // if this node opens the partition again.
                continue;
            };
            if let Err(error) = host.apply_replicated(entry).await {
                tracing::error!(
                    partition = entry.partition.get(),
                    lamport = entry.lamport.get(),
                    %error,
                    "applying a replicated entry to storage failed"
                );
            }
        }
    }
}

impl<R: Runtime> ReplicaObserver for ReplicaBridge<R> {
    fn invalidating(&self, entries: &[WalEntry]) {
        for entry in entries {
            let Some(host) = self.host(entry.partition) else {
                continue;
            };
            host.invalidate(key_of(entry), entry.lamport);
        }
    }

    fn durable(&self, entries: &[WalEntry]) {
        // A queue that has gone means the node is shutting down. The entries
        // are durable in the log, so recovery applies them on the next start.
        if let Some(applies) = self.applies.upgrade() {
            let _ = applies.0.send(Work::Durable(entries.to_vec()));
        }
    }

    fn committed(&self, partition: PartitionId, through: Lamport) {
        if let Some(applies) = self.applies.upgrade() {
            let _ = applies.0.send(Work::Committed(partition, through));
        }
    }

    fn truncated(&self, partition: PartitionId, above: Lamport) {
        let Some(host) = self.host(partition) else {
            return;
        };
        // Done inline rather than queued: a truncation means ownership moved,
        // and the lease this drops is exactly what the new owner is waiting
        // out before it accepts a write. Waiting behind a queue of applies
        // would give away that ordering.
        host.truncated(above);
    }
}

fn key_of(entry: &WalEntry) -> bytes::Bytes {
    match &entry.op {
        orbita_wal::WalOp::Put { key, .. } | orbita_wal::WalOp::Delete { key, .. } => key.clone(),
    }
}
