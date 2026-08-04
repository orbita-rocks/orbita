//! Where a worker joined to a real cluster gets its map, and what it tells the
//! leader group about itself.
//!
//! [`crate::MapSource`] exists so this crate could be built before the control
//! plane was. This is the adapter that closes that seam: the same trait, over
//! `orbita_control::ControlClient`, so nothing above it changes.
//!
//! # Why the map is cached here as well as in the node
//!
//! A control plane outage must not become a data plane outage. Fetching asks
//! the leader group only for what it has that this node does not, and a fetch
//! that fails answers with the map this node already had rather than an error,
//! so a worker keeps routing on its last good map for as long as the control
//! plane is away. The one time an error is honest is the first fetch, because
//! a node that has never had a map cannot serve anything.

use crate::map_source::MapSource;

use orbita_control::{ControlClient, NodeRole, NodeStatus, PartitionProgress};
use orbita_core::{Error, MapVersion, NodeId, PartitionMap, Result};
use orbita_runtime::Runtime;

use std::sync::{Arc, Mutex};

/// The partition map, as published by the leader group.
pub struct ControlMapSource<R: Runtime> {
    client: ControlClient<R>,
    held: Arc<Mutex<Option<PartitionMap>>>,
}

impl<R: Runtime> Clone for ControlMapSource<R> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            held: Arc::clone(&self.held),
        }
    }
}

impl<R: Runtime> ControlMapSource<R> {
    #[must_use]
    pub fn new(client: ControlClient<R>) -> Self {
        Self {
            client,
            held: Arc::new(Mutex::new(None)),
        }
    }

    fn cached(&self) -> Option<PartitionMap> {
        self.held.lock().expect("cached map poisoned").clone()
    }

    fn version(&self) -> MapVersion {
        self.cached().map(|map| map.version()).unwrap_or_default()
    }
}

impl<R: Runtime> MapSource for ControlMapSource<R> {
    async fn fetch(&self) -> Result<PartitionMap> {
        match self.client.fetch_map_if_newer(self.version()).await {
            Ok(Some(map)) => {
                *self.held.lock().expect("cached map poisoned") = Some(map.clone());
                Ok(map)
            }
            Ok(None) => self
                .cached()
                .ok_or_else(|| Error::Internal("the leader group has no map yet".into())),
            Err(error) => match self.cached() {
                Some(map) => {
                    tracing::warn!(%error, "routing on the last map the leader group published");
                    Ok(map)
                }
                None => Err(error),
            },
        }
    }
}

/// Keeps this node's peer directory in step with the cluster.
///
/// The partition map names an owner by id and says nothing about how to reach
/// one, so a node that has routed a request perfectly can still have nowhere
/// to send it. Each node reports its own peer address on every heartbeat, so
/// the leader group holds the answer, and this copies it into the transport.
///
/// This is what lets an operator configure the leader group and nothing else.
/// A static peer list stays supported, and takes effect until the first poll
/// replaces it, which is what a node that starts before the control plane
/// needs.
pub struct PeerDirectorySync<R: Runtime> {
    client: ControlClient<R>,
    transport: crate::PeerTransport,
    local: NodeId,
}

impl<R: Runtime> PeerDirectorySync<R> {
    #[must_use]
    pub fn new(client: ControlClient<R>, transport: crate::PeerTransport, local: NodeId) -> Self {
        Self {
            client,
            transport,
            local,
        }
    }

    /// Refreshes the directory once.
    pub async fn refresh(&self) {
        match self.client.fetch_nodes().await {
            Ok(nodes) => {
                for (node, address) in nodes {
                    if node != self.local {
                        self.transport.set_peer(node, address);
                    }
                }
            }
            // A stale directory is survivable and a missing one is not worth
            // stopping for, because the addresses this node already has keep
            // working while the control plane is away.
            Err(error) => {
                tracing::debug!(%error, "could not refresh the peer directory");
            }
        }
    }
}

/// What this node tells the leader group about itself.
///
/// This doubles as the heartbeat, so it is what keeps the node out of the
/// failure detector, and the progress it carries is what the leader group
/// compares when it has to choose a replacement owner. A node that stops
/// sending this is the input to failover, which is why it goes out on a timer
/// rather than on a request.
pub struct StatusReporter<R: Runtime> {
    client: ControlClient<R>,
    node: NodeId,
    address: String,
}

impl<R: Runtime> StatusReporter<R> {
    #[must_use]
    pub fn new(client: ControlClient<R>, node: NodeId, address: impl Into<String>) -> Self {
        Self {
            client,
            node,
            address: address.into(),
        }
    }

    /// Sends one report, carrying how far this node has got on every partition
    /// it holds.
    ///
    /// Answers whether the report landed. The first `true` is what marks this
    /// node as joined for readiness, because a report the leader group
    /// accepted is the moment the cluster knows this node exists.
    pub async fn report(
        &self,
        map_version: MapVersion,
        partitions: Vec<PartitionProgress>,
    ) -> bool {
        let status = NodeStatus {
            role: NodeRole::Worker,
            address: self.address.clone(),
            map_version,
            partitions,
        };
        match self.client.report_status(self.node, status).await {
            Ok(()) => true,
            Err(error) => {
                // A heartbeat that does not land is what failover is built to
                // survive, so it is worth saying and not worth stopping for.
                tracing::debug!(%error, "reporting status to the leader group failed");
                false
            }
        }
    }
}
