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

use orbita_control::{
    binary_speaks, lifecycle_protocol_active, ClusterVersion, CompatibilityRefusal, ControlClient,
    NodeRole, NodeStatus, PartitionProgress, StatusReportResponse,
};
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
    role: NodeRole,
    address: String,
    /// The active cluster version, as of the last heartbeat that landed.
    ///
    /// This is the value version-dependent behaviour gates on: a node speaks
    /// the cluster version, not its own binary version. `None` until the
    /// first heartbeat is answered.
    active: Arc<Mutex<Option<ClusterVersion>>>,
}

impl<R: Runtime> Clone for StatusReporter<R> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            node: self.node,
            role: self.role,
            address: self.address.clone(),
            active: Arc::clone(&self.active),
        }
    }
}

impl<R: Runtime> StatusReporter<R> {
    #[must_use]
    pub fn new(client: ControlClient<R>, node: NodeId, address: impl Into<String>) -> Self {
        Self::new_with_role(client, node, NodeRole::Worker, address)
    }

    /// Builds a reporter for a node whose cluster role is already known.
    #[must_use]
    pub fn new_with_role(
        client: ControlClient<R>,
        node: NodeId,
        role: NodeRole,
        address: impl Into<String>,
    ) -> Self {
        Self {
            client,
            node,
            role,
            address: address.into(),
            active: Arc::new(Mutex::new(None)),
        }
    }

    /// The active cluster version, from the last heartbeat the leader group
    /// answered. `None` means this node has not been told one yet.
    #[must_use]
    pub fn active_cluster_version(&self) -> Option<ClusterVersion> {
        *self.active.lock().expect("active version poisoned")
    }

    /// Whether this worker may use the finalized lifecycle protocol.
    ///
    /// The question is whether the *cluster* has finalized onto a version that
    /// carries lifecycle state, not whether the cluster happens to be on this
    /// binary's own version. Those coincide only for a node whose binary is
    /// exactly the finalized one, which during a rolling upgrade is precisely
    /// the node this does not describe. Asking the narrower question is issue
    /// \#105: a new-binary worker joining a cluster still on the old version
    /// withheld its `ready` claim, its old-binary leader was still applying
    /// the lifecycle rule, and the worker stayed in membership without ever
    /// being placed. `lifecycle_protocol_active` is the same predicate the
    /// state machine gates on, so both ends of the heartbeat now agree from
    /// the one version they share.
    #[must_use]
    pub fn can_handoff(&self) -> bool {
        self.role == NodeRole::Worker
            && self
                .active_cluster_version()
                .is_some_and(lifecycle_protocol_active)
    }

    /// Sends one report, carrying how far this node has got on every partition
    /// it holds, and reads the active cluster version back off the reply.
    ///
    /// A compatibility refusal is an accepted control-plane answer rather than
    /// a transport failure, so the caller can keep the process running and
    /// hold readiness closed with the exact versions that disagreed.
    pub async fn report(
        &self,
        map_version: MapVersion,
        partitions: Vec<PartitionProgress>,
        ready: bool,
        draining: bool,
    ) -> Result<StatusReportResponse> {
        let status = NodeStatus {
            role: self.role,
            address: self.address.clone(),
            map_version,
            speaks: binary_speaks(),
            ready,
            draining,
            partitions,
        };
        let response = if self.can_handoff() {
            self.client
                .report_status_with_lifecycle(self.node, status)
                .await?
        } else {
            self.client
                .report_status_for_version(self.node, status)
                .await?
        };
        match response {
            // No version in the reply means the leader predates them, which
            // mid-rollout is normal; this node keeps whatever it last knew.
            StatusReportResponse::Accepted {
                map_version,
                cluster_version: None,
            } => {
                if binary_speaks().contains(ClusterVersion::ZERO) {
                    Ok(StatusReportResponse::Accepted {
                        map_version,
                        cluster_version: None,
                    })
                } else {
                    let refusal = CompatibilityRefusal {
                        speaks: binary_speaks(),
                        active: ClusterVersion::ZERO,
                    };
                    tracing::warn!(
                        active_cluster_version = %refusal.active,
                        speaks = %refusal.speaks,
                        reason = %refusal,
                        "the leader predates version reporting and this binary cannot speak its legacy cluster version"
                    );
                    Ok(StatusReportResponse::Incompatible(refusal))
                }
            }
            StatusReportResponse::Accepted {
                map_version,
                cluster_version: Some(cluster_version),
            } => {
                *self.active.lock().expect("active version poisoned") = Some(cluster_version);
                Ok(StatusReportResponse::Accepted {
                    map_version,
                    cluster_version: Some(cluster_version),
                })
            }
            StatusReportResponse::Incompatible(refusal) => {
                *self.active.lock().expect("active version poisoned") = Some(refusal.active);
                tracing::warn!(
                    active_cluster_version = %refusal.active,
                    speaks = %refusal.speaks,
                    reason = %refusal,
                    "the control plane refused this node because it is outside the supported upgrade window"
                );
                Ok(StatusReportResponse::Incompatible(refusal))
            }
        }
    }

    /// Requests one control-plane drain pass for this node.
    pub async fn drain_node(&self) -> Result<bool> {
        self.client.drain_node(self.node).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orbita_control::binary_version;
    use orbita_sim::Simulation;

    /// A reporter that has been told `active` is the cluster's version.
    ///
    /// The transport is never used: `can_handoff` is a pure decision about
    /// two version numbers, and it is the decision, not the round trip, that
    /// issue #105 got wrong.
    fn reporter_told(active: Option<ClusterVersion>) -> StatusReporter<orbita_sim::SimRuntime> {
        let sim = Simulation::new(1);
        let runtime = sim.runtime(NodeId(1));
        let reporter = StatusReporter::new(
            ControlClient::new(runtime, vec![NodeId(2)]),
            NodeId(1),
            "10.0.0.1:7000",
        );
        *reporter.active.lock().expect("active version poisoned") = active;
        reporter
    }

    #[test]
    fn a_worker_claims_lifecycle_state_whenever_the_active_version_carries_it() {
        // The half of #105 that lives on the worker. A binary that is not the
        // one the cluster finalized on must still speak the protocol the
        // cluster finalized, because its leader is judging it by that
        // protocol. Asking whether the active version is *this binary's*
        // version says no for every node in the n-1 window, which is every
        // node a rolling upgrade has just replaced.
        let own = binary_version();
        let ahead = ClusterVersion::new(own.major, own.minor + 1);
        assert!(
            reporter_told(Some(ahead)).can_handoff(),
            "a cluster past 0.1 carries lifecycle state whatever this binary's own version is"
        );
        assert!(reporter_told(Some(orbita_control::PROTOCOL_0_1)).can_handoff());
    }

    #[test]
    fn a_worker_makes_no_lifecycle_claim_before_the_protocol_carries_one() {
        // The other side of the same rule, and the one that makes the fix
        // safe: on a cluster still below 0.1 nobody claims readiness, so no
        // leader is entitled to demand it. That is what lets a new-binary
        // worker be placed by an old-binary leader.
        assert!(!reporter_told(Some(ClusterVersion::ZERO)).can_handoff());
        assert!(
            !reporter_told(None).can_handoff(),
            "a node that has not heard an active version yet assumes nothing"
        );
    }

    #[test]
    fn a_leader_group_node_never_claims_worker_lifecycle_state() {
        let reporter = StatusReporter::new_with_role(
            ControlClient::new(Simulation::new(1).runtime(NodeId(1)), vec![NodeId(2)]),
            NodeId(1),
            NodeRole::Leader,
            "10.0.0.1:7000",
        );
        *reporter.active.lock().expect("active version poisoned") =
            Some(orbita_control::PROTOCOL_0_1);
        assert!(
            !reporter.can_handoff(),
            "a voter owns no partitions to hand"
        );
    }
}
