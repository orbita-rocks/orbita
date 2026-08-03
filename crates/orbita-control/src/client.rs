//! The handle every other crate uses to talk to the leader group.
//!
//! This is the whole dependency the data plane has on the control plane, and
//! it is shaped so that a control plane outage cannot become a data plane
//! outage. Nothing here is called on a request. A worker refreshes its map on
//! a timer and heartbeats on a timer, and if both fail it keeps serving from
//! the map it already has. The map is a value, not a lookup service.

use crate::consensus::ConsensusLog;
use crate::controller::Controller;
use crate::membership::NodeStatus;
use crate::wire::{
    ControlResponse, FetchMapRequest, ReportStatusRequest, METHOD_FETCH_MAP, METHOD_REPORT_STATUS,
};

use orbita_core::{Error, MapVersion, NodeId, PartitionMap, Result};
use orbita_runtime::{PeerCall, Runtime, ServiceId, Transport, TransportError};

use std::sync::Mutex;

/// A client for the leader group.
///
/// Cheap to clone, and clones share the memory of which member answered last,
/// so a worker that has found the leader once does not re-sweep the group on
/// every call.
pub struct ControlClient<R: Runtime> {
    runtime: R,
    members: Vec<NodeId>,
    preferred: std::sync::Arc<Mutex<Option<NodeId>>>,
}

impl<R: Runtime> Clone for ControlClient<R> {
    fn clone(&self) -> Self {
        Self {
            runtime: self.runtime.clone(),
            members: self.members.clone(),
            preferred: std::sync::Arc::clone(&self.preferred),
        }
    }
}

impl<R: Runtime> ControlClient<R> {
    /// Builds a client over a known set of leader group members.
    ///
    /// The membership is configuration rather than discovery, because
    /// discovering the control plane requires something to ask, and that
    /// something would be another control plane.
    #[must_use]
    pub fn new(runtime: R, members: Vec<NodeId>) -> Self {
        Self {
            runtime,
            members,
            preferred: std::sync::Arc::new(Mutex::new(None)),
        }
    }

    /// The current partition map.
    pub async fn fetch_map(&self) -> Result<PartitionMap> {
        self.fetch_map_if_newer(MapVersion::default())
            .await?
            .ok_or_else(|| Error::Internal("the leader group returned no map at version 0".into()))
    }

    /// The partition map, but only if it has moved past `have`.
    ///
    /// This is what a worker's refresh loop should call. In a quiet cluster
    /// every poll answers in a handful of bytes, which is what makes polling
    /// often enough to matter affordable.
    pub async fn fetch_map_if_newer(&self, have: MapVersion) -> Result<Option<PartitionMap>> {
        let payload = FetchMapRequest { have }.encode();
        match self.call(METHOD_FETCH_MAP, payload).await? {
            ControlResponse::Map(map) => Ok(map),
            other => Err(unexpected(&other)),
        }
    }

    /// Reports this node's own view of itself: its role, its address, and how
    /// far it has got on every partition it holds.
    ///
    /// This doubles as the heartbeat. Sending it on a timer is what keeps the
    /// node out of the failure detector, and the progress it carries is what
    /// the leader group uses to choose a replacement owner if it stops.
    pub async fn report_status(&self, node: NodeId, status: NodeStatus) -> Result<()> {
        let payload = ReportStatusRequest { node, status }.encode();
        match self.call(METHOD_REPORT_STATUS, payload).await? {
            ControlResponse::Accepted { .. } => Ok(()),
            other => Err(unexpected(&other)),
        }
    }

    /// The same as [`ControlClient::report_status`], but hands back the map
    /// version the leader group is on so a caller can tell in one round trip
    /// whether its routing is stale.
    pub async fn report_status_for_version(
        &self,
        node: NodeId,
        status: NodeStatus,
    ) -> Result<MapVersion> {
        let payload = ReportStatusRequest { node, status }.encode();
        match self.call(METHOD_REPORT_STATUS, payload).await? {
            ControlResponse::Accepted { map_version } => Ok(map_version),
            other => Err(unexpected(&other)),
        }
    }

    /// Tries the member that answered last, then the rest, following one
    /// redirect.
    ///
    /// Unreachable members are skipped and the next one is tried, because a
    /// leader group is expected to be missing a member from time to time and
    /// that is not something the caller should have to handle.
    async fn call(&self, method: u16, payload: bytes::Bytes) -> Result<ControlResponse> {
        let mut queue: std::collections::VecDeque<NodeId> = std::collections::VecDeque::new();
        if let Some(preferred) = *self.preferred.lock().expect("control client lock poisoned") {
            queue.push_back(preferred);
        }
        for member in &self.members {
            if !queue.contains(member) {
                queue.push_back(*member);
            }
        }
        if queue.is_empty() {
            return Err(Error::Unavailable(
                "no leader group members are configured".into(),
            ));
        }

        let mut last: Option<Error> = None;
        // At most one redirect is followed. A second would mean the group is
        // mid-election, and looping through an election is how a client turns
        // a brief unavailability into a stuck request.
        let mut followed_redirect = false;

        while let Some(target) = queue.pop_front() {
            match self.call_one(target, method, payload.clone()).await {
                Ok(ControlResponse::NotLeader { leader }) => {
                    if let Some(leader) = leader {
                        if !followed_redirect && leader != target {
                            followed_redirect = true;
                            queue.push_front(leader);
                        }
                    }
                    last = Some(Error::Unavailable(format!(
                        "node {target} is not the leader"
                    )));
                }
                Ok(ControlResponse::Error(message)) => {
                    // The leader considered the request and refused it. Trying
                    // another member would get the same answer.
                    return Err(Error::Internal(message));
                }
                Ok(response) => {
                    *self.preferred.lock().expect("control client lock poisoned") = Some(target);
                    return Ok(response);
                }
                Err(e) => last = Some(e),
            }
        }

        Err(last.unwrap_or_else(|| Error::Unavailable("the leader group did not answer".into())))
    }

    async fn call_one(
        &self,
        to: NodeId,
        method: u16,
        payload: bytes::Bytes,
    ) -> Result<ControlResponse> {
        let call = PeerCall {
            service: ServiceId::Control,
            method,
            payload,
        };
        let bytes = self
            .runtime
            .transport()
            .call(to, call)
            .await
            .map_err(transport_error)?;
        ControlResponse::decode(&bytes).map_err(Error::from)
    }
}

/// A client that talks to a controller in this process, with no transport in
/// between.
///
/// A single-node cluster is one process that is both the leader group and the
/// only worker, and making it send itself a message over the network to learn
/// its own partition map would be a round trip to nowhere.
pub struct LocalControlClient<R: Runtime, L: ConsensusLog> {
    controller: Controller<R, L>,
}

impl<R: Runtime, L: ConsensusLog> Clone for LocalControlClient<R, L> {
    fn clone(&self) -> Self {
        Self {
            controller: self.controller.clone(),
        }
    }
}

impl<R: Runtime, L: ConsensusLog> LocalControlClient<R, L> {
    #[must_use]
    pub fn new(controller: Controller<R, L>) -> Self {
        Self { controller }
    }

    pub async fn fetch_map(&self) -> Result<PartitionMap> {
        Ok(self.controller.partition_map().await)
    }

    pub async fn fetch_map_if_newer(&self, have: MapVersion) -> Result<Option<PartitionMap>> {
        Ok(self.controller.partition_map_if_newer(have).await)
    }

    pub async fn report_status(&self, node: NodeId, status: NodeStatus) -> Result<()> {
        self.controller
            .record_status(node, status)
            .await
            .map(|_| ())
    }
}

fn transport_error(e: TransportError) -> Error {
    match e {
        TransportError::Timeout(node) => {
            Error::Unavailable(format!("leader group node {node} timed out"))
        }
        TransportError::Unreachable(node) | TransportError::UnknownPeer(node) => {
            Error::Unavailable(format!("leader group node {node} is unreachable"))
        }
        TransportError::NoHandler(_) => {
            Error::Unavailable("the peer is not serving the control protocol".into())
        }
        TransportError::Remote(message) => Error::Internal(message),
    }
}

fn unexpected(response: &ControlResponse) -> Error {
    Error::Internal(format!(
        "the leader group answered a different question: {response:?}"
    ))
}
