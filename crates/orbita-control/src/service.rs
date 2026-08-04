//! The leader group's side of the worker protocol.
//!
//! Registered against `ServiceId::Control`. It answers two questions and takes
//! no decisions of its own, so a slow or hostile worker can waste a leader's
//! time but cannot move the cluster.

use crate::consensus::ConsensusLog;
use crate::controller::Controller;
use crate::wire::{
    ControlResponse, DrainNodeRequest, FetchMapRequest, ReportStatusRequest, METHOD_DRAIN_NODE,
    METHOD_FETCH_MAP, METHOD_FETCH_NODES, METHOD_REPORT_STATUS, METHOD_REPORT_STATUS_V2,
    METHOD_REPORT_STATUS_V3,
};

use bytes::Bytes;
use orbita_runtime::{NodeId, PeerCall, PeerHandler, Runtime, TransportResult};

/// Serves inbound control traffic for one leader group node.
pub struct ControlService<R: Runtime, L: ConsensusLog> {
    controller: Controller<R, L>,
}

impl<R: Runtime, L: ConsensusLog> Clone for ControlService<R, L> {
    fn clone(&self) -> Self {
        Self {
            controller: self.controller.clone(),
        }
    }
}

impl<R: Runtime, L: ConsensusLog> ControlService<R, L> {
    #[must_use]
    pub fn new(controller: Controller<R, L>) -> Self {
        Self { controller }
    }

    async fn dispatch(&self, call: PeerCall) -> ControlResponse {
        match call.method {
            METHOD_FETCH_MAP => match FetchMapRequest::decode(&call.payload) {
                Ok(request) => {
                    ControlResponse::Map(self.controller.partition_map_if_newer(request.have).await)
                }
                Err(e) => ControlResponse::Error(format!("undecodable fetch: {e}")),
            },
            // The v0.0.1 shape, from a worker old enough not to know about
            // versions. The reply must end at the map version, because that
            // worker rejects trailing bytes.
            METHOD_REPORT_STATUS => match ReportStatusRequest::decode_legacy(&call.payload) {
                Ok(request) => match self
                    .controller
                    .record_status(request.node, request.status)
                    .await
                {
                    Ok(map_version) => ControlResponse::Accepted {
                        map_version,
                        cluster_version: None,
                    },
                    Err(e) => ControlResponse::Error(e.to_string()),
                },
                Err(e) => ControlResponse::Error(format!("undecodable status: {e}")),
            },
            METHOD_REPORT_STATUS_V2 => match ReportStatusRequest::decode_v2(&call.payload) {
                Ok(request) => match self
                    .controller
                    .record_status(request.node, request.status)
                    .await
                {
                    Ok(map_version) => ControlResponse::Accepted {
                        map_version,
                        cluster_version: Some(self.controller.cluster_version().await),
                    },
                    Err(e) => ControlResponse::Error(e.to_string()),
                },
                Err(e) => ControlResponse::Error(format!("undecodable status: {e}")),
            },
            METHOD_REPORT_STATUS_V3 => match ReportStatusRequest::decode(&call.payload) {
                Ok(request) => match self
                    .controller
                    .record_status(request.node, request.status)
                    .await
                {
                    Ok(map_version) => ControlResponse::Accepted {
                        map_version,
                        cluster_version: Some(self.controller.cluster_version().await),
                    },
                    Err(e) => ControlResponse::Error(e.to_string()),
                },
                Err(e) => ControlResponse::Error(format!("undecodable status: {e}")),
            },
            METHOD_DRAIN_NODE => match DrainNodeRequest::decode(&call.payload) {
                Ok(request) => match self.controller.drain_node(request.node).await {
                    Ok(true) => ControlResponse::Accepted {
                        map_version: self.controller.map_version().await,
                        cluster_version: Some(self.controller.cluster_version().await),
                    },
                    Ok(false) => ControlResponse::Error(
                        "ownership transfers committed; waiting for receiving owners to report the new map ready"
                            .into(),
                    ),
                    Err(e) => ControlResponse::Error(e.to_string()),
                },
                Err(e) => ControlResponse::Error(format!("undecodable drain request: {e}")),
            },
            METHOD_FETCH_NODES => ControlResponse::Nodes(self.controller.node_addresses().await),
            other => ControlResponse::Error(format!("unknown control method {other}")),
        }
    }
}

impl<R: Runtime, L: ConsensusLog> PeerHandler for ControlService<R, L> {
    async fn handle(&self, _from: NodeId, call: PeerCall) -> TransportResult<Bytes> {
        // A node that is not the leader answers with the redirect rather than
        // with its own stale copy of the map. Serving a follower's map would
        // be the control plane handing out routing it is not sure about, which
        // is the one thing it exists not to do.
        if !self.controller.log_is_leader().await {
            let leader = self.controller.log_leader().await;
            return Ok(ControlResponse::NotLeader { leader }.encode());
        }
        // Errors travel as a response rather than as a transport failure,
        // because "the leader considered this and said no" and "the leader was
        // unreachable" mean opposite things to a caller: the first must not be
        // retried against another node and the second must be.
        Ok(self.dispatch(call).await.encode())
    }
}
