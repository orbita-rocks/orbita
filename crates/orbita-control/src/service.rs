//! The leader group's side of the worker protocol.
//!
//! Registered against `ServiceId::Control`. It answers two questions and takes
//! no decisions of its own, so a slow or hostile worker can waste a leader's
//! time but cannot move the cluster.

use crate::consensus::ConsensusLog;
use crate::controller::Controller;
use crate::wire::{
    ControlResponse, FetchMapRequest, ReportStatusRequest, METHOD_FETCH_COMMIT_INDEX,
    METHOD_FETCH_MAP, METHOD_FETCH_NODES, METHOD_REPORT_STATUS, METHOD_REPORT_STATUS_V2,
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
            METHOD_REPORT_STATUS_V2 => match ReportStatusRequest::decode(&call.payload) {
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
            METHOD_FETCH_NODES => ControlResponse::Nodes(self.controller.node_addresses().await),
            METHOD_FETCH_COMMIT_INDEX => {
                ControlResponse::CommitIndex(self.controller.commit_index().await)
            }
            other => ControlResponse::Error(format!("unknown control method {other}")),
        }
    }
}

impl<R: Runtime, L: ConsensusLog> PeerHandler for ControlService<R, L> {
    async fn handle(&self, _from: NodeId, call: PeerCall) -> TransportResult<Bytes> {
        // Raft role alone is not authority: a new leader first applies the
        // committed prefix it inherited, and ReadIndex proves it still has a
        // quorum after doing so. A deposed minority therefore cannot serve its
        // stale map, and a new majority cannot serve before its fence is
        // visible locally.
        if let Err(error) = self.controller.ensure_leader_ready().await {
            let response = match error {
                orbita_core::Error::NotLeader { leader } => ControlResponse::NotLeader { leader },
                other => ControlResponse::Unavailable(format!(
                    "control leader is not ready to serve: {other}"
                )),
            };
            return Ok(response.encode());
        }
        // Errors travel as a response rather than as a transport failure,
        // because "the leader considered this and said no" and "the leader was
        // unreachable" mean opposite things to a caller: the first must not be
        // retried against another node and the second must be.
        Ok(self.dispatch(call).await.encode())
    }
}
