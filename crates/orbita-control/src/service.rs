//! The leader group's side of the worker protocol.
//!
//! Registered against `ServiceId::Control`. It answers a handful of questions
//! and takes no decisions of its own, so a slow or hostile worker can waste a
//! leader's time but cannot move the cluster.
//!
//! One of those questions is an `Admin` call another node forwarded here.
//! That belongs on this service rather than on one of its own because the
//! leader readiness gate below is exactly the check a forwarded admin call
//! needs, and because [`crate::ControlClient`] already turns "somebody in this
//! group" into "the member that is currently leader".

use crate::admin::AdminService;
use crate::consensus::ConsensusLog;
use crate::controller::Controller;
use crate::controller::RegistrationOutcome;
use crate::wire::{
    AdminCallRequest, ControlResponse, DrainNodeRequest, FetchMapRequest, ReportStatusRequest,
    METHOD_ADMIN_CALL, METHOD_DRAIN_NODE, METHOD_FETCH_COMMIT_INDEX, METHOD_FETCH_MAP,
    METHOD_FETCH_NODES, METHOD_REPORT_STATUS, METHOD_REPORT_STATUS_V2, METHOD_REPORT_STATUS_V3,
    METHOD_REPORT_STATUS_V4, METHOD_REPORT_STATUS_V5,
};

use bytes::Bytes;
use orbita_runtime::{NodeId, PeerCall, PeerHandler, Runtime, TransportResult};

/// Serves inbound control traffic for one leader group node.
pub struct ControlService<R: Runtime, L: ConsensusLog> {
    controller: Controller<R, L>,
    /// The admin surface a forwarded call runs against. Local-only by
    /// construction, so a call that arrived here can never be sent on again
    /// and two members mid-election cannot bounce one between them.
    admin: AdminService<R, L>,
}

impl<R: Runtime, L: ConsensusLog> Clone for ControlService<R, L> {
    fn clone(&self) -> Self {
        Self {
            controller: self.controller.clone(),
            admin: self.admin.clone(),
        }
    }
}

impl<R: Runtime, L: ConsensusLog> ControlService<R, L> {
    #[must_use]
    pub fn new(controller: Controller<R, L>) -> Self {
        Self {
            admin: AdminService::new(controller.clone()),
            controller,
        }
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
                    Ok(RegistrationOutcome::Accepted(map_version)) => ControlResponse::Accepted {
                        map_version,
                        cluster_version: None,
                    },
                    Ok(RegistrationOutcome::Incompatible(refusal)) => {
                        // A v0.0.1 worker can decode the ordinary error shape,
                        // but not the structured v2 refusal.
                        ControlResponse::Error(refusal.to_string())
                    }
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
                    Ok(RegistrationOutcome::Accepted(map_version)) => ControlResponse::Accepted {
                        map_version,
                        cluster_version: Some(self.controller.cluster_version().await),
                    },
                    Ok(RegistrationOutcome::Incompatible(refusal)) => {
                        ControlResponse::Incompatible(refusal)
                    }
                    Err(e) => ControlResponse::Error(e.to_string()),
                },
                Err(e) => ControlResponse::Error(format!("undecodable status: {e}")),
            },
            METHOD_REPORT_STATUS_V3 => match ReportStatusRequest::decode_v3(&call.payload) {
                Ok(request) => match self
                    .controller
                    .record_status(request.node, request.status)
                    .await
                {
                    Ok(RegistrationOutcome::Accepted(map_version)) => ControlResponse::Accepted {
                        map_version,
                        cluster_version: Some(self.controller.cluster_version().await),
                    },
                    Ok(RegistrationOutcome::Incompatible(refusal)) => {
                        ControlResponse::Incompatible(refusal)
                    }
                    Err(e) => ControlResponse::Error(e.to_string()),
                },
                Err(e) => ControlResponse::Error(format!("undecodable status: {e}")),
            },
            METHOD_REPORT_STATUS_V4 => match ReportStatusRequest::decode_v4(&call.payload) {
                Ok(request) => match self
                    .controller
                    .record_status(request.node, request.status)
                    .await
                {
                    Ok(RegistrationOutcome::Accepted(map_version)) => ControlResponse::Accepted {
                        map_version,
                        cluster_version: Some(self.controller.cluster_version().await),
                    },
                    Ok(RegistrationOutcome::Incompatible(refusal)) => {
                        ControlResponse::Incompatible(refusal)
                    }
                    Err(e) => ControlResponse::Error(e.to_string()),
                },
                Err(e) => ControlResponse::Error(format!("undecodable status: {e}")),
            },
            METHOD_REPORT_STATUS_V5 => match ReportStatusRequest::decode(&call.payload) {
                Ok(request) => match self
                    .controller
                    .record_status(request.node, request.status)
                    .await
                {
                    Ok(RegistrationOutcome::Accepted(map_version)) => ControlResponse::Accepted {
                        map_version,
                        cluster_version: Some(self.controller.cluster_version().await),
                    },
                    Ok(RegistrationOutcome::Incompatible(refusal)) => {
                        ControlResponse::Incompatible(refusal)
                    }
                    Err(e) => ControlResponse::Error(e.to_string()),
                },
                Err(e) => ControlResponse::Error(format!("undecodable status: {e}")),
            },
            METHOD_DRAIN_NODE => match DrainNodeRequest::decode(&call.payload) {
                Ok(request) => match self.controller.drain_node(request.node).await {
                    Ok(complete) => ControlResponse::DrainProgress {
                        complete,
                        map_version: self.controller.map_version().await,
                    },
                    Err(e) => ControlResponse::Error(e.to_string()),
                },
                Err(e) => ControlResponse::Error(format!("undecodable drain request: {e}")),
            },
            METHOD_ADMIN_CALL => match AdminCallRequest::decode(&call.payload) {
                Ok(request) => match self.admin.invoke(request.method, &request.payload).await {
                    Ok(payload) => ControlResponse::AdminOk(payload),
                    // The leader's own status code travels back rather than
                    // being flattened into an error string, because "no such
                    // keyspace" and "the leader is gone" are the two answers
                    // an operator most needs to tell apart.
                    Err(status) => ControlResponse::AdminFailed {
                        code: status.code() as u32,
                        message: status.message().to_string(),
                    },
                },
                Err(e) => ControlResponse::Error(format!("undecodable admin call: {e}")),
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
