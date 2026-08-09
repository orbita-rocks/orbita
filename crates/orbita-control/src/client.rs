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
use crate::model::Credential;
use crate::version::{ClusterVersion, CompatibilityRefusal};
use crate::wire::{
    AdminCallRequest, ControlResponse, DrainNodeRequest, FetchMapRequest, FetchSplitIntentsRequest,
    ReportSplitPreparedV2Request, ReportStatusRequest, METHOD_ADMIN_CALL, METHOD_DRAIN_NODE,
    METHOD_FETCH_AUTH_POLICY, METHOD_FETCH_COMMIT_INDEX, METHOD_FETCH_CREDENTIALS,
    METHOD_FETCH_MAP, METHOD_FETCH_NODES, METHOD_FETCH_SPLIT_INTENTS,
    METHOD_FETCH_SPLIT_INTENTS_V2, METHOD_REPORT_SPLIT_PREPARED_V2, METHOD_REPORT_STATUS,
    METHOD_REPORT_STATUS_V2, METHOD_REPORT_STATUS_V3, METHOD_REPORT_STATUS_V4,
    METHOD_REPORT_STATUS_V5, METHOD_REPORT_STATUS_V6,
};

use orbita_core::{Error, MapVersion, NodeId, PartitionId, PartitionMap, Result};
use orbita_runtime::{PeerCall, Runtime, ServiceId, Transport, TransportError};

use std::sync::Mutex;

/// What the control leader answered a forwarded admin call with.
///
/// A refusal is an outcome rather than an error because the two mean opposite
/// things to the operator holding the terminal: `Failed` is the leader's own
/// considered answer and must reach them intact, while an `Err` from
/// [`ControlClient::admin_call`] means nobody was able to consider it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminOutcome {
    /// The encoded protobuf response, to be handed back untouched.
    Ok(bytes::Bytes),
    /// A `tonic::Code` discriminant and the leader's message.
    Failed { code: u32, message: String },
}

/// The leader group's answer to a version-aware status report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusReportResponse {
    Accepted {
        map_version: MapVersion,
        cluster_version: Option<ClusterVersion>,
    },
    Incompatible(CompatibilityRefusal),
}

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

/// A control call's private transport/protocol outcome.
///
/// Old leaders can express an unsupported method only through a fixed error
/// string. It is decoded once at this boundary so compatibility callers never
/// have to recover protocol meaning from [`Error::Internal`].
#[derive(Debug)]
enum CallError {
    UnsupportedMethod(u16),
    Failed(Error),
}

impl CallError {
    fn into_error(self) -> Error {
        match self {
            Self::UnsupportedMethod(method) => Error::Internal(format!(
                "the leader group does not support control method {method}"
            )),
            Self::Failed(error) => error,
        }
    }
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
        match self.call_required(METHOD_FETCH_MAP, payload).await? {
            ControlResponse::Map(map) => Ok(map),
            other => Err(unexpected(&other)),
        }
    }

    /// Every node the cluster knows, and where peers reach it.
    ///
    /// A worker calls this on the same timer as its heartbeat. The map names
    /// the owner of a partition by id, and this is the only thing that turns
    /// that id into an address, so without it a node can route a request and
    /// still not be able to send it.
    pub async fn fetch_nodes(&self) -> Result<Vec<(NodeId, String)>> {
        match self
            .call_required(METHOD_FETCH_NODES, bytes::Bytes::new())
            .await?
        {
            ControlResponse::Nodes(nodes) => Ok(nodes),
            other => Err(unexpected(&other)),
        }
    }

    /// Every live credential, so a worker can enforce authentication locally.
    ///
    /// A worker calls this on the same timer as its map. The credentials carry
    /// secret hashes, never secrets, and are exactly what the leader group
    /// already replicates, so caching them widens no trust boundary. Enforcing
    /// from the cache is what keeps authentication off the control plane: a
    /// control plane outage leaves a worker checking the last set it fetched
    /// rather than failing every request closed.
    pub async fn fetch_credentials(&self) -> Result<Vec<Credential>> {
        match self
            .call_required(METHOD_FETCH_CREDENTIALS, bytes::Bytes::new())
            .await?
        {
            ControlResponse::Credentials(credentials) => Ok(credentials),
            other => Err(unexpected(&other)),
        }
    }

    /// Whether the leader group requires authentication.
    ///
    /// A node checks this against its own `require_auth` before reporting
    /// ready, so a half-rolled change to the setting cannot leave an
    /// auth-disabled node accepting unauthenticated requests behind the load
    /// balancer while its peers reject them. An `Err` — including from a leader
    /// too old to serve the method — means the policy could not be determined,
    /// and the caller does not gate on it rather than guessing.
    pub async fn fetch_auth_policy(&self) -> Result<bool> {
        match self
            .call_required(METHOD_FETCH_AUTH_POLICY, bytes::Bytes::new())
            .await?
        {
            ControlResponse::AuthPolicy(require_auth) => Ok(require_auth),
            other => Err(unexpected(&other)),
        }
    }

    /// The current leader's committed control-command index.
    ///
    /// A voter compares this authority with its own log and applied state
    /// before becoming Ready. Merely observing a leader does not prove the
    /// voter has the decisions that leader may need for the next quorum.
    pub async fn fetch_commit_index(&self) -> Result<crate::LogIndex> {
        match self
            .call_required(METHOD_FETCH_COMMIT_INDEX, bytes::Bytes::new())
            .await?
        {
            ControlResponse::CommitIndex(index) => Ok(index),
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
        match self.send_status(node, status, false).await? {
            StatusReportResponse::Accepted { .. } => Ok(()),
            StatusReportResponse::Incompatible(refusal) => {
                Err(Error::InvalidArgument(refusal.to_string()))
            }
        }
    }

    /// The same as [`ControlClient::report_status`], but hands back what the
    /// leader group said yes with: the map version it is on, so a caller can
    /// tell in one round trip whether its routing is stale, and the active
    /// cluster version, which is how a node learns what to speak. The
    /// version is `None` when the leader that answered predates versions.
    pub async fn report_status_for_version(
        &self,
        node: NodeId,
        status: NodeStatus,
    ) -> Result<StatusReportResponse> {
        self.send_status(node, status, false).await
    }

    /// Reports lifecycle state after the active protocol enables its wire shape.
    pub async fn report_status_with_lifecycle(
        &self,
        node: NodeId,
        status: NodeStatus,
    ) -> Result<StatusReportResponse> {
        self.send_status(node, status, true).await
    }

    /// Runs one `Admin` gRPC call on whichever member is currently the leader.
    ///
    /// This is the only thing in this file that is called on a request rather
    /// than on a timer, and it is safe to be: an admin call is an operator
    /// action, not data plane traffic, so a control plane outage makes it fail
    /// rather than making a worker fail. What it buys is that an operator may
    /// point `orbita` at any node in the cluster, which is the same promise
    /// the KV path already makes.
    ///
    /// `op_id` names this invocation so that a resend the transport makes after
    /// an ambiguous loss is recognisable as the same operation on the leader,
    /// rather than a fresh one. The caller mints it once and passes the same
    /// value into every retry; regenerating it per attempt would defeat the
    /// point.
    pub async fn admin_call(
        &self,
        method: u32,
        op_id: u128,
        payload: bytes::Bytes,
    ) -> Result<AdminOutcome> {
        let request = AdminCallRequest {
            method,
            op_id,
            payload,
        }
        .encode();
        match self.call_required(METHOD_ADMIN_CALL, request).await? {
            ControlResponse::AdminOk(payload) => Ok(AdminOutcome::Ok(payload)),
            ControlResponse::AdminFailed { code, message } => {
                Ok(AdminOutcome::Failed { code, message })
            }
            other => Err(unexpected(&other)),
        }
    }

    /// Asks the leader to transfer every partition this node still owns.
    /// Returns whether every receiver has acknowledged its handoff map.
    pub async fn drain_node(&self, node: NodeId) -> Result<bool> {
        match self
            .call_required(METHOD_DRAIN_NODE, DrainNodeRequest { node }.encode())
            .await?
        {
            ControlResponse::DrainProgress { complete, .. } => Ok(complete),
            other => Err(unexpected(&other)),
        }
    }

    /// Fetches the splits `node` must prepare child storage for.
    ///
    /// Polled beside the map. The intents cannot ride the map — a frozen
    /// contract that carries only live partitions — so a pending split's
    /// children reach the worker through this instead. See ADR 0009.
    ///
    /// This is the one control method a leader group is allowed not to serve.
    /// A leader binary without the split vocabulary cannot be holding a split
    /// intent, so "no such method" and "no active intents" are the same fact,
    /// and answering the empty set is what lets a new worker boot against an
    /// old leader mid-rollout. Every other refusal — including an unreachable
    /// group — is still an error, because those say nothing about whether a
    /// split is in flight.
    pub async fn fetch_split_intents(&self, node: NodeId) -> Result<crate::SplitIntentSnapshot> {
        match self
            .call(
                METHOD_FETCH_SPLIT_INTENTS_V2,
                FetchSplitIntentsRequest { node }.encode(),
            )
            .await
        {
            Ok(ControlResponse::SplitIntentsV2(snapshot)) => Ok(snapshot),
            Err(CallError::UnsupportedMethod(METHOD_FETCH_SPLIT_INTENTS_V2)) => {
                match self
                    .call(
                        METHOD_FETCH_SPLIT_INTENTS,
                        FetchSplitIntentsRequest { node }.encode(),
                    )
                    .await
                {
                    Ok(ControlResponse::SplitIntents(intents)) if intents.is_empty() => {}
                    Err(CallError::UnsupportedMethod(METHOD_FETCH_SPLIT_INTENTS)) => {}
                    Ok(ControlResponse::SplitIntents(_)) => {
                        return Err(Error::Unavailable(
                            "the leader uses parent-scoped split acknowledgements; finalize the upgrade before splitting"
                                .into(),
                        ));
                    }
                    Ok(other) => return Err(unexpected(&other)),
                    Err(error) => return Err(error.into_error()),
                }
                let map_version = self
                    .fetch_map_if_newer(MapVersion::default())
                    .await?
                    .map_or(MapVersion::default(), |map| map.version());
                Ok(crate::SplitIntentSnapshot {
                    map_version,
                    intents: Vec::new(),
                })
            }
            Ok(other) => Err(unexpected(&other)),
            Err(error) => Err(error.into_error()),
        }
    }

    /// Reports that `node` has durably prepared its child storage for the split
    /// of `parent`. This is the real acknowledgement the completion waits on.
    pub async fn report_split_prepared(
        &self,
        node: NodeId,
        parent: PartitionId,
        lower: PartitionId,
        upper: PartitionId,
    ) -> Result<()> {
        match self
            .call_required(
                METHOD_REPORT_SPLIT_PREPARED_V2,
                ReportSplitPreparedV2Request {
                    node,
                    parent,
                    lower,
                    upper,
                }
                .encode(),
            )
            .await?
        {
            ControlResponse::SplitPrepared => Ok(()),
            other => Err(unexpected(&other)),
        }
    }

    /// Sends the newest report shape, falling back one protocol generation at
    /// a time when a leader does not serve it.
    ///
    /// The fallback is what keeps heartbeats landing mid-rollout: without it,
    /// an upgraded worker reporting to a not-yet-upgraded leader would be
    /// undecodable and drop out of the failure detector for the whole
    /// rollout. Delete alongside the legacy method when the window moves
    /// past 0.0.
    ///
    /// # Why the lifecycle claim is withheld rather than merely ignored
    ///
    /// It would be tempting to send the claim always and let the leader decide
    /// what to do with it, which would make issue #105 impossible on this end.
    /// The encoding forbids it. A `RegisterNode` carrying either bool goes on
    /// the log under a tag the previous binary cannot decode, and that binary
    /// truncates its log at the first entry it cannot read, so one such entry
    /// written during the upgrade window would take the rollback window with
    /// it. Withholding here is what keeps the log rollback-readable, and the
    /// cost — that a leader cannot tell "not ready" from "could not say" — is
    /// paid on the other side, by `ClusterState::lifecycle_enabled` declining
    /// to ask the question at all below 0.1.
    async fn send_status(
        &self,
        node: NodeId,
        status: NodeStatus,
        lifecycle: bool,
    ) -> Result<StatusReportResponse> {
        // Resource reporting is not lifecycle-gated, so both callers try the
        // newest shape. What the gate withholds before finalization is the
        // lifecycle claim itself, which is cleared here rather than by
        // dropping to an older wire shape that cannot carry index memory.
        let status = if lifecycle {
            status
        } else {
            NodeStatus {
                ready: false,
                draining: false,
                ..status
            }
        };
        let payload = ReportStatusRequest {
            node,
            status: status.clone(),
        }
        .encode();
        match self.call(METHOD_REPORT_STATUS_V6, payload).await {
            Ok(ControlResponse::Accepted {
                map_version,
                cluster_version,
            }) => Ok(StatusReportResponse::Accepted {
                map_version,
                cluster_version,
            }),
            Ok(ControlResponse::Incompatible(refusal)) => {
                Ok(StatusReportResponse::Incompatible(refusal))
            }
            Ok(other) => Err(unexpected(&other)),
            Err(CallError::UnsupportedMethod(METHOD_REPORT_STATUS_V6)) => {
                self.send_status_v5(node, status, lifecycle).await
            }
            Err(error) => Err(error.into_error()),
        }
    }

    async fn send_status_v5(
        &self,
        node: NodeId,
        status: NodeStatus,
        lifecycle: bool,
    ) -> Result<StatusReportResponse> {
        let payload = ReportStatusRequest {
            node,
            status: status.clone(),
        }
        .encode_v5();
        match self.call(METHOD_REPORT_STATUS_V5, payload).await {
            Ok(ControlResponse::Accepted {
                map_version,
                cluster_version,
            }) => Ok(StatusReportResponse::Accepted {
                map_version,
                cluster_version,
            }),
            Ok(ControlResponse::Incompatible(refusal)) => {
                Ok(StatusReportResponse::Incompatible(refusal))
            }
            Ok(other) => Err(unexpected(&other)),
            Err(CallError::UnsupportedMethod(METHOD_REPORT_STATUS_V5)) => {
                self.send_status_v4(node, status, lifecycle).await
            }
            Err(error) => Err(error.into_error()),
        }
    }

    /// One rung down: everything but the owner's committed prefix.
    async fn send_status_v4(
        &self,
        node: NodeId,
        status: NodeStatus,
        lifecycle: bool,
    ) -> Result<StatusReportResponse> {
        let payload = ReportStatusRequest {
            node,
            status: status.clone(),
        }
        .encode_v4();
        match self.call(METHOD_REPORT_STATUS_V4, payload).await {
            Ok(ControlResponse::Accepted {
                map_version,
                cluster_version,
            }) => Ok(StatusReportResponse::Accepted {
                map_version,
                cluster_version,
            }),
            Ok(ControlResponse::Incompatible(refusal)) => {
                Ok(StatusReportResponse::Incompatible(refusal))
            }
            Ok(other) => Err(unexpected(&other)),
            Err(CallError::UnsupportedMethod(METHOD_REPORT_STATUS_V4)) => {
                if lifecycle {
                    self.send_status_v3(node, status).await
                } else {
                    self.send_status_v2(node, status).await
                }
            }
            Err(error) => Err(error.into_error()),
        }
    }

    async fn send_status_v3(
        &self,
        node: NodeId,
        status: NodeStatus,
    ) -> Result<StatusReportResponse> {
        let payload = ReportStatusRequest {
            node,
            status: status.clone(),
        }
        .encode_v3();
        match self.call(METHOD_REPORT_STATUS_V3, payload).await {
            Ok(ControlResponse::Accepted {
                map_version,
                cluster_version,
            }) => Ok(StatusReportResponse::Accepted {
                map_version,
                cluster_version,
            }),
            Ok(ControlResponse::Incompatible(refusal)) => {
                Ok(StatusReportResponse::Incompatible(refusal))
            }
            Ok(other) => Err(unexpected(&other)),
            Err(CallError::UnsupportedMethod(METHOD_REPORT_STATUS_V3)) => {
                self.send_status_v2(node, status).await
            }
            Err(error) => Err(error.into_error()),
        }
    }

    async fn send_status_v2(
        &self,
        node: NodeId,
        status: NodeStatus,
    ) -> Result<StatusReportResponse> {
        let payload = ReportStatusRequest {
            node,
            status: status.clone(),
        }
        .encode_v2();
        match self.call(METHOD_REPORT_STATUS_V2, payload).await {
            Ok(ControlResponse::Accepted {
                map_version,
                cluster_version,
            }) => Ok(StatusReportResponse::Accepted {
                map_version,
                cluster_version,
            }),
            Ok(ControlResponse::Incompatible(refusal)) => {
                Ok(StatusReportResponse::Incompatible(refusal))
            }
            Err(CallError::UnsupportedMethod(METHOD_REPORT_STATUS_V2)) => {
                let payload = ReportStatusRequest { node, status }.encode_legacy();
                match self.call_required(METHOD_REPORT_STATUS, payload).await? {
                    ControlResponse::Accepted {
                        map_version,
                        cluster_version,
                    } => Ok(StatusReportResponse::Accepted {
                        map_version,
                        cluster_version,
                    }),
                    other => Err(unexpected(&other)),
                }
            }
            Ok(other) => Err(unexpected(&other)),
            Err(error) => Err(error.into_error()),
        }
    }

    /// Calls a method the leader group is required to serve.
    ///
    /// This is the default. An unsupported method becomes an ordinary error
    /// here, so only the handful of callers that deliberately match on
    /// [`CallError::UnsupportedMethod`] can degrade instead of failing — a
    /// method going missing is otherwise a real fault, not a compatibility
    /// event.
    async fn call_required(&self, method: u16, payload: bytes::Bytes) -> Result<ControlResponse> {
        self.call(method, payload)
            .await
            .map_err(CallError::into_error)
    }

    /// Tries the member that answered last, then the rest, following one
    /// redirect.
    ///
    /// Unreachable members are skipped and the next one is tried, because a
    /// leader group is expected to be missing a member from time to time and
    /// that is not something the caller should have to handle.
    async fn call(
        &self,
        method: u16,
        payload: bytes::Bytes,
    ) -> std::result::Result<ControlResponse, CallError> {
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
            return Err(CallError::Failed(Error::Unavailable(
                "no leader group members are configured".into(),
            )));
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
                    //
                    // The refusal is only a compatibility signal when it names
                    // the very method we just sent. Anything else is a refusal
                    // whose text merely resembles one, and must stay opaque.
                    if legacy_unsupported_method(&message) == Some(method) {
                        return Err(CallError::UnsupportedMethod(method));
                    }
                    return Err(CallError::Failed(Error::Internal(message)));
                }
                Ok(ControlResponse::Unavailable(message)) => {
                    last = Some(Error::Unavailable(message));
                }
                Ok(response) => {
                    *self.preferred.lock().expect("control client lock poisoned") = Some(target);
                    return Ok(response);
                }
                Err(e) => last = Some(e),
            }
        }

        Err(CallError::Failed(last.unwrap_or_else(|| {
            Error::Unavailable("the leader group did not answer".into())
        })))
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
        self.controller.ensure_leader_ready().await?;
        Ok(self.controller.partition_map().await)
    }

    pub async fn fetch_map_if_newer(&self, have: MapVersion) -> Result<Option<PartitionMap>> {
        self.controller.ensure_leader_ready().await?;
        Ok(self.controller.partition_map_if_newer(have).await)
    }

    pub async fn report_status(&self, node: NodeId, status: NodeStatus) -> Result<()> {
        self.controller.ensure_leader_ready().await?;
        match self.controller.record_status(node, status).await? {
            crate::controller::RegistrationOutcome::Accepted(_) => Ok(()),
            crate::controller::RegistrationOutcome::Incompatible(refusal) => {
                Err(Error::InvalidArgument(refusal.to_string()))
            }
        }
    }

    pub async fn fetch_nodes(&self) -> Result<Vec<(NodeId, String)>> {
        self.controller.ensure_leader_ready().await?;
        Ok(self.controller.node_addresses().await)
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

/// Decodes the only unsupported-method shape a pre-versioned leader can send.
/// Exact parsing matters: a leader's unrelated refusal may contain the words
/// "unknown control method" without being this compatibility signal.
fn legacy_unsupported_method(message: &str) -> Option<u16> {
    message
        .strip_prefix("unknown control method ")?
        .parse()
        .ok()
}

fn unexpected(response: &ControlResponse) -> Error {
    Error::Internal(format!(
        "the leader group answered a different question: {response:?}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::{NodeRole, NodeStatus};
    use crate::wire::{METHOD_REPORT_STATUS_V3, METHOD_REPORT_STATUS_V4, METHOD_REPORT_STATUS_V6};

    use orbita_runtime::{PeerCall, PeerHandler, ServiceId, TransportResult};
    use orbita_sim::Simulation;

    /// Behaves exactly like a v0.0.1 leader: it does not know the
    /// version-aware report method, and it decodes and answers the old
    /// shapes. If the fallback in [`ControlClient::send_status`] regresses,
    /// heartbeats to a leader like this stop landing mid-rollout.
    struct V001Leader;

    impl PeerHandler for V001Leader {
        async fn handle(
            &self,
            _from: orbita_runtime::NodeId,
            call: PeerCall,
        ) -> TransportResult<bytes::Bytes> {
            let response = match call.method {
                METHOD_REPORT_STATUS => match ReportStatusRequest::decode_legacy(&call.payload) {
                    // Decoding with the old shape is the assertion that the
                    // fallback re-encoded without the speakable range.
                    Ok(_) => ControlResponse::Accepted {
                        map_version: MapVersion(9),
                        cluster_version: None,
                    },
                    Err(e) => ControlResponse::Error(format!("undecodable status: {e}")),
                },
                METHOD_FETCH_MAP => ControlResponse::Map(Some(PartitionMap::new(MapVersion(9)))),
                other => ControlResponse::Error(format!("unknown control method {other}")),
            };
            Ok(response.encode())
        }
    }

    #[test]
    fn a_report_to_a_v0_0_1_leader_falls_back_to_the_shape_it_understands() {
        let sim = Simulation::new(1);
        let leader = sim.add_node(NodeId(1));
        let worker = sim.add_node(NodeId(2));
        orbita_runtime::Transport::register(leader.transport(), ServiceId::Control, V001Leader);

        let client = ControlClient::new(worker, vec![NodeId(1)]);
        let result = sim.block_on(async move {
            client
                .report_status_with_lifecycle(
                    NodeId(2),
                    NodeStatus::joining(NodeRole::Worker, "10.0.0.2:7000"),
                )
                .await
        });

        assert_eq!(
            result,
            Ok(StatusReportResponse::Accepted {
                map_version: MapVersion(9),
                cluster_version: None,
            }),
            "the report must land through the legacy method, with no version learned"
        );
    }

    #[test]
    fn a_pre_split_leader_makes_split_intents_an_unsupported_optional_method() {
        let sim = Simulation::new(13);
        let leader = sim.add_node(NodeId(1));
        let worker = sim.add_node(NodeId(2));
        orbita_runtime::Transport::register(leader.transport(), ServiceId::Control, V001Leader);
        let client = ControlClient::new(worker, vec![NodeId(1)]);

        let typed = sim.block_on({
            let client = client.clone();
            async move {
                client
                    .call(
                        METHOD_FETCH_SPLIT_INTENTS_V2,
                        FetchSplitIntentsRequest { node: NodeId(2) }.encode(),
                    )
                    .await
            }
        });
        assert!(matches!(
            typed,
            Err(CallError::UnsupportedMethod(METHOD_FETCH_SPLIT_INTENTS_V2))
        ));
        assert_eq!(
            sim.block_on(async move { client.fetch_split_intents(NodeId(2)).await }),
            Ok(crate::SplitIntentSnapshot {
                map_version: MapVersion(9),
                intents: Vec::new(),
            }),
            "a leader without split vocabulary cannot hold an active split"
        );
    }

    #[test]
    fn a_parent_scoped_split_leader_degrades_to_an_empty_versioned_snapshot() {
        struct LegacySplitLeader;
        impl PeerHandler for LegacySplitLeader {
            async fn handle(
                &self,
                _from: orbita_runtime::NodeId,
                call: PeerCall,
            ) -> TransportResult<bytes::Bytes> {
                let response = match call.method {
                    METHOD_FETCH_SPLIT_INTENTS_V2 => {
                        ControlResponse::Error(format!("unknown control method {}", call.method))
                    }
                    METHOD_FETCH_SPLIT_INTENTS => ControlResponse::SplitIntents(Vec::new()),
                    METHOD_FETCH_MAP => {
                        ControlResponse::Map(Some(PartitionMap::new(MapVersion(9))))
                    }
                    other => ControlResponse::Error(format!("unexpected control method {other}")),
                };
                Ok(response.encode())
            }
        }

        let sim = Simulation::new(15);
        let leader = sim.add_node(NodeId(1));
        let worker = sim.add_node(NodeId(2));
        orbita_runtime::Transport::register(
            leader.transport(),
            ServiceId::Control,
            LegacySplitLeader,
        );
        let client = ControlClient::new(worker, vec![NodeId(1)]);

        assert_eq!(
            sim.block_on(async move { client.fetch_split_intents(NodeId(2)).await }),
            Ok(crate::SplitIntentSnapshot {
                map_version: MapVersion(9),
                intents: Vec::new(),
            })
        );
    }

    /// The empty-set degradation is the one place a control refusal is not an
    /// error, so it has to stay pinned to the single refusal that means the
    /// leader has no split vocabulary at all.
    #[test]
    fn split_intent_fetch_keeps_other_refusals_and_unreachable_leaders_fatal() {
        struct RefusingLeader(&'static str);
        impl PeerHandler for RefusingLeader {
            async fn handle(
                &self,
                _from: orbita_runtime::NodeId,
                call: PeerCall,
            ) -> TransportResult<bytes::Bytes> {
                assert_eq!(call.method, METHOD_FETCH_SPLIT_INTENTS_V2);
                Ok(ControlResponse::Error(self.0.into()).encode())
            }
        }

        let sim = Simulation::new(14);
        let worker = sim.add_node(NodeId(2));
        // A refusal that is about split state itself, and one whose text is
        // the compatibility shape but names a method we did not call. Neither
        // says anything about whether a split is in flight, so neither may be
        // read as "no intents".
        for (id, message) in [
            (NodeId(1), "split state is unavailable"),
            (NodeId(3), "unknown control method 9"),
        ] {
            let refusing = sim.add_node(id);
            orbita_runtime::Transport::register(
                refusing.transport(),
                ServiceId::Control,
                RefusingLeader(message),
            );
            let client = ControlClient::new(worker.clone(), vec![id]);
            assert!(
                matches!(
                    sim.block_on(async move { client.fetch_split_intents(NodeId(2)).await }),
                    Err(Error::Internal(refusal)) if refusal == message
                ),
                "the refusal {message:?} must reach the caller unchanged"
            );
        }

        let unreachable = ControlClient::new(worker, vec![NodeId(99)]);
        assert!(matches!(
            sim.block_on(async move { unreachable.fetch_split_intents(NodeId(2)).await }),
            Err(Error::Unavailable(_))
        ));
    }

    #[test]
    fn a_report_to_a_leader_without_index_memory_still_lands() {
        // The resource numbers are new; heartbeats are not. A leader from
        // before this change must still be able to keep a worker out of the
        // failure detector, at the cost of the numbers for that rollout.
        // Two rungs down from the newest method now, which is the point of
        // the chain being a chain.
        struct V3Leader;
        impl PeerHandler for V3Leader {
            async fn handle(
                &self,
                _from: orbita_runtime::NodeId,
                call: PeerCall,
            ) -> TransportResult<bytes::Bytes> {
                let response = match call.method {
                    METHOD_REPORT_STATUS_V3 => {
                        match ReportStatusRequest::decode_v3(&call.payload) {
                            Ok(_) => ControlResponse::Accepted {
                                map_version: MapVersion(11),
                                cluster_version: Some(crate::version::binary_version()),
                            },
                            Err(e) => ControlResponse::Error(format!("undecodable status: {e}")),
                        }
                    }
                    other => ControlResponse::Error(format!("unknown control method {other}")),
                };
                Ok(response.encode())
            }
        }

        let sim = Simulation::new(3);
        let leader = sim.add_node(NodeId(1));
        let worker = sim.add_node(NodeId(2));
        orbita_runtime::Transport::register(leader.transport(), ServiceId::Control, V3Leader);

        let client = ControlClient::new(worker, vec![NodeId(1)]);
        let result = sim.block_on(async move {
            client
                .report_status_with_lifecycle(
                    NodeId(2),
                    NodeStatus::joining(NodeRole::Worker, "10.0.0.2:7000"),
                )
                .await
        });

        assert_eq!(
            result,
            Ok(StatusReportResponse::Accepted {
                map_version: MapVersion(11),
                cluster_version: Some(crate::version::binary_version()),
            })
        );
    }

    #[test]
    fn a_report_to_a_leader_without_the_committed_prefix_still_lands() {
        // The new rung. A leader running the previous binary serves V4 and
        // not V5, and the worker has to lose the committed prefix rather than
        // the heartbeat. This is the fallback chain doing the job it exists
        // for, one rung further down than last time.
        struct V4Leader;
        impl PeerHandler for V4Leader {
            async fn handle(
                &self,
                _from: orbita_runtime::NodeId,
                call: PeerCall,
            ) -> TransportResult<bytes::Bytes> {
                let response = match call.method {
                    METHOD_REPORT_STATUS_V4 => {
                        match ReportStatusRequest::decode_v4(&call.payload) {
                            Ok(request) => {
                                assert!(
                                    request
                                        .status
                                        .partitions
                                        .iter()
                                        .all(|p| p.committed_lamport.is_none()),
                                    "a V4 payload cannot carry a committed prefix"
                                );
                                ControlResponse::Accepted {
                                    map_version: MapVersion(12),
                                    cluster_version: Some(crate::version::binary_version()),
                                }
                            }
                            Err(e) => ControlResponse::Error(format!("undecodable status: {e}")),
                        }
                    }
                    other => ControlResponse::Error(format!("unknown control method {other}")),
                };
                Ok(response.encode())
            }
        }

        let sim = Simulation::new(11);
        let leader = sim.add_node(NodeId(1));
        let worker = sim.add_node(NodeId(2));
        orbita_runtime::Transport::register(leader.transport(), ServiceId::Control, V4Leader);

        let client = ControlClient::new(worker, vec![NodeId(1)]);
        let result = sim.block_on(async move {
            client
                .report_status_with_lifecycle(
                    NodeId(2),
                    NodeStatus::joining(NodeRole::Worker, "10.0.0.2:7000"),
                )
                .await
        });

        assert_eq!(
            result,
            Ok(StatusReportResponse::Accepted {
                map_version: MapVersion(12),
                cluster_version: Some(crate::version::binary_version()),
            })
        );
    }

    #[test]
    fn only_a_missing_method_triggers_the_fallback() {
        // A leader that serves the newest method but refuses the report must not
        // be retried on the legacy method: the refusal is an answer, and
        // retrying it in an older shape could turn one rejection into two
        // registrations.
        struct RefusingLeader;
        impl PeerHandler for RefusingLeader {
            async fn handle(
                &self,
                _from: orbita_runtime::NodeId,
                call: PeerCall,
            ) -> TransportResult<bytes::Bytes> {
                let response = match call.method {
                    METHOD_REPORT_STATUS_V6 => ControlResponse::Error("no".into()),
                    other => panic!("an older method must not be tried, got {other}"),
                };
                Ok(response.encode())
            }
        }

        let sim = Simulation::new(2);
        let leader = sim.add_node(NodeId(1));
        let worker = sim.add_node(NodeId(2));
        orbita_runtime::Transport::register(leader.transport(), ServiceId::Control, RefusingLeader);

        let client = ControlClient::new(worker, vec![NodeId(1)]);
        let result = sim.block_on(async move {
            client
                .report_status_with_lifecycle(
                    NodeId(2),
                    NodeStatus::joining(NodeRole::Worker, "10.0.0.2:7000"),
                )
                .await
        });
        assert!(matches!(result, Err(Error::Internal(_))));
    }
}
