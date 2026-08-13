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
    AdminCallRequest, ControlResponse, DrainNodeRequest, FetchMapRequest, FetchMergeIntentsRequest,
    FetchSplitIntentsRequest, ReportMergePreparedRequest, ReportSplitPreparedRequest,
    ReportSplitPreparedV2Request, ReportStatusRequest, WireMergeIntent, WireSplitIntent,
    METHOD_ADMIN_CALL, METHOD_DRAIN_NODE, METHOD_FETCH_AUTH_POLICY, METHOD_FETCH_COMMIT_INDEX,
    METHOD_FETCH_CREDENTIALS, METHOD_FETCH_MAP, METHOD_FETCH_MERGE_INTENTS, METHOD_FETCH_NODES,
    METHOD_FETCH_SPLIT_INTENTS, METHOD_FETCH_SPLIT_INTENTS_V2, METHOD_REPORT_MERGE_PREPARED,
    METHOD_REPORT_SPLIT_PREPARED, METHOD_REPORT_SPLIT_PREPARED_V2, METHOD_REPORT_STATUS,
    METHOD_REPORT_STATUS_V2, METHOD_REPORT_STATUS_V3, METHOD_REPORT_STATUS_V4,
    METHOD_REPORT_STATUS_V5, METHOD_REPORT_STATUS_V6,
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
    /// Whether this leader group member requires authentication, from its own
    /// configuration. Served on [`METHOD_FETCH_AUTH_POLICY`] so a node can gate
    /// its readiness on agreeing with the cluster's policy rather than trusting
    /// its local `require_auth` in isolation.
    require_auth: bool,
}

impl<R: Runtime, L: ConsensusLog> Clone for ControlService<R, L> {
    fn clone(&self) -> Self {
        Self {
            controller: self.controller.clone(),
            admin: self.admin.clone(),
            require_auth: self.require_auth,
        }
    }
}

impl<R: Runtime, L: ConsensusLog> ControlService<R, L> {
    #[must_use]
    pub fn new(controller: Controller<R, L>) -> Self {
        Self {
            admin: AdminService::new(controller.clone()),
            controller,
            require_auth: false,
        }
    }

    /// Records the cluster's authentication policy this member advertises.
    ///
    /// A node asks the leader group this to check its own `require_auth`
    /// against the cluster's before reporting ready, so that a half-rolled
    /// change to the setting cannot leave an auth-disabled node serving
    /// unauthenticated requests behind the load balancer. Defaults to off,
    /// which is what the test and single-node shapes run with.
    #[must_use]
    pub fn require_auth(mut self, require_auth: bool) -> Self {
        self.require_auth = require_auth;
        self
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
            METHOD_REPORT_STATUS_V5 => match ReportStatusRequest::decode_v5(&call.payload) {
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
            METHOD_REPORT_STATUS_V6 => match ReportStatusRequest::decode(&call.payload) {
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
                // Pre-union merge builds used method 17 for this fetch. Its
                // exact one-node payload cannot decode as a V6 status report.
                Err(status_error) => match FetchMergeIntentsRequest::decode(&call.payload) {
                    Ok(request) => {
                        let (map_version, intents) =
                            self.controller.active_merge_intents_for(request.node).await;
                        ControlResponse::MergeIntents(crate::MergeIntentSnapshot {
                            map_version,
                            intents: intents
                                .into_iter()
                                .map(|(intent, prepared_by_this_node)| WireMergeIntent {
                                    generation: intent.generation,
                                    prepared_by_this_node,
                                })
                                .collect(),
                        })
                    }
                    Err(_) => ControlResponse::Error(format!("undecodable status: {status_error}")),
                },
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
                Ok(request) => match self
                    .admin
                    .invoke(request.method, request.op_id, &request.payload)
                    .await
                {
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
            METHOD_FETCH_SPLIT_INTENTS => match FetchSplitIntentsRequest::decode(&call.payload) {
                // Protocol 0.0 cannot begin a worker-prepared split, and its
                // parent-only acknowledgement cannot safely name a retry.
                Ok(_) => ControlResponse::SplitIntents(Vec::new()),
                Err(e) => ControlResponse::Error(format!("undecodable split intents fetch: {e}")),
            },
            METHOD_FETCH_SPLIT_INTENTS_V2 => {
                match FetchSplitIntentsRequest::decode(&call.payload) {
                    Ok(request) => {
                        let (map_version, intents) =
                            self.controller.active_split_intents_for(request.node).await;
                        ControlResponse::SplitIntentsV2(crate::SplitIntentSnapshot {
                            map_version,
                            intents: intents
                                .into_iter()
                                .map(|(intent, prepared_by_this_node)| WireSplitIntent {
                                    parent: intent.parent,
                                    at: intent.at,
                                    lower: intent.lower,
                                    upper: intent.upper,
                                    prepared_by_this_node,
                                })
                                .collect(),
                        })
                    }
                    Err(e) => {
                        ControlResponse::Error(format!("undecodable split intents fetch: {e}"))
                    }
                }
            }
            METHOD_REPORT_SPLIT_PREPARED => match ReportSplitPreparedRequest::decode(&call.payload)
            {
                // Kept as a no-op for a worker rolling from protocol 0.0. Such
                // a worker is never handed an intent by the legacy fetch above.
                Ok(_) => ControlResponse::SplitPrepared,
                Err(e) => ControlResponse::Error(format!("undecodable split-prepared report: {e}")),
            },
            METHOD_REPORT_SPLIT_PREPARED_V2 => {
                match ReportSplitPreparedV2Request::decode(&call.payload) {
                    Ok(request) => {
                        if self
                            .controller
                            .record_split_prepared(
                                request.node,
                                request.parent,
                                request.lower,
                                request.upper,
                            )
                            .await
                        {
                            ControlResponse::SplitPrepared
                        } else {
                            ControlResponse::Error(
                                "the reported split generation is no longer active for this node"
                                    .into(),
                            )
                        }
                    }
                    Err(e) => {
                        ControlResponse::Error(format!("undecodable split-prepared report: {e}"))
                    }
                }
            }
            METHOD_FETCH_MERGE_INTENTS => match FetchMergeIntentsRequest::decode(&call.payload) {
                Ok(request) => {
                    let (map_version, intents) =
                        self.controller.active_merge_intents_for(request.node).await;
                    ControlResponse::MergeIntents(crate::MergeIntentSnapshot {
                        map_version,
                        intents: intents
                            .into_iter()
                            .map(|(intent, prepared_by_this_node)| WireMergeIntent {
                                generation: intent.generation,
                                prepared_by_this_node,
                            })
                            .collect(),
                    })
                }
                Err(error) => {
                    ControlResponse::Error(format!("undecodable merge intents fetch: {error}"))
                }
            },
            METHOD_REPORT_MERGE_PREPARED => {
                match ReportMergePreparedRequest::decode(&call.payload) {
                    Ok(request) => {
                        if self
                            .controller
                            .record_merge_prepared(request.node, request.generation)
                            .await
                        {
                            ControlResponse::MergePrepared
                        } else {
                            ControlResponse::Error(
                                "merge generation is no longer active for this worker".into(),
                            )
                        }
                    }
                    Err(error) => ControlResponse::Error(format!(
                        "undecodable merge-prepared report: {error}"
                    )),
                }
            }
            METHOD_FETCH_NODES => ControlResponse::Nodes(self.controller.node_addresses().await),
            METHOD_FETCH_CREDENTIALS => ControlResponse::Credentials(
                self.controller
                    .credential_snapshot()
                    .await
                    .credentials()
                    .to_vec(),
            ),
            METHOD_FETCH_COMMIT_INDEX => {
                ControlResponse::CommitIndex(self.controller.commit_index().await)
            }
            METHOD_FETCH_AUTH_POLICY => ControlResponse::AuthPolicy(self.require_auth),
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::membership::{NodeRole, NodeStatus};
    use crate::wire::{ControlResponse, ReportStatusRequest};
    use crate::{ControlCommand, ControlConfig, LogEntry, LogIndex};
    use orbita_core::{Error, Result};
    use orbita_runtime::ServiceId;
    use orbita_sim::{SimRuntime, Simulation};
    use std::sync::Arc;

    /// What this node's log says when asked to prove it may serve.
    #[derive(Clone, Copy)]
    enum Gate {
        Ready,
        Deposed(Option<NodeId>),
        Broken,
    }

    /// A log that decides only one thing: whether the barrier proves leadership.
    ///
    /// Everything else answers emptily, because these tests are about the gate
    /// in front of the dispatch and about the shape of what comes back, not
    /// about consensus.
    #[derive(Clone)]
    struct GateLog {
        gate: Gate,
    }

    impl ConsensusLog for GateLog {
        async fn propose(&self, _command: ControlCommand) -> Result<LogIndex> {
            Ok(0)
        }
        async fn commit_index(&self) -> LogIndex {
            0
        }
        async fn subscribe(&self, _after: LogIndex) -> Result<Vec<LogEntry>> {
            Ok(Vec::new())
        }
        async fn leader_barrier(&self) -> Result<LogIndex> {
            match self.gate {
                Gate::Ready => Ok(0),
                Gate::Deposed(leader) => Err(Error::NotLeader { leader }),
                Gate::Broken => Err(Error::Unavailable("log is not answering".to_string())),
            }
        }
        async fn is_leader(&self) -> bool {
            matches!(self.gate, Gate::Ready)
        }
        async fn leader(&self) -> Option<NodeId> {
            match self.gate {
                Gate::Deposed(leader) => leader,
                _ => None,
            }
        }
        async fn voters(&self) -> Vec<NodeId> {
            vec![NodeId(1)]
        }
        async fn learners(&self) -> Vec<NodeId> {
            Vec::new()
        }
    }

    fn service(sim: &Simulation, gate: Gate) -> ControlService<SimRuntime, GateLog> {
        let runtime = sim.add_node(NodeId(1));
        ControlService::new(Controller::new(
            runtime,
            Arc::new(GateLog { gate }),
            ControlConfig::default(),
        ))
    }

    /// A service over a log that really commits, for the cases that need the
    /// controller to reach a decision rather than only to be gated.
    fn committing_service(
        sim: &Simulation,
    ) -> ControlService<SimRuntime, crate::SingleNodeLog<SimRuntime>> {
        let runtime = sim.add_node(NodeId(1));
        let opening = runtime.clone();
        let log = sim
            .block_on(async move { crate::SingleNodeLog::open(&opening).await })
            .expect("the log opens");
        ControlService::new(Controller::new(runtime, log, ControlConfig::default()))
    }

    fn committed_answer(
        sim: &Simulation,
        service: &ControlService<SimRuntime, crate::SingleNodeLog<SimRuntime>>,
        call: PeerCall,
    ) -> ControlResponse {
        let service = service.clone();
        let encoded = sim
            .block_on(async move { service.handle(NodeId(9), call).await })
            .expect("a refusal is still a reply");
        ControlResponse::decode(&encoded).expect("the reply decodes")
    }

    fn call(method: u16, payload: Bytes) -> PeerCall {
        PeerCall {
            service: ServiceId::Control,
            method,
            payload,
        }
    }

    fn answer(
        sim: &Simulation,
        service: &ControlService<SimRuntime, GateLog>,
        call: PeerCall,
    ) -> ControlResponse {
        let service = service.clone();
        let encoded = sim
            .block_on(async move { service.handle(NodeId(9), call).await })
            .expect("a refusal is still a reply");
        ControlResponse::decode(&encoded).expect("the reply decodes")
    }

    fn status_of(node: u64) -> ReportStatusRequest {
        ReportStatusRequest {
            node: NodeId(node),
            status: NodeStatus::joining(NodeRole::Worker, "10.0.0.1:7101"),
        }
    }

    #[test]
    fn a_deposed_member_names_the_leader_instead_of_serving_its_stale_map() {
        // The gate this service opens with. Raft role alone is not authority:
        // a deposed minority still holds a map, and answering with it is how a
        // worker routes writes to an owner the cluster has already replaced.
        // The reply names the leader so the caller retries in one hop rather
        // than sweeping the group.
        let sim = Simulation::new(1);
        let service = service(&sim, Gate::Deposed(Some(NodeId(7))));

        let response = answer(&sim, &service, call(METHOD_FETCH_MAP, Bytes::new()));

        assert!(
            matches!(response, ControlResponse::NotLeader { leader: Some(l) } if l == NodeId(7)),
            "expected to be pointed at the leader, got {response:?}"
        );
    }

    #[test]
    fn a_leader_that_cannot_prove_itself_is_unavailable_rather_than_wrong() {
        // A barrier that fails for any reason other than having been deposed
        // leaves this node unable to say whether its map is current. Refusing
        // is the only correct answer; serving anyway is the stale read the
        // gate exists to prevent, one level up.
        let sim = Simulation::new(2);
        let service = service(&sim, Gate::Broken);

        let response = answer(&sim, &service, call(METHOD_FETCH_MAP, Bytes::new()));

        assert!(
            matches!(response, ControlResponse::Unavailable(_)),
            "expected unavailable, got {response:?}"
        );
    }

    #[test]
    fn a_refusal_travels_as_a_reply_rather_than_a_transport_failure() {
        // "The leader considered this and said no" and "the leader was
        // unreachable" mean opposite things to a caller: the first must not be
        // retried against another node and the second must be. If a refusal
        // came back as a transport error the client would sweep the group and
        // could be told yes by a member that has not yet learned it lost.
        let sim = Simulation::new(3);
        let service = service(&sim, Gate::Deposed(None));

        let handler = service.clone();
        let sent = sim.block_on(async move {
            handler
                .handle(NodeId(9), call(METHOD_FETCH_MAP, Bytes::new()))
                .await
        });

        assert!(
            sent.is_ok(),
            "a refusal must be a reply the caller can read, got {sent:?}"
        );
    }

    #[test]
    fn an_unknown_method_is_refused_rather_than_fatal() {
        // A worker from a future version, or a confused one. It may waste a
        // leader's time and must not be able to do anything else.
        let sim = Simulation::new(4);
        let service = service(&sim, Gate::Ready);

        let response = answer(&sim, &service, call(u16::MAX, Bytes::new()));

        assert!(
            matches!(response, ControlResponse::Error(_)),
            "expected a refusal, got {response:?}"
        );
    }

    #[test]
    fn an_undecodable_payload_is_refused_rather_than_fatal() {
        // The hostile-worker property this module claims in its own
        // documentation: a bad caller can waste a leader's time but cannot
        // move the cluster. Garbage aimed at a method that does decode its
        // payload has to come back as an error rather than take the node down.
        let sim = Simulation::new(5);
        let service = service(&sim, Gate::Ready);

        let response = answer(
            &sim,
            &service,
            call(METHOD_REPORT_STATUS_V2, Bytes::from_static(&[0xff, 0x00])),
        );

        assert!(
            matches!(response, ControlResponse::Error(_)),
            "expected a refusal, got {response:?}"
        );
    }

    #[test]
    fn a_worker_that_predates_versions_is_answered_without_one() {
        // The rolling-upgrade rule, and the reason the field is optional. A
        // v0.0.1 worker's reply ends at the map version and it rejects
        // anything after it as trailing bytes, so a leader that helpfully
        // appended its cluster version would break every old worker in the
        // middle of the upgrade that introduced it. See ADR 0005.
        let sim = Simulation::new(6);
        let service = committing_service(&sim);

        let response = committed_answer(
            &sim,
            &service,
            call(METHOD_REPORT_STATUS, status_of(2).encode_legacy()),
        );

        match response {
            ControlResponse::Accepted {
                cluster_version, ..
            } => assert_eq!(
                cluster_version, None,
                "a legacy reply must end at the map version"
            ),
            other => panic!("expected the report to be accepted, got {other:?}"),
        }
    }

    #[test]
    fn a_worker_that_knows_about_versions_is_told_which_to_speak() {
        // The other half of the same rule. A versioned worker learns the
        // active cluster version in the same round trip that keeps it out of
        // the failure detector, so it never has to ask separately and can
        // never act on a version it learned at a different moment.
        let sim = Simulation::new(7);
        let service = committing_service(&sim);

        let response = committed_answer(
            &sim,
            &service,
            call(METHOD_REPORT_STATUS_V2, status_of(3).encode_v2()),
        );

        match response {
            ControlResponse::Accepted {
                cluster_version, ..
            } => assert!(
                cluster_version.is_some(),
                "a versioned worker must be told the cluster version"
            ),
            other => panic!("expected the report to be accepted, got {other:?}"),
        }
    }

    #[test]
    fn the_auth_policy_a_member_advertises_is_its_own_configuration() {
        // A node asks the leader group this to check its own require_auth
        // against the cluster's before reporting ready. The point is to catch
        // a half-rolled change to the setting, so the answer has to be what
        // this member is actually configured with rather than a default or
        // whatever the asker hoped for. Serving the wrong one would let an
        // auth-disabled node behind the load balancer report ready.
        let sim = Simulation::new(8);
        let off = service(&sim, Gate::Ready);
        let on = service(&sim, Gate::Ready).require_auth(true);

        assert!(
            matches!(
                answer(&sim, &off, call(METHOD_FETCH_AUTH_POLICY, Bytes::new())),
                ControlResponse::AuthPolicy(false)
            ),
            "a member with auth off must say so"
        );
        assert!(
            matches!(
                answer(&sim, &on, call(METHOD_FETCH_AUTH_POLICY, Bytes::new())),
                ControlResponse::AuthPolicy(true)
            ),
            "and a member with auth on must say so"
        );
    }

    #[test]
    fn a_caller_already_on_the_current_map_is_not_sent_it_again() {
        // Every worker fetches the map on a timer, so answering with the whole
        // thing every time would put the map on the wire once per worker per
        // interval forever, whether or not anything moved. The empty answer is
        // what makes the fetch cheap enough to run often.
        let sim = Simulation::new(9);
        let service = committing_service(&sim);

        let current = {
            let controller = service.controller.clone();
            sim.block_on(async move { controller.map_version().await })
        };

        let response = committed_answer(
            &sim,
            &service,
            call(
                METHOD_FETCH_MAP,
                crate::wire::FetchMapRequest { have: current }.encode(),
            ),
        );

        assert!(
            matches!(response, ControlResponse::Map(None)),
            "a caller that is already current must be told nothing moved, got {response:?}"
        );
    }
}
