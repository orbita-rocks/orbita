//! The client-facing gRPC surface.
//!
//! This layer is thin on purpose. It turns a tonic request into the node's
//! request, and an `orbita_core::Error` into a status through the one mapping
//! in [`crate::status`]. Everything else, meaning routing, conditions, and the
//! read path, belongs to the node so that a request that arrives over the peer
//! transport takes exactly the same path as one that arrives over gRPC.

use crate::auth::Authenticator;
use crate::node::Node;
use crate::readiness::{ReadinessCondition, ReadinessGate, ReadinessState};
use crate::status::to_status;

use orbita_control::Permission;
use orbita_proto::v1::health_server::Health;
use orbita_proto::v1::kv_server::Kv;
use orbita_proto::v1::{
    CheckReadinessRequest, CheckReadinessResponse, DeleteRequest, DeleteResponse, GetLimitsRequest,
    GetLimitsResponse, GetRequest, GetResponse, ListRequest, ListResponse, SetRequest, SetResponse,
};
use orbita_runtime::Runtime;

use std::sync::Arc;
use tonic::{Request, Response, Status};

pub(crate) struct KvService<R: Runtime> {
    node: Arc<Node<R>>,
    /// Checks the credential every request before it can reach a partition.
    /// Held here rather than in the node so a request over the peer transport,
    /// which is already an authenticated node, does not re-run the client-edge
    /// check.
    auth: Arc<Authenticator<R::Clock>>,
}

impl<R: Runtime> KvService<R> {
    pub(crate) fn new(node: Arc<Node<R>>, auth: Arc<Authenticator<R::Clock>>) -> Self {
        Self { node, auth }
    }

    /// Refuses a request whose credential does not permit `permission` on
    /// `keyspace`, before any partition work happens.
    ///
    /// The keyspace comes from the request body and the permission from the
    /// method, so a read cannot borrow a write scope or vice versa.
    ///
    /// The large-`Err` lint is allowed because the `Err` is a `tonic::Status`,
    /// the same type every handler here already returns; boxing it just for
    /// this helper would make the call sites unwrap a box the trait then
    /// re-wraps.
    #[allow(clippy::result_large_err)]
    fn authorize<T>(
        &self,
        request: &Request<T>,
        keyspace: &str,
        permission: Permission,
    ) -> Result<(), Status> {
        self.auth
            .authorize(request.metadata(), keyspace, permission)
            .map_err(|e| to_status(&e))
    }
}

#[tonic::async_trait]
impl<R: Runtime> Kv for KvService<R> {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        self.authorize(&request, &request.get_ref().keyspace, Permission::Read)?;
        self.node
            .get(request.into_inner(), false)
            .await
            .map(Response::new)
            .map_err(|e| to_status(&e))
    }

    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        self.authorize(&request, &request.get_ref().keyspace, Permission::Write)?;
        self.node
            .set(request.into_inner(), false)
            .await
            .map(Response::new)
            .map_err(|e| to_status(&e))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        self.authorize(&request, &request.get_ref().keyspace, Permission::Write)?;
        self.node
            .delete(request.into_inner(), false)
            .await
            .map(Response::new)
            .map_err(|e| to_status(&e))
    }

    async fn get_limits(
        &self,
        request: Request<GetLimitsRequest>,
    ) -> Result<Response<GetLimitsResponse>, Status> {
        // Deliberately unauthenticated. Limits are how a client sizes its gRPC
        // channel before it holds a credential or has picked a keyspace, and
        // the empty-keyspace form returns cluster-wide maxima that no
        // keyspace-scoped credential could name. It exposes no data, only the
        // sizes this cluster will accept.
        self.node
            .limits(&request.into_inner().keyspace)
            .map(Response::new)
            .map_err(|e| to_status(&e))
    }

    async fn list(&self, request: Request<ListRequest>) -> Result<Response<ListResponse>, Status> {
        self.authorize(&request, &request.get_ref().keyspace, Permission::Read)?;
        self.node
            .list(request.into_inner(), false)
            .await
            .map(Response::new)
            .map_err(|e| to_status(&e))
    }
}

/// Serves the `Health` API off the readiness gate.
///
/// It holds the gate rather than the node because readiness is a property of
/// the whole node's startup, part of which (joining the leader group) the node
/// itself does not see.
pub(crate) struct HealthService {
    readiness: Arc<ReadinessGate>,
}

impl HealthService {
    pub(crate) fn new(readiness: Arc<ReadinessGate>) -> Self {
        Self { readiness }
    }
}

#[tonic::async_trait]
impl Health for HealthService {
    async fn check_readiness(
        &self,
        _request: Request<CheckReadinessRequest>,
    ) -> Result<Response<CheckReadinessResponse>, Status> {
        // Answered `Ok` whether or not the node is ready; the verdict is in
        // the body. See the service comment in health.proto for why an unready
        // node is not an errored RPC.
        Ok(Response::new(readiness_response(self.readiness.state())))
    }
}

/// Turns a gate snapshot into the wire answer, every condition included so a
/// probe log shows what an unready node was waiting on.
fn readiness_response(state: ReadinessState) -> CheckReadinessResponse {
    CheckReadinessResponse {
        ready: state.is_ready(),
        conditions: ReadinessCondition::ALL
            .into_iter()
            .map(|condition| orbita_proto::v1::ReadinessCondition {
                name: condition.name().to_owned(),
                met: state.is_met(condition),
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn an_unready_node_answers_the_rpc_and_says_so_in_the_body() {
        let service = HealthService::new(Arc::new(ReadinessGate::new()));
        let response = service
            .check_readiness(Request::new(CheckReadinessRequest {}))
            .await
            .expect("readiness is answered, not errored")
            .into_inner();
        assert!(!response.ready);
    }

    #[tokio::test]
    async fn an_unready_answer_names_every_unmet_condition() {
        let gate = Arc::new(ReadinessGate::new());
        gate.mark(ReadinessCondition::WalRecovered);
        let service = HealthService::new(Arc::clone(&gate));
        let response = service
            .check_readiness(Request::new(CheckReadinessRequest {}))
            .await
            .unwrap()
            .into_inner();
        let unmet: Vec<&str> = response
            .conditions
            .iter()
            .filter(|c| !c.met)
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(
            unmet,
            [
                "cluster-version-compatible",
                "control-plane-joined",
                "partitions-caught-up",
                "replicas-recoverable",
                "accepting-ownership"
            ]
        );
    }

    #[tokio::test]
    async fn a_replica_that_cannot_be_caught_up_reaches_an_operator_over_the_wire() {
        // The state this carries is a durability failure that no retry fixes,
        // and it used to exist only as an error log inside the owner. This is
        // the RPC a probe and `orbita cluster ready` call, so a stranded
        // replica has to be visible in the answer rather than in a log nobody
        // is tailing.
        let gate = Arc::new(ReadinessGate::new());
        for condition in ReadinessCondition::ALL {
            gate.mark(condition);
        }
        gate.clear(ReadinessCondition::ReplicasRecoverable);

        let service = HealthService::new(Arc::clone(&gate));
        let response = service
            .check_readiness(Request::new(CheckReadinessRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert!(!response.ready, "a rolling update must stop at this node");
        assert!(response
            .conditions
            .iter()
            .any(|c| c.name == "replicas-recoverable" && !c.met));
    }

    #[tokio::test]
    async fn a_node_becomes_ready_once_every_condition_is_marked() {
        let gate = Arc::new(ReadinessGate::new());
        let service = HealthService::new(Arc::clone(&gate));
        for condition in ReadinessCondition::ALL {
            gate.mark(condition);
        }
        let response = service
            .check_readiness(Request::new(CheckReadinessRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert!(response.ready);
    }
}
