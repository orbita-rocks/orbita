//! The operator-facing gRPC service.
//!
//! A thin translation layer over [`Controller`], on purpose. Every decision it
//! could make is already made behind it, so an operator forcing a split
//! through the CLI goes down the same path the automatic sweep does and cannot
//! reach a state the sweep could not.
//!
//! # Every node serves this, and only one node answers it
//!
//! Only the current Raft leader may take a control plane decision, but an
//! operator has no way to know which node that is — finding out is what
//! `cluster describe` is *for*. So every node serves the whole surface and a
//! node that cannot answer forwards to the one that can, through
//! [`ControlClient`], which already knows the group's membership and already
//! follows a leader redirect. That mirrors the KV path, where a worker that
//! does not own a key forwards rather than telling the client to go elsewhere;
//! `orbita_server::proxy` has the argument. The alternative, an error naming
//! the leader, cannot even be written honestly here: the control plane records
//! each node's *peer* address, which is on a private network and is not
//! something a client may dial.

use crate::client::{AdminOutcome, ControlClient};
use crate::consensus::ConsensusLog;
use crate::controller::{ClusterView, Controller};
use crate::membership::{NodeHealth, NodeRole};
use crate::model::{Keyspace, KeyspaceConfig, Permission};

use bytes::Bytes;
use orbita_core::{Error, KeyspaceId, NodeId, PartitionId, PartitionInfo};
use orbita_proto::v1 as pb;
use orbita_runtime::Runtime;
use prost_proto::Message as _;
use std::collections::HashMap;
use tonic::{Code, Request, Response, Status};

/// Which `Admin` RPC a forwarded call is.
///
/// A discriminant of this crate's own rather than the gRPC method name,
/// because a name would put the wire's cost and the wire's compatibility
/// story at the mercy of a rename in the proto. Values are append-only: a
/// node mid-rollout may be forwarding to a peer that predates the newest one,
/// and reusing a number would silently run the wrong RPC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
enum AdminMethod {
    CreateKeyspace = 1,
    UpdateKeyspace = 2,
    DeleteKeyspace = 3,
    ListKeyspaces = 4,
    CreateCredential = 5,
    RevokeCredential = 6,
    DescribeCluster = 7,
    FinalizeUpgrade = 8,
    SplitPartition = 9,
    MergePartitions = 10,
    TransferOwnership = 11,
}

impl AdminMethod {
    fn from_u32(value: u32) -> Option<Self> {
        Some(match value {
            1 => Self::CreateKeyspace,
            2 => Self::UpdateKeyspace,
            3 => Self::DeleteKeyspace,
            4 => Self::ListKeyspaces,
            5 => Self::CreateCredential,
            6 => Self::RevokeCredential,
            7 => Self::DescribeCluster,
            8 => Self::FinalizeUpgrade,
            9 => Self::SplitPartition,
            10 => Self::MergePartitions,
            11 => Self::TransferOwnership,
            _ => return None,
        })
    }
}

/// The forwarding node's stable name for one forwarded invocation.
///
/// Carried as a request extension across the re-entry into the ordinary
/// handler so a mutation that must be exactly-once can bind its result to it.
/// A newtype rather than a bare `u128` because a tonic extension is fetched by
/// type, and a bare integer would collide with any other the stack might add.
#[derive(Debug, Clone, Copy)]
struct ForwardedOpId(u128);

/// Draws a fresh operation id from the operating system.
///
/// Like the credential secret it names, this does not go through
/// `orbita_runtime::Rng`: nothing about a deterministic run depends on the
/// value, only on two attempts at the same operation sharing one. The width is
/// a full 128 bits so that two operators creating credentials at the same
/// instant cannot collide onto one derived id.
fn fresh_operation_id() -> Result<u128, Error> {
    let mut buf = [0u8; 16];
    getrandom::getrandom(&mut buf).map_err(|e| {
        Error::Internal(format!(
            "no operating system entropy for an operation id: {e}"
        ))
    })?;
    Ok(u128::from_le_bytes(buf))
}

/// Serves the `Admin` API for one node, wherever the leader happens to be.
pub struct AdminService<R: Runtime, L: ConsensusLog> {
    /// Present when this node hosts the control plane. Answering locally is
    /// still the fast path; it is only unavailable while this member is not
    /// the leader.
    controller: Option<Controller<R, L>>,
    /// Present when this node knows where the leader group is, which is every
    /// node in a real cluster. Absent only for a node with no control plane at
    /// all, which is the static-map test configuration.
    forward: Option<ControlClient<R>>,
    /// Whether a caller must present a credential. Off by default so a cluster
    /// stays easy to bring up; a node running with authentication on turns it
    /// on. See [`AdminService::require_auth`].
    require_auth: bool,
    /// The config-derived root secret hash, if the operator configured one.
    ///
    /// Admin is a cluster-wide privilege, so it is granted only to this root
    /// identity, never derived from a tenant credential's per-keyspace write
    /// (see [`crate::CredentialSnapshot::authorize_admin`]). The hash is
    /// overlaid at authorization time from this node's own configuration, so
    /// every node — leader or forwarding worker — can check it without any
    /// credential state from the log. It is only ever this hash and only in
    /// memory: it is never written to the replicated state. See
    /// [`crate::root_secret_hash`] for why it exists and its blast radius.
    root: Option<[u8; 32]>,
}

impl<R: Runtime, L: ConsensusLog> Clone for AdminService<R, L> {
    fn clone(&self) -> Self {
        Self {
            controller: self.controller.clone(),
            forward: self.forward.clone(),
            require_auth: self.require_auth,
            root: self.root,
        }
    }
}

impl<R: Runtime, L: ConsensusLog> AdminService<R, L> {
    /// Serves from the controller on this node, and forwards nowhere.
    ///
    /// This is what the leader group's own peer handler uses to run a call
    /// another node forwarded to it. Having no forwarding client is what makes
    /// a forwarding loop unrepresentable rather than merely unlikely.
    #[must_use]
    pub fn new(controller: Controller<R, L>) -> Self {
        Self {
            controller: Some(controller),
            forward: None,
            require_auth: false,
            root: None,
        }
    }

    /// Serves nothing locally and forwards everything, which is what a worker
    /// does: it holds no control plane but knows where one is.
    #[must_use]
    pub fn forwarding(client: ControlClient<R>) -> Self {
        Self {
            controller: None,
            forward: Some(client),
            require_auth: false,
            root: None,
        }
    }

    /// Falls back to the leader group when this node's own controller is not
    /// the leader, which is the ordinary state of two members out of three.
    #[must_use]
    pub fn with_forwarding(mut self, client: ControlClient<R>) -> Self {
        self.forward = Some(client);
        self
    }

    /// Configures the bootstrap root credential for this admin surface.
    ///
    /// `root` is the SHA-256 of the operator's root secret, or `None` when no
    /// root is configured. A node hashes the secret once at startup and hands
    /// the hash here, so the plaintext never reaches this layer. See
    /// [`crate::root_secret_hash`].
    #[must_use]
    pub fn root_credential(mut self, root: Option<[u8; 32]>) -> Self {
        self.root = root;
        self
    }

    /// Turns credential enforcement on for this admin surface.
    ///
    /// A cluster with authentication off is an explicit, node-level decision,
    /// not an accident of a missing credential: the flag is what the server and
    /// the CLI agree on, so an operator can always tell an open cluster apart
    /// from one whose caller forgot a token.
    #[must_use]
    pub fn require_auth(mut self, require_auth: bool) -> Self {
        self.require_auth = require_auth;
        self
    }

    /// Wraps this in the generated tonic service, ready to add to a server.
    #[must_use]
    pub fn into_server(self) -> pb::admin_server::AdminServer<Self> {
        pb::admin_server::AdminServer::new(self)
    }

    /// Runs one forwarded call against the local controller.
    ///
    /// The payload is the operator's own request, so this decodes it, runs the
    /// ordinary handler, and encodes what came back. Nothing here re-checks
    /// leadership, because the peer handler that dispatched this already
    /// established it and doing it twice would only widen the window.
    ///
    /// `op_id` is the forwarding node's stable name for this invocation. It is
    /// carried into the re-entered handler as a request extension so that a
    /// mutation which must be exactly-once can tie its outcome to it and refuse
    /// a resend the transport made after an ambiguous loss. A handler that does
    /// not need it simply ignores it.
    pub(crate) async fn invoke(
        &self,
        method: u32,
        op_id: u128,
        payload: &[u8],
    ) -> Result<Bytes, Status> {
        use pb::admin_server::Admin as _;

        let Some(method) = AdminMethod::from_u32(method) else {
            return Err(Status::unimplemented(format!(
                "this node does not serve forwarded admin method {method}"
            )));
        };

        macro_rules! run {
            ($rpc:ident, $request:ty) => {{
                let request = <$request>::decode(payload).map_err(|e| {
                    Status::invalid_argument(format!("undecodable forwarded admin request: {e}"))
                })?;
                let mut request = Request::new(request);
                request.extensions_mut().insert(ForwardedOpId(op_id));
                let response = self.$rpc(request).await?;
                Ok(Bytes::from(response.into_inner().encode_to_vec()))
            }};
        }

        match method {
            AdminMethod::CreateKeyspace => run!(create_keyspace, pb::CreateKeyspaceRequest),
            AdminMethod::UpdateKeyspace => run!(update_keyspace, pb::UpdateKeyspaceRequest),
            AdminMethod::DeleteKeyspace => run!(delete_keyspace, pb::DeleteKeyspaceRequest),
            AdminMethod::ListKeyspaces => run!(list_keyspaces, pb::ListKeyspacesRequest),
            AdminMethod::CreateCredential => run!(create_credential, pb::CreateCredentialRequest),
            AdminMethod::RevokeCredential => run!(revoke_credential, pb::RevokeCredentialRequest),
            AdminMethod::DescribeCluster => run!(describe_cluster, pb::DescribeClusterRequest),
            AdminMethod::FinalizeUpgrade => run!(finalize_upgrade, pb::FinalizeUpgradeRequest),
            AdminMethod::SplitPartition => run!(split_partition, pb::SplitPartitionRequest),
            AdminMethod::MergePartitions => run!(merge_partitions, pb::MergePartitionsRequest),
            AdminMethod::TransferOwnership => {
                run!(transfer_ownership, pb::TransferOwnershipRequest)
            }
        }
    }

    /// Decides whether this node answers a call itself, and sends it to the
    /// leader if not.
    ///
    /// `None` means "you are the leader, go ahead". `Some` is the leader's
    /// answer, already decoded. An error is either the leader's own refusal,
    /// carried back with its code intact, or the fact that nobody could be
    /// reached — which a caller can tell apart, and which is the whole reason
    /// a refusal travels as a value rather than as a transport failure.
    async fn forwarded<Req, Resp>(
        &self,
        method: AdminMethod,
        request: &Req,
    ) -> Result<Option<Resp>, Status>
    where
        Req: prost_proto::Message,
        Resp: prost_proto::Message + Default,
    {
        if let Some(controller) = &self.controller {
            match controller.ensure_leader_ready().await {
                Ok(()) => return Ok(None),
                // Not being the leader is the ordinary state of a member, not
                // an error worth showing an operator, so long as this node can
                // reach the one that is.
                Err(error) if self.forward.is_none() => return Err(status(error)),
                Err(error) => {
                    tracing::debug!(%error, "forwarding an admin call to the control leader");
                }
            }
        }

        let Some(client) = &self.forward else {
            // Unreachable through either constructor, both of which supply at
            // least one of the two. Said out loud rather than panicked on,
            // because an operator is holding this.
            return Err(Status::unavailable(
                "this node has no control plane and knows of no leader group",
            ));
        };

        // Minted once, here, before the first send. Every retry the transport
        // makes underneath `admin_call` carries this same value, which is what
        // lets the leader recognise a resend as the same operation rather than
        // a new one. Regenerating it per attempt would be indistinguishable
        // from an operator issuing the command twice.
        let op_id = fresh_operation_id().map_err(status)?;
        match client
            .admin_call(method as u32, op_id, Bytes::from(request.encode_to_vec()))
            .await
        {
            Ok(AdminOutcome::Ok(payload)) => Resp::decode(payload).map(Some).map_err(|e| {
                Status::internal(format!("the control leader's answer did not decode: {e}"))
            }),
            Ok(AdminOutcome::Failed { code, message }) => {
                Err(Status::new(Code::from_i32(code as i32), message))
            }
            Err(error) => Err(status(error)),
        }
    }

    /// The controller, for a call [`AdminService::forwarded`] has already said
    /// this node should answer itself.
    fn leader(&self) -> &Controller<R, L> {
        self.controller
            .as_ref()
            .expect("forwarded() returns None only when this node holds a ready controller")
    }

    /// Authorizes a cluster-wide admin call from the request's bearer token.
    ///
    /// Checked here, on the node the operator dialled, rather than after the
    /// forward to the leader — because the bearer token rides in request
    /// metadata, and [`AdminService::forwarded`] re-encodes only the protobuf
    /// body, so the token does not survive the hop. That is safe to do off the
    /// leader precisely because admin is root-only: the rule is
    /// [`crate::CredentialSnapshot::authorize_admin`], which grants the cluster
    /// to the config-derived root identity alone and never to a tenant
    /// credential's per-keyspace write. Every node carries that root hash from
    /// its own configuration, so the pass verdict needs no credential state at
    /// all. When this node hosts the leader, its snapshot is folded in only to
    /// sharpen the *refusal* — a known tenant credential is told
    /// `PermissionDenied` rather than `Unauthenticated` — never to widen who
    /// passes.
    async fn authorize<T>(&self, request: &Request<T>) -> Result<(), Status> {
        if !self.require_auth {
            return Ok(());
        }
        let header = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok());
        let secret = crate::bearer_secret(header).map_err(status)?;
        // A forwarding-only node holds no credential state, so it falls back to
        // an empty snapshot the root overlay still authorizes against.
        let (snapshot, now_millis) = match &self.controller {
            Some(controller) => (
                controller.credential_snapshot().await,
                controller.now_millis(),
            ),
            None => (crate::CredentialSnapshot::default(), 0),
        };
        snapshot
            .with_root(self.root)
            .authorize_admin(secret, now_millis)
            .map_err(status)
    }
}

/// What one keyspace's partitions add up to in one observation.
///
/// `stored_bytes` is a lower bound rather than a total whenever
/// `partitions_without_size` is nonzero, which happens while a partition is
/// fenced and has no owner to have reported its size. A lower bound is a true
/// statement and a silently short total is not, and the difference decides
/// whether a keyspace over its quota can be seen to be over it.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct KeyspaceTotals {
    partition_count: u32,
    stored_bytes: u64,
    partitions_without_size: u32,
}

/// Every keyspace's totals, in one pass over one snapshot.
///
/// This exists so that a keyspace message cannot be built without a view the
/// caller already holds. Summing per keyspace on demand rescanned every
/// partition once per keyspace, which is quadratic on the most common admin
/// call, and worse than the cost: each row came from a snapshot taken at a
/// different instant, so a single response could show a partition's bytes in
/// the partition list and a different total for its keyspace beside it.
fn keyspace_totals(view: &ClusterView) -> HashMap<KeyspaceId, KeyspaceTotals> {
    let mut totals: HashMap<KeyspaceId, KeyspaceTotals> = HashMap::new();
    for partition in &view.partitions {
        let entry = totals.entry(partition.info.keyspace).or_default();
        entry.partition_count += 1;
        match partition.size_bytes {
            Some(bytes) => entry.stored_bytes += bytes,
            // Counted rather than skipped. Dropping it would make the sum
            // read as a complete total that happens to be small, which is the
            // reading a keyspace at its quota can least afford.
            None => entry.partitions_without_size += 1,
        }
    }
    totals
}

/// Renders one keyspace against totals taken from a single observation.
fn keyspace_message(
    keyspace: &Keyspace,
    totals: &HashMap<KeyspaceId, KeyspaceTotals>,
) -> pb::Keyspace {
    let totals = totals.get(&keyspace.id).copied().unwrap_or_default();
    pb::Keyspace {
        id: keyspace.id.get(),
        name: keyspace.name.as_str().to_string(),
        config: Some(config_message(&keyspace.config)),
        created_at_millis: keyspace.created_at_millis,
        partition_count: totals.partition_count,
        stored_bytes: totals.stored_bytes,
        partitions_without_size: totals.partitions_without_size,
    }
}

#[tonic::async_trait]
impl<R: Runtime, L: ConsensusLog> pb::admin_server::Admin for AdminService<R, L> {
    async fn create_keyspace(
        &self,
        request: Request<pb::CreateKeyspaceRequest>,
    ) -> Result<Response<pb::Keyspace>, Status> {
        self.authorize(&request).await?;
        let request = request.into_inner();
        if let Some(response) = self
            .forwarded(AdminMethod::CreateKeyspace, &request)
            .await?
        {
            return Ok(Response::new(response));
        }
        let keyspace = self
            .leader()
            .create_keyspace(&request.name, config_from(request.config.as_ref()))
            .await
            .map_err(status)?;
        let view = self.leader().view().await;
        Ok(Response::new(keyspace_message(
            &keyspace,
            &keyspace_totals(&view),
        )))
    }

    async fn update_keyspace(
        &self,
        request: Request<pb::UpdateKeyspaceRequest>,
    ) -> Result<Response<pb::Keyspace>, Status> {
        self.authorize(&request).await?;
        let request = request.into_inner();
        if let Some(response) = self
            .forwarded(AdminMethod::UpdateKeyspace, &request)
            .await?
        {
            return Ok(Response::new(response));
        }
        let keyspace = self
            .leader()
            .update_keyspace(&request.name, config_from(request.config.as_ref()))
            .await
            .map_err(status)?;
        let view = self.leader().view().await;
        Ok(Response::new(keyspace_message(
            &keyspace,
            &keyspace_totals(&view),
        )))
    }

    async fn delete_keyspace(
        &self,
        request: Request<pb::DeleteKeyspaceRequest>,
    ) -> Result<Response<pb::DeleteKeyspaceResponse>, Status> {
        self.authorize(&request).await?;
        let request = request.into_inner();
        // The proto asks for the name twice so that a script cannot destroy a
        // keyspace with one careless argument. Checking it here rather than in
        // the CLI means every client gets the guard, not just ours. Checked
        // before forwarding as well as on the leader, so a mistyped confirm
        // costs one round trip rather than two.
        if request.name != request.confirm_name {
            return Err(Status::invalid_argument(
                "confirm_name must repeat the keyspace name exactly",
            ));
        }
        if let Some(response) = self
            .forwarded(AdminMethod::DeleteKeyspace, &request)
            .await?
        {
            return Ok(Response::new(response));
        }
        self.leader()
            .delete_keyspace(&request.name)
            .await
            .map_err(status)?;
        Ok(Response::new(pb::DeleteKeyspaceResponse {}))
    }

    async fn list_keyspaces(
        &self,
        request: Request<pb::ListKeyspacesRequest>,
    ) -> Result<Response<pb::ListKeyspacesResponse>, Status> {
        self.authorize(&request).await?;
        let request = request.into_inner();
        if let Some(response) = self.forwarded(AdminMethod::ListKeyspaces, &request).await? {
            return Ok(Response::new(response));
        }
        // One view for the whole list. Taking one per keyspace made this
        // quadratic and let two rows disagree about the same cluster.
        let view = self.leader().view().await;
        let totals = keyspace_totals(&view);
        let keyspaces = self
            .leader()
            .list_keyspaces()
            .await
            .into_iter()
            .map(|keyspace| keyspace_message(&keyspace, &totals))
            .collect();
        Ok(Response::new(pb::ListKeyspacesResponse { keyspaces }))
    }

    async fn create_credential(
        &self,
        request: Request<pb::CreateCredentialRequest>,
    ) -> Result<Response<pb::CreateCredentialResponse>, Status> {
        self.authorize(&request).await?;
        // Read before `into_inner` drops the extensions. Present when this call
        // reached the leader by being forwarded; absent when an operator dialled
        // the leader directly, in which case there is no resend to guard against
        // and a fresh id is correct.
        let forwarded_op_id = request.extensions().get::<ForwardedOpId>().map(|f| f.0);
        let request = request.into_inner();
        if let Some(response) = self
            .forwarded(AdminMethod::CreateCredential, &request)
            .await?
        {
            return Ok(Response::new(response));
        }
        let permissions: Vec<Permission> = request
            .permissions
            .iter()
            .filter_map(|p| match pb::Permission::try_from(*p) {
                Ok(pb::Permission::Read) => Some(Permission::Read),
                Ok(pb::Permission::Write) => Some(Permission::Write),
                _ => None,
            })
            .collect();

        let op_id = match forwarded_op_id {
            Some(op_id) => op_id,
            None => fresh_operation_id().map_err(status)?,
        };

        match self
            .leader()
            .create_credential(
                request.keyspaces,
                permissions,
                request.description,
                request.expires_at_millis,
                op_id,
            )
            .await
        {
            Ok((id, secret)) => Ok(Response::new(pb::CreateCredentialResponse {
                credential_id: id,
                secret,
            })),
            // The one place a derived id turns into a visible outcome: a resend
            // of a forwarded creation whose first attempt already committed
            // lands on the same id and is refused here. The secret it returned
            // then is gone, so there is nothing to hand back and inventing a
            // second credential is exactly what this exists to prevent. Named
            // as the ambiguity it is, and coded `Aborted` so a client re-reads
            // rather than blindly retries.
            Err(Error::AlreadyExists) => Err(Status::aborted(
                "this create-credential was retried after an ambiguous connection loss and its \
                 first attempt had already committed; the one-time secret cannot be shown again. \
                 List credentials to find it, or revoke it and reissue if it was not captured.",
            )),
            Err(error) => Err(status(error)),
        }
    }

    async fn revoke_credential(
        &self,
        request: Request<pb::RevokeCredentialRequest>,
    ) -> Result<Response<pb::RevokeCredentialResponse>, Status> {
        self.authorize(&request).await?;
        let request = request.into_inner();
        if let Some(response) = self
            .forwarded(AdminMethod::RevokeCredential, &request)
            .await?
        {
            return Ok(Response::new(response));
        }
        self.leader()
            .revoke_credential(&request.credential_id)
            .await
            .map_err(status)?;
        Ok(Response::new(pb::RevokeCredentialResponse {}))
    }

    async fn describe_cluster(
        &self,
        request: Request<pb::DescribeClusterRequest>,
    ) -> Result<Response<pb::DescribeClusterResponse>, Status> {
        self.authorize(&request).await?;
        let request = request.into_inner();
        if let Some(response) = self
            .forwarded(AdminMethod::DescribeCluster, &request)
            .await?
        {
            return Ok(Response::new(response));
        }
        let filter = request.keyspace;
        let view = self.leader().view().await;
        let snapshot = self.leader().snapshot().await;

        let wanted = if filter.is_empty() {
            None
        } else {
            Some(
                snapshot
                    .keyspace_by_name(&filter)
                    .ok_or_else(|| Status::not_found("no such keyspace"))?
                    .id,
            )
        };

        let nodes = view
            .nodes
            .iter()
            .map(|node| pb::Node {
                id: node.record.id.get(),
                address: node.record.address.clone(),
                role: match node.record.role {
                    NodeRole::Leader => pb::NodeRole::Leader,
                    NodeRole::Worker => pb::NodeRole::Worker,
                } as i32,
                health: match node.record.health {
                    NodeHealth::Healthy => pb::NodeHealth::Healthy,
                    NodeHealth::Suspect => pb::NodeHealth::Suspect,
                    NodeHealth::Dead => pb::NodeHealth::Dead,
                } as i32,
                is_raft_leader: node.is_control_leader,
                speaks_min: Some(version_message(node.record.speaks.min)),
                speaks_max: Some(version_message(node.record.speaks.max)),
                index_memory_bytes: node.index_memory_bytes,
            })
            .collect();

        let partitions = view
            .partitions
            .iter()
            .filter(|p| wanted.is_none_or(|id| p.info.keyspace == id))
            .map(|p| pb::Partition {
                id: p.info.id.get(),
                keyspace_id: p.info.keyspace.get(),
                start_key: p.info.range.start().to_vec(),
                end_key: p.info.range.end().unwrap_or_default().to_vec(),
                // Zero means unowned, since the proto has no optional here.
                // Node ids start at one, so it cannot be confused with a node.
                owner_node_id: p.info.owner.map_or(0, NodeId::get),
                epoch: p.info.epoch.get(),
                committed_lamport: p.committed_lamport.map(orbita_core::Lamport::get),
                replicas: p
                    .replica_progress
                    .iter()
                    .map(|r| pb::Replica {
                        node_id: r.node.get(),
                        applied_lamport: r.applied_lamport.map(orbita_core::Lamport::get),
                        durable_lamport: r.durable_lamport.map(orbita_core::Lamport::get),
                    })
                    .collect(),
                size_bytes: p.size_bytes,
                index_bytes: p.index_bytes,
            })
            .collect();

        // Quota saturation needs both halves, and only the keyspace records
        // carry the limit. Filtered the same way the partitions are, so a
        // describe scoped to one keyspace does not leak the others' usage.
        //
        // Aggregated from the view this request already captured. Asking the
        // controller per keyspace rescanned every partition once per
        // keyspace, and made a describe answer from as many snapshots as
        // there are tenants, so the keyspace rows and the partition rows
        // above them could describe two different moments.
        let totals = keyspace_totals(&view);
        let keyspaces = self
            .leader()
            .list_keyspaces()
            .await
            .into_iter()
            .filter(|keyspace| wanted.is_none_or(|id| keyspace.id == id))
            .map(|keyspace| keyspace_message(&keyspace, &totals))
            .collect();

        Ok(Response::new(pb::DescribeClusterResponse {
            nodes,
            partitions,
            cluster_version: Some(version_message(view.cluster_version)),
            keyspaces,
        }))
    }

    async fn finalize_upgrade(
        &self,
        request: Request<pb::FinalizeUpgradeRequest>,
    ) -> Result<Response<pb::FinalizeUpgradeResponse>, Status> {
        self.authorize(&request).await?;
        let request = request.into_inner();
        if let Some(response) = self
            .forwarded(AdminMethod::FinalizeUpgrade, &request)
            .await?
        {
            return Ok(Response::new(response));
        }
        let finalized = self.leader().finalize_upgrade().await.map_err(status)?;
        Ok(Response::new(pb::FinalizeUpgradeResponse {
            previous: Some(version_message(finalized.previous)),
            active: Some(version_message(finalized.active)),
        }))
    }

    async fn split_partition(
        &self,
        request: Request<pb::SplitPartitionRequest>,
    ) -> Result<Response<pb::SplitPartitionResponse>, Status> {
        self.authorize(&request).await?;
        let request = request.into_inner();
        if let Some(response) = self
            .forwarded(AdminMethod::SplitPartition, &request)
            .await?
        {
            return Ok(Response::new(response));
        }
        crate::metrics::record_split(crate::metrics::Outcome::Unimplemented);
        // Fail closed. The control-plane split protocol exists in the state
        // machine (BeginSplit/MarkSplitPrepared/CompleteSplit/AbortSplit, which
        // structurally refuse to retire a parent until every holder has
        // prepared child storage), but the worker side that actually prepares
        // that storage does not. Completing a split today would publish
        // children backed by empty manifest and WAL paths and make the parent's
        // existing data unreachable, and there is no path yet to quiesce the
        // parent's writes before the swap. Refusing is the only safe answer
        // until that machinery lands; see the crate documentation and #71.
        Err(Status::unimplemented(
            "partition split is disabled until workers can durably prepare child storage and the \
             parent can be quiesced; the control-plane protocol is in place but its data-plane \
             half is not",
        ))
    }

    async fn merge_partitions(
        &self,
        request: Request<pb::MergePartitionsRequest>,
    ) -> Result<Response<pb::MergePartitionsResponse>, Status> {
        self.authorize(&request).await?;
        let request = request.into_inner();
        if let Some(response) = self
            .forwarded(AdminMethod::MergePartitions, &request)
            .await?
        {
            return Ok(Response::new(response));
        }
        crate::metrics::record_merge(crate::metrics::Outcome::Unimplemented);
        // Answering with a clear refusal rather than a half-built merge. See
        // the crate documentation for what a correct one has to guarantee.
        Err(Status::unimplemented(
            "partition merge is not implemented; see the orbita-control crate documentation",
        ))
    }

    async fn transfer_ownership(
        &self,
        request: Request<pb::TransferOwnershipRequest>,
    ) -> Result<Response<pb::TransferOwnershipResponse>, Status> {
        self.authorize(&request).await?;
        let request = request.into_inner();
        if let Some(response) = self
            .forwarded(AdminMethod::TransferOwnership, &request)
            .await?
        {
            return Ok(Response::new(response));
        }
        let partition = PartitionId(request.partition_id);
        self.leader()
            .transfer_ownership(partition, NodeId(request.to_node_id))
            .await
            .map_err(status)?;

        let map = self.leader().partition_map().await;
        Ok(Response::new(pb::TransferOwnershipResponse {
            partition: map.partition(partition).map(partition_message),
        }))
    }
}

fn version_message(version: crate::version::ClusterVersion) -> pb::ClusterVersion {
    pb::ClusterVersion {
        major: version.major,
        minor: version.minor,
    }
}

fn partition_message(info: &PartitionInfo) -> pb::Partition {
    pb::Partition {
        id: info.id.get(),
        keyspace_id: info.keyspace.get(),
        start_key: info.range.start().to_vec(),
        end_key: info.range.end().unwrap_or_default().to_vec(),
        owner_node_id: info.owner.map_or(0, NodeId::get),
        epoch: info.epoch.get(),
        // Nothing was observed on this path, so nothing is claimed.
        committed_lamport: None,
        replicas: info
            .replicas
            .iter()
            .map(|node| pb::Replica {
                node_id: node.get(),
                // A partial view built from the map alone has heard no
                // positions, so it claims none.
                applied_lamport: None,
                durable_lamport: None,
            })
            .collect(),
        size_bytes: None,
        // A partial view that reported an empty index would be inventing a
        // measurement out of the absence of one.
        index_bytes: None,
    }
}

fn config_message(config: &KeyspaceConfig) -> pb::KeyspaceConfig {
    pb::KeyspaceConfig {
        default_ttl_millis: config.default_ttl_millis,
        max_value_bytes: config.max_value_bytes,
        max_storage_bytes: config.max_storage_bytes,
        max_reads_per_second: config.max_reads_per_second,
        max_writes_per_second: config.max_writes_per_second,
    }
}

fn config_from(config: Option<&pb::KeyspaceConfig>) -> KeyspaceConfig {
    config.map_or_else(KeyspaceConfig::default, |c| KeyspaceConfig {
        default_ttl_millis: c.default_ttl_millis,
        max_value_bytes: c.max_value_bytes,
        max_storage_bytes: c.max_storage_bytes,
        max_reads_per_second: c.max_reads_per_second,
        max_writes_per_second: c.max_writes_per_second,
    })
}

/// Maps a control plane error to the one status code a client should act on.
///
/// The mapping lives here rather than in `orbita-core` because the same error
/// means different things at different edges, and guessing wrong turns a
/// retryable condition into a fatal one in somebody's client library.
fn status(error: Error) -> Status {
    match error {
        Error::NotFound | Error::KeyspaceNotFound => Status::not_found(error.to_string()),
        Error::AlreadyExists | Error::KeyspaceAlreadyExists => {
            Status::already_exists(error.to_string())
        }
        Error::InvalidArgument(_) | Error::TooLarge { .. } => {
            Status::invalid_argument(error.to_string())
        }
        Error::Unauthenticated => Status::unauthenticated(error.to_string()),
        Error::PermissionDenied => Status::permission_denied(error.to_string()),
        Error::QuotaExceeded(_) => Status::resource_exhausted(error.to_string()),
        // A redirect and an outage are both retryable, and an admin client
        // reaches the leader group through one endpoint either way, so it
        // cannot act on the difference.
        Error::Unavailable(_) | Error::NotOwner { .. } | Error::NotLeader { .. } => {
            Status::unavailable(error.to_string())
        }
        // A stale epoch reaching an operator means the cluster moved under
        // their command. Aborted is the code that tells a client to re-read
        // and try again rather than to give up.
        Error::StaleEpoch { .. } | Error::VersionMismatch { .. } => {
            Status::aborted(error.to_string())
        }
        Error::Internal(_) => Status::internal(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ControlConfig;
    use crate::consensus::SingleNodeLog;
    use crate::controller::BootstrapSpec;
    use crate::service::ControlService;

    use orbita_runtime::{ServiceId, Transport};
    use orbita_sim::{SimRuntime, Simulation};
    use pb::admin_server::Admin as _;
    use std::sync::Arc;

    /// A cluster of two: node one holds the control plane, node two holds
    /// nothing but knows where node one is. That is the shape of every worker
    /// in a real deployment, and the shape an operator most often points
    /// `orbita` at, because the worker is the node whose client port is
    /// published.
    fn leader_and_a_node_without_one(
        sim: &Simulation,
    ) -> (
        Controller<SimRuntime, SingleNodeLog<SimRuntime>>,
        AdminService<SimRuntime, SingleNodeLog<SimRuntime>>,
    ) {
        let leader_runtime = sim.add_node(orbita_core::NodeId(1));
        let other_runtime = sim.add_node(orbita_core::NodeId(2));

        let opening = leader_runtime.clone();
        let log = sim
            .block_on(async move { SingleNodeLog::open(&opening).await })
            .expect("the consensus log opens");
        let controller = Controller::new(
            leader_runtime.clone(),
            Arc::clone(&log),
            ControlConfig::default(),
        );
        leader_runtime
            .transport()
            .register(ServiceId::Control, ControlService::new(controller.clone()));

        let bootstrapping = controller.clone();
        sim.block_on(async move {
            bootstrapping
                .bootstrap(&BootstrapSpec {
                    keyspace: "default".to_string(),
                    config: KeyspaceConfig::default(),
                    leaders: vec![(orbita_core::NodeId(1), "1:7101".to_string())],
                    workers: Vec::new(),
                })
                .await
        })
        .expect("the cluster bootstraps");

        let admin = AdminService::forwarding(ControlClient::new(
            other_runtime,
            vec![orbita_core::NodeId(1)],
        ));
        (controller, admin)
    }

    #[test]
    fn an_admin_call_to_a_node_that_holds_no_control_plane_reaches_the_leader() {
        let sim = Simulation::new(1);
        let (controller, admin) = leader_and_a_node_without_one(&sim);

        let created = sim
            .block_on(async move {
                admin
                    .create_keyspace(Request::new(pb::CreateKeyspaceRequest {
                        name: "demo".to_string(),
                        config: None,
                    }))
                    .await
            })
            .expect("a node with no controller must forward rather than refuse");

        assert_eq!(created.into_inner().name, "demo");
        assert!(
            sim.block_on(async move { controller.snapshot().await })
                .keyspace_by_name("demo")
                .is_some(),
            "the keyspace has to exist on the leader, not merely be reported"
        );
    }

    #[test]
    fn a_forwarded_read_is_answered_from_the_leaders_state() {
        let sim = Simulation::new(2);
        let (_controller, admin) = leader_and_a_node_without_one(&sim);

        let described = sim
            .block_on(async move {
                admin
                    .describe_cluster(Request::new(pb::DescribeClusterRequest {
                        keyspace: String::new(),
                    }))
                    .await
            })
            .expect("describe is the call an operator makes to find the leader, so it must work")
            .into_inner();

        assert_eq!(
            described
                .keyspaces
                .iter()
                .map(|k| k.name.as_str())
                .collect::<Vec<_>>(),
            vec!["default"]
        );
    }

    #[test]
    fn the_leaders_own_refusal_survives_the_hop_with_its_code() {
        // A forwarded call that fails must not come back as "forwarding
        // failed". The operator needs to see that the keyspace already exists,
        // which is a thing they did, rather than an internal error, which is
        // a thing to escalate.
        let sim = Simulation::new(3);
        let (_controller, admin) = leader_and_a_node_without_one(&sim);

        let refused = sim
            .block_on(async move {
                admin
                    .create_keyspace(Request::new(pb::CreateKeyspaceRequest {
                        name: "default".to_string(),
                        config: None,
                    }))
                    .await
            })
            .expect_err("the keyspace was created by the bootstrap");

        assert_eq!(refused.code(), Code::AlreadyExists);
    }

    #[test]
    fn an_unrecognised_forwarded_method_is_refused_rather_than_guessed_at() {
        let sim = Simulation::new(4);
        let runtime = sim.add_node(orbita_core::NodeId(1));
        let opening = runtime.clone();
        let log = sim
            .block_on(async move { SingleNodeLog::open(&opening).await })
            .expect("the consensus log opens");
        let admin = AdminService::new(Controller::new(runtime, log, ControlConfig::default()));

        let refused = sim
            .block_on(async move { admin.invoke(u32::MAX, 0, &[]).await })
            .expect_err("a method this binary does not know cannot be run");

        assert_eq!(refused.code(), Code::Unimplemented);
    }

    #[test]
    fn a_call_that_arrived_here_is_never_sent_on_again() {
        // The loop this forbids is two members mid-election forwarding to each
        // other. It is prevented structurally rather than by a hop count: the
        // service the peer handler dispatches into holds no client to forward
        // with.
        let sim = Simulation::new(5);
        let runtime = sim.add_node(orbita_core::NodeId(1));
        let opening = runtime.clone();
        let log = sim
            .block_on(async move { SingleNodeLog::open(&opening).await })
            .expect("the consensus log opens");
        let controller = Controller::new(runtime, log, ControlConfig::default());

        assert!(
            AdminService::new(controller).forward.is_none(),
            "ControlService dispatches into this, so a forwarding client here would be a loop"
        );
    }

    #[test]
    fn a_forwarded_credential_replayed_with_its_operation_id_commits_exactly_once() {
        // The peer transport resends a forwarded call after an ambiguous
        // connection loss, and the leader may already have applied the first
        // copy. A credential minted afresh on each apply would commit twice and
        // orphan the first one-time secret; a credential id derived from the
        // operation id makes the resend land on the same id, where the
        // replicated duplicate check refuses it. Without the derivation this
        // second call would succeed with a new id and the assertion below
        // would fail.
        let sim = Simulation::new(6);
        let (controller, _forwarding) = leader_and_a_node_without_one(&sim);

        let op_id = 0x0f0e_0d0c_0b0a_0908_0706_0504_0302_0100_u128;

        let (id, secret) = sim
            .block_on({
                let controller = controller.clone();
                async move {
                    controller
                        .create_credential(
                            vec!["default".to_string()],
                            vec![Permission::Read],
                            "a service".to_string(),
                            None,
                            op_id,
                        )
                        .await
                }
            })
            .expect("the first attempt issues the credential");

        let replay = sim.block_on({
            let controller = controller.clone();
            async move {
                controller
                    .create_credential(
                        vec!["default".to_string()],
                        vec![Permission::Read],
                        "a service".to_string(),
                        None,
                        op_id,
                    )
                    .await
            }
        });
        assert!(
            matches!(replay, Err(Error::AlreadyExists)),
            "a resend carrying the same operation id must be refused, not mint a second \
             credential: {replay:?}"
        );

        // The credential that did commit is untouched: the replay neither
        // replaced it nor invalidated the secret returned the first time.
        let authenticated = sim.block_on({
            let controller = controller.clone();
            let id = id.clone();
            async move {
                controller
                    .authenticate(&id, &secret, "default", Permission::Read)
                    .await
            }
        });
        assert_eq!(
            authenticated,
            Ok(()),
            "the one credential that committed still authenticates"
        );

        // A genuinely different operation is still free to mint its own.
        let (other_id, _) = sim
            .block_on(async move {
                controller
                    .create_credential(
                        vec!["default".to_string()],
                        vec![Permission::Read],
                        "another service".to_string(),
                        None,
                        op_id.wrapping_add(1),
                    )
                    .await
            })
            .expect("a fresh operation id still issues a credential");
        assert_ne!(
            other_id, id,
            "distinct operations must get distinct credentials"
        );
    }

    #[test]
    fn a_forwarded_create_credential_resent_to_the_leader_fails_closed() {
        // The same guarantee, exercised through the exact path a forwarded call
        // takes on the leader: `invoke`, carrying the operation id the
        // forwarding node minted. The first lands; the resend is named as the
        // ambiguity it is rather than silently committing a second credential.
        use prost_proto::Message as _;

        let sim = Simulation::new(7);
        let (controller, _forwarding) = leader_and_a_node_without_one(&sim);
        let leader = AdminService::new(controller);

        let op_id = 0x0000_0000_dead_beef_u128;
        let payload = pb::CreateCredentialRequest {
            keyspaces: vec!["default".to_string()],
            permissions: vec![pb::Permission::Read as i32],
            description: "a service".to_string(),
            expires_at_millis: None,
        }
        .encode_to_vec();
        let method = AdminMethod::CreateCredential as u32;

        let first = sim
            .block_on({
                let leader = leader.clone();
                let payload = payload.clone();
                async move { leader.invoke(method, op_id, &payload).await }
            })
            .expect("the first forwarded attempt commits the credential");
        let first = pb::CreateCredentialResponse::decode(first)
            .expect("the leader's answer is a create-credential response");
        assert!(
            first.credential_id.starts_with("cred-"),
            "the credential id is server-issued, got {}",
            first.credential_id
        );

        let refused = sim
            .block_on(async move { leader.invoke(method, op_id, &payload).await })
            .expect_err("a resend with the same operation id must be refused");
        assert_eq!(
            refused.code(),
            Code::Aborted,
            "a replayed mutation fails closed rather than double-applying"
        );
    }
}
