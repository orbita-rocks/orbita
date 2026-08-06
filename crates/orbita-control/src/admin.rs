//! The operator-facing gRPC service.
//!
//! A thin translation layer over [`Controller`], on purpose. Every decision it
//! could make is already made behind it, so an operator forcing a split
//! through the CLI goes down the same path the automatic sweep does and cannot
//! reach a state the sweep could not.

use crate::consensus::ConsensusLog;
use crate::controller::{ClusterView, Controller};
use crate::membership::{NodeHealth, NodeRole};
use crate::model::{Keyspace, KeyspaceConfig, Permission};

use orbita_core::{Error, KeyspaceId, NodeId, PartitionId, PartitionInfo};
use orbita_proto::v1 as pb;
use orbita_runtime::Runtime;
use std::collections::HashMap;
use tonic::{Request, Response, Status};

/// Serves the `Admin` API for one leader group node.
pub struct AdminService<R: Runtime, L: ConsensusLog> {
    controller: Controller<R, L>,
    /// Whether a caller must present a credential. Off by default so a cluster
    /// stays easy to bring up; a node running with authentication on turns it
    /// on. See [`AdminService::require_auth`].
    require_auth: bool,
}

impl<R: Runtime, L: ConsensusLog> Clone for AdminService<R, L> {
    fn clone(&self) -> Self {
        Self {
            controller: self.controller.clone(),
            require_auth: self.require_auth,
        }
    }
}

impl<R: Runtime, L: ConsensusLog> AdminService<R, L> {
    #[must_use]
    pub fn new(controller: Controller<R, L>) -> Self {
        Self {
            controller,
            require_auth: false,
        }
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

    async fn ready(&self) -> Result<(), Status> {
        self.controller.ensure_leader_ready().await.map_err(status)
    }

    /// Authorizes a cluster-wide admin call from the request's bearer token.
    ///
    /// Admin runs only on a leader group member, which holds the credential
    /// state locally, so this is a lock and a scan rather than a round trip.
    /// The rule is [`crate::CredentialSnapshot::authorize_admin`]: any
    /// unexpired, write-capable credential may operate the cluster.
    async fn authorize<T>(&self, request: &Request<T>) -> Result<(), Status> {
        if !self.require_auth {
            return Ok(());
        }
        let header = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok());
        let secret = crate::bearer_secret(header).map_err(status)?;
        self.controller
            .credential_snapshot()
            .await
            .authorize_admin(secret, self.controller.now_millis())
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
        self.ready().await?;
        let request = request.into_inner();
        let keyspace = self
            .controller
            .create_keyspace(&request.name, config_from(request.config.as_ref()))
            .await
            .map_err(status)?;
        let view = self.controller.view().await;
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
        self.ready().await?;
        let request = request.into_inner();
        let keyspace = self
            .controller
            .update_keyspace(&request.name, config_from(request.config.as_ref()))
            .await
            .map_err(status)?;
        let view = self.controller.view().await;
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
        self.ready().await?;
        let request = request.into_inner();
        // The proto asks for the name twice so that a script cannot destroy a
        // keyspace with one careless argument. Checking it here rather than in
        // the CLI means every client gets the guard, not just ours.
        if request.name != request.confirm_name {
            return Err(Status::invalid_argument(
                "confirm_name must repeat the keyspace name exactly",
            ));
        }
        self.controller
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
        self.ready().await?;
        // One view for the whole list. Taking one per keyspace made this
        // quadratic and let two rows disagree about the same cluster.
        let view = self.controller.view().await;
        let totals = keyspace_totals(&view);
        let keyspaces = self
            .controller
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
        self.ready().await?;
        let request = request.into_inner();
        let permissions: Vec<Permission> = request
            .permissions
            .iter()
            .filter_map(|p| match pb::Permission::try_from(*p) {
                Ok(pb::Permission::Read) => Some(Permission::Read),
                Ok(pb::Permission::Write) => Some(Permission::Write),
                _ => None,
            })
            .collect();

        let (id, secret) = self
            .controller
            .create_credential(
                request.keyspaces,
                permissions,
                request.description,
                request.expires_at_millis,
            )
            .await
            .map_err(status)?;

        Ok(Response::new(pb::CreateCredentialResponse {
            credential_id: id,
            secret,
        }))
    }

    async fn revoke_credential(
        &self,
        request: Request<pb::RevokeCredentialRequest>,
    ) -> Result<Response<pb::RevokeCredentialResponse>, Status> {
        self.authorize(&request).await?;
        self.ready().await?;
        self.controller
            .revoke_credential(&request.into_inner().credential_id)
            .await
            .map_err(status)?;
        Ok(Response::new(pb::RevokeCredentialResponse {}))
    }

    async fn describe_cluster(
        &self,
        request: Request<pb::DescribeClusterRequest>,
    ) -> Result<Response<pb::DescribeClusterResponse>, Status> {
        self.authorize(&request).await?;
        self.ready().await?;
        let filter = request.into_inner().keyspace;
        let view = self.controller.view().await;
        let snapshot = self.controller.snapshot().await;

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
            .controller
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
        self.ready().await?;
        let finalized = self.controller.finalize_upgrade().await.map_err(status)?;
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
        self.ready().await?;
        Err(Status::unimplemented(
            "partition split is disabled until child storage preparation is implemented",
        ))
    }

    async fn merge_partitions(
        &self,
        request: Request<pb::MergePartitionsRequest>,
    ) -> Result<Response<pb::MergePartitionsResponse>, Status> {
        self.authorize(&request).await?;
        self.ready().await?;
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
        self.ready().await?;
        let request = request.into_inner();
        let partition = PartitionId(request.partition_id);
        self.controller
            .transfer_ownership(partition, NodeId(request.to_node_id))
            .await
            .map_err(status)?;

        let map = self.controller.partition_map().await;
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
