//! The operator-facing gRPC service.
//!
//! A thin translation layer over [`Controller`], on purpose. Every decision it
//! could make is already made behind it, so an operator forcing a split
//! through the CLI goes down the same path the automatic sweep does and cannot
//! reach a state the sweep could not.

use crate::consensus::ConsensusLog;
use crate::controller::Controller;
use crate::membership::{NodeHealth, NodeRole};
use crate::model::{Keyspace, KeyspaceConfig, Permission};

use bytes::Bytes;
use orbita_core::{Error, NodeId, PartitionId, PartitionInfo};
use orbita_proto::v1 as pb;
use orbita_runtime::Runtime;
use tonic::{Request, Response, Status};

/// Serves the `Admin` API for one leader group node.
pub struct AdminService<R: Runtime, L: ConsensusLog> {
    controller: Controller<R, L>,
}

impl<R: Runtime, L: ConsensusLog> Clone for AdminService<R, L> {
    fn clone(&self) -> Self {
        Self {
            controller: self.controller.clone(),
        }
    }
}

impl<R: Runtime, L: ConsensusLog> AdminService<R, L> {
    #[must_use]
    pub fn new(controller: Controller<R, L>) -> Self {
        Self { controller }
    }

    /// Wraps this in the generated tonic service, ready to add to a server.
    #[must_use]
    pub fn into_server(self) -> pb::admin_server::AdminServer<Self> {
        pb::admin_server::AdminServer::new(self)
    }

    async fn keyspace_message(&self, keyspace: &Keyspace) -> pb::Keyspace {
        let map = self.controller.partition_map().await;
        let partitions = map.partitions().filter(|p| p.keyspace == keyspace.id);
        let view = self.controller.view().await;
        let stored_bytes = view
            .partitions
            .iter()
            .filter(|p| p.info.keyspace == keyspace.id)
            .map(|p| p.size_bytes)
            .sum();

        pb::Keyspace {
            id: keyspace.id.get(),
            name: keyspace.name.as_str().to_string(),
            config: Some(config_message(&keyspace.config)),
            created_at_millis: keyspace.created_at_millis,
            partition_count: partitions.count() as u32,
            stored_bytes,
        }
    }
}

#[tonic::async_trait]
impl<R: Runtime, L: ConsensusLog> pb::admin_server::Admin for AdminService<R, L> {
    async fn create_keyspace(
        &self,
        request: Request<pb::CreateKeyspaceRequest>,
    ) -> Result<Response<pb::Keyspace>, Status> {
        let request = request.into_inner();
        let keyspace = self
            .controller
            .create_keyspace(&request.name, config_from(request.config.as_ref()))
            .await
            .map_err(status)?;
        Ok(Response::new(self.keyspace_message(&keyspace).await))
    }

    async fn update_keyspace(
        &self,
        request: Request<pb::UpdateKeyspaceRequest>,
    ) -> Result<Response<pb::Keyspace>, Status> {
        let request = request.into_inner();
        let keyspace = self
            .controller
            .update_keyspace(&request.name, config_from(request.config.as_ref()))
            .await
            .map_err(status)?;
        Ok(Response::new(self.keyspace_message(&keyspace).await))
    }

    async fn delete_keyspace(
        &self,
        request: Request<pb::DeleteKeyspaceRequest>,
    ) -> Result<Response<pb::DeleteKeyspaceResponse>, Status> {
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
        _request: Request<pb::ListKeyspacesRequest>,
    ) -> Result<Response<pb::ListKeyspacesResponse>, Status> {
        let mut keyspaces = Vec::new();
        for keyspace in self.controller.list_keyspaces().await {
            keyspaces.push(self.keyspace_message(&keyspace).await);
        }
        Ok(Response::new(pb::ListKeyspacesResponse { keyspaces }))
    }

    async fn create_credential(
        &self,
        request: Request<pb::CreateCredentialRequest>,
    ) -> Result<Response<pb::CreateCredentialResponse>, Status> {
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
                committed_lamport: p.committed_lamport.get(),
                replicas: p
                    .replica_progress
                    .iter()
                    .map(|(node, applied)| pb::Replica {
                        node_id: node.get(),
                        applied_lamport: applied.get(),
                    })
                    .collect(),
                size_bytes: p.size_bytes,
            })
            .collect();

        Ok(Response::new(pb::DescribeClusterResponse {
            nodes,
            partitions,
        }))
    }

    async fn split_partition(
        &self,
        request: Request<pb::SplitPartitionRequest>,
    ) -> Result<Response<pb::SplitPartitionResponse>, Status> {
        let request = request.into_inner();
        let at = if request.split_key.is_empty() {
            None
        } else {
            Some(Bytes::from(request.split_key))
        };
        let (lower, upper) = self
            .controller
            .split_partition(PartitionId(request.partition_id), at)
            .await
            .map_err(status)?;

        let map = self.controller.partition_map().await;
        Ok(Response::new(pb::SplitPartitionResponse {
            lower: map.partition(lower).map(partition_message),
            upper: map.partition(upper).map(partition_message),
        }))
    }

    async fn merge_partitions(
        &self,
        _request: Request<pb::MergePartitionsRequest>,
    ) -> Result<Response<pb::MergePartitionsResponse>, Status> {
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

fn partition_message(info: &PartitionInfo) -> pb::Partition {
    pb::Partition {
        id: info.id.get(),
        keyspace_id: info.keyspace.get(),
        start_key: info.range.start().to_vec(),
        end_key: info.range.end().unwrap_or_default().to_vec(),
        owner_node_id: info.owner.map_or(0, NodeId::get),
        epoch: info.epoch.get(),
        committed_lamport: 0,
        replicas: info
            .replicas
            .iter()
            .map(|node| pb::Replica {
                node_id: node.get(),
                applied_lamport: 0,
            })
            .collect(),
        size_bytes: 0,
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
