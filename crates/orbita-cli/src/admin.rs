//! The admin commands, wrapping the `Admin` gRPC service.
//!
//! Every function here builds a request, makes one call, and turns the
//! response into a view. There is no logic beyond that on purpose: the moment
//! the CLI starts computing something the API does not return, it has become a
//! second implementation, and the two will disagree eventually.
//!
//! None of this needs a server built into the binary. These commands talk to a
//! cluster over the network like any other client would.

use anyhow::{bail, Result};
use orbita_proto::v1::admin_client::AdminClient;
use orbita_proto::v1::{
    CreateCredentialRequest, CreateKeyspaceRequest, DeleteKeyspaceRequest, DescribeClusterRequest,
    DescribeClusterResponse, Keyspace, KeyspaceConfig, ListKeyspacesRequest,
    MergePartitionsRequest, Node, NodeHealth, NodeRole, Partition, Permission,
    RevokeCredentialRequest, SplitPartitionRequest, TransferOwnershipRequest,
    UpdateKeyspaceRequest,
};
use tonic::transport::Channel;

use crate::cli::{
    ClusterCommand, CredentialCommand, KeyspaceCommand, KeyspaceConfigArgs, PartitionCommand,
    PermissionArg,
};
use crate::client::{authed, channel};
use crate::config::Config;
use crate::output::{
    render, Ack, Blob, ClusterView, CredentialView, Format, KeyspaceConfigView, KeyspaceListView,
    KeyspaceView, NodeView, PartitionResultView, PartitionView, PingView, ReplicaView, SplitView,
};

/// Opens an admin client against the configured endpoint.
pub fn connect(config: &Config) -> Result<AdminClient<Channel>> {
    Ok(AdminClient::new(channel(config)?))
}

/// Runs a `keyspace` subcommand and returns what should be printed.
pub async fn keyspace(config: &Config, format: Format, command: KeyspaceCommand) -> Result<String> {
    let mut client = connect(config)?;
    match command {
        KeyspaceCommand::Create { name, config: args } => {
            let request = authed(
                config,
                CreateKeyspaceRequest {
                    name,
                    config: Some(keyspace_config(&args)),
                },
            )?;
            let keyspace = client.create_keyspace(request).await?.into_inner();
            render(format, &keyspace_view(&keyspace))
        }
        KeyspaceCommand::List => {
            let request = authed(config, ListKeyspacesRequest {})?;
            let response = client.list_keyspaces(request).await?.into_inner();
            let view = KeyspaceListView {
                keyspaces: response.keyspaces.iter().map(keyspace_view).collect(),
            };
            render(format, &view)
        }
        KeyspaceCommand::Update { name, config: args } => {
            let request = authed(
                config,
                UpdateKeyspaceRequest {
                    name,
                    config: Some(keyspace_config(&args)),
                },
            )?;
            let keyspace = client.update_keyspace(request).await?.into_inner();
            render(format, &keyspace_view(&keyspace))
        }
        KeyspaceCommand::Delete { name, confirm } => {
            // The server checks this too. Checking here as well means the
            // mistake is caught before a round trip and the message can name
            // both strings.
            if name != confirm {
                bail!("--confirm is {confirm:?} but the keyspace is {name:?}. Nothing was deleted");
            }
            let request = authed(
                config,
                DeleteKeyspaceRequest {
                    name: name.clone(),
                    confirm_name: confirm,
                },
            )?;
            client.delete_keyspace(request).await?;
            render(format, &Ack::new(format!("deleted keyspace {name}")))
        }
    }
}

/// Runs a `credential` subcommand.
pub async fn credential(
    config: &Config,
    format: Format,
    command: CredentialCommand,
) -> Result<String> {
    let mut client = connect(config)?;
    match command {
        CredentialCommand::Create {
            keyspace,
            permission,
            description,
            expires_in,
        } => {
            // An expiry is absolute on the wire so that a slow round trip
            // cannot stretch it, which is the same reason TTLs are absolute.
            let expires_at_millis = expires_in.map(|d| now_millis() + d);
            let request = authed(
                config,
                CreateCredentialRequest {
                    keyspaces: keyspace,
                    permissions: permission.iter().map(|p| permission_code(*p)).collect(),
                    description,
                    expires_at_millis,
                },
            )?;
            let response = client.create_credential(request).await?.into_inner();
            render(
                format,
                &CredentialView {
                    credential_id: response.credential_id,
                    secret: response.secret,
                },
            )
        }
        CredentialCommand::Revoke { credential_id } => {
            let request = authed(
                config,
                RevokeCredentialRequest {
                    credential_id: credential_id.clone(),
                },
            )?;
            client.revoke_credential(request).await?;
            render(
                format,
                &Ack::new(format!("revoked credential {credential_id}")),
            )
        }
    }
}

/// Runs a `cluster` subcommand.
pub async fn cluster(config: &Config, format: Format, command: ClusterCommand) -> Result<String> {
    let mut client = connect(config)?;
    match command {
        ClusterCommand::Describe { keyspace } => {
            let request = authed(
                config,
                DescribeClusterRequest {
                    keyspace: keyspace.unwrap_or_default(),
                },
            )?;
            let response = client.describe_cluster(request).await?.into_inner();
            render(format, &cluster_view(&response))
        }
        ClusterCommand::Ping => {
            let request = authed(
                config,
                DescribeClusterRequest {
                    keyspace: String::new(),
                },
            )?;
            let result = client.describe_cluster(request).await;
            let detail = match &result {
                Ok(_) => "ok".to_owned(),
                Err(status) => status.code().description().to_owned(),
            };
            // The endpoint is not repeated here. Every network command is
            // wrapped with "while talking to <endpoint>" on the way out, and
            // saying it twice in one line reads like two different addresses.
            if !answered(&result) {
                bail!("the node did not answer: {detail}");
            }
            render(
                format,
                &PingView {
                    endpoint: config.client.endpoint.clone(),
                    answered: true,
                    detail,
                },
            )
        }
    }
}

/// Whether a node answered at all, as opposed to not being reachable.
///
/// A probe is asking whether the process is serving, and a node that returns an
/// error has answered: it accepted a connection, read a request, and replied.
/// Only the two statuses that mean nobody replied count as down. Treating any
/// error as down would restart a node for returning a permission error, which
/// is the sort of health check that turns one bad deploy into an outage.
fn answered<T>(result: &Result<T, tonic::Status>) -> bool {
    match result {
        Ok(_) => true,
        Err(status) => !matches!(
            status.code(),
            tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
        ),
    }
}

/// Runs a `partition` subcommand.
pub async fn partition(
    config: &Config,
    format: Format,
    command: PartitionCommand,
) -> Result<String> {
    let mut client = connect(config)?;
    match command {
        PartitionCommand::Split { partition_id, at } => {
            let request = authed(
                config,
                SplitPartitionRequest {
                    partition_id,
                    split_key: at.map(String::into_bytes).unwrap_or_default(),
                },
            )?;
            let response = client.split_partition(request).await?.into_inner();
            render(
                format,
                &SplitView {
                    lower: response.lower.as_ref().map(partition_view),
                    upper: response.upper.as_ref().map(partition_view),
                },
            )
        }
        PartitionCommand::Merge {
            lower_partition_id,
            upper_partition_id,
        } => {
            let request = authed(
                config,
                MergePartitionsRequest {
                    lower_partition_id,
                    upper_partition_id,
                },
            )?;
            let response = client.merge_partitions(request).await?.into_inner();
            render(
                format,
                &PartitionResultView {
                    summary: format!("merged {lower_partition_id} and {upper_partition_id}"),
                    partition: response.merged.as_ref().map(partition_view),
                },
            )
        }
        PartitionCommand::Transfer { partition_id, to } => {
            let request = authed(
                config,
                TransferOwnershipRequest {
                    partition_id,
                    to_node_id: to,
                },
            )?;
            let response = client.transfer_ownership(request).await?.into_inner();
            render(
                format,
                &PartitionResultView {
                    summary: format!("transferred partition {partition_id} to node {to}"),
                    partition: response.partition.as_ref().map(partition_view),
                },
            )
        }
    }
}

fn keyspace_config(args: &KeyspaceConfigArgs) -> KeyspaceConfig {
    KeyspaceConfig {
        default_ttl_millis: args.default_ttl,
        max_value_bytes: args.max_value_bytes,
        max_storage_bytes: args.max_storage_bytes,
        max_reads_per_second: args.max_reads_per_second,
        max_writes_per_second: args.max_writes_per_second,
    }
}

fn permission_code(permission: PermissionArg) -> i32 {
    match permission {
        PermissionArg::Read => Permission::Read as i32,
        PermissionArg::Write => Permission::Write as i32,
    }
}

/// Turns an admin `Keyspace` into the view the output module prints.
#[must_use]
pub fn keyspace_view(keyspace: &Keyspace) -> KeyspaceView {
    let config = keyspace.config.unwrap_or_default();
    KeyspaceView {
        id: keyspace.id,
        name: keyspace.name.clone(),
        partition_count: keyspace.partition_count,
        stored_bytes: keyspace.stored_bytes,
        created_at_millis: keyspace.created_at_millis,
        config: KeyspaceConfigView {
            default_ttl_millis: config.default_ttl_millis,
            max_value_bytes: config.max_value_bytes,
            max_storage_bytes: config.max_storage_bytes,
            max_reads_per_second: config.max_reads_per_second,
            max_writes_per_second: config.max_writes_per_second,
        },
    }
}

/// Turns a `Partition` into its view.
///
/// An empty `end_key` means the range is unbounded above, which is not the
/// same as a zero length upper bound, so it becomes an absent value rather
/// than an empty string.
#[must_use]
pub fn partition_view(partition: &Partition) -> PartitionView {
    PartitionView {
        id: partition.id,
        keyspace_id: partition.keyspace_id,
        start_key: Blob::new(&partition.start_key),
        end_key: if partition.end_key.is_empty() {
            None
        } else {
            Some(Blob::new(&partition.end_key))
        },
        owner_node_id: partition.owner_node_id,
        epoch: partition.epoch,
        committed_lamport: partition.committed_lamport,
        size_bytes: partition.size_bytes,
        replicas: partition
            .replicas
            .iter()
            .map(|r| ReplicaView {
                node_id: r.node_id,
                applied_lamport: r.applied_lamport,
            })
            .collect(),
    }
}

/// Turns a `Node` into its view, spelling the enums out as words.
///
/// An unrecognised value prints as `unknown` rather than failing, because a
/// CLI one version behind the cluster should still be able to show which nodes
/// are dead.
#[must_use]
pub fn node_view(node: &Node) -> NodeView {
    let role = match NodeRole::try_from(node.role) {
        Ok(NodeRole::Leader) => "leader",
        Ok(NodeRole::Worker) => "worker",
        _ => "unknown",
    };
    let health = match NodeHealth::try_from(node.health) {
        Ok(NodeHealth::Healthy) => "healthy",
        Ok(NodeHealth::Suspect) => "suspect",
        Ok(NodeHealth::Dead) => "dead",
        _ => "unknown",
    };
    NodeView {
        id: node.id,
        address: node.address.clone(),
        role: role.to_owned(),
        health: health.to_owned(),
        raft_leader: node.is_raft_leader,
    }
}

/// Turns a cluster description into its view.
#[must_use]
pub fn cluster_view(response: &DescribeClusterResponse) -> ClusterView {
    ClusterView::new(
        response.nodes.iter().map(node_view).collect(),
        response.partitions.iter().map(partition_view).collect(),
    )
}

/// The wall clock, used only to turn a relative expiry into an absolute one.
///
/// Nothing in the cluster reads this. A node goes through
/// `orbita_runtime::Runtime` for the same reason the simulator exists, but a
/// one-shot command that translates `--expires-in 90d` for the operator is not
/// part of that story.
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use orbita_proto::v1::Replica;

    #[test]
    fn a_partition_with_no_upper_bound_has_no_end_key_in_the_view() {
        let view = partition_view(&Partition {
            id: 1,
            keyspace_id: 2,
            start_key: b"a".to_vec(),
            end_key: Vec::new(),
            owner_node_id: 3,
            epoch: 4,
            committed_lamport: 5,
            replicas: vec![Replica {
                node_id: 6,
                applied_lamport: 5,
            }],
            size_bytes: 7,
        });
        assert!(view.end_key.is_none());
        assert_eq!(view.start_key, Blob::new(b"a"));
        assert_eq!(view.replicas.len(), 1);
    }

    #[test]
    fn an_unrecognised_role_or_health_prints_as_unknown_rather_than_failing() {
        let view = node_view(&Node {
            id: 1,
            address: "10.0.0.1:7100".to_owned(),
            role: 99,
            health: 99,
            is_raft_leader: false,
        });
        assert_eq!(view.role, "unknown");
        assert_eq!(view.health, "unknown");
    }

    #[test]
    fn keyspace_limits_left_unset_stay_unset_on_the_wire() {
        let config = keyspace_config(&KeyspaceConfigArgs {
            default_ttl: Some(1000),
            ..KeyspaceConfigArgs::default()
        });
        assert_eq!(config.default_ttl_millis, Some(1000));
        assert_eq!(config.max_value_bytes, None);
        assert_eq!(config.max_storage_bytes, None);
    }

    #[test]
    fn permissions_map_to_the_wire_values_the_server_expects() {
        assert_eq!(
            permission_code(PermissionArg::Read),
            Permission::Read as i32
        );
        assert_eq!(
            permission_code(PermissionArg::Write),
            Permission::Write as i32
        );
    }

    #[test]
    fn a_node_that_replied_with_an_error_still_counts_as_answering() {
        // A node running a build without the call, or refusing the credential,
        // is a node that is up. Restarting it for that would turn one bad
        // deploy into an outage.
        for code in [
            tonic::Code::Unimplemented,
            tonic::Code::PermissionDenied,
            tonic::Code::Internal,
        ] {
            let result: Result<(), _> = Err(tonic::Status::new(code, ""));
            assert!(answered(&result), "{code:?}");
        }
    }

    #[test]
    fn only_a_connection_that_was_never_made_counts_as_not_answering() {
        for code in [tonic::Code::Unavailable, tonic::Code::DeadlineExceeded] {
            let result: Result<(), _> = Err(tonic::Status::new(code, ""));
            assert!(!answered(&result), "{code:?}");
        }
        assert!(answered::<()>(&Ok(())));
    }

    #[test]
    fn a_keyspace_with_no_config_still_renders() {
        let view = keyspace_view(&Keyspace {
            id: 1,
            name: "demo".to_owned(),
            config: None,
            created_at_millis: 0,
            partition_count: 1,
            stored_bytes: 0,
        });
        assert_eq!(view.name, "demo");
        assert_eq!(view.config.default_ttl_millis, None);
    }
}
