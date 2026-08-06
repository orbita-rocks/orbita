//! The CLI against a real gRPC server.
//!
//! The unit tests cover request building and rendering separately. These cover
//! the join: that the client dials, that the credential arrives as a header,
//! that responses come back through the view types, and that the exit codes
//! mean what the help says they mean.
//!
//! The server here is a stand-in with canned answers, not orbita-server. What
//! is being tested is the wire, and a stand-in exercises the same generated
//! code the real one would.

#![forbid(unsafe_code)]

use std::sync::{Arc, Mutex};

use orbita_cli::cli::{
    ClusterCommand, GetArgs, KeyspaceCommand, KeyspaceConfigArgs, SetArgs, EXIT_CONDITION_NOT_MET,
    EXIT_NOT_FOUND,
};
use orbita_cli::config::Layer;
use orbita_cli::output::Format;
use orbita_cli::session::Session;
use orbita_cli::{admin, data};
use orbita_proto::v1::admin_server::{Admin, AdminServer};
use orbita_proto::v1::kv_server::{Kv, KvServer};
use orbita_proto::v1::{
    ClusterVersion, CreateCredentialRequest, CreateCredentialResponse, CreateKeyspaceRequest,
    DeleteKeyspaceRequest, DeleteKeyspaceResponse, DeleteRequest, DeleteResponse,
    DescribeClusterRequest, DescribeClusterResponse, FinalizeUpgradeRequest,
    FinalizeUpgradeResponse, GetRequest, GetResponse, Keyspace, KeyspaceConfig,
    ListKeyspacesRequest, ListKeyspacesResponse, ListRequest, ListResponse, MergePartitionsRequest,
    MergePartitionsResponse, Node, NodeHealth, NodeRole, Partition, Replica,
    RevokeCredentialRequest, RevokeCredentialResponse, SetRequest, SetResponse,
    SplitPartitionRequest, SplitPartitionResponse, TransferOwnershipRequest,
    TransferOwnershipResponse, UpdateKeyspaceRequest,
};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

/// What the stand-in server saw, so a test can assert on the request as well
/// as the response.
#[derive(Debug, Default)]
struct Seen {
    authorization: Option<String>,
    keyspace_name: Option<String>,
    set_key: Option<Vec<u8>>,
    set_condition: bool,
}

#[derive(Clone)]
struct Fake {
    seen: Arc<Mutex<Seen>>,
    /// Whether Get should report a hit, so the miss path can be tested.
    key_exists: bool,
    /// Whether Set should report the condition as met.
    condition_holds: bool,
    /// Whether DescribeCluster reports index memory at all. False is what a
    /// leader group running a binary from before the measurement looks like
    /// on the wire: the field is simply absent.
    reports_index_memory: bool,
    /// The storage quota DescribeCluster reports for its keyspace. `Some(0)`
    /// is a real cap that allows nothing, not the absence of a cap.
    quota_bytes: Option<u64>,
}

impl Fake {
    fn record_auth<T>(&self, request: &Request<T>) {
        let value = request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        self.seen.lock().unwrap().authorization = value;
    }
}

fn keyspace(name: &str) -> Keyspace {
    Keyspace {
        id: 1,
        name: name.to_owned(),
        config: Some(KeyspaceConfig {
            default_ttl_millis: Some(60_000),
            max_value_bytes: Some(1024),
            max_storage_bytes: None,
            max_reads_per_second: None,
            max_writes_per_second: None,
        }),
        created_at_millis: 1_700_000_000_000,
        partition_count: 1,
        stored_bytes: 4096,
        partitions_without_size: 0,
    }
}

#[tonic::async_trait]
impl Admin for Fake {
    async fn create_keyspace(
        &self,
        request: Request<CreateKeyspaceRequest>,
    ) -> Result<Response<Keyspace>, Status> {
        self.record_auth(&request);
        let name = request.into_inner().name;
        self.seen.lock().unwrap().keyspace_name = Some(name.clone());
        Ok(Response::new(keyspace(&name)))
    }

    async fn update_keyspace(
        &self,
        request: Request<UpdateKeyspaceRequest>,
    ) -> Result<Response<Keyspace>, Status> {
        self.record_auth(&request);
        Ok(Response::new(keyspace(&request.into_inner().name)))
    }

    async fn delete_keyspace(
        &self,
        request: Request<DeleteKeyspaceRequest>,
    ) -> Result<Response<DeleteKeyspaceResponse>, Status> {
        self.record_auth(&request);
        let request = request.into_inner();
        if request.name != request.confirm_name {
            return Err(Status::invalid_argument("confirm_name does not match"));
        }
        Ok(Response::new(DeleteKeyspaceResponse {}))
    }

    async fn list_keyspaces(
        &self,
        request: Request<ListKeyspacesRequest>,
    ) -> Result<Response<ListKeyspacesResponse>, Status> {
        self.record_auth(&request);
        Ok(Response::new(ListKeyspacesResponse {
            keyspaces: vec![keyspace("orders"), keyspace("sessions")],
        }))
    }

    async fn create_credential(
        &self,
        request: Request<CreateCredentialRequest>,
    ) -> Result<Response<CreateCredentialResponse>, Status> {
        self.record_auth(&request);
        Ok(Response::new(CreateCredentialResponse {
            credential_id: "cred-1".to_owned(),
            secret: "s3cret".to_owned(),
        }))
    }

    async fn revoke_credential(
        &self,
        request: Request<RevokeCredentialRequest>,
    ) -> Result<Response<RevokeCredentialResponse>, Status> {
        self.record_auth(&request);
        Ok(Response::new(RevokeCredentialResponse {}))
    }

    async fn describe_cluster(
        &self,
        request: Request<DescribeClusterRequest>,
    ) -> Result<Response<DescribeClusterResponse>, Status> {
        self.record_auth(&request);
        Ok(Response::new(DescribeClusterResponse {
            nodes: vec![Node {
                id: 1,
                address: "10.0.0.1:7100".to_owned(),
                role: NodeRole::Leader as i32,
                health: NodeHealth::Healthy as i32,
                is_raft_leader: true,
                speaks_min: Some(ClusterVersion { major: 0, minor: 1 }),
                speaks_max: Some(ClusterVersion { major: 0, minor: 2 }),
                index_memory_bytes: self.reports_index_memory.then_some(2 * 1024 * 1024),
            }],
            partitions: vec![Partition {
                id: 10,
                keyspace_id: 1,
                start_key: Vec::new(),
                end_key: Vec::new(),
                owner_node_id: 2,
                epoch: 3,
                committed_lamport: Some(500),
                replicas: vec![Replica {
                    node_id: 3,
                    applied_lamport: Some(490),
                    durable_lamport: Some(495),
                }],
                size_bytes: Some(1_048_576),
                index_bytes: self.reports_index_memory.then_some(65_536),
            }],
            cluster_version: Some(ClusterVersion { major: 0, minor: 2 }),
            keyspaces: vec![Keyspace {
                config: Some(KeyspaceConfig {
                    max_storage_bytes: self.quota_bytes,
                    ..keyspace("orders").config.expect("the helper sets one")
                }),
                ..keyspace("orders")
            }],
        }))
    }

    async fn split_partition(
        &self,
        _: Request<SplitPartitionRequest>,
    ) -> Result<Response<SplitPartitionResponse>, Status> {
        Err(Status::unimplemented("not needed by these tests"))
    }

    async fn merge_partitions(
        &self,
        _: Request<MergePartitionsRequest>,
    ) -> Result<Response<MergePartitionsResponse>, Status> {
        Err(Status::unimplemented("not needed by these tests"))
    }

    async fn transfer_ownership(
        &self,
        _: Request<TransferOwnershipRequest>,
    ) -> Result<Response<TransferOwnershipResponse>, Status> {
        Err(Status::unimplemented("not needed by these tests"))
    }

    async fn finalize_upgrade(
        &self,
        request: Request<FinalizeUpgradeRequest>,
    ) -> Result<Response<FinalizeUpgradeResponse>, Status> {
        self.record_auth(&request);
        Ok(Response::new(FinalizeUpgradeResponse {
            previous: Some(ClusterVersion { major: 0, minor: 1 }),
            active: Some(ClusterVersion { major: 0, minor: 2 }),
        }))
    }
}

#[tonic::async_trait]
impl Kv for Fake {
    async fn get_limits(
        &self,
        request: Request<orbita_proto::GetLimitsRequest>,
    ) -> Result<Response<orbita_proto::GetLimitsResponse>, Status> {
        self.record_auth(&request);
        Ok(Response::new(orbita_proto::GetLimitsResponse {
            max_key_bytes: 10 * 1024,
            max_value_bytes: 256 * 1024,
            max_list_entries: 1000,
            max_list_bytes: 4 * 1024 * 1024,
            max_message_bytes: 4 * 1024 * 1024 + 64 * 1024,
        }))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        self.record_auth(&request);
        Ok(Response::new(GetResponse {
            found: self.key_exists,
            value: if self.key_exists {
                b"hello".to_vec()
            } else {
                Vec::new()
            },
            version: 42,
            expires_at_millis: None,
        }))
    }

    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        self.record_auth(&request);
        let request = request.into_inner();
        {
            let mut seen = self.seen.lock().unwrap();
            seen.set_key = Some(request.key.clone());
            seen.set_condition = request.condition.is_some();
        }
        Ok(Response::new(SetResponse {
            applied: self.condition_holds,
            version: 43,
            current_version: if self.condition_holds { None } else { Some(41) },
        }))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        self.record_auth(&request);
        Ok(Response::new(DeleteResponse {
            applied: true,
            existed: true,
            current_version: None,
        }))
    }

    async fn list(&self, request: Request<ListRequest>) -> Result<Response<ListResponse>, Status> {
        self.record_auth(&request);
        Ok(Response::new(ListResponse {
            entries: Vec::new(),
            next_cursor: Vec::new(),
        }))
    }
}

/// Starts the stand-in on a loopback port and returns a session pointing at it.
///
/// The port comes from the listener rather than from a guess, so parallel test
/// binaries cannot collide. A non-interactive session is what the one-shot
/// binary builds, and it is what these tests exercise the command functions
/// through, so the channel is dialed once and shared exactly as it is in
/// production.
async fn start(fake: Fake) -> (Session, Arc<Mutex<Seen>>) {
    let seen = Arc::clone(&fake.seen);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();

    let service = fake.clone();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(AdminServer::new(service.clone()))
            .add_service(KvServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });

    let mut layer = Layer::default();
    layer.client.endpoint = Some(format!("http://{address}"));
    layer.client.credential = Some("token-abc".to_owned());
    let config = layer.resolve().unwrap();
    let session = Session::new(config, Format::Human, false).unwrap();
    (session, seen)
}

fn fake() -> Fake {
    Fake {
        seen: Arc::new(Mutex::new(Seen::default())),
        key_exists: true,
        condition_holds: true,
        reports_index_memory: true,
        quota_bytes: None,
    }
}

#[tokio::test]
async fn listing_keyspaces_returns_every_keyspace_the_server_reported() {
    let (session, _) = start(fake()).await;
    let text = admin::keyspace(&session, Format::Human, KeyspaceCommand::List)
        .await
        .unwrap();
    assert!(text.contains("orders"), "{text}");
    assert!(text.contains("sessions"), "{text}");
}

#[tokio::test]
async fn a_credential_reaches_the_server_as_a_bearer_token() {
    let (session, seen) = start(fake()).await;
    admin::keyspace(&session, Format::Json, KeyspaceCommand::List)
        .await
        .unwrap();
    assert_eq!(
        seen.lock().unwrap().authorization.as_deref(),
        Some("Bearer token-abc")
    );
}

#[tokio::test]
async fn creating_a_keyspace_sends_the_name_and_renders_what_came_back() {
    let (session, seen) = start(fake()).await;
    let text = admin::keyspace(
        &session,
        Format::Json,
        KeyspaceCommand::Create {
            name: "orders".to_owned(),
            config: KeyspaceConfigArgs {
                default_ttl: Some(60_000),
                ..KeyspaceConfigArgs::default()
            },
        },
    )
    .await
    .unwrap();
    assert_eq!(
        seen.lock().unwrap().keyspace_name.as_deref(),
        Some("orders")
    );
    let json: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(json["name"], "orders");
    assert_eq!(json["config"]["default_ttl_millis"], 60_000);
}

#[tokio::test]
async fn a_mismatched_confirmation_stops_before_the_request_is_sent() {
    let (session, seen) = start(fake()).await;
    let err = admin::keyspace(
        &session,
        Format::Human,
        KeyspaceCommand::Delete {
            name: "orders".to_owned(),
            confirm: "order".to_owned(),
        },
    )
    .await
    .unwrap_err();
    assert!(
        format!("{err:#}").contains("Nothing was deleted"),
        "{err:#}"
    );
    assert!(seen.lock().unwrap().authorization.is_none());
}

#[tokio::test]
async fn describing_the_cluster_shows_nodes_and_replica_lag() {
    let (session, _) = start(fake()).await;
    let text = admin::cluster(
        &session,
        Format::Human,
        ClusterCommand::Describe { keyspace: None },
    )
    .await
    .unwrap();
    assert!(text.contains("10.0.0.1:7100"), "{text}");
    assert!(text.contains("leader"), "{text}");
    // Replica 3 has applied 490 of the owner's 500.
    assert!(text.contains("3(-10)"), "{text}");
}

/// The summary is what an operator reads first, so it has to be there before
/// any table and it has to be right without reading one.
#[tokio::test]
async fn describing_the_cluster_leads_with_a_summary_of_what_is_wrong() {
    let (session, _) = start(fake()).await;
    let text = admin::cluster(
        &session,
        Format::Human,
        ClusterCommand::Describe { keyspace: None },
    )
    .await
    .unwrap();
    let summary = text.find("CLUSTER").unwrap();
    assert!(summary < text.find("NODES").unwrap(), "{text}");
    assert!(text.contains("1 total, 1 healthy"), "{text}");
    assert!(text.contains("raft leader  node 1"), "{text}");
    // The partition is owned by node 2, which the cluster did not report, so
    // that is worth saying rather than leaving to be noticed.
    assert!(text.contains("2 (unknown)"), "{text}");
}

#[tokio::test]
async fn the_describe_json_carries_the_summary_a_script_would_alert_on() {
    let (session, _) = start(fake()).await;
    let text = admin::cluster(
        &session,
        Format::Json,
        ClusterCommand::Describe { keyspace: None },
    )
    .await
    .unwrap();
    let json: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(json["summary"]["healthy_nodes"], 1);
    assert_eq!(json["summary"]["max_replica_lag"], 10);
    assert_eq!(json["summary"]["partitions_with_an_unhealthy_owner"], 1);
    assert_eq!(json["partitions"][0]["replicas"][0]["applied_lamport"], 490);
}

#[tokio::test]
async fn describing_the_cluster_shows_what_it_is_consuming_end_to_end() {
    // The whole point of the change: an operator can see the cluster is
    // correct and still not know whether it is about to run out of room.
    let (session, _) = start(fake()).await;
    let text = admin::cluster(
        &session,
        Format::Human,
        ClusterCommand::Describe { keyspace: None },
    )
    .await
    .unwrap();
    assert!(text.contains("index memory"), "{text}");
    assert!(text.contains("2.0 MiB"), "the node's index memory: {text}");
    assert!(text.contains("64.0 KiB"), "the partition's index: {text}");
    assert!(
        text.contains("WAL LAG"),
        "the partition table has a column: {text}"
    );
    assert!(
        text.contains("5 wal"),
        "and the summary has the worst one: {text}"
    );
    assert!(text.contains("KEYSPACES"), "{text}");
}

#[tokio::test]
async fn the_describe_json_carries_every_consumption_signal_for_a_script() {
    let (session, _) = start(fake()).await;
    let text = admin::cluster(
        &session,
        Format::Json,
        ClusterCommand::Describe { keyspace: None },
    )
    .await
    .unwrap();
    let json: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(json["nodes"][0]["index_memory_bytes"], 2 * 1024 * 1024);
    assert_eq!(json["partitions"][0]["index_bytes"], 65_536);
    assert_eq!(json["partitions"][0]["size_bytes"], 1_048_576);
    assert_eq!(json["partitions"][0]["replicas"][0]["durable_lamport"], 495);
    assert_eq!(json["summary"]["max_wal_lag"], 5);
    assert_eq!(json["keyspaces"][0]["stored_bytes"], 4096);
    assert_eq!(
        json["keyspaces"][0]["max_storage_bytes"],
        serde_json::Value::Null,
        "a keyspace with no quota reports none rather than a number"
    );
}

#[tokio::test]
async fn a_cluster_that_does_not_report_index_memory_describes_it_as_unknown() {
    // The mixed-version case, end to end: an older leader group leaves the
    // field unset, and every hop from decode to render has to keep saying
    // "nobody measured this" rather than "this index is empty". Zero would
    // tell an operator they have memory headroom during exactly the window
    // where they are most likely to be checking.
    let (session, _) = start(Fake {
        reports_index_memory: false,
        ..fake()
    })
    .await;

    let text = admin::cluster(
        &session,
        Format::Human,
        ClusterCommand::Describe { keyspace: None },
    )
    .await
    .unwrap();
    assert!(text.contains("unknown"), "{text}");
    assert!(
        text.contains("index memory unknown"),
        "the total says it is missing rather than summing to nothing: {text}"
    );
    assert!(
        text.contains("not reporting"),
        "and says how much of it is missing: {text}"
    );
    assert!(
        !text.contains("0 B"),
        "no unmeasured index may be rendered as an empty one: {text}"
    );

    let json: serde_json::Value = serde_json::from_str(
        &admin::cluster(
            &session,
            Format::Json,
            ClusterCommand::Describe { keyspace: None },
        )
        .await
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        json["nodes"][0]["index_memory_bytes"],
        serde_json::Value::Null,
        "a script reading this must not see a number that is not there"
    );
    assert_eq!(
        json["partitions"][0]["index_bytes"],
        serde_json::Value::Null
    );
    assert_eq!(
        json["summary"]["index_memory_bytes"],
        serde_json::Value::Null,
        "and the total is absent rather than a partial sum wearing its name"
    );
    assert_eq!(json["summary"]["nodes_without_index_memory"], 1);
}

#[tokio::test]
async fn a_keyspace_at_a_zero_byte_quota_describes_as_over_and_not_as_unlimited() {
    // Some(0) is a cap that allows nothing. Treated as no measurement it
    // printed the same dash an unlimited keyspace gets, hiding a tenant that
    // is entirely over its limit behind the rendering of one that has none.
    let (session, _) = start(Fake {
        quota_bytes: Some(0),
        ..fake()
    })
    .await;

    let text = admin::cluster(
        &session,
        Format::Human,
        ClusterCommand::Describe { keyspace: None },
    )
    .await
    .unwrap();
    assert!(
        text.contains("at or over their storage quota"),
        "the summary names it: {text}"
    );
    assert!(text.contains("over"), "and the row does too: {text}");

    let json: serde_json::Value = serde_json::from_str(
        &admin::cluster(
            &session,
            Format::Json,
            ClusterCommand::Describe { keyspace: None },
        )
        .await
        .unwrap(),
    )
    .unwrap();
    assert_eq!(json["keyspaces"][0]["max_storage_bytes"], 0);
    assert_eq!(json["summary"]["keyspaces_over_quota"], 1);
}

#[tokio::test]
async fn describing_the_cluster_shows_the_active_version_and_what_each_node_speaks() {
    let (session, _) = start(fake()).await;
    let text = admin::cluster(
        &session,
        Format::Human,
        ClusterCommand::Describe { keyspace: None },
    )
    .await
    .unwrap();
    assert!(text.contains("version      0.2"), "{text}");
    assert!(text.contains("0.1..0.2"), "{text}");
}

#[tokio::test]
async fn finalizing_an_upgrade_reports_the_advance_and_warns_that_rollback_is_gone() {
    let (session, seen) = start(fake()).await;
    let text = admin::cluster(&session, Format::Human, ClusterCommand::FinalizeUpgrade)
        .await
        .unwrap();
    assert!(text.contains("from 0.1 to 0.2"), "{text}");
    assert!(text.contains("not"), "{text}");
    assert!(text.contains("supported"), "{text}");
    assert_eq!(
        seen.lock().unwrap().authorization.as_deref(),
        Some("Bearer token-abc")
    );
}

#[tokio::test]
async fn the_finalize_json_carries_both_versions_for_a_script() {
    let (session, _) = start(fake()).await;
    let text = admin::cluster(&session, Format::Json, ClusterCommand::FinalizeUpgrade)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(json["previous"], "0.1");
    assert_eq!(json["active"], "0.2");
}

#[tokio::test]
async fn a_get_that_found_the_key_exits_zero_and_prints_the_value_first() {
    let (session, _) = start(fake()).await;
    let outcome = data::get(
        &session,
        Format::Human,
        GetArgs {
            keyspace: Some("demo".to_owned()),
            key: Some("greeting".to_owned()),
        },
    )
    .await
    .unwrap();
    assert_eq!(outcome.code, 0);
    assert!(outcome.text.starts_with("hello\n"), "{}", outcome.text);
}

#[tokio::test]
async fn a_get_that_missed_exits_with_the_not_found_code_rather_than_failing() {
    let (session, _) = start(Fake {
        key_exists: false,
        ..fake()
    })
    .await;
    let outcome = data::get(
        &session,
        Format::Json,
        GetArgs {
            keyspace: Some("demo".to_owned()),
            key: Some("greeting".to_owned()),
        },
    )
    .await
    .unwrap();
    assert_eq!(outcome.code, EXIT_NOT_FOUND);
    let json: serde_json::Value = serde_json::from_str(&outcome.text).unwrap();
    assert_eq!(json["found"], false);
    assert!(json["value"].is_null());
}

fn set_args() -> SetArgs {
    SetArgs {
        keyspace: Some("demo".to_owned()),
        key: Some("locks/leader".to_owned()),
        value: Some("node-1".to_owned()),
        value_file: None,
        ttl: None,
        if_not_present: true,
        if_version: None,
    }
}

#[tokio::test]
async fn a_conditional_write_that_applied_exits_zero() {
    let (session, seen) = start(fake()).await;
    let outcome = data::set(&session, Format::Human, set_args())
        .await
        .unwrap();
    assert_eq!(outcome.code, 0);
    assert_eq!(outcome.text, "ok, version 43\n");
    let seen = seen.lock().unwrap();
    assert_eq!(seen.set_key.as_deref(), Some(b"locks/leader".as_slice()));
    assert!(seen.set_condition, "the condition did not reach the server");
}

#[tokio::test]
async fn a_conditional_write_that_lost_the_race_exits_with_its_own_code() {
    let (session, _) = start(Fake {
        condition_holds: false,
        ..fake()
    })
    .await;
    let outcome = data::set(&session, Format::Human, set_args())
        .await
        .unwrap();
    assert_eq!(outcome.code, EXIT_CONDITION_NOT_MET);
    assert!(outcome.text.contains("version 41"), "{}", outcome.text);
}

#[tokio::test]
async fn an_unreachable_endpoint_reports_an_error_rather_than_hanging() {
    let mut layer = Layer::default();
    // Port 1 on loopback is never listening, and connecting fails fast.
    layer.client.endpoint = Some("http://127.0.0.1:1".to_owned());
    let config = layer.resolve().unwrap();
    let session = Session::new(config, Format::Human, false).unwrap();
    let result = admin::keyspace(&session, Format::Human, KeyspaceCommand::List).await;
    assert!(result.is_err());
}

/// Both services are reachable through the one endpoint a client is given,
/// which is what lets the CLI and an application share a single address.
#[tokio::test]
async fn the_admin_and_data_services_share_one_endpoint() {
    let (session, _) = start(fake()).await;
    assert!(
        admin::keyspace(&session, Format::Json, KeyspaceCommand::List)
            .await
            .is_ok()
    );
    assert!(data::get(
        &session,
        Format::Json,
        GetArgs {
            keyspace: Some("demo".to_owned()),
            key: Some("k".to_owned()),
        }
    )
    .await
    .is_ok());
}
