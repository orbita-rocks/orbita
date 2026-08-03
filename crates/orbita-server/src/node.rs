//! The worker: routing, the host registry, and the cached partition map.
//!
//! Everything a client asks for arrives here as a protobuf message and leaves
//! as one. In between, this decides which partition owns the key and whether
//! this node can answer for it, and forwards the untouched request to the
//! owner when it cannot. A client never learns that any of this happened.
//!
//! # Three decisions the brief asked for
//!
//! **Map staleness is repaired lazily.** The cached map is refreshed when a
//! forwarded request comes back saying the receiver is not the owner, and on
//! nothing else. Pushing updates from the leader group would make every worker
//! a subscriber, and polling on a timer spends requests to learn something
//! that is almost always unchanged. Lazy repair makes exactly the first
//! request after a failover pay for it, which is the cheapest correct answer.
//!
//! **A forwarded request is never forwarded again.** A node that receives a
//! proxied request and does not own the partition answers `NotOwner` carrying
//! the current owner. The origin repairs its map and retries once. Two hops
//! would let a stale map anywhere in the cluster turn one request into a loop.
//!
//! **Backpressure is the transport's.** There is no queue of forwarded
//! requests here, so a saturated owner slows the callers holding its
//! connections rather than accumulating work that will be too late by the time
//! it runs.

use crate::host::{HostSpec, LeasePolicy, PartitionHost, PartitionPaths, Read, WriteAck, WriteOp};
use crate::lease::DEFAULT_LEASE_MARGIN;
use crate::map_source::{BoxedMapSource, MapSource};
use crate::proxy;
use crate::replication::{Applies, ReplicaBridge};
use crate::validate;

use bytes::Bytes;
use orbita_core::{
    Error, KeyspaceId, KeyspaceInfo, NodeId, PartitionId, PartitionInfo, PartitionMap, Result,
    Version,
};
use orbita_proto::v1::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, ListEntry, ListRequest, ListResponse,
    SetRequest, SetResponse,
};
use orbita_runtime::{
    PeerCall, PeerHandler, Runtime, ServiceId, Transport, TransportError, TransportResult,
};
use orbita_wal::WalService;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock, Weak};
use std::time::Duration;

/// Where a node's files go.
///
/// The two roots are different layers rather than a style choice: the log goes
/// through `orbita_runtime::Disk`, which is rooted at the data directory and
/// takes relative paths, and RocksDB does its own I/O below that seam and
/// takes real ones.
#[derive(Debug, Clone)]
pub(crate) struct DataLayout {
    pub storage_root: std::path::PathBuf,
    pub wal_root: String,
}

impl DataLayout {
    fn paths(&self, partition: PartitionId) -> PartitionPaths {
        PartitionPaths {
            storage_path: self
                .storage_root
                .join(format!("p{}", partition.get()))
                .to_string_lossy()
                .into_owned(),
            wal_dir: format!("{}/p{}", self.wal_root, partition.get()),
        }
    }
}

/// One worker.
pub(crate) struct Node<R: Runtime> {
    runtime: R,
    node_id: NodeId,
    layout: DataLayout,
    map: RwLock<Arc<PartitionMap>>,
    source: BoxedMapSource,
    hosts: tokio::sync::RwLock<HashMap<PartitionId, Arc<PartitionHost<R>>>>,
    wal_service: WalService<R>,
    /// What turns inbound replication into invalidations and applies. Held
    /// here because it outlives any one partition and has to be handed the
    /// hosts as they open.
    bridge: Arc<ReplicaBridge<R>>,
    /// The queue replicated entries are applied from. Held rather than read:
    /// owning it is what makes the applier stop when this node does, instead
    /// of when the transport that happens to hold the observer is torn down.
    #[allow(dead_code)]
    applies: Arc<Applies>,
    lease: LeasePolicy,
    /// How many reads this node answered from a partition it only replicates.
    replica_reads: AtomicU64,
    /// Whether the last attempt to match the open partitions to the map
    /// failed, which is what makes the next refresh try again.
    unreconciled: std::sync::atomic::AtomicBool,
}

/// Where a request has to go.
enum Hop<R: Runtime> {
    /// This node can answer, and where the request would have gone if it could
    /// not. A replica read carries that because it decides again once it has
    /// read, and needs somewhere to send the request when the second look says
    /// it may not answer after all.
    Local(Arc<PartitionHost<R>>, Option<NodeId>, PartitionId),
    /// The owner, and the partition it owns. The partition travels with it so
    /// that a refusal can name what it is refusing.
    Forward(NodeId, PartitionId),
}

/// What a request needs from the partition, which decides whether a replica
/// may answer it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Purpose {
    /// One key, which per-key invalidation can speak for.
    Read,
    /// A range, which it cannot. Invalidation names one key at a time, so a
    /// replica has nothing that says a whole range is unchanged, and a scan
    /// could return a key whose write it has been told about but not yet
    /// applied. Scans therefore go to the owner. Making them serveable needs a
    /// range-level guard, which is a design decision rather than an omission.
    Scan,
    Write,
}

impl<R: Runtime> Node<R> {
    /// Fetches the map, opens every partition this node holds, and starts
    /// serving peer traffic.
    pub(crate) async fn start(
        runtime: R,
        node_id: NodeId,
        layout: DataLayout,
        source: BoxedMapSource,
        lease_duration: Duration,
    ) -> Result<Arc<Self>> {
        let map = source.fetch().await?;
        let (bridge, applies) = ReplicaBridge::start(&runtime);
        let wal_service = WalService::new();
        // Registered before anything is opened, so the first batch a replica
        // receives cannot land without having been invalidated for.
        wal_service.observe(Arc::clone(&bridge) as Arc<dyn orbita_wal::ReplicaObserver>);

        let node = Arc::new(Self {
            runtime: runtime.clone(),
            node_id,
            layout,
            map: RwLock::new(Arc::new(map)),
            source,
            hosts: tokio::sync::RwLock::new(HashMap::new()),
            wal_service,
            bridge,
            applies,
            lease: LeasePolicy {
                duration: lease_duration,
                margin: DEFAULT_LEASE_MARGIN,
            },
            replica_reads: AtomicU64::new(0),
            unreconciled: std::sync::atomic::AtomicBool::new(false),
        });
        node.reconcile().await?;

        runtime
            .transport()
            .register(ServiceId::Wal, node.wal_service.clone());
        runtime.transport().register(
            ServiceId::Proxy,
            ProxyService {
                node: Arc::downgrade(&node),
            },
        );
        Ok(node)
    }

    #[must_use]
    pub(crate) fn map(&self) -> Arc<PartitionMap> {
        Arc::clone(&self.map.read().expect("partition map poisoned"))
    }

    /// Asks the map source for a newer map and opens or drops hosts to match.
    ///
    /// An older map is ignored rather than applied, because updates can arrive
    /// out of order and applying one would resurrect a deposed owner in this
    /// node's routing table.
    ///
    /// A reconcile that fails is retried on the next refresh even though the
    /// map has not moved again. Opening a partition can fail for reasons that
    /// pass, the loudest being that the previous incarnation still has the
    /// storage engine open, and a node that recorded the new map without
    /// acting on it would refuse every request for a partition it was just
    /// given and never try again.
    pub(crate) async fn refresh_map(&self) -> Result<()> {
        let fetched = self.source.fetch().await?;
        {
            let mut held = self.map.write().expect("partition map poisoned");
            if fetched.version() > held.version() {
                *held = Arc::new(fetched);
            } else if !self.unreconciled.load(Ordering::Acquire) {
                return Ok(());
            }
        }
        let outcome = self.reconcile().await;
        self.unreconciled.store(outcome.is_err(), Ordering::Release);
        if let Err(error) = &outcome {
            tracing::warn!(%error, "could not open every partition this node was given");
        }
        outcome
    }

    /// Brings the set of open partitions in line with the map.
    async fn reconcile(&self) -> Result<()> {
        let map = self.map();
        let wanted: Vec<PartitionInfo> = map.held_by(self.node_id).cloned().collect();

        let mut hosts = self.hosts.write().await;
        hosts.retain(|id, _| {
            let keep = wanted.iter().any(|p| p.id == *id);
            if !keep {
                self.wal_service.unregister(*id);
                self.bridge.unregister(*id);
            }
            keep
        });

        for info in wanted {
            // A host is reopened when its epoch moves or when this node's role
            // in it changes, and both halves matter. The epoch moving is when
            // the log has to be reopened and the pending set thrown away. The
            // role changing at the same epoch is what a promotion looks like
            // from here, because the control plane bumps the epoch when it
            // fences the dead owner and then names the replacement without
            // bumping it again. A node that only watched the epoch would stay
            // a replica of a partition it now owns, and refuse every request
            // for it.
            let owned_here = info.owner == Some(self.node_id);
            let unchanged = hosts
                .get(&info.id)
                .is_some_and(|held| held.epoch() == info.epoch && held.is_owner() == owned_here);
            if unchanged {
                continue;
            }
            // The old incarnation is closed before the new one opens, because
            // both want the same storage engine directory and RocksDB holds a
            // lock on it. Opening first and replacing after would fail every
            // promotion, which is the one time this path matters.
            hosts.remove(&info.id);
            self.wal_service.unregister(info.id);
            self.bridge.unregister(info.id);

            let paths = self.layout.paths(info.id);
            let spec = HostSpec {
                id: info.id,
                epoch: info.epoch,
                range: info.range.clone(),
                lease: self.lease,
            };
            let host = if owned_here {
                self.wal_service.unregister(info.id);
                self.bridge.unregister(info.id);
                PartitionHost::open_owner(self.runtime.clone(), spec, &paths, info.replicas.clone())
                    .await?
            } else {
                let host = PartitionHost::open_replica(self.runtime.clone(), spec, &paths).await?;
                // The bridge is registered before the log, so an entry cannot
                // be accepted by the log with nothing watching it.
                self.bridge.register(&host);
                self.wal_service.register(host.log());
                host
            };
            hosts.insert(info.id, host);
        }
        Ok(())
    }

    fn keyspace(&self, name: &str) -> Result<KeyspaceInfo> {
        validate::keyspace_name(name)?;
        self.map()
            .keyspace_by_name(name)
            .cloned()
            .ok_or(Error::KeyspaceNotFound)
    }

    /// Finds the partition owning `key` and decides whether this node can
    /// answer for it.
    async fn hop(&self, keyspace: KeyspaceId, key: &[u8], purpose: Purpose) -> Result<Hop<R>> {
        let map = self.map();
        let info = map.lookup(keyspace, key).ok_or_else(|| {
            // A hole in the map is not something a retry fixes, so it is
            // reported rather than papered over.
            Error::Internal("no partition covers this key".to_string())
        })?;

        let host = self.hosts.read().await.get(&info.id).cloned();
        if let Some(host) = host {
            // The owner answers everything. A replica answers a single-key
            // read only under the conditions in ADR 0001, and never answers a
            // write or a scan.
            if host.is_owner() || (purpose == Purpose::Read && host.might_serve(key)) {
                return Ok(Hop::Local(host, info.owner, info.id));
            }
        }
        match info.owner {
            Some(owner) => Ok(Hop::Forward(owner, info.id)),
            None => Err(Error::Unavailable(format!(
                "partition {} has no owner",
                info.id
            ))),
        }
    }

    async fn forward<Req, Res>(&self, to: NodeId, method: u16, request: &Req) -> Result<Res>
    where
        Req: prost::Message,
        Res: prost::Message + Default,
    {
        let call = PeerCall {
            service: ServiceId::Proxy,
            method,
            payload: Bytes::from(request.encode_to_vec()),
        };
        let reply = self
            .runtime
            .transport()
            .call(to, call)
            .await
            .map_err(transport_error)?;
        proxy::decode_reply::<Res>(&reply)
    }

    pub(crate) async fn get(&self, request: GetRequest, forwarded: bool) -> Result<GetResponse> {
        match self.try_get(&request, forwarded).await {
            Err(error) if self.should_repair(&error, forwarded) => {
                self.repair().await;
                self.try_get(&request, forwarded).await
            }
            other => other,
        }
    }

    async fn try_get(&self, request: &GetRequest, forwarded: bool) -> Result<GetResponse> {
        let keyspace = self.keyspace(&request.keyspace)?;
        validate::key(&request.key)?;

        let (owner, partition) = match self.hop(keyspace.id, &request.key, Purpose::Read).await? {
            Hop::Local(host, owner, partition) => match host.read(&request.key).await? {
                Read::Served(record) => {
                    if !host.is_owner() {
                        // Counted because "replicas serve reads" is the claim
                        // the read path exists to make, and it is otherwise
                        // invisible from outside: a forwarded read and a
                        // locally served one give the client the same answer.
                        self.replica_reads.fetch_add(1, Ordering::Relaxed);
                    }
                    return Ok(match record {
                        None => GetResponse::default(),
                        Some(record) => GetResponse {
                            found: true,
                            value: record.value.to_vec(),
                            version: record.version.get(),
                            expires_at_millis: record.expires_at_millis,
                        },
                    });
                }
                // A write for this key landed while the read was in progress,
                // so what was read is already old news. The owner answers it.
                Read::MustForward => (owner, partition),
            },
            Hop::Forward(owner, partition) => (Some(owner), partition),
        };

        let Some(owner) = owner else {
            return Err(Error::Unavailable(format!(
                "partition {partition} has no owner"
            )));
        };
        self.refuse_second_hop(forwarded, owner, partition)?;
        self.forward(owner, proxy::METHOD_GET, request).await
    }

    pub(crate) async fn set(&self, request: SetRequest, forwarded: bool) -> Result<SetResponse> {
        match self.try_set(&request, forwarded).await {
            Err(error) if self.should_repair(&error, forwarded) => {
                self.repair().await;
                self.try_set(&request, forwarded).await
            }
            other => other,
        }
    }

    async fn try_set(&self, request: &SetRequest, forwarded: bool) -> Result<SetResponse> {
        let keyspace = self.keyspace(&request.keyspace)?;
        validate::key(&request.key)?;
        validate::value(&request.value)?;
        if let Some(limit) = keyspace.max_value_bytes {
            if request.value.len() as u64 > limit {
                return Err(Error::TooLarge {
                    what: "value",
                    size: request.value.len(),
                    limit: limit as usize,
                });
            }
        }

        match self.hop(keyspace.id, &request.key, Purpose::Write).await? {
            Hop::Local(host, ..) => {
                let op = WriteOp::Put {
                    value: Bytes::from(request.value.clone()),
                    // A keyspace's default TTL applies to a write that did not
                    // ask for one, which is what makes a session keyspace
                    // usable without every caller remembering.
                    ttl_millis: request.ttl_millis.or(keyspace.default_ttl_millis),
                };
                let ack = host
                    .write(
                        Bytes::from(request.key.clone()),
                        op,
                        validate::condition(request.condition.as_ref()),
                    )
                    .await?;
                Ok(SetResponse {
                    applied: ack.applied,
                    version: ack.version.unwrap_or(Version::ZERO).get(),
                    current_version: ack.current_version.map(Version::get),
                })
            }
            Hop::Forward(owner, partition) => {
                self.refuse_second_hop(forwarded, owner, partition)?;
                self.forward(owner, proxy::METHOD_SET, request).await
            }
        }
    }

    pub(crate) async fn delete(
        &self,
        request: DeleteRequest,
        forwarded: bool,
    ) -> Result<DeleteResponse> {
        match self.try_delete(&request, forwarded).await {
            Err(error) if self.should_repair(&error, forwarded) => {
                self.repair().await;
                self.try_delete(&request, forwarded).await
            }
            other => other,
        }
    }

    async fn try_delete(&self, request: &DeleteRequest, forwarded: bool) -> Result<DeleteResponse> {
        let keyspace = self.keyspace(&request.keyspace)?;
        validate::key(&request.key)?;

        match self.hop(keyspace.id, &request.key, Purpose::Write).await? {
            Hop::Local(host, ..) => {
                let ack: WriteAck = host
                    .write(
                        Bytes::from(request.key.clone()),
                        WriteOp::Delete,
                        validate::condition(request.condition.as_ref()),
                    )
                    .await?;
                Ok(DeleteResponse {
                    applied: ack.applied,
                    existed: ack.existed,
                    current_version: ack.current_version.map(Version::get),
                })
            }
            Hop::Forward(owner, partition) => {
                self.refuse_second_hop(forwarded, owner, partition)?;
                self.forward(owner, proxy::METHOD_DELETE, request).await
            }
        }
    }

    pub(crate) async fn list(&self, request: ListRequest, forwarded: bool) -> Result<ListResponse> {
        match self.try_list(&request, forwarded).await {
            Err(error) if self.should_repair(&error, forwarded) => {
                self.repair().await;
                self.try_list(&request, forwarded).await
            }
            other => other,
        }
    }

    async fn try_list(&self, request: &ListRequest, forwarded: bool) -> Result<ListResponse> {
        let keyspace = self.keyspace(&request.keyspace)?;
        validate::prefix(&request.prefix)?;
        let limit = validate::list_limit(request.limit);

        let resume = Cursor::decode(&request.cursor, &request.prefix)?;
        match self
            .hop(keyspace.id, &resume.route_key, Purpose::Scan)
            .await?
        {
            Hop::Local(host, ..) => {
                let page = host
                    .scan(&request.prefix, resume.inner.as_deref(), limit)
                    .await?;
                let entries = page
                    .entries
                    .iter()
                    .map(|entry| ListEntry {
                        key: entry.key.to_vec(),
                        value: if request.include_values {
                            entry.record.value.to_vec()
                        } else {
                            Vec::new()
                        },
                        version: entry.record.version.get(),
                        expires_at_millis: entry.record.expires_at_millis,
                    })
                    .collect();

                let next = match (&page.cursor, page.entries.last()) {
                    // More of this partition to read.
                    (Some(inner), Some(last)) => {
                        Some(Cursor::within(last.key.clone(), inner.clone()).encode())
                    }
                    // This partition is done. The scan continues in the next
                    // one when the prefix can still reach past this range.
                    _ => self
                        .next_range_start(keyspace.id, &resume.route_key, &request.prefix)
                        .map(|start| Cursor::at_partition_boundary(start).encode()),
                };

                Ok(ListResponse {
                    entries,
                    next_cursor: next.map(|c| c.to_vec()).unwrap_or_default(),
                })
            }
            Hop::Forward(owner, partition) => {
                self.refuse_second_hop(forwarded, owner, partition)?;
                self.forward(owner, proxy::METHOD_LIST, request).await
            }
        }
    }

    /// Where a scan picks up in the next partition, if the prefix reaches into
    /// one.
    fn next_range_start(&self, keyspace: KeyspaceId, key: &[u8], prefix: &[u8]) -> Option<Bytes> {
        let map = self.map();
        let end = map.lookup(keyspace, key)?.range.end()?;
        match prefix_upper_bound(prefix) {
            Some(upper) if end >= upper.as_slice() => None,
            _ => Some(Bytes::copy_from_slice(end)),
        }
    }

    /// A node that was forwarded a request and still cannot serve it says so
    /// rather than forwarding again. See the module docs.
    fn refuse_second_hop(
        &self,
        forwarded: bool,
        owner: NodeId,
        partition: PartitionId,
    ) -> Result<()> {
        if forwarded {
            return Err(Error::NotOwner {
                partition,
                owner: Some(owner),
            });
        }
        Ok(())
    }

    fn should_repair(&self, error: &Error, forwarded: bool) -> bool {
        !forwarded && matches!(error, Error::NotOwner { .. } | Error::StaleEpoch { .. })
    }

    async fn repair(&self) {
        if let Err(error) = self.refresh_map().await {
            tracing::warn!(%error, "could not refresh the partition map after a misroute");
        }
    }

    /// Takes a read lease this node's owner offered for one partition.
    ///
    /// Answering `false` is not a failure: it is a replica saying it is not
    /// caught up enough to serve reads, which the owner needs to know because
    /// it decides what to wait for from the same answer.
    async fn accept_lease(&self, grant: &proxy::LeaseGrant) -> Result<bool> {
        let host = self.hosts.read().await.get(&grant.partition).cloned();
        match host {
            Some(host) => Ok(host.accept_lease(grant).await),
            // A partition this node does not hold cannot serve a read from it
            // either, so refusing is the whole answer.
            None => Ok(false),
        }
    }

    /// How many reads this node has answered from a partition it replicates
    /// rather than owns.
    ///
    /// Zero on a node under write load means the read path is not doing its
    /// job, which is otherwise invisible: a forwarded read returns the same
    /// answer as a locally served one, only slower.
    pub(crate) fn replica_reads(&self) -> u64 {
        self.replica_reads.load(Ordering::Relaxed)
    }

    /// How far this node has got on every partition it holds.
    ///
    /// This is what the heartbeat to the leader group carries, and it is what
    /// that group compares when it has to choose a replacement owner.
    pub(crate) async fn progress(&self) -> Vec<orbita_control::PartitionProgress> {
        let hosts: Vec<Arc<PartitionHost<R>>> = self.hosts.read().await.values().cloned().collect();
        let mut progress = Vec::with_capacity(hosts.len());
        for host in hosts {
            progress.push(orbita_control::PartitionProgress {
                partition: host.id(),
                durable_lamport: host.durable_lamport().await,
                applied_lamport: host.committed_lamport().await.unwrap_or_default(),
                size_bytes: host.size_bytes().await.unwrap_or_default(),
            });
        }
        // Sorted so that two reports of the same state are the same bytes,
        // which keeps a heartbeat from looking like a change.
        progress.sort_unstable_by_key(|p| p.partition.get());
        progress
    }

    /// How often the lease heartbeat has to run for this node's leases to stay
    /// live.
    pub(crate) fn lease_interval(&self) -> Duration {
        self.lease.heartbeat_interval()
    }

    /// Renews every lease this node's owned partitions have out.
    ///
    /// One pass rather than a loop, following the control plane's `tick`, so
    /// that a simulated run drives it explicitly and never has a timer that
    /// keeps the world from going idle.
    pub(crate) async fn renew_leases(&self) {
        let hosts: Vec<Arc<PartitionHost<R>>> = self
            .hosts
            .read()
            .await
            .values()
            .filter(|host| host.is_owner())
            .cloned()
            .collect();
        for host in hosts {
            host.renew_leases().await;
        }
    }
}

/// Serves requests other nodes forwarded here.
struct ProxyService<R: Runtime> {
    /// Weak because the node owns the transport that owns this handler, and a
    /// strong reference would keep a shut down node alive forever.
    node: Weak<Node<R>>,
}

impl<R: Runtime> PeerHandler for ProxyService<R> {
    async fn handle(&self, _from: NodeId, call: PeerCall) -> TransportResult<Bytes> {
        let Some(node) = self.node.upgrade() else {
            return Err(TransportError::NoHandler(ServiceId::Proxy));
        };
        Ok(dispatch(&node, call).await)
    }
}

async fn dispatch<R: Runtime>(node: &Node<R>, call: PeerCall) -> Bytes {
    /// Runs one forwarded call, encoding whichever way it went.
    macro_rules! run {
        ($request:ty, $method:ident) => {
            match <$request as prost::Message>::decode(call.payload.as_ref()) {
                Ok(request) => match node.$method(request, true).await {
                    Ok(response) => proxy::encode_ok(&response),
                    Err(error) => proxy::encode_error(&error),
                },
                Err(e) => proxy::encode_error(&Error::InvalidArgument(format!(
                    "undecodable proxied request: {e}"
                ))),
            }
        };
    }

    match call.method {
        proxy::METHOD_LEASE => match proxy::LeaseGrant::decode(&call.payload) {
            Ok(grant) => match node.accept_lease(&grant).await {
                Ok(accepted) => proxy::encode_lease_reply(accepted),
                Err(error) => proxy::encode_error(&error),
            },
            Err(error) => proxy::encode_error(&error),
        },
        proxy::METHOD_GET => run!(GetRequest, get),
        proxy::METHOD_SET => run!(SetRequest, set),
        proxy::METHOD_DELETE => run!(DeleteRequest, delete),
        proxy::METHOD_LIST => run!(ListRequest, list),
        other => proxy::encode_error(&Error::Internal(format!("unknown proxy method {other}"))),
    }
}

fn transport_error(error: TransportError) -> Error {
    match error {
        // A peer that cannot be reached is a routing problem the client should
        // retry, not a request the client got wrong.
        TransportError::Timeout(_) | TransportError::Unreachable(_) => {
            Error::Unavailable(error.to_string())
        }
        other => Error::Internal(other.to_string()),
    }
}

/// Where a `LIST` resumes.
///
/// The storage engine's cursor names a key inside one partition. This wraps it
/// with the key the request has to be routed by, because between two pages the
/// partition that holds that key can split, merge, or move to another node,
/// and the routing decision has to be made afresh each time.
struct Cursor {
    route_key: Bytes,
    inner: Option<Bytes>,
}

impl Cursor {
    const VERSION: u8 = 1;

    fn within(route_key: Bytes, inner: Bytes) -> Self {
        Self {
            route_key,
            inner: Some(inner),
        }
    }

    /// The scan reached the end of a partition, so the next page starts at the
    /// beginning of the next one and needs no engine cursor.
    fn at_partition_boundary(route_key: Bytes) -> Self {
        Self {
            route_key,
            inner: None,
        }
    }

    fn encode(&self) -> Bytes {
        let inner = self.inner.as_deref().unwrap_or_default();
        let mut out = Vec::with_capacity(5 + inner.len() + self.route_key.len());
        out.push(Self::VERSION);
        out.extend_from_slice(&(inner.len() as u32).to_be_bytes());
        out.extend_from_slice(inner);
        out.extend_from_slice(&self.route_key);
        Bytes::from(out)
    }

    fn decode(raw: &[u8], prefix: &[u8]) -> Result<Self> {
        if raw.is_empty() {
            // A scan with no cursor starts at the prefix, which is also how it
            // is routed to the partition holding the first key under it.
            return Ok(Self {
                route_key: Bytes::copy_from_slice(prefix),
                inner: None,
            });
        }
        let malformed = || Error::InvalidArgument("malformed list cursor".to_string());
        if raw.len() < 5 || raw[0] != Self::VERSION {
            return Err(malformed());
        }
        let len = u32::from_be_bytes(raw[1..5].try_into().map_err(|_| malformed())?) as usize;
        if raw.len() < 5 + len {
            return Err(malformed());
        }
        let inner = &raw[5..5 + len];
        Ok(Self {
            route_key: Bytes::copy_from_slice(&raw[5 + len..]),
            inner: (!inner.is_empty()).then(|| Bytes::copy_from_slice(inner)),
        })
    }
}

/// The first key that is past everything under `prefix`.
///
/// `None` means the prefix reaches the end of the keyspace, either because it
/// is empty or because every trailing byte is already the maximum.
fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    while let Some(last) = upper.pop() {
        if last < u8::MAX {
            upper.push(last + 1);
            return Some(upper);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cursor_carries_both_the_routing_key_and_the_engines_own() {
        let cursor = Cursor::within(
            Bytes::from_static(b"users/42"),
            Bytes::from_static(b"engine-state"),
        );
        let decoded = Cursor::decode(&cursor.encode(), b"users/").unwrap();
        assert_eq!(decoded.route_key, Bytes::from_static(b"users/42"));
        assert_eq!(decoded.inner, Some(Bytes::from_static(b"engine-state")));
    }

    #[test]
    fn a_boundary_cursor_carries_no_engine_state() {
        let decoded = Cursor::decode(
            &Cursor::at_partition_boundary(Bytes::from_static(b"m")).encode(),
            b"",
        )
        .unwrap();
        assert_eq!(decoded.route_key, Bytes::from_static(b"m"));
        assert_eq!(
            decoded.inner, None,
            "the next partition starts its own scan at the prefix"
        );
    }

    #[test]
    fn an_absent_cursor_starts_the_scan_at_the_prefix() {
        let decoded = Cursor::decode(b"", b"locks/").unwrap();
        assert_eq!(decoded.route_key, Bytes::from_static(b"locks/"));
        assert!(decoded.inner.is_none());
    }

    #[test]
    fn a_garbled_cursor_is_refused_rather_than_guessed_at() {
        assert!(
            Cursor::decode(&[9, 0, 0, 0, 0], b"").is_err(),
            "wrong version"
        );
        assert!(Cursor::decode(&[1, 0, 0, 0, 9], b"").is_err(), "short body");
    }

    #[test]
    fn a_prefix_bounds_the_partitions_a_scan_can_reach() {
        assert_eq!(prefix_upper_bound(b"ab"), Some(b"ac".to_vec()));
        assert_eq!(prefix_upper_bound(b"a\xff"), Some(b"b".to_vec()));
        assert_eq!(
            prefix_upper_bound(b""),
            None,
            "an empty prefix is unbounded"
        );
        assert_eq!(prefix_upper_bound(b"\xff\xff"), None);
    }
}
