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
//! request finds the map wrong: a forwarded request that comes back saying the
//! receiver is not the owner, or a keyspace name the map has never heard of.
//! Pushing updates from the leader group would make every worker a subscriber,
//! and polling on a timer spends requests to learn something that is almost
//! always unchanged. Lazy repair makes exactly the first request after a
//! failover pay for it, which is the cheapest correct answer. The second
//! trigger is rate limited, because unlike a misroute it can be provoked by a
//! client that simply keeps asking for a name that does not exist.
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

use crate::auth::Authenticator;
use crate::config::DEFAULT_CONTROL_POLL_INTERVAL;
use crate::host::{HostSpec, LeasePolicy, PartitionHost, PartitionPaths, Read, WriteAck, WriteOp};
use crate::lease::DEFAULT_LEASE_MARGIN;
use crate::map_source::{BoxedMapSource, MapSource};
use crate::metrics;
use crate::proxy;
use crate::quota::{Admission, Direction};
use crate::readiness::{ReadinessCondition, ReadinessGate};
use crate::replication::{Applies, ReplicaBridge};
use crate::validate;

use bytes::Bytes;
use orbita_control::Permission;
use orbita_control::WireSplitIntent;
use orbita_core::{
    Error, KeyspaceId, KeyspaceInfo, NodeId, PartitionId, PartitionInfo, PartitionMap, Result,
    Version, MAX_KEY_BYTES, MAX_LIST_BYTES, MAX_LIST_LIMIT, MAX_VALUE_BYTES,
    MESSAGE_OVERHEAD_BYTES,
};
use orbita_format::{load_manifest, PartitionPath};
use orbita_objectstore::ObjectStore;
use orbita_proto::v1::{
    DeleteRequest, DeleteResponse, GetLimitsResponse, GetRequest, GetResponse, ListEntry,
    ListRequest, ListResponse, SetRequest, SetResponse,
};
use orbita_runtime::{
    join_all, Clock, PeerCall, PeerHandler, Runtime, ServiceId, Transport, TransportError,
    TransportResult,
};
use orbita_storage::{ChildSpec, ScanBudget};
use orbita_wal::WalService;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock, Weak};
use std::time::Duration;

/// The gRPC message-size ceiling implied by a keyspace whose values may reach
/// `max_value_bytes`.
///
/// This is the one piece of arithmetic behind both halves of the contract: it
/// is what `GetLimits` publishes as `max_message_bytes`, and, evaluated at the
/// hard value cap by [`max_transport_message_bytes`], what the transport is
/// sized to accept. Keeping it a single function is the whole point: a server
/// that advertised one number and enforced another would let a client send
/// exactly what it was told to and be refused by the transport before its
/// request was ever seen.
pub fn message_bytes_ceiling(max_value_bytes: u64) -> u64 {
    max_value_bytes.max(MAX_LIST_BYTES) + MESSAGE_OVERHEAD_BYTES
}

/// The largest message any keyspace on this cluster can make cross the wire.
///
/// This sizes the gRPC servers' decode and encode limits and the client
/// channel. It is [`message_bytes_ceiling`] at [`MAX_VALUE_BYTES`], the hard
/// cap `GetLimits` clamps every keyspace's value size to, so it is at least as
/// large as any per-keyspace ceiling a client could be told, and the store
/// never advertises a size it would then reject.
pub fn max_transport_message_bytes() -> usize {
    // The cast cannot lose data: the ceiling is a few megabytes and usize is
    // at least 32 bits on every target this builds for.
    message_bytes_ceiling(MAX_VALUE_BYTES as u64) as usize
}

/// The message ceiling the Admin surface uses, which is deliberately not the KV
/// ceiling.
///
/// A `DescribeCluster` or `ListKeyspaces` response grows with the shape of the
/// cluster — one entry per partition, each carrying boundary keys up to
/// [`MAX_KEY_BYTES`] — not with any keyspace's value or list limit. Sizing it
/// from the KV ceiling would cap a truthful description of a few hundred
/// partitions and refuse it with `OUT_OF_RANGE`, a worse failure than the DoS
/// the cap defends against: the store would be unable to describe itself. This
/// is a generous fixed bound instead, large enough for thousands of partitions
/// even at the maximum boundary-key size and far more with the modest
/// boundaries a real cluster has, and small enough to bound a decoder's memory.
/// A cluster that outgrows it needs a paginated Admin contract, not a bigger
/// number here.
pub fn max_admin_message_bytes() -> usize {
    64 * 1024 * 1024
}

/// How stale the storage figure a write is admitted against may be.
///
/// The number is the worker's own view — the sum over the partitions it owns
/// for the keyspace — not a control-plane round trip, because coupling every
/// write to a shared aggregate is exactly the cross-keyspace dependency
/// admission has to avoid. It is remeasured at most this often; between
/// remeasures a keyspace can overshoot its cap only by the writes admitted
/// inside one interval. One second keeps the per-write cost to a cached read
/// while bounding that overshoot to a interval's worth of traffic.
const STORAGE_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// Where a node's data goes.
///
/// The two locations are different layers rather than a style choice: the log
/// goes through `orbita_runtime::Disk`, which is rooted at the data directory
/// and takes relative paths, and the storage engine goes through this one
/// shared `ObjectStore`, addressed by the partition layout `orbita-format`
/// defines.
#[derive(Clone)]
pub(crate) struct DataLayout {
    pub store: Arc<dyn ObjectStore>,
    pub wal_root: String,
    /// How large a log segment grows before it is rolled, which is the unit a
    /// checkpoint removes and therefore how far a replica may fall behind and
    /// still be caught up from the owner's log.
    pub wal_segment_bytes: u64,
}

impl DataLayout {
    fn paths(&self, keyspace: KeyspaceId, partition: PartitionId) -> PartitionPaths {
        PartitionPaths {
            store: Arc::clone(&self.store),
            path: PartitionPath::new("", keyspace, partition),
            wal_dir: format!("{}/p{}", self.wal_root, partition.get()),
            wal_segment_bytes: self.wal_segment_bytes,
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
    /// The startup conditions this node reports. The node marks recovery and
    /// catch-up; the control loop above it marks the join.
    readiness: Arc<ReadinessGate>,
    accepting_writes: AtomicBool,
    /// A drain takes the write side after closing admission. Existing writes
    /// hold the read side through their acknowledgement, so the final progress
    /// report cannot race an acknowledged write.
    writes: tokio::sync::RwLock<()>,
    /// Per-keyspace storage and rate admission, isolated so one tenant's
    /// throttling never touches another's path. See [`crate::quota`].
    admission: Admission,
    /// The credential rule the client edge enforces, the first gate of the
    /// admission boundary. Held here, alongside the rate and storage state, so
    /// a request is authorized and storage-checked against a single keyspace
    /// resolution rather than in independently placed layers. A forwarded
    /// (node-to-node) request skips the credential check — it was already
    /// authenticated at the edge the client reached, which the peer transport
    /// does not carry — but it is still rate- and storage-checked on the node
    /// that serves it, so an internal hop cannot be used to dodge a quota.
    authenticator: Arc<Authenticator<R::Clock>>,
    /// When a request last made this node look again for a keyspace its map
    /// did not know. See [`KEYSPACE_MISS_REPAIR_INTERVAL`].
    keyspace_miss_repaired_at: AtomicU64,
}

/// How often a keyspace this node has never heard of may cost a control plane
/// round trip.
///
/// The map is a cache refreshed on a timer, so a keyspace created a moment ago
/// is absent from it for up to one interval — and `keyspace create` followed
/// by a write is two consecutive lines of the quickstart, well inside that
/// window. Looking again before answering "no such keyspace" is the same
/// repair a misrouted request already triggers.
///
/// It is bounded because the data plane must not become a way to generate
/// control plane load: without a bound, a client looping on a typo would put a
/// fetch per request onto the leader group. At this interval the worst case is
/// the traffic this node already sends on its own.
const KEYSPACE_MISS_REPAIR_INTERVAL: Duration = DEFAULT_CONTROL_POLL_INTERVAL;

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

/// What a Kv request needs to clear admission: the credential permission it is
/// authorized for, paired with the rate-limit direction it is charged against.
///
/// The two travel together so a request can never be authorized as one kind and
/// metered as the other — a read authorized with [`Permission::Read`] is always
/// charged against the read bucket, a write against the write bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Access {
    Read,
    Write,
}

impl Access {
    /// The credential permission the authorizer checks for this access.
    fn permission(self) -> Permission {
        match self {
            Access::Read => Permission::Read,
            Access::Write => Permission::Write,
        }
    }

    /// The rate bucket this access is charged against.
    fn direction(self) -> Direction {
        match self {
            Access::Read => Direction::Read,
            Access::Write => Direction::Write,
        }
    }
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
        readiness: Arc<ReadinessGate>,
        authenticator: Arc<Authenticator<R::Clock>>,
    ) -> Result<Arc<Self>> {
        let map = source.fetch().await?;
        let (bridge, applies) = ReplicaBridge::start(&runtime);
        let wal_service = WalService::new();
        // Registered before anything is opened, so the first batch a replica
        // receives cannot land without having been invalidated for.
        wal_service.observe(Arc::clone(&bridge) as Arc<dyn orbita_wal::ReplicaObserver>);
        wal_service.hydrate_with(Arc::clone(&bridge) as Arc<dyn orbita_wal::PartitionHydrator>);

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
            readiness,
            accepting_writes: AtomicBool::new(true),
            writes: tokio::sync::RwLock::new(()),
            admission: Admission::new(),
            authenticator,
            keyspace_miss_repaired_at: AtomicU64::new(0),
        });
        node.reconcile_hosts().await?;
        // Nothing is known to be stranded before a single heartbeat has gone
        // out, and a node held unready for a verdict it has not reached would
        // never start. The heartbeat corrects this within one interval.
        node.readiness.mark(ReadinessCondition::ReplicasRecoverable);
        // Opening a partition replays its write-ahead log to the trusted end,
        // so the initial reconcile succeeding means every log this node holds
        // is recovered. Marked here rather than inside `reconcile` because a
        // later reconcile reopens single partitions, which is catch-up moving
        // and not recovery happening again. `reconcile` marks catch-up itself.
        node.readiness.mark(ReadinessCondition::WalRecovered);

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

    #[must_use]
    pub(crate) fn id(&self) -> NodeId {
        self.node_id
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
        self.reconcile().await
    }

    /// Brings the set of open partitions in line with the map, and records how
    /// it went.
    ///
    /// The retry flag and the readiness condition are updated while the
    /// `hosts` write lock is still held. `refresh_map` runs concurrently, from
    /// the control loop, from misroute repair, and from the admin surface, and
    /// the lock is the only thing serializing the reconciles underneath them.
    /// Recording after the lock dropped would let a stale failure overwrite a
    /// fresher success: the node would sit unready with the retry flag saying
    /// there is nothing to retry, and nothing would ever re-mark it until the
    /// map version moved.
    async fn reconcile(&self) -> Result<()> {
        self.reconcile_inner(true).await
    }

    /// The local half of a reconcile: open and close hosts to match the map,
    /// and leave any catch-up to the next pass.
    ///
    /// This is what start-up uses. A node that has just recovered a log looks,
    /// to itself, like a node whose every replica is behind, because nothing
    /// has acknowledged anything since the process began. Doing that work here
    /// would put a round trip to every peer of every partition in front of the
    /// listener opening, and a peer that is down would put a timeout there
    /// instead. Whether this node can reach its peers is not what readiness
    /// means, and it is not worth a slow start to find out.
    async fn reconcile_hosts(&self) -> Result<()> {
        self.reconcile_inner(false).await
    }

    async fn reconcile_inner(&self, catch_up: bool) -> Result<()> {
        let map = self.map();
        let wanted: Vec<PartitionInfo> = map.held_by(self.node_id).cloned().collect();

        let mut hosts = self.hosts.write().await;
        let outcome = self.reconcile_locked(&mut hosts, wanted).await;
        self.unreconciled.store(outcome.is_err(), Ordering::Release);
        // Readiness follows the reconcile outcome both ways. A node holding a
        // partition it could not open is not ready, however long it has been
        // running, and a node that has since opened it is ready again.
        match &outcome {
            Ok(()) => self.readiness.mark(ReadinessCondition::PartitionsCaughtUp),
            Err(error) => {
                self.readiness.clear(ReadinessCondition::PartitionsCaughtUp);
                tracing::warn!(%error, "could not open every partition this node was given");
            }
        }
        drop(hosts);

        let complete = if catch_up {
            self.catch_up_owned().await
        } else {
            // Deferred rather than skipped. Leaving the flag raised is what
            // makes the very next poll do the work, instead of waiting for the
            // map to move, which on an idle partition it never does.
            self.owned_replicas_behind().await.is_empty()
        };
        if !complete {
            // Only ever raised here, never lowered, so a stale failure cannot
            // erase a fresher success the way clearing it could. The cost of
            // being wrong is one extra reconcile pass; the cost of not
            // retrying is a partition that stays on fewer copies than the map
            // promises until the map happens to move again, which on an idle
            // partition is never.
            self.unreconciled.store(true, Ordering::Release);
        }
        outcome
    }

    /// The owned partitions with at least one advertised replica that has not
    /// confirmed the committed prefix.
    async fn owned_replicas_behind(&self) -> Vec<Arc<PartitionHost<R>>> {
        self.hosts
            .read()
            .await
            .values()
            .filter(|host| host.is_owner() && !host.replicas_behind().is_empty())
            .cloned()
            .collect()
    }

    /// Carries every advertised replica that is behind up to the committed
    /// prefix, and reports whether every one of them got there.
    ///
    /// Run outside the `hosts` lock, because it talks to peers and every read
    /// of this node's routing table would queue behind it.
    ///
    /// Which partitions get a pass is decided from the per-replica
    /// acknowledgements the WAL already keeps rather than from having just
    /// seen the placement change. That distinction is the whole point. A
    /// replica set is installed before anybody is synchronized to it, so a
    /// pass driven by "the map moved" gets exactly one attempt: if the peer is
    /// unreachable for that attempt, the next reconcile sees an unchanged
    /// placement, calls itself finished, and the partition advertises a copy
    /// that holds nothing until something else moves the map. Driven by what
    /// the replicas have actually confirmed, a failed pass simply stays
    /// pending, and a healthy partition under load asks for nothing because
    /// its appends keep the acknowledgements current.
    ///
    /// Partitions with nothing behind are skipped, because a cluster in good
    /// health would otherwise pay a round trip per replica every time anything
    /// moved the map. A drain does not skip; see [`Node::prepare_handoff`].
    ///
    /// The partitions that do need a pass are worked concurrently, so a single
    /// unreachable peer costs one timeout rather than one per partition it
    /// happens to hold.
    pub(crate) async fn catch_up_owned(&self) -> bool {
        let behind = self.owned_replicas_behind().await;
        let passes: Vec<_> = behind.iter().map(|host| catch_up_one(host)).collect();
        join_all(passes).await.into_iter().all(|done| done)
    }

    /// The body of a reconcile, run under the `hosts` write lock the caller
    /// holds so the outcome it returns is the outcome that gets recorded.
    async fn reconcile_locked(
        &self,
        hosts: &mut HashMap<PartitionId, Arc<PartitionHost<R>>>,
        wanted: Vec<PartitionInfo>,
    ) -> Result<()> {
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
            let held = hosts.get(&info.id).cloned();
            let unchanged = held
                .as_ref()
                .is_some_and(|held| held.epoch() == info.epoch && held.is_owner() == owned_here);
            if unchanged {
                // The replica set is the one thing about a partition that can
                // move without the epoch moving, because placing replicas is
                // not a change of ownership. Adopting it here rather than
                // treating it as a reopen is what keeps an owner that was born
                // unreplicated from acknowledging writes to nobody for the rest
                // of its life. See `PartitionHost::set_replicas`.
                //
                // Nothing is scheduled from this branch. Adopting the set is
                // what makes the new peers show up as behind, and the catch-up
                // pass after the lock works from that rather than from having
                // witnessed the change, so a peer that could not be reached on
                // the first attempt is still owed one on the next pass.
                if let Some(held) = held {
                    if held.set_replicas(&info.replicas) {
                        tracing::info!(
                            partition = info.id.get(),
                            replicas = ?info.replicas,
                            "adopted a replica set placed after the partition opened"
                        );
                    }
                }
                continue;
            }
            // The old incarnation is closed before the new one opens, so that
            // two incarnations never hold the same partition at once: the old
            // one's applier retires with it, and the new one's recovery sees
            // a store nothing else is writing. Opening first and replacing
            // after would break every promotion, which is the one time this
            // path matters.
            hosts.remove(&info.id);
            self.wal_service.unregister(info.id);
            self.bridge.unregister(info.id);

            let paths = self.layout.paths(info.keyspace, info.id);
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

    /// Resolves a keyspace by name, looking again before saying no.
    ///
    /// A name absent from the cached map is a cache miss rather than an
    /// answer, and this node already treats a misroute that way. Rate limited
    /// by [`KEYSPACE_MISS_REPAIR_INTERVAL`].
    async fn keyspace(&self, name: &str) -> Result<KeyspaceInfo> {
        validate::keyspace_name(name)?;
        if let Some(info) = self.map().keyspace_by_name(name).cloned() {
            return Ok(info);
        }
        self.repair_unknown_keyspace().await;
        self.map()
            .keyspace_by_name(name)
            .cloned()
            .ok_or(Error::KeyspaceNotFound)
    }

    async fn repair_unknown_keyspace(&self) {
        let now = self.runtime.clock().monotonic_nanos();
        let last = self.keyspace_miss_repaired_at.load(Ordering::Relaxed);
        if now.saturating_sub(last) < KEYSPACE_MISS_REPAIR_INTERVAL.as_nanos() as u64 {
            return;
        }
        // Losing the exchange means another request is already refetching, and
        // waiting for it would only turn one stale answer into two slow ones.
        if self
            .keyspace_miss_repaired_at
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        if let Err(error) = self.refresh_map().await {
            tracing::debug!(%error, "could not refresh the partition map for an unknown keyspace");
        }
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

    /// Forwards a request to the partition's owner over the peer transport.
    ///
    /// Spanned because this is the intra-cluster hop whose cost is otherwise
    /// invisible: a client sees one latency, but it is split across two nodes,
    /// and only a span that brackets the peer call attributes the difference to
    /// the hop rather than to the owner.
    #[tracing::instrument(name = "kv.forward", skip(self, request), fields(peer = to.get(), method))]
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

    /// The partition a key routes to right now, for a metric or span label.
    ///
    /// A read of the cached map, nothing more: it names where the request is
    /// headed, which is the label an operator groups latency by, without paying
    /// for the routing decision a second time. `None` when the keyspace or the
    /// key does not resolve, in which case the request is about to error and
    /// there is nothing to attribute latency to.
    fn label_partition(&self, keyspace: &str, key: &[u8]) -> Option<PartitionId> {
        let map = self.map();
        let id = map.keyspace_by_name(keyspace)?.id;
        map.lookup(id, key).map(|info| info.id)
    }

    /// Records a request's latency against the partition it routed to.
    ///
    /// Skipped when the partition did not resolve, because a latency sample no
    /// partition can own is a label an operator cannot act on.
    ///
    /// Skipped on a forwarded request, because the ingress node already timed
    /// the same client operation end to end. Recording it again on the owner
    /// would double every forwarded op's count and blend end-to-end latency
    /// with owner-local latency under one label; the owner's work is already
    /// visible through its spans.
    fn record_latency(
        &self,
        keyspace: &str,
        partition: Option<PartitionId>,
        operation: metrics::Operation,
        started_nanos: u64,
        forwarded: bool,
    ) {
        if forwarded {
            return;
        }
        let Some(partition) = partition else { return };
        let elapsed = self
            .runtime
            .clock()
            .monotonic_nanos()
            .saturating_sub(started_nanos);
        metrics::record_request(
            keyspace,
            partition,
            operation,
            elapsed as f64 / 1_000_000_000.0,
        );
    }

    #[tracing::instrument(
        name = "kv.get",
        skip_all,
        fields(keyspace = %request.keyspace, forwarded, partition = tracing::field::Empty)
    )]
    pub(crate) async fn get(
        &self,
        request: GetRequest,
        forwarded: bool,
        credential: Option<&str>,
    ) -> Result<GetResponse> {
        // Timed from before admission so the sample covers the whole of the
        // work this node does for the request, auth and routing included. A
        // request admit rejects returns below without a sample: nothing was
        // served, so there is no work latency to attribute.
        let started = self.runtime.clock().monotonic_nanos();
        let keyspace = self
            .admit(credential, &request.keyspace, Access::Read, forwarded)
            .await?;
        let partition = self.label_partition(&request.keyspace, &request.key);
        if let Some(partition) = partition {
            tracing::Span::current().record("partition", partition.get());
        }
        let result = match self.try_get(&request, forwarded, &keyspace).await {
            Err(error) if self.should_repair(&error, forwarded) => {
                self.repair().await;
                self.try_get(&request, forwarded, &keyspace).await
            }
            other => other,
        };
        self.record_latency(
            &request.keyspace,
            partition,
            metrics::Operation::Get,
            started,
            forwarded,
        );
        result
    }

    async fn try_get(
        &self,
        request: &GetRequest,
        forwarded: bool,
        keyspace: &KeyspaceInfo,
    ) -> Result<GetResponse> {
        validate::key(&request.key)?;

        let (owner, partition) = match self.hop(keyspace.id, &request.key, Purpose::Read).await? {
            Hop::Local(host, owner, partition) => match host.read(&request.key).await? {
                Read::Served(record) => {
                    // Charged on the node that served the read — owner or
                    // serving replica — so reads fanned through many edges
                    // cannot each win a full bucket. See [`Self::charge_rate`].
                    self.charge_rate(keyspace, Access::Read)?;
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

    #[tracing::instrument(
        name = "kv.set",
        skip_all,
        fields(keyspace = %request.keyspace, forwarded, partition = tracing::field::Empty)
    )]
    pub(crate) async fn set(
        &self,
        request: SetRequest,
        forwarded: bool,
        credential: Option<&str>,
    ) -> Result<SetResponse> {
        let started = self.runtime.clock().monotonic_nanos();
        let keyspace = self
            .admit(credential, &request.keyspace, Access::Write, forwarded)
            .await?;
        let partition = self.label_partition(&request.keyspace, &request.key);
        if let Some(partition) = partition {
            tracing::Span::current().record("partition", partition.get());
        }
        let _permit = self.write_permit().await?;
        let result = match self.try_set(&request, forwarded, &keyspace).await {
            Err(error) if self.should_repair(&error, forwarded) => {
                self.repair().await;
                self.try_set(&request, forwarded, &keyspace).await
            }
            other => other,
        };
        self.record_latency(
            &request.keyspace,
            partition,
            metrics::Operation::Set,
            started,
            forwarded,
        );
        result
    }

    async fn try_set(
        &self,
        request: &SetRequest,
        forwarded: bool,
        keyspace: &KeyspaceInfo,
    ) -> Result<SetResponse> {
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
                // The rate is charged here, on the owner every write converges
                // on, and forwarded writes are charged too — see
                // [`Self::charge_rate`].
                self.charge_rate(keyspace, Access::Write)?;
                // Storage is enforced where the data lands. The figure is this
                // owner's own footprint for the keyspace, compared against its
                // share of the cap so N owners cannot each grow to the full cap.
                // The reserved size is the record's whole footprint — key, value
                // and framing — the same accounting storage will add, not the
                // value alone, so a keyed write is never admitted as if it were
                // free below the framing overhead.
                if let Some(cap) = keyspace.max_storage_bytes {
                    let footprint =
                        orbita_storage::record_footprint(request.key.len(), request.value.len());
                    let share = self.storage_share(keyspace.id, cap);
                    self.admit_storage(keyspace.id, share, footprint).await?;
                }
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

    #[tracing::instrument(
        name = "kv.delete",
        skip_all,
        fields(keyspace = %request.keyspace, forwarded, partition = tracing::field::Empty)
    )]
    pub(crate) async fn delete(
        &self,
        request: DeleteRequest,
        forwarded: bool,
        credential: Option<&str>,
    ) -> Result<DeleteResponse> {
        let started = self.runtime.clock().monotonic_nanos();
        let keyspace = self
            .admit(credential, &request.keyspace, Access::Write, forwarded)
            .await?;
        let partition = self.label_partition(&request.keyspace, &request.key);
        if let Some(partition) = partition {
            tracing::Span::current().record("partition", partition.get());
        }
        let _permit = self.write_permit().await?;
        let result = match self.try_delete(&request, forwarded, &keyspace).await {
            Err(error) if self.should_repair(&error, forwarded) => {
                self.repair().await;
                self.try_delete(&request, forwarded, &keyspace).await
            }
            other => other,
        };
        self.record_latency(
            &request.keyspace,
            partition,
            metrics::Operation::Delete,
            started,
            forwarded,
        );
        result
    }

    async fn try_delete(
        &self,
        request: &DeleteRequest,
        forwarded: bool,
        keyspace: &KeyspaceInfo,
    ) -> Result<DeleteResponse> {
        validate::key(&request.key)?;

        match self.hop(keyspace.id, &request.key, Purpose::Write).await? {
            Hop::Local(host, ..) => {
                // A delete is a write for rate purposes and converges on the
                // owner, charged here so forwarded deletes are metered too.
                self.charge_rate(keyspace, Access::Write)?;
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

    /// Reports the sizes this cluster accepts, for the named keyspace or for
    /// the whole cluster when none is named.
    ///
    /// The number this returns as `max_message_bytes` is the same one
    /// [`message_bytes_ceiling`] produces, so the ceiling a client is told and
    /// the ceiling the transport is sized to (see
    /// [`max_transport_message_bytes`]) come from one place and cannot drift.
    ///
    /// The cluster-wide answer is the largest any keyspace allows rather than
    /// the smallest, because a client asking without naming a keyspace is
    /// sizing a connection it intends to reuse, and a connection has to be big
    /// enough for the largest thing that will cross it.
    pub(crate) async fn limits(&self, keyspace: &str) -> Result<GetLimitsResponse> {
        let map = self.map();

        let max_value_bytes = if keyspace.is_empty() {
            map.keyspaces()
                .filter_map(|k| k.max_value_bytes)
                .max()
                .unwrap_or(MAX_VALUE_BYTES as u64)
        } else {
            self.keyspace(keyspace)
                .await?
                .max_value_bytes
                .unwrap_or(MAX_VALUE_BYTES as u64)
        }
        .min(MAX_VALUE_BYTES as u64);

        Ok(GetLimitsResponse {
            max_key_bytes: MAX_KEY_BYTES as u32,
            max_value_bytes: max_value_bytes as u32,
            max_list_entries: MAX_LIST_LIMIT,
            max_list_bytes: MAX_LIST_BYTES,
            // Big enough for whichever direction is larger, a single maximum
            // value or a full list page, plus room for the key, the keyspace
            // name, and framing. Reporting one number means a client sets its
            // channel once and never has to do this arithmetic or get it
            // slightly wrong.
            max_message_bytes: message_bytes_ceiling(max_value_bytes),
        })
    }

    #[tracing::instrument(
        name = "kv.list",
        skip_all,
        fields(keyspace = %request.keyspace, forwarded, partition = tracing::field::Empty)
    )]
    pub(crate) async fn list(
        &self,
        request: ListRequest,
        forwarded: bool,
        credential: Option<&str>,
    ) -> Result<ListResponse> {
        let started = self.runtime.clock().monotonic_nanos();
        let keyspace = self
            .admit(credential, &request.keyspace, Access::Read, forwarded)
            .await?;
        // A scan is routed by where it resumes, so telemetry follows the cursor
        // too: once a scan crosses a partition boundary the later pages belong
        // to the partition they resume into, not the one the prefix started in.
        // A malformed cursor falls back to the prefix; `try_list` decodes it
        // again and turns the failure into the client's error.
        let route_key = Cursor::decode(&request.cursor, &request.prefix)
            .map(|resume| resume.route_key)
            .unwrap_or_else(|_| Bytes::copy_from_slice(&request.prefix));
        let partition = self.label_partition(&request.keyspace, &route_key);
        if let Some(partition) = partition {
            tracing::Span::current().record("partition", partition.get());
        }
        let result = match self.try_list(&request, forwarded, &keyspace).await {
            Err(error) if self.should_repair(&error, forwarded) => {
                self.repair().await;
                self.try_list(&request, forwarded, &keyspace).await
            }
            other => other,
        };
        self.record_latency(
            &request.keyspace,
            partition,
            metrics::Operation::List,
            started,
            forwarded,
        );
        result
    }

    async fn try_list(
        &self,
        request: &ListRequest,
        forwarded: bool,
        keyspace: &KeyspaceInfo,
    ) -> Result<ListResponse> {
        validate::prefix(&request.prefix)?;
        let limit = validate::list_limit(request.limit);

        let resume = Cursor::decode(&request.cursor, &request.prefix)?;
        match self
            .hop(keyspace.id, &resume.route_key, Purpose::Scan)
            .await?
        {
            Hop::Local(host, ..) => {
                // A list is a read; charged on the serving node (owner or
                // replica) so a scan-heavy client cannot dodge the read rate by
                // spreading across edges.
                self.charge_rate(keyspace, Access::Read)?;
                // Bound the page by encoded bytes as well as by count. The byte
                // budget is `MAX_LIST_BYTES`, the same figure the transport
                // ceiling is built from (see [`message_bytes_ceiling`]), so a
                // full page always fits under the ceiling the client was told,
                // and the cursor the scan returns pages the remainder. Values
                // count toward the budget only when the caller asked for them,
                // so a keys-only page is not truncated for bytes it will not
                // send.
                let budget = ScanBudget {
                    max_entries: limit,
                    max_bytes: MAX_LIST_BYTES,
                    include_values: request.include_values,
                };
                let page = host
                    .scan(&request.prefix, resume.inner.as_deref(), budget)
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
    async fn accept_lease(&self, grant: &proxy::LeaseGrant) -> Result<proxy::LeaseReply> {
        let host = self.hosts.read().await.get(&grant.partition).cloned();
        match host {
            Some(host) => Ok(host.accept_lease(grant).await),
            // A partition this node does not hold cannot serve a read from it
            // either, so refusing is the whole answer, and it has no log
            // position to report for one it is not holding.
            None => Ok(proxy::LeaseReply {
                accepted: false,
                durable: None,
            }),
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

    /// Every replica of a partition this node owns that has fallen past what
    /// this node's log still holds.
    ///
    /// Sorted by partition and then node, so a repeated report of an unchanged
    /// state is the same bytes. Empty is the healthy answer; anything else is
    /// a replica that cannot be recovered until hydration exists (issue #17).
    pub(crate) async fn replicas_beyond_retention(
        &self,
    ) -> Vec<(PartitionId, orbita_wal::BeyondRetention)> {
        let hosts: Vec<Arc<PartitionHost<R>>> = self.hosts.read().await.values().cloned().collect();
        let mut fallen: Vec<(PartitionId, orbita_wal::BeyondRetention)> = hosts
            .iter()
            .flat_map(|host| {
                host.replicas_beyond_retention()
                    .into_iter()
                    .map(move |one| (host.id(), one))
            })
            .collect();
        fallen.sort_unstable_by_key(|(partition, one)| (partition.get(), one.node.get()));
        fallen
    }

    /// The oldest Lamport this node's log for `partition` still holds, or
    /// `None` if it does not hold the partition.
    ///
    /// The retention floor. Asking for it is how a scenario establishes that a
    /// gap is genuinely past the log rather than merely large, which is what
    /// separates a replica hydration has to rescue from one an ordinary
    /// catch-up would have reached anyway.
    #[cfg(test)]
    pub(crate) async fn retained_from(
        &self,
        partition: PartitionId,
    ) -> Option<orbita_core::Lamport> {
        let host = self.hosts.read().await.get(&partition).cloned()?;
        Some(host.retained_from().await)
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
                // A storage layer that will not answer has not measured
                // anything, so nothing is claimed. Reporting zero would tell
                // the leader group this partition holds no index, which is
                // the one answer that is certainly wrong.
                index_bytes: host.index_bytes().await.ok(),
                // Only an owner has a committed prefix. A replica reports
                // none rather than borrowing its own durable position, which
                // is a different watermark and would let a describe present
                // one replica's disk as the partition's committed state.
                committed_lamport: host.committed_prefix(),
            });
        }
        // Sorted so that two reports of the same state are the same bytes,
        // which keeps a heartbeat from looking like a change.
        progress.sort_unstable_by_key(|p| p.partition.get());
        progress
    }

    /// The client-edge admission boundary a Kv request crosses, and the single
    /// place its keyspace is resolved.
    ///
    /// In order:
    ///   1. credential authorization — `Unauthenticated` / `PermissionDenied`
    ///   2. keyspace resolution (once, reused by the storage cap and routing on
    ///      the write path, and by the rate charge on the serving node)
    ///
    /// Credential first is deliberate: an unauthenticated caller must never be
    /// routed or measured against a quota, so the deny lands before any
    /// tenant-attributable work. Authorization is judged on the request's
    /// keyspace *name*, before resolution, so a caller who cannot authenticate
    /// learns only that it is `Unauthenticated`, not whether the keyspace
    /// exists.
    ///
    /// The rate limit is deliberately *not* charged here. It is charged on the
    /// node that ends up serving the request — the partition owner for a write,
    /// the owner or a serving replica for a read — see [`Self::charge_rate`].
    /// Metering at the edge that received the request let a client multiply its
    /// budget by fanning one hot partition's traffic through every worker: each
    /// edge held its own token bucket and forwarded the load onto the same
    /// owner, so a keyspace capped at N/s admitted N × worker_count. Charging at
    /// the point of service makes the owner the single meter every path
    /// converges on.
    ///
    /// A forwarded (node-to-node) request skips the credential check: it was
    /// already authenticated at the edge the client reached, and the peer
    /// transport carries no client credential. It still resolves the keyspace
    /// here so the serving path shares this resolution, and it is still rate
    /// charged where it is served, precisely so a forwarded request cannot dodge
    /// the meter.
    async fn admit(
        &self,
        credential: Option<&str>,
        keyspace: &str,
        access: Access,
        forwarded: bool,
    ) -> Result<KeyspaceInfo> {
        if forwarded {
            return self.keyspace(keyspace).await;
        }
        self.authenticator
            .authorize(credential, keyspace, access.permission())?;
        self.keyspace(keyspace).await
    }

    /// Charges one request this node is about to serve against the keyspace's
    /// per-direction rate limit.
    ///
    /// This runs on the node that serves the request, not the edge that received
    /// it, so every path for a partition converges on one meter: all writes on
    /// the owner, all reads on the owner or a serving replica. A forwarded
    /// request is charged here too — it is not exempt — which is what stops a
    /// client fanning a partition's load through many edges to win a full bucket
    /// from each.
    ///
    /// Because a keyspace's configured rate is cluster-wide but each serving
    /// node meters on its own, the rate is split into a per-node share
    /// ([`Self::rate_share`]) so the sum across every node serving the keyspace
    /// cannot exceed the configured total. An unset rate is unlimited; a rate of
    /// zero admits nothing. An `Err` here is a `ResourceExhausted` whose message
    /// names the direction so a client can tell a rate refusal from a storage
    /// one.
    fn charge_rate(&self, keyspace: &KeyspaceInfo, access: Access) -> Result<()> {
        let direction = access.direction();
        let rate = match direction {
            Direction::Read => keyspace.max_reads_per_second,
            Direction::Write => keyspace.max_writes_per_second,
        };
        let Some(limit) = rate else {
            return Ok(());
        };
        let share = self.rate_share(keyspace.id, access, limit);
        let now = self.runtime.clock().monotonic_nanos();
        if self
            .admission
            .admit_rate(keyspace.id, direction, Some(share), now)
        {
            return Ok(());
        }
        let name = &keyspace.name;
        Err(Error::QuotaExceeded(match direction {
            Direction::Read => {
                format!("read rate limit of {limit} reads/s for keyspace {name}")
            }
            Direction::Write => {
                format!("write rate limit of {limit} writes/s for keyspace {name}")
            }
        }))
    }

    /// The slice of a keyspace-wide rate this one node may admit on its own, so
    /// the sum across every node serving the keyspace stays within the
    /// configured limit.
    ///
    /// It is `limit / serving_node_count`, floored — the floor keeps the sum at
    /// or below the limit rather than above it. Two exceptions keep the number
    /// honest at the edges: a limit of zero stays zero (a keyspace configured to
    /// admit nothing must not be handed a token by division), and a positive
    /// limit never floors to zero, because a keyspace with a real rate must not
    /// be silenced just because it is spread across more nodes than it has
    /// tokens. That last clamp is the one documented slack: when a positive
    /// `limit` is smaller than the serving-node count, each node admits one and
    /// the aggregate can exceed `limit` by at most `serving_node_count - limit`
    /// per second. The count is read from the same cached map routing uses, so
    /// during a placement change nodes can briefly disagree on it; the overshoot
    /// that disagreement can cause is bounded by the same slack and lasts only
    /// until the map converges.
    fn rate_share(&self, keyspace: KeyspaceId, access: Access, limit: u32) -> u32 {
        if limit == 0 {
            return 0;
        }
        (limit / self.serving_node_count(keyspace, access)).max(1)
    }

    /// The number of distinct nodes that can serve `access` for `keyspace`, per
    /// this worker's cached map: partition owners for a write, owners and
    /// replicas for a read. At least one.
    ///
    /// This is the divisor that turns a cluster-wide quota into a per-node
    /// share. Reading it from the map the worker already holds — rather than a
    /// control-plane round trip — is what keeps quota enforcement off the
    /// control plane's back and inside the keyspace, respecting the isolation
    /// requirement that no admission decision take a dependency spanning
    /// keyspaces.
    fn serving_node_count(&self, keyspace: KeyspaceId, access: Access) -> u32 {
        use std::collections::HashSet;
        let map = self.map();
        let mut nodes: HashSet<NodeId> = HashSet::new();
        for partition in map.partitions().filter(|p| p.keyspace == keyspace) {
            if let Some(owner) = partition.owner {
                nodes.insert(owner);
            }
            if access == Access::Read {
                nodes.extend(partition.replicas.iter().copied());
            }
        }
        u32::try_from(nodes.len()).unwrap_or(u32::MAX).max(1)
    }

    /// Refuses a write that would carry this owner past its share of the
    /// keyspace's storage cap.
    ///
    /// The cap is keyspace-wide, but each owner measures only the partitions it
    /// holds ([`Self::owned_storage_bytes`]) — a worker has no cheap, current
    /// view of what its peers store, and the control plane's keyspace-wide
    /// aggregate is not reachable here without changing the frozen
    /// `KeyspaceInfo`. Counting only local bytes against the *whole* cap would
    /// let every owner independently grow its subset to the full cap, so a
    /// keyspace split across N owners could store N times its limit. To bound
    /// the aggregate the caller passes this owner's *share* of the cap
    /// ([`Self::storage_share`]), `cap / owner_count`, so the sum across owners
    /// cannot exceed the cap.
    ///
    /// The figure compared against is this owner's cached local view, at most
    /// [`STORAGE_SAMPLE_INTERVAL`] stale, plus the footprint of the write being
    /// admitted. Charging the incoming write means one larger than the remaining
    /// room is refused up front rather than only being noticed by the next
    /// sample. The message names storage so a client can tell it from a rate
    /// refusal.
    ///
    /// The residual: the share is floored, and the local view is sampled, so
    /// within one sample interval an owner can overshoot its share by the writes
    /// admitted in that window, and skew between owners (one holding more than
    /// its share after a split) is under-admitted rather than over — the safe
    /// direction, since the aggregate stays at or under the cap.
    async fn admit_storage(&self, keyspace: KeyspaceId, share: u64, incoming: u64) -> Result<()> {
        let now = self.runtime.clock().monotonic_nanos();
        let freshness = STORAGE_SAMPLE_INTERVAL.as_nanos().min(u128::from(u64::MAX)) as u64;
        let held = match self.admission.storage_sample(keyspace, now, freshness) {
            Some(bytes) => bytes,
            None => {
                let measured = self.owned_storage_bytes(keyspace).await;
                self.admission.record_storage(keyspace, measured, now);
                measured
            }
        };
        if held.saturating_add(incoming) > share {
            return Err(Error::QuotaExceeded(format!(
                "storage cap reached for keyspace {}: this owner's share is {share} bytes ({held} held)",
                keyspace.get()
            )));
        }
        Ok(())
    }

    /// This owner's slice of a keyspace-wide storage cap: `cap / owner_count`,
    /// so the sum of every owner's share cannot exceed the cap.
    ///
    /// The owner count is read from the same cached map routing uses, so no
    /// control-plane round trip and no cross-keyspace dependency stands in the
    /// write path.
    fn storage_share(&self, keyspace: KeyspaceId, cap: u64) -> u64 {
        cap / u64::from(self.serving_node_count(keyspace, Access::Write))
    }

    /// The bytes this worker holds for a keyspace: the sum over the partitions
    /// it currently owns for it.
    ///
    /// A replica's copy is not counted, because the cap is on stored data and
    /// counting every replica would multiply a keyspace's footprint by its
    /// replication factor. This is only this owner's local subset; the
    /// keyspace-wide bound comes from comparing it against a per-owner share,
    /// not from summing peers. See [`Self::admit_storage`].
    async fn owned_storage_bytes(&self, keyspace: KeyspaceId) -> u64 {
        let ids: Vec<PartitionId> = self
            .map()
            .partitions()
            .filter(|p| p.keyspace == keyspace && p.owner == Some(self.node_id))
            .map(|p| p.id)
            .collect();
        let hosts = self.hosts.read().await;
        let mut total = 0u64;
        for id in ids {
            if let Some(host) = hosts.get(&id) {
                total = total.saturating_add(host.size_bytes().await.unwrap_or(0));
            }
        }
        total
    }

    async fn write_permit(&self) -> Result<tokio::sync::RwLockReadGuard<'_, ()>> {
        if !self.accepting_writes.load(Ordering::Acquire) {
            return Err(Error::Unavailable("the node is draining".into()));
        }
        let permit = self.writes.read().await;
        if !self.accepting_writes.load(Ordering::Acquire) {
            return Err(Error::Unavailable("the node is draining".into()));
        }
        Ok(permit)
    }

    /// Closes write admission and waits for every write already admitted to
    /// resolve. Reads continue while the returned guard is held.
    pub(crate) async fn begin_draining(&self) -> tokio::sync::RwLockWriteGuard<'_, ()> {
        self.accepting_writes.store(false, Ordering::Release);
        self.writes.write().await
    }

    /// Makes every owned partition handable: gives up the tail no client was
    /// told about, then carries the replicas to what is left.
    ///
    /// This is what a drain calls on each pass, and it is two steps because
    /// the handoff has two conditions that pull against each other. The
    /// control plane will not give a partition to a replica short of the
    /// owner's advertised position, and a catch-up may not carry a replica
    /// past the committed prefix. An owner sitting on writes that reached its
    /// disk alone satisfies neither: it advertises a number no replica is
    /// allowed to reach.
    ///
    /// Quiescing settles that in the only direction that is honest. Write
    /// admission is already closed and every admitted write has resolved, so
    /// the gap is exactly the writes whose clients were told `Unavailable`,
    /// and this node is the last one that still knows they failed. It drops
    /// them, and what it then advertises is the committed prefix, which is
    /// precisely what a catch-up can deliver.
    ///
    /// Called on every pass rather than once, because a partition can arrive
    /// here between passes: quiescing an already quiesced log is free, and
    /// catching up an already current replica is one round trip that carries
    /// nothing.
    pub(crate) async fn prepare_handoff(&self) {
        let hosts: Vec<Arc<PartitionHost<R>>> = self
            .hosts
            .read()
            .await
            .values()
            .filter(|host| host.is_owner())
            .cloned()
            .collect();
        for host in &hosts {
            if let Err(error) = host.quiesce().await {
                tracing::warn!(
                    partition = host.id().get(),
                    %error,
                    "could not give up the uncommitted tail before handing off"
                );
            }
        }
        let passes: Vec<_> = hosts.iter().map(|host| catch_up_one(host)).collect();
        join_all(passes).await;
    }

    pub(crate) fn owned_partition_count(&self) -> usize {
        self.map()
            .partitions()
            .filter(|partition| partition.owner == Some(self.node_id))
            .count()
    }

    /// How often the lease heartbeat has to run for this node's leases to stay
    /// live.
    pub(crate) fn lease_interval(&self) -> Duration {
        self.lease.heartbeat_interval()
    }

    pub(crate) fn lease_drain(&self) -> Duration {
        self.lease.duration + self.lease.margin
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
        for host in &hosts {
            host.renew_leases().await;
        }
        // The heartbeat is also how an owner learns where each replica's log
        // ends, so this is the moment its verdict can have changed, and the
        // moment replication lag and storage size are freshest to report.
        self.observe_owned_metrics(&hosts).await;
        self.report_replica_recoverability().await;
    }

    /// Emits the per-partition WAL-lag and storage signals for every partition
    /// this node owns.
    ///
    /// Driven from the lease heartbeat rather than a timer of its own, for the
    /// same reason [`Node::report_replica_recoverability`] is: the heartbeat is
    /// what refreshes an owner's picture of its replicas, so reading the lag
    /// off any other clock would only add a window where the metric disagreed
    /// with the readiness the same evidence drives. The keyspace name is
    /// resolved from the cached map so the signal is labelled the way an
    /// operator groups it, and a partition whose keyspace has gone is skipped
    /// rather than emitted under an empty label.
    async fn observe_owned_metrics(&self, hosts: &[Arc<PartitionHost<R>>]) {
        let map = self.map();
        let mut owned = Vec::with_capacity(hosts.len());
        for host in hosts {
            let Some(info) = map.partition(host.id()) else {
                continue;
            };
            let Some(keyspace) = map.keyspace(info.keyspace) else {
                continue;
            };
            owned.push(metrics::OwnedMetric {
                keyspace: keyspace.name.as_str().to_owned(),
                partition: host.id(),
                replication_lag: host.replication_lag(),
                storage_bytes: host.size_bytes().await.ok(),
            });
        }
        // Republished wholesale: the observable gauges report exactly this set,
        // so a partition this node no longer owns drops out of the export by
        // being absent here rather than by anyone retiring its series.
        metrics::publish_owned(owned);
    }

    /// Moves the durability half of readiness to match what the owners here
    /// have established about their replicas.
    ///
    /// Called from the lease heartbeat rather than a timer of its own, because
    /// that heartbeat is what produces the evidence and running the two on
    /// different clocks would only add a window where the gate disagreed with
    /// the thing it reports.
    async fn report_replica_recoverability(&self) {
        let fallen = self.replicas_beyond_retention().await;
        if fallen.is_empty() {
            self.readiness.mark(ReadinessCondition::ReplicasRecoverable);
            return;
        }
        self.readiness
            .clear(ReadinessCondition::ReplicasRecoverable);
        tracing::warn!(
            stranded = fallen.len(),
            "this node owns a partition whose replica cannot be caught up from its log"
        );
    }

    /// Flushes every partition this node currently owns once.
    ///
    /// One pass rather than an internal timer keeps the same production code
    /// directly drivable under deterministic simulation.
    pub(crate) async fn flush_owned(&self) {
        let hosts: Vec<Arc<PartitionHost<R>>> = self
            .hosts
            .read()
            .await
            .values()
            .filter(|host| host.is_owner())
            .cloned()
            .collect();
        for host in hosts {
            if let Err(error) = host.flush().await {
                tracing::warn!(
                    partition = host.id().get(),
                    %error,
                    "periodic flush failed; the WAL remains replayable"
                );
            }
        }
    }

    /// Sweeps orphaned objects from every partition this node currently owns.
    ///
    /// The backstop for objects a failed compaction or an abandoned commit
    /// stranded: without a sweep they accumulate in the bucket forever. Run on
    /// its own slow cadence rather than folded into the flush loop, because the
    /// sweep lists a whole partition prefix and so should run far less often
    /// than a flush. One pass rather than an internal timer keeps the same
    /// production code drivable a step at a time under deterministic simulation.
    ///
    /// A failed sweep is logged and skipped, not retried here: an orphan that
    /// survives one pass is reclaimed on the next, and a store that is refusing
    /// deletes has a louder problem than leaked space.
    /// Prepares durable child storage for every split this node holds the
    /// parent of, and returns the parents it has fully prepared so the caller
    /// can acknowledge them.
    ///
    /// This is the worker's half of the ADR 0009 split. The owner quiesces the
    /// parent and publishes both child manifests over its segments with no
    /// copy; a replica has no writes to quiesce and reaches the same manifests
    /// through the shared bucket. A parent is reported prepared only once both
    /// child manifests are durable, which is the real acknowledgement the
    /// leader's completion waits on — never a mere observation that a map moved.
    ///
    /// A split that is no longer pending — because a required holder died and
    /// the leader aborted it — is detected by its parent being absent from the
    /// intents, and any parent this node had quiesced for it has its writes
    /// reopened here rather than being left stuck closed.
    pub(crate) async fn prepare_pending_splits(
        &self,
        intents: &[WireSplitIntent],
    ) -> Vec<PartitionId> {
        let map = self.map();
        let pending: HashSet<PartitionId> = intents.iter().map(|intent| intent.parent).collect();

        // Reopen writes on any partition we quiesced for a split that is no
        // longer pending. A completed split retires the parent instead, so this
        // only fires on an abort.
        for host in self.hosts.read().await.values() {
            if host.is_owner() && !host.is_admitting_writes() && !pending.contains(&host.id()) {
                host.resume_write_admission();
                tracing::info!(
                    partition = host.id().get(),
                    "reopened writes; the split was abandoned before it completed"
                );
            }
        }

        let mut prepared = Vec::new();
        for intent in intents {
            let Some(parent) = map.partition(intent.parent) else {
                continue;
            };
            let is_owner = parent.owner == Some(self.node_id);
            let is_replica = parent.replicas.contains(&self.node_id);
            if !is_owner && !is_replica {
                continue;
            }
            let Some((low_range, high_range)) = parent.range.clone().split_at(intent.at.clone())
            else {
                // An impossible boundary the leader will never complete against.
                continue;
            };
            let child_epoch = parent.epoch.next();

            if is_owner {
                let host = self.hosts.read().await.get(&intent.parent).cloned();
                let Some(host) = host else { continue };
                let specs = [
                    ChildSpec {
                        id: intent.lower,
                        epoch: child_epoch,
                        range: low_range,
                    },
                    ChildSpec {
                        id: intent.upper,
                        epoch: child_epoch,
                        range: high_range,
                    },
                ];
                if let Err(error) = host.quiesce_and_prepare_children(&specs).await {
                    tracing::warn!(
                        partition = intent.parent.get(),
                        %error,
                        "could not prepare split children; will retry"
                    );
                    continue;
                }
            }

            // Acknowledge only once both child manifests are durable — the
            // owner just published them, a replica waits for that publish.
            if self
                .child_manifests_exist(parent.keyspace, intent.lower, intent.upper)
                .await
            {
                prepared.push(intent.parent);
            }
        }
        prepared
    }

    /// The host for one partition this node holds, for a test that drives the
    /// split's data-plane steps directly.
    #[cfg(test)]
    pub(crate) async fn host(&self, id: PartitionId) -> Option<Arc<PartitionHost<R>>> {
        self.hosts.read().await.get(&id).cloned()
    }

    /// Whether both of a split's child manifests are durably published in the
    /// shared bucket, which is what makes it safe to acknowledge preparation.
    async fn child_manifests_exist(
        &self,
        keyspace: KeyspaceId,
        lower: PartitionId,
        upper: PartitionId,
    ) -> bool {
        for id in [lower, upper] {
            let paths = self.layout.paths(keyspace, id);
            match load_manifest(paths.store.as_ref(), &paths.path).await {
                Ok(Some(_)) => {}
                _ => return false,
            }
        }
        true
    }

    pub(crate) async fn sweep_owned(&self, grace_millis: u64, skew_millis: u64, dry_run: bool) {
        let hosts: Vec<Arc<PartitionHost<R>>> = self.hosts.read().await.values().cloned().collect();

        // Which of one partition's objects a sibling still references in place,
        // per ADR 0009. Gathered across every partition this node holds a copy
        // of — owner or replica — so a split child's reference to the parent's
        // segments protects them when the parent is swept. A child owned only
        // by another node is not visible here; the split protocol freezes a
        // parent's sweep for the split's duration to cover that window.
        let mut shared_by_source: std::collections::BTreeMap<
            PartitionId,
            std::collections::BTreeSet<String>,
        > = std::collections::BTreeMap::new();
        for host in &hosts {
            for (source, names) in host.shared_segment_sources().await {
                shared_by_source.entry(source).or_default().extend(names);
            }
        }

        let empty = std::collections::BTreeSet::new();
        for host in hosts.into_iter().filter(|host| host.is_owner()) {
            let shared = shared_by_source.get(&host.id()).unwrap_or(&empty);
            match host
                .sweep_orphans(shared, grace_millis, skew_millis, dry_run)
                .await
            {
                Ok(report) if report.deleted.is_empty() => {}
                Ok(report) if report.dry_run => tracing::info!(
                    partition = host.id().get(),
                    candidates = report.deleted.len(),
                    "orphan sweep dry run: objects that would be reclaimed"
                ),
                Ok(report) => tracing::info!(
                    partition = host.id().get(),
                    reclaimed = report.deleted.len(),
                    "orphan sweep reclaimed stranded objects"
                ),
                Err(error) => tracing::warn!(
                    partition = host.id().get(),
                    %error,
                    "orphan sweep failed; stranded objects wait for the next pass"
                ),
            }
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
    async fn handle(&self, from: NodeId, call: PeerCall) -> TransportResult<Bytes> {
        let Some(node) = self.node.upgrade() else {
            return Err(TransportError::NoHandler(ServiceId::Proxy));
        };
        Ok(dispatch(&node, from, call).await)
    }
}

/// Runs a request another node forwarded here.
///
/// The receiving half of the forwarding hop, spanned so the owner's own work on
/// a proxied request shows up as a span rather than being folded into the
/// caller's peer call. Trace context does not yet cross the peer frame, so this
/// is a local root rather than a child; the span still isolates the owner's
/// share of a forwarded request's latency.
#[tracing::instrument(name = "proxy.dispatch", skip_all, fields(from = from.get(), method = call.method))]
async fn dispatch<R: Runtime>(node: &Node<R>, from: NodeId, call: PeerCall) -> Bytes {
    /// Runs one forwarded call, encoding whichever way it went.
    macro_rules! run {
        ($request:ty, $method:ident) => {
            match <$request as prost::Message>::decode(call.payload.as_ref()) {
                Ok(request) => match node.$method(request, true, None).await {
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
                Ok(reply) => proxy::encode_lease_reply(reply),
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

/// One partition's catch-up pass, reporting whether it still has work to
/// retry.
///
/// A free function so several of these can be in flight at once: one
/// unreachable peer should cost one timeout, not one per partition it holds.
///
/// A replica that has fallen past the owner's retained log is not counted as
/// outstanding work, however far short of the horizon it is. No number of
/// passes produces entries the log no longer holds, so counting it would leave
/// this node reconciling on every poll forever and would bury the reason under
/// a retry. It is reported instead, through the `replicas-recoverable`
/// readiness condition that the lease heartbeat drives from the same
/// per-replica record this pass reads.
async fn catch_up_one<R: Runtime>(host: &Arc<PartitionHost<R>>) -> bool {
    match host.catch_up_replicas().await {
        Ok(pass) => {
            if !pass.stranded.is_empty() {
                tracing::debug!(
                    partition = host.id().get(),
                    stranded = ?pass.stranded,
                    "a replica is beyond this owner's retained log, so a catch-up cannot help it"
                );
            }
            if pass.is_complete() {
                return true;
            }
            tracing::warn!(
                partition = host.id().get(),
                behind = ?pass.behind,
                horizon = pass.horizon.get(),
                "an advertised replica is still short of the committed prefix"
            );
            false
        }
        Err(error) => {
            tracing::warn!(
                partition = host.id().get(),
                %error,
                "could not catch a replica up"
            );
            false
        }
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
    use crate::map_source::StaticMapSource;
    use async_trait::async_trait;
    use orbita_core::{Epoch, KeyRange, KeyspaceName, MapVersion};
    use orbita_format::testing::MemoryStore;
    use orbita_objectstore::{
        ETag, ObjectError, ObjectMeta, ObjectResult, ObjectStore, Precondition,
    };
    use orbita_sim::Simulation;
    use std::ops::Range;

    /// An in-memory store whose listings can be failed for one reconcile.
    ///
    /// Opening a partition lists its segment and value prefixes before it can
    /// serve. Failing that operation exercises the real open error path while
    /// keeping this simulation independent of a filesystem backend.
    struct ListingFailureStore {
        inner: MemoryStore,
        fail_list: std::sync::atomic::AtomicBool,
    }

    impl ListingFailureStore {
        fn new() -> Self {
            Self {
                inner: MemoryStore::new(),
                fail_list: std::sync::atomic::AtomicBool::new(false),
            }
        }

        fn fail_list(&self, fail: bool) {
            self.fail_list.store(fail, Ordering::Release);
        }
    }

    #[async_trait]
    impl ObjectStore for ListingFailureStore {
        async fn put(&self, key: &str, data: Bytes) -> ObjectResult<ETag> {
            self.inner.put(key, data).await
        }

        async fn put_if(
            &self,
            key: &str,
            data: Bytes,
            precondition: Precondition,
        ) -> ObjectResult<ETag> {
            self.inner.put_if(key, data, precondition).await
        }

        async fn get(&self, key: &str) -> ObjectResult<(Bytes, ETag)> {
            self.inner.get(key).await
        }

        async fn get_range(&self, key: &str, range: Range<u64>) -> ObjectResult<Bytes> {
            self.inner.get_range(key, range).await
        }

        async fn head(&self, key: &str) -> ObjectResult<ObjectMeta> {
            self.inner.head(key).await
        }

        async fn list(&self, prefix: &str) -> ObjectResult<Vec<ObjectMeta>> {
            if self.fail_list.load(Ordering::Acquire) {
                return Err(ObjectError::Transient(
                    "injected partition open failure".to_string(),
                ));
            }
            self.inner.list(prefix).await
        }

        async fn delete(&self, key: &str) -> ObjectResult<()> {
            self.inner.delete(key).await
        }
    }

    fn keyspace_info() -> KeyspaceInfo {
        KeyspaceInfo {
            id: KeyspaceId(1),
            name: KeyspaceName::new("default").unwrap(),
            default_ttl_millis: None,
            max_value_bytes: None,
            max_storage_bytes: None,
            max_reads_per_second: None,
            max_writes_per_second: None,
        }
    }

    fn partition(id: u64, range: KeyRange) -> PartitionInfo {
        PartitionInfo {
            id: PartitionId(id),
            keyspace: KeyspaceId(1),
            range,
            owner: Some(NodeId(1)),
            epoch: Epoch(1),
            replicas: Vec::new(),
        }
    }

    /// One unbounded partition, which is what the node opens at start.
    fn one_partition_map() -> PartitionMap {
        let mut map = PartitionMap::new(MapVersion(1));
        map.insert_keyspace(keyspace_info());
        map.insert_partition(partition(1, KeyRange::unbounded()));
        map
    }

    /// The same keyspace split in two, handing this node a second partition.
    fn two_partition_map() -> PartitionMap {
        let mut map = PartitionMap::new(MapVersion(2));
        map.insert_keyspace(keyspace_info());
        map.insert_partition(partition(
            1,
            KeyRange::new(Bytes::new(), Some(Bytes::from_static(b"m"))).unwrap(),
        ));
        map.insert_partition(partition(
            2,
            KeyRange::new(Bytes::from_static(b"m"), None).unwrap(),
        ));
        assert_eq!(map.check_coverage(), Ok(()));
        map
    }

    /// The transition the readiness gate adds beyond startup: a reconcile that
    /// cannot open a partition unreadies the node, and the retry on the next
    /// refresh, with the map version unchanged, readies it again once the
    /// open succeeds.
    #[test]
    fn a_failed_reconcile_unreadies_the_node_until_a_retry_opens_the_partition() {
        let sim = Simulation::new(7);
        let runtime = sim.add_node(NodeId(1));
        let store = Arc::new(ListingFailureStore::new());
        let layout = DataLayout {
            store: Arc::clone(&store) as Arc<dyn ObjectStore>,
            wal_root: "wal".to_string(),
            wal_segment_bytes: crate::DEFAULT_WAL_SEGMENT_BYTES,
        };

        let source = StaticMapSource::new(one_partition_map());
        let gate = Arc::new(ReadinessGate::new());
        let node = {
            let layout = layout.clone();
            let source = BoxedMapSource::new(source.clone());
            let gate = Arc::clone(&gate);
            sim.block_on(async move {
                let authenticator = Arc::new(Authenticator::new(
                    false,
                    None,
                    std::time::Duration::from_secs(86_400),
                    runtime.clock().clone(),
                ));
                Node::start(
                    runtime,
                    NodeId(1),
                    layout,
                    source,
                    crate::DEFAULT_LEASE_DURATION,
                    gate,
                    authenticator,
                )
                .await
                .expect("the node starts")
            })
        };
        assert!(
            gate.state().is_met(ReadinessCondition::PartitionsCaughtUp),
            "the initial open marks catch-up"
        );

        store.fail_list(true);
        source.set(two_partition_map());
        let refreshing = Arc::clone(&node);
        let outcome = sim.block_on(async move { refreshing.refresh_map().await });
        assert!(outcome.is_err(), "the blocked partition cannot open");
        assert!(
            !gate.state().is_met(ReadinessCondition::PartitionsCaughtUp),
            "a node holding a partition it could not open is not ready"
        );

        store.fail_list(false);
        let refreshing = Arc::clone(&node);
        sim.block_on(async move { refreshing.refresh_map().await })
            .expect("the retry reconciles even though the map version is unchanged");
        assert!(
            gate.state().is_met(ReadinessCondition::PartitionsCaughtUp),
            "readiness returns once every partition is open again"
        );

        drop(node);
    }

    /// A LIST that has paged across a partition boundary must be attributed to
    /// the partition it resumes into, not the one its prefix started in.
    /// Resolves the telemetry partition exactly as [`Node::list`] does — decode
    /// the cursor, take its route key — and shows the answer follows the cursor
    /// past the boundary rather than pinning every later page to partition one.
    #[test]
    fn a_cross_partition_list_page_is_attributed_to_the_cursor_partition() {
        let sim = Simulation::new(11);
        let runtime = sim.add_node(NodeId(1));
        let store = Arc::new(MemoryStore::new());
        let layout = DataLayout {
            store: Arc::clone(&store) as Arc<dyn ObjectStore>,
            wal_root: "wal".to_string(),
            wal_segment_bytes: crate::DEFAULT_WAL_SEGMENT_BYTES,
        };
        let source = BoxedMapSource::new(StaticMapSource::new(two_partition_map()));
        let gate = Arc::new(ReadinessGate::new());
        let node = sim.block_on(async move {
            let authenticator = Arc::new(Authenticator::new(
                false,
                None,
                std::time::Duration::from_secs(86_400),
                runtime.clock().clone(),
            ));
            Node::start(
                runtime,
                NodeId(1),
                layout,
                source,
                crate::DEFAULT_LEASE_DURATION,
                gate,
                authenticator,
            )
            .await
            .expect("the node starts")
        });

        // The first page has no cursor: its route key is the prefix, which
        // lives below "m" and so belongs to partition one.
        let prefix = b"";
        let first = Cursor::decode(b"", prefix)
            .map(|resume| resume.route_key)
            .unwrap();
        assert_eq!(
            node.label_partition("default", &first),
            Some(PartitionId(1)),
        );

        // A later page resumes past the boundary at "m", which is partition
        // two; telemetry has to move with it.
        let boundary = Cursor::at_partition_boundary(Bytes::from_static(b"m")).encode();
        let resumed = Cursor::decode(&boundary, prefix)
            .map(|resume| resume.route_key)
            .unwrap();
        assert_eq!(
            node.label_partition("default", &resumed),
            Some(PartitionId(2)),
            "a scan that crossed into partition two is reported against it",
        );

        drop(node);
    }

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
