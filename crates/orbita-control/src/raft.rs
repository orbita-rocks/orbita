//! Raft under [`ConsensusLog`], via `raft-rs`.
//!
//! # Why `raft-rs` and not `openraft`
//!
//! The hard constraint from `docs/plan/03-control.md` is that consensus IO
//! must go through `orbita_runtime`, or the simulator cannot drive elections
//! deterministically. `raft-rs` is a state machine and nothing else: it never
//! spawns, never sleeps, never touches a socket or a file. The caller ticks
//! it, feeds it messages, and carries out the IO its `Ready` struct asks for.
//! That is exactly the shape this crate needs, because every one of those
//! steps can be routed through a runtime seam.
//!
//! `openraft` was evaluated and rejected on the same constraint. Its storage
//! and network traits could be implemented over `orbita_runtime`, but its
//! timers and task spawning go through an `AsyncRuntime` trait whose methods
//! are associated functions with no instance handle, so there is no way to
//! hand it a node's `SimClock` or the simulator's scheduler. It owns its IO
//! at the type level, and the brief says an implementation that insists on
//! owning its own IO does not fit.
//!
//! # How the pieces map
//!
//! - **Time.** The driver task ticks the `RawNode` off `Clock::sleep`, so a
//!   simulated run explores elections in virtual time.
//! - **Randomness.** `raft-rs` randomizes its election timeout from
//!   `rand::thread_rng`, which would break replayability. The drawn value is
//!   only ever consulted by `tick`, so the driver overwrites it through
//!   `set_randomized_election_timeout` with a value drawn from the runtime's
//!   seeded [`orbita_runtime::Rng`] before every tick, redrawing whenever the
//!   term or role changes. The thread rng still gets called inside `raft-rs`,
//!   but nothing it produces ever influences behaviour.
//! - **Network.** Outbound messages become `Transport::call`s on
//!   [`ServiceId::Raft`]; inbound ones arrive through a registered
//!   [`PeerHandler`] and are stepped into the node. Raft messages are
//!   fire-and-forget, which matches the transport's drop-and-retry world:
//!   the protocol's own retries are the delivery guarantee.
//! - **Storage.** `raft-rs` reads the log through its synchronous `Storage`
//!   trait, served here by an in-memory `MemStorage` mirror. Durability is
//!   the driver's job: every entry and hard-state change is appended to an
//!   `orbita_runtime::Disk` file and fsynced before the node is allowed to
//!   advance, which is the ordering the Raft paper requires. Recovery reads
//!   that file back and rebuilds the mirror.
//!
//! # What a one-node group does
//!
//! Elects itself after one election timeout and then behaves exactly like
//! [`crate::SingleNodeLog`] from above the trait, which is what keeps the
//! `orbita dev` path working with the same implementation production uses.
//!
//! # What is not here yet
//!
//! Snapshots and log compaction: the control log is small and recovery
//! replays it whole, same as [`crate::SingleNodeLog`]. Dynamic membership:
//! the voter set is fixed at open. Both bolt onto this driver without
//! changing anything above the trait.

use crate::command::ControlCommand;
use crate::consensus::{ConsensusLog, LogEntry, LogIndex};

use bytes::{BufMut, Bytes, BytesMut};
use orbita_core::{Error, NodeId, Result};
use orbita_runtime::{
    select, Clock, Disk, DiskError, Either, File, OpenOptions, PeerCall, PeerHandler, Rng, Runtime,
    ServiceId, Transport, TransportError, TransportResult,
};
use prost::Message as _;
use raft::eraftpb::{Entry, EntryType, HardState, Message};
use raft::storage::MemStorage;
use raft::{Config as RaftNodeConfig, RawNode, StateRole};

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// The one method Raft traffic uses on [`ServiceId::Raft`]. A raft message is
/// self-describing, so the envelope needs no further discrimination.
const METHOD_MESSAGE: u16 = 1;

const LOG_PATH: &str = "control/raft";
const VOTERS_PATH: &str = "control/raft-voters";

/// How often the driver ticks the state machine. Election and heartbeat
/// timeouts below are measured in these.
const TICK_INTERVAL: Duration = Duration::from_millis(100);

/// Ticks without a heartbeat before a follower campaigns, before jitter.
/// With jitter the effective timeout is one to two seconds, which matches the
/// heartbeat-to-failover ratios in [`crate::ControlConfig`].
const ELECTION_TICK: usize = 10;

/// Ticks between leader heartbeats: 200ms, comfortably inside the election
/// timeout.
const HEARTBEAT_TICK: usize = 2;

/// `kind u8 | len u32 | crc32 u32 | payload`, appended per record.
///
/// Same reasoning as the single-node log's framing: a per-record checksum
/// means a crash mid-append costs the record being written and nothing before
/// it.
const RECORD_HEADER_BYTES: usize = 9;
const RECORD_ENTRY: u8 = 1;
const RECORD_HARD_STATE: u8 = 2;

/// A consensus log replicated by Raft.
///
/// The handle is deliberately not generic over the runtime: all the IO lives
/// in the driver task, and everything above the trait only ever talks to the
/// shared state and the driver's mailbox.
pub struct RaftLog {
    shared: Arc<Mutex<Shared>>,
    tx: mpsc::UnboundedSender<Event>,
}

/// What the driver publishes for the handle to read without waiting on it.
#[derive(Default)]
struct Shared {
    committed: Vec<ControlCommand>,
    is_leader: bool,
    leader: Option<NodeId>,
}

enum Event {
    /// An inbound peer message to step into the node.
    Message(Box<Message>),
    Propose {
        command: ControlCommand,
        reply: oneshot::Sender<Result<LogIndex>>,
    },
    /// Stops the driver, which is how a test restarts a node without tearing
    /// down the world around it. Production nodes run until the process dies.
    Shutdown,
}

impl RaftLog {
    /// Opens this node's slice of a Raft group and starts its driver task.
    ///
    /// `voters` is the whole group, this node included, and every member must
    /// be started with the same list. Recovery replays whatever the last
    /// incarnation fsynced, truncating a torn tail exactly like the
    /// single-node log does, because dying mid-append is an ordinary end.
    pub async fn open<R: Runtime>(runtime: &R, voters: &[NodeId]) -> Result<Arc<Self>> {
        let local = runtime.transport().local_node();
        if voters.is_empty() {
            return Err(Error::InvalidArgument(
                "the raft voter set cannot be empty".into(),
            ));
        }
        let mut durable_voters = voters.to_vec();
        durable_voters.sort_unstable();
        if durable_voters.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(Error::InvalidArgument(format!(
                "the raft voter set contains a duplicate: {voters:?}"
            )));
        }
        if !voters.contains(&local) {
            return Err(Error::Internal(format!(
                "node {local} is not in the raft voter set"
            )));
        }

        assert_durable_voters(runtime, &durable_voters).await?;

        let file = runtime
            .disk()
            .open(LOG_PATH, OpenOptions::create())
            .await
            .map_err(disk_error)?;
        let size = file.size().await.map_err(disk_error)?;
        let raw = if size == 0 {
            Bytes::new()
        } else {
            match file.read_at(0, size as usize).await {
                Ok(bytes) => bytes,
                // Damage reads as truncation, same as every other log here.
                Err(DiskError::Corrupt { .. }) => Bytes::new(),
                Err(e) => return Err(disk_error(e)),
            }
        };

        let (entries, hard_state, good_bytes) = decode_records(&raw);
        if good_bytes < raw.len() {
            tracing::warn!(
                node = %local,
                dropped = raw.len() - good_bytes,
                "raft log had a torn tail; truncating"
            );
            file.truncate(good_bytes as u64).await.map_err(disk_error)?;
            file.sync().await.map_err(disk_error)?;
        }

        let voter_ids: Vec<u64> = durable_voters.iter().map(|v| v.get()).collect();
        let storage = MemStorage::new_with_conf_state((voter_ids, Vec::new()));
        if let Some(hs) = &hard_state {
            storage.wl().set_hardstate(hs.clone());
        }
        if !entries.is_empty() {
            storage.wl().append(&entries).map_err(raft_error)?;
        }

        let config = RaftNodeConfig {
            id: local.get(),
            election_tick: ELECTION_TICK,
            heartbeat_tick: HEARTBEAT_TICK,
            ..Default::default()
        };
        // Everything worth logging is re-logged through `tracing` at the
        // driver level, so raft's own logger goes nowhere.
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        let node = RawNode::new(&config, storage.clone(), &logger).map_err(raft_error)?;

        let shared = Arc::new(Mutex::new(Shared::default()));
        let (tx, rx) = mpsc::unbounded_channel();
        runtime
            .transport()
            .register(ServiceId::Raft, RaftPeer { tx: tx.clone() });

        let driver = Driver {
            runtime: runtime.clone(),
            local,
            node,
            storage,
            file,
            durable_bytes: good_bytes as u64,
            shared: Arc::clone(&shared),
            rx,
            pending: HashMap::new(),
            next_proposal: 0,
            was_leader: false,
            timeout_key: None,
            timeout_ticks: ELECTION_TICK,
        };
        runtime.spawn(driver.run());

        Ok(Arc::new(Self { shared, tx }))
    }

    /// Asks the driver to stop. Only tests use this; see [`Event::Shutdown`].
    pub fn shutdown(&self) {
        let _ = self.tx.send(Event::Shutdown);
    }
}

async fn assert_durable_voters<R: Runtime>(runtime: &R, voters: &[NodeId]) -> Result<()> {
    let file = runtime
        .disk()
        .open(VOTERS_PATH, OpenOptions::create())
        .await
        .map_err(disk_error)?;
    let configured = encode_voters(voters);
    let size = file.size().await.map_err(disk_error)?;
    if size == 0 {
        file.append(configured).await.map_err(disk_error)?;
        file.sync().await.map_err(disk_error)?;
        return Ok(());
    }
    let durable = file.read_at(0, size as usize).await.map_err(disk_error)?;
    if durable != configured {
        let held = decode_voters(&durable).unwrap_or_default();
        return Err(Error::InvalidArgument(format!(
            "configured raft voters {voters:?} do not match durable voters {held:?}; restore the \
             original fixed peer set or use a new empty data directory for a new cluster"
        )));
    }
    Ok(())
}

fn encode_voters(voters: &[NodeId]) -> Bytes {
    let mut bytes = BytesMut::with_capacity(4 + voters.len() * 8);
    bytes.put_u32_le(voters.len() as u32);
    for voter in voters {
        bytes.put_u64_le(voter.get());
    }
    bytes.freeze()
}

fn decode_voters(bytes: &[u8]) -> Option<Vec<NodeId>> {
    let count = u32::from_le_bytes(bytes.get(..4)?.try_into().ok()?) as usize;
    if bytes.len() != 4 + count * 8 {
        return None;
    }
    Some(
        bytes[4..]
            .chunks_exact(8)
            .map(|chunk| {
                NodeId(u64::from_le_bytes(
                    chunk.try_into().expect("eight-byte chunk"),
                ))
            })
            .collect(),
    )
}

impl ConsensusLog for RaftLog {
    async fn propose(&self, command: ControlCommand) -> Result<LogIndex> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(Event::Propose { command, reply })
            .map_err(|_| driver_gone())?;
        response.await.map_err(|_| driver_gone())?
    }

    async fn commit_index(&self) -> LogIndex {
        self.lock().committed.len() as LogIndex
    }

    async fn subscribe(&self, after: LogIndex) -> Result<Vec<LogEntry>> {
        let shared = self.lock();
        let start = after as usize;
        if start >= shared.committed.len() {
            return Ok(Vec::new());
        }
        Ok(shared.committed[start..]
            .iter()
            .enumerate()
            .map(|(offset, command)| LogEntry {
                index: (start + offset + 1) as LogIndex,
                command: command.clone(),
            })
            .collect())
    }

    async fn is_leader(&self) -> bool {
        self.lock().is_leader
    }

    async fn leader(&self) -> Option<NodeId> {
        self.lock().leader
    }
}

impl RaftLog {
    fn lock(&self) -> std::sync::MutexGuard<'_, Shared> {
        // The lock is never held across an await, so poisoning means a panic
        // mid-update, and propagating it is the right answer.
        self.shared.lock().expect("raft shared state poisoned")
    }
}

/// Receives peer traffic and forwards it to the driver.
///
/// Raft messages get an empty reply rather than an answer: the protocol's
/// responses are messages of its own, sent on the node's own schedule.
struct RaftPeer {
    tx: mpsc::UnboundedSender<Event>,
}

impl PeerHandler for RaftPeer {
    fn handle(
        &self,
        _from: NodeId,
        call: PeerCall,
    ) -> impl Future<Output = TransportResult<Bytes>> + Send {
        // The method is checked before the payload so that a future second
        // method on this service fails loudly instead of being decoded as a
        // raft message that happens to parse.
        let result = if call.method != METHOD_MESSAGE {
            Err(TransportError::Remote(format!(
                "unknown raft method {}",
                call.method
            )))
        } else {
            match Message::decode(call.payload.as_ref()) {
                Ok(message) => {
                    // A send to a stopped driver just drops the message,
                    // which is indistinguishable from the network losing it,
                    // and Raft already tolerates that.
                    let _ = self.tx.send(Event::Message(Box::new(message)));
                    Ok(Bytes::new())
                }
                Err(e) => Err(TransportError::Remote(format!(
                    "undecodable raft message: {e}"
                ))),
            }
        };
        async move { result }
    }
}

/// The task that owns the `RawNode` and does all of its IO.
struct Driver<R: Runtime> {
    runtime: R,
    local: NodeId,
    node: RawNode<MemStorage>,
    /// The in-memory mirror `raft-rs` reads through its `Storage` trait.
    storage: MemStorage,
    /// The durable copy of the mirror, on `orbita_runtime::Disk`.
    file: <R::Disk as Disk>::File,
    /// Where the last fsynced record ends, for rolling back a failed append.
    durable_bytes: u64,
    shared: Arc<Mutex<Shared>>,
    rx: mpsc::UnboundedReceiver<Event>,
    /// Proposals waiting to commit, keyed by the id carried in the entry's
    /// context.
    pending: HashMap<u64, oneshot::Sender<Result<LogIndex>>>,
    next_proposal: u64,
    was_leader: bool,
    /// The `(term, role)` the current election jitter was drawn for.
    timeout_key: Option<(u64, StateRole)>,
    timeout_ticks: usize,
}

impl<R: Runtime> Driver<R> {
    async fn run(mut self) {
        let tick_nanos = TICK_INTERVAL.as_nanos() as u64;
        let mut next_tick = self.runtime.clock().monotonic_nanos() + tick_nanos;
        loop {
            self.assert_election_timeout();

            let now = self.runtime.clock().monotonic_nanos();
            if now >= next_tick {
                self.node.tick();
                // Measured from now rather than accumulated, so a stall does
                // not replay its missed ticks as a burst of instant ones.
                next_tick = now + tick_nanos;
            } else {
                let wait = Duration::from_nanos(next_tick - now);
                match select(self.rx.recv(), self.runtime.clock().sleep(wait)).await {
                    Either::Left(Some(Event::Shutdown)) | Either::Left(None) => return,
                    Either::Left(Some(event)) => self.handle_event(event),
                    Either::Right(()) => {}
                }
            }

            if let Err(e) = self.on_ready().await {
                // A node that cannot persist or cannot read what the quorum
                // committed cannot safely acknowledge anything, so it stops
                // participating, which to the rest of the group looks like a
                // crash and is handled like one.
                tracing::error!(node = %self.local, error = %e, "raft driver stopping");
                for (_, reply) in self.pending.drain() {
                    let _ = reply.send(Err(Error::Unavailable(format!("consensus stopped: {e}"))));
                }
                return;
            }
            self.publish_soft_state();
        }
    }

    /// Pins the randomized election timeout to a value drawn from the seeded
    /// rng, so elections replay from a seed. See the module docs.
    fn assert_election_timeout(&mut self) {
        let key = (self.node.raft.term, self.node.raft.state);
        if self.timeout_key != Some(key) {
            let jitter = (self.runtime.rng().next_u64() % ELECTION_TICK as u64) as usize;
            self.timeout_ticks = ELECTION_TICK + jitter;
            self.timeout_key = Some(key);
        }
        self.node
            .raft
            .set_randomized_election_timeout(self.timeout_ticks);
    }

    fn handle_event(&mut self, event: Event) {
        match event {
            Event::Message(message) => {
                if let Err(e) = self.node.step(*message) {
                    // A stale or misaddressed message is the network's
                    // ordinary weather, not a fault in this node.
                    tracing::debug!(node = %self.local, error = %e, "raft rejected a message");
                }
            }
            Event::Propose { command, reply } => self.handle_propose(command, reply),
            // Shutdown is consumed by the select in `run`; nothing routes it
            // here.
            Event::Shutdown => {}
        }
    }

    fn handle_propose(
        &mut self,
        command: ControlCommand,
        reply: oneshot::Sender<Result<LogIndex>>,
    ) {
        if self.node.raft.state != StateRole::Leader {
            let _ = reply.send(Err(Error::NotLeader {
                leader: self.current_leader(),
            }));
            return;
        }

        // The id rides in the entry's context so the commit can be matched
        // back to the caller. Only this node's own pending map ever looks the
        // id up, so ids need only be unique per driver incarnation.
        let id = self.next_proposal;
        self.next_proposal += 1;
        match self
            .node
            .propose(id.to_le_bytes().to_vec(), command.encode().to_vec())
        {
            Ok(()) => {
                self.pending.insert(id, reply);
            }
            Err(e) => {
                let _ = reply.send(Err(Error::Unavailable(format!(
                    "raft dropped the proposal: {e}"
                ))));
            }
        }
    }

    /// Carries out the IO the state machine asked for, in the order the Raft
    /// paper requires: entries and hard state hit the disk before the
    /// messages that acknowledge them leave the node.
    async fn on_ready(&mut self) -> Result<()> {
        if !self.node.has_ready() {
            return Ok(());
        }
        let mut ready = self.node.ready();

        self.send_messages(ready.take_messages());

        // Nobody compacts the log, so nobody can be asked to install a
        // snapshot. If this fires, compaction was added below without
        // teaching recovery about it, and refusing loudly beats quietly
        // diverging.
        assert!(
            ready.snapshot().is_empty(),
            "received a snapshot but snapshots are never generated"
        );

        self.apply(ready.take_committed_entries())?;

        let mut frames = BytesMut::new();
        if !ready.entries().is_empty() {
            for entry in ready.entries() {
                frames.put_slice(&encode_record(RECORD_ENTRY, &entry.encode_to_vec()));
            }
            self.storage
                .wl()
                .append(ready.entries())
                .map_err(raft_error)?;
        }
        if let Some(hs) = ready.hs() {
            self.storage.wl().set_hardstate(hs.clone());
            frames.put_slice(&encode_record(RECORD_HARD_STATE, &hs.encode_to_vec()));
        }
        self.persist(frames.freeze()).await?;

        self.send_messages(ready.take_persisted_messages());

        let mut light = self.node.advance(ready);
        if let Some(commit) = light.commit_index() {
            // Persisting the commit index is what lets recovery re-deliver
            // committed entries immediately instead of waiting to relearn
            // the index from a quorum.
            let hs = {
                let mut core = self.storage.wl();
                core.mut_hard_state().commit = commit;
                core.hard_state().clone()
            };
            let mut frame = BytesMut::new();
            frame.put_slice(&encode_record(RECORD_HARD_STATE, &hs.encode_to_vec()));
            self.persist(frame.freeze()).await?;
        }
        self.send_messages(light.take_messages());
        self.apply(light.take_committed_entries())?;
        self.node.advance_apply();
        Ok(())
    }

    /// Appends and fsyncs, rolling back to the last durable record on
    /// failure so the file never grows an unreadable middle.
    async fn persist(&mut self, frames: Bytes) -> Result<()> {
        if frames.is_empty() {
            return Ok(());
        }
        let len = frames.len() as u64;
        let written = async {
            self.file.append(frames).await?;
            self.file.sync().await
        }
        .await;
        if let Err(e) = written {
            let _ = self.file.truncate(self.durable_bytes).await;
            return Err(disk_error(e));
        }
        self.durable_bytes += len;
        Ok(())
    }

    /// Feeds committed entries to the shared log and settles the proposals
    /// that produced them.
    fn apply(&mut self, entries: Vec<Entry>) -> Result<()> {
        for entry in entries {
            // Leaders append an empty entry on election, and nothing here
            // proposes conf changes. Neither is a command.
            if entry.entry_type() != EntryType::EntryNormal || entry.data.is_empty() {
                continue;
            }
            // The quorum agreed on bytes this binary cannot read, which means
            // a newer binary wrote them. Skipping would shift every later
            // index on this node and quietly diverge from the members that
            // could read it, so the driver stops instead: to the rest of the
            // group that is a crash, and a crashed node is recoverable where
            // a diverged one is not. Unreachable between same-version
            // members; the guard is for when rolling upgrades arrive.
            let command = ControlCommand::decode(&entry.data).map_err(|e| {
                Error::Internal(format!(
                    "committed entry {} did not decode: {e}",
                    entry.index
                ))
            })?;
            let index = {
                let mut shared = self.shared.lock().expect("raft shared state poisoned");
                shared.committed.push(command);
                shared.committed.len() as LogIndex
            };
            if let Ok(id) = <[u8; 8]>::try_from(entry.context.as_slice()) {
                if let Some(reply) = self.pending.remove(&u64::from_le_bytes(id)) {
                    let _ = reply.send(Ok(index));
                }
            }
        }
        Ok(())
    }

    /// Publishes leadership for the handle to read, and fails the proposals
    /// a deposed leader can no longer promise anything about.
    fn publish_soft_state(&mut self) {
        let is_leader = self.node.raft.state == StateRole::Leader;
        let leader = self.current_leader();
        {
            let mut shared = self.shared.lock().expect("raft shared state poisoned");
            shared.is_leader = is_leader;
            shared.leader = leader;
        }
        if self.was_leader && !is_leader {
            // The entries may still commit under the new leader; failing with
            // NotLeader tells the caller to retry there, and the state
            // machine above the trait is what makes a duplicate harmless.
            for (_, reply) in self.pending.drain() {
                let _ = reply.send(Err(Error::NotLeader { leader }));
            }
        }
        self.was_leader = is_leader;
    }

    fn current_leader(&self) -> Option<NodeId> {
        let id = self.node.raft.leader_id;
        (id != 0).then_some(NodeId(id))
    }

    /// Fires each message at its peer and moves on. Raft's own retransmission
    /// is the delivery guarantee, so a failed call is only worth a debug
    /// line.
    fn send_messages(&self, messages: Vec<Message>) {
        for message in messages {
            let to = NodeId(message.to);
            let payload = Bytes::from(message.encode_to_vec());
            let transport = self.runtime.transport().clone();
            self.runtime.spawn(async move {
                let call = PeerCall {
                    service: ServiceId::Raft,
                    method: METHOD_MESSAGE,
                    payload,
                };
                if let Err(e) = transport.call(to, call).await {
                    tracing::debug!(peer = %to, error = %e, "raft message not delivered");
                }
            });
        }
    }
}

fn encode_record(kind: u8, payload: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(RECORD_HEADER_BYTES + payload.len());
    buf.put_u8(kind);
    buf.put_u32_le(payload.len() as u32);
    buf.put_u32_le(crc32fast::hash(payload));
    buf.put_slice(payload);
    buf.freeze()
}

/// Replays the durable file into the entries and hard state the mirror needs.
///
/// Entry records replay Raft's own truncation rule: an entry at index `i`
/// supersedes everything previously read at `i` or above, because a follower
/// that changed leaders overwrote that suffix. Hard state records apply in
/// order with the last one winning. Returns how many bytes were trustworthy;
/// anything after that is a torn tail for the caller to truncate.
fn decode_records(raw: &[u8]) -> (Vec<Entry>, Option<HardState>, usize) {
    let mut entries: Vec<Entry> = Vec::new();
    let mut hard_state = None;
    let mut offset = 0;

    while offset + RECORD_HEADER_BYTES <= raw.len() {
        let kind = raw[offset];
        let len = u32::from_le_bytes([
            raw[offset + 1],
            raw[offset + 2],
            raw[offset + 3],
            raw[offset + 4],
        ]) as usize;
        let crc = u32::from_le_bytes([
            raw[offset + 5],
            raw[offset + 6],
            raw[offset + 7],
            raw[offset + 8],
        ]);
        let body_start = offset + RECORD_HEADER_BYTES;
        let Some(body_end) = body_start.checked_add(len).filter(|e| *e <= raw.len()) else {
            break;
        };
        let body = &raw[body_start..body_end];
        if crc32fast::hash(body) != crc {
            break;
        }
        match kind {
            RECORD_ENTRY => {
                let Ok(entry) = Entry::decode(body) else {
                    break;
                };
                while entries.last().is_some_and(|last| last.index >= entry.index) {
                    entries.pop();
                }
                entries.push(entry);
            }
            RECORD_HARD_STATE => {
                let Ok(hs) = HardState::decode(body) else {
                    break;
                };
                hard_state = Some(hs);
            }
            // A kind this binary does not know means a newer one wrote it,
            // and everything after it may depend on it. Stop, same as an
            // undecodable body.
            _ => break,
        }
        offset = body_end;
    }

    (entries, hard_state, offset)
}

fn driver_gone() -> Error {
    Error::Unavailable("consensus driver stopped".into())
}

fn disk_error(e: DiskError) -> Error {
    Error::Internal(format!("raft log: {e}"))
}

fn raft_error(e: raft::Error) -> Error {
    Error::Internal(format!("raft: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::NodeRole;
    use orbita_sim::Simulation;

    fn register(id: u64) -> ControlCommand {
        ControlCommand::RegisterNode {
            node: NodeId(id),
            role: NodeRole::Worker,
            address: format!("10.0.0.{id}:7000"),
            speaks: crate::version::binary_speaks(),
        }
    }

    fn entry(index: u64, term: u64, id: u64) -> Entry {
        Entry {
            index,
            term,
            data: register(id).encode().to_vec(),
            ..Default::default()
        }
    }

    fn records(items: &[(u8, Vec<u8>)]) -> Vec<u8> {
        let mut raw = Vec::new();
        for (kind, payload) in items {
            raw.extend_from_slice(&encode_record(*kind, payload));
        }
        raw
    }

    #[test]
    fn a_conflicting_entry_drops_the_suffix_it_overwrote() {
        let raw = records(&[
            (RECORD_ENTRY, entry(1, 1, 1).encode_to_vec()),
            (RECORD_ENTRY, entry(2, 1, 2).encode_to_vec()),
            (RECORD_ENTRY, entry(3, 1, 3).encode_to_vec()),
            // A new leader rewrote index 2, which buries the old 2 and 3.
            (RECORD_ENTRY, entry(2, 2, 4).encode_to_vec()),
        ]);
        let (entries, _, good) = decode_records(&raw);
        assert_eq!(good, raw.len());
        assert_eq!(
            entries
                .iter()
                .map(|e| (e.index, e.term))
                .collect::<Vec<_>>(),
            vec![(1, 1), (2, 2)]
        );
    }

    #[test]
    fn the_last_hard_state_wins() {
        let newer = HardState {
            term: 3,
            vote: 2,
            commit: 5,
        };
        let raw = records(&[
            (RECORD_HARD_STATE, HardState::default().encode_to_vec()),
            (RECORD_HARD_STATE, newer.encode_to_vec()),
        ]);
        let (_, hard_state, good) = decode_records(&raw);
        assert_eq!(good, raw.len());
        assert_eq!(hard_state, Some(newer));
    }

    #[test]
    fn a_torn_tail_costs_the_record_being_written_and_nothing_before_it() {
        let whole = records(&[
            (RECORD_ENTRY, entry(1, 1, 1).encode_to_vec()),
            (RECORD_ENTRY, entry(2, 1, 2).encode_to_vec()),
        ]);
        for cut in 1..whole.len() {
            let (entries, _, good) = decode_records(&whole[..cut]);
            assert!(good <= cut);
            assert!(entries.len() <= 2);
            if !entries.is_empty() {
                assert_eq!(entries[0].index, 1, "the intact prefix survives");
            }
        }
    }

    #[test]
    fn an_unknown_record_kind_stops_recovery() {
        let raw = records(&[
            (RECORD_ENTRY, entry(1, 1, 1).encode_to_vec()),
            (99, b"from the future".to_vec()),
            (RECORD_ENTRY, entry(2, 1, 2).encode_to_vec()),
        ]);
        let (entries, _, good) = decode_records(&raw);
        assert_eq!(entries.len(), 1);
        assert!(good < raw.len());
    }

    #[test]
    fn a_restart_rejects_a_voter_set_that_differs_from_durable_identity() {
        let sim = Simulation::new(9);
        let runtime = sim.add_node(NodeId(1));
        let opening = runtime.clone();
        let log = sim.block_on(async move { RaftLog::open(&opening, &[NodeId(1)]).await.unwrap() });
        log.shutdown();

        let reopening = sim.runtime(NodeId(1));
        let error = sim
            .block_on(async move { RaftLog::open(&reopening, &[NodeId(1), NodeId(2)]).await })
            .err()
            .expect("the changed voter set is rejected");
        assert!(error.to_string().contains("durable voters"), "{error}");
    }

    #[test]
    fn duplicate_voters_are_rejected_before_raft_starts() {
        let sim = Simulation::new(10);
        let runtime = sim.add_node(NodeId(1));
        let error = sim
            .block_on(async move { RaftLog::open(&runtime, &[NodeId(1), NodeId(1)]).await })
            .err()
            .expect("the duplicate is rejected");
        assert!(error.to_string().contains("duplicate"), "{error}");
    }
}
