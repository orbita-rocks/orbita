//! The seam consensus lives behind.
//!
//! [`ConsensusLog`] is three operations: propose a command and learn when it
//! is committed, read the commit index, and read committed entries after a
//! given index. Everything above the trait, meaning [`crate::state`],
//! [`crate::controller`], and [`crate::client`], is where the correctness
//! argument lives, and none of it knows how a command became committed.
//!
//! Two implementations sit underneath it. [`SingleNodeLog`], here, is the
//! durable log for a cluster of one: it is what `orbita dev` runs and what
//! most simulator scenarios need, and it is a correct implementation of the
//! trait for that cluster size. [`crate::RaftLog`] is the replicated one, a
//! Raft group run by `raft-rs` with its clock, network, storage, and
//! randomness all routed through `orbita_runtime`, which is what lets the
//! deterministic simulator drive its elections; the `raft` module documents
//! that mapping.
//!
//! The one observable difference between them is that `propose` fails with
//! "not the leader" on a follower. [`crate::controller::Controller`] already
//! surfaces that, and [`crate::client::ControlClient`] already follows the
//! redirect, because a single-node log is the degenerate case of a leader and
//! not a different shape.

use crate::codec::CodecError;
use crate::command::ControlCommand;

use bytes::{BufMut, Bytes, BytesMut};
use orbita_core::{Error, NodeId, Result};
use orbita_runtime::{Disk, DiskError, File, OpenOptions, Runtime, Transport};

use std::future::Future;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Position in the replicated log. The first entry is 1, so zero means
/// "nothing applied yet" without needing an option.
pub type LogIndex = u64;

/// One committed decision and where it sits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub index: LogIndex,
    pub command: ControlCommand,
}

/// One safe Raft membership operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MembershipChange {
    /// Add a non-voting member so it can catch up before promotion.
    AddLearner(NodeId),
    /// Adds a learner and durably binds the address and node identity every
    /// voter needs to rediscover it after a full restart.
    AddLearnerMember(RaftMember),
    /// Promote a caught-up learner into the voting set.
    Promote(NodeId),
    /// Remove a voter or learner through the replicated Raft configuration.
    Remove(NodeId),
}

/// One Raft participant and the durable discovery identity bound to its id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftMember {
    pub node: NodeId,
    pub address: String,
    pub node_identity: String,
}

/// An ordered, durable, agreed-upon sequence of commands.
///
/// Implementations decide how agreement is reached. Everything above this
/// trait only requires that a committed entry is never lost, never reordered,
/// and never seen at two different indices.
pub trait ConsensusLog: Send + Sync + 'static {
    /// Appends a command and resolves once it is committed, returning its
    /// index.
    ///
    /// Committed means durable and agreed. It does not mean applied: the
    /// caller applies through the returned index itself, because the state
    /// machine lives above this trait and a Raft implementation would drive
    /// the same apply from its own committed stream.
    fn propose(&self, command: ControlCommand) -> impl Future<Output = Result<LogIndex>> + Send;

    /// The highest committed index.
    fn commit_index(&self) -> impl Future<Output = LogIndex> + Send;

    /// Every committed entry after `after`, in order.
    ///
    /// This is the subscription in pull form. A push-based stream would need
    /// its own task and its own backpressure story, and every consumer here
    /// polls anyway: the state machine applies on demand and a follower
    /// catches up on a timer.
    fn subscribe(&self, after: LogIndex) -> impl Future<Output = Result<Vec<LogEntry>>> + Send;

    /// Confirms this node is still leader after processing its current
    /// consensus work, and returns the command index visible at that point.
    ///
    /// Leader-facing reads apply through this index and confirm a second
    /// barrier before answering. A plain `is_leader` check is insufficient:
    /// election can become visible before the state machine has applied the
    /// committed prefix inherited from the previous leader.
    fn leader_barrier(&self) -> impl Future<Output = Result<LogIndex>> + Send;

    /// Whether this node may propose. Always true for a single-node log.
    fn is_leader(&self) -> impl Future<Output = bool> + Send;

    /// Who to redirect a proposal to, when this node is not the leader.
    fn leader(&self) -> impl Future<Output = Option<NodeId>> + Send;

    /// The applied Raft voters, which are authoritative over node roles.
    fn voters(&self) -> impl Future<Output = Vec<NodeId>> + Send {
        async { Vec::new() }
    }

    /// The applied non-voting members available for safe promotion.
    fn learners(&self) -> impl Future<Output = Vec<NodeId>> + Send {
        async { Vec::new() }
    }

    /// Whether a learner has replicated the leader's current log.
    fn learner_caught_up(&self, _node: NodeId) -> impl Future<Output = bool> + Send {
        async { false }
    }

    /// Applies one configuration change and resolves after it is committed.
    fn change_membership(
        &self,
        change: MembershipChange,
    ) -> impl Future<Output = Result<()>> + Send {
        async move {
            Err(Error::InvalidArgument(format!(
                "this consensus log cannot apply membership change {change:?}"
            )))
        }
    }

    /// Whether this implementation hosts a mutable Raft configuration.
    fn manages_membership(&self) -> bool {
        false
    }
}

const FRAME_HEADER_BYTES: usize = 8;
const LOG_PATH: &str = "control/log";

/// A consensus log for a cluster of one.
///
/// Durability comes from `orbita_runtime::Disk`, so a restart replays exactly
/// what was fsynced and nothing else, and the simulator can inject torn tails
/// and lying fsyncs into it like any other file.
///
/// Entries are also held in memory. The control plane's log is metadata
/// decisions rather than data, so it is small enough that reading it back from
/// disk on every catch-up would be work for no benefit. That also means there
/// is no snapshot mechanism yet: recovery replays the whole log. It is the
/// right shape to add one to, and it is not needed until a cluster has
/// accumulated millions of decisions.
pub struct SingleNodeLog<R: Runtime> {
    node: NodeId,
    inner: Mutex<Inner<R>>,
}

struct Inner<R: Runtime> {
    file: <R::Disk as Disk>::File,
    entries: Vec<ControlCommand>,
    /// How far into the file the last entry that is definitely durable ends.
    ///
    /// Kept so that a failed append can be rolled back. A partial write leaves
    /// bytes in the file that no later reader could place, and appending the
    /// next entry after them would bury every entry that followed behind
    /// garbage the recovery scan stops at.
    durable_bytes: u64,
}

impl<R: Runtime> SingleNodeLog<R> {
    /// Opens the log, replaying whatever survived the last shutdown.
    ///
    /// A torn tail is truncated rather than treated as an error, because dying
    /// partway through an append is the expected way a process ends. Anything
    /// in the torn region was never reported as committed to anybody.
    pub async fn open(runtime: &R) -> Result<Arc<Self>> {
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
                // A read that fails its checksum means the log is damaged from
                // that point on, and recovery treats damage as truncation.
                Err(DiskError::Corrupt { .. }) => Bytes::new(),
                Err(e) => return Err(disk_error(e)),
            }
        };

        let recovered = decode_log(&raw);
        if let Some(error) = &recovered.unreadable {
            return Err(unreadable_entry(recovered.good_bytes, raw.len(), error));
        }
        let good_bytes = recovered.good_bytes;
        if good_bytes < raw.len() {
            tracing::warn!(
                node = %runtime.transport().local_node(),
                dropped = raw.len() - good_bytes,
                "control log had a torn tail; truncating"
            );
            file.truncate(good_bytes as u64).await.map_err(disk_error)?;
            file.sync().await.map_err(disk_error)?;
        }
        let entries = recovered.entries;

        Ok(Arc::new(Self {
            node: runtime.transport().local_node(),
            inner: Mutex::new(Inner {
                file,
                entries,
                durable_bytes: good_bytes as u64,
            }),
        }))
    }
}

impl<R: Runtime> ConsensusLog for SingleNodeLog<R> {
    async fn propose(&self, command: ControlCommand) -> Result<LogIndex> {
        let mut inner = self.inner.lock().await;
        let frame = encode_frame(&command.encode());
        let len = frame.len() as u64;

        // Committed means durable. Returning before the fsync would let the
        // caller act on a decision that a power cut could still take back.
        let written = async {
            inner.file.append(frame).await?;
            inner.file.sync().await
        }
        .await;

        if let Err(e) = written {
            // Roll the file back to the last entry that is known good. The
            // caller is told the proposal failed and will retry, and the retry
            // has to land somewhere a reader can find it.
            let _ = inner.file.truncate(inner.durable_bytes).await;
            return Err(disk_error(e));
        }

        inner.durable_bytes += len;
        inner.entries.push(command);
        Ok(inner.entries.len() as LogIndex)
    }

    async fn commit_index(&self) -> LogIndex {
        self.inner.lock().await.entries.len() as LogIndex
    }

    async fn subscribe(&self, after: LogIndex) -> Result<Vec<LogEntry>> {
        let inner = self.inner.lock().await;
        let start = after as usize;
        if start >= inner.entries.len() {
            return Ok(Vec::new());
        }
        Ok(inner.entries[start..]
            .iter()
            .enumerate()
            .map(|(offset, command)| LogEntry {
                index: (start + offset + 1) as LogIndex,
                command: command.clone(),
            })
            .collect())
    }

    async fn leader_barrier(&self) -> Result<LogIndex> {
        Ok(self.commit_index().await)
    }

    async fn is_leader(&self) -> bool {
        true
    }

    async fn leader(&self) -> Option<NodeId> {
        Some(self.node)
    }

    async fn voters(&self) -> Vec<NodeId> {
        vec![self.node]
    }

    async fn learners(&self) -> Vec<NodeId> {
        Vec::new()
    }

    async fn learner_caught_up(&self, _node: NodeId) -> bool {
        false
    }

    async fn change_membership(&self, change: MembershipChange) -> Result<()> {
        Err(Error::InvalidArgument(format!(
            "a single-node development log cannot apply membership change {change:?}"
        )))
    }
}

/// `len u32 | crc32 u32 | payload`.
///
/// The checksum is per entry rather than per file so that a crash midway
/// through an append costs the entry being written and not everything before
/// it, which is the same reasoning the WAL's framing uses.
fn encode_frame(payload: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(FRAME_HEADER_BYTES + payload.len());
    buf.put_u32_le(payload.len() as u32);
    buf.put_u32_le(crc32fast::hash(payload));
    buf.put_slice(payload);
    buf.freeze()
}

/// What replaying the durable log produced, and why it stopped.
struct RecoveredLog {
    entries: Vec<ControlCommand>,
    /// How many bytes the entries came from. Anything past this is a tail the
    /// caller may truncate.
    good_bytes: usize,
    /// Set when reading stopped at a frame that was *whole* and passed its
    /// checksum, and whose payload this binary still could not decode.
    ///
    /// This is the case that must never be confused with a torn tail. A torn
    /// frame is a process that died mid-append and it was never acknowledged
    /// to anybody, so dropping it costs nothing. A whole, checksummed frame is
    /// bytes some binary deliberately wrote and fsynced, and truncating there
    /// throws away that decision *and every decision after it* — which is how
    /// a partition disappears from the routing map and the id counter walks
    /// backwards at the same time. See issue #171.
    unreadable: Option<CodecError>,
}

/// Decodes as far as the bytes are trustworthy.
fn decode_log(raw: &[u8]) -> RecoveredLog {
    let mut entries = Vec::new();
    let mut unreadable = None;
    let mut offset = 0;

    while offset + FRAME_HEADER_BYTES <= raw.len() {
        let len = u32::from_le_bytes([
            raw[offset],
            raw[offset + 1],
            raw[offset + 2],
            raw[offset + 3],
        ]) as usize;
        let crc = u32::from_le_bytes([
            raw[offset + 4],
            raw[offset + 5],
            raw[offset + 6],
            raw[offset + 7],
        ]);
        let body_start = offset + FRAME_HEADER_BYTES;
        let Some(body_end) = body_start.checked_add(len).filter(|e| *e <= raw.len()) else {
            break;
        };
        let body = &raw[body_start..body_end];
        if crc32fast::hash(body) != crc {
            break;
        }
        match ControlCommand::decode(body) {
            Ok(command) => entries.push(command),
            // The checksum passed and the bytes still did not parse, which
            // means a binary that understood this entry wrote it and this one
            // does not. Stopping here is the only safe answer: applying the
            // entries after it would skip a decision every other member made.
            // Reported rather than silently truncated, because the frame is
            // whole and so is everything behind it.
            Err(error) => {
                unreadable = Some(error);
                break;
            }
        }
        offset = body_end;
    }

    RecoveredLog {
        entries,
        good_bytes: offset,
        unreadable,
    }
}

/// The refusal recovery gives rather than destroying a whole, checksummed
/// entry to carry on.
///
/// Loud and fatal on purpose. The alternative that used to be here — truncate
/// and start anyway — produces a node that looks healthy while serving a
/// routing map rolled back to some earlier prefix, which is indistinguishable
/// from working until an acknowledged write turns out to be unreachable. A
/// process that will not start is a page; a process that quietly forgot is a
/// data-loss incident nobody notices for a day.
fn unreadable_entry(at: usize, total: usize, error: &CodecError) -> Error {
    Error::Internal(format!(
        "control log: the entry at byte {at} is whole, passes its checksum, and this binary \
         cannot decode it ({error}). Refusing to start: truncating there would destroy {} bytes \
         of committed decisions. A binary that understands this entry wrote it — run one.",
        total - at
    ))
}

fn disk_error(e: DiskError) -> Error {
    Error::Internal(format!("control log: {e}"))
}

impl From<CodecError> for Error {
    fn from(e: CodecError) -> Self {
        Error::Internal(format!("control message: {e}"))
    }
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
            ready: true,
            draining: false,
        }
    }

    #[test]
    fn entries_come_back_at_the_indices_they_were_committed_at() {
        let sim = Simulation::new(1);
        let runtime = sim.add_node(NodeId(1));
        sim.block_on(async move {
            let log = SingleNodeLog::open(&runtime).await.unwrap();
            assert_eq!(log.propose(register(1)).await.unwrap(), 1);
            assert_eq!(log.propose(register(2)).await.unwrap(), 2);
            assert_eq!(log.commit_index().await, 2);

            let all = log.subscribe(0).await.unwrap();
            assert_eq!(all.len(), 2);
            assert_eq!(all[0].index, 1);
            assert_eq!(all[1].command, register(2));
            assert!(log.subscribe(2).await.unwrap().is_empty(), "nothing after");
        });
    }

    #[test]
    fn a_reopened_log_replays_everything_that_was_committed() {
        let sim = Simulation::new(2);
        let runtime = sim.add_node(NodeId(1));
        let first = runtime.clone();
        sim.block_on(async move {
            let log = SingleNodeLog::open(&first).await.unwrap();
            for id in 1..=5 {
                log.propose(register(id)).await.unwrap();
            }
        });

        let second = sim.runtime(NodeId(1));
        sim.block_on(async move {
            let log = SingleNodeLog::open(&second).await.unwrap();
            assert_eq!(log.commit_index().await, 5);
            assert_eq!(log.subscribe(0).await.unwrap().len(), 5);
        });
    }

    #[test]
    fn a_torn_tail_costs_the_entry_being_written_and_nothing_before_it() {
        let mut entries = BytesMut::new();
        entries.put_slice(&encode_frame(&register(1).encode()));
        entries.put_slice(&encode_frame(&register(2).encode()));
        let whole = entries.freeze();

        for cut in 1..whole.len() {
            let recovered = decode_log(&whole[..cut]);
            let (decoded, good) = (recovered.entries, recovered.good_bytes);
            assert!(good <= cut);
            assert!(
                decoded.len() <= 2,
                "a truncated log must never yield more entries than were written"
            );
            assert!(
                recovered.unreadable.is_none(),
                "a frame cut short is a torn tail, not an entry this binary cannot read"
            );
            if cut >= good && good > 0 {
                assert_eq!(decoded[0], register(1), "the intact prefix survives");
            }
        }
    }

    #[test]
    fn a_flipped_bit_stops_recovery_at_the_damaged_entry() {
        let mut entries = BytesMut::new();
        entries.put_slice(&encode_frame(&register(1).encode()));
        entries.put_slice(&encode_frame(&register(2).encode()));
        let mut raw = entries.to_vec();
        let last = raw.len() - 1;
        raw[last] ^= 0xFF;

        assert_eq!(decode_log(&raw).entries, vec![register(1)]);
    }

    #[test]
    fn recovery_refuses_to_start_rather_than_truncate_an_entry_it_cannot_decode() {
        // The upgrade shape from issue #171: a whole, checksummed entry a
        // newer binary wrote, sitting in front of decisions this one already
        // committed. Truncating there would have taken the tail with it and
        // brought the node up serving a rolled-back map.
        let sim = Simulation::new(4);
        let runtime = sim.add_node(NodeId(1));
        let first = runtime.clone();
        sim.block_on(async move {
            let log = SingleNodeLog::open(&first).await.unwrap();
            for id in 1..=3 {
                log.propose(register(id)).await.unwrap();
            }
        });

        let wedging = sim.runtime(NodeId(1));
        let before = sim.block_on(async move {
            let file = wedging
                .disk()
                .open(LOG_PATH, OpenOptions::create())
                .await
                .unwrap();
            file.append(encode_frame(&[200, 1, 2, 3])).await.unwrap();
            file.sync().await.unwrap();
            file.size().await.unwrap()
        });

        let reopening = sim.runtime(NodeId(1));
        let error = sim
            .block_on(async move { SingleNodeLog::open(&reopening).await })
            .err()
            .expect("an undecodable committed entry is fatal, not a torn tail");
        assert!(error.to_string().contains("Refusing to start"), "{error}");

        let checking = sim.runtime(NodeId(1));
        let after = sim.block_on(async move {
            let file = checking
                .disk()
                .open(LOG_PATH, OpenOptions::create())
                .await
                .unwrap();
            file.size().await.unwrap()
        });
        assert_eq!(
            after, before,
            "a refusal must leave the bytes alone so a binary that can read them still can"
        );
    }
}
