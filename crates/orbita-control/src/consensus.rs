//! The seam where Raft goes.
//!
//! # This is staging, not an oversight
//!
//! `docs/plan/03-control.md` says the leader group runs Raft, and it will.
//! What ships here is the replicated state machine, the failover protocol, the
//! partition map, and the admin surface, with consensus behind
//! [`ConsensusLog`] and a single-node implementation underneath it. That order
//! is deliberate. Everything above the trait is where the correctness argument
//! lives, and none of it gets easier to write or to test with a real Raft
//! underneath. A single-node log is a correct implementation of the trait for
//! a cluster of one, which is what `orbita dev` runs and what most of the
//! simulator scenarios need, so the staging buys a working system now without
//! costing anything later.
//!
//! What it does not do is survive the loss of the leader group node, and
//! nothing in this crate pretends otherwise. A single-node control plane is
//! not a production configuration.
//!
//! # How Raft slots in
//!
//! [`ConsensusLog`] is three operations: propose a command and learn when it
//! is committed, read the commit index, and read committed entries after a
//! given index. That is deliberately the intersection of what `openraft` and
//! `raft-rs` both offer.
//!
//! With `openraft`, `propose` becomes `Raft::client_write`, whose response
//! already carries the log index, and `subscribe` is served from the state
//! machine store's committed entries. `openraft`'s `RaftLogStorage` and
//! `RaftNetwork` are implemented against `orbita_runtime::Disk` and
//! `orbita_runtime::Transport`, which is the seam the brief calls out as the
//! reason to adopt rather than build. Nothing in [`crate::state`],
//! [`crate::controller`], or [`crate::client`] changes, because none of them
//! knows how a command became committed.
//!
//! The one thing that does change is that `propose` starts failing with
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

    /// Whether this node may propose. Always true for a single-node log.
    fn is_leader(&self) -> impl Future<Output = bool> + Send;

    /// Who to redirect a proposal to, when this node is not the leader.
    fn leader(&self) -> impl Future<Output = Option<NodeId>> + Send;
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

        let (entries, good_bytes) = decode_log(&raw);
        if good_bytes < raw.len() {
            tracing::warn!(
                node = %runtime.transport().local_node(),
                dropped = raw.len() - good_bytes,
                "control log had a torn tail; truncating"
            );
            file.truncate(good_bytes as u64).await.map_err(disk_error)?;
            file.sync().await.map_err(disk_error)?;
        }

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

    async fn is_leader(&self) -> bool {
        true
    }

    async fn leader(&self) -> Option<NodeId> {
        Some(self.node)
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

/// Decodes as far as the bytes are trustworthy, returning the entries and how
/// many bytes they came from.
fn decode_log(raw: &[u8]) -> (Vec<ControlCommand>, usize) {
    let mut entries = Vec::new();
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
            Err(_) => break,
        }
        offset = body_end;
    }

    (entries, offset)
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
            let (decoded, good) = decode_log(&whole[..cut]);
            assert!(good <= cut);
            assert!(
                decoded.len() <= 2,
                "a truncated log must never yield more entries than were written"
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

        let (decoded, _) = decode_log(&raw);
        assert_eq!(decoded, vec![register(1)]);
    }
}
