//! One partition's log on one node's disk.
//!
//! This is the part of the crate both roles share. An owner appends batches to
//! it, a replica appends what the owner sent, and both recover from it after a
//! crash. It knows nothing about replication.
//!
//! Segments are whole files. Rolling over at a size bound is what makes
//! truncation cheap: once every entry in a segment is applied and its SSTs are
//! durable, the segment is one `remove` rather than a rewrite.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use orbita_core::{Epoch, Error, Lamport, PartitionId, Result};
use orbita_runtime::{Disk, DiskError, File, OpenOptions, Runtime};
use tokio::sync::Mutex;

use crate::format::{self, FrameError, LogRecord, WalEntry, SEGMENT_HEADER_BYTES};

/// The default size at which a segment is closed and a new one started.
pub const DEFAULT_SEGMENT_TARGET_BYTES: u64 = 64 * 1024 * 1024;

type FileOf<R> = <<R as Runtime>::Disk as Disk>::File;

/// Why recovery stopped trusting the log where it did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TruncationReason {
    /// A frame ran off the end of the file, which is what a crash partway
    /// through an append looks like.
    TornTail,
    /// The bytes are present and are not what was written.
    ChecksumMismatch,
    /// The checksum passed and the record still did not parse, or a segment
    /// header was not ours.
    Malformed,
    /// Lamports went backwards, which means two writers wrote to this log.
    LamportOutOfOrder,
}

/// Where the log stopped being trustworthy, and why.
///
/// Recovery reports this rather than failing, because a torn tail is the
/// expected outcome of a crash. Anything at or after this offset was never
/// acknowledged to a client, so dropping it loses nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Truncation {
    pub segment: u64,
    pub offset: u64,
    pub reason: TruncationReason,
}

/// What a node knows about itself after reading its log back.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RecoveryState {
    /// Entries after the last checkpoint, in Lamport order, ready to be
    /// replayed into the storage engine.
    pub entries: Vec<WalEntry>,
    /// The highest Lamport this node holds durably. The control plane compares
    /// this across replicas to decide who to promote.
    pub durable_lamport: Lamport,
    /// The highest Lamport already applied and made durable in SSTs.
    pub applied_through: Lamport,
    /// The highest epoch this node has ever accepted. A restarted node uses it
    /// to keep rejecting an owner it already fenced.
    pub epoch: Epoch,
    pub truncated: Option<Truncation>,
}

struct SegmentMeta {
    seq: u64,
    max_lamport: Lamport,
}

struct Inner<R: Runtime> {
    segments: Vec<SegmentMeta>,
    current: FileOf<R>,
    current_size: u64,
    next_seq: u64,
    durable: Lamport,
    applied_through: Lamport,
    epoch: Epoch,
}

/// The log for one partition on this node.
///
/// Held behind an `Arc` because the owner path and the inbound replication
/// handler are two callers of the same file, and only one of them may be
/// writing at a time.
pub struct PartitionLog<R: Runtime> {
    runtime: R,
    dir: String,
    partition: PartitionId,
    segment_target_bytes: u64,
    recovery: RecoveryState,
    inner: Mutex<Inner<R>>,
}

impl<R: Runtime> PartitionLog<R> {
    /// Opens the log, recovering it in the process.
    ///
    /// Recovery is not a separate optional step because appending to a log
    /// whose tail has not been examined would build durable state on top of a
    /// torn write.
    pub async fn open(
        runtime: R,
        dir: impl Into<String>,
        partition: PartitionId,
        segment_target_bytes: u64,
    ) -> Result<Arc<Self>> {
        let dir = dir.into();
        let scan = scan_directory(&runtime, &dir, partition).await?;

        let (current, current_size, next_seq, segments) = match scan.segments.last() {
            Some(last) => {
                let path = segment_path(&dir, last.seq);
                let file = open_file(&runtime, &path, OpenOptions::default()).await?;
                let size = file.size().await.map_err(disk_err)?;
                (file, size, last.seq + 1, scan.segments)
            }
            None => {
                let seq = scan.next_seq;
                let (file, size) = create_segment(&runtime, &dir, partition, seq).await?;
                (
                    file,
                    size,
                    seq + 1,
                    vec![SegmentMeta {
                        seq,
                        max_lamport: Lamport::ZERO,
                    }],
                )
            }
        };

        let recovery = RecoveryState {
            entries: scan.entries,
            durable_lamport: scan.durable,
            applied_through: scan.applied_through,
            epoch: scan.epoch,
            truncated: scan.truncated,
        };

        Ok(Arc::new(Self {
            runtime,
            dir,
            partition,
            segment_target_bytes,
            inner: Mutex::new(Inner {
                segments,
                current,
                current_size,
                next_seq,
                durable: recovery.durable_lamport,
                applied_through: recovery.applied_through,
                epoch: recovery.epoch,
            }),
            recovery,
        }))
    }

    /// Which partition this log holds, so a registry can key on it without
    /// the caller having to remember.
    pub fn partition(&self) -> PartitionId {
        self.partition
    }

    /// What the log looked like when it was opened. Kept rather than rescanned
    /// because a second scan would report a clean log and hide the fact that a
    /// tail was dropped.
    pub fn recovery(&self) -> &RecoveryState {
        &self.recovery
    }

    /// How far this node has durably logged. The control plane compares this
    /// across nodes when it picks who to promote.
    pub async fn durable_lamport(&self) -> Lamport {
        self.inner.lock().await.durable
    }

    /// How far the storage engine has applied, which is how much of the log
    /// is a candidate for truncation.
    pub async fn applied_through(&self) -> Lamport {
        self.inner.lock().await.applied_through
    }

    /// The highest epoch this node has accepted, which is what an append is
    /// fenced against.
    pub async fn epoch(&self) -> Epoch {
        self.inner.lock().await.epoch
    }

    /// Appends pre-framed records and fsyncs once for all of them.
    ///
    /// Taking already-encoded frames is what lets a replica store the owner's
    /// bytes verbatim, and what lets several concurrent commits share one
    /// fsync.
    pub(crate) async fn append_frames(&self, frames: &[Bytes], max_lamport: Lamport) -> Result<()> {
        if frames.is_empty() {
            return Ok(());
        }
        let mut buf = BytesMut::with_capacity(frames.iter().map(Bytes::len).sum());
        for frame in frames {
            buf.extend_from_slice(frame);
        }

        let mut inner = self.inner.lock().await;
        if inner.current_size >= self.segment_target_bytes {
            self.roll(&mut inner).await?;
        }
        inner.current.append(buf.freeze()).await.map_err(disk_err)?;
        inner.current.sync().await.map_err(disk_err)?;

        inner.current_size += frames.iter().map(Bytes::len).sum::<usize>() as u64;
        if max_lamport > inner.durable {
            inner.durable = max_lamport;
        }
        if let Some(last) = inner.segments.last_mut() {
            if max_lamport > last.max_lamport {
                last.max_lamport = max_lamport;
            }
        }
        Ok(())
    }

    /// Records that this node has accepted an owner at `epoch`.
    ///
    /// Durable before it is acted on, because a fence that a restart forgets
    /// is a fence that lets a deposed owner write again.
    pub(crate) async fn record_fence(&self, epoch: Epoch) -> Result<()> {
        let mut inner = self.inner.lock().await;
        if epoch <= inner.epoch {
            return Ok(());
        }
        write_record(&mut inner, &LogRecord::Fence { epoch }).await?;
        inner.epoch = epoch;
        Ok(())
    }

    /// Marks everything at or below `applied_through` as safe to drop, then
    /// drops the segments that hold only such entries.
    pub async fn checkpoint(&self, applied_through: Lamport) -> Result<()> {
        let mut inner = self.inner.lock().await;
        if applied_through > inner.durable {
            return Err(Error::InvalidArgument(format!(
                "cannot check point through {applied_through}, only {} is durable",
                inner.durable
            )));
        }
        write_record(&mut inner, &LogRecord::Checkpoint { applied_through }).await?;
        inner.applied_through = applied_through;

        // Only a prefix of segments can go: entries are ordered, so the first
        // segment holding an unapplied entry stops the sweep.
        let current_seq = inner.segments.last().map_or(0, |s| s.seq);
        let mut removable = Vec::new();
        for segment in &inner.segments {
            if segment.seq == current_seq || segment.max_lamport > applied_through {
                break;
            }
            removable.push(segment.seq);
        }
        for seq in &removable {
            self.runtime
                .disk()
                .remove(&segment_path(&self.dir, *seq))
                .await
                .map_err(disk_err)?;
        }
        inner.segments.retain(|s| !removable.contains(&s.seq));
        Ok(())
    }

    /// Reads back entries above `after`, for retransmitting to a replica that
    /// fell behind.
    ///
    /// Returns `None` when the entries needed are older than this node's
    /// checkpoint, which means the replica cannot be caught up from the log
    /// and needs a snapshot instead.
    ///
    /// Public because a node that restarts has to replay into its storage
    /// engine from wherever that engine got to, which is not something this
    /// crate can do for it.
    pub async fn entries_after(&self, after: Lamport) -> Result<Option<Vec<WalEntry>>> {
        let scan = scan_directory(&self.runtime, &self.dir, self.partition).await?;
        let entries: Vec<WalEntry> = scan
            .all_entries
            .into_iter()
            .filter(|e| e.lamport > after)
            .collect();
        match entries.first() {
            Some(first) if first.lamport != after.next() => Ok(None),
            None => Ok(None),
            _ => Ok(Some(entries)),
        }
    }

    /// Discards everything above `lamport`.
    ///
    /// This is how a node that diverged from the current owner gets back onto
    /// the owner's history. See the crate docs for why discarding is safe.
    pub(crate) async fn truncate_above(&self, lamport: Lamport) -> Result<()> {
        let mut inner = self.inner.lock().await;
        if inner.durable <= lamport {
            return Ok(());
        }

        let mut cut: Option<(usize, u64)> = None;
        for (index, segment) in inner.segments.iter().enumerate() {
            let path = segment_path(&self.dir, segment.seq);
            let file = open_file(&self.runtime, &path, OpenOptions::default()).await?;
            let size = file.size().await.map_err(disk_err)?;
            let bytes = read_all(&file, size).await?;
            if let Some(offset) = first_offset_above(&bytes, lamport) {
                cut = Some((index, offset));
                break;
            }
        }

        let Some((index, offset)) = cut else {
            return Ok(());
        };

        for segment in inner.segments.iter().skip(index + 1) {
            self.runtime
                .disk()
                .remove(&segment_path(&self.dir, segment.seq))
                .await
                .map_err(disk_err)?;
        }
        inner.segments.truncate(index + 1);

        let path = segment_path(&self.dir, inner.segments[index].seq);
        let file = open_file(&self.runtime, &path, OpenOptions::default()).await?;
        file.truncate(offset).await.map_err(disk_err)?;
        file.sync().await.map_err(disk_err)?;

        inner.current = file;
        inner.current_size = offset;
        inner.durable = lamport;
        if let Some(last) = inner.segments.last_mut() {
            last.max_lamport = lamport;
        }

        // Cutting the tail can take the fence and checkpoint records with it,
        // since they sit wherever they were written. Writing them again keeps
        // a restart from forgetting which owner this node accepted.
        let epoch = inner.epoch;
        let applied_through = inner.applied_through;
        if epoch > Epoch::ZERO {
            write_record(&mut inner, &LogRecord::Fence { epoch }).await?;
        }
        if applied_through > Lamport::ZERO {
            write_record(&mut inner, &LogRecord::Checkpoint { applied_through }).await?;
        }
        Ok(())
    }

    async fn roll(&self, inner: &mut Inner<R>) -> Result<()> {
        let seq = inner.next_seq;
        let (file, size) = create_segment(&self.runtime, &self.dir, self.partition, seq).await?;
        inner.next_seq = seq + 1;
        inner.current = file;
        inner.current_size = size;
        inner.segments.push(SegmentMeta {
            seq,
            max_lamport: inner.durable,
        });
        Ok(())
    }
}

/// Appends one record and makes it durable before returning.
async fn write_record<R: Runtime>(inner: &mut Inner<R>, record: &LogRecord) -> Result<()> {
    let frame = format::encode(record);
    let len = frame.len() as u64;
    inner.current.append(frame).await.map_err(disk_err)?;
    inner.current.sync().await.map_err(disk_err)?;
    inner.current_size += len;
    Ok(())
}

struct DirectoryScan {
    segments: Vec<SegmentMeta>,
    next_seq: u64,
    entries: Vec<WalEntry>,
    all_entries: Vec<WalEntry>,
    durable: Lamport,
    applied_through: Lamport,
    epoch: Epoch,
    truncated: Option<Truncation>,
}

/// Reads every segment in order, stopping at the first byte it cannot trust.
///
/// Stopping rather than skipping is deliberate. A log is a sequence, and an
/// entry after a hole cannot be replayed without the entry in the hole, so
/// anything past the damage is dropped rather than partially believed.
async fn scan_directory<R: Runtime>(
    runtime: &R,
    dir: &str,
    partition: PartitionId,
) -> Result<DirectoryScan> {
    let names = runtime.disk().list(dir).await.map_err(disk_err)?;
    let mut seqs: Vec<u64> = names.iter().filter_map(|n| parse_segment_name(n)).collect();
    seqs.sort_unstable();

    let next_seq = seqs.last().map_or(1, |s| s + 1);
    let mut scan = DirectoryScan {
        segments: Vec::new(),
        next_seq,
        entries: Vec::new(),
        all_entries: Vec::new(),
        durable: Lamport::ZERO,
        applied_through: Lamport::ZERO,
        epoch: Epoch::ZERO,
        truncated: None,
    };

    let mut stopped_at: Option<usize> = None;
    for (index, seq) in seqs.iter().enumerate() {
        let path = segment_path(dir, *seq);
        let file = open_file(runtime, &path, OpenOptions::default()).await?;
        let size = file.size().await.map_err(disk_err)?;
        let bytes = read_all(&file, size).await?;

        let outcome = scan_segment(&bytes, partition, scan.durable);
        scan.all_entries
            .extend(outcome.records.iter().filter_map(|r| match r {
                LogRecord::Entry(e) => Some(e.clone()),
                _ => None,
            }));
        for record in &outcome.records {
            match record {
                LogRecord::Entry(entry) => {
                    scan.durable = entry.lamport;
                    if entry.epoch > scan.epoch {
                        scan.epoch = entry.epoch;
                    }
                }
                LogRecord::Checkpoint { applied_through } => {
                    scan.applied_through = *applied_through;
                }
                LogRecord::Fence { epoch } => {
                    if *epoch > scan.epoch {
                        scan.epoch = *epoch;
                    }
                }
            }
        }
        scan.segments.push(SegmentMeta {
            seq: *seq,
            max_lamport: scan.durable,
        });

        if let Some(reason) = outcome.stopped {
            scan.truncated = Some(Truncation {
                segment: *seq,
                offset: outcome.valid_end,
                reason,
            });
            if outcome.valid_end < size {
                file.truncate(outcome.valid_end).await.map_err(disk_err)?;
                file.sync().await.map_err(disk_err)?;
            }
            stopped_at = Some(index);
            break;
        }
    }

    // Everything after the damage is unreachable, so it goes rather than
    // sitting on disk waiting to be misread by a future version.
    if let Some(index) = stopped_at {
        for seq in seqs.iter().skip(index + 1) {
            runtime
                .disk()
                .remove(&segment_path(dir, *seq))
                .await
                .map_err(disk_err)?;
        }
    }

    // A segment whose header did not survive holds nothing readable, so it is
    // removed outright rather than kept as an unusable file.
    if let Some(truncation) = scan.truncated {
        if truncation.offset == 0 {
            runtime
                .disk()
                .remove(&segment_path(dir, truncation.segment))
                .await
                .map_err(disk_err)?;
            scan.segments.retain(|s| s.seq != truncation.segment);
        }
    }

    scan.entries = scan
        .all_entries
        .iter()
        .filter(|e| e.lamport > scan.applied_through)
        .cloned()
        .collect();
    Ok(scan)
}

struct SegmentScan {
    records: Vec<LogRecord>,
    valid_end: u64,
    stopped: Option<TruncationReason>,
}

fn scan_segment(bytes: &[u8], partition: PartitionId, mut last_lamport: Lamport) -> SegmentScan {
    let mut out = SegmentScan {
        records: Vec::new(),
        valid_end: 0,
        stopped: None,
    };

    match format::parse_segment_header(bytes) {
        Ok(found) if found == partition => {}
        Ok(_) => {
            out.stopped = Some(TruncationReason::Malformed);
            return out;
        }
        Err(FrameError::Incomplete) => {
            out.stopped = Some(TruncationReason::TornTail);
            return out;
        }
        Err(_) => {
            out.stopped = Some(TruncationReason::Malformed);
            return out;
        }
    }

    let mut pos = SEGMENT_HEADER_BYTES;
    out.valid_end = pos as u64;
    while pos < bytes.len() {
        match format::decode(&bytes[pos..]) {
            Ok((record, used)) => {
                if let Some(lamport) = record.lamport() {
                    if lamport <= last_lamport {
                        out.stopped = Some(TruncationReason::LamportOutOfOrder);
                        break;
                    }
                    last_lamport = lamport;
                }
                pos += used;
                out.valid_end = pos as u64;
                out.records.push(record);
            }
            Err(FrameError::Incomplete) => {
                out.stopped = Some(TruncationReason::TornTail);
                break;
            }
            Err(FrameError::Checksum) => {
                out.stopped = Some(TruncationReason::ChecksumMismatch);
                break;
            }
            Err(FrameError::Malformed) => {
                out.stopped = Some(TruncationReason::Malformed);
                break;
            }
        }
    }
    out
}

/// The offset of the first record holding a Lamport above `lamport`, which is
/// where a divergent tail starts.
fn first_offset_above(bytes: &[u8], lamport: Lamport) -> Option<u64> {
    if format::parse_segment_header(bytes).is_err() {
        return None;
    }
    let mut pos = SEGMENT_HEADER_BYTES;
    while pos < bytes.len() {
        match format::decode(&bytes[pos..]) {
            Ok((record, used)) => {
                if record.lamport().is_some_and(|l| l > lamport) {
                    return Some(pos as u64);
                }
                pos += used;
            }
            Err(_) => return Some(pos as u64),
        }
    }
    None
}

async fn create_segment<R: Runtime>(
    runtime: &R,
    dir: &str,
    partition: PartitionId,
    seq: u64,
) -> Result<(FileOf<R>, u64)> {
    let path = segment_path(dir, seq);
    let file = open_file(runtime, &path, OpenOptions::create()).await?;
    let header = format::segment_header(partition);
    let len = header.len() as u64;
    file.append(header).await.map_err(disk_err)?;
    file.sync().await.map_err(disk_err)?;
    Ok((file, len))
}

async fn open_file<R: Runtime>(runtime: &R, path: &str, options: OpenOptions) -> Result<FileOf<R>> {
    runtime.disk().open(path, options).await.map_err(disk_err)
}

async fn read_all<F: File>(file: &F, size: u64) -> Result<Bytes> {
    if size == 0 {
        return Ok(Bytes::new());
    }
    file.read_at(0, size as usize).await.map_err(disk_err)
}

fn segment_path(dir: &str, seq: u64) -> String {
    format!("{dir}/{seq:012}.wal")
}

fn parse_segment_name(name: &str) -> Option<u64> {
    let stem = name.strip_suffix(".wal")?;
    if stem.len() != 12 || !stem.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    stem.parse().ok()
}

fn disk_err(e: DiskError) -> Error {
    match e {
        DiskError::Corrupt { offset } => {
            Error::Internal(format!("wal: corrupt bytes at offset {offset}"))
        }
        other => Error::Internal(format!("wal disk: {other}")),
    }
}
