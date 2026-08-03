//! The bytes a log record becomes, on disk and on the wire.
//!
//! The same framing is used for both, so a replica can write the frames it
//! received straight to its own log without re-encoding them. That makes the
//! checksum end to end: it is computed once by the owner, and every later
//! reader of those bytes checks the owner's number rather than one some
//! intermediate step recomputed.

use bytes::{BufMut, Bytes, BytesMut};
use orbita_core::{Epoch, Lamport, PartitionId, MAX_KEY_BYTES, MAX_VALUE_BYTES};

/// Marks a file as one of ours, so a stray file in the data directory is
/// rejected rather than parsed as garbage entries.
pub(crate) const SEGMENT_MAGIC: &[u8; 4] = b"OWAL";
pub(crate) const FORMAT_VERSION: u16 = 1;
pub(crate) const SEGMENT_HEADER_BYTES: usize = 16;
pub(crate) const FRAME_HEADER_BYTES: usize = 8;

/// The largest body we will ever believe a frame header. A corrupt length
/// field is the one field we cannot validate before trusting it, so it is
/// bounded by what a legal entry could possibly need.
pub(crate) const MAX_FRAME_BODY_BYTES: usize = MAX_KEY_BYTES + MAX_VALUE_BYTES + 128;

const KIND_ENTRY: u8 = 1;
const KIND_CHECKPOINT: u8 = 2;
const KIND_FENCE: u8 = 3;

const OP_PUT: u8 = 1;
const OP_DELETE: u8 = 2;

/// The mutation a log entry carries.
///
/// Expiry is absolute rather than a duration because replay happens at an
/// arbitrary later time, and a duration would quietly extend a key's life
/// every time the log was replayed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalOp {
    Put {
        key: Bytes,
        value: Bytes,
        expires_at_millis: Option<u64>,
    },
    Delete {
        key: Bytes,
        /// When the tombstone this delete leaves behind may be reclaimed.
        ///
        /// The owner decides it for the same reason it decides a put's expiry:
        /// a replica computing its own deadline from its own clock would keep
        /// the tombstone for a different length of time, and a lagging replica
        /// would keep it longest. That is invisible to clients today, since a
        /// tombstone and a reclaimed tombstone answer a read identically, but
        /// it is the same mistake as recomputing a TTL on a replica and it
        /// costs one field to not make.
        tombstone_expires_at_millis: Option<u64>,
    },
}

/// One replicated write.
///
/// The epoch travels with every entry so that a replica can fence a deposed
/// owner without holding any out-of-band state, and so that recovery can
/// reconstruct the highest epoch this node has seen from the log alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalEntry {
    pub lamport: Lamport,
    pub epoch: Epoch,
    pub partition: PartitionId,
    pub op: WalOp,
}

/// Everything that can appear in a segment.
///
/// Checkpoints and fences are records rather than a side file because the log
/// is the only thing this crate fsyncs, and a fact that is not in the log is a
/// fact that does not survive a crash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LogRecord {
    Entry(WalEntry),
    /// Everything at or below this Lamport is applied and its SSTs are
    /// durable, so the segments holding it can go.
    Checkpoint {
        applied_through: Lamport,
    },
    /// This node has accepted an owner at this epoch. Persisted so that a
    /// restart cannot forget a fence and start accepting a deposed owner
    /// again.
    Fence {
        epoch: Epoch,
    },
}

impl LogRecord {
    pub(crate) fn lamport(&self) -> Option<Lamport> {
        match self {
            LogRecord::Entry(e) => Some(e.lamport),
            _ => None,
        }
    }
}

/// Why a frame could not be turned back into a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FrameError {
    /// The buffer ran out mid-frame, which at the end of a file means a crash
    /// happened partway through an append.
    Incomplete,
    /// The bytes are there but they are not the bytes that were written.
    Checksum,
    /// The checksum passed and the contents still make no sense, which means
    /// a version skew or a bug rather than a bad disk.
    Malformed,
}

pub(crate) fn segment_header(partition: PartitionId) -> Bytes {
    let mut buf = BytesMut::with_capacity(SEGMENT_HEADER_BYTES);
    buf.put_slice(SEGMENT_MAGIC);
    buf.put_u16_le(FORMAT_VERSION);
    buf.put_u16_le(0);
    buf.put_u64_le(partition.get());
    buf.freeze()
}

pub(crate) fn parse_segment_header(buf: &[u8]) -> Result<PartitionId, FrameError> {
    if buf.len() < SEGMENT_HEADER_BYTES {
        return Err(FrameError::Incomplete);
    }
    if &buf[0..4] != SEGMENT_MAGIC {
        return Err(FrameError::Malformed);
    }
    let version = u16::from_le_bytes([buf[4], buf[5]]);
    if version != FORMAT_VERSION {
        return Err(FrameError::Malformed);
    }
    // The reserved word is checked rather than skipped so that every byte of
    // the header is covered by something. A header has no checksum of its own,
    // so validation is all it gets.
    if u16::from_le_bytes([buf[6], buf[7]]) != 0 {
        return Err(FrameError::Malformed);
    }
    let partition = u64::from_le_bytes(buf[8..16].try_into().expect("8 bytes"));
    Ok(PartitionId(partition))
}

/// Frames a record as `len | crc32(len ++ body) | body`.
///
/// The length is inside the checksum because it is the one field a reader has
/// to trust before it can check anything else.
pub(crate) fn encode(record: &LogRecord) -> Bytes {
    let mut body = BytesMut::new();
    match record {
        LogRecord::Entry(entry) => {
            body.put_u8(KIND_ENTRY);
            body.put_u64_le(entry.lamport.get());
            body.put_u64_le(entry.epoch.get());
            body.put_u64_le(entry.partition.get());
            match &entry.op {
                WalOp::Put {
                    key,
                    value,
                    expires_at_millis,
                } => {
                    body.put_u8(OP_PUT);
                    put_blob(&mut body, key);
                    put_blob(&mut body, value);
                    match expires_at_millis {
                        Some(at) => {
                            body.put_u8(1);
                            body.put_u64_le(*at);
                        }
                        None => body.put_u8(0),
                    }
                }
                WalOp::Delete {
                    key,
                    tombstone_expires_at_millis,
                } => {
                    body.put_u8(OP_DELETE);
                    put_blob(&mut body, key);
                    match tombstone_expires_at_millis {
                        Some(at) => {
                            body.put_u8(1);
                            body.put_u64_le(*at);
                        }
                        None => body.put_u8(0),
                    }
                }
            }
        }
        LogRecord::Checkpoint { applied_through } => {
            body.put_u8(KIND_CHECKPOINT);
            body.put_u64_le(applied_through.get());
        }
        LogRecord::Fence { epoch } => {
            body.put_u8(KIND_FENCE);
            body.put_u64_le(epoch.get());
        }
    }

    let len = u32::try_from(body.len()).expect("record bodies are bounded by entry limits");
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&len.to_le_bytes());
    hasher.update(&body);

    let mut out = BytesMut::with_capacity(FRAME_HEADER_BYTES + body.len());
    out.put_u32_le(len);
    out.put_u32_le(hasher.finalize());
    out.put_slice(&body);
    out.freeze()
}

/// Reads one frame from the front of `buf`, returning the record and how many
/// bytes it consumed.
pub(crate) fn decode(buf: &[u8]) -> Result<(LogRecord, usize), FrameError> {
    if buf.len() < FRAME_HEADER_BYTES {
        return Err(FrameError::Incomplete);
    }
    let len = u32::from_le_bytes(buf[0..4].try_into().expect("4 bytes")) as usize;
    let crc = u32::from_le_bytes(buf[4..8].try_into().expect("4 bytes"));
    if len == 0 || len > MAX_FRAME_BODY_BYTES {
        // Nothing legal is this size, so the header itself is damaged. There
        // is no point reading further: we cannot find the next frame boundary.
        return Err(FrameError::Checksum);
    }
    let end = FRAME_HEADER_BYTES + len;
    if buf.len() < end {
        return Err(FrameError::Incomplete);
    }
    let body = &buf[FRAME_HEADER_BYTES..end];

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&(len as u32).to_le_bytes());
    hasher.update(body);
    if hasher.finalize() != crc {
        return Err(FrameError::Checksum);
    }

    Ok((decode_body(body)?, end))
}

fn decode_body(body: &[u8]) -> Result<LogRecord, FrameError> {
    let mut r = Reader::new(body);
    let record = match r.u8()? {
        KIND_ENTRY => {
            let lamport = Lamport(r.u64()?);
            let epoch = Epoch(r.u64()?);
            let partition = PartitionId(r.u64()?);
            let op = match r.u8()? {
                OP_PUT => {
                    let key = r.blob()?;
                    let value = r.blob()?;
                    let expires_at_millis = match r.u8()? {
                        0 => None,
                        1 => Some(r.u64()?),
                        _ => return Err(FrameError::Malformed),
                    };
                    WalOp::Put {
                        key,
                        value,
                        expires_at_millis,
                    }
                }
                OP_DELETE => {
                    let key = r.blob()?;
                    let tombstone_expires_at_millis = match r.u8()? {
                        0 => None,
                        1 => Some(r.u64()?),
                        _ => return Err(FrameError::Malformed),
                    };
                    WalOp::Delete {
                        key,
                        tombstone_expires_at_millis,
                    }
                }
                _ => return Err(FrameError::Malformed),
            };
            LogRecord::Entry(WalEntry {
                lamport,
                epoch,
                partition,
                op,
            })
        }
        KIND_CHECKPOINT => LogRecord::Checkpoint {
            applied_through: Lamport(r.u64()?),
        },
        KIND_FENCE => LogRecord::Fence {
            epoch: Epoch(r.u64()?),
        },
        _ => return Err(FrameError::Malformed),
    };
    if r.remaining() != 0 {
        return Err(FrameError::Malformed);
    }
    Ok(record)
}

fn put_blob(buf: &mut BytesMut, blob: &[u8]) {
    buf.put_u32_le(
        u32::try_from(blob.len()).expect("blobs are bounded by the key and value limits"),
    );
    buf.put_slice(blob);
}

/// A bounds-checked cursor, so a malformed frame is an error rather than a
/// panic.
pub(crate) struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub(crate) fn position(&self) -> usize {
        self.pos
    }

    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8], FrameError> {
        if self.remaining() < n {
            return Err(FrameError::Malformed);
        }
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    pub(crate) fn u8(&mut self) -> Result<u8, FrameError> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn u32(&mut self) -> Result<u32, FrameError> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("4 bytes"),
        ))
    }

    pub(crate) fn u64(&mut self) -> Result<u64, FrameError> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("8 bytes"),
        ))
    }

    pub(crate) fn blob(&mut self) -> Result<Bytes, FrameError> {
        let len = self.u32()? as usize;
        Ok(Bytes::copy_from_slice(self.take(len)?))
    }

    pub(crate) fn string(&mut self) -> Result<String, FrameError> {
        let len = self.u32()? as usize;
        let bytes = self.take(len)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| FrameError::Malformed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(lamport: u64) -> LogRecord {
        LogRecord::Entry(WalEntry {
            lamport: Lamport(lamport),
            epoch: Epoch(3),
            partition: PartitionId(7),
            op: WalOp::Put {
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v"),
                expires_at_millis: Some(1234),
            },
        })
    }

    #[test]
    fn every_record_kind_round_trips() {
        for record in [
            entry(1),
            LogRecord::Entry(WalEntry {
                lamport: Lamport(2),
                epoch: Epoch::ZERO,
                partition: PartitionId(7),
                op: WalOp::Delete {
                    key: Bytes::from_static(b"gone"),
                    tombstone_expires_at_millis: Some(1_700_000_086_400_000),
                },
            }),
            LogRecord::Checkpoint {
                applied_through: Lamport(9),
            },
            LogRecord::Fence { epoch: Epoch(4) },
        ] {
            let frame = encode(&record);
            let (decoded, used) = decode(&frame).expect("round trip");
            assert_eq!(decoded, record);
            assert_eq!(used, frame.len(), "a frame must be fully consumed");
        }
    }

    #[test]
    fn a_frame_cut_anywhere_short_is_incomplete_not_corrupt() {
        let frame = encode(&entry(1));
        for cut in 0..frame.len() {
            assert_eq!(
                decode(&frame[..cut]),
                Err(FrameError::Incomplete),
                "a frame truncated at {cut} bytes is a torn write, not corruption"
            );
        }
    }

    #[test]
    fn flipping_any_byte_fails_the_checksum() {
        let frame = encode(&entry(1));
        for i in 0..frame.len() {
            let mut damaged = frame.to_vec();
            damaged[i] ^= 0x80;
            match decode(&damaged) {
                Err(FrameError::Checksum) => {}
                // Damaging the length field can also make the frame look
                // short, which recovery treats the same way: nothing after
                // this point is trusted.
                Err(FrameError::Incomplete) => {}
                other => panic!("byte {i} damaged but decode returned {other:?}"),
            }
        }
    }

    #[test]
    fn a_segment_header_from_another_partition_is_readable_but_identifiable() {
        let header = segment_header(PartitionId(12));
        assert_eq!(parse_segment_header(&header), Ok(PartitionId(12)));
    }

    #[test]
    fn a_foreign_file_is_not_mistaken_for_a_segment() {
        assert_eq!(
            parse_segment_header(b"not a wal segment at all"),
            Err(FrameError::Malformed)
        );
        assert_eq!(parse_segment_header(b"OWA"), Err(FrameError::Incomplete));
    }
}
