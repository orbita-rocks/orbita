//! The owner-to-replica protocol.
//!
//! Hand-rolled rather than generated, because `Transport` already carries
//! opaque bytes and the whole protocol is three methods over fixed-width
//! fields. A schema compiler would add a build dependency and a code
//! generation step to save about eighty lines.
//!
//! Everything is little endian. Entries travel in exactly the framing they
//! have on disk, so the replica writes the owner's bytes through unchanged.
//!
//! ```text
//! Append  (method 1)  partition u64 | epoch u64 | prev_lamport u64 | count u32 | count frames
//! Fence   (method 2)  partition u64 | epoch u64 | truncate_above u64
//! Status  (method 3)  partition u64
//!
//! Response            status u8
//!   0 Ok              durable_lamport u64 | epoch u64
//!   1 StaleEpoch      current_epoch u64
//!   2 Gap             durable_lamport u64 | epoch u64
//!   3 Error           len u32 | utf8
//! ```

use bytes::{BufMut, Bytes, BytesMut};
use orbita_core::{Epoch, Lamport, PartitionId};

use crate::format::{self, FrameError, LogRecord, Reader, WalEntry};

pub const METHOD_APPEND: u16 = 1;
pub const METHOD_FENCE: u16 = 2;
pub const METHOD_STATUS: u16 = 3;

const STATUS_OK: u8 = 0;
const STATUS_STALE_EPOCH: u8 = 1;
const STATUS_GAP: u8 = 2;
const STATUS_ERROR: u8 = 3;

/// A batch of entries to store, with the Lamport they must follow.
///
/// `prev_lamport` is what keeps replicas free of holes: a replica that is not
/// exactly at `prev_lamport` says so instead of writing a batch it cannot
/// place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AppendRequest {
    pub partition: PartitionId,
    pub epoch: Epoch,
    pub prev_lamport: Lamport,
    /// How far the owner has acknowledged writes to clients.
    ///
    /// Carried on the message that already exists rather than on a message of
    /// its own, for the same reason invalidation is: a replica needs to know
    /// which entries the cluster has committed to, and the owner is sending it
    /// entries anyway. A replica that applied on durability alone would hold a
    /// write nobody was ever told about, and could serve it.
    pub committed: Lamport,
    /// Entries paired with the exact frames they were decoded from.
    pub entries: Vec<(WalEntry, Bytes)>,
}

impl AppendRequest {
    pub(crate) fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u64_le(self.partition.get());
        buf.put_u64_le(self.epoch.get());
        buf.put_u64_le(self.prev_lamport.get());
        buf.put_u64_le(self.committed.get());
        buf.put_u32_le(self.entries.len() as u32);
        for (_, frame) in &self.entries {
            buf.put_slice(frame);
        }
        buf.freeze()
    }

    pub(crate) fn decode(buf: &Bytes) -> Result<Self, FrameError> {
        let mut r = Reader::new(buf);
        let partition = PartitionId(r.u64()?);
        let epoch = Epoch(r.u64()?);
        let prev_lamport = Lamport(r.u64()?);
        let committed = Lamport(r.u64()?);
        let count = r.u32()? as usize;

        let mut entries = Vec::with_capacity(count.min(1024));
        for _ in 0..count {
            let start = r.position();
            let (record, used) = format::decode(&buf[start..])?;
            let LogRecord::Entry(entry) = record else {
                return Err(FrameError::Malformed);
            };
            r.take(used)?;
            entries.push((entry, buf.slice(start..start + used)));
        }
        if r.remaining() != 0 {
            return Err(FrameError::Malformed);
        }
        Ok(Self {
            partition,
            epoch,
            prev_lamport,
            committed,
            entries,
        })
    }

    pub(crate) fn last_lamport(&self) -> Lamport {
        self.entries
            .last()
            .map_or(self.prev_lamport, |(e, _)| e.lamport)
    }
}

/// Tells a replica about a new owner, and where that owner's history ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FenceRequest {
    pub partition: PartitionId,
    pub epoch: Epoch,
    pub truncate_above: Lamport,
}

impl FenceRequest {
    pub(crate) fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(24);
        buf.put_u64_le(self.partition.get());
        buf.put_u64_le(self.epoch.get());
        buf.put_u64_le(self.truncate_above.get());
        buf.freeze()
    }

    pub(crate) fn decode(buf: &Bytes) -> Result<Self, FrameError> {
        let mut r = Reader::new(buf);
        Ok(Self {
            partition: PartitionId(r.u64()?),
            epoch: Epoch(r.u64()?),
            truncate_above: Lamport(r.u64()?),
        })
    }
}

/// Asks how far a node has durably logged, which is the input to the control
/// plane's promotion decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StatusRequest {
    pub partition: PartitionId,
}

impl StatusRequest {
    pub(crate) fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(8);
        buf.put_u64_le(self.partition.get());
        buf.freeze()
    }

    pub(crate) fn decode(buf: &Bytes) -> Result<Self, FrameError> {
        let mut r = Reader::new(buf);
        Ok(Self {
            partition: PartitionId(r.u64()?),
        })
    }
}

/// The one response shape every method shares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WalResponse {
    Ok {
        durable_lamport: Lamport,
        epoch: Epoch,
    },
    /// The caller has been fenced. Carrying the current epoch means a deposed
    /// owner learns it is deposed from the reply rather than from a timeout.
    StaleEpoch {
        current: Epoch,
    },
    /// The replica is behind the batch it was sent and needs the entries from
    /// `durable_lamport` onwards first.
    Gap {
        durable_lamport: Lamport,
        epoch: Epoch,
    },
    Error(String),
}

impl WalResponse {
    pub(crate) fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        match self {
            WalResponse::Ok {
                durable_lamport,
                epoch,
            } => {
                buf.put_u8(STATUS_OK);
                buf.put_u64_le(durable_lamport.get());
                buf.put_u64_le(epoch.get());
            }
            WalResponse::StaleEpoch { current } => {
                buf.put_u8(STATUS_STALE_EPOCH);
                buf.put_u64_le(current.get());
            }
            WalResponse::Gap {
                durable_lamport,
                epoch,
            } => {
                buf.put_u8(STATUS_GAP);
                buf.put_u64_le(durable_lamport.get());
                buf.put_u64_le(epoch.get());
            }
            WalResponse::Error(message) => {
                buf.put_u8(STATUS_ERROR);
                buf.put_u32_le(message.len() as u32);
                buf.put_slice(message.as_bytes());
            }
        }
        buf.freeze()
    }

    pub(crate) fn decode(buf: &Bytes) -> Result<Self, FrameError> {
        let mut r = Reader::new(buf);
        let response = match r.u8()? {
            STATUS_OK => WalResponse::Ok {
                durable_lamport: Lamport(r.u64()?),
                epoch: Epoch(r.u64()?),
            },
            STATUS_STALE_EPOCH => WalResponse::StaleEpoch {
                current: Epoch(r.u64()?),
            },
            STATUS_GAP => WalResponse::Gap {
                durable_lamport: Lamport(r.u64()?),
                epoch: Epoch(r.u64()?),
            },
            STATUS_ERROR => WalResponse::Error(r.string()?),
            _ => return Err(FrameError::Malformed),
        };
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::WalOp;

    fn entry(lamport: u64) -> (WalEntry, Bytes) {
        let entry = WalEntry {
            lamport: Lamport(lamport),
            epoch: Epoch(2),
            partition: PartitionId(1),
            op: WalOp::Delete {
                key: Bytes::from_static(b"k"),
                tombstone_expires_at_millis: None,
            },
        };
        let frame = format::encode(&LogRecord::Entry(entry.clone()));
        (entry, frame)
    }

    #[test]
    fn an_append_round_trips_with_its_frames_intact() {
        let request = AppendRequest {
            partition: PartitionId(1),
            epoch: Epoch(2),
            prev_lamport: Lamport(4),
            committed: Lamport(3),
            entries: vec![entry(5), entry(6)],
        };
        let decoded = AppendRequest::decode(&request.encode()).expect("round trip");
        assert_eq!(decoded, request, "the replica must see the owner's bytes");
    }

    #[test]
    fn a_corrupted_entry_on_the_wire_is_rejected_before_it_reaches_disk() {
        let request = AppendRequest {
            partition: PartitionId(1),
            epoch: Epoch(2),
            prev_lamport: Lamport(0),
            committed: Lamport::ZERO,
            entries: vec![entry(1)],
        };
        let mut bytes = request.encode().to_vec();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        assert!(AppendRequest::decode(&Bytes::from(bytes)).is_err());
    }

    #[test]
    fn every_response_round_trips() {
        for response in [
            WalResponse::Ok {
                durable_lamport: Lamport(9),
                epoch: Epoch(1),
            },
            WalResponse::StaleEpoch { current: Epoch(7) },
            WalResponse::Gap {
                durable_lamport: Lamport(3),
                epoch: Epoch(1),
            },
            WalResponse::Error("nope".into()),
        ] {
            assert_eq!(
                WalResponse::decode(&response.encode()),
                Ok(response.clone())
            );
        }
    }
}
