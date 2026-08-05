//! Forwarding a client request to the node that owns the key.
//!
//! Clients are dumb by design: they connect to any worker and never learn that
//! partitions exist. A worker that is not the owner therefore forwards, and
//! this is the envelope it forwards in.
//!
//! The request on the wire is the client's own protobuf message, re-encoded
//! rather than translated. Translating would mean a second description of
//! every field, and the first thing to rot would be the one nobody looks at.
//!
//! The reply carries either the protobuf response or an `orbita_core::Error`,
//! because the distinction between "the owner said no" and "the network ate
//! it" is what the origin node needs in order to decide whether to repair its
//! map and try again.
//!
//! # The lease heartbeat travels here too
//!
//! ADR 0001 has the owner renew its replicas' read leases on a heartbeat. That
//! is worker-to-worker traffic that belongs to no other subsystem, and
//! `ServiceId` is fixed by the runtime contract, so it rides on this service
//! under its own method rather than earning a service of its own.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use orbita_core::{Epoch, Error, Lamport, NodeId, PartitionId, Result, Version};

pub(crate) const METHOD_GET: u16 = 1;
pub(crate) const METHOD_SET: u16 = 2;
pub(crate) const METHOD_DELETE: u16 = 3;
pub(crate) const METHOD_LIST: u16 = 4;
pub(crate) const METHOD_LEASE: u16 = 5;

const TAG_OK: u8 = 0;
const TAG_ERROR: u8 = 1;

const CODE_NOT_FOUND: u16 = 1;
const CODE_ALREADY_EXISTS: u16 = 2;
const CODE_VERSION_MISMATCH: u16 = 3;
const CODE_KEYSPACE_NOT_FOUND: u16 = 4;
const CODE_KEYSPACE_ALREADY_EXISTS: u16 = 5;
const CODE_NOT_OWNER: u16 = 6;
const CODE_STALE_EPOCH: u16 = 7;
const CODE_TOO_LARGE: u16 = 8;
const CODE_QUOTA_EXCEEDED: u16 = 9;
const CODE_UNAUTHENTICATED: u16 = 10;
const CODE_PERMISSION_DENIED: u16 = 11;
const CODE_UNAVAILABLE: u16 = 12;
const CODE_INVALID_ARGUMENT: u16 = 13;
const CODE_INTERNAL: u16 = 14;

/// One renewal of one replica's read lease.
///
/// `through` is where the owner's log stood when it sent this. A replica takes
/// the lease only if it holds everything up to there, which is what stops it
/// serving a key it has not yet been told is changing. A zero duration is a
/// probe: it takes no lease and says only that the replica is answering.
///
/// `committed` is how far the owner has acknowledged writes to clients. A
/// replica applies nothing beyond it, so its storage never holds a write that
/// no client was ever told about. Without that, a replica would serve a value
/// from a write still in flight, and a read that saw it followed by one that
/// did not is a history no order explains.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LeaseGrant {
    pub partition: PartitionId,
    pub epoch: Epoch,
    pub through: Lamport,
    pub committed: Lamport,
    pub duration_millis: u64,
}

impl LeaseGrant {
    pub(crate) fn encode(&self) -> Bytes {
        let mut out = BytesMut::with_capacity(40);
        out.put_u64(self.partition.get());
        out.put_u64(self.epoch.get());
        out.put_u64(self.through.get());
        out.put_u64(self.committed.get());
        out.put_u64(self.duration_millis);
        out.freeze()
    }

    pub(crate) fn decode(mut raw: &[u8]) -> Result<Self> {
        if raw.remaining() < 40 {
            return Err(Error::InvalidArgument("truncated lease grant".to_string()));
        }
        Ok(Self {
            partition: PartitionId(raw.get_u64()),
            epoch: Epoch(raw.get_u64()),
            through: Lamport(raw.get_u64()),
            committed: Lamport(raw.get_u64()),
            duration_millis: raw.get_u64(),
        })
    }
}

/// What a replica said when its owner offered it a lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LeaseReply {
    pub accepted: bool,
    /// How far the replica's own log reaches, when it said.
    ///
    /// This rides on the heartbeat rather than earning a call of its own
    /// because the owner needs it on exactly the cadence the heartbeat already
    /// runs at, and for exactly the nodes it already reaches. It is what lets
    /// an owner that has just opened its log work out which replicas it can
    /// still catch up without replicating anything first. `None` from a peer
    /// too old to send it, which the owner treats as not knowing rather than
    /// as good news.
    pub durable: Option<Lamport>,
}

/// Encodes what the replica said, in the same envelope as every other reply so
/// that a refusal and a failure stay distinguishable.
///
/// The Lamport is appended after the acceptance bit, so a peer that stops
/// reading where the old reply ended is unaffected and a peer that never sends
/// it is read as having said nothing. See [`decode_lease_reply`].
pub(crate) fn encode_lease_reply(reply: LeaseReply) -> Bytes {
    let mut out = BytesMut::with_capacity(10);
    out.put_u8(TAG_OK);
    out.put_u8(u8::from(reply.accepted));
    if let Some(durable) = reply.durable {
        out.put_u64(durable.get());
    }
    out.freeze()
}

pub(crate) fn decode_lease_reply(raw: &[u8]) -> Result<LeaseReply> {
    let mut buf = raw;
    if buf.remaining() < 1 {
        return Err(Error::Internal("empty lease reply".to_string()));
    }
    match buf.get_u8() {
        TAG_OK if buf.remaining() >= 1 => {
            let accepted = buf.get_u8() != 0;
            // Absent from a peer running the release before this field
            // existed. Tolerated here rather than versioned because the two
            // shapes cannot be confused: one ends, the other carries eight
            // more bytes.
            let durable = (buf.remaining() >= 8).then(|| Lamport(buf.get_u64()));
            Ok(LeaseReply { accepted, durable })
        }
        TAG_OK => Err(Error::Internal("truncated lease reply".to_string())),
        TAG_ERROR => Err(decode_error(buf)?),
        other => Err(Error::Internal(format!("unknown proxy tag {other}"))),
    }
}

/// Encodes a successful response body.
pub(crate) fn encode_ok(body: &impl prost::Message) -> Bytes {
    let mut out = BytesMut::with_capacity(body.encoded_len() + 1);
    out.put_u8(TAG_OK);
    body.encode(&mut out).expect("a BytesMut never runs out");
    out.freeze()
}

/// Encodes a failure, keeping enough of the error for the origin node to act
/// on it rather than only to log it.
pub(crate) fn encode_error(error: &Error) -> Bytes {
    let (code, a, b, message) = match error {
        Error::NotFound => (CODE_NOT_FOUND, 0, 0, String::new()),
        Error::AlreadyExists => (CODE_ALREADY_EXISTS, 0, 0, String::new()),
        Error::VersionMismatch { expected, actual } => (
            CODE_VERSION_MISMATCH,
            expected.get(),
            // Zero means the key was absent, so a real version is stored one
            // higher. Versions start at one, so nothing is lost.
            actual.map_or(0, |v| v.get() + 1),
            String::new(),
        ),
        Error::KeyspaceNotFound => (CODE_KEYSPACE_NOT_FOUND, 0, 0, String::new()),
        Error::KeyspaceAlreadyExists => (CODE_KEYSPACE_ALREADY_EXISTS, 0, 0, String::new()),
        Error::NotOwner { partition, owner } => (
            CODE_NOT_OWNER,
            partition.get(),
            owner.map_or(0, |n| n.get() + 1),
            String::new(),
        ),
        Error::StaleEpoch {
            partition,
            got,
            current,
        } => (
            CODE_STALE_EPOCH,
            partition.get(),
            current.get(),
            got.to_string(),
        ),
        Error::TooLarge { what, size, limit } => (
            CODE_TOO_LARGE,
            *size as u64,
            *limit as u64,
            (*what).to_string(),
        ),
        Error::QuotaExceeded(m) => (CODE_QUOTA_EXCEEDED, 0, 0, m.clone()),
        Error::Unauthenticated => (CODE_UNAUTHENTICATED, 0, 0, String::new()),
        Error::PermissionDenied => (CODE_PERMISSION_DENIED, 0, 0, String::new()),
        Error::Unavailable(m) => (CODE_UNAVAILABLE, 0, 0, m.clone()),
        // A control plane redirect that surfaces on the data path is not
        // something the origin node can route around, so it degrades to the
        // retryable code rather than earning a wire code of its own.
        Error::NotLeader { leader } => (
            CODE_UNAVAILABLE,
            0,
            0,
            format!("not the leader; leader is {leader:?}"),
        ),
        Error::InvalidArgument(m) => (CODE_INVALID_ARGUMENT, 0, 0, m.clone()),
        Error::Internal(m) => (CODE_INTERNAL, 0, 0, m.clone()),
    };

    let mut out = BytesMut::with_capacity(19 + message.len());
    out.put_u8(TAG_ERROR);
    out.put_u16(code);
    out.put_u64(a);
    out.put_u64(b);
    out.put_slice(message.as_bytes());
    out.freeze()
}

/// Decodes a reply into the response the origin node will hand its client.
pub(crate) fn decode_reply<M: prost::Message + Default>(raw: &[u8]) -> Result<M> {
    let mut buf = raw;
    if buf.remaining() < 1 {
        return Err(Error::Internal("empty proxy reply".to_string()));
    }
    match buf.get_u8() {
        TAG_OK => M::decode(buf).map_err(|e| Error::Internal(format!("undecodable reply: {e}"))),
        TAG_ERROR => Err(decode_error(buf)?),
        other => Err(Error::Internal(format!("unknown proxy tag {other}"))),
    }
}

fn decode_error(mut buf: &[u8]) -> Result<Error> {
    if buf.remaining() < 18 {
        return Err(Error::Internal("truncated proxy error".to_string()));
    }
    let code = buf.get_u16();
    let a = buf.get_u64();
    let b = buf.get_u64();
    let message = String::from_utf8_lossy(buf).into_owned();

    Ok(match code {
        CODE_NOT_FOUND => Error::NotFound,
        CODE_ALREADY_EXISTS => Error::AlreadyExists,
        CODE_VERSION_MISMATCH => Error::VersionMismatch {
            expected: Version(a),
            actual: (b > 0).then(|| Version(b - 1)),
        },
        CODE_KEYSPACE_NOT_FOUND => Error::KeyspaceNotFound,
        CODE_KEYSPACE_ALREADY_EXISTS => Error::KeyspaceAlreadyExists,
        CODE_NOT_OWNER => Error::NotOwner {
            partition: PartitionId(a),
            owner: (b > 0).then(|| NodeId(b - 1)),
        },
        CODE_STALE_EPOCH => Error::StaleEpoch {
            partition: PartitionId(a),
            got: Epoch(message.parse().unwrap_or_default()),
            current: Epoch(b),
        },
        CODE_TOO_LARGE => Error::TooLarge {
            // `what` is a static string on the error, so it is narrowed back to
            // one of the things that can actually be too large.
            what: match message.as_str() {
                "key" => "key",
                "prefix" => "prefix",
                _ => "value",
            },
            size: a as usize,
            limit: b as usize,
        },
        CODE_QUOTA_EXCEEDED => Error::QuotaExceeded(message),
        CODE_UNAUTHENTICATED => Error::Unauthenticated,
        CODE_PERMISSION_DENIED => Error::PermissionDenied,
        CODE_UNAVAILABLE => Error::Unavailable(message),
        CODE_INVALID_ARGUMENT => Error::InvalidArgument(message),
        CODE_INTERNAL => Error::Internal(message),
        other => Error::Internal(format!("unknown proxy error code {other}")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use orbita_proto::v1::GetResponse;

    fn round_trip(error: &Error) -> Error {
        decode_reply::<GetResponse>(&encode_error(error)).expect_err("an error stays an error")
    }

    #[test]
    fn a_response_survives_the_hop() {
        let response = GetResponse {
            found: true,
            value: b"v".to_vec(),
            version: 12,
            expires_at_millis: Some(99),
        };
        assert_eq!(
            decode_reply::<GetResponse>(&encode_ok(&response)).unwrap(),
            response
        );
    }

    #[test]
    fn not_owner_keeps_the_owner_so_the_map_can_be_repaired() {
        // Without the owner the origin node would have to guess, and a request
        // for a key whose partition just moved has to succeed without the
        // client noticing anything beyond latency.
        let error = Error::NotOwner {
            partition: PartitionId(3),
            owner: Some(NodeId(7)),
        };
        assert_eq!(round_trip(&error), error);
    }

    #[test]
    fn a_version_mismatch_keeps_the_absence_of_a_key_distinct_from_version_zero() {
        for actual in [None, Some(Version(0)), Some(Version(9))] {
            let error = Error::VersionMismatch {
                expected: Version(4),
                actual,
            };
            assert_eq!(round_trip(&error), error);
        }
    }

    #[test]
    fn every_error_variant_survives_the_hop() {
        for error in [
            Error::NotFound,
            Error::AlreadyExists,
            Error::KeyspaceNotFound,
            Error::KeyspaceAlreadyExists,
            Error::TooLarge {
                what: "key",
                size: 11,
                limit: 10,
            },
            Error::QuotaExceeded("writes".into()),
            Error::Unauthenticated,
            Error::PermissionDenied,
            Error::Unavailable("mid failover".into()),
            Error::InvalidArgument("bad cursor".into()),
            Error::Internal("boom".into()),
            Error::StaleEpoch {
                partition: PartitionId(2),
                got: Epoch(4),
                current: Epoch(5),
            },
        ] {
            assert_eq!(round_trip(&error), error, "{error}");
        }
    }

    #[test]
    fn a_lease_grant_survives_the_hop() {
        let grant = LeaseGrant {
            partition: PartitionId(4),
            epoch: Epoch(9),
            through: Lamport(1234),
            committed: Lamport(1200),
            duration_millis: 500,
        };
        assert_eq!(LeaseGrant::decode(&grant.encode()).unwrap(), grant);
    }

    #[test]
    fn a_refused_lease_is_distinguishable_from_a_failed_one() {
        // The owner acts on the difference: a refusal means nothing there can
        // serve a stale read, and a failure means it has to assume otherwise.
        for accepted in [false, true] {
            let reply = LeaseReply {
                accepted,
                durable: Some(Lamport(41)),
            };
            assert_eq!(
                decode_lease_reply(&encode_lease_reply(reply)).unwrap(),
                reply
            );
        }
        assert!(decode_lease_reply(&encode_error(&Error::NotFound)).is_err());
    }

    #[test]
    fn a_lease_reply_without_a_log_position_is_read_as_saying_nothing_about_one() {
        // What a peer running the release before the position existed sends.
        // Reading its silence as a position would have the owner conclude
        // something about a replica that told it nothing.
        let older = encode_lease_reply(LeaseReply {
            accepted: true,
            durable: None,
        });
        assert_eq!(
            decode_lease_reply(&older).unwrap(),
            LeaseReply {
                accepted: true,
                durable: None
            }
        );
        assert_eq!(older.len(), 2, "the older shape is the one that used to go");

        // And the other direction, which is the one a rolling update needs:
        // a peer running the older release reads the first two bytes and
        // stops, so the position it does not know about cannot confuse it.
        let newer = encode_lease_reply(LeaseReply {
            accepted: true,
            durable: Some(Lamport(41)),
        });
        assert_eq!(newer[..2], older[..], "the reply still starts where it did");
    }

    #[test]
    fn a_truncated_lease_grant_is_refused_rather_than_guessed_at() {
        assert!(LeaseGrant::decode(&[0; 39]).is_err());
        assert!(decode_lease_reply(&[]).is_err());
    }

    #[test]
    fn a_truncated_reply_is_an_internal_error_rather_than_a_panic() {
        assert!(matches!(
            decode_reply::<GetResponse>(&[]),
            Err(Error::Internal(_))
        ));
        assert!(matches!(
            decode_reply::<GetResponse>(&[TAG_ERROR, 0]),
            Err(Error::Internal(_))
        ));
    }
}
