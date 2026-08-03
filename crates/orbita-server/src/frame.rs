//! The wire format peer traffic uses.
//!
//! This is [ADR 0004](../../../docs/adr/0004-peer-traffic-uses-private-framing.md)
//! made concrete. Peer traffic is private, versioned with the binary rather
//! than with a published contract, and already framed by the subsystem that
//! produced it, so it travels as an opaque payload behind a small header
//! instead of inside protobuf.
//!
//! ```text
//! request   length u32 | service u16 | method u16 | request_id u64 | payload
//! response  length u32 | request_id u64 | status u8 | payload
//! ```
//!
//! The length counts everything after itself, so a reader takes four bytes,
//! then takes exactly that many more, and never has to guess where a message
//! ends. The request id is what lets several calls share one connection, which
//! matters because an owner talks to its replicas constantly and a connection
//! per call would spend a handshake on every write.
//!
//! Responses carry a status byte rather than a string tag so that "nobody
//! serves this" stays distinguishable from "the handler refused", which is the
//! distinction the caller acts on.

use bytes::{BufMut, Bytes, BytesMut};
use orbita_runtime::ServiceId;

/// The largest frame this node will read.
///
/// A length prefix arriving from a peer is untrusted input until it has been
/// checked, and the failure mode of not checking it is a single garbled u32
/// turning into a multi-gigabyte allocation. Values are capped well below this
/// by the client API, so a frame near the limit is already a bug somewhere.
pub(crate) const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

/// The header bytes a request carries before its payload.
const REQUEST_HEADER: usize = 2 + 2 + 8;
/// The header bytes a response carries before its payload.
const RESPONSE_HEADER: usize = 8 + 1;

pub(crate) const STATUS_OK: u8 = 0;
pub(crate) const STATUS_NO_HANDLER: u8 = 1;
pub(crate) const STATUS_ERROR: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum FrameError {
    #[error("frame of {0} bytes exceeds the {MAX_FRAME_BYTES} byte limit")]
    TooLarge(usize),
    #[error("frame is shorter than its own header")]
    Truncated,
    #[error("unknown service {0}")]
    UnknownService(u16),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Request {
    pub service: ServiceId,
    pub method: u16,
    pub request_id: u64,
    pub payload: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Response {
    pub request_id: u64,
    pub status: u8,
    pub payload: Bytes,
}

impl Request {
    pub(crate) fn encode(&self) -> Bytes {
        let mut out = BytesMut::with_capacity(4 + REQUEST_HEADER + self.payload.len());
        out.put_u32((REQUEST_HEADER + self.payload.len()) as u32);
        out.put_u16(self.service as u16);
        out.put_u16(self.method);
        out.put_u64(self.request_id);
        out.put_slice(&self.payload);
        out.freeze()
    }

    /// Reads a request from a frame body, meaning the bytes after the length
    /// prefix that the reader has already consumed.
    pub(crate) fn decode(body: &[u8]) -> Result<Self, FrameError> {
        if body.len() < REQUEST_HEADER {
            return Err(FrameError::Truncated);
        }
        let service = u16::from_be_bytes([body[0], body[1]]);
        Ok(Self {
            service: service_of(service)?,
            method: u16::from_be_bytes([body[2], body[3]]),
            request_id: u64::from_be_bytes(body[4..12].try_into().expect("twelve bytes are there")),
            payload: Bytes::copy_from_slice(&body[REQUEST_HEADER..]),
        })
    }
}

impl Response {
    pub(crate) fn encode(&self) -> Bytes {
        let mut out = BytesMut::with_capacity(4 + RESPONSE_HEADER + self.payload.len());
        out.put_u32((RESPONSE_HEADER + self.payload.len()) as u32);
        out.put_u64(self.request_id);
        out.put_u8(self.status);
        out.put_slice(&self.payload);
        out.freeze()
    }

    pub(crate) fn decode(body: &[u8]) -> Result<Self, FrameError> {
        if body.len() < RESPONSE_HEADER {
            return Err(FrameError::Truncated);
        }
        Ok(Self {
            request_id: u64::from_be_bytes(body[0..8].try_into().expect("eight bytes are there")),
            status: body[8],
            payload: Bytes::copy_from_slice(&body[RESPONSE_HEADER..]),
        })
    }
}

/// Checks a length prefix before anything is allocated for it.
pub(crate) fn frame_length(prefix: [u8; 4]) -> Result<usize, FrameError> {
    let length = u32::from_be_bytes(prefix) as usize;
    if length > MAX_FRAME_BYTES {
        return Err(FrameError::TooLarge(length));
    }
    Ok(length)
}

fn service_of(raw: u16) -> Result<ServiceId, FrameError> {
    match raw {
        1 => Ok(ServiceId::Wal),
        2 => Ok(ServiceId::Raft),
        3 => Ok(ServiceId::Proxy),
        4 => Ok(ServiceId::Control),
        other => Err(FrameError::UnknownService(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> Request {
        Request {
            service: ServiceId::Wal,
            method: 7,
            request_id: u64::MAX,
            payload: Bytes::from_static(b"entries"),
        }
    }

    #[test]
    fn a_request_survives_the_wire_unchanged() {
        let encoded = request().encode();
        let length = frame_length(encoded[0..4].try_into().unwrap()).unwrap();
        assert_eq!(
            length,
            encoded.len() - 4,
            "the prefix counts what follows it"
        );
        assert_eq!(Request::decode(&encoded[4..]).unwrap(), request());
    }

    #[test]
    fn a_response_survives_the_wire_unchanged() {
        let response = Response {
            request_id: 12,
            status: STATUS_ERROR,
            payload: Bytes::from_static(b"no"),
        };
        let encoded = response.encode();
        assert_eq!(Response::decode(&encoded[4..]).unwrap(), response);
    }

    #[test]
    fn an_empty_payload_is_a_legal_frame() {
        let call = Request {
            payload: Bytes::new(),
            ..request()
        };
        assert_eq!(Request::decode(&call.encode()[4..]).unwrap(), call);
    }

    #[test]
    fn a_length_that_would_allocate_the_heap_is_refused_before_it_is_read() {
        let prefix = ((MAX_FRAME_BYTES + 1) as u32).to_be_bytes();
        assert!(matches!(frame_length(prefix), Err(FrameError::TooLarge(_))));
    }

    #[test]
    fn a_frame_shorter_than_its_header_is_refused_rather_than_indexed_into() {
        assert_eq!(Request::decode(&[0; 5]), Err(FrameError::Truncated));
        assert_eq!(Response::decode(&[0; 3]), Err(FrameError::Truncated));
    }

    #[test]
    fn a_service_this_build_does_not_know_is_named_in_the_error() {
        assert_eq!(
            Request::decode(&[0, 9, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1]),
            Err(FrameError::UnknownService(9))
        );
    }
}
