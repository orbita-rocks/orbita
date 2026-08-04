//! Fixed-width little-endian encoding for everything this crate persists or
//! sends.
//!
//! Hand-rolled for the same reason the WAL's wire format is: `Transport`
//! already carries opaque bytes, the shapes involved are a couple of dozen
//! fields, and a schema compiler would buy a build step rather than a
//! guarantee. The protobuf definitions in `orbita-proto` are the operator
//! facing surface and are deliberately not reused here, because the replicated
//! log has to keep decoding entries written by an older binary and a
//! hand-written decoder makes that constraint visible.

use bytes::{BufMut, Bytes, BytesMut};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CodecError {
    #[error("buffer ended early: wanted {wanted} bytes at offset {at}")]
    Truncated { at: usize, wanted: usize },

    #[error("unrecognised tag {tag} for {what}")]
    UnknownTag { what: &'static str, tag: u64 },

    #[error("value is not valid utf-8")]
    NotUtf8,

    #[error("trailing bytes after a complete message")]
    Trailing,

    #[error("field out of range: {0}")]
    OutOfRange(&'static str),
}

pub type CodecResult<T> = Result<T, CodecError>;

/// A growable buffer with one method per wire type.
#[derive(Debug, Default)]
pub struct Writer {
    buf: BytesMut,
}

impl Writer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn u8(&mut self, v: u8) -> &mut Self {
        self.buf.put_u8(v);
        self
    }

    pub fn u32(&mut self, v: u32) -> &mut Self {
        self.buf.put_u32_le(v);
        self
    }

    pub fn u64(&mut self, v: u64) -> &mut Self {
        self.buf.put_u64_le(v);
        self
    }

    /// Length-prefixed bytes. Lengths are `u32` because nothing this crate
    /// stores is anywhere near four gigabytes and a varint would only save
    /// space on a log that is already tiny.
    pub fn bytes(&mut self, v: &[u8]) -> &mut Self {
        self.buf.put_u32_le(v.len() as u32);
        self.buf.put_slice(v);
        self
    }

    pub fn str(&mut self, v: &str) -> &mut Self {
        self.bytes(v.as_bytes())
    }

    pub fn opt_u64(&mut self, v: Option<u64>) -> &mut Self {
        match v {
            Some(x) => self.u8(1).u64(x),
            None => self.u8(0),
        }
    }

    pub fn opt_u32(&mut self, v: Option<u32>) -> &mut Self {
        match v {
            Some(x) => self.u8(1).u32(x),
            None => self.u8(0),
        }
    }

    pub fn opt_bytes(&mut self, v: Option<&[u8]>) -> &mut Self {
        match v {
            Some(x) => {
                self.u8(1);
                self.bytes(x)
            }
            None => self.u8(0),
        }
    }

    /// A length-prefixed sequence, written by the caller's closure per item.
    pub fn seq<T>(&mut self, items: &[T], mut each: impl FnMut(&mut Self, &T)) -> &mut Self {
        self.u32(items.len() as u32);
        for item in items {
            each(self, item);
        }
        self
    }

    pub fn finish(&mut self) -> Bytes {
        std::mem::take(&mut self.buf).freeze()
    }
}

/// A cursor over an encoded message.
#[derive(Debug)]
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> CodecResult<&'a [u8]> {
        let end = self.pos.checked_add(n).ok_or(CodecError::Truncated {
            at: self.pos,
            wanted: n,
        })?;
        if end > self.buf.len() {
            return Err(CodecError::Truncated {
                at: self.pos,
                wanted: n,
            });
        }
        let slice = &self.buf[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    pub fn u8(&mut self) -> CodecResult<u8> {
        Ok(self.take(1)?[0])
    }

    pub fn u32(&mut self) -> CodecResult<u32> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    pub fn u64(&mut self) -> CodecResult<u64> {
        let b = self.take(8)?;
        Ok(u64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    pub fn bytes(&mut self) -> CodecResult<Bytes> {
        let len = self.u32()? as usize;
        Ok(Bytes::copy_from_slice(self.take(len)?))
    }

    pub fn string(&mut self) -> CodecResult<String> {
        let len = self.u32()? as usize;
        let raw = self.take(len)?;
        String::from_utf8(raw.to_vec()).map_err(|_| CodecError::NotUtf8)
    }

    pub fn opt_u64(&mut self) -> CodecResult<Option<u64>> {
        if self.u8()? == 0 {
            Ok(None)
        } else {
            Ok(Some(self.u64()?))
        }
    }

    pub fn opt_u32(&mut self) -> CodecResult<Option<u32>> {
        if self.u8()? == 0 {
            Ok(None)
        } else {
            Ok(Some(self.u32()?))
        }
    }

    pub fn opt_bytes(&mut self) -> CodecResult<Option<Bytes>> {
        if self.u8()? == 0 {
            Ok(None)
        } else {
            Ok(Some(self.bytes()?))
        }
    }

    pub fn seq<T>(
        &mut self,
        mut each: impl FnMut(&mut Self) -> CodecResult<T>,
    ) -> CodecResult<Vec<T>> {
        let count = self.u32()? as usize;
        // A corrupt count could ask for a huge allocation before any of the
        // bytes behind it are checked, so the capacity hint is bounded and the
        // vector grows honestly if the count turns out to be real.
        let mut out = Vec::with_capacity(count.min(1024));
        for _ in 0..count {
            out.push(each(self)?);
        }
        Ok(out)
    }

    /// Whether any bytes remain.
    ///
    /// This exists for one job: telling a message written by the previous
    /// release, which simply ends earlier, apart from one that carries a
    /// newer trailing field. It is only sound for a field at the very end of
    /// a message, and every use should say which release it tolerates so the
    /// tolerance can be deleted when the window moves past it.
    pub fn has_more(&self) -> bool {
        self.pos < self.buf.len()
    }

    /// Rejects trailing bytes, which mean the sender and the receiver disagree
    /// about the shape of the message.
    pub fn done(&self) -> CodecResult<()> {
        if self.pos == self.buf.len() {
            Ok(())
        } else {
            Err(CodecError::Trailing)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_scalar_round_trips() {
        let mut w = Writer::new();
        w.u8(7)
            .u32(1 << 20)
            .u64(u64::MAX)
            .str("keyspace")
            .opt_u64(Some(9))
            .opt_u64(None)
            .opt_u32(Some(3))
            .opt_bytes(Some(b"m"))
            .opt_bytes(None);
        let encoded = w.finish();

        let mut r = Reader::new(&encoded);
        assert_eq!(r.u8().unwrap(), 7);
        assert_eq!(r.u32().unwrap(), 1 << 20);
        assert_eq!(r.u64().unwrap(), u64::MAX);
        assert_eq!(r.string().unwrap(), "keyspace");
        assert_eq!(r.opt_u64().unwrap(), Some(9));
        assert_eq!(r.opt_u64().unwrap(), None);
        assert_eq!(r.opt_u32().unwrap(), Some(3));
        assert_eq!(r.opt_bytes().unwrap().as_deref(), Some(&b"m"[..]));
        assert_eq!(r.opt_bytes().unwrap(), None);
        assert_eq!(r.done(), Ok(()));
    }

    #[test]
    fn a_truncated_message_is_an_error_rather_than_a_panic() {
        let mut w = Writer::new();
        let encoded = w.u64(1).finish();
        let mut r = Reader::new(&encoded[..4]);
        assert!(matches!(r.u64(), Err(CodecError::Truncated { .. })));
    }

    #[test]
    fn a_corrupt_length_does_not_allocate_what_it_claims() {
        // The count says a million items and the buffer holds none. Decoding
        // has to fail on the first missing item rather than reserving for all
        // of them, or a single bad byte is an out of memory abort.
        let mut w = Writer::new();
        let encoded = w.u32(1_000_000).finish();
        let mut r = Reader::new(&encoded);
        assert!(r.seq(|r| r.u64()).is_err());
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        let mut w = Writer::new();
        let encoded = w.u64(1).u64(2).finish();
        let mut r = Reader::new(&encoded);
        r.u64().unwrap();
        assert_eq!(r.done(), Err(CodecError::Trailing));
    }
}
