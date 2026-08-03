//! Key ranges.
//!
//! Partitions own half-open key ranges, `[start, end)`, with an absent `end`
//! meaning unbounded. Half-open ranges are what make a split a pure metadata
//! operation: one range becomes two that share a boundary key, and no key can
//! land in both or in neither.

use bytes::Bytes;

/// A half-open key range, `[start, end)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRange {
    start: Bytes,
    end: Option<Bytes>,
}

impl KeyRange {
    /// The range covering every key, which is what a keyspace starts with
    /// before its first split.
    #[must_use]
    pub fn unbounded() -> Self {
        Self {
            start: Bytes::new(),
            end: None,
        }
    }

    /// Builds a range, returning `None` if `end` is not strictly greater than
    /// `start`. An empty range is always a bug in the caller, so it cannot be
    /// constructed.
    #[must_use]
    pub fn new(start: impl Into<Bytes>, end: Option<Bytes>) -> Option<Self> {
        let start = start.into();
        if let Some(e) = &end {
            if e <= &start {
                return None;
            }
        }
        Some(Self { start, end })
    }

    #[must_use]
    pub fn start(&self) -> &[u8] {
        &self.start
    }

    #[must_use]
    pub fn end(&self) -> Option<&[u8]> {
        self.end.as_deref()
    }

    #[must_use]
    pub fn contains(&self, key: &[u8]) -> bool {
        key >= &self.start[..] && self.end.as_ref().is_none_or(|e| key < &e[..])
    }

    /// True if this range is immediately followed by `other`, which is the
    /// precondition for merging the two partitions.
    #[must_use]
    pub fn is_adjacent_to(&self, other: &KeyRange) -> bool {
        self.end.as_ref().is_some_and(|e| e == &other.start)
    }

    /// Splits at `at`, returning the lower and upper halves.
    ///
    /// Returns `None` if `at` is outside the range, since a split point must
    /// produce two non-empty ranges.
    #[must_use]
    pub fn split_at(&self, at: impl Into<Bytes>) -> Option<(KeyRange, KeyRange)> {
        let at = at.into();
        if !self.contains(&at) || at == self.start {
            return None;
        }
        let lower = KeyRange::new(self.start.clone(), Some(at.clone()))?;
        let upper = KeyRange::new(at, self.end.clone())?;
        Some((lower, upper))
    }

    /// Merges two adjacent ranges into one.
    #[must_use]
    pub fn merge(lower: &KeyRange, upper: &KeyRange) -> Option<KeyRange> {
        if !lower.is_adjacent_to(upper) {
            return None;
        }
        KeyRange::new(lower.start.clone(), upper.end.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(start: &'static str, end: Option<&'static str>) -> KeyRange {
        KeyRange::new(Bytes::from(start), end.map(Bytes::from)).expect("valid range")
    }

    #[test]
    fn unbounded_contains_everything() {
        let r = KeyRange::unbounded();
        assert!(r.contains(b""));
        assert!(r.contains(b"zzz"));
    }

    #[test]
    fn empty_ranges_cannot_be_built() {
        assert!(KeyRange::new(Bytes::from("b"), Some(Bytes::from("a"))).is_none());
        assert!(KeyRange::new(Bytes::from("a"), Some(Bytes::from("a"))).is_none());
    }

    #[test]
    fn containment_is_half_open() {
        let r = range("b", Some("d"));
        assert!(!r.contains(b"a"));
        assert!(r.contains(b"b"), "start is inclusive");
        assert!(r.contains(b"c"));
        assert!(!r.contains(b"d"), "end is exclusive");
    }

    #[test]
    fn split_partitions_every_key_exactly_once() {
        let r = range("a", Some("z"));
        let (lo, hi) = r.split_at(Bytes::from("m")).expect("valid split point");

        for key in [&b"a"[..], b"l", b"m", b"y"] {
            assert_eq!(
                lo.contains(key) ^ hi.contains(key),
                r.contains(key),
                "key {key:?} must land in exactly one half"
            );
        }
    }

    #[test]
    fn split_rejects_degenerate_points() {
        let r = range("a", Some("z"));
        assert!(r.split_at(Bytes::from("a")).is_none(), "empty lower half");
        assert!(r.split_at(Bytes::from("z")).is_none(), "out of range");
        assert!(r.split_at(Bytes::from("A")).is_none(), "below start");
    }

    #[test]
    fn merge_inverts_split() {
        let r = range("a", Some("z"));
        let (lo, hi) = r.split_at(Bytes::from("m")).unwrap();
        assert_eq!(KeyRange::merge(&lo, &hi), Some(r));
    }

    #[test]
    fn merge_rejects_non_adjacent() {
        assert!(KeyRange::merge(&range("a", Some("b")), &range("c", None)).is_none());
    }
}
