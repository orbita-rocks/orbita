//! Merging segments, and what a merge is allowed to throw away.
//!
//! Compaction exists to reclaim space rather than to make reads faster, since a
//! lookup never consults more than one segment. That makes its correctness
//! rules short, and all three of them are about not losing something:
//!
//! - Merging preserves, for each key, the record with the highest Lamport.
//! - A tombstone may be dropped only when no retained segment holds an older
//!   record for its key, because dropping it early resurrects the value.
//! - An expired record may be dropped at any time.
//!
//! The result is published by the ordinary commit, which is what makes a
//! compaction that dies half way through cost nothing but the objects it wrote.

use crate::error::{FormatError, Result};
use crate::manifest::SegmentEntry;
use crate::record::SegmentRecord;

use std::collections::BTreeMap;

/// Merges the records of the segments being compacted.
///
/// `retained` is every segment that will still be live afterwards, meaning the
/// ones not being merged. It is what decides whether a tombstone can go: a
/// retained segment whose key bounds cover the key and whose Lamports reach
/// below the tombstone's may hold an older record, and dropping the tombstone
/// would bring that record back.
///
/// The test is deliberately conservative. It is answered from the manifest
/// alone, so it can say "might" where reading the segment would say "does not",
/// and the cost of being wrong that way is a tombstone that survives one more
/// compaction.
pub fn merge(
    inputs: &[Vec<SegmentRecord>],
    now_millis: u64,
    retained: &[SegmentEntry],
) -> Result<Vec<SegmentRecord>> {
    let mut winners: BTreeMap<&[u8], &SegmentRecord> = BTreeMap::new();
    for records in inputs {
        for record in records {
            match winners.get(record.key.as_ref()) {
                None => {
                    winners.insert(record.key.as_ref(), record);
                }
                Some(existing) if existing.lamport < record.lamport => {
                    winners.insert(record.key.as_ref(), record);
                }
                Some(existing) if existing.lamport == record.lamport => {
                    return Err(FormatError::Corrupt(format!(
                        "two records for {:?} at lamport {}",
                        record.key, record.lamport
                    )));
                }
                Some(_) => {}
            }
        }
    }

    Ok(winners
        .into_values()
        .filter(|record| !record.is_expired_at(now_millis))
        .filter(|record| {
            !record.is_tombstone()
                || retained
                    .iter()
                    .any(|entry| entry.may_hold(&record.key) && entry.min_lamport < record.lamport)
        })
        .cloned()
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::RecordValue;

    use bytes::Bytes;
    use orbita_core::Lamport;

    fn put(key: &str, lamport: u64, value: &str) -> SegmentRecord {
        SegmentRecord {
            key: Bytes::copy_from_slice(key.as_bytes()),
            lamport: Lamport(lamport),
            expires_at_millis: None,
            value: RecordValue::Inline(Bytes::copy_from_slice(value.as_bytes())),
        }
    }

    fn tombstone(key: &str, lamport: u64, expires: u64) -> SegmentRecord {
        SegmentRecord {
            key: Bytes::copy_from_slice(key.as_bytes()),
            lamport: Lamport(lamport),
            expires_at_millis: Some(expires),
            value: RecordValue::Tombstone,
        }
    }

    fn entry(name: &str, min_key: &str, max_key: &str, lamports: (u64, u64)) -> SegmentEntry {
        SegmentEntry {
            name: format!("segments/{name}"),
            bytes: 1024,
            record_count: 1,
            min_key: Bytes::copy_from_slice(min_key.as_bytes()),
            max_key: Bytes::copy_from_slice(max_key.as_bytes()),
            min_lamport: Lamport(lamports.0),
            max_lamport: Lamport(lamports.1),
        }
    }

    fn keys(records: &[SegmentRecord]) -> Vec<&[u8]> {
        records.iter().map(|r| r.key.as_ref()).collect()
    }

    #[test]
    fn the_highest_lamport_for_each_key_survives() {
        let merged = merge(
            &[
                vec![put("a", 1, "old"), put("b", 2, "keep")],
                vec![put("a", 9, "new")],
            ],
            0,
            &[],
        )
        .unwrap();

        assert_eq!(keys(&merged), vec![&b"a"[..], b"b"]);
        assert_eq!(merged[0].lamport, Lamport(9));
        assert_eq!(merged[0].value, RecordValue::Inline(Bytes::from("new")));
    }

    #[test]
    fn the_output_is_sorted_and_holds_each_key_once() {
        let merged = merge(
            &[
                vec![put("b", 1, "v"), put("d", 2, "v")],
                vec![put("a", 3, "v"), put("c", 4, "v")],
            ],
            0,
            &[],
        )
        .unwrap();
        assert_eq!(keys(&merged), vec![&b"a"[..], b"b", b"c", b"d"]);
    }

    #[test]
    fn an_expired_record_goes_whatever_else_is_true() {
        let mut record = put("a", 1, "v");
        record.expires_at_millis = Some(100);
        assert!(merge(&[vec![record]], 100, &[]).unwrap().is_empty());
    }

    #[test]
    fn a_tombstone_survives_while_a_retained_segment_may_hold_an_older_record() {
        let retained = entry("s", "a", "z", (1, 4));
        let merged = merge(&[vec![tombstone("m", 7, u64::MAX)]], 0, &[retained]).unwrap();
        assert_eq!(
            keys(&merged),
            vec![&b"m"[..]],
            "dropping it now would resurrect whatever that segment holds"
        );
    }

    #[test]
    fn a_tombstone_goes_when_nothing_retained_could_hold_an_older_record() {
        // Out of key range, and reaching only above the tombstone's lamport.
        let out_of_range = entry("s", "x", "z", (1, 4));
        let too_new = entry("t", "a", "z", (8, 9));
        let merged = merge(
            &[vec![tombstone("m", 7, u64::MAX)]],
            0,
            &[out_of_range, too_new],
        )
        .unwrap();
        assert!(merged.is_empty());
    }

    #[test]
    fn a_tombstone_that_has_expired_goes_regardless() {
        // The bounded horizon the format is honest about: a tombstone is
        // retained for a configured duration recorded as its own expiry.
        let retained = entry("s", "a", "z", (1, 4));
        let merged = merge(&[vec![tombstone("m", 7, 100)]], 100, &[retained]).unwrap();
        assert!(merged.is_empty());
    }

    #[test]
    fn a_tombstone_shadowing_an_older_value_in_the_same_merge_keeps_the_delete() {
        let merged = merge(
            &[vec![put("a", 1, "v")], vec![tombstone("a", 5, u64::MAX)]],
            0,
            &[],
        )
        .unwrap();
        assert!(merged.is_empty(), "both inputs are being rewritten");

        let merged = merge(
            &[vec![tombstone("a", 5, u64::MAX)], vec![put("a", 9, "back")]],
            0,
            &[],
        )
        .unwrap();
        assert_eq!(merged.len(), 1, "a write after a delete is not a delete");
        assert_eq!(merged[0].lamport, Lamport(9));
    }

    #[test]
    fn two_records_for_one_key_at_one_lamport_is_corruption() {
        let outcome = merge(
            &[vec![put("a", 5, "one")], vec![put("a", 5, "two")]],
            0,
            &[],
        );
        assert!(matches!(outcome, Err(FormatError::Corrupt(_))));
    }
}
