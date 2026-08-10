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
//! There are two entry points because there are two tombstone rules. A
//! partial merge ([`merge`]) drops tombstones by coverage, since resurrection
//! is the only risk. A whole-partition merge ([`merge_all`]) keeps every
//! unexpired tombstone, because an engine whose deletes carry a retention
//! period still needs them answering retries until it passes.
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
/// # This is where duplicate Lamports are caught exhaustively
///
/// A merge already has every record for a key in hand, so it compares all of
/// them rather than only each against the current leader. Three segments
/// holding one key at Lamports 5, 9, and 5 is a corrupt partition, and a check
/// that only ever compared against the leader would let the second 5 lose to
/// the 9 and say nothing. [`crate::snapshot`] cannot afford this, since it
/// would have to fetch a Lamport for every candidate rather than only for the
/// ones the manifest cannot separate, so a scrub is what finds these.
pub fn merge(
    inputs: &[Vec<SegmentRecord>],
    now_millis: u64,
    retained: &[SegmentEntry],
) -> Result<Vec<SegmentRecord>> {
    Ok(winners(inputs)?
        .into_iter()
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

/// Merges some of a partition's segments, keeping anything that would uncover
/// an older record if it went.
///
/// This is the rule a bounded compaction needs, and it is neither of the other
/// two. [`merge_all`] may drop an expired record because a whole-partition
/// merge leaves nothing behind for it to have been hiding; do that with
/// segments retained and a read finds the older record underneath, which is a
/// resurrection. [`merge`] drops an unexpired tombstone whenever coverage says
/// no older record exists, which loses the retry-answering property a
/// retention period is for.
///
/// So this keeps the union of what both keep: a record survives unless it is
/// expired *and* nothing retained could be revealed by dropping it. That is
/// strictly more conservative than either, and the cost of being conservative
/// is a record that lives until the next compaction rather than data that
/// comes back from the dead.
///
/// `retained` is every segment that stays live afterwards. As in [`merge`] the
/// coverage test is answered from the manifest alone, so it says "might" where
/// reading the segment would say "does not".
pub fn merge_bounded(
    inputs: &[Vec<SegmentRecord>],
    now_millis: u64,
    retained: &[SegmentEntry],
) -> Result<Vec<SegmentRecord>> {
    Ok(winners(inputs)?
        .into_iter()
        .filter(|record| {
            !record.is_expired_at(now_millis)
                || retained
                    .iter()
                    .any(|entry| entry.may_hold(&record.key) && entry.min_lamport < record.lamport)
        })
        .cloned()
        .collect())
}

/// Merges every segment of a partition, keeping every unexpired tombstone.
///
/// This is the whole-partition compaction a storage engine runs, and its
/// tombstone rule is time-based where [`merge`]'s is coverage-based. Coverage
/// answers "could dropping this resurrect an older value", which is the only
/// question when some segments stay behind. When nothing stays behind that
/// answer is always no, but a tombstone still has work to do: it answers a
/// retrying deleter until its retention passes, and dropping it early makes a
/// retried delete consume a fresh Lamport for a delete that already happened.
/// So an unexpired tombstone survives here, and the expiry rule reclaims it
/// on schedule.
pub fn merge_all(inputs: &[Vec<SegmentRecord>], now_millis: u64) -> Result<Vec<SegmentRecord>> {
    Ok(winners(inputs)?
        .into_iter()
        .filter(|record| !record.is_expired_at(now_millis))
        .cloned()
        .collect())
}

/// The record with the highest Lamport for each key, ascending by key, with
/// the exhaustive duplicate-Lamport check both merges rely on.
fn winners(inputs: &[Vec<SegmentRecord>]) -> Result<Vec<&SegmentRecord>> {
    let mut grouped: BTreeMap<&[u8], Vec<&SegmentRecord>> = BTreeMap::new();
    for records in inputs {
        for record in records {
            grouped.entry(record.key.as_ref()).or_default().push(record);
        }
    }

    let mut winners: Vec<&SegmentRecord> = Vec::with_capacity(grouped.len());
    for (key, mut candidates) in grouped {
        candidates.sort_by_key(|record| record.lamport);
        if let Some(pair) = candidates.windows(2).find(|p| p[0].lamport == p[1].lamport) {
            return Err(FormatError::Corrupt(format!(
                "two records for {key:?} at lamport {}",
                pair[0].lamport
            )));
        }
        winners.push(candidates.pop().expect("a group holds at least one record"));
    }
    Ok(winners)
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
            source: None,
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
    fn a_bounded_merge_keeps_an_expired_record_that_is_hiding_a_retained_one() {
        // The case `merge_all` cannot be used for. Whole-partition merging may
        // drop this because nothing survives underneath it; with a segment
        // retained, dropping it uncovers whatever that segment holds for the
        // same key, and the value comes back after its TTL passed.
        let retained = entry("s", "a", "z", (1, 4));
        let mut record = put("m", 7, "v");
        record.expires_at_millis = Some(100);

        assert!(
            merge_all(&[vec![record.clone()]], 100).unwrap().is_empty(),
            "the whole-partition rule drops it, which is why it is the wrong rule here"
        );
        assert_eq!(
            keys(&merge_bounded(&[vec![record]], 100, &[retained]).unwrap()),
            vec![&b"m"[..]],
        );
    }

    #[test]
    fn a_bounded_merge_keeps_an_unexpired_tombstone_nothing_retained_covers() {
        // The case `merge` cannot be used for. Coverage says no older record
        // can be resurrected, so `merge` drops it — but an unexpired tombstone
        // still has to answer a deleter that retries, exactly as it does in a
        // whole-partition merge.
        let far = entry("s", "x", "z", (99, 100));
        let stone = tombstone("m", 7, u64::MAX);

        assert!(
            merge(&[vec![stone.clone()]], 0, std::slice::from_ref(&far))
                .unwrap()
                .is_empty(),
            "the coverage rule drops it, which is why it is the wrong rule here"
        );
        assert_eq!(
            keys(&merge_bounded(&[vec![stone]], 0, &[far]).unwrap()),
            vec![&b"m"[..]],
        );
    }

    #[test]
    fn a_bounded_merge_still_reclaims_what_is_safe_to_reclaim() {
        // Expired and nothing retained could be under it, so it goes. Without
        // this the rule would be "keep everything" and reclaim nothing.
        let far = entry("s", "x", "z", (99, 100));
        let mut record = put("m", 7, "v");
        record.expires_at_millis = Some(100);
        assert!(merge_bounded(&[vec![record]], 100, &[far])
            .unwrap()
            .is_empty());
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
    fn a_whole_partition_merge_keeps_an_unexpired_tombstone() {
        // The rule merge_all exists for. Nothing is retained, so coverage
        // says the tombstone could go, but a retrying deleter still needs it
        // until its retention passes.
        let merged = merge_all(&[vec![tombstone("m", 7, u64::MAX)]], 0).unwrap();
        assert_eq!(keys(&merged), vec![&b"m"[..]]);
    }

    #[test]
    fn a_whole_partition_merge_reclaims_an_expired_tombstone() {
        let merged = merge_all(&[vec![tombstone("m", 7, 100)]], 100).unwrap();
        assert!(merged.is_empty(), "retention passed, so the work is done");
    }

    #[test]
    fn a_whole_partition_merge_resolves_keys_and_drops_expired_records() {
        let mut expiring = put("c", 4, "v");
        expiring.expires_at_millis = Some(100);
        let merged = merge_all(
            &[
                vec![put("a", 1, "old"), expiring],
                vec![put("a", 9, "new"), put("b", 2, "keep")],
            ],
            100,
        )
        .unwrap();
        assert_eq!(keys(&merged), vec![&b"a"[..], b"b"]);
        assert_eq!(merged[0].lamport, Lamport(9));
    }

    #[test]
    fn a_whole_partition_merge_catches_duplicate_lamports_too() {
        let outcome = merge_all(&[vec![put("a", 5, "one")], vec![put("a", 5, "two")]], 0);
        assert!(matches!(outcome, Err(FormatError::Corrupt(_))));
    }

    #[test]
    fn a_tie_is_caught_even_when_a_higher_lamport_would_have_won() {
        // 5, 9, 5. A check that only compared each record against the current
        // leader would let the second 5 lose to the 9 and report nothing, and
        // the partition would stay corrupt with no error raised.
        let outcome = merge(
            &[
                vec![put("a", 5, "one")],
                vec![put("a", 9, "winner")],
                vec![put("a", 5, "two")],
            ],
            0,
            &[],
        );
        assert!(matches!(outcome, Err(FormatError::Corrupt(_))));
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
