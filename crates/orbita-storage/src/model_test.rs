//! The partition compared against a `BTreeMap` that implements the same rules.
//!
//! Every other test in this crate checks a case someone thought of. This one
//! checks the cases nobody thought of: it draws a long sequence of operations
//! from a seeded generator, runs each one against both the real partition and
//! an obviously-correct model, and fails on the first disagreement. A failure
//! reports its seed, so the exact sequence replays.
//!
//! The model is written to be readable rather than fast, and it deliberately
//! duplicates the semantics instead of calling into the engine. If the two
//! ever shared code they would agree by construction and prove nothing.
//!
//! On top of matching the model, the sequence checks the two properties ADR
//! 0002 turns on: within a partition a version is never reused, and the
//! versions handed out only ever increase. Those hold across the whole run
//! rather than at any one step, so no single operation could catch a break.

use crate::partition::{WriteOutcome, TOMBSTONE_RETENTION_MILLIS};
use crate::testing::{partition_with_clock, ManualClock, TempPartition};

use bytes::Bytes;
use orbita_core::{Lamport, Record, Version, WriteCondition, MAX_LIST_LIMIT};
use orbita_runtime::{Rng, SeededRng};
use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

/// What the model believes is stored under a key, including entries that are
/// no longer visible. Invisible entries still matter because a tombstone
/// answers questions about the delete that produced it.
#[derive(Debug, Clone)]
struct Entry {
    value: Bytes,
    version: Version,
    expires_at_millis: Option<u64>,
    deleted: bool,
}

impl Entry {
    fn is_expired_at(&self, now: u64) -> bool {
        self.expires_at_millis.is_some_and(|e| now >= e)
    }

    fn visible_at(&self, now: u64) -> Option<Record> {
        if self.deleted || self.is_expired_at(now) {
            return None;
        }
        Some(Record {
            value: self.value.clone(),
            version: self.version,
            expires_at_millis: self.expires_at_millis,
        })
    }
}

#[derive(Default)]
struct Model {
    entries: BTreeMap<Vec<u8>, Entry>,
}

impl Model {
    fn get(&self, key: &[u8], now: u64) -> Option<Record> {
        self.entries.get(key).and_then(|e| e.visible_at(now))
    }

    fn check(&self, key: &[u8], condition: WriteCondition, now: u64) -> Option<WriteOutcome> {
        let found = self.get(key, now).map(|r| r.version);
        let holds = match condition {
            WriteCondition::None => true,
            WriteCondition::IfNotPresent => found.is_none(),
            WriteCondition::IfVersion(expected) => found == Some(expected),
        };
        if holds {
            None
        } else {
            Some(WriteOutcome::ConditionFailed { condition, found })
        }
    }

    fn put(
        &mut self,
        lamport: Lamport,
        key: &[u8],
        value: Bytes,
        ttl: Option<Duration>,
        condition: WriteCondition,
        now: u64,
    ) -> WriteOutcome {
        if let Some(failure) = self.check(key, condition, now) {
            return failure;
        }
        let version = Version(lamport.get());
        self.entries.insert(
            key.to_vec(),
            Entry {
                value,
                version,
                expires_at_millis: ttl
                    .map(|d| now.saturating_add(u64::try_from(d.as_millis()).unwrap_or(u64::MAX))),
                deleted: false,
            },
        );
        WriteOutcome::Applied { version }
    }

    fn delete(
        &mut self,
        lamport: Lamport,
        key: &[u8],
        condition: WriteCondition,
        now: u64,
    ) -> WriteOutcome {
        if let Some(failure) = self.check(key, condition, now) {
            return failure;
        }
        if let Some(entry) = self.entries.get(key) {
            if entry.deleted && !entry.is_expired_at(now) {
                return WriteOutcome::Applied {
                    version: entry.version,
                };
            }
        }
        let version = Version(lamport.get());
        self.entries.insert(
            key.to_vec(),
            Entry {
                value: Bytes::new(),
                version,
                expires_at_millis: Some(now.saturating_add(TOMBSTONE_RETENTION_MILLIS)),
                deleted: true,
            },
        );
        WriteOutcome::Applied { version }
    }

    fn scan(&self, prefix: &[u8], now: u64) -> Vec<(Bytes, Record)> {
        self.entries
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .filter_map(|(key, entry)| {
                entry
                    .visible_at(now)
                    .map(|record| (Bytes::copy_from_slice(key), record))
            })
            .collect()
    }
}

/// Watches the versions the partition hands out across a whole run.
#[derive(Default)]
struct VersionLedger {
    issued: HashSet<u64>,
    highest: u64,
}

impl VersionLedger {
    /// Records a version the partition committed.
    ///
    /// A repeat is the ABA that ADR 0002 exists to remove, and a version that
    /// goes backwards would let a client's held token stop matching through no
    /// write of its own.
    fn record(&mut self, version: Version, context: &str) {
        assert!(
            self.issued.insert(version.get()),
            "version {version} was issued twice at {context}"
        );
        assert!(
            version.get() > self.highest,
            "version {version} is not ahead of {} at {context}",
            self.highest
        );
        self.highest = version.get();
    }
}

/// The keys the generator draws from.
///
/// A small alphabet is the point: collisions on the same key are where
/// conditional writes, tombstones, and version allocation actually interact,
/// and a wide key space would almost never produce one.
fn key_of(rng: &SeededRng) -> Vec<u8> {
    let group = rng.below(3);
    let index = rng.below(6);
    format!("g{group}/k{index}").into_bytes()
}

fn condition_of(rng: &SeededRng, plausible: Option<Version>) -> WriteCondition {
    match rng.below(4) {
        0 => WriteCondition::IfNotPresent,
        // Half the version checks aim at what is really there, so that
        // successes and failures both get exercised.
        1 => WriteCondition::IfVersion(plausible.unwrap_or(Version(1))),
        2 => WriteCondition::IfVersion(Version(rng.below(8) + 1)),
        _ => WriteCondition::None,
    }
}

/// Pages through a prefix the way a client would.
async fn page_through(
    partition: &TempPartition,
    prefix: &[u8],
    limit: u32,
) -> Vec<(Bytes, Record)> {
    let mut seen = Vec::new();
    let mut cursor: Option<Bytes> = None;
    loop {
        let page = partition
            .scan(prefix, cursor.as_deref(), limit)
            .await
            .expect("scanning a valid prefix");
        seen.extend(page.entries.into_iter().map(|e| (e.key, e.record)));
        match page.cursor {
            Some(next) => cursor = Some(next),
            None => return seen,
        }
    }
}

async fn run_sequence(seed: u64, operations: usize) {
    let rng = SeededRng::new(seed);
    let (partition, clock): (TempPartition, ManualClock) = partition_with_clock().await;
    let mut model = Model::default();
    let mut ledger = VersionLedger::default();
    let mut now = 1_000u64;
    clock.set_millis(now);

    // The owner's Lamport counter. It runs ahead of the committed Lamport in
    // jumps so that versions come out sparse, which is what they look like in
    // a partition serving more than one key.
    let mut committed = 0u64;

    for step in 0..operations {
        let context = format!("seed {seed}, step {step}");
        let lamport = Lamport(committed + 1 + rng.below(3));

        match rng.below(10) {
            0..=3 => {
                let key = key_of(&rng);
                let value = Bytes::from(vec![u8::try_from(rng.below(256)).unwrap(); 4]);
                let ttl = if rng.chance(1, 3) {
                    Some(Duration::from_millis(rng.below(40)))
                } else {
                    None
                };
                let condition = condition_of(&rng, model.get(&key, now).map(|r| r.version));

                let actual = partition
                    .put(lamport, &key, value.clone(), ttl, condition)
                    .await
                    .expect("a legal put");
                let expected = model.put(lamport, &key, value, ttl, condition, now);
                assert_eq!(actual, expected, "put {key:?} at {context}");

                if let WriteOutcome::Applied { version } = actual {
                    ledger.record(version, &context);
                    committed = lamport.get();
                }
            }
            4..=5 => {
                let key = key_of(&rng);
                let condition = condition_of(&rng, model.get(&key, now).map(|r| r.version));

                let actual = partition
                    .delete(lamport, &key, condition)
                    .await
                    .expect("a legal delete");
                let expected = model.delete(lamport, &key, condition, now);
                assert_eq!(actual, expected, "delete {key:?} at {context}");

                // A delete that finds an existing tombstone commits nothing
                // and reports the older version, so the Lamport stays free.
                if actual
                    == (WriteOutcome::Applied {
                        version: Version(lamport.get()),
                    })
                {
                    ledger.record(Version(lamport.get()), &context);
                    committed = lamport.get();
                }
            }
            6..=7 => {
                let key = key_of(&rng);
                let actual = partition.get(&key).await.expect("a legal get");
                assert_eq!(actual, model.get(&key, now), "get {key:?} at {context}");
            }
            8 => {
                let prefix: Vec<u8> = if rng.chance(1, 2) {
                    Vec::new()
                } else {
                    format!("g{}/", rng.below(3)).into_bytes()
                };
                // Small pages so a sequence exercises many cursor hops.
                let limit = u32::try_from(rng.below(4) + 1).unwrap();
                let actual = page_through(&partition, &prefix, limit).await;
                assert_eq!(
                    actual,
                    model.scan(&prefix, now),
                    "scan {prefix:?} with limit {limit} at {context}"
                );
            }
            _ => {
                // Moving time is what makes TTL interact with everything else,
                // including conditional writes against a key that just expired.
                now = now.saturating_add(rng.below(20));
                clock.set_millis(now);
            }
        }

        assert_eq!(
            partition.committed_lamport().await.unwrap(),
            Lamport(committed),
            "the partition and its owner disagree about the sequence at {context}"
        );
    }

    // Whatever the sequence did, the surviving state has to agree in full.
    assert_eq!(
        page_through(&partition, b"", MAX_LIST_LIMIT).await,
        model.scan(b"", now),
        "final state for seed {seed}"
    );

    // Every version still readable must be one the partition actually issued,
    // which catches a version invented somewhere in the read path.
    for (key, record) in model.scan(b"", now) {
        assert!(
            ledger.issued.contains(&record.version.get()),
            "key {key:?} reads at version {} which was never issued",
            record.version
        );
    }
}

#[tokio::test]
async fn random_operation_sequences_match_a_btreemap_model() {
    for seed in 0..6u64 {
        run_sequence(seed, 400).await;
    }
}

#[tokio::test]
async fn a_long_sequence_on_one_seed_matches_the_model() {
    run_sequence(0xdead_beef, 3_000).await;
}
