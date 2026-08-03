//! The partition map.
//!
//! This is the cluster's answer to "who owns this key?", and it is shared
//! vocabulary rather than control plane internals because both sides need it:
//! the leader group decides it, and every worker caches it to route requests.
//! Putting it here keeps the two from inventing incompatible spellings of the
//! same table.
//!
//! The map is a value, not a service. Fetching it, refreshing it, and agreeing
//! on it are somebody else's problem; this type only answers questions about a
//! snapshot of it.

use crate::ids::{Epoch, KeyspaceId, KeyspaceName, NodeId, PartitionId};
use crate::range::KeyRange;

use std::collections::BTreeMap;
use std::collections::HashMap;

/// A monotonically increasing generation for the whole map.
///
/// A worker compares this against what it holds to decide whether an update is
/// newer, which matters because updates can arrive out of order over different
/// connections. Applying an older map over a newer one would resurrect a
/// deposed owner in the worker's routing table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct MapVersion(pub u64);

impl MapVersion {
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for MapVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// One partition's placement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionInfo {
    pub id: PartitionId,
    pub keyspace: KeyspaceId,
    pub range: KeyRange,
    /// Absent during a failover, meaning writes are unavailable for this range
    /// while reads may still be served by a replica holding a live lease.
    pub owner: Option<NodeId>,
    /// Bumped on every ownership change. A write carrying a stale epoch is
    /// rejected by the replicas, which is what fences a deposed owner.
    pub epoch: Epoch,
    /// Nodes holding a copy, not including the owner.
    pub replicas: Vec<NodeId>,
}

impl PartitionInfo {
    /// Whether `node` holds a copy of this partition, as owner or replica.
    #[must_use]
    pub fn holds(&self, node: NodeId) -> bool {
        self.owner == Some(node) || self.replicas.contains(&node)
    }
}

/// A keyspace's identity and limits, as far as routing and admission need it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyspaceInfo {
    pub id: KeyspaceId,
    pub name: KeyspaceName,
    pub default_ttl_millis: Option<u64>,
    pub max_value_bytes: Option<u64>,
    pub max_storage_bytes: Option<u64>,
    pub max_reads_per_second: Option<u32>,
    pub max_writes_per_second: Option<u32>,
}

/// A snapshot of who owns what.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PartitionMap {
    version: MapVersion,
    /// Keyed by keyspace and range start, so the partition owning a key is the
    /// greatest entry at or below it. A `BTreeMap` gives that lookup directly
    /// rather than by scanning, which matters because it is on every request.
    partitions: BTreeMap<(KeyspaceId, Vec<u8>), PartitionInfo>,
    keyspaces: HashMap<KeyspaceId, KeyspaceInfo>,
    by_name: HashMap<String, KeyspaceId>,
}

impl PartitionMap {
    #[must_use]
    pub fn new(version: MapVersion) -> Self {
        Self {
            version,
            ..Default::default()
        }
    }

    #[must_use]
    pub fn version(&self) -> MapVersion {
        self.version
    }

    pub fn set_version(&mut self, version: MapVersion) {
        self.version = version;
    }

    pub fn insert_keyspace(&mut self, info: KeyspaceInfo) {
        self.by_name.insert(info.name.as_str().to_string(), info.id);
        self.keyspaces.insert(info.id, info);
    }

    /// Removes a keyspace and every partition belonging to it.
    ///
    /// The two go together because a keyspace without partitions is a hole
    /// that `check_coverage` would report, and a partition without a keyspace
    /// routes requests to a tenant that no longer exists.
    pub fn remove_keyspace(&mut self, id: KeyspaceId) {
        if let Some(info) = self.keyspaces.remove(&id) {
            self.by_name.remove(info.name.as_str());
        }
        self.partitions.retain(|(keyspace, _), _| *keyspace != id);
    }

    pub fn keyspaces(&self) -> impl Iterator<Item = &KeyspaceInfo> {
        self.keyspaces.values()
    }

    pub fn insert_partition(&mut self, info: PartitionInfo) {
        let key = (info.keyspace, info.range.start().to_vec());
        self.partitions.insert(key, info);
    }

    pub fn remove_partition(&mut self, keyspace: KeyspaceId, range_start: &[u8]) {
        self.partitions.remove(&(keyspace, range_start.to_vec()));
    }

    #[must_use]
    pub fn keyspace_by_name(&self, name: &str) -> Option<&KeyspaceInfo> {
        self.by_name.get(name).and_then(|id| self.keyspaces.get(id))
    }

    #[must_use]
    pub fn keyspace(&self, id: KeyspaceId) -> Option<&KeyspaceInfo> {
        self.keyspaces.get(&id)
    }

    /// Finds the partition owning `key`.
    ///
    /// Returns `None` when the keyspace has no partition covering the key,
    /// which should be impossible in a healthy cluster and is worth surfacing
    /// as an error rather than a retry: it means the map is corrupt or a split
    /// left a hole.
    #[must_use]
    pub fn lookup(&self, keyspace: KeyspaceId, key: &[u8]) -> Option<&PartitionInfo> {
        let candidate = self
            .partitions
            .range(..=(keyspace, key.to_vec()))
            .next_back()
            .map(|(_, info)| info)?;

        // The greatest entry at or below the key might belong to the previous
        // keyspace, or to a partition whose range ends before the key.
        if candidate.keyspace == keyspace && candidate.range.contains(key) {
            Some(candidate)
        } else {
            None
        }
    }

    #[must_use]
    pub fn partition(&self, id: PartitionId) -> Option<&PartitionInfo> {
        self.partitions.values().find(|p| p.id == id)
    }

    pub fn partitions(&self) -> impl Iterator<Item = &PartitionInfo> {
        self.partitions.values()
    }

    /// Partitions this node holds, as owner or replica.
    pub fn held_by(&self, node: NodeId) -> impl Iterator<Item = &PartitionInfo> {
        self.partitions.values().filter(move |p| p.holds(node))
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.partitions.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }

    /// Checks that every keyspace is covered by partitions with no holes and
    /// no overlaps.
    ///
    /// This is here so that splits and merges can assert it in tests and in
    /// the simulator. A hole means requests for those keys fail, and an
    /// overlap means two owners accept writes for the same key, which is the
    /// worst outcome the system has.
    pub fn check_coverage(&self) -> Result<(), CoverageError> {
        let mut current: Option<(KeyspaceId, Option<Vec<u8>>)> = None;

        for ((keyspace, start), info) in &self.partitions {
            match &current {
                // First partition of a keyspace has to start at the beginning.
                None => {
                    if !start.is_empty() {
                        return Err(CoverageError::GapAtStart {
                            keyspace: *keyspace,
                        });
                    }
                }
                Some((prev_keyspace, prev_end)) if prev_keyspace == keyspace => match prev_end {
                    None => {
                        return Err(CoverageError::Overlap {
                            keyspace: *keyspace,
                            at: start.clone(),
                        })
                    }
                    Some(end) if end != start => {
                        return Err(if end < start {
                            CoverageError::Gap {
                                keyspace: *keyspace,
                                from: end.clone(),
                                to: start.clone(),
                            }
                        } else {
                            CoverageError::Overlap {
                                keyspace: *keyspace,
                                at: start.clone(),
                            }
                        })
                    }
                    Some(_) => {}
                },
                // Previous keyspace ended; it must have ended unbounded.
                Some((prev_keyspace, prev_end)) => {
                    if prev_end.is_some() {
                        return Err(CoverageError::GapAtEnd {
                            keyspace: *prev_keyspace,
                        });
                    }
                    if !start.is_empty() {
                        return Err(CoverageError::GapAtStart {
                            keyspace: *keyspace,
                        });
                    }
                }
            }
            current = Some((*keyspace, info.range.end().map(<[u8]>::to_vec)));
        }

        if let Some((keyspace, end)) = current {
            if end.is_some() {
                return Err(CoverageError::GapAtEnd { keyspace });
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CoverageError {
    #[error("keyspace {keyspace} has no partition covering the start of the range")]
    GapAtStart { keyspace: KeyspaceId },

    #[error("keyspace {keyspace} is not covered to the end of the range")]
    GapAtEnd { keyspace: KeyspaceId },

    #[error("keyspace {keyspace} has a gap between {from:?} and {to:?}")]
    Gap {
        keyspace: KeyspaceId,
        from: Vec<u8>,
        to: Vec<u8>,
    },

    #[error("keyspace {keyspace} has overlapping partitions at {at:?}")]
    Overlap { keyspace: KeyspaceId, at: Vec<u8> },
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    const KS: KeyspaceId = KeyspaceId(1);
    const KS2: KeyspaceId = KeyspaceId(2);

    fn partition(id: u64, keyspace: KeyspaceId, start: &str, end: Option<&str>) -> PartitionInfo {
        PartitionInfo {
            id: PartitionId(id),
            keyspace,
            range: KeyRange::new(
                Bytes::copy_from_slice(start.as_bytes()),
                end.map(|e| Bytes::copy_from_slice(e.as_bytes())),
            )
            .expect("valid range"),
            owner: Some(NodeId(1)),
            epoch: Epoch(1),
            replicas: vec![NodeId(2), NodeId(3)],
        }
    }

    fn map_with(partitions: Vec<PartitionInfo>) -> PartitionMap {
        let mut m = PartitionMap::new(MapVersion(1));
        for p in partitions {
            m.insert_partition(p);
        }
        m
    }

    #[test]
    fn a_single_unbounded_partition_owns_every_key() {
        let m = map_with(vec![partition(1, KS, "", None)]);
        for key in [&b""[..], b"a", b"zzzz"] {
            assert_eq!(m.lookup(KS, key).map(|p| p.id), Some(PartitionId(1)));
        }
    }

    #[test]
    fn lookup_finds_the_partition_whose_range_contains_the_key() {
        let m = map_with(vec![
            partition(1, KS, "", Some("m")),
            partition(2, KS, "m", Some("t")),
            partition(3, KS, "t", None),
        ]);

        assert_eq!(m.lookup(KS, b"a").unwrap().id, PartitionId(1));
        assert_eq!(m.lookup(KS, b"m").unwrap().id, PartitionId(2), "boundary");
        assert_eq!(m.lookup(KS, b"s").unwrap().id, PartitionId(2));
        assert_eq!(m.lookup(KS, b"t").unwrap().id, PartitionId(3), "boundary");
        assert_eq!(m.lookup(KS, b"zz").unwrap().id, PartitionId(3));
    }

    #[test]
    fn keyspaces_do_not_leak_into_each_other() {
        let m = map_with(vec![
            partition(1, KS, "", None),
            partition(2, KS2, "", Some("m")),
        ]);

        // KS2 is not covered past "m", so a key beyond it resolves to nothing
        // rather than falling through to the other keyspace's partition.
        assert_eq!(m.lookup(KS2, b"a").unwrap().id, PartitionId(2));
        assert_eq!(
            m.lookup(KS2, b"z"),
            None,
            "a key past a keyspace's coverage must not match another keyspace"
        );
        assert_eq!(m.lookup(KeyspaceId(99), b"a"), None, "unknown keyspace");
    }

    #[test]
    fn a_key_below_the_first_partition_resolves_to_nothing() {
        // Only reachable from a malformed map, and it must not panic or wrap
        // around to the last partition.
        let m = map_with(vec![partition(1, KS, "m", None)]);
        assert_eq!(m.lookup(KS, b"a"), None);
    }

    #[test]
    fn held_by_reports_both_ownership_and_replication() {
        let m = map_with(vec![partition(1, KS, "", None)]);
        assert_eq!(m.held_by(NodeId(1)).count(), 1, "owner");
        assert_eq!(m.held_by(NodeId(3)).count(), 1, "replica");
        assert_eq!(m.held_by(NodeId(9)).count(), 0, "unrelated node");
    }

    #[test]
    fn complete_coverage_passes() {
        let m = map_with(vec![
            partition(1, KS, "", Some("m")),
            partition(2, KS, "m", None),
            partition(3, KS2, "", None),
        ]);
        assert_eq!(m.check_coverage(), Ok(()));
    }

    #[test]
    fn a_gap_between_partitions_is_caught() {
        // The keys between "m" and "q" belong to nobody, so requests for them
        // would fail with no owner to blame.
        let m = map_with(vec![
            partition(1, KS, "", Some("m")),
            partition(2, KS, "q", None),
        ]);
        assert!(matches!(m.check_coverage(), Err(CoverageError::Gap { .. })));
    }

    #[test]
    fn an_overlap_between_partitions_is_caught() {
        // Two owners for the same key is the worst state the system can be in,
        // so a split or merge that produces it must fail loudly in tests.
        let m = map_with(vec![
            partition(1, KS, "", Some("q")),
            partition(2, KS, "m", None),
        ]);
        assert!(matches!(
            m.check_coverage(),
            Err(CoverageError::Overlap { .. })
        ));
    }

    #[test]
    fn coverage_that_stops_short_of_the_end_is_caught() {
        let m = map_with(vec![partition(1, KS, "", Some("m"))]);
        assert!(matches!(
            m.check_coverage(),
            Err(CoverageError::GapAtEnd { .. })
        ));
    }

    #[test]
    fn coverage_that_does_not_start_at_the_beginning_is_caught() {
        let m = map_with(vec![partition(1, KS, "b", None)]);
        assert!(matches!(
            m.check_coverage(),
            Err(CoverageError::GapAtStart { .. })
        ));
    }

    #[test]
    fn a_split_preserves_coverage() {
        let mut m = map_with(vec![partition(1, KS, "", None)]);
        assert_eq!(m.check_coverage(), Ok(()));

        // What the control plane does to split: replace the parent with two
        // children sharing a boundary.
        m.remove_partition(KS, b"");
        m.insert_partition(partition(2, KS, "", Some("m")));
        m.insert_partition(partition(3, KS, "m", None));

        assert_eq!(m.check_coverage(), Ok(()), "a split must not leave a hole");
        assert_eq!(m.lookup(KS, b"a").unwrap().id, PartitionId(2));
        assert_eq!(m.lookup(KS, b"z").unwrap().id, PartitionId(3));
    }

    #[test]
    fn removing_a_keyspace_takes_its_partitions_with_it() {
        let mut m = map_with(vec![
            partition(1, KS, "", Some("m")),
            partition(2, KS, "m", None),
            partition(3, KS2, "", None),
        ]);
        m.insert_keyspace(KeyspaceInfo {
            id: KS,
            name: KeyspaceName::new("doomed").unwrap(),
            default_ttl_millis: None,
            max_value_bytes: None,
            max_storage_bytes: None,
            max_reads_per_second: None,
            max_writes_per_second: None,
        });

        m.remove_keyspace(KS);

        assert_eq!(m.lookup(KS, b"a"), None, "no routing to a deleted tenant");
        assert!(m.keyspace(KS).is_none());
        assert!(m.keyspace_by_name("doomed").is_none());
        assert_eq!(
            m.lookup(KS2, b"a").map(|p| p.id),
            Some(PartitionId(3)),
            "other tenants are untouched"
        );
        assert_eq!(
            m.check_coverage(),
            Ok(()),
            "removing a keyspace must not leave a hole behind"
        );
    }

    #[test]
    fn map_versions_order_updates() {
        assert!(MapVersion(2) > MapVersion(1));
        assert_eq!(MapVersion(1).next(), MapVersion(2));
    }
}
