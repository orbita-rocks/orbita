//! Where a partition's objects live, and how their names are built.
//!
//! Names are the format's only defence against two writers producing different
//! bytes under one name, which is the single unrecoverable failure it has. They
//! carry the ownership epoch as well as a sequence, so a deposed owner and its
//! replacement cannot collide even while both are briefly writing.

use orbita_core::{Epoch, KeyspaceId, PartitionId};

pub const MANIFEST_NAME: &str = "manifest.json";
pub const SEGMENTS_DIR: &str = "segments";
pub const VALUES_DIR: &str = "values";
pub const SEGMENT_SUFFIX: &str = ".oseg";
pub const VALUE_SUFFIX: &str = ".oval";

/// Formats an identifier the way every path in this format spells one.
///
/// Zero padding is not cosmetic. A writer recovering its next sequence walks a
/// listing, and unpadded hexadecimal sorts `0x10` before `0x9`, which would
/// hand it a sequence it has already used.
#[must_use]
pub fn hex16(value: u64) -> String {
    format!("{value:016x}")
}

/// Parses one of those identifiers, rejecting anything that is not exactly
/// sixteen lowercase hexadecimal digits.
///
/// Strict because these come off a listing of a bucket that may hold objects
/// this format did not write, and a loose parse turns a stranger's file into a
/// sequence number.
#[must_use]
pub fn parse_hex16(text: &str) -> Option<u64> {
    if text.len() != 16
        || !text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return None;
    }
    u64::from_str_radix(text, 16).ok()
}

/// The name of a segment object, relative to the partition directory.
#[must_use]
pub fn segment_name(epoch: Epoch, sequence: u64) -> String {
    format!(
        "{SEGMENTS_DIR}/{}-{}{SEGMENT_SUFFIX}",
        hex16(epoch.get()),
        hex16(sequence)
    )
}

/// The name of a value object, relative to the partition directory.
#[must_use]
pub fn value_name(epoch: Epoch, sequence: u64) -> String {
    format!(
        "{VALUES_DIR}/{}-{}{VALUE_SUFFIX}",
        hex16(epoch.get()),
        hex16(sequence)
    )
}

/// Recovers the epoch and sequence from a relative object name.
///
/// Returns `None` for anything that is not a name this format produces, which
/// is how a listing that contains unrelated objects is walked safely.
#[must_use]
pub fn parse_object_name(relative: &str) -> Option<(Epoch, u64)> {
    let stem = relative
        .strip_prefix(SEGMENTS_DIR)
        .and_then(|rest| rest.strip_prefix('/'))
        .and_then(|rest| rest.strip_suffix(SEGMENT_SUFFIX))
        .or_else(|| {
            relative
                .strip_prefix(VALUES_DIR)
                .and_then(|rest| rest.strip_prefix('/'))
                .and_then(|rest| rest.strip_suffix(VALUE_SUFFIX))
        })?;
    let (epoch, sequence) = stem.split_once('-')?;
    Some((Epoch(parse_hex16(epoch)?), parse_hex16(sequence)?))
}

/// One partition's directory in a bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionPath {
    prefix: String,
    keyspace_id: KeyspaceId,
    partition_id: PartitionId,
}

impl PartitionPath {
    /// Builds the path for one partition under `root`, which may be empty when
    /// the bucket holds nothing else.
    #[must_use]
    pub fn new(root: &str, keyspace_id: KeyspaceId, partition_id: PartitionId) -> Self {
        let root = root.trim_end_matches('/');
        let head = if root.is_empty() {
            String::new()
        } else {
            format!("{root}/")
        };
        Self {
            prefix: format!(
                "{head}keyspaces/{}/partitions/{}/",
                hex16(keyspace_id.get()),
                hex16(partition_id.get())
            ),
            keyspace_id,
            partition_id,
        }
    }

    #[must_use]
    pub fn keyspace_id(&self) -> KeyspaceId {
        self.keyspace_id
    }

    #[must_use]
    pub fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    /// Everything this partition owns sits under here, and nothing else does.
    #[must_use]
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The full object key for a name that the manifest holds relative.
    #[must_use]
    pub fn object(&self, relative: &str) -> String {
        format!("{}{relative}", self.prefix)
    }

    #[must_use]
    pub fn manifest(&self) -> String {
        self.object(MANIFEST_NAME)
    }

    #[must_use]
    pub fn segments_prefix(&self) -> String {
        self.object(&format!("{SEGMENTS_DIR}/"))
    }

    #[must_use]
    pub fn values_prefix(&self) -> String {
        self.object(&format!("{VALUES_DIR}/"))
    }

    /// Turns a full object key back into the name a manifest would hold, or
    /// `None` if the key is not under this partition.
    #[must_use]
    pub fn relative<'a>(&self, key: &'a str) -> Option<&'a str> {
        key.strip_prefix(&self.prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_are_padded_so_a_listing_sorts_numerically() {
        assert_eq!(hex16(9), "0000000000000009");
        assert_eq!(hex16(16), "0000000000000010");
        assert!(hex16(9) < hex16(16), "a listing walks these in order");
    }

    #[test]
    fn identifiers_round_trip() {
        for value in [0, 1, 255, u64::MAX] {
            assert_eq!(parse_hex16(&hex16(value)), Some(value));
        }
    }

    #[test]
    fn loose_identifiers_are_rejected() {
        assert_eq!(parse_hex16("9"), None, "unpadded");
        assert_eq!(parse_hex16("000000000000000F"), None, "uppercase");
        assert_eq!(parse_hex16("00000000000000000"), None, "too long");
        assert_eq!(parse_hex16("000000000000000g"), None, "not hexadecimal");
    }

    #[test]
    fn object_names_round_trip_through_a_listing() {
        let segment = segment_name(Epoch(6), 17);
        assert_eq!(
            segment, "segments/0000000000000006-0000000000000011.oseg",
            "the example in the specification"
        );
        assert_eq!(parse_object_name(&segment), Some((Epoch(6), 17)));

        let value = value_name(Epoch(6), 17);
        assert_eq!(value, "values/0000000000000006-0000000000000011.oval");
        assert_eq!(parse_object_name(&value), Some((Epoch(6), 17)));
    }

    #[test]
    fn unrelated_names_in_a_listing_are_not_mistaken_for_sequences() {
        for name in [
            "manifest.json",
            "segments/nope.oseg",
            "segments/0000000000000006-0000000000000011.txt",
            "elsewhere/0000000000000006-0000000000000011.oseg",
            "segments/0000000000000006.oseg",
        ] {
            assert_eq!(parse_object_name(name), None, "{name}");
        }
    }

    #[test]
    fn a_partition_directory_matches_the_specified_layout() {
        let path = PartitionPath::new("orbita", KeyspaceId(1), PartitionId(7));
        assert_eq!(
            path.manifest(),
            "orbita/keyspaces/0000000000000001/partitions/0000000000000007/manifest.json"
        );
        assert_eq!(
            path.relative(&path.object("segments/x.oseg")),
            Some("segments/x.oseg")
        );
    }

    #[test]
    fn an_empty_root_leaves_no_leading_separator() {
        let path = PartitionPath::new("", KeyspaceId(1), PartitionId(7));
        assert!(path.prefix().starts_with("keyspaces/"), "{}", path.prefix());
    }

    #[test]
    fn a_trailing_separator_on_the_root_is_not_doubled() {
        assert_eq!(
            PartitionPath::new("orbita/", KeyspaceId(1), PartitionId(7)),
            PartitionPath::new("orbita", KeyspaceId(1), PartitionId(7))
        );
    }
}
