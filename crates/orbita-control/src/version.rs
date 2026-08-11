//! The cluster version, and what a binary can speak.
//!
//! Two versions exist and they are not the same thing, per
//! `docs/UPGRADES.md`. The **binary version** is what is in the image tag.
//! The **cluster version** is the protocol and format version every node has
//! agreed to speak; it lives in the replicated state and moves only when an
//! operator runs `orbita cluster finalize-upgrade`. A node speaks the cluster
//! version, not its own binary version, which is what lets nodes of different
//! binary versions work together during a rolling update.
//!
//! Before 1.0 the minor version is the compatibility unit, so a cluster
//! version is a major and a minor and nothing else. The patch component of a
//! binary version deliberately does not appear here: a patch release cannot
//! change the peer protocol or a persisted format, so two patches of the same
//! minor speak identically by rule.

use crate::codec::{CodecResult, Reader, Writer};

/// The protocol and format version a cluster has agreed to speak.
///
/// Ordering is lexicographic on `(major, minor)`, which is derive order, so
/// "newer" and `>` mean the same thing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClusterVersion {
    pub major: u32,
    pub minor: u32,
}

impl ClusterVersion {
    /// Version 0.0, which is both the value of a state machine bootstrap has
    /// not reached and the legitimate active version recovered from a 0.0
    /// cluster's log. It is a floor for the advance rule, not a "never set"
    /// sentinel.
    pub const ZERO: Self = Self { major: 0, minor: 0 };

    #[must_use]
    pub const fn new(major: u32, minor: u32) -> Self {
        Self { major, minor }
    }

    pub(crate) fn encode(self, w: &mut Writer) {
        w.u32(self.major).u32(self.minor);
    }

    pub(crate) fn decode(r: &mut Reader<'_>) -> CodecResult<Self> {
        Ok(Self {
            major: r.u32()?,
            minor: r.u32()?,
        })
    }
}

impl std::fmt::Display for ClusterVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

/// The contiguous range of cluster versions one binary can speak, inclusive
/// at both ends.
///
/// The window is the active version and the one before it, the same n-1 rule
/// Kubernetes uses between its control plane and kubelets. One window, not a
/// list: compatibility code for a version is deleted when the window moves
/// past it, and the window is one version wide by rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VersionRange {
    pub min: ClusterVersion,
    pub max: ClusterVersion,
}

impl VersionRange {
    #[must_use]
    pub const fn new(min: ClusterVersion, max: ClusterVersion) -> Self {
        Self { min, max }
    }

    /// A range covering exactly one version, which is what a binary whose
    /// minor is zero speaks: there is no previous minor to be compatible
    /// with.
    #[must_use]
    pub const fn exactly(version: ClusterVersion) -> Self {
        Self {
            min: version,
            max: version,
        }
    }

    /// Whether this binary can speak `version`.
    #[must_use]
    pub fn contains(&self, version: ClusterVersion) -> bool {
        self.min <= version && version <= self.max
    }

    pub(crate) fn encode(self, w: &mut Writer) {
        self.min.encode(w);
        self.max.encode(w);
    }

    pub(crate) fn decode(r: &mut Reader<'_>) -> CodecResult<Self> {
        Ok(Self {
            min: ClusterVersion::decode(r)?,
            max: ClusterVersion::decode(r)?,
        })
    }
}

impl std::fmt::Display for VersionRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.min == self.max {
            write!(f, "{}", self.max)
        } else {
            write!(f, "{}..{}", self.min, self.max)
        }
    }
}

/// An authoritative refusal to admit a node into the active cluster.
///
/// Both sides of the comparison are retained as data so peer clients can
/// expose an actionable readiness failure without parsing an error string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompatibilityRefusal {
    pub speaks: VersionRange,
    pub active: ClusterVersion,
}

impl std::fmt::Display for CompatibilityRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "node speaks cluster versions {} but the active cluster version is {}; install a binary whose supported range includes {}",
            self.speaks, self.active, self.active
        )
    }
}

/// The first cluster version whose control protocol carries worker lifecycle
/// state: the `ready` and `draining` claims on a status report, the planned
/// handoff built on them, and the partition merge commands at tags 22 through
/// 25.
///
/// Merge was originally assigned to 0.2 so that a 0.1 voter which could not
/// decode those tags could still be rolled back to. Nothing has ever been
/// released, so there is no such voter and no rollback to preserve: 0.1 is
/// still being defined rather than being kept compatible with. See ADR 0012.
pub const PROTOCOL_0_1: ClusterVersion = ClusterVersion::new(0, 1);

/// The next cluster version, reserved rather than in use.
///
/// Nothing gates on this today. It is kept because the compatibility window in
/// [`speaks_for`] is the mechanism a future protocol change will need, and a
/// constant that names the next version is where that change starts.
pub const PROTOCOL_0_2: ClusterVersion = ClusterVersion::new(0, 2);

/// Whether `active` is a cluster version whose protocol carries worker
/// lifecycle state.
///
/// The comparison is against the *active cluster version* and never against
/// the local binary's version, and that distinction is the whole point of
/// this function existing rather than being written out at each call site.
///
/// Two sides consult this and they run different binaries during an upgrade:
/// a worker deciding whether to put a lifecycle claim on its heartbeat, and
/// the state machine deciding whether to hold a node without one out of
/// placement. If either side asked "is the active version *mine*", the two
/// would disagree the moment a node's binary is not the one the cluster
/// finalized on — a worker one minor ahead would suppress its readiness while
/// a leader on the finalized binary still demanded it, and the leader would
/// then withhold placement from a node that is perfectly able to serve. That
/// is issue #105. Asking whether the *cluster* has reached the version that
/// carries the claim gives every node the same answer from the one piece of
/// state they all agree on, which is what `docs/UPGRADES.md` says a cluster
/// version is for.
///
/// It matters twice over for the state machine, where the same question
/// decides how a committed entry applies. See
/// [ADR 0008](../../../docs/adr/0008-a-fenced-owner-stays-a-replica.md) on
/// gating an apply-time rule: a decision that reads the running binary lets
/// one committed entry produce divergent state on two members.
#[must_use]
pub fn lifecycle_protocol_active(active: ClusterVersion) -> bool {
    active >= PROTOCOL_0_1
}

/// The cluster version this binary was built to speak, taken from the crate
/// version, which is the workspace version.
#[must_use]
pub fn binary_version() -> ClusterVersion {
    parse_binary_version(env!("CARGO_PKG_VERSION"))
}

/// The range of cluster versions this binary can speak: its own version and
/// the minor before it.
///
/// A binary at minor zero speaks only its own version. That covers both a
/// brand new major, where the previous minor belongs to a different major and
/// cannot be named by arithmetic, and 0.0, where there is nothing before at
/// all. Either way the honest claim is the narrow one.
#[must_use]
pub fn binary_speaks() -> VersionRange {
    speaks_for(binary_version())
}

/// The window [`binary_speaks`] would return for a binary built at `own`.
///
/// Public because the rule, not the current build, is what callers reason
/// about. Anything gating on compatibility has to be testable at the version
/// pairs a rolling upgrade actually produces, and `binary_speaks` can only
/// ever describe the one version this workspace happens to be at today.
#[must_use]
pub fn speaks_for(own: ClusterVersion) -> VersionRange {
    if own.minor == 0 {
        VersionRange::exactly(own)
    } else {
        VersionRange::new(ClusterVersion::new(own.major, own.minor - 1), own)
    }
}

/// Parses `major.minor` out of a Cargo version string such as `0.2.0-dev`.
///
/// The input is a compile-time constant Cargo already validated as semver, so
/// a failure here is a build that could never have been produced. Panicking
/// at first use is acceptable for that, and the tests below exercise it
/// against the real constant.
fn parse_binary_version(cargo: &str) -> ClusterVersion {
    let mut parts = cargo.split('.');
    let major = parts
        .next()
        .and_then(|p| p.parse().ok())
        .expect("CARGO_PKG_VERSION has a numeric major");
    let minor = parts
        .next()
        .and_then(|p| p.parse().ok())
        .expect("CARGO_PKG_VERSION has a numeric minor");
    ClusterVersion { major, minor }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_crate_version_parses_into_a_cluster_version() {
        // If this stops parsing, `binary_speaks` would panic at startup, so
        // the test is what keeps that panic a build-time impossibility.
        let own = binary_version();
        assert_eq!(own, binary_speaks().max);
    }

    #[test]
    fn a_zero_minor_binary_speaks_only_its_own_version() {
        // 0.1 has no predecessor to roll back to, so the window is a point.
        // The two-version window is what a 0.2 binary gets, and
        // `speaks_for` is tested at that pair directly rather than through
        // whatever version this workspace happens to be at.
        let speaks = binary_speaks();
        assert_eq!(speaks.max, PROTOCOL_0_1);
        assert!(speaks.contains(PROTOCOL_0_1));
        assert!(!speaks.contains(PROTOCOL_0_2));

        let next = speaks_for(PROTOCOL_0_2);
        assert!(next.contains(PROTOCOL_0_1) && next.contains(PROTOCOL_0_2));
    }

    #[test]
    fn a_dev_suffix_does_not_confuse_the_parser() {
        assert_eq!(parse_binary_version("0.2.0-dev"), ClusterVersion::new(0, 2));
        assert_eq!(parse_binary_version("1.10.3"), ClusterVersion::new(1, 10));
    }

    #[test]
    fn versions_order_by_major_then_minor() {
        assert!(ClusterVersion::new(1, 0) > ClusterVersion::new(0, 9));
        assert!(ClusterVersion::new(0, 3) > ClusterVersion::new(0, 2));
    }

    #[test]
    fn a_binary_speaks_its_own_minor_and_the_one_before() {
        assert_eq!(
            parse_speaks("0.4.0"),
            VersionRange::new(ClusterVersion::new(0, 3), ClusterVersion::new(0, 4))
        );
    }

    #[test]
    fn a_binary_at_minor_zero_speaks_only_itself() {
        assert_eq!(
            parse_speaks("0.0.0"),
            VersionRange::exactly(ClusterVersion::new(0, 0))
        );
        assert_eq!(
            parse_speaks("1.0.0"),
            VersionRange::exactly(ClusterVersion::new(1, 0))
        );
    }

    #[test]
    fn a_range_contains_its_ends_and_nothing_outside_them() {
        let range = VersionRange::new(ClusterVersion::new(0, 3), ClusterVersion::new(0, 4));
        assert!(range.contains(ClusterVersion::new(0, 3)));
        assert!(range.contains(ClusterVersion::new(0, 4)));
        assert!(!range.contains(ClusterVersion::new(0, 2)));
        assert!(!range.contains(ClusterVersion::new(0, 5)));
    }

    #[test]
    fn a_range_round_trips_through_the_codec() {
        let range = VersionRange::new(ClusterVersion::new(1, 2), ClusterVersion::new(1, 3));
        let mut w = Writer::new();
        range.encode(&mut w);
        let encoded = w.finish();
        let mut r = Reader::new(&encoded);
        assert_eq!(VersionRange::decode(&mut r), Ok(range));
        assert_eq!(r.done(), Ok(()));
    }

    /// The window a binary at `cargo` would speak, without rebuilding.
    fn parse_speaks(cargo: &str) -> VersionRange {
        speaks_for(parse_binary_version(cargo))
    }
}
