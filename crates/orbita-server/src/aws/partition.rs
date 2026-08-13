//! Which AWS partition a region belongs to, and therefore what its endpoints
//! are called.
//!
//! `sts.{region}.amazonaws.com` is only correct in the commercial partition.
//! In the China partition the same service answers on `amazonaws.com.cn`, and
//! the isolated partitions use their own suffixes entirely. A node that builds
//! the commercial name in Beijing does not get a 403 it can act on — it gets a
//! DNS failure on every credential refresh, which looks like a network problem
//! and is not one.
//!
//! # Why a prefix table and not a lookup service
//!
//! AWS publishes `endpoints.json`, which is large, versioned, and would have to
//! be vendored and kept fresh. The partition boundaries themselves, in
//! contrast, are region *name prefixes* that have been stable for a decade and
//! are the part `endpoints.json` states first. So this table encodes the
//! boundaries, and anything it gets wrong is repairable in one line of
//! configuration: every consumer of this exposes an explicit endpoint
//! override, which is what a private link or a partition invented after this
//! release needs anyway.
//!
//! GovCloud is deliberately absent from the table: `us-gov-*` regions are a
//! separate partition for IAM purposes but still resolve under
//! `amazonaws.com`, so the commercial default is already right for them and a
//! GovCloud entry here would only be a place to introduce a typo.

/// The DNS suffix AWS service endpoints hang off in `region`'s partition.
///
/// Returns the commercial suffix for anything unrecognised. That is the safe
/// direction: an unknown region is far more likely to be a new commercial one
/// (or a MinIO deployment that put something arbitrary in the region field)
/// than a new isolated partition, and the isolated partitions are exactly the
/// deployments that set an explicit endpoint anyway.
pub(crate) fn dns_suffix(region: &str) -> &'static str {
    // Ordered longest-prefix first, because `us-iso-` is a prefix of nothing
    // but `us-isob-` and `us-isof-` would otherwise be swallowed by a shorter
    // match if one were added above them.
    const SUFFIXES: &[(&str, &str)] = &[
        // China: the one partition a commercial deployment plausibly reaches
        // for by accident, and the one the review called out.
        ("cn-", "amazonaws.com.cn"),
        ("us-isob-", "sc2s.sgov.gov"),
        ("us-isof-", "csp.hci.ic.gov"),
        ("us-iso-", "c2s.ic.gov"),
        ("eu-isoe-", "cloud.adc-e.uk"),
    ];

    SUFFIXES
        .iter()
        .find(|(prefix, _)| region.starts_with(prefix))
        .map_or("amazonaws.com", |(_, suffix)| suffix)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_commercial_region_uses_the_commercial_suffix() {
        assert_eq!(dns_suffix("us-east-1"), "amazonaws.com");
        assert_eq!(dns_suffix("eu-west-1"), "amazonaws.com");
        assert_eq!(dns_suffix("ap-southeast-2"), "amazonaws.com");
    }

    #[test]
    fn a_china_region_uses_the_china_suffix() {
        assert_eq!(dns_suffix("cn-north-1"), "amazonaws.com.cn");
        assert_eq!(dns_suffix("cn-northwest-1"), "amazonaws.com.cn");
    }

    #[test]
    fn govcloud_stays_on_the_commercial_suffix() {
        // A separate partition for IAM, but the same DNS suffix, and getting
        // this wrong would break the deployment it was meant to fix.
        assert_eq!(dns_suffix("us-gov-west-1"), "amazonaws.com");
        assert_eq!(dns_suffix("us-gov-east-1"), "amazonaws.com");
    }

    #[test]
    fn the_isolated_partitions_each_have_their_own_suffix() {
        assert_eq!(dns_suffix("us-iso-east-1"), "c2s.ic.gov");
        assert_eq!(dns_suffix("us-isob-east-1"), "sc2s.sgov.gov");
        assert_eq!(dns_suffix("us-isof-south-1"), "csp.hci.ic.gov");
        assert_eq!(dns_suffix("eu-isoe-west-1"), "cloud.adc-e.uk");
    }

    #[test]
    fn an_unrecognised_region_falls_back_to_the_commercial_suffix() {
        // MinIO deployments put arbitrary strings here, and a new commercial
        // region is far likelier than a new isolated partition.
        assert_eq!(dns_suffix("auto"), "amazonaws.com");
        assert_eq!(dns_suffix(""), "amazonaws.com");
    }
}
