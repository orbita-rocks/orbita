//! Quota and rate-limit enforcement, driven through a real node's one admission
//! boundary.
//!
//! The bucket arithmetic is unit-tested in [`crate::quota`]; this is the other
//! half of the promise — that a configured keyspace quota actually turns a
//! request into a `ResourceExhausted`, that storage and rate refusals stay
//! distinguishable, and that a throttled tenant does not reach across into a
//! neighbour. It runs under the simulator so virtual time stands still between
//! requests unless a test advances it, which is what makes a rate assertion
//! deterministic.
//!
//! Because credential and quota enforcement were folded into a single
//! `Node::admit` boundary, this module also pins the order they run in: the
//! credential check comes first, so an unauthenticated caller is refused before
//! it can spend a rate token or be measured against storage, and quotas still
//! bite when authentication is off.

use crate::auth::Authenticator;
use crate::map_source::{BoxedMapSource, StaticMapSource};
use crate::node::{DataLayout, Node};

use orbita_control::{hash_secret, Credential, CredentialSnapshot, Permission};
use orbita_core::{
    Epoch, Error, KeyRange, KeyspaceId, KeyspaceInfo, KeyspaceName, MapVersion, NodeId,
    PartitionId, PartitionInfo, PartitionMap,
};
use orbita_format::testing::MemoryStore;
use orbita_proto::v1::{GetRequest, SetRequest};
use orbita_runtime::Runtime;
use orbita_sim::{SimRuntime, Simulation};

use std::sync::Arc;

/// A keyspace configured with whatever quotas a test cares about, plus a single
/// unbounded partition this node owns.
struct KeyspaceSpec {
    id: u64,
    name: &'static str,
    max_storage_bytes: Option<u64>,
    max_reads_per_second: Option<u32>,
    max_writes_per_second: Option<u32>,
}

fn map(specs: &[KeyspaceSpec]) -> PartitionMap {
    let mut map = PartitionMap::new(MapVersion(1));
    for spec in specs {
        let keyspace = KeyspaceId(spec.id);
        map.insert_keyspace(KeyspaceInfo {
            id: keyspace,
            name: KeyspaceName::new(spec.name).unwrap(),
            default_ttl_millis: None,
            max_value_bytes: None,
            max_storage_bytes: spec.max_storage_bytes,
            max_reads_per_second: spec.max_reads_per_second,
            max_writes_per_second: spec.max_writes_per_second,
        });
        map.insert_partition(PartitionInfo {
            id: PartitionId(spec.id),
            keyspace,
            range: KeyRange::unbounded(),
            owner: Some(NodeId(1)),
            epoch: Epoch(1),
            replicas: Vec::new(),
        });
    }
    assert_eq!(
        map.check_coverage(),
        Ok(()),
        "the fixture must be coverable"
    );
    map
}

/// Starts a node whose quotas are the ones in `specs`, with the authentication
/// mode and credential set the caller has already chosen.
///
/// The authenticator is what the node's admission boundary runs first, so a
/// test can hold quotas fixed and vary only whether — and with what credential —
/// a request authenticates.
fn start_with_auth(
    sim: &Simulation,
    specs: &[KeyspaceSpec],
    authenticator: Arc<Authenticator<<SimRuntime as Runtime>::Clock>>,
) -> Arc<Node<SimRuntime>> {
    let runtime = sim.add_node(NodeId(1));
    let layout = DataLayout {
        store: Arc::new(MemoryStore::new()),
        wal_root: "wal".to_string(),
        wal_segment_bytes: crate::DEFAULT_WAL_SEGMENT_BYTES,
    };
    let source = BoxedMapSource::new(StaticMapSource::new(map(specs)));
    sim.block_on(async move {
        Node::start(
            runtime,
            NodeId(1),
            layout,
            source,
            crate::DEFAULT_LEASE_DURATION,
            Arc::new(crate::ReadinessGate::new()),
            authenticator,
        )
        .await
        .expect("the node starts")
    })
}

/// Starts a node with authentication off, the mode the quota-only tests want:
/// admission still resolves the keyspace and charges the rate and storage
/// limits, it just never asks for a credential.
fn start(sim: &Simulation, specs: &[KeyspaceSpec]) -> Arc<Node<SimRuntime>> {
    let clock = sim.add_node(NodeId(1)).clock().clone();
    let authenticator = Arc::new(Authenticator::new(false, None, std::time::Duration::from_secs(86_400), clock));
    start_with_auth(sim, specs, authenticator)
}

/// Starts a node with authentication on and a single credential that carries
/// `secret` and is scoped to read and write every keyspace in `specs`.
fn start_authed(sim: &Simulation, specs: &[KeyspaceSpec], secret: &str) -> Arc<Node<SimRuntime>> {
    let clock = sim.add_node(NodeId(1)).clock().clone();
    let authenticator = Arc::new(Authenticator::new(true, None, std::time::Duration::from_secs(86_400), clock));
    authenticator.refresh(CredentialSnapshot::new(vec![Credential {
        id: "cred-1".into(),
        secret_hash: hash_secret(secret),
        keyspaces: specs.iter().map(|spec| spec.name.to_string()).collect(),
        permissions: vec![Permission::Read, Permission::Write],
        description: String::new(),
        created_at_millis: 0,
        expires_at_millis: None,
    }]));
    start_with_auth(sim, specs, authenticator)
}

/// The `authorization` header a client presenting `secret` would send.
fn bearer(secret: &str) -> String {
    format!("Bearer {secret}")
}

fn set(keyspace: &str, key: &str, value: &[u8]) -> SetRequest {
    SetRequest {
        keyspace: keyspace.to_string(),
        key: key.as_bytes().to_vec(),
        value: value.to_vec(),
        ttl_millis: None,
        condition: None,
    }
}

fn get(keyspace: &str, key: &str) -> GetRequest {
    GetRequest {
        keyspace: keyspace.to_string(),
        key: key.as_bytes().to_vec(),
    }
}

#[test]
fn a_write_that_would_exceed_the_storage_cap_is_refused_as_storage() {
    let sim = Simulation::new(1);
    let node = start(
        &sim,
        &[KeyspaceSpec {
            id: 1,
            name: "default",
            max_storage_bytes: Some(4),
            max_reads_per_second: None,
            max_writes_per_second: None,
        }],
    );

    let refused = {
        let node = Arc::clone(&node);
        sim.block_on(async move { node.set(set("default", "k", b"12345"), false, None).await })
    }
    .expect_err("a five-byte value cannot fit under a four-byte cap");
    match refused {
        Error::QuotaExceeded(message) => assert!(
            message.contains("storage"),
            "a storage refusal has to be tellable from a rate one, got: {message}"
        ),
        other => panic!("expected a quota refusal, got {other:?}"),
    }
}

#[test]
fn a_write_under_the_storage_cap_is_admitted() {
    let sim = Simulation::new(1);
    let node = start(
        &sim,
        &[KeyspaceSpec {
            id: 1,
            name: "default",
            max_storage_bytes: Some(1 << 20),
            max_reads_per_second: None,
            max_writes_per_second: None,
        }],
    );

    let written = {
        let node = Arc::clone(&node);
        sim.block_on(async move { node.set(set("default", "k", b"small"), false, None).await })
    }
    .expect("a small write fits under a generous cap");
    assert!(written.applied);
}

#[test]
fn exceeding_the_write_rate_is_refused_as_a_write_rate() {
    let sim = Simulation::new(1);
    let node = start(
        &sim,
        &[KeyspaceSpec {
            id: 1,
            name: "default",
            max_storage_bytes: None,
            max_reads_per_second: None,
            max_writes_per_second: Some(1),
        }],
    );

    // Virtual time does not advance between these, so the one-per-second bucket
    // starts full, admits one, and refuses the next.
    let first = {
        let node = Arc::clone(&node);
        sim.block_on(async move { node.set(set("default", "a", b"v"), false, None).await })
    };
    assert!(first.expect("the first write is within budget").applied);

    let second = {
        let node = Arc::clone(&node);
        sim.block_on(async move { node.set(set("default", "b", b"v"), false, None).await })
    }
    .expect_err("the second write in the same instant is over the write rate");
    match second {
        Error::QuotaExceeded(message) => assert!(
            message.contains("write rate"),
            "a write-rate refusal names the write rate, got: {message}"
        ),
        other => panic!("expected a quota refusal, got {other:?}"),
    }
}

#[test]
fn exceeding_the_read_rate_is_refused_independently_of_writes() {
    let sim = Simulation::new(1);
    let node = start(
        &sim,
        &[KeyspaceSpec {
            id: 1,
            name: "default",
            max_storage_bytes: None,
            max_reads_per_second: Some(1),
            // A generous write rate proves the read budget is its own counter.
            max_writes_per_second: Some(1_000),
        }],
    );

    // A write does not spend the read budget.
    {
        let node = Arc::clone(&node);
        sim.block_on(async move { node.set(set("default", "a", b"v"), false, None).await })
    }
    .expect("a write is not charged against reads");

    let first = {
        let node = Arc::clone(&node);
        sim.block_on(async move { node.get(get("default", "a"), false, None).await })
    };
    assert!(first.expect("the first read is within budget").found);

    let second = {
        let node = Arc::clone(&node);
        sim.block_on(async move { node.get(get("default", "a"), false, None).await })
    }
    .expect_err("the second read in the same instant is over the read rate");
    match second {
        Error::QuotaExceeded(message) => assert!(
            message.contains("read rate"),
            "a read-rate refusal names the read rate, got: {message}"
        ),
        other => panic!("expected a quota refusal, got {other:?}"),
    }
}

#[test]
fn a_throttled_neighbour_does_not_stop_a_keyspace_under_its_cap() {
    let sim = Simulation::new(1);
    // Two keyspaces on the one node: a noisy tenant capped at one write per
    // second, and a quiet neighbour with no cap at all.
    let node = start(
        &sim,
        &[
            KeyspaceSpec {
                id: 1,
                name: "noisy",
                max_storage_bytes: None,
                max_reads_per_second: None,
                max_writes_per_second: Some(1),
            },
            KeyspaceSpec {
                id: 2,
                name: "quiet",
                max_storage_bytes: None,
                max_reads_per_second: None,
                max_writes_per_second: None,
            },
        ],
    );

    // Saturate the noisy tenant: the first write lands, the second is refused.
    {
        let node = Arc::clone(&node);
        sim.block_on(async move { node.set(set("noisy", "a", b"v"), false, None).await })
    }
    .expect("the noisy tenant's first write is within budget");
    {
        let node = Arc::clone(&node);
        sim.block_on(async move { node.set(set("noisy", "b", b"v"), false, None).await })
    }
    .expect_err("the noisy tenant is now throttled");

    // The neighbour, reached without touching the noisy tenant's limiter, is
    // completely unaffected.
    let quiet = {
        let node = Arc::clone(&node);
        sim.block_on(async move { node.set(set("quiet", "a", b"v"), false, None).await })
    }
    .expect("a keyspace under its cap must not feel a throttled neighbour");
    assert!(quiet.applied);
}

#[test]
fn the_credential_is_checked_before_any_quota_is_charged() {
    let sim = Simulation::new(1);
    // A keyspace that is simultaneously over its storage cap for the incoming
    // write (five bytes into a four-byte cap) and down to its last write token
    // (one per second, and virtual time will not advance). Either quota, on its
    // own, would refuse this write.
    let node = start_authed(
        &sim,
        &[KeyspaceSpec {
            id: 1,
            name: "default",
            max_storage_bytes: Some(4),
            max_reads_per_second: None,
            max_writes_per_second: Some(1),
        }],
        "s3cret",
    );

    // An unauthenticated write. If the credential gate did not come first, this
    // request would be refused as ResourceExhausted by the storage or rate
    // check. It must instead be refused as Unauthenticated, and — the whole
    // point — leave the rate token unspent and nothing measured against
    // storage.
    let refused = {
        let node = Arc::clone(&node);
        sim.block_on(async move { node.set(set("default", "k", b"12345"), false, None).await })
    }
    .expect_err("an unauthenticated write is refused");
    assert!(
        matches!(refused, Error::Unauthenticated),
        "the credential check must win over both quotas, got: {refused:?}"
    );

    // The proof the deny happened before any counter moved: an authenticated
    // write that fits under the storage cap is admitted, and consumes the one
    // write token. Had the rejected request spent the token or bumped the
    // storage figure, this would be refused as ResourceExhausted instead.
    let credential = bearer("s3cret");
    let admitted = {
        let node = Arc::clone(&node);
        sim.block_on(async move {
            node.set(set("default", "k", b"ok"), false, Some(credential.as_str()))
                .await
        })
    }
    .expect("the token was never spent, and two bytes fit under a four-byte cap");
    assert!(admitted.applied);
}

#[test]
fn a_wrong_credential_is_refused_before_any_quota_is_charged() {
    let sim = Simulation::new(1);
    // One write token, so a request that reached the rate check would spend it.
    let node = start_authed(
        &sim,
        &[KeyspaceSpec {
            id: 1,
            name: "default",
            max_storage_bytes: None,
            max_reads_per_second: None,
            max_writes_per_second: Some(1),
        }],
        "s3cret",
    );

    // A present-but-wrong secret is Unauthenticated, not a permission or quota
    // problem.
    let wrong = bearer("nope");
    let refused = {
        let node = Arc::clone(&node);
        sim.block_on(async move {
            node.set(set("default", "a", b"v"), false, Some(wrong.as_str()))
                .await
        })
    }
    .expect_err("a wrong secret is refused");
    assert!(
        matches!(refused, Error::Unauthenticated),
        "a bad credential must be refused before the rate bucket, got: {refused:?}"
    );

    // The token survives, so the real credential's first write still lands.
    let credential = bearer("s3cret");
    let admitted = {
        let node = Arc::clone(&node);
        sim.block_on(async move {
            node.set(set("default", "a", b"v"), false, Some(credential.as_str()))
                .await
        })
    }
    .expect("the rejected request never reached the rate bucket");
    assert!(admitted.applied);
}

#[test]
fn quotas_still_enforce_when_authentication_is_off() {
    let sim = Simulation::new(1);
    // Two keyspaces so a single auth-off node can show both refusals: one
    // capped on storage, one on write rate.
    let node = start(
        &sim,
        &[
            KeyspaceSpec {
                id: 1,
                name: "storage-capped",
                max_storage_bytes: Some(4),
                max_reads_per_second: None,
                max_writes_per_second: None,
            },
            KeyspaceSpec {
                id: 2,
                name: "rate-capped",
                max_storage_bytes: None,
                max_reads_per_second: None,
                max_writes_per_second: Some(1),
            },
        ],
    );

    // Auth off never asks for a credential, but the keyspace is still resolved
    // and the storage cap still refuses an oversized write.
    let over_storage = {
        let node = Arc::clone(&node);
        sim.block_on(async move {
            node.set(set("storage-capped", "k", b"12345"), false, None)
                .await
        })
    }
    .expect_err("a five-byte value cannot fit under a four-byte cap");
    match over_storage {
        Error::QuotaExceeded(message) => assert!(
            message.contains("storage"),
            "auth off must not disable the storage cap, got: {message}"
        ),
        other => panic!("expected a storage quota refusal, got {other:?}"),
    }

    // And the rate cap still refuses the second write in the same instant.
    {
        let node = Arc::clone(&node);
        sim.block_on(async move { node.set(set("rate-capped", "a", b"v"), false, None).await })
    }
    .expect("the first write is within budget");
    let over_rate = {
        let node = Arc::clone(&node);
        sim.block_on(async move { node.set(set("rate-capped", "b", b"v"), false, None).await })
    }
    .expect_err("the second write in the same instant is over the write rate");
    match over_rate {
        Error::QuotaExceeded(message) => assert!(
            message.contains("write rate"),
            "auth off must not disable the rate cap, got: {message}"
        ),
        other => panic!("expected a rate quota refusal, got {other:?}"),
    }
}

#[test]
fn a_valid_credential_within_its_caps_reads_and_writes_end_to_end() {
    let sim = Simulation::new(1);
    let node = start_authed(
        &sim,
        &[KeyspaceSpec {
            id: 1,
            name: "default",
            max_storage_bytes: Some(1 << 20),
            max_reads_per_second: Some(1_000),
            max_writes_per_second: Some(1_000),
        }],
        "s3cret",
    );

    // A write with the right credential, comfortably inside every cap, is
    // admitted and applied.
    let credential = bearer("s3cret");
    let written = {
        let node = Arc::clone(&node);
        let credential = credential.clone();
        sim.block_on(async move {
            node.set(
                set("default", "k", b"hello"),
                false,
                Some(credential.as_str()),
            )
            .await
        })
    }
    .expect("a valid, in-budget write is admitted");
    assert!(written.applied);

    // And the same credential reads it straight back.
    let read = {
        let node = Arc::clone(&node);
        sim.block_on(async move {
            node.get(get("default", "k"), false, Some(credential.as_str()))
                .await
        })
    }
    .expect("a valid, in-budget read is admitted");
    assert!(read.found);
    assert_eq!(read.value, b"hello");
}
