//! Durable identity and single-cluster bootstrap for combined nodes.
//!
//! Peer discovery alone cannot decide that two fresh partitions are the same
//! cluster. The shared object store supplies the one conditional-create point
//! that both partitions must agree through; see ADR 0010.

use bytes::Bytes;
use orbita_core::{Error, NodeId, Result};
use orbita_objectstore::{ObjectError, ObjectStore, Precondition};
use serde::{Deserialize, Serialize};

use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

const IDENTITY_VERSION: u8 = 1;
const LOCAL_IDENTITY: &str = "control/node-identity.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LocalIdentity {
    version: u8,
    node_id: u64,
    node_identity: String,
    cluster_identity: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ClusterIdentity {
    version: u8,
    cluster_identity: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Claim {
    version: u8,
    cluster_identity: String,
    node_identity: String,
    node_id: u64,
    address: String,
    failure_domain: String,
    eligible: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Certificate {
    version: u8,
    cluster_identity: String,
    voters: Vec<Claim>,
}

/// The immutable bootstrap result every combined node opens Raft from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BootstrapCertificate {
    pub cluster_identity: String,
    pub node_identity: String,
    pub voters: Vec<(NodeId, String)>,
}

/// Joins the object-store identity and waits for one three-node certificate.
///
/// The local identity is fsynced before its claim is visible. A node can
/// therefore never help certify an identity it would forget after a restart.
pub(crate) async fn bootstrap(
    store: Arc<dyn ObjectStore>,
    data_dir: &Path,
    cluster_name: &str,
    node_id: NodeId,
    address: &str,
    failure_domain: &str,
    eligible: bool,
) -> Result<BootstrapCertificate> {
    let mut local = load_or_create_local(data_dir, node_id)?;
    let prefix = format!("clusters/{cluster_name}");
    let identity_key = format!("{prefix}/identity");
    let proposed = ClusterIdentity {
        version: IDENTITY_VERSION,
        cluster_identity: random_identity()?,
    };
    let proposed_bytes = encode(&proposed)?;
    let cluster = match store
        .put_if(
            &identity_key,
            proposed_bytes.clone(),
            Precondition::NotExists,
        )
        .await
    {
        Ok(_) => proposed,
        Err(ObjectError::PreconditionFailed(_)) => {
            let (bytes, _) = store.get(&identity_key).await.map_err(object_error)?;
            decode::<ClusterIdentity>(&bytes, "cluster identity")?
        }
        Err(error) => return Err(object_error(error)),
    };
    if cluster.version != IDENTITY_VERSION {
        return Err(Error::InvalidArgument(format!(
            "cluster identity uses unsupported format {}",
            cluster.version
        )));
    }
    if let Some(held) = &local.cluster_identity {
        if held != &cluster.cluster_identity {
            return Err(Error::InvalidArgument(format!(
                "data directory belongs to cluster {held}, but object storage names {}; use the original bucket or an empty data directory",
                cluster.cluster_identity
            )));
        }
    } else {
        local.cluster_identity = Some(cluster.cluster_identity.clone());
        persist_local(data_dir, &local)?;
    }

    let claim = Claim {
        version: IDENTITY_VERSION,
        cluster_identity: cluster.cluster_identity.clone(),
        node_identity: local.node_identity.clone(),
        node_id: node_id.get(),
        address: address.to_string(),
        failure_domain: failure_domain.to_string(),
        eligible,
    };
    let claim_key = format!("{prefix}/bootstrap/claims/{}", local.node_identity);
    match store
        .put_if(&claim_key, encode(&claim)?, Precondition::NotExists)
        .await
    {
        Ok(_) | Err(ObjectError::PreconditionFailed(_)) => {}
        Err(error) => return Err(object_error(error)),
    }

    let certificate_key = format!("{prefix}/bootstrap/certificate");
    loop {
        match store.get(&certificate_key).await {
            Ok((bytes, _)) => {
                let certificate = decode::<Certificate>(&bytes, "bootstrap certificate")?;
                let mut certificate = validate_certificate(certificate, &cluster.cluster_identity)?;
                certificate.node_identity = local.node_identity.clone();
                return Ok(certificate);
            }
            Err(ObjectError::NotFound(_)) => {}
            Err(error) => return Err(object_error(error)),
        }

        let mut claims = Vec::new();
        for object in store
            .list(&format!("{prefix}/bootstrap/claims/"))
            .await
            .map_err(object_error)?
        {
            let (bytes, _) = store.get(&object.key).await.map_err(object_error)?;
            let claim = decode::<Claim>(&bytes, "bootstrap claim")?;
            if claim.version == IDENTITY_VERSION
                && claim.eligible
                && claim.cluster_identity == cluster.cluster_identity
            {
                claims.push(claim);
            }
        }
        claims = select_claims(claims);
        if claims.len() >= 3 {
            let certificate = Certificate {
                version: IDENTITY_VERSION,
                cluster_identity: cluster.cluster_identity.clone(),
                voters: claims.into_iter().take(3).collect(),
            };
            match store
                .put_if(
                    &certificate_key,
                    encode(&certificate)?,
                    Precondition::NotExists,
                )
                .await
            {
                Ok(_) | Err(ObjectError::PreconditionFailed(_)) => {}
                Err(error) => return Err(object_error(error)),
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn select_claims(mut claims: Vec<Claim>) -> Vec<Claim> {
    claims.sort_by(|a, b| a.node_identity.cmp(&b.node_identity));
    claims.dedup_by_key(|claim| claim.node_id);

    let mut selected = Vec::with_capacity(claims.len());
    let mut domains = BTreeSet::new();
    for claim in &claims {
        if !claim.failure_domain.is_empty() && domains.insert(claim.failure_domain.clone()) {
            selected.push(claim.clone());
        }
    }
    for claim in claims {
        if !selected
            .iter()
            .any(|selected| selected.node_identity == claim.node_identity)
        {
            selected.push(claim);
        }
    }
    selected
}

fn validate_certificate(
    certificate: Certificate,
    expected_cluster: &str,
) -> Result<BootstrapCertificate> {
    if certificate.version != IDENTITY_VERSION
        || certificate.cluster_identity != expected_cluster
        || certificate.voters.len() != 3
        || certificate.voters.iter().any(|claim| {
            !claim.eligible
                || claim.cluster_identity != expected_cluster
                || claim.version != IDENTITY_VERSION
        })
    {
        return Err(Error::InvalidArgument(
            "the object-store bootstrap certificate is invalid".into(),
        ));
    }
    let mut ids: Vec<u64> = certificate
        .voters
        .iter()
        .map(|claim| claim.node_id)
        .collect();
    ids.sort_unstable();
    ids.dedup();
    if ids.len() != 3 {
        return Err(Error::InvalidArgument(
            "the bootstrap certificate repeats a numeric node id".into(),
        ));
    }
    Ok(BootstrapCertificate {
        cluster_identity: certificate.cluster_identity,
        node_identity: String::new(),
        voters: certificate
            .voters
            .into_iter()
            .map(|claim| (NodeId(claim.node_id), claim.address))
            .collect(),
    })
}

fn load_or_create_local(data_dir: &Path, node_id: NodeId) -> Result<LocalIdentity> {
    let path = data_dir.join(LOCAL_IDENTITY);
    if path.exists() {
        let bytes = std::fs::read(&path)
            .map_err(|error| Error::Internal(format!("reading {}: {error}", path.display())))?;
        let identity = decode::<LocalIdentity>(&bytes, "local node identity")?;
        if identity.version != IDENTITY_VERSION || identity.node_id != node_id.get() {
            return Err(Error::InvalidArgument(format!(
                "configured node id {node_id} does not match durable node identity {}",
                identity.node_id
            )));
        }
        return Ok(identity);
    }
    let identity = LocalIdentity {
        version: IDENTITY_VERSION,
        node_id: node_id.get(),
        node_identity: random_identity()?,
        cluster_identity: None,
    };
    persist_local(data_dir, &identity)?;
    Ok(identity)
}

fn persist_local(data_dir: &Path, identity: &LocalIdentity) -> Result<()> {
    let path = data_dir.join(LOCAL_IDENTITY);
    let parent = path.parent().expect("identity path has a parent");
    std::fs::create_dir_all(parent).map_err(|error| {
        Error::Internal(format!(
            "creating identity directory {}: {error}",
            parent.display()
        ))
    })?;
    let temporary = temporary_path(&path);
    let mut file = File::create(&temporary)
        .map_err(|error| Error::Internal(format!("creating {}: {error}", temporary.display())))?;
    file.write_all(&encode(identity)?)
        .and_then(|()| file.sync_all())
        .map_err(|error| Error::Internal(format!("persisting {}: {error}", path.display())))?;
    std::fs::rename(&temporary, &path)
        .map_err(|error| Error::Internal(format!("installing {}: {error}", path.display())))?;
    OpenOptions::new()
        .read(true)
        .open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| Error::Internal(format!("syncing {}: {error}", parent.display())))
}

fn temporary_path(path: &Path) -> PathBuf {
    path.with_extension(format!("tmp-{}", std::process::id()))
}

fn random_identity() -> Result<String> {
    let mut bytes = [0u8; 16];
    getrandom::getrandom(&mut bytes)
        .map_err(|error| Error::Internal(format!("drawing a durable identity: {error}")))?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn encode(value: &impl Serialize) -> Result<Bytes> {
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(|error| Error::Internal(format!("encoding bootstrap state: {error}")))
}

fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8], what: &str) -> Result<T> {
    serde_json::from_slice(bytes)
        .map_err(|error| Error::InvalidArgument(format!("invalid {what}: {error}")))
}

fn object_error(error: ObjectError) -> Error {
    Error::Unavailable(format!("cluster bootstrap object store: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use orbita_format::testing::MemoryStore;

    #[test]
    fn a_node_identity_is_create_once_and_rejects_a_reused_volume() {
        let root = std::env::temp_dir().join(format!(
            "orbita-bootstrap-identity-{}-{}",
            std::process::id(),
            random_identity().unwrap()
        ));
        let first = load_or_create_local(&root, NodeId(7)).unwrap();
        let second = load_or_create_local(&root, NodeId(7)).unwrap();
        assert_eq!(first.node_identity, second.node_identity);
        assert!(load_or_create_local(&root, NodeId(8)).is_err());
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn a_certificate_needs_three_distinct_eligible_identities() {
        let claim = |id| Claim {
            version: IDENTITY_VERSION,
            cluster_identity: "cluster".into(),
            node_identity: format!("node-{id}"),
            node_id: id,
            address: format!("n{id}:7101"),
            failure_domain: String::new(),
            eligible: true,
        };
        let valid = Certificate {
            version: IDENTITY_VERSION,
            cluster_identity: "cluster".into(),
            voters: vec![claim(1), claim(2), claim(3)],
        };
        assert!(validate_certificate(valid, "cluster").is_ok());
        let repeated = Certificate {
            version: IDENTITY_VERSION,
            cluster_identity: "cluster".into(),
            voters: vec![claim(1), claim(1), claim(3)],
        };
        assert!(validate_certificate(repeated, "cluster").is_err());
    }

    #[test]
    fn bootstrap_voters_maximize_failure_domain_spread_before_identity_order() {
        let claim = |identity: &str, id, domain: &str| Claim {
            version: IDENTITY_VERSION,
            cluster_identity: "cluster".into(),
            node_identity: identity.into(),
            node_id: id,
            address: format!("n{id}:7101"),
            failure_domain: domain.into(),
            eligible: true,
        };
        let selected = select_claims(vec![
            claim("a", 1, "zone-a"),
            claim("b", 2, "zone-a"),
            claim("c", 3, "zone-b"),
            claim("d", 4, "zone-c"),
        ]);
        assert_eq!(
            selected[..3]
                .iter()
                .map(|claim| claim.node_id)
                .collect::<Vec<_>>(),
            vec![1, 3, 4]
        );
    }

    #[tokio::test]
    async fn three_concurrent_fresh_nodes_form_exactly_one_cluster() {
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
        let root = std::env::temp_dir().join(format!(
            "orbita-bootstrap-three-{}-{}",
            std::process::id(),
            random_identity().unwrap()
        ));
        let directories: Vec<_> = (1..=4).map(|id| root.join(id.to_string())).collect();
        let first = bootstrap(
            Arc::clone(&store),
            &directories[0],
            "test",
            NodeId(1),
            "n1:7101",
            "a",
            true,
        );
        let second = bootstrap(
            Arc::clone(&store),
            &directories[1],
            "test",
            NodeId(2),
            "n2:7101",
            "b",
            true,
        );
        let third = bootstrap(
            Arc::clone(&store),
            &directories[2],
            "test",
            NodeId(3),
            "n3:7101",
            "c",
            true,
        );
        let (first, second, third) = tokio::join!(first, second, third);
        let certificates = [first.unwrap(), second.unwrap(), third.unwrap()];
        assert!(
            certificates.windows(2).all(|pair| {
                pair[0].cluster_identity == pair[1].cluster_identity
                    && pair[0].voters == pair[1].voters
            }),
            "every concurrent node must read the one conditional certificate"
        );

        let fourth = bootstrap(
            Arc::clone(&store),
            &directories[3],
            "test",
            NodeId(4),
            "n4:7101",
            "d",
            true,
        )
        .await
        .unwrap();
        assert_eq!(fourth.cluster_identity, certificates[0].cluster_identity);
        assert_eq!(fourth.voters, certificates[0].voters);
        assert!(
            !fourth.voters.iter().any(|(node, _)| *node == NodeId(4)),
            "a later worker cannot rewrite the immutable three-voter certificate"
        );
        std::fs::remove_dir_all(root).ok();
    }
}
