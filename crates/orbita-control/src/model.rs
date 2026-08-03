//! The things the leader group stores about a cluster, other than who owns
//! what.
//!
//! Keyspace configuration and credentials live here because they are values
//! that travel through the replicated log and out to the admin API unchanged.
//! Ownership lives in `orbita_core::PartitionMap`, which is shared vocabulary
//! rather than control plane state, and is deliberately not duplicated.

use crate::codec::{CodecError, CodecResult, Reader, Writer};

use orbita_core::{KeyspaceId, KeyspaceInfo, KeyspaceName};

/// Per-keyspace defaults and quotas.
///
/// Every field is optional and unset means unlimited, so a keyspace created
/// without an opinion behaves like a single-tenant store and gains isolation
/// only when someone asks for it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KeyspaceConfig {
    pub default_ttl_millis: Option<u64>,
    pub max_value_bytes: Option<u64>,
    pub max_storage_bytes: Option<u64>,
    pub max_reads_per_second: Option<u32>,
    pub max_writes_per_second: Option<u32>,
}

impl KeyspaceConfig {
    pub(crate) fn encode(&self, w: &mut Writer) {
        w.opt_u64(self.default_ttl_millis)
            .opt_u64(self.max_value_bytes)
            .opt_u64(self.max_storage_bytes)
            .opt_u32(self.max_reads_per_second)
            .opt_u32(self.max_writes_per_second);
    }

    pub(crate) fn decode(r: &mut Reader<'_>) -> CodecResult<Self> {
        Ok(Self {
            default_ttl_millis: r.opt_u64()?,
            max_value_bytes: r.opt_u64()?,
            max_storage_bytes: r.opt_u64()?,
            max_reads_per_second: r.opt_u32()?,
            max_writes_per_second: r.opt_u32()?,
        })
    }
}

/// A keyspace as the leader group holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Keyspace {
    pub id: KeyspaceId,
    pub name: KeyspaceName,
    pub config: KeyspaceConfig,
    pub created_at_millis: u64,
}

impl Keyspace {
    /// The routing view of this keyspace, which is what workers cache.
    ///
    /// Quotas are in the map rather than fetched separately because the worker
    /// sees the traffic and has to enforce the rate limits itself; see the
    /// crate documentation on where enforcement happens.
    #[must_use]
    pub fn info(&self) -> KeyspaceInfo {
        KeyspaceInfo {
            id: self.id,
            name: self.name.clone(),
            default_ttl_millis: self.config.default_ttl_millis,
            max_value_bytes: self.config.max_value_bytes,
            max_storage_bytes: self.config.max_storage_bytes,
            max_reads_per_second: self.config.max_reads_per_second,
            max_writes_per_second: self.config.max_writes_per_second,
        }
    }
}

/// What a credential is allowed to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Permission {
    Read,
    Write,
}

impl Permission {
    pub(crate) fn tag(self) -> u8 {
        match self {
            Permission::Read => 1,
            Permission::Write => 2,
        }
    }

    pub(crate) fn from_tag(tag: u8) -> CodecResult<Self> {
        match tag {
            1 => Ok(Permission::Read),
            2 => Ok(Permission::Write),
            other => Err(CodecError::UnknownTag {
                what: "permission",
                tag: u64::from(other),
            }),
        }
    }
}

/// A credential as stored, which is to say without its secret.
///
/// The secret is hashed before it reaches the replicated log, so a leaked log
/// or a leaked snapshot does not hand out working credentials. Plain SHA-256
/// is enough here and a password hash would not be: the secret is 256 bits of
/// operating system entropy, so there is no dictionary to search.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credential {
    pub id: String,
    pub secret_hash: [u8; 32],
    /// Keyspace names rather than ids, so a credential survives a keyspace
    /// being deleted and recreated only if the operator meant it to.
    pub keyspaces: Vec<String>,
    pub permissions: Vec<Permission>,
    pub description: String,
    pub created_at_millis: u64,
    pub expires_at_millis: Option<u64>,
}

impl Credential {
    /// Whether this credential may perform `permission` on `keyspace` at
    /// `now_millis`.
    #[must_use]
    pub fn allows(&self, keyspace: &str, permission: Permission, now_millis: u64) -> bool {
        if self.expires_at_millis.is_some_and(|at| now_millis >= at) {
            return false;
        }
        self.keyspaces.iter().any(|k| k == keyspace) && self.permissions.contains(&permission)
    }

    pub(crate) fn encode(&self, w: &mut Writer) {
        w.str(&self.id)
            .bytes(&self.secret_hash)
            .seq(&self.keyspaces, |w, k| {
                w.str(k);
            })
            .seq(&self.permissions, |w, p| {
                w.u8(p.tag());
            })
            .str(&self.description)
            .u64(self.created_at_millis)
            .opt_u64(self.expires_at_millis);
    }

    pub(crate) fn decode(r: &mut Reader<'_>) -> CodecResult<Self> {
        let id = r.string()?;
        let hash = r.bytes()?;
        let secret_hash: [u8; 32] = hash
            .as_ref()
            .try_into()
            .map_err(|_| CodecError::OutOfRange("credential secret hash"))?;
        Ok(Self {
            id,
            secret_hash,
            keyspaces: r.seq(Reader::string)?,
            permissions: r.seq(|r| Permission::from_tag(r.u8()?))?,
            description: r.string()?,
            created_at_millis: r.u64()?,
            expires_at_millis: r.opt_u64()?,
        })
    }
}

/// Hashes a secret the way the state machine stores it.
#[must_use]
pub fn hash_secret(secret: &str) -> [u8; 32] {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(secret.as_bytes());
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn credential() -> Credential {
        Credential {
            id: "cred-1".into(),
            secret_hash: hash_secret("hunter2"),
            keyspaces: vec!["catalog".into()],
            permissions: vec![Permission::Read, Permission::Write],
            description: "the catalog service".into(),
            created_at_millis: 1_000,
            expires_at_millis: Some(2_000),
        }
    }

    #[test]
    fn a_credential_round_trips_without_its_secret() {
        let mut w = Writer::new();
        credential().encode(&mut w);
        let encoded = w.finish();
        assert!(
            !encoded.windows(7).any(|win| win == b"hunter2"),
            "the plaintext secret must never reach the log"
        );

        let mut r = Reader::new(&encoded);
        assert_eq!(Credential::decode(&mut r).unwrap(), credential());
    }

    #[test]
    fn a_credential_scoped_to_one_keyspace_cannot_reach_another() {
        let c = credential();
        assert!(c.allows("catalog", Permission::Read, 0));
        assert!(!c.allows("other", Permission::Read, 0));
    }

    #[test]
    fn an_expired_credential_allows_nothing() {
        let c = credential();
        assert!(c.allows("catalog", Permission::Write, 1_999));
        assert!(
            !c.allows("catalog", Permission::Write, 2_000),
            "expiry is inclusive of the deadline"
        );
    }

    #[test]
    fn a_keyspace_config_round_trips_with_its_gaps_intact() {
        let config = KeyspaceConfig {
            default_ttl_millis: Some(60_000),
            max_value_bytes: None,
            max_storage_bytes: Some(1 << 30),
            max_reads_per_second: None,
            max_writes_per_second: Some(500),
        };
        let mut w = Writer::new();
        config.encode(&mut w);
        let encoded = w.finish();
        let mut r = Reader::new(&encoded);
        assert_eq!(KeyspaceConfig::decode(&mut r).unwrap(), config);
    }
}
