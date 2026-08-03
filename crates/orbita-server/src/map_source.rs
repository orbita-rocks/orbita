//! Where a worker gets the partition map.
//!
//! The map is decided by the leader group, and a worker only consumes it. This
//! trait is the seam between the two so that the control plane can be built at
//! the same time as the server: a single node or a test supplies
//! [`StaticMapSource`], and a real cluster will supply an adapter over the
//! control plane's client without anything in this crate changing.

use orbita_core::{
    Epoch, KeyRange, KeyspaceId, KeyspaceInfo, KeyspaceName, MapVersion, NodeId, PartitionId,
    PartitionInfo, PartitionMap, Result,
};

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

/// Something that can hand this worker the current partition map.
///
/// Fetching is fallible and asynchronous because the real implementation is an
/// RPC to the leader group, and a worker has to keep serving from its cached
/// map while that is failing.
pub trait MapSource: Send + Sync + 'static {
    fn fetch(&self) -> impl Future<Output = Result<PartitionMap>> + Send;
}

/// A map that never changes.
///
/// This is what a single-node development server and most tests use. It is
/// also the honest description of a cluster that has not been told about the
/// control plane yet: routing works, ownership simply never moves.
#[derive(Debug, Clone)]
pub struct StaticMapSource {
    map: Arc<Mutex<PartitionMap>>,
}

impl StaticMapSource {
    #[must_use]
    pub fn new(map: PartitionMap) -> Self {
        Self {
            map: Arc::new(Mutex::new(map)),
        }
    }

    /// Replaces the map, so a test can move ownership without standing up a
    /// leader group.
    pub fn set(&self, map: PartitionMap) {
        *self.map.lock().expect("static map poisoned") = map;
    }
}

impl MapSource for StaticMapSource {
    async fn fetch(&self) -> Result<PartitionMap> {
        Ok(self.map.lock().expect("static map poisoned").clone())
    }
}

/// A `MapSource` behind a pointer, so [`crate::ServerConfig`] can hold one
/// without making the whole server generic over it.
///
/// The trait itself returns `impl Future` and so is not dyn-compatible. This is
/// the usual adapter: box the future once per fetch, which happens on a timer
/// rather than on a request.
#[derive(Clone)]
pub struct BoxedMapSource(Arc<dyn ErasedMapSource>);

impl BoxedMapSource {
    #[must_use]
    pub fn new(source: impl MapSource) -> Self {
        Self(Arc::new(Erased(source)))
    }
}

impl std::fmt::Debug for BoxedMapSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BoxedMapSource")
    }
}

impl MapSource for BoxedMapSource {
    fn fetch(&self) -> impl Future<Output = Result<PartitionMap>> + Send {
        self.0.fetch_boxed()
    }
}

trait ErasedMapSource: Send + Sync + 'static {
    fn fetch_boxed(&self) -> Pin<Box<dyn Future<Output = Result<PartitionMap>> + Send + '_>>;
}

struct Erased<S>(S);

impl<S: MapSource> ErasedMapSource for Erased<S> {
    fn fetch_boxed(&self) -> Pin<Box<dyn Future<Output = Result<PartitionMap>> + Send + '_>> {
        Box::pin(self.0.fetch())
    }
}

/// The map a fresh single-node cluster starts with: one keyspace, one
/// unbounded partition, owned by this node with no replicas.
///
/// This exists so that starting a server takes a data directory and nothing
/// else. Everything a larger cluster needs is the same shape with more rows in
/// it.
#[must_use]
pub fn single_node_map(node: NodeId, keyspaces: &[KeyspaceName]) -> PartitionMap {
    let mut map = PartitionMap::new(MapVersion(1));
    for (index, name) in keyspaces.iter().enumerate() {
        let id = KeyspaceId(index as u64 + 1);
        map.insert_keyspace(KeyspaceInfo {
            id,
            name: name.clone(),
            default_ttl_millis: None,
            max_value_bytes: None,
            max_storage_bytes: None,
            max_reads_per_second: None,
            max_writes_per_second: None,
        });
        map.insert_partition(PartitionInfo {
            id: PartitionId(index as u64 + 1),
            keyspace: id,
            range: KeyRange::unbounded(),
            owner: Some(node),
            epoch: Epoch(1),
            replicas: Vec::new(),
        });
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(s: &str) -> KeyspaceName {
        KeyspaceName::new(s).expect("valid name")
    }

    #[test]
    fn a_single_node_map_covers_every_key_of_every_keyspace() {
        let map = single_node_map(NodeId(1), &[name("default"), name("locks")]);
        assert_eq!(map.check_coverage(), Ok(()));
        let ks = map.keyspace_by_name("locks").expect("keyspace is present");
        assert_eq!(
            map.lookup(ks.id, b"anything").unwrap().owner,
            Some(NodeId(1))
        );
    }

    #[tokio::test]
    async fn a_boxed_source_forwards_to_the_one_it_wraps() {
        let inner = StaticMapSource::new(single_node_map(NodeId(4), &[name("default")]));
        let boxed = BoxedMapSource::new(inner.clone());
        assert_eq!(boxed.fetch().await.unwrap().len(), 1);

        inner.set(PartitionMap::new(MapVersion(9)));
        assert_eq!(boxed.fetch().await.unwrap().version(), MapVersion(9));
    }
}
