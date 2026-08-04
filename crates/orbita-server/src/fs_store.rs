//! A filesystem-backed object store for a node that has no bucket.
//!
//! Single-node Orbita has to run from a data directory alone, and the storage
//! engine persists exclusively through `orbita_objectstore::ObjectStore`. This
//! adapter is that trait over local files: object keys become paths under one
//! root, a write is a temp file renamed into place, and the conditional write
//! the manifest swap depends on is serialized by an in-process lock.
//!
//! # Where the compare-and-swap guarantee actually comes from
//!
//! The lock only excludes writers inside this process. That is the deployment
//! this store exists for: one node owns one data directory, the way it always
//! has. Two processes pointed at the same directory could race the manifest,
//! which a real bucket's conditional writes would refuse; a multi-node cluster
//! is expected to bring a real object store rather than a shared filesystem.
//!
//! Entity tags are derived from content (checksum plus length) rather than
//! stored, so they survive a restart without a sidecar file. A rewrite of
//! identical bytes therefore reuses its tag, which is harmless where tags are
//! used: the manifest never republishes identical bytes, because every commit
//! moves its horizon or its segment list.
//!
//! I/O here is synchronous inside async methods, like the engine this store
//! feeds replaced. Objects are written whole and read rarely, and the
//! latency-critical path is the WAL's, which runs through the runtime's disk
//! seam instead.

use async_trait::async_trait;
use bytes::Bytes;
use orbita_objectstore::{ETag, ObjectError, ObjectMeta, ObjectResult, ObjectStore, Precondition};
use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

pub(crate) struct FsStore {
    root: PathBuf,
    /// Serializes conditional writes, which is the whole CAS story here. See
    /// the module docs for why in-process exclusion is the deal.
    writes: Mutex<()>,
    /// Distinguishes temp files when two threads write the same key at once.
    next_temp: AtomicU64,
}

impl FsStore {
    pub(crate) fn new(root: PathBuf) -> Self {
        Self {
            root,
            writes: Mutex::new(()),
            next_temp: AtomicU64::new(0),
        }
    }

    /// The path for an object key, refusing anything that would escape the
    /// root. Keys come from the partition layout and never contain dot-dots,
    /// so a hit here is a bug worth failing loudly on rather than traversing.
    fn path_of(&self, key: &str) -> ObjectResult<PathBuf> {
        let relative = Path::new(key);
        let sane = relative
            .components()
            .all(|c| matches!(c, Component::Normal(_)));
        if key.is_empty() || !sane {
            return Err(ObjectError::Other(format!(
                "object key {key:?} does not stay under the store root"
            )));
        }
        Ok(self.root.join(relative))
    }

    fn read_all(&self, key: &str) -> ObjectResult<Bytes> {
        let path = self.path_of(key)?;
        match std::fs::read(&path) {
            Ok(bytes) => Ok(Bytes::from(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(ObjectError::NotFound(key.to_string()))
            }
            Err(e) => Err(ObjectError::Other(format!("reading {key}: {e}"))),
        }
    }

    fn current_etag(&self, key: &str) -> ObjectResult<Option<ETag>> {
        match self.read_all(key) {
            Ok(bytes) => Ok(Some(etag_of(&bytes))),
            Err(ObjectError::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn write(&self, key: &str, data: &Bytes) -> ObjectResult<ETag> {
        let path = self.path_of(key)?;
        let parent = path
            .parent()
            .ok_or_else(|| ObjectError::Other(format!("object key {key:?} has no parent")))?;
        std::fs::create_dir_all(parent)
            .map_err(|e| ObjectError::Other(format!("creating {}: {e}", parent.display())))?;

        // Temp-then-rename in the same directory, so a crash mid-write leaves
        // either the old object or the new one and never a torn file.
        let temp = parent.join(format!(
            ".tmp-{}-{}",
            std::process::id(),
            self.next_temp.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::write(&temp, data)
            .map_err(|e| ObjectError::Other(format!("writing {key}: {e}")))?;
        std::fs::rename(&temp, &path)
            .map_err(|e| ObjectError::Other(format!("publishing {key}: {e}")))?;
        Ok(etag_of(data))
    }

    fn walk(&self, dir: &Path, keys: &mut Vec<String>) -> ObjectResult<()> {
        let entries = match std::fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => {
                return Err(ObjectError::Other(format!(
                    "listing {}: {e}",
                    dir.display()
                )))
            }
        };
        for entry in entries {
            let entry =
                entry.map_err(|e| ObjectError::Other(format!("listing {}: {e}", dir.display())))?;
            let path = entry.path();
            let name = entry.file_name();
            if path.is_dir() {
                self.walk(&path, keys)?;
            } else if !name.to_string_lossy().starts_with(".tmp-") {
                let relative = path
                    .strip_prefix(&self.root)
                    .expect("walked paths sit under the root");
                // Keys always use forward slashes, whatever the platform's
                // separator is, because that is what was stored.
                let key = relative
                    .components()
                    .map(|c| c.as_os_str().to_string_lossy())
                    .collect::<Vec<_>>()
                    .join("/");
                keys.push(key);
            }
        }
        Ok(())
    }
}

/// A content-derived tag: checksum plus length. See the module docs for why
/// this is enough for the manifest's compare-and-swap.
fn etag_of(data: &[u8]) -> ETag {
    ETag(format!("{:08x}-{:016x}", crc32c::crc32c(data), data.len()))
}

#[async_trait]
impl ObjectStore for FsStore {
    async fn put(&self, key: &str, data: Bytes) -> ObjectResult<ETag> {
        let _guard = self.writes.lock().unwrap_or_else(|e| e.into_inner());
        self.write(key, &data)
    }

    async fn put_if(
        &self,
        key: &str,
        data: Bytes,
        precondition: Precondition,
    ) -> ObjectResult<ETag> {
        let _guard = self.writes.lock().unwrap_or_else(|e| e.into_inner());
        let current = self.current_etag(key)?;
        let holds = match (&precondition, &current) {
            (Precondition::NotExists, None) => true,
            (Precondition::Match(expected), Some(actual)) => expected == actual,
            _ => false,
        };
        if !holds {
            return Err(ObjectError::PreconditionFailed(key.to_string()));
        }
        self.write(key, &data)
    }

    async fn get(&self, key: &str) -> ObjectResult<(Bytes, ETag)> {
        let bytes = self.read_all(key)?;
        let etag = etag_of(&bytes);
        Ok((bytes, etag))
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> ObjectResult<Bytes> {
        let path = self.path_of(key)?;
        let mut file = match std::fs::File::open(&path) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(ObjectError::NotFound(key.to_string()))
            }
            Err(e) => return Err(ObjectError::Other(format!("opening {key}: {e}"))),
        };
        let size = file
            .metadata()
            .map_err(|e| ObjectError::Other(format!("{key}: {e}")))?
            .len();
        if range.start > range.end || range.end > size {
            return Err(ObjectError::Other(format!(
                "range {}..{} is not satisfiable for a {size} byte object",
                range.start, range.end
            )));
        }
        file.seek(SeekFrom::Start(range.start))
            .map_err(|e| ObjectError::Other(format!("seeking {key}: {e}")))?;
        let mut buffer = vec![0u8; (range.end - range.start) as usize];
        file.read_exact(&mut buffer)
            .map_err(|e| ObjectError::Other(format!("reading {key}: {e}")))?;
        Ok(Bytes::from(buffer))
    }

    async fn head(&self, key: &str) -> ObjectResult<ObjectMeta> {
        let bytes = self.read_all(key)?;
        Ok(ObjectMeta {
            key: key.to_string(),
            size: bytes.len() as u64,
            etag: etag_of(&bytes),
        })
    }

    async fn list(&self, prefix: &str) -> ObjectResult<Vec<ObjectMeta>> {
        let mut keys = Vec::new();
        let root = self.root.clone();
        self.walk(&root, &mut keys)?;
        keys.retain(|key| key.starts_with(prefix));
        keys.sort();
        keys.into_iter()
            .map(|key| {
                // The tag costs a read, and a listing's tags are only ever
                // compared by equality, so content-derived stays consistent
                // with every other path here.
                let bytes = self.read_all(&key)?;
                Ok(ObjectMeta {
                    key,
                    size: bytes.len() as u64,
                    etag: etag_of(&bytes),
                })
            })
            .collect()
    }

    async fn delete(&self, key: &str) -> ObjectResult<()> {
        let path = self.path_of(key)?;
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(ObjectError::Other(format!("deleting {key}: {e}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (FsStore, tempdir::Guard) {
        let dir = tempdir::unique("orbita-fs-store-test");
        (FsStore::new(dir.path.clone()), dir)
    }

    /// The smallest possible self-cleaning directory, so these tests leave
    /// nothing behind without pulling in a crate for it.
    mod tempdir {
        use std::path::PathBuf;
        use std::sync::atomic::{AtomicU64, Ordering};

        pub struct Guard {
            pub path: PathBuf,
        }

        impl Drop for Guard {
            fn drop(&mut self) {
                std::fs::remove_dir_all(&self.path).ok();
            }
        }

        pub fn unique(label: &str) -> Guard {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "{label}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&path).expect("a fresh temp directory");
            Guard { path }
        }
    }

    #[tokio::test]
    async fn objects_round_trip_and_survive_a_new_store_over_the_same_root() {
        let (store, dir) = store();
        store
            .put("a/b/object", Bytes::from_static(b"payload"))
            .await
            .unwrap();

        let (bytes, _) = store.get("a/b/object").await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"payload"));

        // A restart is a new store over the same directory.
        let reopened = FsStore::new(dir.path.clone());
        let (bytes, _) = reopened.get("a/b/object").await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"payload"));
    }

    #[tokio::test]
    async fn a_conditional_write_needs_the_current_tag() {
        let (store, _dir) = store();
        let first = store
            .put_if("k", Bytes::from_static(b"a"), Precondition::NotExists)
            .await
            .expect("nothing there yet");
        assert!(
            store
                .put_if("k", Bytes::from_static(b"b"), Precondition::NotExists)
                .await
                .is_err(),
            "the object exists now"
        );

        store
            .put_if(
                "k",
                Bytes::from_static(b"b"),
                Precondition::Match(first.clone()),
            )
            .await
            .expect("the tag is current");
        assert!(
            store
                .put_if("k", Bytes::from_static(b"c"), Precondition::Match(first))
                .await
                .is_err(),
            "a stale tag loses"
        );
    }

    #[tokio::test]
    async fn a_conditional_write_survives_a_restart_because_tags_are_content_derived() {
        let (store, dir) = store();
        let tag = store.put("k", Bytes::from_static(b"v1")).await.unwrap();

        let reopened = FsStore::new(dir.path.clone());
        reopened
            .put_if("k", Bytes::from_static(b"v2"), Precondition::Match(tag))
            .await
            .expect("a tag taken before the restart still matches");
    }

    #[tokio::test]
    async fn a_range_request_returns_exactly_the_range() {
        let (store, _dir) = store();
        store
            .put("k", Bytes::from_static(b"0123456789"))
            .await
            .unwrap();
        assert_eq!(
            store.get_range("k", 2..5).await.unwrap(),
            Bytes::from_static(b"234")
        );
        assert!(store.get_range("k", 8..20).await.is_err(), "past the end");
    }

    #[tokio::test]
    async fn a_listing_is_confined_to_its_prefix_and_sorted() {
        let (store, _dir) = store();
        for key in ["p/b", "p/a", "q/a"] {
            store.put(key, Bytes::from_static(b"x")).await.unwrap();
        }
        let listed: Vec<String> = store
            .list("p/")
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.key)
            .collect();
        assert_eq!(listed, vec!["p/a".to_string(), "p/b".to_string()]);
    }

    #[tokio::test]
    async fn a_key_that_would_escape_the_root_is_refused() {
        let (store, _dir) = store();
        for key in ["../outside", "a/../../outside", "/absolute", ""] {
            assert!(
                store.put(key, Bytes::new()).await.is_err(),
                "{key:?} must not become a path"
            );
        }
    }

    #[tokio::test]
    async fn deleting_a_missing_object_is_not_an_error() {
        let (store, _dir) = store();
        store.delete("never/existed").await.unwrap();
    }
}
