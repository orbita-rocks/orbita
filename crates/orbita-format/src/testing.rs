//! An object store that lives in a `BTreeMap`.
//!
//! The format's correctness rests on compare-and-swap, and a test that cannot
//! make two writers race cannot show that the rules hold. This store makes the
//! race cheap: the whole commit protocol runs against it in microseconds, and
//! the entity tags behave the way a real store's do.
//!
//! It is behind a feature flag because nothing in production should link it.

use async_trait::async_trait;
use bytes::Bytes;
use orbita_objectstore::{ETag, ObjectError, ObjectMeta, ObjectResult, ObjectStore, Precondition};
use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Mutex;

#[derive(Debug, Default)]
pub struct MemoryStore {
    state: Mutex<State>,
}

#[derive(Debug, Default)]
struct State {
    objects: BTreeMap<String, (Bytes, ETag)>,
    /// Entity tags are never reused, including for an object that was deleted
    /// and written again, so a compare-and-swap cannot succeed against a
    /// version that no longer means what the holder thinks.
    next_tag: u64,
    /// Every range served, so a test can assert about what a reader did not
    /// read. The format's claims about cost are claims about requests, and a
    /// test that cannot see the requests cannot check them.
    ranges: Vec<(String, Range<u64>)>,
}

impl State {
    fn tag(&mut self) -> ETag {
        self.next_tag += 1;
        ETag(format!("etag-{}", self.next_tag))
    }
}

impl MemoryStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Every key currently held, for a test that wants to assert about orphans.
    #[must_use]
    pub fn keys(&self) -> Vec<String> {
        self.lock().objects.keys().cloned().collect()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().objects.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lock().objects.is_empty()
    }

    /// Every range this store has served, in order.
    #[must_use]
    pub fn ranges_served(&self) -> Vec<(String, Range<u64>)> {
        self.lock().ranges.clone()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[async_trait]
impl ObjectStore for MemoryStore {
    async fn put(&self, key: &str, data: Bytes) -> ObjectResult<ETag> {
        let mut state = self.lock();
        let tag = state.tag();
        state.objects.insert(key.to_string(), (data, tag.clone()));
        Ok(tag)
    }

    async fn put_if(
        &self,
        key: &str,
        data: Bytes,
        precondition: Precondition,
    ) -> ObjectResult<ETag> {
        let mut state = self.lock();
        let current = state.objects.get(key).map(|(_, tag)| tag.clone());
        let holds = match (&precondition, &current) {
            (Precondition::NotExists, None) => true,
            (Precondition::Match(expected), Some(actual)) => expected == actual,
            _ => false,
        };
        if !holds {
            return Err(ObjectError::PreconditionFailed(key.to_string()));
        }
        let tag = state.tag();
        state.objects.insert(key.to_string(), (data, tag.clone()));
        Ok(tag)
    }

    async fn get(&self, key: &str) -> ObjectResult<(Bytes, ETag)> {
        self.lock()
            .objects
            .get(key)
            .cloned()
            .ok_or_else(|| ObjectError::NotFound(key.to_string()))
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> ObjectResult<Bytes> {
        let mut state = self.lock();
        state.ranges.push((key.to_string(), range.clone()));
        let (data, _) = state
            .objects
            .get(key)
            .ok_or_else(|| ObjectError::NotFound(key.to_string()))?;
        let size = data.len() as u64;
        if range.start >= size || range.end > size || range.start > range.end {
            return Err(ObjectError::Other(format!(
                "range {}..{} is not satisfiable for a {size} byte object",
                range.start, range.end
            )));
        }
        Ok(data.slice(range.start as usize..range.end as usize))
    }

    async fn head(&self, key: &str) -> ObjectResult<ObjectMeta> {
        let state = self.lock();
        let (data, tag) = state
            .objects
            .get(key)
            .ok_or_else(|| ObjectError::NotFound(key.to_string()))?;
        Ok(ObjectMeta {
            key: key.to_string(),
            size: data.len() as u64,
            etag: tag.clone(),
        })
    }

    async fn list(&self, prefix: &str) -> ObjectResult<Vec<ObjectMeta>> {
        let state = self.lock();
        Ok(state
            .objects
            .range(prefix.to_string()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, (data, tag))| ObjectMeta {
                key: key.clone(),
                size: data.len() as u64,
                etag: tag.clone(),
            })
            .collect())
    }

    async fn delete(&self, key: &str) -> ObjectResult<()> {
        self.lock().objects.remove(key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_conditional_write_needs_the_current_tag() {
        let store = MemoryStore::new();
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

        let second = store
            .put_if(
                "k",
                Bytes::from_static(b"b"),
                Precondition::Match(first.clone()),
            )
            .await
            .expect("the tag is current");
        assert_ne!(second, first, "a write always moves the tag");
        assert!(
            store
                .put_if("k", Bytes::from_static(b"c"), Precondition::Match(first))
                .await
                .is_err(),
            "a stale tag loses"
        );
    }

    #[tokio::test]
    async fn a_range_request_returns_exactly_the_range() {
        let store = MemoryStore::new();
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
        let store = MemoryStore::new();
        for key in ["p/b", "p/a", "q/a"] {
            store.put(key, Bytes::new()).await.unwrap();
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
}
