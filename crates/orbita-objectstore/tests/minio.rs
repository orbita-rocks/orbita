//! Integration tests against a live S3-compatible server, MinIO in practice.
//!
//! Ignored by default because CI and most laptops have no bucket to talk to.
//! To run them, point the environment at one and ask for ignored tests:
//!
//! ```sh
//! docker run --rm -p 9000:9000 minio/minio server /data
//! export ORBITA_S3_TEST_ENDPOINT=http://127.0.0.1:9000
//! export ORBITA_S3_TEST_BUCKET=orbita-test        # must already exist
//! export ORBITA_S3_TEST_ACCESS_KEY=minioadmin
//! export ORBITA_S3_TEST_SECRET_KEY=minioadmin
//! cargo test -p orbita-objectstore --all-features -- --ignored
//! ```
//!
//! The conditional-write tests need a MinIO recent enough to implement
//! conditional PUT (early 2025 or later); against an older server they fail
//! with the store's explicit 501 diagnostic, which is itself the finding.

#![cfg(feature = "hyper-client")]

use bytes::Bytes;
use orbita_objectstore::s3::{Credentials, S3Config, S3Store};
use orbita_objectstore::{ObjectError, ObjectStore, Precondition};

fn store_from_env() -> S3Store {
    let var = |name: &str| {
        std::env::var(name)
            .unwrap_or_else(|_| panic!("{name} must be set to run the live S3 tests"))
    };
    S3Store::connect(S3Config {
        endpoint: var("ORBITA_S3_TEST_ENDPOINT"),
        bucket: var("ORBITA_S3_TEST_BUCKET"),
        region: std::env::var("ORBITA_S3_TEST_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
        credentials: Credentials {
            access_key_id: var("ORBITA_S3_TEST_ACCESS_KEY"),
            secret_access_key: var("ORBITA_S3_TEST_SECRET_KEY"),
            session_token: None,
        },
        force_path_style: true,
    })
    .expect("valid live-test configuration")
}

/// A key prefix unique to one test run, so reruns do not trip over leftovers.
fn unique_prefix(test: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after 1970")
        .as_nanos();
    format!("orbita-it/{test}/{nanos}")
}

#[tokio::test]
#[ignore = "needs a live S3-compatible endpoint; see the module docs"]
async fn objects_round_trip_through_a_live_store() {
    let store = store_from_env();
    let prefix = unique_prefix("round-trip");
    let key = format!("{prefix}/hello");

    let etag = store
        .put(&key, Bytes::from_static(b"0123456789"))
        .await
        .expect("put");
    let (bytes, read_tag) = store.get(&key).await.expect("get");
    assert_eq!(bytes, Bytes::from_static(b"0123456789"));
    assert_eq!(read_tag, etag);

    let sliced = store.get_range(&key, 2..5).await.expect("range");
    assert_eq!(sliced, Bytes::from_static(b"234"));

    let meta = store.head(&key).await.expect("head");
    assert_eq!(meta.size, 10);

    let listed = store.list(&format!("{prefix}/")).await.expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].key, key);

    store.delete(&key).await.expect("delete");
    assert!(matches!(
        store.get(&key).await,
        Err(ObjectError::NotFound(_))
    ));
    // Deleting what is already gone is a no-op, not an error.
    store.delete(&key).await.expect("idempotent delete");
}

#[tokio::test]
#[ignore = "needs a live S3-compatible endpoint; see the module docs"]
async fn conditional_writes_enforce_the_manifest_swap_rules() {
    let store = store_from_env();
    let key = format!("{}/manifest", unique_prefix("conditional"));

    let first = store
        .put_if(&key, Bytes::from_static(b"v1"), Precondition::NotExists)
        .await
        .expect("first create wins");

    assert_eq!(
        store
            .put_if(&key, Bytes::from_static(b"v1b"), Precondition::NotExists)
            .await,
        Err(ObjectError::PreconditionFailed(key.clone())),
        "a second creator must lose"
    );

    let second = store
        .put_if(
            &key,
            Bytes::from_static(b"v2"),
            Precondition::Match(first.clone()),
        )
        .await
        .expect("a swap holding the current tag wins");
    assert_ne!(second, first);

    assert_eq!(
        store
            .put_if(&key, Bytes::from_static(b"v3"), Precondition::Match(first))
            .await,
        Err(ObjectError::PreconditionFailed(key.clone())),
        "a deposed writer holding a stale tag must lose"
    );

    let (bytes, _) = store.get(&key).await.expect("get");
    assert_eq!(bytes, Bytes::from_static(b"v2"), "the winner's bytes stay");

    store.delete(&key).await.expect("cleanup");
}
