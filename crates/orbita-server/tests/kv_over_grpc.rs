//! A single node, over a real socket, driven by the generated client.
//!
//! The product's promise is that a client using generated stubs and no
//! hand-written library can do everything in the API, so that is what this
//! test is: `tonic`'s own `KvClient` against a `Server` on a real port. A test
//! that called the service type directly would pass while the wire was broken.

use orbita_core::{KeyspaceName, MAX_KEY_BYTES, MAX_VALUE_BYTES};
use orbita_proto::v1::condition::Kind;
use orbita_proto::v1::kv_client::KvClient;
use orbita_proto::v1::{
    Condition, DeleteRequest, GetRequest, ListRequest, SetRequest, SetResponse,
};
use orbita_server::{Server, ServerConfig, DEFAULT_KEYSPACE};

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use tonic::transport::Channel;
use tonic::Code;

/// A data directory nothing else is using, removed when the test ends.
struct DataDir(PathBuf);

impl DataDir {
    fn new(name: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "orbita-server-test-{}-{name}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).expect("a temp directory");
        Self(path)
    }
}

impl Drop for DataDir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).ok();
    }
}

async fn start(dir: &DataDir) -> (Server, KvClient<Channel>) {
    let config = ServerConfig::single_node(&dir.0).on_ephemeral_port();
    let server = Server::start(config).await.expect("the server starts");
    let client = KvClient::connect(format!("http://{}", server.local_addr()))
        .await
        .expect("a generated client connects");
    (server, client)
}

fn set(key: &str, value: &str) -> SetRequest {
    SetRequest {
        keyspace: DEFAULT_KEYSPACE.to_string(),
        key: key.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
        ttl_millis: None,
        condition: None,
    }
}

fn get(key: &str) -> GetRequest {
    GetRequest {
        keyspace: DEFAULT_KEYSPACE.to_string(),
        key: key.as_bytes().to_vec(),
    }
}

fn list(prefix: &str, limit: u32, cursor: Vec<u8>) -> ListRequest {
    ListRequest {
        keyspace: DEFAULT_KEYSPACE.to_string(),
        prefix: prefix.as_bytes().to_vec(),
        cursor,
        limit,
        include_values: true,
    }
}

#[tokio::test]
async fn a_generated_client_can_do_everything_in_the_api() {
    let dir = DataDir::new("full-api");
    let (server, mut client) = start(&dir).await;

    let written = client
        .set(set("greeting", "hello"))
        .await
        .unwrap()
        .into_inner();
    assert!(written.applied);
    assert!(
        written.version > 0,
        "a write returns the version it produced"
    );

    let read = client.get(get("greeting")).await.unwrap().into_inner();
    assert!(read.found);
    assert_eq!(read.value, b"hello");
    assert_eq!(
        read.version, written.version,
        "the version a read reports is the one the write returned"
    );

    let missing = client.get(get("absent")).await.unwrap().into_inner();
    assert!(!missing.found);

    let listed = client
        .list(list("", 10, Vec::new()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.entries.len(), 1);
    assert_eq!(listed.entries[0].key, b"greeting");
    assert_eq!(listed.entries[0].value, b"hello");

    let deleted = client
        .delete(DeleteRequest {
            keyspace: DEFAULT_KEYSPACE.to_string(),
            key: b"greeting".to_vec(),
            condition: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(deleted.applied);
    assert!(deleted.existed);

    assert!(
        !client
            .get(get("greeting"))
            .await
            .unwrap()
            .into_inner()
            .found
    );
    assert!(client
        .list(list("", 10, Vec::new()))
        .await
        .unwrap()
        .into_inner()
        .entries
        .is_empty());

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_write_is_visible_to_the_very_next_read() {
    // The owner acknowledges a write before applying it to storage, so this is
    // the test that the overlay covering that window actually covers it.
    let dir = DataDir::new("read-your-write");
    let (server, mut client) = start(&dir).await;

    for i in 0..50 {
        let value = format!("v{i}");
        let written = client.set(set("k", &value)).await.unwrap().into_inner();
        let read = client.get(get("k")).await.unwrap().into_inner();
        assert_eq!(read.value, value.as_bytes(), "read after write {i}");
        assert_eq!(read.version, written.version);
    }

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn only_one_compare_and_swap_against_a_version_wins() {
    let dir = DataDir::new("cas");
    let (server, mut client) = start(&dir).await;

    let first = client.set(set("lock", "a")).await.unwrap().into_inner();

    let swap = |version: u64, value: &str| SetRequest {
        condition: Some(Condition {
            kind: Some(Kind::IfVersion(version)),
        }),
        ..set("lock", value)
    };

    let winner = client
        .set(swap(first.version, "b"))
        .await
        .unwrap()
        .into_inner();
    assert!(winner.applied);
    assert!(
        winner.version > first.version,
        "versions never go backwards"
    );

    let loser = client
        .set(swap(first.version, "c"))
        .await
        .unwrap()
        .into_inner();
    assert!(
        !loser.applied,
        "the second swap against one version must lose"
    );
    assert_eq!(
        loser.current_version,
        Some(winner.version),
        "a loser is told what it found so it can retry"
    );
    assert_eq!(
        client.get(get("lock")).await.unwrap().into_inner().value,
        b"b",
        "the loser must not have written anything"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn concurrent_swaps_against_one_version_produce_exactly_one_winner() {
    // This is the wedge use case: several processes racing to take a lock.
    let dir = DataDir::new("cas-race");
    let (server, mut client) = start(&dir).await;
    let addr = server.local_addr();

    let initial = client.set(set("lock", "free")).await.unwrap().into_inner();

    let mut racers = Vec::new();
    for contender in 0..8u64 {
        racers.push(tokio::spawn(async move {
            let mut client = KvClient::connect(format!("http://{addr}")).await.unwrap();
            client
                .set(SetRequest {
                    condition: Some(Condition {
                        kind: Some(Kind::IfVersion(initial.version)),
                    }),
                    ..set("lock", &format!("held-by-{contender}"))
                })
                .await
                .unwrap()
                .into_inner()
        }));
    }

    let mut winners = Vec::new();
    for racer in racers {
        let response: SetResponse = racer.await.unwrap();
        if response.applied {
            winners.push(response);
        }
    }
    assert_eq!(winners.len(), 1, "a lock with two holders is not a lock");

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn if_not_present_creates_a_key_exactly_once() {
    let dir = DataDir::new("if-not-present");
    let (server, mut client) = start(&dir).await;

    let create = || SetRequest {
        condition: Some(Condition {
            kind: Some(Kind::IfNotPresent(true)),
        }),
        ..set("once", "mine")
    };

    assert!(client.set(create()).await.unwrap().into_inner().applied);
    let second = client.set(create()).await.unwrap().into_inner();
    assert!(!second.applied);
    assert!(
        second.current_version.is_some(),
        "the caller is told what is there"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn an_expired_key_is_invisible_to_get_and_list() {
    let dir = DataDir::new("ttl");
    let (server, mut client) = start(&dir).await;

    client
        .set(SetRequest {
            ttl_millis: Some(400),
            ..set("session/a", "soon gone")
        })
        .await
        .unwrap();
    client.set(set("session/b", "stays")).await.unwrap();

    let alive = client.get(get("session/a")).await.unwrap().into_inner();
    assert!(alive.found);
    assert!(
        alive.expires_at_millis.is_some(),
        "a TTL becomes an absolute deadline"
    );

    tokio::time::sleep(std::time::Duration::from_millis(600)).await;

    assert!(
        !client
            .get(get("session/a"))
            .await
            .unwrap()
            .into_inner()
            .found,
        "an expired key is gone whether or not anything has reclaimed it"
    );
    let listed = client
        .list(list("session/", 10, Vec::new()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.entries.len(), 1);
    assert_eq!(listed.entries[0].key, b"session/b");

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_paged_scan_visits_every_key_exactly_once() {
    let dir = DataDir::new("pagination");
    let (server, mut client) = start(&dir).await;

    let total = 25;
    for i in 0..total {
        client.set(set(&format!("item/{i:03}"), "v")).await.unwrap();
    }
    // A key outside the prefix, to prove the prefix bounds the scan.
    client.set(set("other", "v")).await.unwrap();

    let mut seen = Vec::new();
    let mut cursor = Vec::new();
    let mut pages = 0;
    loop {
        let page = client
            .list(list("item/", 7, cursor.clone()))
            .await
            .unwrap()
            .into_inner();
        pages += 1;
        assert!(pages < 20, "pagination is not terminating");
        seen.extend(page.entries.iter().map(|e| e.key.clone()));
        cursor = page.next_cursor;
        if cursor.is_empty() {
            break;
        }
    }

    let mut expected: Vec<Vec<u8>> = (0..total)
        .map(|i| format!("item/{i:03}").into_bytes())
        .collect();
    expected.sort();
    assert_eq!(seen, expected, "no duplicates, no gaps, and in key order");
    assert!(pages > 1, "the page size must actually have paged");

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_list_page_is_capped_at_the_limit_it_was_asked_for() {
    let dir = DataDir::new("list-limit");
    let (server, mut client) = start(&dir).await;

    for i in 0..5 {
        client.set(set(&format!("k{i}"), "v")).await.unwrap();
    }
    let page = client
        .list(list("", 2, Vec::new()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(page.entries.len(), 2);
    assert!(
        !page.next_cursor.is_empty(),
        "a short page must say there is more"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_list_can_ask_for_keys_without_their_values() {
    let dir = DataDir::new("list-keys-only");
    let (server, mut client) = start(&dir).await;

    client
        .set(set("k", "a value nobody asked for"))
        .await
        .unwrap();
    let page = client
        .list(ListRequest {
            include_values: false,
            ..list("", 10, Vec::new())
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(page.entries[0].key, b"k");
    assert!(page.entries[0].value.is_empty());

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn an_oversized_key_or_value_is_rejected_as_invalid_argument() {
    let dir = DataDir::new("limits");
    let (server, mut client) = start(&dir).await;

    let big_key = client
        .set(SetRequest {
            key: vec![b'k'; MAX_KEY_BYTES + 1],
            ..set("ignored", "v")
        })
        .await
        .expect_err("an oversized key is refused");
    assert_eq!(big_key.code(), Code::InvalidArgument);

    let big_value = client
        .set(SetRequest {
            value: vec![b'v'; MAX_VALUE_BYTES + 1],
            ..set("k", "ignored")
        })
        .await
        .expect_err("an oversized value is refused");
    assert_eq!(big_value.code(), Code::InvalidArgument);

    // The limits themselves are allowed, or the documented maximum would be a
    // byte off.
    assert!(client
        .set(SetRequest {
            key: vec![b'k'; MAX_KEY_BYTES],
            value: vec![b'v'; MAX_VALUE_BYTES],
            ..set("ignored", "ignored")
        })
        .await
        .is_ok());

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn an_unknown_keyspace_is_refused_rather_than_created() {
    let dir = DataDir::new("keyspaces");
    let (server, mut client) = start(&dir).await;

    let unknown = client
        .get(GetRequest {
            keyspace: "nobody-made-this".to_string(),
            key: b"k".to_vec(),
        })
        .await
        .expect_err("an unknown keyspace has no data to return");
    assert_eq!(unknown.code(), Code::NotFound);

    let malformed = client
        .get(GetRequest {
            keyspace: "../escape".to_string(),
            key: b"k".to_vec(),
        })
        .await
        .expect_err("a name that could escape a path is refused");
    assert_eq!(malformed.code(), Code::InvalidArgument);

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn keyspaces_do_not_see_each_others_keys() {
    let dir = DataDir::new("multi-keyspace");
    let names = [
        KeyspaceName::new("catalog").unwrap(),
        KeyspaceName::new("locks").unwrap(),
    ];
    let config = ServerConfig::single_node(&dir.0)
        .on_ephemeral_port()
        .with_keyspaces(&names);
    let server = Server::start(config).await.unwrap();
    let mut client = KvClient::connect(format!("http://{}", server.local_addr()))
        .await
        .unwrap();

    client
        .set(SetRequest {
            keyspace: "catalog".to_string(),
            ..set("shared-name", "catalog value")
        })
        .await
        .unwrap();

    let other = client
        .get(GetRequest {
            keyspace: "locks".to_string(),
            key: b"shared-name".to_vec(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!other.found, "a keyspace is an isolated namespace");

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_conditional_delete_that_loses_leaves_the_key_alone() {
    let dir = DataDir::new("conditional-delete");
    let (server, mut client) = start(&dir).await;

    let written = client.set(set("k", "v")).await.unwrap().into_inner();
    let refused = client
        .delete(DeleteRequest {
            keyspace: DEFAULT_KEYSPACE.to_string(),
            key: b"k".to_vec(),
            condition: Some(Condition {
                kind: Some(Kind::IfVersion(written.version + 1)),
            }),
        })
        .await
        .unwrap()
        .into_inner();

    assert!(!refused.applied);
    assert_eq!(refused.current_version, Some(written.version));
    assert!(client.get(get("k")).await.unwrap().into_inner().found);

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn deleting_a_key_that_is_not_there_is_not_an_error() {
    let dir = DataDir::new("delete-absent");
    let (server, mut client) = start(&dir).await;

    let response = client
        .delete(DeleteRequest {
            keyspace: DEFAULT_KEYSPACE.to_string(),
            key: b"never-existed".to_vec(),
            condition: None,
        })
        .await
        .unwrap()
        .into_inner();

    assert!(response.applied, "a retried delete must not report failure");
    assert!(!response.existed);

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn acknowledged_writes_survive_a_restart() {
    // The acknowledgement comes before the apply, so recovery replaying the
    // log is the only thing standing between that ordering and data loss.
    let dir = DataDir::new("restart");

    let (server, mut client) = start(&dir).await;
    let written = client
        .set(set("durable", "yes"))
        .await
        .unwrap()
        .into_inner();
    server.shutdown().await.unwrap();

    let (server, mut client) = start(&dir).await;
    let read = client.get(get("durable")).await.unwrap().into_inner();
    assert!(read.found);
    assert_eq!(read.value, b"yes");
    assert_eq!(read.version, written.version, "a version is not reissued");

    // And the next write continues the sequence rather than restarting it.
    let next = client
        .set(set("durable", "still"))
        .await
        .unwrap()
        .into_inner();
    assert!(next.version > written.version);

    server.shutdown().await.unwrap();
}
