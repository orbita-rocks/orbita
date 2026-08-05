//! The simulated bucket, checked against the store that will drive it.
//!
//! These are the tests of the tester. A fault-injecting transport that quietly
//! disagrees with S3 about a status code or a continuation token would make
//! every scenario built on it prove something about a bucket nobody has, so
//! the contract is asserted here rather than assumed by the scenarios.

use orbita_core::NodeId;
use orbita_objectstore::{ObjectError, Precondition};
use orbita_runtime::Runtime;
use orbita_sim::{SimConfig, Simulation, StoreFault, StoreFaults};

use bytes::Bytes;

fn quiet() -> Simulation {
    Simulation::new(1)
}

#[test]
fn an_object_written_through_the_store_reads_back_byte_for_byte() {
    let sim = quiet();
    let bucket = sim.bucket("orbita");
    let store = bucket.store();

    let read = sim.block_on(async move {
        store
            .put("k/one", Bytes::from_static(b"hello"))
            .await
            .expect("written");
        store.get("k/one").await.expect("read")
    });
    assert_eq!(read.0, Bytes::from_static(b"hello"));
    assert_eq!(
        bucket.object("k/one"),
        Some(Bytes::from_static(b"hello")),
        "a peek at the bucket must agree with what the store read"
    );
}

#[test]
fn a_conditional_write_needs_the_tag_the_bucket_currently_holds() {
    let sim = quiet();
    let bucket = sim.bucket("orbita");
    let store = bucket.store();

    let (again, swapped, stale) = sim.block_on(async move {
        let first = store
            .put_if("m", Bytes::from_static(b"a"), Precondition::NotExists)
            .await
            .expect("nothing there yet");
        let again = store
            .put_if("m", Bytes::from_static(b"b"), Precondition::NotExists)
            .await;
        let swapped = store
            .put_if(
                "m",
                Bytes::from_static(b"b"),
                Precondition::Match(first.clone()),
            )
            .await
            .expect("the tag is current");
        let stale = store
            .put_if("m", Bytes::from_static(b"c"), Precondition::Match(first))
            .await;
        (again, swapped, stale)
    });

    assert_eq!(
        again,
        Err(ObjectError::PreconditionFailed("m".to_string())),
        "the object exists, so if-none-match must lose"
    );
    assert!(
        swapped.0.starts_with('"'),
        "an entity tag is a quoted string on the wire: {swapped:?}"
    );
    assert_eq!(
        stale,
        Err(ObjectError::PreconditionFailed("m".to_string())),
        "a tag the write already moved past must lose"
    );
}

#[test]
fn a_listing_walks_every_page_of_its_continuation() {
    let sim = quiet();
    let bucket = sim.bucket("orbita");
    let store = bucket.store();

    // Comfortably more than one page, so the store's pagination loop is the
    // thing under test rather than an unused branch.
    let keys = sim.block_on(async move {
        for index in 0..11u32 {
            store
                .put(&format!("p/{index:04}"), Bytes::from_static(b"x"))
                .await
                .expect("written");
        }
        store.put("q/other", Bytes::new()).await.expect("written");
        store
            .list("p/")
            .await
            .expect("listed")
            .into_iter()
            .map(|meta| meta.key)
            .collect::<Vec<_>>()
    });

    assert_eq!(keys.len(), 11, "a page boundary dropped objects: {keys:?}");
    assert_eq!(keys[0], "p/0000");
    assert_eq!(keys[10], "p/0010");
}

#[test]
fn a_range_read_returns_only_the_bytes_it_asked_for() {
    let sim = quiet();
    let bucket = sim.bucket("orbita");
    let store = bucket.store();

    let (slice, past_end) = sim.block_on(async move {
        store
            .put("k", Bytes::from_static(b"0123456789"))
            .await
            .expect("written");
        (
            store.get_range("k", 2..5).await,
            store.get_range("k", 20..25).await,
        )
    });
    assert_eq!(slice, Ok(Bytes::from_static(b"234")));
    assert!(past_end.is_err(), "a range past the end is not satisfiable");
}

#[test]
fn a_missing_object_is_reported_as_not_found_rather_than_as_a_failure() {
    let sim = quiet();
    let bucket = sim.bucket("orbita");
    let store = bucket.store();
    let outcome = sim.block_on(async move { store.get("nothing/here").await });
    assert_eq!(outcome, Err(ObjectError::NotFound("nothing/here".into())));
}

#[test]
fn a_lost_request_leaves_the_bucket_untouched() {
    let sim = quiet();
    let bucket = sim.bucket("orbita");
    let store = bucket.store();
    bucket.inject_once("PUT", "k", StoreFault::RequestLost);

    let outcome = sim.block_on(async move { store.put("k", Bytes::from_static(b"v")).await });
    assert!(
        outcome.as_ref().is_err_and(ObjectError::is_retryable),
        "a lost request is retryable: {outcome:?}"
    );
    assert!(
        bucket.object("k").is_none(),
        "a request that never arrived cannot have applied"
    );
}

#[test]
fn a_lost_response_leaves_the_bucket_written_and_the_caller_none_the_wiser() {
    // The fault the whole module exists for. The caller cannot tell this from
    // the case above, and the two differ by whether the data is durable.
    let sim = quiet();
    let bucket = sim.bucket("orbita");
    let store = bucket.store();
    bucket.inject_once("PUT", "k", StoreFault::ResponseLost);

    let outcome = sim.block_on(async move { store.put("k", Bytes::from_static(b"v")).await });
    assert!(
        outcome.as_ref().is_err_and(ObjectError::is_retryable),
        "the caller sees a retryable failure: {outcome:?}"
    );
    assert_eq!(
        bucket.object("k"),
        Some(Bytes::from_static(b"v")),
        "the store applied the write before losing the answer"
    );
}

#[test]
fn a_throttled_request_is_transient_and_applies_nothing() {
    let sim = quiet();
    let bucket = sim.bucket("orbita");
    let store = bucket.store();
    bucket.inject_once("PUT", "k", StoreFault::Status(503));

    let outcome = sim.block_on(async move { store.put("k", Bytes::from_static(b"v")).await });
    assert!(
        outcome.as_ref().is_err_and(ObjectError::is_retryable),
        "a 503 must reach the caller as retryable: {outcome:?}"
    );
    assert!(bucket.object("k").is_none());
}

#[test]
fn a_crash_injected_at_a_request_kills_the_node_that_issued_it() {
    let sim = quiet();
    let node = NodeId(1);
    let runtime = sim.add_node(node);
    let bucket = sim.bucket("orbita");
    let store = bucket.store();
    bucket.inject_once("PUT", "k", StoreFault::CrashNode(node));

    // Spawned on the node, because the point is that the crash takes the task
    // with it rather than letting it observe an error.
    runtime.spawn(async move {
        let _ = store.put("k", Bytes::from_static(b"v")).await;
    });
    sim.run_until_idle();

    assert!(!sim.is_up(node), "the injected crash did not land");
    assert!(
        bucket.object("k").is_none(),
        "the store must not have applied a request it never answered"
    );
}

#[test]
fn two_handles_on_one_name_address_one_bucket() {
    // What makes a replacement worker able to hydrate from what its
    // predecessor wrote. A per-handle bucket would make that untestable.
    let sim = quiet();
    let writer = sim.bucket("orbita");
    let reader = sim.bucket("orbita");
    let store = writer.store();

    sim.block_on(async move {
        store.put("k", Bytes::from_static(b"v")).await.expect("ok");
    });
    assert_eq!(reader.object("k"), Some(Bytes::from_static(b"v")));
    assert_eq!(reader.keys(), vec!["k".to_string()]);
}

#[test]
fn sampled_store_faults_are_drawn_against_the_run_budget() {
    // A fault site outside the budget would let a scenario spend its whole run
    // in recovery, which is the failure mode the budget exists to prevent.
    let mut config = SimConfig::new(9);
    config.store = StoreFaults {
        request_lost_permille: 1000,
        ..StoreFaults::none()
    };
    config.fault_budget = 1;
    let sim = Simulation::with_config(config);
    let bucket = sim.bucket("orbita");
    let store = bucket.store();

    let (first, second) = sim.block_on(async move {
        let first = store.put("a", Bytes::from_static(b"1")).await;
        let second = store.put("b", Bytes::from_static(b"2")).await;
        (first, second)
    });
    assert!(first.is_err(), "the budget's one fault should land");
    assert!(
        second.is_ok(),
        "the budget was spent, so the second write must go through: {second:?}"
    );
    assert_eq!(sim.faults_injected(), 1);
}

#[test]
fn store_faults_stop_when_a_scenario_says_it_is_done_breaking_things() {
    let mut config = SimConfig::new(11);
    config.store = StoreFaults {
        request_lost_permille: 1000,
        ..StoreFaults::none()
    };
    let sim = Simulation::with_config(config);
    let bucket = sim.bucket("orbita");
    sim.stop_injecting_faults();

    let store = bucket.store();
    let outcome = sim.block_on(async move { store.put("a", Bytes::from_static(b"1")).await });
    assert!(
        outcome.is_ok(),
        "a frozen world must serve the recovery a liveness check measures: {outcome:?}"
    );
}

#[test]
fn a_tag_is_never_reused_even_after_the_object_is_deleted() {
    // A compare-and-swap that succeeded against a recycled tag would swap
    // against a version that no longer means what its holder thinks.
    let sim = quiet();
    let bucket = sim.bucket("orbita");
    let store = bucket.store();

    let (first, second) = sim.block_on(async move {
        let first = store.put("k", Bytes::from_static(b"a")).await.expect("ok");
        store.delete("k").await.expect("ok");
        let second = store.put("k", Bytes::from_static(b"b")).await.expect("ok");
        (first, second)
    });
    assert_ne!(first, second);

    let store = bucket.store();
    let stale = sim.block_on(async move {
        store
            .put_if("k", Bytes::from_static(b"c"), Precondition::Match(first))
            .await
    });
    assert!(stale.is_err(), "the recycled tag must not win");
}
