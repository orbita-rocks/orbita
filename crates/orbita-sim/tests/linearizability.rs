//! The checker's own behaviour, checked against histories with known answers.
//!
//! A checker that never reports a violation is worse than no checker, so these
//! pin down both directions: histories that must be accepted, and histories
//! that must be rejected.

use orbita_sim::lin::{check, Recorder, Register, RegisterOp, RegisterRet, Violation};

const BUDGET: u64 = 100_000;

type RegisterRecorder = Recorder<RegisterOp, RegisterRet>;

#[test]
fn a_sequential_history_is_linearizable() {
    let rec = RegisterRecorder::new();
    let w = rec.invoke(1, RegisterOp::Write(7), 0);
    rec.complete(w, RegisterRet::Acked);
    let r = rec.invoke(1, RegisterOp::Read, 1);
    rec.complete(r, RegisterRet::Value(Some(7)));

    assert!(check(&Register, &rec.history(), BUDGET).is_ok());
}

#[test]
fn a_read_that_misses_an_acknowledged_write_is_not_linearizable() {
    let rec = RegisterRecorder::new();
    let w = rec.invoke(1, RegisterOp::Write(7), 0);
    rec.complete(w, RegisterRet::Acked);
    let r = rec.invoke(2, RegisterOp::Read, 1);
    // The write returned before this read was even invoked, so no serial order
    // puts the read first.
    rec.complete(r, RegisterRet::Value(None));

    assert!(matches!(
        check(&Register, &rec.history(), BUDGET),
        Err(Violation::NotLinearizable { .. })
    ));
}

#[test]
fn a_read_concurrent_with_a_write_may_return_either_value() {
    // Client 2's read is invoked while client 1's write is in flight, so both
    // answers are legal and the checker must accept both.
    for observed in [None, Some(7)] {
        let rec = RegisterRecorder::new();
        let w = rec.invoke(1, RegisterOp::Write(7), 0);
        let r = rec.invoke(2, RegisterOp::Read, 0);
        rec.complete(r, RegisterRet::Value(observed));
        rec.complete(w, RegisterRet::Acked);

        assert!(
            check(&Register, &rec.history(), BUDGET).is_ok(),
            "a concurrent read returning {observed:?} was rejected"
        );
    }
}

#[test]
fn writes_that_overlap_can_be_ordered_either_way() {
    let rec = RegisterRecorder::new();
    let a = rec.invoke(1, RegisterOp::Write(1), 0);
    let b = rec.invoke(2, RegisterOp::Write(2), 0);
    rec.complete(a, RegisterRet::Acked);
    rec.complete(b, RegisterRet::Acked);
    let r = rec.invoke(3, RegisterOp::Read, 1);
    // Either write could have landed last, and the checker only has to find
    // one order that works.
    rec.complete(r, RegisterRet::Value(Some(1)));

    assert!(check(&Register, &rec.history(), BUDGET).is_ok());
}

#[test]
fn an_operation_that_never_returned_may_be_treated_as_never_having_happened() {
    let rec = RegisterRecorder::new();
    let lost = rec.invoke(1, RegisterOp::Write(7), 0);
    rec.abandon(lost);
    let r = rec.invoke(2, RegisterOp::Read, 1);
    rec.complete(r, RegisterRet::Value(None));

    assert!(
        check(&Register, &rec.history(), BUDGET).is_ok(),
        "a timed-out write that the client never saw must be allowed to have been lost"
    );
}

#[test]
fn an_operation_that_never_returned_may_also_be_treated_as_having_happened() {
    let rec = RegisterRecorder::new();
    let lost = rec.invoke(1, RegisterOp::Write(7), 0);
    rec.abandon(lost);
    let r = rec.invoke(2, RegisterOp::Read, 1);
    // The peer applied it and the reply was lost, which is the asymmetric
    // partition case and is perfectly legal.
    rec.complete(r, RegisterRet::Value(Some(7)));

    assert!(check(&Register, &rec.history(), BUDGET).is_ok());
}

#[test]
fn a_value_no_client_ever_wrote_is_not_linearizable() {
    let rec = RegisterRecorder::new();
    let r = rec.invoke(1, RegisterOp::Read, 0);
    rec.complete(r, RegisterRet::Value(Some(42)));

    assert!(matches!(
        check(&Register, &rec.history(), BUDGET),
        Err(Violation::NotLinearizable { .. })
    ));
}

#[test]
fn an_empty_history_is_linearizable() {
    let rec = RegisterRecorder::new();
    assert_eq!(
        check(&Register, &rec.history(), BUDGET).unwrap(),
        Vec::new()
    );
}

#[test]
fn the_witness_orders_every_completed_operation() {
    let rec = RegisterRecorder::new();
    let a = rec.invoke(1, RegisterOp::Write(1), 0);
    rec.complete(a, RegisterRet::Acked);
    let b = rec.invoke(1, RegisterOp::Write(2), 1);
    rec.complete(b, RegisterRet::Acked);
    let r = rec.invoke(1, RegisterOp::Read, 2);
    rec.complete(r, RegisterRet::Value(Some(2)));

    assert_eq!(
        check(&Register, &rec.history(), BUDGET).unwrap(),
        vec![0, 1, 2]
    );
}

#[test]
fn a_search_that_runs_out_of_budget_says_so_rather_than_claiming_a_violation() {
    // A wide history with a tiny budget. Reporting this as a violation would
    // be worse than useless, because it would send someone hunting a bug that
    // is not there.
    let rec = RegisterRecorder::new();
    let mut pending = Vec::new();
    for client in 0..12u64 {
        pending.push(rec.invoke(client, RegisterOp::Write(client), 0));
    }
    for inv in pending {
        rec.complete(inv, RegisterRet::Acked);
    }
    let r = rec.invoke(99, RegisterOp::Read, 1);
    rec.complete(r, RegisterRet::Value(Some(1_000)));

    assert!(matches!(
        check(&Register, &rec.history(), 50),
        Err(Violation::Inconclusive { .. })
    ));
}
