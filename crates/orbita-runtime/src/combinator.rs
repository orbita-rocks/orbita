//! Combinators for waiting on more than one thing at a time.
//!
//! These exist because the rest of the contract could not express the shape
//! Orbita is built out of: "send this to three peers and continue once two of
//! them have it." `Runtime::spawn` returns nothing to wait on, so without these
//! every crate that needs a quorum writes its own, and two of them already had
//! before this module existed.
//!
//! Nothing here touches the runtime. A combinator is a future that polls other
//! futures, so it needs no executor, no timer, and no allocation beyond pinning
//! its children. That is why these are plain functions rather than methods on
//! `Runtime`: they behave identically in production and under simulation
//! because there is nothing in them that could differ.
//!
//! The one exception is [`timeout`], which needs a clock, and takes one.

use crate::clock::Clock;

use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

/// Which of two futures finished first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Either<A, B> {
    Left(A),
    Right(B),
}

/// The error from [`timeout`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("timed out")]
pub struct Elapsed;

/// Runs every future concurrently and resolves once `needed` of them have
/// completed, or once all of them have, whichever happens first.
///
/// The results come back in completion order rather than argument order, since
/// a quorum caller wants to know which responses arrived and the ordering of
/// the ones that did not is meaningless. Check the length: a shorter result
/// than `needed` means the futures ran out first, which for a replication
/// quorum means the write did not reach enough peers.
///
/// Futures that have not completed are dropped, which cancels them. For peer
/// calls that is the intent, since a third acknowledgement after the quorum is
/// reached changes nothing.
pub async fn quorum<F>(futures: Vec<F>, needed: usize) -> Vec<F::Output>
where
    F: Future,
{
    let mut pending: Vec<Pin<Box<F>>> = futures.into_iter().map(Box::pin).collect();
    let mut done: Vec<F::Output> = Vec::new();

    poll_fn(move |cx| {
        let mut i = 0;
        while i < pending.len() {
            match pending[i].as_mut().poll(cx) {
                Poll::Ready(value) => {
                    done.push(value);
                    pending.remove(i);
                }
                Poll::Pending => i += 1,
            }
        }

        if done.len() >= needed || pending.is_empty() {
            Poll::Ready(std::mem::take(&mut done))
        } else {
            Poll::Pending
        }
    })
    .await
}

/// Runs every future concurrently and resolves when all of them have.
///
/// Results come back in completion order, matching [`quorum`]. Use `quorum`
/// with `needed` equal to the length if you need every result and want the
/// same shape.
pub async fn join_all<F>(futures: Vec<F>) -> Vec<F::Output>
where
    F: Future,
{
    let n = futures.len();
    quorum(futures, n).await
}

/// Resolves with whichever future finishes first, dropping the other.
pub async fn select<A, B>(a: A, b: B) -> Either<A::Output, B::Output>
where
    A: Future,
    B: Future,
{
    let mut a = Box::pin(a);
    let mut b = Box::pin(b);

    poll_fn(move |cx| {
        // `a` is polled first, so a future that is ready at the same instant as
        // its timeout wins. Preferring the work over the deadline keeps a
        // request that arrived just in time from being reported as expired.
        if let Poll::Ready(v) = a.as_mut().poll(cx) {
            return Poll::Ready(Either::Left(v));
        }
        if let Poll::Ready(v) = b.as_mut().poll(cx) {
            return Poll::Ready(Either::Right(v));
        }
        Poll::Pending
    })
    .await
}

/// Fails with [`Elapsed`] if `future` has not finished within `duration`.
///
/// The clock is a parameter rather than ambient so that a simulated run
/// expires timeouts in virtual time, which is what lets a test cover a lease
/// expiry without waiting for one.
pub async fn timeout<C, F>(clock: &C, duration: Duration, future: F) -> Result<F::Output, Elapsed>
where
    C: Clock,
    F: Future,
{
    match select(future, clock.sleep(duration)).await {
        Either::Left(value) => Ok(value),
        Either::Right(()) => Err(Elapsed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// A future that returns `value` after being polled `delay` times, so a
    /// test can order completions without involving a clock.
    fn after_polls<T: Clone>(delay: usize, value: T) -> impl Future<Output = T> {
        let mut polls = 0;
        poll_fn(move |cx| {
            polls += 1;
            if polls > delay {
                Poll::Ready(value.clone())
            } else {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        })
    }

    #[tokio::test]
    async fn quorum_resolves_without_waiting_for_the_stragglers() {
        let results = quorum(
            vec![
                after_polls(0, "fast"),
                after_polls(1, "middle"),
                after_polls(100, "slow"),
            ],
            2,
        )
        .await;

        assert_eq!(
            results,
            vec!["fast", "middle"],
            "the slow peer must not hold up the quorum"
        );
    }

    #[tokio::test]
    async fn quorum_reports_completion_order_not_argument_order() {
        let results = quorum(vec![after_polls(3, "third"), after_polls(0, "first")], 2).await;
        assert_eq!(results, vec!["first", "third"]);
    }

    #[tokio::test]
    async fn quorum_that_cannot_be_reached_returns_what_it_got() {
        // Two peers answer, three were needed. The caller has to be able to
        // tell this apart from success, or a write would be acknowledged on
        // too few replicas.
        let results = quorum(vec![after_polls(0, 1), after_polls(1, 2)], 3).await;
        assert_eq!(results.len(), 2, "short of quorum, and detectably so");
    }

    #[tokio::test]
    async fn a_zero_quorum_is_already_satisfied() {
        let results = quorum(vec![after_polls(5, 1)], 0).await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn quorum_drops_the_futures_it_did_not_wait_for() {
        let live = Arc::new(AtomicUsize::new(0));

        struct Tracked(Arc<AtomicUsize>);
        impl Drop for Tracked {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }

        let make = |delay: usize| {
            live.fetch_add(1, Ordering::SeqCst);
            let guard = Tracked(live.clone());
            async move {
                let _held = guard;
                after_polls(delay, ()).await;
            }
        };

        let futures = vec![make(0), make(100)];
        assert_eq!(live.load(Ordering::SeqCst), 2);
        quorum(futures, 1).await;

        assert_eq!(
            live.load(Ordering::SeqCst),
            0,
            "an unfinished peer call must be cancelled, not leaked"
        );
    }

    #[tokio::test]
    async fn join_all_waits_for_everything() {
        let results = join_all(vec![after_polls(2, 'a'), after_polls(0, 'b')]).await;
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn select_returns_the_first_to_finish() {
        assert_eq!(
            select(after_polls(5, "slow"), after_polls(0, "quick")).await,
            Either::Right("quick")
        );
    }

    #[tokio::test]
    async fn select_prefers_the_work_over_the_deadline_on_a_tie() {
        // Both are ready on the first poll. Left winning is what keeps
        // `timeout` from reporting a request that just made it as expired.
        assert_eq!(
            select(after_polls(0, "work"), after_polls(0, "deadline")).await,
            Either::Left("work")
        );
    }

    #[tokio::test]
    async fn timeout_passes_through_a_result_that_arrives_in_time() {
        let clock = crate::tokio_runtime::TokioClock::new();
        let got = timeout(&clock, Duration::from_secs(30), after_polls(1, 7)).await;
        assert_eq!(got, Ok(7));
    }

    #[tokio::test]
    async fn timeout_gives_up_on_a_future_that_never_finishes() {
        let clock = crate::tokio_runtime::TokioClock::new();
        let never = poll_fn(|_| Poll::<()>::Pending);
        let got = timeout(&clock, Duration::from_millis(10), never).await;
        assert_eq!(got, Err(Elapsed));
    }
}
