//! Liveness: what has to become true once the world stops breaking.
//!
//! Everything else this crate checks is safety. Linearizability, epoch
//! ordering, and fencing all answer "did the cluster do something wrong",
//! and a cluster that is safe and permanently stuck answers no to all three.
//! Both liveness defects found in Orbita so far were exactly that shape, a
//! promotion starved by unrelated map churn and a drain blocked by an
//! unrelated partition, and both were found by a person reading a diff.
//! Review does not scale across a seed batch; an invariant does.
//!
//! # The shape of the check
//!
//! A scenario breaks the world however it likes, then calls
//! [`converge_within`]. That stops fault injection, heals every link, and
//! requires a set of caller-supplied conditions to become true within a bound
//! of virtual time. Nothing here knows what a partition or an owner is: the
//! conditions are the caller's, because the only crate that can say what
//! "converged" means for a cluster is the one that runs it.
//!
//! Two things this deliberately does not do. It does not restart crashed
//! nodes, because a cluster has to converge on the survivors it actually has
//! rather than on the ones it wishes it had. And it does not treat "no faults
//! were ever injected" as a pass worth much, which is why [`Simulation`]
//! reports [`Simulation::last_fault_nanos`] and the failure text says when the
//! last one landed.

use crate::harness::Failure;
use crate::sim::Simulation;

use std::time::Duration;

/// One convergence condition that does not hold yet.
///
/// Named separately from its evidence so a failure says *which* guarantee
/// broke. "The cluster did not converge" sends someone reading a trace from
/// the top; "every partition is owned: partition 3 is Fenced with no owner"
/// sends them to the promotion path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unmet {
    /// The condition, phrased as the thing that was supposed to be true.
    pub condition: &'static str,
    /// The specific evidence: which partition, which node, which version.
    pub detail: String,
}

impl Unmet {
    #[must_use]
    pub fn new(condition: &'static str, detail: impl Into<String>) -> Self {
        Self {
            condition,
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for Unmet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.condition, self.detail)
    }
}

/// Stops breaking the world, then requires `probe` to come back empty within
/// `bound` of virtual time.
///
/// `poll` is how often the conditions are re-examined. It should be the
/// shortest interval the system under test makes decisions on, because
/// checking faster only costs simulator steps and checking slower blurs the
/// bound by up to one interval.
///
/// The bound belongs to the caller for the same reason the conditions do: it
/// has to be derived from the timings the system was configured with, or it is
/// a number somebody picked. A bound that is too loose proves nothing and a
/// bound that is too tight produces a suite people learn to re-run.
///
/// # Errors
///
/// Returns a [`Failure`] naming every condition still unmet, either at the
/// bound or as soon as the world goes idle, since an idle simulation cannot
/// change its answer no matter how long the deadline is.
pub fn converge_within<P>(
    sim: &Simulation,
    bound: Duration,
    poll: Duration,
    probe: P,
) -> Result<(), Failure>
where
    P: Fn() -> Vec<Unmet>,
{
    sim.stop_injecting_faults();
    sim.heal_all();

    let started = sim.now_nanos();
    let deadline = started + bound.as_nanos() as u64;
    let poll_nanos = poll.as_nanos().max(1) as u64;

    loop {
        let now = sim.now_nanos();
        let unmet = probe();
        if unmet.is_empty() {
            if now > deadline {
                // Nothing observable happened between the previous probe and
                // this one, or the clock would not have jumped over the
                // deadline to reach it. So the cluster was still unconverged
                // at the bound and only finished afterwards, which is the
                // thing the bound exists to refuse.
                return Err(sim.failure(report(sim, bound, now - started, &LATE, false)));
            }
            return Ok(());
        }
        if now >= deadline {
            return Err(sim.failure(report(sim, bound, now - started, &unmet, false)));
        }

        // The last step lands exactly on the deadline rather than overshooting
        // it, so a cluster that converges just past the bound fails rather
        // than passing on the granularity of the poll.
        // The step is clamped to land on the deadline rather than past it, so
        // the bound is a bound rather than a bound plus however far the next
        // timer happened to be.
        sim.run_for(Duration::from_nanos(poll_nanos.min(deadline - now)));
        if sim.now_nanos() == now {
            // Nothing is runnable and no timer is pending. Waiting out the
            // rest of the bound would burn simulator steps to reach the same
            // answer, and saying "idle" is more useful than saying "slow".
            return Err(sim.failure(report(sim, bound, 0, &unmet, true)));
        }
    }
}

/// Reported when every condition holds but only after the bound had passed.
/// A cluster that converges late converges; it just does not converge within
/// the guarantee, and calling that a pass would make the bound decorative.
const LATE: [Unmet; 1] = [Unmet {
    condition: "the cluster converges within the bound",
    detail: String::new(),
}];

fn report(sim: &Simulation, bound: Duration, waited: u64, unmet: &[Unmet], idle: bool) -> String {
    let last_fault = match sim.last_fault_nanos() {
        Some(at) => format!("the last fault landed at {}ms", at / 1_000_000),
        None => "no fault was ever injected, so this run proved little".to_string(),
    };
    let how = if idle {
        "the simulation went idle with the cluster still unconverged".to_string()
    } else {
        format!(
            "the cluster did not converge within {bound:?} of the last fault healing (waited {:?})",
            Duration::from_nanos(waited)
        )
    };
    let conditions: Vec<String> = unmet.iter().map(|u| format!("  - {u}")).collect();
    format!(
        "{how}; {} condition(s) unmet:\n{}\n{last_fault}",
        unmet.len(),
        conditions.join("\n")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::SimRuntime;
    use bytes::Bytes;
    use orbita_core::NodeId;
    use orbita_runtime::{Clock, PeerCall, Runtime, ServiceId, Transport};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    /// A world with a task that keeps waking, so time advances the way it does
    /// in a cluster with a sweep loop in it.
    fn ticking_world(seed: u64) -> Simulation {
        let sim = Simulation::new(seed);
        let node = sim.add_node(NodeId(1));
        let clock = node.clock().clone();
        node.spawn(async move {
            loop {
                clock.sleep(Duration::from_millis(10)).await;
            }
        });
        sim
    }

    #[test]
    fn a_condition_that_becomes_true_in_time_converges() {
        let sim = ticking_world(1);
        let settle_at = sim.now_nanos() + Duration::from_millis(200).as_nanos() as u64;

        let outcome = converge_within(
            &sim,
            Duration::from_secs(1),
            Duration::from_millis(25),
            || {
                if sim.now_nanos() >= settle_at {
                    Vec::new()
                } else {
                    vec![Unmet::new("the value settles", "not yet")]
                }
            },
        );

        assert!(outcome.is_ok(), "{:?}", outcome.err());
    }

    #[test]
    fn a_condition_that_never_becomes_true_names_itself_in_the_failure() {
        let sim = ticking_world(2);

        let failure = converge_within(
            &sim,
            Duration::from_millis(500),
            Duration::from_millis(25),
            || {
                vec![Unmet::new(
                    "every partition is owned",
                    "partition 3 is unowned",
                )]
            },
        )
        .expect_err("a condition that never holds must fail");

        assert!(
            failure.reason.contains("every partition is owned"),
            "the failure must name the condition, got: {}",
            failure.reason
        );
        assert!(
            failure.reason.contains("partition 3 is unowned"),
            "the failure must carry the evidence, got: {}",
            failure.reason
        );
    }

    #[test]
    fn an_idle_world_fails_immediately_rather_than_waiting_out_the_bound() {
        // No task, no timer: the answer at the bound is the answer now.
        let sim = Simulation::new(3);
        let polls = Arc::new(AtomicU64::new(0));
        let counter = Arc::clone(&polls);

        let failure = converge_within(&sim, Duration::from_secs(600), Duration::from_millis(1), {
            move || {
                counter.fetch_add(1, Ordering::Relaxed);
                vec![Unmet::new("something", "never")]
            }
        })
        .expect_err("an idle world cannot converge");

        assert!(failure.reason.contains("went idle"), "{}", failure.reason);
        assert_eq!(
            polls.load(Ordering::Relaxed),
            1,
            "an idle world should be diagnosed on the first probe"
        );
    }

    #[test]
    fn convergence_measures_recovery_rather_than_continued_disruption() {
        let sim = Simulation::with_config(crate::SimConfig::chaotic(4));
        let a = sim.add_node(NodeId(1));
        let b = sim.add_node(NodeId(2));
        b.transport().register(ServiceId::Wal, Sink);
        let chatter = |runtime: SimRuntime| {
            let transport = runtime.transport().clone();
            let clock = runtime.clock().clone();
            runtime.spawn(async move {
                loop {
                    let _ = transport
                        .call(
                            NodeId(2),
                            PeerCall {
                                service: ServiceId::Wal,
                                method: 1,
                                payload: Bytes::new(),
                            },
                        )
                        .await;
                    clock.sleep(Duration::from_millis(1)).await;
                }
            });
        };
        chatter(a);
        sim.run_for(Duration::from_millis(200));
        let before = sim.faults_injected();
        assert!(before > 0, "the world was supposed to be hostile");

        let outcome = converge_within(
            &sim,
            Duration::from_millis(100),
            Duration::from_millis(10),
            Vec::new,
        );
        assert!(outcome.is_ok());

        // Everything after the check is recovery by definition, so no further
        // traffic may draw a fault however long the run continues.
        sim.run_for(Duration::from_millis(500));
        assert_eq!(
            sim.faults_injected(),
            before,
            "fault injection must stay stopped once a convergence check has run"
        );
    }

    #[derive(Clone)]
    struct Sink;

    impl orbita_runtime::PeerHandler for Sink {
        async fn handle(
            &self,
            _from: NodeId,
            _call: PeerCall,
        ) -> Result<Bytes, orbita_runtime::TransportError> {
            Ok(Bytes::new())
        }
    }
}
