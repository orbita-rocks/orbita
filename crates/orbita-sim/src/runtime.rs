//! What a simulated node is handed instead of the real world.
//!
//! `SimRuntime` is the whole seam. A subsystem written against
//! `R: orbita_runtime::Runtime` cannot tell whether it is talking to a disk or
//! to a `Vec<u8>` that is about to lie about `fsync`, and that is the point:
//! the code under test is the same code that ships.

use crate::disk::SimDisk;
use crate::net::SimTransport;
use crate::world::{SimCore, SimSleep, SimYield};

use orbita_core::NodeId;
use orbita_runtime::{Clock, Rng, Runtime};

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

/// One node's view of the simulated world.
#[derive(Clone)]
pub struct SimRuntime {
    core: Arc<SimCore>,
    node: NodeId,
    clock: SimClock,
    disk: SimDisk,
    transport: SimTransport,
    rng: Arc<SimRng>,
}

impl SimRuntime {
    pub(crate) fn new(core: Arc<SimCore>, node: NodeId) -> Self {
        Self {
            clock: SimClock {
                core: core.clone(),
                origin_millis: core.config.wall_clock_origin_millis,
            },
            disk: SimDisk::new(core.clone(), node),
            transport: SimTransport::new(core.clone(), node),
            rng: Arc::new(SimRng { core: core.clone() }),
            core,
            node,
        }
    }

    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node
    }

    /// Hands control back to the scheduler without letting time pass.
    ///
    /// Test code uses this to force an interleaving point in a stretch of
    /// synchronous work, which is where a race would otherwise be invisible to
    /// the simulator.
    pub fn yield_now(&self) -> impl Future<Output = ()> + Send {
        SimYield::new()
    }
}

impl Runtime for SimRuntime {
    type Clock = SimClock;
    type Disk = SimDisk;
    type Transport = SimTransport;
    type Rng = SimRng;

    fn clock(&self) -> &Self::Clock {
        &self.clock
    }

    fn disk(&self) -> &Self::Disk {
        &self.disk
    }

    fn transport(&self) -> &Self::Transport {
        &self.transport
    }

    fn rng(&self) -> &Self::Rng {
        &self.rng
    }

    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.core.spawn_task(Some(self.node), Box::pin(future));
    }
}

/// Virtual time.
///
/// The clock only moves when every runnable task is blocked, so a test can
/// cover an hour of lease expiry in the time it takes to poll a few hundred
/// futures. Nothing here consults the operating system, which is what lets two
/// machines agree on a trace.
#[derive(Clone)]
pub struct SimClock {
    core: Arc<SimCore>,
    origin_millis: u64,
}

impl Clock for SimClock {
    fn now_millis(&self) -> u64 {
        self.origin_millis + self.core.now() / 1_000_000
    }

    fn monotonic_nanos(&self) -> u64 {
        self.core.now()
    }

    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send {
        SimSleep::new(self.core.clone(), duration.as_nanos() as u64)
    }
}

/// The application's randomness, seeded from the run.
///
/// This is a different stream from the one the scheduler and the fault
/// injector draw on, so raising a fault rate does not shift the jitter a
/// subsystem picks and shrinking a repro stays possible.
pub struct SimRng {
    core: Arc<SimCore>,
}

impl Rng for SimRng {
    fn next_u64(&self) -> u64 {
        self.core.app_rng().next_u64()
    }
}
