//! The deterministic simulation harness.
//!
//! Implements every trait in `orbita-runtime` against virtual time, an
//! in-memory network, and a fault-injecting disk, then runs whole clusters
//! inside a single thread driven by a seeded generator. A failing run is
//! identified by its seed and replays exactly.
//!
//! This crate is the product's headline claim made executable, so it starts in
//! the first wave of work rather than after the system exists: every other
//! crate's interesting tests are written against it.
//!
//! # How a test uses it
//!
//! ```
//! use orbita_core::NodeId;
//! use orbita_runtime::{Clock, Runtime};
//! use orbita_sim::Simulation;
//! use std::time::Duration;
//!
//! let sim = Simulation::new(7);
//! let node = sim.add_node(NodeId(1));
//! let clock = node.clock().clone();
//! let slept = sim.block_on(async move {
//!     clock.sleep(Duration::from_secs(3600)).await;
//!     "an hour later"
//! });
//! assert_eq!(slept, "an hour later");
//! assert_eq!(sim.now_nanos(), 3600 * 1_000_000_000);
//! ```
//!
//! # The scheduler
//!
//! Cooperative and single-threaded, with an explicit ready set. Each step
//! draws one runnable task from the seed's stream and polls it once. Time only
//! moves when nothing is runnable, and then only as far as the next timer
//! deadline. Two consequences follow, and they are the reason for the design:
//! the same seed produces the same interleaving on every machine, and a test
//! can cover an hour of lease expiry in a millisecond of real time.
//!
//! The scheduler owns the futures it polls, which means those futures need a
//! reference back into the world. Since `orbita_runtime::Runtime` requires
//! `Send + Sync`, and `unsafe` is forbidden, that is `Arc` and `Mutex` rather
//! than `Rc` and `RefCell`. Nothing ever contends on the mutex; it is there to
//! satisfy the trait bounds, not to coordinate threads.
//!
//! A cooperative scheduler was chosen over a faithful reimplementation of a
//! work-stealing executor because the fidelity that matters here is the order
//! of observable events, not the mechanics of how a real runtime parks a
//! thread. Every point where a task can be preempted is an `await`, and every
//! `await` in Orbita goes through a trait in `orbita-runtime`, so the set of
//! interleavings this explores is the set that can actually happen.
//!
//! # The trace, and what it can replay
//!
//! Every scheduling decision, message, disk operation, and injected fault is
//! recorded as a formatted line. Two runs of one seed produce byte-identical
//! traces, and that is asserted in the test suite rather than assumed.
//!
//! A trace explains a failure; it does not replay one on its own. Replay is
//! seed plus the same binary. A trace rich enough to drive a replay would have
//! to encode every scheduling decision, and it would go stale the moment the
//! code under test changed, which is exactly when someone wants to replay it.
//! Reporting the seed and printing the trace gets the useful half of both.
//!
//! # The fault budget
//!
//! Faults are drawn against a budget and held off until a warm-up interval has
//! passed, both tunable per test through [`SimConfig`]. Injecting a fault on
//! every operation finds shallow bugs quickly and then stops finding anything,
//! because the cluster spends the whole run recovering and never reaches the
//! states worth checking.
//!
//! # The limit, stated honestly
//!
//! RocksDB does its own file I/O beneath `orbita_runtime::Disk`, so this crate
//! cannot inject faults inside it. Simulation covers the distributed protocol
//! layer, meaning WAL replication, consensus, ownership, routing, and the read
//! path, and treats a local RocksDB as a trusted component with faults
//! injected at its API boundary instead. The claim this supports is "the
//! distributed layer is verified under deterministic simulation," not "the
//! whole system is."
//!
//! Work brief: `docs/plan/05-sim.md`.

#![forbid(unsafe_code)]

mod config;
mod disk;
pub mod harness;
pub mod lin;
mod net;
mod runtime;
mod sim;
mod trace;
mod world;

pub use config::{DiskFaults, NetworkFaults, SimConfig};
pub use disk::{SimDisk, SimFile};
pub use harness::{check_seeds, seeds, Failure};
pub use net::SimTransport;
pub use runtime::{SimClock, SimRng, SimRuntime};
pub use sim::{DiskPolicy, Simulation};
pub use trace::Trace;
