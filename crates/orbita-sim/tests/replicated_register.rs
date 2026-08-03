//! The test of the tester.
//!
//! A mock replicated register, deliberately small, whose only interesting knob
//! is how many replicas have to acknowledge a write before the client is told
//! it succeeded. At two of three the register survives losing its owner. At
//! one of three it does not, and the linearizability checker has to notice
//! within a modest number of seeds.
//!
//! This exists because a simulator that never fails is indistinguishable from
//! a simulator that does not work. The brief asks for this to be a permanent
//! part of the suite rather than a check someone ran once, so it lives here
//! and runs on every pull request.
//!
//! The mock is not Orbita. It stands in for the shape of Orbita's write path
//! while the real one is being built, and two things about it are frank
//! shortcuts: routing and failover consult a shared oracle rather than a real
//! control plane, because the failure being demonstrated is in the write
//! quorum and modelling a Raft group here would only add ways for the test to
//! be wrong.

use orbita_core::NodeId;
use orbita_runtime::{
    Clock, Disk, File, OpenOptions, PeerCall, PeerHandler, Rng, Runtime, ServiceId, Transport,
    TransportError,
};
use orbita_sim::lin::{check, Recorder, Register, RegisterOp, RegisterRet, Violation};
use orbita_sim::{Failure, SimConfig, SimRuntime, Simulation};

use bytes::Bytes;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const WRITE: u16 = 1;
const READ: u16 = 2;
const REPLICATE: u16 = 1;

/// Which node clients send to, and which node the control plane has anointed.
/// One shared cell standing in for a partition map.
type Oracle = Arc<Mutex<NodeId>>;

#[derive(Debug, Default, Clone, Copy)]
struct Stored {
    /// What a read returns. Only a write that reached its quorum lands here,
    /// because a value that becomes visible before it is durable enough to
    /// survive a failover is a read of something that may never have happened.
    value: Option<u64>,
    version: u64,
    /// The highest version this node has handed out as an owner. Kept apart
    /// from `version` so a promoted replica cannot reuse a version its
    /// predecessor already assigned.
    reserved: u64,
}

/// One replica of the register, which is also the owner when the oracle says
/// so.
#[derive(Clone)]
struct Node {
    id: NodeId,
    rt: SimRuntime,
    stored: Arc<Mutex<Stored>>,
    owner: Oracle,
    peers: Vec<NodeId>,
    /// How many copies, counting the owner's own, must exist before the client
    /// is told the write succeeded. This is the bug under test.
    ack_quorum: usize,
}

impl Node {
    fn version(&self) -> u64 {
        self.stored.lock().unwrap().version
    }

    fn reserve(&self) -> u64 {
        let mut stored = self.stored.lock().unwrap();
        stored.reserved = stored.reserved.max(stored.version) + 1;
        stored.reserved
    }

    /// Makes a version visible to readers, ignoring one that a newer write has
    /// already overtaken.
    fn commit(&self, version: u64, value: u64) {
        let mut stored = self.stored.lock().unwrap();
        if version > stored.version {
            stored.version = version;
            stored.value = Some(value);
        }
    }

    fn is_owner(&self) -> bool {
        *self.owner.lock().unwrap() == self.id
    }

    async fn write(&self, value: u64) -> Result<Bytes, TransportError> {
        if !self.is_owner() {
            return Err(TransportError::Remote(format!(
                "{} is not the owner",
                self.id
            )));
        }
        let version = self.reserve();

        // The owner logs before it acknowledges, so the disk seam sits on the
        // write path here the way it does in the real system.
        let file = self
            .rt
            .disk()
            .open("register.log", OpenOptions::create())
            .await
            .map_err(|e| TransportError::Remote(e.to_string()))?;
        file.append(record(version, value))
            .await
            .map_err(|e| TransportError::Remote(e.to_string()))?;
        file.sync()
            .await
            .map_err(|e| TransportError::Remote(e.to_string()))?;

        let mut acks = 1;
        let mut deferred = Vec::new();
        for &peer in &self.peers {
            if acks >= self.ack_quorum {
                deferred.push(peer);
                continue;
            }
            if self.replicate(peer, version, value).await.is_ok() {
                acks += 1;
            }
        }
        // Whatever the quorum did not need still goes out, just without the
        // client waiting for it. At a quorum of one that is every copy, which
        // is exactly why the window for losing a write is so easy to miss by
        // inspection.
        for peer in deferred {
            let node = self.clone();
            self.rt.spawn(async move {
                // Copies the quorum did not wait for go out on the background
                // replicator, which batches rather than sending one message
                // per write. That batching interval is the window in which an
                // acknowledged write exists on exactly one machine, and a
                // quorum of one leaves it open for every write.
                let batch = 1_000_000 + node.rt.rng().below(2_000_000);
                node.rt.clock().sleep(Duration::from_nanos(batch)).await;
                let _ = node.replicate(peer, version, value).await;
            });
        }

        if acks >= self.ack_quorum {
            self.commit(version, value);
            Ok(Bytes::new())
        } else {
            Err(TransportError::Remote(
                "write did not reach a quorum".into(),
            ))
        }
    }

    async fn replicate(
        &self,
        peer: NodeId,
        version: u64,
        value: u64,
    ) -> Result<Bytes, TransportError> {
        self.rt
            .transport()
            .call(
                peer,
                PeerCall {
                    service: ServiceId::Wal,
                    method: REPLICATE,
                    payload: record(version, value),
                },
            )
            .await
    }

    fn read(&self) -> Result<Bytes, TransportError> {
        if !self.is_owner() {
            return Err(TransportError::Remote(format!(
                "{} is not the owner",
                self.id
            )));
        }
        Ok(encode(self.stored.lock().unwrap().value))
    }

    fn apply(&self, version: u64, value: u64) -> Result<Bytes, TransportError> {
        // A late copy of an older write must not undo a newer one, which also
        // makes replication idempotent under duplicates and retries.
        self.commit(version, value);
        Ok(Bytes::new())
    }
}

impl PeerHandler for Node {
    fn handle(
        &self,
        _from: NodeId,
        call: PeerCall,
    ) -> impl Future<Output = Result<Bytes, TransportError>> + Send {
        let node = self.clone();
        async move {
            match (call.service, call.method) {
                (ServiceId::Proxy, WRITE) => node.write(decode_u64(&call.payload[..8])).await,
                (ServiceId::Proxy, READ) => node.read(),
                (ServiceId::Wal, REPLICATE) => node.apply(
                    decode_u64(&call.payload[..8]),
                    decode_u64(&call.payload[8..16]),
                ),
                _ => Err(TransportError::Remote("no such method".into())),
            }
        }
    }
}

fn record(version: u64, value: u64) -> Bytes {
    let mut out = Vec::with_capacity(16);
    out.extend_from_slice(&version.to_be_bytes());
    out.extend_from_slice(&value.to_be_bytes());
    Bytes::from(out)
}

fn encode(value: Option<u64>) -> Bytes {
    match value {
        Some(v) => Bytes::from(v.to_be_bytes().to_vec()),
        None => Bytes::new(),
    }
}

fn decode(payload: &Bytes) -> Option<u64> {
    if payload.len() >= 8 {
        Some(decode_u64(&payload[..8]))
    } else {
        None
    }
}

fn decode_u64(bytes: &[u8]) -> u64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(bytes);
    u64::from_be_bytes(buf)
}

/// A client that alternates reads and writes against whichever node the oracle
/// currently names, recording every invocation and completion.
fn spawn_client(
    sim: &Simulation,
    rt: SimRuntime,
    client: u64,
    ops: usize,
    recorder: Recorder<RegisterOp, RegisterRet>,
    owner: Oracle,
) {
    // A driver task rather than a node task, so crashing a server does not
    // take the client with it.
    sim.spawn(async move {
        for i in 0..ops {
            let pause = 200_000 + rt.rng().below(600_000);
            rt.clock().sleep(Duration::from_nanos(pause)).await;

            let target = *owner.lock().unwrap();
            let now = rt.clock().monotonic_nanos();

            if rt.rng().chance(1, 2) {
                let value = client * 1_000 + i as u64 + 1;
                let invocation = recorder.invoke(client, RegisterOp::Write(value), now);
                let result = rt
                    .transport()
                    .call(
                        target,
                        PeerCall {
                            service: ServiceId::Proxy,
                            method: WRITE,
                            payload: Bytes::from(value.to_be_bytes().to_vec()),
                        },
                    )
                    .await;
                match result {
                    Ok(_) => recorder.complete(invocation, RegisterRet::Acked),
                    // The client does not know whether it took effect, and
                    // neither does the checker.
                    Err(_) => recorder.abandon(invocation),
                }
            } else {
                let invocation = recorder.invoke(client, RegisterOp::Read, now);
                let result = rt
                    .transport()
                    .call(
                        target,
                        PeerCall {
                            service: ServiceId::Proxy,
                            method: READ,
                            payload: Bytes::new(),
                        },
                    )
                    .await;
                match result {
                    Ok(payload) => {
                        recorder.complete(invocation, RegisterRet::Value(decode(&payload)));
                    }
                    Err(_) => recorder.abandon(invocation),
                }
            }
        }
    });
}

/// Runs one seed: three replicas, two clients, and one owner crash at a moment
/// the seed picks.
fn run(seed: u64, ack_quorum: usize) -> Result<(), Failure> {
    let mut config = SimConfig::new(seed);
    // Long enough that a healthy call never trips it, short enough that
    // calling a dead node does not dominate the run.
    config.call_timeout = Duration::from_millis(5);
    let sim = Simulation::with_config(config);

    let owner: Oracle = Arc::new(Mutex::new(NodeId(1)));
    let mut nodes = BTreeMap::new();
    for id in 1..=3u64 {
        let rt = sim.add_node(NodeId(id));
        let node = Node {
            id: NodeId(id),
            rt: rt.clone(),
            stored: Arc::new(Mutex::new(Stored::default())),
            owner: owner.clone(),
            peers: (1..=3u64).filter(|p| *p != id).map(NodeId).collect(),
            ack_quorum,
        };
        rt.transport().register(ServiceId::Proxy, node.clone());
        rt.transport().register(ServiceId::Wal, node.clone());
        nodes.insert(NodeId(id), node);
    }

    let recorder: Recorder<RegisterOp, RegisterRet> = Recorder::new();
    for client in 1..=2u64 {
        let rt = sim.add_node(NodeId(100 + client));
        spawn_client(&sim, rt, client, 16, recorder.clone(), owner.clone());
    }

    // Let the cluster work, then take the owner out at a moment the seed
    // chooses. Fixing the instant would test one interleaving forever.
    let until = 3_000_000 + sim.random_below(6_000_000);
    sim.run_for(Duration::from_nanos(until));

    let victim = *owner.lock().unwrap();
    sim.crash(victim);
    let promoted = nodes
        .values()
        .filter(|n| n.id != victim)
        .max_by_key(|n| (n.version(), n.id))
        .expect("two replicas survive a single crash")
        .id;
    *owner.lock().unwrap() = promoted;

    sim.run_until_idle();

    let history = recorder.history();
    match check(&Register, &history, 500_000) {
        Ok(_) => Ok(()),
        // A budget exhaustion says nothing about the system under test, so
        // treating it as a failure would make the suite lie.
        Err(Violation::Inconclusive { .. }) => Ok(()),
        Err(violation) => Err(sim.failure(violation.to_string())),
    }
}

#[test]
fn acknowledging_at_two_of_three_survives_losing_the_owner() {
    orbita_sim::check_seeds(
        "acknowledging_at_two_of_three_survives_losing_the_owner",
        40,
        |seed| run(seed, 2),
    );
}

#[test]
fn acknowledging_at_one_of_three_is_caught_by_the_linearizability_checker() {
    let mut caught = None;
    for seed in orbita_sim::seeds(100) {
        if let Err(failure) = run(seed, 1) {
            caught = Some(failure);
            break;
        }
    }
    let failure = caught.expect(
        "a register that acknowledges at one of three replicas survived every seed, which \
         means the checker cannot see the bug it exists to catch",
    );
    assert!(failure.reason.contains("no sequential order explains"));
}
