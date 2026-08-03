//! Checking that a run could have happened on a single machine.
//!
//! Linearizability is the guarantee Orbita sells, and it is not something a
//! test can assert one operation at a time. It is a property of a whole
//! history: given when each client call was made and when it returned, is
//! there some serial order of those calls, consistent with real time, that a
//! single-threaded model would produce the same answers for?
//!
//! The search is Wing and Gong: repeatedly pick an operation that could go
//! next, apply it to the model, and recurse, backtracking when an answer does
//! not match. An operation can go next only if it was invoked before the
//! earliest return still outstanding, which is what encodes the real-time
//! constraint. Memoising on the pair of model state and remaining set keeps
//! the small histories a test generates cheap; this is not built for
//! production-scale histories and does not need to be.
//!
//! Operations that never returned, because the node was crashed or the call
//! timed out, are the subtle part. The client does not know whether they took
//! effect, so the checker is allowed to place them anywhere after their
//! invocation or to leave them out entirely. Anything stricter would report
//! violations that are not violations.

use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

/// The sequential specification a history is checked against.
pub trait Model {
    type State: Clone + Eq + Hash;
    type Op: Clone + Debug;
    type Ret: Clone + Debug + PartialEq;

    fn init(&self) -> Self::State;

    /// Applies one operation to the model, returning the new state and the
    /// answer a single machine would have given.
    fn step(&self, state: &Self::State, op: &Self::Op) -> (Self::State, Self::Ret);
}

/// What a client learned from an operation.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome<Ret> {
    Completed(Ret),
    /// The call timed out or the node died holding it. It may or may not have
    /// taken effect, and the checker gets to choose whichever explains the
    /// rest of the history.
    Unknown,
}

/// One client call.
#[derive(Debug, Clone)]
pub struct Entry<Op, Ret> {
    pub client: u64,
    pub op: Op,
    pub outcome: Outcome<Ret>,
    /// Ordering positions, not timestamps. Two calls that happen in the same
    /// virtual nanosecond still have an unambiguous order, which the search
    /// depends on.
    pub invoked: u64,
    pub returned: u64,
    /// Virtual time of the invocation, carried only so a printed history lines
    /// up with the trace.
    pub at_nanos: u64,
}

/// A recorded run of client operations.
#[derive(Debug, Clone)]
pub struct History<Op, Ret> {
    entries: Vec<Entry<Op, Ret>>,
}

impl<Op, Ret> Default for History<Op, Ret> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl<Op, Ret> History<Op, Ret> {
    #[must_use]
    pub fn entries(&self) -> &[Entry<Op, Ret>] {
        &self.entries
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl<Op: Debug, Ret: Debug> History<Op, Ret> {
    /// A human-readable dump, printed with a failure so the violation can be
    /// read without rerunning anything.
    #[must_use]
    pub fn render(&self) -> String {
        let mut out = String::new();
        for (i, e) in self.entries.iter().enumerate() {
            out.push_str(&format!(
                "{i:>3} client={} t={} {:?} -> {:?}\n",
                e.client, e.at_nanos, e.op, e.outcome
            ));
        }
        out
    }
}

/// A handle a client task records through.
///
/// Cloneable and shared, because every client in a scenario writes into one
/// history and the order they interleave in is the thing being checked.
pub struct Recorder<Op, Ret> {
    inner: Arc<Mutex<Recording<Op, Ret>>>,
}

impl<Op, Ret> Clone for Recorder<Op, Ret> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<Op, Ret> Default for Recorder<Op, Ret> {
    fn default() -> Self {
        Self::new()
    }
}

struct Recording<Op, Ret> {
    entries: Vec<Entry<Op, Ret>>,
    next_position: u64,
}

/// Identifies an in-flight operation so its completion lands on the right
/// entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Invocation(usize);

impl<Op, Ret> Recorder<Op, Ret> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Recording {
                entries: Vec::new(),
                next_position: 0,
            })),
        }
    }

    /// Records that a client issued an operation. Call this immediately before
    /// the call goes out, or the history will claim a real-time ordering that
    /// did not hold.
    pub fn invoke(&self, client: u64, op: Op, at_nanos: u64) -> Invocation {
        let mut rec = self.inner.lock().expect("recorder lock poisoned");
        rec.next_position += 1;
        let invoked = rec.next_position;
        rec.entries.push(Entry {
            client,
            op,
            outcome: Outcome::Unknown,
            invoked,
            // Until it returns, an operation is outstanding forever, which is
            // exactly how the search should treat it.
            returned: u64::MAX,
            at_nanos,
        });
        Invocation(rec.entries.len() - 1)
    }

    pub fn complete(&self, invocation: Invocation, ret: Ret) {
        let mut rec = self.inner.lock().expect("recorder lock poisoned");
        rec.next_position += 1;
        let returned = rec.next_position;
        let entry = &mut rec.entries[invocation.0];
        entry.outcome = Outcome::Completed(ret);
        entry.returned = returned;
    }

    /// Records that the client never learned the answer. The operation stays
    /// in the history because it may still have taken effect.
    pub fn abandon(&self, _invocation: Invocation) {
        let mut rec = self.inner.lock().expect("recorder lock poisoned");
        rec.next_position += 1;
    }
}

impl<Op: Clone, Ret: Clone> Recorder<Op, Ret> {
    #[must_use]
    pub fn history(&self) -> History<Op, Ret> {
        let rec = self.inner.lock().expect("recorder lock poisoned");
        History {
            entries: rec.entries.clone(),
        }
    }
}

/// Why a history could not be explained.
#[derive(Debug, Clone)]
pub enum Violation {
    /// No serial order explains the history. This is a real bug in the system
    /// under test.
    NotLinearizable { history: String },
    /// The search hit its budget. This says nothing about the system, only
    /// that the history was too large or too concurrent for this checker.
    Inconclusive { explored: u64 },
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotLinearizable { history } => {
                write!(f, "no sequential order explains this history:\n{history}")
            }
            Self::Inconclusive { explored } => {
                write!(f, "linearizability search gave up after {explored} states")
            }
        }
    }
}

/// The order the checker found, as indices into the history. Operations left
/// out are ones that never returned and are explained by never having taken
/// effect.
pub type Witness = Vec<usize>;

/// Checks a history against a model.
///
/// The budget bounds the search rather than the history, because concurrency
/// rather than length is what makes this expensive.
pub fn check<M: Model>(
    model: &M,
    history: &History<M::Op, M::Ret>,
    budget: u64,
) -> Result<Witness, Violation> {
    let mut search = Search {
        model,
        entries: history.entries(),
        seen: HashSet::new(),
        explored: 0,
        budget,
        witness: Vec::new(),
    };
    let remaining: Vec<usize> = (0..history.len()).collect();
    match search.explore(&model.init(), &remaining) {
        Ok(true) => {
            let mut witness = std::mem::take(&mut search.witness);
            witness.reverse();
            Ok(witness)
        }
        Ok(false) => Err(Violation::NotLinearizable {
            history: history.render(),
        }),
        Err(()) => Err(Violation::Inconclusive {
            explored: search.explored,
        }),
    }
}

struct Search<'a, M: Model> {
    model: &'a M,
    entries: &'a [Entry<M::Op, M::Ret>],
    seen: HashSet<(M::State, Vec<usize>)>,
    explored: u64,
    budget: u64,
    witness: Witness,
}

impl<M: Model> Search<'_, M> {
    /// Returns whether some ordering of `remaining` explains the rest of the
    /// history, or `Err` if the budget ran out first.
    fn explore(&mut self, state: &M::State, remaining: &[usize]) -> Result<bool, ()> {
        // Anything still outstanding never returned, so leaving all of it out
        // is a legal explanation and the search is done.
        if remaining
            .iter()
            .all(|&i| matches!(self.entries[i].outcome, Outcome::Unknown))
        {
            return Ok(true);
        }

        self.explored += 1;
        if self.explored > self.budget {
            return Err(());
        }

        let key = (state.clone(), remaining.to_vec());
        if !self.seen.insert(key) {
            return Ok(false);
        }

        let earliest_return = remaining
            .iter()
            .map(|&i| self.entries[i].returned)
            .min()
            .unwrap_or(u64::MAX);

        for (position, &index) in remaining.iter().enumerate() {
            let entry = &self.entries[index];
            // Real time forbids an operation from being linearized after an
            // operation that returned before it was even invoked.
            if entry.invoked > earliest_return {
                continue;
            }
            let (next_state, ret) = self.model.step(state, &entry.op);
            if let Outcome::Completed(expected) = &entry.outcome {
                if &ret != expected {
                    continue;
                }
            }
            let mut rest = Vec::with_capacity(remaining.len() - 1);
            rest.extend_from_slice(&remaining[..position]);
            rest.extend_from_slice(&remaining[position + 1..]);
            if self.explore(&next_state, &rest)? {
                self.witness.push(index);
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// The model Orbita's own tests check against most: one key holding one value.
///
/// A single register is enough to catch the failure that matters, which is an
/// acknowledged write that a later read does not see.
#[derive(Debug, Clone, Copy, Default)]
pub struct Register;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RegisterOp {
    Write(u64),
    Read,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RegisterRet {
    /// The write was acknowledged.
    Acked,
    Value(Option<u64>),
}

impl Model for Register {
    type State = Option<u64>;
    type Op = RegisterOp;
    type Ret = RegisterRet;

    fn init(&self) -> Self::State {
        None
    }

    fn step(&self, state: &Self::State, op: &Self::Op) -> (Self::State, Self::Ret) {
        match op {
            RegisterOp::Write(v) => (Some(*v), RegisterRet::Acked),
            RegisterOp::Read => (*state, RegisterRet::Value(*state)),
        }
    }
}

/// A key-value model for scenarios that touch more than one key.
#[derive(Debug, Clone, Copy, Default)]
pub struct KeyValue;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum KvOp {
    Put(String, u64),
    Get(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum KvRet {
    Acked,
    Value(Option<u64>),
}

impl Model for KeyValue {
    type State = std::collections::BTreeMap<String, u64>;
    type Op = KvOp;
    type Ret = KvRet;

    fn init(&self) -> Self::State {
        std::collections::BTreeMap::new()
    }

    fn step(&self, state: &Self::State, op: &Self::Op) -> (Self::State, Self::Ret) {
        match op {
            KvOp::Put(k, v) => {
                let mut next = state.clone();
                next.insert(k.clone(), *v);
                (next, KvRet::Acked)
            }
            KvOp::Get(k) => (state.clone(), KvRet::Value(state.get(k).copied())),
        }
    }
}
