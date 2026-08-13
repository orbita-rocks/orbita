"""The coordination primitives, driven the way a platform would drive them.

The pitch for this store is locks, leases, and epochs a platform coordinates
on. The rest of the suite proves the KV mechanics underneath. This file proves
the *composition*: that a client with nothing but the generated stubs can build
a mutex, a lease, and a fencing token out of `Condition`, `ttl_millis`, and the
partition-wide `version`, and that they behave under contention and expiry the
way the pitch claims.

Everything here goes through the public Kv API. Where a coordination pattern is
awkward to express that way, the awkwardness is called out in a comment, because
surfacing that friction is a stated goal of this file rather than a distraction
from it.

The building blocks, mapped to the API:

  lock acquire   Set(Condition(if_not_present=True))     one winner, losers see
                                                         applied=False
  lease          the same Set plus ttl_millis            expiry is absence
  renew / release Set/Delete(Condition(if_version=v))    proves you still hold it
  fencing token  the `version` a write returns           partition-wide, strictly
                                                         increasing, never reused

There is no Lock, Lease, or Epoch RPC. These are conventions a client layers on
CAS. That they are only conventions is the point of testing them from outside.

One consequence, surfaced by the last two tests: the "fencing token" is only a
convention, and a weak one. A `Condition` guards a write by the version of the
key being written and nothing else, so a write cannot check that its holder
still owns the lease. Single-key CAS catches a stale writer once the resource
has moved, but it cannot fence the window after a new holder takes the lease and
before it writes the resource. That gap is product finding #104, and it is
recorded here as a strict xfail rather than papered over.
"""

from __future__ import annotations

import threading

import grpc
import pytest
from orbita.v1 import kv_pb2, kv_pb2_grpc

from conftest import DEFAULT_KEYSPACE as KS, poll_until

# Short enough to keep the suite quick, long enough that a loaded machine does
# not expire a lease before the holder has taken it and looked at it.
LEASE_TTL_MILLIS = 400


def _client(node) -> kv_pb2_grpc.KvStub:
    """A second, independent client against the same node.

    The suite's `kv` fixture is one connected stub. A test about two clients
    racing needs two channels, because "two clients" that share a channel is a
    weaker claim: gRPC could be serialising them below the API. Each client here
    gets its own channel, sized to what the server published, exactly as the
    harness sizes its own.
    """
    channel = grpc.insecure_channel(
        f"127.0.0.1:{node.port}",
        options=[
            ("grpc.max_receive_message_length", node.limits.max_message_bytes),
            ("grpc.max_send_message_length", node.limits.max_message_bytes),
        ],
    )
    return kv_pb2_grpc.KvStub(channel)


def _acquire(client, key, ttl_millis=None):
    """Try to take a lock/lease by creating its key if absent.

    Returns the SetResponse. `applied` is whether this caller won, and on a win
    `version` is the fencing token for the epoch it just started.
    """
    return client.Set(
        kv_pb2.SetRequest(
            keyspace=KS,
            key=key,
            value=b"held",
            ttl_millis=ttl_millis,
            condition=kv_pb2.Condition(if_not_present=True),
        )
    )


def test_two_clients_racing_to_acquire_a_lock_produce_exactly_one_winner(node):
    """Contention on a mutex resolves to a single holder, and the loser knows.

    Two independent clients fire `if_not_present` at the same key at the same
    instant. The lock is correct iff exactly one call reports `applied=True`;
    the other must report `applied=False`, and must then be able to learn who
    holds the lock so it can back off rather than assume it holds.

    The loser learns that from `current_version` in its own response, not from
    a follow-up `Get`. That distinction is the whole value of the primitive: a
    second round trip to find the holder reopens the race the conditional write
    was supposed to settle atomically, and it is exactly the round trip a client
    cannot make safely. So this asserts the same contract the sequential loser
    in test_conditions.py gets, because whether contention happened to be
    concurrent or sequential is not something a client controls or should be
    able to observe.

    The follow-up `Get` is still here, checked against the same version, to
    prove the response and the store agree about who holds the lock.
    """
    a, b = _client(node), _client(node)
    results: dict[str, object] = {}
    start = threading.Barrier(2)

    def contend(name, client):
        # The barrier makes both requests leave at the same moment, so the race
        # is real rather than serialised by Python scheduling.
        start.wait()
        results[name] = _acquire(client, b"race/lock")

    threads = [
        threading.Thread(target=contend, args=("a", a)),
        threading.Thread(target=contend, args=("b", b)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    winners = [name for name, r in results.items() if r.applied]
    losers = [name for name, r in results.items() if not r.applied]
    assert len(winners) == 1, f"a mutex must not admit two holders, got {winners}"
    assert len(losers) == 1

    winner = results[winners[0]]
    loser = results[losers[0]]

    # The loser identifies the holder from its own response. Without this a
    # losing client has to re-Get the key, and between the loss and the read the
    # lock can be released and retaken, so what it reads back is not necessarily
    # what beat it.
    assert loser.HasField("current_version"), (
        "a contended if_not_present loser must be told which version beat it, "
        "the same as a loser that arrived second"
    )
    assert loser.current_version == winner.version, (
        "the version the loser is told is the one it must watch to see the lock "
        "freed, so it has to be the winning write's"
    )

    # And the store agrees with what the loser was told.
    loser_client = a if losers[0] == "a" else b
    held = loser_client.Get(kv_pb2.GetRequest(keyspace=KS, key=b"race/lock"))
    assert held.found, "the loser must be able to see the lock is held"
    assert held.version == winner.version


def test_a_lease_holder_discovers_expiry_when_its_next_guarded_op_is_refused(node):
    """A lease that lapses mid-hold is discoverable, and only by asking.

    A holder takes a lease with a TTL, then keeps holding past the deadline. It
    finds out the lease is gone the way the API forces it to: its next guarded
    operation fails. Renewing is a compare-and-swap on the version it was handed
    (`if_version`), and once the key has expired that CAS cannot apply, with an
    unset `current_version` meaning the key is absent rather than merely moved
    on. There is no push, no event, no expiry callback: absence is the signal,
    and the holder only sees it by attempting something. That is the friction
    this test documents while proving the detection works.
    """
    holder = _client(node)

    acquired = _acquire(holder, b"lease/session", ttl_millis=LEASE_TTL_MILLIS)
    assert acquired.applied
    token = acquired.version

    # While the lease is fresh, a renew (CAS on the held version) applies. This
    # is the holder proving it still holds before the deadline.
    renew = holder.Set(
        kv_pb2.SetRequest(
            keyspace=KS,
            key=b"lease/session",
            value=b"held",
            ttl_millis=LEASE_TTL_MILLIS,
            condition=kv_pb2.Condition(if_version=token),
        )
    )
    assert renew.applied, "a live lease must be renewable by its holder"
    token = renew.version

    # Now let it lapse. Expiry is only observable as the key going absent, so we
    # wait for exactly that rather than sleeping a guessed interval.
    poll_until(
        lambda: not holder.Get(
            kv_pb2.GetRequest(keyspace=KS, key=b"lease/session")
        ).found,
        timeout=15.0,
    )

    # The holder, still believing it holds, tries to renew again. This is the
    # moment of discovery: the guarded op is refused because the lease is gone.
    stale_renew = holder.Set(
        kv_pb2.SetRequest(
            keyspace=KS,
            key=b"lease/session",
            value=b"held",
            ttl_millis=LEASE_TTL_MILLIS,
            condition=kv_pb2.Condition(if_version=token),
        )
    )
    assert not stale_renew.applied, "renewing an expired lease must not silently apply"
    assert not stale_renew.HasField("current_version"), (
        "an unset current_version is how the holder learns the lease expired "
        "rather than being stolen at a newer version"
    )

    # And the lease is genuinely free: a fresh client can take it uncontested.
    taker = _client(node)
    reacquired = _acquire(taker, b"lease/session", ttl_millis=LEASE_TTL_MILLIS)
    assert reacquired.applied, "an expired lease must be acquirable by someone new"


def test_a_stale_cas_write_loses_once_a_new_holder_has_moved_the_resource(node):
    """A stale compare-and-swap loses after the resource has moved on.

    This is the *only* piece of the fencing story the public API actually
    enforces, and it is narrower than fencing. The resource is guarded by
    compare-and-swap on its own version. Once a newer holder has written the
    resource, that write moves the resource's version, and any writer still
    holding the version it last saw is refused. So the CAS protects the resource
    from a writer working off stale state — but it does that by comparing the
    RESOURCE's version, and nothing here checks any lease, epoch, or token.

    What this test does NOT prove, and must not be read as proving:

      * It does not prove epoch fencing. Neither `token_a` nor `token_b` (the
        lease versions) participate in the resource-write condition. The stale
        writer is refused because the resource version moved under it, not
        because its epoch was superseded. See ADR 0002 for why versions are the
        only token the API exposes, and product finding #104 for why that is not
        a fence.
      * It does not cover the takeover-before-write window, where a superseded
        holder can still land a write. That window is exercised, and shown to be
        unfenced, by the xfail test below.

    The sequence here deliberately advances the resource under a new holder
    BEFORE the stale writer wakes up, which is precisely the case CAS handles:

      1. A takes the lease and stamps the resource.
      2. A's lease expires; B takes the lease at a strictly newer version.
      3. B advances the resource off the version A last saw.
      4. A wakes up and its CAS on the old resource version is refused, because
         the resource has moved — not because A's epoch is stale.
    """
    a, b = _client(node), _client(node)

    # 1. A takes the lease and stamps the resource. token_a is recorded only to
    #    show below that it plays no part in refusing A's later write.
    lease_a = _acquire(a, b"fence/lease", ttl_millis=LEASE_TTL_MILLIS)
    assert lease_a.applied
    token_a = lease_a.version

    stamp_a = a.Set(
        kv_pb2.SetRequest(
            keyspace=KS,
            key=b"fence/resource",
            value=b"written-by-a",
            condition=kv_pb2.Condition(if_not_present=True),
        )
    )
    assert stamp_a.applied
    resource_seen_by_a = stamp_a.version

    # 2. A's lease lapses; B takes it and gets a strictly newer version. This is
    #    a real property (ADR 0002: monotonic, never reissued), but note it is
    #    only ordering on the lease key — it is not consulted by the CAS below.
    poll_until(
        lambda: not a.Get(kv_pb2.GetRequest(keyspace=KS, key=b"fence/lease")).found,
        timeout=15.0,
    )
    lease_b = _acquire(b, b"fence/lease", ttl_millis=LEASE_TTL_MILLIS)
    assert lease_b.applied, "an expired lease must be acquirable by the next holder"
    token_b = lease_b.version
    assert token_b > token_a, (
        "lease versions increase across epochs (ADR 0002); this is what a fence "
        "WOULD lean on if the API let a write check a second key, which it does "
        "not (see #104)"
    )

    # 3. B advances the resource off the version A saw.
    stamp_b = b.Set(
        kv_pb2.SetRequest(
            keyspace=KS,
            key=b"fence/resource",
            value=b"written-by-b",
            condition=kv_pb2.Condition(if_version=resource_seen_by_a),
        )
    )
    assert stamp_b.applied, "writing on top of the version you last saw applies"
    resource_after_b = stamp_b.version

    # 4. A wakes up and retries its CAS on the resource version it last saw. It
    #    is refused — but strictly because the RESOURCE moved, which the assert
    #    on current_version below makes explicit: A is pointed at the resource's
    #    new version, a resource fact, with nothing about epochs anywhere in it.
    stale_write = a.Set(
        kv_pb2.SetRequest(
            keyspace=KS,
            key=b"fence/resource",
            value=b"written-by-stale-a",
            condition=kv_pb2.Condition(if_version=resource_seen_by_a),
        )
    )
    assert not stale_write.applied, (
        "a CAS on a resource version that has moved must not apply"
    )
    assert stale_write.current_version == resource_after_b, (
        "the refused writer is pointed at the resource's current version — a "
        "compare-and-swap conflict signal, not an epoch-fencing one"
    )

    # The resource carries B's write. This holds because B happened to write
    # first; it is not a guarantee that a superseded holder cannot write. See the
    # xfail test for the case where the stale holder writes before B does.
    final = a.Get(kv_pb2.GetRequest(keyspace=KS, key=b"fence/resource"))
    assert final.value == b"written-by-b"


@pytest.mark.xfail(
    reason=(
        "No native fencing: a write's Condition only checks the version of the "
        "key being written, so it cannot verify the writer still holds the "
        "lease. In the takeover-before-write window the superseded holder's CAS "
        "still matches and applies. Product finding #104. This xfail is strict, "
        "so if a real fence ever makes A refused here, the suite goes red and "
        "this test must be promoted to a passing guarantee."
    ),
    strict=True,
)
def test_a_superseded_holder_is_fenced_before_the_new_holder_writes(node):
    """The window fencing must cover, which the public API does not cover today.

    A genuine fence must stop a superseded holder from writing the resource the
    instant a newer epoch exists — including the window after the new holder has
    taken the lease but BEFORE it has touched the resource. A distributed lock
    that only protects the resource once the new holder writes it has a hole
    exactly the size of the new holder's startup work.

    This test expresses that guarantee and asserts it. It is xfail because the
    guarantee does not hold: with only single-key CAS, the best a client can do
    is compare-and-swap on the resource's own version, and in this window the
    resource still sits at the version the stale holder last saw, so its write
    applies. Threading `token_b` into the condition is not expressible — a
    `Condition` cannot reference a second key (proto/orbita/v1/kv.proto:30,
    core WriteCondition). Cross-reference product finding #104.

    Interleaving under test:

      1. A takes the lease (epoch token_a) and stamps the resource.
      2. A's lease expires; B takes the lease at token_b > token_a.
      3. B has NOT yet written the resource.
      4. Stale A, still believing it holds, writes the resource guarded by the
         only fence the API offers — a CAS on the resource version it last saw.
         A real fence would refuse this. Today it applies, so the assertion that
         it is refused fails, and the xfail records the unsupported window.
    """
    a, b = _client(node), _client(node)

    # 1. A takes the lease and stamps the resource under its epoch.
    lease_a = _acquire(a, b"window/lease", ttl_millis=LEASE_TTL_MILLIS)
    assert lease_a.applied
    token_a = lease_a.version

    stamp_a = a.Set(
        kv_pb2.SetRequest(
            keyspace=KS,
            key=b"window/resource",
            value=b"written-by-a",
            condition=kv_pb2.Condition(if_not_present=True),
        )
    )
    assert stamp_a.applied
    resource_seen_by_a = stamp_a.version

    # 2. A's lease lapses; B takes it at a strictly newer epoch.
    poll_until(
        lambda: not a.Get(kv_pb2.GetRequest(keyspace=KS, key=b"window/lease")).found,
        timeout=15.0,
    )
    lease_b = _acquire(b, b"window/lease", ttl_millis=LEASE_TTL_MILLIS)
    assert lease_b.applied
    token_b = lease_b.version
    assert token_b > token_a

    # 3. B has taken over but has NOT written the resource. The resource still
    #    sits at resource_seen_by_a.
    current = a.Get(kv_pb2.GetRequest(keyspace=KS, key=b"window/resource"))
    assert current.version == resource_seen_by_a, (
        "precondition for the window: B has not moved the resource yet"
    )

    # 4. Stale A writes, guarded by the strongest fence the API can express: a
    #    CAS on the resource version it last saw. A real fence would refuse this
    #    because token_a is superseded. It does not — token_b cannot enter the
    #    condition — so the write applies. The assertion that A is fenced fails,
    #    which is the xfail: the takeover-before-write window is unsupported.
    stale_write = a.Set(
        kv_pb2.SetRequest(
            keyspace=KS,
            key=b"window/resource",
            value=b"written-by-stale-a",
            condition=kv_pb2.Condition(if_version=resource_seen_by_a),
        )
    )
    assert not stale_write.applied, (
        "a superseded holder must not be able to write the resource, even before "
        "the new holder does — this is the fence the store does not provide (#104)"
    )
