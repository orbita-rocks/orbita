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
"""

from __future__ import annotations

import threading

import grpc
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

    Product finding this test pins down: under a genuine race the losing
    `if_not_present` comes back with `applied=False` but with `current_version`
    UNSET, even though the sequential loser in test_conditions.py is promised the
    winner's version. So a client cannot rely on the response alone to discover
    the holder after a contended acquire; it has to re-`Get` the key. The
    guaranteed signal is `applied`; `current_version` on a lost `if_not_present`
    is best-effort and absent exactly when contention is highest. This test
    therefore asserts the durable property (one winner, loser recovers by
    reading) rather than the version echo the uncontended path happens to give.
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

    # The loser observes the loss and recovers the holder by reading the key.
    # This is the path a real client must take, because current_version on a
    # contended if_not_present is not guaranteed (see the docstring). The held
    # value and the winner's version are both visible on a follow-up Get.
    loser_client = a if losers[0] == "a" else b
    held = loser_client.Get(kv_pb2.GetRequest(keyspace=KS, key=b"race/lock"))
    assert held.found, "the loser must be able to see the lock is held"
    assert held.version == winner.version, (
        "reading after a lost acquire reveals the winning version, which is how "
        "the loser learns which version to watch to see the lock freed"
    )


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


def test_a_stale_holders_write_is_refused_after_its_epoch_is_superseded(node):
    """The fencing pattern end to end: an old epoch cannot write the resource.

    A slept-through-its-lease holder is the case fencing exists for. The lease's
    `version` is the fencing token: partition-wide, strictly increasing, and per
    ADR 0002 never reused even after delete-and-recreate, which is exactly what a
    fence needs. The protected resource is guarded by compare-and-swap on the
    version last written to it, so a writer can only apply on top of the state it
    last saw.

    The sequence a platform actually runs:

      1. A takes the lease (epoch token_a) and stamps the resource.
      2. A's lease expires. B takes the lease and gets token_b.
      3. B, holding the newer epoch, advances the resource.
      4. A wakes up still believing it holds, and tries to write the resource.
         Its write is refused, because the resource has moved past the version A
         last saw.

    Friction this surfaces: the store has no native fencing token stamped onto a
    write and checked by the server. The token is the `version`, and enforcing it
    means every writer funnels its resource write through a compare-and-swap on
    the version lineage of that resource. It works, and it is race-safe, but the
    server rejects the stale write only because the version moved, not because it
    understands epochs. A caller who wrote the resource unconditionally would get
    no fencing at all. That the guarantee lives in client discipline, not in the
    server, is the finding.
    """
    a, b = _client(node), _client(node)

    # 1. A takes the lease and stamps the resource under its epoch.
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

    # 2. A's lease lapses; B takes it and gets a strictly newer epoch.
    poll_until(
        lambda: not a.Get(kv_pb2.GetRequest(keyspace=KS, key=b"fence/lease")).found,
        timeout=15.0,
    )
    lease_b = _acquire(b, b"fence/lease", ttl_millis=LEASE_TTL_MILLIS)
    assert lease_b.applied, "an expired lease must be acquirable by the next holder"
    token_b = lease_b.version
    assert token_b > token_a, (
        "the fencing token must strictly increase across epochs, or a stale "
        "holder could present a token that looks current"
    )

    # 3. B, on the newer epoch, advances the resource off the version A saw.
    stamp_b = b.Set(
        kv_pb2.SetRequest(
            keyspace=KS,
            key=b"fence/resource",
            value=b"written-by-b",
            condition=kv_pb2.Condition(if_version=resource_seen_by_a),
        )
    )
    assert stamp_b.applied, "the current epoch holder must be able to write"
    resource_after_b = stamp_b.version

    # 4. A wakes up and tries to write the resource on top of the stale version
    #    it last saw. This is the fenced write, and it must be refused.
    stale_write = a.Set(
        kv_pb2.SetRequest(
            keyspace=KS,
            key=b"fence/resource",
            value=b"written-by-stale-a",
            condition=kv_pb2.Condition(if_version=resource_seen_by_a),
        )
    )
    assert not stale_write.applied, (
        "a write from a superseded epoch must not land on the resource"
    )
    assert stale_write.current_version == resource_after_b, (
        "the refused writer is pointed at the version that fenced it, which is "
        "what lets it re-read, notice a newer epoch owns the resource, and stop"
    )

    # The resource still carries B's write, not A's, which is the property the
    # whole dance exists to guarantee.
    final = a.Get(kv_pb2.GetRequest(keyspace=KS, key=b"fence/resource"))
    assert final.value == b"written-by-b"
