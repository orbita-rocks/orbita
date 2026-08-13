"""A real rolling upgrade across two binary versions, driven end to end.

The upgrade machinery shipped as chart rendering plus Rust unit and integration
tests. Those prove the pieces in isolation; none of them starts a cluster on
one binary, replaces every node with a newer binary one at a time, and checks
from outside that no acknowledged write was lost, that readiness gated each
step, and that the shipped `finalize-upgrade` command then locks the old binary
out. This is that test. The first time this sequence runs should not be at a
user's site.

# The two binaries

The cluster version a binary speaks is derived from its crate version at
compile time (`orbita-control::version`). So a binary one minor below this one
speaks only the previous cluster version, which is exactly the peer the
compatibility gate exists to judge. `previous_binary` produces that older side.

By default that old side is the current source restamped one minor down, not a
prior release: this repository has cut none (no tags, no GitHub releases), and
every revision carrying the upgrade machinery under test is already at the
current minor, so no earlier revision both speaks the lower version *and* runs
this feature. See the note in `harness.previous_binary`. That default shares
code between the two binaries, so this run covers the compatibility GATE and
the finalize/lockout sequence end to end, but cannot catch an incompatibility
introduced *between* releases. Set `ORBITA_PREV_REV` to a real earlier
implementation (or `ORBITA_PREV_BINARY` to a prior release binary) to make it a
true cross-version test; the harness validates that side speaks the previous
minor. When the previous minor is not expressible (a minor-zero workspace) the
whole test skips with the reason rather than pretending to cover the upgrade.
    In particular, the synthetic old binary already knows newer method and command
    decoders, so it cannot reproduce a real old binary's unknown-method response.
    Exact service integration and pre-append log tests cover those compatibility
    boundaries; set `ORBITA_PREV_REV` for the real cross-version-code path.

# The shape

Three leader-group voters and three workers, all started on the old binary, so
the cluster bootstraps at the old cluster version and a real partition is
owned and writable. A writer runs throughout. Each node is then replaced with
the new binary one at a time, waiting for `cluster ready` before the next, the
same gate a StatefulSet rolling update applies. Only once every live node
speaks the new version does `finalize-upgrade` advance it. Then an old-binary
worker tries to rejoin and is refused: it runs, never becomes ready, and names
the version condition it fails. Throughout, the keyspace created while the
cluster was at the old version stays listed and its sentinel value stays
readable, which is the one-way door from the #24 review made observable: the
new binary must read the old control log, not truncate it.
"""

from __future__ import annotations

import json
import re
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

import grpc
import pytest

import harness
from orbita.v1 import kv_pb2, kv_pb2_grpc

from conftest import DEFAULT_KEYSPACE as KS

LEADER_IDS = (1, 2, 3)
WORKER_IDS = (4, 5, 6)
REJOIN_ID = 7
JOIN_ID = 8

# A rolling upgrade of six processes plus two-binary bring-up is not a
# sub-second test, so every wait is generous. A slow machine should make this
# slower, not red.
READY_TIMEOUT = 90.0
OWNER_TIMEOUT = 60.0


@dataclass
class NodeProc:
    """One `orbita serve` process, rolled in place across binaries.

    The id, ports, and data directory are fixed for the life of the node so
    that replacing its binary is exactly what a rolling update does: same
    identity, same storage, newer code.
    """

    node_id: int
    role: str
    client_port: int
    peer_port: int
    data_dir: Path
    log_path: Path
    binary: Path
    leader_peers: str
    proc: subprocess.Popen | None = None
    _log = None

    @property
    def endpoint(self) -> str:
        return f"http://127.0.0.1:{self.client_port}"

    def start(self, binary: Path) -> None:
        self.binary = binary
        self._log = self.log_path.open("ab")
        self.proc = subprocess.Popen(
            [
                str(binary),
                "serve",
                "--node-id",
                str(self.node_id),
                "--role",
                self.role,
                "--listen",
                f"127.0.0.1:{self.client_port}",
                "--advertise",
                f"127.0.0.1:{self.client_port}",
                "--peer-listen",
                f"127.0.0.1:{self.peer_port}",
                "--peer-advertise",
                f"127.0.0.1:{self.peer_port}",
                "--data-dir",
                str(self.data_dir),
                "--leader-peers",
                self.leader_peers,
            ],
            stdout=self._log,
            stderr=subprocess.STDOUT,
        )

    def terminate(self, drain_timeout: float = 30.0) -> None:
        """Stop the process the way a Kubernetes rolling replacement does.

        A StatefulSet rolling update sends SIGTERM and only escalates to SIGKILL
        after the termination grace period. That ordering is the point: the
        version-gated shutdown path runs on SIGTERM and decides Raft stepdown
        and whether failover is rollback-compatible or a finalized handoff. A
        straight SIGKILL would bypass that path, so a rollout that hangs or emits
        the wrong shutdown protocol would still pass this test. So terminate
        gracefully, and only fall back to SIGKILL if the drain overruns, which is
        itself a failure of the shutdown path worth surfacing in the log.
        """
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=drain_timeout)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=30)
        if self._log is not None:
            self._log.close()
            self._log = None

    def kill(self) -> None:
        """Hard-stop for teardown, where a clean drain no longer matters."""
        if self.proc is not None and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=30)
        if self._log is not None:
            self._log.close()
            self._log = None

    def roll(self, binary: Path) -> None:
        # Graceful termination, because a rolling update never SIGKILLs a healthy
        # replica; it drains it so the shutdown path can hand off cleanly.
        self.terminate()
        # The kernel needs a moment to release the two listeners before the
        # replacement binds the same ports.
        time.sleep(1.0)
        self.start(binary)

    def log(self) -> str:
        return self.log_path.read_text(errors="replace") if self.log_path.exists() else ""


@dataclass
class Cluster:
    """A multi-process Orbita cluster the test rolls across binaries."""

    new: Path
    old: Path
    workdir: Path
    leaders: list[NodeProc] = field(default_factory=list)
    workers: list[NodeProc] = field(default_factory=list)
    extra: list[NodeProc] = field(default_factory=list)
    leader_peers: str = ""
    _occupied: set[int] = field(default_factory=set)

    def free_pair(self) -> int:
        """A client port whose client and peer ports collide with no other node.

        Each node binds its client port and the peer port above it. `free_port`
        only proves one pair free at the moment it is called, so two calls can
        hand back adjacent ports where one node's peer is another's client.
        Tracking what is already spoken for is what keeps a six-node cluster
        from binding onto itself.
        """
        for _ in range(200):
            port = harness.free_port()
            if port in self._occupied or port + 1 in self._occupied:
                continue
            self._occupied.update((port, port + 1))
            return port
        raise RuntimeError("could not find a disjoint client/peer port pair")

    def bootstrap(self, worker_ids: tuple[int, ...] = WORKER_IDS) -> None:
        """Start a whole cluster on the old binary.

        The worker set is a parameter because one scenario needs the cluster
        to start short of its replication factor: a partition that already has
        every copy it wants has no placement to give a joining worker, so a
        test of whether a join is placed would pass on a cluster that never
        had a decision to make.
        """
        leader_ports = {nid: self.free_pair() for nid in LEADER_IDS}
        self.leader_peers = ",".join(
            f"{nid}=127.0.0.1:{port + 1}" for nid, port in leader_ports.items()
        )
        for nid in LEADER_IDS:
            self.leaders.append(self._make(nid, "leader", leader_ports[nid]))
        for nid in worker_ids:
            self.workers.append(self._make(nid, "worker", self.free_pair()))
        for node in self.leaders + self.workers:
            node.start(self.old)

    def join_worker(self, node_id: int, binary: Path) -> NodeProc:
        """Add a worker to a running cluster, on whichever binary is asked for.

        This is growing a cluster, which is a different act from replacing a
        node in place: the joining process has no data directory and no prior
        registration, so everything the control plane knows about it comes
        from its first heartbeat.
        """
        node = self._make(node_id, "worker", self.free_pair())
        self.workers.append(node)
        node.start(binary)
        return node

    def _make(self, node_id: int, role: str, client_port: int) -> NodeProc:
        return NodeProc(
            node_id=node_id,
            role=role,
            client_port=client_port,
            peer_port=client_port + 1,
            data_dir=self.workdir / f"{role}-{node_id}",
            log_path=self.workdir / f"{role}-{node_id}.log",
            binary=self.old,
            leader_peers=self.leader_peers,
        )

    def all(self) -> list[NodeProc]:
        return self.leaders + self.workers + self.extra

    def teardown(self) -> None:
        for node in self.all():
            node.kill()

    # -- readiness -----------------------------------------------------------

    def is_ready(self, node: NodeProc) -> bool:
        """Whether the shipped readiness probe says this node is ready.

        `cluster ready` exits 0 only for a node that has joined, recovered, and
        caught up, and non-zero otherwise. Using it rather than a bespoke check
        means the test gates on the exact surface a rollout gates on.
        """
        return (
            subprocess.run(
                [str(self.new), "--endpoint", node.endpoint, "cluster", "ready"],
                capture_output=True,
            ).returncode
            == 0
        )

    def ready_conditions(self, node: NodeProc) -> str:
        result = subprocess.run(
            [str(self.new), "--endpoint", node.endpoint, "cluster", "ready"],
            capture_output=True,
            text=True,
        )
        return result.stdout + result.stderr

    def wait_ready(self, node: NodeProc, timeout: float = READY_TIMEOUT) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if node.proc is not None and node.proc.poll() is not None:
                raise AssertionError(
                    f"node {node.node_id} exited with {node.proc.returncode} "
                    f"instead of becoming ready:\n{node.log()}"
                )
            if self.is_ready(node):
                return
            time.sleep(0.25)
        raise AssertionError(
            f"node {node.node_id} never became ready:\n{self.ready_conditions(node)}"
        )

    def wait_all_ready(self) -> None:
        for node in self.leaders + self.workers:
            self.wait_ready(node)

    # -- leader-directed admin ----------------------------------------------

    def on_leader(self, *args: str) -> str:
        """Run a leader-only admin command, following the leader.

        A follower answers an admin call with "not the leader" rather than
        forwarding, so the test tries each configured voter until one is the
        current leader, which is what a client without server-side forwarding
        has to do.
        """
        last = ""
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            for node in self.leaders:
                result = subprocess.run(
                    [str(self.new), "--endpoint", node.endpoint, *args],
                    capture_output=True,
                    text=True,
                )
                last = result.stdout + result.stderr
                if "not the leader" not in last and result.returncode == 0:
                    return last
            time.sleep(0.25)
        raise AssertionError(f"no leader served {args!r}: {last}")

    def refused_by_leader(self, *args: str) -> str:
        """Run a command that the current leader must reject."""
        last = ""
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            for node in self.leaders:
                result = subprocess.run(
                    [str(self.new), "--endpoint", node.endpoint, *args],
                    capture_output=True,
                    text=True,
                )
                last = result.stdout + result.stderr
                if "not the leader" in last:
                    continue
                if result.returncode != 0:
                    return last
                raise AssertionError(f"leader accepted {args!r} before finalization: {last}")
            time.sleep(0.25)
        raise AssertionError(f"no leader rejected {args!r}: {last}")

    def describe(self) -> dict:
        """`cluster describe` as data, so an assertion reads a field rather
        than a rendered column that exists to be read by a person."""
        last = ""
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            for node in self.leaders:
                result = subprocess.run(
                    [
                        str(self.new),
                        "--endpoint",
                        node.endpoint,
                        "cluster",
                        "describe",
                        "--output",
                        "json",
                    ],
                    capture_output=True,
                    text=True,
                )
                last = result.stdout + result.stderr
                if result.returncode == 0 and "not the leader" not in last:
                    return json.loads(result.stdout)
            time.sleep(0.25)
        raise AssertionError(f"no leader served a describe: {last}")

    def holders(self) -> set[int]:
        """Every node the map names as an owner or a replica of anything."""
        described = self.describe()
        held: set[int] = set()
        for partition in described["partitions"]:
            if partition["owner_node_id"]:
                held.add(partition["owner_node_id"])
            held.update(replica["node_id"] for replica in partition["replicas"])
        return held

    def cluster_version(self) -> str:
        described = self.on_leader("cluster", "describe")
        match = re.search(r"version\s+(\d+\.\d+)", described)
        assert match, f"no version in describe output: {described}"
        return match.group(1)

    def wait_partition_owner(self, timeout: float = OWNER_TIMEOUT) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            described = self.on_leader("cluster", "describe")
            # The partitions table lists an owner id in the OWNER column once
            # one is placed; "none" means the keyspace is not yet writable.
            rows = described.split("PARTITIONS", 1)
            if len(rows) == 2 and re.search(r"\n\s+\d+\s+\d+\s+\[.*?\)\s+\d+", rows[1]):
                return
            time.sleep(0.5)
        raise AssertionError(f"no partition gained an owner:\n{described}")

    def live_worker_endpoints(self) -> list[str]:
        return [w.endpoint for w in self.workers if w.proc is not None and w.proc.poll() is None]


class Writer:
    """A background writer that records only acknowledged writes.

    Each write is a distinct key, so a recorded key proves that value was
    accepted, and reading it back after the upgrade proves it was not lost.
    Writes are aimed at whatever worker is currently up, because any worker
    forwards to the owner, and one worker is being replaced at any moment.
    """

    def __init__(self, cluster: Cluster):
        self._cluster = cluster
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._channels: dict[str, grpc.Channel] = {}
        self.acknowledged: list[tuple[bytes, bytes]] = []
        self.errors = 0

    def _stub(self, endpoint: str) -> kv_pb2_grpc.KvStub:
        channel = self._channels.get(endpoint)
        if channel is None:
            channel = grpc.insecure_channel(endpoint.removeprefix("http://"))
            self._channels[endpoint] = channel
        return kv_pb2_grpc.KvStub(channel)

    def _run(self) -> None:
        counter = 0
        while not self._stop.is_set():
            endpoints = self._cluster.live_worker_endpoints()
            if not endpoints:
                time.sleep(0.05)
                continue
            key = f"upgrade/{counter}".encode()
            value = f"value-{counter}".encode()
            try:
                self._stub(endpoints[counter % len(endpoints)]).Set(
                    kv_pb2.SetRequest(keyspace=KS, key=key, value=value), timeout=2.0
                )
                self.acknowledged.append((key, value))
                counter += 1
            except grpc.RpcError:
                # A worker mid-roll, or a partition briefly without an owner,
                # is expected. The write simply was not acknowledged, so it is
                # not recorded, and the next attempt tries a live worker.
                self.errors += 1
                time.sleep(0.05)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=15)
        for channel in self._channels.values():
            channel.close()


def _read(cluster: Cluster, key: bytes) -> kv_pb2.GetResponse:
    endpoints = cluster.live_worker_endpoints()
    assert endpoints, "no live worker to read from"
    last = None
    for endpoint in endpoints:
        channel = grpc.insecure_channel(endpoint.removeprefix("http://"))
        try:
            return kv_pb2_grpc.KvStub(channel).Get(
                kv_pb2.GetRequest(keyspace=KS, key=key), timeout=5.0
            )
        except grpc.RpcError as error:
            last = error
        finally:
            channel.close()
    raise AssertionError(f"no worker served a read of {key!r}: {last}")


def test_a_rolling_upgrade_preserves_writes_and_locks_out_the_old_binary(
    orbita_binary, previous_binary, tmp_path
):
    # Both versions come from the same parsed workspace version, so a bump to
    # 0.2.0-dev moves old to 0.1 and new to 0.2 without touching this test. A
    # hardcoded "0.0"/"0.1" would fail before exercising anything the day the
    # workspace minor moves, since the old side then bootstraps at the new
    # previous minor, not 0.0.
    old_version = harness.previous_cluster_version()
    new_version = harness.current_cluster_version()

    cluster = Cluster(new=orbita_binary, old=previous_binary, workdir=tmp_path)
    try:
        # A cluster bootstrapped entirely on the old binary comes up at the old
        # cluster version, which is the only starting point from which
        # finalize-upgrade has anything to do.
        cluster.bootstrap()
        cluster.wait_all_ready()
        assert cluster.cluster_version() == old_version, (
            f"the old binary bootstraps at {old_version}"
        )

        # The leader group creates the default keyspace as part of bootstrap, so
        # it is a control-log entry written while the cluster was at 0.0. Once
        # the workers are eligible the single partition gains an owner and the
        # keyspace is writable.
        assert KS in cluster.on_leader("keyspace", "list")
        cluster.wait_partition_owner()

        # A value written while the cluster is at 0.0. Its keyspace and its data
        # both predate every binary swap below, so reading it back at the end is
        # the observable proof that the new binary read the old control log
        # rather than truncating it (#24's one-way door).
        sentinel_key = b"sentinel/pre-upgrade"
        sentinel_value = f"written-at-{old_version}".encode()
        deadline = time.monotonic() + OWNER_TIMEOUT
        while True:
            endpoints = cluster.live_worker_endpoints()
            channel = grpc.insecure_channel(endpoints[0].removeprefix("http://"))
            try:
                kv_pb2_grpc.KvStub(channel).Set(
                    kv_pb2.SetRequest(keyspace=KS, key=sentinel_key, value=sentinel_value),
                    timeout=2.0,
                )
                break
            except grpc.RpcError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.25)
            finally:
                channel.close()

        writer = Writer(cluster)
        writer.start()
        try:
            # Give the writer a moment to accumulate acknowledged writes before
            # anything moves, so the no-loss claim covers real traffic.
            deadline = time.monotonic() + 15.0
            while len(writer.acknowledged) < 5 and time.monotonic() < deadline:
                time.sleep(0.1)
            assert writer.acknowledged, "the cluster never accepted a write before the roll"

            # Roll every node one at a time, workers first. The first replacement
            # is therefore a new worker against an entirely old leader group,
            # which is the documented worker-first upgrade order. Each step
            # waits for readiness before the next, so one replacement cannot
            # hide a crash loop or a failed compatibility handshake.
            for node in cluster.workers:
                node.roll(cluster.new)
                cluster.wait_ready(node)
                assert cluster.cluster_version() == old_version, (
                    "the cluster version must not move until finalize-upgrade"
                )

            for node in cluster.leaders:
                node.roll(cluster.new)
                cluster.wait_ready(node)

            # The old control log has now been
            # recovered by three new-binary leaders in turn. If any of them had
            # truncated it, the keyspace created above would be gone.
            assert KS in cluster.on_leader("keyspace", "list")

            before = len(writer.acknowledged)
            deadline = time.monotonic() + 15.0
            while len(writer.acknowledged) <= before and time.monotonic() < deadline:
                time.sleep(0.1)
            assert len(writer.acknowledged) > before, (
                "no write was acknowledged after the roll finished"
            )
        finally:
            writer.stop()

        # Merge used to be gated behind protocol 0.2 so that a 0.1 voter which
        # could not decode its tags stayed rollback-safe. ADR 0012 withdrew
        # that: nothing was released, so no such voter exists and merge belongs
        # to 0.1. What is asserted here now is that the operation is reachable
        # on this cluster rather than refused by the protocol, and that a
        # nonsense request is still refused on its own merits.
        # The cluster is still at 0.0 here: the roll has finished but nothing
        # has finalized. Merge is refused because no vocabulary has been agreed
        # at all, which is the case the gate still exists for, and the refusal
        # has to come before id validation so a malformed request cannot enter
        # merge logic and reach the log.
        refused = cluster.refused_by_leader("partition", "merge", "1", "2")
        assert "at least 0.1" in refused, (
            f"merge was not refused for want of an agreed protocol: {refused}"
        )
        # It no longer asks for finalization, because there is no later
        # protocol to finalize to. See ADR 0012.
        assert "finalize-upgrade" not in refused, (
            f"merge still asks for finalization: {refused}"
        )

        # Every acknowledged write is still readable. This is the no-lost-writes
        # guarantee stated as the client sees it.
        assert writer.acknowledged
        for key, value in writer.acknowledged:
            found = _read(cluster, key)
            assert found.found, f"{key!r} was acknowledged but is gone"
            assert found.value == value, f"{key!r} came back changed"

        # And the pre-upgrade sentinel survived every leader and worker swap.
        sentinel = _read(cluster, sentinel_key)
        assert sentinel.found and sentinel.value == sentinel_value, (
            f"a value written at {old_version} was lost across the upgrade"
        )

        # Now that every live node speaks the new version, finalize advances it.
        finalized = cluster.on_leader("cluster", "finalize-upgrade")
        assert f"advanced from {old_version} to {new_version}" in finalized, finalized
        assert cluster.cluster_version() == new_version

        # The old binary is now locked out. A node that speaks only the previous minor starts,
        # registers, is refused admission by the control plane, and stays not
        # ready with the version condition named. It must not silently join.
        rejoin_port = cluster.free_pair()
        rejoin = NodeProc(
            node_id=REJOIN_ID,
            role="worker",
            client_port=rejoin_port,
            peer_port=rejoin_port + 1,
            data_dir=tmp_path / "worker-rejoin",
            log_path=tmp_path / "worker-rejoin.log",
            binary=cluster.old,
            leader_peers=cluster.leader_peers,
        )
        cluster.extra.append(rejoin)
        rejoin.start(cluster.old)

        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            if rejoin.proc.poll() is not None:
                break
            if not cluster.is_ready(rejoin):
                conditions = cluster.ready_conditions(rejoin)
                if "cluster-version-compatible" in conditions:
                    break
            time.sleep(0.5)

        assert not cluster.is_ready(rejoin), (
            "the old binary must never report ready against the finalized cluster"
        )
        assert "cluster-version-compatible" in cluster.ready_conditions(rejoin), (
            "the refusal must name the version condition the old binary fails"
        )

        # The lockout is a control-plane decision: the refused node stays out of
        # the node list the leader reports, so it can own nothing.
        described = cluster.on_leader("cluster", "describe")
        assert re.search(rf"\n\s+{REJOIN_ID}\s+", described) is None, (
            f"the refused old node must not appear as a member:\n{described}"
        )
    finally:
        cluster.teardown()


def test_a_new_binary_worker_joining_an_old_leader_cluster_is_given_a_partition(
    orbita_binary, previous_binary, tmp_path
):
    """Growing a cluster mid-upgrade puts the new node to work.

    The test above replaces nodes in place, which is the path a StatefulSet
    rolling update takes and the only path issue #60 could cover. This is the
    other thing an operator does during an upgrade: add capacity while the
    leader group is still on the old binary. Issue #105 is that the new worker
    joined, reported healthy, and was never placed, with nothing anywhere
    saying why — the worst of the three available outcomes, because idle
    capacity that looks fine is capacity nobody goes looking for.

    The cluster starts one worker short of its replication factor so there is
    a placement genuinely waiting to be made. What the joining worker receives
    is therefore a decision the control plane had to take about it, not a
    partition it was handed at bootstrap.
    """
    old_version = harness.previous_cluster_version()

    cluster = Cluster(new=orbita_binary, old=previous_binary, workdir=tmp_path)
    try:
        cluster.bootstrap(worker_ids=(4, 5))
        cluster.wait_all_ready()
        assert cluster.cluster_version() == old_version, (
            f"the old binary bootstraps at {old_version}"
        )
        cluster.wait_partition_owner()

        before = cluster.holders()
        assert JOIN_ID not in before

        # A brand new worker on the new binary, joining a cluster whose leader
        # group is entirely old. Nothing about the cluster moves to meet it:
        # the active version stays where it was, which is exactly the state in
        # which the two binaries have to agree about what a heartbeat means.
        joined = cluster.join_worker(JOIN_ID, cluster.new)
        cluster.wait_ready(joined)
        assert cluster.cluster_version() == old_version, (
            "joining a newer worker must not move the cluster version; only "
            "finalize-upgrade does that"
        )

        deadline = time.monotonic() + OWNER_TIMEOUT
        while time.monotonic() < deadline:
            if JOIN_ID in cluster.holders():
                break
            time.sleep(0.5)
        else:
            raise AssertionError(
                "the joined worker never received a partition; it is in "
                f"membership and owns nothing:\n{json.dumps(cluster.describe(), indent=2)}"
            )
    finally:
        cluster.teardown()
