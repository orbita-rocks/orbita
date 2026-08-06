"""Process and stub plumbing for the end-to-end suite.

The suite exists to check the wire contract from outside the Rust build, so
everything here goes through the same path an ordinary user would: the shipped
binary started as a subprocess, and stubs generated from the checked-in .proto
files by the stock protoc that ships inside grpcio-tools. Nothing imports from
the server, and nothing is hand-written that a code generator could produce.
"""

from __future__ import annotations

import os
import random
import re
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import grpc

REPO_ROOT = Path(__file__).resolve().parents[2]
PROTO_DIR = REPO_ROOT / "proto"
GENERATED_DIR = Path(__file__).resolve().parent / ".generated"

# The keyspace `orbita dev` creates on startup, so that a test needing one key
# does not need an admin call first. The Admin service can create more; see
# test_admin.py.
DEFAULT_KEYSPACE = "default"

# How long we are willing to wait for a node to answer its first request. A
# cold start on a loaded CI runner is the slow case.
STARTUP_TIMEOUT_SECONDS = 60.0


def generate_stubs() -> Path:
    """Regenerate the Python stubs from /proto and return their root.

    This runs on every session rather than being committed. A committed stub
    would only prove that the protos compiled on the day someone ran the
    generator; regenerating proves they still compile with a toolchain that
    knows nothing about tonic or prost, which is half the point of this suite.
    """
    if GENERATED_DIR.exists():
        shutil.rmtree(GENERATED_DIR)
    GENERATED_DIR.mkdir(parents=True)

    protos = sorted(str(p) for p in PROTO_DIR.rglob("*.proto"))
    if not protos:
        raise RuntimeError(f"no .proto files under {PROTO_DIR}")

    # grpc_tools carries its own protoc, so a developer needs no system protoc
    # to run the suite.
    command = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"--proto_path={PROTO_DIR}",
        f"--python_out={GENERATED_DIR}",
        f"--pyi_out={GENERATED_DIR}",
        f"--grpc_python_out={GENERATED_DIR}",
        *protos,
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            "protoc failed on the checked-in protos:\n"
            f"{result.stdout}\n{result.stderr}"
        )
    return GENERATED_DIR


def build_binary() -> Path:
    """Return the path to the orbita binary, building it if it is not there.

    Set ORBITA_BINARY to skip the build, which is what CI does after its own
    cargo step so the test run does not pay for a second one.
    """
    override = os.environ.get("ORBITA_BINARY")
    if override:
        path = Path(override)
        if not path.exists():
            raise RuntimeError(f"ORBITA_BINARY points at {path}, which does not exist")
        return path

    path = REPO_ROOT / "target" / "debug" / "orbita"
    if not path.exists():
        subprocess.run(["cargo", "build", "--bin", "orbita"], cwd=REPO_ROOT, check=True)
    if not path.exists():
        raise RuntimeError(f"cargo build did not produce {path}")
    return path


class PreviousBinaryUnavailable(RuntimeError):
    """Raised when the prior-minor binary cannot be obtained.

    Carries a human sentence the caller turns into a pytest skip, because the
    honest reason ("this repo has never cut a release, so there is no prior
    binary to download") is exactly what a reader of a skipped upgrade test
    needs to see.
    """


def _workspace_version() -> tuple[int, int]:
    """The (major, minor) of the workspace, parsed from the root Cargo.toml.

    The cluster version a binary speaks is derived from the same crate version
    at compile time (`orbita-control::version`), so the previous *minor* is the
    only thing that produces a binary with a different, older speakable range.
    """
    text = (REPO_ROOT / "Cargo.toml").read_text()
    match = re.search(r'^version\s*=\s*"(\d+)\.(\d+)\.', text, re.MULTILINE)
    if not match:
        raise PreviousBinaryUnavailable(
            "could not parse the workspace version out of Cargo.toml"
        )
    return int(match.group(1)), int(match.group(2))


def previous_binary() -> Path:
    """A binary one *minor* older than this one, for the two-binary upgrade test.

    Resolution, in order:

    1. ``ORBITA_PREV_BINARY`` — an explicit path, which is what CI or a
       developer with a real prior release on disk should point at.
    2. A synthesized build: the committed source at HEAD, stamped down to the
       previous minor and compiled. This exists because the repo has cut no
       releases and pushed no tags, so there is nothing to `gh release
       download`; the cluster-version machinery keys off the crate version, so
       the same source at ``0.(minor-1).0`` is a faithful "speaks only the
       previous version" peer for the compatibility gate under test.

    Raises :class:`PreviousBinaryUnavailable` with a specific reason when
    neither path yields a binary, so the test skips loudly rather than lying.
    """
    override = os.environ.get("ORBITA_PREV_BINARY")
    if override:
        path = Path(override)
        if not path.exists():
            raise PreviousBinaryUnavailable(
                f"ORBITA_PREV_BINARY points at {path}, which does not exist"
            )
        return path

    major, minor = _workspace_version()
    if minor == 0:
        # A binary at minor zero speaks only its own version, so there is no
        # older minor to build a distinct peer from. Naming it would be a lie.
        raise PreviousBinaryUnavailable(
            f"the workspace is at {major}.{minor}, whose previous minor is not "
            "expressible; there is no older cluster version to roll from"
        )
    prev = f"{major}.{minor - 1}.0"

    cache = Path(os.environ.get("TMPDIR", "/tmp")) / "orbita-e2e-prev" / prev
    cached = cache / "orbita"
    if cached.exists():
        return cached

    return _build_previous_binary(prev, cache)


def _build_previous_binary(prev: str, cache: Path) -> Path:
    """Compile the HEAD tree stamped down to version ``prev`` and cache it.

    The build happens in an exported copy of the committed tree rather than in
    place, so nothing edits the working Cargo.toml or clobbers the current
    binary in ``target/debug``. It reuses the machine's cargo caches, so in
    practice only the orbita crates recompile.
    """
    if shutil.which("cargo") is None:
        raise PreviousBinaryUnavailable("cargo is not on PATH, so no prior binary can be built")

    src = cache.parent / f"{prev}-src"
    if src.exists():
        shutil.rmtree(src)
    src.mkdir(parents=True)

    export = subprocess.run(
        ["git", "archive", "HEAD"],
        cwd=REPO_ROOT,
        capture_output=True,
    )
    if export.returncode != 0:
        raise PreviousBinaryUnavailable(
            f"git archive HEAD failed: {export.stderr.decode(errors='replace')}"
        )
    unpack = subprocess.run(["tar", "-x", "-C", str(src)], input=export.stdout, capture_output=True)
    if unpack.returncode != 0:
        raise PreviousBinaryUnavailable(
            f"unpacking the HEAD tree failed: {unpack.stderr.decode(errors='replace')}"
        )

    manifest = src / "Cargo.toml"
    stamped = re.sub(
        r'^(version\s*=\s*)"[^"]+"',
        rf'\1"{prev}"',
        manifest.read_text(),
        count=1,
        flags=re.MULTILINE,
    )
    manifest.write_text(stamped)

    build = subprocess.run(
        ["cargo", "build", "--bin", "orbita", "--manifest-path", str(manifest)],
        capture_output=True,
        text=True,
    )
    if build.returncode != 0:
        raise PreviousBinaryUnavailable(
            "building the previous-minor binary failed:\n"
            f"{build.stdout}\n{build.stderr}"
        )

    built = src / "target" / "debug" / "orbita"
    if not built.exists():
        raise PreviousBinaryUnavailable(f"the previous build did not produce {built}")

    cache.mkdir(parents=True, exist_ok=True)
    shutil.copy2(built, cache / "orbita")
    (cache / "orbita").chmod(0o755)
    return cache / "orbita"


# Where this suite looks for ports, chosen to sit below the range every
# operating system hands out for the source port of an outbound connection:
# 49152 upwards on macOS, 32768 upwards on Linux. That matters because this
# suite is a client as well as a server. Asking the kernel for a free port with
# bind(0) draws from the ephemeral range, and every reconnection attempt the
# gRPC channel makes while a node is still starting draws from it too, so the
# port reserved a moment ago for the node's second listener could be taken by
# the test's own socket before the node got to it. That failed as
# "Address already in use" in perhaps one run in four.
PORT_RANGE = (20000, 30000)


def free_port() -> int:
    """Pick a port where this port and the next one are both free.

    A node binds two listeners, one for clients and one for peers, and the peer
    one takes the port above the client one. Checking only the first left the
    suite able to start a node whose second listener collided with something
    else, which failed as an unexplained startup timeout rather than as a port
    conflict.

    There is still a window between closing these sockets and the node binding
    them. What [`PORT_RANGE`] removes is the part of that window this suite
    causes itself; a collision with something else on the machine is still
    possible and is why this retries.
    """
    for _ in range(200):
        port = random.randrange(PORT_RANGE[0], PORT_RANGE[1], 2)
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as first:
                first.bind(("127.0.0.1", port))
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as second:
                    second.bind(("127.0.0.1", port + 1))
        except OSError:
            continue
        return port
    raise RuntimeError("could not find two consecutive free ports")


class Node:
    """One `orbita dev` process and a client connected to it.

    The data directory outlives any single process so that a test can stop the
    node and start it again over the same state, which is the only way to check
    durability from outside.
    """

    def __init__(self, binary: Path, data_dir: Path, pb2_grpc, log_path: Path):
        self._binary = binary
        self._pb2_grpc = pb2_grpc
        self.data_dir = data_dir
        self._log_path = log_path
        self._process: subprocess.Popen | None = None
        self._channel: grpc.Channel | None = None
        self._log = None
        self.port = 0
        self.kv = None
        self.admin = None
        self.limits = None

    def start(self, probe_factory) -> None:
        """Start the process and wait until it actually answers a request.

        Waiting on a real request rather than on a TCP connect matters because
        the listener comes up before storage has opened and the default keyspace
        exists. A fixed sleep would either be slow or flaky, and on a busy CI
        runner it is both.
        """
        # The peer listener takes the port above the client one, so leave a gap.
        self.port = free_port()
        self._log = self._log_path.open("ab")
        self._process = subprocess.Popen(
            [
                str(self._binary),
                "dev",
                "--port",
                str(self.port),
                "--data-dir",
                str(self.data_dir),
                "--keyspace",
                DEFAULT_KEYSPACE,
            ],
            cwd=REPO_ROOT,
            stdout=self._log,
            stderr=subprocess.STDOUT,
        )

        # Connect twice on purpose, because a channel's maximum message size is
        # fixed when it is created and the server is what knows the right one.
        # So: a small channel to ask, then the real one sized from the answer.
        # This is what a client library should do at startup, and doing it here
        # means the suite is the worked example rather than a special case.
        self._channel = self._connect()
        self._bind_stubs()

        deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            if self._process.poll() is not None:
                raise RuntimeError(
                    f"orbita dev exited with {self._process.returncode} during "
                    f"startup. Log:\n{self.log()}"
                )
            try:
                # This doubles as the readiness probe. It names the keyspace,
                # so it fails until the keyspace exists, which is the thing a
                # test needs to be true before it starts. It also touches no
                # partition, so it says the node is answering without saying
                # anything about whether a particular key can be served.
                self.limits = self.kv.GetLimits(probe_factory(), timeout=2.0)
                break
            except grpc.RpcError as error:  # noqa: PERF203
                last_error = error
                time.sleep(0.05)
        else:
            raise RuntimeError(
                f"orbita dev never served a request on port {self.port}: {last_error}\n"
                f"Log:\n{self.log()}"
            )

        # Reconnect at the size the server reported. A client that skipped this
        # would work until somebody stored a value larger than the 4MB default,
        # and then fail inside gRPC with an error about message size that says
        # nothing about Orbita.
        self._channel.close()
        self._channel = self._connect(self.limits.max_message_bytes)
        self._bind_stubs()

    def _connect(self, max_message_bytes: int | None = None):
        """Opens a channel, optionally sized for this cluster's limits."""
        # gRPC waits a second before retrying a refused connection by default,
        # which would put a second on every test in the suite while the node is
        # still opening its storage. Shorten it, since the server is local and
        # about to come up.
        options = [
            ("grpc.initial_reconnect_backoff_ms", 20),
            ("grpc.min_reconnect_backoff_ms", 20),
            ("grpc.max_reconnect_backoff_ms", 200),
        ]
        if max_message_bytes is not None:
            options += [
                ("grpc.max_receive_message_length", max_message_bytes),
                ("grpc.max_send_message_length", max_message_bytes),
            ]
        return grpc.insecure_channel(f"127.0.0.1:{self.port}", options=options)

    def _bind_stubs(self) -> None:
        self.kv = self._pb2_grpc.kv.KvStub(self._channel)
        self.admin = self._pb2_grpc.admin.AdminStub(self._channel)

    def stop(self) -> None:
        """Stop the process and close the channel, tolerating either being gone.

        Teardown runs after failures too, so every step here has to be safe on
        a half-started node.
        """
        if self._channel is not None:
            self._channel.close()
            self._channel = None
        self.kv = None
        self.admin = None
        if self._process is not None:
            if self._process.poll() is None:
                self._process.terminate()
                try:
                    self._process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    self._process.kill()
                    self._process.wait(timeout=15)
            self._process = None
        if self._log is not None:
            self._log.close()
            self._log = None

    def restart(self, probe_factory) -> None:
        self.stop()
        self.start(probe_factory)

    def log(self) -> str:
        if not self._log_path.exists():
            return ""
        return self._log_path.read_text(errors="replace")
