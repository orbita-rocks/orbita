"""Process and stub plumbing for the end-to-end suite.

The suite exists to check the wire contract from outside the Rust build, so
everything here goes through the same path an ordinary user would: the shipped
binary started as a subprocess, and stubs generated from the checked-in .proto
files by the stock protoc that ships inside grpcio-tools. Nothing imports from
the server, and nothing is hand-written that a code generator could produce.
"""

from __future__ import annotations

import os
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

# The keyspace `orbita dev` creates on startup. Nothing else can create one,
# because the Admin service is not implemented yet.
DEFAULT_KEYSPACE = "default"

# How long we are willing to wait for a node to answer its first request. A
# cold RocksDB open on a loaded CI runner is the slow case.
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


def free_port() -> int:
    """Pick a port the kernel says is free.

    There is a window between closing this socket and the node binding it, but
    the alternative is a fixed port that collides with a developer's own node.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


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

    def start(self, get_request_factory) -> None:
        """Start the process and wait until it actually answers a request.

        Waiting on a real Get rather than on a TCP connect matters because the
        listener comes up before RocksDB has opened and the default keyspace
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

        # gRPC waits a second before retrying a refused connection by default,
        # which would put a second on every test in the suite while the node is
        # still opening its storage. Shorten it, since the server is local and
        # about to come up.
        self._channel = grpc.insecure_channel(
            f"127.0.0.1:{self.port}",
            options=[
                ("grpc.initial_reconnect_backoff_ms", 20),
                ("grpc.min_reconnect_backoff_ms", 20),
                ("grpc.max_reconnect_backoff_ms", 200),
            ],
        )
        self.kv = self._pb2_grpc.kv.KvStub(self._channel)
        self.admin = self._pb2_grpc.admin.AdminStub(self._channel)

        deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            if self._process.poll() is not None:
                raise RuntimeError(
                    f"orbita dev exited with {self._process.returncode} during "
                    f"startup. Log:\n{self.log()}"
                )
            try:
                self.kv.Get(get_request_factory(), timeout=2.0)
                return
            except grpc.RpcError as error:  # noqa: PERF203
                last_error = error
                time.sleep(0.05)
        raise RuntimeError(
            f"orbita dev never served a request on port {self.port}: {last_error}\n"
            f"Log:\n{self.log()}"
        )

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

    def restart(self, get_request_factory) -> None:
        self.stop()
        self.start(get_request_factory)

    def log(self) -> str:
        if not self._log_path.exists():
            return ""
        return self._log_path.read_text(errors="replace")
