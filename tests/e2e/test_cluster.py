"""Leader-group formation through the shipped CLI and production sockets."""

from __future__ import annotations

import subprocess
import time

import grpc

import harness
from orbita.v1 import admin_pb2, admin_pb2_grpc


def _command(binary, node_id, client_port, peer_port, peers, data_dir):
    return [
        str(binary),
        "serve",
        "--node-id",
        str(node_id),
        "--role",
        "leader",
        "--listen",
        f"127.0.0.1:{client_port}",
        "--advertise",
        f"127.0.0.1:{client_port}",
        "--peer-listen",
        f"127.0.0.1:{peer_port}",
        "--peer-advertise",
        f"127.0.0.1:{peer_port}",
        "--data-dir",
        str(data_dir),
        "--leader-peers",
        peers,
    ]


def _wait_for_admin(process, stub, log_path):
    deadline = time.monotonic() + harness.STARTUP_TIMEOUT_SECONDS
    last_error = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError(
                f"leader exited with {process.returncode}:\n"
                f"{log_path.read_text(errors='replace')}"
            )
        try:
            stub.ListKeyspaces(admin_pb2.ListKeyspacesRequest(), timeout=2)
            return
        except grpc.RpcError as error:
            last_error = error
            time.sleep(0.05)
    raise AssertionError(f"leader did not become ready: {last_error}")


def _stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=15)


def _disjoint_port_pairs(count):
    ports = []
    occupied = set()
    while len(ports) < count:
        port = harness.free_port()
        if port in occupied or port + 1 in occupied:
            continue
        ports.append(port)
        occupied.update((port, port + 1))
    return ports


def test_two_fixed_voters_form_replicate_and_restart_with_one_peer_unavailable(
    orbita_binary, tmp_path
):
    ports = _disjoint_port_pairs(3)
    peers = ",".join(
        f"{node_id}=127.0.0.1:{port + 1}"
        for node_id, port in enumerate(ports, start=1)
    )
    commands = [
        _command(
            orbita_binary,
            node_id,
            ports[node_id - 1],
            ports[node_id - 1] + 1,
            peers,
            tmp_path / f"leader-{node_id}",
        )
        for node_id in (1, 2)
    ]
    log_paths = [tmp_path / "leader-1.log", tmp_path / "leader-2.log"]
    logs = [path.open("ab") for path in log_paths]
    processes = [
        subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
        for command, log in zip(commands, logs, strict=True)
    ]
    channels = [grpc.insecure_channel(f"127.0.0.1:{port}") for port in ports[:2]]
    stubs = [admin_pb2_grpc.AdminStub(channel) for channel in channels]

    try:
        for process, stub, path in zip(processes, stubs, log_paths, strict=True):
            _wait_for_admin(process, stub, path)

        for stub in stubs:
            try:
                stub.CreateKeyspace(
                    admin_pb2.CreateKeyspaceRequest(name="replicated"), timeout=3
                )
                break
            except grpc.RpcError as error:
                assert error.code() == grpc.StatusCode.UNAVAILABLE
        else:
            raise AssertionError("neither configured voter accepted the proposal")

        def replicated_everywhere():
            return all(
                "replicated"
                in {
                    keyspace.name
                    for keyspace in stub.ListKeyspaces(
                        admin_pb2.ListKeyspacesRequest(), timeout=2
                    ).keyspaces
                }
                for stub in stubs
            )

        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and not replicated_everywhere():
            time.sleep(0.05)
        assert replicated_everywhere()

        _stop(processes[1])
        processes[1] = subprocess.Popen(
            commands[1], stdout=logs[1], stderr=subprocess.STDOUT
        )
        _wait_for_admin(processes[1], stubs[1], log_paths[1])
        names = {
            keyspace.name
            for keyspace in stubs[1]
            .ListKeyspaces(admin_pb2.ListKeyspacesRequest(), timeout=2)
            .keyspaces
        }
        assert "replicated" in names
    finally:
        for channel in channels:
            channel.close()
        for process in processes:
            _stop(process)
        for log in logs:
            log.close()
