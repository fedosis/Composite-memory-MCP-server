"""Shared helpers for native backend unit tests (slice T3)."""

from __future__ import annotations

import asyncio
import atexit
import concurrent.futures
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from memory_server.native.protocol import HEADER, encode_frame
from memory_server.native.schema_v1 import schema_hash
from memory_server.transports.hermes import _get_loop

_E2E_ENDPOINTS: dict[str, tuple[str, Path]] = {}
_E2E_PROCESSES: dict[str, subprocess.Popen] = {}


def run_on_backend_loop(coro, *, timeout: float = 10.0):
    """Run a coroutine on the Hermes owner loop without cancelling on timeout.

    Sync equivalent of ``NativeBackendAdapter._on_backend_loop``: a timeout
    stops waiting for the result but never cancels the backend coroutine
    (``concurrent.futures.Future.result`` never cancels the submitted task,
    unlike ``asyncio.wait_for`` on the ``wrap_future`` wrapper).
    """
    fut = asyncio.run_coroutine_threadsafe(coro, _get_loop())
    try:
        return fut.result(timeout=timeout)
    except concurrent.futures.TimeoutError as exc:
        raise TimeoutError from exc


def make_service_config(tmp_path: Path, **overrides: Any) -> Path:
    """Write a minimal valid service JSON and return its path."""
    root = tmp_path / "store"
    root.mkdir(parents=True, exist_ok=True)
    context = tmp_path / "context"
    context.mkdir(parents=True, exist_ok=True)
    # The AF_UNIX socket path is capped at 107 bytes; a socket below tmp_path
    # would exceed that. Use a short private /tmp directory (already 0700).
    socket_dir = Path(tempfile.mkdtemp(prefix="cmmst3-", dir="/tmp"))
    atexit.register(shutil.rmtree, socket_dir, ignore_errors=True)
    homes = tmp_path / "homes"
    homes.mkdir(parents=True, exist_ok=True)
    (homes / "hermes").mkdir(exist_ok=True)

    document: dict[str, Any] = {
        "version": 1,
        "data_root": str(root),
        "storage_context_home": str(context),
        "socket_path": str(socket_dir / "memory.sock"),
        "allow_homes": [str(homes / "hermes")],
        "backend": {
            "storage_mode": "shared",
            "db_url": f"sqlite+aiosqlite:///{root / 'memory.db'}",
            "vector_backend": "lancedb",
            "lancedb_path": str(root / "lancedb"),
            "graph_snapshot_path": str(root / "graph.json"),
            "vector_collection": "memories",
            "extraction_mode": "llm",
            "llm_model": "deepseek-v4-flash",
            "llm_base_url": "https://api.deepseek.com/v1",
            "max_facts": 5,
            "writer": {"flush_interval": 5.0, "max_batch": 50},
        },
    }
    document.update(overrides)

    path = tmp_path / "service.json"
    path.write_text(json.dumps(document, indent=2), encoding="utf-8")
    os.chmod(path, 0o600)
    return path


def start_service_subprocess(config_path: Path, *, env=None, bootstrap: str | None = None) -> subprocess.Popen:
    """Spawn the actual native CLI, with an isolated environment."""
    document = json.loads(Path(config_path).read_text(encoding="utf-8"))
    socket_path = document["socket_path"]
    _E2E_ENDPOINTS[socket_path] = (document["store_id"], Path(document["allow_homes"][0]))
    child_env = {key: value for key, value in os.environ.items()
                 if not key.startswith("MEMORY_SERVER_") and key != "HERMES_HOME"}
    child_env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
    if env:
        child_env.update(env)
    if bootstrap is not None:
        # Python imports sitecustomize before -m; only this throwaway child
        # gets the delay seam, while the launched CLI remains unchanged.
        bootstrap_dir = Path(config_path).parent / "native-e2e-bootstrap"
        bootstrap_dir.mkdir(exist_ok=True)
        (bootstrap_dir / "sitecustomize.py").write_text(bootstrap, encoding="utf-8")
        child_env["PYTHONPATH"] = f"{bootstrap_dir}{os.pathsep}{child_env['PYTHONPATH']}"
    with (Path(config_path).parent / "service.stderr").open("ab") as diagnostics:
        process = subprocess.Popen(
            [sys.executable, "-m", "memory_server.native.cli", "serve", "--config", str(config_path)],
            env=child_env, stdout=subprocess.DEVNULL, stderr=diagnostics,
        )
    _E2E_PROCESSES[socket_path] = process
    return process


def _recv_exact(sock: socket.socket, count: int) -> bytes:
    result = bytearray()
    while len(result) < count:
        part = sock.recv(count - len(result))
        if not part:
            raise EOFError("socket closed mid-frame")
        result.extend(part)
    return bytes(result)


def _read_frame(sock: socket.socket) -> dict:
    length = HEADER.unpack(_recv_exact(sock, HEADER.size))[0]
    if not 0 < length <= 4 * 1024 * 1024:
        raise ValueError(f"invalid frame length: {length}")
    return json.loads(_recv_exact(sock, length))


def hello(socket_path: Path, store_id: str, home: Path) -> dict:
    """Perform a raw hello; also useful for negative admission checks."""
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(2)
        sock.connect(str(socket_path))
        sock.sendall(encode_frame({
            "v": 1, "op": "hello", "correlation_id": uuid.uuid4().hex,
            "payload": {"schema_hash": schema_hash(), "store_id": store_id, "claimed_home": str(home)},
        }))
        return _read_frame(sock)


def send_op(socket_path: Path, op: str, payload: dict, *, sid: str = "") -> dict:
    """Send one operation after a real hello on the same connection."""
    store_id, home = _E2E_ENDPOINTS[str(socket_path)]
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(5)
        sock.connect(str(socket_path))
        sock.sendall(encode_frame({
            "v": 1, "op": "hello", "correlation_id": uuid.uuid4().hex,
            "payload": {"schema_hash": schema_hash(), "store_id": store_id, "claimed_home": str(home)},
        }))
        greeting = _read_frame(sock)
        if not greeting.get("ok"):
            return greeting
        sock.sendall(encode_frame({
            "v": 1, "op": op, "correlation_id": uuid.uuid4().hex,
            "session_id": sid, "payload": payload,
        }))
        return _read_frame(sock)


def wait_ready(socket_path: Path, *, timeout: float = 15) -> dict:
    """Poll the actual hello response, not merely the pathname."""
    store_id, home = _E2E_ENDPOINTS[str(socket_path)]
    deadline = time.monotonic() + timeout
    last: object = None
    while time.monotonic() < deadline:
        try:
            reply = hello(socket_path, store_id, home)
            if reply.get("state") == "READY":
                return reply
            last = reply
        except (OSError, EOFError, ValueError) as exc:
            last = exc
        process = _E2E_PROCESSES.get(str(socket_path))
        if process is not None and process.poll() is not None:
            raise AssertionError(f"owner exited {process.returncode} before READY: {last!r}")
        time.sleep(0.05)
    diagnostics = home.parent / "service.stderr"
    detail = diagnostics.read_text() if diagnostics.exists() else ""
    raise AssertionError(f"service did not become READY: {last!r}; stderr={detail}")


def make_plugin_home(tmp_path: Path, socket_path: Path, store_id: str) -> Path:
    from memory_server.native.install import CLIENT_CONFIG_NAME

    home = tmp_path / "hermes"
    home.mkdir(parents=True, exist_ok=True)
    sidecar = home / CLIENT_CONFIG_NAME
    sidecar.write_text(json.dumps({
        "version": 1, "socket_path": str(socket_path), "expected_store_id": store_id,
    }), encoding="utf-8")
    os.chmod(sidecar, 0o600)
    return home


class CutAfterWriteProxy:
    """AF_UNIX interposer: forward hello, then drop the mutation's response.

    The client is closed immediately after the last request byte reaches the
    owner socket; the owner connection stays open until it finishes the op.
    """

    def __init__(self, path: Path, target: Path):
        self.path = Path(path)
        self.target = Path(target)
        self.forwarded = threading.Event()
        self.settled = threading.Event()
        self.errors: list[Exception] = []
        self._listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._listener.bind(str(path))
        self._listener.listen(1)
        self._listener.settimeout(5)
        self._thread = threading.Thread(target=self._serve, daemon=True)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *_exc):
        self._listener.close()
        self._thread.join(timeout=7)
        self.path.unlink(missing_ok=True)
        assert not self._thread.is_alive(), "interposer thread leaked"
        if self.errors:
            raise AssertionError(f"interposer failed: {self.errors!r}")

    def _serve(self):
        try:
            while not self.forwarded.is_set():
                client, _ = self._listener.accept()
                with client, socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as owner:
                    client.settimeout(5)
                    owner.settimeout(5)
                    owner.connect(str(self.target))
                    greeting = _read_frame(client)
                    owner.sendall(encode_frame(greeting))
                    client.sendall(encode_frame(_read_frame(owner)))
                    try:
                        mutation = _read_frame(client)
                    except EOFError:
                        # Provider.initialize() probes hello without an operation.
                        continue
                    assert mutation["op"] in ("enqueue_turn", "tool_call")
                    owner.sendall(encode_frame(mutation))
                    self.forwarded.set()
                    client.shutdown(socket.SHUT_RDWR)
                    client.close()
                    self.response = _read_frame(owner)
                    self.settled.set()
        except Exception as exc:
            self.errors.append(exc)
            self.settled.set()
