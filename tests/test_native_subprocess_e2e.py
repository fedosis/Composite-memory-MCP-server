"""Isolated real-process native owner and standalone plugin contract (T7)."""
from __future__ import annotations

import asyncio
import fcntl
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path

import pytest

from memory_server.native.backend import NativeBackendAdapter
from memory_server.native.config import load_service_config
from memory_server.native.protocol import HEADER, MAX_FRAME_BYTES
from tests._native_helpers import (
    CutAfterWriteProxy,
    _read_frame,
    hello,
    make_plugin_home,
    send_op,
    start_service_subprocess,
    wait_ready,
)

PLUGIN_DIR = Path(__file__).resolve().parents[1] / "plugin-dist"
# The standalone artifact implements Hermes' MemoryProvider interface.
for plugin_path in (str(PLUGIN_DIR), "/home/shtorm/.hermes/hermes-agent"):
    if plugin_path not in sys.path:
        sys.path.insert(0, plugin_path)
from memory_server_proxy.provider import MemoryServerProxyProvider  # noqa: E402


def _stop(proc: subprocess.Popen) -> int:
    if proc.poll() is None:
        proc.terminate()
    try:
        return proc.wait(timeout=20)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)
        raise AssertionError("native service did not stop within 20 seconds")


@pytest.fixture(autouse=True)
def e2e_deadline():
    """Hard per-case deadline, including fixture teardown and blocking plugin calls."""
    def expired(_signum, _frame):
        raise TimeoutError("native E2E exceeded 60 seconds")

    previous = signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, 59)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


@pytest.fixture
def sandbox(tmp_path):
    socket_dir = Path(tempfile.mkdtemp(prefix="cmms-e2e-", dir="/tmp"))
    processes: list[subprocess.Popen] = []
    root = tmp_path / "store"
    context = tmp_path / "context"
    context.mkdir()
    home = tmp_path / "hermes"
    home.mkdir()
    sock = socket_dir / "native.sock"
    store_id = str(uuid.uuid4())
    other_home = make_plugin_home(tmp_path / "client-b", sock, store_id)
    document = {
        "version": 1, "store_id": store_id, "data_root": str(root),
        "storage_context_home": str(context), "socket_path": str(sock),
        "allow_homes": [str(home), str(other_home)],
        "backend": {
            "storage_mode": "shared", "db_url": f"sqlite+aiosqlite:///{root / 'memory.db'}",
            "vector_backend": "lancedb", "lancedb_path": str(root / "lancedb"),
            "graph_snapshot_path": str(root / "graph.json"),
            "extraction_mode": "none", "llm_model": None, "max_facts": 5,
            "writer": {"flush_interval": 0.2, "max_batch": 10},
        },
        "limits": {"connect_timeout_s": 2, "pre_drain_s": 3},
    }
    cfg = tmp_path / "service.json"
    cfg.write_text(json.dumps(document), encoding="utf-8")
    cfg.chmod(0o600)
    make_plugin_home(tmp_path, sock, store_id)

    def spawn(*, bootstrap=None):
        proc = start_service_subprocess(cfg, bootstrap=bootstrap)
        processes.append(proc)
        return proc

    try:
        yield root, home, sock, store_id, cfg, spawn, processes
    finally:
        for proc in reversed(processes):
            _stop(proc)
        shutil.rmtree(socket_dir)


def _provider(home: Path, sid: str = "session") -> MemoryServerProxyProvider:
    provider = MemoryServerProxyProvider()
    provider.initialize(sid, hermes_home=str(home))
    assert provider.is_available()
    return provider


def _remember(provider: MemoryServerProxyProvider, marker: str) -> None:
    result = json.loads(provider.handle_tool_call("remember", {
        "subject": marker, "predicate": "is", "object": "durable",
    }))
    assert result["fact"]["subject"] == marker, result


def _search(provider: MemoryServerProxyProvider, marker: str) -> list:
    result = json.loads(provider.handle_tool_call("search", {"query": marker}))
    assert "results" in result, result
    matches = [item for item in result["results"] if marker in json.dumps(item)]
    assert matches, result
    return matches


def test_start_status_stop(sandbox):
    root, home, sock, store_id, cfg, spawn, _ = sandbox
    proc = spawn()
    ready = wait_ready(sock)
    assert ready["store_id"] == store_id and ready["epoch"]
    status = send_op(sock, "status", {})
    assert status["ok"] and status["state"] == "READY" and status["epoch"] == ready["epoch"]
    command = subprocess.run([sys.executable, "-m", "memory_server.native.cli", "status",
                              "--config", str(cfg)], capture_output=True, timeout=5)
    assert command.returncode == 0, command.stderr.decode()
    assert json.loads(command.stdout)["epoch"] == ready["epoch"]
    assert proc.poll() is None
    assert _stop(proc) == 0
    assert not sock.exists()
    with (root / ".cmms-storage.lock").open("rb") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(lock, fcntl.LOCK_UN)


def test_two_clients_share_store(sandbox):
    _, home, sock, _, _, spawn, _ = sandbox
    spawn()
    wait_ready(sock)
    a, b = _provider(home, "A"), _provider(home.parent / "client-b" / "hermes", "B")
    marker = f"shared-{uuid.uuid4().hex}"
    _remember(a, marker)
    assert send_op(sock, "flush_observations", {})["ok"]
    assert len(_search(b, marker)) == 1


def test_client_close_does_not_stop_owner(sandbox):
    _, home, sock, _, _, spawn, _ = sandbox
    proc = spawn()
    wait_ready(sock)
    a, b = _provider(home, "A"), _provider(home.parent / "client-b" / "hermes", "B")
    a.shutdown()
    assert send_op(sock, "status", {})["state"] == "READY"
    assert json.loads(b.handle_tool_call("ping", {}))["status"] == "ok"
    assert proc.poll() is None


def test_second_owner_conflict(sandbox):
    _, home, sock, _, cfg, spawn, _ = sandbox
    first = spawn()
    wait_ready(sock)
    second = spawn()
    assert second.wait(timeout=15) == 5
    assert first.poll() is None
    assert json.loads(_provider(home).handle_tool_call("ping", {}))["status"] == "ok"


def test_committed_survives_clean_restart(sandbox):
    _, home, sock, _, _, spawn, _ = sandbox
    first = spawn()
    wait_ready(sock)
    marker = f"restart-{uuid.uuid4().hex}"
    _remember(_provider(home), marker)
    assert _stop(first) == 0
    spawn()
    wait_ready(sock)
    assert len(_search(_provider(home), marker)) == 1


def test_tool_surface_offline(sandbox):
    _, home, _, _, _, _, _ = sandbox
    # A configured sidecar with no running owner still exposes all 15 tools.
    provider = _provider(home)
    assert len(provider.get_tool_schemas()) == 15
    assert json.loads(provider.handle_tool_call("ping", {}))["code"] == "E_UNAVAILABLE"


def test_wrong_store_id_rejected(sandbox):
    _, home, sock, _, _, spawn, _ = sandbox
    spawn()
    wait_ready(sock)
    wrong_home = home.parent / "wrong"
    make_plugin_home(wrong_home, sock, str(uuid.uuid4()))
    result = json.loads(_provider(wrong_home / "hermes").handle_tool_call("ping", {}))
    assert result["code"] == "E_UNAVAILABLE"
    # Raw handshake proves the service, not just the plugin, rejected the ID.
    assert hello(sock, str(uuid.uuid4()), home)["code"] == "E_STORE_ID"


def test_home_not_allowed(sandbox):
    _, _, sock, store_id, _, spawn, _ = sandbox
    spawn()
    wait_ready(sock)
    assert hello(sock, store_id, sock.parent)["code"] == "E_PROFILE_DENIED"


def test_oversized_frame(sandbox):
    _, _, sock, _, _, spawn, _ = sandbox
    spawn()
    wait_ready(sock)
    with socket.socket(socket.AF_UNIX) as client:
        client.settimeout(3)
        client.connect(str(sock))
        client.sendall(HEADER.pack(MAX_FRAME_BYTES + 1))
        # Over-cap request: the server must close without any response frame
        # (amplification guard, §5.2 p.1) — never synthesize a server code.
        with pytest.raises((EOFError, ConnectionResetError)):
            _read_frame(client)
    # The framing rejection is counted and observable via status.rejected.
    status = send_op(sock, "status", {})
    assert status["rejected"] >= 1


def test_connection_flood_limit(sandbox):
    _, _, sock, _, _, spawn, _ = sandbox
    spawn()
    wait_ready(sock)
    held = []
    try:
        for _ in range(16):
            client = socket.socket(socket.AF_UNIX)
            client.settimeout(3)
            client.connect(str(sock))
            held.append(client)
        # Wait for the listener to admit all 16 (without sending hello).
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            status = send_op(sock, "status", {})
            if status.get("code") == "E_CONNECTION_LIMIT":
                break
            time.sleep(0.02)
        else:
            pytest.fail("16 idle clients did not exhaust the connection limit")
        with socket.socket(socket.AF_UNIX) as extra:
            extra.settimeout(3)
            extra.connect(str(sock))
            try:
                assert _read_frame(extra)["code"] == "E_CONNECTION_LIMIT"
            except EOFError:
                pass
    finally:
        for client in held:
            client.close()


def test_flush_after_enqueue_visible_to_search(sandbox):
    _, home, sock, _, _, spawn, _ = sandbox
    spawn()
    wait_ready(sock)
    marker = f"turn{uuid.uuid4().hex}"
    provider = _provider(home)
    provider.sync_turn(user_content=f"{marker} is a durable fact", assistant_content="Acknowledged",
                       messages=[{"role": "user", "content": f"{marker} is a durable fact"}])
    assert provider._stats["sent"] == 1
    flush = send_op(sock, "flush_observations", {})
    assert (flush.get("ok") and flush.get("flushed", 0) >= 1
            and flush.get("failed") == 0 and flush.get("unsettled") == 0), flush
    assert _search(_provider(home, "reader"), marker)


def test_uncertain_no_duplicate_replay(sandbox):
    _, home, sock, _, _, spawn, _ = sandbox
    spawn()
    wait_ready(sock)
    marker = f"uncertain{uuid.uuid4().hex}"
    interposer = sock.parent / "cut.sock"
    with CutAfterWriteProxy(interposer, sock) as proxy:
        store_id = json.loads((home / "memory_server_proxy.json").read_text())["expected_store_id"]
        make_plugin_home(home.parent, interposer, store_id)
        provider = _provider(home)
        provider.sync_turn(f"{marker} is a durable fact", "Acknowledged",
                           messages=[{"role": "user", "content": f"{marker} is a durable fact"}])
        assert proxy.forwarded.wait(3)
        assert proxy.settled.wait(5)
        assert proxy.response["ok"] is True
        assert provider._stats["uncertain"] >= 1
    # Restore direct transport; never replay the ambiguous mutation.
    store_id = json.loads((home / "memory_server_proxy.json").read_text())["expected_store_id"]
    make_plugin_home(home.parent, sock, store_id)
    assert send_op(sock, "flush_observations", {})["ok"]
    assert len(_search(_provider(home, "reader"), marker)) == 1


def test_stale_socket_blocks_start(sandbox):
    _, _, sock, _, _, spawn, _ = sandbox
    sock.write_text("occupied", encoding="utf-8")
    proc = spawn()
    assert proc.wait(timeout=15) == 4
    assert sock.read_text(encoding="utf-8") == "occupied"


def test_prefetch_cache_invalidated_on_restart(sandbox):
    _, home, sock, _, _, spawn, _ = sandbox
    proc = spawn()
    wait_ready(sock)
    provider = _provider(home, "sess-1")
    epoch = provider._epoch
    assert epoch
    provider._generation = 1
    provider._cache["prefetch"] = ("q", "sess-1", "STALE-CONTEXT", 1, epoch)
    # Restart the owner: a fresh epoch must invalidate the stale prefetch cache
    # before it is ever served (§8.5).
    assert _stop(proc) == 0
    spawn()
    wait_ready(sock)
    assert provider.prefetch("q", session_id="sess-1") == ""
    assert provider._cache == {}


def test_status_does_not_start_backend(sandbox):
    root, _, _, _, cfg, _, _ = sandbox
    before = sorted(str(p.relative_to(cfg.parent)) for p in cfg.parent.rglob("*") if p.is_file())
    env = os.environ.copy()
    env.pop("HERMES_HOME", None)
    env.pop("MEMORY_SERVER_PATH", None)
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
    result = subprocess.run([sys.executable, "-m", "memory_server.native.cli", "status", "--config", str(cfg)],
                            env=env, capture_output=True, timeout=10)
    after = sorted(str(p.relative_to(cfg.parent)) for p in cfg.parent.rglob("*") if p.is_file())
    assert result.returncode == 2, result.stderr.decode()
    assert not root.exists() and before == after


async def test_timeout_does_not_cancel_backend(sandbox):
    _, _, _, _, cfg, _, _ = sandbox
    adapter = NativeBackendAdapter(load_service_config(cfg))
    finished = threading.Event()
    observed = {}
    original_wrap = asyncio.wrap_future

    def capture(future):
        wrapped = original_wrap(future)
        observed["wrapped"] = wrapped
        return wrapped

    async def backend():
        await asyncio.sleep(0.2)
        finished.set()

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(asyncio, "wrap_future", capture)
        with pytest.raises(TimeoutError):
            await adapter._on_backend_loop(backend(), timeout=0.01)
    assert await asyncio.to_thread(finished.wait, 3)
    assert observed["wrapped"].done() and not observed["wrapped"].cancelled()
    assert observed["wrapped"].exception() is None

    # Test-only bootstrap delays the real backend coroutine, not the socket
    # response; the owner still runs its normal CLI and real store lifecycle.
    _, home, sock, _, cfg, spawn, _ = sandbox
    document = json.loads(cfg.read_text())
    document["limits"]["enqueue_timeout_s"] = 0.05
    cfg.write_text(json.dumps(document), encoding="utf-8")
    bootstrap = """
import asyncio
from memory_server.native.backend import NativeBackendAdapter
original = NativeBackendAdapter._on_backend_loop
async def delayed(self, coro, *, timeout):
    if timeout == self._config.limits.enqueue_timeout_s:
        original_coro = coro
        async def finish_late():
            await asyncio.sleep(0.25)
            return await original_coro
        coro = finish_late()
    return await original(self, coro, timeout=timeout)
NativeBackendAdapter._on_backend_loop = delayed
"""
    spawn(bootstrap=bootstrap)
    wait_ready(sock)
    marker = f"late{uuid.uuid4().hex}"
    request = send_op(sock, "enqueue_turn", {"messages": [
        {"role": "user", "content": f"{marker} is durable"},
    ]}, sid="delayed")
    assert request["code"] == "E_UNCERTAIN", request
    deadline = time.monotonic() + 8
    status = {}
    while time.monotonic() < deadline:
        status = send_op(sock, "status", {})
        assert status["ok"], status
        result = send_op(sock, "tool_call", {"name": "search", "args": {"query": marker}})
        assert result["ok"], result
        found = json.loads(result["result"])["results"]
        if status["pending"]["live_pending"] == 0 and found:
            break
        time.sleep(0.05)
    else:
        pytest.fail(f"late mutation did not persist: {status!r}")
    assert len(_search(_provider(home), marker)) == 1
