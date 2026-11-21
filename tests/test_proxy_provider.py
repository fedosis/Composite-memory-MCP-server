"""Unit tests for ``MemoryServerProxyProvider`` (slice T5).

No real service: a fake AF_UNIX owner in a short private socket dir (or a dropped
connection), plus direct state manipulation for the cache/generation/epoch logic.
"""

from __future__ import annotations

import atexit
import json
import shutil
import socket
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
for _p in (str(REPO_ROOT / "plugin-dist"), "/home/shtorm/.hermes/hermes-agent"):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from memory_server_proxy import settings, wire
from memory_server_proxy.provider import MemoryServerProxyProvider


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------

def _short_socket_dir() -> Path:
    d = Path(tempfile.mkdtemp(prefix="cmmsproxy-", dir="/tmp"))
    atexit.register(shutil.rmtree, d, ignore_errors=True)
    return d


def _write_sidecar(home: Path, *, socket_path: str, store_id: str | None = None) -> str:
    store_id = store_id or str(uuid.uuid4())
    settings.write_sidecar(home, socket_path=socket_path, expected_store_id=store_id)
    return store_id


class FakeNativeServer:
    """Minimal AF_UNIX owner emulator: hello -> one op -> response -> close."""

    def __init__(self, path: Path, *, epoch: str = "epoch-1", store_id: str | None = None,
                 schema_hash: str | None = None):
        self.path = Path(path)
        self.epoch = epoch
        self.store_id = store_id or str(uuid.uuid4())
        self.schema_hash = schema_hash or "f" * 64
        self.connections = 0
        self.ops: list[tuple[str, str, dict]] = []
        self.hellos: list[dict] = []
        self._handlers: dict = {}
        self._drop_next_response = False
        self._stop = threading.Event()
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.bind(str(self.path))
        self._sock.listen(5)
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def __enter__(self) -> "FakeNativeServer":
        return self

    def __exit__(self, *exc) -> None:
        self.stop()

    def on(self, op: str, handler) -> None:
        self._handlers[op] = handler

    def drop_next_response(self) -> None:
        self._drop_next_response = True

    def _serve(self) -> None:
        self._sock.settimeout(0.2)
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            self.connections += 1
            try:
                conn.settimeout(3.0)
                hello = wire.read_frame_blocking(conn)
                self.hellos.append(hello)
                conn.sendall(wire.encode_frame(wire.response_ok(
                    hello["correlation_id"], state="READY", store_id=self.store_id,
                    epoch=self.epoch, schema_hash=self.schema_hash)))
                req = wire.read_frame_blocking(conn)
                op = req["op"]
                self.ops.append((op, req.get("session_id", ""), req.get("payload")))
                if self._drop_next_response:
                    self._drop_next_response = False
                    continue
                handler = self._handlers.get(op)
                if handler is not None:
                    fields = handler(req.get("payload", {}), req.get("session_id", ""))
                    conn.sendall(wire.encode_frame(wire.response_ok(req["correlation_id"], **fields)))
                else:
                    conn.sendall(wire.encode_frame(wire.response_ok(req["correlation_id"], result="")))
            except (OSError, wire.WireError):
                pass
            finally:
                try:
                    conn.close()
                except OSError:
                    pass

    def stop(self) -> None:
        self._stop.set()
        try:
            self._sock.close()
        except OSError:
            pass
        self._thread.join(timeout=2.0)
        try:
            self.path.unlink(missing_ok=True)
        except OSError:
            pass


def _online_provider(home: Path, sock_path: Path) -> tuple[MemoryServerProxyProvider, str]:
    store_id = _write_sidecar(home, socket_path=str(sock_path))
    p = MemoryServerProxyProvider()
    p._home = str(home)
    p._session_id = "sess-1"
    p._sidecar = settings.read_sidecar(home)
    p.get_tool_schemas()  # load artifacts
    return p, store_id


@pytest.fixture
def provider(tmp_path: Path) -> MemoryServerProxyProvider:
    """An offline provider: valid sidecar + schemas, unreachable owner."""
    _write_sidecar(tmp_path, socket_path=str(tmp_path / "no-such-sock"))
    p = MemoryServerProxyProvider()
    p.initialize("sess-1", hermes_home=str(tmp_path))
    return p


# --------------------------------------------------------------------------
# availability
# --------------------------------------------------------------------------

def test_is_available_without_socket(tmp_path: Path, monkeypatch) -> None:
    _write_sidecar(tmp_path, socket_path=str(tmp_path / "sock"))
    p = MemoryServerProxyProvider()

    def _boom(*_a, **_k):
        raise ConnectionError("socket forbidden")

    monkeypatch.setattr(socket, "socket", _boom)
    p.initialize("s", hermes_home=str(tmp_path))
    assert p.is_available() is True
    assert p.unavailable_reason() == ""


def test_unavailable_without_config(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("HERMES_HOME", raising=False)
    p = MemoryServerProxyProvider()
    assert p.is_available() is False
    assert p.unavailable_reason() != ""


def test_schemas_pre_init() -> None:
    p = MemoryServerProxyProvider()
    schemas = p.get_tool_schemas()
    assert len(schemas) == 15
    assert schemas[0]["name"] == "invalidate"
    # no sidecar yet -> still offline
    assert p.is_available() is False


def test_identity_signature_no_epoch(tmp_path: Path) -> None:
    store_id = _write_sidecar(tmp_path, socket_path=str(tmp_path / "sock"))
    p = MemoryServerProxyProvider()
    p.initialize("s", hermes_home=str(tmp_path))
    sig = p.identity_signature()
    assert "epoch" not in sig
    assert sig["provider"] == "memory_server_proxy"
    assert sig["home"] == str(tmp_path)
    assert sig["store_id"] == store_id
    assert len(sig["schema_hash"]) == 64
    assert len(sig["config"]) == 64


# --------------------------------------------------------------------------
# prefetch cache
# --------------------------------------------------------------------------

def test_prefetch_offline_returns_empty(provider: MemoryServerProxyProvider) -> None:
    assert provider.prefetch("anything", session_id="sess-1") == ""


def test_prefetch_cache_hit_same_query_sid(provider: MemoryServerProxyProvider) -> None:
    provider._generation = 1
    provider._epoch = "epoch-1"
    provider._cache["prefetch"] = ("q", "sess-1", "CONTEXT", 1, "epoch-1")
    assert provider.prefetch("q", session_id="sess-1") == "CONTEXT"


def test_prefetch_rejects_other_query(provider: MemoryServerProxyProvider) -> None:
    provider._generation = 1
    provider._epoch = "epoch-1"
    provider._cache["prefetch"] = ("q", "sess-1", "CONTEXT", 1, "epoch-1")
    assert provider.prefetch("other", session_id="sess-1") == ""


def test_epoch_change_invalidates_cache(provider: MemoryServerProxyProvider) -> None:
    provider._generation = 3
    provider._epoch = "old"
    provider._cache["prefetch"] = ("q", "sess-1", "CONTEXT", 3, "old")
    provider._record_epoch("new")
    assert provider._cache == {}
    assert provider.prefetch("q", session_id="sess-1") == ""


def test_generation_guard_drops_stale_publish(provider: MemoryServerProxyProvider) -> None:
    provider._generation = 5
    provider._session_id = "sess-1"
    provider._epoch = "e"
    assert provider._publish("q", "sess-1", "text", 4) is False  # stale generation
    assert provider._cache == {}
    assert provider._publish("q", "sess-1", "text", 5) is True
    assert provider._cache["prefetch"][2] == "text"


def test_queue_prefetch_one_in_flight_one_replacement(provider: MemoryServerProxyProvider) -> None:
    started = threading.Event()
    release = threading.Event()
    calls: list[str] = []

    def fake_recall(query: str, sid: str) -> str:
        calls.append(query)
        started.set()
        release.wait(timeout=10)
        return "TEXT-" + query

    provider._recall = fake_recall
    provider._epoch = "e"
    provider._session_id = "sess-1"

    provider.queue_prefetch("A", session_id="sess-1")
    assert started.wait(timeout=5)
    assert provider._worker is not None and provider._worker.is_alive()

    provider.queue_prefetch("B", session_id="sess-1")
    assert provider._pending_query == "B"
    provider.queue_prefetch("C", session_id="sess-1")
    assert provider._pending_query == "C"  # B replaced

    release.set()
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and len(calls) < 2:
        time.sleep(0.01)
    assert calls == ["A", "C"]
    assert provider._cache.get("prefetch") is not None
    assert provider._cache["prefetch"][2] == "TEXT-C"
    provider.shutdown()


# --------------------------------------------------------------------------
# writes
# --------------------------------------------------------------------------

def test_sync_turn_uncertain_no_retry(tmp_path: Path) -> None:
    sock_dir = _short_socket_dir()
    sock_path = sock_dir / "memory.sock"
    p, store_id = _online_provider(tmp_path, sock_path)
    with FakeNativeServer(sock_path, store_id=store_id) as server:
        server.drop_next_response()
        p.sync_turn("hi", "hello", session_id="sess-1")
        assert p._stats["uncertain"] == 1
        assert p._stats["sent"] == 0
        assert [op[0] for op in server.ops] == ["enqueue_turn"]
        assert set(server.hellos[0]) == {"v", "op", "correlation_id", "payload"}
        assert server.hellos[0]["payload"] == {
            "schema_hash": p._schema_hash_value(), "store_id": store_id, "claimed_home": str(tmp_path),
        }


def test_on_session_end_flushes(tmp_path: Path) -> None:
    sock_dir = _short_socket_dir()
    sock_path = sock_dir / "memory.sock"
    p, store_id = _online_provider(tmp_path, sock_path)
    with FakeNativeServer(sock_path, store_id=store_id) as server:
        server.on("flush_observations",
                  lambda payload, sid: {"flushed": 1, "failed": 0, "requeued": 0, "unsettled": 0})
        p.on_session_end([])
        assert any(op[0] == "flush_observations" for op in server.ops)
        assert p._stats["dropped"] == 0


def test_on_session_switch_rebinds_and_invalidates(tmp_path: Path) -> None:
    sock_dir = _short_socket_dir()
    sock_path = sock_dir / "memory.sock"
    p, store_id = _online_provider(tmp_path, sock_path)
    with FakeNativeServer(sock_path, store_id=store_id) as server:
        server.on("flush_observations",
                  lambda payload, sid: {"flushed": 1, "failed": 0, "requeued": 0, "unsettled": 0})
        p._generation = 7
        p._epoch = "e"
        p._cache["prefetch"] = ("q", "sess-1", "CONTEXT", 7, "e")
        p.on_session_switch("new-sess")
        assert p._session_id == "new-sess"
        assert p._generation == 8
        assert p._cache == {}
        assert any(op[0] == "flush_observations" for op in server.ops)


def test_handle_tool_call_offline_error_json(provider: MemoryServerProxyProvider) -> None:
    result = provider.handle_tool_call("ping", {})
    assert json.loads(result) == {"error": "memory_server_proxy unavailable",
                                  "code": "E_UNAVAILABLE", "tool": "ping"}


# --------------------------------------------------------------------------
# host contract basics
# --------------------------------------------------------------------------

def test_backup_paths_empty() -> None:
    assert MemoryServerProxyProvider().backup_paths() == []


def test_recall_status_none() -> None:
    assert MemoryServerProxyProvider().recall_status() is None


def test_config_schema_two_fields() -> None:
    schema = MemoryServerProxyProvider().get_config_schema()
    assert len(schema) == 2
    assert [f["key"] for f in schema] == ["socket_path", "expected_store_id"]


def test_save_config_atomic_0600(tmp_path: Path) -> None:
    p = MemoryServerProxyProvider()
    store_id = str(uuid.uuid4())
    p.save_config({"socket_path": "/tmp/cmms.sock", "expected_store_id": store_id}, str(tmp_path))
    path = tmp_path / settings.CLIENT_CONFIG_NAME
    assert path.exists()
    assert path.stat().st_mode & 0o777 == 0o600
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["socket_path"] == "/tmp/cmms.sock"
    assert data["expected_store_id"] == store_id


def test_shutdown_stops_worker_under_2s(provider: MemoryServerProxyProvider) -> None:
    started = threading.Event()
    release = threading.Event()

    def slow_recall(query: str, sid: str) -> str:
        started.set()
        release.wait(timeout=30)
        return "X"

    provider._recall = slow_recall
    provider._epoch = "e"
    provider._session_id = "sess-1"
    provider.queue_prefetch("q", session_id="sess-1")
    assert started.wait(timeout=5)

    t0 = time.monotonic()
    provider.shutdown()
    elapsed = time.monotonic() - t0
    assert provider._worker_stop.is_set()
    assert elapsed < 2.5
    release.set()
