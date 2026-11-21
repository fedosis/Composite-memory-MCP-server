"""Unit tests for the native backend adapter (slice T3).

All tests use fake providers/writers and the real Hermes owner loop
(``memory_server.transports.hermes._get_loop()``). No real runtime or store is
started.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any

import pytest

from memory_server.native.backend import BackendStartError, NativeBackendAdapter
from memory_server.native.config import load_service_config
from memory_server.plugins.hermes.provider import HermesProvider
from memory_server.plugins.hermes.writer import (
    FlushReport,
    WriterBacklogError,
)
from memory_server.plugins.hermes.writer import (
    PendingSnapshot as WriterPendingSnapshot,
)
from memory_server.transports.hermes import _get_loop
from tests._native_helpers import make_service_config


@pytest.fixture
def config(tmp_path):
    return load_service_config(make_service_config(tmp_path))


def _snap(live: int = 0, terminal: int = 0, retry: int = 0, inflight: int = 0) -> WriterPendingSnapshot:
    return WriterPendingSnapshot(
        live_pending=live,
        terminal_failed=terminal,
        retry_waiting=retry,
        in_flight=inflight,
    )


class _FakeWriter:
    """Minimal writer double for the native enqueue seam."""

    def __init__(self, snapshot: WriterPendingSnapshot | None = None):
        self.snapshot = snapshot or _snap()
        self.turn_ids: list[str | None] = []
        self.add_turn_calls = 0

    def pending_snapshot(self) -> WriterPendingSnapshot:
        return self.snapshot

    async def add_turn(self, messages: Any, turn_id: str | None = None) -> None:
        self.add_turn_calls += 1
        self.turn_ids.append(turn_id)


async def test_execute_tool_async_returns_same_string(config):
    sentinel = "exact-same-string-result"

    class P:
        async def execute_tool_async(self, name, args):
            assert name == "ping"
            assert args == {"a": 1}
            return sentinel

    adapter = NativeBackendAdapter(config, provider=P())
    result = await adapter.execute_tool_async("ping", {"a": 1})
    assert result is sentinel


async def test_tool_path_ignores_session_id(config):
    class P:
        def __init__(self):
            self._session_id = "other"

        async def execute_tool_async(self, name, args):
            return self._session_id

    adapter = NativeBackendAdapter(config, provider=P())
    assert await adapter.execute_tool_async("ping", {}) == "other"


async def test_enqueue_uses_explicit_sid(config):
    provider = HermesProvider()
    writer = _FakeWriter()
    provider._writer = writer
    adapter = NativeBackendAdapter(config, provider=provider)

    await adapter.enqueue_turn_async([{"role": "user"}], session_id="sid-1")

    assert writer.turn_ids == ["sid-1"]


async def test_enqueue_does_not_queue_prefetch(config, monkeypatch):
    provider = HermesProvider()
    writer = _FakeWriter()
    provider._writer = writer
    calls: list = []
    monkeypatch.setattr(provider, "queue_prefetch", lambda *a, **k: calls.append(1))
    adapter = NativeBackendAdapter(config, provider=provider)

    await adapter.enqueue_turn_async([], session_id="sid-1")

    assert calls == []
    assert writer.turn_ids == ["sid-1"]


async def test_enqueue_gate_inside_coroutine(config):
    provider = HermesProvider()
    writer = _FakeWriter(snapshot=_snap(live=64))
    provider._writer = writer
    adapter = NativeBackendAdapter(config, provider=provider)

    with pytest.raises(WriterBacklogError) as excinfo:
        await adapter.enqueue_turn_async([], session_id="s")

    assert excinfo.value.code == "E_BACKLOG"
    assert writer.add_turn_calls == 0


async def test_recall_does_not_touch_context_cache(config):
    provider = HermesProvider()
    provider._provider = object()  # non-None so recall proceeds

    async def _fake_prefetch(query, max_results=5):
        return "recalled"

    provider._prefetch_async = _fake_prefetch
    adapter = NativeBackendAdapter(config, provider=provider)

    result = await adapter.recall_context_async("q", session_id="s", max_results=3)

    assert result == "recalled"
    assert provider._context_cache == {}


async def test_flush_returns_report_fields(config):
    class _FlushWriter:
        def __init__(self, report):
            self._report = report

        async def flush(self):
            return 0

        async def settle(self):
            return self._report

    provider = HermesProvider()
    provider._writer = _FlushWriter(
        FlushReport(total=10, flushed=3, failed=1, unsettled=4, failed_records=1, requeued=2)
    )
    adapter = NativeBackendAdapter(config, provider=provider)

    result = await adapter.flush_observations_async()

    assert result == {"flushed": 3, "failed": 1, "requeued": 2, "unsettled": 4}
    assert set(result) == {"flushed", "failed", "requeued", "unsettled"}
    assert all(isinstance(value, int) for value in result.values())


def test_start_failure_maps_to_backend_start_error(config, monkeypatch):
    adapter = NativeBackendAdapter(config)

    def boom():
        raise RuntimeError("boom")

    monkeypatch.setattr(adapter, "_start_sync", boom)
    with pytest.raises(BackendStartError) as excinfo:
        asyncio.run(adapter.start())

    assert excinfo.value.code == "E_START_FAILED"


async def test_timeout_does_not_cancel_backend(config):
    completed = threading.Event()
    outcome: dict = {}

    async def slow():
        await asyncio.sleep(0.4)
        outcome["value"] = "done"
        completed.set()
        return "done"

    adapter = NativeBackendAdapter(config)
    with pytest.raises(TimeoutError):
        await adapter._on_backend_loop(slow(), timeout=0.05)

    # The backend coroutine keeps running and finishes: the timeout only stops
    # waiting, it does not cancel the wrap_future / backend coroutine.
    assert completed.wait(timeout=5.0)
    assert outcome.get("value") == "done"
    await asyncio.sleep(0.05)


async def test_backend_calls_run_on_backend_loop(config):
    loop = _get_loop()
    captured: dict = {}

    class P:
        async def execute_tool_async(self, name, args):
            captured["thread"] = threading.get_ident()
            captured["loop"] = asyncio.get_running_loop()
            return "ok"

    adapter = NativeBackendAdapter(config, provider=P())
    assert await adapter.execute_tool_async("ping", {}) == "ok"

    assert captured["loop"] is loop

    async def _ident():
        return threading.get_ident()

    fut = asyncio.run_coroutine_threadsafe(_ident(), loop)
    assert captured["thread"] == fut.result(timeout=5)
