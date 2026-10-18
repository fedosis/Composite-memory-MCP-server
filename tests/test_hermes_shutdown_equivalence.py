"""C3 shutdown-equivalence and S1 parity against the C2 cutover provider.

Contract/parity only. No behavioral RED.
Clock vs time.monotonic split (F4).
"""
from __future__ import annotations

import asyncio
import threading
import time

import pytest
from storage.outbox_worker import OutboxWorker

from memory_server.plugins.hermes import provider as provider_mod
from memory_server.plugins.hermes.provider import HermesProvider
from memory_server.providers.embedding_provider import MockEmbeddingProvider
from memory_server.transports.hermes import HermesRuntimeBridge, _get_loop, _run_async
from tests.test_runtime_factory import isolated  # noqa: F401
from tests.test_runtime_lifecycle import Clock, held, pump


def _on_owner(awaitable, timeout=5.0):
    return asyncio.run_coroutine_threadsafe(awaitable, _get_loop()).result(timeout)


def _init_cutover_provider(tmp_path, monkeypatch):
    monkeypatch.setenv("MEMORY_VECTOR_BACKEND", "lancedb")
    import memory_server.providers.embedding_provider as embedding_module
    monkeypatch.setattr(
        embedding_module, "SentenceTransformerEmbeddingProvider", MockEmbeddingProvider
    )
    provider = HermesProvider()
    config = {
        "db_url": f"sqlite+aiosqlite:///{tmp_path / 'memory.db'}",
        "path": str(tmp_path),
    }
    provider.initialize(
        session_id="c3",
        hermes_home=str(tmp_path),
        config=config,
    )
    return provider


def _inject_clock(monkeypatch, clock):
    orig = HermesRuntimeBridge.__init__

    def wrapped(self, *args, **kwargs):
        kwargs["clock"] = clock
        kwargs["sleep"] = clock.sleep
        return orig(self, *args, **kwargs)

    monkeypatch.setattr(HermesRuntimeBridge, "__init__", wrapped)
    monkeypatch.setattr(provider_mod, "HermesRuntimeBridge", HermesRuntimeBridge)


def _spy(events, obj, name, label=None):
    orig = getattr(obj, name, None)
    tag = label or name

    async def as_async(*args, **kwargs):
        events.append(tag)
        return await orig(*args, **kwargs) if orig is not None else None

    def as_sync(*args, **kwargs):
        events.append(tag)
        return orig(*args, **kwargs) if orig is not None else None

    setattr(obj, name, as_async if orig is None or asyncio.iscoroutinefunction(orig) else as_sync)


def test_hermes_s1_deadlines_match_mcp_offsets(tmp_path, monkeypatch):
    provider = _init_cutover_provider(tmp_path, monkeypatch)
    try:
        runtime = provider._bridge.runtime
        assert provider._bridge.clock is None and runtime._clock is time.monotonic
        report = _run_async(provider._bridge.close(), timeout=30.0)
        t0 = report.phases["closing"]
        assert tuple(round(d - t0) for d in report.phases["deadlines"]) == (10, 15, 25, 30)
        assert len(report.phases["deadlines"]) == 4
        assert report.lock_retained is False
    finally:
        try:
            provider.shutdown()
        except Exception:
            pass


def test_hermes_s1_phase_order_stop_pause_drain_cancel_settle_flush_close(tmp_path, monkeypatch):
    closes = {"n": 0}
    orig_close = OutboxWorker.close

    async def counting_close(self):
        closes["n"] += 1
        return await orig_close(self)

    monkeypatch.setattr(OutboxWorker, "close", counting_close)
    provider = _init_cutover_provider(tmp_path, monkeypatch)
    try:
        runtime = provider._bridge.runtime
        assert provider._bridge.clock is None and runtime._clock is time.monotonic
        worker = runtime.dependencies.outbox_worker
        if worker is None:
            pytest.fail("ENV_BLOCKED: outbox_worker is None")
        events = []
        hooks = provider._bridge.hooks
        deps = runtime.dependencies
        _spy(events, hooks, "stop_admission")
        _spy(events, hooks, "invalidate_prefetch_generation")
        _spy(events, hooks, "pause_writer")
        _spy(events, worker, "stop", "worker.stop")
        _spy(events, hooks, "drain_producers")
        _spy(events, hooks, "settle_writer")
        _spy(events, hooks, "flush_final")
        _spy(events, deps.vector, "close", "vector")
        _spy(events, deps.embedder, "close", "embedder")
        _spy(events, deps.graph, "close", "graph")
        _spy(events, deps.sqlite, "close", "sqlite")
        report = _run_async(provider._bridge.close(), timeout=30.0)
        assert events[:7] == [
            "stop_admission",
            "invalidate_prefetch_generation",
            "pause_writer",
            "worker.stop",
            "drain_producers",
            "settle_writer",
            "flush_final",
        ]
        assert events[-4:] == ["vector", "embedder", "graph", "sqlite"]
        assert "outbox_worker" not in events
        assert closes["n"] == 0
        assert report.code in {None, "E_RUNTIME_FLUSH_FAILED"}
    finally:
        try:
            provider.shutdown()
        except Exception:
            pass


def test_settle_writer_translates_unknown_in_flight_only(tmp_path, monkeypatch):
    provider = _init_cutover_provider(tmp_path, monkeypatch)
    try:
        runtime = provider._bridge.runtime
        assert provider._bridge.clock is None and runtime._clock is time.monotonic
        writer = provider._writer
        hooks = provider._bridge.hooks

        async def hanging(batch):
            raise asyncio.CancelledError

        writer._write_callback = hanging
        writer._max_batch = 3

        async def setup():
            await writer.add_turn("a", "1")
            await writer.add_turn("b", "2")
            await writer.add_turn("c", "3")
            await writer.flush()
            result = await hooks.settle_writer(time.monotonic() + 25.0)
            snap = await writer.settle_custody()
            return result, snap

        result, snap = _on_owner(setup())
        assert result["unsettled"] == 3
        assert set(result) == {"flushed", "failed", "requeued", "unsettled"}
        assert "safe_queued" not in result and "safe_queued_items" not in result
        assert snap.invariant_ok and snap.unknown_in_flight_items == 3
        assert snap.safe_queued_items == 0
    finally:
        try:
            provider.shutdown()
        except Exception:
            pass


def test_settle_writer_mismatch_blocks_with_nonzero_unsettled(tmp_path, monkeypatch):
    provider = _init_cutover_provider(tmp_path, monkeypatch)
    try:
        runtime = provider._bridge.runtime
        assert provider._bridge.clock is None and runtime._clock is time.monotonic
        writer = provider._writer
        hooks = provider._bridge.hooks

        async def mismatch():
            await writer.add_turn("x", "x")
            writer.stop_admission()
            await writer.pause()
            writer._total_flushed += 1
            snap = await writer.settle_custody()
            assert snap.invariant_ok is False
            translated = await hooks.settle_writer(time.monotonic() + 25.0)
            assert translated["unsettled"] >= 1

        _on_owner(mismatch())
        with pytest.raises(RuntimeError, match="close-failed"):
            provider.shutdown()
        assert runtime._report.code == "E_RUNTIME_SHUTDOWN_TIMEOUT"
        assert runtime.state == "FAILED"
        assert runtime.lock is not None
        held(runtime.lock.root)
    finally:
        try:
            provider.shutdown()
        except Exception:
            pass


def test_flush_final_raw_unsettled(tmp_path, monkeypatch):
    provider = _init_cutover_provider(tmp_path, monkeypatch)
    try:
        runtime = provider._bridge.runtime
        assert provider._bridge.clock is None and runtime._clock is time.monotonic
        writer = provider._writer
        hooks = provider._bridge.hooks
        writer._write_callback = lambda batch: None
        seen = {}

        async def setup():
            await writer.add_turn("a", "a")
            await writer.add_turn("b", "b")
            settled = await hooks.settle_writer(time.monotonic() + 25)
            assert settled["unsettled"] == 0
            orig = hooks.flush_final

            async def wrapped(deadline):
                assert deadline > time.monotonic()
                seen["before"] = writer._report().unsettled
                result = await orig(deadline)
                seen["after"] = result["unsettled"]
                return result

            hooks.flush_final = wrapped

        _on_owner(setup())
        report = _run_async(provider._bridge.close(), timeout=30.0)
        assert seen["before"] == 2
        assert seen["after"] == 0
        assert report.unsettled == 0
        assert report.lock_retained is False
    finally:
        try:
            provider.shutdown()
        except Exception:
            pass


def test_prefetch_generation_bump_does_not_claim_task_termination(tmp_path, monkeypatch):
    provider = _init_cutover_provider(tmp_path, monkeypatch)
    try:
        runtime = provider._bridge.runtime
        assert provider._bridge.clock is None and runtime._clock is time.monotonic

        async def make_gate():
            return asyncio.Event()

        gate = _on_owner(make_gate())
        orig = provider._bridge.spawn_prefetch

        def wrapped(factory):
            return orig(lambda: gate.wait())

        provider._bridge.spawn_prefetch = wrapped
        before = provider._prefetch_generation
        provider.queue_prefetch("who is Alice")
        deadline = time.monotonic() + 5
        rec = None
        while time.monotonic() < deadline:
            _, records, _ = runtime.owned_snapshot()
            rec = next((item for item in records if item.kind == "hermes-prefetch"), None)
            if rec is not None and rec.task is not None and rec.state in {"RESERVED", "ATTACHED"}:
                break
            time.sleep(0.05)
        assert rec is not None and rec.task is not None
        assert rec.state in {"RESERVED", "ATTACHED"}
        task = rec.task
        _on_owner(provider._bridge.hooks.invalidate_prefetch_generation())
        assert provider._prefetch_generation == before + 1
        assert task.done() is False and task.cancelled() is False
        _, records, _ = runtime.owned_snapshot()
        assert any(item.task is task for item in records)
        _get_loop().call_soon_threadsafe(gate.set)
    finally:
        try:
            provider.shutdown()
        except Exception:
            pass


def test_repeated_close_reuses_cleanup_task(tmp_path, monkeypatch):
    provider = _init_cutover_provider(tmp_path, monkeypatch)
    try:
        runtime = provider._bridge.runtime
        assert provider._bridge.clock is None and runtime._clock is time.monotonic
        first = _run_async(provider._bridge.close(), timeout=30.0)
        cleanup = runtime._cleanup_task
        second = _run_async(provider._bridge.close(), timeout=30.0)
        assert second is first or (second.code == first.code and second.phases == first.phases)
        assert runtime._cleanup_task is cleanup
        provider.shutdown()
        assert provider._shut_down is True
        provider.shutdown()
        assert provider._shut_down is True
        assert runtime.state == "CLOSED"
    finally:
        try:
            provider.shutdown()
        except Exception:
            pass


def test_cleanup_retry_same_owner(tmp_path, monkeypatch):
    clock = Clock()
    _inject_clock(monkeypatch, clock)
    provider = _init_cutover_provider(tmp_path, monkeypatch)
    try:
        runtime = provider._bridge.runtime
        assert runtime._clock is clock
        hold, calls = threading.Event(), []
        handle = _on_owner(runtime.submit_native(lambda: (calls.append(1), hold.wait()), kind="c3-retry"))
        closer = asyncio.run_coroutine_threadsafe(provider._bridge.close(), _get_loop())
        _on_owner(pump())
        live_task = runtime._cleanup_task
        lock = runtime.lock
        retry = asyncio.run_coroutine_threadsafe(provider._bridge.close(), _get_loop())
        assert runtime._cleanup_task is live_task
        _on_owner(clock.advance(15.0))
        assert closer.result(timeout=30).code == "E_RUNTIME_SHUTDOWN_TIMEOUT"
        assert retry.result(timeout=30).code == "E_RUNTIME_SHUTDOWN_TIMEOUT"
        rec_id = next(n.id for n in runtime.owned_snapshot()[2])
        assert lock is runtime.lock and lock is not None
        held(lock.root)
        hold.set()
        handle.thread.join(timeout=2)
        _on_owner(pump())
        final = _run_async(provider._bridge.close(), timeout=30.0)
        assert calls == [1]
        assert rec_id not in {n.id for n in runtime.owned_snapshot()[2]}
        assert final.lock_retained is False
    finally:
        try:
            provider.shutdown()
        except Exception:
            pass
