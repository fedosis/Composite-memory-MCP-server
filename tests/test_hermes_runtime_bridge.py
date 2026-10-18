"""C1 direct contract coverage for the unused Hermes runtime bridge."""

import asyncio
import concurrent.futures

import pytest

import memory_server.transports.hermes as hermes_transport
from memory_server.settings import get_settings
from memory_server.transports.hermes import (
    HermesRuntimeBridge,
    HermesRuntimeHooks,
    _get_loop,
    _hermes_runtime_config,
    _run_async,
)
from tests.test_runtime_factory import coherent_layout, isolated  # noqa: F401
from tests.test_runtime_lifecycle import Clock


def _run_on_owner(awaitable, timeout=5.0):
    return asyncio.run_coroutine_threadsafe(awaitable, _get_loop()).result(timeout)


def _make_bridge(tmp_path, *, clock=None):
    layout = coherent_layout(tmp_path)
    settings = get_settings()
    hooks = HermesRuntimeHooks()
    config = _hermes_runtime_config(layout, settings)
    kwargs = {"clock": clock, "sleep": clock.sleep} if clock is not None else {}
    return HermesRuntimeBridge(
        layout=layout,
        settings=settings,
        config=config,
        hooks=hooks,
        profile_id="c1-test-profile",
        write_callback=lambda batch: None,
        flush_interval=3600.0,
        max_batch=50,
        start_outbox=False,
        **kwargs,
    )


async def _spawn_owned_blocker(bridge, gate):
    return bridge.spawn_prefetch(lambda: gate.wait())


def test_run_async_reentrancy_raises_on_owner_loop():
    loop = _get_loop()

    async def exercise():
        probe = asyncio.sleep(0)
        try:
            with pytest.raises(RuntimeError, match="^cannot nest _run_async on the owner loop$"):
                _run_async(probe)
        finally:
            probe.close()

    assert _run_on_owner(exercise(), timeout=5.0) is None
    assert loop is _get_loop()


def test_run_async_timeout_does_not_cancel_owned_cleanup(tmp_path, monkeypatch):
    clock = Clock()
    bridge = _make_bridge(tmp_path, clock=clock)
    runtime = _run_on_owner(bridge.start())
    assert runtime is bridge.runtime and runtime.state == "READY"
    assert runtime.lock is not None and bridge.manager._loop is _get_loop()
    assert bridge.factory is not None and bridge.factory.runtime is runtime

    async def create_gate():
        return asyncio.Event()

    gate = _run_on_owner(create_gate())
    blocker = _run_on_owner(_spawn_owned_blocker(bridge, gate))
    recorded = []
    original = hermes_transport.asyncio.run_coroutine_threadsafe

    def recording(coro, loop):
        future = original(coro, loop)
        recorded.append(future)
        return future

    with monkeypatch.context() as scoped:
        scoped.setattr(hermes_transport.asyncio, "run_coroutine_threadsafe", recording)
        with pytest.raises(concurrent.futures.TimeoutError):
            _run_async(bridge.close(), timeout=0.01)

    assert len(recorded) == 1 and recorded[0].cancelled() is False

    async def live_state():
        records = runtime.owned_snapshot()[1]
        return runtime._cleanup_task, runtime.lock, any(record.task is blocker for record in records)

    cleanup_task, lock, blocker_owned = _run_on_owner(live_state())
    assert cleanup_task is not None and not cleanup_task.done() and not cleanup_task.cancelled()
    assert lock is not None and blocker_owned and not blocker.done()

    retry = asyncio.run_coroutine_threadsafe(bridge.close(), _get_loop())
    try:
        _run_on_owner(clock.advance(10.0))
        report = retry.result(timeout=5.0)
        assert runtime._cleanup_task is cleanup_task
        assert recorded[0].result(timeout=5.0) is report
        assert runtime.state == "CLOSED" and runtime.lock is None
        assert blocker.done() and not report.lock_retained
    finally:
        if not blocker.done():
            _get_loop().call_soon_threadsafe(gate.set)
        _run_on_owner(bridge.close())
