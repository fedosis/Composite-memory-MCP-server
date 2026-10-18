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

# C2 sequential slice. C1 content above is frozen.

import threading  # noqa: E402

from memory_server.plugins.hermes import provider as provider_mod  # noqa: E402
from memory_server.plugins.hermes.provider import HermesProvider  # noqa: E402
from memory_server.providers.embedding_provider import MockEmbeddingProvider  # noqa: E402
from tests.test_runtime_lifecycle import held  # noqa: E402


def _init_file_provider(tmp_path, monkeypatch, **init_kwargs) -> HermesProvider:
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
    config.update(init_kwargs.get("config", {}))
    provider.initialize(
        session_id=init_kwargs.get("session_id", "c2"),
        hermes_home=str(tmp_path),
        config=config,
    )
    return provider


def test_bridge_binds_manager_to_shared_loop(tmp_path, monkeypatch):
    provider = _init_file_provider(tmp_path, monkeypatch)
    try:
        assert provider._bridge.manager._loop is _get_loop()
        assert provider._bridge.runtime.state == "READY"
        assert provider._bridge.runtime.lock is not None
    finally:
        provider.shutdown()


def test_start_publishes_only_after_ready(tmp_path, monkeypatch):
    import memory_server.runtime.runtime as runtime_mod
    gate = threading.Event()
    entered = threading.Event()
    orig = runtime_mod.CMMSRuntime.start

    async def gated(self):
        entered.set()
        assert gate.wait(timeout=60)
        return await orig(self)

    monkeypatch.setattr(runtime_mod.CMMSRuntime, "start", gated)
    monkeypatch.setenv("MEMORY_VECTOR_BACKEND", "lancedb")
    import memory_server.providers.embedding_provider as embedding_module
    monkeypatch.setattr(
        embedding_module, "SentenceTransformerEmbeddingProvider", MockEmbeddingProvider
    )
    provider = HermesProvider()
    error = {}

    def run():
        try:
            provider.initialize(
                session_id="c2-ready",
                hermes_home=str(tmp_path),
                config={"db_url": f"sqlite+aiosqlite:///{tmp_path / 'memory.db'}", "path": str(tmp_path)},
            )
        except Exception as exc:
            error["exc"] = exc

    worker = threading.Thread(target=run)
    worker.start()
    assert entered.wait(timeout=60)
    assert provider._initialized is False and provider._provider is None
    gate.set()
    worker.join(timeout=60)
    assert error == {}
    try:
        assert provider._initialized is True
        assert provider._bridge.runtime.state == "READY"
    finally:
        provider.shutdown()


def test_partial_start_failure_leaves_no_second_owner(tmp_path, monkeypatch):
    from memory_server.runtime.runtime_factory import RuntimeFactory
    closes = {"n": 0}
    from storage.outbox_worker import OutboxWorker
    orig_close = OutboxWorker.close

    async def spy_close(self):
        closes["n"] += 1
        return await orig_close(self)

    monkeypatch.setattr(OutboxWorker, "close", spy_close)
    orig_init = RuntimeFactory.__init__

    def wrapped(self, layout, *args, **kwargs):
        async def boom(_runtime):
            raise RuntimeError("c2-partial")
        overrides = dict(kwargs.get("overrides") or {})
        overrides["embedder"] = boom
        kwargs["overrides"] = overrides
        return orig_init(self, layout, *args, **kwargs)

    monkeypatch.setattr(RuntimeFactory, "__init__", wrapped)
    monkeypatch.setenv("MEMORY_VECTOR_BACKEND", "lancedb")
    import memory_server.providers.embedding_provider as embedding_module
    monkeypatch.setattr(
        embedding_module, "SentenceTransformerEmbeddingProvider", MockEmbeddingProvider
    )
    provider = HermesProvider()
    with pytest.raises(RuntimeError, match="c2-partial"):
        provider.initialize(
            session_id="c2-partial",
            hermes_home=str(tmp_path),
            config={"db_url": f"sqlite+aiosqlite:///{tmp_path / 'memory.db'}", "path": str(tmp_path)},
        )
    assert provider._root_lock is None
    runtime = getattr(getattr(provider, "_bridge", None), "runtime", None)
    runtime_holds = runtime is not None and getattr(runtime, "lock", None) is not None
    root = provider._storage_layout.data_root
    if runtime_holds:
        held(root)
    else:
        from memory_server.storage_lock import RuntimeStorageLock
        lock = RuntimeStorageLock.acquire_exclusive(root, timeout=0)
        lock.release_exclusive()
    assert closes["n"] == 0


def test_factory_injects_seams_through_bridge_not_provider(tmp_path, monkeypatch):
    provider = _init_file_provider(tmp_path, monkeypatch)
    try:
        runtime = provider._bridge.runtime
        worker = runtime.dependencies.outbox_worker
        assert worker._task_spawner is runtime.spawn_owned_task
        assert worker._native_submitter is runtime.submit_native
        assert runtime.dependencies.vector._native_submitter is runtime.submit_native
        assert provider._outbox_worker is worker
    finally:
        provider.shutdown()


def test_worker_parent_kind_is_hermes_worker_parent(tmp_path, monkeypatch):
    provider = _init_file_provider(tmp_path, monkeypatch)
    try:
        _, records, _ = provider._bridge.runtime.owned_snapshot()
        assert any(rec.kind == "hermes-worker-parent" for rec in records)
        assert provider._outbox_task is provider._bridge.hooks.worker_task
    finally:
        provider.shutdown()


def test_prefetch_spawn_owned_kind_and_generation_is_not_termination(tmp_path, monkeypatch):
    provider = _init_file_provider(tmp_path, monkeypatch)
    gate = None
    try:
        async def _prefetch_gate():
            return asyncio.Event()

        gate = _run_on_owner(_prefetch_gate())
        original = provider._queue_prefetch_async

        async def _held_prefetch(*args, **kwargs):
            await gate.wait()
            return await original(*args, **kwargs)

        monkeypatch.setattr(provider, "_queue_prefetch_async", _held_prefetch)
        provider.queue_prefetch("who is Alice")
        deadline = __import__("time").monotonic() + 5
        rec = None
        while __import__("time").monotonic() < deadline:
            _, records, _ = provider._bridge.runtime.owned_snapshot()
            rec = next((item for item in records if item.kind == "hermes-prefetch"), None)
            if rec is not None:
                break
            __import__("time").sleep(0.05)
        assert rec is not None and rec.state in {"RESERVED", "ATTACHED"}
        task = rec.task
        provider._bump_prefetch_generation()
        _, records, _ = provider._bridge.runtime.owned_snapshot()
        live = next(item for item in records if item.kind == "hermes-prefetch")
        assert live.task is task and task.done() is False
    finally:
        if gate is not None:
            _get_loop().call_soon_threadsafe(gate.set)
        provider.shutdown()


def test_extractor_identity_through_bridge(tmp_path, monkeypatch):
    import memory_server.api.learn as learn_mod
    import memory_server.plugins.hermes as hermes_pkg
    created = {}
    orig_factory = provider_mod.build_llm_extractor_from_cfg

    def spy_factory(cfg, *, hermes_home):
        created["fn"] = orig_factory(cfg, hermes_home=hermes_home)
        created["n"] = created.get("n", 0) + 1
        return created["fn"]

    monkeypatch.setattr(provider_mod, "build_llm_extractor_from_cfg", spy_factory)
    captured = {}
    orig_learn = learn_mod.learn

    async def learn_spy(**kwargs):
        captured.update(kwargs)
        return await orig_learn(**kwargs)

    monkeypatch.setattr(learn_mod, "learn", learn_spy)
    provider = _init_file_provider(tmp_path, monkeypatch)
    try:
        provider_mod._run_async(
            provider._handle_batch_write(
                [([{"role": "user", "content": "Alice is an engineer at Acme Corporation."}], "t1")]
            )
        )
        assert captured["llm_extractor"] is provider._llm_extractor is created["fn"]
        assert created["n"] == 1
        assert hermes_pkg.HermesProvider is HermesProvider
        assert captured["native_submitter"] is provider._bridge.runtime.submit_native
    finally:
        provider.shutdown()


def test_red_hermes_integrated_extractor_close_retains_ex_while_thread_alive(tmp_path, monkeypatch):
    import time
    recorded = {}
    barrier = threading.Event()
    calls = {"n": 0}

    def blocked(text):
        calls["n"] += 1
        recorded["thread"] = threading.current_thread()
        barrier.wait(timeout=60)
        return {
            "facts": [{"subject": "Docker", "predicate": "is", "object": "container", "confidence": 0.9}],
            "decisions": [],
        }

    monkeypatch.setattr(provider_mod, "build_llm_extractor_from_cfg", lambda cfg, *, hermes_home: blocked)
    monkeypatch.setenv("MEMORY_SERVER_LLM_TIMEOUT_SECONDS", "0.3")
    monkeypatch.setenv("MEMORY_VECTOR_BACKEND", "lancedb")
    import memory_server.providers.embedding_provider as embedding_module
    monkeypatch.setattr(
        embedding_module, "SentenceTransformerEmbeddingProvider", MockEmbeddingProvider
    )
    provider = HermesProvider()
    db_path = tmp_path / "memory.db"
    provider.initialize(
        session_id="c2-red",
        hermes_home=str(tmp_path),
        config={
            "db_url": f"sqlite+aiosqlite:///{db_path}",
            "path": str(tmp_path),
            "llm_timeout_seconds": 0.3,
        },
    )
    assert db_path.exists()
    assert provider._storage_layout.vector.local_path is not None
    try:
        provider_mod._run_async(
            provider._handle_batch_write(
                [([{"role": "user", "content": "Alice is an engineer at Acme Corporation."}], "t1")]
            ),
            timeout=10.0,
        )
        if "thread" not in recorded:
            pytest.fail("ENV_BLOCKED: extractor wrap never fired")
        assert recorded["thread"].is_alive()
        assert calls["n"] == 1
        assert barrier.is_set() is False
        shut = {"exc": None}

        def do_shutdown():
            try:
                provider.shutdown()
            except Exception as exc:
                shut["exc"] = exc

        closer = threading.Thread(target=do_shutdown)
        root = provider._storage_layout.data_root
        runtime = getattr(getattr(provider, "_bridge", None), "runtime", None)
        lock_root = runtime.lock.root if runtime is not None and getattr(runtime, "lock", None) is not None else None
        closer.start()
        time.sleep(0.3)
        held(root)
        if lock_root is not None:
            held(lock_root)
        assert provider._root_lock is None
        if closer.is_alive():
            pass
        else:
            assert shut["exc"] is not None
            message = str(shut["exc"]).lower()
            code = str(getattr(shut["exc"], "code", ""))
            assert "E_RUNTIME_SHUTDOWN_TIMEOUT" in code or "timeout" in message or "close-failed" in message
        barrier.set()
        recorded["thread"].join(timeout=10)
        closer.join(timeout=40)
        if shut["exc"] is not None or provider._cleanup_failed:
            provider.shutdown()
        assert calls["n"] == 1
    finally:
        barrier.set()
        thread = recorded.get("thread")
        if thread is not None and thread.is_alive():
            thread.join(timeout=5)
        try:
            provider.shutdown()
        except Exception:
            pass


def test_no_root_lock_and_no_provider_constructed_outbox(tmp_path, monkeypatch):
    provider = _init_file_provider(tmp_path, monkeypatch)
    try:
        assert provider._root_lock is None
        assert provider._bridge.runtime.lock is not None
        assert provider._outbox_worker is provider._bridge.runtime.dependencies.outbox_worker
        text = __import__("pathlib").Path(provider_mod.__file__).read_text()
        assert "OutboxWorker(" not in text
        assert "RuntimeStorageLock.acquire(" not in text
    finally:
        provider.shutdown()
