"""DETAIL §3 C3: real provider/bridge/manager tests on disposable stores only."""
from __future__ import annotations

import asyncio
import gc
import threading
import weakref
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from memory_server.plugins.hermes import provider as native
from memory_server.plugins.hermes.provider import HermesProvider
from memory_server.plugins.hermes.writer import WriterQueue
from memory_server.runtime import ShutdownReport
from memory_server.runtime.runtime import CMMSRuntime
from memory_server.runtime.runtime_manager import CloseAllReport, ProfileCloseOutcome, RuntimeManagerError
from memory_server.storage_lock import RuntimeStorageLock
from memory_server.transports import hermes as adapter


@pytest.fixture
def harness(synthetic_storage_env, monkeypatch):
    env = synthetic_storage_env
    env.assert_injection()
    providers = []
    counts = {"factory": 0, "lock": 0, "get": 0}
    create = adapter.RuntimeFactory.create
    acquire = RuntimeStorageLock.acquire_exclusive_async
    get = adapter.RuntimeManager.get

    def factory(*args, **kwargs):
        counts["factory"] += 1
        # Keep the real SQLite, graph, worker, runtime and storage lock; no
        # optional backend or network/model access is needed for these tests.
        overrides = dict(kwargs.pop("overrides", None) or {})
        overrides.update(vector=lambda runtime: None, embedder=lambda runtime: None)
        return create(*args, overrides=overrides, **kwargs)

    async def lock(*args, **kwargs):
        counts["lock"] += 1
        return await acquire(*args, **kwargs)

    async def manager_get(self, *args, **kwargs):
        counts["get"] += 1
        return await get(self, *args, **kwargs)

    monkeypatch.setattr(adapter.RuntimeFactory, "create", factory)
    monkeypatch.setattr(RuntimeStorageLock, "acquire_exclusive_async", lock)
    monkeypatch.setattr(adapter.RuntimeManager, "get", manager_get)

    def make(name="one", *, initialize=True):
        home = env.root / name
        home.mkdir(exist_ok=True)
        provider = HermesProvider()
        providers.append(provider)
        if initialize:
            provider.initialize("session", hermes_home=str(home))
            env.assert_provider_synthetic(provider, profile_home=home)
        return provider

    make.release = providers.remove
    yield make, counts, env
    for provider in reversed(providers):
        if not provider._shut_down:
            provider.shutdown()
    assert not adapter._SHARED


def bridge_copy(provider, **changes):
    old = provider._bridge
    values = dict(layout=old.layout, settings=old.settings, config=old.config,
                  hooks=adapter.HermesRuntimeHooks(), profile_id=old.profile_id,
                  write_callback=provider._handle_batch_write, flush_interval=600, max_batch=50,
                  start_outbox=old.start_outbox, clock=old.clock, sleep=old.sleep)
    values.update(changes)
    return adapter.HermesRuntimeBridge(**values)


def usable(provider):
    # Persist and read back through the actual provider API, not just READY.
    import json

    result = json.loads(adapter._run_async(
        provider._handle_remember(subject="lease", predicate="has", object="custody")
    ))
    readback = json.loads(adapter._run_async(provider._handle_search(query="lease")))
    assert result["fact"]["id"] in {item["id"] for item in readback["results"]}
    assert provider._bridge.runtime.state == "READY"


def test_equal_descriptor_two_providers_share_runtime_and_single_outbox(harness, monkeypatch):
    make, counts, _ = harness
    first = make(initialize=False)
    second = make(initialize=False)
    gate, entered = threading.Event(), threading.Event()
    start = WriterQueue.start
    calls = []

    async def gated(writer):
        calls.append(writer)
        if len(calls) == 1:
            entered.set()
            await asyncio.to_thread(gate.wait)
        return await start(writer)

    monkeypatch.setattr(WriterQueue, "start", gated)
    home = harness[2].root / "one"
    with ThreadPoolExecutor(2) as pool:
        pending = pool.submit(first.initialize, "first", hermes_home=str(home))
        assert entered.wait(10)
        second.initialize("second", hermes_home=str(home))
        entry = second._bridge._entry
        assert entry.worker_started and entry.worker_task is not None
        assert entry.leases == 2
        gate.set()
        pending.result(timeout=10)
    assert first._bridge.runtime is second._bridge.runtime
    assert first._outbox_task is second._outbox_task is entry.worker_task
    assert counts == {"factory": 1, "lock": 1, "get": 2}


@pytest.mark.parametrize("change", ["config", "layout", "outbox", "clock", "sleep"])
def test_unequal_descriptors_same_profile_fail_closed_before_lock(harness, change):
    make, counts, _ = harness
    first = make()
    changes = {
        "config": {"config": replace(first._bridge.config, sqlite_busy_timeout_ms=1)},
        "layout": {"profile_id": first._bridge.profile_id,
                   "layout": replace(first._bridge.layout, compatibility=("different",))},
        "outbox": {"start_outbox": False}, "clock": {"clock": lambda: 0},
        "sleep": {"sleep": asyncio.sleep},
    }
    second = bridge_copy(first, **changes[change])
    before = counts.copy()
    with pytest.raises(RuntimeManagerError, match="E_RUNTIME_DESCRIPTOR_CONFLICT"):
        adapter._run_async(second.start())
    assert counts == before
    assert not second._held and first._bridge._entry.leases == 1
    assert isinstance(adapter._run_async(second.close()), ShutdownReport)


def test_different_profiles_remain_isolated(harness):
    make, counts, _ = harness
    first, second = make("one"), make("two")
    assert first._bridge.runtime is not second._bridge.runtime
    assert first._outbox_task is not second._outbox_task
    assert counts["factory"] == counts["lock"] == 2
    usable(first)
    usable(second)


def test_cross_loop_equal_descriptor_fails_closed_without_lease(harness):
    make, counts, _ = harness
    first = make()
    for populated in (True, False):
        if not populated:
            first.shutdown()
        second = bridge_copy(first)
        before = counts.copy()
        with pytest.raises(RuntimeManagerError, match="E_RUNTIME_MANAGER_WRONG_LOOP"):
            asyncio.run(second.start())
        assert counts == before
        assert not second._held
        assert len(adapter._SHARED) == int(populated)


@pytest.mark.parametrize("failure", ["writer", "binding"])
def test_second_initialize_failure_keeps_first_provider_usable(harness, monkeypatch, failure):
    make, _, env = harness
    first = make()
    second = make(initialize=False)
    task = first._outbox_task
    with monkeypatch.context() as patch:
        if failure == "writer":
            patch.setattr(WriterQueue, "start", AsyncMock(side_effect=ValueError("injected")))
        else:
            patch.setattr(native, "serialize_layout_redacted", lambda _: (_ for _ in ()).throw(ValueError("injected")))
        with pytest.raises(ValueError):
            second.initialize("failed", hermes_home=str(env.root / "one"))
    assert first._bridge._entry.leases == 1
    assert not second._bridge._held
    assert first._outbox_task is task and not task.done()
    usable(first)


def test_non_final_close_keeps_second_provider_usable(harness):
    make, _, _ = harness
    first, second = make(), make()
    entry, task = first._bridge._entry, first._outbox_task
    token = first._bridge._token
    first.shutdown()
    assert entry.leases == 1 and token not in entry.hooks._hooks
    assert entry.worker_task is task and not task.done()
    assert second._bridge.runtime.lock is not None
    assert first._bridge._last_report.code is None
    usable(second)


@pytest.mark.parametrize("fault", ["exception", "failed", "timeout", "unsettled", "invariant"])
def test_non_final_close_failure_retains_lease_and_lock(harness, monkeypatch, fault):
    make, _, _ = harness
    first, second = make(), make()
    bridge, writer = first._bridge, first._writer
    entry, token = bridge._entry, bridge._token
    flush, settle = writer.flush_final, writer.settle_custody

    async def bad_flush(deadline):
        report = await flush(deadline)
        if fault == "exception":
            raise ValueError("injected")
        if fault == "timeout":
            raise TimeoutError()
        return replace(report, **({"failed": 1} if fault == "failed" else {"unsettled": 1}))

    async def bad_settle():
        return replace(await settle(), invariant_ok=False)

    with monkeypatch.context() as patch:
        if fault == "invariant":
            patch.setattr(writer, "settle_custody", bad_settle)
        else:
            patch.setattr(writer, "flush_final", bad_flush)
        with pytest.raises(RuntimeError, match="cleanup failed"):
            first.shutdown()
        report = bridge._last_report
        assert report.code == "E_RUNTIME_CLEANUP_FAILED" and report.lock_retained
        assert report.retained_resources == ("writer",)
        assert bridge._held and entry.leases == 2
        assert entry.hooks._hooks[token] is bridge.hooks
        assert bridge.writer is writer and bridge.runtime.lock is not None
        assert not entry.poisoned and not entry.closing
        usable(second)
    first.shutdown()
    assert not bridge._held and entry.leases == 1


def test_final_close_releases_lock_and_runtime(harness, monkeypatch):
    make, _, _ = harness
    first, second = make(), make()
    runtime = first._bridge.runtime
    close = runtime.close
    calls = []

    async def counted():
        calls.append(1)
        return await close()

    monkeypatch.setattr(runtime, "close", counted)
    first.shutdown()
    assert not calls and runtime.lock is not None
    second.shutdown()
    assert calls == [1] and runtime.lock is None and runtime.state == "CLOSED"
    assert not adapter._SHARED
    assert adapter._run_async(second._bridge.close()) is second._bridge._last_report


def test_provider_specific_writer_callbacks_routed(harness):
    make, _, _ = harness
    first, second = make(), make()
    batches = [[], []]
    generations = [0, 0]

    async def exercise():
        for index, provider in enumerate((first, second)):
            async def callback(batch, index=index):
                batches[index].extend(batch)
            provider._writer._write_callback = callback
            def generation(index=index):
                generations[index] += 1
            provider._bridge.hooks.bind_generation(generation)
            await provider._writer.add_turn([{"role": "user", "content": str(index)}], str(index))
            await provider._writer.flush()
        await first._bridge._entry.hooks.invalidate_prefetch_generation()
    adapter._run_async(exercise())
    assert [[turn for _, turn in batch] for batch in batches] == [["0"], ["1"]]
    assert generations == [1, 1]


def test_no_retained_closed_provider(harness):
    make, _, _ = harness
    first, second = make(), make()
    entry = first._bridge._entry
    token = first._bridge._token
    provider_ref, writer_ref = weakref.ref(first), weakref.ref(first._writer)
    first.shutdown()

    async def stale():
        entry.hooks.unregister(token)
        assert await entry.hooks._invoke(token, "invalidate_prefetch_generation") is None
    adapter._run_async(stale())
    # The harness owns providers for guaranteed teardown; release that test-only
    # reference as well before asking whether shared runtime state retains it.
    make.release(first)
    del first
    gc.collect()
    assert writer_ref() is None
    assert token not in entry.hooks._hooks
    assert provider_ref() is None
    assert second.is_available()


@pytest.mark.parametrize("phase", ["writer", "manager"])
def test_last_close_blocks_concurrent_acquire_before_await(harness, monkeypatch, phase):
    make, counts, _ = harness
    first = make()
    second = bridge_copy(first)
    entry = first._bridge._entry

    async def exercise():
        entered, gate = asyncio.Event(), asyncio.Event()
        target = first._writer if phase == "writer" else entry.manager
        name = "flush_final" if phase == "writer" else "close_all"
        original = getattr(target, name)

        async def blocked(*args):
            entered.set()
            await gate.wait()
            return await original(*args)
        monkeypatch.setattr(target, name, blocked)
        closing = asyncio.create_task(first._bridge.close())
        await entered.wait()
        assert entry.closing
        before = counts.copy()
        with pytest.raises(RuntimeManagerError, match="E_RUNTIME_START_FAILED"):
            await second.start()
        assert not second._held and counts == before
        gate.set()
        assert (await closing).code is None
    adapter._run_async(exercise())
    first.shutdown()


def test_concurrent_closes_and_start_rollback_do_not_double_release(harness):
    make, _, _ = harness
    first, second = make(), make()
    entry = first._bridge._entry

    async def exercise():
        reports = await asyncio.gather(first._bridge.close(), second._bridge.close(), first._bridge.close())
        assert all(isinstance(report, ShutdownReport) and report.code is None for report in reports)
        assert entry.leases == 0
        assert not first._bridge._held and not second._bridge._held
    adapter._run_async(exercise())
    first.shutdown()
    second.shutdown()


def test_dead_loop_nonempty_registry_refuses_replacement(harness, monkeypatch):
    make, counts, _ = harness
    first = make()
    entry = first._bridge._entry
    before = counts.copy()
    with monkeypatch.context() as patch:
        patch.setattr(adapter, "_loop", None)
        patch.setattr(asyncio, "new_event_loop", lambda: pytest.fail("replacement loop"))
        with pytest.raises(RuntimeManagerError, match="E_RUNTIME_START_FAILED"):
            adapter._get_loop()
    assert entry.poisoned and counts == before
    entry.poisoned = False  # injected loop loss did not actually kill its owner


@pytest.mark.parametrize(
    "alias", ["same", "normalized", "parent", "child", "sqlite", "sidecar", "symlink", "hardlink"],
)
def test_process_registry_rejects_path_aliases_before_second_manager(harness, alias):
    make, counts, env = harness
    first = make()
    original = first._bridge.layout
    other = env.root / "other"
    other.mkdir()
    layout = replace(original, data_root=other, root_lock_path=other / ".cmms-storage.lock",
                     graph_snapshot_path=other / "graph.json", graph_lock_path=other / "graph.json.lock",
                     vector=replace(original.vector, local_path=other / "vector"),
                     sqlite=replace(original.sqlite, local_path=other / "memory.db"))
    if alias in {"same", "normalized", "parent", "child"}:
        root = {"same": original.data_root, "parent": original.data_root.parent,
                "normalized": original.data_root / ".." / original.data_root.name,
                "child": original.data_root / "child"}[alias]
        layout = replace(layout, data_root=root)
    elif alias in {"sqlite", "sidecar"}:
        path = original.sqlite.local_path
        if alias == "sidecar":
            path = path.with_name(path.name + "-wal")
        layout = replace(layout, sqlite=replace(layout.sqlite, local_path=path))
    elif alias == "symlink":
        link = other / "linked"
        link.symlink_to(original.data_root, target_is_directory=True)
        layout = replace(layout, data_root=link / "child")
    else:
        import os
        os.link(original.sqlite.local_path, other / "memory.db")
    second = bridge_copy(first, layout=layout, config=replace(first._bridge.config, layout=layout), profile_id="other")
    before = counts.copy()
    try:
        with pytest.raises(RuntimeManagerError, match="E_RUNTIME_RESOURCE_(ALIAS|IDENTITY_CHANGED)"):
            adapter._run_async(second.start())
        assert not second._held and counts == before
    finally:
        if alias == "hardlink":
            (other / "memory.db").unlink()


def test_drain_shared_worker_after_creator_provider_close(harness, monkeypatch):
    make, _, _ = harness
    first, second = make(), make()
    entry, task = first._bridge._entry, first._outbox_task
    first.shutdown()
    drained = []
    drain = entry.hooks.drain_producers

    async def observed(deadline):
        assert entry.worker_task is task
        await drain(deadline)
        drained.append(task.done())
    monkeypatch.setattr(entry.hooks, "drain_producers", observed)
    second.shutdown()
    assert drained == [True]


def test_bridge_close_always_shutdown_report(harness):
    make, _, _ = harness
    first = make()
    never = bridge_copy(first)
    assert adapter._run_async(never.close()) == ShutdownReport()
    with pytest.raises(RuntimeManagerError, match="E_RUNTIME_START_FAILED"):
        adapter._run_async(never.start())
    assert isinstance(adapter._run_async(first._bridge.close()), ShutdownReport)
    assert isinstance(adapter._run_async(first._bridge.close()), ShutdownReport)
    first.shutdown()


@pytest.mark.parametrize("complete,code", [(True, None), (True, "E_RUNTIME_FLUSH_FAILED"),
                                           (True, "E_OTHER"), (False, None), (False, "E_OTHER")])
def test_close_all_report_mapping_and_registry_retention(harness, complete, code):
    make, _, _ = harness
    first = make()
    entry = first._bridge._entry

    async def exercise():
        # Mapping fault injection occurs after real cleanup, so no test leaves
        # actual runtime/lock ownership behind when exercising retained reports.
        result = await first._bridge.close()
        assert result.code is None
        outcomes = (ProfileCloseOutcome("p", ShutdownReport(retained_resources=("sqlite",)), code, not complete),
                    ProfileCloseOutcome("q", ShutdownReport(retained_resources=("ignored",)), None, False))
        mapped = adapter._manager_report(CloseAllReport(outcomes, True, complete, code), entry.hooks)
        assert isinstance(mapped, ShutdownReport)
        assert mapped.code == (code if complete else code or "E_RUNTIME_CLOSE_ALL_FAILED")
        assert mapped.lock_retained is not complete
        assert mapped.retained_resources == (() if complete else ("sqlite",))
        malformed = adapter._manager_report(CloseAllReport((), True, False, None), entry.hooks)
        assert malformed.retained_resources == ("runtime",)
    adapter._run_async(exercise())
    first.shutdown()


def test_hook_bad_snapshot_cannot_be_hidden_or_double_counted(harness, monkeypatch):
    make, _, _ = harness
    first = make()
    make()
    entry = first._bridge._entry
    settle = first._writer.settle_custody

    async def bad():
        return replace(await settle(), invariant_ok=False)

    async def exercise():
        with monkeypatch.context() as patch:
            patch.setattr(first._writer, "settle_custody", bad)
            result = await entry.hooks.settle_writer(0)
            assert result["unsettled"] >= 1
            assert (await entry.hooks.flush_final(0))["unsettled"] >= 1
        assert (await entry.hooks.settle_writer(0))["unsettled"] == 0
        assert (await entry.hooks.flush_final(0))["flushed"] == 0
    adapter._run_async(exercise())
    first.shutdown()
    assert len(entry.hooks.ledger) == 2 and len(entry.hooks._hooks) == 1


def test_worker_claim_spawn_failure_is_poisoned_not_retried(harness, monkeypatch):
    make, counts, env = harness
    calls = []
    spawn = CMMSRuntime.spawn_owned_task

    def fail(self, callback, **kwargs):
        if kwargs.get("kind") == "hermes-worker-parent":
            calls.append(1)
            raise ValueError("spawn")
        return spawn(self, callback, **kwargs)
    first = make(initialize=False)
    with monkeypatch.context() as patch:
        patch.setattr(CMMSRuntime, "spawn_owned_task", fail)
        with pytest.raises(ValueError):
            first.initialize("failure", hermes_home=str(env.root / "one"))
    assert calls == [1] and counts["factory"] == counts["lock"] == 1
    assert first._bridge._entry.worker_started
    assert first._bridge._entry.poisoned
    assert not adapter._SHARED  # complete rollback is the only registry removal


def test_start_and_close_waiter_cancellation_preserves_owned_cleanup(harness, monkeypatch):
    make, _, _ = harness
    first = make()
    second = bridge_copy(first)
    start = WriterQueue.start

    async def exercise():
        entered, gate = asyncio.Event(), asyncio.Event()

        async def blocked(writer):
            entered.set()
            await gate.wait()
            await start(writer)
        with monkeypatch.context() as patch:
            patch.setattr(WriterQueue, "start", blocked)
            waiter = asyncio.create_task(second.start())
            await entered.wait()
            waiter.cancel()
            await asyncio.sleep(0)
            assert not waiter.done()
            gate.set()
            with pytest.raises(asyncio.CancelledError):
                await waiter
        assert not second._held and not second._published
        assert first._bridge._entry.leases == 1
        entered.clear()
        gate.clear()
        flush = first._writer.flush_final

        async def blocked_flush(deadline):
            entered.set()
            await gate.wait()
            return await flush(deadline)
        with monkeypatch.context() as patch:
            patch.setattr(first._writer, "flush_final", blocked_flush)
            waiter = asyncio.create_task(first._bridge.close())
            await entered.wait()
            waiter.cancel()
            await asyncio.sleep(0)
            assert not waiter.done()
            gate.set()
            assert isinstance(await waiter, ShutdownReport)
        assert not adapter._SHARED
    adapter._run_async(exercise())
    first.shutdown()


@pytest.mark.parametrize("kind", ["complete", "flush_failed", "other", "incomplete", "missing", "exception"])
def test_manager_result_controls_registry_removal(harness, monkeypatch, kind):
    make, counts, _ = harness
    first = make()
    bridge, entry = first._bridge, first._bridge._entry
    successor = bridge_copy(first)
    close_all = entry.manager.close_all
    cleaned = []

    async def injected():
        actual = await close_all()
        assert actual.complete
        cleaned.append(actual)
        if kind == "exception":
            raise ValueError("private exception text must not escape")
        complete = kind in {"complete", "flush_failed", "other"}
        code = {"flush_failed": "E_RUNTIME_FLUSH_FAILED", "other": "E_OTHER"}.get(kind)
        outcomes = () if kind == "missing" else (
            ProfileCloseOutcome("retained", ShutdownReport(retained_resources=("vector", "sqlite")), None,
                                not complete),
            ProfileCloseOutcome("not-retained", ShutdownReport(retained_resources=("ignored",)), None, False),
            ProfileCloseOutcome("fallback", None, None, not complete),
        )
        return CloseAllReport(outcomes, True, complete, code)

    with monkeypatch.context() as patch:
        patch.setattr(entry.manager, "close_all", injected)
        report = adapter._run_async(bridge.close())
    assert isinstance(report, ShutdownReport)
    assert cleaned and not bridge._held and entry.leases == 0
    assert bridge.runtime.lock is None  # actual synthetic cleanup precedes report injection
    complete = kind in {"complete", "flush_failed", "other"}
    assert bool(adapter._SHARED) is not complete
    assert report.lock_retained is not complete
    if not complete:
        assert entry.poisoned and entry.closing
        assert report.code == "E_RUNTIME_CLOSE_ALL_FAILED"
        expected = ("runtime", "sqlite", "vector") if kind == "incomplete" else ("runtime",)
        assert report.retained_resources == expected
        before = counts.copy()
        with pytest.raises(RuntimeManagerError, match="E_RUNTIME_START_FAILED"):
            adapter._run_async(successor.start())
        assert counts == before and not successor._held
        assert adapter._run_async(bridge.close()) is report
        if kind == "exception":
            assert report.errors == ("ValueError",)
    else:
        assert report.code == {"flush_failed": "E_RUNTIME_FLUSH_FAILED", "other": "E_OTHER"}.get(kind)
        assert not report.retained_resources
    # Fault-injection teardown only: the real CloseAllReport above proved
    # complete and released the actual synthetic lock. Undo only the injected
    # report/reservation; production code has no such recovery bypass.
    with adapter._loop_lock:
        adapter._SHARED.pop(adapter._key(entry.descriptor), None)
    bridge._last_report = ShutdownReport()
    bridge._close_task = None
    first.shutdown()


def test_stale_fanout_snapshot_rechecks_token_liveness(harness):
    make, _, _ = harness
    first = make()
    shared = first._bridge._entry.hooks

    async def exercise():
        gate, entered = asyncio.Event(), asyncio.Event()
        original = first._bridge.hooks.invalidate_prefetch_generation

        async def blocked():
            entered.set()
            await gate.wait()
            await original()
        first._bridge.hooks.invalidate_prefetch_generation = blocked
        stale = adapter.HermesRuntimeHooks()
        stale.bind_generation(lambda: pytest.fail("unregistered callback"))
        token = shared.register(stale)
        pending = asyncio.create_task(shared.invalidate_prefetch_generation())
        await entered.wait()
        assert shared._inflight[first._bridge._token] == 1
        shared.unregister(token)
        shared.unregister(token)
        gate.set()
        await pending
        first._bridge.hooks.invalidate_prefetch_generation = original
    adapter._run_async(exercise())


def test_detached_terminal_counters_counted_once(harness):
    make, _, _ = harness
    first, second = make(), make()
    entry = first._bridge._entry

    async def enqueue():
        for provider in (first, second):
            async def write(batch):
                return None
            provider._writer._write_callback = write
            await provider._writer.add_turn([{"role": "user", "content": "synthetic"}], "turn")
    adapter._run_async(enqueue())
    first.shutdown()
    assert first._bridge._last_report.flushed == 1
    assert entry.hooks.totals()["flushed"] == 1
    second.shutdown()
    assert second._bridge._last_report.flushed == 2
    assert entry.hooks.totals()["flushed"] == 2


def test_empty_registry_permits_loop_replacement(harness, monkeypatch):
    assert not adapter._SHARED
    adapter._get_loop()
    with monkeypatch.context() as patch:
        patch.setattr(adapter, "_loop", None)
        patch.setattr(adapter, "_loop_thread", None)
        loop = adapter._get_loop()
        thread = adapter._loop_thread
        assert adapter._run_async(asyncio.sleep(0, result="running")) == "running"
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)
        assert not thread.is_alive()
        loop.close()


def test_concurrent_close_and_provisional_start_rollback(harness, monkeypatch):
    make, _, env = harness
    first = make()
    second = make(initialize=False)
    entry = first._bridge._entry
    entered, gate = threading.Event(), threading.Event()

    async def failing_start(writer):
        entered.set()
        await asyncio.to_thread(gate.wait)
        raise ValueError("provisional writer")

    with monkeypatch.context() as patch:
        patch.setattr(WriterQueue, "start", failing_start)
        with ThreadPoolExecutor(1) as pool:
            pending = pool.submit(second.initialize, "provisional", hermes_home=str(env.root / "one"))
            assert entered.wait(10)
            assert entry.leases == 2
            first.shutdown()
            assert entry.leases == 1 and entry.factory.runtime.state == "READY"
            gate.set()
            with pytest.raises(ValueError, match="provisional writer"):
                pending.result(timeout=10)
    assert entry.leases == 0 and entry.factory.runtime.lock is None
    assert not adapter._SHARED
    assert isinstance(adapter._run_async(second._bridge.close()), ShutdownReport)


def test_missing_running_loop_is_stable_wrong_loop_error(harness):
    make, counts, _ = harness
    first = make()
    second = bridge_copy(first)
    before = counts.copy()
    with pytest.raises(RuntimeManagerError, match="E_RUNTIME_MANAGER_WRONG_LOOP"):
        second._acquire()
    assert not second._held and counts == before


def test_physical_directory_alias_evidence_rejected_before_manager(harness, monkeypatch):
    make, counts, env = harness
    first = make()
    source = first._bridge.layout
    other = env.root / "alternate-directory"
    other.mkdir()
    layout = replace(source, data_root=other, root_lock_path=other / ".cmms-storage.lock",
                     graph_snapshot_path=other / "graph.json", graph_lock_path=other / "graph.json.lock",
                     vector=replace(source.vector, local_path=other / "vector"),
                     sqlite=replace(source.sqlite, local_path=other / "memory.db"))
    inspect = adapter.inspect_component_chain_nofollow
    identity = inspect(source.data_root, anchor=source.data_root)[-1]

    def aliased(path, **kwargs):
        # Deterministic bind-mount-shaped evidence without mounting anything.
        return tuple(replace(item, device=identity.device, inode=identity.inode)
                     if item.lexical_path == str(other) else item for item in inspect(path, **kwargs))

    monkeypatch.setattr(adapter, "inspect_component_chain_nofollow", aliased)
    second = bridge_copy(first, profile_id="alternate", layout=layout,
                         config=replace(first._bridge.config, layout=layout))
    before = counts.copy()
    with pytest.raises(RuntimeManagerError, match="E_RUNTIME_RESOURCE_ALIAS"):
        adapter._run_async(second.start())
    assert counts == before and not second._held


def test_hook_registration_duplicate_and_foreign_loop_fail_without_mutation(harness):
    make, _, _ = harness
    first = make()
    shared, hooks = first._bridge._entry.hooks, first._bridge.hooks
    before = tuple(shared._hooks)

    async def duplicate():
        with pytest.raises(RuntimeManagerError, match="E_RUNTIME_DESCRIPTOR_CONFLICT"):
            shared.register(hooks)
    adapter._run_async(duplicate())

    async def foreign():
        with pytest.raises(RuntimeManagerError, match="E_RUNTIME_MANAGER_WRONG_LOOP"):
            shared.register(adapter.HermesRuntimeHooks())
    asyncio.run(foreign())
    assert tuple(shared._hooks) == before


def test_uncertain_provisional_writer_cleanup_retains_and_poisons(harness, monkeypatch):
    make, counts, env = harness
    first = make()
    second = make(initialize=False)
    with monkeypatch.context() as patch:
        patch.setattr(WriterQueue, "start", AsyncMock(side_effect=ValueError("start fault")))
        patch.setattr(WriterQueue, "flush_final", AsyncMock(side_effect=TimeoutError()))
        with pytest.raises(ValueError, match="start fault"):
            second.initialize("failed", hermes_home=str(env.root / "one"))
    entry = first._bridge._entry
    assert entry.poisoned and entry.closing and entry.leases == 2
    assert second._cleanup_failed and second._bridge._held
    assert second._bridge.writer is second._bridge.hooks.writer
    assert second._settings is not None and not second._initialized
    assert second._bridge.runtime is None  # partial start was never published
    successor = bridge_copy(first)
    before = counts.copy()
    with pytest.raises(RuntimeManagerError, match="E_RUNTIME_START_FAILED"):
        adapter._run_async(successor.start())
    assert counts == before and not successor._held
    usable(first)
    # Fault restoration only: no turn was admitted to this partial writer and
    # its real custody snapshot proves zero outstanding items. Production never
    # clears poisoned ownership to recover from unknown write outcomes.
    snapshot = adapter._run_async(second._bridge.writer.settle_custody())
    assert snapshot.invariant_ok and snapshot.raw_unsettled == 0
    entry.poisoned = entry.closing = False
    second._bridge._close_task = None
    second.shutdown()
    assert entry.leases == 1


def test_spawn_failure_reservation_rejects_reuse_without_second_spawn(harness, monkeypatch):
    make, counts, env = harness
    first = make(initialize=False)
    entered, gate = threading.Event(), threading.Event()
    close_all = adapter.RuntimeManager.close_all
    spawn = CMMSRuntime.spawn_owned_task
    calls = []

    async def blocked_close(manager):
        entered.set()
        await asyncio.to_thread(gate.wait)
        return await close_all(manager)

    def failing_spawn(runtime, callback, **kwargs):
        if kwargs.get("kind") == "hermes-worker-parent":
            calls.append(1)
            raise ValueError("spawn fault")
        return spawn(runtime, callback, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(adapter.RuntimeManager, "close_all", blocked_close)
        patch.setattr(CMMSRuntime, "spawn_owned_task", failing_spawn)
        with ThreadPoolExecutor(1) as pool:
            pending = pool.submit(first.initialize, "failed", hermes_home=str(env.root / "one"))
            try:
                assert entered.wait(10)
                entry = first._bridge._entry
                assert entry.poisoned and entry.closing and entry.worker_started
                successor = bridge_copy(first)
                before = counts.copy()
                with pytest.raises(RuntimeManagerError, match="E_RUNTIME_START_FAILED"):
                    adapter._run_async(successor.start())
                assert counts == before and not successor._held and calls == [1]
            finally:
                gate.set()
            with pytest.raises(ValueError, match="spawn fault"):
                pending.result(timeout=10)
    assert not adapter._SHARED


def test_sync_timeout_does_not_cancel_owned_start_or_abandon_rollback(harness, monkeypatch):
    make, _, _ = harness
    first = make()
    second = bridge_copy(first)
    entered, gate = threading.Event(), threading.Event()
    start = WriterQueue.start

    async def blocked(writer):
        entered.set()
        await asyncio.to_thread(gate.wait)
        await start(writer)

    with monkeypatch.context() as patch:
        patch.setattr(WriterQueue, "start", blocked)
        with pytest.raises(TimeoutError):
            adapter._run_async(second.start(), timeout=0)
        assert entered.wait(10)

        async def rollback():
            closing = asyncio.create_task(second.close())
            await asyncio.sleep(0)
            assert second._close_requested
            gate.set()
            return await closing
        report = adapter._run_async(rollback())
    assert isinstance(report, ShutdownReport) and report.code is None
    assert not second._held and not second._published
    assert first._bridge._entry.leases == 1
    usable(first)
