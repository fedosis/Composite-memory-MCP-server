"""Hermes composition adapter for the transport-neutral CMMS runtime."""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Callable

from memory_server.paths import StorageLayoutError, inspect_component_chain_nofollow
from memory_server.plugins.hermes.writer import WriterQueue
from memory_server.runtime import CMMSRuntime, RuntimeDependencies, RuntimeNotReadyError, ShutdownReport
from memory_server.runtime.config import RuntimeConfig
from memory_server.runtime.runtime_factory import RuntimeFactory
from memory_server.runtime.runtime_manager import (
    RuntimeDescriptor,
    RuntimeManager,
    RuntimeManagerError,
    _evidence,
    _overlap,
    _reject_unsafe,
    _resource_paths,
)

_loop: asyncio.AbstractEventLoop | None = None
_loop_thread: threading.Thread | None = None
_loop_lock = threading.Lock()

HERMES_SQLITE_BUSY_TIMEOUT_MS = 60000


def _get_loop() -> asyncio.AbstractEventLoop:
    """Return the process-wide Hermes owner loop, creating it lazily."""
    global _loop, _loop_thread
    with _loop_lock:
        if _loop is not None and _loop.is_running():
            return _loop
        if _SHARED:
            for entry in _SHARED.values():
                entry.poisoned = True
            raise RuntimeManagerError("E_RUNTIME_START_FAILED")
        loop = asyncio.new_event_loop()
        _loop = loop
        ready = threading.Event()

        def _run() -> None:
            asyncio.set_event_loop(loop)
            loop.call_soon(ready.set)
            loop.run_forever()

        _loop_thread = threading.Thread(target=_run, daemon=True, name="cmms-provider-loop")
        _loop_thread.start()
        ready.wait()
        return loop


def _run_async(coro, timeout: float = 30.0):
    """Submit to the Hermes owner loop without cancelling on sync timeout."""
    loop = _get_loop()
    try:
        current = asyncio.get_running_loop()
    except RuntimeError:
        current = None
    if current is loop:
        raise RuntimeError("cannot nest _run_async on the owner loop")
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    try:
        return future.result(timeout=timeout)
    except concurrent.futures.TimeoutError:
        raise


def _supports_background_outbox(db_url: str) -> bool:
    """Reject only transient in-memory SQLite URLs."""
    normalized = db_url.strip().lower()
    return normalized not in {
        "sqlite+aiosqlite://",
        "sqlite+aiosqlite:///:memory:",
    }


def _hermes_runtime_config(layout, settings) -> RuntimeConfig:
    captured = RuntimeConfig.capture(layout, settings)
    outbox = dict(captured.outbox)
    outbox["busy_timeout_ms"] = HERMES_SQLITE_BUSY_TIMEOUT_MS
    return replace(
        captured,
        sqlite_busy_timeout_ms=HERMES_SQLITE_BUSY_TIMEOUT_MS,
        outbox=outbox,
    )


class HermesRuntimeHooks:
    """Hermes writer/producer implementation of the runtime hook protocol."""

    def __init__(self) -> None:
        self.worker_task: asyncio.Task | None = None
        self.writer: WriterQueue | None = None
        self._invalidate_generation: Callable[[], None] | None = None

    def bind_generation(self, fn: Callable[[], None]) -> None:
        self._invalidate_generation = fn

    async def stop_admission(self) -> None:
        if self.writer is not None:
            self.writer.stop_admission()

    async def invalidate_prefetch_generation(self) -> None:
        if self._invalidate_generation is not None:
            self._invalidate_generation()

    async def pause_writer(self) -> None:
        if self.writer is not None:
            await self.writer.pause()

    async def drain_producers(self, deadline: float) -> None:
        if self.worker_task is not None:
            await self.worker_task

    async def settle_writer(self, deadline: float) -> dict[str, int]:
        if self.writer is None:
            return {"flushed": 0, "failed": 0, "requeued": 0, "unsettled": 0}
        snapshot = await self.writer.settle_custody()
        if snapshot.invariant_ok:
            unsettled = snapshot.unknown_in_flight_items
        else:
            unsettled = max(
                snapshot.raw_unsettled,
                snapshot.safe_queued_items
                + snapshot.safe_retry_items
                + snapshot.unknown_in_flight_items,
                1,
            )
        return {
            "flushed": snapshot.flushed,
            "failed": snapshot.failed,
            "requeued": snapshot.requeued,
            "unsettled": unsettled,
        }

    async def flush_final(self, deadline: float) -> dict[str, int]:
        if self.writer is None:
            return {"flushed": 0, "failed": 0, "requeued": 0, "unsettled": 0}
        report = await self.writer.flush_final(deadline)
        snapshot = await self.writer.settle_custody()
        unsettled = report.unsettled
        if not _custody_ok(snapshot, final=True):
            unsettled = max(1, unsettled, snapshot.raw_unsettled)
        return {
            "flushed": report.flushed,
            "failed": report.failed,
            "requeued": report.requeued,
            "unsettled": unsettled,
        }


SharedKey = tuple[str, tuple, tuple]
_SHARED: dict[SharedKey, _SharedRuntime] = {}
_COUNTERS = ("flushed", "failed", "requeued", "unsettled")


@dataclass
class _Settlement:
    latest: dict[str, int] = field(default_factory=lambda: dict.fromkeys(_COUNTERS, 0))
    phases: dict = field(default_factory=dict)
    errors: tuple[str, ...] = ()


class _SharedHooks:
    """Owner-loop token gates and scalar-only, per-writer settlement history."""

    def __init__(self, loop, loop_thread):
        self.loop = loop
        self.loop_thread = loop_thread
        self.entry: _SharedRuntime | None = None
        self._hooks: dict[object, HermesRuntimeHooks] = {}
        self._inflight: dict[object, int] = {}
        self.ledger: dict[object, _Settlement] = {}

    def _check(self):
        if asyncio.get_running_loop() is not self.loop or threading.get_ident() != self.loop_thread:
            raise RuntimeManagerError("E_RUNTIME_MANAGER_WRONG_LOOP")

    def register(self, hooks):
        self._check()
        if any(value is hooks for value in self._hooks.values()):
            raise RuntimeManagerError("E_RUNTIME_DESCRIPTOR_CONFLICT")
        token = object()
        self._hooks[token] = hooks
        self._inflight[token] = 0
        self.ledger[token] = _Settlement()
        return token

    def unregister(self, token):
        self._check()
        if self._inflight.get(token, 0):
            raise RuntimeManagerError("E_RUNTIME_CLEANUP_FAILED")
        self._hooks.pop(token, None)
        self._inflight.pop(token, None)

    async def quiesce(self, token):
        while self._inflight.get(token, 0):
            await asyncio.sleep(0)

    async def _invoke(self, token, phase, *args):
        self._check()
        hooks = self._hooks.get(token)
        if hooks is None:
            return None
        self._inflight[token] += 1
        record = self.ledger[token]
        try:
            result = await getattr(hooks, phase)(*args)
            if isinstance(result, dict):
                record.latest = {key: int(result.get(key, 0)) for key in _COUNTERS}
            record.phases[phase] = result
            return result
        except BaseException as exc:
            record.errors += (type(exc).__name__,)
            record.latest["unsettled"] = max(1, record.latest["unsettled"])
            raise
        finally:
            self._inflight[token] -= 1

    async def _fanout(self, phase, *args):
        for token in tuple(self._hooks):
            await self._invoke(token, phase, *args)

    def totals(self):
        return {key: sum(record.latest[key] for record in self.ledger.values()) for key in _COUNTERS}

    async def stop_admission(self):
        await self._fanout("stop_admission")

    async def invalidate_prefetch_generation(self):
        await self._fanout("invalidate_prefetch_generation")

    async def pause_writer(self):
        await self._fanout("pause_writer")

    async def drain_producers(self, deadline):
        if self.entry is not None and self.entry.worker_task is not None:
            await self.entry.worker_task

    async def settle_writer(self, deadline):
        await self._fanout("settle_writer", deadline)
        return self.totals()

    async def flush_final(self, deadline):
        if self.totals()["unsettled"]:
            return self.totals()
        await self._fanout("flush_final", deadline)
        return self.totals()


def _custody_ok(snapshot, *, final=False):
    return snapshot.invariant_ok and snapshot.unknown_in_flight_items == 0 and (
        not final or (
            snapshot.raw_unsettled == 0
            and snapshot.safe_queued_items == 0
            and snapshot.safe_retry_items == 0
        )
    )


@dataclass
class _SharedRuntime:
    descriptor: RuntimeDescriptor
    layout: object
    config: RuntimeConfig
    manager: RuntimeManager
    loop: asyncio.AbstractEventLoop
    loop_thread: int
    start_outbox: bool
    hooks: _SharedHooks
    leases: int = 0
    factory: RuntimeFactory | None = None
    worker_started: bool = False
    worker_task: asyncio.Task | None = None
    closing: bool = False
    poisoned: bool = False
    close_task: asyncio.Task | None = None
    graph_loaded: bool = False
    clock: object = None
    sleep: object = None


def _key(descriptor):
    return descriptor.profile_id, descriptor.layout_identity, descriptor.config_identity


def _resource_evidence(layout):
    paths = _resource_paths(layout)
    chains = {}
    try:
        for path in paths:
            chain = inspect_component_chain_nofollow(path, anchor=Path("/"))
            _reject_unsafe({Path(item.lexical_path): (item.kind, item.device, item.inode, item.mode, item.nlink)
                            for item in chain})
            chains[path] = chain
        evidence = _evidence(paths)
        _reject_unsafe(evidence)
        for path, chain in chains.items():
            if inspect_component_chain_nofollow(path, anchor=Path("/")) != chain:
                raise RuntimeManagerError("E_RUNTIME_RESOURCE_IDENTITY_CHANGED")
        if _evidence(paths) != evidence:
            raise RuntimeManagerError("E_RUNTIME_RESOURCE_IDENTITY_CHANGED")
    except StorageLayoutError as exc:
        code = "E_RUNTIME_RESOURCE_ALIAS" if exc.code in {
            "E_PATH_SYMLINK_PARENT", "E_PATH_SPECIAL_FILE",
        } else "E_RUNTIME_RESOURCE_IDENTITY_CHANGED"
        raise RuntimeManagerError(code) from exc
    except OSError as exc:
        raise RuntimeManagerError("E_RUNTIME_RESOURCE_IDENTITY_CHANGED") from exc
    return evidence, chains


def _admit_resources(layout, reuse):
    candidate, chains = _resource_evidence(layout)
    for entry in _SHARED.values():
        existing, other_chains = _resource_evidence(entry.layout)
        if entry is reuse:
            continue
        for left, li in candidate.items():
            for right, ri in existing.items():
                if _overlap(left, right) or (
                    li[0] != "absent" and ri[0] != "absent" and li[1:3] == ri[1:3]
                ):
                    raise RuntimeManagerError("E_RUNTIME_RESOURCE_ALIAS")
                # Equal physical directories under different spellings reserve
                # their subtrees too. Identical lexical common ancestors are OK.
                for lc in chains[left]:
                    for rc in other_chains[right]:
                        if (lc.kind == rc.kind == "directory" and lc.lexical_path != rc.lexical_path
                                and (lc.device, lc.inode) == (rc.device, rc.inode)):
                            raise RuntimeManagerError("E_RUNTIME_RESOURCE_ALIAS")


async def _join_owned(task):
    """Cleanup is never abandoned by cancellation of an observing waiter."""
    while True:
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            if task.done():
                return task.result()


def _manager_report(result, hooks):
    reports = [outcome.report for outcome in result.outcomes if outcome.report is not None]
    # Shared hook totals already appear in runtime reports. The token ledger is
    # authoritative; only runtime unsettled custody can add a stronger failure.
    counts = hooks.totals()
    for name in _COUNTERS:
        counts[name] = max(counts[name], sum(getattr(report, name) for report in reports))
    retained = set()
    if not result.complete:
        for outcome in result.outcomes:
            if outcome.retained:
                retained.update(outcome.report.retained_resources if outcome.report is not None
                                and outcome.report.retained_resources else ("runtime",))
        if not retained:
            retained.add("runtime")
    phases = {}
    runtime_phases = {str(index): report.phases for index, report in enumerate(reports) if report.phases}
    writer_phases = {str(index): record.phases for index, record in enumerate(hooks.ledger.values()) if record.phases}
    if runtime_phases:
        phases["runtime"] = runtime_phases
    if writer_phases:
        phases["writers"] = writer_phases
    return ShutdownReport(
        **counts,
        phases=phases,
        errors=tuple(error for report in reports for error in report.errors)
        + tuple(error for record in hooks.ledger.values() for error in record.errors),
        code=result.error_code if result.complete else result.error_code or "E_RUNTIME_CLOSE_ALL_FAILED",
        lock_retained=not result.complete,
        retained_resources=tuple(sorted(retained)),
    )


class HermesRuntimeBridge:
    """One provider lease over a process-wide owner-loop runtime reservation."""

    def __init__(
        self,
        *,
        layout,
        settings,
        config: RuntimeConfig,
        hooks: HermesRuntimeHooks,
        profile_id: str,
        write_callback,
        flush_interval: float,
        max_batch: int,
        start_outbox: bool,
        clock=None,
        sleep=None,
    ) -> None:
        self.layout = layout
        self.settings = settings
        self.config = config
        self.hooks = hooks
        self.profile_id = profile_id
        self.write_callback = write_callback
        self.flush_interval = flush_interval
        self.max_batch = max_batch
        self.start_outbox = start_outbox
        self.clock = clock
        self.sleep = sleep
        _get_loop()
        self.manager: RuntimeManager | None = None
        self.factory: RuntimeFactory | None = None
        self.runtime: CMMSRuntime | None = None
        self.writer: WriterQueue | None = None
        self.worker_task: asyncio.Task | None = None
        self._entry: _SharedRuntime | None = None
        self._held = False
        self._token = None
        self._published = False
        self._start_task: asyncio.Task | None = None
        self._close_task: asyncio.Task | None = None
        self._close_requested = False
        self._last_report: ShutdownReport | None = None

    def _acquire(self):
        try:
            running = asyncio.get_running_loop()
        except RuntimeError as exc:
            raise RuntimeManagerError("E_RUNTIME_MANAGER_WRONG_LOOP") from exc
        ident = threading.get_ident()
        with _loop_lock:
            if (running is not _loop or not running.is_running() or _loop_thread is None
                    or ident != _loop_thread.ident):
                raise RuntimeManagerError("E_RUNTIME_MANAGER_WRONG_LOOP")
            descriptor = RuntimeDescriptor.from_layout(self.profile_id, layout=self.layout, config=self.config)
            key = _key(descriptor)
            entry = _SHARED.get(key)
            for registered in _SHARED.values():
                if registered.descriptor.profile_id == self.profile_id and registered is not entry:
                    raise RuntimeManagerError("E_RUNTIME_DESCRIPTOR_CONFLICT")
            if entry is not None:
                if running is not entry.loop or ident != entry.loop_thread:
                    raise RuntimeManagerError("E_RUNTIME_MANAGER_WRONG_LOOP")
                if entry.closing or entry.poisoned:
                    raise RuntimeManagerError("E_RUNTIME_START_FAILED")
                if (entry.start_outbox != self.start_outbox or entry.clock is not self.clock
                        or entry.sleep is not self.sleep):
                    raise RuntimeManagerError("E_RUNTIME_DESCRIPTOR_CONFLICT")
            _admit_resources(self.layout, entry)
            if entry is None:
                shared_hooks = _SharedHooks(running, ident)
                # The factory captures only this entry's frozen composition;
                # no provider, bridge or provider callback is retained.
                def factory_builder(value, **kwargs):
                    overrides = None if entry.start_outbox else {"outbox_worker": lambda runtime: None}
                    runtime_kwargs = {}
                    if entry.clock is not None:
                        runtime_kwargs["clock"] = entry.clock
                    if entry.sleep is not None:
                        runtime_kwargs["sleep"] = entry.sleep
                    entry.factory = RuntimeFactory.create(value, overrides=overrides, **kwargs, **runtime_kwargs)
                    return entry.factory

                manager = RuntimeManager(loop=running, factory_builder=factory_builder)
                entry = _SharedRuntime(descriptor, self.layout, self.config, manager, running, ident,
                                       self.start_outbox, shared_hooks, clock=self.clock, sleep=self.sleep)
                shared_hooks.entry = entry
                _SHARED[key] = entry
            entry.leases += 1
            self._held = True
            self._entry = entry
            self.manager = entry.manager
            return entry

    async def start(self) -> CMMSRuntime:
        if self._close_requested:
            raise RuntimeManagerError("E_RUNTIME_START_FAILED")
        if self._start_task is None:
            self._start_task = asyncio.create_task(self._start())
        try:
            return await asyncio.shield(self._start_task)
        except asyncio.CancelledError:
            self._close_requested = True
            try:
                await _join_owned(self._start_task)
            except BaseException:
                pass
            await self.close()
            raise

    async def _start(self):
        try:
            entry = self._acquire()
            runtime = await entry.manager.get(entry.descriptor, layout=entry.layout, config=entry.config,
                                              dependencies=RuntimeDependencies(hooks=entry.hooks))
            if runtime.state != "READY":
                raise RuntimeNotReadyError()
            with _loop_lock:
                if entry.poisoned or entry.closing:
                    raise RuntimeManagerError("E_RUNTIME_START_FAILED")
                if not entry.graph_loaded:
                    if runtime.dependencies.graph is not None:
                        runtime.dependencies.graph.load_snapshot()
                    entry.graph_loaded = True
                if not entry.worker_started:
                    entry.worker_started = True
                    try:
                        worker = runtime.dependencies.outbox_worker
                        if worker is not None:
                            entry.worker_task = runtime.spawn_owned_task(worker.run, kind="hermes-worker-parent")
                    except BaseException:
                        entry.poisoned = True
                        raise
            writer = WriterQueue(self.write_callback, self.flush_interval, self.max_batch)
            self._token = entry.hooks.register(self.hooks)
            self.writer = writer
            self.hooks.writer = writer
            await writer.start()
            if self._close_requested:
                raise RuntimeManagerError("E_RUNTIME_START_FAILED")
            self.runtime = runtime
            self.factory = entry.factory
            self.worker_task = entry.worker_task
            self._published = True
            return runtime
        except BaseException:
            report = await _join_owned(asyncio.create_task(self._release()))
            if report.lock_retained or report.retained_resources:
                with _loop_lock:
                    if self._entry is not None:
                        self._entry.poisoned = True
                        self._entry.closing = True
            raise

    async def close(self) -> ShutdownReport:
        self._close_requested = True
        if self._close_task is None or (
            self._close_task.done() and self._held and self._entry is not None and not self._entry.poisoned
        ):
            self._close_task = asyncio.create_task(self._close())
        return await _join_owned(self._close_task)

    async def _close(self):
        if self._start_task is not None and not self._start_task.done():
            try:
                await _join_owned(self._start_task)
            except BaseException:
                pass
        return await self._release()

    async def _remove_writer(self, entry, *, last):
        if self.writer is None:
            return ShutdownReport()
        token = self._token
        record = entry.hooks.ledger[token]
        deadline = time.monotonic() + 25
        record.errors = ()
        try:
            async with asyncio.timeout_at(asyncio.get_running_loop().time() + 25):
                self.writer.stop_admission()
                await entry.hooks._invoke(token, "invalidate_prefetch_generation")
                await self.writer.pause()
                await entry.hooks.quiesce(token)
                snapshot = await self.writer.settle_custody()
                record.phases["settle"] = snapshot
                record.latest = {key: getattr(snapshot, key) for key in _COUNTERS if key != "unsettled"}
                record.latest["unsettled"] = snapshot.unknown_in_flight_items
                if not _custody_ok(snapshot):
                    record.latest["unsettled"] = max(1, snapshot.raw_unsettled)
                    raise RuntimeManagerError("E_RUNTIME_CLEANUP_FAILED")
                report = await self.writer.flush_final(deadline)
                record.phases["flush"] = report
                record.latest = {key: getattr(report, key) for key in _COUNTERS}
                snapshot = await self.writer.settle_custody()
                record.phases["final_custody"] = snapshot
                if report.unsettled or not _custody_ok(snapshot, final=True):
                    record.latest["unsettled"] = max(1, report.unsettled, snapshot.raw_unsettled)
                    raise RuntimeManagerError("E_RUNTIME_CLEANUP_FAILED")
                if report.failed and not last:
                    raise RuntimeManagerError("E_RUNTIME_CLEANUP_FAILED")
                await entry.hooks.quiesce(token)
                entry.hooks.unregister(token)
                self.hooks.writer = None
                self.hooks._invalidate_generation = None
                self.writer = None
                self.write_callback = None
                self._token = None
                return ShutdownReport(**record.latest)
        except BaseException as exc:
            record.errors += (type(exc).__name__,)
            return ShutdownReport(**record.latest, code="E_RUNTIME_CLEANUP_FAILED", lock_retained=True,
                                  retained_resources=("writer",), errors=record.errors)

    async def _release(self):
        while True:
            with _loop_lock:
                entry = self._entry
                if not self._held or entry is None:
                    return self._last_report or ShutdownReport()
                if entry.poisoned and self._last_report is not None:
                    return self._last_report
                previous = entry.close_task
                if previous is None or previous.done():
                    last = entry.leases == 1
                    entry.closing = True
                    entry.close_task = asyncio.current_task()
                    break
            await _join_owned(previous)
        report = await self._remove_writer(entry, last=last)
        if report.lock_retained:
            with _loop_lock:
                entry.poisoned = entry.poisoned or last
                entry.closing = last
                entry.close_task = None
                self._last_report = report
            return report
        with _loop_lock:
            entry.leases -= 1
            self._held = False
            self._published = False
            if not last:
                entry.closing = False
                entry.close_task = None
                self._last_report = report
                return report
        try:
            result = await entry.manager.close_all()
            report = _manager_report(result, entry.hooks)
            with _loop_lock:
                if result.complete:
                    _SHARED.pop(_key(entry.descriptor), None)
                else:
                    entry.poisoned = True
        except BaseException as exc:
            report = ShutdownReport(code="E_RUNTIME_CLOSE_ALL_FAILED", lock_retained=True,
                                    retained_resources=("runtime",), errors=(type(exc).__name__,))
            with _loop_lock:
                entry.poisoned = True
        self._last_report = report
        return report

    def spawn_prefetch(self, coroutine_factory) -> asyncio.Task:
        runtime = self.runtime
        if runtime is None or runtime.state != "READY":
            raise RuntimeNotReadyError()
        return runtime.spawn_owned_task(coroutine_factory, kind="hermes-prefetch")
