"""Hermes composition adapter for the transport-neutral CMMS runtime."""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
from dataclasses import replace
from typing import Callable

from memory_server.plugins.hermes.writer import WriterQueue
from memory_server.runtime import CMMSRuntime, RuntimeDependencies, RuntimeNotReadyError, ShutdownReport
from memory_server.runtime.config import RuntimeConfig
from memory_server.runtime.runtime_factory import RuntimeFactory
from memory_server.runtime.runtime_manager import RuntimeDescriptor, RuntimeManager

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
        loop = asyncio.new_event_loop()
        _loop = loop

        def _run() -> None:
            asyncio.set_event_loop(loop)
            loop.run_forever()

        _loop_thread = threading.Thread(target=_run, daemon=True, name="cmms-provider-loop")
        _loop_thread.start()
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
        return {
            "flushed": report.flushed,
            "failed": report.failed,
            "requeued": report.requeued,
            "unsettled": report.unsettled,
        }


class HermesRuntimeBridge:
    """Own one RuntimeManager while remaining unused by HermesProvider in C1."""

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
        loop = _get_loop()

        def factory_builder(value, **kwargs):
            overrides = None if self.start_outbox else {"outbox_worker": lambda runtime: None}
            runtime_kwargs = {}
            if self.clock is not None:
                runtime_kwargs["clock"] = self.clock
            if self.sleep is not None:
                runtime_kwargs["sleep"] = self.sleep
            factory = RuntimeFactory.create(
                value,
                overrides=overrides,
                **kwargs,
                **runtime_kwargs,
            )
            self.factory = factory
            return factory

        self.manager = RuntimeManager(loop=loop, factory_builder=factory_builder)
        self.factory: RuntimeFactory | None = None
        self.runtime: CMMSRuntime | None = None
        self.writer: WriterQueue | None = None
        self.worker_task: asyncio.Task | None = None
        self._published = False

    async def start(self) -> CMMSRuntime:
        if self._published and self.runtime is not None and self.runtime.state == "READY":
            return self.runtime
        try:
            descriptor = RuntimeDescriptor.from_layout(
                self.profile_id,
                layout=self.layout,
                config=self.config,
            )
            dependencies = RuntimeDependencies(hooks=self.hooks)
            runtime = await self.manager.get(
                descriptor,
                layout=self.layout,
                config=self.config,
                dependencies=dependencies,
            )
            if runtime.state != "READY":
                await self.manager.stop_profile(self.profile_id)
                raise RuntimeNotReadyError()
            self.runtime = runtime
            if runtime.dependencies.graph is not None:
                runtime.dependencies.graph.load_snapshot()
            writer = WriterQueue(self.write_callback, self.flush_interval, self.max_batch)
            self.writer = writer
            self.hooks.writer = writer
            await writer.start()
            if runtime.dependencies.outbox_worker is not None:
                self.worker_task = runtime.spawn_owned_task(
                    runtime.dependencies.outbox_worker.run,
                    kind="hermes-worker-parent",
                )
                self.hooks.worker_task = self.worker_task
            self._published = True
            return runtime
        except BaseException:
            try:
                await self.manager.stop_profile(self.profile_id)
            except BaseException:
                pass
            raise

    async def close(self) -> ShutdownReport:
        report = await self.manager.stop_profile(self.profile_id)
        return report if report is not None else ShutdownReport()

    def spawn_prefetch(self, coroutine_factory) -> asyncio.Task:
        runtime = self.runtime
        if runtime is None or runtime.state != "READY":
            raise RuntimeNotReadyError()
        return runtime.spawn_owned_task(coroutine_factory, kind="hermes-prefetch")
