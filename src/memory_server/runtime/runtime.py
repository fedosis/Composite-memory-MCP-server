"""Single-loop lifecycle and bounded S1 cleanup, independent of adapters.

Exclusivity is guaranteed only against cooperating, lock-aware binaries.
Pre-lock/shared-only binaries and direct lock-ignorant writers remain outside
that guarantee. Operator quiescence and launch fencing are prerequisites.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import copy
import inspect
import time
from collections.abc import Mapping
from dataclasses import dataclass, field, fields, is_dataclass, replace
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable
from uuid import uuid4

from memory_server.runtime.dependencies import RuntimeDependencies
from memory_server.runtime.errors import (
    CMMSRuntimeError,
    RuntimeCleanupFailed,
    RuntimeClosingError,
    RuntimeFailedError,
    RuntimeNotReadyError,
    RuntimeShutdownTimeout,
    StorageOwnershipError,
)
from memory_server.storage_lock import RuntimeStorageLock, StorageLockError


def _freeze(value: Any) -> Any:
    if is_dataclass(value):
        value = {f.name: getattr(value, f.name) for f in fields(value)}
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (tuple, list)):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_freeze(item) for item in value)
    return copy.deepcopy(value)


@dataclass
class ShutdownReport:
    phases: dict[str, Any] = field(default_factory=dict)
    flushed: int = 0
    failed: int = 0
    requeued: int = 0
    unsettled: int = 0
    retained_resources: tuple[str, ...] = ()
    lock_retained: bool = False
    code: str | None = None
    errors: tuple[str, ...] = ()


class CMMSRuntime:
    """Resources, tasks and uncertain outcomes stay owned until proven safe.

    Recipes accept this runtime and return a constructed resource; a recipe
    must clean up its own unpublished construction failure. Once returned, the
    runtime records the handle before any further await. No concrete provider
    recipe, settings read or transport import lives here.
    """

    def __init__(
        self,
        layout: Any,
        dependencies: RuntimeDependencies | None = None,
        *,
        config: Any = None,
        profile_id: str | None = None,
        acquire_lock: Callable[..., Any] | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Any] = asyncio.sleep,
    ) -> None:
        self.layout = layout  # StorageLayout is already frozen; Path is immutable.
        root = layout if isinstance(layout, (str, Path)) else getattr(layout, "data_root", None)
        if root is None:
            root = layout.root
        self.root = Path(root)
        self.config = _freeze(config or {})
        self.runtime_id = uuid4().hex
        self.profile_id = profile_id or str(self.root)
        self.dependencies = dependencies or RuntimeDependencies()
        self._acquire_lock = acquire_lock or RuntimeStorageLock.acquire_exclusive_async
        self._clock, self._sleep = clock, sleep
        self.state = "NEW"
        self.lock: Any = None
        self._start_task: asyncio.Task | None = None
        self._cleanup_task: asyncio.Task | None = None
        self._admitted: set[asyncio.Task] = set()
        self._executors: dict[Any, asyncio.Future] = {}
        self._built: list[tuple[str, Any]] = []
        self._closed: set[str] = set()
        self._steps: dict[str, asyncio.Task] = {}
        self._report: ShutdownReport | None = None
        self._start_error: BaseException | None = None
        self._rollback = False
        self._restart_safe = False
        self._flush_result: Any = None

    @classmethod
    def create(cls, layout: Any, dependencies: RuntimeDependencies | None = None, **kwargs: Any) -> CMMSRuntime:
        return cls(layout, dependencies, **kwargs)

    @property
    def started(self) -> bool:
        return self.state == "READY"

    async def start(self) -> CMMSRuntime:
        if self.state == "READY":
            return self
        if self.state == "CLOSING":
            raise RuntimeClosingError()
        if self.state == "CLOSED" or (self.state == "FAILED" and not self._restart_safe):
            raise RuntimeFailedError()
        if self.state == "FAILED":
            self._start_task = self._cleanup_task = None
            self._built.clear()
            self._closed.clear()
            self._steps.clear()
            self._start_error = None
            self._rollback = self._restart_safe = False
        if self._start_task is None:
            self.state = "STARTING"
            self._start_task = asyncio.create_task(self._start())
        try:
            return await asyncio.shield(self._start_task)
        except asyncio.CancelledError:
            # A vanished caller cannot abandon a future lock grant. The retained
            # cleanup task settles startup even if nobody calls close again.
            self._begin_close()
            raise

    async def _start(self) -> CMMSRuntime:
        try:
            try:
                self.lock = await self._acquire_lock(self.root)
            except StorageLockError as exc:
                raise StorageOwnershipError(exc) from exc
            self._built = [
                (n, getattr(self.dependencies, n))
                for n in ("sqlite", "graph", "embedder", "vector")
                if getattr(self.dependencies, n) is not None
            ]
            for name, build in self.dependencies.recipes:
                if self.state != "STARTING":
                    raise RuntimeClosingError()
                resource = await self._maybe(build(self))
                self._built.append((name, resource))
                self.dependencies = replace(self.dependencies, **{name: resource})
            if self.state != "STARTING":
                raise RuntimeClosingError()
            self.state = "READY"
            return self
        except BaseException as exc:
            self._start_error = exc
            if self.state != "CLOSING":
                self._rollback = True
                self.state = "CLOSING"
                self._cleanup_task = asyncio.create_task(self._cleanup(self._clock(), rollback=True))
                report = await asyncio.shield(self._cleanup_task)
                exc.cleanup_report = report
                self._restart_safe = report.code is None
                self.state = "FAILED"
            raise

    def _check_admission(self) -> None:
        if self.state in {"NEW", "STARTING"}:
            raise RuntimeNotReadyError()
        if self.state in {"CLOSING", "CLOSED"}:
            raise RuntimeClosingError()
        if self.state == "FAILED":
            raise RuntimeFailedError()

    def admit(self) -> asyncio.Task:
        self._check_admission()
        task = asyncio.current_task()
        if task is None:
            raise RuntimeNotReadyError()
        self.register_admitted(task)
        return task

    def release_admitted(self, token: asyncio.Task) -> None:
        """Release an operation token when its operation, not host task, ends."""
        self._admitted.discard(token)

    def register_admitted(self, task: asyncio.Task) -> None:
        self._check_admission()
        self._admitted.add(task)

    def register_executor(self, future: Any) -> None:
        self._check_admission()
        # Never cancel the wrapper: its done() does not establish native exit.
        self._executors[future] = (
            asyncio.wrap_future(future) if isinstance(future, concurrent.futures.Future) else future
        )

    def _begin_close(self) -> None:
        if self.state == "CLOSED":
            return
        if self._cleanup_task is None or self._cleanup_task.done():
            self.state = "CLOSING"
            t0 = self._clock()  # scheduling delay counts from the state transition
            self._cleanup_task = asyncio.create_task(self._cleanup(t0, rollback=self._rollback))

    async def close(self) -> ShutdownReport:
        if self.state == "CLOSED":
            return self._report
        if self.state == "NEW":
            self.state, self._report = "CLOSED", ShutdownReport()
            return self._report
        self._begin_close()
        return await asyncio.shield(self._cleanup_task)

    @staticmethod
    async def _maybe(value: Any) -> Any:
        return await value if inspect.isawaitable(value) else value

    def _step(self, key: str, fn: Callable | None, *args: Any) -> asyncio.Task:
        if key not in self._steps:

            async def invoke():
                return await self._maybe(fn(*args)) if fn else None

            self._steps[key] = asyncio.create_task(invoke())
        return self._steps[key]

    async def _wait(self, tasks: set[asyncio.Future], end: float) -> bool:
        """A timer is only a ceiling: no mandatory sleep, no cancellation join."""
        live = {task for task in tasks if not task.done()}
        if not live:
            return True
        remaining = end - self._clock()
        if remaining <= 0:
            return False
        timer = asyncio.create_task(self._sleep(remaining))
        try:
            while live and not timer.done():
                await asyncio.wait(live | {timer}, return_when=asyncio.FIRST_COMPLETED)
                live = {task for task in live if not task.done()}
            return not live and self._clock() <= end
        finally:
            timer.cancel()  # owned clock task; never a resource-touching task

    async def _finish_step(self, key: str, fn: Callable | None, end: float, *args: Any, cleanup: bool = False) -> Any:
        error = RuntimeCleanupFailed if cleanup else RuntimeShutdownTimeout
        if self._clock() >= end and key not in self._steps:
            raise error()
        task = self._step(key, fn, *args)
        if not await self._wait({task}, end):
            raise error()
        try:
            return task.result()
        except BaseException:
            # A failed resource close can be retried, but a cancelled/failed
            # flush has an unknown commit outcome and is never replayed here.
            if cleanup:
                del self._steps[key]
            raise

    async def _cleanup(self, t0: float, *, rollback: bool = False) -> ShutdownReport:
        report = ShutdownReport(phases={"closing": t0, "deadlines": (t0 + 10, t0 + 15, t0 + 25, t0 + 30)})
        try:
            if not rollback and self._start_task is not None and not self._start_task.done():
                self._start_task.cancel()
                if not await self._wait({self._start_task}, t0 + 15):
                    raise RuntimeShutdownTimeout()
            if not rollback and self._start_task is not None and self._start_task.done():
                if not self._start_task.cancelled():
                    self._start_task.exception()  # retrieve original failure, never stringify secrets
            hooks = self.dependencies.hooks

            def hook(name):
                return getattr(hooks, name, None)

            if not rollback:
                for name in ("stop_admission", "invalidate_prefetch_generation", "pause_writer"):
                    await self._finish_step(name, hook(name), t0 + 10)
                worker = self.dependencies.outbox_worker
                await self._finish_step("outbox_stop", getattr(worker, "stop", None), t0 + 10)
                drain = self._step("drain", hook("drain_producers"), t0 + 10)
                participants = self._admitted | {drain} | set(self._executors.values())
                await self._wait(participants, t0 + 10)
                report.phases["drain"] = self._clock()
                for task in self._admitted | {drain}:
                    if not task.done():
                        task.cancel()
                await self._wait(participants, t0 + 15)
                live = [task for task in self._admitted | {drain} if not task.done()]
                live += [future for future in self._executors if not future.done()]
                report.errors = tuple(
                    type(t.exception()).__name__
                    for t in participants
                    if t.done() and not t.cancelled() and t.exception() is not None
                )
                report.phases["cancel"] = self._clock()
                if live:
                    report.unsettled = len(live)
                    raise RuntimeShutdownTimeout()
                settled = await self._finish_step("settle", hook("settle_writer"), t0 + 25, t0 + 25)
                if self._count(settled, "unsettled"):
                    report.unsettled = self._count(settled, "unsettled")
                    del self._steps["settle"]  # resume settlement, not an unknown write
                    raise RuntimeShutdownTimeout()
                self._flush_result = await self._finish_step("flush", hook("flush_final"), t0 + 25, t0 + 25)
                for name in ("flushed", "failed", "requeued", "unsettled"):
                    setattr(report, name, self._count(self._flush_result, name))
                report.phases["flush"] = self._clock()
                if report.unsettled:
                    # The call terminated, but its queue ledger did not settle.
                    # Retry must first resume settle_writer on the SAME owner;
                    # flush_final then resumes that ledger, never replays items.
                    # A still-running flush task is retained instead (above).
                    del self._steps["settle"]
                    del self._steps["flush"]
                    raise RuntimeShutdownTimeout()
            order = (
                list(reversed(self._built))
                if rollback
                else [(n, getattr(self.dependencies, n)) for n in ("vector", "embedder", "graph", "sqlite")]
            )
            for name, obj in order:
                if obj is None or name in self._closed or name not in self.dependencies.owns_close:
                    continue
                # OutboxWorker is always a borrower, regardless of ownership flags.
                if name == "outbox_worker":
                    continue
                close = getattr(obj, "close", None) or getattr(obj, "dispose", None)
                await self._finish_step("close:" + name, close, t0 + 30, cleanup=True)
                self._closed.add(name)
                report.phases[name] = self._clock()
            if self.lock is not None:
                try:
                    self.lock.release_exclusive()
                except StorageLockError as exc:
                    raise StorageOwnershipError(exc) from exc
                self.lock = None
            report.code = "E_RUNTIME_FLUSH_FAILED" if report.failed else None
            self.state = "CLOSED"
        except CMMSRuntimeError as exc:
            report.code = exc.code
            self.state = "FAILED"
        except BaseException as exc:
            report.code = "E_RUNTIME_CLEANUP_FAILED"
            report.errors += (type(exc).__name__,)
            self.state = "FAILED"
        report.lock_retained = self.lock is not None
        if self.state == "FAILED":
            report.retained_resources = tuple(n for n, _ in self._built if n not in self._closed)
        self._report = report
        return report

    @staticmethod
    def _count(result: Any, name: str) -> int:
        value = result.get(name, 0) if isinstance(result, Mapping) else getattr(result, name, 0)
        return int(value)

    async def __aenter__(self) -> CMMSRuntime:
        return await self.start()

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()
