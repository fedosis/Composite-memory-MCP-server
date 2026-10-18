"""Single-loop lifecycle and bounded S1 cleanup, independent of adapters.

Exclusivity is guaranteed only against cooperating, lock-aware binaries.
Pre-lock/shared-only binaries and direct lock-ignorant writers remain outside
that guarantee. Operator quiescence and launch fencing are prerequisites.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import copy
import inspect
import os
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass, field, fields, is_dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable
from uuid import uuid4

from memory_server.paths import StorageLayoutError
from memory_server.runtime.dependencies import CLOSE_ORDER, RuntimeDependencies
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

# O2/D1 codes. They stay OUTSIDE the closed migration catalogue
# (profile_migration.py:431-448) and are never registered in it.
E_QDRANT_REMOTE_NAMESPACE_UNCONFIRMED = "E_QDRANT_REMOTE_NAMESPACE_UNCONFIRMED"
E_RUNTIME_WRITABLE_SPLIT_LAYOUT = "E_RUNTIME_WRITABLE_SPLIT_LAYOUT"
E_RUNTIME_AMBIGUOUS_SQL_URI = "E_RUNTIME_AMBIGUOUS_SQL_URI"
E_RUNTIME_UNPROVABLE_RESOURCE_OWNERSHIP = "E_RUNTIME_UNPROVABLE_RESOURCE_OWNERSHIP"
_AMBIGUOUS_SQL_KINDS = frozenset({"file_uri", "remote"})


def ensure_layout_eligible(layout: Any) -> None:
    """O2/§2.1 eligibility, evaluated from the frozen layout only (no I/O).

    Called before the EX acquisition and before every recipe/constructor, so a
    refusal can never create a lock entry or open a resource.

    * Remote Qdrant outside profile mode is ALWAYS refused (owner decision Q1):
      no confirmation mechanism exists and none may be invented, so the runtime
      fails closed with ``E_QDRANT_REMOTE_NAMESPACE_UNCONFIRMED``. Profile mode
      keeps its unchanged ``E_QDRANT_PROFILE_NAMESPACE_UNDEFINED`` refusal at
      resolution time (``paths.py:366-367``).
    * A writable ``legacy-split-layout:<label>`` alias is refused; a projection
      that is marked unavailable is NOT one, because it stays unopened
      (``paths.py:325-334``), and ``ephemeral-projection:vector`` is not an
      alias at all (``paths.py:368-369``).
    * An ambiguous/opaque SQL target (``file_uri``/``remote``) and unprovable
      local resource ownership are refused; ``sqlite.kind == "memory"`` stays
      supported.
    """
    if not (hasattr(layout, "mode") or hasattr(layout, "data_root")):
        # A bare root path (the C02 unit-level seam) declares no store, no
        # alias and no ownership claim, so there is nothing to refuse.
        return
    mode = getattr(layout, "mode", None)
    vector = getattr(layout, "vector", None)
    sqlite = getattr(layout, "sqlite", None)
    if (
        vector is not None
        and getattr(vector, "backend", None) == "qdrant"
        and getattr(vector, "kind", None) == "remote"
        and mode != "profile"
    ):
        raise StorageLayoutError(
            E_QDRANT_REMOTE_NAMESPACE_UNCONFIRMED,
            "remote Qdrant has no confirmable namespace exclusivity outside profile mode",
        )
    if sqlite is not None and getattr(sqlite, "kind", None) in _AMBIGUOUS_SQL_KINDS:
        raise StorageLayoutError(
            E_RUNTIME_AMBIGUOUS_SQL_URI,
            f"ambiguous SQLite target kind: {getattr(sqlite, 'kind', None)}",
        )
    if sqlite is not None and getattr(sqlite, "kind", None) == "file" and getattr(sqlite, "local_path", None) is None:
        raise StorageLayoutError(E_RUNTIME_UNPROVABLE_RESOURCE_OWNERSHIP, "sqlite file target has no provable path")
    unavailable = getattr(layout, "unavailable_projections", None) or frozenset()
    for flag in getattr(layout, "compatibility", ()) or ():
        if not str(flag).startswith("legacy-split-layout:"):
            continue  # ephemeral-projection:vector and other flags are not aliases
        if str(flag).split(":", 1)[1] in unavailable:
            continue  # unopened/degraded projection: no writable alias can be opened
        raise StorageLayoutError(
            E_RUNTIME_WRITABLE_SPLIT_LAYOUT,
            f"writable split layout rejected before construction: {flag}",
        )
    if vector is not None and getattr(vector, "kind", None) == "local" and getattr(vector, "local_path", None) is None:
        raise StorageLayoutError(E_RUNTIME_UNPROVABLE_RESOURCE_OWNERSHIP, "local vector target has no provable path")
    if not getattr(layout, "data_root", None):
        raise StorageLayoutError(E_RUNTIME_UNPROVABLE_RESOURCE_OWNERSHIP, "layout owns no provable root")


def _freeze(value: Any) -> Any:
    if is_dataclass(value):
        # A FROZEN dataclass config snapshot (RuntimeConfig) is already deep
        # immutable and must keep attribute access; a plain/nested structure is
        # copied into MappingProxyType as before.
        if getattr(type(value), "__dataclass_params__", None) is not None and type(value).__dataclass_params__.frozen:
            return value
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


_OWNED_LINEAGE: contextvars.ContextVar[str | None] = contextvars.ContextVar("_OWNED_LINEAGE", default=None)

_TASK_LIVE = frozenset({"RESERVED", "ATTACHED", "ATTACH_UNKNOWN"})
_NATIVE_DEAD = "TERMINATED"


@dataclass
class TaskRecord:
    id: str
    kind: str
    state: str
    task: asyncio.Task | None
    pre_close: bool
    terminal_type: str | None


@dataclass
class NativeRecord:
    id: str
    kind: str
    lineage_id: str | None
    state: str
    native_future: concurrent.futures.Future | None
    thread: threading.Thread | None
    async_result: asyncio.Future | None
    reaper: asyncio.Task | None
    pre_close: bool
    capacity_held: bool
    capacity_released: bool
    terminal_type: str | None


class NativeCallHandle:
    """Caller-facing native call; result() never cancels the mirror or thread."""

    def __init__(
        self,
        *,
        native_future: concurrent.futures.Future,
        thread: threading.Thread,
        async_result: asyncio.Future,
        _record: NativeRecord,
    ) -> None:
        self._native_future = native_future
        self._thread = thread
        self._async_result = async_result
        self._record = _record

    @property
    def native_future(self) -> concurrent.futures.Future:
        return self._native_future

    @property
    def thread(self) -> threading.Thread:
        return self._thread

    @property
    def async_result(self) -> asyncio.Future:
        return self._async_result

    async def result(self) -> Any:
        return await asyncio.shield(self._async_result)

    @property
    def terminated(self) -> bool:
        return self._record.state == _NATIVE_DEAD


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
        self._owned_epoch = 0
        self._owned_tasks: dict[str, TaskRecord] = {}
        self._owned_natives: dict[str, NativeRecord] = {}
        self._pre_close_admitted: frozenset[asyncio.Task] = frozenset()
        self._max_native_workers = min(32, (os.cpu_count() or 1) + 4)
        self._native_capacity = asyncio.Semaphore(self._max_native_workers)
        self.spawn_owned_task = self.spawn_owned_task
        self.submit_native = self.submit_native

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
        self.preflight()
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
                self.dependencies = self.dependencies.with_resource(name, resource)
            if self.state != "STARTING":
                raise RuntimeClosingError()
            worker = getattr(self.dependencies, "outbox_worker", None)
            if worker is not None:
                if getattr(worker, "_task_spawner", None) is None or getattr(worker, "_native_submitter", None) is None:
                    raise RuntimeError("runtime-created OutboxWorker missing task_spawner/native_submitter")
            vector = getattr(self.dependencies, "vector", None)
            if vector is not None and type(vector).__name__ == "LanceDBProvider":
                if getattr(vector, "_native_submitter", None) is None:
                    raise RuntimeError("runtime-created LanceDBProvider missing native_submitter")
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

    def preflight(self) -> None:
        """O2/§2.1 eligibility gate.

        Runs in the caller before the retained start task is created, hence
        strictly before the EX acquisition and before every recipe: a refusal
        leaves the runtime in ``NEW``, with no lock entry and no constructed
        resource.
        """
        ensure_layout_eligible(self.layout)

    def register_executor(self, future: Any) -> None:
        self._check_admission()
        # Never cancel the wrapper: its done() does not establish native exit.
        self._executors[future] = (
            asyncio.wrap_future(future) if isinstance(future, concurrent.futures.Future) else future
        )

    def _bump(self) -> None:
        self._owned_epoch += 1

    def owned_snapshot(self) -> tuple[int, tuple[TaskRecord, ...], tuple[NativeRecord, ...]]:
        return (self._owned_epoch, tuple(self._owned_tasks.values()), tuple(self._owned_natives.values()))

    def _live_tasks(self, records) -> list[TaskRecord]:
        return [r for r in records if r.state in _TASK_LIVE]

    def _live_natives(self, records) -> list[NativeRecord]:
        return [r for r in records if r.state != _NATIVE_DEAD]

    def _owned_wait_set(self) -> set[asyncio.Future]:
        wait_set: set[asyncio.Future] = set()
        _, tasks, natives = self.owned_snapshot()
        for rec in self._live_tasks(tasks):
            if rec.task is not None:
                wait_set.add(rec.task)
        for rec in self._live_natives(natives):
            if rec.reaper is not None:
                wait_set.add(rec.reaper)
        return wait_set

    def _lineage_allows_native(self) -> bool:
        rec_id = _OWNED_LINEAGE.get()
        if rec_id is not None:
            rec = self._owned_tasks.get(rec_id)
            if rec is not None and rec.pre_close and rec.state in {"RESERVED", "ATTACHED"}:
                return True
        task = asyncio.current_task()
        if task is not None and task in self._pre_close_admitted:
            return True
        if task is not None:
            for rec in self._owned_tasks.values():
                if rec.task is task and rec.pre_close and rec.state == "ATTACHED":
                    return True
        return False

    def _raise_if_not_ready(self) -> None:
        if self.state in {"CLOSING", "CLOSED"}:
            raise RuntimeClosingError()
        if self.state in {"NEW", "STARTING"}:
            raise RuntimeNotReadyError()
        if self.state == "FAILED":
            raise RuntimeFailedError()
        raise RuntimeNotReadyError()

    def spawn_owned_task(self, coroutine_factory, *, kind: str) -> asyncio.Task:
        if self.state != "READY":
            self._raise_if_not_ready()
        rec = TaskRecord(
            id=uuid4().hex, kind=kind, state="RESERVED", task=None, pre_close=True, terminal_type=None
        )
        self._owned_tasks[rec.id] = rec
        self._bump()
        ctx = contextvars.copy_context()

        def _make():
            _OWNED_LINEAGE.set(rec.id)
            return coroutine_factory()

        try:
            coro = ctx.run(_make)
        except BaseException as exc:
            rec.terminal_type = type(exc).__name__
            del self._owned_tasks[rec.id]
            self._bump()
            raise
        loop = asyncio.get_running_loop()
        try:
            try:
                task = loop.create_task(coro, context=ctx)
            except TypeError:
                task = loop.create_task(coro)
        except BaseException as exc:
            started = inspect.getcoroutinestate(coro) != inspect.CORO_CREATED
            if not started:
                coro.close()
                rec.terminal_type = type(exc).__name__
                del self._owned_tasks[rec.id]
                self._bump()
                raise
            rec.state = "ATTACH_UNKNOWN"
            rec.terminal_type = type(exc).__name__
            self._bump()
            raise
        if rec.task is not None and rec.task is not task:
            raise RuntimeError("duplicate attachment")
        rec.task = task
        rec.state = "ATTACHED"

        def _on_done(t: asyncio.Task) -> None:
            loop.call_soon_threadsafe(self._schedule_task_reap, rec.id)

        task.add_done_callback(_on_done)
        return task

    def _schedule_task_reap(self, rec_id: str) -> None:
        rec = self._owned_tasks.get(rec_id)
        if rec is None or rec.task is None or not rec.task.done():
            return
        if rec.task.cancelled():
            rec.terminal_type = "CancelledError"
        else:
            exc = rec.task.exception()
            rec.terminal_type = None if exc is None else type(exc).__name__
        rec.state = "TERMINATED"
        del self._owned_tasks[rec.id]
        self._bump()

    def _release_capacity_once(self, rec: NativeRecord) -> None:
        if rec.capacity_held and not rec.capacity_released:
            rec.capacity_released = True
            rec.capacity_held = False
            self._native_capacity.release()

    @staticmethod
    def _mirror_call(loop: asyncio.AbstractEventLoop, async_result: asyncio.Future, fn: Callable[[], None]) -> None:
        def _run() -> None:
            if not async_result.done():
                fn()

        loop.call_soon_threadsafe(_run)

    def _native_target(self, rec, function, args, kwargs, ctx, native_future, async_result, loop) -> None:
        if not native_future.set_running_or_notify_cancel():
            self._mirror_call(
                loop, async_result, lambda: async_result.set_exception(concurrent.futures.CancelledError())
            )
            return
        try:
            result = ctx.run(function, *args, **kwargs)
        except BaseException as exc:
            rec.terminal_type = type(exc).__name__
            if not native_future.done():
                native_future.set_exception(exc)
            self._mirror_call(loop, async_result, lambda e=exc: async_result.set_exception(e))
            return
        if not native_future.done():
            native_future.set_result(result)
        self._mirror_call(loop, async_result, lambda r=result: async_result.set_result(r))

    async def _reap_native(self, rec: NativeRecord) -> None:
        if rec.async_result is not None:
            try:
                await asyncio.shield(rec.async_result)
            except BaseException as exc:
                rec.terminal_type = rec.terminal_type or type(exc).__name__
        if (
            rec.native_future is not None
            and rec.native_future.done()
            and rec.thread is not None
            and rec.thread.is_alive()
        ):
            rec.state = "FUTURE_DONE"
        while rec.thread is not None and rec.thread.is_alive():
            rec.thread.join(0)
            if rec.thread.is_alive():
                await asyncio.sleep(0)
        future_terminal = rec.native_future is None or rec.native_future.done()
        dead = rec.thread is None or not rec.thread.is_alive()
        if future_terminal and dead:
            rec.state = "TERMINATED"
            self._release_capacity_once(rec)
            if rec.id in self._owned_natives:
                del self._owned_natives[rec.id]
                self._bump()

    async def submit_native(self, function, /, *args, kind: str, **kwargs) -> NativeCallHandle:
        if self.state == "READY":
            pass
        elif self.state == "CLOSING" and self._lineage_allows_native():
            pass
        elif self.state == "CLOSING":
            raise RuntimeClosingError()
        else:
            self._check_admission()
        rec = NativeRecord(
            id=uuid4().hex,
            kind=kind,
            lineage_id=_OWNED_LINEAGE.get(),
            state="WAITING_CAPACITY",
            native_future=None,
            thread=None,
            async_result=None,
            reaper=None,
            pre_close=True,
            capacity_held=False,
            capacity_released=False,
            terminal_type=None,
        )
        self._owned_natives[rec.id] = rec
        self._bump()
        try:
            await self._native_capacity.acquire()
        except asyncio.CancelledError:
            del self._owned_natives[rec.id]
            self._bump()
            raise
        rec.capacity_held = True
        loop = asyncio.get_running_loop()
        ctx = contextvars.copy_context()
        native_future: concurrent.futures.Future = concurrent.futures.Future()
        async_result = loop.create_future()
        thread = threading.Thread(
            target=self._native_target,
            args=(rec, function, args, kwargs, ctx, native_future, async_result, loop),
            daemon=False,
            name=f"cmms-native-{kind}-{rec.id[:8]}",
        )
        rec.native_future = native_future
        rec.thread = thread
        rec.async_result = async_result
        try:
            rec.reaper = loop.create_task(self._reap_native(rec))
        except BaseException as exc:
            rec.terminal_type = type(exc).__name__
            if not native_future.done():
                native_future.set_exception(exc)
            if not async_result.done():
                async_result.set_exception(exc)
            self._release_capacity_once(rec)
            del self._owned_natives[rec.id]
            self._bump()
            raise
        rec.state = "REGISTERED"
        try:
            thread.start()
        except BaseException as exc:
            clean = thread.ident is None and not thread.is_alive()
            if clean:
                rec.terminal_type = type(exc).__name__
                if not native_future.done():
                    native_future.set_exception(exc)
                if not async_result.done():
                    async_result.set_exception(exc)
                self._release_capacity_once(rec)
                if rec.state == "TERMINATED" and rec.id in self._owned_natives:
                    del self._owned_natives[rec.id]
                    self._bump()
                raise
            rec.state = "START_UNKNOWN"
            rec.terminal_type = type(exc).__name__
            self._bump()
            raise
        handle = NativeCallHandle(
            native_future=native_future, thread=thread, async_result=async_result, _record=rec
        )
        if thread.ident is None:
            rec.state = "START_UNKNOWN"
            self._bump()
            return handle
        rec.state = "RUNNING"
        return handle

    async def _quiesce_owned(self, *, cancel: bool, end: float) -> None:
        while True:
            epoch, tasks, natives = self.owned_snapshot()
            live_t = self._live_tasks(tasks)
            live_n = self._live_natives(natives)
            if cancel:
                for rec in live_t:
                    if rec.task is not None and not rec.task.done():
                        rec.task.cancel()
            wait_set: set[asyncio.Future] = set()
            for rec in live_t:
                if rec.task is not None:
                    wait_set.add(rec.task)
            for rec in live_n:
                if rec.reaper is not None:
                    wait_set.add(rec.reaper)
            ok = await self._wait(wait_set, end)
            if not wait_set and (live_t or live_n):
                remaining = end - self._clock()
                if remaining > 0:
                    await self._sleep(remaining)
                ok = False
            epoch2, tasks2, natives2 = self.owned_snapshot()
            live_t2 = self._live_tasks(tasks2)
            live_n2 = self._live_natives(natives2)
            if not live_t2 and not live_n2 and epoch2 == epoch:
                return
            if (not ok) or self._clock() >= end or (not live_t2 and not live_n2 and epoch2 != epoch):
                if not live_t2 and not live_n2 and epoch2 != epoch:
                    if self._clock() >= end:
                        raise RuntimeShutdownTimeout()
                    if not wait_set:
                        await asyncio.sleep(0)
                    continue
                raise RuntimeShutdownTimeout()
            if not wait_set:
                await asyncio.sleep(0)

    def _begin_close(self) -> None:
        if self.state == "CLOSED":
            return
        if self._cleanup_task is None or self._cleanup_task.done():
            self.state = "CLOSING"
            self._pre_close_admitted = frozenset(self._admitted)
            for rec in self._owned_tasks.values():
                rec.pre_close = True
            for rec in self._owned_natives.values():
                rec.pre_close = True
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
                await self._wait(participants | self._owned_wait_set(), t0 + 10)
                report.phases["drain"] = self._clock()
                for task in self._admitted | {drain}:
                    if not task.done():
                        task.cancel()
                _, owned_tasks, _ = self.owned_snapshot()
                for rec in self._live_tasks(owned_tasks):
                    if rec.task is not None and not rec.task.done():
                        rec.task.cancel()
                await self._wait(participants | self._owned_wait_set(), t0 + 15)
                try:
                    await self._quiesce_owned(cancel=True, end=t0 + 15)
                except RuntimeShutdownTimeout:
                    _, owned_tasks, owned_natives = self.owned_snapshot()
                    live = [task for task in self._admitted | {drain} if not task.done()]
                    live += [future for future in self._executors if not future.done()]
                    live += self._live_tasks(owned_tasks)
                    live += self._live_natives(owned_natives)
                    report.unsettled = len(live)
                    report.phases["cancel"] = self._clock()
                    raise
                live = [task for task in self._admitted | {drain} if not task.done()]
                live += [future for future in self._executors if not future.done()]
                _, owned_tasks, owned_natives = self.owned_snapshot()
                live += self._live_tasks(owned_tasks)
                live += self._live_natives(owned_natives)
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
                else [(n, getattr(self.dependencies, n)) for n in CLOSE_ORDER]
            )
            _, owned_tasks, owned_natives = self.owned_snapshot()
            if self._live_tasks(owned_tasks) or self._live_natives(owned_natives):
                raise RuntimeShutdownTimeout()
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
