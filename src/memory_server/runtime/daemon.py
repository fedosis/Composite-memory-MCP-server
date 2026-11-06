"""Single-loop daemon leases over the existing manager, factory and custody.

The lock protects only the in-process publication/admission table. It is NOT
storage authority. RuntimeManager.get and CMMSRuntime.admit remain mandatory;
EX ownership and resource disposal are exclusively the existing runtime's job.
"""
from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from types import MappingProxyType

from memory_server.application.profile_bundle import BundlePolicies, ProfileBundle
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import (
    ProfileBinding,
    ProfileRegistry,
    ProfileSession,
    RequestContext,
    RuntimeObservation,
)
from memory_server.paths import StorageLayoutError
from memory_server.runtime.errors import StorageOwnershipError
from memory_server.runtime.runtime import CMMSRuntime, NativeSettlementTimeout, ShutdownReport
from memory_server.runtime.runtime_manager import CloseAllReport, ProfileCloseOutcome, RuntimeManager

_DRAIN_SECONDS = 30.0


class LeaseError(Exception):
    """Never retain input, exception text, paths or credentials in public errors."""

    def __init__(self, code: d.ErrorCode) -> None:
        self.code = code
        super().__init__(code)


def _error(exc: BaseException) -> LeaseError:
    code = getattr(exc, "code", None)
    if code in d.ErrorCode.__args__:
        return LeaseError(code)
    cause = getattr(exc, "cause_code", None)
    if code == "E_LOCK_TIMEOUT" or cause == "E_LOCK_TIMEOUT" or (
        getattr(getattr(exc, "cause", None), "code", None) == "E_LOCK_TIMEOUT"
    ):
        return LeaseError("E_WRITER_ACTIVE")
    if isinstance(exc, (StorageOwnershipError, StorageLayoutError)) or code in {
        "E_RUNTIME_RESOURCE_ALIAS", "E_RUNTIME_RESOURCE_IDENTITY_CHANGED", "E_LOCK_RELEASE_UNCERTAIN",
    }:
        return LeaseError("E_STORAGE")
    if code == "E_RUNTIME_MANAGER_CLOSED":
        return LeaseError("E_RUNTIME_CLOSING")
    return LeaseError("E_RUNTIME_FAILED")


@dataclass
class _Entry:
    binding: ProfileBinding
    task: asyncio.Task | None = None
    runtime: CMMSRuntime | None = None
    bundle: ProfileBundle | None = None
    admissions: int = 0
    stopping: bool = False
    failed: bool = False
    close_task: asyncio.Task | None = None
    report: ShutdownReport | None = None


@dataclass
class _Held:
    entry: _Entry
    session: ProfileSession
    token: asyncio.Task
    depth: int = 1


_LEASE_CONTEXT = ContextVar("daemon_projection_lease", default=None)


@dataclass(frozen=True)
class _ProjectionEnvelope:
    value: object
    scope: object


@dataclass(frozen=True)
class _ProjectionResult:
    value: object
    _handle: object | None = field(default=None, repr=False, compare=False)

    async def result(self):
        if self._handle is not None and not await self._handle.drain(time.monotonic() + _DRAIN_SECONDS):
            raise NativeSettlementTimeout("native settlement timeout")
        return self.value

    async def drain(self, deadline: float) -> bool:
        if self._handle is None:
            return True
        return await self._handle.drain(deadline)

    @property
    def terminated(self) -> bool:
        return self._handle is None or bool(self._handle.terminated)


def _projection_lease(daemon, profile_id):
    held = daemon._held.get(asyncio.current_task(), {}).get(profile_id)
    return getattr(held, "projection_proof", _LEASE_CONTEXT.get())


class _ProjectionAdapter:
    def __init__(self, manager):
        self.manager = manager

    def _refresh_projection_result(self, binding, envelope):
        if envelope.scope.records:
            receipt = envelope.scope.records[-1]
            self.manager.refresh_projection_identity(
                binding.descriptor, receipt=receipt, proof=envelope.scope.proof or receipt.proof
            )
        else:
            self.manager._abort_projection_call(envelope.scope)

    def submitter(self, runtime, binding):
        from memory_server.providers.graph_provider import _PROJECTION_CALL, _close_publication

        async def submit(function, /, *args, kind, **kwargs):
            scope = self.manager._prepare_projection_call(binding, native=True)

            def wrapped():
                token = _PROJECTION_CALL.set(scope)
                try:
                    value = function(*args, **kwargs)
                    scope.sealed = True
                    return _ProjectionEnvelope(value, scope)
                except BaseException:
                    scope.abandoned = True
                    for receipt in scope.records:
                        _close_publication(receipt)
                    raise
                finally:
                    _PROJECTION_CALL.reset(token)

            def cleanup(future):
                if scope.abandoned:
                    self.manager._abort_projection_call(scope)

            raw_handle = None
            try:
                raw_handle = await runtime.submit_native(wrapped, kind=kind)
                raw_handle.async_result.add_done_callback(cleanup)
                envelope = await raw_handle.result()
                self._refresh_projection_result(binding, envelope)
                return _ProjectionResult(envelope.value, raw_handle)
            except BaseException:
                scope.abandoned = True
                if raw_handle is None or raw_handle.async_result.done():
                    self.manager._abort_projection_call(scope)
                raise

        return submit


class Daemon:
    """Construct inside its one running loop; never creates another manager.

    Auth/permission verification precedes this seam (C2); registry.resolve still
    enforces profile scope. S06 persists disable with its generation CAS, then
    calls stop_profile; re-enable does not override a retained owner.
    """

    def __init__(self, registry: ProfileRegistry, *, policies: BundlePolicies,
                 manager: RuntimeManager | None = None, clock=time.monotonic) -> None:
        self.registry = registry
        self.manager = manager if manager is not None else RuntimeManager()
        self._loop = asyncio.get_running_loop()
        self._lock = asyncio.Lock()
        self._entries: dict[str, _Entry] = {}
        self._held: dict[asyncio.Task, dict[str, _Held]] = {}
        self._policies = policies
        self._clock = clock
        self._closing = False
        self._close_task: asyncio.Task | None = None

    def _check(self) -> None:
        if asyncio.get_running_loop() is not self._loop:
            raise LeaseError("E_RUNTIME_NOT_READY")
        if self._closing:
            raise LeaseError("E_RUNTIME_CLOSING")

    async def _compose(self, entry: _Entry) -> ProfileBundle:
        binding = entry.binding
        try:
            runtime = await self.manager.get(binding.descriptor, layout=binding.layout, config=binding.config)
            entry.runtime = runtime
            graph = runtime.dependencies.graph
            if graph is not None and getattr(graph, "_snapshot_path", None) is not None:
                publisher = self.manager.bind_projection_publisher(
                    binding.descriptor,
                    graph=graph,
                    lease_probe=lambda: _projection_lease(self, binding.profile.id),
                )
                worker = runtime.dependencies.outbox_worker
                if worker is not None:
                    if worker._native_submitter != runtime.submit_native:
                        from memory_server.runtime.runtime_manager import RuntimeManagerError

                        raise RuntimeManagerError("E_RUNTIME_DESCRIPTOR_CONFLICT") from None
                    worker._native_submitter = _ProjectionAdapter(self.manager).submitter(runtime, publisher)
            bundle = await ProfileBundle.compose(runtime, self._policies)
            # Publication is atomic under the same table lock as shutdown/stop.
            async with self._lock:
                if self._closing or entry.stopping:
                    raise LeaseError("E_RUNTIME_CLOSING")
                entry.bundle = bundle
            return bundle
        except BaseException:
            # A failed composition is never published. Close through the real
            # manager; a failed close retains this entry and the original EX.
            report = await self.manager.stop_profile(binding.profile.id)
            entry.report = report
            entry.failed = report is None or report.lock_retained or bool(report.retained_resources)
            raise

    @asynccontextmanager
    async def acquire(self, ctx: RequestContext):
        self._check()
        if ctx.profile_id is None:
            raise LeaseError("E_VALIDATION")
        if ctx.deadline <= self._clock():
            raise LeaseError("E_TIMEOUT")
        try:
            async with self._lock:
                self._check()
                binding = await self.registry.resolve(ctx.principal, ctx.profile_id)
                entry = self._entries.get(ctx.profile_id)
                if entry is not None and entry.failed:
                    raise LeaseError("E_RUNTIME_FAILED")
                if entry is not None and entry.stopping:
                    raise LeaseError("E_PROFILE_DISABLED")
                if entry is None or (entry.task.done() and entry.bundle is None and not entry.failed):
                    entry = _Entry(binding)
                    self._entries[ctx.profile_id] = entry
                    entry.task = asyncio.create_task(self._compose(entry))
                    # Retrieve failure even if every waiter is cancelled.
                    entry.task.add_done_callback(lambda t: None if t.cancelled() else t.exception())
                task = entry.task
            async with asyncio.timeout_at(ctx.deadline):
                bundle = await asyncio.shield(task)
            async with self._lock:
                self._check()
                if entry.stopping:
                    raise LeaseError("E_PROFILE_DISABLED")
                # Reuse admission authority on EVERY acquire, not just startup.
                runtime = await self.manager.get(binding.descriptor, layout=binding.layout, config=binding.config)
                caller = asyncio.current_task()
                held = self._held.setdefault(caller, {}).get(ctx.profile_id)
                if held is None:
                    token = runtime.admit()
                    session = ProfileSession(binding, bundle.application, runtime)
                    held = _Held(entry, session, token)
                    self._held[caller][ctx.profile_id] = held
                    entry.admissions += 1
                    held.projection_proof = self.manager._issue_lease_proof(
                        binding.descriptor,
                        owner=caller,
                        held=held,
                        validate=lambda: (
                            self._held.get(caller, {}).get(ctx.profile_id) is held
                            and held.depth > 0
                            and held.session.binding.descriptor == binding.descriptor
                            and held.session.runtime is runtime
                            and held.token in runtime._admitted
                        ),
                    )
                else:
                    held.depth += 1
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            raise LeaseError("E_TIMEOUT") from None
        except Exception as exc:
            raise _error(exc) from None
        projection_context = _LEASE_CONTEXT.set(held.projection_proof)
        try:
            yield held.session
        finally:
            # Synchronous release cannot be interrupted by a second cancellation.
            _LEASE_CONTEXT.reset(projection_context)
            held.depth -= 1
            if held.depth == 0:
                self.manager._revoke_lease_proof(held.projection_proof)
                runtime.release_admitted(held.token)
                entry.admissions -= 1
                del self._held[caller][ctx.profile_id]
                if not self._held[caller]:
                    del self._held[caller]
                if entry.stopping and not entry.admissions:
                    self._begin_stop(entry)
                    await asyncio.shield(entry.close_task)

    def _begin_stop(self, entry: _Entry) -> None:
        if entry.close_task is not None:
            return

        async def close():
            if entry.task is not None and not entry.task.done():
                try:
                    await asyncio.shield(entry.task)
                except BaseException:
                    pass  # Original composition failure is retained on entry.task.
            report = await self.manager.stop_profile(entry.binding.profile.id)
            entry.report = report
            entry.failed = report is not None and (
                report.lock_retained or bool(report.retained_resources)
                or report.code not in {None, "E_RUNTIME_FLUSH_FAILED"}
            )
            if not entry.failed:
                async with self._lock:
                    if self._entries.get(entry.binding.profile.id) is entry:
                        del self._entries[entry.binding.profile.id]
            return report

        entry.close_task = asyncio.create_task(close())

    def snapshot(self) -> Mapping[str, RuntimeObservation]:
        """Detached non-opening observations on this daemon's single loop.

        Startup has no published runtime yet: mark that uncertainty retained,
        never mistake an opening entry for a verified absent/unowned profile.
        """
        if asyncio.get_running_loop() is not self._loop:
            raise LeaseError("E_RUNTIME_NOT_READY")
        observations = {}
        for profile_id, entry in self._entries.items():
            runtime = entry.runtime
            owned = runtime is not None and runtime.lock is not None
            starting = entry.task is not None and not entry.task.done()
            failed = entry.failed or (runtime is not None and runtime.state == "FAILED")
            if entry.task is not None and entry.task.done():
                failed = failed or entry.task.cancelled() or entry.task.exception() is not None
            stopping = self._closing or entry.stopping
            report = entry.report
            retained = (starting and runtime is None) or bool(
                report is not None and (report.lock_retained or report.retained_resources or report.unsettled)
            ) or (failed and owned)
            state = "not_started"
            if failed:
                state = "failed"
            elif stopping or (runtime is not None and runtime.state == "CLOSING"):
                state = "closing"
            elif entry.bundle is not None and runtime is not None and runtime.state == "READY":
                state = "ready"
            observations[profile_id] = RuntimeObservation(state, owned, entry.admissions, stopping, retained)
        return MappingProxyType(observations)

    async def stop_profile(self, profile_id: str) -> ShutdownReport | None:
        """Close admission before awaiting; retain owner while callers hold it."""
        if asyncio.get_running_loop() is not self._loop:
            raise LeaseError("E_RUNTIME_NOT_READY")
        # Single-loop synchronous table access: even a contended publication
        # lock must not postpone closing admission past the first await.
        entry = self._entries.get(profile_id)
        if entry is None:
            return None
        entry.stopping = True
        if entry.admissions:
            return None
        self._begin_stop(entry)
        return await asyncio.shield(entry.close_task)

    async def close_all(self) -> CloseAllReport:
        if asyncio.get_running_loop() is not self._loop:
            raise LeaseError("E_RUNTIME_NOT_READY")
        # No await before admission closes. All later acquire paths recheck it.
        self._closing = True
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close_owned())
        return await asyncio.shield(self._close_task)

    async def _close_owned(self) -> CloseAllReport:
        async with self._lock:
            entries = tuple(reversed(tuple(self._entries.values())))
            for entry in entries:
                entry.stopping = True
        outcomes = []
        for entry in entries:
            self._begin_stop(entry)
            try:
                report = await asyncio.shield(entry.close_task)
                outcomes.append(ProfileCloseOutcome(entry.binding.profile.id, report,
                                                   report.code if report else None, entry.failed))
            except BaseException:
                entry.failed = True
                outcomes.append(ProfileCloseOutcome(entry.binding.profile.id, None, "E_RUNTIME_FAILED", True))
        # Seal the manager too, including any retained/unpublished startup owner.
        manager_report = await self.manager.close_all()
        retained = any(outcome.retained for outcome in outcomes) or not manager_report.complete
        error = next((item.error_code for item in outcomes if item.error_code), manager_report.error_code)
        return CloseAllReport(tuple(outcomes), True, not retained, error)
