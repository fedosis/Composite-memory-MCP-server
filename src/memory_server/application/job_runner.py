"""Runtime-owned job dispatch; startup reconciliation requires external EX custody."""

from __future__ import annotations

import asyncio
import math
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from time import monotonic
from typing import TYPE_CHECKING

from memory_server.control.jobs import JobError, Reservation, TerminalUpdate
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import ProfileLeases, RebuildRunner, ReplayRunner, RequestContext

if TYPE_CHECKING:
    from memory_server.runtime.runtime import CMMSRuntime


class JobCoordinator:
    def __init__(
        self,
        metadata,
        leases: ProfileLeases,
        *,
        authorize,
        replay: ReplayRunner | None = None,
        rebuild: RebuildRunner | None = None,
        events=None,
        execution_seconds=300,
        startup_custody: Callable[..., None] | None = None,
    ):
        if (
            type(execution_seconds) not in (int, float)
            or not math.isfinite(execution_seconds)
            or execution_seconds <= 0
        ):
            raise JobError("E_VALIDATION")
        self.metadata, self.leases, self.authorize = metadata, leases, authorize
        self.replay, self.rebuild, self.events = replay, rebuild, events
        self.execution_seconds = execution_seconds
        self._closing = False
        self._attachments = {}
        self._startup_custody = startup_custody

    @staticmethod
    def _deadline(deadline):
        if type(deadline) not in (float, int) or not math.isfinite(deadline):
            raise JobError("E_VALIDATION")
        if monotonic() >= deadline:
            raise JobError("E_TIMEOUT")

    def _admission(self):
        if self._closing:
            raise JobError("E_RUNTIME_CLOSING")

    async def _handshake(self, attached, deadline):
        cancelled = False
        try:
            while not attached.done():
                self._deadline(deadline)
                try:
                    async with asyncio.timeout_at(deadline):
                        await asyncio.shield(attached)
                except asyncio.CancelledError:
                    # Repeated client cancellation cannot cancel the accepted child.
                    cancelled = True
            attached.result()
        except TimeoutError:
            raise JobError("E_TIMEOUT") from None
        finally:
            if cancelled:
                raise asyncio.CancelledError()

    async def attach(self, ctx: RequestContext, reservation: Reservation) -> None:
        self._admission()
        self._deadline(ctx.deadline)
        await self.authorize(ctx)
        if (ctx.profile_id, ctx.principal.subject) != (reservation.profile_id, reservation.subject):
            raise JobError("E_PERMISSION_DENIED")
        job = self.metadata.get(reservation.job_id, reservation.profile_id, reservation.subject)
        if job is None:
            raise JobError("E_NOT_FOUND")
        runner = self.replay if job.operation == "outbox_replay" else self.rebuild
        if runner is None or not callable(getattr(runner, "run", None)):
            raise JobError("E_CAPABILITY_UNAVAILABLE")
        # Bootstrap remains admitted until the separate child has its own token.
        async with self.leases.acquire(ctx) as bootstrap:
            self._admission()
            existing = self._attachments.get(job.id)
            if existing is not None:
                await self._handshake(existing[1], min(ctx.deadline, monotonic() + 5))
                return
            if len(self._attachments) >= 128:
                raise JobError("E_RATE_LIMIT")
            attached = asyncio.get_running_loop().create_future()
            attached.add_done_callback(lambda f: None if f.cancelled() else f.exception())
            child_ctx = replace(ctx, deadline=monotonic() + self.execution_seconds)
            task = bootstrap.runtime.spawn_owned_task(
                lambda: self._child(child_ctx, reservation, bootstrap, runner, attached), kind="job"
            )
            self._attachments[job.id] = (task, attached)

            def settled(done):
                if not attached.done():
                    attached.set_exception(JobError("E_RUNTIME_CLOSING"))
                if self._attachments.get(job.id, (None,))[0] is done:
                    del self._attachments[job.id]
                if not done.cancelled():
                    done.exception()

            task.add_done_callback(settled)
            await self._handshake(attached, min(ctx.deadline, monotonic() + 5))

    async def _event(self, claim, job):
        if self.events is None or job is None:
            return
        # Publish only the immutable, scoped H01 readback; never runner payloads.
        await self.events(
            d.Event(
                id=job.id,
                type="job.updated",
                profile_id=job.profile_id,
                timestamp=datetime.now(UTC),
                request_id=job.request_id,
                memory_id=None,
                job_id=job.id,
                severity="info",
                data=d.EventData(
                    revision=claim.revision + 1,
                    from_state="running",
                    to_state=job.status,
                    operation=job.operation,
                    pending=None,
                    code=job.error_code,
                ),
            )
        )

    async def _child(self, ctx, reservation, bootstrap, runner, attached):
        claim = None
        try:
            await self.authorize(ctx)
            async with self.leases.acquire(ctx) as session:
                if session.runtime is not bootstrap.runtime or session.binding != bootstrap.binding:
                    raise JobError("E_RUNTIME_NOT_READY")
                self._admission()
                self._deadline(ctx.deadline)
                attached.set_result(None)
                claim = self.metadata.claim(
                    reservation.job_id, ctx.profile_id, ctx.principal.subject, 0, now=datetime.now(UTC)
                )
                if claim is None:
                    return
                try:
                    async with asyncio.timeout_at(ctx.deadline):
                        result = await runner.run(session, claim.job, claim.payload)
                    if type(result) is not d.JobResult:
                        raise JobError("E_INTERNAL")
                    job = self.metadata.complete(claim, result, now=datetime.now(UTC))
                except asyncio.CancelledError:
                    job = self.metadata.fail(claim, "E_JOB_INTERRUPTED", now=datetime.now(UTC))
                    await self._event(claim, job)
                    raise
                except Exception as exc:
                    code = "E_TIMEOUT" if isinstance(exc, TimeoutError) else "E_INTERNAL"
                    if (
                        isinstance(exc, JobError)
                        and type(exc.code) is str
                        and exc.code
                        in frozenset(
                            {
                                "E_TIMEOUT",
                                "E_STORAGE",
                                "E_VECTOR_BACKEND",
                                "E_GRAPH_BACKEND",
                                "E_OUTBOX",
                                "E_PROJECTION_UNAVAILABLE",
                                "E_CAPABILITY_UNAVAILABLE",
                                "E_MAINTENANCE_CONFLICT",
                                "E_VERSION_CONFLICT",
                                "E_RUNTIME_NOT_READY",
                                "E_RUNTIME_CLOSING",
                                "E_RUNTIME_FAILED",
                                "E_JOB_INTERRUPTED",
                                "E_INTERNAL",
                            }
                        )
                    ):
                        code = exc.code
                    job = self.metadata.fail(claim, code, now=datetime.now(UTC))
                await self._event(claim, job)
        except BaseException as exc:
            if not attached.done():
                attached.set_exception(JobError(getattr(exc, "code", "E_RUNTIME_CLOSING")))
            raise

    async def reconcile_startup(
        self, *, profile_id: str | None = None, custody: CMMSRuntime | None = None, deadline: float
    ) -> int:
        """Caller must hold exclusive startup custody; never mounted on the SH daemon path."""
        self._deadline(deadline)
        if profile_id is None or custody is None:
            if self._attachments or any(observation.owned for observation in self.leases.snapshot().values()):
                raise JobError("E_RUNTIME_NOT_READY")
            raise JobError("E_RUNTIME_NOT_READY")
        if self._attachments or self._startup_custody is None:
            raise JobError("E_RUNTIME_NOT_READY")
        count = 0
        while True:
            self._deadline(deadline)
            self._startup_custody(custody, profile_id=profile_id)
            records = self.metadata.scan_reconcile_scoped(profile_id=profile_id, limit=100, deadline=deadline)
            self._deadline(deadline)
            if not records:
                return count
            for record in records:
                self._deadline(deadline)
                claim = record.claim
                if record.claim.job.profile_id != profile_id:
                    raise JobError("E_STORAGE")
                self._startup_custody(custody, profile_id=profile_id)
                job = self.metadata.finish(
                    claim.job.id,
                    claim.job.profile_id,
                    claim.subject,
                    claim.revision,
                    "running",
                    TerminalUpdate("interrupted", None, "E_JOB_INTERRUPTED", datetime.now(UTC), phase="reconcile"),
                )
                count += job is not None
            await asyncio.sleep(0)

    async def close(self, *, deadline: float) -> None:
        # Single-loop admission closes synchronously, before the first await.
        self._closing = True
        if type(deadline) not in (float, int) or not math.isfinite(deadline):
            raise JobError("E_VALIDATION")
        tasks = {entry[0] for entry in self._attachments.values() if not entry[0].done()}
        if not tasks:
            return
        remaining = max(0, deadline - monotonic())
        if remaining:
            _, tasks = await asyncio.wait(tasks, timeout=remaining)
        for task in tasks:
            task.cancel()
        # Do not release runtimes/native ownership: their existing close barrier owns it.
