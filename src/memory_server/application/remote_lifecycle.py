"""Remote lifecycle mutations: one canonical transaction, bounded postcommit tails."""

from __future__ import annotations

import asyncio
import math
import sqlite3
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from time import monotonic
from typing import Literal
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy import event, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from storage.models.belief import BeliefORM
from storage.models.fact import FactORM
from storage.models.lifecycle import LifecycleEventORM
from storage.outbox import OutboxRepository
from storage.repositories.http_request_repo import HttpRequestRepository
from storage.repositories.lifecycle_repo import LifecycleConflictError, normalize_version
from storage.repositories.receipt_repo import ReceiptRepository

from memory_server.application.contracts import LifecycleCommand
from memory_server.application.mutation_uow import (
    MUTATION_REPLY,
    MutationReply,
    MutationUoW,
    _digest,
    _is_busy,
)
from memory_server.control.policy import require_permission
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import AuditOutcome, AuditTicket, GatewayDependencies, RequestContext
from memory_server.services.lifecycle_service import (
    InvalidLifecycleTransitionError,
    LifecycleMemoryNotFoundError,
    LifecyclePropagationLimitError,
)
from memory_server.transports.http.errors import HttpError


def _aware(value):
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def validate_lifecycle_id(value: object) -> str:
    if type(value) is not str or len(value) != 36:
        raise HttpError("E_VALIDATION")
    try:
        if str(UUID(value)) != value:
            raise ValueError
    except (ValueError, TypeError, AttributeError):
        raise HttpError("E_VALIDATION") from None
    return value


def _memory(row, kind, receipt_id):
    return d.Memory(
        id=row.id,
        memory_type=kind,
        content=None,
        payload=None,
        confidence=float(row.confidence),
        lifecycle_state=row.lifecycle_state,
        revision=normalize_version(row.version),
        source_version=row.version,
        source=None,
        creator=None,
        created_at=_aware(row.created_at),
        updated_at=_aware(row.updated_at),
        valid_from=None,
        valid_to=None,
        epistemic_status=None,
        authority_level=None,
        risk_tags=[],
        receipt_id=receipt_id,
        masked=True,
        projection=d.ItemProjection(vector="unknown", graph="unknown", outbox="pending"),
    )


@dataclass(frozen=True)
class _EffectEvent:
    memory_type: Literal["fact", "belief"]
    event: d.Event


async def _settle(awaitable):
    async def bounded():
        try:
            async with asyncio.timeout(1.0):
                return True, await awaitable
        except Exception:
            return False, None

    task = asyncio.create_task(bounded())
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
    return task.result()


def _sqlite_busy(exc: Exception) -> bool:
    original = getattr(exc, "orig", exc)
    if not isinstance(original, sqlite3.OperationalError):
        return False
    code = getattr(original, "sqlite_errorcode", None)
    # Extended codes (e.g. SQLITE_BUSY_SNAPSHOT) share the primary low byte.
    if type(code) is int:
        return code & 0xFF in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED)
    return _is_busy(exc)


class _LifecycleMutationUoW(MutationUoW):
    async def _attempt(self, ctx, command, handler, digest):
        # Journal purge/read and COMMIT are outside the handler's SQL mapper.
        # Classify before MutationUoW.run sanitizes the exception to E_STORAGE.
        try:
            return await super()._attempt(ctx, command, handler, digest)
        except (SQLAlchemyError, sqlite3.Error) as exc:
            if _sqlite_busy(exc):
                raise HttpError("E_REQUEST_IN_PROGRESS") from None
            raise


class LifecycleHandler:
    def __init__(self, scope, ctx, *, ticket: str):
        self._scope, self._ctx, self._ticket = scope, ctx, ticket
        self.committed_events = ()

    async def apply(self, session: AsyncSession, command: d.ProfileMutationCommand) -> d.TransitionResult:
        self.committed_events = ()
        captured = {}

        def capture(sync, flush_context):
            for row in sync.new:
                if isinstance(row, LifecycleEventORM):
                    captured[row.id] = (
                        row.id,
                        row.memory_id,
                        row.memory_type,
                        row.from_state,
                        row.to_state,
                        _aware(row.timestamp),
                    )
                    if len(captured) > 513:
                        raise HttpError("E_INTERNAL")

        event.listen(session.sync_session, "after_flush", capture)
        try:
            return await self._apply(session, command, captured)
        except LifecycleConflictError as exc:
            details = {
                k: v for k, v in (("expected", exc.expected), ("actual", exc.actual)) if type(v) is int and v >= 1
            }
            if exc.memory_id == command.path_ids["id"] or any(
                [
                    await session.scalar(select(model.id).where(model.id == exc.memory_id).limit(1))
                    for model in (FactORM, BeliefORM)
                ]
            ):
                details["id"] = exc.memory_id
            raise HttpError("E_VERSION_CONFLICT", details=details) from None
        except LifecycleMemoryNotFoundError:
            raise HttpError("E_NOT_FOUND") from None
        except InvalidLifecycleTransitionError:
            raise HttpError("E_VALIDATION") from None
        except LifecyclePropagationLimitError:
            raise HttpError("E_STORAGE") from None
        except (SQLAlchemyError, sqlite3.Error) as exc:
            raise HttpError("E_REQUEST_IN_PROGRESS" if _sqlite_busy(exc) else "E_STORAGE") from None
        except (HttpError, asyncio.CancelledError):
            raise
        except Exception:
            raise HttpError("E_INTERNAL") from None
        finally:
            event.remove(session.sync_session, "after_flush", capture)

    async def _apply(self, session, command, captured):
        payload = command.payload
        native = LifecycleCommand(
            memory_id=command.path_ids["id"],
            memory_type=payload.memory_type,
            to_state="discarded" if command.operation == "memories.invalidate" else payload.to_state,
            reason=payload.reason,
            triggered_by=self._ctx.principal.subject,
            expected_version=payload.expected_version,
        )
        result = await self._scope.application.lifecycle.transition_in_session(session, native, strict_remote=True)
        records = list(captured.values())
        expected = [(native.memory_id, payload.memory_type, payload.expected_version + 1)] + [
            (p["memory_id"], p["memory_type"], normalize_version(p["version"])) for p in result.propagated
        ]
        if len(records) != len(expected):
            raise HttpError("E_INTERNAL")
        effects = []
        for index, (record, (ident, kind, revision)) in enumerate(zip(records, expected, strict=True)):
            eid, memory_id, memory_type, from_state, to_state, timestamp = record
            if (memory_id, memory_type) != (ident, kind) or (
                index == 0 and (from_state, to_state) != (result.from_state, result.to_state)
            ):
                raise HttpError("E_INTERNAL")
            wire = d.Event(
                id=eid,
                type=("memory.invalidated" if command.operation == "memories.invalidate" else "memory.transitioned")
                if index == 0
                else "memory.updated",
                profile_id=self._ctx.profile_id,
                timestamp=timestamp,
                request_id=self._ctx.request_id,
                memory_id=ident,
                job_id=None,
                severity="info",
                data=d.EventData(
                    revision=revision,
                    from_state=from_state,
                    to_state=to_state,
                    operation=command.operation,
                    pending=None,
                    code=None,
                ),
            )
            if len(wire.model_dump_json().encode()) >= 4096:
                raise HttpError("E_INTERNAL")
            effects.append(_EffectEvent(kind, wire))
        parent, parent_receipt = None, None
        for ident, kind in dict.fromkeys((ident, kind) for ident, kind, _ in expected):
            model = FactORM if kind == "fact" else BeliefORM
            row = (
                await session.execute(select(model).where(model.id == ident).execution_options(populate_existing=True))
            ).scalar_one_or_none()
            if row is None:
                raise HttpError("E_INTERNAL")
            receipt = await ReceiptRepository(session).get(ident)
            if receipt is not None:
                if receipt.memory_type != kind:
                    raise HttpError("E_INTERNAL")
                entries = []
                for index, effect in enumerate(effects):
                    e = effect.event
                    if e.memory_id == ident and effect.memory_type == kind:
                        entries.append(
                            dict(
                                id=e.id,
                                kind="lifecycle" if index == 0 else "confidence",
                                timestamp=e.timestamp.isoformat(),
                                actor=self._ctx.principal.subject,
                                reason=payload.reason if index == 0 else "parent_invalidated",
                                revision=e.data.revision,
                                from_state=e.data.from_state,
                                to_state=e.data.to_state,
                                receipt_id=receipt.id,
                            )
                        )
                if (
                    await ReceiptRepository(session).update(
                        receipt.id,
                        lifecycle_state=row.lifecycle_state,
                        confidence=row.confidence,
                        version=str(row.version),
                        updated_at=row.updated_at,
                        history=[*receipt.history, *entries],
                    )
                    is None
                ):
                    raise HttpError("E_STORAGE")
            if kind == "fact":
                index_payload = {
                    "subject": row.subject,
                    "predicate": row.predicate,
                    "object": row.object,
                    "source": row.source,
                }
            else:
                if not isinstance(row.tags, list):
                    raise HttpError("E_INTERNAL")
                index_payload = {
                    "proposition": row.proposition,
                    "confidence": row.confidence,
                    "tags": list(row.tags),
                    "source": row.source,
                }
            try:
                await OutboxRepository(session).add_entry(kind, ident, f"index_{kind}", index_payload)
            except (SQLAlchemyError, sqlite3.Error):
                raise
            except Exception:
                raise HttpError("E_OUTBOX") from None
            if (ident, kind) == (native.memory_id, payload.memory_type):
                parent, parent_receipt = row, receipt.id if receipt is not None else None
        await session.flush()
        self.committed_events = tuple(effects)
        return d.TransitionResult(
            memory_id=native.memory_id,
            memory=_memory(parent, payload.memory_type, parent_receipt),
            from_state=result.from_state,
            to_state=result.to_state,
            propagated=[
                d.Propagation(
                    memory_id=p["memory_id"],
                    action="confidence_demoted",
                    reason="parent_invalidated",
                    revision=normalize_version(p["version"]),
                )
                for p in result.propagated
            ],
            receipt_id=parent_receipt,
            audit_id=self._ticket,
            request_id=self._ctx.request_id,
        )


class RemoteLifecyclePort:
    def __init__(
        self,
        deps: GatewayDependencies,
        *,
        ticket_provider: Callable[[], str | None],
        publish_outcome: Callable[[AuditTicket, AuditOutcome], Awaitable[bool]],
        ttl_seconds=86400,
        events=None,
    ):
        if (
            not callable(ticket_provider)
            or not callable(publish_outcome)
            or (events is not None and not callable(events.publish))
        ):
            raise ValueError("invalid lifecycle dependencies")
        if type(ttl_seconds) is not int or not 1 <= ttl_seconds <= 604800:
            raise ValueError("invalid lifecycle TTL")
        self._deps, self._ticket_provider, self._publish_outcome = deps, ticket_provider, publish_outcome
        self._ttl, self._events = ttl_seconds, events

    async def execute(self, ctx: RequestContext, command: d.LifecycleCommandV1) -> d.TransitionResult:
        op = command.operation
        if op not in {"memories.transition", "memories.invalidate"} or type(command) is not d.COMMAND_TYPES[op]:
            raise HttpError("E_VALIDATION")
        try:
            command = type(command).model_validate(command.model_dump())
        except (ValidationError, TypeError, ValueError):
            raise HttpError("E_VALIDATION") from None
        require_permission(
            ctx.principal,
            "memory.transition" if op == "memories.transition" else "memory.invalidate",
            profile_id=ctx.profile_id,
        )
        if ctx.profile_id is None or command.profile_id != ctx.profile_id:
            raise HttpError("E_VALIDATION")
        if set(command.path_ids) != {"id"} or not command.payload.reason.strip():
            raise HttpError("E_VALIDATION")
        validate_lifecycle_id(command.path_ids["id"])
        if type(ctx.idempotency_key) is not UUID:
            raise HttpError("E_IDEMPOTENCY_REQUIRED")
        if (
            type(ctx.deadline) not in (int, float)
            or not math.isfinite(ctx.deadline)
            or ctx.deadline <= asyncio.get_running_loop().time()
        ):
            raise HttpError("E_TIMEOUT")
        self._deps.tokens.verify_principal(ctx.principal, now=datetime.now(UTC))
        try:
            ticket = self._ticket_provider()
        except Exception:
            raise HttpError("E_AUDIT_UNAVAILABLE") from None
        if type(ticket) is not str or not 1 <= len(ticket) <= 256:
            raise HttpError("E_AUDIT_UNAVAILABLE")
        if self._deps.leases is None:
            raise HttpError("E_RUNTIME_NOT_READY")
        started = monotonic()
        async with self._deps.leases.acquire(ctx) as scope:
            factory = scope.application.memory.session_factory
            uow = _LifecycleMutationUoW(
                factory,
                verify_principal=lambda p: self._deps.tokens.verify_principal(p, now=datetime.now(UTC)),
                ticket_provider=lambda: ticket,
                ttl_seconds=self._ttl,
            )
            handler = LifecycleHandler(scope, ctx, ticket=ticket)
            try:
                result = await uow.run(ctx, command, handler)
            except asyncio.CancelledError:
                ok, recovered = await _settle(self._resolve(factory, ctx, command))
                if not ok or recovered is None:
                    raise
                result, original, audit = recovered
                if original == ctx.request_id and audit != ticket:
                    raise HttpError("E_INTERNAL") from None
                MUTATION_REPLY.set(MutationReply(original != ctx.request_id, original))
            reply = MUTATION_REPLY.get()
            if reply is not None and reply.replayed:
                await _settle(self._reconcile(factory, ctx, command))
            else:
                await _settle(self._tail(factory, uow, ctx, result, ticket, handler.committed_events, started))
            return result

    async def _resolve(self, factory, ctx, command):
        async with factory() as session:
            row = await HttpRequestRepository(session).find(
                ctx.principal.subject, ctx.profile_id, command.operation, str(ctx.idempotency_key).lower()
            )
            if row is None or row.digest != _digest(command):
                return None
            return d.TransitionResult.model_validate_json(row.response_snapshot), row.request_id, row.audit_ticket_id

    async def _reconcile(self, factory, ctx, command):
        async with factory() as session, session.begin():
            repo = HttpRequestRepository(session)
            row = await repo.find(
                ctx.principal.subject, ctx.profile_id, command.operation, str(ctx.idempotency_key).lower()
            )
            if row is None or not row.pending_publication or not row.audit_ticket_id:
                return
            result = d.TransitionResult.model_validate_json(row.response_snapshot)
            outcome = AuditOutcome(200, None, (result.receipt_id,) if result.receipt_id else (), 0.0)
            if await self._publish_outcome(AuditTicket(row.audit_ticket_id, row.request_id), outcome) is True:
                await repo.mark_published(row.request_id)

    async def _tail(self, factory, uow, ctx, result, ticket, effects, started):
        async def audit():
            outcome = AuditOutcome(
                200, None, (result.receipt_id,) if result.receipt_id else (), (monotonic() - started) * 1000
            )
            if await self._publish_outcome(AuditTicket(ticket, ctx.request_id), outcome) is True:
                await uow.mark_published(ctx.request_id)

        tasks = [asyncio.create_task(audit()), asyncio.create_task(self._produce(factory, effects))]
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _produce(self, factory, effects):
        if self._events is None or not effects:
            return
        captured = {e.event.id: e for e in effects}
        observed = {}
        ids = list(captured)
        async with factory() as session:
            for offset in range(0, len(ids), 128):
                rows = (
                    (
                        await session.execute(
                            select(LifecycleEventORM)
                            .where(LifecycleEventORM.id.in_(ids[offset : offset + 128]))
                            .limit(129)
                        )
                    )
                    .scalars()
                    .all()
                )
                for row in rows:
                    observed[row.id] = (
                        row.memory_id,
                        row.memory_type,
                        row.from_state,
                        row.to_state,
                        _aware(row.timestamp),
                    )
        if set(observed) != set(captured):
            return
        for eid, effect in captured.items():
            e = effect.event
            if observed[eid] != (e.memory_id, effect.memory_type, e.data.from_state, e.data.to_state, e.timestamp):
                return
        tasks = [asyncio.create_task(self._events.publish(effect.event)) for effect in effects]
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
