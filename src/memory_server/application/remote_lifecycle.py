# ruff: noqa: E501
"""Remote lifecycle orchestration over the shared mutation UoW."""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from time import monotonic
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from storage.models.belief import BeliefORM
from storage.models.fact import FactORM
from storage.outbox import OutboxRepository
from storage.repositories.lifecycle_repo import LifecycleConflictError, normalize_version
from storage.repositories.receipt_repo import ReceiptRepository

from memory_server.application.contracts import LifecycleCommand
from memory_server.application.mutation_uow import MUTATION_REPLY, MutationUoW
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
    from memory_server.transports.http.routes.lifecycle import validate_lifecycle_id as validate
    return validate(value)


def _memory(row, kind, receipt_id, outbox=True):
    return d.Memory(
        id=row.id, memory_type=kind, content=None, payload=None, confidence=float(row.confidence),
        lifecycle_state=row.lifecycle_state, revision=normalize_version(row.version), source_version=row.version,
        source=None, creator=None, created_at=_aware(row.created_at), updated_at=_aware(row.updated_at),
        valid_from=None, valid_to=None, epistemic_status=None, authority_level=None, risk_tags=[],
        receipt_id=receipt_id, masked=True,
        projection=d.ItemProjection(vector="unknown", graph="unknown", outbox="pending" if outbox else "unknown"),
    )


class LifecycleHandler:
    def __init__(self, scope, ctx, *, ticket: str):
        self._scope, self._ctx, self._ticket = scope, ctx, ticket

    async def apply(self, session: AsyncSession, command: d.ProfileMutationCommand) -> d.TransitionResult:
        payload = command.payload
        if command.operation == "memories.invalidate":
            target = "discarded"
        else:
            target = payload.to_state
        native = LifecycleCommand(
            memory_id=command.path_ids["id"], memory_type=payload.memory_type, to_state=target,
            reason=payload.reason, triggered_by=self._ctx.principal.subject,
            expected_version=payload.expected_version,
        )
        try:
            result = await self._scope.application.lifecycle.transition_in_session(session, native, strict_remote=True)
        except LifecycleConflictError as exc:
            details = {"expected": exc.expected, "actual": exc.actual, "id": exc.memory_id}
            raise HttpError("E_VERSION_CONFLICT", details=details) from None
        except LifecycleMemoryNotFoundError:
            raise HttpError("E_NOT_FOUND") from None
        except InvalidLifecycleTransitionError:
            raise HttpError("E_VALIDATION") from None
        except LifecyclePropagationLimitError:
            raise HttpError("E_STORAGE") from None
        kind = payload.memory_type
        model = FactORM if kind == "fact" else BeliefORM
        row = (await session.execute(select(model).where(model.id == command.path_ids["id"]))).scalar_one_or_none()
        if row is None:
            raise HttpError("E_INTERNAL")
        receipt = await ReceiptRepository(session).get(row.id)
        receipt_id = receipt.id if receipt is not None and receipt.memory_type == kind else None
        if receipt is not None and receipt.memory_type != kind:
            raise HttpError("E_INTERNAL")
        if receipt is not None:
            history = list(receipt.history)
            history.append({"kind": "lifecycle", "timestamp": _aware(row.updated_at).isoformat(), "actor": self._ctx.principal.subject,
                            "reason": payload.reason, "revision": normalize_version(row.version),
                            "from_state": result.from_state, "to_state": result.to_state, "receipt_id": receipt.id})
            updated = await ReceiptRepository(session).update(receipt.id, lifecycle_state=row.lifecycle_state,
                confidence=row.confidence, version=str(row.version), updated_at=row.updated_at, history=history)
            if updated is None:
                raise HttpError("E_STORAGE")
        outbox = OutboxRepository(session)
        if kind == "fact":
            payload_json = {"subject": row.subject, "predicate": row.predicate, "object": row.object, "source": row.source}
        else:
            payload_json = {"proposition": row.proposition, "confidence": row.confidence, "tags": list(row.tags), "source": row.source}
        await outbox.add_entry(kind, row.id, f"index_{kind}", payload_json)
        propagated = [d.Propagation(memory_id=p["memory_id"], action="confidence_demoted", reason="parent_invalidated",
                                     revision=normalize_version(p["version"])) for p in result.propagated]
        return d.TransitionResult(memory_id=row.id, memory=_memory(row, kind, receipt_id), from_state=result.from_state,
                                  to_state=result.to_state, propagated=propagated, receipt_id=receipt_id,
                                  audit_id=self._ticket, request_id=self._ctx.request_id)


class RemoteLifecyclePort:
    def __init__(self, deps: GatewayDependencies, *, ticket_provider: Callable[[], str | None],
                 publish_outcome: Callable[[AuditTicket, AuditOutcome], Awaitable[bool]], ttl_seconds=86400, events=None):
        if not callable(ticket_provider) or not callable(publish_outcome):
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
            type(command).model_validate(command.model_dump())
            validate_lifecycle_id(command.path_ids.get("id"))
        except Exception:
            raise HttpError("E_VALIDATION") from None
        permission = "memory.transition" if op == "memories.transition" else "memory.invalidate"
        if permission not in ctx.principal.permissions:
            raise HttpError("E_PERMISSION_DENIED")
        if ctx.profile_id is None or command.profile_id != ctx.profile_id or ctx.profile_id not in ctx.principal.profiles:
            raise HttpError("E_PERMISSION_DENIED")
        if type(ctx.idempotency_key) is not UUID:
            raise HttpError("E_IDEMPOTENCY_REQUIRED")
        if not ctx.deadline > asyncio.get_running_loop().time():
            raise HttpError("E_TIMEOUT")
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
            uow = MutationUoW(factory, verify_principal=lambda p: self._deps.tokens.verify_principal(p, now=datetime.now(UTC)),
                              ticket_provider=lambda: ticket, ttl_seconds=self._ttl)
            result = await uow.run(ctx, command, LifecycleHandler(scope, ctx, ticket=ticket))
            reply = MUTATION_REPLY.get()
            if reply is None or not reply.replayed:
                outcome = AuditOutcome(200, None, (result.receipt_id,) if result.receipt_id else (), (monotonic()-started)*1000)
                try:
                    if await self._publish_outcome(AuditTicket(ticket, ctx.request_id), outcome) is True:
                        await uow.mark_published(ctx.request_id)
                except Exception:
                    pass
            return result
