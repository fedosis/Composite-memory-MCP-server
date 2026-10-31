"""Request-wide canonical transaction and durable, profile-scoped idempotency.

Handlers must use only the supplied session, never commit/rollback it, and
perform no external side effects. The guard is not a sandbox for other sessions.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
from collections.abc import Callable
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from uuid import UUID

from sqlalchemy import event
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from storage.repositories.http_request_repo import HttpRequestRepository

from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import MutationHandler, Principal, RequestContext
from memory_server.transports.http.errors import HttpError, error_code


@dataclass(frozen=True)
class MutationReply:
    replayed: bool
    original_request_id: str


MUTATION_REPLY: ContextVar[MutationReply | None] = ContextVar("s12_mutation_reply", default=None)

_HTTP_SHAPE = MappingProxyType(
    {
        "memories.create": ("POST", "/api/v1/profiles/{profile}/memories"),
        "learn": ("POST", "/api/v1/profiles/{profile}/learn"),
        "memories.transition": ("POST", "/api/v1/profiles/{profile}/memories/{id}/transition"),
        "memories.invalidate": ("POST", "/api/v1/profiles/{profile}/memories/{id}/invalidate"),
        "beliefs.set": ("POST", "/api/v1/profiles/{profile}/beliefs"),
        "conflicts.resolve": ("POST", "/api/v1/profiles/{profile}/conflicts/{id}/resolve"),
        "relations.create": ("POST", "/api/v1/profiles/{profile}/relations"),
        "relations.delete": ("DELETE", "/api/v1/profiles/{profile}/relations/{id}"),
    }
)


def _digest(command: d.ProfileMutationCommand) -> str:
    method, route = _HTTP_SHAPE[command.operation]
    envelope = {
        "v": 1,
        "method": method,
        "route": route,
        "operation": command.operation,
        "profile_id": command.profile_id,
        "path_ids": dict(command.path_ids),
        "payload": command.payload.model_dump(mode="json"),
    }
    return hashlib.sha256(json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _naive_utc(dt: datetime) -> datetime:
    return dt.astimezone(UTC).replace(tzinfo=None)


def _is_busy(exc: Exception) -> bool:
    message = str(getattr(exc, "orig", exc)).lower()
    return any(marker in message for marker in ("database is locked", "table is locked", "database is busy"))


def _is_journal_unique(exc: IntegrityError) -> bool:
    message = str(getattr(exc, "orig", exc))
    return "http_request_journal" in message and "UNIQUE constraint failed" in message


def _storage_code(exc: Exception) -> str:
    return "E_STORAGE"


class _RetryableWrite(Exception):  # noqa: N818
    pass


class _JournalRace(_RetryableWrite):  # noqa: N818
    pass


class _WriterBusy(_RetryableWrite):  # noqa: N818
    pass


class _HandlerCommit(Exception):  # noqa: N818
    pass


def _result_refs(result: d.MutationResult) -> list[str]:
    if isinstance(result, d.WriteResult):
        return (
            [memory.id for memory in result.memories]
            + [receipt.id for receipt in result.receipts]
            + list(result.outbox_ids)
        )
    if isinstance(result, d.TransitionResult):
        return [result.memory_id] + ([result.receipt_id] if result.receipt_id else [])
    if isinstance(result, d.ConflictResult):
        return [result.conflict_id, *result.receipt_ids]
    if isinstance(result, d.RelationResult):
        return [result.relation.id, *result.receipt_ids, *result.outbox_ids]
    raise HttpError("E_INTERNAL")


class MutationUoW:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        verify_principal: Callable[[Principal], None],
        ttl_seconds: int = 86_400,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        ticket_provider: Callable[[], str | None] = lambda: None,
        max_attempts: int = 2,
    ) -> None:
        if not all(callable(value) for value in (session_factory, verify_principal, clock, ticket_provider)):
            raise ValueError("invalid mutation dependencies")
        if type(ttl_seconds) is not int or not 1 <= ttl_seconds <= 604800:
            raise ValueError("invalid mutation TTL")
        if type(max_attempts) is not int or not 1 <= max_attempts <= 5:
            raise ValueError("invalid mutation attempts")
        self._session_factory = session_factory
        self._verify = verify_principal
        self._ttl = ttl_seconds
        self._clock = clock
        self._ticket_provider = ticket_provider
        self._max_attempts = max_attempts

    def _ticket(self) -> str | None:
        try:
            ticket = self._ticket_provider()
        except Exception:
            raise HttpError("E_INTERNAL") from None
        if ticket is not None and (type(ticket) is not str or not 1 <= len(ticket) <= 256):
            raise HttpError("E_INTERNAL")
        return ticket

    async def run(
        self,
        ctx: RequestContext,
        command: d.ProfileMutationCommand,
        handler: MutationHandler,
    ) -> d.MutationResult:
        MUTATION_REPLY.set(None)
        try:
            operation = command.operation
            if type(operation) is not str or operation not in _HTTP_SHAPE:
                raise ValueError("invalid operation")
            if not isinstance(command, d.COMMAND_TYPES[operation]):
                raise ValueError("invalid command type")
            type(command).model_validate(command.model_dump())
        except Exception:
            raise HttpError("E_VALIDATION") from None
        if ctx.profile_id is None or command.profile_id != ctx.profile_id:
            raise HttpError("E_VALIDATION")
        if ctx.profile_id not in ctx.principal.profiles:
            raise HttpError("E_PERMISSION_DENIED")
        if type(ctx.idempotency_key) is not UUID:
            raise HttpError("E_IDEMPOTENCY_REQUIRED")
        if not ctx.deadline > asyncio.get_running_loop().time():
            raise HttpError("E_TIMEOUT")
        try:
            self._verify(ctx.principal)
        except Exception as exc:
            raise HttpError(error_code(exc)) from None
        digest = _digest(command)
        for attempt in range(1, self._max_attempts + 1):
            try:
                result, replayed, original = await self._attempt(ctx, command, handler, digest)
            except _RetryableWrite:
                if attempt == self._max_attempts:
                    raise HttpError("E_REQUEST_IN_PROGRESS") from None
                continue
            except HttpError:
                raise
            except IntegrityError as exc:
                raise HttpError(error_code(exc)) from None
            except (OperationalError, sqlite3.Error) as exc:
                raise HttpError(_storage_code(exc)) from None
            except Exception:
                # Corrupt snapshots and violated result shapes never expose DTO/SQL text.
                raise HttpError("E_INTERNAL") from None
            MUTATION_REPLY.set(MutationReply(replayed, original))
            return result
        raise HttpError("E_INTERNAL")

    async def _attempt(self, ctx, command, handler, digest):
        idem_key = str(ctx.idempotency_key).lower()
        async with self._session_factory() as session, session.begin():
            now = _naive_utc(self._clock())
            repo = HttpRequestRepository(session)
            await repo.purge_expired(now, limit=50)
            row = await repo.find(ctx.principal.subject, ctx.profile_id, command.operation, idem_key)
            if row is not None:
                if row.expires_at <= now and not row.pending_publication:
                    await repo.delete(row)
                elif row.digest != digest:
                    raise HttpError("E_IDEMPOTENCY_CONFLICT")
                else:
                    result = d.RESULT_TYPES[command.operation].model_validate_json(row.response_snapshot)
                    return result, True, row.request_id
            if await repo.pending(ctx.profile_id):
                raise HttpError("E_AUDIT_UNAVAILABLE")
            result = await self._apply_guarded(session, command, handler)
            try:
                await session.flush()
            except IntegrityError as exc:
                raise HttpError(error_code(exc)) from None
            except (OperationalError, sqlite3.Error) as exc:
                if _is_busy(exc):
                    raise _WriterBusy() from None
                raise HttpError(_storage_code(exc)) from None
            expected = d.RESULT_TYPES[command.operation]
            if not isinstance(result, expected):
                raise HttpError("E_INTERNAL")
            result = expected.model_validate(result.model_dump())
            ticket = self._ticket()
            await repo.add(
                subject=ctx.principal.subject,
                profile_id=ctx.profile_id,
                operation=command.operation,
                idem_key=idem_key,
                digest=digest,
                response_snapshot=result.model_dump_json(),
                result_refs=json.dumps(_result_refs(result), separators=(",", ":")),
                request_id=ctx.request_id,
                audit_ticket_id=ticket,
                pending_publication=int(ticket is not None),
                created_at=now,
                expires_at=now + timedelta(seconds=self._ttl),
            )
            try:
                await session.flush()
            except IntegrityError as exc:
                if _is_journal_unique(exc):
                    raise _JournalRace() from None
                raise HttpError(error_code(exc)) from None
            except (OperationalError, sqlite3.Error) as exc:
                if _is_busy(exc):
                    raise _WriterBusy() from None
                raise HttpError(_storage_code(exc)) from None
            return result, False, ctx.request_id

    async def _apply_guarded(self, session, command, handler):
        def deny(sync_session):
            if not sync_session.in_nested_transaction():
                raise _HandlerCommit()

        event.listen(session.sync_session, "before_commit", deny)
        try:
            try:
                result = await handler.apply(session, command)
            except HttpError:
                raise
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                raise HttpError(error_code(exc)) from None
        finally:
            event.remove(session.sync_session, "before_commit", deny)
        if not session.in_transaction():
            raise HttpError("E_INTERNAL")
        return result

    async def mark_published(self, request_id: str) -> bool:
        """Internal publisher seam: caller must already have published the outcome."""
        async with self._session_factory() as session, session.begin():
            return await HttpRequestRepository(session).mark_published(request_id)
