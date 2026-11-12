"""Internal relation handler: authority and transaction belong to the S12 caller."""

import asyncio
import json
import math
import sqlite3
from datetime import UTC, datetime
from time import monotonic
from uuid import UUID, uuid4

from pydantic import TypeAdapter, ValidationError
from sqlalchemy import event, text
from sqlalchemy.exc import SQLAlchemyError
from storage.models.relation import ClaimRelationORM
from storage.models.relation_revision import RelationRevisionORM
from storage.outbox import OutboxRepository
from storage.repositories.http_request_repo import HttpRequestRepository
from storage.repositories.lifecycle_repo import normalize_version
from storage.repositories.receipt_repo import ReceiptRepository
from storage.repositories.relation_repo import RelationRepository
from storage.repositories.relation_revision_repo import (
    RelationEventContext,
    RelationRevisionConflict,
    RelationRevisionRepository,
    RelationRevisionStorageError,
)

from memory_server.application.graph_read_service import decode_relation_id, map_relation, relation_id
from memory_server.application.mutation_uow import MUTATION_REPLY, MutationReply, MutationUoW, _digest
from memory_server.control.policy import require_permission
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import AuditOutcome, AuditTicket, RequestContext
from memory_server.models.receipt import MemoryReceipt
from memory_server.transports.http.errors import HttpError
from memory_server.transports.http.routes.relations_write import relation_status


def validate_relation_command(ctx: RequestContext, command):
    try:
        if type(command) not in (d.RelationsCreateCommand, d.RelationsDeleteCommand):
            raise ValueError
        op = command.operation
        if op not in ("relations.create", "relations.delete") or type(command) is not d.COMMAND_TYPES[op]:
            raise ValueError
        payload = command.payload
        expected_type = d.RelationRequest if op == "relations.create" else d.RelationDeleteRequest
        if type(payload) is not expected_type or command.profile_id != ctx.profile_id or ctx.profile_id is None:
            raise ValueError
        for model in (command, payload):
            if set(model.__dict__) - set(type(model).model_fields) or model.model_extra:
                raise ValueError
        if op == "relations.create":
            key = (payload.source_id, payload.target_id, payload.relation_type)
            if command.path_ids != {}:
                raise ValueError
            relation_id(*key)
        else:
            if set(command.path_ids) != {"id"}:
                raise ValueError
            key = decode_relation_id(command.path_ids["id"])
        if key[2] == "derived_from":
            raise ValueError
        for name, field in expected_type.model_fields.items():
            if name not in ("expected_versions", "expected_version"):
                TypeAdapter(field.rebuild_annotation()).validate_python(payload.__dict__.get(name), strict=True)
        if not payload.reason.strip():
            raise ValueError
        versions = payload.__dict__.get("expected_versions", {})
        if (
            type(versions) is not dict
            or set(versions) - set(key[:2])
            or any(type(v) is not int or v < 1 for v in versions.values())
        ):
            raise ValueError
        missing = set(versions) != set(key[:2])
        if op == "relations.delete":
            if "expected_version" not in payload.__dict__:
                missing = True
            elif type(payload.expected_version) is not int or payload.expected_version < 1:
                raise ValueError
        if missing:
            raise HttpError("E_PRECONDITION_REQUIRED")
        command = type(command).model_validate(command.model_dump(), strict=True)
    except (ValidationError, ValueError, TypeError, AttributeError, KeyError, UnicodeError):
        raise HttpError("E_VALIDATION") from None
    require_permission(ctx.principal, "relation.write", profile_id=ctx.profile_id)
    if type(ctx.idempotency_key) is not UUID:
        raise HttpError("E_IDEMPOTENCY_REQUIRED")
    if type(ctx.deadline) not in (int, float) or not math.isfinite(ctx.deadline):
        raise HttpError("E_VALIDATION")
    if monotonic() >= ctx.deadline:
        raise HttpError("E_TIMEOUT")
    return command


def _bounded(value):
    serialized = (
        value.model_dump_json()
        if isinstance(value, d.RelationResult)
        else json.dumps(value, ensure_ascii=False, allow_nan=False)
    )
    if len(serialized.encode("utf-8")) > 65536:
        raise HttpError("E_STORAGE", details={"limit": 65536})


def _cas(ident, expected, actual):
    if expected != actual:
        raise HttpError("E_VERSION_CONFLICT", details={"id": ident, "expected": expected, "actual": actual})


class RelationMutationHandler:
    def __init__(self, ctx: RequestContext, *, audit_id: str):
        self.ctx, self.audit_id = ctx, audit_id
        self.statements = self.instructions = 0

    async def apply(self, session, command) -> d.RelationResult:
        command = validate_relation_command(self.ctx, command)
        if type(self.audit_id) is not str or not 1 <= len(self.audit_id) <= 256:
            raise HttpError("E_INTERNAL")
        self.statements = self.instructions = 0
        connection = await session.connection()
        sync = connection.sync_connection
        stopped = None

        def before(*_):
            self.statements += 1
            if monotonic() >= self.ctx.deadline:
                raise HttpError("E_TIMEOUT")
            if self.statements > 64:
                raise HttpError("E_STORAGE", details={"limit": 64})

        def progress():
            nonlocal stopped
            self.instructions += 1000
            if monotonic() >= self.ctx.deadline:
                stopped = "E_TIMEOUT"
            elif self.instructions >= 2_000_000:
                stopped = "E_STORAGE"
            return int(stopped is not None)

        async def bridge(callback):
            return await connection.run_sync(lambda c: c.connection.dbapi_connection.run_async(callback))

        event.listen(sync, "before_cursor_execute", before)
        try:
            await bridge(lambda driver: driver.set_progress_handler(progress, 1000))
            async with asyncio.timeout_at(self.ctx.deadline):
                return await self._apply(session, command)
        except RelationRevisionConflict as exc:
            if exc.actual is None and command.operation == "relations.delete":
                raise HttpError("E_NOT_FOUND") from None
            details = (
                {}
                if exc.expected is None or exc.actual is None
                else {
                    "id": relation_id(*exc.id),
                    "expected": exc.expected,
                    "actual": exc.actual,
                }
            )
            raise HttpError("E_VERSION_CONFLICT", details=details) from None
        except TimeoutError:
            raise HttpError("E_TIMEOUT") from None
        except (SQLAlchemyError, sqlite3.Error) as exc:
            code = getattr(getattr(exc, "orig", exc), "sqlite_errorcode", None)
            busy = type(code) is int and code & 255 in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED)
            raise HttpError(stopped or ("E_REQUEST_IN_PROGRESS" if busy else "E_STORAGE")) from None
        finally:
            event.remove(sync, "before_cursor_execute", before)

            async def reset():
                try:
                    await bridge(lambda driver: driver.set_progress_handler(None, 0))
                except BaseException:
                    await connection.invalidate()
                    raise

            cleanup = asyncio.create_task(reset())
            cancelled = False
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    cancelled = True
            cleanup.result()
            if cancelled:
                raise asyncio.CancelledError

    async def _apply(self, session, command):
        payload = command.payload
        deleted = command.operation == "relations.delete"
        key = (
            decode_relation_id(command.path_ids["id"])
            if deleted
            else (
                payload.source_id,
                payload.target_id,
                payload.relation_type,
            )
        )
        ident = relation_id(*key)
        canonical = await session.get(ClaimRelationORM, key)
        revisions = RelationRevisionRepository(session)
        before = await revisions.state(key, canonical.created_at if canonical else None)
        endpoints = {}
        for endpoint in dict.fromkeys(key[:2]):
            query = " UNION ALL ".join(
                f"SELECT '{kind}' AS kind,version FROM {table} WHERE id=:id"
                for kind, table in (
                    ("fact", "facts"),
                    ("belief", "beliefs"),
                    ("decision", "decisions"),
                    ("skill", "skills"),
                )
            )
            matches = (await session.execute(text(query), {"id": endpoint})).all()
            if len(matches) != 1:
                raise HttpError("E_NOT_FOUND")
            endpoints[endpoint] = matches[0]
        if deleted and not before.canonical_present:
            raise HttpError("E_NOT_FOUND")
        if any(row.kind not in ("fact", "belief") for row in endpoints.values()):
            raise HttpError("E_VALIDATION")
        observed = {}
        for endpoint, row in endpoints.items():
            try:
                actual = normalize_version(row.version) if row.kind == "fact" else row.version
            except ValueError:
                raise RelationRevisionStorageError() from None
            if type(actual) is not int or actual < 1:
                raise RelationRevisionStorageError()
            observed[endpoint] = actual
            _cas(endpoint, payload.expected_versions[endpoint], actual)
        if deleted:
            _cas(ident, payload.expected_version, before.revision)
        elif before.canonical_present:
            raise HttpError("E_VERSION_CONFLICT")
        next_revision = before.next_revision()
        for endpoint in endpoints:
            rows = (
                await session.execute(
                    text("SELECT 1 FROM claim_relations WHERE source_id=:id OR target_id=:id LIMIT 513"),
                    {"id": endpoint},
                )
            ).all()
            if len(rows) > 512:
                raise HttpError("E_STORAGE", details={"limit": 512})
        pair = (
            await session.execute(
                text("SELECT 1 FROM claim_relations WHERE source_id=:s AND target_id=:t LIMIT 17"),
                {"s": key[0], "t": key[1]},
            )
        ).all()
        if len(pair) > 16:
            raise HttpError("E_STORAGE", details={"limit": 16})
        changed = datetime.now(UTC)
        action_id = str(uuid4())
        context = RelationEventContext(
            str(uuid4()), changed, self.ctx.request_id, self.ctx.principal.subject, action_id
        )
        history = dict(
            kind="relation",
            operation=command.operation,
            source_id=key[0],
            target_id=key[1],
            relation_type=key[2],
            id=ident,
            request_id=self.ctx.request_id,
            actor=self.ctx.principal.subject,
            reason=payload.reason,
            expected_versions=payload.expected_versions,
            observed_versions=observed,
            before=before.revision,
            after=next_revision,
            deleted=deleted,
        )
        _bounded(history)
        prospective = d.RelationResult(
            relation=d.Relation(
                id=ident,
                source_id=key[0],
                target_id=key[1],
                relation_type=key[2],
                canonical=True,
                revision=next_revision,
                created_at=before.canonical_created_at or changed,
            ),
            deleted=deleted,
            audit_id=self.audit_id,
            receipt_ids=[action_id],
            outbox_ids=[str(uuid4())],
            request_id=self.ctx.request_id,
        )
        _bounded(prospective)
        repo = RelationRepository(session)
        if deleted:
            state = await repo.delete_versioned(
                key, before=before, expected_revision=payload.expected_version, event=context
            )
        else:
            canonical, state = await repo.create_versioned(*key, event=context)
        await ReceiptRepository(session).create(
            MemoryReceipt(
                id=action_id,
                memory_type=endpoints[key[0]].kind,
                source="gateway",
                created_by=self.ctx.principal.subject,
                timestamp=changed,
                updated_at=changed,
                history=[history],
            )
        )
        try:
            intent = await OutboxRepository(session).add_entry(
                record_type=endpoints[key[0]].kind,
                record_id=key[0],
                operation="relation_sync",
                payload=dict(
                    v=1,
                    source_id=key[0],
                    target_id=key[1],
                    relation_type=key[2],
                    revision=state.revision,
                    event_id=state.event_id,
                ),
            )
            await session.flush()
        except (SQLAlchemyError, sqlite3.Error):
            raise HttpError("E_OUTBOX") from None
        row = type(
            "CapturedRelation",
            (),
            dict(source_id=key[0], target_id=key[1], relation_type=key[2], created_at=state.relation_created_at),
        )
        result = prospective.model_copy(
            update={"relation": map_relation(row, state.revision), "outbox_ids": [intent.id]}
        )
        try:
            result = d.RelationResult.model_validate(result.model_dump(), strict=True)
        except ValidationError:
            raise HttpError("E_INTERNAL") from None
        _bounded(result)
        return result


async def _relation_settle(awaitable):
    task = asyncio.ensure_future(awaitable)
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
    return task.result()


class RemoteRelationsWritePort:
    def __init__(self, deps, *, ticket_provider, publish_outcome, ttl_seconds=86400, events=None):
        if (
            not callable(ticket_provider)
            or not callable(publish_outcome)
            or (events is not None and not callable(getattr(events, "publish", None)))
        ):
            raise ValueError("invalid relation mutation dependencies")
        if type(ttl_seconds) is not int or not 1 <= ttl_seconds <= 604800:
            raise ValueError("invalid relation mutation TTL")
        self._deps, self._ticket_provider, self._publish_outcome = deps, ticket_provider, publish_outcome
        self._ttl, self._events = ttl_seconds, events

    async def execute(self, ctx: RequestContext, command):
        self._deps.tokens.verify_principal(ctx.principal, now=datetime.now(UTC))
        command = validate_relation_command(ctx, command)
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
            uow = MutationUoW(
                factory,
                verify_principal=lambda p: self._deps.tokens.verify_principal(p, now=datetime.now(UTC)),
                ticket_provider=lambda: ticket,
                ttl_seconds=self._ttl,
            )
            handler = RelationMutationHandler(ctx, audit_id=ticket)

            async def run():
                result = await uow.run(ctx, command, handler)
                return result, MUTATION_REPLY.get()

            task = asyncio.create_task(run())
            try:
                try:
                    result, reply = await asyncio.shield(task)
                except asyncio.CancelledError:
                    task.cancel()
                    try:
                        await _relation_settle(task)
                    except BaseException:
                        pass
                    recovered = await _relation_settle(self._resolve(factory, ctx, command))
                    if recovered is None:
                        raise
                    result, row = recovered
                    reply = MutationReply(row.request_id != ctx.request_id, row.request_id)
                    if not reply.replayed and row.audit_ticket_id != ticket:
                        raise HttpError("E_INTERNAL")
                MUTATION_REPLY.set(reply)
                relation_status(result, command.operation == "relations.delete")
                try:
                    if reply is not None and reply.replayed:
                        await _relation_settle(self._reconcile(factory, ctx, command))
                    else:
                        await _relation_settle(self._tail(factory, ctx, command, result, ticket, started))
                except Exception:
                    # Commit is authoritative; pending publication remains durable.
                    pass
                return result
            finally:
                if not task.done():
                    task.cancel()
                try:
                    await _relation_settle(task)
                except BaseException:
                    pass

    async def _resolve(self, factory, ctx, command):
        async with asyncio.timeout(5), factory() as session:
            row = await HttpRequestRepository(session).find(
                ctx.principal.subject, ctx.profile_id, command.operation, str(ctx.idempotency_key).lower()
            )
            if row is None or row.digest != _digest(command):
                return None
            result = d.RelationResult.model_validate_json(row.response_snapshot, strict=True)
            relation_status(result, command.operation == "relations.delete")
            if result.audit_id != row.audit_ticket_id or result.request_id != row.request_id:
                raise HttpError("E_INTERNAL")
            return result, row

    async def _reconcile(self, factory, ctx, command):
        resolved = await self._resolve(factory, ctx, command)
        if resolved is None:
            return
        result, row = resolved
        if row.pending_publication:
            await self._audit(factory, result, row.audit_ticket_id, 0.0)

    async def _audit(self, factory, result, ticket, latency):
        outcome = AuditOutcome(relation_status(result, result.deleted), None, tuple(result.receipt_ids), latency)
        if await self._publish_outcome(AuditTicket(ticket, result.request_id), outcome) is True:
            async with factory() as session, session.begin():
                await HttpRequestRepository(session).mark_published(result.request_id)

    async def _tail(self, factory, ctx, command, result, ticket, started):
        tasks = [
            asyncio.create_task(self._audit(factory, result, ticket, (monotonic() - started) * 1000)),
            asyncio.create_task(self._produce(factory, ctx, command, result, ticket)),
        ]
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _produce(self, factory, ctx, command, result, ticket):
        if self._events is None:
            return
        rel = result.relation
        key = decode_relation_id(rel.id)
        if len(result.receipt_ids) != 1 or len(result.outbox_ids) != 1:
            return
        async with asyncio.timeout(5), factory() as session:
            row = await session.get(RelationRevisionORM, (*key, rel.revision))
            resolved = await self._resolve(factory, ctx, command)
            if row is None or resolved is None:
                return
            original, journal = resolved
            receipt = (
                await session.execute(
                    text("SELECT history FROM receipts WHERE id=:id LIMIT 1"), {"id": result.receipt_ids[0]}
                )
            ).scalar_one_or_none()
            outbox = (
                await session.execute(
                    text("SELECT operation,record_id,payload_json AS payload FROM outbox_entries WHERE id=:id LIMIT 1"),
                    {"id": result.outbox_ids[0]},
                )
            ).first()

            def utc(value):
                return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)

            if (
                original != result
                or journal.audit_ticket_id != ticket
                or row.request_id != result.request_id
                or row.action_receipt_id != result.receipt_ids[0]
                or bool(row.deleted) != result.deleted
                or utc(row.relation_created_at) != rel.created_at
                or receipt is None
                or outbox is None
                or outbox.operation != "relation_sync"
                or outbox.record_id != key[0]
            ):
                return
            payload = json.loads(outbox.payload)
            history = json.loads(receipt)
            if (
                payload
                != dict(
                    v=1,
                    source_id=key[0],
                    target_id=key[1],
                    relation_type=key[2],
                    revision=rel.revision,
                    event_id=row.event_id,
                )
                or len(history) != 1
                or any(
                    history[0].get(k) != v
                    for k, v in dict(
                        source_id=key[0],
                        target_id=key[1],
                        relation_type=key[2],
                        after=rel.revision,
                        request_id=result.request_id,
                        deleted=result.deleted,
                        operation=command.operation,
                    ).items()
                )
            ):
                return
            wire = d.Event(
                id=row.event_id,
                type="relation.deleted" if result.deleted else "relation.created",
                profile_id=ctx.profile_id,
                timestamp=utc(row.changed_at),
                request_id=row.request_id,
                memory_id=row.source_id,
                job_id=None,
                severity="info",
                data=d.EventData(
                    revision=row.revision,
                    operation=command.operation,
                    from_state=None,
                    to_state=None,
                    pending=None,
                    code=None,
                ),
            )
            wire = d.Event.model_validate(wire.model_dump(), strict=True)
            if len(wire.model_dump_json().encode("utf-8")) > 4096:
                return
        await self._events.publish(wire)
