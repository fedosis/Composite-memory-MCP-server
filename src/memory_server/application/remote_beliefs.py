"""Bounded belief reads and supplied-session, scoped remote mutations."""

from __future__ import annotations

import asyncio
import json
import math
import sqlite3
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from memory_server.application.errors import UnknownDependencyError
from memory_server.application.memory_read_service import (
    ReadKey,
    ReadResources,
    b64,
    canonical,
    project_for_caller,
    revision,
    unb64,
    utc,
)
from memory_server.control.policy import require_permission
from memory_server.evaluation.relation import RelationClassifier
from memory_server.evaluation.validator import is_valid_transition
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import GatewayDependencies, ProfileSession, RequestContext
from memory_server.models.belief import Belief
from memory_server.models.receipt import LifecycleState
from memory_server.transports.http.errors import HttpError

# isort: split
# Write authority: supplied-session foundation and scoped mutation port.
from uuid import uuid4

from sqlalchemy import event, select
from storage.models.belief import BeliefORM, EvidenceORM
from storage.models.fact import FactORM
from storage.models.lifecycle import LifecycleEventORM
from storage.outbox import OutboxRepository
from storage.repositories import BeliefRepository, LifecycleRepository, ReceiptRepository
from storage.repositories.http_request_repo import HttpRequestRepository
from storage.repositories.lifecycle_repo import LifecycleConflictError, normalize_version

from memory_server.admission import AdmissionDecision, MemoryAdmissionGate
from memory_server.application.mutation_uow import MUTATION_REPLY, MutationReply, MutationUoW, _digest, _is_busy
from memory_server.gateway_contracts.ports import AuditOutcome, AuditTicket
from memory_server.models.evidence import Evidence
from memory_server.models.receipt import MemoryReceipt
from memory_server.services.lifecycle_service import (
    InvalidLifecycleTransitionError,
    LifecycleMemoryNotFoundError,
    LifecyclePropagationLimitError,
    LifecycleService,
    LifecycleTransitionRequest,
)

if TYPE_CHECKING:
    from memory_server.transports.http.routes.beliefs import BeliefCursorCodec

BeliefReadCommand = d.BeliefsListCommand | d.BeliefsGetCommand | d.ConflictsListCommand
BeliefReadResult = d.Page[d.Memory] | d.Memory | d.Page[d.Conflict]
READ_OPERATIONS = frozenset({"beliefs.list", "beliefs.get", "conflicts.list"})
PAIR_BUDGET, EVIDENCE_BUDGET, VM_BUDGET = 512, 1024, 2_000_000


def validate_belief_id(value: object) -> str:
    try:
        if type(value) is not str or len(value) != 36 or str(UUID(value)) != value:
            raise ValueError
    except (ValueError, TypeError, AttributeError):
        raise HttpError("E_VALIDATION") from None
    return value


def encode_conflict_id(a_id: str, b_id: str) -> str:
    pair = sorted((validate_belief_id(a_id), validate_belief_id(b_id)))
    if pair[0] == pair[1]:
        raise HttpError("E_VALIDATION")
    return "conf_" + b64(canonical(pair))


def decode_conflict_id(value: object) -> tuple[str, str]:
    try:
        if type(value) is not str or not value.startswith("conf_") or len(value) > 261:
            raise ValueError
        raw = unb64(value[5:])
        if len(raw) > 128:
            raise ValueError
        pair = json.loads(raw)
        if type(pair) is not list or len(pair) != 2:
            raise ValueError
        a, b = (validate_belief_id(v) for v in pair)
        if a >= b or encode_conflict_id(a, b) != value:
            raise ValueError
        return a, b
    except (ValueError, TypeError, UnicodeError, HttpError):
        raise HttpError("E_VALIDATION") from None


def validate_read_command(command: BeliefReadCommand) -> BeliefReadCommand:
    try:
        op = command.operation
        if op not in READ_OPERATIONS or type(command) is not d.COMMAND_TYPES[op]:
            raise ValueError
        command = type(command).model_validate(command.model_dump(), strict=True)
        if command.path_ids != ({} if op != "beliefs.get" else {"id": command.path_ids.get("id")}):
            raise ValueError
        if op == "beliefs.get":
            validate_belief_id(command.path_ids["id"])
        if op == "beliefs.list":
            q = command.payload
            if q.lifecycle_state not in {v.value for v in LifecycleState} or type(q.min_confidence) is bool:
                raise ValueError
            if not math.isfinite(q.min_confidence):
                raise ValueError
    except (ValidationError, ValueError, TypeError, KeyError, AttributeError):
        raise HttpError("E_VALIDATION") from None
    if op == "beliefs.list" and (
        any(getattr(q, key) is not None for key in ("proposition", "source", "creator", "source_id")) or q.tags != []
    ):
        raise HttpError("E_PERMISSION_DENIED")
    return command


def _record_valid(record, requested=None):
    value = record.memory
    try:
        validate_belief_id(value["id"])
        if (
            record.memory_type != "belief"
            or revision(record) is None
            or (requested is not None and value["id"] != requested)
        ):
            raise ValueError
    except (HttpError, ValueError, TypeError, KeyError):
        raise HttpError("E_INTERNAL") from None
    return record


async def _settle(awaitable):
    """Shield AND join owned cleanup, including repeated cancellation."""
    task = asyncio.ensure_future(awaitable)
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
    result = task.result()
    if cancelled:
        raise asyncio.CancelledError
    return result


class RemoteBeliefsPort:
    def __init__(
        self,
        deps: GatewayDependencies,
        bind_read: Callable[[ProfileSession], ReadResources],
        *,
        cursor_codec: BeliefCursorCodec,
    ):
        if deps.leases is None:
            raise HttpError("E_RUNTIME_NOT_READY")
        if not callable(bind_read) or cursor_codec is None:
            raise HttpError("E_CAPABILITY_UNAVAILABLE")
        self.deps, self.bind_read, self.cursor_codec = deps, bind_read, cursor_codec

    async def execute(self, ctx: RequestContext, command: BeliefReadCommand) -> BeliefReadResult:
        self.deps.tokens.verify_principal(ctx.principal, now=datetime.now(UTC))
        require_permission(ctx.principal, "belief.read", profile_id=ctx.profile_id)
        command = validate_read_command(command)
        if ctx.profile_id is None or command.profile_id != ctx.profile_id:
            raise HttpError("E_VALIDATION")
        if type(ctx.deadline) not in (int, float) or not math.isfinite(ctx.deadline):
            raise HttpError("E_VALIDATION")
        if time.monotonic() >= ctx.deadline:
            raise HttpError("E_TIMEOUT")
        state = None
        if getattr(command.payload, "cursor", None) is not None:
            state = self.cursor_codec.validate(ctx.principal, command, now=self.cursor_codec.clock())
        try:
            async with self.deps.leases.acquire(ctx) as scope:
                resources = self.bind_read(scope)
                if not callable(resources.session_factory) or not all(
                    callable(getattr(resources.provider, name, None)) for name in ("read_memory", "read_memory_page")
                ):
                    raise HttpError("E_CAPABILITY_UNAVAILABLE")
                session = resources.session_factory()
                try:
                    async with session.begin():
                        handler = BeliefsReadHandler(
                            session, resources, ctx, cursor_codec=self.cursor_codec, state=state
                        )
                        result = await handler.execute(command)
                    self.deps.tokens.verify_principal(ctx.principal, now=datetime.now(UTC))
                    handler.check_deadline()
                    return result
                finally:
                    await _settle(session.close())
        except HttpError:
            raise
        except TimeoutError:
            raise HttpError("E_TIMEOUT") from None
        except UnknownDependencyError:
            raise HttpError("E_RUNTIME_NOT_READY") from None
        except (SQLAlchemyError, sqlite3.Error):
            raise HttpError("E_STORAGE") from None
        except Exception:
            raise HttpError("E_INTERNAL") from None


class BeliefsReadHandler:
    def __init__(
        self,
        session: AsyncSession,
        resources: ReadResources,
        ctx: RequestContext,
        *,
        cursor_codec: BeliefCursorCodec,
        state: dict | None,
    ):
        self.session, self.resources, self.ctx = session, resources, ctx
        self.cursor_codec, self.state = cursor_codec, state
        self.instructions, self.reason, self.evidence_used = 0, None, 0

    def check_deadline(self):
        if time.monotonic() >= self.ctx.deadline:
            raise HttpError("E_TIMEOUT")

    def _progress(self):
        if time.monotonic() >= self.ctx.deadline:
            self.reason = "timeout"
        else:
            self.instructions += 1000
            if self.instructions >= VM_BUDGET:
                self.reason = "budget"
        return int(self.reason is not None)

    async def execute(self, command: BeliefReadCommand) -> BeliefReadResult:
        conn = await self.session.connection()

        async def install(driver):
            if not callable(getattr(driver, "create_function", None)) or not callable(
                getattr(driver, "set_progress_handler", None)
            ):
                raise HttpError("E_CAPABILITY_UNAVAILABLE")
            await driver.create_function(
                "s15_time", 1, lambda value: utc(value).isoformat(timespec="microseconds"), deterministic=True
            )
            await driver.set_progress_handler(self._progress, 1000)

        async def clear():
            try:
                await conn.run_sync(
                    lambda c: c.connection.dbapi_connection.run_async(lambda a: a.set_progress_handler(None, 0))
                )
            except Exception:
                await conn.invalidate()

        try:
            async with asyncio.timeout_at(self.ctx.deadline):
                await conn.run_sync(lambda c: c.connection.dbapi_connection.run_async(install))
                await self.session.execute(text("BEGIN"))
                method = {
                    "beliefs.list": self.list_beliefs,
                    "beliefs.get": self.get_belief,
                    "conflicts.list": self.list_conflicts,
                }[command.operation]
                result = await method(command)
                self.check_deadline()
                return result
        except (SQLAlchemyError, sqlite3.Error):
            if self.reason == "timeout":
                raise HttpError("E_TIMEOUT") from None
            if self.reason == "budget":
                raise HttpError("E_STORAGE", details={"limit": VM_BUDGET}) from None
            raise HttpError("E_STORAGE") from None
        finally:
            await _settle(clear())

    async def _read(self, ident):
        self.check_deadline()
        record = await self.resources.provider.read_memory(self.session, ident, memory_type="belief")
        self.check_deadline()
        return _record_valid(record, ident)

    def _project(self, record, operation):
        self.check_deadline()
        return project_for_caller(self.ctx, operation, _record_valid(record), "unknown")

    def _page(self, command, items, more, last, upper):
        cursor = (
            self.cursor_codec.issue(
                self.ctx, command, last=last, upper=upper, limit=command.payload.limit, now=self.cursor_codec.clock()
            )
            if more
            else None
        )
        self.check_deadline()
        return d.RESULT_TYPES[command.operation](
            items=items, has_more=more, next_cursor=cursor, request_id=self.ctx.request_id
        )

    async def list_beliefs(self, command: d.BeliefsListCommand) -> d.Page[d.Memory]:
        q = command.payload
        query = d.MemoryListQuery(
            limit=q.limit,
            cursor=None,
            memory_types=["belief"],
            lifecycle_states=[q.lifecycle_state],
            min_confidence=q.min_confidence,
            max_confidence=None,
            source=None,
            tags=[],
            authority_level=None,
            epistemic_status=None,
            created_from=None,
            created_to=None,
            updated_from=None,
            updated_to=None,
            valid_at=None,
            risk_tags=[],
        )
        page = await self.resources.provider.read_memory_page(
            self.session,
            query,
            after=ReadKey(*self.state["last"]) if self.state else None,
            upper=ReadKey(*self.state["upper"]) if self.state else None,
        )
        items = [self._project(record, command.operation) for record in page.records]
        last = (utc(items[-1].created_at).isoformat(timespec="microseconds"), "belief", items[-1].id) if items else None
        upper = (page.upper.created_at, "belief", page.upper.id) if page.upper else None
        if page.has_more and (last is None or upper is None):
            raise HttpError("E_INTERNAL")
        return self._page(command, items, page.has_more, last, upper)

    async def get_belief(self, command: d.BeliefsGetCommand) -> d.Memory:
        return self._project(await self._read(command.path_ids["id"]), command.operation)

    async def _sql(self, sql, params=None):
        self.check_deadline()
        rows = (await self.session.execute(text(sql), params or {})).all()
        self.check_deadline()
        return rows

    def _model(self, record):
        value = dict(record.memory)
        for key in ("tags", "source_ids"):
            if type(value.get(key)) is str:
                value[key] = json.loads(value[key])
            if type(value.get(key)) is not list or any(type(v) is not str for v in value[key]):
                raise HttpError("E_INTERNAL")
        for key in ("created_at", "updated_at", "last_reinforced_at"):
            value[key] = utc(value[key])
        model = Belief.model_validate(value, strict=True)
        if not math.isfinite(model.confidence):
            raise HttpError("E_INTERNAL")
        return model

    async def _conflict(self, a, b, command):
        av, bv = a.memory, b.memory
        params = {"a": av["id"], "b": bv["id"]}
        edges = await self._sql(
            "SELECT source_id,target_id FROM claim_relations WHERE relation_type='derived_from' "
            "AND source_id IN (:a,:b) AND target_id IN (:a,:b) "
            "ORDER BY source_id COLLATE BINARY,target_id COLLATE BINARY LIMIT 5",
            params,
        )
        if len(edges) > 4:
            raise HttpError("E_INTERNAL")
        actions = []
        for action, targets, state in (
            ("keep_a", [bv], "discarded"),
            ("keep_b", [av], "discarded"),
            ("merge", [av, bv], "superseded"),
            ("mark_contradicted", [av, bv], "contradicted"),
            ("discard_both", [av, bv], "discarded"),
            ("defer", [], ""),
        ):
            if all(is_valid_transition(v["lifecycle_state"], state) for v in targets) and not any(
                edge.target_id in {v["id"] for v in targets} for edge in edges
            ):
                actions.append(action)
        rows = await self._sql(
            "SELECT id,belief_id,weight FROM evidence WHERE belief_id IN (:a,:b) "
            "ORDER BY belief_id COLLATE BINARY,created_at,id COLLATE BINARY LIMIT :remaining_plus_one",
            {**params, "remaining_plus_one": EVIDENCE_BUDGET - self.evidence_used + 1},
        )
        self.evidence_used += len(rows)
        if self.evidence_used > EVIDENCE_BUDGET:
            raise HttpError("E_STORAGE", details={"limit": EVIDENCE_BUDGET})
        summaries = {av["id"]: [], bv["id"]: []}
        for row in rows:
            summaries[row.belief_id].append(
                d.EvidenceSummary(
                    method=None,
                    sources=[],
                    session_id=None,
                    confidence=row.weight,
                    source_date=None,
                    derived_from=[],
                    claim_type=None,
                )
            )
        return d.Conflict(
            id=encode_conflict_id(av["id"], bv["id"]),
            belief_a=self._project(a, command.operation),
            belief_b=self._project(b, command.operation),
            evidence_a=summaries[av["id"]],
            evidence_b=summaries[bv["id"]],
            kind="contradiction",
            allowed_actions=actions,
            analysis_complete=False,
        )

    async def list_conflicts(self, command: d.ConflictsListCommand) -> d.Page[d.Conflict]:
        classifier = RelationClassifier()
        if not callable(getattr(classifier, "classify_pair", None)):
            raise HttpError("E_CAPABILITY_UNAVAILABLE")
        columns = "s15_time(a.created_at),a.id,s15_time(b.created_at),b.id"
        base = (
            "SELECT a.id AS a_id,s15_time(a.created_at) AS a_created,b.id AS b_id,s15_time(b.created_at) AS b_created "
            "FROM beliefs AS a JOIN beliefs AS b ON a.id COLLATE BINARY < b.id COLLATE BINARY "
            "WHERE a.lifecycle_state IN ('active','contradicted') AND b.lifecycle_state IN ('active','contradicted')"
        )
        order = "s15_time(a.created_at) {0},a.id COLLATE BINARY {0},s15_time(b.created_at) {0},b.id COLLATE BINARY {0}"

        def position(row):
            return row.a_created, row.a_id, row.b_created, row.b_id

        upper = self.state["upper"] if self.state else None
        if upper is None:
            maximum = await self._sql(base + " ORDER BY " + order.format("DESC") + " LIMIT 1")
            if not maximum:
                return self._page(command, [], False, None, None)
            upper = position(maximum[0])
        names = ("ua", "ui", "ub", "uj")
        params = dict(zip(names, upper, strict=True))
        sql = base + f" AND ({columns}) <= (:ua,:ui,:ub,:uj)"
        if self.state:
            sql += f" AND ({columns}) > (:la,:li,:lb,:lj)"
            params.update(zip(("la", "li", "lb", "lj"), self.state["last"], strict=True))
        candidates = await self._sql(
            sql + " ORDER BY " + order.format("ASC") + " LIMIT :scan_limit", {**params, "scan_limit": PAIR_BUDGET + 1}
        )
        cache, matches = {}, []
        for row in candidates[:PAIR_BUDGET]:
            a_id, b_id = validate_belief_id(row.a_id), validate_belief_id(row.b_id)
            if a_id >= b_id:
                raise HttpError("E_INTERNAL")
            for ident in (a_id, b_id):
                if ident not in cache:
                    try:
                        cache[ident] = await self._read(ident)
                    except HttpError as exc:
                        if exc.code == "E_NOT_FOUND":
                            raise HttpError("E_INTERNAL") from None
                        raise
            self.check_deadline()
            verdict = classifier.classify_pair(
                self._model(cache[a_id]),
                self._model(cache[b_id]),
                context_a=None,
                context_b=None,
                strict_same_context=False,
            )
            self.check_deadline()
            if verdict.relation == "contradiction":
                matches.append((position(row), cache[a_id], cache[b_id]))
                if len(matches) > command.payload.limit:
                    break
        more = len(matches) > command.payload.limit
        if not more and len(candidates) > PAIR_BUDGET:
            raise HttpError("E_STORAGE", details={"limit": PAIR_BUDGET})
        returned = matches[: command.payload.limit]
        items = [await self._conflict(a, b, command) for _, a, b in returned]
        return self._page(command, items, more, returned[-1][0] if returned else None, upper)


def validate_belief_mutation_command(command):
    if type(command) is d.ConflictsResolveCommand:
        return _validate_resolve_command(command)
    try:
        if type(command) is not d.BeliefsSetCommand or command.operation != "beliefs.set":
            raise ValueError
        for model in (command, command.payload, *command.payload.sources):
            if set(model.__dict__) - set(type(model).model_fields) or model.model_extra:
                raise ValueError
        command = d.BeliefsSetCommand.model_validate(command.model_dump(), strict=True)
        payload = command.payload
        if command.path_ids or not payload.proposition.strip() or not payload.reason.strip():
            raise ValueError
        if payload.replace_belief_id is not None:
            validate_belief_id(payload.replace_belief_id)
        for ident in payload.expected_versions:
            validate_belief_id(ident)
        for summary in payload.sources:
            for ident in summary.derived_from:
                validate_belief_id(ident)
        return command
    except (ValidationError, ValueError, TypeError, AttributeError, KeyError):
        raise HttpError("E_VALIDATION") from None


def _validate_resolve_command(command):
    try:
        if command.operation != "conflicts.resolve":
            raise ValueError
        for model in (command, command.payload):
            if set(model.__dict__) - set(type(model).model_fields) or model.model_extra:
                raise ValueError
        command = d.ConflictsResolveCommand.model_validate(command.model_dump(), strict=True)
        payload = command.payload
        a, b = validate_belief_id(payload.belief_a_id), validate_belief_id(payload.belief_b_id)
        if (
            a == b
            or set(command.path_ids) != {"id"}
            or decode_conflict_id(command.path_ids["id"]) != tuple(sorted((a, b)))
            or not payload.reason.strip()
        ):
            raise ValueError
        for ident in payload.expected_versions:
            validate_belief_id(ident)
        if payload.resolution == "merge":
            if (
                type(payload.new_proposition) is not str
                or not 1 <= len(payload.new_proposition) <= 2048
                or not payload.new_proposition.strip()
            ):
                raise ValueError
        elif payload.new_proposition is not None:
            raise ValueError
        return command
    except (ValidationError, ValueError, TypeError, AttributeError, KeyError):
        raise HttpError("E_VALIDATION") from None


def _write_busy(exc):
    original = getattr(exc, "orig", exc)
    if not isinstance(original, sqlite3.OperationalError):
        return False
    code = getattr(original, "sqlite_errorcode", None)
    return code & 255 in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED) if type(code) is int else _is_busy(exc)


async def _write_settle(awaitable):
    async def bounded():
        async with asyncio.timeout(1.0):
            return await awaitable

    task = asyncio.create_task(bounded())
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
    return task.result()


class _BeliefsMutationUoW(MutationUoW):
    async def _attempt(self, ctx, command, handler, digest):
        try:
            return await super()._attempt(ctx, command, handler, digest)
        except (SQLAlchemyError, sqlite3.Error) as exc:
            if _write_busy(exc):
                raise HttpError("E_REQUEST_IN_PROGRESS") from None
            raise


def _write_version(row, kind="belief"):
    value = row.version
    if kind == "fact":
        try:
            return normalize_version(value)
        except ValueError:
            raise HttpError("E_INTERNAL") from None
    if type(value) is not int or value < 1:
        raise HttpError("E_INTERNAL")
    return value


def _write_valid(row, kind):
    _write_version(row, kind)
    if (
        type(row.confidence) not in (float, int)
        or not math.isfinite(row.confidence)
        or not 0 <= row.confidence <= 1
        or row.lifecycle_state not in {s.value for s in LifecycleState}
    ):
        raise HttpError("E_INTERNAL")
    if kind == "belief" and (
        type(row.proposition) is not str
        or not row.proposition.strip()
        or any(type(v) is not list or any(type(s) is not str for s in v) for v in (row.tags, row.source_ids))
    ):
        raise HttpError("E_INTERNAL")
    return row


def _write_cas(row, versions):
    if set(versions) - {row.id}:
        raise HttpError("E_VALIDATION")
    if row.id not in versions:
        raise HttpError("E_PRECONDITION_REQUIRED")
    actual = _write_version(row)
    if versions[row.id] != actual:
        raise HttpError("E_VERSION_CONFLICT", details={"id": row.id, "expected": versions[row.id], "actual": actual})


def _write_memory(row, kind, receipt_id, pending):
    version = _write_version(row, kind)
    return d.Memory(
        id=row.id,
        memory_type=kind,
        content=None,
        payload=None,
        confidence=row.confidence,
        lifecycle_state=row.lifecycle_state,
        revision=version,
        source_version=version,
        source=None,
        creator=None,
        created_at=utc(row.created_at),
        updated_at=utc(row.updated_at),
        risk_tags=[],
        receipt_id=receipt_id,
        masked=True,
        projection=d.ItemProjection(vector="unknown", graph="unknown", outbox="pending" if pending else "unknown"),
    )


def _write_receipt(receipt):
    history = []
    for entry in receipt.history:
        if not isinstance(entry, dict) or entry.get("kind") not in {
            "receipt",
            "lifecycle",
            "confidence",
            "admission",
            "relation",
        }:
            continue
        if type(entry.get("id")) is not str or type(entry.get("timestamp")) is not str:
            continue
        try:
            history.append(
                d.HistoryEvent(
                    id=entry["id"],
                    kind=entry["kind"],
                    timestamp=utc(entry["timestamp"]),
                    actor=None,
                    reason=None,
                    revision=entry.get("revision"),
                    from_state=entry.get("from_state"),
                    to_state=entry.get("to_state"),
                    receipt_id=receipt.id,
                    evidence=None,
                )
            )
        except (ValidationError, ValueError, TypeError):
            continue
    return d.Receipt(
        id=receipt.id,
        memory_type=receipt.memory_type,
        source=None,
        created_by=None,
        timestamp=utc(receipt.timestamp),
        confidence=receipt.confidence,
        verification_status=receipt.verification_status.value,
        lifecycle_state=receipt.lifecycle_state,
        source_version=receipt.version,
        history=history,
        history_complete=False,
    )


def _write_admission(decision: AdmissionDecision):
    metadata = decision.metadata
    return d.AdmissionResult(
        admitted=decision.admitted,
        tag=decision.tag.value,
        ttl_days=decision.ttl_days,
        score=decision.score,
        memory_kind=metadata.get("memory_kind") if isinstance(metadata.get("memory_kind"), str) else None,
        authority_level=metadata.get("authority_level") if isinstance(metadata.get("authority_level"), str) else None,
        epistemic_status=metadata.get("epistemic_status")
        if isinstance(metadata.get("epistemic_status"), str)
        else None,
        risk_tags=[tag for tag in metadata.get("risk_tags", []) if isinstance(tag, str)],
        reason_codes=list(decision.reason_codes),
    )


class BeliefsMutationHandler:
    def __init__(self, scope, ctx, *, ticket, gate=None, clock=None):
        self.scope, self.ctx, self.ticket = scope, ctx, ticket
        self.gate = MemoryAdmissionGate() if gate is None else gate
        self.clock = clock or (lambda: datetime.now(UTC))
        self._reset()

    def _reset(self):
        self.captured, self.observed, self.receipts = {}, {}, {}
        self.history_count, self.history_bytes, self.instructions = 0, 0, 0
        self.interrupted, self.effects, self.outbox_ids = None, [], []

    def _progress(self):
        self.instructions += 1000
        if time.monotonic() >= self.ctx.deadline:
            self.interrupted = "timeout"
        elif self.instructions >= VM_BUDGET:
            self.interrupted = "budget"
        return int(self.interrupted is not None)

    async def apply(self, session, command):
        self._reset()
        command = validate_belief_mutation_command(command)
        conn = await session.connection()

        def capture(sync, flush_context):
            for row in sync.new:
                if isinstance(row, LifecycleEventORM):
                    self.captured[row.id] = (
                        row.id,
                        row.memory_type,
                        row.memory_id,
                        row.from_state,
                        row.to_state,
                        utc(row.timestamp),
                    )
                    if len(self.captured) > 1026:
                        raise HttpError("E_INTERNAL")

        async def install(driver):
            if not callable(getattr(driver, "set_progress_handler", None)):
                raise HttpError("E_CAPABILITY_UNAVAILABLE")
            await driver.set_progress_handler(self._progress, 1000)

        async def clear():
            try:
                await conn.run_sync(
                    lambda c: c.connection.dbapi_connection.run_async(lambda a: a.set_progress_handler(None, 0))
                )
            except BaseException:
                await conn.invalidate()
                raise

        event.listen(session.sync_session, "after_flush", capture)
        try:
            async with asyncio.timeout_at(self.ctx.deadline):
                await conn.run_sync(lambda c: c.connection.dbapi_connection.run_async(install))
                if type(command) is d.ConflictsResolveCommand:
                    return await self.resolve_in_session(
                        session,
                        command.payload,
                        conflict_id=command.path_ids["id"],
                        expected_versions=dict(command.payload.expected_versions),
                        reason=command.payload.reason,
                        actor=self.ctx.principal.subject,
                    )
                return await self.set_in_session(
                    session,
                    command.payload,
                    expected_versions=dict(command.payload.expected_versions),
                    reason=command.payload.reason,
                    actor=self.ctx.principal.subject,
                )
        except LifecycleConflictError as exc:
            details = {}
            matches = [key for key in self.observed if key[1] == exc.memory_id]
            if len(matches) == 1:
                kind, ident = matches[0]
                details = {"id": ident}
                if type(exc.expected) is int and exc.expected >= 1:
                    details["expected"] = exc.expected
                row = await self._load(session, kind, ident)
                if row is not None and _write_version(row, kind) != self.observed[(kind, ident)][0]:
                    details["actual"] = _write_version(row, kind)
            raise HttpError("E_VERSION_CONFLICT", details=details) from None
        except LifecycleMemoryNotFoundError:
            raise HttpError("E_NOT_FOUND") from None
        except InvalidLifecycleTransitionError:
            raise HttpError("E_VALIDATION") from None
        except LifecyclePropagationLimitError:
            raise HttpError("E_STORAGE", details={"limit": 512}) from None
        except TimeoutError:
            raise HttpError("E_TIMEOUT") from None
        except (SQLAlchemyError, sqlite3.Error) as exc:
            if self.interrupted == "timeout":
                raise HttpError("E_TIMEOUT") from None
            if self.interrupted == "budget":
                raise HttpError("E_STORAGE", details={"limit": VM_BUDGET}) from None
            raise HttpError("E_REQUEST_IN_PROGRESS" if _write_busy(exc) else "E_STORAGE") from None
        except (HttpError, asyncio.CancelledError):
            raise
        except Exception:
            raise HttpError("E_INTERNAL") from None
        finally:
            event.remove(session.sync_session, "after_flush", capture)
            try:
                await _write_settle(clear())
            except BaseException:
                await _write_settle(conn.invalidate())
                raise

    async def _load(self, session, kind, ident):
        model = BeliefORM if kind == "belief" else FactORM
        row = await session.scalar(
            select(model).where(model.id == ident).limit(1).execution_options(populate_existing=True)
        )
        return _write_valid(row, kind) if row is not None else None

    async def _receipt(self, session, ident, kind):
        if ident in self.receipts:
            return self.receipts[ident]
        data = (
            await session.execute(
                text("SELECT length(CAST(history AS BLOB)),json_valid(history) FROM receipts WHERE id=:id LIMIT 1"),
                {"id": ident},
            )
        ).first()
        receipt = None
        if data is not None:
            if data[0] > 1048576:
                raise HttpError("E_STORAGE", details={"limit": 1048576})
            if (
                not data[1]
                or await session.scalar(
                    text("SELECT json_type(history) FROM receipts WHERE id=:id LIMIT 1"), {"id": ident}
                )
                != "array"
            ):
                raise HttpError("E_INTERNAL")
            rows = (
                await session.execute(
                    text("SELECT value FROM json_each((SELECT history FROM receipts WHERE id=:id LIMIT 1)) LIMIT 1025"),
                    {"id": ident},
                )
            ).all()
            if len(rows) > 1024:
                raise HttpError("E_STORAGE", details={"limit": 1024})
            self.history_count += len(rows)
            self.history_bytes += data[0]
            self._aggregate()
            receipt = await ReceiptRepository(session).get(ident)
            if receipt is None or receipt.memory_type != kind:
                raise HttpError("E_INTERNAL")
            self.history_bytes += len(json.dumps(receipt.history).encode()) - data[0]
            self._aggregate()
        self.receipts[ident] = receipt
        return receipt

    def _aggregate(self):
        if self.history_count > 8192 or self.history_bytes > 8388608:
            raise HttpError("E_STORAGE")

    def _history_budget(self, receipt, entries):
        existing = receipt.history if receipt is not None else []
        history = [*existing, *entries]
        size = len(json.dumps(history).encode())
        if len(history) > 1024 or size > 1048576:
            raise HttpError("E_STORAGE", details={"limit": 1024 if len(history) > 1024 else 1048576})
        self.history_count += len(entries)
        self.history_bytes += size - (len(json.dumps(existing).encode()) if receipt else 0)
        self._aggregate()
        return history

    async def _persist_receipt(self, session, row, kind, entries, *, source=None, actor=None, required=False):
        receipt = self.receipts.get(row.id)
        if receipt is None and not required:
            return None
        history = [*(receipt.history if receipt else []), *entries]
        if len(history) > 1024 or len(json.dumps(history).encode()) > 1048576:
            raise HttpError("E_STORAGE")
        if receipt is None:
            receipt = await ReceiptRepository(session).create(
                MemoryReceipt(
                    id=row.id,
                    memory_type=kind,
                    source=source,
                    created_by=actor,
                    timestamp=utc(self.clock()),
                    confidence=row.confidence,
                    lifecycle_state=row.lifecycle_state,
                    version=str(row.version),
                    history=history,
                )
            )
        else:
            fields = dict(
                confidence=row.confidence,
                lifecycle_state=row.lifecycle_state,
                version=str(row.version),
                updated_at=row.updated_at,
                history=history,
            )
            if source is not None:
                fields["source"] = source
            receipt = await ReceiptRepository(session).update(row.id, **fields)
        await session.flush()
        fresh = await session.scalar(select(type(row).id).where(type(row).id == row.id).limit(1))
        persisted = await session.scalar(text("SELECT history FROM receipts WHERE id=:id LIMIT 1"), {"id": row.id})
        if fresh != row.id or receipt is None or persisted is None or json.loads(persisted) != history:
            raise HttpError("E_STORAGE")
        self.receipts[row.id] = receipt
        return receipt

    async def _index(self, session, kind, row):
        if len(self.outbox_ids) >= 1027:
            raise HttpError("E_INTERNAL")
        payload = (
            dict(proposition=row.proposition, confidence=row.confidence, tags=list(row.tags), source=row.source)
            if kind == "belief"
            else dict(subject=row.subject, predicate=row.predicate, object=row.object, source=row.source)
        )
        try:
            entry = await OutboxRepository(session).add_entry(kind, row.id, "index_" + kind, payload)
        except (SQLAlchemyError, sqlite3.Error):
            raise
        except Exception:
            raise HttpError("E_OUTBOX") from None
        self.outbox_ids.append(entry.id)

    async def _plan_transitions(self, session, requests):
        if len(requests) > 2:
            raise HttpError("E_INTERNAL")
        frontier, plan = {}, []
        for request in requests:
            key = (request.memory_type, request.memory_id)
            root = await self._load(session, *key)
            if root is None:
                raise HttpError("E_NOT_FOUND")
            if not is_valid_transition(root.lifecycle_state, request.to_state):
                raise HttpError("E_VALIDATION")
            if _write_version(root, key[0]) != request.expected_version:
                raise HttpError(
                    "E_VERSION_CONFLICT",
                    details={
                        "id": root.id,
                        "expected": request.expected_version,
                        "actual": _write_version(root, key[0]),
                    },
                )
            frontier.setdefault(key, (_write_version(root, key[0]), root.lifecycle_state, root.confidence))
            version, state, confidence = frontier[key]
            plan.append((key, version + 1, state, request.to_state, confidence, request.reason, "lifecycle"))
            frontier[key] = (version + 1, request.to_state, confidence)
            if request.to_state not in {"superseded", "contradicted", "discarded"}:
                continue
            refs = (
                (
                    await session.execute(
                        text(
                            "SELECT source_id FROM claim_relations WHERE target_id=:id "
                            "AND relation_type='derived_from' "
                            "ORDER BY source_id COLLATE BINARY LIMIT 513"
                        ),
                        {"id": root.id},
                    )
                )
                .scalars()
                .all()
            )
            if len(refs) > 512:
                raise HttpError("E_STORAGE", details={"limit": 512})
            for ident in dict.fromkeys(refs):
                dependent = await self._load(session, "fact", ident)
                kind = "fact"
                if dependent is None:
                    dependent, kind = await self._load(session, "belief", ident), "belief"
                if dependent is None:
                    continue
                key = (kind, ident)
                frontier.setdefault(
                    key, (_write_version(dependent, kind), dependent.lifecycle_state, dependent.confidence)
                )
                version, state, confidence = frontier[key]
                confidence = max(0.1, round(confidence * 0.8, 6))
                plan.append((key, version + 1, state, state, confidence, "parent_invalidated", "confidence"))
                frontier[key] = (version + 1, state, confidence)
        if len(plan) > 1026 or len(frontier) > 1027:
            raise HttpError("E_INTERNAL")
        for key in frontier:
            row = await self._load(session, *key)
            self.observed[key] = (_write_version(row, key[0]), row.lifecycle_state, row.confidence)
        return plan, frontier

    def _event_entry(self, effect, event_id, timestamp):
        key, version, before, after, confidence, reason, kind = effect
        return dict(
            id=event_id,
            kind=kind,
            timestamp=utc(timestamp).isoformat(timespec="microseconds"),
            revision=version,
            from_state=before,
            to_state=after,
            receipt_id=key[1],
            actor=self.ctx.principal.subject,
            reason=reason,
        )

    async def _transitions(self, session, requests, plan, frontier):
        captured_before = len(self.captured)
        provider = self.scope.application.read_resources().provider
        results = await LifecycleService(provider).transition_many_in_session(
            session, requests, graph=None, strict_remote=True
        )
        captured = list(self.captured.values())[captured_before:]
        if len(captured) != len(plan):
            raise HttpError("E_INTERNAL")
        propagated = [p for result in results for p in result.propagated]
        expected = [e for e in plan if e[-1] == "confidence"]
        if [
            (p["memory_type"], p["memory_id"], normalize_version(p["version"]), p["confidence"]) for p in propagated
        ] != [(*e[0], e[1], e[4]) for e in expected]:
            raise HttpError("E_INTERNAL")
        entries = {}
        for record, effect in zip(captured, plan, strict=True):
            eid, kind, ident, before, after, stamp = record
            if ((kind, ident), before, after) != (effect[0], effect[2], effect[3]):
                raise HttpError("E_INTERNAL")
            entries.setdefault((kind, ident), []).append(self._event_entry(effect, eid, stamp))
        rows = {}
        for key, predicate in frontier.items():
            row = await self._load(session, *key)
            if row is None or (_write_version(row, key[0]), row.lifecycle_state, row.confidence) != predicate:
                raise HttpError("E_VERSION_CONFLICT", details={"id": key[1]})
            rows[key] = row
            await self._persist_receipt(session, row, key[0], entries[key])
            await self._index(session, key[0], row)
        self.effects = captured
        return rows

    async def _claims(self, session, sources):
        claims, mapped, refs = [], [], []
        if len(sources) > 1024:
            raise HttpError("E_STORAGE", details={"limit": 1024})
        for summary in sources:
            claims.append(
                dict(
                    method=summary.method,
                    sources=list(summary.sources),
                    session_id=summary.session_id,
                    confidence=summary.confidence,
                    source_date=summary.source_date,
                    derived_from=list(summary.derived_from),
                    claim_type=summary.claim_type,
                    trusted=False,
                    origin="remote_claim",
                )
            )
            refs.extend(summary.derived_from)
            for source in summary.sources or [""]:
                mapped.append(
                    (
                        summary.method if summary.method is not None else "observation",
                        source,
                        summary.confidence if summary.confidence is not None else 0.5,
                    )
                )
                if len(mapped) > 1024:
                    raise HttpError("E_STORAGE", details={"limit": 1024})
            if len(refs) > 1024:
                raise HttpError("E_STORAGE", details={"limit": 1024})
        for ident in dict.fromkeys(refs):
            found = await session.scalar(
                text(
                    "SELECT id FROM facts WHERE id=:id UNION ALL SELECT id FROM beliefs WHERE id=:id "
                    "UNION ALL SELECT id FROM decisions WHERE id=:id "
                    "UNION ALL SELECT id FROM skills WHERE id=:id LIMIT 1"
                ),
                {"id": ident},
            )
            if found is None:
                raise HttpError("E_NOT_FOUND")
        return claims, mapped

    async def _evidence(self, session, row, mapped, actor):
        expected = []
        for method, source, weight in mapped:
            model = Evidence(belief_id=row.id, source_type=method, source_id=source, weight=weight, contributor=actor)
            session.add(EvidenceORM.from_pydantic(model))
            expected.append((model.id, method, source, weight, actor))
        await session.flush()
        rows = (
            (
                await session.execute(
                    select(EvidenceORM).where(EvidenceORM.belief_id == row.id).order_by(EvidenceORM.id).limit(1025)
                )
            )
            .scalars()
            .all()
        )
        observed = {r.id: (r.id, r.source_type, r.source_id, r.weight, r.contributor) for r in rows}
        if len(rows) > 1024 or any(observed.get(e[0]) != e for e in expected):
            raise HttpError("E_STORAGE")
        source_ids = list(row.source_ids)
        fresh = await self._load(session, "belief", row.id)
        if fresh is None or fresh.source_ids != source_ids:
            raise HttpError("E_INTERNAL")

    async def set_in_session(self, session, payload, *, expected_versions, reason, actor):
        payload = validate_belief_mutation_command(
            d.BeliefsSetCommand(
                operation="beliefs.set",
                profile_id=self.ctx.profile_id,
                path_ids={},
                payload=payload,
            )
        ).payload
        if type(expected_versions) is not dict or any(type(v) is not int or v < 1 for v in expected_versions.values()):
            raise HttpError("E_VALIDATION")
        expected_versions = dict(expected_versions)
        if (
            actor != self.ctx.principal.subject
            or reason != payload.reason
            or expected_versions != payload.expected_versions
        ):
            raise HttpError("E_VALIDATION")
        target = None
        if payload.replace_belief_id is not None:
            target = await self._load(session, "belief", payload.replace_belief_id)
            if target is None:
                raise HttpError("E_NOT_FOUND")
        candidates = (
            await session.execute(
                text(
                    "SELECT id,proposition FROM beliefs WHERE lifecycle_state='active' "
                    "ORDER BY id COLLATE BINARY LIMIT 513"
                )
            )
        ).all()
        if len(candidates) > 512:
            raise HttpError("E_STORAGE", details={"limit": 512})
        matches = []
        for row in candidates:
            if type(row.proposition) is not str or not row.proposition.strip():
                raise HttpError("E_INTERNAL")
            if row.proposition.strip().lower() == payload.proposition.strip().lower():
                matches.append(row.id)
        if len(matches) > 1 or (target is not None and matches and matches[0] != target.id):
            raise HttpError("E_VALIDATION")
        if target is None and matches:
            target = await self._load(session, "belief", matches[0])
        if target is None and expected_versions:
            raise HttpError("E_VALIDATION")
        if target is not None:
            _write_cas(target, expected_versions)
        reinforce = target is not None and payload.replace_belief_id is None
        claims, mapped = await self._claims(session, payload.sources)
        plan, frontier, requests = [], {}, []
        if payload.replace_belief_id is not None:
            requests = [
                LifecycleTransitionRequest(
                    target.id, "belief", "superseded", reason, actor, expected_versions[target.id]
                )
            ]
            plan, frontier = await self._plan_transitions(session, requests)
        if reinforce:
            existing = (
                await session.execute(
                    text("SELECT id FROM evidence WHERE belief_id=:id ORDER BY id LIMIT 1025"), {"id": target.id}
                )
            ).all()
            if len(existing) + len(mapped) > 1024:
                raise HttpError("E_STORAGE", details={"limit": 1024})
        decision = None
        if not reinforce:
            if not callable(getattr(self.gate, "classify", None)):
                raise HttpError("E_CAPABILITY_UNAVAILABLE")
            decision = self.gate.classify(payload.proposition, source_scope="remote", force=False, now=self.clock())
            if not isinstance(decision, AdmissionDecision):
                raise HttpError("E_INTERNAL")
            if not decision.admitted:
                raise HttpError("E_ADMISSION_REJECTED")
        now = utc(self.clock())
        ident = target.id if reinforce else str(uuid4())
        version = _write_version(target) + 1 if target is not None else 1
        confidence = (
            max(0.0, min(1.0, (target.confidence + payload.confidence) / 2)) if reinforce else payload.confidence
        )
        entry = dict(
            id=ident,
            kind="receipt",
            timestamp=now.isoformat(timespec="microseconds"),
            actor=actor,
            reason=reason,
            revision=version,
            receipt_id=ident,
            claims=claims,
            operation="beliefs.set",
            request_id=self.ctx.request_id,
            source=payload.source,
            tags=list(payload.tags),
            admission=decision.to_metadata() if decision else None,
        )
        if reinforce:
            entry.update(
                kind="confidence",
                previous_confidence=target.confidence,
                new_confidence=confidence,
                from_state="active",
                to_state="active",
            )
            self.observed[("belief", ident)] = (_write_version(target), target.lifecycle_state, target.confidence)
        grouped = {}
        for effect in plan:
            grouped.setdefault(effect[0], []).append(self._event_entry(effect, "0" * 36, now))
        for (kind, item_id), entries in grouped.items():
            receipt = await self._receipt(session, item_id, kind)
            if receipt is not None:
                self._history_budget(receipt, entries)
        receipt = await self._receipt(session, ident, "belief")
        self._history_budget(receipt, [entry])
        if reinforce:
            await LifecycleRepository(session).transition(
                BeliefRepository(session),
                memory_id=ident,
                memory_type="belief",
                from_state=target.lifecycle_state,
                to_state=target.lifecycle_state,
                expected_version=expected_versions[ident],
                confidence=confidence,
                reason=reason,
                triggered_by=actor,
            )
            row = await self._load(session, "belief", ident)
            captured = list(self.captured.values())
            if (
                len(captured) != 1
                or captured[0][1:5] != ("belief", ident, "active", "active")
                or row.version != version
            ):
                raise HttpError("E_INTERNAL")
            entry.update(
                id=captured[0][0],
                timestamp=captured[0][-1].isoformat(timespec="microseconds"),
                from_state="active",
                to_state="active",
            )
            row.tags = list(dict.fromkeys([*row.tags, *payload.tags]))
            row.last_reinforced_at = row.updated_at = now
        else:
            created = await BeliefRepository(session).create(
                Belief(
                    id=ident,
                    proposition=payload.proposition,
                    confidence=confidence,
                    source=payload.source,
                    creator=actor,
                    tags=list(payload.tags),
                    version=version,
                    created_at=now,
                    updated_at=now,
                    last_reinforced_at=now,
                )
            )
            row = await self._load(session, "belief", created.id)
        row.source_ids = list(dict.fromkeys([*row.source_ids, *(source for _, source, _ in mapped if source)]))
        await self._evidence(session, row, mapped, actor)
        await self._persist_receipt(session, row, "belief", [entry], source=payload.source, actor=actor, required=True)
        await self._index(session, "belief", row)
        returned = [row]
        if requests:
            final = await self._transitions(session, requests, plan, frontier)
            returned.append(final[("belief", target.id)])
        await session.flush()
        result = d.WriteResult(
            request_id=self.ctx.request_id,
            memories=[_write_memory(r, "belief", r.id if self.receipts.get(r.id) else None, True) for r in returned],
            receipts=[_write_receipt(self.receipts[r.id]) for r in returned if self.receipts.get(r.id)],
            admission=[_write_admission(decision)] if decision else [],
            outbox_ids=list(self.outbox_ids),
            reinforced_ids=[ident] if reinforce else [],
        )
        if len(result.model_dump_json().encode()) > 1048576:
            raise HttpError("E_STORAGE", details={"limit": 1048576})
        return d.WriteResult.model_validate(result.model_dump(), strict=True)

    async def resolve_in_session(self, session, payload, *, conflict_id, expected_versions, reason, actor):
        payload = validate_belief_mutation_command(
            d.ConflictsResolveCommand(
                operation="conflicts.resolve",
                profile_id=self.ctx.profile_id,
                path_ids={"id": conflict_id},
                payload=payload,
            )
        ).payload
        if (
            type(expected_versions) is not dict
            or any(type(v) is not int or v < 1 for v in expected_versions.values())
            or expected_versions != payload.expected_versions
            or actor != self.ctx.principal.subject
            or reason != payload.reason
        ):
            raise HttpError("E_VALIDATION")
        expected_versions = dict(expected_versions)
        pair = [await self._load(session, "belief", ident) for ident in (payload.belief_a_id, payload.belief_b_id)]
        if any(row is None for row in pair):
            raise HttpError("E_NOT_FOUND")
        for row in pair:
            _write_cas(row, {row.id: expected_versions[row.id]})
        before = [dict(id=r.id, version=r.version, state=r.lifecycle_state) for r in pair]
        action = payload.resolution
        parents = (
            pair[1:] if action == "keep_a" else pair[:1] if action == "keep_b" else pair if action != "defer" else []
        )
        target_state = (
            "superseded" if action == "merge" else "contradicted" if action == "mark_contradicted" else "discarded"
        )
        requests = [
            LifecycleTransitionRequest(r.id, "belief", target_state, reason, actor, expected_versions[r.id])
            for r in parents
        ]
        # Inspect raw direct refs before hydration: pair overlap is not a native demotion.
        for request in requests:
            if not is_valid_transition(
                next(r.lifecycle_state for r in pair if r.id == request.memory_id), target_state
            ):
                raise HttpError("E_VALIDATION")
        for request in requests:
            refs = (
                (
                    await session.execute(
                        text(
                            "SELECT source_id FROM claim_relations WHERE target_id=:id "
                            "AND relation_type='derived_from' ORDER BY source_id COLLATE BINARY LIMIT 513"
                        ),
                        {"id": request.memory_id},
                    )
                )
                .scalars()
                .all()
            )
            if len(refs) > 512:
                raise HttpError("E_STORAGE", details={"limit": 512})
            if set(refs) & set(expected_versions):
                raise HttpError("E_VERSION_CONFLICT")
        plan, frontier = await self._plan_transitions(session, requests)
        now, action_id = utc(self.clock()), str(uuid4())
        confidence = (pair[0].confidence + pair[1].confidence) / 2
        decision, evidence, new = None, [], None
        fields = ("source_type", "source_id", "weight", "contributor", "note", "created_at")
        if action == "merge":
            evidence = (
                await session.scalars(
                    select(EvidenceORM)
                    .where(EvidenceORM.belief_id.in_([r.id for r in pair]))
                    .order_by(
                        EvidenceORM.belief_id.collate("BINARY"),
                        EvidenceORM.created_at,
                        EvidenceORM.id.collate("BINARY"),
                    )
                    .limit(1025)
                )
            ).all()
            if len(evidence) > 1024:
                raise HttpError("E_STORAGE", details={"limit": 1024})
            for row in evidence:
                if (
                    any(type(getattr(row, f)) is not str for f in ("source_type", "source_id", "contributor"))
                    or type(row.weight) not in (int, float)
                    or not math.isfinite(row.weight)
                    or (row.note is not None and type(row.note) is not str)
                    or not isinstance(row.created_at, datetime)
                ):
                    raise HttpError("E_INTERNAL")
            if not callable(getattr(self.gate, "classify", None)):
                raise HttpError("E_CAPABILITY_UNAVAILABLE")
            decision = self.gate.classify(payload.new_proposition, source_scope="remote", force=False, now=self.clock())
            if not isinstance(decision, AdmissionDecision):
                raise HttpError("E_INTERNAL")
            if not decision.admitted:
                raise HttpError("E_ADMISSION_REJECTED")
            new = Belief(
                id=str(uuid4()),
                proposition=payload.new_proposition,
                confidence=min(1.0, confidence),
                creator=actor,
                source="conflict_resolution",
                tags=list(dict.fromkeys([*pair[0].tags, *pair[1].tags])),
                source_ids=list(
                    dict.fromkeys(
                        [*pair[0].source_ids, *pair[1].source_ids, *(r.source_id for r in evidence if r.source_id)]
                    )
                ),
                version=1,
                created_at=now,
                updated_at=now,
                last_reinforced_at=now,
            )
        grouped = {}
        for effect in plan:
            grouped.setdefault(effect[0], []).append(self._event_entry(effect, "0" * 36, now))
        for row in pair:
            await self._receipt(session, row.id, "belief")
        for (kind, ident), entries in grouped.items():
            receipt = await self._receipt(session, ident, kind)
            if receipt is not None:
                self._history_budget(receipt, entries)
        new_entry = dict(
            id=new.id if new else action_id,
            kind="receipt",
            timestamp=now.isoformat(timespec="microseconds"),
            operation="conflicts.resolve",
            actor=actor,
            reason=reason,
            admission=decision.to_metadata() if decision else None,
        )
        if new is not None:
            self._history_budget(None, [new_entry])
        entry = dict(
            id=action_id,
            kind="receipt",
            timestamp=now.isoformat(timespec="microseconds"),
            operation="conflicts.resolve",
            resolution=action,
            conflict_id=conflict_id,
            request_id=self.ctx.request_id,
            actor=actor,
            reason=reason,
            pair=[r.id for r in pair],
            before=before,
            after=[
                dict(
                    id=r.id,
                    version=frontier.get(("belief", r.id), (r.version, r.lifecycle_state, r.confidence))[0],
                    state=frontier.get(("belief", r.id), (r.version, r.lifecycle_state, r.confidence))[1],
                )
                for r in pair
            ],
            effects=[self._event_entry(e, "0" * 36, now) for e in plan],
            new_id=new.id if new else None,
            admission=decision.to_metadata() if decision else None,
        )
        self._history_budget(None, [entry])
        if new is not None:
            await BeliefRepository(session).create(new)
            copied = []
            for original in evidence:
                copy = EvidenceORM(id=str(uuid4()), belief_id=new.id, **{f: getattr(original, f) for f in fields})
                session.add(copy)
                copied.append((copy.id, *(getattr(copy, f) for f in fields)))
            await session.flush()
            actual = (
                await session.scalars(select(EvidenceORM).where(EvidenceORM.belief_id == new.id).limit(1025))
            ).all()
            if {r.id: tuple(getattr(r, f) for f in fields) for r in actual} != {r[0]: r[1:] for r in copied}:
                raise HttpError("E_STORAGE")
            fresh = await self._load(session, "belief", new.id)
            if fresh.source_ids != new.source_ids:
                raise HttpError("E_STORAGE")
            await self._persist_receipt(
                session, fresh, "belief", [new_entry], source="conflict_resolution", actor=actor, required=True
            )
            await self._index(session, "belief", fresh)
        if requests:
            final = await self._transitions(session, requests, plan, frontier)
            pair = [final.get(("belief", r.id), r) for r in pair]
            entry["effects"] = [
                self._event_entry(e, record[0], record[-1]) for e, record in zip(plan, self.effects, strict=True)
            ]
        receipt = MemoryReceipt(
            id=action_id,
            memory_type="belief",
            source="conflict_resolution",
            created_by=actor,
            timestamp=now,
            confidence=confidence,
            version="1",
            history=[entry],
        )
        await ReceiptRepository(session).create(receipt)
        await session.flush()
        readback = await ReceiptRepository(session).get(action_id)
        raw = (
            await session.execute(
                text("SELECT id,source,created_by,version,history FROM receipts WHERE id=:id LIMIT 1"),
                {"id": action_id},
            )
        ).first()
        if (
            readback is None
            or readback.id != action_id
            or readback.history != [entry]
            or raw is None
            or tuple(raw[:4]) != (action_id, "conflict_resolution", actor, "1")
            or json.loads(raw[4]) != [entry]
        ):
            raise HttpError("E_STORAGE")
        returned = [*pair, *([fresh] if new is not None else [])]
        receipt_ids = list(dict.fromkeys([action_id, *(r.id for r in returned if self.receipts.get(r.id))]))
        result = d.ConflictResult(
            conflict_id=conflict_id,
            resolution=action,
            memories=[
                _write_memory(
                    r,
                    "belief",
                    r.id if self.receipts.get(r.id) else None,
                    ("belief", r.id) in frontier or (new is not None and r.id == new.id),
                )
                for r in returned
            ],
            receipt_ids=receipt_ids,
            audit_id=self.ticket,
            request_id=self.ctx.request_id,
        )
        if len(result.model_dump_json().encode()) > 1048576:
            raise HttpError("E_STORAGE", details={"limit": 1048576})
        return d.ConflictResult.model_validate(result.model_dump(), strict=True)


class RemoteBeliefsMutationPort:
    def __init__(
        self, deps, *, ticket_provider, publish_outcome, ttl_seconds=86400, events=None, gate=None, clock=None
    ):
        if (
            not callable(ticket_provider)
            or not callable(publish_outcome)
            or (events is not None and not callable(getattr(events, "publish", None)))
            or (clock is not None and not callable(clock))
        ):
            raise ValueError("invalid belief mutation dependencies")
        if type(ttl_seconds) is not int or not 1 <= ttl_seconds <= 604800:
            raise ValueError("invalid belief mutation TTL")
        self._deps, self._ticket_provider, self._publish_outcome = deps, ticket_provider, publish_outcome
        self._ttl, self._events = ttl_seconds, events
        self._gate = MemoryAdmissionGate() if gate is None else gate
        self._clock = clock or (lambda: datetime.now(UTC))

    async def execute(self, ctx: RequestContext, command: d.BeliefsSetCommand) -> d.WriteResult:
        if type(command) not in (d.BeliefsSetCommand, d.ConflictsResolveCommand) or (
            type(command),
            command.operation,
        ) not in (
            (d.BeliefsSetCommand, "beliefs.set"),
            (d.ConflictsResolveCommand, "conflicts.resolve"),
        ):
            raise HttpError("E_VALIDATION")
        self._deps.tokens.verify_principal(ctx.principal, now=datetime.now(UTC))
        require_permission(
            ctx.principal,
            "belief.resolve" if type(command) is d.ConflictsResolveCommand else "belief.write",
            profile_id=ctx.profile_id,
        )
        if ctx.profile_id is None or command.profile_id != ctx.profile_id:
            raise HttpError("E_VALIDATION")
        command = validate_belief_mutation_command(command)
        if type(ctx.idempotency_key) is not UUID:
            raise HttpError("E_IDEMPOTENCY_REQUIRED")
        if (
            type(ctx.deadline) not in (int, float)
            or not math.isfinite(ctx.deadline)
            or ctx.deadline <= time.monotonic()
        ):
            raise HttpError("E_TIMEOUT")
        try:
            ticket = self._ticket_provider()
        except Exception:
            raise HttpError("E_AUDIT_UNAVAILABLE") from None
        if type(ticket) is not str or not 1 <= len(ticket) <= 256:
            raise HttpError("E_AUDIT_UNAVAILABLE")
        if self._deps.leases is None:
            raise HttpError("E_RUNTIME_NOT_READY")
        started = time.monotonic()
        async with self._deps.leases.acquire(ctx) as scope:
            factory = scope.application.memory.session_factory
            uow = _BeliefsMutationUoW(
                factory,
                verify_principal=lambda p: self._deps.tokens.verify_principal(p, now=datetime.now(UTC)),
                ticket_provider=lambda: ticket,
                ttl_seconds=self._ttl,
            )
            handler = BeliefsMutationHandler(scope, ctx, ticket=ticket, gate=self._gate, clock=self._clock)
            try:
                result = await uow.run(ctx, command, handler)
            except asyncio.CancelledError:
                try:
                    recovered = await _write_settle(self._resolve(factory, ctx, command))
                except Exception:
                    recovered = None
                if recovered is None:
                    raise
                result, original, audit = recovered
                if original == ctx.request_id and audit != ticket:
                    raise HttpError("E_INTERNAL") from None
                MUTATION_REPLY.set(MutationReply(original != ctx.request_id, original))
            reply = MUTATION_REPLY.get()
            try:
                if reply is not None and reply.replayed:
                    await _write_settle(self._reconcile(factory, ctx, command))
                else:
                    await _write_settle(self._tail(factory, uow, ctx, command, result, ticket, handler, started))
            except Exception:
                # Canonical commit is authoritative; publication remains pending.
                pass
            return result

    async def _resolve(self, factory, ctx, command):
        async with factory() as session:
            row = await HttpRequestRepository(session).find(
                ctx.principal.subject, ctx.profile_id, command.operation, str(ctx.idempotency_key).lower()
            )
            if row is None or row.digest != _digest(command):
                return None
            return (
                d.RESULT_TYPES[command.operation].model_validate_json(row.response_snapshot),
                row.request_id,
                row.audit_ticket_id,
            )

    @staticmethod
    def _status(result):
        return 200 if type(result) is d.ConflictResult or result.reinforced_ids else 201

    async def _reconcile(self, factory, ctx, command):
        async with factory() as session, session.begin():
            repo = HttpRequestRepository(session)
            row = await repo.find(
                ctx.principal.subject, ctx.profile_id, command.operation, str(ctx.idempotency_key).lower()
            )
            if row is None or row.digest != _digest(command) or not row.pending_publication or not row.audit_ticket_id:
                return
            result = d.RESULT_TYPES[command.operation].model_validate_json(row.response_snapshot)
            outcome = AuditOutcome(
                self._status(result),
                None,
                tuple(result.receipt_ids) if type(result) is d.ConflictResult else tuple(r.id for r in result.receipts),
                0.0,
            )
            if await self._publish_outcome(AuditTicket(row.audit_ticket_id, row.request_id), outcome) is True:
                await repo.mark_published(row.request_id)

    async def _tail(self, factory, uow, ctx, command, result, ticket, handler, started):
        async def audit():
            outcome = AuditOutcome(
                self._status(result),
                None,
                tuple(result.receipt_ids) if type(result) is d.ConflictResult else tuple(r.id for r in result.receipts),
                (time.monotonic() - started) * 1000,
            )
            if await self._publish_outcome(AuditTicket(ticket, ctx.request_id), outcome) is True:
                await uow.mark_published(ctx.request_id)

        tasks = [asyncio.create_task(audit()), asyncio.create_task(self._produce(factory, ctx, command, handler))]
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _produce(self, factory, ctx, command, handler):
        records = tuple(handler.captured.values())
        if self._events is None or not records:
            return
        if len(records) > 1026:
            return
        observed = {}
        ids = [r[0] for r in records]
        async with factory() as session:
            for offset in range(0, len(ids), 128):
                rows = (
                    await session.scalars(
                        select(LifecycleEventORM).where(LifecycleEventORM.id.in_(ids[offset : offset + 128])).limit(129)
                    )
                ).all()
                for row in rows:
                    observed[row.id] = (
                        row.id,
                        row.memory_type,
                        row.memory_id,
                        row.from_state,
                        row.to_state,
                        utc(row.timestamp),
                    )
        if set(observed) != set(ids) or any(observed[r[0]] != r for r in records):
            return
        revisions = {key: value[0] for key, value in handler.observed.items()}
        events = []
        for index, (eid, kind, ident, before, after, stamp) in enumerate(records):
            key = (kind, ident)
            if key not in revisions:
                return
            revisions[key] += 1
            wire = d.Event(
                id=eid,
                type="memory.transitioned"
                if (type(command) is d.ConflictsResolveCommand and before != after)
                or (
                    type(command) is d.BeliefsSetCommand
                    and index == 0
                    and command.payload.replace_belief_id is not None
                )
                else "memory.updated",
                profile_id=ctx.profile_id,
                timestamp=stamp,
                request_id=ctx.request_id,
                memory_id=ident,
                job_id=None,
                severity="info",
                data=d.EventData(
                    revision=revisions[key],
                    from_state=before,
                    to_state=after,
                    operation=command.operation,
                    pending=None,
                    code=None,
                ),
            )
            if len(wire.model_dump_json().encode()) >= 4096:
                return
            events.append(wire)
        tasks = [asyncio.create_task(self._events.publish(event)) for event in events]
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
