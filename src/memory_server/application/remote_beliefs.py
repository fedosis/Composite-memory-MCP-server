"""Borrowed, bounded canonical belief reads; no mutation authority."""

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
