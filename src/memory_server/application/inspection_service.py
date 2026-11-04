"""Admitted, read-only inspection of admission policy and canonical mutation impact."""

from __future__ import annotations

import asyncio
import base64
import json
import math
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from time import monotonic
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy import bindparam, event, text
from sqlalchemy.exc import SQLAlchemyError
from storage.models.relation import ClaimRelationORM
from storage.repositories.lifecycle_repo import normalize_version
from storage.repositories.relation_revision_repo import RelationRevisionRepository

from memory_server.admission import MemoryAdmissionGate
from memory_server.application.memory_read_service import ReadResources, project_for_caller
from memory_server.control.policy import require_permission
from memory_server.evaluation.validator import is_valid_transition
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import GatewayDependencies, ProfileSession, RequestContext
from memory_server.models.receipt import LifecycleState
from memory_server.transports.http.errors import HttpError
from memory_server.transports.http.routes.inspectors import (
    _fields,
    _preview_matrix,
    _result_admission,
    _result_preview,
    _uuid,
)
from memory_server.transports.http.routes.profiles import validate_profile_id

_PROBE = """
SELECT id,'fact' AS kind,version,lifecycle_state FROM facts WHERE id=:id
UNION ALL SELECT id,'belief' AS kind,version,lifecycle_state FROM beliefs WHERE id=:id
UNION ALL SELECT id,'decision' AS kind,version,lifecycle_state FROM decisions WHERE id=:id
UNION ALL SELECT id,'skill' AS kind,version,lifecycle_state FROM skills WHERE id=:id
ORDER BY kind COLLATE BINARY LIMIT 4
"""
_BATCH = """
SELECT id,'fact' AS kind,version,lifecycle_state FROM facts WHERE id IN :ids
UNION ALL SELECT id,'belief' AS kind,version,lifecycle_state FROM beliefs WHERE id IN :ids
UNION ALL SELECT id,'decision' AS kind,version,lifecycle_state FROM decisions WHERE id IN :ids
UNION ALL SELECT id,'skill' AS kind,version,lifecycle_state FROM skills WHERE id IN :ids
ORDER BY id COLLATE BINARY,kind COLLATE BINARY LIMIT 400
"""
_REFS = """
SELECT source_id FROM claim_relations
WHERE target_id=:id AND relation_type='derived_from'
ORDER BY source_id COLLATE BINARY LIMIT 513
"""
_IMPACT_ONE = """
SELECT DISTINCT source_id FROM claim_relations
WHERE target_id=:a AND relation_type='derived_from' AND source_id<>:a
ORDER BY source_id COLLATE BINARY LIMIT 101
"""
_IMPACT_PAIR = """
SELECT DISTINCT source_id FROM claim_relations
WHERE target_id IN (:a,:b) AND relation_type='derived_from' AND source_id NOT IN (:a,:b)
ORDER BY source_id COLLATE BINARY LIMIT 101
"""
_INVALIDATING = frozenset(("superseded", "contradicted", "discarded"))


def _decode_handle(value, kind):
    """Application-side codec; direct commands do not trust transport validation."""
    if kind == "relation":
        prefix, limit, raw_limit, arity = "rel_", 2048, 1024, 3
    else:
        prefix, limit, raw_limit, arity = "conf_", 261, 128, 2
    try:
        if type(value) is not str or not value.startswith(prefix) or len(value) > limit:
            raise ValueError("invalid handle")
        encoded = value[len(prefix) :]
        raw = base64.b64decode(encoded + "=" * (-len(encoded) % 4), altchars=b"-_", validate=True)
        if len(raw) > raw_limit:
            raise ValueError("oversize handle")
        parts = json.loads(raw.decode("utf-8"))
        if type(parts) is not list or len(parts) != arity:
            raise ValueError("invalid tuple")
        for endpoint in parts[:2]:
            if type(endpoint) is not str or str(UUID(endpoint)) != endpoint:
                raise ValueError("noncanonical endpoint")
        if kind == "relation":
            if parts[2] not in ("supports", "contradicts", "derives"):
                raise ValueError("unsupported relation")
        elif parts[0] >= parts[1]:
            raise ValueError("unordered pair")
        compact = json.dumps(parts, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        canonical = prefix + base64.urlsafe_b64encode(compact).decode("ascii").rstrip("=")
        if canonical != value:
            raise ValueError("noncanonical handle")
        return tuple(parts)
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise HttpError("E_VALIDATION") from None


def _now():
    return datetime.now(UTC)


def _deadline(ctx):
    try:
        finite = type(ctx.deadline) in (int, float) and math.isfinite(ctx.deadline)
    except OverflowError:
        finite = False
    if not finite:
        raise HttpError("E_VALIDATION")
    if monotonic() >= ctx.deadline:
        raise HttpError("E_TIMEOUT")


def _instant(clock):
    value = clock()
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise HttpError("E_INTERNAL")
    return value.astimezone(UTC)


def _version(row):
    try:
        if row.kind == "fact":
            return normalize_version(row.version)
        if type(row.version) is not int or row.version < 1:
            raise ValueError("invalid belief revision")
        return row.version
    except (ValueError, TypeError):
        raise HttpError("E_INTERNAL") from None


def _state(row):
    try:
        return LifecycleState(row.lifecycle_state)
    except (ValueError, TypeError):
        raise HttpError("E_INTERNAL") from None


async def _settle_cleanup(cleanup: asyncio.Task) -> None:
    cancelled = False
    while not cleanup.done():
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            cancelled = True
        except BaseException:
            break
    try:
        cleanup.result()
    finally:
        if cancelled:
            raise asyncio.CancelledError


@dataclass(frozen=True)
class InspectionService:
    deps: GatewayDependencies
    bind_read: Callable[[ProfileSession], ReadResources]
    gate: MemoryAdmissionGate = field(default_factory=MemoryAdmissionGate)
    clock: Callable[[], datetime] = _now

    def __post_init__(self):
        if not all(callable(value) for value in (self.bind_read, getattr(self.gate, "classify", None), self.clock)):
            raise ValueError("invalid inspection dependencies")

    async def execute(self, ctx: RequestContext, command: d.InspectionCommand) -> d.InspectionResult:
        try:
            op = command.operation
            if op not in ("admission.preview", "mutations.preview") or type(command) is not d.COMMAND_TYPES[op]:
                raise ValueError("invalid command")
            payload_type = d.AdmissionRequest if op == "admission.preview" else d.PreviewRequest
            if type(command.payload) is not payload_type:
                raise ValueError("invalid payload")
            command = d.COMMAND_TYPES[op].model_validate(_fields(command), strict=True)
            if command.profile_id != ctx.profile_id:
                raise ValueError("profile binding mismatch")
            if command.profile_id is None:
                raise ValueError("profile required")
            validate_profile_id(command.profile_id)
            expected_paths = set() if op == "admission.preview" else {"id"}
            if set(command.path_ids) != expected_paths:
                raise ValueError("invalid paths")
            if op == "mutations.preview":
                _uuid(command.path_ids["id"])
                _preview_matrix(command.payload)
        except (ValidationError, ValueError, TypeError, AttributeError, KeyError, UnicodeError, RecursionError):
            raise HttpError("E_VALIDATION") from None
        require_permission(
            ctx.principal, "memory.write" if op == "admission.preview" else "memory.read", profile_id=ctx.profile_id
        )
        self.deps.tokens.verify_principal(ctx.principal, now=datetime.now(UTC))
        _deadline(ctx)
        if self.deps.leases is None:
            raise HttpError("E_RUNTIME_NOT_READY")
        try:
            async with self.deps.leases.acquire(ctx) as scope:
                if op == "admission.preview":
                    result = await self._admission(command.payload)
                else:
                    resources = self.bind_read(scope)
                    if not callable(getattr(resources, "session_factory", None)) or not callable(
                        getattr(resources.provider, "read_memory", None)
                    ):
                        raise HttpError("E_CAPABILITY_UNAVAILABLE")
                    result = await self._preview(ctx, command, resources)
                self.deps.tokens.verify_principal(ctx.principal, now=datetime.now(UTC))
                _deadline(ctx)
                validator = _result_admission if op == "admission.preview" else _result_preview
                result = validator(result)
                if op == "mutations.preview":
                    self._validate_binding(command, result)
                return result
        except (SQLAlchemyError, sqlite3.Error) as exc:  # noqa: F841 - mutation privacy seam
            raise HttpError("E_STORAGE") from None
        except TimeoutError:
            raise HttpError("E_TIMEOUT") from None

    async def _admission(self, payload: d.AdmissionRequest) -> d.AdmissionResult:
        try:
            decision = self.gate.classify(
                payload.text, source_scope=payload.source_scope, now=_instant(self.clock), force=False
            )
            metadata = decision.metadata
            if type(metadata) is not dict:
                raise ValueError("invalid metadata")
            for key in ("memory_kind", "authority_level", "epistemic_status"):
                if metadata.get(key) is not None and type(metadata[key]) is not str:
                    raise ValueError("invalid metadata scalar")
            for value in (metadata.get("risk_tags", []), decision.reason_codes):
                if type(value) is not list or any(type(item) is not str for item in value):
                    raise ValueError("invalid policy codes")
            if type(decision.admitted) is not bool:
                raise ValueError("invalid admission")
            if decision.ttl_days is not None and type(decision.ttl_days) is not int:
                raise ValueError("invalid TTL")
            if type(decision.score) not in (int, float) or not math.isfinite(decision.score):
                raise ValueError("invalid score")
            result = d.AdmissionResult(
                admitted=decision.admitted,
                tag=decision.tag.value,
                ttl_days=decision.ttl_days,
                score=decision.score,
                memory_kind=metadata.get("memory_kind"),
                authority_level=metadata.get("authority_level"),
                epistemic_status=metadata.get("epistemic_status"),
                risk_tags=list(metadata.get("risk_tags", [])),
                reason_codes=list(decision.reason_codes),
            )
            return _result_admission(result)
        except HttpError:
            raise
        except Exception:
            raise HttpError("E_INTERNAL") from None

    async def _preview(self, ctx: RequestContext, command: d.Command, resources: ReadResources) -> d.PreviewResult:
        snapshot = _PreviewSnapshot(resources.session_factory(), resources, ctx, self.clock)
        return await snapshot.execute(command)

    def _validate_binding(self, command, result):
        """The request-free route validator cannot establish these bindings."""
        root = command.path_ids["id"]
        payload = command.payload
        if result.memory_id != root:
            raise HttpError("E_INTERNAL")
        extra = set(result.observed_versions) - set(result.affected_ids)
        if payload.operation == "relation_delete":
            endpoints = _decode_handle(payload.relation_id, "relation")[:2]
            mandatory = list(dict.fromkeys(endpoints))
            vocabulary = {"relation_delete"}
            if extra != {payload.relation_id}:
                raise HttpError("E_INTERNAL")
        elif payload.operation == "resolve":
            mandatory = list(_decode_handle(payload.conflict_id, "conflict"))
            vocabulary = {"keep_a", "keep_b", "merge", "mark_contradicted", "discard_both", "defer"}
        elif payload.operation == "invalidate":
            mandatory = [root]
            vocabulary = {"invalidate"}
        else:
            mandatory = [root]
            vocabulary = {state.value for state in LifecycleState}
        if payload.operation != "relation_delete" and extra:
            raise HttpError("E_INTERNAL")
        if result.affected_ids[: len(mandatory)] != mandatory:
            raise HttpError("E_INTERNAL")
        rest = result.affected_ids[len(mandatory) :]
        if rest != sorted(rest) or set(rest) & set(mandatory):
            raise HttpError("E_INTERNAL")
        if not set(result.allowed_actions) <= vocabulary:
            raise HttpError("E_INTERNAL")


class _PreviewSnapshot:
    def __init__(self, session, resources, ctx, clock):
        self.session, self.resources, self.ctx, self.clock = session, resources, ctx, clock
        self.statements = 0
        self.instructions = 0
        self.abort_reason = None
        self.connection = None
        self._listener_installed = False
        self._rollback_listener_installed = False
        self._progress_installed = False
        self._cleaning = False

    def _progress(self):
        if monotonic() >= self.ctx.deadline:
            self.abort_reason = "timeout"
            return 1
        self.instructions += 1000
        if self.instructions >= 2_000_000:
            self.abort_reason = "budget"
            return 1
        return 0

    def _statement(self, connection, cursor, statement, parameters, context, executemany):
        _deadline(self.ctx)
        token = statement.lstrip().split(None, 1)[0].upper()
        if token not in ("SELECT", "BEGIN", "ROLLBACK"):
            raise HttpError("E_STORAGE")
        if self.statements >= 63:
            raise HttpError("E_STORAGE", details={"limit": 64})
        self.statements += 1

    def _rollback(self, connection):
        self.statements += 1
        if self.statements > 64:
            raise HttpError("E_STORAGE", details={"limit": 64})

    async def _handler(self, connection, callback, step):
        def install(conn):
            bridge = getattr(conn.connection.dbapi_connection, "run_async", None)
            if not callable(bridge):
                raise HttpError("E_CAPABILITY_UNAVAILABLE")

            async def set_handler(driver):
                setter = getattr(driver, "set_progress_handler", None)
                if not callable(setter):
                    raise HttpError("E_CAPABILITY_UNAVAILABLE")
                await setter(callback, step)

            return bridge(set_handler)

        run_sync = getattr(connection, "run_sync", None)
        if not callable(run_sync):
            raise HttpError("E_CAPABILITY_UNAVAILABLE")
        try:
            await run_sync(install)
        except (AttributeError, NotImplementedError):
            raise HttpError("E_CAPABILITY_UNAVAILABLE") from None

    async def execute(self, command):
        session = self.session
        session.autoflush = False
        try:
            async with asyncio.timeout_at(self.ctx.deadline):
                connection = await session.connection()
                self.connection = connection
                event.listen(connection.sync_connection, "before_cursor_execute", self._statement)
                self._listener_installed = True
                event.listen(connection.sync_connection, "rollback", self._rollback)
                self._rollback_listener_installed = True
                await self._handler(connection, self._progress, 1000)
                self._progress_installed = True
                await session.execute(text("BEGIN"))
                self.clock_at_snapshot = _instant(self.clock)
                return await self._inspect(command)
        except (SQLAlchemyError, sqlite3.Error) as exc:  # noqa: F841 - mutation privacy seam
            if self.abort_reason == "timeout" or monotonic() >= self.ctx.deadline:
                raise HttpError("E_TIMEOUT") from None
            if self.abort_reason == "budget":
                raise HttpError("E_STORAGE", details={"limit": 2_000_000}) from None
            raise HttpError("E_STORAGE") from None
        except TimeoutError:
            raise HttpError("E_TIMEOUT") from None
        finally:
            cleanup = asyncio.create_task(self._cleanup(self.connection))
            try:
                await _settle_cleanup(cleanup)
            except HttpError:
                if self.abort_reason == "timeout" or monotonic() >= self.ctx.deadline:
                    raise HttpError("E_TIMEOUT") from None
                raise

    async def _query(self, sql, params):
        _deadline(self.ctx)
        if self.statements >= 63:
            raise HttpError("E_STORAGE", details={"limit": 64})
        result = await self.session.execute(text(sql), params)
        return result.all()

    async def _probe(self, ids):
        found = {}
        for ident in dict.fromkeys(ids):
            result = await self._query(_PROBE, {"id": ident})
            if len(result) != 1:
                raise HttpError("E_NOT_FOUND")
            found[ident] = result[0]
        return found

    async def _refs(self, ident):
        return [row[0] for row in await self._query(_REFS, {"id": ident})]

    async def _impact(self, parents, mandatory):
        selected = []
        truncated = False
        if parents:
            params = {"a": parents[0]}
            sql = _IMPACT_ONE
            if len(parents) == 2:
                params["b"] = parents[1]
                sql = _IMPACT_PAIR
            raw = [row[0] for row in await self._query(sql, params)]
            truncated = len(raw) > 100
            candidates = raw[:100]
            if candidates:
                query = text(_BATCH).bindparams(bindparam("ids", expanding=True))
                result = await self.session.execute(query, {"ids": candidates})
                grouped = {}
                for row in result.all():
                    grouped.setdefault(row.id, []).append(row)
                for ident in candidates:
                    hits = grouped.get(ident, [])
                    if len(hits) > 1:
                        raise HttpError("E_NOT_FOUND")
                    if hits and hits[0].kind in ("fact", "belief"):
                        selected.append(hits[0])
        selected.sort(key=lambda row: row.id)
        selected = [row for row in selected if row.id not in mandatory]
        room = 100 - len(mandatory)
        truncated = truncated or len(selected) > room
        return selected[:room], truncated

    async def _relation(self, command):
        payload = command.payload
        relation_id = payload.relation_id
        key = _decode_handle(relation_id, "relation")
        mandatory = list(dict.fromkeys(key[:2]))
        if command.path_ids["id"] not in mandatory:
            raise HttpError("E_NOT_FOUND")
        found = await self._probe(mandatory)
        if any(row.kind not in ("fact", "belief") for row in found.values()):
            raise HttpError("E_VALIDATION")
        if found[command.path_ids["id"]].kind != payload.memory_type:
            raise HttpError("E_NOT_FOUND")
        canonical = await self.session.get(ClaimRelationORM, key)
        state = await RelationRevisionRepository(self.session).state(key, canonical.created_at if canonical else None)
        if canonical is None or state.revision is None or (state.latest and state.latest.deleted):
            raise HttpError("E_NOT_FOUND")
        observed = {ident: _version(found[ident]) for ident in mandatory}
        observed[relation_id] = state.revision
        feasible = True
        for ident in mandatory:
            incidence = await self._query(
                "SELECT 1 FROM claim_relations WHERE source_id=:id OR target_id=:id LIMIT 513", {"id": ident}
            )
            if len(incidence) > 512:
                feasible = False
        directed = await self._query(
            "SELECT 1 FROM claim_relations WHERE source_id=:s AND target_id=:t LIMIT 17",
            {"s": key[0], "t": key[1]},
        )
        feasible = feasible and len(directed) <= 16
        return mandatory, observed, ["relation_delete"] if feasible else [], [], found

    async def _resolve(self, command):
        pair = _decode_handle(command.payload.conflict_id, "conflict")
        if command.path_ids["id"] not in pair:
            raise HttpError("E_NOT_FOUND")
        found = await self._probe(pair)
        if any(row.kind != "belief" for row in found.values()):
            raise HttpError("E_VALIDATION")
        refs_by_parent = {parent: await self._refs(parent) for parent in pair}
        actions = {
            "keep_a": ((pair[1], "discarded"),),
            "keep_b": ((pair[0], "discarded"),),
            "merge": ((pair[0], "superseded"), (pair[1], "superseded")),
            "mark_contradicted": ((pair[0], "contradicted"), (pair[1], "contradicted")),
            "discard_both": ((pair[0], "discarded"), (pair[1], "discarded")),
            "defer": (),
        }
        allowed, parents = [], set()
        for action, targets in actions.items():
            feasible = True
            for parent, target in targets:
                refs = refs_by_parent[parent]
                if len(refs) > 512 or not is_valid_transition(_state(found[parent]), LifecycleState(target)):
                    feasible = False
                if set(refs) & set(pair):
                    feasible = False
            if action == "merge":
                evidence = await self._query(
                    "SELECT id FROM evidence WHERE belief_id IN (:a,:b) "
                    "ORDER BY belief_id COLLATE BINARY,created_at,id COLLATE BINARY LIMIT 1025",
                    {"a": pair[0], "b": pair[1]},
                )
                if len(evidence) > 1024:
                    feasible = False
            if feasible:
                allowed.append(action)
                parents.update(parent for parent, _ in targets)
        observed = {ident: _version(found[ident]) for ident in pair}
        return list(pair), observed, allowed, sorted(parents), found

    async def _inspect(self, command):
        payload = command.payload
        root = command.path_ids["id"]
        if payload.operation == "relation_delete":
            mandatory, observed, allowed, parents, found = await self._relation(command)
        elif payload.operation == "resolve":
            mandatory, observed, allowed, parents, found = await self._resolve(command)
        else:
            found = await self._probe([root])
            row = found[root]
            if row.kind not in ("fact", "belief"):
                raise HttpError("E_VALIDATION")
            if row.kind != payload.memory_type:
                raise HttpError("E_NOT_FOUND")
            current = _state(row)
            state_actions = [target.value for target in LifecycleState if is_valid_transition(current, target)]
            requested = "discarded" if payload.operation == "invalidate" else payload.to_state
            if requested not in state_actions:
                raise HttpError("E_VALIDATION")
            refs = await self._refs(root)
            allowed = [target for target in state_actions if len(refs) <= 512 or target not in _INVALIDATING]
            if payload.operation == "invalidate":
                allowed = ["invalidate"] if "discarded" in allowed else []
            parents = [root] if requested in _INVALIDATING and len(refs) <= 512 else []
            mandatory, observed = [root], {root: _version(row)}
        record = await self.resources.provider.read_memory(self.session, root, memory_type=found[root].kind)
        projection = project_for_caller(self.ctx, "mutations.preview", record, "unknown").projection
        selected, truncated = await self._impact(parents, mandatory)
        affected = mandatory + [row.id for row in selected]
        observed.update({row.id: _version(row) for row in selected})
        result = d.PreviewResult(
            memory_id=root,
            observed_versions=observed,
            allowed_actions=allowed,
            affected_ids=affected,
            affected_count=len(affected),
            count_exact=not truncated,
            truncated=truncated,
            projections=projection,
            expires_at=self.clock_at_snapshot + timedelta(seconds=30),
            preview_only=True,
        )
        return _result_preview(result)

    async def _cleanup(self, connection):
        failed = False
        if connection is not None and self._progress_installed:
            try:
                await self._handler(connection, None, 0)
            except BaseException:
                failed = True
                # rollback releases the session's borrowed connection to its
                # pool; discard a connection with an unreset handler first.
                try:
                    await connection.invalidate()
                except BaseException:
                    failed = True
        try:
            await self.session.rollback()
        except BaseException:
            failed = True
        if connection is not None:
            try:
                if self._listener_installed:
                    event.remove(connection.sync_connection, "before_cursor_execute", self._statement)
            except BaseException:
                failed = True
            try:
                if self._rollback_listener_installed:
                    event.remove(connection.sync_connection, "rollback", self._rollback)
            except BaseException:
                failed = True
        if failed and connection is not None:
            try:
                await connection.invalidate()
            except BaseException:
                failed = True
        try:
            await self.session.close()
        except BaseException:
            failed = True
        if failed:
            if connection is not None:
                try:
                    await connection.invalidate()
                except BaseException:
                    raise HttpError("E_STORAGE") from None
            raise HttpError("E_STORAGE")
