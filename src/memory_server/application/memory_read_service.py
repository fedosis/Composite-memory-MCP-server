"""Scoped, borrowed-session reads; fresh allowlisted DTOs, never raw caches.

upper is the maximum matching creation key at issue time, not an insertion
watermark; backdated inserts at or below upper may appear; pages are not an
MVCC snapshot. Admission heuristics are not a complete secret-detection policy.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from memory_server.admission import MemoryAdmissionGate
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import (
    MemoryReadPort,
    ProfileLeases,
    ProfileSession,
    RequestContext,
    TracePort,
)
from memory_server.transports.http.errors import HttpError

if TYPE_CHECKING:
    from memory_server.providers.sqlite_provider import SQLiteProvider

ReadClassification = Literal["clean", "sensitive", "unknown"]


@dataclass(frozen=True)
class ReadResources:
    provider: SQLiteProvider
    session_factory: Callable[[], AsyncSession]


@dataclass(frozen=True)
class ReadRecord:
    memory: dict
    memory_type: str
    receipt: dict | None = None
    history: tuple = ()


@dataclass(frozen=True)
class ReadKey:
    created_at: str
    memory_type: str
    id: str


@dataclass(frozen=True)
class HistoryKey:
    timestamp: str | None
    id: str


@dataclass(frozen=True)
class HistorySlice:
    events: tuple
    has_more: bool
    incomplete: bool
    upper: HistoryKey | None
    keys: tuple[HistoryKey, ...] = ()


@dataclass(frozen=True)
class MemorySlice:
    records: tuple[ReadRecord, ...]
    has_more: bool
    upper: ReadKey | None


def receipt_history_complete(page: HistorySlice) -> bool:
    if page.incomplete or page.has_more:
        return False
    # A complete available prefix still has no proof of coverage of all sources.
    # Keep this separate from known-incomplete; never infer source coverage from
    # an empty lookahead or the absence of unsupported events.
    return False


def utc(value):
    dt = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def b64(value):
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


def unb64(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise ValueError("invalid encoding")
    raw = base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)
    if b64(raw) != value:
        raise ValueError("noncanonical encoding")
    return raw


def filter_digest(command):
    return hashlib.sha256(
        canonical(
            {
                "path_ids": command.path_ids,
                "filters": command.payload.model_dump(mode="json", exclude={"cursor", "limit"}),
            }
        )
    ).hexdigest()


def key_order(key):
    if isinstance(key, ReadKey):
        return key.created_at, key.memory_type, key.id
    source, source_id, index = json.loads(unb64(key.id))
    return (key.timestamp is not None, key.timestamp or "", 0 if source == "receipt" else 1, source_id, index)


def classify_read_content(record: ReadRecord, gate) -> ReadClassification:
    """Visit raw fields before projection; no truncated-prefix declassification."""
    budget = 32768
    sensitive = False
    try:

        def visit(value, depth=0):
            nonlocal budget, sensitive
            if depth > 32:
                raise ValueError
            if isinstance(value, str):
                budget -= len(value)
                if budget < 0:
                    raise ValueError
                if value:
                    decision = gate.classify(value, force=False, source_scope="server")
                    if not decision.admitted:
                        raise ValueError
                    sensitive |= bool(decision.metadata.get("risk_tags"))
            elif isinstance(value, dict):
                if value.get("risk_tags"):
                    sensitive = True
                for k, v in value.items():
                    if not isinstance(k, str):
                        raise ValueError
                    visit(k, depth + 1)
                    visit(v, depth + 1)
            elif isinstance(value, (tuple, list)):
                for child in value:
                    visit(child, depth + 1)
            elif value is not None and type(value) not in (float, int, bool, datetime):
                raise ValueError

        memory = dict(record.memory)
        for field in ("tags", "source_ids", "rejected_alternatives", "steps", "constraints", "validation"):
            if field in memory and memory[field] is not None:
                if isinstance(memory[field], str):
                    memory[field] = json.loads(memory[field])
                if not isinstance(memory[field], list) or not all(isinstance(v, str) for v in memory[field]):
                    raise ValueError
        visit(memory)
        visit(record.receipt)
        visit(record.history)
        if record.memory.get("risk_tags"):
            sensitive = True
        return "sensitive" if sensitive else "clean"
    except Exception:
        return "unknown"


def revision(record):
    value = record.memory.get("version")
    if record.memory_type == "belief":
        return value if type(value) is int and value > 0 else None
    if record.memory_type == "fact" and type(value) is str and re.fullmatch(r"[0-9]+", value):
        try:
            parsed = int(value)
            return parsed if parsed > 0 else None
        except ValueError:
            return None
    return None


def check_reveal_revision(memory_type, expected_revision, observed_revision):
    if memory_type in {"decision", "skill"}:
        if expected_revision is not None:
            raise HttpError("E_VALIDATION")
    elif expected_revision is None:
        raise HttpError("E_PRECONDITION_REQUIRED")
    elif observed_revision is None:
        raise HttpError("E_CAPABILITY_UNAVAILABLE")
    elif expected_revision != observed_revision:
        raise HttpError("E_VERSION_CONFLICT")


def _payload(record):
    value, kind = record.memory, record.memory_type
    fields = {
        "fact": ("subject", "predicate", "object"),
        "belief": ("proposition", "tags", "source_ids"),
        "decision": ("context", "choice", "rejected_alternatives", "reason"),
        "skill": ("name", "purpose", "steps", "constraints", "validation", "success_rate"),
    }[kind]
    values = {field: value[field] for field in fields}
    for field in {"tags", "source_ids", "rejected_alternatives", "steps", "constraints", "validation"} & values.keys():
        if isinstance(values[field], str):
            values[field] = json.loads(values[field])
    if kind == "skill":
        values["skill_version"] = value["version"]
    cls = {"fact": d.FactPayload, "belief": d.BeliefPayload, "decision": d.DecisionPayload, "skill": d.SkillPayload}[
        kind
    ]
    return cls.model_validate(values)


def project_for_caller(ctx, operation, record, classification):
    """Fresh C4 allowlist, whole-null payload/evidence, no mutable raw cache."""
    raw = operation == "memories.reveal" and "memory.reveal" in ctx.principal.permissions and classification == "clean"
    value = record.memory
    payload = None
    if raw:
        try:
            payload = _payload(record)
        except (ValueError, TypeError, KeyError):
            raw = False
    version = value.get("version")
    safe_version = type(version) is int or (
        type(version) is str and len(version) <= 128 and re.fullmatch(r"[0-9]+(?:\.[0-9]+)*", version)
    )
    return d.Memory(
        id=value["id"],
        memory_type=record.memory_type,
        content=json.dumps(payload.model_dump(), ensure_ascii=False) if raw else None,
        payload=payload if raw else None,
        confidence=value["confidence"],
        lifecycle_state=value["lifecycle_state"],
        revision=revision(record),
        source_version=version if raw or safe_version else None,
        source=value.get("source") if raw else None,
        creator=value.get("creator") if raw else None,
        created_at=utc(value["created_at"]),
        updated_at=utc(value["updated_at"]),
        risk_tags=[],
        receipt_id=record.receipt["id"] if record.receipt else None,
        masked=not raw,
        projection=d.ItemProjection(vector="unknown", graph="unknown", outbox="unknown"),
    )


def masked_event(event):
    value = event.model_dump() if isinstance(event, d.HistoryEvent) else event
    return d.HistoryEvent(
        id=value["id"],
        kind=value["kind"],
        timestamp=utc(value["timestamp"]) if value.get("timestamp") else None,
        revision=value.get("revision"),
        from_state=value.get("from_state"),
        to_state=value.get("to_state"),
        receipt_id=value.get("receipt_id"),
        actor=None,
        reason=None,
        evidence=None,
    )


class MemoryReadService(MemoryReadPort):
    """One acquired scope, fresh explicit session and read transaction per read.

    upper is the maximum matching creation key at issue time, not an insertion
    watermark; backdated inserts at or below upper may appear; pages are not an
    MVCC snapshot.
    """

    def __init__(self, leases, bind_read, *, classifier, traces=None, cursor_codec=None):
        self.leases, self.bind_read, self.classifier = leases, bind_read, classifier
        self.traces, self.cursor_codec = traces, cursor_codec

    async def execute(self, ctx: RequestContext, command: d.MemoryReadCommand) -> d.MemoryReadResult:
        try:
            return await self._execute(ctx, command)
        except SQLAlchemyError:
            raise HttpError("E_STORAGE") from None

    async def _execute(self, ctx: RequestContext, command):
        permission = "memory.reveal" if command.operation == "memories.reveal" else "memory.read"
        if permission not in ctx.principal.permissions or ctx.profile_id not in ctx.principal.profiles:
            raise HttpError("E_PERMISSION_DENIED")
        if command.profile_id != ctx.profile_id:
            raise HttpError("E_VALIDATION")
        query = command.payload
        if command.operation == "memories.list" and any(
            getattr(query, n) for n in ("source", "tags", "risk_tags", "authority_level", "epistemic_status")
        ):
            raise HttpError("E_PERMISSION_DENIED")
        if command.operation == "lookup" and query.kind in ("request", "relation"):
            if ("audit.read" if query.kind == "request" else "relation.read") not in ctx.principal.permissions:
                raise HttpError("E_PERMISSION_DENIED")
            raise HttpError("E_CAPABILITY_UNAVAILABLE")
        state = None
        if getattr(query, "cursor", None):
            if self.cursor_codec is None:
                raise HttpError("E_CAPABILITY_UNAVAILABLE")
            state = self.cursor_codec.validate(ctx.principal, command, now=self.cursor_codec.clock())
        async with self.leases.acquire(ctx) as scope:
            if command.operation == "lookup" and query.kind == "trace":
                if self.traces is None:
                    raise HttpError("E_CAPABILITY_UNAVAILABLE")
                result = await self.traces.execute(
                    ctx,
                    d.TracesGetCommand(
                        operation="traces.get", profile_id=ctx.profile_id, path_ids={"id": query.id}, payload=d.Empty()
                    ),
                )
                result = d.Trace.model_validate(result.model_dump())
                if result.profile_id != ctx.profile_id or result.id != query.id:
                    raise HttpError("E_NOT_FOUND")

                def contribution(item):
                    return d.TraceContribution(
                        memory_id=item.memory_id,
                        source=item.source,
                        raw_score=item.raw_score,
                        normalized_score=item.normalized_score,
                        rank_before_merge=item.rank_before_merge,
                        rank_after_merge=item.rank_after_merge,
                        matched_terms=[],
                        relation_path=[],
                        dedup_winner_id=item.dedup_winner_id,
                    )

                # Only canonical hashes may cross this provenance boundary.
                # Unknown/free-text build labels are not safe GET metadata.
                result = d.Trace(
                    id=result.id,
                    request_id=result.request_id,
                    profile_id=result.profile_id,
                    operation=result.operation,
                    query=None,
                    stages={k: [contribution(v) for v in items] for k, items in result.stages.items()},
                    stage_status=dict(result.stage_status),
                    merge=[contribution(v) for v in result.merge],
                    deduplicated=[contribution(v) for v in result.deduplicated],
                    final=[contribution(v) for v in result.final],
                    created_at=result.created_at,
                    expires_at=result.expires_at,
                    settings_digest=(
                        result.settings_digest if re.fullmatch(r"[0-9a-f]{64}", result.settings_digest) else ""
                    ),
                    code_revision=(
                        result.code_revision
                        if re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", result.code_revision)
                        else ""
                    ),
                )
                return d.LookupResult(kind="trace", id=query.id, target=result, found=True)
            resources = self.bind_read(scope)
            async with resources.session_factory() as session:
                async with session.begin():
                    # sqlite3 legacy transaction mode does not BEGIN on SELECT.
                    # Establish the actual read snapshot before max/page queries.
                    await session.execute(text("BEGIN"))
                    provider = resources.provider
                    if command.operation == "memories.list":
                        page = await provider.read_memory_page(
                            session,
                            query,
                            after=state["last"] if state else None,
                            upper=state["upper"] if state else None,
                        )
                        items = [
                            project_for_caller(ctx, command.operation, record, "unknown") for record in page.records
                        ]
                        last = (
                            None
                            if not items
                            else ReadKey(
                                utc(items[-1].created_at).isoformat(timespec="microseconds"),
                                items[-1].memory_type,
                                items[-1].id,
                            )
                        )
                        return d.Page[d.Memory](
                            items=items,
                            has_more=page.has_more,
                            next_cursor=self._cursor(ctx, command, page, last),
                            request_id=ctx.request_id,
                        )
                    ident = query.id if command.operation == "lookup" else command.path_ids["id"]
                    record = await provider.read_memory(session, ident, getattr(query, "memory_type", None))
                    if command.operation == "memories.reveal":
                        check_reveal_revision(record.memory_type, query.expected_revision, revision(record))
                        history = await provider.read_history_page(
                            session, ident, record.memory_type, limit=200, after=None, upper=None
                        )
                        classified = ReadRecord(record.memory, record.memory_type, record.receipt, history.events)
                        classification = (
                            "unknown"
                            if history.incomplete or history.has_more
                            else classify_read_content(classified, self.classifier)
                        )
                        return project_for_caller(ctx, command.operation, record, classification)
                    if (
                        command.operation == "history.list"
                        or command.operation == "receipts.get"
                        or (command.operation == "lookup" and query.kind == "receipt")
                    ):
                        page = await provider.read_history_page(
                            session,
                            ident,
                            record.memory_type,
                            limit=query.limit if command.operation == "history.list" else 200,
                            after=state["last"] if state else None,
                            upper=state["upper"] if state else None,
                        )
                        events = [masked_event(event) for event in page.events]
                        if command.operation == "history.list":
                            return d.Page[d.HistoryEvent](
                                items=events,
                                has_more=page.has_more,
                                next_cursor=self._cursor(ctx, command, page, page.keys[-1] if page.keys else None),
                                request_id=ctx.request_id,
                            )
                        receipt = record.receipt
                        if receipt is None:
                            raise HttpError("E_NOT_FOUND")
                        result = d.Receipt(
                            id=receipt["id"],
                            memory_type=record.memory_type,
                            source=None,
                            created_by=None,
                            source_version=None,
                            timestamp=utc(receipt["timestamp"]),
                            confidence=receipt["confidence"],
                            verification_status=receipt["verification_status"],
                            lifecycle_state=receipt["lifecycle_state"],
                            history=events,
                            history_complete=receipt_history_complete(page),
                        )
                    else:
                        result = project_for_caller(ctx, command.operation, record, "unknown")
                    if command.operation == "lookup":
                        return d.LookupResult(kind=query.kind, id=query.id, target=result, found=True)
                    return result

    def _cursor(self, ctx, command, page, last):
        if not page.has_more:
            return None
        if self.cursor_codec is None or last is None or page.upper is None:
            raise HttpError("E_CAPABILITY_UNAVAILABLE")
        return self.cursor_codec.issue(
            ctx, command, last=last, upper=page.upper, limit=command.payload.limit, now=self.cursor_codec.clock()
        )

    async def lookup(self, ctx: RequestContext, query: d.LookupQuery) -> d.LookupResult:
        command = d.COMMAND_TYPES["lookup"](operation="lookup", profile_id=ctx.profile_id, path_ids={}, payload=query)
        return await self.execute(ctx, command)


def compose_memory_read_port(
    leases: ProfileLeases,
    bind_read: Callable[[ProfileSession], ReadResources],
    *,
    classifier: MemoryAdmissionGate,
    traces: TracePort | None = None,
) -> MemoryReadPort:
    return MemoryReadService(leases, bind_read, classifier=classifier, traces=traces)
