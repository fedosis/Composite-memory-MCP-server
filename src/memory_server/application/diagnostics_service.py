"""Borrowed-session and log-isolation primitives for S10 diagnostics."""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from math import isfinite
from time import monotonic
from typing import Awaitable, Callable, Mapping, Sequence, TypeVar
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from storage.outbox import OutboxDiagnosticRow, OutboxKey, OutboxRepository, classify_diagnostic_error

from memory_server.application.contracts import ReflectionCommand
from memory_server.application.graph_read_service import map_consistency
from memory_server.application.memory_read_service import ReadResources
from memory_server.application.reflection_service import ReflectionService
from memory_server.control.policy import require_permission
from memory_server.evaluation.auditor import MemoryAuditor
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import DiagnosticsPort, ProfileLeases, ProfileSession, RequestContext
from memory_server.providers.sqlite_provider import (
    DiagnosticsDeadlineExceeded,
    DiagnosticsReadWork,
    DiagnosticsWorkExceeded,
)
from memory_server.transports.http.errors import HttpError


@dataclass(frozen=True)
class DiagnosticsResources:
    canonical: ReadResources
    profile: d.Profile
    vector: object | None
    graph: object | None
    validator: object | None
    confidence: object | None
    reflection: object | None
    writer_owned: bool | None


_diagnostics_active: ContextVar[bool] = ContextVar("diagnostics_active", default=False)
_PROVIDER_LOGGERS = (
    "memory_server.providers.lancedb_provider",
    "memory_server.providers.qdrant_provider",
)


class _DiagnosticsProviderFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if _diagnostics_active.get():
            record.msg = "diagnostics backend failure"
            record.args = ()
            record.exc_info = None
            record.exc_text = None
            record.stack_info = None
        return True


_provider_filter = _DiagnosticsProviderFilter()


def install_diagnostics_logging_filter() -> None:
    """Install one task-aware filter on the two reviewed provider loggers."""
    for name in _PROVIDER_LOGGERS:
        logger = logging.getLogger(name)
        if _provider_filter not in logger.filters:
            logger.addFilter(_provider_filter)


@asynccontextmanager
async def diagnostics_log_scope():
    install_diagnostics_logging_filter()
    token = _diagnostics_active.set(True)
    try:
        yield
    finally:
        _diagnostics_active.reset(token)


T = TypeVar("T")
DiagnosticsOperation = Callable[[object, DiagnosticsReadWork, datetime], Awaitable[T]]


async def run_borrowed_diagnostics(
    ctx: RequestContext,
    resources: DiagnosticsResources | object,
    operation: DiagnosticsOperation[T],
    *,
    clock: Callable[[], float] = monotonic,
) -> T:
    """Run one diagnostics operation in one bounded borrowed SQLite snapshot."""
    remaining = ctx.deadline - clock()
    if remaining <= 0:
        raise HttpError("E_TIMEOUT")
    hard = min(ctx.deadline, clock() + 30.0)
    work = DiagnosticsReadWork(
        soft_deadline=hard - min(0.050, max(0.0, hard - clock()) / 10),
        hard_deadline=hard,
    )
    canonical = resources.canonical

    async def produce() -> T:
        sampled_at = datetime.now(UTC)
        async with diagnostics_log_scope():
            async with canonical.session_factory() as session:
                async with session.begin():
                    async with canonical.provider.diagnostics_window(session, work=work):
                        await session.execute(text("BEGIN"))
                        return await operation(session, work, sampled_at)

    producer = asyncio.create_task(produce())
    try:
        result = await asyncio.wait_for(asyncio.shield(producer), max(0.0, hard - clock()))
        work.check()
        return result
    except (asyncio.CancelledError, TimeoutError, DiagnosticsDeadlineExceeded):
        work.stop = work.stop or "timeout"
        raise HttpError("E_TIMEOUT") from None
    except DiagnosticsWorkExceeded:
        raise HttpError("E_STORAGE") from None
    except (SQLAlchemyError, sqlite3.Error, OSError, ValueError):
        if work.stop == "timeout":
            raise HttpError("E_TIMEOUT") from None
        raise HttpError("E_STORAGE") from None
    finally:
        while not producer.done():
            try:
                await asyncio.shield(producer)
            except asyncio.CancelledError:
                work.stop = work.stop or "timeout"
            except Exception:
                break
        if producer.done() and not producer.cancelled():
            producer.exception()


_TABLES = ("facts", "beliefs", "decisions", "skills")
_LIFECYCLE = (
    "candidate",
    "validated",
    "active",
    "stale",
    "archived",
    "forgotten",
    "superseded",
    "contradicted",
    "discarded",
)


@dataclass(frozen=True)
class AuditCheck:
    section: str
    code: str
    severity: str
    count: int | None
    available: bool = True


def _uuid(value: str) -> str:
    if str(UUID(value)) != value:
        raise ValueError
    return value


def map_outbox_item(row: OutboxDiagnosticRow) -> d.OutboxItem:
    operations = {"index_fact", "index_belief", "index_decision", "index_skill"}
    statuses = {"pending": "pending", "processing": "processing", "completed": "applied", "failed": "failed"}
    if (
        row.record_type not in {"fact", "belief", "decision", "skill"}
        or row.operation not in operations
        or row.status not in statuses
        or type(row.retry_count) is not int
        or row.retry_count < 0
    ):
        raise ValueError
    return d.OutboxItem(
        event_id=_uuid(row.id),
        memory_id=_uuid(row.record_id),
        record_type=row.record_type,
        operation=row.operation,
        status=statuses[row.status],
        attempts=row.retry_count,
        target_backend=None,
        last_error_code=classify_diagnostic_error(row.error),
        next_retry=None,
        created_at=row.created_at,
        processed_at=row.processed_at,
    )


def map_audit_result(
    ctx: RequestContext,
    checks: Sequence[AuditCheck],
    *,
    query: d.AuditQuery,
    sampled_at: datetime,
    snapshot_truncated: bool,
) -> d.AuditResult:
    coalesced = {}
    for check in checks:
        key = (check.section, check.code)
        previous = coalesced.get(key)
        if previous is None:
            coalesced[key] = check
        else:
            coalesced[key] = replace(
                check,
                available=previous.available and check.available,
                count=max(previous.count or 0, check.count or 0),
            )
    checks = tuple(coalesced.values())
    selected = sorted({check.section for check in checks})
    sections = {}
    findings = []
    for section in selected:
        group = [check for check in checks if check.section == section]
        sections[section] = (
            "unavailable"
            if any(not item.available for item in group)
            else "warning"
            if any((item.count or 0) > 0 for item in group)
            else "ok"
        )
    ordinal = 0
    for check in sorted(checks, key=lambda item: (item.section, item.code)):
        if (check.count or 0) > 0:
            ordinal += 1
            findings.append(
                d.AuditFinding(
                    id=f"s10:{check.section}:{check.code}:{ordinal}",
                    category="relations" if check.section == "graph_orphans" else check.section,
                    severity=check.severity,
                    object_kind=None,
                    object_id=None,
                    code=check.code,
                    aggregate=True,
                )
            )
    truncated = len(findings) > query.limit
    return d.AuditResult(
        findings=d.Page[d.AuditFinding](
            items=findings[: query.limit], next_cursor=None, has_more=False, request_id=ctx.request_id
        ),
        sections=sections,
        sampled_at=sampled_at,
        complete=not (snapshot_truncated or truncated or "unavailable" in sections.values()),
    )


def _strict_count(value: object) -> int:
    if type(value) is not int or value < 0:
        raise HttpError("E_INTERNAL")
    return value


def map_index_status(
    counts: Mapping[str, int],
    *,
    vector_count: int | None,
    outbox_pending: int,
    outbox_failed: int,
    graph_available: bool,
    sampled_at: datetime,
) -> d.IndexStatus:
    required = set(_TABLES)
    if set(counts) != required:
        raise HttpError("E_INTERNAL")
    canonical = sum(_strict_count(counts[key]) for key in _TABLES)
    if vector_count is not None:
        vector_count = _strict_count(vector_count)
    return d.IndexStatus(
        sqlite_records=canonical,
        vector_records=vector_count,
        graph_relations=None,
        vector_lag=None,
        graph_lag=None,
        outbox_pending=_strict_count(outbox_pending),
        outbox_failed=_strict_count(outbox_failed),
        graph_consistency=map_consistency("unknown" if graph_available else "unavailable"),
        last_success=None,
        sampled_at=sampled_at,
    )


def map_overview(
    resources: DiagnosticsResources,
    counts: Mapping[str, int],
    *,
    lifecycle_counts: Mapping[str, int],
    unresolved_conflicts: int,
    indexes: d.IndexStatus,
    schema_revision: str | None,
) -> d.Overview:
    if set(counts) != set(_TABLES) or set(lifecycle_counts) != set((*_LIFECYCLE, "unknown")):
        raise HttpError("E_INTERNAL")
    if type(resources.writer_owned) is not bool:
        raise HttpError("E_RUNTIME_NOT_READY")
    profile = resources.profile.model_copy(update={"storage_root": None})
    values = {key: _strict_count(counts[key]) for key in _TABLES}
    lifecycle = {key: _strict_count(lifecycle_counts[key]) for key in (*_LIFECYCLE, "unknown")}
    return d.Overview(
        profile=profile,
        **values,
        lifecycle_counts=lifecycle,
        unresolved_conflicts=_strict_count(unresolved_conflicts),
        audit_warnings=None,
        indexes=indexes,
        schema_revision=schema_revision,
        last_migration=None,
        writer_owned=resources.writer_owned,
    )


class BorrowedReflectionProvider:
    def __init__(self, beliefs, session, work):
        self.beliefs = list(beliefs)
        self.session = session
        self.work = work
        self.ids = tuple(belief.id for belief in self.beliefs)
        self.evidence_error = None

    async def search_beliefs(self, **_kwargs):
        return list(self.beliefs)

    async def _get_session(self):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, _kind, error, _traceback):
        if error is not None and self.evidence_error is None:
            self.evidence_error = error
        return False

    async def execute(self, statement, params=None):
        try:
            self.work.check()
            if (
                not self.ids
                or type(params) is not dict
                or not params
                or set(params) != {f"bid_{i}" for i in range(len(params))}
                or any(value not in self.ids for value in params.values())
            ):
                raise HttpError("E_INTERNAL")
            placeholders = ",".join(f":bid_{i}" for i in range(len(params)))
            where = f"WHERE belief_id IN ({placeholders})"
            source = (
                f"SELECT belief_id, COUNT(*) AS count, AVG(weight) AS avg_weight, source_type"
                f" FROM evidence {where} GROUP BY belief_id, source_type ORDER BY belief_id"
            )
            overall = (
                f"SELECT belief_id, COUNT(*) AS count, AVG(weight) AS avg_weight"
                f" FROM evidence {where} GROUP BY belief_id"
            )
            zero = f"SELECT COUNT(*) FROM evidence {where} AND weight = 0"
            sql = " ".join(str(statement).split())
            if sql == source:
                safe = (
                    f"SELECT belief_id, COUNT(*) AS count, AVG(weight) AS avg_weight, '[MASKED]' AS source_type"
                    f" FROM evidence {where} GROUP BY belief_id ORDER BY belief_id"
                )
            elif sql in (overall, zero):
                safe = sql
            else:
                raise HttpError("E_INTERNAL")
            result = await self.session.execute(text(safe), params)
            self.work.check()
            return result
        except Exception as error:
            self.evidence_error = self.evidence_error or error
            raise


def _raw_int(raw: Mapping[str, object], key: str) -> int:
    if key not in raw:
        raise HttpError("E_INTERNAL")
    value = raw[key]
    if type(value) is not int or value < 0:
        raise HttpError("E_INTERNAL")
    return value


def _raw_float(raw: Mapping[str, object], key: str) -> float:
    if key not in raw:
        raise HttpError("E_INTERNAL")
    value = raw[key]
    if type(value) not in (int, float) or not isfinite(value) or not 0.0 <= value <= 1.0:
        raise HttpError("E_INTERNAL")
    return value


def _safe_pair(
    pair: Mapping[str, object], *, relations: bool, selected: frozenset[str]
) -> tuple[dict[str, object] | None, bool]:
    if not isinstance(pair, Mapping):
        return None, True
    try:
        left, right = pair["belief_a_id"], pair["belief_b_id"]
        if type(left) is not str or type(right) is not str:
            return None, True
        left, right = _uuid(left), _uuid(right)
        if left not in selected or right not in selected:
            return None, True

        def score(key):
            if key not in pair:
                raise HttpError("E_INTERNAL")
            value = pair[key]
            if type(value) not in (int, float) or not isfinite(value) or not 0.0 <= value <= 1.0:
                raise HttpError("E_INTERNAL")
            return value

        detected, incomplete = pair.get("detected_at"), False
        if detected is not None:
            try:
                if type(detected) is not str:
                    raise ValueError
                parsed = datetime.fromisoformat(detected.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    raise ValueError
                detected = parsed.astimezone(UTC).isoformat()
            except ValueError:
                detected, incomplete = None, True
        else:
            incomplete = True
        result = {
            "belief_a_id": left,
            "belief_b_id": right,
            "confidence_a": score("confidence_a"),
            "confidence_b": score("confidence_b"),
            "overlap_score": score("overlap_score"),
            "detected_at": detected,
        }
        if relations:
            relation = pair.get("relation")
            if relation not in {"contradiction", "entailment", "neutral"}:
                return None, True
            result.update(confidence=score("confidence"), relation=relation)
        else:
            result["detection_score"] = score("detection_score")
        return result, incomplete
    except (KeyError, TypeError, ValueError):
        return None, True


def map_reflect_result(
    ctx: RequestContext,
    request: d.ReflectRequest,
    raw: Mapping[str, object],
    *,
    sampled_at: datetime,
    selected_count: int,
    truncated: bool,
    evidence_unavailable: bool,
) -> d.ReflectResult:
    common = {"sampled_at": sampled_at.isoformat(), "selected_count": selected_count}
    mode = request.mode
    evidence_unavailable |= raw.get("evidence_stats_status") == "error"
    selected = frozenset(raw.get("_selected_ids", ()))
    if mode == "overview":
        states = {key: 0 for key in (*_LIFECYCLE, "unknown")}
        lifecycle_raw = raw["by_lifecycle_state"]
        if not isinstance(lifecycle_raw, Mapping):
            raise HttpError("E_INTERNAL")
        for key, value in lifecycle_raw.items():
            if type(key) is not str or type(value) is not int or value < 0:
                raise HttpError("E_INTERNAL")
            states[key if key in states else "unknown"] += value
        confidence = raw["confidence"]
        if not isinstance(confidence, Mapping):
            raise HttpError("E_INTERNAL")
        sections = common | {
            "total_beliefs": _raw_int(raw, "total_beliefs"),
            "lifecycle_counts": states,
            "confidence": dict(confidence),
            "contradiction_count": _raw_int(raw, "contradiction_count"),
            "stale_count": _raw_int(raw, "stale_count"),
            "decaying_next_7d": _raw_int(raw, "decaying_next_7d"),
            "no_evidence_count": None if evidence_unavailable else raw["no_evidence_count"],
            "evidence_stats_status": "unavailable" if evidence_unavailable else "ok",
        }
    elif mode == "contradictions":
        source, pairs = raw.get("contradictions", []), []
        for item in source:
            safe, incomplete = _safe_pair(item, relations=False, selected=selected)
            truncated |= incomplete
            if safe is not None:
                pairs.append(safe)
        sections = common | {
            "total": _raw_int(raw, "total"),
            "pairs": sorted(pairs, key=lambda pair: (pair["belief_a_id"], pair["belief_b_id"]))[: request.limit],
        }
        truncated |= len(pairs) > request.limit
    elif mode == "decay":
        sections = common | {
            key: _raw_int(raw, key) for key in ("stale_now", "stale_7d", "archived_7d", "forgotten_7d")
        }
    elif mode == "topics":
        sections = common | {
            "topic_count": len(raw["topics"]),
            "untagged_count": _raw_int(raw, "untagged_count"),
        }
    elif mode == "evidence_audit":
        unavailable = evidence_unavailable or raw.get("evidence_stats_status") == "error"
        sections = common | {
            key: (None if unavailable else raw.get(key, 0))
            for key in ("total", "with_evidence", "without_evidence", "avg_evidence_per_belief", "zero_weight_entries")
        }
        if not unavailable:
            for key in ("with_evidence", "without_evidence", "avg_evidence_per_belief", "zero_weight_entries"):
                if key not in raw:
                    raise HttpError("E_INTERNAL")
        sections["total"] = _raw_int(raw, "total")
        sections["evidence_stats_status"] = "unavailable" if unavailable else "ok"
        truncated |= unavailable
    elif mode == "confidence":
        beliefs = []
        for item in raw.get("beliefs", []):
            if not isinstance(item, Mapping):
                raise HttpError("E_INTERNAL")
            confidence = _raw_float(item, "confidence")
            try:
                ident = _uuid(item["id"])
                if ident not in selected:
                    raise ValueError
            except (ValueError, TypeError, AttributeError, KeyError):
                truncated = True
                continue
            beliefs.append(
                {
                    "id": ident,
                    "confidence": confidence,
                    "lifecycle_state": item["lifecycle_state"] if item["lifecycle_state"] in _LIFECYCLE else "unknown",
                }
            )
        truncated |= len(beliefs) > request.limit
        sections = common | {
            "histogram": raw.get("histogram", {}),
            "lowest_count": _raw_int(raw, "lowest_count"),
            "beliefs": beliefs[: request.limit],
        }
    else:
        source, pairs = raw.get("relations", []), []
        for item in source:
            safe, incomplete = _safe_pair(item, relations=True, selected=selected)
            truncated |= incomplete
            if safe is not None:
                pairs.append(safe)
        sections = common | {
            key: _raw_int(raw, key) for key in ("total", "contradictions", "entailments", "neutral_pairs")
        }
        sections["pairs"] = sorted(
            pairs, key=lambda pair: (pair["belief_a_id"], pair["belief_b_id"], pair["relation"])
        )[: request.limit]
        truncated |= len(pairs) > request.limit
    validate_reflect_sections(mode, sections)
    return d.ReflectResult(
        mode=mode, sections=sections, complete=not (truncated or evidence_unavailable), request_id=ctx.request_id
    )


_REFLECT_KEYS = {
    "overview": {
        "sampled_at",
        "selected_count",
        "total_beliefs",
        "lifecycle_counts",
        "confidence",
        "contradiction_count",
        "stale_count",
        "decaying_next_7d",
        "no_evidence_count",
        "evidence_stats_status",
    },
    "contradictions": {"sampled_at", "selected_count", "total", "pairs"},
    "decay": {"sampled_at", "selected_count", "stale_now", "stale_7d", "archived_7d", "forgotten_7d"},
    "topics": {"sampled_at", "selected_count", "topic_count", "untagged_count"},
    "evidence_audit": {
        "sampled_at",
        "selected_count",
        "total",
        "with_evidence",
        "without_evidence",
        "avg_evidence_per_belief",
        "zero_weight_entries",
        "evidence_stats_status",
    },
    "confidence": {"sampled_at", "selected_count", "histogram", "lowest_count", "beliefs"},
    "relations": {"sampled_at", "selected_count", "total", "contradictions", "entailments", "neutral_pairs", "pairs"},
}


def validate_reflect_sections(mode: str, sections: object) -> None:
    """Check the frozen S10 reflect wire schema, including scalar leaves."""
    expected = _REFLECT_KEYS.get(mode)
    if type(sections) is not dict or expected is None or set(sections) != expected:
        raise HttpError("E_INTERNAL")

    def stamp(value, nullable=False):
        if nullable and value is None:
            return
        try:
            parsed = datetime.fromisoformat(value) if type(value) is str else None
            if parsed is None or parsed.utcoffset() != UTC.utcoffset(None):
                raise ValueError
        except (ValueError, TypeError, OverflowError):
            raise HttpError("E_INTERNAL") from None

    def score(value):
        if type(value) not in (int, float) or not isfinite(value) or not 0.0 <= value <= 1.0:
            raise HttpError("E_INTERNAL")

    stamp(sections["sampled_at"])
    _strict_count(sections["selected_count"])
    if sections["selected_count"] > 100:
        raise HttpError("E_INTERNAL")
    scalar_counts = expected - {
        "sampled_at",
        "lifecycle_counts",
        "confidence",
        "histogram",
        "pairs",
        "beliefs",
        "evidence_stats_status",
        "avg_evidence_per_belief",
    }
    unavailable = sections.get("evidence_stats_status") == "unavailable"
    if "evidence_stats_status" in sections and sections["evidence_stats_status"] not in {"ok", "unavailable"}:
        raise HttpError("E_INTERNAL")
    for key in scalar_counts:
        if (
            unavailable
            and key in {"no_evidence_count", "total", "with_evidence", "without_evidence", "zero_weight_entries"}
            and sections[key] is None
        ):
            continue
        _strict_count(sections[key])
    if mode == "overview":
        lifecycle, confidence = sections["lifecycle_counts"], sections["confidence"]
        if type(lifecycle) is not dict or set(lifecycle) != set((*_LIFECYCLE, "unknown")):
            raise HttpError("E_INTERNAL")
        if type(confidence) is not dict or set(confidence) != {
            "high_0.8_1.0",
            "medium_0.5_0.8",
            "low_0.0_0.5",
            "average",
        }:
            raise HttpError("E_INTERNAL")
        for count in lifecycle.values():
            _strict_count(count)
        for key in ("high_0.8_1.0", "medium_0.5_0.8", "low_0.0_0.5"):
            _strict_count(confidence[key])
        score(confidence["average"])
    elif mode == "evidence_audit":
        average = sections["avg_evidence_per_belief"]
        if not (unavailable and average is None):
            if type(average) not in (int, float) or not isfinite(average) or average < 0:
                raise HttpError("E_INTERNAL")
    elif mode == "confidence":
        histogram = sections["histogram"]
        if type(histogram) is not dict or set(histogram) != {"0.9_1.0", "0.7_0.9", "0.5_0.7", "0.3_0.5", "0.0_0.3"}:
            raise HttpError("E_INTERNAL")
        for count in histogram.values():
            _strict_count(count)
        if type(sections["beliefs"]) is not list or len(sections["beliefs"]) > 100:
            raise HttpError("E_INTERNAL")
        for belief in sections["beliefs"]:
            if type(belief) is not dict or set(belief) != {"id", "confidence", "lifecycle_state"}:
                raise HttpError("E_INTERNAL")
            try:
                _uuid(belief["id"])
            except (TypeError, ValueError, AttributeError):
                raise HttpError("E_INTERNAL") from None
            score(belief["confidence"])
            if belief["lifecycle_state"] not in (*_LIFECYCLE, "unknown"):
                raise HttpError("E_INTERNAL")
    elif mode in {"contradictions", "relations"}:
        if type(sections["pairs"]) is not list or len(sections["pairs"]) > 100:
            raise HttpError("E_INTERNAL")
        pair_keys = {"belief_a_id", "belief_b_id", "confidence_a", "confidence_b", "overlap_score", "detected_at"}
        pair_keys |= {"confidence", "relation"} if mode == "relations" else {"detection_score"}
        for pair in sections["pairs"]:
            if type(pair) is not dict or set(pair) != pair_keys:
                raise HttpError("E_INTERNAL")
            for key in ("belief_a_id", "belief_b_id"):
                try:
                    _uuid(pair[key])
                except (ValueError, TypeError, AttributeError):
                    raise HttpError("E_INTERNAL") from None
            for key in (
                "confidence_a",
                "confidence_b",
                "overlap_score",
                "confidence" if mode == "relations" else "detection_score",
            ):
                score(pair[key])
            stamp(pair["detected_at"], nullable=True)
            if mode == "relations" and pair["relation"] not in {"contradiction", "entailment", "neutral"}:
                raise HttpError("E_INTERNAL")


class DiagnosticsService:
    def __init__(
        self,
        leases: ProfileLeases,
        bind: Callable[[ProfileSession], DiagnosticsResources],
        *,
        native=None,
        cursor_codec=None,
        metrics=None,
        clock: Callable[[], float] = monotonic,
    ):
        self.leases, self.bind, self.native, self.cursor_codec, self.metrics, self.clock = (
            leases,
            bind,
            native,
            cursor_codec,
            metrics,
            clock,
        )

    async def audit(self, ctx: RequestContext, query: d.AuditQuery) -> d.AuditResult:
        command = d.COMMAND_TYPES["audit"](operation="audit", profile_id=ctx.profile_id, path_ids={}, payload=query)
        return await self.execute(ctx, command)

    async def outbox_list(self, ctx: RequestContext, query: d.OutboxQuery) -> d.Page[d.OutboxItem]:
        command = d.COMMAND_TYPES["outbox.list"](
            operation="outbox.list", profile_id=ctx.profile_id, path_ids={}, payload=query
        )
        return await self.execute(ctx, command)

    async def indexes_status(self, ctx: RequestContext) -> d.IndexStatus:
        command = d.COMMAND_TYPES["indexes.status"](
            operation="indexes.status", profile_id=ctx.profile_id, path_ids={}, payload=d.Empty()
        )
        return await self.execute(ctx, command)

    async def overview(self, ctx: RequestContext) -> d.Overview:
        command = d.COMMAND_TYPES["overview"](
            operation="overview", profile_id=ctx.profile_id, path_ids={}, payload=d.Empty()
        )
        return await self.execute(ctx, command)

    async def reflect(self, ctx: RequestContext, request: d.ReflectRequest) -> d.ReflectResult:
        command = d.COMMAND_TYPES["reflect"](
            operation="reflect", profile_id=ctx.profile_id, path_ids={}, payload=request
        )
        return await self.execute(ctx, command)

    async def execute(self, ctx: RequestContext, command: d.DiagnosticsCommand) -> d.DiagnosticsResult:
        if command.operation not in {"audit", "indexes.status", "outbox.list", "overview", "reflect"}:
            raise HttpError("E_VALIDATION")
        if ctx.profile_id != command.profile_id:
            raise HttpError("E_PERMISSION_DENIED")
        permission = "profile.read" if command.operation in {"indexes.status", "overview"} else "audit.read"
        require_permission(ctx.principal, permission, profile_id=command.profile_id)
        if command.operation == "audit" and command.payload.cursor is not None:
            raise HttpError("E_CURSOR_INVALID")
        if command.operation == "outbox.list" and command.payload.cursor is not None:
            self.cursor_codec.validate(ctx.principal, command, now=self.cursor_codec.clock())
        async with self.leases.acquire(ctx) as scope:
            observation = self.leases.snapshot().get(ctx.profile_id)
            if observation is None:
                raise HttpError("E_RUNTIME_NOT_READY")
            resources = replace(self.bind(scope), writer_owned=observation.owned)
            custody = self.native(scope) if self.native is not None else None
            try:
                return await run_borrowed_diagnostics(
                    ctx,
                    resources,
                    lambda session, work, sampled: self._operation(ctx, command, resources, session, work, sampled),
                    clock=self.clock,
                )
            finally:
                if custody is not None:
                    cleanup = asyncio.create_task(custody.drain())
                    while not cleanup.done():
                        try:
                            await asyncio.shield(cleanup)
                        except asyncio.CancelledError:
                            continue
                    cleanup.result()

    async def _counts(self, resources, session, work):
        return {
            table: await resources.canonical.provider.diagnostics_count(session, table, work=work) for table in _TABLES
        }

    async def _indexes(self, resources, session, work, sampled, counts=None):
        counts = await self._counts(resources, session, work) if counts is None else counts
        repo = OutboxRepository(session)
        pending = await repo.diagnostic_count(status="pending", work=work)
        failed = await repo.diagnostic_count(status="failed", work=work)
        vector_count = None
        if resources.vector is not None and callable(getattr(resources.vector, "count_points", None)):
            try:
                vector_count = await resources.vector.count_points()
            except (DiagnosticsWorkExceeded, DiagnosticsDeadlineExceeded):
                raise
            except HttpError:
                raise
            except Exception:
                raise HttpError("E_VECTOR_BACKEND") from None
        return map_index_status(
            counts,
            vector_count=vector_count,
            outbox_pending=pending,
            outbox_failed=failed,
            graph_available=resources.graph is not None,
            sampled_at=sampled,
        )

    async def _audit(self, ctx, query, resources, session, work, sampled):
        provider, checks = resources.canonical.provider, []
        requested = (
            {query.audit_type}
            if query.audit_type != "full"
            else {"lifecycle", "receipts", "relations", "indexes", "confidence", "consistency"}
        )
        counts = {}
        if requested & {"lifecycle", "receipts", "indexes", "confidence", "consistency"}:
            counts = {
                table: await provider.diagnostics_count(session, table, work=work) for table in (*_TABLES, "receipts")
            }
        # One executor for the frozen SQL predicates. Table domains are explicit:
        # receipt metadata participates in lifecycle/confidence, not canonical joins.
        predicates = (
            ("lifecycle", "invalid_lifecycle", "STATE_METADATA_INVALID", "error", (*_TABLES, "receipts")),
            ("lifecycle", "receipt_state", "RECEIPT_STATE_MISMATCH", "warning", _TABLES),
            ("receipts", "receipt_missing", "RECEIPT_MISSING", "warning", _TABLES),
            ("receipts", "receipt_type", "RECEIPT_TYPE_MISMATCH", "warning", _TABLES),
            ("receipts", "receipt_orphan", "RECEIPT_ORPHAN", "warning", (None,)),
            ("relations", "relation_endpoints", "RELATION_ENDPOINT_INVALID", "error", (None,)),
            ("relations", "relation_type", "RELATION_TYPE_INVALID", "error", (None,)),
            ("confidence", "confidence", "CONFIDENCE_INVALID", "error", (*_TABLES, "receipts")),
            ("confidence", "confidence_low", "CONFIDENCE_LOW", "warning", (*_TABLES, "receipts")),
            ("consistency", "consistency", "CONFIDENCE_STATE_MISMATCH", "warning", _TABLES),
        )
        for section, predicate, code, severity, tables in predicates:
            if section in requested:
                count = 0
                for table in tables:
                    count += await provider.diagnostics_check_count(session, predicate, table=table, work=work)
                checks.append(AuditCheck(section, code, severity, count))
        if "relations" in requested:
            checks.append(AuditCheck("graph_orphans", "RELATION_ENDPOINT_INVALID", "error", None, False))
        if "indexes" in requested:
            statuses = await OutboxRepository(session).diagnostic_status_counts(work=work)
            checks.extend(
                (
                    AuditCheck("indexes", "OUTBOX_FAILED", "warning", statuses["failed"]),
                    AuditCheck("indexes", "VECTOR_COUNT_MISMATCH", "warning", None, False),
                )
            )
        snapshot = None
        if requested & {"lifecycle", "receipts", "confidence", "consistency"}:
            if resources.validator is None or resources.confidence is None:
                raise HttpError("E_CAPABILITY_UNAVAILABLE")
            snapshot = await provider.diagnostics_audit_snapshot(session, per_type_limit=200, work=work)
            auditor = MemoryAuditor(
                resources.validator,
                confidence_engine=resources.confidence,
                persisted_records=[asdict(record) for record in snapshot.records],
            )
            # Raw auditor messages never cross the mapper. Check identity determines
            # the closed code; SQL and snapshot evidence coalesce to one aggregate.
            methods = (
                ("lifecycle", "STATE_METADATA_INVALID", "error", auditor.check_lifecycle_violations),
                ("receipts", "RECEIPT_MISSING", "warning", auditor.check_missing_receipts),
                ("confidence", "CONFIDENCE_LOW", "warning", auditor.check_confidence_issues),
                ("consistency", "CONFIDENCE_STATE_MISMATCH", "warning", auditor.audit_consistency),
            )
            for section, code, severity, check in methods:
                if section in requested:
                    work.check()
                    checks.append(AuditCheck(section, code, severity, len(check())))
                    work.check()
        if "indexes" in requested:
            # Count equality is not proof of a shared vector domain. The existing
            # auditor is invoked with persisted counts but no unproved vector count.
            if resources.validator is not None and resources.confidence is not None:
                auditor = MemoryAuditor(
                    resources.validator,
                    confidence_engine=resources.confidence,
                    sqlite_counts=counts,
                    persisted_records=[asdict(record) for record in snapshot.records] if snapshot else [],
                )
                auditor.check_sql_vector_drift()
            checks.append(AuditCheck("indexes", "OUTBOX_STATUS_INVALID", "warning", statuses.get("unknown", 0)))
        work.check()
        return map_audit_result(
            ctx, checks, query=query, sampled_at=sampled, snapshot_truncated=bool(snapshot and snapshot.truncated)
        )

    async def _operation(self, ctx, command, resources, session, work, sampled):
        if command.operation == "audit":
            return await self._audit(ctx, command.payload, resources, session, work, sampled)
        if command.operation == "indexes.status":
            return await self._indexes(resources, session, work, sampled)
        if command.operation == "outbox.list":
            query, state = command.payload, None
            if query.cursor is not None:
                state = self.cursor_codec.validate(ctx.principal, command, now=self.cursor_codec.clock())
            page = await OutboxRepository(session).diagnostic_page(
                limit=query.limit,
                last=state.last if state else None,
                upper=state.upper if state else None,
                status=query.status,
                record_id=query.memory_id,
                work=work,
            )
            more, rows = len(page.rows) > query.limit, page.rows[: query.limit]
            cursor = None
            if more:
                last = OutboxKey(rows[-1].created_at.isoformat(timespec="microseconds"), rows[-1].id)
                cursor = self.cursor_codec.issue(
                    ctx,
                    command,
                    last=last,
                    upper=page.upper,
                    limit=query.limit,
                    now=self.cursor_codec.clock(),
                    expires_at=state.expires_at if state else None,
                )
            return d.Page[d.OutboxItem](
                items=[map_outbox_item(row) for row in rows],
                next_cursor=cursor,
                has_more=more,
                request_id=ctx.request_id,
            )
        if command.operation == "overview":
            counts = await self._counts(resources, session, work)
            lifecycle = {key: 0 for key in (*_LIFECYCLE, "unknown")}
            for table in _TABLES:
                for key, value in (
                    await resources.canonical.provider.diagnostics_lifecycle_counts(session, table, work=work)
                ).items():
                    lifecycle[key] += value
            indexes = await self._indexes(resources, session, work, sampled, counts=counts)
            revision = await resources.canonical.provider.diagnostics_schema_revision(session, work=work)
            unresolved = await resources.canonical.provider.diagnostics_unresolved_conflict_count(session, work=work)
            return map_overview(
                resources,
                counts,
                lifecycle_counts=lifecycle,
                unresolved_conflicts=unresolved,
                indexes=indexes,
                schema_revision=revision,
            )
        if resources.reflection is None:
            raise HttpError("E_CAPABILITY_UNAVAILABLE")
        request = command.payload
        provider = await build_borrowed_reflection(resources, session, request, work)
        window = provider.window
        reflection = ReflectionService(provider, policy=resources.reflection.policy)
        reflection_command = ReflectionCommand(**request.model_dump())
        raw = await (
            reflection.relations(reflection_command)
            if request.mode == "relations"
            else reflection.reflect(reflection_command)
        )
        if provider.evidence_error is not None:
            raise provider.evidence_error
        raw["_selected_ids"] = tuple(belief.id for belief in window.rows)
        return map_reflect_result(
            ctx,
            request,
            raw,
            sampled_at=sampled,
            selected_count=len(window.rows),
            truncated=window.truncated,
            evidence_unavailable=False,
        )


async def build_borrowed_reflection(resources: DiagnosticsResources, session, request: d.ReflectRequest, work):
    if resources.reflection is None:
        raise HttpError("E_CAPABILITY_UNAVAILABLE")
    minimum = (
        resources.reflection.policy.reflect_min_confidence if request.min_confidence is None else request.min_confidence
    )
    window = await resources.canonical.provider.diagnostics_reflect_beliefs(
        session, topic=request.topic or None, min_confidence=minimum, limit=request.limit, work=work
    )
    provider = BorrowedReflectionProvider(window.rows, session, work)
    provider.window = window
    return provider


def create_diagnostics_port(leases, bind, *, native, cursor_codec, metrics) -> DiagnosticsPort:
    return DiagnosticsService(leases, bind, native=native, cursor_codec=cursor_codec, metrics=metrics)
