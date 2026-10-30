"""Borrowed-session and log-isolation primitives for S10 diagnostics."""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, replace
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
    selected = {check.section for check in checks}
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
        if check.available and (check.count or 0) > 0:
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


class _BorrowedReflectionProvider:
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
        sql = " ".join(str(statement).split())
        lower = sql.lower()
        expected = {f"bid_{index}": ident for index, ident in enumerate(self.ids)}
        if not self.ids or params != expected or "where belief_id in (" not in lower:
            raise HttpError("E_INTERNAL")
        placeholders = ",".join(f":bid_{index}" for index in range(len(self.ids)))
        try:
            if (
                lower.startswith("select belief_id, count(*) as count, avg(weight) as avg_weight, source_type from evidence")
                and lower.endswith("group by belief_id, source_type order by belief_id")
            ):
                safe = (
                    "SELECT belief_id,COUNT(*) AS count,AVG(weight) AS avg_weight,"
                    "'[MASKED]' AS source_type FROM evidence WHERE belief_id IN ("
                    f"{placeholders}) GROUP BY belief_id ORDER BY belief_id"
                )
            elif (
                lower.startswith("select belief_id, count(*) as count, avg(weight) as avg_weight from evidence")
                and lower.endswith("group by belief_id")
            ):
                safe = (
                    "SELECT belief_id,COUNT(*) AS count,AVG(weight) AS avg_weight FROM evidence "
                    f"WHERE belief_id IN ({placeholders}) GROUP BY belief_id"
                )
            elif lower.startswith("select count(*) from evidence") and lower.endswith("and weight = 0"):
                safe = f"SELECT COUNT(*) FROM evidence WHERE belief_id IN ({placeholders}) AND weight=0"
            else:
                raise HttpError("E_INTERNAL")
            result = await self.session.execute(text(safe), expected)
            self.work.check()
            return result
        except Exception as error:
            self.evidence_error = error
            raise


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
            value = pair[key]
            if type(value) is not float or not isfinite(value) or not 0.0 <= value <= 1.0:
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
    selected = frozenset(raw.get("_selected_ids", ()))
    if mode == "overview":
        states = {key: 0 for key in (*_LIFECYCLE, "unknown")}
        for key, value in dict(raw.get("by_lifecycle_state", {})).items():
            states[key if key in states else "unknown"] += int(value)
        sections = common | {
            "total_beliefs": int(raw.get("total_beliefs", selected_count)),
            "lifecycle_counts": states,
            "confidence": raw.get("confidence", {}),
            "contradiction_count": int(raw.get("contradiction_count", 0)),
            "stale_count": int(raw.get("stale_count", 0)),
            "decaying_next_7d": int(raw.get("decaying_next_7d", 0)),
            "no_evidence_count": None if evidence_unavailable else raw.get("no_evidence_count"),
            "evidence_stats_status": "unavailable" if evidence_unavailable else "ok",
        }
    elif mode == "contradictions":
        source, pairs = raw.get("contradictions", []), []
        for item in source:
            safe, incomplete = _safe_pair(item, relations=False, selected=selected)
            truncated |= incomplete
            if safe is not None:
                pairs.append(safe)
        sections = common | {"total": int(raw.get("total", len(pairs))), "pairs": pairs[: request.limit]}
        truncated |= len(pairs) > request.limit
    elif mode == "decay":
        sections = common | {
            key: int(raw.get(key, 0)) for key in ("stale_now", "stale_7d", "archived_7d", "forgotten_7d")
        }
    elif mode == "topics":
        sections = common | {
            "topic_count": len(raw.get("topics", [])),
            "untagged_count": int(raw.get("untagged_count", 0)),
        }
    elif mode == "evidence_audit":
        unavailable = evidence_unavailable or raw.get("evidence_stats_status") == "error"
        sections = common | {
            key: (None if unavailable else raw.get(key, 0))
            for key in ("total", "with_evidence", "without_evidence", "avg_evidence_per_belief", "zero_weight_entries")
        }
        sections["total"] = int(raw.get("total", selected_count))
        sections["evidence_stats_status"] = "unavailable" if unavailable else "ok"
        truncated |= unavailable
    elif mode == "confidence":
        beliefs = [
            {
                "id": _uuid(str(item["id"])),
                "confidence": float(item["confidence"]),
                "lifecycle_state": item["lifecycle_state"] if item["lifecycle_state"] in _LIFECYCLE else "unknown",
            }
            for item in raw.get("beliefs", [])
        ]
        sections = common | {
            "histogram": raw.get("histogram", {}),
            "lowest_count": int(raw.get("lowest_count", 0)),
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
            key: int(raw.get(key, 0)) for key in ("total", "contradictions", "entailments", "neutral_pairs")
        }
        sections["pairs"] = pairs[: request.limit]
        truncated |= len(pairs) > request.limit
    return d.ReflectResult(mode=mode, sections=sections, complete=not truncated, request_id=ctx.request_id)


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

    async def execute(self, ctx: RequestContext, command: d.DiagnosticsCommand) -> d.DiagnosticsResult:
        if command.operation not in {"audit", "indexes.status", "outbox.list", "overview", "reflect"}:
            raise HttpError("E_VALIDATION")
        if ctx.profile_id != command.profile_id:
            raise HttpError("E_PERMISSION_DENIED")
        permission = "profile.read" if command.operation in {"indexes.status", "overview"} else "audit.read"
        require_permission(ctx.principal, permission, profile_id=command.profile_id)
        if command.operation == "audit" and command.payload.cursor is not None:
            raise HttpError("E_CURSOR_INVALID")
        async with self.leases.acquire(ctx) as scope:
            observation = self.leases.snapshot().get(ctx.profile_id)
            if observation is None:
                raise HttpError("E_RUNTIME_NOT_READY")
            resources = replace(self.bind(scope), writer_owned=observation.owned)
            return await run_borrowed_diagnostics(
                ctx,
                resources,
                lambda session, work, sampled: self._operation(ctx, command, resources, session, work, sampled),
                clock=self.clock,
            )

    async def _counts(self, resources, session, work):
        return {
            table: await resources.canonical.provider.diagnostics_count(session, table, work=work) for table in _TABLES
        }

    async def _indexes(self, resources, session, work, sampled):
        counts = await self._counts(resources, session, work)
        repo = OutboxRepository(session)
        statuses = await repo.diagnostic_status_counts(work=work)
        vector_count = None
        if resources.vector is not None:
            try:
                vector_count = await resources.vector.count_points()
            except Exception:
                raise HttpError("E_VECTOR_BACKEND") from None
        return d.IndexStatus(
            sqlite_records=sum(counts.values()),
            vector_records=vector_count,
            graph_relations=None,
            vector_lag=None,
            graph_lag=None,
            outbox_pending=statuses["pending"],
            outbox_failed=statuses["failed"],
            graph_consistency=map_consistency("unknown" if resources.graph is not None else "unavailable"),
            last_success=None,
            sampled_at=sampled,
        )

    async def _audit(self, ctx, query, resources, session, work, sampled):
        provider, checks = resources.canonical.provider, []
        requested = (
            {query.audit_type}
            if query.audit_type != "full"
            else {"lifecycle", "receipts", "relations", "indexes", "confidence", "consistency"}
        )
        if "lifecycle" in requested:
            checks.append(
                AuditCheck(
                    "lifecycle",
                    "STATE_METADATA_INVALID",
                    "error",
                    sum(
                        [
                            await provider.diagnostics_check_count(session, "invalid_lifecycle", table=t, work=work)
                            for t in _TABLES
                        ]
                    ),
                )
            )
            checks.append(
                AuditCheck(
                    "lifecycle",
                    "RECEIPT_STATE_MISMATCH",
                    "warning",
                    sum(
                        [
                            await provider.diagnostics_check_count(session, "receipt_state", table=t, work=work)
                            for t in _TABLES
                        ]
                    ),
                )
            )
        if "receipts" in requested:
            for check, code in (("receipt_missing", "RECEIPT_MISSING"), ("receipt_type", "RECEIPT_TYPE_MISMATCH")):
                checks.append(
                    AuditCheck(
                        "receipts",
                        code,
                        "warning",
                        sum(
                            [
                                await provider.diagnostics_check_count(session, check, table=t, work=work)
                                for t in _TABLES
                            ]
                        ),
                    )
                )
            checks.append(
                AuditCheck(
                    "receipts",
                    "RECEIPT_ORPHAN",
                    "warning",
                    await provider.diagnostics_check_count(session, "receipt_orphan", table=None, work=work),
                )
            )
        if "relations" in requested:
            checks.extend(
                (
                    AuditCheck(
                        "relations",
                        "RELATION_ENDPOINT_INVALID",
                        "error",
                        await provider.diagnostics_check_count(session, "relation_endpoints", table=None, work=work),
                    ),
                    AuditCheck(
                        "relations",
                        "RELATION_TYPE_INVALID",
                        "error",
                        await provider.diagnostics_check_count(session, "relation_type", table=None, work=work),
                    ),
                    AuditCheck("graph_orphans", "RELATION_ENDPOINT_INVALID", "error", None, False),
                )
            )
        if "indexes" in requested:
            statuses = await OutboxRepository(session).diagnostic_status_counts(work=work)
            checks.extend(
                (
                    AuditCheck("indexes", "OUTBOX_FAILED", "warning", statuses["failed"]),
                    AuditCheck("indexes", "VECTOR_COUNT_MISMATCH", "warning", None, False),
                )
            )
        if "confidence" in requested:
            checks.extend(
                (
                    AuditCheck(
                        "confidence",
                        "CONFIDENCE_INVALID",
                        "error",
                        sum(
                            [
                                await provider.diagnostics_check_count(session, "confidence", table=t, work=work)
                                for t in _TABLES
                            ]
                        ),
                    ),
                    AuditCheck(
                        "confidence",
                        "CONFIDENCE_LOW",
                        "warning",
                        sum(
                            [
                                await provider.diagnostics_check_count(session, "confidence_low", table=t, work=work)
                                for t in _TABLES
                            ]
                        ),
                    ),
                )
            )
        if "consistency" in requested:
            checks.append(
                AuditCheck(
                    "consistency",
                    "CONFIDENCE_STATE_MISMATCH",
                    "warning",
                    sum(
                        [
                            await provider.diagnostics_check_count(session, "consistency", table=t, work=work)
                            for t in _TABLES
                        ]
                    ),
                )
            )
        snapshot = (
            await provider.diagnostics_audit_snapshot(session, per_type_limit=200, work=work)
            if requested & {"lifecycle", "receipts", "confidence", "consistency"}
            else None
        )
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
            indexes = await self._indexes(resources, session, work, sampled)
            revision = await resources.canonical.provider.diagnostics_schema_revision(session, work=work)
            return d.Overview(
                profile=resources.profile,
                **counts,
                lifecycle_counts=lifecycle,
                unresolved_conflicts=await resources.canonical.provider.diagnostics_unresolved_conflict_count(
                    session, work=work
                ),
                audit_warnings=None,
                indexes=indexes,
                schema_revision=revision,
                last_migration=None,
                writer_owned=resources.writer_owned,
            )
        if resources.reflection is None:
            raise HttpError("E_CAPABILITY_UNAVAILABLE")
        request = command.payload
        minimum = (
            resources.reflection.policy.reflect_min_confidence
            if request.min_confidence is None
            else request.min_confidence
        )
        window = await resources.canonical.provider.diagnostics_reflect_beliefs(
            session, topic=request.topic, min_confidence=minimum, limit=request.limit, work=work
        )
        provider = _BorrowedReflectionProvider(window.rows, session, work)
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


def create_diagnostics_port(leases, bind, *, native, cursor_codec, metrics) -> DiagnosticsPort:
    return DiagnosticsService(leases, bind, native=native, cursor_codec=cursor_codec, metrics=metrics)
