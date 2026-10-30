"""Borrowed-session and log-isolation primitives for S10 diagnostics."""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from time import monotonic
from typing import Awaitable, Callable, Sequence, TypeVar
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from storage.outbox import OutboxDiagnosticRow, OutboxKey, OutboxRepository, classify_diagnostic_error

from memory_server.application.memory_read_service import ReadResources
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

    async def execute(self, ctx: RequestContext, command: d.DiagnosticsCommand) -> d.DiagnosticsResult:
        if command.operation not in {"audit", "outbox.list"}:
            raise HttpError("E_VALIDATION")
        if ctx.profile_id != command.profile_id:
            raise HttpError("E_PERMISSION_DENIED")
        permission = "audit.read"
        require_permission(ctx.principal, permission, profile_id=command.profile_id)
        if command.operation == "audit" and command.payload.cursor is not None:
            raise HttpError("E_CURSOR_INVALID")
        if command.operation == "outbox.list" and command.payload.cursor is not None:
            self.cursor_codec.validate(ctx.principal, command, now=self.cursor_codec.clock())
        async with self.leases.acquire(ctx) as scope:
            resources = self.bind(scope)
            return await run_borrowed_diagnostics(
                ctx,
                resources,
                lambda session, work, sampled: self._operation(ctx, command, resources, session, work, sampled),
                clock=self.clock,
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
        raise HttpError("E_VALIDATION")


def create_diagnostics_port(leases, bind, *, native, cursor_codec, metrics) -> DiagnosticsPort:
    return DiagnosticsService(leases, bind, native=native, cursor_codec=cursor_codec, metrics=metrics)
