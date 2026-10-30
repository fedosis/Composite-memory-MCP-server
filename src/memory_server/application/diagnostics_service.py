"""Borrowed-session and log-isolation primitives for S10 diagnostics."""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime
from time import monotonic
from typing import Awaitable, Callable, TypeVar

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from memory_server.application.memory_read_service import ReadResources
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import RequestContext
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
    writer_owned: bool


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
