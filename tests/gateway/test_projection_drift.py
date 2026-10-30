"""B1 audit drift is unavailable without domain provenance; never repair."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import event, text

from tests.gateway.test_http_diagnostics import _ctx, _foundation, _resources
from tests.gateway.test_http_diagnostics import database as database


@pytest.mark.asyncio
async def test_drift_audit_indexes_unavailable_without_fake_graph_zero(database):
    service, _, _ = _foundation()
    from memory_server.gateway_contracts import dto as d

    resources = _resources(database)
    query = d.AuditQuery(audit_type="indexes", limit=200, cursor=None)
    statements = []

    def trace(*args):
        statements.append(args[2])

    event.listen(database.engine.sync_engine, "before_cursor_execute", trace)
    try:
        result = await service.run_borrowed_diagnostics(
            _ctx(),
            resources,
            lambda session, work, sampled: service.DiagnosticsService(None, None)._audit(
                _ctx(), query, resources, session, work, sampled
            ),
        )
    finally:
        event.remove(database.engine.sync_engine, "before_cursor_execute", trace)
    assert result.sections == {"indexes": "unavailable"}
    assert result.complete is False and result.findings.items == []
    assert statements and all(sql.lstrip().split()[0].upper() in {"SELECT", "BEGIN"} for sql in statements)


@pytest.mark.asyncio
async def test_runner_statement_overflow_is_storage_and_hooks_reset(database):
    service, _, _ = _foundation(require_b1=False)
    from memory_server.transports.http.errors import HttpError

    observed = []

    async def overflow(session, work, sampled):
        assert (work.statement_vm_limit, work.operation_vm_limit, work.statements_max) == (8_000_000, 40_000_000, 256)
        observed.append(sampled)
        for _ in range(257):
            await session.execute(text("SELECT 1"))

    with pytest.raises(HttpError, match="E_STORAGE"):
        await service.run_borrowed_diagnostics(_ctx(), _resources(database), overflow)
    async with database._session_factory() as session:
        assert (await session.execute(text("SELECT 1"))).scalar_one() == 1
    assert len(observed) == 1 and observed[0].tzinfo is not None


@pytest.mark.asyncio
async def test_runner_cancellation_drains_before_return(database):
    service, _, _ = _foundation(require_b1=False)
    from memory_server.transports.http.errors import HttpError

    started, finished = asyncio.Event(), asyncio.Event()

    async def slow(session, work, sampled):
        started.set()
        try:
            await asyncio.sleep(0.03)
            work.check()
        finally:
            finished.set()

    task = asyncio.create_task(service.run_borrowed_diagnostics(_ctx(), _resources(database), slow))
    await asyncio.wait_for(started.wait(), 5)
    task.cancel()
    with pytest.raises(HttpError, match="E_TIMEOUT"):
        await task
    assert finished.is_set()
