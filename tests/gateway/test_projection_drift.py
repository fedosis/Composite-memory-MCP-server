"""B1 audit drift is unavailable without domain provenance; never repair."""

from __future__ import annotations

import asyncio
import hashlib

import pytest
from sqlalchemy import event, text

from tests.gateway.test_http_diagnostics import _ctx, _foundation, _resources, _seed_outbox
from tests.gateway.test_http_diagnostics import database as database


async def _hash_tables(provider):
    async with provider._session_factory() as session:
        hashes = {}
        for table in ("facts", "beliefs", "decisions", "skills", "receipts", "claim_relations", "outbox_entries"):
            rows = (await session.execute(text(f"SELECT * FROM {table}"))).all()
            hashes[table] = hashlib.sha256(repr(sorted(tuple(row) for row in rows)).encode()).hexdigest()
        return hashes


@pytest.mark.asyncio
async def test_drift_audit_indexes_unavailable_without_fake_graph_zero(database):
    service, _, _ = _foundation()
    from memory_server.gateway_contracts import dto as d

    resources = _resources(database)
    query = d.AuditQuery(audit_type="indexes", limit=200, cursor=None)
    await _seed_outbox(database, 4)
    before = await _hash_tables(database)
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
    assert await _hash_tables(database) == before
    assert result.sections == {"indexes": "unavailable"}
    assert result.complete is False
    assert [(item.code, item.aggregate, item.object_id) for item in result.findings.items] == [
        ("OUTBOX_FAILED", True, None)
    ]
    assert statements and all(sql.lstrip().split()[0].upper() in {"SELECT", "BEGIN"} for sql in statements)
    assert hasattr(service.DiagnosticsService, "indexes_status"), "B2 API missing"

    class VectorClient:
        rows = [1, 2]

        async def count_points(self):
            return len(self.rows)

    resources.vector, resources.graph = VectorClient(), object()
    for size in (2, 1, 3):
        resources.vector.rows = list(range(size))
        status = await service.run_borrowed_diagnostics(
            _ctx(),
            resources,
            lambda session, work, sampled: service.DiagnosticsService(None, None)._indexes(
                resources, session, work, sampled
            ),
        )
        assert status.vector_records == size
        assert status.graph_consistency.projection == "unknown"
        assert status.vector_lag is None and status.graph_lag is None
        assert status.graph_relations is None and status.last_success is None
    resources.vector, resources.graph = None, None
    status = await service.run_borrowed_diagnostics(
        _ctx(),
        resources,
        lambda session, work, sampled: service.DiagnosticsService(None, None)._indexes(
            resources, session, work, sampled
        ),
    )
    assert status.vector_records is None and status.graph_consistency.projection == "unavailable"
    assert await _hash_tables(database) == before


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
