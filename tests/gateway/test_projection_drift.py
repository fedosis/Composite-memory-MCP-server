"""B1 audit drift is unavailable without domain provenance; never repair."""

from __future__ import annotations

import asyncio
import hashlib

import pytest
from sqlalchemy import event, text

from tests.gateway.test_http_diagnostics import _ctx, _foundation, _http, _resources, _seed_diag, _seed_outbox
from tests.gateway.test_http_diagnostics import database as database
from tests.gateway.test_http_diagnostics import diagnostic_http as diagnostic_http
from tests.gateway.test_http_diagnostics import diagnostics as diagnostics
from tests.gateway.test_http_diagnostics import offline_embedder as offline_embedder
from tests.gateway.test_http_diagnostics import shell as shell


async def _hash_tables(provider):
    async with provider._session_factory() as session:
        hashes = {}
        for table in ("facts", "beliefs", "decisions", "skills", "receipts", "claim_relations", "outbox_entries"):
            rows = (await session.execute(text(f"SELECT * FROM {table}"))).all()
            hashes[table] = hashlib.sha256(repr(sorted(tuple(row) for row in rows)).encode()).hexdigest()
        return hashes


@pytest.mark.asyncio
async def test_drift_audit_indexes_unavailable_without_fake_graph_zero(database, diagnostics):
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
    from dataclasses import replace

    class VectorClient:
        rows = [1, 2]

        async def count_points(self):
            return len(self.rows)

    resources.vector, resources.graph = VectorClient(), object()
    for size in (2, 1, 3):
        resources.vector.rows = list(range(size))
        diagnostics.bind = lambda _scope: replace(
            diagnostics._test_resources, vector=resources.vector, graph=resources.graph
        )
        status = await diagnostics.indexes_status(_ctx())
        assert status.vector_records == size
        assert status.graph_consistency.projection == "unknown"
        assert status.vector_lag is None and status.graph_lag is None
        assert status.graph_relations is None and status.last_success is None
    resources.vector, resources.graph = None, None
    status = await diagnostics.indexes_status(_ctx())
    assert diagnostics._test_lease.acquired == diagnostics._test_lease.snapshots == 4
    assert status.vector_records is None and status.graph_consistency.projection == "unavailable"
    assert await _hash_tables(database) == before


@pytest.mark.asyncio
async def test_s10_http_drift_does_not_write(diagnostic_http):
    from tests.gateway.test_http_shell import rows

    shell = diagnostic_http
    scope = await _seed_diag(shell)
    provider, vector = scope.runtime.dependencies.sqlite, scope.runtime.dependencies.vector
    before = await _hash_tables(provider)
    counts = []
    for n in range(3):
        assert await vector.upsert(point_id=f"vector-{n}", vector=[1.0] + [0.0] * 383)
        response = await _http(shell, "indexes/status")
        assert response.status == 200, response.body
        counts.append(response.json["vector_records"])
        for key in ("vector_lag", "graph_lag", "graph_relations", "last_success"):
            assert response.json[key] is None
        assert response.json["graph_consistency"]["projection"] == "unknown"
    assert counts == [1, 2, 3]
    table = await vector._get_table()
    await vector._blocking(table.delete, "id = 'vector-1'")
    status = await _http(shell, "indexes/status")
    assert status.json["vector_records"] == 2
    for _, suffix in (("audit", "audit"), ("outbox", "outbox"), ("overview", "overview"), ("reflect", "reflect")):
        assert (await _http(shell, suffix)).status == 200
    assert await _hash_tables(provider) == before
    assert len(rows(shell)) == 8 and all(row["state"] == "terminal" for row in rows(shell))


@pytest.mark.asyncio
async def test_runner_statement_overflow_is_storage_and_hooks_reset(database):
    service, _, _ = _foundation()
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
    service, _, _ = _foundation()
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
