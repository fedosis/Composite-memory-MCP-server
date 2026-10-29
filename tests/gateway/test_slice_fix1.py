"""FIX1 regressions across real SQLite and mounted TCP search boundaries."""

import asyncio
import json
import sqlite3
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from time import monotonic
from types import SimpleNamespace

import pytest
from prometheus_client import generate_latest
from sqlalchemy import event, insert
from sqlalchemy.exc import SQLAlchemyError
from storage.models.fact import FactORM
from storage.repositories.fact_repo import FactRepository

from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import HttpConfig, RequestContext
from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.runtime.daemon_cli import run_daemon
from tests.gateway.test_slice_runtime import (
    SocketServer,
    build_registry,
    free_port,
    http_request,
    seed_fact,
    wait_started,
)
from tests.gateway.test_slice_runtime import (
    offline_embedder as offline_embedder,
)


@pytest.fixture
async def provider(tmp_path):
    value = SQLiteProvider("sqlite+aiosqlite:///" + str(tmp_path / "memory.db"))
    await value.initialize()
    try:
        yield value
    finally:
        await value.close()


@pytest.mark.parametrize("query", ["a%b", "a_b", "a\\b", "ёж%ю", "ёж_ю", "ёж\\ю"])
async def test_fix1_like_matches_literal_characters(provider, query):
    # Real fallback SQL, not a mocked execute: FTS deliberately unavailable.
    async with provider._session_factory() as session:
        session.add_all(
            [
                FactORM(id="literal", subject="literal", predicate="contains", object=query, dedup_key="literal"),
                FactORM(
                    id="decoy",
                    subject="decoy",
                    predicate="contains",
                    object=query.replace("%", "X").replace("_", "X").replace("\\", "X"),
                    dedup_key="decoy",
                ),
            ]
        )
        await session.commit()
        repo = FactRepository(session)
        repo._fts5_available = False
        statements = []

        def trace(conn, cursor, sql, params, context, many):
            statements.append(sql)

        event.listen(provider.engine.sync_engine, "before_cursor_execute", trace)
        try:
            assert [f.id for f in await repo.search(text=query)] == ["literal"]
            assert "ESCAPE '\\'" in statements[-1]
        finally:
            event.remove(provider.engine.sync_engine, "before_cursor_execute", trace)


@pytest.mark.parametrize("fts", [True, False])
async def test_fix1_wildcard_miss_on_20k_never_becomes_a_page(provider, fts):
    async with provider._session_factory() as session:
        await session.execute(
            insert(FactORM),
            [
                {
                    "id": f"row-{i}",
                    "subject": "ordinary",
                    "predicate": "contains",
                    "object": f"value{i}",
                    "dedup_key": f"row-{i}",
                }
                for i in range(20000)
            ],
        )
        await session.commit()
        repo = FactRepository(session)
        repo._fts5_available = fts
        statements = []

        def trace(conn, cursor, sql, params, context, many):
            statements.append(sql)

        event.listen(provider.engine.sync_engine, "before_cursor_execute", trace)
        try:
            for query in ("%", "_", "\\", "!!!", "…", " %_\\!? "):
                statements.clear()
                assert await repo.search(text=query, limit=100) == [], query
                assert not any(" LIKE " in sql for sql in statements), statements
            for query in ("absent-marker", "нет-совпадения!", "a%b"):
                assert await repo.search(text=query, limit=100) == [], query
        finally:
            event.remove(provider.engine.sync_engine, "before_cursor_execute", trace)


@pytest.fixture
async def running(tmp_path):
    registry, _ = await build_registry(tmp_path)
    config = HttpConfig(
        control_path=tmp_path / "control" / "control.sqlite", host="127.0.0.1", port=free_port(), rate_burst=200
    )
    holder = SimpleNamespace(server=None, app=None, config=config)

    def factory(server_config):
        holder.server = SocketServer(server_config)
        holder.app = server_config.app
        return holder.server

    from memory_server.transports.http.app import create_http_app

    task = asyncio.create_task(run_daemon(config, registry, factory=create_http_app, server_factory=factory))
    try:
        async with asyncio.timeout(5):
            while holder.server is None:
                await asyncio.sleep(0.01)
        holder.port = await wait_started(holder.server)
        now = datetime.now(UTC)
        created = holder.app.dependencies.tokens._bootstrap_create(
            d.TokenCreate(
                subject="FIX1_TOKEN_CANARY",
                profiles=["alpha"],
                permissions=["memory.read"],
                expires_at=now + timedelta(hours=1),
            ),
            now=now,
        )
        holder.bearer = f"cmms_{created.token.id}.{created.secret}"
        holder.principal = await holder.app.dependencies.tokens.authenticate(holder.bearer, now=now)
        yield holder
    finally:
        if holder.server is not None:
            holder.server.should_exit = True
        await asyncio.wait_for(task, 10)


async def search(running, query):
    headers = {}
    status, body = await http_request(
        running.port,
        "POST",
        "/api/v1/profiles/alpha/search",
        body=json.dumps(
            {"query": query, "mode": "keyword", "filters": {"source": None, "min_confidence": None}}
        ).encode(),
        headers={"Authorization": "Bearer " + running.bearer, "Content-Type": "application/json"},
        response_headers=headers,
    )
    return status, body, headers["x-request-id"]


@pytest.mark.parametrize("query", ["\u0000", "a\u0001b", "a\nb", "a\tb", "a\u007fb"])
async def test_fix1_controls_rejected_before_lease_and_audit(running, monkeypatch, query):
    calls = []
    acquire = running.app.dependencies.leases.acquire

    @asynccontextmanager
    async def counted(ctx):
        calls.append(ctx.request_id)
        async with acquire(ctx) as scope:
            yield scope

    monkeypatch.setattr(running.app.dependencies.leases, "acquire", counted)
    status, body, request_id = await search(running, query)
    assert (status, body.get("error", {}).get("code")) == (422, "E_VALIDATION")
    assert calls == []
    with sqlite3.connect(running.config.control_path.parent / "http-access" / "access.sqlite") as db:
        assert db.execute("SELECT count(*) FROM access_audit WHERE request_id=?", (request_id,)).fetchone() == (0,)


@pytest.mark.parametrize("outcome", ["hit", "miss", "storage", "unready"])
async def test_fix1_retrieval_metrics_on_success_and_refusal(running, monkeypatch, outcome):
    deps = running.app.dependencies
    ctx = RequestContext("FIX1_REQUEST_CANARY", running.principal, "alpha", None, monotonic() + 30)
    async with deps.leases.acquire(ctx) as scope:
        application = scope.application
        resources = application.read_resources()
        await seed_fact(resources, "FIX1_ID_CANARY", "FIX1_QUERY_CANARY")
    if outcome == "storage":

        async def broken(*args, **kwargs):
            await asyncio.sleep(0.01)
            raise SQLAlchemyError("FIX1_SQL_CANARY")

        monkeypatch.setattr(resources.provider, "search_facts_in_session", broken)
    elif outcome == "unready":
        monkeypatch.setattr(application, "_read_provider", None)
    calls = []
    real_completed = deps.metrics.retrieval_completed

    def completed(ctx, stage, elapsed, code):
        calls.append((ctx, stage, elapsed, code))
        real_completed(ctx, stage, elapsed, code)

    monkeypatch.setattr(deps.metrics, "retrieval_completed", completed)
    started = monotonic()
    status, body, request_id = await search(running, "absent" if outcome == "miss" else "FIX1_QUERY_CANARY")
    wall = monotonic() - started
    expected_code = {"storage": "E_STORAGE", "unready": "E_RUNTIME_NOT_READY"}.get(outcome)
    assert status == (503 if expected_code else 200), body
    if expected_code:
        assert body["error"]["code"] == expected_code
    else:
        assert body["total"] == (0 if outcome == "miss" else 1)
    assert len(calls) == 1
    metric_ctx, stage, elapsed, code = calls[0]
    assert metric_ctx.request_id == request_id and stage == "fts" and code == expected_code
    assert 0 < elapsed <= wall
    if outcome == "storage":
        assert elapsed >= 0.01
    families = list(deps.metrics._registry.collect())
    samples = [
        sample
        for family in families
        for sample in family.samples
        if sample.name.startswith("cmms_retrieval_duration_seconds")
    ]
    assert samples
    assert all(set(s.labels) <= {"stage", "profile", "le"} for s in samples)
    assert all(s.labels["stage"] == "fts" and s.labels["profile"] == "alpha" for s in samples)
    assert next(s.value for s in samples if s.name.endswith("_count")) == 1
    assert next(s.value for s in samples if s.name.endswith("_sum")) == elapsed
    exposition = generate_latest(deps.metrics._registry)
    assert b"CANARY" not in exposition and running.bearer.encode() not in exposition
    assert request_id.encode() not in exposition
