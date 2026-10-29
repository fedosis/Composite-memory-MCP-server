"""S08a TCP regression tests added before implementation."""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from time import monotonic
from types import SimpleNamespace

import pytest
from sqlalchemy import event, text
from storage.models.fact import FactORM
from storage.repositories.fact_repo import FactRepository

from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import HttpConfig, RequestContext
from memory_server.providers.embedding_provider import MockEmbeddingProvider
from memory_server.runtime import runtime_factory
from memory_server.runtime.daemon_cli import run_daemon
from memory_server.transports.http.app import create_http_app
from tests.gateway.test_slice_runtime import (
    SocketServer,
    build_registry,
    free_port,
    http_request,
    seed_fact,
    wait_started,
)


@pytest.fixture(autouse=True)
def offline_embedder(monkeypatch):
    monkeypatch.setitem(runtime_factory._DEFAULT_RECIPES, "embedder", lambda runtime: MockEmbeddingProvider())


async def _start(tmp_path):
    registry, _ = await build_registry(tmp_path)
    config = HttpConfig(
        control_path=tmp_path / "control" / "control.sqlite",
        host="127.0.0.1",
        port=free_port(),
        rate_per_minute=10000,
        rate_burst=1000,
    )
    holder = SimpleNamespace(server=None, app=None)

    def factory(server_config):
        holder.server = SocketServer(server_config)
        holder.app = server_config.app
        return holder.server

    task = asyncio.create_task(run_daemon(config, registry, factory=create_http_app, server_factory=factory))
    async with asyncio.timeout(5):
        while holder.server is None:
            await asyncio.sleep(0.01)
    port = await wait_started(holder.server)
    now = datetime.now(UTC)
    created = holder.app.dependencies.tokens._bootstrap_create(
        d.TokenCreate(
            subject="s08a", profiles=["alpha", "beta"], permissions=["memory.read"], expires_at=now + timedelta(hours=1)
        ),
        now=now,
    )
    bearer = f"cmms_{created.token.id}.{created.secret}"
    principal = await holder.app.dependencies.tokens.authenticate(bearer, now=now)
    ctx = RequestContext("s08a-seed", principal, "alpha", None, now.timestamp() + 30)
    async with holder.app.dependencies.leases.acquire(ctx) as scope:
        await seed_fact(scope.application.read_resources(), "s08a-marker", "alpha")
    return holder, task, port, {"Authorization": "Bearer " + bearer}


@pytest.mark.asyncio
async def test_s08a_metadata_filter_accepted_over_tcp(tmp_path):
    holder, task, port, auth = await _start(tmp_path)
    try:
        status, body = await http_request(
            port,
            "POST",
            "/api/v1/profiles/alpha/search",
            body=b'{"query":"alpha","filters":{"source":null,"min_confidence":0.0}}',
            headers={**auth, "Content-Type": "application/json"},
        )
        assert status == 200, body
        assert body["total"] == 1
    finally:
        holder.server.should_exit = True
        await asyncio.wait_for(task, 10)


@pytest.mark.asyncio
async def test_s08a_context_mounted_over_tcp(tmp_path):
    from memory_server.transports.http.routes import retrieval

    holder, task, port, auth = await _start(tmp_path)
    try:
        parsers = {"search": retrieval._parse_search, "context": retrieval._parse_context}
        mounted = [spec for spec in holder.app.read_routes if spec.operation in parsers]
        assert {spec.operation for spec in mounted} == set(parsers)
        for spec in mounted:
            # Anchor the expected parser independently: the shared map itself may be swapped.
            canonical = replace(retrieval.RETRIEVAL_ROUTE_SPECS[spec.operation], parse=parsers[spec.operation])
            assert spec.parse is canonical.parse
        status, body = await http_request(
            port,
            "POST",
            "/api/v1/profiles/alpha/context",
            body=b'{"task":"alpha","subject":null,"max_results":20,"include_inactive":false,"trace":false}',
            headers={**auth, "Content-Type": "application/json"},
        )
        assert status == 200, body
        assert body["package"]["total"] == 1
        assert len(body["memories"]) == 1
    finally:
        holder.server.should_exit = True
        await asyncio.wait_for(task, 10)


@asynccontextmanager
async def live_server(tmp_path):
    holder, task, port, auth = await _start(tmp_path)
    try:
        yield holder, port, auth
    finally:
        holder.server.should_exit = True
        await asyncio.wait_for(task, 10)


async def read_scope(holder, auth, profile="alpha"):
    principal = await holder.app.dependencies.tokens.authenticate(
        auth["Authorization"].removeprefix("Bearer "), now=datetime.now(UTC)
    )
    return holder.app.dependencies.leases.acquire(
        RequestContext("s08a-fixture", principal, profile, None, monotonic() + 60)
    )


async def post(port, auth, operation="search", profile="alpha", **changes):
    payload = (
        {"query": "needle", "mode": "keyword", "filters": {"source": None, "min_confidence": None}}
        if operation == "search"
        else {"task": "needle", "subject": None}
    )
    payload.update(changes)
    response_headers = {}
    status, body = await http_request(
        port,
        "POST",
        f"/api/v1/profiles/{profile}/{operation}",
        body=json.dumps(payload).encode(),
        headers={**auth, "Content-Type": "application/json"},
        response_headers=response_headers,
    )
    assert response_headers["cache-control"] == "no-store"
    assert response_headers["x-request-id"] == body.get("request_id", body.get("error", {}).get("request_id"))
    assert "CANARY" not in json.dumps(response_headers)
    return status, body


async def insert_facts(resources, rows):
    async with resources.session_factory() as session, session.begin():
        for index, row in enumerate(rows):
            values = dict(
                id=f"f{index}",
                subject="needle",
                predicate="CANARY_predicate",
                object="needle",
                dedup_key=f"CANARY_dedup{index}",
                confidence=0.8,
                source="CANARY_source",
                creator="CANARY_creator",
                lifecycle_state="active",
                verification_status="candidate",
                version="1",
                created_at=datetime(2026, 1, 1) + timedelta(seconds=index),
                updated_at=datetime(2026, 1, 1) + timedelta(seconds=index),
            )
            values.update(row)
            session.add(FactORM(**values))


@pytest.mark.asyncio
@pytest.mark.parametrize("no_fts", [False, True])
async def test_metadata_matrix_before_limit_two_profiles(tmp_path, no_fts):
    async with live_server(tmp_path) as (holder, port, auth):
        for profile in ("alpha", "beta"):
            async with await read_scope(holder, auth, profile) as scope:
                resources = scope.application.read_resources()
                await insert_facts(
                    resources,
                    [
                        {"id": "eligible", "confidence": 0.9, "lifecycle_state": "validated"},
                        {"id": "active", "confidence": 1.0},
                        {"id": "candidate", "confidence": 0.0, "lifecycle_state": "candidate"},
                        {"id": "archived", "confidence": 0.95, "lifecycle_state": "archived"},
                        {"id": "rejected", "lifecycle_state": "rejected"},
                        {"id": "top-ineligible", "confidence": 0.1, "object": "needle " * 12},
                    ],
                )
                if no_fts:
                    async with resources.session_factory() as session, session.begin():
                        await session.execute(text("DROP TABLE facts_fts"))
                async with resources.session_factory() as session:
                    unfiltered = await FactRepository(session).search(text="needle", limit=1, lifecycle_state="active")
                    assert [f.id for f in unfiltered] == ["top-ineligible"]
            cases = [
                ({"memory_types": ["belief"]}, False, set()),
                ({"memory_types": ["fact"]}, False, {"eligible", "active", "candidate", "top-ineligible"}),
                ({"memory_types": ["fact", "belief"]}, False, {"eligible", "active", "candidate", "top-ineligible"}),
                ({"min_confidence": 0.0}, False, {"eligible", "active", "candidate", "top-ineligible"}),
                ({"min_confidence": 1.0}, False, {"active"}),
                ({"min_confidence": 0.9}, False, {"active", "eligible"}),
                ({"lifecycle_states": ["active"]}, False, {"active", "top-ineligible"}),
                ({"lifecycle_states": ["validated"]}, False, {"eligible"}),
                ({"lifecycle_states": ["candidate"]}, False, {"candidate"}),
                ({"lifecycle_states": ["archived"]}, False, set()),
                ({"lifecycle_states": ["archived"]}, True, {"archived"}),
                ({"lifecycle_states": ["rejected"]}, True, {"rejected"}),
                ({"lifecycle_states": ["active", "archived"]}, False, {"active", "top-ineligible"}),
                ({"lifecycle_states": ["active", "archived"]}, True, {"active", "top-ineligible", "archived"}),
                ({"lifecycle_states": ["unknown"]}, True, set()),
                (
                    {
                        "memory_types": ["fact", "belief"],
                        "lifecycle_states": ["validated", "archived"],
                        "min_confidence": 0.9,
                    },
                    True,
                    {"eligible", "archived"},
                ),
            ]
            for filters, inactive, expected in cases:
                status, result = await post(
                    port,
                    auth,
                    profile=profile,
                    include_inactive=inactive,
                    filters={"source": None, "min_confidence": None, **filters},
                )
                assert status == 200, (filters, result)
                assert {hit["memory"]["id"] for hit in result["results"]} == expected, (filters, result)
            # More ineligible rows than LIMIT; a late eligible MUST survive SQL filtering.
            status, result = await post(
                port,
                auth,
                profile=profile,
                limit=1,
                filters={"source": None, "min_confidence": 0.9, "lifecycle_states": ["validated"]},
            )
            assert status == 200 and [h["memory"]["id"] for h in result["results"]] == ["eligible"], result


@pytest.mark.asyncio
@pytest.mark.parametrize("no_fts", [False, True])
async def test_neutral_filter_parity_real_sqlite(tmp_path, no_fts):
    async with live_server(tmp_path) as (holder, port, auth):
        async with await read_scope(holder, auth) as scope:
            resources = scope.application.read_resources()
            await insert_facts(
                resources,
                [
                    {"id": "p1", "object": 'needle quote" star* AND percent% under_score back\\slash'},
                    {"id": "p2", "object": 'needle quote" other AND percent% under_score back\\slash'},
                    {"id": "p3", "object": "unrelated"},
                ],
            )
            if no_fts:
                async with resources.session_factory() as session, session.begin():
                    await session.execute(text("DROP TABLE facts_fts"))
            for query in ('quote"', "star*", "needle AND", "percent%", "under_score", "back\\slash", "!!!", "needle"):
                for neutral in ({"min_confidence": 0.0}, {"memory_types": ["fact"]}):
                    async with resources.session_factory() as session:
                        repo = FactRepository(session)
                        expected = await repo.search(text=query, limit=2, lifecycle_state="active")
                        over = await repo.search(text=query, limit=3, lifecycle_state="active")
                        actual = await resources.provider.search_facts_in_session(
                            session, text=query, limit=2, include_inactive=False, **neutral
                        )
                    assert [f.id for f in actual] == [f.id for f in expected], (query, neutral)
                    status, result = await post(
                        port, auth, query=query, limit=2, filters={"source": None, "min_confidence": None, **neutral}
                    )
                    assert status == 200, (query, result)
                    assert [h["memory"]["id"] for h in result["results"]] == [f.id for f in expected]
                    async with resources.session_factory() as session:
                        actual_over = await resources.provider.search_facts_in_session(
                            session, text=query, limit=3, include_inactive=False, **neutral
                        )
                    assert (len(actual_over) > 2) == (len(over) > 2)


@pytest.mark.asyncio
@pytest.mark.parametrize("no_fts", [False, True])
async def test_debug_logs_exclude_retrieval_bind_parameters(tmp_path, caplog, monkeypatch, no_fts):
    # Start with inherited DEBUG, including when another daemon test set WARNING.
    monkeypatch.setattr(logging.getLogger("aiosqlite"), "level", logging.NOTSET)
    with caplog.at_level(logging.DEBUG):
        async with live_server(tmp_path) as (holder, port, auth):
            if no_fts:
                async with await read_scope(holder, auth) as scope:
                    resources = scope.application.read_resources()
                    async with resources.session_factory() as session, session.begin():
                        await session.execute(text("DROP TABLE facts_fts"))
            canaries = ("FIX1_CANARY_TASK", "FIX1_CANARY_SUBJECT", "FIX1_CANARY_QUERY")
            status, body = await post(port, auth, "context", task=canaries[0], subject=canaries[1])
            assert status == 200, body
            status, body = await post(port, auth, query=canaries[2], filters={"source": None, "min_confidence": 0.0})
            assert status == 200, body
            logging.getLogger("fix1.capture").debug("debug capture is active")
        assert "debug capture is active" in caplog.text
        leaked = [record for record in caplog.records if any(value in record.getMessage() for value in canaries)]
        assert not leaked, [(record.name, record.getMessage()) for record in leaked]


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["task", "subject", "query"])
@pytest.mark.parametrize("control", ["\n", "\t", "\x00"])
async def test_nonprintable_retrieval_rejected_before_audit_and_lease(tmp_path, monkeypatch, field, control):
    async with live_server(tmp_path) as (holder, port, auth):
        calls = []
        acquire = holder.app.dependencies.leases.acquire

        def counted(ctx):
            calls.append(ctx.request_id)
            return acquire(ctx)

        monkeypatch.setattr(holder.app.dependencies.leases, "acquire", counted)
        operation = "search" if field == "query" else "context"
        status, body = await post(port, auth, operation, **{field: "FIX1_FORGED" + control + "entry"})
        assert status == 422 and body["error"]["code"] == "E_VALIDATION", body
        assert calls == []
        with sqlite3.connect(tmp_path / "control" / "http-access" / "access.sqlite") as db:
            assert db.execute("SELECT count(*) FROM access_audit").fetchone()[0] == 0


@pytest.mark.asyncio
async def test_preflight_audit_modes_and_canaries(tmp_path, caplog, monkeypatch):
    async with live_server(tmp_path) as (holder, port, auth):
        deps = holder.app.dependencies
        original = deps.leases.acquire
        calls = []

        def acquire(ctx):
            calls.append(ctx.profile_id)
            return original(ctx)

        monkeypatch.setattr(deps.leases, "acquire", acquire)
        now = datetime.now(UTC)
        token = deps.tokens._bootstrap_create(
            d.TokenCreate(
                subject="revealer",
                profiles=["alpha", "beta"],
                permissions=["memory.read", "memory.reveal"],
                expires_at=now + timedelta(hours=1),
            ),
            now=now,
        )
        revealer = {"Authorization": f"Bearer cmms_{token.token.id}.{token.secret}"}
        for profile in ("alpha", "beta"):
            for filters in ({"source": ""}, {"source": "CANARY_filter"}, {"tags": [""]}, {"tags": ["CANARY_tag"]}):
                for headers in (auth, revealer):
                    status, body = await post(
                        port, headers, profile=profile, filters={"source": None, "min_confidence": None, **filters}
                    )
                    assert status == 422 and body["error"]["code"] == "E_VALIDATION"
            for operation, change in (
                ("search", {"mode": "semantic"}),
                ("search", {"mode": "graph"}),
                ("search", {"trace": True}),
                ("context", {"trace": True}),
            ):
                status, body = await post(port, auth, operation=operation, profile=profile, **change)
                assert status == 503 and body["error"]["code"] == "E_CAPABILITY_UNAVAILABLE"
        assert not calls
        # Rejections precede audit and lease, not merely before SQL.
        with sqlite3.connect(tmp_path / "control" / "http-access" / "access.sqlite") as db:
            assert db.execute("SELECT count(*) FROM access_audit").fetchone()[0] == 0
        status, keyword = await post(port, auth, query="alpha")
        status2, hybrid = await post(port, auth, query="alpha", mode="hybrid")
        assert status == status2 == 200
        assert keyword["results"] == hybrid["results"] and keyword["total"] == 1
        assert keyword["degraded"] == []
        assert hybrid["degraded"] == [
            {"source": source, "code": "E_CAPABILITY_UNAVAILABLE"} for source in ("semantic", "graph")
        ]
        for hit in hybrid["results"]:
            assert hit["source"] == "fts" and hit["score"] == 0.0 and hit["memory"]["masked"]
        assert "CANARY_filter" not in caplog.text and "CANARY_tag" not in caplog.text


@pytest.mark.asyncio
async def test_retrieval_audit_intent_one_lease_stage_custody(tmp_path, monkeypatch):
    async with live_server(tmp_path) as (holder, port, auth):
        deps = holder.app.dependencies
        acquire = deps.leases.acquire
        completed = deps.metrics.retrieval_completed
        active, observed, stages = set(), [], []

        @asynccontextmanager
        async def watched(ctx):
            with sqlite3.connect(tmp_path / "control" / "http-access" / "access.sqlite") as db:
                row = db.execute(
                    "SELECT state,profile_id,token_id,operation FROM access_audit WHERE request_id=?", (ctx.request_id,)
                ).fetchall()
            assert len(row) == 1 and row[0][0] == "intent" and row[0][1] == ctx.profile_id
            assert row[0][2] == ctx.principal.token_id
            observed.append((ctx.request_id, ctx.profile_id, row[0][3]))
            async with acquire(ctx) as scope:
                active.add(ctx.request_id)
                try:
                    yield scope
                finally:
                    active.remove(ctx.request_id)

        def measured(ctx, stage, duration, error):
            assert ctx.request_id in active
            assert duration >= 0 and stage in ("fts", "context")
            stages.append((ctx.request_id, ctx.profile_id, stage, error))
            completed(ctx, stage, duration, error)

        monkeypatch.setattr(deps.leases, "acquire", watched)
        monkeypatch.setattr(deps.metrics, "retrieval_completed", measured)
        results = await asyncio.gather(
            *(
                post(port, auth, operation, profile, **changes)
                for operation, profile, changes in (
                    ("search", "alpha", {"query": "alpha"}),
                    ("context", "alpha", {"task": "alpha"}),
                    ("search", "beta", {"query": "alpha"}),
                    ("context", "beta", {"task": "alpha"}),
                )
            )
        )
        assert all(status == 200 for status, _ in results), results
        assert len(observed) == len(stages) == len({r for r, _, _ in observed}) == 4
        assert {(r, p) for r, p, _ in observed} == {(r, p) for r, p, _, _ in stages}
        assert all(error is None for _, _, _, error in stages)
        for status, body in results:
            with sqlite3.connect(tmp_path / "control" / "http-access" / "access.sqlite") as db:
                rows = db.execute("SELECT * FROM access_audit WHERE request_id=?", (body["request_id"],)).fetchall()
            assert len(rows) == 1 and "CANARY" not in str(rows)
        assert results[0][1]["total"] == results[1][1]["package"]["total"] == 1
        assert results[2][1]["total"] == results[3][1]["package"]["total"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("no_fts", [False, True])
async def test_search_filter_sql_and_snapshot(tmp_path, no_fts):
    async with live_server(tmp_path) as (holder, port, auth):
        async with await read_scope(holder, auth) as scope:
            resources = scope.application.read_resources()
            await insert_facts(resources, [{"id": "match", "confidence": 0.9}])
            if no_fts:
                async with resources.session_factory() as session, session.begin():
                    await session.execute(text("DROP TABLE facts_fts"))
            statements = []

            def capture(conn, cursor, statement, parameters, context, executemany):
                statements.append((id(conn), statement))

            event.listen(resources.provider.engine.sync_engine, "before_cursor_execute", capture)
            try:
                status, body = await post(
                    port, auth, limit=1, filters={"source": None, "min_confidence": 0.8, "lifecycle_states": ["active"]}
                )
            finally:
                event.remove(resources.provider.engine.sync_engine, "before_cursor_execute", capture)
            assert status == 200 and body["total"] == 1
            branch = "LIKE" if no_fts else "MATCH"
            matching = [(conn, sql) for conn, sql in statements if branch in sql]
            assert len(matching) == 1, statements
            sql = matching[0][1]
            where_start = sql.index("WHERE")
            limit_start = sql.index("LIMIT")
            assert where_start < sql.index("f.confidence >=") < limit_start
            assert where_start < sql.index("f.lifecycle_state IN") < limit_start
            # Both the default active restriction and explicit lifecycle filter belong in WHERE.
            assert sql[where_start:limit_start].count("f.lifecycle_state IN") == 2
            assert any(sql.strip().upper() == "BEGIN" for _, sql in statements)
            assert len({conn for conn, _ in statements}) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure,expected",
    [
        ("no such table: facts_fts", 200),
        ("malformed MATCH expression", 200),
        ("unable to use function MATCH in the requested context", 200),
        ("no such module: fts5", 200),
        ("database is locked", 503),
        ("disk I/O error", 503),
    ],
)
async def test_filter_fts_failure_classification_over_tcp(tmp_path, failure, expected):
    from sqlalchemy.exc import OperationalError

    async with live_server(tmp_path) as (holder, port, auth):
        async with await read_scope(holder, auth) as scope:
            resources = scope.application.read_resources()
            await insert_facts(resources, [{"id": "good", "confidence": 0.9}, {"id": "bad", "confidence": 0.1}])

            def fault(conn, cursor, statement, parameters, context, executemany):
                if "MATCH :" in statement or "MATCH ?" in statement:
                    raise OperationalError("CANARY_SQL", {"canary": "CANARY_PARAM"}, sqlite3.OperationalError(failure))

            event.listen(resources.provider.engine.sync_engine, "before_cursor_execute", fault)
            try:
                status, body = await post(port, auth, filters={"source": None, "min_confidence": 0.8})
            finally:
                event.remove(resources.provider.engine.sync_engine, "before_cursor_execute", fault)
            assert status == expected, body
            if status == 200:
                assert [hit["memory"]["id"] for hit in body["results"]] == ["good"]
            else:
                assert body["error"]["code"] == "E_STORAGE"
            assert "CANARY" not in json.dumps(body)


@pytest.mark.asyncio
async def test_retrieval_strict_validation_and_durable_audit(tmp_path, caplog, monkeypatch):
    async with live_server(tmp_path) as (holder, port, auth):
        for operation, fields in (
            ("search", {"limit": 0}),
            ("search", {"limit": 101}),
            ("search", {"limit": True}),
            ("search", {"filters": {"source": None, "min_confidence": float("nan")}}),
            ("search", {"filters": {"source": None, "min_confidence": -0.1}}),
            ("context", {"max_results": 0}),
            ("context", {"max_results": 101}),
            ("context", {"task": "x" * 4097}),
            ("context", {"extra": "CANARY_extra"}),
        ):
            status, body = await post(port, auth, operation, **fields)
            assert status == 422 and body["error"]["code"] == "E_VALIDATION"
        for operation in ("search", "context"):
            path = f"/api/v1/profiles/alpha/{operation}"
            status, body = await http_request(port, "POST", path, body=b"{}", headers=auth)
            assert status == 415
            status, body = await http_request(
                port, "POST", path, body=b"{", headers={**auth, "Content-Type": "application/json"}
            )
            assert status == 422
            status, body = await http_request(
                port, "POST", path + "?CANARY_query=1", body=b"{}", headers={**auth, "Content-Type": "application/json"}
            )
            assert status == 422
            status, body = await post(
                port, auth, operation, **({"query": "alpha"} if operation == "search" else {"task": "alpha"})
            )
            assert status == 200, body
            with sqlite3.connect(tmp_path / "control" / "http-access" / "access.sqlite") as db:
                rows = db.execute(
                    "SELECT state,profile_id,operation FROM access_audit WHERE request_id=?", (body["request_id"],)
                ).fetchall()
            # Durable audit updates the intent row in place to terminal.
            assert rows == [("terminal", "alpha", operation)], rows
        deps = holder.app.dependencies
        acquired = []
        original_acquire = deps.leases.acquire

        def counted(ctx):
            acquired.append(ctx.request_id)
            return original_acquire(ctx)

        monkeypatch.setattr(deps.leases, "acquire", counted)

        async def failed(*args, **kwargs):
            raise OSError("CANARY_AUDIT_FAILURE")

        for operation in ("search", "context"):
            for stage in ("begin", "finish"):
                acquired.clear()
                with monkeypatch.context() as patch:
                    patch.setattr(deps.access, stage, failed)
                    status, body = await post(port, auth, operation)
                assert status == 503 and body["error"]["code"] == "E_AUDIT_UNAVAILABLE"
                assert len(acquired) == (0 if stage == "begin" else 1)
                assert "CANARY" not in json.dumps(body)
                with sqlite3.connect(tmp_path / "control" / "http-access" / "access.sqlite") as db:
                    rows = db.execute(
                        "SELECT state FROM access_audit WHERE request_id=?", (body["error"]["request_id"],)
                    ).fetchall()
                assert rows == ([] if stage == "begin" else [("intent",)])
        assert "CANARY" not in caplog.text
