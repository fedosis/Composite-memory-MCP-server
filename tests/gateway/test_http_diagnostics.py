"""B1 diagnostics tests: service/auditor/outbox only; HTTP remains B3-mount RED."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import event, text

AT = datetime(2026, 10, 30, 12, 0, tzinfo=UTC)
IDS = tuple(f"00000000-0000-4000-8000-{n:012d}" for n in range(1, 1401))


def _principal(subject="b1-reader"):
    from memory_server.gateway_contracts.ports import Principal

    return Principal(
        "00000000-0000-4000-8000-000000009999",
        subject,
        None,
        frozenset({"alpha", "beta"}),
        frozenset({"audit.read", "profile.read"}),
        1,
        AT + timedelta(days=1),
    )


def _ctx(profile="alpha"):
    from memory_server.gateway_contracts.ports import RequestContext

    return RequestContext("req-b1", _principal(), profile, None, asyncio.get_running_loop().time() + 30)


def _foundation():
    import importlib

    service = importlib.import_module("memory_server.application.diagnostics_service")
    provider = importlib.import_module("memory_server.providers.sqlite_provider")
    outbox = importlib.import_module("storage.outbox")
    return service, provider, outbox


@pytest.fixture(autouse=True)
def offline_embedder(monkeypatch):
    from memory_server.providers.embedding_provider import MockEmbeddingProvider
    from memory_server.runtime import runtime_factory

    monkeypatch.setitem(runtime_factory._DEFAULT_RECIPES, "embedder", lambda runtime: MockEmbeddingProvider())


@pytest.fixture
async def database(tmp_path):
    from memory_server.providers.sqlite_provider import SQLiteProvider

    provider = SQLiteProvider("sqlite+aiosqlite:///" + str(tmp_path / "b1.sqlite"))
    await provider.initialize()
    try:
        yield provider
    finally:
        await provider.close()


async def _seed_outbox(provider, count=1400):
    async with provider._session_factory() as session, session.begin():
        from storage.outbox import OutboxEntryORM

        for n in range(count):
            session.add(
                OutboxEntryORM(
                    id=IDS[n],
                    record_id=IDS[n % 11],
                    record_type=("fact", "belief", "decision", "skill")[n % 4],
                    operation=("index_fact", "index_belief", "index_decision", "index_skill")[n % 4],
                    payload_json=json.dumps({"secret": "B1_PAYLOAD_CANARY"}),
                    status=("pending", "processing", "completed", "failed")[n % 4],
                    retry_count=n % 4,
                    error=("E_TIMEOUT" if n % 7 == 0 else "raw B1 error"),
                    created_at=AT.replace(tzinfo=None),
                    processed_at=AT.replace(tzinfo=None) if n % 4 >= 2 else None,
                )
            )


@pytest.fixture
async def shell(tmp_path):
    import importlib

    module = importlib.import_module("tests.gateway.test_http_shell")
    from memory_server.providers.lancedb_provider import LanceDBProvider

    for profile in ("alpha", "beta", "gamma"):
        root = tmp_path / module.CANARY / profile
        root.mkdir(parents=True)
        vector = LanceDBProvider(db_path=str(root / "lancedb"))
        await vector.create_collection("memories")
        await vector.close()
    async for value in module.shell.__wrapped__(tmp_path):
        yield value


@pytest.fixture
async def diagnostic_http(shell):
    from memory_server.runtime.read_composition import compose_read_ports
    from memory_server.transports.http.app import create_http_app

    deps, specs = compose_read_ports(shell.deps, shell.config)
    shell.app = create_http_app(shell.config, deps, read_routes=specs)
    shell.bearer, _ = shell.mint(permissions=("profile.read", "audit.read"))
    shell.specs = specs
    try:
        yield shell
    finally:
        await shell.app.close()


REFLECT = {"topic": None, "min_confidence": None, "context": None}
S10 = (
    ("audit", "audit"),
    ("indexes.status", "indexes/status"),
    ("outbox.list", "outbox"),
    ("overview", "overview"),
    ("reflect", "reflect"),
)
SECRETS = tuple(
    "S10_" + name + "_CANARY"
    for name in ("FACT", "PROPOSITION", "TAG", "SOURCE", "ERROR", "QUERY", "TOPIC", "CONTEXT", "CURSOR", "FOREIGN_ID")
)


async def _http(shell, suffix, *, profile="alpha", bearer=None, body=None, query=b"", receive=None):
    from tests.gateway.test_http_shell import request

    return await request(
        shell.app,
        f"/api/v1/profiles/{profile}/{suffix}",
        method="POST" if suffix == "reflect" else "GET",
        bearer=bearer or shell.bearer,
        body=body if body is not None else REFLECT if suffix == "reflect" else b"",
        query=query,
        receive=receive,
    )


async def _seed_diag(shell, profile="alpha", foreign=False):
    from storage.models.belief import BeliefORM
    from storage.models.fact import FactORM
    from storage.models.receipt import MemoryReceiptORM
    from storage.models.relation import ClaimRelationORM
    from storage.outbox import OutboxEntryORM

    from memory_server.gateway_contracts.ports import RequestContext

    principal = await shell.tokens.authenticate(shell.bearer, now=datetime.now(UTC))
    ctx = RequestContext("seed", principal, profile, None, asyncio.get_running_loop().time() + 30)
    async with shell.leases.acquire(ctx) as scope:
        provider = scope.runtime.dependencies.sqlite
        async with provider._session_factory() as session, session.begin():
            session.add(
                FactORM(
                    id=IDS[0],
                    subject=SECRETS[0],
                    predicate="is",
                    object="safe",
                    dedup_key=IDS[0],
                    creator="test",
                    confidence=0.8,
                    source=SECRETS[3],
                    lifecycle_state="active",
                    verification_status="validated",
                )
            )
            session.add(
                BeliefORM(
                    id=IDS[1],
                    proposition=SECRETS[1],
                    confidence=0.8,
                    source=SECRETS[3],
                    creator="test",
                    source_ids=[],
                    tags=[SECRETS[2], SECRETS[6] if foreign else "safe-topic"],
                    lifecycle_state="active",
                    verification_status="validated",
                )
            )
            session.add(
                MemoryReceiptORM(
                    id=IDS[0],
                    memory_type="fact",
                    source=SECRETS[3],
                    created_by="test",
                    timestamp=AT.replace(tzinfo=None),
                    lifecycle_state="active",
                    verification_status="validated",
                    confidence=0.8,
                )
            )
            for n in range(3, 6) if foreign else range(3, 5):
                session.add(
                    OutboxEntryORM(
                        id=IDS[n],
                        record_id=IDS[1],
                        record_type="belief",
                        operation="index_belief",
                        payload_json=json.dumps({"secret": SECRETS[1]}),
                        status="failed",
                        retry_count=1,
                        error=SECRETS[4],
                        created_at=AT.replace(tzinfo=None),
                    )
                )
            if foreign:
                session.add(
                    MemoryReceiptORM(
                        id=IDS[7],
                        memory_type="belief",
                        source=SECRETS[3],
                        created_by="test",
                        timestamp=AT.replace(tzinfo=None),
                        lifecycle_state="active",
                        verification_status="validated",
                        confidence=0.8,
                    )
                )
                session.add(ClaimRelationORM(source_id=IDS[0], target_id=IDS[1], relation_type="supports"))
        return scope


def _sweep(shell, caplog, responses):
    import base64

    from prometheus_client import generate_latest

    from tests.gateway.test_http_shell import rows

    surfaces = repr(rows(shell)) + generate_latest(shell.metrics._registry).decode()
    for response in responses:
        surfaces += response.body.decode()
        cursor = response.json.get("next_cursor")
        if cursor:
            surfaces += base64.urlsafe_b64decode(cursor.split(".")[0] + "==").decode()
    surfaces += repr([(r.msg, r.args, r.exc_text, r.exc_info, r.stack_info) for r in caplog.records])
    assert all(secret not in surfaces for secret in SECRETS), surfaces
    allowed = {"profile", "operation", "status_class", "le", "projection", "kind", "state"}
    for metric in shell.metrics._registry.collect():
        for sample in metric.samples:
            assert set(sample.labels) <= allowed, sample
            if "profile" in sample.labels:
                assert sample.labels["profile"] in {"alpha", "beta", "gamma", "_unknown"}
            if "operation" in sample.labels:
                from memory_server.gateway_contracts.catalog import CATALOG

                assert sample.labels["operation"] in CATALOG.by_operation
            if "status_class" in sample.labels:
                assert sample.labels["status_class"] in {"2xx", "4xx", "5xx"}


@pytest.mark.asyncio
async def test_s10_five_routes_mount_and_permissions(diagnostic_http, monkeypatch):
    from dataclasses import replace

    from memory_server.gateway_contracts import dto as d
    from memory_server.transports.http.app import create_http_app
    from tests.gateway.test_http_shell import code, rows

    shell = diagnostic_http
    await _seed_diag(shell)
    models = (d.AuditResult, d.IndexStatus, d.Page[d.OutboxItem], d.Overview, d.ReflectResult)
    for (op, suffix), model in zip(S10, models, strict=True):
        permission = "profile.read" if op in {"indexes.status", "overview"} else "audit.read"
        bearer, _ = shell.mint(permissions=(permission,))
        response = await _http(shell, suffix, bearer=bearer)
        assert response.status == 200, response.body
        assert set(response.json) == set(model.model_fields)
        model.model_validate_json(response.body)
        wrong, _ = shell.mint(permissions=("audit.read" if permission == "profile.read" else "profile.read",))
        before = rows(shell)
        code(await _http(shell, suffix, bearer=wrong), "E_PERMISSION_DENIED", 403)
        assert rows(shell) == before
        ports = dict(shell.app.dependencies.ports)
        del ports[op]
        missing = create_http_app(shell.config, replace(shell.app.dependencies, ports=ports), read_routes=shell.specs)
        with monkeypatch.context() as m:

            async def forbidden(*a, **k):
                pytest.fail("missing port reached intent/lease")

            m.setattr(shell.access, "begin", forbidden)
            m.setattr(shell.leases, "acquire", forbidden)
            from tests.gateway.test_http_shell import request

            result = await request(
                missing,
                f"/api/v1/profiles/alpha/{suffix}",
                bearer=bearer,
                method="POST" if op == "reflect" else "GET",
                body=REFLECT if op == "reflect" else b"",
            )
            code(result, "E_CAPABILITY_UNAVAILABLE", 503)
        await missing.close()
    assert len(shell.specs) == 19
    code(await _http(shell, "unknown"), "E_NOT_FOUND", 404)


@pytest.mark.parametrize(
    "kind",
    [
        "owner",
        "parser",
        "validator",
        "subclass",
        "codec",
        "nonoutbox-codec",
        "codec-subclass",
        "duplicate",
        "method",
        "path",
        "permission",
    ],
)
def test_s10_mount_rejections(shell, kind):
    from dataclasses import replace

    from memory_server.transports.http.app import create_http_app
    from memory_server.transports.http.routes.diagnostics import (
        DiagnosticsRouteSpec,
        OutboxCursorCodec,
        create_diagnostics_routes,
    )

    codec = OutboxCursorCodec(b"s" * 32, 300)
    spec, indexes, outbox, _, _ = create_diagnostics_routes(cursor_codec=codec)

    class Subspec(DiagnosticsRouteSpec):
        pass

    class Subcodec(OutboxCursorCodec):
        pass

    def forbidden(*a, **k):
        pytest.fail("substituted callable executed")

    cases = {
        "owner": (replace(spec, operation="profiles.list", path="/api/v1/profiles", permission="profile.read"),),
        "parser": (replace(spec, parse=forbidden),),
        "validator": (replace(spec, validate_result=forbidden),),
        "subclass": (Subspec(**vars(spec)),),
        "codec": (replace(outbox, cursor_codec=object()),),
        "codec-subclass": (replace(outbox, cursor_codec=Subcodec(b"s" * 32, 300)),),
        "nonoutbox-codec": (replace(indexes, cursor_codec=codec),),
        "duplicate": (spec, spec),
        "method": (replace(spec, method="POST"),),
        "path": (replace(spec, path="/wrong"),),
        "permission": (replace(spec, permission="profile.read"),),
    }
    with pytest.raises(ValueError):
        create_http_app(shell.config, shell.deps, read_routes=cases[kind])
    create_http_app(shell.config, shell.deps, read_routes=(replace(spec), outbox))


@pytest.mark.asyncio
async def test_s10_audit_metrics_canaries(diagnostic_http, monkeypatch, caplog):
    from tests.gateway.test_http_shell import code, rows

    shell = diagnostic_http
    scope = await _seed_diag(shell)
    provider = scope.runtime.dependencies.sqlite
    sql = []
    event.listen(provider.engine.sync_engine, "before_cursor_execute", lambda c, u, s, p, x, m: sql.append(s))

    async def snapshot():
        async with provider._session_factory() as session:
            names = (
                await session.execute(text("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"))
            ).scalars()
            return {name: (await session.execute(text(f'SELECT * FROM "{name}"'))).all() for name in names}

    before = await snapshot()
    graph_before = scope.runtime.dependencies.graph.to_dict()
    original_begin, original_finish = shell.access.begin, shell.access.finish
    calls = []

    async def begin(ctx, op):
        ticket = await original_begin(ctx, op)
        assert rows(shell)[-1]["state"] == "intent"
        calls.append(("begin", op))
        return ticket

    async def finish(ticket, outcome):
        calls.append(("finish", ticket.request_id))
        await original_finish(ticket, outcome)

    monkeypatch.setattr(shell.access, "begin", begin)
    monkeypatch.setattr(shell.access, "finish", finish)
    responses = []
    for op, suffix in S10:
        response = await _http(shell, suffix, query=b"limit=1" if op == "outbox.list" else b"")
        assert response.status == 200, response.body
        responses.append(response)
        assert rows(shell)[-1]["operation"] == op and rows(shell)[-1]["state"] == "terminal"
        assert rows(shell)[-1]["result"] == "success"
    assert len(rows(shell)) == 5 and len(calls) == 10
    assert responses[2].json["items"][0]["memory_id"] == IDS[1]
    assert responses[3].json["profile"]["storage_root"] is None
    assert responses[3].json["writer_owned"] == shell.leases.snapshot()["alpha"].owned
    assert before == await snapshot()
    assert scope.runtime.dependencies.graph.to_dict() == graph_before
    assert not any(
        s.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE", "REPLACE", "CREATE", "DROP")) for s in sql
    )
    _sweep(shell, caplog, responses)
    calls.clear()

    async def fault(*a, **k):
        raise OSError(SECRETS[4])

    with monkeypatch.context() as m:
        m.setattr(shell.access, "begin", fault)
        m.setattr(shell.leases, "acquire", lambda *a: pytest.fail("begin failure acquired lease"))
        code(await _http(shell, "audit"), "E_AUDIT_UNAVAILABLE", 503)
    assert calls == [] and len(rows(shell)) == 5
    with monkeypatch.context() as m:
        m.setattr(shell.access, "finish", fault)
        response = await _http(shell, "audit")
        code(response, "E_AUDIT_UNAVAILABLE", 503)
    pending = rows(shell)[-1]
    assert len(rows(shell)) == 6 and pending["state"] == "intent"
    assert [r["request_id"] for r in rows(shell)].count(pending["request_id"]) == 1
    import sqlite3

    filename = shell.access._db.execute("PRAGMA database_list").fetchone()[2]
    with sqlite3.connect(filename) as durable:
        assert (
            durable.execute("SELECT state FROM access_audit WHERE request_id=?", (pending["request_id"],)).fetchone()[0]
            == "intent"
        )
    _sweep(shell, caplog, [response])
    assert shell.leases.snapshot()["alpha"].admissions == 0


@pytest.mark.asyncio
async def test_s10_cross_profile_and_missing(diagnostic_http, monkeypatch, caplog):
    from tests.gateway.test_http_shell import code, request, rows

    shell = diagnostic_http
    a = await _seed_diag(shell)
    await _seed_diag(shell, "beta", foreign=True)
    alpha, _ = shell.mint(profiles=("alpha",), permissions=("profile.read", "audit.read"))
    before = rows(shell)
    with monkeypatch.context() as m:

        async def forbidden(*a, **k):
            pytest.fail("wrong scope reached registry")

        m.setattr(shell.registry, "resolve", forbidden)
        for _, suffix in S10:
            code(await _http(shell, suffix, profile="beta", bearer=alpha), "E_PERMISSION_DENIED", 403)
    assert rows(shell) == before
    out_a, out_b = await _http(shell, "outbox"), await _http(shell, "outbox", profile="beta")
    assert [i["event_id"] for i in out_a.json["items"]] == list(IDS[3:5])
    assert [i["event_id"] for i in out_b.json["items"]] == list(IDS[3:6])
    topic = {**REFLECT, "mode": "confidence", "topic": SECRETS[6]}
    reflected = await _http(shell, "reflect", body=topic)
    other = await _http(shell, "reflect", profile="beta", body=topic)
    assert reflected.status == other.status == 200
    assert reflected.json["sections"] != other.json["sections"]
    audit_a = await _http(shell, "audit", query=b"audit_type=receipts")
    audit_b = await _http(shell, "audit", profile="beta", query=b"audit_type=receipts")
    assert audit_a.status == audit_b.status == 200
    assert not any(i["code"] == "RECEIPT_ORPHAN" for i in audit_a.json["findings"]["items"])
    assert any(i["code"] == "RECEIPT_ORPHAN" for i in audit_b.json["findings"]["items"])
    relation_a = await _http(shell, "reflect", body={**REFLECT, "mode": "relations"})
    relation_b = await _http(shell, "reflect", profile="beta", body={**REFLECT, "mode": "relations"})
    assert relation_a.status == relation_b.status == 200
    assert relation_a.json["sections"] != relation_b.json["sections"]
    assert IDS[5] not in out_a.body.decode() and IDS[7] not in audit_a.body.decode()
    ov_a, ov_b = await _http(shell, "overview"), await _http(shell, "overview", profile="beta")
    assert ov_a.json["facts"] == ov_b.json["facts"] == 1
    assert ov_a.json["beliefs"] == ov_b.json["beliefs"] == 1
    unknown, _ = shell.mint(profiles=("missing",), permissions=("profile.read",))
    code(await _http(shell, "overview", profile="missing", bearer=unknown), "E_PROFILE_NOT_FOUND", 404)
    from memory_server.transports.http.app import with_profile_ports
    from memory_server.transports.http.routes.profiles import bind_control_pending

    with_profile_ports(shell.app, pending_control=bind_control_pending(shell.registry, shell.store))
    disabled = await request(
        shell.app,
        "/api/v1/profiles/beta/disable",
        method="POST",
        bearer=shell.mint()[0],
        body={"reason": "test", "confirmation": True, "expected_generation": 1},
        headers=[("Idempotency-Key", str(uuid4()))],
    )
    assert disabled.status == 200, disabled.body
    code(await _http(shell, "overview", profile="beta"), "E_PROFILE_DISABLED", 409)
    with monkeypatch.context() as m:

        async def broken(*a, **k):
            raise OSError(SECRETS[4])

        m.setattr(a.runtime.dependencies.sqlite, "diagnostics_count", broken)
        code(await _http(shell, "overview"), "E_STORAGE", 503)
    shell.app.guard.profiles[("alpha", "profile.read")] = shell.config.retrieval_concurrency
    code(await _http(shell, "overview"), "E_RATE_LIMIT", 429)
    shell.app.guard.profiles.clear()
    with monkeypatch.context() as m:

        async def slow(*a, **k):
            await asyncio.sleep(10)

        m.setattr(a.runtime.dependencies.sqlite, "diagnostics_count", slow)
        shell.app.config = __import__("dataclasses").replace(
            shell.config, request_timeout_seconds=0.05, header_timeout_seconds=0.05
        )
        code(await _http(shell, "overview"), "E_TIMEOUT", 504)
    _sweep(shell, caplog, [out_a, out_b, reflected, other, audit_a, audit_b, ov_a, ov_b])


@pytest.mark.asyncio
async def test_s10_request_size_and_audit_cursor(diagnostic_http):
    from tests.gateway.test_http_shell import code, rows

    shell = diagnostic_http
    chunks = iter([b"x" * (shell.config.max_body_bytes // 2)] * 3)

    async def streamed():
        return {"type": "http.request", "body": next(chunks), "more_body": True}

    code(await _http(shell, "reflect", receive=streamed), "E_REQUEST_TOO_LARGE", 413)
    assert rows(shell) == []
    for cursor in (b"", b"random", SECRETS[8].encode(), b"foreign", b"expired"):
        code(await _http(shell, "audit", query=b"cursor=" + cursor), "E_CURSOR_INVALID", 400)
        assert rows(shell) == []
    for suffix in ("indexes/status", "overview"):
        code(await _http(shell, suffix, query=b"unknown=1"), "E_VALIDATION", 422)
        code(await _http(shell, suffix, body=b"{}"), "E_VALIDATION", 422)
        assert rows(shell) == []
    result = await _http(shell, "audit")
    assert result.status == 200 and len(rows(shell)) == 1


@pytest.mark.asyncio
async def test_s10_provider_log_scope_isolation(diagnostic_http, monkeypatch, caplog):
    from tests.gateway.test_http_shell import code

    shell = diagnostic_http
    scope = await _seed_diag(shell)
    vector = scope.runtime.dependencies.vector
    assert vector is not None
    client = await vector._get_table()
    assert client is not None

    def broken(*a, **k):
        raise RuntimeError(SECRETS[4])

    logger = logging.getLogger("memory_server.providers.lancedb_provider")
    with monkeypatch.context() as m, caplog.at_level(logging.ERROR):
        m.setattr(type(client), "count_rows", broken)
        response = await _http(shell, "indexes/status")
        code(response, "E_VECTOR_BACKEND", 503)
        records = [r for r in caplog.records if r.name == logger.name]
        assert records and all(r.args == () and r.exc_info is None and r.exc_text is None for r in records)
        assert all(SECRETS[4] not in repr(vars(r)) for r in records)
        logger.error("outside %s", SECRETS[4])
        assert caplog.records[-1].args == (SECRETS[4],)
    assert shell.leases.snapshot()["alpha"].admissions == 0
    import threading

    started, release = threading.Event(), threading.Event()

    def blocked(*a, **k):
        started.set()
        assert release.wait(5)
        logger.error("native %s", SECRETS[4], exc_info=True)
        return 0

    with monkeypatch.context() as m, caplog.at_level(logging.ERROR):
        m.setattr(type(client), "count_rows", blocked)
        task = asyncio.create_task(_http(shell, "indexes/status"))
        try:
            assert await asyncio.to_thread(started.wait, 5)
            caplog.clear()
            logger.error("concurrent %s", "ordinary")
            assert caplog.records[-1].args == ("ordinary",)
            task.cancel()
            await asyncio.sleep(0.02)
            assert not task.done() and shell.leases.snapshot()["alpha"].admissions == 1
        finally:
            release.set()
        code(await asyncio.wait_for(task, 5), "E_TIMEOUT", 504)
        assert any(r.msg == "diagnostics backend failure" and r.args == () for r in caplog.records)
        assert all(SECRETS[4] not in repr(vars(r)) for r in caplog.records)
        logger.error("after cancel %s", "ordinary-after")
        assert caplog.records[-1].args == ("ordinary-after",)
    assert shell.leases.snapshot()["alpha"].admissions == 0


@pytest.mark.asyncio
async def test_s10_http_canary_modes_and_identifiers(diagnostic_http, caplog):
    from tests.gateway.test_http_shell import code, rows

    shell = diagnostic_http
    scope = await _seed_diag(shell)
    responses = []
    for mode in ("overview", "topics", "contradictions", "confidence", "decay", "evidence_audit", "relations"):
        response = await _http(shell, "reflect", body={**REFLECT, "mode": mode, "context": SECRETS[7]})
        assert response.status == 200, response.body
        responses.append(response)
    for suffix, query in (("audit", b"query=" + SECRETS[5].encode()), ("outbox", b"cursor=" + SECRETS[8].encode())):
        before = rows(shell)
        response = await _http(shell, suffix, query=query)
        assert response.status in (400, 422)
        assert rows(shell) == before
        responses.append(response)
    provider = scope.runtime.dependencies.sqlite
    async with provider._session_factory() as session, session.begin():
        assert SECRETS[0] in repr((await session.execute(text("SELECT * FROM facts"))).all())
        assert SECRETS[1] in repr((await session.execute(text("SELECT * FROM beliefs"))).all())
        assert SECRETS[4] in repr((await session.execute(text("SELECT * FROM outbox_entries"))).all())
        await session.execute(
            text("UPDATE outbox_entries SET record_id=:unsafe WHERE id=:id"), {"unsafe": SECRETS[9], "id": IDS[3]}
        )
    response = await _http(shell, "outbox")
    code(response, "E_STORAGE", 503)
    responses.append(response)
    safe = await _http(shell, "outbox", query=("memory_id=" + IDS[1]).encode())
    assert safe.status == 200 and [r["event_id"] for r in safe.json["items"]] == [IDS[4]]
    responses.append(safe)
    _sweep(shell, caplog, responses)


@pytest.mark.asyncio
async def test_s10_tcp_publication(diagnostic_http):
    from tests.gateway.test_http_security import wire
    from tests.gateway.test_slice_runtime import http_request

    shell = diagnostic_http
    await _seed_diag(shell)
    async with wire(shell.app, shell.config) as port:
        for _, suffix in S10:
            status, payload = await http_request(
                port,
                "POST" if suffix == "reflect" else "GET",
                f"/api/v1/profiles/alpha/{suffix}",
                body=json.dumps(REFLECT).encode() if suffix == "reflect" else b"",
                headers={"Authorization": "Bearer " + shell.bearer, "Content-Type": "application/json"},
            )
            assert status == 200 and "error" not in payload, payload


@pytest.mark.asyncio
async def test_s10_binder_optional_errors_are_narrow(diagnostic_http, monkeypatch):
    from memory_server.application.errors import UnknownDependencyError
    from memory_server.runtime.read_composition import bind_diagnostics

    scope = await _seed_diag(diagnostic_http)
    bound = bind_diagnostics(scope)
    assert bound.canonical.provider is scope.runtime.dependencies.sqlite
    assert bound.vector is scope.runtime.dependencies.vector and bound.graph is scope.runtime.dependencies.graph
    assert bound.reflection is scope.application.reflection
    assert bound.validator is scope.application.audit._validator
    assert bound.confidence is scope.application.audit._confidence_engine
    assert bound.writer_owned == diagnostic_http.leases.snapshot()["alpha"].owned
    for owner, attr, field in (
        (scope.runtime.dependencies, "vector", "vector"),
        (scope.runtime.dependencies, "graph", "graph"),
        (scope.application, "reflection", "reflection"),
        (scope.application.audit, "_validator", "validator"),
        (scope.application.audit, "_confidence_engine", "confidence"),
        (scope.application, "audit", "validator"),
    ):
        for exception in (UnknownDependencyError, RuntimeError):

            def fail(_self):
                raise exception("optional fault")

            with monkeypatch.context() as m:
                m.setattr(type(owner), attr, property(fail), raising=False)
                if exception is UnknownDependencyError:
                    assert getattr(bind_diagnostics(scope), field) is None
                else:
                    with pytest.raises(RuntimeError, match="optional fault"):
                        bind_diagnostics(scope)


@pytest.mark.asyncio
async def test_audit_dispatcher_truncation_unavailable_and_coalescing(database):
    service, provider_module, _ = _foundation()
    resources = _resources(database)
    service_obj = service.DiagnosticsService(None, None)
    from memory_server.gateway_contracts import dto as d

    async with database._session_factory() as session, session.begin():
        work = provider_module.DiagnosticsReadWork(
            soft_deadline=asyncio.get_running_loop().time() + 30, hard_deadline=asyncio.get_running_loop().time() + 30
        )
        async with database.diagnostics_window(session, work=work):
            await session.execute(text("BEGIN"))
            for audit_type, expected in (
                ("lifecycle", {"lifecycle"}),
                ("receipts", {"receipts"}),
                ("relations", {"relations", "graph_orphans"}),
                ("indexes", {"indexes"}),
                (
                    "full",
                    {"lifecycle", "receipts", "relations", "graph_orphans", "indexes", "confidence", "consistency"},
                ),
            ):
                result = await service_obj._audit(
                    _ctx(), d.AuditQuery(audit_type=audit_type, limit=1, cursor=None), resources, session, work, AT
                )
                assert set(result.sections) == expected
                assert result.findings.has_more is False and result.findings.next_cursor is None
            result = await service_obj._audit(
                _ctx(), d.AuditQuery(audit_type="full", limit=1, cursor=None), resources, session, work, AT
            )
            assert result.complete is False  # graph_orphans is required but unavailable
            assert len(result.findings.items) <= 1
            assert len({item.id for item in result.findings.items}) == len(result.findings.items)
            assert all(item.aggregate and item.object_id is None for item in result.findings.items)
            assert "B1_PAYLOAD_CANARY" not in result.model_dump_json()


@pytest.mark.asyncio
async def test_outbox_real_page_tie_filters_and_six_known_unknown_codes(database):
    _, provider_module, outbox_module = _foundation()
    await _seed_outbox(database)
    statements = []

    def trace(*args):
        statements.append(args[2])

    event.listen(database.engine.sync_engine, "before_cursor_execute", trace)
    try:
        async with database._session_factory() as session, session.begin():
            work = provider_module.DiagnosticsReadWork(
                soft_deadline=asyncio.get_running_loop().time() + 30,
                hard_deadline=asyncio.get_running_loop().time() + 30,
            )
            async with database.diagnostics_window(session, work=work):
                await session.execute(text("BEGIN"))
                from storage.outbox import OutboxRepository

                repo = OutboxRepository(session)
                seen = []
                last = upper = None
                while True:
                    page = await repo.diagnostic_page(
                        limit=37, last=last, upper=upper, status=None, record_id=None, work=work
                    )
                    upper = page.upper
                    rows = page.rows[:37]
                    seen.extend(row.id for row in rows)
                    if len(page.rows) <= 37:
                        break
                    last = outbox_module.OutboxKey(rows[-1].created_at.isoformat(timespec="microseconds"), rows[-1].id)
                assert seen == sorted(IDS[:1400]) and len(seen) == len(set(seen)) == 1400
                filtered = await repo.diagnostic_page(
                    limit=200, last=None, upper=None, status="applied", record_id=IDS[2], work=work
                )
                assert filtered.rows and all(
                    row.status == "completed" and row.record_id == IDS[2] for row in filtered.rows
                )
    finally:
        event.remove(database.engine.sync_engine, "before_cursor_execute", trace)
    selects = [
        sql.lower()
        for sql in statements
        if sql.lstrip().lower().startswith("select") and "outbox_entries" in sql.lower()
    ]
    assert selects and all("payload_json" not in sql for sql in selects)
    known = {
        "E_TIMEOUT",
        "E_STORAGE",
        "E_VECTOR_BACKEND",
        "E_GRAPH_BACKEND",
        "E_PROJECTION_UNAVAILABLE",
        "E_CAPABILITY_UNAVAILABLE",
    }
    assert {outbox_module.classify_diagnostic_error(x) for x in known} == known
    assert all(
        outbox_module.classify_diagnostic_error(x) is None
        for x in (None, "", " E_TIMEOUT", "E_TIMEOUT ", "E_TIMEOUT: B1_PAYLOAD_CANARY", RuntimeError("E_TIMEOUT"))
    )


@pytest.mark.parametrize(
    "provider_logger", ["memory_server.providers.lancedb_provider", "memory_server.providers.qdrant_provider"]
)
@pytest.mark.asyncio
async def test_b1_canary_provider_logs(database, caplog, provider_logger):
    service, provider_module, _ = _foundation()
    before = []
    async with database._session_factory() as session:
        before = (await session.execute(text("SELECT COUNT(*) FROM outbox_entries"))).scalar_one()
    logger = logging.getLogger(provider_logger)
    async with service.diagnostics_log_scope():
        with caplog.at_level(logging.ERROR, logger=logger.name):
            logger.error("B1_LOG_CANARY %s", "secret")
    assert "B1_LOG_CANARY" not in caplog.text and "secret" not in caplog.text
    with caplog.at_level(logging.ERROR):
        logger.error("OUTSIDE_SCOPE_VISIBLE")
        async with service.diagnostics_log_scope():
            logging.getLogger("b1.foreign").error("FOREIGN_LOGGER_VISIBLE")
    assert "OUTSIDE_SCOPE_VISIBLE" in caplog.text and "FOREIGN_LOGGER_VISIBLE" in caplog.text
    async with database._session_factory() as session:
        after = (await session.execute(text("SELECT COUNT(*) FROM outbox_entries"))).scalar_one()
    assert after == before
    assert provider_module.DiagnosticsReadWork().statements == 0


@pytest.mark.asyncio
async def test_b1_cross_profile_service_rejects_before_lease_or_write():
    service, _, _ = _foundation()
    from memory_server.gateway_contracts import dto as d
    from memory_server.transports.http.errors import HttpError

    command = d.COMMAND_TYPES["outbox.list"](
        operation="outbox.list",
        profile_id="beta",
        path_ids={},
        payload=d.OutboxQuery(limit=1, cursor=None, status=None, memory_id=None),
    )
    with pytest.raises(HttpError, match="E_PERMISSION_DENIED"):
        await service.DiagnosticsService(None, None).execute(_ctx("alpha"), command)


def _resources(database):
    from memory_server.evaluation.confidence import ConfidenceEngine
    from memory_server.evaluation.validator import Validator

    return SimpleNamespace(
        canonical=SimpleNamespace(provider=database, session_factory=database._session_factory),
        validator=Validator(),
        confidence=ConfidenceEngine(),
    )


@pytest.mark.asyncio
async def test_audit_snapshot_truncation_calls_five_real_auditor_checks(database, monkeypatch):
    service, _, _ = _foundation()
    from memory_server.evaluation.auditor import MemoryAuditor
    from memory_server.gateway_contracts import dto as d

    # Real SQL snapshot contains the first 200 rows; the 201st is only a sentinel.
    async with database._session_factory() as session, session.begin():
        for i in range(201):
            from storage.models.fact import FactORM

            session.add(
                FactORM(
                    id=IDS[i],
                    subject="B1_CONTENT_CANARY",
                    predicate="p",
                    object="o",
                    dedup_key=IDS[i],
                    confidence=0.1,
                    lifecycle_state="active",
                    verification_status="unverified",
                )
            )
    calls, snapshots = [], []
    original_init = MemoryAuditor.__init__

    def init(self, *args, **kwargs):
        snapshots.append(kwargs["persisted_records"])
        assert isinstance(kwargs["persisted_records"], list)
        return original_init(self, *args, **kwargs)

    monkeypatch.setattr(MemoryAuditor, "__init__", init)
    for name in (
        "check_lifecycle_violations",
        "check_missing_receipts",
        "check_confidence_issues",
        "audit_consistency",
        "check_sql_vector_drift",
    ):
        original = getattr(MemoryAuditor, name)

        def wrap(self, original=original, name=name):
            calls.append(name)
            return original(self)

        monkeypatch.setattr(MemoryAuditor, name, wrap)
    resources = _resources(database)
    query = d.AuditQuery(audit_type="full", limit=200, cursor=None)
    result = await service.run_borrowed_diagnostics(
        _ctx(),
        resources,
        lambda session, work, sampled: service.DiagnosticsService(None, None)._audit(
            _ctx(), query, resources, session, work, sampled
        ),
    )
    assert len(set(calls)) == 5 and all(len(records) == 200 for records in snapshots)
    assert result.complete is False and result.sections["graph_orphans"] == "unavailable"
    assert "B1_CONTENT_CANARY" not in result.model_dump_json()
    assert sum(item.code == "RECEIPT_MISSING" for item in result.findings.items) == 1
    assert sum(item.code == "CONFIDENCE_LOW" for item in result.findings.items) == 1


def test_audit_coalescing_is_deterministic_and_closed_validators():
    service, _, _ = _foundation()
    from memory_server.gateway_contracts import dto as d
    from memory_server.transports.http.errors import HttpError
    from memory_server.transports.http.routes.diagnostics import validate_audit_result

    checks = [service.AuditCheck("receipts", "RECEIPT_MISSING", "warning", n) for n in (3, 1, 8)]
    query = d.AuditQuery(audit_type="receipts", limit=1, cursor=None)
    ctx = SimpleNamespace(request_id="b1")
    result = service.map_audit_result(ctx, checks, query=query, sampled_at=AT, snapshot_truncated=False)
    reverse = service.map_audit_result(ctx, checks[::-1], query=query, sampled_at=AT, snapshot_truncated=False)
    assert result == reverse and len(result.findings.items) == 1
    assert validate_audit_result(result) == result
    unavailable = service.map_audit_result(
        ctx,
        checks + [service.AuditCheck("receipts", "RECEIPT_MISSING", "warning", None, False)],
        query=query,
        sampled_at=AT,
        snapshot_truncated=False,
    )
    assert unavailable.sections == {"receipts": "unavailable"}
    assert unavailable.complete is False and len(unavailable.findings.items) == 1
    for change in ({"raw": "B1_CONTENT_CANARY"}, {"receipts": "healthy"}):
        with pytest.raises(HttpError, match="E_INTERNAL"):
            validate_audit_result(result.model_copy(update={"sections": change}))
    finding = result.findings.items[0].model_copy(update={"code": "B1_CONTENT_CANARY"})
    with pytest.raises(HttpError, match="E_INTERNAL"):
        validate_audit_result(
            result.model_copy(update={"findings": result.findings.model_copy(update={"items": [finding]})})
        )


def test_s10b2_route_specs_include_metrics_surface():
    from memory_server.transports.http.routes.diagnostics import DIAGNOSTICS_ROUTE_SPECS

    assert set(DIAGNOSTICS_ROUTE_SPECS) == {"audit", "indexes.status", "outbox.list", "overview", "reflect"}


@pytest.mark.asyncio
async def test_s10b2_service_accepts_indexes_operation():
    service, _, _ = _foundation()
    from memory_server.gateway_contracts import dto as d

    command = d.COMMAND_TYPES["indexes.status"](
        operation="indexes.status", profile_id="alpha", path_ids={}, payload=d.Empty()
    )
    assert command.operation == "indexes.status"
    assert hasattr(service.DiagnosticsService, "indexes_status")


@pytest.mark.asyncio
async def test_outbox_service_codec_wire_enum_and_no_writes(shell, tmp_path, monkeypatch):
    service, _, _ = _foundation()
    from memory_server.gateway_contracts import dto as d
    from memory_server.gateway_contracts.ports import RequestContext
    from memory_server.providers.embedding_provider import MockEmbeddingProvider
    from memory_server.runtime import runtime_factory
    from memory_server.transports.http.errors import HttpError
    from memory_server.transports.http.routes.diagnostics import OutboxCursorCodec, validate_outbox_page

    monkeypatch.setitem(runtime_factory._DEFAULT_RECIPES, "embedder", lambda runtime: MockEmbeddingProvider())
    from memory_server.providers.lancedb_provider import LanceDBProvider

    for binding in shell.bindings[:2]:
        vector = LanceDBProvider(db_path=str(binding.layout.data_root / "lancedb"))
        await vector.create_collection("memories")
        await vector.close()
    bearer, _ = shell.mint(permissions=("audit.read",))
    principal = await shell.tokens.authenticate(bearer, now=datetime.now(UTC))

    def context(profile):
        return RequestContext("b1-service", principal, profile, None, asyncio.get_running_loop().time() + 30)

    def bind(scope):
        return service.DiagnosticsResources(
            scope.application.read_resources(), scope.binding.profile, None, None, None, None, None, None
        )

    codec = OutboxCursorCodec.from_path(tmp_path / "outbox.key", 900)
    obj = service.DiagnosticsService(shell.leases, bind, cursor_codec=codec)
    async with shell.leases.acquire(context("alpha")) as scope:
        provider = scope.application.read_resources().provider
        await _seed_outbox(provider, 9)
        async with provider._session_factory() as session, session.begin():
            codes = [
                "E_TIMEOUT",
                "E_STORAGE",
                "E_VECTOR_BACKEND",
                "E_GRAPH_BACKEND",
                "E_PROJECTION_UNAVAILABLE",
                "E_CAPABILITY_UNAVAILABLE",
                "E_TIMEOUT: B1_RAW_ERROR_CANARY",
                " E_TIMEOUT",
                None,
            ]
            for ident, error in zip(IDS, codes):
                await session.execute(
                    text("UPDATE outbox_entries SET error=:error WHERE id=:id"), {"error": error, "id": ident}
                )
        statements = []

        def trace(*args):
            statements.append(args[2])

        event.listen(provider.engine.sync_engine, "before_cursor_execute", trace)
        try:
            seen, cursor = [], None
            for _ in range(3):
                command = d.COMMAND_TYPES["outbox.list"](
                    operation="outbox.list",
                    profile_id="alpha",
                    path_ids={},
                    payload=d.OutboxQuery(limit=3, cursor=cursor, status=None, memory_id=None),
                )
                page = validate_outbox_page(await obj.execute(context("alpha"), command))
                seen.extend(page.items)
                cursor = page.next_cursor
            assert cursor is None and [item.event_id for item in seen] == list(IDS[:9])
            assert [item.last_error_code for item in seen] == codes[:6] + [None, None, None]
            assert [item.memory_id for item in seen] == list(IDS[:9])
            assert [item.attempts for item in seen] == [n % 4 for n in range(9)]
            wire = page.model_dump_json()
            assert all(
                secret not in wire
                for secret in ("B1_RAW_ERROR_CANARY", "B1_PAYLOAD_CANARY", '"error"', "payload_json", '"payload"')
            )
            invalid = seen[0].model_copy(update={"last_error_code": "E_UNKNOWN"})
            with pytest.raises(HttpError, match="E_INTERNAL"):
                validate_outbox_page(page.model_copy(update={"items": [invalid]}))
            beta = d.COMMAND_TYPES["outbox.list"](
                operation="outbox.list",
                profile_id="beta",
                path_ids={},
                payload=d.OutboxQuery(limit=3, cursor=None, status=None, memory_id=None),
            )
            assert (await obj.execute(context("beta"), beta)).items == []
            assert all(sql.lstrip().split()[0].upper() in {"SELECT", "BEGIN"} for sql in statements)
        finally:
            event.remove(provider.engine.sync_engine, "before_cursor_execute", trace)


@pytest.mark.parametrize("layer", ["audit", "finding", "audit_page", "section", "outbox", "outbox_item"])
def test_b1_validators_reject_extra_nested_keys(layer):
    service, _, _ = _foundation()
    from memory_server.gateway_contracts import dto as d
    from memory_server.transports.http.errors import HttpError
    from memory_server.transports.http.routes.diagnostics import validate_audit_result, validate_outbox_page

    audit = service.map_audit_result(
        SimpleNamespace(request_id="r"),
        [service.AuditCheck("receipts", "RECEIPT_MISSING", "warning", 1)],
        query=d.AuditQuery(audit_type="receipts", limit=200, cursor=None),
        sampled_at=AT,
        snapshot_truncated=False,
    ).model_dump()
    if layer == "outbox" or layer == "outbox_item":
        raw = {"items": [], "has_more": False, "next_cursor": None, "request_id": "r"}
        if layer == "outbox":
            raw["payload_json"] = "CANARY"
        else:
            raw["items"] = [{"error": "CANARY"}]
        validator = validate_outbox_page
    else:
        raw = audit
        target = (
            raw
            if layer == "audit"
            else raw["findings"]["items"][0]
            if layer == "finding"
            else raw["findings"]
            if layer == "audit_page"
            else raw["sections"]
        )
        target["payload_json"] = "CANARY"
        validator = validate_audit_result
    with pytest.raises(HttpError, match="E_INTERNAL"):
        validator(SimpleNamespace(model_dump=lambda: raw))


@pytest.mark.asyncio
async def test_audit_missing_capability_and_finding_limit_are_honest(database):
    service, _, _ = _foundation()
    from memory_server.gateway_contracts import dto as d
    from memory_server.transports.http.errors import HttpError

    resources = _resources(database)
    resources.confidence = None
    query = d.AuditQuery(audit_type="lifecycle", limit=1, cursor=None)
    with pytest.raises(HttpError, match="E_CAPABILITY_UNAVAILABLE"):
        await service.run_borrowed_diagnostics(
            _ctx(),
            resources,
            lambda session, work, sampled: service.DiagnosticsService(None, None)._audit(
                _ctx(), query, resources, session, work, sampled
            ),
        )
    checks = [
        service.AuditCheck("lifecycle", "STATE_METADATA_INVALID", "error", 1),
        service.AuditCheck("lifecycle", "RECEIPT_STATE_MISMATCH", "warning", 1),
    ]
    result = service.map_audit_result(_ctx(), checks, query=query, sampled_at=AT, snapshot_truncated=False)
    assert result.complete is False and len(result.findings.items) == 1
    assert result.sections == {"lifecycle": "warning"} and result.sampled_at == AT


@pytest.mark.asyncio
async def test_outbox_cursor_validation_precedes_lease(tmp_path):
    service, _, outbox = _foundation()
    from memory_server.gateway_contracts import dto as d
    from memory_server.transports.http.errors import HttpError
    from memory_server.transports.http.routes.diagnostics import OutboxCursorCodec

    codec = OutboxCursorCodec.from_path(tmp_path / "cursor.key", 900)
    ctx = _ctx()

    def command(cursor=None, status=None):
        return d.COMMAND_TYPES["outbox.list"](
            operation="outbox.list",
            profile_id="alpha",
            path_ids={},
            payload=d.OutboxQuery(limit=1, cursor=cursor, status=status, memory_id=None),
        )

    point = outbox.OutboxKey(AT.isoformat(timespec="microseconds"), IDS[0])
    token = codec.issue(ctx, command(), last=point, upper=point, limit=1, now=codec.clock())
    for candidate in (command(token, "failed"), command(token[:-1] + ("A" if token[-1] != "A" else "B"))):
        with pytest.raises(HttpError, match="E_CURSOR_INVALID"):
            await service.DiagnosticsService(None, None, cursor_codec=codec).execute(ctx, candidate)


def _command(*, limit=100, cursor=None, status=None, memory_id=None):
    from memory_server.gateway_contracts import dto as d

    return d.COMMAND_TYPES["outbox.list"](
        operation="outbox.list",
        profile_id="alpha",
        path_ids={},
        payload=d.OutboxQuery(limit=limit, cursor=cursor, status=status, memory_id=memory_id),
    )


def test_outbox_cursor_golden_binding_tamper_expiry_and_from_path(tmp_path):
    import base64
    import hashlib
    import hmac
    import importlib

    from memory_server.gateway_contracts.ports import RequestContext
    from memory_server.transports.http.errors import HttpError

    _, _, outbox = _foundation()
    routes = importlib.import_module("memory_server.transports.http.routes.diagnostics")
    codec = routes.OutboxCursorCodec(b"k" * 32, ttl_seconds=300, clock=lambda: AT)
    ctx = RequestContext("request", _principal("diagnostic-reader"), "alpha", None, 1.0)
    key1 = outbox.OutboxKey("2026-10-30T12:00:00.000000+00:00", IDS[0])
    key2 = outbox.OutboxKey("2026-10-30T12:00:00.000000+00:00", IDS[999])
    command = _command(limit=100, status="applied", memory_id=IDS[2])
    token = codec.issue(ctx, command, last=key1, upper=key2, limit=100, now=AT)
    golden = (
        "eyJleHBpcmVzX2F0IjoiMjAyNi0xMC0zMFQxMjowNTowMCswMDowMCIsImZpbHRlcl9zaGEyNTYiOiIyNGMyMzNiNTQwNDdmNmEx"
        "YmZiZjZmZTc1MzU3YzhmNDQ0ZjJmOTkxZGE3NjVlMDFkZjQ1M2I5OWQ5NmVjNDY0IiwiaXNzdWVkX2F0IjoiMjAyNi0xMC0zMFQx"
        "MjowMDowMCswMDowMCIsImxhc3QiOlsiMjAyNi0xMC0zMFQxMjowMDowMC4wMDAwMDArMDA6MDAiLCIwMDAwMDAwMC0wMDAwLTQw"
        "MDAtODAwMC0wMDAwMDAwMDAwMDEiXSwibGltaXQiOjEwMCwib3BlcmF0aW9uIjoib3V0Ym94Lmxpc3QiLCJwcm9maWxlX2lkIjoi"
        "YWxwaGEiLCJzdWJqZWN0IjoiZGlhZ25vc3RpYy1yZWFkZXIiLCJ1cHBlciI6WyIyMDI2LTEwLTMwVDEyOjAwOjAwLjAwMDAwMCsw"
        "MDowMCIsIjAwMDAwMDAwLTAwMDAtNDAwMC04MDAwLTAwMDAwMDAwMTAwMCJdLCJ2IjoxfQ.v15tnX4XUE-oJFlFWJoTNIAiRcmY6"
        "kmJM4Itjp9vi5c"
    )
    assert len(golden.encode("ascii")) == 614
    assert token.encode("ascii") == golden.encode("ascii")
    state = codec.validate(ctx.principal, _command(limit=20, cursor=token, status="applied", memory_id=IDS[2]), now=AT)
    assert (state.last, state.upper, state.limit, state.expires_at) == (key1, key2, 100, AT + timedelta(seconds=300))
    for changed in (
        token[:-1] + ("A" if token[-1] != "A" else "B"),
        routes.OutboxCursorCodec(b"z" * 32).issue(ctx, command, last=key1, upper=key2, limit=100, now=AT),
    ):
        with pytest.raises(HttpError, match="E_CURSOR_INVALID"):
            codec.validate(ctx.principal, _command(cursor=changed, status="applied", memory_id=IDS[2]), now=AT)
    for wrong in (
        _command(limit=100, cursor=token, status="pending", memory_id=IDS[2]),
        _command(limit=100, cursor=token, status="applied", memory_id=IDS[3]),
        _command(limit=101, cursor=token, status="applied", memory_id=IDS[2]),
    ):
        with pytest.raises(HttpError, match="E_CURSOR_INVALID"):
            codec.validate(ctx.principal, wrong, now=AT)
    with pytest.raises(HttpError, match="E_CURSOR_EXPIRED"):
        codec.validate(
            ctx.principal, _command(cursor=token, status="applied", memory_id=IDS[2]), now=AT + timedelta(seconds=300)
        )
    with pytest.raises(HttpError, match="E_CURSOR_INVALID"):
        codec.validate(
            ctx.principal, _command(cursor=token + "x", status="applied", memory_id=IDS[2]), now=AT + timedelta(days=1)
        )

    # Sign malformed bodies correctly: rejection must not be a bad-MAC shortcut.
    raw = base64.urlsafe_b64decode(token.split(".")[0] + "==")
    extra = json.loads(raw)
    extra["extra"] = "forbidden"
    for malformed in (
        json.dumps(extra, sort_keys=True, separators=(",", ":")).encode(),
        json.dumps(json.loads(raw), sort_keys=True, indent=1).encode(),
    ):
        body = base64.urlsafe_b64encode(malformed).decode().rstrip("=")
        signature = base64.urlsafe_b64encode(hmac.digest(b"k" * 32, body.encode(), hashlib.sha256)).decode().rstrip("=")
        with pytest.raises(HttpError, match="E_CURSOR_INVALID"):
            codec.validate(
                ctx.principal, _command(cursor=body + "." + signature, status="applied", memory_id=IDS[2]), now=AT
            )

    path = tmp_path / "control" / "http-outbox-cursor.key"
    disk = routes.OutboxCursorCodec.from_path(path, ttl_seconds=900)
    assert path.stat().st_mode & 0o777 == 0o600 and len(path.read_bytes()) == 32
    assert (
        disk.validate(
            ctx.principal,
            _command(cursor=disk.issue(ctx, _command(), last=key1, upper=key2, limit=100, now=AT)),
            now=AT,
        ).upper
        == key2
    )
    with pytest.raises(ValueError):
        routes.OutboxCursorCodec.from_path(path, ttl_seconds=901)
    reopened = routes.OutboxCursorCodec.from_path(path, ttl_seconds=900)
    disk_token = disk.issue(ctx, _command(), last=key1, upper=key2, limit=100, now=AT)
    assert reopened.validate(ctx.principal, _command(cursor=disk_token), now=AT).last == key1
    path.chmod(0o644)
    with pytest.raises(HttpError, match="E_STORAGE"):
        routes.OutboxCursorCodec.from_path(path)


@pytest.mark.asyncio
async def test_work_window_statement_vm_sticky_cleanup_and_deadline(database):
    service, provider_module, _ = _foundation()
    now = asyncio.get_running_loop().time()
    async with database._session_factory() as session:
        work = provider_module.DiagnosticsReadWork(
            statements_max=1,
            soft_deadline=now + 10,
            hard_deadline=now + 10,
        )
        with pytest.raises(provider_module.DiagnosticsWorkExceeded):
            async with database.diagnostics_window(session, work=work):
                await session.execute(text("SELECT 1"))
                await session.execute(text("SELECT 2"))
        assert "s10_diagnostics_work" not in session.info
        assert work.stop == "statements"
        with pytest.raises(provider_module.DiagnosticsWorkExceeded):
            work.check()
        assert (await session.execute(text("SELECT 3"))).scalar_one() == 3

    async with database._session_factory() as session:
        work = provider_module.DiagnosticsReadWork(
            statement_vm_limit=2_000,
            operation_vm_limit=2_000,
            soft_deadline=now + 10,
            hard_deadline=now + 10,
        )
        with pytest.raises(provider_module.DiagnosticsWorkExceeded):
            async with database.diagnostics_window(session, work=work):
                await session.execute(
                    text(
                        "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL "
                        "SELECT x+1 FROM n WHERE x<1000000) SELECT sum(x) FROM n"
                    )
                )
        assert work.stop in {"statement_vm", "operation_vm"}
        with pytest.raises(provider_module.DiagnosticsWorkExceeded):
            work.check()
        assert "s10_diagnostics_work" not in session.info
        # The same connection must no longer have the SQLite progress hook.
        assert (
            await session.execute(
                text(
                    "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<10000) SELECT sum(x) FROM n"
                )
            )
        ).scalar_one() == sum(range(1, 10001))

    deadline = provider_module.DiagnosticsReadWork(soft_deadline=now - 1, hard_deadline=now - 1)
    with pytest.raises(provider_module.DiagnosticsDeadlineExceeded):
        deadline.check()
    assert deadline.stop == "timeout"
    from memory_server.transports.http.errors import HttpError

    observed = []

    async def overflow(session, work, sampled):
        observed.append(work)
        assert service._diagnostics_active.get() is True
        assert work.statements_max == 256
        for _ in range(257):
            await session.execute(text("SELECT 1"))

    assert service._diagnostics_active.get() is False
    with pytest.raises(HttpError, match="E_STORAGE"):
        await service.run_borrowed_diagnostics(_ctx(), _resources(database), overflow)
    assert len(observed) == 1
    assert (observed[0].statements, observed[0].stop) == (257, "statements")
    assert service._diagnostics_active.get() is False

    # Exercise exceptional finally in this task, not just the producer's context.
    async with database._session_factory() as session:
        work = provider_module.DiagnosticsReadWork()
        with pytest.raises(RuntimeError, match="cleanup sentinel"):
            async with service.diagnostics_log_scope(), database.diagnostics_window(session, work=work):
                assert service._diagnostics_active.get() is True
                await session.execute(text("SELECT 1"))
                raise RuntimeError("cleanup sentinel")
        assert service._diagnostics_active.get() is False
        assert "s10_diagnostics_work" not in session.info
        assert (await session.execute(text("SELECT 3"))).scalar_one() == 3


@pytest.mark.asyncio
async def test_lifecycle_audit_does_not_spend_statements_on_discarded_groups(database):
    service, _, _ = _foundation()
    from memory_server.gateway_contracts import dto as d

    statements = []

    def trace(*args):
        statements.append(args[2])

    resources = _resources(database)
    event.listen(database.engine.sync_engine, "before_cursor_execute", trace)
    try:
        result = await service.run_borrowed_diagnostics(
            _ctx(),
            resources,
            lambda session, work, sampled: service.DiagnosticsService(None, None)._audit(
                _ctx(), d.AuditQuery(audit_type="lifecycle", limit=200, cursor=None), resources, session, work, sampled
            ),
        )
    finally:
        event.remove(database.engine.sync_engine, "before_cursor_execute", trace)
    assert result.sections == {"lifecycle": "ok"}
    assert result.complete is True and result.findings.items == []
    assert statements and not any("GROUP BY" in sql.upper() for sql in statements)


@pytest.fixture
async def diagnostics(database):
    from contextlib import asynccontextmanager

    from memory_server.application.contracts import ReflectionPolicy
    from memory_server.application.diagnostics_service import DiagnosticsResources, DiagnosticsService
    from memory_server.application.memory_read_service import ReadResources
    from memory_server.gateway_contracts import dto as d

    profile = d.Profile(
        id="alpha",
        display_name="Alpha",
        description=None,
        enabled=True,
        generation=1,
        runtime_state="ready",
        capabilities=(),
        storage_root="/secret/root",
    )
    resources = DiagnosticsResources(
        canonical=ReadResources(database, database._session_factory),
        profile=profile,
        vector=None,
        graph=None,
        validator=None,
        confidence=None,
        reflection=SimpleNamespace(policy=ReflectionPolicy.capture()),
        writer_owned=True,
    )

    class Lease:
        acquired = 0
        snapshots = 0

        @asynccontextmanager
        async def acquire(self, _ctx):
            self.acquired += 1
            yield SimpleNamespace()

        def snapshot(self):
            self.snapshots += 1
            return {"alpha": SimpleNamespace(owned=True)}

    lease = Lease()
    service = DiagnosticsService(lease, lambda _scope: resources)
    service._test_resources = resources
    service._test_lease = lease
    yield service


@pytest.mark.asyncio
async def test_indexes_status_honest_unknown(diagnostics):
    from dataclasses import replace

    diagnostics.bind = lambda _scope: replace(diagnostics._test_resources, graph=object())
    result = await diagnostics.indexes_status(_ctx())
    assert result.sqlite_records == 0
    assert result.vector_records is None
    assert result.graph_relations is None and result.graph_lag is None
    assert result.vector_lag is None and result.last_success is None
    assert result.graph_consistency.projection == "unknown"
    assert diagnostics._test_lease.acquired == 1 and diagnostics._test_lease.snapshots == 1


@pytest.mark.asyncio
async def test_overview_profile_redacted_and_counts(diagnostics, database):
    from storage.models.belief import BeliefORM
    from storage.models.decision import DecisionORM
    from storage.models.fact import FactORM
    from storage.models.receipt import MemoryReceiptORM
    from storage.models.relation import ClaimRelationORM
    from storage.models.skill import SkillORM

    a, b = IDS[0], IDS[1]
    async with database._session_factory() as session, session.begin():
        session.add_all(
            [
                FactORM(
                    id=IDS[2],
                    subject="f",
                    predicate="p",
                    object="o",
                    dedup_key=IDS[2],
                    confidence=0.8,
                    lifecycle_state="active",
                    verification_status="validated",
                ),
                BeliefORM(
                    id=a,
                    proposition="a",
                    confidence=0.8,
                    source="test",
                    creator="test",
                    source_ids=[],
                    tags=[],
                    lifecycle_state="contradicted",
                    verification_status="validated",
                ),
                BeliefORM(
                    id=b,
                    proposition="b",
                    confidence=0.8,
                    source="test",
                    creator="test",
                    source_ids=[],
                    tags=[],
                    lifecycle_state="contradicted",
                    verification_status="validated",
                ),
                DecisionORM(
                    id=IDS[3],
                    context="",
                    choice="d",
                    rejected_alternatives="[]",
                    reason="test",
                    confidence=0.8,
                    lifecycle_state="active",
                    verification_status="validated",
                ),
                SkillORM(
                    id=IDS[4],
                    name="s",
                    purpose="test",
                    confidence=0.8,
                    lifecycle_state="active",
                    verification_status="validated",
                ),
                MemoryReceiptORM(
                    id=a,
                    memory_type="belief",
                    source="test",
                    created_by="test",
                    timestamp=AT.replace(tzinfo=None),
                    confidence=0.8,
                    verification_status="validated",
                    lifecycle_state="contradicted",
                ),
            ]
        )
        session.add_all(
            [
                ClaimRelationORM(source_id=a, target_id=b, relation_type="contradicts"),
                ClaimRelationORM(source_id=b, target_id=a, relation_type="contradicts"),
                ClaimRelationORM(source_id=a, target_id=a, relation_type="contradicts"),
                ClaimRelationORM(source_id=a, target_id=IDS[2], relation_type="contradicts"),
                ClaimRelationORM(source_id=a, target_id=b, relation_type="entails"),
            ]
        )
    result = await diagnostics.overview(_ctx())
    assert result.profile.storage_root is None
    assert (result.facts, result.beliefs, result.decisions, result.skills) == (1, 2, 1, 1)
    assert result.unresolved_conflicts == 1
    assert set(result.lifecycle_counts) == {
        "candidate",
        "validated",
        "active",
        "stale",
        "archived",
        "forgotten",
        "superseded",
        "contradicted",
        "discarded",
        "unknown",
    }
    assert result.lifecycle_counts == {
        "candidate": 0,
        "validated": 0,
        "active": 3,
        "stale": 0,
        "archived": 0,
        "forgotten": 0,
        "superseded": 0,
        "contradicted": 2,
        "discarded": 0,
        "unknown": 0,
    }
    assert result.indexes.sqlite_records == 5
    assert result.indexes.vector_records is None
    assert result.indexes.graph_relations is None
    assert result.indexes.vector_lag is None and result.indexes.graph_lag is None
    assert result.indexes.last_success is None
    assert result.audit_warnings is None
    assert result.last_migration is None
    assert result.schema_revision is None
    assert diagnostics._test_lease.acquired == 1 and diagnostics._test_lease.snapshots == 1


@pytest.mark.asyncio
async def test_overview_unresolved_conflicts_deduplicates_and_rejects_ambiguous_endpoints(diagnostics, database):
    from storage.models.belief import BeliefORM
    from storage.models.fact import FactORM
    from storage.models.relation import ClaimRelationORM

    async with database._session_factory() as session, session.begin():
        for i in range(4):
            session.add(
                BeliefORM(
                    id=IDS[i],
                    proposition="conflict",
                    confidence=0.8,
                    source="test",
                    creator="test",
                    source_ids=[],
                    tags=[],
                    lifecycle_state="contradicted",
                    verification_status="validated",
                )
            )
        # This untyped ID resolves in two canonical domains and must not form a pair.
        session.add(
            FactORM(
                id=IDS[2],
                subject="ambiguous",
                predicate="p",
                object="o",
                dedup_key=IDS[2],
                confidence=0.8,
                lifecycle_state="contradicted",
                verification_status="validated",
            )
        )
        for source, target in (
            (IDS[0], IDS[1]),
            (IDS[1], IDS[0]),
            (IDS[0], IDS[0]),
            (IDS[0], IDS[2]),
            (IDS[0], IDS[20]),
            (IDS[0], "invalid"),
        ):
            session.add(ClaimRelationORM(source_id=source, target_id=target, relation_type="contradicts"))
    result = await diagnostics.overview(_ctx())
    assert result.unresolved_conflicts == 1
    assert (result.facts, result.beliefs) == (1, 4)


@pytest.mark.asyncio
async def test_overview_schema_revision_states(diagnostics, database):
    async def read():
        return (await diagnostics.overview(_ctx())).schema_revision

    assert await read() is None
    async with database._session_factory() as session, session.begin():
        await session.execute(text("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)"))
    assert await read() is None
    async with database._session_factory() as session, session.begin():
        await session.execute(text("INSERT INTO alembic_version(version_num) VALUES ('one')"))
        await session.execute(text("INSERT INTO alembic_version(version_num) VALUES ('two')"))
    assert await read() is None
    async with database._session_factory() as session, session.begin():
        await session.execute(text("DELETE FROM alembic_version WHERE version_num='two'"))
    assert await read() == "one"


@pytest.mark.asyncio
async def test_overview_schema_revision_storage_failures_are_e_storage(diagnostics, database, monkeypatch):
    async def fail(_session, *, work):
        raise ValueError("unrelated schema SQL failure")

    monkeypatch.setattr(database, "diagnostics_schema_revision", fail)
    from memory_server.transports.http.errors import HttpError

    with pytest.raises(HttpError, match="E_STORAGE"):
        await diagnostics.overview(_ctx())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode", ["overview", "contradictions", "decay", "topics", "evidence_audit", "confidence", "relations"]
)
async def test_reflect_modes_and_masking(diagnostics, database, mode):
    from storage.models.belief import BeliefORM

    from memory_server.gateway_contracts import dto as d
    from memory_server.transports.http.routes.diagnostics import validate_reflect_result

    async with database._session_factory() as session, session.begin():
        session.add(
            BeliefORM(
                id=IDS[0],
                proposition="REFLECT_SECRET",
                confidence=0.8,
                source="secret-source",
                creator="test",
                source_ids=[],
                tags=["secret-topic"],
                lifecycle_state="active",
                verification_status="validated",
            )
        )
    request = d.ReflectRequest(
        mode=mode, topic=None, min_confidence=0.0, limit=1, context=None, strict_same_context=True
    )
    result = await diagnostics.reflect(_ctx(), request)
    assert result.mode == mode
    assert validate_reflect_result(result) == result
    common = {"sampled_at": result.sections["sampled_at"], "selected_count": 1}
    lifecycle = {
        key: 0
        for key in (
            "candidate",
            "validated",
            "active",
            "stale",
            "archived",
            "forgotten",
            "superseded",
            "contradicted",
            "discarded",
            "unknown",
        )
    }
    lifecycle["active"] = 1
    expected = {
        "overview": common
        | {
            "total_beliefs": 1,
            "lifecycle_counts": lifecycle,
            "confidence": {"high_0.8_1.0": 1, "medium_0.5_0.8": 0, "low_0.0_0.5": 0, "average": 0.8},
            "contradiction_count": 0,
            "stale_count": 0,
            "decaying_next_7d": 0,
            "no_evidence_count": 1,
            "evidence_stats_status": "ok",
        },
        "contradictions": common | {"total": 0, "pairs": []},
        "decay": common | {"stale_now": 0, "stale_7d": 0, "archived_7d": 0, "forgotten_7d": 0},
        "topics": common | {"topic_count": 1, "untagged_count": 0},
        "evidence_audit": common
        | {
            "total": 1,
            "with_evidence": 0,
            "without_evidence": 1,
            "avg_evidence_per_belief": 0,
            "zero_weight_entries": 0,
            "evidence_stats_status": "ok",
        },
        "confidence": common
        | {
            "histogram": {"0.9_1.0": 0, "0.7_0.9": 1, "0.5_0.7": 0, "0.3_0.5": 0, "0.0_0.3": 0},
            "lowest_count": 1,
            "beliefs": [{"id": IDS[0], "confidence": 0.8, "lifecycle_state": "active"}],
        },
        "relations": common | {"total": 0, "contradictions": 0, "entailments": 0, "neutral_pairs": 0, "pairs": []},
    }[mode]
    assert result.sections == expected
    assert "REFLECT_SECRET" not in result.model_dump_json()
    assert "secret-source" not in result.model_dump_json()
    assert "secret-topic" not in result.model_dump_json()


@pytest.mark.asyncio
async def test_reflect_borrowed_evidence_and_limits(database):
    service, _, _ = _foundation()
    from storage.models.belief import BeliefORM

    from memory_server.application.contracts import ReflectionPolicy
    from memory_server.application.diagnostics_service import run_borrowed_diagnostics
    from memory_server.gateway_contracts import dto as d

    ident = IDS[0]
    async with database._session_factory() as session, session.begin():
        session.add(
            BeliefORM(
                id=ident,
                proposition="Docker is better",
                confidence=0.8,
                source="test",
                creator="test",
                source_ids=[],
                tags=["masked-topic"],
                lifecycle_state="active",
                verification_status="validated",
            )
        )
    resources = _resources(database)
    resources.reflection = SimpleNamespace(policy=ReflectionPolicy.capture())
    resources.profile = d.Profile(
        id="alpha",
        display_name="Alpha",
        description=None,
        enabled=True,
        generation=1,
        runtime_state="ready",
        capabilities=(),
        storage_root=None,
    )
    resources.writer_owned = True
    request = d.ReflectRequest(
        mode="evidence_audit", topic="masked-topic", min_confidence=0.0, limit=1, context=None, strict_same_context=True
    )
    result = await run_borrowed_diagnostics(
        _ctx(),
        resources,
        lambda session, work, sampled: service.DiagnosticsService(None, None)._operation(
            _ctx(),
            d.COMMAND_TYPES["reflect"](operation="reflect", profile_id="alpha", path_ids={}, payload=request),
            resources,
            session,
            work,
            sampled,
        ),
    )
    assert result.sections["selected_count"] == 1
    assert result.sections["evidence_stats_status"] == "ok"
    assert "masked-topic" not in result.model_dump_json()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode", ["overview", "contradictions", "decay", "topics", "evidence_audit", "confidence", "relations"]
)
async def test_b2_real_reflection_single_snapshot(diagnostics, database, mode):
    from storage.models.belief import BeliefORM

    from memory_server.gateway_contracts import dto as d
    from memory_server.transports.http.routes.diagnostics import validate_reflect_result

    async with database._session_factory() as session, session.begin():
        for i in range(3):
            session.add(
                BeliefORM(
                    id=IDS[i],
                    proposition="B2_PROPOSITION_CANARY",
                    confidence=0.8,
                    source="B2_SOURCE_CANARY",
                    creator="test",
                    source_ids=[],
                    tags=["B2_TAG_CANARY"],
                    lifecycle_state="active",
                    verification_status="validated",
                )
            )
    statements = []

    def trace(_conn, _cursor, sql, _params, _context, _many):
        if sql.lstrip().upper().startswith("SELECT"):
            statements.append(sql)

    event.listen(database.engine.sync_engine, "before_cursor_execute", trace)
    try:
        request = d.ReflectRequest(
            mode=mode,
            topic="B2_TAG_CANARY",
            min_confidence=0.0,
            context="B2_CONTEXT_CANARY",
            strict_same_context=False,
            limit=2,
        )
        result = await diagnostics.reflect(_ctx(), request)
        assert result.sections["selected_count"] == 2
        assert not result.complete
        assert validate_reflect_result(result) == result
        assert len([sql for sql in statements if "FROM beliefs" in sql]) == 1
        assert all(
            secret not in result.model_dump_json()
            for secret in ("B2_PROPOSITION_CANARY", "B2_TAG_CANARY", "B2_CONTEXT_CANARY", "B2_SOURCE_CANARY")
        )
    finally:
        event.remove(database.engine.sync_engine, "before_cursor_execute", trace)


@pytest.mark.parametrize("nested", [True, "CANARY", {"arbitrary": 1}, [1]])
def test_b2_reflect_nested_numeric_closed(nested):
    service, _, _ = _foundation()
    from memory_server.gateway_contracts import dto as d
    from memory_server.transports.http.errors import HttpError
    from memory_server.transports.http.routes.diagnostics import validate_reflect_result

    result = d.ReflectResult(
        mode="confidence",
        request_id="r",
        complete=True,
        sections={
            "sampled_at": AT.isoformat(),
            "selected_count": 1,
            "lowest_count": 0,
            "beliefs": [],
            "histogram": {"0.9_1.0": nested, "0.7_0.9": 0, "0.5_0.7": 0, "0.3_0.5": 0, "0.0_0.3": 0},
        },
    )
    with pytest.raises(HttpError, match="E_INTERNAL"):
        validate_reflect_result(result)


@pytest.mark.asyncio
async def test_b2_indexes_uses_exact_status_counts(database):
    service, _, _ = _foundation()
    await _seed_outbox(database, count=8)
    statements = []

    def trace(_conn, _cursor, sql, _params, _context, _many):
        if sql.lstrip().upper().startswith("SELECT"):
            statements.append(sql)

    event.listen(database._engine.sync_engine, "before_cursor_execute", trace)
    resources = _resources(database)
    resources.vector, resources.graph = None, None
    try:
        result = await service.run_borrowed_diagnostics(
            _ctx(),
            resources,
            lambda session, work, sampled: service.DiagnosticsService(None, None)._indexes(
                resources, session, work, sampled
            ),
        )
        assert result.outbox_pending == result.outbox_failed == 2
        assert len(statements) == 6
        assert all("GROUP BY" not in sql.upper() for sql in statements)
    finally:
        event.remove(database._engine.sync_engine, "before_cursor_execute", trace)


@pytest.mark.asyncio
async def test_b2_real_lease_native_and_overview(shell, monkeypatch):
    service, _, _ = _foundation()
    from memory_server.gateway_contracts.ports import RequestContext
    from memory_server.providers.embedding_provider import MockEmbeddingProvider
    from memory_server.providers.lancedb_provider import LanceDBProvider
    from memory_server.runtime import runtime_factory
    from memory_server.runtime.read_composition import bind_native
    from memory_server.transports.http.routes.diagnostics import validate_index_status, validate_overview

    monkeypatch.setitem(runtime_factory._DEFAULT_RECIPES, "embedder", lambda runtime: MockEmbeddingProvider())
    vector = LanceDBProvider(db_path=str(shell.bindings[0].layout.data_root / "lancedb"))
    await vector.create_collection("memories")
    await vector.close()
    principal = await shell.tokens.authenticate(shell.bearer, now=datetime.now(UTC))
    ctx = RequestContext("b2-native", principal, "alpha", None, asyncio.get_running_loop().time() + 30)

    def bind(scope):
        return service.DiagnosticsResources(
            scope.application.read_resources(),
            scope.binding.profile,
            scope.runtime.dependencies.vector,
            scope.runtime.dependencies.graph,
            None,
            None,
            scope.application.reflection,
            None,
        )

    obj = service.DiagnosticsService(shell.leases, bind, native=bind_native)
    status = await obj.indexes_status(ctx)
    assert validate_index_status(status) == status and status.vector_records == 0
    overview = await obj.overview(ctx)
    assert validate_overview(overview) == overview
    assert overview.profile.storage_root is None and overview.writer_owned is True
    assert overview.indexes.sampled_at.tzinfo is not None
    assert overview.indexes.sqlite_records == sum(
        (overview.facts, overview.beliefs, overview.decisions, overview.skills)
    )


@pytest.mark.asyncio
async def test_b2_topic_minimum_and_bounded_evidence(database):
    service, _, _ = _foundation()
    from dataclasses import replace

    from storage.models.belief import BeliefORM, EvidenceORM

    from memory_server.application.contracts import ReflectionPolicy
    from memory_server.gateway_contracts import dto as d
    from memory_server.transports.http.errors import HttpError

    async with database._session_factory() as session, session.begin():
        for i in range(102):
            session.add(
                BeliefORM(
                    id=IDS[i],
                    proposition="B2_PROPOSITION_CANARY",
                    confidence=(0.0 if i == 0 else 1.0),
                    source="test",
                    creator="test",
                    source_ids=[],
                    tags=[] if i == 0 else ["B2_TOPIC_CANARY"],
                    lifecycle_state="active",
                    verification_status="validated",
                )
            )
        await session.flush()
        for i in range(30):
            session.add(
                EvidenceORM(
                    id=IDS[i], belief_id=IDS[1], source_type=f"B2_SOURCE_CANARY_{i}", source_id=IDS[i], weight=0.0
                )
            )
    resources = _resources(database)
    resources.reflection = SimpleNamespace(policy=replace(ReflectionPolicy.capture(), reflect_min_confidence=1.0))

    async def run(request, inspect=None):
        async def operation(session, work, sampled):
            adapter = await service.build_borrowed_reflection(resources, session, request, work)
            if inspect is not None:
                await inspect(adapter)
            return adapter

        return await service.run_borrowed_diagnostics(_ctx(), resources, operation)

    for minimum, expected in ((None, IDS[1]), (0.0, IDS[0]), (1.0, IDS[1])):
        adapter = await run(d.ReflectRequest(topic=None, min_confidence=minimum, context=None, limit=1))
        assert [belief.id for belief in adapter.beliefs] == [expected]
    adapter = await run(d.ReflectRequest(topic="B2_TOPIC_CANARY", min_confidence=0.0, context=None, limit=1))
    assert adapter.beliefs[0].id == IDS[1]
    adapter = await run(d.ReflectRequest(topic=None, min_confidence=0.0, context=None, limit=100))
    assert len(adapter.beliefs) == 100 and adapter.window.truncated

    async def inspect(adapter):
        from storage.repositories.evidence_repo import EvidenceRepository

        stats = await EvidenceRepository(adapter).aggregate_stats([IDS[1]])
        assert stats[IDS[1]]["by_source_type"] == {"[MASKED]": 30}
        assert stats[IDS[1]]["count"] == 30
        assert await adapter.search_beliefs(limit=9999) == adapter.beliefs
        with pytest.raises(HttpError, match="E_INTERNAL"):
            await adapter.execute(text("DELETE FROM evidence"), {"bid_0": IDS[1]})
        assert adapter.evidence_error is not None

    await run(d.ReflectRequest(topic="B2_TOPIC_CANARY", min_confidence=0.0, context=None, limit=1), inspect)


@pytest.mark.parametrize("limit", [0, 101, True, False])
def test_b2_reflect_parser_rejects_limits(limit):
    service, _, _ = _foundation()
    from starlette.requests import Request

    from memory_server.transports.http.errors import HttpError
    from memory_server.transports.http.routes.diagnostics import parse_reflect

    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/reflect",
            "path_params": {"profile": "alpha"},
            "query_string": b"",
            "headers": [(b"content-type", b"application/json")],
        }
    )
    with pytest.raises(HttpError, match="E_VALIDATION"):
        parse_reflect(
            request,
            json.dumps({"topic": None, "min_confidence": None, "context": None, "limit": limit}).encode(),
            _principal(),
        )


def test_b2_reflect_pair_cap_drop_timestamp_and_numeric():
    service, _, _ = _foundation()
    from memory_server.gateway_contracts import dto as d
    from memory_server.transports.http.errors import HttpError

    pair = {
        "belief_a_id": IDS[0],
        "belief_b_id": IDS[1],
        "confidence_a": 0.8,
        "confidence_b": 0.8,
        "overlap_score": 0.8,
        "detection_score": 0.8,
        "detected_at": "invalid",
        "proposition": "B2_TEXT_CANARY",
    }
    request = d.ReflectRequest(mode="contradictions", topic=None, min_confidence=0.0, context=None, limit=1)

    def mapped(rows):
        return service.map_reflect_result(
            SimpleNamespace(request_id="r"),
            request,
            {"total": len(rows), "contradictions": rows, "_selected_ids": IDS[:2]},
            sampled_at=AT,
            selected_count=2,
            truncated=False,
            evidence_unavailable=False,
        )

    for rows in ([pair], [pair, pair], [dict(pair, belief_a_id="B2_TEXT_CANARY"), pair]):
        result = mapped(rows)
        assert not result.complete and len(result.sections["pairs"]) == 1
        assert result.sections["pairs"][0]["detected_at"] is None
        assert "B2_TEXT_CANARY" not in result.model_dump_json()
    with pytest.raises(HttpError, match="E_INTERNAL"):
        mapped([dict(pair, confidence_a=True)])


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["overview", "evidence_audit", "confidence"])
async def test_b2_swallowed_evidence_storage_failure_is_sticky(database, mode):
    service, _, _ = _foundation()
    from storage.models.belief import BeliefORM

    from memory_server.application.contracts import ReflectionPolicy
    from memory_server.gateway_contracts import dto as d
    from memory_server.transports.http.errors import HttpError

    async with database._session_factory() as session, session.begin():
        session.add(
            BeliefORM(
                id=IDS[0],
                proposition="CANARY",
                confidence=0.8,
                source="test",
                creator="test",
                source_ids=[],
                tags=[],
                lifecycle_state="active",
                verification_status="validated",
            )
        )
        await session.execute(text("DROP TABLE evidence"))
    resources = _resources(database)
    resources.reflection = SimpleNamespace(policy=ReflectionPolicy.capture())
    command = d.COMMAND_TYPES["reflect"](
        operation="reflect",
        profile_id="alpha",
        path_ids={},
        payload=d.ReflectRequest(mode=mode, topic=None, min_confidence=0.0, context=None),
    )
    with pytest.raises(HttpError, match="E_STORAGE"):
        await service.run_borrowed_diagnostics(
            _ctx(),
            resources,
            lambda session, work, sampled: service.DiagnosticsService(None, None)._operation(
                _ctx(), command, resources, session, work, sampled
            ),
        )


@pytest.mark.asyncio
async def test_b2_reflect_absent_capability_is_503(diagnostics):
    from dataclasses import replace

    from memory_server.gateway_contracts import dto as d
    from memory_server.transports.http.errors import HttpError

    diagnostics.bind = lambda _scope: replace(diagnostics._test_resources, reflection=None)
    with pytest.raises(HttpError, match="E_CAPABILITY_UNAVAILABLE"):
        await diagnostics.reflect(
            _ctx(), d.ReflectRequest(mode="overview", topic=None, min_confidence=0.0, limit=1, context=None)
        )


@pytest.mark.asyncio
async def test_b2_execute_validates_malformed_reflect_commands(diagnostics):
    from memory_server.gateway_contracts import dto as d
    from memory_server.transports.http.errors import HttpError

    commands = (
        d.COMMAND_TYPES["reflect"].model_construct(
            operation="reflect",
            profile_id="alpha",
            path_ids={},
            payload={
                "mode": "invalid",
                "topic": None,
                "min_confidence": 0.0,
                "limit": 1,
                "context": None,
                "strict_same_context": True,
            },
        ),
        d.COMMAND_TYPES["reflect"].model_construct(
            operation="reflect",
            profile_id="alpha",
            path_ids={},
            payload={
                "mode": "overview",
                "topic": None,
                "min_confidence": 0.0,
                "limit": 1,
                "context": None,
                "strict_same_context": True,
                "extra": "forbidden",
            },
        ),
    )
    for command in commands:
        with pytest.raises(HttpError, match="E_VALIDATION"):
            await diagnostics.execute(_ctx(), command)


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [1, 100])
async def test_b2_reflect_public_limits(diagnostics, database, limit):
    from storage.models.belief import BeliefORM

    from memory_server.gateway_contracts import dto as d

    async with database._session_factory() as session, session.begin():
        for ident in IDS[:101]:
            session.add(
                BeliefORM(
                    id=ident,
                    proposition="limit",
                    confidence=0.8,
                    source="test",
                    creator="test",
                    source_ids=[],
                    tags=[],
                    lifecycle_state="active",
                    verification_status="validated",
                )
            )
    result = await diagnostics.reflect(
        _ctx(), d.ReflectRequest(mode="overview", topic=None, min_confidence=0.0, limit=limit, context=None)
    )
    assert result.sections["selected_count"] == limit
    assert result.sections["total_beliefs"] == limit
    assert result.complete is False


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["overview", "indexes.status", "reflect"])
async def test_b2_base_public_execute_regression_probes(diagnostics, operation):
    from dataclasses import replace

    from memory_server.gateway_contracts import dto as d
    from memory_server.transports.http.errors import HttpError

    diagnostics.bind = lambda _scope: replace(diagnostics._test_resources, vector=object())
    payload = (
        d.ReflectRequest(mode="topics", topic=None, min_confidence=0.0, limit=1, context=None)
        if operation == "reflect"
        else d.Empty()
    )
    command = d.COMMAND_TYPES[operation](operation=operation, profile_id="alpha", path_ids={}, payload=payload)
    try:
        result = await diagnostics.execute(_ctx(), command)
        observed = result.sections["topic_count"] if operation == "reflect" else result.model_dump()
    except HttpError as error:
        observed = error.code
    assert observed == (0 if operation == "reflect" else "E_VECTOR_BACKEND")


@pytest.mark.asyncio
async def test_b2_reflect_empty_selection_does_not_scan_evidence(diagnostics, database):
    from storage.models.belief import BeliefORM, EvidenceORM

    from memory_server.gateway_contracts import dto as d

    statements = []

    async with database._session_factory() as session, session.begin():
        session.add(
            BeliefORM(
                id=IDS[10],
                proposition="NONSELECTED",
                confidence=0.8,
                source="test",
                creator="test",
                source_ids=[],
                tags=["present"],
                lifecycle_state="active",
                verification_status="validated",
            )
        )
        await session.flush()
        session.add(
            EvidenceORM(id=IDS[10], belief_id=IDS[10], source_type="nonselected", source_id=IDS[10], weight=1.0)
        )

    def trace(_conn, _cursor, sql, _params, _context, _many):
        if sql.lstrip().upper().startswith("SELECT"):
            statements.append(sql)

    event.listen(database.engine.sync_engine, "before_cursor_execute", trace)
    try:
        result = await diagnostics.reflect(
            _ctx(), d.ReflectRequest(mode="evidence_audit", topic="absent", min_confidence=0.0, limit=1, context=None)
        )
    finally:
        event.remove(database.engine.sync_engine, "before_cursor_execute", trace)
    assert result.sections["selected_count"] == 0
    assert statements
    assert not any("FROM EVIDENCE" in sql.upper() for sql in statements)


@pytest.mark.asyncio
@pytest.mark.parametrize("vector", [object(), SimpleNamespace(count_points=object())])
async def test_b2_vector_present_without_callable_is_backend_error(diagnostics, vector):
    from memory_server.transports.http.errors import HttpError

    diagnostics.bind = lambda scope: diagnostics._test_resources
    # Rebind the borrowed resource through the fixture's lease closure.
    original = diagnostics.bind
    resources = original(None)
    diagnostics.bind = lambda _scope: resources.__class__(
        canonical=resources.canonical,
        profile=resources.profile,
        vector=vector,
        graph=resources.graph,
        validator=resources.validator,
        confidence=resources.confidence,
        reflection=resources.reflection,
        writer_owned=resources.writer_owned,
    )
    with pytest.raises(HttpError, match="E_VECTOR_BACKEND"):
        await diagnostics.indexes_status(_ctx())


@pytest.mark.asyncio
async def test_b2_vector_throw_is_backend_error_and_absent_is_unknown(diagnostics, caplog):
    from memory_server.transports.http.errors import HttpError

    resources = diagnostics.bind(None)

    class Broken:
        async def count_points(self):
            try:
                raise RuntimeError("VECTOR_LOG_CANARY")
            except RuntimeError:
                logging.getLogger("memory_server.providers.lancedb_provider").exception("VECTOR_LOG_CANARY")
                raise

    diagnostics.bind = lambda _scope: resources.__class__(
        canonical=resources.canonical,
        profile=resources.profile,
        vector=Broken(),
        graph=resources.graph,
        validator=resources.validator,
        confidence=resources.confidence,
        reflection=resources.reflection,
        writer_owned=resources.writer_owned,
    )
    with caplog.at_level(logging.ERROR, logger="memory_server.providers.lancedb_provider"):
        with pytest.raises(HttpError, match="E_VECTOR_BACKEND"):
            await diagnostics.indexes_status(_ctx())
    assert "VECTOR_LOG_CANARY" not in caplog.text
    diagnostics.bind = lambda _scope: resources
    assert (await diagnostics.indexes_status(_ctx())).vector_records is None


@pytest.mark.parametrize("change", ["topic", "min_confidence", "context", "mode", "media"])
def test_b2_reflect_required_fields_and_media(change):
    service, _, _ = _foundation()
    from starlette.requests import Request

    from memory_server.transports.http.errors import HttpError
    from memory_server.transports.http.routes.diagnostics import parse_reflect

    values = {"topic": None, "min_confidence": None, "context": None}
    if change in values:
        del values[change]
    elif change == "mode":
        values[change] = "invalid"
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/reflect",
            "path_params": {"profile": "alpha"},
            "query_string": b"",
            "headers": [(b"content-type", b"text/plain" if change == "media" else b"application/json")],
        }
    )
    with pytest.raises(HttpError, match="E_UNSUPPORTED_MEDIA_TYPE" if change == "media" else "E_VALIDATION"):
        parse_reflect(request, json.dumps(values).encode(), _principal())
