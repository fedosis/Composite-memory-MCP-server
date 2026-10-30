"""S10A storage, borrowed-session, cursor, and log-scope foundation tests."""

from __future__ import annotations

import asyncio
import importlib
import json
import logging
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import event, text
from storage.models.belief import BeliefORM, EvidenceORM
from storage.outbox import OutboxEntryORM, OutboxRepository

from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import Principal, RequestContext
from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.transports.http.errors import HttpError
from tests.gateway.test_http_retrieval import http_request, live_server, read_scope

IDS = tuple(f"00000000-0000-4000-8000-{number:012d}" for number in range(1, 1201))
AT = datetime(2026, 10, 30, 12, 0, tzinfo=UTC)


def _module(name: str):
    try:
        return importlib.import_module(name)
    except ModuleNotFoundError as error:
        assert error.name == name
        return None


def _foundation():
    service = _module("memory_server.application.diagnostics_service")
    routes = _module("memory_server.transports.http.routes.diagnostics")
    provider = importlib.import_module("memory_server.providers.sqlite_provider")
    outbox = importlib.import_module("storage.outbox")
    assert service is not None, "S10A diagnostics service foundation is absent"
    assert routes is not None, "S10A diagnostics cursor foundation is absent"
    required_provider = {
        "DiagnosticsReadWork",
        "DiagnosticsWorkExceeded",
        "DiagnosticsDeadlineExceeded",
        "AuditSnapshot",
        "BeliefWindow",
    }
    assert required_provider <= set(vars(provider)), required_provider - set(vars(provider))
    required_outbox = {
        "OutboxKey",
        "OutboxDiagnosticRow",
        "OutboxDiagnosticPage",
        "classify_diagnostic_error",
    }
    assert required_outbox <= set(vars(outbox)), required_outbox - set(vars(outbox))
    return service, routes, provider, outbox


@pytest.fixture
async def database(tmp_path):
    provider = SQLiteProvider("sqlite+aiosqlite:///" + str(tmp_path / "diagnostics.sqlite"))
    await provider.initialize()
    try:
        yield provider
    finally:
        await provider.close()


def _principal(subject: str = "diagnostic-reader") -> Principal:
    return Principal(
        token_id="00000000-0000-4000-8000-000000009999",
        subject=subject,
        client_id=None,
        profiles=frozenset({"alpha"}),
        permissions=frozenset({"audit.read", "profile.read"}),
        credential_generation=1,
        expires_at=AT + timedelta(days=1),
    )


def _command(*, limit=100, cursor=None, status=None, memory_id=None):
    return d.COMMAND_TYPES["outbox.list"](
        operation="outbox.list",
        profile_id="alpha",
        path_ids={},
        payload=d.OutboxQuery(limit=limit, cursor=cursor, status=status, memory_id=memory_id),
    )


async def _seed_outbox(provider, count=1000):
    async with provider._session_factory() as session:
        async with session.begin():
            for index in range(count):
                session.add(
                    OutboxEntryORM(
                        id=IDS[index],
                        record_id=IDS[index % 17],
                        record_type=("fact", "belief", "decision", "skill")[index % 4],
                        operation=("index_fact", "index_belief", "index_decision", "index_skill")[index % 4],
                        payload_json=json.dumps({"canary": "MUST_NOT_BE_SELECTED"}),
                        status=("pending", "processing", "completed", "failed")[index % 4],
                        retry_count=index % 3,
                        error="E_TIMEOUT" if index % 13 == 0 else "raw secret exception",
                        created_at=AT.replace(tzinfo=None),
                        processed_at=AT.replace(tzinfo=None) if index % 4 in {2, 3} else None,
                    )
                )


async def _diagnostic_auth(holder, *, permissions=("profile.read", "audit.read"), profiles=("alpha", "beta")):
    now = datetime.now(UTC)
    created = holder.app.dependencies.tokens._bootstrap_create(
        d.TokenCreate(
            subject="s10-reader", profiles=list(profiles), permissions=list(permissions), expires_at=now + timedelta(hours=1)
        ),
        now=now,
    )
    return {"Authorization": f"Bearer cmms_{created.token.id}.{created.secret}"}


async def _seed_runtime_diagnostics(holder, auth, *, profile="alpha"):
    async with await read_scope(holder, auth, profile) as scope:
        resources = scope.application.read_resources()
        async with resources.session_factory() as session, session.begin():
            session.add(
                BeliefORM(
                    id=IDS[0], proposition="Docker containers are better", confidence=0.8, source="source",
                    creator="creator", source_ids=[], tags=["s10-topic"], created_at=AT.replace(tzinfo=None),
                    updated_at=AT.replace(tzinfo=None), last_reinforced_at=AT.replace(tzinfo=None), version=1,
                    verification_status="candidate", lifecycle_state="active",
                )
            )
            session.add(
                BeliefORM(
                    id=IDS[3], proposition="Docker containers are worse", confidence=0.7, source="source",
                    creator="creator", source_ids=[], tags=["s10-topic"], created_at=AT.replace(tzinfo=None),
                    updated_at=AT.replace(tzinfo=None), last_reinforced_at=AT.replace(tzinfo=None), version=1,
                    verification_status="candidate", lifecycle_state="active",
                )
            )
            session.add(
                EvidenceORM(
                    id="evidence-s10", belief_id=IDS[0], source_type="S10_SOURCE_CANARY", source_id="secret",
                    weight=0.7, contributor="creator", created_at=AT.replace(tzinfo=None), note="S10_NOTE_CANARY",
                )
            )
            for offset, status in enumerate(("pending", "processing"), 1):
                session.add(
                    OutboxEntryORM(
                        id=IDS[offset], record_id=IDS[0], record_type="belief", operation="index_belief",
                        payload_json='{"canary":"S10_OUTBOX_CANARY"}', status=status, retry_count=0,
                        error=None, created_at=AT.replace(tzinfo=None), processed_at=None,
                    )
                )


async def test_s10_five_routes_mount_and_permissions(tmp_path):
    """Exercise production composition, runtime leases, borrowed SQL, and reflection over TCP."""
    async with live_server(tmp_path) as (holder, port, _):
        auth = await _diagnostic_auth(holder)
        await _seed_runtime_diagnostics(holder, auth)
        calls = (
            ("GET", "audit", b""),
            ("GET", "indexes/status", b""),
            ("GET", "outbox", b""),
            ("GET", "overview", b""),
            (
                "POST",
                "reflect",
                json.dumps({"topic": None, "min_confidence": None, "context": None}).encode(),
            ),
        )
        results = {}
        for method, suffix, body in calls:
            headers = auth | ({"Content-Type": "application/json"} if method == "POST" else {})
            status, result = await http_request(
                port, method, f"/api/v1/profiles/alpha/{suffix}", body=body, headers=headers
            )
            assert status == 200, (suffix, result)
            results[suffix] = result
        assert results["indexes/status"]["outbox_pending"] == 1
        assert results["overview"]["writer_owned"] is True
        assert results["reflect"]["sections"]["evidence_stats_status"] == "ok"
        assert results["reflect"]["sections"]["no_evidence_count"] == 1
        assert "Docker containers are better" not in json.dumps(results)
        assert "Docker containers are worse" not in json.dumps(results)
        assert "S10_SOURCE_CANARY" not in json.dumps(results)
        profile_only = await _diagnostic_auth(holder, permissions=("profile.read",))
        audit_only = await _diagnostic_auth(holder, permissions=("audit.read",))
        for suffix in ("indexes/status", "overview"):
            assert (await http_request(port, "GET", f"/api/v1/profiles/alpha/{suffix}", headers=profile_only))[0] == 200
            assert (await http_request(port, "GET", f"/api/v1/profiles/alpha/{suffix}", headers=audit_only))[0] == 403
        for suffix in ("audit", "outbox"):
            assert (await http_request(port, "GET", f"/api/v1/profiles/alpha/{suffix}", headers=audit_only))[0] == 200
            assert (await http_request(port, "GET", f"/api/v1/profiles/alpha/{suffix}", headers=profile_only))[0] == 403


async def test_reflect_borrowed_evidence_and_limits(tmp_path):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await _diagnostic_auth(holder)
        await _seed_runtime_diagnostics(holder, auth)
        payload = json.dumps({"topic": None, "min_confidence": None, "context": None}).encode()
        status, overview = await http_request(
            port,
            "POST",
            "/api/v1/profiles/alpha/reflect",
            body=payload,
            headers=auth | {"Content-Type": "application/json"},
        )
        assert status == 200, overview
        assert overview["sections"]["evidence_stats_status"] == "ok"
        assert overview["sections"]["no_evidence_count"] == 1
        contradictions = json.dumps(
            {"mode": "contradictions", "topic": None, "min_confidence": 0.0, "context": None}
        ).encode()
        status, result = await http_request(
            port,
            "POST",
            "/api/v1/profiles/alpha/reflect",
            body=contradictions,
            headers=auth | {"Content-Type": "application/json"},
        )
        assert status == 200, result
        assert result["sections"]["total"] == 1
        assert len(result["sections"]["pairs"]) == 1
        assert set(result["sections"]["pairs"][0]) == {
            "belief_a_id", "belief_b_id", "confidence_a", "confidence_b", "overlap_score",
            "detection_score", "detected_at",
        }
        assert "Docker containers are better" not in json.dumps(result)
        assert "Docker containers are worse" not in json.dumps(result)


async def test_outbox_keyset_ties_filters_and_raw_projection(database):
    _, _, provider_module, outbox_module = _foundation()
    await _seed_outbox(database)
    statements = []

    def trace(_conn, _cursor, statement, _params, _context, _many):
        statements.append(statement)

    event.listen(database.engine.sync_engine, "before_cursor_execute", trace)
    try:
        async with database._session_factory() as session:
            work = provider_module.DiagnosticsReadWork(
                soft_deadline=asyncio.get_running_loop().time() + 10,
                hard_deadline=asyncio.get_running_loop().time() + 10,
            )
            async with database.diagnostics_window(session, work=work):
                await session.execute(text("BEGIN"))
                repo = OutboxRepository(session)
                seen = []
                upper = last = None
                while True:
                    page = await repo.diagnostic_page(
                        limit=37,
                        last=last,
                        upper=upper,
                        status=None,
                        record_id=None,
                        work=work,
                    )
                    upper = page.upper
                    rows = page.rows[:37]
                    assert all(isinstance(row.created_at, datetime) for row in rows)
                    assert all(row.created_at == AT for row in rows)
                    assert all(row.processed_at is None or isinstance(row.processed_at, datetime) for row in rows)
                    seen.extend(row.id for row in rows)
                    if len(page.rows) <= 37:
                        break
                    last = outbox_module.OutboxKey(rows[-1].created_at.isoformat(timespec="microseconds"), rows[-1].id)
                assert seen == sorted(IDS[:1000])
                assert len(seen) == len(set(seen)) == 1000
                filtered = await repo.diagnostic_page(
                    limit=200,
                    last=None,
                    upper=None,
                    status="applied",
                    record_id=IDS[2],
                    work=work,
                )
                assert filtered.rows and all(
                    row.status == "completed" and row.record_id == IDS[2] for row in filtered.rows
                )
    finally:
        event.remove(database.engine.sync_engine, "before_cursor_execute", trace)
    page_selects = [
        sql.lower()
        for sql in statements
        if "outbox_entries" in sql.lower() and sql.lstrip().lower().startswith("select")
    ]
    assert page_selects
    assert all("payload_json" not in sql and " limit " in " " + sql.replace("\n", " ") + " " for sql in page_selects)
    assert any("status" in sql and "record_id" in sql for sql in page_selects)


async def test_outbox_counts_and_closed_error_classifier(database):
    _, _, provider_module, outbox_module = _foundation()
    await _seed_outbox(database, 40)
    async with database._session_factory() as session:
        work = provider_module.DiagnosticsReadWork(
            soft_deadline=asyncio.get_running_loop().time() + 10,
            hard_deadline=asyncio.get_running_loop().time() + 10,
        )
        async with database.diagnostics_window(session, work=work):
            repo = OutboxRepository(session)
            assert await repo.diagnostic_count(status="completed", work=work) == 10
            assert await repo.diagnostic_status_counts(work=work) == {
                "pending": 10,
                "processing": 10,
                "completed": 10,
                "failed": 10,
            }
    allowed = {
        "E_TIMEOUT",
        "E_STORAGE",
        "E_VECTOR_BACKEND",
        "E_GRAPH_BACKEND",
        "E_PROJECTION_UNAVAILABLE",
        "E_CAPABILITY_UNAVAILABLE",
    }
    assert {outbox_module.classify_diagnostic_error(value) for value in allowed} == allowed
    for value in (None, "", " E_TIMEOUT", "E_TIMEOUT ", "E_TIMEOUT: secret", "TimeoutError", ValueError("E_TIMEOUT")):
        assert outbox_module.classify_diagnostic_error(value) is None


def test_outbox_cursor_golden_binding_tamper_expiry_and_from_path(tmp_path):
    _, routes, _, outbox = _foundation()
    codec = routes.OutboxCursorCodec(b"k" * 32, ttl_seconds=300, clock=lambda: AT)
    ctx = RequestContext("request", _principal(), "alpha", None, 1.0)
    key1 = outbox.OutboxKey("2026-10-30T12:00:00.000000+00:00", IDS[0])
    key2 = outbox.OutboxKey("2026-10-30T12:00:00.000000+00:00", IDS[999])
    command = _command(limit=100, status="applied", memory_id=IDS[2])
    token = codec.issue(ctx, command, last=key1, upper=key2, limit=100, now=AT)
    assert token == (
        "eyJleHBpcmVzX2F0IjoiMjAyNi0xMC0zMFQxMjowNTowMCswMDowMCIsImZpbHRlcl9zaGEyNTYiOiIyNGMyMzNiNTQwNDdmNmEx"
        "YmZiZjZmZTc1MzU3YzhmNDQ0ZjJmOTkxZGE3NjVlMDFkZjQ1M2I5OWQ5NmVjNDY0IiwiaXNzdWVkX2F0IjoiMjAyNi0xMC0zMFQx"
        "MjowMDowMCswMDowMCIsImxhc3QiOlsiMjAyNi0xMC0zMFQxMjowMDowMC4wMDAwMDArMDA6MDAiLCIwMDAwMDAwMC0wMDAwLTQw"
        "MDAtODAwMC0wMDAwMDAwMDAwMDEiXSwibGltaXQiOjEwMCwib3BlcmF0aW9uIjoib3V0Ym94Lmxpc3QiLCJwcm9maWxlX2lkIjoi"
        "YWxwaGEiLCJzdWJqZWN0IjoiZGlhZ25vc3RpYy1yZWFkZXIiLCJ1cHBlciI6WyIyMDI2LTEwLTMwVDEyOjAwOjAwLjAwMDAwMCsw"
        "MDowMCIsIjAwMDAwMDAwLTAwMDAtNDAwMC04MDAwLTAwMDAwMDAwMTAwMCJdLCJ2IjoxfQ.v15tnX4XUE-oJFlFWJoTNIAiRcmY6"
        "kmJM4Itjp9vi5c"
    )
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


async def test_work_window_statement_vm_sticky_cleanup_and_deadline(database):
    (
        _,
        _,
        provider_module,
        _,
    ) = _foundation()
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

    deadline = provider_module.DiagnosticsReadWork(soft_deadline=now - 1, hard_deadline=now - 1)
    with pytest.raises(provider_module.DiagnosticsDeadlineExceeded):
        deadline.check()
    assert deadline.stop == "timeout"


@pytest.mark.parametrize("overflow_kind", ("statements", "statement_vm", "operation_vm"))
async def test_borrowed_runner_one_begin_sample_and_typed_failures(database, overflow_kind):
    service, _, provider_module, _ = _foundation()
    sampled = []
    statements = []

    def trace(_conn, _cursor, statement, _params, _context, _many):
        statements.append(statement.strip().upper())

    event.listen(database.engine.sync_engine, "before_cursor_execute", trace)
    resources = SimpleNamespace(canonical=SimpleNamespace(provider=database, session_factory=database._session_factory))
    ctx = RequestContext("request", _principal(), "alpha", None, asyncio.get_running_loop().time() + 10)

    async def operation(session, work, sampled_at):
        assert (work.statement_vm_limit, work.operation_vm_limit, work.statements_max) == (8_000_000, 40_000_000, 256)
        sampled.append(sampled_at)
        return await database.diagnostics_count(session, "facts", work=work), sampled_at

    try:
        result = await service.run_borrowed_diagnostics(ctx, resources, operation)
    finally:
        event.remove(database.engine.sync_engine, "before_cursor_execute", trace)
    assert result == (0, sampled[0]) and len(sampled) == 1
    assert statements.count("BEGIN") == 1

    async def overflow(session, work, _sampled_at):
        if overflow_kind == "statements":
            work.statements = work.statements_max
            work.check(statement=True)
        setattr(work, overflow_kind + "_limit", 2_000)
        await session.execute(
            text("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<1000000) SELECT sum(x) FROM n")
        )

    with pytest.raises(HttpError, match="E_STORAGE"):
        await service.run_borrowed_diagnostics(ctx, resources, overflow)
    expired = RequestContext("request", _principal(), "alpha", None, asyncio.get_running_loop().time() - 1)
    with pytest.raises(HttpError, match="E_TIMEOUT"):
        await service.run_borrowed_diagnostics(expired, resources, operation)


async def test_provider_log_filter_is_scoped_and_cancel_safe():
    service, _, _, _ = _foundation()
    records = []

    class Capture(logging.Handler):
        def emit(self, record):
            records.append(record)

    logger = logging.getLogger("memory_server.providers.lancedb_provider")
    handler = Capture()
    logger.addHandler(handler)
    logger.setLevel(logging.ERROR)
    service.install_diagnostics_logging_filter()
    try:
        logger.error("outside %s", "VISIBLE")
        async with service.diagnostics_log_scope():
            logger.exception("inside %s", "SECRET", exc_info=ValueError("CANARY"))
            child = asyncio.create_task(asyncio.sleep(0))
            await child
        logger.error("after %s", "VISIBLE2")
    finally:
        logger.removeHandler(handler)
    assert records[0].getMessage() == "outside VISIBLE"
    assert records[1].getMessage() == "diagnostics backend failure"
    assert records[1].args == () and records[1].exc_info is None and records[1].exc_text is None
    assert records[2].getMessage() == "after VISIBLE2"
