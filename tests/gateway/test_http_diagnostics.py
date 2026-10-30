"""B1 diagnostics tests: service/auditor/outbox only; HTTP remains B3-mount RED."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

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


def _foundation(*, require_b1=True):
    import importlib

    service = importlib.import_module("memory_server.application.diagnostics_service")
    if require_b1:
        assert hasattr(service, "DiagnosticsService"), "B1 service not implemented"
    provider = importlib.import_module("memory_server.providers.sqlite_provider")
    outbox = importlib.import_module("storage.outbox")
    return service, provider, outbox


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
    async for value in module.shell.__wrapped__(tmp_path):
        yield value


@pytest.mark.asyncio
async def test_b1_http_routes_are_unmounted_until_b3(shell, caplog):
    from memory_server.runtime.read_composition import compose_read_ports
    from memory_server.transports.http.app import create_http_app
    from tests.gateway.test_http_shell import request, rows

    deps, specs = compose_read_ports(shell.deps, shell.config)
    assert {spec.operation for spec in specs}.isdisjoint(
        {"audit", "indexes.status", "outbox.list", "overview", "reflect"}
    )
    app = create_http_app(shell.config, deps, read_routes=specs)
    before = rows(shell)
    for method, suffix, body in (
        ("GET", "audit", b""),
        ("GET", "indexes/status", b""),
        ("GET", "outbox", b""),
        ("GET", "overview", b""),
        ("POST", "reflect", b'{"topic":null,"min_confidence":null,"context":null}'),
    ):
        response = await request(
            app,
            f"/api/v1/profiles/alpha/{suffix}",
            method=method,
            bearer=shell.bearer,
            body=body,
            headers=[("Content-Type", "application/json")] if method == "POST" else [],
            query=b"cursor=B1_CURSOR_CANARY",
        )
        assert response.status == 404, (method, suffix, response.json)
        assert response.json["error"]["code"] == "E_NOT_FOUND"
        assert "B1_PAYLOAD_CANARY" not in response.body.decode()
    assert rows(shell) == before
    from prometheus_client import generate_latest

    surfaces = repr(rows(shell)) + caplog.text + generate_latest(shell.metrics._registry).decode()
    assert all(secret not in surfaces for secret in ("B1_CURSOR_CANARY", "B1_PAYLOAD_CANARY"))

    assert shell.leases.snapshot() == {}
    await app.close()


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
    _, provider_module, outbox_module = _foundation(require_b1=False)
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
    service, provider_module, _ = _foundation(require_b1=False)
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

    _, _, outbox = _foundation(require_b1=False)
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
    service, provider_module, _ = _foundation(require_b1=False)
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
