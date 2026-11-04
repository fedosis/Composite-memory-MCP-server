"""S17 inspectors: real daemon composition, authentication and admitted SQLite."""

import asyncio
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from time import monotonic
from types import SimpleNamespace
from uuid import uuid4

import pytest

from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import HttpConfig, RequestContext
from memory_server.runtime.daemon_cli import run_daemon
from memory_server.transports.http.app import create_http_app
from tests.gateway.test_http_shell import request
from tests.gateway.test_slice_runtime import build_registry
from tests.gateway.test_slice_runtime import offline_embedder as offline_embedder

T0 = datetime(2030, 1, 2, 3, 4, 5, tzinfo=UTC)
F = "00000000-0000-4000-8000-000000000001"
A = "00000000-0000-4000-8000-000000000002"
B = "00000000-0000-4000-8000-000000000003"
ADMISSION = "/api/v1/profiles/alpha/admission/preview"
FIELDS = set(d.AdmissionResult.model_fields)
PERMISSIONS = [
    "memory.read",
    "memory.write",
    "memory.transition",
    "memory.invalidate",
    "belief.read",
    "belief.write",
    "belief.resolve",
    "relation.read",
    "relation.write",
    "profile.read",
    "profile.manage",
    "audit.read",
]


def context(state, profile="alpha"):
    return RequestContext("req_s17_seed", state.principal, profile, None, monotonic() + 30)


def mint(state, permissions, profiles=("alpha", "beta"), expired=False):
    now = datetime.now(UTC) - (timedelta(hours=2) if expired else timedelta())
    created = state.deps.tokens._bootstrap_create(
        d.TokenCreate(
            subject="s17", profiles=list(profiles), permissions=permissions, expires_at=now + timedelta(hours=1)
        ),
        now=now,
    )
    return f"cmms_{created.token.id}.{created.secret}"


@asynccontextmanager
async def inspection_http_context(tmp_path):
    registry, roots = await build_registry(tmp_path)
    config = HttpConfig(
        control_path=tmp_path / "control" / "control.sqlite",
        rate_burst=10000,
        rate_per_minute=10000,
        retrieval_concurrency=16,
    )
    state = SimpleNamespace(config=config, roots=roots)
    ready, stop = asyncio.Event(), asyncio.Event()

    def factory(config, deps, *, read_routes, mutation_routes):
        state.deps, state.specs, state.mutation_specs = deps, read_routes, mutation_routes
        state.app = create_http_app(config, deps, read_routes=read_routes, mutation_routes=mutation_routes)
        return state.app

    class Server:
        def __init__(self, config):
            self.started = False

        async def serve(self):
            self.started = True
            ready.set()
            await stop.wait()

    task = asyncio.create_task(run_daemon(config, registry, factory=factory, server_factory=Server))
    waiter = asyncio.create_task(ready.wait())
    try:
        done, _ = await asyncio.wait((task, waiter), timeout=15, return_when=asyncio.FIRST_COMPLETED)
        if task in done:
            await task
        assert waiter in done, "S17_COMPOSITION daemon did not compose"
        state.bearer = mint(state, PERMISSIONS)
        state.principal = await state.deps.tokens.authenticate(state.bearer, now=datetime.now(UTC))
        yield state
    finally:
        stop.set()
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)
        assert await asyncio.wait_for(task, 20) == 0


@pytest.fixture
async def inspection_http(tmp_path):
    async with inspection_http_context(tmp_path) as state:
        yield state


async def post(state, body, *, path=ADMISSION, bearer=None, **kwargs):
    return await request(
        state.app, path, method="POST", body=body, bearer=state.bearer if bearer is None else bearer, **kwargs
    )


def error(response, status, code, marker):
    assert response.status == status, (marker, response.status, response.body)
    assert response.json["error"]["code"] == code, (marker, response.body)
    assert response.json["error"]["details"] == {}, (marker, response.body)


async def rows(state, sql, params=None, profile="alpha"):
    from sqlalchemy import text

    async with state.deps.leases.acquire(context(state, profile)) as scope:
        async with scope.application.read_resources().session_factory() as session:
            return [tuple(row) for row in (await session.execute(text(sql), params or {})).all()]


async def domain_snapshot(state, profile="alpha"):
    tables = (
        "facts",
        "beliefs",
        "decisions",
        "skills",
        "evidence",
        "receipts",
        "claim_relations",
        "relation_revisions",
        "lifecycle_states",
        "lifecycle_events",
        "outbox_entries",
        "http_request_journal",
    )
    return {table: await rows(state, f'SELECT * FROM "{table}" ORDER BY rowid', profile=profile) for table in tables}


async def test_s17_admission_public_mount_required(inspection_http):
    response = await post(inspection_http, {"text": "ok"})
    assert response.status == 200, f"S17_RED_MOUNT_REQUIRED expected200 actual{response.status} {response.body!r}"
    assert set(response.json) == FIELDS, "S17_RED_MOUNT_REQUIRED"


async def test_s17_admission_matches_real_gate(inspection_http, monkeypatch):
    from memory_server.admission import MemoryAdmissionGate

    state, calls = inspection_http, []
    service = state.deps.ports["admission.preview"]
    object.__setattr__(service, "clock", lambda: T0)
    original = MemoryAdmissionGate.classify

    def classify(self, text, **kwargs):
        calls.append((text, kwargs))
        return original(self, text, **kwargs)

    monkeypatch.setattr(MemoryAdmissionGate, "classify", classify)
    await rows(state, "SELECT 1")  # Warm the real profile before attributing sessions.
    from sqlalchemy.ext.asyncio import AsyncSession

    sessions = []

    def forbidden_session(*args, **kwargs):
        sessions.append(1)
        pytest.fail("S17_GATE_PARITY SQL-session-not-opened")

    monkeypatch.setattr(AsyncSession, "__init__", forbidden_session)
    for text in (
        "ok",
        "The project uses SQLite for canonical memory storage.",
        "Important: never share private credentials.",
    ):
        for scope in ("user", "agent", None):
            expected = original(MemoryAdmissionGate(), text, source_scope=scope or "agent", now=T0, force=False)
            before = len(calls)
            body = {"text": text} | ({} if scope is None else {"source_scope": scope})
            response = await post(state, body)
            assert response.status == 200, ("S17_GATE_PARITY", response.body)
            if text == "ok":
                assert response.json["tag"] == "ephemeral", "S17_GATE_PARITY ephemeral"
                assert response.json["admitted"] is False, "S17_GATE_PARITY ephemeral not admitted"
                assert len(sessions) == 0, "S17_GATE_PARITY ephemeral zero SQL sessions"
            metadata = expected.metadata
            assert response.json == dict(
                admitted=expected.admitted,
                tag=expected.tag.value,
                ttl_days=expected.ttl_days,
                score=expected.score,
                memory_kind=metadata.get("memory_kind"),
                authority_level=metadata.get("authority_level"),
                epistemic_status=metadata.get("epistemic_status"),
                risk_tags=metadata.get("risk_tags", []),
                reason_codes=list(expected.reason_codes),
            ), "S17_GATE_PARITY"
            assert calls[before:] == [(text, dict(source_scope=scope or "agent", now=T0, force=False))], (
                "S17_GATE_PARITY"
            )


async def test_s17_admission_strict_wire(inspection_http, monkeypatch):
    from memory_server.transports.http.errors import HttpError
    from memory_server.transports.http.routes.inspectors import _decode

    # Keep the named M07 semantic discriminator ahead of independent depth cases.
    for value in (b"NaN", b"Infinity", b"-Infinity", b"1e999"):
        rejected = False
        try:
            _decode(b'{"unknown":{"number":' + value + b"}}")
        except HttpError as exc:
            rejected = exc.code == "E_VALIDATION"
        assert rejected, "S17_WIRE_STRICT nonfinite unknown nested number"
    depth32 = b'{"x":' * 32 + b"0" + b"}" * 32
    assert isinstance(_decode(depth32), dict), "S17_WIRE_STRICT depth32"
    for nested in (b'{"x":' * 33 + b"0" + b"}" * 33, b'{"x":' * 2000 + b"0" + b"}" * 2000):
        with pytest.raises(HttpError) as caught:
            _decode(nested)
        assert caught.value.code == "E_VALIDATION", "S17_WIRE_STRICT depth33/RecursionError"
        error(
            await post(inspection_http, nested, headers=(("Content-Type", "application/json"),)),
            422,
            "E_VALIDATION",
            "S17_WIRE_STRICT depth33/RecursionError",
        )
    from memory_server.transports.http.routes import inspectors

    def recurse(value, depth=0):
        return recurse(value, depth + 1)

    with monkeypatch.context() as patch:
        patch.setattr(inspectors, "_finite", recurse)
        error(await post(inspection_http, {"text": "ok"}), 422, "E_VALIDATION", "S17_WIRE_STRICT actual RecursionError")

    bad = [
        b"\xff",
        b"\xef\xbb\xbf{}",
        b"{} {}",
        b"null",
        b"[]",
        b"1",
        b'"x"',
        b'{"text":"x","text":"y"}',
        b'{"x":{"a":1,"a":2}}',
        b'{"text":"\\ud800"}',
        b'{"text":""}',
        b'{"text":true}',
        b'{"text":"ok","source_scope":null}',
    ]
    bad += [b'{"x":' + value + b"}" for value in (b"NaN", b"Infinity", b"-Infinity", b"1e999")]
    bad += [
        json.dumps({"text": "ok", key: value}).encode()
        for key, value in (("force", True), ("now", "2030"), ("ttl", 1), ("score", 0.5), ("extra", 1))
    ]
    bad += [json.dumps({"text": "x" * 32769}).encode()]
    for body in bad:
        response = await post(inspection_http, body, headers=(("Content-Type", "application/json"),))
        error(response, 422, "E_VALIDATION", "S17_WIRE_STRICT")
    for value in (b"NaN", b"Infinity", b"-Infinity", b"1e999"):
        rejected = False
        try:
            _decode(b'{"unknown":{"number":' + value + b"}}")
        except HttpError as exc:
            rejected = exc.code == "E_VALIDATION"
        assert rejected, "S17_WIRE_STRICT nonfinite unknown nested number"
    for text in ("x", " " + "x" * 32766 + " "):
        assert (await post(inspection_http, {"text": text})).status == 200, "S17_WIRE_STRICT"
    for query in (b"x=", b"cursor=", b"x=1&x=2"):
        error(await post(inspection_http, {"text": "ok"}, query=query), 422, "E_VALIDATION", "S17_WIRE_STRICT")
    for headers in ((), (("Content-Type", "text/plain"),)):
        error(
            await post(inspection_http, b'{"text":"ok"}', headers=headers),
            415,
            "E_UNSUPPORTED_MEDIA_TYPE",
            "S17_WIRE_STRICT",
        )
    error(
        await post(
            inspection_http,
            b'{"text":"ok"}',
            headers=(("Content-Type", "application/json"), ("Content-Type", "application/json")),
        ),
        422,
        "E_VALIDATION",
        "S17_WIRE_STRICT",
    )
    error(
        await post(inspection_http, b" " * (1048576 + 1), headers=(("Content-Type", "application/json"),)),
        413,
        "E_REQUEST_TOO_LARGE",
        "S17_WIRE_STRICT",
    )


async def test_s17_admission_auth_scope(tmp_path):
    from memory_server.transports.http.routes.inspectors import create_inspection_routes

    assert create_inspection_routes()[0].permission == "memory.write", "S17_AUTH_SCOPE"
    try:
        async with inspection_http_context(tmp_path) as state:
            assert (await post(state, {"text": "ok"}, bearer=mint(state, ["memory.write"]))).status == 200, (
                "S17_AUTH_SCOPE"
            )
            error(
                await post(state, {"text": "ok"}, bearer=mint(state, ["memory.read"])),
                403,
                "E_PERMISSION_DENIED",
                "S17_AUTH_SCOPE",
            )
            for bearer in ("", mint(state, ["memory.write"], expired=True)):
                error(await post(state, {"text": "ok"}, bearer=bearer), 401, "E_AUTH_REQUIRED", "S17_AUTH_SCOPE")
            error(
                await post(
                    state,
                    {"text": "ok"},
                    path=ADMISSION.replace("alpha", "beta"),
                    bearer=mint(state, ["memory.write"], ("alpha",)),
                ),
                403,
                "E_PERMISSION_DENIED",
                "S17_AUTH_SCOPE",
            )
            for warm in (False, True):
                revoked = mint(state, ["memory.write"])
                if warm:
                    assert (await post(state, {"text": "ok"}, bearer=revoked)).status == 200, "S17_AUTH_SCOPE cached"
                state.deps.tokens._unchecked_revoke(
                    state.deps.tokens._local_context(datetime.now(UTC)),
                    revoked[5:].split(".", 1)[0],
                    d.ControlMutation(reason="revoke", confirmation=True, expected_generation=1),
                    datetime.now(UTC),
                    local=True,
                )
                error(
                    await post(state, {"text": "ok"}, bearer=revoked), 401, "E_AUTH_REQUIRED", "S17_AUTH_SCOPE revoke"
                )
            error(
                await post(
                    state,
                    {"text": "ok"},
                    path=ADMISSION.replace("alpha", "unknown"),
                    bearer=mint(state, ["memory.write"], ("unknown",)),
                ),
                404,
                "E_PROFILE_NOT_FOUND",
                "S17_AUTH_SCOPE scoped unknown",
            )
            disabled = await request(
                state.app,
                "/api/v1/profiles/alpha/disable",
                method="POST",
                bearer=state.bearer,
                body=dict(reason="test", confirmation=True, expected_generation=1),
                headers=(("Idempotency-Key", str(uuid4())),),
            )
            assert disabled.status == 200, ("S17_AUTH_SCOPE disable", disabled.body)
            error(await post(state, {"text": "ok"}), 409, "E_PROFILE_DISABLED", "S17_AUTH_SCOPE disabled")
    except ValueError as exc:
        raise AssertionError("S17_AUTH_SCOPE canonical descriptor must mount") from exc


async def test_s17_admission_no_domain_write(inspection_http, monkeypatch):
    import copy

    from sqlalchemy.ext.asyncio import AsyncSession

    state = inspection_http
    for profile in ("alpha", "beta"):
        await rows(state, "SELECT 1", profile=profile)
    async with state.deps.leases.acquire(context(state)) as scope:
        graph = scope.runtime.dependencies.graph
        graph_before = copy.deepcopy(graph.to_dict())
        persisted_before = graph._snapshot_path.read_bytes() if graph._snapshot_path.exists() else None
    before = await domain_snapshot(state)
    audit = len(list(state.deps.access._db.execute("SELECT * FROM access_audit")))
    ids, sessions = set(), []
    for _ in range(10):
        with monkeypatch.context() as patch:

            def forbidden_session(*args, **kwargs):
                sessions.append(1)
                pytest.fail("S17_NO_WRITE admission SQL-session-not-opened")

            patch.setattr(AsyncSession, "__init__", forbidden_session)
            response = await post(
                state, {"text": "ok"}, headers=(("Idempotency-Key", "ignored"), ("If-Match", "ignored"))
            )
        assert response.status == 200, "S17_NO_WRITE"
        assert sessions == [], "S17_NO_WRITE admission zero SQL sessions"
        assert graph.to_dict() == graph_before, "S17_NO_WRITE admission graph"
        persisted_after = graph._snapshot_path.read_bytes() if graph._snapshot_path.exists() else None
        assert persisted_after == persisted_before, "S17_NO_WRITE admission persisted graph"
        ids.add(response.headers["x-request-id"])
        assert "idempotency-replayed" not in response.headers, "S17_NO_WRITE"
        assert await domain_snapshot(state) == before, "S17_NO_WRITE"
    assert len(ids) == 10, "S17_NO_WRITE"
    completed = list(state.deps.access._db.execute("SELECT * FROM access_audit ORDER BY seq"))[audit:]
    assert len(completed) == 10, "S17_NO_WRITE"
    assert all(row["state"] == "terminal" and row["result"] == "success" for row in completed), "S17_NO_WRITE"


async def test_s17_admission_privacy_failure(inspection_http, caplog, monkeypatch):
    state = inspection_http
    response = await post(state, {"text": "S17_CONTENT_CANARY S17_SECRET_CANARY /S17_PATH_CANARY"})
    assert response.status == 200, "S17_PRIVACY"
    for canary in ("S17_CONTENT_CANARY", "S17_SECRET_CANARY", "/S17_PATH_CANARY"):
        assert canary not in response.body.decode() + caplog.text, "S17_PRIVACY"
    service = state.deps.ports["admission.preview"]
    old = service.gate
    object.__setattr__(
        service,
        "gate",
        SimpleNamespace(
            classify=lambda *a, **k: SimpleNamespace(
                admitted=True,
                tag=SimpleNamespace(value="durable"),
                ttl_days=True,
                score=float("nan"),
                metadata={"risk_tags": "S17_REASON_CANARY"},
                reason_codes=[],
            )
        ),
    )
    try:
        error(await post(state, {"text": "ok"}), 500, "E_INTERNAL", "S17_PRIVACY")
    finally:
        object.__setattr__(service, "gate", old)
    from prometheus_client import generate_latest

    from memory_server.application.inspection_service import InspectionService

    original = InspectionService._admission
    responses = [response]
    corruptions = [
        ("admitted", 1),
        ("ttl_days", True),
        ("score", float("inf")),
        ("score", float("nan")),
        ("score", True),
        ("risk_tags", "S17_REASON_CANARY"),
        ("risk_tags", [True]),
        ("reason_codes", [123]),
        ("memory_kind", []),
        ("authority_level", {}),
        ("epistemic_status", []),
        ("tag", "unknown"),
        ("hidden", "S17_SECRET_CANARY"),
    ]
    for key, value in corruptions:

        async def corrupt(self, command, key=key, value=value):
            result = await original(self, command)
            result.__dict__[key] = value
            return result

        with monkeypatch.context() as patch:
            patch.setattr(InspectionService, "_admission", corrupt)
            result = await post(state, {"text": "S17_CONTENT_CANARY /S17_PATH_CANARY"})
        error(result, 500, "E_INTERNAL", "S17_PRIVACY result corruption")
        responses.append(result)
    access = [dict(row) for row in state.deps.access._db.execute("SELECT * FROM access_audit")]
    egress = caplog.text + repr(access) + generate_latest(state.deps.metrics._registry).decode()
    egress += "".join(r.body.decode() for r in responses)
    for canary in (
        "S17_CONTENT_CANARY",
        "S17_REASON_CANARY",
        "S17_SQL_CANARY",
        "S17_SECRET_CANARY",
        "/S17_PATH_CANARY",
    ):
        assert canary not in egress, "S17_PRIVACY access/metrics/result canary scan"


async def test_s17_inspection_composition_exact_sets(tmp_path, monkeypatch):
    from dataclasses import replace

    from memory_server.gateway_contracts.catalog import CATALOG
    from memory_server.runtime.read_composition import compose_inspection_ports
    from memory_server.transports.http.routes.inspectors import InspectionRouteSpec, create_inspection_routes

    specs = create_inspection_routes()
    assert all(type(s) is InspectionRouteSpec for s in specs), "S17_COMPOSITION"
    try:
        async with inspection_http_context(tmp_path) as state:
            reads = {
                "memories.list",
                "memories.get",
                "receipts.get",
                "history.list",
                "lookup",
                "memories.reveal",
                "search",
                "context",
                "relations.for_memory",
                "relations.list",
                "relations.get",
                "graph.neighborhood",
                "graph.path",
                "graph.entity",
                "audit",
                "indexes.status",
                "outbox.list",
                "overview",
                "reflect",
                "traces.get",
                "traces.list",
                "beliefs.list",
                "beliefs.get",
                "conflicts.list",
                "admission.preview",
                "mutations.preview",
            }
            writes = {
                "memories.create",
                "learn",
                "memories.transition",
                "memories.invalidate",
                "beliefs.set",
                "conflicts.resolve",
                "relations.create",
                "relations.delete",
            }
            shell = {"health", "ready", "profiles.list", "profiles.get", "profiles.enable", "profiles.disable"}
            assert {s.operation for s in state.specs} == reads, "S17_COMPOSITION"
            assert {s.operation for s in state.mutation_specs} == writes, "S17_COMPOSITION"
            assert set(state.deps.ports) == reads | writes, "S17_COMPOSITION"
            assert (len(state.specs), len(state.mutation_specs), len(state.deps.ports), len(state.app.routes)) == (
                26,
                8,
                34,
                40,
            ), "S17_COMPOSITION"
            assert {r.name for r in state.app.routes} == reads | writes | shell, "S17_COMPOSITION"
            ports = {k: v for k, v in state.deps.ports.items() if k not in {"admission.preview", "mutations.preview"}}
            from memory_server.transports.http.errors import HttpError
            from memory_server.transports.http.guards import ReadRouteSpec

            with pytest.raises(HttpError) as caught:
                compose_inspection_ports(replace(state.deps, leases=None), state.config)
            assert caught.value.code == "E_RUNTIME_NOT_READY", "S17_COMPOSITION missing lease"
            with monkeypatch.context() as patch:
                patch.setattr(
                    state.deps.leases, "acquire", lambda *args: pytest.fail("S17_COMPOSITION compose opened profile")
                )
                composed, mounted = compose_inspection_ports(replace(state.deps, ports=ports), state.config)
            for spec in specs:
                with pytest.raises(ValueError):
                    create_http_app(state.config, state.deps, mutation_routes=(spec,))
                invalid_specs = [
                    (spec, spec),
                    (replace(spec, method="GET"),),
                    (replace(spec, operation="memories.list"),),
                    (
                        ReadRouteSpec(
                            spec.operation, spec.method, spec.path, spec.permission, spec.parse, spec.validate_result
                        ),
                    ),
                    (replace(spec, parse=lambda *args: None),),
                    (replace(spec, validate_result=lambda *args: None),),
                ]
                for invalid in invalid_specs:
                    with pytest.raises(ValueError):
                        create_http_app(state.config, state.deps, read_routes=invalid)
            assert all(composed.ports[k] is v for k, v in ports.items()), "S17_COMPOSITION"
            assert composed.ports["admission.preview"] is composed.ports["mutations.preview"], "S17_COMPOSITION"
            assert mounted == specs and all(a is b for a, b in zip(mounted, specs)), "S17_COMPOSITION"
            for spec in specs:
                assert CATALOG.by_operation[spec.operation].mutation is False, "S17_COMPOSITION"
                for clone in (replace(spec), replace(spec, permission="profile.read"), replace(spec, path="/bad")):
                    with pytest.raises(ValueError):
                        create_http_app(state.config, state.deps, read_routes=(clone,))
                missing = create_http_app(state.config, replace(state.deps, ports={}), read_routes=(spec,))
                audit_before = [dict(row) for row in state.deps.access._db.execute("SELECT * FROM access_audit")]
                with monkeypatch.context() as patch:
                    patch.setattr(
                        state.deps.leases,
                        "acquire",
                        lambda *args: pytest.fail("S17_COMPOSITION missing port acquired lease"),
                    )
                    response = await request(
                        missing,
                        spec.path.replace("{profile}", "alpha").replace("{id}", F),
                        method="POST",
                        bearer=state.bearer,
                        body={"text": "ok"}
                        if spec.operation == "admission.preview"
                        else dict(
                            operation="invalidate",
                            memory_type="fact",
                            to_state=None,
                            relation_id=None,
                            conflict_id=None,
                        ),
                    )
                assert [
                    dict(row) for row in state.deps.access._db.execute("SELECT * FROM access_audit")
                ] == audit_before, "S17_COMPOSITION missing port no intent"
                error(response, 503, "E_CAPABILITY_UNAVAILABLE", "S17_COMPOSITION")
            print("S17_COMPOSITION reads=26 mutations=8 descriptors=34 routes=40")
    except ValueError as exc:
        raise AssertionError("S17_COMPOSITION canonical inspectors must mount") from exc


async def test_s17_inspection_headers_audit_boundary(inspection_http, monkeypatch):
    state = inspection_http
    response = await post(state, {"text": "ok"}, headers=(("X-Request-ID", "untrusted"),))
    assert response.status == 200, "S17_HTTP_BOUNDARY"
    assert response.headers["x-request-id"].startswith("req_"), "S17_HTTP_BOUNDARY"
    assert "request_id" not in response.json, "S17_HTTP_BOUNDARY"
    for key, value in (
        ("cache-control", "no-store"),
        ("x-content-type-options", "nosniff"),
        ("referrer-policy", "no-referrer"),
    ):
        assert response.headers[key] == value, "S17_HTTP_BOUNDARY"
    assert "set-cookie" not in response.headers, "S17_HTTP_BOUNDARY"
    for path in (ADMISSION, f"/api/v1/profiles/alpha/memories/{F}/preview"):
        for method in ("GET", "HEAD", "DELETE"):
            result = await request(state.app, path, method=method, bearer=state.bearer)
            assert result.status == 405, "S17_HTTP_BOUNDARY"
            if method == "HEAD":
                assert result.body == b"", "S17_HTTP_BOUNDARY"
        before = list(state.deps.access._db.execute("SELECT * FROM access_audit"))
        assert (await request(state.app, path, method="OPTIONS")).status == 204, "S17_HTTP_BOUNDARY"
        assert list(state.deps.access._db.execute("SELECT * FROM access_audit")) == before, "S17_HTTP_BOUNDARY"
    errors = []
    cookie = await post(state, {"text": "ok"}, bearer="", headers=(("Cookie", "session=untrusted"),))
    error(cookie, 401, "E_AUTH_REQUIRED", "S17_HTTP_BOUNDARY cookie-only")
    errors.append(cookie)
    malformed = await post(state, {"text": None}, headers=(("X-Request-ID", "untrusted"),))
    error(malformed, 422, "E_VALIDATION", "S17_HTTP_BOUNDARY")
    errors.append(malformed)

    async def audit_failure(*args, **kwargs):
        raise OSError("S17_SECRET_CANARY /S17_PATH_CANARY")

    before = [dict(row) for row in state.deps.access._db.execute("SELECT * FROM access_audit")]
    with monkeypatch.context() as patch:
        patch.setattr(state.deps.access, "begin", audit_failure)
        patch.setattr(
            state.deps.leases, "acquire", lambda *args: pytest.fail("S17_HTTP_BOUNDARY begin failure acquired lease")
        )
        failed_begin = await post(state, {"text": "ok"})
    error(failed_begin, 503, "E_AUDIT_UNAVAILABLE", "S17_HTTP_BOUNDARY begin failure")
    assert [dict(row) for row in state.deps.access._db.execute("SELECT * FROM access_audit")] == before, (
        "S17_HTTP_BOUNDARY"
    )
    errors.append(failed_begin)
    with monkeypatch.context() as patch:
        patch.setattr(state.deps.access, "finish", audit_failure)
        failed_finish = await post(state, {"text": "ok"})
    error(failed_finish, 503, "E_AUDIT_UNAVAILABLE", "S17_HTTP_BOUNDARY finish failure")
    after = [dict(row) for row in state.deps.access._db.execute("SELECT * FROM access_audit ORDER BY seq")]
    assert len(after) == len(before) + 1 and after[-1]["state"] == "intent", "S17_HTTP_BOUNDARY finish durable intent"
    errors.append(failed_finish)
    for result in errors:
        assert result.headers["x-request-id"].startswith("req_") and result.headers["x-request-id"] != "untrusted", (
            "S17_HTTP_BOUNDARY"
        )
        assert result.json["error"]["request_id"] == result.headers["x-request-id"], (
            "S17_HTTP_BOUNDARY error request ID"
        )
        for key, value in (
            ("cache-control", "no-store"),
            ("x-content-type-options", "nosniff"),
            ("referrer-policy", "no-referrer"),
        ):
            assert result.headers[key] == value, "S17_HTTP_BOUNDARY error headers"
        assert "set-cookie" not in result.headers, "S17_HTTP_BOUNDARY error cookie"
