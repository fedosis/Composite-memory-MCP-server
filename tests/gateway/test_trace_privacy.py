"""S11B persistence, privacy, custody, and HTTP-boundary tests."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sqlite3
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from memory_server.control.store import ControlError
from memory_server.control.traces import TracesStore
from memory_server.gateway_contracts import dto as d
from memory_server.transports.http.errors import HttpError
from memory_server.transports.http.routes import traces as trace_routes
from tests.gateway.test_http_retrieval import similarity_embedder as similarity_embedder
from tests.gateway.test_http_shell import shell as shell
from tests.gateway.test_retrieval_trace import _command
from tests.gateway.test_retrieval_trace import gateway as gateway

CANARY = "TRACE_QUERY_CANARY_CONTENT_CANARY"


def _trace(ident="trace-1", profile="alpha", created=None, *, operation="search"):
    created = created or datetime(2026, 1, 1, tzinfo=UTC)
    return {
        "id": ident,
        "request_id": "request-1",
        "profile_id": profile,
        "operation": operation,
        "query": None,
        "stages": {"fts": []},
        "stage_status": {"fts": "ran"},
        "merge": [],
        "deduplicated": [],
        "final": [],
        "created_at": created.isoformat(),
        "expires_at": (created + timedelta(seconds=30)).isoformat(),
        "settings_digest": "0" * 64,
        "code_revision": "test",
    }


def _ctx(profile="alpha", *, deadline=1000.0, permissions=("memory.read",)):
    return SimpleNamespace(
        request_id="request-1",
        profile_id=profile,
        deadline=deadline,
        principal=SimpleNamespace(subject="subject-1", permissions=set(permissions), profiles={profile}),
    )


@pytest.mark.asyncio
async def test_trace_persists_no_query_or_content(tmp_path, caplog):
    from tests.gateway.test_http_retrieval import http_request, live_server, post, read_scope, seed_fact

    async with live_server(tmp_path) as (holder, port, auth):
        async with await read_scope(holder, auth) as scope:
            await seed_fact(scope.application.read_resources(), "trace-canary-memory", CANARY)
        status, result = await post(port, auth, query=CANARY, mode="keyword", trace=True)
        assert status == 200 and result["trace_id"] and result["results"]
        assert CANARY not in json.dumps(result)
        status, trace = await http_request(
            port, "GET", f"/api/v1/profiles/alpha/retrieval/trace/{result['trace_id']}", headers=auth
        )
        assert status == 200 and trace["query"] is None and trace["final"]
        assert CANARY not in json.dumps(trace) and CANARY not in caplog.text
        for seq in list(trace["stages"].values()) + [trace["merge"], trace["deduplicated"], trace["final"]]:
            assert all(c["matched_terms"] == [] and c["relation_path"] == [] for c in seq)
        paths = list(tmp_path.rglob("traces.sqlite3*"))
        assert paths
        for path in paths:
            assert CANARY.encode().hex() not in path.read_bytes().hex()


def test_trace_ttl_and_cap(tmp_path):
    now = datetime(2026, 1, 1, tzinfo=UTC)
    store = TracesStore(tmp_path, ttl_seconds=10, max_per_profile=2, max_payload_bytes=4096)
    for ident, profile, offset in (("a1", "alpha", 1), ("a2", "alpha", 2), ("a3", "alpha", 3), ("b1", "beta", 1)):
        t = now + timedelta(seconds=offset)
        store.insert(_trace(ident, profile, t), now=t)
    assert store.get("alpha", "a1", now=now + timedelta(seconds=3)) is None
    assert store.get("alpha", "a2", now=now + timedelta(seconds=3)) is not None
    assert store.get("alpha", "a3", now=now + timedelta(seconds=3)) is not None
    assert store.get("beta", "b1", now=now + timedelta(seconds=3)) is not None
    assert store.get("alpha", "a2", now=now + timedelta(seconds=40)) is None
    with sqlite3.connect(tmp_path / "traces.sqlite3") as db:
        assert db.execute("select count(*) from traces where profile_id='alpha'").fetchone()[0] == 0


def test_trace_cross_profile_and_cursor_binding(tmp_path):
    from memory_server.transports.http.routes.traces import TraceCursorCodec

    now = datetime(2026, 1, 1, tzinfo=UTC)
    codec = TraceCursorCodec(b"k" * 32, ttl_seconds=60)
    command = d.TracesListCommand(
        operation="traces.list", profile_id="alpha", path_ids={}, payload=d.PageQuery(limit=10)
    )
    token = codec.issue(_ctx("alpha"), command, last=(now.isoformat(), "x"), limit=10, now=now)
    listed = command.model_copy(update={"payload": d.PageQuery(limit=10, cursor=token)})
    assert codec.validate(_ctx("alpha").principal, listed, now=now)
    foreign = listed.model_copy(update={"profile_id": "beta"})
    with pytest.raises(HttpError, match="E_CURSOR_INVALID"):
        codec.validate(_ctx("beta").principal, foreign, now=now)
    store = TracesStore(tmp_path, ttl_seconds=30, max_per_profile=10, max_payload_bytes=4096)
    store.insert(_trace("foreign", "beta"), now=now)
    assert store.get("alpha", "foreign", now=now) is None


def test_trace_validators_closed():
    raw = d.Trace.model_validate_json(json.dumps(_trace()), strict=True).model_dump()
    assert trace_routes._validate_trace(SimpleNamespace(model_dump=lambda: raw)).id == "trace-1"
    for change in (
        {"query": CANARY},
        {"extra": "closed"},
        {"stage_status": {"fts": "unknown"}},
        {"stage_status": {"secret": "ran"}},
        {"stages": {"secret": []}},
        {"merge": [{"source": "secret"}]},
        {"merge": [{"source": "fts", "matched_terms": [CANARY]}]},
        {"final": [{"source": "fts", "relation_path": [CANARY]}]},
    ):
        bad = {**raw, **change}
        with pytest.raises(HttpError, match="E_INTERNAL"):
            trace_routes._validate_trace(SimpleNamespace(model_dump=lambda: bad))
    item = d.TraceSummary.model_validate_json(
        json.dumps({k: v for k, v in _trace().items() if k in d.TraceSummary.model_fields}), strict=True
    )
    for page in (
        d.Page[d.TraceSummary](items=[item], has_more=True, next_cursor=None, request_id="r"),
        d.Page[d.TraceSummary](
            items=[item.model_copy(update={"settings_digest": "x" * 64})],
            has_more=False,
            next_cursor=None,
            request_id="r",
        ),
    ):
        with pytest.raises(HttpError, match="E_INTERNAL"):
            trace_routes._validate_page(page)


@pytest.mark.asyncio
async def test_trace_fail_quiet_insert(gateway, monkeypatch):
    from memory_server.application import retrieval_trace as module
    from tests.gateway.test_retrieval_trace import _identity

    base, ctx = gateway
    plain = await base.execute(ctx, _command("keyword", False))
    service = base.traces
    for fault in ("insert", "canonical", "clock", "dto"):

        def fail(*_args, **_kwargs):
            raise OSError("injected failure")

        before = service.dropped
        with monkeypatch.context() as m:
            if fault == "insert":
                m.setattr(service._store, "insert", fail)
            elif fault == "canonical":
                m.setattr(module, "canonical", fail)
            elif fault == "clock":
                m.setattr(service, "clock", fail)
            else:
                m.setattr(module.d, "Trace", fail)
            traced = await base.execute(ctx, _command("keyword", True))
        assert _identity(traced) == _identity(plain)
        assert traced.trace_id is None and service.dropped == before + 1
    assert service._store.page("alpha", limit=10, cursor=None, now=datetime.now(UTC)) == []

    import time

    def slow(*_args, **_kwargs):
        time.sleep(0.3)
        return "late-id"

    with monkeypatch.context() as m:
        m.setattr(service._store, "insert", slow)
        traced = await base.execute(ctx, _command("keyword", True))
        assert _identity(traced) == _identity(plain)
        assert traced.trace_id is None
    await asyncio.sleep(0.1)
    assert traced.trace_id is None


def test_trace_schema_rehearsal(tmp_path):
    now = datetime(2026, 1, 1, tzinfo=UTC)
    store = TracesStore(tmp_path, ttl_seconds=30, max_per_profile=2, max_payload_bytes=4096)
    store.insert(_trace(), now=now)
    reopened = TracesStore(tmp_path, ttl_seconds=30, max_per_profile=2, max_payload_bytes=4096)
    assert reopened.get("alpha", "trace-1", now=now)["id"] == "trace-1"
    with sqlite3.connect(tmp_path / "traces.sqlite3") as db:
        assert db.execute("SELECT v FROM schema_meta WHERE k='version'").fetchone() == (1,)
        assert json.loads(db.execute("SELECT body_json FROM traces").fetchone()[0])["id"] == "trace-1"

    with sqlite3.connect(tmp_path / "traces.sqlite3") as db:
        db.execute("update schema_meta set v=99 where k='version'")
        db.commit()
    with pytest.raises(ControlError):
        TracesStore(tmp_path, ttl_seconds=30, max_per_profile=2, max_payload_bytes=4096)


@pytest.mark.asyncio
async def test_trace_http_get_after_search(tmp_path):
    from tests.gateway.test_http_retrieval import http_request, live_server, post

    async with live_server(tmp_path) as (_holder, port, auth):
        status, body = await post(port, auth, query=CANARY, mode="keyword", trace=True)
        assert status == 200 and body["trace_id"]
        assert CANARY not in json.dumps(body)
        for path in tmp_path.rglob("*"):
            if path.is_file():
                assert CANARY.encode() not in path.read_bytes()
        status, trace = await http_request(
            port, "GET", f"/api/v1/profiles/alpha/retrieval/trace/{body['trace_id']}", headers=auth
        )
        assert status == 200, trace
        assert trace["stages"]
        status, page = await http_request(port, "GET", "/api/v1/profiles/alpha/retrieval/traces?limit=10", headers=auth)
        assert status == 200 and any(item["id"] == body["trace_id"] for item in page["items"])
        foreign_status, foreign = await http_request(
            port, "GET", f"/api/v1/profiles/beta/retrieval/trace/{body['trace_id']}", headers=auth
        )
        assert foreign_status == 404 and foreign["error"]["code"] == "E_NOT_FOUND"
        db_path = next(tmp_path.rglob("traces.sqlite3"))
        with sqlite3.connect(db_path) as db:
            original = db.execute("SELECT body_json FROM traces WHERE id=?", (body["trace_id"],)).fetchone()[0]
        for change in ({"extra": "closed"}, {"merge": [{"source": "secret"}]}, {"stage_status": {"fts": "unknown"}}):
            with sqlite3.connect(db_path) as db:
                db.execute(
                    "UPDATE traces SET body_json=? WHERE id=?",
                    (json.dumps({**json.loads(original), **change}), body["trace_id"]),
                )
            status, error = await http_request(
                port, "GET", f"/api/v1/profiles/alpha/retrieval/trace/{body['trace_id']}", headers=auth
            )
            assert status == 500 and error["error"]["code"] == "E_INTERNAL"
        with sqlite3.connect(db_path) as db:
            db.execute("UPDATE traces SET body_json=? WHERE id=?", (original, body["trace_id"]))
        status, context = await post(port, auth, operation="context", trace=True)
        assert status == 200 and context["trace_id"]
        status, record = await http_request(
            port, "GET", f"/api/v1/profiles/alpha/retrieval/trace/{context['trace_id']}", headers=auth
        )
        assert status == 200 and record["stages"] == {"context": []}
        status, first = await http_request(port, "GET", "/api/v1/profiles/alpha/retrieval/traces?limit=1", headers=auth)
        assert status == 200 and first["has_more"] and first["next_cursor"]
        from urllib.parse import quote

        suffix = "?limit=1&cursor=" + quote(first["next_cursor"], safe="")
        status, second = await http_request(
            port, "GET", "/api/v1/profiles/alpha/retrieval/traces" + suffix, headers=auth
        )
        assert status == 200 and not second["has_more"] and second["items"][0]["id"] != first["items"][0]["id"]
        status, foreign = await http_request(
            port, "GET", "/api/v1/profiles/beta/retrieval/traces" + suffix, headers=auth
        )
        assert status == 400 and foreign["error"]["code"] == "E_CURSOR_INVALID"
        with sqlite3.connect(db_path) as db:
            db.execute("UPDATE traces SET expires_at=? WHERE id=?", ("2000-01-01T00:00:00+00:00", body["trace_id"]))
        status, expired = await http_request(
            port, "GET", f"/api/v1/profiles/alpha/retrieval/trace/{body['trace_id']}", headers=auth
        )
        assert status == 404 and expired["error"]["code"] == "E_NOT_FOUND"
        with sqlite3.connect(db_path) as db:
            assert db.execute("SELECT count(*) FROM traces WHERE id=?", (body["trace_id"],)).fetchone() == (0,)


@pytest.mark.asyncio
async def test_no_write_outside_trace_store(gateway, tmp_path):
    base, ctx = gateway

    def snapshot():
        result = {}
        for path in tmp_path.rglob("*"):
            if path.is_file() and path.read_bytes()[:16] == b"SQLite format 3\x00":
                with sqlite3.connect(path) as db:
                    result[path.name + str(path.parent)] = hashlib.sha256("\n".join(db.iterdump()).encode()).hexdigest()
        return result

    before = snapshot()
    assert any("control.sqlite" in k for k in before)
    assert any("access.sqlite" in k for k in before)
    assert len(before) >= 4
    for mode in ("keyword", "hybrid", "context"):
        assert (await base.execute(ctx, _command(mode, False))).trace_id is None
        assert (await base.execute(ctx, _command(mode, True))).trace_id is not None
    after = snapshot()
    changed = [k for k in before if before[k] != after[k]]
    assert len(changed) == 1 and changed[0].startswith("traces.sqlite3")
    print("NO_WRITE_BEFORE", before)
    print("NO_WRITE_AFTER", after)


def test_trace_spec_mount_identity(shell):
    from memory_server.transports.http.app import create_http_app
    from memory_server.transports.http.guards import ReadRouteSpec

    codec = trace_routes.TraceCursorCodec(b"k" * 32)
    routes = trace_routes.create_trace_routes(cursor_codec=codec)
    assert len(create_http_app(shell.config, shell.deps, read_routes=routes).read_routes) == 2

    class Subclass(trace_routes.TraceRouteSpec):
        pass

    for spec in routes:
        plain = ReadRouteSpec(spec.operation, spec.method, spec.path, spec.permission, spec.parse, spec.validate_result)
        for bad in (
            replace(spec, parse=lambda *_: None),
            replace(spec, validate_result=lambda *_: None),
            replace(spec, method="POST"),
            replace(spec, path="/other"),
            replace(spec, permission="audit.read"),
            replace(spec, cursor_codec=object()),
            Subclass(**spec.__dict__),
            plain,
        ):
            with pytest.raises(ValueError):
                create_http_app(shell.config, shell.deps, read_routes=(bad,))
    with pytest.raises(ValueError):
        create_http_app(shell.config, shell.deps, read_routes=(routes[0], routes[0]))


def test_trace_cursor_tamper_binding_and_ttl():
    from memory_server.transports.http.routes.traces import TraceCursorCodec

    now = datetime(2026, 1, 1, tzinfo=UTC)
    codec = TraceCursorCodec(b"c" * 32, ttl_seconds=10)
    command = d.TracesListCommand(
        operation="traces.list", profile_id="alpha", path_ids={}, payload=d.PageQuery(limit=5)
    )
    token = codec.issue(_ctx(), command, last=(now.isoformat(), "id"), limit=5, now=now)
    body, signature = token.split(".")
    import base64
    import hmac

    from memory_server.application.memory_read_service import canonical

    payload_data = json.loads(trace_routes._unb64(body))
    for change in (
        {"v": True},
        {"profile_id": "beta"},
        {"subject": "other"},
        {"operation": "traces.get"},
        {"last": [now.isoformat(), 1]},
        {"last": ["not-a-timestamp", "id"]},
        {"last": [now.isoformat(), "a/b"]},
        {"expires_at": (now + timedelta(seconds=11)).isoformat()},
        {"issued_at": (now + timedelta(seconds=1)).isoformat()},
        {"limit": True},
        {"extra": 1},
    ):
        raw = base64.urlsafe_b64encode(canonical({**payload_data, **change})).decode().rstrip("=")
        sig = base64.urlsafe_b64encode(hmac.digest(codec._key, raw.encode(), "sha256")).decode().rstrip("=")
        command_bad = command.model_copy(update={"payload": d.PageQuery(limit=5, cursor=raw + "." + sig)})
        with pytest.raises(HttpError, match="E_CURSOR_INVALID"):
            codec.validate(_ctx().principal, command_bad, now=now)

    tampered = ("A" if body[0] != "A" else "B") + body[1:] + "." + signature
    for payload, when, code in (
        (d.PageQuery(limit=5, cursor=tampered), now, "E_CURSOR_INVALID"),
        (d.PageQuery(limit=6, cursor=token), now, "E_CURSOR_INVALID"),
        (d.PageQuery(limit=5, cursor=token), now + timedelta(seconds=11), "E_CURSOR_EXPIRED"),
    ):
        with pytest.raises(HttpError, match=code):
            codec.validate(_ctx().principal, command.model_copy(update={"payload": payload}), now=when)


@pytest.mark.parametrize("suffix", ["", "-journal", "-wal", "-shm"])
@pytest.mark.parametrize("unsafe", ["symlink", "mode", "hardlink", "uid"])
def test_trace_store_refuses_symlink_and_unsafe_mode(tmp_path, monkeypatch, suffix, unsafe):
    target = tmp_path / "real.sqlite3"
    target.write_bytes(b"")
    target.chmod(0o600)
    path = tmp_path / ("traces.sqlite3" + suffix)
    if unsafe == "symlink":
        path.symlink_to(target)
    elif unsafe == "hardlink":
        os.link(target, path)
    else:
        path.write_bytes(b"")
        path.chmod(0o644 if unsafe == "mode" else 0o600)
    if unsafe == "uid":
        original = os.stat

        def wrong_uid(name, *args, **kwargs):
            info = original(name, *args, **kwargs)
            if name == path.name:
                values = list(info)
                values[4] = os.geteuid() + 1
                return os.stat_result(values)
            return info

        monkeypatch.setattr(os, "stat", wrong_uid)
    with pytest.raises(ControlError) as caught:
        TracesStore(tmp_path, ttl_seconds=30, max_per_profile=2, max_payload_bytes=4096)
    assert caught.value.code == "E_STORAGE"


@pytest.mark.asyncio
async def test_trace_requires_permission_and_scoped_profile(tmp_path, monkeypatch):
    from time import monotonic

    from memory_server.application.gateway_retrieval import GatewayRetrievalPort
    from memory_server.gateway_contracts.ports import RequestContext
    from tests.gateway.test_http_retrieval import http_request, live_server

    async with live_server(tmp_path) as (holder, port, auth):
        deps = holder.app.dependencies
        calls = []
        original = deps.leases.acquire

        def counted(ctx):
            calls.append(ctx.request_id)
            return original(ctx)

        monkeypatch.setattr(deps.leases, "acquire", counted)
        now = datetime.now(UTC)
        token = deps.tokens._bootstrap_create(
            d.TokenCreate(
                subject="no-memory",
                profiles=["alpha"],
                permissions=["profile.read"],
                expires_at=now + timedelta(hours=1),
            ),
            now=now,
        )
        for path in ("/api/v1/profiles/alpha/retrieval/trace/id", "/api/v1/profiles/alpha/retrieval/traces"):
            status, error = await http_request(
                port, "GET", path, headers={"Authorization": f"Bearer cmms_{token.token.id}.{token.secret}"}
            )
            assert status == 403 and error["error"]["code"] == "E_PERMISSION_DENIED"
        assert calls == []
        principal = await deps.tokens.authenticate(auth["Authorization"][7:], now=now)
        ctx = RequestContext("permission-test", principal, "alpha", None, monotonic() + 30)
        command = d.TracesGetCommand(operation="traces.get", profile_id="beta", path_ids={"id": "x"}, payload=d.Empty())
        with pytest.raises(HttpError, match="E_PERMISSION_DENIED"):
            await deps.ports["traces.get"].execute(ctx, command)
        base = deps.ports["search"]
        missing = GatewayRetrievalPort(base.leases, base.bind_read, base.metrics, native=base.native, traces=None)
        with pytest.raises(HttpError, match="E_CAPABILITY_UNAVAILABLE"):
            await missing.execute(ctx, _command("keyword", True))
        assert calls == []


@pytest.mark.asyncio
async def test_trace_contribution_cap_drops_record(gateway, monkeypatch):
    base, ctx = gateway
    monkeypatch.setattr(base.traces, "_max_contributions", 1)
    result = await base.execute(ctx, _command("keyword", True))
    assert result.results and result.trace_id is None
    assert base.traces._store.page("alpha", limit=10, cursor=None, now=datetime.now(UTC)) == []


@pytest.mark.asyncio
async def test_trace_payload_cap_drops_record(gateway, monkeypatch):
    base, ctx = gateway
    monkeypatch.setattr(base.traces._store, "max_payload_bytes", 32)
    result = await base.execute(ctx, _command("keyword", True))
    assert result.results and result.trace_id is None and base.traces.dropped == 1
    assert base.traces._store.page("alpha", limit=10, cursor=None, now=datetime.now(UTC)) == []


@pytest.mark.asyncio
async def test_trace_concurrent_insert_cap_soft(tmp_path):
    store = TracesStore(tmp_path, ttl_seconds=30, max_per_profile=1, max_payload_bytes=4096)
    now = datetime(2026, 1, 1, tzinfo=UTC)

    async def insert(ident):
        return await asyncio.to_thread(store.insert, _trace(ident, created=now), now=now)

    results = await asyncio.gather(insert("a"), insert("b"))
    assert len(results) == 2
    with sqlite3.connect(tmp_path / "traces.sqlite3") as db:
        assert db.execute("select count(*) from traces where profile_id='alpha'").fetchone()[0] <= 1


@pytest.mark.asyncio
async def test_trace_http_list_empty(tmp_path):
    from tests.gateway.test_http_retrieval import http_request, live_server

    async with live_server(tmp_path) as (_holder, port, auth):
        status, page = await http_request(port, "GET", "/api/v1/profiles/alpha/retrieval/traces?limit=10", headers=auth)
        assert status == 200, page
        assert page["items"] == [] and page["next_cursor"] is None and not page["has_more"]
