"""S09A production TCP/Guard/lease/SQLite regression boundaries."""

from __future__ import annotations

import asyncio
import base64
import importlib
import importlib.util
import json
import logging
import sqlite3
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from urllib.parse import urlencode

import pytest
from sqlalchemy import text
from storage.repositories.relation_repo import RelationRepository

from memory_server.gateway_contracts import dto as d
from tests.gateway.test_http_retrieval import http_request, live_server, read_scope
from tests.gateway.test_slice_runtime import seed_fact

F = "00000000-0000-4000-8000-000000000001"
G = "00000000-0000-4000-8000-000000000002"
H = "00000000-0000-4000-8000-000000000003"
BASIC = (
    "rel_WyIwMDAwMDAwMC0wMDAwLTQwMDAtODAwMC0wMDAwMDAwMDAwMDEi"
    "LCIwMDAwMDAwMC0wMDAwLTQwMDAtODAwMC0wMDAwMDAwMDAwMDIiLCJzdXBwb3J0cyJd"
)
CANARY = "S09_SECRET_subject_task_query_predicate_canary"


def rel_id(source, target, kind="supports"):
    return "rel_" + base64.urlsafe_b64encode(
        json.dumps([source, target, kind], ensure_ascii=False, separators=(",", ":")).encode()
    ).decode().rstrip("=")


async def relation_auth(holder, *, profiles=("alpha", "beta"), permissions=("relation.read",), subject="s09-reader"):
    now = datetime.now(UTC)
    created = holder.app.dependencies.tokens._bootstrap_create(
        d.TokenCreate(
            subject=subject, profiles=list(profiles), permissions=list(permissions), expires_at=now + timedelta(hours=1)
        ),
        now=now,
    )
    return {"Authorization": f"Bearer cmms_{created.token.id}.{created.secret}"}


async def get(port, auth, suffix, *, profile="alpha"):
    headers = {}
    status, body = await http_request(
        port, "GET", f"/api/v1/profiles/{profile}/" + suffix, headers=auth, response_headers=headers
    )
    assert headers.get("cache-control") == "no-store"
    return status, body


async def seed(holder, auth, *, profile="alpha", ids=(F, G, H), relations=None):
    if relations is None:
        relations = [(F, G, "supports"), (G, F, "contradicts"), (F, F, "derives"), (H, G, "derived_from")]
    async with await read_scope(holder, auth, profile) as scope:
        resources = scope.application.read_resources()
        for ident in ids:
            await seed_fact(resources, ident, CANARY)
        async with resources.session_factory() as session:
            async with session.begin():
                for source, target, kind in relations:
                    await session.execute(
                        text("INSERT INTO claim_relations VALUES (:s,:t,:r,:d)"),
                        dict(s=source, t=target, r=kind, d="2026-10-30 00:00:00.000000"),
                    )
    return resources


def audit_rows(tmp_path):
    with sqlite3.connect(tmp_path / "control/http-access/access.sqlite") as connection:
        return connection.execute("SELECT * FROM access_audit").fetchall()


@pytest.mark.asyncio
async def test_s09_six_routes_mount_and_permissions(tmp_path):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder)
        await seed(holder, auth)
        for route in ("relations", f"memories/{F}/relations", "relations/" + BASIC):
            status, body = await get(port, auth, route)
            assert status == 200, body
        denied = await relation_auth(holder, permissions=("graph.read",))
        for route in ("relations", f"memories/{F}/relations", "relations/" + BASIC):
            status, body = await get(port, denied, route)
            assert (status, body["error"]["code"]) == (403, "E_PERMISSION_DENIED")
        status, body = await get(port, {}, "relations")
        assert (status, body["error"]["code"]) == (401, "E_AUTH_REQUIRED")
        for method, route in (
            ("GET", "graph/neighborhood"),
            ("POST", "graph/path"),
            ("GET", "graph/entity/ent_missing"),
        ):
            status, body = await http_request(port, method, "/api/v1/profiles/alpha/" + route, headers=auth)
            assert (status, body["error"]["code"]) == (404, "E_NOT_FOUND")


@pytest.mark.asyncio
async def test_relation_tuple_id_roundtrip_stable(tmp_path):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder)
        await seed(holder, auth)
        status, row = await get(port, auth, "relations/" + BASIC)
        assert status == 200, row
        assert row == dict(
            id=BASIC,
            source_id=F,
            target_id=G,
            relation_type="supports",
            canonical=True,
            revision=None,
            created_at="2026-10-30T00:00:00Z",
        )
        assert row["canonical"] is True and row["revision"] is None
    assert importlib.util.find_spec("memory_server.application.graph_read_service") is not None
    codec = importlib.import_module("memory_server.application.graph_read_service")
    assert codec.relation_id(F, G, "supports") == BASIC
    assert codec.decode_relation_id(BASIC) == (F, G, "supports")
    assert (
        codec.relation_id("факт-α", "对象", "derived_from")
        == "rel_WyLRhNCw0LrRgi3OsSIsIuWvueixoSIsImRlcml2ZWRfZnJvbSJd"
    )
    assert (
        len(
            {
                codec.relation_id(F, G, "supports"),
                codec.relation_id(G, F, "supports"),
                codec.relation_id(F, G, "contradicts"),
            }
        )
        == 3
    )
    for bad in (
        "rel_@@",
        "rel_W10",
        "rel_eyJ4IjoxfQ",
        "rel_WyJhIiwiYiIsIm93bnMiXQ",
        "rel_WyJhIiwiYiIsInN1cHBvcnRzIl0=",
        "rel_" + "A" * 2045,
        rel_id("x" * 1010, "b"),
        rel_id("ent_bad", "b"),
    ):
        with pytest.raises(Exception) as error:
            codec.decode_relation_id(bad)
        assert error.value.code == "E_VALIDATION"


@pytest.mark.asyncio
async def test_relation_keyset_filters_before_limit(tmp_path):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder)
        resources = await seed(holder, auth)
        async with resources.session_factory() as session:
            async with session.begin():
                # Real collision across canonical namespaces plus orphan/invalid tail.
                await session.execute(
                    text(
                        "INSERT INTO skills(id,name,version,purpose,steps,constraints,validation,success_rate,"
                        "creator,created_at,updated_at,confidence,verification_status,lifecycle_state) "
                        "VALUES (:id,'n','1','p','[]','[]','[]',0,'test',:d,:d,0.5,'candidate','active')"
                    ),
                    dict(id=H, d="2026-10-30 00:00:00.000000"),
                )
                for source, target, kind in [(F, "orphan", "supports"), ("slug", G, "supports"), (G, G, "unknown")]:
                    await session.execute(
                        text("INSERT INTO claim_relations VALUES (:s,:t,:r,:d)"),
                        dict(s=source, t=target, r=kind, d="2026-10-30 00:00:00.000000"),
                    )
        for route in ("relations", f"memories/{F}/relations"):
            found, cursor = [], None
            for _ in range(5):
                query = {"limit": 1}
                if cursor:
                    query["cursor"] = cursor
                status, body = await get(port, auth, route + "?" + urlencode(query))
                assert status == 200, body
                assert len(body["items"]) == 1
                found.extend(item["id"] for item in body["items"])
                assert body["has_more"] is (body["next_cursor"] is not None)
                cursor = body["next_cursor"]
                if not cursor:
                    break
            assert found == [rel_id(F, F, "derives"), BASIC, rel_id(G, F, "contradicts")]
        status, body = await get(port, auth, f"memories/{H}/relations")
        assert (status, body["error"]["code"]) == (422, "E_VALIDATION")


@pytest.mark.asyncio
async def test_relation_cursor_scope_and_validation(tmp_path, monkeypatch):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder)
        await seed(holder, auth)
        status, page = await get(port, auth, "relations?limit=2")
        assert status == 200, page
        cursor = page["next_cursor"]
        assert cursor
        acquisitions = []
        acquire = holder.app.dependencies.leases.acquire

        @asynccontextmanager
        async def watched(ctx):
            acquisitions.append(ctx.profile_id)
            async with acquire(ctx) as scope:
                yield scope

        monkeypatch.setattr(holder.app.dependencies.leases, "acquire", watched)
        other = await relation_auth(holder, subject="other-reader")
        for request_auth, profile, route, value, limit in (
            (auth, "beta", "relations", cursor, 2),
            (other, "alpha", "relations", cursor, 2),
            (auth, "alpha", f"memories/{F}/relations", cursor, 2),
            (auth, "alpha", "relations", cursor + "a", 2),
            (auth, "alpha", "relations", cursor, 3),
        ):
            before = len(audit_rows(tmp_path))
            status, body = await get(
                port, request_auth, route + "?" + urlencode(dict(cursor=value, limit=limit)), profile=profile
            )
            assert (status, body["error"]["code"]) == (400, "E_CURSOR_INVALID")
            assert len(audit_rows(tmp_path)) == before
        assert acquisitions == []
        status, second = await get(port, auth, "relations?" + urlencode(dict(cursor=cursor, limit=1)))
        assert status == 200 and second["has_more"], second
        status, body = await get(port, auth, "relations?" + urlencode(dict(cursor=second["next_cursor"], limit=2)))
        assert (status, body["error"]["code"]) == (400, "E_CURSOR_INVALID")
        spec = next(s for s in holder.app.read_routes if s.operation == "relations.list")
        expiry = datetime.fromisoformat(
            json.loads(base64.urlsafe_b64decode(cursor.split(".")[0] + "==="))["expires_at"]
        )
        monkeypatch.setattr(spec.cursor_codec, "clock", lambda: expiry)
        status, body = await get(port, auth, "relations?" + urlencode(dict(cursor=cursor, limit=1)))
        assert (status, body["error"]["code"]) == (410, "E_CURSOR_EXPIRED")


@pytest.mark.asyncio
async def test_graph_profile_missing_node_matrix(tmp_path):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder)
        await seed(holder, auth, ids=(F, G), relations=[(F, G, "supports")])
        await seed(holder, auth, profile="beta", ids=(F, H), relations=[(F, H, "supports")])
        for route in (
            f"memories/{H}/relations",
            "memories/random/relations",
            "relations/" + rel_id(F, H),
            "relations/" + rel_id(F, "random"),
        ):
            status, body = await get(port, auth, route)
            assert (status, body["error"]["code"]) == (404, "E_NOT_FOUND")
            assert body["error"]["details"] == {}
        status, body = await get(port, auth, "relations")
        assert status == 200 and [row["id"] for row in body["items"]] == [BASIC]
        status, body = await get(port, auth, f"memories/{H}/relations", profile="beta")
        assert status == 200 and body["items"][0]["id"] == rel_id(F, H)
        scoped = await relation_auth(holder, profiles=("alpha",))
        status, body = await get(port, scoped, "relations", profile="beta")
        assert (status, body["error"]["code"]) == (403, "E_PERMISSION_DENIED")


@pytest.mark.asyncio
@pytest.mark.parametrize("query", ["limit=0", "limit=201", "limit=1&limit=2", "limit=+1", "limit=1.0", "query=secret"])
async def test_relations_strict_preintent_validation(tmp_path, query):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder)
        before = audit_rows(tmp_path)
        status, body = await get(port, auth, "relations?" + query)
        assert (status, body["error"]["code"]) == (422, "E_VALIDATION")
        assert audit_rows(tmp_path) == before


@pytest.mark.asyncio
async def test_graph_read_audit_metrics_canaries(tmp_path, caplog):
    caplog.set_level(logging.DEBUG)
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder)
        await seed(holder, auth)
        captured = []
        for route in ("relations", f"memories/{F}/relations", "relations/" + BASIC, "relations/" + rel_id(F, "absent")):
            status, body = await get(port, auth, route)
            assert status in (200, 404)
            captured.append(body)
        status, body = await get(port, auth, "relations?" + urlencode(dict(cursor=CANARY)))
        assert status == 400
        captured.append(body)
        rows = audit_rows(tmp_path)
        assert len(rows) == 4
        with sqlite3.connect(tmp_path / "control/http-access/access.sqlite") as db:
            assert db.execute("SELECT state,result FROM access_audit").fetchall() == [
                ("terminal", "success"),
                ("terminal", "success"),
                ("terminal", "success"),
                ("terminal", "E_NOT_FOUND"),
            ]
        metrics = holder.app.dependencies.metrics
        from prometheus_client import generate_latest

        exposition = generate_latest(metrics._registry).decode()
        decoded_ids = [
            base64.urlsafe_b64decode(item["id"][4:] + "===").decode()
            for response in captured
            for item in response.get("items", [])
        ]
        output = (
            json.dumps(captured)
            + str(decoded_ids)
            + str(rows)
            + str(exposition)
            + "\n".join(record.getMessage() for record in caplog.records)
        )
        assert CANARY not in output
        for forbidden in ("query=", "cursor=", "task=", "subject="):
            assert forbidden not in str(exposition)
        for operation in ("relations.for_memory", "relations.list", "relations.get"):
            assert f'operation="{operation}"' in exposition
        assert 'stage="graph"' not in exposition
        assert F not in str(rows) and G not in str(rows)
        print("CANARY_SWEEP_OK responses audit metrics captured_logs decoded_relation_ids")


@pytest.mark.asyncio
async def test_relation_audit_faults_do_not_bypass_guard(tmp_path, monkeypatch):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder)
        await seed(holder, auth)
        status, body = await get(port, auth, "relations")
        assert status == 200, body
        deps = holder.app.dependencies
        acquired = []
        acquire = deps.leases.acquire

        @asynccontextmanager
        async def watched(ctx):
            acquired.append(ctx.request_id)
            with sqlite3.connect(tmp_path / "control/http-access/access.sqlite") as db:
                assert db.execute(
                    "SELECT state FROM access_audit WHERE request_id=?", (ctx.request_id,)
                ).fetchone() == ("intent",)
            async with acquire(ctx) as scope:
                yield scope

        monkeypatch.setattr(deps.leases, "acquire", watched)

        async def failed(*args, **kwargs):
            raise OSError(CANARY)

        for stage in ("begin", "finish"):
            acquired.clear()
            with monkeypatch.context() as patch:
                patch.setattr(deps.access, stage, failed)
                status, body = await get(port, auth, "relations")
            assert (status, body["error"]["code"]) == (503, "E_AUDIT_UNAVAILABLE")
            assert len(acquired) == (0 if stage == "begin" else 1)
            assert CANARY not in json.dumps(body)


@pytest.mark.asyncio
async def test_graph_mount_closed_descriptors(tmp_path):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder)
        status, body = await get(port, auth, "relations")
        assert status == 200, body
        graph = importlib.import_module("memory_server.transports.http.routes.graph")
        from memory_server.transports.http.app import HttpApp
        from memory_server.transports.http.guards import ReadRouteSpec

        specs = graph.create_graph_read_routes(cursor_codec=graph.RelationCursorCodec(bytes(range(32))))
        assert len(graph.GRAPH_READ_ROUTE_SPECS) == 6 and len(specs) == 3
        first = specs[0]
        for bad in (
            replace(first, parse=lambda *_: None),
            replace(first, validate_result=lambda v: v),
            replace(first, permission="graph.read"),
            replace(first, path="/evil"),
            replace(first, method="POST"),
            replace(first, cursor_codec=None),
            ReadRouteSpec(
                first.operation, first.method, first.path, first.permission, first.parse, first.validate_result
            ),
        ):
            with pytest.raises(ValueError):
                HttpApp(holder.app.config, holder.app.dependencies, read_routes=(bad,))
        with pytest.raises(ValueError):
            HttpApp(holder.app.config, holder.app.dependencies, read_routes=(first, first))


@pytest.mark.asyncio
async def test_graph_snapshot_and_late_response_isolation(tmp_path, monkeypatch):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder)
        await seed(holder, auth)
        entered, release = asyncio.Event(), asyncio.Event()
        original = RelationRepository.read_keyset_page

        async def paused(repo, **kwargs):
            result = await original(repo, **kwargs)
            entered.set()
            await release.wait()
            return result

        monkeypatch.setattr(RelationRepository, "read_keyset_page", paused)
        # A real producer stays under lease until its late SQL result and cleanup settle.
        request = asyncio.create_task(get(port, auth, "relations"))
        await asyncio.wait_for(entered.wait(), 10)
        assert holder.app.dependencies.leases.snapshot()["alpha"].admissions == 1
        release.set()
        status, body = await request
        assert status == 200 and len(body["items"]) == 4
        status, body = await get(port, auth, "relations", profile="beta")
        assert status == 200 and body["items"] == []


@pytest.mark.asyncio
async def test_relation_cursor_golden_schema_and_key_custody(tmp_path):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder, subject="alice")
        status, body = await get(port, auth, "relations")
        assert status == 200, body
        module = importlib.import_module("memory_server.transports.http.routes.graph")
        from storage.repositories.relation_repo import RelationKey

        from memory_server.gateway_contracts.ports import RequestContext

        principal = await holder.app.dependencies.tokens.authenticate(auth["Authorization"][7:], now=datetime.now(UTC))
        principal = replace(principal, profiles=frozenset({"A"}))
        now = datetime(2026, 10, 30, tzinfo=UTC)
        ctx = RequestContext("golden", principal, "A", None, 1)
        command = d.COMMAND_TYPES["relations.list"](
            operation="relations.list", profile_id="A", path_ids={}, payload=d.PageQuery(limit=50)
        )
        codec = module.RelationCursorCodec(bytes(range(32)))
        last = RelationKey("2026-10-30T00:00:00.000000+00:00", F, G, "supports")
        upper = RelationKey("2026-10-30T00:00:01.000000+00:00", G, F, "contradicts")
        token = codec.issue(ctx, command, last=last, upper=upper, limit=50, now=now)
        raw = base64.urlsafe_b64decode(token.split(".")[0] + "===")
        payload = json.loads(raw)
        assert payload["filter_sha256"] == "242db37a50309c5d3371273d7cde25e64ca018b87e9a73ffec04ed10d76b91cf"
        assert codec.validate(principal, command, token, now=now).last == last
        import hmac

        def sign(data):
            encoded = json.dumps(data, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
            part = base64.urlsafe_b64encode(encoded).decode().rstrip("=")
            signature = (
                base64.urlsafe_b64encode(hmac.digest(bytes(range(32)), part.encode(), "sha256")).decode().rstrip("=")
            )
            return part + "." + signature

        assert sign(payload) == token
        for bad in (
            token + "a",
            token.split(".")[0] + ".AA",
            "A" * 8193,
            sign(dict(payload, v=True)),
            sign(dict(payload, limit=True)),
            sign(dict(payload, extra=1)),
            sign(dict(payload, last=["bad", F, G, "supports"])),
            sign(dict(payload, issued_at="2026-10-30T00:00:01+00:00")),
            sign(dict(payload, expires_at="2026-10-30T00:15:01+00:00")),
        ):
            with pytest.raises(Exception) as error:
                codec.validate(principal, command, bad, now=now)
            assert error.value.code == "E_CURSOR_INVALID"
        with pytest.raises(Exception) as error:
            codec.validate(principal, command, token, now=now + timedelta(seconds=900))
        assert error.value.code == "E_CURSOR_EXPIRED"
        path = tmp_path / "cursor" / "relation.key"
        first = module.RelationCursorCodec.from_path(path)
        second = module.RelationCursorCodec.from_path(path)
        issued = first.issue(ctx, command, last=last, upper=upper, limit=50, now=now)
        assert second.validate(principal, command, issued, now=now).upper == upper
        assert path.stat().st_mode & 0o777 == 0o600
        path.chmod(0o644)
        with pytest.raises(Exception) as error:
            module.RelationCursorCodec.from_path(path)
        assert error.value.code == "E_STORAGE"


@pytest.mark.asyncio
async def test_relation_explicit_snapshot_survives_concurrent_delete(tmp_path, monkeypatch):
    from sqlalchemy.ext.asyncio import AsyncSession

    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder)
        resources = await seed(holder, auth, ids=(F, G), relations=[(F, G, "supports")])
        status, body = await get(port, auth, "relations")
        assert status == 200 and len(body["items"]) == 1, body
        selected, continue_read = asyncio.Event(), asyncio.Event()
        original = AsyncSession.execute

        async def paused(session, statement, *args, **kwargs):
            result = await original(session, statement, *args, **kwargs)
            if "INDEXED BY s09_order" in str(statement) and "DESC" in str(statement):
                selected.set()
                await continue_read.wait()
            return result

        monkeypatch.setattr(AsyncSession, "execute", paused)
        request = asyncio.create_task(get(port, auth, "relations"))
        await asyncio.wait_for(selected.wait(), 10)
        try:
            async with resources.session_factory() as session:
                async with session.begin():
                    await session.execute(text("DELETE FROM facts WHERE id=:id"), dict(id=G))
        finally:
            continue_read.set()
        status, body = await request
        assert status == 200 and [item["id"] for item in body["items"]] == [BASIC], body
        status, body = await get(port, auth, "relations")
        assert status == 200 and body["items"] == [], body
