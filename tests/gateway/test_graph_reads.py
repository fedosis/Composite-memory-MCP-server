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
LEFT_FACT = "00000000-0000-4000-8000-000000000004"
RIGHT_FACT = "00000000-0000-4000-8000-000000000005"
BASIC = (
    "rel_WyIwMDAwMDAwMC0wMDAwLTQwMDAtODAwMC0wMDAwMDAwMDAwMDEi"
    "LCIwMDAwMDAwMC0wMDAwLTQwMDAtODAwMC0wMDAwMDAwMDAwMDIiLCJzdXBwb3J0cyJd"
)
ENT_SUBJECT = "ent_WyIwMDAwMDAwMC0wMDAwLTQwMDAtODAwMC0wMDAwMDAwMDAwMDEiLCJzdWJqZWN0Il0"
ENT_OBJECT = "ent_WyIwMDAwMDAwMC0wMDAwLTQwMDAtODAwMC0wMDAwMDAwMDAwMDEiLCJvYmplY3QiXQ"
CANARY = "S09_SECRET_subject_task_query_predicate_canary"


def rel_id(source, target, kind="supports"):
    return "rel_" + base64.urlsafe_b64encode(
        json.dumps([source, target, kind], ensure_ascii=False, separators=(",", ":")).encode()
    ).decode().rstrip("=")


def ent_id(fact, slot="subject"):
    return "ent_" + base64.urlsafe_b64encode(
        json.dumps([fact, slot], ensure_ascii=False, separators=(",", ":")).encode()
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


async def seed_projection(holder, auth, *, profile="alpha"):
    async with await read_scope(holder, auth, profile) as scope:
        graph = scope.runtime.dependencies.graph
        subject = "slice01b"
        object_name = CANARY
        graph._suspend_persistence = True
        try:
            for ident, name in ((subject, subject), (object_name.lower(), object_name)):
                if graph.get_node(ident) is None:
                    graph.add_node(ident, "entity", name, {"secret": CANARY})
            if graph.get_edge(subject, object_name.lower()) is None:
                graph.add_edge(subject, object_name.lower(), CANARY, {"secret": CANARY})
                graph.add_edge(subject, object_name.lower(), "supports", {"secret": CANARY})
        finally:
            graph._suspend_persistence = False
        return graph


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
            assert status == 200, (route, body)
        denied = await relation_auth(holder, permissions=("graph.read",))
        for route in ("relations", f"memories/{F}/relations", "relations/" + BASIC):
            status, body = await get(port, denied, route)
            assert (status, body["error"]["code"]) == (403, "E_PERMISSION_DENIED")
        status, body = await get(port, {}, "relations")
        assert (status, body["error"]["code"]) == (401, "E_AUTH_REQUIRED")
        graph_auth = denied
        await seed_projection(holder, graph_auth)
        positives = (
            ("GET", f"graph/neighborhood?memory_id={F}", b""),
            (
                "POST",
                "graph/path",
                json.dumps({"source_id": ENT_SUBJECT, "target_id": ENT_OBJECT}).encode(),
            ),
            ("GET", "graph/entity/" + ENT_SUBJECT, b""),
        )
        for method, route, payload in positives:
            request_headers = dict(graph_auth)
            if method == "POST":
                request_headers["Content-Type"] = "application/json"
            status, body = await http_request(
                port, method, "/api/v1/profiles/alpha/" + route, body=payload, headers=request_headers
            )
            assert status == 200, (route, body)
            assert body["graph_consistency"]["projection"] == "unknown"
        for method, route, payload in positives:
            request_headers = dict(auth)
            if method == "POST":
                request_headers["Content-Type"] = "application/json"
            status, body = await http_request(
                port, method, "/api/v1/profiles/alpha/" + route, body=payload, headers=request_headers
            )
            assert (status, body["error"]["code"]) == (403, "E_PERMISSION_DENIED")


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
        graph_auth = await relation_auth(holder, permissions=("graph.read",))
        await seed_projection(holder, graph_auth)
        for method, route, payload in (
            ("GET", f"graph/neighborhood?memory_id={F}", b""),
            ("POST", "graph/path", json.dumps({"source_id": ENT_SUBJECT, "target_id": ENT_OBJECT}).encode()),
            ("GET", "graph/entity/" + ENT_SUBJECT, b""),
        ):
            headers = dict(graph_auth)
            if method == "POST":
                headers["Content-Type"] = "application/json"
            status, body = await http_request(
                port, method, "/api/v1/profiles/alpha/" + route, body=payload, headers=headers
            )
            assert status == 200, body
            captured.append(body)
        rows = audit_rows(tmp_path)
        assert len(rows) == 7
        with sqlite3.connect(tmp_path / "control/http-access/access.sqlite") as db:
            assert db.execute("SELECT state,result FROM access_audit").fetchall() == [
                ("terminal", "success"),
                ("terminal", "success"),
                ("terminal", "success"),
                ("terminal", "E_NOT_FOUND"),
                ("terminal", "success"),
                ("terminal", "success"),
                ("terminal", "success"),
            ]
        metrics = holder.app.dependencies.metrics
        from prometheus_client import generate_latest

        exposition = generate_latest(metrics._registry).decode()
        decoded_ids = [
            base64.urlsafe_b64decode(item["id"][4:] + "===").decode()
            for response in captured
            for item in response.get("items", [])
        ]
        graph_codec = importlib.import_module("memory_server.application.graph_read_service")
        decoded_handles = []

        def decode_handles(value):
            if isinstance(value, dict):
                for child in value.values():
                    decode_handles(child)
            elif isinstance(value, list):
                for child in value:
                    decode_handles(child)
            elif isinstance(value, str) and value.startswith(("ent_", "proj_", "rel_")):
                decoded_handles.append(value)
                if value.startswith("ent_"):
                    decoded = list(graph_codec.decode_entity_id(value))
                elif value.startswith("proj_"):
                    decoded = list(graph_codec.decode_projection_id(value))
                else:
                    decoded = list(graph_codec.decode_relation_id(value))
                decoded_handles.append(json.dumps(decoded))
                decode_handles(decoded)

        decode_handles(captured)
        assert all(any(item.startswith(prefix) for item in decoded_handles) for prefix in ("ent_", "proj_", "rel_"))
        output = (
            json.dumps(captured)
            + str(decoded_ids)
            + str(decoded_handles)
            + str(rows)
            + str(exposition)
            + "\n".join(record.getMessage() for record in caplog.records)
        )
        assert CANARY not in output
        assert CANARY.lower() not in output and "slice01b" not in output
        for forbidden in ("query=", "cursor=", "task=", "subject="):
            assert forbidden not in str(exposition)
        for operation in (
            "relations.for_memory",
            "relations.list",
            "relations.get",
            "graph.neighborhood",
            "graph.path",
            "graph.entity",
        ):
            assert f'operation="{operation}"' in exposition
        assert 'stage="graph"' in exposition
        assert F not in str(rows) and G not in str(rows)
        print("CANARY_SWEEP_OK responses audit metrics captured_logs decoded_ent_proj_rel_recursive")


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
        assert len(graph.GRAPH_READ_ROUTE_SPECS) == 6 and len(specs) == 6
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
async def test_graph_canonical_vs_projection(tmp_path):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder, permissions=("graph.read", "relation.read"))
        await seed(holder, auth, ids=(F, G), relations=[(F, G, "supports")])
        await seed_projection(holder, auth)
        status, body = await get(port, auth, f"graph/neighborhood?memory_id={F}&depth=2")
        assert status == 200, body
        assert {node["id"] for node in body["nodes"]} >= {F, G, ENT_SUBJECT, ENT_OBJECT}
        canonical = [edge for edge in body["edges"] if edge["canonical"]]
        projected = [edge for edge in body["edges"] if not edge["canonical"]]
        assert len(canonical) == 1 and canonical[0]["canonical_relation_id"] == BASIC
        assert len(projected) == 1
        assert projected[0]["relation_type"] == "[MASKED]"
        assert projected[0]["canonical_relation_id"] is None and projected[0]["revision"] is None
        assert CANARY not in json.dumps(body)
        status, hidden = await get(
            port,
            auth,
            "graph/neighborhood?" + urlencode({"entity_id": ENT_SUBJECT, "relation_types": CANARY}),
        )
        assert status == 200 and hidden["edges"] == [], hidden
        status, masked = await get(
            port,
            auth,
            "graph/neighborhood?" + urlencode({"entity_id": ENT_SUBJECT, "relation_types": "[MASKED]"}),
        )
        assert status == 200 and len(masked["edges"]) == 1, masked


@pytest.mark.asyncio
async def test_entity_unknown_and_masked_namespace(tmp_path):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder, permissions=("graph.read",))
        await seed(holder, auth, ids=(F,), relations=[])
        await seed_projection(holder, auth)
        status, body = await get(port, auth, "graph/entity/" + ENT_SUBJECT)
        assert status == 200, body
        assert {node["id"] for node in body["nodes"]} == {ENT_SUBJECT, ENT_OBJECT}
        assert all(node["masked"] and node["label"] == "[MASKED]" for node in body["nodes"])
        assert len(body["edges"]) == 1 and body["edges"][0]["relation_type"] == "[MASKED]"
        assert CANARY not in json.dumps(body)
        status, body = await get(port, auth, "graph/entity/" + ENT_SUBJECT, profile="beta")
        assert (status, body["error"]["code"], body["error"]["details"]) == (404, "E_NOT_FOUND", {})
        async with await read_scope(holder, auth) as scope:
            resources = scope.application.read_resources()
            async with resources.session_factory() as session, session.begin():
                await session.execute(
                    text(
                        "INSERT INTO skills(id,name,version,purpose,steps,constraints,validation,success_rate,"
                        "creator,created_at,updated_at,confidence,verification_status,lifecycle_state) "
                        "VALUES (:id,'n','1','p','[]','[]','[]',0,'test',:d,:d,0.5,'candidate','active')"
                    ),
                    dict(id=F, d="2026-10-30 00:00:00.000000"),
                )
        status, body = await get(port, auth, "graph/entity/" + ENT_SUBJECT)
        assert (status, body["error"]["code"]) == (404, "E_NOT_FOUND")


@pytest.mark.asyncio
async def test_graph_paths_direction_diamond_and_self(tmp_path):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder, permissions=("graph.read",))
        resources = await seed(holder, auth, ids=(F,), relations=[])
        async with resources.session_factory() as session:
            async with session.begin():
                await session.execute(
                    text(
                        "INSERT INTO facts(id,subject,predicate,object,dedup_key,confidence,creator,"
                        "created_at,updated_at,verification_status,lifecycle_state,version) VALUES "
                        "(:id,:subject,'hidden','unused',:id,0.8,'test','2026-10-30 00:00:00.000000',"
                        "'2026-10-30 00:00:00.000000','candidate','active','1')"
                    ),
                    [dict(id=LEFT_FACT, subject="Left"), dict(id=RIGHT_FACT, subject="Right")],
                )
        graph = await seed_projection(holder, auth)
        graph._suspend_persistence = True
        try:
            for ident, name in (("left", "Left"), ("right", "Right")):
                graph.add_node(ident, "entity", name)
            graph.add_edge("slice01b", "left", "hidden")
            graph.add_edge("slice01b", "right", "hidden")
            graph.add_edge("left", CANARY.lower(), "hidden")
            graph.add_edge("right", CANARY.lower(), "hidden")
        finally:
            graph._suspend_persistence = False

        async def path(source, target):
            payload = json.dumps({"source_id": source, "target_id": target}).encode()
            headers = {**auth, "Content-Type": "application/json"}
            return await http_request(port, "POST", "/api/v1/profiles/alpha/graph/path", payload, headers)

        status, body = await path(ENT_SUBJECT, ENT_OBJECT)
        assert status == 200, body
        assert len(body["paths"]) == 3
        assert body["paths"][0] == [ENT_SUBJECT, ENT_OBJECT]
        assert {tuple(path) for path in body["paths"][1:]} == {
            (ENT_SUBJECT, ent_id(LEFT_FACT), ENT_OBJECT),
            (ENT_SUBJECT, ent_id(RIGHT_FACT), ENT_OBJECT),
        }
        status, reverse = await path(ENT_OBJECT, ENT_SUBJECT)
        assert status == 200 and reverse["paths"] == []
        status, same = await path(ENT_SUBJECT, ENT_SUBJECT)
        assert status == 200 and same["paths"] == [[ENT_SUBJECT]]


@pytest.mark.asyncio
async def test_graph_strict_validation_and_missing_witness(tmp_path):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder, permissions=("graph.read",))
        before = audit_rows(tmp_path)
        for route in (
            "graph/neighborhood",
            "graph/neighborhood?memory_id=x&entity_id=" + ENT_SUBJECT,
            "graph/neighborhood?memory_id=x&depth=0",
            "graph/entity/ent_@@",
        ):
            status, body = await get(port, auth, route)
            assert (status, body["error"]["code"]) == (422, "E_VALIDATION")
        assert audit_rows(tmp_path) == before
        await seed(holder, auth, ids=(F,), relations=[])
        status, body = await get(port, auth, "graph/entity/" + ENT_SUBJECT)
        assert (status, body["error"]["code"]) == (404, "E_NOT_FOUND")


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
        with pytest.raises(Exception) as expired_issue:
            codec.issue(ctx, command, last=last, upper=upper, limit=50, now=now, expires_at=now)
        assert getattr(expired_issue.value, "code", None) == "E_CURSOR_EXPIRED"
        for bad in (
            token + "a",
            token.split(".")[0] + ".AA",
            "A" * 8193,
            sign(dict(payload, v=True)),
            sign(dict(payload, limit=True)),
            sign(dict(payload, extra=1)),
            sign(dict(payload, last=["bad", F, G, "supports"])),
            sign(dict(payload, issued_at="2026-10-30T00:00:01+00:00")),
            sign(dict(payload, issued_at="0001-01-01T00:00:00+01:00")),
            sign(dict(payload, expires_at="2026-10-30T00:15:01+00:00")),
        ):
            with pytest.raises(Exception) as error:
                codec.validate(principal, command, bad, now=now)
            assert getattr(error.value, "code", None) == "E_CURSOR_INVALID"
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


@pytest.mark.asyncio
async def test_graph_filters_before_response_quotas(tmp_path):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder, permissions=("graph.read",))
        resources = await seed(holder, auth, ids=(F, G), relations=[(F, F, "contradicts"), (F, G, "supports")])
        async with resources.session_factory() as session, session.begin():
            await session.execute(
                text("INSERT INTO claim_relations VALUES (:s,:t,:r,:d)"),
                [
                    dict(s=F, t=f"noise-{i:03}", r="unrelated", d="2026-10-30 00:00:00.000000")
                    for i in range(20)
                ],
            )
        status, body = await get(
            port,
            auth,
            "graph/neighborhood?" + urlencode({"memory_id": F, "relation_types": "supports", "edge_limit": 1}),
        )
        assert status == 200, body
        assert [edge["relation_type"] for edge in body["edges"]] == ["supports"]
        assert body["truncated"] is False


@pytest.mark.asyncio
async def test_graph_consistency_absence_lag_unknown(tmp_path):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder, permissions=("graph.read",))
        await seed(holder, auth, ids=(F, G), relations=[(F, G, "supports")])
        status, body = await get(port, auth, f"graph/neighborhood?memory_id={F}")
        assert status == 200, body
        consistency = body["graph_consistency"]
        assert consistency["projection"] == "unknown"
        assert all(consistency[field] is None for field in (
            "outbox_lag", "last_sync", "missing_projection", "extra_projection", "canonical_edges", "projected_edges",
        ))
        async with await read_scope(holder, auth) as scope:
            previous = scope.runtime.dependencies.graph
            object.__setattr__(scope.runtime.dependencies, "graph", None)
            try:
                status, absent = await get(port, auth, f"graph/neighborhood?memory_id={F}")
                assert status == 200, absent
                assert absent["graph_consistency"] == {**consistency, "projection": "unavailable"}
                status, entity = await get(port, auth, "graph/entity/" + ENT_SUBJECT)
                assert status == 503, entity
                assert entity["error"]["code"] == "E_CAPABILITY_UNAVAILABLE"
            finally:
                object.__setattr__(scope.runtime.dependencies, "graph", previous)


@pytest.mark.asyncio
async def test_node_cap_independent(tmp_path):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder, permissions=("graph.read",))
        await seed(holder, auth, ids=(F, G, H), relations=[(F, G, "supports"), (G, H, "supports")])
        status, body = await get(port, auth, f"graph/neighborhood?memory_id={F}&limit=1&edge_limit=200")
        assert status == 200, body
        assert len(body["nodes"]) == 1 and body["truncation_reason"] == "nodes"


@pytest.mark.asyncio
async def test_edge_cap_independent(tmp_path):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder, permissions=("graph.read",))
        await seed(holder, auth, ids=(F, G, H), relations=[(F, G, "supports"), (F, H, "contradicts")])
        status, body = await get(port, auth, f"graph/neighborhood?memory_id={F}&limit=100&edge_limit=1")
        assert status == 200, body
        assert len(body["edges"]) == 1 and body["truncation_reason"] == "edges"


@pytest.mark.asyncio
async def test_depth_cap_independent(tmp_path):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder, permissions=("graph.read",))
        await seed(holder, auth, ids=(F, G, H), relations=[(F, G, "supports"), (G, H, "supports")])
        status, body = await get(port, auth, f"graph/neighborhood?memory_id={F}&depth=1&limit=100&edge_limit=200")
        assert status == 200, body
        assert body["truncation_reason"] == "depth"
        assert {node["id"] for node in body["nodes"]} == {F, G}
        assert {edge["canonical_relation_id"] for edge in body["edges"]} == {BASIC}


@pytest.mark.asyncio
async def test_filtered_work_and_path_amplification(tmp_path):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder, permissions=("graph.read",))
        resources = await seed(holder, auth, ids=(F, G), relations=[(F, G, "supports")])
        async with resources.session_factory() as session, session.begin():
            await session.execute(
                text("INSERT INTO claim_relations VALUES (:s,:t,:r,:d)"),
                [dict(s=F, t=f"filtered-{i:03}", r="ignored", d="2026-10-30 00:00:00.000000") for i in range(101)],
            )
        status, body = await get(
            port, auth,
            "graph/neighborhood?" + urlencode({"memory_id": F, "relation_types": "supports", "edge_limit": 1}),
        )
        assert status == 200, body
        assert [edge["relation_type"] for edge in body["edges"]] == ["supports"]
        # Dense canonical DAG: few nodes/edges, exponentially many path states.
        ids = [f"00000000-0000-4000-8000-{i + 300:012d}" for i in range(9)]
        await seed(holder, auth, ids=ids, relations=[
            (source, target, "supports") for i, source in enumerate(ids) for target in ids[i + 1:]
        ])
        status, body = await http_request(
            port, "POST", "/api/v1/profiles/alpha/graph/path",
            body=json.dumps({"source_id": ids[0], "target_id": ids[-1], "max_depth": 4}).encode(),
            headers={**auth, "Content-Type": "application/json"},
        )
        assert status == 503, body
        assert body["error"]["code"] == "E_GRAPH_BACKEND"


@pytest.mark.asyncio
async def test_c1_canonical_f_g_path_seed_projection_positive(tmp_path):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder, permissions=("graph.read",))
        await seed(holder, auth, ids=(F, G), relations=[(F, G, "supports")])
        await seed_projection(holder, auth)
        payload = json.dumps({"source_id": F, "target_id": G, "max_depth": 2}).encode()
        status, body = await http_request(
            port, "POST", "/api/v1/profiles/alpha/graph/path", body=payload,
            headers={**auth, "Content-Type": "application/json"},
        )
        assert status == 200, body
        assert body["paths"] == [[F, G]]
        assert {edge["canonical_relation_id"] for edge in body["edges"]} == {BASIC}


@pytest.mark.asyncio
async def test_soft_native_stop_preserves_root_prefix_priority(tmp_path, monkeypatch):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder, permissions=("graph.read",))
        await seed(holder, auth, ids=(F, G), relations=[(F, G, "supports")])
        graph = await seed_projection(holder, auth)
        original = graph.read_window

        def stopped(*args, **kwargs):
            return replace(original(*args, **kwargs), soft_stopped=True)

        monkeypatch.setattr(graph, "read_window", stopped)
        for limits in ("", "&limit=1&edge_limit=1&depth=1"):
            status, body = await get(port, auth, f"graph/neighborhood?memory_id={F}" + limits)
            assert status == 200, body
            assert body["nodes"] and body["nodes"][0]["id"] == F
            assert body["truncated"] is True and body["truncation_reason"] == "timeout"


@pytest.mark.asyncio
async def test_graph_unavailable_is_bound_from_scoped_runtime(tmp_path, monkeypatch):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder, permissions=("graph.read",))
        await seed(holder, auth, ids=(F, G), relations=[(F, G, "supports")])
        async with await read_scope(holder, auth) as scope:
            previous = scope.runtime.dependencies.graph
            object.__setattr__(scope.runtime.dependencies, "graph", None)
            try:
                status, body = await get(port, auth, f"graph/neighborhood?memory_id={F}")
            finally:
                object.__setattr__(scope.runtime.dependencies, "graph", previous)
        assert status == 200, body
        assert body["graph_consistency"]["projection"] == "unavailable"


@pytest.mark.asyncio
async def test_path_quota_retains_source_and_target_before_middle(tmp_path):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder, permissions=("graph.read",))
        await seed(holder, auth, ids=(F, G, H), relations=[(H, G, "supports"), (G, F, "supports")])
        payload = json.dumps({"source_id": H, "target_id": F, "max_depth": 3, "node_limit": 2}).encode()
        status, body = await http_request(
            port, "POST", "/api/v1/profiles/alpha/graph/path", body=payload,
            headers={**auth, "Content-Type": "application/json"},
        )
        assert status == 200, body
        assert {node["id"] for node in body["nodes"]} == {H, F}
        assert body["paths"] == [] and body["truncation_reason"] == "nodes"


@pytest.mark.asyncio
async def test_soft_native_stop_without_validated_entity_prefix_is_504(tmp_path, monkeypatch):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder, permissions=("graph.read",))
        await seed(holder, auth, ids=(F, G), relations=[(F, G, "supports")])
        graph = await seed_projection(holder, auth)
        original = graph.read_window

        def stop_before_admission(*args, **kwargs):
            # Exercise the real native checkpoint before any node is copied.
            kwargs["clock"] = lambda: kwargs["soft_deadline"]
            result = original(*args, **kwargs)
            assert result.soft_stopped and not result.nodes
            return result

        monkeypatch.setattr(graph, "read_window", stop_before_admission)
        status, body = await get(port, auth, "graph/entity/" + ENT_SUBJECT)
        assert status == 504, body
        assert body["error"]["code"] == "E_TIMEOUT"
        status, body = await get(port, auth, f"graph/neighborhood?memory_id={F}")
        assert status == 200, body
        assert body["nodes"][0]["id"] == F
        assert body["truncation_reason"] == "timeout"
