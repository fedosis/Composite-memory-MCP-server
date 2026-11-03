"""S16A existing HTTP read compatibility; no relation write routes."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from tests.gateway.test_belief_write_foundation import pause_index_consumer as pause_index_consumer
from tests.gateway.test_graph_reads import BASIC, F, G, relation_auth, seed, seed_projection
from tests.gateway.test_http_beliefs import belief_http as belief_http
from tests.gateway.test_http_beliefs import context
from tests.gateway.test_http_retrieval import http_request, live_server
from tests.gateway.test_slice_runtime import offline_embedder as offline_embedder

pytestmark = pytest.mark.asyncio


async def test_relation_get_etag_and_canonical_revision_readback(tmp_path):
    # Keep the behavioral BASE discriminator before all candidate-only imports.
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder)
        await seed(holder, auth)
        headers = {}
        status, item = await http_request(
            port,
            "GET",
            "/api/v1/profiles/alpha/relations/" + BASIC,
            headers=auth,
            response_headers=headers,
        )
        assert status == 200, item
        assert item["revision"] == 1, "S16A_VIRTUAL_REVISION_ONE"
        assert headers.get("etag") == f'"r:{BASIC}:1"', "S16A_RELATION_ETAG"
        assert headers["cache-control"] == "no-store"
        graph_auth = await relation_auth(holder, permissions=("relation.read", "graph.read"))
        await seed_projection(holder, graph_auth)
        for route in ("relations", f"memories/{F}/relations", f"graph/neighborhood?memory_id={F}"):
            tags = {}
            status, body = await http_request(
                port, "GET", "/api/v1/profiles/alpha/" + route, headers=graph_auth, response_headers=tags
            )
            assert status == 200, body
            assert "etag" not in tags
            values = body.get("items", body.get("edges", []))
            assert values
            assert all(row["revision"] == 1 for row in values if row["canonical"])
            assert all(row["revision"] is None for row in values if not row["canonical"])
        tags = {}
        status, _ = await http_request(
            port, "GET", "/api/v1/profiles/beta/relations/" + BASIC, headers=auth, response_headers=tags
        )
        assert status == 404 and "etag" not in tags
        from datetime import UTC, datetime
        from uuid import uuid4

        from storage.models.relation import ClaimRelationORM
        from storage.repositories.relation_repo import RelationRepository
        from storage.repositories.relation_revision_repo import RelationEventContext, RelationRevisionRepository

        from tests.gateway.test_http_retrieval import read_scope

        async with await read_scope(holder, auth, "alpha") as scope:
            async with scope.application.read_resources().session_factory() as session, session.begin():
                repo = RelationRepository(session)
                row = await session.get(ClaimRelationORM, (F, G, "supports"))
                before = await RelationRevisionRepository(session).state((F, G, "supports"), row.created_at)
                await repo.delete_versioned(
                    (F, G, "supports"),
                    before=before,
                    expected_revision=1,
                    event=RelationEventContext(str(uuid4()), datetime.now(UTC)),
                )
                await repo.create(F, G, "supports")
        tags = {}
        status, body = await http_request(
            port, "GET", "/api/v1/profiles/alpha/relations/" + BASIC, headers=auth, response_headers=tags
        )
        assert status == 200 and body["revision"] == 3
        assert tags["etag"] == f'"r:{BASIC}:3"'


async def test_relation_read_etag_descriptor_identity_is_sealed(belief_http):
    from memory_server.transports.http.app import create_http_app
    from memory_server.transports.http.routes.graph import GraphReadRouteSpec

    state = belief_http
    specs = [s for s in state.specs if type(s) is GraphReadRouteSpec]
    assert len(specs) == 6
    noops = [s.etag for s in specs if s.operation != "relations.get"]
    assert len(noops) == 5 and len(set(noops)) == 1
    for spec in specs:
        create_http_app(state.config, state.deps, read_routes=(spec,))
        for callback in (lambda v: "evil", lambda v: spec.etag(v)):
            with pytest.raises(ValueError):
                create_http_app(state.config, state.deps, read_routes=(replace(spec, etag=callback),))

        class SubSpec(GraphReadRouteSpec):
            pass

        for forged in (SubSpec(**spec.__dict__), SimpleNamespace(**spec.__dict__)):
            with pytest.raises(ValueError):
                create_http_app(state.config, state.deps, read_routes=(forged,))


async def test_relation_read_foundation_keeps_catalog_counts(belief_http):
    from tests.gateway.test_http_shell import request

    state = belief_http
    assert (len(state.specs), len(state.mutation_specs), len(state.specs) + len(state.mutation_specs)) == (24, 6, 30)
    assert len(state.app.routes) == 36
    assert not {"relations.create", "relations.delete"} & set(state.deps.ports)
    for path, method in (("relations", "POST"), ("relations/" + BASIC, "DELETE")):
        response = await request(state.app, "/api/v1/profiles/alpha/" + path, method=method, bearer=state.bearer)
        assert response.status == 405


async def test_relation_read_compatibility_and_privacy(tmp_path, caplog):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder, profiles=("alpha",))
        await seed(holder, auth)
        for profile, expected in (("alpha", 200), ("beta", 403)):
            tags = {}
            status, body = await http_request(
                port, "GET", f"/api/v1/profiles/{profile}/relations/" + BASIC, headers=auth, response_headers=tags
            )
            assert status == expected
            assert tags["cache-control"] == "no-store"
            assert tags["x-content-type-options"] == "nosniff"
            assert "CANARY" not in str(body)
            if status != 200:
                assert "etag" not in tags
        status, body = await http_request(port, "GET", "/api/v1/profiles/alpha/relations?cursor=bad", headers=auth)
        assert status == 400 and body["error"]["code"] == "E_CURSOR_INVALID" and "CANARY" not in str(body)
    assert "S09_SECRET_subject_task_query_predicate_canary" not in caplog.text


async def test_legacy_add_relation_mcp_hermes_parity(belief_http):
    from memory_server.application.contracts import RelationCommand
    from memory_server.application.relation_service import RelationService
    from tests.gateway.test_http_beliefs import ident, orm_seed, sql
    from tests.gateway.test_http_shell import request

    state = belief_http
    await orm_seed(state, [dict(id=ident(1), proposition="legacy"), dict(id=ident(2), proposition="other")])
    async with state.deps.leases.acquire(context(state)) as scope:
        resources = scope.application.read_resources()
        graph = scope.runtime.dependencies.graph
        graph._suspend_persistence = True
        for i in (1, 2):
            graph.add_node(ident(i), "belief", "[MASKED]")

        async def get_graph():
            return graph

        service = RelationService(provider=resources.provider, get_graph=get_graph)
        result = await service.add_relation(
            RelationCommand(source_id=ident(1), target_id=ident(2), relation_type="supports")
        )
        again = await service.add_relation(
            RelationCommand(source_id=ident(1), target_id=ident(2), relation_type="supports")
        )
        assert result == again
        graph._suspend_persistence = False
    assert await sql(state, "SELECT revision,deleted,actor FROM relation_revisions") == [(1, 0, None)]
    response = await request(state.app, "/api/v1/profiles/alpha/beliefs/" + ident(1), bearer=state.bearer)
    assert response.status == 200 and response.headers["etag"] == f'"m:belief:{ident(1)}:1"'


@pytest.mark.parametrize(
    "canonical,revision",
    [
        (True, None),
        (True, "1"),
        (True, 0),
        (True, -1),
        (True, True),
        (True, 9223372036854775808),
        (False, 1),
        (None, None),
        (1, 1),
        ("true", 1),
    ],
)
async def test_graph_result_rejects_corrupt_per_edge_invariant(belief_http, canonical, revision):
    from memory_server.application.graph_read_service import map_consistency, map_graph_result
    from memory_server.gateway_contracts import dto as d
    from tests.gateway.test_http_beliefs import expect_error

    node = d.GraphNode(id=F, kind="belief", label="[MASKED]", lifecycle_state=None, masked=True)
    edge = d.GraphEdge.model_construct(
        id=BASIC,
        source_id=F,
        target_id=F,
        relation_type="supports",
        canonical=canonical,
        canonical_relation_id=BASIC,
        revision=revision,
    )
    with expect_error("E_INTERNAL"):
        map_graph_result(context(belief_http), [node], [edge], [], consistency=map_consistency("unknown"), stop=None)


@pytest.mark.parametrize("revision", [None, "1", True, 0, -1, 9223372036854775808])
async def test_relation_etag_rejects_corrupt_revision(belief_http, revision):
    from datetime import UTC, datetime

    from memory_server.gateway_contracts import dto as d
    from memory_server.transports.http.routes.graph import relation_etag
    from tests.gateway.test_http_beliefs import expect_error

    item = d.Relation.model_construct(
        id=BASIC,
        source_id=F,
        target_id=G,
        relation_type="supports",
        canonical=True,
        revision=revision,
        created_at=datetime.now(UTC),
    )
    with expect_error("E_INTERNAL"):
        relation_etag(item)


async def test_guard_does_not_execute_subclass_etag(belief_http):
    from time import monotonic
    from uuid import uuid4

    from starlette.requests import Request

    from memory_server.transports.http.guards import Guard
    from memory_server.transports.http.routes.graph import GraphReadRouteSpec

    state = belief_http
    auth = await relation_auth(SimpleNamespace(app=SimpleNamespace(dependencies=state.deps)))
    canonical = next(s for s in state.specs if s.operation == "relations.list")
    called = []

    def callback(value):
        called.append(value)
        return "evil"

    class SubSpec(GraphReadRouteSpec):
        pass

    spec = SubSpec(**replace(canonical, etag=callback).__dict__)
    request = Request(
        dict(
            type="http",
            method="GET",
            scheme="http",
            path="/api/v1/profiles/alpha/relations",
            raw_path=b"/api/v1/profiles/alpha/relations",
            query_string=b"",
            path_params={"profile": "alpha"},
            headers=[(b"authorization", auth["Authorization"].encode())],
        )
    )
    response = await Guard(state.config, state.deps).execute(
        request, spec, b"", "req_" + str(uuid4()), monotonic() + 30
    )
    assert response.status_code == 200 and "etag" not in response.headers
    assert called == []


@pytest.mark.parametrize("case,expected", [("create", 201), ("delete", 200), ("missing-cas", 428)])
async def test_s16c_behavioral_red_mount_missing(belief_http, case, expected):
    from datetime import UTC, datetime, timedelta
    from uuid import uuid4

    from storage.repositories.relation_repo import RelationRepository

    from memory_server.gateway_contracts import dto as d
    from tests.gateway.test_http_beliefs import ident, orm_seed
    from tests.gateway.test_http_shell import request

    state = belief_http
    await orm_seed(state, [dict(id=ident(i), proposition="mount") for i in (1, 2)])
    now = datetime.now(UTC)
    token = state.deps.tokens._bootstrap_create(
        d.TokenCreate(
            subject="s16c", profiles=["alpha"], permissions=["relation.write"], expires_at=now + timedelta(hours=1)
        ),
        now=now,
    )
    bearer = f"cmms_{token.token.id}.{token.secret}"
    versions = {ident(1): 1, ident(2): 1}
    path = "/api/v1/profiles/alpha/relations"
    method = "POST"
    body = dict(
        source_id=ident(1), target_id=ident(2), relation_type="supports", expected_versions=versions, reason="mount"
    )
    if case == "delete":
        from memory_server.application.graph_read_service import relation_id

        async with state.deps.leases.acquire(context(state)) as scope:
            async with scope.application.memory.session_factory() as session, session.begin():
                await RelationRepository(session).create(ident(1), ident(2), "supports")
        path += "/" + relation_id(ident(1), ident(2), "supports")
        method = "DELETE"
        body = dict(expected_version=1, expected_versions=versions, reason="mount", confirmation=True)
    elif case == "missing-cas":
        del body["expected_versions"]
    response = await request(
        state.app, path, method=method, bearer=bearer, body=body, headers=(("Idempotency-Key", str(uuid4())),)
    )
    assert response.status == expected, (
        f"S16C_PUBLIC_MOUNT_REQUIRED case={case} actual={response.status} expected={expected} {response.body!r}"
    )
