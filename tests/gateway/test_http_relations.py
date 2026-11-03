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
    state = belief_http
    assert (len(state.specs), len(state.mutation_specs), len(state.specs) + len(state.mutation_specs)) == (24, 8, 32)
    assert len(state.app.routes) == 38
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
    assert {spec.operation for spec in state.mutation_specs} == writes
    assert set(state.deps.ports) == {spec.operation for spec in state.specs} | writes


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


@pytest.fixture
async def s16c(belief_http):
    from tests.gateway.test_relation_versions import relations

    return await relations.__wrapped__(belief_http)


async def _s16c_http(state, *, deleted=False, revision=1, key=None, headers=(), body=None, **kwargs):
    from uuid import uuid4

    from tests.gateway.test_http_shell import request
    from tests.gateway.test_relation_versions import command

    cmd = command(deleted=deleted, revision=revision)
    path = "/api/v1/profiles/alpha/relations" + ("/" + cmd.path_ids["id"] if deleted else "")
    return await request(
        state.app,
        path,
        method="DELETE" if deleted else "POST",
        bearer=state.bearer,
        body=cmd.payload.model_dump(mode="json") if body is None else body,
        headers=(("Idempotency-Key", str(key or uuid4())), *headers),
        **kwargs,
    )


async def test_s16c_write_mount_catalog_permissions_and_counts(s16c):
    from memory_server.gateway_contracts.catalog import CATALOG
    from memory_server.transports.http.app import create_http_app
    from memory_server.transports.http.routes.relations_write import create_relation_mutation_routes

    state = s16c
    marker = "S16C_MOUNT_COUNTS"
    assert (len(state.specs), len(state.mutation_specs), len(state.deps.ports), len(state.app.routes)) == (
        24,
        8,
        32,
        38,
    ), marker
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
    assert {s.operation for s in state.mutation_specs} == writes, marker
    for spec in create_relation_mutation_routes():
        row = CATALOG.by_operation[spec.operation]
        assert (row.owner, row.mutation, row.permission) == ("S16", True, "relation.write"), marker
        for changes in (
            {},
            {"method": "GET"},
            {"path": "/bad"},
            {"permission": "memory.write"},
            {"parse": lambda *a: None},
            {"status": lambda r: 200},
        ):
            with pytest.raises(ValueError, match="catalogue"):
                create_http_app(state.config, state.deps, mutation_routes=(replace(spec, **changes),))
        with pytest.raises(ValueError):
            create_http_app(state.config, state.deps, mutation_routes=(spec, spec))
    missing = create_http_app(
        state.config, replace(state.deps, ports={}), mutation_routes=create_relation_mutation_routes()
    )
    for deleted in (False, True):
        response = await _s16c_http(SimpleNamespace(app=missing, bearer=state.bearer), deleted=deleted)
        assert response.status == 503 and response.json["error"]["code"] == "E_CAPABILITY_UNAVAILABLE", marker


@pytest.mark.parametrize(
    "body",
    [b"[]", b"null", b'{"reason":"x","reason":"y"}', b'{"x":NaN}', b"\xff", b'{"expected_versions":{"a":1,"a":2}}'],
)
async def test_s16c_route_parser_abi_and_closed_fields(s16c, body):
    import inspect

    from memory_server.transports.http.routes.relations_write import create_relation_mutation_routes

    for spec in create_relation_mutation_routes():
        assert len(inspect.signature(spec.parse).parameters) == 3, "S16C_PARSE_CLOSED"
        response = await _s16c_http(
            s16c, deleted=spec.operation.endswith("delete"), body=body, headers=(("Content-Type", "application/json"),)
        )
        assert response.status == 422, ("S16C_PARSE_CLOSED", response.body)


def _s16c_headers(case, ident):
    tag = f'"r:{ident}:1"'
    values = {
        "absent": (),
        "matching": (tag,),
        "strong-relation": (tag,),
        "strong-memory": ('"m:belief:other:1"',),
        "weak": ("W/" + tag,),
        "star": ("*",),
        "list": (tag + "," + tag,),
        "duplicate-list": (tag + "," + tag,),
        "repeated-same": (tag, tag),
        "repeated-different": (tag, '"r:other:1"'),
        "empty": ("",),
        "comma": (",",),
        "malformed": ("bad",),
        "revision-mismatch": (f'"r:{ident}:2"',),
        "foreign-id": ('"r:rel_other:1"',),
    }
    return tuple(("If-Match", v) for v in values[case])


@pytest.mark.parametrize(
    "deleted,case",
    [
        (False, c)
        for c in (
            "absent",
            "strong-relation",
            "strong-memory",
            "empty",
            "comma",
            "weak",
            "star",
            "list",
            "duplicate-list",
            "repeated-same",
            "repeated-different",
            "malformed",
        )
    ]
    + [
        (True, c)
        for c in (
            "absent",
            "matching",
            "revision-mismatch",
            "foreign-id",
            "weak",
            "star",
            "list",
            "duplicate-list",
            "repeated-same",
            "repeated-different",
            "empty",
            "comma",
            "malformed",
        )
    ],
)
async def test_s16c_if_match_q4a_and_body_authority(s16c, monkeypatch, deleted, case):
    from tests.gateway.test_relation_versions import command, snapshot

    state = s16c
    if deleted:
        assert (await _s16c_http(state)).status == 201
    ident = command(deleted=True).path_ids["id"]
    allowed = case == "absent" or (deleted and case == "matching")
    before = await snapshot(state)
    audit_before = list(state.deps.access._db.execute("SELECT * FROM access_audit"))

    def forbidden(*a):
        pytest.fail("S16C_IF_MATCH unexpected lease")

    with monkeypatch.context() as m:
        if not allowed:
            m.setattr(type(state.deps.leases), "acquire", forbidden)
        response = await _s16c_http(state, deleted=deleted, headers=_s16c_headers(case, ident))
    marker = "S16C_DELETE_IF_MATCH_STRONG" if deleted else "S16C_CREATE_IF_MATCH_REJECT"
    assert response.status == ((200 if deleted else 201) if allowed else 422), (marker, response.body)
    if not allowed:
        assert await snapshot(state) == before, "S16C_IF_MATCH"
        assert list(state.deps.access._db.execute("SELECT * FROM access_audit")) == audit_before, "S16C_IF_MATCH"


@pytest.mark.parametrize("deleted", [False, True])
@pytest.mark.parametrize("missing", ["map", "endpoint", "revision"])
@pytest.mark.parametrize(
    "case",
    [
        "absent",
        "matching",
        "weak",
        "star",
        "empty",
        "list",
        "repeated-same",
        "repeated-different",
        "foreign-id",
        "malformed",
        "revision-mismatch",
    ],
)
async def test_s16c_missing_cas_428_vs_malformed_422(s16c, monkeypatch, deleted, missing, case):
    from tests.gateway.test_relation_versions import command, snapshot

    cmd = command(deleted=deleted)
    body = cmd.payload.model_dump(mode="json")
    if missing == "map" or (missing == "revision" and not deleted):
        del body["expected_versions"]
    elif missing == "endpoint":
        body["expected_versions"].pop(body["expected_versions"].keys().__iter__().__next__())
    else:
        del body["expected_version"]
    ident = command(deleted=True).path_ids["id"]
    allowed = (
        case == "absent"
        or (deleted and case == "matching")
        or (deleted and missing == "revision" and case == "revision-mismatch")
    )
    before = await snapshot(s16c)

    def forbidden(*a):
        pytest.fail("S16C_HEADER_PRECEDENCE unexpected lease")

    with monkeypatch.context() as m:
        m.setattr(type(s16c.deps.leases), "acquire", forbidden)
        response = await _s16c_http(s16c, deleted=deleted, body=body, headers=_s16c_headers(case, ident))
    assert response.status == (428 if allowed else 422), ("S16C_HEADER_PRECEDENCE", response.body)
    assert response.json["error"]["details"] == {}, "S16C_PRECONDITION"
    assert await snapshot(s16c) == before, "S16C_PRECONDITION"


@pytest.mark.parametrize(
    "case", ["extra", "missing", "bool", "null", "zero", "float", "reason", "actor", "type", "self"]
)
async def test_s16c_path_tuple_and_map_binding(s16c, case):
    from tests.gateway.test_relation_versions import command

    body = command().payload.model_dump(mode="json")
    expected = 422
    if case == "extra":
        body["expected_versions"]["extra"] = 1
    elif case == "missing":
        body["expected_versions"].pop(body["target_id"])
        expected = 428
    elif case in ("bool", "null", "zero", "float"):
        body["expected_versions"][body["source_id"]] = {"bool": True, "null": None, "zero": 0, "float": 1.0}[case]
    elif case == "reason":
        body["reason"] = "   "
    elif case == "actor":
        body["actor"] = "forged"
    elif case == "type":
        body["relation_type"] = "derived_from"
    else:
        body["target_id"] = body["source_id"]
        body["expected_versions"] = {body["source_id"]: 1}
        expected = 201
    response = await _s16c_http(s16c, body=body)
    assert response.status == expected, ("S16C_PATH_BINDING", response.body)


@pytest.mark.parametrize("permission", ["relation.read", "memory.write", "belief.write"])
async def test_s16c_auth_scope_and_replay_reauthentication(s16c, permission):
    from datetime import UTC, datetime, timedelta

    from memory_server.gateway_contracts import dto as d
    from tests.gateway.test_http_shell import request
    from tests.gateway.test_relation_versions import command

    now = datetime.now(UTC)
    token = s16c.deps.tokens._bootstrap_create(
        d.TokenCreate(
            subject="limited", profiles=["alpha"], permissions=[permission], expires_at=now + timedelta(hours=1)
        ),
        now=now,
    )
    body = command().payload.model_dump(mode="json")
    for profile, bearer, expected in (
        ("alpha", None, 401),
        ("alpha", f"cmms_{token.token.id}.{token.secret}", 403),
        ("beta", f"cmms_{token.token.id}.{token.secret}", 403),
    ):
        result = await request(
            s16c.app, f"/api/v1/profiles/{profile}/relations", method="POST", bearer=bearer, body=body
        )
        assert result.status == expected, "S16C_AUTH_SCOPE"


async def test_s16c_foreign_missing_and_ambiguous_404(s16c):
    from tests.gateway.test_http_beliefs import sql
    from tests.gateway.test_relation_versions import command, snapshot

    body = command().payload.model_dump(mode="json")
    await sql(s16c, "UPDATE beliefs SET version=2 WHERE id=:id", {"id": body["source_id"]})
    await sql(s16c, "DELETE FROM beliefs WHERE id=:id", {"id": body["target_id"]})
    before = await snapshot(s16c)
    response = await _s16c_http(s16c, body=body)
    assert response.status == 404 and response.json["error"]["details"] == {}, "S16C_NO_ORACLE"
    assert await snapshot(s16c) == before, "S16C_NO_ORACLE"


async def test_s16c_create_delete_and_worker_readback(s16c):
    from tests.gateway.test_http_beliefs import sql
    from tests.gateway.test_relation_versions import _s16b_graph, _s16b_process, _s16b_queue, _s16b_reopen

    first = await _s16c_http(s16c)
    assert first.status == 201, ("S16C_REAL_WRITE", first.body)
    graph, path = await _s16b_graph(s16c)
    assert (await _s16b_process(s16c, (await _s16b_queue(s16c))[0]))[0] == "completed", "S16C_REAL_WRITE"
    assert _s16b_reopen(path)["edges"], "S16C_REAL_WRITE"
    second = await _s16c_http(s16c, deleted=True)
    assert second.status == 200, ("S16C_REAL_WRITE", second.body)
    assert await sql(s16c, "SELECT revision,deleted FROM relation_revisions ORDER BY revision") == [(1, 0), (2, 1)]
    assert await sql(s16c, "SELECT * FROM claim_relations") == []
    assert len(await sql(s16c, "SELECT id FROM receipts")) == 2
    assert len(await sql(s16c, "SELECT id FROM http_request_journal")) == 2


@pytest.mark.parametrize("case", ["source", "target", "relation", "duplicate", "absent"])
async def test_s16c_endpoint_and_relation_cas_409_atomic(s16c, case):
    from tests.gateway.test_http_beliefs import sql
    from tests.gateway.test_relation_versions import command, snapshot

    deleted = case in ("relation", "absent")
    if case in ("relation", "duplicate"):
        assert (await _s16c_http(s16c)).status == 201
    body = command(deleted=deleted).payload.model_dump(mode="json")
    if case in ("source", "target"):
        await sql(s16c, "UPDATE beliefs SET version=2 WHERE id=:id", {"id": body[case + "_id"]})
    if case == "relation":
        body["expected_version"] = 2
    before = await snapshot(s16c)
    result = await _s16c_http(s16c, deleted=deleted, body=body)
    assert result.status == (404 if case == "absent" else 409), ("S16C_CAS_ATOMIC", result.body)
    assert await snapshot(s16c) == before, "S16C_CAS_ATOMIC"


async def test_s16c_recreate_replay_and_aba(s16c):
    from uuid import uuid4

    from tests.gateway.test_http_beliefs import sql

    create_key, delete_key = uuid4(), uuid4()
    first = await _s16c_http(s16c, key=create_key)
    second = await _s16c_http(s16c, deleted=True, key=delete_key)
    third = await _s16c_http(s16c)
    assert [r.json["relation"]["revision"] for r in (first, second, third)] == [1, 2, 3], "S16C_ABA_REPLAY"
    assert (await _s16c_http(s16c, deleted=True)).status == 409, "S16C_ABA_REPLAY"
    for deleted, key, original in ((False, create_key, first), (True, delete_key, second)):
        result = await _s16c_http(s16c, deleted=deleted, key=key)
        assert result.body == original.body and result.status == original.status, "S16C_ABA_REPLAY"
        assert result.headers["idempotency-replayed"] == "true", "S16C_ABA_REPLAY"
    assert len(await sql(s16c, "SELECT * FROM claim_relations")) == 1, "S16C_ABA_REPLAY"


async def test_s16c_idempotency_digest_snapshot_x10(s16c):
    from uuid import uuid4

    from tests.gateway.test_relation_versions import command, snapshot

    key = uuid4()
    first = await _s16c_http(s16c, key=key)
    before = await snapshot(s16c)
    for _ in range(10):
        result = await _s16c_http(s16c, key=key)
        assert result.status == 201 and result.body == first.body, "S16C_REPLAY_ONCE"
    body = command().payload.model_dump(mode="json")
    body["reason"] += "changed"
    result = await _s16c_http(s16c, key=key, body=body)
    assert result.status == 409 and result.json["error"]["code"] == "E_IDEMPOTENCY_CONFLICT", "S16C_REPLAY_ONCE"
    assert await snapshot(s16c) == before, "S16C_REPLAY_ONCE"


async def test_s16c_direct_port_ticket_and_lease_contract(s16c, monkeypatch):
    from memory_server.application.remote_relations import RemoteRelationsWritePort
    from tests.gateway.test_http_beliefs import expect_error
    from tests.gateway.test_relation_versions import command, fresh, snapshot

    before = await snapshot(s16c)

    def forbidden(*a):
        pytest.fail("S16C_PORT_LEASE unexpected lease")

    with monkeypatch.context() as m:
        m.setattr(type(s16c.deps.leases), "acquire", forbidden)
        port = RemoteRelationsWritePort(
            s16c.deps, ticket_provider=lambda: None, publish_outcome=s16c.deps.access.publish_committed
        )
        with expect_error("E_AUDIT_UNAVAILABLE"):
            await port.execute(fresh(s16c), command())
        with expect_error("E_VALIDATION"):
            await port.execute(fresh(s16c), object())
    assert await snapshot(s16c) == before, "S16C_PORT_LEASE"
    for value in (True, 0, 604801):
        with pytest.raises(ValueError):
            RemoteRelationsWritePort(
                s16c.deps,
                ticket_provider=lambda: "ticket",
                publish_outcome=s16c.deps.access.publish_committed,
                ttl_seconds=value,
            )


async def test_s16c_privacy_all_egress(s16c, caplog):
    from uuid import uuid4

    key = uuid4()
    first = await _s16c_http(s16c, key=key)
    replay = await _s16c_http(s16c, key=key)
    error = await _s16c_http(s16c)
    assert first.status == replay.status == 201 and error.status == 409, "S16C_PRIVACY"
    assert "CANARY" not in str((first.body, replay.body, error.body, caplog.text)), "S16C_PRIVACY"
    assert set(first.json) == {"relation", "deleted", "audit_id", "receipt_ids", "outbox_ids", "request_id"}, (
        "S16C_PRIVACY"
    )


@pytest.mark.parametrize("corruption", ["extra", "nested", "bool", "deleted", "id"])
async def test_s16c_result_status_and_injected_corruption(s16c, corruption):
    from memory_server.gateway_contracts import dto as d
    from memory_server.transports.http.routes.relations_write import relation_status
    from tests.gateway.test_http_beliefs import expect_error

    response = await _s16c_http(s16c)
    assert response.status == 201, "S16C_RESULT_CLOSED"
    result = d.RelationResult.model_validate_json(response.body)
    if corruption == "extra":
        result.__dict__["reason"] = "CANARY"
    elif corruption == "nested":
        result.relation.__dict__["actor"] = "CANARY"
    elif corruption == "bool":
        result.relation.__dict__["revision"] = True
    elif corruption == "deleted":
        result.__dict__["deleted"] = True
    else:
        result.relation.__dict__["id"] = "rel_bad"
    with expect_error("E_INTERNAL"):
        relation_status(result, False)


async def test_s16c_limits_media_csrf_deadline_late_isolation(s16c):
    import asyncio
    from uuid import uuid4

    from memory_server.transports.http.app import create_http_app
    from tests.gateway.test_http_shell import request
    from tests.gateway.test_relation_versions import command, snapshot

    marker = "S16C_NO_COOKIE_AUTH"
    body = command().payload.model_dump(mode="json")
    before = await snapshot(s16c)
    cookie = await request(
        s16c.app,
        "/api/v1/profiles/alpha/relations",
        method="POST",
        body=body,
        headers=(("Cookie", "session=untrusted"), ("Idempotency-Key", str(uuid4()))),
    )
    assert cookie.status == 401 and "set-cookie" not in cookie.headers, marker
    assert await snapshot(s16c) == before, marker
    session = await request(s16c.app, "/api/v1/session", method="POST", bearer=s16c.bearer, body={})
    assert session.status == 404, marker
    for options, expected in (
        (dict(query=b"force=1"), 422),
        (dict(body=b"{}"), 415),
        (dict(body=b"x" * (s16c.config.max_body_bytes + 1)), 413),
    ):
        result = await _s16c_http(s16c, **options)
        assert result.status == expected, ("S16C_LIMITS", result.body)
        assert "set-cookie" not in result.headers, marker

    def app(config):
        return create_http_app(config, s16c.deps, read_routes=s16c.specs, mutation_routes=s16c.mutation_specs)

    limited = SimpleNamespace(app=app(replace(s16c.config, rate_burst=1)), bearer=s16c.bearer)
    assert (await _s16c_http(limited, body={**body, "reason": " "})).status == 422
    # The unauthenticated source budget is also bounded, before parsing/authentication.
    assert (await _s16c_http(limited)).status == 429, "S16C_LIMITS"
    timed = SimpleNamespace(
        app=app(replace(s16c.config, request_timeout_seconds=0.05, header_timeout_seconds=0.01)),
        bearer=s16c.bearer,
    )

    async def stalled_body():
        await asyncio.Event().wait()

    assert (await _s16c_http(timed, receive=stalled_body)).status == 504, "S16C_LIMITS"
    assert await snapshot(s16c) == before, "S16C_LIMITS"
    origin = (("Origin", "https://evil.example"),)
    key = uuid4()
    first = await _s16c_http(s16c, key=key, headers=origin)
    replay = await _s16c_http(s16c, key=key, headers=origin)
    deleted = await _s16c_http(s16c, deleted=True, headers=origin)
    assert (first.status, replay.status, deleted.status) == (201, 201, 200), marker
    assert replay.body == first.body, marker
    for result in (first, replay, deleted):
        assert "set-cookie" not in result.headers, marker
    for suffix in ("", "/" + first.json["relation"]["id"]):
        read = await request(s16c.app, "/api/v1/profiles/alpha/relations" + suffix, bearer=s16c.bearer)
        assert read.status == (200 if not suffix else 404), "S16C_LIMITS"
    beta = await request(s16c.app, "/api/v1/profiles/beta/relations", bearer=s16c.bearer)
    assert beta.status == 200 and beta.json["items"] == [], "S16C_LIMITS"


async def test_s16c_relation_only_self_cycles_and_legacy_parity(s16c):
    from tests.gateway.test_relation_versions import command, snapshot

    before = await snapshot(s16c)
    for source, target, kind in ((1, 2, "supports"), (2, 1, "supports"), (1, 2, "contradicts"), (1, 1, "derives")):
        from tests.gateway.test_http_beliefs import ident

        body = command(source=ident(source), target=ident(target), kind=kind).payload.model_dump(mode="json")
        result = await _s16c_http(s16c, body=body)
        assert result.status == 201, ("S16C_RELATION_ONLY", result.body)
    after = await snapshot(s16c)
    for table in ("beliefs", "facts", "evidence", "lifecycle_states", "lifecycle_events"):
        assert after[table] == before[table], ("S16C_RELATION_ONLY", table)


async def test_s16c_composition_preserves_existing_ports(s16c):
    from memory_server.runtime.read_composition import compose_relation_mutation_ports
    from tests.gateway.test_http_beliefs import expect_error

    state = s16c
    old = {
        k: v
        for k, v in state.deps.ports.items()
        if not k.startswith("relations.") or k in ("relations.list", "relations.get")
    }
    deps, specs = compose_relation_mutation_ports(replace(state.deps, ports=old), state.config)
    assert set(deps.ports) - set(old) == {"relations.create", "relations.delete"}, "S16C_COMPOSE"
    assert set(old) - set(deps.ports) == set(), "S16C_COMPOSE"
    assert all(deps.ports[k] is v for k, v in old.items()), "S16C_COMPOSE"
    port = deps.ports["relations.create"]
    assert port is deps.ports["relations.delete"] and port._ttl == state.config.idempotency_ttl_seconds, "S16C_COMPOSE"
    assert port._events is state.deps.events and port._ticket_provider() is None, "S16C_COMPOSE"
    assert len(specs) == 2, "S16C_COMPOSE"
    with expect_error("E_RUNTIME_NOT_READY"):
        compose_relation_mutation_ports(replace(state.deps, leases=None), state.config)
