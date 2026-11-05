"""S15A public-daemon read regression discriminators."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from time import monotonic
from types import SimpleNamespace

import pytest

from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import HttpConfig, RequestContext
from memory_server.runtime.daemon_cli import run_daemon
from memory_server.transports.http.app import create_http_app
from tests.gateway.test_http_shell import request
from tests.gateway.test_slice_runtime import build_registry
from tests.gateway.test_slice_runtime import offline_embedder as offline_embedder

READS = {"beliefs.list", "beliefs.get", "conflicts.list"}
WRITES = {
    "memories.create",
    "learn",
    "memories.transition",
    "memories.invalidate",
    "beliefs.set",
    "conflicts.resolve",
    "relations.create",
    "relations.delete",
}
IDENT = "00000000-0000-4000-8000-000000000001"


@pytest.fixture
async def belief_http(tmp_path):
    registry, roots = await build_registry(tmp_path)
    config = HttpConfig(control_path=tmp_path / "control" / "control.sqlite", rate_burst=200)
    state = SimpleNamespace(config=config, roots=roots, specs=(), mutation_specs=())
    ready, stop = asyncio.Event(), asyncio.Event()

    def factory(config, deps, *, read_routes, mutation_routes):
        state.specs, state.mutation_specs = read_routes, mutation_routes
        state.deps = deps
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
        assert waiter in done, "daemon did not compose"
        now = datetime.now(UTC)
        created = state.deps.tokens._bootstrap_create(
            d.TokenCreate(
                subject="s15a",
                profiles=["alpha", "beta"],
                permissions=["belief.read", "memory.read"],
                expires_at=now + timedelta(hours=1),
            ),
            now=now,
        )
        state.bearer = f"cmms_{created.token.id}.{created.secret}"
        state.principal = await state.deps.tokens.authenticate(state.bearer, now=now)
        yield state
    finally:
        stop.set()
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)
        assert await asyncio.wait_for(task, 20) == 0


def context(state, profile="alpha"):
    return RequestContext("seed-s15a", state.principal, profile, None, monotonic() + 30)


async def native_seed(state, profile, proposition):
    from memory_server.application.belief_service import BeliefService
    from memory_server.application.contracts import SetBeliefCommand

    async with state.deps.leases.acquire(context(state, profile)) as scope:
        provider = scope.application.read_resources().provider
        return await BeliefService(provider).set_belief(SetBeliefCommand(proposition=proposition))


async def test_beliefs_get_by_id_is_not_text_search(belief_http):
    state = belief_http
    seeded = await native_seed(state, "alpha", "S15A_ALPHA_PROPOSITION_CANARY")
    ident = seeded["belief"]["id"]
    # Explicit ORM fixture: equal UUID in separate stores, divergent versions/content.
    from sqlalchemy import text
    from storage.models.belief import BeliefORM

    async with state.deps.leases.acquire(context(state, "beta")) as scope:
        async with scope.application.read_resources().session_factory() as session, session.begin():
            session.add(
                BeliefORM(
                    id=ident, proposition="S15A_BETA_CANARY", confidence=0.7, source="test", creator="test", version=9
                )
            )
    async with state.deps.leases.acquire(context(state)) as scope:
        async with scope.application.read_resources().session_factory() as session, session.begin():
            await session.execute(text("UPDATE beliefs SET version=3 WHERE id=:id"), {"id": ident})
    response = await request(state.app, f"/api/v1/profiles/alpha/beliefs/{ident}", bearer=state.bearer)
    assert response.status == 200, response.body
    assert response.json["id"] == ident
    assert response.json["revision"] == response.json["source_version"] == 3
    assert response.headers["etag"] == f'"m:belief:{ident}:3"'
    assert response.json["masked"] and response.json["payload"] is None
    foreign = await request(state.app, f"/api/v1/profiles/beta/beliefs/{ident}", bearer=state.bearer)
    assert foreign.status == 200 and foreign.json["revision"] == 9


async def test_belief_composition_regression_counts(belief_http):
    state = belief_http
    assert (len(state.specs), len(state.mutation_specs), len(state.specs) + len(state.mutation_specs)) == (26, 8, 34)
    assert len(state.app.routes) == 40
    reads = {spec.operation for spec in state.specs}
    assert READS <= reads
    assert {spec.operation for spec in state.mutation_specs} == WRITES
    assert set(state.deps.ports) == reads | WRITES | {"jobs.get"}
    port = state.deps.ports["beliefs.set"]
    assert port is state.deps.ports["conflicts.resolve"]
    assert all(port is not state.deps.ports[op] for op in (READS | WRITES) - {"beliefs.set", "conflicts.resolve"})


async def orm_seed(state, rows, profile="alpha"):
    """Explicit synthetic ORM fixtures for ties/corruption/frontier bounds."""
    from storage.models.belief import BeliefORM

    async with state.deps.leases.acquire(context(state, profile)) as scope:
        async with scope.application.read_resources().session_factory() as session, session.begin():
            session.add_all([BeliefORM(**row) for row in rows])


def ident(number):
    return f"00000000-0000-4000-8000-{number:012x}"


async def sql(state, statement, params=None, profile="alpha"):
    from sqlalchemy import text

    async with state.deps.leases.acquire(context(state, profile)) as scope:
        async with scope.application.read_resources().session_factory() as session, session.begin():
            result = await session.execute(text(statement), params or {})
            return [tuple(row) for row in result] if result.returns_rows else []


async def http(state, suffix="beliefs", *, profile="alpha", **kwargs):
    kwargs.setdefault("bearer", state.bearer)
    return await request(state.app, f"/api/v1/profiles/{profile}/{suffix}", **kwargs)


def command(operation="beliefs.list", *, profile="alpha", **values):
    model = {"beliefs.list": d.BeliefListQuery, "beliefs.get": d.Empty, "conflicts.list": d.PageQuery}[operation]
    if operation == "beliefs.list":
        values = {**dict(cursor=None, proposition=None, source=None, creator=None, source_id=None), **values}
    path = {"id": IDENT} if operation == "beliefs.get" else {}
    return d.COMMAND_TYPES[operation](operation=operation, profile_id=profile, path_ids=path, payload=model(**values))


def expect_error(code):
    from memory_server.transports.http.errors import HttpError

    return pytest.raises(HttpError, match=code)


async def test_belief_surface_mount_catalog_and_permissions(belief_http):
    from dataclasses import replace

    from memory_server.gateway_contracts.catalog import CATALOG
    from memory_server.transports.http.routes.beliefs import BeliefReadRouteSpec

    state = belief_http
    specs = [s for s in state.specs if s.operation in READS]
    assert [s.operation for s in specs] == ["beliefs.list", "beliefs.get", "conflicts.list"]
    for spec in specs:
        row = CATALOG.by_operation[spec.operation]
        assert (row.owner, row.mutation, spec.method, spec.permission) == ("S15", False, "GET", "belief.read")
        for update in (
            {"method": "POST"},
            {"path": "/bad"},
            {"permission": "memory.read"},
            {"parse": lambda *a: None},
            {"validate_result": lambda a: a},
            {"etag": lambda a: "evil"},
        ):
            with pytest.raises(ValueError):
                create_http_app(state.config, state.deps, read_routes=(replace(spec, **update),))
        if spec.operation != "beliefs.get":
            with pytest.raises(ValueError):
                create_http_app(state.config, state.deps, read_routes=(replace(spec, cursor_codec=None),))

        class SubSpec(BeliefReadRouteSpec):
            pass

        with pytest.raises(ValueError):
            create_http_app(state.config, state.deps, read_routes=(SubSpec(**spec.__dict__),))
        with pytest.raises(ValueError):
            create_http_app(state.config, state.deps, read_routes=(spec, spec))
        missing = create_http_app(state.config, replace(state.deps, ports={}), read_routes=(spec,))
        response = await request(
            missing, spec.path.replace("{profile}", "alpha").replace("{id}", IDENT), bearer=state.bearer
        )
        assert response.status == 503
    assert (await http(state, method="POST")).status == 403
    assert (await http(state, "conflicts/conf_x/resolve", method="POST")).status == 403


async def test_belief_list_exact_privacy_filter_matrix(belief_http, monkeypatch):
    from urllib.parse import urlencode

    state = belief_http
    calls = []
    original = type(state.deps.leases).acquire

    def acquire(self, ctx):
        calls.append(ctx.profile_id)
        return original(self, ctx)

    monkeypatch.setattr(type(state.deps.leases), "acquire", acquire)
    for field in ("proposition", "tags", "source", "creator", "source_id"):
        for value in ("", "CANARY", "null", "[]"):
            response = await http(state, query=urlencode({field: value}).encode())
            assert response.status == 403, response.body
            assert response.json["error"]["code"] == "E_PERMISSION_DENIED"
            assert calls == []
        cmd = command()
        cmd.payload.__dict__[field] = [""] if field == "tags" else ""
        with expect_error("E_PERMISSION_DENIED"):
            await state.deps.ports["beliefs.list"].execute(context(state), cmd)
        assert calls == []
    for key in ("risk_tags", "authority_level", "epistemic_status", "unknown"):
        assert (await http(state, query=f"{key}=x".encode())).status == 422
    assert (await http(state, query=b"source=x&limit=bad")).status == 422
    assert calls == []
    for query in (b"", b"limit=1", b"min_confidence=0.5", b"lifecycle_state=contradicted"):
        assert (await http(state, query=query)).status == 200
    assert len(calls) == 4
    assert (await state.deps.ports["beliefs.list"].execute(context(state), command())).items == []


async def test_belief_parser_precedence_and_typed_errors(belief_http, monkeypatch):
    state = belief_http

    def forbidden(*args):
        pytest.fail("invalid request acquired a lease")

    monkeypatch.setattr(type(state.deps.leases), "acquire", forbidden)
    for suffix in ("beliefs", f"beliefs/{IDENT}", "conflicts"):
        for header in ("", "*", 'W/"x"', '"x","y"'):
            assert (await http(state, suffix, headers=(("If-Match", header),))).status == 422
        assert (await http(state, suffix, headers=(("If-Match", ""), ("If-Match", "*")))).status == 422
        for body in (b"x", b"{}"):
            assert (await http(state, suffix, body=body)).status == 422
    for query in (
        b"limit=1&limit=2",
        b"limit=0",
        b"limit=201",
        b"limit=true",
        b"min_confidence=NaN",
        b"min_confidence=Inf",
        b"min_confidence=1e9999",
        b"min_confidence=-1",
        b"lifecycle_state=trusted",
    ):
        assert (await http(state, query=query)).status == 422
    assert (await http(state, query=b"cursor=")).status == 400
    assert (await http(state, "conflicts", query=b"pair_policy=x")).status == 422
    for spelling in (ident(10).upper(), IDENT.replace("-", ""), "{" + IDENT + "}", "not-id"):
        assert (await http(state, "beliefs/" + spelling)).status == 422
    assert (await http(state, f"beliefs/{IDENT}", query=b"limit=1")).status == 422


async def test_belief_negative_auth_permission_profile_matrix(belief_http, monkeypatch):
    state = belief_http

    def forbidden(*args):
        pytest.fail("auth rejection acquired a lease")

    monkeypatch.setattr(type(state.deps.leases), "acquire", forbidden)
    now = datetime.now(UTC)
    for permissions in (["belief.write"], ["belief.resolve"], ["memory.read"]):
        token = state.deps.tokens._bootstrap_create(
            d.TokenCreate(
                subject="denied", profiles=["alpha"], permissions=permissions, expires_at=now + timedelta(hours=1)
            ),
            now=now,
        )
        bearer = f"cmms_{token.token.id}.{token.secret}"
        for suffix in ("beliefs", f"beliefs/{IDENT}", "conflicts"):
            assert (await http(state, suffix, bearer=bearer, query=b"unknown=x")).status == 403
    for bearer in (None, "bad"):
        assert (await http(state, bearer=bearer)).status == 401
    assert (await http(state, profile="foreign")).status == 403


async def test_beliefs_get_etag_stage_local_header_seam(belief_http, monkeypatch):
    from dataclasses import replace

    from memory_server.providers.sqlite_provider import SQLiteProvider
    from memory_server.transports.http.routes.beliefs import belief_etag, validate_belief, validate_beliefs_page

    state = belief_http
    await orm_seed(state, [dict(id=IDENT, proposition="CANARY", version=7)])
    response = await http(state, f"beliefs/{IDENT}")
    assert response.status == 200, response.body
    assert response.headers.get("etag") == f'"m:belief:{IDENT}:7"'
    item = d.Memory.model_validate_json(response.body)
    for invalid in (None, True, "7", 0, -1):
        forged = item.model_copy(update={"revision": invalid})
        with expect_error("E_INTERNAL"):
            belief_etag(forged)
        with expect_error("E_INTERNAL"):
            validate_beliefs_page(d.Page[d.Memory](items=[forged], has_more=False, next_cursor=None, request_id="x"))
    with expect_error("E_INTERNAL"):
        validate_belief(item.model_copy(update={"source_version": 8}))
    assert "etag" not in (await http(state)).headers
    assert "etag" not in (await http(state, "conflicts")).headers
    assert "etag" not in (await http(state, "beliefs/" + ident(999))).headers
    original = SQLiteProvider.read_memory
    for invalid in (None, True, "7", 0, -1):

        async def corrupt(self, *args, bad=invalid, **kwargs):
            record = await original(self, *args, **kwargs)
            return replace(record, memory={**record.memory, "version": bad})

        monkeypatch.setattr(SQLiteProvider, "read_memory", corrupt)
        failed = await http(state, f"beliefs/{IDENT}")
        assert failed.status == 500 and "etag" not in failed.headers


async def test_beliefs_list_keyset_filters_before_limit(belief_http, monkeypatch):
    from urllib.parse import urlencode

    from sqlalchemy.ext.asyncio import AsyncSession

    state = belief_http
    timestamp = datetime(2025, 1, 1)
    await orm_seed(
        state,
        [
            dict(
                id=ident(i),
                proposition=f"fixture {i}",
                created_at=timestamp,
                confidence=0.8 if i % 2 else 0.2,
                lifecycle_state="active" if i < 8 else "contradicted",
            )
            for i in range(1, 10)
        ],
    )
    calls = []
    original = AsyncSession.execute

    async def trace(self, statement, params=None, *args, **kwargs):
        calls.append((str(statement), params))
        return await original(self, statement, params, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "execute", trace)
    seen, cursor = [], None
    while True:
        query = dict(limit=1, min_confidence=0.5)
        if cursor is not None:
            query["cursor"] = cursor
        response = await http(state, query=urlencode(query).encode())
        assert response.status == 200, response.body
        seen += [v["id"] for v in response.json["items"]]
        assert set(response.json) == {"items", "has_more", "next_cursor", "request_id"}
        cursor = response.json["next_cursor"]
        assert response.json["has_more"] == (cursor is not None)
        if cursor is None:
            break
    assert seen == [ident(i) for i in (1, 3, 5, 7)]
    pages = [(s, p) for s, p in calls if "LIMIT" in s and "beliefs" in s and "confidence" in s]
    assert pages and all("ORDER BY" in s and "WHERE" in s for s, p in pages)
    assert any(p and 2 in p.values() for s, p in pages)
    first = await http(state, query=b"limit=1")
    await orm_seed(state, [dict(id=ident(99), proposition="future", created_at=datetime(2026, 1, 1))])
    cursor = first.json["next_cursor"]
    assert (await http(state, query=urlencode(dict(limit=2, cursor=cursor)).encode())).status == 400
    seen = [v["id"] for v in first.json["items"]]
    while cursor:
        response = await http(state, query=urlencode(dict(limit=1, cursor=cursor)).encode())
        assert response.status == 200, response.body
        seen += [v["id"] for v in response.json["items"]]
        cursor = response.json["next_cursor"]
    assert seen == [ident(i) for i in range(1, 8)]


async def test_conflicts_list_scoped_pairs_actions_and_analysis(belief_http, monkeypatch):
    from memory_server.application.remote_beliefs import BeliefsReadHandler, decode_conflict_id
    from memory_server.evaluation.relation import RelationClassifier

    original_model = BeliefsReadHandler._model

    def checked_model(self, record):
        try:
            return original_model(self, record)
        except Exception as exc:
            pytest.fail(f"synthetic classifier hydration failed: {exc}")

    monkeypatch.setattr(BeliefsReadHandler, "_model", checked_model)
    state = belief_http
    await orm_seed(
        state,
        [
            dict(id=ident(1), proposition="Docker is better than Podman", created_at=datetime(2025, 2, 1)),
            dict(id=ident(2), proposition="Docker is worse than Podman", created_at=datetime(2025, 1, 1)),
            dict(id=ident(3), proposition="Apples grow on trees"),
            dict(id=ident(4), proposition="Docker is worse than Podman", lifecycle_state="discarded"),
        ],
    )
    original, calls = RelationClassifier.classify_pair, []

    def classify(self, a, b, **kwargs):
        calls.append((a.id, b.id, kwargs))
        return original(self, a, b, **kwargs)

    monkeypatch.setattr(RelationClassifier, "classify_pair", classify)
    response = await http(state, "conflicts")
    assert response.status == 200, response.body
    assert len(response.json["items"]) == 1
    item = response.json["items"][0]
    assert decode_conflict_id(item["id"]) == (ident(1), ident(2))
    assert item["analysis_complete"] is False and item["kind"] == "contradiction"
    assert len(calls) == 3 and all(
        k == dict(context_a=None, context_b=None, strict_same_context=False) for a, b, k in calls
    )
    assert item["allowed_actions"] == ["keep_a", "keep_b", "merge", "mark_contradicted", "discard_both", "defer"]
    await sql(
        state,
        "INSERT INTO claim_relations (source_id,target_id,relation_type,created_at) "
        "VALUES (:a,:b,'derived_from','2025-01-01 00:00:00')",
        dict(a=ident(1), b=ident(2)),
    )
    response = await http(state, "conflicts")
    assert response.status == 200, response.body
    assert response.json["items"][0]["allowed_actions"] == ["keep_b", "defer"]
    await sql(state, "DELETE FROM claim_relations")
    await sql(state, "UPDATE beliefs SET lifecycle_state='contradicted' WHERE id=:id", {"id": ident(1)})
    response = await http(state, "conflicts")
    assert response.status == 200, response.body
    assert response.json["items"][0]["allowed_actions"] == ["keep_a", "keep_b", "discard_both", "defer"]
    for source, target in ((ident(1), ident(1)), (ident(1), ident(2)), (ident(2), ident(1))):
        await sql(
            state,
            "INSERT INTO claim_relations (source_id,target_id,relation_type,created_at) "
            "VALUES (:a,:b,'derived_from','2025-01-01')",
            dict(a=source, b=target),
        )
    response = await http(state, "conflicts")
    assert response.status == 200 and response.json["items"][0]["allowed_actions"] == ["defer"], response.body
    monkeypatch.setattr(RelationClassifier, "classify_pair", None)
    assert (await http(state, "conflicts")).status == 503

    def broken(*args, **kwargs):
        raise ValueError("CLASSIFIER_CANARY")

    monkeypatch.setattr(RelationClassifier, "classify_pair", broken)
    response = await http(state, "conflicts")
    assert response.status == 500 and b"CLASSIFIER_CANARY" not in response.body


async def test_conflicts_cursor_scope_tamper_expiry_and_work_bound(belief_http):
    import hashlib
    import hmac
    import json
    from dataclasses import replace

    from memory_server.application.memory_read_service import b64, canonical, unb64
    from memory_server.transports.http.routes.beliefs import BeliefCursorCodec

    state = belief_http
    now = datetime(2026, 1, 1, tzinfo=UTC)
    codec = BeliefCursorCodec(b"x" * 32, 60)
    cmd = command("conflicts.list", limit=2)
    pos = ("2025-01-01T00:00:00.000000+00:00", ident(1), "2025-01-01T00:00:00.000000+00:00", ident(2))
    token = codec.issue(context(state), cmd, last=pos, upper=pos, limit=2, now=now)
    payload = json.loads(unb64(token.split(".")[0]))
    assert (
        payload["filter_sha256"]
        == hashlib.sha256(
            canonical(dict(filters={}, path_ids={}, pair_policy="s15a-active-contradicted-contradiction-v1"))
        ).hexdigest()
    )
    assert "pair_policy" not in payload
    assert codec.validate(state.principal, cmd, token, now=now)["last"] == pos

    def sign(value):
        body = b64(canonical(value))
        return body + "." + b64(hmac.digest(b"x" * 32, body.encode(), "sha256"))

    for key, value in (
        ("profile_id", "beta"),
        ("subject", "other"),
        ("operation", "beliefs.list"),
        ("filter_sha256", "bad"),
        ("limit", 1),
        ("v", True),
        ("last", list(reversed(pos))),
    ):
        with expect_error("E_CURSOR_INVALID"):
            codec.validate(state.principal, cmd, sign({**payload, key: value}), now=now + timedelta(seconds=61))
    for bad in (token + "=", token[:-1] + "!", "", token + ".x", token.split(".")[0] + "." + b64(b"\0" * 32)):
        with expect_error("E_CURSOR_INVALID"):
            codec.validate(state.principal, cmd, bad, now=now)
    with expect_error("E_CURSOR_EXPIRED"):
        codec.validate(state.principal, cmd, token, now=now + timedelta(seconds=61))
    with expect_error("E_CURSOR_INVALID"):
        codec.validate(replace(state.principal, subject="other"), cmd, token, now=now)
    with expect_error("E_CURSOR_INVALID"):
        codec.validate(state.principal, command("conflicts.list", limit=3), token, now=now)
    await orm_seed(state, [dict(id=ident(i), proposition="Docker is better than Podman") for i in range(1, 34)])
    response = await http(state, "conflicts", query=b"limit=1")
    assert response.status == 503, response.body
    assert response.json["error"]["details"] == {"limit": 512}


async def test_foreign_conflict_is_404_without_version_oracle(belief_http):
    from memory_server.application.remote_beliefs import decode_conflict_id, encode_conflict_id

    state = belief_http
    await orm_seed(
        state,
        [
            dict(id=ident(10), proposition="Docker is better than Podman"),
            dict(id=ident(11), proposition="Docker is worse than Podman"),
        ],
        profile="beta",
    )
    assert decode_conflict_id(encode_conflict_id(ident(10), ident(11))) == (ident(10), ident(11))
    response = await http(state, "beliefs/" + ident(10))
    assert response.status == 404 and response.json["error"]["details"] == {} and "etag" not in response.headers
    response = await http(state, "conflicts")
    assert response.status == 200 and response.json["items"] == []
    response = await http(state, "conflicts", profile="beta")
    assert response.status == 200 and len(response.json["items"]) == 1, response.body


async def test_belief_privacy_masked_nested_evidence_and_replay(belief_http, caplog):
    state = belief_http
    await orm_seed(
        state,
        [
            dict(
                id=ident(1),
                proposition="Docker is better than Podman",
                source="SOURCE_CANARY",
                creator="CREATOR_CANARY",
                tags=["TAG_CANARY"],
                source_ids=["ID_CANARY"],
            ),
            dict(id=ident(2), proposition="Docker is worse than Podman"),
        ],
    )
    for suffix in ("beliefs", "beliefs/" + ident(1), "conflicts"):
        response = await http(state, suffix)
        assert response.status == 200, response.body
        assert not any(v.encode() in response.body for v in ("Docker", "CANARY"))
        items = response.json.get("items", [response.json])
        for item in items:
            for memory in [item["belief_a"], item["belief_b"]] if "belief_a" in item else [item]:
                assert all(memory[k] is None for k in ("content", "payload", "source", "creator"))
                assert memory["masked"] and memory["risk_tags"] == []
    assert "CANARY" not in caplog.text


async def test_belief_conflict_id_decode_is_canonical():
    from memory_server.application.memory_read_service import b64, canonical
    from memory_server.application.remote_beliefs import decode_conflict_id, encode_conflict_id

    a, b = ident(10), ident(11)
    good = encode_conflict_id(a, b)
    assert decode_conflict_id(good) == (a, b)
    bad = [
        good + "=",
        "x" + good,
        "conf_!",
        "conf_" + b64(b"\xff"),
        "conf_" + b64(canonical([b, a])),
        "conf_" + b64(canonical([a, a])),
        "conf_" + b64(canonical([a, b, a])),
        "conf_" + b64(canonical([a.upper(), b])),
        "conf_" + b64(canonical([a, b]) + b" "),
    ]
    for value in bad:
        with expect_error("E_VALIDATION"):
            decode_conflict_id(value)


async def test_belief_read_direct_port_revalidates_before_lease(belief_http, monkeypatch):
    from dataclasses import replace

    state = belief_http

    def forbidden(*args):
        pytest.fail("invalid direct request acquired lease")

    monkeypatch.setattr(type(state.deps.leases), "acquire", forbidden)
    port = state.deps.ports["beliefs.list"]
    cmd = command()
    for ctx in (replace(context(state), profile_id="beta"), replace(context(state), deadline=True)):
        with expect_error("E_VALIDATION"):
            await port.execute(ctx, cmd)
    with expect_error("E_TIMEOUT"):
        await port.execute(replace(context(state), deadline=0), cmd)
    for value in (True, float("nan"), float("inf")):
        cmd = command()
        cmd.payload.__dict__["min_confidence"] = value
        with expect_error("E_VALIDATION"):
            await port.execute(context(state), cmd)
    cmd = command()
    cmd.path_ids["extra"] = "x"
    with expect_error("E_VALIDATION"):
        await port.execute(context(state), cmd)


async def test_belief_read_sql_budget_cleanup_is_profile_local(belief_http, monkeypatch):
    from sqlalchemy import text

    from memory_server.application.remote_beliefs import BeliefsReadHandler

    state = belief_http
    original = BeliefsReadHandler.list_beliefs
    handlers = []

    async def heavy(self, cmd):
        handlers.append(self)
        await self.session.execute(
            text("WITH RECURSIVE x(n) AS (VALUES(0) UNION ALL SELECT n+1 FROM x WHERE n<10000000) SELECT sum(n) FROM x")
        )
        pytest.fail("VM ceiling did not interrupt SQL")

    monkeypatch.setattr(BeliefsReadHandler, "list_beliefs", heavy)
    response = await http(state)
    assert response.status == 503, response.body
    assert response.json["error"]["details"] == {"limit": 2000000}
    assert handlers[0].instructions == 2000000 and handlers[0].reason == "budget"
    monkeypatch.setattr(BeliefsReadHandler, "list_beliefs", original)
    for profile in ("alpha", "beta", "alpha"):
        response = await http(state, profile=profile)
        assert response.status == 200, response.body


async def test_belief_limits_deadline_media_and_late_response_isolation(belief_http, monkeypatch):
    from dataclasses import replace

    from memory_server.application.remote_beliefs import BeliefsReadHandler

    state = belief_http
    response = await http(state, headers=(("Content-Type", "text/plain"),))
    assert response.status == 200
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["referrer-policy"] == "no-referrer"
    response = await http(state, body=b"x" * (state.config.max_body_bytes + 1))
    assert response.status == 413
    original = BeliefsReadHandler.list_beliefs
    entered = asyncio.Event()

    async def slow(self, cmd):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(BeliefsReadHandler, "list_beliefs", slow)
    with expect_error("E_TIMEOUT"):
        await state.deps.ports["beliefs.list"].execute(replace(context(state), deadline=monotonic() + 0.1), command())
    assert entered.is_set()
    monkeypatch.setattr(BeliefsReadHandler, "list_beliefs", original)
    assert (await http(state, profile="beta")).status == 200
    assert (await http(state)).status == 200


async def test_native_belief_observable_parity(belief_http):
    import inspect
    import subprocess
    from pathlib import Path

    from memory_server.application.belief_service import BeliefService

    state = belief_http
    seeded = await native_seed(state, "alpha", "Docker is better than Podman")
    assert seeded["belief"]["proposition"] == "Docker is better than Podman"
    source = inspect.getsourcefile(BeliefService)
    root = Path(__file__).resolve().parents[2]
    relative = str(Path(source).relative_to(root))
    assert Path(source).read_bytes() == subprocess.check_output(["git", "show", "646b87f:" + relative], cwd=root)


async def test_belief_read_import_graph_and_no_domain_writes(belief_http, monkeypatch):
    import ast
    from pathlib import Path

    from sqlalchemy.ext.asyncio import AsyncSession

    state = belief_http
    await orm_seed(state, [dict(id=IDENT, proposition="CANARY")])
    root = Path(__file__).resolve().parents[2]
    for path in (
        root / "src/memory_server/application/remote_beliefs.py",
        root / "src/memory_server/transports/http/routes/beliefs.py",
    ):
        import subprocess

        source = path.read_text()
        tree = ast.parse(source)
        baseline = subprocess.check_output(["git", "show", "05ea109:" + str(path.relative_to(root))], cwd=root).decode()
        original = ast.parse(baseline)
        definitions = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
        symbols = {}
        for node in tree.body:
            if isinstance(node, definitions):
                symbols[node.name] = node
            elif isinstance(node, (ast.Import, ast.ImportFrom)):
                symbols.update({a.asname or a.name.split(".")[0]: node for a in node.names})
            elif isinstance(node, (ast.Assign, ast.AnnAssign)):
                symbols.update(
                    {n.id: node for n in ast.walk(node) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store)}
                )
        roots = [n for n in original.body if isinstance(n, definitions)]
        for node in roots:
            assert ast.get_source_segment(source, symbols[node.name]) == ast.get_source_segment(baseline, node)
        pending, seen, closure = [n.name for n in roots], set(), []
        while pending:
            name = pending.pop()
            if name in seen or name not in symbols:
                continue
            seen.add(name)
            node = symbols[name]
            closure.extend(ast.walk(node))
            pending.extend(n.id for n in ast.walk(node) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load))
        forbidden = {"read_memory_records", "resolve_fact_rows", "flush", "set_belief", "resolve_conflict"}
        assert not any(isinstance(n, ast.Attribute) and n.attr in forbidden for n in closure)
        assert not any(
            isinstance(n, ast.ImportFrom)
            and any(x in (n.module or "") for x in ("mutation_uow", "belief_service", "lifecycle_service"))
            for n in closure
        )
        assert not any(
            (isinstance(n, ast.Attribute) and n.attr in {"commit", "create_async_engine"})
            or (isinstance(n, ast.Name) and n.id in {"commit", "create_async_engine"})
            for n in ast.walk(tree)
        )
    tables = [
        r[0] for r in await sql(state, "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    ]

    async def snapshot():
        return {
            t: await sql(state, 'SELECT * FROM "' + t + '" ORDER BY rowid')
            for t in tables
            if not t.endswith("_fts") and "_fts_" not in t
        }

    before = await snapshot()
    statements = []
    original = AsyncSession.execute

    async def trace(self, statement, *args, **kwargs):
        statements.append(str(statement))
        return await original(self, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "execute", trace)
    for suffix in ("beliefs", "beliefs/" + IDENT, "conflicts"):
        assert (await http(state, suffix)).status == 200
    assert not any(s.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")) for s in statements)
    assert await snapshot() == before


async def test_belief_guard_revalidates_injected_nested_results(belief_http):
    from dataclasses import replace

    from memory_server.transports.http.routes.beliefs import validate_beliefs_page, validate_conflicts_page

    state = belief_http
    await orm_seed(
        state,
        [
            dict(id=ident(1), proposition="Docker is better than Podman"),
            dict(id=ident(2), proposition="Docker is worse than Podman"),
        ],
    )
    item = d.Memory.model_validate_json((await http(state, "beliefs/" + IDENT)).body)
    raw = item.model_copy(update=dict(content="CANARY", source="CANARY", creator="CANARY", masked=False))
    assert (
        validate_beliefs_page(d.Page[d.Memory](items=[raw], has_more=False, next_cursor=None, request_id="x"))
        .items[0]
        .content
        is None
    )
    pair = d.Page[d.Conflict].model_validate_json((await http(state, "conflicts")).body)
    pair.items[0].__dict__["belief_a"] = raw
    pair.items[0].evidence_a.append(
        d.EvidenceSummary(
            method="CANARY",
            sources=["CANARY"],
            session_id="CANARY",
            confidence=0.7,
            source_date="CANARY",
            derived_from=["CANARY"],
            claim_type="authority",
        )
    )
    validated = validate_conflicts_page(pair)
    assert "CANARY" not in validated.model_dump_json()
    pair.items[0].__dict__["id"] = "conf_bad"
    with expect_error("E_INTERNAL"):
        validate_conflicts_page(pair)

    class Injected:
        async def execute(self, ctx, cmd):
            return raw

    app = create_http_app(
        state.config,
        replace(state.deps, ports={**state.deps.ports, "beliefs.get": Injected()}),
        read_routes=state.specs,
        mutation_routes=state.mutation_specs,
    )
    response = await request(app, "/api/v1/profiles/alpha/beliefs/" + IDENT, bearer=state.bearer)
    assert response.status == 200 and b"CANARY" not in response.body
    assert response.headers["etag"] == f'"m:belief:{IDENT}:1"'


async def test_belief_expiry_revocation_and_reveal_no_bypass(belief_http):
    state = belief_http
    now = datetime.now(UTC)
    permissions = ["belief.read", "memory.reveal"]
    created = state.deps.tokens._bootstrap_create(
        d.TokenCreate(
            subject="reveal", profiles=["alpha"], permissions=permissions, expires_at=now + timedelta(hours=1)
        ),
        now=now,
    )
    bearer = f"cmms_{created.token.id}.{created.secret}"
    principal = await state.deps.tokens.authenticate(bearer, now=now)
    await orm_seed(state, [dict(id=IDENT, proposition="CANARY")])
    response = await http(state, "beliefs/" + IDENT, bearer=bearer)
    assert response.status == 200 and response.json["masked"] and response.json["content"] is None
    assert (await http(state, bearer=bearer, query=b"source=")).status == 403
    state.deps.tokens._unchecked_revoke(
        state.deps.tokens._local_context(now),
        created.token.id,
        d.ControlMutation(reason="test", confirmation=True, expected_generation=1),
        now,
        local=True,
    )
    assert (await http(state, bearer=bearer)).status == 401
    from dataclasses import replace

    from memory_server.control.policy import TokenError

    with pytest.raises(TokenError) as revoked:
        await state.deps.ports["beliefs.list"].execute(replace(context(state), principal=principal), command())
    assert revoked.value.code == "E_AUTH_REQUIRED"
    past = now - timedelta(hours=2)
    old = state.deps.tokens._bootstrap_create(
        d.TokenCreate(
            subject="expired", profiles=["alpha"], permissions=["belief.read"], expires_at=past + timedelta(hours=1)
        ),
        now=past,
    )
    assert (await http(state, bearer=f"cmms_{old.token.id}.{old.secret}")).status == 401


async def test_belief_etag_exact_type_excludes_subclass_at_e1(belief_http):
    from dataclasses import replace

    from starlette.requests import Request

    from memory_server.transports.http.routes.beliefs import BeliefReadRouteSpec

    state = belief_http
    await orm_seed(state, [dict(id=IDENT, proposition="CANARY")])
    spec = next(s for s in state.specs if s.operation == "beliefs.get")
    called = []

    def forged(value):
        called.append(value.id)
        return '"evil"'

    class SubSpec(BeliefReadRouteSpec):
        pass

    subclass = SubSpec(**{**spec.__dict__, "etag": forged})
    scope = dict(
        type="http",
        method="GET",
        path="/api/v1/profiles/alpha/beliefs/" + IDENT,
        headers=[(b"authorization", ("Bearer " + state.bearer).encode())],
        query_string=b"",
        path_params={"profile": "alpha", "id": IDENT},
        s06={"spec": subclass},
    )
    # The mount refuses this subclass; here cross the real Guard/E1 boundary
    # directly to prove E1 itself grants no subclass header authority.
    response = await state.app.guard.execute_read(
        Request(scope), subclass, b"", "req_00000000-0000-4000-8000-000000000001", monotonic() + 30
    )
    assert response.status_code == 200 and "etag" not in response.headers
    assert called == []
    canonical = replace(spec)
    scope["s06"]["spec"] = canonical
    response = await state.app.guard.execute_read(
        Request(scope), canonical, b"", "req_00000000-0000-4000-8000-000000000002", monotonic() + 30
    )
    assert response.status_code == 200 and response.headers.get("etag") == f'"m:belief:{IDENT}:1"'


async def test_belief_cursor_key_custody_and_restart(belief_http, tmp_path):
    import os
    from dataclasses import replace

    from memory_server.runtime.read_composition import compose_belief_read_ports
    from memory_server.transports.http.routes.beliefs import BeliefCursorCodec

    state = belief_http
    path = state.config.control_path.parent / "http-belief-cursor.key"
    assert path.stat().st_mode & 0o777 == 0o600 and len(path.read_bytes()) == 32
    first = next(s.cursor_codec for s in state.specs if s.operation == "beliefs.list")
    second = BeliefCursorCodec.from_path(path, state.config.cursor_ttl_seconds)
    now = datetime.now(UTC)
    pos = ("2025-01-01T00:00:00.000000+00:00", "belief", IDENT)
    cmd = command(limit=1)
    token = first.issue(context(state), cmd, last=pos, upper=pos, limit=1, now=now)
    assert second.validate(state.principal, cmd, token, now=now)["last"] == pos
    for name, value, mode in (("short", b"x", 0o600), ("mode", b"x" * 32, 0o644)):
        bad = tmp_path / name
        bad.write_bytes(value)
        os.chmod(bad, mode)
        with expect_error("E_STORAGE"):
            BeliefCursorCodec.from_path(bad)
    link = tmp_path / "link"
    link.symlink_to(path)
    with expect_error("E_STORAGE"):
        BeliefCursorCodec.from_path(link)
    with expect_error("E_RUNTIME_NOT_READY"):
        compose_belief_read_ports(replace(state.deps, leases=None), state.config)


async def test_belief_get_scoped_id_sql_and_no_search(belief_http, monkeypatch):
    from sqlalchemy.ext.asyncio import AsyncSession

    from memory_server.application.belief_service import BeliefService

    state = belief_http
    await orm_seed(state, [dict(id=IDENT, proposition=ident(2))])

    def forbidden(*args, **kwargs):
        pytest.fail("native text search called by belief read")

    monkeypatch.setattr(BeliefService, "get_belief", forbidden)
    original, statements = AsyncSession.execute, []

    async def trace(self, statement, params=None, *args, **kwargs):
        statements.append((str(statement), params))
        return await original(self, statement, params, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "execute", trace)
    response = await http(state, "beliefs/" + ident(2))
    assert response.status == 404 and "etag" not in response.headers
    response = await http(state, "beliefs/" + IDENT)
    assert response.status == 200 and response.json["id"] == IDENT
    assert any("FROM beliefs WHERE id=:id LIMIT 1" in s and p == {"id": IDENT} for s, p in statements)
    assert not any("FROM facts" in s or "FROM decisions" in s or "FROM skills" in s for s, p in statements)


@pytest.mark.parametrize("extra_at_512", [False, True])
async def test_conflicts_frontier_512_boundary(belief_http, monkeypatch, extra_at_512):
    from sqlalchemy.ext.asyncio import AsyncSession

    from memory_server.evaluation.relation import RelationClassifier

    state = belief_http
    # Equal timestamp gives exact UUID-oriented candidate order. Pair 512=(27,32).
    phrases = {1: "Docker is better than Podman", 2: "Docker is worse than Podman"}
    if extra_at_512:
        phrases.update({27: "Python is better than Java", 32: "Python is worse than Java"})
    await orm_seed(
        state,
        [
            dict(id=ident(i), proposition=phrases.get(i, f"unrelatedword{i}"), created_at=datetime(2025, 1, 1))
            for i in range(1, 34)
        ],
    )
    classify, calls = RelationClassifier.classify_pair, []

    def counted(self, a, b, **kwargs):
        calls.append((a.id, b.id))
        return classify(self, a, b, **kwargs)

    monkeypatch.setattr(RelationClassifier, "classify_pair", counted)
    execute, statements = AsyncSession.execute, []

    async def traced(self, statement, params=None, *args, **kwargs):
        statements.append((str(statement), params))
        return await execute(self, statement, params, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "execute", traced)
    response = await http(state, "conflicts", query=b"limit=1")
    assert len(calls) == 512
    assert calls[-1] == (ident(27), ident(32))
    assert any(p and p.get("scan_limit") == 513 for s, p in statements)
    if extra_at_512:
        assert response.status == 200, response.body
        assert response.json["has_more"] is True and len(response.json["items"]) == 1
        from urllib.parse import urlencode

        response = await http(
            state, "conflicts", query=urlencode(dict(limit=1, cursor=response.json["next_cursor"])).encode()
        )
        # Last is the returned pair, not the extra at 512. The continuation
        # has 513 candidates and only one match, so it must fail closed too.
        assert response.status == 503 and response.json["error"]["details"] == {"limit": 512}, response.body
        await sql(
            state,
            "DELETE FROM beliefs WHERE id NOT IN (:a,:b,:c,:d)",
            dict(a=ident(1), b=ident(2), c=ident(27), d=ident(32)),
        )
        first = await http(state, "conflicts", query=b"limit=1")
        assert first.status == 200 and first.json["has_more"] is True, first.body
        await orm_seed(
            state, [dict(id=ident(0), proposition="Docker is worse than Podman", created_at=datetime(2026, 1, 1))]
        )
        second = await http(
            state, "conflicts", query=urlencode(dict(limit=1, cursor=first.json["next_cursor"])).encode()
        )
        assert second.status == 200 and second.json["has_more"] is False, second.body
        assert second.json["items"][0]["belief_a"]["id"] == ident(27)
        assert first.json["items"][0]["id"] != second.json["items"][0]["id"]
    else:
        assert response.status == 503 and response.json["error"]["details"] == {"limit": 512}, response.body


async def test_conflicts_evidence_occurrence_budget(belief_http):
    from storage.models.belief import EvidenceORM

    state = belief_http
    await orm_seed(
        state,
        [
            dict(id=ident(1), proposition="Docker is better than Podman"),
            dict(id=ident(2), proposition="Docker is worse than Podman"),
        ],
    )

    async def add(first, last):
        async with state.deps.leases.acquire(context(state)) as scope:
            async with scope.application.read_resources().session_factory() as session, session.begin():
                session.add_all(
                    [
                        EvidenceORM(
                            id=ident(i + 100),
                            belief_id=ident(1),
                            source_type="CANARY",
                            source_id="SOURCE_CANARY",
                            weight=0.7,
                            contributor="CONTRIBUTOR_CANARY",
                            note="NOTE_CANARY",
                        )
                        for i in range(first, last)
                    ]
                )

    await add(0, 1024)
    response = await http(state, "conflicts")
    assert response.status == 200, response.body
    evidence = response.json["items"][0]["evidence_a"]
    assert len(evidence) == 1024 and b"CANARY" not in response.body
    assert all(
        e
        == dict(
            method=None, sources=[], session_id=None, confidence=0.7, source_date=None, derived_from=[], claim_type=None
        )
        for e in evidence
    )
    await add(1024, 1025)
    response = await http(state, "conflicts")
    assert response.status == 503 and response.json["error"]["details"] == {"limit": 1024}, response.body


async def test_belief_cancel_cleanup_retains_admission(belief_http, monkeypatch):
    import aiosqlite

    from memory_server.application.remote_beliefs import BeliefsReadHandler

    state = belief_http
    entered, clearing, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    clear = aiosqlite.Connection.set_progress_handler

    async def held_clear(self, callback, n):
        if callback is None:
            clearing.set()
            await release.wait()
        return await clear(self, callback, n)

    monkeypatch.setattr(aiosqlite.Connection, "set_progress_handler", held_clear)
    original = BeliefsReadHandler.list_beliefs

    async def held(self, cmd):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(BeliefsReadHandler, "list_beliefs", held)
    task = asyncio.create_task(state.deps.ports["beliefs.list"].execute(context(state), command()))
    await asyncio.wait_for(entered.wait(), 5)
    task.cancel()
    try:
        _, pending = await asyncio.wait({asyncio.create_task(clearing.wait())}, timeout=1)
        for waiter in pending:
            waiter.cancel()
        assert clearing.is_set(), "real driver reset was omitted"
        task.cancel()
        done, _ = await asyncio.wait({task}, timeout=0.05)
        assert not done, "admission released before owned clear completed"
        assert state.deps.leases.snapshot()["alpha"].admissions > 0
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
    assert task.cancelled()
    assert state.deps.leases.snapshot()["alpha"].admissions == 0
    monkeypatch.setattr(BeliefsReadHandler, "list_beliefs", original)
    for profile in ("alpha", "beta"):
        assert (await http(state, profile=profile)).status == 200


@pytest.fixture
async def belief_write(belief_http):
    state = belief_http
    now = datetime.now(UTC)
    created = state.deps.tokens._bootstrap_create(
        d.TokenCreate(
            subject="s15b1",
            profiles=["alpha", "beta"],
            permissions=["belief.write", "belief.read"],
            expires_at=now + timedelta(hours=1),
        ),
        now=now,
    )
    state.write_bearer = f"cmms_{created.token.id}.{created.secret}"
    state.write_principal = await state.deps.tokens.authenticate(state.write_bearer, now=now)
    return state


def set_payload(**updates):
    return {
        "proposition": "PostgreSQL transactions provide atomic durable database updates.",
        "replace_belief_id": None,
        "reason": "S15B1_REASON_CANARY",
        **updates,
    }


async def post_set(state, payload=None, *, key=None, headers=(), **kwargs):
    from uuid import uuid4

    return await http(
        state,
        method="POST",
        bearer=state.write_bearer,
        body=set_payload() if payload is None else payload,
        headers=(("Idempotency-Key", str(key or uuid4())), *headers),
        **kwargs,
    )


async def test_beliefs_set_create_persisted_receipt_and_outbox(belief_write):
    state = belief_write
    response = await post_set(state)
    assert response.status == 201, response.body
    result = response.json
    memory = result["memories"][0]
    ident = memory["id"]
    assert memory["revision"] == memory["source_version"] == 1
    assert memory["masked"] and memory["content"] is None and memory["payload"] is None
    assert memory["source"] is memory["creator"] is None
    assert result["receipts"][0]["id"] == ident
    assert result["receipts"][0]["source"] is result["receipts"][0]["created_by"] is None
    assert result["admission"][0]["admitted"] is True
    assert await sql(state, "SELECT version,source,creator FROM beliefs WHERE id=:id", {"id": ident}) == [
        (1, "agent", "s15b1")
    ]
    assert await sql(state, "SELECT id,created_by FROM receipts WHERE id=:id", {"id": ident}) == [(ident, "s15b1")]
    assert {r[0] for r in await sql(state, "SELECT id FROM outbox_entries WHERE record_id=:id", {"id": ident})} == set(
        result["outbox_ids"]
    )
    assert (
        len(
            await sql(
                state, "SELECT request_id FROM http_request_journal WHERE request_id=:id", {"id": result["request_id"]}
            )
        )
        == 1
    )
    assert b"S15B1_REASON_CANARY" not in response.body


async def set_snapshot(state, profile="alpha"):
    """Fresh supplied sessions; indexing-worker bookkeeping is not canonical state."""
    tables = (
        "beliefs",
        "facts",
        "evidence",
        "claim_relations",
        "lifecycle_states",
        "lifecycle_events",
        "receipts",
        "http_request_journal",
    )
    result = {table: await sql(state, f"SELECT * FROM {table} ORDER BY 1", profile=profile) for table in tables}
    result["outbox_entries"] = await sql(
        state, "SELECT id,record_type,record_id,operation,payload_json FROM outbox_entries ORDER BY id", profile=profile
    )
    return result


def error_code(response, code, status):
    assert response.status == status, response.body
    assert response.json["error"]["code"] == code, response.body
    assert "etag" not in response.headers
    return response.json["error"]["details"]


async def test_beliefs_set_reinforcement_requires_client_cas(belief_write, monkeypatch):
    from uuid import uuid4

    state = belief_write
    original = await post_set(state, set_payload(confidence=0.8, tags=["first"]))
    assert original.status == 201, original.body
    ident = original.json["memories"][0]["id"]
    before = await set_snapshot(state)
    assert error_code(await post_set(state), "E_PRECONDITION_REQUIRED", 428) == {}
    assert error_code(await post_set(state, set_payload(expected_versions={ident: 2})), "E_VERSION_CONFLICT", 409) == {
        "id": ident,
        "expected": 2,
        "actual": 1,
    }
    assert await set_snapshot(state) == before
    port = state.deps.ports["beliefs.set"]

    def forbidden(*a, **k):
        pytest.fail("reinforcement/replay must not classify")

    monkeypatch.setattr(port._gate, "classify", forbidden)
    payload = set_payload(expected_versions={ident: 1}, confidence=0.4, tags=["first", "second"])
    key = uuid4()
    updated = await post_set(state, payload, key=key)
    assert updated.status == 200, updated.body
    assert updated.json["reinforced_ids"] == [ident] and updated.json["admission"] == []
    assert updated.json["memories"][0]["revision"] == 2
    assert updated.json["memories"][0]["confidence"] == pytest.approx(0.6)
    import json

    stored = (await sql(state, "SELECT version,tags FROM beliefs WHERE id=:id", {"id": ident}))[0]
    assert stored[0] == 2 and json.loads(stored[1]) == ["first", "second"]
    before = await set_snapshot(state)
    for _ in range(10):
        replay = await post_set(state, payload, key=key)
        assert replay.status == 200 and replay.json == updated.json
        assert replay.headers["idempotency-replayed"] == "true"
        assert replay.headers["x-original-request-id"] == updated.json["request_id"]
        assert replay.headers["x-request-id"] != updated.headers["x-request-id"]
    assert await set_snapshot(state) == before
    error_code(await post_set(state, {**payload, "reason": "changed"}, key=key), "E_IDEMPOTENCY_CONFLICT", 409)


async def test_beliefs_set_replace_requires_client_cas(belief_write):
    state = belief_write
    created = await post_set(state)
    old = created.json["memories"][0]["id"]
    payload = set_payload(proposition="SQLite serializes concurrent canonical database writers.", replace_belief_id=old)
    before = await set_snapshot(state)
    error_code(await post_set(state, {**payload, "replace_belief_id": IDENT}), "E_NOT_FOUND", 404)
    error_code(await post_set(state, {**payload, "expected_versions": {IDENT: 1}}), "E_VALIDATION", 422)
    assert error_code(await post_set(state, payload), "E_PRECONDITION_REQUIRED", 428) == {}
    error_code(await post_set(state, {**payload, "expected_versions": {old: 9}}), "E_VERSION_CONFLICT", 409)
    error_code(await post_set(state, set_payload(replace_belief_id=IDENT)), "E_NOT_FOUND", 404)
    assert await set_snapshot(state) == before
    # Same matching ID must still replace, never take reinforcement first.
    response = await post_set(state, set_payload(replace_belief_id=old, expected_versions={old: 1}))
    assert response.status == 201, response.body
    new, previous = response.json["memories"]
    assert new["id"] != old and new["revision"] == 2
    assert previous["id"] == old and previous["lifecycle_state"] == "superseded" and previous["revision"] == 2
    assert response.json["reinforced_ids"] == []
    for receipt in response.json["receipts"]:
        assert await sql(state, "SELECT id FROM receipts WHERE id=:id", {"id": receipt["id"]}) == [(receipt["id"],)]
    assert await sql(state, "SELECT version,lifecycle_state FROM beliefs WHERE id=:id", {"id": old}) == [
        (2, "superseded")
    ]


@pytest.mark.parametrize(
    "headers",
    [
        (("If-Match", ""),),
        (("If-Match", "*"),),
        (("If-Match", 'W/"x"'),),
        (("If-Match", '"m:belief:00000000-0000-4000-8000-000000000001:1"'),),
        (("If-Match", '"a", "b"'),),
        (("If-Match", '"a"'), ("If-Match", '"b"')),
    ],
)
@pytest.mark.parametrize("mode", ["missing", "stale", "foreign", "replay"])
async def test_beliefs_set_q5_header_matrix_before_lease(belief_write, monkeypatch, headers, mode):
    from uuid import uuid4

    state = belief_write
    created = await post_set(state)
    ident = created.json["memories"][0]["id"]
    key = uuid4()
    payload = set_payload(expected_versions={ident: 1})
    if mode == "missing":
        payload.pop("expected_versions")
    elif mode == "stale":
        payload["expected_versions"] = {ident: 9}
    elif mode == "foreign":
        payload = set_payload(replace_belief_id=IDENT)
    else:
        assert (await post_set(state, payload, key=key)).status == 200
    before = await set_snapshot(state)
    calls = []
    original = type(state.deps.leases).acquire

    def track(self, ctx):
        calls.append(ctx)
        return original(self, ctx)

    with monkeypatch.context() as patch:
        patch.setattr(type(state.deps.leases), "acquire", track)
        error_code(await post_set(state, payload, key=key, headers=headers), "E_VALIDATION", 422)
        assert calls == []
    assert await set_snapshot(state) == before


@pytest.mark.parametrize(
    "change",
    [
        {"auto_resolve": True},
        {"auto_resolve": False},
        {"actor": "forged"},
        {"force": True},
        {"confidence": True},
        {"confidence": "0.5"},
        {"confidence": float("nan")},
        {"confidence": float("inf")},
        {"proposition": "  "},
        {"reason": "\t"},
        {"replace_belief_id": "00000000000040008000000000000001"},
        {"expected_versions": {IDENT: True}},
        {"expected_versions": {IDENT: 0}},
        {"expected_versions": {IDENT: "1"}},
        {"expected_versions": None},
        {
            "sources": [
                {
                    "method": None,
                    "sources": [],
                    "session_id": None,
                    "confidence": True,
                    "source_date": None,
                    "derived_from": [],
                    "claim_type": None,
                }
            ]
        },
    ],
)
async def test_beliefs_set_strict_parser_precedes_lookup(belief_write, monkeypatch, change):
    state = belief_write

    def forbidden(*a, **k):
        pytest.fail("invalid DTO reached lease")

    monkeypatch.setattr(type(state.deps.leases), "acquire", forbidden)
    error_code(await post_set(state, set_payload(**change)), "E_VALIDATION", 422)


@pytest.mark.parametrize(
    "body", [b"{}", b"[]", b"null", b'{"proposition":"a","proposition":"b"}', b"\xff", b'{"confidence":NaN}']
)
async def test_beliefs_set_strict_json(belief_write, body):
    error_code(await post_set(belief_write, body, headers=(("Content-Type", "application/json"),)), "E_VALIDATION", 422)


async def test_beliefs_set_admission_evidence_claims_and_rollback(belief_write, monkeypatch):
    import json

    from storage.repositories.receipt_repo import ReceiptRepository

    from memory_server.admission import MemoryAdmissionGate

    state = belief_write
    before = await set_snapshot(state)
    error_code(await post_set(state, set_payload(proposition="ok")), "E_ADMISSION_REJECTED", 422)
    assert await set_snapshot(state) == before
    summary = dict(
        method="authority",
        sources=["opaque:CANARY", "opaque:CANARY"],
        session_id="SESSION_CANARY",
        confidence=0.0,
        source_date="DATE_CANARY",
        derived_from=[],
        claim_type="authority",
    )
    payload = set_payload(sources=[summary])
    decisions = []
    original = MemoryAdmissionGate.classify

    def classify(self, *a, **kw):
        assert kw["source_scope"] == "remote" and kw["force"] is False
        result = original(self, *a, **kw)
        decisions.append(result)
        return result

    monkeypatch.setattr(MemoryAdmissionGate, "classify", classify)
    response = await post_set(state, payload)
    assert response.status == 201, response.body
    assert len(decisions) == 1 and response.json["admission"][0]["score"] == decisions[0].score
    ident = response.json["memories"][0]["id"]
    assert (
        await sql(
            state,
            "SELECT source_type,source_id,weight,contributor FROM evidence WHERE belief_id=:id ORDER BY id",
            {"id": ident},
        )
        == [("authority", "opaque:CANARY", 0.0, "s15b1")] * 2
    )
    raw = (await sql(state, "SELECT history FROM receipts WHERE id=:id", {"id": ident}))[0][0]
    assert json.loads(raw)[0]["claims"] == [{**summary, "trusted": False, "origin": "remote_claim"}]
    assert b"CANARY" not in response.body
    before = await set_snapshot(state)
    original_create = ReceiptRepository.create
    calls = []

    async def broken(self, receipt):
        calls.append(id(self._session) if hasattr(self, "_session") else True)
        await original_create(self, receipt)
        raise RuntimeError("PRIVATE_STORAGE_CANARY")

    with monkeypatch.context() as patch:
        patch.setattr(ReceiptRepository, "create", broken)
        failure = await post_set(state, set_payload(proposition="MariaDB supports transactional database updates."))
        error_code(failure, "E_INTERNAL", 500)
        assert b"PRIVATE_STORAGE_CANARY" not in failure.body
    assert calls and await set_snapshot(state) == before


async def test_beliefs_set_original_snapshot_replay_and_revocation(belief_write):
    from uuid import uuid4

    state = belief_write
    key = uuid4()
    original = await post_set(state, key=key)
    ident = original.json["memories"][0]["id"]
    assert (await post_set(state, set_payload(expected_versions={ident: 1}))).status == 200
    replay = await post_set(state, key=key)
    assert replay.status == 201 and replay.json == original.json
    # Scope and grants are rechecked for replay, not merely on the original write.
    read_only = await http(state, method="POST", body=set_payload(), headers=(("Idempotency-Key", str(key)),))
    error_code(read_only, "E_PERMISSION_DENIED", 403)


async def test_beliefs_set_canonical_descriptor_and_missing_binding(belief_write):
    from dataclasses import replace
    from uuid import uuid4

    from memory_server.transports.http.routes import beliefs as routes

    state = belief_write
    specs = routes.create_belief_mutation_routes()
    assert len(specs) == 2 and specs[0] is routes.BELIEF_MUTATION_ROUTE_SPECS["beliefs.set"]
    assert specs[1] is routes.BELIEF_MUTATION_ROUTE_SPECS["conflicts.resolve"]
    spec = specs[0]
    for fields in (
        {},
        {"parse": lambda *a: None},
        {"status": lambda *a: 200},
        {"method": "GET"},
        {"path": "/bad"},
        {"permission": "memory.write"},
    ):
        with pytest.raises(ValueError):
            create_http_app(state.config, state.deps, mutation_routes=(replace(spec, **fields),))

    class SubSpec(type(spec)):
        pass

    with pytest.raises(ValueError):
        create_http_app(state.config, state.deps, mutation_routes=(SubSpec(**spec.__dict__),))
    with pytest.raises(ValueError):
        create_http_app(state.config, state.deps, mutation_routes=(spec, spec))
    ports = dict(state.deps.ports)
    del ports["beliefs.set"]
    missing = create_http_app(state.config, replace(state.deps, ports=ports), mutation_routes=specs)
    response = await request(
        missing,
        spec.path.replace("{profile}", "alpha"),
        method="POST",
        bearer=state.write_bearer,
        body=set_payload(),
        headers=(("Idempotency-Key", str(uuid4())),),
    )
    error_code(response, "E_CAPABILITY_UNAVAILABLE", 503)


async def test_beliefs_set_postcommit_pending_reconciles_original(belief_write, monkeypatch):
    from uuid import uuid4

    state = belief_write
    port = state.deps.ports["beliefs.set"]
    original_publish = port._publish_outcome
    calls = []

    async def fail(ticket, outcome):
        calls.append((ticket, outcome))
        return False

    monkeypatch.setattr(port, "_publish_outcome", fail)
    key = uuid4()
    response = await post_set(state, key=key)
    assert response.status == 201
    assert len(calls) == 1 and calls[0][1].status == 201
    assert await sql(state, "SELECT pending_publication FROM http_request_journal") == [(1,)]
    error_code(
        await post_set(state, set_payload(proposition="MySQL supports database transactions.")),
        "E_AUDIT_UNAVAILABLE",
        503,
    )

    async def publish(ticket, outcome):
        calls.append((ticket, outcome))
        return await original_publish(ticket, outcome)

    monkeypatch.setattr(port, "_publish_outcome", publish)
    replay = await post_set(state, key=key)
    assert replay.json == response.json
    assert calls[-1][0] == calls[0][0]
    assert await sql(state, "SELECT pending_publication FROM http_request_journal") == [(0,)]
    audit_rows = list(state.deps.access._db.execute("SELECT * FROM access_audit"))
    assert audit_rows and "S15B1_REASON_CANARY" not in repr(audit_rows)


async def test_beliefs_set_events_use_only_persisted_ids(belief_write, monkeypatch):
    from uuid import uuid4

    state = belief_write
    events = []

    class Sink:
        async def publish(self, event):
            assert await sql(state, "SELECT id FROM lifecycle_events WHERE id=:id", {"id": event.id}) == [(event.id,)]
            events.append(event)

    port = state.deps.ports["beliefs.set"]
    monkeypatch.setattr(port, "_events", Sink())
    created = await post_set(state)
    assert created.status == 201 and events == []
    ident = created.json["memories"][0]["id"]
    key = uuid4()
    payload = set_payload(expected_versions={ident: 1})
    updated = await post_set(state, payload, key=key)
    assert updated.status == 200 and len(events) == 1
    assert events[0].type == "memory.updated" and events[0].data.revision == 2
    assert (await post_set(state, payload, key=key)).json == updated.json
    assert len(events) == 1
    replaced = await post_set(state, set_payload(replace_belief_id=ident, expected_versions={ident: 2}))
    assert replaced.status == 201 and len(events) == 2
    assert events[-1].type == "memory.transitioned" and events[-1].memory_id == ident
    assert len(events[-1].model_dump_json().encode()) < 4096
    assert "CANARY" not in events[-1].model_dump_json()


async def test_belief_write_import_graph_and_read_partition():
    import ast
    import subprocess
    from pathlib import Path

    from memory_server.application import remote_beliefs as module
    from memory_server.transports.http.routes import beliefs as routes

    base = "08ebb12a0b47bff1bbc649e4aefcfe227fd443fc"
    for current, suffix, stop in (
        (module, "application/remote_beliefs.py", "validate_belief_mutation_command"),
        (routes, "transports/http/routes/beliefs.py", None),
    ):
        filename = Path(current.__file__)
        old = ast.parse(subprocess.check_output(["git", "show", f"{base}:src/memory_server/{suffix}"], text=True))
        new = ast.parse(filename.read_text())
        definitions = {
            n.name: n for n in new.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
        }
        for node in old.body:
            if getattr(node, "name", None) == stop:
                break
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                assert ast.dump(node) == ast.dump(definitions[node.name]), node.name
        for node in ast.walk(new):
            if isinstance(node, ast.Call):
                name = getattr(node.func, "attr", getattr(node.func, "id", ""))
                assert name not in {"commit", "create_async_engine", "AsyncSession"}
    assert hasattr(module, "RemoteBeliefsMutationPort")
    assert hasattr(module.BeliefsMutationHandler, "resolve_in_session")


async def test_beliefs_set_cancel_cleanup_holds_lease(belief_write, monkeypatch):
    import fcntl
    import os

    import aiosqlite

    from memory_server.application.remote_beliefs import BeliefsMutationHandler

    state = belief_write
    before = await set_snapshot(state)
    staged, cleanup, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original_set = BeliefsMutationHandler.set_in_session
    original_progress = aiosqlite.Connection.set_progress_handler

    async def paused_set(self, *a, **kw):
        await original_set(self, *a, **kw)
        staged.set()
        await asyncio.Event().wait()

    async def paused_progress(self, handler, n):
        if handler is None:
            cleanup.set()
            await release.wait()
        return await original_progress(self, handler, n)

    with monkeypatch.context() as patch:
        patch.setattr(BeliefsMutationHandler, "set_in_session", paused_set)
        patch.setattr(aiosqlite.Connection, "set_progress_handler", paused_progress)
        task = asyncio.create_task(post_set(state))
        try:
            await asyncio.wait_for(staged.wait(), 5)
            task.cancel()
            await asyncio.wait_for(cleanup.wait(), 5)
            for _ in range(3):
                task.cancel()
                await asyncio.sleep(0)
            assert not task.done()
            assert state.deps.leases.snapshot()["alpha"].admissions == 1
            fd = os.open(state.roots["alpha"] / ".cmms-storage.lock", os.O_RDWR)
            try:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                os.close(fd)
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
        assert task.cancelled()
    assert state.deps.leases.snapshot()["alpha"].admissions == 0
    assert await set_snapshot(state) == before
    assert (await post_set(state)).status == 201


async def test_beliefs_set_cancel_after_durable_commit_recovers_original_key(belief_write, monkeypatch):
    from uuid import uuid4

    from memory_server.application.remote_beliefs import _BeliefsMutationUoW

    state = belief_write
    key = uuid4()
    original = _BeliefsMutationUoW.run

    async def committed_cancel(self, *a, **kw):
        await original(self, *a, **kw)
        raise asyncio.CancelledError

    with monkeypatch.context() as patch:
        patch.setattr(_BeliefsMutationUoW, "run", committed_cancel)
        response = await post_set(state, key=key)
        assert response.status == 201, response.body
    replay = await post_set(state, key=key)
    assert replay.json == response.json
    assert len(await sql(state, "SELECT id FROM beliefs")) == 1
    assert len(await sql(state, "SELECT request_id FROM http_request_journal")) == 1
    assert state.deps.leases.snapshot()["alpha"].admissions == 0


async def test_beliefs_set_cancel_tail_joins_audit_and_events(belief_write, monkeypatch):
    from uuid import uuid4

    state = belief_write
    port = state.deps.ports["beliefs.set"]
    seeded = await post_set(state)
    ident = seeded.json["memories"][0]["id"]
    entered, cleaned, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    class Sink:
        async def publish(self, event):
            entered.set()
            try:
                await release.wait()
            finally:
                cleaned.set()

    monkeypatch.setattr(port, "_events", Sink())
    key = uuid4()
    task = asyncio.create_task(post_set(state, set_payload(expected_versions={ident: 1}), key=key))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        for _ in range(3):
            task.cancel()
            await asyncio.sleep(0)
        assert not task.done()
        assert state.deps.leases.snapshot()["alpha"].admissions == 1
        # Audit must not wait on the event branch.
        async with asyncio.timeout(0.8):
            while await sql(
                state, "SELECT pending_publication FROM http_request_journal WHERE idem_key=:key", {"key": str(key)}
            ) != [(0,)]:
                await asyncio.sleep(0)
    finally:
        release.set()
        result = await asyncio.wait_for(task, 5)
    assert result.status == 200 and cleaned.is_set()
    assert state.deps.leases.snapshot()["alpha"].admissions == 0


@pytest.fixture
async def belief_socket(tmp_path, monkeypatch):
    # Reuse the real socket/public-daemon/paused-worker harness, not its lifecycle operations.
    from tests.gateway.test_http_lifecycle import lifecycle

    async for state in lifecycle.__wrapped__(tmp_path, monkeypatch):
        state.bearer, _ = state.mint(permissions=("belief.write", "belief.read"))
        state.principal = await state.deps.tokens.authenticate(state.bearer, now=datetime.now(UTC))
        yield state


async def test_beliefs_set_real_socket_concurrent_cas_and_writer_reservation(belief_socket, monkeypatch):
    import json
    from uuid import uuid4

    from sqlalchemy import text

    from memory_server.application.remote_beliefs import BeliefsMutationHandler
    from tests.gateway.test_slice_runtime import http_request

    state = belief_socket

    async def send(payload):
        key = str(uuid4())
        for attempt in range(40):
            response = await http_request(
                state.config.port,
                "POST",
                "/api/v1/profiles/alpha/beliefs",
                json.dumps(payload).encode(),
                {"Authorization": "Bearer " + state.bearer, "Content-Type": "application/json", "Idempotency-Key": key},
            )
            if response[0] not in (409, 503) or response[1]["error"]["code"] not in {
                "E_REQUEST_IN_PROGRESS",
                "E_AUDIT_UNAVAILABLE",
            }:
                return response
            await asyncio.sleep(0.05)
        pytest.fail(f"original-key convergence exhausted: {response}")

    status, created = await send(set_payload())
    assert status == 201, created
    ident = created["memories"][0]["id"]
    entered, release = asyncio.Event(), asyncio.Event()
    original = BeliefsMutationHandler.set_in_session

    async def paused(self, session, *a, **kw):
        assert session.in_transaction()
        entered.set()
        await release.wait()
        return await original(self, session, *a, **kw)

    with monkeypatch.context() as patch:
        patch.setattr(BeliefsMutationHandler, "set_in_session", paused)
        first = asyncio.create_task(send(set_payload(expected_versions={ident: 1})))
        try:
            await asyncio.wait_for(entered.wait(), 5)
            # A second actual connection cannot reserve a writer before the first CAS.
            from sqlalchemy.exc import OperationalError

            async with state.factories["alpha"]() as contender:
                await contender.execute(text("PRAGMA busy_timeout=0"))
                with pytest.raises(OperationalError) as exc:
                    await contender.execute(text("UPDATE beliefs SET confidence=0.9 WHERE id=:id"), {"id": ident})
                assert exc.value.orig.sqlite_errorcode & 255 in (5, 6)
            second = asyncio.create_task(send(set_payload(expected_versions={ident: 1})))
        finally:
            release.set()
        results = await asyncio.wait_for(asyncio.gather(first, second), 10)
    assert sorted(status for status, _ in results) == [200, 409], results
    loser = next(body for status, body in results if status == 409)
    assert loser["error"]["code"] == "E_VERSION_CONFLICT"
    assert loser["error"]["details"] == {"id": ident, "expected": 1, "actual": 2}
    async with state.factories["alpha"]() as session:
        assert await session.scalar(text("SELECT version FROM beliefs WHERE id=:id"), {"id": ident}) == 2
        assert await session.scalar(text("SELECT count(*) FROM lifecycle_events")) == 1


async def test_beliefs_set_receipt_readback_failure_rolls_back(belief_write, monkeypatch):
    from sqlalchemy import text
    from storage.repositories.receipt_repo import ReceiptRepository

    state = belief_write
    before = await set_snapshot(state)
    original = ReceiptRepository.create
    calls = []

    async def vanished(self, receipt):
        result = await original(self, receipt)
        calls.append(result.id)
        # Real INSERT followed by a real in-transaction disappearance; never a fake success.
        session = self._session
        await session.execute(text("DELETE FROM receipts WHERE id=:id"), {"id": result.id})
        return result

    with monkeypatch.context() as patch:
        patch.setattr(ReceiptRepository, "create", vanished)
        error_code(await post_set(state), "E_STORAGE", 503)
    assert calls and await set_snapshot(state) == before


async def test_beliefs_set_direct_port_validation_before_lease(belief_write, monkeypatch):
    from dataclasses import replace
    from uuid import uuid4

    from memory_server.application import remote_beliefs as m

    state = belief_write
    port = m.RemoteBeliefsMutationPort(
        state.deps, ticket_provider=lambda: "direct-ticket", publish_outcome=state.deps.access.publish_committed
    )
    ctx = RequestContext("direct", state.write_principal, "alpha", uuid4(), monotonic() + 10)
    cmd = d.BeliefsSetCommand(
        operation="beliefs.set", profile_id="alpha", path_ids={}, payload=d.BeliefRequest(**set_payload())
    )
    calls = []
    original = type(state.deps.leases).acquire

    def track(self, ctx):
        calls.append(ctx)
        return original(self, ctx)

    monkeypatch.setattr(type(state.deps.leases), "acquire", track)
    for current, command_ in (
        (ctx, command()),
        (replace(ctx, profile_id="beta"), cmd),
        (replace(ctx, idempotency_key=None), cmd),
        (replace(ctx, deadline=float("nan")), cmd),
        (replace(ctx, principal=state.principal), cmd),
    ):
        with pytest.raises(Exception) as exc:
            await port.execute(current, command_)
        assert getattr(exc.value, "code", None) in {
            "E_VALIDATION",
            "E_IDEMPOTENCY_REQUIRED",
            "E_TIMEOUT",
            "E_PERMISSION_DENIED",
        }
    assert calls == []
    cmd.payload.__dict__["actor"] = "forged"
    with expect_error("E_VALIDATION"):
        await port.execute(ctx, cmd)
    assert calls == []
    for ttl in (True, 0, 604801, 1.0):
        with pytest.raises(ValueError):
            m.RemoteBeliefsMutationPort(
                state.deps,
                ticket_provider=lambda: "t",
                publish_outcome=state.deps.access.publish_committed,
                ttl_seconds=ttl,
            )


async def test_beliefs_set_event_timeout_is_bounded_and_audit_independent(belief_write, monkeypatch):
    state = belief_write
    created = await post_set(state)
    ident = created.json["memories"][0]["id"]
    cleaned = asyncio.Event()

    class Sink:
        async def publish(self, event):
            try:
                await asyncio.Event().wait()
            finally:
                cleaned.set()

    monkeypatch.setattr(state.deps.ports["beliefs.set"], "_events", Sink())
    response = await asyncio.wait_for(post_set(state, set_payload(expected_versions={ident: 1})), 3)
    assert response.status == 200 and cleaned.is_set()
    assert await sql(state, "SELECT pending_publication FROM http_request_journal ORDER BY request_id") == [(0,), (0,)]
    assert state.deps.leases.snapshot()["alpha"].admissions == 0


async def test_beliefs_set_missing_foreign_and_ambiguous_target_precedence(belief_write):
    state = belief_write
    created = await post_set(state, profile="beta")
    foreign = created.json["memories"][0]["id"]
    assert (
        error_code(
            await post_set(state, set_payload(replace_belief_id=foreign, expected_versions={foreign: 99})),
            "E_NOT_FOUND",
            404,
        )
        == {}
    )
    await orm_seed(
        state,
        [
            dict(id=ident(n), proposition="  İSTANBUL DATABASE TRANSACTIONS  ", confidence=0.6, version=1)
            for n in (80, 81)
        ],
    )
    before = await set_snapshot(state)
    error_code(
        await post_set(state, set_payload(proposition="i\u0307stanbul database transactions")), "E_VALIDATION", 422
    )
    assert await set_snapshot(state) == before
    error_code(await post_set(state, set_payload(expected_versions={foreign: 1})), "E_VALIDATION", 422)


async def test_beliefs_set_revoked_replay_denied(belief_write):
    from uuid import uuid4

    state = belief_write
    key = uuid4()
    response = await post_set(state, key=key)
    assert response.status == 201
    # Real control-store revocation; authentication must run again before journal replay.
    now = datetime.now(UTC)
    state.deps.tokens._db.execute(
        "UPDATE tokens SET revoked_at=?,generation=generation+1 WHERE id=?",
        (now.isoformat(), state.write_principal.token_id),
    )
    error_code(await post_set(state, key=key), "E_AUTH_REQUIRED", 401)


async def test_beliefs_set_real_commit_cancel_journal_recovery(belief_write, monkeypatch):
    from uuid import uuid4

    import aiosqlite

    state = belief_write
    await set_snapshot(state)  # open the real scoped pool before installing the commit barrier
    committed, release = asyncio.Event(), asyncio.Event()
    original = aiosqlite.Connection.commit
    once = True

    async def commit(self):
        nonlocal once
        await original(self)
        if once:
            once = False
            committed.set()
            await release.wait()

    key = uuid4()
    with monkeypatch.context() as patch:
        patch.setattr(aiosqlite.Connection, "commit", commit)
        task = asyncio.create_task(post_set(state, key=key))
        try:
            await asyncio.wait_for(committed.wait(), 5)
            assert state.deps.leases.snapshot()["alpha"].admissions == 1
            task.cancel()
            response = await asyncio.wait_for(task, 5)
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
    assert response.status == 201, response.body
    assert (await post_set(state, key=key)).json == response.json
    assert len(await sql(state, "SELECT id FROM beliefs")) == 1
    assert state.deps.leases.snapshot()["alpha"].admissions == 0


@pytest.fixture
async def conflict_http(belief_http):
    from tests.gateway.test_conflict_cas import resolve_http

    return await resolve_http.__wrapped__(belief_http)


@pytest.mark.parametrize(
    "headers",
    [
        (("If-Match", ""),),
        (("If-Match", "*"),),
        (("If-Match", 'W/"x"'),),
        (("If-Match", '"m:belief:00000000-0000-4000-8000-000000000001:1"'),),
        (("If-Match", '"a", "b"'),),
        (("If-Match", '"a"'), ("If-Match", '"b"')),
    ],
)
@pytest.mark.parametrize("mode", ["missing", "stale", "foreign", "replay"])
async def test_conflicts_resolve_q5_header_matrix_before_lease(conflict_http, monkeypatch, headers, mode):
    from uuid import uuid4

    from tests.gateway.test_conflict_cas import resolve, resolve_payload

    state, key = conflict_http, uuid4()
    payload = resolve_payload("defer")
    if mode == "missing":
        payload.pop("expected_versions")
    elif mode == "stale":
        payload["expected_versions"] = {ident(1): 9, ident(2): 9}
    elif mode == "foreign":
        await sql(state, "DELETE FROM beliefs WHERE id=:id", dict(id=ident(2)))
    else:
        assert (await resolve(state, payload, key=key)).status == 200
    before = await set_snapshot(state)
    calls, original = [], type(state.deps.leases).acquire

    def tracked(self, ctx):
        calls.append(ctx)
        return original(self, ctx)

    with monkeypatch.context() as patch:
        patch.setattr(type(state.deps.leases), "acquire", tracked)
        error_code(await resolve(state, payload, key=key, headers=headers), "E_VALIDATION", 422)
        assert calls == []
    assert await set_snapshot(state) == before


@pytest.mark.parametrize(
    "changes,status",
    [
        ({"expected_versions": {}}, 428),
        ({"expected_versions": {IDENT: 1}}, 428),
        ({"expected_versions": None}, 422),
        ({"expected_versions": {IDENT: True}}, 422),
        ({"expected_versions": {IDENT: 0}}, 422),
        ({"expected_versions": {IDENT: "1"}}, 422),
        ({"expected_versions": {ident(3): 1}}, 422),
        ({"confirmation": 1, "expected_versions": {}}, 422),
        ({"confirmation": False, "expected_versions": {}}, 422),
        ({"reason": "  ", "expected_versions": {}}, 422),
        ({"resolution": "unknown", "expected_versions": {}}, 422),
        ({"auto_resolve": True, "expected_versions": {}}, 422),
        ({"auto_resolve": False, "expected_versions": {}}, 422),
        ({"new_proposition": "ignored", "expected_versions": {}}, 422),
        ({"belief_a_id": ident(2), "expected_versions": {}}, 422),
        ({"belief_a_id": IDENT.replace("-", ""), "expected_versions": {}}, 422),
        ({"resolution": "merge", "new_proposition": "  ", "expected_versions": {}}, 422),
        ({"resolution": "merge", "new_proposition": "x" * 2049, "expected_versions": {}}, 422),
    ],
)
async def test_conflicts_resolve_parser_precedence(conflict_http, monkeypatch, changes, status):
    from tests.gateway.test_conflict_cas import resolve, resolve_payload

    def forbidden(*a, **kw):
        pytest.fail("malformed request reached lease")

    monkeypatch.setattr(type(conflict_http.deps.leases), "acquire", forbidden)
    error_code(
        await resolve(conflict_http, resolve_payload(**changes)),
        "E_VALIDATION" if status == 422 else "E_PRECONDITION_REQUIRED",
        status,
    )


async def test_conflicts_resolve_foreign_precedes_stale_and_pair_binding(conflict_http):
    from tests.gateway.test_conflict_cas import conflict_path, resolve, resolve_payload

    state = conflict_http
    payload = resolve_payload(expected_versions={ident(1): 9, ident(2): 9})
    await sql(state, "DELETE FROM beliefs WHERE id=:id", dict(id=ident(2)))
    assert error_code(await resolve(state, payload), "E_NOT_FOUND", 404) == {}
    error_code(await resolve(state, payload, conflict=conflict_path(ident(1), ident(3))), "E_VALIDATION", 422)
    # The read-only token cannot invoke either mutation, irrespective of malformed DTO.
    for suffix in ("beliefs", f"conflicts/{conflict_path()}/resolve"):
        error_code(
            await http(
                state, suffix, method="POST", body={}, headers=(("Idempotency-Key", str(__import__("uuid").uuid4())),)
            ),
            "E_PERMISSION_DENIED",
            403,
        )


async def test_conflicts_resolve_canonical_descriptor(conflict_http):
    from dataclasses import replace
    from uuid import uuid4

    from memory_server.gateway_contracts.catalog import CATALOG
    from memory_server.transports.http.routes import beliefs as routes
    from tests.gateway.test_conflict_cas import conflict_path, resolve_payload

    state = conflict_http
    specs = routes.create_belief_mutation_routes()
    assert [s.operation for s in specs] == ["beliefs.set", "conflicts.resolve"]
    for spec in specs:
        row = CATALOG.by_operation[spec.operation]
        assert row.owner == "S15" and row.mutation is True
        assert spec is routes.BELIEF_MUTATION_ROUTE_SPECS[spec.operation]
        assert (spec.method, spec.path, spec.permission) == (row.method, row.route, row.permission)
        for fields in (
            {},
            {"parse": lambda *a: None},
            {"status": lambda *a: 200},
            {"method": "GET"},
            {"path": "/bad"},
            {"permission": "memory.write"},
        ):
            with pytest.raises(ValueError):
                create_http_app(state.config, state.deps, mutation_routes=(replace(spec, **fields),))
    spec = specs[1]
    ports = dict(state.deps.ports)
    del ports[spec.operation]
    app = create_http_app(state.config, replace(state.deps, ports=ports), mutation_routes=specs)
    response = await request(
        app,
        f"/api/v1/profiles/alpha/conflicts/{conflict_path()}/resolve",
        method="POST",
        bearer=state.resolve_bearer,
        body=resolve_payload(),
        headers=(("Idempotency-Key", str(uuid4())),),
    )
    error_code(response, "E_CAPABILITY_UNAVAILABLE", 503)


async def test_both_belief_mutations_negative_auth_and_scope(conflict_http):
    from uuid import uuid4

    from tests.gateway.test_conflict_cas import conflict_path, resolve_payload

    state = conflict_http
    now = datetime.now(UTC)

    def mint(grants, profiles=("alpha",), *, when=now):
        token = state.deps.tokens._bootstrap_create(
            d.TokenCreate(
                subject="negative",
                profiles=list(profiles),
                permissions=list(grants),
                expires_at=when + timedelta(hours=1),
            ),
            now=when,
        )
        return f"cmms_{token.token.id}.{token.secret}", token

    for suffix, payload, permission in (
        ("beliefs", set_payload(), "belief.write"),
        (f"conflicts/{conflict_path()}/resolve", resolve_payload(), "belief.resolve"),
    ):

        async def send(bearer, profile="alpha"):
            return await http(
                state,
                suffix,
                profile=profile,
                method="POST",
                body=payload,
                bearer=bearer,
                headers=(("Idempotency-Key", str(uuid4())),),
            )

        error_code(await send(None), "E_AUTH_REQUIRED", 401)
        for grants in (
            ("belief.read",),
            ("memory.write",),
            ("memory.transition",),
            (("belief.write",) if permission == "belief.resolve" else ("belief.resolve",)),
        ):
            bearer, _ = mint(grants)
            error_code(await send(bearer), "E_PERMISSION_DENIED", 403)
        expired, _ = mint((permission,), when=now - timedelta(hours=2))
        error_code(await send(expired), "E_AUTH_REQUIRED", 401)
        scoped, _ = mint((permission,))
        error_code(await send(scoped, "beta"), "E_PERMISSION_DENIED", 403)
        unknown, _ = mint((permission,), profiles=("unknown",))
        error_code(await send(unknown, "unknown"), "E_PROFILE_NOT_FOUND", 404)
        revoked, token = mint((permission,))
        await state.deps.tokens.authenticate(revoked, now=now)
        state.deps.tokens._unchecked_revoke(
            state.deps.tokens._local_context(now),
            token.token.id,
            d.ControlMutation(reason="test", confirmation=True, expected_generation=1),
            now,
            local=True,
        )
        error_code(await send(revoked), "E_AUTH_REQUIRED", 401)


async def test_resolve_direct_port_rejects_other_operation_before_lease(conflict_http, monkeypatch):
    from uuid import uuid4

    from memory_server.transports.http.errors import HttpError

    state = conflict_http
    principal = await state.deps.tokens.authenticate(state.resolve_bearer, now=datetime.now(UTC))
    ctx = RequestContext("direct-resolve", principal, "alpha", uuid4(), monotonic() + 10)

    def forbidden(*a, **kw):
        pytest.fail("nonmutation operation reached lease")

    monkeypatch.setattr(type(state.deps.leases), "acquire", forbidden)
    for cmd in (command(), object()):
        with pytest.raises(HttpError) as error:
            await state.deps.ports["conflicts.resolve"].execute(ctx, cmd)
        assert error.value.code == "E_VALIDATION"
