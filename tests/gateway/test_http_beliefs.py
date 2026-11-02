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
WRITES = {"memories.create", "learn", "memories.transition", "memories.invalidate"}
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
    assert (len(state.specs), len(state.mutation_specs), len(state.specs) + len(state.mutation_specs)) == (24, 4, 28)
    reads = {spec.operation for spec in state.specs}
    assert READS <= reads
    assert {spec.operation for spec in state.mutation_specs} == WRITES
    assert set(state.deps.ports) == reads | WRITES
    assert "beliefs.set" not in state.deps.ports and "conflicts.resolve" not in state.deps.ports


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
    assert (await http(state, method="POST")).status == 405
    assert (await http(state, "conflicts/conf_x/resolve", method="POST")).status == 404


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
    assert response.headers["etag"] == f'"m:belief:{IDENT}:7"'
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
        tree = ast.parse(path.read_text())
        forbidden = {
            "read_memory_records",
            "resolve_fact_rows",
            "commit",
            "flush",
            "set_belief",
            "resolve_conflict",
            "create_async_engine",
        }
        assert not any(isinstance(n, ast.Attribute) and n.attr in forbidden for n in ast.walk(tree))
        assert not any(
            isinstance(n, ast.ImportFrom)
            and any(x in (n.module or "") for x in ("mutation_uow", "belief_service", "lifecycle_service"))
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
    await asyncio.wait_for(clearing.wait(), 5)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    assert state.deps.leases.snapshot()["alpha"].admissions > 0
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 5)
    assert state.deps.leases.snapshot()["alpha"].admissions == 0
    monkeypatch.setattr(BeliefsReadHandler, "list_beliefs", original)
    for profile in ("alpha", "beta"):
        assert (await http(state, profile=profile)).status == 200
