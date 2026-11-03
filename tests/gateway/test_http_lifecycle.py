"""S14 acceptance through public daemon composition and real profile databases."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import text
from storage.models.belief import BeliefORM
from storage.models.fact import FactORM
from storage.models.receipt import MemoryReceiptORM

from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import HttpConfig, RequestContext
from memory_server.runtime.daemon_cli import run_daemon
from memory_server.transports.http.app import create_http_app
from tests.gateway.test_http_shell import code, request
from tests.gateway.test_slice_runtime import SocketServer, build_registry, free_port, wait_started

pytestmark = pytest.mark.asyncio
ROOT = "00000000-0000-0000-0000-000000000001"
CHILD = "00000000-0000-0000-0000-000000000002"
OTHER = "00000000-0000-0000-0000-000000000003"
TABLES = (
    "facts",
    "beliefs",
    "lifecycle_states",
    "lifecycle_events",
    "receipts",
    "outbox_entries",
    "http_request_journal",
)


@pytest.fixture
async def lifecycle(tmp_path, monkeypatch):
    from storage.outbox_worker import OutboxWorker

    from memory_server.providers.embedding_provider import MockEmbeddingProvider
    from memory_server.runtime import runtime_factory

    monkeypatch.setitem(runtime_factory._DEFAULT_RECIPES, "embedder", lambda runtime: MockEmbeddingProvider())
    gate, workers = asyncio.Event(), []
    original = OutboxWorker.run

    async def paused(worker):
        workers.append(worker)
        await gate.wait()
        await original(worker)

    monkeypatch.setattr(OutboxWorker, "run", paused)
    registry, _ = await build_registry(tmp_path)
    config = HttpConfig(
        control_path=tmp_path / "control" / "control.sqlite",
        port=free_port(),
        rate_per_minute=60000,
        rate_burst=1000,
        retrieval_concurrency=16,
    )
    state = SimpleNamespace(server=None, app=None, factories={}, config=config)

    def factory(server_config):
        state.server = SocketServer(server_config)
        state.app = server_config.app
        return state.server

    task = asyncio.create_task(run_daemon(config, registry, factory=create_http_app, server_factory=factory))
    try:
        async with asyncio.timeout(15):
            while state.server is None:
                if task.done():
                    await task
                await asyncio.sleep(0.01)
        await wait_started(state.server)
        state.deps = state.app.dependencies

        def mint(
            permissions=("memory.transition", "memory.invalidate", "memory.read"),
            profiles=("alpha", "beta"),
            expired=False,
        ):
            now = datetime.now(UTC)
            issued = now - timedelta(hours=2) if expired else now
            created = state.deps.tokens._bootstrap_create(
                d.TokenCreate(
                    subject="s14-actor",
                    profiles=list(profiles),
                    permissions=list(permissions),
                    expires_at=issued + timedelta(hours=1),
                ),
                now=issued,
            )
            return f"cmms_{created.token.id}.{created.secret}", created.token.id

        state.mint = mint
        state.bearer, state.token_id = mint()
        state.principal = await state.deps.tokens.authenticate(state.bearer, now=datetime.now(UTC))
        for profile in ("alpha", "beta"):
            ctx = RequestContext("seed-s14", state.principal, profile, None, asyncio.get_running_loop().time() + 60)
            async with state.deps.leases.acquire(ctx) as scope:
                state.factories[profile] = scope.application.memory.session_factory
        yield state
    finally:
        for worker in workers:
            worker.stop()
        gate.set()
        if state.server is not None:
            state.server.should_exit = True
        await asyncio.wait_for(task, 30)


async def seed(h, ident=ROOT, kind="fact", profile="alpha", state="active", version=1, confidence=0.8, receipt=True):
    """Synthetic storage characterization, not the application-ingestion fixture.

    Residual: fixed cross-profile IDs, arbitrary persisted state/version, missing
    receipts, and a 0.1 belief confidence cannot all be requested via remember /
    set_belief. Keep those exact fixtures for legacy/CAS/corruption tests. The
    fact happy path below separately seeds via the real application API/lease.
    """
    async with h.factories[profile]() as session, session.begin():
        common = dict(id=ident, lifecycle_state=state, confidence=confidence, source="S14_SOURCE_CANARY")
        if kind == "fact":
            session.add(
                FactORM(
                    **common,
                    subject="S14_CONTENT_CANARY",
                    predicate="p",
                    object="o",
                    dedup_key=ident,
                    creator="seed",
                    version=str(version),
                )
            )
        else:
            session.add(BeliefORM(**common, proposition="S14_CONTENT_CANARY", tags=["tag"], version=version))
        if receipt:
            session.add(
                MemoryReceiptORM(
                    id=ident,
                    memory_type=kind,
                    source="seed-source",
                    created_by="seed-creator",
                    timestamp=datetime(2026, 1, 1),
                    confidence=confidence,
                    version=str(version),
                    lifecycle_state=state,
                    verification_status="unverified",
                    history=json.dumps([{"kind": "seed"}]),
                )
            )


async def seed_application_fact(h):
    from memory_server.application.contracts import RememberCommand

    ctx = RequestContext("seed-remember", h.principal, "alpha", None, asyncio.get_running_loop().time() + 30)
    async with h.deps.leases.acquire(ctx) as scope:
        result = await scope.application.memory.remember(
            RememberCommand("S14_CONTENT_CANARY", "p", "o", confidence=0.8, source="S14_SOURCE_CANARY")
        )
        return result["fact"].id


async def edges(h, pairs, profile="alpha"):
    async with h.factories[profile]() as session, session.begin():
        for source, target, relation in pairs:
            await session.execute(
                text(
                    "INSERT INTO claim_relations (source_id,target_id,relation_type,created_at) "
                    "VALUES (:s,:t,:r,CURRENT_TIMESTAMP)"
                ),
                dict(s=source, t=target, r=relation),
            )


async def snapshot(h, profile="alpha"):
    async with h.factories[profile]() as session:
        return {
            table: sorted(
                [dict(r) for r in (await session.execute(text(f"SELECT * FROM {table}"))).mappings()],
                key=lambda r: str(r["id"] if "id" in r else r.get("request_id")),
            )
            for table in TABLES
        }


def payload(op="transition", kind="fact", **changes):
    result = dict(memory_type=kind, reason="S14_REASON_CANARY", expected_version=1, confirmation=True)
    if op == "transition":
        result["to_state"] = "stale"
    return {**result, **changes}


async def post(
    h, op="transition", ident=ROOT, kind="fact", profile="alpha", key=None, body=None, bearer=None, headers=(), **kwargs
):
    return await request(
        h.app,
        f"/api/v1/profiles/{profile}/memories/{ident}/{op}",
        method="POST",
        bearer=bearer or h.bearer,
        body=payload(op, kind) if body is None else body,
        headers=[("Idempotency-Key", key or str(uuid4())), *headers],
        **kwargs,
    )


def delta(before, after, table, count):
    assert len(after[table]) - len(before[table]) == count, (table, before[table], after[table])


async def test_transition_fact_persists_result_and_one_atomic_effect(lifecycle, monkeypatch):
    from memory_server.application.memory_service import MemoryService

    h = lifecycle
    remembered, remember = [], MemoryService.remember

    async def observe(service, command, **kwargs):
        result = await remember(service, command, **kwargs)
        remembered.append(result["fact"].id)
        return result

    monkeypatch.setattr(MemoryService, "remember", observe)
    ident = await seed_application_fact(h)
    assert remembered == [ident], "happy-path seed bypassed application remember"
    before = await snapshot(h)
    response = await post(h, ident=ident)
    assert response.status == 200, response.body
    result, after = response.json, await snapshot(h)
    assert result["from_state"] == "active" and result["to_state"] == "stale"
    assert result["memory"]["revision"] == 2 and result["memory"]["source_version"] == "2"
    assert after["facts"][0]["version"] == "2" and after["facts"][0]["lifecycle_state"] == "stale"
    for table in ("lifecycle_states", "lifecycle_events", "outbox_entries", "http_request_journal"):
        delta(before, after, table, 1)
    receipt = after["receipts"][0]
    assert receipt["version"] == "2"
    assert json.loads(receipt["history"])[:-1] == json.loads(before["receipts"][0]["history"])
    assert len(json.loads(receipt["history"])) == len(json.loads(before["receipts"][0]["history"])) + 1
    assert result["receipt_id"] == ident and result["audit_id"]
    memory = result["memory"]
    assert memory["masked"] and all(memory[field] is None for field in ("source", "creator", "content", "payload"))
    assert memory["projection"] == dict(vector="unknown", graph="unknown", outbox="pending")
    assert after["outbox_entries"][0]["operation"] == "index_fact"


async def test_transition_belief_uses_integer_cas(lifecycle):
    await seed(lifecycle, kind="belief")
    r = await post(lifecycle, kind="belief")
    assert r.status == 200, r.body
    assert type(r.json["memory"]["source_version"]) is int
    s = await snapshot(lifecycle)
    assert s["beliefs"][0]["version"] == 2 and not s["facts"]
    assert s["outbox_entries"][0]["operation"] == "index_belief"


@pytest.mark.parametrize("kind", ["fact", "belief"])
async def test_invalidate_uses_discarded_domain_transition(lifecycle, kind):
    await seed(lifecycle, kind=kind)
    r = await post(lifecycle, op="invalidate", kind=kind)
    assert r.status == 200, r.body
    assert r.json["to_state"] == "discarded" and r.json["memory"]["revision"] == 2
    assert (await snapshot(lifecycle))["facts" if kind == "fact" else "beliefs"][0]["lifecycle_state"] == "discarded"


@pytest.mark.parametrize("state,target", [("candidate", "discarded"), ("discarded", "discarded"), ("stale", "active")])
async def test_invalid_transition_is_422_without_writes(lifecycle, state, target):
    await seed(lifecycle, state=state)
    before = await snapshot(lifecycle)
    code(await post(lifecycle, body=payload(to_state=target)), "E_VALIDATION", 422)
    assert await snapshot(lifecycle) == before


async def forbid_lease(h, monkeypatch):
    def denied(*args, **kwargs):
        pytest.fail("rejected request reached real lease/registry boundary")

    monkeypatch.setattr(h.deps.leases, "acquire", denied)


@pytest.mark.parametrize("op", ["transition", "invalidate"])
async def test_reason_and_confirmation_closed_validation(lifecycle, monkeypatch, op):
    await forbid_lease(lifecycle, monkeypatch)
    bads = [{"reason": value} for value in (None, "", " " * 3, "x" * 2049)]
    bads += [{"confirmation": value} for value in (None, False, 1, "true")]
    bads += [{field: "canary"} for field in ("actor", "force", "path", "provider")]
    bodies = [payload(op, **bad) for bad in bads]
    bodies += [{k: v for k, v in payload(op).items() if k != field} for field in ("reason", "confirmation")]
    for body in bodies:
        code(await post(lifecycle, op=op, body=body), "E_VALIDATION", 422)


@pytest.mark.parametrize("op", ["transition", "invalidate"])
@pytest.mark.parametrize("kind", ["fact", "belief"])
async def test_missing_version_is_428_not_dto_422(lifecycle, monkeypatch, op, kind):
    await seed(lifecycle, kind=kind)
    await seed(lifecycle, OTHER, kind=kind, profile="beta")
    await forbid_lease(lifecycle, monkeypatch)
    for ident in (ROOT, CHILD, OTHER):
        for headers in ([], [("If-Match", f'"m:{kind}:{ident}:1"')]):
            body = payload(op, kind)
            del body["expected_version"]
            r = await post(lifecycle, op, ident, kind, body=body, headers=headers)
            code(r, "E_PRECONDITION_REQUIRED", 428)
            assert r.json["error"]["details"] == {}
    assert not list(lifecycle.deps.access._db.execute("SELECT * FROM access_audit"))


async def test_bad_version_types_are_422(lifecycle, monkeypatch):
    await forbid_lease(lifecycle, monkeypatch)
    for version in (None, True, 0, -1, "1", float("nan"), float("inf")):
        code(await post(lifecycle, body=payload(expected_version=version)), "E_VALIDATION", 422)
    for body in (b"{", b'{"expected_version":NaN}', b'{"expected_version":Infinity}'):
        code(await post(lifecycle, body=body, headers=[("Content-Type", "application/json")]), "E_VALIDATION", 422)
    body = payload(confirmation=False)
    del body["expected_version"]
    code(await post(lifecycle, body=body), "E_VALIDATION", 422)


async def test_stale_version_has_typed_expected_actual(lifecycle):
    await seed(lifecycle, version=2)
    before = await snapshot(lifecycle)
    r = await post(lifecycle)
    code(r, "E_VERSION_CONFLICT", 409)
    assert r.json["error"]["details"] == dict(expected=1, actual=2, id=ROOT)
    assert await snapshot(lifecycle) == before


async def test_if_match_optional_and_agreement(lifecycle):
    await seed(lifecycle)
    await seed(lifecycle, CHILD)
    assert (await post(lifecycle)).status == 200
    assert (await post(lifecycle, ident=CHILD, headers=[("If-Match", f'"m:fact:{CHILD}:1"')])).status == 200
    good = f'"m:fact:{ROOT}:1"'
    for tag in (
        f'"m:belief:{ROOT}:1"',
        f'"m:fact:{OTHER}:1"',
        f'"m:fact:{ROOT}:2"',
        "W/" + good,
        "*",
        good + "," + good,
        "",
        "null",
        f'"m:fact:{ROOT}:01"',
        " " + good,
    ):
        code(await post(lifecycle, headers=[("If-Match", tag)]), "E_VALIDATION", 422)
    code(await post(lifecycle, headers=[("If-Match", good), ("If-Match", good)]), "E_VALIDATION", 422)


@pytest.mark.parametrize("op", ["transition", "invalidate"])
async def test_decision_skill_rejected_before_lookup(lifecycle, monkeypatch, op):
    from memory_server.transports.http.errors import HttpError

    await forbid_lease(lifecycle, monkeypatch)
    operation = "memories." + op
    payload_class = d.TransitionRequest if op == "transition" else d.InvalidateRequest
    valid = d.COMMAND_TYPES[operation](
        operation=operation, profile_id="alpha", path_ids={"id": ROOT},
        payload=payload_class(**payload(op)),
    )
    ctx = RequestContext("forged-type", lifecycle.principal, "alpha", uuid4(), asyncio.get_running_loop().time() + 30)
    for kind in ("decision", "skill"):
        code(await post(lifecycle, op=op, kind=kind), "E_VALIDATION", 422)
        forged = valid.model_copy(update={"payload": valid.payload.model_copy(update={"memory_type": kind})})
        with pytest.raises(HttpError, match="E_VALIDATION"):
            await port(lifecycle).execute(ctx, forged)


async def test_scoped_missing_and_foreign_id_are_404(lifecycle):
    await seed(lifecycle, profile="beta")
    before = await snapshot(lifecycle, "beta")
    code(await post(lifecycle), "E_NOT_FOUND", 404)
    await seed(lifecycle, CHILD)
    code(await post(lifecycle, ident=CHILD, kind="belief"), "E_NOT_FOUND", 404)
    assert await snapshot(lifecycle, "beta") == before


async def test_permission_separation_for_each_operation(lifecycle, monkeypatch):
    await forbid_lease(lifecycle, monkeypatch)
    for permission in ("memory.write", "memory.read", "memory.transition", "memory.invalidate"):
        bearer, _ = lifecycle.mint(permissions=(permission,))
        for op in ("transition", "invalidate"):
            if permission == "memory." + op:
                continue
            code(
                await post(lifecycle, op=op, bearer=bearer, ident="bad", body=b"{", key="bad"),
                "E_PERMISSION_DENIED",
                403,
            )


async def test_replay_x10_returns_original_snapshot(lifecycle):
    await seed(lifecycle)
    key = str(uuid4())
    first = await post(lifecycle, key=key)
    assert first.status == 200, first.body
    second = await post(lifecycle, body=payload(to_state="archived", expected_version=2))
    assert second.status == 200, second.body
    before = await snapshot(lifecycle)
    for _ in range(10):
        replay = await post(lifecycle, key=key)
        assert replay.status == 200 and replay.body == first.body
        assert replay.headers["idempotency-replayed"] == "true"
        assert replay.headers["x-original-request-id"] == first.json["request_id"]
        assert replay.headers["x-request-id"] != first.headers["x-request-id"]
    assert await snapshot(lifecycle) == before


async def test_same_key_changed_body_or_resource_conflicts(lifecycle):
    await seed(lifecycle)
    await seed(lifecycle, CHILD)
    key = str(uuid4())
    assert (await post(lifecycle, key=key)).status == 200
    for kwargs in (dict(body=payload(reason="changed")), dict(body=payload(to_state="discarded")), dict(ident=CHILD)):
        code(await post(lifecycle, key=key, **kwargs), "E_IDEMPOTENCY_CONFLICT", 409)
    await seed(lifecycle, profile="beta")
    assert (await post(lifecycle, key=key, profile="beta")).status == 200
    assert (await post(lifecycle, op="invalidate", ident=CHILD, key=key)).status == 200


async def test_idempotency_header_and_content_type(lifecycle):
    path = f"/api/v1/profiles/alpha/memories/{ROOT}/transition"
    code(
        await request(lifecycle.app, path, method="POST", bearer=lifecycle.bearer, body=payload()),
        "E_IDEMPOTENCY_REQUIRED",
        400,
    )
    code(await post(lifecycle, key="bad"), "E_VALIDATION", 422)
    code(await post(lifecycle, headers=[("Idempotency-Key", str(uuid4()))]), "E_VALIDATION", 422)
    code(await post(lifecycle, body=b"{}", headers=[("Content-Type", "text/plain")]), "E_UNSUPPORTED_MEDIA_TYPE", 415)
    for method in ("GET", "PUT", "DELETE"):
        assert (await request(lifecycle.app, path, method=method, bearer=lifecycle.bearer)).status == 405
    assert (await post(lifecycle, op="unknown")).status == 404
    code(await post(lifecycle, query=b"force=true"), "E_VALIDATION", 422)
    await seed(lifecycle)
    assert (
        await post(
            lifecycle,
            body=json.dumps(payload()).encode(),
            headers=[("Content-Type", "application/json; charset=utf-8")],
        )
    ).status == 200


async def test_direct_derived_from_propagation_only(lifecycle):
    h = lifecycle
    await seed(h)
    await seed(h, CHILD, confidence=0.81234567)
    await seed(h, OTHER, "belief", confidence=0.1)
    extra = [str(uuid4()) for _ in range(4)]
    for ident in extra:
        await seed(h, ident)
    await edges(
        h,
        [
            (CHILD, ROOT, "derived_from"),
            (OTHER, ROOT, "derived_from"),
            (extra[0], CHILD, "derived_from"),
            (extra[1], ROOT, "supports"),
            (ROOT, extra[2], "derived_from"),
            (str(uuid4()), ROOT, "derived_from"),
        ],
    )
    r = await post(h, op="invalidate")
    assert r.status == 200, r.body
    assert {p["memory_id"] for p in r.json["propagated"]} == {CHILD, OTHER}
    for p in r.json["propagated"]:
        assert (p["action"], p["reason"], p["revision"]) == ("confidence_demoted", "parent_invalidated", 2)
    s = await snapshot(h)
    facts = {f["id"]: f for f in s["facts"]}
    assert facts[CHILD]["confidence"] == 0.649877 and facts[CHILD]["lifecycle_state"] == "active"
    assert s["beliefs"][0]["confidence"] == 0.1
    assert all(facts[i]["version"] == "1" for i in extra)


@pytest.mark.parametrize("target", ["superseded", "contradicted", "discarded", "stale"])
async def test_all_invalidating_targets_propagate(lifecycle, target):
    await seed(lifecycle)
    await seed(lifecycle, CHILD)
    await edges(lifecycle, [(CHILD, ROOT, "derived_from")])
    r = await post(lifecycle, body=payload(to_state=target))
    assert r.status == 200, r.body
    assert len(r.json["propagated"]) == (0 if target == "stale" else 1)


@pytest.fixture
def prepared_graph_registry(monkeypatch):
    """Publish a real graph before RuntimeManager captures file identity."""
    from memory_server.providers.graph_provider import SimpleGraph

    original = build_registry

    async def prepare(tmp_path):
        registry, roots = await original(tmp_path)
        graph = SimpleGraph(snapshot_path=roots["alpha"] / "graph.json")
        for ident, kind in ((ROOT, "fact"), (CHILD, "fact"), (OTHER, "belief")):
            graph.add_node(id=ident, type=kind, name=ident, attributes={})
        graph.add_edge(source_id=CHILD, target_id=ROOT, relation="derived_from")
        graph.add_edge(source_id=ROOT, target_id=OTHER, relation="derived_from")
        return registry, roots

    monkeypatch.setitem(globals(), "build_registry", prepare)


@pytest.fixture
async def graph_lifecycle(prepared_graph_registry, lifecycle):
    return lifecycle


async def graph_only_dependents(h):
    """Read the lease's real graph, with no canonical SQL relations."""
    from memory_server.providers.graph_provider import SimpleGraph

    await seed(h)
    await seed(h, CHILD)
    await seed(h, OTHER, "belief")
    ctx = RequestContext("graph-parity", h.principal, "alpha", None, asyncio.get_running_loop().time() + 30)
    async with h.deps.leases.acquire(ctx) as scope:
        graph = await scope.application.lifecycle._graph()
        assert isinstance(graph, SimpleGraph)
        # Native compatibility includes legacy reversed graph edges.
        assert {(edge.source_id, edge.target_id) for edge in graph.search_by_relation("derived_from")} == {
            (CHILD, ROOT), (ROOT, OTHER)
        }
    async with h.factories["alpha"]() as session:
        assert (await session.execute(text("SELECT count(*) FROM claim_relations"))).scalar_one() == 0
    return graph


@pytest.mark.parametrize("lookup", ["error", "empty"])
async def test_strict_sql_lookup_failure_is_not_success(graph_lifecycle, monkeypatch, lookup):
    lifecycle = graph_lifecycle
    from sqlalchemy.exc import OperationalError
    from sqlalchemy.ext.asyncio import AsyncSession

    from memory_server.application.lifecycle_service import LifecycleService as ApplicationLifecycleService

    graph = await graph_only_dependents(lifecycle)
    before = await snapshot(lifecycle)
    original, calls = AsyncSession.execute, []
    graph_calls = []

    def forbidden_graph(*args, **kwargs):
        graph_calls.append((args, kwargs))
        pytest.fail("strict remote lifecycle touched graph authority")

    async def forbidden_acquire(*args, **kwargs):
        forbidden_graph(*args, **kwargs)

    async def fault(session, statement, *args, **kwargs):
        if "SELECT source_id FROM claim_relations" in str(statement):
            calls.append(str(statement))
            if lookup == "error":
                raise OperationalError("S14_CANARY_SQL", {}, Exception("failed"))
        return await original(session, statement, *args, **kwargs)

    with monkeypatch.context() as m:
        m.setattr(AsyncSession, "execute", fault)
        m.setattr(ApplicationLifecycleService, "_graph", forbidden_acquire)
        m.setattr(graph, "search_by_relation", forbidden_graph)
        response = await post(lifecycle, op="invalidate")
    assert len(calls) == 1 and graph_calls == []
    after = await snapshot(lifecycle)
    if lookup == "error":
        code(response, "E_STORAGE", 503)
        assert after == before
    else:
        assert response.status == 200, response.body
        assert response.json["propagated"] == []
        assert next(row for row in after["facts"] if row["id"] == CHILD) == next(
            row for row in before["facts"] if row["id"] == CHILD
        )
        assert after["beliefs"] == before["beliefs"]
        assert {row["memory_id"] for row in after["lifecycle_events"]} == {ROOT}
        assert {row["record_id"] for row in after["outbox_entries"]} == {ROOT}


@pytest.mark.parametrize("parent_receipt", [True, False])
async def test_receipts_and_outbox_match_persisted_effects(lifecycle, parent_receipt):
    await seed(lifecycle, receipt=parent_receipt)
    await seed(lifecycle, CHILD, "belief")
    await edges(lifecycle, [(CHILD, ROOT, "derived_from")])
    r = await post(lifecycle, op="invalidate")
    assert r.status == 200, r.body
    s = await snapshot(lifecycle)
    assert len(s["receipts"]) == (2 if parent_receipt else 1)
    assert r.json["receipt_id"] == (ROOT if parent_receipt else None)
    assert len(s["outbox_entries"]) == 2
    events = {e["id"]: e for e in s["lifecycle_events"]}
    for receipt in s["receipts"]:
        assert (receipt["source"], receipt["created_by"], receipt["verification_status"]) == (
            "seed-source",
            "seed-creator",
            "unverified",
        )
        history = json.loads(receipt["history"])
        assert history[0] == {"kind": "seed"} and len(history) == 2
        assert history[1]["id"] in events and history[1]["revision"] == 2
        assert receipt["version"] == "2"
    outbox = {e["record_id"]: json.loads(e["payload_json"]) for e in s["outbox_entries"]}
    assert outbox[CHILD] == dict(
        proposition="S14_CONTENT_CANARY", confidence=0.64, tags=["tag"], source="S14_SOURCE_CANARY"
    )
    assert set(outbox[ROOT]) == {"subject", "predicate", "object", "source"}


@pytest.mark.parametrize("count", [0, 1, 512, 513])
async def test_propagation_bound_512_and_overflow_513(lifecycle, monkeypatch, count):
    from sqlalchemy.ext.asyncio import AsyncSession

    await seed(lifecycle)
    ids = [str(uuid4()) for _ in range(count)]
    # Include fully hydrated fanout as well as dangling references in the bound.
    if count == 512:
        async with lifecycle.factories["alpha"]() as session, session.begin():
            session.add_all(
                [
                    FactORM(
                        id=ident,
                        subject="bound",
                        predicate="p",
                        object="o",
                        dedup_key=ident,
                        confidence=0.8,
                        version="1",
                        lifecycle_state="active",
                    )
                    for ident in ids
                ]
            )
    await edges(lifecycle, [(ident, ROOT, "derived_from") for ident in ids])
    before = await snapshot(lifecycle)
    original, queries = AsyncSession.execute, []

    async def trace(session, statement, *args, **kwargs):
        result = await original(session, statement, *args, **kwargs)
        if "SELECT source_id FROM claim_relations" in str(statement):
            frozen = result.freeze()
            queries.append((str(statement), len(frozen().scalars().all())))
            return frozen()
        return result

    monkeypatch.setattr(AsyncSession, "execute", trace)
    r = await post(lifecycle, op="invalidate")
    assert len(queries) == 1 and "LIMIT 513" in queries[0][0]
    assert queries[0][1] == count and queries[0][1] <= 513
    if count == 513:
        code(r, "E_STORAGE", 503)
        assert await snapshot(lifecycle) == before
    else:
        assert r.status == 200, r.body
        assert len(r.json["propagated"]) == (512 if count == 512 else 0)
        if count == 512:
            after = await snapshot(lifecycle)
            assert len(after["lifecycle_events"]) == len(after["outbox_entries"]) == 513


@pytest.mark.parametrize("mode", ["native", "http"])
async def test_self_edge_and_cycle_characterization(lifecycle, mode):
    await seed(lifecycle)
    await seed(lifecycle, CHILD)
    await edges(lifecycle, [(ROOT, ROOT, "derived_from"), (CHILD, ROOT, "derived_from"), (ROOT, CHILD, "derived_from")])
    if mode == "native":
        from memory_server.services.lifecycle_service import LifecycleService

        ctx = RequestContext("native-self", lifecycle.principal, "alpha", None, asyncio.get_running_loop().time() + 30)
        async with lifecycle.deps.leases.acquire(ctx) as scope:
            result = await LifecycleService(scope.application.lifecycle._provider).transition(ROOT, "fact", "discarded")
        assert int(result.memory.version) == 2
        assert [int(p["version"]) for p in result.propagated] == [3, 2]
        after = await snapshot(lifecycle)
        assert len(after["lifecycle_events"]) == 3 and len(after["outbox_entries"]) == 0
        assert next(f for f in after["facts"] if f["id"] == ROOT)["version"] == "3"
        return
    r = await post(lifecycle, op="invalidate")
    assert r.status == 200, r.body
    assert r.json["memory"]["revision"] == 3
    assert [p["revision"] for p in r.json["propagated"]] == [3, 2]
    s = await snapshot(lifecycle)
    assert len(s["lifecycle_events"]) == 3 and len(s["outbox_entries"]) == 2
    receipt = next(row for row in s["receipts"] if row["id"] == ROOT)
    assert [entry["revision"] for entry in json.loads(receipt["history"])[1:]] == [2, 3]


def port(h):
    return h.deps.ports["memories.transition"]


async def test_auth_revoke_expiry_and_wrong_profile(lifecycle):
    h = lifecycle
    await seed(h)
    key = str(uuid4())
    assert (await post(h, key=key)).status == 200
    for bearer, expected_code, expected_status in (
        ("malformed", "E_AUTH_REQUIRED", 401),
        (h.mint(expired=True)[0], "E_AUTH_REQUIRED", 401),
        (h.mint(profiles=("beta",))[0], "E_PERMISSION_DENIED", 403),
    ):
        code(await post(h, bearer=bearer), expected_code, expected_status)
    h.deps.tokens._unchecked_revoke(
        h.deps.tokens._local_context(datetime.now(UTC)),
        h.token_id,
        d.ControlMutation(reason="revoke", confirmation=True, expected_generation=1),
        datetime.now(UTC),
        local=True,
    )
    code(await post(h, key=key), "E_AUTH_REQUIRED", 401)
    fresh, _ = h.mint(profiles=("unknown",))
    code(await post(h, profile="unknown", bearer=fresh), "E_PROFILE_NOT_FOUND", 404)
    code(
        await request(
            h.app,
            f"/api/v1/profiles/alpha/memories/{ROOT}/transition",
            method="POST",
            body=payload(),
            headers=[("Idempotency-Key", str(uuid4()))],
        ),
        "E_AUTH_REQUIRED",
        401,
    )
    admin, _ = h.mint(permissions=("profile.manage",))
    disabled = await request(
        h.app,
        "/api/v1/profiles/alpha/disable",
        method="POST",
        bearer=admin,
        body=dict(reason="test", confirmation=True, expected_generation=1),
        headers=[("Idempotency-Key", str(uuid4()))],
    )
    assert disabled.status == 200, disabled.body
    bearer, _ = h.mint()
    code(await post(h, bearer=bearer), "E_PROFILE_DISABLED", 409)


async def test_path_ids_and_forged_context_are_revalidated(lifecycle, monkeypatch):
    from dataclasses import replace

    from memory_server.transports.http.errors import HttpError

    h = lifecycle
    await forbid_lease(h, monkeypatch)
    for ident in ("bad", "rel_" + ROOT, ROOT.replace("-", ""), "AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA"):
        code(await post(h, ident=ident), "E_VALIDATION", 422)
    command = d.COMMAND_TYPES["memories.transition"](
        operation="memories.transition",
        profile_id="alpha",
        path_ids={"id": ROOT},
        payload=d.TransitionRequest(**payload()),
    )
    ctx = RequestContext("direct", h.principal, "alpha", uuid4(), asyncio.get_running_loop().time() + 30)
    for changed in (replace(ctx, deadline=float("inf")), replace(ctx, deadline=True), replace(ctx, profile_id="beta")):
        with pytest.raises(HttpError):
            await port(h).execute(changed, command)
    for paths in ({}, {"id": ROOT, "extra": ROOT}, {"id": "bad"}):
        forged = command.model_copy(update={"path_ids": paths})
        with pytest.raises(HttpError):
            await port(h).execute(ctx, forged)


@pytest.mark.parametrize("failure", ["false", "raise", "hang"])
async def test_postcommit_publish_failure_keeps_success_and_pending(lifecycle, monkeypatch, failure):
    h = lifecycle
    await seed(h)
    await seed(h, CHILD)
    original, calls = port(h)._publish_outcome, []

    async def failed(ticket, outcome):
        calls.append(ticket)
        if failure == "raise":
            raise RuntimeError("publish unavailable")
        if failure == "hang":
            await asyncio.Event().wait()
        return False

    monkeypatch.setattr(port(h), "_publish_outcome", failed)
    key = str(uuid4())
    first = await post(h, key=key)
    assert first.status == 200, first.body
    s = await snapshot(h)
    assert s["http_request_journal"][0]["pending_publication"] == 1
    code(await post(h, ident=CHILD), "E_AUDIT_UNAVAILABLE", 503)
    assert await snapshot(h) == s
    monkeypatch.setattr(port(h), "_publish_outcome", original)
    replay = await post(h, key=key)
    assert replay.body == first.body
    assert (await snapshot(h))["http_request_journal"][0]["pending_publication"] == 0
    assert len(calls) == 1


async def test_replay_does_not_publish_current_ticket(lifecycle, monkeypatch):
    h = lifecycle
    await seed(h)
    original, calls = port(h)._publish_outcome, []

    async def track(ticket, outcome):
        calls.append(ticket)
        return await original(ticket, outcome) if len(calls) > 1 else False

    monkeypatch.setattr(port(h), "_publish_outcome", track)
    key = str(uuid4())
    first = await post(h, key=key)
    replay = await post(h, key=key)
    assert first.status == replay.status == 200 and first.body == replay.body
    assert len(calls) == 2
    assert all(t.id == first.json["audit_id"] and t.request_id == first.json["request_id"] for t in calls)
    rows = [dict(r) for r in h.deps.access._db.execute("SELECT * FROM access_audit")]
    assert len(rows) == 2 and all(r["state"] == "terminal" for r in rows)
    assert len((await snapshot(h))["lifecycle_events"]) == 1


async def test_ticket_missing_or_begin_failure_prevents_lease(lifecycle, monkeypatch):
    from memory_server.transports.http.errors import HttpError

    h = lifecycle
    await forbid_lease(h, monkeypatch)

    async def failed(*args, **kwargs):
        raise RuntimeError("S14_AUDIT_CANARY")

    with monkeypatch.context() as m:
        m.setattr(h.deps.access, "begin", failed)
        code(await post(h), "E_AUDIT_UNAVAILABLE", 503)
    monkeypatch.setattr(port(h), "_ticket_provider", lambda: None)
    command = d.COMMAND_TYPES["memories.transition"](
        operation="memories.transition",
        profile_id="alpha",
        path_ids={"id": ROOT},
        payload=d.TransitionRequest(**payload()),
    )
    with pytest.raises(HttpError, match="E_AUDIT_UNAVAILABLE"):
        await port(h).execute(
            RequestContext("direct", h.principal, "alpha", uuid4(), asyncio.get_running_loop().time() + 30), command
        )


@pytest.mark.parametrize("boundary", ["root_event", "dependent_event", "receipt", "outbox", "journal", "result"])
async def test_each_precommit_failure_rolls_back_all_tables(lifecycle, monkeypatch, boundary):
    from storage.outbox import OutboxRepository
    from storage.repositories.http_request_repo import HttpRequestRepository
    from storage.repositories.lifecycle_repo import LifecycleRepository
    from storage.repositories.receipt_repo import ReceiptRepository

    from memory_server.application.remote_lifecycle import LifecycleHandler

    h = lifecycle
    await seed(h)
    await seed(h, CHILD)
    await seed(h, OTHER)
    await edges(h, [(CHILD, ROOT, "derived_from"), (OTHER, ROOT, "derived_from")])
    before = await snapshot(h)
    owner, method = {
        "root_event": (LifecycleRepository, "record_event"),
        "dependent_event": (LifecycleRepository, "record_event"),
        "receipt": (ReceiptRepository, "update"),
        "outbox": (OutboxRepository, "add_entry"),
        "journal": (HttpRequestRepository, "add"),
        "result": (LifecycleHandler, "apply"),
    }[boundary]
    original, calls = getattr(owner, method), []

    async def fault(self, *args, **kwargs):
        result = await original(self, *args, **kwargs)
        calls.append(result)
        if boundary == "dependent_event" and len(calls) < 3:
            return result
        if boundary == "result":
            return result.model_copy(update={"memory_id": None})
        raise RuntimeError("S14_FAILURE_CANARY")

    with monkeypatch.context() as m:
        m.setattr(owner, method, fault)
        r = await post(h, op="invalidate")
        assert r.status == (503 if boundary == "outbox" else 500), r.body
    assert calls and await snapshot(h) == before


async def test_committed_event_metadata_only_and_no_replay_produce(lifecycle, monkeypatch):
    h = lifecycle
    await seed(h)
    await seed(h, CHILD)
    await edges(h, [(CHILD, ROOT, "derived_from")])
    published, verified = [], []

    async def publish(event):
        published.append(event)
        s = await snapshot(h)
        assert event.id in {e["id"] for e in s["lifecycle_events"]}
        assert len(s["http_request_journal"]) == 1
        verified.append(event.id)

    monkeypatch.setattr(port(h), "_events", SimpleNamespace(publish=publish))
    key = str(uuid4())
    first = await post(h, op="invalidate", key=key)
    assert first.status == 200, first.body
    assert [e.type for e in published] == ["memory.invalidated", "memory.updated"]
    assert [e.data.revision for e in published] == [2, 2]
    assert set(verified) == {e.id for e in published}
    for e in published:
        wire = e.model_dump_json()
        assert len(wire.encode()) < 4096 and "CANARY" not in wire
        assert e.request_id == first.json["request_id"] and e.profile_id == "alpha"
        assert e.data.operation == "memories.invalidate"
    assert (await post(h, op="invalidate", key=key)).body == first.body
    assert len(published) == 2


@pytest.mark.parametrize("failure", ["raise", "hang"])
async def test_c8_sink_failure_is_joined_and_independent_of_audit(lifecycle, monkeypatch, failure):
    h = lifecycle
    await seed(h)
    await seed(h, CHILD)
    await edges(h, [(CHILD, ROOT, "derived_from")])
    attempts, settled = [], []

    async def publish(event):
        attempts.append(event.id)
        try:
            if failure == "raise":
                raise RuntimeError("sink unavailable")
            await asyncio.Event().wait()
        finally:
            settled.append(event.id)

    monkeypatch.setattr(port(h), "_events", SimpleNamespace(publish=publish))
    key = str(uuid4())
    r = await post(h, op="invalidate", key=key)
    assert r.status == 200, r.body
    assert len(attempts) == 2 and set(settled) == set(attempts)
    s = await snapshot(h)
    assert s["http_request_journal"][0]["pending_publication"] == 0
    assert (await post(h, op="invalidate", key=key)).body == r.body
    assert len(attempts) == 2


async def test_c8_readback_mismatch_is_best_effort(lifecycle, monkeypatch):
    from sqlalchemy.ext.asyncio import AsyncSession

    h = lifecycle
    await seed(h)
    original, calls = AsyncSession.execute, []

    async def publish(event):
        calls.append(event)

    async def fault(session, statement, *args, **kwargs):
        if "lifecycle_events.id IN" in str(statement):
            raise RuntimeError("readback unavailable")
        return await original(session, statement, *args, **kwargs)

    monkeypatch.setattr(port(h), "_events", SimpleNamespace(publish=publish))
    with monkeypatch.context() as m:
        m.setattr(AsyncSession, "execute", fault)
        r = await post(h)
        assert r.status == 200, r.body
    assert calls == [] and len((await snapshot(h))["lifecycle_events"]) == 1


async def test_mount_and_missing_ports_fail_closed(lifecycle, monkeypatch):
    from dataclasses import replace

    from memory_server.transports.http.routes.lifecycle import create_lifecycle_routes

    h = lifecycle
    assert len(h.app.routes) == 38
    assert h.deps.ports["memories.transition"] is h.deps.ports["memories.invalidate"]
    assert h.deps.ports["memories.transition"] is not h.deps.ports["learn"]
    for spec in create_lifecycle_routes():
        with pytest.raises(ValueError):
            create_http_app(h.config, h.deps, mutation_routes=(replace(spec),))
    for op in ("transition", "invalidate"):
        deps = replace(h.deps, ports={k: v for k, v in h.deps.ports.items() if k != "memories." + op})
        app = create_http_app(h.config, deps, mutation_routes=create_lifecycle_routes())
        r = await request(
            app,
            f"/api/v1/profiles/alpha/memories/{ROOT}/{op}",
            method="POST",
            bearer=h.bearer,
            body=payload(op),
            headers=[("Idempotency-Key", str(uuid4()))],
        )
        code(r, "E_CAPABILITY_UNAVAILABLE", 503)


async def test_validation_precedence(lifecycle, monkeypatch):
    h = lifecycle
    await forbid_lease(h, monkeypatch)
    missing = payload()
    del missing["expected_version"]
    code(await post(h, ident="bad", body=missing), "E_VALIDATION", 422)
    path = "/api/v1/profiles/alpha/memories/bad/transition"
    code(await request(h.app, path, method="POST", bearer=h.bearer, body=missing), "E_IDEMPOTENCY_REQUIRED", 400)
    for tag in ("", "null", "*", 'W/"bad"'):
        code(await post(h, body=missing, headers=[("If-Match", tag)]), "E_VALIDATION", 422)
    code(await post(h, body=missing), "E_PRECONDITION_REQUIRED", 428)
    assert not list(h.deps.access._db.execute("SELECT * FROM access_audit"))


async def test_lifecycle_ttl_and_optional_header_same_digest(lifecycle, monkeypatch):
    h = lifecycle
    await seed(h)
    monkeypatch.setattr(port(h), "_ttl", 7)
    key = str(uuid4())
    first = await post(h, key=key)
    r = await post(h, key=key, headers=[("If-Match", f'"m:fact:{ROOT}:1"')])
    assert r.status == 200 and r.body == first.body
    row = (await snapshot(h))["http_request_journal"][0]
    assert (datetime.fromisoformat(row["expires_at"]) - datetime.fromisoformat(row["created_at"])).total_seconds() == 7


async def test_production_routes_have_no_provider_construction(lifecycle):
    import ast
    import inspect

    from memory_server.transports.http.routes import lifecycle as routes

    tree = ast.parse(inspect.getsource(routes))
    imports = [n.module or "" for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)]
    assert not any("provider" in module or "runtime" in module for module in imports)
    code(await post(lifecycle, ident="bad"), "E_VALIDATION", 422)


async def test_limits_headers_and_canary_redaction(lifecycle, caplog):
    h = lifecycle
    await seed(h)
    r = await post(h)
    assert r.status == 200, r.body
    assert r.headers["cache-control"] == "no-store"
    assert r.headers["x-content-type-options"] == "nosniff" and r.headers["referrer-policy"] == "no-referrer"
    assert b"CANARY" not in r.body and "CANARY" not in caplog.text
    row = (await snapshot(h))["http_request_journal"][0]
    assert "CANARY" not in row["response_snapshot"]
    oversized = await post(h, body=b"x" * (h.config.max_body_bytes + 1), headers=[("Content-Type", "application/json")])
    assert oversized.status == 413
    from dataclasses import replace

    from memory_server.transports.http.routes.lifecycle import create_lifecycle_routes

    app = create_http_app(
        replace(
            h.config,
            request_timeout_seconds=0.02,
            header_timeout_seconds=0.01,
            rate_burst=1,
            rate_per_minute=1,
            allowed_origins=("http://localhost:3000",),
        ),
        h.deps,
        mutation_routes=create_lifecycle_routes(),
    )
    path = f"/api/v1/profiles/alpha/memories/{ROOT}/transition"
    allowed = await request(app, path, method="OPTIONS", headers=[("Origin", "http://localhost:3000")])
    assert allowed.headers["access-control-allow-origin"] == "http://localhost:3000"
    assert "If-Match" in allowed.headers["access-control-allow-headers"]
    denied = await request(app, path, method="OPTIONS", headers=[("Origin", "http://foreign")])
    assert "access-control-allow-origin" not in denied.headers

    async def stall():
        await asyncio.Event().wait()

    timeout = await request(app, path, method="POST", bearer=h.bearer, receive=stall)
    code(timeout, "E_TIMEOUT", 504)
    rate = await request(app, path, method="POST", bearer=h.bearer, body=payload())
    code(rate, "E_RATE_LIMIT", 429)
    assert rate.headers["retry-after"] == "1"


@pytest.mark.parametrize("lookup", ["error", "empty"])
@pytest.mark.parametrize(
    "entrypoint", ["transition", "transition_many", "default", "false", "batch_default", "batch_false"]
)
async def test_native_graph_fallback_default_false_parity(graph_lifecycle, monkeypatch, lookup, entrypoint):
    from sqlalchemy.exc import OperationalError
    from sqlalchemy.ext.asyncio import AsyncSession

    from memory_server.application.contracts import LifecycleCommand
    from memory_server.services.lifecycle_service import LifecycleService, LifecycleTransitionRequest

    h = graph_lifecycle
    graph = await graph_only_dependents(h)
    original, queries, searches = AsyncSession.execute, [], []
    search = graph.search_by_relation

    async def observe_sql(session, statement, *args, **kwargs):
        sql = str(statement)
        if sql.lstrip().startswith("SELECT") and "FROM claim_relations" in sql:
            queries.append(sql)
            if lookup == "error":
                raise OperationalError("S14_NATIVE_SQL", {}, Exception("failed"))
            result = await original(session, statement, *args, **kwargs)
            frozen = result.freeze()
            assert frozen().all() == []
            return frozen()
        return await original(session, statement, *args, **kwargs)

    def observe_graph(relation):
        searches.append(relation)
        return search(relation)

    command = LifecycleCommand(memory_id=ROOT, memory_type="fact", to_state="discarded")
    ctx = RequestContext("native-graph", h.principal, "alpha", None, asyncio.get_running_loop().time() + 30)
    with monkeypatch.context() as m:
        m.setattr(AsyncSession, "execute", observe_sql)
        m.setattr(graph, "search_by_relation", observe_graph)
        async with h.deps.leases.acquire(ctx) as scope:
            application = scope.application.lifecycle
            assert await application._graph() is graph
            if entrypoint == "transition":
                result = await application.transition(command)
            elif entrypoint == "transition_many":
                result = (await application.transition_many([command]))[0]
            else:
                kwargs = {"strict_remote": False} if entrypoint in ("false", "batch_false") else {}
                async with scope.application.memory.session_factory() as session, session.begin():
                    if entrypoint.startswith("batch_"):
                        domain = LifecycleService(application._provider)
                        native_request = LifecycleTransitionRequest(ROOT, "fact", "discarded")
                        outcome = (await domain.transition_many_in_session(
                            session, [native_request], graph=graph, **kwargs
                        ))[0]
                    else:
                        outcome = await application.transition_in_session(session, command, **kwargs)
                    result = application._payload(outcome)
    assert len(queries) == 1 and searches == ["derived_from"]
    assert result["memory"]["lifecycle_state"] == "discarded"
    assert int(result["memory"]["version"]) == 2
    assert result["event"] == dict(
        memory_id=ROOT, memory_type="fact", from_state="active", to_state="discarded",
        reason="lifecycle transition", triggered_by="system",
    )
    assert {item["memory_id"] for item in result["propagated"]} == {CHILD, OTHER}
    for item in result["propagated"]:
        assert item == dict(
            memory_id=item["memory_id"], memory_type="fact" if item["memory_id"] == CHILD else "belief",
            confidence=0.64, version=2, reason="parent_invalidated",
        )
    after = await snapshot(h)
    facts = {row["id"]: row for row in after["facts"]}
    assert (facts[ROOT]["lifecycle_state"], facts[ROOT]["version"]) == ("discarded", "2")
    assert (facts[CHILD]["lifecycle_state"], facts[CHILD]["version"], facts[CHILD]["confidence"]) == (
        "active", "2", 0.64
    )
    belief = after["beliefs"][0]
    assert (belief["lifecycle_state"], belief["version"], belief["confidence"]) == ("active", 2, 0.64)
    assert {row["memory_id"] for row in after["lifecycle_events"]} == {ROOT, CHILD, OTHER}
    assert len(after["lifecycle_events"]) == 3
    assert after["outbox_entries"] == after["http_request_journal"] == []


async def test_legacy_lifecycle_observable_parity(lifecycle):
    from memory_server.services.lifecycle_service import LifecycleService, LifecycleTransitionRequest

    h = lifecycle
    await seed(h)
    await seed(h, CHILD)
    ctx = RequestContext("native", h.principal, "alpha", None, asyncio.get_running_loop().time() + 30)
    async with h.deps.leases.acquire(ctx) as scope:
        service = LifecycleService(scope.application.lifecycle._provider)
        first = await service.transition(ROOT, "fact", "stale")
        second = await service.transition_many([LifecycleTransitionRequest(CHILD, "fact", "stale")])
        assert first.memory.version == "2" or first.memory.version == 2
        assert first.event["to_state"] == second[0].event["to_state"] == "stale"
        for kind in ("decision", "skill"):
            with pytest.raises(ValueError, match="memory_type must be one of: fact, belief"):
                await service.transition(ROOT, kind, "stale")
    assert len((await snapshot(h))["lifecycle_events"]) == 2


@pytest.mark.parametrize("change", ["version", "state", "vanish", "old", "query_failure"])
async def test_typed_conflict_constructor_and_lost_update_observation(lifecycle, monkeypatch, change):
    from sqlalchemy.exc import OperationalError
    from sqlalchemy.ext.asyncio import AsyncSession
    from storage.repositories.lifecycle_repo import LifecycleConflictError, _cas_transition

    await seed(lifecycle)
    exc = LifecycleConflictError("compatible")
    assert str(exc) == "compatible" and exc.args == ("compatible",)
    original, updates = AsyncSession.execute, []

    async def intercept(session, statement, *args, **kwargs):
        sql = str(statement)
        if sql.startswith("UPDATE facts SET") and not updates:
            updates.append(sql)
            if change == "version":
                await original(session, text("UPDATE facts SET version='3' WHERE id=:id"), {"id": ROOT})
            elif change == "state":
                await original(session, text("UPDATE facts SET lifecycle_state='stale' WHERE id=:id"), {"id": ROOT})
            elif change == "vanish":
                await original(session, text("DELETE FROM facts WHERE id=:id"), {"id": ROOT})
            else:
                # Real SQLite trigger suppresses UPDATE without faking rowcount.
                await original(
                    session,
                    text("CREATE TEMP TRIGGER s14_ignore BEFORE UPDATE ON facts BEGIN SELECT RAISE(IGNORE); END"),
                )
        elif change == "query_failure" and updates and sql.startswith("SELECT facts.version"):
            raise OperationalError("observation", {}, Exception("failed"))
        return await original(session, statement, *args, **kwargs)

    with monkeypatch.context() as m:
        m.setattr(AsyncSession, "execute", intercept)
        async with lifecycle.factories["alpha"]() as session:
            with pytest.raises(OperationalError if change == "query_failure" else LifecycleConflictError) as caught:
                await _cas_transition(session, FactORM, ROOT, "stale", "active", 1)
            if change != "query_failure":
                assert caught.value.expected == 1 and caught.value.memory_id == ROOT
                assert caught.value.actual == {"version": 3, "state": 1, "vanish": None, "old": None}[change]
            await session.rollback()
    assert len(updates) == 1
