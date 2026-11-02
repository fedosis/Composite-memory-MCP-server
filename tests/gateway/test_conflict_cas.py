"""S15B2 resolve through the production daemon, Guard, UoW and SQLite."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from memory_server.gateway_contracts import dto as d
from tests.gateway.test_belief_write_foundation import pause_index_consumer as pause_index_consumer
from tests.gateway.test_http_beliefs import belief_http as belief_http
from tests.gateway.test_http_beliefs import context, error_code, http, ident, orm_seed, set_snapshot, sql
from tests.gateway.test_slice_runtime import offline_embedder as offline_embedder


@pytest.fixture
async def resolve_http(belief_http):
    state = belief_http
    now = datetime.now(UTC)
    token = state.deps.tokens._bootstrap_create(
        d.TokenCreate(
            subject="s15b2",
            profiles=["alpha", "beta"],
            permissions=["belief.read", "belief.write", "belief.resolve"],
            expires_at=now + timedelta(hours=1),
        ),
        now=now,
    )
    state.resolve_bearer = f"cmms_{token.token.id}.{token.secret}"
    await orm_seed(
        state,
        [
            dict(
                id=ident(1),
                proposition="PostgreSQL provides atomic database transactions.",
                confidence=0.8,
                tags=["a"],
                source_ids=["declared:a"],
            ),
            dict(
                id=ident(2),
                proposition="SQLite serializes concurrent database writers.",
                confidence=0.6,
                tags=["a", "b"],
                source_ids=["declared:b"],
            ),
        ],
    )
    return state


def resolve_payload(action="keep_a", **updates):
    return (
        dict(
            belief_a_id=ident(1),
            belief_b_id=ident(2),
            resolution=action,
            new_proposition="Database transactions provide atomic durable updates." if action == "merge" else None,
            expected_versions={ident(1): 1, ident(2): 1},
            reason="  RESOLVE_REASON_CANARY  ",
            confirmation=True,
        )
        | updates
    )


def conflict_path(a=ident(1), b=ident(2)):
    from memory_server.application.remote_beliefs import encode_conflict_id

    return encode_conflict_id(a, b)


async def resolve(state, payload=None, *, key=None, conflict=None, headers=(), **kwargs):
    return await http(
        state,
        f"conflicts/{conflict or conflict_path()}/resolve",
        method="POST",
        bearer=state.resolve_bearer,
        body=resolve_payload() if payload is None else payload,
        headers=(("Idempotency-Key", str(key or uuid4())), *headers),
        **kwargs,
    )


@pytest.mark.parametrize("action", ["keep_a", "keep_b", "merge", "mark_contradicted", "discard_both", "defer"])
async def test_conflicts_resolve_keep_merge_discard_semantics(resolve_http, action):
    state = resolve_http
    response = await resolve(state, resolve_payload(action))
    assert response.status == 200, response.body
    assert response.json["resolution"] == action
    assert len(response.json["memories"]) == (3 if action == "merge" else 2)
    for receipt_id in response.json["receipt_ids"]:
        assert await sql(state, "SELECT id FROM receipts WHERE id=:id", {"id": receipt_id}) == [(receipt_id,)]
    assert b"CANARY" not in response.body
    states = [(m["revision"], m["lifecycle_state"]) for m in response.json["memories"][:2]]
    assert (
        states
        == {
            "keep_a": [(1, "active"), (2, "discarded")],
            "keep_b": [(2, "discarded"), (1, "active")],
            "merge": [(2, "superseded")] * 2,
            "mark_contradicted": [(2, "contradicted")] * 2,
            "discard_both": [(2, "discarded")] * 2,
            "defer": [(1, "active")] * 2,
        }[action]
    )


ACTIONS = ["keep_a", "keep_b", "merge", "mark_contradicted", "discard_both", "defer"]


@pytest.mark.parametrize("action", ACTIONS)
@pytest.mark.parametrize("side", [1, 2])
async def test_both_conflict_versions_checked_in_one_uow(resolve_http, action, side):
    state = resolve_http
    before = await set_snapshot(state)
    versions = {ident(1): 1, ident(2): 1} | {ident(side): 9}
    assert error_code(
        await resolve(state, resolve_payload(action, expected_versions=versions)), "E_VERSION_CONFLICT", 409
    ) == dict(id=ident(side), expected=9, actual=1)
    assert await set_snapshot(state) == before


@pytest.mark.parametrize(
    "action,source,target",
    [
        ("keep_a", 1, 2),
        ("keep_b", 2, 1),
        ("keep_a", 2, 2),
        ("keep_b", 1, 1),
        ("merge", 1, 1),
        ("merge", 2, 1),
        ("mark_contradicted", 1, 2),
        ("discard_both", 2, 1),
    ],
)
async def test_keep_a_derived_from_b_fail_closed_409(resolve_http, action, source, target):
    state = resolve_http
    await sql(
        state,
        "INSERT INTO claim_relations(source_id,target_id,relation_type,created_at) "
        "VALUES(:s,:t,'derived_from',CURRENT_TIMESTAMP)",
        dict(s=ident(source), t=ident(target)),
    )
    before = await set_snapshot(state)
    assert error_code(await resolve(state, resolve_payload(action)), "E_VERSION_CONFLICT", 409) == {}
    assert await set_snapshot(state) == before


async def seed_evidence(state, count):
    from storage.models.belief import EvidenceORM

    async with state.deps.leases.acquire(context(state)) as scope:
        async with scope.application.memory.session_factory() as session, session.begin():
            session.add_all(
                [
                    EvidenceORM(
                        id=ident(i + 100),
                        belief_id=ident(1 if i % 2 else 2),
                        source_type="authority",
                        source_id=f"opaque:{i}",
                        weight=0.0 if i % 2 else 0.7,
                        contributor="CONTRIBUTOR_CANARY",
                        note=f"NOTE_CANARY:{i}",
                        created_at=datetime(2024, 1, 2),
                    )
                    for i in range(count)
                ]
            )


@pytest.mark.parametrize("action", ACTIONS)
async def test_merge_evidence_and_every_returned_receipt_persisted(resolve_http, action):
    import json

    state = resolve_http
    await seed_evidence(state, 3)
    originals = await sql(state, "SELECT * FROM evidence ORDER BY id")
    response = await resolve(state, resolve_payload(action))
    assert response.status == 200, response.body
    for rid in response.json["receipt_ids"]:
        raw = await sql(state, "SELECT id,source,created_by,version,history FROM receipts WHERE id=:id", dict(id=rid))
        assert len(raw) == 1 and raw[0][0] == rid and json.loads(raw[0][4])
    assert response.json["receipt_ids"][0] not in [m["id"] for m in response.json["memories"]]
    if action == "merge":
        new = response.json["memories"][-1]["id"]
        fields = "source_type,source_id,weight,contributor,note,created_at"
        copied = await sql(state, f"SELECT {fields} FROM evidence WHERE belief_id=:id ORDER BY source_id", dict(id=new))
        original_fields = await sql(
            state, f"SELECT {fields} FROM evidence WHERE belief_id<>:id ORDER BY source_id", dict(id=new)
        )
        assert copied == original_fields and len(copied) == 3
        declared = json.loads((await sql(state, "SELECT source_ids FROM beliefs WHERE id=:id", dict(id=new)))[0][0])
        assert declared[:2] == ["declared:a", "declared:b"] and set(declared[2:]) == {
            "opaque:0",
            "opaque:1",
            "opaque:2",
        }
        assert await sql(state, "SELECT * FROM evidence WHERE belief_id<>:id ORDER BY id", dict(id=new)) == originals
    else:
        assert await sql(state, "SELECT * FROM evidence ORDER BY id") == originals


@pytest.mark.parametrize("where", ["select", "insert"])
async def test_merge_evidence_copy_failure_rolls_back_everything(resolve_http, monkeypatch, where):
    from sqlalchemy import event
    from sqlalchemy.exc import OperationalError
    from storage.models.belief import EvidenceORM

    from memory_server.application import remote_beliefs as m

    state = resolve_http
    await seed_evidence(state, 3)
    before, crossed = await set_snapshot(state), []
    if where == "insert":

        def fail(mapper, conn, target):
            if target.belief_id not in (ident(1), ident(2)):
                crossed.append(target.id)
                if len(crossed) == 2:
                    raise OperationalError("copy", {}, RuntimeError("COPY_CANARY"))

        event.listen(EvidenceORM, "after_insert", fail)
        try:
            response = await resolve(state, resolve_payload("merge"))
        finally:
            event.remove(EvidenceORM, "after_insert", fail)
    else:
        original = m.AsyncSession.scalars

        async def fail(self, statement, *a, **kw):
            result = await original(self, statement, *a, **kw)
            if not crossed and "FROM evidence" in str(statement):
                crossed.append(str(statement))
                raise OperationalError("read", {}, RuntimeError("COPY_CANARY"))
            return result

        with monkeypatch.context() as patch:
            patch.setattr(m.AsyncSession, "scalars", fail)
            response = await resolve(state, resolve_payload("merge"))
    error_code(response, "E_STORAGE", 503)
    assert crossed and await set_snapshot(state) == before


@pytest.mark.parametrize("count", [0, 1, 1024, 1025])
async def test_merge_evidence_budget_no_truncation(resolve_http, count):
    state = resolve_http
    await seed_evidence(state, count)
    before = await set_snapshot(state)
    response = await resolve(state, resolve_payload("merge"))
    if count > 1024:
        assert error_code(response, "E_STORAGE", 503) == {"limit": 1024}
        assert await set_snapshot(state) == before
    else:
        assert response.status == 200, response.body
        new = response.json["memories"][-1]["id"]
        assert await sql(state, "SELECT count(*) FROM evidence WHERE belief_id=:id", dict(id=new)) == [(count,)]


async def test_same_key_concurrent_and_duplicate_x10_replay_once(resolve_http):
    import asyncio

    state, key = resolve_http, uuid4()
    payload = resolve_payload("defer")
    responses = await asyncio.gather(resolve(state, payload, key=key), resolve(state, payload, key=key))
    first = next(r for r in responses if r.status == 200)
    for r in responses:
        assert r.status == 200 or (r.status == 409 and r.json["error"]["code"] == "E_REQUEST_IN_PROGRESS")
    assert (await resolve(state)).status == 200
    before = await set_snapshot(state)
    for _ in range(10):
        replay = await resolve(state, payload, key=key)
        assert replay.status == 200 and replay.json == first.json
        assert replay.headers["idempotency-replayed"] == "true"
        assert replay.headers["x-original-request-id"] == first.json["request_id"]
        assert replay.headers["x-request-id"] != first.headers["x-request-id"]
    assert await set_snapshot(state) == before
    error_code(await resolve(state, {**payload, "reason": "changed"}, key=key), "E_IDEMPOTENCY_CONFLICT", 409)


async def test_mark_contradicted_and_defer_are_remote_not_legacy_branches(resolve_http, monkeypatch):
    from memory_server.services.lifecycle_service import LifecycleService

    state, calls = resolve_http, []
    original = LifecycleService.transition_many_in_session

    async def tracked(self, session, requests, **kw):
        calls.append((id(session), [(r.memory_id, r.expected_version, r.to_state) for r in requests]))
        return await original(self, session, requests, **kw)

    monkeypatch.setattr(LifecycleService, "transition_many_in_session", tracked)
    before = await set_snapshot(state)
    first = await resolve(state, resolve_payload("defer"))
    assert first.status == 200 and calls == []
    after = await set_snapshot(state)
    for table in before.keys() - {"receipts", "http_request_journal"}:
        assert before[table] == after[table]
    second = await resolve(state, resolve_payload("mark_contradicted"))
    assert second.status == 200, second.body
    assert len(calls) == 1 and calls[0][1] == [(ident(1), 1, "contradicted"), (ident(2), 1, "contradicted")]
    assert len(await sql(state, "SELECT id FROM lifecycle_events")) == 2


async def test_resolution_two_parent_shared_dependents_policy_ledger(resolve_http):
    import json

    state = resolve_http
    await orm_seed(state, [dict(id=ident(3), proposition="shared dependent", confidence=0.8)])
    await sql(
        state,
        "INSERT INTO claim_relations(source_id,target_id,relation_type,created_at) "
        "VALUES(:d,:a,'derived_from',CURRENT_TIMESTAMP),(:d,:b,'derived_from',CURREN"
        "T_TIMESTAMP)",
        dict(d=ident(3), a=ident(1), b=ident(2)),
    )
    from tests.gateway.test_http_beliefs import set_payload

    response = await http(
        state,
        method="POST",
        bearer=state.resolve_bearer,
        body=set_payload(proposition="shared dependent", confidence=0.8, expected_versions={ident(3): 1}),
        headers=(("Idempotency-Key", str(uuid4())),),
    )
    assert response.status == 200, response.body
    old_indexes = len(await sql(state, "SELECT id FROM outbox_entries"))
    response = await resolve(state, resolve_payload("discard_both"))
    assert response.status == 200, response.body
    assert await sql(state, "SELECT version,confidence FROM beliefs WHERE id=:id", dict(id=ident(3))) == [(4, 0.512)]
    history = json.loads((await sql(state, "SELECT history FROM receipts WHERE id=:id", dict(id=ident(3))))[0][0])
    assert [e["revision"] for e in history[-2:]] == [3, 4]
    assert len({e["id"] for e in history[-2:]}) == 2
    assert len(await sql(state, "SELECT id FROM outbox_entries")) == old_indexes + 3
    assert len(await sql(state, "SELECT id FROM lifecycle_events WHERE memory_id=:id", dict(id=ident(3)))) == 3


@pytest.mark.parametrize("count", [0, 1, 512, 513])
@pytest.mark.parametrize("hydrated", [False, True])
async def test_pair_propagation_overlap_and_bounds(resolve_http, monkeypatch, count, hydrated):
    from storage.repositories.belief_repo import BeliefRepository

    state = resolve_http
    effects, original = [], BeliefRepository.transition_lifecycle_state

    async def tracked(self, *a, **kw):
        effects.append(a)
        return await original(self, *a, **kw)

    monkeypatch.setattr(BeliefRepository, "transition_lifecycle_state", tracked)
    if hydrated:
        await orm_seed(
            state, [dict(id=ident(i + 100), proposition=f"dependent {i}", confidence=0.1) for i in range(count)]
        )
    async with state.deps.leases.acquire(context(state)) as scope:
        from sqlalchemy import text

        async with scope.application.memory.session_factory() as session, session.begin():
            for i in range(count):
                await session.execute(
                    text(
                        "INSERT INTO claim_relations(source_id,target_id,relation_type,created_at) "
                        "VALUES(:s,:t,'derived_from',CURRENT_TIMESTAMP)"
                    ),
                    dict(s=ident(i + 100), t=ident(2)),
                )
    before = await set_snapshot(state)
    response = await resolve(state)
    if count > 512:
        assert error_code(response, "E_STORAGE", 503) == {"limit": 512}
        assert effects == [], "overflow must fail before domain effects, not just roll them back"
        assert await set_snapshot(state) == before
    else:
        assert response.status == 200, response.body
        assert len(await sql(state, "SELECT id FROM lifecycle_events")) == 1 + (count if hydrated else 0)


@pytest.mark.parametrize("boundary", ["domain", "new", "receipt", "action_read", "outbox", "journal"])
async def test_resolution_precommit_fault_matrix_atomic(resolve_http, monkeypatch, boundary):
    from sqlalchemy import text

    from memory_server.application import remote_beliefs as m

    state = resolve_http
    before, crossed = await set_snapshot(state), []
    owner, method = {
        "domain": (m.LifecycleService, "transition_many_in_session"),
        "new": (m.BeliefRepository, "create"),
        "receipt": (m.ReceiptRepository, "create"),
        "action_read": (m.ReceiptRepository, "get"),
        "outbox": (m.OutboxRepository, "add_entry"),
        "journal": (m.HttpRequestRepository, "add"),
    }[boundary]
    original = getattr(owner, method)

    async def broken(self, *a, **kw):
        result = await original(self, *a, **kw)
        if boundary == "action_read" and (result is None or result.id in (ident(1), ident(2))):
            return result
        crossed.append(method)
        if boundary == "action_read":
            await self._session.execute(text("DELETE FROM receipts WHERE id=:id"), dict(id=result.id))
            return result
        raise RuntimeError("FAULT_CANARY")

    with monkeypatch.context() as patch:
        patch.setattr(owner, method, broken)
        response = await resolve(state, resolve_payload("merge"))
    error_code(
        response,
        "E_OUTBOX" if boundary == "outbox" else "E_STORAGE" if boundary == "action_read" else "E_INTERNAL",
        503 if boundary in ("outbox", "action_read") else 500,
    )
    assert crossed and await set_snapshot(state) == before


async def test_postcommit_audit_gap_replay_reconciles_original_ticket(resolve_http, monkeypatch):
    state, key = resolve_http, uuid4()
    port = state.deps.ports["conflicts.resolve"]
    original, calls = port._publish_outcome, []

    async def fail(ticket, outcome):
        calls.append((ticket, outcome))
        return False

    monkeypatch.setattr(port, "_publish_outcome", fail)
    first = await resolve(state, resolve_payload("defer"), key=key)
    assert first.status == 200
    assert calls[0][1].receipt_ids == tuple(first.json["receipt_ids"])
    error_code(await resolve(state), "E_AUDIT_UNAVAILABLE", 503)

    async def heal(ticket, outcome):
        calls.append((ticket, outcome))
        return await original(ticket, outcome)

    monkeypatch.setattr(port, "_publish_outcome", heal)
    replay = await resolve(state, resolve_payload("defer"), key=key)
    assert replay.json == first.json and calls[-1][0] == calls[0][0]
    assert await sql(state, "SELECT pending_publication FROM http_request_journal") == [(0,)]


async def test_defer_and_replay_do_not_emit_memory_outbox_events(resolve_http, monkeypatch):
    state, events, key = resolve_http, [], uuid4()

    class Sink:
        async def publish(self, event):
            assert await sql(state, "SELECT id FROM lifecycle_events WHERE id=:id", dict(id=event.id)) == [(event.id,)]
            events.append(event)

    monkeypatch.setattr(state.deps.ports["conflicts.resolve"], "_events", Sink())
    assert (await resolve(state, resolve_payload("defer"))).status == 200
    assert events == [] and await sql(state, "SELECT id FROM outbox_entries") == []
    first = await resolve(state, resolve_payload("mark_contradicted"), key=key)
    assert first.status == 200 and len(events) == 2
    assert all(e.type == "memory.transitioned" and len(e.model_dump_json().encode()) < 4096 for e in events)
    assert (await resolve(state, resolve_payload("mark_contradicted"), key=key)).json == first.json
    assert len(events) == 2


@pytest.mark.parametrize("count", [1023, 1024])
async def test_receipt_history_budget_no_truncation(resolve_http, count):
    import json

    from storage.repositories.receipt_repo import ReceiptRepository

    from memory_server.models.receipt import MemoryReceipt

    state = resolve_http
    async with state.deps.leases.acquire(context(state)) as scope:
        async with scope.application.memory.session_factory() as session, session.begin():
            await ReceiptRepository(session).create(
                MemoryReceipt(
                    id=ident(2),
                    memory_type="belief",
                    source="fixture",
                    created_by="fixture",
                    timestamp=datetime.now(UTC),
                    history=[dict(kind="legacy")] * count,
                )
            )
    before = await set_snapshot(state)
    response = await resolve(state)
    if count == 1024:
        assert error_code(response, "E_STORAGE", 503) == {"limit": 1024}
        assert await set_snapshot(state) == before
    else:
        assert response.status == 200, response.body
        assert (
            len(json.loads((await sql(state, "SELECT history FROM receipts WHERE id=:id", dict(id=ident(2))))[0][0]))
            == 1024
        )


async def test_write_sql_budget_cleanup_and_session_ownership(resolve_http, monkeypatch):
    from sqlalchemy import text

    from memory_server.application import remote_beliefs as m

    state = resolve_http
    handlers = []

    async def heavy(self, session, *a, **kw):
        handlers.append(self)
        await session.execute(
            text("WITH RECURSIVE x(n) AS (VALUES(0) UNION ALL SELECT n+1 FROM x WHERE n<10000000) SELECT sum(n) FROM x")
        )
        pytest.fail("real SQLite VM budget omitted")

    with monkeypatch.context() as patch:
        patch.setattr(m.BeliefsMutationHandler, "resolve_in_session", heavy)
        assert error_code(await resolve(state), "E_STORAGE", 503) == {"limit": 2000000}
    assert handlers[0].instructions == 2000000
    assert (await resolve(state, resolve_payload("defer"))).status == 200
    assert (await http(state, profile="beta")).status == 200


@pytest.fixture
async def resolve_socket(tmp_path, monkeypatch):
    from sqlalchemy import text

    from tests.gateway.test_http_lifecycle import lifecycle

    async for state in lifecycle.__wrapped__(tmp_path, monkeypatch):
        state.bearer, _ = state.mint(permissions=("belief.write", "belief.read", "belief.resolve"))
        async with state.factories["alpha"]() as session, session.begin():
            await session.execute(
                text(
                    "INSERT INTO "
                    "beliefs(id,proposition,confidence,source,creator,source_ids,tags,created_at"
                    ",updated_at,last_reinforced_at,version,verification_status,lifecycle_state)"
                    " VALUES(:a,'Database atomic "
                    "transactions',0.8,'test','test','[]','[]',CURRENT_TIMESTAMP,CURRENT_TIMESTA"
                    "MP,CURRENT_TIMESTAMP,1,'candidate','active'),(:b,'SQLite serializes "
                    "writes',0.6,'test','test','[]','[]',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CUR"
                    "RENT_TIMESTAMP,1,'candidate','active')"
                ),
                dict(a=ident(1), b=ident(2)),
            )
        yield state


async def socket_resolve(state, payload, *, key=None, profile="alpha"):
    import asyncio
    import json
    from time import monotonic

    from tests.gateway.test_slice_runtime import http_request

    key, deadline = key or uuid4(), monotonic() + 2
    for _ in range(40):
        response = await http_request(
            state.config.port,
            "POST",
            f"/api/v1/profiles/{profile}/conflicts/{conflict_path()}/resolve",
            json.dumps(payload).encode(),
            {
                "Authorization": "Bearer " + state.bearer,
                "Content-Type": "application/json",
                "Idempotency-Key": str(key),
            },
        )
        if response[0] not in (409, 503) or response[1]["error"]["code"] not in {
            "E_REQUEST_IN_PROGRESS",
            "E_AUDIT_UNAVAILABLE",
        }:
            return response
        if monotonic() >= deadline:
            break
        await asyncio.sleep(0.01)
    pytest.fail(f"original-key convergence exhausted: {response}")


async def test_pair_preflight_holds_sqlite_writer_reservation(resolve_socket, monkeypatch):
    import asyncio

    from sqlalchemy import text
    from sqlalchemy.exc import OperationalError

    from memory_server.application.remote_beliefs import BeliefsMutationHandler

    state = resolve_socket
    entered, release, drivers = asyncio.Event(), asyncio.Event(), []
    original = BeliefsMutationHandler.resolve_in_session

    async def held(self, session, *a, **kw):
        conn = await session.connection()
        drivers.append(await conn.run_sync(lambda c: id(c.connection.driver_connection)))
        assert await session.scalar(text("SELECT count(*) FROM http_request_journal")) == 0
        entered.set()
        await release.wait()
        return await original(self, session, *a, **kw)

    with monkeypatch.context() as patch:
        patch.setattr(BeliefsMutationHandler, "resolve_in_session", held)
        first = asyncio.create_task(socket_resolve(state, resolve_payload()))
        try:
            await asyncio.wait_for(entered.wait(), 5)
            async with state.factories["alpha"]() as contender:
                conn = await contender.connection()
                drivers.append(await conn.run_sync(lambda c: id(c.connection.driver_connection)))
                assert drivers[0] != drivers[1]
                await contender.execute(text("PRAGMA busy_timeout=0"))
                with pytest.raises(OperationalError) as exc:
                    await contender.execute(text("UPDATE beliefs SET version=99 WHERE id=:id"), dict(id=ident(1)))
                assert exc.value.orig.sqlite_errorcode & 255 in (5, 6)
        finally:
            release.set()
            response = await asyncio.wait_for(first, 5)
    assert response[0] == 200, response
    loser = await socket_resolve(state, resolve_payload())
    assert loser[0] == 409 and loser[1]["error"]["code"] == "E_VERSION_CONFLICT"


@pytest.mark.parametrize("actions", [("keep_a", "keep_b"), ("keep_a", "merge"), ("merge", "merge")])
@pytest.mark.parametrize("reverse", [False, True])
async def test_concurrent_keep_merge_one_winner(resolve_socket, monkeypatch, actions, reverse):
    import asyncio

    from sqlalchemy import text

    from memory_server.application.remote_beliefs import _BeliefsMutationUoW

    state = resolve_socket
    barrier, original = asyncio.Barrier(2), _BeliefsMutationUoW.run
    arrivals = 0

    async def synchronized(self, *a, **kw):
        nonlocal arrivals
        arrivals += 1
        if arrivals <= 2:
            await barrier.wait()
        return await original(self, *a, **kw)

    payloads = [
        resolve_payload(action, **(dict(belief_a_id=ident(2), belief_b_id=ident(1)) if reverse else {}))
        for action in actions
    ]
    with monkeypatch.context() as patch:
        patch.setattr(_BeliefsMutationUoW, "run", synchronized)
        results = await asyncio.wait_for(asyncio.gather(*(socket_resolve(state, p) for p in payloads)), 8)
    assert sorted(r[0] for r in results) == [200, 409], results
    assert next(r[1]["error"]["code"] for r in results if r[0] == 409) == "E_VERSION_CONFLICT"
    async with state.factories["alpha"]() as session:
        assert await session.scalar(text("SELECT count(*) FROM http_request_journal")) == 1
        assert (
            await session.scalar(
                text(
                    "SELECT count(*) FROM receipts WHERE source='conflict_resolution' AND id "
                    "NOT IN (SELECT id FROM beliefs)"
                )
            )
            == 1
        )


async def test_resolution_cas_precedes_any_effect_and_receipt(resolve_http, monkeypatch):
    from sqlalchemy import text

    from memory_server.application import remote_beliefs as m

    state = resolve_http
    before, crossed = await set_snapshot(state), []
    original = m.BeliefRepository.transition_lifecycle_state

    async def vanish(self, memory_id, *a, **kw):
        crossed.append(memory_id)
        await self._session.execute(text("DELETE FROM beliefs WHERE id=:id"), dict(id=memory_id))
        return await original(self, memory_id, *a, **kw)

    with monkeypatch.context() as patch:
        patch.setattr(m.BeliefRepository, "transition_lifecycle_state", vanish)
        assert error_code(await resolve(state), "E_VERSION_CONFLICT", 409) == dict(id=ident(2), expected=1)
    assert crossed == [ident(2)] and await set_snapshot(state) == before


async def test_pending_publication_race_converges_not_blanket_storage_retry(resolve_http, monkeypatch):
    import asyncio

    state = resolve_http
    port = state.deps.ports["conflicts.resolve"]
    entered, release = asyncio.Event(), asyncio.Event()
    original = port._publish_outcome

    async def held(*a):
        entered.set()
        await release.wait()
        return await original(*a)

    monkeypatch.setattr(port, "_publish_outcome", held)
    task = asyncio.create_task(resolve(state))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        error_code(await resolve(state), "E_AUDIT_UNAVAILABLE", 503)
    finally:
        release.set()
        first = await asyncio.wait_for(task, 5)
    assert first.status == 200
    error_code(await resolve(state), "E_VERSION_CONFLICT", 409)


@pytest.mark.parametrize("committed", [False, True])
async def test_cancel_before_and_during_commit_recovers_by_original_key(resolve_http, monkeypatch, committed):
    import asyncio

    import aiosqlite

    from memory_server.application.remote_beliefs import BeliefsMutationHandler

    state, key = resolve_http, uuid4()
    entered, release = asyncio.Event(), asyncio.Event()
    once = True
    commit = aiosqlite.Connection.commit
    original = BeliefsMutationHandler.resolve_in_session

    async def held_commit(self):
        nonlocal once
        await commit(self)
        if once:
            once = False
            entered.set()
            await release.wait()

    async def held_precommit(self, session, *a, **kw):
        entered.set()
        try:
            await release.wait()
        finally:
            await release.wait()
        return await original(self, session, *a, **kw)

    before = await set_snapshot(state)
    with monkeypatch.context() as patch:
        if committed:
            patch.setattr(aiosqlite.Connection, "commit", held_commit)
        else:
            patch.setattr(BeliefsMutationHandler, "resolve_in_session", held_precommit)
        task = asyncio.create_task(resolve(state, key=key))
        try:
            await asyncio.wait_for(entered.wait(), 5)
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            assert state.deps.leases.snapshot()["alpha"].admissions > 0
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
    assert state.deps.leases.snapshot()["alpha"].admissions == 0
    # Outcome is determined by journal, never inferred from network cancellation.
    rows = await sql(
        state, "SELECT response_snapshot FROM http_request_journal WHERE idem_key=:key", dict(key=str(key))
    )
    if not rows:
        assert not committed and await set_snapshot(state) == before
    replay = await resolve(state, key=key)
    assert replay.status == 200, replay.body
    assert (
        len(await sql(state, "SELECT request_id FROM http_request_journal WHERE idem_key=:key", dict(key=str(key))))
        == 1
    )


async def test_resolution_two_parent_maximum_occurrences(resolve_http):
    from sqlalchemy import text

    state = resolve_http
    await orm_seed(
        state, [dict(id=ident(i + 100), proposition=f"shared child {i}", confidence=0.1) for i in range(512)]
    )
    async with state.deps.leases.acquire(context(state)) as scope:
        async with scope.application.memory.session_factory() as session, session.begin():
            for parent in (ident(1), ident(2)):
                await session.execute(
                    text(
                        "INSERT INTO claim_relations(source_id,target_id,relation_type,created_at) "
                        "SELECT id,:p,'derived_from',CURRENT_TIMESTAMP FROM beliefs WHERE id NOT IN (:a,:b)"
                    ),
                    dict(p=parent, a=ident(1), b=ident(2)),
                )
    response = await resolve(state, resolve_payload("discard_both"))
    assert response.status == 200, response.body
    assert await sql(state, "SELECT count(*) FROM lifecycle_events") == [(1026,)]
    assert await sql(state, "SELECT count(*) FROM outbox_entries") == [(514,)]
    assert await sql(
        state, "SELECT DISTINCT version,confidence FROM beliefs WHERE id NOT IN (:a,:b)", dict(a=ident(1), b=ident(2))
    ) == [(3, 0.1)]


@pytest.mark.parametrize("action", ["merge", "mark_contradicted", "discard_both"])
async def test_invalid_second_transition_has_no_first_effect(resolve_http, action):
    state = resolve_http
    await sql(state, "UPDATE beliefs SET lifecycle_state='discarded' WHERE id=:id", dict(id=ident(2)))
    before = await set_snapshot(state)
    error_code(await resolve(state, resolve_payload(action)), "E_VALIDATION", 422)
    assert await set_snapshot(state) == before


async def test_defer_different_keys_and_ttl_are_not_once_ever(resolve_http):
    import json

    state = resolve_http
    first = await resolve(state, resolve_payload("defer"))
    second = await resolve(state, resolve_payload("defer"))
    assert first.status == second.status == 200
    assert first.json["receipt_ids"] != second.json["receipt_ids"]
    assert await sql(state, "SELECT version FROM beliefs ORDER BY id") == [(1,), (1,)]
    assert await sql(state, "SELECT count(*) FROM lifecycle_events") == [(0,)]
    key = uuid4()
    third = await resolve(state, resolve_payload("defer"), key=key)
    await sql(
        state,
        "UPDATE http_request_journal SET expires_at='2020-01-01 00:00:00' WHERE idem_key=:key",
        dict(key=str(key)),
    )
    fourth = await resolve(state, resolve_payload("defer"), key=key)
    assert fourth.status == 200 and third.json["receipt_ids"] != fourth.json["receipt_ids"]
    for rid in fourth.json["receipt_ids"]:
        raw = json.loads((await sql(state, "SELECT history FROM receipts WHERE id=:id", dict(id=rid)))[0][0])
        assert raw[0]["reason"] == "  RESOLVE_REASON_CANARY  "


async def test_merge_readback_checks_database_not_identity_map(resolve_http):
    from sqlalchemy import event, text
    from storage.models.belief import EvidenceORM

    state = resolve_http
    await seed_evidence(state, 1)
    before, crossed = await set_snapshot(state), []

    def corrupt(mapper, connection, row):
        if row.belief_id not in (ident(1), ident(2)):
            crossed.append(row.id)
            connection.execute(text("UPDATE evidence SET note='CORRUPTED' WHERE id=:id"), dict(id=row.id))

    event.listen(EvidenceORM, "after_insert", corrupt)
    try:
        response = await resolve(state, resolve_payload("merge"))
    finally:
        event.remove(EvidenceORM, "after_insert", corrupt)
    error_code(response, "E_STORAGE", 503)
    assert len(crossed) == 1 and await set_snapshot(state) == before


async def test_merge_final_indexing_follows_effect_frontier(resolve_http, monkeypatch):
    from storage.outbox import OutboxRepository

    state, recorded = resolve_http, []
    original = OutboxRepository.add_entry

    async def tracked(self, kind, ident, *a, **kw):
        result = await original(self, kind, ident, *a, **kw)
        recorded.append((kind, ident))
        return result

    monkeypatch.setattr(OutboxRepository, "add_entry", tracked)
    response = await resolve(state, resolve_payload("merge"))
    assert response.status == 200, response.body
    assert recorded == [("belief", m["id"]) for m in response.json["memories"]]
