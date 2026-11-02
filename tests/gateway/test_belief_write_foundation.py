"""S15B0: supplied-session writes through real daemon resources and S12."""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime
from time import monotonic
from uuid import uuid4

import pytest
from sqlalchemy import text

from memory_server.gateway_contracts import dto as d
from tests.gateway.test_http_beliefs import belief_http as belief_http
from tests.gateway.test_http_beliefs import context, expect_error, ident, orm_seed, sql
from tests.gateway.test_slice_runtime import offline_embedder as offline_embedder


def foundation():
    from memory_server.application import remote_beliefs as m

    assert hasattr(m, "BeliefsMutationHandler"), "S15B0 supplied-session handler is missing"
    assert hasattr(m, "validate_belief_mutation_command"), "S15B0 strict validator is missing"
    return m


def command(**values):
    payload = d.BeliefRequest(
        **dict(
            proposition="Python is preferred for our production backend services",
            replace_belief_id=None,
            reason="  authenticated reason CANARY  ",
        )
        | values
    )
    return d.BeliefsSetCommand(operation="beliefs.set", profile_id="alpha", path_ids={}, payload=payload)


async def write(state, cmd=None, *, ctx=None, handler=None):
    m = foundation()
    cmd = cmd or command()
    ctx = ctx or replace(context(state), request_id=str(uuid4()), idempotency_key=uuid4(), deadline=monotonic() + 30)
    async with state.deps.leases.acquire(ctx) as scope:
        uow = m._BeliefsMutationUoW(
            scope.application.memory.session_factory,
            verify_principal=lambda p: state.deps.tokens.verify_principal(p, now=datetime.now(UTC)),
        )
        handler = handler or m.BeliefsMutationHandler(scope, ctx, ticket="test-intent")
        result = await uow.run(ctx, cmd, handler)
        return result, ctx


async def snapshot(state):
    return {
        table: await sql(state, f'SELECT * FROM "{table}" ORDER BY rowid')
        for table in (
            "beliefs",
            "facts",
            "evidence",
            "claim_relations",
            "lifecycle_states",
            "lifecycle_events",
            "receipts",
            "outbox_entries",
            "http_request_journal",
            "beliefs_fts",
        )
    }


async def test_foundation_present_without_future_surface():
    m = foundation()
    assert not hasattr(m, "RemoteBeliefsMutationPort")
    assert not hasattr(m.BeliefsMutationHandler, "resolve_in_session")


@pytest.mark.parametrize(
    "field,value",
    [
        ("confidence", True),
        ("confidence", float("nan")),
        ("reason", " \t"),
        ("proposition", "\u2003"),
        ("replace_belief_id", ""),
        ("expected_versions", {ident(1): True}),
        ("expected_versions", {ident(1).replace("-", ""): 1}),
    ],
)
async def test_strict_direct_command_revalidation(field, value):
    m = foundation()
    cmd = command()
    cmd.payload.__dict__[field] = value
    with expect_error("E_VALIDATION"):
        m.validate_belief_mutation_command(cmd)


async def test_defensive_copy_and_extra_fields():
    m = foundation()
    cmd = command(expected_versions={ident(1): 1})
    validated = m.validate_belief_mutation_command(cmd)
    cmd.payload.expected_versions[ident(1)] = 99
    assert validated.payload.expected_versions == {ident(1): 1}
    for forged in (command().model_copy(update={"path_ids": {"id": ident(1)}}), command()):
        if not forged.path_ids:
            forged.payload.__dict__["auto_resolve"] = False
        with expect_error("E_VALIDATION"):
            m.validate_belief_mutation_command(forged)


async def test_create_receipts_evidence_admission_and_masked_journal(belief_http):
    state = belief_http
    m = foundation()
    summary = d.EvidenceSummary(
        method="authority",
        sources=["opaque CANARY", "opaque CANARY"],
        session_id="session CANARY",
        confidence=0.0,
        source_date="date CANARY",
        derived_from=[],
        claim_type="authority",
    )
    empty = summary.model_copy(update=dict(method=None, sources=[], confidence=None))
    cmd = command(sources=[summary, empty], source="source CANARY", tags=["tag CANARY"])
    result, ctx = await write(state, cmd)
    memory, receipt = result.memories[0], result.receipts[0]
    assert memory.id == receipt.id and memory.revision == memory.source_version == 1
    assert memory.masked and memory.content is memory.payload is memory.creator is memory.source is None
    assert receipt.source is receipt.created_by is None and not receipt.history_complete
    assert all(h.actor is h.reason is h.evidence is None for h in receipt.history)
    decision = m.MemoryAdmissionGate().classify(cmd.payload.proposition, source_scope="remote", force=False)
    assert result.admission == [m._write_admission(decision)]
    assert len(result.outbox_ids) == 1 and result.reinforced_ids == []
    raw = await sql(state, "SELECT creator,source,source_ids,version FROM beliefs WHERE id=:id", {"id": memory.id})
    assert raw == [(ctx.principal.subject, "source CANARY", '["opaque CANARY"]', 1)]
    evidence = await sql(state, "SELECT source_type,source_id,weight,contributor FROM evidence ORDER BY rowid")
    assert evidence == [("authority", "opaque CANARY", 0.0, ctx.principal.subject)] * 2 + [
        ("observation", "", 0.5, ctx.principal.subject)
    ]
    history = json.loads((await sql(state, "SELECT history FROM receipts WHERE id=:id", {"id": receipt.id}))[0][0])
    assert history[0]["actor"] == ctx.principal.subject and history[0]["reason"] == cmd.payload.reason
    claims = history[0]["claims"]
    assert claims[0] == dict(summary.model_dump(), trusted=False, origin="remote_claim")
    assert claims[1] == dict(empty.model_dump(), trusted=False, origin="remote_claim")
    persisted = await sql(state, "SELECT id,record_id FROM outbox_entries")
    assert persisted == [(result.outbox_ids[0], memory.id)]
    stored = (await sql(state, "SELECT response_snapshot FROM http_request_journal"))[0][0]
    assert d.WriteResult.model_validate_json(stored) == result and "CANARY" not in stored


async def test_reinforcement_client_cas_union_and_replay(belief_http):
    state = belief_http
    first, _ = await write(state, command(confidence=0.8, tags=["old", "both"]))
    target = first.memories[0].id
    before = await snapshot(state)
    with expect_error("E_PRECONDITION_REQUIRED"):
        await write(state)
    with expect_error("E_VERSION_CONFLICT") as stale:
        await write(state, command(expected_versions={target: 2}))
    assert stale.value.details == {"id": target, "expected": 2, "actual": 1}
    assert await snapshot(state) == before
    cmd = command(confidence=0.2, tags=["both", "new"], source="current", expected_versions={target: 1})
    result, ctx = await write(state, cmd)
    assert result.reinforced_ids == [target] and result.admission == []
    assert result.memories[0].confidence == 0.5 and result.memories[0].revision == 2
    assert json.loads((await sql(state, "SELECT tags FROM beliefs"))[0][0]) == ["old", "both", "new"]
    events = await sql(state, "SELECT id,reason,triggered_by FROM lifecycle_events")
    assert len(events) == 1 and events[0][1:] == (cmd.payload.reason, ctx.principal.subject)
    assert result.receipts[0].history[-1].id == events[0][0]
    assert result.receipts[0].history[-1].revision == 2
    after = await snapshot(state)
    replay, _ = await write(state, cmd, ctx=replace(ctx, request_id="new-request"))
    assert replay == result and await snapshot(state) == after
    with expect_error("E_IDEMPOTENCY_CONFLICT"):
        await write(state, command(reason="changed", expected_versions={target: 1}), ctx=ctx)


@pytest.mark.parametrize("case", ["missing", "collision", "ambiguous", "extra", "missing_cas", "stale"])
async def test_q3_replace_precedence_and_no_effects(belief_http, case):
    state = belief_http
    foundation()
    await orm_seed(state, [dict(id=ident(1), proposition="old"), dict(id=ident(2), proposition="other")])
    values = dict(proposition="new", replace_belief_id=ident(1), expected_versions={ident(1): 1})
    code = "E_VALIDATION"
    if case == "missing":
        values.update(replace_belief_id=ident(9), proposition="other")
        code = "E_NOT_FOUND"
    elif case == "collision":
        values["proposition"] = "other"
    elif case == "ambiguous":
        await orm_seed(state, [dict(id=ident(3), proposition="OTHER")])
        values["proposition"] = " other "
    elif case == "extra":
        values["expected_versions"] = {ident(2): 1}
    elif case == "missing_cas":
        values["expected_versions"] = {}
        code = "E_PRECONDITION_REQUIRED"
    else:
        values["expected_versions"] = {ident(1): 2}
        code = "E_VERSION_CONFLICT"
    before = await snapshot(state)
    with expect_error(code):
        await write(state, command(**values))
    assert await snapshot(state) == before


async def test_replace_same_match_self_dependent_ledger_and_receipts(belief_http):
    state = belief_http
    root, _ = await write(state, command(confidence=0.9))
    old = root.memories[0].id
    dep, _ = await write(state, command(proposition="Our database engine is PostgreSQL for production", confidence=0.7))
    child = dep.memories[0].id
    for source in (old, child, ident(99)):
        await sql(
            state,
            "INSERT INTO claim_relations(source_id,target_id,relation_type) VALUES(:s,:t,'derived_from')",
            {"s": source, "t": old},
        )
    result, _ = await write(state, command(replace_belief_id=old, expected_versions={old: 1}))
    new, final_old = result.memories
    assert new.id != old and new.revision == 2 and final_old.id == old and final_old.revision == 3
    assert final_old.lifecycle_state == "superseded" and final_old.confidence == 0.72
    assert [r.id for r in result.receipts] == [new.id, old]
    assert len(result.outbox_ids) == 3 and len(set(result.outbox_ids)) == 3
    events = await sql(state, "SELECT id,memory_id,to_state FROM lifecycle_events ORDER BY rowid")
    assert [r[1] for r in events] == [old, *sorted([old, child])]
    histories = {r[0]: json.loads(r[1]) for r in await sql(state, "SELECT id,history FROM receipts")}
    assert [h["revision"] for h in histories[old][1:]] == [2, 3]
    assert [h["id"] for h in histories[old][1:]] == [e[0] for e in events if e[1] == old]
    assert histories[child][-1]["revision"] == 2
    assert histories[child][-1]["reason"] == "parent_invalidated"
    indexes = await sql(
        state,
        "SELECT record_id FROM outbox_entries WHERE id IN (:a,:b,:c)",
        dict(zip(("a", "b", "c"), result.outbox_ids, strict=True)),
    )
    assert {r[0] for r in indexes} == {new.id, old, child}


@pytest.mark.parametrize("count", [512, 513])
async def test_unicode_active_matching_boundary(belief_http, count):
    foundation()
    state = belief_http
    await orm_seed(state, [dict(id=ident(i), proposition=f"unique {i}") for i in range(1, count + 1)])
    await sql(state, "UPDATE beliefs SET proposition=:p WHERE id=:id", {"p": "\u2003İSTANBUL\u2003", "id": ident(1)})
    before = await snapshot(state)
    if count == 513:
        with expect_error("E_STORAGE") as error:
            await write(state, command(proposition="i\u0307stanbul", expected_versions={ident(1): 1}))
        assert error.value.details == {"limit": 512}
        assert await snapshot(state) == before
    else:
        result, _ = await write(state, command(proposition="i\u0307stanbul", expected_versions={ident(1): 1}))
        assert result.reinforced_ids == [ident(1)]


@pytest.mark.parametrize("count", [512, 513])
async def test_replace_propagation_bound_includes_missing_refs(belief_http, count):
    foundation()
    state = belief_http
    await orm_seed(state, [dict(id=ident(1), proposition="old")])
    async with state.deps.leases.acquire(context(state)) as scope:
        async with scope.application.memory.session_factory() as session, session.begin():
            await session.execute(
                text("INSERT INTO claim_relations(source_id,target_id,relation_type) VALUES(:s,:t,'derived_from')"),
                [{"s": ident(i + 100), "t": ident(1)} for i in range(count)],
            )
    before = await snapshot(state)
    cmd = command(replace_belief_id=ident(1), expected_versions={ident(1): 1})
    if count == 513:
        with expect_error("E_STORAGE"):
            await write(state, cmd)
        assert await snapshot(state) == before
    else:
        result, _ = await write(state, cmd)
        assert result.memories[1].revision == 2 and len(result.outbox_ids) == 2


@pytest.mark.parametrize("count", [1024, 1025])
async def test_evidence_expansion_boundary_no_truncation(belief_http, count):
    foundation()
    summary = d.EvidenceSummary(
        method=None,
        sources=["s"] * count,
        session_id=None,
        confidence=0.0,
        source_date=None,
        derived_from=[],
        claim_type=None,
    )
    before = await snapshot(belief_http)
    if count == 1025:
        with expect_error("E_STORAGE"):
            await write(belief_http, command(sources=[summary]))
        assert await snapshot(belief_http) == before
    else:
        await write(belief_http, command(sources=[summary]))
        assert (await sql(belief_http, "SELECT count(*) FROM evidence"))[0][0] == 1024
        with expect_error("E_STORAGE"):
            target = (await sql(belief_http, "SELECT id FROM beliefs"))[0][0]
            await write(belief_http, command(sources=[summary], expected_versions={target: 1}))
        assert (await sql(belief_http, "SELECT version FROM beliefs"))[0][0] == 1


@pytest.mark.parametrize("existing", [1023, 1024, 1025])
async def test_receipt_history_existing_plus_planned_bound(belief_http, existing):
    foundation()
    first, _ = await write(belief_http)
    target = first.memories[0].id
    await sql(belief_http, "UPDATE receipts SET history=:h", {"h": json.dumps([{}] * existing)})
    before = await snapshot(belief_http)
    cmd = command(expected_versions={target: 1})
    if existing >= 1024:
        with expect_error("E_STORAGE"):
            await write(belief_http, cmd)
        assert await snapshot(belief_http) == before
    else:
        await write(belief_http, cmd)
        assert len(json.loads((await sql(belief_http, "SELECT history FROM receipts"))[0][0])) == 1024


@pytest.mark.parametrize("boundary", ["evidence", "receipt", "outbox", "journal"])
async def test_real_boundary_failure_rolls_back_all_tables(belief_http, monkeypatch, boundary):
    foundation()
    from sqlalchemy.ext.asyncio import AsyncSession
    from storage.models.belief import EvidenceORM
    from storage.outbox import OutboxRepository
    from storage.repositories.http_request_repo import HttpRequestRepository
    from storage.repositories.receipt_repo import ReceiptRepository

    before = await snapshot(belief_http)
    if boundary == "evidence":
        original = AsyncSession.flush

        async def fail(self, *a, **kw):
            inserted = any(isinstance(row, EvidenceORM) for row in self.new)
            result = await original(self, *a, **kw)
            if inserted:
                raise RuntimeError("CANARY")
            return result

        monkeypatch.setattr(AsyncSession, "flush", fail)
    else:
        cls, method = {
            "receipt": (ReceiptRepository, "create"),
            "outbox": (OutboxRepository, "add_entry"),
            "journal": (HttpRequestRepository, "add"),
        }[boundary]
        original = getattr(cls, method)

        async def fail(self, *a, **kw):
            await original(self, *a, **kw)
            raise RuntimeError("CANARY")

        monkeypatch.setattr(cls, method, fail)
    summary = d.EvidenceSummary(
        method=None, sources=[], session_id=None, confidence=None, source_date=None, derived_from=[], claim_type=None
    )
    with expect_error("E_OUTBOX" if boundary == "outbox" else "E_INTERNAL"):
        await write(belief_http, command(sources=[summary]))
    monkeypatch.undo()
    assert await snapshot(belief_http) == before


async def test_writer_reservation_two_independent_connections(belief_http):
    m = foundation()
    state = belief_http
    await orm_seed(state, [dict(id=ident(1), proposition="existing")])
    ctx = replace(context(state), request_id=str(uuid4()), idempotency_key=uuid4())
    async with state.deps.leases.acquire(ctx) as scope:
        factory = scope.application.memory.session_factory
        entered, release = asyncio.Event(), asyncio.Event()
        handler = m.BeliefsMutationHandler(scope, ctx, ticket="test-intent")
        original = handler.set_in_session
        drivers = []

        async def held(session, *args, **kwargs):
            conn = await session.connection()
            drivers.append(await conn.run_sync(lambda c: id(c.connection.driver_connection)))
            entered.set()
            await release.wait()
            return await original(session, *args, **kwargs)

        handler.set_in_session = held
        task = asyncio.create_task(write(state, handler=handler, ctx=ctx))
        try:
            await asyncio.wait_for(entered.wait(), 5)
            async with factory() as competing:
                conn = await competing.connection()
                drivers.append(await conn.run_sync(lambda c: id(c.connection.driver_connection)))
                assert drivers[0] != drivers[1]
                await competing.execute(text("PRAGMA busy_timeout=1"))
                from sqlalchemy.exc import OperationalError

                with pytest.raises(OperationalError) as busy:
                    await competing.execute(text("UPDATE beliefs SET version=99 WHERE id=:id"), {"id": ident(1)})
                assert m._write_busy(busy.value)
        finally:
            release.set()
            await asyncio.wait_for(task, 10)
    assert (await sql(state, "SELECT version FROM beliefs WHERE id=:id", {"id": ident(1)})) == [(1,)]
