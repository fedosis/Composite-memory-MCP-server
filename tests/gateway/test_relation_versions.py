"""S16A foundation: real admitted leases, S12 transactions and SQLite readback."""

import ast
import asyncio
import json
import os
import random
import sqlite3
import subprocess
import sys
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import monotonic
from uuid import uuid4

import pytest
from sqlalchemy import event, text

from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import AuditOutcome
from tests.gateway.test_belief_write_foundation import pause_index_consumer as pause_index_consumer
from tests.gateway.test_http_beliefs import belief_http as belief_http
from tests.gateway.test_http_beliefs import context, expect_error, ident, orm_seed, sql
from tests.gateway.test_slice_runtime import offline_embedder as offline_embedder

pytestmark = pytest.mark.asyncio
ROOT = Path(__file__).resolve().parents[2]
TYPES = ("supports", "contradicts", "derives", "derived_from")
MAX = 9223372036854775807


def foundation():
    from memory_server.application import remote_relations

    return remote_relations


@pytest.fixture
async def relations(belief_http):
    state = belief_http
    now = datetime.now(UTC)
    token = state.deps.tokens._bootstrap_create(
        d.TokenCreate(
            subject="s16a",
            profiles=["alpha", "beta"],
            permissions=["relation.write", "relation.read", "graph.read", "memory.read", "belief.read"],
            expires_at=now + timedelta(hours=1),
        ),
        now=now,
    )
    state.bearer = f"cmms_{token.token.id}.{token.secret}"
    state.principal = await state.deps.tokens.authenticate(state.bearer, now=now)
    state.tickets = {}
    await orm_seed(state, [dict(id=ident(i), proposition=f"endpoint CANARY {i}", version=1) for i in (1, 2, 3)])
    return state


def command(*, deleted=False, revision=1, source=None, target=None, kind="supports", **values):
    source, target = source or ident(1), target or ident(2)
    payload = dict(expected_versions={k: 1 for k in (source, target)}, reason="  S16_REASON_CANARY  ")
    op = "relations.delete" if deleted else "relations.create"
    if deleted:
        payload.update(expected_version=revision, confirmation=True)
        from memory_server.application.graph_read_service import relation_id

        path = {"id": relation_id(source, target, kind)}
        cls = d.RelationDeleteRequest
    else:
        payload.update(source_id=source, target_id=target, relation_type=kind)
        path, cls = {}, d.RelationRequest
    return d.COMMAND_TYPES[op](operation=op, profile_id="alpha", path_ids=path, payload=cls(**(payload | values)))


def fresh(state):
    return replace(context(state), request_id="req_" + str(uuid4()), idempotency_key=uuid4(), deadline=monotonic() + 30)


async def write(state, cmd=None, *, ctx=None, handler=None, publish=True):
    from memory_server.application.mutation_uow import MUTATION_REPLY, MutationUoW

    cmd, ctx = cmd or command(), ctx or fresh(state)
    ticket = state.tickets.get(ctx.idempotency_key)
    if ticket is None:
        ticket = await state.deps.access.begin(ctx, cmd.operation)
        state.tickets[ctx.idempotency_key] = ticket
    async with state.deps.leases.acquire(ctx) as scope:
        uow = MutationUoW(
            scope.application.memory.session_factory,
            verify_principal=lambda p: state.deps.tokens.verify_principal(p, now=datetime.now(UTC)),
            ticket_provider=lambda: ticket.id,
        )
        handler = handler or foundation().RelationMutationHandler(ctx, audit_id=ticket.id)
        result = await uow.run(ctx, cmd, handler)
        assert handler.statements <= 64 and handler.instructions <= 2_001_000
        state.last_handler = handler
        reply = MUTATION_REPLY.get()
        if publish:
            assert await state.deps.access.publish_committed(
                ticket,
                AuditOutcome(
                    200 if cmd.operation == "relations.delete" else 201,
                    None,
                    tuple(result.receipt_ids),
                    0.0,
                ),
            )
            await uow.mark_published(reply.original_request_id)
            async with scope.application.memory.session_factory() as readback:
                assert (
                    await readback.execute(
                        text("SELECT pending_publication FROM http_request_journal WHERE request_id=:id"),
                        {"id": reply.original_request_id},
                    )
                ).scalar_one() == 0
        return result, ctx


async def snapshot(state, *, journal=True):
    tables = (
        "facts",
        "beliefs",
        "evidence",
        "claim_relations",
        "relation_revisions",
        "receipts",
        "outbox_entries",
        "lifecycle_states",
        "lifecycle_events",
    )
    if journal:
        tables += ("http_request_journal",)
    return {t: await sql(state, f'SELECT * FROM "{t}" ORDER BY rowid') for t in tables}


async def test_create_delete_recreate_monotone_tombstone_aba(relations):
    state = relations
    first, _ = await write(state)
    deleted, _ = await write(state, command(deleted=True))
    third, _ = await write(state)
    assert [r.relation.revision for r in (first, deleted, third)] == [1, 2, 3]
    assert first.relation.id == deleted.relation.id == third.relation.id
    assert deleted.relation.created_at == first.relation.created_at
    before = await snapshot(state)
    with expect_error("E_VERSION_CONFLICT") as err:
        await write(state, command(deleted=True))
    assert err.value.details == dict(id=third.relation.id, expected=1, actual=3)
    assert await snapshot(state) == before
    fourth, _ = await write(state, command(deleted=True, revision=3))
    assert fourth.relation.revision == 4 and fourth.deleted
    assert await sql(state, "SELECT revision,deleted FROM relation_revisions ORDER BY revision") == [
        (1, 0),
        (2, 1),
        (3, 0),
        (4, 1),
    ]
    await sql(
        state,
        "INSERT INTO claim_relations VALUES(:s,:t,'derives','2026-01-01 00:00:00.000000')",
        dict(s=ident(1), t=ident(2)),
    )
    old, _ = await write(state, command(deleted=True, kind="derives"))
    assert old.relation.revision == 2
    assert await sql(state, "SELECT revision FROM relation_revisions WHERE relation_type='derives'") == [(2,)]


@pytest.mark.parametrize(
    "case,code",
    [
        ("source", "E_VERSION_CONFLICT"),
        ("target", "E_VERSION_CONFLICT"),
        ("relation", "E_VERSION_CONFLICT"),
        ("foreign", "E_NOT_FOUND"),
        ("ambiguous", "E_NOT_FOUND"),
        ("missing", "E_PRECONDITION_REQUIRED"),
        ("bool", "E_VALIDATION"),
        ("extra", "E_VALIDATION"),
        ("blank", "E_VALIDATION"),
        ("confirmation", "E_VALIDATION"),
    ],
)
async def test_relation_and_both_endpoint_cas_precede_all_effects(relations, case, code):
    state = relations
    await write(state)
    cmd = command(deleted=True)
    if case in ("source", "target"):
        cmd.payload.expected_versions[ident(1 if case == "source" else 2)] = 9
    elif case == "relation":
        cmd.payload.__dict__["expected_version"] = 9
    elif case == "foreign":
        await sql(state, "DELETE FROM beliefs WHERE id=:id", {"id": ident(2)})
        cmd.payload.expected_versions[ident(1)] = 9
    elif case == "ambiguous":
        from storage.repositories.fact_repo import FactRepository

        from memory_server.models.fact import Fact

        async with state.deps.leases.acquire(context(state)) as scope:
            async with scope.application.memory.session_factory() as session, session.begin():
                await FactRepository(session).create(Fact(id=ident(2), subject="s", predicate="p", object="o"))
        cmd.payload.expected_versions[ident(1)] = 9
    elif case == "missing":
        del cmd.payload.expected_versions[ident(2)]
    elif case == "bool":
        cmd.payload.expected_versions[ident(1)] = True
    elif case == "extra":
        cmd.payload.expected_versions[ident(3)] = 1
    elif case == "blank":
        cmd.payload.__dict__["reason"] = " \t"
    else:
        cmd.payload.__dict__["confirmation"] = 1
    before = await snapshot(state)
    if case in ("missing", "bool", "extra", "blank", "confirmation"):
        with expect_error(code):
            foundation().validate_relation_command(fresh(state), cmd)
    else:
        with expect_error(code) as err:
            await write(state, cmd)
        if code == "E_NOT_FOUND":
            assert err.value.details == {}
        else:
            expected_id = ident(1 if case == "source" else 2) if case != "relation" else cmd.path_ids["id"]
            assert err.value.details == dict(id=expected_id, expected=9, actual=1)
    assert await snapshot(state) == before


async def test_duplicate_live_create_and_missing_delete_policy(relations):
    state = relations
    original, ctx = await write(state)
    before = await snapshot(state)
    with expect_error("E_VERSION_CONFLICT"):
        await write(state)
    assert await snapshot(state) == before
    deletion, dctx = await write(state, command(deleted=True))
    with expect_error("E_NOT_FOUND"):
        await write(state, command(deleted=True, revision=2))
    await write(state)
    after = await snapshot(state)
    assert (await write(state, ctx=ctx))[0] == original
    assert (await write(state, command(deleted=True), ctx=dctx))[0] == deletion
    assert await snapshot(state) == after
    with expect_error("E_IDEMPOTENCY_CONFLICT"):
        await write(state, command(reason="changed"), ctx=ctx)
    assert await snapshot(state) == after


async def test_every_relation_action_receipt_and_outbox_id_is_persisted(relations):
    state = relations
    result, ctx = await write(state, publish=False)
    event_id, action_id, actor, request_id = (
        await sql(state, "SELECT event_id,action_receipt_id,actor,request_id FROM relation_revisions")
    )[0]
    assert result.receipt_ids == [action_id] and actor == ctx.principal.subject and request_id == ctx.request_id
    assert len({event_id, action_id, *result.outbox_ids, result.audit_id, ctx.request_id}) == 5
    receipt = (await sql(state, "SELECT source,created_by,history FROM receipts WHERE id=:id", {"id": action_id}))[0]
    history = json.loads(receipt[2])
    assert receipt[:2] == ("gateway", ctx.principal.subject)
    assert len(history) == 1 and history[0]["reason"] == command().payload.reason
    assert history[0]["before"] is None and history[0]["after"] == 1
    intent = (await sql(state, "SELECT id,record_type,record_id,operation,payload_json,status FROM outbox_entries"))[0]
    assert intent[:4] == (result.outbox_ids[0], "belief", ident(1), "relation_sync")
    assert intent[5] == "pending"
    assert json.loads(intent[4]) == dict(
        v=1, source_id=ident(1), target_id=ident(2), relation_type="supports", revision=1, event_id=event_id
    )
    journal = (
        await sql(state, "SELECT response_snapshot,audit_ticket_id,pending_publication FROM http_request_journal")
    )[0]
    assert d.RelationResult.model_validate_json(journal[0]) == result
    assert journal[1:] == (result.audit_id, 1)
    assert "CANARY" not in result.model_dump_json() and "CANARY" not in journal[0] and "CANARY" not in intent[4]


@pytest.mark.parametrize(
    "boundary", ["canonical", "ledger", "receipt", "outbox", "result", "journal", "commit", "early_commit"]
)
@pytest.mark.parametrize("deleted", [False, True])
async def test_relation_precommit_fault_matrix_rolls_back_all_tables(relations, monkeypatch, boundary, deleted):
    from sqlalchemy.ext.asyncio import AsyncSession
    from storage.outbox import OutboxRepository
    from storage.repositories.http_request_repo import HttpRequestRepository
    from storage.repositories.receipt_repo import ReceiptRepository
    from storage.repositories.relation_repo import RelationRepository
    from storage.repositories.relation_revision_repo import RelationRevisionRepository

    if deleted:
        await write(relations)
    before = await snapshot(relations)
    handler = None
    if boundary == "early_commit":
        ctx = fresh(relations)
        handler = foundation().RelationMutationHandler(ctx, audit_id="test")
        original = handler._apply

        async def early(session, cmd):
            value = await original(session, cmd)
            await session.commit()
            return value

        handler._apply = early
    elif boundary in ("result", "commit"):
        if boundary == "result":
            monkeypatch.setattr(foundation(), "map_relation", lambda *a: (_ for _ in ()).throw(RuntimeError("CANARY")))
        else:
            original_commit = AsyncSession.commit

            def fail_commit(session):
                raise RuntimeError("commit CANARY")

            async with relations.deps.leases.acquire(context(relations)) as scope:
                engine = scope.application.memory.session_factory.kw["bind"].sync_engine
            event.listen(engine, "commit", fail_commit)
    else:
        cls, method = {
            "canonical": (RelationRepository, "delete_versioned" if deleted else "create_versioned"),
            "ledger": (RelationRevisionRepository, "append"),
            "receipt": (ReceiptRepository, "create"),
            "outbox": (OutboxRepository, "add_entry"),
            "journal": (HttpRequestRepository, "add"),
        }[boundary]
        original = getattr(cls, method)

        async def fail(self, *args, **kw):
            await original(self, *args, **kw)
            raise RuntimeError("fault CANARY")

        monkeypatch.setattr(cls, method, fail)
    try:
        with expect_error("E_INTERNAL"):
            await write(relations, command(deleted=deleted), handler=handler, ctx=ctx if handler else None)
    finally:
        if boundary == "commit":
            event.remove(engine, "commit", fail_commit)
            assert AsyncSession.commit is original_commit
    assert await snapshot(relations) == before


async def test_endpoint_preflight_holds_real_sqlite_writer_reservation(relations):
    from sqlalchemy.exc import OperationalError

    state, ctx = relations, fresh(relations)
    handler = foundation().RelationMutationHandler(ctx, audit_id="reservation")
    entered, release = asyncio.Event(), asyncio.Event()
    original = handler._apply
    trace = []

    async def held(session, cmd):
        assert (
            await session.execute(text("SELECT version FROM beliefs WHERE id=:id"), {"id": ident(1)})
        ).scalar_one() == 1
        entered.set()
        await release.wait()
        return await original(session, cmd)

    handler._apply = held
    async with state.deps.leases.acquire(ctx) as scope:
        factory = scope.application.memory.session_factory
        engine = factory.kw["bind"].sync_engine

        def record(conn, cursor, statement, *args):
            trace.append(statement)

        event.listen(engine, "before_cursor_execute", record)
        task = asyncio.create_task(write(state, ctx=ctx, handler=handler))
        try:
            await asyncio.wait_for(entered.wait(), 5)
            async with factory() as competing:
                await competing.execute(text("PRAGMA busy_timeout=1"))
                with pytest.raises(OperationalError) as err:
                    await competing.execute(text("UPDATE beliefs SET version=99 WHERE id=:id"), {"id": ident(1)})
                assert err.value.orig.sqlite_errorcode & 255 in (5, 6)
        finally:
            release.set()
            await asyncio.wait_for(task, 10)
            event.remove(engine, "before_cursor_execute", record)
    purge = next(i for i, s in enumerate(trace) if s.lstrip().upper().startswith("DELETE FROM HTTP_REQUEST_JOURNAL"))
    lookup = next(i for i, s in enumerate(trace) if s.startswith("SELECT version FROM beliefs"))
    assert purge < lookup and await sql(state, "SELECT DISTINCT version FROM beliefs") == [(1,)]


@pytest.mark.parametrize("kind", TYPES)
async def test_native_repository_recreation_tracks_tombstone_without_output_drift(relations, kind):
    from storage.repositories.relation_repo import RelationRepository
    from storage.repositories.relation_revision_repo import RelationEventContext, RelationRevisionRepository

    state = relations
    key = ident(1), ident(2), kind
    async with state.deps.leases.acquire(context(state)) as scope:
        factory = scope.application.memory.session_factory
        async with factory() as session, session.begin():
            repo = RelationRepository(session)
            row = await repo.create(*key)
            created = row.created_at
            assert await repo.create(*key) is row
            assert row.source_id == key[0] and row.target_id == key[1] and row.relation_type == kind
        async with factory() as session, session.begin():
            revisions = RelationRevisionRepository(session)
            before = await revisions.state(key, created)
            await RelationRepository(session).delete_versioned(
                key, before=before, expected_revision=1, event=RelationEventContext(str(uuid4()), datetime.now(UTC))
            )
        async with factory() as session, session.begin():
            row = await RelationRepository(session).create(*key)
            await RelationRepository(session).create(*key)
            assert (await RelationRevisionRepository(session).latest(key)).revision == 3
    rows = await sql(
        state, ("SELECT revision,deleted,request_id,actor,action_receipt_id FROM relation_revisions ORDER BY revision")
    )
    assert rows == [(1, 0, None, None, None), (2, 1, None, None, None), (3, 0, None, None, None)]
    assert await sql(state, "SELECT * FROM outbox_entries") == []
    if kind != "derived_from":
        before = await snapshot(state)
        with expect_error("E_VERSION_CONFLICT"):
            await write(state, command(deleted=True, kind=kind))
        assert await snapshot(state) == before


async def test_relation_only_no_propagation_or_evidence_effects(relations):
    state = relations
    before = await snapshot(state)
    for source, target, kind in [
        (ident(1), ident(1), "supports"),
        (ident(2), ident(1), "supports"),
        (ident(1), ident(2), "contradicts"),
        (ident(1), ident(2), "supports"),
    ]:
        await write(state, command(source=source, target=target, kind=kind))
    await write(state, command(deleted=True))
    assert await sql(
        state,
        ("SELECT source_id,target_id,relation_type FROM claim_relations ORDER BY source_id,target_id,relation_type"),
    ) == [(ident(1), ident(1), "supports"), (ident(1), ident(2), "contradicts"), (ident(2), ident(1), "supports")]
    after = await snapshot(state)
    for table in ("facts", "beliefs", "evidence", "lifecycle_states", "lifecycle_events"):
        assert before[table] == after[table]
    cmd = command(deleted=True, kind="derived_from")
    with expect_error("E_VALIDATION"):
        foundation().validate_relation_command(fresh(state), cmd)


@pytest.mark.parametrize("mode", ["same", "create", "delete"])
async def test_same_key_concurrent_x10_and_restart_replay_once(relations, mode):
    state = relations
    if mode == "delete":
        await write(state)
    cmd = command(deleted=mode == "delete")
    shared = fresh(state)
    start = asyncio.Event()

    async def contender():
        await start.wait()
        ctx = shared if mode == "same" else fresh(state)
        for attempt in range(40):
            try:
                return await write(state, cmd, ctx=ctx)
            except Exception as exc:
                if getattr(exc, "code", None) not in ("E_REQUEST_IN_PROGRESS", "E_AUDIT_UNAVAILABLE"):
                    return exc
                await asyncio.sleep(0.02)
        pytest.fail("contention did not settle to a semantic result")

    # Create the durable shared ticket before concurrent uses of its key.
    if mode == "same":
        state.tickets[shared.idempotency_key] = await state.deps.access.begin(shared, cmd.operation)
    tasks = [asyncio.create_task(contender()) for _ in range(10 if mode == "same" else 2)]
    start.set()
    outcomes = await asyncio.gather(*tasks)
    wins = [r for r in outcomes if isinstance(r, tuple)]
    errors = [r for r in outcomes if not isinstance(r, tuple)]
    if mode == "same":
        assert len(wins) == 10 and all(r[0] == wins[0][0] for r in wins), outcomes
    else:
        assert len(wins) == 1, outcomes
        assert [e.code for e in errors] == ["E_NOT_FOUND" if mode == "delete" else "E_VERSION_CONFLICT"]
    count = 2 if mode == "delete" else 1
    assert await sql(state, "SELECT count(*) FROM relation_revisions") == [(count,)]
    assert await sql(state, "SELECT count(*) FROM receipts") == [(count,)]
    assert await sql(state, "SELECT count(*) FROM outbox_entries") == [(count,)]
    result, ctx = wins[0]
    after = await snapshot(state)
    report = await state.deps.leases.stop_profile("alpha")
    assert report is not None
    assert (await write(state, cmd, ctx=replace(ctx, request_id="req_" + str(uuid4()))))[0] == result
    assert await snapshot(state) == after


async def test_different_key_delete_frontier_has_one_winner(relations):
    await test_same_key_concurrent_x10_and_restart_replay_once(relations, "delete")


async def test_concurrent_create_unique_tuple_no_dual_receipt(relations):
    await test_same_key_concurrent_x10_and_restart_replay_once(relations, "create")


async def test_relation_precommit_cancel_rolls_back_and_resets_budget(relations):
    state, ctx = relations, fresh(relations)
    handler = foundation().RelationMutationHandler(ctx, audit_id="cancel")
    original = handler._apply
    entered = asyncio.Event()

    async def held(session, cmd):
        await original(session, cmd)
        entered.set()
        await asyncio.Event().wait()

    handler._apply = held
    before = await snapshot(state)
    task = asyncio.create_task(write(state, ctx=ctx, handler=handler))
    await asyncio.wait_for(entered.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 10)
    assert await snapshot(state) == before
    await orm_seed(state, [dict(id=ident(1), proposition="beta")], profile="beta")
    assert await sql(state, "SELECT count(*) FROM beliefs", profile="beta") == [(1,)]
    assert (await write(state))[0].relation.revision == 1


@pytest.mark.parametrize(
    "mode", ["revision_live", "revision_dead", "incidence", "pair", "vm", "sql", "deadline", "result"]
)
async def test_revision_and_result_overflow_are_typed_atomic(relations, monkeypatch, mode):
    from storage.repositories.relation_revision_repo import RelationRevisionRepository

    state = relations
    if mode.startswith("revision"):
        await write(state)
        if mode == "revision_dead":
            await write(state, command(deleted=True))
        await sql(
            state,
            (
                "UPDATE relation_revisions SET revision=:max WHERE revision=(SELECT max(revi"
                "sion) FROM relation_revisions)"
            ),
            {"max": MAX},
        )
    elif mode == "incidence":
        await sql(
            state,
            (
                "INSERT INTO claim_relations SELECT :s, printf('other-%d',value),'derived_fr"
                "om','2026-01-01 00:00:00.000000' FROM json_each(:xs)"
            ),
            {"s": ident(1), "xs": json.dumps(list(range(513)))},
        )
    elif mode == "pair":
        await sql(
            state,
            (
                "INSERT INTO claim_relations SELECT :s,:t, printf('unknown-%d',value),'2026-"
                "01-01 00:00:00.000000' FROM json_each(:xs)"
            ),
            {"s": ident(1), "t": ident(2), "xs": json.dumps(list(range(17)))},
        )
    ctx = fresh(state)
    handler = foundation().RelationMutationHandler(ctx, audit_id="overflow")
    if mode in ("vm", "sql", "deadline"):

        async def heavy(session, cmd):
            if mode == "vm":
                await session.execute(
                    text(
                        (
                            "WITH RECURSIVE x(n) AS (VALUES(0) UNION ALL SELECT n+1 FROM x WHERE n<10000"
                            "000) SELECT sum(n) FROM x"
                        )
                    )
                )
            elif mode == "sql":
                for _ in range(65):
                    await session.execute(text("SELECT 1"))
            else:
                await asyncio.sleep(0.5)
            pytest.fail("budget omitted")

        handler._apply = heavy
        if mode == "deadline":
            ctx = replace(ctx, deadline=monotonic() + 0.2)
            handler.ctx = ctx
    elif mode == "result":
        original = foundation().map_relation

        def huge(*args):
            result = original(*args)
            return result.model_copy(update={"id": "x" * 65537})

        monkeypatch.setattr(foundation(), "map_relation", huge)
    before = await snapshot(state)
    with expect_error("E_TIMEOUT" if mode == "deadline" else "E_STORAGE"):
        await write(state, command(deleted=mode == "revision_live", revision=MAX), ctx=ctx, handler=handler)
    assert await snapshot(state) == before
    if mode == "vm":
        assert handler.instructions == 2_000_000
    if mode == "sql":
        assert handler.statements == 65
    # Strict frontier type and byte thresholds are exercised independently too.
    from storage.repositories.relation_revision_repo import RelationRevisionStorageError, positive

    for invalid in (True, None, "1", 0, -1, MAX + 1):
        with pytest.raises(RelationRevisionStorageError):
            positive(invalid)
    assert positive(MAX) == MAX
    foundation()._bounded("x" * 65534)
    with expect_error("E_STORAGE"):
        foundation()._bounded("x" * 65535)
    assert RelationRevisionRepository is not None


async def test_relation_state_machine_property(relations):
    state, rng = relations, random.Random(1601)
    live, revision = False, 0
    replays = []
    for _ in range(32):
        action = rng.choice(("create", "delete", "replay"))
        if action == "replay" and replays:
            cmd, result, ctx = rng.choice(replays)
            before = await snapshot(state)
            assert (await write(state, cmd, ctx=ctx))[0] == result
            assert await snapshot(state) == before
            continue
        delete = action == "delete"
        cmd = command(deleted=delete, revision=max(1, revision))
        if delete != live:
            before = await snapshot(state)
            with expect_error("E_NOT_FOUND" if delete else "E_VERSION_CONFLICT"):
                await write(state, cmd)
            assert await snapshot(state) == before
        else:
            result, ctx = await write(state, cmd)
            revision += 1
            live = not delete
            assert result.relation.revision == revision
            replays.append((cmd, result, ctx))
        assert await sql(state, "SELECT count(*) FROM claim_relations") == [(int(live),)]
        assert await sql(state, "SELECT count(*) FROM relation_revisions") == [(revision,)]


async def test_relation_id_property_is_directed_canonical_and_bounded():
    from memory_server.application.graph_read_service import decode_relation_id, relation_id

    rng = random.Random(1602)
    for _ in range(40):
        source, target = "".join(rng.choices("αЖ中abc", k=20)), "".join(rng.choices("xyz", k=15))
        key = source, target, rng.choice(TYPES)
        rid = relation_id(*key)
        assert decode_relation_id(rid) == key and rid != relation_id(target, source, key[2])
        for bad in (rid + "=", rid + "x", "proj_" + rid[4:], "ent_" + rid[4:], "conf_" + rid[4:]):
            with expect_error("E_VALIDATION"):
                decode_relation_id(bad)
    with expect_error("E_VALIDATION"):
        relation_id("x" * 1025, "y", "supports")


def catalog(path):
    with sqlite3.connect(path) as conn:
        return conn.execute(
            ("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name")
        ).fetchall()


def historical(path):
    from alembic import command as alembic_command
    from alembic.config import Config

    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(ROOT / "alembic"))
    cfg.set_main_option(
        "version_locations", os.pathsep.join(str(ROOT / p) for p in ("alembic/versions", "migrations/versions"))
    )
    cfg.set_main_option("sqlalchemy.url", f"sqlite:///{path}")
    alembic_command.upgrade(cfg, "0007")
    base = Path(os.environ["S16A_BASE_ROOT"])
    code = """import asyncio,sys
from pathlib import Path
from memory_server.providers.sqlite_provider import SQLiteProvider
assert Path(sys.modules[SQLiteProvider.__module__].__file__).resolve().is_relative_to(Path(sys.argv[2]))
async def main():
 p=SQLiteProvider('sqlite+aiosqlite:///'+sys.argv[1])
 try: await p.initialize()
 finally: await p.close()
asyncio.run(main())"""
    old = subprocess.run(
        [sys.executable, "-B", "-c", code, str(path), str(base)],
        cwd=base,
        env=dict(os.environ, PYTHONPATH=f"{base}/src:{base}"),
        capture_output=True,
        text=True,
    )
    assert old.returncode == 0, old.stdout + old.stderr
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session
    from storage.models.belief import BeliefORM
    from storage.models.receipt import MemoryReceiptORM
    from storage.outbox import OutboxEntryORM

    from memory_server.models.receipt import MemoryReceipt

    engine = create_engine(f"sqlite:///{path}")
    try:
        with Session(engine) as session, session.begin():
            session.add_all([BeliefORM(id=ident(i), proposition=f"historical endpoint {i}") for i in (1, 2, 3, 4, 10)])
            session.add(
                MemoryReceiptORM.from_pydantic(
                    MemoryReceipt(
                        id=ident(1),
                        memory_type="belief",
                        source="historical",
                        created_by="fixture",
                        timestamp=datetime(2020, 1, 1, tzinfo=UTC),
                    )
                )
            )
            session.add(
                OutboxEntryORM(
                    id=str(uuid4()),
                    record_type="belief",
                    record_id=ident(1),
                    operation="index_belief",
                    payload_json="{}",
                    status="pending",
                )
            )
    finally:
        engine.dispose()
    with sqlite3.connect(path) as conn:
        for i, kind in enumerate(TYPES):
            conn.execute(
                "INSERT INTO claim_relations VALUES(?,?,?,?)",
                (ident(i + 1), ident(10), kind, "2020-01-02 03:04:05.123456"),
            )
        assert conn.execute("SELECT version_num FROM alembic_version").fetchall() == [("0007",)]
        assert conn.execute("SELECT count(*) FROM sqlite_master WHERE name='relation_revisions'").fetchone() == (0,)


async def initialize(path):
    from memory_server.providers.sqlite_provider import SQLiteProvider

    provider = SQLiteProvider(f"sqlite+aiosqlite:///{path}")
    try:
        await provider.initialize()
    finally:
        await provider.close()


async def test_revision_schema_forward_reopen_and_occupied_definition(tmp_path):
    path = tmp_path / "old0007.db"
    historical(path)
    old_schema = catalog(path)
    with sqlite3.connect(path) as conn:
        old_data = {
            row[1]: sorted(conn.execute('SELECT * FROM "' + row[1].replace('"', '""') + '"').fetchall(), key=repr)
            for row in old_schema
            if row[0] == "table"
        }
        old_rows = conn.execute("SELECT * FROM claim_relations").fetchall()
    await initialize(path)
    after = catalog(path)
    assert all(row in after for row in old_schema)
    with sqlite3.connect(path) as conn:
        for table, rows in old_data.items():
            assert sorted(conn.execute('SELECT * FROM "' + table.replace('"', '""') + '"').fetchall(), key=repr) == rows
        assert conn.execute("SELECT * FROM claim_relations").fetchall() == old_rows
        assert conn.execute("SELECT * FROM relation_revisions").fetchall() == []
        assert conn.execute("SELECT version_num FROM alembic_version").fetchall() == [("0007",)]
    await initialize(path)
    assert catalog(path) == after
    print("REHEARSAL old0007/reopen", path, "old_rows", len(old_rows))


@pytest.mark.parametrize("missing_other", [False, True])
@pytest.mark.parametrize(
    "variant", ["column", "type", "nullable", "pk", "check", "unique", "view", "trigger", "extra_index"]
)
async def test_occupied_revision_schema_fails_before_any_ddl(tmp_path, variant, missing_other):
    from sqlalchemy.engine import Engine
    from storage.repositories.relation_revision_repo import RelationRevisionStorageError

    path = tmp_path / "occupied.db"
    await initialize(path)
    with sqlite3.connect(path) as conn:
        ddl = conn.execute("SELECT sql FROM sqlite_master WHERE name='relation_revisions'").fetchone()[0]
        conn.execute("DROP TABLE relation_revisions")
        if variant == "view":
            conn.execute("CREATE VIEW relation_revisions AS SELECT 1 AS revision")
        else:
            ddl = {
                "column": ddl.replace("actor VARCHAR,", "actor VARCHAR, alien INTEGER,"),
                "type": ddl.replace("revision INTEGER", "revision VARCHAR"),
                "nullable": ddl.replace("deleted INTEGER NOT NULL", "deleted INTEGER"),
                "pk": ddl.replace(
                    "source_id, target_id, relation_type, revision", "target_id, source_id, relation_type, revision"
                ),
                "check": ddl.replace(
                    "typeof(revision) = 'integer' AND revision BETWEEN 1 AND 9223372036854775807", "revision >= 0"
                ),
                "unique": ddl.replace(
                    "CONSTRAINT s16_event_unique UNIQUE (event_id)",
                    "CONSTRAINT s16_event_unique CHECK (event_id IS NOT NULL)",
                ),
            }.get(variant, ddl)
            conn.execute(ddl)
            if variant == "trigger":
                conn.execute(
                    (
                        "CREATE TRIGGER hostile AFTER INSERT ON relation_revisions BEGIN DELETE FROM"
                        " relation_revisions; END"
                    )
                )
            if variant == "extra_index":
                conn.execute("CREATE INDEX hostile ON relation_revisions(actor)")
        if missing_other:
            conn.execute("DROP TABLE skills")
    before, statements = catalog(path), []

    def trace(conn, cursor, statement, *args):
        statements.append(statement)

    event.listen(Engine, "before_cursor_execute", trace)
    try:
        with pytest.raises(RelationRevisionStorageError) as err:
            await initialize(path)
        assert str(err.value) == "Relation revision storage invariant failed"
    finally:
        event.remove(Engine, "before_cursor_execute", trace)
    assert not [
        s
        for s in statements
        if s.lstrip().split()[0].upper() in {"CREATE", "ALTER", "DROP", "INSERT", "UPDATE", "DELETE", "REPLACE"}
    ], statements
    assert catalog(path) == before
    print("REHEARSAL occupied", variant, "other_missing", missing_other, "statements", len(statements), "zero_ddl_dml")


async def test_schema_admission_head_remains_0007():
    from memory_server.profile_migration import ACCEPTED_SQLITE_SCHEMA_REVISIONS
    from tests.test_profile_migration import _s203_tree_head_revisions

    assert ACCEPTED_SQLITE_SCHEMA_REVISIONS == _s203_tree_head_revisions() == frozenset({"0007"})
    tree = ast.parse((ROOT / "src/memory_server/profile_migration.py").read_text())
    assert any(isinstance(n, ast.Name) and n.id == "ACCEPTED_SQLITE_SCHEMA_REVISIONS" for n in ast.walk(tree))


@pytest.mark.parametrize("populated", [False, True])
async def test_schema_rollback_preserves_populated_tombstones(tmp_path, populated):
    base = Path(os.environ["S16A_BASE_ROOT"])
    assert (
        subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=base, text=True).strip()
        == "48611fb11b5386f85cd10e3ec6aa333b99cd3ae9"
    )
    path = tmp_path / "rollback.db"
    historical(path)
    await initialize(path)
    if populated:
        with sqlite3.connect(path) as conn:
            for r in (2, 3, 4):
                conn.execute(
                    "INSERT INTO relation_revisions VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        ident(1),
                        ident(10),
                        "supports",
                        r,
                        r % 2 == 0,
                        "2020-01-02 03:04:05.123456",
                        "2026-01-01 00:00:00.000000",
                        str(uuid4()),
                        None,
                        None,
                        None,
                    ),
                )
            conn.execute("DELETE FROM claim_relations WHERE relation_type='supports'")
    with sqlite3.connect(path) as conn:
        before = conn.execute("SELECT * FROM relation_revisions ORDER BY revision").fetchall()
    schema = catalog(path)
    code = """import asyncio,sys,json
from pathlib import Path
from memory_server.providers import sqlite_provider as m
assert Path(m.__file__).resolve().is_relative_to(Path(sys.argv[2]))
async def main():
 p=m.SQLiteProvider('sqlite+aiosqlite:///'+sys.argv[1])
 try:
  await p.initialize()
  async with p._engine.connect() as c:
   print('BASE_READ', (await c.exec_driver_sql('SELECT count(*) FROM claim_relations')).scalar_one(), m.__file__)
 finally: await p.close()
asyncio.run(main())"""
    env = dict(os.environ, PYTHONPATH=f"{base}/src:{base}")
    result = subprocess.run(
        [sys.executable, "-B", "-c", code, str(path), str(base)], cwd=base, env=env, text=True, capture_output=True
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "BASE_READ" in result.stdout
    await initialize(path)
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT * FROM relation_revisions ORDER BY revision").fetchall() == before
    assert catalog(path) == schema
    print(
        "REHEARSAL rollback",
        populated,
        result.stdout.strip(),
        "writers_stopped worker_not_started preserved",
        len(before),
    )


async def test_schema_startup_crash_reopen_preserves_old_rows(tmp_path):
    path = tmp_path / "crash.db"
    historical(path)
    before = catalog(path)
    code = """import asyncio,os,sys
from sqlalchemy import event
from sqlalchemy.engine import Engine
from memory_server.providers.sqlite_provider import SQLiteProvider
def crash(conn,cursor,statement,*args):
 if 'CREATE TABLE relation_revisions' in statement: os._exit(71)
event.listen(Engine,'before_cursor_execute',crash)
asyncio.run(SQLiteProvider('sqlite+aiosqlite:///'+sys.argv[1]).initialize())"""
    result = subprocess.run(
        [sys.executable, "-B", "-c", code, str(path)], cwd=ROOT, env=os.environ.copy(), capture_output=True, text=True
    )
    assert result.returncode == 71, result.stdout + result.stderr
    assert all(row in catalog(path) for row in before)
    await initialize(path)
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT count(*) FROM claim_relations").fetchone() == (4,)
        assert conn.execute("SELECT count(*) FROM relation_revisions").fetchone() == (0,)
    print("REHEARSAL crash boundary exit71 reopen old_rows4 ledger0", path)


async def test_old_rows_have_virtual_revision_one_without_read_writes(relations):
    from memory_server.application.graph_read_service import relation_id
    from tests.gateway.test_http_shell import request

    state = relations
    for kind in TYPES:
        await sql(
            state,
            "INSERT INTO claim_relations VALUES(:s,:t,:r,:d)",
            dict(s=ident(1), t=ident(2), r=kind, d="2020-01-02 03:04:05.123456"),
        )
    before = await snapshot(state)
    for kind in TYPES:
        response = await request(
            state.app, "/api/v1/profiles/alpha/relations/" + relation_id(ident(1), ident(2), kind), bearer=state.bearer
        )
        assert response.status == 200 and response.json["revision"] == 1
        assert response.json["created_at"] == "2020-01-02T03:04:05.123456Z"
    for route in ("relations", f"memories/{ident(1)}/relations", f"graph/neighborhood?memory_id={ident(1)}"):
        path, _, query = route.partition("?")
        response = await request(state.app, "/api/v1/profiles/alpha/" + path, query=query.encode(), bearer=state.bearer)
        assert response.status == 200, response.body
        values = response.json.get("items", response.json.get("edges", []))
        assert len(values) == 4 and all(row["revision"] == 1 for row in values)
    assert await snapshot(state) == before


@pytest.mark.parametrize("count,maximum", [(200, 200), (1000, 1000)])
async def test_revision_reads_are_batched_inside_snapshot(relations, count, maximum):
    from storage.models.relation import ClaimRelationORM
    from storage.repositories.relation_repo import RelationKey
    from storage.repositories.relation_revision_repo import RelationRevisionRepository

    state = relations
    async with state.deps.leases.acquire(context(state)) as scope:
        factory = scope.application.memory.session_factory
        async with factory() as session, session.begin():
            await session.execute(
                text(
                    (
                        "INSERT INTO claim_relations SELECT printf('source-%d',value),:t,'supports',"
                        "'2020-01-01 00:00:00.000000' FROM json_each(:xs)"
                    )
                ),
                {"t": ident(2), "xs": json.dumps(list(range(count)))},
            )
        trace = []
        engine = factory.kw["bind"].sync_engine

        def record(conn, cursor, statement, params, *args):
            if statement.startswith("WITH inputs"):
                trace.append((statement, len(params)))

        event.listen(engine, "before_cursor_execute", record)
        try:
            async with factory() as session, session.begin():
                await session.execute(text("BEGIN"))
                from sqlalchemy import select

                rows = (await session.execute(select(ClaimRelationORM))).scalars().all()
                async with factory() as writer, writer.begin():
                    await writer.execute(text("DELETE FROM claim_relations"))
                values = await RelationRevisionRepository(session).revisions_for_rows(rows, maximum=maximum)
                assert len(values) == count and set(values.values()) == {1}
                assert len(trace) == (count + 127) // 128
                assert max(p for _, p in trace) <= 384
                assert all("ORDER BY h.revision DESC LIMIT 1" in s for s, _ in trace)
                for row in rows:
                    assert set(
                        vars(RelationKey("2020-01-01T00:00:00Z", row.source_id, row.target_id, row.relation_type))
                    ) == {"created_at", "source_id", "target_id", "relation_type"}
        finally:
            event.remove(engine, "before_cursor_execute", record)
    print("BATCH", count, "chunks", len(trace), "max_params", max(p for _, p in trace), "same_snapshot")


@pytest.mark.parametrize("kind", ["fact", "belief"])
async def test_endpoint_fact_legacy_and_strict_belief_cas(relations, kind):
    state = relations
    if kind == "fact":
        from storage.repositories.fact_repo import FactRepository

        from memory_server.models.fact import Fact

        await sql(state, "DELETE FROM beliefs WHERE id=:id", {"id": ident(1)})
        async with state.deps.leases.acquire(context(state)) as scope:
            async with scope.application.memory.session_factory() as session, session.begin():
                await FactRepository(session).create(Fact(id=ident(1), subject="s", predicate="p", object="o"))
                await session.execute(text("UPDATE facts SET version='0.1.0' WHERE id=:id"), {"id": ident(1)})
    result, _ = await write(state)
    assert result.relation.revision == 1
    assert (await sql(state, "SELECT memory_type FROM receipts")) == [(kind,)]
    assert await sql(
        state, "SELECT version FROM " + ("facts" if kind == "fact" else "beliefs") + " WHERE id=:id", {"id": ident(1)}
    ) == [("0.1.0" if kind == "fact" else 1,)]


async def test_occupied_temp_shadow_and_import_has_no_io(tmp_path):
    from sqlalchemy import create_engine
    from storage.base import Base
    from storage.models.relation_revision import RelationRevisionORM, _before_create
    from storage.repositories.relation_revision_repo import RelationRevisionStorageError

    engine = create_engine("sqlite:///" + str(tmp_path / "temp-shadow.db"))
    try:
        with engine.begin() as conn:
            Base.metadata.create_all(conn)
            conn.exec_driver_sql("CREATE TEMP TABLE relation_revisions (revision INTEGER)")
            with pytest.raises(RelationRevisionStorageError):
                Base.metadata.create_all(conn)
        assert event.contains(Base.metadata, "before_create", _before_create)
        assert RelationRevisionORM.__table__.metadata is Base.metadata
    finally:
        engine.dispose()
    code = """import importlib,socket,sqlite3
import sqlalchemy
from sqlalchemy.engine import Engine
def forbidden(*a,**kw): raise AssertionError('IMPORT_IO')
sqlite3.connect=forbidden
socket.socket=forbidden
Engine.connect=forbidden
importlib.import_module('storage.models.relation_revision')
importlib.import_module('storage.models.relation_revision')
print('IMPORT_NO_IO')"""
    result = subprocess.run(
        [sys.executable, "-B", "-c", code], cwd=ROOT, env=os.environ.copy(), capture_output=True, text=True
    )
    assert result.returncode == 0 and "IMPORT_NO_IO" in result.stdout, result.stdout + result.stderr


async def test_revision_deep_history_uses_latest_index_and_overflow_before_query(relations):
    from sqlalchemy import select
    from storage.models.relation import ClaimRelationORM
    from storage.repositories.relation_revision_repo import RelationRevisionRepository, RelationRevisionStorageError

    state = relations
    first, _ = await write(state)
    async with state.deps.leases.acquire(context(state)) as scope:
        factory = scope.application.memory.session_factory
        async with factory() as session, session.begin():
            canonical = (await session.execute(select(ClaimRelationORM))).scalar_one()
            for revision in range(2, 1001):
                await session.execute(
                    text(
                        (
                            "INSERT INTO relation_revisions SELECT source_id,target_id,relation_type,:r,"
                            "0,relation_created_at,changed_at,:e,NULL,NULL,NULL FROM relation_revisions "
                            "WHERE revision=1"
                        )
                    ),
                    {"r": revision, "e": str(uuid4())},
                )
        trace = []
        engine = factory.kw["bind"].sync_engine

        def record(conn, cursor, statement, params, *args):
            trace.append(statement)

        event.listen(engine, "before_cursor_execute", record)
        try:
            async with factory() as session, session.begin():
                repo = RelationRevisionRepository(session)
                mapping = await repo.revisions_for_rows([canonical], maximum=200)
                assert list(mapping.values()) == [1000]
                assert len(trace) == 1
                plan = (
                    await (await session.connection()).exec_driver_sql(
                        "EXPLAIN QUERY PLAN " + trace[0], (ident(1), ident(2), "supports")
                    )
                ).all()
                assert any("SEARCH h USING COVERING INDEX" in row[3] for row in plan), plan
                before = len(trace)
                with pytest.raises(RelationRevisionStorageError):
                    await repo.revisions_for_rows([canonical] * 201, maximum=200)
                assert len(trace) == before
        finally:
            event.remove(engine, "before_cursor_execute", record)
    assert first.relation.revision == 1
    print("DEEP_HISTORY", 1000, "fetched_latest", mapping, "plan", plan)


async def test_native_ingestion_derived_from_actual_call_path(relations):
    from memory_server.services.ingestion_service import MemoryIngestionService

    state = relations
    async with state.deps.leases.acquire(context(state)) as scope:
        result = await MemoryIngestionService(scope.application.memory.session_factory).remember(
            "synthetic subject",
            "uses",
            "synthetic object",
            metadata={"evidence": {"derived_from": [ident(1)]}},
        )
    assert set(result) == {"fact", "receipt"}
    assert await sql(state, "SELECT source_id,target_id,relation_type,revision,actor FROM relation_revisions") == [
        (result["fact"].id, ident(1), "derived_from", 1, None)
    ]
    assert await sql(state, "SELECT operation FROM outbox_entries") == [("index_fact",)]


async def test_actual_read_service_page_and_graph_batch_ceilings(relations, monkeypatch):
    from storage.repositories.relation_revision_repo import RelationRevisionRepository

    from tests.gateway.test_http_shell import request

    state = relations
    await orm_seed(state, [dict(id=ident(i), proposition=f"batch endpoint {i}") for i in range(10, 260)])
    await sql(
        state,
        (
            "INSERT INTO claim_relations SELECT :s,b.id,k.value,'2020-01-01 00:00:00.000"
            "000' FROM beliefs b,json_each(:types) k WHERE b.id>=:lo AND b.id<:hi"
        ),
        {"s": ident(1), "lo": ident(10), "hi": ident(35), "types": json.dumps(TYPES)},
    )
    await sql(
        state,
        (
            "INSERT INTO claim_relations SELECT s.id,t.id,k.value,'2020-01-01 00:00:00.0"
            "00000' FROM beliefs s,beliefs t,json_each(:types) k WHERE s.id>=:lo AND s.i"
            "d<:hi AND t.id>=:a AND t.id<:b"
        ),
        {"lo": ident(10), "hi": ident(35), "a": ident(40), "b": ident(49), "types": json.dumps(TYPES)},
    )
    original, captures = RelationRevisionRepository.revisions_for_rows, []

    async def capture(self, rows, *, maximum):
        conn = await self.session.connection()
        captures.append(
            (
                len(rows),
                maximum,
                self.session.in_transaction(),
                await conn.run_sync(lambda c: c.connection.driver_connection.in_transaction),
            )
        )
        return await original(self, rows, maximum=maximum)

    monkeypatch.setattr(RelationRevisionRepository, "revisions_for_rows", capture)
    page = await request(state.app, "/api/v1/profiles/alpha/relations", query=b"limit=200", bearer=state.bearer)
    assert page.status == 200 and len(page.json["items"]) == 200 and page.json["has_more"], page.body
    assert captures == [(200, 200, True, True)]
    captures.clear()
    graph = await request(
        state.app,
        "/api/v1/profiles/alpha/graph/path",
        method="POST",
        headers=(("Content-Type", "application/json"),),
        body=json.dumps(
            dict(source_id=ident(1), target_id=ident(3), max_depth=2, edge_limit=1000, node_limit=2)
        ).encode(),
        bearer=state.bearer,
    )
    assert graph.status == 200, graph.body
    assert len(graph.json["edges"]) <= 1000, graph.body
    assert captures == [(1000, 1000, True, True)]
    print("SERVICE_BATCH page200 graph1000 before_release", captures)


@pytest.mark.parametrize("deleted", [False, True])
@pytest.mark.parametrize("boundary,count", [("incidence", 512), ("incidence", 513), ("pair", 16), ("pair", 17)])
async def test_exact_preflight_capacity_boundary_create_and_delete(relations, deleted, boundary, count):
    state = relations
    if deleted:
        await write(state)
    if boundary == "incidence":
        await sql(
            state,
            (
                "INSERT INTO claim_relations SELECT :s,printf('bound-%d',value),'derived_fro"
                "m','2020-01-01 00:00:00.000000' FROM json_each(:xs)"
            ),
            {"s": ident(1), "xs": json.dumps(list(range(count - int(deleted))))},
        )
    else:
        await sql(
            state,
            (
                "INSERT INTO claim_relations SELECT :s,:t,printf('unknown-%d',value),'2020-0"
                "1-01 00:00:00.000000' FROM json_each(:xs)"
            ),
            {"s": ident(1), "t": ident(2), "xs": json.dumps(list(range(count - int(deleted))))},
        )
    before = await snapshot(state)
    if count in (513, 17):
        with expect_error("E_STORAGE"):
            await write(state, command(deleted=deleted))
        assert await snapshot(state) == before
    else:
        result, _ = await write(state, command(deleted=deleted))
        assert result.deleted == deleted and result.relation.revision == (2 if deleted else 1)
        print(
            "MUTATION_BUDGET",
            boundary,
            count,
            "delete",
            deleted,
            "statements",
            state.last_handler.statements,
            "instructions",
            state.last_handler.instructions,
        )


async def test_cancel_during_queued_budget_reset_propagates_before_commit(relations, monkeypatch):
    import aiosqlite

    state, ctx = relations, fresh(relations)
    before = await snapshot(state)
    entered, release = asyncio.Event(), asyncio.Event()
    original = aiosqlite.Connection.set_progress_handler
    paused = False

    async def held(driver, callback, interval):
        nonlocal paused
        if callback is None and not paused:
            paused = True
            entered.set()
            await release.wait()
        return await original(driver, callback, interval)

    with monkeypatch.context() as instrument:
        instrument.setattr(aiosqlite.Connection, "set_progress_handler", held)
        task = asyncio.create_task(write(state, ctx=ctx))
        try:
            await asyncio.wait_for(entered.wait(), 5)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done(), "lease cleanup must retain the queued SQLite reset"
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 10)
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
    assert await snapshot(state) == before, "S16A_CANCEL_RESET_MUST_ROLL_BACK"
    assert (await write(state))[0].relation.revision == 1


@pytest.mark.parametrize(
    "stamp,expected",
    [
        ("2020-01-02 03:04:05", datetime(2020, 1, 2, 3, 4, 5, tzinfo=UTC)),
        ("2020-01-02T05:04:05.123456+02:00", datetime(2020, 1, 2, 3, 4, 5, 123456, tzinfo=UTC)),
    ],
)
async def test_virtual_legacy_delete_fences_utc_instant_not_timestamp_format(relations, stamp, expected):
    from memory_server.transports.http.errors import HttpError

    state = relations
    await sql(
        state,
        "INSERT INTO claim_relations VALUES(:s,:t,'supports',:stamp)",
        {"s": ident(1), "t": ident(2), "stamp": stamp},
    )
    try:
        result, _ = await write(state, command(deleted=True))
    except HttpError as exc:
        pytest.fail("S16A_LEGACY_TIMESTAMP_DELETE: " + exc.code)
    assert result.relation.revision == 2 and result.relation.created_at == expected
    assert await sql(state, "SELECT count(*) FROM claim_relations") == [(0,)]
    assert await sql(state, "SELECT revision,deleted FROM relation_revisions") == [(2, 1)]


# S16P: publication receipts are tested across real disk/native/lease boundaries.
@pytest.fixture
async def s16p_env(tmp_path):
    from contextlib import asynccontextmanager

    from memory_server.application.profile_bundle import BundlePolicies
    from memory_server.control.profiles import ProfileRegistry, RegistryConfiguration
    from memory_server.control.store import ControlStore
    from memory_server.runtime.daemon import Daemon
    from memory_server.runtime.runtime_manager import RuntimeManager
    from memory_server.settings import Settings
    from tests.gateway.test_daemon import factory, make_binding

    @asynccontextmanager
    async def environment(mode="first_publish_no_lock"):
        settings = Settings(_env_file=None, outbox_poll_interval_seconds=60)
        bindings = tuple(make_binding(tmp_path / name, name, settings) for name in ("alpha", "beta"))
        path = bindings[0].layout.graph_snapshot_path
        lock = bindings[0].layout.graph_lock_path
        if mode == "existing_snapshot_existing_lock":
            path.write_text('{"nodes": {}, "edges": []}')
        if mode in ("existing_snapshot_existing_lock", "initial_absent_snapshot_existing_lock"):
            lock.touch()
        store = ControlStore(tmp_path / "control")
        registry = ProfileRegistry(store, RegistryConfiguration(bindings))
        daemon = Daemon(
            registry, policies=BundlePolicies.capture(settings), manager=RuntimeManager(factory_builder=factory)
        )
        try:
            yield daemon, bindings
        finally:
            await daemon.close_all()
            store.close()

    return environment


def _s16p_ctx(profile="alpha"):
    from tests.gateway.test_daemon import ctx

    return ctx(profile)


def _s16p_code(error):
    return getattr(error, "code", type(error).__name__)


def _s16p_replace(path, content=b'{"nodes": {}, "edges": []}'):
    temp = path.with_name(path.name + ".foreign")
    temp.write_bytes(content)
    os.replace(temp, path)


def _s16p_capture(monkeypatch, manager):
    records = []
    original = manager.refresh_projection_identity

    def capture(descriptor, *, receipt, proof=None):
        records.append((descriptor, receipt, proof))

    monkeypatch.setattr(manager, "refresh_projection_identity", capture)
    return records, original


def _s16p_fault_lock(path, mode):
    if mode == "foreign_lock_after_unlock":
        subprocess.run(
            [
                sys.executable,
                "-c",
                "import os,pathlib,sys; p=pathlib.Path(sys.argv[1]); "
                "t=p.with_name('foreign-lock'); t.write_bytes(b'foreign-lock-sentinel'); os.replace(t,p)",
                str(path),
            ],
            check=True,
        )
    elif mode == "lock_to_symlink":
        sentinel = path.with_name("foreign-sentinel")
        sentinel.write_bytes(b"foreign-lock-sentinel")
        path.unlink()
        path.symlink_to(sentinel)
    elif mode == "lock_nlink2":
        os.link(path, path.with_name("hardlink"))


def _s16p_lock_race(monkeypatch, mode, manager, lock):
    import memory_server.providers.graph_provider as gp

    if mode == "foreign_lock_before_create":
        pin = gp._pin_parent

        def raced(path):
            fd = pin(path)
            if not lock.exists():
                subprocess.run(
                    [
                        sys.executable,
                        "-c",
                        "import pathlib,sys; pathlib.Path(sys.argv[1]).write_bytes(b'foreign-lock-sentinel')",
                        str(lock),
                    ],
                    check=True,
                )
            return fd

        monkeypatch.setattr(gp, "_pin_parent", raced)
    else:
        refresh = manager.refresh_projection_identity

        def fault(descriptor, *, receipt, proof=None):
            _s16p_fault_lock(lock, mode)
            return refresh(descriptor, receipt=receipt, proof=proof)

        monkeypatch.setattr(manager, "refresh_projection_identity", fault)


_S16P_MODES = (
    "first_publish_no_lock",
    "existing_snapshot_existing_lock",
    "initial_absent_snapshot_existing_lock",
    "foreign_lock_before_create",
    "foreign_lock_after_unlock",
    "lock_to_symlink",
    "lock_nlink2",
)


async def _s16p_publish_next(s16p_env, monkeypatch, mode, native):
    from memory_server.runtime.daemon import LeaseError

    negative = mode.startswith("foreign_") or mode.startswith("lock_")
    marker = "S16P_PUBLICATION_NEXT_LEASE" if native else "S16P_LEGACY_NEXT_LEASE"
    async with s16p_env(mode) as (daemon, bindings):
        manager = daemon.manager
        path, lock = bindings[0].layout.graph_snapshot_path, bindings[0].layout.graph_lock_path
        replaces, creates = [], []
        replace_fn, open_fn = os.replace, os.open

        def observed_replace(*args, **kwargs):
            result = replace_fn(*args, **kwargs)
            replaces.append(path.stat().st_ino)
            return result

        def observed_open(name, flags, *args, **kwargs):
            if (str(name) == lock.name or str(name) == str(lock)) and flags & os.O_CREAT:
                creates.append((flags, kwargs.get("dir_fd")))
            return open_fn(name, flags, *args, **kwargs)

        monkeypatch.setattr(os, "replace", observed_replace)
        monkeypatch.setattr(os, "open", observed_open)
        async with daemon.acquire(_s16p_ctx()) as session:
            graph = session.runtime.dependencies.graph
            worker = session.runtime.dependencies.outbox_worker
            entry = manager._entries[0]
            before = dict(entry.evidence)
            if mode == "first_publish_no_lock":
                assert not path.exists() and not lock.exists(), marker
            if negative:
                _s16p_lock_race(monkeypatch, mode, manager, lock)
            if not native:
                try:
                    graph.add_node("s16p", "fact", "published")
                    outcome = "success"
                except Exception as error:
                    outcome = _s16p_code(error)
        if native:
            try:
                await worker._blocking(graph.add_node, "s16p", "fact", "published", kind="s16p-publish")
                outcome = "success"
            except Exception as error:
                outcome = _s16p_code(error)
        if negative:
            assert outcome == "E_RUNTIME_RESOURCE_IDENTITY_CHANGED", "S16P_LOCK_IDENTITY_UNCHANGED"
            assert entry.evidence == before, "S16P_LOCK_IDENTITY_UNCHANGED"
            if mode in ("foreign_lock_before_create", "foreign_lock_after_unlock", "lock_to_symlink"):
                assert lock.read_bytes() == b"foreign-lock-sentinel", "S16P_LOCK_IDENTITY_UNCHANGED"
            assert len(creates) <= 1, "S16P_LOCK_IDENTITY_UNCHANGED"
            return
        try:
            async with daemon.acquire(_s16p_ctx()) as again:
                outcome = "success" if again.runtime is session.runtime else "different-runtime"
        except LeaseError as error:
            outcome = error.code
        assert replaces and outcome == "success", marker
        assert json.loads(path.read_text())["nodes"]["s16p"]["id"] == "s16p", marker
        assert entry.evidence[path][2] == path.stat().st_ino == replaces[-1], marker
        assert {p: v for p, v in entry.evidence.items() if p not in (path, lock)} == {
            p: v for p, v in before.items() if p not in (path, lock)
        }, marker
        if mode == "first_publish_no_lock":
            assert len(creates) == 1 and creates[0][1] is not None, "S16P_LOCK_IDENTITY_UNCHANGED"
            assert creates[0][0] & os.O_EXCL and creates[0][0] & os.O_NOFOLLOW, "S16P_LOCK_IDENTITY_UNCHANGED"
        async with daemon.acquire(_s16p_ctx()):
            graph.add_node("s16p-second", "fact", "second")
        assert len(creates) == (1 if mode == "first_publish_no_lock" else 0), "S16P_LOCK_IDENTITY_UNCHANGED"


@pytest.mark.parametrize("mode", _S16P_MODES)
async def test_s16p_legacy_persist_then_next_lease(s16p_env, monkeypatch, mode):
    await _s16p_publish_next(s16p_env, monkeypatch, mode, False)


@pytest.mark.parametrize("mode", _S16P_MODES)
async def test_s16p_publication_proof_then_next_lease(s16p_env, monkeypatch, mode):
    await _s16p_publish_next(s16p_env, monkeypatch, mode, True)


@pytest.mark.parametrize(
    "fault",
    ["none", "arbitrary_object", "copied_lease", "released_lease", "foreign_task", "other_manager", "forged_receipt"],
)
async def test_s16p_refresh_requires_live_proof(s16p_env, monkeypatch, fault):
    import copy

    from memory_server.runtime.runtime_manager import RuntimeManager

    async with s16p_env() as (daemon, _):
        records, refresh = _s16p_capture(monkeypatch, daemon.manager)
        async with daemon.acquire(_s16p_ctx()) as session:
            session.runtime.dependencies.graph.add_node("a", "fact", "a")
            descriptor, receipt, proof = records[-1]
            before = dict(daemon.manager._entries[0].evidence)
            bad = None if fault == "none" else object() if fault == "arbitrary_object" else proof
            if fault == "copied_lease":
                bad = copy.copy(proof)
            if fault == "forged_receipt":
                receipt = copy.copy(receipt)
            if fault == "released_lease":
                daemon.manager._revoke_lease_proof(proof)
            if fault == "other_manager":
                refresh = RuntimeManager().refresh_projection_identity

            def attempt():
                try:
                    refresh(descriptor, receipt=receipt, proof=bad)
                    return "success"
                except Exception as error:
                    return _s16p_code(error)

            if fault == "foreign_task":

                async def foreign():
                    return attempt()

                outcome = await asyncio.create_task(foreign())
            else:
                outcome = attempt()
            assert outcome == "E_RUNTIME_RESOURCE_IDENTITY_CHANGED", "S16P_PROOF_REQUIRED"
            assert daemon.manager._entries[0].evidence == before, "S16P_PROOF_REQUIRED"


@pytest.mark.parametrize(
    "fault",
    [
        "final_symlink",
        "parent_symlink",
        "hardlink_nlink2",
        "fifo",
        "parent_replaced_directory",
        "unrelated_resource_changed",
    ],
)
async def test_s16p_refresh_rejects_unsafe_without_evidence_change(s16p_env, monkeypatch, fault):
    async with s16p_env() as (daemon, bindings):
        records, refresh = _s16p_capture(monkeypatch, daemon.manager)
        async with daemon.acquire(_s16p_ctx()) as session:
            graph = session.runtime.dependencies.graph
            graph.add_node("a", "fact", "a")
            descriptor, receipt, proof = records[-1]
            path = bindings[0].layout.graph_snapshot_path
            before = dict(daemon.manager._entries[0].evidence)
            if fault == "final_symlink":
                retained = path.with_name("retained")
                path.rename(retained)
                path.symlink_to(retained)
            elif fault == "hardlink_nlink2":
                os.link(path, path.with_name("hardlink"))
            elif fault == "fifo":
                path.unlink()
                os.mkfifo(path)
            elif fault.startswith("parent_"):
                parent = path.parent
                moved = parent.with_name("moved-alpha")
                parent.rename(moved)
                if fault == "parent_symlink":
                    parent.symlink_to(moved, target_is_directory=True)
                else:
                    parent.mkdir()
            else:
                unrelated = bindings[0].layout.vector.local_path
                unrelated.mkdir()
            try:
                refresh(descriptor, receipt=receipt, proof=proof)
                outcome = "success"
            except Exception as error:
                outcome = _s16p_code(error)
            expected = (
                "E_RUNTIME_RESOURCE_ALIAS"
                if fault in ("final_symlink", "hardlink_nlink2", "fifo")
                else "E_RUNTIME_RESOURCE_IDENTITY_CHANGED"
            )
            assert outcome == expected, "S16P_UNSAFE_EVIDENCE_UNCHANGED"
            assert daemon.manager._entries[0].evidence == before, "S16P_UNSAFE_EVIDENCE_UNCHANGED"


async def test_s16p_refresh_rejects_foreign_descriptor(s16p_env, monkeypatch):
    async with s16p_env() as (daemon, bindings):
        async with daemon.acquire(_s16p_ctx()):
            pass
        async with daemon.acquire(_s16p_ctx("beta")) as session:
            graph = session.runtime.dependencies.graph
            worker = session.runtime.dependencies.outbox_worker
        records, refresh = _s16p_capture(monkeypatch, daemon.manager)
        await worker._blocking(graph.add_node, "b", "fact", "b", kind="s16p-beta")
        descriptor, receipt, proof = records[-1]
        # Narrow immutable-binding fault: beta-valid disk/fds and registered writer,
        # but an alpha immutable binding. Only _publication_matches owns this guard.
        object.__setattr__(receipt.scope.binding, "descriptor", bindings[0].descriptor)
        before = [dict(e.evidence) for e in daemon.manager._entries]
        try:
            refresh(descriptor, receipt=receipt, proof=proof)
            outcome = "success"
        except Exception as error:
            outcome = _s16p_code(error)
        assert outcome == "E_RUNTIME_DESCRIPTOR_CONFLICT", "S16P_FOREIGN_REJECTED"
        assert before == [e.evidence for e in daemon.manager._entries], "S16P_FOREIGN_REJECTED"
        with pytest.raises(Exception) as error:
            daemon.manager.bind_projection_publisher(bindings[0].descriptor, graph=graph, lease_probe=lambda: None)
        assert _s16p_code(error.value) == "E_RUNTIME_DESCRIPTOR_CONFLICT", "S16P_FOREIGN_REJECTED"


@pytest.mark.parametrize("fault", ["foreign_regular_fd", "dup_foreign_fd", "closed_reused_number"])
async def test_s16p_refresh_rejects_foreign_fd(s16p_env, monkeypatch, tmp_path, fault):
    async with s16p_env() as (daemon, _):
        records, refresh = _s16p_capture(monkeypatch, daemon.manager)
        async with daemon.acquire(_s16p_ctx()) as session:
            session.runtime.dependencies.graph.add_node("a", "fact", "a")
            descriptor, receipt, proof = records[-1]
            original = receipt.snapshot
            foreign = (tmp_path / "alpha" / "foreign-fd").open("w+")
            try:
                if fault == "closed_reused_number":
                    number = original.fileno()
                    original.close()
                    os.dup2(foreign.fileno(), number)
                    substitute = os.fdopen(number, "r+")
                elif fault == "dup_foreign_fd":
                    substitute = os.fdopen(os.dup(foreign.fileno()), "r+")
                else:
                    substitute = foreign
                object.__setattr__(receipt, "snapshot", substitute)
                try:
                    refresh(descriptor, receipt=receipt, proof=proof)
                    outcome = "success"
                except Exception as error:
                    outcome = _s16p_code(error)
                assert outcome == "E_RUNTIME_RESOURCE_IDENTITY_CHANGED", "S16P_FOREIGN_FD_REJECTED"
            finally:
                original.close()
                foreign.close()


_S16P_LOCK_PROBE = """import fcntl,sys
f=open(sys.argv[1], 'r+b')
try:
 fcntl.flock(f, fcntl.LOCK_EX|fcntl.LOCK_NB)
except BlockingIOError:
 sys.exit(7)
"""


@pytest.mark.parametrize(
    "mode",
    [
        "load_snapshot",
        "resync",
        "plain_file_lock",
        "unlocked_read",
        "windows_fallback",
        "fsync_failure",
        "dir_fsync_failure",
        "unlock_before_mint",
    ],
)
async def test_s16p_writer_custody_and_reader_no_proof(s16p_env, monkeypatch, mode):
    import memory_server.providers.graph_provider as gp

    async with s16p_env() as (daemon, bindings):
        async with daemon.acquire(_s16p_ctx()) as session:
            graph = session.runtime.dependencies.graph
            graph.add_node("a", "fact", "a")
            minted = []
            mint = gp._SnapshotPublication._mint
            lock = bindings[0].layout.graph_lock_path

            def observed(publication):
                probe = subprocess.run([sys.executable, "-c", _S16P_LOCK_PROBE, str(lock)])
                assert probe.returncode == 7, "S16P_WRITER_ONLY_PROOF"
                assert publication.snapshot.fileno() != publication.lock.fileno(), "S16P_WRITER_ONLY_PROOF"
                mint(publication)
                minted.append(publication.receipt)

            monkeypatch.setattr(gp._SnapshotPublication, "_mint", observed)
            graph.add_node("b", "fact", "b")
            assert len(minted) == 1, "S16P_WRITER_ONLY_PROOF"
            assert subprocess.run([sys.executable, "-c", _S16P_LOCK_PROBE, str(lock)]).returncode == 0, (
                "S16P_WRITER_ONLY_PROOF"
            )
            minted.clear()
            before = dict(daemon.manager._entries[0].evidence)
            if mode == "load_snapshot":
                graph.load_snapshot()
            elif mode == "resync":
                graph._resync_from_disk(graph._snapshot_path)
            elif mode == "plain_file_lock":
                with gp._file_lock(lock, managed=graph._projection_binding) as yielded:
                    assert yielded is None, "S16P_WRITER_ONLY_PROOF"
            elif mode == "unlocked_read":
                json.loads(graph._snapshot_path.read_text())
            else:
                if mode == "windows_fallback":
                    monkeypatch.setattr(gp, "_fcntl", None)
                elif mode.endswith("fsync_failure"):
                    fsync = os.fsync

                    def broken(fd):
                        import stat

                        if mode == "fsync_failure" or stat.S_ISDIR(os.fstat(fd).st_mode):
                            raise OSError("S16P_SYNTHETIC_CANARY")
                        return fsync(fd)

                    monkeypatch.setattr(os, "fsync", broken)
                else:
                    check = gp._SnapshotPublication._check_lock

                    def unlocked(publication):
                        check(publication)
                        gp._fcntl.flock(publication.lock.fileno(), gp._fcntl.LOCK_UN)

                    monkeypatch.setattr(gp._SnapshotPublication, "_check_lock", unlocked)
                try:
                    graph.add_node("c", "fact", "c")
                except Exception:
                    pass
            assert not minted and daemon.manager._entries[0].evidence == before, "S16P_WRITER_ONLY_PROOF"
            with pytest.raises(Exception) as error:
                daemon.manager.refresh_projection_identity(bindings[0].descriptor, receipt=object(), proof=object())
            assert _s16p_code(error.value) == "E_RUNTIME_RESOURCE_IDENTITY_CHANGED", "S16P_WRITER_ONLY_PROOF"


async def test_s16p_refresh_preserves_foreign_alias_denial(s16p_env, monkeypatch):
    async with s16p_env() as (daemon, _):
        async with daemon.acquire(_s16p_ctx("beta")):
            pass
        records, refresh = _s16p_capture(monkeypatch, daemon.manager)
        async with daemon.acquire(_s16p_ctx()) as session:
            graph = session.runtime.dependencies.graph
            graph.add_node("a", "fact", "a")
            descriptor, receipt, proof = records[-1]
            beta = next(e for e in daemon.manager._entries if e.descriptor.profile_id == "beta")
            # Reservation fault after publication isolates the unchanged alias loop.
            beta.evidence[Path("/s16p-foreign-reservation")] = receipt.staged
            before = [dict(e.evidence) for e in daemon.manager._entries]
            try:
                refresh(descriptor, receipt=receipt, proof=proof)
                outcome = "success"
            except Exception as error:
                outcome = _s16p_code(error)
            assert outcome == "E_RUNTIME_RESOURCE_ALIAS", "S16P_ALIAS_STILL_DENIED"
            assert before == [e.evidence for e in daemon.manager._entries], "S16P_ALIAS_STILL_DENIED"


@pytest.mark.parametrize("boundary", ["file_fsync", "dir_fsync", "refresh"])
async def test_s16p_refresh_failure_is_typed_and_atomic(s16p_env, monkeypatch, caplog, boundary):
    async with s16p_env() as (daemon, _):
        async with daemon.acquire(_s16p_ctx()) as session:
            graph = session.runtime.dependencies.graph
            before = dict(daemon.manager._entries[0].evidence)
            canary = "/private/S16P_CANARY SELECT credential"
            if boundary == "refresh":

                def broken(*args):
                    raise OSError(canary)

                monkeypatch.setattr(daemon.manager, "_same_published_fd", broken)
            else:
                fsync = os.fsync

                def broken(fd):
                    import stat

                    if boundary == "file_fsync" or stat.S_ISDIR(os.fstat(fd).st_mode):
                        raise OSError(canary)
                    return fsync(fd)

                monkeypatch.setattr(os, "fsync", broken)
            try:
                graph.from_dict({"nodes": {"a": {"id": "a", "type": "fact", "name": "a"}}, "edges": []})
                error = None
            except Exception as caught:
                error = caught
            assert _s16p_code(error) == "E_RUNTIME_RESOURCE_IDENTITY_CHANGED", "S16P_TYPED_ATOMIC"
            assert error.__suppress_context__ and canary not in str(error) + caplog.text, "S16P_TYPED_ATOMIC"
            assert daemon.manager._entries[0].evidence == before and graph._full_publish_pending, "S16P_TYPED_ATOMIC"
        try:
            async with daemon.acquire(_s16p_ctx()):
                outcome = "success"
        except Exception as caught:
            outcome = _s16p_code(caught)
        assert outcome == "E_STORAGE", "S16P_TYPED_ATOMIC"


async def test_s16p_no_publication_unchanged(s16p_env, monkeypatch, tmp_path):
    from memory_server.providers.graph_provider import SimpleGraph

    async with s16p_env() as (daemon, bindings):
        async with daemon.acquire(_s16p_ctx()) as session:
            graph = session.runtime.dependencies.graph
            before = dict(daemon.manager._entries[0].evidence)
            records, refresh = _s16p_capture(monkeypatch, daemon.manager)
            async with daemon.acquire(_s16p_ctx()) as nested:
                assert nested.runtime is session.runtime, "S16P_NO_PUBLISH_NO_REFRESH"
                graph.save_snapshot()
            graph.save_snapshot(tmp_path / "unmanaged.json")
            SimpleGraph().save_snapshot()
            assert not records and before == daemon.manager._entries[0].evidence, "S16P_NO_PUBLISH_NO_REFRESH"
            scope = daemon.manager._prepare_projection_call(graph._projection_binding, native=False)
            with pytest.raises(Exception) as error:
                refresh(bindings[0].descriptor, receipt=scope, proof=scope.proof)
            assert _s16p_code(error.value) == "E_RUNTIME_RESOURCE_IDENTITY_CHANGED", "S16P_NO_PUBLISH_NO_REFRESH"
            daemon.manager._abort_projection_call(scope)
            _s16p_replace(bindings[0].layout.graph_snapshot_path)
        try:
            async with daemon.acquire(_s16p_ctx()):
                outcome = "success"
        except Exception as error:
            outcome = _s16p_code(error)
        assert outcome == "E_STORAGE", "S16P_NO_PUBLISH_NO_REFRESH"
    legacy = SimpleGraph(tmp_path / "legacy" / "graph.json")
    legacy.add_node("l", "fact", "legacy")
    reopened = SimpleGraph(legacy._snapshot_path)
    reopened.load_snapshot()
    assert reopened.get_node("l") is not None, "S16P_NO_PUBLISH_NO_REFRESH"


@pytest.mark.parametrize(
    "mode",
    [
        "native",
        "direct_loop",
        "refresh_error",
        "close_before_refresh",
        "cancelled_scope",
        "multi_publish_chain",
        "entry_completion",
        "batch_completion",
        "entry_refresh_error",
        "batch_refresh_error",
    ],
)
async def test_s16p_publish_refresh_order_and_completion(s16p_env, monkeypatch, mode):
    import memory_server.providers.graph_provider as gp

    async with s16p_env() as (daemon, _):
        async with daemon.acquire(_s16p_ctx()) as session:
            runtime = session.runtime
            graph, worker = runtime.dependencies.graph, runtime.dependencies.outbox_worker
        events = []
        fsync, replace_fn, mint = os.fsync, os.replace, gp._SnapshotPublication._mint
        refresh, consume = daemon.manager.refresh_projection_identity, daemon.manager._consume_publication

        def sync(fd):
            import stat

            events.append("dir_fsync" if stat.S_ISDIR(os.fstat(fd).st_mode) else "file_fsync")
            return fsync(fd)

        def replace_observed(*args, **kwargs):
            events.append("replace")
            return replace_fn(*args, **kwargs)

        def minted(publication):
            events.append("readback")
            mint(publication)
            events.append("mint")

        def consumed(receipt):
            events.append("refresh_commit")
            consume(receipt)
            events.append("consume")

        def refreshed(descriptor, *, receipt, proof=None):
            probe = subprocess.run([sys.executable, "-c", _S16P_LOCK_PROBE, str(graph._projection_binding.lock_path)])
            assert probe.returncode == 0, "S16P_REFRESH_ORDER"
            events.append("unlock")
            if mode == "refresh_error" or mode.endswith("_refresh_error"):
                _s16p_replace(graph._snapshot_path)
            elif mode == "close_before_refresh":
                daemon.manager._entries[0].state = "CLOSING"
            elif mode == "cancelled_scope":
                receipt.scope.abandoned = True
            return refresh(descriptor, receipt=receipt, proof=proof)

        monkeypatch.setattr(os, "fsync", sync)
        monkeypatch.setattr(os, "replace", replace_observed)
        monkeypatch.setattr(gp._SnapshotPublication, "_mint", minted)
        monkeypatch.setattr(daemon.manager, "refresh_projection_identity", refreshed)
        monkeypatch.setattr(daemon.manager, "_consume_publication", consumed)

        if mode.startswith(("entry_", "batch_")):
            from storage.outbox import OutboxRepository

            worker._embedder = None
            completed = OutboxRepository.mark_completed

            async def marked(repo, entry_id):
                events.append("mark_completed")
                return await completed(repo, entry_id)

            monkeypatch.setattr(OutboxRepository, "mark_completed", marked)
            async with worker._session_factory() as db:
                row = await OutboxRepository(db).add_entry(
                    "fact",
                    "s16p-row",
                    "index_fact",
                    {"subject": "subject", "predicate": "supports", "object": "object"},
                )
                await db.commit()
            if mode.startswith("batch_"):
                await worker._process_fact_batch([row])
            else:
                async with worker._session_factory() as db:
                    await worker._process_entry(db, OutboxRepository(db), row)
                    await db.commit()
            async with worker._session_factory() as db:
                status = (
                    await db.execute(text("SELECT status FROM outbox_entries WHERE id=:id"), {"id": row.id})
                ).scalar_one()
            if mode.endswith("_refresh_error"):
                assert status != "completed" and "mark_completed" not in events, "S16P_REFRESH_ORDER"
            else:
                assert status == "completed" and events.index("consume") < events.index("mark_completed"), (
                    "S16P_REFRESH_ORDER"
                )
            return

        def publication():
            graph.add_node("a", "fact", "a")
            if mode == "multi_publish_chain":
                graph.add_node("b", "fact", "b")
            events.append("native_return")
            return True

        before = dict(daemon.manager._entries[0].evidence)
        try:
            if mode == "direct_loop":
                async with daemon.acquire(_s16p_ctx()):
                    publication()
            else:
                assert await worker._blocking(publication, kind="s16p-order") is True, "S16P_REFRESH_ORDER"
            outcome = "success"
        except Exception as error:
            outcome = _s16p_code(error)
        if mode in ("refresh_error", "close_before_refresh", "cancelled_scope"):
            assert outcome == "E_RUNTIME_RESOURCE_IDENTITY_CHANGED" and "consume" not in events, "S16P_REFRESH_ORDER"
            assert before == daemon.manager._entries[0].evidence, "S16P_REFRESH_ORDER"
        else:
            assert outcome == "success", "S16P_REFRESH_ORDER"
            expected = ["file_fsync", "replace", "dir_fsync", "readback", "mint", "unlock", "refresh_commit", "consume"]
            assert [events.index(e) for e in expected] == sorted(events.index(e) for e in expected), (
                "S16P_REFRESH_ORDER"
            )
            if mode != "direct_loop":
                assert events.index("native_return") < events.index("refresh_commit"), "S16P_REFRESH_ORDER"
            assert not graph._journal and not daemon.manager._projection_calls, "S16P_REFRESH_ORDER"


@pytest.mark.parametrize("mode", ["loop_add_node", "loop_save_snapshot"])
async def test_s16p_native_publication_concurrent_loop_persist_no_hang(tmp_path, mode):
    # A parent-process watchdog is required because a blocked loop cannot time out itself.
    child = subprocess.Popen(
        [sys.executable, "-B", "-c", _S16P_CONCURRENT_CHILD, str(tmp_path), mode],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        output, _ = child.communicate(timeout=10)
        outcome = child.returncode
    except subprocess.TimeoutExpired:
        child.kill()
        output, _ = child.communicate()
        outcome = "watchdog-killed-and-reaped"
    print(output)
    assert outcome == 0 and "CONTENDED_LOOP_ENTRY" in output and "NATIVE_UNLOCKED" in output, "S16P_NATIVE_LOOP_NO_HANG"


_S16P_CONCURRENT_CHILD = r"""
import asyncio, sys, threading
from pathlib import Path
from memory_server.application.profile_bundle import BundlePolicies
from memory_server.control.profiles import ProfileRegistry, RegistryConfiguration
from memory_server.control.store import ControlStore
from memory_server.providers import graph_provider as gp
from memory_server.runtime.daemon import Daemon
from memory_server.runtime.runtime_manager import RuntimeManager
from memory_server.settings import Settings
from tests.gateway.test_daemon import factory, make_binding, ctx
async def main():
    root, mode = Path(sys.argv[1]), sys.argv[2]
    settings = Settings(_env_file=None, outbox_poll_interval_seconds=0.01)
    binding = make_binding(root/'profile', 'alpha', settings)
    binding.layout.graph_snapshot_path.write_text('{"nodes": {}, "edges": []}')
    binding.layout.graph_lock_path.touch()
    store = ControlStore(root/'control')
    daemon = Daemon(ProfileRegistry(store, RegistryConfiguration((binding,))),
                    policies=BundlePolicies.capture(settings), manager=RuntimeManager(factory_builder=factory))
    async with daemon.acquire(ctx()) as session:
        graph, worker = session.runtime.dependencies.graph, session.runtime.dependencies.outbox_worker
    entered, competing, release = threading.Event(), threading.Event(), threading.Event()
    original = gp._SnapshotPublication.write
    initialize = gp._SnapshotPublication.__init__
    def initialize_observed(publication, *args):
        initialize(publication, *args)
        if threading.current_thread() is threading.main_thread() and entered.is_set():
            print("CONTENDED_LOOP_ENTRY", flush=True)
            competing.set()
    gp._SnapshotPublication.__init__ = initialize_observed
    def mint(publication):
        if threading.current_thread() is not threading.main_thread():
            entered.set()
            release.wait()
        original(publication)
    gp._SnapshotPublication.write = mint
    def control():
        entered.wait()
        competing.wait()
        release.set()
    control_thread = threading.Thread(target=control, daemon=True)
    control_thread.start()
    async def native():
        try:
            await worker._blocking(graph.add_node, 'native', 'fact', 'native', kind='s16p-concurrent')
        except Exception as error:
            assert getattr(error, 'code', '') == 'E_RUNTIME_RESOURCE_IDENTITY_CHANGED'
        print('NATIVE_UNLOCKED', flush=True)
    task = asyncio.create_task(native())
    while not entered.is_set():
        await asyncio.sleep(0.001)
    try:
        if mode == 'loop_add_node':
            graph.add_node('loop', 'fact', 'loop')
        else:
            graph.save_snapshot()
    except Exception as error:
        assert getattr(error, 'code', '') == 'E_RUNTIME_RESOURCE_IDENTITY_CHANGED'
    await task
    control_thread.join(1)
    await daemon.close_all()
    store.close()
asyncio.run(main())
"""


@pytest.mark.parametrize("mode", ["persist", "same_path_export", "managed_reader_lock"])
async def test_s16p_managed_missing_parent_never_mkdir(s16p_env, monkeypatch, mode):
    import memory_server.providers.graph_provider as gp

    async with s16p_env() as (daemon, _):
        async with daemon.acquire(_s16p_ctx()) as session:
            graph = session.runtime.dependencies.graph
            parent = graph._snapshot_path.parent
            pin = gp._pin_parent

            def removed(path):
                if parent.exists():
                    parent.rename(parent.with_name("removed-parent"))
                return pin(path)

            monkeypatch.setattr(gp, "_pin_parent", removed)
            mkdirs = []
            mkdir = os.mkdir

            def recorded(*args, **kwargs):
                mkdirs.append(args)
                return mkdir(*args, **kwargs)

            monkeypatch.setattr(os, "mkdir", recorded)
            try:
                if mode == "persist":
                    graph.add_node("a", "fact", "a")
                elif mode == "same_path_export":
                    graph.save_snapshot(graph._snapshot_path)
                else:
                    with gp._file_lock(graph._projection_binding.lock_path, managed=graph._projection_binding):
                        pass
                outcome = "success"
            except Exception as error:
                outcome = _s16p_code(error)
            assert not mkdirs and not parent.exists(), "S16P_MANAGED_NO_MKDIR"
            assert outcome == "E_RUNTIME_RESOURCE_IDENTITY_CHANGED", "S16P_MANAGED_NO_MKDIR"


@pytest.mark.parametrize(
    "mode",
    [
        "double_consume",
        "expired",
        "abandoned_scope",
        "foreign_publish_after_unlock",
        "changed_generation",
        "refresh_without_publish",
    ],
)
async def test_s16p_receipt_replay_expiry_and_intervening_publish(s16p_env, monkeypatch, mode):
    import memory_server.runtime.runtime_manager as rm

    async with s16p_env() as (daemon, bindings):
        records, refresh = _s16p_capture(monkeypatch, daemon.manager)
        async with daemon.acquire(_s16p_ctx()) as session:
            graph = session.runtime.dependencies.graph
            graph.add_node("a", "fact", "a")
            descriptor, receipt, proof = records[-1]
            if mode == "double_consume":
                refresh(descriptor, receipt=receipt, proof=proof)
            elif mode == "expired":
                from types import SimpleNamespace

                monkeypatch.setattr(rm, "time", SimpleNamespace(monotonic=lambda: receipt.expires_at + 1))
            elif mode == "abandoned_scope":
                receipt.scope.abandoned = True
            elif mode == "foreign_publish_after_unlock":
                _s16p_replace(graph._snapshot_path)
            elif mode == "changed_generation":
                graph._projection_binding.state[2] += 1
            else:
                # A registered armed invocation is not a completed writer. Build an
                # unsigned reader-derived record at the isolated issuer fault seam.
                from memory_server.providers.graph_provider import _pin_parent, _PublicationReceipt

                refresh(descriptor, receipt=receipt, proof=proof)
                scope = daemon.manager._prepare_projection_call(graph._projection_binding, native=False)
                path = graph._snapshot_path
                receipt = _PublicationReceipt(
                    scope,
                    dict(scope.before),
                    dict(scope.before),
                    scope.chains,
                    scope.before[path],
                    scope.before[scope.binding.lock_path],
                    False,
                    path.open("r+"),
                    scope.binding.lock_path.open("r+b"),
                    _pin_parent(path),
                    None,
                    os.getpid(),
                    0,
                    monotonic(),
                    monotonic() + 30,
                    (),
                    graph._nodes,
                    graph._edges,
                )
                scope.records.append(receipt)
                proof = scope.proof
            before = dict(daemon.manager._entries[0].evidence)
            try:
                refresh(bindings[0].descriptor, receipt=receipt, proof=proof)
                outcome = "success"
            except Exception as error:
                outcome = _s16p_code(error)
            assert outcome == "E_RUNTIME_RESOURCE_IDENTITY_CHANGED", "S16P_RECEIPT_ONESHOT"
            assert before == daemon.manager._entries[0].evidence, "S16P_RECEIPT_ONESHOT"


@pytest.mark.parametrize("mode", ["consumed", "active", "native_cancel", "native_error"])
async def test_s16p_abort_consumed_or_missing_is_noop(s16p_env, monkeypatch, mode):
    import threading

    async with s16p_env() as (daemon, _):
        async with daemon.acquire(_s16p_ctx()) as session:
            graph = session.runtime.dependencies.graph
            worker = session.runtime.dependencies.outbox_worker
        manager = daemon.manager
        records, refresh = _s16p_capture(monkeypatch, manager)
        if mode.startswith("native_"):
            minted, release = threading.Event(), threading.Event()

            def publication():
                graph.add_node("a", "fact", "a")
                minted.set()
                release.wait(3)
                if mode == "native_error":
                    raise ValueError("synthetic after mint")

            task = asyncio.create_task(worker._blocking(publication, kind="s16p-cleanup"))
            while not minted.is_set():
                await asyncio.sleep(0.001)
            scope = next(iter(manager._projection_calls))
            if mode == "native_cancel":
                task.cancel()
            release.set()
            try:
                await task
            except (asyncio.CancelledError, ValueError):
                pass
            for _ in range(100):
                if not manager._projection_calls:
                    break
                await asyncio.sleep(0.01)
            assert not manager._projection_calls and all(r.snapshot.closed and r.lock.closed for r in scope.records), (
                "S16P_ABORT_NOOP"
            )
            return
        async with daemon.acquire(_s16p_ctx()):
            graph.add_node("a", "fact", "a")
            descriptor, receipt, proof = records[-1]
            if mode == "consumed":
                refresh(descriptor, receipt=receipt, proof=proof)
            generation = graph._projection_binding.state[2]
            evidence = dict(manager._entries[0].evidence)
            manager.abort_projection_publication(receipt)
            manager.abort_projection_publication(receipt)
            manager.abort_projection_publication(object())
            assert generation == graph._projection_binding.state[2] and evidence == manager._entries[0].evidence, (
                "S16P_ABORT_NOOP"
            )
            assert receipt.snapshot.closed and receipt.lock.closed, "S16P_ABORT_NOOP"
            with pytest.raises(Exception) as error:
                refresh(descriptor, receipt=receipt, proof=proof)
            assert _s16p_code(error.value) == "E_RUNTIME_RESOURCE_IDENTITY_CHANGED", "S16P_ABORT_NOOP"


# S16B: exercise the real admitted worker, canonical S12 queue and durable graph.
async def _s16b_process(state, entry_id):
    from storage.outbox import OutboxEntry, OutboxEntryORM, OutboxRepository

    async with state.deps.leases.acquire(context(state)) as scope:
        worker = scope.runtime.dependencies.outbox_worker
        async with worker._session_factory() as db:
            row = await db.get(OutboxEntryORM, entry_id, populate_existing=True)
            outcome = await worker._process_entry(db, OutboxRepository(db), OutboxEntry.from_orm(row))
            await db.commit()
        async with worker._session_factory() as db:
            row = await db.get(OutboxEntryORM, entry_id, populate_existing=True)
            return outcome, row.status, row.error


async def _s16b_queue(state):
    return [
        row[0]
        for row in await sql(state, "SELECT id FROM outbox_entries WHERE operation='relation_sync' ORDER BY rowid")
    ]


async def _s16b_graph(state):
    async with state.deps.leases.acquire(context(state)) as scope:
        graph = scope.runtime.dependencies.graph
        return graph, graph._snapshot_path


def _s16b_matching(data, source=None, target=None, kind="supports"):
    return [
        e
        for e in data["edges"]
        if (e["source_id"], e["target_id"], e["relation"]) == (source or ident(1), target or ident(2), kind)
    ]


def _s16b_reopen(path):
    from memory_server.providers.graph_provider import SimpleGraph

    reopened = SimpleGraph(path)
    reopened.load_snapshot()
    return reopened.to_dict()


async def test_outbox_reordered_create_delete_recreate_converges_to_latest(relations):
    import itertools

    state = relations
    await write(state)
    await write(state, command(deleted=True))
    await write(state)
    queue = await _s16b_queue(state)
    before = await snapshot(state)
    graph, path = await _s16b_graph(state)
    for order in itertools.permutations(queue):
        for entry_id in (*order, order[0]):
            outcome, status, _ = await _s16b_process(state, entry_id)
            assert outcome == status == "completed", "S16B_LATEST_STATE"
            assert len(_s16b_matching(graph.to_dict())) == len(_s16b_matching(_s16b_reopen(path))) == 1, (
                "S16B_LATEST_STATE"
            )
            after = await snapshot(state)
            assert all(after[k] == before[k] for k in before if k != "outbox_entries"), "S16B_LATEST_STATE"
    await write(state, command(deleted=True, revision=3))
    for entry_id in (*queue, (await _s16b_queue(state))[-1]):
        outcome, status, _ = await _s16b_process(state, entry_id)
        assert outcome == status == "completed" and not _s16b_matching(_s16b_reopen(path)), "S16B_LATEST_STATE"


async def test_outbox_exact_type_removal_keeps_parallel_and_reverse_edges(relations):
    state = relations
    await write(state)
    await write(state, command(deleted=True))
    graph, path = await _s16b_graph(state)
    nodes = {
        ident(i): dict(id=ident(i), type="belief", name="preserved", attributes={"generation": i}) for i in (1, 2, 3)
    }
    edges = [
        dict(source_id=ident(1), target_id=ident(2), relation=k, attributes={"copy": i})
        for i, k in enumerate(("supports", "supports", "supports", "contradicts", "derived_from", "derives", "entity"))
    ]
    edges[2]["attributes"] = {"canonical": True}
    edges += [dict(source_id=ident(2), target_id=ident(1), relation="supports", attributes={"reverse": True})]
    async with state.deps.leases.acquire(context(state)):
        graph.from_dict(dict(nodes=nodes, edges=edges))
    before = _s16b_reopen(path)
    await _s16b_process(state, (await _s16b_queue(state))[0])
    expected = dict(nodes=before["nodes"], edges=[e for e in before["edges"] if e not in _s16b_matching(before)])
    assert graph.to_dict() == _s16b_reopen(path) == expected, "S16B_EXACT_TYPE"
    assert (await sql(state, "SELECT status FROM outbox_entries WHERE operation='relation_sync' ORDER BY rowid"))[
        0
    ] == ("completed",), "S16B_EXACT_TYPE"


async def test_relation_worker_typed_errors_have_sanitized_str(relations, monkeypatch, caplog):
    state = relations
    await write(state)
    queue = await _s16b_queue(state)
    graph, path = await _s16b_graph(state)
    canary = "/private/S16B_CANARY SELECT credential payload reason"

    def broken(*args, **kwargs):
        raise RuntimeError(canary)

    # Existing boundary on BASE too: do not require an absent strict symbol.
    monkeypatch.setattr(os, "replace", broken)
    outcome, status, error = await _s16b_process(state, queue[0])
    allowed = {
        "RELATION_PAYLOAD_INVALID",
        "RELATION_INVARIANT_FAILED",
        "RELATION_GRAPH_UNAVAILABLE",
        "RELATION_PROJECTION_LIMIT",
        "RELATION_PROJECTION_TIMEOUT",
        "RELATION_PERSISTENCE_FAILED",
        "RELATION_SNAPSHOT_INVALID",
        "RELATION_READBACK_FAILED",
        "Relation projection failed",
    }
    assert outcome == status == "pending" and error in allowed, "S16B_SANITIZED_ERROR"
    assert canary not in error + caplog.text and all(r.exc_info is None for r in caplog.records), "S16B_SANITIZED_ERROR"
    assert not path.exists() and not _s16b_matching(graph.to_dict()), "S16B_SANITIZED_ERROR"


async def _s16b_until(predicate):
    async with asyncio.timeout(10):
        while not predicate():
            await asyncio.sleep(0.001)


async def test_strict_graph_capacity_wait_bounded_by_deadline(relations, monkeypatch):
    import threading
    from types import SimpleNamespace

    import memory_server.providers.graph_provider as gp

    state = relations
    await write(state)
    queue = await _s16b_queue(state)
    before = await snapshot(state)
    release = threading.Event()
    async with state.deps.leases.acquire(context(state)) as scope:
        runtime = scope.runtime
        graph = runtime.dependencies.graph
        path = graph._snapshot_path
        handles = [await runtime.submit_native(release.wait, kind="s16b-capacity") for _ in range(16)]
        task = asyncio.create_task(_s16b_process(state, queue[0]))
        try:
            await _s16b_until(
                lambda: task.done() or any(r.state == "WAITING_CAPACITY" for r in runtime._owned_natives.values())
            )
            pending = not task.done()
            monkeypatch.setattr(gp, "time", SimpleNamespace(monotonic=lambda: monotonic() + 120))
            await asyncio.sleep(0.01)
            held = not task.done()
        finally:
            release.set()
            await asyncio.gather(*(h.result() for h in handles))
        result = await task
        await _s16b_until(lambda: not runtime._owned_natives)
        assert pending and held and result == ("pending", "pending", "RELATION_PROJECTION_TIMEOUT"), (
            "S16B_CAPACITY_DEADLINE"
        )
        assert not path.exists() and not graph.to_dict()["edges"], "S16B_CAPACITY_DEADLINE"
        assert not graph._projection_binding.manager._projection_calls, "S16B_CAPACITY_DEADLINE"
    after = await snapshot(state)
    assert all(before[k] == after[k] for k in before if k != "outbox_entries"), "S16B_CAPACITY_DEADLINE"


async def test_strict_graph_deadline_after_mutate_settles_then_timeout(relations, monkeypatch):
    from types import SimpleNamespace

    import memory_server.providers.graph_provider as gp

    state = relations
    await write(state)
    queue = await _s16b_queue(state)
    graph, path = await _s16b_graph(state)
    replay = graph._replay_op
    events = []

    def expire(staging, op):
        replay(staging, op)
        events.append("mutated")
        monkeypatch.setattr(gp, "time", SimpleNamespace(monotonic=lambda: monotonic() + 120))

    monkeypatch.setattr(graph, "_replay_op", expire)
    outcome = await _s16b_process(state, queue[0])
    assert events == ["mutated"] and outcome == ("pending", "pending", "RELATION_PROJECTION_TIMEOUT"), (
        "S16B_MUTATE_SETTLE_TIMEOUT"
    )
    assert len(_s16b_matching(_s16b_reopen(path))) == 1, "S16B_MUTATE_SETTLE_TIMEOUT"
    assert not graph._projection_binding.manager._projection_calls, "S16B_MUTATE_SETTLE_TIMEOUT"


async def test_cancel_after_native_start_retains_custody(relations, monkeypatch):
    import threading

    state = relations
    await write(state)
    queue = await _s16b_queue(state)
    graph, path = await _s16b_graph(state)
    started, release = threading.Event(), threading.Event()
    original = graph._replay_op

    def pause(staging, op):
        started.set()
        assert release.wait(10), "S16B_CANCELLED_CUSTODY"
        return original(staging, op)

    monkeypatch.setattr(graph, "_replay_op", pause)
    async with state.deps.leases.acquire(context(state)) as scope:
        engine_path = scope.runtime.dependencies.outbox_worker._engine.url.database
        runtime = scope.runtime
        task = asyncio.create_task(_s16b_process(state, queue[0]))
        try:
            await _s16b_until(lambda: started.is_set() or task.done())
            task.cancel()
            await asyncio.sleep(0.01)
            task.cancel()
            await asyncio.sleep(0.01)
            retained = started.is_set() and not task.done()
            with sqlite3.connect(engine_path, timeout=0) as competing:
                try:
                    competing.execute("UPDATE relation_revisions SET revision=revision")
                    blocked = False
                except sqlite3.OperationalError:
                    blocked = True
        finally:
            release.set()
        cancelled = False
        try:
            await task
        except asyncio.CancelledError:
            cancelled = True
        await _s16b_until(lambda: not runtime._owned_natives)
        assert retained and blocked and cancelled, "S16B_CANCELLED_CUSTODY"
        assert len(_s16b_matching(_s16b_reopen(path))) == 1, "S16B_CANCELLED_CUSTODY"
        assert await sql(state, "SELECT status,retry_count FROM outbox_entries WHERE operation='relation_sync'") == [
            ("pending", 0)
        ], "S16B_CANCELLED_CUSTODY"
        assert not graph._projection_binding.manager._projection_calls, "S16B_CANCELLED_CUSTODY"


async def test_strict_graph_repeat_and_masked_nodes(relations):
    state = relations
    await write(state)
    entry = (await _s16b_queue(state))[0]
    graph, path = await _s16b_graph(state)
    for _ in range(10):
        assert (await _s16b_process(state, entry))[:2] == ("completed", "completed"), "S16B_IDEMPOTENT_MASKED"
        raw = _s16b_reopen(path)
        assert len(_s16b_matching(raw)) == 1, "S16B_IDEMPOTENT_MASKED"
        assert raw["nodes"] == {
            ident(i): dict(id=ident(i), type="belief", name="[MASKED]", attributes={}) for i in (1, 2)
        }, "S16B_IDEMPOTENT_MASKED"
        assert graph.to_dict() == raw, "S16B_IDEMPOTENT_MASKED"
    await write(state, command(deleted=True))
    for _ in range(10):
        assert (await _s16b_process(state, entry))[0] == "completed", "S16B_IDEMPOTENT_MASKED"
        assert _s16b_reopen(path)["nodes"] == raw["nodes"] and not _s16b_matching(graph.to_dict()), (
            "S16B_IDEMPOTENT_MASKED"
        )


async def test_worker_holds_sqlite_fence_until_native_apply_settles(relations, monkeypatch):
    import threading

    state = relations
    await write(state)
    graph, _ = await _s16b_graph(state)
    started, release = threading.Event(), threading.Event()
    replay = graph._replay_op

    def barrier(staging, op):
        started.set()
        assert release.wait(10), "S16B_FENCE_HELD"
        replay(staging, op)

    monkeypatch.setattr(graph, "_replay_op", barrier)
    async with state.deps.leases.acquire(context(state)) as scope:
        engine = scope.runtime.dependencies.outbox_worker._engine
        task = asyncio.create_task(_s16b_process(state, (await _s16b_queue(state))[0]))
        try:
            await _s16b_until(started.is_set)
            with sqlite3.connect(engine.url.database, timeout=0) as competing:
                try:
                    competing.execute("UPDATE relation_revisions SET revision=revision")
                    blocked = False
                except sqlite3.OperationalError:
                    blocked = True
            assert blocked and not task.done(), "S16B_FENCE_HELD"
        finally:
            release.set()
            await task
        with sqlite3.connect(engine.url.database, timeout=0) as competing:
            competing.execute("UPDATE relation_revisions SET revision=revision")


async def test_relation_worker_cancelled_error_retains_custody(relations, monkeypatch):
    await test_cancel_after_native_start_retains_custody(relations, monkeypatch)


@pytest.mark.parametrize(
    "fault",
    [
        "no_router",
        "no_strict",
        "no_native",
        "memory_only",
        "writer_false",
        "permission",
        "pre_replace",
        "readback_false",
    ],
)
async def test_strict_graph_failure_never_marks_completed(relations, monkeypatch, fault):
    from storage.outbox import OutboxRepository

    state = relations
    await write(state)
    before = await snapshot(state)
    graph, path = await _s16b_graph(state)
    entry_id = (await _s16b_queue(state))[0]
    async with state.deps.leases.acquire(context(state)):
        graph.from_dict(dict(nodes={}, edges=[]))
    disk_before = path.read_bytes()
    completions = []
    completed = OutboxRepository.mark_completed

    async def track(self, entry_id):
        completions.append(entry_id)
        return await completed(self, entry_id)

    def fail(*args, **kwargs):
        raise PermissionError("S16B_FAULT_CANARY /private/path")

    monkeypatch.setattr(OutboxRepository, "mark_completed", track)
    async with state.deps.leases.acquire(context(state)) as scope:
        worker = scope.runtime.dependencies.outbox_worker
        if fault == "no_router":
            monkeypatch.setattr(worker, "_graph_router", None)
        elif fault == "no_strict":
            monkeypatch.setattr(graph, "reconcile_claim_relation", None)
        elif fault == "no_native":
            monkeypatch.setattr(worker, "_native_submitter", None)
        elif fault == "memory_only":
            monkeypatch.setattr(graph, "_snapshot_path", None)
        elif fault == "writer_false":
            monkeypatch.setattr(graph, "_write_relation_snapshot_strict", lambda *a, **kw: False)
        elif fault in ("permission", "pre_replace"):
            if fault == "permission":
                open_fn = os.open

                def denied(name, flags, *args, **kwargs):
                    if str(name).endswith(".tmp"):
                        return fail()
                    return open_fn(name, flags, *args, **kwargs)

                monkeypatch.setattr(os, "open", denied)
            else:
                monkeypatch.setattr(os, "replace", fail)
        elif fault == "readback_false":
            reader = graph._read_relation_snapshot_strict

            def wrong(*args):
                raw = reader(*args)
                return dict(nodes=raw["nodes"], edges=[])

            monkeypatch.setattr(graph, "_read_relation_snapshot_strict", wrong)
        result = await _s16b_process(state, entry_id)
        # Post-replace uncertainty deliberately invalidates the next lease;
        # inspect canonical SQL through this still-held supplied runtime.
        async with worker._session_factory() as db:
            after = {k: (await db.execute(text(f'SELECT * FROM "{k}" ORDER BY rowid'))).all() for k in before}
    assert result[:2] == ("pending", "pending") and not completions, "S16B_STRICT_FAILURE"
    assert all(before[k] == after[k] for k in before if k != "outbox_entries"), "S16B_STRICT_FAILURE"
    assert "S16B_FAULT_CANARY" not in result[2], "S16B_STRICT_FAILURE"
    if fault != "readback_false":
        assert path.read_bytes() == disk_before and not graph.to_dict()["edges"], "S16B_STRICT_FAILURE"


async def test_strict_graph_persisted_readback_and_reopen(relations, monkeypatch):
    from storage.outbox import OutboxRepository

    state = relations
    await write(state)
    graph, path = await _s16b_graph(state)
    completed = OutboxRepository.mark_completed
    observed = []

    async def verify_first(self, entry):
        independent = json.loads(path.read_bytes())
        observed.append(independent)
        assert independent == graph.to_dict() == _s16b_reopen(path), "S16B_DURABLE_READBACK"
        assert len(_s16b_matching(independent)) == 1, "S16B_DURABLE_READBACK"
        return await completed(self, entry)

    monkeypatch.setattr(OutboxRepository, "mark_completed", verify_first)
    assert (await _s16b_process(state, (await _s16b_queue(state))[0]))[0] == "completed" and observed, (
        "S16B_DURABLE_READBACK"
    )


@pytest.mark.parametrize("dimension", ["pair", "nodes", "edges", "bytes"])
async def test_strict_graph_pair_and_snapshot_bounds(relations, monkeypatch, dimension):
    import memory_server.providers.graph_provider as gp

    state = relations
    await write(state)
    graph, path = await _s16b_graph(state)
    nodes = {ident(i): dict(id=ident(i), type="belief", name="kept", attributes={}) for i in (1, 2)}
    edges = []
    if dimension == "pair":
        edges = [dict(source_id=ident(1), target_id=ident(2), relation="unrelated", attributes={}) for _ in range(17)]
    elif dimension == "nodes":
        nodes.update({f"n{i}": dict(id=f"n{i}", type="entity", name="kept", attributes={}) for i in range(9999)})
    elif dimension == "edges":
        nodes["other"] = dict(id="other", type="entity", name="kept", attributes={})
        edges = [dict(source_id="other", target_id="other", relation="unrelated", attributes={}) for _ in range(20001)]
    else:
        nodes[ident(1)]["name"] = "x" * 16777217
    async with state.deps.leases.acquire(context(state)):
        graph.from_dict(dict(nodes=nodes, edges=edges))
    before = path.read_bytes()
    memory = graph.to_dict()
    builds = []
    builder = graph._build_state_from

    def tracked(raw):
        builds.append(True)
        return builder(raw)

    monkeypatch.setattr(graph, "_build_state_from", tracked)
    result = await _s16b_process(state, (await _s16b_queue(state))[0])
    assert result == ("pending", "pending", "RELATION_PROJECTION_LIMIT"), "S16B_BOUND_PRECOPY"
    assert not builds and path.read_bytes() == before and graph.to_dict() == memory, "S16B_BOUND_PRECOPY"
    assert gp._MAX_SNAPSHOT_BYTES == 256 * 1024 * 1024, "S16B_BOUND_PRECOPY"


@pytest.mark.parametrize(
    "fault", ["bool_v", "bool_revision", "event", "future", "extra", "binding", "missing_endpoint"]
)
async def test_relation_worker_binding_invariants_and_sql_budget(relations, monkeypatch, fault):
    from storage.outbox import OutboxEntry, OutboxEntryORM, OutboxRepository

    state = relations
    await write(state)
    entry_id = (await _s16b_queue(state))[0]
    graph, path = await _s16b_graph(state)
    statements = []
    async with state.deps.leases.acquire(context(state)) as scope:
        worker = scope.runtime.dependencies.outbox_worker
        if fault == "missing_endpoint":
            await sql(state, "DELETE FROM beliefs WHERE id=:id", dict(id=ident(2)))
        async with worker._session_factory() as db:
            row = await db.get(OutboxEntryORM, entry_id)
            entry = OutboxEntry.from_orm(row)
            p = dict(entry.payload)
            if fault == "bool_v":
                p["v"] = True
            elif fault == "bool_revision":
                p["revision"] = True
            elif fault == "event":
                p["event_id"] = str(uuid4())
            elif fault == "future":
                p["revision"] = 999
            elif fault == "extra":
                p["reason"] = "forbidden"
            elif fault == "binding":
                entry.record_id = ident(3)
            entry.payload_json = json.dumps(p)
            conn = await db.connection()

            def trace(conn, cursor, statement, *args):
                statements.append(statement)

            event.listen(conn.sync_connection, "before_cursor_execute", trace)
            try:
                result = await worker._process_entry(db, OutboxRepository(db), entry)
                await db.commit()
            finally:
                event.remove(conn.sync_connection, "before_cursor_execute", trace)
    assert result == "pending" and not path.exists() and not graph.to_dict()["edges"], "S16B_BINDING_BUDGET"
    assert len(statements) <= 64, "S16B_BINDING_BUDGET"
    if fault in ("event", "future", "missing_endpoint"):
        assert statements[0].startswith("UPDATE relation_revisions SET revision=revision"), "S16B_BINDING_BUDGET"


async def test_strict_graph_exact_intent_replay_preserves_neighbours(relations):
    from memory_server.providers.graph_provider import SimpleGraph

    state = relations
    await write(state)
    graph, path = await _s16b_graph(state)
    nodes = {ident(i): dict(id=ident(i), type="belief", name="kept", attributes={"v": i}) for i in (1, 2)}
    edges = [dict(source_id=ident(2), target_id=ident(1), relation="contradicts", attributes={"generation": 2})]
    async with state.deps.leases.acquire(context(state)):
        graph.from_dict(dict(nodes=nodes, edges=edges))
    # The new replay branch must be pure and use only exact directed/type intent.
    staging = SimpleGraph()
    staging.from_dict(dict(nodes=nodes, edges=edges))
    for live in (True, True, False, False):
        graph._replay_op(
            staging, ("reconcile_claim_relation", ident(1), ident(2), "supports", live, "belief", "belief")
        )
        assert len(_s16b_matching(staging.to_dict())) == int(live), "S16B_INTENT_REPLAY"
        assert [e for e in staging.to_dict()["edges"] if e not in _s16b_matching(staging.to_dict())] == edges, (
            "S16B_INTENT_REPLAY"
        )
        assert staging.to_dict()["nodes"] == nodes, "S16B_INTENT_REPLAY"
    assert (await _s16b_process(state, (await _s16b_queue(state))[0]))[0] == "completed", "S16B_INTENT_REPLAY"
    assert _s16b_reopen(path)["nodes"] == nodes and edges[0] in _s16b_reopen(path)["edges"], "S16B_INTENT_REPLAY"


async def test_relation_b_counts_and_frozen_boundaries(relations):
    state = relations
    marker = "S16B_SCOPE_COUNTS"
    assert (len(state.specs), len(state.mutation_specs), len(state.specs) + len(state.mutation_specs)) == (24, 6, 30), (
        marker
    )
    assert len(state.app.routes) == 36 and not {"relations.create", "relations.delete"} & set(state.deps.ports), marker
    base = "2896fae938feb904cdb70f7461a1b9fcc26cf6f5"
    paths = {
        "storage/outbox_worker.py": (
            "_blocking",
            "_provider_write_error",
            "_process_index_fact",
            "_process_index_decision",
            "_process_index_skill",
            "_process_index_belief",
            "_poll_once",
        ),
        "src/memory_server/providers/graph_provider.py": (
            "get_edge",
            "delete_edge",
            "_persist_snapshot",
            "_write_snapshot",
            "_managed_persist",
            "_snapshot_publication",
            "_read_state_file",
            "load_snapshot",
        ),
    }
    for path, names in paths.items():
        original = subprocess.check_output(["git", "show", f"{base}:{path}"], cwd=ROOT, text=True)
        current = (ROOT / path).read_text()
        for name in names:

            def source(value):
                node = next(
                    n
                    for n in ast.walk(ast.parse(value))
                    if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name
                )
                return "".join(value.splitlines(keepends=True)[node.lineno - 1 : node.end_lineno])

            assert source(original) == source(current), (marker, path, name)
    worker = (ROOT / "storage/outbox_worker.py").read_text()
    frozen = subprocess.check_output(
        ["git", "show", f"{base}:storage/outbox_worker.py"], cwd=ROOT, text=True
    ).splitlines(keepends=True)[428:450]
    assert "".join(frozen) in worker, marker


async def _s16b_receipt_rounds(state, monkeypatch, *, live, rounds):
    from storage.outbox import OutboxRepository

    import memory_server.providers.graph_provider as gp

    await write(state)
    if not live:
        await write(state, command(deleted=True))
    entry = (await _s16b_queue(state))[0]
    graph, path = await _s16b_graph(state)
    binding = graph._projection_binding
    manager = binding.manager
    trace, receipts = [], []
    mint = gp._SnapshotPublication._mint
    refresh = manager.refresh_projection_identity
    completed = OutboxRepository.mark_completed
    verify = graph.verify_claim_relation
    original_publish = graph.reconcile_claim_relation

    def issue(publication):
        result = mint(publication)
        receipts.append(publication.receipt)
        trace.append("mint")
        return result

    def consume(descriptor, *, receipt, proof=None):
        trace.append("refresh")
        result = refresh(descriptor, receipt=receipt, proof=proof)
        trace.append("consume")
        return result

    def published(*args, **kwargs):
        original_publish(*args, **kwargs)
        trace.append("native_return")

    def readback(*args, **kwargs):
        verify(*args, **kwargs)
        trace.append("post_refresh_readback")

    async def complete(self, entry_id):
        trace.append("mark_completed")
        return await completed(self, entry_id)

    monkeypatch.setattr(gp._SnapshotPublication, "_mint", issue)
    monkeypatch.setattr(manager, "refresh_projection_identity", consume)
    monkeypatch.setattr(graph, "reconcile_claim_relation", published)
    monkeypatch.setattr(graph, "verify_claim_relation", readback)
    monkeypatch.setattr(OutboxRepository, "mark_completed", complete)
    marker = "S16B_STRICT_RECEIPT_NEXT_LEASE"
    for i in range(rounds):
        trace.clear()
        result = await _s16b_process(state, entry)
        assert result[:2] == ("completed", "completed"), marker
        assert trace == ["mint", "native_return", "refresh", "consume", "post_refresh_readback", "mark_completed"], (
            marker
        )
        assert len(receipts) == i + 1 and receipts[-1].snapshot.closed and receipts[-1].lock.closed, marker
        assert not manager._projection_calls, marker
        raw = _s16b_reopen(path)
        assert raw == graph.to_dict() and len(_s16b_matching(raw)) == int(live), marker
        async with state.deps.leases.acquire(context(state)) as scope:
            assert scope.runtime.dependencies.graph is graph, marker
        evidence = dict(binding.state[0])
        with pytest.raises(Exception):
            refresh(binding.descriptor, receipt=receipts[-1], proof=receipts[-1].proof)
        assert binding.state[0] == evidence, "S16B_REPEAT_RECEIPT_IDEMPOTENT"
    assert len({id(r) for r in receipts}) == rounds, "S16B_REPEAT_RECEIPT_IDEMPOTENT"


@pytest.mark.parametrize("live", [False, True])
async def test_relation_strict_publication_receipt_refresh_next_lease(relations, monkeypatch, live):
    await _s16b_receipt_rounds(relations, monkeypatch, live=live, rounds=1)


@pytest.mark.parametrize("live", [False, True])
async def test_relation_strict_repeat_consumes_fresh_receipt(relations, monkeypatch, live):
    await _s16b_receipt_rounds(relations, monkeypatch, live=live, rounds=10)


@pytest.mark.parametrize("fault", ["refresh", "post_refresh_readback"])
async def test_relation_refresh_failure_never_marks_completed(relations, monkeypatch, fault):
    from storage.outbox import OutboxRepository

    import memory_server.providers.graph_provider as gp

    state = relations
    await write(state)
    entry = (await _s16b_queue(state))[0]
    graph, path = await _s16b_graph(state)
    binding = graph._projection_binding
    manager = binding.manager
    before = dict(binding.state[0])
    receipts, completions = [], []
    mint = gp._SnapshotPublication._mint

    def issue(publication):
        result = mint(publication)
        receipts.append(publication.receipt)
        return result

    def refuse(*args, **kwargs):
        raise gp.SimpleGraph.StrictRelationProjectionError("RELATION_READBACK_FAILED") from None

    async def forbidden(self, entry_id):
        completions.append(entry_id)

    monkeypatch.setattr(gp._SnapshotPublication, "_mint", issue)
    monkeypatch.setattr(OutboxRepository, "mark_completed", forbidden)
    if fault == "refresh":
        monkeypatch.setattr(manager, "refresh_projection_identity", refuse)
    else:
        monkeypatch.setattr(graph, "verify_claim_relation", refuse)
    result = await _s16b_process(state, entry)
    marker = "S16B_REFRESH_NO_COMPLETION"
    assert result[:2] == ("pending", "pending") and not completions, marker
    assert len(receipts) == 1 and receipts[0].snapshot.closed and receipts[0].lock.closed, marker
    assert not manager._projection_calls and len(_s16b_matching(_s16b_reopen(path))) == 1, marker
    if fault == "refresh":
        assert binding.state[0] == before, marker
        with pytest.raises(Exception):
            async with state.deps.leases.acquire(context(state)):
                pytest.fail(marker)
    else:
        assert binding.state[0] != before, marker
        async with state.deps.leases.acquire(context(state)):
            pass
