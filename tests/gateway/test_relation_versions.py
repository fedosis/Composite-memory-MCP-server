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
