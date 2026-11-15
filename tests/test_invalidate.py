"""Synthetic SQL readback for lifecycle-bound invalidation."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from storage.models.relation import ClaimRelationORM
from storage.repositories import BeliefRepository, FactRepository
from storage.repositories.lifecycle_repo import LifecycleConflictError
from storage.repositories.relation_repo import RelationRepository
from storage.repositories.relation_revision_repo import RelationRevisionRepository

from memory_server.api.invalidate import InvalidateError, invalidate
from memory_server.models import Belief, Fact
from memory_server.providers.sqlite_provider import SQLiteProvider

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def database(tmp_path):
    provider = SQLiteProvider(f"sqlite+aiosqlite:///{tmp_path / 'invalidate.sqlite'}")
    await provider.initialize()
    try:
        yield provider
    finally:
        await provider.close()


async def seed(provider, kind="fact", ident="old", state="active"):
    async with await provider._get_session() as session:
        if kind == "fact":
            await FactRepository(session).create(Fact(
                id=ident, subject=ident, predicate="is", object="synthetic",
                source="test", lifecycle_state=state,
            ))
        else:
            await BeliefRepository(session).create(Belief(
                id=ident, proposition=ident, lifecycle_state=state,
            ))
        await session.commit()


async def snapshot(provider):
    async with await provider._get_session() as session:
        return {t: (await session.execute(text(f'SELECT * FROM "{t}" ORDER BY rowid'))).all()
                for t in ("facts", "beliefs", "lifecycle_states", "lifecycle_events",
                          "claim_relations", "relation_revisions", "receipts", "outbox_entries")}


@pytest.mark.parametrize("kind", ["fact", "belief"])
async def test_invalidate_exact_source_envelope(database, kind):
    await seed(database, kind)
    result = await invalidate(database, "old", kind, reason="  obsolete  ")
    assert set(result) == {"memory", "event", "propagated", "superseded_by"}
    assert result["memory"]["id"] == "old"
    assert result["memory"]["lifecycle_state"] == "discarded"
    assert result["memory"]["version"] == 2
    assert result["event"] == dict(memory_id="old", memory_type=kind,
                                   from_state="active", to_state="discarded",
                                   reason="obsolete", triggered_by="agent")
    assert result["propagated"] == [] and result["superseded_by"] is None
    state = await snapshot(database)
    assert len(state["lifecycle_events"]) == len(state["lifecycle_states"]) == 1


@pytest.mark.parametrize("kind", ["fact", "belief"])
@pytest.mark.parametrize("case,message", [
    ("type", "memory_type must be one of: fact, belief"),
    ("target", "to_state must be one of: discarded, superseded, stale"),
    ("reason", "reason must be non-blank"),
    ("required", "superseded_by is required when to_state='superseded'"),
    ("self", "a memory cannot supersede itself"),
    ("missing", "not found"), ("inactive", "is not active"),
    ("wrong_type", "not found"), ("cross_store", "not found"),
])
async def test_invalidate_validation_before_writes(database, tmp_path, kind, case, message):
    await seed(database, kind)
    kwargs = dict(memory_type=kind, reason="why", to_state="superseded", superseded_by="new")
    if case == "type":
        kwargs.update(memory_type="skill", to_state="contradicted", reason=" ")
    elif case == "target":
        kwargs.update(to_state="contradicted", reason=" ")
    elif case == "reason":
        kwargs.update(reason=" \t", superseded_by="")
    elif case == "required":
        kwargs.update(superseded_by="")
    elif case == "self":
        kwargs.update(superseded_by="old")
    elif case == "inactive":
        await seed(database, kind, "new", "stale")
    elif case == "wrong_type":
        await seed(database, "belief" if kind == "fact" else "fact", "new")
    elif case == "cross_store":
        other = SQLiteProvider(f"sqlite+aiosqlite:///{tmp_path / 'other.sqlite'}")
        await other.initialize()
        try:
            await seed(other, kind, "new")
        finally:
            await other.close()
    before = await snapshot(database)
    with pytest.raises(InvalidateError) as error:
        await invalidate(database, "old", **kwargs)
    assert message in str(error.value)
    assert await snapshot(database) == before


@pytest.mark.parametrize("kind", ["fact", "belief"])
async def test_successor_revision_uses_persisted_lifecycle_event(database, monkeypatch, kind):
    await seed(database, kind)
    await seed(database, kind, "new")
    monkeypatch.setattr(RelationRepository, "create", AsyncMock(side_effect=AssertionError("unversioned")))
    result = await invalidate(database, "old", f" {kind.upper()} ", reason="new evidence",
                              to_state="superseded", superseded_by="new", expected_version="1")
    assert result["superseded_by"] == "new"
    async with await database._get_session() as session:
        row = (await session.execute(text(
            "SELECT e.id,e.timestamp,r.event_id,r.changed_at FROM lifecycle_events e "
            "JOIN relation_revisions r ON e.id=r.event_id WHERE e.memory_id='old'"
        ))).one()
        assert row.id == row.event_id and row.timestamp == row.changed_at
        assert await session.get(ClaimRelationORM, ("old", "new", "superseded_by")) is not None
        assert row.id not in ("old", "new")


@pytest.mark.parametrize("version", ["99", "nonsense", ""])
async def test_invalidate_cas_conflict_rolls_back_everything(database, version):
    await seed(database)
    await seed(database, ident="new")
    before = await snapshot(database)
    with pytest.raises((LifecycleConflictError, ValueError)):
        await invalidate(database, "old", reason="why", superseded_by="new", expected_version=version)
    assert await snapshot(database) == before


async def test_losing_cas_rolls_back_everything(database, monkeypatch):
    from storage.repositories import lifecycle_repo

    await seed(database)
    before = await snapshot(database)
    original = AsyncSession.execute

    async def lose(session, statement, *args, **kwargs):
        result = await original(session, statement, *args, **kwargs)
        if getattr(statement, "is_update", False) and statement.table.name == "facts":
            return SimpleNamespace(rowcount=0)
        return result

    monkeypatch.setattr(AsyncSession, "execute", lose)
    with pytest.raises(lifecycle_repo.LifecycleConflictError):
        await invalidate(database, "old", reason="why")
    assert await snapshot(database) == before


@pytest.mark.parametrize("fault", ["append", "check", "unique", "commit", "propagation"])
async def test_successor_failure_rolls_back_cas_event_and_propagation(database, monkeypatch, fault):
    from memory_server.services.lifecycle_service import LifecycleService

    await seed(database)
    await seed(database, ident="new")
    await seed(database, ident="dependent")
    async with await database._get_session() as session:
        session.add(ClaimRelationORM(source_id="dependent", target_id="old", relation_type="derived_from"))
        await session.commit()
    before = await snapshot(database)
    append = RelationRevisionRepository.append

    async def fail_append(repo, key, **kwargs):
        if fault == "append":
            raise RuntimeError("append injected")
        result = await append(repo, key, **kwargs)
        # Exercise the actual SQL CHECK/UNIQUE constraints, not a validator
        # rejection before reaching those constraints.
        relation_type = "'unknown'" if fault == "check" else "relation_type"
        revision = "revision" if fault == "check" else "revision+1"
        await repo.session.execute(text(
            "INSERT INTO relation_revisions "
            "SELECT source_id,target_id," + relation_type + "," + revision + ",deleted,"
            "relation_created_at,changed_at,event_id,request_id,actor,action_receipt_id "
            "FROM relation_revisions"
        ))
        return result

    if fault == "commit":
        monkeypatch.setattr(AsyncSession, "commit", AsyncMock(side_effect=RuntimeError("commit injected")))
    elif fault == "propagation":
        monkeypatch.setattr(LifecycleService, "_propagate_dependents",
                            AsyncMock(side_effect=RuntimeError("propagation injected")))
    else:
        monkeypatch.setattr(RelationRevisionRepository, "append", fail_append)
    with pytest.raises(Exception) as failure:
        await invalidate(database, "old", reason="why", to_state="superseded", superseded_by="new")
    if fault == "check":
        assert "CHECK constraint failed: s16_relation_vocabulary" in str(failure.value)
    elif fault == "unique":
        assert "UNIQUE constraint failed: relation_revisions.event_id" in str(failure.value)
    assert await snapshot(database) == before


async def test_legacy_superseded_by_no_synthetic_backfill(database):
    from memory_server.api.search import search

    await seed(database)
    await seed(database, ident="new")
    async with await database._get_session() as session:
        session.add(ClaimRelationORM(source_id="old", target_id="new", relation_type="superseded_by"))
        await session.commit()
    before = await snapshot(database)
    results = await search(database, include_inactive=True)
    assert next(f for f in results["results"] if f["id"] == "old")["superseded_by"] == "new"
    assert await snapshot(database) == before
    await invalidate(database, "old", reason="why", to_state="superseded", superseded_by="new")
    after = await snapshot(database)
    assert after["claim_relations"] == before["claim_relations"]
    assert after["relation_revisions"] == []
    assert len(after["lifecycle_events"]) == 1


@pytest.mark.parametrize("target", ["discarded", "superseded", "stale"])
async def test_propagation_preserved_and_stale_exempt(database, target):
    await seed(database)
    await seed(database, ident="new")
    await seed(database, ident="dependent")
    async with await database._get_session() as session:
        session.add(ClaimRelationORM(source_id="dependent", target_id="old", relation_type="derived_from"))
        await session.commit()
    result = await invalidate(database, "old", reason="why", to_state=target,
                              superseded_by="new" if target == "superseded" else "")
    assert len(result["propagated"]) == (0 if target == "stale" else 1)
    async with await database._get_session() as session:
        dependent = await FactRepository(session).get("dependent")
        assert dependent.confidence == (1.0 if target == "stale" else 0.8)
        events = (await session.execute(text("SELECT id FROM lifecycle_events"))).scalars().all()
        assert len(events) == len(set(events)) == (1 if target == "stale" else 2)


async def test_invalidate_native_mcp_parity_and_defaults(database, monkeypatch, synthetic_storage_env):
    from memory_server.plugins.hermes import provider as native
    from memory_server.transports import mcp_stdio as mcp

    await seed(database, ident="native")
    await seed(database, ident="mcp")
    plugin = native.HermesProvider()
    monkeypatch.setattr(plugin, "_require_provider", lambda: database)
    monkeypatch.setattr(native, "_get_graph", AsyncMock(return_value=None))
    monkeypatch.setattr(mcp, "_get_provider", AsyncMock(return_value=database))
    monkeypatch.setattr(mcp, "_get_graph_router", AsyncMock(return_value=SimpleNamespace(graph=None)))
    runtime = SimpleNamespace(admit=lambda: "token", release_admitted=lambda token: None)
    monkeypatch.setattr(mcp, "_runtime_or_raise", lambda: SimpleNamespace(runtime=runtime))
    a = json.loads(await plugin._get_tool_handler("invalidate")(memory_id="native", reason="why"))
    b = json.loads(await mcp.invalidate_tool("mcp", "fact", "why"))
    assert set(a) == set(b) == {"memory", "event", "propagated", "superseded_by"}
    assert a["event"]["triggered_by"] == "agent" and b["event"]["triggered_by"] == "system"
    assert a["event"]["to_state"] == b["event"]["to_state"] == "discarded"
    schema = next(s for s in plugin.get_tool_schemas() if s["name"] == "invalidate")["parameters"]
    assert schema["required"] == ["memory_id", "reason"]
    assert schema["properties"]["to_state"]["enum"] == ["discarded", "superseded", "stale"]
    tools = {t.name: t for t in await mcp.mcp.list_tools()}
    assert tools["invalidate"].inputSchema["required"] == ["memory_id", "memory_type", "reason"]
    assert tools["invalidate"].inputSchema["properties"]["to_state"]["default"] == "discarded"
