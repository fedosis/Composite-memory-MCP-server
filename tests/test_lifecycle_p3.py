"""Tests for Phase 3 lifecycle service and tool integration."""

from __future__ import annotations

import asyncio
import json

import pytest
from sqlalchemy import text
from storage.repositories import LifecycleRepository
from storage.repositories.belief_repo import BeliefRepository

import memory_server.server as server_module
from memory_server.models import Belief, Fact
from memory_server.providers.graph_provider import SimpleGraph
from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.services.lifecycle_service import LifecycleService


@pytest.fixture
async def provider():
    p = SQLiteProvider(url="sqlite+aiosqlite:///:memory:")
    await p.initialize()
    yield p
    await p.close()


@pytest.mark.asyncio
class TestLifecycleServiceP3:
    @pytest.mark.parametrize(
        "initial_state,target_state",
        [
            ("superseded", "stale"),
            ("contradicted", "stale"),
            ("discarded", "archived"),
        ],
    )
    async def test_belief_belief_matrix_transitions_are_allowed(
        self, provider, initial_state, target_state
    ):
        belief = Belief(
            proposition=f"{initial_state} -> {target_state}",
            lifecycle_state=initial_state,
        )
        await provider.create_belief(belief)

        service = LifecycleService(provider)
        result = await service.transition(
            memory_id=belief.id,
            memory_type="belief",
            to_state=target_state,
            reason="matrix transition",
            expected_version=1,
        )

        assert result.from_state == initial_state
        assert result.to_state == target_state
        assert result.memory.lifecycle_state == target_state
        assert result.memory.version == 2

    async def test_transition_writes_event_and_bumps_version(self, provider):
        fact = Fact(id="fact-1", subject="Docker", predicate="runs_on", object="OMV8")
        await provider.create_fact(fact)

        service = LifecycleService(provider)
        result = await service.transition(
            memory_id="fact-1",
            memory_type="fact",
            to_state="stale",
            reason="Decay threshold reached",
            expected_version=1,
        )

        assert result.memory.lifecycle_state == "stale"
        assert result.memory.version == 2

        async with await provider._get_session() as session:
            repo = LifecycleRepository(session)
            events = await repo.get_events("fact-1")

        assert len(events) == 1
        assert events[0]["from_state"] == "active"
        assert events[0]["to_state"] == "stale"
        assert events[0]["reason"] == "Decay threshold reached"

    async def test_invalid_transition_backwards_raises(self, provider):
        fact = Fact(id="fact-2", subject="Caddy", predicate="serves", object="HTTPS")
        await provider.create_fact(fact)

        service = LifecycleService(provider)
        await service.transition(
            memory_id="fact-2",
            memory_type="fact",
            to_state="stale",
            reason="Decay threshold reached",
            expected_version=1,
        )

        with pytest.raises(ValueError, match="Invalid lifecycle transition"):
            await service.transition(
                memory_id="fact-2",
                memory_type="fact",
                to_state="validated",
                reason="backwards",
                expected_version=2,
            )

    async def test_expected_version_mismatch_raises(self, provider):
        fact = Fact(id="fact-3", subject="Nginx", predicate="serves", object="TLS")
        await provider.create_fact(fact)

        service = LifecycleService(provider)
        with pytest.raises(ValueError, match="expected_version mismatch"):
            await service.transition(
                memory_id="fact-3",
                memory_type="fact",
                to_state="stale",
                reason="race",
                expected_version=99,
            )

    @pytest.mark.parametrize("memory_type", ["fact", "belief"])
    async def test_event_failure_rolls_back_state_and_version(self, provider, memory_type):
        if memory_type == "fact":
            memory = Fact(id="fact-event-rollback", subject="Rollback", predicate="has", object="event")
            await provider.create_fact(memory)
        else:
            memory = Belief(id="belief-event-rollback", proposition="Rollback event")
            await provider.create_belief(memory)
        async with await provider._get_session() as session:
            await session.execute(text(
                "CREATE TRIGGER reject_lifecycle_event BEFORE INSERT ON lifecycle_events "
                "BEGIN SELECT RAISE(ABORT, 'event rejected'); END"
            ))
            await session.commit()

        with pytest.raises(Exception, match="event rejected"):
            await LifecycleService(provider).transition(
                memory.id, memory_type, "stale", expected_version=1
            )

        current = await provider.get_fact(memory.id) if memory_type == "fact" else await provider.get_belief(memory.id)
        assert current is not None
        assert current.lifecycle_state == "active"
        assert current.version == 1
        async with await provider._get_session() as session:
            events = await LifecycleRepository(session).get_events(memory.id)
        assert events == []

    @pytest.mark.parametrize("memory_type", ["fact", "belief"])
    async def test_file_sqlite_concurrent_same_version_has_one_winner(self, tmp_path, memory_type):
        db_url = f"sqlite+aiosqlite:///{tmp_path / 'cas.db'}"
        first = SQLiteProvider(url=db_url)
        second = SQLiteProvider(url=db_url)
        await first.initialize()
        await second.initialize()
        try:
            if memory_type == "fact":
                memory = Fact(id="cas-fact", subject="CAS", predicate="is", object="atomic")
                await first.create_fact(memory)
            else:
                memory = Belief(id="cas-belief", proposition="CAS is atomic")
                await first.create_belief(memory)

            barrier = asyncio.Barrier(2)
            reads = 0
            connections = []

            class BarrierLifecycleService(LifecycleService):
                async def _load_memory(self, *args, **kwargs):
                    nonlocal reads
                    result = await super()._load_memory(*args, **kwargs)
                    reads += 1
                    connection = await args[0].connection()
                    connections.append(connection.sync_connection.connection.dbapi_connection)
                    await asyncio.wait_for(barrier.wait(), timeout=5)
                    return result

            services = [BarrierLifecycleService(first), BarrierLifecycleService(second)]

            async def run(service):
                return await service.transition(
                    memory.id, memory_type, "stale", expected_version=1
                )

            results = await asyncio.wait_for(
                asyncio.gather(run(services[0]), run(services[1]), return_exceptions=True), timeout=10
            )
            assert reads == 2
            assert connections[0] is not connections[1]
            assert sum(not isinstance(result, Exception) for result in results) == 1
            conflicts = [result for result in results if isinstance(result, ValueError)]
            assert len(conflicts) == 1
            assert "expected_version mismatch" in str(conflicts[0])
            current = (
                await first.get_fact(memory.id) if memory_type == "fact" else await first.get_belief(memory.id)
            )
            assert current is not None
            assert current.version == 2
            assert current.lifecycle_state == "stale"
            async with await first._get_session() as session:
                events = await LifecycleRepository(session).get_events(memory.id)
            assert len(events) == 1
        finally:
            await first.close()
            await second.close()

    async def test_legacy_fact_expected_version_semver_string_is_accepted(self, provider):
        fact = Fact(
            id="fact-legacy",
            subject="Legacy",
            predicate="uses",
            object="Version",
            version="0.1.0",
        )
        await provider.create_fact(fact)

        service = LifecycleService(provider)
        result = await service.transition(
            memory_id="fact-legacy",
            memory_type="fact",
            to_state="stale",
            reason="legacy compatibility",
            expected_version="0.1.0",
        )

        assert result.memory.lifecycle_state == "stale"
        assert result.memory.version == 2

    @pytest.mark.parametrize("memory_type", ["fact", "belief"])
    async def test_version_changed_after_read_without_state_change_conflicts(self, tmp_path, memory_type):
        p = SQLiteProvider(url=f"sqlite+aiosqlite:///{tmp_path / 'interleaved.db'}")
        await p.initialize()
        try:
            if memory_type == "fact":
                memory = Fact(id="version-only-fact", subject="CAS", predicate="guards", object="version-only writer")
                await p.create_fact(memory)
            else:
                memory = Belief(proposition="CAS guards version-only writer")
                await p.create_belief(memory)

            class InterleavedService(LifecycleService):
                async def _load_memory(self, *args, **kwargs):
                    result = await super()._load_memory(*args, **kwargs)
                    async with await p._get_session() as other:
                        table = "facts" if memory_type == "fact" else "beliefs"
                        await other.execute(text(f"UPDATE {table} SET version = 2 WHERE id = :id"), {"id": memory.id})
                        await other.commit()
                    return result

            with pytest.raises(ValueError, match="expected_version mismatch"):
                await InterleavedService(p).transition(memory.id, memory_type, "stale", expected_version=1)
            current = await p.get_fact(memory.id) if memory_type == "fact" else await p.get_belief(memory.id)
            assert current is not None
            assert (current.lifecycle_state, current.version) == ("active", 2)
            async with await p._get_session() as session:
                assert await LifecycleRepository(session).get_events(memory.id) == []
        finally:
            await p.close()

    @pytest.mark.parametrize("stored_version, expected_version", [("0.1.0", "0.1.0"), (" 1 ", " 1 "), ("0", 0)])
    async def test_legacy_fact_versions_are_cas_matched_as_raw_sql(self, provider, stored_version, expected_version):
        fact = Fact(
            id=f"fact-legacy-{stored_version.strip().replace('.', '-')}",
            subject="Legacy raw",
            predicate="has",
            object=stored_version,
        )
        await provider.create_fact(fact)
        async with await provider._get_session() as session:
            await session.execute(
                text("UPDATE facts SET version = :version WHERE id = :id"),
                {"version": stored_version, "id": fact.id},
            )
            await session.commit()

        result = await LifecycleService(provider).transition(
            fact.id, "fact", "stale", expected_version=expected_version
        )
        assert result.memory.version == 2

    async def test_invalid_memory_type_raises(self, provider):
        service = LifecycleService(provider)
        with pytest.raises(ValueError, match="memory_type must be one of"):
            await service.transition(
                memory_id="missing",
                memory_type="foo",
                to_state="stale",
                reason="invalid type",
            )

    async def test_invalidate_demotes_dependent_confidence(self, provider, monkeypatch):
        parent = Fact(
            id="fact-parent",
            subject="Source",
            predicate="supports",
            object="Claim",
            confidence=1.0,
        )
        child = Fact(
            id="fact-child",
            subject="Derived",
            predicate="depends_on",
            object="Parent",
            confidence=0.5,
        )
        await provider.create_fact(parent)
        await provider.create_fact(child)

        graph = SimpleGraph()
        graph.add_node(
            id=parent.id,
            type="fact",
            name="parent",
            attributes={},
        )
        graph.add_node(
            id=child.id,
            type="fact",
            name="child",
            attributes={},
        )
        graph.add_edge(
            source_id=child.id,
            target_id=parent.id,
            relation="derived_from",
        )

        monkeypatch.setattr(server_module, "_provider", provider)
        monkeypatch.setattr(server_module, "_graph", graph)
        monkeypatch.setattr(server_module, "_graph_router", None)

        payload = json.loads(
            await server_module.invalidate_tool(
                memory_id=parent.id,
                memory_type="fact",
                reason="parent invalidated",
            )
        )

        assert payload["memory"]["lifecycle_state"] == "discarded"
        assert payload["memory"]["version"] == 2
        assert payload["propagated"]
        assert payload["propagated"][0]["memory_id"] == child.id
        assert payload["propagated"][0]["reason"] == "parent_invalidated"

        updated_child = await provider.get_fact(child.id)
        assert updated_child is not None
        assert updated_child.lifecycle_state == "active"
        assert updated_child.confidence == pytest.approx(0.4)

        async with await provider._get_session() as session:
            repo = LifecycleRepository(session)
            child_events = await repo.get_events(child.id)

        assert len(child_events) == 1
        assert child_events[0]["from_state"] == "active"
        assert child_events[0]["to_state"] == "active"
        assert child_events[0]["reason"] == "parent_invalidated"

    async def test_set_belief_replace_writes_lifecycle_event(self, provider, monkeypatch):
        old = Belief(proposition="Old policy", confidence=0.6)
        await provider.create_belief(old)

        graph = SimpleGraph()
        monkeypatch.setattr(server_module, "_provider", provider)
        monkeypatch.setattr(server_module, "_graph", graph)
        monkeypatch.setattr(server_module, "_graph_router", None)

        payload = json.loads(
            await server_module.set_belief_tool(
                proposition="New policy",
                confidence=0.9,
                replace_belief_id=old.id,
            )
        )

        assert payload["superseded"]["id"] == old.id
        superseded = await provider.get_belief(old.id)
        assert superseded is not None
        assert superseded.lifecycle_state == "superseded"
        assert superseded.version == 2

        async with await provider._get_session() as session:
            repo = LifecycleRepository(session)
            events = await repo.get_events(old.id)

        assert len(events) == 1
        assert events[0]["from_state"] == "active"
        assert events[0]["to_state"] == "superseded"
        assert events[0]["reason"] == f"Replaced by {payload['belief']['id']}"

    async def test_set_belief_replace_rolls_back_when_creation_fails(
        self, provider, monkeypatch
    ):
        old = Belief(proposition="Rollback policy", confidence=0.6)
        await provider.create_belief(old)

        async def boom(*args, **kwargs):
            raise RuntimeError("boom")

        graph = SimpleGraph()
        monkeypatch.setattr(server_module, "_provider", provider)
        monkeypatch.setattr(server_module, "_graph", graph)
        monkeypatch.setattr(server_module, "_graph_router", None)
        monkeypatch.setattr(provider, "create_in_transaction", boom)

        with pytest.raises(RuntimeError, match="boom"):
            await server_module.set_belief_tool(
                proposition="New rollback policy",
                confidence=0.9,
                replace_belief_id=old.id,
            )

        reloaded = await provider.get_belief(old.id)
        assert reloaded is not None
        assert reloaded.lifecycle_state == "active"
        assert reloaded.version == 1

    async def test_resolve_conflict_merge_rolls_back_when_creation_fails(
        self, provider, monkeypatch
    ):
        belief_a = Belief(proposition="A policy", confidence=0.8)
        belief_b = Belief(proposition="B policy", confidence=0.6)
        await provider.create_belief(belief_a)
        await provider.create_belief(belief_b)

        async def boom(*args, **kwargs):
            raise RuntimeError("boom")

        graph = SimpleGraph()
        monkeypatch.setattr(server_module, "_provider", provider)
        monkeypatch.setattr(server_module, "_graph", graph)
        monkeypatch.setattr(server_module, "_graph_router", None)
        monkeypatch.setattr(provider, "create_in_transaction", boom)

        with pytest.raises(RuntimeError, match="boom"):
            await server_module.resolve_conflict_tool(
                belief_a_id=belief_a.id,
                belief_b_id=belief_b.id,
                resolution="merge",
                new_proposition="Combined policy",
            )

        reloaded_a = await provider.get_belief(belief_a.id)
        reloaded_b = await provider.get_belief(belief_b.id)
        assert reloaded_a is not None and reloaded_a.lifecycle_state == "active"
        assert reloaded_b is not None and reloaded_b.lifecycle_state == "active"

    async def test_propagation_uses_sql_relations_when_graph_unavailable(
        self, provider, monkeypatch
    ):
        parent = Fact(
            id="fact-parent-sql",
            subject="Source",
            predicate="supports",
            object="Claim",
            confidence=1.0,
        )
        child = Fact(
            id="fact-child-sql",
            subject="Derived",
            predicate="depends_on",
            object="Parent",
            confidence=0.5,
        )
        await provider.create_fact(parent)
        await provider.create_fact(child)
        await provider.create_in_transaction(
            relation_entries=[
                {
                    "source_id": child.id,
                    "target_id": parent.id,
                    "relation_type": "derived_from",
                }
            ]
        )

        class BrokenGraph:
            def search_by_relation(self, relation: str):
                raise RuntimeError("graph unavailable")

        monkeypatch.setattr(server_module, "_provider", provider)
        monkeypatch.setattr(server_module, "_graph", BrokenGraph())
        monkeypatch.setattr(server_module, "_graph_router", None)

        payload = json.loads(
            await server_module.invalidate_tool(
                memory_id=parent.id,
                memory_type="fact",
                reason="parent invalidated",
            )
        )

        assert payload["propagated"]
        assert payload["propagated"][0]["memory_id"] == child.id

        updated_child = await provider.get_fact(child.id)
        assert updated_child is not None
        assert updated_child.confidence == pytest.approx(0.4)
        assert updated_child.version == 2


@pytest.mark.asyncio
class TestServerBeliefReinforcement:
    async def test_set_belief_returns_fresh_belief_and_persists_history(
        self, provider, monkeypatch
    ):
        original = Belief(proposition="User prefers Docker", confidence=0.4)
        await provider.create_belief(original)

        monkeypatch.setattr(server_module, "_provider", provider)
        monkeypatch.setattr(server_module, "_graph", None)
        monkeypatch.setattr(server_module, "_graph_router", None)

        payload = json.loads(
            await server_module.set_belief_tool(
                proposition="User prefers Docker",
                confidence=0.8,
                sources=json.dumps(
                    [
                        {
                            "source_type": "fact",
                            "source_id": "fact-1",
                            "weight": 0.8,
                        }
                    ]
                ),
                tags=json.dumps(["docker", "infra"]),
                source="test",
            )
        )

        assert payload["reinforced"] is True
        assert payload["belief"]["id"] == original.id
        assert payload["belief"]["confidence"] == pytest.approx(0.6)
        assert payload["receipt"]["confidence"] == pytest.approx(0.6)
        assert payload["receipt"]["history"][-1]["kind"] == "reinforce"
        assert payload["receipt"]["history"][-1]["previous_confidence"] == pytest.approx(0.4)
        assert payload["receipt"]["history"][-1]["confidence"] == pytest.approx(0.6)
        assert set(payload["belief"]["tags"]) == {"docker", "infra"}
        assert set(payload["belief"]["source_ids"]) == {"fact-1"}

        stored = await provider.get_belief(original.id)
        fresh_receipt = await provider.get_receipt(original.id)
        assert stored is not None
        assert stored.confidence == pytest.approx(0.6)
        assert set(stored.tags) == {"docker", "infra"}
        assert set(stored.source_ids) == {"fact-1"}
        assert fresh_receipt is not None
        assert fresh_receipt.history[-1]["kind"] == "reinforce"

    async def test_set_belief_rolls_back_when_reinforced_at_fails(
        self, provider, monkeypatch
    ):
        original = Belief(proposition="Rollback Docker", confidence=0.4)
        await provider.create_belief(original)

        async def boom(self, belief_id: str):
            raise RuntimeError("reinforced_at failed")

        monkeypatch.setattr(BeliefRepository, "update_reinforced_at", boom)
        monkeypatch.setattr(server_module, "_provider", provider)
        monkeypatch.setattr(server_module, "_graph", None)
        monkeypatch.setattr(server_module, "_graph_router", None)

        with pytest.raises(RuntimeError, match="reinforced_at failed"):
            await server_module.set_belief_tool(
                proposition="Rollback Docker",
                confidence=0.8,
                sources="[]",
                tags="[]",
                source="test",
            )

        stored = await provider.get_belief(original.id)
        receipt = await provider.get_receipt(original.id)
        assert stored is not None
        assert stored.confidence == pytest.approx(0.4)
        assert receipt is None
