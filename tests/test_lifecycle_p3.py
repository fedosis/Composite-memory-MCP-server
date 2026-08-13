"""Tests for Phase 3 lifecycle service and tool integration."""

from __future__ import annotations

import json

import pytest
from storage.repositories import LifecycleRepository

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
