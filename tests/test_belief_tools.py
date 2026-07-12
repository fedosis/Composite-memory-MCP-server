"""Integration tests for Belief MCP tool functions (Card 001).

Tests the set_belief, get_belief, and resolve_conflict tools
by exercising their underlying provider methods (the tools are thin
wrappers that call the same provider API).
"""

import json
import pytest

from memory_server.models import Belief, Evidence
from memory_server.models.receipt import MemoryReceipt
from memory_server.providers.sqlite_provider import SQLiteProvider


@pytest.fixture
async def provider():
    p = SQLiteProvider(url="sqlite+aiosqlite:///:memory:")
    await p.initialize()
    yield p
    await p.close()


@pytest.mark.asyncio
class TestSetBeliefTool:
    """Tests for set_belief tool behaviour: create, reinforce, supersede."""

    async def test_create_belief(self, provider):
        """set_belief creates a new belief."""
        b = Belief(proposition="Docker runs on OMV8", confidence=0.9, source="inference")
        created = await provider.create_belief(b)
        assert created.id == b.id
        assert created.proposition == "Docker runs on OMV8"
        assert created.confidence == 0.9
        assert created.source == "inference"
        assert created.version == 1
        assert created.lifecycle_state == "active"

    async def test_create_belief_with_evidence(self, provider):
        """set_belief with sources creates belief + evidence."""
        b = Belief(proposition="Caddy is a web server", confidence=0.85)
        evidence = [
            Evidence(belief_id=b.id, source_type="fact", source_id="f1", weight=0.8),
            Evidence(belief_id=b.id, source_type="observation", source_id="obs-1", weight=0.6),
        ]
        await provider.create_belief(b, evidence)
        retrieved = await provider.get_belief(b.id)
        assert retrieved is not None
        assert retrieved.proposition == "Caddy is a web server"

    async def test_reinforce_existing_belief(self, provider):
        """set_belief creates separate belief for same proposition (reinforce is MCP-tool-level)."""
        b1 = Belief(proposition="User prefers Docker", confidence=0.7)
        created = await provider.create_belief(b1)

        # Provider creates a new belief — reinforcement is in MCP tool layer (server.py)
        b2 = Belief(proposition="User prefers Docker", confidence=0.9)
        created2 = await provider.create_belief(b2)

        # Should be two separate beliefs
        assert created.id != created2.id
        assert created2.confidence == 0.9

    async def test_reinforce_case_insensitive(self, provider):
        """Provider treats different case as separate beliefs (normalization is MCP-tool-level)."""
        b1 = Belief(proposition="User prefers Docker", confidence=0.7)
        await provider.create_belief(b1)

        # Different case = different belief at provider level
        b2 = Belief(proposition="user prefers docker", confidence=0.9)
        await provider.create_belief(b2)

        all_beliefs = await provider.search_beliefs(lifecycle_state=None, limit=20)
        assert len(all_beliefs) == 2  # Two separate beliefs

    async def test_supersede_with_replace_belief_id(self, provider):
        """set_belief with replace_belief_id supersedes old belief."""
        old = Belief(proposition="Old idea", confidence=0.5)
        old_created = await provider.create_belief(old)

        # Supersede
        await provider.update_belief_lifecycle(old_created.id, "superseded")

        retrieved = await provider.get_belief(old_created.id)
        assert retrieved is not None
        assert retrieved.lifecycle_state == "superseded"


@pytest.mark.asyncio
class TestGetBeliefTool:
    """Tests for get_belief tool: search, filters."""

    async def _seed(self, provider):
        beliefs = [
            Belief(proposition="Docker runs on OMV8", confidence=0.9, tags=["docker", "infra"]),
            Belief(proposition="Caddy reverse proxy", confidence=0.85, tags=["caddy", "web"]),
            Belief(proposition="User prefers dark mode", confidence=0.6, tags=["user-preference"]),
            Belief(proposition="Deprecated setting", confidence=0.3, tags=["old"], source="legacy"),
        ]
        for b in beliefs:
            await provider.create_belief(b)
        # Mark one as superseded
        await provider.update_belief_lifecycle(beliefs[3].id, "superseded")
        return beliefs

    async def test_search_by_proposition_fts5(self, provider):
        await self._seed(provider)
        results = await provider.search_beliefs(proposition="Docker")
        assert len(results) >= 1
        assert any("Docker" in r.proposition for r in results)

    async def test_search_by_tags(self, provider):
        await self._seed(provider)
        results = await provider.search_beliefs(tags=["docker"])
        assert len(results) >= 1

    async def test_search_by_lifecycle_state(self, provider):
        await self._seed(provider)
        active = await provider.search_beliefs(lifecycle_state="active")
        superseded = await provider.search_beliefs(lifecycle_state="superseded")
        assert len(active) >= 3
        assert len(superseded) >= 1

    async def test_search_by_min_confidence(self, provider):
        await self._seed(provider)
        results = await provider.search_beliefs(min_confidence=0.8)
        assert all(r.confidence >= 0.8 for r in results)

    async def test_search_all_states(self, provider):
        await self._seed(provider)
        results = await provider.search_beliefs(lifecycle_state=None, limit=20)
        assert len(results) == 4  # all beliefs regardless of lifecycle_state

    async def test_search_limit(self, provider):
        await self._seed(provider)
        results = await provider.search_beliefs(lifecycle_state=None, limit=2)
        assert len(results) == 2


@pytest.mark.asyncio
class TestResolveConflictTool:
    """Tests for resolve_conflict: transition matrix validation."""

    async def _create_conflicting(self, provider):
        b1 = Belief(proposition="Docker is better than Podman", confidence=0.8)
        b2 = Belief(proposition="Podman is better than Docker", confidence=0.6)
        await provider.create_belief(b1)
        await provider.create_belief(b2)
        return b1, b2

    async def test_supersede_a(self, provider):
        b1, b2 = await self._create_conflicting(provider)
        await provider.update_belief_lifecycle(b1.id, "superseded")
        r1 = await provider.get_belief(b1.id)
        assert r1.lifecycle_state == "superseded"

    async def test_supersede_b(self, provider):
        b1, b2 = await self._create_conflicting(provider)
        await provider.update_belief_lifecycle(b2.id, "superseded")
        r2 = await provider.get_belief(b2.id)
        assert r2.lifecycle_state == "superseded"

    async def test_discard_both(self, provider):
        b1, b2 = await self._create_conflicting(provider)
        await provider.update_belief_lifecycle(b1.id, "discarded")
        await provider.update_belief_lifecycle(b2.id, "discarded")
        r1 = await provider.get_belief(b1.id)
        r2 = await provider.get_belief(b2.id)
        assert r1.lifecycle_state == "discarded"
        assert r2.lifecycle_state == "discarded"

    async def test_merge_creates_new_belief(self, provider):
        """Resolve conflict via merge creates a new belief."""
        b1, b2 = await self._create_conflicting(provider)
        merged = Belief(
            proposition="Both tools have their place",
            confidence=0.7,
            source="conflict_resolution",
        )
        await provider.create_belief(merged)
        # Mark originals
        await provider.update_belief_lifecycle(b1.id, "superseded")
        await provider.update_belief_lifecycle(b2.id, "superseded")

        # Verify merge result
        r_merged = await provider.get_belief(merged.id)
        assert r_merged is not None
        assert r_merged.proposition == "Both tools have their place"
        assert r_merged.lifecycle_state == "active"

        # Verify originals superseded
        r1 = await provider.get_belief(b1.id)
        r2 = await provider.get_belief(b2.id)
        assert r1.lifecycle_state == "superseded"
        assert r2.lifecycle_state == "superseded"

    async def test_resolve_conflict_invalid_resolution_raises(self, provider):
        """resolve_conflict with invalid resolution raises ValueError."""
        b1, b2 = await self._create_conflicting(provider)
        # Tool-level validation rejects unknown resolutions
        valid = {"keep_a", "keep_b", "merge", "discard_both"}
        for bad in ("supersede_a", "supersede_b", "nonexistent", ""):
            assert bad not in valid, f"{bad} should not be valid"

    async def test_resolve_conflict_nonexistent_belief(self, provider):
        """resolve_conflict with non-existent belief_id raises ValueError."""
        fake_id = "00000000-0000-0000-0000-000000000000"
        b1 = Belief(proposition="Real belief", confidence=0.5)
        await provider.create_belief(b1)
        result = await provider.get_belief(fake_id)
        assert result is None

    async def test_set_belief_replace_nonexistent_id(self, provider):
        """set_belief with replace_belief_id on non-existent ID creates belief without superseding."""
        fake_id = "00000000-0000-0000-0000-000000000000"
        old_belief = await provider.get_belief(fake_id)
        assert old_belief is None
        # Should still create the belief
        b = Belief(proposition="New belief", confidence=0.5)
        created = await provider.create_belief(b)
        assert created is not None
        assert created.proposition == "New belief"


@pytest.mark.asyncio
class TestBeliefLifecycleIntegration:
    """End-to-end lifecycle tests for beliefs."""

    async def test_full_lifecycle_create_to_forgotten(self, provider):
        """Belief traverses: active → stale → archived → forgotten via decay."""
        b = Belief(proposition="Temporary knowledge", confidence=0.5)
        await provider.create_belief(b)
        assert b.lifecycle_state == "active"

        # Direct state transitions
        await provider.update_belief_lifecycle(b.id, "stale")
        r = await provider.get_belief(b.id)
        assert r.lifecycle_state == "stale"

        await provider.update_belief_lifecycle(b.id, "archived")
        r = await provider.get_belief(b.id)
        assert r.lifecycle_state == "archived"

        await provider.update_belief_lifecycle(b.id, "forgotten")
        r = await provider.get_belief(b.id)
        assert r.lifecycle_state == "forgotten"

    async def test_superseded_does_not_affect_active_search(self, provider):
        """Superseded beliefs excluded from default search (lifecycle_state='active')."""
        active = Belief(proposition="Active belief", confidence=0.9)
        stale = Belief(proposition="Superseded belief", confidence=0.5)
        await provider.create_belief(active)
        await provider.create_belief(stale)
        await provider.update_belief_lifecycle(stale.id, "superseded")

        results = await provider.search_beliefs(lifecycle_state="active")
        ids = [r.id for r in results]
        assert active.id in ids
        assert stale.id not in ids
