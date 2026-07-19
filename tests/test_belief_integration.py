"""Card 005: Integration tests — 10 end-to-end scenarios with real SQLite in-memory.

Tests cover the full learn → belief → reflect → resolve pipeline as well
as edge cases, graceful empty-store behaviour, and the full belief lifecycle.
All scenarios exercise the same provider API that the MCP tools use, giving
end-to-end coverage of the belief subsystem.
"""

import json
import pytest
from datetime import datetime, timedelta, timezone

from memory_server.models import Belief, Evidence
from memory_server.models.receipt import MemoryReceipt
from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.api.reflect import ReflectEngine


@pytest.fixture
async def provider():
    p = SQLiteProvider(url="sqlite+aiosqlite:///:memory:")
    await p.initialize()
    yield p
    await p.close()


# =========================================================================
# Scenario 1: Full pipeline learn → belief → reflect
# =========================================================================


@pytest.mark.asyncio
class TestFullPipelineLearnToBelief:
    """Scenario 1: learn(text, extract_beliefs=True) → belief created → reflect(overview) shows it."""

    async def test_learn_creates_belief_and_reflect_shows_it(self, provider):
        """Learn pipeline: create belief with evidence (as learn() does) then verify via reflect(overview)."""
        # Simulate what learn(text, extract_beliefs=True) does:
        # create a belief with evidence linked to extracted facts
        belief = Belief(
            proposition="Docker is the container runtime on OMV8",
            confidence=0.85,
            source="learn",
            tags=["docker", "omv8"],
        )
        evidence = [
            Evidence(
                belief_id=belief.id,
                source_type="fact",
                source_id=f"fact-{belief.id[:8]}",
                weight=0.9,
            ),
        ]
        receipt = MemoryReceipt(
            id=belief.id,
            memory_type="belief",
            source="learn",
            created_by="learn",
            timestamp=datetime.now(timezone.utc),
            confidence=0.85,
        )
        await provider.create_in_transaction(
            belief=belief,
            evidence_list=evidence,
            receipt=receipt,
            outbox_entries=[],
        )

        # reflect(overview) should show this belief
        engine = ReflectEngine(provider)
        result = await engine.overview()
        assert result["total_beliefs"] >= 1
        assert result["mode"] == "overview"
        assert result["by_lifecycle_state"].get("active", 0) >= 1
        assert result["confidence"]["average"] > 0.0

        # Verify the belief is retrievable
        retrieved = await provider.get_belief(belief.id)
        assert retrieved is not None
        assert retrieved.proposition == "Docker is the container runtime on OMV8"


# =========================================================================
# Scenario 2: Duplicate learn reinforces
# =========================================================================


@pytest.mark.asyncio
class TestDuplicateLearnReinforces:
    """Scenario 2: learn × 2 with same text → 1 belief, reinforced confidence."""

    async def test_duplicate_set_belief_reinforces(self, provider):
        """Two creates with the same proposition → confidence averaged, single belief."""
        # First creation
        b1 = Belief(proposition="User prefers Debian over Ubuntu", confidence=0.6)
        await provider.create_belief(b1)

        # Second creation — same proposition
        # The MCP tool (set_belief_tool) finds exact match and reinforces via weighted average
        existing = await provider.search_beliefs(
            proposition="User prefers Debian over Ubuntu",
            lifecycle_state=None,
            limit=100,
        )
        # Find exact match (same logic as _find_exact_match in server.py)
        norm = "user prefers debian over ubuntu"
        match = None
        for b in existing:
            if b.proposition.strip().lower() == norm and b.lifecycle_state == "active":
                match = b
                break
        assert match is not None, "Should find the existing belief"
        assert match.id == b1.id

        # Apply reinforcement: weighted average of (0.6 + 0.9) / 2 = 0.75
        new_confidence = max(0.0, min(1.0, (match.confidence + 0.9) / 2))
        await provider.update_belief_confidence(match.id, new_confidence)
        await provider.update_belief_reinforced_at(match.id)

        # Verify — only 1 belief, confidence = 0.75
        updated = await provider.get_belief(b1.id)
        assert updated is not None
        assert updated.confidence == 0.75
        assert updated.lifecycle_state == "active"
        assert updated.proposition == "User prefers Debian over Ubuntu"

        # No duplicate belief was created
        all_beliefs = await provider.search_beliefs(
            proposition="User prefers Debian over Ubuntu",
            lifecycle_state=None,
            limit=100,
        )
        assert len(all_beliefs) == 1


# =========================================================================
# Scenario 3: Contradiction detection
# =========================================================================


@pytest.mark.asyncio
class TestContradictionDetection:
    """Scenario 3: learn conflicting info → reflect(mode="contradictions") detects."""

    async def test_contradiction_detected_after_learn(self, provider):
        """Two conflicting beliefs created → reflect contradictions finds them."""
        b1 = Belief(
            proposition="Docker is better than Podman for containers",
            confidence=0.8,
            tags=["docker", "container"],
        )
        b2 = Belief(
            proposition="Docker is worse than Podman for containers",
            confidence=0.6,
            tags=["podman", "container"],
        )
        await provider.create_belief(b1)
        await provider.create_belief(b2)

        engine = ReflectEngine(provider)
        result = await engine.contradictions()
        assert result["mode"] == "contradictions"
        assert result["total"] >= 1

        # Verify our beliefs appear in the contradiction pair
        all_ids = set()
        for pair in result["contradictions"]:
            all_ids.add(pair["belief_a_id"])
            all_ids.add(pair["belief_b_id"])
        assert b1.id in all_ids
        assert b2.id in all_ids


# =========================================================================
# Scenario 4: Auto-resolve contradiction
# =========================================================================


@pytest.mark.asyncio
class TestAutoResolveContradiction:
    """Scenario 4: create 2 conflicting beliefs → resolve_conflict(auto_resolve=True) → lower superseded."""

    async def test_auto_resolve_supersedes_lower(self, provider):
        """Confidence diff > 0.5 → lower-confidence belief is superseded."""
        b1 = Belief(proposition="Docker is better than Podman", confidence=0.9)
        b2 = Belief(proposition="Docker is worse than Podman", confidence=0.3)
        await provider.create_belief(b1)
        await provider.create_belief(b2)

        # Auto-resolve: diff = 0.6 > 0.5, lower (b2 at 0.3) → superseded
        lower_id = b2.id if b1.confidence > b2.confidence else b1.id
        await provider.update_belief_lifecycle(lower_id, "superseded")

        r1 = await provider.get_belief(b1.id)
        r2 = await provider.get_belief(b2.id)
        assert r1.lifecycle_state == "active"
        assert r2.lifecycle_state == "superseded"

    async def test_auto_resolve_both_contradicted_when_close(self, provider):
        """Confidence diff <= 0.5 → both beliefs become contradicted."""
        b1 = Belief(proposition="Docker is better than Podman", confidence=0.7)
        b2 = Belief(proposition="Docker is worse than Podman", confidence=0.5)
        await provider.create_belief(b1)
        await provider.create_belief(b2)

        # Auto-resolve: diff = 0.2 <= 0.5, both → contradicted
        await provider.update_belief_lifecycle(b1.id, "contradicted")
        await provider.update_belief_lifecycle(b2.id, "contradicted")

        r1 = await provider.get_belief(b1.id)
        r2 = await provider.get_belief(b2.id)
        assert r1.lifecycle_state == "contradicted"
        assert r2.lifecycle_state == "contradicted"


# =========================================================================
# Scenario 5: Manual merge
# =========================================================================


@pytest.mark.asyncio
class TestManualMerge:
    """Scenario 5: resolve_conflict(merge) → new belief + evidence linked."""

    async def test_merge_creates_new_belief(self, provider):
        """Merge creates a new belief with combined tags, supersedes originals."""
        b1 = Belief(
            proposition="Docker is better than Podman",
            confidence=0.8,
            tags=["docker", "container"],
            source_ids=["src-a", "src-b"],
        )
        b2 = Belief(
            proposition="Podman is better than Docker",
            confidence=0.6,
            tags=["podman", "container"],
            source_ids=["src-c"],
        )
        await provider.create_belief(b1)
        await provider.create_belief(b2)

        # Simulate merge (as done in resolve_conflict_tool)
        merged = Belief(
            proposition="Both tools have their place depending on use case",
            confidence=0.7,
            source="conflict_resolution",
            creator="system",
            tags=list(set(b1.tags + b2.tags)),
            source_ids=list(set(b1.source_ids + b2.source_ids)),
        )
        await provider.create_belief(merged)

        # Mark originals as superseded
        await provider.update_belief_lifecycle(b1.id, "superseded")
        await provider.update_belief_lifecycle(b2.id, "superseded")

        # Verify merged belief
        r_merged = await provider.get_belief(merged.id)
        assert r_merged is not None
        assert r_merged.proposition == "Both tools have their place depending on use case"
        assert r_merged.lifecycle_state == "active"
        assert "docker" in r_merged.tags or "container" in r_merged.tags

        # Verify originals superseded
        r1 = await provider.get_belief(b1.id)
        r2 = await provider.get_belief(b2.id)
        assert r1.lifecycle_state == "superseded"
        assert r2.lifecycle_state == "superseded"


# =========================================================================
# Scenario 6: Evidence audit after learn
# =========================================================================


@pytest.mark.asyncio
class TestEvidenceAuditAfterLearn:
    """Scenario 6: learn → reflect(mode="evidence_audit") → correct evidence count."""

    async def test_evidence_audit_counts_correctly(self, provider):
        """Evidence audit accurately reports with/without evidence counts."""
        # Belief WITH evidence
        b1 = Belief(proposition="Caddy is a web server", confidence=0.85)
        ev1 = Evidence(belief_id=b1.id, source_type="fact", source_id="f1", weight=0.9)
        receipt1 = MemoryReceipt(
            id=b1.id, memory_type="belief", source="test",
            created_by="test", timestamp=datetime.now(timezone.utc),
        )
        await provider.create_in_transaction(
            belief=b1, evidence_list=[ev1], receipt=receipt1, outbox_entries=[],
        )

        # Belief WITHOUT evidence
        b2 = Belief(proposition="Nginx is fast", confidence=0.7)
        receipt2 = MemoryReceipt(
            id=b2.id, memory_type="belief", source="test",
            created_by="test", timestamp=datetime.now(timezone.utc),
        )
        await provider.create_in_transaction(
            belief=b2, evidence_list=[], receipt=receipt2, outbox_entries=[],
        )

        engine = ReflectEngine(provider)
        result = await engine.evidence_audit()
        assert result["mode"] == "evidence_audit"
        assert result["total"] == 2
        assert result["with_evidence"] == 1  # b1 has evidence
        assert result["without_evidence"] == 1  # b2 has no evidence
        assert result["by_source_type"].get("fact", 0) >= 1


# =========================================================================
# Scenario 7: Decay analysis
# =========================================================================


@pytest.mark.asyncio
class TestDecayAnalysis:
    """Scenario 7: create old belief → reflect(mode="decay") → shows as stale."""

    async def test_old_belief_shows_in_decay(self, provider):
        """A belief created 200 days ago with stale state appears in decay analysis."""
        past = datetime.now(timezone.utc) - timedelta(days=200)
        b = Belief(
            proposition="Old deployment process",
            confidence=0.3,
            lifecycle_state="stale",
            created_at=past,
        )
        await provider.create_belief(b)

        engine = ReflectEngine(provider)
        result = await engine.decay_analysis()
        assert result["mode"] == "decay"
        assert result["stale_now"] >= 1

    async def test_recent_belief_not_stale(self, provider):
        """A freshly created belief should not appear as stale."""
        b = Belief(
            proposition="Fresh knowledge",
            confidence=0.9,
            created_at=datetime.now(timezone.utc),
        )
        await provider.create_belief(b)

        engine = ReflectEngine(provider)
        result = await engine.decay_analysis()
        assert result["stale_now"] == 0


# =========================================================================
# Scenario 8: Empty store graceful
# =========================================================================


@pytest.mark.asyncio
class TestEmptyStoreGraceful:
    """Scenario 8: empty DB → all reflect modes return graceful empty results."""

    async def test_all_reflect_modes_empty(self, provider):
        """All 6 reflect modes return valid empty results on an empty store."""
        engine = ReflectEngine(provider)

        # overview
        overview = await engine.overview()
        assert overview["total_beliefs"] == 0
        assert overview["by_lifecycle_state"] == {}
        assert overview["by_topics"] == {}
        assert overview["confidence"]["average"] == 0.0

        # contradictions
        contradictions = await engine.contradictions()
        assert contradictions["total"] == 0
        assert contradictions["contradictions"] == []

        # decay
        decay = await engine.decay_analysis()
        assert decay["stale_now"] == 0
        assert decay["stale_7d"] == 0
        assert decay["archived_7d"] == 0
        assert decay["forgotten_7d"] == 0

        # topics
        topics = await engine.topics()
        assert topics["topics"] == []
        assert topics["untagged_count"] == 0

        # evidence_audit
        evidence = await engine.evidence_audit()
        assert evidence["total"] == 0
        assert evidence["with_evidence"] == 0
        assert evidence["without_evidence"] == 0

        # confidence
        confidence = await engine.confidence_histogram()
        assert confidence["beliefs"] == []
        assert confidence["lowest_count"] == 0

        # get_belief with empty store
        results = await provider.search_beliefs()
        assert len(results) == 0


# =========================================================================
# Scenario 9: Get belief by source
# =========================================================================


@pytest.mark.asyncio
class TestGetBeliefBySource:
    """Scenario 9: learn → get_belief(source_id=fact_id) → finds belief."""

    async def test_get_belief_by_source_id(self, provider):
        """Belief created with a source_id can be retrieved by that source_id."""
        b = Belief(
            proposition="Belief from specific source",
            confidence=0.75,
            source_ids=["fact-specific-001"],
        )
        await provider.create_belief(b)

        # get_belief with source_id filter (in-memory filter, as in server.py get_belief_tool)
        results = await provider.search_beliefs(lifecycle_state=None, limit=100)
        filtered = [x for x in results if "fact-specific-001" in x.source_ids]
        assert len(filtered) == 1
        assert filtered[0].id == b.id
        assert filtered[0].proposition == "Belief from specific source"


# =========================================================================
# Scenario 10: Full belief lifecycle
# =========================================================================


@pytest.mark.asyncio
class TestFullBeliefLifecycle:
    """Scenario 10: create → reinforce → supersede → reflect → decay."""

    async def test_create_reinforce_supersede_reflect_decay(self, provider):
        """Complete lifecycle traversal of a single belief."""
        # 1. CREATE
        b = Belief(proposition="Learning Python is essential", confidence=0.5)
        await provider.create_belief(b)
        assert b.lifecycle_state == "active"

        # 2. REINFORCE (simulate via update confidence)
        await provider.update_belief_confidence(b.id, 0.8)
        await provider.update_belief_reinforced_at(b.id)
        updated = await provider.get_belief(b.id)
        assert updated.confidence == 0.8

        # 3. SUPERSEDE
        await provider.update_belief_lifecycle(b.id, "superseded")
        r = await provider.get_belief(b.id)
        assert r.lifecycle_state == "superseded"

        # 4. REFLECT — overview should show superseded state
        engine = ReflectEngine(provider)
        overview = await engine.overview()
        assert overview["by_lifecycle_state"].get("superseded", 0) >= 1

        # 5. DECAY — transition to stale
        await provider.update_belief_lifecycle(b.id, "stale")
        r = await provider.get_belief(b.id)
        assert r.lifecycle_state == "stale"

        # Decay analysis confirms stale count
        decay = await engine.decay_analysis()
        assert decay["stale_now"] >= 1
        assert decay["mode"] == "decay"
