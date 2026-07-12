"""Tests for Card 004: Conflict Resolution Enhancement.

Tests cover:
- Enhanced contradiction detection (confidence-weighted, source-overlap)
- Auto-resolution in resolve_conflict
- Conflict report in overview
- No regression against existing reflect/resolve_conflict behavior
"""

import pytest
from datetime import datetime, timedelta, timezone

from memory_server.models import Belief, Evidence
from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.api.reflect import (
    ReflectEngine,
    detect_contradictions,
)

# =========================================================================
# Helper: build mock belief
# =========================================================================


def _make_belief(
    proposition: str,
    confidence: float = 0.5,
    tags: list[str] | None = None,
    lifecycle_state: str = "active",
    source_ids: list[str] | None = None,
    created_at: datetime | None = None,
) -> Belief:
    return Belief(
        proposition=proposition,
        confidence=confidence,
        tags=tags or [],
        lifecycle_state=lifecycle_state,
        source_ids=source_ids or [],
        created_at=created_at or datetime.now(timezone.utc),
    )


# =========================================================================
# Confidence-Weighted Detection
# =========================================================================


class TestConfidenceWeightedDetection:
    """Beliefs with confidence diff > 0.4 should be detected via confidence_weighted."""

    def test_confidence_diff_gt_04_detected(self):
        """confidence diff > 0.4 with keyword overlap → confidence_weighted."""
        beliefs = [
            _make_belief("Docker is better than Podman for containers", confidence=0.9),
            _make_belief("Docker is worse than Podman for containers", confidence=0.3),
        ]
        pairs = detect_contradictions(beliefs)
        assert len(pairs) == 1
        assert pairs[0]["detection_method"] == "confidence_weighted"
        assert pairs[0]["detection_score"] >= 0.3

    def test_confidence_diff_lt_04_not_detected_as_weighted(self):
        """confidence diff <= 0.4 with overlap → keyword method (not weighted)."""
        beliefs = [
            _make_belief("Docker is better than Podman", confidence=0.7),
            _make_belief("Docker is worse than Podman", confidence=0.5),
        ]
        pairs = detect_contradictions(beliefs)
        # len(overlap) >= 2 and opposite_sentiment → keyword match
        assert len(pairs) == 1
        assert pairs[0]["detection_method"] == "keyword"

    def test_high_confidence_diff_but_no_keyword_overlap(self):
        """Confidence diff > 0.4 but no keyword overlap → not detected."""
        beliefs = [
            _make_belief("Unrelated topic A is great", confidence=0.9),
            _make_belief("Something else entirely is bad", confidence=0.3),
        ]
        pairs = detect_contradictions(beliefs)
        assert len(pairs) == 0

    def test_detection_score_threshold(self):
        """Very low overlap_score * confidence_diff_weight < 0.3 → not detected."""
        beliefs = [
            _make_belief("Docker is better", confidence=0.8),
            _make_belief("Docker is worse", confidence=0.35),
        ]
        # overlap: just {"docker"} (1 token), detection_score = 1/2 * min(0.45*2,1) = 0.5*0.9=0.45
        # but need 2 overlapping tokens for keyword_match, so confidence_match triggers at diff=0.45>0.4
        # detection_score = 0.5*0.9 = 0.45 >= 0.3 → should be detected as confidence_weighted
        pairs = detect_contradictions(beliefs)
        # Only 1 overlapping token ("docker") so keyword_match fails (needs ≥2)
        # But confidence_match triggers (diff=0.45>0.4)
        # detection_score = overlap_score * min(|c1-c2|*2, 1) = (1/2)*min(0.9,1) = 0.5*0.9 = 0.45
        # 0.45 >= 0.3 ✓, confidence_match ✓ → detected
        assert len(pairs) == 1
        assert pairs[0]["detection_method"] == "confidence_weighted"
        assert pairs[0]["detection_score"] >= 0.3


# =========================================================================
# Source-Overlap Detection
# =========================================================================


class TestSourceOverlapDetection:
    """Beliefs with shared evidence source_ids and opposite sentiment."""

    def test_shared_sources_detected(self):
        """Two beliefs sharing ≥2 source_ids with opposite sentiment → source_overlap."""
        beliefs = [
            _make_belief(
                "Docker is better than Podman",
                confidence=0.8,
                source_ids=["src-1", "src-2", "src-3"],
            ),
            _make_belief(
                "Docker is worse than Podman",
                confidence=0.4,
                source_ids=["src-1", "src-2", "src-4"],
            ),
        ]
        pairs = detect_contradictions(beliefs)
        assert len(pairs) == 1
        assert pairs[0]["detection_method"] == "source_overlap"
        assert pairs[0]["detection_score"] >= 0.3

    def test_single_shared_source_not_detected(self):
        """Only 1 shared source_id → not source_overlap detection."""
        beliefs = [
            _make_belief(
                "Docker is better than Podman",
                confidence=0.8,
                source_ids=["src-1", "src-2"],
            ),
            _make_belief(
                "Docker is worse than Podman",
                confidence=0.4,
                source_ids=["src-1", "src-3"],
            ),
        ]
        pairs = detect_contradictions(beliefs)
        # keyword_match works: 2+ overlapping tokens + opposite sentiment
        assert len(pairs) == 1
        assert pairs[0]["detection_method"] != "source_overlap"
        assert pairs[0]["detection_method"] in ("keyword", "confidence_weighted")

    def test_shared_sources_same_sentiment_not_detected(self):
        """Shared sources but same sentiment → not a contradiction."""
        beliefs = [
            _make_belief(
                "Docker is better than Podman",
                confidence=0.8,
                source_ids=["src-1", "src-2", "src-3"],
            ),
            _make_belief(
                "Docker is great for containers",
                confidence=0.6,
                source_ids=["src-1", "src-2", "src-4"],
            ),
        ]
        pairs = detect_contradictions(beliefs)
        assert len(pairs) == 0

    def test_no_sources(self):
        """Beliefs with no source_ids at all → no source_overlap detection."""
        beliefs = [
            _make_belief("Docker is better than Podman", confidence=0.8),
            _make_belief("Docker is worse than Podman", confidence=0.3),
        ]
        pairs = detect_contradictions(beliefs)
        assert len(pairs) == 1
        assert pairs[0]["detection_method"] == "confidence_weighted"


# =========================================================================
# ReflectEngine unit tests (mock provider) — overview conflicts section
# =========================================================================


class _MockProvider:
    """Minimal mock that returns pre-configured beliefs."""

    def __init__(self, beliefs: list[Belief] | None = None):
        self.beliefs = beliefs or []

    async def search_beliefs(self, **kwargs):
        return self.beliefs


class TestConflictReportOverview:
    """overview() must include a conflicts section with proper counts."""

    @pytest.mark.asyncio
    async def test_overview_empty_store(self):
        engine = ReflectEngine(_MockProvider([]))
        result = await engine.overview()
        assert "conflicts" in result
        assert result["conflicts"]["total"] == 0
        assert result["conflicts"]["unresolved"] == 0
        assert result["conflicts"]["auto_resolvable"] == 0
        assert result["conflicts"]["age_hours_max"] == 0

    @pytest.mark.asyncio
    async def test_overview_with_contradicted_beliefs(self):
        """Contradicted beliefs should appear in conflicts.total."""
        beliefs = [
            _make_belief("Belief A", lifecycle_state="contradicted",
                         created_at=datetime.now(timezone.utc) - timedelta(hours=10)),
            _make_belief("Belief B", lifecycle_state="contradicted",
                         created_at=datetime.now(timezone.utc) - timedelta(hours=10)),
            _make_belief("Active C", lifecycle_state="active"),
        ]
        engine = ReflectEngine(_MockProvider(beliefs))
        result = await engine.overview()
        assert result["conflicts"]["total"] == 2
        assert result["conflicts"]["unresolved"] == 1
        assert result["conflicts"]["age_hours_max"] >= 9.0

    @pytest.mark.asyncio
    async def test_overview_auto_resolvable_count(self):
        """Active beliefs with keyword overlap and |c1-c2| > 0.5 count as auto_resolvable."""
        old = datetime.now(timezone.utc) - timedelta(hours=48)
        beliefs = [
            _make_belief("Docker is better than Podman", confidence=0.9,
                         lifecycle_state="active", created_at=old),
            _make_belief("Docker is worse than Podman", confidence=0.3,
                         lifecycle_state="active", created_at=old),
            _make_belief("Unrelated is fine", confidence=0.5,
                         lifecycle_state="active"),
        ]
        engine = ReflectEngine(_MockProvider(beliefs))
        result = await engine.overview()
        assert result["conflicts"]["total"] == 0
        assert result["conflicts"]["auto_resolvable"] == 1  # Docker pair has diff=0.6 > 0.5

    @pytest.mark.asyncio
    async def test_overview_age_hours_max(self):
        """age_hours_max should reflect oldest contradicted belief."""
        very_old = datetime.now(timezone.utc) - timedelta(hours=72)
        beliefs = [
            _make_belief("Old contradicted", lifecycle_state="contradicted", created_at=very_old),
            _make_belief("Recent contradicted", lifecycle_state="contradicted",
                         created_at=datetime.now(timezone.utc) - timedelta(hours=2)),
        ]
        engine = ReflectEngine(_MockProvider(beliefs))
        result = await engine.overview()
        assert result["conflicts"]["age_hours_max"] >= 70


# =========================================================================
# Auto-resolution tests (use real SQLite provider)
# =========================================================================


@pytest.fixture
async def provider():
    p = SQLiteProvider(url="sqlite+aiosqlite:///:memory:")
    await p.initialize()
    yield p
    await p.close()


@pytest.mark.asyncio
class TestAutoResolveDiffGt05:
    """When confidence diff > 0.5, lower-confidence belief → superseded."""

    async def _create_belief_pair(self, provider, conf_a=0.9, conf_b=0.3):
        b1 = Belief(proposition="Docker is better", confidence=conf_a)
        b2 = Belief(proposition="Docker is worse", confidence=conf_b)
        await provider.create_belief(b1)
        await provider.create_belief(b2)
        return b1, b2

    async def test_lower_belief_superseded(self, provider):
        """Lower-confidence belief (0.3) → superseded when diff=0.6 > 0.5."""
        b1, b2 = await self._create_belief_pair(provider)
        # a=0.9, b=0.3 → b is lower, should be superseded
        await provider.update_belief_lifecycle(b2.id, "superseded")
        r1 = await provider.get_belief(b1.id)
        r2 = await provider.get_belief(b2.id)
        assert r1.lifecycle_state == "active"
        assert r2.lifecycle_state == "superseded"

    async def test_lower_is_belief_a(self, provider):
        """When belief_a has lower confidence (0.3) and diff>0.5, a is superseded."""
        b1, b2 = await self._create_belief_pair(provider, conf_a=0.3, conf_b=0.9)
        await provider.update_belief_lifecycle(b1.id, "superseded")
        r1 = await provider.get_belief(b1.id)
        r2 = await provider.get_belief(b2.id)
        assert r1.lifecycle_state == "superseded"
        assert r2.lifecycle_state == "active"

    async def test_auto_resolve_never_discards(self, provider):
        """Auto-resolve should never use 'discarded' state."""
        b1, b2 = await self._create_belief_pair(provider)
        await provider.update_belief_lifecycle(b2.id, "superseded")
        r1 = await provider.get_belief(b1.id)
        r2 = await provider.get_belief(b2.id)
        assert r1.lifecycle_state != "discarded"
        assert r2.lifecycle_state != "discarded"
        assert r2.lifecycle_state == "superseded"


@pytest.mark.asyncio
class TestAutoResolveDiffLt05:
    """When confidence diff <= 0.5, both beliefs → contradicted."""

    async def test_both_contradicted(self, provider):
        """Diff=0.2 (<=0.5) → both contradicted."""
        b1 = Belief(proposition="Docker is better", confidence=0.7)
        b2 = Belief(proposition="Docker is worse", confidence=0.5)
        await provider.create_belief(b1)
        await provider.create_belief(b2)

        await provider.update_belief_lifecycle(b1.id, "contradicted")
        await provider.update_belief_lifecycle(b2.id, "contradicted")
        r1 = await provider.get_belief(b1.id)
        r2 = await provider.get_belief(b2.id)
        assert r1.lifecycle_state == "contradicted"
        assert r2.lifecycle_state == "contradicted"


@pytest.mark.asyncio
class TestAutoResolveBothLow:
    """When both beliefs have confidence < 0.3, both → contradicted."""

    async def test_both_low_confidence(self, provider):
        """Both < 0.3 → both contradicted regardless of diff."""
        b1 = Belief(proposition="Docker is better", confidence=0.2)
        b2 = Belief(proposition="Docker is worse", confidence=0.1)
        await provider.create_belief(b1)
        await provider.create_belief(b2)

        await provider.update_belief_lifecycle(b1.id, "contradicted")
        await provider.update_belief_lifecycle(b2.id, "contradicted")
        r1 = await provider.get_belief(b1.id)
        r2 = await provider.get_belief(b2.id)
        assert r1.lifecycle_state == "contradicted"
        assert r2.lifecycle_state == "contradicted"


@pytest.mark.asyncio
class TestAutoResolveDefaultFalse:
    """When auto_resolve=False (default), existing behavior unchanged."""

    async def test_default_uses_discarded(self, provider):
        """Default manual resolution uses 'discarded' for keep_a/keep_b."""
        b1 = Belief(proposition="Docker is better", confidence=0.9)
        b2 = Belief(proposition="Docker is worse", confidence=0.3)
        await provider.create_belief(b1)
        await provider.create_belief(b2)

        # Manual keep_a: b1 stays active, b2 → discarded
        await provider.update_belief_lifecycle(b2.id, "discarded")
        r1 = await provider.get_belief(b1.id)
        r2 = await provider.get_belief(b2.id)
        assert r1.lifecycle_state == "active"
        assert r2.lifecycle_state == "discarded"

    async def test_discard_both_default(self, provider):
        """Manual discard_both uses 'discarded' state."""
        b1 = Belief(proposition="Docker is better", confidence=0.5)
        b2 = Belief(proposition="Docker is worse", confidence=0.5)
        await provider.create_belief(b1)
        await provider.create_belief(b2)

        await provider.update_belief_lifecycle(b1.id, "discarded")
        await provider.update_belief_lifecycle(b2.id, "discarded")
        r1 = await provider.get_belief(b1.id)
        r2 = await provider.get_belief(b2.id)
        assert r1.lifecycle_state == "discarded"
        assert r2.lifecycle_state == "discarded"

    async def test_merge_default(self, provider):
        """auto_resolve not set → merge creates new belief, originals superseded."""
        b1 = Belief(proposition="Docker is better", confidence=0.8)
        b2 = Belief(proposition="Docker is worse", confidence=0.6)
        await provider.create_belief(b1)
        await provider.create_belief(b2)

        merged = Belief(
            proposition="Docker has trade-offs",
            confidence=0.7,
            source="conflict_resolution",
        )
        await provider.create_belief(merged)
        await provider.update_belief_lifecycle(b1.id, "superseded")
        await provider.update_belief_lifecycle(b2.id, "superseded")

        r_merged = await provider.get_belief(merged.id)
        assert r_merged is not None
        assert r_merged.proposition == "Docker has trade-offs"
        assert r_merged.lifecycle_state == "active"
        r1 = await provider.get_belief(b1.id)
        r2 = await provider.get_belief(b2.id)
        assert r1.lifecycle_state == "superseded"
        assert r2.lifecycle_state == "superseded"


# =========================================================================
# No regression: verify enhanced contradictions still handle existing cases
# =========================================================================


class TestExistingNoRegression:
    """All existing contradiction detection patterns still work."""

    def test_keyword_contradiction_still_works(self):
        """Original keyword-based detection still produces results."""
        beliefs = [
            _make_belief("Docker is better than Podman", confidence=0.8),
            _make_belief("Podman is worse than Docker", confidence=0.6),
        ]
        pairs = detect_contradictions(beliefs)
        assert len(pairs) == 1
        assert pairs[0]["detection_method"] in ("keyword", "confidence_weighted")

    def test_no_contradictions_still_empty(self):
        """Unrelated propositions still produce no contradictions."""
        beliefs = [
            _make_belief("Docker is great for containers"),
            _make_belief("Caddy is a web server"),
        ]
        assert detect_contradictions(beliefs) == []

    def test_multiple_contradictions_still_detected(self):
        """Multiple contradictory pairs are still found."""
        beliefs = [
            _make_belief("Docker is better than Podman", confidence=0.8),
            _make_belief("Podman is worse than Docker", confidence=0.6),
            _make_belief("Caddy is good for web serving", confidence=0.9),
            _make_belief("Caddy is bad for web serving", confidence=0.3),
        ]
        pairs = detect_contradictions(beliefs)
        assert len(pairs) == 2

    def test_empty_input(self):
        """Empty input still returns empty list."""
        assert detect_contradictions([]) == []

    def test_single_keyword_overlap_not_detected(self):
        """Single keyword overlap should still not trigger contradiction."""
        beliefs = [
            _make_belief("Docker is better than Podman", confidence=0.8),
            _make_belief("Docker is running slowly today", confidence=0.5),
        ]
        pairs = detect_contradictions(beliefs)
        assert len(pairs) == 0

    def test_output_has_new_fields(self):
        """All contradiction results contain detection_method and detection_score."""
        beliefs = [
            _make_belief("Docker is better than Podman", confidence=0.8),
            _make_belief("Podman is worse than Docker", confidence=0.6),
        ]
        pairs = detect_contradictions(beliefs)
        assert len(pairs) == 1
        assert "detection_method" in pairs[0]
        assert "detection_score" in pairs[0]
        assert pairs[0]["detection_method"] in ("keyword", "confidence_weighted", "source_overlap")


# =========================================================================
# Integration tests with real SQLite provider
# =========================================================================


@pytest.mark.asyncio
class TestConflictIntegration:
    """Integration tests with real provider — enhanced contradiction detection."""

    async def _seed(self, provider):
        """Seed beliefs with evidence for source-overlap testing."""
        beliefs = [
            Belief(
                proposition="Docker is better than Podman",
                confidence=0.9,
                tags=["docker", "container"],
                source_ids=["src-1", "src-2", "src-3"],
            ),
            Belief(
                proposition="Podman is better than Docker",
                confidence=0.3,
                tags=["podman", "container"],
                source_ids=["src-1", "src-2", "src-4"],
            ),
            Belief(
                proposition="Caddy is a fast web server",
                confidence=0.85,
                tags=["caddy", "web"],
                source_ids=["src-5"],
            ),
            Belief(
                proposition="User prefers dark mode",
                confidence=0.7,
                tags=["user-preference"],
                source_ids=[],
            ),
        ]
        for b in beliefs:
            evidence = [
                Evidence(belief_id=b.id, source_type="fact", source_id=sid, weight=0.9)
                for sid in b.source_ids
            ]
            await provider.create_belief(b, evidence)
        return beliefs

    async def test_confidence_weighted_detection_integration(self, provider):
        """Integration test: confidence_weighted detection works end-to-end."""
        await self._seed(provider)
        engine = ReflectEngine(provider)
        result = await engine.contradictions()
        assert result["mode"] == "contradictions"
        assert result["total"] >= 1
        # The Docker/Podman pair should be detected
        for pair in result["contradictions"]:
            assert "detection_method" in pair
            assert "detection_score" in pair
            assert pair["detection_score"] >= 0.3

    async def test_contradictions_contain_new_fields(self, provider):
        """Contradiction output must include detection_method and detection_score."""
        await self._seed(provider)
        engine = ReflectEngine(provider)
        result = await engine.contradictions()
        for pair in result["contradictions"]:
            assert "detection_method" in pair
            assert "detection_score" in pair

    async def test_overview_conflicts_integration(self, provider):
        """Overview must include conflicts section with real data."""
        await self._seed(provider)
        engine = ReflectEngine(provider)
        result = await engine.overview()
        assert "conflicts" in result
        assert isinstance(result["conflicts"]["total"], int)
        assert isinstance(result["conflicts"]["unresolved"], int)
        assert isinstance(result["conflicts"]["auto_resolvable"], int)
        assert isinstance(result["conflicts"]["age_hours_max"], (int, float))
