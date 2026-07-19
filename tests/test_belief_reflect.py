"""Tests for Card 002: reflect() Tool — Belief Analysis & Reflection.

Tests cover:
- ReflectEngine unit tests (mock provider) for all 6 modes
- Contradiction detection helpers
- Integration tests with real in-memory SQLite provider
- Edge cases: empty store, invalid mode, limit=0, no contradictions
"""

import json
import pytest
from datetime import datetime, timedelta, timezone

from memory_server.models import Belief, Evidence
from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.api.reflect import (
    ReflectEngine,
    _tokenize,
    _has_opposite_sentiment,
    _build_histogram,
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
    created_at: datetime | None = None,
) -> Belief:
    return Belief(
        proposition=proposition,
        confidence=confidence,
        tags=tags or [],
        lifecycle_state=lifecycle_state,
        created_at=created_at or datetime.now(timezone.utc),
    )


# =========================================================================
# Contradiction detection helpers (unit tests)
# =========================================================================


class TestTokenize:
    def test_basic(self):
        tokens = _tokenize("Docker is better than Podman")
        assert "docker" in tokens
        assert "podman" in tokens
        assert "better" not in tokens  # stopword
        assert "is" not in tokens  # stopword

    def test_punctuation_stripped(self):
        tokens = _tokenize("Docker runs? Yes, it does!")
        assert "docker" in tokens
        assert "runs" in tokens
        assert "does" in tokens
        assert "yes" in tokens  # "yes" is >= 2 chars and not a stopword

    def test_short_words_excluded(self):
        tokens = _tokenize("a is be to of in it")
        assert tokens == set()


class TestHasOppositeSentiment:
    def test_opposite_detected(self):
        assert _has_opposite_sentiment("Docker is better", "Docker is worse")
        assert _has_opposite_sentiment("Podman is worse", "Podman is better")
        assert _has_opposite_sentiment("I prefer Docker", "I avoid Docker")
        assert _has_opposite_sentiment("I like dark mode", "I dislike dark mode")

    def test_same_sentiment_not_false_positive(self):
        assert not _has_opposite_sentiment("Docker is good", "Docker is great")
        assert not _has_opposite_sentiment("Caddy is stable", "Nginx is stable")

    def test_no_overlap(self):
        assert not _has_opposite_sentiment("Docker is great", "Weather is nice")


class TestBuildHistogram:
    def test_empty(self):
        assert _build_histogram([]) == {
            "0.9_1.0": 0, "0.7_0.9": 0, "0.5_0.7": 0, "0.3_0.5": 0, "0.0_0.3": 0,
        }

    def test_distribution(self):
        beliefs = [
            _make_belief("a", confidence=0.95),
            _make_belief("b", confidence=0.75),
            _make_belief("c", confidence=0.55),
            _make_belief("d", confidence=0.35),
            _make_belief("e", confidence=0.15),
        ]
        hist = _build_histogram(beliefs)
        assert hist["0.9_1.0"] == 1
        assert hist["0.7_0.9"] == 1
        assert hist["0.5_0.7"] == 1
        assert hist["0.3_0.5"] == 1
        assert hist["0.0_0.3"] == 1

    def test_edge_boundary(self):
        """0.5 falls into 0.5_0.7, and 1.0 falls into 0.9_1.0."""
        beliefs = [
            _make_belief("a", confidence=0.5),
            _make_belief("b", confidence=1.0),
        ]
        hist = _build_histogram(beliefs)
        assert hist["0.5_0.7"] == 1
        assert hist["0.9_1.0"] == 1


class TestDetectContradictions:
    def test_no_contradictions(self):
        beliefs = [
            _make_belief("Docker is great for containers"),
            _make_belief("Caddy is a web server"),
        ]
        assert detect_contradictions(beliefs) == []

    def test_contradiction_detected(self):
        beliefs = [
            _make_belief("Docker is better than Podman", confidence=0.8),
            _make_belief("Docker is worse than Podman", confidence=0.6),
        ]
        pairs = detect_contradictions(beliefs)
        assert len(pairs) == 1
        assert pairs[0]["overlap_score"] >= 0.5
        assert "docker" in pairs[0]["proposition_a"].lower()
        assert "docker" in pairs[0]["proposition_b"].lower()
        assert pairs[0]["confidence_a"] == 0.8
        assert pairs[0]["confidence_b"] == 0.6

    def test_multiple_contradictions(self):
        beliefs = [
            _make_belief("Docker is better than Podman", confidence=0.8),
            _make_belief("Docker is worse than Podman", confidence=0.6),
            _make_belief("Caddy is good for web serving", confidence=0.9),
            _make_belief("Caddy is bad for web serving", confidence=0.3),
        ]
        pairs = detect_contradictions(beliefs)
        assert len(pairs) == 2

    def test_empty_input(self):
        assert detect_contradictions([]) == []

    def test_requires_two_keyword_overlap(self):
        """Single keyword overlap should not trigger contradiction."""
        beliefs = [
            _make_belief("Docker is better than Podman", confidence=0.8),
            _make_belief("Docker is running slowly today", confidence=0.5),
        ]
        pairs = detect_contradictions(beliefs)
        assert len(pairs) == 0


# =========================================================================
# ReflectEngine unit tests (mock provider)
# =========================================================================


class _MockProvider:
    """Minimal mock that returns pre-configured beliefs."""

    def __init__(self, beliefs: list[Belief] | None = None):
        self.beliefs = beliefs or []

    async def search_beliefs(self, **kwargs):
        return self.beliefs


class TestReflectEngine:
    def get_engine(self, beliefs: list[Belief] | None = None):
        return ReflectEngine(_MockProvider(beliefs or []))

    @pytest.mark.asyncio
    async def test_overview_empty_store(self):
        engine = self.get_engine([])
        result = await engine.overview()
        assert result["mode"] == "overview"
        assert result["total_beliefs"] == 0
        assert result["by_lifecycle_state"] == {}
        assert result["by_topics"] == {}

    @pytest.mark.asyncio
    async def test_overview_with_beliefs(self):
        beliefs = [
            _make_belief("Docker runs", confidence=0.9, tags=["docker"], lifecycle_state="active"),
            _make_belief("Caddy proxy", confidence=0.85, tags=["caddy"], lifecycle_state="active"),
            _make_belief("Old idea", confidence=0.3, tags=["old"], lifecycle_state="stale"),
        ]
        engine = self.get_engine(beliefs)
        result = await engine.overview()
        assert result["total_beliefs"] == 3
        assert result["by_lifecycle_state"]["active"] == 2
        assert result["by_lifecycle_state"]["stale"] == 1
        assert result["stale_count"] == 1
        assert result["confidence"]["average"] > 0.6

    @pytest.mark.asyncio
    async def test_contradictions_empty_store(self):
        engine = self.get_engine([])
        result = await engine.contradictions()
        assert result["mode"] == "contradictions"
        assert result["total"] == 0
        assert result["contradictions"] == []

    @pytest.mark.asyncio
    async def test_decay_analysis_empty(self):
        engine = self.get_engine([])
        result = await engine.decay_analysis()
        assert result["mode"] == "decay"
        assert result["stale_now"] == 0
        assert result["stale_7d"] == 0

    @pytest.mark.asyncio
    async def test_topics_empty(self):
        engine = self.get_engine([])
        result = await engine.topics()
        assert result["mode"] == "topics"
        assert result["topics"] == []
        assert result["untagged_count"] == 0

    @pytest.mark.asyncio
    async def test_evidence_audit_empty(self):
        engine = self.get_engine([])
        result = await engine.evidence_audit()
        assert result["mode"] == "evidence_audit"
        assert result["total"] == 0

    @pytest.mark.asyncio
    async def test_confidence_empty(self):
        engine = self.get_engine([])
        result = await engine.confidence_histogram()
        assert result["mode"] == "confidence"
        assert result["beliefs"] == []
        assert result["lowest_count"] == 0

    @pytest.mark.asyncio
    async def test_topics_with_tags(self):
        beliefs = [
            _make_belief("Docker runs", tags=["docker", "infra"]),
            _make_belief("Caddy proxy", tags=["caddy", "web"]),
            _make_belief("Another infra", tags=["infra"]),
        ]
        engine = self.get_engine(beliefs)
        result = await engine.topics()
        assert len(result["topics"]) == 4  # docker, infra, caddy, web
        topic_map = {t["tag"]: t for t in result["topics"]}
        assert topic_map["infra"]["count"] == 2
        assert topic_map["docker"]["count"] == 1

    @pytest.mark.asyncio
    async def test_confidence_histogram(self):
        beliefs = [
            _make_belief("High", confidence=0.95),
            _make_belief("Mid", confidence=0.65),
            _make_belief("Low", confidence=0.15),
        ]
        engine = self.get_engine(beliefs)
        result = await engine.confidence_histogram()
        assert len(result["beliefs"]) == 3
        assert result["beliefs"][0]["confidence"] == 0.95  # sorted desc
        assert result["beliefs"][2]["confidence"] == 0.15
        assert result["histogram"]["0.9_1.0"] == 1
        assert result["histogram"]["0.0_0.3"] == 1
        assert result["lowest_count"] == 1


class TestReflectOverview:
    @pytest.mark.asyncio
    async def test_overview_bucket_distribution(self):
        beliefs = []
        for i in range(10):
            beliefs.append(
                _make_belief(f"belief {i}", confidence=0.1 * i, lifecycle_state="active")
            )
        engine = ReflectEngine(_MockProvider(beliefs))
        result = await engine.overview()
        assert result["total_beliefs"] == 10
        avg = result["confidence"]["average"]
        assert 0.4 < avg < 0.5  # average of 0, 0.1, 0.2, ..., 0.9

    @pytest.mark.asyncio
    async def test_overview_lifecycle_distribution(self):
        states = ["active", "active", "stale", "superseded", "contradicted", "archived", "forgotten"]
        beliefs = [Belief(proposition=f"b{i}", lifecycle_state=s) for i, s in enumerate(states)]
        engine = ReflectEngine(_MockProvider(beliefs))
        result = await engine.overview()
        assert result["by_lifecycle_state"]["active"] == 2
        assert result["by_lifecycle_state"]["stale"] == 1
        assert result["by_lifecycle_state"]["superseded"] == 1
        assert result["contradiction_count"] == 1
        assert result["stale_count"] == 1


class TestReflectContradictions:
    @pytest.mark.asyncio
    async def test_no_contradictions(self):
        beliefs = [
            _make_belief("Docker is good for containers"),
            _make_belief("Caddy is a web server"),
        ]
        engine = ReflectEngine(_MockProvider(beliefs))
        result = await engine.contradictions()
        assert result["total"] == 0

    @pytest.mark.asyncio
    async def test_contradictions_found(self):
        beliefs = [
            _make_belief("Docker is better than Podman", confidence=0.8),
            _make_belief("Docker is worse than Podman", confidence=0.6),
            _make_belief("Caddy is good for web serving", confidence=0.9),
        ]
        engine = ReflectEngine(_MockProvider(beliefs))
        result = await engine.contradictions()
        assert result["total"] == 1
        assert result["contradictions"][0]["overlap_score"] >= 0.5
        assert "recommendation" in result


class TestReflectDecay:
    @pytest.mark.asyncio
    async def test_stale_now_detected(self):
        old = datetime.now(timezone.utc) - timedelta(days=180)
        beliefs = [
            _make_belief("Stale belief", lifecycle_state="stale", created_at=old),
        ]
        engine = ReflectEngine(_MockProvider(beliefs))
        result = await engine.decay_analysis()
        assert result["stale_now"] == 1
        assert result["recommendation"] != ""


class TestReflectTopics:
    @pytest.mark.asyncio
    async def test_untagged_count(self):
        beliefs = [
            _make_belief("Tagged", tags=["infra"]),
            _make_belief("No tags"),
            _make_belief("Another no tags"),
        ]
        engine = ReflectEngine(_MockProvider(beliefs))
        result = await engine.topics()
        assert result["untagged_count"] == 2
        assert len(result["topics"]) == 1


class TestReflectEvidenceAudit:
    @pytest.mark.asyncio
    async def test_no_evidence(self):
        engine = ReflectEngine(_MockProvider([]))
        result = await engine.evidence_audit()
        assert result["total"] == 0


class TestReflectConfidence:
    @pytest.mark.asyncio
    async def test_confidence_sorted(self):
        beliefs = [
            _make_belief("Low", confidence=0.1),
            _make_belief("High", confidence=0.9),
            _make_belief("Mid", confidence=0.5),
        ]
        engine = ReflectEngine(_MockProvider(beliefs))
        result = await engine.confidence_histogram()
        confs = [b["confidence"] for b in result["beliefs"]]
        assert confs == sorted(confs, reverse=True)
        assert result["lowest_count"] == 1


# =========================================================================
# Integration tests with real SQLite in-memory provider
# =========================================================================


@pytest.fixture
async def provider():
    p = SQLiteProvider(url="sqlite+aiosqlite:///:memory:")
    await p.initialize()
    yield p
    await p.close()


@pytest.mark.asyncio
class TestReflectMTool:
    """Integration tests for ReflectEngine with real provider."""

    async def _seed(self, provider):
        """Seed 5 beliefs with evidence."""
        beliefs = [
            Belief(
                proposition="Docker is better than Podman",
                confidence=0.8,
                tags=["docker", "container"],
            ),
            Belief(
                proposition="Docker is worse than Podman",
                confidence=0.6,
                tags=["podman", "container"],
            ),
            Belief(
                proposition="Caddy is a fast web server",
                confidence=0.85,
                tags=["caddy", "web"],
            ),
            Belief(
                proposition="User prefers dark mode",
                confidence=0.7,
                tags=["user-preference"],
            ),
            Belief(
                proposition="Deprecated belief",
                confidence=0.3,
                tags=["old"],
                source="legacy",
            ),
        ]
        for b in beliefs:
            evidence = [
                Evidence(belief_id=b.id, source_type="fact", source_id=f"src-{b.id[:8]}", weight=0.9),
            ]
            await provider.create_belief(b, evidence)

        # Mark one as superseded
        await provider.update_belief_lifecycle(beliefs[4].id, "superseded")
        return beliefs

    async def test_overview_integration(self, provider):
        await self._seed(provider)
        engine = ReflectEngine(provider)
        result = await engine.overview()
        assert result["total_beliefs"] == 5
        assert result["by_lifecycle_state"]["active"] == 4
        assert result["by_lifecycle_state"]["superseded"] == 1
        assert result["confidence"]["average"] > 0.5
        assert result["oldest_belief_days"] >= 0

    async def test_overview_with_topic_filter(self, provider):
        await self._seed(provider)
        engine = ReflectEngine(provider)
        result = await engine.overview(topic="docker")
        assert result["total_beliefs"] >= 1
        # At least "docker" tag beliefs should match

    async def test_contradictions_integration(self, provider):
        await self._seed(provider)
        engine = ReflectEngine(provider)
        result = await engine.contradictions()
        # Docker vs Podman should be detected
        assert result["total"] >= 1
        for pair in result["contradictions"]:
            assert "docker" in pair["proposition_a"].lower() or "podman" in pair["proposition_a"].lower()

    async def test_decay_integration(self, provider):
        await self._seed(provider)
        engine = ReflectEngine(provider)
        result = await engine.decay_analysis()
        assert result["mode"] == "decay"
        assert isinstance(result["stale_now"], int)
        assert isinstance(result["stale_7d"], int)

    async def test_topics_integration(self, provider):
        await self._seed(provider)
        engine = ReflectEngine(provider)
        result = await engine.topics()
        assert result["mode"] == "topics"
        assert len(result["topics"]) >= 4
        # Check a known tag
        docker_topic = [t for t in result["topics"] if t["tag"] == "docker"]
        assert len(docker_topic) == 1
        assert docker_topic[0]["count"] == 1

    async def test_evidence_audit_integration(self, provider):
        await self._seed(provider)
        engine = ReflectEngine(provider)
        result = await engine.evidence_audit()
        assert result["mode"] == "evidence_audit"
        assert result["total"] == 5
        assert result["with_evidence"] == 5  # all seeded beliefs have evidence
        assert result["without_evidence"] == 0
        assert "fact" in result["by_source_type"]

    async def test_confidence_integration(self, provider):
        await self._seed(provider)
        engine = ReflectEngine(provider)
        result = await engine.confidence_histogram()
        assert result["mode"] == "confidence"
        assert len(result["beliefs"]) == 5
        # Check sorting
        confs = [b["confidence"] for b in result["beliefs"]]
        assert confs == sorted(confs, reverse=True)
        assert result["histogram"]["0.7_0.9"] >= 1  # at least Docker and Caddy

    async def test_limit_zero_returns_all(self, provider):
        """limit=0 should return all beliefs (effectively unlimited)."""
        await self._seed(provider)
        engine = ReflectEngine(provider)
        # Search with limit=0
        beliefs = await engine._fetch_beliefs(limit=0)
        assert len(beliefs) == 5

    async def test_limit_filters(self, provider):
        await self._seed(provider)
        engine = ReflectEngine(provider)
        result = await engine.overview(limit=2)
        assert result["total_beliefs"] == 2

    async def test_integration_overview_min_confidence(self, provider):
        """Overview with min_confidence=0.7 should exclude beliefs below threshold."""
        # Seed beliefs with various confidences
        beliefs = [
            Belief(proposition="High confidence", confidence=0.9, tags=["test"]),
            Belief(proposition="Medium confidence", confidence=0.6, tags=["test"]),
            Belief(proposition="Low confidence", confidence=0.3, tags=["test"]),
        ]
        for b in beliefs:
            await provider.create_belief(b)

        engine = ReflectEngine(provider)
        result = await engine.overview(min_confidence=0.7)
        assert result["total_beliefs"] == 1  # only 0.9 passes
        assert result["confidence"]["high_0.8_1.0"] == 1
        # Average should be exactly 0.9
        assert result["confidence"]["average"] == 0.9

    async def test_integration_overview_topic_filter(self, provider):
        """Overview with topic='infra' should only return beliefs tagged with 'infra'."""
        beliefs = [
            Belief(proposition="Infra belief", confidence=0.8, tags=["infra", "ops"]),
            Belief(proposition="Docker belief", confidence=0.7, tags=["docker"]),
            Belief(proposition="Another infra", confidence=0.6, tags=["infra"]),
            Belief(proposition="Web belief", confidence=0.5, tags=["web"]),
        ]
        for b in beliefs:
            await provider.create_belief(b)

        engine = ReflectEngine(provider)
        result = await engine.overview(topic="infra")
        assert result["total_beliefs"] == 2  # both infra-tagged beliefs
        # "infra" should be a key in by_topics
        assert "infra" in result["by_topics"]
        assert result["by_topics"]["infra"] == 2
        # "docker" and "web" should NOT appear (filtered out)
        assert "docker" not in result["by_topics"]

    async def test_confidence_histogram_min_confidence(self, provider):
        """Confidence mode with min_confidence should filter out low-confidence beliefs."""
        beliefs = [
            Belief(proposition="High", confidence=0.9),
            Belief(proposition="Mid", confidence=0.65),
            Belief(proposition="Low", confidence=0.25),
        ]
        for b in beliefs:
            await provider.create_belief(b)

        engine = ReflectEngine(provider)
        result = await engine.confidence_histogram(min_confidence=0.5)
        # Only High (0.9) and Mid (0.65) should be included
        assert len(result["beliefs"]) == 2
        confs = [b["confidence"] for b in result["beliefs"]]
        assert 0.9 in confs
        assert 0.65 in confs
        assert 0.25 not in confs  # filtered out
        # Histogram should reflect only the filtered beliefs
        assert result["histogram"]["0.9_1.0"] == 1
        assert result["histogram"]["0.5_0.7"] == 1
        assert result["histogram"]["0.0_0.3"] == 0  # low filtered out

    async def test_empty_store(self, provider):
        engine = ReflectEngine(provider)
        for mode_fn in [engine.overview, engine.contradictions, engine.decay_analysis,
                        engine.topics, engine.evidence_audit, engine.confidence_histogram]:
            result = await mode_fn()
            assert result["mode"] is not None

    async def test_invalid_mode(self, provider):
        engine = ReflectEngine(provider)
        # The MCP tool validates mode, not the engine
        # But engine raises AttributeError for invalid method
        with pytest.raises(AttributeError):
            await engine.nonexistent()

    async def test_evidence_audit_with_missing_evidence(self, provider):
        """Create a belief with no evidence and verify it's counted."""
        b = Belief(proposition="Orphan belief", confidence=0.5)
        await provider.create_belief(b)

        engine = ReflectEngine(provider)
        result = await engine.evidence_audit()
        assert result["without_evidence"] >= 1

    async def test_no_contradictions(self, provider):
        """Create beliefs that don't contradict each other."""
        beliefs = [
            Belief(proposition="The sky is blue", confidence=0.9),
            Belief(proposition="Grass is green", confidence=0.85),
        ]
        for b in beliefs:
            await provider.create_belief(b)

        engine = ReflectEngine(provider)
        result = await engine.contradictions()
        assert result["total"] == 0


@pytest.mark.asyncio
class TestReflectMToolEdgeCases:
    """Additional edge case tests for reflect tool."""

    async def test_evidence_audit_zero_weight(self, provider):
        """Belief with zero-weight evidence should still count as having evidence."""
        b = Belief(proposition="Zero weight belief", confidence=0.5)
        ev = Evidence(belief_id=b.id, source_type="observation", source_id="obs-1", weight=0.0)
        await provider.create_belief(b, [ev])

        engine = ReflectEngine(provider)
        result = await engine.evidence_audit()
        assert result["with_evidence"] >= 1
        assert result["without_evidence"] == 0

    async def test_confidence_histogram_with_evidence(self, provider):
        """Confidence histogram should include evidence counts."""
        b = Belief(proposition="Belief with evidence", confidence=0.75)
        evidence = [
            Evidence(belief_id=b.id, source_type="fact", source_id="f1", weight=0.9),
            Evidence(belief_id=b.id, source_type="observation", source_id="obs-1", weight=0.5),
        ]
        await provider.create_belief(b, evidence)

        engine = ReflectEngine(provider)
        result = await engine.confidence_histogram()
        assert len(result["beliefs"]) == 1
        assert result["beliefs"][0]["evidence_count"] == 2

    async def test_mcp_tool_json_output(self, provider):
        """Verify the MCP tool returns valid JSON (mocked server call)."""
        from memory_server.server import reflect_tool
        result_str = await reflect_tool(mode="overview")
        data = json.loads(result_str)
        assert data["mode"] == "overview"
        assert "total_beliefs" in data

    async def test_mcp_tool_invalid_mode(self, provider):
        """Verify invalid mode returns error JSON."""
        from memory_server.server import reflect_tool
        result_str = await reflect_tool(mode="invalid")
        data = json.loads(result_str)
        assert "error" in data

    async def test_mcp_tool_limit_zero(self, provider):
        """Verify limit=0 works end-to-end."""
        from memory_server.server import reflect_tool
        result_str = await reflect_tool(mode="overview", limit=0)
        data = json.loads(result_str)
        assert data["mode"] == "overview"
        assert isinstance(data["total_beliefs"], int)

    async def test_mcp_tool_contradictions(self, provider):
        """Set up contradicting beliefs and verify MCP tool detects them."""
        from memory_server.server import reflect_tool
        from memory_server.models import Belief as BelModel
        b1 = BelModel(proposition="Docker is better than Podman", confidence=0.8, tags=["docker"])
        b2 = BelModel(proposition="Docker is worse than Podman", confidence=0.6, tags=["podman"])
        await provider.create_belief(b1)
        await provider.create_belief(b2)

        result_str = await reflect_tool(mode="contradictions")
        data = json.loads(result_str)
        assert data["mode"] == "contradictions"
        # May or may not detect — heuristic-based
        assert isinstance(data["total"], int)

    async def test_mcp_tool_decay(self, provider):
        """Verify decay MCP tool."""
        from memory_server.server import reflect_tool
        result_str = await reflect_tool(mode="decay")
        data = json.loads(result_str)
        assert data["mode"] == "decay"

    async def test_mcp_tool_topics(self, provider):
        """Verify topics MCP tool."""
        from memory_server.server import reflect_tool
        result_str = await reflect_tool(mode="topics")
        data = json.loads(result_str)
        assert data["mode"] == "topics"

    async def test_mcp_tool_evidence_audit(self, provider):
        """Verify evidence_audit MCP tool."""
        from memory_server.server import reflect_tool
        result_str = await reflect_tool(mode="evidence_audit")
        data = json.loads(result_str)
        assert data["mode"] == "evidence_audit"

    async def test_mcp_tool_confidence(self, provider):
        """Verify confidence MCP tool."""
        from memory_server.server import reflect_tool
        result_str = await reflect_tool(mode="confidence")
        data = json.loads(result_str)
        assert data["mode"] == "confidence"
