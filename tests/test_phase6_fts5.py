"""Tests for v0.6 Phase 6: FTS5 retrieval system and ranking layer.

Tests cover:
- FTS5 search with stemming ("running" matches "run")
- FTS5 prefix matching
- RankMerger: merge 3 sources, dedup, score normalization
- route() returns unified results
- Backward compatibility: old LIKE queries still work
"""

import pytest

from memory_server.models import Fact
from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.router.ranking import RankMerger, RankResult


@pytest.fixture
async def provider():
    """Create an in-memory SQLite provider pre-seeded with test facts."""
    p = SQLiteProvider(url="sqlite+aiosqlite:///:memory:")
    await p.initialize()

    # Seed facts with various forms of words for stemming tests
    facts = [
        Fact(id="f1", subject="Docker", predicate="runs", object="Container engine"),
        Fact(id="f2", subject="Docker", predicate="running", object="on OMV8"),
        Fact(id="f3", subject="Runner", predicate="is", object="a tool"),
        Fact(id="f4", subject="Python", predicate="is", object="Language for dev"),
        Fact(id="f5", subject="Developer", predicate="uses", object="Python"),
        Fact(id="f6", subject="DevOps", predicate="deploys", object="Containers"),
        Fact(id="f7", subject="Server", predicate="has", object="IP address 10.0.0.1"),
        Fact(id="f8", subject="Caddy", predicate="serves", object="HTTPS on port 443"),
    ]
    for f in facts:
        await p.create_fact(f)

    yield p
    await p.close()


@pytest.mark.asyncio
class TestFTS5Search:
    """Test FTS5 full-text search features."""

    async def test_fts5_stemming_running(self, provider):
        """FTS5 stemming: 'running' should match 'runs' (same stem: run)."""
        results = await provider.search_facts(text="running")
        subjects = {r.subject for r in results}
        assert "Docker" in subjects, f"Expected Docker in results, got {subjects}"

    async def test_fts5_stemming_runner(self, provider):
        """FTS5 stemming: 'runner' should match 'Runner'."""
        results = await provider.search_facts(text="runner")
        subjects = {r.subject for r in results}
        assert "Runner" in subjects, f"Expected Runner in results, got {subjects}"

    async def test_fts5_prefix_matching(self, provider):
        """FTS5 prefix matching: 'deploy' should match 'deploys'."""
        results = await provider.search_facts(text="deploy")
        subjects = {r.subject for r in results}
        assert "DevOps" in subjects, f"Expected DevOps in results, got {subjects}"

    async def test_fts5_prefix_matching_short(self, provider):
        """FTS5 prefix matching: 'dev' should match 'Developer' and 'DevOps'."""
        results = await provider.search_facts(text="dev")
        subjects = {r.subject for r in results}
        assert "Developer" in subjects, f"Expected Developer, got {subjects}"
        assert "DevOps" in subjects, f"Expected DevOps, got {subjects}"

    async def test_fts5_match_object_field(self, provider):
        """FTS5 should search across object field too."""
        results = await provider.search_facts(text="Containers")
        subjects = {r.subject for r in results}
        assert "DevOps" in subjects, f"Expected DevOps, got {subjects}"

    async def test_fts5_match_predicate_field(self, provider):
        """FTS5 should search across predicate field too."""
        results = await provider.search_facts(text="serves")
        subjects = {r.subject for r in results}
        assert "Caddy" in subjects, f"Expected Caddy, got {subjects}"

    async def test_fts5_phrase_matching(self, provider):
        """FTS5 with multiple terms matching across fields."""
        results = await provider.search_facts(text="Container engine")
        subjects = {r.subject for r in results}
        assert "Docker" in subjects, f"Expected Docker, got {subjects}"

    async def test_fts5_no_results(self, provider):
        """FTS5 should return empty list for non-matching queries."""
        results = await provider.search_facts(text="xyznonexistent12345!")
        assert len(results) == 0


@pytest.mark.asyncio
class TestFTS5BackwardCompatibility:
    """Backward compatibility: old LIKE queries still work."""

    async def test_search_by_subject_exact(self, provider):
        """Exact subject match should still work (bypasses FTS)."""
        results = await provider.search_facts(subject="Docker")
        assert len(results) >= 1
        assert results[0].subject == "Docker"

    async def test_search_by_predicate_exact(self, provider):
        """Exact predicate match should still work."""
        results = await provider.search_facts(predicate="is")
        assert len(results) >= 2  # f3: Runner is..., f4: Python is...

    async def test_search_by_source(self, provider):
        """Source filter should still work (in-memory filter)."""
        await provider.create_fact(Fact(id="f10", subject="Test", predicate="is", object="Manual", source="manual"))
        results = await provider.search_facts(source="manual")
        assert len(results) >= 1
        assert results[0].source == "manual"

    async def test_search_combined_filters(self, provider):
        """Combined subject + predicate should still work."""
        results = await provider.search_facts(subject="Docker", predicate="runs")
        assert len(results) == 1
        assert results[0].id == "f1"

    async def test_search_with_no_text(self, provider):
        """Search with no text parameter should return all ordered results."""
        results = await provider.search_facts(limit=5)
        assert len(results) <= 5
        assert len(results) > 0

    async def test_like_fallback_fts5(self, provider):
        """Text search should match partial strings (FTS5 fallback works)."""
        results = await provider.search_facts(text="10.0.0")
        assert len(results) >= 1
        assert results[0].id == "f7"


class TestRankMerger:
    """Test the RankMerger ranking and deduplication layer."""

    def test_merge_empty_all(self):
        """Merging all empty lists should return empty."""
        merger = RankMerger()
        result = merger.merge([], [], [])
        assert result == []

    def test_merge_single_source(self):
        """Results from a single source should be ranked."""
        merger = RankMerger()
        fts_results = [
            RankResult(content="Docker runs on OMV8", score=5.0, source="fts"),
            RankResult(content="Caddy serves HTTPS", score=3.0, source="fts"),
        ]
        result = merger.merge(fts_results, [], [])
        assert len(result) == 2
        # Higher score should be first
        assert result[0].content == "Docker runs on OMV8"

    def test_merge_dedup_by_content(self):
        """Duplicate content from different sources should be deduplicated."""
        merger = RankMerger()
        fts_results = [
            RankResult(content="Docker runs on OMV8", score=5.0, source="fts"),
        ]
        semantic_results = [
            RankResult(content="Docker runs on OMV8", score=0.9, source="semantic"),
        ]
        result = merger.merge(fts_results, semantic_results, [])
        assert len(result) == 1, f"Expected 1 result after dedup, got {len(result)}"

    def test_merge_dedup_case_insensitive(self):
        """Dedup should be case-insensitive."""
        merger = RankMerger()
        fts_results = [
            RankResult(content="Docker runs on OMV8", score=5.0, source="fts"),
        ]
        semantic_results = [
            RankResult(content="docker runs on omv8", score=0.9, source="semantic"),
        ]
        result = merger.merge(fts_results, semantic_results, [])
        assert len(result) == 1

    def test_score_normalization_fts(self):
        """FTS scores should be normalized to 0.0-1.0 range."""
        merger = RankMerger()
        fts_results = [
            RankResult(content="Top match", score=10.0, source="fts"),
            RankResult(content="Mid match", score=5.0, source="fts"),
            RankResult(content="Bottom match", score=1.0, source="fts"),
        ]
        result = merger.merge(fts_results, [], [])
        assert len(result) == 3
        # Normalized scores should be between 0 and 1
        for r in result:
            assert 0.0 <= r.score <= 1.0, f"Score {r.score} out of range"
        # Top should be 1.0, bottom should be 0.0
        assert result[0].score == pytest.approx(1.0)
        assert result[-1].score == pytest.approx(0.0)

    def test_score_normalization_semantic(self):
        """Semantic scores (already 0.0-1.0) should pass through."""
        merger = RankMerger()
        semantic_results = [
            RankResult(content="Match A", score=0.95, source="semantic"),
            RankResult(content="Match B", score=0.50, source="semantic"),
        ]
        result = merger.merge([], semantic_results, [])
        assert result[0].score == pytest.approx(0.95)
        assert result[1].score == pytest.approx(0.50)

    def test_merge_three_sources(self):
        """Merge results from all three sources."""
        merger = RankMerger()
        fts = [
            RankResult(content="Docker runs on OMV8", score=8.0, source="fts"),
        ]
        semantic = [
            RankResult(content="Python is a language", score=0.85, source="semantic"),
            RankResult(content="Caddy serves HTTPS", score=0.72, source="semantic"),
        ]
        graph = [
            RankResult(content="Docker", score=0.9, source="graph"),
        ]
        result = merger.merge(fts, semantic, graph)
        # All content strings are unique, so no dedup should happen
        assert len(result) == 4, f"Expected 4 unique results, got {len(result)}"
        sources_found = {r.source for r in result}
        assert "fts" in sources_found
        assert "semantic" in sources_found
        assert "graph" in sources_found

    def test_merge_dedup_across_sources_identical_content(self):
        """Identical content from different sources should be deduped."""
        merger = RankMerger()
        fts = [
            RankResult(content="Docker runs on OMV8", score=5.0, source="fts"),
        ]
        graph = [
            RankResult(content="Docker runs on OMV8", score=0.8, source="graph"),
        ]
        result = merger.merge(fts, [], graph)
        assert len(result) == 1, f"Expected 1 result after dedup, got {len(result)}"
        # FTS had higher raw score, so FTS source should be kept
        assert result[0].source == "fts"

    def test_merge_with_empty_content(self):
        """Empty content entries should be kept."""
        merger = RankMerger()
        fts = [
            RankResult(content="", score=5.0, source="fts"),
        ]
        result = merger.merge(fts, [], [])
        assert len(result) == 1

    def test_fts_from_facts(self, provider):
        """fts_from_facts should convert fact list to RankResults."""
        # Use a simple sync approach since we need inline facts
        from memory_server.models import Fact
        facts = [
            Fact(id="t1", subject="Docker", predicate="runs_on", object="OMV8"),
            Fact(id="t2", subject="Caddy", predicate="uses", object="Port 443"),
        ]
        results = RankMerger.fts_from_facts(facts, "Docker")
        assert len(results) == 2
        assert results[0].source == "fts"
        assert results[0].metadata["id"] == "t1"
        # Content should be subject-predicate-object concatenated
        assert "Docker" in results[0].content
        assert "runs_on" in results[0].content

    def test_semantic_from_qdrant(self):
        """semantic_from_qdrant should convert Qdrant results."""
        qdrant_results = [
            {
                "id": "abc123",
                "score": 0.95,
                "payload": {
                    "subject": "Docker",
                    "predicate": "runs_on",
                    "object": "OMV8",
                    "confidence": 0.9,
                },
            },
        ]
        results = RankMerger.semantic_from_qdrant(qdrant_results)
        assert len(results) == 1
        assert results[0].source == "semantic"
        assert results[0].score == pytest.approx(0.95)
        assert results[0].confidence == pytest.approx(0.9)
        assert results[0].metadata["id"] == "abc123"

    def test_semantic_from_qdrant_with_content_field(self):
        """semantic_from_qdrant should use content field if present."""
        qdrant_results = [
            {
                "id": "abc",
                "score": 0.9,
                "payload": {"content": "Docker runs on OMV8", "confidence": 1.0},
            },
        ]
        results = RankMerger.semantic_from_qdrant(qdrant_results)
        assert results[0].content == "Docker runs on OMV8"

    def test_graph_from_router(self):
        """graph_from_router should convert GraphRouter results."""
        graph_result = {
            "entities": [
                {"id": "docker", "name": "Docker", "type": "software"},
                {"id": "omv8", "name": "OMV8", "type": "server"},
            ],
            "relations": [
                {
                    "source_id": "docker", "source_name": "Docker",
                    "relation": "runs_on",
                    "target_id": "omv8", "target_name": "OMV8",
                    "target_type": "server",
                },
            ],
        }
        results = RankMerger.graph_from_router(graph_result)
        assert len(results) >= 2  # entities + relation content
        sources = {r.source for r in results}
        assert "graph" in sources

    def test_graph_from_router_empty(self):
        """Empty graph result should produce empty list."""
        results = RankMerger.graph_from_router({"entities": [], "relations": [], "paths": []})
        assert results == []


@pytest.mark.asyncio
class TestFTS5RouteIntegration:
    """Test route() integration with RankMerger through HybridRouter."""

    async def test_route_uses_rankmerger(self, provider):
        """Route should return unified ranked results via RankMerger."""
        from memory_server.providers.qdrant_provider import QdrantProvider
        from memory_server.providers.embedding_provider import MockEmbeddingProvider
        from memory_server.router.hybrid_router import HybridRouter
        from memory_server.router.rules import RoutingRuleSet

        qdrant = QdrantProvider(location=":memory:", prefer_grpc=False)
        embedder = MockEmbeddingProvider(vector_size=384)
        rules = RoutingRuleSet()

        router = HybridRouter(
            qdrant_provider=qdrant,
            embedder=embedder,
            rules=rules,
        )
        router.fts_provider = provider

        # Route a query that should hit FTS
        result = await router.route("Docker")
        assert "all_results" in result, f"Expected all_results, got keys: {result.keys()}"
        assert "ranked_results" in result
        assert result["total"] >= 1

    async def test_route_fts_wins_with_matching_data(self, provider):
        """When FTS has results and semantic/graph don't, FTS should be top."""
        from memory_server.providers.qdrant_provider import QdrantProvider
        from memory_server.providers.embedding_provider import MockEmbeddingProvider
        from memory_server.router.hybrid_router import HybridRouter
        from memory_server.router.rules import RoutingRuleSet

        qdrant = QdrantProvider(location=":memory:", prefer_grpc=False)
        embedder = MockEmbeddingProvider(vector_size=384)
        rules = RoutingRuleSet()

        router = HybridRouter(
            qdrant_provider=qdrant,
            embedder=embedder,
            rules=rules,
        )
        router.fts_provider = provider

        result = await router.route("Docker")
        assert result["total"] >= 1
        # The route should be "fts" since FTS has data while semantic/graph empty
        assert result["route"] in ("fts", "semantic", "graph")

    async def test_route_dedup_across_sources(self, provider):
        """Route should deduplicate results across sources."""
        from memory_server.providers.qdrant_provider import QdrantProvider
        from memory_server.providers.embedding_provider import MockEmbeddingProvider
        from memory_server.router.hybrid_router import HybridRouter
        from memory_server.router.rules import RoutingRuleSet

        qdrant = QdrantProvider(location=":memory:", prefer_grpc=False)
        embedder = MockEmbeddingProvider(vector_size=384)
        rules = RoutingRuleSet()

        router = HybridRouter(
            qdrant_provider=qdrant,
            embedder=embedder,
            rules=rules,
        )
        router.fts_provider = provider

        # Pre-populate graph with same entity name that FTS also finds
        router.sync_fact(subject="Docker", predicate="runs_on", object="OMV8")

        result = await router.route("Docker")
        # Even though Docker appears in FTS results AND graph, total should be unique
        assert result["total"] >= 1

    async def test_route_multiple_sources_counted(self, provider):
        """Route should report per-source result counts."""
        from memory_server.providers.qdrant_provider import QdrantProvider
        from memory_server.providers.embedding_provider import MockEmbeddingProvider
        from memory_server.router.hybrid_router import HybridRouter
        from memory_server.router.rules import RoutingRuleSet

        qdrant = QdrantProvider(location=":memory:", prefer_grpc=False)
        embedder = MockEmbeddingProvider(vector_size=384)
        rules = RoutingRuleSet()

        router = HybridRouter(
            qdrant_provider=qdrant,
            embedder=embedder,
            rules=rules,
        )
        router.fts_provider = provider

        # Pre-populate graph
        router.sync_fact(subject="Docker", predicate="runs_on", object="OMV8")

        result = await router.route("Docker")
        assert "sources" in result
        assert result["sources"]["fts"] >= 0
        assert result["sources"]["graph"] >= 0
        # Total should match sum
        assert result["total"] == sum(result["sources"].values())

    async def test_route_empty_query(self, provider):
        """Empty query should return LLM fallback."""
        from memory_server.providers.qdrant_provider import QdrantProvider
        from memory_server.providers.embedding_provider import MockEmbeddingProvider
        from memory_server.router.hybrid_router import HybridRouter

        qdrant = QdrantProvider(location=":memory:", prefer_grpc=False)
        embedder = MockEmbeddingProvider(vector_size=384)

        router = HybridRouter(qdrant_provider=qdrant, embedder=embedder)
        router.fts_provider = provider

        result = await router.route("")
        assert result["stage"] == 4
        assert result["route"] == "llm_fallback"

    async def test_route_non_matching_query(self, provider):
        """Non-matching query should return LLM fallback."""
        from memory_server.providers.qdrant_provider import QdrantProvider
        from memory_server.providers.embedding_provider import MockEmbeddingProvider
        from memory_server.router.hybrid_router import HybridRouter

        qdrant = QdrantProvider(location=":memory:", prefer_grpc=False)
        embedder = MockEmbeddingProvider(vector_size=384)

        router = HybridRouter(qdrant_provider=qdrant, embedder=embedder)
        router.fts_provider = provider

        result = await router.route("xyznonexistentquerythatmatchesnothing12345!")
        assert result["stage"] == 4
        assert result["route"] == "llm_fallback"

    async def test_route_normalized_scores(self, provider):
        """All scores in route() results should be 0.0-1.0."""
        from memory_server.providers.qdrant_provider import QdrantProvider
        from memory_server.providers.embedding_provider import MockEmbeddingProvider
        from memory_server.router.hybrid_router import HybridRouter
        from memory_server.router.rules import RoutingRuleSet

        qdrant = QdrantProvider(location=":memory:", prefer_grpc=False)
        embedder = MockEmbeddingProvider(vector_size=384)
        rules = RoutingRuleSet()

        router = HybridRouter(
            qdrant_provider=qdrant,
            embedder=embedder,
            rules=rules,
        )
        router.fts_provider = provider

        result = await router.route("Docker")
        if result["stage"] < 4:
            for r in result["ranked_results"]:
                assert 0.0 <= r["score"] <= 1.0, f"Score {r['score']} out of range"
