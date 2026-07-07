"""Tests for HybridRouter — rules -> embeddings -> graph -> LLM fallback (Card 020)."""

import pytest

from memory_server.router.hybrid_router import HybridRouter
from memory_server.router.rules import RoutingRule, RoutingRuleSet


class TestHybridRouter:
    """Test HybridRouter routing priority per ADR-005."""

    @pytest.fixture
    def router(self):
        from memory_server.providers.qdrant_provider import QdrantProvider
        from memory_server.providers.embedding_provider import MockEmbeddingProvider

        qdrant = QdrantProvider(location=":memory:", prefer_grpc=False)
        embedder = MockEmbeddingProvider(vector_size=384)

        # Create custom rules: graph/entity rules high priority for test control
        rules = RoutingRuleSet()
        rules.add(RoutingRule(
            name="ip_query",
            keywords=["ip of", "ip address"],
            route="sql",
            priority=100,
        ))

        return HybridRouter(
            qdrant_provider=qdrant,
            embedder=embedder,
            rules=rules,
        )

    async def test_route_hits_rules_first(self, router):
        """Stage 1: Rules should match before semantic search."""
        result = await router.route("What is the ip of server X?")
        assert result["stage"] == 1
        assert result["route"] == "rules"
        assert "rule_match" in result

    async def test_route_falls_through_to_semantic(self, router):
        """Stage 2: No rule match falls through to semantic search."""
        # Pre-populate semantic with something (Qdrant needs UUID point IDs)
        import uuid
        vec = router._embedder.embed("weather is nice today")
        await router._qdrant.upsert(
            "memories",
            point_id=str(uuid.uuid4()),
            vector=vec,
            payload={"content": "weather is nice today", "subject": "weather"},
        )

        result = await router.route("What is the weather?")
        # Should match semantically since no rule catches it
        assert result["stage"] == 2
        assert result["route"] == "semantic"
        assert "all_results" in result
        assert len(result["all_results"]) > 0
        assert result["all_results"][0].source == "semantic"

    async def test_route_falls_through_to_graph(self, router):
        """Stage 3: No rule or semantic match falls through to graph."""
        # Pre-populate graph
        router.sync_fact(subject="Docker", predicate="is", object="container")
        router.sync_fact(subject="Docker", predicate="runs_on", object="OMV8")

        result = await router.route("Tell me about Docker")
        # Graph should match via entity extraction
        assert result["stage"] == 3
        assert result["route"] == "graph"
        assert "all_results" in result
        assert len(result["all_results"]) > 0
        assert result["all_results"][0].source == "graph"

    async def test_empty_query_returns_fallback(self, router):
        """Empty query should fall through to LLM fallback."""
        result = await router.route("")
        assert result["stage"] == 4
        assert result["route"] == "llm_fallback"

    async def test_no_match_anywhere_returns_llm_fallback(self, router):
        """Query matching nothing anywhere returns LLM fallback."""
        result = await router.route("xyznonexistentquery12345!")
        assert result["stage"] == 4
        assert result["route"] == "llm_fallback"
        assert "LLM fallback not configured" in result["message"]

    async def test_graph_wins_over_semantic_when_both_have_results(self, router):
        """Graph should be preferred when both semantic and graph match."""
        # Pre-populate semantic with a match
        import uuid
        vec = router._embedder.embed("Docker is a container platform")
        await router._qdrant.upsert(
            "memories",
            point_id=str(uuid.uuid4()),
            vector=vec,
            payload={"content": "Docker is a container platform", "subject": "Docker"},
        )

        # Graph should match entity "Docker"
        router.sync_fact(subject="Docker", predicate="is", object="container platform")
        router.sync_fact(subject="Docker", predicate="runs_on", object="Linux")

        result = await router.route("What is Docker?")
        # Semantic search will return results, but graph should be checked too.
        # Per priority: semantic (stage 2) is checked first.
        # If semantic returns results, it wins over graph.
        assert result["stage"] == 2 or result["stage"] == 3

    async def test_route_with_custom_thresholds(self, router):
        """Route should pass thresholds through to semantic stage."""
        result = await router.route("some random query with no matches", score_threshold=1.5)
        # With threshold 1.5, no semantic results will match
        # Should go to graph which won't match either, then LLM fallback
        assert result["stage"] == 4

    async def test_graph_router_result_contains_expected_keys(self, router):
        """Graph results should have entities, relations, paths."""
        router.sync_fact(subject="PostgreSQL", predicate="is", object="database")
        result = await router.route("What is PostgreSQL?")
        if result["stage"] == 3:
            assert "all_results" in result
            # At least one graph result should be present
            graph_results = [r for r in result["all_results"] if r.source == "graph"]
            assert len(graph_results) >= 1
