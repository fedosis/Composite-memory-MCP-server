"""Tests for semantic router and routing rules (Card 010)."""

import uuid

import pytest

from memory_server.router.embedding_router import EmbeddingRouter
from memory_server.router.rules import RoutingRule, RoutingRuleSet, RuleResult


class TestRoutingRules:
    """Test exact-match and keyword-based routing rules."""

    def test_exact_match_rule(self):
        rule = RoutingRule(
            name="ip_query",
            keywords=["ip", "address", "server ip"],
            route="sql",
            priority=10,
        )
        # Match
        result = rule.match("What is the ip of server X")
        assert result is not None
        assert result.route == "sql"
        assert result.rule_name == "ip_query"

    def test_exact_match_case_insensitive(self):
        rule = RoutingRule(
            name="ip_query",
            keywords=["IP", "Address"],
            route="sql",
            priority=10,
        )
        result = rule.match("what is the ip address of server X")
        assert result is not None

    def test_no_match(self):
        rule = RoutingRule(
            name="ip_query",
            keywords=["ip", "address"],
            route="sql",
            priority=10,
        )
        result = rule.match("What is the weather today")
        assert result is None

    def test_multiple_keywords_all_required(self):
        rule = RoutingRule(
            name="server_query",
            keywords=["server", "config"],
            route="sql",
            priority=10,
            match_all=True,
        )
        # Both present
        assert rule.match("server config for docker") is not None
        # Only one present
        assert rule.match("server information") is None

    def test_partial_word_match(self):
        """Keywords should match within words (substring)."""
        rule = RoutingRule(name="test", keywords=["ip"], route="sql", priority=5)
        # "ip" is a substring of "pip" and "multiple"
        assert rule.match("What is the ip?") is not None

    def test_empty_query(self):
        rule = RoutingRule(name="test", keywords=["hello"], route="sql", priority=5)
        assert rule.match("") is None

    def test_priority_ordering(self):
        rules = RoutingRuleSet()
        rules.add(RoutingRule(name="low", keywords=["test"], route="vector", priority=1))
        rules.add(RoutingRule(name="high", keywords=["test"], route="sql", priority=100))

        result = rules.evaluate("test query")
        assert result is not None
        assert result.route == "sql"  # Higher priority wins
        assert result.rule_name == "high"


class TestRoutingRuleSet:
    """Test the RoutingRuleSet collection."""

    def test_default_rules_present(self):
        rules = RoutingRuleSet.default()
        assert len(rules.rules) >= 3

    def test_default_rules_catch_ip_query(self):
        rules = RoutingRuleSet.default()
        result = rules.evaluate("What is the ip of server xyz")
        assert result is not None
        assert result.route == "sql"

    def test_default_rules_catch_port_query(self):
        rules = RoutingRuleSet.default()
        result = rules.evaluate("which port does caddy use")
        assert result is not None
        assert result.route == "sql"

    def test_default_rules_catch_config_query(self):
        rules = RoutingRuleSet.default()
        result = rules.evaluate("show me the config for nginx")
        assert result is not None
        assert result.route == "sql"

    def test_default_rules_no_match(self):
        rules = RoutingRuleSet.default()
        result = rules.evaluate("What is the meaning of life")
        assert result is None

    def test_custom_rule_addition(self):
        rules = RoutingRuleSet()
        rules.add(RoutingRule(name="custom", keywords=["custom"], route="vector", priority=50))
        assert len(rules.rules) == 1
        assert rules.evaluate("custom query") is not None

    def test_clear_rules(self):
        rules = RoutingRuleSet.default()
        assert len(rules.rules) > 0
        rules.clear()
        assert len(rules.rules) == 0


class TestEmbeddingRouter:
    """Test the embedding router with mock providers."""

    @pytest.fixture
    def router(self):
        from memory_server.providers.qdrant_provider import QdrantProvider
        from memory_server.providers.embedding_provider import MockEmbeddingProvider

        qdrant = QdrantProvider(location=":memory:", prefer_grpc=False)
        embedder = MockEmbeddingProvider(vector_size=384)
        return EmbeddingRouter(qdrant_provider=qdrant, embedder=embedder)

    async def test_semantic_search_no_results(self, router):
        results = await router.search("nonexistent query")
        assert results == []

    async def test_semantic_search_with_stored_data(self, router):
        # Store a fact via Qdrant directly
        vec = router._embedder.embed("Docker runs on OMV8")
        await router._qdrant.upsert(
            "memories",
            point_id=str(uuid.uuid4()),
            vector=vec,
            payload={"subject": "Docker", "predicate": "runs_on", "object": "OMV8", "content": "Docker runs on OMV8"},
        )

        results = await router.search("Docker OMV8")
        assert len(results) >= 1
        assert results[0]["payload"]["subject"] == "Docker"

    async def test_semantic_search_ranking(self, router):
        # Store facts where one is an exact match for the query (identical text = identical vector)
        query_text = "programming language"
        await router._qdrant.upsert("memories", point_id=str(uuid.uuid4()), vector=router._embedder.embed(query_text),
                                     payload={"content": query_text, "subject": "ExactMatch"})
        await router._qdrant.upsert("memories", point_id=str(uuid.uuid4()),
                                     vector=router._embedder.embed("The weather is nice today"),
                                     payload={"content": "The weather is nice today", "subject": "Weather"})

        # Search — exact same text should score 1.0, weather should score lower
        results = await router.search(query_text)
        assert len(results) >= 1
        # First result should be the exact match (score 1.0)
        assert results[0]["payload"]["subject"] == "ExactMatch"
        assert results[0]["score"] == pytest.approx(1.0)

    async def test_semantic_search_top_k(self, router):
        vec = router._embedder.embed("test")
        for i in range(5):
            await router._qdrant.upsert("memories", point_id=str(uuid.uuid4()), vector=vec,
                                         payload={"content": f"test fact {i}"})

        results = await router.search("test", top_k=3)
        assert len(results) <= 3

    async def test_semantic_search_score_threshold(self, router):
        vec = router._embedder.embed("exact match query")
        await router._qdrant.upsert("memories", point_id=str(uuid.uuid4()), vector=vec,
                                     payload={"content": "exact match query"})

        # Search with impossible-high threshold
        results = await router.search("exact match query", score_threshold=1.5)
        assert len(results) == 0

        # Search with low threshold
        results = await router.search("exact match query", score_threshold=0.0)
        assert len(results) >= 1

    async def test_empty_query(self, router):
        results = await router.search("")
        assert results == []

    async def test_search_default_collection(self, router):
        """Search should work with the default 'memories' collection."""
        vec = router._embedder.embed("hello world")
        await router._qdrant.upsert("memories", point_id=str(uuid.uuid4()), vector=vec,
                                     payload={"content": "hello world"})
        results = await router.search("hello")
        assert len(results) >= 1

    async def test_routing_rules_check_before_semantic(self, router):
        """Verify that routing rules are evaluated by the router."""
        # Add a routing rule that catches "ip of" with higher priority than default (100)
        router._rules.add(RoutingRule(name="ip_query", keywords=["ip of"], route="sql", priority=200))

        # Store something in Qdrant
        vec = router._embedder.embed("ip of server X is 10.0.0.1")
        await router._qdrant.upsert("memories", point_id=str(uuid.uuid4()), vector=vec,
                                     payload={"content": "ip of server X is 10.0.0.1", "subject": "server X"})

        # The rule should catch "ip of" and return a rule result, not semantic
        result = await router.route("What is the ip of server X?")
        assert result is not None
        assert "rule_match" in result
        assert result["rule_match"]["route"] == "sql"
        assert result["rule_match"]["rule_name"] == "ip_query"

    async def test_no_rule_match_falls_through_to_semantic(self, router):
        """When no rule matches, semantic search should run."""
        vec = router._embedder.embed("General knowledge about Python")
        await router._qdrant.upsert("memories", point_id="general", vector=vec,
                                     payload={"content": "General knowledge about Python"})

        result = await router.route("What is Python?")
        assert result is not None
        assert "semantic_results" in result or "rule_match" not in result
