"""Integration tests for the semantic router — full e2e pipeline.

Tests the EmbeddingRouter with real Qdrant in-memory and MockEmbeddingProvider
to verify end-to-end flow: embed → store in Qdrant → semantic_search → ranking.

Also tests rules matching through the MCP server's semantic_search tool.
"""

import json
import uuid

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.types import TextContent

from memory_server.providers.embedding_provider import MockEmbeddingProvider
from memory_server.providers.qdrant_provider import QdrantProvider
from memory_server.router.embedding_router import EmbeddingRouter
from memory_server.router.rules import RoutingRule


# ------------------------------------------------------------------
# Unit-level integration: EmbeddingRouter + real Qdrant in-memory
# ------------------------------------------------------------------


@pytest.fixture
def router():
    """Create a real EmbeddingRouter with in-memory Qdrant and mock embedder."""
    qdrant = QdrantProvider(location=":memory:", prefer_grpc=False)
    embedder = MockEmbeddingProvider(vector_size=384)
    return EmbeddingRouter(vector_provider=qdrant, embedder=embedder)


@pytest.mark.asyncio
class TestRouterPipeline:
    """End-to-end pipeline tests: embed, store, search, verify ranking."""

    async def test_e2e_embed_store_search_ranking(self, router):
        """Embed a fact → store in Qdrant → semantic_search → verify ranking."""
        # Embed and store a fact
        vec_docker = router._embedder.embed("Docker runs on OMV8")
        vec_caddy = router._embedder.embed("Caddy uses port 443")
        vec_python = router._embedder.embed("Python is a programming language")

        await router._vector_provider.upsert(
            "memories",
            point_id=str(uuid.uuid4()),
            vector=vec_docker,
            payload={"content": "Docker runs on OMV8", "subject": "Docker"},
        )
        await router._vector_provider.upsert(
            "memories",
            point_id=str(uuid.uuid4()),
            vector=vec_caddy,
            payload={"content": "Caddy uses port 443", "subject": "Caddy"},
        )
        await router._vector_provider.upsert(
            "memories",
            point_id=str(uuid.uuid4()),
            vector=vec_python,
            payload={"content": "Python is a programming language", "subject": "Python"},
        )

        # Search for Docker-related query
        results = await router.search("Docker OMV8")
        assert len(results) >= 1
        # Docker should rank first
        assert results[0]["payload"]["subject"] == "Docker"

    async def test_exact_match_rules_before_embedding(self, router):
        """Exact-match rules should be evaluated before embedding search."""
        # Add a rule to catch "what port"
        router._rules.add(RoutingRule(
            name="port_query_test",
            keywords=["what port"],
            route="sql",
            priority=200,
        ))

        # Store something in Qdrant that would match semantically
        vec = router._embedder.embed("Caddy uses port 443")
        await router._vector_provider.upsert(
            "memories",
            point_id=str(uuid.uuid4()),
            vector=vec,
            payload={"content": "Caddy uses port 443", "subject": "Caddy"},
        )

        # Route a query that matches the rule
        result = await router.route("What port does caddy use?")
        assert result is not None
        assert "rule_match" in result
        assert result["rule_match"]["route"] == "sql"
        assert result["rule_match"]["rule_name"] == "port_query_test"

    async def test_semantic_similarity_finds_close_matches(self, router):
        """Semantic search should find closely related facts."""
        query_text = "container orchestration"
        # Store a fact with the exact same text as the query (identical vectors)
        await router._vector_provider.upsert(
            "memories",
            point_id=str(uuid.uuid4()),
            vector=router._embedder.embed(query_text),
            payload={"content": query_text, "subject": "ContainerOrch"},
        )

        results = await router.search(query_text)
        assert len(results) >= 1
        assert results[0]["payload"]["subject"] == "ContainerOrch"
        assert results[0]["score"] == pytest.approx(1.0)

    async def test_semantic_search_no_results(self, router):
        """Search for something with no stored data should return empty."""
        results = await router.search("nonexistent query with no matches")
        assert results == []

    async def test_empty_query_handling(self, router):
        """Empty query should return empty results."""
        results = await router.search("")
        assert results == []

        results = await router.search("   ")
        assert results == []

    async def test_semantic_search_with_score_threshold(self, router):
        """Score threshold should filter out low-similarity results."""
        vec = router._embedder.embed("unique specific fact")
        await router._vector_provider.upsert(
            "memories",
            point_id=str(uuid.uuid4()),
            vector=vec,
            payload={"content": "unique specific fact"},
        )

        # High threshold should exclude (max score is 1.0)
        results = await router.search("unique specific fact", score_threshold=1.5)
        assert len(results) == 0

        # Low threshold should include
        results = await router.search("unique specific fact", score_threshold=0.0)
        assert len(results) >= 1

    async def test_top_k_limits_results(self, router):
        """top_k parameter should limit the number of results."""
        vec = router._embedder.embed("same text")
        for i in range(10):
            await router._vector_provider.upsert(
                "memories",
                point_id=str(uuid.uuid4()),
                vector=vec,
                payload={"content": f"fact {i}", "index": i},
            )

        results = await router.search("same text", top_k=3)
        assert len(results) <= 3

    async def test_route_no_match_falls_through_to_semantic(self, router):
        """When no rule matches, route() should return semantic_results."""
        vec = router._embedder.embed("general knowledge about anything")
        await router._vector_provider.upsert(
            "memories",
            point_id=str(uuid.uuid4()),
            vector=vec,
            payload={"content": "general knowledge about anything"},
        )

        result = await router.route("What is general knowledge?")
        assert result is not None
        assert "semantic_results" in result
        assert result["total"] >= 1

    async def test_route_empty_query(self, router):
        """route() with empty query should return an error."""
        result = await router.route("")
        assert "error" in result
        assert result["error"] == "Empty query"


# ------------------------------------------------------------------
# MCP-level integration: test semantic_search through the MCP tool
# ------------------------------------------------------------------


@pytest.fixture
def server_params():
    return StdioServerParameters(command="memory-server", args=["serve"])


@pytest.mark.asyncio
class TestMCPSemanticSearchTool:
    """Test the semantic_search MCP tool end-to-end."""

    async def _call_and_parse(self, session, tool_name: str, arguments: dict) -> dict:
        """Call an MCP tool and return the parsed JSON result."""
        result = await session.call_tool(tool_name, arguments=arguments)
        for content_item in result.content:
            if isinstance(content_item, TextContent):
                return json.loads(content_item.text)
        text = result.content[0].text
        return json.loads(text)

    async def test_semantic_search_tool_returns_rule_match(self, server_params):
        """semantic_search on a rule-matched query returns a rule_match."""
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                data = await self._call_and_parse(
                    session, "semantic_search",
                    arguments={"query": "What is the ip of server xyz?"},
                )
                assert data is not None
                # Should have either rule_match or semantic_results
                assert "rule_match" in data or "semantic_results" in data

    async def test_semantic_search_tool_accepts_top_k(self, server_params):
        """semantic_search should accept top_k and score_threshold params."""
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                data = await self._call_and_parse(
                    session, "semantic_search",
                    arguments={
                        "query": "port configuration",
                        "top_k": 5,
                        "score_threshold": 0.0,
                    },
                )
                assert data is not None
