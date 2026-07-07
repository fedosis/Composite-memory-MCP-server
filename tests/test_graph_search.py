"""Tests for graph_search MCP tool (Card 019)."""

import json

import pytest

from memory_server.server import graph_search_fn, _get_graph_router


@pytest.mark.asyncio
class TestGraphSearchTool:
    """Test graph_search MCP tool."""

    @pytest.fixture(autouse=True)
    async def setup_graph(self):
        """Ensure a fresh graph router for each test."""
        # The singleton is lazily created — we can't clean if already dirty,
        # but tests in fresh processes work fine. For the same session,
        # we let _get_graph_router reuse the existing one.
        yield

    async def test_search_by_entity_name(self):
        """Search for entity by name finds neighbors."""
        router = await _get_graph_router()
        # Pre-populate
        router.sync_fact(subject="Docker", predicate="runs_on", object="OMV8")
        router.sync_fact(subject="Docker", predicate="is", object="container platform")

        result = json.loads(await graph_search_fn(query="Docker"))
        assert "nodes" in result
        assert "edges" in result
        node_names = {n["name"] for n in result["nodes"]}
        assert "Docker" in node_names

    async def test_search_by_entity_id(self):
        """Direct node lookup by entity_id."""
        router = await _get_graph_router()
        router.sync_fact(subject="PostgreSQL", predicate="is", object="database")
        result = json.loads(await graph_search_fn(entity_id="postgresql"))
        assert "nodes" in result
        assert len(result["nodes"]) >= 1
        assert result["nodes"][0]["name"] == "PostgreSQL"

    async def test_search_entity_id_not_found(self):
        """Nonexistent entity_id returns empty."""
        result = json.loads(await graph_search_fn(entity_id="nonexistent"))
        assert result["nodes"] == []
        assert result["edges"] == []
        assert result["paths"] == []

    async def test_pathfinding_between_entities(self):
        """Pathfinding between source and target."""
        router = await _get_graph_router()
        router.sync_fact(subject="ServerA", predicate="hosts", object="WebApp")
        router.sync_fact(subject="WebApp", predicate="uses", object="Nginx")

        result = json.loads(await graph_search_fn(source_id="servera", target_id="nginx"))
        assert "paths" in result
        assert len(result["paths"]) >= 1
        first_path = result["paths"][0]
        path_names = [n["name"] for n in first_path]
        assert "ServerA" in path_names
        assert "Nginx" in path_names

    async def test_pathfinding_no_path(self):
        """Pathfinding between unrelated entities returns empty paths."""
        router = await _get_graph_router()
        router.sync_fact(subject="Isolated1", predicate="is", object="thing1")
        router.sync_fact(subject="Isolated2", predicate="is", object="thing2")

        result = json.loads(await graph_search_fn(source_id="isolated1", target_id="isolated2"))
        assert result["paths"] == []

    async def test_search_no_match(self):
        """No results for unrelated query."""
        result = json.loads(await graph_search_fn(query="nonexistent_entity_xyz"))
        assert result["nodes"] == []
        assert result["edges"] == []
        assert result["paths"] == []

    async def test_search_empty_params(self):
        """Empty query returns empty result, not error."""
        result = json.loads(await graph_search_fn())
        assert "nodes" in result
        assert "edges" in result
        assert "paths" in result
        assert result["nodes"] == []

    async def test_search_includes_edges(self):
        """Result includes edges related to matched entities."""
        router = await _get_graph_router()
        router.sync_fact(subject="App", predicate="deployed_on", object="ServerY")

        result = json.loads(await graph_search_fn(query="App"))
        assert len(result["edges"]) >= 1
        edge = result["edges"][0]
        assert "source_id" in edge
        assert "target_id" in edge
        assert "relation" in edge
