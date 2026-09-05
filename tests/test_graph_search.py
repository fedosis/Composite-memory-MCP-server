"""Tests for graph_search MCP tool (Card 019)."""

import json

import pytest

from memory_server.server import _get_graph_router, graph_search_fn


@pytest.mark.asyncio
class TestGraphSearchTool:
    """Test graph_search MCP tool."""

    @pytest.fixture(autouse=True)
    async def setup_graph(self, graph_test_isolation):
        """Give every graph test its own persisted snapshot and singletons."""
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

    async def test_search_by_relation_type(self):
        """Relation-aware search returns typed relation pairs."""
        router = await _get_graph_router()
        router.sync_fact(subject="Claim A", predicate="contradicts", object="Claim B")
        router.sync_fact(subject="Claim A", predicate="supports", object="Claim C")

        result = json.loads(await graph_search_fn(relation_type="contradicts"))
        assert result["paths"] == []
        assert len(result["edges"]) == 1
        edge = result["edges"][0]
        assert edge["relation"] == "contradicts"
        assert edge["source_name"] == "Claim A"
        assert edge["target_name"] == "Claim B"


class TestGraphSearchDiamondAndCycle:
    """PROV-6 via the server tool: pathfinding must keep all simple paths.

    Uses the graph_test_isolation fixture (tmp_path snapshot), so the live
    data/graph.json is never touched.
    """

    @pytest.fixture(autouse=True)
    async def setup_graph(self, graph_test_isolation):
        yield

    @staticmethod
    def _path_ids(path):
        return tuple(n["id"] for n in path)

    async def _build_diamond(self):
        router = await _get_graph_router()
        # ServerA -> App1 -> Db  and  ServerA -> App2 -> Db
        router.sync_fact(subject="ServerA", predicate="hosts", object="App1")
        router.sync_fact(subject="ServerA", predicate="hosts", object="App2")
        router.sync_fact(subject="App1", predicate="uses", object="Db")
        router.sync_fact(subject="App2", predicate="uses", object="Db")
        return router

    async def test_pathfinding_diamond_returns_both_paths(self):
        await self._build_diamond()
        result = json.loads(await graph_search_fn(source_id="servera", target_id="db"))
        path_sigs = {self._path_ids(p) for p in result["paths"]}
        assert path_sigs == {("servera", "app1", "db"), ("servera", "app2", "db")}

    async def test_pathfinding_with_cycle_terminates_and_returns_simple_path(self):
        router = await _get_graph_router()
        router.sync_fact(subject="CycA", predicate="links", object="CycB")
        router.sync_fact(subject="CycB", predicate="links", object="CycA")  # cycle
        router.sync_fact(subject="CycB", predicate="reaches", object="Goal")

        result = json.loads(await graph_search_fn(source_id="cyca", target_id="goal"))
        assert len(result["paths"]) >= 1
        for path in result["paths"]:
            ids = self._path_ids(path)
            assert len(ids) == len(set(ids)), f"non-simple path returned: {ids}"
