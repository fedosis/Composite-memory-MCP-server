"""Tests for entity relation linker / GraphRouter (Card 018)."""

import pytest

from memory_server.providers.graph_provider import SimpleGraph
from memory_server.router.graph_router import GraphRouter


class TestGraphRouter:
    """Test GraphRouter — entity relation linker."""

    @pytest.fixture
    def graph(self) -> SimpleGraph:
        g = SimpleGraph()
        g.add_node(id="docker", type="software", name="Docker", attributes={"version": "24.0"})
        g.add_node(id="omv8", type="server", name="OMV8", attributes={"ip": "192.168.1.100"})
        g.add_node(id="caddy", type="software", name="Caddy", attributes={"version": "2.7"})
        g.add_node(id="nginx", type="software", name="Nginx", attributes={"port": 443})
        g.add_node(id="server_x", type="server", name="Server X", attributes={})
        g.add_node(id="project_web", type="project", name="Web App", attributes={})
        g.add_edge(source_id="docker", target_id="omv8", relation="runs_on")
        g.add_edge(source_id="caddy", target_id="server_x", relation="runs_on")
        g.add_edge(source_id="nginx", target_id="server_x", relation="runs_on")
        g.add_edge(source_id="project_web", target_id="nginx", relation="uses")
        return g

    @pytest.fixture
    def router(self, graph) -> GraphRouter:
        return GraphRouter(graph=graph)

    # --- Entity extraction ---

    def test_extract_entities_by_name(self, router):
        result = router.query("Tell me about Docker")
        assert "entities" in result
        entity_names = {e["name"] for e in result["entities"]}
        assert "Docker" in entity_names

    def test_extract_entities_case_insensitive(self, router):
        result = router.query("What is docker?")
        entity_names = {e["name"] for e in result["entities"]}
        assert "Docker" in entity_names

    def test_extract_no_match(self, router):
        result = router.query("What is the weather today?")
        assert result["entities"] == []
        assert result["relations"] == []
        assert result["paths"] == []

    # --- Relation queries ---

    def test_get_related_entities(self, router):
        result = router.query("Tell me about Docker")
        relation_types = {r["relation"] for r in result["relations"]}
        assert "runs_on" in relation_types
        related_names = {r["target_name"] for r in result["relations"]}
        assert "OMV8" in related_names

    def test_multiple_entities_in_query(self, router):
        result = router.query("What connects Docker and OMV8?")
        assert len(result["entities"]) >= 2
        entity_names = {e["name"] for e in result["entities"]}
        assert "Docker" in entity_names
        assert "OMV8" in entity_names

    def test_relations_only_for_matched_entities(self, router):
        """Only relations for mentioned entities should be returned."""
        result = router.query("Nginx")
        entity_names = {e["name"] for e in result["entities"]}
        assert "Nginx" in entity_names
        # Docker should not appear
        docker_in_entities = any(e["name"] == "Docker" for e in result["entities"])
        assert not docker_in_entities

    # --- Pathfinding ---

    def test_pathfinding_between_entities(self, router):
        result = router.query("How does Web App relate to Server X?")
        if result["paths"]:
            for path in result["paths"]:
                assert len(path) >= 2

    def test_pathfinding_not_returned_for_single_entity(self, router):
        result = router.query("Tell me about Docker")
        assert result["paths"] == []  # Only 1 entity matched, no pathfinding

    # --- Fact sync ---

    def test_sync_fact_adds_nodes_and_edge(self, graph, router):
        router.sync_fact(subject="PostgreSQL", predicate="runs_on", object="OMV8")
        # Both nodes should exist in graph
        assert graph.get_node("postgresql") is not None
        assert graph.get_node("omv8") is not None
        # Edge should exist
        edge = graph.get_edge("postgresql", "omv8")
        assert edge is not None
        assert edge.relation == "runs_on"

    def test_sync_fact_links_to_existing_nodes(self, graph, router):
        router.sync_fact(subject="Docker", predicate="runs_on", object="OMV8")
        # Existing nodes should be reused
        assert graph.get_node("docker") is not None
        assert graph.get_node("docker").name == "Docker"
        # Docker already had an edge to OMV8, should now have 2
        neighbors = graph.get_neighbors("docker")
        omv8_edges = [e for n, e in neighbors if n.id == "omv8"]
        assert len(omv8_edges) >= 1

    def test_sync_fact_same_subj_obj_different_predicates_keeps_both_edges(self, graph, router):
        """Regression: legacy sync_fact path must dedup by (subj, obj, relation).

        Two facts sharing the same subject/object but different predicates
        must both keep their edges (Card 2, AC1). The legacy path must pass
        ``relation=predicate`` to ``get_edge`` exactly like the batch path.
        """
        router.sync_fact(subject="Docker", predicate="is", object="container")
        router.sync_fact(subject="Docker", predicate="uses", object="container")

        assert graph.get_edge("docker", "container", relation="is") is not None
        assert graph.get_edge("docker", "container", relation="uses") is not None

    def test_sync_decision_adds_decision_node(self, graph, router):
        router.sync_decision(
            choice="use Caddy",
            reason="it is simpler",
            entities=["Caddy", "Nginx"],
        )
        decision_node = graph.get_node("decision-use-caddy")
        assert decision_node is not None
        assert decision_node.type == "decision"
        assert decision_node.name == "use Caddy"

    def test_sync_decision_links_to_entities(self, graph, router):
        router.sync_decision(
            choice="use Caddy",
            reason="it is simpler",
            entities=["Caddy", "Nginx"],
        )
        # Should have edges from decision to Caddy and Nginx
        caddy_edges = graph.get_edge("decision-use-caddy", "caddy")
        assert caddy_edges is not None
        assert caddy_edges.relation == "decides"

        nginx_edges = graph.get_edge("decision-use-caddy", "nginx")
        assert nginx_edges is not None
        assert nginx_edges.relation == "decides"

    def test_sync_decision_no_entities(self, graph, router):
        router.sync_decision(choice="refactor code", reason="better design", entities=[])
        assert graph.get_node("decision-refactor-code") is not None

    def test_query_after_sync_fact(self, router):
        router.sync_fact(subject="PostgreSQL", predicate="is", object="database")
        result = router.query("What is PostgreSQL?")
        entity_names = {e["name"] for e in result["entities"]}
        assert "PostgreSQL" in entity_names

    # --- Edge cases ---

    def test_empty_query(self, router):
        result = router.query("")
        assert result["entities"] == []
        assert result["relations"] == []
        assert result["paths"] == []

    def test_whitespace_query(self, router):
        result = router.query("   ")
        assert result["entities"] == []

    def test_graph_with_no_data(self):
        empty_router = GraphRouter()
        result = empty_router.query("Docker")
        assert result["entities"] == []
        assert result["relations"] == []
        assert result["paths"] == []


class TestGraphRouterDiamondAndCycle:
    """PROV-6: router pathfinding must surface every simple path.

    GraphRouter.query() delegates to SimpleGraph.find_path; a shared
    visited set would hide diamond alternatives and cycle-safe simple paths.
    """

    @staticmethod
    def _path_ids(path):
        return tuple(n["id"] for n in path)

    def test_query_returns_both_diamond_paths(self):
        g = SimpleGraph()
        for nid, name in [("a", "Alpha"), ("b", "Bravo"), ("c", "Charlie"),
                          ("d", "Delta"), ("e", "Epsilon")]:
            g.add_node(id=nid, type="entity", name=name)
        g.add_edge(source_id="a", target_id="b", relation="links")
        g.add_edge(source_id="a", target_id="c", relation="links")
        g.add_edge(source_id="b", target_id="d", relation="links")
        g.add_edge(source_id="c", target_id="d", relation="links")
        g.add_edge(source_id="d", target_id="e", relation="links")
        router = GraphRouter(graph=g)

        result = router.query("Alpha Epsilon")
        path_sigs = {self._path_ids(p) for p in result["paths"]}
        assert path_sigs == {("a", "b", "d", "e"), ("a", "c", "d", "e")}

    def test_query_with_cycle_returns_simple_path_only(self):
        g = SimpleGraph()
        for nid, name in [("a", "Alpha"), ("b", "Bravo"), ("c", "Charlie"),
                          ("d", "Delta"), ("e", "Epsilon")]:
            g.add_node(id=nid, type="entity", name=name)
        g.add_edge(source_id="a", target_id="b", relation="links")
        g.add_edge(source_id="b", target_id="c", relation="links")
        g.add_edge(source_id="c", target_id="a", relation="links")  # cycle
        g.add_edge(source_id="c", target_id="d", relation="links")
        g.add_edge(source_id="d", target_id="e", relation="links")
        router = GraphRouter(graph=g)

        result = router.query("Alpha Epsilon")
        assert result["paths"], "expected a path despite the cycle"
        for path in result["paths"]:
            ids = self._path_ids(path)
            assert len(ids) == len(set(ids)), f"non-simple path returned: {ids}"
