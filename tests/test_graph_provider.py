"""Tests for in-memory graph engine (Card 017)."""

import pytest
import json

from memory_server.providers.graph_provider import SimpleGraph, GraphNode, GraphEdge


class TestSimpleGraph:
    """Test SimpleGraph — pure-Python in-memory graph engine."""

    @pytest.fixture
    def graph(self) -> SimpleGraph:
        g = SimpleGraph()
        g.add_node(id="server1", type="server", name="Docker Host 1", attributes={"ip": "10.0.0.1"})
        g.add_node(id="server2", type="server", name="Docker Host 2", attributes={"ip": "10.0.0.2"})
        g.add_node(id="project1", type="project", name="Web App", attributes={"repo": "github.com/org/web"})
        g.add_node(id="project2", type="project", name="API Service", attributes={"repo": "github.com/org/api"})
        g.add_node(id="service1", type="service", name="Nginx", attributes={"port": 80})
        g.add_edge(source_id="server1", target_id="server2", relation="connects_to", attributes={"via": "vpn"})
        g.add_edge(source_id="server1", target_id="project1", relation="hosts", attributes={"since": "2025-01-01"})
        g.add_edge(source_id="server2", target_id="project2", relation="hosts", attributes={"since": "2025-02-01"})
        g.add_edge(source_id="project1", target_id="service1", relation="uses", attributes={"type": "reverse_proxy"})
        return g

    # --- Node CRUD ---

    def test_add_node(self):
        g = SimpleGraph()
        node = g.add_node(id="n1", type="test", name="Test Node", attributes={"key": "val"})
        assert node.id == "n1"
        assert node.type == "test"
        assert node.name == "Test Node"
        assert node.attributes == {"key": "val"}

    def test_get_node(self, graph):
        node = graph.get_node("server1")
        assert node is not None
        assert node.id == "server1"
        assert node.type == "server"
        assert node.name == "Docker Host 1"

    def test_get_node_not_found(self, graph):
        assert graph.get_node("nonexistent") is None

    def test_add_duplicate_node_raises(self):
        g = SimpleGraph()
        g.add_node(id="n1", type="test", name="Original")
        with pytest.raises(ValueError, match="already exists"):
            g.add_node(id="n1", type="test", name="Duplicate")

    def test_delete_node(self, graph):
        graph.delete_node("service1")
        assert graph.get_node("service1") is None

    def test_delete_node_nonexistent(self, graph):
        with pytest.raises(KeyError, match="not found"):
            graph.delete_node("nonexistent")

    def test_get_all_nodes(self, graph):
        nodes = graph.get_all_nodes()
        assert len(nodes) == 5

    # --- Edge CRUD ---

    def test_add_edge(self, graph):
        g = SimpleGraph()
        g.add_node(id="a", type="test", name="A")
        g.add_node(id="b", type="test", name="B")
        edge = g.add_edge(source_id="a", target_id="b", relation="connected", attributes={"weight": 1})
        assert edge.source_id == "a"
        assert edge.target_id == "b"
        assert edge.relation == "connected"
        assert edge.attributes == {"weight": 1}

    def test_add_edge_missing_source_raises(self):
        g = SimpleGraph()
        g.add_node(id="b", type="test", name="B")
        with pytest.raises(KeyError, match="Source node"):
            g.add_edge(source_id="a", target_id="b", relation="connected")

    def test_add_edge_missing_target_raises(self):
        g = SimpleGraph()
        g.add_node(id="a", type="test", name="A")
        with pytest.raises(KeyError, match="Target node"):
            g.add_edge(source_id="a", target_id="b", relation="connected")

    def test_get_edge(self, graph):
        edge = graph.get_edge("server1", "server2")
        assert edge is not None
        assert edge.relation == "connects_to"
        assert edge.attributes == {"via": "vpn"}

    def test_get_edge_not_found(self, graph):
        assert graph.get_edge("server1", "project2") is None

    def test_delete_edge(self, graph):
        graph.delete_edge("server1", "server2")
        assert graph.get_edge("server1", "server2") is None

    def test_delete_edge_nonexistent(self, graph):
        with pytest.raises(KeyError, match="not found"):
            graph.delete_edge("nope", "nada")

    # --- Neighbor traversal ---

    def test_get_neighbors_all(self, graph):
        neighbors = graph.get_neighbors("server1")
        assert len(neighbors) == 2  # server2 + project1
        node_ids = {n.id for n, e in neighbors}
        assert "server2" in node_ids
        assert "project1" in node_ids

    def test_get_neighbors_by_relation(self, graph):
        neighbors = graph.get_neighbors("server1", relation="hosts")
        assert len(neighbors) == 1
        node_id, edge = neighbors[0]
        assert node_id.id == "project1"
        assert edge.relation == "hosts"

    def test_get_neighbors_no_match(self, graph):
        neighbors = graph.get_neighbors("server1", relation="nonexistent")
        assert neighbors == []

    def test_get_neighbors_nonexistent_node(self, graph):
        neighbors = graph.get_neighbors("nonexistent")
        assert neighbors == []

    # --- Pathfinding ---

    def test_find_path_direct(self, graph):
        paths = graph.find_path("server1", "server2", max_depth=2)
        assert len(paths) >= 1
        # Direct edge: server1 -> server2
        assert paths[0][0].id == "server1"
        assert paths[0][-1].id == "server2"

    def test_find_path_two_hops(self, graph):
        paths = graph.find_path("server1", "service1", max_depth=3)
        assert len(paths) >= 1
        # server1 -> project1 -> service1
        path_node_ids = [n.id for n in paths[0]]
        assert path_node_ids[0] == "server1"
        assert path_node_ids[-1] == "service1"

    def test_find_path_no_path(self, graph):
        # Isolated: add a node with no edges
        graph.add_node(id="isolated", type="test", name="Isolated")
        paths = graph.find_path("server1", "isolated", max_depth=4)
        assert paths == []

    def test_find_path_max_depth_respected(self, graph):
        paths = graph.find_path("server1", "service1", max_depth=1)
        # 1-hop max depth won't reach service1 (needs 2 hops)
        assert paths == []

    def test_find_path_same_node(self, graph):
        paths = graph.find_path("server1", "server1", max_depth=3)
        assert len(paths) == 1
        assert len(paths[0]) == 1
        assert paths[0][0].id == "server1"

    # --- Search by type / relation ---

    def test_search_by_type(self, graph):
        servers = graph.search_by_type("server")
        assert len(servers) == 2
        assert all(n.type == "server" for n in servers)

    def test_search_by_type_no_match(self, graph):
        results = graph.search_by_type("database")
        assert results == []

    def test_search_by_relation(self, graph):
        edges = graph.search_by_relation("hosts")
        assert len(edges) == 2
        assert all(e.relation == "hosts" for e in edges)

    def test_search_by_relation_no_match(self, graph):
        results = graph.search_by_relation("nonexistent")
        assert results == []

    # --- Serialization ---

    def test_to_dict_and_from_dict(self, graph):
        data = graph.to_dict()
        assert "nodes" in data
        assert "edges" in data

        g2 = SimpleGraph()
        g2.from_dict(data)
        assert len(g2.get_all_nodes()) == 5
        assert g2.get_node("server1") is not None
        assert g2.get_edge("server1", "server2") is not None

    def test_json_roundtrip(self, graph):
        data = graph.to_dict()
        json_str = json.dumps(data)
        loaded = json.loads(json_str)
        g2 = SimpleGraph()
        g2.from_dict(loaded)
        assert len(g2.get_all_nodes()) == 5
        assert g2.get_neighbors("server1")[0][0].name == "Docker Host 2"

    def test_empty_graph(self):
        g = SimpleGraph()
        assert g.get_all_nodes() == []
        assert g.search_by_type("anything") == []
        assert g.search_by_relation("anything") == []
        assert g.find_path("a", "b") == []
