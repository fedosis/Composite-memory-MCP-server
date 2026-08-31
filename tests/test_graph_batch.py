"""Tests for GraphRouter.sync_facts_batch (SPEC: cmms-graph-batch).

Covers:
- AC1: batch of N facts (including two with same subject/object, different
  predicates) → all N edges present, nodes created once.
- AC2: sync_facts_batch is idempotent — running twice does not duplicate
  nodes/edges.
- AC3: snapshot is written exactly once per batch.
- S7: get_edge(..., relation=...) filter.
"""

import pytest  # noqa: F401  (kept for symmetry with sibling test modules)

from memory_server.providers.graph_provider import SimpleGraph
from memory_server.router.graph_router import GraphRouter


class CountingGraph(SimpleGraph):
    """SimpleGraph subclass that counts _write_snapshot calls."""

    def __init__(self, snapshot_path):
        super().__init__(snapshot_path=snapshot_path)
        self.write_count = 0

    def _write_snapshot(self, snapshot_path):
        self.write_count += 1
        super()._write_snapshot(snapshot_path)


S7_TRIPLES = [
    {"subject": "Docker", "predicate": "is", "object": "container"},
    {"subject": "Docker", "predicate": "runs_on", "object": "OMV8"},
    # S7: same subject/object as the first triple, different predicate.
    {"subject": "Docker", "predicate": "uses", "object": "container"},
    {"subject": "PostgreSQL", "predicate": "is", "object": "database"},
]

# Unique node ids: docker, container, omv8, postgresql, database.
EXPECTED_NODES = 5
# Unique edges: (docker,container,is), (docker,omv8,runs_on),
# (docker,container,uses), (postgresql,database,is).
EXPECTED_EDGES = 4


class TestGetEdgeRelationFilter:
    def test_relation_filter_returns_matching_edge(self):
        graph = SimpleGraph()
        graph.add_node(id="a", type="entity", name="A")
        graph.add_node(id="b", type="entity", name="B")
        graph.add_edge(source_id="a", target_id="b", relation="is")
        graph.add_edge(source_id="a", target_id="b", relation="uses")

        # Backward compatible: no relation → first edge.
        first = graph.get_edge("a", "b")
        assert first is not None and first.relation == "is"
        # Filtered lookup returns the matching edge (S7).
        use_edge = graph.get_edge("a", "b", relation="uses")
        assert use_edge is not None and use_edge.relation == "uses"
        assert graph.get_edge("a", "b", relation="missing") is None


class TestSyncFactsBatch:
    def test_batch_creates_all_edges_and_nodes_once(self):
        """AC1: all edges present (incl. same s/o different predicates),
        nodes created exactly once."""
        graph = SimpleGraph()
        router = GraphRouter(graph=graph)

        router.sync_facts_batch(S7_TRIPLES)

        assert len(graph.get_all_nodes()) == EXPECTED_NODES
        data = graph.to_dict()
        assert len(data["edges"]) == EXPECTED_EDGES

        # S7: both docker→container edges survive with distinct relations.
        assert graph.get_edge("docker", "container", relation="is") is not None
        assert graph.get_edge("docker", "container", relation="uses") is not None
        assert graph.get_edge("docker", "omv8", relation="runs_on") is not None
        assert graph.get_edge("postgresql", "database", relation="is") is not None

    def test_batch_is_idempotent(self):
        """AC2: running twice does not duplicate nodes/edges."""
        graph = SimpleGraph()
        router = GraphRouter(graph=graph)

        router.sync_facts_batch(S7_TRIPLES)
        router.sync_facts_batch(S7_TRIPLES)

        assert len(graph.get_all_nodes()) == EXPECTED_NODES
        assert len(graph.to_dict()["edges"]) == EXPECTED_EDGES

    def test_batch_empty_is_noop(self):
        graph = SimpleGraph()
        router = GraphRouter(graph=graph)

        router.sync_facts_batch([])

        assert graph.get_all_nodes() == []

    def test_snapshot_written_once_per_batch(self, tmp_path):
        """AC3: one snapshot write per batch, not one per graph operation."""
        graph = CountingGraph(tmp_path / "graph.json")
        router = GraphRouter(graph=graph)

        router.sync_facts_batch(S7_TRIPLES)

        assert graph.write_count == 1
        assert (tmp_path / "graph.json").exists()

    def test_snapshot_content_matches_batch(self, tmp_path):
        """Batch result survives a snapshot round-trip."""
        snapshot_path = tmp_path / "graph.json"
        graph = SimpleGraph(snapshot_path=snapshot_path)
        router = GraphRouter(graph=graph)

        router.sync_facts_batch(S7_TRIPLES)

        loaded = SimpleGraph(snapshot_path=snapshot_path)
        loaded.load_snapshot()
        assert len(loaded.get_all_nodes()) == EXPECTED_NODES
        assert len(loaded.to_dict()["edges"]) == EXPECTED_EDGES

    def test_batch_merges_with_existing_graph(self):
        """Existing nodes/edges are reused, only new ones are added."""
        graph = SimpleGraph()
        graph.add_node(id="docker", type="entity", name="Docker")
        graph.add_node(id="omv8", type="entity", name="OMV8")
        graph.add_edge(source_id="docker", target_id="omv8", relation="runs_on")
        router = GraphRouter(graph=graph)

        router.sync_facts_batch(S7_TRIPLES)

        assert len(graph.get_all_nodes()) == EXPECTED_NODES
        assert len(graph.to_dict()["edges"]) == EXPECTED_EDGES


class TestSyncFactsBatchValidation:
    """Card 2, AC2: batch input validation is atomic — no mutation on invalid input.

    Every triple is validated BEFORE any graph mutation, so a rejected batch
    leaves nodes, edges, and the snapshot untouched. ``None`` / non-list
    containers raise ValueError (SPEC: ``None`` is no longer a silent no-op).
    """

    def _graph_and_router(self, tmp_path):
        graph = CountingGraph(tmp_path / "graph.json")
        return graph, GraphRouter(graph=graph)

    @pytest.mark.parametrize("bad_container", [None, "x", ("a", "b")])
    def test_container_must_be_list(self, tmp_path, bad_container):
        """None / str / tuple → ValueError with no mutation; a list is required."""
        graph, router = self._graph_and_router(tmp_path)
        before = graph.to_dict()
        with pytest.raises(ValueError):
            router.sync_facts_batch(bad_container)
        assert graph.to_dict() == before
        assert graph.write_count == 0

    @pytest.mark.parametrize(
        "bad_triple",
        [
            ["a"],  # non-dict entry
            {"predicate": "is", "object": "o"},  # missing subject
            {"subject": "s", "object": "o"},  # missing predicate
            {"subject": "s", "predicate": "is"},  # missing object
            {"subject": 1, "predicate": "is", "object": "o"},  # non-string value
            {"subject": "", "predicate": "is", "object": "o"},  # empty string
            {"subject": "   ", "predicate": "is", "object": "o"},  # ASCII ws
            {"subject": "\u00a0", "predicate": "is", "object": "o"},  # Unicode ws
            {"subject": "\u2003", "predicate": "is", "object": "o"},  # Unicode ws
            {"subject": "\t", "predicate": "is", "object": "o"},  # tab
        ],
    )
    def test_invalid_triples_rejected_atomically(self, tmp_path, bad_triple):
        """Any invalid triple → whole batch ValueError, graph untouched."""
        graph, router = self._graph_and_router(tmp_path)
        before = graph.to_dict()
        with pytest.raises(ValueError):
            router.sync_facts_batch([bad_triple])
        assert graph.to_dict() == before
        assert graph.write_count == 0

    def test_rejected_entry_is_logged(self, tmp_path, caplog):
        """The rejected entry appears in the warning log."""
        graph, router = self._graph_and_router(tmp_path)
        rejected = {"subject": "   ", "predicate": "is", "object": "o"}
        with pytest.raises(ValueError):
            router.sync_facts_batch([rejected])
        assert "rejected invalid triple" in caplog.text
        assert "{'subject': '   '" in caplog.text

    def test_extra_keys_ignored(self):
        """Extra keys are allowed and ignored — batch succeeds, edges created."""
        graph = SimpleGraph()
        router = GraphRouter(graph=graph)
        router.sync_facts_batch([
            {
                "subject": "Docker",
                "predicate": "is",
                "object": "container",
                "source": "test",
                "extra": 1,
            },
        ])
        assert graph.get_edge("docker", "container", relation="is") is not None

    def test_valid_invalid_valid_whole_batch_rejected(self, tmp_path):
        """[valid, invalid, valid] → whole batch rejected, nothing written."""
        graph, router = self._graph_and_router(tmp_path)
        before = graph.to_dict()
        with pytest.raises(ValueError):
            router.sync_facts_batch([
                {"subject": "A", "predicate": "is", "object": "B"},
                {"subject": "   ", "predicate": "is", "object": "B"},
                {"subject": "C", "predicate": "is", "object": "D"},
            ])
        assert graph.to_dict() == before
        assert graph.write_count == 0

    def test_valid_batch_after_rejected_batch_works(self, tmp_path):
        """A valid batch still works after a rejected one (only valid calls mutate)."""
        graph, router = self._graph_and_router(tmp_path)
        with pytest.raises(ValueError):
            router.sync_facts_batch([{"subject": " ", "predicate": "is", "object": "B"}])
        router.sync_facts_batch([
            {"subject": "Docker", "predicate": "is", "object": "container"},
        ])
        assert graph.get_edge("docker", "container", relation="is") is not None

    def test_empty_list_is_noop(self, tmp_path):
        """Empty list stays a no-op: no exception, no mutation, no snapshot."""
        graph, router = self._graph_and_router(tmp_path)
        router.sync_facts_batch([])
        assert graph.get_all_nodes() == []
        assert graph.write_count == 0
