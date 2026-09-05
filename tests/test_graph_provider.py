"""Tests for in-memory graph engine (Card 017)."""

import json
from pathlib import Path

import pytest

from memory_server.providers.graph_provider import SimpleGraph
from memory_server.router.graph_router import GraphRouter


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

    def test_snapshot_persistence_roundtrip(self, tmp_path):
        snapshot_path = tmp_path / "graph.json"

        g = SimpleGraph(snapshot_path=snapshot_path)
        g.add_node(id="a", type="fact", name="Claim A")
        g.add_node(id="b", type="fact", name="Claim B")
        g.add_edge(source_id="a", target_id="b", relation="derived_from")

        assert snapshot_path.exists()

        g2 = SimpleGraph(snapshot_path=snapshot_path)
        g2.load_snapshot()

        assert g2.get_node("a") is not None
        assert g2.get_node("b") is not None
        edge = g2.get_edge("a", "b")
        assert edge is not None
        assert edge.relation == "derived_from"

    def test_empty_graph(self):
        g = SimpleGraph()
        assert g.get_all_nodes() == []
        assert g.search_by_type("anything") == []
        assert g.search_by_relation("anything") == []
        assert g.find_path("a", "b") == []


class TestFindPathSimplePaths:
    """PROV-6: find_path must use per-path visited sets.

    A shared visited set silently drops alternate routes in diamond graphs
    and can only ever report one path per node; cycles must terminate and
    every returned path must be a simple path (no repeated node).
    """

    def _diamond(self) -> SimpleGraph:
        """Two distinct routes from a to e: a->b->d->e and a->c->d->e."""
        g = SimpleGraph()
        for nid in ("a", "b", "c", "d", "e"):
            g.add_node(id=nid, type="n", name=nid.upper())
        g.add_edge(source_id="a", target_id="b", relation="r")
        g.add_edge(source_id="a", target_id="c", relation="r")
        g.add_edge(source_id="b", target_id="d", relation="r")
        g.add_edge(source_id="c", target_id="d", relation="r")
        g.add_edge(source_id="d", target_id="e", relation="r")
        return g

    def test_diamond_returns_both_paths(self):
        g = self._diamond()
        paths = g.find_path("a", "e", max_depth=4)
        signatures = {tuple(n.id for n in path) for path in paths}
        assert signatures == {("a", "b", "d", "e"), ("a", "c", "d", "e")}

    def test_diamond_paths_are_simple_and_shortest_first(self):
        """BFS enumeration keeps paths sorted by length (shortest first)."""
        g = SimpleGraph()
        for nid in ("a", "b", "c", "d", "e"):
            g.add_node(id=nid, type="n", name=nid.upper())
        g.add_edge(source_id="a", target_id="b", relation="r")  # direct a->b
        g.add_edge(source_id="a", target_id="c", relation="r")
        g.add_edge(source_id="c", target_id="b", relation="r")
        lengths = [len(p) for p in g.find_path("a", "b", max_depth=4)]
        assert lengths == sorted(lengths)
        assert all(len(p) == len(set(n.id for n in p)) for p in g.find_path("a", "b", max_depth=4))

    def test_cycle_terminates_and_returns_only_simple_paths(self):
        """a->b->c->a cycle; the only a->e route must not loop forever."""
        g = SimpleGraph()
        for nid in ("a", "b", "c", "d", "e"):
            g.add_node(id=nid, type="n", name=nid.upper())
        g.add_edge(source_id="a", target_id="b", relation="r")
        g.add_edge(source_id="b", target_id="c", relation="r")
        g.add_edge(source_id="c", target_id="a", relation="r")  # cycle back
        g.add_edge(source_id="c", target_id="d", relation="r")
        g.add_edge(source_id="d", target_id="e", relation="r")

        paths = g.find_path("a", "e", max_depth=6)
        assert paths == [[g.get_node(n) for n in ("a", "b", "c", "d", "e")]]
        # Every path is simple even when cycles exist.
        for path in paths:
            ids = [n.id for n in path]
            assert len(ids) == len(set(ids))

    def test_self_loop_does_not_duplicate_paths(self):
        g = SimpleGraph()
        for nid in ("a", "b"):
            g.add_node(id=nid, type="n", name=nid.upper())
        g.add_edge(source_id="a", target_id="a", relation="self")
        g.add_edge(source_id="a", target_id="b", relation="r")
        paths = g.find_path("a", "b", max_depth=4)
        assert len(paths) == 1
        assert [n.id for n in paths[0]] == ["a", "b"]

    def test_cycle_route_still_found_via_other_branch(self):
        """Cycles prune only the looping branch, not other simple routes."""
        g = SimpleGraph()
        for nid in ("a", "b", "c", "x", "y"):
            g.add_node(id=nid, type="n", name=nid.upper())
        # a->b->c->a is a cycle; the real route is a->x->y.
        g.add_edge(source_id="a", target_id="b", relation="r")
        g.add_edge(source_id="b", target_id="c", relation="r")
        g.add_edge(source_id="c", target_id="a", relation="r")
        g.add_edge(source_id="a", target_id="x", relation="r")
        g.add_edge(source_id="x", target_id="y", relation="r")
        paths = g.find_path("a", "y", max_depth=6)
        assert [n.id for n in paths[0]] == ["a", "x", "y"]


class TestFromDictValidation:
    """PROV-5: from_dict must validate the FULL snapshot before replacing.

    A malformed snapshot raises and MUST NOT change the current in-memory
    graph (the old implementation cleared nodes first, then half-loaded).
    """

    def _populated(self) -> SimpleGraph:
        g = SimpleGraph()
        g.add_node(id="a", type="entity", name="A")
        g.add_node(id="b", type="entity", name="B")
        g.add_edge(source_id="a", target_id="b", relation="r")
        return g

    @pytest.mark.parametrize(
        "bad",
        [
            None,
            [],
            {"nodes": "nope", "edges": []},
            {"nodes": {}, "edges": "nope"},
            {"nodes": {1: {"id": "a"}}, "edges": []},
            {"nodes": {"a": "not-a-dict"}, "edges": []},
            {"nodes": {"a": {"id": "a", "type": 5, "name": "A"}}, "edges": []},
            {
                "nodes": {"a": {"id": "a", "type": "e", "name": "A", "attributes": []}},
                "edges": [],
            },
            {
                "nodes": {"a": {"id": "a", "type": "e", "name": "A"}},
                "edges": [{"source_id": "a"}],
            },
            {
                "nodes": {"a": {"id": "a", "type": "e", "name": "A"}},
                "edges": [{"source_id": "a", "target_id": 7}],
            },
            # Dangling edge: endpoint not among the nodes.
            {
                "nodes": {"a": {"id": "a", "type": "e", "name": "A"}},
                "edges": [{"source_id": "a", "target_id": "missing"}],
            },
            # Duplicate node id inside the node payloads.
            {"nodes": {"k1": {"id": "dup"}, "k2": {"id": "dup"}}, "edges": []},
        ],
    )
    def test_malformed_snapshot_does_not_change_old_graph(self, bad):
        g = self._populated()
        before = g.to_dict()
        with pytest.raises(ValueError):
            g.from_dict(bad)
        assert g.to_dict() == before
        assert g.get_node("a") is not None
        assert g.get_edge("a", "b") is not None

    def test_valid_snapshot_replaces_state(self):
        g = self._populated()
        g.from_dict(
            {
                "nodes": {"x": {"id": "x", "type": "e", "name": "X"}},
                "edges": [],
            }
        )
        assert g.get_node("x") is not None
        assert g.get_node("a") is None

    def test_malformed_json_file_load_keeps_old_graph(self, tmp_path):
        path = tmp_path / "graph.json"
        g = SimpleGraph(snapshot_path=path)
        g.add_node(id="keep", type="e", name="Keep")
        path.write_text("{ this is not json", encoding="utf-8")
        g.load_snapshot()
        assert g.get_node("keep") is not None

    def test_structurally_invalid_file_load_keeps_old_graph(self, tmp_path):
        path = tmp_path / "graph.json"
        g = SimpleGraph(snapshot_path=path)
        g.add_node(id="keep", type="e", name="Keep")
        path.write_text(
            json.dumps({"nodes": {"a": {"id": "a", "type": "e", "name": "A"}},
                        "edges": [{"source_id": "a", "target_id": "ghost"}]}),
            encoding="utf-8",
        )
        g.load_snapshot()
        assert g.get_node("keep") is not None  # stale graph survived, no partial load


class TestSnapshotCrashSafety:
    """MIG-6: a crash/failure mid-write never leaves a torn graph.json."""

    def _assert_only_valid_json_in_dir(self, snapshot_path):
        if snapshot_path.exists():
            json.loads(snapshot_path.read_text(encoding="utf-8"))
        keep = {snapshot_path.name, snapshot_path.name + ".lock"}
        leftovers = [p.name for p in snapshot_path.parent.iterdir() if p.name not in keep]
        assert leftovers == [], f"stray temp files left behind: {leftovers}"

    def _tmp_leftovers(self, snapshot_path):
        keep = {snapshot_path.name, snapshot_path.name + ".lock"}
        return [p.name for p in snapshot_path.parent.iterdir() if p.name not in keep]

    def test_replace_failure_leaves_old_file_intact_and_no_tmp(self, tmp_path, monkeypatch):
        import os

        snapshot_path = tmp_path / "graph.json"
        g = SimpleGraph(snapshot_path=snapshot_path)
        g.add_node(id="a", type="e", name="A")
        original = snapshot_path.read_bytes()

        def boom(src, dst):
            raise OSError("injected crash before atomic replace")

        monkeypatch.setattr(os, "replace", boom)
        # Failure is logged, never raised; the old file is untouched.
        g.add_node(id="b", type="e", name="B")
        assert snapshot_path.read_bytes() == original
        assert g.get_node("b") is not None  # mutation kept in memory/journal
        self._assert_only_valid_json_in_dir(snapshot_path)

        # Next successful write retries the pending journal and lands B.
        monkeypatch.undo()
        g.add_node(id="c", type="e", name="C")
        data = json.loads(snapshot_path.read_text(encoding="utf-8"))
        assert set(data["nodes"]) == {"a", "b", "c"}
        self._assert_only_valid_json_in_dir(snapshot_path)

    def test_write_failure_inside_batch_rolls_back_memory_and_journal(self, tmp_path, monkeypatch):
        """A raising save_snapshot inside sync_facts_batch rolls memory back."""
        from memory_server.router.graph_router import GraphRouter

        path = tmp_path / "graph.json"
        g = SimpleGraph(snapshot_path=path)
        g.add_node(id="seed", type="e", name="Seed")
        before = g.to_dict()

        def boom(*args, **kwargs):
            raise RuntimeError("disk full")

        g.save_snapshot = boom
        with pytest.raises(RuntimeError):
            GraphRouter(graph=g).sync_facts_batch(
                [{"subject": "A", "predicate": "is", "object": "B"}]
            )
        assert g.to_dict() == before
        assert g._journal == []
        # The on-disk snapshot still holds only the seed node.
        data = json.loads(path.read_text(encoding="utf-8"))
        assert set(data["nodes"]) == {"seed"}

    def test_killed_writer_never_leaves_torn_json(self, tmp_path):
        """A hard-killed concurrent writer leaves old-or-new valid JSON."""
        import subprocess
        import sys
        import textwrap
        import time

        repo_root = Path(__file__).resolve().parents[1]
        code = textwrap.dedent(
            """
            import json, sys, time
            from memory_server.providers.graph_provider import SimpleGraph
            path = sys.argv[1]
            g = SimpleGraph(snapshot_path=path)
            g.load_snapshot()
            i = 0
            while True:
                g.add_node(id=f"killed-{i}", type="e", name=f"K{i}", attributes={"pad": "x" * 20000})
                i += 1
                if i % 5 == 0:
                    time.sleep(0.001)
            """
        )
        snapshot_path = tmp_path / "graph.json"
        for attempt in range(3):
            if snapshot_path.exists():
                snapshot_path.unlink()
            proc = subprocess.Popen(
                [sys.executable, "-c", code, str(snapshot_path)],
                cwd=repo_root,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            # Wait until the writer has published at least one snapshot.
            deadline = time.monotonic() + 15
            while not snapshot_path.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            time.sleep(0.05)  # let it get into a write cycle
            proc.kill()
            proc.wait(timeout=10)
            assert snapshot_path.exists(), "writer never published a snapshot"
            # graph.json must never be torn: valid JSON, old-or-new content.
            data = json.loads(snapshot_path.read_text(encoding="utf-8"))  # must not raise
            assert "nodes" in data
            # A crash can leave a unique .tmp behind; the next locked writer
            # must clean it up (never overwrite a live writer's temp file).
            writer = SimpleGraph(snapshot_path=snapshot_path)
            writer.load_snapshot()
            writer.add_node(id=f"after-{attempt}", type="e", name=f"A{attempt}")
            json.loads(snapshot_path.read_text(encoding="utf-8"))
            assert self._tmp_leftovers(snapshot_path) == []


class TestInterProcessPersistence:
    """PROV-6 / MIG-6: concurrent processes must not lose each other's writes.

    Runs REAL separate Python processes (not threads, not asyncio.Lock)
    against one shared graph.json: both writers' changes survive a reopen,
    and deletions performed by one process are not resurrected by another
    process holding a stale in-memory snapshot.
    """

    _WRITER_CODE = """
import sys
from memory_server.providers.graph_provider import SimpleGraph
from memory_server.router.graph_router import GraphRouter

path, tag, count, mode = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4]
g = SimpleGraph(snapshot_path=path)
g.load_snapshot()
router = GraphRouter(graph=g)
if mode == "delete":
    for node in list(g.get_all_nodes()):
        if node.id.startswith("victim-"):
            g.delete_node(node.id)
for i in range(count):
    router.sync_fact(subject=f"{tag} Subject {i}", predicate="rel", object=f"{tag} Object {i}")
"""

    def _run_writer(self, path, tag, count, mode="add"):
        import subprocess
        import sys

        repo_root = Path(__file__).resolve().parents[1]
        subprocess.run(
            [sys.executable, "-c", self._WRITER_CODE, str(path), tag, str(count), mode],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
            timeout=180,
        )

    def test_two_processes_both_changes_survive_reopen(self, tmp_path):
        import subprocess
        import sys

        path = tmp_path / "graph.json"
        procs = [
            subprocess.Popen(
                [sys.executable, "-c", self._WRITER_CODE, str(path), tag, "25", "add"],
                cwd=Path(__file__).resolve().parents[1],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            for tag in ("P1", "P2")
        ]
        for proc in procs:
            proc.wait(timeout=180)

        reopened = SimpleGraph(snapshot_path=path)
        reopened.load_snapshot()
        ids = {n.id for n in reopened.get_all_nodes()}
        # P1 and P2 each add 25 subject + 25 object nodes == 100 total.
        assert len(ids) == 100, f"lost updates: got {len(ids)} nodes"
        assert {"p1-subject-0", "p1-object-24", "p2-subject-0", "p2-object-24"} <= ids

    def test_stale_process_does_not_resurrect_deletions(self, tmp_path):
        path = tmp_path / "graph.json"

        # Seed a graph, then keep a STALE in-memory copy (the "gateway").
        seed = SimpleGraph(snapshot_path=path)
        for i in range(3):
            seed.add_node(id=f"victim-{i}", type="e", name=f"Victim {i}")
        seed.add_node(id="base-a", type="e", name="Base A")
        seed.add_edge(source_id="victim-0", target_id="base-a", relation="r")

        stale_gateway = SimpleGraph(snapshot_path=path)
        stale_gateway.load_snapshot()  # snapshot contains the victims

        # A second process deletes the victims and adds its own facts.
        self._run_writer(path, "Del", 3, mode="delete")

        # The stale gateway now writes new facts from its OLD in-memory
        # snapshot; without a journal-rebase it would resurrect victims.
        GraphRouter(graph=stale_gateway).sync_fact(
            subject="Gateway New", predicate="adds", object="Thing"
        )

        reopened = SimpleGraph(snapshot_path=path)
        reopened.load_snapshot()
        ids = {n.id for n in reopened.get_all_nodes()}
        assert not any(nid.startswith("victim-") for nid in ids), "deleted node resurrected"
        assert "base-a" in ids
        assert "del-subject-0" in ids and "del-object-2" in ids
        assert "gateway-new" in ids and "thing" in ids


class TestConcurrentWriteCost:
    """Reasonable transaction cost on a moderately sized graph (tmp only)."""

    def test_batched_ingest_on_medium_graph_completes(self, tmp_path):
        import time

        from memory_server.router.graph_router import GraphRouter

        path = tmp_path / "graph.json"
        g = SimpleGraph(snapshot_path=path)
        router = GraphRouter(graph=g)
        facts = [
            {"subject": f"Topic {n}", "predicate": "rel", "object": f"Entity {n}"}
            for n in range(1500)
        ]
        started = time.monotonic()
        for i in range(0, len(facts), 100):
            router.sync_facts_batch(facts[i : i + 100])
        elapsed = time.monotonic() - started

        reopened = SimpleGraph(snapshot_path=path)
        reopened.load_snapshot()
        assert len(reopened.get_all_nodes()) == 3000
        assert len(reopened.to_dict()["edges"]) == 1500
        # Generous bound: correctness is the point, this only guards against
        # accidental quadratic blowups from per-write full-file rewrites.
        assert elapsed < 30.0, f"batch ingest took {elapsed:.2f}s"
        print(f"\n[cost] 1500 facts / 3000 nodes batch ingest: {elapsed:.3f}s")
