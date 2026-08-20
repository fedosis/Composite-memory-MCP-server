"""Tests for scripts/reindex_graph.py (SPEC: cmms-graph-reindex).

Covers:
- AC2: re-running the reindex is idempotent — node/edge counts stable.
- AC3: existing graph content is preserved (merge, not replace).
- AC4: only graph.json changes — SQLite is opened read-only.
- dry-run: reports the diff without writing.
"""

import importlib.util
import sqlite3
from pathlib import Path
from types import ModuleType

import pytest  # noqa: F401  (kept for symmetry with sibling test modules)


def _load_script() -> ModuleType:
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "reindex_graph.py"
    spec = importlib.util.spec_from_file_location("reindex_graph", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _make_db(path: Path, rows: list[tuple[str, str, str]]) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE facts ("
        "id TEXT PRIMARY KEY, subject TEXT, predicate TEXT, object TEXT, "
        "confidence REAL DEFAULT 1.0, source TEXT, creator TEXT, "
        "created_at TEXT, updated_at TEXT, verification_status TEXT, "
        "lifecycle_state TEXT, version TEXT)"
    )
    conn.executemany(
        "INSERT INTO facts (id, subject, predicate, object) "
        "VALUES (?, ?, ?, ?)",
        [(str(i), s, p, o) for i, (s, p, o) in enumerate(rows)],
    )
    conn.commit()
    conn.close()


def _make_graph(path: Path, nodes: dict, edges: list[dict]) -> None:
    path.write_text(
        __import__("json").dumps({"nodes": nodes, "edges": edges}),
        encoding="utf-8",
    )


FACTS = [
    ("Docker", "is", "container"),
    ("Docker", "runs_on", "OMV8"),
    ("PostgreSQL", "is", "database"),
    ("Kubernetes", "orchestrates", "Docker"),
]


class TestReindexGraph:
    def test_merges_and_preserves_existing_content(self, tmp_path):
        """AC3: existing nodes/edges survive; new fact nodes/edges are added."""
        mod = _load_script()
        db = tmp_path / "memory.db"
        graph_file = tmp_path / "graph.json"
        _make_db(db, FACTS)
        _make_graph(
            graph_file,
            nodes={
                "omv8": {"id": "omv8", "type": "entity", "name": "OMV8", "attributes": {}},
            },
            edges=[
                {"source_id": "omv8", "target_id": "omv8", "relation": "self", "attributes": {}},
            ],
        )

        result = mod.reindex(db, graph_file)

        assert result["after_nodes"] == 6  # omv8 (shared) + 5 new fact entities
        assert result["after_edges"] == 5  # self + 4 fact edges
        # Existing content preserved.
        data = __import__("json").loads(graph_file.read_text(encoding="utf-8"))
        assert "omv8" in data["nodes"]
        assert any(e["relation"] == "self" for e in data["edges"])
        # New fact content present.
        assert "docker" in data["nodes"]
        assert any(
            e["source_id"] == "docker" and e["relation"] == "runs_on"
            for e in data["edges"]
        )

    def test_rerun_is_idempotent(self, tmp_path):
        """AC2: second pass adds no nodes/edges."""
        mod = _load_script()
        db = tmp_path / "memory.db"
        graph_file = tmp_path / "graph.json"
        _make_db(db, FACTS)
        _make_graph(graph_file, nodes={}, edges=[])

        first = mod.reindex(db, graph_file)
        second = mod.reindex(db, graph_file)

        assert first["after_nodes"] == second["after_nodes"] == 6
        assert first["after_edges"] == second["after_edges"] == 4
        assert second["added_nodes"] == 0
        assert second["added_edges"] == 0

    def test_dry_run_writes_nothing(self, tmp_path):
        mod = _load_script()
        db = tmp_path / "memory.db"
        graph_file = tmp_path / "graph.json"
        _make_db(db, FACTS)
        _make_graph(graph_file, nodes={}, edges=[])

        result = mod.reindex(db, graph_file, dry_run=True)

        assert result["dry_run"] is True
        assert result["added_nodes"] == 6
        assert result["added_edges"] == 4
        # Content unchanged (SimpleGraph may rewrite JSON formatting on load,
        # so compare parsed content rather than raw bytes).
        assert __import__("json").loads(graph_file.read_text(encoding="utf-8")) == {
            "nodes": {},
            "edges": [],
        }

    def test_db_opened_read_only(self, tmp_path):
        """AC4: the pass never writes to SQLite."""
        mod = _load_script()
        db = tmp_path / "memory.db"
        graph_file = tmp_path / "graph.json"
        _make_db(db, FACTS)
        _make_graph(graph_file, nodes={}, edges=[])
        before = db.read_bytes()

        mod.reindex(db, graph_file)

        assert db.read_bytes() == before
