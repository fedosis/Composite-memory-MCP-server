#!/usr/bin/env python3
"""One-time graph reindex: backfill graph.json from the SQLite facts table.

Reads every fact (subject, predicate, object) from the CMMS SQLite store and
feeds them through ``GraphRouter.sync_facts_batch`` in chunks, merging into
the existing live graph (never replacing it). This closes the historical
SQL/graph backlog that predates the outbox batch-sync fix.

Graph-only: the script opens SQLite read-only and never touches LanceDB.

Usage:
    python scripts/reindex_graph.py [--db data/memory.db] [--graph data/graph.json]
                                    [--chunk-size 1000] [--dry-run]

Examples:
    # Real pass against the live store (defaults resolve to the repo data dir)
    python scripts/reindex_graph.py

    # Preview what would change without writing anything
    python scripts/reindex_graph.py --dry-run

    # Larger batches, fewer snapshot writes
    python scripts/reindex_graph.py --chunk-size 5000

Idempotent: sync_facts_batch dedups nodes by id and edges by
(source_id, target_id, relation), so re-running adds nothing new.

Caveats:
- The running gateway caches the graph in memory (loads graph.json once at
  startup) and rewrites the file wholesale on its next mutation. A backfill
  done while the gateway holds a pre-backfill snapshot can therefore be
  reverted by the gateway's next save. If that happens, re-run this script;
  for durable closure, restart the gateway afterwards (from a shell outside
  the gateway process, e.g. ``systemctl --user restart hermes-gateway.service``).
- This is a graph-only pass: SQLite is opened read-only and LanceDB is never
  touched.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from pathlib import Path

from memory_server.providers.graph_provider import SimpleGraph
from memory_server.router.graph_router import GraphRouter


def _repo_root() -> Path:
    """Return the checked-out CMMS repository root (same as paths.cmms_repo_root)."""
    return Path(__file__).resolve().parents[1]


def _iter_facts(db_path: Path, chunk_size: int):
    """Yield lists of fact dicts from the facts table, read-only.

    The connection is opened in ``mode=ro`` so the pass can never write to
    SQLite. Rows are read in one consistent snapshot query; the gateway may
    keep writing new facts afterwards — the sync is additive and idempotent,
    so those are simply picked up by a later run.
    """
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT subject, predicate, object FROM facts "
            "WHERE subject IS NOT NULL AND object IS NOT NULL "
            "AND trim(subject) != '' AND trim(object) != '' "
            "ORDER BY rowid"
        )
        chunk: list[dict[str, str]] = []
        for row in cur:
            chunk.append(
                {
                    "subject": row["subject"],
                    "predicate": row["predicate"] or "",
                    "object": row["object"],
                }
            )
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk
    finally:
        conn.close()


def reindex(
    db_path: Path,
    graph_path: Path,
    *,
    chunk_size: int = 1000,
    dry_run: bool = False,
    out=print,
) -> dict:
    """Merge every fact from SQLite into the graph snapshot.

    Args:
        db_path: Path to the SQLite database (read-only access).
        graph_path: Path to the graph.json snapshot to merge into.
        chunk_size: Facts per ``sync_facts_batch`` call (one snapshot write per call).
        dry_run: Compute the diff but do not mutate or write the graph.
        out: Callable for progress/result lines.

    Returns:
        Dict with before/after node/edge counts, added counts, elapsed seconds,
        and facts read.
    """
    graph_path = Path(graph_path)
    started = time.monotonic()

    # --- 1. Load existing graph (merge, never replace) -------------------
    # Guard against silently clobbering a corrupt snapshot: SimpleGraph's
    # load_snapshot logs and leaves an empty graph on parse/validation
    # errors, which a later save would otherwise rebuild from nothing over
    # the corrupt file. Validate the full snapshot shape up front instead
    # and abort rather than reindex over it (MIG-6 / PR-11).
    if graph_path.exists():
        try:
            import json as _json

            try:
                raw = _json.loads(graph_path.read_text(encoding="utf-8"))
            except ValueError:
                raise SystemExit(
                    f"Aborting: {graph_path} exists but is not valid JSON — "
                    "refusing to reindex over a possibly corrupt snapshot. "
                    "Inspect the file first."
                ) from None
            if not isinstance(raw, dict) or "nodes" not in raw:
                raise SystemExit(
                    f"Aborting: {graph_path} exists but is not a valid graph "
                    "snapshot (missing 'nodes' key). Inspect the file first."
                )
            # Full structural validation (nodes/edges shape, dangling edge
            # endpoints, duplicate ids) — same rules SimpleGraph.from_dict
            # enforces, so a malformed file can never be half-loaded and
            # then republished by this process.
            SimpleGraph._build_state_from(raw)
        except SystemExit:
            raise
        except Exception as exc:
            raise SystemExit(
                f"Aborting: {graph_path} exists but is structurally invalid "
                f"({exc}) — refusing to reindex over a possibly corrupt "
                "snapshot. Inspect the file first."
            ) from None

    graph = SimpleGraph(snapshot_path=graph_path)
    if graph_path.exists():
        graph.load_snapshot()

    before_nodes = len(graph.get_all_nodes())
    before_edges = len(graph.to_dict()["edges"])

    if dry_run:
        # Compute what the pass would add without mutating the graph.
        existing_nodes = {n.id for n in graph.get_all_nodes()}
        existing_edges = {
            (e["source_id"], e["target_id"], e["relation"])
            for e in graph.to_dict()["edges"]
        }
        facts_read = 0
        new_nodes: set[str] = set()
        new_edges: set[tuple[str, str, str]] = set()
        for chunk in _iter_facts(db_path, chunk_size):
            for triple in chunk:
                facts_read += 1
                sid = GraphRouter._to_node_id(triple["subject"])
                tid = GraphRouter._to_node_id(triple["object"])
                if sid not in existing_nodes:
                    new_nodes.add(sid)
                if tid not in existing_nodes:
                    new_nodes.add(tid)
                key = (sid, tid, triple["predicate"])
                if key not in existing_edges:
                    new_edges.add(key)
        elapsed = time.monotonic() - started
        out(
            f"DRY-RUN — no changes written.\n"
            f"  facts read:      {facts_read}\n"
            f"  graph before:    {before_nodes} nodes, {before_edges} edges\n"
            f"  would add:       {len(new_nodes)} nodes, {len(new_edges)} edges\n"
            f"  graph after:     {before_nodes + len(new_nodes)} nodes, "
            f"{before_edges + len(new_edges)} edges\n"
            f"  elapsed:         {elapsed:.2f}s"
        )
        return {
            "dry_run": True,
            "facts_read": facts_read,
            "before_nodes": before_nodes,
            "before_edges": before_edges,
            "added_nodes": len(new_nodes),
            "added_edges": len(new_edges),
            "elapsed_seconds": elapsed,
        }

    # --- 2. Feed facts through the batch sync (one snapshot per chunk) ---
    router = GraphRouter(graph=graph)
    facts_read = 0
    for i, chunk in enumerate(_iter_facts(db_path, chunk_size), start=1):
        facts_read += len(chunk)
        router.sync_facts_batch(chunk)  # mutates in-memory, one save_snapshot()
        out(
            f"  chunk {i}: {facts_read}/{_total_facts(db_path)} facts "
            f"→ {len(graph.get_all_nodes())} nodes"
        )

    # --- 3. Final counts -------------------------------------------------
    after_nodes = len(graph.get_all_nodes())
    after_edges = len(graph.to_dict()["edges"])
    elapsed = time.monotonic() - started

    out(
        f"REINDEX COMPLETE\n"
        f"  facts read:      {facts_read}\n"
        f"  graph before:    {before_nodes} nodes, {before_edges} edges\n"
        f"  graph after:     {after_nodes} nodes, {after_edges} edges\n"
        f"  added:           {after_nodes - before_nodes} nodes, "
        f"{after_edges - before_edges} edges\n"
        f"  graph file:      {graph_path}\n"
        f"  elapsed:         {elapsed:.2f}s"
    )
    return {
        "dry_run": False,
        "facts_read": facts_read,
        "before_nodes": before_nodes,
        "before_edges": before_edges,
        "after_nodes": after_nodes,
        "after_edges": after_edges,
        "added_nodes": after_nodes - before_nodes,
        "added_edges": after_edges - before_edges,
        "elapsed_seconds": elapsed,
    }


def _total_facts(db_path: Path) -> int:
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        return int(
            conn.execute(
                "SELECT COUNT(*) FROM facts "
                "WHERE subject IS NOT NULL AND object IS NOT NULL "
                "AND trim(subject) != '' AND trim(object) != ''"
            ).fetchone()[0]
        )
    finally:
        conn.close()


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "One-time graph reindex: merge every SQLite fact into graph.json "
            "(graph-only, read-only on SQLite, idempotent)."
        )
    )
    root = _repo_root()
    parser.add_argument(
        "--db",
        type=Path,
        default=root / "data" / "memory.db",
        help=f"SQLite database path (default: {root / 'data' / 'memory.db'})",
    )
    parser.add_argument(
        "--graph",
        type=Path,
        default=root / "data" / "graph.json",
        help=f"Graph snapshot path to merge into (default: {root / 'data' / 'graph.json'})",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1000,
        help="Facts per sync_facts_batch call / snapshot write (default: 1000)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview added nodes/edges without writing the graph",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    db_path = Path(args.db).resolve()
    graph_path = Path(args.graph).resolve()

    if not db_path.exists():
        print(f"❌ SQLite database not found: {db_path}", file=sys.stderr)
        return 1
    if not graph_path.exists():
        print(f"❌ Graph snapshot not found: {graph_path}", file=sys.stderr)
        return 1

    reindex(db_path, graph_path, chunk_size=args.chunk_size, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
