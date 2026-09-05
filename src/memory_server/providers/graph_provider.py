"""Pure-Python in-memory graph engine for entity relation storage.

No external dependencies — uses Python dicts + sets.
Designed for fast testing and simple persistence via JSON dump/load.

Persistence protocol (PR-11 / PROV-5 / PROV-6 / MIG-6):

Snapshots are shared between processes (the gateway process and the
``scripts/reindex_graph.py`` process both read-modify-write the same
``graph.json``).  To keep concurrent writers from clobbering each other
every persisted mutation is a *transaction*:

    acquire inter-process lock on ``<snapshot>.lock``
    -> reload the current on-disk state (fresh base)
    -> replay this instance's pending mutation journal onto the fresh base
    -> write a UNIQUE temp file (never a shared ``.tmp`` name)
    -> atomically replace the snapshot
    -> refresh this instance's in-memory state to the merged result
    -> release the lock

A lock held only around ``replace`` would not help: the in-memory snapshot
of a long-lived gateway is stale by the time it mutates, and writing that
stale whole-memory state would revert the other process's changes.  The
journal keeps the *delta* of local mutations, and the delta is applied to
the fresh on-disk state under the lock, so deletions on disk never
resurrect and a reindexed generation is never published from a stale copy.
``load_snapshot`` also takes the lock so a reader never races a writer.

The lock is an advisory ``fcntl.flock`` on POSIX (``msvcrt`` fallback on
Windows) — stdlib only, so the package gains no new dependency.
"""

from __future__ import annotations

import copy
import json
import logging
import os
import threading
import time
import uuid
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Optional

logger = logging.getLogger(__name__)

try:  # POSIX
    import fcntl as _fcntl
except ImportError:  # pragma: no cover - Windows
    _fcntl = None

# How long a writer/reader waits for the inter-process snapshot lock before
# giving up (writes are skipped with an error log; a crash releases flock).
GRAPH_LOCK_TIMEOUT = float(os.environ.get("MEMORY_GRAPH_LOCK_TIMEOUT", "30"))


@dataclass
class GraphNode:
    """A node in the knowledge graph representing an entity."""

    id: str
    type: str
    name: str
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class GraphEdge:
    """A directed edge connecting two nodes in the knowledge graph."""

    source_id: str
    target_id: str
    relation: str
    attributes: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Inter-process lock helpers (stdlib only).
# ---------------------------------------------------------------------------


def _snapshot_lock_path(snapshot_path: Path) -> Path:
    """Return the advisory lock file path for a snapshot file."""
    return snapshot_path.with_name(snapshot_path.name + ".lock")


@contextmanager
def _file_lock(lock_path: Path, timeout: float = GRAPH_LOCK_TIMEOUT) -> Iterator[None]:
    """Acquire an exclusive advisory lock on *lock_path*.

    Blocking-with-timeout: if the lock cannot be acquired within *timeout*
    seconds a ``TimeoutError`` is raised (callers treat that as a failed
    write / skipped load, never as an unlocked write).  The OS releases the
    lock automatically if the process dies, so a crashed writer can never
    deadlock later writers.
    """
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fh = open(lock_path, "a+b")  # noqa: PTH123 - file must be created next to snapshot
    try:
        if _fcntl is not None:
            deadline = time.monotonic() + timeout
            while True:
                try:
                    _fcntl.flock(fh.fileno(), _fcntl.LOCK_EX | _fcntl.LOCK_NB)
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError(
                            f"timed out waiting for graph lock {lock_path}"
                        ) from None
                    time.sleep(0.05)
            try:
                yield
            finally:
                _fcntl.flock(fh.fileno(), _fcntl.LOCK_UN)
        else:  # pragma: no cover - Windows fallback
            import msvcrt

            fh.seek(0, os.SEEK_END)
            if fh.tell() == 0:
                fh.write(b"\0")
                fh.flush()
            fh.seek(0)
            deadline = time.monotonic() + timeout
            while True:
                try:
                    msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)  # type: ignore[attr-defined]
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError(
                            f"timed out waiting for graph lock {lock_path}"
                        ) from None
                    time.sleep(0.05)
            try:
                yield
            finally:
                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)  # type: ignore[attr-defined]
    finally:
        fh.close()


def _cleanup_stale_tmp(snapshot_path: Path) -> None:
    """Remove leftover temp files for *snapshot_path*.

    MUST be called while holding the inter-process lock: any ``.tmp`` file
    for this snapshot that exists then is a leftover from a crashed writer
    (a live writer holds the lock), so removing it is safe.  Both the
    current unique-name scheme (``.<name>.*.tmp``) and the pre-PR-11 fixed
    name (``graph.json.tmp``) are cleaned up.
    """
    try:
        for leftover in snapshot_path.parent.glob(f".{snapshot_path.name}.*.tmp"):
            try:
                leftover.unlink(missing_ok=True)
            except OSError:
                pass
        legacy = snapshot_path.with_name(snapshot_path.name + ".tmp")
        try:
            legacy.unlink(missing_ok=True)
        except OSError:
            pass
    except OSError:
        pass


class _MalformedSnapshotError(ValueError):
    """Raised when an on-disk snapshot cannot be parsed/validated."""


class SimpleGraph:
    """Pure-Python in-memory graph engine.

    Stores nodes and edges in dicts and adjacency sets.
    Supports JSON serialization for persistence.
    """

    def __init__(self, snapshot_path: str | Path | None = None) -> None:
        self._nodes: dict[str, GraphNode] = {}
        self._edges: dict[str, dict[str, list[GraphEdge]]] = {}  # source -> target -> [edges]
        self._snapshot_path = Path(snapshot_path) if snapshot_path else None
        self._suspend_persistence = False
        # Pending local mutations not yet on disk (see module docstring).  The
        # journal is only maintained for snapshot-backed graphs; memory-only
        # graphs never persist so they never need one.
        self._journal: list[tuple[Any, ...]] = []
        # Set when from_dict() replaces the whole state: the next persist is a
        # full publish of the in-memory state, not a journal rebase.
        self._full_publish_pending = False
        # Serializes transactions on this instance within one process; the
        # file lock serializes across processes.
        self._thread_lock = threading.Lock()

    @contextmanager
    def suspend_persistence(self) -> Iterator[None]:
        """Temporarily suspend snapshot writes (batch mutation support).

        Transactional: on ANY exception inside the with-block the in-memory
        ``_nodes``/``_edges`` AND the pending-mutation journal are restored
        to the state captured on entry and the exception is re-raised, so a
        failed batch never leaves partial mutations behind (Card 3b, SPEC
        scope 2). A deep copy is required — ``add_edge`` appends into
        nested lists, so a shallow dict copy would not restore them. Pair
        with ``save_snapshot()``.
        """
        snapshot_nodes = copy.deepcopy(self._nodes)
        snapshot_edges = copy.deepcopy(self._edges)
        snapshot_journal = list(self._journal)
        snapshot_publish = self._full_publish_pending
        self._suspend_persistence = True
        try:
            yield
        except Exception:
            self._nodes.clear()
            self._nodes.update(snapshot_nodes)
            self._edges.clear()
            self._edges.update(snapshot_edges)
            self._journal = snapshot_journal
            self._full_publish_pending = snapshot_publish
            raise
        finally:
            self._suspend_persistence = False

    # --- Node operations ---

    def add_node(
        self,
        id: str,
        type: str,
        name: str,
        attributes: dict[str, Any] | None = None,
    ) -> GraphNode:
        """Add a node to the graph.

        Args:
            id: Unique node identifier.
            type: Node type (e.g., "server", "project", "service").
            name: Human-readable node name.
            attributes: Optional dict of additional attributes.

        Returns:
            The created GraphNode.

        Raises:
            ValueError: If a node with the same id already exists.
        """
        if id in self._nodes:
            raise ValueError(f"Node '{id}' already exists")
        node = GraphNode(id=id, type=type, name=name, attributes=attributes or {})
        self._nodes[id] = node
        if self._snapshot_path is not None:
            self._journal.append(("add_node", id, type, name, node.attributes))
        self._persist_if_needed()
        return node

    def get_node(self, id: str) -> Optional[GraphNode]:
        """Get a node by id.

        Args:
            id: Node identifier.

        Returns:
            GraphNode if found, None otherwise.
        """
        return self._nodes.get(id)

    def delete_node(self, id: str) -> None:
        """Delete a node and all its edges.

        Args:
            id: Node identifier.

        Raises:
            KeyError: If the node doesn't exist.
        """
        if id not in self._nodes:
            raise KeyError(f"Node '{id}' not found")
        # Remove all edges involving this node
        self._edges.pop(id, None)
        for source in list(self._edges.keys()):
            self._edges[source].pop(id, None)
            if not self._edges[source]:
                del self._edges[source]
        del self._nodes[id]
        if self._snapshot_path is not None:
            self._journal.append(("delete_node", id))
        self._persist_if_needed()

    def get_all_nodes(self) -> list[GraphNode]:
        """Get all nodes in the graph.

        Returns:
            List of all GraphNode objects.
        """
        return list(self._nodes.values())

    # --- Edge operations ---

    def add_edge(
        self,
        source_id: str,
        target_id: str,
        relation: str,
        attributes: dict[str, Any] | None = None,
    ) -> GraphEdge:
        """Add a directed edge between two nodes.

        Args:
            source_id: Source node id.
            target_id: Target node id.
            relation: Relation type (e.g., "hosts", "uses", "connects_to").
            attributes: Optional dict of edge attributes.

        Returns:
            The created GraphEdge.

        Raises:
            KeyError: If source or target node doesn't exist.
        """
        if source_id not in self._nodes:
            raise KeyError(f"Source node '{source_id}' not found")
        if target_id not in self._nodes:
            raise KeyError(f"Target node '{target_id}' not found")

        edge = GraphEdge(
            source_id=source_id,
            target_id=target_id,
            relation=relation,
            attributes=attributes or {},
        )

        if source_id not in self._edges:
            self._edges[source_id] = {}
        if target_id not in self._edges[source_id]:
            self._edges[source_id][target_id] = []
        self._edges[source_id][target_id].append(edge)
        if self._snapshot_path is not None:
            self._journal.append(("add_edge", source_id, target_id, relation, edge.attributes))
        self._persist_if_needed()

        return edge

    def get_edge(
        self,
        source_id: str,
        target_id: str,
        relation: str | None = None,
    ) -> Optional[GraphEdge]:
        """Get the first edge between two nodes, optionally filtered by relation.

        Args:
            source_id: Source node id.
            target_id: Target node id.
            relation: Optional relation filter. When provided, only edges with
                this relation are considered.

        Returns:
            First matching GraphEdge if any exist, None otherwise.
        """
        if source_id in self._edges and target_id in self._edges[source_id]:
            for edge in self._edges[source_id][target_id]:
                if relation is None or edge.relation == relation:
                    return edge
        return None

    def delete_edge(self, source_id: str, target_id: str) -> None:
        """Delete all edges between two nodes.

        Args:
            source_id: Source node id.
            target_id: Target node id.

        Raises:
            KeyError: If no edges exist between these nodes.
        """
        if (
            source_id not in self._edges
            or target_id not in self._edges[source_id]
        ):
            raise KeyError(f"Edge from '{source_id}' to '{target_id}' not found")
        del self._edges[source_id][target_id]
        if not self._edges[source_id]:
            del self._edges[source_id]
        if self._snapshot_path is not None:
            self._journal.append(("delete_edge", source_id, target_id))
        self._persist_if_needed()

    # --- Neighbor traversal ---

    def get_neighbors(
        self,
        node_id: str,
        relation: str | None = None,
    ) -> list[tuple[GraphNode, GraphEdge]]:
        """Get all neighbors of a node, optionally filtered by relation.

        Args:
            node_id: Node id to find neighbors for.
            relation: Optional relation type filter.

        Returns:
            List of (neighbor_node, edge) tuples.
        """
        if node_id not in self._nodes:
            return []

        neighbors: list[tuple[GraphNode, GraphEdge]] = []
        if node_id in self._edges:
            for target_id, edges in self._edges[node_id].items():
                for edge in edges:
                    if relation is None or edge.relation == relation:
                        target_node = self._nodes.get(target_id)
                        if target_node is not None:
                            neighbors.append((target_node, edge))

        # Also check for incoming edges (reverse direction)
        for source_id, targets in self._edges.items():
            if source_id == node_id:
                continue
            if node_id in targets:
                for edge in targets[node_id]:
                    if relation is None or edge.relation == relation:
                        source_node = self._nodes.get(source_id)
                        if source_node is not None:
                            neighbors.append((source_node, edge))

        return neighbors

    # --- Pathfinding ---

    def find_path(
        self,
        source_id: str,
        target_id: str,
        max_depth: int = 4,
    ) -> list[list[GraphNode]]:
        """Find all simple paths between two nodes up to max_depth.

        Uses BFS over (current node, path-so-far) states, so paths are
        returned shortest-first. The visited set is PER PATH (the current
        path's own nodes), never shared across frontier branches: a shared
        ``visited`` set silently drops alternative routes in diamond-shaped
        graphs (Astra audit, PROV-6). Because each branch only follows nodes
        not already on its own path, every returned path is a simple path
        and cyclic graphs cannot loop forever.

        Args:
            source_id: Starting node id.
            target_id: Target node id.
            max_depth: Maximum path length in edges (default 4).

        Returns:
            List of simple paths, where each path is a list of GraphNode
            objects ordered from source to target.
        """
        if source_id not in self._nodes or target_id not in self._nodes:
            return []

        if source_id == target_id:
            return [[self._nodes[source_id]]]

        # BFS over (node, simple-path-so-far) states.
        queue: deque[tuple[str, tuple[str, ...]]] = deque([(source_id, (source_id,))])
        paths: list[list[GraphNode]] = []

        while queue:
            current, path = queue.popleft()
            if len(path) - 1 >= max_depth:
                continue

            for target_id_candidate in self._edges.get(current, ()):
                # Per-path visited: a node already on THIS path would make a
                # cycle; it is allowed on other paths (diamond alternatives).
                if target_id_candidate in path:
                    continue
                new_path = path + (target_id_candidate,)
                if target_id_candidate == target_id:
                    paths.append([self._nodes[nid] for nid in new_path])
                elif len(new_path) - 1 < max_depth:
                    queue.append((target_id_candidate, new_path))

        return paths

    # --- Search ---

    def search_by_type(self, type: str) -> list[GraphNode]:
        """Find all nodes of a given type.

        Args:
            type: Node type to filter by.

        Returns:
            List of matching GraphNode objects.
        """
        return [n for n in self._nodes.values() if n.type == type]

    def search_by_relation(self, relation: str) -> list[GraphEdge]:
        """Find all edges with a given relation.

        Args:
            relation: Relation type to filter by.

        Returns:
            List of matching GraphEdge objects.
        """
        edges: list[GraphEdge] = []
        for targets in self._edges.values():
            for edge_list in targets.values():
                for edge in edge_list:
                    if edge.relation == relation:
                        edges.append(edge)
        return edges

    # --- Serialization ---

    def to_dict(self) -> dict[str, Any]:
        """Serialize graph to a JSON-compatible dict.

        Returns:
            Dict with "nodes" and "edges" keys.
        """
        return {
            "nodes": {
                nid: {
                    "id": n.id,
                    "type": n.type,
                    "name": n.name,
                    "attributes": n.attributes,
                }
                for nid, n in self._nodes.items()
            },
            "edges": [
                {
                    "source_id": e.source_id,
                    "target_id": e.target_id,
                    "relation": e.relation,
                    "attributes": e.attributes,
                }
                for targets in self._edges.values()
                for edge_list in targets.values()
                for e in edge_list
            ],
        }

    def from_dict(self, data: dict[str, Any]) -> None:
        """Load graph state from a dict (inverse of to_dict).

        VALIDATES THE FULL SNAPSHOT FIRST and only then replaces the current
        state: a malformed snapshot (bad shape, dangling edge, duplicate id,
        ...) raises ``ValueError`` and leaves the existing graph untouched
        (PR-11 / PROV-5). On a snapshot-backed graph the replacement is
        published to disk as one locked, atomic full-state write.

        Args:
            data: Dict with "nodes" and "edges" keys.

        Raises:
            ValueError: If *data* is not a valid full graph snapshot.
        """
        nodes, edges = self._build_state_from(data)
        self._swap_state(nodes, edges)
        self._journal = []
        self._full_publish_pending = self._snapshot_path is not None
        if self._snapshot_path is not None:
            # Full-state publish under the inter-process lock.
            self._persist_snapshot()

    def load_snapshot(self, path: str | Path | None = None) -> None:
        """Load graph state from a JSON snapshot file if it exists.

        Reads under the inter-process lock so a concurrent writer can never
        be observed mid-transaction.  A missing file, unparseable JSON, or a
        structurally invalid snapshot is logged and leaves the current
        in-memory state untouched (never partially loaded, PR-11 / PROV-5).
        """
        snapshot_path = Path(path) if path is not None else self._snapshot_path
        if snapshot_path is None:
            return
        if not snapshot_path.exists():
            return
        lock_path = _snapshot_lock_path(snapshot_path)
        try:
            with _file_lock(lock_path):
                try:
                    text = snapshot_path.read_text(encoding="utf-8")
                except OSError as exc:
                    logger.warning("Failed to read graph snapshot %s: %s", snapshot_path, exc)
                    return
                try:
                    data = json.loads(text)
                except Exception:
                    logger.exception("Failed to load graph snapshot from %s", snapshot_path)
                    return
        except TimeoutError:
            logger.exception("Timed out waiting for graph lock %s", lock_path)
            return
        except OSError as exc:
            # Lock file could not be created (e.g. read-only data dir) —
            # fall back to an unlocked read. Writes are atomic (unique tmp +
            # os.replace), so a reader can never observe torn JSON.
            logger.warning(
                "Could not lock %s (%s); reading snapshot without lock", lock_path, exc
            )
            try:
                text = snapshot_path.read_text(encoding="utf-8")
                data = json.loads(text)
            except Exception:
                logger.exception("Failed to load graph snapshot from %s", snapshot_path)
                return

        try:
            nodes, edges = self._build_state_from(data)
        except ValueError:
            logger.exception(
                "Refusing to load malformed graph snapshot %s — current graph unchanged",
                snapshot_path,
            )
            return
        self._swap_state(nodes, edges)
        self._journal = []
        self._full_publish_pending = False

    def save_snapshot(self, path: str | Path | None = None) -> None:
        """Write the graph state to a JSON snapshot file.

        Without *path* this flushes pending mutations through the locked
        rebase transaction (see module docstring).  With an explicit *path*
        the current in-memory state is exported to that file atomically.
        """
        snapshot_path = Path(path) if path is not None else self._snapshot_path
        if snapshot_path is None:
            return
        if path is None:
            self._persist_snapshot()
        else:
            self._export_snapshot(snapshot_path)

    def _persist_if_needed(self) -> None:
        if self._suspend_persistence:
            return
        self._persist_snapshot()

    def _persist_snapshot(self) -> None:
        """Flush this graph to its snapshot path (locked transaction).

        Modes:
        * ``_full_publish_pending`` (set by ``from_dict``): publish the
          in-memory state as-is under the lock (whole-state replacement).
        * pending journal: lock -> reload fresh on-disk state -> replay the
          journal onto it -> write unique tmp -> atomic replace -> refresh
          in-memory state to the merged result.
        * nothing pending: nothing to write; resync memory from disk so a
          bare ``save_snapshot()`` can never publish a stale generation.
        """
        snapshot_path = self._snapshot_path
        if snapshot_path is None:
            return

        if self._full_publish_pending:
            self._full_publish_pending = False
            try:
                with self._thread_lock:
                    with _file_lock(_snapshot_lock_path(snapshot_path)):
                        _cleanup_stale_tmp(snapshot_path)
                        self._write_snapshot(snapshot_path)
            except Exception:
                logger.exception("Failed to publish graph snapshot to %s", snapshot_path)
            return

        if not self._journal:
            self._resync_from_disk(snapshot_path)
            return

        try:
            with self._thread_lock:
                with _file_lock(_snapshot_lock_path(snapshot_path)):
                    if snapshot_path.exists():
                        try:
                            fresh = self._read_state_file(snapshot_path)
                        except _MalformedSnapshotError:
                            # Never clobber a corrupt file with a rebase that
                            # silently drops its content.
                            logger.exception(
                                "Refusing to overwrite malformed snapshot %s — "
                                "keeping file and pending mutations for retry",
                                snapshot_path,
                            )
                            return
                    else:
                        fresh = None

                    # Merge fresh disk state with this instance's journal.
                    merged = SimpleGraph()
                    if fresh is not None:
                        merged._nodes, merged._edges = fresh
                    for op in self._journal:
                        self._replay_op(merged, op)

                    # Refresh memory to the merged truth BEFORE writing so a
                    # failure still leaves memory == disk + pending journal.
                    self._nodes, self._edges = merged._nodes, merged._edges
                    _cleanup_stale_tmp(snapshot_path)
                    # Treat only an explicit False (failed write) as failure:
                    # subclasses that override _write_snapshot for counting may
                    # return None after a successful super() call.
                    if self._write_snapshot(snapshot_path) is not False:
                        self._journal = []
        except Exception:
            logger.exception("Failed to persist graph snapshot to %s", snapshot_path)

    def _resync_from_disk(self, snapshot_path: Path) -> None:
        """Refresh in-memory state from disk when nothing local is pending."""
        if not snapshot_path.exists():
            return
        try:
            with _file_lock(_snapshot_lock_path(snapshot_path)):
                try:
                    data = json.loads(snapshot_path.read_text(encoding="utf-8"))
                except Exception:
                    return
                nodes, edges = self._build_state_from(data)
            self._swap_state(nodes, edges)
            self._journal = []
        except Exception:
            logger.exception("Failed to resync graph state from %s", snapshot_path)

    def _export_snapshot(self, snapshot_path: Path) -> None:
        """Atomic export of the current in-memory state to *snapshot_path*."""
        try:
            with self._thread_lock:
                with _file_lock(_snapshot_lock_path(snapshot_path)):
                    _cleanup_stale_tmp(snapshot_path)
                    self._write_snapshot(snapshot_path)
        except Exception:
            logger.exception("Failed to export graph snapshot to %s", snapshot_path)

    def _read_state_file(self, snapshot_path: Path) -> tuple[dict, dict]:
        """Read and fully validate a snapshot file.

        Returns:
            (nodes, edges) built from the file.

        Raises:
            _MalformedSnapshotError: if the file is missing, unparseable, or not
                a valid full graph state.
        """
        try:
            text = snapshot_path.read_text(encoding="utf-8")
            data = json.loads(text)
            return self._build_state_from(data)
        except _MalformedSnapshotError:
            raise
        except Exception as exc:
            raise _MalformedSnapshotError(f"{snapshot_path}: {exc}") from exc

    @staticmethod
    def _build_state_from(data: Any) -> tuple[dict[str, GraphNode], dict[str, dict[str, list[GraphEdge]]]]:
        """Validate *data* and build fresh node/edge structures from it.

        This is the single validation path for ``from_dict``, snapshot
        loads, and journal rebase reads: the whole snapshot is checked
        BEFORE anything touches the live graph, so a malformed snapshot can
        never leave (or replace) a partially loaded state.

        Raises:
            ValueError: on any structural problem.
        """
        if not isinstance(data, dict):
            raise ValueError(f"graph snapshot must be a dict, got {type(data).__name__}")
        nodes_raw = data.get("nodes", {}) or {}
        if not isinstance(nodes_raw, dict):
            raise ValueError(f"'nodes' must be a dict, got {type(nodes_raw).__name__}")
        edges_raw = data.get("edges", []) or []
        if not isinstance(edges_raw, list):
            raise ValueError(f"'edges' must be a list, got {type(edges_raw).__name__}")

        nodes: dict[str, GraphNode] = {}
        for key, ndata in nodes_raw.items():
            if not isinstance(key, str):
                raise ValueError(f"node key {key!r}: expected string, got {type(key).__name__}")
            if not isinstance(ndata, dict):
                raise ValueError(f"node {key!r}: expected dict, got {type(ndata).__name__}")
            nid = ndata.get("id", key)
            if not isinstance(nid, str) or not nid:
                raise ValueError(f"node {key!r}: invalid id {nid!r}")
            if nid in nodes:
                raise ValueError(f"duplicate node id {nid!r}")
            ntype = ndata.get("type", "")
            name = ndata.get("name", "")
            if not isinstance(ntype, str) or not isinstance(name, str):
                raise ValueError(f"node {key!r}: type/name must be strings")
            attributes = ndata.get("attributes", {})
            if attributes is None:
                attributes = {}
            if not isinstance(attributes, dict):
                raise ValueError(f"node {key!r}: attributes must be a dict")
            nodes[nid] = GraphNode(id=nid, type=ntype, name=name, attributes=attributes)

        edges: dict[str, dict[str, list[GraphEdge]]] = {}
        for i, edata in enumerate(edges_raw):
            if not isinstance(edata, dict):
                raise ValueError(f"edge {i}: expected dict, got {type(edata).__name__}")
            source_id = edata.get("source_id")
            target_id = edata.get("target_id")
            if not isinstance(source_id, str) or not source_id:
                raise ValueError(f"edge {i}: source_id must be a non-empty string")
            if not isinstance(target_id, str) or not target_id:
                raise ValueError(f"edge {i}: target_id must be a non-empty string")
            if source_id not in nodes:
                raise ValueError(f"edge {i}: source node {source_id!r} not present in nodes")
            if target_id not in nodes:
                raise ValueError(f"edge {i}: target node {target_id!r} not present in nodes")
            relation = edata.get("relation", "")
            if not isinstance(relation, str):
                raise ValueError(f"edge {i}: relation must be a string")
            attributes = edata.get("attributes", {})
            if attributes is None:
                attributes = {}
            if not isinstance(attributes, dict):
                raise ValueError(f"edge {i}: attributes must be a dict")
            edges.setdefault(source_id, {}).setdefault(target_id, []).append(
                GraphEdge(
                    source_id=source_id,
                    target_id=target_id,
                    relation=relation,
                    attributes=attributes,
                )
            )
        return nodes, edges

    def _swap_state(self, nodes: dict[str, GraphNode], edges: dict[str, dict[str, list[GraphEdge]]]) -> None:
        """Replace the in-memory graph state wholesale (post-validation)."""
        self._nodes = nodes
        self._edges = edges

    def _replay_op(self, graph: "SimpleGraph", op: tuple[Any, ...]) -> None:
        """Re-apply one pending journal op onto *graph* (tolerant replay).

        Replay applies the mutation's INTENT onto the fresh on-disk state.
        Idempotent/tolerant on purpose: producers guard with
        ``get_node``/``get_edge`` against their own stale memory, so a
        concurrently-created node/edge already satisfying the op is kept,
        and an op whose endpoint vanished concurrently is dropped with a
        warning instead of failing the whole transaction.
        """
        kind = op[0]
        if kind == "add_node":
            _, nid, ntype, name, attributes = op
            if graph._nodes.get(nid) is None:
                graph._nodes[nid] = GraphNode(
                    id=nid, type=ntype, name=name, attributes=attributes or {}
                )
            # else: a concurrent writer already created this node — keep it.
        elif kind == "delete_node":
            _, nid = op
            if nid in graph._nodes:
                graph.delete_node(nid)
        elif kind == "add_edge":
            _, source_id, target_id, relation, attributes = op
            if source_id not in graph._nodes or target_id not in graph._nodes:
                logger.warning(
                    "Dropping pending edge %s->%s (%s): endpoint deleted concurrently",
                    source_id,
                    target_id,
                    relation,
                )
                return
            if graph.get_edge(source_id, target_id, relation=relation) is None:
                graph.add_edge(
                    source_id=source_id,
                    target_id=target_id,
                    relation=relation,
                    attributes=attributes or {},
                )
        elif kind == "delete_edge":
            _, source_id, target_id = op
            if source_id in graph._edges and target_id in graph._edges[source_id]:
                graph.delete_edge(source_id, target_id)

    def _write_snapshot(self, snapshot_path: Path) -> bool:
        """Atomically write the current state to *snapshot_path*.

        The caller MUST hold the inter-process lock for *snapshot_path*
        (transactions and exports acquire it; this method never does, so
        flock is not re-entered on a second fd of the same file).  Writes to
        a UNIQUE temp file and then atomically replaces the snapshot, so a
        crash can never leave a torn ``graph.json``.

        Returns:
            True on success, False on failure (failure is logged, never
            raised — persistence is best-effort for producers).
        """
        try:
            payload = json.dumps(self.to_dict(), ensure_ascii=False, indent=2)
            snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = snapshot_path.with_name(
                f".{snapshot_path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
            )
            try:
                tmp_path.write_text(payload, encoding="utf-8")
                os.replace(tmp_path, snapshot_path)
            finally:
                try:
                    tmp_path.unlink(missing_ok=True)
                except OSError:
                    pass
            return True
        except Exception:
            logger.exception("Failed to persist graph snapshot to %s", snapshot_path)
            return False
