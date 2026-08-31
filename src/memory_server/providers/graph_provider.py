"""Pure-Python in-memory graph engine for entity relation storage.

No external dependencies — uses Python dicts + sets.
Designed for fast testing and simple persistence via JSON dump/load.
"""

from __future__ import annotations

import copy
import json
import logging
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Optional

logger = logging.getLogger(__name__)


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

    @contextmanager
    def suspend_persistence(self) -> Iterator[None]:
        """Temporarily suspend snapshot writes (batch mutation support).

        Transactional: on ANY exception inside the with-block the in-memory
        ``_nodes``/``_edges`` are restored to the state captured on entry and
        the exception is re-raised, so a failed batch never leaves partial
        mutations behind (Card 3b, SPEC scope 2). A deep copy is required —
        ``add_edge`` appends into nested lists, so a shallow dict copy would
        not restore them. Pair with ``save_snapshot()``.
        """
        snapshot_nodes = copy.deepcopy(self._nodes)
        snapshot_edges = copy.deepcopy(self._edges)
        self._suspend_persistence = True
        try:
            yield
        except Exception:
            self._nodes.clear()
            self._nodes.update(snapshot_nodes)
            self._edges.clear()
            self._edges.update(snapshot_edges)
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
        """Find all paths between two nodes up to max_depth.

        Uses simple BFS to find shortest paths.

        Args:
            source_id: Starting node id.
            target_id: Target node id.
            max_depth: Maximum path length (default 4).

        Returns:
            List of paths, where each path is a list of GraphNode objects.
        """
        if source_id not in self._nodes or target_id not in self._nodes:
            return []

        if source_id == target_id:
            return [[self._nodes[source_id]]]

        # BFS
        visited: set[str] = set()
        queue: list[tuple[str, list[str]]] = [(source_id, [source_id])]
        paths: list[list[GraphNode]] = []

        while queue:
            current, path = queue.pop(0)
            if len(path) - 1 >= max_depth:
                continue

            if current in self._edges:
                for target_id_candidate in self._edges[current]:
                    if target_id_candidate in visited and target_id_candidate != target_id:
                        continue
                    new_path = path + [target_id_candidate]
                    if target_id_candidate == target_id:
                        paths.append([self._nodes[nid] for nid in new_path])
                    elif len(new_path) - 1 < max_depth:
                        queue.append((target_id_candidate, new_path))
                        visited.add(target_id_candidate)

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

        Args:
            data: Dict with "nodes" and "edges" keys.
        """
        self._suspend_persistence = True
        try:
            self._nodes.clear()
            self._edges.clear()
            for nid, ndata in data.get("nodes", {}).items():
                self.add_node(
                    id=ndata["id"],
                    type=ndata.get("type", ""),
                    name=ndata.get("name", ""),
                    attributes=ndata.get("attributes", {}),
                )
            for edata in data.get("edges", []):
                self.add_edge(
                    source_id=edata["source_id"],
                    target_id=edata["target_id"],
                    relation=edata.get("relation", ""),
                    attributes=edata.get("attributes", {}),
                )
        finally:
            self._suspend_persistence = False
        self._persist_snapshot()

    def load_snapshot(self, path: str | Path | None = None) -> None:
        """Load graph state from a JSON snapshot file if it exists."""
        snapshot_path = Path(path) if path is not None else self._snapshot_path
        if snapshot_path is None:
            return
        if not snapshot_path.exists():
            return
        try:
            data = json.loads(snapshot_path.read_text(encoding="utf-8"))
        except Exception:
            logger.exception("Failed to load graph snapshot from %s", snapshot_path)
            return
        self.from_dict(data)

    def save_snapshot(self, path: str | Path | None = None) -> None:
        """Write the graph state to a JSON snapshot file."""
        snapshot_path = Path(path) if path is not None else self._snapshot_path
        if snapshot_path is None:
            return
        self._write_snapshot(snapshot_path)

    def _persist_if_needed(self) -> None:
        if self._suspend_persistence:
            return
        self._persist_snapshot()

    def _persist_snapshot(self) -> None:
        if self._snapshot_path is None:
            return
        self._write_snapshot(self._snapshot_path)

    def _write_snapshot(self, snapshot_path: Path) -> None:
        try:
            snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            payload = json.dumps(self.to_dict(), ensure_ascii=False, indent=2)
            tmp_path = snapshot_path.with_suffix(snapshot_path.suffix + ".tmp")
            tmp_path.write_text(payload, encoding="utf-8")
            tmp_path.replace(snapshot_path)
        except Exception:
            logger.exception("Failed to persist graph snapshot to %s", snapshot_path)
