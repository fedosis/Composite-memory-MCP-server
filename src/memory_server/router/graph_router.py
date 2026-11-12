"""Graph router — entity relation linker for the knowledge graph.

Extracts entity references from query text, queries the in-memory graph
for related entities and relations, and returns structured context.

Per ADR-005 routing order (stage 3): evaluated after rules and semantic search.
"""

from __future__ import annotations

import logging
from typing import Any

from memory_server.providers import graph_provider as _graph_provider
from memory_server.providers.graph_provider import GraphBudgetExceeded, SimpleGraph

logger = logging.getLogger(__name__)


def _valid_triple(t) -> bool:
    """Return True if *t* is a valid fact triple dict.

    A valid triple is a dict with all three of ``subject``, ``predicate``,
    ``object`` present as non-empty strings. ``str.strip()`` rejects both
    ASCII and Unicode whitespace (``"   "``, ``"\\t"``, ``"\\u00a0"``,
    ``"\\u2003"``). Extra keys are allowed and ignored.
    """
    return isinstance(t, dict) and all(
        isinstance(v, str) and v.strip()
        for v in (t.get("subject"), t.get("predicate"), t.get("object"))
    )


class GraphRouter:
    """Routes queries through entity relation lookups in the knowledge graph.

    Args:
        graph: Optional SimpleGraph instance. Creates a new one if not provided.
        max_path_depth: Maximum pathfinding depth used by ``query()``
            (default 4, Settings-driven at production call sites).
    """

    def __init__(
        self,
        graph: SimpleGraph,
        max_path_depth: int = 4,
    ) -> None:
        if graph is None:
            raise ValueError("E_CAPABILITY_UNAVAILABLE")
        self._graph = graph
        self._max_path_depth = max_path_depth

    # --- Entity extraction ---

    def _extract_entities(self, text: str) -> list[dict[str, Any]]:
        """Extract potential entity references from query text.

        Matches against known entity names (case-insensitive) in the graph.

        Args:
            text: Query text.

        Returns:
            List of matched entity dicts with id, name, type, attributes.
        """
        if not text or not text.strip():
            return []
        text_lower = text.lower().strip()

        matched: list[dict[str, Any]] = []
        for node in self._graph.get_all_nodes():
            if node.name.lower() in text_lower or node.id.lower() in text_lower:
                matched.append({
                    "id": node.id,
                    "name": node.name,
                    "type": node.type,
                    "attributes": node.attributes,
                })
        return matched

    # --- Query ---

    def query(self, text: str) -> dict[str, Any]:
        """Query the graph for entity relations.

        Steps:
        1. Extract entity references from query text.
        2. For each matched entity, find related entities and relations.
        3. If multiple entities matched, attempt pathfinding.

        Args:
            text: Query text.

        Returns:
            Dict with keys:
                - entities: list of matched entity dicts
                - relations: list of relation dicts
                - paths: list of node-path lists (if multiple entities)
        """
        if not text or not text.strip():
            return {"entities": [], "relations": [], "paths": []}

        entities = self._extract_entities(text)
        if not entities:
            return {"entities": [], "relations": [], "paths": []}

        # Get relations for each matched entity
        relations: list[dict[str, Any]] = []
        seen_rel: set[str] = set()
        for entity in entities:
            neighbors = self._graph.get_neighbors(entity["id"])
            for neighbor_node, edge in neighbors:
                rel_key = f"{edge.source_id}:{edge.target_id}:{edge.relation}"
                if rel_key not in seen_rel:
                    seen_rel.add(rel_key)
                    relations.append({
                        "source_id": edge.source_id,
                        "source_name": entity["name"],
                        "relation": edge.relation,
                        "target_id": neighbor_node.id,
                        "target_name": neighbor_node.name,
                        "target_type": neighbor_node.type,
                    })

        # Pathfinding when multiple entities are mentioned
        paths: list[list[dict[str, Any]]] = []
        if len(entities) >= 2:
            for i in range(len(entities)):
                for j in range(i + 1, len(entities)):
                    found_paths = self._graph.find_path(
                        entities[i]["id"],
                        entities[j]["id"],
                        max_depth=self._max_path_depth,
                    )
                    for p in found_paths:
                        paths.append([
                            {"id": n.id, "name": n.name, "type": n.type}
                            for n in p
                        ])

        return {
            "entities": entities,
            "relations": relations,
            "paths": paths,
        }

    def query_bounded(
        self,
        text: str,
        *,
        max_nodes: int,
        max_edges: int,
        max_pairs: int,
        max_depth: int,
    ) -> dict[str, Any]:
        """Run the opt-in cumulative-budget graph traversal."""
        limits = (
            ("nodes", max_nodes, 500),
            ("edges", max_edges, 1000),
            ("pairs", max_pairs, 100),
            ("depth", max_depth, 4),
        )
        for dimension, value, hard_cap in limits:
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"invalid graph {dimension} budget")
            if value > hard_cap:
                raise GraphBudgetExceeded(dimension, hard_cap, value)

        with _graph_provider._bounded_traversal(
            max_nodes, max_edges, max_pairs, max_depth
        ):
            entities = self._extract_entities_bounded(text)
            relations: list[dict[str, Any]] = []
            seen_rel: set[str] = set()
            for entity in entities:
                neighbors = self._graph.get_neighbors(
                    entity["id"],
                    edge_limit=_graph_provider._remaining("edges"),
                )
                for neighbor_node, edge in neighbors:
                    if not all(type(getattr(edge, field, None)) is str and getattr(edge, field)
                               for field in ("source_id", "target_id", "relation")):
                        continue
                    rel_key = (edge.source_id, edge.target_id, edge.relation)
                    if rel_key in seen_rel:
                        continue
                    seen_rel.add(rel_key)
                    source_node = self._graph.get_node(edge.source_id)
                    target_node = self._graph.get_node(edge.target_id)
                    relations.append({
                        "source_id": edge.source_id,
                        "source_name": source_node.name if source_node else edge.source_id,
                        "relation": edge.relation,
                        "target_id": edge.target_id,
                        "target_name": target_node.name if target_node else edge.target_id,
                        "target_type": target_node.type if target_node else "",
                    })

            paths: list[list[dict[str, Any]]] = []
            for i in range(len(entities)):
                for j in range(i + 1, len(entities)):
                    _graph_provider._charge_pair()
                    found_paths = self._graph.find_path(
                        entities[i]["id"],
                        entities[j]["id"],
                        max_depth=max_depth,
                        node_limit=_graph_provider._remaining("nodes"),
                    )
                    for path in found_paths:
                        paths.append([
                            {"id": node.id, "name": node.name, "type": node.type}
                            for node in path
                        ])

            return {"entities": entities, "relations": relations, "paths": paths}

    def _extract_entities_bounded(self, text: str) -> list[dict[str, Any]]:
        text_lower = text.lower().strip()
        matched: list[dict[str, Any]] = []
        for node in self._graph.get_all_nodes(
            scan_limit=_graph_provider._remaining("nodes")
        ):
            if (type(getattr(node, "name", None)) is not str or not node.name
                    or type(getattr(node, "id", None)) is not str or not node.id):
                continue
            if node.name.lower() in text_lower or node.id.lower() in text_lower:
                matched.append({
                    "id": node.id,
                    "name": node.name,
                    "type": node.type,
                    "attributes": node.attributes,
                })
        return matched

    # --- Fact sync ---

    def sync_fact(
        self,
        subject: str,
        predicate: str,
        object: str,
    ) -> None:
        """Sync an extracted fact into the graph.

        Creates subject node, object node, and an edge between them.
        Reuses existing nodes if they already exist.

        Args:
            subject: Subject entity name.
            predicate: Relation/predicate.
            object: Object entity name.
        """
        subj_id = self._to_node_id(subject)
        obj_id = self._to_node_id(object)

        # Add or skip existing nodes
        if self._graph.get_node(subj_id) is None:
            self._graph.add_node(
                id=subj_id,
                type="entity",
                name=subject,
            )
        if self._graph.get_node(obj_id) is None:
            self._graph.add_node(
                id=obj_id,
                type="entity",
                name=object,
            )

        # Add edge if it doesn't exist
        existing = self._graph.get_edge(subj_id, obj_id, relation=predicate)
        if existing is None:
            self._graph.add_edge(
                source_id=subj_id,
                target_id=obj_id,
                relation=predicate,
            )

    def sync_facts_batch(self, triples: list[dict[str, Any]]) -> None:
        """Sync a batch of facts into the graph with a single snapshot write.

        Each triple is a dict with ``subject``, ``predicate``, ``object`` keys.
        Creates any missing nodes exactly once, then adds edges deduplicated
        by ``(source_id, target_id, relation)`` so two facts sharing the same
        subject/object but different predicates both keep their edges (S7).

        All mutations run with persistence suspended and the graph snapshot is
        written exactly once inside the transaction scope, replacing the ~6
        full snapshot writes per fact of the per-entry ``sync_fact`` path.
        The single write stays inside the with-block on purpose (Card 3b): if
        ``save_snapshot`` raises, ``suspend_persistence`` rolls the in-memory
        batch back so the graph is never left half-mutated with a stale
        snapshot. Real disk-write errors are still swallowed by
        ``SimpleGraph._write_snapshot`` (pre-existing); the rollback path is
        exercised when ``save_snapshot`` itself raises.

        Args:
            triples: List of fact dicts with subject/predicate/object keys.
        """
        # Input validation — atomic by construction: every triple is checked
        # BEFORE any graph mutation (no nodes, edges, or snapshot writes), so
        # an invalid batch is rejected as a whole with no partial writes
        # (Card 2, AC2). None / non-list containers raise ValueError.
        if not isinstance(triples, list):
            raise ValueError(
                "sync_facts_batch expects a list of triples, "
                f"got {type(triples).__name__}"
            )
        if not triples:
            return

        for triple in triples:
            if not _valid_triple(triple):
                logger.warning("sync_facts_batch rejected invalid triple")
                raise ValueError(f"invalid triple in sync_facts_batch: {triple!r}")

        # Collect unique node ids/names up front.
        pending_nodes: dict[str, str] = {}
        for triple in triples:
            subj = triple.get("subject", "")
            obj = triple.get("object", "")
            pending_nodes[self._to_node_id(subj)] = subj
            pending_nodes[self._to_node_id(obj)] = obj

        with self._graph.suspend_persistence():
            # One pass to determine which nodes already exist, then add the rest.
            for nid, name in pending_nodes.items():
                if self._graph.get_node(nid) is None:
                    self._graph.add_node(id=nid, type="entity", name=name)

            # Add edges, deduped by (source_id, target_id, relation).
            seen: set[tuple[str, str, str]] = set()
            for triple in triples:
                source_id = self._to_node_id(triple.get("subject", ""))
                target_id = self._to_node_id(triple.get("object", ""))
                relation = triple.get("predicate", "")
                key = (source_id, target_id, relation)
                if key in seen:
                    continue
                seen.add(key)
                if (
                    self._graph.get_edge(source_id, target_id, relation=relation)
                    is None
                ):
                    self._graph.add_edge(
                        source_id=source_id,
                        target_id=target_id,
                        relation=relation,
                    )

            # Single disk write for the whole batch. Inside the with-block on
            # purpose: if save_snapshot raises, suspend_persistence rolls the
            # in-memory batch back (Card 3b). Real disk-write errors are still
            # swallowed by SimpleGraph._write_snapshot (pre-existing); the
            # rollback path is exercised when save_snapshot itself raises.
            self._graph.save_snapshot()

    def sync_decision(
        self,
        choice: str,
        reason: str,
        entities: list[str],
    ) -> None:
        """Sync an extracted decision into the graph.

        Creates a decision node and links it to mentioned entities.

        Args:
            choice: Decision choice text.
            reason: Decision reason.
            entities: List of entity names mentioned in the decision.
        """
        decision_id = self._to_node_id(f"decision-{choice}")
        if self._graph.get_node(decision_id) is None:
            self._graph.add_node(
                id=decision_id,
                type="decision",
                name=choice,
                attributes={"reason": reason},
            )

        # Link to mentioned entities
        for entity_name in entities:
            entity_id = self._to_node_id(entity_name)
            if self._graph.get_node(entity_id) is not None:
                existing = self._graph.get_edge(decision_id, entity_id)
                if existing is None:
                    self._graph.add_edge(
                        source_id=decision_id,
                        target_id=entity_id,
                        relation="decides",
                    )

    def sync_skill(
        self,
        purpose: str,
        steps: list[str],
    ) -> None:
        """Sync an extracted skill into the graph.

        Creates a skill node with purpose and steps attributes.

        Args:
            purpose: Skill purpose description.
            steps: List of steps for the skill.
        """
        skill_id = self._to_node_id(f"skill-{purpose}")
        if self._graph.get_node(skill_id) is None:
            self._graph.add_node(
                id=skill_id,
                type="skill",
                name=purpose,
                attributes={"steps": steps},
            )

    # --- Graph access ---

    @property
    def graph(self) -> SimpleGraph:
        """Get the underlying graph instance."""
        return self._graph

    # --- Helpers ---

    @staticmethod
    def _to_node_id(name: str) -> str:
        """Convert a name to a consistent node ID.

        Lowercases, replaces spaces with hyphens, strips non-alphanumeric.

        Args:
            name: Entity name or text.

        Returns:
            Normalized node id.
        """
        return name.lower().replace(" ", "-")
