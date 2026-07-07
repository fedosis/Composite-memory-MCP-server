"""Graph router — entity relation linker for the knowledge graph.

Extracts entity references from query text, queries the in-memory graph
for related entities and relations, and returns structured context.

Per ADR-005 routing order (stage 3): evaluated after rules and semantic search.
"""

from __future__ import annotations

import logging
from typing import Any

from memory_server.providers.graph_provider import SimpleGraph

logger = logging.getLogger(__name__)


class GraphRouter:
    """Routes queries through entity relation lookups in the knowledge graph.

    Args:
        graph: Optional SimpleGraph instance. Creates a new one if not provided.
    """

    def __init__(self, graph: SimpleGraph | None = None) -> None:
        self._graph = graph or SimpleGraph()

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
                        max_depth=4,
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
        existing = self._graph.get_edge(subj_id, obj_id)
        if existing is None:
            self._graph.add_edge(
                source_id=subj_id,
                target_id=obj_id,
                relation=predicate,
            )

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
