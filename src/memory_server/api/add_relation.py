"""MCP tool helper: add_relation — create typed inter-claim relations."""

from __future__ import annotations

from typing import Any

from memory_server.providers.graph_provider import SimpleGraph
from memory_server.providers.sqlite_provider import SQLiteProvider

_ALLOWED_RELATION_TYPES = {"supports", "contradicts", "derives"}


async def add_relation(
    graph: SimpleGraph,
    source_id: str,
    target_id: str,
    relation_type: str,
    provider: SQLiteProvider | None = None,
) -> dict[str, Any]:
    """Create a typed relation edge between two existing graph nodes."""
    normalized = relation_type.strip().lower()
    if normalized not in _ALLOWED_RELATION_TYPES:
        raise ValueError(
            "relation_type must be one of: supports, contradicts, derives"
        )

    source_node = graph.get_node(source_id)
    if source_node is None:
        raise KeyError(f"Source node '{source_id}' not found")

    target_node = graph.get_node(target_id)
    if target_node is None:
        raise KeyError(f"Target node '{target_id}' not found")

    if provider is not None:
        await provider.create_in_transaction(
            relation_entries=[
                {
                    "source_id": source_id,
                    "target_id": target_id,
                    "relation_type": normalized,
                }
            ]
        )

    edge = None
    for candidate in graph.search_by_relation(normalized):
        if candidate.source_id == source_id and candidate.target_id == target_id:
            edge = candidate
            break
    if edge is None:
        edge = graph.add_edge(
            source_id=source_id,
            target_id=target_id,
            relation=normalized,
        )

    return {
        "edge": {
            "source_id": edge.source_id,
            "target_id": edge.target_id,
            "relation": edge.relation,
            "attributes": edge.attributes,
        },
        "source": {
            "id": source_node.id,
            "type": source_node.type,
            "name": source_node.name,
            "attributes": source_node.attributes,
        },
        "target": {
            "id": target_node.id,
            "type": target_node.type,
            "name": target_node.name,
            "attributes": target_node.attributes,
        },
    }
