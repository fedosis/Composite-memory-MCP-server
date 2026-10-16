"""MCP tool helper: add_relation — create typed inter-claim relations."""

from __future__ import annotations

from typing import Any

from memory_server.application.contracts import RelationCommand
from memory_server.application.relation_service import RelationService
from memory_server.providers.graph_provider import SimpleGraph
from memory_server.providers.sqlite_provider import SQLiteProvider


async def add_relation(
    graph: SimpleGraph,
    source_id: str,
    target_id: str,
    relation_type: str,
    provider: SQLiteProvider | None = None,
) -> dict[str, Any]:
    """Create a typed relation edge between two existing graph nodes."""
    async def get_graph() -> SimpleGraph:
        return graph

    return await RelationService(get_graph=get_graph, provider=provider).add_relation(
        RelationCommand(source_id, target_id, relation_type)
    )
