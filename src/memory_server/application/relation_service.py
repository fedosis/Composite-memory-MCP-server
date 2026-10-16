"""Application relation use case over borrowed graph and SQLite handles."""

from __future__ import annotations

from typing import Any, Awaitable, Callable

from memory_server.application.contracts import RelationCommand
from memory_server.application.errors import UnknownDependencyError

_ALLOWED_RELATION_TYPES = {"supports", "contradicts", "derives"}


class RelationService:
    """Create a relation while preserving the legacy SQL/graph ordering."""

    def __init__(
        self,
        *,
        get_graph: Callable[[], Awaitable[Any]],
        provider: Any | None = None,
    ) -> None:
        if get_graph is None:
            raise UnknownDependencyError("relation graph")
        self._get_graph = get_graph
        self._provider = provider

    async def add_relation(self, command: RelationCommand) -> dict[str, Any]:
        graph = await self._get_graph()
        normalized = command.relation_type.strip().lower()
        if normalized not in _ALLOWED_RELATION_TYPES:
            raise ValueError(
                "relation_type must be one of: supports, contradicts, derives"
            )

        source_node = graph.get_node(command.source_id)
        if source_node is None:
            raise KeyError(f"Source node '{command.source_id}' not found")

        target_node = graph.get_node(command.target_id)
        if target_node is None:
            raise KeyError(f"Target node '{command.target_id}' not found")

        if self._provider is not None:
            await self._provider.create_in_transaction(
                relation_entries=[
                    {
                        "source_id": command.source_id,
                        "target_id": command.target_id,
                        "relation_type": normalized,
                    }
                ]
            )

        edge = None
        for candidate in graph.search_by_relation(normalized):
            if (
                candidate.source_id == command.source_id
                and candidate.target_id == command.target_id
            ):
                edge = candidate
                break
        if edge is None:
            edge = graph.add_edge(
                source_id=command.source_id,
                target_id=command.target_id,
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
