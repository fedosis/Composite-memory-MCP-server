"""Transport-free graph retrieval application service."""

from __future__ import annotations

from typing import Any, Awaitable, Callable

from memory_server.application.contracts import GraphPolicy, GraphSearchCommand
from memory_server.application.errors import UnknownDependencyError


class GraphService:
    """Graph search orchestration over a borrowed graph router."""

    def __init__(
        self,
        get_graph_router: Callable[[], Awaitable[Any]] | None = None,
        *,
        policy: GraphPolicy | None = None,
    ) -> None:
        self._get_graph_router = get_graph_router
        self._policy = policy if policy is not None else GraphPolicy.capture()

    @property
    def policy(self) -> GraphPolicy:
        return self._policy

    async def search(self, command: GraphSearchCommand) -> dict:
        if self._get_graph_router is None:
            raise UnknownDependencyError("graph router")
        router = await self._get_graph_router()
        graph = router.graph
        nodes: list[dict] = []
        edges: list[dict] = []
        paths: list[list[dict]] = []

        if command.relation_type:
            normalized_relation = command.relation_type.strip().lower()
            matched_edges = graph.search_by_relation(normalized_relation)
            seen_nodes: dict[str, dict] = {}
            for edge in matched_edges:
                source_node = graph.get_node(edge.source_id)
                target_node = graph.get_node(edge.target_id)
                if source_node is not None:
                    seen_nodes[source_node.id] = {
                        "id": source_node.id,
                        "name": source_node.name,
                        "type": source_node.type,
                        "attributes": source_node.attributes,
                    }
                if target_node is not None:
                    seen_nodes[target_node.id] = {
                        "id": target_node.id,
                        "name": target_node.name,
                        "type": target_node.type,
                        "attributes": target_node.attributes,
                    }
                edges.append({
                    "source_id": edge.source_id,
                    "source_name": source_node.name if source_node is not None else edge.source_id,
                    "target_id": edge.target_id,
                    "target_name": target_node.name if target_node is not None else edge.target_id,
                    "relation": edge.relation,
                    "attributes": edge.attributes,
                })
            nodes = list(seen_nodes.values())
        elif command.entity_id:
            node = graph.get_node(command.entity_id)
            if node is not None:
                nodes.append({
                    "id": node.id,
                    "name": node.name,
                    "type": node.type,
                    "attributes": node.attributes,
                })
                neighbors = graph.get_neighbors(command.entity_id)
                for neighbor_node, edge in neighbors:
                    nodes.append({
                        "id": neighbor_node.id,
                        "name": neighbor_node.name,
                        "type": neighbor_node.type,
                        "attributes": neighbor_node.attributes,
                    })
                    edges.append({
                        "source_id": edge.source_id,
                        "target_id": edge.target_id,
                        "relation": edge.relation,
                        "attributes": edge.attributes,
                    })
        elif command.source_id and command.target_id:
            found_paths = graph.find_path(
                command.source_id,
                command.target_id,
                max_depth=self._policy.graph_max_path_depth,
            )
            for path in found_paths:
                paths.append([
                    {"id": node.id, "name": node.name, "type": node.type}
                    for node in path
                ])
        elif command.query:
            result = router.query(command.query)
            nodes = result.get("entities", [])
            edges = result.get("relations", [])
            paths = result.get("paths", [])

        return {"nodes": nodes, "edges": edges, "paths": paths}
