"""Transport-free graph retrieval application service."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Literal

from memory_server.application.contracts import GraphPolicy, GraphSearchCommand
from memory_server.application.errors import InvalidCommandError, UnknownDependencyError
from memory_server.router.embedding_router import NativeSubmitter


@dataclass(frozen=True, slots=True)
class GraphBudget:
    max_nodes: int = 500
    max_edges: int = 1000
    max_pairs: int = 100
    max_depth: int = 4


@dataclass(frozen=True, slots=True)
class GraphOutcome:
    state: Literal["ok", "failed"]
    relations: tuple[dict[str, Any], ...]
    code: Literal["E_GRAPH_BACKEND"] | None


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

    async def query_bounded(
        self,
        command: GraphSearchCommand,
        *,
        budget: GraphBudget | None = None,
        native: "NativeSubmitter",
    ) -> GraphOutcome:
        if any(
            getattr(command, field)
            for field in ("entity_id", "source_id", "target_id", "relation_type")
        ):
            raise InvalidCommandError(
                "bounded graph search accepts query only"
            )
        if self._get_graph_router is None:
            raise UnknownDependencyError("graph router")
        try:
            router = await self._get_graph_router()
        except ValueError:
            raise UnknownDependencyError("graph router") from None
        if router is None:
            raise UnknownDependencyError("graph router")
        limits = budget if budget is not None else GraphBudget()

        def run() -> GraphOutcome:
            try:
                result = router.query_bounded(
                    command.query,
                    max_nodes=limits.max_nodes,
                    max_edges=limits.max_edges,
                    max_pairs=limits.max_pairs,
                    max_depth=limits.max_depth,
                )
                relations: list[dict[str, Any]] = []
                graph = router.graph
                for relation in result.get("relations", ()):
                    if not isinstance(relation, dict):
                        continue
                    source_id = relation.get("source_id")
                    target_id = relation.get("target_id")
                    edge_relation = relation.get("relation")
                    if not all(
                        isinstance(value, str) and value
                        for value in (source_id, target_id, edge_relation)
                    ):
                        continue
                    source = graph.get_node(source_id)
                    target = graph.get_node(target_id)
                    if (
                        source is None
                        or target is None
                        or source.type != "entity"
                        or target.type != "entity"
                        or not isinstance(source.name, str)
                        or not source.name
                        or not isinstance(target.name, str)
                        or not target.name
                    ):
                        continue
                    relations.append({
                        "source_id": source.id,
                        "target_id": target.id,
                        "source_name": source.name,
                        "target_name": target.name,
                        "relation": edge_relation,
                    })
                return GraphOutcome("ok", tuple(relations), None)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                code = getattr(exc, "code", None)
                if isinstance(code, str) and code.startswith("E_RUNTIME_"):
                    raise
                return GraphOutcome("failed", (), "E_GRAPH_BACKEND")

        handle = await native(run, kind="retrieval-graph")
        return await handle.result()
