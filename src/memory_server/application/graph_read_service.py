"""Scoped canonical relation reads. Graph traversal is deliberately not mounted."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from collections import deque
from dataclasses import dataclass
from time import monotonic
from typing import TYPE_CHECKING, Callable, Literal
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from storage.repositories.relation_repo import (
    RelationKey,
    RelationReadWork,
    RelationRepository,
    _relation_wire_ok,
)

from memory_server.application.gateway_retrieval import NativeCustody
from memory_server.application.memory_read_service import ReadResources, b64, project_for_caller, unb64, utc
from memory_server.control.policy import require_permission
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import (
    GraphReadPort,
    MetricsSink,
    ProfileLeases,
    ProfileSession,
    RequestContext,
)
from memory_server.providers.graph_provider import (
    GraphBudgetExceeded,
    GraphReadDeadlineExceeded,
    ReadWindow,
    SimpleGraph,
)
from memory_server.transports.http.errors import HttpError

if TYPE_CHECKING:
    from memory_server.transports.http.routes.graph import RelationCursorCodec

RelationType = Literal["supports", "contradicts", "derives", "derived_from"]
_TYPES = ("supports", "contradicts", "derives", "derived_from")
MEMORY_HANDLE_MAX_BYTES = 1024
REL_ID_MAX_BYTES = 2048
REL_TUPLE_MAX_BYTES = 1024
ENTITY_HANDLE_MAX_BYTES = 128
ENTITY_TUPLE_MAX_BYTES = 80
PROJECTION_ID_MAX_BYTES = 2048
PROJECTION_TUPLE_MAX_BYTES = 1024
GRAPH_MAX_NODES = 500
GRAPH_MAX_EDGES = 1000
GRAPH_MAX_PAIRS = 100
WITNESS_CANDIDATES_MAX = 100


def wire_json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def validate_memory_id(value):
    try:
        if not isinstance(value, str) or not value or len(value) > 1024 or len(value.encode("utf-8")) > 1024:
            raise ValueError
        if value.startswith(("rel_", "ent_", "proj_")):
            raise ValueError
    except (ValueError, UnicodeError):
        raise HttpError("E_VALIDATION") from None
    return value


def relation_id(source_id: str, target_id: str, relation_type: RelationType) -> str:
    if relation_type not in _TYPES or not _relation_wire_ok(source_id, target_id, relation_type):
        raise HttpError("E_VALIDATION")
    return "rel_" + b64(wire_json([source_id, target_id, relation_type]))


def decode_relation_id(value: str) -> tuple[str, str, RelationType]:
    try:
        if not isinstance(value, str) or len(value) > REL_ID_MAX_BYTES or not value.startswith("rel_"):
            raise ValueError
        raw = unb64(value[4:])
        if len(raw) > REL_TUPLE_MAX_BYTES:
            raise ValueError
        data = json.loads(raw.decode("utf-8"))
        if type(data) is not list or len(data) != 3 or any(type(v) is not str for v in data):
            raise ValueError
        if relation_id(*data) != value:
            raise ValueError
        return tuple(data)
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise HttpError("E_VALIDATION") from None


def entity_id(fact_id: str, slot: Literal["subject", "object"]) -> str:
    try:
        if type(fact_id) is not str or str(UUID(fact_id)) != fact_id or slot not in {"subject", "object"}:
            raise ValueError
        value = "ent_" + b64(wire_json([fact_id, slot]))
        if len(value.encode("utf-8")) > ENTITY_HANDLE_MAX_BYTES:
            raise ValueError
        return value
    except (ValueError, TypeError, UnicodeError):
        raise HttpError("E_VALIDATION") from None


def decode_entity_id(value: str) -> tuple[str, Literal["subject", "object"]]:
    try:
        if (
            type(value) is not str
            or len(value.encode("utf-8")) > ENTITY_HANDLE_MAX_BYTES
            or not value.startswith("ent_")
        ):
            raise ValueError
        raw = unb64(value[4:])
        if len(raw) > ENTITY_TUPLE_MAX_BYTES:
            raise ValueError
        data = json.loads(raw.decode("utf-8"))
        if type(data) is not list or len(data) != 2 or any(type(item) is not str for item in data):
            raise ValueError
        if entity_id(data[0], data[1]) != value:
            raise ValueError
        return data[0], data[1]
    except (ValueError, TypeError, UnicodeError, RecursionError, HttpError):
        raise HttpError("E_VALIDATION") from None


def projection_id(source_id: str, target_id: str, relation_type: Literal["[MASKED]"] = "[MASKED]") -> str:
    try:
        if (
            type(source_id) is not str
            or type(target_id) is not str
            or not source_id
            or not target_id
            or relation_type != "[MASKED]"
        ):
            raise ValueError
        raw = wire_json([source_id, target_id, relation_type])
        if len(raw) > PROJECTION_TUPLE_MAX_BYTES:
            raise ValueError
        value = "proj_" + b64(raw)
        if len(value.encode("utf-8")) > PROJECTION_ID_MAX_BYTES:
            raise ValueError
        return value
    except (ValueError, TypeError, UnicodeError):
        raise HttpError("E_VALIDATION") from None


def decode_projection_id(value: str) -> tuple[str, str, Literal["[MASKED]"]]:
    try:
        if (
            type(value) is not str
            or len(value.encode("utf-8")) > PROJECTION_ID_MAX_BYTES
            or not value.startswith("proj_")
        ):
            raise ValueError
        raw = unb64(value[5:])
        if len(raw) > PROJECTION_TUPLE_MAX_BYTES:
            raise ValueError
        data = json.loads(raw.decode("utf-8"))
        if type(data) is not list or len(data) != 3 or any(type(item) is not str for item in data):
            raise ValueError
        if projection_id(*data) != value:
            raise ValueError
        return data[0], data[1], data[2]
    except (ValueError, TypeError, UnicodeError, RecursionError, HttpError):
        raise HttpError("E_VALIDATION") from None


def map_relation(row) -> d.Relation:
    return d.Relation(
        id=relation_id(row.source_id, row.target_id, row.relation_type),
        source_id=row.source_id,
        target_id=row.target_id,
        relation_type=row.relation_type,
        canonical=True,
        revision=None,
        created_at=utc(row.created_at),
    )


def map_memory_node(ctx: RequestContext, operation: str, record) -> d.GraphNode:
    projected = project_for_caller(ctx, operation, record, classification="unknown")
    return d.GraphNode(
        id=projected.id,
        kind=projected.memory_type,
        label="[MASKED]",
        lifecycle_state=projected.lifecycle_state,
        masked=True,
    )


def map_entity_node(representative: str) -> d.GraphNode:
    decode_entity_id(representative)
    return d.GraphNode(
        id=representative,
        kind="entity",
        label="[MASKED]",
        lifecycle_state=None,
        masked=True,
    )


def map_canonical_edge(row) -> d.GraphEdge:
    ident = relation_id(row.source_id, row.target_id, row.relation_type)
    return d.GraphEdge(
        id=ident,
        source_id=row.source_id,
        target_id=row.target_id,
        relation_type=row.relation_type,
        canonical=True,
        canonical_relation_id=ident,
        revision=None,
    )


def map_projection_edge(source_id: str, target_id: str) -> d.GraphEdge:
    return d.GraphEdge(
        id=projection_id(source_id, target_id),
        source_id=source_id,
        target_id=target_id,
        relation_type="[MASKED]",
        canonical=False,
        canonical_relation_id=None,
        revision=None,
    )


def map_consistency(projection: Literal["unavailable", "unknown"]) -> d.GraphConsistency:
    if projection not in {"unavailable", "unknown"}:
        raise HttpError("E_INTERNAL")
    return d.GraphConsistency(
        canonical_store="sqlite",
        projection=projection,
        outbox_lag=None,
        canonical_edges=None,
        projected_edges=None,
        missing_projection=None,
        extra_projection=None,
        last_sync=None,
    )


def map_graph_result(
    ctx: RequestContext,
    nodes,
    edges,
    paths,
    *,
    consistency: d.GraphConsistency,
    stop: Literal["timeout", "nodes", "edges", "depth"] | None,
) -> d.GraphResult:
    node_ids = [node.id for node in nodes]
    edge_ids = [edge.id for edge in edges]
    if (
        len(node_ids) != len(set(node_ids))
        or len(edge_ids) != len(set(edge_ids))
        or any(
            edge.source_id not in node_ids or edge.target_id not in node_ids or edge.revision is not None
            for edge in edges
        )
        or any(any(vertex not in node_ids for vertex in path) for path in paths)
        or consistency.projection not in {"unavailable", "unknown"}
    ):
        raise HttpError("E_INTERNAL")
    return d.GraphResult(
        nodes=list(nodes),
        edges=list(edges),
        paths=[list(path) for path in paths],
        graph_consistency=consistency,
        truncated=stop is not None,
        truncation_reason=stop,
        request_id=ctx.request_id,
    )


@dataclass
class _ReadLedger:
    nodes: int = 0
    edges: int = 0
    pairs: int = 0
    candidates: int = 0

    _LIMITS = {"nodes": GRAPH_MAX_NODES, "edges": GRAPH_MAX_EDGES, "pairs": GRAPH_MAX_PAIRS,
               "candidates": WITNESS_CANDIDATES_MAX}

    def charge(self, dimension: Literal["nodes", "edges", "pairs", "candidates"], amount: int = 1) -> None:
        if dimension not in self._LIMITS or type(amount) is not int or amount < 1:
            raise ValueError("invalid graph charge")
        attempted = getattr(self, dimension) + amount
        if attempted > self._LIMITS[dimension]:
            raise GraphBudgetExceeded(dimension, self._LIMITS[dimension], attempted)
        setattr(self, dimension, attempted)

    def remaining(self, dimension: Literal["nodes", "edges", "pairs", "candidates"]) -> int:
        return self._LIMITS[dimension] - getattr(self, dimension)

    def absorb(self, window: ReadWindow) -> None:
        self.charge("nodes", window.work.nodes) if window.work.nodes else None
        self.charge("edges", window.work.edges) if window.work.edges else None
        self.charge("pairs", window.work.pairs) if window.work.pairs else None


@dataclass(frozen=True)
class GraphReadResources:
    canonical: ReadResources
    graph: SimpleGraph | None


ReadBinder = Callable[[ProfileSession], GraphReadResources]
NativeBinder = Callable[[ProfileSession], NativeCustody]


class GraphReadService:
    def __init__(
        self,
        leases: ProfileLeases,
        bind: ReadBinder,
        *,
        native: NativeBinder,
        cursor_codec: RelationCursorCodec,
        metrics: MetricsSink,
        clock: Callable[[], float] = monotonic,
    ):
        self.leases, self.bind, self.native = leases, bind, native
        self.cursor_codec, self.metrics, self.clock = cursor_codec, metrics, clock

    async def execute(self, ctx: RequestContext, command: d.GraphReadCommand) -> d.GraphReadResult:
        operations = {
            "relations.for_memory",
            "relations.list",
            "relations.get",
            "graph.neighborhood",
            "graph.path",
            "graph.entity",
        }
        if command.operation not in operations:
            raise HttpError("E_VALIDATION")
        if ctx.profile_id != command.profile_id:
            raise HttpError("E_PERMISSION_DENIED")
        permission = "relation.read" if command.operation.startswith("relations.") else "graph.read"
        require_permission(ctx.principal, permission, profile_id=command.profile_id)
        if command.operation.startswith("graph."):
            return await self._execute_graph(ctx, command)
        state = None
        if command.operation == "relations.get":
            ident = decode_relation_id(command.path_ids["id"])
        else:
            ident = command.path_ids.get("id")
            if ident is not None:
                validate_memory_id(ident)
            if command.payload.cursor is not None:
                state = self.cursor_codec.validate(ctx.principal, command, now=self.cursor_codec.clock())
        remaining = ctx.deadline - self.clock()
        if remaining <= 0:
            raise HttpError("E_TIMEOUT")
        work = RelationReadWork(soft_deadline=ctx.deadline - min(0.050, remaining / 10), hard_deadline=ctx.deadline)
        async with self.leases.acquire(ctx) as scope:
            resources = self.bind(scope).canonical

            async def produce():
                async with resources.session_factory() as session:
                    async with session.begin():
                        repo = RelationRepository(session)
                        async with repo._read_window(work):
                            await session.execute(text("BEGIN"))
                            if command.operation == "relations.get":
                                row = await repo.get_by_tuple(*ident, work=work)
                                if row is None:
                                    raise HttpError("E_NOT_FOUND")
                                return map_relation(row)
                            if ident is not None:
                                await resources.provider.read_memory(session, ident)
                            page = await repo.read_keyset_page(
                                limit=command.payload.limit,
                                last=state.last if state else None,
                                upper=state.upper if state else None,
                                memory_id=ident,
                                work=work,
                            )
                            more = len(page.rows) > command.payload.limit
                            rows = page.rows[: command.payload.limit]
                            cursor = None
                            if more:
                                cursor = self.cursor_codec.issue(
                                    ctx,
                                    command,
                                    last=RelationKey(**vars(rows[-1])),
                                    upper=page.upper,
                                    limit=command.payload.limit,
                                    now=self.cursor_codec.clock(),
                                    expires_at=state.expires_at if state else None,
                                )
                            return d.Page[d.Relation](
                                items=[map_relation(row) for row in rows],
                                has_more=more,
                                next_cursor=cursor,
                                request_id=ctx.request_id,
                            )

            producer = asyncio.create_task(produce())
            try:
                result = await asyncio.wait_for(asyncio.shield(producer), max(0, ctx.deadline - self.clock()))
                if self.clock() >= ctx.deadline:
                    raise TimeoutError
                return result
            except (asyncio.CancelledError, TimeoutError):
                work.stop = "hard"
                raise HttpError("E_TIMEOUT") from None
            except GraphBudgetExceeded:
                raise HttpError("E_GRAPH_BACKEND") from None
            except (SQLAlchemyError, sqlite3.Error, OSError):
                raise HttpError("E_STORAGE") from None
            finally:
                # The lease owns the producer, queued reset, rollback and close.
                # Repeated cancellation cannot release it with SQLite still alive.
                while not producer.done():
                    try:
                        await asyncio.shield(producer)
                    except asyncio.CancelledError:
                        work.stop = "hard"
                    except Exception:
                        break
                if producer.done() and not producer.cancelled():
                    producer.exception()

    @staticmethod
    def _root(value: str) -> tuple[Literal["memory", "entity"], str]:
        if isinstance(value, str) and value.startswith("ent_"):
            decode_entity_id(value)
            return "entity", value
        validate_memory_id(value)
        return "memory", value

    async def _resolve_root(self, ctx, operation, resources, session, repo, work, value):
        kind, value = self._root(value)
        if kind == "entity":
            fact_id, slot = decode_entity_id(value)
            eligible = await repo.endpoint_eligibility((fact_id,), work=work)
            if len(eligible) != 1 or eligible[0].memory_type != "fact":
                raise HttpError("E_NOT_FOUND")
            record = await resources.canonical.provider.read_memory(session, fact_id, "fact")
            name = record.memory.get(slot)
            if type(name) is not str or not name:
                raise HttpError("E_NOT_FOUND")
            return {
                "kind": "entity",
                "wire": value,
                "internal": name.lower().replace(" ", "-"),
                "name": name,
                "record": record,
            }
        record = await resources.canonical.provider.read_memory(session, value)
        seeds = []
        if record.memory_type == "fact":
            for slot in ("subject", "object"):
                name = record.memory.get(slot)
                if type(name) is str and name:
                    seeds.append((name.lower().replace(" ", "-"), entity_id(value, slot), name))
        else:
            seeds.append((value, value, None))
        return {
            "kind": "memory",
            "wire": value,
            "internal": value,
            "record": record,
            "seeds": tuple(seeds),
            "node": map_memory_node(ctx, operation, record),
        }

    async def _canonical_window(self, repo, roots, *, depth, direction, query, ledger, work):
        queue = deque((root, 0) for root in roots)
        visited = set()
        rows = {}
        depth_pending = False
        while queue:
            root, level = queue.popleft()
            if root in visited:
                continue
            visited.add(root)
            if level >= depth:
                depth_pending = True
                continue
            remaining = ledger.remaining("edges")
            if remaining <= 0:
                raise GraphBudgetExceeded("edges", GRAPH_MAX_EDGES, ledger.edges + 1)
            page = await repo.expand_incident(
                root,
                direction=direction,
                relation_types=tuple(query.relation_types),
                memory_types=tuple(getattr(query, "memory_types", ())),
                lifecycle_states=tuple(getattr(query, "lifecycle_states", ())),
                limit=min(100, remaining),
                last=None,
                work=work,
            )
            for row in page.rows:
                ledger.charge("edges")
                key = (row.source_id, row.target_id, row.relation_type)
                rows[key] = row
                if direction == "out":
                    neighbors = (row.target_id,) if row.source_id == root else ()
                else:
                    neighbors = tuple(
                        endpoint for endpoint in (row.source_id, row.target_id) if endpoint != root
                    ) or (root,)
                for neighbor in neighbors:
                    if neighbor not in visited:
                        queue.append((neighbor, level + 1))
            if len(page.rows) > min(100, remaining):
                raise GraphBudgetExceeded("edges", GRAPH_MAX_EDGES, ledger.edges + 1)
        return tuple(rows.values()), depth_pending

    async def _projection(self, repo, window, query, ledger, work):
        if window is None:
            return {}, (), {}
        names = tuple(dict.fromkeys(node.name for node in window.nodes if node.type == "entity"))
        if len(names) > ledger.remaining("candidates"):
            raise GraphBudgetExceeded("candidates", WITNESS_CANDIDATES_MAX, ledger.candidates + len(names))
        witnesses = (
            await repo.resolve_witnesses(names, limit=ledger.remaining("candidates") + 1, work=work)
            if names
            else ()
        )
        if len(witnesses) > ledger.remaining("candidates"):
            raise GraphBudgetExceeded("candidates", WITNESS_CANDIDATES_MAX, ledger.candidates + len(witnesses))
        by_name = {}
        returned_names = set()
        for fact_id, slot, name, lifecycle in witnesses:
            ledger.charge("candidates")
            returned_names.add(name)
            if getattr(query, "memory_types", ()) and "fact" not in query.memory_types:
                continue
            if getattr(query, "lifecycle_states", ()) and lifecycle not in query.lifecycle_states:
                continue
            try:
                handle = entity_id(fact_id, slot)
            except HttpError:
                continue
            by_name.setdefault(name, []).append(handle)
        for name in names:
            if name not in returned_names:
                ledger.charge("candidates")

        internal_to_rep = {}
        handle_to_rep = {}
        nodes = {}
        for node in window.nodes:
            if node.type != "entity" or node.name not in by_name or node.id != node.name.lower().replace(" ", "-"):
                continue
            representative = min(by_name[node.name])
            internal_to_rep[node.id] = representative
            for handle in by_name[node.name]:
                handle_to_rep[handle] = representative
            nodes[representative] = map_entity_node(representative)
        edges = {}
        projection_allowed = not query.relation_types or "[MASKED]" in query.relation_types
        if projection_allowed:
            for edge in window.edges:
                source = internal_to_rep.get(edge.source_id)
                target = internal_to_rep.get(edge.target_id)
                if source is None or target is None:
                    continue
                mapped = map_projection_edge(source, target)
                edges[mapped.id] = mapped
        return nodes, tuple(edges.values()), handle_to_rep

    async def _execute_graph(self, ctx: RequestContext, command: d.GraphReadCommand) -> d.GraphResult:
        if command.operation == "graph.neighborhood":
            query = command.payload
            raw_roots = (query.entity_id or query.memory_id,)
            depth, direction = query.depth, "both"
        elif command.operation == "graph.path":
            query = command.payload
            raw_roots = (query.source_id, query.target_id)
            depth, direction = query.max_depth, "out"
        else:
            query = d.NeighborhoodQuery(
                memory_id=None,
                entity_id=command.path_ids["entity_id"],
                depth=1,
                limit=100,
                edge_limit=200,
            )
            raw_roots = (command.path_ids["entity_id"],)
            depth, direction = 1, "both"
        root_kinds = tuple(self._root(root)[0] for root in raw_roots)
        remaining = ctx.deadline - self.clock()
        if remaining <= 0:
            raise HttpError("E_TIMEOUT")
        soft = ctx.deadline - min(0.050, remaining / 10)
        work = RelationReadWork(soft_deadline=soft, hard_deadline=ctx.deadline)
        ledger = _ReadLedger()

        async with self.leases.acquire(ctx) as scope:
            resources = self.bind(scope)
            if resources.graph is None and "entity" in root_kinds:
                raise HttpError("E_CAPABILITY_UNAVAILABLE")
            custody = self.native(scope)
            window = None
            resolved = []
            try:
                async with resources.canonical.session_factory() as session, session.begin():
                    repo = RelationRepository(session)
                    for root in raw_roots:
                        resolved.append(
                            await self._resolve_root(ctx, command.operation, resources, session, repo, work, root)
                        )

                native_roots = []
                for root in resolved:
                    if root["kind"] == "entity":
                        native_roots.append(root["internal"])
                    else:
                        native_roots.extend(seed[0] for seed in root["seeds"])
                native_roots = tuple(dict.fromkeys(native_roots))
                if resources.graph is not None and native_roots:
                    started, code = self.clock(), "E_INTERNAL"
                    try:
                        handle = await custody(
                            resources.graph.read_window,
                            native_roots,
                            depth=depth,
                            direction=direction,
                            soft_deadline=soft,
                            hard_deadline=ctx.deadline,
                            max_nodes=ledger.remaining("nodes"),
                            max_edges=ledger.remaining("edges"),
                            max_pairs=ledger.remaining("pairs"),
                            kind="graph-read",
                        )
                        window = await handle.result()
                        ledger.absorb(window)
                        code = None
                    except GraphReadDeadlineExceeded:
                        code = "E_TIMEOUT"
                        raise HttpError("E_TIMEOUT") from None
                    except GraphBudgetExceeded:
                        code = "E_GRAPH_BACKEND"
                        raise
                    except Exception:
                        code = "E_GRAPH_BACKEND"
                        raise HttpError("E_GRAPH_BACKEND") from None
                    finally:
                        self.metrics.retrieval_completed(ctx, "graph", self.clock() - started, code)
                    await custody.drain()

                async with resources.canonical.session_factory() as session, session.begin():
                    await session.execute(text("BEGIN"))
                    repo = RelationRepository(session)
                    final_roots = [
                        await self._resolve_root(ctx, command.operation, resources, session, repo, work, root)
                        for root in raw_roots
                    ]
                    canonical_root_ids = tuple(root["wire"] for root in final_roots if root["kind"] == "memory")
                    canonical_rows, canonical_depth = await self._canonical_window(
                        repo,
                        canonical_root_ids,
                        depth=depth,
                        direction=direction,
                        query=query,
                        ledger=ledger,
                        work=work,
                    ) if canonical_root_ids else ((), False)
                    canonical_ids = set(canonical_root_ids)
                    for row in canonical_rows:
                        canonical_ids.update((row.source_id, row.target_id))
                    canonical_nodes = {}
                    for ident in sorted(canonical_ids):
                        record = await resources.canonical.provider.read_memory(session, ident)
                        canonical_nodes[ident] = map_memory_node(ctx, command.operation, record)
                    projection_nodes, projection_edges, representatives = await self._projection(
                        repo, window, query, ledger, work
                    )

                nodes = {**canonical_nodes, **projection_nodes}
                edges = {edge.id: edge for edge in (map_canonical_edge(row) for row in canonical_rows)}
                edges.update({edge.id: edge for edge in projection_edges})
                wire_roots = []
                for root in final_roots:
                    if root["kind"] == "memory":
                        wire_roots.append(root["wire"])
                        wire_roots.extend(
                            representatives[seed[1]] for seed in root["seeds"] if seed[1] in representatives
                        )
                    else:
                        representative = representatives.get(root["wire"])
                        if representative is None:
                            raise HttpError("E_NOT_FOUND")
                        wire_roots.append(representative)

                soft_stop = window is not None and window.soft_stopped
                depth_stop = canonical_depth or (window is not None and bool(window.depth_frontier))
                if self.clock() >= ctx.deadline:
                    raise HttpError("E_TIMEOUT")
                if command.operation == "graph.path":
                    return self._path_result(
                        ctx,
                        query,
                        tuple(wire_roots),
                        nodes,
                        tuple(edges.values()),
                        soft_stop,
                        depth_stop,
                        resources.graph is not None,
                    )
                return self._neighborhood_result(
                    ctx, query, tuple(wire_roots), nodes, tuple(edges.values()), soft_stop, depth_stop,
                    resources.graph is not None,
                )
            except HttpError:
                raise
            except GraphBudgetExceeded:
                raise HttpError("E_GRAPH_BACKEND") from None
            except (SQLAlchemyError, sqlite3.Error, OSError):
                raise HttpError("E_STORAGE") from None
            finally:
                await custody.drain()

    def _neighborhood_result(self, ctx, query, roots, nodes, edges, soft_stop, depth_stop, graph_present):
        adjacency = {ident: [] for ident in nodes}
        for edge in edges:
            if edge.source_id in nodes and edge.target_id in nodes:
                adjacency[edge.source_id].append((edge.target_id, edge))
                adjacency[edge.target_id].append((edge.source_id, edge))
        queue = deque((root, 0) for root in roots if root in nodes)
        selected_nodes, selected_edges, seen = {}, {}, set()
        reason = "timeout" if soft_stop else None
        while queue and reason is None:
            current, level = queue.popleft()
            if current not in selected_nodes:
                if len(selected_nodes) >= query.limit:
                    reason = "nodes"
                    break
                selected_nodes[current] = nodes[current]
            if current in seen:
                continue
            seen.add(current)
            candidates = sorted(adjacency.get(current, ()), key=lambda item: (item[0], item[1].id))
            if level >= query.depth:
                if candidates:
                    reason = "depth"
                continue
            for neighbor, edge in candidates:
                if neighbor not in selected_nodes:
                    if len(selected_nodes) >= query.limit:
                        reason = "nodes"
                        break
                    selected_nodes[neighbor] = nodes[neighbor]
                    queue.append((neighbor, level + 1))
                if edge.id not in selected_edges:
                    if len(selected_edges) >= query.edge_limit:
                        reason = "edges"
                        break
                    selected_edges[edge.id] = edge
        if reason is None and depth_stop:
            reason = "depth"
        return map_graph_result(
            ctx,
            tuple(selected_nodes.values()),
            tuple(selected_edges.values()),
            (),
            consistency=map_consistency("unknown" if graph_present else "unavailable"),
            stop=reason,
        )

    def _path_result(self, ctx, query, roots, nodes, edges, soft_stop, depth_stop, graph_present):
        source, target = roots
        if source not in nodes or target not in nodes:
            raise HttpError("E_NOT_FOUND")
        adjacency = {ident: [] for ident in nodes}
        edge_by_pair = {}
        for edge in edges:
            if edge.source_id in nodes and edge.target_id in nodes:
                adjacency[edge.source_id].append(edge.target_id)
                edge_by_pair.setdefault((edge.source_id, edge.target_id), edge)
        paths = []
        queue = deque([(source, (source,))])
        while queue and not soft_stop:
            current, path = queue.popleft()
            if current == target:
                paths.append(path)
                continue
            if len(path) - 1 >= query.max_depth:
                continue
            for neighbor in sorted(set(adjacency.get(current, ()))):
                if neighbor not in path:
                    queue.append((neighbor, path + (neighbor,)))
        paths.sort(key=lambda path: (len(path), path))
        selected_ids = set(roots)
        selected_edges = {}
        for path in paths:
            selected_ids.update(path)
            for pair in zip(path, path[1:]):
                edge = edge_by_pair[pair]
                selected_edges[edge.id] = edge
        reason = "timeout" if soft_stop else ("depth" if depth_stop and not paths else None)
        if len(selected_ids) > query.node_limit:
            reason = reason or "nodes"
            selected_ids = set(sorted(selected_ids)[: query.node_limit])
        selected_edges = {
            ident: edge for ident, edge in selected_edges.items()
            if edge.source_id in selected_ids and edge.target_id in selected_ids
        }
        if len(selected_edges) > query.edge_limit:
            reason = reason or "edges"
            selected_edges = dict(list(sorted(selected_edges.items()))[: query.edge_limit])
        paths = [path for path in paths if all(vertex in selected_ids for vertex in path)]
        return map_graph_result(
            ctx,
            tuple(nodes[ident] for ident in sorted(selected_ids)),
            tuple(selected_edges.values()),
            tuple(paths),
            consistency=map_consistency("unknown" if graph_present else "unavailable"),
            stop=reason,
        )

    async def relations_for_memory(self, ctx: RequestContext, memory_id: str, query: d.PageQuery) -> d.Page[d.Relation]:
        return await self.execute(
            ctx,
            d.COMMAND_TYPES["relations.for_memory"](
                operation="relations.for_memory", profile_id=ctx.profile_id, path_ids={"id": memory_id}, payload=query
            ),
        )

    async def relations_list(self, ctx: RequestContext, query: d.PageQuery) -> d.Page[d.Relation]:
        return await self.execute(
            ctx,
            d.COMMAND_TYPES["relations.list"](
                operation="relations.list", profile_id=ctx.profile_id, path_ids={}, payload=query
            ),
        )

    async def relations_get(self, ctx: RequestContext, relation_id: str) -> d.Relation:
        return await self.execute(
            ctx,
            d.COMMAND_TYPES["relations.get"](
                operation="relations.get", profile_id=ctx.profile_id, path_ids={"id": relation_id}, payload=d.Empty()
            ),
        )

    async def graph_neighborhood(self, ctx: RequestContext, query: d.NeighborhoodQuery) -> d.GraphResult:
        return await self.execute(
            ctx,
            d.COMMAND_TYPES["graph.neighborhood"](
                operation="graph.neighborhood", profile_id=ctx.profile_id, path_ids={}, payload=query
            ),
        )

    async def graph_path(self, ctx: RequestContext, query: d.PathRequest) -> d.GraphResult:
        return await self.execute(
            ctx,
            d.COMMAND_TYPES["graph.path"](
                operation="graph.path", profile_id=ctx.profile_id, path_ids={}, payload=query
            ),
        )

    async def graph_entity(self, ctx: RequestContext, entity_id_value: str) -> d.GraphResult:
        return await self.execute(
            ctx,
            d.COMMAND_TYPES["graph.entity"](
                operation="graph.entity",
                profile_id=ctx.profile_id,
                path_ids={"entity_id": entity_id_value},
                payload=d.Empty(),
            ),
        )


def create_graph_read_port(
    leases: ProfileLeases,
    bind: ReadBinder,
    *,
    native: NativeBinder,
    cursor_codec: RelationCursorCodec,
    metrics: MetricsSink,
) -> GraphReadPort:
    return GraphReadService(leases, bind, native=native, cursor_codec=cursor_codec, metrics=metrics)
