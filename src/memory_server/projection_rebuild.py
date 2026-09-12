"""Deterministic projection mapping and migration verification seams.

Slice S0 keeps this module as a contract seam only: the projection rebuild and
staged-verification bodies are implemented in a later slice. Nothing here is
wired into a runtime path yet, and none of these functions touches a store.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import AbstractSet, Literal, Mapping, Sequence
from uuid import NAMESPACE_DNS, uuid5

from memory_server.providers.graph_provider import SimpleGraph
from memory_server.router.graph_router import GraphRouter


@dataclass(frozen=True)
class CanonicalProjectionRecord:
    record_type: Literal["fact", "decision", "skill", "belief"]
    record_id: str
    operation: Literal["index_fact", "index_decision", "index_skill", "index_belief"]
    payload: Mapping[str, object]


@dataclass(frozen=True)
class RebuildResult:
    eligible_counts: Mapping[str, int]
    vector_ids_digest: str
    graph_nodes_digest: str
    graph_edges_digest: str
    completed_batches: int


@dataclass(frozen=True)
class ProjectionVerification:
    valid: bool
    vector_ids_digest: str = ""
    graph_nodes_digest: str = ""
    graph_edges_digest: str = ""
    errors: tuple[str, ...] = ()


def deterministic_vector_id(record_type: str, record_id: str) -> str:
    return str(uuid5(NAMESPACE_DNS, f"{record_type}:{record_id}"))


def vector_text(record: CanonicalProjectionRecord) -> str:
    if record.record_type == "fact":
        payload = record.payload
        return f"{payload.get('subject', '')} {payload.get('predicate', '')} {payload.get('object', '')}"
    return str(record.payload.get("proposition", ""))


def id_digest(ids: AbstractSet[str]) -> str:
    return hashlib.sha256("\n".join(sorted(ids)).encode()).hexdigest()


@dataclass(frozen=True)
class MappedProjection:
    vector_text: str = ""
    point_id: str = ""
    payload: Mapping[str, object] = None  # type: ignore[assignment]


def _nonblank(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _valid_steps(value: object) -> bool:
    return isinstance(value, list) and all(isinstance(step, str) and step.strip() for step in value)


def is_eligible(record: CanonicalProjectionRecord) -> bool:
    payload = record.payload
    lifecycle = payload.get("lifecycle_state")
    if record.record_type == "belief":
        return lifecycle == "active" and _nonblank(payload.get("proposition"))
    if record.record_type == "fact":
        return lifecycle in {"candidate", "validated", "active"} and all(
            _nonblank(payload.get(key)) for key in ("subject", "predicate", "object")
        )
    if record.record_type == "decision":
        return lifecycle in {"candidate", "validated", "active"} and all(
            _nonblank(payload.get(key)) for key in ("choice", "reason", "context")
        )
    if record.record_type == "skill":
        return (
            lifecycle in {"candidate", "validated", "active"}
            and _nonblank(payload.get("purpose"))
            and _valid_steps(payload.get("steps"))
        )
    return False


def map_projection_record(record: CanonicalProjectionRecord) -> MappedProjection:
    """Create the exact shared vector projection for a canonical row."""
    payload = record.payload
    if record.record_type == "fact":
        value = {
            "subject": payload["subject"],
            "predicate": payload["predicate"],
            "object": payload["object"],
            "source": payload.get("source", ""),
            "memory_type": "fact",
        }
        return MappedProjection(
            f"{payload['subject']} {payload['predicate']} {payload['object']}",
            deterministic_vector_id("fact", record.record_id),
            value,
        )
    if record.record_type == "belief":
        value = {
            "proposition": payload["proposition"],
            "confidence": payload.get("confidence", 0.5),
            "tags": payload.get("tags", []),
            "source": payload.get("source", ""),
            "memory_type": "belief",
        }
        return MappedProjection(str(payload["proposition"]), deterministic_vector_id("belief", record.record_id), value)
    return MappedProjection()


def _node_id(router: GraphRouter, prefix: str, value: str) -> str:
    return router._to_node_id(f"{prefix}{value}")


def build_shared_projection_graph(records: Sequence[CanonicalProjectionRecord]) -> SimpleGraph:
    """Build graph projections in pinned two phases without inventing nodes."""
    router = GraphRouter(graph=SimpleGraph())
    ordered = sorted((record for record in records if is_eligible(record)), key=lambda r: (r.record_type, r.record_id))
    facts = [r for r in ordered if r.record_type == "fact"]
    decisions = [r for r in ordered if r.record_type == "decision"]
    skills = [r for r in ordered if r.record_type == "skill"]
    for record in facts:
        payload = record.payload
        for name in (payload["subject"], payload["object"]):
            node_id = router._to_node_id(str(name))
            if router.graph.get_node(node_id) is None:
                router.graph.add_node(id=node_id, type="entity", name=str(name))
    for record in decisions:
        payload = record.payload
        node_id = _node_id(router, "decision-", str(payload["choice"]))
        if router.graph.get_node(node_id) is None:
            router.graph.add_node(
                id=node_id, type="decision", name=str(payload["choice"]), attributes={"reason": payload["reason"]}
            )
    for record in skills:
        payload = record.payload
        node_id = _node_id(router, "skill-", str(payload["purpose"]))
        if router.graph.get_node(node_id) is None:
            router.graph.add_node(
                id=node_id, type="skill", name=str(payload["purpose"]), attributes={"steps": payload["steps"]}
            )
    seen: set[tuple[str, str, str]] = set()
    for record in facts:
        payload = record.payload
        key = (
            router._to_node_id(str(payload["subject"])),
            router._to_node_id(str(payload["object"])),
            str(payload["predicate"]),
        )
        if key not in seen:
            seen.add(key)
            router.graph.add_edge(*key)
    for record in decisions:
        payload = record.payload
        key = (
            _node_id(router, "decision-", str(payload["choice"])),
            router._to_node_id(str(payload["context"])),
            "decides",
        )
        if key not in seen and router.graph.get_node(key[1]) is not None:
            seen.add(key)
            router.graph.add_edge(*key)
    return router.graph


def graph_id_digests(graph: SimpleGraph) -> tuple[str, str]:
    nodes = {node.id for node in graph.get_all_nodes()}
    edges = {
        f"{edge.source_id}|{edge.target_id}|{edge.relation}"
        for targets in graph._edges.values()
        for group in targets.values()
        for edge in group
    }
    return id_digest(nodes), id_digest(edges)


async def iter_canonical_projection_records(snapshot_url: str, *, batch_size: int):
    """Yield eligible canonical SQL rows ordered by type and id."""
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    import json

    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    tables = {
        "belief": ("beliefs", "index_belief"),
        "decision": ("decisions", "index_decision"),
        "fact": ("facts", "index_fact"),
        "skill": ("skills", "index_skill"),
    }
    engine = create_async_engine(snapshot_url)
    rows: list[CanonicalProjectionRecord] = []
    try:
        async with engine.connect() as conn:
            for record_type, (table, operation) in tables.items():
                result = await conn.execute(text(f"SELECT * FROM {table} ORDER BY id"))
                for row in result.mappings():
                    payload = dict(row)
                    for key in ("steps", "tags"):
                        if isinstance(payload.get(key), str):
                            payload[key] = json.loads(payload[key])
                    record = CanonicalProjectionRecord(record_type, str(payload.pop("id")), operation, payload)
                    if is_eligible(record):
                        rows.append(record)
    finally:
        await engine.dispose()
    ordered = sorted(rows, key=lambda record: (record.record_type, record.record_id))
    for offset in range(0, len(ordered), batch_size):
        for record in ordered[offset : offset + batch_size]:
            yield record


async def rebuild_projections(*args, **kwargs) -> RebuildResult:
    """Rebuild vector/graph projections from canonical SQL (later slice)."""
    return RebuildResult({}, id_digest(set()), id_digest(set()), id_digest(set()), 0)


async def verify_staged_projections(*args, **kwargs) -> ProjectionVerification:
    """Verify staged projections before publication (later slice)."""
    return ProjectionVerification(True)
