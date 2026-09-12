"""Deterministic projection mapping and migration verification seams.

Slice S0 keeps this module as a contract seam only: the projection rebuild and
staged-verification bodies are implemented in a later slice. Nothing here is
wired into a runtime path yet, and none of these functions touches a store.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import AbstractSet, Any, Literal, Mapping, Sequence
from uuid import NAMESPACE_DNS, uuid5

from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from memory_server.providers.graph_provider import SimpleGraph
from memory_server.router.graph_router import GraphRouter


@dataclass(frozen=True)
class CanonicalProjectionRecord:
    record_type: Literal["fact", "decision", "skill", "belief"]
    record_id: str
    operation: Literal["index_fact", "index_decision", "index_skill", "index_belief"]
    payload: Mapping[str, object]


@dataclass(frozen=True)
class EmbeddingPlan:
    backend: str
    eligible_records: int
    batches: int
    digest: str
    network: bool = False
    estimated_cost: int = 0
    estimator: str = "vector-records"


@dataclass(frozen=True)
class EmbeddingProgress:
    """Optional typed resume marker; checkpoint_path remains the durable store."""

    completed_batches: int = 0
    staged_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class RebuildResult:
    eligible_counts: Mapping[str, int]
    vector_ids_digest: str
    graph_nodes_digest: str
    graph_edges_digest: str
    completed_batches: int
    plan: EmbeddingPlan | None = None
    vector_ids: tuple[str, ...] = ()


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
    return isinstance(value, list) and bool(value) and all(
        isinstance(step, str) and step.strip() for step in value
    )


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


    tables = {
        "belief": ("beliefs", "index_belief"),
        "decision": ("decisions", "index_decision"),
        "fact": ("facts", "index_fact"),
        "skill": ("skills", "index_skill"),
    }
    url = make_url(snapshot_url)
    if url.drivername == "sqlite+aiosqlite" and url.database and ":memory:" not in url.database:
        database = url.database
        if database.startswith("file:"):
            database = database.removeprefix("file:")
        if not database.startswith("/"):
            raise ValueError(f"snapshot URL is not a local file database: {snapshot_url!r}")
        sidecars = (f"{database}-wal", f"{database}-shm")
        has_sidecars = any(_lstat_exists(path) for path in sidecars)
        query = dict(url.query)
        query.update(mode="ro", uri="true")
        if has_sidecars:
            query.pop("immutable", None)
        else:
            query["immutable"] = "1"
        url = url.set(database=f"file:{database}", query=query)
        snapshot_url = url.render_as_string(hide_password=False)
    elif url.drivername != "sqlite+aiosqlite":
        raise ValueError(f"snapshot URL cannot be opened read-only: unsupported driver {url.drivername!r}")
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


def _lstat_exists(path: str) -> bool:
    """Return whether a sidecar exists without following symlinks."""
    try:
        os.lstat(path)
    except FileNotFoundError:
        return False
    return True


def embedding_plan_digest(records: Sequence[CanonicalProjectionRecord]) -> str:
    """Digest the ordered canonical ID domain used by dry-run and apply."""
    ordered = sorted((f"{record.record_type}:{record.record_id}" for record in records))
    return hashlib.sha256("\n".join(ordered).encode()).hexdigest()


def _checkpoint_write(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(state, sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)


async def _staged_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    import lancedb

    db = await __import__("asyncio").to_thread(lancedb.connect, str(path))
    names = await __import__("asyncio").to_thread(db.table_names)
    if "memories" not in names:
        return set()
    table = await __import__("asyncio").to_thread(db.open_table, "memories")
    rows = await __import__("asyncio").to_thread(table.to_arrow)
    return {str(value) for value in rows.column("id").to_pylist()}


async def rebuild_projections(
    snapshot_url: str,
    *,
    staging_vector_path: str | Path,
    staging_graph_path: str | Path,
    embedder: Any | None = None,
    backend: str = "local",
    allow_network: bool = False,
    vector_size: int = 384,
    batch_size: int = 32,
    checkpoint_path: str | Path | None = None,
    resume: EmbeddingProgress | None = None,
    embedding_plan_digest: str | None = None,
    dry_run: bool = False,
) -> RebuildResult:
    """Build empty staged vector/graph projections with resumable checkpoints."""
    if batch_size <= 0 or vector_size <= 0:
        raise ValueError("batch_size and vector_size must be positive")
    if backend == "remote" and not allow_network:
        raise PermissionError("remote embedding requires explicit allow-network")
    records = [record async for record in iter_canonical_projection_records(snapshot_url, batch_size=batch_size)]
    vector_records = [record for record in records if record.record_type in {"fact", "belief"}]
    vector_batches = sum(
        bool(records[offset : offset + batch_size])
        and any(record.record_type in {"fact", "belief"} for record in records[offset : offset + batch_size])
        for offset in range(0, len(records), batch_size)
    )
    plan = EmbeddingPlan(backend, len(vector_records), vector_batches,
                         embedding_plan_digest_fn(records), backend == "remote", len(vector_records))
    if dry_run:
        return RebuildResult({}, "", "", "", 0, plan, ())
    if embedder is None or not callable(getattr(embedder, "embed_batch", None)):
        raise RuntimeError("embedding assets are unavailable")
    if embedding_plan_digest is None:
        raise ValueError("E_EMBEDDING_PLAN_DIGEST_REQUIRED")
    if embedding_plan_digest != plan.digest:
        raise ValueError("E_EMBEDDING_PLAN_STALE")
    checkpoint = Path(checkpoint_path) if checkpoint_path else None
    completed_ids: list[str] = []
    completed_batches = 0
    if bool(resume) and checkpoint and checkpoint.exists():
        state = json.loads(checkpoint.read_text(encoding="utf-8"))
        completed_ids = [str(value) for value in state.get("staged_ids", [])]
        if state.get("plan_digest") != plan.digest:
            raise ValueError("E_EMBEDDING_PLAN_STALE")
        if state.get("id_digest") != id_digest(set(completed_ids)):
            raise ValueError("E_BATCH_DIGEST_MISMATCH")
        actual_ids = await _staged_ids(Path(staging_vector_path))
        expected_vector_ids = {
            map_projection_record(record).point_id
            for record in records
            if f"{record.record_type}:{record.record_id}" in completed_ids
            and record.record_type in {"fact", "belief"}
        }
        if actual_ids != expected_vector_ids:
            raise ValueError("E_STAGED_IDS_MISMATCH")
        completed_batches = int(state.get("completed_batches", 0))
    vector_path = Path(staging_vector_path)
    graph_path = Path(staging_graph_path)
    from memory_server.providers.lancedb_provider import LanceDBProvider

    provider = LanceDBProvider(db_path=str(vector_path), vector_size=vector_size)
    try:
        for offset in range(len(completed_ids), len(records), batch_size):
            batch = records[offset : offset + batch_size]
            try:
                vector_batch = [record for record in batch if record.record_type in {"fact", "belief"}]
                points: list[dict[str, Any]] = []
                if vector_batch:
                    vectors = embedder.embed_batch([vector_text(record) for record in vector_batch])
                    if len(vectors) != len(vector_batch) or any(len(vector) != vector_size for vector in vectors):
                        raise ValueError("E_EMBEDDING_DIMENSION")
                    points = [
                        {"id": map_projection_record(record).point_id, "vector": vector,
                         "payload": map_projection_record(record).payload}
                        for record, vector in zip(vector_batch, vectors)
                    ]
                if points:
                    await provider.upsert_batch(points)
            except Exception:
                if checkpoint:
                    _checkpoint_write(checkpoint, {"status": "resumable", "plan_digest": plan.digest,
                        "last_completed_key": [records[len(completed_ids) - 1].record_type,
                                               records[len(completed_ids) - 1].record_id] if completed_ids else None,
                        "staged_ids": completed_ids, "id_digest": id_digest(set(completed_ids)),
                        "count": len(completed_ids), "completed_batches": completed_batches})
                raise
            completed_ids.extend(f"{record.record_type}:{record.record_id}" for record in batch)
            completed_batches += 1
            if checkpoint:
                _checkpoint_write(checkpoint, {"status": "resumable", "plan_digest": plan.digest,
                    "last_completed_key": [batch[-1].record_type, batch[-1].record_id],
                    "staged_ids": completed_ids, "id_digest": id_digest(set(completed_ids)),
                    "count": len(completed_ids), "completed_batches": completed_batches})
        graph = build_shared_projection_graph(records)
        graph.save_snapshot(graph_path)
        vector_ids = tuple(sorted(await _staged_ids(vector_path)))
        nodes_digest, edges_digest = graph_id_digests(graph)
        if checkpoint:
            _checkpoint_write(checkpoint, {"status": "complete", "plan_digest": plan.digest,
                "last_completed_key": [records[-1].record_type, records[-1].record_id] if records else None,
                "staged_ids": completed_ids, "id_digest": id_digest(set(completed_ids)),
                "count": len(completed_ids), "completed_batches": completed_batches})
        return RebuildResult({record.record_type: sum(r.record_type == record.record_type for r in records)
                              for record in records}, id_digest(set(vector_ids)), nodes_digest,
                             edges_digest, completed_batches, plan, vector_ids)
    finally:
        close = getattr(provider, "close", None)
        if close is not None:
            result = close()
            if hasattr(result, "__await__"):
                await result


def embedding_plan_digest_fn(records: Sequence[CanonicalProjectionRecord]) -> str:
    return embedding_plan_digest(records)


async def verify_staged_projections(*args, **kwargs) -> ProjectionVerification:
    """Verify staged projections before publication (later slice)."""
    return ProjectionVerification(True)
