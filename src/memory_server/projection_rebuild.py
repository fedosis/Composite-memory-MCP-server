"""Deterministic projection mapping and migration verification seams.

Slice S0 keeps this module as a contract seam only: the projection rebuild and
staged-verification bodies are implemented in a later slice. Nothing here is
wired into a runtime path yet, and none of these functions touches a store.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import AbstractSet, Literal, Mapping
from uuid import NAMESPACE_DNS, uuid5


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


async def iter_canonical_projection_records(snapshot_url: str, *, batch_size: int):
    """Yield canonical projection records from a snapshot (later slice)."""
    if False:  # pragma: no cover - contract seam, no records are produced yet
        yield None
    return


async def rebuild_projections(*args, **kwargs) -> RebuildResult:
    """Rebuild vector/graph projections from canonical SQL (later slice)."""
    return RebuildResult({}, id_digest(set()), id_digest(set()), id_digest(set()), 0)


async def verify_staged_projections(*args, **kwargs) -> ProjectionVerification:
    """Verify staged projections before publication (later slice)."""
    return ProjectionVerification(True)
