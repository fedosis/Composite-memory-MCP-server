"""Deterministic projection mapping and migration verification seams.

This module is the ONE place staged/published projection verification lives:
`verify_staged_projections` (DETAIL 10.3) and `verify_published_projections` are
the single implementation the migration engine ASKS, and S3-05 replaced the S0
contract stub with them. The module therefore also carries the seam's own
implemented-capability flag, which is the channel the engine's capability gate
reads -- never a verdict this module returns:

    STAGED_VERIFICATION_IMPLEMENTED = True

The S0 stub that answered ``ProjectionVerification(True)`` to a staged entry that
did not exist is GONE; the engine's gate still rejects that behaviour, and a
return value is still never the thing the gate trusts.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import math
import os
import re
import sqlite3
import stat
import time
from collections import Counter
from dataclasses import dataclass, field
from functools import wraps
from pathlib import Path
from types import MappingProxyType
from typing import AbstractSet, Any, Literal, Mapping, Sequence
from uuid import NAMESPACE_DNS, uuid4, uuid5

import prometheus_client
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from memory_server.evaluation.metrics import metrics_registry
from memory_server.providers.graph_provider import SimpleGraph
from memory_server.router.graph_router import GraphRouter
from memory_server.runtime.runtime import MaintenanceError

_projection_verification_failures = prometheus_client.Counter(
    "cmms_projection_verification_failures_total",
    "Projection verification failures",
    ["store"],
    registry=metrics_registry,
)
_projection_failure_children = {
    store: _projection_verification_failures.labels(store=store) for store in ("vector", "graph")
}


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
    """The verdict of ONE staged or published verification.

    The first five fields are the S0 contract and keep their meaning. The S3-05
    fields are additive and every one of them carries a default, so the S0
    construction (`ProjectionVerification(True)`) stays a valid object -- which is
    exactly why it is the shape the engine's capability gate must still reject
    when a seam answers it. The verifier fills the fields from the ACTUAL reopened
    artifacts and from the expectation it derives itself from the canonical
    snapshot.
    """

    valid: bool
    vector_ids_digest: str = ""
    graph_nodes_digest: str = ""
    graph_edges_digest: str = ""
    errors: tuple[str, ...] = ()
    basis: str = ""
    artifacts: tuple[str, ...] = ()
    expected_vector_ids: tuple[str, ...] = ()
    vector_ids: tuple[str, ...] = ()
    staging_digest: str = ""
    vector_table: str = ""
    vector_dimension: int = 0
    vector_row_count: int = 0
    vector_metric: str = ""
    metric_verifiable: bool = False
    graph_nodes: tuple[tuple[str, str], ...] = ()
    graph_edges: tuple[tuple[str, str, str], ...] = ()
    graph_node_count: int = 0
    graph_edge_count: int = 0
    snapshot_integrity: str = ""
    snapshot_revision: str = ""
    outbox_counts: Mapping[str, int | str] = field(default_factory=dict)


@dataclass(frozen=True)
class ExpectedProjection:
    """The expected projection identity, derived from the canonical corpus only.

    Derived by pure set algebra over the eligible canonical records (addendum
    A.3.2/A.3.3): nodes by `(id, type)`, edges by `(source_id, target_id,
    relation)`, vectors by the deterministic point id. It is NOT derived from
    `RebuildResult`, from a staged store, or from any digest the rebuild
    computed -- `build_shared_projection_graph` is deliberately not called.
    """

    eligible_counts: Mapping[str, int]
    vector_ids: frozenset[str]
    graph_nodes: frozenset[tuple[str, str]]
    graph_edges: frozenset[tuple[str, str, str]]


# The seam's OWN report of what it implements. The engine's capability gate reads
# THIS (or the seam's refusal of a negative probe) and never a return value; see
# `profile_migration.staged_verification_capability`.
STAGED_VERIFICATION_IMPLEMENTED = True


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


# ---------------------------------------------------------------------------
# S3-05 -- independent staged/published verification
# (DETAIL 10.3, DETAIL.md:637-645 and DETAIL 19.10, DETAIL.md:1077; addendum
# PART A, sha d3dbdf58f19152d1b6efaffa6eab41954eca95580b78465446a2dbabfb1c5784).
#
# This block is the ONE staged/published verification implementation in the
# project. `profile_migration` (the engine) CALLS it and never re-implements it.
#
# The boundaries this block keeps:
# * The expectation is re-derived HERE from the canonical snapshot the caller
#   names -- never from `RebuildResult`, from a staged store, or from any digest
#   the rebuild computed (addendum A.5; matrix S3-05 acceptance 2).
# * Store-level checks are the S3-02/S3-03 hooks (`describe_collection`,
#   `validate_collection`, `validate_snapshot`), consumed, not duplicated.
#   `describe_collection` RAISES while `validate_collection` returns a refusal
#   object (R-S302-d): both shapes are handled. `_PAYLOAD_CONTRACT` is consumed
#   as it is -- including its string `source` requirement, so a NULL canonical
#   `source` refuses the whole artifact (R-S302-g) and the contract is never
#   weakened here. The duplicate-ID diagnostic is consumed (R-S302-c).
# * The plain-table metric is NOT artifact evidence on lancedb 0.34.0: the
#   verdict reports the provider's expectation and marks it unverifiable
#   (R-S302-a).
# * `SimpleGraph.load_snapshot` is deliberately NOT used to reopen an artifact:
#   it would create a `.lock` sibling next to the artifact under verification,
#   i.e. the verifier would mutate what it verifies. Structural validation stays
#   with S3-03's read-only no-follow `validate_snapshot`; the exact `(id, type)`
#   and edge-key sets are read by a bounded no-follow reader here.
# * A zero-node graph means EMPTY, never complete (R-S303-a), and a NON-EMPTY
#   canonical corpus can never pass empty digests (acceptance 4).
# * Every store this module opens is released in a `finally` before the caller
#   may rename anything (DETAIL 10.3, last bullet). lancedb 0.34.0 exposes no
#   connection/table `close()` (R-S302-b), so what is released is the provider
#   or handle THIS module owns -- no more is claimed.
# ---------------------------------------------------------------------------

_GRAPH_SNAPSHOT_MAX_BYTES = 64 * 1024 * 1024
EMPTY_ID_DIGEST = hashlib.sha256(b"").hexdigest()

_OPEN_VERIFICATION_HANDLES: list[str] = []


def outstanding_verification_handles() -> tuple[str, ...]:
    """Stores the verifier opened and has not released yet; empty when released."""
    return tuple(_OPEN_VERIFICATION_HANDLES)


@contextlib.contextmanager
def _held_verification_handle(label: str):
    _OPEN_VERIFICATION_HANDLES.append(label)
    try:
        yield
    finally:
        _OPEN_VERIFICATION_HANDLES.remove(label)


def _normalized_node_id(value: object) -> str:
    """The pinned A.3.2 node-id normalization (``GraphRouter._to_node_id``)."""
    return GraphRouter._to_node_id(str(value))


def _prefixed_node_id(prefix: str, value: object) -> str:
    return GraphRouter._to_node_id(f"{prefix}{value}")


def expected_projection(records: Sequence[CanonicalProjectionRecord]) -> ExpectedProjection:
    """Derive the expected projection identity from the canonical corpus.

    Pure set algebra over the eligible records, in the pinned
    ``(record_type, record_id)`` order of DETAIL 10.2 and the two-phase
    construction of addendum A.3.2: phase 1 materialises every implied node with
    its type, phase 2 creates an edge only between existing nodes and creates no
    node. ``build_shared_projection_graph`` is NOT called: this derivation must
    not depend on the rebuild's own output.
    """
    ordered = sorted(
        (record for record in records if is_eligible(record)),
        key=lambda record: (record.record_type, record.record_id),
    )
    counts: dict[str, int] = {}
    node_types: dict[str, str] = {}
    vector_ids: set[str] = set()
    for record in ordered:
        counts[record.record_type] = counts.get(record.record_type, 0) + 1
        payload = record.payload
        if record.record_type == "fact":
            vector_ids.add(deterministic_vector_id("fact", record.record_id))
            for name in (payload["subject"], payload["object"]):
                node_types.setdefault(_normalized_node_id(name), "entity")
        elif record.record_type == "belief":
            vector_ids.add(deterministic_vector_id("belief", record.record_id))
        elif record.record_type == "decision":
            node_types.setdefault(_prefixed_node_id("decision-", payload["choice"]), "decision")
        elif record.record_type == "skill":
            node_types.setdefault(_prefixed_node_id("skill-", payload["purpose"]), "skill")
    edges: set[tuple[str, str, str]] = set()
    for record in ordered:
        payload = record.payload
        if record.record_type == "fact":
            edges.add(
                (
                    _normalized_node_id(payload["subject"]),
                    _normalized_node_id(payload["object"]),
                    str(payload["predicate"]),
                )
            )
        elif record.record_type == "decision":
            target = _normalized_node_id(payload["context"])
            if target in node_types:
                edges.add((_prefixed_node_id("decision-", payload["choice"]), target, "decides"))
    return ExpectedProjection(
        eligible_counts=counts,
        vector_ids=frozenset(vector_ids),
        graph_nodes=frozenset(node_types.items()),
        graph_edges=frozenset(edges),
    )


def expected_graph_digests(expected: ExpectedProjection) -> tuple[str, str]:
    """A.3.3 digest domain: node IDS and edge KEYS, as ``graph_id_digests`` does."""
    nodes = {node_id for node_id, _ in expected.graph_nodes}
    edges = {f"{source}|{target}|{relation}" for source, target, relation in expected.graph_edges}
    return id_digest(nodes), id_digest(edges)


@dataclass
class _ProjectionObservation:
    """What the verifier ACTUALLY read back out of the artifacts."""

    vector_ids: tuple[str, ...] = ()
    vector_ids_digest: str = ""
    vector_table: str = ""
    vector_dimension: int = 0
    vector_row_count: int = 0
    vector_metric: str = ""
    metric_verifiable: bool = False
    graph_node_count: int = 0
    graph_edge_count: int = 0
    graph_nodes_digest: str = ""
    graph_edges_digest: str = ""
    snapshot_integrity: str = ""
    snapshot_revision: str = ""
    outbox_counts: Mapping[str, int | str] = field(default_factory=dict)


def _add_error(errors: list[str], message: str) -> None:
    if message not in errors:
        errors.append(message)


def _snapshot_path_from_url(snapshot_url: str) -> Path:
    """The local file a canonical snapshot URL names, or a refusal."""
    url = make_url(snapshot_url)
    database = url.database or ""
    if database.startswith("file:"):
        database = database.removeprefix("file:")
    if url.drivername != "sqlite+aiosqlite" or not database or ":memory:" in database:
        raise ValueError(f"the snapshot URL is not a local file database: {snapshot_url!r}")
    if not database.startswith("/"):
        raise ValueError(f"the snapshot URL is not absolute: {snapshot_url!r}")
    return Path(database)


def _observe_snapshot(
    snapshot_url: str, observed: _ProjectionObservation, errors: list[str]
) -> None:
    """DETAIL 10.3 first bullet: integrity, required tables and the revision.

    The check itself is the engine's ONE snapshot verifier (`verify_snapshot`,
    S2-03) rather than a second implementation; this module imports it lazily
    because ``profile_migration`` imports this module at its own import time.
    """
    from memory_server import profile_migration

    try:
        path = _snapshot_path_from_url(snapshot_url)
    except ValueError as exc:
        _add_error(errors, str(exc))
        return
    verification, diagnostics = profile_migration.verify_snapshot(path)
    observed.snapshot_integrity = str(verification.get("integrity", "unknown"))
    observed.snapshot_revision = str(verification.get("alembic_revision") or "")
    counts = verification.get("outbox_counts")
    if isinstance(counts, Mapping):
        observed.outbox_counts = dict(counts)
    if observed.snapshot_integrity != "ok":
        _add_error(errors, "the snapshot PRAGMA integrity_check did not return ok")
    if not verification.get("revision_accepted"):
        _add_error(errors, "the snapshot Alembic revision is absent, ambiguous or not accepted")
    for diagnostic in diagnostics:
        _add_error(errors, f"the snapshot was refused ({diagnostic.code}): {diagnostic.message}")


async def _release_provider(provider: Any) -> None:
    """Release a provider THIS module opened; never claims more (R-S302-b)."""
    close = getattr(provider, "close", None)
    if close is None:
        return
    result = close()
    if hasattr(result, "__await__"):
        await result


async def _observe_vector_artifact(
    expected: ExpectedProjection,
    artifact_path: Path,
    observed: _ProjectionObservation,
    errors: list[str],
    *,
    expected_vector_size: int,
) -> None:
    """Reopen the vector artifact and compare it with the expectation exactly."""
    from memory_server.providers.lancedb_provider import LanceDBProvider

    provider = LanceDBProvider(db_path=str(artifact_path), vector_size=expected_vector_size)
    with _held_verification_handle(f"vector:{artifact_path}"):
        try:
            described = False
            try:
                description = await provider.describe_collection()
            except Exception as exc:  # R-S302-d: this hook RAISES on a refusal.
                _add_error(
                    errors,
                    f"vector artifact is unreadable: {type(exc).__name__}: {exc}",
                )
            else:
                described = True
                observed.vector_table = description.table
                observed.vector_dimension = description.vector_size
                observed.vector_row_count = description.row_count
                observed.vector_metric = description.metric
                # R-S302-a: an expectation, never store-attested evidence.
                observed.metric_verifiable = False
                if description.table != "memories":
                    _add_error(errors, f"vector table {description.table!r} is not 'memories'")
                if description.vector_size != expected_vector_size:
                    _add_error(
                        errors,
                        f"vector dimension {description.vector_size} != {expected_vector_size}",
                    )
                if description.row_count != len(expected.vector_ids):
                    _add_error(
                        errors,
                        f"vector row count {description.row_count} != {len(expected.vector_ids)}",
                    )
            validation = await provider.validate_collection(
                expected_ids=set(expected.vector_ids), expected_vector_size=expected_vector_size
            )
            observed.vector_ids_digest = validation.ids_digest
            for duplicate in validation.duplicate_ids:
                _add_error(errors, f"duplicate IDs: {duplicate}")
            for problem in validation.errors:
                _add_error(errors, problem)
            if not validation.errors and not validation.valid:
                _add_error(errors, "the vector artifact was refused without a cause")
            if described:
                actual_ids = await _staged_ids(Path(artifact_path))
                observed.vector_ids = tuple(sorted(actual_ids))
                expected_ids = set(expected.vector_ids)
                if actual_ids != expected_ids:
                    _add_error(
                        errors,
                        "vector ID set mismatch: missing "
                        f"{sorted(expected_ids - actual_ids)} extra {sorted(actual_ids - expected_ids)}",
                    )
                if observed.vector_ids_digest != id_digest(expected_ids):
                    _add_error(errors, "vector ID digest mismatch")
        finally:
            await _release_provider(provider)


def _read_graph_snapshot(path: Path) -> Mapping[str, Any]:
    """Bounded, no-follow, read-only read of a graph snapshot's identity.

    Structural validation is S3-03's ``validate_snapshot`` and is not repeated
    here; this reader exists because that hook returns counts and
    attribute-inclusive digests, while the acceptance criteria compare the exact
    ``(id, type)`` node set and edge-key set. See the block comment for why
    ``SimpleGraph.load_snapshot`` is not used.
    """
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    with os.fdopen(descriptor, "rb") as stream:
        raw = stream.read(_GRAPH_SNAPSHOT_MAX_BYTES + 1)
    if len(raw) > _GRAPH_SNAPSHOT_MAX_BYTES:
        raise ValueError("the graph snapshot exceeds the bounded read size")
    data = json.loads(raw.decode("utf-8"))
    if not isinstance(data, dict):
        raise ValueError("the graph snapshot is not a JSON object")
    return data


def _observe_graph_artifact(
    expected: ExpectedProjection, artifact_path: Path, observed: _ProjectionObservation, errors: list[str]
) -> None:
    """Validate the graph artifact structurally, then compare its identity."""
    validation = SimpleGraph.validate_snapshot(Path(artifact_path))
    if not validation.valid:
        _add_error(errors, f"graph artifact is invalid: {validation.error}")
    try:
        data = _read_graph_snapshot(Path(artifact_path))
        nodes_raw = data.get("nodes") or {}
        edges_raw = data.get("edges") or []
        actual_nodes = {
            (str(node["id"]), str(node.get("type", "")))
            for node in (nodes_raw.values() if isinstance(nodes_raw, Mapping) else ())
            if isinstance(node, Mapping) and "id" in node
        }
        edge_keys = [
            (str(edge.get("source_id")), str(edge.get("target_id")), str(edge.get("relation")))
            for edge in (edges_raw if isinstance(edges_raw, list) else ())
            if isinstance(edge, Mapping)
        ]
    except Exception as exc:
        _add_error(errors, f"graph artifact is unreadable: {type(exc).__name__}: {exc}")
        return
    node_ids = {node_id for node_id, _ in actual_nodes}
    observed.graph_node_count = len(actual_nodes)
    observed.graph_edge_count = len(edge_keys)
    observed.graph_nodes_digest = id_digest(node_ids)
    observed.graph_edges_digest = id_digest({f"{s}|{t}|{r}" for s, t, r in edge_keys})
    if not validation.valid:
        # The structural refusal already refuses the artifact; its own cause is
        # in `errors` and the identity above is still reported.
        return
    expected_node_ids = {node_id for node_id, _ in expected.graph_nodes}
    if not actual_nodes and expected_node_ids:
        _add_error(
            errors,
            f"graph artifact is EMPTY (0 nodes) but the canonical corpus implies "
            f"{len(expected_node_ids)} nodes: zero nodes is never proof of completeness",
        )
    if actual_nodes != set(expected.graph_nodes):
        _add_error(
            errors,
            "graph node set mismatch: missing "
            f"{sorted(set(expected.graph_nodes) - actual_nodes)} "
            f"extra {sorted(actual_nodes - set(expected.graph_nodes))}",
        )
    orphans = sorted(
        f"{source}|{target}" for source, target, _ in edge_keys if source not in node_ids or target not in node_ids
    )
    if orphans:
        _add_error(errors, "graph artifact has orphan edges: " + ", ".join(orphans))
    duplicated = sorted(
        key for key, count in Counter(edge_keys).items() if count > 1
    )
    if duplicated:
        _add_error(
            errors,
            "duplicate parallel graph edges (multiplicity > 1): "
            + ", ".join(f"{source}|{target}|{relation}" for source, target, relation in duplicated),
        )
    if set(edge_keys) != set(expected.graph_edges):
        _add_error(
            errors,
            "graph edge set mismatch: missing "
            f"{sorted(set(expected.graph_edges) - set(edge_keys))} "
            f"extra {sorted(set(edge_keys) - set(expected.graph_edges))}",
        )
    expected_nodes_digest, expected_edges_digest = expected_graph_digests(expected)
    if observed.graph_nodes_digest != expected_nodes_digest:
        _add_error(errors, "graph node ID digest mismatch")
    if observed.graph_edges_digest != expected_edges_digest:
        _add_error(errors, "graph edge key digest mismatch")


def _verification_digest(expected: ExpectedProjection, observed: _ProjectionObservation) -> str:
    """One deterministic identity over the expectation and what was reopened.

    Deliberately NOT salted with the basis: identical staged and published
    content must produce an identical digest, which is what lets the engine
    prove `matches_staged` on a reopened publication.
    """
    members = [
        "counts=" + ",".join(f"{name}:{count}" for name, count in sorted(expected.eligible_counts.items())),
        "expected_vector_ids=" + id_digest(expected.vector_ids),
        "expected_nodes=" + expected_graph_digests(expected)[0],
        "expected_edges=" + expected_graph_digests(expected)[1],
        "actual_vector_ids=" + observed.vector_ids_digest,
        "actual_nodes=" + observed.graph_nodes_digest,
        "actual_edges=" + observed.graph_edges_digest,
    ]
    return hashlib.sha256("\n".join(members).encode()).hexdigest()


def _empty_verification(basis: str, errors: list[str]) -> ProjectionVerification:
    return ProjectionVerification(
        valid=False,
        graph_nodes_digest=EMPTY_ID_DIGEST,
        graph_edges_digest=EMPTY_ID_DIGEST,
        errors=tuple(errors),
        basis=basis,
        artifacts=("vector", "graph"),
        staging_digest=hashlib.sha256("\n".join(sorted(errors)).encode()).hexdigest(),
    )


async def _verify_projection_artifacts(
    *,
    basis: str,
    snapshot_url: str,
    vector_path: str | Path,
    graph_path: str | Path,
    expected_vector_size: int,
    batch_size: int,
) -> ProjectionVerification:
    """The ONE verification body: staged and published differ only by basis."""
    errors: list[str] = []
    observed = _ProjectionObservation()
    try:
        records = [
            record async for record in iter_canonical_projection_records(snapshot_url, batch_size=batch_size)
        ]
    except Exception as exc:
        return _empty_verification(
            basis, [f"the canonical snapshot could not be read: {type(exc).__name__}: {exc}"]
        )
    expected = expected_projection(records)
    if records and not expected.vector_ids and not expected.graph_nodes:
        _add_error(
            errors,
            "the canonical corpus is non-empty but implies no projection at all: "
            "empty digests must never be accepted as proof",
        )
    _observe_snapshot(snapshot_url, observed, errors)
    vector_errors_before = len(errors)
    await _observe_vector_artifact(
        expected, Path(vector_path), observed, errors, expected_vector_size=expected_vector_size
    )
    if len(errors) > vector_errors_before:
        _projection_failure_children["vector"].inc()
    graph_errors_before = len(errors)
    _observe_graph_artifact(expected, Path(graph_path), observed, errors)
    if len(errors) > graph_errors_before:
        _projection_failure_children["graph"].inc()
    expected_nodes_digest, expected_edges_digest = expected_graph_digests(expected)
    actual_nodes_digest = observed.graph_nodes_digest or EMPTY_ID_DIGEST
    actual_edges_digest = observed.graph_edges_digest or EMPTY_ID_DIGEST
    if observed.graph_nodes_digest != expected_nodes_digest:
        _add_error(errors, "graph node ID digest does not match the canonical corpus")
    if observed.graph_edges_digest != expected_edges_digest:
        _add_error(errors, "graph edge key digest does not match the canonical corpus")
    return ProjectionVerification(
        valid=not errors,
        vector_ids_digest=observed.vector_ids_digest,
        graph_nodes_digest=actual_nodes_digest,
        graph_edges_digest=actual_edges_digest,
        errors=tuple(errors),
        basis=basis,
        artifacts=("vector", "graph"),
        expected_vector_ids=tuple(sorted(expected.vector_ids)),
        vector_ids=observed.vector_ids,
        staging_digest=_verification_digest(expected, observed),
        vector_table=observed.vector_table,
        vector_dimension=observed.vector_dimension,
        vector_row_count=observed.vector_row_count,
        vector_metric=observed.vector_metric,
        metric_verifiable=observed.metric_verifiable,
        graph_nodes=tuple(sorted(expected.graph_nodes)),
        graph_edges=tuple(sorted(expected.graph_edges)),
        graph_node_count=observed.graph_node_count,
        graph_edge_count=observed.graph_edge_count,
        snapshot_integrity=observed.snapshot_integrity,
        snapshot_revision=observed.snapshot_revision,
        outbox_counts=observed.outbox_counts,
    )


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
    """The ids currently in a staged/published vector store, handle released.

    S3-04 review residual R-S304-e: this helper used to leave the opened table
    handle behind. It now releases it in a ``finally`` (lancedb 0.34.0 exposes
    ``close_lsm_writers()``, not ``close()``) and never creates a directory for a
    path that does not exist.
    """
    if not path.exists():
        return set()
    import asyncio

    import lancedb

    db = await asyncio.to_thread(lancedb.connect, str(path))
    names = await asyncio.to_thread(db.table_names)
    if "memories" not in names:
        return set()
    table = await asyncio.to_thread(db.open_table, "memories")
    with _held_verification_handle(f"staged_ids:{path}"):
        try:
            rows = await asyncio.to_thread(table.to_arrow)
            return {str(value) for value in rows.column("id").to_pylist()}
        finally:
            close = getattr(table, "close_lsm_writers", None)
            if close is not None:
                await asyncio.to_thread(close)


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


async def verify_staged_projections(
    *,
    snapshot_url: str,
    staging_vector_path: str | Path,
    staging_graph_path: str | Path,
    expected_vector_size: int = 384,
    batch_size: int = 32,
) -> ProjectionVerification:
    """Verify the STAGED projections before publication (DETAIL 10.3).

    The canonical snapshot is re-read here to derive the expected identity
    independently, then the ACTUAL staged artifacts are reopened and compared
    with it: vector table/schema/dimension/row count/ID set/ID digest/payload
    contract/duplicate IDs, the graph's structural validity, its exact
    ``(id, type)`` node set, its exact edge-key set, orphan edges, parallel-edge
    multiplicity and the ID digests over the A.3.3 domain, plus the snapshot's
    integrity/table set/Alembic revision. Nothing is inferred from
    ``RebuildResult`` and no artifact is mutated; every store this call opens is
    released before it returns, so the caller may rename the staged entries.

    The signature is keyword-only ON PURPOSE, and it stays that way: the S2-06
    capability gate probes this seam with a single positional argument and treats
    a qualified refusal as an implemented capability, so a positional call must
    keep failing rather than being accommodated by widening this contract. S3-06
    therefore opens the gate through the seam's own explicit flag defined in this
    module (`STAGED_VERIFICATION_IMPLEMENTED`), which is the channel that genuinely
    qualifies this implementation -- not through the probe and never through a
    verdict.
    """
    return await _verify_projection_artifacts(
        basis="staged",
        snapshot_url=snapshot_url,
        vector_path=staging_vector_path,
        graph_path=staging_graph_path,
        expected_vector_size=expected_vector_size,
        batch_size=batch_size,
    )


async def verify_published_projections(
    *,
    snapshot_url: str,
    published_vector_path: str | Path,
    published_graph_path: str | Path,
    expected_vector_size: int = 384,
    batch_size: int = 32,
) -> ProjectionVerification:
    """Reopen the PUBLISHED projections and reverify them before `complete`.

    The same one verification body runs against the published entries, so the
    reopened publication must reproduce the same expectation (and, through the
    verdict's identity digest, the same staged identity). A publication that
    cannot be reopened, or that reopens different, never yields a `verified`
    checkpoint.
    """
    return await _verify_projection_artifacts(
        basis="published",
        snapshot_url=snapshot_url,
        vector_path=published_vector_path,
        graph_path=published_graph_path,
        expected_vector_size=expected_vector_size,
        batch_size=batch_size,
    )


# S20A1: owner-only, additive maintenance substrate. Legacy observers stay frozen.
_OWNED_LIMITS = MappingProxyType({
    "reserve": 5 * 1024**3, "rows": 2000, "chunk_bytes": 64 * 1024**2,
    "snapshot": 900, "stage": 900, "verify": 300, "job": 3600,
    "generations": 2, "retention": 24 * 3600, "journal_bytes": 4096,
    "overhead": 64 * 1024 * 16})
_OWNED_TABLES = (("belief", "beliefs"), ("decision", "decisions"), ("fact", "facts"), ("skill", "skills"))
_OWNED_DIRECTORY = ".projection-rebuild"
_OWNED_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC


@dataclass(frozen=True, init=False)
class SnapshotRecord:
    run_id: str
    profile_id: str
    snapshot_url: str
    snapshot_digest: str
    records: tuple[CanonicalProjectionRecord, ...]
    content_digest: str


class _FrozenProjectionList(list):
    def _deny(self, *args, **kwargs):
        raise TypeError("immutable snapshot")

    __setitem__ = __delitem__ = __iadd__ = __imul__ = append = clear = extend = _deny
    insert = pop = remove = reverse = sort = _deny


def _owned_freeze(value):
    if isinstance(value, dict):
        return MappingProxyType({key: _owned_freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return _FrozenProjectionList(_owned_freeze(item) for item in value)
    return value


def _owned_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _owned_check_time(end):
    if time.monotonic() >= end:
        raise MaintenanceError("E_TIMEOUT")


def _owned_free(fd, required=0):
    space = os.fstatvfs(fd)
    free = space.f_bavail * space.f_frsize
    if free < _OWNED_LIMITS["reserve"] + required:
        raise MaintenanceError("E_STORAGE")
    return free


@contextlib.contextmanager
def _owned_dir(name, *, parent=None):
    fd = os.open(name, _OWNED_FLAGS | os.O_DIRECTORY, dir_fd=parent)
    try:
        info = os.fstat(fd)
        if info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise MaintenanceError("E_STORAGE")
        yield fd
    finally:
        os.close(fd)


def _owned_file(fd, name, flags=os.O_RDONLY):
    opened = os.open(name, flags | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=fd)
    info = os.fstat(opened)
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid():
        os.close(opened)
        raise MaintenanceError("E_STORAGE")
    return opened


def _owned_inventory(fd, name, *, sync=False):
    info = os.stat(name, dir_fd=fd, follow_symlinks=False)
    digest, size = hashlib.sha256(), 0
    if stat.S_ISDIR(info.st_mode):
        with _owned_dir(name, parent=fd) as child:
            for entry in sorted(os.listdir(child)):
                identity = _owned_inventory(child, entry, sync=sync)
                digest.update(_owned_json((entry, identity)))
                size += identity[3]
            if sync:
                os.fsync(child)
    else:
        with os.fdopen(_owned_file(fd, name), "rb") as stream:
            while block := stream.read(_OWNED_LIMITS["chunk_bytes"]):
                digest.update(block)
                size += len(block)
            if sync:
                os.fsync(stream.fileno())
    return [info.st_dev, info.st_ino, digest.hexdigest(), size]


def _owned_sanitize(function, args):
    try:
        return True, function(*args)
    except MaintenanceError as exc:
        code = exc.code
    except Exception:
        code = "E_STORAGE"
    return False, code


async def _owned_native(runtime, custody, end, function, *args):
    runtime.check_maintenance(custody)
    _owned_check_time(end)
    handle = await runtime.submit_native(_owned_sanitize, function, args, kind="projection-rebuild")
    async with asyncio.timeout_at(end):
        ok, value = await handle.result()
        if not await handle.drain(end):
            raise MaintenanceError("E_TIMEOUT")
    runtime.check_maintenance(custody)
    if not ok:
        raise MaintenanceError(value)
    return value


def _owned_paths(runtime):
    layout = runtime.layout
    vector = getattr(layout, "vector", None)
    paths = {"vector": getattr(vector, "local_path", None), "graph": getattr(layout, "graph_snapshot_path", None)}
    if getattr(vector, "backend", None) != "lancedb":
        raise MaintenanceError("E_PROJECTION_UNAVAILABLE")
    for target, path in paths.items():
        if not isinstance(path, Path) or path.parent != runtime.root or path.name in {"", ".", ".."}:
            raise MaintenanceError("E_STORAGE")
        paths[target] = path.name
    return paths


def _owned_read_records(path, end):
    records, canonical = [], hashlib.sha256()
    with sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True) as connection:
        connection.row_factory = sqlite3.Row
        if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise MaintenanceError("E_STORAGE")
        for kind, table in _OWNED_TABLES:
            cursor = connection.execute(f"SELECT * FROM {table} ORDER BY id")
            while rows := cursor.fetchmany(_OWNED_LIMITS["rows"]):
                chunk, chunk_bytes = [], 0
                for row in rows:
                    _owned_check_time(end)
                    payload = dict(row)
                    for key in ("steps", "tags"):
                        if isinstance(payload.get(key), str):
                            payload[key] = json.loads(payload[key])
                    key = str(payload.pop("id"))
                    raw = _owned_json((kind, key, payload))
                    if len(raw) > _OWNED_LIMITS["chunk_bytes"]:
                        raise MaintenanceError("E_STORAGE")
                    if chunk_bytes + len(raw) > _OWNED_LIMITS["chunk_bytes"]:
                        records.extend(chunk)
                        chunk, chunk_bytes = [], 0
                    canonical.update(raw)
                    chunk_bytes += len(raw)
                    chunk.append(CanonicalProjectionRecord(kind, key, f"index_{kind}", _owned_freeze(payload)))
                records.extend(chunk)
    return tuple(records), canonical.hexdigest()


def _owned_journal(fd, state):
    state["free_after"] = _owned_free(fd)
    raw = _owned_json(state)
    if len(raw) > _OWNED_LIMITS["journal_bytes"]:
        raise MaintenanceError("E_STORAGE")
    name = f"journal-{uuid4().hex}.tmp"
    opened = _owned_file(fd, name, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
    with os.fdopen(opened, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(name, "journal.json", src_dir_fd=fd, dst_dir_fd=fd)
    os.fsync(fd)


def _owned_errors(function):
    @wraps(function)
    async def boundary(*args, custody, **kwargs):
        try:
            return await function(*args, custody=custody, **kwargs)
        except OSError as exc:
            code = "E_TIMEOUT" if isinstance(exc, TimeoutError) else "E_STORAGE"
            logging.getLogger(__name__).warning("owned I/O failure errno=%s", exc.errno)
        custody._identity[0]._maintenance_state = "blocked"
        # Raise outside the handler: no path-bearing exception context escapes.
        raise MaintenanceError(code)
    return boundary


@_owned_errors
async def snapshot_owned_projection_records(runtime, *, custody) -> SnapshotRecord:
    runtime.check_maintenance(custody)
    if runtime._maintenance_state != "active" or runtime.dependencies.sqlite is None:
        raise MaintenanceError("E_MAINTENANCE_CONFLICT")
    paths = _owned_paths(runtime)
    end, run_id = min(custody._identity[-1], time.monotonic() + _OWNED_LIMITS["snapshot"]), uuid4().hex
    with _owned_dir(runtime.root) as root:
        old = {target: _owned_inventory(root, name) for target, name in paths.items()}
        required = math.ceil(1.25 * sum(value[3] for value in old.values())) + _OWNED_LIMITS["overhead"]
        free = _owned_free(root, required)
        if _OWNED_DIRECTORY not in os.listdir(root):
            os.mkdir(_OWNED_DIRECTORY, 0o700, dir_fd=root)
            os.fsync(root)
        with _owned_dir(_OWNED_DIRECTORY, parent=root) as parent:
            if len(os.listdir(parent)) >= _OWNED_LIMITS["generations"]:
                raise MaintenanceError("E_STORAGE")
            os.mkdir(run_id, 0o700, dir_fd=parent)
            os.fsync(parent)
            with _owned_dir(run_id, parent=parent) as run:
                os.close(_owned_file(run, "snapshot.db", os.O_WRONLY | os.O_CREAT | os.O_EXCL))
                state = {"version": 1, "run": run_id,
                         "profile": hashlib.sha256(runtime.profile_id.encode()).hexdigest(),
                         "root": [os.fstat(root).st_dev, os.fstat(root).st_ino],
                         "config": hashlib.sha256(repr(runtime.config).encode()).hexdigest(),
                         "paths": paths, "old": old, "staged": {},
                         "targets": [], "phase": "snapshot", "intent": None, "snapshot": None,
                         "bytes_written": 0, "bytes_verified": 0, "free_before": free, "free_after": free}
                _owned_journal(run, state)
    destination = runtime.root / _OWNED_DIRECTORY / run_id / "snapshot.db"

    async def backup():
        error = None
        try:
            async with runtime.dependencies.sqlite.engine.connect() as connection:
                raw = await connection.get_raw_connection()
                with sqlite3.connect(destination, check_same_thread=False) as target:
                    def progress(status, remaining, total):
                        _owned_check_time(end)
                        with _owned_dir(runtime.root) as root:
                            _owned_free(root)

                    pending = runtime.spawn_owned_task(lambda: raw.driver_connection.backup(
                        target, pages=256, progress=progress), kind="projection-backup-driver")
                    cancelled = False
                    while not pending.done():
                        try:
                            await asyncio.shield(pending)
                        except asyncio.CancelledError:
                            cancelled = True
                    pending.result()
                    if cancelled:
                        raise asyncio.CancelledError()
        except Exception:
            error = "E_TIMEOUT" if time.monotonic() >= end else "E_STORAGE"
        if error:
            raise MaintenanceError(error)

    task = runtime.spawn_owned_task(backup, kind="projection-backup")
    try:
        async with asyncio.timeout_at(end):
            await asyncio.shield(task)
        runtime.check_maintenance(custody)
        records, content_digest = await _owned_native(runtime, custody, end, _owned_read_records, destination, end)
        observed, errors = _ProjectionObservation(), []
        await _owned_native(runtime, custody, end, _observe_snapshot,
                            f"sqlite+aiosqlite:///{destination}", observed, errors)
        if errors:
            raise MaintenanceError("E_STORAGE")
        with _owned_dir(destination.parent) as run:
            identity = _owned_inventory(run, "snapshot.db", sync=True)
            os.fsync(run)
            state.update(snapshot=identity, bytes_written=identity[3], bytes_verified=identity[3], phase="captured")
            _owned_journal(run, state)
        snapshot = object.__new__(SnapshotRecord)
        vars(snapshot).update(run_id=run_id, profile_id=runtime.profile_id,
                              snapshot_url=f"sqlite+aiosqlite:///{destination}", snapshot_digest=identity[2],
                              records=records, content_digest=content_digest)
        registry = vars(runtime).setdefault("_projection_snapshots", {})
        registry[id(snapshot)] = (snapshot, custody, state, time.monotonic())
        return snapshot
    except BaseException:
        runtime._maintenance_state = "blocked"
        raise


def _owned_vector_read(path, records, dimension, end):
    import lancedb
    import pyarrow as pa

    from memory_server.providers.lancedb_provider import _payload_error

    expected = {map_projection_record(record).point_id: dict(map_projection_record(record).payload)
                for record in records if is_eligible(record) and record.record_type in {"fact", "belief"}}
    database = lancedb.connect(str(path))
    table = database.open_table("memories")
    if not {"id", "vector", "_metadata"}.issubset(table.schema.names):
        raise MaintenanceError("E_VECTOR_BACKEND")
    vector = table.schema.field("vector").type
    if not pa.types.is_fixed_size_list(vector) or vector.list_size != dimension:
        raise MaintenanceError("E_VECTOR_BACKEND")
    seen, verified = set(), 0
    with table.search().limit(None).to_batches(batch_size=1) as reader:
        for batch in reader:
            _owned_check_time(end)
            if batch.num_rows > _OWNED_LIMITS["rows"] or batch.nbytes > _OWNED_LIMITS["chunk_bytes"]:
                raise MaintenanceError("E_STORAGE")
            verified += batch.nbytes
            for row in batch.to_pylist():
                payload, key = json.loads(row["_metadata"]), row["id"]
                if key in seen or key not in expected or _payload_error(payload) or payload != expected[key]:
                    raise MaintenanceError("E_VECTOR_BACKEND")
                if len(row["vector"]) != dimension or any(not math.isfinite(v) for v in row["vector"]):
                    raise MaintenanceError("E_VECTOR_BACKEND")
                seen.add(key)
    if seen != set(expected):
        raise MaintenanceError("E_VECTOR_BACKEND")
    return verified


def _owned_verify(path, targets, records, dimension, end):
    expected, verified = expected_projection(records), 0
    if "vector" in targets:
        verified += _owned_vector_read(path / "vector", records, dimension, end)
    if "graph" in targets:
        observed, errors = _ProjectionObservation(), []
        _observe_graph_artifact(expected, path / "graph", observed, errors)
        if errors:
            raise MaintenanceError("E_GRAPH_BACKEND")
        verified += (path / "graph").stat().st_size
    _owned_check_time(end)
    return verified


def _owned_stage(path, records, targets, embedder, dimension, end):
    if "vector" in targets:
        import lancedb
        import pyarrow as pa

        database = lancedb.connect(str(path / "vector"))
        schema = pa.schema([("id", pa.string()), ("vector", pa.list_(pa.float32(), dimension)),
                            ("_metadata", pa.string())])
        table = database.create_table("memories", schema=schema)
        batch, size = [], 0

        def write():
            _owned_check_time(end)
            vectors = embedder.embed_batch([item.vector_text for item in batch])
            if len(vectors) != len(batch) or any(len(vector) != dimension for vector in vectors):
                raise MaintenanceError("E_VECTOR_BACKEND")
            table.add([{"id": item.point_id, "vector": vector, "_metadata": _owned_json(dict(item.payload)).decode()}
                       for item, vector in zip(batch, vectors)])

        for record in records:
            if not is_eligible(record) or record.record_type not in {"fact", "belief"}:
                continue
            item = map_projection_record(record)
            item_size = len(_owned_json(dict(item.payload))) + len(item.vector_text.encode()) + dimension * 8
            if item_size > _OWNED_LIMITS["chunk_bytes"]:
                raise MaintenanceError("E_STORAGE")
            if batch and (len(batch) >= _OWNED_LIMITS["rows"] or size + item_size > _OWNED_LIMITS["chunk_bytes"]):
                write()
                batch, size = [], 0
            batch.append(item)
            size += item_size
        if batch:
            write()
    if "graph" in targets:
        _owned_check_time(end)
        build_shared_projection_graph(records).save_snapshot(path / "graph")
    for parent, directories, files in os.walk(path, followlinks=False):
        for name in directories:
            os.chmod(Path(parent) / name, 0o700, follow_symlinks=False)
        for name in files:
            os.chmod(Path(parent) / name, 0o600, follow_symlinks=False)
    _owned_check_time(end)


@_owned_errors
async def rebuild_owned_projections(snapshot, *, custody, targets, allow_network_embedding=False) -> RebuildResult:
    runtime = custody._identity[0]
    runtime.check_maintenance(custody)
    registered = getattr(runtime, "_projection_snapshots", {}).get(id(snapshot))
    if not registered or registered[0] is not snapshot or registered[1] is not custody:
        raise MaintenanceError("E_MAINTENANCE_CONFLICT")
    if not isinstance(targets, frozenset) or not targets or not targets <= {"vector", "graph"}:
        raise MaintenanceError("E_VALIDATION")
    if type(allow_network_embedding) is not bool:
        raise MaintenanceError("E_VALIDATION")
    if targets & getattr(runtime.layout, "unavailable_projections", frozenset()):
        raise MaintenanceError("E_PROJECTION_UNAVAILABLE")
    if any(getattr(runtime.dependencies, target) is not None for target in targets):
        raise MaintenanceError("E_MAINTENANCE_CONFLICT")
    embedder = runtime.dependencies.embedder
    if "vector" in targets:
        from memory_server.providers import embedding_provider as embedding

        if embedder is None or not callable(getattr(embedder, "embed_batch", None)):
            raise MaintenanceError("E_PROJECTION_UNAVAILABLE")
        local = type(embedder) is embedding.MockEmbeddingProvider or (
            type(embedder) is embedding.SentenceTransformerEmbeddingProvider and embedder._model is not None)
        if not allow_network_embedding and not local:
            raise MaintenanceError("E_CAPABILITY_UNAVAILABLE")
    _, _, state, started = registered
    if state["phase"] != "captured":
        raise MaintenanceError("E_MAINTENANCE_CONFLICT")
    end = min(custody._identity[-1], started + _OWNED_LIMITS["job"])
    run_path, dimension = runtime.root / _OWNED_DIRECTORY / snapshot.run_id, getattr(runtime.config, "vector_size", 384)
    try:
        records, digest = await _owned_native(runtime, custody, end, _owned_read_records, run_path / "snapshot.db", end)
        if digest != snapshot.content_digest:
            raise MaintenanceError("E_STORAGE")
        with _owned_dir(runtime.root) as root, _owned_dir(run_path) as run:
            if _owned_inventory(run, "snapshot.db") != state["snapshot"]:
                raise MaintenanceError("E_STORAGE")
            if any(_owned_inventory(root, name) != state["old"][target] for target, name in state["paths"].items()):
                raise MaintenanceError("E_STORAGE")
            _owned_free(root, math.ceil(1.25 * sum(item[3] for item in state["old"].values()))
                        + _OWNED_LIMITS["overhead"])
            os.mkdir("stage", 0o700, dir_fd=run)
            os.fsync(run)
            state.update(targets=sorted(targets), phase="stage")
            _owned_journal(run, state)
        stage_end = min(end, time.monotonic() + _OWNED_LIMITS["stage"])
        await _owned_native(runtime, custody, stage_end, _owned_stage, run_path / "stage", records,
                            targets, embedder, dimension, stage_end)
        verify_end = min(end, time.monotonic() + _OWNED_LIMITS["verify"])
        verified = await _owned_native(runtime, custody, verify_end, _owned_verify, run_path / "stage",
                                      targets, records, dimension, verify_end)
        with _owned_dir(runtime.root) as root, _owned_dir(run_path) as run:
            with _owned_dir("stage", parent=run) as stage:
                state["staged"] = {target: _owned_inventory(stage, target, sync=True) for target in targets}
                state.update(phase="verified", bytes_verified=state["bytes_verified"] + verified,
                             bytes_written=state["bytes_written"] + sum(item[3] for item in state["staged"].values()))
                _owned_journal(run, state)
                for target in sorted(targets):
                    runtime.check_maintenance(custody)
                    _owned_check_time(end)
                    name = state["paths"][target]
                    if _owned_inventory(root, name) != state["old"][target]:
                        raise MaintenanceError("E_STORAGE")
                    state.update(phase="publish", intent=target)
                    _owned_journal(run, state)
                    os.rename(name, f"old-{target}", src_dir_fd=root, dst_dir_fd=run)
                    os.fsync(root)
                    os.fsync(run)
                    os.rename(target, name, src_dir_fd=stage, dst_dir_fd=root)
                    os.fsync(stage)
                    os.fsync(root)
                    if _owned_inventory(root, name) != state["staged"][target]:
                        raise MaintenanceError("E_STORAGE")
                    state.update(intent=None)
                    _owned_journal(run, state)
            state.update(phase="published")
            _owned_journal(run, state)
        runtime._maintenance_state = "blocked"
        expected = expected_projection(records)
        nodes_digest, edges_digest = expected_graph_digests(expected)
        return RebuildResult(expected.eligible_counts, id_digest(expected.vector_ids), nodes_digest, edges_digest, 1)
    except BaseException:
        runtime._maintenance_state = "blocked"
        raise


def _owned_load_journal(fd, runtime, run_id, root):
    opened = _owned_file(fd, "journal.json")
    with os.fdopen(opened, "rb") as stream:
        raw = stream.read(_OWNED_LIMITS["journal_bytes"] + 1)
    if len(raw) > _OWNED_LIMITS["journal_bytes"]:
        raise MaintenanceError("E_STORAGE")

    def unique(pairs):
        result = dict(pairs)
        if len(result) != len(pairs):
            raise MaintenanceError("E_STORAGE")
        return result

    state = json.loads(raw, object_pairs_hook=unique)
    keys = {"version", "run", "profile", "root", "config", "paths", "old", "staged", "targets", "phase",
            "intent", "snapshot", "bytes_written", "bytes_verified", "free_before", "free_after"}
    if (not isinstance(state, dict) or set(state) != keys or type(state["version"]) is not int
        or state["version"] != 1 or state["run"] != run_id
        or state["profile"] != hashlib.sha256(runtime.profile_id.encode()).hexdigest()
        or state["config"] != hashlib.sha256(repr(runtime.config).encode()).hexdigest()
        or state["root"] != [os.fstat(root).st_dev, os.fstat(root).st_ino]
        or state["paths"] != _owned_paths(runtime)
        or state["phase"] not in {"snapshot", "captured", "stage", "verified", "publish", "published",
                                  "rollback", "reconciled"}
        or not isinstance(state["targets"], list) or state["targets"] != sorted(set(state["targets"]))
        or not set(state["targets"]) <= {"vector", "graph"}
        or state["intent"] not in [None, *state["targets"]]
        or any(type(state[key]) is not int or state[key] < 0
               for key in ("bytes_written", "bytes_verified", "free_before", "free_after"))):
        raise MaintenanceError("E_STORAGE")
    if (not isinstance(state["old"], dict) or set(state["old"]) != {"vector", "graph"}
        or not isinstance(state["staged"], dict) or not set(state["staged"]) <= {"vector", "graph"}):
        raise MaintenanceError("E_STORAGE")
    snapshots = ([] if state["snapshot"] is None and state["phase"] in {"snapshot", "reconciled"}
                 else [state["snapshot"]])
    for identity in [*state["old"].values(), *state["staged"].values(), *snapshots]:
        if (not isinstance(identity, list) or len(identity) != 4
            or any(type(identity[index]) is not int or identity[index] < 0 for index in (0, 1, 3))
            or not isinstance(identity[2], str) or not re.fullmatch("[0-9a-f]{64}", identity[2])):
            raise MaintenanceError("E_STORAGE")
    return state


def _reconcile_owned_projections(runtime):
    """Called under the newly acquired EX, before the first provider recipe."""
    error = None
    try:
        if not os.path.lexists(runtime.root / _OWNED_DIRECTORY):
            return
        if runtime.state != "STARTING" or runtime.lock is None or runtime.lock._fh is None:
            raise MaintenanceError("E_RUNTIME_NOT_READY")
        if any(getattr(runtime.dependencies, target) is not None for target in ("vector", "graph")):
            raise MaintenanceError("E_STORAGE")
        end = runtime._clock() + _OWNED_LIMITS["verify"]
        with _owned_dir(runtime.root) as root, _owned_dir(_OWNED_DIRECTORY, parent=root) as parent:
            generations = os.listdir(parent)
            if len(generations) > _OWNED_LIMITS["generations"]:
                raise MaintenanceError("E_STORAGE")
            for run_id in sorted(generations):
                _owned_check_time(end)
                if len(run_id) != 32 or any(char not in "0123456789abcdef" for char in run_id):
                    raise MaintenanceError("E_STORAGE")
                with _owned_dir(run_id, parent=parent) as run:
                    state = _owned_load_journal(run, runtime, run_id, root)
                    for target, name in state["paths"].items():
                        _owned_check_time(end)
                        old = state["old"][target]
                        quarantine = f"old-{target}"
                        live = _owned_inventory(root, name) if name in os.listdir(root) else None
                        if quarantine not in os.listdir(run):
                            if live != old:
                                raise MaintenanceError("E_STORAGE")
                            continue
                        if target not in state["targets"] or _owned_inventory(run, quarantine) != old:
                            raise MaintenanceError("E_STORAGE")
                        state.update(phase="rollback", intent=target)
                        _owned_journal(run, state)
                        with _owned_dir("stage", parent=run) as stage:
                            if live is not None:
                                if live != state["staged"].get(target) or target in os.listdir(stage):
                                    raise MaintenanceError("E_STORAGE")
                                os.rename(name, target, src_dir_fd=root, dst_dir_fd=stage)
                                os.fsync(root)
                                os.fsync(stage)
                            elif target not in os.listdir(stage):
                                raise MaintenanceError("E_STORAGE")
                            os.rename(quarantine, name, src_dir_fd=run, dst_dir_fd=root)
                            os.fsync(run)
                            os.fsync(root)
                            if _owned_inventory(root, name) != old:
                                raise MaintenanceError("E_STORAGE")
                        state.update(intent=None)
                        _owned_journal(run, state)
                    state.update(phase="reconciled", intent=None)
                    _owned_journal(run, state)
    except MaintenanceError as exc:
        error = exc.code
    except Exception:
        error = "E_STORAGE"
    if error:
        raise MaintenanceError(error)
