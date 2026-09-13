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

import contextlib
import hashlib
import json
import os
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import AbstractSet, Any, Literal, Mapping, Sequence
from uuid import NAMESPACE_DNS, uuid5

import prometheus_client
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from memory_server.evaluation.metrics import metrics_registry
from memory_server.providers.graph_provider import SimpleGraph
from memory_server.router.graph_router import GraphRouter

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


_SHARED_NODE_ID_ROUTER = GraphRouter()


def _normalized_node_id(value: object) -> str:
    """The pinned A.3.2 node-id normalization (``GraphRouter._to_node_id``)."""
    return _SHARED_NODE_ID_ROUTER._to_node_id(str(value))


def _prefixed_node_id(prefix: str, value: object) -> str:
    return _SHARED_NODE_ID_ROUTER._to_node_id(f"{prefix}{value}")


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
