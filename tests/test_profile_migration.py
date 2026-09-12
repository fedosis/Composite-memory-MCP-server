"""Profile-migration dry-run/manifest guard tests.

IMPL slice S0 keeps these guard-level assertions only; the DETAIL 10.1-10.6
migration contracts are implemented and tested in a later slice. Every test
runs through the ``synthetic_storage_env`` fixture, so the deployment
environment is scrubbed and migration roots stay inside the pytest temporary
root.
"""
from __future__ import annotations

import ast
import errno
import hashlib
import inspect
import json
import os
import re
import sqlite3
import stat
import subprocess
import sys
import threading
import time
from dataclasses import asdict, replace
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Mapping, cast
from urllib.parse import quote
from uuid import NAMESPACE_DNS, uuid5

import pytest

import memory_server.profile_migration as profile_migration
import memory_server.projection_rebuild as projection_rebuild
from memory_server.profile_migration import (
    ArtifactIdentity,
    MigrationManifest,
    MigrationMode,
    MigrationRequest,
    append_manifest_event,
    apply_profile_migration,
    load_manifest,
    plan_profile_migration,
    resume_profile_migration,
    rollback_profile_migration,
)
from memory_server.projection_rebuild import CanonicalProjectionRecord


def _s301_records() -> list[CanonicalProjectionRecord]:
    return [
        CanonicalProjectionRecord("decision", "d1", "index_decision", {
            "choice": "Pick Caddy", "reason": "safe", "context": "Widget", "lifecycle_state": "active",
        }),
        CanonicalProjectionRecord("decision", "d2", "index_decision", {
            "choice": "pick caddy", "reason": "same", "context": "Widget", "lifecycle_state": "active",
        }),
        CanonicalProjectionRecord("fact", "f1", "index_fact", {
            "subject": "Widget", "predicate": "uses", "object": "Caddy", "source": "s", "lifecycle_state": "active",
        }),
    ]


def test_s301_positive_later_fact_context_has_one_decides_edge() -> None:
    graph = projection_rebuild.build_shared_projection_graph(_s301_records())
    edges = [
        edge
        for targets in graph._edges.values()
        for group in targets.values()
        for edge in group
        if edge.relation == "decides"
    ]
    assert len(edges) == 1
    assert (edges[0].source_id, edges[0].target_id, edges[0].relation) == (
        "decision-pick-caddy", "widget", "decides"
    )


def test_s301_negative_unmatched_context_creates_no_target_or_edge() -> None:
    records = [CanonicalProjectionRecord("decision", "d1", "index_decision", {
        "choice": "Pick Caddy", "reason": "safe", "context": "Missing", "lifecycle_state": "active",
    })]
    graph = projection_rebuild.build_shared_projection_graph(records)
    assert graph.get_node("missing") is None
    assert [
        edge
        for targets in graph._edges.values()
        for group in targets.values()
        for edge in group
        if edge.relation == "decides"
    ] == []


def test_s301_mapping_is_order_independent_for_ids_types_and_edge_digest() -> None:
    forward = projection_rebuild.build_shared_projection_graph(_s301_records())
    reverse = projection_rebuild.build_shared_projection_graph(list(reversed(_s301_records())))
    def node_keys(graph):
        return {(node.id, node.type) for node in graph.get_all_nodes()}

    def edge_keys(graph):
        return {
            (edge.source_id, edge.target_id, edge.relation)
            for targets in graph._edges.values()
            for group in targets.values()
            for edge in group
        }

    assert node_keys(forward) == node_keys(reverse)
    assert edge_keys(forward) == edge_keys(reverse)
    assert projection_rebuild.graph_id_digests(forward) == projection_rebuild.graph_id_digests(reverse)


def test_s301_exact_vector_mapping() -> None:
    mapped = projection_rebuild.map_projection_record(_s301_records()[-1])
    assert mapped.vector_text == "Widget uses Caddy"
    assert mapped.point_id == str(uuid5(NAMESPACE_DNS, "fact:f1"))
    assert mapped.payload == {
        "subject": "Widget", "predicate": "uses", "object": "Caddy",
        "source": "s", "memory_type": "fact",
    }


def test_s301_belief_mapping_has_vector_payload_and_no_graph_projection() -> None:
    record = CanonicalProjectionRecord("belief", "b1", "index_belief", {
        "proposition": "Sky is blue", "confidence": 0.8, "tags": ["color"],
        "source": "observation", "lifecycle_state": "active",
    })
    mapped = projection_rebuild.map_projection_record(record)
    assert mapped.vector_text == "Sky is blue"
    assert mapped.point_id == str(uuid5(NAMESPACE_DNS, "belief:b1"))
    assert set(mapped.payload) == {"proposition", "confidence", "tags", "source", "memory_type"}
    graph = projection_rebuild.build_shared_projection_graph([record])
    assert all(node.type != "belief" for node in graph.get_all_nodes())


def test_s301_skill_eligibility_requires_nonempty_string_steps() -> None:
    def skill(steps):
        return CanonicalProjectionRecord("skill", "s1", "index_skill", {
            "purpose": "Use the tool", "steps": steps, "lifecycle_state": "active",
        })
    assert not projection_rebuild.is_eligible(skill([]))
    assert not projection_rebuild.is_eligible(skill([" "]))
    assert not projection_rebuild.is_eligible(skill(["ok", 1]))
    assert projection_rebuild.is_eligible(skill(["ok"]))


def _seed_s301_snapshot(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.executescript("""
        CREATE TABLE beliefs (id TEXT PRIMARY KEY, proposition TEXT, confidence REAL,
            source TEXT, tags TEXT, lifecycle_state TEXT);
        CREATE TABLE decisions (id TEXT PRIMARY KEY, choice TEXT, reason TEXT,
            context TEXT, lifecycle_state TEXT);
        CREATE TABLE facts (id TEXT PRIMARY KEY, subject TEXT, predicate TEXT,
            object TEXT, source TEXT, lifecycle_state TEXT);
        CREATE TABLE skills (id TEXT PRIMARY KEY, purpose TEXT, steps TEXT,
            lifecycle_state TEXT);
        INSERT INTO beliefs VALUES ('b0', 'candidate belief', 0.5, 's', '[]', 'candidate');
        INSERT INTO beliefs VALUES ('b1', 'active belief', 0.8, 's', '["tag"]', 'active');
        INSERT INTO decisions VALUES ('d0', 'ignored', 'why', 'ctx', 'archived');
        INSERT INTO decisions VALUES ('d1', 'choose', 'why', 'ctx', 'active');
        INSERT INTO facts VALUES ('f0', 'A', 'is', 'B', 's', 'archived');
        INSERT INTO facts VALUES ('f1', 'A', 'is', 'B', 's', 'candidate');
        INSERT INTO skills VALUES ('s0', 'empty', '[]', 'active');
        INSERT INTO skills VALUES ('s1', 'usable', '["step"]', 'validated');
    """)
    connection.commit()
    connection.close()


@pytest.mark.asyncio
async def test_s301_iterator_is_ordered_bounded_and_read_only(tmp_path: Path) -> None:
    db_path = tmp_path / "snapshot.db"
    _seed_s301_snapshot(db_path)
    url = f"sqlite+aiosqlite:///{db_path}"
    records = [record async for record in projection_rebuild.iter_canonical_projection_records(url, batch_size=2)]
    assert [(record.record_type, record.record_id) for record in records] == [
        ("belief", "b1"), ("decision", "d1"), ("fact", "f1"), ("skill", "s1")
    ]
    with pytest.raises(ValueError, match="batch_size must be positive"):
        async for _ in projection_rebuild.iter_canonical_projection_records(url, batch_size=0):
            pass


@pytest.mark.asyncio
async def test_s301_iterator_uses_plain_ro_for_wal_and_preserves_committed_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "wal-snapshot.db"
    _seed_s301_snapshot(db_path)
    writer = sqlite3.connect(db_path)
    writer.execute("PRAGMA journal_mode=WAL")
    writer.execute("INSERT INTO facts VALUES ('f2', 'A', 'has', 'WAL', 's', 'active')")
    writer.commit()
    assert (db_path.parent / f"{db_path.name}-wal").lstat().st_size > 0

    captured: list[str] = []
    real_create = projection_rebuild.create_async_engine

    def capture_engine(url: str, **kwargs):
        captured.append(url)
        return real_create(url, **kwargs)

    monkeypatch.setattr(projection_rebuild, "create_async_engine", capture_engine)
    records = [record async for record in projection_rebuild.iter_canonical_projection_records(
        f"sqlite+aiosqlite:///{db_path}", batch_size=10
    )]
    assert [(record.record_type, record.record_id) for record in records] == [
        ("belief", "b1"), ("decision", "d1"), ("fact", "f1"), ("fact", "f2"), ("skill", "s1")
    ]
    assert len(captured) == 1
    assert "mode=ro" in captured[0]
    assert "immutable=1" not in captured[0]
    ro_engine = real_create(captured[0])
    try:
        async with ro_engine.connect() as conn:
            with pytest.raises(Exception, match="readonly|read-only"):
                await conn.run_sync(lambda sync: sync.exec_driver_sql("CREATE TABLE forbidden (id TEXT)"))
    finally:
        await ro_engine.dispose()
        writer.close()


@pytest.mark.asyncio
async def test_s301_iterator_query_url_is_rewritten_without_corrupting_query(tmp_path: Path) -> None:
    db_path = tmp_path / "query-snapshot.db"
    _seed_s301_snapshot(db_path)
    records = [record async for record in projection_rebuild.iter_canonical_projection_records(
        f"sqlite+aiosqlite:///{db_path}?timeout=5", batch_size=10
    )]
    assert [(record.record_type, record.record_id) for record in records] == [
        ("belief", "b1"), ("decision", "d1"), ("fact", "f1"), ("skill", "s1")
    ]


@pytest.mark.asyncio
async def test_s301_iterator_uses_immutable_fast_path_without_sidecars(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "clean-snapshot.db"
    _seed_s301_snapshot(db_path)
    captured: list[str] = []
    real_create = projection_rebuild.create_async_engine

    def capture_engine(url: str, **kwargs):
        captured.append(url)
        return real_create(url, **kwargs)

    monkeypatch.setattr(projection_rebuild, "create_async_engine", capture_engine)
    records = [record async for record in projection_rebuild.iter_canonical_projection_records(
        f"sqlite+aiosqlite:///{db_path}", batch_size=10
    )]
    assert len(records) == 4
    assert len(captured) == 1
    assert "mode=ro" in captured[0]
    assert "immutable=1" in captured[0]


def test_s301_rebuild_matches_real_graph_router_fact_path() -> None:
    record = _s301_records()[-1]
    rebuilt = projection_rebuild.build_shared_projection_graph([record])
    runtime_router = projection_rebuild.GraphRouter(graph=projection_rebuild.SimpleGraph())
    subject = cast(str, record.payload["subject"])
    predicate = cast(str, record.payload["predicate"])
    object_name = cast(str, record.payload["object"])
    runtime_router.sync_fact(subject, predicate, object_name)
    runtime = runtime_router.graph
    assert {(node.id, node.type) for node in rebuilt.get_all_nodes()} == {
        (node.id, node.type) for node in runtime.get_all_nodes()
    }
    assert {(edge.source_id, edge.target_id, edge.relation)
            for targets in rebuilt._edges.values() for group in targets.values() for edge in group} == {
        (edge.source_id, edge.target_id, edge.relation)
        for targets in runtime._edges.values() for group in targets.values() for edge in group
    }


def _seed_source_sql(home: Path) -> Path:
    data_dir = home / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    db_path = data_dir / "memory.db"
    connection = sqlite3.connect(db_path)
    connection.execute("create table facts(id text)")
    connection.commit()
    connection.close()
    return db_path


def test_dry_plan_is_read_only_and_reports_wal_blocker(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _seed_source_sql(home)
    (home / "data/memory.db-wal").write_bytes(b"wal")

    before = db_path.read_bytes()
    plan = plan_profile_migration(MigrationRequest(home, source_sql=db_path))
    assert db_path.read_bytes() == before
    assert any(d.code == "E_SQLITE_WAL_ACTIVE" for d in plan.blockers)


def test_apply_requires_exact_confirmation_and_attestation(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _seed_source_sql(home)
    plan = plan_profile_migration(MigrationRequest(home, source_sql=db_path))
    with pytest.raises(ValueError, match="E_CONFIRM_TARGET"):
        apply_profile_migration(plan)


def test_invalid_mode_is_rejected_without_creating_run(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")
    home = tmp_path / "home"
    db_path = _seed_source_sql(home)
    request = MigrationRequest(home, source_sql=db_path, mode=cast(MigrationMode, "bogus"))
    before = _tree_snapshot(home)
    with pytest.raises(ValueError, match="E_INVALID_MODE"):
        plan_profile_migration(request)
    assert _tree_snapshot(home) == before


def _tree_snapshot(root: Path) -> tuple[tuple[str, int, bytes | None], ...]:
    entries = []
    for path in sorted(root.rglob("*")):
        relative = str(path.relative_to(root))
        if path.is_file():
            entries.append((relative, path.stat().st_mode, path.read_bytes()))
        else:
            entries.append((relative, path.stat().st_mode, None))
    return tuple(entries)


def _write_manifest_for_entrypoint(path: Path, plan) -> None:
    manifest = MigrationManifest(
        schema_version=1,
        run_id=plan.request.run_id,
        strategy=plan.request.strategy,
        checkpoint="locked",
        status="running",
        source_identity=plan.source_sql,
        target_identities_before=plan.targets,
        config_digest=plan.config_digest,
        runtime_stop_attestation={"value": plan.request.stop_attestation},
        artifacts={},
        embedding=asdict(plan.embedding),
    )
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(asdict(manifest), sort_keys=True, default=str))


def test_apply_requires_apply_mode_without_mutating_tree(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _seed_source_sql(home)
    request = MigrationRequest(home, source_sql=db_path, confirm_target=str(home), stop_attestation="ticket")
    plan = plan_profile_migration(request)
    before = _tree_snapshot(home)
    with pytest.raises(ValueError, match="E_APPLY_MODE_REQUIRED"):
        apply_profile_migration(plan)
    assert _tree_snapshot(home) == before


def test_apply_rejects_stale_embedding_plan_without_run(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")
    home = tmp_path / "home"
    db_path = _seed_source_sql(home)
    request = MigrationRequest(
        home,
        source_sql=db_path,
        mode="apply",
        confirm_target=str(home),
        stop_attestation="ticket",
        embedding_plan_digest="stale",
    )
    plan = plan_profile_migration(request)
    before = _tree_snapshot(home)
    with pytest.raises(ValueError, match="E_EMBEDDING_PLAN_STALE"):
        apply_profile_migration(plan)
    assert _tree_snapshot(home) == before


def test_resume_refuses_unfinished_migration(tmp_path: Path, synthetic_storage_env) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")
    home = tmp_path / "home"
    db_path = _seed_source_sql(home)
    request = MigrationRequest(
        home, source_sql=db_path, mode="resume", confirm_target=str(home), stop_attestation="ticket"
    )
    plan = plan_profile_migration(request)
    manifest_path = home / ".cmms-migrations" / request.run_id / "manifest.json"
    _write_manifest_for_entrypoint(manifest_path, plan)
    before = _tree_snapshot(home)
    with pytest.raises(ValueError, match="E_MIGRATION_NOT_IMPLEMENTED"):
        resume_profile_migration(manifest_path, request)
    assert _tree_snapshot(home) == before


def test_rollback_refuses_unfinished_migration_without_mutating_manifest(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")
    home = tmp_path / "home"
    db_path = _seed_source_sql(home)
    request = MigrationRequest(
        home, source_sql=db_path, mode="rollback", confirm_target=str(home), stop_attestation="ticket"
    )
    plan = plan_profile_migration(request)
    manifest_path = home / ".cmms-migrations" / request.run_id / "manifest.json"
    _write_manifest_for_entrypoint(manifest_path, plan)
    before = _tree_snapshot(home)
    # S2-08 moved this boundary and this leg is updated for it, not weakened: at this
    # slice rollback is an ACTION entrypoint that reads the run's own durable records,
    # so a run whose report records no original artifact identity is refused with THIS
    # card's own Stop instead of the slice-level "not implemented". What this node
    # exists for -- rollback refuses AND leaves the tree byte-identical -- is
    # unchanged and still asserted below on a real tree.
    with pytest.raises(ValueError, match="E_ROLLBACK_IDENTITY_MISSING"):
        rollback_profile_migration(manifest_path, request)
    assert _tree_snapshot(home) == before


# ---------------------------------------------------------------------------
# S2-01 -- bounded manifest schema, hash-chain events, durable atomic persistence
# ---------------------------------------------------------------------------

_S2_RUN_ID = "0f" * 16
_S2_ESCAPE = "E_MANIFEST_PATH_ESCAPE"
_S2_FORBIDDEN = "E_FORBIDDEN_TARGET_ROOT"


def _record_field(record: Any, name: str) -> Any:
    """Read one field of a manifest sub-record.

    The pre-fix code materializes nested records as plain mappings; the S2-01
    code materializes them as typed records. The S2-01 regression assertions
    must address both shapes so the filed RED can be produced on BASE.
    """
    if isinstance(record, Mapping):
        return record.get(name)
    return getattr(record, name)


def _s2_manifest(**overrides: Any) -> MigrationManifest:
    values: dict[str, Any] = {
        "schema_version": 1,
        "run_id": _S2_RUN_ID,
        "strategy": "rebuild-from-profile-sql",
        "checkpoint": "locked",
        "status": "running",
        "source_identity": ArtifactIdentity("/synthetic/home/data/memory.db", "regular_file"),
        "target_identities_before": {},
        "config_digest": "ab" * 32,
        "runtime_stop_attestation": {"value": "ticket"},
        "artifacts": {},
        "completed_steps": ["planned"],
        "events": [],
        "embedding": {},
        "failure": None,
    }
    values.update(overrides)
    return MigrationManifest(**values)


def _s2_run_dir(tmp_path: Path) -> Path:
    run_dir = Path(tmp_path) / "runs" / _S2_RUN_ID
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def _s2_write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, sort_keys=True, default=str), encoding="utf-8")


def _s2_pair(tmp_path: Path) -> tuple[Path, bytes]:
    """Return (live manifest path, its byte snapshot) written through the module."""
    run_dir = _s2_run_dir(tmp_path)
    live = run_dir / "manifest.json"
    profile_migration._write_manifest(live, _s2_manifest())
    return live, live.read_bytes()


def test_s2_load_manifest_rejects_unknown_schema_version(tmp_path: Path) -> None:
    live, before = _s2_pair(tmp_path)
    payload = asdict(_s2_manifest())
    payload["schema_version"] = 99
    bad = live.parent / "bad-version.json"
    _s2_write_json(bad, payload)

    with pytest.raises(ValueError, match="E_MANIFEST_VERSION"):
        load_manifest(bad)
    assert live.read_bytes() == before


@pytest.mark.parametrize(
    "run_id",
    ["../../escape", "/absolute/run", "run/with/slash", "not-hex-32", "a" * 200],
)
def test_s2_load_manifest_rejects_unbounded_or_traversing_run_id(tmp_path: Path, run_id: str) -> None:
    run_dir = _s2_run_dir(tmp_path)
    bad = run_dir / "bad-runid.json"
    payload = asdict(_s2_manifest(run_id=run_id))
    _s2_write_json(bad, payload)

    with pytest.raises(ValueError, match="E_MANIFEST_(PATH_ESCAPE|SCHEMA)"):
        load_manifest(bad)


@pytest.mark.parametrize(
    ("relative_path", "code"),
    [
        ("/etc/passwd", _S2_ESCAPE),
        ("../../live/memory.db", _S2_ESCAPE),
        ("~/.hermes/data/memory.db", _S2_ESCAPE),
        ("/", _S2_FORBIDDEN),
        (".", _S2_FORBIDDEN),
    ],
)
def test_s2_load_manifest_rejects_run_path_escape(tmp_path: Path, relative_path: str, code: str) -> None:
    live, before = _s2_pair(tmp_path)
    payload = asdict(_s2_manifest())
    payload["artifacts"] = {"vector": {"relative_path": relative_path, "kind": "directory"}}
    bad = live.parent / "escape.json"
    _s2_write_json(bad, payload)

    with pytest.raises(ValueError, match=code):
        load_manifest(bad)
    assert live.read_bytes() == before


def test_s2_load_manifest_rejects_oversized_manifest(tmp_path: Path) -> None:
    live, before = _s2_pair(tmp_path)
    payload = asdict(_s2_manifest())
    payload["artifacts"] = {
        "filler": {"relative_path": "filler", "kind": "regular_file", "note": "x" * (2 * 1024 * 1024)}
    }
    bad = live.parent / "oversized.json"
    _s2_write_json(bad, payload)
    assert bad.stat().st_size > 1024 * 1024

    with pytest.raises(ValueError, match="E_MANIFEST_SCHEMA"):
        load_manifest(bad)
    assert live.read_bytes() == before


def test_s2_load_manifest_rejects_tampered_event_chain(tmp_path: Path) -> None:
    live, _ = _s2_pair(tmp_path)
    append_manifest_event(live, {"operation": "locked", "checkpoint": "locked", "payload": {"step": 1}})
    before = live.read_bytes()

    mutated_payload = json.loads(live.read_text())
    mutated_payload["events"][0]["payload"] = {"step": 999}
    tampered = live.parent / "tampered-payload.json"
    _s2_write_json(tampered, mutated_payload)

    mutated_sequence = json.loads(live.read_text())
    mutated_sequence["events"][0]["sequence"] = 7
    out_of_order = live.parent / "tampered-sequence.json"
    _s2_write_json(out_of_order, mutated_sequence)

    for candidate in (tampered, out_of_order):
        with pytest.raises(ValueError, match="E_MANIFEST_TAMPERED"):
            load_manifest(candidate)
    assert live.read_bytes() == before


def test_s2_manifest_round_trip_preserves_typed_state(tmp_path: Path) -> None:
    run_dir = _s2_run_dir(tmp_path)
    live = run_dir / "manifest.json"
    written = _s2_manifest(completed_steps=["planned", "locked"])
    profile_migration._write_manifest(live, written)

    loaded = load_manifest(live)
    assert loaded.schema_version == 1
    assert loaded.run_id == _S2_RUN_ID
    assert loaded.checkpoint == "locked"
    assert loaded.status == "running"
    assert loaded.config_digest == "ab" * 32
    assert loaded.completed_steps == ["planned", "locked"]
    assert loaded.source_identity.lexical_path == "/synthetic/home/data/memory.db"
    assert loaded.events == []
    assert live.read_bytes() == live.read_bytes()


def test_s2_load_manifest_rejects_unknown_or_missing_top_level_keys(tmp_path: Path) -> None:
    live, before = _s2_pair(tmp_path)
    payload = asdict(_s2_manifest())
    payload["injected_field"] = "extra"
    unknown = live.parent / "unknown-key.json"
    _s2_write_json(unknown, payload)

    missing = json.loads(live.read_text())
    missing.pop("config_digest")
    absent = live.parent / "missing-key.json"
    _s2_write_json(absent, missing)

    for candidate in (unknown, absent):
        with pytest.raises(ValueError, match="E_MANIFEST_SCHEMA"):
            load_manifest(candidate)
    assert live.read_bytes() == before


def test_s2_append_manifest_event_keeps_append_only_hash_chain(tmp_path: Path) -> None:
    run_dir = _s2_run_dir(tmp_path)
    live = run_dir / "manifest.json"
    profile_migration._write_manifest(live, _s2_manifest())

    first = append_manifest_event(
        live, {"operation": "locked", "checkpoint": "locked", "payload": {"step": 1}}
    )
    first_event = _record_field(first.events[0], "digest")
    first_snapshot = (first_event, _record_field(first.events[0], "operation"))
    second = append_manifest_event(live, {"operation": "backed_up", "checkpoint": "backed_up"})

    assert len(first.events) == 1
    assert len(second.events) == 2
    assert _record_field(second.events[0], "sequence") == 1
    assert _record_field(second.events[0], "prev_sha256") == "0" * 64
    assert _record_field(second.events[1], "sequence") == 2
    assert _record_field(second.events[1], "prev_sha256") == first_event
    assert re.fullmatch(r"[0-9a-f]{64}", str(first_event))
    assert _record_field(second.events[1], "digest") != first_event
    assert (
        _record_field(second.events[0], "digest"),
        _record_field(second.events[0], "operation"),
    ) == first_snapshot
    assert _record_field(second.events[1], "checkpoint") == "backed_up"

    reloaded = load_manifest(live)
    assert [_record_field(event, "sequence") for event in reloaded.events] == [1, 2]
    assert [_record_field(event, "prev_sha256") for event in reloaded.events] == [
        "0" * 64,
        first_event,
    ]


def test_s2_write_manifest_flushes_fsyncs_replaces_then_fsyncs_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_dir = _s2_run_dir(tmp_path)
    live = run_dir / "manifest.json"
    profile_migration._write_manifest(live, _s2_manifest(completed_steps=["planned"]))
    original = live.read_bytes()

    observations: dict[str, Any] = {"kinds": []}
    real_fsync = os.fsync

    def tracing_fsync(fd: int) -> None:
        info = os.fstat(fd)
        if stat.S_ISDIR(info.st_mode):
            observations["kinds"].append("dir")
            observations["dir_fd_path"] = os.readlink(f"/proc/self/fd/{fd}")
            observations["target_at_dir_fsync"] = live.read_bytes()
        elif stat.S_ISREG(info.st_mode):
            observations["kinds"].append("file")
            temp_path = os.readlink(f"/proc/self/fd/{fd}")
            observations["flushed_temp_bytes"] = Path(temp_path).read_bytes()
            observations["target_at_file_fsync"] = live.read_bytes()
        real_fsync(fd)

    monkeypatch.setattr(os, "fsync", tracing_fsync)
    profile_migration._write_manifest(live, _s2_manifest(completed_steps=["planned", "locked"]))
    monkeypatch.undo()

    assert observations["kinds"] == ["file", "dir"]
    assert observations["dir_fd_path"] == str(run_dir)
    assert observations["target_at_file_fsync"] == original
    assert observations["flushed_temp_bytes"] == live.read_bytes()
    assert observations["target_at_dir_fsync"] == live.read_bytes()
    assert load_manifest(live).completed_steps == ["planned", "locked"]


def test_s2_write_manifest_enforces_0700_run_directory_and_0600_manifest(tmp_path: Path) -> None:
    run_dir = _s2_run_dir(tmp_path)
    os.chmod(run_dir, 0o755)
    live = run_dir / "manifest.json"
    profile_migration._write_manifest(live, _s2_manifest())

    assert stat.S_IMODE(run_dir.stat().st_mode) == 0o700
    assert stat.S_IMODE(live.stat().st_mode) == 0o600

    fresh = Path(tmp_path) / "fresh" / _S2_RUN_ID
    profile_migration._write_manifest(fresh / "manifest.json", _s2_manifest())
    assert stat.S_IMODE(fresh.stat().st_mode) == 0o700


def test_s2_write_manifest_refuses_run_directory_identity_mismatch(tmp_path: Path) -> None:
    mismatched = Path(tmp_path) / "runs" / ("11" * 16)
    mismatched.mkdir(parents=True)

    with pytest.raises(ValueError, match="E_MANIFEST_PATH_ESCAPE"):
        profile_migration._write_manifest(mismatched / "manifest.json", _s2_manifest())


def test_s2_write_manifest_refuses_temp_symlink_and_temp_collision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_dir = _s2_run_dir(tmp_path)
    live = run_dir / "manifest.json"
    victim = run_dir / "victim.bin"
    victim.write_bytes(b"ORIGINAL")
    legacy_temp = run_dir / "manifest.tmp"
    legacy_temp.symlink_to(victim)
    monkeypatch.setattr(
        profile_migration,
        "_temp_manifest_name",
        lambda target: Path(target).parent / "manifest.tmp",
        raising=False,
    )

    with pytest.raises(ValueError, match="E_MANIFEST_TAMPERED"):
        profile_migration._write_manifest(live, _s2_manifest())
    assert victim.read_bytes() == b"ORIGINAL"
    assert not live.exists()

    legacy_temp.unlink()
    legacy_temp.write_bytes(b"stale-temp")
    with pytest.raises(ValueError, match="E_MANIFEST_TAMPERED"):
        profile_migration._write_manifest(live, _s2_manifest())
    assert legacy_temp.read_bytes() == b"stale-temp"


def test_s2_persist_failure_before_fsync_preserves_previous_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_dir = _s2_run_dir(tmp_path)
    live = run_dir / "manifest.json"
    profile_migration._write_manifest(live, _s2_manifest(completed_steps=["planned"]))
    before = live.read_bytes()

    real_fsync = os.fsync
    state = {"calls": 0}

    def faulting_fsync(fd: int) -> None:
        state["calls"] += 1
        # Real filesystem fault: the descriptor is closed before the real fsync,
        # so the KERNEL returns EBADF at the fsync syscall itself. Nothing is
        # fabricated; the production call site is exercised unchanged.
        os.close(fd)
        real_fsync(fd)

    monkeypatch.setattr(os, "fsync", faulting_fsync)
    try:
        with pytest.raises(OSError) as failure:
            profile_migration._write_manifest(live, _s2_manifest(completed_steps=["planned", "locked"]))
    finally:
        monkeypatch.undo()

    assert state["calls"] == 1
    assert failure.value.errno == errno.EBADF
    assert live.read_bytes() == before
    assert load_manifest(live).completed_steps == ["planned"]


def test_s2_persist_failure_after_fsync_before_replace_preserves_previous_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_dir = _s2_run_dir(tmp_path)
    live = run_dir / "manifest.json"
    profile_migration._write_manifest(live, _s2_manifest(completed_steps=["planned"]))
    before = live.read_bytes()

    real_fsync = os.fsync
    state = {"file_fsyncs": 0}

    def faulting_fsync(fd: int) -> None:
        real_fsync(fd)
        if stat.S_ISREG(os.fstat(fd).st_mode):
            state["file_fsyncs"] += 1
            # Fault strictly between the real file fsync and os.replace: drop
            # the run directory's write permission so the kernel refuses the
            # rename with EACCES.
            os.chmod(run_dir, 0o500)

    monkeypatch.setattr(os, "fsync", faulting_fsync)
    try:
        for _ in range(2):
            with pytest.raises(OSError):
                profile_migration._write_manifest(live, _s2_manifest(completed_steps=["planned", "locked"]))
    finally:
        monkeypatch.undo()
        os.chmod(run_dir, 0o700)

    assert state["file_fsyncs"] == 2
    assert live.read_bytes() == before
    assert load_manifest(live).completed_steps == ["planned"]

    residue = sorted(path for path in run_dir.iterdir() if path.name != "manifest.json")
    assert len(residue) == 2
    assert len({path.name for path in residue}) == 2
    for path in residue:
        assert path.is_file() and not path.is_symlink()
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        # The temp was fully written and flushed before the injected fault.
        assert json.loads(path.read_text())["completed_steps"] == ["planned", "locked"]


def test_s2_persist_failure_after_replace_keeps_the_new_manifest_complete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_dir = _s2_run_dir(tmp_path)
    live = run_dir / "manifest.json"
    profile_migration._write_manifest(live, _s2_manifest(completed_steps=["planned"]))
    before = live.read_bytes()

    real_fsync = os.fsync
    state = {"file": 0, "dir": 0}

    def faulting_fsync(fd: int) -> None:
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            state["dir"] += 1
            # Real kernel fault at the directory fsync, i.e. strictly AFTER the
            # replace: the descriptor is closed, so the kernel answers EBADF.
            os.close(fd)
            real_fsync(fd)
            return
        state["file"] += 1
        real_fsync(fd)

    monkeypatch.setattr(os, "fsync", faulting_fsync)
    try:
        with pytest.raises(OSError) as failure:
            profile_migration._write_manifest(live, _s2_manifest(completed_steps=["planned", "locked"]))
    finally:
        monkeypatch.undo()

    assert state == {"file": 1, "dir": 1}
    assert failure.value.errno == errno.EBADF
    # Atomicity after a completed replace: the destination holds one complete
    # new manifest, never a torn or partial document, and no temp residue.
    assert live.read_bytes() != before
    assert load_manifest(live).completed_steps == ["planned", "locked"]
    assert [path.name for path in run_dir.iterdir() if path.name != "manifest.json"] == []


def test_s2_config_digest_is_nonsecret_storage_identity(tmp_path: Path, synthetic_storage_env) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")
    home = tmp_path / "home"
    db_path = _seed_source_sql(home)

    plan_a = plan_profile_migration(
        MigrationRequest(home, source_sql=db_path, confirm_target="SECRET-A", stop_attestation="SECRET-A")
    )
    plan_b = plan_profile_migration(
        MigrationRequest(home, source_sql=db_path, confirm_target="SECRET-B", stop_attestation="SECRET-B")
    )

    other_home = tmp_path / "other-home"
    other_db = _seed_source_sql(other_home)
    plan_c = plan_profile_migration(
        MigrationRequest(
            other_home, source_sql=other_db, confirm_target="SECRET-A", stop_attestation="SECRET-A"
        )
    )

    assert re.fullmatch(r"[0-9a-f]{64}", plan_a.config_digest)
    assert plan_a.request.run_id != plan_b.request.run_id
    assert plan_a.config_digest == plan_b.config_digest
    assert "SECRET" not in plan_a.config_digest
    assert hashlib.sha256(b"SECRET-A").hexdigest() != plan_a.config_digest
    # Everything above is satisfied by a degenerate constant digest; a different
    # non-secret storage identity must produce a different digest.
    assert re.fullmatch(r"[0-9a-f]{64}", plan_c.config_digest)
    assert plan_c.config_digest != plan_a.config_digest


def test_s2_load_manifest_refuses_non_regular_manifest_path_without_blocking(
    tmp_path: Path, synthetic_storage_env
) -> None:
    """A non-regular manifest path is refused instead of blocking the reader.

    ``os.open(O_RDONLY)`` on a FIFO with no writer blocks until a writer opens
    it, so the ``S_ISREG`` refusal in ``_read_bounded_manifest_bytes`` is only
    reachable when that open is non-blocking (``O_NONBLOCK``, a no-op for
    regular files). The read therefore runs in a child process under a hard
    ``subprocess`` timeout: a regression that reintroduces the blocking open
    makes this node FAIL on the timeout instead of hanging the suite.
    """
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    run_dir = _s2_run_dir(tmp_path)
    fifo = run_dir / "manifest.json"
    os.mkfifo(fifo, 0o600)
    assert stat.S_ISFIFO(os.lstat(fifo).st_mode)

    # The child reads the module under test from the same source tree, so the
    # node cannot silently exercise a stale installed copy.
    module_path = Path(profile_migration.__file__).resolve()
    tree_root = module_path.parents[2]
    child_env = dict(os.environ)
    child_env["PYTHONPATH"] = os.pathsep.join((str(tree_root / "src"), str(tree_root)))
    child_env["PYTHONDONTWRITEBYTECODE"] = "1"
    child_source = (
        "import sys\n"
        "from memory_server.profile_migration import load_manifest\n"
        "try:\n"
        "    load_manifest(sys.argv[1])\n"
        "except ValueError as exc:\n"
        "    print('REFUSED:' + str(exc))\n"
        "    raise SystemExit(0)\n"
        "print('LOADED')\n"
        "raise SystemExit(3)\n"
    )

    try:
        child = subprocess.run(
            [sys.executable, "-c", child_source, str(fifo)],
            capture_output=True,
            text=True,
            timeout=15,
            env=child_env,
            check=False,
        )
    except subprocess.TimeoutExpired:
        pytest.fail(
            "load_manifest blocked on a FIFO manifest path: no refusal within 15s "
            "(the read-side open must be non-blocking)"
        )

    assert child.returncode == 0, f"stdout={child.stdout!r} stderr={child.stderr!r}"
    assert child.stdout.startswith("REFUSED:"), child.stdout
    assert "E_MANIFEST_TAMPERED" in child.stdout, child.stdout
    # The refusal came from the regular-file check, not from anything having
    # opened the FIFO for writing.
    assert stat.S_ISFIFO(os.lstat(fifo).st_mode)


# ---------------------------------------------------------------------------
# S2-02 -- mutation-free no-follow inventory and raw/effective config report
#
# Every node below addresses a behaviour the pre-fix planner does not have:
# configured data root selection, RAW-YAML canonical SQL selection with the
# environment reported separately, ``sql_action``, legacy candidates with their
# exact RAW link string, disk margin 1.25, the WAL/SHM/journal presence matrix
# with stable codes, the sidecar-gated immutable read-only probe, streaming
# identity digests (never the bounded 64 KiB reader), and the no-follow
# run-directory path chain. Accessors are shape-tolerant so that the filed RED
# on the parent commit is a behavioural assertion failure and never a
# collection ``ImportError``.
# ---------------------------------------------------------------------------

_S202_CONFIG_BLOCK: dict[str, Any] = {
    "storage_mode": "profile",
    "data_root": ".",
    "db_url": "sqlite+aiosqlite:///data/memory.db",
    "vector_backend": "lancedb",
    "lancedb_path": "data/lancedb",
    "graph_snapshot_path": "data/graph.json",
}


def _s202_request(profile_home: Path, **fields: Any) -> MigrationRequest:
    """Request restricted to the fields THIS checkout understands.

    The pre-fix dataclass has no ``raw_config``/``raw_config_path`` field.
    Dropping the unknown field keeps the module importable, so the filed RED is
    a behavioural assertion failure, never a collection error.
    """
    import dataclasses as _dataclasses

    accepted = {item.name for item in _dataclasses.fields(MigrationRequest)}
    known = {key: value for key, value in fields.items() if key in accepted}
    return MigrationRequest(profile_home, **known)


def _s202_report(plan: Any) -> dict[str, Any]:
    """The dry-run report mapping, or an empty mapping on the pre-fix planner."""
    report = getattr(plan, "report", None)
    return dict(report) if isinstance(report, Mapping) else {}


def _s202_section(plan: Any, *path: str) -> Any:
    """Nested report lookup that yields ``{}`` for anything the plan lacks.

    Shape-tolerant on purpose: on the pre-fix planner every lookup resolves to
    ``{}`` so the filed RED is an assertion failure, never a KeyError/ImportError.
    """
    current: Any = _s202_report(plan)
    for key in path:
        if not isinstance(current, Mapping) or key not in current:
            return {}
        current = current[key]
    return current


def _s202_codes(plan: Any) -> list[str]:
    return [item.code for item in plan.blockers]


def _s202_seed_db_at(path: Path) -> Path:
    """Create a real sidecar-free SQLite database at exactly ``path``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.execute("create table facts(id text)")
    connection.execute("insert into facts values('one')")
    connection.commit()
    connection.close()
    return path


def _s202_seed_sized_db(path: Path, minimum_bytes: int) -> Path:
    """A valid SQLite database comfortably larger than ``minimum_bytes``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.execute("create table facts(id text, body blob)")
    connection.execute("insert into facts values('one', zeroblob(?))", (max(minimum_bytes - 4096, 0),))
    connection.commit()
    connection.close()
    assert path.stat().st_size > minimum_bytes
    return path


def _s202_external_link(tmp_path: Path, home: Path, name: str = "lancedb") -> Path:
    """Plant a final symlink where a profile-local projection is expected."""
    external = tmp_path / f"external-{name}"
    external.mkdir()
    (external / "victim.bin").write_bytes(b"VICTIM")
    (home / "data").mkdir(parents=True, exist_ok=True)
    (home / "data" / name).symlink_to(external, target_is_directory=True)
    return external


def test_s202_configured_data_root_selects_the_canonical_root(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    configured = home / "profile-data"
    db_path = _s202_seed_db_at(configured / "data" / "memory.db")

    plan = plan_profile_migration(
        _s202_request(home, configured_data_root=str(configured), raw_config=_S202_CONFIG_BLOCK)
    )

    assert plan.layout.data_root == configured
    assert plan.layout.profile_home == home
    # The canonical SQL is selected under the configured root, not under the
    # profile home the pre-fix planner hardcoded.
    assert plan.source_sql.lexical_path == str(configured / "data" / "memory.db")
    assert plan.source_sql.sha256 is not None
    assert "E_SOURCE_SQL_REQUIRED" not in _s202_codes(plan)
    assert db_path.read_bytes() == db_path.read_bytes()


def test_s202_raw_yaml_selects_canonical_sql_and_env_is_reported_separately(
    tmp_path: Path, synthetic_storage_env, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    canonical = _s202_seed_db_at(home / "custom" / "canonical.db")
    monkeypatch.setenv("MEMORY_SERVER_DB_URL", "sqlite+aiosqlite:///env/other.db?token=SECRET")
    raw = dict(_S202_CONFIG_BLOCK, db_url="sqlite+aiosqlite:///custom/canonical.db")

    plan = plan_profile_migration(_s202_request(home, raw_config=raw))

    # The RAW YAML layer selects the canonical SQL; env never re-selects it.
    assert plan.source_sql.lexical_path == str(canonical)
    report = _s202_report(plan)
    assert report.get("source_sql_origin") == "raw_config"
    config = report.get("config", {})
    assert config.get("raw", {}).get("db_url") == "sqlite+aiosqlite:///custom/canonical.db"
    assert str(config.get("env", {}).get("db_url", "")).startswith("sqlite+aiosqlite:///env/other.db")
    assert config.get("divergence") == ["db_url"]
    assert config.get("settings_consulted") is False
    # No unredacted sensitive URI value is emitted.
    assert "SECRET" not in json.dumps(report, sort_keys=True)


def test_s202_sql_action_is_preserve_in_place(tmp_path: Path, synthetic_storage_env) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _s202_seed_db_at(home / "data" / "memory.db")

    plan = plan_profile_migration(_s202_request(home, source_sql=db_path))

    assert getattr(plan, "sql_action", None) == "preserve_in_place"
    assert _s202_report(plan).get("sql_action") == "preserve_in_place"
    operations = [(item.operation, item.artifact, item.path) for item in plan.planned_operations]
    assert ("snapshot", "sqlite", str(db_path)) in operations
    # SQLite is never a projection publication target in this strategy.
    assert all(not (operation == "publish" and artifact == "sqlite") for operation, artifact, _ in operations)


def test_s202_legacy_projection_link_is_inventory_only_with_raw_link_string(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _s202_seed_db_at(home / "data" / "memory.db")
    external = _s202_external_link(tmp_path, home)
    before = _tree_snapshot(external)

    plan = plan_profile_migration(_s202_request(home, source_sql=db_path, raw_config=_S202_CONFIG_BLOCK))

    legacy = list(plan.legacy_projections)
    assert len(legacy) == 1
    assert legacy[0].kind == "symlink"
    # The EXACT raw link string, never a resolved referent.
    assert legacy[0].raw_link_target == str(external)
    assert legacy[0].sha256 is None
    dispositions = _s202_report(plan).get("legacy_projections", [])
    assert [item.get("disposition") for item in dispositions] == ["preserve-only; not imported"]
    assert [item.get("raw_link_target") for item in dispositions] == [str(external)]
    # The referent tree was inventoried, never traversed, never imported.
    assert _tree_snapshot(external) == before
    assert "E_LEGACY_PROJECTION_IMPORT_FORBIDDEN" not in _s202_codes(plan)


def test_s202_disk_margin_is_reported_and_unknown_space_blocks(
    tmp_path: Path, synthetic_storage_env, monkeypatch: pytest.MonkeyPatch
) -> None:
    import shutil as _shutil

    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _s202_seed_sized_db(home / "data" / "memory.db", 200_000)

    plan = plan_profile_migration(_s202_request(home, source_sql=db_path))
    assert plan.required_bytes is not None and plan.required_bytes >= db_path.stat().st_size
    assert plan.available_bytes is not None and plan.available_bytes > 0
    assert _s202_section(plan, "disk", "margin_ratio") == 1.25
    assert _s202_section(plan, "disk", "within_margin") is True
    assert "E_INSUFFICIENT_SPACE" not in _s202_codes(plan)

    tight = type("_Usage", (), {"free": 1, "used": 0, "total": 1})()
    monkeypatch.setattr(_shutil, "disk_usage", lambda path: tight)
    squeezed = plan_profile_migration(_s202_request(home, source_sql=db_path))
    assert "E_INSUFFICIENT_SPACE" in _s202_codes(squeezed)
    assert _s202_section(squeezed, "disk", "within_margin") is False
    monkeypatch.undo()

    def _unprovable(path: str) -> Any:
        raise OSError(errno.EACCES, "free space unprovable")

    monkeypatch.setattr(_shutil, "disk_usage", _unprovable)
    unknown = plan_profile_migration(_s202_request(home, source_sql=db_path))
    monkeypatch.undo()
    # Unknown free space stays a blocker, never a guess.
    assert unknown.available_bytes is None
    assert "E_INSUFFICIENT_SPACE" in _s202_codes(unknown)


def test_s202_sidecar_presence_matrix_uses_stable_codes(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _s202_seed_db_at(home / "data" / "memory.db")
    # Zero-length WAL and a MISSING SHM: presence is what blocks, not content.
    (home / "data" / "memory.db-wal").write_bytes(b"")

    wal_plan = plan_profile_migration(_s202_request(home, source_sql=db_path))
    codes = _s202_codes(wal_plan)
    assert "E_SQLITE_WAL_ACTIVE" in codes
    assert "E_SQLITE_SHM_AMBIGUOUS" in codes
    sidecars = _s202_section(wal_plan, "sidecars")
    assert sidecars == {
        "wal_present": True,
        "wal_size": 0,
        "shm_present": False,
        "journal_present": False,
    }
    # A present sidecar closes the SQLite probe entirely.
    assert _s202_section(wal_plan, "sqlite", "open_policy") == "sidecars_present_no_open"

    # A rollback journal has its own stable code (never the SHM code).
    (home / "data" / "memory.db-journal").write_bytes(b"")
    journal_plan = plan_profile_migration(_s202_request(home, source_sql=db_path))
    assert "E_SQLITE_HOT_JOURNAL" in _s202_codes(journal_plan)
    assert _s202_section(journal_plan, "sidecars", "journal_present") is True


def test_s202_sidecar_presence_performs_no_sqlite_open_at_all(
    tmp_path: Path, synthetic_storage_env, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _s202_seed_db_at(home / "data" / "memory.db")
    (home / "data" / "memory.db-shm").write_bytes(b"")

    opened: list[Any] = []
    real_connect = sqlite3.connect

    def _counting_connect(*args: Any, **kwargs: Any) -> Any:
        opened.append(args)
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", _counting_connect)
    before = _tree_snapshot(home)
    plan = plan_profile_migration(_s202_request(home, source_sql=db_path))
    monkeypatch.undo()

    assert opened == []
    assert _s202_section(plan, "sqlite", "opened") is False
    assert _s202_section(plan, "sqlite", "open_policy") == "sidecars_present_no_open"
    assert _s202_section(plan, "sqlite", "schema") == "unknown"
    assert _s202_section(plan, "sqlite", "integrity") == "unknown"
    assert _s202_section(plan, "sqlite", "uri") is None
    assert _tree_snapshot(home) == before


def test_s202_sidecar_free_source_runs_encoded_immutable_readonly_probe(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    # A space in the path proves the URI is percent-encoded.
    db_path = _s202_seed_db_at(home / "data" / "my memory.db")
    before = _tree_snapshot(home)

    plan = plan_profile_migration(_s202_request(home, source_sql=db_path))

    assert _s202_section(plan, "sqlite", "opened") is True
    assert _s202_section(plan, "sqlite", "open_policy") == "sidecars_absent_immutable_ro"
    assert _s202_section(plan, "sqlite", "schema") == "known"
    assert _s202_section(plan, "sqlite", "integrity") == "ok"
    assert "facts" in tuple(_s202_section(plan, "sqlite", "tables"))
    assert _s202_section(plan, "sqlite", "counts", "facts") == 1
    uri = str(_s202_section(plan, "sqlite", "uri"))
    assert uri.startswith("file:")
    assert uri.endswith("?mode=ro&immutable=1")
    assert "my%20memory.db" in uri
    assert " " not in uri
    assert _s202_section(plan, "invariance", "artifacts") == "ok"
    # The probe is side-effect free: no bytes, no entries changed.
    assert _tree_snapshot(home) == before


def test_s202_probe_refuses_when_a_sibling_entry_appears_mid_probe(
    tmp_path: Path, synthetic_storage_env, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _s202_seed_db_at(home / "data" / "memory.db")
    real_connect = sqlite3.connect
    state = {"injected": False}

    def _injecting_connect(*args: Any, **kwargs: Any) -> Any:
        connection = real_connect(*args, **kwargs)
        if not state["injected"]:
            state["injected"] = True
            # Real filesystem mutation of a sibling sidecar entry while the
            # optional probe is open.
            (db_path.parent / (db_path.name + "-journal")).write_bytes(b"")
        return connection

    monkeypatch.setattr(sqlite3, "connect", _injecting_connect)
    plan = plan_profile_migration(_s202_request(home, source_sql=db_path))
    monkeypatch.undo()

    assert state["injected"] is True
    assert "E_SQLITE_PROBE_UNSAFE" in _s202_codes(plan)
    assert _s202_section(plan, "sqlite", "open_policy") == "sidecars_absent_immutable_ro"
    # A difference disables the optimization: report unknown, never guess.
    assert _s202_section(plan, "sqlite", "schema") == "unknown"
    assert _s202_section(plan, "sqlite", "integrity") == "unknown"
    assert _s202_section(plan, "invariance", "artifacts") == "failed"


def test_s202_identity_digest_covers_bytes_beyond_64k(
    tmp_path: Path, synthetic_storage_env
) -> None:
    from memory_server.storage_lock import read_regular_file_nofollow

    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _s202_seed_sized_db(home / "data" / "memory.db", 200_000)

    first = plan_profile_migration(_s202_request(home, source_sql=db_path)).source_sql.sha256
    assert first is not None
    with db_path.open("r+b") as handle:
        handle.seek(150_000)
        handle.write(b"\xff")
    second = plan_profile_migration(_s202_request(home, source_sql=db_path)).source_sql.sha256

    assert second is not None and second != first
    # The bounded 64 KiB reader cannot see that byte at all (residual F7).
    bounded = read_regular_file_nofollow(db_path)
    assert len(bounded) <= 65536
    assert hashlib.sha256(bounded).hexdigest() != second


def test_s202_shrinking_source_mid_read_is_refused_not_truncated(
    tmp_path: Path, synthetic_storage_env, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _s202_seed_sized_db(home / "data" / "memory.db", 200_000)
    real_read = os.read
    state = {"hit": False}

    def _truncating_read(descriptor: int, size: int) -> bytes:
        if not state["hit"]:
            try:
                target = os.readlink(f"/proc/self/fd/{descriptor}")
            except OSError:
                target = ""
            if target == str(db_path):
                state["hit"] = True
                os.ftruncate(descriptor, 0)
        return real_read(descriptor, size)

    monkeypatch.setattr(os, "read", _truncating_read)
    try:
        plan = plan_profile_migration(_s202_request(home, source_sql=db_path))
    finally:
        monkeypatch.undo()

    assert state["hit"] is True
    # A short read must never be silently digested as the whole file.
    assert plan.source_sql.sha256 is None
    assert "E_ARTIFACT_IDENTITY_CHANGED" in _s202_codes(plan)


def test_s202_run_directory_chain_symlink_is_refused_without_traversal(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _s202_seed_db_at(home / "data" / "memory.db")
    external = tmp_path / "external-runs"
    external.mkdir()
    (external / "planted.bin").write_bytes(b"PLANTED")
    (home / ".cmms-migrations").symlink_to(external, target_is_directory=True)
    before = _tree_snapshot(external)

    plan = plan_profile_migration(_s202_request(home, source_sql=db_path))

    assert "E_PATH_SYMLINK_PARENT" in _s202_codes(plan)
    # The planted referent was never traversed and nothing was created in it.
    assert _tree_snapshot(external) == before
    assert sorted(path.name for path in external.iterdir()) == ["planted.bin"]
    assert not (external / plan.request.run_id).exists()


def test_s202_existing_lock_is_inspected_readonly(tmp_path: Path, synthetic_storage_env) -> None:
    from memory_server.storage_lock import MaintenanceStorageLocks

    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _s202_seed_db_at(home / "data" / "memory.db")

    absent = plan_profile_migration(_s202_request(home, source_sql=db_path))
    assert absent.lock_availability == "unknown"
    assert not (home / ".cmms-storage.lock").exists()

    locks = MaintenanceStorageLocks.acquire([home])
    try:
        held = plan_profile_migration(_s202_request(home, source_sql=db_path))
        assert not (home / ".cmms-storage.lock").is_symlink()
    finally:
        locks.release()

    assert held.lock_availability == "held"
    assert "E_WRITER_ACTIVE" in _s202_codes(held)
    released = plan_profile_migration(_s202_request(home, source_sql=db_path))
    assert released.lock_availability == "available"


def test_s202_plan_is_mutation_free_including_listings(
    tmp_path: Path, synthetic_storage_env, monkeypatch: pytest.MonkeyPatch
) -> None:
    import socket as _socket

    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _s202_seed_db_at(home / "data" / "memory.db")
    (home / "data" / "memory.db-wal").write_bytes(b"wal-bytes")
    _s202_external_link(tmp_path, home, name="lancedb")
    # A config file larger than the bounded 64 KiB reader.
    (home / "config.yaml").write_bytes(b"memory:\n  providers: {}\n" + b"# pad line\n" * 12000)
    before = _tree_snapshot(home)
    assert not (home / ".cmms-migrations").exists()

    def _no_socket(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("dry-run opened a network socket")

    monkeypatch.setattr(_socket, "socket", _no_socket, raising=False)
    plan = plan_profile_migration(_s202_request(home, source_sql=db_path, raw_config=_S202_CONFIG_BLOCK))
    monkeypatch.undo()

    assert _tree_snapshot(home) == before
    assert not (home / ".cmms-migrations").exists()
    assert not (home / ".cmms-storage.lock").exists()
    assert plan.embedding.network is False


def test_s202_hardlinked_config_file_is_refused_and_inventoried_nofollow(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _s202_seed_db_at(home / "data" / "memory.db")
    victim = tmp_path / "victim.yaml"
    payload = b"memory:\n  providers:\n    memory_server:\n      path: /install\n"
    victim.write_bytes(payload)
    os.link(victim, home / "config.yaml")

    plan = plan_profile_migration(_s202_request(home, source_sql=db_path))

    assert "E_PATH_HARDLINK_UNSAFE" in _s202_codes(plan)
    assert _s202_section(plan, "config_file", "kind") == "regular_file"
    assert _s202_section(plan, "config_file", "sha256") is None
    assert victim.read_bytes() == payload


def test_s202_external_and_overlapping_targets_are_refused(
    tmp_path: Path, synthetic_storage_env
) -> None:
    from memory_server.paths import StorageLayoutError

    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _s202_seed_db_at(home / "data" / "memory.db")

    # StorageLayoutError carries its stable code in .code (str() is the message).
    with pytest.raises(StorageLayoutError) as external:
        plan_profile_migration(
            _s202_request(
                home,
                configured_data_root=str(tmp_path / "outside"),
                raw_config=_S202_CONFIG_BLOCK,
            )
        )
    assert external.value.code == "E_PROFILE_ROOT_EXTERNAL"

    overlapping = dict(_S202_CONFIG_BLOCK, graph_snapshot_path="data/lancedb")
    with pytest.raises(StorageLayoutError) as overlap:
        plan_profile_migration(_s202_request(home, raw_config=overlapping, source_sql=db_path))
    assert overlap.value.code == "E_PATH_OVERLAP"


def test_s202_report_is_json_serializable_and_bounded(tmp_path: Path, synthetic_storage_env) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _s202_seed_sized_db(home / "data" / "memory.db", 100_000)
    # A config file larger than the bounded 64 KiB reader: its identity digest
    # must come from the streaming fd-relative read, not from a truncated one.
    (home / "config.yaml").write_bytes(b"memory:\n  providers: {}\n" + b"# pad line\n" * 12000)

    plan = plan_profile_migration(_s202_request(home, source_sql=db_path))
    report = _s202_report(plan)

    assert report.get("schema_version") == 1
    assert report.get("mode") == "dry-run"
    assert report.get("strategy") == "rebuild-from-profile-sql"
    for key in (
        "source_sql",
        "source_sidecars",
        "sidecars",
        "legacy_projections",
        "target",
        "path_checks",
        "config",
        "config_file",
        "parents",
        "collisions",
        "sqlite",
        "disk",
        "invariance",
        "lock_availability",
        "planned_operations",
        "runtime_stop_instructions",
        "proposed_manifest_path",
        "warnings",
        "blockers",
    ):
        assert key in report, key
    encoded = json.dumps(report, sort_keys=True)
    assert len(encoded) < 200_000
    assert report["proposed_manifest_path"] == str(
        home / ".cmms-migrations" / plan.request.run_id / "manifest.json"
    )
    # The config FILE identity is a real streaming digest, not a truncated one.
    assert _s202_section(plan, "config_file", "kind") == "regular_file"
    assert (home / "config.yaml").stat().st_size > 65536
    assert re.fullmatch(r"[0-9a-f]{64}", str(_s202_section(plan, "config_file", "sha256")))


# ---------------------------------------------------------------------------
# S2-03 -- qualified SQLite transaction probe and run-directory safety snapshot
#
# DETAIL 6.3 step 10, 7.3 and 10.1. Every node below addresses a behaviour the
# pre-fix module does not have: the exact bounded ``BEGIN IMMEDIATE`` /
# ``ROLLBACK`` write-lock probe (never an ``immutable=1`` read-only URI), its
# byte/entry/parent invariance proof on a sidecar-free regular source, the
# competing-writer refusal that proves write-lock exclusion, the
# ``sqlite3.Connection.backup`` safety snapshot taken from a percent-encoded
# immutable read-only source URI, and the snapshot's integrity / schema /
# Alembic revision / ID / outbox verification read back from the RUN DIRECTORY
# instead of the live source.
#
# CLASSIFICATION OF THE FILED PRE-FIX RED (S2-03 fix round, review F2): those
# accessors are shape-tolerant, so on a module that lacks the S2-03 API they
# return ``({}, [])`` and the filed RED failures are SENTINEL comparisons against
# that missing-capability result (``assert None == 'sidecars_absent_...'``,
# ``assert {} is True``) -- a missing-capability RED, which routing-matrix line 63
# says is NEVER a behavioural safety proof. It is labelled as such in
# S2-03_EVIDENCE/S2-03_FIX1_RED_RELABEL.md; the behavioural RED is the separate
# F1 capture S2-03_FIX1_BEHAVIOURAL_RED.log, where the API exists and a real
# database at the tree's real head is refused.
# ---------------------------------------------------------------------------

def _s203_tree_head_revisions() -> frozenset[str]:
    """The tree's real Alembic head(s), derived from its OWN configuration.

    F1: both ``version_locations`` configured in the repository's ``alembic.ini``
    are read (``alembic/versions`` AND ``migrations/versions``), every revision
    file in each is parsed, and a revision that no other revision names as a
    ``down_revision`` is a head. The derivation imports nothing (no alembic, no
    SQLAlchemy -- alembic is a dev-only extra and must not become a runtime
    dependency) and is deliberately independent of the production constant, so a
    wrong constant cannot mirror itself into these tests the way it did in S2-03.
    """
    tree_root = Path(profile_migration.__file__).resolve().parents[2]
    ini_path = tree_root / "alembic.ini"
    match = re.search(
        r"^version_locations\s*=\s*(.+)$", ini_path.read_text(encoding="utf-8"), re.MULTILINE
    )
    assert match is not None, f"no version_locations entry in {ini_path}"
    locations = [
        part.replace("%(here)s", str(tree_root)) for part in match.group(1).strip().split(os.pathsep)
    ]
    assert len(locations) == 2, locations
    revisions: dict[str, Any] = {}
    for location in locations:
        for revision_file in sorted(Path(location).glob("*.py")):
            assignments: dict[str, Any] = {}
            for node in ast.parse(revision_file.read_text(encoding="utf-8")).body:
                names: list[tuple[str, Any]] = []
                if isinstance(node, ast.Assign):
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            names.append((target.id, node.value))
                elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                    names.append((node.target.id, node.value))
                for name, value in names:
                    if name in {"revision", "down_revision"} and value is not None:
                        assignments[name] = ast.literal_eval(value)
            assert isinstance(assignments.get("revision"), str), revision_file
            revisions[assignments["revision"]] = assignments.get("down_revision")
    parents: set[str] = set()
    for down in revisions.values():
        if isinstance(down, str):
            parents.add(down)
        elif isinstance(down, (tuple, list)):
            parents.update(item for item in down if isinstance(item, str))
    return frozenset(set(revisions) - parents)


_S203_TREE_HEADS = _s203_tree_head_revisions()
# The tree's real head, DERIVED from both configured version locations rather
# than mirrored from the production constant -- the mirrored literal is exactly
# how the F1 defect (an interior node 7a1b2c3d4e5f accepted as "the head") hid.
S203_HEAD_REVISION = next(iter(_S203_TREE_HEADS))
S203_REQUIRED_CODES = ("E_SQLITE_PROBE_UNSAFE", "E_SQLITE_SCHEMA", "E_BACKUP_COLLISION")


def test_s203_accepted_revisions_are_the_tree_head_from_both_version_locations() -> None:
    """The accepted set is this tree's real head, from BOTH version locations.

    F1: the accepted-revision gate must move with the tree. This node recomputes
    the head from ``alembic.ini``'s two ``version_locations`` and compares it to
    the production constant, so adding, removing or merging a revision fails
    here instead of silently refusing every genuinely current database. The
    anchored literal below is the pin the reviewer asked for: it and
    ``ACCEPTED_SQLITE_SCHEMA_REVISIONS`` must be updated together.
    """
    heads = _s203_tree_head_revisions()
    assert heads == frozenset({"0005"}), heads
    assert frozenset(profile_migration.ACCEPTED_SQLITE_SCHEMA_REVISIONS) == heads
    # 7a1b2c3d4e5f is consumed as a parent by the merge revision 0005, so it is
    # an interior node of the merged DAG and never a head.
    assert "7a1b2c3d4e5f" not in heads
    assert len(heads) == 1


def test_s203_source_at_the_tree_head_revision_is_accepted(
    tmp_path: Path, synthetic_storage_env
) -> None:
    """A real database stamped at the tree's ACTUAL head is accepted.

    F1 behavioural proof: the S2-03 gate accepted only the interior node
    7a1b2c3d4e5f and REFUSED a database stamped at the real head (0005) with
    E_SQLITE_SCHEMA, so it refused every genuinely up-to-date database. A real
    (synthetic, sidecar-free) database stamped at the head must qualify with no
    diagnostics at all.
    """
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _s203_seed_canonical_db(home / "data" / "memory.db", revision=S203_HEAD_REVISION)
    run_dir = _s203_run_dir(tmp_path)
    before = _tree_snapshot(home)

    report, diagnostics = _s203_qualify(db_path, run_dir)

    verification = _s203_section(report, "snapshot", "verification")
    assert verification.get("alembic_revision") == S203_HEAD_REVISION
    assert verification.get("revision_accepted") is True
    assert _s203_section(report, "probe", "qualified") is True
    assert _s203_section(report, "snapshot", "created") is True
    codes = _s203_codes(diagnostics)
    assert "E_SQLITE_SCHEMA" not in codes, codes
    assert diagnostics == []
    assert _tree_snapshot(home) == before


def test_s203_qualifier_has_no_public_writer_refusal_bypass(
    tmp_path: Path, synthetic_storage_env
) -> None:
    """The public API cannot skip the write-lock exclusion proof.

    F3: ``qualify_sqlite_source`` exposed an undeclared keyword-only
    ``competing_writer=False`` that skipped the competitor-refusal enforcement
    entirely, so a caller could get ``qualified=True`` with no exclusion proof
    at all. After the fix the knob is gone from the public surface: the
    signature carries no such parameter and passing it is a TypeError, so
    production behaviour cannot lose the proof.
    """
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _s203_seed_canonical_db(home / "data" / "memory.db")
    run_dir = _s203_run_dir(tmp_path)

    signature = inspect.signature(profile_migration.qualify_sqlite_source)
    assert "competing_writer" not in signature.parameters
    with pytest.raises(TypeError):
        profile_migration.qualify_sqlite_source(db_path, run_dir=run_dir, competing_writer=False)


def test_s203_probe_reports_unsafe_when_the_competing_writer_is_not_refused(
    tmp_path: Path, synthetic_storage_env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A competing writer that is NOT refused is E_SQLITE_PROBE_UNSAFE.

    Reviewer mutation M2 deleted this enforcement (profile_migration.py:1344) and
    the whole suite still passed: the branch was untested. The refusal outcome is
    replaced at its private seam so the enforcement branch is exercised
    deterministically on a real synthetic database -- no exclusion proof, no
    qualification, no snapshot. Deleting the enforcement makes this node fail.
    """
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _s203_seed_canonical_db(home / "data" / "memory.db")
    run_dir = _s203_run_dir(tmp_path)
    before = _tree_snapshot(home)

    def _not_refused(uri: str) -> dict[str, Any]:
        return {
            "attempted": True,
            "refused": False,
            "sqlite_errorname": None,
            "detail": "injected: the competing writer acquired the write lock",
        }

    monkeypatch.setattr(profile_migration, "_competing_writer_refusal", _not_refused)

    report, diagnostics = _s203_qualify(db_path, run_dir)

    assert "E_SQLITE_PROBE_UNSAFE" in _s203_codes(diagnostics)
    probe = _s203_section(report, "probe")
    assert probe.get("in_transaction") is True
    assert probe.get("rolled_back") is True
    assert _s203_section(report, "probe", "competing_writer", "refused") is False
    assert probe.get("unsafe") is True
    assert probe.get("qualified") is False
    assert _s203_section(report, "snapshot") is None
    assert not (run_dir / "snapshot").exists()
    # The refusal is recorded, not executed: the source is still untouched.
    assert _tree_snapshot(home) == before


def test_s203_probe_reports_unsafe_when_the_bounded_transaction_is_not_held(
    tmp_path: Path, synthetic_storage_env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A probe that did not hold and roll back one transaction is UNSAFE.

    Reviewer mutation M4 deleted the in_transaction/rolled_back enforcement
    (profile_migration.py:1333) and the whole suite still passed. The real probe
    runs here and its transaction flags are then falsified at the private seam,
    so the enforcement branch is exercised for real. Deleting the enforcement
    makes this node fail.
    """
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _s203_seed_canonical_db(home / "data" / "memory.db")
    run_dir = _s203_run_dir(tmp_path)
    before = _tree_snapshot(home)

    original_probe = profile_migration._transaction_probe

    def _no_transaction(source: Path, **kwargs: Any) -> dict[str, Any]:
        report = original_probe(source, **kwargs)
        report["in_transaction"] = False
        return report

    monkeypatch.setattr(profile_migration, "_transaction_probe", _no_transaction)

    report, diagnostics = _s203_qualify(db_path, run_dir)

    assert "E_SQLITE_PROBE_UNSAFE" in _s203_codes(diagnostics)
    probe = _s203_section(report, "probe")
    # The competing writer WAS refused, so this is the transaction branch only.
    assert _s203_section(report, "probe", "competing_writer", "refused") is True
    assert probe.get("in_transaction") is False
    assert probe.get("unsafe") is True
    assert probe.get("qualified") is False
    assert _s203_section(report, "snapshot") is None
    assert not (run_dir / "snapshot").exists()
    assert _tree_snapshot(home) == before


def _s203_qualify(source: Path, run_dir: Path, **kwargs: Any) -> tuple[dict[str, Any], list[Any]]:
    """Call the S2-03 qualifier, or ``({}, [])`` when the API is absent.

    The ``({}, [])`` fallback is what makes the filed pre-fix RED a SENTINEL
    missing-capability result rather than a collection ``ImportError``; that is
    its honest classification (see the block comment above), not a claim of
    behavioural proof.
    """
    api = getattr(profile_migration, "qualify_sqlite_source", None)
    if api is None:
        return {}, []
    report, diagnostics = api(source, run_dir=run_dir, **kwargs)
    return dict(report), list(diagnostics)


def _s203_verify(snapshot_path: Path, **kwargs: Any) -> tuple[dict[str, Any], list[Any]]:
    """Call the S2-03 snapshot verification, or ``({}, [])`` when it is absent.

    A sentinel fallback, exactly as ``_s203_qualify`` above -- missing capability,
    never behavioural evidence.
    """
    api = getattr(profile_migration, "verify_snapshot", None)
    if api is None:
        return {}, []
    report, diagnostics = api(snapshot_path, **kwargs)
    return dict(report), list(diagnostics)


def _s203_section(report: Mapping[str, Any], *path: str) -> Any:
    """Nested lookup yielding ``{}`` for anything the module does not report."""
    current: Any = report
    for key in path:
        if not isinstance(current, Mapping) or key not in current:
            return {}
        current = current[key]
    return current


def _s203_codes(diagnostics: list[Any]) -> list[str]:
    return [str(getattr(item, "code", "")) for item in diagnostics]


def _s203_uri(source: Path, query: str) -> str:
    return "file:" + quote(str(source), safe="/") + "?" + query


def _s203_seed_canonical_db(
    path: Path, *, journal_mode: str | None = None, revision: str = S203_HEAD_REVISION
) -> Path:
    """A synthetic, sidecar-free database carrying the snapshot's target schema."""
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    try:
        if journal_mode is not None:
            connection.execute(f"PRAGMA journal_mode={journal_mode}")
        connection.execute("create table alembic_version(version_num text)")
        connection.execute("insert into alembic_version values(?)", (revision,))
        connection.execute("create table facts(id text, subject text)")
        connection.executemany(
            "insert into facts values(?,?)", [(f"fact-{index}", f"subject-{index}") for index in range(3)]
        )
        connection.execute("create table outbox_entries(id text, status text)")
        connection.executemany(
            "insert into outbox_entries values(?,?)",
            [
                ("outbox-1", "pending"),
                ("outbox-2", "pending"),
                ("outbox-3", "completed"),
                ("outbox-4", "failed"),
                ("outbox-5", "processing"),
            ],
        )
        connection.commit()
    finally:
        connection.close()
    return path


def _s203_header_modes(path: Path) -> tuple[int, int]:
    header = path.read_bytes()[:20]
    return header[18], header[19]


def _s203_run_dir(tmp_path: Path) -> Path:
    return tmp_path / "run" / "0123456789abcdef0123456789abcdef"


def _s203_sidecar_paths(db_path: Path) -> tuple[Path, ...]:
    return tuple(db_path.with_name(db_path.name + suffix) for suffix in ("-wal", "-shm", "-journal"))


def test_s203_module_under_test_is_the_tree_that_owns_this_test_file() -> None:
    """The loaded module is the tree under test, never the ambient agent venv."""
    module_path = Path(profile_migration.__file__).resolve()
    assert Path(__file__).resolve().parents[1] in module_path.parents
    digest = hashlib.sha256(module_path.read_bytes()).hexdigest()
    assert re.fullmatch(r"[0-9a-f]{64}", digest)
    print(f"module under test: {module_path} sha256={digest}")


def test_s203_sidecar_free_rollback_source_probe_is_byte_and_entry_invariant(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _s203_seed_canonical_db(home / "data" / "memory.db")
    assert _s203_header_modes(db_path) == (1, 1)
    run_dir = _s203_run_dir(tmp_path)
    before = _tree_snapshot(home)

    report, diagnostics = _s203_qualify(db_path, run_dir)

    assert diagnostics == []
    probe = _s203_section(report, "probe")
    assert probe.get("policy") == "sidecars_absent_writable_probe"
    assert probe.get("performed") is True
    assert probe.get("in_transaction") is True
    assert probe.get("rolled_back") is True
    assert probe.get("qualified") is True
    assert probe.get("unsafe") is False
    assert probe.get("journal_mode_header") == "rollback"
    assert probe.get("failure") is None
    assert probe.get("sql") == ["BEGIN IMMEDIATE", "ROLLBACK"]
    uri = str(probe.get("uri"))
    assert uri == _s203_uri(db_path, "mode=rw")
    assert "immutable" not in uri
    assert _s203_section(report, "probe", "invariance", "artifacts") == "ok"
    assert _s203_section(report, "probe", "invariance", "parent") == "ok"
    # ROLLBACK left the database, every sidecar and the parent listing unchanged.
    assert _tree_snapshot(home) == before
    assert [path for path in _s203_sidecar_paths(db_path) if path.exists()] == []


def test_s203_sidecar_free_wal_source_probe_is_byte_and_entry_invariant(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _s203_seed_canonical_db(home / "data" / "memory.db", journal_mode="wal")
    # The image is WAL-mode but sidecar-free: header write/read version 2.
    assert _s203_header_modes(db_path) == (2, 2)
    assert [path for path in _s203_sidecar_paths(db_path) if path.exists()] == []
    run_dir = _s203_run_dir(tmp_path)
    before = _tree_snapshot(home)

    report, diagnostics = _s203_qualify(db_path, run_dir)

    assert diagnostics == []
    probe = _s203_section(report, "probe")
    assert probe.get("journal_mode_header") == "wal"
    assert probe.get("performed") is True
    assert probe.get("qualified") is True
    assert _s203_section(report, "probe", "invariance", "artifacts") == "ok"
    assert _s203_section(report, "probe", "invariance", "parent") == "ok"
    # A WAL-mode source is probed without creating a WAL, an SHM or a journal.
    assert _tree_snapshot(home) == before
    assert [path for path in _s203_sidecar_paths(db_path) if path.exists()] == []


def test_s203_probe_records_the_exact_runtime_triple_and_encoded_uri(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    # A space in the path proves the probe URI is percent-encoded too.
    db_path = _s203_seed_canonical_db(home / "data" / "my memory.db")
    run_dir = _s203_run_dir(tmp_path)

    report, _ = _s203_qualify(db_path, run_dir)

    runtime = _s203_section(report, "probe", "runtime")
    assert runtime.get("sqlite_version") == sqlite3.sqlite_version
    assert runtime.get("sqlite3_module") == getattr(sqlite3, "version", "unknown")
    assert runtime.get("python") == ".".join(str(part) for part in sys.version_info[:3])
    assert runtime.get("platform") == sys.platform
    assert re.fullmatch(r"[0-9a-f]{64}", str(runtime.get("digest")))
    uri = str(_s203_section(report, "probe", "uri"))
    assert uri.startswith("file:")
    assert "my%20memory.db" in uri
    assert " " not in uri
    assert uri.endswith("?mode=rw")
    assert "immutable" not in uri


def test_s203_probe_proves_write_lock_exclusion_against_a_competing_writer(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _s203_seed_canonical_db(home / "data" / "memory.db")
    run_dir = _s203_run_dir(tmp_path)
    before = _tree_snapshot(home)
    observed: dict[str, Any] = {}

    def _competing_writer() -> None:
        """An independent connection is refused while the probe holds the lock."""
        try:
            connection = sqlite3.connect(
                _s203_uri(db_path, "mode=rw"), uri=True, timeout=0, isolation_level=None
            )
        except sqlite3.Error as exc:  # pragma: no cover - open refusal is also a refusal
            observed["open"] = f"{type(exc).__name__}: {exc}"
            return
        try:
            connection.execute("BEGIN IMMEDIATE")
            observed["begin_immediate"] = "acquired"
            connection.execute("ROLLBACK")
        except sqlite3.Error as exc:
            observed["begin_immediate"] = f"{type(exc).__name__}: {exc}"
            observed["sqlite_errorname"] = getattr(exc, "sqlite_errorname", None)
        finally:
            connection.close()

    report, diagnostics = _s203_qualify(db_path, run_dir, while_locked=_competing_writer)

    assert diagnostics == []
    assert observed.get("begin_immediate", "") != "acquired"
    assert "database is locked" in str(observed.get("begin_immediate", ""))
    assert observed.get("sqlite_errorname") == "SQLITE_BUSY"
    recorded = _s203_section(report, "probe", "competing_writer")
    assert recorded.get("attempted") is True
    assert recorded.get("refused") is True
    assert recorded.get("sqlite_errorname") == "SQLITE_BUSY"
    assert _s203_section(report, "probe", "qualified") is True
    # The observation hook and the refusal left the source untouched.
    assert _tree_snapshot(home) == before
    assert [path for path in _s203_sidecar_paths(db_path) if path.exists()] == []


def test_s203_probe_refuses_when_a_live_writer_holds_the_database(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _s203_seed_canonical_db(home / "data" / "memory.db")
    run_dir = _s203_run_dir(tmp_path)
    holder = sqlite3.connect(_s203_uri(db_path, "mode=rw"), uri=True, timeout=0, isolation_level=None)
    holder.execute("BEGIN IMMEDIATE")
    try:
        report, diagnostics = _s203_qualify(db_path, run_dir)
    finally:
        holder.execute("ROLLBACK")
        holder.close()

    assert "E_SQLITE_PROBE_UNSAFE" in _s203_codes(diagnostics)
    probe = _s203_section(report, "probe")
    assert probe.get("qualified") is False
    assert probe.get("unsafe") is True
    assert probe.get("rolled_back") in (False, None)
    assert _s203_section(report, "probe", "failure", "sqlite_errorname") == "SQLITE_BUSY"
    # The exact attempted URI is recorded even when the probe could not take the
    # lock, and it is never the immutable read-only one.
    attempted = str(probe.get("uri"))
    assert attempted.endswith("?mode=rw")
    assert "immutable" not in attempted
    # No snapshot may be created from an unqualified source.
    assert _s203_section(report, "snapshot") is None
    assert not (run_dir / "snapshot").exists()


def test_s203_probe_reports_unsafe_when_a_parent_entry_appears_mid_probe(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _s203_seed_canonical_db(home / "data" / "memory.db")
    run_dir = _s203_run_dir(tmp_path)
    planted = db_path.with_name(db_path.name + "-journal")

    def _plant_entry() -> None:
        planted.write_bytes(b"")

    report, diagnostics = _s203_qualify(db_path, run_dir, while_locked=_plant_entry)

    assert "E_SQLITE_PROBE_UNSAFE" in _s203_codes(diagnostics)
    assert _s203_section(report, "probe", "invariance", "artifacts") == "failed"
    assert _s203_section(report, "probe", "unsafe") is True
    assert _s203_section(report, "probe", "qualified") is False
    assert _s203_section(report, "snapshot") is None
    assert not (run_dir / "snapshot").exists()


def test_s203_sidecar_present_source_is_never_probed_or_snapshotted(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _s203_seed_canonical_db(home / "data" / "memory.db", journal_mode="wal")
    wal = db_path.with_name(db_path.name + "-wal")
    shm = db_path.with_name(db_path.name + "-shm")
    wal.write_bytes(b"")
    shm.write_bytes(b"")
    run_dir = _s203_run_dir(tmp_path)
    before = _tree_snapshot(home)

    report, diagnostics = _s203_qualify(db_path, run_dir)

    codes = _s203_codes(diagnostics)
    assert "E_SQLITE_WAL_ACTIVE" in codes
    assert "E_SQLITE_SHM_AMBIGUOUS" in codes
    probe = _s203_section(report, "probe")
    assert probe.get("policy") == "sidecars_present_no_probe"
    assert probe.get("performed") is False
    assert probe.get("uri") is None
    assert _s203_section(report, "snapshot") is None
    assert not (run_dir / "snapshot").exists()
    # No recovery, no checkpoint: the planted sidecars are byte-identical.
    assert _tree_snapshot(home) == before
    assert wal.read_bytes() == b""
    assert shm.read_bytes() == b""


def test_s203_snapshot_uses_the_backup_api_from_an_immutable_readonly_uri(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _s203_seed_canonical_db(home / "data" / "my memory.db")
    run_dir = _s203_run_dir(tmp_path)
    before = _tree_snapshot(home)

    report, diagnostics = _s203_qualify(db_path, run_dir)

    assert "E_SQLITE_PROBE_UNSAFE" not in _s203_codes(diagnostics)
    snapshot = _s203_section(report, "snapshot")
    snapshot_path = Path(str(snapshot.get("path")))
    assert run_dir in snapshot_path.parents
    assert snapshot_path.is_file()
    assert snapshot.get("created") is True
    assert snapshot.get("api") == "sqlite3.Connection.backup"
    source_uri = str(snapshot.get("source_uri"))
    assert source_uri == _s203_uri(db_path, "mode=ro&immutable=1")
    assert "my%20memory.db" in source_uri
    assert " " not in source_uri
    assert re.fullmatch(r"[0-9a-f]{64}", str(snapshot.get("snapshot_sha256")))
    assert snapshot.get("size") == snapshot_path.stat().st_size
    # No WAL/SHM copied, no checkpoint, no recovery: the source tree is identical.
    assert _tree_snapshot(home) == before
    assert sorted(path.name for path in snapshot_path.parent.iterdir()) == [snapshot_path.name]
    assert not (home / "data" / "my memory.db-wal").exists()
    assert not (home / "data" / "my memory.db-shm").exists()


def test_s203_snapshot_verification_reports_schema_revision_ids_and_outbox(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _s203_seed_canonical_db(home / "data" / "memory.db")
    run_dir = _s203_run_dir(tmp_path)

    report, _ = _s203_qualify(db_path, run_dir)

    verification = _s203_section(report, "snapshot", "verification")
    assert verification.get("integrity") == "ok"
    assert verification.get("schema") == "known"
    tables = set(verification.get("tables", ()))
    assert {"alembic_version", "facts", "outbox_entries"} <= tables
    assert verification.get("alembic_revision") == S203_HEAD_REVISION
    assert verification.get("revision_accepted") is True
    facts = _s203_section(verification, "ids", "facts")
    assert facts.get("count") == 3
    assert re.fullmatch(r"[0-9a-f]{64}", str(facts.get("digest")))
    assert re.fullmatch(r"[0-9a-f]{64}", str(verification.get("ids_digest")))
    assert verification.get("outbox_counts") == {
        "pending": 2,
        "processing": 1,
        "completed": 1,
        "failed": 1,
    }
    assert tuple(verification.get("unexpected_outbox_statuses", ())) == ()


def test_s203_snapshot_verification_reads_the_run_dir_copy_not_the_live_source(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _s203_seed_canonical_db(home / "data" / "memory.db")
    run_dir = _s203_run_dir(tmp_path)
    report, _ = _s203_qualify(db_path, run_dir)
    snapshot_path = Path(str(_s203_section(report, "snapshot", "path")))
    assert snapshot_path.is_file()

    live = sqlite3.connect(db_path)
    try:
        live.execute("insert into facts values('post-snapshot','x')")
        live.execute("insert into outbox_entries values('outbox-6','pending')")
        live.commit()
    finally:
        live.close()

    verified, diagnostics = _s203_verify(snapshot_path)

    assert diagnostics == []
    assert _s203_section(verified, "ids", "facts", "count") == 3
    assert _s203_section(verified, "outbox_counts", "pending") == 2
    after = sqlite3.connect(db_path)
    try:
        assert after.execute("select count(*) from facts").fetchone()[0] == 4
        assert after.execute("select count(*) from outbox_entries").fetchone()[0] == 6
    finally:
        after.close()


def test_s203_snapshot_with_an_unaccepted_revision_blocks_with_a_stable_code(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _s203_seed_canonical_db(home / "data" / "memory.db", revision="deadbeef0000")
    run_dir = _s203_run_dir(tmp_path)

    report, diagnostics = _s203_qualify(db_path, run_dir)

    assert "E_SQLITE_SCHEMA" in _s203_codes(diagnostics)
    verification = _s203_section(report, "snapshot", "verification")
    assert verification.get("alembic_revision") == "deadbeef0000"
    assert verification.get("revision_accepted") is False
    assert _s203_section(report, "probe", "qualified") is True


def test_s203_snapshot_never_overwrites_an_existing_run_artifact(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _s203_seed_canonical_db(home / "data" / "memory.db")
    run_dir = _s203_run_dir(tmp_path)
    existing = run_dir / "snapshot" / "memory.db"
    existing.parent.mkdir(parents=True)
    existing.write_bytes(b"PRE-EXISTING-RUN-ARTIFACT")

    report, diagnostics = _s203_qualify(db_path, run_dir)

    assert "E_BACKUP_COLLISION" in _s203_codes(diagnostics)
    assert _s203_section(report, "snapshot", "created") is False
    assert existing.read_bytes() == b"PRE-EXISTING-RUN-ARTIFACT"


def test_s203_snapshot_refuses_a_symlinked_run_artifact_directory(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _s203_seed_canonical_db(home / "data" / "memory.db")
    run_dir = _s203_run_dir(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    run_dir.mkdir(parents=True)
    (run_dir / "snapshot").symlink_to(outside, target_is_directory=True)

    report, diagnostics = _s203_qualify(db_path, run_dir)

    assert "E_PATH_FINAL_SYMLINK_UNSAFE" in _s203_codes(diagnostics)
    assert _s203_section(report, "snapshot", "created") is False
    assert list(outside.iterdir()) == []


def test_s203_snapshot_refuses_a_special_file_at_the_run_artifact_path(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="migration root")

    home = tmp_path / "home"
    db_path = _s203_seed_canonical_db(home / "data" / "memory.db")
    run_dir = _s203_run_dir(tmp_path)
    fifo_dir = run_dir / "snapshot"
    fifo_dir.mkdir(parents=True)
    os.mkfifo(fifo_dir / "memory.db")

    report, diagnostics = _s203_qualify(db_path, run_dir)

    assert "E_PATH_SPECIAL_FILE" in _s203_codes(diagnostics)
    assert _s203_section(report, "snapshot", "created") is False


# ---------------------------------------------------------------------------
# S2-04 -- mixed-version maintenance preconditions and lock lifetime ownership
#
# The BEHAVIOURAL nodes below drive only the already-approved public surface
# (`apply_profile_migration` / `resume_profile_migration` /
# `rollback_profile_migration` / `plan_profile_migration`), so they fail at the
# card's parent commit for a behavioural reason -- the entrypoint reaches the
# unimplemented-engine gate instead of the new fail-closed code -- and never
# with a collection ImportError. Nodes that reference a symbol introduced by
# this card import it inside the body and are labelled MISSING-CAPABILITY.
# ---------------------------------------------------------------------------


def _s204_request(home: Path, db_path: Path, **fields: Any) -> MigrationRequest:
    fields.setdefault("stop_attestation", "maintenance-ticket")
    return MigrationRequest(
        home,
        source_sql=db_path,
        confirm_target=str(home),
        **fields,
    )


def _s204_planned_request(home: Path, db_path: Path, **fields: Any):
    request = _s204_request(home, db_path, mode="apply", **fields)
    digest = plan_profile_migration(request).embedding.digest
    confirmed = replace(request, embedding_plan_digest=digest)
    return confirmed, plan_profile_migration(confirmed)


def _s204_old_writer(database: str, ready, release) -> None:
    connection = sqlite3.connect(database, isolation_level=None)
    connection.execute("BEGIN IMMEDIATE")
    ready.put(os.getpid())
    release.wait(10)
    connection.rollback()
    connection.close()


def _s204_late_writer(database: str, delay: float) -> None:
    time.sleep(delay)
    for _ in range(3):
        connection = sqlite3.connect(database)
        connection.execute("insert into facts(id) values ('late')")
        connection.commit()
        connection.close()
        time.sleep(1.0)


def test_s204_missing_attestation_is_refused_by_every_entrypoint_independently(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    home = tmp_path / "home"
    db_path = _seed_source_sql(home)
    request = _s204_request(home, db_path, stop_attestation=None)
    apply_request = replace(request, mode="apply")
    plan = plan_profile_migration(apply_request)
    manifest_path = home / ".cmms-migrations" / request.run_id / "manifest.json"
    with pytest.raises(ValueError, match="E_STOP_ATTESTATION_REQUIRED"):
        apply_profile_migration(plan)
    with pytest.raises(ValueError, match="E_STOP_ATTESTATION_REQUIRED"):
        resume_profile_migration(manifest_path, replace(request, mode="resume"))
    with pytest.raises(ValueError, match="E_STOP_ATTESTATION_REQUIRED"):
        rollback_profile_migration(manifest_path, replace(request, mode="rollback"))


def test_s204_apply_refuses_a_source_changed_since_it_was_planned(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    home = tmp_path / "home"
    db_path = _seed_source_sql(home)
    request, plan = _s204_planned_request(home, db_path)
    assert request.mode == "apply"
    connection = sqlite3.connect(db_path)
    connection.execute("insert into facts(id) values ('changed')")
    connection.commit()
    connection.close()
    before = _tree_snapshot(home)
    with pytest.raises(ValueError, match="E_PLAN_STALE"):
        apply_profile_migration(plan)
    assert _tree_snapshot(home) == before


def test_s204_apply_refuses_an_unbounded_stop_attestation(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    home = tmp_path / "home"
    db_path = _seed_source_sql(home)
    request, plan = _s204_planned_request(home, db_path, stop_attestation="x" * 4096)
    assert (request.stop_attestation or "") == "x" * 4096
    with pytest.raises(ValueError, match="E_ATTESTATION_UNBOUNDED"):
        apply_profile_migration(plan)


def test_s204_apply_detects_a_real_old_sqlite_writer_and_never_signals_it(
    tmp_path: Path, synthetic_storage_env
) -> None:
    import multiprocessing

    env = synthetic_storage_env
    env.assert_injection()
    home = tmp_path / "home"
    db_path = _seed_source_sql(home)
    request, plan = _s204_planned_request(home, db_path)
    ready: multiprocessing.Queue[int] = multiprocessing.Queue()
    release = multiprocessing.Event()
    proc = multiprocessing.Process(target=_s204_old_writer, args=(str(db_path), ready, release))
    proc.start()
    try:
        child = ready.get(timeout=8)
        assert child != os.getpid()
        with pytest.raises(ValueError, match="E_OLD_WRITER_ACTIVE"):
            apply_profile_migration(plan)
        assert proc.is_alive()
    finally:
        release.set()
        proc.join(10)
        if proc.is_alive():
            proc.terminate()
    assert proc.exitcode == 0


def test_s204_apply_detects_a_source_written_during_the_quiet_interval(
    tmp_path: Path, synthetic_storage_env
) -> None:
    import multiprocessing

    env = synthetic_storage_env
    env.assert_injection()
    home = tmp_path / "home"
    db_path = _seed_source_sql(home)
    request, plan = _s204_planned_request(home, db_path)
    proc = multiprocessing.Process(target=_s204_late_writer, args=(str(db_path), 1.5))
    proc.start()
    try:
        with pytest.raises(ValueError, match="E_SOURCE_CHANGED"):
            apply_profile_migration(plan)
    finally:
        proc.join(12)
        if proc.is_alive():
            proc.terminate()
    assert proc.exitcode == 0


def test_s204_every_entrypoint_replans_independently_without_inherited_state(
    tmp_path: Path, synthetic_storage_env, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    home = tmp_path / "home"
    db_path = _seed_source_sql(home)
    request, plan = _s204_planned_request(home, db_path)
    calls: list[str] = []
    original = profile_migration.plan_profile_migration

    def counting(candidate: MigrationRequest):
        calls.append(candidate.mode)
        return original(candidate)

    monkeypatch.setattr(profile_migration, "plan_profile_migration", counting)
    manifest_path = home / ".cmms-migrations" / request.run_id / "manifest.json"
    with pytest.raises(ValueError, match="E_MIGRATION_NOT_IMPLEMENTED"):
        apply_profile_migration(plan)
    # S2-07 moved this boundary and this leg is updated for it, not weakened:
    # `resume` now repeats the preconditions and reads the run's own manifest, so
    # with no manifest at all it is refused with its own stable code. The property
    # this node exists for -- that resume replans for ITSELF, on its own mode, and
    # inherits nothing from the caller -- is asserted by `calls` below.
    with pytest.raises(ValueError, match="E_MANIFEST_ABSENT"):
        resume_profile_migration(manifest_path, replace(request, mode="resume"))
    # S2-08 moved this leg too, in the same way and for the same reason: rollback now
    # repeats its own preconditions and reads the run's own manifest, so with no
    # manifest at all it refuses with the manifest code rather than the slice-level
    # "not implemented". The property this node exists for -- every entrypoint replans
    # for ITSELF, on its own mode, and inherits nothing -- is asserted by `calls` below.
    with pytest.raises(ValueError, match="E_MANIFEST_ABSENT"):
        rollback_profile_migration(manifest_path, replace(request, mode="rollback"))
    assert calls == ["apply", "resume", "rollback"]


def test_s204_preconditions_record_a_bounded_attestation_digest_and_time(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    home = tmp_path / "home"
    db_path = _seed_source_sql(home)
    record = profile_migration.record_stop_attestation
    validate = profile_migration.validate_mutation_preconditions
    attestation = record("maintenance-ticket", roots=(home,))
    assert attestation.digest == hashlib.sha256(b"maintenance-ticket").hexdigest()
    assert attestation.value_bytes == len(b"maintenance-ticket")
    parsed = datetime.fromisoformat(attestation.recorded_at)
    assert parsed.tzinfo is not None
    assert attestation.process_classes
    with pytest.raises(ValueError, match="E_ATTESTATION_UNBOUNDED"):
        record("x" * 4096, roots=(home,))
    with pytest.raises(ValueError, match="E_STOP_ATTESTATION_REQUIRED"):
        record("   ", roots=(home,))
    request, plan = _s204_planned_request(home, db_path)
    preconditions = validate(request, plan=plan)
    assert preconditions.attestation.digest == attestation.digest
    assert preconditions.attestation.recorded_at
    assert preconditions.quiet_interval == 2.0
    assert preconditions.probe["qualified"] is True
    assert preconditions.writer_state["covered"] is True


def test_s204_unknown_and_unsupported_writer_states_fail_closed(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    home = tmp_path / "home"
    db_path = _seed_source_sql(home)
    request, plan = _s204_planned_request(home, db_path)
    validate = profile_migration.validate_mutation_preconditions
    with pytest.raises(ValueError, match="E_WRITER_INVENTORY_UNSUPPORTED"):
        validate(request, plan=plan, proc_root=tmp_path / "absent-proc")


def test_s204_maintenance_locks_cover_every_root_and_hold_the_graph_lock(
    tmp_path: Path, synthetic_storage_env
) -> None:
    import fcntl

    env = synthetic_storage_env
    env.assert_injection()
    home = tmp_path / "home"
    db_path = _seed_source_sql(home)
    request, plan = _s204_planned_request(home, db_path)
    acquire = profile_migration.acquire_maintenance_locks
    locks = acquire(plan, timeout=2)
    graph = Path(plan.layout.graph_lock_path)
    graph_inode = os.lstat(graph).st_ino
    competing = os.open(graph, os.O_RDWR)
    try:
        assert list(locks.roots) == sorted(locks.roots, key=lambda item: os.fsencode(str(item)))
        assert locks.released is False
        with pytest.raises(BlockingIOError):
            fcntl.flock(competing, fcntl.LOCK_EX | fcntl.LOCK_NB)
    finally:
        os.close(competing)
        locks.release()
    assert os.lstat(graph).st_ino == graph_inode
    assert graph.exists()
    assert locks.released is True


def test_s204_a_symlinked_vector_store_is_never_a_lock_root(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    home = tmp_path / "home"
    db_path = _seed_source_sql(home)
    external = tmp_path / "external-lancedb"
    external.mkdir()
    vector = home / "data" / "lancedb"
    vector.symlink_to(external, target_is_directory=True)
    request, plan = _s204_planned_request(home, db_path)
    assert plan.layout.vector.local_path is not None
    acquire = profile_migration.acquire_maintenance_locks
    locks = acquire(plan, timeout=2)
    try:
        roots = [str(root) for root in locks.roots]
        assert all(os.path.islink(root) is False for root in roots)
        assert str(vector) not in roots
        assert str(vector.parent) in roots
    finally:
        locks.release()
    assert external.is_dir()


def test_s204_quiet_interval_is_two_seconds_under_pytest_and_five_in_a_cli_process(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    resolve = profile_migration.resolve_quiet_interval
    assert resolve() == 2.0
    assert resolve(3.5) == 3.5
    tree = Path(profile_migration.__file__).resolve().parents[2]
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "import memory_server.profile_migration as m; print(m.resolve_quiet_interval())",
        ],
        capture_output=True,
        text=True,
        cwd=str(tmp_path),
        env={
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "PYTHONPATH": f"{tree / 'src'}:{tree}",
            "PYTHONDONTWRITEBYTECODE": "1",
        },
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "5.0"


def test_s204_the_implementation_never_signals_a_process() -> None:
    import memory_server.storage_lock as storage_lock_module

    for path in (profile_migration.__file__, storage_lock_module.__file__):
        source = Path(path).read_text()
        for forbidden in ("os.kill", "import signal", "signal.SIG", "send_signal", "subprocess"):
            assert forbidden not in source, f"{path} references {forbidden}"


# ---------------------------------------------------------------------------
# FIX ROUND 1 (cross-provider review R1 / finding F2). The freshness gate of
# `validate_mutation_preconditions` and the lock stage of this same card could
# not be composed: `_plan_identity_digest` digested `plan.targets`, which
# includes the two coordination entries `acquire_maintenance_locks` CREATES
# (`root_lock`, `graph_lock`) and by design never unlinks, so after one lock
# cycle any caller-supplied plan was `E_PLAN_STALE` and the cause-specific
# fail-closed code (`E_OLD_WRITER_ACTIVE` / `E_WRITER_ACTIVE`) was masked.
# `_plan_identity_digest` now excludes exactly those two coordination labels
# while `plan.targets`, the writer inventory and the lock stage stay unchanged.
# These three nodes are BEHAVIOURAL at the fixed base 44bde33: the first fails
# with `E_PLAN_STALE` where it requires a valid result, and the second and third
# receive `E_PLAN_STALE` where they require the cause-specific refusal code.
# ---------------------------------------------------------------------------


def _s204_fix1_lock_cycle(plan: Any) -> None:
    """One real lock cycle: take every maintenance lock, then release it."""
    locks = profile_migration.acquire_maintenance_locks(plan, timeout=2)
    locks.release()


def _s204_fix1_live_upgraded_runtime(root: str, ready, release) -> None:
    """A real upgraded runtime: the shared runtime lock held on its data root."""
    import memory_server.storage_lock as storage_lock

    lock = storage_lock.RuntimeStorageLock.acquire(Path(root), timeout=3)
    ready.put(os.getpid())
    release.wait(10)
    lock.release()


def test_s204_fix1_a_lock_cycle_leaves_a_planned_plan_fresh(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    home = tmp_path / "home"
    db_path = _seed_source_sql(home)
    request, plan = _s204_planned_request(home, db_path)
    root_lock = Path(plan.layout.root_lock_path)
    graph_lock = Path(plan.layout.graph_lock_path)
    assert not root_lock.exists()
    assert not graph_lock.exists()
    _s204_fix1_lock_cycle(plan)
    assert root_lock.exists()
    assert graph_lock.exists()
    # Lock-then-validate: the caller's plan was built BEFORE the lock cycle, and
    # taking the locks must not have invalidated it.
    preconditions = profile_migration.validate_mutation_preconditions(request, plan=plan)
    assert preconditions.plan_digest == preconditions.replan_digest


def test_s204_fix1_the_first_validation_after_a_lock_cycle_names_the_live_old_writer(
    tmp_path: Path, synthetic_storage_env
) -> None:
    import multiprocessing

    env = synthetic_storage_env
    env.assert_injection()
    home = tmp_path / "home"
    db_path = _seed_source_sql(home)
    request, plan = _s204_planned_request(home, db_path)
    _s204_fix1_lock_cycle(plan)
    ready: multiprocessing.Queue[int] = multiprocessing.Queue()
    release = multiprocessing.Event()
    proc = multiprocessing.Process(target=_s204_old_writer, args=(str(db_path), ready, release))
    proc.start()
    try:
        child = ready.get(timeout=8)
        assert child != os.getpid()
        # FIRST validation, plan built before the lock cycle: it must name the
        # live writer, not the lock stage's own coordination growth.
        with pytest.raises(ValueError, match="E_OLD_WRITER_ACTIVE"):
            apply_profile_migration(plan)
        assert proc.is_alive()
    finally:
        release.set()
        proc.join(10)
        if proc.is_alive():
            proc.terminate()
    assert proc.exitcode == 0


def test_s204_fix1_a_live_upgraded_runtime_is_still_detected_after_a_lock_cycle(
    tmp_path: Path, synthetic_storage_env
) -> None:
    import multiprocessing

    import memory_server.storage_lock as storage_lock

    env = synthetic_storage_env
    env.assert_injection()
    home = tmp_path / "home"
    db_path = _seed_source_sql(home)
    request, plan = _s204_planned_request(home, db_path)
    _s204_fix1_lock_cycle(plan)
    ready: multiprocessing.Queue[int] = multiprocessing.Queue()
    release = multiprocessing.Event()
    proc = multiprocessing.Process(
        target=_s204_fix1_live_upgraded_runtime, args=(str(plan.layout.data_root), ready, release)
    )
    proc.start()
    try:
        child = ready.get(timeout=8)
        assert child != os.getpid()
        preconditions = profile_migration.validate_mutation_preconditions(request, plan=plan)
        holders = preconditions.writer_state["upgraded_holders"]
        assert [record["pid"] for record in holders] == [child]
        assert holders[0]["label_key"] == "root_lock"
        assert holders[0]["raw_label"] == str(Path(plan.layout.root_lock_path))
        assert holders[0]["classification"] == "upgraded_lock_holder"
        # Second, independent detection channel: the lock stage itself must still
        # refuse to hand over the entry the live runtime holds.
        with pytest.raises(storage_lock.StorageLockError) as timeout:
            profile_migration.acquire_maintenance_locks(plan, timeout=0.3)
        assert timeout.value.code == "E_LOCK_TIMEOUT"
        assert proc.is_alive()
    finally:
        release.set()
        proc.join(10)
        if proc.is_alive():
            proc.terminate()
    assert proc.exitcode == 0


# ---------------------------------------------------------------------------
# S2-05 -- immutable backup of regular artifacts and final legacy link entries
#
# DETAIL 9.1 / 10.1. PRE-FIX CLASSIFICATION OF THIS SECTION (filed with the RED
# in S2-05_EVIDENCE):
#
# * ``test_s205_replacing_a_recorded_coordination_entry_is_plan_stale`` is the
#   BEHAVIOURAL pre-fix node of the N1 pin. It drives only the already-approved
#   public surface (``plan_profile_migration`` +
#   ``validate_mutation_preconditions``) and fails at the parent commit because
#   the replacement is ACCEPTED -- "DID NOT RAISE ValueError matching
#   'E_PLAN_STALE'" -- where E_PLAN_STALE is required after the pin.
# * ``test_s205_the_shared_streaming_digest_covers_bytes_beyond_64k`` is the
#   BEHAVIOURAL node of the identity-digest contract: it exercises the real
#   streaming reader over a real descriptor at the parent commit. It PASSES
#   there (a guard, not a RED) and fails if an identity digest is ever replaced
#   by the bounded 64 KiB reader (routing-matrix residual F7).
# * ``test_s205_a_plan_recording_absent_coordination_entries_stays_fresh`` is the
#   behavioural NEGATIVE CONTROL of the pin: a legitimate lock cycle must not
#   read as a stale plan.
# * every other node drives the backup stage, which the parent commit does not
#   have AT ALL. Those are MISSING-CAPABILITY nodes: the tolerant accessors
#   return None/{} so the pre-fix failure is an assertion, never a collection
#   ImportError, a TypeError/KeyError from a helper, and never pytest.fail. The
#   behavioural coverage of the same contract is the three nodes above plus the
#   post-publication verification every backup node performs on real files.
# ---------------------------------------------------------------------------

S205_RUN_DIRECTORY_NAME = ".cmms-migrations"
S205_BACKUP_NAME = "backup"
S205_LINK_ENTRIES_NAME = "link-entries"
S205_REPORT_NAME = "backup-report.json"


def _s205_api(name: str) -> Any:
    """The S2-05 callable, or None -- MISSING CAPABILITY, never behavioural proof."""
    return getattr(profile_migration, name, None)


def _s205_home(tmp_path: Path) -> tuple[Path, Path]:
    """A synthetic profile: canonical SQL, a real graph file, a real vector tree."""
    home = tmp_path / "home"
    db_path = _s203_seed_canonical_db(home / "data" / "memory.db")
    (home / "data" / "graph.json").write_bytes(b'{"graph": true}\n')
    vector = home / "data" / "lancedb"
    (vector / "nested").mkdir(parents=True)
    (vector / "a.bin").write_bytes(b"A" * 4096)
    (vector / "nested" / "b.bin").write_bytes(b"B" * 8192)
    return home, db_path


def _s205_backup(plan: Any) -> Any:
    """create_run_backup(plan), or None when the capability does not exist."""
    api = _s205_api("create_run_backup")
    return api(plan) if api is not None else None


def _s205_backup_artifact(plan: Any, label: str, path: Path) -> Any:
    """backup_artifact(plan, label, path), or None when the capability is absent."""
    api = _s205_api("backup_artifact")
    return api(plan, label, path) if api is not None else None


def _s205_entries(result: Any) -> dict[str, Any]:
    """Backup entries keyed by artifact label; {} when the API is absent."""
    return {
        str(getattr(entry, "artifact", "")): entry
        for entry in (getattr(result, "entries", None) or ())
    }


def _s205_field(item: Any, name: str, default: Any = None) -> Any:
    """Tolerant field read: a missing result yields the default, never a KeyError."""
    return getattr(item, name, default)


def _s205_run_dir(home: Path, plan: Any) -> Path:
    return home / S205_RUN_DIRECTORY_NAME / plan.request.run_id


def _s205_file_sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _s205_mode(path: Path) -> int:
    return stat.S_IMODE(os.lstat(path).st_mode)


def _s205_legacy_home(tmp_path: Path) -> tuple[Path, Path, Path]:
    """A profile whose vector store is a final symlink, as the live layout is."""
    home, db_path = _s205_home(tmp_path)
    external = tmp_path / "external-lancedb"
    external.mkdir()
    (external / "referent.bin").write_bytes(b"REFERENT")
    vector = home / "data" / "lancedb"
    import shutil as _shutil

    _shutil.rmtree(vector)
    vector.symlink_to(external, target_is_directory=True)
    return home, db_path, external


def _s205_legacy_plan(home: Path, db_path: Path):
    """The approved S2-02 fixture request, extended with the apply fields."""
    request = _s202_request(
        home,
        source_sql=db_path,
        raw_config=_S202_CONFIG_BLOCK,
        mode="apply",
        confirm_target=str(home),
        stop_attestation="maintenance-ticket",
    )
    return request, plan_profile_migration(request)


def test_s205_replacing_a_recorded_coordination_entry_is_plan_stale(
    tmp_path: Path, synthetic_storage_env
) -> None:
    """N1 pin: a coordination entry the plan recorded as PRESENT keeps its inode.

    S2-04 excluded ``root_lock``/``graph_lock`` from ``_plan_identity_digest``
    because the lock stage creates them and never unlinks them, so a legitimate
    lock cycle must not read as a stale plan. That also stopped anything from
    noticing that ``root_lock`` had been REPLACED by a different ordinary regular
    file (``nlink == 1``), which the old digest refused. This node is the
    behavioural pre-fix RED for the pin: at the parent commit the replacement is
    accepted and the validation returns instead of refusing.
    """
    env = synthetic_storage_env
    env.assert_injection()
    home, db_path = _s205_home(tmp_path)
    request, plan = _s204_planned_request(home, db_path)
    root_lock = Path(plan.layout.root_lock_path)
    assert plan.targets["root_lock"].kind == "absent"
    assert not root_lock.exists()

    _s204_fix1_lock_cycle(plan)
    assert root_lock.is_file()
    # A plan built AFTER the lock cycle records the entry as PRESENT.
    planned = plan_profile_migration(request)
    assert planned.blockers == ()
    recorded = planned.targets["root_lock"]
    assert recorded.kind == "regular_file"
    assert (recorded.device, recorded.inode) == (
        os.lstat(root_lock).st_dev,
        os.lstat(root_lock).st_ino,
    )

    # The coordination entry is replaced by a DIFFERENT ordinary regular file.
    # The replacement is created first and renamed over the entry, so the new
    # inode is already allocated and the filesystem cannot hand back the freed
    # one (an unlink-then-create can and did reuse it, which would make this
    # node's premise, not its contract, the thing under test).
    replacement_path = home / "replacement-coordination-entry"
    replacement_path.write_bytes(b"replaced-coordination-entry\n")
    os.replace(replacement_path, root_lock)
    replacement = os.lstat(root_lock)
    assert replacement.st_nlink == 1
    assert replacement.st_ino != recorded.inode

    with pytest.raises(ValueError, match="E_PLAN_STALE"):
        profile_migration.validate_mutation_preconditions(request, plan=planned)


def test_s205_the_same_coordination_inode_keeps_a_planned_plan_fresh(
    tmp_path: Path, synthetic_storage_env
) -> None:
    """Negative control of the pin: a live lock re-acquisition is NOT drift.

    The pin must confirm the inode only. Re-acquiring the lock touches the same
    inode's size and mtime (the reason S2-04 removed the entries from the digest
    in the first place), and that must stay fresh -- otherwise the pin would make
    the lock stage and the freshness gate uncomposable again.
    """
    env = synthetic_storage_env
    env.assert_injection()
    home, db_path = _s205_home(tmp_path)
    request, plan = _s204_planned_request(home, db_path)
    _s204_fix1_lock_cycle(plan)
    planned = plan_profile_migration(request)
    root_lock = Path(planned.layout.root_lock_path)
    recorded = planned.targets["root_lock"]
    with root_lock.open("r+b") as handle:
        handle.write(b"ticket")
    after = os.lstat(root_lock)
    assert after.st_ino == recorded.inode
    assert (after.st_size, after.st_mtime_ns) != (recorded.size, recorded.mtime_ns)

    preconditions = profile_migration.validate_mutation_preconditions(request, plan=planned)
    assert preconditions.plan_digest == preconditions.replan_digest


def test_s205_a_plan_recording_absent_coordination_entries_stays_fresh(
    tmp_path: Path, synthetic_storage_env
) -> None:
    """Negative control: entries the plan recorded as ABSENT may appear later.

    The lock stage of this same card creates them, so a plan built before the
    lock cycle must stay fresh after it -- the behaviour S2-04 fix round 1
    established, kept here as the pin's other negative control.
    """
    env = synthetic_storage_env
    env.assert_injection()
    home, db_path = _s205_home(tmp_path)
    request, plan = _s204_planned_request(home, db_path)
    assert plan.targets["root_lock"].kind == "absent"
    assert plan.targets["graph_lock"].kind == "absent"
    _s204_fix1_lock_cycle(plan)
    assert Path(plan.layout.root_lock_path).exists()
    assert Path(plan.layout.graph_lock_path).exists()

    preconditions = profile_migration.validate_mutation_preconditions(request, plan=plan)
    assert preconditions.plan_digest == preconditions.replan_digest


def test_s205_regular_tree_backup_is_verified_byte_for_byte(
    tmp_path: Path, synthetic_storage_env
) -> None:
    """DETAIL 10.1: the regular tree is copied, verified and never overwritten.

    Every regular file entry must carry the source's SHA-256, mode, size and
    identity, its published copy must be byte-identical and mode 0600, the
    directory tree must be mirrored at 0700, the run must own a durable report,
    and the source tree must be byte-identical afterwards.
    """
    env = synthetic_storage_env
    env.assert_injection()
    home, db_path = _s205_home(tmp_path)
    request, plan = _s204_planned_request(home, db_path)
    graph_source = Path(plan.layout.graph_snapshot_path)
    vector_source = Path(plan.targets["vector"].lexical_path)
    assert graph_source == home / "data" / "graph.json"
    assert vector_source == home / "data" / "lancedb"
    run_dir = _s205_run_dir(home, plan)
    before = _tree_snapshot(home / "data")

    result = _s205_backup(plan)

    assert _s205_field(result, "run_dir") == str(run_dir)
    assert _s205_field(result, "run_id") == plan.request.run_id
    assert _s205_field(result, "report_path") == str(run_dir / S205_REPORT_NAME)
    entries = _s205_entries(result)
    assert entries, "no backup entry was recorded at all"
    for entry in entries.values():
        if _s205_field(entry, "kind") != "regular_file":
            continue
        source = Path(str(_s205_field(entry, "source_path")))
        published = run_dir / str(_s205_field(entry, "run_relative_path"))
        assert published.is_file()
        assert published.read_bytes() == source.read_bytes()
        assert _s205_file_sha(published) == _s205_field(entry, "sha256")
        assert _s205_file_sha(source) == _s205_field(entry, "sha256")
        assert _s205_field(entry, "size") == os.lstat(source).st_size
        assert _s205_field(entry, "mode") == stat.S_IMODE(os.lstat(source).st_mode)
        assert _s205_field(entry, "device") == os.lstat(source).st_dev
        assert _s205_field(entry, "inode") == os.lstat(source).st_ino
        assert _s205_field(entry, "present") is True
        assert _s205_field(entry, "digest_scope") == "content"
        assert _s205_mode(published) == 0o600
    # The regular target file and the directory tree are both recorded.
    graph_entry = entries.get("target:graph")
    assert _s205_field(graph_entry, "sha256") == _s205_file_sha(graph_source)
    tree_entry = entries.get("target:vector")
    assert _s205_field(tree_entry, "kind") == "directory"
    assert _s205_field(tree_entry, "digest_scope") == "listing"
    assert re.fullmatch(r"[0-9a-f]{64}", str(_s205_field(tree_entry, "sha256")))
    assert _s205_field(tree_entry, "device") == os.lstat(vector_source).st_dev
    assert _s205_field(tree_entry, "inode") == os.lstat(vector_source).st_ino
    assert _s205_mode(run_dir / S205_BACKUP_NAME) == 0o700
    assert _s205_mode(run_dir / S205_BACKUP_NAME / "vector") == 0o700
    assert _s205_mode(run_dir / S205_BACKUP_NAME / "vector" / "nested") == 0o700
    assert (
        run_dir / S205_BACKUP_NAME / "vector" / "nested" / "b.bin"
    ).read_bytes() == (vector_source / "nested" / "b.bin").read_bytes()
    assert "target:vector/nested/b.bin" in entries
    # The durable report records every entry, the SQL record and the digest.
    report_path = run_dir / S205_REPORT_NAME
    assert report_path.is_file()
    assert _s205_mode(report_path) == 0o600
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert len(report["entries"]) == len(entries)
    assert re.fullmatch(r"[0-9a-f]{64}", report["digest"])
    assert _s205_field(result, "digest") == report["digest"]
    assert report["run_id"] == plan.request.run_id
    assert report["sqlite"]["api"] == profile_migration.SNAPSHOT_API
    assert report["sqlite"]["path"] == plan.source_sql.lexical_path
    assert report["coordination"]["root_lock"]["kind"] == "absent"
    assert report["graph_lock"]["created"] is False
    # The source tree is byte-identical after the backup.
    assert _tree_snapshot(home / "data") == before


def test_s205_an_existing_run_backup_is_never_overwritten(
    tmp_path: Path, synthetic_storage_env
) -> None:
    """DETAIL 10.1: never overwrite a backup and never reuse a run id."""
    env = synthetic_storage_env
    env.assert_injection()
    home, db_path = _s205_home(tmp_path)
    request, plan = _s204_planned_request(home, db_path)
    run_dir = _s205_run_dir(home, plan)

    result = _s205_backup(plan)

    published = run_dir / S205_BACKUP_NAME / "graph.json"
    report_path = run_dir / S205_REPORT_NAME
    assert published.is_file()
    assert report_path.is_file()
    published_before = published.read_bytes()
    report_before = report_path.read_bytes()
    assert _s205_field(result, "report_path") == str(report_path)

    with pytest.raises(ValueError, match="E_BACKUP_COLLISION"):
        _s205_backup(plan)

    assert published.read_bytes() == published_before
    assert report_path.read_bytes() == report_before


def test_s205_interior_symlink_special_file_and_hardlink_are_refused(
    tmp_path: Path, synthetic_storage_env
) -> None:
    """DETAIL 10.1: reject an interior symlink, a special file and a hard link.

    Nothing may be published by a refused tree: the whole tree is staged and
    verified before any published name exists, so the backup area stays absent
    and the source tree stays byte-identical.
    """
    env = synthetic_storage_env
    env.assert_injection()
    home, db_path = _s205_home(tmp_path)
    request, plan = _s204_planned_request(home, db_path)
    vector_source = Path(plan.targets["vector"].lexical_path)
    run_dir = _s205_run_dir(home, plan)
    before = _tree_snapshot(home / "data")

    (vector_source / "escape").symlink_to(home / "data" / "graph.json")
    with pytest.raises(ValueError, match="E_PATH_FINAL_SYMLINK_UNSAFE"):
        _s205_backup_artifact(plan, "target:vector", vector_source)
    (vector_source / "escape").unlink()

    os.mkfifo(vector_source / "pipe")
    with pytest.raises(ValueError, match="E_PATH_SPECIAL_FILE"):
        _s205_backup_artifact(plan, "target:vector", vector_source)
    (vector_source / "pipe").unlink()

    os.link(vector_source / "a.bin", vector_source / "a-link.bin")
    assert os.lstat(vector_source / "a-link.bin").st_nlink == 2
    with pytest.raises(ValueError, match="E_PATH_HARDLINK_UNSAFE"):
        _s205_backup_artifact(plan, "target:vector", vector_source)
    (vector_source / "a-link.bin").unlink()

    assert not (run_dir / S205_BACKUP_NAME).exists()
    assert _tree_snapshot(home / "data") == before


def test_s205_a_final_legacy_symlink_is_backed_up_as_a_raw_link_entry(
    tmp_path: Path, synthetic_storage_env
) -> None:
    """DETAIL 10.1: a final legacy symlink is its exact RAW link entry only.

    The referent is never opened, enumerated, hashed, copied, modified or
    validated as a store; an absent entry is recorded explicitly instead of
    being silently skipped.
    """
    env = synthetic_storage_env
    env.assert_injection()
    home, db_path, external = _s205_legacy_home(tmp_path)
    request, plan = _s205_legacy_plan(home, db_path)
    vector = home / "data" / "lancedb"
    assert vector.is_symlink()
    raw_target = os.readlink(vector)
    legacy = [identity for identity in plan.legacy_projections if identity.kind == "symlink"]
    assert [identity.lexical_path for identity in legacy] == [str(vector)]
    assert [identity.raw_link_target for identity in legacy] == [raw_target]
    referent_before = _tree_snapshot(external)
    run_dir = _s205_run_dir(home, plan)

    result = _s205_backup(plan)

    entries = _s205_entries(result)
    link_entries = [
        entry for entry in entries.values() if _s205_field(entry, "kind") == "symlink"
    ]
    assert link_entries, "no link entry was recorded"
    assert {_s205_field(entry, "raw_link_target") for entry in link_entries} == {raw_target}
    for entry in link_entries:
        assert _s205_field(entry, "present") is True
        assert _s205_field(entry, "sha256") is None
        assert _s205_field(entry, "digest_scope") == ""
        published = run_dir / str(_s205_field(entry, "run_relative_path"))
        assert published.is_symlink()
        assert os.readlink(published) == raw_target
        assert str(published).startswith(str(run_dir / S205_BACKUP_NAME / S205_LINK_ENTRIES_NAME))
    # The referent was inventoried, never traversed, never copied.
    assert _tree_snapshot(external) == referent_before
    assert not (run_dir / S205_BACKUP_NAME / "vector").exists()

    # An absent entry is recorded explicitly.
    absent = _s205_backup_artifact(plan, "legacy:absent", home / "data" / "graph.json.legacy")
    assert isinstance(absent, tuple)
    absent_entry = _s205_entries(type("R", (), {"entries": absent})()).get("legacy:absent")
    assert _s205_field(absent_entry, "present") is False
    assert _s205_field(absent_entry, "kind") == "absent"
    assert _s205_field(absent_entry, "run_relative_path") is None
    assert _s205_field(absent_entry, "sha256") is None
    assert not (run_dir / S205_BACKUP_NAME / S205_LINK_ENTRIES_NAME / "legacy-absent").exists()


def test_s205_the_s2_03_source_snapshot_survives_the_backup_stage(
    tmp_path: Path, synthetic_storage_env
) -> None:
    """DETAIL 10.1: the S2-03 safety snapshot is not rewritten by the backup."""
    env = synthetic_storage_env
    env.assert_injection()
    home, db_path = _s205_home(tmp_path)
    request, plan = _s204_planned_request(home, db_path)
    run_dir = _s205_run_dir(home, plan)

    report, diagnostics = _s203_qualify(db_path, run_dir)
    assert _s203_section(report, "snapshot", "created") is True, diagnostics
    snapshot = run_dir / "snapshot" / "memory.db"
    assert snapshot.is_file()
    snapshot_before = snapshot.read_bytes()
    stat_before = os.lstat(snapshot)

    result = _s205_backup(plan)

    assert _s205_field(result, "report_path") == str(run_dir / S205_REPORT_NAME)
    stat_after = os.lstat(snapshot)
    assert snapshot.read_bytes() == snapshot_before
    assert (stat_after.st_ino, stat_after.st_size, stat_after.st_mtime_ns) == (
        stat_before.st_ino,
        stat_before.st_size,
        stat_before.st_mtime_ns,
    )
    verified = json.loads((run_dir / S205_REPORT_NAME).read_text(encoding="utf-8"))
    assert verified["sqlite"]["snapshot"]["kind"] == "regular_file"
    assert verified["sqlite"]["snapshot"]["sha256"] == hashlib.sha256(snapshot_before).hexdigest()
    assert profile_migration.verify_snapshot(snapshot)[0]["revision_accepted"] is True


def test_s205_backup_identity_digest_changes_for_a_byte_beyond_64k(
    tmp_path: Path, synthetic_storage_env
) -> None:
    """Acceptance 2 / residual F7: the backup identity digest is not 64 KiB-bounded.

    A byte beyond the first 64 KiB must move the digest the backup records and
    publishes, while the bounded 64 KiB reader cannot see it at all.
    """
    from memory_server.storage_lock import read_regular_file_nofollow

    env = synthetic_storage_env
    env.assert_injection()
    home, db_path = _s205_home(tmp_path)
    graph_source = home / "data" / "graph.json"
    graph_source.write_bytes(b"G" * 200_000)
    original = graph_source.read_bytes()
    request, plan = _s204_planned_request(home, db_path)

    first = _s205_backup(plan)
    first_digest = _s205_field(_s205_entries(first).get("target:graph"), "sha256")
    assert re.fullmatch(r"[0-9a-f]{64}", str(first_digest))
    assert first_digest == hashlib.sha256(original).hexdigest()

    with graph_source.open("r+b") as handle:
        handle.seek(150_000)
        handle.write(b"\xff")
    mutated = graph_source.read_bytes()
    assert mutated[:65536] == original[:65536]
    assert mutated != original

    second_plan = plan_profile_migration(replace(request, run_id="b" * 32))
    second = _s205_backup(second_plan)
    second_digest = _s205_field(_s205_entries(second).get("target:graph"), "sha256")
    assert second_digest == hashlib.sha256(mutated).hexdigest()
    assert second_digest != first_digest
    second_run_dir = _s205_run_dir(home, second_plan)
    assert (second_run_dir / S205_BACKUP_NAME / "graph.json").read_bytes() == mutated

    # The bounded 64 KiB reader cannot see that byte at all (residual F7).
    bounded = read_regular_file_nofollow(graph_source)
    assert len(bounded) <= 65536
    assert hashlib.sha256(bounded).hexdigest() == hashlib.sha256(original[:65536]).hexdigest()
    assert hashlib.sha256(bounded).hexdigest() != second_digest


def test_s205_the_shared_streaming_digest_covers_bytes_beyond_64k(tmp_path: Path) -> None:
    """BEHAVIOURAL at the parent commit: the identity reader reads the whole file.

    This is the behavioural node of the same identity contract the backup
    records: the streaming, size-bounded, looped fd-relative read the backup
    reuses must digest every byte, and the bounded 64 KiB reader must not.
    """
    from memory_server.storage_lock import read_regular_file_nofollow

    path = tmp_path / "identity.bin"
    path.write_bytes(b"S" * 200_000)
    streamed = profile_migration._streamed_digest
    with path.open("rb") as handle:
        first, refusal = streamed(handle.fileno(), os.fstat(handle.fileno()), artifact="probe")
    assert refusal is None
    assert first == hashlib.sha256(path.read_bytes()).hexdigest()

    with path.open("r+b") as handle:
        handle.seek(150_000)
        handle.write(b"\x00")
    with path.open("rb") as handle:
        second, refusal = streamed(handle.fileno(), os.fstat(handle.fileno()), artifact="probe")
    assert refusal is None
    assert second == hashlib.sha256(path.read_bytes()).hexdigest()
    assert second != first

    bounded = read_regular_file_nofollow(path)
    assert len(bounded) <= 65536
    assert hashlib.sha256(bounded).hexdigest() not in {first, second}


def test_s205_an_injected_copy_failure_leaves_the_source_and_backup_intact(
    tmp_path: Path, synthetic_storage_env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Evidence to clear: an injected COPY failure retains source and backup.

    The failure is injected at the staged write of the run's own staging
    directory. Nothing may appear under a published backup name, no report may be
    written, and the source tree must be byte-identical: a failure can never
    produce a partial backup or a target swap.
    """
    env = synthetic_storage_env
    env.assert_injection()
    home, db_path = _s205_home(tmp_path)
    request, plan = _s204_planned_request(home, db_path)
    run_dir = _s205_run_dir(home, plan)
    before = _tree_snapshot(home / "data")
    real_write = os.write

    def _failing_write(descriptor: int, data: bytes) -> int:
        try:
            target = os.readlink(f"/proc/self/fd/{descriptor}")
        except OSError:
            target = ""
        if "backup-tmp" in target:
            raise OSError(errno.EIO, "injected copy failure")
        return real_write(descriptor, data)

    monkeypatch.setattr(os, "write", _failing_write)
    try:
        with pytest.raises(ValueError, match="E_BACKUP_VERIFY"):
            _s205_backup(plan)
    finally:
        monkeypatch.undo()

    assert _tree_snapshot(home / "data") == before
    assert not (run_dir / S205_BACKUP_NAME).exists()
    assert not (run_dir / S205_REPORT_NAME).exists()


def test_s205_an_injected_fsync_failure_publishes_nothing(
    tmp_path: Path, synthetic_storage_env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Evidence to clear: an injected FSYNC failure publishes no backup at all."""
    env = synthetic_storage_env
    env.assert_injection()
    home, db_path = _s205_home(tmp_path)
    request, plan = _s204_planned_request(home, db_path)
    run_dir = _s205_run_dir(home, plan)
    before = _tree_snapshot(home / "data")
    real_fsync = os.fsync

    def _failing_fsync(descriptor: int) -> None:
        try:
            target = os.readlink(f"/proc/self/fd/{descriptor}")
        except OSError:
            target = ""
        if "backup-tmp" in target:
            raise OSError(errno.EIO, "injected fsync failure")
        return real_fsync(descriptor)

    monkeypatch.setattr(os, "fsync", _failing_fsync)
    try:
        with pytest.raises(ValueError, match="E_BACKUP_VERIFY"):
            _s205_backup(plan)
    finally:
        monkeypatch.undo()

    assert _tree_snapshot(home / "data") == before
    assert not (run_dir / S205_BACKUP_NAME).exists()
    assert not (run_dir / S205_REPORT_NAME).exists()


def test_s205_a_created_graph_lock_is_recorded_and_cleanup_removes_exactly_it(
    tmp_path: Path, synthetic_storage_env
) -> None:
    """DETAIL 10.1: the graph lock inode is never replaced, and its creation is
    recorded so post-unlock cleanup removes exactly what this run created."""
    import memory_server.storage_lock as storage_lock_module

    env = synthetic_storage_env
    env.assert_injection()
    home, db_path = _s205_home(tmp_path)
    request, plan = _s204_planned_request(home, db_path)
    graph_lock = Path(plan.layout.graph_lock_path)
    root_lock = Path(plan.layout.root_lock_path)
    assert not graph_lock.exists()
    run_dir = _s205_run_dir(home, plan)

    locks = profile_migration.acquire_maintenance_locks(plan, timeout=2)
    try:
        recorded = _s205_api("graph_lock_creation_record")
        record = recorded(plan, locks) if recorded is not None else {}
        identity = getattr(locks, "graph_lock_identity", None)
        assert identity is not None, "the lock owner cannot report its graph lock identity"
        held = os.lstat(graph_lock)
        assert (record.get("device"), record.get("inode")) == (held.st_dev, held.st_ino)
        assert (record.get("device"), record.get("inode")) == (identity.st_dev, identity.st_ino)
        assert record.get("created") is True
        assert record.get("held") is True
        assert record.get("path") == str(graph_lock)
        create_locked = _s205_api("create_run_backup")
        assert create_locked is not None
        result = create_locked(plan, locks=locks)
        assert _s205_field(result, "run_dir") == str(run_dir)
    finally:
        locks.release()

    # The backup stage never replaces the lock inode it recorded.
    assert os.lstat(graph_lock).st_ino == held.st_ino
    report = json.loads((run_dir / S205_REPORT_NAME).read_text(encoding="utf-8"))
    assert report["graph_lock"]["created"] is True
    assert (report["graph_lock"]["device"], report["graph_lock"]["inode"]) == (
        held.st_dev,
        held.st_ino,
    )
    assert _s205_field(result, "graph_lock")["created"] is True

    # Post-unlock cleanup removes exactly the entry this run created. The
    # replacement entry is created while the original still exists, so its inode
    # is guaranteed different: an inode freed by a preceding unlink can and does
    # get handed straight back, which would make this premise, not the contract,
    # the thing under test.
    foreign = home / "foreign-lock-entry"
    foreign.write_bytes(b"foreign\n")
    assert os.lstat(foreign).st_ino != held.st_ino

    remove = _s205_api("remove_created_lock_entry") or getattr(
        storage_lock_module, "remove_created_lock_entry", None
    )
    assert remove is not None, "no identity-checked lock cleanup exists"
    assert remove(report["graph_lock"]) is True
    assert not graph_lock.exists()
    assert root_lock.exists()

    # A REPLACED entry is refused: cleanup can never delete a foreign lock.
    os.replace(foreign, graph_lock)
    assert os.lstat(graph_lock).st_ino != held.st_ino
    with pytest.raises(storage_lock_module.StorageLockError) as replaced:
        remove(report["graph_lock"])
    assert replaced.value.code == "E_ARTIFACT_IDENTITY_CHANGED"
    assert graph_lock.exists()
    # A record that does not claim creation is refused too.
    with pytest.raises(storage_lock_module.StorageLockError) as not_created:
        remove(dict(report["graph_lock"], created=False))
    assert not_created.value.code == "E_LOCK_RELEASE_UNSAFE"


def test_s205_staging_and_publication_share_one_filesystem(
    tmp_path: Path, synthetic_storage_env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DETAIL 9.1: staging and publication must be on the source's filesystem.

    The refusal is exercised directly (a real second filesystem is not available
    under the sandbox), and the real backup call is spied on to prove it consults
    the check with the RUN directory's device for every artifact it copies.
    """
    env = synthetic_storage_env
    env.assert_injection()
    home, db_path = _s205_home(tmp_path)
    request, plan = _s204_planned_request(home, db_path)
    run_dir = _s205_run_dir(home, plan)

    check = _s205_api("_require_same_device")
    assert check is not None, "no same-filesystem check exists"
    run_device = os.lstat(plan.layout.data_root).st_dev
    check(run_device, run_device, artifact="same-device control")
    with pytest.raises(ValueError, match="E_CROSS_FILESYSTEM_PUBLICATION"):
        check(run_device, run_device + 1, artifact="injected device mismatch")

    calls: list[tuple[int, int, str]] = []
    real = profile_migration._require_same_device

    def _spy(run_device_arg: int, source_device_arg: int, *, artifact: str) -> None:
        calls.append((run_device_arg, source_device_arg, artifact))
        real(run_device_arg, source_device_arg, artifact=artifact)

    monkeypatch.setattr(profile_migration, "_require_same_device", _spy)
    result = _s205_backup(plan)

    assert calls, "the backup never checked the filesystem of its sources"
    assert all(run == source for run, source, _ in calls)
    assert all(call_run == run_device for call_run, _, _ in calls)
    assert _s205_field(result, "run_dir") == str(run_dir)


# ---------------------------------------------------------------------------
# S2-06 -- the forward state machine, the per-artifact publication and the
# CAPABILITY-GATED transition (DETAIL 9.3, 10.3, 10.4; routing-matrix
# residual F1, which is this card's entry condition AND its Stop).
#
# The card's contract in one paragraph: the ten forward checkpoints are
# reachable ONLY after real prerequisite evidence; the per-artifact publication
# emits exactly `prestate_revalidated`, `prestate_quarantined|absent`,
# `staging_published`, `parent_fsynced` and renames the quarantined prestate out
# of the way before renaming the staged entry into the vacant target, fsyncing
# BOTH parents; `published`/`verified`/`complete` may be entered ONLY when the
# verification seam REPORTS an implemented capability -- never because of what
# the seam RETURNED -- and the public `apply`/`resume`/`rollback` stay
# fail-closed while `verify_staged_projections` is the S0 stub
# (`projection_rebuild.py:68-70`), so no false end-to-end success is claimable.
#
# Evidence classification used by every node below, honestly:
#  * BEHAVIOURAL  -- the node drives an EXISTING public code path at BASE and
#    observes the wrong outcome (`Failed: DID NOT RAISE ...` or a real
#    assertion on a real result). Three nodes are behavioural: the manifest
#    accepting a gated checkpoint with no prerequisite evidence, the manifest
#    refusing the graph-lock creation record, and the link-entry path not
#    fsyncing the `backup` directory it created.
#  * MISSING-CAPABILITY -- the capability does not exist at BASE at all (there
#    is no forward state machine, no publication primitive, no capability
#    report). Those nodes fail on an explicit `... is not None` assertion, never
#    on an AttributeError/ImportError/TypeError from a helper, and never on
#    `pytest.fail`. Each one names the behavioural node that will carry the same
#    contract once the stage is wired (S3-06).
#  * CONTROL -- passes on BOTH sides and pins a boundary that must not move
#    (the entrypoints stay fail-closed; the primitives stay unwired).
# ---------------------------------------------------------------------------

S206_DIGEST = "ab" * 32
S206_RUN_DIRECTORY_NAME = ".cmms-migrations"
S206_STAGING_DIRECTORY_NAME = "staging"
S206_QUARANTINE_RELATIVE = ("quarantine", "prepublish")
S206_FORWARD_CHECKPOINTS: tuple[str, ...] = (
    "planned",
    "locked",
    "backed_up",
    "sqlite_snapshotted",
    "projections_built",
    "staged_verified",
    "publishing",
    "published",
    "verified",
    "complete",
)
S206_EVIDENCE_CODES: dict[str, str] = {
    "planned": "plan_digest",
    "locked": "lock_ownership",
    "backed_up": "backup_report",
    "sqlite_snapshotted": "snapshot_verification",
    "projections_built": "rebuild_result",
    "staged_verified": "staged_verification",
    "publishing": "publication_plan",
    "published": "publication_events",
    "verified": "reopen_verification",
    "complete": "manifest_update",
}
# The three the card names, plus `staged_verified`: DETAIL 10.3 IS the seam's
# contract, so recording a stub verdict as a completed checkpoint would be the
# false success this card exists to forbid. Disclosed in the summary.
S206_GATED_CHECKPOINTS: tuple[str, ...] = ("staged_verified", "published", "verified", "complete")
S206_PUBLICATION_EVENTS: tuple[str, ...] = (
    "prestate_revalidated",
    "prestate_quarantined",
    "prestate_absent",
    "staging_published",
    "parent_fsynced",
)
S206_PUBLISHED_SEQUENCE: tuple[str, ...] = (
    "prestate_revalidated",
    "prestate_quarantined",
    "staging_published",
    "parent_fsynced",
)
S206_STAGING_NAMES: dict[str, str] = {"vector": "lancedb", "graph": "graph.json"}


def _s206_api(name: str) -> Any:
    """The S2-06 callable, or None -- MISSING CAPABILITY, never behavioural proof."""
    return getattr(profile_migration, name, None)


def _s206_detail(checkpoint: str) -> dict[str, Any]:
    """A FRESH structurally valid evidence detail for one checkpoint."""
    code = S206_EVIDENCE_CODES[checkpoint]
    reopened = {"matches_staged": True, "device": 1, "inode": 2, "digest": S206_DIGEST}
    details: dict[str, dict[str, Any]] = {
        "plan_digest": {"plan_digest": S206_DIGEST, "config_digest": S206_DIGEST},
        "lock_ownership": {
            "held": True,
            "roots": ("/synthetic/root",),
            "identities": {"root_lock": (1, 2), "graph_lock": (1, 3)},
        },
        "backup_report": {"report_digest": S206_DIGEST, "entries": 3},
        "snapshot_verification": {
            "integrity": "ok",
            "revision": sorted(profile_migration.ACCEPTED_SQLITE_SCHEMA_REVISIONS)[0],
            "snapshot_device": 1,
        },
        "rebuild_result": {
            "completed_batches": 2,
            "vector_ids_digest": S206_DIGEST,
            "graph_nodes_digest": S206_DIGEST,
            "graph_edges_digest": S206_DIGEST,
        },
        "staged_verification": {
            "basis": "explicit_flag",
            "staging_digest": S206_DIGEST,
            "artifacts": ("vector", "graph"),
        },
        "publication_plan": {
            "targets": ("vector", "graph"),
            "pinned": {"vector": S206_DIGEST, "graph": S206_DIGEST},
        },
        "publication_events": {
            "artifacts": {
                "vector": list(S206_PUBLISHED_SEQUENCE),
                "graph": list(S206_PUBLISHED_SEQUENCE),
            }
        },
        "reopen_verification": {"artifacts": {"vector": dict(reopened), "graph": dict(reopened)}},
        "manifest_update": {
            "manifest_digest": S206_DIGEST,
            "manifest_bytes": 1024,
            "checkpoint": checkpoint,
        },
    }
    return details[code]


def _s206_evidence(checkpoint: str, **overrides: Any) -> Any:
    """A real-shaped CheckpointEvidence for one checkpoint, or None without the API."""
    factory = _s206_api("CheckpointEvidence")
    if factory is None:
        return None
    detail = _s206_detail(checkpoint)
    detail.update(overrides)
    return factory(
        checkpoint=checkpoint,
        code=S206_EVIDENCE_CODES[checkpoint],
        digest=S206_DIGEST,
        detail=detail,
    )


def _s206_capability(implemented: bool = True, basis: str = "explicit_flag") -> Any:
    factory = _s206_api("StagedVerificationCapability")
    if factory is None:
        return None
    return factory(implemented=implemented, basis=basis)


def _s206_validate(current: str, target: str, evidence: Any, capability: Any = None) -> Any:
    api = _s206_api("validate_forward_transition")
    if api is None:
        return None
    return api(current, target, evidence, capability=capability)


def _s206_advance(path: Path, target: str, evidence: Any = None, capability: Any = None) -> Any:
    api = _s206_api("advance_manifest_checkpoint")
    if api is None:
        return None
    return api(path, target, evidence=evidence, capability=capability)


def _s206_identity(path: Path) -> Any:
    """The no-follow identity of a REAL entry, exactly as the planner records it.

    ``mode`` is the FULL ``st_mode`` (the planner stores ``component.mode``), and
    a directory carries no size/mtime digest, which is why those stay None.
    """
    info = os.lstat(path)
    if stat.S_ISDIR(info.st_mode):
        return ArtifactIdentity(str(path), "directory", info.st_dev, info.st_ino, info.st_mode)
    return ArtifactIdentity(
        str(path),
        "regular_file",
        info.st_dev,
        info.st_ino,
        info.st_mode,
        info.st_size,
        info.st_mtime_ns,
        hashlib.sha256(path.read_bytes()).hexdigest(),
    )


def _s206_run_dir(home: Path, plan: Any) -> Path:
    return home / S206_RUN_DIRECTORY_NAME / plan.request.run_id


def test_s206_a_gated_checkpoint_event_is_refused_without_real_prerequisite_evidence(
    tmp_path: Path,
) -> None:
    """BEHAVIOURAL pre-fix RED: at BASE the manifest records `published` freely.

    DETAIL 9.3 makes `published` reachable only after both artifacts are present
    and reopenable, and acceptance 5 makes the transition capability-gated. The
    manifest is where a checkpoint is recorded, so at BASE this node drives the
    REAL `append_manifest_event` with a gated checkpoint and NO prerequisite
    evidence: it returns happily instead of refusing, which is the false success
    the card forbids. The refusal must also be fail-closed (byte-identical
    manifest) and must not disturb a NON-gated checkpoint (the control that keeps
    this node honest on both sides).
    """
    live, _before = _s2_pair(tmp_path)
    append = profile_migration.append_manifest_event

    appended = append(live, {"operation": "advance:locked", "checkpoint": "locked"})
    assert len(appended.events) == 1
    after_control = live.read_bytes()

    with pytest.raises(ValueError, match="E_CHECKPOINT_PREREQUISITE_MISSING"):
        append(live, {"operation": "publish:vector", "checkpoint": "published"})
    assert live.read_bytes() == after_control


def test_s206_the_s0_stub_seam_reports_no_capability_and_the_gate_stays_shut(
    tmp_path: Path,
) -> None:
    """MISSING-CAPABILITY at BASE: there is no capability report at all.

    RETARGETED BY S3-05 (disclosed): this node's subject was the S0 STUB. S3-05
    replaced that stub with the real verifier, so the two lines that asserted the
    stub's positional call contract (`verify_staged_projections(probe)` returning
    a valid-looking verdict) no longer describe the seam. The clause the node
    exists for is UNCHANGED and is now proven with a forged verdict: the gate
    reads the seam's CAPABILITY REPORT and never its return value.

    The report still says `implemented=False`: the real seam's contract is
    keyword-only, so S2-06's single-positional negative probe cannot qualify it
    and every gated transition below is still refused by cause. Opening the gate
    -- and `complete` end-to-end -- is S3-06's deliverable, which is exactly the
    hand-off this node records.

    Behavioural successor (S3-06): the node drives the public entrypoints once
    the stage is wired, so the same contract gets a behavioural RED then.
    """
    import asyncio

    import memory_server.projection_rebuild as projection_rebuild

    report = _s206_api("staged_verification_capability")
    assert report is not None, "no capability report for the verification seam exists"

    capability = report()
    assert capability.implemented is False
    assert capability.basis
    assert "not implemented" not in capability.basis
    assert capability.basis == "unimplemented", capability.basis

    # The REAL seam (S3-05) refuses an unverifiable staged input when it is
    # called with the contract it actually owns. Its REFUSAL is not what opens
    # the gate -- the report above is -- but the returned verdict must not be
    # readable as a success either.
    seam_verdict = asyncio.run(
        projection_rebuild.verify_staged_projections(
            snapshot_url=f"sqlite+aiosqlite:///{tmp_path / 'absent.db'}",
            staging_vector_path=tmp_path / "absent-staged-entry",
            staging_graph_path=tmp_path / "absent-graph.json",
        )
    )
    assert seam_verdict.valid is False, seam_verdict.errors

    # The gate reads the capability report ONLY: a verdict that CLAIMS
    # valid=True opens nothing.
    forged = projection_rebuild.ProjectionVerification(True)
    assert forged.valid is True

    for target, current in (
        ("staged_verified", "projections_built"),
        ("published", "publishing"),
        ("verified", "published"),
        ("complete", "verified"),
    ):
        with pytest.raises(ValueError, match="E_STAGED_VERIFICATION_CAPABILITY_MISSING"):
            _s206_validate(current, target, [_s206_evidence(target)], capability)


def test_s206_only_the_seams_own_capability_report_opens_the_gated_transitions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """MISSING-CAPABILITY at BASE: the gate itself does not exist.

    The gate is opened by the SEAM'S report and by nothing else: the explicit
    capability flag is the seam's own channel (DETAIL 10.3 implementation), and
    once the seam reports it, the gated checkpoints become reachable. Nothing in
    this node lets a caller assert `valid=True` and walk in.
    """
    import memory_server.projection_rebuild as projection_rebuild

    report = _s206_api("staged_verification_capability")
    assert report is not None, "no capability report for the verification seam exists"

    stub = report()
    assert stub.implemented is False
    for target, current in (
        ("staged_verified", "projections_built"),
        ("published", "publishing"),
        ("complete", "verified"),
    ):
        with pytest.raises(ValueError, match="E_STAGED_VERIFICATION_CAPABILITY_MISSING"):
            _s206_validate(current, target, [_s206_evidence(target)], stub)

    monkeypatch.setattr(projection_rebuild, "STAGED_VERIFICATION_IMPLEMENTED", True, raising=False)
    opened = report()
    assert opened.implemented is True
    assert opened.basis == "explicit_flag"
    for target, current in (
        ("staged_verified", "projections_built"),
        ("published", "publishing"),
        ("verified", "published"),
        ("complete", "verified"),
    ):
        assert _s206_validate(current, target, [_s206_evidence(target)], opened) == target


def test_s206_the_ten_forward_checkpoints_advance_only_with_their_own_evidence(
    tmp_path: Path,
) -> None:
    """MISSING-CAPABILITY at BASE: there is no forward state machine.

    DETAIL 9.3's chain is walked with the capability the seam REPORTS (the real
    S3 verifier is that card's deliverable): every step needs ITS OWN real
    prerequisite evidence, a missing evidence object is refused, an evidence
    object belonging to another checkpoint is refused, a skipped step is refused
    as out of order, and the gated steps refuse a seam that reports no
    capability. The last three steps are reached for real -- with both artifacts
    actually published and reopenable -- by the per-artifact node below; this node
    pins that `published` cannot be announced without the run's OWN recorded
    publication events even when the capability IS there.
    """
    run_dir = _s2_run_dir(tmp_path)
    live = run_dir / "manifest.json"
    profile_migration._write_manifest(live, _s2_manifest(checkpoint="planned", completed_steps=[]))

    assert _s206_api("advance_manifest_checkpoint") is not None, "no forward state machine exists"
    capable = _s206_capability(implemented=True, basis="explicit_flag")

    current = "planned"
    for target in S206_FORWARD_CHECKPOINTS[1:7]:
        with pytest.raises(ValueError, match="E_CHECKPOINT_PREREQUISITE_MISSING"):
            _s206_advance(live, target, evidence=None, capability=capable)
        with pytest.raises(ValueError, match="E_CHECKPOINT_EVIDENCE_INVALID"):
            _s206_advance(live, target, evidence=[_s206_evidence("planned")], capability=capable)
        manifest = _s206_advance(live, target, evidence=[_s206_evidence(target)], capability=capable)
        assert manifest is not None, "no forward state machine exists"
        assert manifest.checkpoint == target
        current = target

    assert current == "publishing"
    reloaded = profile_migration.load_manifest(live)
    assert [event.checkpoint for event in reloaded.events] == list(S206_FORWARD_CHECKPOINTS[1:7])
    assert reloaded.completed_steps[-1] == "publishing"

    with pytest.raises(ValueError, match="E_CHECKPOINT_OUT_OF_ORDER"):
        _s206_advance(live, "verified", evidence=[_s206_evidence("verified")])
    with pytest.raises(ValueError, match="E_STAGED_VERIFICATION_CAPABILITY_MISSING"):
        _s206_advance(
            live,
            "published",
            evidence=[_s206_evidence("published")],
            capability=_s206_capability(implemented=False, basis="unimplemented"),
        )
    with pytest.raises(ValueError, match="E_PUBLICATION_INCOMPLETE"):
        _s206_advance(live, "published", evidence=[_s206_evidence("published")], capability=capable)
    assert profile_migration.load_manifest(live).checkpoint == "publishing"


def test_s206_the_public_entrypoints_stay_fail_closed_while_the_seam_is_a_stub(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, synthetic_storage_env
) -> None:
    """CONTROL (passes on BOTH sides): no false end-to-end success is claimable.

    acceptance 1 and 5: while the verification seam is the S0 stub, `apply` and
    `resume` stay non-success and they do not start to proceed even when the seam DOES
    report an implemented capability -- the general `apply` stays closed until S3-06
    and no card may bypass that ordering (routing-matrix S2-06 split_further / the
    card's ordering note). The rollback leg asserted here is its absent-manifest
    refusal on a real tree: S2-08 did NOT put rollback behind this gate, because
    rollback publishes nothing and restores a pre-state the run already backed up, so
    no slice-level "not implemented" applies to it any more.
    """
    import memory_server.projection_rebuild as projection_rebuild

    env = synthetic_storage_env
    env.assert_injection()
    home, db_path = _s205_home(tmp_path)
    request, plan = _s204_planned_request(home, db_path)
    live = tmp_path / "run" / plan.request.run_id / "manifest.json"
    before = _tree_snapshot(tmp_path)

    entrypoints = (("apply_profile_migration", (plan,)),)
    for name, args in entrypoints:
        with pytest.raises(ValueError, match="E_MIGRATION_NOT_IMPLEMENTED"):
            getattr(profile_migration, name)(*args)

    # S2-07 and S2-08 moved ONE leg of this boundary each and this node is updated for
    # them, not weakened: `resume` repeats the preconditions and reads the run's own
    # manifest, and so does `rollback` at S2-08 -- the latter because it restores a
    # pre-state the run already backed up and publishes nothing, so it needs no staged
    # verification and does not touch the gate `apply` waits on. What this node exists
    # to pin is unchanged and still asserted below: neither entrypoint is a success,
    # neither touches anything, and neither classifies a run it cannot read.
    with pytest.raises(ValueError, match="E_MANIFEST_ABSENT"):
        profile_migration.resume_profile_migration(live, request)
    with pytest.raises(ValueError, match="E_MANIFEST_ABSENT"):
        profile_migration.rollback_profile_migration(live, request)

    monkeypatch.setattr(projection_rebuild, "STAGED_VERIFICATION_IMPLEMENTED", True, raising=False)
    with pytest.raises(ValueError, match="E_MIGRATION_NOT_IMPLEMENTED"):
        profile_migration.apply_profile_migration(plan)

    assert _tree_snapshot(tmp_path) == before


def test_s206_the_engine_primitives_are_not_wired_into_the_public_entrypoints(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, synthetic_storage_env
) -> None:
    """CONTROL (passes on BOTH sides) + the F7 status of the S2-05 stage.

    F7 of the S2-05 review asks for the ten missing-capability nodes to be
    re-anchored on the entrypoints "once the stage is wired into apply/resume/
    rollback". This card's ordering forbids that wiring: the entrypoints stay
    fail-closed until S3-06. The node therefore PINS the boundary as a control --
    the backup stage and the publication primitive are never reached from a
    public entrypoint and no run directory is created -- and states the residual
    honestly instead of letting a reader assume the wiring happened.
    """
    env = synthetic_storage_env
    env.assert_injection()
    home, db_path = _s205_home(tmp_path)
    request, plan = _s204_planned_request(home, db_path)

    reached: list[str] = []

    def _record(name: str) -> Any:
        def _spy(*args: Any, **kwargs: Any) -> Any:
            reached.append(name)
            return None

        return _spy

    for name in ("create_run_backup", "publish_artifact", "advance_manifest_checkpoint"):
        monkeypatch.setattr(profile_migration, name, _record(name), raising=False)

    with pytest.raises(ValueError, match="E_MIGRATION_NOT_IMPLEMENTED"):
        profile_migration.apply_profile_migration(plan)
    assert reached == []
    assert not _s206_run_dir(home, plan).exists()


# ---------------------------------------------------------------------------
# S2-06 -- the per-artifact publication itself (DETAIL 10.4) and the two
# carry-ins of the S2-05 review that live on this card's paths (F6, F9).
# ---------------------------------------------------------------------------


def _s206_prepare_staging(run_dir: Path, artifact: str, payload: bytes) -> Path:
    """A REAL prepared staging entry under ``<run>/staging/`` (DETAIL 9.1).

    ``staging/lancedb`` is a directory and ``staging/graph.json`` a regular file,
    so both the directory and the file branch of the swap are exercised on a real
    filesystem; nothing here is a mock and nothing is written outside the run
    directory (or /dev/shm for the cross-filesystem node).
    """
    staging = run_dir / S206_STAGING_DIRECTORY_NAME
    staging.mkdir(parents=True, exist_ok=True)
    entry = staging / S206_STAGING_NAMES[artifact]
    if artifact == "vector":
        (entry / "nested").mkdir(parents=True, exist_ok=True)
        (entry / "part.bin").write_bytes(payload)
        (entry / "nested" / "more.bin").write_bytes(b"nested-" + payload[:16])
    else:
        entry.write_bytes(payload)
    return entry


def _s206_publish(
    plan: Any, artifact: str, staged_identity: Any, run_dir: Path, manifest_path: Path | None = None
) -> Any:
    """publish_artifact(...), or None when the capability does not exist (BASE)."""
    api = _s206_api("publish_artifact")
    if api is None:
        return None
    return api(plan, artifact, staged_identity=staged_identity, run_dir=run_dir, manifest_path=manifest_path)


def _s206_fsync_spy(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record the real path behind every ``os.fsync`` descriptor, then fsync."""
    fsynced: list[str] = []
    real_fsync = os.fsync

    def _spy(descriptor: int) -> None:
        try:
            fsynced.append(os.path.realpath(f"/proc/self/fd/{descriptor}"))
        except OSError:
            fsynced.append("")
        return real_fsync(descriptor)

    monkeypatch.setattr(os, "fsync", _spy)
    return fsynced


def _s206_quarantine_parent(run_dir: Path) -> Path:
    return run_dir.joinpath(*S206_QUARANTINE_RELATIVE)


def _s206_publishing_manifest(plan: Any, run_dir: Path, name: str = "manifest.json") -> Path:
    """A real, durable manifest at the `publishing` checkpoint of this run."""
    run_dir.mkdir(parents=True, exist_ok=True)
    live = run_dir / name
    profile_migration._write_manifest(live, _s2_manifest(run_id=plan.request.run_id, checkpoint="publishing"))
    return live


def test_s206_publication_quarantines_the_pinned_prestate_then_renames_staging_into_the_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, synthetic_storage_env
) -> None:
    """MISSING-CAPABILITY at BASE: no per-artifact publication primitive exists.

    Behavioural successor (S3-06): the same node drives the entrypoint once the
    stage is wired into `apply`, so the swap contract gets a behavioural RED
    then. At HEAD this node is the real thing on a real filesystem: the pinned
    prestate is quarantined (retained byte for byte), the staged directory is
    renamed into the now-vacant target (same inode -- a rename, not a copy), both
    parents are fsynced, and the four DETAIL 9.3 publishing events are emitted in
    order for THIS artifact only.
    """
    env = synthetic_storage_env
    env.assert_injection()
    home, db_path = _s205_home(tmp_path)
    request, plan = _s205_legacy_plan(home, db_path)
    run_dir = _s206_run_dir(home, plan)
    run_dir.mkdir(parents=True, exist_ok=True)
    target = Path(plan.targets["vector"].lexical_path)
    old_bytes = (target / "a.bin").read_bytes()
    old_inode = plan.targets["vector"].inode

    payload = b"STAGED-VECTOR-PAYLOAD" * 8
    staged = _s206_prepare_staging(run_dir, "vector", payload)
    staged_identity = _s206_identity(staged)
    fsynced = _s206_fsync_spy(monkeypatch)

    result = _s206_publish(plan, "vector", staged_identity, run_dir)
    assert result is not None, "no per-artifact publication primitive exists at this commit"

    assert [event.event for event in result.events] == list(S206_PUBLISHED_SEQUENCE)
    assert {event.artifact for event in result.events} == {"vector"}
    assert result.prestate == "quarantined"

    quarantine_parent = _s206_quarantine_parent(run_dir)
    quarantined = run_dir / result.quarantine_relative_path
    assert quarantined.parent == quarantine_parent
    assert quarantined.is_dir()
    assert (quarantined / "a.bin").read_bytes() == old_bytes
    assert quarantined.stat().st_ino == old_inode

    assert not staged.exists()
    assert (target / "part.bin").read_bytes() == payload
    assert (target / "nested" / "more.bin").read_bytes() == b"nested-" + payload[:16]
    assert target.stat().st_ino == staged_identity.inode

    parents = {os.path.realpath(item) for item in result.parents_fsynced}
    assert parents == {
        os.path.realpath(str(target.parent)),
        os.path.realpath(str(quarantine_parent)),
    }
    assert parents <= set(fsynced)


def test_s206_an_absent_prestate_is_recorded_as_absence_and_never_quarantined(
    tmp_path: Path, synthetic_storage_env
) -> None:
    """MISSING-CAPABILITY at BASE (same behavioural successor as the node above).

    DETAIL 10.4 step 2: an absent prestate is an explicit ABSENCE event, not a
    silent skip and not a quarantine entry; the staged entry still lands in the
    vacant target name.
    """
    env = synthetic_storage_env
    env.assert_injection()
    home, db_path = _s205_home(tmp_path)
    target = home / "data" / "graph.json"
    target.unlink()
    request, plan = _s205_legacy_plan(home, db_path)
    assert plan.targets["graph"].kind == "absent"
    run_dir = _s206_run_dir(home, plan)
    run_dir.mkdir(parents=True, exist_ok=True)

    payload = b'{"staged": true}\n'
    staged = _s206_prepare_staging(run_dir, "graph", payload)
    staged_identity = _s206_identity(staged)

    result = _s206_publish(plan, "graph", staged_identity, run_dir)
    assert result is not None, "no per-artifact publication primitive exists at this commit"
    assert [event.event for event in result.events] == [
        "prestate_revalidated",
        "prestate_absent",
        "staging_published",
        "parent_fsynced",
    ]
    assert result.prestate == "absent"
    assert not result.quarantine_relative_path
    assert not _s206_quarantine_parent(run_dir).exists()
    assert target.read_bytes() == payload
    assert not staged.exists()


def test_s206_a_stale_or_replaced_pinned_prestate_is_refused_before_anything_is_swapped(
    tmp_path: Path, synthetic_storage_env
) -> None:
    """MISSING-CAPABILITY at BASE; the stop condition is "stale prestate".

    DETAIL 10.4 step 1 revalidates the pinned prestate BEFORE any rename. The
    replacement here is a real one: a different inode already allocated under a
    temporary name and renamed over the target, so the filesystem cannot hand the
    freed inode back and the premise of the node cannot silently invert.
    """
    env = synthetic_storage_env
    env.assert_injection()
    home, db_path = _s205_home(tmp_path)
    request, plan = _s205_legacy_plan(home, db_path)
    run_dir = _s206_run_dir(home, plan)
    run_dir.mkdir(parents=True, exist_ok=True)
    target = Path(plan.targets["graph"].lexical_path)
    staged = _s206_prepare_staging(run_dir, "graph", b"staged-graph\n")
    staged_identity = _s206_identity(staged)

    api = _s206_api("publish_artifact")
    assert api is not None, "no per-artifact publication primitive exists at this commit"

    replacement = home / "replacement-graph.json"
    replacement.write_bytes(b'{"replaced": true}\n')
    os.replace(replacement, target)
    assert os.lstat(target).st_ino != plan.targets["graph"].inode

    with pytest.raises(ValueError, match="E_PUBLICATION_PRESTATE_CHANGED"):
        api(plan, "graph", staged_identity=staged_identity, run_dir=run_dir)
    assert target.read_bytes() == b'{"replaced": true}\n'
    assert staged.exists()
    assert not _s206_quarantine_parent(run_dir).exists()


def test_s206_injected_failures_before_and_after_each_operation_retain_the_prestate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, synthetic_storage_env
) -> None:
    """MISSING-CAPABILITY at BASE; the injected failures are REAL.

    The card's clearance evidence demands failures injected before AND after each
    operation. Three are exercised here on real filesystems, each with the
    manifest's own event chain inspected afterwards:

    * the quarantine rename fails  -> nothing moved, no quarantine entry, and the
      event chain stops at `prestate_revalidated`;
    * the publish rename fails     -> the OLD entry is retained in the run
      quarantine (no data loss), the target is vacant, the staged entry still
      exists, and `staging_published` was never recorded;
    * the failure is the REAL `os.rename`, not a helper of this test.
    """
    env = synthetic_storage_env
    env.assert_injection()
    home, db_path = _s205_home(tmp_path)
    request, plan = _s205_legacy_plan(home, db_path)
    run_dir = _s206_run_dir(home, plan)
    run_dir.mkdir(parents=True, exist_ok=True)
    live = _s206_publishing_manifest(plan, run_dir)
    target = Path(plan.targets["vector"].lexical_path)
    old_bytes = (target / "a.bin").read_bytes()
    quarantine_parent = _s206_quarantine_parent(run_dir)

    api = _s206_api("publish_artifact")
    assert api is not None, "no per-artifact publication primitive exists at this commit"
    staged = _s206_prepare_staging(run_dir, "vector", b"first-attempt")
    staged_identity = _s206_identity(staged)

    real_rename = os.rename

    def _always_fail(source: Any, destination: Any, *args: Any, **kwargs: Any) -> None:
        raise OSError(errno.EIO, "injected rename failure")

    monkeypatch.setattr(os, "rename", _always_fail)
    with pytest.raises(ValueError, match="E_PUBLICATION_RENAME_FAILED"):
        api(plan, "vector", staged_identity=staged_identity, run_dir=run_dir, manifest_path=live)
    monkeypatch.setattr(os, "rename", real_rename)

    assert (target / "a.bin").read_bytes() == old_bytes
    assert staged.is_dir()
    # Nothing was quarantined: the run-owned quarantine DIRECTORY may have been
    # created by the attempted step, but no entry ever landed in it.
    if quarantine_parent.exists():
        assert list(quarantine_parent.iterdir()) == []
    assert [event.operation for event in profile_migration.load_manifest(live).events] == [
        "vector.prestate_revalidated"
    ]

    seen: list[int] = []

    def _fail_second(source: Any, destination: Any, *args: Any, **kwargs: Any) -> None:
        seen.append(1)
        if len(seen) == 2:
            raise OSError(errno.EIO, "injected rename failure")
        return real_rename(source, destination, *args, **kwargs)

    # A second, fresh run manifest so the chain inspected below belongs to this
    # attempt only (the first attempt already recorded its revalidation).
    second = _s206_publishing_manifest(plan, run_dir, name="manifest-phase2.json")
    monkeypatch.setattr(os, "rename", _fail_second)
    with pytest.raises(ValueError, match="E_PUBLICATION_RENAME_FAILED"):
        api(plan, "vector", staged_identity=staged_identity, run_dir=run_dir, manifest_path=second)
    monkeypatch.setattr(os, "rename", real_rename)

    quarantined = sorted(quarantine_parent.iterdir())
    assert len(quarantined) == 1
    assert (quarantined[0] / "a.bin").read_bytes() == old_bytes
    assert not target.exists()
    assert staged.is_dir()
    operations = [event.operation for event in profile_migration.load_manifest(second).events]
    assert operations == ["vector.prestate_revalidated", "vector.prestate_quarantined"]


def test_s206_a_real_second_filesystem_refuses_publication_and_same_device_publishes(
    tmp_path: Path, synthetic_storage_env
) -> None:
    """MISSING-CAPABILITY at BASE; the STOP condition is exercised on REAL devices.

    `/dev/shm` is device 28 and the run directory here is device 66306, so the
    refusal is produced by the kernel on a genuine cross-device rename of a REAL
    prepared staging entry -- no mocking of `st_dev` anywhere. The same-device
    control then publishes the same staged layout, which also proves the refused
    attempt left no stale prestate behind. (S2-05's fix round established that
    the certified runner grants /dev/shm; the docstring that claimed otherwise
    inside the frozen S2-05 node is recorded as residual N-1 there.)
    """
    import shutil as _shutil
    import tempfile

    env = synthetic_storage_env
    env.assert_injection()
    home, db_path = _s205_home(tmp_path)
    request, plan = _s205_legacy_plan(home, db_path)
    target = Path(plan.targets["graph"].lexical_path)
    api = _s206_api("publish_artifact")
    assert api is not None, "no per-artifact publication primitive exists at this commit"
    prestate = target.read_bytes()

    foreign_root = Path(tempfile.mkdtemp(dir="/dev/shm", prefix="s206-device-"))
    foreign_run = foreign_root / plan.request.run_id
    try:
        assert os.stat(foreign_root).st_dev != os.stat(home).st_dev
        foreign_run.mkdir()
        staged = _s206_prepare_staging(foreign_run, "graph", b'{"cross-device": true}\n')
        with pytest.raises(ValueError, match="E_CROSS_FILESYSTEM_PUBLICATION"):
            api(plan, "graph", staged_identity=_s206_identity(staged), run_dir=foreign_run)
        assert target.read_bytes() == prestate
        assert staged.exists()
        foreign_quarantine = _s206_quarantine_parent(foreign_run)
        if foreign_quarantine.exists():
            assert list(foreign_quarantine.iterdir()) == []
    finally:
        _shutil.rmtree(foreign_root, ignore_errors=True)

    same_run = _s206_run_dir(home, plan)
    same_run.mkdir(parents=True, exist_ok=True)
    staged_same = _s206_prepare_staging(same_run, "graph", b'{"same-device": true}\n')
    result = api(plan, "graph", staged_identity=_s206_identity(staged_same), run_dir=same_run)
    assert result is not None
    assert result.prestate == "quarantined"
    assert target.read_bytes() == b'{"same-device": true}\n'


def test_s206_a_failure_between_the_rename_and_the_manifest_update_is_not_a_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, synthetic_storage_env
) -> None:
    """MISSING-CAPABILITY at BASE; the stop condition is "failure between rename
    and manifest update" and it is the card's most dangerous window.

    The manifest write of `staging_published` is injected to fail AFTER the
    rename has already landed: the target holds the new bytes, the old entry is
    retained in the run quarantine, the event chain stops at
    `prestate_quarantined` -- and `published` remains UNREACHABLE even with the
    seam reporting an implemented capability, because the state machine reads the
    manifest's OWN recorded events, not the caller's claim.
    """
    import memory_server.projection_rebuild as projection_rebuild

    env = synthetic_storage_env
    env.assert_injection()
    home, db_path = _s205_home(tmp_path)
    request, plan = _s205_legacy_plan(home, db_path)
    run_dir = _s206_run_dir(home, plan)
    run_dir.mkdir(parents=True, exist_ok=True)
    live = _s206_publishing_manifest(plan, run_dir)
    target = Path(plan.targets["graph"].lexical_path)
    old_bytes = target.read_bytes()

    api = _s206_api("publish_artifact")
    assert api is not None, "no per-artifact publication primitive exists at this commit"
    staged = _s206_prepare_staging(run_dir, "graph", b'{"half-published": true}\n')
    staged_identity = _s206_identity(staged)

    real_write = profile_migration._write_manifest
    calls: list[int] = []

    def _fail_after_the_rename(manifest_target: Path, manifest: Any) -> Any:
        calls.append(1)
        if len(calls) >= 3:
            raise ValueError("E_MANIFEST_WRITE_INJECTED")
        return real_write(manifest_target, manifest)

    monkeypatch.setattr(profile_migration, "_write_manifest", _fail_after_the_rename)
    with pytest.raises(ValueError, match="E_MANIFEST_WRITE_INJECTED"):
        api(plan, "graph", staged_identity=staged_identity, run_dir=run_dir, manifest_path=live)
    monkeypatch.setattr(profile_migration, "_write_manifest", real_write)

    assert target.read_bytes() == b'{"half-published": true}\n'
    quarantined = sorted(_s206_quarantine_parent(run_dir).iterdir())
    assert len(quarantined) == 1
    assert quarantined[0].read_bytes() == old_bytes
    operations = [event.operation for event in profile_migration.load_manifest(live).events]
    assert operations == ["graph.prestate_revalidated", "graph.prestate_quarantined"]

    monkeypatch.setattr(projection_rebuild, "STAGED_VERIFICATION_IMPLEMENTED", True, raising=False)
    with pytest.raises(ValueError, match="E_PUBLICATION_INCOMPLETE"):
        _s206_advance(live, "published", evidence=[_s206_evidence("published")])


def test_s206_publishing_is_per_artifact_and_never_a_multi_artifact_atomic_claim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, synthetic_storage_env
) -> None:
    """MISSING-CAPABILITY at BASE; DETAIL 9.3 + acceptance 4.

    The two artifacts really are published one after the other. After the vector
    swap only, `published` is refused as incomplete -- even with the seam
    reporting an implemented capability; after the graph swap both artifacts are
    reopenable and only then does the chain reach `published`, then `verified`
    (reopen exact) and `complete` (durable manifest update). The per-artifact
    results carry their OWN events, which is the explicit refusal of a
    multi-artifact atomic claim.
    """
    import memory_server.projection_rebuild as projection_rebuild

    env = synthetic_storage_env
    env.assert_injection()
    home, db_path = _s205_home(tmp_path)
    request, plan = _s205_legacy_plan(home, db_path)
    run_dir = _s206_run_dir(home, plan)
    run_dir.mkdir(parents=True, exist_ok=True)
    live = _s206_publishing_manifest(plan, run_dir)

    publish = _s206_api("publish_artifact")
    reopen = _s206_api("reopen_published_artifact")
    assert publish is not None, "no per-artifact publication primitive exists at this commit"
    assert reopen is not None, "no reopen verification of a published entry exists"
    monkeypatch.setattr(projection_rebuild, "STAGED_VERIFICATION_IMPLEMENTED", True, raising=False)

    published: dict[str, Any] = {}
    for artifact in ("vector", "graph"):
        staged = _s206_prepare_staging(run_dir, artifact, f"staged-{artifact}".encode() * 4)
        published[artifact] = publish(
            plan, artifact, staged_identity=_s206_identity(staged), run_dir=run_dir, manifest_path=live
        )
        assert published[artifact] is not None
        assert {event.artifact for event in published[artifact].events} == {artifact}
        if artifact == "vector":
            with pytest.raises(ValueError, match="E_PUBLICATION_INCOMPLETE"):
                _s206_advance(live, "published", evidence=[_s206_evidence("published")])

    assert _s206_advance(live, "published", evidence=[_s206_evidence("published")]) is not None

    reopened = {label: reopen(plan, label, published[label]) for label in ("vector", "graph")}
    assert all(report["matches_staged"] is True for report in reopened.values())
    assert _s206_advance(live, "verified", evidence=[_s206_evidence("verified", artifacts=reopened)]) is not None
    manifest_update = {
        "manifest_digest": hashlib.sha256(live.read_bytes()).hexdigest(),
        "manifest_bytes": live.stat().st_size,
    }
    final = _s206_advance(live, "complete", evidence=[_s206_evidence("complete", **manifest_update)])
    assert final is not None
    assert final.checkpoint == "complete"
    assert profile_migration.load_manifest(live).checkpoint == "complete"


def test_s206_the_published_entry_is_reopened_exact_and_a_post_swap_replacement_is_refused(
    tmp_path: Path, synthetic_storage_env
) -> None:
    """MISSING-CAPABILITY at BASE; acceptance 4 ("reopen exact verification").

    The reopen compares what is AT the final name now with the pinned staged
    identity: the rename preserves the inode, so the device/inode must be the
    staged entry's own, and a regular file must still digest to the staged bytes.
    A replacement planted after the swap therefore cannot be recorded as
    `verified`.
    """
    env = synthetic_storage_env
    env.assert_injection()
    home, db_path = _s205_home(tmp_path)
    request, plan = _s205_legacy_plan(home, db_path)
    run_dir = _s206_run_dir(home, plan)
    run_dir.mkdir(parents=True, exist_ok=True)
    target = Path(plan.targets["graph"].lexical_path)

    payload = b'{"reopen": "exact"}\n'
    staged = _s206_prepare_staging(run_dir, "graph", payload)
    staged_identity = _s206_identity(staged)
    result = _s206_publish(plan, "graph", staged_identity, run_dir)
    assert result is not None, "no per-artifact publication primitive exists at this commit"

    reopen = _s206_api("reopen_published_artifact")
    assert reopen is not None, "no reopen verification of a published entry exists"
    report = reopen(plan, "graph", result)
    assert report["matches_staged"] is True
    assert report["device"] == staged_identity.device
    assert report["inode"] == staged_identity.inode
    assert report["digest"] == staged_identity.sha256

    replacement = home / "replacement-after-swap.json"
    replacement.write_bytes(b'{"swapped": true}\n')
    os.replace(replacement, target)
    with pytest.raises(ValueError, match="E_PUBLICATION_REOPEN_MISMATCH"):
        reopen(plan, "graph", result)


def test_s206_the_manifest_carries_and_round_trips_the_graph_lock_creation_record(
    tmp_path: Path, synthetic_storage_env
) -> None:
    """BEHAVIOURAL pre-fix RED (S2-05 review F9, DETAIL 10.1).

    DETAIL 10.1: "If migration created an absent graph lock, manifest records
    that fact for post-unlock rollback cleanup." At BASE the manifest schema has
    no such field and the REAL strict deserializer refuses a manifest that
    carries it (`E_MANIFEST_SCHEMA`), so the DETAIL sentence was unimplementable;
    at HEAD the record round-trips as durable typed state. The record itself is
    produced by the real S2-05 lock path, not hand-written.
    """
    env = synthetic_storage_env
    env.assert_injection()
    home, db_path = _s205_home(tmp_path)
    request, plan = _s205_legacy_plan(home, db_path)
    run_dir = home / S206_RUN_DIRECTORY_NAME / plan.request.run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    live = run_dir / "manifest.json"

    locks = profile_migration.acquire_maintenance_locks(plan, timeout=2)
    try:
        record = dict(profile_migration.graph_lock_creation_record(plan, locks))
    finally:
        locks.release()
    assert record["created"] is True
    assert record["held"] is True

    payload = asdict(_s2_manifest(run_id=plan.request.run_id))
    payload["graph_lock"] = record
    _s2_write_json(live, payload)

    loaded = profile_migration.load_manifest(live)
    assert dict(loaded.graph_lock) == record


def test_s206_a_durable_manifest_update_records_the_graph_lock_and_is_reopened(
    tmp_path: Path, synthetic_storage_env
) -> None:
    """MISSING-CAPABILITY at BASE: nothing writes the record into the manifest.

    The durable update goes through the real atomic manifest writer and is then
    RE-READ from disk, so the claim is about the file, not about a Python object
    that happens to be in memory.
    """
    env = synthetic_storage_env
    env.assert_injection()
    home, db_path = _s205_home(tmp_path)
    request, plan = _s205_legacy_plan(home, db_path)
    run_dir = home / S206_RUN_DIRECTORY_NAME / plan.request.run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    live = run_dir / "manifest.json"
    profile_migration._write_manifest(live, _s2_manifest(run_id=plan.request.run_id))

    api = _s206_api("record_graph_lock_creation")
    assert api is not None, "no manifest record of the created graph lock exists"

    locks = profile_migration.acquire_maintenance_locks(plan, timeout=2)
    try:
        record = dict(profile_migration.graph_lock_creation_record(plan, locks))
    finally:
        locks.release()

    updated = api(live, record)
    assert dict(updated.graph_lock) == record
    on_disk = profile_migration.load_manifest(live)
    assert dict(on_disk.graph_lock) == record
    assert str(record["path"]) in live.read_text(encoding="utf-8")


def test_s206_the_link_entry_backup_fsyncs_the_created_backup_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, synthetic_storage_env
) -> None:
    """BEHAVIOURAL pre-fix RED (S2-05 review F6).

    In the link-entry branch only the inner `link-entries` descriptor is fsynced
    at BASE, so the `backup` directory entry the same call just created can be
    lost by a crash while the symlink survives. The node observes the REAL
    `os.fsync` calls of the real backup path -- no assertion is made about a
    mock -- and only the link path is run, so the regular-file path's fsync of
    the same directory cannot mask the gap.
    """
    env = synthetic_storage_env
    env.assert_injection()
    home, db_path, _external = _s205_legacy_home(tmp_path)
    request, plan = _s205_legacy_plan(home, db_path)
    vector = home / "data" / "lancedb"
    assert vector.is_symlink()
    run_dir = _s205_run_dir(home, plan)
    fsynced = _s206_fsync_spy(monkeypatch)

    entries = _s205_backup_artifact(plan, "legacy:0", vector)
    assert entries is not None, "no backup stage exists at this commit"
    assert [entry.kind for entry in entries] == ["symlink"]

    backup_dir = os.path.realpath(str(run_dir / "backup"))
    link_entries = os.path.realpath(str(run_dir / "backup" / "link-entries"))
    assert link_entries in fsynced
    assert backup_dir in fsynced, (
        "the `backup` directory entry created for the link entry was never fsynced"
    )


# ---------------------------------------------------------------------------
# S2-07 -- the deterministic resume classifier for the checkpoint and the
# target/staging/quarantine triads (DETAIL 9.3, 10.5), plus the two S2-06
# review carry-ins this card owns: R3 (the crash states the classifier must
# classify, and what it may honestly claim about them) and R7/F-1 (the durable
# record could name an UNQUALIFIED evidence object).
# ---------------------------------------------------------------------------

S207_STAGING_DIRECTORY_NAME = "staging"
S207_SEQUENCE_QUARANTINED: tuple[str, ...] = (
    "prestate_revalidated",
    "prestate_quarantined",
    "staging_published",
    "parent_fsynced",
)
S207_SEQUENCE_ABSENT: tuple[str, ...] = (
    "prestate_revalidated",
    "prestate_absent",
    "staging_published",
    "parent_fsynced",
)
# Crash boundaries of ONE artifact's DETAIL 9.3 event sequence. Each entry says
# which artifact crashed, what the run's OWN chain recorded by then, the REAL
# on-disk triad that boundary leaves, and the single event the classifier must
# derive as the next operation.
S207_CRASH_BOUNDARIES: dict[str, dict[str, Any]] = {
    "prestate_revalidated": {
        "artifact": "vector",
        "prestate_absent": False,
        "recorded": ("prestate_revalidated",),
        "state": "prestate_intact",
        "quarantine_present": False,
        "staging_present": True,
        "next_event": "prestate_quarantined",
    },
    "prestate_quarantined": {
        "artifact": "vector",
        "prestate_absent": False,
        "recorded": ("prestate_revalidated", "prestate_quarantined"),
        "state": "prestate_quarantined",
        "quarantine_present": True,
        "staging_present": True,
        "next_event": "staging_published",
    },
    "prestate_absent": {
        "artifact": "graph",
        "prestate_absent": True,
        "recorded": ("prestate_revalidated", "prestate_absent"),
        "state": "prestate_absent",
        "quarantine_present": False,
        "staging_present": True,
        "next_event": "staging_published",
    },
    "staging_published": {
        "artifact": "vector",
        "prestate_absent": False,
        "recorded": ("prestate_revalidated", "prestate_quarantined", "staging_published"),
        "state": "staging_published",
        "quarantine_present": True,
        "staging_present": False,
        "next_event": "parent_fsynced",
    },
    "parent_fsynced": {
        "artifact": "vector",
        "prestate_absent": False,
        "recorded": S207_SEQUENCE_QUARANTINED,
        "state": "complete",
        "quarantine_present": True,
        "staging_present": False,
        "next_event": None,
    },
}


def _s207_api(name: str) -> Any:
    """The S2-07 callable, or None -- MISSING CAPABILITY, never behavioural proof."""
    return getattr(profile_migration, name, None)


def _s207_manifest(
    plan: Any, checkpoint: str = "publishing", **overrides: Any
) -> MigrationManifest:
    """A manifest bound to a REAL plan: run id, source identity and digest agree."""
    values: dict[str, Any] = {
        "schema_version": 1,
        "run_id": plan.request.run_id,
        "strategy": plan.request.strategy,
        "checkpoint": checkpoint,
        "status": "running",
        "source_identity": plan.source_sql,
        "target_identities_before": {
            label: plan.targets[label] for label in ("vector", "graph") if label in plan.targets
        },
        "config_digest": plan.config_digest,
        "runtime_stop_attestation": {"value": "maintenance-ticket"},
        "artifacts": {},
        "completed_steps": [
            "planned",
            "locked",
            "backed_up",
            "sqlite_snapshotted",
            "projections_built",
        ],
        "events": [],
        "embedding": {},
        "failure": None,
    }
    values.update(overrides)
    return MigrationManifest(**values)


def _s207_write_manifest(
    plan: Any,
    run_dir: Path,
    *,
    checkpoint: str = "publishing",
    name: str = "manifest.json",
    **overrides: Any,
) -> Path:
    run_dir.mkdir(parents=True, exist_ok=True)
    live = run_dir / name
    profile_migration._write_manifest(live, _s207_manifest(plan, checkpoint, **overrides))
    return live


def _s207_stage(run_dir: Path, artifact: str, payload: bytes = b"S207-STAGED") -> Path:
    """A REAL prepared staging entry: a directory for vector, a file for graph."""
    staging = run_dir / S207_STAGING_DIRECTORY_NAME
    staging.mkdir(parents=True, exist_ok=True)
    entry = staging / S206_STAGING_NAMES[artifact]
    if artifact == "vector":
        (entry / "nested").mkdir(parents=True, exist_ok=True)
        (entry / "part.bin").write_bytes(payload)
        (entry / "nested" / "deep.bin").write_bytes(b"deep-" + payload[:8])
    else:
        entry.write_bytes(payload)
    return entry


def _s207_quarantine_entry(run_dir: Path, artifact: str, run_id: str) -> Path:
    """The run-owned quarantine entry of one artifact, by the module's own naming."""
    return (
        run_dir
        / profile_migration.QUARANTINE_DIRECTORY_NAME
        / profile_migration.QUARANTINE_PREPUBLISH_NAME
        / profile_migration._quarantine_entry_name(artifact, run_id)
    )


def _s207_operations(manifest_path: Path) -> list[str]:
    """The run's own recorded event names, in order, as a plain list."""
    return [event.operation for event in load_manifest(manifest_path).events]


def _s207_fail_nth_rename(monkeypatch: pytest.MonkeyPatch, nth: int) -> None:
    """Fail the nth REAL ``os.rename``: the publication stops exactly there."""
    real = os.rename
    seen: list[int] = []

    def _rename(source: Any, destination: Any, *args: Any, **kwargs: Any) -> None:
        seen.append(1)
        if len(seen) == nth:
            raise OSError(errno.EIO, "injected rename failure")
        return real(source, destination, *args, **kwargs)

    monkeypatch.setattr(os, "rename", _rename)


def _s207_fail_nth_manifest_write(monkeypatch: pytest.MonkeyPatch, nth: int) -> None:
    """Fail the nth durable manifest write: the chain stops one event short."""
    real = profile_migration._write_manifest
    seen: list[int] = []

    def _write(target: Path, manifest: Any) -> Any:
        seen.append(1)
        if len(seen) >= nth:
            raise ValueError("E_MANIFEST_WRITE_INJECTED")
        return real(target, manifest)

    monkeypatch.setattr(profile_migration, "_write_manifest", _write)


def _s207_publish(plan: Any, artifact: str, staged: Path, run_dir: Path, live: Path) -> Any:
    return profile_migration.publish_artifact(
        plan, artifact, staged_identity=_s206_identity(staged), run_dir=run_dir, manifest_path=live
    )


def _s207_crash_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, boundary: str
) -> dict[str, Any]:
    """Drive the REAL publication primitive to one crash boundary.

    Every input is real: a real backup report from the shipped backup stage, a
    real prepared staging entry for BOTH artifacts, and a crash produced by
    injecting the real ``os.rename`` / the real durable manifest write. Nothing
    about the resulting target/staging/quarantine triad is synthesised.
    """
    if boundary == "unrecorded_swap":
        spec = dict(S207_CRASH_BOUNDARIES["prestate_quarantined"])
        spec["recorded"] = ("prestate_revalidated", "prestate_quarantined")
        # The rename LANDED, so the staged entry is no longer under `staging/`:
        # it is the entry now sitting at the target name.
        spec["staging_present"] = False
        spec["state"] = "ambiguous"
    else:
        spec = S207_CRASH_BOUNDARIES[boundary]
    artifact = str(spec["artifact"])
    home, db_path = _s205_home(tmp_path)
    target = home / "data" / S206_STAGING_NAMES[artifact]
    if spec["prestate_absent"]:
        target.unlink()
    request, plan = _s205_legacy_plan(home, db_path)
    assert (plan.targets[artifact].kind == "absent") is bool(spec["prestate_absent"])
    run_dir = _s206_run_dir(home, plan)
    report = profile_migration.create_run_backup(plan)
    assert report is not None, "no backup stage exists at this commit"
    live = _s207_write_manifest(plan, run_dir)
    staged = {label: _s207_stage(run_dir, label) for label in ("vector", "graph")}

    if boundary == "parent_fsynced":
        assert _s207_publish(plan, artifact, staged[artifact], run_dir, live) is not None
    elif boundary == "unrecorded_swap":
        # The publish rename LANDS but its own event never reaches the manifest:
        # the real "failure between rename and manifest update" window.
        _s207_fail_nth_manifest_write(monkeypatch, 3)
        with pytest.raises(ValueError, match="E_MANIFEST_WRITE_INJECTED"):
            _s207_publish(plan, artifact, staged[artifact], run_dir, live)
    elif boundary == "staging_published":
        _s207_fail_nth_manifest_write(monkeypatch, 4)
        with pytest.raises(ValueError, match="E_MANIFEST_WRITE_INJECTED"):
            _s207_publish(plan, artifact, staged[artifact], run_dir, live)
    else:
        _s207_fail_nth_rename(monkeypatch, 2 if boundary == "prestate_quarantined" else 1)
        with pytest.raises(ValueError, match="E_PUBLICATION_RENAME_FAILED"):
            _s207_publish(plan, artifact, staged[artifact], run_dir, live)

    assert _s207_operations(live) == [f"{artifact}.{name}" for name in spec["recorded"]]
    quarantine = _s207_quarantine_entry(run_dir, artifact, plan.request.run_id)
    assert quarantine.exists() is bool(spec["quarantine_present"])
    assert staged[artifact].exists() is bool(spec["staging_present"])
    return {
        "home": home,
        "db_path": db_path,
        "request": request,
        "plan": plan,
        "artifact": artifact,
        "run_dir": run_dir,
        "live": live,
        "target": target,
        "quarantine": quarantine,
        "staged": staged,
        "spec": spec,
    }


def _s207_classify(manifest: Any, plan: Any, run_dir: Path) -> Any:
    api = _s207_api("classify_publication_triads")
    return None if api is None else api(manifest, plan, run_dir=run_dir)


def _s207_triad(triads: Any, label: str) -> Any:
    return next(item for item in triads if item.artifact == label)


def _s207_backup_copy(run_dir: Path) -> Path:
    """A REAL backed-up regular file, located from the run's own report."""
    report = json.loads((run_dir / profile_migration.BACKUP_REPORT_NAME).read_text(encoding="utf-8"))
    for entry in report["entries"]:
        if entry["present"] and entry["kind"] == "regular_file":
            return run_dir / entry["run_relative_path"]
    raise AssertionError("the backup report records no regular-file entry to check")


@pytest.mark.parametrize("boundary", sorted(S207_CRASH_BOUNDARIES))
def test_s207_every_publication_event_crash_boundary_is_classified(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, synthetic_storage_env, boundary: str
) -> None:
    """MISSING-CAPABILITY at BASE: no deterministic triad classifier exists.

    The crash state itself is behavioural evidence on BOTH sides -- the run's own
    chain and the real triad are produced by the shipped publication primitive.
    Behavioural successor of this contract: the classifier is driven from the
    entrypoint in `test_s207_resume_refuses_with_the_gate_of_the_single_unambiguous_operation`
    and `test_s207_a_completed_run_has_nothing_to_repeat_and_resume_is_byte_stable`,
    which are behavioural on both sides; wiring the classifier into `apply` is
    S3-06 (routing-matrix S2-06 split_further).
    """
    env = synthetic_storage_env
    env.assert_injection()
    state = _s207_crash_state(tmp_path, monkeypatch, boundary)
    spec = state["spec"]

    triads = _s207_classify(load_manifest(state["live"]), state["plan"], state["run_dir"])
    assert triads is not None, "no deterministic target/staging/quarantine classifier exists"

    triad = _s207_triad(triads, state["artifact"])
    assert triad.recorded_events == spec["recorded"]
    assert triad.state == spec["state"]
    assert triad.next_event == spec["next_event"]
    assert triad.quarantine_present is spec["quarantine_present"]
    assert triad.staging_present is spec["staging_present"]

    other = _s207_triad(triads, "graph" if state["artifact"] == "vector" else "vector")
    assert other.recorded_events == ()
    assert other.next_event == "prestate_revalidated"


def test_s207_resume_refuses_with_the_gate_of_the_single_unambiguous_operation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, synthetic_storage_env
) -> None:
    """BEHAVIOURAL at BASE: `resume` refuses with E_MIGRATION_NOT_IMPLEMENTED.

    At HEAD it repeats every precondition, classifies a real mid-publication crash
    and refuses with the CAUSE of the ONE operation it found: exactly one next
    operation exists, and its gate -- the verification seam that is still the S0
    stub -- is what keeps resume from executing it (acceptance 3 / R1 / R4).
    """
    env = synthetic_storage_env
    env.assert_injection()
    state = _s207_crash_state(tmp_path, monkeypatch, "prestate_quarantined")
    manifest_bytes = state["live"].read_bytes()

    with pytest.raises(ValueError, match="E_STAGED_VERIFICATION_CAPABILITY_MISSING"):
        profile_migration.resume_profile_migration(state["live"], state["request"])

    assert state["live"].read_bytes() == manifest_bytes
    assert state["quarantine"].exists(), "the retained prestate copy was touched"


def test_s207_the_classifier_reports_the_phase_and_the_single_next_operation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, synthetic_storage_env
) -> None:
    """MISSING-CAPABILITY at BASE: `classify_resume` does not exist yet.

    Behavioural successor of the SAME contract: the entrypoint node
    `test_s207_resume_refuses_with_the_gate_of_the_single_unambiguous_operation`
    and the parametrized triad node above are behavioural on both sides; what this
    node adds is the classification itself, which is what an operator reads when
    the entrypoint refuses. At HEAD it is the real thing: the phase, the single
    next operation per artifact, the gate and the tolerations this run is expected
    to produce.
    """
    env = synthetic_storage_env
    env.assert_injection()
    state = _s207_crash_state(tmp_path, monkeypatch, "prestate_quarantined")

    api = _s207_api("classify_resume")
    assert api is not None, "no deterministic resume classifier exists at this commit"
    outcome = api(state["live"], state["request"])

    assert outcome.phase == "publishing"
    assert outcome.checkpoint == "publishing"
    assert outcome.next_checkpoint == "published"
    assert outcome.next_operations == (
        "vector.staging_published",
        "graph.prestate_revalidated",
    )
    assert outcome.blocked_by == "E_STAGED_VERIFICATION_CAPABILITY_MISSING"
    assert outcome.backup_state == "verified"
    assert "E_BACKUP_COLLISION:run" in outcome.tolerated_blockers
    vector = _s207_triad(outcome.triads, "vector")
    assert vector.state == "prestate_quarantined"
    assert vector.next_event == "staging_published"
    assert vector.detail["pin_source"] == "record"

    # Before `publishing` the triads are not consulted at all, and the ONE next
    # checkpoint is the capability-gated verifier: batch/embedding state is S3's.
    home, db_path = _s205_home(tmp_path / "batch")
    batch_request, batch_plan = _s205_legacy_plan(home, db_path)
    assert profile_migration.create_run_backup(batch_plan) is not None
    batch_live = _s207_write_manifest(
        batch_plan, _s206_run_dir(home, batch_plan), checkpoint="projections_built"
    )
    batch = api(batch_live, batch_request)
    assert batch.phase == "prepublication"
    assert batch.checkpoint == "projections_built"
    assert batch.triads == ()
    assert batch.next_operations == ()
    assert batch.next_checkpoint == "staged_verified"
    assert batch.blocked_by == "E_STAGED_VERIFICATION_CAPABILITY_MISSING"
    assert batch.backup_state == "verified"


def test_s207_a_completed_run_has_nothing_to_repeat_and_resume_is_byte_stable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, synthetic_storage_env
) -> None:
    """BEHAVIOURAL at BASE; acceptance 4 -- a repeated resume repeats no swap.

    Both artifacts are published by the REAL primitive, so the run's own events
    (not a claim) say the sequence is complete. Two resumes then execute nothing,
    the manifest is byte-identical, and the stale quarantine, the source and the
    backup are all retained; the only thing left is the gated next checkpoint,
    which is what both calls refuse on.
    """
    env = synthetic_storage_env
    env.assert_injection()
    state = _s207_crash_state(tmp_path, monkeypatch, "parent_fsynced")
    assert _s207_publish(
        state["plan"], "graph", state["staged"]["graph"], state["run_dir"], state["live"]
    ) is not None

    live = state["live"]
    run_dir = state["run_dir"]
    manifest_bytes = live.read_bytes()
    source_bytes = state["db_path"].read_bytes()
    report_bytes = (run_dir / profile_migration.BACKUP_REPORT_NAME).read_bytes()
    quarantined_before = sorted(path.name for path in state["quarantine"].parent.iterdir())
    tree_before = _tree_snapshot(tmp_path)

    for _attempt in range(2):
        with pytest.raises(ValueError, match="E_STAGED_VERIFICATION_CAPABILITY_MISSING"):
            profile_migration.resume_profile_migration(live, state["request"])

    assert live.read_bytes() == manifest_bytes, "a repeated resume rewrote the manifest"
    assert _tree_snapshot(tmp_path) == tree_before, "a repeated resume mutated the filesystem"
    assert state["db_path"].read_bytes() == source_bytes, "resume touched the source"
    assert (run_dir / profile_migration.BACKUP_REPORT_NAME).read_bytes() == report_bytes
    assert sorted(path.name for path in state["quarantine"].parent.iterdir()) == quarantined_before
    assert not state["staged"]["vector"].exists()
    assert not state["staged"]["graph"].exists()


def test_s207_an_event_that_contradicts_the_disk_is_refused_as_ambiguous(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, synthetic_storage_env
) -> None:
    """BEHAVIOURAL at BASE; DETAIL 10.5's ambiguous refusal.

    The run's chain says the prestate was quarantined, but the target name is NOT
    vacant: the rename landed and its event never did. Two candidate readings
    exist, so resume must return E_PUBLICATION_AMBIGUOUS and require manual
    escalation -- never guess, and never "repair" the chain.
    """
    env = synthetic_storage_env
    env.assert_injection()
    state = _s207_crash_state(tmp_path, monkeypatch, "unrecorded_swap")
    assert state["target"].exists(), "the swap must have landed for this fixture to mean anything"
    assert not state["staged"]["vector"].exists()
    manifest_bytes = state["live"].read_bytes()

    with pytest.raises(ValueError, match="E_PUBLICATION_AMBIGUOUS"):
        profile_migration.resume_profile_migration(state["live"], state["request"])

    assert state["live"].read_bytes() == manifest_bytes, (
        "an ambiguous refusal must not rewrite the run's own record"
    )


def test_s207_a_duplicated_publication_sequence_is_refused_and_stays_unrecoverable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, synthetic_storage_env
) -> None:
    """BEHAVIOURAL at BASE; the S2-06 review's R3 fail-closed case.

    When one artifact's events appear TWICE in a manifest, the shipped predicate
    `_published_sequence_is_complete` returns False forever (len != 4). Resume
    must classify that honestly as unrecoverable instead of treating the
    duplicate as progress.
    """
    env = synthetic_storage_env
    env.assert_injection()
    state = _s207_crash_state(tmp_path, monkeypatch, "parent_fsynced")
    profile_migration.append_manifest_event(
        state["live"],
        {
            "checkpoint": "publishing",
            "operation": "vector.prestate_revalidated",
            "payload": {"duplicate": True},
        },
    )
    recorded = profile_migration.publication_events_from_manifest(load_manifest(state["live"]))
    assert len(recorded["vector"]) == 5
    assert profile_migration._published_sequence_is_complete(recorded["vector"]) is False

    with pytest.raises(ValueError, match="E_PUBLICATION_AMBIGUOUS"):
        profile_migration.resume_profile_migration(state["live"], state["request"])


def test_s207_a_missing_required_triad_entry_is_refused_and_nothing_is_deleted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, synthetic_storage_env
) -> None:
    """BEHAVIOURAL at BASE; the card's "missing required triad entry" Stop.

    Two real missing-entry cases, both refused: the staged entry the next
    operation would publish is gone, and the quarantine entry the run's own event
    claims is gone. Neither the retained quarantine nor the stale staging is ever
    deleted by resume.
    """
    env = synthetic_storage_env
    env.assert_injection()
    state = _s207_crash_state(tmp_path, monkeypatch, "prestate_quarantined")

    retained = state["quarantine"].parent.parent / "retained-old-entry"
    os.rename(state["quarantine"], retained)
    with pytest.raises(ValueError, match="E_RESUME_TRIAD_INCOMPLETE"):
        profile_migration.resume_profile_migration(state["live"], state["request"])
    assert retained.exists(), "resume must never delete a retained prestate copy"
    os.rename(retained, state["quarantine"])

    import shutil as _shutil

    _shutil.rmtree(state["staged"]["vector"])
    with pytest.raises(ValueError, match="E_RESUME_TRIAD_INCOMPLETE"):
        profile_migration.resume_profile_migration(state["live"], state["request"])
    assert state["quarantine"].exists()


def test_s207_before_publishing_resume_continues_only_through_verified_steps(
    tmp_path: Path, synthetic_storage_env
) -> None:
    """BEHAVIOURAL at BASE; acceptance 2.

    Before `publishing` the triads are never consulted: the next step stays shut
    by CAUSE -- the capability-gated verifier while the seam is the S0 stub, and an
    engine stage this slice has not wired for the checkpoints before it. Batch and
    embedding state is S3's, and nothing on disk moves.
    """
    env = synthetic_storage_env
    env.assert_injection()
    home, db_path = _s205_home(tmp_path)
    request, plan = _s205_legacy_plan(home, db_path)
    run_dir = _s206_run_dir(home, plan)
    assert profile_migration.create_run_backup(plan) is not None
    batch_live = _s207_write_manifest(plan, run_dir, checkpoint="projections_built")
    early_live = _s207_write_manifest(
        plan, run_dir, checkpoint="backed_up", name="manifest-early.json"
    )
    tree_before = _tree_snapshot(tmp_path)

    with pytest.raises(ValueError, match="E_STAGED_VERIFICATION_CAPABILITY_MISSING"):
        profile_migration.resume_profile_migration(batch_live, request)
    with pytest.raises(ValueError, match="E_MIGRATION_NOT_IMPLEMENTED"):
        profile_migration.resume_profile_migration(early_live, request)

    assert _tree_snapshot(tmp_path) == tree_before, "a pre-publication resume touched the tree"


def test_s207_resume_refuses_a_changed_source_a_changed_config_or_a_changed_backup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, synthetic_storage_env
) -> None:
    """BEHAVIOURAL at BASE; acceptance 1 and the card's Stop list.

    Every identity a resume must repeat is really re-checked against the run's own
    durable records: the source's no-follow identity, the config digest and every
    backed-up entry's size and digest.
    """
    env = synthetic_storage_env
    env.assert_injection()
    state = _s207_crash_state(tmp_path, monkeypatch, "prestate_revalidated")

    connection = sqlite3.connect(state["db_path"])
    connection.execute("insert into facts(id) values ('after-the-crash')")
    connection.commit()
    connection.close()
    with pytest.raises(ValueError, match="E_RESUME_SOURCE_CHANGED"):
        profile_migration.resume_profile_migration(state["live"], state["request"])

    config_state = _s207_crash_state(tmp_path / "config", monkeypatch, "prestate_revalidated")
    tampered = _s207_write_manifest(
        config_state["plan"], config_state["run_dir"], config_digest="cd" * 32
    )
    with pytest.raises(ValueError, match="E_RESUME_CONFIG_CHANGED"):
        profile_migration.resume_profile_migration(tampered, config_state["request"])

    backup_state = _s207_crash_state(tmp_path / "backup", monkeypatch, "prestate_revalidated")
    copy = _s207_backup_copy(backup_state["run_dir"])
    copy.write_bytes(copy.read_bytes() + b"corrupted")
    with pytest.raises(ValueError, match="E_RESUME_BACKUP_CHANGED"):
        profile_migration.resume_profile_migration(backup_state["live"], backup_state["request"])


def test_s207_resume_refuses_a_missing_a_broken_and_a_foreign_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, synthetic_storage_env
) -> None:
    """BEHAVIOURAL at BASE; resume must not inherit a claim it cannot read.

    A missing manifest, a manifest whose bytes are truncated (the broken
    chain/digest Stop) and a manifest that belongs to a DIFFERENT run id are all
    refused after the preconditions repeat, never before.
    """
    env = synthetic_storage_env
    env.assert_injection()
    state = _s207_crash_state(tmp_path, monkeypatch, "prestate_revalidated")

    with pytest.raises(ValueError, match="E_MANIFEST_ABSENT"):
        profile_migration.resume_profile_migration(tmp_path / "absent" / "manifest.json", state["request"])

    truncated = state["run_dir"] / "manifest-truncated.json"
    truncated.write_bytes(state["live"].read_bytes()[:64])
    with pytest.raises(ValueError, match="E_MANIFEST_SCHEMA"):
        profile_migration.resume_profile_migration(truncated, state["request"])

    foreign = _s207_write_manifest(
        state["plan"],
        state["home"] / ".cmms-migrations" / ("e" * 32),
        run_id="e" * 32,
    )
    with pytest.raises(ValueError, match="E_RESUME_RUN_MISMATCH"):
        profile_migration.resume_profile_migration(foreign, state["request"])


def test_s207_the_durable_record_names_the_qualified_evidence_object(tmp_path: Path) -> None:
    """BEHAVIOURAL pre-fix RED for R7 (the S2-06 review's F-1, reproduced).

    `advance_manifest_checkpoint` recorded the first evidence object matching the
    CHECKPOINT, while the transition itself qualifies on checkpoint AND code, so a
    decoy object for the same checkpoint could be written into the append-only
    record as the justification. The event must name the qualified object.
    """
    run_dir = _s2_run_dir(tmp_path)
    live = run_dir / "manifest.json"
    profile_migration._write_manifest(live, _s2_manifest(checkpoint="planned"))

    factory = profile_migration.CheckpointEvidence
    decoy = factory(
        checkpoint="locked", code="plan_digest", digest="cc" * 32, detail=_s206_detail("planned")
    )
    qualified = factory(
        checkpoint="locked", code="lock_ownership", digest="ab" * 32, detail=_s206_detail("locked")
    )
    advanced = profile_migration.advance_manifest_checkpoint(live, "locked", evidence=[decoy, qualified])
    assert advanced.checkpoint == "locked"

    event = load_manifest(live).events[-1]
    assert event.payload["evidence"] == "lock_ownership", (
        "the durable record names an evidence object that the transition never qualified"
    )
    assert event.payload["digest"] == "ab" * 32


def _s207_staged_key(entry: Path) -> Any:
    """The module's OWN identity key of a staged entry, observed through its parent.

    Built from the shipped observer (`_observed_entry_identity`) and the shipped
    field set (`_identity_key`), so a mutation assertion is measured in exactly the
    units the production comparison uses -- and it is available on BOTH sides of
    the fix, so it cannot make this node's failure vacuous at BASE.
    """
    with profile_migration.storage_lock.open_directory_nofollow(entry.parent) as parent_fd:
        observed = profile_migration._observed_entry_identity(
            parent_fd, entry.name, artifact="s207-staged-probe"
        )
    return profile_migration._identity_key(observed)


def test_s207_resume_refuses_a_staged_entry_that_is_not_the_identity_the_run_recorded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, synthetic_storage_env
) -> None:
    """BEHAVIOURAL at BASE; acceptance 1 / DETAIL 10.5 -- the STAGED identity class.

    BEHAVIOURAL at BASE, and it must be read that way: at BASE this node FAILS on
    the refusal CAUSE, not on a missing symbol. The BASE entrypoint refuses a
    changed staged entry with ``E_STAGED_VERIFICATION_CAPABILITY_MISSING`` -- the
    identical code it returns for an INTACT staged entry -- so the assertion that
    fails at BASE is the one demanding ``E_RESUME_STAGING_CHANGED``. Nothing here
    depends on a symbol, helper or signature that BASE does not already have.

    Two REAL crash states, both driven through the shipped publication primitive
    (`_s207_crash_state`), cover both arms where the run's own record is at or
    after `prestate_revalidated` and the staged entry is present and required:

    * position 1 (`prestate_revalidated`, vector -- a staged DIRECTORY): the entry
      is replaced by a REGULAR FILE carrying different real bytes, so the
      position-independent field set (kind/device/inode/mode/size/mtime_ns/sha256/
      raw_link_target) differs in kind, size and digest;
    * position 2 (`prestate_absent`, graph -- a staged REGULAR FILE): its real
      bytes AND size change after the crash (11 B -> 4000 B).

    The intact CONTROL is asserted first in each arm: the same state with the
    staged entry untouched must NOT raise the new code -- it refuses on the
    capability gate -- so the refusal below is caused by the mutation and not by
    the fixture. Each mutation is additionally asserted to change the module's OWN
    observed identity key (`_s207_staged_key`), which is measured with the shipped
    observer on BOTH sides of the fix.

    The honest limit of the comparison is disclosed in the SUMMARY: a DIRECTORY's
    identity key carries dev/ino/mode and no size and no digest, so a like-for-like
    directory swap is detected only when its inode or its mode changes -- while
    this node was written, `rmtree` + `mkdir` REUSED the inode, which is exactly
    why this leg swaps the KIND instead of pretending a directory digest exists.
    """
    env = synthetic_storage_env
    env.assert_injection()

    # --- position 1: vector's staged entry is a DIRECTORY ------------------------
    state = _s207_crash_state(tmp_path, monkeypatch, "prestate_revalidated")
    staged = state["staged"]["vector"]
    key_before = _s207_staged_key(staged)
    manifest_bytes = state["live"].read_bytes()

    with pytest.raises(ValueError, match="E_STAGED_VERIFICATION_CAPABILITY_MISSING"):
        profile_migration.resume_profile_migration(state["live"], state["request"])

    import shutil as _shutil

    # A LIKE-FOR-LIKE directory swap is NOT a reliable mutation here: `rmtree` +
    # `mkdir` reuses the inode on this filesystem (observed while writing this
    # node), and a directory's identity key carries dev/ino/mode only. The mutation
    # therefore replaces the staged DIRECTORY with a REGULAR FILE of real bytes:
    # kind, size and digest all change, deterministically.
    _shutil.rmtree(staged)
    staged.write_bytes(b"SWAPPED-STAGED-ENTRY-BYTES" * 250)
    assert _s207_staged_key(staged) != key_before, "the fixture did not change the observed identity"

    with pytest.raises(ValueError, match="E_RESUME_STAGING_CHANGED"):
        profile_migration.resume_profile_migration(state["live"], state["request"])

    assert state["live"].read_bytes() == manifest_bytes, "a refusal rewrote the run's own record"
    assert staged.exists(), "a refusal must not delete the entry it refused to publish"

    # --- position 2: graph's staged entry is a REGULAR FILE, real bytes + size ---
    file_state = _s207_crash_state(tmp_path / "file", monkeypatch, "prestate_absent")
    staged_file = file_state["staged"]["graph"]
    file_key_before = _s207_staged_key(staged_file)
    size_before = os.lstat(staged_file).st_size
    file_manifest_bytes = file_state["live"].read_bytes()

    with pytest.raises(ValueError, match="E_STAGED_VERIFICATION_CAPABILITY_MISSING"):
        profile_migration.resume_profile_migration(file_state["live"], file_state["request"])

    staged_file.write_bytes(b"S207-SWAPPED-STAGED-BYTES" * 200)
    assert os.lstat(staged_file).st_size != size_before, "the fixture did not change the entry's size"
    assert _s207_staged_key(staged_file) != file_key_before, "the fixture did not change the identity"

    with pytest.raises(ValueError, match="E_RESUME_STAGING_CHANGED"):
        profile_migration.resume_profile_migration(file_state["live"], file_state["request"])

    assert file_state["live"].read_bytes() == file_manifest_bytes
    assert staged_file.exists(), "a refusal must not delete the entry it refused to publish"


# ---------------------------------------------------------------------------
# S2-08 -- rollback to the exact pre-state with a retained diagnostic quarantine
# (DETAIL 9.3's rollback event chain, DETAIL 10.6's ten steps).
#
# PRE-FIX CLASSIFICATION OF THIS SECTION (filed with the RED in S2-08_EVIDENCE):
# every node below drives the REAL `rollback_profile_migration` entrypoint on a
# REAL stopped run produced by the shipped backup (`create_run_backup`) and
# publication (`publish_artifact`) primitives. At BASE that entrypoint DOES exist
# and refuses with `E_MIGRATION_NOT_IMPLEMENTED` after its own precondition pass
# (the S2-07 tree has it at `profile_migration.py:4095-4100`), so every node here
# fails at BASE with a WRONG REFUSAL CAUSE or a missing restore -- a behavioural
# failure, never a collection ImportError, never a TypeError/KeyError raised by a
# helper of this file, and never `pytest.fail`. The single GUARD node
# (`test_s208_the_rollback_entrypoint_repeats_the_apply_guards`) PASSES at BASE by
# design, because the BASE entrypoint already validates those guards; it is
# labelled GUARD and is NOT counted as a RED.
#
# Honest limit of the pre-state comparison (R-6/O-3, disclosed in the SUMMARY):
# `_s208_tree` records a directory's mode and its CHILDREN's own no-follow
# identity/bytes, so a directory pre-state is compared by its children's content
# and NOT by a recursive directory digest -- this project's identity model has no
# directory content digest (`_observed_entry_identity` carries size/mtime/sha256
# = None for a directory), and this card does not widen it.
# ---------------------------------------------------------------------------

S208_QUARANTINE_FAILED_NAME = "failed-current"
S208_RESTORE_NAME = "restore"
S208_ROLLBACK_CHAIN: tuple[str, ...] = (
    "rollback.locked",
    "rollback.current_quarantined",
    "rollback.prestate_restored",
    "rollback.verified",
    "rolled_back",
)
S208_FAILED_EVENT = "rollback.failed"
S208_PAYLOADS: dict[str, bytes] = {
    "vector": b"S208-PUBLISHED-VECTOR" * 4,
    "graph": b'{"s208": "published"}\n',
}


def _s208_request(plan: Any, **overrides: Any) -> Any:
    """The plan's own request, in ROLLBACK mode (the entrypoint's own guard)."""
    return replace(plan.request, mode="rollback", **overrides)


def _s208_tree(root: Path) -> tuple[tuple[str, str, int, bytes | None], ...]:
    """A NO-FOLLOW description of one entry: relpath, kind, mode and real bytes.

    A symlink is recorded by its exact raw target string and is never traversed
    (``rglob`` does not follow a symlinked directory), an absent entry is its own
    explicit record, and a directory contributes its children's entries -- there is
    no recursive directory content digest anywhere in this project (R-6/O-3), so
    none is invented here either.
    """
    if not os.path.lexists(root):
        return (("", "absent", 0, None),)
    info = os.lstat(root)
    if stat.S_ISLNK(info.st_mode):
        return (("", "symlink:" + os.readlink(root), stat.S_IMODE(info.st_mode), None),)
    if stat.S_ISREG(info.st_mode):
        return (("", "regular_file", stat.S_IMODE(info.st_mode), root.read_bytes()),)
    assert stat.S_ISDIR(info.st_mode), "the fixture root is not a regular entry"
    entries: list[tuple[str, str, int, bytes | None]] = [
        ("", "directory", stat.S_IMODE(info.st_mode), None)
    ]
    for path in sorted(root.rglob("*")):
        child = os.lstat(path)
        relative = str(path.relative_to(root))
        if stat.S_ISLNK(child.st_mode):
            entries.append((relative, "symlink:" + os.readlink(path), stat.S_IMODE(child.st_mode), None))
        elif stat.S_ISDIR(child.st_mode):
            entries.append((relative, "directory", stat.S_IMODE(child.st_mode), None))
        elif stat.S_ISREG(child.st_mode):
            entries.append((relative, "regular_file", stat.S_IMODE(child.st_mode), path.read_bytes()))
        else:
            entries.append((relative, "special", stat.S_IMODE(child.st_mode), None))
    return tuple(entries)


def _s208_backup_entries(run_dir: Path) -> dict[str, Any]:
    """The run's OWN backup report, keyed by its recorded artifact label."""
    report = json.loads(
        (run_dir / profile_migration.BACKUP_REPORT_NAME).read_text(encoding="utf-8")
    )
    return {str(entry["artifact"]): entry for entry in report["entries"]}


def _s208_publish(plan: Any, artifact: str, staged: Path, run_dir: Path, live: Path | None) -> Any:
    """publish_artifact(...) against the REAL plan, with the REAL staged identity."""
    return profile_migration.publish_artifact(
        plan, artifact, staged_identity=_s206_identity(staged), run_dir=run_dir, manifest_path=live
    )


def _s208_run(
    tmp_path: Path, *, legacy: bool = True, publish: tuple[str, ...] = ("vector",), live: bool = True
) -> dict[str, Any]:
    """A REAL stopped run: real pre-state, real backup, real per-artifact publication.

    ``legacy`` selects the vector pre-state (a final symlink vs a directory),
    ``publish`` names the artifacts the shipped ``publish_artifact`` primitive
    really swapped into place, and ``live`` False drives that publication WITHOUT a
    manifest, so the run's own record carries no publication event at all.
    """
    if legacy:
        home, db_path, referent = _s205_legacy_home(tmp_path)
    else:
        home, db_path = _s205_home(tmp_path)
        referent = home / "data" / "lancedb"
    request, plan = _s205_legacy_plan(home, db_path)
    run_dir = _s206_run_dir(home, plan)
    assert profile_migration.create_run_backup(plan) is not None, "no backup stage exists at this commit"
    manifest_path = _s207_write_manifest(plan, run_dir) if live else None
    targets = {label: Path(plan.targets[label].lexical_path) for label in ("vector", "graph")}
    prestates = {label: _s208_tree(targets[label]) for label in ("vector", "graph")}
    staging = {label: _s207_stage(run_dir, label, S208_PAYLOADS[label]) for label in ("vector", "graph")}
    for label in publish:
        assert (
            _s208_publish(plan, label, staging[label], run_dir, manifest_path) is not None
        ), "no per-artifact publication primitive exists at this commit"
    published = {label: _s208_tree(targets[label]) for label in ("vector", "graph")}
    return {
        "home": home,
        "db_path": db_path,
        "referent": referent,
        "plan": plan,
        "request": _s208_request(plan),
        "run_dir": run_dir,
        "manifest_path": manifest_path,
        "targets": targets,
        "prestates": prestates,
        "published": published,
        "staging": staging,
    }


def _s208_failed_current(run_dir: Path) -> list[Path]:
    """Every entry of the run-owned ``quarantine/failed-current`` diagnostic area."""
    parent = run_dir / profile_migration.QUARANTINE_DIRECTORY_NAME / S208_QUARANTINE_FAILED_NAME
    if not parent.exists():
        return []
    return sorted(parent.iterdir())


def _s208_open_spy(monkeypatch: pytest.MonkeyPatch, root: Path) -> list[str]:
    """Record every ``os.open`` path, then open for real (a transparent spy)."""
    seen: list[str] = []
    real_open = os.open

    def _open(path: Any, *args: Any, **kwargs: Any) -> Any:
        seen.append(str(path))
        try:
            seen.append(os.path.realpath(str(path)))
        except OSError:
            pass
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(os, "open", _open)
    return seen


def _s208_opens_touching(seen: list[str], root: Path) -> list[str]:
    """Which recorded open paths name ``root`` or anything inside it."""
    literal = str(root)
    resolved = os.path.realpath(root)
    return [
        item
        for item in seen
        if item == literal
        or item.startswith(literal + os.sep)
        or item == resolved
        or item.startswith(resolved + os.sep)
    ]


def test_s208_a_real_rollback_restores_the_exact_raw_symlink_prestate_and_retains_every_copy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, synthetic_storage_env
) -> None:
    """ROLLBACK of a published SYMLINK pre-state: exact raw target, referent untouched.

    DETAIL 10.6 steps 3-6 and 9: the CURRENT target identity is checked against the
    run's own durable record, the run-created directory is quarantined (never
    deleted), the prior symlink is restored from the retained backup as its EXACT
    raw target created at a run-owned staging name and renamed into the vacant
    target, and the original backup, the run's prepublish quarantine and the SQLite
    and legacy trees are all retained.
    """
    env = synthetic_storage_env
    env.assert_injection()
    state = _s208_run(tmp_path, legacy=True, publish=("vector",))
    target = state["targets"]["vector"]
    run_dir = state["run_dir"]
    referent = state["referent"]
    referent_before = _s208_tree(referent)
    sqlite_before = state["db_path"].read_bytes()
    backup_link = run_dir / "backup" / "link-entries" / "vector"
    assert backup_link.is_symlink(), "the shipped backup did not record the raw link entry"
    raw_prestate = os.readlink(backup_link)

    seen = _s208_open_spy(monkeypatch, referent)
    returned = rollback_profile_migration(state["manifest_path"], state["request"])

    assert returned.status == "rolled_back"
    assert returned.checkpoint == "publishing"
    # The pre-state is back, exactly: the same raw link string, nothing followed.
    assert _s208_tree(target) == state["prestates"]["vector"], "the prior symlink was not restored exactly"
    assert os.readlink(target) == raw_prestate
    assert not os.path.islink(referent)
    assert _s208_tree(referent) == referent_before, "the referent was touched"
    assert _s208_opens_touching(seen, referent) == [], "the referent was opened"
    # The run-created artifact is quarantined, never deleted, byte for byte.
    failed = _s208_failed_current(run_dir)
    assert len(failed) == 1, "the run-created current artifact was not quarantined exactly once"
    assert _s208_tree(failed[0]) == state["published"]["vector"]
    assert re.fullmatch(r"vector-[0-9a-f]{32}", failed[0].name), "the diagnostic quarantine name is not unique"
    # Everything else is retained untouched.
    assert state["db_path"].read_bytes() == sqlite_before, "the SQLite store was touched"
    assert _s208_tree(state["targets"]["graph"]) == state["prestates"]["graph"]
    retained = run_dir / profile_migration.QUARANTINE_DIRECTORY_NAME / profile_migration.QUARANTINE_PREPUBLISH_NAME
    retained_links = [path for path in retained.iterdir() if os.path.islink(path)]
    assert [os.readlink(path) for path in retained_links] == [raw_prestate]
    assert backup_link.is_symlink() and os.readlink(backup_link) == raw_prestate, "the original backup was lost"
    assert (run_dir / profile_migration.BACKUP_REPORT_NAME).exists()
    # The durable rollback chain, appended to the run's own record.
    assert _s207_operations(state["manifest_path"]) == [
        *(f"vector.{name}" for name in S206_PUBLISHED_SEQUENCE),
        *S208_ROLLBACK_CHAIN,
    ]


def test_s208_a_regular_backup_is_copied_to_a_unique_staging_entry_verified_and_renamed(
    tmp_path: Path, synthetic_storage_env
) -> None:
    """acceptance 3: the REGULAR backup is copied, verified and renamed; original kept.

    The restored regular file must be the pre-state byte for byte (mode included),
    the run-created replacement must be retained in the diagnostic quarantine, the
    restore staging entry must be uniquely named and gone (it was renamed into the
    vacant target), and the ORIGINAL BACKUP COPY must still hold the PRE-state
    bytes -- which is what proves the restore was made from it and not left in place.
    """
    env = synthetic_storage_env
    env.assert_injection()
    state = _s208_run(tmp_path, legacy=False, publish=("graph",))
    target = state["targets"]["graph"]
    run_dir = state["run_dir"]
    original = state["prestates"]["graph"]

    returned = rollback_profile_migration(state["manifest_path"], state["request"])

    assert returned.status == "rolled_back"
    assert _s208_tree(target) == original, "the regular pre-state was not restored byte for byte"
    assert os.lstat(target).st_size == len(original[0][3] or b"")
    failed = _s208_failed_current(run_dir)
    assert len(failed) == 1
    assert _s208_tree(failed[0]) == state["published"]["graph"]
    assert re.fullmatch(r"graph-[0-9a-f]{32}", failed[0].name)
    backup_copy = run_dir / "backup" / "graph.json"
    assert backup_copy.read_bytes() == (original[0][3] or b""), "the original backup is no longer the pre-state"
    staging_area = run_dir / S208_RESTORE_NAME
    assert staging_area.is_dir(), "the rollback owns no restore staging area"
    assert [path.name for path in staging_area.iterdir()] == [], "the unique staging entry was not renamed away"


def test_s208_a_prior_absence_is_restored_to_absence_after_quarantining_only_the_run_created_entry(
    tmp_path: Path, synthetic_storage_env
) -> None:
    """acceptance 2 / DETAIL 10.6 step 7: a prior absence is restored to absence."""
    env = synthetic_storage_env
    env.assert_injection()
    home, db_path, _external = _s205_legacy_home(tmp_path)
    (home / "data" / "graph.json").unlink()
    request, plan = _s205_legacy_plan(home, db_path)
    assert plan.targets["graph"].kind == "absent"
    run_dir = _s206_run_dir(home, plan)
    assert profile_migration.create_run_backup(plan) is not None
    live = _s207_write_manifest(plan, run_dir)
    target = home / "data" / "graph.json"
    staged = _s207_stage(run_dir, "graph", b'{"s208": "was-absent"}\n')
    assert _s208_publish(plan, "graph", staged, run_dir, live) is not None
    assert target.read_bytes() == b'{"s208": "was-absent"}\n'
    published = _s208_tree(target)

    returned = rollback_profile_migration(live, _s208_request(plan))

    assert returned.status == "rolled_back"
    assert not os.path.lexists(target), "a prior absence was not restored to absence"
    failed = _s208_failed_current(run_dir)
    assert len(failed) == 1
    assert _s208_tree(failed[0]) == published, "only the exact run-created entry may be quarantined"


def test_s208_the_current_target_identity_is_checked_and_drift_is_refused_before_anything_moves(
    tmp_path: Path, synthetic_storage_env
) -> None:
    """acceptance 1 / the card's Stop: target drift is refused, retaining all copies.

    The run's own record pins the identity of the entry it published (the S2-07
    `staged_identity_key` payload key). A DIFFERENT entry is put at the target name
    -- a real rename over the vacant name, so the filesystem cannot hand the freed
    inode back -- and the rollback must refuse with the drift code instead of
    quarantining a stranger.
    """
    env = synthetic_storage_env
    env.assert_injection()
    state = _s208_run(tmp_path, legacy=False, publish=("graph",))
    target = state["targets"]["graph"]
    run_dir = state["run_dir"]
    manifest_before = state["manifest_path"].read_bytes()
    backup_before = _s208_tree(run_dir / "backup")

    replacement = state["home"] / "drifted-graph.json"
    replacement.write_bytes(b'{"drifted": true}\n')
    moved = state["home"] / "published-graph-moved-away.json"
    os.replace(target, moved)
    os.replace(replacement, target)
    assert _s208_tree(target) != state["published"]["graph"]

    with pytest.raises(ValueError, match="E_ROLLBACK_TARGET_DRIFT"):
        rollback_profile_migration(state["manifest_path"], state["request"])

    assert state["manifest_path"].read_bytes() == manifest_before, "a refusal rewrote the run's record"
    assert _s208_tree(target) == _s208_tree(replacement) or target.read_bytes() == b'{"drifted": true}\n'
    assert _s208_failed_current(run_dir) == [], "a refused rollback quarantined something"
    assert _s208_tree(run_dir / "backup") == backup_before
    assert moved.exists(), "a refused rollback deleted the entry it refused to act on"


def test_s208_a_missing_recorded_target_identity_is_refused_rather_than_inferred(
    tmp_path: Path, synthetic_storage_env
) -> None:
    """acceptance 1 + R-4/O-2: no durable comparable identity ⇒ refuse, never infer.

    The run's record here really claims a publication for `graph` but carries no
    comparable staged identity key at its `prestate_revalidated` event (the
    publication was driven without a manifest and the events were then appended
    without the key through the shipped `append_manifest_event`). The target does
    not hold the pre-state, so the rollback cannot check the CURRENT TARGET
    IDENTITY against a durable reference and must REFUSE -- never fall back to the
    fresh plan, and never treat the unrecorded entry as confirmed.
    """
    env = synthetic_storage_env
    env.assert_injection()
    state = _s208_run(tmp_path, legacy=False, publish=("graph",), live=False)
    run_dir = state["run_dir"]
    live = _s207_write_manifest(state["plan"], run_dir)
    append_manifest_event(
        live,
        {
            "checkpoint": "publishing",
            "operation": "graph.prestate_revalidated",
            "payload": {"pinned_kind": "regular_file", "observed_kind": "regular_file"},
        },
    )
    append_manifest_event(
        live,
        {
            "checkpoint": "publishing",
            "operation": "graph.staging_published",
            "payload": {"path_present": True},
        },
    )
    assert _s208_tree(state["targets"]["graph"]) != state["prestates"]["graph"]
    manifest_before = live.read_bytes()

    with pytest.raises(ValueError, match="E_ROLLBACK_IDENTITY_MISSING"):
        rollback_profile_migration(live, state["request"])

    assert live.read_bytes() == manifest_before
    assert _s208_failed_current(run_dir) == []


def test_s208_a_backup_hash_mismatch_is_refused_before_anything_moves(
    tmp_path: Path, synthetic_storage_env
) -> None:
    """the card's Stop: a backup hash mismatch refuses with nothing mutated.

    The run's own report is rewritten so that its SELF-digest still validates and
    only one regular-file entry's recorded sha256 stops matching the copy it
    describes -- exactly the mismatch the guard exists for.
    """
    env = synthetic_storage_env
    env.assert_injection()
    state = _s208_run(tmp_path, legacy=False, publish=("graph",))
    run_dir = state["run_dir"]
    report_path = run_dir / profile_migration.BACKUP_REPORT_NAME
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    entries = [entry for entry in payload["entries"] if entry["artifact"] == "target:graph"]
    assert entries and entries[0]["present"] is True
    entries[0]["sha256"] = "0" * 64
    body = {key: value for key, value in payload.items() if key != "digest"}
    payload["digest"] = profile_migration._digest(body)
    report_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    manifest_before = state["manifest_path"].read_bytes()
    published = _s208_tree(state["targets"]["graph"])

    with pytest.raises(ValueError, match="E_ROLLBACK_BACKUP_MISMATCH"):
        rollback_profile_migration(state["manifest_path"], state["request"])

    assert state["manifest_path"].read_bytes() == manifest_before
    assert _s208_tree(state["targets"]["graph"]) == published
    assert _s208_failed_current(run_dir) == []


def test_s208_a_failed_restore_rename_retains_every_copy_and_records_rollback_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, synthetic_storage_env
) -> None:
    """acceptance 5 / DETAIL 10.6 step 10: a failed step retains all copies, no retry.

    The SECOND real ``os.rename`` of the rollback is the restore rename (the first is
    the diagnostic quarantine). It is injected to fail, so the published entry is
    already retained in the diagnostic quarantine and the restore has not happened:
    the run must record `rollback_failed`, keep every copy, and never retry.
    """
    env = synthetic_storage_env
    env.assert_injection()
    state = _s208_run(tmp_path, legacy=True, publish=("vector",))
    run_dir = state["run_dir"]
    target = state["targets"]["vector"]
    published = _s208_tree(target)
    real_rename = os.rename
    seen: list[int] = []

    def _fail_the_second(source: Any, destination: Any, *args: Any, **kwargs: Any) -> None:
        seen.append(1)
        if len(seen) == 2:
            raise OSError(errno.EIO, "injected rollback restore rename failure")
        return real_rename(source, destination, *args, **kwargs)

    monkeypatch.setattr(os, "rename", _fail_the_second)
    with pytest.raises(ValueError, match="E_PUBLICATION_RENAME_FAILED"):
        rollback_profile_migration(state["manifest_path"], state["request"])
    monkeypatch.setattr(os, "rename", real_rename)

    assert len(seen) == 2 and seen == [1, 1], "the rollback renamed more than the two steps it performed"
    recorded = load_manifest(state["manifest_path"])
    assert recorded.status == "rollback_failed"
    assert (recorded.failure or {}).get("code") == "E_PUBLICATION_RENAME_FAILED"
    failed = _s208_failed_current(run_dir)
    assert len(failed) == 1 and _s208_tree(failed[0]) == published, "the quarantined copy was not retained"
    assert not os.path.lexists(target), "the failed restore left something at the target"
    retained = run_dir / profile_migration.QUARANTINE_DIRECTORY_NAME / profile_migration.QUARANTINE_PREPUBLISH_NAME
    assert any(os.path.islink(path) for path in retained.iterdir()), "the run's own quarantine was not retained"
    assert (run_dir / "backup" / "link-entries" / "vector").is_symlink(), "the original backup was lost"
    assert _s207_operations(state["manifest_path"]) == [
        *(f"vector.{name}" for name in S206_PUBLISHED_SEQUENCE),
        "rollback.locked",
        "rollback.current_quarantined",
        S208_FAILED_EVENT,
    ]


def test_s208_a_failed_restore_verification_retains_every_copy_without_an_automatic_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, synthetic_storage_env
) -> None:
    """acceptance 5: the reopen verifies the restored regular artifact; failure keeps all.

    The injected fault calls the REAL restore rename and then appends bytes to the
    file it just put in place, so the verification genuinely reads different bytes.
    The rollback must refuse with the verification code, keep the diagnostic
    quarantine, the original backup and the restored (now unverified) copy, and must
    NOT swap anything again.
    """
    env = synthetic_storage_env
    env.assert_injection()
    state = _s208_run(tmp_path, legacy=False, publish=("graph",))
    run_dir = state["run_dir"]
    target = state["targets"]["graph"]
    published = state["published"]["graph"]
    target_parent = os.path.realpath(target.parent)
    real_rename = os.rename
    restore_renames: list[int] = []

    def _corrupt_after_the_restore_rename(source: Any, destination: Any, *args: Any, **kwargs: Any) -> Any:
        outcome = real_rename(source, destination, *args, **kwargs)
        descriptor = kwargs.get("dst_dir_fd")
        if (
            destination == target.name
            and descriptor is not None
            and os.path.realpath(f"/proc/self/fd/{descriptor}") == target_parent
        ):
            restore_renames.append(1)
            with target.open("ab") as handle:
                handle.write(b"CORRUPTED-AFTER-THE-RESTORE-RENAME")
        return outcome

    # S2-08 fix round 1 (review F3.ii / R3.2): the shipped constant is now the
    # DETAIL-registered name `E_ROLLBACK_VERIFY` (DETAIL.md:807), so this node's
    # expectation was renamed with it. At BASE 54bb05c9 the delta still raises
    # `E_ROLLBACK_VERIFY_FAILED`, so the rename itself is a behavioural difference at BASE.
    monkeypatch.setattr(os, "rename", _corrupt_after_the_restore_rename)
    with pytest.raises(ValueError, match="E_ROLLBACK_VERIFY:"):
        rollback_profile_migration(state["manifest_path"], state["request"])
    monkeypatch.setattr(os, "rename", real_rename)

    assert restore_renames == [1], "the rollback retried the destructive swap"
    recorded = load_manifest(state["manifest_path"])
    assert recorded.status == "rollback_failed"
    assert (recorded.failure or {}).get("code") == "E_ROLLBACK_VERIFY"
    assert target.read_bytes() != (state["prestates"]["graph"][0][3] or b"")
    assert b"CORRUPTED-AFTER" in target.read_bytes()
    assert (run_dir / "backup" / "graph.json").read_bytes() == (state["prestates"]["graph"][0][3] or b"")
    failed = _s208_failed_current(run_dir)
    assert len(failed) == 1 and _s208_tree(failed[0]) == published
    assert _s207_operations(state["manifest_path"]) == [
        *(f"graph.{name}" for name in S206_PUBLISHED_SEQUENCE),
        "rollback.locked",
        "rollback.current_quarantined",
        "rollback.prestate_restored",
        S208_FAILED_EVENT,
    ]


def test_s208_the_rollback_chain_and_the_parent_fsyncs_of_a_restored_artifact_are_recorded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, synthetic_storage_env
) -> None:
    """DETAIL 9.3/10.6 steps 8-9: the rollback chain, the fsyncs and the reopen.

    The REAL ``os.fsync`` calls are observed, so the claim is about the descriptors
    the rollback actually fsynced: the target parent, the diagnostic quarantine
    directory and the run-owned restore staging directory all appear.
    """
    env = synthetic_storage_env
    env.assert_injection()
    state = _s208_run(tmp_path, legacy=False, publish=("graph",))
    run_dir = state["run_dir"]
    fsynced = _s206_fsync_spy(monkeypatch)

    returned = rollback_profile_migration(state["manifest_path"], state["request"])

    assert returned.status == "rolled_back"
    assert _s207_operations(state["manifest_path"]) == [
        *(f"graph.{name}" for name in S206_PUBLISHED_SEQUENCE),
        *S208_ROLLBACK_CHAIN,
    ]
    parents = {
        os.path.realpath(str(state["targets"]["graph"].parent)),
        os.path.realpath(
            str(run_dir / profile_migration.QUARANTINE_DIRECTORY_NAME / S208_QUARANTINE_FAILED_NAME)
        ),
        os.path.realpath(str(run_dir / S208_RESTORE_NAME)),
    }
    assert parents <= set(fsynced), "a parent of a rollback rename was never fsynced"


def test_s208_the_rollback_entrypoint_repeats_the_apply_guards(
    tmp_path: Path, synthetic_storage_env
) -> None:
    """GUARD (PASSES at BASE by design): rollback repeats the apply guards itself.

    Intent, exact canonical target, attestation and the apply flag are re-validated
    by the entrypoint on its own; nothing is inherited from the run that produced the
    manifest. This node is NOT a RED -- the BASE entrypoint already validates these --
    it pins that the rollback slice did not drop them.
    """
    env = synthetic_storage_env
    env.assert_injection()
    state = _s208_run(tmp_path, legacy=True, publish=("vector",))
    tree_before = _s208_tree(state["home"])

    with pytest.raises(ValueError, match="E_STOP_ATTESTATION_REQUIRED"):
        rollback_profile_migration(
            state["manifest_path"], replace(state["request"], stop_attestation=None)
        )
    with pytest.raises(ValueError, match="E_CONFIRM_TARGET_MISMATCH"):
        rollback_profile_migration(
            state["manifest_path"], replace(state["request"], confirm_target=str(state["home"] / "elsewhere"))
        )
    with pytest.raises(ValueError, match="E_APPLY_MODE_REQUIRED"):
        rollback_profile_migration(state["manifest_path"], replace(state["request"], mode="dry-run"))

    assert _s208_tree(state["home"]) == tree_before, "a refused rollback mutated the tree"


# ---------------------------------------------------------------------------
# S2-08 FIX ROUND 1 (attempt 1/2) -- the four nodes that answer the cross-provider
# review's F1 (BLOCKING, the directory restore leg), F2 (the retry disposition, PINNED
# here, not changed), F4 (the pre-move refusal of an added unrecorded backup entry) and
# F5 (the failure record's exception class). Every node drives the REAL
# `rollback_profile_migration` entrypoint; no node fabricates a result.
# ---------------------------------------------------------------------------
def _s208_fix1_content_map(
    tree: tuple[tuple[str, str, int, bytes | None], ...],
) -> dict[str, tuple[str, int | None, str | None]]:
    """`_s208_tree` capture -> {relpath: (kind, size, sha256)}, path by path.

    Only a regular entry carries bytes in the capture, so kind/size/sha256 are the
    fields this fix round's byte-exactness claim is asserted on -- no recursive
    directory digest is invented anywhere, exactly as the identity model requires.
    """
    return {
        relative: (
            kind,
            None if payload is None else len(payload),
            None if payload is None else hashlib.sha256(payload).hexdigest(),
        )
        for relative, kind, _mode, payload in tree
    }


def test_s208_fix1_a_directory_prestate_with_a_subdirectory_is_restored_byte_exactly(
    tmp_path: Path, synthetic_storage_env
) -> None:
    """R1/F1: a pre-state DIRECTORY holding a SUBDIRECTORY is restored byte-exactly.

    BEHAVIOURAL at BASE 54bb05c9, and the failure the RED capture must show is THE TREE
    ASSERTION BELOW -- not a collection ImportError, not a helper TypeError, not a bare
    bare failure. At BASE the rollback quarantines the published entry and then dies
    inside `_stage_restored_tree`: `_run_directory_chain(staged_root_fd, parts[:-1])`
    creates only the leaf's ANCESTORS and the shipped code then `os.chmod`s the LEAF
    `nested`, so the leaf does not exist yet -- `FileNotFoundError: [Errno 2] 'nested'`,
    re-wrapped during unwinding by `open_directory_nofollow` into
    `StorageLockError('storage lock failure')`. THE TARGET IS LEFT ABSENT, so
    `_s208_tree(target)` is `(("", "absent", 0, None),)` while the recorded pre-state is
    the fixture's real `data/lancedb/` tree, and the comparison fails on exactly that.
    The raised exception is captured (never swallowed) and asserted AFTER the tree, so
    the node keeps its own claim: this passes only when the pre-state is really back.

    What is asserted, in order: kind/size/sha256 path-by-path AND the whole no-follow
    tree (modes included) equal the recorded pre-state; the rollback did not raise; the
    run-created entry is retained byte-for-byte in the unique diagnostic quarantine; the
    retained backup still holds the pre-state; `restore/` is empty; the recorded event
    chain is the shipped rollback chain.
    """
    env = synthetic_storage_env
    env.assert_injection()
    state = _s208_run(tmp_path, legacy=False, publish=("vector",))
    target = state["targets"]["vector"]
    run_dir = state["run_dir"]
    prestate = state["prestates"]["vector"]
    assert (target / "nested").is_dir(), "the fixture no longer carries a nested directory"
    backup_before = _s208_tree(run_dir / "backup")

    outcome: Any = None
    try:
        outcome = rollback_profile_migration(state["manifest_path"], state["request"])
    except BaseException as exc:  # noqa: BLE001 - the BASE directory-leg failure is CAPTURED
        outcome = exc

    # THE claim of this node. At BASE the target is absent and THIS is what fails.
    assert _s208_tree(target) == prestate, (
        "the directory pre-state (with its subdirectory) was not restored byte-exactly; "
        f"rollback outcome = {outcome!r}"
    )
    assert _s208_fix1_content_map(_s208_tree(target)) == _s208_fix1_content_map(prestate), (
        "the restored tree does not match the pre-state path-by-path on kind/size/sha256"
    )
    assert not isinstance(outcome, BaseException), f"the rollback raised: {outcome!r}"
    assert outcome.status == "rolled_back"
    assert outcome.checkpoint == "publishing"
    failed = _s208_failed_current(run_dir)
    assert len(failed) == 1, "the run-created entry was not quarantined exactly once"
    assert _s208_tree(failed[0]) == state["published"]["vector"], "the quarantine copy was not retained"
    assert re.fullmatch(r"vector-[0-9a-f]{32}", failed[0].name), "the quarantine name is not unique"
    assert _s208_tree(run_dir / "backup") == backup_before, "the retained backup was changed"
    staging_area = run_dir / S208_RESTORE_NAME
    assert [path.name for path in staging_area.iterdir()] == [], "the staging entry was left behind"
    assert _s207_operations(state["manifest_path"]) == [
        *(f"vector.{name}" for name in S206_PUBLISHED_SEQUENCE),
        *S208_ROLLBACK_CHAIN,
    ]


def test_s208_fix1_an_injected_directory_restore_failure_is_recorded_cause_specifically(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, synthetic_storage_env
) -> None:
    """R1.2/R1.3: a failed DIRECTORY restore is refused cause-specifically, all copies kept.

    BEHAVIOURAL at BASE 54bb05c9: the injected `OSError` escapes the
    `open_directory_nofollow` body, whose unwinding re-wraps it into
    `StorageLockError('storage lock failure')`, so at BASE the durable record stores
    `code='storage lock failure'` -- neither a stable code nor the real cause -- and a
    half-built `restore/<name>-<hex>/stage-*` entry is left behind. So at BASE the first
    failing assertion is the one that requires the refusal to be a `ValueError` carrying
    `E_ROLLBACK_RESTORE_FAILED`; at BASE it is a `StorageLockError`.

    The invocation is monkeypatched (the ONLY way to make the real staging walk fail
    deterministically on this host); the quarantine, the restore area and the backup are
    all the REAL ones the shipped entrypoint works on.

    Asserted: the refusal is this card's `ValueError` family and names
    `E_ROLLBACK_RESTORE_FAILED` AND the underlying cause class; the durable record carries
    that exact code, the `rollback.prestate_restored` step and the root-cause exception
    class; the diagnostic quarantine keeps the published entry; the target was left
    vacant; the backup report and the whole backup tree are byte-identical; `restore/` is
    EMPTY (no half-built staging entry); the chain is `rollback.locked`,
    `rollback.current_quarantined`, `rollback.failed` with no retry event.
    """
    env = synthetic_storage_env
    env.assert_injection()
    state = _s208_run(tmp_path, legacy=False, publish=("vector",))
    target = state["targets"]["vector"]
    run_dir = state["run_dir"]
    published = _s208_tree(target)
    backup_before = _s208_tree(run_dir / "backup")
    report_before = (run_dir / profile_migration.BACKUP_REPORT_NAME).read_bytes()

    def _fail_the_directory_staging(*_args: Any, **_kwargs: Any) -> Any:
        raise OSError(errno.EIO, "fix1-injected directory restore failure")

    monkeypatch.setattr(profile_migration, "_stage_restored_tree", _fail_the_directory_staging)
    outcome: Any = None
    try:
        rollback_profile_migration(state["manifest_path"], state["request"])
    except BaseException as exc:  # noqa: BLE001 - the refusal IS the object under test
        outcome = exc

    assert outcome is not None, "the injected directory-restore failure was not refused"
    assert isinstance(outcome, ValueError), (
        "the restore failure was not mapped out of the storage layer into this card's "
        f"fail-closed refusal family: {outcome!r}"
    )
    assert "E_ROLLBACK_RESTORE_FAILED" in str(outcome), f"the refusal is not cause-specific: {outcome!r}"
    assert "OSError" in str(outcome), f"the refusal does not name the underlying cause class: {outcome!r}"

    recorded = load_manifest(state["manifest_path"])
    assert recorded.status == "rollback_failed"
    failure = recorded.failure or {}
    assert failure.get("code") == "E_ROLLBACK_RESTORE_FAILED", f"the durable code is not the cause: {failure!r}"
    assert failure.get("step") == "rollback.prestate_restored", f"the recorded step is wrong: {failure!r}"
    assert failure.get("exception") == "OSError", f"the failure record omits the exception class: {failure!r}"

    failed = _s208_failed_current(run_dir)
    assert len(failed) == 1 and _s208_tree(failed[0]) == published, "the quarantined copy was not retained"
    assert not os.path.lexists(target), "the failed restore left something at the target"
    assert _s208_tree(run_dir / "backup") == backup_before, "the retained backup was changed"
    assert (run_dir / profile_migration.BACKUP_REPORT_NAME).read_bytes() == report_before
    staging_area = run_dir / S208_RESTORE_NAME
    assert [path.name for path in staging_area.iterdir()] == [], "a half-built staging entry was left behind"
    assert _s207_operations(state["manifest_path"]) == [
        *(f"vector.{name}" for name in S206_PUBLISHED_SEQUENCE),
        "rollback.locked",
        "rollback.current_quarantined",
        S208_FAILED_EVENT,
    ]


def test_s208_fix1_a_clean_retry_after_a_partial_rollback_refuses_with_a_stated_cause(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, synthetic_storage_env
) -> None:
    """R2/F2 disposition PIN -- PASSES at BASE by design; this node is NOT a RED node.

    Two published artifacts with the FOURTH real `os.rename` (the `graph` restore)
    injected to fail is exactly the interrupted state review F2 reproduced: `vector` is
    already restored to its recorded pre-state and `graph` is left vacant. A CLEAN retry
    then refuses `E_ROLLBACK_TARGET_DRIFT` -- "vector is at its recorded pre-state
    although the run's own record records that it published this artifact" -- because a
    restored copy can never match the recorded pre-publish identity and that Stop fires
    before `restore_only` can repair the artifact that is actually vacant.

    THAT IS THE SHIPPED BEHAVIOUR OF THIS ROUND. The retry disposition the review
    preferred was NOT implemented: accepting a restored-but-unrecorded pre-state as
    already done means neutering one arm of the `E_ROLLBACK_TARGET_DRIFT` Stop, and
    making per-artifact restore progress durable means changing the rollback event-chain
    semantics (its event set/order) -- both are outside this round's hard scope limits
    ("do not weaken or alter the four Stop conditions", "do not alter the rollback event
    chain semantics"). The limitation is stated in the fix-round SUMMARY and in the
    corrected comment of `_decide_rollback_artifact`, the residual is owned by S3-06, and
    THIS node pins what ships: a cause-specific refusal, nothing deleted, every copy
    retained, and no automatic retry.

    Asserted: attempt 1 refuses `E_PUBLICATION_RENAME_FAILED` after exactly four renames;
    the retry refuses `E_ROLLBACK_TARGET_DRIFT` WITHOUT rewriting the run's record (the
    refusal is read-only); the `vector` pre-state is back (its symlink raw target); the
    `graph` target is vacant; both published entries are retained in the diagnostic
    quarantine; the retained backup is byte-identical; the chain's tail is
    `rollback.locked, rollback.current_quarantined, rollback.failed`.
    """
    env = synthetic_storage_env
    env.assert_injection()
    state = _s208_run(tmp_path, legacy=True, publish=("vector", "graph"))
    run_dir = state["run_dir"]
    vector_target = state["targets"]["vector"]
    graph_target = state["targets"]["graph"]
    backup_before = _s208_tree(run_dir / "backup")
    real_rename = os.rename
    seen: list[int] = []

    def _fail_the_fourth(source: Any, destination: Any, *args: Any, **kwargs: Any) -> None:
        seen.append(1)
        if len(seen) == 4:
            raise OSError(errno.EIO, "fix1-injected graph restore rename failure")
        return real_rename(source, destination, *args, **kwargs)

    monkeypatch.setattr(os, "rename", _fail_the_fourth)
    first: Any = None
    try:
        rollback_profile_migration(state["manifest_path"], state["request"])
    except BaseException as exc:  # noqa: BLE001 - attempt 1's refusal is asserted below
        first = exc
    monkeypatch.setattr(os, "rename", real_rename)

    assert len(seen) == 4, f"attempt 1 performed {len(seen)} renames, not the four of this state"
    assert isinstance(first, ValueError) and "E_PUBLICATION_RENAME_FAILED" in str(first), (
        f"attempt 1 did not refuse as the interrupted publication: {first!r}"
    )
    assert load_manifest(state["manifest_path"]).status == "rollback_failed"
    manifest_before_retry = state["manifest_path"].read_bytes()

    with pytest.raises(ValueError, match="E_ROLLBACK_TARGET_DRIFT"):
        rollback_profile_migration(state["manifest_path"], state["request"])

    assert state["manifest_path"].read_bytes() == manifest_before_retry, (
        "the refused retry rewrote the run's own record instead of refusing read-only"
    )
    # The partial attempt's OWN record is what stands: the retry refused before the chain.
    recorded = load_manifest(state["manifest_path"])
    assert recorded.status == "rollback_failed"
    assert (recorded.failure or {}).get("code") == "E_PUBLICATION_RENAME_FAILED"
    # Every copy is retained and nothing was deleted by either attempt.
    assert _s208_tree(vector_target) == state["prestates"]["vector"], "the restored pre-state was disturbed"
    assert not os.path.lexists(graph_target), "the vacant artifact is not vacant"
    failed = _s208_failed_current(run_dir)
    assert len(failed) == 2, "the two published entries were not both retained in the quarantine"
    assert _s208_tree(run_dir / "backup") == backup_before, "the retained backup was changed"
    assert _s207_operations(state["manifest_path"])[-3:] == [
        "rollback.locked",
        "rollback.current_quarantined",
        S208_FAILED_EVENT,
    ]


def test_s208_fix1_an_unrecorded_file_added_to_the_backup_is_refused_before_anything_moves(
    tmp_path: Path, synthetic_storage_env
) -> None:
    """R4/F4: an ADDED unrecorded entry in the backup is refused BEFORE the quarantine.

    BEHAVIOURAL at BASE 54bb05c9: there the added file is noticed only inside
    `_stage_restored_tree`, i.e. AFTER the run-created entry has already been quarantined
    (and the internal refusal is masked into `StorageLockError('storage lock failure')`),
    so at BASE the FIRST failing assertion is the whole-home fingerprint below -- the
    target has been moved away and a half-built `restore/` entry has appeared. The refusal
    must instead happen in the READ-ONLY decision phase (reusing the retained-backup
    verification path, not a second one), where nothing has moved yet.

    Asserted: the refusal is this card's `ValueError` family and names
    `E_ROLLBACK_IDENTITY_MISSING`; the whole home tree fingerprint AND the manifest bytes
    are identical before/after; no diagnostic quarantine entry appears; the published
    entry is untouched.
    """
    env = synthetic_storage_env
    env.assert_injection()
    state = _s208_run(tmp_path, legacy=False, publish=("vector",))
    run_dir = state["run_dir"]
    target = state["targets"]["vector"]
    entries = _s208_backup_entries(run_dir)
    backup_root = run_dir / entries["target:vector"]["run_relative_path"]
    (backup_root / "RV-UNRECORDED-EXTRA.bin").write_bytes(b"RV-UNRECORDED-EXTRA")
    home_before = _s208_tree(state["home"])
    manifest_before = state["manifest_path"].read_bytes()

    outcome: Any = None
    try:
        rollback_profile_migration(state["manifest_path"], state["request"])
    except BaseException as exc:  # noqa: BLE001 - the refusal IS the object under test
        outcome = exc

    # THE claim of this node. At BASE the quarantine already moved the published entry.
    assert _s208_tree(state["home"]) == home_before, (
        f"a refused rollback mutated the home tree; rollback outcome = {outcome!r}"
    )
    assert state["manifest_path"].read_bytes() == manifest_before, "a refusal rewrote the run's record"
    assert isinstance(outcome, ValueError), (
        f"the added unrecorded entry was not refused cause-specifically: {outcome!r}"
    )
    assert "E_ROLLBACK_IDENTITY_MISSING" in str(outcome), f"the refusal is not cause-specific: {outcome!r}"
    assert _s208_failed_current(run_dir) == [], "the refusal happened AFTER something moved"
    assert _s208_tree(target) == state["published"]["vector"], "the published entry was disturbed"


class _CountingEmbedder:
    def __init__(self, dimension: int = 4, fail: BaseException | None = None) -> None:
        self.dimension = dimension
        self.fail = fail
        self.calls = 0

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        self.calls += 1
        if self.fail is not None:
            raise self.fail
        return [[float(index + 1)] * self.dimension for index, _ in enumerate(texts)]


@pytest.mark.asyncio
async def test_s304_dry_run_is_embedder_free_and_digest_is_independent(tmp_path: Path) -> None:
    db_path = tmp_path / "snapshot.db"
    _seed_s301_snapshot(db_path)
    embedder = _CountingEmbedder()
    report = await projection_rebuild.rebuild_projections(
        f"sqlite+aiosqlite:///{db_path}",
        staging_vector_path=tmp_path / "vectors",
        staging_graph_path=tmp_path / "graph.json",
        embedder=embedder,
        vector_size=4,
        batch_size=2,
        dry_run=True,
    )
    assert report.plan.backend == "local"
    assert report.plan.eligible_records == 2
    assert report.plan.batches == 2
    assert report.plan.estimated_cost == 2
    assert report.plan.estimator == "vector-records"
    assert report.plan.digest == hashlib.sha256("belief:b1\ndecision:d1\nfact:f1\nskill:s1".encode()).hexdigest()
    assert embedder.calls == 0
    assert not (tmp_path / "vectors").exists()
    assert not (tmp_path / "graph.json").exists()


@pytest.mark.asyncio
async def test_s304_real_sqlite_lancedb_graph_and_checkpoint_resume(tmp_path: Path) -> None:
    db_path = tmp_path / "snapshot.db"
    _seed_s301_snapshot(db_path)
    embedder = _CountingEmbedder()
    checkpoint = tmp_path / "checkpoint.json"
    result = await projection_rebuild.rebuild_projections(
        f"sqlite+aiosqlite:///{db_path}",
        staging_vector_path=tmp_path / "vectors",
        staging_graph_path=tmp_path / "graph.json",
        embedder=embedder,
        vector_size=4,
        batch_size=2,
        checkpoint_path=checkpoint,
        embedding_plan_digest=projection_rebuild.embedding_plan_digest(
            [record async for record in projection_rebuild.iter_canonical_projection_records(
                f"sqlite+aiosqlite:///{db_path}", batch_size=2
            )]
        ),
    )
    assert result.completed_batches == 2
    state = json.loads(checkpoint.read_text())
    assert state["last_completed_key"] == ["skill", "s1"]
    assert state["count"] == 4
    assert state["id_digest"] == hashlib.sha256("belief:b1\ndecision:d1\nfact:f1\nskill:s1".encode()).hexdigest()
    assert set(result.vector_ids) == {str(uuid5(NAMESPACE_DNS, "fact:f1")), str(uuid5(NAMESPACE_DNS, "belief:b1"))}
    resumed = await projection_rebuild.rebuild_projections(
        f"sqlite+aiosqlite:///{db_path}",
        staging_vector_path=tmp_path / "vectors",
        staging_graph_path=tmp_path / "graph.json",
        embedder=embedder,
        vector_size=4,
        batch_size=2,
        checkpoint_path=checkpoint,
        resume=True,
        embedding_plan_digest=result.plan.digest,
    )
    assert resumed.vector_ids == result.vector_ids
    assert resumed.graph_nodes_digest == result.graph_nodes_digest
    assert resumed.graph_edges_digest == result.graph_edges_digest


@pytest.mark.asyncio
async def test_s304_plan_drift_and_backend_failure_are_resumable(tmp_path: Path) -> None:
    db_path = tmp_path / "snapshot.db"
    _seed_s301_snapshot(db_path)
    checkpoint = tmp_path / "checkpoint.json"
    with pytest.raises(ValueError, match="E_EMBEDDING_PLAN_STALE"):
        await projection_rebuild.rebuild_projections(
            f"sqlite+aiosqlite:///{db_path}", staging_vector_path=tmp_path / "vectors",
            staging_graph_path=tmp_path / "graph.json", embedder=_CountingEmbedder(),
            vector_size=4, batch_size=2, checkpoint_path=checkpoint,
            embedding_plan_digest="0" * 64,
        )
    embedder = _CountingEmbedder(fail=RuntimeError("quota"))
    with pytest.raises(RuntimeError, match="quota"):
        await projection_rebuild.rebuild_projections(
            f"sqlite+aiosqlite:///{db_path}", staging_vector_path=tmp_path / "vectors",
            staging_graph_path=tmp_path / "graph.json", embedder=embedder,
            vector_size=4, batch_size=2, checkpoint_path=checkpoint,
            embedding_plan_digest=projection_rebuild.embedding_plan_digest(
                [record async for record in projection_rebuild.iter_canonical_projection_records(
                    f"sqlite+aiosqlite:///{db_path}", batch_size=2
                )]
            ),
        )
    state = json.loads(checkpoint.read_text())
    assert state["status"] == "resumable"
    assert state["count"] == 0
    assert not (tmp_path / "graph.json").exists()


@pytest.mark.asyncio
async def test_s304_remote_backend_requires_explicit_allow_network(tmp_path: Path) -> None:
    db_path = tmp_path / "snapshot.db"
    _seed_s301_snapshot(db_path)
    with pytest.raises(PermissionError, match="allow-network"):
        await projection_rebuild.rebuild_projections(
            f"sqlite+aiosqlite:///{db_path}", staging_vector_path=tmp_path / "vectors",
            staging_graph_path=tmp_path / "graph.json", embedder=_CountingEmbedder(),
            backend="remote", allow_network=False, dry_run=False,
        )


@pytest.mark.asyncio
async def test_s304_wrong_dimension_stays_resumable_without_graph(tmp_path: Path) -> None:
    db_path = tmp_path / "snapshot.db"
    _seed_s301_snapshot(db_path)
    checkpoint = tmp_path / "checkpoint.json"
    records = [record async for record in projection_rebuild.iter_canonical_projection_records(
        f"sqlite+aiosqlite:///{db_path}", batch_size=2
    )]
    with pytest.raises(ValueError, match="E_EMBEDDING_DIMENSION"):
        await projection_rebuild.rebuild_projections(
            f"sqlite+aiosqlite:///{db_path}", staging_vector_path=tmp_path / "vectors",
            staging_graph_path=tmp_path / "graph.json", embedder=_CountingEmbedder(dimension=3),
            vector_size=4, batch_size=2, checkpoint_path=checkpoint,
            embedding_plan_digest=projection_rebuild.embedding_plan_digest(records),
        )
    state = json.loads(checkpoint.read_text())
    assert state["status"] == "resumable"
    assert state["count"] == 0
    assert not (tmp_path / "graph.json").exists()


@pytest.mark.asyncio
async def test_s304_remote_backend_uses_local_fake_http_endpoint(tmp_path: Path) -> None:
    db_path = tmp_path / "snapshot.db"
    _seed_s301_snapshot(db_path)

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            size = int(self.headers["Content-Length"])
            import json as _json
            count = len(_json.loads(self.rfile.read(size))["input"])
            body = _json.dumps({"data": [{"index": i, "embedding": [0.1] * 4} for i in range(count)]}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args: Any) -> None:
            return

    try:
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    except PermissionError:
        pytest.skip("certified sandbox denies local socket binding")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        from memory_server.providers.embedding_provider import OpenAIEmbeddingProvider
        embedder = OpenAIEmbeddingProvider(base_url=f"http://127.0.0.1:{server.server_port}/v1", api_key="local-fake")
        records = [record async for record in projection_rebuild.iter_canonical_projection_records(
            f"sqlite+aiosqlite:///{db_path}", batch_size=4
        )]
        result = await projection_rebuild.rebuild_projections(
            f"sqlite+aiosqlite:///{db_path}", staging_vector_path=tmp_path / "vectors",
            staging_graph_path=tmp_path / "graph.json", embedder=embedder,
            backend="remote", allow_network=True, vector_size=4, batch_size=4,
            embedding_plan_digest=projection_rebuild.embedding_plan_digest(records),
        )
        assert result.plan.network is True
        assert len(result.vector_ids) == 2
    finally:
        server.shutdown()
        server.server_close()


class _S304PartialEmbedder(_CountingEmbedder):
    def __init__(self) -> None:
        super().__init__()
        self.fail_on_second = True
        self.text_batches: list[list[str]] = []

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        self.text_batches.append(list(texts))
        if self.fail_on_second and self.calls == 1:
            self.calls += 1
            raise RuntimeError("quota-on-second-batch")
        return super().embed_batch(texts)


def _s304_plan_digest(records: list[CanonicalProjectionRecord]) -> str:
    return projection_rebuild.embedding_plan_digest(records)


async def _s304_make_partial_checkpoint(tmp_path: Path) -> tuple[Path, _S304PartialEmbedder, str]:
    db_path = tmp_path / "snapshot.db"
    _seed_s301_snapshot(db_path)
    records = [record async for record in projection_rebuild.iter_canonical_projection_records(
        f"sqlite+aiosqlite:///{db_path}", batch_size=2
    )]
    embedder = _S304PartialEmbedder()
    checkpoint = tmp_path / "checkpoint.json"
    with pytest.raises(RuntimeError, match="quota-on-second-batch"):
        await projection_rebuild.rebuild_projections(
            f"sqlite+aiosqlite:///{db_path}", staging_vector_path=tmp_path / "vectors",
            staging_graph_path=tmp_path / "graph.json", embedder=embedder, vector_size=4,
            batch_size=2, checkpoint_path=checkpoint, embedding_plan_digest=_s304_plan_digest(records),
        )
    state = json.loads(checkpoint.read_text())
    assert state["last_completed_key"] == ["decision", "d1"]
    return checkpoint, embedder, f"sqlite+aiosqlite:///{db_path}"


@pytest.mark.asyncio
async def test_s304_resume_tampered_id_digest_refuses(tmp_path: Path) -> None:
    checkpoint, embedder, snapshot_url = await _s304_make_partial_checkpoint(tmp_path)
    state = json.loads(checkpoint.read_text())
    state["id_digest"] = "0" * 64
    checkpoint.write_text(json.dumps(state))
    embedder.fail_on_second = False
    with pytest.raises(ValueError, match="E_BATCH_DIGEST_MISMATCH"):
        await projection_rebuild.rebuild_projections(
            snapshot_url, staging_vector_path=tmp_path / "vectors", staging_graph_path=tmp_path / "graph.json",
            embedder=embedder, vector_size=4, batch_size=2, checkpoint_path=checkpoint, resume=True,
            embedding_plan_digest=projection_rebuild.embedding_plan_digest(
                [
                    record
                    async for record in projection_rebuild.iter_canonical_projection_records(snapshot_url, batch_size=2)
                ]
            ),
        )


@pytest.mark.asyncio
async def test_s304_resume_staged_id_mismatch_refuses(tmp_path: Path) -> None:
    checkpoint, embedder, snapshot_url = await _s304_make_partial_checkpoint(tmp_path)
    state = json.loads(checkpoint.read_text())
    state["staged_ids"] = ["belief:b1", "fact:f1"]
    state["id_digest"] = projection_rebuild.id_digest(set(state["staged_ids"]))
    checkpoint.write_text(json.dumps(state))
    embedder.fail_on_second = False
    with pytest.raises(ValueError, match="E_STAGED_IDS_MISMATCH"):
        await projection_rebuild.rebuild_projections(
            snapshot_url, staging_vector_path=tmp_path / "vectors", staging_graph_path=tmp_path / "graph.json",
            embedder=embedder, vector_size=4, batch_size=2, checkpoint_path=checkpoint, resume=True,
            embedding_plan_digest=projection_rebuild.embedding_plan_digest(
                [
                    record
                    async for record in projection_rebuild.iter_canonical_projection_records(snapshot_url, batch_size=2)
                ]
            ),
        )


@pytest.mark.asyncio
async def test_s304_resume_plan_digest_drift_refuses(tmp_path: Path) -> None:
    checkpoint, embedder, snapshot_url = await _s304_make_partial_checkpoint(tmp_path)
    state = json.loads(checkpoint.read_text())
    state["plan_digest"] = "1" * 64
    checkpoint.write_text(json.dumps(state))
    embedder.fail_on_second = False
    records = [
        record
        async for record in projection_rebuild.iter_canonical_projection_records(snapshot_url, batch_size=2)
    ]
    with pytest.raises(ValueError, match="E_EMBEDDING_PLAN_STALE"):
        await projection_rebuild.rebuild_projections(
            snapshot_url, staging_vector_path=tmp_path / "vectors", staging_graph_path=tmp_path / "graph.json",
            embedder=embedder, vector_size=4, batch_size=2, checkpoint_path=checkpoint, resume=True,
            embedding_plan_digest=_s304_plan_digest(records),
        )


@pytest.mark.asyncio
async def test_s304_resume_partial_embeds_only_remaining_batches(tmp_path: Path) -> None:
    checkpoint, embedder, snapshot_url = await _s304_make_partial_checkpoint(tmp_path)
    first_calls = embedder.calls
    embedder.fail_on_second = False
    records = [
        record
        async for record in projection_rebuild.iter_canonical_projection_records(snapshot_url, batch_size=2)
    ]
    result = await projection_rebuild.rebuild_projections(
        snapshot_url, staging_vector_path=tmp_path / "vectors", staging_graph_path=tmp_path / "graph.json",
        embedder=embedder, vector_size=4, batch_size=2, checkpoint_path=checkpoint, resume=True,
        embedding_plan_digest=_s304_plan_digest(records),
    )
    assert first_calls == 2
    assert embedder.calls == 3
    assert embedder.text_batches[-1] == ["A is B"]
    assert result.completed_batches == 2


@pytest.mark.asyncio
async def test_s304_apply_requires_embedding_plan_digest(tmp_path: Path) -> None:
    db_path = tmp_path / "snapshot.db"
    _seed_s301_snapshot(db_path)
    with pytest.raises(ValueError, match="E_EMBEDDING_PLAN_DIGEST_REQUIRED"):
        await projection_rebuild.rebuild_projections(
            f"sqlite+aiosqlite:///{db_path}", staging_vector_path=tmp_path / "vectors",
            staging_graph_path=tmp_path / "graph.json", embedder=_CountingEmbedder(),
            vector_size=4, batch_size=2,
        )


@pytest.mark.asyncio
async def test_s304_remote_allow_path_uses_confined_synthetic_embedder(tmp_path: Path) -> None:
    db_path = tmp_path / "snapshot.db"
    _seed_s301_snapshot(db_path)
    records = [record async for record in projection_rebuild.iter_canonical_projection_records(
        f"sqlite+aiosqlite:///{db_path}", batch_size=4
    )]
    embedder = _CountingEmbedder()
    result = await projection_rebuild.rebuild_projections(
        f"sqlite+aiosqlite:///{db_path}", staging_vector_path=tmp_path / "vectors",
        staging_graph_path=tmp_path / "graph.json", embedder=embedder, backend="remote",
        allow_network=True, vector_size=4, batch_size=4, embedding_plan_digest=_s304_plan_digest(records),
    )
    assert result.plan.network is True
    assert result.plan.eligible_records == 2
    assert embedder.calls == 1
    assert len(result.vector_ids) == 2


# ---------------------------------------------------------------------------
# S3-05 -- independent staged/published verification
# (DETAIL 10.3, DETAIL.md:637-645; DETAIL 19.10, DETAIL.md:1077; addendum PART A)
#
# The verifier under test is `memory_server.projection_rebuild`: the ONE place
# staged/published verification lives. Every node drives REAL artifacts -- a
# real SQLite snapshot carrying the accepted revision, a real LanceDB staged
# table built through the real provider and a real graph JSON -- so no
# verification-boundary input here is a mock. The expected node/edge/vector
# sets are derived by the verifier ITSELF from the canonical snapshot, never
# from `RebuildResult`, so these nodes also pin that the engine cannot hand it
# the rebuild's own answer.
# ---------------------------------------------------------------------------

S305_VECTOR_SIZE = 4

_S305_CANONICAL_TABLES = """
        CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL);
        CREATE TABLE beliefs (id TEXT PRIMARY KEY, proposition TEXT, confidence REAL,
            source TEXT, tags TEXT, lifecycle_state TEXT);
        CREATE TABLE decisions (id TEXT PRIMARY KEY, choice TEXT, reason TEXT,
            context TEXT, lifecycle_state TEXT);
        CREATE TABLE facts (id TEXT PRIMARY KEY, subject TEXT, predicate TEXT,
            object TEXT, source TEXT, lifecycle_state TEXT);
        CREATE TABLE skills (id TEXT PRIMARY KEY, purpose TEXT, steps TEXT,
            lifecycle_state TEXT);
        CREATE TABLE outbox_entries (id TEXT PRIMARY KEY, status TEXT, created_at TEXT);
        INSERT INTO alembic_version VALUES ('0005');
"""


def _s305_snapshot(path: Path, *, extra_fact: bool = False, null_source_fact: bool = False) -> str:
    """A real snapshot the verifier can verify, with a real eligible corpus.

    Eligible projection mapping for the default corpus (DETAIL 10.2 / addendum
    A.3.2): vectors `fact:f1`, `fact:f2`, `belief:b1`; graph nodes `widget`,
    `caddy`, `b`, `c` (entity), `decision-pick-caddy` (decision, both `d1` and
    the case-colliding `d2` collapse onto it) and `skill-usable` (skill); graph
    edges `widget|uses|caddy`, `b|uses|c` and exactly one
    `decision-pick-caddy|widget|decides`.
    """
    connection = sqlite3.connect(path)
    connection.executescript(_S305_CANONICAL_TABLES)
    connection.executescript("""
        INSERT INTO beliefs VALUES ('b1', 'active belief', 0.8, 's', '["tag"]', 'active');
        INSERT INTO decisions VALUES ('d1', 'Pick Caddy', 'safe', 'Widget', 'active');
        INSERT INTO decisions VALUES ('d2', 'pick caddy', 'same', 'Widget', 'active');
        INSERT INTO facts VALUES ('f0', 'A', 'is', 'B', 's', 'archived');
        INSERT INTO facts VALUES ('f1', 'Widget', 'uses', 'Caddy', 's', 'active');
        INSERT INTO facts VALUES ('f2', 'B', 'uses', 'C', 's', 'validated');
        INSERT INTO skills VALUES ('s1', 'usable', '["step"]', 'validated');
        INSERT INTO skills VALUES ('s0', 'empty', '[]', 'active');
        INSERT INTO outbox_entries VALUES ('o1', 'pending', '2026-09-13T00:00:00Z');
    """)
    if extra_fact:
        connection.execute(
            "INSERT INTO facts VALUES ('f3', 'Extra', 'uses', 'Delta', 's', 'active')"
        )
    if null_source_fact:
        connection.execute(
            "INSERT INTO facts VALUES ('f4', 'Nulled', 'uses', 'Source', NULL, 'active')"
        )
    connection.commit()
    connection.close()
    return f"sqlite+aiosqlite:///{path}"


def _s305_expected_vector_ids() -> set[str]:
    """The pinned vector ID set, derived in the TEST from the corpus mapping."""
    return {
        str(uuid5(NAMESPACE_DNS, "fact:f1")),
        str(uuid5(NAMESPACE_DNS, "fact:f2")),
        str(uuid5(NAMESPACE_DNS, "belief:b1")),
    }


_S305_EXPECTED_NODE_IDS = {"widget", "caddy", "b", "c", "decision-pick-caddy", "skill-usable"}
# A.3.3 digest domain: the edge KEY is `source_id|target_id|relation`, exactly
# as `projection_rebuild.graph_id_digests` already encodes it.
_S305_EXPECTED_EDGE_KEYS = {
    "widget|caddy|uses",
    "b|c|uses",
    "decision-pick-caddy|widget|decides",
}


async def _s305_staged(tmp_path: Path, db_path: Path, *, run_dir: Path | None = None) -> tuple[str, Path, Path]:
    """Build REAL staged artifacts with the S3-04 rebuild into a staged layout.

    The staged layout is the S2-06 engine's own DETAIL 9.1 shape:
    ``<run_dir>/staging/lancedb`` and ``<run_dir>/staging/graph.json``.
    """
    url = f"sqlite+aiosqlite:///{db_path}"
    root = tmp_path if run_dir is None else run_dir
    vector_path = root / "staging" / "lancedb"
    graph_path = root / "staging" / "graph.json"
    records = [
        record
        async for record in projection_rebuild.iter_canonical_projection_records(url, batch_size=2)
    ]
    await projection_rebuild.rebuild_projections(
        url,
        staging_vector_path=vector_path,
        staging_graph_path=graph_path,
        embedder=_CountingEmbedder(dimension=S305_VECTOR_SIZE),
        vector_size=S305_VECTOR_SIZE,
        batch_size=2,
        embedding_plan_digest=projection_rebuild.embedding_plan_digest(records),
    )
    return url, vector_path, graph_path


async def _s305_verify(url: str, vector_path: Path, graph_path: Path):
    """Ask the ONE verifier with the keyword-only contract it owns."""
    return await projection_rebuild.verify_staged_projections(
        snapshot_url=url,
        staging_vector_path=vector_path,
        staging_graph_path=graph_path,
        expected_vector_size=S305_VECTOR_SIZE,
    )


def _s305_graph_json(graph_path: Path) -> dict:
    return json.loads(graph_path.read_text(encoding="utf-8"))


def _s305_write_graph_json(graph_path: Path, data: dict) -> None:
    graph_path.write_text(json.dumps(data), encoding="utf-8")


def _s305_inventory(root: Path) -> dict[str, str]:
    """Path -> sha256 for every regular file under *root*, for an untouched check."""
    inventory: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if path.is_file():
            inventory[str(path.relative_to(root))] = hashlib.sha256(path.read_bytes()).hexdigest()
    return inventory


@pytest.mark.asyncio
async def test_s305_staged_verification_is_green_on_real_artifacts(tmp_path: Path) -> None:
    """Acceptance 1: the ACTUAL reopened artifacts must match exact expectations.

    At BASE the seam is the unconditional stub, so this node dies on
    ``verdict.valid is True`` with no identity at all: MISSING CAPABILITY.
    """
    db_path = tmp_path / "snapshot.db"
    url = _s305_snapshot(db_path)
    url, vector_path, graph_path = await _s305_staged(tmp_path, db_path)

    verdict = await _s305_verify(url, vector_path, graph_path)

    assert verdict.valid is True, verdict.errors
    assert verdict.errors == ()
    assert verdict.basis == "staged"
    assert tuple(verdict.artifacts) == ("vector", "graph")
    # Actual reopened vector identity.
    assert set(verdict.vector_ids) == _s305_expected_vector_ids()
    assert verdict.vector_ids_digest == projection_rebuild.id_digest(_s305_expected_vector_ids())
    assert verdict.vector_row_count == 3
    assert verdict.vector_dimension == S305_VECTOR_SIZE
    assert verdict.vector_table == "memories"
    # Actual reopened graph identity, in the A.3.3 digest domain (ids + edge keys).
    assert verdict.graph_nodes_digest == projection_rebuild.id_digest(_S305_EXPECTED_NODE_IDS)
    assert verdict.graph_edges_digest == projection_rebuild.id_digest(_S305_EXPECTED_EDGE_KEYS)
    assert verdict.graph_node_count == 6
    assert verdict.graph_edge_count == 3
    # Snapshot integrity / schema / revision (DETAIL 10.3 first bullet).
    assert verdict.snapshot_integrity == "ok"
    assert verdict.snapshot_revision == "0005"
    assert verdict.outbox_counts.get("pending") == 1
    # R-S302-a: the plain-table metric is NOT artifact evidence.
    assert verdict.metric_verifiable is False
    assert len(verdict.staging_digest) == 64
    assert verdict.expected_vector_ids == tuple(sorted(_s305_expected_vector_ids()))


@pytest.mark.asyncio
async def test_s305_expected_sets_come_from_the_snapshot_not_from_the_rebuild(
    tmp_path: Path,
) -> None:
    """Acceptance 2: expectations are re-derived from the snapshot handed in.

    The staged artifacts are built from ONE corpus and then verified against a
    DIFFERENT snapshot (one extra eligible fact). An expectation echoed from the
    rebuild's own `RebuildResult` -- or read back off the staged store -- would
    accept it; an independent derivation from the snapshot must refuse.

    At BASE the stub answers `valid=True`: BEHAVIOURAL failure.
    """
    built_db = tmp_path / "built.db"
    _s305_snapshot(built_db)
    _, vector_path, graph_path = await _s305_staged(tmp_path, built_db)

    moved_on_db = tmp_path / "moved-on.db"
    moved_on_url = _s305_snapshot(moved_on_db, extra_fact=True)
    verdict = await _s305_verify(moved_on_url, vector_path, graph_path)

    assert verdict.valid is False, "a changed canonical corpus must refuse the artifact"
    assert any("ID set mismatch" in error or "digest" in error for error in verdict.errors), verdict.errors
    assert str(uuid5(NAMESPACE_DNS, "fact:f3")) in verdict.expected_vector_ids
    assert str(uuid5(NAMESPACE_DNS, "fact:f3")) not in set(verdict.vector_ids)


@pytest.mark.asyncio
async def test_s305_same_counts_wrong_ids_must_fail(tmp_path: Path) -> None:
    """Acceptance 4: equal counts with WRONG IDs must fail (counts are never proof).

    One real ID is replaced by a real stranger ID, so the staged store keeps
    exactly three rows and three distinct ids.

    At BASE the stub answers `valid=True`: BEHAVIOURAL failure.
    """
    from memory_server.providers.lancedb_provider import LanceDBProvider

    db_path = tmp_path / "snapshot.db"
    url = _s305_snapshot(db_path)
    url, vector_path, graph_path = await _s305_staged(tmp_path, db_path)

    provider = LanceDBProvider(db_path=str(vector_path), vector_size=S305_VECTOR_SIZE)
    await provider.delete(point_id=str(uuid5(NAMESPACE_DNS, "fact:f1")))
    await provider.upsert_batch([
        {
            "id": "ffffffff-0000-0000-0000-000000000001",
            "vector": [0.5] * S305_VECTOR_SIZE,
            "payload": {
                "subject": "Widget", "predicate": "uses", "object": "Caddy",
                "source": "s", "memory_type": "fact",
            },
        }
    ])
    await provider.close()

    description = await LanceDBProvider(
        db_path=str(vector_path), vector_size=S305_VECTOR_SIZE
    ).describe_collection()
    assert description.row_count == 3, "the adversarial store must keep the SAME count"

    verdict = await _s305_verify(url, vector_path, graph_path)

    assert verdict.valid is False, "same counts with wrong IDs must never pass"
    assert any("ID set mismatch" in error for error in verdict.errors), verdict.errors


@pytest.mark.asyncio
async def test_s305_tampered_payload_is_refused(tmp_path: Path) -> None:
    """Acceptance 1 + R-S302-g: the payload allowlist is enforced per row.

    The same ID is rewritten with a payload that carries an unknown key (and a
    NULL `source` variant in the second half), so the ID set, the ID digest and
    the row count are all unchanged and ONLY the payload contract can catch it.

    At BASE the stub answers `valid=True`: BEHAVIOURAL failure.
    """
    from memory_server.providers.lancedb_provider import LanceDBProvider

    db_path = tmp_path / "snapshot.db"
    url = _s305_snapshot(db_path)
    url, vector_path, graph_path = await _s305_staged(tmp_path, db_path)

    provider = LanceDBProvider(db_path=str(vector_path), vector_size=S305_VECTOR_SIZE)
    await provider.upsert_batch([
        {
            "id": str(uuid5(NAMESPACE_DNS, "fact:f1")),
            "vector": [0.5] * S305_VECTOR_SIZE,
            "payload": {
                "subject": "Widget", "predicate": "uses", "object": "Caddy",
                "source": "s", "memory_type": "fact", "extra_key": "tampered",
            },
        }
    ])
    await provider.close()

    verdict = await _s305_verify(url, vector_path, graph_path)

    assert verdict.valid is False, "an unknown payload key must refuse the artifact"
    assert any("unknown payload keys" in error for error in verdict.errors), verdict.errors
    assert any("invalid payload" in error for error in verdict.errors), verdict.errors


@pytest.mark.asyncio
async def test_s305_null_source_payload_refuses_the_whole_artifact(tmp_path: Path) -> None:
    """R-S302-g decision: a NULL canonical `source` refuses the artifact, verbatim.

    `_PAYLOAD_CONTRACT` requires a string `source`, so the verifier must surface
    the provider's cause-specific refusal (it is NOT weakened here) and fail the
    whole artifact closed.

    At BASE the stub answers `valid=True`: BEHAVIOURAL failure.
    """
    db_path = tmp_path / "snapshot.db"
    url = _s305_snapshot(db_path, null_source_fact=True)
    url, vector_path, graph_path = await _s305_staged(tmp_path, db_path)

    verdict = await _s305_verify(url, vector_path, graph_path)

    assert verdict.valid is False, "a NULL source payload must refuse the artifact"
    assert any(
        "wrong value type for 'source'" in error for error in verdict.errors
    ), verdict.errors


@pytest.mark.asyncio
async def test_s305_tampered_vector_dimension_is_refused(tmp_path: Path) -> None:
    """Acceptance 1: the ACTUAL reopened dimension must be the expected one.

    A staged store is built at dimension 7 and verified against the run's
    expected dimension 4.

    At BASE the stub answers `valid=True`: BEHAVIOURAL failure.
    """
    from memory_server.providers.lancedb_provider import LanceDBProvider

    db_path = tmp_path / "snapshot.db"
    url = _s305_snapshot(db_path)
    vector_path = tmp_path / "staging" / "lancedb"
    graph_path = tmp_path / "staging" / "graph.json"
    records = [
        record
        async for record in projection_rebuild.iter_canonical_projection_records(url, batch_size=2)
    ]
    await projection_rebuild.rebuild_projections(
        url,
        staging_vector_path=vector_path,
        staging_graph_path=graph_path,
        embedder=_CountingEmbedder(dimension=7),
        vector_size=7,
        batch_size=2,
        embedding_plan_digest=projection_rebuild.embedding_plan_digest(records),
    )
    description = await LanceDBProvider(db_path=str(vector_path), vector_size=7).describe_collection()
    assert description.vector_size == 7

    verdict = await _s305_verify(url, vector_path, graph_path)

    assert verdict.valid is False, "a dimension mismatch must refuse the artifact"
    assert any("dimension" in error for error in verdict.errors), verdict.errors


@pytest.mark.asyncio
async def test_s305_missing_graph_edge_must_fail(tmp_path: Path) -> None:
    """Acceptance 1/4: a missing graph edge must fail even with equal counts.

    The staged graph stays structurally valid (S3-03's own hook accepts it and
    every node still exists), so ONLY the exact expected edge-key comparison can
    catch the dropped edge.

    At BASE the stub answers `valid=True`: BEHAVIOURAL failure.
    """
    from memory_server.providers.graph_provider import SimpleGraph

    db_path = tmp_path / "snapshot.db"
    url = _s305_snapshot(db_path)
    url, vector_path, graph_path = await _s305_staged(tmp_path, db_path)

    data = _s305_graph_json(graph_path)
    removed = [edge for edge in data["edges"] if edge["target_id"] == "caddy"]
    assert removed, "the fixture must contain the edge this node removes"
    data["edges"] = [edge for edge in data["edges"] if edge["target_id"] != "caddy"]
    _s305_write_graph_json(graph_path, data)
    validation = SimpleGraph.validate_snapshot(graph_path)
    assert validation.valid is True, validation.error
    assert validation.node_count == 6
    assert validation.edge_count == 2

    verdict = await _s305_verify(url, vector_path, graph_path)

    assert verdict.valid is False, "a missing graph edge must refuse the artifact"
    assert any("edge" in error for error in verdict.errors), verdict.errors
    assert verdict.graph_edges_digest != projection_rebuild.id_digest(_S305_EXPECTED_EDGE_KEYS)


@pytest.mark.asyncio
async def test_s305_orphan_graph_edge_must_fail(tmp_path: Path) -> None:
    """Acceptance 1/4: an extra/orphan edge must fail.

    The extra edge names a target no node carries, which is exactly what the
    S3-03 structural hook refuses.

    At BASE the stub answers `valid=True`: BEHAVIOURAL failure.
    """
    from memory_server.providers.graph_provider import SimpleGraph

    db_path = tmp_path / "snapshot.db"
    url = _s305_snapshot(db_path)
    url, vector_path, graph_path = await _s305_staged(tmp_path, db_path)

    data = _s305_graph_json(graph_path)
    data["edges"].append(
        {"source_id": "widget", "target_id": "ghost", "relation": "mentions", "attributes": {}}
    )
    _s305_write_graph_json(graph_path, data)
    validation = SimpleGraph.validate_snapshot(graph_path)
    assert validation.valid is False, "the orphan edge must be structurally refused"

    verdict = await _s305_verify(url, vector_path, graph_path)

    assert verdict.valid is False, "an orphan edge must refuse the artifact"
    assert any("not present in nodes" in error for error in verdict.errors), verdict.errors


@pytest.mark.asyncio
async def test_s305_falsely_successful_graph_save_is_refused(tmp_path: Path) -> None:
    """Acceptance 1/4: a save that REPORTS success while the content is wrong.

    A graph with one extra, fully connected node is saved through the real
    provider (``save_snapshot`` returns normally), and S3-03's structural hook
    accepts it. Only the exact expected ``(id, type)`` node set can catch it.

    At BASE the stub answers `valid=True`: BEHAVIOURAL failure.
    """
    from memory_server.providers.graph_provider import SimpleGraph

    db_path = tmp_path / "snapshot.db"
    url = _s305_snapshot(db_path)
    url, vector_path, graph_path = await _s305_staged(tmp_path, db_path)

    reopened = SimpleGraph(snapshot_path=graph_path)
    reopened.load_snapshot(graph_path)
    reopened.add_node(id="ghost", type="entity", name="Ghost")
    reopened.add_edge("widget", "ghost", "mentions")
    reopened.save_snapshot(graph_path)
    validation = SimpleGraph.validate_snapshot(graph_path)
    assert validation.valid is True, validation.error
    assert validation.node_count == 7, "the falsely saved graph really carries the extra node"

    verdict = await _s305_verify(url, vector_path, graph_path)

    assert verdict.valid is False, "an extra node must refuse the artifact"
    assert any("node" in error for error in verdict.errors), verdict.errors
    assert verdict.graph_node_count == 7
    assert verdict.graph_nodes_digest != projection_rebuild.id_digest(_S305_EXPECTED_NODE_IDS)


@pytest.mark.asyncio
async def test_s305_duplicate_decides_edge_multiplicity_is_refused(tmp_path: Path) -> None:
    """A.3.2/A.4.1: ``decides`` is deduplicated by ``(source, target, relation)``.

    ``d1`` = "Pick Caddy" and ``d2`` = "pick caddy" both normalize to
    ``decision-pick-caddy``, so this corpus is the A.3.2 collision corpus. A
    second parallel ``decides`` edge for the same pair is what the runtime's
    ``get_edge`` guard never creates; the id-set digest CANNOT see it (A.3.3),
    so multiplicity is asserted separately and exactly.

    At BASE the stub answers `valid=True`: BEHAVIOURAL failure.
    """
    from memory_server.providers.graph_provider import SimpleGraph

    db_path = tmp_path / "snapshot.db"
    url = _s305_snapshot(db_path)
    url, vector_path, graph_path = await _s305_staged(tmp_path, db_path)

    data = _s305_graph_json(graph_path)
    decides = [edge for edge in data["edges"] if edge["relation"] == "decides"]
    assert len(decides) == 1, "the real rebuild must create exactly ONE decides edge"
    data["edges"].append(dict(decides[0]))
    _s305_write_graph_json(graph_path, data)
    validation = SimpleGraph.validate_snapshot(graph_path)
    assert validation.valid is True, validation.error
    assert validation.edge_count == 4
    duplicated = [f"{e['source_id']}|{e['target_id']}|{e['relation']}" for e in data["edges"]]
    assert projection_rebuild.id_digest(set(duplicated)) == projection_rebuild.id_digest(
        _S305_EXPECTED_EDGE_KEYS
    ), "the edge ID-SET digest cannot see a duplicated parallel edge"

    verdict = await _s305_verify(url, vector_path, graph_path)

    assert verdict.valid is False, "a duplicated parallel edge must refuse the artifact"
    assert any("duplicate" in error or "multiplicity" in error for error in verdict.errors), verdict.errors


@pytest.mark.asyncio
async def test_s305_empty_store_against_a_nonempty_corpus_must_fail(tmp_path: Path) -> None:
    """Acceptance 4: a NON-EMPTY corpus can never pass empty digests.

    Two real adversarial shapes: (a) the staged entries are absent entirely, and
    (b) the staged VECTOR store is REAL but EMPTY (a created table with zero
    rows) while the canonical corpus implies three vectors. Neither may pass,
    and the refused verdict must report the empty-input digest rather than an
    accepted identity.

    At BASE the stub answers `valid=True`: BEHAVIOURAL failure.
    """
    from memory_server.providers.lancedb_provider import LanceDBProvider

    db_path = tmp_path / "snapshot.db"
    url = _s305_snapshot(db_path)
    url, vector_path, graph_path = await _s305_staged(tmp_path, db_path)

    absent = await projection_rebuild.verify_staged_projections(
        snapshot_url=url,
        staging_vector_path=tmp_path / "absent" / "lancedb",
        staging_graph_path=tmp_path / "absent" / "graph.json",
        expected_vector_size=S305_VECTOR_SIZE,
    )
    assert absent.valid is False, "unreadable staged artifacts must refuse"
    assert absent.errors

    empty_path = tmp_path / "empty-staging" / "lancedb"
    empty_provider = LanceDBProvider(db_path=str(empty_path), vector_size=S305_VECTOR_SIZE)
    await empty_provider.upsert_batch([
        {
            "id": "placeholder-row",
            "vector": [0.0] * S305_VECTOR_SIZE,
            "payload": {
                "subject": "Widget", "predicate": "uses", "object": "Caddy",
                "source": "s", "memory_type": "fact",
            },
        }
    ])
    await empty_provider.delete(point_id="placeholder-row")
    await empty_provider.close()
    assert await empty_provider.count_points() == 0, "the fixture store must really be EMPTY"

    empty = await projection_rebuild.verify_staged_projections(
        snapshot_url=url,
        staging_vector_path=empty_path,
        staging_graph_path=graph_path,
        expected_vector_size=S305_VECTOR_SIZE,
    )

    assert empty.valid is False, "an empty store cannot prove a non-empty corpus"
    joined = " | ".join(empty.errors)
    assert "vector" in joined, empty.errors
    assert "ID set mismatch" in joined or "row count" in joined, empty.errors
    assert projection_rebuild.id_digest(set()) == EMPTY_DIGEST
    assert empty.vector_ids_digest == EMPTY_DIGEST
    assert empty.vector_ids == ()
    assert empty.vector_ids_digest != projection_rebuild.id_digest(_s305_expected_vector_ids())


EMPTY_DIGEST = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


def _s305_open_fds_under(root: Path) -> list[str]:
    """Real descriptor scan: which of THIS process's fds point under *root*."""
    prefix = str(root)
    found: list[str] = []
    for entry in sorted(os.listdir("/proc/self/fd")):
        try:
            target = os.readlink(f"/proc/self/fd/{entry}")
        except OSError:
            continue
        if target.startswith(prefix):
            found.append(f"{entry}->{target}")
    return found


def _s305_source_inventory(db_path: Path) -> dict[str, Any]:
    """Byte identity of the source DB and its sidecar set, plus outbox rows."""
    files = {}
    for suffix in ("", "-wal", "-shm"):
        path = Path(f"{db_path}{suffix}")
        files[path.name] = (
            hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else "ABSENT"
        )
    connection = sqlite3.connect(db_path)
    try:
        rows = connection.execute(
            "SELECT id, status FROM outbox_entries ORDER BY id"
        ).fetchall()
    finally:
        connection.close()
    return {"files": files, "outbox": rows}


@pytest.mark.asyncio
async def test_s305_published_stores_are_reopened_and_reverified_before_complete(
    tmp_path: Path,
) -> None:
    """Acceptance 5 + the S2-06 evidence contract, through the ENGINE.

    The engine ASKS the one verifier: `staged_verification_evidence` builds the
    `staged_verified` evidence and `reopen_verification_evidence` builds the
    `verified` (reopen) evidence. The gated transitions stay unreachable
    WITHOUT the seam's capability report -- the gate is untouched -- and the
    reopen must match the staged identity exactly, so a tampered published store
    removes `complete` from reach.

    At BASE the stub answers `valid=True` with no identity: the node dies on the
    evidence detail (MISSING CAPABILITY) and the tampered half dies
    BEHAVIOURALLY.
    """
    from types import SimpleNamespace

    from memory_server.providers.lancedb_provider import LanceDBProvider

    db_path = tmp_path / "snapshot.db"
    url = _s305_snapshot(db_path)
    run_dir = tmp_path / "run"
    url, vector_path, graph_path = await _s305_staged(run_dir, db_path, run_dir=run_dir)
    staged_verdict = await _s305_verify(url, vector_path, graph_path)

    assert profile_migration.staged_projection_paths(run_dir) == {
        "vector": run_dir / "staging" / "lancedb",
        "graph": run_dir / "staging" / "graph.json",
    }, "the DETAIL 9.1 staged layout must come from the engine's own constants"

    staged_evidence = profile_migration.staged_verification_evidence(
        db_path, run_dir, expected_vector_size=S305_VECTOR_SIZE
    )
    assert staged_evidence.checkpoint == "staged_verified"
    assert staged_evidence.code == "staged_verification"
    assert set(staged_evidence.detail) == {"basis", "staging_digest", "artifacts"}
    assert staged_evidence.detail["basis"] == "staged"
    assert tuple(staged_evidence.detail["artifacts"]) == ("vector", "graph")
    assert staged_evidence.detail["staging_digest"] == staged_verdict.staging_digest

    capable = profile_migration.StagedVerificationCapability(True, "explicit_flag")
    assert (
        profile_migration.validate_forward_transition(
            "projections_built", "staged_verified", [staged_evidence], capability=capable
        )
        == "staged_verified"
    )
    # The gate itself is NOT weakened: without the seam's capability report the
    # same evidence is still refused by cause.
    with pytest.raises(ValueError, match="E_STAGED_VERIFICATION_CAPABILITY_MISSING"):
        profile_migration.validate_forward_transition(
            "projections_built",
            "staged_verified",
            [staged_evidence],
            capability=profile_migration.StagedVerificationCapability(False, "unimplemented"),
        )

    published = tmp_path / "published"
    published.mkdir()
    plan = SimpleNamespace(
        targets={
            "vector": SimpleNamespace(lexical_path=str(published / "lancedb")),
            "graph": SimpleNamespace(lexical_path=str(published / "graph.json")),
        }
    )
    assert profile_migration.published_projection_paths(plan) == {
        "vector": published / "lancedb",
        "graph": published / "graph.json",
    }
    os.replace(vector_path, published / "lancedb")
    os.replace(graph_path, published / "graph.json")

    reopened = profile_migration.published_reopen_verdict(
        db_path, plan, expected_vector_size=S305_VECTOR_SIZE
    )
    assert reopened.valid is True, reopened.errors
    assert reopened.basis == "published"

    reopen_evidence = profile_migration.reopen_verification_evidence(
        db_path, plan, staged_verdict, expected_vector_size=S305_VECTOR_SIZE
    )
    assert reopen_evidence.checkpoint == "verified"
    assert reopen_evidence.code == "reopen_verification"
    assert set(reopen_evidence.detail["artifacts"]) == {"vector", "graph"}
    assert reopen_evidence.detail["artifacts"]["vector"]["matches_staged"] is True
    assert reopen_evidence.detail["artifacts"]["graph"]["matches_staged"] is True
    assert (
        profile_migration.validate_forward_transition(
            "published", "verified", [reopen_evidence], capability=capable
        )
        == "verified"
    )

    provider = LanceDBProvider(db_path=str(published / "lancedb"), vector_size=S305_VECTOR_SIZE)
    await provider.delete(point_id=str(uuid5(NAMESPACE_DNS, "belief:b1")))
    await provider.close()

    tampered = profile_migration.published_reopen_verdict(
        db_path, plan, expected_vector_size=S305_VECTOR_SIZE
    )
    assert tampered.valid is False, "a tampered published store must not reopen verified"
    with pytest.raises(ValueError, match="E_PUBLISHED_REOPEN_MISMATCH"):
        profile_migration.reopen_verification_evidence(
            db_path, plan, staged_verdict, expected_vector_size=S305_VECTOR_SIZE
        )


@pytest.mark.asyncio
async def test_s305_handles_are_released_before_rename_and_the_source_is_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Acceptance 5: handles released before the rename; source/outbox untouched.

    Evidence, all real: the verifier CLOSES every provider it opened (observed on
    the provider object itself), no tracked verification handle survives, no
    descriptor of this process points into the staged artifacts, the staged
    inventory is byte-identical before/after (the verifier wrote nothing), the
    verified entries then take a real rename and a real write in the same
    process, and the source DB, its sidecars and the outbox rows are unchanged.

    At BASE the verdict carries no identity at all: the node dies on
    `outstanding_verification_handles` (MISSING CAPABILITY).
    """
    from memory_server.providers.lancedb_provider import LanceDBProvider

    db_path = tmp_path / "snapshot.db"
    url = _s305_snapshot(db_path)
    run_dir = tmp_path / "run"
    url, vector_path, graph_path = await _s305_staged(run_dir, db_path, run_dir=run_dir)
    source_before = _s305_source_inventory(db_path)
    staged_before = _s305_inventory(run_dir / "staging")
    assert staged_before, "the fixture must have staged something"

    closed: list[str] = []
    original_close = LanceDBProvider.close

    async def counting_close(self) -> None:
        closed.append(str(self._db_path))
        return await original_close(self)

    monkeypatch.setattr(LanceDBProvider, "close", counting_close, raising=False)

    verdict = await _s305_verify(url, vector_path, graph_path)
    assert verdict.valid is True, verdict.errors
    assert verdict.staging_digest, "the verdict must carry a real staging digest"

    assert closed, "the verifier must RELEASE the provider(s) it opened"
    assert projection_rebuild.outstanding_verification_handles() == (), (
        "every store handle the verifier opened must be released"
    )
    assert _s305_open_fds_under(run_dir / "staging") == [], (
        "no descriptor of this process may still point into the staged artifacts"
    )
    assert _s305_inventory(run_dir / "staging") == staged_before, (
        "verification must not mutate the artifacts it opens"
    )

    published = tmp_path / "published"
    published.mkdir()
    os.replace(vector_path, published / "lancedb")
    os.replace(graph_path, published / "graph.json")
    provider = LanceDBProvider(db_path=str(published / "lancedb"), vector_size=S305_VECTOR_SIZE)
    await provider.upsert_batch([
        {
            "id": "write-after-rename",
            "vector": [0.25] * S305_VECTOR_SIZE,
            "payload": {
                "proposition": "written after the rename",
                "confidence": 0.5,
                "tags": [],
                "source": "s",
                "memory_type": "belief",
            },
        }
    ])
    assert await provider.count_points() == 4, "the reopened store must still accept writes"
    await provider.close()

    assert _s305_source_inventory(db_path) == source_before, (
        "the source DB, its sidecars and the outbox rows must be unchanged"
    )


@pytest.mark.asyncio
async def test_s305_the_engine_calls_the_one_verifier_and_holds_no_second_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The Stop condition: ONE verifier, and the engine CALLS it.

    Two independent proofs: (a) the engine's evidence builder delegates to
    `projection_rebuild.verify_staged_projections` -- observed through a spy that
    delegates to the real function -- and (b) the engine module's own AST never
    touches the provider store hooks (`describe_collection`,
    `validate_collection`, `validate_snapshot`, `LanceDBProvider`, `SimpleGraph`),
    so a second store-level verification implementation cannot be hiding there.

    At BASE this dies on the evidence detail (MISSING CAPABILITY).
    """
    db_path = tmp_path / "snapshot.db"
    _s305_snapshot(db_path)
    run_dir = tmp_path / "run"
    await _s305_staged(run_dir, db_path, run_dir=run_dir)

    observed: list[dict] = []
    original = projection_rebuild.verify_staged_projections

    async def spy(**kwargs):
        observed.append(kwargs)
        return await original(**kwargs)

    monkeypatch.setattr(projection_rebuild, "verify_staged_projections", spy)
    evidence = profile_migration.staged_verification_evidence(
        db_path, run_dir, expected_vector_size=S305_VECTOR_SIZE
    )

    assert len(observed) == 1, "the engine must ask the seam exactly once"
    assert observed[0]["snapshot_url"] == f"sqlite+aiosqlite:///{db_path}"
    assert observed[0]["staging_vector_path"] == run_dir / "staging" / "lancedb"
    assert observed[0]["staging_graph_path"] == run_dir / "staging" / "graph.json"
    assert len(evidence.detail["staging_digest"]) == 64

    delegated = await original(**observed[0])
    assert evidence.detail["staging_digest"] == delegated.staging_digest, (
        "the engine must publish the SEAM's verdict, not its own recomputation"
    )

    tree = ast.parse(Path(profile_migration.__file__).read_text(encoding="utf-8"))
    attributes = {node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)}
    names = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
    for token in (
        "describe_collection",
        "validate_collection",
        "validate_snapshot",
        "LanceDBProvider",
        "SimpleGraph",
    ):
        assert token not in attributes | names, (
            f"the engine must not carry a second verification implementation ({token})"
        )
