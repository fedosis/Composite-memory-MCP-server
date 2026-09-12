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
import time
from dataclasses import asdict, replace
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, cast
from urllib.parse import quote

import pytest

import memory_server.profile_migration as profile_migration
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
    with pytest.raises(ValueError, match="E_MIGRATION_NOT_IMPLEMENTED"):
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
    with pytest.raises(ValueError, match="E_MIGRATION_NOT_IMPLEMENTED"):
        resume_profile_migration(manifest_path, replace(request, mode="resume"))
    with pytest.raises(ValueError, match="E_MIGRATION_NOT_IMPLEMENTED"):
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
