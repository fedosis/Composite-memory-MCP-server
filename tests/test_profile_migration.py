"""Profile-migration dry-run/manifest guard tests.

IMPL slice S0 keeps these guard-level assertions only; the DETAIL 10.1-10.6
migration contracts are implemented and tested in a later slice. Every test
runs through the ``synthetic_storage_env`` fixture, so the deployment
environment is scrubbed and migration roots stay inside the pytest temporary
root.
"""
from __future__ import annotations

import errno
import hashlib
import json
import os
import re
import sqlite3
import stat
import subprocess
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping, cast

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
