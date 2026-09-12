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
