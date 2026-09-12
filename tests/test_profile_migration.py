"""Profile-migration dry-run/manifest guard tests.

IMPL slice S0 keeps these guard-level assertions only; the DETAIL 10.1-10.6
migration contracts are implemented and tested in a later slice. Every test
runs through the ``synthetic_storage_env`` fixture, so the deployment
environment is scrubbed and migration roots stay inside the pytest temporary
root.
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from pathlib import Path
from typing import cast

import pytest

from memory_server.profile_migration import (
    MigrationManifest,
    MigrationMode,
    MigrationRequest,
    apply_profile_migration,
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
