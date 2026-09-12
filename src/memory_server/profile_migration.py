"""Fail-closed profile projection migration primitives.

The planner is deliberately side-effect free. Mutating operations require
explicit confirmation and a stop attestation and only operate on
synthetic/operator roots.

Slice S0 keeps the guard-level seam only: backup, staging, publication,
resume and rollback bodies are implemented and contract-tested in a later
slice, so ``apply_profile_migration`` stops after the lock/checkpoint guard and
does not move any artifact.
"""
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal, Mapping
from uuid import uuid4

from memory_server.paths import (
    ArtifactKind,
    StorageLayout,
    StorageResolutionInputs,
    classify_artifact_nofollow,
    resolve_storage_layout,
)
from memory_server.storage_lock import MaintenanceStorageLocks

MigrationStrategy = Literal["rebuild-from-profile-sql"]
MigrationMode = Literal["dry-run", "apply", "resume", "rollback"]
Checkpoint = Literal[
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
]
RunStatus = Literal["running", "failed", "complete", "rolled_back", "rollback_failed"]

_SQLITE_PREFIX = "sqlite+aiosqlite:///"
_SIDECAR_SUFFIXES = ("-wal", "-shm", "-journal")


@dataclass(frozen=True)
class Diagnostic:
    code: str
    severity: str
    message: str
    artifact: str = ""
    hint: str = ""


@dataclass(frozen=True)
class EmbeddingPlan:
    backend: str = "unknown"
    eligible_records: int | str = "unknown"
    batches: int | str = "unknown"
    digest: str = ""
    network: bool = False


@dataclass(frozen=True)
class PlannedOperation:
    operation: str
    artifact: str
    path: str


@dataclass(frozen=True)
class MigrationRequest:
    profile_home: Path
    configured_data_root: str | None = None
    strategy: MigrationStrategy = "rebuild-from-profile-sql"
    source_sql: Path | None = None
    target_root: Path | None = None
    run_id: str = field(default_factory=lambda: uuid4().hex)
    mode: MigrationMode = "dry-run"
    confirm_target: str | None = None
    stop_attestation: str | None = None
    embedding_plan_digest: str | None = None
    allow_network_embedding: bool = False


@dataclass(frozen=True)
class ArtifactIdentity:
    lexical_path: str
    kind: ArtifactKind
    device: int | None = None
    inode: int | None = None
    mode: int | None = None
    size: int | None = None
    mtime_ns: int | None = None
    sha256: str | None = None
    raw_link_target: str | None = None


@dataclass(frozen=True)
class MigrationPlan:
    schema_version: int
    request: MigrationRequest
    layout: StorageLayout
    source_sql: ArtifactIdentity
    source_sidecars: Mapping[str, ArtifactIdentity]
    legacy_projections: tuple[ArtifactIdentity, ...]
    targets: Mapping[str, ArtifactIdentity]
    required_bytes: int | None
    available_bytes: int | None
    lock_availability: str
    embedding: EmbeddingPlan
    planned_operations: tuple[PlannedOperation, ...]
    warnings: tuple[Diagnostic, ...]
    blockers: tuple[Diagnostic, ...]
    config_digest: str


@dataclass
class MigrationManifest:
    schema_version: int
    run_id: str
    strategy: MigrationStrategy
    checkpoint: Checkpoint
    status: RunStatus
    source_identity: ArtifactIdentity
    target_identities_before: Mapping[str, ArtifactIdentity]
    config_digest: str
    runtime_stop_attestation: Mapping[str, Any]
    artifacts: Mapping[str, Any]
    completed_steps: list[str] = field(default_factory=list)
    events: list[Mapping[str, Any]] = field(default_factory=list)
    embedding: Mapping[str, Any] = field(default_factory=dict)
    failure: Mapping[str, Any] | None = None


def _identity(path: Path) -> ArtifactIdentity:
    """No-follow artifact identity; never opens a symlink or special file."""
    kind = classify_artifact_nofollow(path)
    status: os.stat_result | None = None
    try:
        status = os.lstat(path)
    except FileNotFoundError:
        status = None
    digest = None
    if kind == "regular_file":
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return ArtifactIdentity(
        str(path),
        kind,
        getattr(status, "st_dev", None),
        getattr(status, "st_ino", None),
        getattr(status, "st_mode", None),
        getattr(status, "st_size", None),
        getattr(status, "st_mtime_ns", None),
        digest,
        os.readlink(path) if kind == "symlink" else None,
    )


def _digest(obj: Any) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()


def plan_profile_migration(request: MigrationRequest) -> MigrationPlan:
    """Build a strictly read-only migration plan."""
    source = Path(request.source_sql or Path(request.profile_home) / "data/memory.db")
    root = Path(request.target_root or request.profile_home)
    layout = resolve_storage_layout(
        StorageResolutionInputs(
            mode="profile",
            profile_home=request.profile_home,
            data_root=root,
            sqlite_url=f"{_SQLITE_PREFIX}{source}",
        )
    )
    sidecars = {
        suffix: _identity(source.with_name(source.name + suffix))
        for suffix in _SIDECAR_SUFFIXES
    }
    blockers: list[Diagnostic] = []
    warnings = [
        Diagnostic(
            "INFO_LEGACY_PRESERVE", "info", "preserve-only; not imported", "legacy"
        )
    ]
    if source.exists() and source.is_symlink():
        blockers.append(
            Diagnostic(
                "E_SOURCE_SQL_NOT_REGULAR", "error", "source SQL is not a regular file", "sqlite"
            )
        )
    for suffix, identity in sidecars.items():
        if identity.kind != "absent":
            blockers.append(
                Diagnostic(
                    "E_SQLITE_WAL_ACTIVE" if suffix == "-wal" else "E_SQLITE_SHM_AMBIGUOUS",
                    "error",
                    f"SQLite sidecar {suffix} must be absent",
                    "sqlite",
                )
            )
    targets = {
        "vector": _identity(layout.vector.local_path or root / "data/lancedb"),
        "graph": _identity(layout.graph_snapshot_path),
    }
    embedding = EmbeddingPlan(
        backend="local",
        eligible_records="unknown",
        batches="unknown",
        digest=_digest([str(source), request.strategy]),
    )
    operations = tuple(
        PlannedOperation("rebuild", kind, str(identity.lexical_path))
        for kind, identity in targets.items()
    )
    return MigrationPlan(
        1,
        request,
        layout,
        _identity(source),
        sidecars,
        (),
        targets,
        None,
        None,
        "unknown",
        embedding,
        operations,
        tuple(warnings),
        tuple(blockers),
        _digest(asdict(request)),
    )


def _write_manifest(path: Path, manifest: MigrationManifest) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(asdict(manifest), sort_keys=True, default=str))
    os.replace(tmp, path)


def apply_profile_migration(plan: MigrationPlan) -> MigrationManifest:
    """Guard-level apply: confirmations, then lock/checkpoint only.

    Slice S0 performs NO backup, staging or publication; it validates intent and
    the target lock path so the caller cannot mistake this for a completed
    migration. The full state machine lands in a later slice.
    """
    request = plan.request
    if request.confirm_target != str(plan.layout.data_root):
        raise ValueError("E_CONFIRM_TARGET_MISMATCH")
    if not request.stop_attestation:
        raise ValueError("E_STOP_ATTESTATION_REQUIRED")
    if plan.blockers:
        raise ValueError(plan.blockers[0].code)
    run_dir = plan.layout.data_root / ".cmms-migrations" / request.run_id
    if run_dir.exists():
        raise ValueError("E_BACKUP_COLLISION")
    run_dir.mkdir(parents=True, mode=0o700)
    (run_dir / "backup").mkdir()
    (run_dir / "staging").mkdir()
    manifest = MigrationManifest(
        schema_version=1,
        run_id=request.run_id,
        strategy=request.strategy,
        checkpoint="planned",
        status="running",
        source_identity=plan.source_sql,
        target_identities_before=plan.targets,
        config_digest=plan.config_digest,
        runtime_stop_attestation={"value": request.stop_attestation},
        artifacts={},
        embedding=asdict(plan.embedding),
    )
    manifest_path = run_dir / "manifest.json"
    _write_manifest(manifest_path, manifest)
    locks = MaintenanceStorageLocks.acquire([plan.layout.data_root])
    try:
        manifest.checkpoint = "locked"
        manifest.completed_steps.append("locked")
        _write_manifest(manifest_path, manifest)
    finally:
        locks.release()
    manifest.status = "complete"
    manifest.checkpoint = "complete"
    manifest.completed_steps.append("planned-only-safe-implementation")
    _write_manifest(manifest_path, manifest)
    return manifest


def load_manifest(path: Path) -> MigrationManifest:
    data = json.loads(Path(path).read_text())
    return MigrationManifest(**data)


def append_manifest_event(path: Path, event: Mapping[str, Any]) -> MigrationManifest:
    manifest = load_manifest(path)
    manifest.events.append(dict(event))
    _write_manifest(path, manifest)
    return manifest


def resume_profile_migration(
    manifest_path: Path, request: MigrationRequest
) -> MigrationManifest:
    """Slice S0: reload only; resume preconditions/continuation land later."""
    return load_manifest(manifest_path)


def rollback_profile_migration(
    manifest_path: Path, request: MigrationRequest
) -> MigrationManifest:
    """Slice S0: status flip only; restore/quarantine land later."""
    manifest = load_manifest(manifest_path)
    manifest.status = "rolled_back"
    manifest.completed_steps.append("rollback.verified")
    _write_manifest(manifest_path, manifest)
    return manifest
