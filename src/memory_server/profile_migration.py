"""Fail-closed profile projection migration primitives.

The planner is deliberately side-effect free. Mutating operations require
explicit confirmation and a stop attestation and only operate on
synthetic/operator roots.

Slice S0 exposes planning only. Mutating entrypoints refuse until the later
engine slices provide backup, staging, publication, resume and rollback.

Slice S2-01 adds the bounded manifest contract: a strictly typed, bounded
deserializer, an append-only event hash chain, a non-secret ``config_digest``
storage identity, and a durable atomic manifest write (unique no-follow 0600
temp -> flush -> fsync -> replace -> directory fsync) inside a 0700 run
directory. Every refusal is fail-closed and leaves the previously materialized
manifest byte-identical.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import stat
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Literal, Mapping, cast, get_args
from uuid import uuid4

from memory_server.paths import (
    ArtifactKind,
    StorageLayout,
    StorageResolutionInputs,
    classify_artifact_nofollow,
    resolve_storage_layout,
    serialize_layout_redacted,
)

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

MANIFEST_SCHEMA_VERSION = 1

# S2-01 bounds. Every manifest byte is bounded before it is interpreted, so a
# tampered, oversized or hostile file can never drive unbounded work or memory.
MAX_MANIFEST_BYTES = 1 << 20
MAX_MANIFEST_STRING_BYTES = 4096
MAX_MANIFEST_EVENTS = 4096
MAX_MANIFEST_ENTRIES = 512
MAX_MANIFEST_DEPTH = 8

_RUN_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")
_DIGEST_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_TIMESTAMP_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")
_EVENT_GENESIS = "0" * 64

_STRATEGIES = frozenset(get_args(MigrationStrategy))
_CHECKPOINTS = frozenset(get_args(Checkpoint))
_RUN_STATUSES = frozenset(get_args(RunStatus))
_ARTIFACT_KINDS = frozenset(get_args(ArtifactKind))

_MANIFEST_OPTIONAL_FIELDS = frozenset({"completed_steps", "events", "embedding", "failure"})
_MANIFEST_FIELDS = frozenset(
    {
        "schema_version", "run_id", "strategy", "checkpoint", "status", "source_identity",
        "target_identities_before", "config_digest", "runtime_stop_attestation", "artifacts",
    }
    | _MANIFEST_OPTIONAL_FIELDS
)
_MANIFEST_REQUIRED_FIELDS = _MANIFEST_FIELDS - _MANIFEST_OPTIONAL_FIELDS
_ARTIFACT_IDENTITY_FIELDS = frozenset(
    {"lexical_path", "kind", "device", "inode", "mode", "size", "mtime_ns", "sha256", "raw_link_target"}
)
_IDENTITY_NUMBERS = ("device", "inode", "mode", "size", "mtime_ns")
_MANIFEST_ARTIFACT_FIELDS = frozenset({"relative_path", "kind", "sha256", "size", "present"})
_MANIFEST_EVENT_FIELDS = frozenset(
    {"sequence", "timestamp", "prev_sha256", "digest", "checkpoint", "operation", "payload"}
)
_EVENT_INPUT_FIELDS = frozenset({"checkpoint", "operation", "payload"})


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


@dataclass(frozen=True)
class ManifestArtifact:
    """One run-owned artifact recorded in the manifest.

    ``relative_path`` is always relative to the run directory: an absolute,
    traversing, home-relative or root path is refused by the deserializer.
    """

    relative_path: str
    kind: str = "regular_file"
    sha256: str | None = None
    size: int | None = None
    present: bool = False


@dataclass(frozen=True)
class ManifestEvent:
    """One element of the manifest's append-only event hash chain.

    ``prev_sha256`` is the previous event's digest (``_EVENT_GENESIS`` for the
    first event) and ``digest`` is the SHA-256 of the canonical event body, so
    any reordering, removal or payload edit is detectable on load.
    """

    sequence: int
    timestamp: str
    prev_sha256: str
    digest: str
    checkpoint: Checkpoint
    operation: str
    payload: Mapping[str, Any] = field(default_factory=dict)


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
    events: list[ManifestEvent] = field(default_factory=list)
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


def _canonical_bytes(value: Any) -> bytes:
    """Canonical JSON encoding: stable key order, no whitespace, ASCII only."""
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str, allow_nan=False
    ).encode("utf-8")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _manifest_failure(code: str, detail: str = "") -> ValueError:
    return ValueError(f"{code}: {detail}" if detail else code)


# ---------------------------------------------------------------- bounds
def _bounded_str(value: Any, *, field_name: str, max_bytes: int = MAX_MANIFEST_STRING_BYTES) -> str:
    if not isinstance(value, str):
        raise _manifest_failure("E_MANIFEST_SCHEMA", f"{field_name} must be a string")
    if len(value.encode("utf-8", "surrogatepass")) > max_bytes:
        raise _manifest_failure("E_MANIFEST_SCHEMA", f"{field_name} exceeds {max_bytes} bytes")
    return value


def _bounded_int(value: Any, *, field_name: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise _manifest_failure("E_MANIFEST_SCHEMA", f"{field_name} must be an integer")
    if value < minimum:
        raise _manifest_failure("E_MANIFEST_SCHEMA", f"{field_name} is below the permitted minimum")
    return value


def _optional_int(value: Any, *, field_name: str) -> int | None:
    return None if value is None else _bounded_int(value, field_name=field_name)


def _digest_str(value: Any, *, field_name: str) -> str:
    text = _bounded_str(value, field_name=field_name, max_bytes=64)
    if not _DIGEST_PATTERN.match(text):
        raise _manifest_failure("E_MANIFEST_SCHEMA", f"{field_name} must be a sha256 hex digest")
    return text


def _validate_json_value(value: Any, *, field_name: str, depth: int = 0) -> None:
    """Refuse anything that is not a bounded, finite, JSON-representable value."""
    if depth > MAX_MANIFEST_DEPTH:
        raise _manifest_failure("E_MANIFEST_SCHEMA", f"{field_name} nests deeper than {MAX_MANIFEST_DEPTH}")
    if value is None or isinstance(value, bool) or isinstance(value, int):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise _manifest_failure("E_MANIFEST_SCHEMA", f"{field_name} is not a finite number")
        return
    if isinstance(value, str):
        _bounded_str(value, field_name=field_name)
        return
    if isinstance(value, Mapping):
        if len(value) > MAX_MANIFEST_ENTRIES:
            raise _manifest_failure("E_MANIFEST_SCHEMA", f"{field_name} has too many entries")
        for key, item in value.items():
            _bounded_str(key, field_name=f"{field_name} key", max_bytes=128)
            _validate_json_value(item, field_name=f"{field_name}.{key}", depth=depth + 1)
        return
    if isinstance(value, (list, tuple)):
        if len(value) > MAX_MANIFEST_ENTRIES:
            raise _manifest_failure("E_MANIFEST_SCHEMA", f"{field_name} has too many entries")
        for index, item in enumerate(value):
            _validate_json_value(item, field_name=f"{field_name}[{index}]", depth=depth + 1)
        return
    raise _manifest_failure("E_MANIFEST_SCHEMA", f"{field_name} is not a bounded JSON value")


def _bounded_str_list(value: Any, *, field_name: str) -> list[str]:
    if not isinstance(value, (list, tuple)) or len(value) > MAX_MANIFEST_ENTRIES:
        raise _manifest_failure("E_MANIFEST_SCHEMA", f"{field_name} must be a bounded list")
    return [_bounded_str(item, field_name=f"{field_name}[]") for item in value]


def _bounded_mapping(value: Any, *, field_name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise _manifest_failure("E_MANIFEST_SCHEMA", f"{field_name} must be a mapping")
    _validate_json_value(value, field_name=field_name)
    return dict(value)


def _require_mapping(value: Any, *, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise _manifest_failure("E_MANIFEST_SCHEMA", f"{field_name} must be a mapping")
    return value


def _reject_unknown(value: Mapping[str, Any], allowed: frozenset[str], *, field_name: str) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise _manifest_failure("E_MANIFEST_SCHEMA", f"{field_name} has unknown keys {unknown}")


def _validate_run_id(run_id: Any) -> str:
    """A run id is a bare 32-hex run directory name; it can never be a path."""
    if not isinstance(run_id, str) or len(run_id.encode("utf-8", "surrogatepass")) > 64:
        raise _manifest_failure("E_MANIFEST_SCHEMA", "run_id must be a bounded string")
    unpadded = run_id != run_id.strip() or run_id in {".", ".."} or run_id.startswith("~")
    if not run_id or unpadded or "/" in run_id or "\\" in run_id:
        raise _manifest_failure("E_MANIFEST_PATH_ESCAPE", "run_id must be a bare run directory name")
    if not _RUN_ID_PATTERN.match(run_id):
        raise _manifest_failure("E_MANIFEST_SCHEMA", "run_id must be 32 lowercase hex characters")
    return run_id


def _validate_run_relative_path(value: Any, *, field_name: str) -> str:
    """Run-owned artifact paths are run-directory relative, never roots."""
    text = _bounded_str(value, field_name=field_name)
    if not text:
        raise _manifest_failure("E_MANIFEST_SCHEMA", f"{field_name} must be non-empty")
    normalized = text.replace("\\", "/")
    if normalized.startswith("~"):
        raise _manifest_failure("E_MANIFEST_PATH_ESCAPE", f"{field_name} must not address a home root")
    if normalized == "/" or normalized in {".", "./"}:
        raise _manifest_failure("E_FORBIDDEN_TARGET_ROOT", f"{field_name} names a forbidden root")
    if PurePosixPath(text).is_absolute() or PureWindowsPath(text).is_absolute() or normalized.startswith("/"):
        raise _manifest_failure("E_MANIFEST_PATH_ESCAPE", f"{field_name} must be run-directory relative")
    parts = PurePosixPath(normalized).parts
    if not parts or ".." in parts:
        raise _manifest_failure("E_MANIFEST_PATH_ESCAPE", f"{field_name} must not traverse the run directory")
    return text


# ------------------------------------------------------- typed decoding
def _decode_artifact_identity(payload: Any, *, field_name: str) -> ArtifactIdentity:
    payload = _require_mapping(payload, field_name=field_name)
    _reject_unknown(payload, _ARTIFACT_IDENTITY_FIELDS, field_name=field_name)
    for required in ("lexical_path", "kind"):
        if required not in payload:
            raise _manifest_failure("E_MANIFEST_SCHEMA", f"{field_name}.{required} is required")
    kind = _bounded_str(payload["kind"], field_name=f"{field_name}.kind", max_bytes=32)
    if kind not in _ARTIFACT_KINDS:
        raise _manifest_failure("E_MANIFEST_SCHEMA", f"{field_name}.kind is not an artifact kind")
    digest = payload.get("sha256")
    link_target = payload.get("raw_link_target")
    numbers = [_optional_int(payload.get(name), field_name=f"{field_name}.{name}") for name in _IDENTITY_NUMBERS]
    return ArtifactIdentity(
        _bounded_str(payload["lexical_path"], field_name=f"{field_name}.lexical_path"),
        cast(ArtifactKind, kind),
        numbers[0],
        numbers[1],
        numbers[2],
        numbers[3],
        numbers[4],
        None if digest is None else _digest_str(digest, field_name=f"{field_name}.sha256"),
        None if link_target is None else _bounded_str(link_target, field_name=f"{field_name}.raw_link_target"),
    )


def _decode_manifest_artifact(payload: Any, *, field_name: str) -> ManifestArtifact:
    payload = _require_mapping(payload, field_name=field_name)
    _reject_unknown(payload, _MANIFEST_ARTIFACT_FIELDS, field_name=field_name)
    if "relative_path" not in payload:
        raise _manifest_failure("E_MANIFEST_SCHEMA", f"{field_name}.relative_path is required")
    kind = _bounded_str(payload.get("kind", "regular_file"), field_name=f"{field_name}.kind", max_bytes=32)
    if kind not in _ARTIFACT_KINDS:
        raise _manifest_failure("E_MANIFEST_SCHEMA", f"{field_name}.kind is not an artifact kind")
    present = payload.get("present", False)
    if not isinstance(present, bool):
        raise _manifest_failure("E_MANIFEST_SCHEMA", f"{field_name}.present must be a boolean")
    digest = payload.get("sha256")
    return ManifestArtifact(
        _validate_run_relative_path(payload["relative_path"], field_name=f"{field_name}.relative_path"),
        kind,
        None if digest is None else _digest_str(digest, field_name=f"{field_name}.sha256"),
        _optional_int(payload.get("size"), field_name=f"{field_name}.size"),
        present,
    )


def _iter_bounded_entries(payload: Any, *, field_name: str) -> list[tuple[str, Any]]:
    body = _require_mapping(payload, field_name=field_name)
    if len(body) > MAX_MANIFEST_ENTRIES:
        raise _manifest_failure("E_MANIFEST_SCHEMA", f"{field_name} has too many entries")
    entries: list[tuple[str, Any]] = []
    for key, item in body.items():
        name = _bounded_str(key, field_name=f"{field_name} key", max_bytes=128)
        if not name:
            raise _manifest_failure("E_MANIFEST_SCHEMA", f"{field_name} key must be non-empty")
        entries.append((name, item))
    return entries


def _decode_manifest_artifact_map(payload: Any, *, field_name: str) -> dict[str, ManifestArtifact]:
    return {
        name: _decode_manifest_artifact(item, field_name=f"{field_name}.{name}")
        for name, item in _iter_bounded_entries(payload, field_name=field_name)
    }


def _decode_artifact_identity_map(payload: Any, *, field_name: str) -> dict[str, ArtifactIdentity]:
    return {
        name: _decode_artifact_identity(item, field_name=f"{field_name}.{name}")
        for name, item in _iter_bounded_entries(payload, field_name=field_name)
    }


def _event_body(
    sequence: int,
    timestamp: str,
    prev_sha256: str,
    checkpoint: str,
    operation: str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "sequence": sequence,
        "timestamp": timestamp,
        "prev_sha256": prev_sha256,
        "checkpoint": checkpoint,
        "operation": operation,
        "payload": dict(payload),
    }


def _decode_manifest_event(payload: Any, *, index: int, previous_sha256: str) -> ManifestEvent:
    field_name = f"events[{index}]"
    payload = _require_mapping(payload, field_name=field_name)
    _reject_unknown(payload, _MANIFEST_EVENT_FIELDS, field_name=field_name)
    for required in sorted(_MANIFEST_EVENT_FIELDS):
        if required not in payload:
            raise _manifest_failure("E_MANIFEST_SCHEMA", f"{field_name}.{required} is required")
    sequence = _bounded_int(payload["sequence"], field_name=f"{field_name}.sequence", minimum=1)
    if sequence != index + 1:
        raise _manifest_failure("E_MANIFEST_TAMPERED", f"{field_name}.sequence is not the next logical step")
    timestamp = _bounded_str(payload["timestamp"], field_name=f"{field_name}.timestamp", max_bytes=64)
    if not _TIMESTAMP_PATTERN.match(timestamp):
        raise _manifest_failure("E_MANIFEST_SCHEMA", f"{field_name}.timestamp must be a UTC instant")
    prev_sha256 = _digest_str(payload["prev_sha256"], field_name=f"{field_name}.prev_sha256")
    if prev_sha256 != previous_sha256:
        raise _manifest_failure("E_MANIFEST_TAMPERED", f"{field_name} breaks the event hash chain")
    digest = _digest_str(payload["digest"], field_name=f"{field_name}.digest")
    checkpoint = _bounded_str(payload["checkpoint"], field_name=f"{field_name}.checkpoint", max_bytes=32)
    if checkpoint not in _CHECKPOINTS:
        raise _manifest_failure("E_MANIFEST_SCHEMA", f"{field_name}.checkpoint is not a checkpoint")
    operation = _bounded_str(payload["operation"], field_name=f"{field_name}.operation", max_bytes=128)
    if not operation:
        raise _manifest_failure("E_MANIFEST_SCHEMA", f"{field_name}.operation must be non-empty")
    body_payload = _bounded_mapping(payload["payload"], field_name=f"{field_name}.payload")
    body = _event_body(sequence, timestamp, prev_sha256, checkpoint, operation, body_payload)
    if _digest(body) != digest:
        raise _manifest_failure("E_MANIFEST_TAMPERED", f"{field_name}.digest does not match its canonical body")
    return ManifestEvent(
        sequence, timestamp, prev_sha256, digest, cast(Checkpoint, checkpoint), operation, body_payload
    )


def _decode_manifest(payload: Any) -> MigrationManifest:
    payload = _require_mapping(payload, field_name="manifest")
    _reject_unknown(payload, _MANIFEST_FIELDS, field_name="manifest")
    for required in sorted(_MANIFEST_REQUIRED_FIELDS):
        if required not in payload:
            raise _manifest_failure("E_MANIFEST_SCHEMA", f"manifest.{required} is required")
    schema_version = _bounded_int(payload["schema_version"], field_name="manifest.schema_version")
    if schema_version != MANIFEST_SCHEMA_VERSION:
        raise _manifest_failure("E_MANIFEST_VERSION", f"unsupported manifest schema {schema_version}")
    run_id = _validate_run_id(payload["run_id"])
    enums: dict[str, str] = {}
    for name, allowed in (
        ("strategy", _STRATEGIES),
        ("checkpoint", _CHECKPOINTS),
        ("status", _RUN_STATUSES),
    ):
        chosen = _bounded_str(payload[name], field_name=f"manifest.{name}", max_bytes=64)
        if chosen not in allowed:
            raise _manifest_failure("E_MANIFEST_SCHEMA", f"manifest.{name} is not a supported value")
        enums[name] = chosen

    events_raw = payload.get("events", [])
    if not isinstance(events_raw, (list, tuple)) or len(events_raw) > MAX_MANIFEST_EVENTS:
        raise _manifest_failure("E_MANIFEST_SCHEMA", "manifest.events must be a bounded list")
    events: list[ManifestEvent] = []
    previous_sha256 = _EVENT_GENESIS
    for index, item in enumerate(events_raw):
        event = _decode_manifest_event(item, index=index, previous_sha256=previous_sha256)
        events.append(event)
        previous_sha256 = event.digest

    failure = payload.get("failure")
    if failure is not None:
        failure = _bounded_mapping(failure, field_name="manifest.failure")

    return MigrationManifest(
        schema_version=schema_version,
        run_id=run_id,
        strategy=cast(MigrationStrategy, enums["strategy"]),
        checkpoint=cast(Checkpoint, enums["checkpoint"]),
        status=cast(RunStatus, enums["status"]),
        source_identity=_decode_artifact_identity(
            payload["source_identity"], field_name="manifest.source_identity"
        ),
        target_identities_before=_decode_artifact_identity_map(
            payload["target_identities_before"], field_name="manifest.target_identities_before"
        ),
        config_digest=_digest_str(payload["config_digest"], field_name="manifest.config_digest"),
        runtime_stop_attestation=_bounded_mapping(
            payload["runtime_stop_attestation"], field_name="manifest.runtime_stop_attestation"
        ),
        artifacts=_decode_manifest_artifact_map(payload["artifacts"], field_name="manifest.artifacts"),
        completed_steps=_bounded_str_list(payload.get("completed_steps", []), field_name="manifest.completed_steps"),
        events=events,
        embedding=_bounded_mapping(payload.get("embedding", {}), field_name="manifest.embedding"),
        failure=failure,
    )


def _encode_manifest(manifest: MigrationManifest) -> bytes:
    """Validate and canonicalize a manifest before it may reach the filesystem.

    The payload is decoded again through the same strict deserializer, so a
    manifest that could not be loaded is never written.
    """
    return _canonical_bytes(asdict(_decode_manifest(asdict(manifest))))


def _config_identity(request: MigrationRequest, layout: StorageLayout) -> dict[str, Any]:
    """The NONSECRET storage identity hashed into ``config_digest``.

    Only canonical storage settings and store locations enter the digest. The
    stop attestation, the confirmation string, the run id, embedder secrets and
    memory content never do: two runs of the same storage layout share one
    digest whatever the operator's secret inputs were.
    """
    return {
        "strategy": request.strategy,
        "profile_home": str(request.profile_home),
        "layout": serialize_layout_redacted(layout),
    }


def plan_profile_migration(request: MigrationRequest) -> MigrationPlan:
    """Build a strictly read-only migration plan."""
    if request.mode not in {"dry-run", "apply", "resume", "rollback"}:
        raise ValueError("E_INVALID_MODE")
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
        _digest(_config_identity(request, layout)),
    )


def _prepare_run_directory(run_dir: Path, run_id: str) -> None:
    """Create or re-assert the run directory: named after run_id, mode 0700.

    A symlinked or non-directory target is refused (E_MANIFEST_TAMPERED) rather
    than followed, and a run directory whose name is not the run id is refused
    (E_MANIFEST_PATH_ESCAPE), so a reused or ambiguous run identity cannot be
    silently written through.
    """
    if run_dir.name != run_id:
        raise _manifest_failure("E_MANIFEST_PATH_ESCAPE", "manifest run directory must be named after run_id")
    try:
        status = os.lstat(run_dir)
    except FileNotFoundError:
        status = None
    if status is None:
        run_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    elif stat.S_ISLNK(status.st_mode) or not stat.S_ISDIR(status.st_mode):
        raise _manifest_failure("E_MANIFEST_TAMPERED", "manifest run directory is not a real directory")
    os.chmod(run_dir, 0o700)


def _temp_manifest_name(target: Path) -> Path:
    """A unique same-directory temp name that cannot be a pre-existing file."""
    return target.parent / f".{target.name}.{os.getpid()}.{uuid4().hex}.tmp"


def _fsync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_manifest(path: Path, manifest: MigrationManifest) -> None:
    """Durably and atomically materialize a manifest, or leave the old one intact.

    Unique no-follow 0600 temp -> full write -> flush/fsync -> os.replace ->
    directory fsync. The temp name is fresh per call and opened O_EXCL|O_NOFOLLOW,
    so a pre-existing file or symlink at the temp path is refused instead of
    followed. A failure before the rename leaves the previously materialized
    manifest byte-identical; the temp is left in place for forensics rather than
    unlinked on an error path.
    """
    target = Path(path)
    payload = _encode_manifest(manifest)
    run_dir = target.parent
    _prepare_run_directory(run_dir, manifest.run_id)
    temp = _temp_manifest_name(target)
    try:
        descriptor = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    except OSError as exc:
        raise _manifest_failure(
            "E_MANIFEST_TAMPERED",
            f"manifest temp path {temp.name} is not an exclusive no-follow create: {exc.strerror}",
        ) from exc
    try:
        os.fchmod(descriptor, 0o600)
        written = 0
        while written < len(payload):
            written += os.write(descriptor, payload[written:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temp, target)
    _fsync_directory(run_dir)


def _require_mutation_preconditions(
    request: MigrationRequest, plan: MigrationPlan | None = None
) -> None:
    if request.confirm_target != str(plan.layout.data_root if plan else request.profile_home):
        raise ValueError("E_CONFIRM_TARGET_MISMATCH")
    if not request.stop_attestation:
        raise ValueError("E_STOP_ATTESTATION_REQUIRED")
    if request.mode == "dry-run":
        raise ValueError("E_APPLY_MODE_REQUIRED")
    if plan is not None:
        if not request.embedding_plan_digest:
            raise ValueError("E_EMBEDDING_PLAN_DIGEST_REQUIRED")
        if request.embedding_plan_digest != plan.embedding.digest:
            raise ValueError("E_EMBEDDING_PLAN_STALE")
    if plan is not None and plan.blockers:
        raise ValueError(plan.blockers[0].code)


def apply_profile_migration(plan: MigrationPlan) -> MigrationManifest:
    """Reject apply until the migration engine is implemented."""
    _require_mutation_preconditions(plan.request, plan)
    raise ValueError("E_MIGRATION_NOT_IMPLEMENTED")


def _read_bounded_manifest_bytes(target: Path) -> bytes:
    """Read at most ``MAX_MANIFEST_BYTES`` from a no-follow regular file.

    The open is ``O_NONBLOCK`` because a FIFO (or another special file) at the
    manifest path would otherwise block the ``O_RDONLY`` open until a writer
    appears, which would make the regular-file refusal below unreachable. It is
    a no-op for regular files, so the bounded read itself is unaffected.
    """
    try:
        descriptor = os.open(target, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        raise
    except OSError as exc:
        detail = f"manifest is not a readable regular file: {exc.strerror}"
        raise _manifest_failure("E_MANIFEST_TAMPERED", detail) from exc
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise _manifest_failure("E_MANIFEST_TAMPERED", "manifest is not a regular file")
        if info.st_size > MAX_MANIFEST_BYTES:
            raise _manifest_failure("E_MANIFEST_SCHEMA", f"manifest exceeds {MAX_MANIFEST_BYTES} bytes")
        chunks: list[bytes] = []
        remaining = MAX_MANIFEST_BYTES + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(65536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
    finally:
        os.close(descriptor)
    data = b"".join(chunks)
    if len(data) > MAX_MANIFEST_BYTES:
        raise _manifest_failure("E_MANIFEST_SCHEMA", f"manifest exceeds {MAX_MANIFEST_BYTES} bytes")
    return data


def load_manifest(path: Path) -> MigrationManifest:
    """Strictly rehydrate a manifest: every layer is typed, bounded and hashed.

    Unknown schema version, malformed or unknown keys, unbounded strings, an
    escaping run id or run path, an oversized document, a broken event hash
    chain and a canonical-digest mismatch are all fatal; nothing is coerced.
    """
    data = _read_bounded_manifest_bytes(Path(path))
    try:
        payload = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _manifest_failure("E_MANIFEST_SCHEMA", "manifest is not a UTF-8 JSON document") from exc
    return _decode_manifest(payload)


def append_manifest_event(path: Path, event: Mapping[str, Any]) -> MigrationManifest:
    """Append one event to the append-only hash chain, then persist atomically.

    The new event's sequence, UTC timestamp, previous-event SHA-256 and
    canonical body digest are derived here; the caller supplies only the
    checkpoint, operation and payload. Existing events are never rewritten.
    """
    manifest = load_manifest(path)
    if not isinstance(event, Mapping):
        raise _manifest_failure("E_MANIFEST_SCHEMA", "event must be a mapping")
    unknown = sorted(set(event) - _EVENT_INPUT_FIELDS)
    if unknown:
        raise _manifest_failure("E_MANIFEST_SCHEMA", f"event has unknown keys {unknown}")
    operation = _bounded_str(event.get("operation", ""), field_name="event.operation", max_bytes=128)
    if not operation:
        raise _manifest_failure("E_MANIFEST_SCHEMA", "event.operation must be non-empty")
    checkpoint = _bounded_str(
        event.get("checkpoint", manifest.checkpoint), field_name="event.checkpoint", max_bytes=32
    )
    if checkpoint not in _CHECKPOINTS:
        raise _manifest_failure("E_MANIFEST_SCHEMA", "event.checkpoint is not a checkpoint")
    payload = _bounded_mapping(event.get("payload", {}), field_name="event.payload")
    sequence = len(manifest.events) + 1
    if sequence > MAX_MANIFEST_EVENTS:
        raise _manifest_failure("E_MANIFEST_SCHEMA", f"manifest exceeds {MAX_MANIFEST_EVENTS} events")
    previous_sha256 = manifest.events[-1].digest if manifest.events else _EVENT_GENESIS
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    body = _event_body(sequence, timestamp, previous_sha256, checkpoint, operation, payload)
    appended = ManifestEvent(
        sequence, timestamp, previous_sha256, _digest(body), cast(Checkpoint, checkpoint), operation, payload
    )
    updated = replace(manifest, events=[*manifest.events, appended])
    _write_manifest(path, updated)
    return updated


def resume_profile_migration(
    manifest_path: Path, request: MigrationRequest
) -> MigrationManifest:
    """Reject resume until continuation is implemented."""
    _require_mutation_preconditions(request)
    raise ValueError("E_MIGRATION_NOT_IMPLEMENTED")


def rollback_profile_migration(
    manifest_path: Path, request: MigrationRequest
) -> MigrationManifest:
    """Reject rollback until restore/quarantine is implemented."""
    _require_mutation_preconditions(request)
    raise ValueError("E_MIGRATION_NOT_IMPLEMENTED")
