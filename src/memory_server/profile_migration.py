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

Slice S2-02 replaces the planner with a full mutation-free no-follow inventory
and a raw/effective config report:

* the canonical SQL is selected from the RAW YAML layer (``use_env=False``) —
  there is no source autodetection ambiguity; the environment is reported
  SEPARATELY and never re-selects the source;
* every identity digest (source / config / parent / sidecar) is produced by a
  streaming, size-bounded, looped fd-relative read that fstat's the SAME
  descriptor before and after the read. The bounded 64 KiB readers in
  ``storage_lock`` (``read_file_nofollow`` / ``read_regular_file_nofollow``) are
  deliberately NOT used for identity: they hash at most the first 64 KiB and
  tolerate a short read (routing-matrix residual F7);
* the run-directory path chain, the source, sidecars, config file, parents,
  targets and legacy candidates are inventoried through pinned no-follow
  descriptors: intermediate symlinks, hard links and special files are refused
  and a final symlink is recorded by its exact RAW link string, never followed;
* sidecars present means NO SQLite open at all; only an all-absent sidecar set
  allows bounded ``immutable=1&mode=ro`` metadata queries, and the source bytes
  plus every sidecar entry are re-checked across that optional open;
* the disk margin (1.25), lock state, collisions and the operation plan are
  populated instead of guessed; unknown space stays a blocker.

Still planner-only: every mutating entrypoint fails closed.
"""
from __future__ import annotations

import errno
import hashlib
import json
import math
import os
import re
import shutil
import sqlite3
import stat
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Literal, Mapping, cast, get_args
from urllib.parse import quote
from uuid import uuid4

from memory_server import storage_lock
from memory_server.paths import (
    ArtifactKind,
    StorageLayout,
    StorageLayoutError,
    StorageResolutionInputs,
    ValueOrigin,
    inspect_component_chain_nofollow,
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
_SIDECAR_CODES = {
    "-wal": "E_SQLITE_WAL_ACTIVE",
    "-shm": "E_SQLITE_SHM_AMBIGUOUS",
    "-journal": "E_SQLITE_HOT_JOURNAL",
}

# S2-02 inventory bounds. ``IDENTITY_READ_CHUNK``/``MAX_IDENTITY_BYTES`` bound
# the streaming identity digest; the loop reads exactly the size observed on the
# descriptor and refuses anything it cannot prove (short read, growth, identity
# change), so a truncated digest can never be reported as a whole-file digest.
IDENTITY_READ_CHUNK = 1 << 20
MAX_IDENTITY_BYTES = 1 << 36
MAX_INVENTORY_ENTRIES = 64
MAX_SQLITE_TABLES = 64
SQLITE_COUNT_CAP = 100_000
DISK_MARGIN_RATIO = 1.25
SQL_ACTION_PRESERVE_IN_PLACE = "preserve_in_place"
RUN_DIRECTORY_NAME = ".cmms-migrations"
MANIFEST_FILE_NAME = "manifest.json"
CONFIG_FILE_NAME = "config.yaml"
CANONICAL_SQLITE_URL = "sqlite+aiosqlite:///data/memory.db"
SQLITE_OPEN_POLICY_ABSENT = "sidecars_absent_immutable_ro"
SQLITE_OPEN_POLICY_PRESENT = "sidecars_present_no_open"
SQLITE_OPEN_POLICY_NOT_REGULAR = "source_not_regular_no_open"
_UNKNOWN_OUTBOX_COUNTS: dict[str, str] = {
    "pending": "unknown",
    "processing": "unknown",
    "completed": "unknown",
    "failed": "unknown",
}
_RUNTIME_STOP_INSTRUCTIONS = (
    "stop every CMMS runtime for this profile and confirm no writer remains",
    "re-run this dry-run after shutdown; a zero-byte WAL is still an apply blocker",
)
_LEGACY_DISPOSITION = "preserve-only; not imported"
_LAYOUT_ORIGIN_ALIASES = {
    "sqlite": "db_url",
    "vector": "vector_backend",
    "graph": "graph_snapshot_path",
    "mode": "storage_mode",
    "root": "data_root",
}
_IDENTITY_KEY_FIELDS = ("kind", "device", "inode", "mode", "size", "mtime_ns", "sha256", "raw_link_target")

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
    # S2-02: the RAW config block (``memory.providers.memory_server`` as read
    # from YAML). The planner never reads env, a live config file or Settings to
    # select the canonical SQL; the caller hands the parsed raw block in and the
    # raw layer (``use_env=False``) is the only selector.
    raw_config: Mapping[str, Any] | None = None
    raw_config_path: Path | None = None


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
    # S2-02 additive fields. ``sql_action`` is the strategy contract for the
    # source database (DETAIL 4.2) and ``report`` is the stable dry-run report
    # of DETAIL 8 as a plain, JSON-serializable mapping.
    sql_action: str = SQL_ACTION_PRESERVE_IN_PLACE
    report: Mapping[str, Any] = field(default_factory=dict)


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


def _note(diagnostics: list[Diagnostic], diagnostic: Diagnostic) -> None:
    """Append a diagnostic once; repeated findings never duplicate a blocker."""
    if diagnostic not in diagnostics:
        diagnostics.append(diagnostic)


def _path_code(exc: OSError) -> str:
    if isinstance(exc, PermissionError) or exc.errno in (errno.EACCES, errno.EPERM):
        return "E_LOCK_PERMISSION"
    return "E_ARTIFACT_IDENTITY_CHANGED"


def _same_inode(first: os.stat_result, second: os.stat_result) -> bool:
    return (
        first.st_dev == second.st_dev
        and first.st_ino == second.st_ino
        and stat.S_IFMT(first.st_mode) == stat.S_IFMT(second.st_mode)
    )


def _identity_key(identity: ArtifactIdentity) -> tuple[Any, ...]:
    return tuple(getattr(identity, name) for name in _IDENTITY_KEY_FIELDS)


def _regular_identity(info: os.stat_result, lexical: str, *, sha256: str | None = None) -> ArtifactIdentity:
    return ArtifactIdentity(
        lexical, "regular_file", info.st_dev, info.st_ino, info.st_mode, info.st_size, info.st_mtime_ns, sha256, None
    )


def _streamed_digest(
    descriptor: int, opened: os.stat_result, *, artifact: str
) -> tuple[str | None, Diagnostic | None]:
    """Digest a regular file through ONE descriptor, fstat'ed before and after.

    The loop reads exactly the size observed on that descriptor in bounded
    chunks. A short read (EOF before the recorded size), growth beyond it, or
    any device/inode/size/mtime change is refused instead of being silently
    digested as the whole file — the failure mode of the bounded 64 KiB reader.
    """
    if opened.st_size > MAX_IDENTITY_BYTES:
        return None, Diagnostic(
            "E_ARTIFACT_IDENTITY_CHANGED",
            "error",
            f"{artifact} exceeds the bounded identity read bound; identity unproven",
            artifact,
        )
    digest = hashlib.sha256()
    remaining = opened.st_size
    while remaining > 0:
        chunk = os.read(descriptor, min(IDENTITY_READ_CHUNK, remaining))
        if not chunk:
            return None, Diagnostic(
                "E_ARTIFACT_IDENTITY_CHANGED",
                "error",
                f"{artifact} returned a short read; identity unproven",
                artifact,
            )
        digest.update(chunk)
        remaining -= len(chunk)
    if os.read(descriptor, 1):
        return None, Diagnostic(
            "E_ARTIFACT_IDENTITY_CHANGED",
            "error",
            f"{artifact} grew while it was being read; identity unproven",
            artifact,
        )
    after = os.fstat(descriptor)
    if (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        return None, Diagnostic(
            "E_ARTIFACT_IDENTITY_CHANGED",
            "error",
            f"{artifact} changed while it was being read; identity unproven",
            artifact,
        )
    return digest.hexdigest(), None


def _digest_regular_path(
    path: Path, *, artifact: str, notes: list[Diagnostic], digest: bool
) -> ArtifactIdentity:
    """Read a regular file relative to its validated, pinned parent descriptor."""
    lexical = str(path)
    parent, name = path.parent, path.name
    try:
        with storage_lock.open_directory_nofollow(parent) as parent_fd:
            try:
                before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            except FileNotFoundError:
                return ArtifactIdentity(lexical, "absent")
            except OSError as exc:
                _note(notes, Diagnostic(_path_code(exc), "error", f"{artifact} cannot be inspected", artifact))
                return ArtifactIdentity(lexical, "regular_file")
            if not stat.S_ISREG(before.st_mode):
                _note(
                    notes,
                    Diagnostic(
                        "E_ARTIFACT_IDENTITY_CHANGED",
                        "error",
                        f"{artifact} changed kind between inspections",
                        artifact,
                    ),
                )
                return ArtifactIdentity(lexical, "regular_file")
            if before.st_nlink != 1:
                _note(
                    notes,
                    Diagnostic(
                        "E_PATH_HARDLINK_UNSAFE",
                        "error",
                        f"{artifact} is a hard-linked regular file",
                        artifact,
                    ),
                )
                return _regular_identity(before, lexical)
            try:
                # O_NONBLOCK is a no-op for regular files and keeps a FIFO swap
                # between the stat above and this open from blocking forever.
                descriptor = os.open(
                    name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK, dir_fd=parent_fd
                )
            except OSError as exc:
                _note(
                    notes,
                    Diagnostic(_path_code(exc), "error", f"{artifact} cannot be opened no-follow", artifact),
                )
                return _regular_identity(before, lexical)
            try:
                opened = os.fstat(descriptor)
                if not _same_inode(before, opened) or not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1:
                    _note(
                        notes,
                        Diagnostic(
                            "E_ARTIFACT_IDENTITY_CHANGED",
                            "error",
                            f"{artifact} identity changed between stat and open",
                            artifact,
                        ),
                    )
                    return _regular_identity(opened, lexical)
                if not digest:
                    return _regular_identity(opened, lexical)
                sha256, refusal = _streamed_digest(descriptor, opened, artifact=artifact)
                if refusal is not None:
                    _note(notes, refusal)
                    return _regular_identity(opened, lexical)
                return _regular_identity(opened, lexical, sha256=sha256)
            finally:
                os.close(descriptor)
    except storage_lock.StorageLockError as exc:
        if exc.code == "E_PATH_ABSENT":
            return ArtifactIdentity(lexical, "absent")
        _note(
            notes,
            Diagnostic(exc.code, "error", f"{artifact} parent chain is not a no-follow real directory", artifact),
        )
        return ArtifactIdentity(lexical, "regular_file")


def _inventory(
    path: Path,
    *,
    artifact: str,
    diagnostics: list[Diagnostic] | None = None,
    digest: bool = True,
) -> ArtifactIdentity:
    """Mutation-free no-follow identity of exactly one filesystem entry (S2-02).

    The component chain is inspected through pinned no-follow descriptors: an
    intermediate symlink or a non-directory intermediate component is fatal
    there and is reported as a blocker instead of being traversed. A final
    symlink is inventoried by its exact RAW link string and never resolved.
    Regular files are digested through ``_digest_regular_path``; hard links and
    special files are refused.
    """
    notes = diagnostics if diagnostics is not None else []
    lexical = str(path)
    try:
        component = inspect_component_chain_nofollow(Path(lexical), anchor=Path("/"))[-1]
    except StorageLayoutError as exc:
        _note(
            notes,
            Diagnostic(exc.code, "error", f"{artifact} path chain is not a no-follow real path", artifact),
        )
        return ArtifactIdentity(lexical, "special")
    if component.kind != "regular_file":
        return ArtifactIdentity(
            lexical,
            component.kind,
            component.device,
            component.inode,
            component.mode,
            None,
            None,
            None,
            component.raw_link_target,
        )
    if component.nlink != 1:
        _note(
            notes,
            Diagnostic("E_PATH_HARDLINK_UNSAFE", "error", f"{artifact} is a hard-linked regular file", artifact),
        )
        return ArtifactIdentity(lexical, "regular_file", component.device, component.inode, component.mode)
    return _digest_regular_path(Path(lexical), artifact=artifact, notes=notes, digest=digest)


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


def _config_identity(
    request: MigrationRequest,
    layout: StorageLayout,
    *,
    source_origin: str = "default",
    config_view: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """The NONSECRET storage identity hashed into ``config_digest``.

    Only canonical storage settings and store locations enter the digest. The
    stop attestation, the confirmation string, the run id, embedder secrets and
    memory content never do: two runs of the same storage layout share one
    digest whatever the operator's secret inputs were.
    """
    view = config_view or {}
    return {
        "strategy": request.strategy,
        "profile_home": str(request.profile_home),
        "layout": serialize_layout_redacted(layout),
        "source_sql_origin": source_origin,
        "raw_config": view.get("raw", {}),
        "effective_config": view.get("effective", {}),
        "config_divergence": list(view.get("divergence", ())),
    }


def _layout_origins(config_report: Any) -> dict[str, ValueOrigin]:
    """Origin labels for the frozen layout, taken from the RAW report."""
    origins = {key: ValueOrigin(kind) for key, kind in config_report.raw_origins.items()}
    for alias, key in _LAYOUT_ORIGIN_ALIASES.items():
        origins.setdefault(alias, origins.get(key, ValueOrigin("default")))
    return origins


def _available_bytes(root: Path) -> tuple[int | None, str]:
    """Free bytes of the filesystem that will own *root*; never a guess.

    The nearest existing ancestor is used because a not-yet-created directory
    would live on its parent's filesystem. If the kernel refuses to answer, the
    caller must report ``unknown`` and block instead of inventing a number.
    """
    candidate = Path(root)
    while True:
        try:
            return int(shutil.disk_usage(str(candidate)).free), ""
        except OSError as exc:
            if candidate == candidate.parent:
                return None, f"free space could not be proven ({exc.strerror})"
            candidate = candidate.parent


def _bounded_count(connection: sqlite3.Connection, table: str) -> int | str:
    """Row count of one table, bounded by ``SQLITE_COUNT_CAP``.

    The identifier comes from the database's own schema and is still quoted; a
    count that reaches the cap is reported as ``"<cap>+"`` instead of a number
    that pretends to be exact.
    """
    quoted = '"' + table.replace('"', '""') + '"'
    row = connection.execute(
        f"SELECT COUNT(*) FROM (SELECT 1 FROM {quoted} LIMIT ?)", (SQLITE_COUNT_CAP + 1,)
    ).fetchone()
    total = int(row[0]) if row else 0
    return f"{SQLITE_COUNT_CAP}+" if total > SQLITE_COUNT_CAP else total


def _immutable_readonly_probe(source: Path) -> dict[str, Any]:
    """Bounded metadata queries over an encoded ``immutable=1&mode=ro`` URI.

    Never used while a WAL/SHM/journal exists: ``immutable=1`` is only sound
    once sidecar absence has established a clean checkpointed image. No
    SQLiteProvider, SQLAlchemy, aiosqlite, ``PRAGMA wal_checkpoint``, recovery
    or normal open is involved — one stdlib read-only connection to the source.
    """
    uri = "file:" + quote(str(source), safe="/") + "?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True, timeout=0)
    try:
        connection.execute("PRAGMA query_only = ON")
        tables = tuple(
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
                " AND name NOT LIKE 'sqlite_%' ORDER BY name LIMIT ?",
                (MAX_SQLITE_TABLES,),
            )
        )
        counts = {name: _bounded_count(connection, name) for name in tables}
        integrity_row = connection.execute("PRAGMA integrity_check(1)").fetchone()
    finally:
        connection.close()
    integrity = "ok" if integrity_row and integrity_row[0] == "ok" else "failed"
    return {"uri": uri, "schema": "known", "integrity": integrity, "tables": tables, "counts": counts}


def _sidecar_entries_key(source: Path) -> tuple[Any, ...]:
    """Identity of the database and all four parent directory entries.

    DETAIL 7.2 requires the database bytes and every sidecar entry to be
    snapshotted before/after the optional immutable open; the database digest is
    carried by the surrounding inventory and this key covers the entries.
    """
    entries = [source, *(source.with_name(source.name + suffix) for suffix in _SIDECAR_SUFFIXES)]
    return tuple(
        _identity_key(_inventory(entry, artifact="sidecar", digest=False)) for entry in entries
    )


def _qualify_sqlite(
    source: Path, source_identity: ArtifactIdentity, sidecars: Mapping[str, ArtifactIdentity]
) -> tuple[dict[str, Any], list[Diagnostic]]:
    """Sidecar-gated SQLite qualification; anything unproven stays ``unknown``."""
    diagnostics: list[Diagnostic] = []
    report: dict[str, Any] = {
        "opened": False,
        "uri": None,
        "schema": "unknown",
        "integrity": "unknown",
        "tables": (),
        "counts": {},
        "outbox_counts": dict(_UNKNOWN_OUTBOX_COUNTS),
    }
    if any(item.kind != "absent" for item in sidecars.values()):
        report["open_policy"] = SQLITE_OPEN_POLICY_PRESENT
        return report, diagnostics
    if source_identity.kind != "regular_file" or source_identity.size is None:
        report["open_policy"] = SQLITE_OPEN_POLICY_NOT_REGULAR
        return report, diagnostics
    entries_before = _sidecar_entries_key(source)
    try:
        knowledge = _immutable_readonly_probe(source)
    except sqlite3.Error as exc:
        report["open_policy"] = SQLITE_OPEN_POLICY_ABSENT
        diagnostics.append(
            Diagnostic(
                "E_SQLITE_INTEGRITY",
                "error",
                f"immutable read-only probe could not read the source ({type(exc).__name__})",
                "sqlite",
            )
        )
        return report, diagnostics
    report["open_policy"] = SQLITE_OPEN_POLICY_ABSENT
    report["opened"] = True
    report.update(knowledge)
    if entries_before != _sidecar_entries_key(source) or _identity_key(source_identity) != _identity_key(
        _inventory(source, artifact="source")
    ):
        # A difference disables the optimization outright: report unknown.
        report["schema"] = "unknown"
        report["integrity"] = "unknown"
        report["tables"] = ()
        report["counts"] = {}
        diagnostics.append(
            Diagnostic(
                "E_SQLITE_PROBE_UNSAFE",
                "error",
                "source bytes or sidecar entries changed across the immutable probe;"
                " the read-only optimization is disabled",
                "sqlite",
            )
        )
        return report, diagnostics
    if report["integrity"] != "ok":
        diagnostics.append(
            Diagnostic("E_SQLITE_INTEGRITY", "error", "PRAGMA integrity_check did not return ok", "sqlite")
        )
    return report, diagnostics


def plan_profile_migration(request: MigrationRequest) -> MigrationPlan:
    """Build a strictly read-only migration plan and dry-run report (S2-02).

    Nothing is created, opened for writing, imported from an embedder or sent
    over the network. The canonical SQL is selected from the RAW YAML layer
    (``use_env=False``) and the environment is reported separately, so no
    autodetection can silently re-select the source. Every identity digest is a
    streaming size-bounded fd-relative read; the run-directory chain, source,
    sidecars, config file, parents, targets and legacy candidates are
    inventoried no-follow, and disk margin, lock state, collisions and the
    operation plan are populated instead of guessed.
    """
    if request.mode not in {"dry-run", "apply", "resume", "rollback"}:
        raise ValueError("E_INVALID_MODE")

    from memory_server.plugins.hermes.config import build_storage_config_report

    config_report = build_storage_config_report(dict(request.raw_config or {}), include_env=True)
    config_view = config_report.as_dict()
    raw = config_report.raw
    diagnostics: list[Diagnostic] = []
    warnings: list[Diagnostic] = []

    configured_root = (request.configured_data_root or "").strip()
    data_root: str | Path | None = configured_root or None
    if data_root is None and request.target_root is not None:
        data_root = request.target_root
    if data_root is None:
        data_root = raw.get("data_root") or "."

    if request.source_sql is not None:
        source_url = f"{_SQLITE_PREFIX}{Path(request.source_sql)}"
        source_origin = "request"
    else:
        source_url = config_report.canonical_sqlite_url
        source_origin = (
            "raw_config"
            if raw.get("db_url") not in (None, "", CANONICAL_SQLITE_URL)
            else "default"
        )

    layout = resolve_storage_layout(
        StorageResolutionInputs(
            mode="profile",
            profile_home=request.profile_home,
            data_root=data_root,
            sqlite_url=source_url,
            vector_backend=str(raw.get("vector_backend") or "lancedb"),
            lancedb_path=str(raw.get("lancedb_path") or "data/lancedb"),
            graph_snapshot_path=str(raw.get("graph_snapshot_path") or "data/graph.json"),
            qdrant_location=str(raw.get("qdrant_location") or ":memory:"),
            vector_collection=str(raw.get("vector_collection") or "memories"),
            origins=_layout_origins(config_report),
        )
    )
    root = layout.data_root

    if request.source_sql is not None:
        source = Path(request.source_sql)
    elif layout.sqlite.local_path is not None:
        source = layout.sqlite.local_path
    else:
        source = Path(request.profile_home) / "data" / "memory.db"
        source_origin = "unresolved"
        diagnostics.append(
            Diagnostic("E_SOURCE_SQL_REQUIRED", "error", "the raw db_url selects no local SQL file", "sqlite")
        )

    source_identity = _inventory(source, artifact="source", diagnostics=diagnostics)
    if source_identity.kind == "absent":
        diagnostics.append(Diagnostic("E_SOURCE_SQL_REQUIRED", "error", "source SQL is absent", "sqlite"))
    elif source_identity.kind != "regular_file":
        diagnostics.append(
            Diagnostic("E_SOURCE_SQL_NOT_REGULAR", "error", "source SQL is not a regular file", "sqlite")
        )

    sidecar_paths = {suffix: source.with_name(source.name + suffix) for suffix in _SIDECAR_SUFFIXES}
    sidecars = {
        suffix: _inventory(path, artifact=f"sidecar{suffix}", diagnostics=diagnostics)
        for suffix, path in sidecar_paths.items()
    }
    sidecar_matrix = {
        "wal_present": sidecars["-wal"].kind != "absent",
        "wal_size": sidecars["-wal"].size,
        "shm_present": sidecars["-shm"].kind != "absent",
        "journal_present": sidecars["-journal"].kind != "absent",
    }
    for suffix, code in _SIDECAR_CODES.items():
        identity = sidecars[suffix]
        if identity.kind == "absent":
            continue
        if identity.kind == "symlink":
            diagnostics.append(
                Diagnostic("E_PATH_FINAL_SYMLINK_UNSAFE", "error", f"sidecar {suffix} is a symlink", "sqlite")
            )
        elif identity.kind != "regular_file":
            diagnostics.append(
                Diagnostic("E_PATH_SPECIAL_FILE", "error", f"sidecar {suffix} is not a regular file", "sqlite")
            )
        diagnostics.append(
            Diagnostic(code, "error", f"SQLite sidecar {suffix} is present; dry-run does not open SQLite", "sqlite")
        )
    if sidecar_matrix["wal_present"] and not sidecar_matrix["shm_present"]:
        diagnostics.append(
            Diagnostic(
                "E_SQLITE_SHM_AMBIGUOUS",
                "error",
                "WAL is present without an SHM sidecar; writer and cleanup state are ambiguous",
                "sqlite",
            )
        )

    config_path = (
        Path(request.raw_config_path)
        if request.raw_config_path is not None
        else Path(request.profile_home) / CONFIG_FILE_NAME
    )
    config_identity = _inventory(config_path, artifact="config", diagnostics=diagnostics)

    target_paths: dict[str, Path] = {
        "sqlite": source,
        "graph": layout.graph_snapshot_path,
        "graph_lock": layout.graph_lock_path,
        "root_lock": layout.root_lock_path,
    }
    if layout.vector.local_path is not None:
        target_paths["vector"] = layout.vector.local_path
    targets = {
        label: _inventory(path, artifact=f"target:{label}", diagnostics=diagnostics)
        for label, path in target_paths.items()
    }

    legacy: list[ArtifactIdentity] = []
    legacy_entries: list[dict[str, Any]] = []
    legacy_notes: list[Diagnostic] = []
    candidates: list[tuple[str, Path | None]] = []
    store_paths = {
        "sqlite": layout.sqlite.local_path,
        "vector": layout.vector.local_path,
        "graph": layout.graph_snapshot_path,
    }
    for entry in layout.compatibility:
        if entry.startswith("legacy-split-layout:"):
            label = entry.split(":", 1)[1]
            candidates.append((f"legacy-split-layout:{label}", store_paths.get(label)))
    profile_local = (
        ("profile-lancedb", root / "data" / "lancedb"),
        ("profile-graph", root / "data" / "graph.json"),
    )
    for label, path in profile_local:
        if path not in (layout.vector.local_path, layout.graph_snapshot_path):
            candidates.append((label, path))
    seen: set[str] = set()
    for label, candidate in candidates:
        if candidate is None or len(legacy) >= MAX_INVENTORY_ENTRIES:
            continue
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        identity = _inventory(Path(key), artifact=f"legacy:{label}", diagnostics=legacy_notes)
        if identity.kind == "absent":
            continue
        legacy.append(identity)
        legacy_entries.append(
            {
                "artifact": f"legacy:{label}",
                "path": key,
                "kind": identity.kind,
                "raw_link_target": identity.raw_link_target,
                "sha256": identity.sha256,
                "disposition": _LEGACY_DISPOSITION,
            }
        )
        if identity.kind == "symlink":
            warnings.append(
                Diagnostic(
                    "W_LEGACY_SPLIT_LAYOUT",
                    "warning",
                    f"legacy projection {label} is a symlink; preserve-only, never traversed",
                    f"legacy:{label}",
                )
            )
    for note in legacy_notes:
        warnings.append(Diagnostic(note.code, "warning", note.message, note.artifact))

    if config_report.divergence:
        warnings.append(
            Diagnostic(
                "INFO_CONFIG_ENV_DIVERGENCE",
                "info",
                f"environment would override the raw storage config for {list(config_report.divergence)};"
                " the canonical SQL is selected from the raw YAML layer",
                "config",
            )
        )
    warnings.append(
        Diagnostic(
            "INFO_CONFIG_SETTINGS_LAYER",
            "info",
            "the dry-run report never consults Settings/.env; an effective origin of 'default'"
            " can still be overridden by that layer at runtime",
            "config",
        )
    )

    required_bytes: int | None = None
    if source_identity.kind == "regular_file" and source_identity.size is not None:
        required_bytes = source_identity.size + sum(
            identity.size
            for label, identity in targets.items()
            if label != "sqlite" and identity.kind == "regular_file" and identity.size is not None
        )
    else:
        diagnostics.append(
            Diagnostic(
                "E_INSUFFICIENT_SPACE",
                "error",
                "required space is unproven: the source size is unknown",
                "disk",
            )
        )
    available_bytes, disk_error = _available_bytes(root)
    if available_bytes is None:
        diagnostics.append(Diagnostic("E_INSUFFICIENT_SPACE", "error", disk_error, "disk"))
    within_margin: bool | None = None
    if required_bytes is not None and available_bytes is not None:
        within_margin = available_bytes >= required_bytes * DISK_MARGIN_RATIO
        if not within_margin:
            diagnostics.append(
                Diagnostic(
                    "E_INSUFFICIENT_SPACE",
                    "error",
                    f"free space {available_bytes} is below required {required_bytes} with margin"
                    f" {DISK_MARGIN_RATIO}",
                    "disk",
                )
            )

    inspection = storage_lock.inspect_existing_lock_readonly(root)
    lock_availability = inspection.availability
    lock_view: dict[str, Any] = {
        "availability": inspection.availability,
        "error": inspection.error,
        "owner": asdict(inspection.owner) if inspection.owner is not None else None,
    }
    if inspection.availability == "held":
        diagnostics.append(
            Diagnostic("E_WRITER_ACTIVE", "error", "an existing storage lock is held by a live writer", "lock")
        )
    elif inspection.availability == "unknown":
        warnings.append(
            Diagnostic(
                "E_WRITER_STATE_UNKNOWN",
                "warning",
                "runtime writer state is unknown: no conclusive mutation-free lock evidence",
                "lock",
            )
        )
    if inspection.error:
        warnings.append(Diagnostic("E_LOCK_ENTRY_UNSAFE", "warning", inspection.error, "lock"))

    sqlite_report, probe_diagnostics = _qualify_sqlite(source, source_identity, sidecars)
    for diagnostic in probe_diagnostics:
        _note(diagnostics, diagnostic)

    run_dir = root / RUN_DIRECTORY_NAME / request.run_id
    manifest_path = run_dir / MANIFEST_FILE_NAME
    collisions: list[dict[str, Any]] = []
    for label, path in (("run_directory", run_dir), ("manifest", manifest_path)):
        identity = _inventory(path, artifact=f"collision:{label}", diagnostics=diagnostics)
        if identity.kind != "absent":
            collisions.append({"artifact": label, "path": str(path), "kind": identity.kind})
    if collisions:
        diagnostics.append(
            Diagnostic("E_BACKUP_COLLISION", "error", "a run-owned path for this run id already exists", "run")
        )

    parents: dict[str, ArtifactIdentity] = {}
    parent_candidates = [source, *sidecar_paths.values(), config_path, *target_paths.values(), manifest_path]
    for candidate in parent_candidates:
        parent = candidate.parent
        key = str(parent)
        if key in parents or len(parents) >= MAX_INVENTORY_ENTRIES:
            continue
        parents[key] = _inventory(
            parent, artifact=f"parent:{parent.name or '/'}", diagnostics=diagnostics, digest=False
        )

    invariance_targets: list[tuple[str, Path, tuple[Any, ...]]] = [
        ("source", source, _identity_key(source_identity)),
        ("target:sqlite", source, _identity_key(targets["sqlite"])),
        *[(f"sidecar{suffix}", path, _identity_key(sidecars[suffix])) for suffix, path in sidecar_paths.items()],
        *[(f"parent:{key}", Path(key), _identity_key(identity)) for key, identity in parents.items()],
    ]
    changed = sorted(
        {
            label
            for label, path, expected in invariance_targets
            if _identity_key(_inventory(path, artifact=label)) != expected
        }
    )
    if changed:
        diagnostics.append(
            Diagnostic(
                "E_ARTIFACT_IDENTITY_CHANGED",
                "error",
                f"artifacts changed across the plan: {changed}; byte/listing invariance failed",
                "invariance",
            )
        )

    operations: list[PlannedOperation] = [PlannedOperation("validate", "sqlite", str(source))]
    if source_identity.kind == "regular_file":
        operations.append(PlannedOperation("snapshot", "sqlite", str(source)))
    for label, identity in sorted(targets.items()):
        if label == "sqlite" or identity.kind != "regular_file":
            continue
        operations.append(PlannedOperation("backup", label, identity.lexical_path))
    for label in ("vector", "graph"):
        path = target_paths.get(label)
        if path is None:
            continue
        operations.append(PlannedOperation("rebuild", label, str(path)))
        operations.append(PlannedOperation("publish", label, str(path)))
    for identity in legacy:
        operations.append(PlannedOperation("preserve", "legacy", identity.lexical_path))

    path_checks = [
        {
            "artifact": label,
            "path": identity.lexical_path,
            "kind": identity.kind,
            "sha256": identity.sha256,
            "ok": identity.kind in ("absent", "regular_file", "directory"),
        }
        for label, identity in [
            ("source", source_identity),
            *[(f"sidecar{suffix}", identity) for suffix, identity in sidecars.items()],
            ("config", config_identity),
            *[(f"target:{label}", identity) for label, identity in targets.items()],
            *[(f"parent:{key}", identity) for key, identity in parents.items()],
        ]
    ][:MAX_INVENTORY_ENTRIES]

    embedding = EmbeddingPlan(
        backend=str(raw.get("vector_backend") or "lancedb"),
        eligible_records="unknown",
        batches="unknown",
        digest=_digest([str(source), request.strategy, config_view.get("canonical_sqlite_url")]),
        network=False,
    )

    report: dict[str, Any] = {
        "schema_version": 1,
        "mode": request.mode,
        "strategy": request.strategy,
        "sql_action": SQL_ACTION_PRESERVE_IN_PLACE,
        "source_sql_origin": source_origin,
        "source_sql": asdict(source_identity),
        "source_sidecars": {suffix: asdict(identity) for suffix, identity in sidecars.items()},
        "sidecars": sidecar_matrix,
        "config": config_view,
        "config_file": asdict(config_identity),
        "target": {
            "root": str(root),
            "sqlite": str(source),
            "vector": str(layout.vector.local_path) if layout.vector.local_path is not None else None,
            "graph": str(layout.graph_snapshot_path),
        },
        "parents": [asdict(identity) for identity in parents.values()],
        "legacy_projections": legacy_entries,
        "path_checks": path_checks,
        "collisions": collisions,
        "disk": {
            "required_bytes": required_bytes,
            "available_bytes": available_bytes,
            "margin_ratio": DISK_MARGIN_RATIO,
            "within_margin": within_margin,
        },
        "sqlite": sqlite_report,
        "outbox_counts": dict(_UNKNOWN_OUTBOX_COUNTS),
        "invariance": {"artifacts": "failed" if changed else "ok", "changed": changed},
        "lock_availability": lock_availability,
        "lock": lock_view,
        "planned_operations": [asdict(operation) for operation in operations],
        "runtime_stop_instructions": list(_RUNTIME_STOP_INSTRUCTIONS),
        "proposed_manifest_path": str(manifest_path),
        "warnings": [asdict(item) for item in warnings],
        "blockers": [asdict(item) for item in diagnostics],
    }

    return MigrationPlan(
        schema_version=1,
        request=request,
        layout=layout,
        source_sql=source_identity,
        source_sidecars=sidecars,
        legacy_projections=tuple(legacy),
        targets=targets,
        required_bytes=required_bytes,
        available_bytes=available_bytes,
        lock_availability=lock_availability,
        embedding=embedding,
        planned_operations=tuple(operations),
        warnings=tuple(warnings),
        blockers=tuple(diagnostics),
        config_digest=_digest(
            _config_identity(request, layout, source_origin=source_origin, config_view=config_view)
        ),
        sql_action=SQL_ACTION_PRESERVE_IN_PLACE,
        report=report,
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
