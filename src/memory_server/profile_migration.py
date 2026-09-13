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

Slice S2-03 qualifies the maintenance path's SQLite side of DETAIL 6.3 step 10,
7.3 and 10.1, without wiring it into ``apply`` (publication belongs to the later
engine slices):

* ``qualify_sqlite_source`` considers ONLY a sidecar-free regular source, so a
  present WAL, SHM or journal -- including a zero-byte one -- means no SQLite
  open of any kind: no recovery, no checkpoint, no trimmed WAL;
* the exact bounded ``BEGIN IMMEDIATE`` / ``ROLLBACK`` write-lock probe is run
  through a WRITABLE ``mode=rw`` URI (never the ``immutable=1`` read-only one,
  which cannot take a write lock), together with a competing-writer refusal, and
  the database bytes, every sidecar entry and the parent listing must be
  identical across it or the answer is ``E_SQLITE_PROBE_UNSAFE``;
* the safety snapshot is taken with ``sqlite3.Connection.backup`` from the
  percent-encoded ``mode=ro&immutable=1`` URI into the run directory, then
  reopened THERE for integrity, table set, Alembic revision, bounded ordered ID
  digests and outbox status counts -- never against the live source, and never by
  copying the database file, its WAL or its SHM.

Still planner-only: every mutating entrypoint fails closed.

Slice S2-04 adds the mixed-version maintenance preconditions and lifetime
ownership of every lock:

* ``apply`` / ``resume`` / ``rollback`` each call ``validate_mutation_preconditions``
  independently -- intent, a bounded and timestamped attestation digest, a FRESH
  replan (a caller's stale plan is ``E_PLAN_STALE``), and sidecar/path/disk
  checks recomputed from that replan. No entrypoint inherits another's validated
  state, and nothing is mutated before every check has passed;
* the writer state is inventoried from ``/proc``: every other process's raw
  descriptor labels (matched WITHOUT following any referent, so a legacy symlink
  is matched as the raw link string) plus the system-wide advisory lock table. An
  old pre-upgrade writer is ``E_OLD_WRITER_ACTIVE``, a live upgraded runtime
  ``E_WRITER_ACTIVE``, an incomplete inventory ``E_WRITER_STATE_UNKNOWN`` and a
  missing inventory source ``E_WRITER_INVENTORY_UNSUPPORTED``; nothing is ever
  signalled;
* identity AND hash stability of the source, its sidecars and the projections is
  proven across the quiet interval (2 s under tests, 5 s for the CLI);
* ``acquire_maintenance_locks`` locks every source/target root exclusively in
  canonical sorted order and holds the applicable graph lock without inode
  replacement. The lock object owns its descriptors for its whole lifetime: a
  release is refused inside an open critical section and the lock entry is never
  unlinked, so the resource stays protected after the lock is dropped;
* the prequalified bounded SQLite transaction probe runs last, on the fresh
  identities only.

Slice S2-05 adds the immutable run-owned backup of the regular artifacts and of
the final legacy link entries (DETAIL 9.1, 10.1):

* every regular file and every directory is copied through PINNED no-follow
  descriptors with SHA-256, mode, size and identity recorded; an interior
  symlink, a special file and a hard-linked regular file are refused, and every
  identity digest is the streaming, size-bounded, looped fd-relative read
  fstat'ed before and after on the SAME descriptor -- the bounded 64 KiB readers
  in ``storage_lock`` are never used for it (routing-matrix residual F7);
* a final legacy symlink is backed up as a NEW symlink entry carrying the exact
  RAW ``readlink`` string; its referent is never opened, enumerated, hashed,
  copied or validated as a store, and an absent entry is recorded explicitly;
* nothing in the run directory is ever overwritten (every publication is an
  atomic no-overwrite ``os.link`` of a verified staged copy inside the same
  filesystem, and a collision is ``E_BACKUP_COLLISION``), every write is
  fsync'ed, the run directory is 0700 and every file it owns is 0600, and a
  cross-device run directory is refused before a single byte is copied;
* the held graph lock inode is never replaced: creating an absent graph lock is
  recorded with the (device, inode) of the descriptor the lock stage actually
  owns, so post-unlock cleanup can remove exactly what this run created and
  never a foreign or replaced entry.

Slice S2-06 adds the forward state machine and the per-artifact publication
(DETAIL 9.2, 9.3, 10.3, 10.4; routing-matrix residual F1):

* the ten forward checkpoints advance one step at a time and only with the
  checkpoint's OWN qualified prerequisite evidence, and the same rule is
  enforced where a checkpoint becomes state -- a gated checkpoint cannot even be
  RECORDED in the manifest without it;
* `staged_verified`, `published`, `verified` and `complete` are reachable only
  when the verification seam REPORTS an implemented capability (its explicit
  flag, or a negative probe it refuses) and never because of the seam's return
  value; the S0 stub in ``projection_rebuild`` answers ``True`` to a staged entry
  that does not exist, so while it is in place those checkpoints are unreachable
  and the public entrypoints stay fail-closed;
* publication is per artifact and never an atomic claim: revalidate the pinned
  prestate, quarantine the old entry (or record its absence), rename the verified
  staged entry into the vacant target, fsync both parents, and emit
  ``prestate_revalidated``, ``prestate_quarantined|absent``,
  ``staging_published``, ``parent_fsynced``; `published` additionally requires
  the run's OWN recorded events for BOTH artifacts, so a half-published run can
  never be reported as published;
* ``verified`` needs a reopen of the published entry that still matches the
  pinned staged identity exactly, and ``complete`` needs the durable manifest
  update to have been re-read from disk; a graph lock this run CREATED is carried
  into the manifest for post-unlock cleanup (DETAIL 10.1).

Wiring the stage into ``apply``/``resume``/``rollback`` was NOT part of this
slice: the ordering was not bypassed here. S3-06 has since done that wiring --
the three entrypoints no longer raise ``E_MIGRATION_NOT_IMPLEMENTED`` anywhere,
because there is no unwired stage left to report.

Slice S2-08 adds the rollback entrypoint (DETAIL 9.3's rollback event chain,
DETAIL 10.6 steps 1-10), and it is the ONLY thing that changes about that
sentence:

* ``rollback_profile_migration`` repeats every maintenance precondition, re-confirms
  the run's own durable records (run id, source identity, the run-path collision and
  the backup report with every recorded entry) and then checks the
  CURRENT TARGET IDENTITY of each publication artifact against the identity the run's
  OWN publication recorded (the S2-07 ``staged_identity_key``), never against the
  fresh plan: where the record carries no comparable identity the rollback REFUSES
  (``E_ROLLBACK_IDENTITY_MISSING``) instead of inferring equality
  (routing-matrix R-4; owner S3-06);
* the ``config_digest`` re-confirmation of DETAIL 10.5 is deliberately NOT repeated
  by rollback: DETAIL 10.6's guard list does not include it, and the run's OWN
  publication legitimately changes the redacted layout when a legacy symlinked vector
  store is replaced by the staged directory, so that digest can never match after a
  publication (measured; see the entrypoint's docstring);
* the run-created current artifact is renamed into a UNIQUE
  ``quarantine/failed-current/`` entry and never deleted, a recorded prior absence is
  left absent, and the recorded pre-state is copied from the RETAINED backup into a
  unique verified run-owned ``restore/`` staging entry that is then renamed into the
  vacant target -- a prior symlink by its exact raw ``readlink`` string, verified by
  ``lstat`` + ``readlink`` only, its referent never touched;
* the rollback records its OWN durable chain (``rollback.locked``,
  ``rollback.current_quarantined``, ``rollback.prestate_restored``,
  ``rollback.verified``, ``rolled_back``), fsyncs the parents of every rename,
  reopens and verifies the restored entries, and on ANY failure retains every copy,
  records ``rollback_failed`` with the stable cause, and never retries destructively.
* ``apply`` and ``resume`` are byte-unchanged and stay fail-closed: rollback publishes
  nothing and needs no staged verification, so the capability gate is untouched.
"""
from __future__ import annotations

import asyncio
import contextlib
import errno
import hashlib
import json
import logging
import math
import os
import re
import shutil
import sqlite3
import stat
import sys
import threading
import time
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Callable, Iterable, Iterator, Literal, Mapping, cast, get_args
from urllib.parse import quote
from uuid import uuid4

import prometheus_client

from memory_server import projection_rebuild, storage_lock
from memory_server.evaluation.metrics import metrics_registry
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

logger = logging.getLogger(__name__)
_migration_runs = prometheus_client.Counter(
    "cmms_migration_runs_total",
    "Terminal profile migration runs",
    ["state", "strategy"],
    registry=metrics_registry,
)
_MIGRATION_TERMINAL_STATES = frozenset({"complete", "failed", "rolled_back", "rollback_failed"})


def _manifest_observability_totals(artifacts: Mapping[str, Any]) -> tuple[int, int]:
    counts = bytes_total = 0
    for value in artifacts.values():
        if isinstance(value, Mapping):
            nested_counts, nested_bytes = _manifest_observability_totals(value)
            counts += nested_counts
            bytes_total += nested_bytes
        elif isinstance(value, list):
            nested_counts, nested_bytes = _manifest_observability_totals(
                {str(i): item for i, item in enumerate(value)}
            )
            counts += nested_counts
            bytes_total += nested_bytes
    if isinstance(artifacts.get("count"), int):
        counts += artifacts["count"]
    if isinstance(artifacts.get("size"), int):
        bytes_total += artifacts["size"]
    if isinstance(artifacts.get("bytes"), int):
        bytes_total += artifacts["bytes"]
    return counts, bytes_total


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

# S2-04 maintenance bounds. The quiet interval is the DETAIL 6.3 step 11 window
# (2 s under tests, 5 s for the CLI); the attestation and the lock-root set are
# bounded so no operator input can drive unbounded work or memory.
QUIET_INTERVAL_SECONDS_TESTS = 2.0
QUIET_INTERVAL_SECONDS_CLI = 5.0
MAX_QUIET_INTERVAL_SECONDS = 60.0
MAX_ATTESTATION_BYTES = 512
MAX_MAINTENANCE_ROOTS = 16
DEFAULT_PROC_ROOT = storage_lock.PROC_ROOT_DEFAULT
MAX_SQLITE_TABLES = 64
SQLITE_COUNT_CAP = 100_000
DISK_MARGIN_RATIO = 1.25
SQL_ACTION_PRESERVE_IN_PLACE = "preserve_in_place"
RUN_DIRECTORY_NAME = ".cmms-migrations"
MANIFEST_FILE_NAME = "manifest.json"
CONFIG_FILE_NAME = "config.yaml"

# S2-05 bounds and stable strings: the immutable run-owned backup of the regular
# artifacts and of the final legacy link entries (DETAIL 9.1, 10.1).
#
# Nothing is overwritten and nothing is followed: every byte is read through a
# pinned no-follow descriptor, staged inside the run directory, verified there
# and published with ``os.link`` -- the atomic, no-overwrite publication -- so a
# copy, fsync or publication failure can leave a staged temp for forensics but
# can never leave a partial file under a published backup name, and an existing
# backup is never replaced. The entry and depth bounds make a hostile or
# accidentally huge tree a bounded refusal instead of an unbounded copy.
BACKUP_DIRECTORY_NAME = "backup"
BACKUP_LINK_ENTRIES_NAME = "link-entries"
BACKUP_STAGING_NAME = "staging"
BACKUP_STAGING_TEMP_NAME = "backup-tmp"
BACKUP_REPORT_NAME = "backup-report.json"
BACKUP_REPORT_SCHEMA_VERSION = 1
BACKUP_FILE_MODE = 0o600
BACKUP_DIRECTORY_MODE = 0o700
MAX_BACKUP_ENTRIES = 1024
MAX_BACKUP_DEPTH = 16
MAX_BACKUP_NAME_BYTES = 200
_RUN_OWNED_NAME_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,%d}$" % MAX_BACKUP_NAME_BYTES)
# The run-directory subpaths DETAIL 9.1 names literally for the two projection
# targets: ``backup/graph.json`` and ``backup/vector``. A target label outside
# this table keeps its own sanitized local name.
_BACKUP_TARGET_NAMES = {"graph": "graph.json", "vector": "vector"}
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

# ---------------------------------------------------------------------------
# S2-03 bounds and stable strings: the qualified write-lock probe and the
# run-owned safety snapshot (DETAIL 6.3 step 10, 7.3, 10.1).
#
# The probe is the ONLY place a writable SQLite handle is opened, and it is a
# separate query string from the immutable read-only one on purpose: an
# ``immutable=1`` connection cannot take a write lock, so a write-lock probe
# through that URI would prove nothing (card S2-03 acceptance 2).
# ---------------------------------------------------------------------------
SQLITE_READONLY_IMMUTABLE_QUERY = "mode=ro&immutable=1"
SQLITE_WRITABLE_PROBE_QUERY = "mode=rw"
PROBE_BEGIN_SQL = "BEGIN IMMEDIATE"
PROBE_ROLLBACK_SQL = "ROLLBACK"
SQLITE_PROBE_POLICY_ABSENT = "sidecars_absent_writable_probe"
SQLITE_PROBE_POLICY_PRESENT = "sidecars_present_no_probe"
SQLITE_PROBE_POLICY_NOT_REGULAR = "source_not_regular_no_probe"
SQLITE_HEADER_MAGIC = b"SQLite format 3\x00"
SQLITE_HEADER_WAL = 2
SQLITE_HEADER_ROLLBACK = 1
MAX_PARENT_ENTRIES = 1024
SNAPSHOT_DIR_NAME = "snapshot"
SNAPSHOT_DB_NAME = "memory.db"
SNAPSHOT_API = "sqlite3.Connection.backup"
SNAPSHOT_VERIFY_OPEN_POLICY = "run_directory_normal_open"
OUTBOX_TABLE_NAME = "outbox_entries"
OUTBOX_STATUSES = ("pending", "processing", "completed", "failed")
MAX_DISTINCT_OUTBOX_STATUSES = 16
# The minimum a snapshot must carry for THIS verification: the revision marker,
# the canonical record table whose ordered ID digest is recorded, and the outbox
# queue whose status counts are recorded. Requiring the accepted head revision
# below transitively implies the rest of the canonical schema.
REQUIRED_SNAPSHOT_TABLES = ("alembic_version", "facts", OUTBOX_TABLE_NAME)
# Migration head of THIS tree, derived from BOTH ``version_locations`` that
# alembic.ini configures: ``%(here)s/alembic/versions`` AND
# ``%(here)s/migrations/versions``. Those ten revision files form ONE merged DAG
# whose single head is the mergepoint ``0005``, whose ``down_revision`` is the
# pair ("b2f3a4c5d6e7", "7a1b2c3d4e5f"). ``7a1b2c3d4e5f`` is therefore an
# INTERIOR node of the merged DAG (it is consumed as a parent by ``0005``), NOT
# a head; reading only one version location is what made S2-03 accept it. An
# intermediate or unknown revision is a blocker, never silently accepted for a
# rebuild source.
#
# The set is explicit on purpose. A runtime derivation was rejected: alembic is
# a dev-only extra (pyproject ``[project.optional-dependencies].dev``), not a
# runtime dependency, and the migration tree lives at the repository root rather
# than inside the installed ``memory_server`` package, so neither an ``alembic``
# import nor a version-location walk is available to the deployed module. The
# drift is instead pinned by
# ``tests/test_profile_migration.py::test_s203_accepted_revisions_are_the_tree_head_from_both_version_locations``,
# which recomputes the head from both configured locations (stdlib only) and
# fails when it changes.
ACCEPTED_SQLITE_SCHEMA_REVISIONS = frozenset({"0005"})
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

_MANIFEST_OPTIONAL_FIELDS = frozenset({"completed_steps", "events", "embedding", "failure", "graph_lock"})
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


# DETAIL §12 is the single source of truth for the stable diagnostic catalogue.
DETAIL_DIAGNOSTIC_CODES = (
    "E_HERMES_HOME_REQUIRED", "E_PROFILE_ROOT_EXTERNAL", "E_SHARED_ROOT_REQUIRED",
    "E_SHARED_ROOT_RELATIVE", "E_SHARED_SPLIT_LAYOUT", "E_PATH_OUTSIDE_ROOT",
    "E_PATH_SYMLINK_PARENT", "E_PATH_FINAL_SYMLINK_UNSAFE", "W_LEGACY_SPLIT_LAYOUT",
    "E_PATH_SPECIAL_FILE", "E_PATH_OVERLAP", "E_FORBIDDEN_TARGET_ROOT",
    "E_ARTIFACT_IDENTITY_CHANGED", "E_QDRANT_PROFILE_NAMESPACE_UNDEFINED", "E_PROJECTION_UNAVAILABLE",
    "E_SOURCE_SQL_REQUIRED", "E_SOURCE_SQL_NOT_REGULAR", "E_SQLITE_WAL_ACTIVE",
    "E_SQLITE_SHM_AMBIGUOUS", "E_SQLITE_HOT_JOURNAL", "E_SQLITE_READONLY_UNSAFE",
    "E_SQLITE_PROBE_UNSAFE", "E_SQLITE_INTEGRITY", "E_SQLITE_SCHEMA", "E_SOURCE_CHANGED",
    "E_LEGACY_PROJECTION_IMPORT_FORBIDDEN", "E_INSUFFICIENT_SPACE", "E_BACKUP_COLLISION",
    "E_BACKUP_VERIFY", "E_STAGING_VERIFY", "E_EMBEDDER_UNAVAILABLE", "E_EMBEDDER_AUTH_REQUIRED",
    "E_EMBEDDER_DIMENSION", "E_EMBEDDER_PARTIAL", "E_CROSS_FILESYSTEM_PUBLICATION",
    "E_PUBLICATION_AMBIGUOUS", "E_ROLLBACK_VERIFY", "E_CONFIRMATION_REQUIRED",
    "E_CONFIRM_TARGET_MISMATCH", "E_STOP_ATTESTATION_REQUIRED", "E_WRITER_ACTIVE",
    "E_WRITER_STATE_UNKNOWN", "E_LOCK_HELD", "E_LOCK_TIMEOUT", "E_LOCK_PERMISSION",
    "E_LOCK_UNSUPPORTED", "E_LOCK_ENTRY_UNSAFE", "E_MANIFEST_VERSION", "E_MANIFEST_SCHEMA",
    "E_MANIFEST_TAMPERED", "E_MANIFEST_PATH_ESCAPE", "E_CONFIG_DIGEST_CHANGED",
)
DIAGNOSTIC_MESSAGE_LIMIT = 512

def _diagnostic_entry(code: str) -> dict[str, str]:
    return {
        "severity": "warning" if code.startswith("W_") else "error",
        "message": f"{code} reported by the storage safety checks",
        "artifact": "storage",
        "hint": "Inspect the affected artifact and resolve the condition before retrying.",
    }

DIAGNOSTIC_CONTRACT = {code: _diagnostic_entry(code) for code in DETAIL_DIAGNOSTIC_CODES}

# Process status is deliberately a separate table: changing diagnostic text or
# emission must never change the lifecycle/control-flow contract.
EXIT_CODE_CONDITIONS = {
    0: {"code": "OK", "phase": "success"},
    1: {"code": "E_PATH_OUTSIDE_ROOT", "phase": "precondition"},
    2: {"code": "CLI_USAGE", "phase": "usage"},
    3: {"code": "E_MIGRATION_STAGE_FAILED", "phase": "apply"},
    4: {"code": "E_PUBLICATION_AMBIGUOUS", "phase": "rollback"},
    5: {"code": "E_MANIFEST_TAMPERED", "phase": "manifest"},
    6: {"code": "E_LOCK_ENTRY_UNSAFE", "phase": "lock"},
}

def exit_code_for_diagnostic(code: str, *, phase: str = "precondition") -> int:
    if phase == "success":
        return 0
    if phase == "usage":
        return 2
    if phase in {"manifest", "resume-manifest"} or code.startswith("E_MANIFEST_"):
        return 5
    if phase in {"lock", "writer"} or code.startswith(("E_LOCK_", "E_WRITER_")):
        return 6
    if phase in {"rollback", "publication"} or code in {"E_PUBLICATION_AMBIGUOUS", "E_ROLLBACK_VERIFY"}:
        return 4
    if phase in {"apply", "resume"}:
        return 3
    return 1


def _bounded_diagnostic_text(value: str, code: str) -> str:
    text = str(value)
    text = re.sub(r"(?i)(token|secret|password|api[_-]?key)\s*[=:]\s*[^\s,;]+", r"\1=[redacted]", text)
    text = re.sub(r"(?<![\w.])(?:~|/)(?:[^\s,;]+/)+[^\s,;]+", "[path redacted]", text)
    if len(text) > DIAGNOSTIC_MESSAGE_LIMIT:
        text = text[:DIAGNOSTIC_MESSAGE_LIMIT - 14] + "…[truncated]"
    return text


@dataclass(frozen=True)
class Diagnostic:
    code: str
    severity: str
    message: str
    artifact: str = ""
    hint: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "message", _bounded_diagnostic_text(self.message, self.code))
        object.__setattr__(self, "artifact", _bounded_diagnostic_text(self.artifact, self.code))
        object.__setattr__(self, "hint", _bounded_diagnostic_text(self.hint, self.code))


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
    # S2-06 (DETAIL 10.1): if this migration CREATED an absent graph lock, the
    # manifest records that fact -- path plus the exact (device, inode) the lock
    # stage owns -- so post-unlock rollback cleanup removes exactly what this run
    # created and never a foreign or replaced entry. Optional and additive: an
    # older manifest without the field decodes to None and schema_version stays 1.
    graph_lock: Mapping[str, Any] | None = None


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

    graph_lock = payload.get("graph_lock")
    if graph_lock is not None:
        graph_lock = _bounded_mapping(graph_lock, field_name="manifest.graph_lock")

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
        graph_lock=graph_lock,
    )


def _encode_manifest(manifest: MigrationManifest) -> bytes:
    """Validate and canonicalize a manifest before it may reach the filesystem.

    The payload is decoded again through the same strict deserializer, so a
    manifest that could not be loaded is never written.
    """
    return _canonical_bytes(asdict(_decode_manifest(asdict(manifest))))


_CONFIG_DIGEST_LAYOUT_KEYS = (
    "mode",
    "profile_home",
    "data_root",
    "sqlite",
    "vector",
    "graph",
    "compatibility",
    "unavailable_projections",
)


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
        "layout": {
            key: serialize_layout_redacted(layout)[key]
            for key in _CONFIG_DIGEST_LAYOUT_KEYS
        },
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


def _sqlite_uri(source: Path, query: str) -> str:
    """Percent-encoded absolute ``file:`` URI for exactly one query string.

    ``quote(..., safe="/")`` encodes spaces and every other URI-reserved byte of
    the path, so a directory or file name that contains ``?``, ``#`` or a space
    cannot silently add to or truncate the query. The read-only immutable and the
    writable probe URI are built here so both are stated in one place (S2-03).
    """
    return "file:" + quote(str(source), safe="/") + "?" + query


def _immutable_readonly_probe(source: Path) -> dict[str, Any]:
    """Bounded metadata queries over an encoded ``immutable=1&mode=ro`` URI.

    Never used while a WAL/SHM/journal exists: ``immutable=1`` is only sound
    once sidecar absence has established a clean checkpointed image. No
    SQLiteProvider, SQLAlchemy, aiosqlite, ``PRAGMA wal_checkpoint``, recovery
    or normal open is involved — one stdlib read-only connection to the source.
    """
    uri = _sqlite_uri(source, SQLITE_READONLY_IMMUTABLE_QUERY)
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


# ---------------------------------------------------------------------------
# S2-03 -- qualified write-lock probe (DETAIL 6.3 step 10) and run-owned safety
# snapshot (DETAIL 7.3, 10.1)
#
# Nothing here is reachable from the dry-run planner: DETAIL 7.2 forbids a
# normal SQLite open in dry-run, so the writable handle below only exists on
# the apply/maintenance path, on a source that already passed the sidecar-free
# and regular-file gates. Every function is fail-closed: an unproven step is a
# blocker with a stable code, never a weaker contract.
# ---------------------------------------------------------------------------


def _sqlite_runtime_triple() -> dict[str, Any]:
    """The exact Python/SQLite/platform combination carrying a probe result.

    DETAIL 6.3 step 10 permits the write-lock probe only when this exact
    combination was proven side-effect free, so the combination travels with the
    probe report instead of being assumed from the source tree.
    """
    uname = os.uname() if hasattr(os, "uname") else None
    triple: dict[str, Any] = {
        "python": ".".join(str(part) for part in sys.version_info[:3]),
        "sqlite3_module": getattr(sqlite3, "version", "unknown"),
        "sqlite_version": sqlite3.sqlite_version,
        "platform": sys.platform,
        "machine": getattr(uname, "machine", "unknown"),
        "release": getattr(uname, "release", "unknown"),
    }
    triple["digest"] = _digest(triple)
    return triple


def _journal_mode_hint(source: Path) -> str:
    """Journal mode from the database header, without opening SQLite at all.

    Bytes 18/19 of the header are the file format write/read version: 2 is WAL,
    1 is the legacy rollback-journal format. Reading them is how the
    qualification log can name the mode it qualified without an open of its own.
    """
    try:
        descriptor = os.open(source, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
    except OSError:
        return "unknown"
    try:
        header = os.read(descriptor, 100)
    finally:
        os.close(descriptor)
    if len(header) < 20 or not header.startswith(SQLITE_HEADER_MAGIC):
        return "unknown"
    if header[18] == SQLITE_HEADER_WAL:
        return "wal"
    return "rollback" if header[18] == SQLITE_HEADER_ROLLBACK else "unknown"


def _parent_listing_key(source: Path) -> tuple[tuple[str, ...], str]:
    """Sorted no-follow parent-directory entry names plus a proof status.

    ``ok`` means every entry name was listed and the listing is bounded. A
    directory with more than ``MAX_PARENT_ENTRIES`` entries, or one that cannot
    be listed no-follow, is reported as unproven rather than compared partially.
    """
    try:
        with storage_lock.open_directory_nofollow(source.parent) as descriptor:
            names = tuple(sorted(os.listdir(descriptor)))
    except (OSError, storage_lock.StorageLockError):
        return (), "unreadable"
    if len(names) > MAX_PARENT_ENTRIES:
        return names[:MAX_PARENT_ENTRIES], "unbounded"
    return names, "ok"


def _probe_invariance_key(source: Path) -> tuple[tuple[Any, ...], tuple[tuple[str, ...], str]]:
    """(database+sidecar identities, parent listing) captured around the probe."""
    artifacts = (
        _identity_key(_inventory(source, artifact="source")),
        _sidecar_entries_key(source),
    )
    return artifacts, _parent_listing_key(source)


def _competing_writer_refusal(uri: str) -> dict[str, Any]:
    """Try to take the write lock from a second connection; record the answer.

    Write-lock exclusion is only proven when an independent connection is
    REFUSED while the probe holds the transaction. The competing connection
    never writes: acquiring ``BEGIN IMMEDIATE`` is already enough to show the
    exclusion does not hold.
    """
    outcome: dict[str, Any] = {
        "attempted": True,
        "refused": False,
        "sqlite_errorname": None,
        "detail": "a competing writer acquired the write lock; exclusion is not proven",
    }
    connection = sqlite3.connect(uri, uri=True, timeout=0, isolation_level=None)
    try:
        try:
            connection.execute(PROBE_BEGIN_SQL)
        except sqlite3.Error as exc:
            outcome["refused"] = True
            outcome["sqlite_errorname"] = getattr(exc, "sqlite_errorname", None)
            outcome["detail"] = str(exc)
        else:
            connection.execute(PROBE_ROLLBACK_SQL)
    finally:
        connection.close()
    return outcome


def _transaction_probe(source: Path, *, while_locked: Callable[[], Any] | None = None) -> dict[str, Any]:
    """The exact bounded ``BEGIN IMMEDIATE`` / ``ROLLBACK`` no-logical-write probe.

    DETAIL 6.3 step 10 and 7.3: the probe is NEVER attempted through an
    ``immutable=1`` read-only URI, because an immutable connection cannot take a
    write lock and could not prove anything. ``isolation_level=None`` makes the
    driver emit the two statements verbatim instead of opening an implicit
    transaction of its own; ``timeout=0`` bounds the lock wait so a live writer
    is reported instead of waited out; ``ROLLBACK`` is issued before the handle
    is closed so the qualified sequence is the recorded one.

    The competing-writer refusal is ALWAYS taken: the probe has no way to skip
    the exclusion proof (review F3 removed the ``competing_writer`` bypass, which
    let a caller obtain ``qualified=True`` with no exclusion proof at all).

    ``while_locked`` is an optional observation hook invoked while the
    transaction is held, which is how the qualification tests observe the
    exclusion from an independent connection.
    """
    uri = _sqlite_uri(source, SQLITE_WRITABLE_PROBE_QUERY)
    report: dict[str, Any] = {
        "uri": uri,
        "sql": [PROBE_BEGIN_SQL, PROBE_ROLLBACK_SQL],
        "performed": False,
        "in_transaction": False,
        "rolled_back": False,
        "competing_writer": {
            "attempted": False,
            "refused": False,
            "sqlite_errorname": None,
            "detail": "",
        },
    }
    connection = sqlite3.connect(uri, uri=True, timeout=0, isolation_level=None)
    try:
        connection.execute(PROBE_BEGIN_SQL)
        report["performed"] = True
        report["in_transaction"] = bool(connection.in_transaction)
        report["competing_writer"] = _competing_writer_refusal(uri)
        if while_locked is not None:
            while_locked()
        connection.execute(PROBE_ROLLBACK_SQL)
        report["rolled_back"] = True
    finally:
        connection.close()
    return report


def _qualify_transaction_probe(
    source: Path,
    *,
    source_identity: ArtifactIdentity,
    sidecars: Mapping[str, ArtifactIdentity],
    while_locked: Callable[[], Any] | None = None,
) -> tuple[dict[str, Any], list[Diagnostic]]:
    """Qualify the exact transaction probe on a sidecar-free regular source only.

    Order (DETAIL 6.3 step 10, 7.3): sidecar-free regular source first, then the
    probe with its competing-writer refusal, then the byte/entry/parent
    invariance proof. A source with ANY sidecar -- including a zero-byte WAL --
    is never opened at all, so no recovery, no checkpoint and no trimmed WAL can
    happen here. Everything unproven is ``E_SQLITE_PROBE_UNSAFE``.
    """
    diagnostics: list[Diagnostic] = []
    report: dict[str, Any] = {
        "runtime": _sqlite_runtime_triple(),
        "policy": SQLITE_PROBE_POLICY_NOT_REGULAR,
        "uri": None,
        "sql": [PROBE_BEGIN_SQL, PROBE_ROLLBACK_SQL],
        "journal_mode_header": "unknown",
        "performed": False,
        "in_transaction": False,
        "rolled_back": False,
        "competing_writer": {
            "attempted": False,
            "refused": False,
            "sqlite_errorname": None,
            "detail": "",
        },
        "invariance": {"artifacts": "unknown", "parent": "unknown"},
        "failure": None,
        "unsafe": False,
        "qualified": False,
    }
    if any(item.kind != "absent" for item in sidecars.values()):
        report["policy"] = SQLITE_PROBE_POLICY_PRESENT
        for suffix, code in _SIDECAR_CODES.items():
            if sidecars[suffix].kind != "absent":
                diagnostics.append(
                    Diagnostic(
                        code,
                        "error",
                        f"SQLite sidecar {suffix} is present; the write-lock probe is not run",
                        "sqlite",
                    )
                )
        return report, diagnostics
    if source_identity.kind != "regular_file" or source_identity.size is None:
        report["policy"] = SQLITE_PROBE_POLICY_NOT_REGULAR
        diagnostics.append(
            Diagnostic(
                "E_SOURCE_SQL_NOT_REGULAR",
                "error",
                "the write-lock probe is only run on a regular file source",
                "sqlite",
            )
        )
        return report, diagnostics
    report["policy"] = SQLITE_PROBE_POLICY_ABSENT
    report["uri"] = _sqlite_uri(source, SQLITE_WRITABLE_PROBE_QUERY)
    report["journal_mode_header"] = _journal_mode_hint(source)
    before_artifacts, before_parent = _probe_invariance_key(source)
    try:
        probe = _transaction_probe(source, while_locked=while_locked)
    except sqlite3.Error as exc:
        # The probe could not take the write lock at all (a live writer, an
        # unwritable source or another open refusal). No transaction was held, so
        # nothing changed, but exclusion and the ROLLBACK contract stay UNPROVEN.
        report["unsafe"] = True
        report["failure"] = {
            "sqlite_errorname": getattr(exc, "sqlite_errorname", None),
            "detail": str(exc),
        }
        diagnostics.append(
            Diagnostic(
                "E_SQLITE_PROBE_UNSAFE",
                "error",
                "the bounded write-lock probe could not take the write lock; exclusion is not proven",
                "sqlite",
            )
        )
        return report, diagnostics
    report.update(probe)
    after_artifacts, after_parent = _probe_invariance_key(source)
    artifacts_ok = before_artifacts == after_artifacts
    parent_ok = before_parent == after_parent and before_parent[1] == "ok"
    report["invariance"] = {
        "artifacts": "ok" if artifacts_ok else "failed",
        "parent": "ok" if parent_ok else "failed",
    }
    if not artifacts_ok or not parent_ok:
        report["unsafe"] = True
        diagnostics.append(
            Diagnostic(
                "E_SQLITE_PROBE_UNSAFE",
                "error",
                "the source database, a sidecar entry or the parent listing changed across the probe",
                "sqlite",
            )
        )
        return report, diagnostics
    if not report["in_transaction"] or not report["rolled_back"]:
        report["unsafe"] = True
        diagnostics.append(
            Diagnostic(
                "E_SQLITE_PROBE_UNSAFE",
                "error",
                "the probe did not hold and roll back exactly one bounded transaction",
                "sqlite",
            )
        )
        return report, diagnostics
    if not report["competing_writer"]["refused"]:
        report["unsafe"] = True
        diagnostics.append(
            Diagnostic(
                "E_SQLITE_PROBE_UNSAFE",
                "error",
                "a competing writer was not refused; write-lock exclusion is not proven",
                "sqlite",
            )
        )
        return report, diagnostics
    report["qualified"] = not report["unsafe"]
    return report, diagnostics


def _existing_entry_kind(path: Path) -> str:
    """No-follow kind of one entry: absent/directory/regular_file/symlink/special."""
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        return "absent"
    except OSError:
        return "unreadable"
    if stat.S_ISLNK(info.st_mode):
        return "symlink"
    if stat.S_ISDIR(info.st_mode):
        return "directory"
    return "regular_file" if stat.S_ISREG(info.st_mode) else "special"


def _unknown_snapshot_verification() -> dict[str, Any]:
    """The verification shape a snapshot reports until it is actually read."""
    return {
        "open_policy": SNAPSHOT_VERIFY_OPEN_POLICY,
        "integrity": "unknown",
        "schema": "unknown",
        "tables": (),
        "alembic_revision": None,
        "revision_accepted": False,
        "ids": {},
        "ids_digest": None,
        "outbox_counts": dict(_UNKNOWN_OUTBOX_COUNTS),
        "unexpected_outbox_statuses": (),
    }


def _quoted_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _ordered_id_digest(connection: sqlite3.Connection, table: str) -> dict[str, Any]:
    """Bounded, order-stable digest of one table's ``id`` column.

    Rows are streamed in ``id`` order and folded into one digest, so the result
    is a stable identity of the snapshot's ID set without materializing it. A
    table that reaches ``SQLITE_COUNT_CAP`` is reported as capped instead of a
    digest that pretends to cover every row.
    """
    quoted = _quoted_identifier(table)
    cursor = connection.execute(
        f"SELECT id FROM {quoted} WHERE id IS NOT NULL ORDER BY id LIMIT ?", (SQLITE_COUNT_CAP + 1,)
    )
    digest = hashlib.sha256()
    count = 0
    try:
        while True:
            rows = cursor.fetchmany(256)
            if not rows:
                break
            for (value,) in rows:
                if count >= SQLITE_COUNT_CAP:
                    return {"count": f"{SQLITE_COUNT_CAP}+", "digest": None}
                digest.update(str(value).encode("utf-8", "surrogatepass"))
                digest.update(b"\x00")
                count += 1
    finally:
        cursor.close()
    return {"count": count, "digest": digest.hexdigest()}


def _bounded_status_count(connection: sqlite3.Connection, table: str, status: str) -> int | str:
    """Row count of one outbox status, bounded by ``SQLITE_COUNT_CAP``."""
    quoted = _quoted_identifier(table)
    row = connection.execute(
        f"SELECT COUNT(*) FROM (SELECT 1 FROM {quoted} WHERE status = ? LIMIT ?)",
        (status, SQLITE_COUNT_CAP + 1),
    ).fetchone()
    total = int(row[0]) if row else 0
    return f"{SQLITE_COUNT_CAP}+" if total > SQLITE_COUNT_CAP else total


def verify_snapshot(
    snapshot_path: Path, *, accepted_revisions: frozenset[str] = ACCEPTED_SQLITE_SCHEMA_REVISIONS
) -> tuple[dict[str, Any], list[Diagnostic]]:
    """Reopen a run-owned snapshot and verify it IN the run directory.

    DETAIL 7.3 and 10.3: the snapshot is opened normally where it lives -- the
    live source is never re-opened, so a source that moved on after the snapshot
    was taken cannot influence the verdict. Integrity, the table set, the
    Alembic revision, the bounded ordered ID digests and the outbox status
    counts are all read from the snapshot itself; anything else stays ``unknown``
    and blocks with a stable code.
    """
    path = Path(snapshot_path)
    diagnostics: list[Diagnostic] = []
    verification = _unknown_snapshot_verification()
    identity = _inventory(path, artifact="snapshot")
    if identity.kind != "regular_file" or identity.size is None:
        diagnostics.append(
            Diagnostic("E_BACKUP_VERIFY", "error", "the snapshot is not a readable regular file", "snapshot")
        )
        return verification, diagnostics
    try:
        connection = sqlite3.connect(path)
    except sqlite3.Error as exc:
        diagnostics.append(
            Diagnostic(
                "E_BACKUP_VERIFY",
                "error",
                f"the snapshot could not be opened for verification ({type(exc).__name__})",
                "snapshot",
            )
        )
        return verification, diagnostics
    try:
        integrity_row = connection.execute("PRAGMA integrity_check(1)").fetchone()
        integrity = "ok" if integrity_row and integrity_row[0] == "ok" else "failed"
        tables = tuple(
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
                " ORDER BY name LIMIT ?",
                (MAX_SQLITE_TABLES,),
            )
        )
        missing = tuple(name for name in REQUIRED_SNAPSHOT_TABLES if name not in tables)
        revision_rows = (
            connection.execute("SELECT version_num FROM alembic_version ORDER BY version_num LIMIT 2").fetchall()
            if "alembic_version" in tables
            else []
        )
        revision = str(revision_rows[0][0]) if len(revision_rows) == 1 else None
        ids: dict[str, Any] = {}
        for table in tables:
            columns = tuple(
                str(row[1]) for row in connection.execute(f"PRAGMA table_info({_quoted_identifier(table)})")
            )
            if "id" in columns:
                ids[table] = _ordered_id_digest(connection, table)
    except sqlite3.Error as exc:
        diagnostics.append(
            Diagnostic(
                "E_BACKUP_VERIFY",
                "error",
                f"the snapshot could not be verified ({type(exc).__name__})",
                "snapshot",
            )
        )
        return verification, diagnostics
    finally:
        connection.close()
    verification["integrity"] = integrity
    verification["schema"] = "known"
    verification["tables"] = tables
    verification["alembic_revision"] = revision
    verification["revision_accepted"] = bool(revision is not None and revision in accepted_revisions)
    verification["ids"] = ids
    verification["ids_digest"] = _digest(ids)
    if OUTBOX_TABLE_NAME in tables:
        connection = sqlite3.connect(path)
        try:
            verification["outbox_counts"] = {
                status: _bounded_status_count(connection, OUTBOX_TABLE_NAME, status)
                for status in OUTBOX_STATUSES
            }
            statuses = tuple(
                str(row[0])
                for row in connection.execute(
                    f"SELECT DISTINCT status FROM {_quoted_identifier(OUTBOX_TABLE_NAME)} ORDER BY status LIMIT ?",
                    (MAX_DISTINCT_OUTBOX_STATUSES,),
                )
            )
        except sqlite3.Error as exc:
            diagnostics.append(
                Diagnostic(
                    "E_BACKUP_VERIFY",
                    "error",
                    f"the snapshot outbox counts could not be read ({type(exc).__name__})",
                    "snapshot",
                )
            )
            return verification, diagnostics
        finally:
            connection.close()
        verification["unexpected_outbox_statuses"] = tuple(
            status for status in statuses if status not in OUTBOX_STATUSES
        )
    if integrity != "ok":
        diagnostics.append(
            Diagnostic(
                "E_BACKUP_VERIFY", "error", "PRAGMA integrity_check on the snapshot did not return ok", "snapshot"
            )
        )
    if missing:
        diagnostics.append(
            Diagnostic(
                "E_SQLITE_SCHEMA",
                "error",
                f"the snapshot is missing required tables {list(missing)}",
                "snapshot",
            )
        )
    if not verification["revision_accepted"]:
        diagnostics.append(
            Diagnostic(
                "E_SQLITE_SCHEMA",
                "error",
                "the snapshot Alembic revision is absent, ambiguous or not accepted",
                "snapshot",
            )
        )
    return verification, diagnostics


def _snapshot_source_sql(
    source: Path, *, run_dir: Path, accepted_revisions: frozenset[str] = ACCEPTED_SQLITE_SCHEMA_REVISIONS
) -> tuple[dict[str, Any], list[Diagnostic]]:
    """Materialize the run-owned safety snapshot from the immutable read-only source.

    DETAIL 7.3 and 10.1: the snapshot is taken with
    ``sqlite3.Connection.backup`` from the SAME percent-encoded
    ``mode=ro&immutable=1`` URI the dry-run probe qualified. The database file is
    never copied, and no WAL/SHM is copied, checkpointed or recovered. An
    existing run artifact is never overwritten, and a symlinked or special
    snapshot path is refused instead of followed.
    """
    diagnostics: list[Diagnostic] = []
    report: dict[str, Any] = {
        "path": None,
        "source_uri": _sqlite_uri(source, SQLITE_READONLY_IMMUTABLE_QUERY),
        "api": SNAPSHOT_API,
        "created": False,
        "snapshot_sha256": None,
        "size": None,
        "verification": _unknown_snapshot_verification(),
    }
    directory = Path(run_dir) / SNAPSHOT_DIR_NAME
    target = directory / SNAPSHOT_DB_NAME
    report["path"] = str(target)
    directory_kind = _existing_entry_kind(directory)
    if directory_kind == "symlink":
        diagnostics.append(
            Diagnostic(
                "E_PATH_FINAL_SYMLINK_UNSAFE",
                "error",
                "the run-owned snapshot directory is a symlink and is never followed",
                "snapshot",
            )
        )
        return report, diagnostics
    if directory_kind in {"special", "regular_file"}:
        diagnostics.append(
            Diagnostic(
                "E_PATH_SPECIAL_FILE",
                "error",
                "the run-owned snapshot directory path is not a directory",
                "snapshot",
            )
        )
        return report, diagnostics
    if directory_kind == "unreadable":
        diagnostics.append(
            Diagnostic("E_BACKUP_VERIFY", "error", "the run-owned snapshot directory cannot be inspected", "snapshot")
        )
        return report, diagnostics
    if directory_kind == "absent":
        try:
            directory.mkdir(parents=True, mode=0o700)
            os.chmod(directory, 0o700)
        except OSError as exc:
            diagnostics.append(
                Diagnostic(
                    _path_code(exc),
                    "error",
                    "the run-owned snapshot directory could not be created",
                    "snapshot",
                )
            )
            return report, diagnostics
    target_kind = _existing_entry_kind(target)
    if target_kind == "symlink":
        diagnostics.append(
            Diagnostic(
                "E_PATH_FINAL_SYMLINK_UNSAFE",
                "error",
                "a symlink already exists at the snapshot path; it is never followed or overwritten",
                "snapshot",
            )
        )
        return report, diagnostics
    if target_kind == "special":
        diagnostics.append(
            Diagnostic("E_PATH_SPECIAL_FILE", "error", "the snapshot path is not a regular file", "snapshot")
        )
        return report, diagnostics
    if target_kind == "regular_file":
        diagnostics.append(
            Diagnostic(
                "E_BACKUP_COLLISION",
                "error",
                "a run artifact already exists at the snapshot path; it is never overwritten",
                "snapshot",
            )
        )
        return report, diagnostics
    if target_kind == "unreadable":
        diagnostics.append(
            Diagnostic("E_BACKUP_VERIFY", "error", "the snapshot path cannot be inspected no-follow", "snapshot")
        )
        return report, diagnostics
    try:
        source_connection = sqlite3.connect(report["source_uri"], uri=True, timeout=0)
    except sqlite3.Error as exc:
        diagnostics.append(
            Diagnostic(
                "E_BACKUP_VERIFY",
                "error",
                f"the qualified immutable read-only source could not be opened ({type(exc).__name__})",
                "snapshot",
            )
        )
        return report, diagnostics
    try:
        source_connection.execute("PRAGMA query_only = ON")
        try:
            target_connection = sqlite3.connect(target)
        except sqlite3.Error as exc:
            diagnostics.append(
                Diagnostic(
                    "E_BACKUP_VERIFY",
                    "error",
                    f"the run-owned snapshot could not be created ({type(exc).__name__})",
                    "snapshot",
                )
            )
            return report, diagnostics
        try:
            source_connection.backup(target_connection)
        except sqlite3.Error as exc:
            diagnostics.append(
                Diagnostic(
                    "E_BACKUP_VERIFY",
                    "error",
                    f"sqlite3.Connection.backup could not complete ({type(exc).__name__})",
                    "snapshot",
                )
            )
            return report, diagnostics
        finally:
            target_connection.close()
    finally:
        source_connection.close()
    report["created"] = True
    _fsync_directory(directory)
    identity = _inventory(target, artifact="snapshot")
    report["snapshot_sha256"] = identity.sha256
    report["size"] = identity.size
    verification, verification_diagnostics = verify_snapshot(target, accepted_revisions=accepted_revisions)
    report["verification"] = verification
    diagnostics.extend(verification_diagnostics)
    return report, diagnostics


def qualify_sqlite_source(
    source: Path,
    *,
    run_dir: Path,
    while_locked: Callable[[], Any] | None = None,
    accepted_revisions: frozenset[str] = ACCEPTED_SQLITE_SCHEMA_REVISIONS,
) -> tuple[dict[str, Any], list[Diagnostic]]:
    """Qualify one SQLite source and materialize its run-owned safety snapshot.

    DETAIL 6.3 step 10, 7.3 and 10.1, fail-closed and in this order:

    1. only a sidecar-free regular source is considered at all (a present WAL,
       SHM or journal -- including a zero-byte one -- means NO SQLite open of any
       kind, so no recovery, no checkpoint and no trimmed WAL);
    2. the exact ``BEGIN IMMEDIATE`` / ``ROLLBACK`` write-lock probe runs on the
       source together with a competing-writer refusal, and the database bytes,
       every sidecar entry and the parent listing are proven unchanged across it;
    3. only then is the safety snapshot taken with ``sqlite3.Connection.backup``
       from the percent-encoded immutable read-only URI and verified inside the
       run directory.

    Any unproven step is ``E_SQLITE_PROBE_UNSAFE`` and no snapshot is created
    from an unqualified source. The source is never checkpointed, recovered,
    trimmed or copied, and the writable handle is the bounded probe only.
    """
    source = Path(source)
    run_dir = Path(run_dir)
    diagnostics: list[Diagnostic] = []
    source_identity = _inventory(source, artifact="source", diagnostics=diagnostics)
    sidecars = {
        suffix: _inventory(
            source.with_name(source.name + suffix), artifact=f"sidecar{suffix}", diagnostics=diagnostics
        )
        for suffix in _SIDECAR_SUFFIXES
    }
    probe, probe_diagnostics = _qualify_transaction_probe(
        source,
        source_identity=source_identity,
        sidecars=sidecars,
        while_locked=while_locked,
    )
    diagnostics.extend(probe_diagnostics)
    report: dict[str, Any] = {"probe": probe, "snapshot": None}
    if probe_diagnostics:
        return report, diagnostics
    snapshot, snapshot_diagnostics = _snapshot_source_sql(
        source, run_dir=run_dir, accepted_revisions=accepted_revisions
    )
    report["snapshot"] = snapshot
    diagnostics.extend(snapshot_diagnostics)
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


_MAINTENANCE_PROCESS_CLASSES = (
    "hermes-profile-runtime",
    "standalone-runtime",
    "legacy-memory-server",
)


# The maintenance coordination entries: the root lock and the applicable graph
# lock. They are not payload artifacts. Their entire purpose is to be CREATED by
# the lock stage (``acquire_maintenance_locks`` -> ``MaintenanceStorageLocks``)
# and, by design, never unlinked, so a lock cycle legitimately turns them from
# ``absent`` into ``regular_file``.
#
# They are therefore deliberately excluded from the freshness digest below while
# remaining in ``plan.targets``, in the writer inventory and in the disk
# requirement. Digesting them made the freshness gate and the lock stage of this
# same card non-composable: after one lock cycle every caller-supplied plan was
# ``E_PLAN_STALE``, which masked the cause-specific fail-closed codes
# (``E_OLD_WRITER_ACTIVE`` / ``E_WRITER_ACTIVE``) that acceptance 4 requires a
# live-activity scenario to report first. A live upgraded runtime is still
# detected -- by the writer inventory (``upgraded_holders`` -> a matching
# resource fd becomes ``upgraded_writers``) and by ``E_LOCK_TIMEOUT`` when the
# held entry is reacquired exclusively -- and an unsafe replacement of either
# entry is still refused by ``open``-time verification (``E_LOCK_ENTRY_UNSAFE`` /
# ``E_ARTIFACT_IDENTITY_CHANGED``), so nothing is weakened.
_COORDINATION_TARGET_LABELS = ("root_lock", "graph_lock")


def _plan_identity_digest(plan: MigrationPlan) -> str:
    """Canonical identity digest of one plan's source, sidecars and targets.

    Two plans of the same unchanged artifacts share this digest whatever their
    request fields were; any identity, size, mtime or content change moves it,
    which is exactly the staleness test of DETAIL 6.3 step 4.

    The coordination entries listed in ``_COORDINATION_TARGET_LABELS`` are
    excluded: the lock stage of this card creates them and never unlinks them,
    so they are not part of the payload identity the caller planned against.
    Excluding them is exactly what stops a legitimate lock cycle from reading as
    a stale plan, and it is why the INODE of an entry the plan recorded as
    present is confirmed separately by ``_confirm_coordination_identities``
    instead of being dropped from verification altogether (S2-05 N1 pin).
    """
    return _digest(
        {
            "source": asdict(plan.source_sql),
            "sidecars": {
                suffix: asdict(identity) for suffix, identity in plan.source_sidecars.items()
            },
            "targets": {
                label: asdict(identity)
                for label, identity in plan.targets.items()
                if label not in _COORDINATION_TARGET_LABELS
            },
            "config_digest": plan.config_digest,
        }
    )


def _confirm_coordination_identities(plan: MigrationPlan, fresh: MigrationPlan) -> None:
    """Confirm the inode of a coordination entry the caller's plan recorded.

    S2-04 removed ``root_lock``/``graph_lock`` from ``_plan_identity_digest``
    because a legitimate lock cycle CREATES them and never unlinks them, so any
    lock-then-validate order was a false ``E_PLAN_STALE``. That removal also
    stopped noticing a coordination entry REPLACED by a different ordinary
    regular file (``nlink == 1``), which the old digest refused -- an unsound
    detection, but a real blind spot once it was gone.

    The digest still cannot carry those entries, so the identity is confirmed
    here: when the caller's plan recorded the entry as PRESENT, the fresh replan
    must show the same ``(kind, device, inode)``. A size or mtime change from a
    legitimate re-acquisition of the SAME inode is not drift and stays fresh; a
    different inode, a vanished entry and an entry that degraded to a refused
    kind are ``E_PLAN_STALE``. An entry the plan recorded as ABSENT may
    legitimately exist now -- this run's own lock stage created it -- and is
    never stale. Only the two coordination labels are treated this way: every
    other target keeps its place in ``_plan_identity_digest``.
    """
    for label in _COORDINATION_TARGET_LABELS:
        recorded = plan.targets.get(label)
        if recorded is None or recorded.kind == "absent":
            continue
        observed = fresh.targets.get(label)
        if observed is None or observed.kind == "absent":
            raise ValueError("E_PLAN_STALE")
        if (recorded.kind, recorded.device, recorded.inode) != (
            observed.kind,
            observed.device,
            observed.inode,
        ):
            raise ValueError("E_PLAN_STALE")


@dataclass(frozen=True)
class StopAttestation:
    """Bounded, recorded stop attestation (DETAIL 6.3 step 3)."""

    value_bytes: int
    digest: str
    recorded_at: str
    roots: tuple[str, ...]
    process_classes: tuple[str, ...]


@dataclass(frozen=True)
class MutationPreconditions:
    """Everything apply/resume/rollback must prove before any target mutation."""

    mode: str
    roots: tuple[str, ...]
    graph_lock_path: str
    attestation: StopAttestation
    plan_digest: str | None
    replan_digest: str
    quiet_interval: float
    probe: Mapping[str, Any]
    stability: Mapping[str, Any]
    writer_state: Mapping[str, Any]


def _running_under_pytest() -> bool:
    return "pytest" in sys.modules


def resolve_quiet_interval(explicit: float | None = None) -> float:
    """DETAIL 6.3 step 11: 2 s by default under tests, 5 s for the CLI.

    An explicit value always wins (it is what an operator or a caller passes);
    otherwise the default is the test default only when this interpreter really
    has pytest loaded, so a CLI process can never silently take the short
    interval.
    """
    if explicit is None:
        return QUIET_INTERVAL_SECONDS_TESTS if _running_under_pytest() else QUIET_INTERVAL_SECONDS_CLI
    value = float(explicit)
    if not math.isfinite(value) or value < 0 or value > MAX_QUIET_INTERVAL_SECONDS:
        raise ValueError("E_QUIET_INTERVAL_INVALID")
    return value


def record_stop_attestation(attestation: Any, *, roots: Iterable[Path]) -> StopAttestation:
    """Bound, digest and timestamp ``--attest-runtimes-stopped``; never store it raw."""
    if not isinstance(attestation, str) or not attestation.strip():
        raise ValueError("E_STOP_ATTESTATION_REQUIRED")
    payload = attestation.encode("utf-8")
    if len(payload) > MAX_ATTESTATION_BYTES or b"\x00" in payload:
        raise ValueError("E_ATTESTATION_UNBOUNDED")
    canonical_roots = tuple(
        sorted({str(_canonical_root(Path(root))) for root in roots}, key=lambda value: os.fsencode(value))
    )
    if len(canonical_roots) > MAX_MAINTENANCE_ROOTS:
        raise ValueError("E_ATTESTATION_UNBOUNDED")
    return StopAttestation(
        len(payload),
        hashlib.sha256(payload).hexdigest(),
        datetime.now(timezone.utc).isoformat(),
        canonical_roots,
        _MAINTENANCE_PROCESS_CLASSES,
    )


def _canonical_root(path: Path) -> Path:
    value = os.path.expanduser(os.fspath(path))
    if not value or "\x00" in value:
        raise ValueError("E_PATH_INVALID")
    return Path(os.path.abspath(os.path.normpath(value)))


def maintenance_lock_roots(plan: MigrationPlan) -> tuple[Path, ...]:
    """The source and target roots of DETAIL 6.3 step 8, sorted lexically.

    Canonicalised and deduplicated first, so two spellings of one root can never
    produce two locks and the acquisition order is stable across machines.
    """
    candidates: list[Path] = [Path(plan.layout.data_root)]
    if plan.request.target_root is not None:
        candidates.append(Path(plan.request.target_root))
    vector = plan.layout.vector.local_path
    if vector is not None:
        # The vector store may be a symlink to another filesystem (this
        # deployment's live layout is exactly that). A symlink is never a lock
        # root -- the lock entry must live in a real directory -- so the lock
        # goes into the store's parent and the entry itself is never followed.
        candidates.append(Path(vector).parent)
    candidates.append(Path(plan.layout.graph_snapshot_path).parent)
    candidates.append(Path(plan.layout.graph_lock_path).parent)
    canonical = {_canonical_root(candidate) for candidate in candidates}
    return tuple(sorted(canonical, key=lambda path: os.fsencode(str(path))))


def _writer_labels(plan: MigrationPlan) -> tuple[dict[str, str], tuple[str, ...]]:
    """RAW label strings to look for in other processes' descriptor tables.

    Every value is the exact string the kernel prints for the entry -- the lock
    entries, the source and its sidecars, the graph snapshot, the vector store
    and a legacy link's recorded referent string. Nothing here is resolved, so a
    legacy referent that is itself a symlink is matched as the raw string and is
    never followed.
    """
    labels: dict[str, str] = {
        "root_lock": str(Path(plan.layout.root_lock_path)),
        "graph_lock": str(Path(plan.layout.graph_lock_path)),
    }
    labels["source"] = plan.source_sql.lexical_path
    for suffix, identity in plan.source_sidecars.items():
        labels[f"sidecar{suffix}"] = identity.lexical_path
    labels["graph_snapshot"] = str(Path(plan.layout.graph_snapshot_path))
    vector = plan.layout.vector.local_path
    if vector is not None:
        labels["vector"] = str(Path(vector))
    for index, identity in enumerate(plan.legacy_projections):
        if identity.kind == "absent":
            continue
        labels[f"legacy_link{index}"] = identity.lexical_path
        if identity.raw_link_target:
            labels[f"legacy_referent{index}"] = identity.raw_link_target
    return labels, _COORDINATION_TARGET_LABELS


def _writer_inventory(
    plan: MigrationPlan,
    *,
    proc_root: str | Path = DEFAULT_PROC_ROOT,
    exclude_pids: Iterable[int] = (),
) -> storage_lock.WriterInventory:
    labels, lock_keys = _writer_labels(plan)
    return storage_lock.scan_writer_inventory(
        labels,
        lock_label_keys=lock_keys,
        proc_root=proc_root,
        exclude_pids=(os.getpid(), *exclude_pids),
    )


def _writer_state_report(inventory: storage_lock.WriterInventory) -> dict[str, Any]:
    return {
        "covered": inventory.covered,
        "code": inventory.code,
        "scanned_pids": inventory.scanned_pids,
        "excluded_foreign_namespace": inventory.excluded_foreign_namespace,
        "excluded_foreign_credential": inventory.excluded_foreign_credential,
        "gaps": list(inventory.gaps),
        "legacy_writers": [asdict(record) for record in inventory.legacy_writers],
        "upgraded_holders": [asdict(record) for record in inventory.upgraded_holders],
        "upgraded_writers": [asdict(record) for record in inventory.upgraded_writers],
        "lock_records": [asdict(record) for record in inventory.lock_records],
    }


def _require_fresh_sidecar_path_and_disk_checks(plan: MigrationPlan) -> None:
    """DETAIL 6.3 steps 5-7, recomputed from the FRESH plan.

    A caller-supplied plan is never trusted for these: they are re-derived from
    the replan that just happened, so an entrypoint cannot inherit another
    invocation's evidence.
    """
    if plan.source_sql.kind == "absent":
        raise ValueError("E_SOURCE_SQL_REQUIRED")
    if plan.source_sql.kind != "regular_file":
        raise ValueError("E_SOURCE_SQL_NOT_REGULAR")
    for suffix in _SIDECAR_SUFFIXES:
        identity = plan.source_sidecars.get(suffix)
        if identity is not None and identity.kind != "absent":
            raise ValueError(_SIDECAR_CODES[suffix])
    data_root = str(_canonical_root(Path(plan.layout.data_root)))
    manifest_path = str(
        _canonical_root(
            Path(plan.layout.data_root) / RUN_DIRECTORY_NAME / plan.request.run_id / MANIFEST_FILE_NAME
        )
    )
    if manifest_path != data_root and not manifest_path.startswith(data_root.rstrip("/") + "/"):
        raise ValueError("E_MANIFEST_PATH_ESCAPE")
    available, _ = _available_bytes(Path(plan.layout.data_root))
    if available is None or plan.required_bytes is None:
        raise ValueError("E_INSUFFICIENT_SPACE")
    if available < plan.required_bytes * DISK_MARGIN_RATIO:
        raise ValueError("E_INSUFFICIENT_SPACE")


def _observed_identity(path: Path, artifact: str) -> dict[str, Any]:
    return asdict(_inventory(Path(path), artifact=artifact))


def _observe_across_quiet_interval(
    plan: MigrationPlan, interval: float
) -> tuple[dict[str, Any], tuple[str, ...]]:
    """Identity AND hash stability of every watched artifact across the interval."""
    watched: list[tuple[str, Path]] = [("source", Path(plan.source_sql.lexical_path))]
    for suffix, identity in plan.source_sidecars.items():
        watched.append((f"sidecar{suffix}", Path(identity.lexical_path)))
    watched.append(("graph_snapshot", Path(plan.layout.graph_snapshot_path)))
    vector = plan.layout.vector.local_path
    if vector is not None:
        watched.append(("vector", Path(vector)))
    before = {label: _observed_identity(path, label) for label, path in watched}
    time.sleep(interval)
    after = {label: _observed_identity(path, label) for label, path in watched}
    changed = tuple(sorted(label for label in before if before[label] != after[label]))
    return {
        "quiet_interval_seconds": interval,
        "artifacts": list(before),
        "before": before,
        "after": after,
        "changed": list(changed),
        "sha256_before": {label: value.get("sha256") for label, value in before.items()},
        "sha256_after": {label: value.get("sha256") for label, value in after.items()},
    }, changed


def _qualify_maintenance_probe(plan: MigrationPlan) -> dict[str, Any]:
    report, diagnostics = _qualify_transaction_probe(
        Path(plan.source_sql.lexical_path),
        source_identity=plan.source_sql,
        sidecars=plan.source_sidecars,
    )
    payload = dict(report)
    payload["diagnostics"] = [item.code for item in diagnostics]
    return payload


def validate_mutation_preconditions(
    request: MigrationRequest,
    *,
    plan: MigrationPlan | None = None,
    proc_root: str | Path = DEFAULT_PROC_ROOT,
    quiet_interval: float | None = None,
    exclude_pids: Iterable[int] = (),
) -> MutationPreconditions:
    """Validate every precondition of DETAIL 6.3 before any artifact is mutated.

    Called independently by ``apply``, ``resume`` and ``rollback`` -- and by any
    later engine stage -- so no entrypoint inherits another's validated state:

    1. intent: mode, exact canonical target confirmation and the ``--apply`` rule
       (the approved S2/S0 checks, unchanged);
    2. a bounded attestation: non-blank, size-bounded, digested and timestamped,
       with the affected roots and the listed process classes recorded;
    3. the replan: a FRESH ``plan_profile_migration`` of the current no-follow
       identities. A caller-supplied plan whose identity digest no longer matches
       is ``E_PLAN_STALE`` -- a stale plan is never trusted -- and, because the
       two coordination entries are outside that digest by design (S2-04), the
       inode of any one the plan recorded as PRESENT is confirmed against the
       fresh replan as well (``_confirm_coordination_identities``, S2-05);
    4. sidecars, paths and disk recomputed from that fresh plan;
    5. the complete ``/proc`` descriptor and lock inventory: an old writer
       ``E_OLD_WRITER_ACTIVE``, a live upgraded runtime ``E_WRITER_ACTIVE``, an
       incomplete or unreadable inventory ``E_WRITER_STATE_UNKNOWN`` and a
       missing inventory source ``E_WRITER_INVENTORY_UNSUPPORTED`` all fail
       closed, and no process is ever signalled;
    6. identity AND hash stability of every watched artifact across the quiet
       interval (2 s under tests, 5 s for the CLI); any change is
       ``E_SOURCE_CHANGED``;
    7. the prequalified bounded SQLite transaction probe; anything unproven is
       ``E_SQLITE_PROBE_UNSAFE``.

    Ordering note: DETAIL 6.3 lists the probe (10), the interval (11) and the
    inventory (12). This function runs the inventory before the interval and the
    probe last, so detection happens strictly earlier and the only step that
    takes a writable handle on the source is the last one; nothing is weakened.
    """
    _require_mutation_preconditions(request, plan)
    fresh = plan_profile_migration(request)
    roots = maintenance_lock_roots(fresh)
    attestation = record_stop_attestation(request.stop_attestation, roots=roots)
    replan_digest = _plan_identity_digest(fresh)
    plan_digest = _plan_identity_digest(plan) if plan is not None else None
    if plan_digest is not None and plan_digest != replan_digest:
        raise ValueError("E_PLAN_STALE")
    if plan is not None:
        # The coordination entries are outside the digest (S2-04), so their
        # recorded inode is confirmed here instead of being unverified (S2-05).
        _confirm_coordination_identities(plan, fresh)
    _require_fresh_sidecar_path_and_disk_checks(fresh)
    inventory = _writer_inventory(fresh, proc_root=proc_root, exclude_pids=exclude_pids)
    if not inventory.covered:
        raise ValueError(inventory.code or "E_WRITER_STATE_UNKNOWN")
    if inventory.legacy_writers:
        raise ValueError("E_OLD_WRITER_ACTIVE")
    if inventory.upgraded_writers:
        raise ValueError("E_WRITER_ACTIVE")
    interval = resolve_quiet_interval(quiet_interval)
    stability, changed = _observe_across_quiet_interval(fresh, interval)
    if changed:
        raise ValueError("E_SOURCE_CHANGED")
    probe = _qualify_maintenance_probe(fresh)
    if not probe.get("qualified"):
        raise ValueError("E_SQLITE_PROBE_UNSAFE")
    return MutationPreconditions(
        mode=request.mode,
        roots=tuple(str(root) for root in roots),
        graph_lock_path=str(Path(fresh.layout.graph_lock_path)),
        attestation=attestation,
        plan_digest=plan_digest,
        replan_digest=replan_digest,
        quiet_interval=interval,
        probe=probe,
        stability=stability,
        writer_state=_writer_state_report(inventory),
    )


def acquire_maintenance_locks(
    plan: MigrationPlan,
    *,
    timeout: float = storage_lock.DEFAULT_LOCK_TIMEOUT,
    proc_root: str | Path = DEFAULT_PROC_ROOT,
    exclude_pids: Iterable[int] = (),
) -> storage_lock.MaintenanceStorageLocks:
    """DETAIL 6.3 steps 8-9 plus the mixed-version writer re-check.

    Every source/target root is locked exclusively in canonical sorted order and
    the applicable graph lock is held without inode replacement. While holding
    them the writer inventory is repeated, so a writer that appeared between the
    pure checks and the lock is still fatal. The returned object owns every
    descriptor for its whole lifetime: ``release`` is refused inside an open
    critical section (``E_LOCK_RELEASE_UNSAFE``) and never unlinks the lock
    entry, so the resource stays protected after the lock is dropped. On any
    refusal the locks taken so far are released before the error propagates.
    """
    locks = storage_lock.MaintenanceStorageLocks.acquire(
        maintenance_lock_roots(plan),
        timeout=timeout,
        graph_lock_path=Path(plan.layout.graph_lock_path),
    )
    try:
        inventory = _writer_inventory(plan, proc_root=proc_root, exclude_pids=exclude_pids)
        if not inventory.covered:
            raise storage_lock.StorageLockError(inventory.code or "E_WRITER_STATE_UNKNOWN")
        if inventory.legacy_writers:
            raise storage_lock.StorageLockError("E_OLD_WRITER_ACTIVE")
        if inventory.upgraded_writers:
            raise storage_lock.StorageLockError("E_WRITER_ACTIVE")
    except BaseException:
        locks.release()
        raise
    return locks


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
    started = time.perf_counter()
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
    counts, bytes_total = _manifest_observability_totals(manifest.artifacts)
    event = manifest.events[-1] if manifest.events else None
    if manifest.status in _MIGRATION_TERMINAL_STATES:
        _migration_runs.labels(state=manifest.status, strategy=manifest.strategy).inc()
    logger.info(
        "cmms.migration %s",
        json.dumps(
            {
                "run_id": manifest.run_id,
                "checkpoint": manifest.checkpoint,
                "artifact_operation": event.operation if event else "manifest_write",
                "counts": counts,
                "bytes": bytes_total,
                "elapsed": time.perf_counter() - started,
                "manifest_path": f"<redacted>/{target.name}",
            },
            sort_keys=True,
        ),
    )


# ---------------------------------------------------------------------------
# S2-05 -- the immutable run-owned backup of the regular artifacts and of the
# final legacy link entries (DETAIL 9.1, 10.1).
#
# The whole slice is built on three promises:
#
# 1. NOTHING IS FOLLOWED. Every entry is inspected through the pinned no-follow
#    descriptor of its parent, opened with O_NOFOLLOW, and re-checked against the
#    immediately preceding no-follow stat. A final symlink is either one of the
#    plan's OWN legacy projections -- recorded as a raw link entry -- or refused;
#    an interior symlink, a special file and a hard-linked regular file are
#    refused. The referent of a legacy link is never opened, enumerated, hashed,
#    copied, modified or validated as a store.
# 2. NOTHING IS OVERWRITTEN. Every regular file is staged inside the run
#    directory, verified there, and published with ``os.link`` -- the atomic
#    no-overwrite publication on the same filesystem -- so a collision is
#    ``E_BACKUP_COLLISION`` and a copy/fsync failure leaves a staged temp for
#    forensics but never a partial file under a published backup name.
# 3. EVERY IDENTITY COMES FROM A STREAMING READ. Identity digests use the
#    S2-02 reader (``_streamed_digest``): a looped, size-bounded fd-relative read
#    with fstat before and after on the SAME descriptor. The bounded 64 KiB
#    readers in ``storage_lock`` are never used for a backup identity.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BackupEntry:
    """One verified backup record (DETAIL 10.1).

    ``source_path``/``kind``/``device``/``inode``/``mode``/``size`` are the
    identity of the ORIGINAL entry as measured through the pinned descriptor it
    was read with; ``sha256`` is the digest of a regular file's whole content or
    of a directory's bounded listing (``digest_scope`` says which, and a symlink
    has neither); ``present`` is the explicit absence record this slice owes
    instead of a silent skip; ``run_relative_path`` is the run-directory
    relative destination, None when nothing was created because nothing exists.
    """

    artifact: str
    source_path: str
    run_relative_path: str | None
    kind: str
    present: bool
    digest_scope: str
    sha256: str | None = None
    size: int | None = None
    mode: int | None = None
    device: int | None = None
    inode: int | None = None
    raw_link_target: str | None = None


@dataclass(frozen=True)
class RunBackup:
    """The immutable backup of one run id: entries, records and report path."""

    run_id: str
    run_dir: str
    report_path: str
    entries: tuple[BackupEntry, ...]
    sqlite: Mapping[str, Any]
    graph_lock: Mapping[str, Any]
    digest: str
    created_at: str


def _backup_failure(code: str, detail: str = "") -> ValueError:
    """A stable, fail-closed backup refusal; never a silent partial backup."""
    return ValueError(f"{code}: {detail}" if detail else code)


def _staged_file_name() -> str:
    """A fresh staged name inside the run directory that cannot pre-exist."""
    return f"stage-{os.getpid()}-{uuid4().hex}"


def _backup_entry_name(label: str) -> str:
    """A bounded, filesystem-safe run-owned name derived from an artifact label.

    The label is a PLAN artifact label (``target:graph``, ``legacy:0``), never
    caller path input. The local part after the prefix is used, and the two
    target labels of DETAIL 9.1 keep the run-directory subpaths that section
    names literally (``backup/vector`` and ``backup/graph.json``); every other
    label is sanitized into the run-owned name alphabet, so no label can address
    a parent component, an absolute path, a home root or a link.
    """
    prefix, _, local = label.partition(":")
    if prefix == "target":
        candidate = _BACKUP_TARGET_NAMES.get(local, local)
    elif prefix == "legacy":
        candidate = f"legacy-{local}"
    else:
        candidate = label
    safe = re.sub(r"[^A-Za-z0-9._-]", "-", candidate).strip("-") or "entry"
    return safe[:MAX_BACKUP_NAME_BYTES]


def _backup_run_directory(plan: MigrationPlan) -> Path:
    """The plan's own run directory: ``<data_root>/.cmms-migrations/<run-id>``."""
    return Path(plan.layout.data_root) / RUN_DIRECTORY_NAME / _validate_run_id(plan.request.run_id)


def _require_same_device(run_device: int, source_device: int, *, artifact: str) -> None:
    """DETAIL 9.1: the run directory must share the source's filesystem.

    Staging and publication are the same run directory, and the publication of a
    staged copy is an atomic link inside one filesystem. A cross-device run
    directory therefore cannot publish atomically at all, so the backup refuses
    BEFORE any byte is copied instead of leaving a copy that would have to be
    moved across devices.
    """
    if run_device != source_device:
        raise _backup_failure(
            "E_CROSS_FILESYSTEM_PUBLICATION",
            f"{artifact} is on device {source_device} but the run directory is on device {run_device}",
        )


def _take_backup_slot(budget: list[int], artifact: str) -> None:
    """Count one bounded backup entry; exceeding the bound is a refusal."""
    budget[0] += 1
    if budget[0] > MAX_BACKUP_ENTRIES:
        raise _backup_failure(
            "E_BACKUP_VERIFY", f"{artifact} exceeds the {MAX_BACKUP_ENTRIES}-entry backup bound"
        )


@contextlib.contextmanager
def _staging_directory(run_dir: Path) -> Iterator[tuple[int, Path]]:
    """The run-owned staging directory, opened through pinned descriptors."""
    target = Path(run_dir) / BACKUP_STAGING_NAME / BACKUP_STAGING_TEMP_NAME
    with storage_lock.open_directory_nofollow(target, create=True) as descriptor:
        yield descriptor, target


@contextlib.contextmanager
def _run_child_directory(parent_fd: int, name: str) -> Iterator[int]:
    """Open (creating at 0700) one run-owned child directory, never a link.

    ``name`` is a single bounded component built by this module, never caller
    path input. The create is idempotent, and the open is
    ``O_DIRECTORY | O_NOFOLLOW``: a symlink or a non-directory planted at a
    run-owned path is refused instead of traversed.
    """
    if not _RUN_OWNED_NAME_PATTERN.match(name):
        raise _backup_failure("E_MANIFEST_PATH_ESCAPE", "run-owned directory name is not a bounded component")
    with contextlib.suppress(FileExistsError):
        os.mkdir(name, BACKUP_DIRECTORY_MODE, dir_fd=parent_fd)
    try:
        descriptor = os.open(
            name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent_fd
        )
    except FileNotFoundError as exc:
        raise _backup_failure("E_BACKUP_VERIFY", f"run-owned directory {name} does not exist") from exc
    except OSError as exc:
        raise _backup_failure(
            "E_PATH_SPECIAL_FILE", f"run-owned directory {name} is not a real directory: {exc.strerror}"
        ) from exc
    try:
        os.fchmod(descriptor, BACKUP_DIRECTORY_MODE)
        yield descriptor
    finally:
        os.close(descriptor)


@contextlib.contextmanager
def _run_directory_chain(root_fd: int, parts: tuple[str, ...]) -> Iterator[int]:
    """Create/open a bounded run-owned directory chain through pinned descriptors."""
    if not parts:
        yield root_fd
        return
    with _run_child_directory(root_fd, parts[0]) as child_fd:
        with _run_directory_chain(child_fd, parts[1:]) as deep_fd:
            yield deep_fd


@contextlib.contextmanager
def _pinned_child_regular_file(
    parent_fd: int, name: str, *, artifact: str, before: os.stat_result
) -> Iterator[tuple[int, os.stat_result]]:
    """Open one regular child relative to its pinned parent descriptor.

    A final symlink, a special file and a hard-linked regular file are refused
    with the stable path codes, and the fstat of the descriptor must match the
    immediately preceding no-follow stat, so nothing is ever read through a link
    or through a multiply-linked inode.
    """
    if stat.S_ISLNK(before.st_mode):
        raise _backup_failure("E_PATH_FINAL_SYMLINK_UNSAFE", f"{artifact} is a symlink and is never followed")
    if not stat.S_ISREG(before.st_mode):
        raise _backup_failure("E_PATH_SPECIAL_FILE", f"{artifact} is not a regular file")
    if before.st_nlink != 1:
        raise _backup_failure("E_PATH_HARDLINK_UNSAFE", f"{artifact} is a hard-linked regular file")
    try:
        # O_NONBLOCK is a no-op for regular files and keeps a FIFO swap between
        # the stat above and this open from blocking forever.
        descriptor = os.open(
            name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK, dir_fd=parent_fd
        )
    except OSError as exc:
        raise _backup_failure(_path_code(exc), f"{artifact} cannot be opened no-follow: {exc.strerror}") from exc
    try:
        opened = os.fstat(descriptor)
        if not _same_inode(before, opened) or not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1:
            raise _backup_failure("E_ARTIFACT_IDENTITY_CHANGED", f"{artifact} identity changed between stat and open")
        yield descriptor, opened
    finally:
        os.close(descriptor)


@contextlib.contextmanager
def _pinned_child_directory(
    parent_fd: int, name: str, *, artifact: str, before: os.stat_result
) -> Iterator[tuple[int, os.stat_result]]:
    """Open one directory child relative to its pinned parent descriptor."""
    if stat.S_ISLNK(before.st_mode):
        raise _backup_failure("E_PATH_FINAL_SYMLINK_UNSAFE", f"{artifact} is a symlink and is never followed")
    if not stat.S_ISDIR(before.st_mode):
        raise _backup_failure("E_PATH_SPECIAL_FILE", f"{artifact} is not a directory")
    try:
        descriptor = os.open(
            name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent_fd
        )
    except OSError as exc:
        raise _backup_failure(_path_code(exc), f"{artifact} cannot be opened no-follow: {exc.strerror}") from exc
    try:
        opened = os.fstat(descriptor)
        if not _same_inode(before, opened) or not stat.S_ISDIR(opened.st_mode):
            raise _backup_failure("E_ARTIFACT_IDENTITY_CHANGED", f"{artifact} identity changed between stat and open")
        yield descriptor, opened
    finally:
        os.close(descriptor)


def _bounded_child_listing(parent_fd: int, *, artifact: str) -> tuple[tuple[str, os.stat_result], ...]:
    """A bounded, deterministic no-follow listing of one pinned directory."""
    try:
        names = sorted(os.listdir(parent_fd), key=os.fsencode)
    except OSError as exc:
        raise _backup_failure(_path_code(exc), f"{artifact} cannot be listed: {exc.strerror}") from exc
    if len(names) > MAX_PARENT_ENTRIES:
        raise _backup_failure(
            "E_BACKUP_VERIFY", f"{artifact} has more than {MAX_PARENT_ENTRIES} entries; the listing is unbounded"
        )
    listing: list[tuple[str, os.stat_result]] = []
    for child in names:
        try:
            listing.append((child, os.stat(child, dir_fd=parent_fd, follow_symlinks=False)))
        except OSError as exc:
            raise _backup_failure(
                _path_code(exc), f"{artifact}/{child} cannot be inspected: {exc.strerror}"
            ) from exc
    return tuple(listing)


def _listing_digest(listing: tuple[tuple[str, os.stat_result], ...]) -> str:
    """The identity digest of a directory listing.

    Deliberately built from the deterministic fields only: ``st_atime`` moves
    when the very files being copied are read, so it is not part of any
    directory identity here.
    """
    return _digest(
        [
            [name, stat.S_IFMT(info.st_mode), info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns]
            for name, info in listing
        ]
    )


def _stage_regular_file(
    source_fd: int,
    opened: os.stat_result,
    staging_fd: int,
    staged_name: str,
    *,
    artifact: str,
) -> tuple[str, os.stat_result]:
    """Stream one regular file into the staging area, fsync it, and verify it.

    The source is read in ``IDENTITY_READ_CHUNK`` bounded chunks until exactly
    the size observed on its descriptor has been consumed and hashed, then
    fstat'ed AFTER on the SAME descriptor: a short read, a growth, a truncation
    or any device/inode/size/mtime change is refused instead of being published
    as the whole file. The staged copy is then re-opened through its own pinned
    descriptor and re-digested with the same streaming reader, so a partial or
    mutated copy cannot reach a published name.
    """
    try:
        descriptor = os.open(
            staged_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            BACKUP_FILE_MODE,
            dir_fd=staging_fd,
        )
    except FileExistsError as exc:
        raise _backup_failure("E_BACKUP_COLLISION", f"{artifact} already has a staged copy") from exc
    except OSError as exc:
        raise _backup_failure("E_BACKUP_VERIFY", f"{artifact} cannot be staged: {exc.strerror}") from exc
    try:
        os.fchmod(descriptor, BACKUP_FILE_MODE)
        digest = hashlib.sha256()
        written = 0
        remaining = opened.st_size
        while remaining > 0:
            chunk = os.read(source_fd, min(IDENTITY_READ_CHUNK, remaining))
            if not chunk:
                raise _backup_failure(
                    "E_BACKUP_VERIFY", f"{artifact} returned a short read; the copy is not the whole file"
                )
            digest.update(chunk)
            offset = 0
            while offset < len(chunk):
                offset += os.write(descriptor, chunk[offset:])
            written += len(chunk)
            remaining -= len(chunk)
        if os.read(source_fd, 1):
            raise _backup_failure("E_BACKUP_VERIFY", f"{artifact} grew while it was being copied")
        os.fsync(descriptor)
    except OSError as exc:
        code = "E_INSUFFICIENT_SPACE" if exc.errno == errno.ENOSPC else "E_BACKUP_VERIFY"
        raise _backup_failure(
            code, f"{artifact} could not be copied into the run directory: {exc.strerror}"
        ) from exc
    finally:
        os.close(descriptor)
    after = os.fstat(source_fd)
    if (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise _backup_failure("E_ARTIFACT_IDENTITY_CHANGED", f"{artifact} changed while it was being copied")
    if written != opened.st_size:
        raise _backup_failure("E_BACKUP_VERIFY", f"{artifact} copied {written} of {opened.st_size} bytes")
    staged_digest, staged_stat = _verify_pinned_regular_file(staging_fd, staged_name, artifact=artifact)
    if staged_digest != digest.hexdigest() or staged_stat.st_size != opened.st_size:
        raise _backup_failure("E_BACKUP_VERIFY", f"{artifact} staged copy does not match the source digest")
    # Durability before publication: the staged bytes and their directory entry
    # are on disk before any name can resolve to them.
    os.fsync(staging_fd)
    return staged_digest, staged_stat


def _verify_pinned_regular_file(
    directory_fd: int, name: str, *, artifact: str
) -> tuple[str, os.stat_result]:
    """Re-open a run-owned regular file through its pinned descriptor and verify it.

    The digest is the SAME streaming, size-bounded, looped fd-relative read used
    for every identity in this module; the bounded 64 KiB readers are never used
    for it (routing-matrix residual F7). The entry must be a single-link regular
    file and it must carry the run-owned 0600 mode.
    """
    try:
        descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=directory_fd)
    except OSError as exc:
        raise _backup_failure(
            "E_BACKUP_VERIFY", f"{artifact} cannot be reopened for verification: {exc.strerror}"
        ) from exc
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1:
            raise _backup_failure("E_BACKUP_VERIFY", f"{artifact} is not a single-link regular file")
        digest, refusal = _streamed_digest(descriptor, opened, artifact=artifact)
        if refusal is not None:
            raise _backup_failure("E_BACKUP_VERIFY", f"{artifact} identity unproven: {refusal.message}")
        if stat.S_IMODE(opened.st_mode) != BACKUP_FILE_MODE:
            raise _backup_failure("E_BACKUP_VERIFY", f"{artifact} is not mode 0600")
        return str(digest), opened
    finally:
        os.close(descriptor)


def _publish_staged(
    staging_fd: int, destination_fd: int, staged_name: str, destination_name: str, *, artifact: str
) -> None:
    """Publish a verified staged file: atomic, and never over an existing one.

    ``os.link`` inside one filesystem is the no-overwrite publication DETAIL 10.1
    requires: an existing backup makes it fail with EEXIST instead of being
    silently replaced, and because the published name appears only when the link
    succeeds, no partial file is ever visible under it. The staged name is then
    unlinked, so the published entry is a plain ``nlink == 1`` regular file.
    """
    try:
        os.link(
            staged_name,
            destination_name,
            src_dir_fd=staging_fd,
            dst_dir_fd=destination_fd,
            follow_symlinks=False,
        )
    except FileExistsError as exc:
        raise _backup_failure(
            "E_BACKUP_COLLISION", f"{artifact} is already backed up and is never overwritten"
        ) from exc
    except OSError as exc:
        raise _backup_failure("E_BACKUP_VERIFY", f"{artifact} could not be published: {exc.strerror}") from exc
    os.unlink(staged_name, dir_fd=staging_fd)
    os.fsync(destination_fd)


def _verify_published(
    destination_fd: int, destination_name: str, *, expected_digest: str, artifact: str
) -> None:
    """Re-read a published backup through its pinned descriptor and compare it."""
    digest, _info = _verify_pinned_regular_file(destination_fd, destination_name, artifact=artifact)
    if digest != expected_digest:
        raise _backup_failure("E_BACKUP_VERIFY", f"{artifact} published copy does not match the source digest")


def _absent_backup_entry(label: str, path: str) -> BackupEntry:
    """The explicit absence record DETAIL 10.1 requires instead of a silent skip."""
    return BackupEntry(
        artifact=label,
        source_path=str(path),
        run_relative_path=None,
        kind="absent",
        present=False,
        digest_scope="",
    )


def _legacy_projection_paths(plan: MigrationPlan) -> frozenset[str]:
    """The exact lexical paths of the plan's own legacy projections."""
    return frozenset(identity.lexical_path for identity in plan.legacy_projections)


def _backup_regular_file(
    parent_fd: int,
    name: str,
    before: os.stat_result,
    source_path: Path,
    run_dir: Path,
    run_fd: int,
    run_device: int,
    label: str,
    budget: list[int],
) -> tuple[BackupEntry, ...]:
    """Copy and verify exactly one regular file into the run-owned backup area."""
    _require_same_device(run_device, before.st_dev, artifact=label)
    _take_backup_slot(budget, label)
    published_name = _backup_entry_name(label)
    with _pinned_child_regular_file(parent_fd, name, artifact=label, before=before) as (
        descriptor,
        opened,
    ):
        staged_name = _staged_file_name()
        with _staging_directory(run_dir) as (staging_fd, _staging_path):
            digest, staged_stat = _stage_regular_file(
                descriptor, opened, staging_fd, staged_name, artifact=label
            )
            with _run_child_directory(run_fd, BACKUP_DIRECTORY_NAME) as backup_fd:
                _publish_staged(staging_fd, backup_fd, staged_name, published_name, artifact=label)
                _verify_published(
                    backup_fd, published_name, expected_digest=digest, artifact=label
                )
    return (
        BackupEntry(
            artifact=label,
            source_path=str(source_path),
            run_relative_path=f"{BACKUP_DIRECTORY_NAME}/{published_name}",
            kind="regular_file",
            present=True,
            digest_scope="content",
            sha256=digest,
            size=opened.st_size,
            mode=stat.S_IMODE(opened.st_mode),
            device=opened.st_dev,
            inode=opened.st_ino,
        ),
    )


def _stage_tree(
    source_fd: int,
    staging_fd: int,
    *,
    label: str,
    source_path: Path,
    parts: tuple[str, ...],
    depth: int,
    budget: list[int],
    directories: list[dict[str, Any]],
    files: list[dict[str, Any]],
) -> tuple[tuple[str, os.stat_result], ...]:
    """Stage one directory subtree and return its bounded child listing.

    The walk never follows a link: each child is inspected through the pinned
    descriptor of its parent, and a refusal is raised BEFORE anything is
    published, so the published backup area is untouched by a refused tree (no
    partial mirror, no target swap). The listing is re-read after the walk and
    must be identical, so a directory that changed underneath the walk is
    ``E_ARTIFACT_IDENTITY_CHANGED`` instead of a copy that silently mixes two
    states.

    What this walk does NOT re-check per child is the DEVICE: the equality of the
    run directory's filesystem with the artifact's own is enforced once, for the
    top-level artifact (``_require_same_device`` at the regular-file, tree and
    link branches). A mount point INSIDE a tree is therefore copied across, which
    is harmless here -- the copy is a pinned fd-to-fd read into the run
    directory's staging area, never a rename of the source -- and no rename of a
    source entry is ever attempted. The cross-filesystem STOP of this engine is
    enforced where it can actually lose data: the publication renames
    (``_rename_entry``) refuse ``EXDEV`` on real devices. (S2-06 resolved the
    S2-05 review's F5 by making this docstring state exactly what is enforced
    instead of claiming a per-child check that the code does not perform; a
    per-child device refusal would add a fail-closed branch that cannot be
    exercised on this host without an unprivileged mount.)"""

    if depth > MAX_BACKUP_DEPTH:
        raise _backup_failure("E_BACKUP_VERIFY", f"{label} exceeds the backup depth bound")
    listing = _bounded_child_listing(source_fd, artifact=label)
    for name, info in listing:
        child_parts = (*parts, name)
        child_artifact = f"{label}/{'/'.join(child_parts)}"
        child_source = source_path / name
        if stat.S_ISLNK(info.st_mode):
            raise _backup_failure(
                "E_PATH_FINAL_SYMLINK_UNSAFE",
                f"{child_artifact} is a symlink inside a backed-up directory and is never followed",
            )
        if stat.S_ISDIR(info.st_mode):
            _take_backup_slot(budget, child_artifact)
            with _pinned_child_directory(
                source_fd, name, artifact=child_artifact, before=info
            ) as (child_fd, child_opened):
                descendants = _stage_tree(
                    child_fd,
                    staging_fd,
                    label=label,
                    source_path=child_source,
                    parts=child_parts,
                    depth=depth + 1,
                    budget=budget,
                    directories=directories,
                    files=files,
                )
            directories.append(
                {
                    "parts": child_parts,
                    "artifact": child_artifact,
                    "source_path": str(child_source),
                    "stat": child_opened,
                    "digest": _listing_digest(descendants),
                }
            )
        elif stat.S_ISREG(info.st_mode):
            _take_backup_slot(budget, child_artifact)
            if info.st_nlink != 1:
                raise _backup_failure(
                    "E_PATH_HARDLINK_UNSAFE", f"{child_artifact} is a hard-linked regular file"
                )
            staged_name = _staged_file_name()
            with _pinned_child_regular_file(
                source_fd, name, artifact=child_artifact, before=info
            ) as (descriptor, child_opened):
                digest, staged_stat = _stage_regular_file(
                    descriptor, child_opened, staging_fd, staged_name, artifact=child_artifact
                )
            files.append(
                {
                    "parts": child_parts,
                    "artifact": child_artifact,
                    "staged": staged_name,
                    "digest": digest,
                    "stat": child_opened,
                    "staged_stat": staged_stat,
                }
            )
        else:
            raise _backup_failure(
                "E_PATH_SPECIAL_FILE", f"{child_artifact} is neither a regular file nor a directory"
            )
    if _listing_digest(_bounded_child_listing(source_fd, artifact=label)) != _listing_digest(listing):
        raise _backup_failure("E_ARTIFACT_IDENTITY_CHANGED", f"{label} changed while it was being backed up")
    return listing


def _publish_tree(
    run_fd: int,
    entry_name: str,
    directories: list[dict[str, Any]],
    files: list[dict[str, Any]],
    staging_fd: int,
) -> None:
    """Publish a fully staged subtree with atomic no-overwrite creates."""
    with _run_child_directory(run_fd, BACKUP_DIRECTORY_NAME) as backup_fd:
        with _run_child_directory(backup_fd, entry_name) as root_fd:
            for record in sorted(directories, key=lambda item: len(item["parts"])):
                with _run_directory_chain(root_fd, record["parts"]) as destination_fd:
                    os.fsync(destination_fd)
            for record in files:
                with _run_directory_chain(root_fd, record["parts"][:-1]) as parent_fd:
                    _publish_staged(
                        staging_fd,
                        parent_fd,
                        record["staged"],
                        record["parts"][-1],
                        artifact=record["artifact"],
                    )
                    _verify_published(
                        parent_fd,
                        record["parts"][-1],
                        expected_digest=record["digest"],
                        artifact=record["artifact"],
                    )
            for record in sorted(directories, key=lambda item: -len(item["parts"])):
                with _run_directory_chain(root_fd, record["parts"]) as destination_fd:
                    os.fsync(destination_fd)
            os.fsync(root_fd)


def _backup_directory_tree(
    parent_fd: int,
    name: str,
    before: os.stat_result,
    source_path: Path,
    run_dir: Path,
    run_fd: int,
    run_device: int,
    label: str,
    budget: list[int],
) -> tuple[BackupEntry, ...]:
    """Mirror one directory tree without following any link (DETAIL 10.1)."""
    _require_same_device(run_device, before.st_dev, artifact=label)
    _take_backup_slot(budget, label)
    entry_name = _backup_entry_name(label)
    directories: list[dict[str, Any]] = []
    files: list[dict[str, Any]] = []
    with _pinned_child_directory(parent_fd, name, artifact=label, before=before) as (
        source_fd,
        opened,
    ):
        with _staging_directory(run_dir) as (staging_fd, _staging_path):
            listing = _stage_tree(
                source_fd,
                staging_fd,
                label=label,
                source_path=source_path,
                parts=(),
                depth=1,
                budget=budget,
                directories=directories,
                files=files,
            )
            _publish_tree(run_fd, entry_name, directories, files, staging_fd)
    entries = [
        BackupEntry(
            artifact=label,
            source_path=str(source_path),
            run_relative_path=f"{BACKUP_DIRECTORY_NAME}/{entry_name}",
            kind="directory",
            present=True,
            digest_scope="listing",
            sha256=_listing_digest(listing),
            mode=stat.S_IMODE(opened.st_mode),
            device=opened.st_dev,
            inode=opened.st_ino,
        )
    ]
    entries.extend(
        BackupEntry(
            artifact=record["artifact"],
            source_path=record["source_path"],
            run_relative_path=(
                f"{BACKUP_DIRECTORY_NAME}/{entry_name}/{'/'.join(record['parts'])}"
            ),
            kind="directory",
            present=True,
            digest_scope="listing",
            sha256=record["digest"],
            mode=stat.S_IMODE(record["stat"].st_mode),
            device=record["stat"].st_dev,
            inode=record["stat"].st_ino,
        )
        for record in directories
    )
    entries.extend(
        BackupEntry(
            artifact=record["artifact"],
            source_path=str(source_path / Path(*record["parts"])),
            run_relative_path=(
                f"{BACKUP_DIRECTORY_NAME}/{entry_name}/{'/'.join(record['parts'])}"
            ),
            kind="regular_file",
            present=True,
            digest_scope="content",
            sha256=record["digest"],
            size=record["stat"].st_size,
            mode=stat.S_IMODE(record["stat"].st_mode),
            device=record["stat"].st_dev,
            inode=record["stat"].st_ino,
        )
        for record in files
    )
    return tuple(entries)


def _backup_link_entry(
    parent_fd: int,
    name: str,
    before: os.stat_result,
    source_path: Path,
    run_fd: int,
    run_device: int,
    label: str,
    budget: list[int],
) -> tuple[BackupEntry, ...]:
    """Back up a final legacy symlink as a NEW raw link entry (DETAIL 10.1).

    Only the exact raw ``readlink`` string is read and recreated under the
    run-owned ``link-entries`` directory: the referent is NEVER opened,
    enumerated, hashed, copied, modified or validated as a store. The published
    entry is created with ``symlink`` itself -- an atomic no-overwrite create,
    so an existing entry is ``E_BACKUP_COLLISION`` -- and it is then re-read and
    compared byte for byte with the raw string it must carry.
    """
    _require_same_device(run_device, before.st_dev, artifact=label)
    _take_backup_slot(budget, label)
    entry_name = _backup_entry_name(label)
    try:
        raw_target = os.readlink(name, dir_fd=parent_fd)
    except OSError as exc:
        raise _backup_failure(
            _path_code(exc), f"{label} raw link string cannot be read: {exc.strerror}"
        ) from exc
    with _run_child_directory(run_fd, BACKUP_DIRECTORY_NAME) as backup_fd:
        with _run_child_directory(backup_fd, BACKUP_LINK_ENTRIES_NAME) as link_fd:
            try:
                os.symlink(raw_target, entry_name, dir_fd=link_fd)
            except FileExistsError as exc:
                raise _backup_failure(
                    "E_BACKUP_COLLISION", f"{label} already has a link entry and is never overwritten"
                ) from exc
            except OSError as exc:
                raise _backup_failure(
                    "E_BACKUP_VERIFY", f"{label} link entry cannot be created: {exc.strerror}"
                ) from exc
            os.fsync(link_fd)
            try:
                published = os.readlink(entry_name, dir_fd=link_fd)
            except OSError as exc:
                raise _backup_failure(
                    "E_BACKUP_VERIFY", f"{label} link entry cannot be re-read: {exc.strerror}"
                ) from exc
            if published != raw_target:
                raise _backup_failure(
                    "E_BACKUP_VERIFY", f"{label} link entry does not carry the exact raw link string"
                )
        # S2-06 (S2-05 review F6): the `backup` directory entry this call just
        # created must be durable too, or a crash can persist the symlink while
        # losing `link-entries` entirely. The tree path already double-fsyncs its
        # directories (deeper first, then the root); this is the same order.
        os.fsync(backup_fd)
    return (
        BackupEntry(
            artifact=label,
            source_path=str(source_path),
            run_relative_path=f"{BACKUP_DIRECTORY_NAME}/{BACKUP_LINK_ENTRIES_NAME}/{entry_name}",
            kind="symlink",
            present=True,
            digest_scope="",
            mode=stat.S_IMODE(before.st_mode),
            raw_link_target=raw_target,
        ),
    )


def _backup_path(
    plan: MigrationPlan,
    label: str,
    path: Path,
    run_dir: Path,
    run_fd: int,
    run_device: int,
    budget: list[int],
) -> tuple[BackupEntry, ...]:
    """Back up one artifact path, dispatching on the kind it really is.

    The kind is decided by the filesystem through the pinned no-follow
    descriptor of the path's parent, never by the caller or by the plan: a
    regular file and a directory are copied, an entry that is one of the plan's
    OWN legacy projections is recorded as a raw link entry when it is a symlink,
    an absent entry is recorded explicitly, and every other symlink, special
    file or hard-linked regular file is refused.
    """
    candidate = Path(path)
    legacy_paths = _legacy_projection_paths(plan)
    try:
        with storage_lock.open_directory_nofollow(candidate.parent) as parent_fd:
            try:
                before = os.stat(candidate.name, dir_fd=parent_fd, follow_symlinks=False)
            except FileNotFoundError:
                return (_absent_backup_entry(label, str(candidate)),)
            if stat.S_ISLNK(before.st_mode):
                if str(candidate) not in legacy_paths:
                    raise _backup_failure(
                        "E_PATH_FINAL_SYMLINK_UNSAFE",
                        f"{label} is a symlink that is not one of the plan's legacy projections;"
                        " it is never followed",
                    )
                return _backup_link_entry(
                    parent_fd, candidate.name, before, candidate, run_fd, run_device, label, budget
                )
            if stat.S_ISDIR(before.st_mode):
                return _backup_directory_tree(
                    parent_fd,
                    candidate.name,
                    before,
                    candidate,
                    run_dir,
                    run_fd,
                    run_device,
                    label,
                    budget,
                )
            if stat.S_ISREG(before.st_mode):
                return _backup_regular_file(
                    parent_fd,
                    candidate.name,
                    before,
                    candidate,
                    run_dir,
                    run_fd,
                    run_device,
                    label,
                    budget,
                )
            raise _backup_failure(
                "E_PATH_SPECIAL_FILE", f"{label} is neither a regular file nor a directory"
            )
    except storage_lock.StorageLockError as exc:
        if exc.code == "E_PATH_ABSENT":
            return (_absent_backup_entry(label, str(candidate)),)
        raise _backup_failure(
            exc.code, f"{label} path chain is not a no-follow real path"
        ) from exc


def backup_artifact(
    plan: MigrationPlan, label: str, path: Path, *, run_dir: Path | None = None
) -> tuple[BackupEntry, ...]:
    """Back up exactly one plan artifact into the run-owned backup area.

    ``label`` is the plan's artifact label (``target:graph``, ``target:vector``,
    ``legacy:<index>``) and only names the run-owned destination; the KIND is
    always decided by the filesystem through pinned no-follow descriptors. An
    absent entry is recorded explicitly, and nothing inside the run directory is
    ever overwritten.
    """
    run = _backup_run_directory(plan) if run_dir is None else Path(run_dir)
    _prepare_run_directory(run, plan.request.run_id)
    bounded_label = _bounded_str(label, field_name="backup.label", max_bytes=256)
    budget: list[int] = [0]
    with storage_lock.open_directory_nofollow(run) as run_fd:
        run_device = os.fstat(run_fd).st_dev
        return _backup_path(
            plan, bounded_label, Path(path), run, run_fd, run_device, budget
        )


def graph_lock_creation_record(plan: MigrationPlan, locks: Any = None) -> dict[str, Any]:
    """Record whether THIS run created an absent graph lock, and its exact entry.

    DETAIL 10.1: the held lock inode is never replaced, and a graph lock this
    migration CREATED must be removable after unlock. The identity is therefore
    taken from the LOCK OWNER's own descriptor
    (``MaintenanceStorageLocks.graph_lock_identity``) and never from a fresh path
    walk that a swap could redirect, so post-unlock cleanup
    (``storage_lock.remove_created_lock_entry``) removes exactly what this run
    created. ``created`` False means the entry pre-existed and cleanup must
    leave it in place.
    """
    record: dict[str, Any] = {
        "path": str(Path(plan.layout.graph_lock_path)),
        "run_id": plan.request.run_id,
        "created": False,
        "device": None,
        "inode": None,
        "mode": None,
        "nlink": None,
        "held": False,
    }
    if locks is None:
        return record
    identity = locks.graph_lock_identity
    if identity is None:
        return record
    record.update(
        {
            "created": bool(locks.graph_lock_created),
            "device": identity.st_dev,
            "inode": identity.st_ino,
            "mode": stat.S_IMODE(identity.st_mode),
            "nlink": identity.st_nlink,
            "held": True,
        }
    )
    return record


def _backup_sqlite_record(plan: MigrationPlan, run_dir: Path) -> dict[str, Any]:
    """The SQL source is preserved in place and covered by the S2-03 snapshot.

    DETAIL 10.1: the safety snapshot uses ``sqlite3.Connection.backup``, never a
    file copy, so this stage does NOT copy the database file, its WAL or its
    SHM. When the run already holds the S2-03 snapshot, its identity is recorded
    and the snapshot itself is never rewritten or replaced here.
    """
    snapshot = Path(run_dir) / SNAPSHOT_DIR_NAME / SNAPSHOT_DB_NAME
    record: dict[str, Any] = {
        "path": plan.source_sql.lexical_path,
        "action": plan.sql_action,
        "api": SNAPSHOT_API,
        "snapshot": None,
    }
    if os.path.lexists(snapshot):
        record["snapshot"] = asdict(_inventory(snapshot, artifact="backup:snapshot"))
    return record


def _write_run_report(run_dir: Path, run_fd: int, payload: Mapping[str, Any]) -> str:
    """Durably publish the run's backup report, never over an existing one.

    Staged, fsync'ed, verified and then linked into place, exactly like every
    other run-owned artifact: a pre-existing report is ``E_BACKUP_COLLISION``
    (a run id is never reused), and a failure leaves no report at all.
    """
    blob = _canonical_bytes(payload)
    if len(blob) > MAX_MANIFEST_BYTES:
        raise _backup_failure("E_BACKUP_VERIFY", f"the backup report exceeds {MAX_MANIFEST_BYTES} bytes")
    staged = _staged_file_name()
    try:
        with _staging_directory(run_dir) as (staging_fd, _staging_path):
            descriptor = os.open(
                staged,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                BACKUP_FILE_MODE,
                dir_fd=staging_fd,
            )
            try:
                os.fchmod(descriptor, BACKUP_FILE_MODE)
                written = 0
                while written < len(blob):
                    written += os.write(descriptor, blob[written:])
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            os.fsync(staging_fd)
            digest, _info = _verify_pinned_regular_file(staging_fd, staged, artifact="backup report")
            if digest != hashlib.sha256(blob).hexdigest():
                raise _backup_failure("E_BACKUP_VERIFY", "the backup report does not match what was staged")
            _publish_staged(staging_fd, run_fd, staged, BACKUP_REPORT_NAME, artifact="backup report")
    except OSError as exc:
        code = "E_INSUFFICIENT_SPACE" if exc.errno == errno.ENOSPC else "E_BACKUP_VERIFY"
        raise _backup_failure(code, f"the backup report could not be written: {exc.strerror}") from exc
    return digest


def create_run_backup(plan: MigrationPlan, *, locks: Any = None) -> RunBackup:
    """Back up every regular artifact and final legacy link entry of one plan.

    DETAIL 9.1/10.1, run-owned and immutable:

    * the destination is the plan's own ``<data_root>/.cmms-migrations/<run-id>/``
      (mode 0700, created only here and only once -- a run id that already owns a
      backup report is ``E_BACKUP_COLLISION`` and is never reused);
    * every target the plan inventoried is backed up by KIND: a regular file is
      copied and verified, a directory is mirrored recursively without following
      a link, and a legacy projection is recorded as its exact raw link entry;
    * the SQL source is NOT copied (the S2-03 ``sqlite3.Connection.backup``
      snapshot is the safety copy): only its record is written, and the snapshot
      already present in the run directory is recorded and left untouched;
    * the graph lock is never replaced: the creation record of the lock stage is
      carried into the report so post-unlock cleanup can remove exactly what this
      run created;
    * the whole result is written as one durable, bounded, mode-0600
      ``backup-report.json`` inside the run directory, and nothing in the run
      directory is ever overwritten.
    """
    run_dir = _backup_run_directory(plan)
    _prepare_run_directory(run_dir, plan.request.run_id)
    report_target = run_dir / BACKUP_REPORT_NAME
    if os.path.lexists(report_target):
        raise _backup_failure(
            "E_BACKUP_COLLISION", "this run id already owns a backup report; a run id is never reused"
        )
    budget: list[int] = [0]
    entries: list[BackupEntry] = []
    with storage_lock.open_directory_nofollow(run_dir) as run_fd:
        run_device = os.fstat(run_fd).st_dev
        for label in ("graph", "vector"):
            identity = plan.targets.get(label)
            if identity is None:
                continue
            entries.extend(
                _backup_path(
                    plan,
                    f"target:{label}",
                    Path(identity.lexical_path),
                    run_dir,
                    run_fd,
                    run_device,
                    budget,
                )
            )
        for index, identity in enumerate(plan.legacy_projections):
            entries.extend(
                _backup_path(
                    plan,
                    f"legacy:{index}",
                    Path(identity.lexical_path),
                    run_dir,
                    run_fd,
                    run_device,
                    budget,
                )
            )
        sqlite_record = _backup_sqlite_record(plan, run_dir)
        graph_lock = graph_lock_creation_record(plan, locks)
        coordination = {
            label: asdict(plan.targets[label]) if label in plan.targets else None
            for label in _COORDINATION_TARGET_LABELS
        }
        created_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        payload: dict[str, Any] = {
            "schema_version": BACKUP_REPORT_SCHEMA_VERSION,
            "run_id": _validate_run_id(plan.request.run_id),
            "created_at": created_at,
            "entries": [asdict(entry) for entry in entries],
            "sqlite": sqlite_record,
            "coordination": coordination,
            "graph_lock": dict(graph_lock),
        }
        digest = _digest(payload)
        payload["digest"] = digest
        _write_run_report(run_dir, run_fd, payload)
    return RunBackup(
        run_id=plan.request.run_id,
        run_dir=str(run_dir),
        report_path=str(report_target),
        entries=tuple(entries),
        sqlite=sqlite_record,
        graph_lock=dict(graph_lock),
        digest=digest,
        created_at=created_at,
    )


def acknowledge_embedding_plan(request: MigrationRequest, plan: MigrationPlan) -> None:
    """Require the exact dry-run embedding digest before mutation or resume."""
    if not request.embedding_plan_digest:
        raise ValueError("E_EMBEDDING_PLAN_DIGEST_REQUIRED")
    if request.embedding_plan_digest != plan.embedding.digest:
        raise ValueError("E_EMBEDDING_PLAN_STALE")


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
        acknowledge_embedding_plan(request, plan)
    if plan is not None and plan.blockers:
        raise ValueError(plan.blockers[0].code)


def apply_profile_migration(plan: MigrationPlan, *, embedder: Any = None) -> MigrationManifest:
    """Run the REAL migration pipeline and return the manifest it durably holds.

    S3-06 wired the entrypoint S2-06 kept fail-closed. It independently
    re-validates every maintenance precondition (nothing is inherited from the
    caller's plan beyond the identity the staleness check compares against),
    materializes the run's manifest at `planned`, and then runs

        SQL snapshot -> backup -> staging -> rebuild -> REAL staged verification
        -> publish -> reopen -> `complete`

    through the ONE forward stepper that `resume` also uses, so each checkpoint is
    still earned by its own evidence and the `complete` state is reachable only
    end to end. ``embedder`` is the only injection point and it is optional:
    without it the deployment's own local provider is resolved, and a plan that
    needs the network without ``allow_network_embedding`` is refused rather than
    guessed at.
    """
    preconditions = validate_mutation_preconditions(plan.request, plan=plan)
    path = run_manifest_path(plan)
    _create_run_manifest(plan, preconditions)
    return _run_forward(plan, path, embedder=embedder)


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


def _append_event_to_manifest(
    manifest: MigrationManifest,
    checkpoint: Checkpoint,
    operation: str,
    payload: Mapping[str, Any],
) -> MigrationManifest:
    """Append one event to the hash chain in memory; the caller persists it.

    Sequence number, UTC timestamp, previous-event SHA-256 and the canonical body
    digest are derived here, so two callers (the public append and the forward
    state machine) can never disagree about the chain's shape, and no event is
    ever rewritten in place.
    """
    sequence = len(manifest.events) + 1
    if sequence > MAX_MANIFEST_EVENTS:
        raise _manifest_failure("E_MANIFEST_SCHEMA", f"manifest exceeds {MAX_MANIFEST_EVENTS} events")
    previous_sha256 = manifest.events[-1].digest if manifest.events else _EVENT_GENESIS
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    body = _event_body(sequence, timestamp, previous_sha256, checkpoint, operation, payload)
    appended = ManifestEvent(
        sequence, timestamp, previous_sha256, _digest(body), checkpoint, operation, payload
    )
    return replace(manifest, events=[*manifest.events, appended])


def append_manifest_event(
    path: Path,
    event: Mapping[str, Any],
    *,
    evidence: Iterable[CheckpointEvidence] | None = None,
    capability: StagedVerificationCapability | None = None,
) -> MigrationManifest:
    """Append one event to the append-only hash chain, then persist atomically.

    The new event's sequence, UTC timestamp, previous-event SHA-256 and
    canonical body digest are derived here; the caller supplies only the
    checkpoint, operation and payload. Existing events are never rewritten.

    S2-06: a checkpoint that DETAIL 9.3 gates may not be RECORDED without the
    real prerequisite evidence that justifies it, so the guarantee holds at the
    manifest boundary as well -- the place where a checkpoint actually becomes
    state. A gated append is validated exactly like a forward transition (order,
    evidence, and, for `published`, the run's own recorded publication events),
    and a refusal leaves the materialized manifest byte-identical.
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

    if checkpoint in CAPABILITY_GATED_CHECKPOINTS:
        supplied = [item for item in (evidence or ()) if isinstance(item, CheckpointEvidence)]
        if not supplied:
            raise _manifest_failure(
                "E_CHECKPOINT_PREREQUISITE_MISSING",
                f"{checkpoint} may not be recorded without its {CHECKPOINT_EVIDENCE_CODES[checkpoint]} evidence",
            )
        validate_forward_transition(
            manifest.checkpoint, checkpoint, supplied, capability=capability, manifest=manifest
        )

    updated = _append_event_to_manifest(
        manifest, cast(Checkpoint, checkpoint), operation, payload
    )
    _write_manifest(path, updated)
    return load_manifest(path)


def resume_profile_migration(
    manifest_path: Path, request: MigrationRequest, *, embedder: Any = None
) -> MigrationManifest:
    """Classify a stopped run, then CONTINUE it to `complete` or refuse by cause.

    S2-07 (DETAIL 9.3, 10.5). Resume is an ACTION entrypoint, so it stays
    fail-closed exactly like `apply`: it repeats every precondition the apply path
    repeats -- replan, no-follow identities, config digest, writer inventory, quiet
    interval and the bounded SQLite probe -- then re-confirms the RUN's own durable
    records against the filesystem, classifies the run through the deterministic
    `classify_resume` primitive, and then either continues or REFUSES WITH THE
    CAUSE:

    1. a manifest that cannot be read is refused before anything else
       (``E_MANIFEST_ABSENT``; a broken chain/digest stays fatal in
       ``load_manifest``), and so is a manifest belonging to another run id
       (``E_RESUME_RUN_MISMATCH``);
    2. the source's no-follow identity and the config digest must still be the
       ones the run recorded (``E_RESUME_SOURCE_CHANGED``,
       ``E_RESUME_CONFIG_CHANGED``), and every entry the run's OWN backup report
       records must still verify (``E_RESUME_BACKUP_CHANGED``,
       ``E_RESUME_BACKUP_MISSING``);
    3. at or after ``publishing`` each artifact's target/staging/quarantine triad
       is classified against the run's OWN event history. A contradiction between
       that chain and the disk, a duplicated sequence, or a missing required entry
       is ``E_PUBLICATION_AMBIGUOUS`` / ``E_RESUME_TRIAD_INCOMPLETE``, and a staged
       entry that is no longer the identity the run recorded is
       ``E_RESUME_STAGING_CHANGED`` -- never a guess, never a repair;
    4. when EXACTLY ONE next operation remains, the refusal is that operation's own
       gate: ``E_STAGED_VERIFICATION_CAPABILITY_MISSING`` while the verification
       seam reports no implemented capability. That is the ONLY cause left, because
       S3-06 wired every engine stage; when nothing is blocked the entrypoint
       CONTINUES the run through the forward stepper and returns the manifest.

    S3-06 IS that wiring: the classification is now EXECUTED. When the classifier
    leaves nothing blocked, this entrypoint continues the run through the same
    forward stepper `apply` uses -- including the per-artifact publication events a
    crash left unrecorded, which it executes from the run's OWN durable record with
    the shipped primitives -- and returns the manifest the run durably holds. A
    repeated resume repeats nothing: every step whose checkpoint is already recorded
    is skipped, an artifact whose event sequence is complete is never republished,
    and a failure already recorded for the same step and cause is not recorded
    twice. `classify_resume` remains the public classification and still refuses
    ambiguity, so a run this entrypoint cannot continue is still refused BY CAUSE
    before any mutation.
    """
    outcome = classify_resume(manifest_path, request)
    if outcome.blocked_by:
        raise ValueError(outcome.blocked_by)
    plan = plan_profile_migration(request)
    return _run_forward(plan, Path(manifest_path), embedder=embedder)


# Slice S2-08 moved `rollback_profile_migration` to the S2-08 section at the bottom
# of this module: its restore primitives reuse the S2-05/S2-06/S2-07 helpers defined
# below it. This also supersedes, FOR ROLLBACK ONLY, the sentence further down that
# the public `rollback` stays fail-closed until S3-06: rollback publishes nothing and
# restores a pre-state the run already backed up, so it needs no staged verification
# and does not touch that gate -- while `apply` and `resume` stay fail-closed exactly
# as they are.


# ---------------------------------------------------------------------------
# S2-06 -- the forward state machine and the per-artifact publication
# (DETAIL 9.2, 9.3, 10.3, 10.4; routing-matrix residual F1, which is this
# card's entry condition AND its Stop).
#
# Three promises, and every fail-closed branch below exists to keep one of them:
#
# 1. A CHECKPOINT IS EARNED, NOT ANNOUNCED. The ten forward checkpoints of
#    DETAIL 9.3 advance one step at a time, and each step needs its OWN real
#    prerequisite evidence -- a structured object whose detail is qualified
#    before the step is accepted -- so `backed_up` cannot be claimed without a
#    backup report, `sqlite_snapshotted` without an accepted integrity/revision
#    observation, and so on. The manifest is where a checkpoint becomes state, so
#    the same rule is enforced at the manifest boundary: `append_manifest_event`
#    refuses to RECORD a gated checkpoint without its evidence and leaves the
#    materialized manifest byte-identical.
#
# 2. NO FALSE END-TO-END SUCCESS. `staged_verified`, `published`, `verified` and
#    `complete` may be entered ONLY when the verification seam REPORTS an
#    implemented capability (an explicit flag, or a negative probe the seam
#    refuses) and NEVER because of the seam's return value: a seam that answers
#    `ProjectionVerification(True)` to a staged input that does not exist is
#    exactly why a return value must not be read, and that behaviour is still
#    rejected by the report (S3-06's `test_s306_the_gate_still_refuses_...`). The
#    S0 stub that USED to answer that way is gone: S3-05 landed the real verifier
#    in `memory_server.projection_rebuild`, and S3-06 set that seam's own explicit
#    implemented-capability flag, so the gate is open for a seam that qualifies
#    itself. The card names the last three checkpoints; `staged_verified` is gated
#    here as well because DETAIL 10.3 IS the seam's contract. There is exactly ONE
#    verification implementation in this project
#    (`memory_server.projection_rebuild`); this module only ASKS it.
#
# 3. PER ARTIFACT, NEVER ATOMIC. Publication is per artifact: revalidate the
#    pinned prestate, quarantine the old entry (or record its absence), rename
#    the verified staged entry into the vacant target, fsync BOTH parents, and
#    record `prestate_revalidated`, `prestate_quarantined|absent`,
#    `staging_published`, `parent_fsynced`. `published` additionally requires the
#    run's OWN recorded publication events for both artifacts, so a half-published
#    or crash-interrupted run can never be reported as published, and no claim of
#    a multi-artifact atomic transaction is made anywhere.
#
# While that stub existed the public `apply`/`resume`/`rollback` stayed
# fail-closed. S3-06 then wired the stage: the verifier is real (S3-05), the seam
# reports its capability through its own flag, and `apply`/`resume` run the real
# pipeline below. `rollback` publishes nothing and never consulted this gate at
# all, so it is unaffected. S2-06's ordering was therefore honoured, not bypassed
# (routing-matrix S2-06 `split_further`).
# ---------------------------------------------------------------------------

FORWARD_CHECKPOINTS: tuple[Checkpoint, ...] = (
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

CHECKPOINT_EVIDENCE_CODES: Mapping[Checkpoint, str] = {
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

CAPABILITY_GATED_CHECKPOINTS: frozenset[Checkpoint] = frozenset(
    {"staged_verified", "published", "verified", "complete"}
)

STAGED_VERIFICATION_CAPABILITY_FLAG = "STAGED_VERIFICATION_IMPLEMENTED"
CAPABILITY_BASIS_EXPLICIT_FLAG = "explicit_flag"
CAPABILITY_BASIS_NEGATIVE_PROBE = "negative_probe"
CAPABILITY_BASIS_UNIMPLEMENTED = "unimplemented"
# The negative probe's input: a staged entry that cannot exist. It is only ever
# passed to the seam as an argument -- never created, opened or enumerated.
STAGED_VERIFICATION_PROBE_NAME = "s2-06-unverifiable-staged-entry"

PUBLICATION_ARTIFACTS: tuple[str, ...] = ("vector", "graph")
PUBLICATION_EVENT_REVALIDATED = "prestate_revalidated"
PUBLICATION_EVENT_QUARANTINED = "prestate_quarantined"
PUBLICATION_EVENT_ABSENT = "prestate_absent"
PUBLICATION_EVENT_STAGING_PUBLISHED = "staging_published"
PUBLICATION_EVENT_PARENT_FSYNCED = "parent_fsynced"
PUBLICATION_PUBLISHED_EVENTS: tuple[str, ...] = (
    PUBLICATION_EVENT_REVALIDATED,
    PUBLICATION_EVENT_QUARANTINED,
    PUBLICATION_EVENT_STAGING_PUBLISHED,
    PUBLICATION_EVENT_PARENT_FSYNCED,
)
PUBLICATION_EVENT_CHECKPOINT: Checkpoint = "publishing"
PUBLICATION_EVENT_OPERATION_SEPARATOR = "."
ARTIFACT_STAGING_NAMES: Mapping[str, str] = {"vector": "lancedb", "graph": "graph.json"}
QUARANTINE_DIRECTORY_NAME = "quarantine"
QUARANTINE_PREPUBLISH_NAME = "prepublish"
GRAPH_LOCK_MANIFEST_FIELD = "graph_lock"
# The `prestate_revalidated` payload carries the staged identity THIS run verified,
# by the position-independent key field set (`_identity_key`) and never by
# filename: a caller's identity names the caller's path, while a resume observes
# the entry's own basename, so only a path-free field set can compare the two.
# ADDITIVE: `staged_digest` and every other payload key are unchanged.
PUBLICATION_PAYLOAD_STAGED_IDENTITY = "staged_identity_key"


@dataclass(frozen=True)
class CheckpointEvidence:
    """One checkpoint's real prerequisite evidence, bounded before it is read."""

    checkpoint: Checkpoint
    code: str
    digest: str
    detail: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StagedVerificationCapability:
    """What the verification seam REPORTS about itself (never what it returned).

    ``basis`` is the channel the report came through: ``explicit_flag`` (the
    seam's own implemented-capability flag) or ``negative_probe`` (the seam
    refused an input a real verifier cannot accept). ``implemented`` False means
    the gated checkpoints stay unreachable, whatever the seam's last return value
    happened to be.
    """

    implemented: bool
    basis: str
    detail: str = ""


@dataclass(frozen=True)
class PublicationEvent:
    """One DETAIL 9.3 publishing event of one artifact."""

    artifact: str
    event: str
    detail: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PublicationResult:
    """The outcome of ONE artifact's publication: never an atomic claim."""

    artifact: str
    prestate: str
    quarantine_relative_path: str | None
    published_path: str
    staged_identity: ArtifactIdentity
    published_identity: ArtifactIdentity
    events: tuple[PublicationEvent, ...]
    parents_fsynced: tuple[str, ...]


def forward_checkpoint_index(checkpoint: str) -> int:
    """The position of a forward checkpoint, or ``E_CHECKPOINT_UNKNOWN``."""
    if checkpoint not in FORWARD_CHECKPOINTS:
        raise _manifest_failure("E_CHECKPOINT_UNKNOWN", f"{checkpoint} is not a forward checkpoint")
    return FORWARD_CHECKPOINTS.index(checkpoint)


def _published_sequence_is_complete(events: tuple[str, ...]) -> bool:
    """DETAIL 9.3's per-artifact sequence, with either quarantine or absence."""
    if len(events) != len(PUBLICATION_PUBLISHED_EVENTS):
        return False
    return (
        events[0] == PUBLICATION_EVENT_REVALIDATED
        and events[1] in (PUBLICATION_EVENT_QUARANTINED, PUBLICATION_EVENT_ABSENT)
        and events[2] == PUBLICATION_EVENT_STAGING_PUBLISHED
        and events[3] == PUBLICATION_EVENT_PARENT_FSYNCED
    )


def publication_events_from_manifest(manifest: MigrationManifest) -> dict[str, tuple[str, ...]]:
    """The publication events the RUN ITSELF recorded, per artifact.

    This is the evidence a state machine must read: the caller's claim about a
    publication is not, because the manifest's chain is the run's own record of
    what actually happened, in order.
    """
    collected: dict[str, list[str]] = {label: [] for label in PUBLICATION_ARTIFACTS}
    for event in manifest.events:
        for label in PUBLICATION_ARTIFACTS:
            prefix = label + PUBLICATION_EVENT_OPERATION_SEPARATOR
            if event.operation.startswith(prefix):
                collected[label].append(event.operation[len(prefix) :])
    return {label: tuple(names) for label, names in collected.items()}


def _run_verification_probe(probe: Any, argument: Any) -> Any:
    """Run the seam's coroutine from this synchronous engine, bounded and closed.

    A running loop (an async caller) is honoured by running the probe on its own
    short-lived daemon thread, so the seam's contract does not depend on who
    calls the engine.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(probe(argument))
    results: list[Any] = []
    failures: list[BaseException] = []

    def _runner() -> None:
        try:
            results.append(asyncio.run(probe(argument)))
        except BaseException as exc:  # noqa: BLE001 - re-raised on the caller's thread
            failures.append(exc)

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    thread.join()
    if failures:
        raise failures[0]
    return results[0]


def staged_verification_capability(seam: Any = None) -> StagedVerificationCapability:
    """Report whether the verification seam IMPLEMENTS staged verification.

    DETAIL 10.3 is the seam's contract and the card's F1 clause makes this report
    -- never the seam's return value -- the thing the gate reads. The report has
    two independent channels:

    * the seam's own explicit capability flag, and
    * a negative probe: a real verifier must REFUSE a staged entry that does not
      exist (with the module's fail-closed ``ValueError``, or by reporting
      ``valid=False``). The S0 stub that returned ``ProjectionVerification(True)``
      is gone -- S3-05 landed the real verifier -- and its behaviour survives only
      as the CLASS this report must still reject, which is why a return value is
      never the thing the gate trusts.

    A probe that raises anything else (including a ``TypeError`` from an
    unimplemented call contract) reports NO capability: the gate stays shut
    rather than opening on an answer nobody can qualify.
    """
    module = projection_rebuild if seam is None else seam
    if getattr(module, STAGED_VERIFICATION_CAPABILITY_FLAG, None) is True:
        return StagedVerificationCapability(True, CAPABILITY_BASIS_EXPLICIT_FLAG)
    probe = getattr(module, "verify_staged_projections", None)
    if probe is None:
        return StagedVerificationCapability(
            False, CAPABILITY_BASIS_UNIMPLEMENTED, "the seam exposes no staged verifier at all"
        )
    argument = Path(os.sep) / STAGED_VERIFICATION_PROBE_NAME
    try:
        verdict = _run_verification_probe(probe, argument)
    except ValueError as exc:
        return StagedVerificationCapability(
            True, CAPABILITY_BASIS_NEGATIVE_PROBE, f"the seam refused the probe: {exc}"[:MAX_MANIFEST_STRING_BYTES]
        )
    except BaseException as exc:  # noqa: BLE001 - an unqualifiable answer is not a capability
        return StagedVerificationCapability(
            False,
            CAPABILITY_BASIS_UNIMPLEMENTED,
            f"the probe could not be qualified: {type(exc).__name__}",
        )
    if getattr(verdict, "valid", None) is False:
        return StagedVerificationCapability(
            True, CAPABILITY_BASIS_NEGATIVE_PROBE, "the seam reported the negative probe invalid"
        )
    return StagedVerificationCapability(
        False,
        CAPABILITY_BASIS_UNIMPLEMENTED,
        "the seam accepted a staged entry that does not exist",
    )


_EVIDENCE_REQUIRED_DETAIL: Mapping[str, tuple[str, ...]] = {
    "plan_digest": ("plan_digest", "config_digest"),
    "lock_ownership": ("held", "roots", "identities"),
    "backup_report": ("report_digest", "entries"),
    "snapshot_verification": ("integrity", "revision", "snapshot_device"),
    "rebuild_result": ("completed_batches", "vector_ids_digest", "graph_nodes_digest", "graph_edges_digest"),
    "staged_verification": ("basis", "staging_digest", "artifacts"),
    "publication_plan": ("targets", "pinned"),
    "publication_events": ("artifacts",),
    "reopen_verification": ("artifacts",),
    "manifest_update": ("manifest_digest", "manifest_bytes", "checkpoint"),
}
_EVIDENCE_DIGEST_DETAIL: Mapping[str, tuple[str, ...]] = {
    "plan_digest": ("plan_digest", "config_digest"),
    "backup_report": ("report_digest",),
    "rebuild_result": ("vector_ids_digest", "graph_nodes_digest", "graph_edges_digest"),
    "staged_verification": ("staging_digest",),
    "manifest_update": ("manifest_digest",),
}


def _is_bounded_digest(value: Any) -> bool:
    return isinstance(value, str) and _DIGEST_PATTERN.match(value) is not None


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _evidence_problem(evidence: CheckpointEvidence, target: Checkpoint) -> str:
    """Qualify one evidence object against the checkpoint it claims to justify.

    An empty string means the evidence is usable. Every other return value is a
    reason the transition is refused, and the caller reports it as
    ``E_CHECKPOINT_EVIDENCE_INVALID`` so a caller can never reach a checkpoint
    with a placeholder, a wrong-checkpoint object, or a digest that is not the
    bounded SHA-256 the manifest format uses.
    """
    expected = CHECKPOINT_EVIDENCE_CODES[target]
    if evidence.checkpoint != target:
        return f"{evidence.checkpoint} evidence cannot justify {target}"
    if evidence.code != expected:
        return f"{target} needs {expected} evidence, not {evidence.code}"
    if not _is_bounded_digest(evidence.digest):
        return "the evidence digest is not a bounded SHA-256"
    if not isinstance(evidence.detail, Mapping):
        return "the evidence detail must be a mapping"
    detail = evidence.detail
    for key in _EVIDENCE_REQUIRED_DETAIL[expected]:
        if key not in detail:
            return f"{expected} evidence is missing {key}"
    for key in _EVIDENCE_DIGEST_DETAIL.get(expected, ()):
        if not _is_bounded_digest(detail[key]):
            return f"{expected}.{key} is not a bounded SHA-256"
    if expected == "lock_ownership":
        if detail["held"] is not True:
            return "lock ownership must report a HELD lock"
        roots = detail["roots"]
        if not isinstance(roots, (list, tuple)) or not roots or len(roots) > MAX_MAINTENANCE_ROOTS:
            return "lock ownership must name a bounded set of roots"
        identities = detail["identities"]
        if not isinstance(identities, Mapping) or not identities:
            return "lock ownership must carry the held identities"
    if expected == "snapshot_verification":
        if detail["integrity"] != "ok":
            return "the snapshot integrity_check is not ok"
        if detail["revision"] not in ACCEPTED_SQLITE_SCHEMA_REVISIONS:
            return "the snapshot revision is not accepted"
        if not _is_int(detail["snapshot_device"]):
            return "the snapshot device is not an integer"
    if expected == "backup_report":
        entries = detail["entries"]
        if not _is_int(entries) or entries < 1:
            return "the backup report records no backed-up entries"
    if expected == "rebuild_result":
        if not _is_int(detail["completed_batches"]) or detail["completed_batches"] < 0:
            return "the rebuild result carries no completed-batch count"
    if expected == "staged_verification":
        if not isinstance(detail["basis"], str) or not detail["basis"]:
            return "the staged verification records no capability basis"
        artifacts = detail["artifacts"]
        if not isinstance(artifacts, (list, tuple)) or tuple(artifacts) != PUBLICATION_ARTIFACTS:
            return "the staged verification does not cover both publication artifacts"
    if expected == "publication_plan":
        if not isinstance(detail["targets"], (list, tuple)) or tuple(detail["targets"]) != PUBLICATION_ARTIFACTS:
            return "the publication plan does not cover both publication artifacts"
        pinned = detail["pinned"]
        if not isinstance(pinned, Mapping) or set(pinned) != set(PUBLICATION_ARTIFACTS):
            return "the publication plan does not pin both prestate identities"
    if expected in ("publication_events", "reopen_verification"):
        artifacts = detail["artifacts"]
        if not isinstance(artifacts, Mapping) or set(artifacts) != set(PUBLICATION_ARTIFACTS):
            return f"{expected} must cover both publication artifacts"
        for label in PUBLICATION_ARTIFACTS:
            entry = artifacts[label]
            if expected == "publication_events":
                if not isinstance(entry, (list, tuple)) or not _published_sequence_is_complete(tuple(entry)):
                    return f"{label} has no complete per-artifact publication sequence"
            elif not isinstance(entry, Mapping) or entry.get("matches_staged") is not True:
                return f"{label} was not reopened exactly"
    if expected == "manifest_update":
        if detail["checkpoint"] != target:
            return "the manifest update does not record the checkpoint it justifies"
        if not _is_int(detail["manifest_bytes"]) or detail["manifest_bytes"] < 1:
            return "the manifest update records no durable manifest bytes"
    return ""


def validate_forward_transition(
    current: str,
    target: str,
    evidence: Iterable[CheckpointEvidence] | None = None,
    *,
    capability: StagedVerificationCapability | None = None,
    manifest: MigrationManifest | None = None,
) -> Checkpoint:
    """Validate exactly one forward checkpoint step, or refuse it by cause.

    Refusal codes, in the order they are decided, are stable:
    ``E_CHECKPOINT_UNKNOWN`` (not a checkpoint), ``E_CHECKPOINT_OUT_OF_ORDER``
    (a step is skipped), ``E_STAGED_VERIFICATION_CAPABILITY_MISSING`` (a gated
    checkpoint while the seam reports no implemented capability),
    ``E_CHECKPOINT_PREREQUISITE_MISSING`` (no evidence at all for this
    checkpoint), ``E_CHECKPOINT_EVIDENCE_INVALID`` (evidence that cannot justify
    it) and ``E_PUBLICATION_INCOMPLETE`` (the run's OWN recorded publication
    events do not yet cover both artifacts).
    """
    index = forward_checkpoint_index(target)
    checkpoint = cast(Checkpoint, target)
    if forward_checkpoint_index(current) + 1 != index:
        raise _manifest_failure(
            "E_CHECKPOINT_OUT_OF_ORDER", f"{current} cannot advance directly to {target}"
        )
    if checkpoint in CAPABILITY_GATED_CHECKPOINTS:
        report = capability if capability is not None else staged_verification_capability()
        if not getattr(report, "implemented", False):
            raise _manifest_failure(
                "E_STAGED_VERIFICATION_CAPABILITY_MISSING",
                f"the verification seam reports no implemented capability for {target}",
            )
    supplied = [item for item in (evidence or ()) if isinstance(item, CheckpointEvidence)]
    matching = [
        item
        for item in supplied
        if item.checkpoint == checkpoint and item.code == CHECKPOINT_EVIDENCE_CODES[checkpoint]
    ]
    if not matching:
        if supplied:
            raise _manifest_failure(
                "E_CHECKPOINT_EVIDENCE_INVALID",
                f"none of the supplied evidence justifies {target}",
            )
        raise _manifest_failure(
            "E_CHECKPOINT_PREREQUISITE_MISSING",
            f"{target} has no {CHECKPOINT_EVIDENCE_CODES[checkpoint]} evidence",
        )
    problem = _evidence_problem(matching[0], checkpoint)
    if problem:
        raise _manifest_failure("E_CHECKPOINT_EVIDENCE_INVALID", problem)
    if manifest is not None and checkpoint == "published":
        recorded = publication_events_from_manifest(manifest)
        incomplete = [
            label
            for label in PUBLICATION_ARTIFACTS
            if not _published_sequence_is_complete(recorded.get(label, ()))
        ]
        if incomplete:
            raise _manifest_failure(
                "E_PUBLICATION_INCOMPLETE",
                "published needs both artifacts' own complete publication events; missing: "
                + ", ".join(incomplete),
            )
    return checkpoint


def advance_manifest_checkpoint(
    manifest_path: Path,
    target: str,
    *,
    evidence: Iterable[CheckpointEvidence] | None = None,
    capability: StagedVerificationCapability | None = None,
    graph_lock: Mapping[str, Any] | None = None,
) -> MigrationManifest:
    """Advance the durable manifest by exactly one validated checkpoint.

    The transition is validated against the MANIFEST's own checkpoint and its own
    recorded publication events, the appended event and the optional graph-lock
    creation record are written in ONE atomic manifest update, and the manifest is
    then RE-READ from disk before anything is returned: a claim about a
    checkpoint is a claim about the file, not about an in-memory object.
    """
    path = Path(manifest_path)
    manifest = load_manifest(path)
    checkpoint = validate_forward_transition(
        manifest.checkpoint, target, evidence, capability=capability, manifest=manifest
    )
    supplied = [item for item in (evidence or ()) if isinstance(item, CheckpointEvidence)]
    # R7 (S2-06 review F-1, reproduced by its own probe): the recorded evidence
    # object must be the one the transition QUALIFIED. Filtering by checkpoint
    # alone let a decoy object for the same checkpoint be written into the
    # append-only record as the justification, so the two-key filter of
    # `validate_forward_transition` is reused here verbatim. The object is
    # guaranteed to exist: the transition above already refused without it.
    matching = next(
        item
        for item in supplied
        if item.checkpoint == checkpoint and item.code == CHECKPOINT_EVIDENCE_CODES[checkpoint]
    )
    updated = _append_event_to_manifest(
        manifest,
        checkpoint,
        f"advance{_checkpoint_operation_separator()}{checkpoint}",
        {"evidence": matching.code, "digest": matching.digest},
    )
    updated = replace(
        updated,
        checkpoint=checkpoint,
        completed_steps=[*manifest.completed_steps, checkpoint][:MAX_MANIFEST_ENTRIES],
    )
    if graph_lock is not None:
        updated = replace(
            updated,
            graph_lock=_bounded_mapping(dict(graph_lock), field_name="manifest.graph_lock"),
        )
    _write_manifest(path, updated)
    reopened = load_manifest(path)
    if reopened.checkpoint != checkpoint:
        raise _manifest_failure(
            "E_MANIFEST_UPDATE_UNPROVEN", f"the durable manifest does not record {checkpoint}"
        )
    return reopened


def _checkpoint_operation_separator() -> str:
    """The one separator used by both checkpoint and publication event names."""
    return PUBLICATION_EVENT_OPERATION_SEPARATOR


def record_graph_lock_creation(manifest_path: Path, record: Mapping[str, Any]) -> MigrationManifest:
    """Durably record a graph lock this run CREATED (DETAIL 10.1).

    The record is the lock OWNER's own identity (``device``/``inode`` of the
    descriptor the lock stage holds), so post-unlock cleanup removes exactly what
    this run created. Written through the same atomic single-update path as a
    checkpoint and re-read from disk afterwards.
    """
    path = Path(manifest_path)
    manifest = load_manifest(path)
    bounded = _bounded_mapping(dict(record), field_name="manifest.graph_lock")
    if not bounded.get("path") or not isinstance(bounded.get("created"), bool):
        raise _manifest_failure(
            "E_MANIFEST_GRAPH_LOCK_INVALID",
            "the graph-lock record must carry a path and a boolean created flag",
        )
    updated = replace(manifest, graph_lock=bounded)
    _write_manifest(path, updated)
    reopened = load_manifest(path)
    if reopened.graph_lock != bounded:
        raise _manifest_failure(
            "E_MANIFEST_UPDATE_UNPROVEN", "the durable manifest does not carry the graph-lock record"
        )
    return reopened


def _publication_failure(code: str, detail: str = "") -> ValueError:
    return _manifest_failure(code, detail)


def _observed_entry_identity(parent_fd: int, name: str, *, artifact: str) -> ArtifactIdentity:
    """The no-follow identity of one entry through its PINNED parent descriptor.

    A regular file is digested through a pinned ``O_NOFOLLOW`` descriptor with the
    S2-02 streaming reader (fstat before and after on the SAME descriptor); a
    directory, a symlink and a special file are recorded by their own metadata,
    and a symlink is recorded by its exact RAW link string. Nothing here follows a
    link, and an entry that is not there is ``absent`` rather than an error the
    caller has to interpret.
    """
    try:
        info = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return ArtifactIdentity(str(name), "absent")
    if stat.S_ISLNK(info.st_mode):
        try:
            raw = os.readlink(name, dir_fd=parent_fd)
        except OSError as exc:
            raise _publication_failure(_path_code(exc), f"{artifact} raw link string cannot be read") from exc
        return ArtifactIdentity(
            str(name), "symlink", info.st_dev, info.st_ino, info.st_mode, None, None, None, raw
        )
    if stat.S_ISDIR(info.st_mode):
        return ArtifactIdentity(str(name), "directory", info.st_dev, info.st_ino, info.st_mode)
    if stat.S_ISREG(info.st_mode):
        if info.st_nlink != 1:
            raise _publication_failure("E_PATH_HARDLINK_UNSAFE", f"{artifact} is a hard-linked regular file")
        try:
            descriptor = os.open(
                name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK, dir_fd=parent_fd
            )
        except OSError as exc:
            raise _publication_failure(_path_code(exc), f"{artifact} cannot be opened no-follow") from exc
        try:
            opened = os.fstat(descriptor)
            digest, refusal = _streamed_digest(descriptor, opened, artifact=artifact)
        finally:
            os.close(descriptor)
        if refusal is not None:
            raise _publication_failure(refusal.code, refusal.message)
        return ArtifactIdentity(
            str(name),
            "regular_file",
            opened.st_dev,
            opened.st_ino,
            opened.st_mode,
            opened.st_size,
            opened.st_mtime_ns,
            digest,
        )
    return ArtifactIdentity(str(name), "special", info.st_dev, info.st_ino, info.st_mode)


def _require_prestate_unchanged(pinned: ArtifactIdentity, observed: ArtifactIdentity, *, artifact: str) -> None:
    """DETAIL 10.4 step 1: the exact prestate must still be the pinned one."""
    if pinned.kind != observed.kind:
        raise _publication_failure(
            "E_PUBLICATION_PRESTATE_CHANGED",
            f"{artifact} was pinned as {pinned.kind} but is now {observed.kind}",
        )
    if pinned.kind == "symlink" and pinned.raw_link_target != observed.raw_link_target:
        raise _publication_failure(
            "E_PUBLICATION_PRESTATE_CHANGED", f"{artifact} raw link string changed since it was pinned"
        )
    if _identity_key(pinned) != _identity_key(observed):
        raise _publication_failure(
            "E_PUBLICATION_PRESTATE_CHANGED", f"{artifact} identity changed since it was pinned"
        )


def _rename_entry(
    source_fd: int, source_name: str, destination_fd: int, destination_name: str, *, artifact: str
) -> None:
    """One atomic same-filesystem rename through PINNED descriptors.

    ``EXDEV`` is the cross-filesystem STOP of this card: it is refused with its
    own stable code BEFORE anything is unlinked, and it is produced by the kernel
    on real devices rather than predicted from metadata. Any other failure is
    ``E_PUBLICATION_RENAME_FAILED``, and the entry stays where it was.

    R6 (S2-06 review, this card's residual): the SOURCE parent is fsynced as soon
    as the rename landed. Without it a crash can persist the destination entry
    while the source entry's disappearance is lost, leaving a duplicate staging
    entry behind for the next attempt to trip over.
    """
    try:
        os.rename(source_name, destination_name, src_dir_fd=source_fd, dst_dir_fd=destination_fd)
        os.fsync(source_fd)
    except OSError as exc:
        if exc.errno == errno.EXDEV:
            raise _publication_failure(
                "E_CROSS_FILESYSTEM_PUBLICATION",
                f"{artifact} cannot be renamed across filesystems (EXDEV)",
            ) from exc
        raise _publication_failure(
            "E_PUBLICATION_RENAME_FAILED", f"{artifact} rename failed: {exc.strerror}"
        ) from exc


def _open_quarantine_directory(run_fd: int) -> int:
    """Open (creating at 0700) the run-owned ``quarantine/prepublish`` directory."""
    with _run_child_directory(run_fd, QUARANTINE_DIRECTORY_NAME) as quarantine_fd:
        with _run_child_directory(quarantine_fd, QUARANTINE_PREPUBLISH_NAME) as prepublish_fd:
            return os.dup(prepublish_fd)


def _quarantine_entry_name(artifact: str, run_id: str) -> str:
    """A bounded, run-owned quarantine entry name that cannot collide silently."""
    return _backup_entry_name(f"{artifact}-{_validate_run_id(run_id)}")


def _record_publication_event(
    manifest_path: Path | None, artifact: str, event: str, detail: Mapping[str, Any]
) -> None:
    """Append one publishing event to the run's own durable event chain."""
    if manifest_path is None:
        return
    append_manifest_event(
        Path(manifest_path),
        {
            "checkpoint": PUBLICATION_EVENT_CHECKPOINT,
            "operation": f"{artifact}{PUBLICATION_EVENT_OPERATION_SEPARATOR}{event}",
            "payload": dict(detail),
        },
    )


def publish_artifact(
    plan: MigrationPlan,
    artifact: str,
    *,
    staged_identity: ArtifactIdentity,
    run_dir: Path | None = None,
    manifest_path: Path | None = None,
) -> PublicationResult:
    """Publish ONE verified staged artifact into its vacant target (DETAIL 10.4).

    Steps, in order, with the event each one records:

    1. revalidate the target parent and the EXACT pinned prestate no-follow
       (``prestate_revalidated``) -- a stale or replaced prestate is refused here,
       before anything moves;
    2. an absent prestate is recorded explicitly (``prestate_absent``); otherwise
       the entry -- a regular file, a directory, or the final symlink itself, which
       is NEVER followed -- is renamed into the run-owned
       ``quarantine/prepublish`` area (``prestate_quarantined``) and BOTH parents
       are fsynced;
    3. the verified staged entry is revalidated against the caller's pinned
       ``staged_identity`` and renamed into the now-vacant final name
       (``staging_published``) with the same-filesystem only ``os.rename``;
    4. both parents are fsynced again and the sequence closes with
       ``parent_fsynced``.

    Nothing is copied, no path is traversed and no event is recorded that the run
    did not actually perform: the result carries THIS artifact's own identity
    proof, and no multi-artifact atomicity is claimed anywhere.
    """
    label = _bounded_str(artifact, field_name="publication.artifact", max_bytes=32)
    if label not in PUBLICATION_ARTIFACTS:
        raise _publication_failure("E_PUBLICATION_ARTIFACT_UNKNOWN", f"{label} is not a publication artifact")
    pinned = plan.targets.get(label)
    if pinned is None:
        raise _publication_failure("E_PUBLICATION_TARGET_MISSING", f"the plan records no {label} target")
    run = _backup_run_directory(plan) if run_dir is None else Path(run_dir)
    _prepare_run_directory(run, plan.request.run_id)
    if manifest_path is not None:
        _require_staged_verified_reached(Path(manifest_path), artifact=label)
    target = Path(pinned.lexical_path)
    quarantine_parent = run / QUARANTINE_DIRECTORY_NAME / QUARANTINE_PREPUBLISH_NAME
    events: list[PublicationEvent] = []
    quarantined_relative: str | None = None
    prestate = "absent"
    parents_fsynced: tuple[str, ...] = (str(target.parent),)

    with storage_lock.open_directory_nofollow(run) as run_fd:
        with storage_lock.open_directory_nofollow(target.parent) as target_parent_fd:
            observed = _observed_entry_identity(target_parent_fd, target.name, artifact=label)
            _require_prestate_unchanged(pinned, observed, artifact=label)
            revalidated = {
                "pinned_kind": pinned.kind,
                "observed_kind": observed.kind,
                "pinned_digest": _digest(asdict(pinned)),
                "staged_digest": _digest(asdict(staged_identity)),
                # ADDED (S2-07 fix round 1, review F-1): the staged identity's
                # position-independent key field set, so a resume can compare the
                # entry it OBSERVES with the entry THIS run verified. `staged_digest`
                # stays as it is -- it digests the caller's identity object, whose
                # `lexical_path` is the caller's path, so it can never be compared
                # with an observed identity's whole-object digest.
                PUBLICATION_PAYLOAD_STAGED_IDENTITY: {
                    "fields": list(_IDENTITY_KEY_FIELDS),
                    "values": list(_identity_key(staged_identity)),
                },
            }
            events.append(PublicationEvent(label, PUBLICATION_EVENT_REVALIDATED, revalidated))
            _record_publication_event(manifest_path, label, PUBLICATION_EVENT_REVALIDATED, revalidated)

            quarantine_fd: int | None = None
            try:
                if pinned.kind == "absent":
                    absent_detail = {"prestate": "absent"}
                    events.append(PublicationEvent(label, PUBLICATION_EVENT_ABSENT, absent_detail))
                    _record_publication_event(
                        manifest_path, label, PUBLICATION_EVENT_ABSENT, absent_detail
                    )
                else:
                    quarantine_fd = _open_quarantine_directory(run_fd)
                    entry_name = _quarantine_entry_name(label, plan.request.run_id)
                    try:
                        os.stat(entry_name, dir_fd=quarantine_fd, follow_symlinks=False)
                    except FileNotFoundError:
                        pass
                    else:
                        raise _publication_failure(
                            "E_PUBLICATION_COLLISION",
                            f"{label} already owns a quarantine entry and it is never overwritten",
                        )
                    _rename_entry(
                        target_parent_fd, target.name, quarantine_fd, entry_name, artifact=label
                    )
                    _fsync_pair(target_parent_fd, quarantine_fd)
                    prestate = "quarantined"
                    quarantined_relative = (
                        f"{QUARANTINE_DIRECTORY_NAME}/{QUARANTINE_PREPUBLISH_NAME}/{entry_name}"
                    )
                    quarantined_detail = {
                        "quarantine": quarantined_relative,
                        "kind": pinned.kind,
                        "identity": _digest(asdict(pinned)),
                    }
                    events.append(
                        PublicationEvent(label, PUBLICATION_EVENT_QUARANTINED, quarantined_detail)
                    )
                    _record_publication_event(
                        manifest_path, label, PUBLICATION_EVENT_QUARANTINED, quarantined_detail
                    )
                    parents_fsynced = (str(target.parent), str(quarantine_parent))

                vacant = _observed_entry_identity(target_parent_fd, target.name, artifact=label)
                if vacant.kind != "absent":
                    raise _publication_failure(
                        "E_PUBLICATION_PRESTATE_CHANGED",
                        f"{label} target name is not vacant after the quarantine step",
                    )

                with _run_child_directory(run_fd, BACKUP_STAGING_NAME) as staging_fd:
                    staged_name = ARTIFACT_STAGING_NAMES[label]
                    staged = _observed_entry_identity(staging_fd, staged_name, artifact=label)
                    if staged.kind == "absent":
                        raise _publication_failure(
                            "E_STAGING_ENTRY_ABSENT", f"{label} has no staged entry to publish"
                        )
                    if _identity_key(staged) != _identity_key(staged_identity):
                        raise _publication_failure(
                            "E_STAGING_IDENTITY_CHANGED",
                            f"{label} staged entry is not the identity that was verified",
                        )
                    _rename_entry(staging_fd, staged_name, target_parent_fd, target.name, artifact=label)
                    os.fsync(target_parent_fd)
                published_detail = {
                    "identity": _digest(asdict(staged_identity)),
                    "path_present": True,
                }
                events.append(
                    PublicationEvent(label, PUBLICATION_EVENT_STAGING_PUBLISHED, published_detail)
                )
                _record_publication_event(
                    manifest_path, label, PUBLICATION_EVENT_STAGING_PUBLISHED, published_detail
                )
                if quarantine_fd is not None:
                    _fsync_pair(target_parent_fd, quarantine_fd)
                synced_detail = {"parents": list(parents_fsynced)}
                events.append(PublicationEvent(label, PUBLICATION_EVENT_PARENT_FSYNCED, synced_detail))
                _record_publication_event(
                    manifest_path, label, PUBLICATION_EVENT_PARENT_FSYNCED, synced_detail
                )
            finally:
                if quarantine_fd is not None:
                    os.close(quarantine_fd)

            published_identity = _observed_entry_identity(
                target_parent_fd, target.name, artifact=label
            )

    return PublicationResult(
        artifact=label,
        prestate=prestate,
        quarantine_relative_path=quarantined_relative,
        published_path=str(target),
        staged_identity=staged_identity,
        published_identity=published_identity,
        events=tuple(events),
        parents_fsynced=parents_fsynced,
    )


def _require_staged_verified_reached(manifest_path: Path, *, artifact: str) -> None:
    """R4: a publication that belongs to a RUN may follow only an EARNED `staged_verified`.

    The primitive stays callable as a component (the S2-06 nodes drive it that
    way, without a manifest), but the moment a run's own manifest is supplied the
    entry must be part of a run whose durable record reached `staged_verified` --
    the capability-gated checkpoint that is the only licence DETAIL 9.3 gives for
    `publishing`. Before S3-06 the guard was unnecessary because nothing was wired;
    now it is the difference between a verified swap and an unverified one.
    """
    manifest = load_manifest(Path(manifest_path))
    if forward_checkpoint_index(manifest.checkpoint) < forward_checkpoint_index("staged_verified"):
        raise _publication_failure(
            E_PUBLICATION_WITHOUT_STAGED_VERIFICATION,
            f"{artifact} may not be published from checkpoint `{manifest.checkpoint}`:"
            " `staged_verified` was never earned for this run",
        )


def _fsync_pair(first_fd: int, second_fd: int) -> None:
    """Fsync both parents of a rename (DETAIL 10.4 step 5), source first."""
    os.fsync(first_fd)
    os.fsync(second_fd)


def reopen_published_artifact(
    plan: MigrationPlan, artifact: str, result: PublicationResult
) -> dict[str, Any]:
    """Reopen the published entry and compare it with the pinned staged identity.

    DETAIL 9.3: `published` may only be followed by `verified` after both
    artifacts are "present and reopenable". The comparison is exact and
    no-follow: the publication was a RENAME, so the entry at the final name must
    be the very inode that was staged, with the staged mode, size, mtime and --
    for a regular file -- the same streamed SHA-256. A replacement planted after
    the swap therefore cannot be recorded as verified.
    """
    label = _bounded_str(artifact, field_name="publication.artifact", max_bytes=32)
    if label not in PUBLICATION_ARTIFACTS:
        raise _publication_failure("E_PUBLICATION_ARTIFACT_UNKNOWN", f"{label} is not a publication artifact")
    target = Path(plan.targets[label].lexical_path)
    with storage_lock.open_directory_nofollow(target.parent) as parent_fd:
        observed = _observed_entry_identity(parent_fd, target.name, artifact=label)
    if _identity_key(result.staged_identity) != _identity_key(observed):
        raise _publication_failure(
            "E_PUBLICATION_REOPEN_MISMATCH",
            f"{label} at the final name is not the staged entry that was published",
        )
    return {
        "artifact": label,
        "kind": observed.kind,
        "device": observed.device,
        "inode": observed.inode,
        "mode": observed.mode,
        "size": observed.size,
        "mtime_ns": observed.mtime_ns,
        "digest": observed.sha256 or "",
        "matches_staged": True,
    }


# ---------------------------------------------------------------------------
# S2-07 -- the deterministic resume classifier
# (DETAIL 9.3's forward checkpoints and per-artifact publication events,
# DETAIL 10.5's resume rules) and the two carry-ins the S2-06 review assigned to
# this card: R3 (the crash states the classifier must classify, and what it may
# honestly claim about them) and R7/F-1 (the durable record could name an
# UNQUALIFIED evidence object -- fixed at `advance_manifest_checkpoint`).
#
# Three promises, and every fail-closed branch below exists to keep one of them:
#
# 1. REPEAT, THEN DECIDE. Nothing is inherited from the caller. `resume` repeats
#    the whole DETAIL 6.3 precondition set and re-confirms the RUN's own durable
#    records -- run id, source identity, config digest, backup entries -- against
#    the live filesystem before a single decision is made.
# 2. ONE OPERATION, OR NONE. Each artifact's target/staging/quarantine triad is
#    classified against the run's OWN event history: the recorded events must be a
#    PREFIX of that artifact's DETAIL 9.3 sequence and the disk must agree with
#    that prefix, and then the next event is the single unambiguous next
#    operation. A contradiction between the chain and the disk, a duplicated
#    sequence, or a missing required entry is refused with a stable code, never
#    guessed and never repaired.
# 3. IT NAMES, IT DOES NOT EXECUTE. The classifier reports what the run is
#    licensed to continue with; `blocked_by` names the gate that keeps that
#    operation from being executed HERE. Executing it is S3-06's wiring, because
#    publication requires evidence that `staged_verified` was reached and
#    `verify_staged_projections` is still the S0 stub (S2-06 review R1/R4).
#    Because this path performs no mutation at all -- no swap, no repair, no
#    deletion -- a repeated resume repeats no verified swap.
#
# Honest limits of this slice (disclosed, not worked around):
# * `resume` does NOT execute `next_operations`, so it cannot by itself finish a
#   half-published run; it reports the exact operation that would, and the cause
#   that keeps it shut.
# * A manifest at or after `publishing` is currently only reachable from a
#   test or a future (S3) run, because `staged_verified` is capability-gated and
#   the seam is a stub. The classifier is therefore exercised on REAL crash
#   states produced by the shipped publication primitive.
# * `manifest.target_identities_before` is the durable home of the pinned
#   prestate. Nothing in this project populates it yet, so the classifier falls
#   back to the live plan while the run's own record has not decided the
#   sequence, and says which source it used (`pin_source`).
# ---------------------------------------------------------------------------

RESUME_PHASE_PREPUBLICATION = "prepublication"
RESUME_PHASE_PUBLISHING = "publishing"
RESUME_PHASE_POSTPUBLICATION = "postpublication"

RESUME_TRIAD_PRESTATE_INTACT = "prestate_intact"
RESUME_TRIAD_PRESTATE_QUARANTINED = "prestate_quarantined"
RESUME_TRIAD_PRESTATE_ABSENT = "prestate_absent"
RESUME_TRIAD_STAGING_PUBLISHED = "staging_published"
RESUME_TRIAD_COMPLETE = "complete"

RESUME_BACKUP_VERIFIED = "verified"
RESUME_BACKUP_ABSENT = "absent"
RESUME_BACKUP_NOT_REQUIRED = "not_required"

RESUME_PIN_SOURCE_RECORD = "record"
RESUME_PIN_SOURCE_PLAN = "plan"

RESUME_TOLERATED_RUN_PATH_COLLISION = "E_BACKUP_COLLISION"
_PUBLICATION_SEQUENCE_QUARANTINED: tuple[str, ...] = (
    PUBLICATION_EVENT_REVALIDATED,
    PUBLICATION_EVENT_QUARANTINED,
    PUBLICATION_EVENT_STAGING_PUBLISHED,
    PUBLICATION_EVENT_PARENT_FSYNCED,
)
_PUBLICATION_SEQUENCE_ABSENT: tuple[str, ...] = (
    PUBLICATION_EVENT_REVALIDATED,
    PUBLICATION_EVENT_ABSENT,
    PUBLICATION_EVENT_STAGING_PUBLISHED,
    PUBLICATION_EVENT_PARENT_FSYNCED,
)
_PUBLISHING_CHECKPOINT_INDEX = FORWARD_CHECKPOINTS.index(PUBLICATION_EVENT_CHECKPOINT)


@dataclass(frozen=True)
class ArtifactTriad:
    """One artifact's target/staging/quarantine triad, classified.

    ``recorded_events`` is what the RUN ITSELF recorded for this artifact, in
    order; ``state`` is the DETAIL 9.3 position that both the record and the disk
    agree on; ``next_event`` is the single next publication event, or None when
    this artifact's sequence is already complete. ``detail`` carries the facts the
    decision was made from (kinds, which source pinned the prestate kind, and
    whether the published entry could be bound to the recorded staged identity).
    """

    artifact: str
    recorded_events: tuple[str, ...]
    state: str
    next_event: str | None
    target_kind: str
    quarantine_present: bool
    staging_present: bool
    detail: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ResumeOutcome:
    """What a resume DECIDED. Never a claim that the migration finished.

    ``blocked_by`` is the stable code that keeps ``next_operations`` from being
    executed by this slice (empty only when nothing remains at all);
    ``tolerated_blockers`` names the replan diagnostics a resumed run is EXPECTED
    to produce, so the tolerance is auditable instead of silent.
    """

    phase: str
    checkpoint: str
    next_checkpoint: str | None
    triads: tuple[ArtifactTriad, ...]
    next_operations: tuple[str, ...]
    blocked_by: str
    backup_state: str
    tolerated_blockers: tuple[str, ...]
    manifest: MigrationManifest


def _load_resume_manifest(manifest_path: Path) -> MigrationManifest:
    """The manifest a resume may act on, or a stable refusal.

    A missing manifest is ``E_MANIFEST_ABSENT``; every malformed, unbounded,
    escaping, tampered or chain-broken document keeps the fatal code
    ``load_manifest`` already raises.
    """
    try:
        return load_manifest(Path(manifest_path))
    except FileNotFoundError as exc:
        raise ValueError("E_MANIFEST_ABSENT") from exc


def _require_resume_source_identity(plan: MigrationPlan, manifest: MigrationManifest) -> None:
    """The source must still be the identity the run recorded (DETAIL 10.5)."""
    if _identity_key(plan.source_sql) != _identity_key(manifest.source_identity):
        raise ValueError("E_RESUME_SOURCE_CHANGED")


def _require_resume_config_digest(plan: MigrationPlan, manifest: MigrationManifest) -> None:
    """The config digest must still be the one the run recorded (DETAIL 10.5)."""
    if plan.config_digest != manifest.config_digest:
        raise ValueError("E_RESUME_CONFIG_CHANGED")


def _require_resume_plan(plan: MigrationPlan) -> tuple[str, ...]:
    """A replan of a RESUMED run is expected to collide with its own run path.

    ``E_BACKUP_COLLISION`` -- "a run-owned path for this run id already exists" --
    is the definition of a resumed run, so it is tolerated and REPORTED; every
    other blocker still refuses the resume with its own stable code.
    """
    tolerated = tuple(
        sorted(
            f"{item.code}:{item.artifact}"
            for item in plan.blockers
            if item.code == RESUME_TOLERATED_RUN_PATH_COLLISION
        )
    )
    blocking = [
        item.code for item in plan.blockers if item.code != RESUME_TOLERATED_RUN_PATH_COLLISION
    ]
    if blocking:
        raise ValueError(blocking[0])
    return tolerated


def _resume_backup_state(run_dir: Path, manifest: MigrationManifest) -> str:
    """Confirm the run's OWN backup report and every entry it records.

    DETAIL 10.5 repeats "backup hashes" on resume. The report is the run's own
    durable record of what was copied, so it is re-read and its recorded digest,
    run id and per-entry size/digest are re-checked against the run directory. A
    rotted or replaced backup copy is ``E_RESUME_BACKUP_CHANGED``; a run that has
    passed ``backed_up`` with no report at all is ``E_RESUME_BACKUP_MISSING``.
    """
    required = forward_checkpoint_index(manifest.checkpoint) >= FORWARD_CHECKPOINTS.index(
        "backed_up"
    )
    report_path = Path(run_dir) / BACKUP_REPORT_NAME
    if not os.path.lexists(report_path):
        if required:
            raise ValueError("E_RESUME_BACKUP_MISSING")
        return RESUME_BACKUP_NOT_REQUIRED
    try:
        payload = json.loads(_read_bounded_manifest_bytes(report_path).decode("utf-8"))
        if not isinstance(payload, Mapping):
            raise ValueError("E_RESUME_BACKUP_CHANGED")
        recorded_digest = payload.get("digest")
        body = {key: value for key, value in payload.items() if key != "digest"}
        if not _is_bounded_digest(recorded_digest) or _digest(body) != recorded_digest:
            raise ValueError("E_RESUME_BACKUP_CHANGED")
        if payload.get("run_id") != manifest.run_id:
            raise ValueError("E_RESUME_BACKUP_CHANGED")
        entries = payload.get("entries")
        if not isinstance(entries, (list, tuple)) or len(entries) > MAX_BACKUP_ENTRIES:
            raise ValueError("E_RESUME_BACKUP_CHANGED")
        for entry in entries:
            if not isinstance(entry, Mapping):
                raise ValueError("E_RESUME_BACKUP_CHANGED")
            if entry.get("present") is not True:
                continue
            relative = _validate_run_relative_path(
                entry.get("run_relative_path"), field_name="backup entry.run_relative_path"
            )
            destination = Path(run_dir) / relative
            kind = entry.get("kind")
            if kind == "regular_file":
                # The shared, no-follow, fstat-before-and-after streaming reader is
                # the ONE copy verifier in this project; it never raises, it
                # degrades to a sha256-less identity, and that is a refusal here.
                identity = _digest_regular_path(
                    destination, artifact=f"backup:{destination.name}", notes=[], digest=True
                )
                if (
                    identity.kind != "regular_file"
                    or identity.size != entry.get("size")
                    or identity.sha256 != entry.get("sha256")
                ):
                    raise ValueError("E_RESUME_BACKUP_CHANGED")
            elif kind == "directory":
                if not stat.S_ISDIR(os.lstat(destination).st_mode):
                    raise ValueError("E_RESUME_BACKUP_CHANGED")
            elif kind == "symlink":
                if os.readlink(destination) != entry.get("raw_link_target"):
                    raise ValueError("E_RESUME_BACKUP_CHANGED")
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        if isinstance(exc, ValueError) and str(exc) == "E_RESUME_BACKUP_CHANGED":
            raise
        raise ValueError("E_RESUME_BACKUP_CHANGED") from exc
    return RESUME_BACKUP_VERIFIED


def _resume_entry_identity(parent: Path, name: str, *, artifact: str) -> ArtifactIdentity:
    """The no-follow identity of one triad entry; ``absent`` when it is not there.

    A missing parent chain is NOT an error here: it is exactly how an unstarted
    artifact looks, and the classifier decides what that means.
    """
    try:
        with storage_lock.open_directory_nofollow(parent) as parent_fd:
            return _observed_entry_identity(parent_fd, name, artifact=artifact)
    except storage_lock.StorageLockError as exc:
        if exc.code == "E_PATH_ABSENT":
            return ArtifactIdentity(str(name), "absent")
        raise _publication_failure(
            exc.code, f"{artifact} path chain is not a no-follow real path"
        ) from exc


def _publication_records(
    manifest: MigrationManifest, label: str
) -> tuple[tuple[str, ...], tuple[Mapping[str, Any], ...]]:
    """One artifact's recorded publication events: names AND payloads, in order."""
    prefix = label + PUBLICATION_EVENT_OPERATION_SEPARATOR
    names: list[str] = []
    payloads: list[Mapping[str, Any]] = []
    for event in manifest.events:
        if event.operation.startswith(prefix):
            names.append(event.operation[len(prefix) :])
            payloads.append(event.payload)
    return tuple(names), tuple(payloads)


def _recorded_staging_identity(payload: Mapping[str, Any]) -> tuple[Any, ...] | None:
    """The staged identity the run recorded at ``prestate_revalidated``, or None.

    The record is honoured only when it names EXACTLY the module's own
    position-independent key field set (``_IDENTITY_KEY_FIELDS``); anything else
    carries nothing comparable and is reported as such instead of being guessed at.
    No lexical path takes part on either side of the comparison.
    """
    recorded = payload.get(PUBLICATION_PAYLOAD_STAGED_IDENTITY)
    if not isinstance(recorded, Mapping):
        return None
    if list(recorded.get("fields", ())) != list(_IDENTITY_KEY_FIELDS):
        return None
    values = recorded.get("values")
    if not isinstance(values, (list, tuple)) or len(values) != len(_IDENTITY_KEY_FIELDS):
        return None
    return tuple(values)


def _require_resume_staged_identity(
    payload: Mapping[str, Any], observed_staging: ArtifactIdentity, *, artifact: str
) -> str:
    """DETAIL 10.5's STAGED class: the run's own record against the DISK.

    ``E_RESUME_SOURCE_CHANGED``, ``E_RESUME_CONFIG_CHANGED`` and every backed-up
    entry are confirmed against the run's own durable record; this is the same
    confirmation for the staged entry that the next recorded operation would
    publish, and it is the ONE reference a resume can trust: at resume time the
    caller can only reconstruct the staged identity by observing the same entry,
    so its own identity is self-referential.

    Returns where the confirmed identity came from -- ``record`` when the run's
    record carried the key, ``unrecorded`` when it did not -- so the caller can
    REPORT what could not be confirmed instead of implying that it was.
    """
    recorded = _recorded_staging_identity(payload)
    if recorded is None:
        return "unrecorded"
    if _identity_key(observed_staging) != recorded:
        raise _publication_failure(
            "E_RESUME_STAGING_CHANGED",
            f"{artifact}'s staged entry is not the identity the run recorded when it "
            "revalidated its prestate",
        )
    return "record"


def _classify_artifact(
    manifest: MigrationManifest,
    plan: MigrationPlan,
    run_dir: Path,
    label: str,
    names: tuple[str, ...],
    payloads: tuple[Mapping[str, Any], ...],
) -> ArtifactTriad:
    """Classify ONE artifact's triad against the run's own event history."""
    quarantined_recorded = PUBLICATION_EVENT_QUARANTINED in names
    absent_recorded = PUBLICATION_EVENT_ABSENT in names
    pinned = plan.targets.get(label)
    if pinned is None:
        raise _publication_failure("E_PUBLICATION_TARGET_MISSING", f"the plan records no {label} target")
    if quarantined_recorded:
        sequence = _PUBLICATION_SEQUENCE_QUARANTINED
        pin_source = RESUME_PIN_SOURCE_RECORD
    elif absent_recorded:
        sequence = _PUBLICATION_SEQUENCE_ABSENT
        pin_source = RESUME_PIN_SOURCE_RECORD
    else:
        # No sequence decision is recorded yet, so the live pin is still the
        # prestate and it decides which sequence this artifact is on.
        sequence = (
            _PUBLICATION_SEQUENCE_ABSENT
            if pinned.kind == "absent"
            else _PUBLICATION_SEQUENCE_QUARANTINED
        )
        pin_source = RESUME_PIN_SOURCE_PLAN

    if len(names) > len(sequence) or names != sequence[: len(names)]:
        raise _publication_failure(
            "E_PUBLICATION_AMBIGUOUS",
            f"{label} recorded {list(names)} for its own {list(sequence)} sequence; "
            "the events cannot be ordered into exactly one next operation",
        )

    position = len(names)
    next_event = sequence[position] if position < len(sequence) else None
    target = Path(pinned.lexical_path)
    observed_target = _resume_entry_identity(target.parent, target.name, artifact=label)
    staging_entry = Path(run_dir) / BACKUP_STAGING_NAME / ARTIFACT_STAGING_NAMES[label]
    observed_staging = _resume_entry_identity(
        staging_entry.parent, staging_entry.name, artifact=label
    )
    quarantine_entry = (
        Path(run_dir)
        / QUARANTINE_DIRECTORY_NAME
        / QUARANTINE_PREPUBLISH_NAME
        / _quarantine_entry_name(label, manifest.run_id)
    )
    observed_quarantine = _resume_entry_identity(
        quarantine_entry.parent, quarantine_entry.name, artifact=label
    )
    quarantine_present = observed_quarantine.kind != "absent"
    staging_present = observed_staging.kind != "absent"
    detail: dict[str, Any] = {
        "sequence_position": position,
        "pin_source": pin_source,
        "target_kind": observed_target.kind,
        "staging_kind": observed_staging.kind,
        "quarantine_kind": observed_quarantine.kind,
        "next_event": next_event,
    }

    if position <= 1:
        if not staging_present:
            raise _manifest_failure(
                "E_RESUME_TRIAD_INCOMPLETE",
                f"{label} has no staged entry to publish, which its own record requires",
            )
        # position == 0 is the ONE arm with nothing to compare against: the run has
        # recorded no `prestate_revalidated` yet, so it holds no staged identity at
        # all and this class cannot be confirmed here. That arm's behaviour is left
        # unchanged ON PURPOSE (review F-1; the same limitation class as F-7).
        if position >= 1:
            detail["staged_identity_source"] = _require_resume_staged_identity(
                payloads[0], observed_staging, artifact=label
            )
        if quarantine_present:
            raise _publication_failure(
                "E_PUBLICATION_AMBIGUOUS",
                f"{label} already owns a quarantine entry while its own record records no "
                "quarantine step",
            )
        if position == 1:
            recorded_pinned_kind = payloads[0].get("pinned_kind")
            if recorded_pinned_kind != observed_target.kind:
                raise _publication_failure(
                    "E_PUBLICATION_AMBIGUOUS",
                    f"{label} was revalidated as {recorded_pinned_kind!r} but the target is "
                    f"{observed_target.kind!r}",
                )
        state = RESUME_TRIAD_PRESTATE_INTACT

    elif position == 2:
        if observed_target.kind != "absent":
            raise _publication_failure(
                "E_PUBLICATION_AMBIGUOUS",
                f"{label}'s own record says its prestate was quarantined but the target name is "
                f"not vacant (it holds {observed_target.kind!r}); two candidates are possible",
            )
        if not staging_present:
            raise _manifest_failure(
                "E_RESUME_TRIAD_INCOMPLETE",
                f"{label} has no staged entry to publish, which its own record requires",
            )
        detail["staged_identity_source"] = _require_resume_staged_identity(
            payloads[0], observed_staging, artifact=label
        )
        if quarantined_recorded:
            if not quarantine_present:
                raise _manifest_failure(
                    "E_RESUME_TRIAD_INCOMPLETE",
                    f"{label}'s own record says its prestate was quarantined but the quarantine "
                    "entry is missing",
                )
            pinned_digest = payloads[0].get("pinned_digest")
            quarantined_digest = payloads[1].get("identity")
            if (
                _is_bounded_digest(pinned_digest)
                and _is_bounded_digest(quarantined_digest)
                and pinned_digest != quarantined_digest
            ):
                raise _publication_failure(
                    "E_PUBLICATION_AMBIGUOUS",
                    f"{label}'s own record pins one prestate and quarantines another",
                )
            # RENAMED (S2-07 fix round 1, review F-2): this is a pure
            # WELL-FORMEDNESS check of the quarantined digest, True for any
            # syntactically valid digest. It used to be published as
            # `digest_bound`, which named a BINDING comparison that lives further
            # down this function; the name now says what it is.
            detail["quarantine_digest_bounded"] = _is_bounded_digest(quarantined_digest)
            state = RESUME_TRIAD_PRESTATE_QUARANTINED
        else:
            if quarantine_present:
                raise _publication_failure(
                    "E_PUBLICATION_AMBIGUOUS",
                    f"{label} owns a quarantine entry although its own record records an absent "
                    "prestate",
                )
            state = RESUME_TRIAD_PRESTATE_ABSENT

    else:
        if observed_target.kind == "absent":
            raise _publication_failure(
                "E_PUBLICATION_AMBIGUOUS",
                f"{label}'s own record says the staged entry was published but the target name "
                "is vacant",
            )
        if staging_present:
            raise _publication_failure(
                "E_PUBLICATION_AMBIGUOUS",
                f"{label} still has a staged entry AND an entry at the target name; two "
                "candidates are possible",
            )
        if quarantined_recorded:
            if not quarantine_present:
                raise _manifest_failure(
                    "E_RESUME_TRIAD_INCOMPLETE",
                    f"{label}'s retained prestate copy is missing from the quarantine area",
                )
            if _identity_key(observed_target) == _identity_key(observed_quarantine):
                raise _publication_failure(
                    "E_PUBLICATION_AMBIGUOUS",
                    f"{label}'s quarantined prestate is sitting at the target name",
                )
        elif quarantine_present:
            raise _publication_failure(
                "E_PUBLICATION_AMBIGUOUS",
                f"{label} owns a quarantine entry although its own record records an absent "
                "prestate",
            )
        recorded_staged_digest = payloads[0].get("staged_digest")
        published_digest = payloads[position - 1].get("identity")
        if (
            _is_bounded_digest(recorded_staged_digest)
            and _is_bounded_digest(published_digest)
        ):
            detail["staged_record_consistent"] = recorded_staged_digest == published_digest
        # The run's own record digests the identity object ITS CALLER passed to
        # `publish_artifact`, while an observed identity carries the entry's own
        # basename. Where the two agree the binding is exact; where they do not,
        # that is REPORTED instead of being mistaken for a refusal. `digest_bound`
        # is ONLY this binding comparison -- the quarantined digest's
        # well-formedness check is reported separately, as
        # `quarantine_digest_bounded` (S2-07 fix round 1, review F-2).
        detail["digest_bound"] = _digest(asdict(observed_target)) == published_digest
        if position == len(sequence):
            state = RESUME_TRIAD_COMPLETE
            if not _published_sequence_is_complete(names):
                raise _publication_failure(
                    "E_PUBLICATION_AMBIGUOUS",
                    f"{label}'s recorded sequence is not complete by the state machine's own "
                    "predicate",
                )
        else:
            state = RESUME_TRIAD_STAGING_PUBLISHED

    return ArtifactTriad(
        artifact=label,
        recorded_events=names,
        state=state,
        next_event=next_event,
        target_kind=observed_target.kind,
        quarantine_present=quarantine_present,
        staging_present=staging_present,
        detail=detail,
    )


def classify_publication_triads(
    manifest: MigrationManifest, plan: MigrationPlan, *, run_dir: Path
) -> tuple[ArtifactTriad, ...]:
    """Classify EVERY publication artifact's target/staging/quarantine triad.

    Deterministic and read-only: the same manifest, plan and run directory always
    yield the same classification, and nothing on disk is modified or deleted.
    Each artifact's recorded events are read through the shipped
    ``publication_events_from_manifest`` view and independently re-derived from
    the same chain, so the two can never disagree silently. A refusal is a stable
    ``E_PUBLICATION_AMBIGUOUS`` (not single-valued), ``E_RESUME_TRIAD_INCOMPLETE``
    (a required entry is missing) or ``E_RESUME_STAGING_CHANGED`` (the staged entry
    is no longer the identity the run itself recorded) -- there is no "best guess"
    branch.
    """
    recorded: dict[str, tuple[str, ...]] = {}
    payloads: dict[str, tuple[Mapping[str, Any], ...]] = {}
    claimed = publication_events_from_manifest(manifest)
    for label in PUBLICATION_ARTIFACTS:
        names, item_payloads = _publication_records(manifest, label)
        if names != claimed[label]:
            raise _publication_failure(
                "E_PUBLICATION_AMBIGUOUS", f"the run's own views of {label}'s events disagree"
            )
        recorded[label] = names
        payloads[label] = item_payloads
    return tuple(
        _classify_artifact(
            manifest, plan, Path(run_dir), label, recorded[label], payloads[label]
        )
        for label in PUBLICATION_ARTIFACTS
    )


def _resume_blocked_by(next_checkpoint: str | None, next_operations: tuple[str, ...]) -> str:
    """The stable code that keeps the ONE next operation from being executed here.

    Empty only when nothing remains. S3-06 wired every engine stage, so the ONLY
    cause that can still keep an operation from being executed is the verification
    capability itself: a capability-gated checkpoint and any publication operation
    both need the seam to REPORT an implemented capability, and while it does not
    the gate stays shut and says so. The former "an engine stage this slice has not
    wired" branch is GONE -- the stages are wired, and the branch is replaced by
    the real transition rather than by a weaker refusal.
    """
    if next_checkpoint is None and not next_operations:
        return ""
    gated = next_checkpoint is not None and next_checkpoint in CAPABILITY_GATED_CHECKPOINTS
    if (gated or next_operations) and not staged_verification_capability().implemented:
        return "E_STAGED_VERIFICATION_CAPABILITY_MISSING"
    return ""


# ---------------------------------------------------------------------------
# S3-05 -- the engine ASKS the one verifier (DETAIL 10.3, DETAIL.md:637-645;
# DETAIL 19.10, DETAIL.md:1077; addendum PART A).
#
# There is exactly ONE staged/published verification implementation in this
# project and it lives in `memory_server.projection_rebuild`. This section adds
# no second one: it resolves the artifact paths the run owns, CALLS the seam, and
# turns the seam's verdict into the S2-06 `CheckpointEvidence` objects the
# forward checkpoints already require (`staged_verification`, `reopen_verification`
# -- see `_EVIDENCE_REQUIRED_DETAIL`). No store is opened here; nothing below
# reads a table, a snapshot or a graph, and nothing below can substitute a
# verdict of its own.
#
# Layout hand-off (S3-04 review residual R-S304-a): the staged artifacts live in
# the run's own staging directory under the very names the S2-06 publication code
# already uses -- `BACKUP_STAGING_NAME` + `ARTIFACT_STAGING_NAMES`, i.e. DETAIL
# 9.1's `<run>/staging/{lancedb,graph.json}`. The published artifacts are the
# run's PINNED targets (`plan.targets[label].lexical_path`), which is exactly
# where DETAIL 10.4 renames the verified staged entry to. No new run-directory
# scheme and no new staging name is introduced, so the engine's own
# staged-identity revalidation and this hand-off cannot disagree.
# ---------------------------------------------------------------------------

STAGED_VERIFICATION_REFUSED = "E_STAGED_VERIFICATION_REFUSED"
PUBLISHED_REOPEN_MISMATCH = "E_PUBLISHED_REOPEN_MISMATCH"
# The provider's default embedding dimension (`lancedb_provider.DEFAULT_VECTOR_SIZE`,
# 384). Repeated as a literal so the engine keeps no provider import of its own --
# the dimension stays a CALLER expectation, and the seam reports the store's own
# dimension back so a mismatch is refused rather than assumed.
DEFAULT_PROJECTION_VECTOR_SIZE = 384


def staged_projection_paths(run_dir: Path) -> dict[str, Path]:
    """The DETAIL 9.1 staged entries of one run, from the engine's own names."""
    staging = Path(run_dir) / BACKUP_STAGING_NAME
    return {label: staging / ARTIFACT_STAGING_NAMES[label] for label in PUBLICATION_ARTIFACTS}


def published_projection_paths(plan: MigrationPlan) -> dict[str, Path]:
    """The published entries of one run: its pinned per-artifact targets."""
    return {label: Path(plan.targets[label].lexical_path) for label in PUBLICATION_ARTIFACTS}


def snapshot_url_for(path: Path) -> str:
    """The canonical snapshot URL the seam re-reads the expectation from."""
    return f"sqlite+aiosqlite:///{Path(path).absolute()}"


def _verdict_reason(verdict: Any) -> str:
    errors = tuple(getattr(verdict, "errors", ()))
    return "; ".join(errors) if errors else "the verification seam refused without a cause"


def staged_projection_verification(
    snapshot_path: Path,
    run_dir: Path,
    *,
    expected_vector_size: int = DEFAULT_PROJECTION_VECTOR_SIZE,
    batch_size: int = 32,
) -> Any:
    """CALL the one verifier on this run's staged entries.

    ``expected_vector_size`` is the run's configured expectation, not something
    the store attests: the seam compares the reopened schema against it and
    reports the store's own dimension back.
    """
    paths = staged_projection_paths(run_dir)
    return _run_verification_probe(
        lambda _ignored: projection_rebuild.verify_staged_projections(
            snapshot_url=snapshot_url_for(snapshot_path),
            staging_vector_path=paths["vector"],
            staging_graph_path=paths["graph"],
            expected_vector_size=expected_vector_size,
            batch_size=batch_size,
        ),
        None,
    )


def published_reopen_verdict(
    snapshot_path: Path,
    plan: MigrationPlan,
    *,
    expected_vector_size: int = DEFAULT_PROJECTION_VECTOR_SIZE,
    batch_size: int = 32,
) -> Any:
    """CALL the one verifier on this run's PUBLISHED entries (before `complete`)."""
    paths = published_projection_paths(plan)
    return _run_verification_probe(
        lambda _ignored: projection_rebuild.verify_published_projections(
            snapshot_url=snapshot_url_for(snapshot_path),
            published_vector_path=paths["vector"],
            published_graph_path=paths["graph"],
            expected_vector_size=expected_vector_size,
            batch_size=batch_size,
        ),
        None,
    )


def staged_verification_evidence(
    snapshot_path: Path,
    run_dir: Path,
    *,
    expected_vector_size: int = DEFAULT_PROJECTION_VECTOR_SIZE,
    batch_size: int = 32,
) -> CheckpointEvidence:
    """The `staged_verified` checkpoint's evidence, built from the seam's verdict.

    A refused staged verification raises instead of producing evidence, so the
    checkpoint cannot be recorded for an artifact the seam refused.
    """
    verdict = staged_projection_verification(
        snapshot_path, run_dir, expected_vector_size=expected_vector_size, batch_size=batch_size
    )
    return _staged_evidence(verdict)


def _artifact_identity(verdict: Any, label: str) -> tuple[Any, ...]:
    """One artifact's verified identity, comparable across staged and published."""
    if label == "vector":
        return (tuple(getattr(verdict, "vector_ids", ())), getattr(verdict, "vector_ids_digest", ""))
    return (getattr(verdict, "graph_nodes_digest", ""), getattr(verdict, "graph_edges_digest", ""))


def reopen_verification_evidence(
    snapshot_path: Path,
    plan: MigrationPlan,
    staged_verdict: Any = None,
    *,
    expected_vector_size: int = DEFAULT_PROJECTION_VECTOR_SIZE,
    batch_size: int = 32,
) -> CheckpointEvidence:
    """The `verified` checkpoint's evidence: the reopened publication, per artifact.

    Every artifact must be reopened AND must match the identity it is compared
    against; otherwise this refuses with ``E_PUBLISHED_REOPEN_MISMATCH`` and
    `complete` stays out of reach. The reference the comparison used is NAMED in
    the evidence (`reference`), because a resume at or after `publishing` no
    longer holds the staged verdict in memory -- the staged entries were renamed
    away by the publication, so re-running staged verification is impossible:

    * ``staged_verdict`` -- the strict two-verdict comparison, used whenever the
      run computed the staged verdict in the same call (the `apply` path);
    * ``snapshot_expectation`` -- the seam's OWN expectation, re-derived from the
      SAME canonical snapshot the staged verification used. The published entry is
      the very entry the publication renamed (which `publish_artifact` revalidated
      against the run's recorded staged identity before the rename) and the seam
      compares it against that independently derived expectation, so the two
      verdicts are equal BY CONSTRUCTION rather than by assumption. The
      alternative -- trusting the run's own recorded digests as the reference --
      is exactly the false success this module refuses.
    """
    verdict = published_reopen_verdict(
        snapshot_path, plan, expected_vector_size=expected_vector_size, batch_size=batch_size
    )
    reopened = bool(getattr(verdict, "valid", False))
    reference = "staged_verdict" if staged_verdict is not None else "snapshot_expectation"
    artifacts: dict[str, dict[str, Any]] = {}
    for label in PUBLICATION_ARTIFACTS:
        if staged_verdict is not None:
            matches = reopened and _artifact_identity(verdict, label) == _artifact_identity(
                staged_verdict, label
            )
        else:
            matches = reopened
        artifacts[label] = {
            "matches_staged": bool(matches),
            "digest": _digest(_artifact_identity(verdict, label)),
            "reference": reference,
        }
    if not all(entry["matches_staged"] for entry in artifacts.values()):
        raise _manifest_failure(
            PUBLISHED_REOPEN_MISMATCH,
            f"the reopened publication does not match the verified staged identity: {_verdict_reason(verdict)}",
        )
    detail = {"artifacts": artifacts}
    return CheckpointEvidence("verified", "reopen_verification", _digest(detail), detail)


def classify_resume(manifest_path: Path, request: MigrationRequest) -> ResumeOutcome:
    """Repeat every precondition, confirm every durable identity, then classify.

    This is the DETERMINISTIC CLASSIFIER itself, and it is public so the decision
    can be inspected without an action entrypoint's refusal in the way: it repeats
    the whole DETAIL 6.3 precondition set, re-confirms the run's own durable
    records, and returns what a resume would DECIDE. It mutates nothing, and it
    refuses ambiguity rather than resolving it:

    * ``E_MANIFEST_ABSENT`` / ``E_RESUME_RUN_MISMATCH`` -- no manifest for this run;
    * ``E_RESUME_SOURCE_CHANGED`` / ``E_RESUME_CONFIG_CHANGED`` /
      ``E_RESUME_BACKUP_CHANGED`` / ``E_RESUME_BACKUP_MISSING`` -- a duplicated
      precondition moved under the run;
    * ``E_RESUME_STAGING_CHANGED`` -- the staged entry the next recorded operation
      would publish is no longer the identity the run recorded when it revalidated
      its prestate (confirmed only where the run HAS such a record);
    * ``E_PUBLICATION_AMBIGUOUS`` / ``E_RESUME_TRIAD_INCOMPLETE`` -- the triad is
      not single-valued, or an entry the run's own record requires is missing.

    ``blocked_by`` is the stable code that keeps ``next_operations`` from being
    executed by this slice; it is empty only when nothing remains at all.
    """
    validate_mutation_preconditions(request)
    manifest = _load_resume_manifest(manifest_path)
    if manifest.run_id != request.run_id:
        raise ValueError("E_RESUME_RUN_MISMATCH")
    plan = plan_profile_migration(request)
    _require_resume_source_identity(plan, manifest)
    _require_resume_config_digest(plan, manifest)
    tolerated = _require_resume_plan(plan)
    run_dir = _backup_run_directory(plan)
    backup_state = _resume_backup_state(run_dir, manifest)

    index = forward_checkpoint_index(manifest.checkpoint)
    if index < _PUBLISHING_CHECKPOINT_INDEX:
        next_checkpoint = FORWARD_CHECKPOINTS[index + 1]
        return ResumeOutcome(
            phase=RESUME_PHASE_PREPUBLICATION,
            checkpoint=manifest.checkpoint,
            next_checkpoint=next_checkpoint,
            triads=(),
            next_operations=(),
            blocked_by=_resume_blocked_by(next_checkpoint, ()),
            backup_state=backup_state,
            tolerated_blockers=tolerated,
            manifest=manifest,
        )

    triads = classify_publication_triads(manifest, plan, run_dir=run_dir)
    next_operations = tuple(
        f"{triad.artifact}{PUBLICATION_EVENT_OPERATION_SEPARATOR}{triad.next_event}"
        for triad in triads
        if triad.next_event is not None
    )
    next_checkpoint = (
        FORWARD_CHECKPOINTS[index + 1] if index + 1 < len(FORWARD_CHECKPOINTS) else None
    )
    return ResumeOutcome(
        phase=(
            RESUME_PHASE_PUBLISHING
            if index == _PUBLISHING_CHECKPOINT_INDEX
            else RESUME_PHASE_POSTPUBLICATION
        ),
        checkpoint=manifest.checkpoint,
        next_checkpoint=next_checkpoint,
        triads=triads,
        next_operations=next_operations,
        blocked_by=_resume_blocked_by(next_checkpoint, next_operations),
        backup_state=backup_state,
        tolerated_blockers=tolerated,
        manifest=manifest,
    )


# ---------------------------------------------------------------------------
# S3-06 -- the engine wired END TO END
# (DETAIL 9.3's forward state machine, DETAIL 10.1-10.6, DETAIL 14.3; the
# routing-matrix card `id: S3-06`).
#
# Every fail-closed branch this card removes is replaced by a REAL, evidenced
# transition -- never by a weaker refusal, never by a fabricated verdict:
#
# 1. THE PIPELINE IS REAL. `apply` runs SQL snapshot -> backup -> staging ->
#    rebuild -> REAL verification -> publish -> reopen -> `complete`. The snapshot
#    comes from the S2-03 qualifier, the projections from the S3-04 rebuild, the
#    verdicts from the ONE verifier in `memory_server.projection_rebuild` (this
#    module only ASKS it -- there is no second implementation here) and the swap
#    from the S2-06 publication primitive. Each step is still gated by the S2-06
#    evidence requirement, so a checkpoint is EARNED.
# 2. THE GATE IS OPENED BY A QUALIFIED CHANNEL. S3-05 landed the real verifier and
#    the seam now carries its own explicit implemented-capability flag; that flag
#    is what `staged_verification_capability` reports, never a return value.
# 3. NOTHING IS INVENTED. A failure retains the last completed checkpoint, sets
#    `status=failed` and records the step, the stable code and the exception class
#    (DETAIL 9.3). No digest is synthesized anywhere on a refusal path, and a
#    refusal reported for the same step and cause twice is not recorded twice.
# 4. RESUME CONTINUES. `resume` EXECUTES the classification `classify_resume`
#    derived -- including the per-artifact publication events a crash left
#    unrecorded -- and refuses only where the classifier itself refuses.
#
# `rollback` is untouched by this section: it publishes nothing, so it needs no
# staged verification and does not consult this gate.
# ---------------------------------------------------------------------------

DEFAULT_PROJECTION_BATCH_SIZE = 32
E_EMBEDDER_UNAVAILABLE = "E_EMBEDDER_UNAVAILABLE"
E_NETWORK_EMBEDDING_REQUIRED = "E_NETWORK_EMBEDDING_REQUIRED"
E_MIGRATION_STAGE_FAILED = "E_MIGRATION_STAGE_FAILED"
E_RUN_ALREADY_EXISTS = "E_RUN_ALREADY_EXISTS"
E_PUBLICATION_WITHOUT_STAGED_VERIFICATION = "E_PUBLICATION_WITHOUT_STAGED_VERIFICATION"
# A remote vector backend (or a plan that declares network embedding) may not be
# used unless the operator opted in; the concrete remote provider is config
# activation, which acceptance 4 keeps OUT of this engine.
NETWORK_VECTOR_BACKENDS: frozenset[str] = frozenset({"remote", "qdrant"})


def run_manifest_path(plan: MigrationPlan) -> Path:
    """The run's own manifest: ``<data_root>/.cmms-migrations/<run-id>/manifest.json``."""
    return _backup_run_directory(plan) / MANIFEST_FILE_NAME


def resolve_migration_embedder(
    plan: MigrationPlan, *, embedder: Any = None, request: MigrationRequest | None = None
) -> Any:
    """The embedder the rebuild may use -- resolved or refused, never invented.

    A caller-supplied embedder must really embed. Otherwise the deployment's LOCAL
    provider is constructed (its model loads lazily, so construction is offline and
    side-effect free) and a plan that asks for a remote vector backend or declares
    network embedding is refused unless the request opted in with
    ``allow_network_embedding``. No mock, stub or fabricated vector source is ever
    returned; if no embedder can be obtained the rebuild refuses by itself.
    """
    if embedder is not None:
        if not callable(getattr(embedder, "embed_batch", None)):
            raise ValueError(E_EMBEDDER_UNAVAILABLE)
        return embedder
    asking = plan.request if request is None else request
    needs_network = bool(getattr(plan.embedding, "network", False)) or str(
        getattr(plan.embedding, "backend", "")
    ) in NETWORK_VECTOR_BACKENDS
    if needs_network and not asking.allow_network_embedding:
        raise ValueError(E_NETWORK_EMBEDDING_REQUIRED)
    from memory_server.providers.embedding_provider import SentenceTransformerEmbeddingProvider

    resolved = SentenceTransformerEmbeddingProvider()
    if not callable(getattr(resolved, "embed_batch", None)):
        raise ValueError(E_EMBEDDER_UNAVAILABLE)
    return resolved


def _run_blocking(factory: Callable[[], Any]) -> Any:
    """Run one coroutine factory from this synchronous engine, bounded and closed."""
    return _run_verification_probe(lambda _ignored: factory(), None)


def _planned_evidence(
    plan: MigrationPlan, preconditions: MutationPreconditions
) -> CheckpointEvidence:
    detail = {
        "plan_digest": preconditions.replan_digest,
        "config_digest": plan.config_digest,
    }
    return CheckpointEvidence("planned", "plan_digest", _digest(detail), detail)


def _lock_ownership_evidence(plan: MigrationPlan, locks: Any) -> CheckpointEvidence:
    identities: dict[str, Any] = {}
    for root in getattr(locks, "roots", ()):
        try:
            info = os.lstat(root)
        except OSError as exc:
            raise _manifest_failure("E_LOCK_OWNERSHIP_UNPROVEN", f"locked root {root} vanished") from exc
        identities[str(root)] = [info.st_dev, info.st_ino]
    detail: dict[str, Any] = {
        "held": bool(getattr(locks, "roots", ())) and locks is not None,
        "roots": [str(root) for root in getattr(locks, "roots", ())],
        "identities": identities,
    }
    return CheckpointEvidence("locked", "lock_ownership", _digest(detail), detail)


def _backup_evidence(backup: Any) -> CheckpointEvidence:
    detail = {"report_digest": str(backup.digest), "entries": len(tuple(backup.entries))}
    return CheckpointEvidence("backed_up", "backup_report", _digest(detail), detail)


def _snapshot_report(plan: MigrationPlan, run_dir: Path) -> Mapping[str, Any]:
    """The run-owned SQL safety snapshot, created once and reused idempotently.

    DETAIL 10.5: a completed idempotent step continues. A snapshot a previous
    attempt already materialized is therefore VERIFIED and reused rather than
    refused as a collision, and one that does not verify stops the run.
    """
    existing = Path(run_dir) / SNAPSHOT_DIR_NAME / SNAPSHOT_DB_NAME
    if os.path.lexists(existing):
        verification, diagnostics = verify_snapshot(existing)
        if diagnostics:
            raise _manifest_failure(diagnostics[0].code, diagnostics[0].message)
        return {"path": str(existing), "verification": verification}
    report, diagnostics = qualify_sqlite_source(
        Path(plan.source_sql.lexical_path), run_dir=Path(run_dir)
    )
    if diagnostics:
        raise _manifest_failure(diagnostics[0].code, diagnostics[0].message)
    snapshot = report.get("snapshot")
    if not isinstance(snapshot, Mapping):
        raise _manifest_failure("E_SQLITE_PROBE_UNSAFE", "the source yielded no usable snapshot")
    return snapshot


def _snapshot_evidence(snapshot: Mapping[str, Any]) -> CheckpointEvidence:
    verification = snapshot["verification"]
    detail = {
        "integrity": str(verification.get("integrity", "")),
        "revision": str(verification.get("alembic_revision", "")),
        "snapshot_device": int(os.lstat(Path(str(snapshot["path"]))).st_dev),
    }
    return CheckpointEvidence("sqlite_snapshotted", "snapshot_verification", _digest(detail), detail)


def _rebuild_step(plan: MigrationPlan, run_dir: Path, *, embedder: Any) -> Any:
    """Build the run's staged projections from the safety snapshot (DETAIL 10.2)."""
    snapshot_path = Path(run_dir) / SNAPSHOT_DIR_NAME / SNAPSHOT_DB_NAME
    url = snapshot_url_for(snapshot_path)
    resolved = resolve_migration_embedder(plan, embedder=embedder)
    records = _run_blocking(
        lambda: _collect_records(url, batch_size=DEFAULT_PROJECTION_BATCH_SIZE)
    )
    staging = staged_projection_paths(Path(run_dir))
    return _run_blocking(
        lambda: projection_rebuild.rebuild_projections(
            url,
            staging_vector_path=staging["vector"],
            staging_graph_path=staging["graph"],
            embedder=resolved,
            vector_size=DEFAULT_PROJECTION_VECTOR_SIZE,
            batch_size=DEFAULT_PROJECTION_BATCH_SIZE,
            embedding_plan_digest=projection_rebuild.embedding_plan_digest(records),
        )
    )


async def _collect_records(url: str, *, batch_size: int) -> list[Any]:
    return [
        record
        async for record in projection_rebuild.iter_canonical_projection_records(
            url, batch_size=batch_size
        )
    ]


def _rebuild_evidence(result: Any) -> CheckpointEvidence:
    detail = {
        "completed_batches": int(getattr(result, "completed_batches", 0)),
        "vector_ids_digest": str(getattr(result, "vector_ids_digest", "")),
        "graph_nodes_digest": str(getattr(result, "graph_nodes_digest", "")),
        "graph_edges_digest": str(getattr(result, "graph_edges_digest", "")),
    }
    return CheckpointEvidence("projections_built", "rebuild_result", _digest(detail), detail)


def _staged_evidence(verdict: Any) -> CheckpointEvidence:
    """The `staged_verified` evidence, built from the seam's OWN verdict."""
    if not getattr(verdict, "valid", False):
        raise _manifest_failure(STAGED_VERIFICATION_REFUSED, _verdict_reason(verdict))
    detail = {
        "basis": str(getattr(verdict, "basis", "")),
        "staging_digest": str(getattr(verdict, "staging_digest", "")),
        "artifacts": list(getattr(verdict, "artifacts", ())),
    }
    return CheckpointEvidence("staged_verified", "staged_verification", _digest(detail), detail)


def _publication_plan_evidence(plan: MigrationPlan) -> CheckpointEvidence:
    detail = {
        "targets": list(PUBLICATION_ARTIFACTS),
        "pinned": {
            label: _digest(asdict(plan.targets[label])) for label in PUBLICATION_ARTIFACTS
        },
    }
    return CheckpointEvidence("publishing", "publication_plan", _digest(detail), detail)


def _publication_events_evidence(manifest: MigrationManifest) -> CheckpointEvidence:
    recorded = publication_events_from_manifest(manifest)
    detail = {
        "artifacts": {label: list(recorded.get(label, ())) for label in PUBLICATION_ARTIFACTS}
    }
    return CheckpointEvidence("published", "publication_events", _digest(detail), detail)


def _manifest_update_evidence(manifest_path: Path) -> CheckpointEvidence:
    payload = Path(manifest_path).read_bytes()
    detail = {
        "manifest_digest": hashlib.sha256(payload).hexdigest(),
        "manifest_bytes": len(payload),
        "checkpoint": "complete",
    }
    return CheckpointEvidence("complete", "manifest_update", _digest(detail), detail)


def _staged_entry_identity(path: Path, *, artifact: str) -> ArtifactIdentity:
    """The no-follow identity of a staged entry observed through its own parent."""
    candidate = Path(path)
    with storage_lock.open_directory_nofollow(candidate.parent) as parent_fd:
        return _observed_entry_identity(parent_fd, candidate.name, artifact=artifact)


def _create_run_manifest(
    plan: MigrationPlan, preconditions: MutationPreconditions
) -> MigrationManifest:
    """Materialize the run's manifest at ``planned`` -- the first durable state.

    A run id that already owns a manifest is ``E_RUN_ALREADY_EXISTS``: a run id is
    never reused. The `planned` event carries the plan evidence the first
    transition would have required, so the chain starts complete rather than with
    an unproven first element.
    """
    path = run_manifest_path(plan)
    if os.path.lexists(path):
        raise _manifest_failure(
            E_RUN_ALREADY_EXISTS, "this run id already owns a manifest; a run id is never reused"
        )
    _prepare_run_directory(path.parent, plan.request.run_id)
    manifest = MigrationManifest(
        schema_version=MANIFEST_SCHEMA_VERSION,
        run_id=plan.request.run_id,
        strategy=plan.request.strategy,
        checkpoint="planned",
        status="running",
        source_identity=plan.source_sql,
        target_identities_before={
            label: plan.targets[label] for label in PUBLICATION_ARTIFACTS if label in plan.targets
        },
        config_digest=plan.config_digest,
        runtime_stop_attestation=asdict(preconditions.attestation),
        artifacts={},
        completed_steps=["planned"],
        events=[],
        embedding=asdict(plan.embedding),
        failure=None,
    )
    evidence = _planned_evidence(plan, preconditions)
    seeded = _append_event_to_manifest(
        manifest,
        "planned",
        f"advance{_checkpoint_operation_separator()}planned",
        {"evidence": evidence.code, "digest": evidence.digest},
    )
    _write_manifest(path, replace(seeded, checkpoint="planned", completed_steps=["planned"]))
    reopened = load_manifest(path)
    if reopened.checkpoint != "planned":
        raise _manifest_failure(
            "E_MANIFEST_UPDATE_UNPROVEN", "the durable manifest does not record `planned`"
        )
    return reopened


def _record_forward_failure(manifest_path: Path, step: str, exc: BaseException) -> None:
    """DETAIL 9.3: keep the last completed checkpoint and record the cause, in full.

    The stable code is the refusal's own leading token when it has one (this
    module raises ``CODE: detail`` / ``CODE``), the step is the stage that failed,
    and the exception CLASS is the root cause of the chain -- never the wrapping
    ``ValueError``. A failure already recorded for the same step and cause is not
    recorded a second time: a repeated resume must repeat nothing, including its
    own failure record.
    """
    path = Path(manifest_path)
    if not os.path.lexists(path):
        return
    message = str(exc)
    token = message.split(":", 1)[0].strip() if message else ""
    code = token if re.fullmatch(r"E_[A-Z0-9_]+", token) else E_MIGRATION_STAGE_FAILED
    with contextlib.suppress(Exception):
        manifest = load_manifest(path)
        failure: dict[str, Any] = {
            "code": code[:128],
            "step": step[:128],
            "message": message[:512],
            "exception": _failure_exception_class(exc)[:128],
            "completed_steps": list(manifest.completed_steps)[:MAX_MANIFEST_ENTRIES],
        }
        recorded = manifest.failure or {}
        if (
            manifest.status == "failed"
            and recorded.get("step") == failure["step"]
            and recorded.get("code") == failure["code"]
        ):
            return
        _write_manifest(path, replace(manifest, status="failed", failure=failure))


_PUBLICATION_EVENT_SEQUENCE: tuple[str, ...] = (
    PUBLICATION_EVENT_REVALIDATED,
    PUBLICATION_EVENT_QUARANTINED,
    PUBLICATION_EVENT_STAGING_PUBLISHED,
    PUBLICATION_EVENT_PARENT_FSYNCED,
)


def _failure_step(manifest_path: Path, cursor: Mapping[str, str]) -> str:
    """Name the step that actually failed, refining a per-artifact cursor.

    The forward stepper reports a publication failure per ARTIFACT (the shipped
    primitive records four events of its own), so the run's OWN durable record is
    consulted: the first event of that artifact's DETAIL 9.3 sequence it has not
    recorded yet is exactly the event the failure hit -- the filesystem operation
    is performed before its event is written. A step that is not a publication
    operation is returned unchanged.
    """
    step = str(cursor.get("step", ""))
    artifact, _separator, _event = step.partition(PUBLICATION_EVENT_OPERATION_SEPARATOR)
    if artifact not in PUBLICATION_ARTIFACTS:
        return step
    with contextlib.suppress(Exception):
        recorded = _publication_records(load_manifest(Path(manifest_path)), artifact)[0]
        if len(recorded) < len(_PUBLICATION_EVENT_SEQUENCE):
            return (
                f"{artifact}{PUBLICATION_EVENT_OPERATION_SEPARATOR}"
                f"{_PUBLICATION_EVENT_SEQUENCE[len(recorded)]}"
            )
    return step


def _publish_artifact_now(
    plan: MigrationPlan,
    label: str,
    *,
    run_dir: Path,
    manifest_path: Path,
    staged: Path,
) -> None:
    """Publish one fully staged artifact with the shipped primitive."""
    publish_artifact(
        plan,
        label,
        staged_identity=_staged_entry_identity(staged, artifact=label),
        run_dir=Path(run_dir),
        manifest_path=Path(manifest_path),
    )


def _continue_publication(
    plan: MigrationPlan,
    label: str,
    names: tuple[str, ...],
    *,
    run_dir: Path,
    manifest_path: Path,
    cursor: dict[str, str],
) -> None:
    """Execute the ONE remaining step of one artifact's DETAIL 9.3 event sequence.

    `classify_resume` has already proved that the run's own recorded events are a
    PREFIX of this artifact's sequence and that the disk agrees with that prefix,
    so exactly one operation remains. This function performs it with the same
    primitives `publish_artifact` uses -- the rename, the no-follow revalidation,
    the fsyncs and the per-event durable record -- and never repairs, guesses or
    deletes anything. The staged identity it publishes is the one the run's OWN
    `prestate_revalidated` record pinned.
    """
    run = Path(run_dir)
    target = Path(plan.targets[label].lexical_path)
    events = list(names)
    _recorded_names, payloads = _publication_records(load_manifest(manifest_path), label)
    recorded_identity = _recorded_staging_identity(payloads[0]) if payloads else None
    if events == [PUBLICATION_EVENT_REVALIDATED]:
        with storage_lock.open_directory_nofollow(run) as run_fd:
            with storage_lock.open_directory_nofollow(target.parent) as parent_fd:
                observed = _observed_entry_identity(parent_fd, target.name, artifact=label)
                if observed.kind == "absent":
                    cursor["step"] = f"{label}.{PUBLICATION_EVENT_ABSENT}"
                    _record_publication_event(
                        manifest_path, label, PUBLICATION_EVENT_ABSENT, {"prestate": "absent"}
                    )
                else:
                    cursor["step"] = f"{label}.{PUBLICATION_EVENT_QUARANTINED}"
                    quarantine_fd = _open_quarantine_directory(run_fd)
                    try:
                        entry = _quarantine_entry_name(label, plan.request.run_id)
                        _rename_entry(parent_fd, target.name, quarantine_fd, entry, artifact=label)
                        os.fsync(quarantine_fd)
                        _record_publication_event(
                            manifest_path,
                            label,
                            PUBLICATION_EVENT_QUARANTINED,
                            {
                                "quarantine": f"{QUARANTINE_DIRECTORY_NAME}/"
                                f"{QUARANTINE_PREPUBLISH_NAME}/{entry}",
                                "kind": observed.kind,
                                "identity": _digest(asdict(observed)),
                            },
                        )
                    finally:
                        os.close(quarantine_fd)
        events.append(PUBLICATION_EVENT_QUARANTINED)
    if events[-1] in (PUBLICATION_EVENT_QUARANTINED, PUBLICATION_EVENT_ABSENT):
        staged = staged_projection_paths(run)[label]
        staged_identity = _staged_entry_identity(staged, artifact=label)
        if recorded_identity is not None and _identity_key(staged_identity) != recorded_identity:
            raise _publication_failure(
                "E_STAGING_IDENTITY_CHANGED",
                f"{label} staged entry is not the identity this run revalidated its prestate against",
            )
        cursor["step"] = f"{label}.{PUBLICATION_EVENT_STAGING_PUBLISHED}"
        with storage_lock.open_directory_nofollow(staged.parent) as staging_fd:
            with storage_lock.open_directory_nofollow(target.parent) as parent_fd:
                _rename_entry(staging_fd, staged.name, parent_fd, target.name, artifact=label)
                os.fsync(parent_fd)
        _record_publication_event(
            manifest_path,
            label,
            PUBLICATION_EVENT_STAGING_PUBLISHED,
            {"identity": _digest(asdict(staged_identity)), "path_present": True},
        )
        events.append(PUBLICATION_EVENT_STAGING_PUBLISHED)
    if events[-1] == PUBLICATION_EVENT_STAGING_PUBLISHED:
        cursor["step"] = f"{label}.{PUBLICATION_EVENT_PARENT_FSYNCED}"
        parents = [str(target.parent)]
        with storage_lock.open_directory_nofollow(target.parent) as parent_fd:
            os.fsync(parent_fd)
        quarantine_parent = run / QUARANTINE_DIRECTORY_NAME / QUARANTINE_PREPUBLISH_NAME
        if os.path.isdir(quarantine_parent):
            with storage_lock.open_directory_nofollow(quarantine_parent) as quarantine_fd:
                os.fsync(quarantine_fd)
            parents.append(str(quarantine_parent))
        _record_publication_event(
            manifest_path, label, PUBLICATION_EVENT_PARENT_FSYNCED, {"parents": parents}
        )


def _publish_remaining(
    plan: MigrationPlan,
    manifest_path: Path,
    *,
    run_dir: Path,
    cursor: dict[str, str],
) -> MigrationManifest:
    """Publish every artifact whose OWN recorded event sequence is not complete.

    Nothing is republished: an artifact whose sequence is already complete is
    skipped, which is what makes a repeated resume repeat no swap (acceptance 4).
    """
    path = Path(manifest_path)
    recorded = publication_events_from_manifest(load_manifest(path))
    staging = staged_projection_paths(Path(run_dir))
    for label in PUBLICATION_ARTIFACTS:
        names = tuple(recorded.get(label, ()))
        if _published_sequence_is_complete(names):
            continue
        if not names:
            cursor["step"] = f"{label}.{PUBLICATION_EVENT_REVALIDATED}"
            _publish_artifact_now(
                plan, label, run_dir=Path(run_dir), manifest_path=path, staged=staging[label]
            )
        else:
            _continue_publication(
                plan, label, names, run_dir=Path(run_dir), manifest_path=path, cursor=cursor
            )
    return load_manifest(path)


def _run_forward(
    plan: MigrationPlan,
    manifest_path: Path,
    *,
    embedder: Any = None,
) -> MigrationManifest:
    """Advance one run to `complete`, one earned checkpoint at a time.

    Idempotent by construction: a step whose checkpoint is already recorded is
    skipped (DETAIL 10.5's "completed idempotent steps may continue"), a
    publication whose per-artifact sequence is already complete is not repeated,
    and any failure is recorded exactly once with its stable cause before it is
    re-raised. The maintenance locks are held for the whole run -- they are taken
    here rather than trusted from a caller -- and released afterwards even when
    the run fails, which is what keeps the root-lock protection intact.
    """
    path = Path(manifest_path)
    run_dir = path.parent
    cursor = {"step": "locked"}
    locks: Any = None
    try:
        locks = acquire_maintenance_locks(plan)
        manifest = load_manifest(path)
        if forward_checkpoint_index(manifest.checkpoint) < forward_checkpoint_index("locked"):
            manifest = advance_manifest_checkpoint(
                path, "locked", evidence=[_lock_ownership_evidence(plan, locks)]
            )
        if getattr(locks, "graph_lock_created", False):
            manifest = record_graph_lock_creation(path, graph_lock_creation_record(plan, locks))
        if forward_checkpoint_index(manifest.checkpoint) < forward_checkpoint_index("backed_up"):
            cursor["step"] = "backed_up"
            backup = create_run_backup(plan, locks=locks)
            manifest = advance_manifest_checkpoint(
                path, "backed_up", evidence=[_backup_evidence(backup)]
            )
        if forward_checkpoint_index(manifest.checkpoint) < forward_checkpoint_index(
            "sqlite_snapshotted"
        ):
            cursor["step"] = "sqlite_snapshotted"
            manifest = advance_manifest_checkpoint(
                path, "sqlite_snapshotted", evidence=[_snapshot_evidence(_snapshot_report(plan, run_dir))]
            )
        if forward_checkpoint_index(manifest.checkpoint) < forward_checkpoint_index(
            "projections_built"
        ):
            cursor["step"] = "projections_built"
            manifest = advance_manifest_checkpoint(
                path, "projections_built", evidence=[_rebuild_evidence(_rebuild_step(plan, run_dir, embedder=embedder))]
            )
        staged_verdict = None
        if forward_checkpoint_index(manifest.checkpoint) < forward_checkpoint_index("staged_verified"):
            cursor["step"] = "staged_verified"
            staged_verdict = staged_projection_verification(
                run_dir / SNAPSHOT_DIR_NAME / SNAPSHOT_DB_NAME,
                run_dir,
                expected_vector_size=DEFAULT_PROJECTION_VECTOR_SIZE,
                batch_size=DEFAULT_PROJECTION_BATCH_SIZE,
            )
            manifest = advance_manifest_checkpoint(
                path, "staged_verified", evidence=[_staged_evidence(staged_verdict)]
            )
        if forward_checkpoint_index(manifest.checkpoint) < forward_checkpoint_index("publishing"):
            cursor["step"] = "publishing"
            manifest = advance_manifest_checkpoint(
                path, "publishing", evidence=[_publication_plan_evidence(plan)]
            )
        if forward_checkpoint_index(manifest.checkpoint) < forward_checkpoint_index("published"):
            cursor["step"] = "published"
            manifest = _publish_remaining(plan, path, run_dir=run_dir, cursor=cursor)
            cursor["step"] = "published"
            manifest = advance_manifest_checkpoint(
                path, "published", evidence=[_publication_events_evidence(manifest)]
            )
        if forward_checkpoint_index(manifest.checkpoint) < forward_checkpoint_index("verified"):
            cursor["step"] = "verified"
            manifest = advance_manifest_checkpoint(
                path,
                "verified",
                evidence=[
                    reopen_verification_evidence(
                        run_dir / SNAPSHOT_DIR_NAME / SNAPSHOT_DB_NAME,
                        plan,
                        staged_verdict,
                        expected_vector_size=DEFAULT_PROJECTION_VECTOR_SIZE,
                        batch_size=DEFAULT_PROJECTION_BATCH_SIZE,
                    )
                ],
            )
        if forward_checkpoint_index(manifest.checkpoint) < forward_checkpoint_index("complete"):
            cursor["step"] = "complete"
            manifest = advance_manifest_checkpoint(
                path, "complete", evidence=[_manifest_update_evidence(path)]
            )
        if manifest.status != "complete":
            # DETAIL 9.3: `status` is orthogonal to the checkpoint chain, and a run
            # that reached `complete` is COMPLETE -- never left claiming "running".
            _write_manifest(path, replace(manifest, status="complete"))
            manifest = load_manifest(path)
            if manifest.status != "complete":
                raise _manifest_failure(
                    "E_MANIFEST_UPDATE_UNPROVEN", "the durable manifest does not record `complete`"
                )
        return manifest
    except BaseException as exc:
        _record_forward_failure(path, _failure_step(path, cursor), exc)
        raise
    finally:
        if locks is not None:
            with contextlib.suppress(Exception):
                locks.release()


# ---------------------------------------------------------------------------
# S2-08 -- rollback to the exact pre-state with a retained diagnostic quarantine
# (DETAIL 9.3's separate rollback event chain; DETAIL 10.6 steps 1-10).
#
# Three promises, and every fail-closed branch below exists to keep one of them:
#
# 1. THE RECORD DECIDES, THE DISK IS CHECKED. Nothing is inferred from the fresh
#    plan. The CURRENT TARGET IDENTITY is checked against the run's OWN durable
#    record -- the S2-07 `staged_identity_key` payload the publication wrote when it
#    revalidated its prestate -- and the ORIGINAL pre-state against the run's OWN
#    backup report. Where the record carries no comparable identity the rollback
#    REFUSES (`E_ROLLBACK_IDENTITY_MISSING`) instead of inferring equality from the
#    live plan, because `manifest.target_identities_before` is still unpopulated in
#    this project (routing-matrix R-4; owner S3-06).
#
# 2. NOTHING IS DELETED. The run-created current artifact is renamed into a UNIQUE
#    `quarantine/failed-current/` entry (its name carries a fresh random suffix, so
#    a repeat cannot collide with a retained copy); the pre-state is restored from
#    the retained backup through a freshly verified run-owned staging entry; and the
#    backup, the run's own prepublish quarantine, the SQLite source, the legacy
#    stores and every unrelated config entry are only ever READ. A failed step
#    retains every copy, records `rollback_failed` and is never retried
#    automatically.
#
# 3. A RESTORE IS COPIED, VERIFIED AND RENAMED -- NEVER MOVED. A regular file and a
#    directory are streamed from the backup copy into a UNIQUE run-owned
#    `restore/` staging entry, verified THERE, and only then renamed into the vacant
#    target on the same filesystem; a prior symlink is created at the staging name
#    with the exact raw `readlink` string and verified by `lstat` + `readlink` only.
#
# Honest limits, disclosed rather than worked around:
#
# * A DIRECTORY's identity is not its contents in this project's model
#   (`_observed_entry_identity` records a directory as kind/device/inode/mode with
#   size/mtime/sha256 = None), so a restored directory is verified at that identity
#   level AND, for every regular file inside it, against the digest the run's own
#   backup report recorded for that child path. That is a CONTENT check of the files
#   and NOT a recursive directory digest; no recursive digest is invented here
#   (routing-matrix R-6/O-3; residual owner: the next writer of this seam / S3-06).
# * Rollback does NOT need the staged-verification seam and does not ask it: it
#   restores a pre-state the run already backed up and it publishes nothing, so the
#   capability gate that keeps `apply`/`resume` from claiming a verified publication
#   is untouched by this slice.
# * `apply` and `resume` remain fail-closed exactly as S2-06/S2-07 left them; wiring
#   the publication engine into them is still S3-06's deliverable.
#
# The rollback chain is recorded as EVENTS at the run's recording checkpoint, with
# the chain step as the event OPERATION -- the representation the manifest schema
# admits (the chain values are not forward checkpoints, and `status` carries only
# `rolled_back` / `rollback_failed`), and the same way the per-artifact publication
# steps are recorded. Existing events are never rewritten.
# ---------------------------------------------------------------------------

ROLLBACK_RESTORE_DIRECTORY_NAME = "restore"
QUARANTINE_FAILED_CURRENT_NAME = "failed-current"
ROLLBACK_EVENT_LOCKED = "rollback.locked"
ROLLBACK_EVENT_CURRENT_QUARANTINED = "rollback.current_quarantined"
ROLLBACK_EVENT_PRESTATE_RESTORED = "rollback.prestate_restored"
ROLLBACK_EVENT_VERIFIED = "rollback.verified"
ROLLBACK_EVENT_ROLLED_BACK = "rolled_back"
ROLLBACK_EVENT_FAILED = "rollback.failed"
ROLLBACK_EVENT_CHAIN: tuple[str, ...] = (
    ROLLBACK_EVENT_LOCKED,
    ROLLBACK_EVENT_CURRENT_QUARANTINED,
    ROLLBACK_EVENT_PRESTATE_RESTORED,
    ROLLBACK_EVENT_VERIFIED,
    ROLLBACK_EVENT_ROLLED_BACK,
)
ROLLBACK_ACTION_NONE = "none"
ROLLBACK_ACTION_RESTORE_ONLY = "restore_only"
ROLLBACK_ACTION_QUARANTINE_AND_RESTORE = "quarantine_and_restore"
ROLLBACK_CODE_TERMINAL = "E_ROLLBACK_TERMINAL"
ROLLBACK_CODE_RUN_MISMATCH = "E_ROLLBACK_RUN_MISMATCH"
ROLLBACK_CODE_TARGET_DRIFT = "E_ROLLBACK_TARGET_DRIFT"
ROLLBACK_CODE_IDENTITY_MISSING = "E_ROLLBACK_IDENTITY_MISSING"
ROLLBACK_CODE_BACKUP_MISMATCH = "E_ROLLBACK_BACKUP_MISMATCH"
ROLLBACK_CODE_UNSAFE_PARENT = "E_ROLLBACK_UNSAFE_PARENT"
# S2-08 fix round 1, review F3.ii / R3.2: this condition already HAS a code in the
# operator-facing stable registry -- `E_ROLLBACK_VERIFY` (DETAIL.md:807, DETAIL §12) -- so
# the delta's coined near-synonym `E_ROLLBACK_VERIFY_FAILED` was RENAMED to it rather than
# superseded: the two artifacts disagreed about a code an operator will grep for, and the
# DETAIL-registered name is the authority. Constant, call sites, docstring and tests moved
# together.
ROLLBACK_CODE_VERIFY = "E_ROLLBACK_VERIFY"
# S2-08 fix round 1, review F1.2 / R1: the restore step's OWN failure must not reach the
# durable record as the masked `'storage lock failure'` string that
# `storage_lock.open_directory_nofollow` produces while unwinding an escaping `OSError`.
# A raw `OSError` (or the wrapper it becomes) around the restore call is mapped to THIS
# cause-specific code, with the underlying code and the wrapped cause class in the message.
ROLLBACK_CODE_RESTORE_FAILED = "E_ROLLBACK_RESTORE_FAILED"
# The SAME backup guard a resume repeats, reported under this card's own code.
_ROLLBACK_BACKUP_REFUSALS = frozenset({"E_RESUME_BACKUP_CHANGED", "E_RESUME_BACKUP_MISSING"})


def _rollback_failure(code: str, detail: str = "") -> ValueError:
    """A stable, fail-closed rollback refusal; never a silent partial restore."""
    return ValueError(f"{code}: {detail}" if detail else code)


def _rollback_entry_name(label: str, *, unique: bool = False) -> str:
    """A bounded run-owned name; ``unique`` appends a fresh random suffix.

    The unique form is what makes a diagnostic quarantine entry (and a restore
    staging entry) incapable of colliding with a copy an earlier attempt retained:
    a rollback never overwrites what it, or a previous attempt, kept.
    """
    local = _backup_entry_name(label)[: MAX_BACKUP_NAME_BYTES - 33]
    return f"{local}-{uuid4().hex}" if unique else local


def _discard_staged_entry(parent_fd: int, name: str) -> None:
    """Remove ONE half-built run-owned staging entry that THIS attempt just created.

    S2-08 fix round 1 (review F1.3 / R1.3). A failed restore must not leave a half-built
    ``restore/<artifact>-<hex>/stage-*`` entry behind, so the entry THIS attempt staged is
    removed again -- and ONLY it. The name is this attempt's own
    (``_rollback_entry_name(unique=True)``), so a previous attempt's retained entry can
    never be named by it, and nothing else -- the unique diagnostic quarantine, the
    retained backup, the run's own prepublish quarantine, the SQLite snapshot, the legacy
    stores, the target -- is touched by this function at all. The walk is no-follow and
    bounded, and it NEVER raises: a cleanup must not mask the failure already in flight.
    """
    if not _RUN_OWNED_NAME_PATTERN.match(name):
        return
    try:
        info = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except OSError:
        return
    if stat.S_ISDIR(info.st_mode):
        try:
            child_fd: int | None = os.open(
                name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent_fd
            )
        except OSError:
            return
        try:
            for child in os.listdir(child_fd):
                _discard_staged_entry(child_fd, child)
        except OSError:
            pass
        finally:
            os.close(child_fd)
        with contextlib.suppress(OSError):
            os.rmdir(name, dir_fd=parent_fd)
        return
    with contextlib.suppress(OSError):
        os.unlink(name, dir_fd=parent_fd)


def _rollback_backup_entries(
    run_dir: Path, manifest: MigrationManifest
) -> dict[str, Mapping[str, Any]]:
    """Confirm the run's OWN backup report, then return it keyed by its labels.

    The confirmation is the SAME guard a resume repeats (`_resume_backup_state`:
    report self-digest, run id and every recorded entry's size/digest against the copy
    it describes) reused instead of duplicated; its stable refusal is reported under
    this card's own backup-mismatch code, with the underlying code preserved in the
    message. The returned entries are what the restore binds against: one record per
    artifact plus one per child of a backed-up directory tree.

    S2-08 fix round 1 (review F4): the report's per-entry guard cannot see an entry the
    backup copy holds and the report does not record, so ``_rollback_prevalidate_backup_tree``
    additionally compares the retained DIRECTORY backup against the run's own record HERE --
    in the read-only phase of the entrypoint, before the locks and before anything moves.
    """
    try:
        _resume_backup_state(Path(run_dir), manifest)
    except ValueError as exc:
        code = str(exc).split(":", 1)[0]
        if code in _ROLLBACK_BACKUP_REFUSALS:
            raise _rollback_failure(ROLLBACK_CODE_BACKUP_MISMATCH, f"{code}: {exc}") from exc
        raise
    report_path = Path(run_dir) / BACKUP_REPORT_NAME
    if not os.path.lexists(report_path):
        # `_resume_backup_state` above has already accepted a run that has passed no
        # checkpoint requiring a report, so there is no recorded pre-state at all --
        # every artifact then refuses on its missing identity, below.
        return {}
    payload = json.loads(_read_bounded_manifest_bytes(report_path).decode("utf-8"))
    raw = payload.get("entries")
    if not isinstance(raw, (list, tuple)) or len(raw) > MAX_BACKUP_ENTRIES:
        raise _rollback_failure(
            ROLLBACK_CODE_BACKUP_MISMATCH, "the backup report lists no bounded entries"
        )
    entries: dict[str, Mapping[str, Any]] = {}
    for entry in raw:
        if isinstance(entry, Mapping) and isinstance(entry.get("artifact"), str):
            entries[str(entry["artifact"])] = entry
    # S2-08 fix round 1 (review F4): the retained directory backups are compared with the
    # run's own record HERE, in the read-only phase, so an ADDED unrecorded entry is
    # refused BEFORE the diagnostic quarantine moves anything.
    _rollback_prevalidate_backup_tree(Path(run_dir), entries)
    return entries


def _rollback_backup_tree_paths(backup_root: Path, *, artifact: str) -> dict[str, str]:
    """Every entry the RETAINED BACKUP COPY of one directory artifact really holds.

    A read-only, no-follow walk through pinned descriptors (the same discipline the staged
    restore uses), bounded by the backup depth and entry bounds; nothing is copied, opened
    for write or followed. S2-08 fix round 1 (review F4): this is what lets the read-only
    decision phase compare the backup copy's own FILE SET with the run's recorded one
    BEFORE anything moves, instead of discovering an added entry after the quarantine.
    """
    resolved: dict[str, str] = {}

    def walk(parent_fd: int, parts: tuple[str, ...], depth: int) -> None:
        if depth > MAX_BACKUP_DEPTH:
            raise _rollback_failure(
                ROLLBACK_CODE_BACKUP_MISMATCH,
                f"the retained backup of {artifact} nests deeper than the backup depth bound",
            )
        for name, info in _bounded_child_listing(parent_fd, artifact=f"rollback:{artifact}"):
            child_parts = (*parts, name)
            relative = "/".join(child_parts)
            if stat.S_ISLNK(info.st_mode):
                resolved[relative] = "symlink"
            elif stat.S_ISDIR(info.st_mode):
                resolved[relative] = "directory"
                with _pinned_child_directory(
                    parent_fd, name, artifact=f"rollback:{artifact}/{relative}", before=info
                ) as (child_fd, _opened):
                    walk(child_fd, child_parts, depth + 1)
            elif stat.S_ISREG(info.st_mode):
                resolved[relative] = "regular_file"
            else:
                resolved[relative] = "special"
            if len(resolved) > MAX_BACKUP_ENTRIES:
                raise _rollback_failure(
                    ROLLBACK_CODE_BACKUP_MISMATCH,
                    f"the retained backup of {artifact} holds more than the backup entry bound",
                )

    try:
        with storage_lock.open_directory_nofollow(backup_root) as root_fd:
            walk(root_fd, (), 1)
    except storage_lock.StorageLockError as exc:
        raise _rollback_failure(
            ROLLBACK_CODE_BACKUP_MISMATCH,
            f"the retained backup of {artifact} cannot be walked ({exc.code})",
        ) from exc
    return resolved


def _rollback_prevalidate_backup_tree(
    run_dir: Path, entries: Mapping[str, Mapping[str, Any]]
) -> None:
    """Refuse an unrecorded entry in a retained directory backup BEFORE anything moves.

    S2-08 fix round 1 (review F4). The per-entry guard a resume repeats
    (``_resume_backup_state``) confirms every RECORDED entry against the copy it
    describes, and it catches a REMOVED or CHANGED child before anything moves -- but it
    cannot see an entry the backup copy holds and the run's report does not record. That
    gap was only closed inside ``_stage_restored_tree``, i.e. AFTER the run-created
    artifact had already been quarantined. This pass closes it in the READ-ONLY phase with
    the SAME cause-specific code, by comparing the walk above with the run's OWN record.
    """
    for label, entry in entries.items():
        if entry.get("present") is not True or entry.get("kind") != "directory":
            continue
        if not label.startswith("target:"):
            continue
        prefix = f"{label}/"
        recorded = {
            key[len(prefix) :]: str(item.get("kind"))
            for key, item in entries.items()
            if key.startswith(prefix)
        }
        backup_root = _rollback_backup_location(run_dir, entry, label)
        for relative, kind in _rollback_backup_tree_paths(backup_root, artifact=label).items():
            if recorded.get(relative) != kind:
                raise _rollback_failure(
                    ROLLBACK_CODE_IDENTITY_MISSING,
                    f"the retained backup of {label} holds {relative}, which the run's own report does "
                    f"not record as a {kind}",
                )


def _rollback_artifact_records(
    entries: Mapping[str, Mapping[str, Any]], label: str
) -> tuple[Mapping[str, Any] | None, dict[str, Mapping[str, Any]]]:
    """One artifact's pre-state record and the records of its recorded children."""
    root_label = f"target:{label}"
    descendants = {
        key[len(root_label) + 1 :]: entry
        for key, entry in entries.items()
        if key.startswith(root_label + "/")
    }
    return entries.get(root_label), descendants


def _rollback_target_identity(parent: Path, name: str, *, artifact: str) -> ArtifactIdentity:
    """The no-follow identity of a target entry, or the UNSAFE PARENT Stop.

    The parent is opened ``O_DIRECTORY | O_NOFOLLOW`` through its whole chain: an
    intermediate symlink, a non-directory or a missing path is this card's
    ``E_ROLLBACK_UNSAFE_PARENT`` and is never traversed. A final symlink is recorded
    by its exact raw link string and never followed; an absent target is ``absent``,
    which the decision below interprets rather than treats as an error.
    """
    try:
        with storage_lock.open_directory_nofollow(parent) as parent_fd:
            return _observed_entry_identity(parent_fd, name, artifact=artifact)
    except storage_lock.StorageLockError as exc:
        raise _rollback_failure(
            ROLLBACK_CODE_UNSAFE_PARENT,
            f"{artifact} target parent is not a no-follow real directory ({exc.code})",
        ) from exc


def _rollback_prestate_matches(observed: ArtifactIdentity, entry: Mapping[str, Any] | None) -> bool:
    """Is the target entry still the UNTOUCHED pre-state the backup recorded?

    An absent record matches only an absent entry. A regular file is compared by
    device/inode/size/digest/mode and a directory by device/inode/mode; a directory's
    CONTENTS are not covered by this project's identity model (R-6/O-3) and this
    comparison does not pretend otherwise. A symlink is compared by its exact raw link
    string only.
    """
    if entry is None:
        return False
    kind = entry.get("kind")
    if kind == "absent":
        return observed.kind == "absent"
    if observed.kind != kind:
        return False
    if kind == "regular_file":
        return (
            observed.device == entry.get("device")
            and observed.inode == entry.get("inode")
            and observed.size == entry.get("size")
            and observed.sha256 == entry.get("sha256")
            and observed.mode is not None
            and stat.S_IMODE(observed.mode) == entry.get("mode")
        )
    if kind == "directory":
        return (
            observed.device == entry.get("device")
            and observed.inode == entry.get("inode")
            and observed.mode is not None
            and stat.S_IMODE(observed.mode) == entry.get("mode")
        )
    if kind == "symlink":
        return observed.raw_link_target == entry.get("raw_link_target")
    return False


def _rollback_restored_matches(observed: ArtifactIdentity, entry: Mapping[str, Any]) -> bool:
    """Is a RESTORED entry the recorded pre-state (a copy cannot keep the inode)?

    DETAIL 10.6 restores by COPYING a verified backup copy and renaming it in, so the
    device/inode of the original pre-state cannot and must not be required here; the
    CONTENT (size + streamed sha256 for a regular file), the KIND and the MODE are.
    """
    kind = entry.get("kind")
    if kind == "absent":
        return observed.kind == "absent"
    if observed.kind != kind:
        return False
    if kind == "regular_file":
        return (
            observed.size == entry.get("size")
            and observed.sha256 == entry.get("sha256")
            and observed.mode is not None
            and stat.S_IMODE(observed.mode) == entry.get("mode")
        )
    if kind == "directory":
        return observed.mode is not None and stat.S_IMODE(observed.mode) == entry.get("mode")
    if kind == "symlink":
        return observed.raw_link_target == entry.get("raw_link_target")
    return False


@dataclass(frozen=True)
class RollbackDecision:
    """What rollback will do to ONE artifact, decided from the run's own record."""

    artifact: str
    target_path: str
    action: str
    recorded_target_identity: str
    prestate_kind: str


@dataclass(frozen=True)
class RollbackStep:
    """One artifact's decision plus the run's own records the decision rests on."""

    decision: RollbackDecision
    prestate: Mapping[str, Any] | None
    descendants: Mapping[str, Mapping[str, Any]]
    recorded_identity: tuple[Any, ...] | None = None


def _decide_rollback_artifact(
    label: str,
    pinned: ArtifactIdentity,
    names: tuple[str, ...],
    recorded: tuple[Any, ...] | None,
    prestate: Mapping[str, Any] | None,
    observed: ArtifactIdentity,
) -> RollbackDecision:
    """Decide ONE artifact's rollback from the run's OWN record and the disk.

    "Matches the recorded staged identity" means the entry at the target IS the entry
    this run published (a rename preserves it, so device/inode/mode/size/mtime/digest
    all still match the record the publication wrote), and that is the POSITIVE fact
    every decision is based on -- never the event prefix alone, because a publication
    whose rename landed without its event is exactly the crash state a rollback exists
    for. What follows from it:

    * a positively identified run-created entry is quarantined and the pre-state is
      restored (`quarantine_and_restore`);
    * a VACANT target whose recorded pre-state was not absent is restored
      (`restore_only`) -- the crash state between the quarantine and the publish rename.

      CORRECTED IN PLACE, S2-08 fix round 1 (review F2): this comment used to add "the state
      an interrupted earlier attempt leaves", and that was FALSE for a PARTIALLY restored
      multi-artifact rollback. An attempt that already restored one artifact leaves that
      target at its RECORDED PRE-STATE -- not vacant -- and a restored copy cannot carry the
      recorded pre-publication inode, so the drift Stop below fires for it first and a clean
      retry never reaches `restore_only` to repair the artifact that IS vacant: the tool
      cannot finish its own interrupted rollback and reports `E_ROLLBACK_TARGET_DRIFT` as the
      cause. That is the SHIPPED behaviour, pinned by
      `test_s208_fix1_a_clean_retry_after_a_partial_rollback_refuses_with_a_stated_cause` and
      named as a residual (owner S3-06) in the S2-08 fix-round SUMMARY. Completing such a
      retry would require either accepting a byte-equivalent restored pre-state where the
      run's own record claims a publication (that is one arm of the drift Stop) or making
      per-artifact restore progress durable (a rollback event-chain change); both are outside
      this fix round's scope.
    * a target that is provably still the untouched pre-state is left alone (`none`)
      -- unless the run's own record also claims it published this artifact, which the
      disk contradicts (`E_ROLLBACK_TARGET_DRIFT`);
    * anything else is a Stop -- `E_ROLLBACK_TARGET_DRIFT` when the target is neither
      of the two recorded entries, and `E_ROLLBACK_IDENTITY_MISSING` when the record
      carries no comparable staged identity at all, so the CURRENT target identity
      CANNOT be checked against a durable reference (R-4/O-2: refuse, never infer from
      the fresh plan, never treat an unrecorded entry as confirmed).
    """
    if prestate is None:
        raise _rollback_failure(
            ROLLBACK_CODE_IDENTITY_MISSING,
            f"the run's own backup report records nothing for {label}, so its ORIGINAL identity is "
            "unknown and the entry at the target cannot be checked against any durable reference",
        )
    identity = _digest(recorded) if recorded is not None else ""
    kind = str(prestate.get("kind"))
    if recorded is not None and _identity_key(observed) == recorded:
        return RollbackDecision(
            label, pinned.lexical_path, ROLLBACK_ACTION_QUARANTINE_AND_RESTORE, identity, kind
        )
    if observed.kind == "absent":
        action = ROLLBACK_ACTION_NONE if kind == "absent" else ROLLBACK_ACTION_RESTORE_ONLY
        return RollbackDecision(label, pinned.lexical_path, action, identity, kind)
    if _rollback_prestate_matches(observed, prestate):
        if PUBLICATION_EVENT_STAGING_PUBLISHED in names:
            raise _rollback_failure(
                ROLLBACK_CODE_TARGET_DRIFT,
                f"{label} is at its recorded pre-state although the run's own record records that it "
                "published this artifact",
            )
        return RollbackDecision(label, pinned.lexical_path, ROLLBACK_ACTION_NONE, identity, kind)
    if recorded is None:
        raise _rollback_failure(
            ROLLBACK_CODE_IDENTITY_MISSING,
            f"the run's own record carries no comparable staged identity for {label}, so the entry at "
            f"{pinned.lexical_path} cannot be checked against a durable reference",
        )
    raise _rollback_failure(
        ROLLBACK_CODE_TARGET_DRIFT,
        f"the entry at {pinned.lexical_path} is neither the identity this run published nor its "
        "recorded pre-state",
    )


def _observe_rollback_steps(
    plan: MigrationPlan, manifest: MigrationManifest, entries: Mapping[str, Mapping[str, Any]]
) -> tuple[RollbackStep, ...]:
    """Observe every target no-follow and decide -- this function MUTATES NOTHING.

    Every Stop of this card is raised here, before the first rename: an unsafe parent
    chain, a drifted target, a missing recorded identity. Everything is read through
    pinned no-follow descriptors and the run's own durable records.
    """
    steps: list[RollbackStep] = []
    for label in PUBLICATION_ARTIFACTS:
        pinned = plan.targets.get(label)
        if pinned is None:
            raise _rollback_failure(
                ROLLBACK_CODE_IDENTITY_MISSING, f"the plan records no {label} target to roll back"
            )
        _names, payloads = _publication_records(manifest, label)
        recorded = _recorded_staging_identity(payloads[0]) if payloads else None
        names, _payloads = _publication_records(manifest, label)
        prestate, descendants = _rollback_artifact_records(entries, label)
        target = Path(pinned.lexical_path)
        observed = _rollback_target_identity(target.parent, target.name, artifact=label)
        steps.append(
            RollbackStep(
                decision=_decide_rollback_artifact(label, pinned, names, recorded, prestate, observed),
                prestate=prestate,
                descendants=descendants,
                recorded_identity=recorded,
            )
        )
    return tuple(steps)


def _append_rollback_event(
    manifest_path: Path,
    step: str,
    payload: Mapping[str, Any],
    *,
    status: str | None = None,
    failure: Mapping[str, Any] | None = None,
) -> MigrationManifest:
    """Append ONE step of the rollback chain durably, with optional terminal state.

    The event is appended through the SAME in-memory chain helper the forward state
    machine uses, so an existing event is never rewritten and the sequence number, the
    previous-event link and the canonical body digest are derived in exactly one
    place; the terminal status and a recorded failure travel in the SAME atomic
    manifest write as the event that caused them.
    """
    updated = _append_event_to_manifest(
        load_manifest(Path(manifest_path)), PUBLICATION_EVENT_CHECKPOINT, step, payload
    )
    if status is not None:
        updated = replace(updated, status=cast(RunStatus, status))
    if failure is not None:
        updated = replace(updated, failure=dict(failure))
    _write_manifest(Path(manifest_path), updated)
    return load_manifest(Path(manifest_path))


def _failure_exception_class(exc: BaseException) -> str:
    """The class NAME of a failure's ROOT cause, never the wrapping refusal.

    DETAIL 9.3's failure record carries the exception class. Recording the class of the
    refusal this module raises would always say `ValueError` and say nothing, so the
    ``__cause__`` chain is followed to its root: for the restore leg that is the real
    ``FileNotFoundError`` / ``OSError`` behind the masked storage-lock string. The walk is
    bounded, so a pathological chain can never spin here.
    """
    cause = exc
    for _ in range(8):
        if cause.__cause__ is None:
            break
        cause = cause.__cause__
    return type(cause).__name__


def _record_rollback_failure(
    manifest_path: Path, code: str, step: str, message: str, *, exc: BaseException | None = None
) -> None:
    """Record `rollback_failed` and its stable cause WITHOUT masking the real error.

    S2-08 fix round 1 (review F5): the record now also carries the exception CLASS of the
    failure's root cause -- the field DETAIL 9.3's failure record names and the shipped
    record omitted.
    """
    failure = {"code": code[:128], "step": step[:128], "message": message[:512]}
    if exc is not None:
        failure["exception"] = _failure_exception_class(exc)[:128]
    with contextlib.suppress(Exception):
        _append_rollback_event(
            manifest_path,
            ROLLBACK_EVENT_FAILED,
            {"code": failure["code"], "step": failure["step"]},
            status="rollback_failed",
            failure=failure,
        )


def _quarantine_current_rollback(
    run_fd: int, target_parent_fd: int, target: Path, artifact: str, expected: tuple[Any, ...]
) -> str:
    """DETAIL 10.6 step 4: rename the CURRENT entry into a UNIQUE diagnostic quarantine.

    The entry is re-observed no-follow immediately before the rename and must still be
    the identity the decision was made from, so a swap that happened in between is
    refused instead of quarantining a stranger. ``quarantine/failed-current`` is
    run-owned, mode 0700, and the entry name carries a fresh random suffix, so nothing
    a previous attempt retained can be overwritten. Both parents are fsynced.
    """
    with _run_child_directory(run_fd, QUARANTINE_DIRECTORY_NAME) as quarantine_fd:
        with _run_child_directory(quarantine_fd, QUARANTINE_FAILED_CURRENT_NAME) as failed_fd:
            entry_name = _rollback_entry_name(artifact, unique=True)
            observed = _observed_entry_identity(target_parent_fd, target.name, artifact=artifact)
            if _identity_key(observed) != expected:
                raise _rollback_failure(
                    ROLLBACK_CODE_TARGET_DRIFT,
                    f"{artifact} changed between the rollback decision and its quarantine",
                )
            _rename_entry(target_parent_fd, target.name, failed_fd, entry_name, artifact=artifact)
            _fsync_pair(target_parent_fd, failed_fd)
    return f"{QUARANTINE_DIRECTORY_NAME}/{QUARANTINE_FAILED_CURRENT_NAME}/{entry_name}"


def _rollback_backup_location(run_dir: Path, entry: Mapping[str, Any], artifact: str) -> Path:
    """The retained backup copy one pre-state record names, never a caller path."""
    relative = _validate_run_relative_path(
        entry.get("run_relative_path"), field_name=f"rollback {artifact} run_relative_path"
    )
    return Path(run_dir) / relative


def _stage_restored_tree(
    backup_root: Path,
    staged_root_fd: int,
    *,
    artifact: str,
    descendants: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    """Copy a backed-up directory into the run-owned staging entry and verify it.

    The walk is the S2-05 ``_stage_tree`` (pinned no-follow descriptors, a stable
    listing re-checked after the walk, no interior symlink ever followed) into a FRESH
    staging directory, and every regular file it staged is then bound to the digest
    the run's OWN backup report recorded for that child path -- the content check a
    directory copy can honestly make, since this project has no recursive directory
    digest. The assembled tree is returned as its relative paths.

    S2-08 fix round 1 (review F1.1): the RECORDED directories are created and their
    recorded modes applied AFTER the staged files, deepest first. ``_stage_tree`` never
    creates a directory entry -- it flattens regular files into ``stage-*`` names at the
    staged root and only RECORDS the directories it walked -- so the shipped version
    opened the leaf's ANCESTORS (``parts[:-1]``) and then ``os.chmod``ed a leaf that did
    not exist yet: a deterministic ``FileNotFoundError`` for ANY recorded child directory,
    raised after the quarantine. The leaf is now created by ``_run_directory_chain`` for
    the FULL ``parts`` tuple and its recorded mode is applied through the returned
    descriptor. The order also matters for fidelity: ``_run_child_directory`` resets a
    directory it opens to ``BACKUP_DIRECTORY_MODE``, so applying the modes last -- and
    deepest-first, where the only chains that touch a directory are its own and its
    descendants' -- leaves every recorded directory at its recorded mode.
    """
    directories: list[dict[str, Any]] = []
    files: list[dict[str, Any]] = []
    budget: list[int] = [0]
    with storage_lock.open_directory_nofollow(backup_root) as backup_root_fd:
        _stage_tree(
            backup_root_fd,
            staged_root_fd,
            label=f"rollback:{artifact}",
            source_path=backup_root,
            parts=(),
            depth=1,
            budget=budget,
            directories=directories,
            files=files,
        )
    staged: list[str] = []
    for record in files:
        parts = tuple(record["parts"])
        relative = "/".join(parts)
        recorded_file = descendants.get(relative)
        if recorded_file is None or recorded_file.get("kind") != "regular_file":
            raise _rollback_failure(
                ROLLBACK_CODE_IDENTITY_MISSING,
                f"the backup of {artifact} holds {relative}, which the run's own report does not record "
                "as a regular file",
            )
        if (
            str(recorded_file.get("sha256")) != str(record["digest"])
            or recorded_file.get("size") != record["stat"].st_size
        ):
            raise _rollback_failure(
                ROLLBACK_CODE_BACKUP_MISMATCH,
                f"the retained backup copy of {artifact}/{relative} does not match the digest the run "
                "recorded for it",
            )
        with _run_directory_chain(staged_root_fd, parts[:-1]) as parent_fd:
            _rename_entry(staged_root_fd, str(record["staged"]), parent_fd, parts[-1], artifact=artifact)
            os.chmod(parts[-1], int(recorded_file.get("mode") or BACKUP_FILE_MODE), dir_fd=parent_fd)
            os.fsync(parent_fd)
        staged.append(relative)
    for record in sorted(directories, key=lambda item: len(item["parts"]), reverse=True):
        parts = tuple(record["parts"])
        relative = "/".join(parts)
        recorded_directory = descendants.get(relative)
        if recorded_directory is None or recorded_directory.get("kind") != "directory":
            raise _rollback_failure(
                ROLLBACK_CODE_IDENTITY_MISSING,
                f"the backup of {artifact} holds the directory {relative}, which the run's own "
                "report does not record as a directory",
            )
        with _run_directory_chain(staged_root_fd, parts) as leaf_fd:
            os.fchmod(leaf_fd, int(recorded_directory.get("mode") or BACKUP_DIRECTORY_MODE))
        staged.append(relative)
    return staged


def _restore_prestate(
    run_dir: Path, step: RollbackStep, restore_fd: int, target_parent_fd: int, target: Path
) -> dict[str, Any]:
    """DETAIL 10.6 steps 5-7: restore ONE recorded pre-state from the retained backup.

    A regular file and a directory are copied from the backup copy into a UNIQUE
    run-owned ``restore/`` staging entry (verified THERE, at 0600/0700 while it is
    staged) and only then renamed into the vacant target; a prior symlink is created at
    the staging name with the EXACT raw ``readlink`` string the run recorded and
    renamed in the same way; a recorded absence leaves the target vacant. The backup
    copy is only ever READ, and both parents of the final rename are fsynced.

    S2-08 fix round 1 (review F1.3 / R1.3): EVERY failure path of every branch discards the
    ONE staging entry this attempt created (``_discard_staged_entry``), so a half-built
    ``restore/<artifact>-<hex>/stage-*`` entry is never left behind. Nothing else is
    removed, and a previous attempt's retained entry can never carry this attempt's name.
    """
    artifact = step.decision.artifact
    entry = step.prestate
    if entry is None or entry.get("kind") == "absent":
        return {"artifact": artifact, "kind": "absent", "restored": False, "staging": None}
    kind = str(entry.get("kind"))
    backup_path = _rollback_backup_location(run_dir, entry, artifact)
    staged_name = _rollback_entry_name(artifact, unique=True)
    if kind == "regular_file":
        try:
            digest = ""
            source_size = -1
            try:
                with storage_lock.open_directory_nofollow(backup_path.parent) as backup_parent_fd:
                    before = os.stat(backup_path.name, dir_fd=backup_parent_fd, follow_symlinks=False)
                    with _pinned_child_regular_file(
                        backup_parent_fd, backup_path.name, artifact=f"rollback:{artifact}", before=before
                    ) as (descriptor, opened):
                        digest, _staged = _stage_regular_file(
                            descriptor, opened, restore_fd, staged_name, artifact=f"rollback:{artifact}"
                        )
                        source_size = opened.st_size
            except storage_lock.StorageLockError as exc:
                raise _rollback_failure(
                    ROLLBACK_CODE_BACKUP_MISMATCH,
                    f"the retained backup of {artifact} cannot be read ({exc.code})",
                ) from exc
            if digest != entry.get("sha256") or source_size != entry.get("size"):
                raise _rollback_failure(
                    ROLLBACK_CODE_BACKUP_MISMATCH,
                    f"the staged restore of {artifact} does not match the digest the run recorded",
                )
            os.chmod(staged_name, int(entry.get("mode") or BACKUP_FILE_MODE), dir_fd=restore_fd)
            os.fsync(restore_fd)
            _rename_entry(restore_fd, staged_name, target_parent_fd, target.name, artifact=artifact)
            _fsync_pair(target_parent_fd, restore_fd)
        except BaseException:
            _discard_staged_entry(restore_fd, staged_name)
            raise
        return {"artifact": artifact, "kind": kind, "restored": True, "staging": staged_name}
    if kind == "directory":
        try:
            with _run_child_directory(restore_fd, staged_name) as staged_root_fd:
                staged_identity = os.fstat(staged_root_fd)
                entries = _stage_restored_tree(
                    backup_path, staged_root_fd, artifact=artifact, descendants=step.descendants
                )
            os.chmod(staged_name, int(entry.get("mode") or BACKUP_DIRECTORY_MODE), dir_fd=restore_fd)
            os.fsync(restore_fd)
            _rename_entry(restore_fd, staged_name, target_parent_fd, target.name, artifact=artifact)
            _fsync_pair(target_parent_fd, restore_fd)
        except BaseException:
            _discard_staged_entry(restore_fd, staged_name)
            raise
        return {
            "artifact": artifact,
            "kind": kind,
            "restored": True,
            "staging": staged_name,
            "staged_inode": staged_identity.st_ino,
            "entries": len(entries),
        }
    if kind == "symlink":
        raw = entry.get("raw_link_target")
        if not isinstance(raw, str):
            raise _rollback_failure(
                ROLLBACK_CODE_IDENTITY_MISSING,
                f"the run's record of {artifact} carries no raw link string",
            )
        try:
            retained = os.readlink(backup_path)
        except OSError as exc:
            raise _rollback_failure(
                ROLLBACK_CODE_BACKUP_MISMATCH,
                f"the retained link entry of {artifact} cannot be read",
            ) from exc
        if retained != raw:
            raise _rollback_failure(
                ROLLBACK_CODE_BACKUP_MISMATCH,
                f"the retained link entry of {artifact} does not carry the raw string the run recorded",
            )
        try:
            os.symlink(raw, staged_name, dir_fd=restore_fd)
        except OSError as exc:
            raise _rollback_failure(
                ROLLBACK_CODE_VERIFY,
                f"the restore staging link of {artifact} cannot be created",
            ) from exc
        try:
            os.fsync(restore_fd)
            if os.readlink(staged_name, dir_fd=restore_fd) != raw:
                raise _rollback_failure(
                    ROLLBACK_CODE_VERIFY,
                    f"the restore staging link of {artifact} does not carry the raw string it must",
                )
            _rename_entry(restore_fd, staged_name, target_parent_fd, target.name, artifact=artifact)
            _fsync_pair(target_parent_fd, restore_fd)
        except BaseException:
            _discard_staged_entry(restore_fd, staged_name)
            raise
        return {"artifact": artifact, "kind": kind, "restored": True, "staging": staged_name}
    raise _rollback_failure(
        ROLLBACK_CODE_IDENTITY_MISSING,
        f"the run recorded no restorable kind for {artifact}: {kind!r}",
    )


def _verify_restored(step: RollbackStep, observation: Mapping[str, Any], target: Path) -> dict[str, Any]:
    """DETAIL 10.6 step 8: verify what was restored, or refuse with all copies kept.

    A restored regular file is reopened no-follow and must carry the recorded size,
    streamed SHA-256 and mode; a restored directory must be the very entry that was
    staged (its identity -- with its files already digest-bound above); a restored
    symlink is verified by ``lstat`` + ``readlink`` ONLY, so its referent is neither
    opened nor validated as a store. Any mismatch is ``E_ROLLBACK_VERIFY`` and
    nothing is deleted, because DETAIL 10.6 step 10 retains every copy and forbids a
    destructive automatic retry.
    """
    artifact = step.decision.artifact
    entry = step.prestate or {}
    kind = str(entry.get("kind"))
    observed = _rollback_target_identity(target.parent, target.name, artifact=artifact)
    if kind == "directory":
        if observed.kind != "directory" or observed.inode != observation.get("staged_inode"):
            raise _rollback_failure(
                ROLLBACK_CODE_VERIFY,
                f"the restored {artifact} directory is not the entry that was staged and verified",
            )
        return {"artifact": artifact, "kind": kind, "inode": observed.inode, "mode": observed.mode}
    if not _rollback_restored_matches(observed, entry):
        raise _rollback_failure(
            ROLLBACK_CODE_VERIFY,
            f"the restored {artifact} does not match the pre-state the run recorded",
        )
    return {
        "artifact": artifact,
        "kind": kind,
        "size": observed.size,
        "digest": observed.sha256 or "",
    }


def _execute_rollback(
    manifest_path: Path,
    manifest: MigrationManifest,
    run_dir: Path,
    steps: tuple[RollbackStep, ...],
) -> MigrationManifest:
    """Perform the decided rollback and record its own durable chain (DETAIL 10.6).

    Order: ``rollback.locked``; quarantine every positively identified run-created
    entry into its unique diagnostic quarantine; ``rollback.current_quarantined``;
    restore every recorded pre-state from the retained backup through a freshly
    verified staging entry; ``rollback.prestate_restored``; verify what was restored;
    ``rollback.verified``; ``rolled_back`` with the terminal status. On ANY failure
    once the chain has started the copies are retained and `rollback_failed` is
    recorded, then the original error is re-raised -- no destructive automatic retry
    happens anywhere in this function.
    """
    _append_rollback_event(
        manifest_path,
        ROLLBACK_EVENT_LOCKED,
        {"run_id": manifest.run_id, "artifacts": [step.decision.artifact for step in steps]},
    )
    current = ROLLBACK_EVENT_LOCKED
    try:
        with storage_lock.open_directory_nofollow(run_dir) as run_fd:
            with _run_child_directory(run_fd, ROLLBACK_RESTORE_DIRECTORY_NAME) as restore_fd:
                current = ROLLBACK_EVENT_CURRENT_QUARANTINED
                quarantined: list[dict[str, Any]] = []
                for step in steps:
                    if step.decision.action != ROLLBACK_ACTION_QUARANTINE_AND_RESTORE:
                        continue
                    target = Path(step.decision.target_path)
                    if step.recorded_identity is None:
                        raise _rollback_failure(
                            ROLLBACK_CODE_IDENTITY_MISSING,
                            f"{step.decision.artifact} has no recorded identity to quarantine",
                        )
                    with storage_lock.open_directory_nofollow(target.parent) as target_parent_fd:
                        quarantined.append(
                            {
                                "artifact": step.decision.artifact,
                                "path": _quarantine_current_rollback(
                                    run_fd,
                                    target_parent_fd,
                                    target,
                                    step.decision.artifact,
                                    step.recorded_identity,
                                ),
                            }
                        )
                _append_rollback_event(
                    manifest_path, ROLLBACK_EVENT_CURRENT_QUARANTINED, {"quarantined": quarantined}
                )
                current = ROLLBACK_EVENT_PRESTATE_RESTORED
                restorations: list[tuple[RollbackStep, dict[str, Any]]] = []
                for step in steps:
                    if step.decision.action == ROLLBACK_ACTION_NONE:
                        continue
                    target = Path(step.decision.target_path)
                    # S2-08 fix round 1 (review F1.2 / R1.2): the restore call runs INSIDE an
                    # `open_directory_nofollow` body, and that context manager re-wraps an
                    # escaping `OSError` into `StorageLockError('storage lock failure')` while
                    # unwinding -- which is how the directory-leg defect reached the durable
                    # record as a MASKED cause. A raw `OSError` (or the wrapper it becomes) is
                    # therefore mapped HERE into this card's own cause-specific refusal family,
                    # with the underlying code and the wrapped cause class in the message.
                    # This card's own `_rollback_failure` refusals are `ValueError`s and pass
                    # through untouched, so every existing cause-specific code is preserved.
                    try:
                        with storage_lock.open_directory_nofollow(target.parent) as target_parent_fd:
                            restorations.append(
                                (step, _restore_prestate(run_dir, step, restore_fd, target_parent_fd, target))
                            )
                    except storage_lock.StorageLockError as exc:
                        cause = exc.__cause__
                        underlying = getattr(exc, "code", "") or "storage lock failure"
                        raise _rollback_failure(
                            ROLLBACK_CODE_RESTORE_FAILED,
                            f"the restore of {step.decision.artifact} failed: {underlying}"
                            + (f" (from {type(cause).__name__})" if cause is not None else ""),
                        ) from exc
                    except OSError as exc:
                        raise _rollback_failure(
                            ROLLBACK_CODE_RESTORE_FAILED,
                            f"the restore of {step.decision.artifact} failed: {type(exc).__name__} "
                            f"{exc.strerror or exc}",
                        ) from exc
                _append_rollback_event(
                    manifest_path,
                    ROLLBACK_EVENT_PRESTATE_RESTORED,
                    {"restored": [observation for _step, observation in restorations]},
                )
                current = ROLLBACK_EVENT_VERIFIED
                verified = [
                    _verify_restored(step, observation, Path(step.decision.target_path))
                    for step, observation in restorations
                ]
                _append_rollback_event(manifest_path, ROLLBACK_EVENT_VERIFIED, {"verified": verified})
    except BaseException as exc:
        _record_rollback_failure(manifest_path, str(exc).split(":", 1)[0], current, str(exc), exc=exc)
        raise
    return _append_rollback_event(
        manifest_path,
        ROLLBACK_EVENT_ROLLED_BACK,
        {"artifacts": [step.decision.artifact for step in steps]},
        status="rolled_back",
    )


def rollback_profile_migration(
    manifest_path: Path, request: MigrationRequest
) -> MigrationManifest:
    """Restore a stopped run's exact pre-state, retaining every copy (DETAIL 10.6).

    Rollback is an ACTION entrypoint, so it repeats EVERYTHING itself: the shared
    ``validate_mutation_preconditions`` set (intent, exact canonical target, the
    bounded attestation, a FRESH replan, the sidecar/path/disk checks recomputed from
    it, the ``/proc`` writer inventory, the identity-and-hash stability interval and
    the bounded SQLite transaction probe), then the run's own durable records -- run id
    (``E_ROLLBACK_RUN_MISMATCH``), source identity, the tolerated run-path collision,
    and the backup report with every recorded entry (a mismatch is
    ``E_ROLLBACK_BACKUP_MISMATCH``) -- then DECIDES read-only, so a refusal has no
    filesystem side effect at all, and only then takes the SAME maintenance locks the
    engine takes, so both destructive renames -- and the no-follow re-observation that
    precedes each of them -- happen under the run's own locks.

    DELIBERATE DEVIATION FROM THE RESUME SET, disclosed: the ``config_digest``
    re-confirmation of DETAIL 10.5 is NOT repeated here. DETAIL 10.6's guard list (its
    steps 1-2) is manifest/hash chain, backup hashes, apply flag, exact target, stop
    attestation and all locks/probes, and the digest is not among them; more
    importantly, the run's OWN publication legitimately invalidates it in the legacy
    layout, where the redacted layout of a symlinked vector store carries
    ``compatibility: ["legacy-split-layout:vector"]`` and
    ``unavailable_projections: ["vector"]`` and the publication replaces that symlink
    with a real staged directory. Measured on this tree with
    ``S2-08_EVIDENCE/gate_tmp/s208_probe_config_digest.py``, which builds two roots at
    DIFFERENT paths (``…/s208-probe-digest-…/real`` and ``…/link``) and measures
    ``CONFIG_DIGEST_EQUAL=False``: that probe therefore does NOT by itself isolate the
    symlink-vs-directory cause, because the path difference alone could explain it, and the
    cross-provider review's own SAME-ROOT probe with byte-identical paths
    (``SAME_ROOT_CONFIG_DIGEST_EQUAL=False``, ``PATHS_IDENTICAL=True``) is what confirms the
    substance. S2-08 fix round 1 (review F6): this docstring said "for two roots that differ
    only in that respect", which described the probe inaccurately. A rollback that refused
    on the digest could never roll back the very layout acceptance 4 names, so the source
    identity is still re-confirmed and the digest is not.

    Before anything moves, the CURRENT TARGET IDENTITY of every publication artifact
    is checked no-follow against the identity the run's own publication recorded, and
    the recorded pre-state against the backup report -- including, since S2-08 fix round 1
    (review F4), the retained DIRECTORY backup copies compared with the run's own recorded
    entry set, so an ADDED unrecorded entry is refused there instead of after the
    quarantine; this card's Stops are raised there (unsafe parent; target drift, including
    a record that claims a publication the disk does not show; and a missing durable
    identity, which is REFUSED rather than inferred from the fresh plan). Only then does
    the rollback quarantine the run-created current artifact into a unique diagnostic
    quarantine, copy the recorded pre-state from the retained backup into a unique verified
    staging entry and rename it into the vacant target, fsync the parents, verify the
    restored entries and append the ``rollback.locked -> rollback.current_quarantined ->
    rollback.prestate_restored -> rollback.verified -> rolled_back`` chain with
    ``status=rolled_back``. A failure of the RESTORE step itself (an `OSError` escaping it,
    or the `StorageLockError` the storage layer wraps it into) is reported as
    ``E_ROLLBACK_RESTORE_FAILED`` with the underlying cause class, and the ONE staging entry
    that attempt created is removed again (S2-08 fix round 1, review F1.2/F1.3).

    A run whose own record already reports a completed rollback is refused
    (``E_ROLLBACK_TERMINAL``): this entrypoint is a restore, never a status no-op, and
    a terminal run is not silently re-processed. On any failure once the chain has
    started, every copy is retained, ``rollback_failed`` and the stable cause are
    recorded in the same manifest write -- together with the exception CLASS of the
    failure's root cause (S2-08 fix round 1, review F5) -- and the error is raised; there
    is no automatic destructive retry, and a refused rollback that never moved anything
    leaves the manifest byte-identical.
    """
    validate_mutation_preconditions(request)
    manifest = _load_resume_manifest(manifest_path)
    if manifest.run_id != request.run_id:
        raise _rollback_failure(ROLLBACK_CODE_RUN_MISMATCH, "the manifest belongs to another run id")
    if manifest.status == "rolled_back":
        raise _rollback_failure(
            ROLLBACK_CODE_TERMINAL,
            "the run's own durable record already reports a completed rollback",
        )
    plan = plan_profile_migration(request)
    _require_resume_source_identity(plan, manifest)
    _require_resume_plan(plan)
    run_dir = _backup_run_directory(plan)
    entries = _rollback_backup_entries(run_dir, manifest)
    # The read-only decision runs BEFORE the locks on purpose: the lock stage CREATES
    # its lock entries and by design never unlinks them, so a refusal that decided
    # first has NO filesystem side effect at all -- which is the property this card's
    # refusal path owes. Nothing is weakened by the order: the lock stage re-runs the
    # ENTIRE mixed-version writer inventory under the acquisition, and every entry that
    # is about to move is re-observed no-follow immediately before its rename, inside
    # the locked section.
    steps = _observe_rollback_steps(plan, manifest, entries)
    locks = acquire_maintenance_locks(plan)
    try:
        return _execute_rollback(manifest_path, manifest, run_dir, steps)
    finally:
        locks.release()
