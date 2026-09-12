"""Pure, no-follow storage layout resolution for CMMS.

Resolution is lexical and strictly read-only: this module never creates a
directory, opens a lock or database, copies, renames, deletes, performs a
network call or constructs a provider. Symlinks are inspected with ``lstat``
and are never followed.
"""
from __future__ import annotations

import os
import stat
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Mapping, cast
from urllib.parse import urlsplit

StorageMode = Literal["profile", "shared", "standalone"]
StoreKind = Literal["sqlite", "lancedb", "qdrant_remote", "qdrant_memory", "graph"]
ArtifactKind = Literal["absent", "regular_file", "directory", "symlink", "special"]
OriginKind = Literal["default", "settings", "yaml", "env", "legacy_env"]

_DEFAULT_SQLITE_URL = "sqlite+aiosqlite:///data/memory.db"
_AIOSQLITE_PREFIX = "sqlite+aiosqlite:///"
_SQLITE_PREFIX = "sqlite:///"


class StorageLayoutError(ValueError):
    """Typed layout failure carrying a stable DETAIL diagnostic code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class ValueOrigin:
    kind: OriginKind
    key: str | None = None


@dataclass(frozen=True)
class ComponentIdentity:
    lexical_path: str
    kind: ArtifactKind
    device: int | None = None
    inode: int | None = None
    mode: int | None = None
    uid: int | None = None
    gid: int | None = None
    raw_link_target: str | None = None


@dataclass(frozen=True)
class SQLiteLocation:
    configured_url: str
    effective_url: str
    local_path: Path | None
    kind: Literal["file", "memory", "file_uri", "remote"]
    query: str
    origin: ValueOrigin


@dataclass(frozen=True)
class VectorLocation:
    backend: Literal["lancedb", "qdrant"]
    local_path: Path | None
    qdrant_location: str | None
    collection: str
    kind: Literal["local", "remote", "memory"]
    origin: ValueOrigin


@dataclass(frozen=True)
class StorageResolutionInputs:
    mode: StorageMode = "profile"
    profile_home: str | Path | None = None
    data_root: str | Path | None = None
    sqlite_url: str = _DEFAULT_SQLITE_URL
    vector_backend: str = "lancedb"
    lancedb_path: str | Path = "data/lancedb"
    graph_snapshot_path: str | Path = "data/graph.json"
    qdrant_location: str = ":memory:"
    vector_collection: str = "memories"
    origins: Mapping[str, ValueOrigin] = field(default_factory=dict)
    legacy_split_compat: bool = True


@dataclass(frozen=True)
class StorageLayout:
    mode: StorageMode
    profile_home: Path | None
    data_root: Path
    sqlite: SQLiteLocation
    vector: VectorLocation
    graph_snapshot_path: Path
    graph_lock_path: Path
    root_lock_path: Path
    origins: Mapping[str, ValueOrigin]
    compatibility: tuple[str, ...] = ()
    unavailable_projections: frozenset[str] = frozenset()


def _lexical(value: str | Path) -> Path:
    """Return the absolute lexical form of ``value`` (never dereferences links)."""
    text = os.path.expanduser(str(value))
    if not text or "\x00" in text:
        raise StorageLayoutError("E_PATH_OUTSIDE_ROOT", "blank or NUL path")
    return Path(os.path.abspath(os.path.normpath(text)))


def classify_artifact_nofollow(path: Path) -> ArtifactKind:
    """Classify a path with ``lstat`` only; never follow a final symlink."""
    try:
        mode = os.lstat(path).st_mode
    except FileNotFoundError:
        return "absent"
    if stat.S_ISLNK(mode):
        return "symlink"
    if stat.S_ISREG(mode):
        return "regular_file"
    if stat.S_ISDIR(mode):
        return "directory"
    return "special"


def _identity(path: Path) -> ComponentIdentity:
    try:
        status = os.lstat(path)
    except FileNotFoundError:
        return ComponentIdentity(str(path), "absent")
    kind = classify_artifact_nofollow(path)
    return ComponentIdentity(
        str(path),
        kind,
        status.st_dev,
        status.st_ino,
        status.st_mode,
        getattr(status, "st_uid", None),
        getattr(status, "st_gid", None),
        os.readlink(path) if kind == "symlink" else None,
    )


def inspect_component_chain_nofollow(
    path: Path, *, anchor: Path
) -> tuple[ComponentIdentity, ...]:
    """Inspect every existing component from the filesystem root to ``path``.

    Any symlink or non-directory component below ``anchor`` is fatal; missing
    tail components are recorded but never created.
    """
    path, anchor = _lexical(path), _lexical(anchor)
    try:
        relative = path.relative_to(anchor)
    except ValueError as exc:
        raise StorageLayoutError(
            "E_PATH_OUTSIDE_ROOT", f"path outside approved root: {path}"
        ) from exc
    result = []
    current = anchor
    for part in ("/",) + anchor.parts[1:]:
        current = Path(part) if current == Path("/") else current / part
        result.append(_identity(current))
    for part in relative.parts:
        current = current / part
        identity = _identity(current)
        result.append(identity)
        if current == path:
            continue
        if identity.kind == "symlink":
            raise StorageLayoutError("E_PATH_SYMLINK_PARENT", str(current))
        if identity.kind not in ("directory", "absent"):
            raise StorageLayoutError("E_PATH_SPECIAL_FILE", str(current))
    return tuple(result)


def _under(root: Path, candidate: Path) -> bool:
    try:
        return os.path.commonpath((str(root), str(candidate))) == str(root)
    except ValueError:
        return False


def _store_path(
    raw: str | Path, root: Path, *, final_symlink_compat: bool = False
) -> tuple[Path, bool]:
    raw_text = str(raw)
    candidate = _lexical(raw_text if os.path.isabs(raw_text) else root / raw_text)
    if not _under(root, candidate):
        raise StorageLayoutError("E_PATH_OUTSIDE_ROOT", str(candidate))
    inspect_component_chain_nofollow(candidate, anchor=root)
    final = classify_artifact_nofollow(candidate)
    if final == "symlink":
        if final_symlink_compat:
            return candidate, True
        raise StorageLayoutError("E_PATH_FINAL_SYMLINK_UNSAFE", str(candidate))
    if final == "special":
        raise StorageLayoutError("E_PATH_SPECIAL_FILE", str(candidate))
    return candidate, False


def resolve_sqlite_location(
    raw_url: str, *, data_root: Path, origin: ValueOrigin
) -> SQLiteLocation:
    """Resolve one SQLite URL against ``data_root`` without filesystem writes."""
    raw_url = raw_url or _DEFAULT_SQLITE_URL
    if not raw_url.lower().startswith("sqlite"):
        return SQLiteLocation(raw_url, raw_url, None, "remote", "", origin)
    if raw_url.startswith(_AIOSQLITE_PREFIX):
        prefix = _AIOSQLITE_PREFIX
    elif raw_url.startswith(_SQLITE_PREFIX):
        prefix = _SQLITE_PREFIX
    else:
        kind = "memory" if ":memory:" in raw_url else "file_uri"
        return SQLiteLocation(raw_url, raw_url, None, kind, urlsplit(raw_url).query, origin)
    segment = raw_url[len(prefix):]
    if segment in (":memory:", "", "/:memory:"):
        return SQLiteLocation(raw_url, raw_url, None, "memory", "", origin)
    path_part, _, query = segment.partition("?")
    is_absolute = path_part.startswith("/")
    local_path = _lexical(path_part if is_absolute else data_root / path_part)
    effective = f"{prefix}{local_path}" + (f"?{query}" if query else "")
    return SQLiteLocation(raw_url, effective, local_path, "file", query, origin)


def resolve_storage_layout(inputs: StorageResolutionInputs) -> StorageLayout:
    """Resolve the immutable storage layout; performs no I/O."""
    mode = inputs.mode
    home = _lexical(inputs.profile_home) if inputs.profile_home is not None else None
    if mode == "profile":
        if home is None:
            raise StorageLayoutError(
                "E_HERMES_HOME_REQUIRED", "profile mode requires hermes_home"
            )
        raw_root = inputs.data_root
        if raw_root is None or str(raw_root).strip() in ("", "."):
            root = home
        elif os.path.isabs(str(raw_root)):
            root = _lexical(raw_root)
        else:
            root = _lexical(home / str(raw_root))
        if not _under(home, root):
            raise StorageLayoutError("E_PROFILE_ROOT_EXTERNAL", str(root))
    elif mode == "shared":
        if inputs.data_root is None or not os.path.isabs(str(inputs.data_root)):
            raise StorageLayoutError(
                "E_SHARED_ROOT_REQUIRED", "shared mode requires absolute data_root"
            )
        root = _lexical(inputs.data_root)
    else:
        root = _lexical(inputs.data_root) if inputs.data_root is not None else Path.cwd()
    if classify_artifact_nofollow(root) not in ("directory", "absent"):
        raise StorageLayoutError("E_PATH_SYMLINK_PARENT", str(root))

    sqlite = resolve_sqlite_location(
        inputs.sqlite_url,
        data_root=root,
        origin=inputs.origins.get("sqlite", ValueOrigin("default")),
    )
    if sqlite.local_path is not None and not _under(root, sqlite.local_path):
        if mode == "shared":
            raise StorageLayoutError("E_SHARED_SPLIT_LAYOUT", str(sqlite.local_path))
        raise StorageLayoutError("E_PROFILE_ROOT_EXTERNAL", str(sqlite.local_path))

    vector_path: Path | None = None
    unavailable: set[str] = set()
    compatibility: list[str] = []
    if inputs.vector_backend == "lancedb":
        vector_path, linked = _store_path(
            inputs.lancedb_path, root, final_symlink_compat=inputs.legacy_split_compat
        )
        if linked:
            compatibility.append("legacy-split-layout:vector")
            unavailable.add("vector")
    if inputs.vector_backend == "qdrant":
        vector_kind: Literal["local", "remote", "memory"] = (
            "memory" if inputs.qdrant_location == ":memory:" else "remote"
        )
    else:
        vector_kind = "local"
    vector = VectorLocation(
        cast("Literal['lancedb', 'qdrant']", inputs.vector_backend),
        vector_path,
        inputs.qdrant_location if inputs.vector_backend == "qdrant" else None,
        inputs.vector_collection,
        vector_kind,
        inputs.origins.get("vector", ValueOrigin("settings")),
    )
    graph, linked = _store_path(
        inputs.graph_snapshot_path, root, final_symlink_compat=inputs.legacy_split_compat
    )
    if linked:
        compatibility.append("legacy-split-layout:graph")
        unavailable.add("graph")
    if mode == "shared" and any(
        entry.startswith("legacy-split") for entry in compatibility
    ):
        raise StorageLayoutError("E_SHARED_SPLIT_LAYOUT", "shared layout contains split artifact")
    return StorageLayout(
        mode,
        home,
        root,
        sqlite,
        vector,
        graph,
        graph.with_name(graph.name + ".lock"),
        root / ".cmms-storage.lock",
        dict(inputs.origins),
        tuple(compatibility),
        frozenset(unavailable),
    )


def serialize_layout_redacted(layout: StorageLayout) -> dict[str, object]:
    """Serialize a layout with no credentials or memory content."""
    return {
        "mode": layout.mode,
        "profile_home": str(layout.profile_home) if layout.profile_home else None,
        "data_root": str(layout.data_root),
        "sqlite": str(layout.sqlite.local_path or layout.sqlite.effective_url),
        "vector": str(layout.vector.local_path or layout.vector.qdrant_location),
        "graph": str(layout.graph_snapshot_path),
        "compatibility": list(layout.compatibility),
        "unavailable_projections": sorted(layout.unavailable_projections),
    }


def validate_write_target(layout: StorageLayout, path: Path) -> None:
    """Validate a write target against the frozen layout (no I/O, no writes)."""
    candidate = _lexical(path)
    if not _under(layout.data_root, candidate):
        raise StorageLayoutError("E_PATH_OUTSIDE_ROOT", str(candidate))
    inspect_component_chain_nofollow(candidate, anchor=layout.data_root)
    if classify_artifact_nofollow(candidate) == "symlink":
        raise StorageLayoutError("E_PATH_FINAL_SYMLINK_UNSAFE", str(candidate))


def cmms_repo_root() -> Path:
    """Return the CMMS installation checkout (import path, never a data base)."""
    return Path(__file__).resolve().parents[2]
