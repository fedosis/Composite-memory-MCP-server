"""Pure, lexical and no-follow storage layout resolution for CMMS."""

from __future__ import annotations

import os
import stat
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Literal, Mapping, cast
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

StorageMode = Literal["profile", "shared", "standalone"]
StoreKind = Literal["sqlite", "lancedb", "qdrant_remote", "qdrant_memory", "graph"]
ArtifactKind = Literal["absent", "regular_file", "directory", "symlink", "special"]
OriginKind = Literal["default", "settings", "yaml", "env", "legacy_env"]

_DEFAULT_SQLITE_URL = "sqlite+aiosqlite:///data/memory.db"


class StorageLayoutError(ValueError):
    """A layout failure with a stable diagnostic code."""

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
    nlink: int | None = None


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
    installation_path: str | Path | None = None


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
    text = os.path.expanduser(str(value))
    if not text.strip() or "\x00" in text:
        raise StorageLayoutError("E_PATH_OUTSIDE_ROOT", "blank or NUL path")
    return Path(os.path.abspath(os.path.normpath(text)))


def _kind(mode: int) -> ArtifactKind:
    if stat.S_ISLNK(mode):
        return "symlink"
    if stat.S_ISREG(mode):
        return "regular_file"
    if stat.S_ISDIR(mode):
        return "directory"
    return "special"


def _component(path: Path, info: os.stat_result, raw: str | None = None) -> ComponentIdentity:
    return ComponentIdentity(
        str(path),
        _kind(info.st_mode),
        info.st_dev,
        info.st_ino,
        info.st_mode,
        info.st_uid,
        info.st_gid,
        raw,
        info.st_nlink,
    )


def inspect_component_chain_nofollow(path: Path, *, anchor: Path) -> tuple[ComponentIdentity, ...]:
    """Inspect entries through pinned no-follow parent descriptors, never referents.

    Absent tails are recorded without I/O. The final link entry may be inventoried;
    no caller may use this read-only snapshot as authorization for a later write.
    """
    candidate, approved = _lexical(path), _lexical(anchor)
    if not _under(approved, candidate):
        raise StorageLayoutError("E_PATH_OUTSIDE_ROOT", str(candidate))
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    fd = os.open("/", flags)
    current = Path("/")
    identities = [_component(current, os.fstat(fd))]
    try:
        parts = candidate.parts[1:]
        for index, name in enumerate(parts):
            current /= name
            try:
                info = os.stat(name, dir_fd=fd, follow_symlinks=False)
            except FileNotFoundError:
                identities.append(ComponentIdentity(str(current), "absent"))
                for missing in parts[index + 1 :]:
                    current /= missing
                    identities.append(ComponentIdentity(str(current), "absent"))
                break
            kind = _kind(info.st_mode)
            raw = os.readlink(name, dir_fd=fd) if kind == "symlink" else None
            identity = _component(current, info, raw)
            identities.append(identity)
            final = index == len(parts) - 1
            if kind == "symlink":
                after = os.stat(name, dir_fd=fd, follow_symlinks=False)
                if (info.st_dev, info.st_ino, info.st_mode) != (after.st_dev, after.st_ino, after.st_mode):
                    raise StorageLayoutError("E_ARTIFACT_IDENTITY_CHANGED", str(current))
                if not final:
                    raise StorageLayoutError("E_PATH_SYMLINK_PARENT", str(current))
            if final:
                break
            if kind != "directory":
                raise StorageLayoutError("E_PATH_SPECIAL_FILE", str(current))
            child = os.open(name, flags, dir_fd=fd)
            try:
                after = os.fstat(child)
                if (info.st_dev, info.st_ino, info.st_mode) != (after.st_dev, after.st_ino, after.st_mode):
                    raise StorageLayoutError("E_ARTIFACT_IDENTITY_CHANGED", str(current))
            except BaseException:
                os.close(child)
                raise
            os.close(fd)
            fd = child
    except OSError as exc:
        raise StorageLayoutError("E_ARTIFACT_IDENTITY_CHANGED", "no-follow inspection failed") from exc
    finally:
        os.close(fd)
    return tuple(identities)


def classify_artifact_nofollow(path: Path) -> ArtifactKind:
    return inspect_component_chain_nofollow(path, anchor=Path("/"))[-1].kind


def _under(root: Path, candidate: Path) -> bool:
    try:
        return os.path.commonpath((str(root), str(candidate))) == str(root)
    except ValueError:
        return False


def _inspect_store(path: Path, *, directory: bool = False, allow_link: bool = False) -> bool:
    identity = inspect_component_chain_nofollow(path, anchor=Path("/"))[-1]
    if identity.kind == "symlink":
        if allow_link:
            return True
        raise StorageLayoutError("E_PATH_FINAL_SYMLINK_UNSAFE", str(path))
    if identity.kind not in ("absent", "directory" if directory else "regular_file"):
        raise StorageLayoutError("E_PATH_SPECIAL_FILE", str(path))
    if identity.kind == "regular_file" and identity.nlink != 1:
        raise StorageLayoutError("E_PATH_HARDLINK_UNSAFE", str(path))
    return False


def _candidate(raw: str | Path, root: Path) -> tuple[Path, bool]:
    expanded = os.path.expanduser(str(raw))
    if not expanded.strip() or "\x00" in expanded:
        raise StorageLayoutError("E_PATH_OUTSIDE_ROOT", "blank or NUL path")
    absolute = os.path.isabs(expanded)
    return _lexical(expanded if absolute else root / expanded), absolute


def resolve_sqlite_location(raw_url: str, *, data_root: Path, origin: ValueOrigin) -> SQLiteLocation:
    """Resolve SQLite path forms while preserving scheme and query text."""
    # An explicitly blank URL is the documented in-memory form.  The
    # dataclass default is applied only when the argument is omitted.
    configured = raw_url
    if configured == "":
        return SQLiteLocation(configured, configured, None, "memory", "", origin)
    if configured in ("sqlite://", "sqlite+aiosqlite://", ":memory:"):
        return SQLiteLocation(configured, configured, None, "memory", "", origin)
    prefixes = ("sqlite+aiosqlite:///", "sqlite:///")
    prefix = next((p for p in prefixes if configured.startswith(p)), None)
    if prefix is None:
        if configured.startswith("file:"):
            return SQLiteLocation(configured, configured, None, "file_uri", urlsplit(configured).query, origin)
        if not configured.lower().startswith("sqlite"):
            return SQLiteLocation(configured, configured, None, "remote", "", origin)
        return SQLiteLocation(configured, configured, None, "file_uri", urlsplit(configured).query, origin)
    segment = configured[len(prefix) :]
    path_part, separator, query = segment.partition("?")
    query_text = query if separator else ""
    query_pairs = dict(parse_qsl(query_text, keep_blank_values=True))
    if path_part.startswith("file:") or query_pairs.get("uri", "").lower() in {"1", "true", "yes"}:
        # SQLite URI filenames are opaque to the local-path resolver.  Keep
        # the configured spelling (not urlunsplit's slash normalization).
        return SQLiteLocation(configured, configured, None, "file_uri", query_text, origin)
    if path_part in ("", ":memory:", "/:memory:"):
        return SQLiteLocation(configured, configured, None, "memory", query_text, origin)
    # Three slashes encode a path relative to the chosen root. Four slashes
    # leave a leading slash in the database segment and are POSIX absolute.
    absolute = path_part.startswith("/")
    local = _lexical(path_part if absolute else data_root / path_part)
    effective = f"{prefix}{local}" + (f"?{query}" if separator else "")
    return SQLiteLocation(configured, effective, local, "file", query if separator else "", origin)


def resolve_storage_layout(inputs: StorageResolutionInputs) -> StorageLayout:
    """Freeze a complete layout after pure lexical/no-follow validation."""
    mode = inputs.mode
    if mode not in ("profile", "shared", "standalone"):
        raise StorageLayoutError("E_STORAGE_MODE_INVALID", f"unsupported storage mode: {mode!r}")
    home = None
    if inputs.profile_home is not None:
        if not str(inputs.profile_home).strip():
            raise StorageLayoutError("E_HERMES_HOME_REQUIRED", "profile_home is blank")
        home = _lexical(inputs.profile_home)
    if mode == "profile":
        if home is None or classify_artifact_nofollow(home) != "directory":
            raise StorageLayoutError("E_HERMES_HOME_REQUIRED", "profile home must be an existing real directory")
        raw = inputs.data_root
        root = home if raw is None or str(raw).strip() in ("", ".") else _candidate(raw, home)[0]
        if not _under(home, root):
            raise StorageLayoutError("E_PROFILE_ROOT_EXTERNAL", "external root requires explicit shared mode")
    elif inputs.data_root is None or not str(inputs.data_root).strip():
        if mode == "shared":
            raise StorageLayoutError("E_SHARED_ROOT_REQUIRED", "shared mode requires an absolute root")
        root = _lexical(Path.cwd())
    else:
        expanded = os.path.expanduser(str(inputs.data_root))
        if not os.path.isabs(expanded):
            code = "E_SHARED_ROOT_RELATIVE" if mode == "shared" else "E_STANDALONE_ROOT_RELATIVE"
            raise StorageLayoutError(code, "explicit root must be absolute")
        root = _lexical(expanded)
    if root == Path("/"):
        raise StorageLayoutError("E_FORBIDDEN_TARGET_ROOT", "filesystem root cannot own storage")
    root_kind = classify_artifact_nofollow(root)
    if root_kind == "symlink":
        raise StorageLayoutError("E_PATH_SYMLINK_PARENT", str(root))
    if root_kind not in ("directory", "absent"):
        raise StorageLayoutError("E_PATH_SPECIAL_FILE", str(root))

    compatibility: list[str] = []
    unavailable: set[str] = set()
    # Standalone without an explicit root retains legacy absolute per-store
    # Settings as well as CWD-relative defaults. Explicit roots require coherence.
    standalone_legacy = mode == "standalone" and inputs.data_root is None

    def store(label: str, path: Path, *, absolute: bool, directory: bool = False) -> Path:
        if not _under(root, path):
            if mode == "shared":
                raise StorageLayoutError("E_SHARED_SPLIT_LAYOUT", str(path))
            if not absolute or not (standalone_legacy or (mode == "profile" and inputs.legacy_split_compat)):
                raise StorageLayoutError("E_PATH_OUTSIDE_ROOT", str(path))
            compatibility.append(f"legacy-split-layout:{label}")
        linked = _inspect_store(
            path,
            directory=directory,
            allow_link=label in ("vector", "graph") and mode == "profile" and inputs.legacy_split_compat,
        )
        if linked:
            warning = f"legacy-split-layout:{label}"
            if warning not in compatibility:
                compatibility.append(warning)
            unavailable.add(label)
        return path

    sqlite = resolve_sqlite_location(
        inputs.sqlite_url,
        data_root=root,
        origin=inputs.origins.get("sqlite", ValueOrigin("default")),
    )
    if sqlite.local_path is not None:
        absolute_sql = inputs.sqlite_url.startswith(("sqlite:////", "sqlite+aiosqlite:////"))
        store("sqlite", sqlite.local_path, absolute=absolute_sql)
        # Runtime may legitimately use WAL. S1 only rejects unsafe entries;
        # clean-sidecar-only migration policy/probes remain in S2.
        for suffix in ("-wal", "-shm", "-journal"):
            _inspect_store(Path(str(sqlite.local_path) + suffix))
    elif sqlite.kind == "file_uri":
        compatibility.append("unsupported-file-uri:sqlite")

    backend = inputs.vector_backend
    if backend not in ("lancedb", "qdrant"):
        raise StorageLayoutError("E_VECTOR_BACKEND_INVALID", "unknown vector backend")
    vector_path = None
    qdrant = None
    vector_kind: Literal["local", "remote", "memory"] = "local"
    if backend == "lancedb":
        candidate, absolute = _candidate(inputs.lancedb_path, root)
        vector_path = store("vector", candidate, absolute=absolute, directory=True)
    else:
        qdrant = inputs.qdrant_location
        if not qdrant or not qdrant.strip() or "\x00" in qdrant:
            raise StorageLayoutError("E_QDRANT_PROFILE_NAMESPACE_UNDEFINED", "Qdrant location is required")
        vector_kind = "memory" if qdrant == ":memory:" else "remote"
        if mode == "profile" and vector_kind == "remote":
            raise StorageLayoutError("E_QDRANT_PROFILE_NAMESPACE_UNDEFINED", "remote Qdrant has no profile namespace")
        if vector_kind == "memory":
            compatibility.append("ephemeral-projection:vector")
    candidate, absolute = _candidate(inputs.graph_snapshot_path, root)
    graph = store("graph", candidate, absolute=absolute)
    graph_lock = graph.with_name(graph.name + ".lock")
    root_lock = root / ".cmms-storage.lock"
    _inspect_store(graph_lock)
    _inspect_store(root_lock)
    targets = [graph, graph_lock, root_lock]
    if vector_path is not None:
        targets.append(vector_path)
    if sqlite.local_path is not None:
        targets.extend(
            [sqlite.local_path, *(Path(str(sqlite.local_path) + suffix) for suffix in ("-wal", "-shm", "-journal"))]
        )
    for index, left in enumerate(targets):
        if left == root:
            raise StorageLayoutError("E_PATH_OVERLAP", "store cannot replace its root")
        for right in targets[index + 1 :]:
            if _under(left, right) or _under(right, left):
                raise StorageLayoutError("E_PATH_OVERLAP", "storage artifacts overlap")
    origins = MappingProxyType(dict(inputs.origins))
    vector_origin = origins.get(
        "lancedb_path" if backend == "lancedb" else "qdrant_location", origins.get("vector", ValueOrigin("default"))
    )
    vector = VectorLocation(
        cast("Literal['lancedb', 'qdrant']", backend),
        vector_path,
        qdrant,
        inputs.vector_collection,
        vector_kind,
        vector_origin,
    )
    return StorageLayout(
        mode,
        home,
        root,
        sqlite,
        vector,
        graph,
        graph_lock,
        root_lock,
        origins,
        tuple(compatibility),
        frozenset(unavailable),
    )


def _redact_url(url: str, *, redact_all_query: bool = False) -> str:
    parts = urlsplit(url)
    netloc = parts.hostname or ""
    if parts.port is not None:
        netloc += f":{parts.port}"
    query = []
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        query.append(
            (
                key,
                "<redacted>"
                if redact_all_query
                or any(word in key.lower() for word in ("token", "key", "secret", "password", "credential"))
                else value,
            )
        )
    return urlunsplit((parts.scheme, netloc, parts.path, urlencode(query), parts.fragment))


def serialize_layout_redacted(layout: StorageLayout) -> dict[str, object]:
    return {
        "mode": layout.mode,
        "profile_home": str(layout.profile_home) if layout.profile_home else None,
        "data_root": str(layout.data_root),
        "sqlite": _redact_url(layout.sqlite.effective_url),
        "vector": (
            _redact_url(layout.vector.qdrant_location, redact_all_query=True)
            if layout.vector.qdrant_location and layout.vector.kind == "remote"
            else str(layout.vector.local_path or layout.vector.qdrant_location)
        ),
        "graph": str(layout.graph_snapshot_path),
        "compatibility": list(layout.compatibility),
        "unavailable_projections": sorted(layout.unavailable_projections),
    }


def validate_write_target(layout: StorageLayout, path: Path) -> None:
    """Reinspect a candidate; legacy overrides authorize only exact store paths.

    This is not a file-open capability. Write-side callers must use the no-follow
    descriptor primitives rather than relying on this snapshot alone.
    """
    candidate = _lexical(path)
    if candidate == layout.data_root:
        raise StorageLayoutError("E_FORBIDDEN_TARGET_ROOT", str(candidate))
    if not _under(layout.data_root, candidate):
        permitted = {
            layout.sqlite.local_path,
            layout.vector.local_path,
            layout.graph_snapshot_path,
            layout.graph_lock_path,
        }
        if candidate not in permitted or not layout.compatibility or layout.mode == "shared":
            raise StorageLayoutError("E_PATH_OUTSIDE_ROOT", str(candidate))
    identity = inspect_component_chain_nofollow(candidate, anchor=Path("/"))[-1]
    if identity.kind == "symlink":
        raise StorageLayoutError("E_PATH_FINAL_SYMLINK_UNSAFE", str(candidate))
    if identity.kind == "special":
        raise StorageLayoutError("E_PATH_SPECIAL_FILE", str(candidate))
    if identity.kind == "regular_file" and identity.nlink != 1:
        raise StorageLayoutError("E_PATH_HARDLINK_UNSAFE", str(candidate))


def cmms_repo_root() -> Path:
    """Return the installation checkout for import compatibility only."""
    return Path(__file__).resolve().parents[2]
