"""Service JSON v1: load, validate, provision and fail-closed layout preflight.

The native owner-service is configured by a single operator-owned JSON file
(DETAIL §7.1). This module is deliberately the *only* place that knows the
file schema. It validates the whole document, provisions the stable
``store_id`` atomically, and resolves the storage layout through the existing
plugin config — without ever opening a store or creating ``data_root``.

Failure codes are stable diagnostics (never wire codes). Exactly one code per
failure class:

``E_CONFIG_READ``
    The file is not a private (owner-only) regular file.
``E_CONFIG_JSON``
    The body is not a JSON object, or a key repeats.
``E_CONFIG_UNKNOWN_KEY``
    A key outside the schema (top level, ``backend``, ``limits``, ``writer``).
``E_CONFIG_VERSION``
    ``version != 1``.
``E_CONFIG_PATH``
    Any path-shaped problem (relative, ``..``, outside ``data_root``,
    over-long socket path, unreadable storage context).
``E_CONFIG_ALLOW_HOMES``
    ``allow_homes`` is empty, relative, missing, not a directory, or repeats.
``E_CONFIG_BACKEND``
    Type/enum/range problem inside ``backend`` (paths stay ``E_CONFIG_PATH``).
``E_CONFIG_LIMITS``
    Type/range problem inside ``limits``.
``E_STORE_ID``
    ``store_id`` is not a canonical UUIDv4.
``E_LAYOUT_MISMATCH``
    The resolved layout disagrees with the service intent.
"""

from __future__ import annotations

import json
import os
import stat
import uuid
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

from memory_server.paths import (
    StorageLayout,
    StorageLayoutError,
    ValueOrigin,
    resolve_sqlite_location,
)
from memory_server.plugins.hermes.config import HermesPluginConfig

SERVICE_CONFIG_VERSION: int = 1

REQUIRED_KEYS: frozenset[str] = frozenset(
    {"version", "data_root", "storage_context_home", "socket_path", "allow_homes", "backend"}
)
OPTIONAL_KEYS: frozenset[str] = frozenset({"store_id", "limits"})

_BACKEND_KEYS: frozenset[str] = frozenset(
    {
        "storage_mode",
        "db_url",
        "vector_backend",
        "lancedb_path",
        "graph_snapshot_path",
        "vector_collection",
        "extraction_mode",
        "llm_model",
        "llm_base_url",
        "max_facts",
        "writer",
    }
)
_WRITER_KEYS: frozenset[str] = frozenset({"flush_interval", "max_batch"})
_VECTOR_BACKENDS: frozenset[str] = frozenset({"lancedb", "qdrant"})
_EXTRACTION_MODES: frozenset[str] = frozenset({"llm", "none"})
_SQLITE_PREFIX = "sqlite+aiosqlite://"
_MAX_SOCKET_BYTES = 107
_MIN_FRAME_BYTES = 1024
_MAX_FRAME_BYTES = 4 * 1024 * 1024


class ServiceConfigError(ValueError):
    """A service-config failure with a stable diagnostic code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class WriterLimits:
    flush_interval: float = 5.0
    max_batch: int = 50


@dataclass(frozen=True)
class Limits:
    frame_bytes: int = 4 * 1024 * 1024
    max_connections: int = 16
    max_operations: int = 16
    hello_timeout_s: float = 5.0
    read_idle_s: float = 5.0
    connect_timeout_s: float = 2.0
    tool_timeout_s: float = 60.0
    recall_timeout_s: float = 10.0
    enqueue_timeout_s: float = 5.0
    flush_timeout_s: float = 30.0
    live_turns: int = 64
    terminal_failed: int = 64
    pre_drain_s: float = 8.0


@dataclass(frozen=True)
class BackendConfig:
    storage_mode: str
    data_root: str
    db_url: str | None
    vector_backend: str
    lancedb_path: str | None
    graph_snapshot_path: str | None
    vector_collection: str
    extraction_mode: str | None
    llm_model: str | None
    llm_base_url: str | None
    max_facts: int
    writer: WriterLimits


@dataclass(frozen=True)
class ServiceConfig:
    path: Path
    version: int
    store_id: str
    data_root: Path
    storage_context_home: Path
    socket_path: Path
    allow_homes: tuple[Path, ...]
    backend: BackendConfig
    limits: Limits
    raw: Mapping[str, Any]


# --------------------------------------------------------------- read / parse
def _no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ServiceConfigError("E_CONFIG_JSON", f"duplicate JSON key: {key!r}")
        result[key] = value
    return result


def _read_document(path: Path) -> dict[str, Any]:
    """Read a private regular file and parse it as a JSON object."""
    candidate = Path(path)
    try:
        info = os.lstat(candidate)
    except OSError as exc:
        raise ServiceConfigError("E_CONFIG_READ", f"cannot stat service config {candidate}: {exc}") from exc
    if stat.S_ISLNK(info.st_mode):
        raise ServiceConfigError("E_CONFIG_READ", f"service config {candidate} is a symlink")
    if not stat.S_ISREG(info.st_mode):
        raise ServiceConfigError("E_CONFIG_READ", f"service config {candidate} is not a regular file")
    if info.st_mode & 0o077:
        raise ServiceConfigError("E_CONFIG_READ", f"service config {candidate} must be mode 0600-0700")
    if info.st_uid != os.geteuid():
        raise ServiceConfigError("E_CONFIG_READ", f"service config {candidate} must be owned by the current euid")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        fd = os.open(candidate, flags)
    except OSError as exc:
        raise ServiceConfigError("E_CONFIG_READ", f"cannot open service config {candidate}: {exc}") from exc
    try:
        chunks: list[bytes] = []
        while True:
            block = os.read(fd, 65536)
            if not block:
                break
            chunks.append(block)
    except OSError as exc:
        raise ServiceConfigError("E_CONFIG_READ", f"cannot read service config {candidate}: {exc}") from exc
    finally:
        os.close(fd)
    try:
        text = b"".join(chunks).decode("utf-8")
        document = json.loads(text, object_pairs_hook=_no_duplicates)
    except ServiceConfigError:
        raise
    except (UnicodeDecodeError, ValueError) as exc:
        raise ServiceConfigError("E_CONFIG_JSON", f"service config {candidate} is not valid JSON: {exc}") from exc
    if not isinstance(document, dict):
        raise ServiceConfigError("E_CONFIG_JSON", "service config root must be a JSON object")
    return document


# --------------------------------------------------------------- primitives
def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _require_mapping(document: Mapping[str, Any], key: str, code: str) -> Mapping[str, Any]:
    value = document.get(key)
    if not isinstance(value, Mapping):
        raise ServiceConfigError(code, f"{key} must be an object")
    return value


def _reject_unknown(mapping: Mapping[str, Any], allowed: frozenset[str], section: str) -> None:
    unknown = sorted(set(mapping) - allowed)
    if unknown:
        raise ServiceConfigError(
            "E_CONFIG_UNKNOWN_KEY", f"unknown key(s) in {section}: {', '.join(unknown)}"
        )


def _path_value(value: Any, key: str) -> Path:
    """Validate a path-shaped value: absolute, no ``..``, no NUL."""
    if not isinstance(value, str) or not value.strip():
        raise ServiceConfigError("E_CONFIG_PATH", f"{key} must be a non-empty string")
    if "\x00" in value:
        raise ServiceConfigError("E_CONFIG_PATH", f"{key} contains NUL")
    if not os.path.isabs(value):
        raise ServiceConfigError("E_CONFIG_PATH", f"{key} must be absolute: {value!r}")
    if ".." in Path(value).parts:
        raise ServiceConfigError("E_CONFIG_PATH", f"{key} must not contain '..': {value!r}")
    return Path(value)


def _under(root: Path, path: Path) -> bool:
    try:
        candidate = Path(path).resolve()
        anchor = Path(root).resolve()
    except OSError:
        return False
    return candidate == anchor or anchor in candidate.parents


def _require_under(root: Path, path: Path, key: str) -> None:
    if not _under(root, path):
        raise ServiceConfigError("E_CONFIG_PATH", f"{key} must live under data_root: {path}")


def _canonical_uuid4(value: str) -> bool:
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError, TypeError):
        return False
    return str(parsed) == value and parsed.version == 4


# --------------------------------------------------------------- validation
def _validate_top_level(document: Mapping[str, Any]) -> None:
    _reject_unknown(document, REQUIRED_KEYS | OPTIONAL_KEYS, "service config")
    missing = sorted(REQUIRED_KEYS - set(document))
    if missing:
        code = {
            "version": "E_CONFIG_VERSION",
            "data_root": "E_CONFIG_PATH",
            "storage_context_home": "E_CONFIG_PATH",
            "socket_path": "E_CONFIG_PATH",
            "allow_homes": "E_CONFIG_ALLOW_HOMES",
            "backend": "E_CONFIG_BACKEND",
        }[missing[0]]
        raise ServiceConfigError(code, f"missing required key(s): {', '.join(missing)}")

    version = document["version"]
    if not _is_int(version) or version != SERVICE_CONFIG_VERSION:
        raise ServiceConfigError("E_CONFIG_VERSION", f"unsupported service config version: {version!r}")

    store_id = document.get("store_id")
    if store_id is not None:
        if not isinstance(store_id, str):
            raise ServiceConfigError("E_STORE_ID", "store_id must be a string")
        if store_id and not _canonical_uuid4(store_id):
            raise ServiceConfigError("E_STORE_ID", f"store_id is not a canonical UUIDv4: {store_id!r}")

    data_root = _path_value(document["data_root"], "data_root")
    _validate_data_root(data_root)

    context_home = _path_value(document["storage_context_home"], "storage_context_home")
    _validate_directory(context_home, "storage_context_home", "E_CONFIG_PATH")

    socket_path = _path_value(document["socket_path"], "socket_path")
    encoded = os.fsencode(str(socket_path))
    if len(encoded) > _MAX_SOCKET_BYTES:
        raise ServiceConfigError(
            "E_CONFIG_PATH", f"socket_path exceeds {_MAX_SOCKET_BYTES} bytes ({len(encoded)})"
        )
    validate_socket_parent(socket_path, euid=os.geteuid())

    _validate_allow_homes(document["allow_homes"])


def _validate_data_root(data_root: Path) -> None:
    try:
        resolved = data_root.resolve()
    except OSError as exc:
        raise ServiceConfigError("E_CONFIG_PATH", f"cannot resolve data_root: {exc}") from exc
    if resolved == Path("/"):
        raise ServiceConfigError("E_CONFIG_PATH", "data_root must not be the filesystem root")
    try:
        home = Path.home().resolve()
    except OSError:
        home = None
    if home is not None and resolved == home:
        raise ServiceConfigError("E_CONFIG_PATH", "data_root must not be the home directory")
    if resolved.exists():
        if not resolved.is_dir():
            raise ServiceConfigError("E_CONFIG_PATH", f"data_root is not a directory: {resolved}")
        return
    parent = resolved.parent
    while not parent.exists():
        if parent == parent.parent:
            break
        parent = parent.parent
    if not parent.is_dir():
        raise ServiceConfigError("E_CONFIG_PATH", f"data_root cannot be created under {parent}")


def _validate_directory(path: Path, key: str, code: str) -> Path:
    try:
        resolved = path.resolve()
    except OSError as exc:
        raise ServiceConfigError(code, f"cannot resolve {key}: {exc}") from exc
    if not resolved.exists() or not resolved.is_dir():
        raise ServiceConfigError(code, f"{key} must be an existing directory: {resolved}")
    return resolved


def _validate_allow_homes(value: Any) -> None:
    if not isinstance(value, list) or not value:
        raise ServiceConfigError("E_CONFIG_ALLOW_HOMES", "allow_homes must be a non-empty list")
    seen: list[Path] = []
    for entry in value:
        if not isinstance(entry, str) or not entry.strip():
            raise ServiceConfigError("E_CONFIG_ALLOW_HOMES", "allow_homes entries must be non-empty strings")
        if not os.path.isabs(entry):
            raise ServiceConfigError("E_CONFIG_ALLOW_HOMES", f"allow_homes entry must be absolute: {entry!r}")
        if ".." in Path(entry).parts:
            raise ServiceConfigError("E_CONFIG_ALLOW_HOMES", f"allow_homes entry must be canonical: {entry!r}")
        resolved = _validate_directory(Path(entry), "allow_homes entry", "E_CONFIG_ALLOW_HOMES")
        if resolved in seen:
            raise ServiceConfigError("E_CONFIG_ALLOW_HOMES", f"duplicate allow_homes entry: {entry!r}")
        seen.append(resolved)


def _validate_backend(document: Mapping[str, Any], data_root: Path) -> BackendConfig:
    backend = _require_mapping(document, "backend", "E_CONFIG_BACKEND")
    _reject_unknown(backend, _BACKEND_KEYS, "backend")

    storage_mode = backend.get("storage_mode")
    if storage_mode != "shared":
        raise ServiceConfigError("E_CONFIG_BACKEND", f"backend.storage_mode must be 'shared': {storage_mode!r}")

    vector_backend = backend.get("vector_backend")
    if not isinstance(vector_backend, str) or vector_backend not in _VECTOR_BACKENDS:
        raise ServiceConfigError("E_CONFIG_BACKEND", f"backend.vector_backend invalid: {vector_backend!r}")

    db_url = backend.get("db_url")
    if db_url is not None:
        if not isinstance(db_url, str) or not db_url:
            raise ServiceConfigError("E_CONFIG_BACKEND", "backend.db_url must be a non-empty string or null")
        if not db_url.startswith(_SQLITE_PREFIX):
            raise ServiceConfigError(
                "E_CONFIG_BACKEND", f"backend.db_url must use {_SQLITE_PREFIX}: {db_url!r}"
            )
        location = resolve_sqlite_location(db_url, data_root=data_root, origin=ValueOrigin("yaml"))
        if location.local_path is not None:
            _require_under(data_root, location.local_path, "backend.db_url path")

    lancedb_path = backend.get("lancedb_path")
    if lancedb_path is not None:
        candidate = _path_value(lancedb_path, "backend.lancedb_path")
        _require_under(data_root, candidate, "backend.lancedb_path")
        lancedb_path = str(candidate)
    elif vector_backend == "lancedb":
        raise ServiceConfigError("E_CONFIG_BACKEND", "backend.lancedb_path is required for lancedb")

    graph_raw = backend.get("graph_snapshot_path")
    if graph_raw is None:
        raise ServiceConfigError("E_CONFIG_BACKEND", "backend.graph_snapshot_path is required")
    graph_path = _path_value(graph_raw, "backend.graph_snapshot_path")
    _require_under(data_root, graph_path, "backend.graph_snapshot_path")

    collection = backend.get("vector_collection", "memories")
    if not isinstance(collection, str) or not collection.strip():
        raise ServiceConfigError("E_CONFIG_BACKEND", "backend.vector_collection must be a non-empty string")

    extraction_mode = backend.get("extraction_mode")
    if extraction_mode is not None and extraction_mode not in _EXTRACTION_MODES:
        raise ServiceConfigError("E_CONFIG_BACKEND", f"backend.extraction_mode invalid: {extraction_mode!r}")

    llm_model = backend.get("llm_model")
    if llm_model is not None and (not isinstance(llm_model, str) or not llm_model.strip()):
        raise ServiceConfigError("E_CONFIG_BACKEND", "backend.llm_model must be a non-empty string")

    llm_base_url = backend.get("llm_base_url")
    if llm_base_url is not None:
        _validate_base_url(llm_base_url)

    max_facts = backend.get("max_facts", 5)
    if not _is_int(max_facts) or not 1 <= max_facts <= 50:
        raise ServiceConfigError("E_CONFIG_BACKEND", f"backend.max_facts must be an int in 1..50: {max_facts!r}")

    writer_raw = backend.get("writer", {})
    if not isinstance(writer_raw, Mapping):
        raise ServiceConfigError("E_CONFIG_BACKEND", "backend.writer must be an object")
    _reject_unknown(writer_raw, _WRITER_KEYS, "backend.writer")
    flush_interval = writer_raw.get("flush_interval", 5.0)
    if not _is_number(flush_interval) or not float(flush_interval) > 0:
        raise ServiceConfigError("E_CONFIG_BACKEND", "backend.writer.flush_interval must be > 0")
    max_batch = writer_raw.get("max_batch", 50)
    if not _is_int(max_batch) or max_batch < 1:
        raise ServiceConfigError("E_CONFIG_BACKEND", "backend.writer.max_batch must be an int >= 1")

    return BackendConfig(
        storage_mode="shared",
        data_root=str(data_root),
        db_url=db_url,
        vector_backend=vector_backend,
        lancedb_path=lancedb_path,
        graph_snapshot_path=str(graph_path),
        vector_collection=collection,
        extraction_mode=extraction_mode,
        llm_model=llm_model,
        llm_base_url=llm_base_url,
        max_facts=max_facts,
        writer=WriterLimits(flush_interval=float(flush_interval), max_batch=max_batch),
    )


def _validate_base_url(value: Any) -> None:
    from urllib.parse import urlsplit

    if not isinstance(value, str) or not value.strip():
        raise ServiceConfigError("E_CONFIG_BACKEND", "backend.llm_base_url must be a non-empty string")
    if any(ch.isspace() for ch in value):
        raise ServiceConfigError("E_CONFIG_BACKEND", "backend.llm_base_url must not contain whitespace")
    parts = urlsplit(value)
    if parts.scheme not in {"http", "https"} or not parts.netloc:
        raise ServiceConfigError("E_CONFIG_BACKEND", f"backend.llm_base_url is not an http(s) URL: {value!r}")
    if "@" in parts.netloc:
        raise ServiceConfigError("E_CONFIG_BACKEND", "backend.llm_base_url must not carry userinfo")


_INT_LIMITS = ("frame_bytes", "max_connections", "max_operations", "live_turns", "terminal_failed")
_POSITIVE_INT_LIMITS = ("max_connections", "max_operations", "live_turns", "terminal_failed")
_FLOAT_LIMITS = (
    "hello_timeout_s",
    "read_idle_s",
    "connect_timeout_s",
    "tool_timeout_s",
    "recall_timeout_s",
    "enqueue_timeout_s",
    "flush_timeout_s",
    "pre_drain_s",
)


def _validate_limits(document: Mapping[str, Any]) -> Limits:
    raw = document.get("limits")
    if raw is None:
        return Limits()
    if not isinstance(raw, Mapping):
        raise ServiceConfigError("E_CONFIG_LIMITS", "limits must be an object")
    _reject_unknown(raw, frozenset(Limits.__dataclass_fields__), "limits")

    values: dict[str, Any] = {}
    for key in _INT_LIMITS:
        value = raw.get(key, getattr(Limits, key))
        if not _is_int(value):
            raise ServiceConfigError("E_CONFIG_LIMITS", f"limits.{key} must be an int")
        values[key] = value
    if not _MIN_FRAME_BYTES <= values["frame_bytes"] <= _MAX_FRAME_BYTES:
        raise ServiceConfigError(
            "E_CONFIG_LIMITS", f"limits.frame_bytes must be in [{_MIN_FRAME_BYTES}, {_MAX_FRAME_BYTES}]"
        )
    for key in _POSITIVE_INT_LIMITS:
        if values[key] < 1:
            raise ServiceConfigError("E_CONFIG_LIMITS", f"limits.{key} must be >= 1")
    for key in _FLOAT_LIMITS:
        value = raw.get(key, getattr(Limits, key))
        if not _is_number(value):
            raise ServiceConfigError("E_CONFIG_LIMITS", f"limits.{key} must be a number")
        number = float(value)
        if not number > 0 or number != number or number in (float("inf"), float("-inf")):
            raise ServiceConfigError("E_CONFIG_LIMITS", f"limits.{key} must be a finite value > 0")
        values[key] = number
    return Limits(**values)


def _validate(path: Path, document: Mapping[str, Any]) -> ServiceConfig:
    _validate_top_level(document)
    data_root = Path(document["data_root"]).resolve()
    backend = _validate_backend(document, data_root)
    limits = _validate_limits(document)
    store_id = document.get("store_id") or ""
    return ServiceConfig(
        path=Path(os.path.abspath(path)),
        version=SERVICE_CONFIG_VERSION,
        store_id=store_id,
        data_root=data_root,
        storage_context_home=Path(document["storage_context_home"]).resolve(),
        socket_path=Path(document["socket_path"]),
        allow_homes=tuple(Path(entry).resolve() for entry in document["allow_homes"]),
        backend=backend,
        limits=limits,
        raw=MappingProxyType(dict(document)),
    )


# ------------------------------------------------------------------- public
def load_service_config(path: Path) -> ServiceConfig:
    """Validate the whole service JSON without touching any store."""
    document = _read_document(Path(path))
    return _validate(Path(path), document)


def provision_store_id(path: Path) -> str:
    """Return the stable ``store_id``, writing a fresh UUIDv4 exactly once."""
    config_path = Path(path)
    document = _read_document(config_path)
    config = _validate(config_path, document)
    if config.store_id:
        return config.store_id

    store_id = str(uuid.uuid4())
    document["store_id"] = store_id
    payload = (json.dumps(document, indent=2, ensure_ascii=False) + "\n").encode("utf-8")
    _atomic_write(config_path, payload)
    # Re-validate what actually landed on disk (fail-closed, no partial trust).
    written = load_service_config(config_path)
    if written.store_id != store_id:
        raise ServiceConfigError("E_STORE_ID", "provisioned store_id did not persist")
    return store_id


def _atomic_write(path: Path, payload: bytes) -> None:
    directory = path.parent
    tmp_path = directory / f".{path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    fd = os.open(tmp_path, flags, 0o600)
    try:
        try:
            view = memoryview(payload)
            while view:
                written = os.write(fd, view)
                view = view[written:]
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(tmp_path, path)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
    dir_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0)
    dir_fd = os.open(directory, dir_flags)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def validate_socket_parent(socket_path: Path, *, euid: int) -> None:
    """Require a real, owner-only directory to hold the AF_UNIX socket."""
    socket = Path(socket_path)
    parent = socket.parent
    try:
        info = os.lstat(parent)
    except OSError as exc:
        raise ServiceConfigError("E_CONFIG_PATH", f"socket parent is unavailable: {parent}: {exc}") from exc
    if stat.S_ISLNK(info.st_mode):
        raise ServiceConfigError("E_CONFIG_PATH", f"socket parent is a symlink: {parent}")
    if not stat.S_ISDIR(info.st_mode):
        raise ServiceConfigError("E_CONFIG_PATH", f"socket parent is not a directory: {parent}")
    if info.st_mode & 0o077:
        raise ServiceConfigError("E_CONFIG_PATH", f"socket parent must be mode 0700: {parent}")
    if info.st_uid != euid:
        raise ServiceConfigError("E_CONFIG_PATH", f"socket parent must be owned by euid: {parent}")
    try:
        socket_info = os.lstat(socket)
    except FileNotFoundError:
        return
    except OSError as exc:
        raise ServiceConfigError("E_CONFIG_PATH", f"cannot stat socket path {socket}: {exc}") from exc
    if stat.S_ISLNK(socket_info.st_mode):
        raise ServiceConfigError("E_CONFIG_PATH", f"socket path is a symlink: {socket}")


def build_plugin_config(config: ServiceConfig) -> Any:
    """Project the service backend into a ``HermesPluginConfig``.

    The service JSON is authoritative, so environment overrides are skipped:
    ``preflight_layout`` must validate the operator's intent, not ambient
    ``MEMORY_SERVER_*`` values.
    """
    backend = config.backend
    data: dict[str, Any] = {
        "storage_mode": "shared",
        "data_root": str(config.data_root),
        "vector_backend": backend.vector_backend,
        "vector_collection": backend.vector_collection,
        "extraction_mode": backend.extraction_mode,
        "llm_model": backend.llm_model,
        "llm_base_url": backend.llm_base_url,
        "max_facts": backend.max_facts,
        "writer": {
            "flush_interval": backend.writer.flush_interval,
            "max_batch": backend.writer.max_batch,
        },
    }
    if backend.db_url is not None:
        data["db_url"] = backend.db_url
    if backend.lancedb_path is not None:
        data["lancedb_path"] = backend.lancedb_path
    if backend.graph_snapshot_path is not None:
        data["graph_snapshot_path"] = backend.graph_snapshot_path
    return HermesPluginConfig.from_dict(data, use_env=False)


def _mismatch(message: str) -> ServiceConfigError:
    return ServiceConfigError("E_LAYOUT_MISMATCH", message)


def preflight_layout(config: ServiceConfig, settings: Any) -> StorageLayout:
    """Resolve and verify the storage layout; never opens a store."""
    if not config.store_id:
        raise ServiceConfigError("E_STORE_ID", "store_id must be provisioned before preflight")
    plugin_cfg = build_plugin_config(config)
    try:
        layout = plugin_cfg.resolve_storage_layout(
            hermes_home=str(config.storage_context_home),
            settings=settings,
        )
    except StorageLayoutError as exc:
        raise _mismatch(f"layout rejected: {exc.code}: {exc}") from exc

    data_root = config.data_root.resolve()
    if layout.mode != "shared":
        raise _mismatch(f"layout mode is {layout.mode!r}, expected 'shared'")
    if layout.data_root != data_root:
        raise _mismatch(f"layout data_root {layout.data_root} != {data_root}")
    if layout.sqlite.local_path is not None and not _under(data_root, layout.sqlite.local_path):
        raise _mismatch(f"sqlite path escapes data_root: {layout.sqlite.local_path}")
    if layout.vector.local_path is not None and not _under(data_root, layout.vector.local_path):
        raise _mismatch(f"vector path escapes data_root: {layout.vector.local_path}")
    if not _under(data_root, layout.graph_snapshot_path):
        raise _mismatch(f"graph snapshot escapes data_root: {layout.graph_snapshot_path}")
    if layout.root_lock_path != data_root / ".cmms-storage.lock":
        raise _mismatch(f"root lock is {layout.root_lock_path}, expected {data_root / '.cmms-storage.lock'}")
    return layout
