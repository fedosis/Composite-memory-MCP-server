"""Plugin configuration schema for the Hermes MemoryProvider plugin.

Config is loaded from Hermes config.yaml under memory.providers.memory_server,
or from environment variables with MEMORY_SERVER_ prefix.

HERM-1/2: a single env resolver (``_env_overrides``) feeds BOTH constructors
(``from_dict`` with ``use_env=True`` and ``from_env``). ``use_env=False``
skips every env-backed field — config-file values are validated raw.
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Mapping
from urllib.parse import urlsplit

from memory_server.paths import (
    StorageLayout,
    StorageLayoutError,
    StorageResolutionInputs,
    ValueOrigin,
    cmms_repo_root,
    resolve_sqlite_location,
    resolve_storage_layout,
)
from memory_server.settings import get_settings

if TYPE_CHECKING:
    from memory_server.settings import Settings

# Query keys whose value is a secret and must never be printed (DETAIL 4.3).
_SECRET_QUERY_WORDS = ("token", "key", "secret", "password", "credential")
_REDACTED = "<redacted>"

# Env vars that this config block resolves itself. Extraction/LLM tuning
# values (MEMORY_SERVER_LLM_MODEL etc.) are deliberately NOT part of the
# snapshot — they are resolved by the resolver layer at runtime; from_env
# keeps them None (except llm_base_url, which is both a config block value
# and a data-plane env var).
_ENV_PATH = "MEMORY_SERVER_PATH"
_ENV_DB_URL = "MEMORY_SERVER_DB_URL"
_ENV_MAX_FACTS = "MEMORY_SERVER_MAX_FACTS"
_ENV_WRITER_FLUSH_INTERVAL = "MEMORY_SERVER_WRITER_FLUSH_INTERVAL"
_ENV_WRITER_MAX_BATCH = "MEMORY_SERVER_WRITER_MAX_BATCH"
_ENV_LLM_BASE_URL = "MEMORY_SERVER_LLM_BASE_URL"
_ENV_STORAGE_MODE = "MEMORY_SERVER_STORAGE_MODE"
_ENV_DATA_ROOT = "MEMORY_SERVER_DATA_ROOT"
_ENV_VECTOR_BACKEND = "MEMORY_SERVER_VECTOR_BACKEND"
_ENV_LANCEDB_PATH = "MEMORY_SERVER_LANCEDB_PATH"
_ENV_GRAPH_PATH = "MEMORY_SERVER_GRAPH_SNAPSHOT_PATH"
_ENV_QDRANT = "MEMORY_SERVER_QDRANT_LOCATION"
_ENV_COLLECTION = "MEMORY_SERVER_VECTOR_COLLECTION"

_ORIGIN_ENV = {
    "path": _ENV_PATH,
    "db_url": _ENV_DB_URL,
    "max_facts": _ENV_MAX_FACTS,
    "storage_mode": _ENV_STORAGE_MODE,
    "data_root": _ENV_DATA_ROOT,
    "vector_backend": _ENV_VECTOR_BACKEND,
    "lancedb_path": _ENV_LANCEDB_PATH,
    "graph_snapshot_path": _ENV_GRAPH_PATH,
    "qdrant_location": _ENV_QDRANT,
    "vector_collection": _ENV_COLLECTION,
    "llm_base_url": _ENV_LLM_BASE_URL,
}

_LEGACY_ENV = {
    "vector_backend": "MEMORY_VECTOR_BACKEND",
    "graph_snapshot_path": "MEMORY_GRAPH_SNAPSHOT_PATH",
    "qdrant_location": "MEMORY_QDRANT_URL",
}


def _env_str(name: str) -> str | None:
    """Read a string env var; empty/blank values count as unset."""
    raw = os.environ.get(name)
    if raw is None:
        return None
    value = raw.strip()
    return value or None


def _env_float(name: str) -> float | None:
    """Read a float env var; malformed/non-finite values count as unset."""
    raw = _env_str(name)
    if raw is None:
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value):
        return None
    return value


def _env_int(name: str) -> int | None:
    """Read an int env var; malformed values count as unset."""
    raw = _env_str(name)
    if raw is None:
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None
    return value


def _env_overrides(use_env: bool) -> dict[str, Any]:
    """Single env snapshot for the config block.

    Reads each env var at most once and normalizes every value (blank ->
    unset, malformed numbers -> unset). With ``use_env=False`` no env var is
    read at all — every entry stays None so constructors fall through to
    config-file values/defaults.
    """
    if not use_env:
        return {
            "path": None,
            "db_url": None,
            "max_facts": None,
            "writer_flush_interval": None,
            "writer_max_batch": None,
            "llm_base_url": None,
            "storage_mode": None,
            "data_root": None,
            "vector_backend": None,
            "lancedb_path": None,
            "graph_snapshot_path": None,
            "qdrant_location": None,
            "vector_collection": None,
        }
    return {
        "path": _env_str(_ENV_PATH),
        "db_url": _env_str(_ENV_DB_URL),
        "max_facts": _env_int(_ENV_MAX_FACTS),
        "writer_flush_interval": _env_float(_ENV_WRITER_FLUSH_INTERVAL),
        "writer_max_batch": _env_int(_ENV_WRITER_MAX_BATCH),
        "llm_base_url": _env_str(_ENV_LLM_BASE_URL),
        "storage_mode": _env_str(_ENV_STORAGE_MODE),
        "data_root": _env_str(_ENV_DATA_ROOT),
        "vector_backend": _env_str(_ENV_VECTOR_BACKEND),
        "lancedb_path": _env_str(_ENV_LANCEDB_PATH),
        "graph_snapshot_path": _env_str(_ENV_GRAPH_PATH),
        "qdrant_location": _env_str(_ENV_QDRANT),
        "vector_collection": _env_str(_ENV_COLLECTION),
        "legacy": {key: _env_str(value) for key, value in _LEGACY_ENV.items()},
    }


@dataclass(frozen=True)
class StorageEnvSnapshot:
    """One immutable, normalized view of storage-related configuration."""

    path: str | None = None
    db_url: str | None = None
    storage_mode: str | None = None
    data_root: str | None = None
    vector_backend: str | None = None
    lancedb_path: str | None = None
    graph_snapshot_path: str | None = None
    qdrant_location: str | None = None
    vector_collection: str | None = None
    values: Mapping[str, str | None] = field(default_factory=dict)
    origins: Mapping[str, ValueOrigin] = field(default_factory=dict)


def _choose(name: str, yaml: Any, env: dict[str, Any], default: Any) -> tuple[Any, ValueOrigin]:
    value = env.get(name)
    if value is not None:
        return value, ValueOrigin("env", _ORIGIN_ENV.get(name))
    value = yaml if yaml is not None and yaml != "" else None
    if value is not None:
        return value, ValueOrigin("yaml")
    value = env.get("legacy", {}).get(name)
    if value is not None:
        return value, ValueOrigin("legacy_env", _LEGACY_ENV.get(name))
    return default, ValueOrigin("default")


# ------------------------------------------------- raw/effective config report
# S2-02 (DETAIL 3.2/11.1/11.2). The storage keys whose value decides the
# canonical SQL and every local store path.
def _redacted_url_text(url: str) -> str:
    """Redact secrets from a configured URL while keeping its exact spelling.

    Only userinfo and secret-looking query VALUES are replaced (DETAIL 4.3);
    every other byte of the configured spelling survives. ``paths._redact_url``
    rebuilds through ``urlunsplit``, which collapses one slash of a
    ``scheme:///relative`` SQLite URL because the netloc is empty, so the report
    redacts in place instead of reformatting.
    """
    parts = urlsplit(url)
    if not parts.scheme:
        return url
    netloc = parts.hostname or ""
    if parts.port is not None:
        netloc += f":{parts.port}"
    rebuilt = f"{parts.scheme}://{netloc}{parts.path}"
    if parts.query:
        # Split the RAW query text so every non-secret parameter keeps its exact
        # configured spelling; only secret-looking values are replaced.
        segments = []
        for segment in parts.query.split("&"):
            key, separator, _ = segment.partition("=")
            if any(word in key.lower() for word in _SECRET_QUERY_WORDS):
                segments.append(f"{key}{separator or '='}{_REDACTED}")
            else:
                segments.append(segment)
        rebuilt += "?" + "&".join(segments)
    if parts.fragment:
        rebuilt += f"#{parts.fragment}"
    return rebuilt


_STORAGE_REPORT_KEYS = (
    "db_url",
    "storage_mode",
    "data_root",
    "vector_backend",
    "lancedb_path",
    "graph_snapshot_path",
    "qdrant_location",
    "vector_collection",
)

# The RAW layer's own defaults. They are restated here (never read from
# ``Settings``) because the raw report must be produced with zero environment
# reads and zero ``Settings``/``.env`` access.
_STORAGE_REPORT_DEFAULTS: dict[str, str] = {
    "db_url": "sqlite+aiosqlite:///data/memory.db",
    "storage_mode": "profile",
    "data_root": ".",
    "vector_backend": "lancedb",
    "lancedb_path": "data/lancedb",
    "graph_snapshot_path": "data/graph.json",
    "qdrant_location": ":memory:",
    "vector_collection": "memories",
}


@dataclass(frozen=True)
class StorageConfigReport:
    """Raw YAML, environment and effective storage configuration in one view.

    ``raw`` is resolved with ``use_env=False``: zero environment reads and zero
    ``Settings``/``.env`` access, so it is exactly what the config FILE (or its
    default) selects — this is the layer the migration planner uses to pick the
    canonical SQL, which removes any source-autodetection ambiguity. ``env`` is
    the environment snapshot, reported SEPARATELY and never folded into ``raw``.
    ``effective`` is the value the runtime would select (canonical env wins over
    YAML, YAML over a deployed legacy alias, then the raw default).

    ``settings_consulted`` is False by contract: this report never reads
    ``Settings``/``.env``, so an ``effective`` origin of ``default`` can still be
    overridden by the Settings layer at runtime.
    """

    raw: Mapping[str, str | None]
    raw_origins: Mapping[str, str]
    env: Mapping[str, str | None]
    env_origins: Mapping[str, str | None]
    effective: Mapping[str, str | None]
    effective_origins: Mapping[str, str]
    divergence: tuple[str, ...]
    canonical_sqlite_url: str
    settings_consulted: bool = False

    def as_dict(self) -> dict[str, Any]:
        """JSON-friendly view; URL-ish values are redacted, never secret."""

        def _redacted(values: Mapping[str, str | None]) -> dict[str, str | None]:
            return {
                key: _redacted_url_text(value) if isinstance(value, str) and "://" in value else value
                for key, value in values.items()
            }

        return {
            "raw": _redacted(self.raw),
            "raw_origins": dict(self.raw_origins),
            "env": _redacted(self.env),
            "env_origins": dict(self.env_origins),
            "effective": _redacted(self.effective),
            "effective_origins": dict(self.effective_origins),
            "divergence": list(self.divergence),
            "canonical_sqlite_url": (
                _redacted_url_text(self.canonical_sqlite_url)
                if "://" in self.canonical_sqlite_url
                else self.canonical_sqlite_url
            ),
            "settings_consulted": self.settings_consulted,
        }


def build_storage_config_report(
    raw_data: dict[str, Any] | None = None,
    *,
    include_env: bool = True,
) -> StorageConfigReport:
    """Build the raw/effective storage configuration report (S2-02).

    ``include_env=False`` performs ZERO environment reads — ``_env_overrides``
    short-circuits before touching ``os.environ`` — and is the mode migration
    and doctor use to select the canonical SQL from the raw config file with no
    autodetection ambiguity. ``include_env=True`` additionally snapshots the
    environment into the SEPARATE ``env`` view.
    """
    data = raw_data or {}
    raw_config = HermesPluginConfig.from_dict(data, use_env=False)
    env = _env_overrides(use_env=include_env) if include_env else _env_overrides(use_env=False)
    raw: dict[str, str | None] = {}
    raw_origins: dict[str, str] = {}
    env_values: dict[str, str | None] = {}
    env_origins: dict[str, str | None] = {}
    effective: dict[str, str | None] = {}
    effective_origins: dict[str, str] = {}
    for key in _STORAGE_REPORT_KEYS:
        candidate = getattr(raw_config, key)
        raw[key] = None if candidate is None else str(candidate)
        raw_origins[key] = raw_config.storage_origins.get(key, ValueOrigin("default")).kind
        from_env = env.get(key)
        env_values[key] = None if from_env is None else str(from_env)
        env_origins[key] = _ORIGIN_ENV.get(key)
        selected, origin = _choose(key, data.get(key), env, _STORAGE_REPORT_DEFAULTS[key])
        effective[key] = None if selected is None else str(selected)
        effective_origins[key] = origin.kind
    divergence = tuple(
        sorted(
            key
            for key in _STORAGE_REPORT_KEYS
            if env_values[key] is not None and env_values[key] != raw[key]
        )
    )
    return StorageConfigReport(
        raw=MappingProxyType(raw),
        raw_origins=MappingProxyType(raw_origins),
        env=MappingProxyType(env_values),
        env_origins=MappingProxyType(env_origins),
        effective=MappingProxyType(effective),
        effective_origins=MappingProxyType(effective_origins),
        divergence=divergence,
        canonical_sqlite_url=str(raw_config.db_url),
    )


def _coerce_max_facts(value: Any, default: int) -> int:
    """Coerce max_facts from env/config; malformed values fall to default."""
    if isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


@dataclass
class WriterConfig:
    """Configuration for the async batch writer queue."""

    flush_interval: float = 5.0
    """Seconds between automatic flushes."""

    max_batch: int = 50
    """Maximum number of items to process in a single batch flush."""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WriterConfig:
        """Create from a config dict (from Hermes config.yaml)."""
        return cls(
            flush_interval=float(data.get("flush_interval", 5.0)),
            max_batch=int(data.get("max_batch", 50)),
        )


@dataclass
class HermesPluginConfig:
    """Full configuration for the Hermes MemoryProvider plugin.

    Loads from Hermes config.yaml structure or environment variables.
    Environment variables take precedence over config file values.
    """

    db_url: str = "sqlite+aiosqlite:///data/memory.db"
    """SQLite database URL."""

    cmms_path: str = ""
    """Path to the CMMS installation directory (defaults to repo root)."""

    cmms_path_source: str = "default"
    """Where ``cmms_path`` came from: ``"env"``, ``"config"``, or ``"default"``."""

    writer: WriterConfig = field(default_factory=WriterConfig)
    """Async batch writer configuration."""

    max_facts: int = 5
    """Maximum number of facts/decisions injected into the system prompt
    context on each prefetch. Shared across all profiles (single CMMS)."""

    extraction_mode: str | None = None
    llm_model: str | None = None
    llm_timeout_seconds: float | None = None
    llm_max_input_chars: int | None = None
    llm_confidence_gate: float | None = None
    # Explicit LLM endpoint for the extraction model (env or the
    # ``memory.providers.memory_server`` config block). API keys stay in the
    # environment; enables self-hosted OpenAI-compatible endpoints.
    llm_base_url: str | None = None
    storage_mode: str = "profile"
    data_root: str = "."
    vector_backend: str = "lancedb"
    lancedb_path: str = "data/lancedb"
    graph_snapshot_path: str = "data/graph.json"
    qdrant_location: str = ":memory:"
    vector_collection: str = "memories"
    storage_origins: dict[str, ValueOrigin] = field(default_factory=dict)
    storage_snapshot: StorageEnvSnapshot = field(default_factory=StorageEnvSnapshot)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any] | None,
        *,
        use_env: bool = True,
    ) -> HermesPluginConfig:
        """Create config from a dict (from Hermes config.yaml).

        Falls back to env vars when config keys are missing. An empty
        ``path`` (or an empty/absent ``MEMORY_SERVER_PATH``) defaults to the
        CMMS repo root for import compatibility.
        ``use_env=False`` skips environment overrides (used by doctor to
        validate the raw config value).
        """
        data = data or {}
        env = _env_overrides(use_env=use_env)
        if use_env and (env.get("storage_mode") is not None or env.get("data_root") is not None):
            if env.get("storage_mode") is None:
                raise StorageLayoutError(
                    "E_SHARED_ROOT_REQUIRED",
                    "MEMORY_SERVER_STORAGE_MODE is required with MEMORY_SERVER_DATA_ROOT",
                )
            if env.get("data_root") is None:
                raise StorageLayoutError(
                    "E_SHARED_ROOT_REQUIRED",
                    "MEMORY_SERVER_DATA_ROOT is required with MEMORY_SERVER_STORAGE_MODE",
                )
        settings = get_settings() if use_env else None
        defaults = {
            "db_url": str(settings.db_url) if settings else "sqlite+aiosqlite:///data/memory.db",
            "vector_backend": getattr(settings, "vector_backend", "lancedb") if settings else "lancedb",
            "lancedb_path": str(getattr(settings, "lancedb_path", "data/lancedb")) if settings else "data/lancedb",
            "graph_snapshot_path": str(getattr(settings, "graph_snapshot_path", "data/graph.json"))
            if settings
            else "data/graph.json",
            "qdrant_location": getattr(settings, "qdrant_location", ":memory:") if settings else ":memory:",
            "vector_collection": getattr(settings, "vector_collection", "memories") if settings else "memories",
        }
        selected: dict[str, Any] = {}
        origins: dict[str, ValueOrigin] = {}
        for key, default in defaults.items():
            selected[key], origins[key] = _choose(key, data.get(key), env, default)
            if origins[key].kind == "default" and settings is not None:
                origins[key] = ValueOrigin("settings")
        origins["sqlite"] = origins["db_url"]
        origins["vector"] = origins["vector_backend"]
        origins["graph"] = origins["graph_snapshot_path"]
        selected["path"], path_origin = _choose("path", data.get("path"), env, str(cmms_repo_root()))
        origins["path"] = path_origin
        selected["max_facts"], origins["max_facts"] = _choose("max_facts", data.get("max_facts"), env, 5)
        selected["llm_base_url"], origins["llm_base_url"] = _choose("llm_base_url", data.get("llm_base_url"), env, None)
        selected["storage_mode"], origins["mode"] = _choose("storage_mode", data.get("storage_mode"), env, "profile")
        selected["data_root"], origins["root"] = _choose("data_root", data.get("data_root"), env, ".")
        origins["storage_mode"] = origins["mode"]
        origins["data_root"] = origins["root"]
        selected["vector_backend"] = str(selected["vector_backend"])
        snapshot_values = MappingProxyType(
            {
                key: selected.get(key)
                for key in (
                    "path",
                    "db_url",
                    "storage_mode",
                    "data_root",
                    "vector_backend",
                    "lancedb_path",
                    "graph_snapshot_path",
                    "qdrant_location",
                    "vector_collection",
                )
            }
        )
        snapshot_origins = MappingProxyType(dict(origins))
        snapshot = StorageEnvSnapshot(
            **{k: selected.get(k) for k in StorageEnvSnapshot.__dataclass_fields__ if k not in {"values", "origins"}},
            values=snapshot_values,
            origins=snapshot_origins,
        )
        writer_cfg = WriterConfig.from_dict(data.get("writer", {}) or {})
        if env["writer_flush_interval"] is not None:
            writer_cfg.flush_interval = env["writer_flush_interval"]
        elif env.get("legacy", {}).get("writer_flush_interval") is not None:
            writer_cfg.flush_interval = env["legacy"]["writer_flush_interval"]
        if env["writer_max_batch"] is not None:
            writer_cfg.max_batch = env["writer_max_batch"]
        elif env.get("legacy", {}).get("writer_max_batch") is not None:
            writer_cfg.max_batch = env["legacy"]["writer_max_batch"]

        if selected["path"]:
            cmms_path = str(selected["path"])
            source = "config" if path_origin.kind == "yaml" else path_origin.kind
        else:
            cmms_path, source = str(cmms_repo_root()), "default"

        return cls(
            db_url=selected["db_url"],
            cmms_path=cmms_path,
            cmms_path_source=source,
            writer=writer_cfg,
            max_facts=_coerce_max_facts(selected["max_facts"], 5),
            extraction_mode=data.get("extraction_mode"),
            llm_model=data.get("llm_model"),
            llm_timeout_seconds=data.get("llm_timeout_seconds"),
            llm_max_input_chars=data.get("llm_max_input_chars"),
            llm_confidence_gate=data.get("llm_confidence_gate"),
            llm_base_url=selected["llm_base_url"],
            storage_mode=selected["storage_mode"],
            data_root=selected["data_root"],
            vector_backend=selected["vector_backend"],
            lancedb_path=selected["lancedb_path"],
            graph_snapshot_path=selected["graph_snapshot_path"],
            qdrant_location=selected["qdrant_location"],
            vector_collection=selected["vector_collection"],
            storage_origins=origins,
            storage_snapshot=snapshot,
        )

    @classmethod
    def from_env(cls) -> HermesPluginConfig:
        """Create config from environment variables only."""
        return cls.from_dict({}, use_env=True)

    def validate_shared_root(self, expected: str | None = None) -> None:
        """Compatibility wrapper for validating the installation path."""
        if not self.cmms_path:
            return
        try:
            self.validate_installation_path(expected or str(cmms_repo_root()))
        except ValueError as exc:
            raise ValueError(
                "memory.providers.memory_server.path must point at the shared "
                f"CMMS repo root ({Path(expected or str(cmms_repo_root())).resolve()}), "
                f"got {Path(self.cmms_path).resolve()}."
            ) from exc

    def validate_installation_path(self, expected: str | None = None) -> None:
        """Validate installation/import path only; it is not a data root."""
        if expected is not None and Path(self.cmms_path).absolute() != Path(expected).absolute():
            raise ValueError(f"installation path mismatch: {self.cmms_path}")

    def resolve_storage_layout(self, *, hermes_home: str, settings: "Settings", native: bool = True) -> StorageLayout:
        """Freeze the storage layout once; pure, performs no I/O.

        ``profile`` (native default) requires a non-blank ``hermes_home`` and
        keeps every local store below it. ``shared`` requires an absolute
        ``data_root`` and is only ever selected explicitly. ``standalone`` is
        the legacy native-provider configuration (``hermes_home`` absent and an
        in-memory ``db_url``; see ``HermesProvider.initialize``) and the MCP
        server mode: it claims no profile identity and keeps the
        working-directory-relative defaults unless ``data_root`` is set.
        """
        mode = self.storage_mode
        if mode not in ("profile", "shared", "standalone"):
            raise StorageLayoutError("E_STORAGE_MODE_INVALID", f"invalid storage mode: {mode!r}")
        profile_home: str | None = None
        if mode == "profile":
            if not native or not (hermes_home or "").strip():
                raise StorageLayoutError("E_HERMES_HOME_REQUIRED", "profile mode requires hermes_home")
            profile_home = hermes_home
        defaults = {
            "db_url": type(self).db_url,
            "vector_backend": type(self).vector_backend,
            "lancedb_path": type(self).lancedb_path,
            "graph_snapshot_path": type(self).graph_snapshot_path,
            "qdrant_location": type(self).qdrant_location,
            "vector_collection": type(self).vector_collection,
        }
        fallback = {}
        origins = dict(self.storage_origins)
        for key, default in defaults.items():
            current = getattr(self, key)
            # from_dict already captured env/YAML/Settings/default exactly once.
            # Direct dataclass construction may use the supplied Settings fallback.
            if key in origins:
                fallback[key] = current
            elif current != default:
                fallback[key] = current
                origins[key] = ValueOrigin("yaml", key)
            else:
                fallback[key] = getattr(settings, key, current)
                origins[key] = ValueOrigin("settings", key)
        origins["sqlite"] = origins["db_url"]
        origins["vector"] = origins["vector_backend"]
        origins["graph"] = origins["graph_snapshot_path"]
        data_root = self.data_root
        if (
            mode == "standalone"
            and data_root == "."
            and origins.get("root", ValueOrigin("default")).kind in {"default", "settings"}
        ):
            data_root = None
        return resolve_storage_layout(
            StorageResolutionInputs(
                mode=mode,
                profile_home=profile_home,
                data_root=data_root,
                sqlite_url=fallback["db_url"],
                vector_backend=fallback["vector_backend"],
                lancedb_path=fallback["lancedb_path"],
                graph_snapshot_path=fallback["graph_snapshot_path"],
                qdrant_location=fallback["qdrant_location"],
                vector_collection=fallback["vector_collection"],
                origins=origins,
                installation_path=self.cmms_path or None,
            )
        )

    def resolve_db_url(self, hermes_home: str) -> str:
        """Resolve the database URL without filesystem side effects."""
        return resolve_sqlite_location(
            self.db_url,
            data_root=Path(hermes_home),
            origin=self.storage_origins.get("sqlite", ValueOrigin("default")),
        ).effective_url
