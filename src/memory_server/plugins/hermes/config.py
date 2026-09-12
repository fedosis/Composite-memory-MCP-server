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
from typing import TYPE_CHECKING, Any

from memory_server.paths import (
    StorageLayout,
    StorageLayoutError,
    StorageResolutionInputs,
    ValueOrigin,
    cmms_repo_root,
    resolve_storage_layout,
)
from memory_server.settings import get_settings

if TYPE_CHECKING:
    from memory_server.settings import Settings

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
            "storage_mode": None, "data_root": None, "vector_backend": None,
            "lancedb_path": None, "graph_snapshot_path": None,
            "qdrant_location": None, "vector_collection": None,
        }
    return {
        "path": _env_str(_ENV_PATH),
        "db_url": _env_str(_ENV_DB_URL),
        "max_facts": _env_int(_ENV_MAX_FACTS),
        "writer_flush_interval": _env_float(_ENV_WRITER_FLUSH_INTERVAL),
        "writer_max_batch": _env_int(_ENV_WRITER_MAX_BATCH),
        "llm_base_url": _env_str(_ENV_LLM_BASE_URL),
        "storage_mode": _env_str(_ENV_STORAGE_MODE), "data_root": _env_str(_ENV_DATA_ROOT),
        "vector_backend": _env_str(_ENV_VECTOR_BACKEND) or _env_str("MEMORY_VECTOR_BACKEND"),
        "lancedb_path": _env_str(_ENV_LANCEDB_PATH),
        "graph_snapshot_path": _env_str(_ENV_GRAPH_PATH) or _env_str("MEMORY_GRAPH_SNAPSHOT_PATH"),
        "qdrant_location": _env_str(_ENV_QDRANT) or _env_str("MEMORY_QDRANT_URL"),
        "vector_collection": _env_str(_ENV_COLLECTION),
    }


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
    storage_origins: dict[str, ValueOrigin] = field(default_factory=dict)

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
        CMMS repo root so every profile shares the same data directory.
        ``use_env=False`` skips environment overrides (used by doctor to
        validate the raw config value).
        """
        data = data or {}
        env = _env_overrides(use_env=use_env)

        writer_cfg = WriterConfig.from_dict(data.get("writer", {}) or {})
        if env["writer_flush_interval"] is not None:
            writer_cfg.flush_interval = env["writer_flush_interval"]
        if env["writer_max_batch"] is not None:
            writer_cfg.max_batch = env["writer_max_batch"]

        env_path = env["path"]
        config_path = data.get("path")
        if env_path:
            cmms_path, source = env_path, "env"
        elif config_path:
            cmms_path, source = config_path, "config"
        else:
            cmms_path, source = str(cmms_repo_root()), "default"

        return cls(
            db_url=env["db_url"] or data.get("db_url") or str(get_settings().db_url),
            cmms_path=cmms_path,
            cmms_path_source=source,
            writer=writer_cfg,
            max_facts=_coerce_max_facts(
                env["max_facts"] if env["max_facts"] is not None
                else data.get("max_facts"),
                5,
            ),
            extraction_mode=data.get("extraction_mode"),
            llm_model=data.get("llm_model"),
            llm_timeout_seconds=data.get("llm_timeout_seconds"),
            llm_max_input_chars=data.get("llm_max_input_chars"),
            llm_confidence_gate=data.get("llm_confidence_gate"),
            llm_base_url=(
                env["llm_base_url"] or data.get("llm_base_url")
            ),
            storage_mode=env["storage_mode"] or data.get("storage_mode") or "profile",
            data_root=env["data_root"] or data.get("data_root") or ".",
            storage_origins={
                "mode": ValueOrigin(
                    "env" if env["storage_mode"] else "yaml" if "storage_mode" in data else "default",
                    _ENV_STORAGE_MODE if env["storage_mode"] else None,
                ),
                "root": ValueOrigin(
                    "env" if env["data_root"] else "yaml" if "data_root" in data else "default",
                    _ENV_DATA_ROOT if env["data_root"] else None,
                ),
            },
        )

    @classmethod
    def from_env(cls) -> HermesPluginConfig:
        """Create config from environment variables only."""
        env = _env_overrides(use_env=True)
        if env["path"]:
            cmms_path, source = env["path"], "env"
        else:
            cmms_path, source = str(cmms_repo_root()), "default"
        return cls(
            db_url=env["db_url"] or str(get_settings().db_url),
            cmms_path=cmms_path,
            cmms_path_source=source,
            writer=WriterConfig(
                flush_interval=(
                    env["writer_flush_interval"]
                    if env["writer_flush_interval"] is not None
                    else 5.0
                ),
                max_batch=(
                    env["writer_max_batch"]
                    if env["writer_max_batch"] is not None
                    else 50
                ),
            ),
            max_facts=(
                env["max_facts"] if env["max_facts"] is not None else 5
            ),
            llm_base_url=env["llm_base_url"],
            storage_mode=env["storage_mode"] or "profile",
            data_root=env["data_root"] or ".",
        )

    def validate_shared_root(self, expected: str | None = None) -> None:
        """Assert cmms_path points at the shared CMMS repo root.

        Raises ValueError if the configured path is set to anything other
        than the CMMS repository root — per-profile paths fragment the
        vector index and graph snapshot. Used by doctor/install validation.
        """
        if not self.cmms_path:
            return
        expected_root = Path(expected or str(cmms_repo_root())).resolve()
        configured = Path(self.cmms_path).resolve()
        if configured != expected_root:
            raise ValueError(
                "memory.providers.memory_server.path must point at the shared "
                f"CMMS repo root ({expected_root}), got {configured}. "
                "Per-profile data dirs fragment the LanceDB index and graph."
            )

    def validate_installation_path(self, expected: str | None = None) -> None:
        """Validate installation/import path only; it is not a data root."""
        if expected is not None and Path(self.cmms_path).absolute() != Path(expected).absolute():
            raise ValueError(f"installation path mismatch: {self.cmms_path}")

    def resolve_storage_layout(
        self, *, hermes_home: str, settings: "Settings", native: bool = True
    ) -> StorageLayout:
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
            raise StorageLayoutError(
                "E_STORAGE_MODE_INVALID", f"invalid storage mode: {mode!r}"
            )
        profile_home: str | None = None
        if mode == "profile":
            if not native or not (hermes_home or "").strip():
                raise StorageLayoutError(
                    "E_HERMES_HOME_REQUIRED", "profile mode requires hermes_home"
                )
            profile_home = hermes_home
        return resolve_storage_layout(StorageResolutionInputs(
            mode=mode,
            profile_home=profile_home,
            data_root=self.data_root,
            sqlite_url=self.db_url,
            vector_backend=getattr(settings, "vector_backend", "lancedb"),
            lancedb_path=getattr(settings, "lancedb_path", "data/lancedb"),
            graph_snapshot_path=getattr(settings, "graph_snapshot_path", "data/graph.json"),
            qdrant_location=getattr(settings, "qdrant_location", ":memory:"),
            vector_collection=getattr(settings, "vector_collection", "memories"),
            origins=self.storage_origins,
        ))

    def resolve_db_url(self, hermes_home: str) -> str:
        """Resolve the database URL without filesystem side effects."""
        if self.db_url.startswith("sqlite+aiosqlite:///"):
            path_part = self.db_url[len("sqlite+aiosqlite:///"):]
            if not path_part.startswith("/"):
                # Relative path — resolve against hermes_home
                resolved = Path(hermes_home) / path_part
                return f"sqlite+aiosqlite:///{resolved}"
        return self.db_url
