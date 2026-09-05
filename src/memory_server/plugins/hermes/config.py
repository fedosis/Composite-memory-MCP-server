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
from typing import Any

from memory_server.paths import cmms_repo_root
from memory_server.settings import get_settings

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
        }
    return {
        "path": _env_str(_ENV_PATH),
        "db_url": _env_str(_ENV_DB_URL),
        "max_facts": _env_int(_ENV_MAX_FACTS),
        "writer_flush_interval": _env_float(_ENV_WRITER_FLUSH_INTERVAL),
        "writer_max_batch": _env_int(_ENV_WRITER_MAX_BATCH),
        "llm_base_url": _env_str(_ENV_LLM_BASE_URL),
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

    def resolve_db_url(self, hermes_home: str) -> str:
        """Resolve the database URL, expanding paths relative to hermes_home.

        If db_url is a relative path like 'sqlite+aiosqlite:///data/memory.db',
        make it relative to hermes_home for profile isolation.
        """
        if self.db_url.startswith("sqlite+aiosqlite:///"):
            path_part = self.db_url[len("sqlite+aiosqlite:///"):]
            if not path_part.startswith("/"):
                # Relative path — resolve against hermes_home
                resolved = Path(hermes_home) / path_part
                resolved.parent.mkdir(parents=True, exist_ok=True)
                return f"sqlite+aiosqlite:///{resolved}"
        return self.db_url
