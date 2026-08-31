"""Plugin configuration schema for the Hermes MemoryProvider plugin.

Config is loaded from Hermes config.yaml under memory.providers.memory_server,
or from environment variables with MEMORY_SERVER_ prefix.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from memory_server.paths import cmms_repo_root
from memory_server.settings import get_settings


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

        writer_cfg = WriterConfig.from_dict(data.get("writer", {}))

        env_path = os.environ.get("MEMORY_SERVER_PATH") if use_env else None
        config_path = data.get("path")
        if env_path:
            cmms_path, source = env_path, "env"
        elif config_path:
            cmms_path, source = config_path, "config"
        else:
            cmms_path, source = str(cmms_repo_root()), "default"

        return cls(
            db_url=os.environ.get(
                "MEMORY_SERVER_DB_URL",
                data.get("db_url") or get_settings().db_url,
            ),
            cmms_path=cmms_path,
            cmms_path_source=source,
            writer=writer_cfg,
            max_facts=int(
                os.environ.get("MEMORY_SERVER_MAX_FACTS", data.get("max_facts") or 5)
            ),
        )

    @classmethod
    def from_env(cls) -> HermesPluginConfig:
        """Create config from environment variables only."""
        env_path = os.environ.get("MEMORY_SERVER_PATH")
        if env_path:
            cmms_path, source = env_path, "env"
        else:
            cmms_path, source = str(cmms_repo_root()), "default"
        return cls(
            db_url=os.environ.get(
                "MEMORY_SERVER_DB_URL",
                get_settings().db_url,
            ),
            cmms_path=cmms_path,
            cmms_path_source=source,
            writer=WriterConfig(
                flush_interval=float(
                    os.environ.get("MEMORY_SERVER_WRITER_FLUSH_INTERVAL", "5.0")
                ),
                max_batch=int(
                    os.environ.get("MEMORY_SERVER_WRITER_MAX_BATCH", "50")
                ),
            ),
            max_facts=int(os.environ.get("MEMORY_SERVER_MAX_FACTS", "5")),
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
