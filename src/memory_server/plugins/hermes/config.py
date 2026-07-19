"""Plugin configuration schema for the Hermes MemoryProvider plugin.

Config is loaded from Hermes config.yaml under memory.providers.memory_server,
or from environment variables with MEMORY_SERVER_ prefix.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


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
    """Path to the CMMS installation directory (auto-detected if empty)."""

    writer: WriterConfig = field(default_factory=WriterConfig)
    """Async batch writer configuration."""

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> HermesPluginConfig:
        """Create config from a dict (from Hermes config.yaml).

        Falls back to env vars when config keys are missing.
        """
        data = data or {}

        writer_cfg = WriterConfig.from_dict(data.get("writer", {}))

        return cls(
            db_url=os.environ.get(
                "MEMORY_SERVER_DB_URL",
                data.get("db_url") or "sqlite+aiosqlite:///data/memory.db",
            ),
            cmms_path=os.environ.get(
                "MEMORY_SERVER_PATH",
                data.get("path") or "",
            ),
            writer=writer_cfg,
        )

    @classmethod
    def from_env(cls) -> HermesPluginConfig:
        """Create config from environment variables only."""
        return cls(
            db_url=os.environ.get(
                "MEMORY_SERVER_DB_URL",
                "sqlite+aiosqlite:///data/memory.db",
            ),
            cmms_path=os.environ.get("MEMORY_SERVER_PATH", ""),
            writer=WriterConfig(
                flush_interval=float(
                    os.environ.get("MEMORY_SERVER_WRITER_FLUSH_INTERVAL", "5.0")
                ),
                max_batch=int(
                    os.environ.get("MEMORY_SERVER_WRITER_MAX_BATCH", "50")
                ),
            ),
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
