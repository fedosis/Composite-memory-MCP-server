"""Shared helpers for standalone scripts under scripts/."""

import os

_DEFAULT_DB_URL = "sqlite+aiosqlite:////home/shtorm/memory-server/data/memory.db"


def get_db_url() -> str:
    """Resolve the database URL from MEMORY_SERVER_DB_URL, else the default."""
    return os.environ.get("MEMORY_SERVER_DB_URL", _DEFAULT_DB_URL)
