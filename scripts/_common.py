"""Shared helpers for standalone scripts under scripts/."""

import os
import sys
from pathlib import Path
from urllib.parse import unquote, urlparse

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

# Identify the live checkout from its actual repository root, not a fixed
# developer path. An isolated copy of scripts remains usable for tests.
_LIVE_DB = (ROOT / "data" / "memory.db").resolve()
_IS_LIVE_CHECKOUT = (ROOT / ".git").exists()


def _sqlite_path(url: str) -> Path | None:
    parsed = urlparse(url)
    if not parsed.scheme.startswith("sqlite"):
        return None
    raw = unquote(parsed.path)
    if not raw:
        return None
    return Path(raw).resolve()


def get_db_url() -> str:
    """Resolve a DB URL relative to this checkout, never the live checkout."""
    configured = os.environ.get("MEMORY_SERVER_DB_URL")
    if configured:
        db_path = _sqlite_path(configured)
        if _IS_LIVE_CHECKOUT and db_path == _LIVE_DB:
            raise RuntimeError(f"refusing live memory-server DB target: {db_path}")
        return configured
    default_url = f"sqlite+aiosqlite:///{ROOT / 'data' / 'memory.db'}"
    db_path = _sqlite_path(default_url)
    if _IS_LIVE_CHECKOUT and db_path == _LIVE_DB:
        raise RuntimeError(f"refusing live memory-server DB target: {db_path}")
    return default_url
