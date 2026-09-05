"""Shared helpers for standalone scripts under scripts/."""

import os
import sys
from pathlib import Path
from urllib.parse import unquote, urlparse

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

_LIVE_ROOT = Path("/home/shtorm/memory-server").resolve()
_LIVE_DB = (_LIVE_ROOT / "data" / "memory.db").resolve()


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
        if db_path == _LIVE_DB:
            raise RuntimeError(f"refusing live memory-server DB target: {db_path}")
        return configured
    default_url = f"sqlite+aiosqlite:///{ROOT / 'data' / 'memory.db'}"
    db_path = _sqlite_path(default_url)
    if db_path == _LIVE_DB:
        raise RuntimeError(f"refusing live memory-server DB target: {db_path}")
    return default_url
