"""Shared helpers for native backend unit tests (slice T3)."""

from __future__ import annotations

import asyncio
import atexit
import concurrent.futures
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

from memory_server.transports.hermes import _get_loop


def run_on_backend_loop(coro, *, timeout: float = 10.0):
    """Run a coroutine on the Hermes owner loop without cancelling on timeout.

    Sync equivalent of ``NativeBackendAdapter._on_backend_loop``: a timeout
    stops waiting for the result but never cancels the backend coroutine
    (``concurrent.futures.Future.result`` never cancels the submitted task,
    unlike ``asyncio.wait_for`` on the ``wrap_future`` wrapper).
    """
    fut = asyncio.run_coroutine_threadsafe(coro, _get_loop())
    try:
        return fut.result(timeout=timeout)
    except concurrent.futures.TimeoutError as exc:
        raise TimeoutError from exc


def make_service_config(tmp_path: Path, **overrides: Any) -> Path:
    """Write a minimal valid service JSON and return its path."""
    root = tmp_path / "store"
    root.mkdir(parents=True, exist_ok=True)
    context = tmp_path / "context"
    context.mkdir(parents=True, exist_ok=True)
    # The AF_UNIX socket path is capped at 107 bytes; a socket below tmp_path
    # would exceed that. Use a short private /tmp directory (already 0700).
    socket_dir = Path(tempfile.mkdtemp(prefix="cmmst3-", dir="/tmp"))
    atexit.register(shutil.rmtree, socket_dir, ignore_errors=True)
    homes = tmp_path / "homes"
    homes.mkdir(parents=True, exist_ok=True)
    (homes / "hermes").mkdir(exist_ok=True)

    document: dict[str, Any] = {
        "version": 1,
        "data_root": str(root),
        "storage_context_home": str(context),
        "socket_path": str(socket_dir / "memory.sock"),
        "allow_homes": [str(homes / "hermes")],
        "backend": {
            "storage_mode": "shared",
            "db_url": f"sqlite+aiosqlite:///{root / 'memory.db'}",
            "vector_backend": "lancedb",
            "lancedb_path": str(root / "lancedb"),
            "graph_snapshot_path": str(root / "graph.json"),
            "vector_collection": "memories",
            "extraction_mode": "llm",
            "llm_model": "deepseek-v4-flash",
            "llm_base_url": "https://api.deepseek.com/v1",
            "max_facts": 5,
            "writer": {"flush_interval": 5.0, "max_batch": 50},
        },
    }
    document.update(overrides)

    path = tmp_path / "service.json"
    path.write_text(json.dumps(document, indent=2), encoding="utf-8")
    os.chmod(path, 0o600)
    return path
