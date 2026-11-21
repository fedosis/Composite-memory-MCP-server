"""Sidecar config (``memory_server_proxy.json``) read/write/validate — stdlib only.

The sidecar is the single client config: an absolute ``socket_path`` and the
``expected_store_id`` UUID. No secrets, no backend paths, no env overrides.
"""

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path
from typing import Any

CLIENT_CONFIG_NAME = "memory_server_proxy.json"
SIDECAR_VERSION = 1


def read_sidecar(hermes_home: str | Path) -> dict | None:
    """Read and parse the sidecar; ``None`` when missing/unreadable/not a JSON object."""
    path = Path(hermes_home) / CLIENT_CONFIG_NAME
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        data = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    return data


def validate_sidecar(data: Any) -> dict:
    """Validate the parsed sidecar; raise ``ValueError`` with a clear message."""
    if not isinstance(data, dict):
        raise ValueError("memory_server_proxy config must be a JSON object")
    if data.get("version") != SIDECAR_VERSION:
        raise ValueError("memory_server_proxy config has an unsupported version")
    socket_path = data.get("socket_path")
    if not isinstance(socket_path, str) or not socket_path:
        raise ValueError("memory_server_proxy config: socket_path must be a non-empty string")
    if not Path(socket_path).is_absolute():
        raise ValueError("memory_server_proxy config: socket_path must be an absolute path")
    store_id = data.get("expected_store_id")
    try:
        canonical = isinstance(store_id, str) and str(uuid.UUID(store_id)) == store_id
    except (ValueError, AttributeError, TypeError):
        canonical = False
    if not canonical:
        raise ValueError("memory_server_proxy config: expected_store_id must be a canonical UUID")
    return data


def write_sidecar(hermes_home: str | Path, *, socket_path: str, expected_store_id: str) -> Path:
    """Atomically write the sidecar (tmp + rename) with mode ``0600``."""
    document = {"version": SIDECAR_VERSION, "socket_path": socket_path,
                "expected_store_id": expected_store_id}
    validate_sidecar(document)
    path = Path(hermes_home) / CLIENT_CONFIG_NAME
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{CLIENT_CONFIG_NAME}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    os.chmod(tmp, 0o600)
    tmp.replace(path)
    return path
