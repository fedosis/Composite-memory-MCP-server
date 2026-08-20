"""Shared CMMS path resolution.

Single source of truth for locating the CMMS repository root. Every module
that needs to default a data path to the repo root (config, CLI, provider)
imports :func:`cmms_repo_root` from here, so a layout change cannot silently
diverge across callers.
"""

from __future__ import annotations

from pathlib import Path


def cmms_repo_root() -> Path:
    """Return the checked-out CMMS repository root.

    Resolved from this package's location: ``src/memory_server/paths.py``
    → ``memory_server`` → ``src`` → project root.
    """
    return Path(__file__).resolve().parents[2]
