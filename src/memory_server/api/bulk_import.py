"""Bulk import utilities for curated MEMORY.md files.

Compatibility facade: ``parse_memory_md`` moved to the application service and
is re-exported here, so the existing import path
``memory_server.api.bulk_import.parse_memory_md`` keeps resolving (strangler
move, not a copy). ``import_memory_md`` keeps its signature and result shape and
delegates to the transport-free application service.

``bulk_import`` is a library use case ONLY — it is not published as a new tool
(no ``@mcp.tool``, no schema file, no CLI command, no transport registration).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from memory_server.admission import MemoryAdmissionGate
from memory_server.application import BulkImportCommand, MemoryService, parse_memory_md
from memory_server.providers.sqlite_provider import SQLiteProvider

__all__ = ["import_memory_md", "parse_memory_md"]


async def import_memory_md(
    provider: SQLiteProvider,
    path: str | Path,
    *,
    source: str = "MEMORY.md",
    gate: MemoryAdmissionGate | None = None,
) -> dict[str, Any]:
    """Import durable/important entries from a MEMORY.md file.

    Ephemeral entries are skipped before any database write. Imported entries are
    stored as simple facts with admission metadata in the receipt history.
    """
    command = BulkImportCommand(path=path, source=source, gate=gate)
    return await MemoryService(provider._session_factory).bulk_import(command)
