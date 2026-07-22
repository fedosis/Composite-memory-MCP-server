"""Bulk import utilities for curated MEMORY.md files."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from memory_server.admission import MemoryAdmissionGate
from memory_server.api.remember import remember
from memory_server.providers.sqlite_provider import SQLiteProvider


def parse_memory_md(path: str | Path) -> list[str]:
    """Extract importable memory entries from a Markdown MEMORY.md file.

    The parser intentionally keeps v0 conservative: bullet/list items become
    candidate memories, headings and fenced code blocks are ignored.
    """
    memory_path = Path(path)
    entries: list[str] = []
    in_fence = False
    for raw_line in memory_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line.startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence or not line or line.startswith("#"):
            continue
        if line.startswith(("- ", "* ")):
            entry = line[2:].strip()
        elif len(line) > 40:
            entry = line
        else:
            continue
        if entry:
            entries.append(entry)
    return entries


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
    admission_gate = gate or MemoryAdmissionGate()
    imported: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    for entry in parse_memory_md(path):
        decision = admission_gate.classify(entry, source_scope=source)
        if not decision.admitted:
            skipped.append({"text": entry, "decision": decision.to_metadata()})
            continue
        result = await remember(
            provider,
            subject=source,
            predicate="contains",
            object=entry,
            confidence=decision.score,
            source=source,
            admission=decision,
        )
        imported.append({
            "text": entry,
            "fact_id": result["fact"].id,
            "decision": decision.to_metadata(),
        })

    return {
        "imported": len(imported),
        "skipped": len(skipped),
        "items": imported,
        "skipped_items": skipped,
    }
