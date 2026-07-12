"""MCP tool: learn — extract and store facts, decisions, and skills from free text.

Thin wrapper around MemoryIngestionService.learn() which handles
all extraction and single-transaction writes.
"""

from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.services.ingestion_service import MemoryIngestionService


async def learn(
    provider: SQLiteProvider,
    text: str,
    source: str = "user",
) -> dict:
    """Extract facts, decisions, and skills from natural language text and store them.

    Delegates to MemoryIngestionService for single-transaction writes:
    all extracted items + receipts + outbox entries are committed atomically.

    Args:
        provider: Initialized SQLiteProvider instance.
        text: Natural language text to analyze and extract knowledge from.
        source: Source identifier (default "user").

    Returns:
        Dict with keys:
            - facts: list of {receipt, item} for extracted facts
            - decisions: list of {receipt, item} for extracted decisions
            - skills: list of {receipt, item} for extracted skills
            - receipts: flat list of all receipts
    """
    svc = MemoryIngestionService(provider._session_factory)
    return await svc.learn(
        text=text,
        source=source,
    )
