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
    extract_beliefs: bool = False,
    min_belief_confidence: float = 0.6,
) -> dict:
    """Extract facts, decisions, skills, and optionally beliefs from natural language text.

    Delegates to MemoryIngestionService for single-transaction writes:
    all extracted items + receipts + outbox entries are committed atomically.

    When extract_beliefs=True, also runs belief extraction AFTER the main
    transaction (outside its scope) and creates/reinforces beliefs with
    evidence linked to extracted facts.

    Args:
        provider: Initialized SQLiteProvider instance.
        text: Natural language text to analyze and extract knowledge from.
        source: Source identifier (default "user").
        extract_beliefs: If True, also extract and store beliefs (default False).
        min_belief_confidence: Minimum confidence to create a belief (default 0.6).

    Returns:
        Dict with keys:
            - facts: list of {receipt, item} for extracted facts
            - decisions: list of {receipt, item} for extracted decisions
            - skills: list of {receipt, item} for extracted skills
            - beliefs: list of {belief, extracted, reinforced} (when extract_beliefs=True)
            - receipts: flat list of all receipts
    """
    svc = MemoryIngestionService(provider._session_factory)
    return await svc.learn(
        text=text,
        source=source,
        extract_beliefs=extract_beliefs,
        min_belief_confidence=min_belief_confidence,
    )
