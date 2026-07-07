"""MCP tool: search — keyword text search over stored facts.

Wraps SQLiteProvider.search_facts with keyword text search (SQL LIKE)
and returns matched facts with confidence scores.
"""

from typing import Optional

from memory_server.models import Fact
from memory_server.providers.sqlite_provider import SQLiteProvider


async def search(
    provider: SQLiteProvider,
    query: str = "",
    subject: Optional[str] = None,
    predicate: Optional[str] = None,
    limit: int = 50,
) -> dict:
    """Search facts by keyword text with optional filters.

    Args:
        provider: Initialized SQLiteProvider instance.
        query: Free-text keyword to search across subject, predicate, object.
        subject: Optional subject filter (exact match).
        predicate: Optional predicate filter (exact match).
        limit: Maximum number of results to return (default 50).

    Returns:
        Dict with 'results' (list of fact dicts) and 'total' (int).
    """
    facts: list[Fact] = await provider.search_facts(
        text=query if query else None,
        subject=subject,
        predicate=predicate,
        limit=limit,
    )

    return {
        "results": [f.model_dump() for f in facts],
        "total": len(facts),
    }
