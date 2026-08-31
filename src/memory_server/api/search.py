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
    limit: int | None = None,
    include_inactive: bool = False,
) -> dict:
    """Search facts by keyword text with optional filters.

    Args:
        provider: Initialized SQLiteProvider instance.
        query: Free-text keyword to search across subject, predicate, object.
        subject: Optional subject filter (exact match).
        predicate: Optional predicate filter (exact match).
        limit: Maximum number of results to return. When None (default),
            resolves from ``MEMORY_SERVER_SEARCH_DEFAULT_LIMIT`` via Settings.

    Returns:
        Dict with 'results' (list of fact dicts) and 'total' (int).
    """
    from memory_server.settings import get_settings

    if limit is None:
        limit = get_settings().search_default_limit
    facts: list[Fact] = await provider.search_facts(
        text=query if query else None,
        subject=subject,
        predicate=predicate,
        limit=limit,
        include_inactive=include_inactive,
    )

    return {
        "results": [f.model_dump(mode="json") for f in facts],
        "total": len(facts),
    }
