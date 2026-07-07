"""MCP tool: get_context — retrieve structured context about a task.

Scans facts and decisions for relevant information about the given task
or subject and returns structured context to the agent.
"""

from typing import Optional

from memory_server.providers.sqlite_provider import SQLiteProvider


async def get_context(
    provider: SQLiteProvider,
    task: str,
    subject: Optional[str] = None,
    max_results: int = 10,
) -> dict:
    """Retrieve structured context for a task.

    Args:
        provider: Initialized SQLiteProvider instance.
        task: The task description or search query.
        subject: Optional subject filter.
        max_results: Maximum number of results to return.

    Returns:
        Dict with 'facts', 'decisions', and 'total' keys.
    """
    # Search facts by text (task) and optionally by subject
    facts = await provider.search_facts(
        text=task if task else None,
        subject=subject,
        limit=max_results,
    )

    # Also search by subject if task is a name or entity
    if task and not subject:
        subject_facts = await provider.search_facts(
            subject=task,
            limit=max_results,
        )
        # Merge deduped
        existing_ids = {f.id for f in facts}
        for f in subject_facts:
            if f.id not in existing_ids:
                facts.append(f)

    # Decisions: search by context text
    decisions = []  # Full decision search will be added in later cards

    # Limit to max_results
    facts = facts[:max_results]

    return {
        "facts": [f.model_dump(mode="json") for f in facts],
        "decisions": decisions,
        "total": len(facts),
    }
