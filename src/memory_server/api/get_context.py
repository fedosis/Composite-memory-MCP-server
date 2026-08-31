"""MCP tool: get_context — retrieve structured context about a task.

Scans facts and decisions for relevant information about the given task
or subject and returns structured context to the agent.
"""

from typing import Optional

from storage.dedup import ACTIVE_LIFECYCLE_STATES, decision_dedup_key

from memory_server.models import Decision
from memory_server.providers.sqlite_provider import SQLiteProvider

# Over-fetch factor for the decisions search: duplicates are dropped after
# retrieval, so we fetch more than the final budget to still fill it.
DEDUP_OVERFETCH_FACTOR = 4


async def get_context(
    provider: SQLiteProvider,
    task: str,
    subject: Optional[str] = None,
    max_results: int = 10,
    include_inactive: bool = False,
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
        include_inactive=include_inactive,
    )

    # Also search by subject if task is a name or entity
    if task and not subject:
        subject_facts = await provider.search_facts(
            subject=task,
            limit=max_results,
            include_inactive=include_inactive,
        )
        # Merge deduped
        existing_ids = {f.id for f in facts}
        for f in subject_facts:
            if f.id not in existing_ids:
                facts.append(f)

    # Decisions: search by context text (Decision model has no subject field).
    # Over-fetch so dedup below can still fill the budget: duplicate decisions
    # (same context/choice ingested across turns) are collapsed to the most
    # recent row instead of flooding the injected context block.
    decisions = await provider.search_decisions(
        text=task if task else None,
        limit=max_results * DEDUP_OVERFETCH_FACTOR,
    )
    if not include_inactive:
        decisions = [d for d in decisions if d.lifecycle_state in ACTIVE_LIFECYCLE_STATES]
    # Dedup by the NORMALIZED (context, choice) key — (context.strip(),
    # whitespace-collapsed 200-char prefix of choice) — the same key the write
    # path (find_existing) and the DB partial unique index use, so
    # near-duplicate variants of the same decision collapse (W1).
    # The winner per key is chosen best-first: ACTIVE rows beat inactive ones
    # (W3 — archived/rejected rows can't evict active ones even with
    # include_inactive=True), then HIGHER confidence wins (W4 — a
    # high-confidence decision must never be hidden behind a low-confidence
    # duplicate), then newest, then highest id (deterministic tie-break).
    decisions.sort(
        key=lambda d: (
            d.lifecycle_state in ACTIVE_LIFECYCLE_STATES,
            d.confidence,
            d.created_at,
            d.id,
        ),
        reverse=True,
    )
    seen: dict[tuple[str, str], Decision] = {}
    for d in decisions:
        seen.setdefault(decision_dedup_key(d.context, d.choice), d)
    decisions = list(seen.values())[:max_results]

    # Limit to max_results
    facts = facts[:max_results]

    return {
        "facts": [f.model_dump(mode="json") for f in facts],
        "decisions": [d.model_dump(mode="json") for d in decisions],
        "total": len(facts) + len(decisions),
    }
