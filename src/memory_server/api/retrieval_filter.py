"""Filter only vector IDs positively known to belong to retired SQL rows.

Vector IDs are deterministic UUIDs, not SQL primary keys. Unknown and graph
results remain visible; absence from the retired set never proves retirement.
"""

from __future__ import annotations

from typing import Any, Iterable

from memory_server.projection_rebuild import deterministic_vector_id

RANK_ID_KEYS = ("vector_id", "id", "fact_id")


def retired_vector_ids(non_active: Iterable[str]) -> set[str]:
    ids = set(non_active)
    return {deterministic_vector_id("fact", i) for i in ids} | {
        deterministic_vector_id("belief", i) for i in ids
    }


async def load_retired(provider: Any) -> set[str]:
    """Load both ID-only retirement sets in one provider session."""
    from storage.repositories.belief_repo import BeliefRepository
    from storage.repositories.fact_repo import FactRepository

    async with await provider._get_session() as session:
        non_active = await FactRepository(session).non_active_ids()
        non_active |= await BeliefRepository(session).non_active_ids()
    return retired_vector_ids(non_active)


def filter_rank_results(results, retired, include_inactive: bool = False):
    if include_inactive:
        return list(results)
    kept = []
    for row in results:
        if getattr(row, "source", None) == "graph":
            kept.append(row)
            continue
        metadata = getattr(row, "metadata", None) or {}
        ids = {metadata.get(k) for k in RANK_ID_KEYS if metadata.get(k)}
        if ids & retired:
            continue
        kept.append(row)
    return kept


def filter_semantic_results(rows, retired, include_inactive: bool = False):
    if include_inactive:
        return list(rows)
    return [row for row in rows if not (row.get("id") and row["id"] in retired)]


def rebuild_semantic_payload(payload, retired, include_inactive=False):
    """Preserve the envelope while rebuilding semantic rows and count."""
    rows = payload.get("semantic_results")
    if rows is None:
        return payload
    filtered = filter_semantic_results(rows, retired, include_inactive)
    return {**payload, "semantic_results": filtered, "total": len(filtered)}


def rebuild_route_payload(payload, retired, include_inactive=False):
    """Rebuild ranked rows/counts; do not invent result fields or sources."""
    all_results = payload.get("all_results")
    if all_results is None:
        return payload
    filtered = filter_rank_results(all_results, retired, include_inactive)
    rebuilt = dict(payload)
    rebuilt["all_results"] = filtered
    rebuilt["ranked_results"] = [row.__dict__ for row in filtered]
    rebuilt["total"] = len(filtered)
    if "sources" in payload:
        counts = {"fts": 0, "semantic": 0, "graph": 0}
        for row in filtered:
            if getattr(row, "source", None) in counts:
                counts[row.source] += 1
        rebuilt["sources"] = counts
    return rebuilt
