"""Evidence repository — CRUD operations for evidence entries."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from memory_server.models.evidence import Evidence
from storage.models.belief import BeliefORM, EvidenceORM

# SQL aggregate query for evidence stats per belief
_EVIDENCE_AGGREGATE_SQL = """
    SELECT
        belief_id,
        COUNT(*) AS count,
        AVG(weight) AS avg_weight,
        source_type
    FROM evidence
    {where_clause}
    GROUP BY belief_id, source_type
    ORDER BY belief_id
"""


class EvidenceRepository:
    """Repository for evidence CRUD operations."""

    def __init__(self, session: AsyncSession):
        self._session = session

    async def create(self, evidence: Evidence) -> Evidence:
        """Create a new evidence entry and sync source_ids on parent belief."""
        orm = EvidenceORM.from_pydantic(evidence)
        self._session.add(orm)
        await self._session.flush()

        # Sync source_ids on the parent BeliefORM
        await self._sync_source_ids(evidence.belief_id)

        await self._session.refresh(orm)
        return orm.to_pydantic()

    async def get_by_belief_id(self, belief_id: str) -> list[Evidence]:
        """Get all evidence entries for a belief."""
        stmt = (
            select(EvidenceORM)
            .where(EvidenceORM.belief_id == belief_id)
            .order_by(EvidenceORM.created_at.desc())
        )
        result = await self._session.execute(stmt)
        return [row.to_pydantic() for row in result.scalars().all()]

    async def get_active_weights(self, belief_id: str) -> list[float]:
        """Get all active evidence weights for a belief (for confidence aggregation)."""
        stmt = (
            select(EvidenceORM.weight)
            .where(EvidenceORM.belief_id == belief_id)
        )
        result = await self._session.execute(stmt)
        return [row[0] for row in result.fetchall() if row[0] is not None]

    async def aggregate_stats(
        self, belief_ids: list[str] | None = None
    ) -> dict[str, dict]:
        """Return {belief_id: {count, avg_weight, by_source_type}} for all or specified beliefs.

        Aggregation at SQL level to avoid N+1 queries at scale.
        """
        import logging

        logging.getLogger(__name__)

        where_clause = ""
        params: dict = {}
        if belief_ids:
            placeholders = ",".join(f":bid_{i}" for i in range(len(belief_ids)))
            where_clause = f"WHERE belief_id IN ({placeholders})"
            params = {f"bid_{i}": bid for i, bid in enumerate(belief_ids)}

        sql = _EVIDENCE_AGGREGATE_SQL.format(where_clause=where_clause)
        result = await self._session.execute(
            __import__("sqlalchemy").text(sql), params
        )
        rows = result.fetchall()

        stats: dict[str, dict] = {}
        for row in rows:
            bid = row[0]
            if bid not in stats:
                stats[bid] = {
                    "count": 0,
                    "avg_weight": 0.0,
                    "by_source_type": {},
                }
            stats[bid]["count"] += row[1]
            by_type = stats[bid]["by_source_type"]
            by_type[row[3]] = by_type.get(row[3], 0) + row[1]

        # Overall avg_weight from a separate GROUP BY (no source_type split)
        overall_sql = """
            SELECT belief_id, COUNT(*) AS count, AVG(weight) AS avg_weight
            FROM evidence
            {where_clause}
            GROUP BY belief_id
        """
        overall_sql = overall_sql.format(where_clause=where_clause)
        overall = await self._session.execute(
            __import__("sqlalchemy").text(overall_sql), params
        )
        for row in overall.fetchall():
            bid = row[0]
            if bid in stats:
                stats[bid]["avg_weight"] = round(float(row[2]), 4) if row[2] is not None else 0.0

        return stats

    async def _sync_source_ids(self, belief_id: str) -> None:
        """Synchronise the denormalized source_ids field on the parent BeliefORM."""
        belief_orm = await self._session.get(BeliefORM, belief_id)
        if belief_orm is None:
            return

        # Get all evidence source_ids for this belief
        stmt = (
            select(EvidenceORM.source_id)
            .where(EvidenceORM.belief_id == belief_id)
            .distinct()
        )
        result = await self._session.execute(stmt)
        source_ids = list({row[0] for row in result.fetchall() if row[0]})

        # Deduplicate via set to ensure uniqueness
        belief_orm.source_ids = source_ids
