"""Evidence repository — CRUD operations for evidence entries."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from memory_server.models.evidence import Evidence
from storage.models.belief import BeliefORM, EvidenceORM


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
