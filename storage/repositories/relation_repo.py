"""Claim relation repository — canonical SQL CRUD for inter-claim relations."""

from __future__ import annotations

from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from storage.models.relation import ClaimRelationORM


class RelationRepository:
    """Repository for canonical claim relations."""

    def __init__(self, session: AsyncSession):
        self._session = session

    async def create(
        self,
        source_id: str,
        target_id: str,
        relation_type: str,
    ) -> ClaimRelationORM:
        """Create a relation if it does not already exist."""
        existing = await self._session.get(
            ClaimRelationORM,
            (source_id, target_id, relation_type),
        )
        if existing is not None:
            return existing

        orm = ClaimRelationORM(
            source_id=source_id,
            target_id=target_id,
            relation_type=relation_type,
        )
        self._session.add(orm)
        await self._session.flush()
        await self._session.refresh(orm)
        return orm

    async def get_dependents(
        self,
        target_id: str,
        relation_type: str = "derived_from",
    ) -> list[str]:
        """Return source IDs that depend on the given target."""
        stmt = (
            select(ClaimRelationORM.source_id)
            .where(ClaimRelationORM.target_id == target_id)
            .where(ClaimRelationORM.relation_type == relation_type)
        )
        result = await self._session.execute(stmt)
        return [row[0] for row in result.fetchall()]

    async def get_by_source(
        self,
        source_id: str,
        relation_type: Optional[str] = None,
    ) -> list[ClaimRelationORM]:
        """Return relations originating from a source memory."""
        stmt = select(ClaimRelationORM).where(ClaimRelationORM.source_id == source_id)
        if relation_type is not None:
            stmt = stmt.where(ClaimRelationORM.relation_type == relation_type)
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

