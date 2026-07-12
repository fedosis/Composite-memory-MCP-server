"""Decision repository — CRUD operations for decisions."""

from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from memory_server.models import Decision
from storage.models.decision import DecisionORM


class DecisionRepository:
    """Repository for decision CRUD operations."""

    def __init__(self, session: AsyncSession):
        self._session = session

    async def create(self, decision: Decision) -> Decision:
        orm = DecisionORM.from_pydantic(decision)
        self._session.add(orm)
        await self._session.flush()
        await self._session.refresh(orm)
        return orm.to_pydantic()

    async def get(self, decision_id: str) -> Optional[Decision]:
        result = await self._session.get(DecisionORM, decision_id)
        return result.to_pydantic() if result else None

    async def search(
        self,
        choice: Optional[str] = None,
        text: Optional[str] = None,
        limit: int = 50,
    ) -> list[Decision]:
        stmt = select(DecisionORM)
        if choice is not None:
            stmt = stmt.where(DecisionORM.choice == choice)
        if text is not None:
            pattern = f"%{text}%"
            stmt = stmt.where(
                DecisionORM.context.like(pattern)
                | DecisionORM.choice.like(pattern)
                | DecisionORM.reason.like(pattern)
            )
        stmt = stmt.limit(limit).order_by(DecisionORM.created_at.desc())
        result = await self._session.execute(stmt)
        return [row.to_pydantic() for row in result.scalars().all()]

    async def delete(self, decision_id: str) -> bool:
        orm = await self._session.get(DecisionORM, decision_id)
        if orm is None:
            return False
        await self._session.delete(orm)
        await self._session.flush()
        return True
