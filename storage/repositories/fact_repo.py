"""Fact repository — CRUD operations for facts."""

from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from memory_server.models import Fact
from storage.models.fact import FactORM


class FactRepository:
    """Repository for fact CRUD operations."""

    def __init__(self, session: AsyncSession):
        self._session = session

    async def create(self, fact: Fact) -> Fact:
        orm = FactORM.from_pydantic(fact)
        self._session.add(orm)
        await self._session.commit()
        return orm.to_pydantic()

    async def get(self, fact_id: str) -> Optional[Fact]:
        result = await self._session.get(FactORM, fact_id)
        return result.to_pydantic() if result else None

    async def search(
        self,
        subject: Optional[str] = None,
        predicate: Optional[str] = None,
        text: Optional[str] = None,
        limit: int = 50,
    ) -> list[Fact]:
        stmt = select(FactORM)
        if subject is not None:
            stmt = stmt.where(FactORM.subject == subject)
        if predicate is not None:
            stmt = stmt.where(FactORM.predicate == predicate)
        if text is not None:
            pattern = f"%{text}%"
            stmt = stmt.where(
                FactORM.subject.like(pattern)
                | FactORM.predicate.like(pattern)
                | FactORM.object.like(pattern)
            )
        stmt = stmt.limit(limit).order_by(FactORM.created_at.desc())
        result = await self._session.execute(stmt)
        return [row.to_pydantic() for row in result.scalars().all()]

    async def update(self, fact_id: str, **kwargs) -> Optional[Fact]:
        orm = await self._session.get(FactORM, fact_id)
        if orm is None:
            return None
        for key, value in kwargs.items():
            if hasattr(orm, key):
                setattr(orm, key, value)
        await self._session.commit()
        return orm.to_pydantic()

    async def delete(self, fact_id: str) -> bool:
        orm = await self._session.get(FactORM, fact_id)
        if orm is None:
            return False
        await self._session.delete(orm)
        await self._session.commit()
        return True
