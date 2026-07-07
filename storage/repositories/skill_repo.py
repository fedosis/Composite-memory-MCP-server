"""Skill repository — CRUD operations for skills."""

from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from memory_server.models import Skill
from storage.models.skill import SkillORM


class SkillRepository:
    """Repository for skill CRUD operations."""

    def __init__(self, session: AsyncSession):
        self._session = session

    async def create(self, skill: Skill) -> Skill:
        orm = SkillORM.from_pydantic(skill)
        self._session.add(orm)
        await self._session.commit()
        return orm.to_pydantic()

    async def get(self, skill_id: str) -> Optional[Skill]:
        result = await self._session.get(SkillORM, skill_id)
        return result.to_pydantic() if result else None

    async def search(self, purpose: Optional[str] = None, limit: int = 50) -> list[Skill]:
        stmt = select(SkillORM)
        if purpose is not None:
            stmt = stmt.where(SkillORM.purpose == purpose)
        stmt = stmt.limit(limit).order_by(SkillORM.created_at.desc())
        result = await self._session.execute(stmt)
        return [row.to_pydantic() for row in result.scalars().all()]

    async def delete(self, skill_id: str) -> bool:
        orm = await self._session.get(SkillORM, skill_id)
        if orm is None:
            return False
        await self._session.delete(orm)
        await self._session.commit()
        return True
