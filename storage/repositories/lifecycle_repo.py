"""Lifecycle repository — lifecycle state and event tracking."""

from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from storage.models.lifecycle import LifecycleEventORM, LifecycleStateORM


class LifecycleRepository:
    """Repository for lifecycle state and event tracking."""

    def __init__(self, session: AsyncSession):
        self._session = session

    async def get_state(self, memory_id: str) -> Optional[str]:
        stmt = (
            select(LifecycleStateORM)
            .where(LifecycleStateORM.memory_id == memory_id)
            .order_by(LifecycleStateORM.updated_at.desc())
            .limit(1)
        )
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        return row.current_state if row else None

    async def set_state(
        self,
        memory_id: str,
        memory_type: str,
        new_state: str,
        previous_state: Optional[str] = None,
        confidence: float = 0.5,
    ) -> None:
        orm = LifecycleStateORM(
            id=str(uuid4()),
            memory_id=memory_id,
            memory_type=memory_type,
            current_state=new_state,
            previous_state=previous_state,
            confidence=confidence,
            updated_at=datetime.now(timezone.utc),
        )
        self._session.add(orm)
        await self._session.commit()

    async def record_event(
        self,
        memory_id: str,
        memory_type: str,
        from_state: str,
        to_state: str,
        reason: str = "",
        triggered_by: str = "system",
    ) -> None:
        event = LifecycleEventORM(
            id=str(uuid4()),
            memory_id=memory_id,
            memory_type=memory_type,
            from_state=from_state,
            to_state=to_state,
            reason=reason,
            triggered_by=triggered_by,
            timestamp=datetime.now(timezone.utc),
        )
        self._session.add(event)
        await self._session.commit()

    async def get_events(
        self,
        memory_id: str,
        limit: int = 50,
    ) -> list[dict]:
        stmt = (
            select(LifecycleEventORM)
            .where(LifecycleEventORM.memory_id == memory_id)
            .order_by(LifecycleEventORM.timestamp.desc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return [
            {
                "id": e.id,
                "memory_id": e.memory_id,
                "from_state": e.from_state,
                "to_state": e.to_state,
                "reason": e.reason,
                "triggered_by": e.triggered_by,
                "timestamp": e.timestamp.isoformat() if isinstance(e.timestamp, datetime) else str(e.timestamp),
            }
            for e in result.scalars().all()
        ]
