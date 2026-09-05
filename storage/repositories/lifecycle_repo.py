"""Lifecycle repository — lifecycle state and event tracking."""

from datetime import datetime
from typing import Any, Optional
from uuid import uuid4

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from storage.base import utcnow
from storage.models.lifecycle import LifecycleEventORM, LifecycleStateORM


class LifecycleConflictError(ValueError):
    """The requested revision/state no longer matches; roll back the whole UOW."""


def normalize_version(version: Any) -> int:
    """Canonical revision; support legacy initial and padded integer versions.

    NULL and 0.1.0 denote revision 1. Non-positive integer revisions also
    denote 1. Unknown strings are rejected, never interpreted via SQLite CAST.
    """
    if version is None or version == "0.1.0":
        return 1
    if isinstance(version, int) and not isinstance(version, bool):
        return max(1, version)
    if isinstance(version, str):
        value = version.strip()
        if value == "0.1.0":
            return 1
        if value.isascii() and value.isdecimal():
            return max(1, int(value))
    raise ValueError(f"Unsupported lifecycle version: {version!r}")


async def _cas_transition(
    session: AsyncSession,
    model: Any,
    memory_id: str,
    new_state: str,
    expected_state: str,
    expected_version: int,
    confidence: float | None = None,
) -> Any:
    """Shared fact/belief CAS primitive; no commit or rollback here."""
    row = (await session.execute(
        select(model.version, model.lifecycle_state).where(model.id == memory_id)
    )).one_or_none()
    if row is None or normalize_version(row.version) != expected_version or row.lifecycle_state != expected_state:
        raise LifecycleConflictError(f"expected_version mismatch for {memory_id}: expected {expected_version}")
    values = {
        "lifecycle_state": new_state,
        "version": str(expected_version + 1) if model.__tablename__ == "facts" else expected_version + 1,
        "updated_at": utcnow(),
    }
    if confidence is not None:
        values["confidence"] = confidence
    result = await session.execute(
        update(model)
        .where(model.id == memory_id, model.version == row.version, model.lifecycle_state == expected_state)
        .values(**values)
        .execution_options(synchronize_session=False)
    )
    if result.rowcount != 1:
        raise LifecycleConflictError(f"expected_version mismatch for {memory_id}: expected {expected_version}")
    orm = await session.get(model, memory_id, populate_existing=True)
    return orm.to_pydantic()


class LifecycleRepository:
    """Repository for lifecycle state and event tracking."""

    def __init__(self, session: AsyncSession):
        self._session = session

    async def transition(
        self,
        repository: Any,
        *,
        memory_id: str,
        memory_type: str,
        from_state: str,
        to_state: str,
        expected_version: int,
        reason: str = "",
        triggered_by: str = "system",
        confidence: float | None = None,
    ) -> Any:
        """CAS + state history + event in this session; the UOW owner commits.

        Any exception must propagate to the UOW owner, who rolls back the whole
        transaction. Do not catch a conflict and commit earlier batch writes.
        """
        if repository._session is not self._session:
            raise ValueError("Lifecycle repositories must share the UOW session")
        updated = await repository.transition_lifecycle_state(
            memory_id, to_state, from_state, expected_version, confidence=confidence
        )
        await self.set_state(memory_id, memory_type, to_state, from_state, updated.confidence)
        await self.record_event(memory_id, memory_type, from_state, to_state, reason, triggered_by)
        return updated

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
            updated_at=utcnow(),
        )
        self._session.add(orm)
        await self._session.flush()

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
            timestamp=utcnow(),
        )
        self._session.add(event)
        await self._session.flush()

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
                "timestamp": (
                e.timestamp.isoformat()
                if isinstance(e.timestamp, datetime)
                else str(e.timestamp)
            ),
            }
            for e in result.scalars().all()
        ]
