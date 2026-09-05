"""Decision repository — CRUD operations for decisions."""

from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from memory_server.models import Decision
from storage.dedup import ACTIVE_LIFECYCLE_STATES, decision_dedup_key
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

    async def update(self, decision_id: str, **kwargs) -> Optional[Decision]:
        orm = await self._session.get(DecisionORM, decision_id)
        if orm is None:
            return None
        for key, value in kwargs.items():
            if hasattr(orm, key):
                setattr(orm, key, value)
        await self._session.flush()
        await self._session.refresh(orm)
        return orm.to_pydantic()

    async def find_existing(
        self, context: str, choice: str
    ) -> Optional[Decision]:
        """Return the best ACTIVE decision matching a normalized (context, choice) key.

        Used by the ingestion write path to deduplicate decisions: the same
        decision can arrive repeatedly (e.g. under different ``hermes_turn_*``
        session sources, or as near-duplicate variants whose ``choice`` grew a
        parenthetical), and should be skipped instead of creating a new row.

        Matching uses the NORMALIZED dedup key (context stripped, choice
        whitespace-collapsed to a 200-char prefix) — the same key the read
        path (``get_context``) and the DB partial unique index use, so
        write-path skip, read-path collapse, and DB constraint all agree.

        Only ACTIVE rows (candidate/validated/active) participate (W3): a
        rejected/archived decision must NOT permanently block re-ingestion of
        the same key.

        Among matching rows the BEST one is returned: higher confidence first
        (W4 — a high-confidence decision must never be hidden behind a
        low-confidence duplicate), then newest, then highest id.

        Args:
            context: The decision context (matched after stripping).
            choice: The decision choice (matched after normalization).

        Returns:
            The best matching active Decision, or None if none exists.
        """
        norm_context, norm_choice = decision_dedup_key(context, choice)
        stmt = (
            select(DecisionORM)
            .where(
                DecisionORM.context == norm_context,
                DecisionORM.dedup_key == norm_choice,
                DecisionORM.lifecycle_state.in_(ACTIVE_LIFECYCLE_STATES),
            )
            .order_by(
                DecisionORM.confidence.desc(),
                DecisionORM.created_at.desc(),
                DecisionORM.id.desc(),
            )
            .limit(1)
        )
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        return row.to_pydantic() if row else None

    async def search(
        self,
        context: Optional[str] = None,
        choice: Optional[str] = None,
        reason: Optional[str] = None,
        source: Optional[str] = None,
        creator: Optional[str] = None,
        text: Optional[str] = None,
        limit: int = 50,
    ) -> list[Decision]:
        stmt = select(DecisionORM)
        if context is not None:
            stmt = stmt.where(DecisionORM.context == context)
        if choice is not None:
            stmt = stmt.where(DecisionORM.choice == choice)
        if reason is not None:
            stmt = stmt.where(DecisionORM.reason == reason)
        if source is not None:
            stmt = stmt.where(DecisionORM.source == source)
        if creator is not None:
            stmt = stmt.where(DecisionORM.creator == creator)
        if text is not None:
            pattern = f"%{text}%"
            stmt = stmt.where(
                DecisionORM.context.like(pattern)
                | DecisionORM.choice.like(pattern)
                | DecisionORM.reason.like(pattern)
            )
        # Deterministic ordering: newest first, id DESC as final tie-break so
        # rows sharing a created_at (same ingestion batch) have a stable order.
        stmt = stmt.order_by(
            DecisionORM.created_at.desc(), DecisionORM.id.desc()
        ).limit(limit)
        result = await self._session.execute(stmt)
        return [row.to_pydantic() for row in result.scalars().all()]

    async def delete(self, decision_id: str) -> bool:
        orm = await self._session.get(DecisionORM, decision_id)
        if orm is None:
            return False
        await self._session.delete(orm)
        await self._session.flush()
        return True
