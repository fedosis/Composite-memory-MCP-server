"""Belief repository — CRUD operations for beliefs.

Supports FTS5 full-text search on proposition via beliefs_fts virtual table,
with backward-compatible LIKE fallback.
"""

from __future__ import annotations

from typing import Optional

from sqlalchemy import cast, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.types import String

from memory_server.models.belief import Belief
from storage.models.belief import BeliefORM

# FTS5 MATCH query wrapper
FTS5_SEARCH_SQL = text("""
    SELECT beliefs.id, beliefs.proposition, beliefs.confidence,
           beliefs.source, beliefs.creator, beliefs.source_ids,
           beliefs.tags, beliefs.created_at, beliefs.updated_at,
           beliefs.last_reinforced_at, beliefs.version,
           beliefs.verification_status, beliefs.lifecycle_state
    FROM beliefs_fts
    JOIN beliefs ON beliefs_fts.rowid = beliefs.rowid
    WHERE beliefs_fts MATCH :query
    ORDER BY rank
    LIMIT :limit
""")


class BeliefRepository:
    """Repository for belief CRUD operations."""

    def __init__(self, session: AsyncSession):
        self._session = session
        self._fts5_available: Optional[bool] = None

    async def _check_fts5(self) -> bool:
        """Check if FTS5 virtual table exists in this database."""
        if self._fts5_available is not None:
            return self._fts5_available
        try:
            result = await self._session.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='beliefs_fts'")
            )
            self._fts5_available = result.scalar() is not None
        except Exception:
            self._fts5_available = False
        return self._fts5_available

    @staticmethod
    def _fts5_query(text_query: str) -> str:
        """Convert a plain-text query into FTS5 query syntax."""
        if not text_query or not text_query.strip():
            return ""
        special_chars = set('^+-*()~<>"{}')
        terms = []
        for term in text_query.strip().split():
            sanitized = "".join(
                f'\\{ch}' if ch in special_chars else ch
                for ch in term
            )
            if sanitized:
                terms.append(f'"{sanitized}"*')
        return " AND ".join(terms)

    async def create(self, belief: Belief) -> Belief:
        """Create a new belief."""
        orm = BeliefORM.from_pydantic(belief)
        self._session.add(orm)
        await self._session.flush()
        await self._session.refresh(orm)
        return orm.to_pydantic()

    async def get_by_id(self, belief_id: str) -> Optional[Belief]:
        """Get a belief by ID."""
        result = await self._session.get(BeliefORM, belief_id)
        return result.to_pydantic() if result else None

    async def search(
        self,
        proposition: Optional[str] = None,
        tags: Optional[list[str]] = None,
        lifecycle_state: Optional[str] = "active",
        min_confidence: Optional[float] = None,
        source: Optional[str] = None,
        creator: Optional[str] = None,
        limit: int = 10,
    ) -> list[Belief]:
        """Search beliefs with FTS5 full-text search or LIKE/WHERE fallback."""
        # Try FTS5 first if proposition text search is requested
        if proposition and await self._check_fts5():
            fts5_q = self._fts5_query(proposition)
            if fts5_q:
                try:
                    fetch_limit = limit * 5 if limit > 0 else 100000  # effectively unlimited
                    result = await self._session.execute(
                        FTS5_SEARCH_SQL,
                        {"query": fts5_q, "limit": fetch_limit},  # fetch extra for filtering
                    )
                    rows = result.mappings().all()
                    if rows:
                        beliefs = [Belief(**row) for row in rows]
                        # Apply in-memory filters
                        return self._apply_filters(beliefs, tags, lifecycle_state,
                                                     min_confidence, source, creator, limit)
                except Exception:
                    pass  # Fall through to LIKE

        # Fallback: standard query
        stmt = select(BeliefORM)
        conditions = []
        if proposition is not None:
            pattern = f"%{proposition}%"
            conditions.append(BeliefORM.proposition.like(pattern))
        if tags is not None:
            # SQLite JSON — cast to text for LIKE fallback
            for tag in tags:
                conditions.append(cast(BeliefORM.tags, String).like(f"%{tag}%"))
        if lifecycle_state is not None:
            conditions.append(BeliefORM.lifecycle_state == lifecycle_state)
        if min_confidence is not None:
            conditions.append(BeliefORM.confidence >= min_confidence)
        if source is not None:
            conditions.append(BeliefORM.source == source)
        if creator is not None:
            conditions.append(BeliefORM.creator == creator)

        for cond in conditions:
            stmt = stmt.where(cond)
        if limit > 0:
            stmt = stmt.limit(limit)
        stmt = stmt.order_by(BeliefORM.created_at.desc())
        result = await self._session.execute(stmt)
        return [row.to_pydantic() for row in result.scalars().all()]

    def _apply_filters(
        self,
        beliefs: list[Belief],
        tags: Optional[list[str]] = None,
        lifecycle_state: Optional[str] = None,
        min_confidence: Optional[float] = None,
        source: Optional[str] = None,
        creator: Optional[str] = None,
        limit: int = 10,
    ) -> list[Belief]:
        """Apply in-memory filters to FTS5 results."""
        filtered = beliefs
        if tags:
            tag_set = set(tags)
            filtered = [b for b in filtered if tag_set.intersection(b.tags)]
        if lifecycle_state is not None:
            filtered = [b for b in filtered if b.lifecycle_state == lifecycle_state]
        if min_confidence is not None:
            filtered = [b for b in filtered if b.confidence >= min_confidence]
        if source is not None:
            filtered = [b for b in filtered if b.source == source]
        if creator is not None:
            filtered = [b for b in filtered if b.creator == creator]
        if limit > 0:
            filtered = filtered[:limit]
        return filtered

    async def update_confidence(self, belief_id: str, new_confidence: float) -> Optional[Belief]:
        """Update the confidence of a belief."""
        orm = await self._session.get(BeliefORM, belief_id)
        if orm is None:
            return None
        orm.confidence = max(0.0, min(1.0, new_confidence))
        from datetime import datetime, timezone
        orm.updated_at = datetime.now(timezone.utc)
        await self._session.flush()
        await self._session.refresh(orm)
        return orm.to_pydantic()

    async def update_lifecycle_state(self, belief_id: str, new_state: str) -> Optional[Belief]:
        """Update the lifecycle state of a belief."""
        orm = await self._session.get(BeliefORM, belief_id)
        if orm is None:
            return None
        orm.lifecycle_state = new_state
        from datetime import datetime, timezone
        orm.updated_at = datetime.now(timezone.utc)
        await self._session.flush()
        await self._session.refresh(orm)
        return orm.to_pydantic()

    async def update_reinforced_at(self, belief_id: str) -> Optional[Belief]:
        """Update last_reinforced_at timestamp."""
        orm = await self._session.get(BeliefORM, belief_id)
        if orm is None:
            return None
        from datetime import datetime, timezone
        orm.last_reinforced_at = datetime.now(timezone.utc)
        orm.updated_at = datetime.now(timezone.utc)
        await self._session.flush()
        await self._session.refresh(orm)
        return orm.to_pydantic()

    async def increment_version(self, belief_id: str) -> Optional[Belief]:
        """Increment the version counter."""
        orm = await self._session.get(BeliefORM, belief_id)
        if orm is None:
            return None
        orm.version = (orm.version or 1) + 1
        from datetime import datetime, timezone
        orm.updated_at = datetime.now(timezone.utc)
        await self._session.flush()
        await self._session.refresh(orm)
        return orm.to_pydantic()
