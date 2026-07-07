"""Fact repository — CRUD operations for facts.

v0.6 Phase 6: Uses SQLite FTS5 full-text search when available,
with backward-compatible LIKE fallback.
"""

from typing import Optional

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from memory_server.models import Fact
from storage.models.fact import FactORM

# FTS5 MATCH query wrapper — turns a user text query into an FTS5 query.
# Supports stemmed search (FTS5's default porter stemmer) and prefix matching.
# We sanitise the input to prevent FTS5 syntax errors while preserving
# the search intent.
FTS5_SEARCH_SQL = text("""
    SELECT facts.id, facts.subject, facts.predicate,
           facts.object, facts.confidence, facts.source, facts.creator,
           facts.created_at, facts.updated_at, facts.verification_status,
           facts.lifecycle_state, facts.version
    FROM facts_fts
    JOIN facts ON facts_fts.rowid = facts.rowid
    WHERE facts_fts MATCH :query
    ORDER BY rank
    LIMIT :limit
""")


class FactRepository:
    """Repository for fact CRUD operations."""

    def __init__(self, session: AsyncSession):
        self._session = session
        self._fts5_available: Optional[bool] = None

    async def _check_fts5(self) -> bool:
        """Check if FTS5 virtual table exists in this database."""
        if self._fts5_available is not None:
            return self._fts5_available
        try:
            result = await self._session.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='facts_fts'")
            )
            self._fts5_available = result.scalar() is not None
        except Exception:
            self._fts5_available = False
        return self._fts5_available

    @staticmethod
    def _fts5_query(text_query: str) -> str:
        """Convert a plain-text query into FTS5 query syntax.

        - Splits on whitespace
        - Appends * for prefix matching on each term
        - Escapes FTS5 special characters
        - Joins with AND (all terms must match)
        """
        if not text_query or not text_query.strip():
            return ""
        # Characters that need escaping in FTS5
        special_chars = set('^+-*()~<>"{}')
        terms = []
        for term in text_query.strip().split():
            # Escape any special characters
            sanitized = "".join(
                f'\\{ch}' if ch in special_chars else ch
                for ch in term
            )
            if sanitized:
                # Add prefix wildcard so "run" matches "running", "runner", etc.
                terms.append(f'"{sanitized}"*')
        return " AND ".join(terms)

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
        """Search facts with FTS5 full-text search or LIKE fallback.

        When `text` is provided, attempts FTS5 MATCH first (with stemmed
        prefix matching). Falls back to LIKE if FTS5 is not available or
        if the FTS5 query yields no results.
        """
        # If text search is requested, try FTS5 first
        if text and await self._check_fts5():
            fts5_q = self._fts5_query(text)
            if fts5_q:
                try:
                    result = await self._session.execute(
                        FTS5_SEARCH_SQL,
                        {"query": fts5_q, "limit": limit},
                    )
                    rows = result.mappings().all()
                    if rows:
                        facts = []
                        for row in rows:
                            facts.append(Fact(**row))
                        return facts
                except Exception:
                    pass  # Fall through to LIKE

        # Fallback: standard LIKE query (original behavior)
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
