"""Belief repository — CRUD operations for beliefs.

Supports FTS5 full-text search on proposition via beliefs_fts virtual table,
with backward-compatible LIKE fallback.

FTS-fallback classification (Card 2, D7): the fallback catches ONLY the
expected SQLite "FTS unavailable / malformed query" situations — catching
``(SQLAlchemyOperationalError, sqlite3.OperationalError)`` whose message
matches an FTS marker (``no such table: beliefs_fts``, ``malformed MATCH
expression``, ``unable to use function MATCH in the requested context``,
``no such module: fts5``) means the FTS path is not usable and falling back
to LIKE/WHERE is correct. ANY other exception type (IntegrityError,
ProgrammingError, ...) or an OperationalError whose message does NOT match
(e.g. ``database is locked``, ``database disk image is malformed``,
``disk I/O error``, ``unable to open database file``) is a REAL DB
operational failure and must propagate — it is NOT evidence that FTS is
unavailable, and must never silently fall back or permanently cache
``_fts5_available = False``.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Optional

from pydantic import ValidationError
from sqlalchemy import cast, select, text
from sqlalchemy.exc import OperationalError as SQLAlchemyOperationalError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.types import String

from memory_server.models.belief import Belief
from storage.models.belief import BeliefORM
from storage.repositories.lifecycle_repo import _cas_transition

logger = logging.getLogger(__name__)

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

# Narrow catch tuple for the FTS fallback (see module docstring).
_FTS_FALLBACK_ERRORS = (SQLAlchemyOperationalError, sqlite3.OperationalError)

# Messages that prove the failure is FTS-related, not a real DB problem.
_FTS_FALLBACK_MARKERS = (
    "no such table: beliefs_fts",
    "malformed MATCH expression",
    "unable to use function MATCH in the requested context",
    "no such module: fts5",
)


def _is_expected_fts_failure(exc: BaseException) -> bool:
    """Return True if *exc* indicates FTS is unavailable or the query malformed.

    Checks ``str(exc)`` and, for SQLAlchemy-wrapped DBAPI errors, the original
    driver exception (``.orig``).
    """
    messages = [str(exc)]
    orig = getattr(exc, "orig", None)
    if orig is not None and orig is not exc:
        messages.append(str(orig))
    return any(marker in msg for marker in _FTS_FALLBACK_MARKERS for msg in messages)


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
        except _FTS_FALLBACK_ERRORS as exc:
            if _is_expected_fts_failure(exc):
                logger.debug(
                    "FTS5 probe failed (%s); FTS unavailable — using fallback", exc
                )
                self._fts5_available = False
            else:
                # Real DB operational failure (e.g. "database is locked") is
                # NOT evidence that FTS is unavailable — propagate it and do
                # NOT cache _fts5_available=False (a transient failure must
                # not permanently disable FTS for this repo instance).
                raise
        return self._fts5_available

    @staticmethod
    def _fts5_query(text_query: str) -> str:
        """Convert a plain-text query into FTS5 query syntax.

        - Splits on whitespace
        - Appends * for prefix matching on each term
        - Quotes each term; FTS5 string literals make operator chars
          (^ + - * ( ) ~ < > { } [ ]) literal, so the ONLY character that
          needs escaping inside a term is the double quote itself, and FTS5
          escapes it by DOUBLING (""), not by backslash (FTS5 has no
          backslash escape — \" terminates the string early).
        - Joins with AND (all terms must match)
        """
        if not text_query or not text_query.strip():
            return ""
        terms = []
        for term in text_query.strip().split():
            sanitized = term.replace('"', '""')
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
                except _FTS_FALLBACK_ERRORS as exc:
                    if _is_expected_fts_failure(exc):
                        logger.debug(
                            "FTS5 search failed (%s); falling back to LIKE", exc
                        )
                    else:
                        # Real DB failure — do not hide it behind the LIKE
                        # fallback (see module docstring).
                        raise
                except ValidationError as exc:
                    # FTS result rows cannot be mapped to Belief: the JSON
                    # list columns (source_ids/tags) arrive as strings in the
                    # FTS5 result projection, so the FTS path is unusable for
                    # this store. Falling back to LIKE preserves the
                    # pre-existing behavior — but logged now instead of
                    # silently swallowed by the old bare except.
                    logger.debug(
                        "FTS5 result mapping failed (%s); falling back to LIKE", exc
                    )
                # Fall through to LIKE

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
        orm.updated_at = datetime.now(timezone.utc)
        await self._session.flush()
        await self._session.refresh(orm)
        return orm.to_pydantic()

    async def transition_lifecycle_state(
        self,
        memory_id: str,
        new_state: str,
        expected_state: str,
        expected_version: int,
        *,
        confidence: float | None = None,
    ) -> Belief:
        """Session-aware CAS; use LifecycleRepository.transition to add history/event."""
        return await _cas_transition(
            self._session, BeliefORM, memory_id, new_state,
            expected_state, expected_version, confidence,
        )

    async def update_lifecycle_state(self, belief_id: str, new_state: str) -> Optional[Belief]:
        """Update the lifecycle state of a belief."""
        orm = await self._session.get(BeliefORM, belief_id)
        if orm is None:
            return None
        orm.lifecycle_state = new_state
        orm.updated_at = datetime.now(timezone.utc)
        await self._session.flush()
        await self._session.refresh(orm)
        return orm.to_pydantic()

    async def update_reinforced_at(self, belief_id: str) -> Optional[Belief]:
        """Update last_reinforced_at timestamp."""
        orm = await self._session.get(BeliefORM, belief_id)
        if orm is None:
            return None
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
        orm.updated_at = datetime.now(timezone.utc)
        await self._session.flush()
        await self._session.refresh(orm)
        return orm.to_pydantic()
