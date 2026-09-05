"""Fact repository — CRUD operations for facts.

v0.6 Phase 6: Uses SQLite FTS5 full-text search when available,
with backward-compatible LIKE fallback.

FTS-fallback classification (Card 2, D7): the fallback catches ONLY the
expected SQLite "FTS unavailable / malformed query" situations — catching
``(SQLAlchemyOperationalError, sqlite3.OperationalError)`` whose message
matches an FTS marker (``no such table: facts_fts``, ``malformed MATCH
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

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select, text
from sqlalchemy.exc import OperationalError as SQLAlchemyOperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from memory_server.models import Fact
from storage.dedup import ACTIVE_LIFECYCLE_STATES, normalize_spo_component
from storage.models.fact import FactORM
from storage.repositories.lifecycle_repo import _cas_transition

logger = logging.getLogger(__name__)

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

# Narrow catch tuple for the FTS fallback (see module docstring).
_FTS_FALLBACK_ERRORS = (SQLAlchemyOperationalError, sqlite3.OperationalError)

# Messages that prove the failure is FTS-related, not a real DB problem.
_FTS_FALLBACK_MARKERS = (
    "no such table: facts_fts",
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
                # Add prefix wildcard so "run" matches "running", "runner", etc.
                terms.append(f'"{sanitized}"*')
        return " AND ".join(terms)

    async def create(self, fact: Fact) -> Fact:
        orm = FactORM.from_pydantic(fact)
        self._session.add(orm)
        await self._session.flush()
        await self._session.refresh(orm)
        return orm.to_pydantic()

    async def get(self, fact_id: str) -> Optional[Fact]:
        result = await self._session.get(FactORM, fact_id)
        return result.to_pydantic() if result else None

    async def find_existing(
        self, subject: str, predicate: str, object: str
    ) -> Optional[Fact]:
        target_subject = normalize_spo_component(subject)
        target_predicate = normalize_spo_component(predicate)
        target_object = normalize_spo_component(object)
        components = (
            (FactORM.subject, target_subject),
            (FactORM.predicate, target_predicate),
            (FactORM.object, target_object),
        )
        predicates = [FactORM.lifecycle_state.in_(ACTIVE_LIFECYCLE_STATES)]
        for column, component in components:
            for token in component.split():
                predicates.append(column.contains(token, autoescape=True))
        stmt = select(FactORM).where(*predicates).order_by(
            FactORM.confidence.desc(),
            FactORM.created_at.desc(),
            FactORM.id.desc(),
        )
        result = await self._session.execute(stmt)
        for row in result.scalars().all():
            if (
                normalize_spo_component(row.subject) == target_subject
                and normalize_spo_component(row.predicate) == target_predicate
                and normalize_spo_component(row.object) == target_object
            ):
                return row.to_pydantic()
        return None

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
                except _FTS_FALLBACK_ERRORS as exc:
                    if _is_expected_fts_failure(exc):
                        logger.debug(
                            "FTS5 search failed (%s); falling back to LIKE", exc
                        )
                    else:
                        # Real DB failure — do not hide it behind the LIKE
                        # fallback (see module docstring).
                        raise
                # Fall through to LIKE

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
    ) -> Fact:
        """Session-aware CAS; use LifecycleRepository.transition to add history/event."""
        return await _cas_transition(
            self._session, FactORM, memory_id, new_state,
            expected_state, expected_version, confidence,
        )

    async def update_lifecycle_state(self, fact_id: str, new_state: str) -> Optional[Fact]:
        orm = await self._session.get(FactORM, fact_id)
        if orm is None:
            return None
        orm.lifecycle_state = new_state
        orm.updated_at = datetime.now(timezone.utc)
        await self._session.flush()
        await self._session.refresh(orm)
        return orm.to_pydantic()

    async def increment_version(self, fact_id: str) -> Optional[Fact]:
        orm = await self._session.get(FactORM, fact_id)
        if orm is None:
            return None
        raw_version = orm.version
        try:
            current_version = int(str(raw_version).strip())
        except (TypeError, ValueError):
            current_version = 0
        orm.version = str(max(1, current_version + 1))
        orm.updated_at = datetime.now(timezone.utc)
        await self._session.flush()
        await self._session.refresh(orm)
        return orm.to_pydantic()

    async def delete(self, fact_id: str) -> bool:
        orm = await self._session.get(FactORM, fact_id)
        if orm is None:
            return False
        await self._session.delete(orm)
        await self._session.flush()
        return True
