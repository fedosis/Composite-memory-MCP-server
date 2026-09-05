"""Tests for FactRepository FTS fallback classification (Card 2, D7).

Narrow-except matrix split by failure SITE:
- (P) probe failures inside ``_check_fts5`` (sqlite_master probe query)
- (M) MATCH-query failures inside the search FTS block

Expected FTS failures (marker in message) fall back / disable FTS; real DB
operational failures (e.g. "database is locked") propagate and never cache
``_fts5_available = False``.
"""

import logging
import sqlite3

import pytest
from sqlalchemy.exc import IntegrityError as SQLAlchemyIntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from storage.base import Base
from storage.dedup import fact_dedup_key
from storage.repositories.fact_repo import FactRepository

from memory_server.models import Fact


@pytest.fixture
async def engine():
    """Create an in-memory SQLite engine with all tables + facts_fts."""
    eng = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # Create facts FTS5 virtual table (mirrors SQLiteProvider DDL).
        await conn.exec_driver_sql(
            "CREATE VIRTUAL TABLE IF NOT EXISTS facts_fts "
            "USING fts5(subject, predicate, object, "
            "content=facts, content_rowid=rowid)"
        )
        # Triggers to keep FTS index in sync
        await conn.exec_driver_sql(
            "CREATE TRIGGER IF NOT EXISTS facts_ai AFTER INSERT ON facts BEGIN "
            "INSERT INTO facts_fts(rowid, subject, predicate, object) "
            "VALUES (new.rowid, new.subject, new.predicate, new.object); END"
        )
        await conn.exec_driver_sql(
            "CREATE TRIGGER IF NOT EXISTS facts_ad AFTER DELETE ON facts BEGIN "
            "INSERT INTO facts_fts(facts_fts, rowid, subject, predicate, object) "
            "VALUES('delete', old.rowid, old.subject, old.predicate, old.object); END"
        )
        await conn.exec_driver_sql(
            "CREATE TRIGGER IF NOT EXISTS facts_au AFTER UPDATE ON facts BEGIN "
            "INSERT INTO facts_fts(facts_fts, rowid, subject, predicate, object) "
            "VALUES('delete', old.rowid, old.subject, old.predicate, old.object); "
            "INSERT INTO facts_fts(rowid, subject, predicate, object) "
            "VALUES (new.rowid, new.subject, new.predicate, new.object); END"
        )
    yield eng
    await eng.dispose()


@pytest.fixture
async def session(engine):
    """Create a session with the engine."""
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as s:
        yield s


@pytest.fixture
def repo(session):
    return FactRepository(session)


def _patch_execute(session, monkeypatch, *, fail_when, exc):
    """Raise *exc* from session.execute when the SQL text contains *fail_when*."""

    original = session.execute

    async def execute(stmt, *args, **kwargs):
        if fail_when in str(stmt):
            raise exc
        return await original(stmt, *args, **kwargs)

    monkeypatch.setattr(session, "execute", execute)


@pytest.mark.asyncio
class TestFactRepoFTSFallback:
    """Narrow-except matrix for the FTS fallback (Card 2, Test plan 6)."""

    async def _seed(self, repo):
        await repo.create(
            Fact(
                id="f1",
                subject="Docker",
                predicate="runs_on",
                object="OMV",
                source="test",
                dedup_key=fact_dedup_key("Docker", "runs_on", "OMV"),
            )
        )
        await repo.create(
            Fact(
                id="f2",
                subject="Nginx",
                predicate="proxies",
                object="Web",
                source="test",
                dedup_key=fact_dedup_key("Nginx", "proxies", "Web"),
            )
        )

    async def test_fts_normal_path_used(self, repo):
        """Probe passes + MATCH succeeds → FTS used (results found)."""
        await self._seed(repo)
        results = await repo.search(text="Docker")
        assert [f.id for f in results] == ["f1"]

    async def test_active_lifecycle_filter_includes_validated_facts(self, repo):
        """The active filter includes every configured active lifecycle state."""
        await repo.create(
            Fact(
                id="validated-fact",
                subject="Validated Docker",
                predicate="runs_on",
                object="OMV",
                source="test",
                lifecycle_state="validated",
                dedup_key=fact_dedup_key("Validated Docker", "runs_on", "OMV"),
            )
        )

        results = await repo.search(
            subject="Validated Docker", lifecycle_state="active"
        )

        assert [fact.id for fact in results] == ["validated-fact"]
        fts_results = await repo.search(text="Validated", lifecycle_state="active")
        assert [fact.id for fact in fts_results] == ["validated-fact"]

    async def test_probe_expected_failure_disables_fts_and_falls_back(
        self, repo, monkeypatch, caplog
    ):
        """(P1) probe: 'no such table: facts_fts' → FTS off, LIKE used."""
        caplog.set_level(logging.DEBUG, logger="storage.repositories.fact_repo")
        await self._seed(repo)
        _patch_execute(
            repo._session,
            monkeypatch,
            fail_when="sqlite_master",
            exc=sqlite3.OperationalError("no such table: facts_fts"),
        )
        assert await repo._check_fts5() is False
        assert repo._fts5_available is False
        assert "FTS unavailable" in caplog.text
        results = await repo.search(text="Docker")
        assert [f.id for f in results] == ["f1"]  # LIKE fallback returned results

    async def test_match_expected_failure_falls_through_to_like(
        self, repo, monkeypatch, caplog
    ):
        """(M1) MATCH: 'malformed MATCH expression' → LIKE fallback."""
        caplog.set_level(logging.DEBUG, logger="storage.repositories.fact_repo")
        await self._seed(repo)
        _patch_execute(
            repo._session,
            monkeypatch,
            fail_when="facts_fts MATCH",
            exc=sqlite3.OperationalError("malformed MATCH expression"),
        )
        assert await repo._check_fts5() is True  # probe passed
        results = await repo.search(text="Docker")
        assert [f.id for f in results] == ["f1"]  # LIKE fallback
        assert "falling back to LIKE" in caplog.text

    async def test_match_unable_to_use_function_falls_through_to_like(
        self, repo, monkeypatch, caplog
    ):
        """(M1b) MATCH: 'unable to use function MATCH ...' → LIKE fallback."""
        caplog.set_level(logging.DEBUG, logger="storage.repositories.fact_repo")
        await self._seed(repo)
        _patch_execute(
            repo._session,
            monkeypatch,
            fail_when="facts_fts MATCH",
            exc=sqlite3.OperationalError(
                "unable to use function MATCH in the requested context"
            ),
        )
        results = await repo.search(text="Docker")
        assert [f.id for f in results] == ["f1"]

    async def test_probe_real_db_failure_propagates_not_cached(self, repo, monkeypatch):
        """(P2) probe: 'database is locked' → propagates, FTS NOT cached False."""
        await self._seed(repo)
        _patch_execute(
            repo._session,
            monkeypatch,
            fail_when="sqlite_master",
            exc=sqlite3.OperationalError("database is locked"),
        )
        with pytest.raises(sqlite3.OperationalError):
            await repo.search(text="Docker")
        assert repo._fts5_available is None  # transient failure must not disable FTS

    async def test_match_real_db_failure_propagates(self, repo, monkeypatch):
        """(M2) MATCH: 'database is locked' → propagates (no LIKE fallback)."""
        await self._seed(repo)
        _patch_execute(
            repo._session,
            monkeypatch,
            fail_when="facts_fts MATCH",
            exc=sqlite3.OperationalError("database is locked"),
        )
        with pytest.raises(sqlite3.OperationalError):
            await repo.search(text="Docker")

    async def test_integrity_error_propagates(self, repo, monkeypatch):
        """Non-OperationalError (IntegrityError) propagates from the FTS site."""
        await self._seed(repo)
        _patch_execute(
            repo._session,
            monkeypatch,
            fail_when="facts_fts MATCH",
            exc=SQLAlchemyIntegrityError(
                "SELECT ...", {}, sqlite3.IntegrityError("UNIQUE constraint failed")
            ),
        )
        with pytest.raises(SQLAlchemyIntegrityError):
            await repo.search(text="Docker")
