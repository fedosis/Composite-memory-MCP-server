"""Integration tests for BeliefRepository (Card 001)."""

import logging
import sqlite3

import pytest
from sqlalchemy.exc import IntegrityError as SQLAlchemyIntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from storage.base import Base
from storage.repositories.belief_repo import BeliefRepository

from memory_server.models import Belief


@pytest.fixture
async def engine():
    """Create an in-memory SQLite engine with all tables."""
    eng = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # Create beliefs FTS5 virtual table
        await conn.exec_driver_sql(
            "CREATE VIRTUAL TABLE IF NOT EXISTS beliefs_fts "
            "USING fts5(proposition, content=beliefs, content_rowid=rowid)"
        )
        # Triggers to keep FTS index in sync
        await conn.exec_driver_sql(
            "CREATE TRIGGER IF NOT EXISTS beliefs_ai AFTER INSERT ON beliefs BEGIN "
            "INSERT INTO beliefs_fts(rowid, proposition) "
            "VALUES (new.rowid, new.proposition); END"
        )
        await conn.exec_driver_sql(
            "CREATE TRIGGER IF NOT EXISTS beliefs_ad AFTER DELETE ON beliefs BEGIN "
            "INSERT INTO beliefs_fts(beliefs_fts, rowid, proposition) "
            "VALUES('delete', old.rowid, old.proposition); END"
        )
        await conn.exec_driver_sql(
            "CREATE TRIGGER IF NOT EXISTS beliefs_au AFTER UPDATE ON beliefs BEGIN "
            "INSERT INTO beliefs_fts(beliefs_fts, rowid, proposition) "
            "VALUES('delete', old.rowid, old.proposition); "
            "INSERT INTO beliefs_fts(rowid, proposition) "
            "VALUES (new.rowid, new.proposition); END"
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
    return BeliefRepository(session)


@pytest.mark.asyncio
class TestBeliefRepoCreate:
    async def test_create_belief(self, repo):
        b = Belief(proposition="Docker runs on OMV8", confidence=0.9)
        created = await repo.create(b)
        assert created.id == b.id
        assert created.proposition == "Docker runs on OMV8"
        assert created.confidence == 0.9

    async def test_get_by_id(self, repo):
        b = Belief(proposition="Test belief", tags=["important"])
        created = await repo.create(b)
        retrieved = await repo.get_by_id(created.id)
        assert retrieved is not None
        assert retrieved.proposition == "Test belief"
        assert "important" in retrieved.tags

    async def test_get_by_id_not_found(self, repo):
        result = await repo.get_by_id("nonexistent-id")
        assert result is None

    async def test_create_with_tags(self, repo):
        b = Belief(proposition="Tagged belief", tags=["docker", "deployment", "prod"])
        created = await repo.create(b)
        assert len(created.tags) == 3
        assert "docker" in created.tags

    async def test_create_with_source_ids(self, repo):
        b = Belief(proposition="Backed belief", source_ids=["fact-1", "fact-2"])
        created = await repo.create(b)
        assert len(created.source_ids) == 2

    async def test_create_multiple(self, repo):
        b1 = await repo.create(Belief(proposition="First belief"))
        b2 = await repo.create(Belief(proposition="Second belief"))
        assert b1.id != b2.id


@pytest.mark.asyncio
class TestBeliefRepoFTS5:
    async def test_search_by_proposition_fts5(self, repo):
        await repo.create(Belief(proposition="Docker deployment guide"))
        await repo.create(Belief(proposition="Docker compose file format"))
        await repo.create(Belief(proposition="Kubernetes cluster setup"))

        results = await repo.search(proposition="Docker")
        assert len(results) == 2
        assert all("Docker" in r.proposition for r in results)

    async def test_search_by_proposition_like_fallback(self, repo):
        results = await repo.search(proposition="Nonexistent")
        assert len(results) == 0

    async def test_search_empty_proposition_all(self, repo):
        """Search with no proposition returns all beliefs."""
        await repo.create(Belief(proposition="A", lifecycle_state="active"))
        await repo.create(Belief(proposition="B", lifecycle_state="active"))
        results = await repo.search(proposition=None)
        assert len(results) == 2

    async def test_search_partial_match_fts5(self, repo):
        await repo.create(Belief(proposition="PostgreSQL configuration tuning"))
        results = await repo.search(proposition="PostgreSQL")
        assert len(results) == 1


@pytest.mark.asyncio
class TestBeliefRepoSearch:
    async def test_search_by_lifecycle_state(self, repo):
        await repo.create(Belief(proposition="Active belief", lifecycle_state="active"))
        await repo.create(Belief(proposition="Superseded belief", lifecycle_state="superseded"))

        results = await repo.search(lifecycle_state="superseded")
        assert len(results) == 1
        assert results[0].lifecycle_state == "superseded"

    async def test_search_by_lifecycle_state_default(self, repo):
        """Search defaults to lifecycle_state='active'."""
        await repo.create(Belief(proposition="Active belief", lifecycle_state="active"))
        await repo.create(Belief(proposition="Superseded belief", lifecycle_state="superseded"))

        results = await repo.search(proposition=None, lifecycle_state="active")
        assert len(results) == 1
        assert results[0].lifecycle_state == "active"

    async def test_search_by_source(self, repo):
        await repo.create(Belief(proposition="From manual", source="manual"))
        await repo.create(Belief(proposition="From auto", source="auto"))

        results = await repo.search(source="manual")
        assert len(results) == 1
        assert results[0].source == "manual"

    async def test_search_by_creator(self, repo):
        await repo.create(Belief(proposition="Alice belief", creator="alice"))
        await repo.create(Belief(proposition="Bob belief", creator="bob"))

        results = await repo.search(creator="alice")
        assert len(results) == 1
        assert results[0].creator == "alice"

    async def test_search_by_min_confidence(self, repo):
        await repo.create(Belief(proposition="High conf", confidence=0.9))
        await repo.create(Belief(proposition="Low conf", confidence=0.3))

        results = await repo.search(min_confidence=0.5)
        assert len(results) == 1
        assert results[0].confidence >= 0.5

    async def test_search_limit(self, repo):
        for i in range(10):
            await repo.create(Belief(proposition=f"Belief {i}"))
        results = await repo.search(limit=3)
        assert len(results) == 3

    async def test_search_combined_filters(self, repo):
        await repo.create(Belief(
            proposition="Docker prod config",
            tags=["docker", "prod"],
            source="manual",
            confidence=0.9,
        ))
        await repo.create(Belief(
            proposition="Docker dev config",
            tags=["docker", "dev"],
            source="manual",
            confidence=0.7,
        ))

        # Search by proposition FTS5 + in-memory tags filter
        results = await repo.search(
            proposition="Docker",
            tags=["prod"],
            source="manual",
        )
        assert len(results) == 1
        assert "prod" in results[0].tags

    async def test_search_by_tags_in_memory(self, repo):
        """Tags filtering in FTS5 path works correctly."""
        await repo.create(Belief(
            proposition="Kubernetes prod setup",
            tags=["k8s", "prod"],
            source="auto",
        ))
        await repo.create(Belief(
            proposition="Kubernetes dev setup",
            tags=["k8s", "dev"],
            source="auto",
        ))

        results = await repo.search(
            proposition="Kubernetes",
            tags=["prod"],
        )
        assert len(results) == 1


@pytest.mark.asyncio
class TestBeliefRepoUpdate:
    async def test_update_confidence(self, repo):
        b = await repo.create(Belief(proposition="Test", confidence=0.5))
        updated = await repo.update_confidence(b.id, 0.9)
        assert updated is not None
        assert updated.confidence == 0.9

    async def test_update_confidence_clamps(self, repo):
        b = await repo.create(Belief(proposition="Test", confidence=0.5))
        updated = await repo.update_confidence(b.id, 2.0)
        assert updated is not None
        assert updated.confidence == 1.0  # clamped

    async def test_update_confidence_not_found(self, repo):
        result = await repo.update_confidence("nonexistent", 0.9)
        assert result is None

    async def test_update_lifecycle_state(self, repo):
        b = await repo.create(Belief(proposition="Test", lifecycle_state="active"))
        updated = await repo.update_lifecycle_state(b.id, "superseded")
        assert updated is not None
        assert updated.lifecycle_state == "superseded"

    async def test_update_lifecycle_state_not_found(self, repo):
        result = await repo.update_lifecycle_state("nonexistent", "archived")
        assert result is None

    async def test_update_reinforced_at(self, repo):
        b = await repo.create(Belief(proposition="Test"))
        original_reinforced = b.last_reinforced_at
        updated = await repo.update_reinforced_at(b.id)
        assert updated is not None
        assert updated.last_reinforced_at >= original_reinforced

    async def test_increment_version(self, repo):
        b = await repo.create(Belief(proposition="Test", version=1))
        updated = await repo.increment_version(b.id)
        assert updated is not None
        assert updated.version == 2


def _patch_execute(session, monkeypatch, *, fail_when, exc):
    """Raise *exc* from session.execute when the SQL text contains *fail_when*."""

    original = session.execute

    async def execute(stmt, *args, **kwargs):
        if fail_when in str(stmt):
            raise exc
        return await original(stmt, *args, **kwargs)

    monkeypatch.setattr(session, "execute", execute)


@pytest.mark.asyncio
class TestBeliefRepoFTSFallback:
    """Narrow-except matrix for the FTS fallback (Card 2, Test plan 6).

    Split by failure SITE: (P) probe failures inside ``_check_fts5`` vs
    (M) MATCH-query failures inside the search FTS block. Expected FTS
    failures fall back / disable FTS; real DB failures propagate.
    """

    async def _seed(self, repo):
        await repo.create(Belief(proposition="Docker runs on OMV"))
        await repo.create(Belief(proposition="Nginx proxies web"))

    async def test_fts_normal_path_used(self, repo):
        """Probe passes + MATCH succeeds → FTS used (results found)."""
        await self._seed(repo)
        results = await repo.search(proposition="Docker")
        assert len(results) == 1
        assert results[0].proposition == "Docker runs on OMV"

    async def test_probe_expected_failure_disables_fts_and_falls_back(
        self, repo, monkeypatch, caplog
    ):
        """(P1) probe: 'no such table: beliefs_fts' → FTS off, LIKE used."""
        caplog.set_level(logging.DEBUG, logger="storage.repositories.belief_repo")
        await self._seed(repo)
        _patch_execute(
            repo._session,
            monkeypatch,
            fail_when="sqlite_master",
            exc=sqlite3.OperationalError("no such table: beliefs_fts"),
        )
        assert await repo._check_fts5() is False
        assert repo._fts5_available is False
        assert "FTS unavailable" in caplog.text
        results = await repo.search(proposition="Docker")
        assert len(results) == 1  # LIKE fallback returned results

    async def test_match_expected_failure_falls_through_to_like(
        self, repo, monkeypatch, caplog
    ):
        """(M1) MATCH: 'malformed MATCH expression' → LIKE fallback."""
        caplog.set_level(logging.DEBUG, logger="storage.repositories.belief_repo")
        await self._seed(repo)
        _patch_execute(
            repo._session,
            monkeypatch,
            fail_when="beliefs_fts MATCH",
            exc=sqlite3.OperationalError("malformed MATCH expression"),
        )
        assert await repo._check_fts5() is True  # probe passed
        results = await repo.search(proposition="Docker")
        assert len(results) == 1  # LIKE fallback
        assert "falling back to LIKE" in caplog.text

    async def test_match_unable_to_use_function_falls_through_to_like(
        self, repo, monkeypatch, caplog
    ):
        """(M1b) MATCH: 'unable to use function MATCH ...' → LIKE fallback."""
        caplog.set_level(logging.DEBUG, logger="storage.repositories.belief_repo")
        await self._seed(repo)
        _patch_execute(
            repo._session,
            monkeypatch,
            fail_when="beliefs_fts MATCH",
            exc=sqlite3.OperationalError(
                "unable to use function MATCH in the requested context"
            ),
        )
        results = await repo.search(proposition="Docker")
        assert len(results) == 1

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
            await repo.search(proposition="Docker")
        assert repo._fts5_available is None  # transient failure must not disable FTS

    async def test_match_real_db_failure_propagates(self, repo, monkeypatch):
        """(M2) MATCH: 'database is locked' → propagates (no LIKE fallback)."""
        await self._seed(repo)
        _patch_execute(
            repo._session,
            monkeypatch,
            fail_when="beliefs_fts MATCH",
            exc=sqlite3.OperationalError("database is locked"),
        )
        with pytest.raises(sqlite3.OperationalError):
            await repo.search(proposition="Docker")

    async def test_integrity_error_propagates(self, repo, monkeypatch):
        """Non-OperationalError (IntegrityError) propagates from the FTS site."""
        await self._seed(repo)
        _patch_execute(
            repo._session,
            monkeypatch,
            fail_when="beliefs_fts MATCH",
            exc=SQLAlchemyIntegrityError(
                "SELECT ...", {}, sqlite3.IntegrityError("UNIQUE constraint failed")
            ),
        )
        with pytest.raises(SQLAlchemyIntegrityError):
            await repo.search(proposition="Docker")
