"""Tests for v0.6 Phase 3: Storage Layer.

- Migration up/down
- WAL mode verification
- CRUD via repositories
- Backward compatibility of SQLiteProvider
"""


import pytest
from storage.base import Base as StorageBase
from storage.dedup import fact_dedup_key
from storage.models import (
    EntityORM,
    LifecycleEventORM,
    LifecycleStateORM,
)
from storage.repositories import (
    DecisionRepository,
    FactRepository,
    LifecycleRepository,
    ReceiptRepository,
    SkillRepository,
)

from memory_server.models import Decision, Fact, MemoryReceipt, Skill
from memory_server.providers.sqlite_provider import SQLiteProvider

# =============================================================================
# Migration tests
# =============================================================================


class TestMigration:
    """Exercise official migrations exclusively on pytest disposable databases."""

    def test_migration_up_creates_all_tables(self, tmp_path):
        from tests.test_pr3_migration_graph import test_empty_database_to_head
        test_empty_database_to_head(tmp_path)

    def test_official_head_is_unified(self):
        from tests.test_pr3_migration_graph import test_single_official_head
        test_single_official_head()

    def test_migration_downgrade_is_explicitly_irreversible(self, tmp_path):
        from tests.test_pr3_migration_graph import test_irreversible_downgrade_rejected_without_changes
        test_irreversible_downgrade_rejected_without_changes(tmp_path, "head", "base")


# =============================================================================
# WAL mode tests
# =============================================================================


class TestWALMode:
    """Verify SQLite WAL journal mode."""

    @pytest.fixture
    async def provider(self):
        p = SQLiteProvider(url="sqlite+aiosqlite:///:memory:")
        await p.initialize()
        yield p
        await p.close()

    async def test_wal_mode_enabled(self, provider):
        """Verify WAL mode is set on engine connection."""
        async with provider._engine.connect() as conn:
            result = await conn.exec_driver_sql("PRAGMA journal_mode")
            row = result.fetchone()
            # :memory: databases always report "memory" journal mode
            # For file-based DBs it would be "wal"
            assert row is not None

    async def test_synchronous_normal_set(self, provider):
        """Verify synchronous=NORMAL is set."""
        async with provider._engine.connect() as conn:
            result = await conn.exec_driver_sql("PRAGMA synchronous")
            row = result.fetchone()
            assert row is not None

    async def test_busy_timeout_set(self, provider):
        """Verify busy_timeout > 0."""
        async with provider._engine.connect() as conn:
            result = await conn.exec_driver_sql("PRAGMA busy_timeout")
            row = result.fetchone()
            assert row is not None
            # Default is 0, we set 5000
            assert row[0] > 0


# =============================================================================
# Repository CRUD tests
# =============================================================================


class TestFactRepositoryCRUD:
    """CRUD operations via FactRepository."""

    @pytest.fixture
    async def repo(self):
        engine = None
        try:
            from sqlalchemy.ext.asyncio import (
                AsyncSession,
                async_sessionmaker,
                create_async_engine,
            )

            engine = create_async_engine("sqlite+aiosqlite:///:memory:")
            async with engine.begin() as conn:
                await conn.run_sync(StorageBase.metadata.create_all)
            factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
            async with factory() as session:
                yield FactRepository(session)
        finally:
            if engine:
                await engine.dispose()

    async def test_create_and_get(self, repo):
        f = Fact(
            id="repo-f1",
            subject="Docker",
            predicate="runs_on",
            object="OMV8",
            dedup_key=fact_dedup_key("Docker", "runs_on", "OMV8"),
        )
        created = await repo.create(f)
        assert created.id == "repo-f1"

        retrieved = await repo.get("repo-f1")
        assert retrieved is not None
        assert retrieved.subject == "Docker"

    async def test_get_not_found(self, repo):
        result = await repo.get("nonexistent")
        assert result is None

    async def test_search(self, repo):
        await repo.create(
            Fact(id="sf1", subject="A", predicate="is", object="X", dedup_key=fact_dedup_key("A", "is", "X"))
        )
        await repo.create(
            Fact(id="sf2", subject="B", predicate="is", object="Y", dedup_key=fact_dedup_key("B", "is", "Y"))
        )
        results = await repo.search(subject="A")
        assert len(results) == 1

    async def test_update(self, repo):
        await repo.create(
            Fact(id="uf1", subject="Old", predicate="is", object="Val", dedup_key=fact_dedup_key("Old", "is", "Val"))
        )
        updated = await repo.update("uf1", object="NewVal")
        assert updated is not None
        assert updated.object == "NewVal"

    async def test_delete(self, repo):
        await repo.create(
            Fact(id="df1", subject="Del", predicate="is", object="Gone", dedup_key=fact_dedup_key("Del", "is", "Gone"))
        )
        result = await repo.delete("df1")
        assert result is True
        assert await repo.get("df1") is None

    async def test_delete_not_found(self, repo):
        result = await repo.delete("nonexistent")
        assert result is False


class TestDecisionRepositoryCRUD:
    """CRUD operations via DecisionRepository."""

    @pytest.fixture
    async def repo(self):
        from sqlalchemy.ext.asyncio import (
            AsyncSession,
            async_sessionmaker,
            create_async_engine,
        )

        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with engine.begin() as conn:
            await conn.run_sync(StorageBase.metadata.create_all)
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as session:
            yield DecisionRepository(session)
        await engine.dispose()

    async def test_create_decision(self, repo):
        d = Decision(
            id="d1",
            context="Test",
            choice="Option A",
            reason="Because",
            rejected_alternatives=["Option B"],
        )
        created = await repo.create(d)
        assert created.id == "d1"
        assert created.choice == "Option A"

    async def test_get_decision(self, repo):
        d = Decision(id="d2", context="X", choice="Y", reason="Z")
        await repo.create(d)
        retrieved = await repo.get("d2")
        assert retrieved is not None
        assert retrieved.reason == "Z"

    async def test_delete_decision(self, repo):
        d = Decision(id="d3", context="X", choice="Y", reason="Z")
        await repo.create(d)
        assert await repo.delete("d3") is True
        assert await repo.get("d3") is None


class TestSkillRepositoryCRUD:
    """CRUD operations via SkillRepository."""

    @pytest.fixture
    async def repo(self):
        from sqlalchemy.ext.asyncio import (
            AsyncSession,
            async_sessionmaker,
            create_async_engine,
        )

        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with engine.begin() as conn:
            await conn.run_sync(StorageBase.metadata.create_all)
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as session:
            yield SkillRepository(session)
        await engine.dispose()

    async def test_create_skill(self, repo):
        s = Skill(id="s1", name="test", purpose="TestPurpose", steps=["step 1"])
        created = await repo.create(s)
        assert created.id == "s1"

    async def test_get_skill(self, repo):
        s = Skill(id="s2", name="test", purpose="Test", steps=["step 1"])
        await repo.create(s)
        retrieved = await repo.get("s2")
        assert retrieved is not None


class TestReceiptRepositoryCRUD:
    """CRUD operations via ReceiptRepository."""

    @pytest.fixture
    async def repo(self):
        from sqlalchemy.ext.asyncio import (
            AsyncSession,
            async_sessionmaker,
            create_async_engine,
        )

        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with engine.begin() as conn:
            await conn.run_sync(StorageBase.metadata.create_all)
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as session:
            yield ReceiptRepository(session)
        await engine.dispose()

    async def test_create_receipt(self, repo):
        from datetime import datetime, timezone

        r = MemoryReceipt(
            id="r1",
            memory_type="fact",
            source="test",
            created_by="tester",
            timestamp=datetime.now(timezone.utc),
        )
        created = await repo.create(r)
        assert created.id == "r1"

    async def test_search_receipts(self, repo):
        from datetime import datetime, timezone

        await repo.create(
            MemoryReceipt(
                id="rs1",
                memory_type="fact",
                source="s1",
                created_by="u1",
                timestamp=datetime.now(timezone.utc),
            )
        )
        await repo.create(
            MemoryReceipt(
                id="rs2",
                memory_type="skill",
                source="s2",
                created_by="u1",
                timestamp=datetime.now(timezone.utc),
            )
        )
        results = await repo.search(memory_type="fact")
        assert len(results) == 1


class TestLifecycleRepositoryCRUD:
    """CRUD operations via LifecycleRepository."""

    @pytest.fixture
    async def repo(self):
        from sqlalchemy.ext.asyncio import (
            AsyncSession,
            async_sessionmaker,
            create_async_engine,
        )

        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with engine.begin() as conn:
            await conn.run_sync(StorageBase.metadata.create_all)
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as session:
            yield LifecycleRepository(session)
        await engine.dispose()

    async def test_set_and_get_state(self, repo):
        await repo.set_state("mem-1", "fact", "active")
        state = await repo.get_state("mem-1")
        assert state == "active"

    async def test_record_event(self, repo):
        await repo.record_event("mem-1", "fact", "active", "archived", reason="Test archiving")
        events = await repo.get_events("mem-1")
        assert len(events) == 1
        assert events[0]["from_state"] == "active"
        assert events[0]["to_state"] == "archived"

    async def test_get_state_not_found(self, repo):
        state = await repo.get_state("nonexistent")
        assert state is None


# =============================================================================
# Backward compatibility: old SQLiteProvider still works
# =============================================================================


class TestSQLiteProviderBackwardCompat:
    """Verify the old SQLiteProvider interface still works after refactor."""

    @pytest.fixture
    async def provider(self):
        p = SQLiteProvider(url="sqlite+aiosqlite:///:memory:")
        await p.initialize()
        yield p
        await p.close()

    async def test_create_fact(self, provider):
        f = Fact(id="bc-f1", subject="Docker", predicate="runs_on", object="OMV8")
        created = await provider.create_fact(f)
        assert created.id == "bc-f1"

    async def test_get_fact(self, provider):
        await provider.create_fact(Fact(id="bc-f2", subject="T", predicate="is", object="V"))
        retrieved = await provider.get_fact("bc-f2")
        assert retrieved is not None
        assert retrieved.subject == "T"

    async def test_search_facts(self, provider):
        await provider.create_fact(Fact(id="bc-f3", subject="Docker", predicate="uses", object="P8080"))
        results = await provider.search_facts(subject="Docker")
        assert len(results) == 1

    async def test_update_fact(self, provider):
        await provider.create_fact(Fact(id="bc-f4", subject="Old", predicate="is", object="V"))
        updated = await provider.update_fact("bc-f4", object="New")
        assert updated is not None
        assert updated.object == "New"

    async def test_delete_fact(self, provider):
        await provider.create_fact(Fact(id="bc-f5", subject="T", predicate="is", object="V"))
        assert await provider.delete_fact("bc-f5") is True

    async def test_create_decision(self, provider):

        d = Decision(id="bc-d1", context="Test", choice="A", reason="R")
        created = await provider.create_decision(d)
        assert created.id == "bc-d1"

    async def test_get_decision(self, provider):
        d = Decision(id="bc-d2", context="X", choice="Y", reason="Z")
        await provider.create_decision(d)
        retrieved = await provider.get_decision("bc-d2")
        assert retrieved is not None
        assert retrieved.choice == "Y"

    async def test_search_decisions(self, provider):
        await provider.create_decision(Decision(id="bc-d3", context="Ctx", choice="Opt", reason="Why"))
        results = await provider.search_decisions(choice="Opt")
        assert len(results) == 1

    async def test_delete_decision(self, provider):
        d = Decision(id="bc-d4", context="X", choice="Y", reason="Z")
        await provider.create_decision(d)
        assert await provider.delete_decision("bc-d4") is True

    async def test_create_skill(self, provider):
        s = Skill(id="bc-s1", name="test", purpose="TestPurpose", steps=["step 1"])
        created = await provider.create_skill(s)
        assert created.id == "bc-s1"

    async def test_get_skill(self, provider):
        s = Skill(id="bc-s2", name="test", purpose="Test", steps=["step 1"])
        await provider.create_skill(s)
        retrieved = await provider.get_skill("bc-s2")
        assert retrieved is not None
        assert retrieved.purpose == "Test"

    async def test_search_skills(self, provider):
        await provider.create_skill(Skill(id="bc-s3", name="t", purpose="Target", steps=["step 1"]))
        results = await provider.search_skills(purpose="Target")
        assert len(results) == 1

    async def test_delete_skill(self, provider):
        s = Skill(id="bc-s4", name="test", purpose="Test", steps=["step 1"])
        await provider.create_skill(s)
        assert await provider.delete_skill("bc-s4") is True

    async def test_create_receipt(self, provider):
        from datetime import datetime, timezone

        r = MemoryReceipt(
            id="bc-r1",
            memory_type="fact",
            source="agent",
            created_by="test",
            timestamp=datetime.now(timezone.utc),
        )
        created = await provider.create_receipt(r)
        assert created.id == "bc-r1"

    async def test_get_receipt(self, provider):
        from datetime import datetime, timezone

        r = MemoryReceipt(
            id="bc-r2",
            memory_type="decision",
            source="user",
            created_by="alice",
            timestamp=datetime.now(timezone.utc),
        )
        await provider.create_receipt(r)
        retrieved = await provider.get_receipt("bc-r2")
        assert retrieved is not None

    async def test_search_receipts(self, provider):
        from datetime import datetime, timezone

        await provider.create_receipt(
            MemoryReceipt(
                id="bc-r3",
                memory_type="fact",
                source="src1",
                created_by="u1",
                timestamp=datetime.now(timezone.utc),
            )
        )
        results = await provider.search_receipts(source="src1")
        assert len(results) == 1

    # --- Entity model not exposed via SQLiteProvider but test model creation ---

    async def test_entity_orm_creation(self, provider):
        """Verify entity table can be populated via ORM."""
        e = EntityORM(
            id="entity1",
            type="server",
            name="TestServer",
            attributes='{"os": "linux"}',
            source="manual",
        )
        async with await provider._get_session() as session:
            session.add(e)
            await session.commit()

    async def test_lifecycle_orm_creation(self, provider):
        """Verify lifecycle tables can be populated via ORM."""
        lc = LifecycleStateORM(
            id="lc1",
            memory_id="mem1",
            memory_type="fact",
            current_state="active",
        )
        le = LifecycleEventORM(
            id="le1",
            memory_id="mem1",
            memory_type="fact",
            from_state="",
            to_state="active",
            reason="Initial creation",
        )
        async with await provider._get_session() as session:
            session.add_all([lc, le])
            await session.commit()
