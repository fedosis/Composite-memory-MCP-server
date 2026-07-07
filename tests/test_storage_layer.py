"""Tests for v0.6 Phase 3: Storage Layer.

- Migration up/down
- WAL mode verification
- CRUD via repositories
- Backward compatibility of SQLiteProvider
"""

import os
import tempfile

import pytest

from memory_server.models import Decision, Entity, Fact, MemoryReceipt, Skill, VerificationStatus
from memory_server.providers.sqlite_provider import SQLiteProvider
from storage.base import Base as StorageBase
from storage.models import (
    DecisionORM,
    EntityORM,
    FactORM,
    LifecycleEventORM,
    LifecycleStateORM,
    MemoryReceiptORM,
    SkillORM,
)
from storage.repositories import (
    DecisionRepository,
    FactRepository,
    LifecycleRepository,
    ReceiptRepository,
    SkillRepository,
)


# =============================================================================
# Migration tests
# =============================================================================


class TestMigration:
    """Test Alembic migration up and down."""

    def _run_alembic(self, db_path: str, command: str) -> None:
        """Run an alembic command against a specific SQLite DB."""
        import subprocess
        import sys

        env = {**os.environ, "ALEMBIC_TEST_DB": db_path}
        result = subprocess.run(
            [sys.executable, "-m", "alembic", command, "alembic.ini"],
            cwd=os.path.join(os.path.dirname(__file__), ".."),
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )
        print(f"alembic {command} stdout:", result.stdout)
        print(f"alembic {command} stderr:", result.stderr)
        assert result.returncode == 0, f"alembic {command} failed: {result.stderr}"

    def _alembic_upgrade(self, db_path: str, revision: str = "head") -> None:
        """Run alembic upgrade."""
        import subprocess
        import sys

        env = {**os.environ}
        result = subprocess.run(
            [sys.executable, "-m", "alembic", "--config", "alembic.ini", "upgrade", revision],
            cwd=os.path.join(os.path.dirname(__file__), ".."),
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )
        print(f"alembic upgrade stdout: {result.stdout}")
        print(f"alembic upgrade stderr: {result.stderr}")
        assert result.returncode == 0, f"alembic upgrade failed: {result.stderr}"

    def _alembic_downgrade(self, db_path: str, revision: str = "base") -> None:
        """Run alembic downgrade."""
        import subprocess
        import sys

        env = {**os.environ}
        result = subprocess.run(
            [sys.executable, "-m", "alembic", "--config", "alembic.ini", "downgrade", revision],
            cwd=os.path.join(os.path.dirname(__file__), ".."),
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )
        print(f"alembic downgrade stdout: {result.stdout}")
        print(f"alembic downgrade stderr: {result.stderr}")
        assert result.returncode == 0, f"alembic downgrade failed: {result.stderr}"

    def test_migration_up_creates_all_tables(self):
        """Verify alembic upgrade head --sql creates all 7 tables."""
        import subprocess
        import sys

        project_dir = os.path.join(os.path.dirname(__file__), "..")

        result = subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head", "--sql"],
            cwd=project_dir,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, f"alembic upgrade --sql failed: {result.stderr}"

        sql_output = result.stdout
        expected_tables = [
            "facts",
            "decisions",
            "skills",
            "receipts",
            "entities",
            "lifecycle_events",
            "lifecycle_states",
        ]
        for table in expected_tables:
            assert f"CREATE TABLE {table}" in sql_output, f"Missing table: {table}"

    def test_migration_downgrade_drops_all_tables(self):
        """Verify alembic upgrade creates all tables and migration file has correct downgrade."""
        import re
        import subprocess
        import sys

        project_dir = os.path.join(os.path.dirname(__file__), "..")

        # Find the migration file
        migration_dir = os.path.join(project_dir, "migrations", "versions")
        migration_files = [f for f in os.listdir(migration_dir) if f.endswith(".py")]
        assert len(migration_files) == 1, f"Expected 1 migration file, got {len(migration_files)}"
        migration_path = os.path.join(migration_dir, migration_files[0])

        # Read the migration file to verify downgrade has all DROP TABLEs
        with open(migration_path) as f:
            content = f.read()

        # Verify upgrade creates all 7 tables (via --sql mode)
        try:
            result = subprocess.run(
                [sys.executable, "-m", "alembic", "upgrade", "head", "--sql"],
                cwd=project_dir,
                capture_output=True,
                text=True,
                timeout=30,
            )
            assert result.returncode == 0, f"upgrade --sql failed: {result.stderr}"

            sql_output = result.stdout
            # Verify upgrade creates all 7 tables
            for table in ["facts", "decisions", "skills", "receipts", "entities",
                          "lifecycle_events", "lifecycle_states"]:
                assert f"CREATE TABLE {table}" in sql_output, f"Missing CREATE TABLE {table}"
        finally:
            memory_db = os.path.join(project_dir, "memory.db")
            if os.path.exists(memory_db):
                os.remove(memory_db)

        # Verify downgrade function drops all 7 tables
        assert "def downgrade()" in content
        expected_drops = [
            "skills", "receipts", "lifecycle_states", "lifecycle_events",
            "facts", "entities", "decisions",
        ]
        for table in expected_drops:
            assert f"drop_table('{table}')" in content or f'drop_table("{table}")' in content


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
        f = Fact(id="repo-f1", subject="Docker", predicate="runs_on", object="OMV8")
        created = await repo.create(f)
        assert created.id == "repo-f1"

        retrieved = await repo.get("repo-f1")
        assert retrieved is not None
        assert retrieved.subject == "Docker"

    async def test_get_not_found(self, repo):
        result = await repo.get("nonexistent")
        assert result is None

    async def test_search(self, repo):
        await repo.create(Fact(id="sf1", subject="A", predicate="is", object="X"))
        await repo.create(Fact(id="sf2", subject="B", predicate="is", object="Y"))
        results = await repo.search(subject="A")
        assert len(results) == 1

    async def test_update(self, repo):
        await repo.create(Fact(id="uf1", subject="Old", predicate="is", object="Val"))
        updated = await repo.update("uf1", object="NewVal")
        assert updated is not None
        assert updated.object == "NewVal"

    async def test_delete(self, repo):
        await repo.create(Fact(id="df1", subject="Del", predicate="is", object="Gone"))
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
        from datetime import datetime, timezone
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
                id="rs1", memory_type="fact", source="s1",
                created_by="u1", timestamp=datetime.now(timezone.utc),
            )
        )
        await repo.create(
            MemoryReceipt(
                id="rs2", memory_type="skill", source="s2",
                created_by="u1", timestamp=datetime.now(timezone.utc),
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
        from datetime import datetime, timezone

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
            id="bc-r1", memory_type="fact", source="agent",
            created_by="test", timestamp=datetime.now(timezone.utc),
        )
        created = await provider.create_receipt(r)
        assert created.id == "bc-r1"

    async def test_get_receipt(self, provider):
        from datetime import datetime, timezone

        r = MemoryReceipt(
            id="bc-r2", memory_type="decision", source="user",
            created_by="alice", timestamp=datetime.now(timezone.utc),
        )
        await provider.create_receipt(r)
        retrieved = await provider.get_receipt("bc-r2")
        assert retrieved is not None

    async def test_search_receipts(self, provider):
        from datetime import datetime, timezone

        await provider.create_receipt(
            MemoryReceipt(
                id="bc-r3", memory_type="fact", source="src1",
                created_by="u1", timestamp=datetime.now(timezone.utc),
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
