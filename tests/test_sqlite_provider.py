"""Tests for SQLite provider (Card 003)."""

import pytest
from sqlalchemy import event
from storage.adapters.legacy_provider import LegacySQLiteProviderAdapter
from storage.dedup import fact_dedup_key
from storage.outbox_worker import OutboxWorker

import memory_server.providers.sqlite_provider as sqlite_provider_module
from memory_server.models import Fact as DomainFact
from memory_server.models import MemoryReceipt, VerificationStatus
from memory_server.providers.sqlite_provider import SQLiteProvider


def make_fact(**kwargs):
    return DomainFact(
        **kwargs,
        dedup_key=fact_dedup_key(kwargs["subject"], kwargs["predicate"], kwargs["object"]),
    )


@pytest.fixture
async def provider():
    """Create an in-memory SQLite provider for testing."""
    p = SQLiteProvider(url="sqlite+aiosqlite:///:memory:")
    await p.initialize()
    yield p
    await p.close()


@pytest.mark.asyncio
class TestFactCRUD:
    async def test_create_fact(self, provider):
        f = make_fact(id="f1", subject="Docker", predicate="runs_on", object="OMV8")
        created = await provider.create_fact(f)
        assert created.id == "f1"
        assert created.subject == "Docker"

    async def test_get_fact(self, provider):
        f = make_fact(id="f2", subject="Test", predicate="is", object="Working")
        await provider.create_fact(f)
        retrieved = await provider.get_fact("f2")
        assert retrieved is not None
        assert retrieved.subject == "Test"
        assert retrieved.predicate == "is"
        assert retrieved.object == "Working"

    async def test_get_fact_not_found(self, provider):
        result = await provider.get_fact("nonexistent")
        assert result is None

    async def test_search_facts_by_subject(self, provider):
        await provider.create_fact(
            make_fact(id="f3", subject="Docker", predicate="uses", object="Port 8080")
        )
        await provider.create_fact(
            make_fact(id="f4", subject="Nginx", predicate="uses", object="Port 80")
        )
        results = await provider.search_facts(subject="Docker")
        assert len(results) == 1
        assert results[0].id == "f3"

    async def test_search_facts_by_predicate(self, provider):
        await provider.create_fact(make_fact(id="f5", subject="A", predicate="runs_on", object="X"))
        await provider.create_fact(make_fact(id="f6", subject="B", predicate="depends_on", object="Y"))
        results = await provider.search_facts(predicate="runs_on")
        assert len(results) == 1
        assert results[0].id == "f5"

    async def test_search_facts_by_object(self, provider):
        await provider.create_fact(make_fact(id="f7", subject="S1", predicate="has", object="Target"))
        results = await provider.search_facts(object="Target")
        assert len(results) == 1

    async def test_search_facts_by_source(self, provider):
        await provider.create_fact(
            make_fact(id="f8", subject="X", predicate="is", object="Y", source="manual")
        )
        await provider.create_fact(
            make_fact(id="f9", subject="X", predicate="is", object="Z", source="auto")
        )
        results = await provider.search_facts(source="manual")
        assert len(results) == 1

    async def test_search_facts_text_search(self, provider):
        await provider.create_fact(
            make_fact(id="f10", subject="Docker", predicate="is", object="Container")
        )
        await provider.create_fact(
            make_fact(id="f11", subject="Caddy", predicate="is", object="Web Server")
        )
        results = await provider.search_facts(text="Docker")
        assert len(results) == 1

    async def test_search_facts_empty_results(self, provider):
        results = await provider.search_facts(subject="DoesNotExist")
        assert results == []

    async def test_search_facts_excludes_inactive_by_default(self, provider):
        active = await provider.create_fact(
            make_fact(id="f-inactive-1", subject="Active", predicate="is", object="Visible")
        )
        inactive = await provider.create_fact(
            make_fact(id="f-inactive-2", subject="Old", predicate="is", object="Hidden")
        )
        await provider.update_fact(inactive.id, lifecycle_state="superseded")

        default_results = await provider.search_facts(limit=10)
        assert [fact.id for fact in default_results] == [active.id]

        all_results = await provider.search_facts(limit=10, include_inactive=True)
        assert {fact.id for fact in all_results} == {active.id, inactive.id}

    async def test_update_fact(self, provider):
        f = make_fact(id="f12", subject="Old", predicate="is", object="Value")
        await provider.create_fact(f)
        updated = await provider.update_fact("f12", object="NewValue")
        assert updated is not None
        assert updated.object == "NewValue"
        # Verify persisted
        retrieved = await provider.get_fact("f12")
        assert retrieved.object == "NewValue"

    async def test_update_fact_not_found(self, provider):
        result = await provider.update_fact("nonexistent", object="value")
        assert result is None

    async def test_delete_fact(self, provider):
        f = make_fact(id="f13", subject="Temp", predicate="is", object="Removed")
        await provider.create_fact(f)
        result = await provider.delete_fact("f13")
        assert result is True
        retrieved = await provider.get_fact("f13")
        assert retrieved is None

    async def test_delete_fact_not_found(self, provider):
        result = await provider.delete_fact("nonexistent")
        assert result is False


@pytest.mark.asyncio
class TestProviderInitialization:
    async def test_initialize_skips_facts_fts_rebuild_when_index_already_populated(self, tmp_path, monkeypatch):
        db_url = f"sqlite+aiosqlite:///{tmp_path / 'provider.db'}"

        provider = SQLiteProvider(url=db_url)
        await provider.initialize()
        await provider.create_fact(
            make_fact(id="fts-existing", subject="Docker", predicate="runs_on", object="OMV")
        )
        await provider.close()

        executed_sql: list[str] = []
        original_create_async_engine = sqlite_provider_module.create_async_engine

        def instrumented_create_async_engine(*args, **kwargs):
            engine = original_create_async_engine(*args, **kwargs)

            def capture_sql(conn, cursor, statement, parameters, context, executemany):
                executed_sql.append(statement)

            event.listen(engine.sync_engine, "before_cursor_execute", capture_sql)
            return engine

        monkeypatch.setattr(
            sqlite_provider_module,
            "create_async_engine",
            instrumented_create_async_engine,
        )

        provider = SQLiteProvider(url=db_url)
        await provider.initialize()
        results = await provider.search_facts(text="Docker")
        await provider.close()

        assert [fact.id for fact in results] == ["fts-existing"]
        assert not any(
            "facts_fts" in statement and "'rebuild'" in statement
            for statement in executed_sql
        )

    async def test_initialize_rebuilds_facts_fts_when_index_is_missing(self, tmp_path):
        db_url = f"sqlite+aiosqlite:///{tmp_path / 'provider.db'}"

        provider = SQLiteProvider(url=db_url)
        await provider.initialize()
        await provider.create_fact(
            make_fact(id="fts-rebuild", subject="Docker", predicate="runs_on", object="OMV")
        )

        engine = provider.engine
        assert engine is not None
        async with engine.begin() as conn:
            await conn.exec_driver_sql("DROP TRIGGER IF EXISTS facts_ai")
            await conn.exec_driver_sql("DROP TRIGGER IF EXISTS facts_ad")
            await conn.exec_driver_sql("DROP TRIGGER IF EXISTS facts_au")
            await conn.exec_driver_sql("DROP TABLE IF EXISTS facts_fts")

        await provider.close()

        provider = SQLiteProvider(url=db_url)
        await provider.initialize()
        results = await provider.search_facts(text="Docker")

        engine = provider.engine
        assert engine is not None
        async with engine.connect() as conn:
            facts_fts_count = await conn.exec_driver_sql("SELECT count(*) FROM facts_fts")
            count = facts_fts_count.scalar_one()

        await provider.close()

        assert [fact.id for fact in results] == ["fts-rebuild"]
        assert count == 1


@pytest.mark.asyncio
class TestFileBackedJournalMode:
    async def test_file_backed_connections_have_wal_and_busy_timeout(self, tmp_path):
        db_url = f"sqlite+aiosqlite:///{tmp_path / 'journal-policy.db'}"

        provider = SQLiteProvider(url=db_url)
        await provider.initialize()
        try:
            engine = provider.engine
            assert engine is not None
            async with engine.connect() as conn:
                journal_mode = (await conn.exec_driver_sql("PRAGMA journal_mode")).scalar_one()
                busy_timeout = (await conn.exec_driver_sql("PRAGMA busy_timeout")).scalar_one()
            assert str(journal_mode).lower() == "wal"
            assert busy_timeout == provider._busy_timeout_ms
        finally:
            await provider.close()

        adapter = LegacySQLiteProviderAdapter(url=db_url)
        await adapter.initialize()
        try:
            engine = adapter._engine
            assert engine is not None
            async with engine.connect() as conn:
                journal_mode = (await conn.exec_driver_sql("PRAGMA journal_mode")).scalar_one()
                busy_timeout = (await conn.exec_driver_sql("PRAGMA busy_timeout")).scalar_one()
            assert str(journal_mode).lower() == "wal"
            assert busy_timeout == 5000
        finally:
            await adapter.close()

        provider_for_worker = SQLiteProvider(url=db_url)
        await provider_for_worker.initialize()
        worker = OutboxWorker(engine=provider_for_worker.engine, db_url=db_url)
        await worker.initialize()
        try:
            engine = worker._engine
            assert engine is not None
            async with engine.connect() as conn:
                journal_mode = (await conn.exec_driver_sql("PRAGMA journal_mode")).scalar_one()
                busy_timeout = (await conn.exec_driver_sql("PRAGMA busy_timeout")).scalar_one()
            assert str(journal_mode).lower() == "wal"
            assert busy_timeout == worker._busy_timeout_ms
        finally:
            await worker.close()
            await provider_for_worker.close()


@pytest.mark.asyncio
class TestReceiptCRUD:
    async def test_create_receipt(self, provider):
        from datetime import datetime, timezone

        r = MemoryReceipt(
            id="r1",
            memory_type="fact",
            source="agent1",
            created_by="test",
            timestamp=datetime.now(timezone.utc),
        )
        created = await provider.create_receipt(r)
        assert created.id == "r1"
        assert created.memory_type == "fact"

    async def test_get_receipt(self, provider):
        from datetime import datetime, timezone

        r = MemoryReceipt(
            id="r2",
            memory_type="decision",
            source="user",
            created_by="alice",
            timestamp=datetime.now(timezone.utc),
            confidence=0.8,
            verification_status=VerificationStatus.CANDIDATE,
        )
        await provider.create_receipt(r)
        retrieved = await provider.get_receipt("r2")
        assert retrieved is not None
        assert retrieved.source == "user"
        assert retrieved.verification_status == VerificationStatus.CANDIDATE

    async def test_get_receipt_not_found(self, provider):
        result = await provider.get_receipt("nonexistent")
        assert result is None

    async def test_search_receipts_by_source(self, provider):
        from datetime import datetime, timezone

        await provider.create_receipt(
            MemoryReceipt(
                id="r3", memory_type="fact", source="test-src",
                created_by="u1", timestamp=datetime.now(timezone.utc),
            )
        )
        await provider.create_receipt(
            MemoryReceipt(
                id="r4", memory_type="fact", source="other-src",
                created_by="u2", timestamp=datetime.now(timezone.utc),
            )
        )
        results = await provider.search_receipts(source="test-src")
        assert len(results) == 1
        assert results[0].id == "r3"

    async def test_search_receipts_by_memory_type(self, provider):
        from datetime import datetime, timezone

        await provider.create_receipt(
            MemoryReceipt(
                id="r5", memory_type="fact", source="s1",
                created_by="u1", timestamp=datetime.now(timezone.utc),
            )
        )
        await provider.create_receipt(
            MemoryReceipt(
                id="r6", memory_type="skill", source="s1",
                created_by="u1", timestamp=datetime.now(timezone.utc),
            )
        )
        results = await provider.search_receipts(memory_type="fact")
        assert len(results) == 1
        assert results[0].id == "r5"
