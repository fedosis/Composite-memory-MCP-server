"""Tests for SQLite provider (Card 003)."""

import pytest

from memory_server.models import Fact, MemoryReceipt, VerificationStatus
from memory_server.providers.sqlite_provider import SQLiteProvider


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
        f = Fact(id="f1", subject="Docker", predicate="runs_on", object="OMV8")
        created = await provider.create_fact(f)
        assert created.id == "f1"
        assert created.subject == "Docker"

    async def test_get_fact(self, provider):
        f = Fact(id="f2", subject="Test", predicate="is", object="Working")
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
            Fact(id="f3", subject="Docker", predicate="uses", object="Port 8080")
        )
        await provider.create_fact(
            Fact(id="f4", subject="Nginx", predicate="uses", object="Port 80")
        )
        results = await provider.search_facts(subject="Docker")
        assert len(results) == 1
        assert results[0].id == "f3"

    async def test_search_facts_by_predicate(self, provider):
        await provider.create_fact(Fact(id="f5", subject="A", predicate="runs_on", object="X"))
        await provider.create_fact(Fact(id="f6", subject="B", predicate="depends_on", object="Y"))
        results = await provider.search_facts(predicate="runs_on")
        assert len(results) == 1
        assert results[0].id == "f5"

    async def test_search_facts_by_object(self, provider):
        await provider.create_fact(Fact(id="f7", subject="S1", predicate="has", object="Target"))
        results = await provider.search_facts(object="Target")
        assert len(results) == 1

    async def test_search_facts_by_source(self, provider):
        await provider.create_fact(
            Fact(id="f8", subject="X", predicate="is", object="Y", source="manual")
        )
        await provider.create_fact(
            Fact(id="f9", subject="X", predicate="is", object="Z", source="auto")
        )
        results = await provider.search_facts(source="manual")
        assert len(results) == 1

    async def test_search_facts_text_search(self, provider):
        await provider.create_fact(
            Fact(id="f10", subject="Docker", predicate="is", object="Container")
        )
        await provider.create_fact(
            Fact(id="f11", subject="Caddy", predicate="is", object="Web Server")
        )
        results = await provider.search_facts(text="Docker")
        assert len(results) == 1

    async def test_search_facts_empty_results(self, provider):
        results = await provider.search_facts(subject="DoesNotExist")
        assert results == []

    async def test_update_fact(self, provider):
        f = Fact(id="f12", subject="Old", predicate="is", object="Value")
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
        f = Fact(id="f13", subject="Temp", predicate="is", object="Removed")
        await provider.create_fact(f)
        result = await provider.delete_fact("f13")
        assert result is True
        retrieved = await provider.get_fact("f13")
        assert retrieved is None

    async def test_delete_fact_not_found(self, provider):
        result = await provider.delete_fact("nonexistent")
        assert result is False


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
