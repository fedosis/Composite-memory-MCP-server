"""Tests for remember MCP tool (Card 006)."""

import pytest

from memory_server.api.remember import remember
from memory_server.models import Fact, MemoryReceipt, VerificationStatus
from memory_server.providers.sqlite_provider import SQLiteProvider


@pytest.fixture
async def provider():
    p = SQLiteProvider(url="sqlite+aiosqlite:///:memory:")
    await p.initialize()
    yield p
    await p.close()


@pytest.mark.asyncio
class TestRemember:
    async def test_store_valid_fact_and_return_receipt(self, provider):
        result = await remember(
            provider,
            subject="Docker",
            predicate="runs_on",
            object="OMV8",
        )
        assert isinstance(result, dict)
        assert "receipt" in result
        receipt = result["receipt"]
        assert isinstance(receipt, MemoryReceipt)
        assert receipt.memory_type == "fact"
        assert receipt.source == "user"
        assert receipt.verification_status == VerificationStatus.CANDIDATE
        assert receipt.confidence == 1.0
        assert receipt.id is not None

    async def test_store_with_confidence_and_source(self, provider):
        result = await remember(
            provider,
            subject="Caddy",
            predicate="uses",
            object="Port 443",
            confidence=0.9,
            source="manual",
        )
        receipt = result["receipt"]
        assert receipt.confidence == 0.9
        assert receipt.source == "manual"

    async def test_retrieve_and_verify_receipt(self, provider):
        result = await remember(
            provider,
            subject="Test",
            predicate="is",
            object="Working",
        )
        receipt = result["receipt"]
        # The receipt should be retrievable via the fact's stored data
        # We can verify the fact was actually stored
        fact_id = receipt.id
        stored_fact = await provider.get_fact(fact_id)
        assert stored_fact is not None
        assert stored_fact.subject == "Test"
        assert stored_fact.predicate == "is"
        assert stored_fact.object == "Working"

        # Also verify the receipt was stored
        stored_receipt = await provider.get_receipt(fact_id)
        assert stored_receipt is not None
        assert stored_receipt.verification_status == VerificationStatus.CANDIDATE

    async def test_store_invalid_data_raises_error(self, provider):
        with pytest.raises(ValueError, match="subject"):
            await remember(
                provider,
                subject="",  # Empty subject should be invalid
                predicate="is",
                object="Test",
            )

    async def test_store_invalid_confidence(self, provider):
        with pytest.raises(ValueError, match="confidence"):
            await remember(
                provider,
                subject="X",
                predicate="is",
                object="Y",
                confidence=2.0,  # Out of [0, 1] range
            )

    async def test_returns_fact_in_result(self, provider):
        result = await remember(
            provider,
            subject="Docker",
            predicate="runs_on",
            object="OMV8",
            source="test",
        )
        assert "fact" in result
        fact = result["fact"]
        assert fact.subject == "Docker"
        assert fact.predicate == "runs_on"
        assert fact.object == "OMV8"
        assert fact.source == "test"
