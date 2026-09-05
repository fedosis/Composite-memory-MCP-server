"""Tests for remember MCP tool (Card 006)."""

import pytest
from sqlalchemy import func, select
from storage.models.fact import FactORM

from memory_server.api.remember import remember
from memory_server.models import MemoryReceipt, VerificationStatus
from memory_server.providers.graph_provider import SimpleGraph
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

    async def test_store_with_evidence_chain(self, provider):
        graph = SimpleGraph()
        parent = await remember(
            provider,
            subject="Docker",
            predicate="runs_on",
            object="OMV8",
            graph=graph,
        )
        parent_id = parent["receipt"].id

        result = await remember(
            provider,
            subject="OMV8",
            predicate="runs",
            object="Docker",
            metadata={
                "evidence": {
                    "method": "inference",
                    "sources": ["parent-fact"],
                    "session_id": "session-123",
                    "confidence": 0.8,
                    "source_date": "2026-08-13",
                    "derived_from": [parent_id],
                    "claim_type": "fact",
                },
                "scope": "derived",
                "ttl_days": 30,
            },
            graph=graph,
        )

        receipt = result["receipt"]
        assert receipt.history
        assert receipt.history[0]["metadata"]["evidence"]["derived_from"] == [parent_id]
        assert receipt.history[0]["metadata"]["evidence"]["claim_type"] == "fact"
        assert receipt.history[0]["metadata"]["scope"] == "derived"
        assert receipt.history[0]["metadata"]["ttl_days"] == 30

        child_fact = result["fact"]
        edge = graph.get_edge(child_fact.id, parent_id)
        assert edge is not None
        assert edge.relation == "derived_from"

        sql_dependents = await provider.get_claim_dependents(parent_id)
        assert child_fact.id in sql_dependents

    async def test_store_with_backward_compatible_metadata(self, provider):
        result = await remember(
            provider,
            subject="Caddy",
            predicate="listens_on",
            object="443",
            metadata={
                "method": "web_search",
                "sources": ["https://example.com"],
                "session_id": "sess-1",
                "confidence": 0.7,
                "source_date": "2026-08-13",
                "scope": "user",
                "ttl_days": 7,
            },
        )

        receipt = result["receipt"]
        assert receipt.history
        assert receipt.history[0]["metadata"]["scope"] == "user"
        assert receipt.history[0]["metadata"]["ttl_days"] == 7
        assert "evidence" not in receipt.history[0]["metadata"]

    async def test_invalid_derived_from_raises_error(self, provider):
        with pytest.raises(ValueError, match="derived_from"):
            await remember(
                provider,
                subject="X",
                predicate="is",
                object="Y",
                metadata={
                    "evidence": {
                        "derived_from": "not-a-list",
                        "claim_type": "fact",
                    }
                },
            )

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

    async def test_search_fts5_still_works_after_evidence_metadata(self, provider):
        await remember(
            provider,
            subject="Docker",
            predicate="runs_on",
            object="OMV8",
            metadata={
                "evidence": {
                    "derived_from": [],
                    "claim_type": "fact",
                }
            },
        )

        results = await provider.search_facts(text="Docker")
        assert results
        assert any(f.subject == "Docker" for f in results)

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

    async def test_repeated_fact_is_reinforced_without_duplicate_row(self, provider):
        first = await remember(
            provider, subject="Repeat", predicate="is", object="stable", confidence=0.4
        )
        second = await remember(
            provider, subject="Repeat", predicate="is", object="stable", confidence=0.9
        )

        assert second["receipt"].id == first["receipt"].id
        assert second["fact"].id == first["fact"].id
        assert second["fact"].confidence == 0.9
        async with provider._session_factory() as session:
            count = await session.scalar(select(func.count()).select_from(FactORM))
        assert count == 1
