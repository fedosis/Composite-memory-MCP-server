"""Integration tests for SQLiteProvider belief CRUD (Card 001)."""

import pytest

from memory_server.models import Belief, Evidence
from memory_server.providers.sqlite_provider import SQLiteProvider


@pytest.fixture
async def provider():
    p = SQLiteProvider(url="sqlite+aiosqlite:///:memory:")
    await p.initialize()
    yield p
    await p.close()


@pytest.mark.asyncio
class TestBeliefProviderCRUD:
    async def test_create_belief(self, provider):
        b = Belief(proposition="Docker runs on OMV8", confidence=0.9)
        created = await provider.create_belief(b)
        assert created.id == b.id
        assert created.proposition == "Docker runs on OMV8"
        assert created.confidence == 0.9

    async def test_create_belief_with_evidence(self, provider):
        b = Belief(proposition="Caddy is a web server", confidence=0.85)
        evidence = [
            Evidence(belief_id=b.id, source_type="fact", source_id="f1", weight=0.8),
            Evidence(belief_id=b.id, source_type="observation", source_id="obs-1", weight=0.6),
        ]
        created = await provider.create_belief(b, evidence)
        assert created.id == b.id
        assert created.proposition == "Caddy is a web server"

    async def test_get_belief(self, provider):
        b = Belief(proposition="Test belief", tags=["important"])
        created = await provider.create_belief(b)
        retrieved = await provider.get_belief(created.id)
        assert retrieved is not None
        assert retrieved.proposition == "Test belief"
        assert "important" in retrieved.tags

    async def test_get_belief_not_found(self, provider):
        result = await provider.get_belief("nonexistent")
        assert result is None

    async def test_search_beliefs_default(self, provider):
        """Search without filters returns active beliefs."""
        b1 = await provider.create_belief(Belief(proposition="First", lifecycle_state="active"))
        b2 = await provider.create_belief(Belief(proposition="Second", lifecycle_state="superseded"))

        results = await provider.search_beliefs()
        assert len(results) >= 1

    async def test_search_beliefs_by_proposition(self, provider):
        await provider.create_belief(Belief(proposition="Docker deployment"))
        await provider.create_belief(Belief(proposition="Kubernetes cluster"))

        results = await provider.search_beliefs(proposition="Docker")
        assert len(results) == 1
        assert "Docker" in results[0].proposition

    async def test_search_beliefs_by_tags(self, provider):
        await provider.create_belief(Belief(proposition="Prod config", tags=["prod", "docker"]))
        await provider.create_belief(Belief(proposition="Dev config", tags=["dev", "docker"]))

        results = await provider.search_beliefs(tags=["prod"])
        assert len(results) == 1
        assert "prod" in results[0].tags

    async def test_search_beliefs_by_lifecycle_state(self, provider):
        await provider.create_belief(Belief(proposition="Active", lifecycle_state="active"))
        await provider.create_belief(Belief(proposition="Superseded", lifecycle_state="superseded"))

        results = await provider.search_beliefs(lifecycle_state="superseded")
        assert len(results) == 1
        assert results[0].lifecycle_state == "superseded"

    async def test_search_beliefs_by_source(self, provider):
        await provider.create_belief(Belief(proposition="Manual fact", source="manual"))
        await provider.create_belief(Belief(proposition="Auto fact", source="auto"))

        results = await provider.search_beliefs(source="manual")
        assert len(results) == 1

    async def test_search_beliefs_by_creator(self, provider):
        await provider.create_belief(Belief(proposition="Alice idea", creator="alice"))
        await provider.create_belief(Belief(proposition="Bob idea", creator="bob"))

        results = await provider.search_beliefs(creator="alice")
        assert len(results) == 1

    async def test_search_beliefs_by_min_confidence(self, provider):
        await provider.create_belief(Belief(proposition="High", confidence=0.9))
        await provider.create_belief(Belief(proposition="Low", confidence=0.3))

        results = await provider.search_beliefs(min_confidence=0.5)
        assert len(results) == 1
        assert results[0].confidence >= 0.5

    async def test_search_beliefs_limit(self, provider):
        for i in range(10):
            await provider.create_belief(Belief(proposition=f"Belief {i}"))
        results = await provider.search_beliefs(limit=3)
        assert len(results) == 3

    async def test_update_belief_confidence(self, provider):
        b = await provider.create_belief(Belief(proposition="Test", confidence=0.5))
        updated = await provider.update_belief_confidence(b.id, 0.9)
        assert updated is not None
        assert updated.confidence == 0.9

    async def test_update_belief_lifecycle(self, provider):
        b = await provider.create_belief(Belief(proposition="Test"))
        updated = await provider.update_belief_lifecycle(b.id, "superseded")
        assert updated is not None
        assert updated.lifecycle_state == "superseded"

    async def test_create_in_transaction(self, provider):
        from memory_server.models.receipt import MemoryReceipt
        from datetime import datetime, timezone

        b = Belief(proposition="Transaction test")
        r = MemoryReceipt(
            id=b.id,
            memory_type="belief",
            source="test",
            created_by="tester",
            timestamp=datetime.now(timezone.utc),
        )
        ev = Evidence(belief_id=b.id, source_type="fact", source_id="f-txn", weight=0.5)

        await provider.create_in_transaction(
            belief=b,
            evidence_list=[ev],
            receipt=r,
            outbox_entries=[
                {
                    "record_type": "belief",
                    "record_id": b.id,
                    "operation": "index_belief",
                    "payload": {"proposition": b.proposition, "tags": [], "confidence": 0.5, "source": "test"},
                }
            ],
        )

        retrieved = await provider.get_belief(b.id)
        assert retrieved is not None
        assert retrieved.proposition == "Transaction test"
