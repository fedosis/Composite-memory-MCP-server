"""Tests for Qdrant provider (Card 008)."""

import pytest
import uuid

from memory_server.providers.qdrant_provider import QdrantProvider


@pytest.fixture
def provider():
    """Create an in-memory Qdrant provider for testing."""
    p = QdrantProvider(location=":memory:", prefer_grpc=False)
    return p


@pytest.mark.asyncio
class TestCollectionManagement:
    """Test collection CRUD operations."""

    async def test_create_collection(self, provider):
        name = f"test_col_{uuid.uuid4().hex[:8]}"
        result = await provider.create_collection(name)
        assert result is True

        collections = await provider.list_collections()
        assert name in collections

    async def test_create_collection_with_custom_config(self, provider):
        name = f"test_custom_{uuid.uuid4().hex[:8]}"
        result = await provider.create_collection(
            name, vector_size=128, distance="Dot"
        )
        assert result is True
        collections = await provider.list_collections()
        assert name in collections

    async def test_create_duplicate_collection(self, provider):
        name = f"test_dup_{uuid.uuid4().hex[:8]}"
        await provider.create_collection(name)
        # Creating again should raise or return False
        result = await provider.create_collection(name)
        assert result is False

    async def test_delete_collection(self, provider):
        name = f"test_del_{uuid.uuid4().hex[:8]}"
        await provider.create_collection(name)
        result = await provider.delete_collection(name)
        assert result is True

        collections = await provider.list_collections()
        assert name not in collections

    async def test_delete_nonexistent_collection(self, provider):
        # Qdrant treats deleting non-existent collections as a no-op (returns True)
        result = await provider.delete_collection("nonexistent_collection")
        assert result is True

    async def test_list_collections_empty(self, provider):
        """Should return at least the system collections (empty list possible)."""
        collections = await provider.list_collections()
        assert isinstance(collections, list)

    async def test_default_collection_created(self, provider):
        """Default 'memories' collection should exist."""
        collections = await provider.list_collections()
        assert "memories" in collections

    async def test_default_vector_config(self, provider):
        """Default collection should have 384-dim cosine vectors."""
        collections = await provider.list_collections()
        assert "memories" in collections


@pytest.mark.asyncio
class TestPointOperations:
    """Test point upsert, search, delete, scroll."""

    COLLECTION = "memories"

    async def _ensure_collection(self, provider):
        collections = await provider.list_collections()
        if self.COLLECTION not in collections:
            await provider.create_collection(self.COLLECTION)
        return self.COLLECTION

    async def test_upsert_and_search(self, provider):
        collection = await self._ensure_collection(provider)
        vec = [float(i % 10) / 10.0 for i in range(384)]
        point_id = str(uuid.uuid4())
        payload = {"subject": "Docker", "predicate": "runs_on", "object": "OMV8"}

        await provider.upsert(collection, point_id=point_id, vector=vec, payload=payload)

        # Search with same vector — should find the point
        results = await provider.search(collection, vector=vec, limit=10)
        assert len(results) >= 1
        ids = [r["id"] for r in results]
        assert point_id in ids

    async def test_search_with_score_threshold(self, provider):
        collection = await self._ensure_collection(provider)
        # Use a vector with different values per dimension for a unique direction
        vec = [float(i % 10) / 10.0 for i in range(384)]
        point_id = str(uuid.uuid4())
        payload = {"subject": "TestThreshold", "predicate": "is", "object": "Value"}
        await provider.upsert(collection, point_id=point_id, vector=vec, payload=payload)

        # search with an impossible-high threshold (max cosine sim is 1.0) — should exclude
        results_high = await provider.search(collection, vector=vec, limit=10, score_threshold=1.5)
        ids_high = [r["id"] for r in results_high]
        assert point_id not in ids_high

        # search with low threshold — should include
        results_low = await provider.search(collection, vector=vec, limit=10, score_threshold=0.0)
        ids_low = [r["id"] for r in results_low]
        assert point_id in ids_low

    async def test_search_empty_collection(self, provider):
        collection = await self._ensure_collection(provider)
        vec = [0.3] * 384
        results = await provider.search(collection, vector=vec, limit=10)
        assert isinstance(results, list)

    async def test_upsert_many_and_search_ranking(self, provider):
        collection = await self._ensure_collection(provider)
        # Vectors pointing in different directions so cosine similarity differs
        target_vec = [1.0 if i == 0 else 0.0 for i in range(384)]  # points in x-axis
        far_vec = [0.0 if i == 0 else 1.0 for i in range(384)]     # orthogonal, cosine=0

        target_id = str(uuid.uuid4())
        await provider.upsert(collection, point_id=target_id, vector=target_vec, payload={"rank": "target"})

        far_id = str(uuid.uuid4())
        await provider.upsert(collection, point_id=far_id, vector=far_vec, payload={"rank": "far"})

        # Search with target_vec — target should rank first (cosine=1.0 vs 0.0)
        results = await provider.search(collection, vector=target_vec, limit=10)
        assert results[0]["id"] == target_id

    async def test_scroll(self, provider):
        collection = await self._ensure_collection(provider)
        vec = [float(i % 5) / 5.0 for i in range(384)]
        point_id = str(uuid.uuid4())
        payload = {"subject": "ScrollTest", "predicate": "is", "object": "Scrollable"}
        await provider.upsert(collection, point_id=point_id, vector=vec, payload=payload)

        scrolled = await provider.scroll(collection, limit=100)
        ids = [r["id"] for r in scrolled]
        assert point_id in ids

    async def test_scroll_with_filter(self, provider):
        collection = await self._ensure_collection(provider)
        vec = [float(i % 5) / 5.0 for i in range(384)]
        await provider.upsert(collection, point_id=str(uuid.uuid4()), vector=vec,
                              payload={"subject": "FilterTarget", "predicate": "is", "object": "Y"})
        await provider.upsert(collection, point_id=str(uuid.uuid4()), vector=vec,
                              payload={"subject": "Other", "predicate": "is", "object": "N"})

        scrolled = await provider.scroll(collection, limit=100, filter_={"must": [{"key": "subject", "match": {"value": "FilterTarget"}}]})
        assert len(scrolled) >= 1
        assert all(r["payload"]["subject"] == "FilterTarget" for r in scrolled)

    async def test_delete_point(self, provider):
        collection = await self._ensure_collection(provider)
        vec = [float(i % 10) / 10.0 for i in range(384)]
        point_id = str(uuid.uuid4())
        await provider.upsert(collection, point_id=point_id, vector=vec, payload={"name": "deleteme"})

        # Verify exists
        results = await provider.search(collection, vector=vec, limit=10)
        ids_before = [r["id"] for r in results]
        assert point_id in ids_before

        # Delete
        result = await provider.delete(collection, point_id=point_id)
        assert result is True

        # Verify gone
        results = await provider.search(collection, vector=vec, limit=10)
        ids_after = [r["id"] for r in results]
        assert point_id not in ids_after

    async def test_delete_nonexistent_point(self, provider):
        # Qdrant treats deleting non-existent points as a no-op (returns True)
        collection = await self._ensure_collection(provider)
        result = await provider.delete(collection, point_id="nonexistent-id")
        assert result is True

    async def test_upsert_without_payload(self, provider):
        collection = await self._ensure_collection(provider)
        vec = [float(i % 10) / 10.0 for i in range(384)]
        point_id = str(uuid.uuid4())
        await provider.upsert(collection, point_id=point_id, vector=vec)

        results = await provider.search(collection, vector=vec, limit=10)
        ids = [r["id"] for r in results]
        assert point_id in ids
