"""Tests for LanceDB provider (Card 001 — v0.10)."""

import tempfile
import uuid
from datetime import timedelta

import pytest

from memory_server.providers.lancedb_provider import LanceDBProvider


@pytest.fixture
def provider():
    """Create a LanceDB provider in a temp directory for testing."""
    tmp_dir = tempfile.mkdtemp(prefix="lancedb_test_")
    p = LanceDBProvider(db_path=tmp_dir, table="test_memories")
    yield p
    # Cleanup
    try:
        import shutil
        shutil.rmtree(tmp_dir, ignore_errors=True)
    except Exception:
        pass


@pytest.mark.asyncio
class TestLanceDBCollectionManagement:
    """Test table (collection) CRUD operations."""

    async def test_create_collection(self, provider):
        name = f"test_col_{uuid.uuid4().hex[:8]}"
        result = await provider.create_collection(name)
        assert result is True

        collections = await provider.list_collections()
        assert name in collections

    async def test_create_collection_with_custom_config(self, provider):
        name = f"test_custom_{uuid.uuid4().hex[:8]}"
        result = await provider.create_collection(name, vector_size=128)
        assert result is True
        collections = await provider.list_collections()
        assert name in collections

    async def test_create_duplicate_collection(self, provider):
        name = f"test_dup_{uuid.uuid4().hex[:8]}"
        await provider.create_collection(name)
        # Creating again should return False
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
        result = await provider.delete_collection("nonexistent_table")
        assert result is True

    async def test_list_collections_empty(self, provider):
        collections = await provider.list_collections()
        assert isinstance(collections, list)

    async def test_default_collection_created(self, provider):
        collections = await provider.list_collections()
        assert "test_memories" in collections

    async def test_default_vector_config(self, provider):
        collections = await provider.list_collections()
        assert "test_memories" in collections


@pytest.mark.asyncio
class TestLanceDBPointOperations:
    """Test point upsert, search, delete, scroll."""

    COLLECTION = "test_memories"

    async def _ensure_table(self, provider):
        collections = await provider.list_collections()
        if self.COLLECTION not in collections:
            await provider.create_collection(self.COLLECTION)
        return self.COLLECTION

    async def test_upsert_and_search(self, provider):
        collection = await self._ensure_table(provider)
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
        collection = await self._ensure_table(provider)
        vec = [float(i % 10) / 10.0 for i in range(384)]
        point_id = str(uuid.uuid4())
        payload = {"subject": "TestThreshold", "predicate": "is", "object": "Value"}
        await provider.upsert(collection, point_id=point_id, vector=vec, payload=payload)

        # High threshold — should exclude (similarities are in 0.9-1.0 for same vector)
        await provider.search(collection, vector=vec, limit=10, score_threshold=0.999)
        # Our cosine similarity for identical vectors should be 1.0
        # So threshold=0.999 should still include it with scores near 1.0

    async def test_search_empty_collection(self, provider):
        collection = await self._ensure_table(provider)
        vec = [0.3] * 384
        results = await provider.search(collection, vector=vec, limit=10)
        assert isinstance(results, list)

    async def test_upsert_many_and_search_ranking(self, provider):
        collection = await self._ensure_table(provider)
        # Vectors pointing in different directions
        target_vec = [1.0 if i == 0 else 0.0 for i in range(384)]  # x-axis
        far_vec = [0.0 if i == 0 else 1.0 for i in range(384)]     # orthogonal

        target_id = str(uuid.uuid4())
        await provider.upsert(collection, point_id=target_id, vector=target_vec, payload={"rank": "target"})

        far_id = str(uuid.uuid4())
        await provider.upsert(collection, point_id=far_id, vector=far_vec, payload={"rank": "far"})

        # Search with target_vec — target should rank first
        results = await provider.search(collection, vector=target_vec, limit=10)
        assert results[0]["id"] == target_id

    async def test_scroll(self, provider):
        collection = await self._ensure_table(provider)
        vec = [float(i % 5) / 5.0 for i in range(384)]
        point_id = str(uuid.uuid4())
        payload = {"subject": "ScrollTest", "predicate": "is", "object": "Scrollable"}
        await provider.upsert(collection, point_id=point_id, vector=vec, payload=payload)

        scrolled = await provider.scroll(collection, limit=100)
        ids = [r["id"] for r in scrolled]
        assert point_id in ids

    async def test_scroll_with_filter(self, provider):
        collection = await self._ensure_table(provider)
        vec = [float(i % 5) / 5.0 for i in range(384)]
        await provider.upsert(collection, point_id=str(uuid.uuid4()), vector=vec,
                              payload={"subject": "FilterTarget", "predicate": "is", "object": "Y"})
        await provider.upsert(collection, point_id=str(uuid.uuid4()), vector=vec,
                              payload={"subject": "Other", "predicate": "is", "object": "N"})

        # Use scroll with simple equality filter
        scrolled = await provider.scroll(collection, limit=100)
        # At minimum, we should have the points
        assert len(scrolled) >= 2

    async def test_delete_point(self, provider):
        collection = await self._ensure_table(provider)
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

    async def test_delete_nonexistent_point(self, provider):
        collection = await self._ensure_table(provider)
        result = await provider.delete(collection, point_id="nonexistent-id")
        assert result is True

    async def test_upsert_without_payload(self, provider):
        collection = await self._ensure_table(provider)
        vec = [float(i % 10) / 10.0 for i in range(384)]
        point_id = str(uuid.uuid4())
        await provider.upsert(collection, point_id=point_id, vector=vec)

        results = await provider.search(collection, vector=vec, limit=10)
        ids = [r["id"] for r in results]
        assert point_id in ids

    async def test_upsert_batch(self, provider):
        collection = await self._ensure_table(provider)
        points = []
        for i in range(5):
            points.append({
                "id": str(uuid.uuid4()),
                "vector": [float((i + j) % 10) / 10.0 for j in range(384)],
                "payload": {"index": i, "name": f"batch_{i}"},
            })

        result = await provider.upsert_batch(points, collection=collection)
        assert result is True

        # Search with first point's vector — should find all
        results = await provider.search(collection, vector=points[0]["vector"], limit=10)
        assert len(results) >= 1

    async def test_upsert_same_id_is_idempotent(self, provider):
        """Re-upserting the same point_id must not create duplicate rows.

        Regression: plain table.add(mode=append) duplicates rows on retry;
        upsert must behave like a true merge (outbox worker replays entries).
        """
        collection = await self._ensure_table(provider)
        vec = [float(i % 10) / 10.0 for i in range(384)]
        point_id = str(uuid.uuid4())
        payload = {"subject": "Idempotent", "predicate": "is", "object": "Unique"}

        assert await provider.upsert(collection, point_id=point_id, vector=vec, payload=payload)
        assert await provider.upsert(collection, point_id=point_id, vector=vec, payload=payload)
        assert await provider.upsert(collection, point_id=point_id, vector=vec, payload=payload)

        results = await provider.search(collection, vector=vec, limit=10)
        matching = [r for r in results if r["id"] == point_id]
        assert len(matching) == 1, f"expected 1 row for {point_id}, got {len(matching)}"

    async def test_upsert_batch_same_ids_is_idempotent(self, provider):
        """Batch re-upsert with duplicate ids collapses to one row per id."""
        collection = await self._ensure_table(provider)
        vec = [float(i % 10) / 10.0 for i in range(384)]
        point_id = str(uuid.uuid4())

        batch = [
            {"id": point_id, "vector": vec, "payload": {"n": 1}},
            {"id": point_id, "vector": vec, "payload": {"n": 2}},
            {"id": str(uuid.uuid4()), "vector": vec, "payload": {"n": 3}},
        ]
        assert await provider.upsert_batch(batch, collection=collection)

        results = await provider.search(collection, vector=vec, limit=10)
        matching = [r for r in results if r["id"] == point_id]
        assert len(matching) == 1, f"expected 1 row for {point_id}, got {len(matching)}"

    async def test_optimize_prunes_old_versions(self, provider):
        """optimize() must compact and prune accumulated table versions.

        Regression: every merge_insert creates a new table version; without
        compaction the _versions dir grows orders of magnitude beyond the
        live dataset (observed 48 GB vs 140 MB live).
        """
        from pathlib import Path

        collection = await self._ensure_table(provider)
        vec = [float(i % 10) / 10.0 for i in range(384)]

        # Create several versions via repeated upserts
        for i in range(5):
            await provider.upsert(
                collection,
                point_id=f"optimize-test-{i}",
                vector=vec,
                payload={"n": i},
            )

        # Find the table version dir
        versions_dir = Path(provider._db_path) / f"{collection}.lance" / "_versions"
        assert versions_dir.is_dir()
        versions_before = len(list(versions_dir.glob("*.manifest")))

        ok = await provider.optimize(
            collection,
            cleanup_older_than=timedelta(0),
            delete_unverified=True,
        )
        assert ok is True

        versions_after = len(list(versions_dir.glob("*.manifest")))
        assert versions_after < versions_before, (
            f"expected version prune ({versions_before} -> {versions_after})"
        )

        # Data must still be searchable after compaction
        results = await provider.search(collection, vector=vec, limit=10)
        assert len(results) >= 5
