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


@pytest.mark.asyncio
class TestLanceDBSafeContract:
    """Restricted filter contract (CORE-5/6, PROV-4).

    Real temp LanceDB; safe literals; allowlist/validation of field
    identifiers; must/should/must_not fully supported or rejected (never
    ignored); where() before limit(); dot score monotone w.r.t. the installed
    version's documented distance, without abs().
    """

    @pytest.fixture
    async def dot_provider(self, tmp_path):
        p = LanceDBProvider(
            db_path=str(tmp_path / "db"),
            table="memories",
            metric="dot",
            vector_size=2,
        )
        yield p
        await p.close()

    @pytest.fixture
    async def plain_provider(self, tmp_path):
        p = LanceDBProvider(
            db_path=str(tmp_path / "db"),
            table="memories",
            metric="cosine",
            vector_size=2,
        )
        yield p
        await p.close()

    async def _seed_two_vectors(self, provider):
        """Upsert [1,0] and [2,0] rows (both point along +x; [2,0] is the
        higher-dot match for query [1,0])."""
        await provider.upsert(
            None, point_id="unit", vector=[1.0, 0.0], payload={"kind": "unit"}
        )
        await provider.upsert(
            None, point_id="scaled", vector=[2.0, 0.0], payload={"kind": "scaled"}
        )

    async def test_dot_ranking_is_monotonic_without_abs(self, dot_provider):
        """dot scores must stay monotone w.r.t. the raw dot product.

        Regression: the old 1/(1+abs(distance)) transform tied the 2x-vector
        (distance -1) with the orthogonal vector and ranked the unit vector
        ABOVE the higher-dot 2x vector. score must be monotone in similarity:
        scaled > unit (dot 2 > dot 1), with no abs() folding negatives.
        """
        await self._seed_two_vectors(dot_provider)
        await dot_provider.upsert(
            None, point_id="neg", vector=[-1.0, 0.0], payload={"kind": "neg"}
        )

        results = await dot_provider.search(None, vector=[1.0, 0.0], limit=10)
        ids = [r["id"] for r in results]
        scores = {r["id"]: r["score"] for r in results}

        # [2,0] has dot 2 (distance -1) -> highest similarity -> ranks first.
        assert ids[0] == "scaled", f"expected scaled first, got {ids}"
        # Monotone scores: scaled > unit > neg, strictly.
        assert scores["scaled"] > scores["unit"] > scores["neg"], scores

    async def test_dot_threshold_filters_and_keeps_ranking(self, dot_provider):
        await self._seed_two_vectors(dot_provider)

        # Score(scaled)=1.5, score(unit)=1.0 for query [1,0]; a threshold
        # between them keeps only the stronger dot match.
        strict = await dot_provider.search(
            None, vector=[1.0, 0.0], limit=10, score_threshold=1.25
        )
        assert [r["id"] for r in strict] == ["scaled"]

        loose = await dot_provider.search(
            None, vector=[1.0, 0.0], limit=10, score_threshold=0.5
        )
        assert [r["id"] for r in loose] == ["scaled", "unit"]

    async def test_dot_threshold_zero_excludes_negative_dot(self, dot_provider):
        """No abs(): a strongly negative dot (dissimilar) must score below 0,
        so the default 0.0 threshold excludes it instead of letting abs() lift
        it above a merely-orthogonal neighbor."""
        await self._seed_two_vectors(dot_provider)
        await dot_provider.upsert(
            None, point_id="neg", vector=[-5.0, 0.0], payload={}
        )
        results = await dot_provider.search(
            None, vector=[1.0, 0.0], limit=10, score_threshold=0.0
        )
        ids = [r["id"] for r in results]
        assert "scaled" in ids and "unit" in ids
        assert "neg" not in ids, f"negative-dot row must be excluded: {ids}"

    async def test_quoted_and_injection_like_ids_are_safe(self, plain_provider):
        """IDs containing quotes / SQL keywords are matched literally and
        never break out of the generated filter expression."""
        vec = [1.0, 0.0]
        evil = "evil' OR '1'='1"
        quoted = 'quo"te'
        await plain_provider.upsert(
            None, point_id=evil, vector=vec, payload={"label": "evil"}
        )
        await plain_provider.upsert(
            None, point_id=quoted, vector=vec, payload={"label": "quoted"}
        )

        # Search with filter: only the evil row comes back.
        by_evil = await plain_provider.search(
            None, vector=vec, limit=10, filter_={"id": evil}
        )
        assert [r["id"] for r in by_evil] == [evil]

        # Scroll filter: exactly one row, the evil one.
        scrolled = await plain_provider.scroll(None, limit=100, filter_={"id": evil})
        assert [r["id"] for r in scrolled] == [evil]

        # Delete by the injection-like id must remove only that row.
        assert await plain_provider.delete(None, point_id=evil) is True
        remaining = await plain_provider.scroll(None, limit=100)
        assert [r["id"] for r in remaining] == [quoted]

        # Double-quote id is also matched and deleted exactly.
        assert await plain_provider.delete(None, point_id=quoted) is True
        remaining = await plain_provider.scroll(None, limit=100)
        assert remaining == []

    async def test_filtered_scroll_limit(self, plain_provider):
        """where() runs before limit(): a filtered scroll with limit=1 must
        return the single matching row, not the first unfiltered row."""
        for i, vid in enumerate(["r1", "r2", "r3"]):
            await plain_provider.upsert(
                None,
                point_id=vid,
                vector=[float(i), 1.0],
                payload={"i": i},
            )
        filtered = await plain_provider.scroll(None, limit=1, filter_={"id": "r2"})
        assert [r["id"] for r in filtered] == ["r2"]
        assert filtered[0]["payload"] == {"i": 1}

    async def test_search_where_before_limit(self, plain_provider):
        """Filtered search must respect limit on the FILTERED candidate set."""
        vec = [1.0, 0.0]
        await plain_provider.upsert(None, point_id="keep1", vector=vec, payload={})
        await plain_provider.upsert(None, point_id="drop1", vector=vec, payload={})
        await plain_provider.upsert(None, point_id="keep2", vector=vec, payload={})
        results = await plain_provider.search(
            None,
            vector=vec,
            limit=1,
            filter_={"id": "keep1"},
        )
        assert [r["id"] for r in results] == ["keep1"]

    async def _make_typed_table(self, provider):
        """Create a table with several scalar column types in the same db
        (schema is read from the real table at filter time)."""
        import asyncio

        import pyarrow as pa

        db = await provider._get_db()
        name = "typed"
        if name in await asyncio.to_thread(db.table_names):
            return name
        schema = pa.schema([
            pa.field("id", pa.utf8()),
            pa.field("vector", pa.list_(pa.float32(), 2)),
            pa.field("s", pa.utf8()),
            pa.field("n", pa.int64()),
            pa.field("f", pa.float64()),
            pa.field("b", pa.bool_()),
            pa.field("_metadata", pa.utf8()),
        ])
        await asyncio.to_thread(
            db.create_table, name, schema=schema, exist_ok=True,
        )
        table = await asyncio.to_thread(db.open_table, name)
        data = pa.table({
            "id": pa.array(["a", "b", "c"], type=pa.utf8()),
            "vector": pa.array(
                [[1.0, 0.0], [2.0, 0.0], [0.0, 1.0]],
                type=pa.list_(pa.float32(), 2),
            ),
            "s": pa.array(["Docker", "o'brien", "Podman"], type=pa.utf8()),
            "n": pa.array([1, 2, 3], type=pa.int64()),
            "f": pa.array([1.5, 2.5, 3.5], type=pa.float64()),
            "b": pa.array([True, False, True], type=pa.bool_()),
            "_metadata": pa.array(["{}", "{}", "{}"], type=pa.utf8()),
        })
        await asyncio.to_thread(table.add, data)
        return name

    async def test_filter_values_of_multiple_types(self, plain_provider):
        """str/int/float/bool equality values compile to typed literals and
        are validated against the real column type."""
        name = await self._make_typed_table(plain_provider)
        vec = [1.0, 0.0]

        # str equality (with quote escaping through the whole path)
        by_str = await plain_provider.search(
            name, vector=vec, limit=10, filter_={"s": "o'brien"}
        )
        assert [r["id"] for r in by_str] == ["b"]

        # int equality
        by_int = await plain_provider.search(
            name, vector=vec, limit=10, filter_={"n": 2}
        )
        assert [r["id"] for r in by_int] == ["b"]

        # float equality
        by_float = await plain_provider.scroll(name, limit=10, filter_={"f": 2.5})
        assert [r["id"] for r in by_float] == ["b"]

        # bool equality
        by_bool = await plain_provider.scroll(name, limit=10, filter_={"b": True})
        assert sorted(r["id"] for r in by_bool) == ["a", "c"]

        # multi-condition flat AND
        both = await plain_provider.scroll(
            name, limit=10, filter_={"n": 2, "b": False}
        )
        assert [r["id"] for r in both] == ["b"]

    async def test_must_should_must_not_fully_supported(self, plain_provider):
        """Qdrant-style structured filters are implemented (AND/OR/NOT), not
        silently dropped."""
        name = await self._make_typed_table(plain_provider)

        must = await plain_provider.scroll(
            name,
            limit=10,
            filter_={"must": [{"key": "n", "match": {"value": 2}}]},
        )
        assert [r["id"] for r in must] == ["b"]

        should = await plain_provider.scroll(
            name,
            limit=10,
            filter_={"should": [
                {"key": "n", "match": {"value": 1}},
                {"key": "n", "match": {"value": 3}},
            ]},
        )
        assert sorted(r["id"] for r in should) == ["a", "c"]

        must_not = await plain_provider.scroll(
            name,
            limit=10,
            filter_={"must_not": [{"key": "n", "match": {"value": 2}}]},
        )
        assert sorted(r["id"] for r in must_not) == ["a", "c"]

        combined = await plain_provider.scroll(
            name,
            limit=10,
            filter_={
                "must": [{"key": "b", "match": {"value": True}}],
                "must_not": [{"key": "n", "match": {"value": 3}}],
            },
        )
        assert [r["id"] for r in combined] == ["a"]

    async def test_unsupported_expressions_rejected(self, plain_provider):
        from memory_server.providers.exceptions import ProviderSearchError

        name = await self._make_typed_table(plain_provider)
        vec = [1.0, 0.0]
        cases = [
            # unknown field on a provider table (only id is filterable)
            (None, {"subject": "Docker"}),
            # _metadata is a JSON string column — never filterable
            (None, {"_metadata": "x"}),
            # payload columns that do not exist in a provider-managed table
            (None, {"must": [{"key": "subject", "match": {"value": "x"}}]}),
            # flat + structured mixed
            (name, {"id": "a", "must": [{"key": "n", "match": {"value": 1}}]}),
            # non-scalar value
            (name, {"n": [1, 2]}),
            # type mismatch: string column vs int value
            (name, {"id": 123}),
            # type mismatch: int column vs string value
            (name, {"n": "two"}),
            # bool column vs non-bool
            (name, {"b": 1}),
            # unsupported condition keys (range-like)
            (name, {"must": [
                {"key": "n", "match": {"value": 2}, "range": {"gte": 1}}
            ]}),
            # unsupported match shape (range instead of value)
            (name, {"must": [{"key": "n", "match": {"range": {"gte": 1}}}]}),
            # non-dict condition inside must
            (name, {"must": ["n = 2"]}),
            # list filter is not a valid type at all
            (name, ["n = 2"]),
        ]
        for coll, filt in cases:
            with pytest.raises(ProviderSearchError, match="filter"):
                await plain_provider.search(
                    coll, vector=vec, limit=10, filter_=filt
                )
            with pytest.raises(ProviderSearchError, match="filter"):
                await plain_provider.scroll(coll, limit=10, filter_=filt)

    async def test_raw_string_filter_escape_hatch_still_works(self, plain_provider):
        """Trusted native LanceDB filter strings remain accepted."""
        name = await self._make_typed_table(plain_provider)
        results = await plain_provider.scroll(name, limit=10, filter_="n = 2")
        assert [r["id"] for r in results] == ["b"]

    async def test_default_threshold_and_ranking_after_filter(self, plain_provider):
        """Cosine ranking still correct on a filtered search (post-filter
        ordering is by score, not insertion)."""
        await plain_provider.upsert(None, point_id="far", vector=[0.0, 1.0], payload={})
        await plain_provider.upsert(None, point_id="near", vector=[1.0, 0.0], payload={})
        results = await plain_provider.search(
            None, vector=[1.0, 0.0], limit=10, filter_={"id": "far"}
        )
        assert [r["id"] for r in results] == ["far"]
        # Orthogonal cosine distance = 1 -> similarity 1 - 1/2 = 0.5 exactly.
        assert abs(results[0]["score"] - 0.5) < 1e-6
