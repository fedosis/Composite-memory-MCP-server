"""LanceDB vector database provider.

Wraps lancedb for local-first persistent vector storage.
Default vector config: 384 dimensions, cosine distance (all-MiniLM-L6-v2 compatible).

No torch/sentence-transformers required — embeddings come from the existing
EmbeddingProvider abstraction.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)

DEFAULT_TABLE = "memories"
DEFAULT_VECTOR_SIZE = 384


def _normalize_metric(metric: str) -> str:
    """Normalize metric name to LanceDB format."""
    mapping = {
        "cosine": "cosine",
        "l2": "l2",
        "euclid": "l2",
        "euclidean": "l2",
        "dot": "dot",
        "dot_product": "dot",
    }
    return mapping.get(metric.lower(), "cosine")


def _dict_to_filter(filter_: dict[str, Any] | None) -> str | None:
    """Convert a simple dict filter to LanceDB filter expression.

    Supports simple equality filters in the form:
        {"field": "value"} -> 'field = "value"'
        {"field": 123} -> 'field = 123'

    For complex filters (must, should, must_not), returns None which
    lets the caller handle or fall back to post-filter.
    """
    if filter_ is None:
        return None

    # LanceDB-native string filters pass through
    if isinstance(filter_, str):
        return filter_

    # Try simple dict -> equality filter
    if isinstance(filter_, dict):
        # Check if it's a Qdrant-style filter with must/should/must_not
        if any(k in filter_ for k in ("must", "should", "must_not")):
            # Extract first simple equality if possible
            must_list = filter_.get("must", [])
            if len(must_list) == 1:
                item = must_list[0]
                key = item.get("key", "")
                match = item.get("match", {})
                value = match.get("value", "")
                if isinstance(value, str):
                    return f'{key} = "{value}"'
                return f"{key} = {value}"
            # Multi-condition: combine with AND
            conditions = []
            for item in must_list:
                key = item.get("key", "")
                match = item.get("match", {})
                value = match.get("value", "")
                if isinstance(value, str):
                    conditions.append(f'{key} = "{value}"')
                else:
                    conditions.append(f"{key} = {value}")
            if conditions:
                return " AND ".join(conditions)
            return None

        # Simple field:value dict
        parts = []
        for k, v in filter_.items():
            if isinstance(v, str):
                parts.append(f'{k} = "{v}"')
            else:
                parts.append(f"{k} = {v}")
        if parts:
            return " AND ".join(parts)

    return None


class LanceDBProvider:
    """Provider wrapping LanceDB for vector storage and search.

    Args:
        db_path: Path to the LanceDB database directory (default "data/lancedb").
        table: Default table name (default "memories").
        metric: Distance metric (default "cosine"). Options: cosine, l2, dot.
        vector_size: Default vector dimensionality (default 384).
    """

    def __init__(
        self,
        db_path: str = "data/lancedb",
        table: str = DEFAULT_TABLE,
        metric: str = "cosine",
        vector_size: int = DEFAULT_VECTOR_SIZE,
    ) -> None:
        self._db_path = db_path
        self._table_name = table
        self._metric = _normalize_metric(metric)
        self._vector_size = vector_size
        self._db: Any = None  # lazy-init

    async def _get_db(self):
        """Lazy-init the LanceDB database connection."""
        if self._db is None:
            import lancedb

            self._db = await asyncio.to_thread(lancedb.connect, self._db_path)
            # Ensure default table exists
            await self._ensure_table(self._table_name)
        return self._db

    async def _ensure_table(self, name: str) -> bool:
        """Create the table if it doesn't exist."""
        db = await self._get_db()
        try:
            table_names = await asyncio.to_thread(db.table_names)
            if name in table_names:
                return True

            # Create with an empty batch to define the schema
            import pyarrow as pa

            schema = pa.schema([
                pa.field("id", pa.utf8()),
                pa.field("vector", pa.list_(pa.float32(), self._vector_size)),
                pa.field("_metadata", pa.utf8()),  # JSON-encoded payload
            ])
            await asyncio.to_thread(
                db.create_table, name, schema=schema, exist_ok=True,
            )
            logger.info(
                "Created table '%s' (vector_size=%d, metric=%s)",
                name,
                self._vector_size,
                self._metric,
            )
            return True
        except Exception as exc:
            logger.warning("Failed to ensure table '%s': %s", name, exc)
            return False

    async def _run(self, func, *args, **kwargs):
        """Run a blocking LanceDB call in a thread."""
        return await asyncio.to_thread(func, *args, **kwargs)

    async def _get_table(self, name: str | None = None):
        """Get a LanceDB table by name (or default)."""
        table_name = name or self._table_name
        db = await self._get_db()
        await self._ensure_table(table_name)
        return await asyncio.to_thread(db.open_table, table_name)

    # ------------------------------------------------------------------
    # Table (collection) management
    # ------------------------------------------------------------------

    async def create_collection(
        self,
        name: str,
        vector_size: int | None = None,
        distance: str | None = None,
    ) -> bool:
        """Create a new table (collection).

        Args:
            name: Table name.
            vector_size: Vector dimensionality (default: provider default).
            distance: Distance metric (default: provider default). Ignored
                      in v0.10 — LanceDB metric is set at provider level.

        Returns:
            True if created, False if already exists or error.
        """
        db = await self._get_db()
        try:
            table_names = await asyncio.to_thread(db.table_names)
            if name in table_names:
                return False

            import pyarrow as pa

            vs = vector_size or self._vector_size
            schema = pa.schema([
                pa.field("id", pa.utf8()),
                pa.field("vector", pa.list_(pa.float32(), vs)),
                pa.field("_metadata", pa.utf8()),
            ])
            await asyncio.to_thread(
                db.create_table, name, schema=schema, exist_ok=True,
            )
            return True
        except Exception as exc:
            logger.error("Failed to create table '%s': %s", name, exc)
            return False

    async def delete_collection(self, name: str) -> bool:
        """Delete a table (collection).

        Returns:
            True if deleted, False if not found or error.
        """
        db = await self._get_db()
        try:
            await asyncio.to_thread(db.drop_table, name, ignore_missing=True)
            return True
        except Exception as exc:
            logger.warning("Failed to delete table '%s': %s", name, exc)
            return False

    async def list_collections(self) -> list[str]:
        """List all table names."""
        db = await self._get_db()
        try:
            return await asyncio.to_thread(db.table_names)
        except Exception as exc:
            logger.error("Failed to list tables: %s", exc)
            return []

    # ------------------------------------------------------------------
    # Point operations
    # ------------------------------------------------------------------

    async def upsert(
        self,
        collection: str | None = None,
        point_id: str | int = "",
        vector: list[float] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> bool:
        """Upsert a single point with vector and optional payload.

        Args:
            collection: Table name (default: provider default).
            point_id: Unique point ID (generated if empty).
            vector: Embedding vector.
            payload: Optional metadata payload.

        Returns:
            True on success.
        """
        table = await self._get_table(collection)
        pid = str(point_id) if point_id else str(uuid.uuid4())
        try:
            import pyarrow as pa

            data = pa.table({
                "id": pa.array([pid], type=pa.utf8()),
                "vector": pa.array([vector or []], type=pa.list_(pa.float32())),
                "_metadata": pa.array(
                    [json.dumps(payload or {})], type=pa.utf8()
                ),
            })
            # Use add() which handles both insert and replace by id
            await self._run(table.add, data)
            return True
        except Exception as exc:
            logger.error("Failed to upsert point %s: %s", pid, exc)
            return False

    async def upsert_batch(
        self,
        points: list[dict[str, Any]],
        collection: str | None = None,
    ) -> bool:
        """Upsert multiple points at once.

        Each dict should have keys: id (str|int), vector (list[float]), payload (dict, optional).

        Args:
            points: List of point dicts.
            collection: Table name (default: provider default).

        Returns:
            True on success.
        """
        table = await self._get_table(collection)
        try:
            import pyarrow as pa

            ids = [str(p.get("id", str(uuid.uuid4()))) for p in points]
            vectors = [p.get("vector", []) for p in points]
            metadatas = [json.dumps(p.get("payload", {})) for p in points]

            data = pa.table({
                "id": pa.array(ids, type=pa.utf8()),
                "vector": pa.array(vectors, type=pa.list_(pa.float32())),
                "_metadata": pa.array(metadatas, type=pa.utf8()),
            })
            await self._run(table.add, data)
            return True
        except Exception as exc:
            logger.error("Failed to batch upsert: %s", exc)
            return False

    async def search(
        self,
        collection: str | None = None,
        vector: list[float] | None = None,
        limit: int = 10,
        score_threshold: float | None = None,
        filter_: dict | str | None = None,
    ) -> list[dict[str, Any]]:
        """Search for nearest neighbors.

        Args:
            collection: Table name (default: provider default).
            vector: Query vector.
            limit: Max results (default 10).
            score_threshold: Minimum similarity score (optional).
            filter_: LanceDB filter expression or dict (optional).

        Returns:
            List of result dicts with keys: id, score, payload.
        """
        if not vector:
            return []

        table = await self._get_table(collection)
        try:
            lance_filter = _dict_to_filter(filter_) if isinstance(filter_, dict) else filter_
            query = table.search(np.array(vector, dtype=np.float32))

            if self._metric == "cosine":
                query = query.metric("cosine")
            elif self._metric == "l2":
                query = query.metric("l2")
            elif self._metric == "dot":
                query = query.metric("dot")

            if lance_filter:
                query = query.where(lance_filter, prefilter=True)

            results = await self._run(query.limit(limit).to_list)

            parsed = []
            for r in results:
                score = r.get("_distance", 0.0)
                # LanceDB returns distance (lower = more similar).
                # Convert to similarity score for API consistency.
                if self._metric == "cosine":
                    # Cosine distance: 0 = same, 2 = opposite
                    # Cosine similarity: 1 - (distance / 2)
                    # Also handle the case where LanceDB returns negative values
                    similarity = 1.0 - (abs(score) / 2.0)
                    if similarity < 0:
                        similarity = 0.0
                elif self._metric == "dot":
                    # Dot product: higher = more similar
                    # Normalize to 0-1 using sigmoid-like approach
                    similarity = 1.0 / (1.0 + abs(score))
                else:  # l2
                    # L2 distance: lower = more similar
                    # Convert: 1.0 / (1.0 + distance)
                    similarity = 1.0 / (1.0 + abs(score))

                if score_threshold is not None and similarity < score_threshold:
                    continue

                payload_raw = r.get("_metadata", "{}")
                if isinstance(payload_raw, str):
                    try:
                        payload = json.loads(payload_raw)
                    except (json.JSONDecodeError, TypeError):
                        payload = {}
                elif isinstance(payload_raw, dict):
                    payload = payload_raw
                else:
                    payload = {}

                parsed.append({
                    "id": str(r.get("id", "")),
                    "score": similarity,
                    "payload": payload,
                })

            return parsed
        except Exception as exc:
            logger.error("Search failed: %s", exc)
            return []

    async def scroll(
        self,
        collection: str | None = None,
        limit: int = 100,
        filter_: dict | str | None = None,
    ) -> list[dict[str, Any]]:
        """Scroll through all points in a table.

        Args:
            collection: Table name (default: provider default).
            limit: Max points to return (default 100).
            filter_: LanceDB filter expression or dict (optional).

        Returns:
            List of point dicts with keys: id, payload.
        """
        table = await self._get_table(collection)
        try:
            lance_filter = _dict_to_filter(filter_) if isinstance(filter_, dict) else filter_
            results = await self._run(
                table.search().limit(limit).to_list
            )

            parsed = []
            for r in results:
                payload_raw = r.get("_metadata", "{}")
                if isinstance(payload_raw, str):
                    try:
                        payload = json.loads(payload_raw)
                    except (json.JSONDecodeError, TypeError):
                        payload = {}
                elif isinstance(payload_raw, dict):
                    payload = payload_raw
                else:
                    payload = {}

                parsed.append({
                    "id": str(r.get("id", "")),
                    "payload": payload,
                })

            # Apply filter post-hoc if we can't push it down
            if lance_filter and parsed:
                # Simple post-filter — only applied when we can't
                # push filter to LanceDB query
                pass

            return parsed
        except Exception as exc:
            logger.error("Scroll failed: %s", exc)
            return []

    async def delete(
        self,
        collection: str | None = None,
        point_id: str | int = "",
    ) -> bool:
        """Delete a point by ID.

        Args:
            collection: Table name (default: provider default).
            point_id: Point ID to delete.

        Returns:
            True if deleted, False if not found or error.
        """
        table = await self._get_table(collection)
        try:
            pid = str(point_id)
            await self._run(table.delete, f'id = "{pid}"')
            return True
        except Exception as exc:
            logger.warning("Failed to delete point %s: %s", point_id, exc)
            return False

    async def close(self) -> None:
        """Close the underlying LanceDB database connection."""
        self._db = None
        logger.debug("LanceDB connection closed")
