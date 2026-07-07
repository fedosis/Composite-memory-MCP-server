"""Qdrant vector database provider.

Wraps the QdrantClient for both in-memory (testing) and HTTP (production) modes.
Default vector config: 384 dimensions, cosine distance (all-MiniLM-L6-v2 compatible).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from uuid import uuid4

from qdrant_client import QdrantClient as _QdrantClient
from qdrant_client.http import models as qmodels

logger = logging.getLogger(__name__)

DEFAULT_COLLECTION = "memories"
DEFAULT_VECTOR_SIZE = 384
DEFAULT_DISTANCE = qmodels.Distance.COSINE


def _normalize_distance(distance: str) -> qmodels.Distance:
    """Convert string distance to Qdrant Distance enum."""
    mapping = {
        "cosine": qmodels.Distance.COSINE,
        "dot": qmodels.Distance.DOT,
        "euclid": qmodels.Distance.EUCLID,
        "manhattan": qmodels.Distance.MANHATTAN,
    }
    return mapping.get(distance.lower(), qmodels.Distance.COSINE)


class QdrantProvider:
    """Provider wrapping QdrantClient for vector storage and search.

    Args:
        location: ":memory:" for local (in-memory) mode, or host URL for remote.
        port: Qdrant gRPC/REST port (default 6333).
        prefer_grpc: Use gRPC if available (default False).
        collection: Default collection name (default "memories").
        vector_size: Default vector dimensionality (default 384).
        distance: Distance metric (default "cosine").
        api_key: Optional API key for cloud Qdrant.
    """

    def __init__(
        self,
        location: str = ":memory:",
        port: int = 6333,
        prefer_grpc: bool = False,
        collection: str = DEFAULT_COLLECTION,
        vector_size: int = DEFAULT_VECTOR_SIZE,
        distance: str = "cosine",
        api_key: str | None = None,
    ) -> None:
        self._collection = collection
        self._vector_size = vector_size
        self._distance = _normalize_distance(distance)
        self._api_key = api_key

        if location == ":memory:":
            self._client = _QdrantClient(location=location, prefer_grpc=False)
        else:
            self._client = _QdrantClient(
                host=location,
                port=port,
                prefer_grpc=prefer_grpc,
                api_key=api_key,
            )

        # Auto-create default collection
        self._ensure_collection(collection)

    def _ensure_collection(self, name: str) -> bool:
        """Create collection if it doesn't exist (sync helper)."""
        try:
            collections = self._client.get_collections()
            existing = {c.name for c in collections.collections}
            if name not in existing:
                self._client.create_collection(
                    collection_name=name,
                    vectors_config=qmodels.VectorParams(
                        size=self._vector_size,
                        distance=self._distance,
                    ),
                )
                logger.info(
                    "Created collection '%s' (size=%d, distance=%s)",
                    name,
                    self._vector_size,
                    self._distance,
                )
            return True
        except Exception as exc:
            logger.warning("Failed to ensure collection '%s': %s", name, exc)
            return False

    async def _run(self, func, *args, **kwargs):
        """Run a blocking Qdrant call in a thread."""
        return await asyncio.to_thread(func, *args, **kwargs)

    # ------------------------------------------------------------------
    # Collection management
    # ------------------------------------------------------------------

    async def create_collection(
        self,
        name: str,
        vector_size: int | None = None,
        distance: str | None = None,
    ) -> bool:
        """Create a new collection.

        Args:
            name: Collection name.
            vector_size: Vector dimensionality (default: provider default).
            distance: Distance metric (default: provider default).

        Returns:
            True if created, False if already exists or error.
        """
        try:
            collections = await self._run(self._client.get_collections)
            existing = {c.name for c in collections.collections}
            if name in existing:
                return False

            await self._run(
                self._client.create_collection,
                collection_name=name,
                vectors_config=qmodels.VectorParams(
                    size=vector_size or self._vector_size,
                    distance=_normalize_distance(distance) if distance else self._distance,
                ),
            )
            return True
        except Exception as exc:
            logger.error("Failed to create collection '%s': %s", name, exc)
            return False

    async def delete_collection(self, name: str) -> bool:
        """Delete a collection.

        Returns:
            True if deleted, False if not found or error.
        """
        try:
            await self._run(self._client.delete_collection, collection_name=name)
            return True
        except Exception as exc:
            logger.warning("Failed to delete collection '%s': %s", name, exc)
            return False

    async def list_collections(self) -> list[str]:
        """List all collection names."""
        try:
            collections = await self._run(self._client.get_collections)
            return [c.name for c in collections.collections]
        except Exception as exc:
            logger.error("Failed to list collections: %s", exc)
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
            collection: Collection name (default: provider default).
            point_id: Unique point ID (generated if empty).
            vector: Embedding vector.
            payload: Optional metadata payload.

        Returns:
            True on success.
        """
        col = collection or self._collection
        pid = point_id or str(uuid4())
        points = [
            qmodels.PointStruct(
                id=pid,
                vector=vector or [],
                payload=payload or {},
            )
        ]
        try:
            await self._run(self._client.upsert, collection_name=col, points=points)
            return True
        except Exception as exc:
            logger.error("Failed to upsert point %s in '%s': %s", pid, col, exc)
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
            collection: Collection name (default: provider default).

        Returns:
            True on success.
        """
        col = collection or self._collection
        point_structs = [
            qmodels.PointStruct(
                id=p.get("id", str(uuid4())),
                vector=p.get("vector", []),
                payload=p.get("payload", {}),
            )
            for p in points
        ]
        try:
            await self._run(
                self._client.upsert, collection_name=col, points=point_structs
            )
            return True
        except Exception as exc:
            logger.error("Failed to batch upsert in '%s': %s", col, exc)
            return False

    async def search(
        self,
        collection: str | None = None,
        vector: list[float] | None = None,
        limit: int = 10,
        score_threshold: float | None = None,
        filter_: dict | None = None,
    ) -> list[dict[str, Any]]:
        """Search for nearest neighbors.

        Args:
            collection: Collection name (default: provider default).
            vector: Query vector.
            limit: Max results (default 10).
            score_threshold: Minimum similarity score (optional).
            filter_: Qdrant filter dict (optional).

        Returns:
            List of result dicts with keys: id, score, payload.
        """
        col = collection or self._collection
        if not vector:
            return []

        try:
            qfilter = qmodels.Filter(**filter_) if filter_ else None
            resp = await self._run(
                self._client.query_points,
                collection_name=col,
                query=vector,
                limit=limit,
                score_threshold=score_threshold,
                query_filter=qfilter,
                with_payload=True,
                with_vectors=False,
            )
            return [
                {
                    "id": str(h.id),
                    "score": h.score,
                    "payload": h.payload or {},
                }
                for h in resp.points
            ]
        except Exception as exc:
            logger.error("Search failed in '%s': %s", col, exc)
            return []

    async def scroll(
        self,
        collection: str | None = None,
        limit: int = 100,
        filter_: dict | None = None,
    ) -> list[dict[str, Any]]:
        """Scroll through all points in a collection.

        Args:
            collection: Collection name (default: provider default).
            limit: Max points to return (default 100).
            filter_: Qdrant filter dict (optional).

        Returns:
            List of point dicts with keys: id, payload.
        """
        col = collection or self._collection
        try:
            qfilter = qmodels.Filter(**filter_) if filter_ else None
            records, _ = await self._run(
                self._client.scroll,
                collection_name=col,
                limit=limit,
                scroll_filter=qfilter,
                with_vectors=False,
            )
            return [
                {
                    "id": str(r.id),
                    "payload": r.payload or {},
                }
                for r in records
            ]
        except Exception as exc:
            logger.error("Scroll failed in '%s': %s", col, exc)
            return []

    async def delete(
        self,
        collection: str | None = None,
        point_id: str | int = "",
    ) -> bool:
        """Delete a point by ID.

        Args:
            collection: Collection name (default: provider default).
            point_id: Point ID to delete.

        Returns:
            True if deleted, False if not found or error.
        """
        col = collection or self._collection
        try:
            await self._run(
                self._client.delete,
                collection_name=col,
                points_selector=qmodels.PointIdsList(
                    points=[point_id],
                ),
            )
            return True
        except Exception as exc:
            logger.warning("Failed to delete point %s in '%s': %s", point_id, col, exc)
            return False

    async def close(self) -> None:
        """Close the underlying Qdrant client."""
        try:
            self._client.close()
        except Exception:
            pass
