"""Outbox worker — async background task that processes outbox entries.

The worker polls the outbox table for pending entries, processes them
(embeds facts → Qdrant, syncs to graph), and marks them as completed.
Failed entries are retried up to 3 times before being marked as failed.

The worker is resilient to crashes: pending entries survive server
restarts because the outbox table is durable (SQLite WAL mode).
Processing is idempotent — processing the same entry twice is safe
because Qdrant upsert is idempotent and graph operations are additive.
"""

import asyncio
import logging

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from memory_server.providers.embedding_provider import SentenceTransformerEmbeddingProvider
from memory_server.providers.qdrant_provider import QdrantProvider
from memory_server.providers.lancedb_provider import LanceDBProvider
from memory_server.router.graph_router import GraphRouter
from storage.base import Base
from storage.outbox import OutboxEntry, OutboxRepository

logger = logging.getLogger(__name__)

# Maximum number of retries before marking an entry as failed
MAX_RETRIES = 3

# Polling interval in seconds
POLL_INTERVAL_SECONDS = 1.0


class OutboxWorker:
    """Background worker that processes outbox entries.

    Usage:
        worker = OutboxWorker(
            db_url="sqlite+aiosqlite:///memory.db",
            qdrant=qdrant_provider,
            embedder=embedder_provider,
            graph_router=graph_router,
        )
        asyncio.create_task(worker.run())
    """

    def __init__(
        self,
        db_url: str = "",
        *,
        engine=None,
        qdrant: QdrantProvider | LanceDBProvider | None = None,
        embedder: SentenceTransformerEmbeddingProvider | None = None,
        graph_router: GraphRouter | None = None,
    ):
        self._db_url = db_url
        self._engine = engine
        self._qdrant = qdrant
        self._embedder = embedder
        self._graph_router = graph_router
        self._session_factory: async_sessionmaker[AsyncSession] | None = None

    async def initialize(self) -> None:
        """Initialize the worker — create session factory from existing engine."""
        if self._engine is None:
            self._engine = create_async_engine(self._db_url, echo=False)

            async with self._engine.connect() as conn:
                await conn.exec_driver_sql("PRAGMA journal_mode=WAL")
                await conn.exec_driver_sql("PRAGMA synchronous=NORMAL")
                await conn.exec_driver_sql("PRAGMA busy_timeout=5000")

            async with self._engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)

        self._session_factory = async_sessionmaker(
            self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

    async def close(self) -> None:
        """Dispose of the engine."""
        if self._engine:
            await self._engine.dispose()

    async def run(self) -> None:
        """Main loop — poll outbox forever."""
        logger.info("Outbox worker started, polling every %ss", POLL_INTERVAL_SECONDS)
        while True:
            try:
                await self._poll_once()
            except Exception:
                logger.exception("Outbox worker poll cycle failed")
            await asyncio.sleep(POLL_INTERVAL_SECONDS)

    async def _poll_once(self) -> None:
        """Single poll cycle: fetch pending entries and process them."""
        if self._session_factory is None:
            return

        async with self._session_factory() as session:
            repo = OutboxRepository(session)
            entries = await repo.get_pending(limit=50)

            if not entries:
                return

            logger.debug("Outbox worker processing %d entries", len(entries))

            for entry in entries:
                await self._process_entry(session, repo, entry)

            await session.commit()

    async def _process_entry(
        self,
        session: AsyncSession,
        repo: OutboxRepository,
        entry: OutboxEntry,
    ) -> None:
        """Process a single outbox entry."""
        # Mark as processing
        await repo.mark_processing(entry.id)

        try:
            if entry.operation == "index_fact":
                await self._process_index_fact(entry)
            elif entry.operation == "index_decision":
                await self._process_index_decision(entry)
            elif entry.operation == "index_skill":
                await self._process_index_skill(entry)
            elif entry.operation == "index_belief":
                await self._process_index_belief(entry)
            else:
                raise ValueError(f"Unknown operation: {entry.operation}")

            await repo.mark_completed(entry.id)
            logger.debug("Outbox entry %s completed (%s)", entry.id, entry.operation)

        except Exception as e:
            error_msg = str(e)
            new_retry = await repo.increment_retry(entry.id, error_msg)

            if new_retry >= MAX_RETRIES:
                await repo.mark_failed(entry.id, error_msg)
                logger.error(
                    "Outbox entry %s failed after %d retries: %s",
                    entry.id,
                    new_retry,
                    error_msg,
                )
            else:
                logger.warning(
                    "Outbox entry %s failed (retry %d/%d): %s",
                    entry.id,
                    new_retry,
                    MAX_RETRIES,
                    error_msg,
                )

    async def _process_index_fact(self, entry: OutboxEntry) -> None:
        """Process an index_fact entry: embed + upsert to Qdrant + sync to graph.

        Idempotent: Qdrant upsert replaces by point_id, graph sync is additive.
        """
        payload = entry.payload

        subject = payload.get("subject", "")
        predicate = payload.get("predicate", "")
        obj = payload.get("object", "")
        source = payload.get("source", "")
        fact_id = entry.record_id

        fact_text = f"{subject} {predicate} {obj}"

        # Embed (sync call, run in thread to avoid blocking)
        if self._embedder:
            vector = await asyncio.to_thread(self._embedder.embed, fact_text)

            # Upsert into Qdrant (idempotent) — use deterministic UUID from record_id
            if self._qdrant:
                import uuid
                point_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"fact:{fact_id}"))
                await self._qdrant.upsert(
                    point_id=point_uuid,
                    vector=vector,
                    payload={
                        "subject": subject,
                        "predicate": predicate,
                        "object": obj,
                        "source": source,
                        "memory_type": "fact",
                    },
                )

        # Sync to graph (idempotent — additive)
        if self._graph_router:
            await asyncio.to_thread(
                self._graph_router.sync_fact, subject, predicate, obj
            )

    async def _process_index_decision(self, entry: OutboxEntry) -> None:
        """Process an index_decision entry: sync to graph.

        Idempotent: graph sync creates nodes/edges if they don't exist.
        """
        if self._graph_router is None:
            logger.debug("No graph router configured, skipping decision index")
            return

        payload = entry.payload

        await asyncio.to_thread(
            self._graph_router.sync_decision,
            choice=payload.get("choice", ""),
            reason=payload.get("reason", ""),
            entities=[payload.get("context", "")],
        )

    async def _process_index_skill(self, entry: OutboxEntry) -> None:
        """Process an index_skill entry: sync to graph.

        Idempotent: graph sync creates nodes/edges if they don't exist.
        """
        if self._graph_router is None:
            logger.debug("No graph router configured, skipping skill index")
            return

        payload = entry.payload

        await asyncio.to_thread(
            self._graph_router.sync_skill,
            purpose=payload.get("purpose", ""),
            steps=payload.get("steps", []),
        )

    async def _process_index_belief(self, entry: OutboxEntry) -> None:
        """Process an index_belief entry: embed + upsert to Qdrant + sync to graph.

        Indexes the belief proposition in Qdrant for semantic search.
        Graph sync is deferred (GraphRouter.sync_belief is optional).

        Idempotent: Qdrant upsert replaces by point_id, graph sync is additive.
        """
        import uuid

        payload = entry.payload

        proposition = payload.get("proposition", "")
        confidence = payload.get("confidence", 0.5)
        tags = payload.get("tags", [])
        source = payload.get("source", "system")
        belief_id = entry.record_id

        # Embed and index into Qdrant
        if self._embedder and proposition:
            vector = await asyncio.to_thread(self._embedder.embed, proposition)

            if self._qdrant:
                point_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"belief:{belief_id}"))
                await self._qdrant.upsert(
                    point_id=point_uuid,
                    vector=vector,
                    payload={
                        "proposition": proposition,
                        "confidence": confidence,
                        "tags": tags,
                        "source": source,
                        "memory_type": "belief",
                    },
                )

        # Sync to graph (if GraphRouter supports it)
        if self._graph_router and hasattr(self._graph_router, "sync_belief"):
            await asyncio.to_thread(
                self._graph_router.sync_belief,
                proposition=proposition,
                tags=tags,
            )

    # ── utility for server integration ──────────────────────────────

    async def process_all_pending(self) -> dict:
        """Process all pending entries synchronously (for testing).

        Returns a summary dict with counts of processed/failed entries.
        """
        processed = 0
        failed = 0

        if self._session_factory is None:
            return {"processed": processed, "failed": failed}

        async with self._session_factory() as session:
            repo = OutboxRepository(session)
            entries = await repo.get_pending(limit=500)

            for entry in entries:
                await self._process_entry(session, repo, entry)

            await session.commit()

        return {"processed": len(entries)}
