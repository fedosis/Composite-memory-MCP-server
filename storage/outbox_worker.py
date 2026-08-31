"""Outbox worker — async background task that processes outbox entries.

The worker polls the outbox table for pending entries, processes them
(embeds facts → Qdrant, syncs to graph), and marks them as completed.
Failed entries are retried up to 3 times before being marked as failed.

The worker is resilient to crashes: pending entries survive server
restarts because the outbox table is durable (SQLite WAL mode).
Processing is idempotent — processing the same entry twice is safe
because Qdrant upsert is idempotent and graph operations are additive.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

if TYPE_CHECKING:
    from memory_server.providers.embedding_provider import SentenceTransformerEmbeddingProvider
    from memory_server.providers.lancedb_provider import LanceDBProvider
    from memory_server.providers.qdrant_provider import QdrantProvider
    from memory_server.router.graph_router import GraphRouter

from storage.base import Base
from storage.outbox import OutboxEntry, OutboxRepository

logger = logging.getLogger(__name__)


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
        max_retries: int = 3,
        poll_interval_seconds: float = 1.0,
        poll_batch_size: int = 500,
        fact_batch_chunk_size: int = 32,
        compact_interval_seconds: int = 1800,
        compact_cleanup_hours: int = 1,
        stale_processing_seconds: int = 600,
        process_pending_limit: int = 500,
        busy_timeout_ms: int = 5000,
    ):
        self._db_url = db_url
        self._engine = engine
        self._qdrant = qdrant
        self._embedder = embedder
        self._graph_router = graph_router
        self._max_retries = max_retries
        self._poll_interval_seconds = poll_interval_seconds
        self._poll_batch_size = poll_batch_size
        self._fact_batch_chunk_size = fact_batch_chunk_size
        self._compact_interval_seconds = compact_interval_seconds
        self._compact_cleanup_hours = compact_cleanup_hours
        self._stale_processing_seconds = stale_processing_seconds
        self._process_pending_limit = process_pending_limit
        self._busy_timeout_ms = busy_timeout_ms
        self._session_factory: async_sessionmaker[AsyncSession] | None = None
        self._stop_requested = False
        self._last_compact_at: float = 0.0

    def stop(self) -> None:
        """Signal the run loop to exit after the current iteration.

        Unlike ``asyncio.Task.cancel()`` this works across threads and event
        loops: ``run()`` checks the flag each cycle, so a worker scheduled via
        ``asyncio.run_coroutine_threadsafe()`` (HermesProvider path) exits
        cleanly instead of polling a disposed engine forever.
        """
        self._stop_requested = True

    async def initialize(self) -> None:
        """Initialize the worker — create session factory from existing engine."""
        if self._engine is None:
            self._engine = create_async_engine(self._db_url, echo=False)

            async with self._engine.connect() as conn:
                await conn.exec_driver_sql("PRAGMA journal_mode=WAL")
                await conn.exec_driver_sql("PRAGMA synchronous=NORMAL")
                await conn.exec_driver_sql(f"PRAGMA busy_timeout={self._busy_timeout_ms}")

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
        """Main loop — poll outbox until ``stop()`` is requested."""
        logger.info(
            "Outbox worker started, polling every %ss",
            self._poll_interval_seconds,
        )
        while not self._stop_requested:
            try:
                processed = await self._poll_once()
            except Exception:
                logger.exception("Outbox worker poll cycle failed")
                processed = 0
            # Adaptive backoff: keep draining while there is work, only sleep
            # the full interval when the queue is empty. With a large backlog
            # this avoids wasting ~50% of wall time in asyncio.sleep.
            # Compaction is attempted every cycle (throttled internally by
            # the compact interval) — gating it on an empty queue meant a
            # permanent backlog skipped it forever, letting LanceDB versions
            # accumulate unboundedly.
            await self._maybe_compact()
            if processed == 0:
                await asyncio.sleep(self._poll_interval_seconds)
            else:
                await asyncio.sleep(0)
        logger.info("Outbox worker stopped")

    async def _maybe_compact(self) -> None:
        """Schedule LanceDB compaction when due, without blocking the loop.

        Compaction prunes the accumulated table versions that every
        merge_insert creates, keeping the on-disk store near the live dataset
        size. Runs as a background task so a long VACUUM never stalls the
        drain loop. Throttled to once per ``self._compact_interval_seconds``.

        The ``cleanup_older_than`` threshold is explicit: LanceDB's default
        (~7 days) deletes nothing on a busy store where every version is
        younger than a week, which is exactly how versions accumulated to
        hundreds of GB before this fix.
        """
        import time

        if self._qdrant is None:
            return
        optimize = getattr(self._qdrant, "optimize", None)
        if optimize is None:
            return
        if time.monotonic() - self._last_compact_at < self._compact_interval_seconds:
            return
        self._last_compact_at = time.monotonic()

        async def _compact() -> None:
            try:
                ok = await optimize(
                    cleanup_older_than=timedelta(hours=self._compact_cleanup_hours),
                )
                if ok:
                    logger.info("Outbox worker: LanceDB compaction done")
            except Exception:
                logger.exception("Outbox worker: LanceDB compaction failed")

        asyncio.create_task(_compact())

    async def _poll_once(self) -> int:
        """Single poll cycle: fetch pending entries and process them.

        Claim (mark_processing) is committed in its own short transaction
        before the heavy embedding work, so the SQLite write lock is held
        only for the status update, not for the whole embed + upsert cycle.
        Stale ``processing`` entries from a crashed run are reset first.

        This method does NOT aggregate per-entry outcomes — it returns the
        number of entries attempted, regardless of how many completed,
        failed, or stayed pending. Outcome counts live only in
        ``process_all_pending`` (D5 boundary).

        Returns:
            Number of entries processed (0 when the queue was empty).
        """
        if self._session_factory is None:
            return 0

        async with self._session_factory() as session:
            repo = OutboxRepository(session)
            # Recover entries left in "processing" by a crashed worker.
            await repo.reset_stale_processing(
                max_age_seconds=self._stale_processing_seconds
            )
            entries = await repo.get_pending(limit=self._poll_batch_size)

            if not entries:
                return 0

            logger.debug(
                "Outbox worker found %d pending entries",
                len(entries),
            )

            # Claim: mark as processing and commit immediately. This is a
            # short write lock; the heavy embed/upsert below runs outside
            # any SQLite transaction so other writers are not blocked.
            for entry in entries:
                await repo.mark_processing(entry.id)
            await session.commit()

        # Batch-friendly operations: group index_fact entries and process
        # them in one embed + one upsert_batch call instead of one model
        # invocation per fact. Other operation types are rare and keep the
        # per-entry path.
        fact_entries = [e for e in entries if e.operation == "index_fact"]
        other_entries = [e for e in entries if e.operation != "index_fact"]

        if fact_entries:
            await self._process_fact_batch(fact_entries)

        for entry in other_entries:
            async with self._session_factory() as session:
                repo = OutboxRepository(session)
                await self._process_entry(session, repo, entry)
                await session.commit()

        return len(entries)

    async def _process_fact_batch(
        self,
        entries: list[OutboxEntry],
    ) -> None:
        """Process a batch of index_fact entries with batched embedding.

        Embeds all fact texts with one ``embed_batch`` call, upserts them
        with one ``upsert_batch`` call, then syncs each fact to the graph.
        This is dramatically faster than per-entry embed + upsert for large
        backlogs.

        The batch is processed in small chunks; if a chunk fails, its
        entries fall back to the per-entry path so a single toxic entry
        (malformed payload, embedder hiccup) cannot mark 499 healthy
        entries as failed or stall the queue. Each chunk is committed in
        its own short transaction, keeping SQLite write locks minimal.

        Chunk outcomes are NEVER converted into an outcome aggregate here —
        the per-entry fallback updates DB state via ``_process_entry`` (its
        returned statuses are ignored; DB state is authoritative), and
        ``_poll_once`` still returns ``len(entries)`` attempted (D5
        boundary). Only ``process_all_pending`` tallies per-entry statuses.
        """
        if not entries:
            return

        assert self._session_factory is not None  # guaranteed by caller (_poll_once)

        for start in range(0, len(entries), self._fact_batch_chunk_size):
            chunk = entries[start : start + self._fact_batch_chunk_size]
            try:
                await self._process_fact_chunk(chunk)
                async with self._session_factory() as session:
                    repo = OutboxRepository(session)
                    for entry in chunk:
                        await repo.mark_completed(entry.id)
                    await session.commit()
            except Exception as exc:
                logger.warning(
                    "Fact chunk of %d entries failed (%s); falling back to per-entry",
                    len(chunk),
                    exc,
                )
                async with self._session_factory() as session:
                    repo = OutboxRepository(session)
                    for entry in chunk:
                        await self._process_entry(session, repo, entry)
                    await session.commit()

    async def _process_fact_chunk(
        self,
        chunk: list[OutboxEntry],
    ) -> None:
        """Embed and upsert a chunk of index_fact entries in one call each.

        Raises on any failure; the caller decides whether to retry the
        chunk or fall back to per-entry processing.
        """
        payloads = [e.payload for e in chunk]
        texts = [
            f"{p.get('subject', '')} {p.get('predicate', '')} {p.get('object', '')}"
            for p in payloads
        ]
        record_ids = [e.record_id for e in chunk]

        # Embed all texts in one batch call (sync, run in thread)
        if self._embedder:
            vectors = await asyncio.to_thread(self._embedder.embed_batch, texts)
            if len(vectors) != len(texts):
                raise RuntimeError(
                    f"embed_batch returned {len(vectors)} vectors for {len(texts)} texts"
                )

            if self._qdrant:
                import uuid

                points = []
                for i, payload in enumerate(payloads):
                    point_uuid = str(
                        uuid.uuid5(uuid.NAMESPACE_DNS, f"fact:{record_ids[i]}")
                    )
                    points.append(
                        {
                            "id": point_uuid,
                            "vector": vectors[i],
                            "payload": {
                                "subject": payload.get("subject", ""),
                                "predicate": payload.get("predicate", ""),
                                "object": payload.get("object", ""),
                                "source": payload.get("source", ""),
                                "memory_type": "fact",
                            },
                        }
                    )
                ok = await self._qdrant.upsert_batch(points)
                if not ok:
                    raise RuntimeError(
                        f"upsert_batch returned False for {len(points)} points"
                    )
            else:
                logger.warning("_process_fact_chunk: no vector provider — skipping upsert")

        # Sync to graph in one batch (single snapshot write for the chunk
        # instead of ~6 per fact).
        if self._graph_router:
            await asyncio.to_thread(self._graph_router.sync_facts_batch, payloads)

    async def _process_entry(
        self,
        session: AsyncSession,
        repo: OutboxRepository,
        entry: OutboxEntry,
    ) -> str:
        """Process a single outbox entry.

        Exception contract: any exception raised by an indexing step (embed,
        vector upsert, graph sync) is a FAILURE — the entry is never marked
        completed. ``mark_completed`` is reached only on a clean return; the
        exception handler increments the retry counter and either marks the
        entry failed (exhausted in this run) or leaves it pending for the
        next poll. Result-checking of False returns is deliberately NOT part
        of this contract (provider-contract card).

        Returns:
            A status string consumed only by ``process_all_pending``:
            - ``"completed"`` — marked completed (clean return).
            - ``"failed"`` — retries exhausted; marked failed in this run.
            - ``"pending"`` — failed retryably; reset to pending for retry.
        """
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
            return "completed"

        except Exception as e:
            error_msg = str(e)
            new_retry = await repo.increment_retry(entry.id, error_msg)

            if new_retry >= self._max_retries:
                await repo.mark_failed(entry.id, error_msg)
                logger.error(
                    "Outbox entry %s failed after %d retries: %s",
                    entry.id,
                    new_retry,
                    error_msg,
                )
                return "failed"
            else:
                logger.warning(
                    "Outbox entry %s failed (retry %d/%d): %s",
                    entry.id,
                    new_retry,
                    self._max_retries,
                    error_msg,
                )
                return "pending"

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
            else:
                logger.warning("_process_index_fact: no vector provider — skipping upsert")

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
        """Process an index_belief entry: embed + upsert to Qdrant.

        Beliefs are vector-indexed ONLY — graph sync is intentionally
        skipped: beliefs are not graph entities in this design, and
        ``GraphRouter`` has no ``sync_belief`` API (only sync_fact /
        sync_facts_batch / sync_decision / sync_skill).

        Idempotent: Qdrant upsert replaces by point_id.
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

        # No graph sync — beliefs are vector-indexed only (see docstring).
        # The previous ``hasattr(self._graph_router, "sync_belief")``
        # fallback was dead code (GraphRouter has no sync_belief) and has
        # been removed (Card 2, D6).

    # ── utility for server integration ──────────────────────────────

    async def process_all_pending(self) -> dict:
        """Process all pending entries synchronously (for testing).

        Returns:
            ``{"processed": N, "failed": M}`` where N = entries COMPLETED in
            this run and M = entries whose retries were EXHAUSTED (marked
            failed) in this run. Entries that fail retryably stay
            ``pending`` (reset by ``increment_retry``) and are counted in
            NEITHER bucket — they are re-picked by the next poll/run.
        """
        processed = 0
        failed = 0

        if self._session_factory is None:
            return {"processed": processed, "failed": failed}

        async with self._session_factory() as session:
            repo = OutboxRepository(session)
            entries = await repo.get_pending(limit=self._process_pending_limit)

            for entry in entries:
                status = await self._process_entry(session, repo, entry)
                if status == "completed":
                    processed += 1
                elif status == "failed":
                    failed += 1
                # "pending" is counted in neither bucket.

            await session.commit()

        return {"processed": processed, "failed": failed}
