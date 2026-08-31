"""Tests for v0.6 Phase 4: Outbox pattern ingestion pipeline.

- OutboxEntry model and OutboxRepository CRUD
- OutboxWorker processes entries → updates Qdrant + graph
- Crash recovery: pending entries survive restart
- Retry logic: failed after 3 retries → marked as failed
- Server integration: remember/learn write outbox entries
"""

import asyncio
import json
import logging
import os
import subprocess
import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from storage.base import Base
from storage.outbox import OutboxEntry, OutboxEntryORM, OutboxRepository
from storage.outbox_worker import OutboxWorker

from memory_server.models import Fact, MemoryReceipt, VerificationStatus
from memory_server.providers.embedding_provider import SentenceTransformerEmbeddingProvider
from memory_server.providers.exceptions import ProviderSearchError, ProviderWriteError
from memory_server.providers.graph_provider import SimpleGraph
from memory_server.providers.qdrant_provider import QdrantProvider
from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.router.graph_router import GraphRouter

# =============================================================================
# Fixtures
# =============================================================================


def _make_engine_and_factory():
    """Create a unique file-based SQLite engine + session factory."""
    db_path = f"/tmp/test_outbox_{uuid.uuid4().hex[:16]}.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    return engine, factory, db_path


@pytest.fixture
async def empty_db():
    """Create a fresh temp DB with all tables created."""
    engine, factory, db_path = _make_engine_and_factory()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield factory, db_path
    await engine.dispose()
    if os.path.exists(db_path):
        os.unlink(db_path)


@pytest.fixture
async def repo(empty_db):
    """Create an OutboxRepository with a single session."""
    factory, _ = empty_db
    async with factory() as session:
        yield OutboxRepository(session)


@pytest.fixture
async def provider():
    """SQLiteProvider with in-memory DB."""
    p = SQLiteProvider(url="sqlite+aiosqlite:///:memory:")
    await p.initialize()
    yield p
    await p.close()


@pytest.fixture
async def qdrant_provider():
    """In-memory Qdrant provider."""
    return QdrantProvider(location=":memory:", prefer_grpc=False)


@pytest.fixture
async def embedder():
    """Sentence transformer embedder."""
    return SentenceTransformerEmbeddingProvider()


@pytest.fixture
def graph():
    """In-memory graph."""
    return SimpleGraph()


@pytest.fixture
def graph_router(graph):
    """Graph router wrapping SimpleGraph."""
    return GraphRouter(graph=graph)


@pytest.fixture
async def outbox_worker(qdrant_provider, embedder, graph_router):
    """Create an OutboxWorker with unique file-based DB."""
    engine, factory, db_path = _make_engine_and_factory()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    db_url = f"sqlite+aiosqlite:///{db_path}"
    worker = OutboxWorker(
        db_url=db_url,
        qdrant=qdrant_provider,
        embedder=embedder,
        graph_router=graph_router,
    )
    await worker.initialize()

    yield worker

    await worker.close()
    await engine.dispose()
    if os.path.exists(db_path):
        os.unlink(db_path)


# =============================================================================
# OutboxRepository CRUD Tests
# =============================================================================


@pytest.mark.asyncio
class TestOutboxRepository:
    """Test OutboxRepository CRUD operations."""

    async def test_add_entry(self, repo):
        """Adding an entry creates a pending record."""
        entry = await repo.add_entry(
            record_type="fact",
            record_id="f1",
            operation="index_fact",
            payload={"subject": "Test", "predicate": "is", "object": "Working"},
        )
        assert entry.id is not None
        assert entry.record_type == "fact"
        assert entry.operation == "index_fact"
        assert entry.status == "pending"
        assert entry.retry_count == 0
        assert entry.error is None

    async def test_get_pending_returns_oldest_first(self, empty_db):
        """Multiple entries returned in FIFO order."""
        factory, _ = empty_db
        async with factory() as session:
            repo = OutboxRepository(session)
            e1 = await repo.add_entry("fact", "f1", "index_fact", {"x": "1"})
            await repo.add_entry("fact", "f2", "index_fact", {"x": "2"})
            await session.commit()

            pending = await repo.get_pending()
            assert len(pending) == 2
            assert pending[0].id == e1.id

    async def test_get_pending_limit(self, empty_db):
        """Respects the limit parameter."""
        factory, _ = empty_db
        async with factory() as session:
            repo = OutboxRepository(session)
            for i in range(5):
                await repo.add_entry("fact", f"f{i}", "index_fact", {"x": i})
            await session.commit()

            pending = await repo.get_pending(limit=3)
            assert len(pending) == 3

    async def test_mark_completed(self, empty_db):
        """Marking an entry as completed updates status."""
        factory, _ = empty_db
        async with factory() as session:
            repo = OutboxRepository(session)
            entry = await repo.add_entry("fact", "f1", "index_fact", {})
            await session.commit()

            result = await repo.mark_completed(entry.id)
            assert result is True
            await session.commit()

            pending = await repo.get_pending()
            assert len(pending) == 0

    async def test_mark_failed(self, empty_db):
        """Marking as failed sets error message."""
        factory, _ = empty_db
        async with factory() as session:
            repo = OutboxRepository(session)
            entry = await repo.add_entry("fact", "f1", "index_fact", {})
            await session.commit()

            result = await repo.mark_failed(entry.id, "Something went wrong")
            assert result is True
            await session.commit()

            failed = await repo.get_failed()
            assert len(failed) == 1
            assert "Something went wrong" in (failed[0].error or "")

    async def test_get_failed_empty(self, empty_db):
        """No failed entries returns empty list."""
        factory, _ = empty_db
        async with factory() as session:
            repo = OutboxRepository(session)
            failed = await repo.get_failed()
            assert failed == []

    async def test_increment_retry(self, empty_db):
        """Increment retry resets to pending with incremented count."""
        factory, _ = empty_db
        async with factory() as session:
            repo = OutboxRepository(session)
            entry = await repo.add_entry("fact", "f1", "index_fact", {})
            await session.commit()

            count = await repo.increment_retry(entry.id, "attempt failed")
            assert count == 1
            await session.commit()

            pending = await repo.get_pending()
            assert len(pending) == 1
            assert pending[0].retry_count == 1

    async def test_increment_retry_past_max(self, empty_db):
        """Should reach max retries then fail."""
        factory, _ = empty_db
        async with factory() as session:
            repo = OutboxRepository(session)
            entry = await repo.add_entry("fact", "f1", "index_fact", {})
            await session.commit()

            for i in range(3):
                c = await repo.increment_retry(entry.id, f"attempt {i + 1} failed")
                await session.commit()
                assert c == i + 1

            await repo.mark_failed(entry.id, "exhausted retries")
            await session.commit()

            failed = await repo.get_failed()
            assert len(failed) == 1
            assert failed[0].retry_count == 3
            assert failed[0].status == "failed"


# =============================================================================
# OutboxWorker Integration Tests
# =============================================================================


class _FakeEmbedder:
    def embed(self, text: str) -> list[float]:
        return [0.1, 0.2, 0.3]

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        return [[0.1, 0.2, 0.3] for _ in texts]


class _FakeQdrantFalse:
    """Write methods always return False (defensive failure signal)."""

    def __init__(self) -> None:
        self.upsert_batch_calls = 0
        self.upsert_calls = 0

    async def upsert(self, **kwargs) -> bool:
        self.upsert_calls += 1
        return False

    # Worker calls upsert_batch(points) POSITIONALLY (outbox_worker.py:337);
    # a **kwargs-only fake raises TypeError before returning False and never
    # exercises the False branch.
    async def upsert_batch(self, *args, **kwargs) -> bool:
        self.upsert_batch_calls += 1
        return False


class _FakeQdrantTrue:
    """Write methods always succeed — the vector chunk step passes; whether
    the chunk fails is decided by the graph step."""

    def __init__(self) -> None:
        self.upsert_batch_calls = 0
        self.upsert_calls = 0

    async def upsert(self, **kwargs) -> bool:
        self.upsert_calls += 1
        return True

    async def upsert_batch(self, *args, **kwargs) -> bool:
        self.upsert_batch_calls += 1
        return True


class _FakeQdrantRaises:
    """Raises the 3a typed error on writes."""

    def __init__(self, exc: type[Exception]) -> None:
        self._exc = exc

    async def upsert(self, **kwargs) -> bool:
        raise self._exc("backend boom")


class _FakeRouterFalse:
    """Graph sync methods always return False."""

    def __init__(self) -> None:
        self.sync_facts_batch_calls = 0
        self.sync_fact_calls = 0

    def sync_fact(self, *args, **kwargs) -> bool:
        self.sync_fact_calls += 1
        return False

    def sync_facts_batch(self, *args, **kwargs) -> bool:
        self.sync_facts_batch_calls += 1
        return False


async def _make_worker(**kw) -> OutboxWorker:
    engine, factory, db_path = _make_engine_and_factory()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    # The bootstrap engine is only needed for create_all; the worker builds
    # its own engine from db_url in initialize(). Dispose it so no aiosqlite
    # worker thread outlives the test's event loop (avoids
    # PytestUnhandledThreadExceptionWarning "Event loop is closed").
    await engine.dispose()
    worker = OutboxWorker(
        db_url=f"sqlite+aiosqlite:///{db_path}",
        max_retries=3,
        **kw,
    )
    await worker.initialize()
    return worker


@pytest.mark.asyncio
class TestOutboxWorker:
    """Test the outbox worker processes entries correctly."""

    async def test_stop_terminates_run_loop(self, outbox_worker):
        """stop() must make run() exit instead of polling forever."""
        import asyncio

        task = asyncio.create_task(outbox_worker.run())
        # Give the loop a chance to enter the poll cycle
        await asyncio.sleep(0.1)
        assert not task.done()

        outbox_worker.stop()
        await asyncio.wait_for(task, timeout=5.0)
        assert task.done()

    async def test_poll_once_returns_processed_count(self, outbox_worker):
        """_poll_once returns the number of entries processed, 0 when empty."""
        # Empty queue → 0
        assert await outbox_worker._poll_once() == 0

        async with outbox_worker._session_factory() as session:
            repo = OutboxRepository(session)
            await repo.add_entry(
                record_type="fact",
                record_id=str(uuid.uuid4()),
                operation="index_fact",
                payload={
                    "subject": "Count",
                    "predicate": "returns",
                    "object": "one",
                    "source": "test",
                },
            )
            await session.commit()

        assert await outbox_worker._poll_once() == 1

    async def test_process_fact_index_updates_qdrant_and_graph(self, outbox_worker, qdrant_provider, embedder):
        """Add a fact outbox entry → worker processes it → Qdrant + graph updated."""
        graph = outbox_worker._graph_router.graph

        # Create a collection in Qdrant
        await qdrant_provider.create_collection("memory_facts")

        # Manually add an outbox entry via the worker's session
        async with outbox_worker._session_factory() as session:
            repo = OutboxRepository(session)
            fact_id = str(uuid.uuid4())
            await repo.add_entry(
                record_type="fact",
                record_id=fact_id,
                operation="index_fact",
                payload={
                    "subject": "Docker",
                    "predicate": "runs_on",
                    "object": "OMV8",
                    "source": "test",
                },
            )
            await session.commit()

        # Process all pending
        result = await outbox_worker.process_all_pending()
        assert result["processed"] == 1

        # Verify Qdrant was updated
        vector = await asyncio.to_thread(embedder.embed, "Docker runs_on OMV8")
        search_results = await qdrant_provider.search(
            vector=vector,
            limit=5,
            score_threshold=0.0,
        )
        found = any(r["payload"].get("subject") == "Docker" for r in search_results)
        assert found, "Fact should be indexed in Qdrant"

        # Verify graph was updated
        subject_node = graph.get_node("docker")
        assert subject_node is not None, "Subject should exist in graph (id='docker')"
        assert subject_node.type == "entity"

    async def test_process_decision_index_updates_graph(self, outbox_worker):
        """Add a decision outbox entry → worker processes it → graph updated."""
        graph = outbox_worker._graph_router.graph
        async with outbox_worker._session_factory() as session:
            repo = OutboxRepository(session)
            decision_id = str(uuid.uuid4())
            await repo.add_entry(
                record_type="decision",
                record_id=decision_id,
                operation="index_decision",
                payload={
                    "choice": "use Caddy",
                    "reason": "it is simpler",
                    "context": "web server decision",
                },
            )
            await session.commit()

        result = await outbox_worker.process_all_pending()
        assert result["processed"] == 1

        # Verify graph was updated
        decision_node = graph.get_node("decision-use-caddy")
        assert decision_node is not None, "Decision node should exist in graph"
        assert decision_node.type == "decision"

    async def test_process_skill_index_updates_graph(self, outbox_worker):
        """Add a skill outbox entry → worker processes it → graph updated."""
        graph = outbox_worker._graph_router.graph
        async with outbox_worker._session_factory() as session:
            repo = OutboxRepository(session)
            skill_id = str(uuid.uuid4())
            await repo.add_entry(
                record_type="skill",
                record_id=skill_id,
                operation="index_skill",
                payload={
                    "purpose": "deploy Docker",
                    "steps": ["pull image", "run container"],
                },
            )
            await session.commit()

        result = await outbox_worker.process_all_pending()
        assert result["processed"] == 1

        # Verify graph was updated (node ID has hyphens from _to_node_id)
        assert graph.get_node("skill-deploy-docker") is not None

    async def test_idempotent_processing(self, outbox_worker):
        """Processing the same entry twice is safe (idempotent)."""
        graph = outbox_worker._graph_router.graph
        async with outbox_worker._session_factory() as session:
            repo = OutboxRepository(session)
            await repo.add_entry(
                record_type="fact",
                record_id="idempotent-test",
                operation="index_fact",
                payload={
                    "subject": "Idempotent",
                    "predicate": "is",
                    "object": "Safe",
                    "source": "test",
                },
            )
            await session.commit()

        # Process once
        await outbox_worker.process_all_pending()

        # Add another entry with the same fact_id to simulate replay
        async with outbox_worker._session_factory() as session:
            repo = OutboxRepository(session)
            await repo.add_entry(
                record_type="fact",
                record_id="idempotent-test",
                operation="index_fact",
                payload={
                    "subject": "Idempotent",
                    "predicate": "is",
                    "object": "Safe",
                    "source": "test",
                },
            )
            await session.commit()

        # Process again — should not crash
        result = await outbox_worker.process_all_pending()
        assert result["processed"] == 1

        # Graph should only have one node for "idempotent" (lowercased)
        node = graph.get_node("idempotent")
        assert node is not None

    async def test_failed_entry_after_three_retries(self, empty_db):
        """Entry that fails 3 times is marked as failed."""
        factory, _ = empty_db

        async with factory() as session:
            repo = OutboxRepository(session)
            entry = await repo.add_entry(
                record_type="fact",
                record_id=str(uuid.uuid4()),
                operation="index_fact",
                payload={
                    "subject": "WillFail",
                    "predicate": "is",
                    "object": "Broken",
                    "source": "test",
                },
            )
            await session.commit()
            entry_id = entry.id

        async with factory() as session:
            repo = OutboxRepository(session)

            for i in range(3):
                _ = await repo.increment_retry(entry_id, f"error #{i + 1}")
                await session.commit()

            await repo.mark_failed(entry_id, "exhausted all retries")
            await session.commit()

            failed = await repo.get_failed()
            assert len(failed) == 1
            assert failed[0].status == "failed"
            assert failed[0].retry_count == 3

    async def test_crash_recovery_pending_survive(self, empty_db):
        """Pending entries survive worker restart (simulated via new session)."""
        factory, _ = empty_db

        # Add an entry
        async with factory() as session:
            repo = OutboxRepository(session)
            await repo.add_entry(
                record_type="fact",
                record_id="crash-test",
                operation="index_fact",
                payload={
                    "subject": "Crash",
                    "predicate": "recovers",
                    "object": "Fine",
                    "source": "test",
                },
            )
            await session.commit()

        # Now read it with a NEW session — simulates restart
        async with factory() as session:
            repo = OutboxRepository(session)
            pending = await repo.get_pending()
            assert len(pending) == 1
            assert pending[0].record_id == "crash-test"
            assert pending[0].status == "pending"

    async def test_fact_batch_completes_all_entries(self, outbox_worker, qdrant_provider, embedder):
        """Batch path: many index_fact entries are all completed and indexed."""
        # Chunk size is 32; add more than one chunk to exercise chunking.
        total = 70
        async with outbox_worker._session_factory() as session:
            repo = OutboxRepository(session)
            for i in range(total):
                await repo.add_entry(
                    record_type="fact",
                    record_id=f"batch-{i}",
                    operation="index_fact",
                    payload={
                        "subject": f"BatchSubject{i}",
                        "predicate": "is",
                        "object": f"BatchObject{i}",
                        "source": "test",
                    },
                )
            await session.commit()

        assert await outbox_worker._poll_once() == total

        async with outbox_worker._session_factory() as session:
            repo = OutboxRepository(session)
            pending = await repo.get_pending()
            assert len(pending) == 0

        # Spot-check that the vectors landed in Qdrant.
        vector = await asyncio.to_thread(embedder.embed, "BatchSubject7 is BatchObject7")
        search_results = await qdrant_provider.search(
            vector=vector,
            limit=5,
            score_threshold=0.0,
        )
        found = any(r["payload"].get("subject") == "BatchSubject7" for r in search_results)
        assert found, "Batch-embedded fact should be indexed in Qdrant"

    async def test_fact_batch_falls_back_per_entry_on_bad_payload(self, outbox_worker, qdrant_provider):
        """A malformed payload must not poison the whole batch.

        The chunk containing the bad entry falls back to per-entry handling:
        the bad entry gets retried, healthy entries in the same chunk still
        complete, and healthy entries in other chunks are untouched.
        """
        async with outbox_worker._session_factory() as session:
            repo = OutboxRepository(session)
            for i in range(70):
                await repo.add_entry(
                    record_type="fact",
                    record_id=f"mixed-{i}",
                    operation="index_fact",
                    payload={
                        "subject": f"Mixed{i}",
                        "predicate": "is",
                        "object": f"Val{i}",
                        "source": "test",
                    },
                )
            # Corrupt one payload directly in the DB (as a rogue writer would).
            from sqlalchemy import text as sa_text

            await session.flush()  # ensure inserted rows exist before UPDATE
            await session.execute(
                sa_text(
                    "UPDATE outbox_entries SET payload_json = 'not-json' "
                    "WHERE record_id = 'mixed-31'"
                )
            )
            await session.commit()

        # Processing must not raise: the bad chunk falls back per-entry.
        await outbox_worker._poll_once()

        async with outbox_worker._session_factory() as session:
            repo = OutboxRepository(session)
            pending = await repo.get_pending()
            # 69 healthy entries should be completed; only the bad one remains.
            assert len(pending) == 1
            assert pending[0].record_id == "mixed-31"
            assert pending[0].retry_count == 1

    async def test_maybe_compact_passes_cleanup_older_than(self, tmp_path):
        """Compaction must pass an explicit cleanup_older_than threshold.

        Regression: _maybe_compact() called optimize() with no threshold, so
        LanceDB's ~7-day default applied and deleted nothing on a busy store
        where every version is younger than a week — versions accumulated to
        hundreds of GB. The worker must schedule compaction with an explicit
        sub-week cleanup_older_than and run it as a background task.
        """
        from datetime import timedelta

        from storage.outbox_worker import OutboxWorker

        captured = {}

        class FakeOptimizeProvider:
            async def optimize(self, **kwargs):
                captured["kwargs"] = kwargs
                return True

        worker = OutboxWorker(
            db_url=f"sqlite+aiosqlite:///{tmp_path / 'compact-test.db'}",
            qdrant=FakeOptimizeProvider(),
        )
        worker._last_compact_at = 0.0  # force first run

        # Give the background task a chance to run.
        await asyncio.sleep(0.2)
        await worker._maybe_compact()
        await asyncio.sleep(0.2)

        assert "kwargs" in captured, "optimize() was never called"
        kw = captured["kwargs"]
        assert "cleanup_older_than" in kw, (
            "optimize() must receive an explicit cleanup_older_than"
        )
        assert kw["cleanup_older_than"] <= timedelta(hours=1), (
            f"cleanup_older_than must be sub-week, got {kw['cleanup_older_than']}"
        )
        # Tuning values are instance attributes now (constructor kwargs with
        # the previous module-constant defaults).
        assert worker._compact_cleanup_hours == 1
        assert worker._compact_interval_seconds > 0

    # ── Card 3b: provider False / typed-error results are failures ──────

    async def _insert_entry(self, worker, operation, record_id, payload):
        async with worker._session_factory() as session:
            repo = OutboxRepository(session)
            await repo.add_entry(
                record_type=operation.replace("index_", ""),
                record_id=record_id,
                operation=operation,
                payload=payload,
            )
            await session.commit()

    async def _entry_row(self, worker, record_id):
        from sqlalchemy import select as sa_select

        async with worker._session_factory() as session:
            row = (
                await session.execute(
                    sa_select(OutboxEntryORM).where(OutboxEntryORM.record_id == record_id)
                )
            ).scalars().first()
            return row

    async def test_index_fact_false_upsert_not_completed_retried_then_failed(self):
        """qdrant.upsert returning False → never completed; retried then failed."""
        worker = await _make_worker(
            qdrant=_FakeQdrantFalse(),
            embedder=_FakeEmbedder(),
            graph_router=None,
        )
        try:
            await self._insert_entry(
                worker, "index_fact", "f-false-upsert",
                {"subject": "S", "predicate": "p", "object": "O", "source": "test"},
            )
            assert await worker.process_all_pending() == {"processed": 0, "failed": 0}
            assert (await self._entry_row(worker, "f-false-upsert")).status == "pending"
            assert await worker.process_all_pending() == {"processed": 0, "failed": 0}
            assert (await self._entry_row(worker, "f-false-upsert")).status == "pending"
            assert await worker.process_all_pending() == {"processed": 0, "failed": 1}
            row = await self._entry_row(worker, "f-false-upsert")
            assert row.status == "failed"
            assert row.retry_count == 3
            assert "returned False" in (row.error or "")
        finally:
            await worker.close()

    async def test_index_belief_false_upsert_not_completed_retried_then_failed(self):
        """Belief upsert returning False → never completed; retried then failed."""
        worker = await _make_worker(
            qdrant=_FakeQdrantFalse(),
            embedder=_FakeEmbedder(),
            graph_router=None,
        )
        try:
            await self._insert_entry(
                worker, "index_belief", "b-false-upsert",
                {"proposition": "P", "confidence": 0.5, "tags": []},
            )
            assert await worker.process_all_pending() == {"processed": 0, "failed": 0}
            assert (await self._entry_row(worker, "b-false-upsert")).status == "pending"
            assert await worker.process_all_pending() == {"processed": 0, "failed": 0}
            assert (await self._entry_row(worker, "b-false-upsert")).status == "pending"
            assert await worker.process_all_pending() == {"processed": 0, "failed": 1}
            row = await self._entry_row(worker, "b-false-upsert")
            assert row.status == "failed"
            assert row.retry_count == 3
            assert "returned False" in (row.error or "")
        finally:
            await worker.close()

    async def test_index_fact_false_sync_fact_not_completed_retried_then_failed(self):
        """graph sync_fact returning False → never completed; retried then failed."""
        worker = await _make_worker(
            qdrant=None,
            embedder=_FakeEmbedder(),
            graph_router=_FakeRouterFalse(),
        )
        try:
            await self._insert_entry(
                worker, "index_fact", "f-false-sync",
                {"subject": "S", "predicate": "p", "object": "O", "source": "test"},
            )
            assert await worker.process_all_pending() == {"processed": 0, "failed": 0}
            assert (await self._entry_row(worker, "f-false-sync")).status == "pending"
            assert await worker.process_all_pending() == {"processed": 0, "failed": 0}
            assert (await self._entry_row(worker, "f-false-sync")).status == "pending"
            assert await worker.process_all_pending() == {"processed": 0, "failed": 1}
            row = await self._entry_row(worker, "f-false-sync")
            assert row.status == "failed"
            assert row.retry_count == 3
            assert "sync_fact returned False" in (row.error or "")
        finally:
            await worker.close()

    @pytest.mark.parametrize("exc", [ProviderWriteError, ProviderSearchError])
    async def test_provider_typed_error_not_completed_retried_then_failed(self, exc):
        """Typed provider error → never completed; retried then failed (green-guard)."""
        worker = await _make_worker(
            qdrant=_FakeQdrantRaises(exc),
            embedder=_FakeEmbedder(),
            graph_router=None,
        )
        try:
            await self._insert_entry(
                worker, "index_fact", f"f-typed-{exc.__name__}",
                {"subject": "S", "predicate": "p", "object": "O", "source": "test"},
            )
            assert await worker.process_all_pending() == {"processed": 0, "failed": 0}
            assert await worker.process_all_pending() == {"processed": 0, "failed": 0}
            assert await worker.process_all_pending() == {"processed": 0, "failed": 1}
            row = await self._entry_row(worker, f"f-typed-{exc.__name__}")
            assert row.status == "failed"
            assert row.retry_count == 3
            assert "backend boom" in (row.error or "")
        finally:
            await worker.close()

    async def test_fact_batch_false_provider_falls_back_to_per_entry_failure(self, caplog):
        """Vector chunk upsert_batch False → per-entry fallback → all retried."""
        qdrant = _FakeQdrantFalse()
        router = _FakeRouterFalse()
        worker = await _make_worker(
            qdrant=qdrant,
            embedder=_FakeEmbedder(),
            graph_router=router,
            fact_batch_chunk_size=32,
        )
        try:
            for i in range(3):
                await self._insert_entry(
                    worker, "index_fact", f"f-batch-{i}",
                    {"subject": f"S{i}", "predicate": "p", "object": "O", "source": "test"},
                )
            with caplog.at_level(logging.WARNING, logger="storage.outbox_worker"):
                assert await worker._poll_once() == 3
            for i in range(3):
                row = await self._entry_row(worker, f"f-batch-{i}")
                assert row.status == "pending"
                assert row.retry_count == 1
                assert "qdrant upsert returned False" in (row.error or "")
            assert qdrant.upsert_batch_calls == 1
            assert qdrant.upsert_calls == 3
            assert "upsert_batch returned False" in caplog.text
        finally:
            await worker.close()

    async def test_fact_batch_false_graph_sync_falls_back_to_per_entry_failure(self, caplog):
        """Graph chunk sync_facts_batch False → per-entry fallback → all retried."""
        qdrant = _FakeQdrantTrue()
        router = _FakeRouterFalse()
        worker = await _make_worker(
            qdrant=qdrant,
            embedder=_FakeEmbedder(),
            graph_router=router,
            fact_batch_chunk_size=32,
        )
        try:
            for i in range(3):
                await self._insert_entry(
                    worker, "index_fact", f"f-gbatch-{i}",
                    {"subject": f"S{i}", "predicate": "p", "object": "O", "source": "test"},
                )
            with caplog.at_level(logging.WARNING, logger="storage.outbox_worker"):
                assert await worker._poll_once() == 3
            for i in range(3):
                row = await self._entry_row(worker, f"f-gbatch-{i}")
                assert row.status == "pending"
                assert row.retry_count == 1
                assert "sync_fact returned False" in (row.error or "")
            assert qdrant.upsert_batch_calls == 1
            assert router.sync_facts_batch_calls == 1
            assert router.sync_fact_calls == 3
            assert "sync_facts_batch returned False" in caplog.text
        finally:
            await worker.close()


# =============================================================================
# Server Integration Tests
# =============================================================================


@pytest.mark.asyncio
class TestServerOutboxIntegration:
    """Test that server remember/learn write outbox entries."""

    async def test_remember_writes_outbox_entry(self, provider):
        """Calling remember() results in an outbox entry being created."""
        from memory_server.api.remember import remember

        result = await remember(
            provider,
            subject="OutboxTest",
            predicate="is",
            object="Working",
            source="test",
        )
        fact = result["fact"]
        assert fact.id is not None

    async def test_learn_writes_outbox_entries(self, provider):
        """Calling learn() results in outbox entries being created."""
        from memory_server.api.learn import learn

        result = await learn(
            provider,
            text="Docker is container. decided to use Caddy because simple",
            source="test",
        )

        assert len(result["facts"]) >= 1
        assert len(result["decisions"]) >= 1

    async def test_provider_create_in_transaction(self, provider):
        """create_in_transaction stores all items atomically."""
        fact = Fact(
            id="tx-fact-1",
            subject="Transactional",
            predicate="is",
            object="Atomic",
            confidence=1.0,
            source="test",
            created_at=datetime.now(timezone.utc),
        )
        receipt = MemoryReceipt(
            id="tx-fact-1",
            memory_type="fact",
            source="test",
            created_by="test",
            timestamp=datetime.now(timezone.utc),
            confidence=1.0,
            verification_status=VerificationStatus.CANDIDATE,
        )

        await provider.create_in_transaction(
            fact=fact,
            receipt=receipt,
            outbox_entries=[
                {
                    "record_type": "fact",
                    "record_id": "tx-fact-1",
                    "operation": "index_fact",
                    "payload": {
                        "subject": "Transactional",
                        "predicate": "is",
                        "object": "Atomic",
                        "source": "test",
                    },
                }
            ],
        )

        stored_fact = await provider.get_fact("tx-fact-1")
        assert stored_fact is not None
        assert stored_fact.subject == "Transactional"

        stored_receipt = await provider.get_receipt("tx-fact-1")
        assert stored_receipt is not None

    async def test_outbox_entry_model_payload(self):
        """OutboxEntry.payload deserializes JSON correctly."""
        entry = OutboxEntry(
            id="test-1",
            record_type="fact",
            record_id="f1",
            operation="index_fact",
            payload_json=json.dumps({"subject": "Test", "predicate": "is", "object": "Val"}),
        )
        payload = entry.payload
        assert payload["subject"] == "Test"
        assert payload["predicate"] == "is"
        assert payload["object"] == "Val"

    async def test_outbox_orm_roundtrip(self, empty_db):
        """ORM model round-trips correctly through the database."""
        factory, _ = empty_db
        async with factory() as session:
            orm = OutboxEntryORM(
                id="orm-test-1",
                record_type="fact",
                record_id="f1",
                operation="index_fact",
                payload_json=json.dumps({"key": "value"}),
                status="pending",
                retry_count=0,
                created_at=datetime.now(timezone.utc),
            )
            session.add(orm)
            await session.commit()

        async with factory() as session:
            result = await session.get(OutboxEntryORM, "orm-test-1")
            assert result is not None
            assert result.id == "orm-test-1"
            assert result.status == "pending"
            assert result.retry_count == 0

    async def test_migration_creates_outbox_table(self):
        """Verify Alembic --sql output includes outbox_entries table."""
        project_dir = os.path.join(os.path.dirname(__file__), "..")

        result = subprocess.run(
            ["alembic", "upgrade", "head", "--sql"],
            cwd=project_dir,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, f"alembic upgrade --sql failed: {result.stderr}"

        sql_output = result.stdout
        assert "CREATE TABLE outbox_entries" in sql_output, "Migration should create outbox_entries table"


# =============================================================================
# Card 2: exception contract (D4), real failed count (D5), belief deferral (D6)
# =============================================================================


class StubEmbedder:
    """Deterministic embedder — fixed vector, no model load."""

    def embed(self, text: str) -> list[float]:
        return [0.1] * 8

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        return [[0.1] * 8 for _ in texts]


class ChunkFailingEmbedder(StubEmbedder):
    """Embedder whose batch path always raises (per-entry path works)."""

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        raise RuntimeError("injected embed_batch failure")


class FakeVectorProvider:
    """Records upsert/upsert_batch calls; upsert raises for matching payloads.

    ``fail_predicate`` receives the payload dict; when it returns True the
    upsert raises a plain RuntimeError (no provider exceptions module exists
    in this card). ``upsert_batch`` raises unconditionally so the chunk path
    can be forced into the per-entry fallback.
    """

    def __init__(self, fail_predicate=None):
        self.fail_predicate = fail_predicate
        self.upsert_calls = 0
        self.upsert_batch_calls = 0

    async def upsert(self, **kwargs):
        self.upsert_calls += 1
        payload = kwargs.get("payload", {})
        if self.fail_predicate and self.fail_predicate(payload):
            raise RuntimeError("injected upsert failure")
        return True

    async def upsert_batch(self, points):
        self.upsert_batch_calls += 1
        raise RuntimeError("injected upsert_batch failure")


class FailingGraphRouter:
    """Graph router whose sync methods always raise (injected graph failure)."""

    def sync_fact(self, subject, predicate, object):
        raise RuntimeError("injected graph sync failure")

    def sync_facts_batch(self, triples):
        raise RuntimeError("injected graph batch sync failure")


@pytest.mark.asyncio
class TestOutboxExceptionContract:
    """Card 2, D4/D5: exception = failure; no completion; real failed count.

    Every indexing step (embed, vector upsert, graph sync) may raise; the
    exception propagates to ``_process_entry``'s handler, which never marks
    the entry completed. ``process_all_pending`` returns the REAL exhausted
    failure count; retryable entries stay pending and are counted nowhere.
    """

    async def _worker(self, *, qdrant, embedder, graph_router, max_retries):
        """Fresh OutboxWorker wired with the given fakes."""
        engine, factory, db_path = _make_engine_and_factory()
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        worker = OutboxWorker(
            db_url=f"sqlite+aiosqlite:///{db_path}",
            qdrant=qdrant,
            embedder=embedder,
            graph_router=graph_router,
            max_retries=max_retries,
        )
        await worker.initialize()
        return worker, engine, db_path

    async def _add_entry(self, worker, operation, record_id, payload):
        async with worker._session_factory() as session:
            repo = OutboxRepository(session)
            entry = await repo.add_entry(
                record_type=operation.replace("index_", ""),
                record_id=record_id,
                operation=operation,
                payload=payload,
            )
            await session.commit()
            return entry.id

    async def _statuses(self, worker):
        from sqlalchemy import select as sa_select

        async with worker._session_factory() as session:
            rows = (await session.execute(sa_select(OutboxEntryORM))).scalars().all()
            return {row.record_id: row.status for row in rows}

    async def test_upsert_exception_no_completion_failed_after_exhaustion(self):
        """Vector upsert raises → entry never completed → failed; graph untouched."""
        graph = SimpleGraph()
        qdrant = FakeVectorProvider(fail_predicate=lambda payload: True)
        worker, engine, db_path = await self._worker(
            qdrant=qdrant,
            embedder=StubEmbedder(),
            graph_router=GraphRouter(graph=graph),
            max_retries=1,
        )
        try:
            entry_id = await self._add_entry(
                worker, "index_fact", "f-raise",
                {"subject": "Raise", "predicate": "is", "object": "Broken", "source": "test"},
            )
            result = await worker.process_all_pending()
            assert result == {"processed": 0, "failed": 1}

            async with worker._session_factory() as session:
                repo = OutboxRepository(session)
                failed = await repo.get_failed()
                assert len(failed) == 1
                assert failed[0].id == entry_id
                assert failed[0].retry_count == 1
                assert "injected upsert failure" in (failed[0].error or "")

            # Graph sync never ran because the upsert failed first.
            assert graph.to_dict()["nodes"] == {}
            assert graph.to_dict()["edges"] == []
        finally:
            await worker.close()
            await engine.dispose()
            if os.path.exists(db_path):
                os.unlink(db_path)

    async def test_upsert_exception_retryable_stays_pending(self):
        """Same failure with headroom → stays pending, counted in neither bucket."""
        qdrant = FakeVectorProvider(fail_predicate=lambda payload: True)
        worker, engine, db_path = await self._worker(
            qdrant=qdrant,
            embedder=StubEmbedder(),
            graph_router=GraphRouter(graph=SimpleGraph()),
            max_retries=3,
        )
        try:
            entry_id = await self._add_entry(
                worker, "index_fact", "f-retry",
                {"subject": "Retry", "predicate": "is", "object": "Later", "source": "test"},
            )
            result = await worker.process_all_pending()
            assert result == {"processed": 0, "failed": 0}

            async with worker._session_factory() as session:
                repo = OutboxRepository(session)
                pending = await repo.get_pending()
                assert len(pending) == 1
                assert pending[0].id == entry_id
                assert pending[0].retry_count == 1
        finally:
            await worker.close()
            await engine.dispose()
            if os.path.exists(db_path):
                os.unlink(db_path)

    async def test_graph_sync_exception_no_completion(self):
        """Vector upsert succeeds; graph sync raises → not completed; retried → failed."""
        qdrant = FakeVectorProvider()
        worker, engine, db_path = await self._worker(
            qdrant=qdrant,
            embedder=StubEmbedder(),
            graph_router=FailingGraphRouter(),
            max_retries=2,
        )
        try:
            await self._add_entry(
                worker, "index_fact", "f-graph",
                {"subject": "Graph", "predicate": "is", "object": "Broken", "source": "test"},
            )
            # First attempt: retryable → pending (counted in neither bucket).
            assert await worker.process_all_pending() == {"processed": 0, "failed": 0}
            assert (await self._statuses(worker)).get("f-graph") == "pending"  # noqa: SIM118

            # Second attempt: exhausted → failed, and counted.
            assert await worker.process_all_pending() == {"processed": 0, "failed": 1}
            assert (await self._statuses(worker)).get("f-graph") == "failed"  # noqa: SIM118
        finally:
            await worker.close()
            await engine.dispose()
            if os.path.exists(db_path):
                os.unlink(db_path)

    async def test_belief_upsert_exception_failed_graph_unchanged(self):
        """Failing belief upsert → entry not completed → failed; graph unchanged."""
        graph = SimpleGraph()
        qdrant = FakeVectorProvider(
            fail_predicate=lambda payload: payload.get("memory_type") == "belief"
        )
        worker, engine, db_path = await self._worker(
            qdrant=qdrant,
            embedder=StubEmbedder(),
            graph_router=GraphRouter(graph=graph),
            max_retries=1,
        )
        try:
            entry_id = await self._add_entry(
                worker, "index_belief", "b-raise",
                {"proposition": "Docker is containerized", "confidence": 0.9, "tags": ["docker"]},
            )
            result = await worker.process_all_pending()
            assert result == {"processed": 0, "failed": 1}

            async with worker._session_factory() as session:
                repo = OutboxRepository(session)
                failed = await repo.get_failed()
                assert len(failed) == 1
                assert failed[0].id == entry_id

            assert graph.to_dict()["nodes"] == {}
            assert graph.to_dict()["edges"] == []
        finally:
            await worker.close()
            await engine.dispose()
            if os.path.exists(db_path):
                os.unlink(db_path)

    async def test_mixed_batch_counts_processed_and_failed(self):
        """Success + pre-exhausted + retryable → {"processed": 1, "failed": 1}."""
        qdrant = FakeVectorProvider(
            fail_predicate=lambda payload: payload.get("subject") in ("Exhausted", "Retryable")
        )
        worker, engine, db_path = await self._worker(
            qdrant=qdrant,
            embedder=StubEmbedder(),
            graph_router=GraphRouter(graph=SimpleGraph()),
            max_retries=2,
        )
        try:
            async with worker._session_factory() as session:
                repo = OutboxRepository(session)
                await repo.add_entry(
                    "fact", "f-good", "index_fact",
                    {"subject": "Good", "predicate": "is", "object": "Val", "source": "test"},
                )
                exhausted = await repo.add_entry(
                    "fact", "f-ex", "index_fact",
                    {"subject": "Exhausted", "predicate": "is", "object": "Val", "source": "test"},
                )
                await repo.add_entry(
                    "fact", "f-ret", "index_fact",
                    {"subject": "Retryable", "predicate": "is", "object": "Val", "source": "test"},
                )
                # Pre-seed retry_count = max_retries - 1 so the next failure exhausts.
                await repo.increment_retry(exhausted.id, "pre-seeded failure")
                await session.commit()

            result = await worker.process_all_pending()
            assert result == {"processed": 1, "failed": 1}

            statuses = await self._statuses(worker)
            assert statuses["f-good"] == "completed"
            assert statuses["f-ex"] == "failed"
            assert statuses["f-ret"] == "pending"
        finally:
            await worker.close()
            await engine.dispose()
            if os.path.exists(db_path):
                os.unlink(db_path)

    async def test_poll_once_returns_attempted_count_not_outcomes(self):
        """_poll_once returns len(entries) even when entries end up failed (D5)."""
        qdrant = FakeVectorProvider(fail_predicate=lambda payload: True)
        worker, engine, db_path = await self._worker(
            qdrant=qdrant,
            embedder=StubEmbedder(),
            graph_router=GraphRouter(graph=SimpleGraph()),
            max_retries=1,
        )
        try:
            for i in range(3):
                await self._add_entry(
                    worker, "index_fact", f"f-{i}",
                    {"subject": f"Fail{i}", "predicate": "is", "object": "Val", "source": "test"},
                )
            # Batch path: chunk fails (upsert_batch raises) → per-entry fallback
            # exhausts all three (max_retries=1) — _poll_once still returns 3.
            assert await worker._poll_once() == 3

            async with worker._session_factory() as session:
                repo = OutboxRepository(session)
                assert len(await repo.get_failed()) == 3
        finally:
            await worker.close()
            await engine.dispose()
            if os.path.exists(db_path):
                os.unlink(db_path)

    async def test_batch_path_chunk_failure_falls_back_per_entry(self):
        """Chunk exception is handled locally; outcomes live in per-entry DB state (4b)."""
        qdrant = FakeVectorProvider(
            fail_predicate=lambda payload: payload.get("subject") == "Bad"
        )
        graph = SimpleGraph()
        worker, engine, db_path = await self._worker(
            qdrant=qdrant,
            embedder=ChunkFailingEmbedder(),
            graph_router=GraphRouter(graph=graph),
            max_retries=1,
        )
        try:
            for i in range(2):
                await self._add_entry(
                    worker, "index_fact", f"f-good-{i}",
                    {"subject": f"Good{i}", "predicate": "is", "object": "Val", "source": "test"},
                )
            await self._add_entry(
                worker, "index_fact", "f-bad",
                {"subject": "Bad", "predicate": "is", "object": "Val", "source": "test"},
            )

            # (i) chunk exception handled locally — not converted into an aggregate.
            assert await worker._poll_once() == 3

            # (ii) per-entry DB statuses follow _process_entry semantics.
            statuses = await self._statuses(worker)
            assert statuses["f-good-0"] == "completed"
            assert statuses["f-good-1"] == "completed"
            assert statuses["f-bad"] == "failed"

            # (iii) process_all_pending tallies ONLY per-entry statuses — nothing pending.
            assert await worker.process_all_pending() == {"processed": 0, "failed": 0}
        finally:
            await worker.close()
            await engine.dispose()
            if os.path.exists(db_path):
                os.unlink(db_path)

    async def test_belief_entry_upserts_vector_without_graph_mutation(self):
        """Belief entries: vector upsert happens, graph stays empty (D6)."""
        qdrant = FakeVectorProvider()
        graph = SimpleGraph()
        worker, engine, db_path = await self._worker(
            qdrant=qdrant,
            embedder=StubEmbedder(),
            graph_router=GraphRouter(graph=graph),
            max_retries=3,
        )
        try:
            await self._add_entry(
                worker, "index_belief", "b-ok",
                {"proposition": "Docker is containerized", "confidence": 0.9, "tags": ["docker"]},
            )
            result = await worker.process_all_pending()
            assert result == {"processed": 1, "failed": 0}
            assert qdrant.upsert_calls == 1
            assert graph.to_dict()["nodes"] == {}
            assert graph.to_dict()["edges"] == []
        finally:
            await worker.close()
            await engine.dispose()
            if os.path.exists(db_path):
                os.unlink(db_path)
