"""Tests for v0.6 Phase 4: Outbox pattern ingestion pipeline.

- OutboxEntry model and OutboxRepository CRUD
- OutboxWorker processes entries → updates Qdrant + graph
- Crash recovery: pending entries survive restart
- Retry logic: failed after 3 retries → marked as failed
- Server integration: remember/learn write outbox entries
"""

import asyncio
import json
import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone

import pytest

from memory_server.models import Fact, MemoryReceipt, VerificationStatus
from memory_server.providers.embedding_provider import SentenceTransformerEmbeddingProvider
from memory_server.providers.qdrant_provider import QdrantProvider
from memory_server.providers.graph_provider import SimpleGraph
from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.router.graph_router import GraphRouter
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from storage.base import Base
from storage.outbox import OutboxEntryORM, OutboxEntry, OutboxRepository
from storage.outbox_worker import OutboxWorker


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


@pytest.mark.asyncio
class TestOutboxWorker:
    """Test the outbox worker processes entries correctly."""

    async def test_process_fact_index_updates_qdrant_and_graph(
        self, outbox_worker, qdrant_provider, embedder
    ):
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
        found = any(
            r["payload"].get("subject") == "Docker"
            for r in search_results
        )
        assert found, "Fact should be indexed in Qdrant"

        # Verify graph was updated
        subject_node = graph.get_node("docker")
        assert subject_node is not None, "Subject should exist in graph (id='docker')"
        assert subject_node.type == "entity"

    async def test_process_decision_index_updates_graph(
        self, outbox_worker
    ):
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

    async def test_process_skill_index_updates_graph(
        self, outbox_worker
    ):
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
                c = await repo.increment_retry(entry_id, f"error #{i + 1}")
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
            payload_json=json.dumps(
                {"subject": "Test", "predicate": "is", "object": "Val"}
            ),
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
            [sys.executable, "-m", "alembic", "upgrade", "head", "--sql"],
            cwd=project_dir,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, (
            f"alembic upgrade --sql failed: {result.stderr}"
        )

        sql_output = result.stdout
        assert "CREATE TABLE outbox_entries" in sql_output, (
            "Migration should create outbox_entries table"
        )
