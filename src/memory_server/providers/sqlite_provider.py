"""Async SQLite provider using SQLAlchemy + aiosqlite.

Per ADR-002: Facts use SQLite/PostgreSQL storage.
Per ADR-010: v0.1a uses SQLite only.

v0.6: Refactored to delegate to the new storage layer
(storage/models + storage/repositories) instead of maintaining
duplicate inline ORM models.
"""

from typing import Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from storage.base import Base
from storage.outbox import OutboxRepository
from storage.repositories import (
    BeliefRepository,
    DecisionRepository,
    EvidenceRepository,
    FactRepository,
    ReceiptRepository,
    SkillRepository,
)

from memory_server.models import (
    Belief,
    Decision,
    Evidence,
    Fact,
    MemoryReceipt,
    Skill,
)


class SQLiteProvider:
    """Async SQLite provider for CRUD operations on facts and receipts.

    v0.6: Delegates to the new storage layer (storage/repositories)
    for all CRUD operations. Keeps the same public interface for
    backward compatibility.

    WAL mode is enabled on initialization for concurrent read performance.
    """

    def __init__(self, url: str = "sqlite+aiosqlite:///memory.db"):
        self._url = url
        self._engine = None
        self._session_factory = None

    @property
    def engine(self):
        """Expose the SQLAlchemy engine for sharing with OutboxWorker."""
        return self._engine

    async def initialize(self):
        """Create engine, tables, and session factory with WAL mode."""
        self._engine = create_async_engine(self._url, echo=False)

        # Enable WAL mode for concurrent read performance
        async with self._engine.connect() as conn:
            await conn.exec_driver_sql("PRAGMA journal_mode=WAL")
            await conn.exec_driver_sql("PRAGMA synchronous=NORMAL")
            await conn.exec_driver_sql("PRAGMA busy_timeout=5000")

        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            # Create FTS5 virtual table on the facts table
            await conn.exec_driver_sql(
                "CREATE VIRTUAL TABLE IF NOT EXISTS facts_fts "
                "USING fts5(subject, predicate, object, "
                "content=facts, content_rowid=rowid)"
            )
            # Triggers to keep FTS index in sync
            await conn.exec_driver_sql(
                "CREATE TRIGGER IF NOT EXISTS facts_ai AFTER INSERT ON facts BEGIN "
                "INSERT INTO facts_fts(rowid, subject, predicate, object) "
                "VALUES (new.rowid, new.subject, new.predicate, new.object); END"
            )
            await conn.exec_driver_sql(
                "CREATE TRIGGER IF NOT EXISTS facts_ad AFTER DELETE ON facts BEGIN "
                "INSERT INTO facts_fts(facts_fts, rowid, subject, predicate, object) "
                "VALUES('delete', old.rowid, old.subject, old.predicate, old.object); END"
            )
            await conn.exec_driver_sql(
                "CREATE TRIGGER IF NOT EXISTS facts_au AFTER UPDATE ON facts BEGIN "
                "INSERT INTO facts_fts(facts_fts, rowid, subject, predicate, object) "
                "VALUES('delete', old.rowid, old.subject, old.predicate, old.object); "
                "INSERT INTO facts_fts(rowid, subject, predicate, object) "
                "VALUES (new.rowid, new.subject, new.predicate, new.object); END"
            )
            # Populate FTS with existing data
            await conn.exec_driver_sql(
                "INSERT OR IGNORE INTO facts_fts(facts_fts, rowid, subject, predicate, object) "
                "SELECT 'rebuild', rowid, subject, predicate, object FROM facts"
            )
            # Create beliefs FTS5 virtual table
            await conn.exec_driver_sql(
                "CREATE VIRTUAL TABLE IF NOT EXISTS beliefs_fts "
                "USING fts5(proposition, "
                "content=beliefs, content_rowid=rowid)"
            )
            # Triggers to keep beliefs FTS index in sync
            await conn.exec_driver_sql(
                "CREATE TRIGGER IF NOT EXISTS beliefs_ai AFTER INSERT ON beliefs BEGIN "
                "INSERT INTO beliefs_fts(rowid, proposition) "
                "VALUES (new.rowid, new.proposition); END"
            )
            await conn.exec_driver_sql(
                "CREATE TRIGGER IF NOT EXISTS beliefs_ad AFTER DELETE ON beliefs BEGIN "
                "INSERT INTO beliefs_fts(beliefs_fts, rowid, proposition) "
                "VALUES('delete', old.rowid, old.proposition); END"
            )
            await conn.exec_driver_sql(
                "CREATE TRIGGER IF NOT EXISTS beliefs_au AFTER UPDATE ON beliefs BEGIN "
                "INSERT INTO beliefs_fts(beliefs_fts, rowid, proposition) "
                "VALUES('delete', old.rowid, old.proposition); "
                "INSERT INTO beliefs_fts(rowid, proposition) "
                "VALUES (new.rowid, new.proposition); END"
            )
            # Populate beliefs FTS with existing data
            await conn.exec_driver_sql(
                "INSERT OR IGNORE INTO beliefs_fts(beliefs_fts, rowid, proposition) "
                "SELECT 'rebuild', rowid, proposition FROM beliefs"
            )

        self._session_factory = async_sessionmaker(
            self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

    async def close(self):
        """Dispose of the engine."""
        if self._engine:
            await self._engine.dispose()

    async def _get_session(self) -> AsyncSession:
        return self._session_factory()

    async def _get_fact_repo(self, session: AsyncSession) -> FactRepository:
        return FactRepository(session)

    async def _get_decision_repo(self, session: AsyncSession) -> DecisionRepository:
        return DecisionRepository(session)

    async def _get_skill_repo(self, session: AsyncSession) -> SkillRepository:
        return SkillRepository(session)

    async def _get_receipt_repo(self, session: AsyncSession) -> ReceiptRepository:
        return ReceiptRepository(session)

    async def _get_belief_repo(self, session: AsyncSession) -> BeliefRepository:
        return BeliefRepository(session)

    async def _get_evidence_repo(self, session: AsyncSession) -> EvidenceRepository:
        return EvidenceRepository(session)

    # --- Fact CRUD ---

    async def create_fact(self, fact: Fact) -> Fact:
        async with await self._get_session() as session:
            repo = await self._get_fact_repo(session)
            result = await repo.create(fact)
            await session.commit()
            return result

    async def get_fact(self, fact_id: str) -> Optional[Fact]:
        async with await self._get_session() as session:
            repo = await self._get_fact_repo(session)
            return await repo.get(fact_id)

    async def search_facts(
        self,
        subject: Optional[str] = None,
        predicate: Optional[str] = None,
        object: Optional[str] = None,
        source: Optional[str] = None,
        text: Optional[str] = None,
        min_confidence: Optional[float] = None,
        max_confidence: Optional[float] = None,
        limit: int = 50,
    ) -> list[Fact]:
        async with await self._get_session() as session:
            repo = await self._get_fact_repo(session)
            results = await repo.search(
                subject=subject, predicate=predicate, text=text, limit=limit
            )
            # Apply additional filters in-memory for backward compat
            if source is not None:
                results = [r for r in results if r.source == source]
            if min_confidence is not None:
                results = [r for r in results if r.confidence >= min_confidence]
            if max_confidence is not None:
                results = [r for r in results if r.confidence <= max_confidence]
            return results[:limit]

    async def update_fact(self, fact_id: str, **kwargs: Any) -> Optional[Fact]:
        async with await self._get_session() as session:
            repo = await self._get_fact_repo(session)
            result = await repo.update(fact_id, **kwargs)
            await session.commit()
            return result

    async def delete_fact(self, fact_id: str) -> bool:
        async with await self._get_session() as session:
            repo = await self._get_fact_repo(session)
            result = await repo.delete(fact_id)
            await session.commit()
            return result

    # --- Decision CRUD ---

    async def create_decision(self, decision: Decision) -> Decision:
        async with await self._get_session() as session:
            repo = await self._get_decision_repo(session)
            result = await repo.create(decision)
            await session.commit()
            return result

    async def get_decision(self, decision_id: str) -> Optional[Decision]:
        async with await self._get_session() as session:
            repo = await self._get_decision_repo(session)
            return await repo.get(decision_id)

    async def search_decisions(
        self,
        context: Optional[str] = None,
        choice: Optional[str] = None,
        reason: Optional[str] = None,
        source: Optional[str] = None,
        text: Optional[str] = None,
        limit: int = 50,
    ) -> list[Decision]:
        async with await self._get_session() as session:
            repo = await self._get_decision_repo(session)
            results = await repo.search(choice=choice, text=text, limit=limit)
            if source is not None:
                results = [r for r in results if r.source == source]
            return results[:limit]

    async def delete_decision(self, decision_id: str) -> bool:
        async with await self._get_session() as session:
            repo = await self._get_decision_repo(session)
            result = await repo.delete(decision_id)
            await session.commit()
            return result

    # --- Skill CRUD ---

    async def create_skill(self, skill: Skill) -> Skill:
        async with await self._get_session() as session:
            repo = await self._get_skill_repo(session)
            result = await repo.create(skill)
            await session.commit()
            return result

    async def get_skill(self, skill_id: str) -> Optional[Skill]:
        async with await self._get_session() as session:
            repo = await self._get_skill_repo(session)
            return await repo.get(skill_id)

    async def search_skills(
        self,
        purpose: Optional[str] = None,
        name: Optional[str] = None,
        text: Optional[str] = None,
        min_success_rate: Optional[float] = None,
        limit: int = 50,
    ) -> list[Skill]:
        async with await self._get_session() as session:
            repo = await self._get_skill_repo(session)
            results = await repo.search(purpose=purpose, limit=limit)
            if name is not None:
                results = [r for r in results if r.name == name]
            return results[:limit]

    async def delete_skill(self, skill_id: str) -> bool:
        async with await self._get_session() as session:
            repo = await self._get_skill_repo(session)
            result = await repo.delete(skill_id)
            await session.commit()
            return result

    # --- Receipt CRUD ---

    async def create_receipt(self, receipt: MemoryReceipt) -> MemoryReceipt:
        async with await self._get_session() as session:
            repo = await self._get_receipt_repo(session)
            result = await repo.create(receipt)
            await session.commit()
            return result

    async def get_receipt(self, receipt_id: str) -> Optional[MemoryReceipt]:
        async with await self._get_session() as session:
            repo = await self._get_receipt_repo(session)
            return await repo.get(receipt_id)

    async def search_receipts(
        self,
        source: Optional[str] = None,
        memory_type: Optional[str] = None,
        created_by: Optional[str] = None,
        limit: int = 50,
    ) -> list[MemoryReceipt]:
        async with await self._get_session() as session:
            repo = await self._get_receipt_repo(session)
            return await repo.search(memory_type=memory_type, source=source, limit=limit)

    # --- Belief CRUD ---

    async def create_belief(self, belief: Belief, evidence: list[Evidence] | None = None) -> Belief:
        """Create a new belief with optional evidence entries."""
        async with await self._get_session() as session:
            repo = await self._get_belief_repo(session)
            result = await repo.create(belief)
            if evidence:
                ev_repo = await self._get_evidence_repo(session)
                for ev in evidence:
                    ev.belief_id = belief.id
                    await ev_repo.create(ev)
            await session.commit()
            return result

    async def get_belief(self, belief_id: str) -> Optional[Belief]:
        """Get a belief by ID."""
        async with await self._get_session() as session:
            repo = await self._get_belief_repo(session)
            return await repo.get_by_id(belief_id)

    async def search_beliefs(
        self,
        proposition: Optional[str] = None,
        tags: Optional[list[str]] = None,
        lifecycle_state: Optional[str] = None,
        min_confidence: Optional[float] = None,
        source: Optional[str] = None,
        creator: Optional[str] = None,
        limit: int = 10,
    ) -> list[Belief]:
        """Search beliefs with various filters."""
        async with await self._get_session() as session:
            repo = await self._get_belief_repo(session)
            return await repo.search(
                proposition=proposition,
                tags=tags,
                lifecycle_state=lifecycle_state,
                min_confidence=min_confidence,
                source=source,
                creator=creator,
                limit=limit,
            )

    async def update_belief_confidence(
        self, belief_id: str, new_confidence: float
    ) -> Optional[Belief]:
        """Update the confidence of a belief."""
        async with await self._get_session() as session:
            repo = await self._get_belief_repo(session)
            result = await repo.update_confidence(belief_id, new_confidence)
            await session.commit()
            return result

    async def update_belief_lifecycle(self, belief_id: str, new_state: str) -> Optional[Belief]:
        """Update the lifecycle state of a belief."""
        async with await self._get_session() as session:
            repo = await self._get_belief_repo(session)
            result = await repo.update_lifecycle_state(belief_id, new_state)
            await session.commit()
            return result

    async def update_belief_reinforced_at(self, belief_id: str) -> Optional[Belief]:
        """Update the last_reinforced_at timestamp of a belief."""
        async with await self._get_session() as session:
            repo = await self._get_belief_repo(session)
            result = await repo.update_reinforced_at(belief_id)
            await session.commit()
            return result

    async def increment_belief_version(self, belief_id: str) -> Optional[Belief]:
        """Increment the version counter of a belief."""
        async with await self._get_session() as session:
            repo = await self._get_belief_repo(session)
            result = await repo.increment_version(belief_id)
            await session.commit()
            return result

    # --- Outbox ---

    async def add_outbox_entry(
        self,
        record_type: str,
        record_id: str,
        operation: str,
        payload: dict,
    ) -> None:
        """Add an outbox entry for async indexing.

        Creates the entry in its own transaction. For atomicity with
        the primary store write, use create_in_transaction() which
        shares a single session for all operations.

        Args:
            record_type: "fact", "decision", "skill", or "belief".
            record_id: ID of the record in the primary store.
            operation: "index_fact", "index_decision", "index_skill", or "index_belief".
            payload: Data needed for indexing.
        """
        async with await self._get_session() as session:
            repo = OutboxRepository(session)
            await repo.add_entry(
                record_type=record_type,
                record_id=record_id,
                operation=operation,
                payload=payload,
            )
            await session.commit()

    async def create_in_transaction(
        self,
        fact: Optional["Fact"] = None,
        receipt: Optional["MemoryReceipt"] = None,
        outbox_entries: Optional[list[dict]] = None,
        belief: Optional["Belief"] = None,
        evidence_list: Optional[list["Evidence"]] = None,
    ) -> None:
        """Create fact, belief, receipt, evidence, and outbox entries in a single transaction.

        Args:
            fact: Optional Fact to create.
            belief: Optional Belief to create.
            evidence_list: Optional list of Evidence entries (requires belief).
            receipt: Optional MemoryReceipt to create.
            outbox_entries: Optional list of outbox entry dicts, each with:
                record_type, record_id, operation, payload.
        """
        async with await self._get_session() as session:
            if fact is not None:
                fact_repo = await self._get_fact_repo(session)
                await fact_repo.create(fact)
            if belief is not None:
                belief_repo = await self._get_belief_repo(session)
                await belief_repo.create(belief)
                if evidence_list:
                    ev_repo = await self._get_evidence_repo(session)
                    for ev in evidence_list:
                        ev.belief_id = belief.id
                        await ev_repo.create(ev)
            if receipt is not None:
                receipt_repo = await self._get_receipt_repo(session)
                await receipt_repo.create(receipt)
            if outbox_entries:
                outbox_repo = OutboxRepository(session)
                for entry in outbox_entries:
                    await outbox_repo.add_entry(
                        record_type=entry["record_type"],
                        record_id=entry["record_id"],
                        operation=entry["operation"],
                        payload=entry["payload"],
                    )
            await session.commit()
