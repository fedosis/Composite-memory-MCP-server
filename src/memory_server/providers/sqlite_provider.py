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
    DecisionRepository,
    FactRepository,
    ReceiptRepository,
    SkillRepository,
)

from memory_server.models import (
    Decision,
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

    # --- Fact CRUD ---

    async def create_fact(self, fact: Fact) -> Fact:
        async with await self._get_session() as session:
            repo = await self._get_fact_repo(session)
            return await repo.create(fact)

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
            return await repo.update(fact_id, **kwargs)

    async def delete_fact(self, fact_id: str) -> bool:
        async with await self._get_session() as session:
            repo = await self._get_fact_repo(session)
            return await repo.delete(fact_id)

    # --- Decision CRUD ---

    async def create_decision(self, decision: Decision) -> Decision:
        async with await self._get_session() as session:
            repo = await self._get_decision_repo(session)
            return await repo.create(decision)

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
            return await repo.delete(decision_id)

    # --- Skill CRUD ---

    async def create_skill(self, skill: Skill) -> Skill:
        async with await self._get_session() as session:
            repo = await self._get_skill_repo(session)
            return await repo.create(skill)

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
            return await repo.delete(skill_id)

    # --- Receipt CRUD ---

    async def create_receipt(self, receipt: MemoryReceipt) -> MemoryReceipt:
        async with await self._get_session() as session:
            repo = await self._get_receipt_repo(session)
            return await repo.create(receipt)

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
            record_type: "fact", "decision", or "skill".
            record_id: ID of the record in the primary store.
            operation: "index_fact", "index_decision", or "index_skill".
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
    ) -> None:
        """Create fact, receipt, and outbox entries in a single transaction.

        Args:
            fact: Optional Fact to create.
            receipt: Optional MemoryReceipt to create.
            outbox_entries: Optional list of outbox entry dicts, each with:
                record_type, record_id, operation, payload.
        """
        async with await self._get_session() as session:
            if fact is not None:
                fact_repo = await self._get_fact_repo(session)
                await fact_repo.create(fact)
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
