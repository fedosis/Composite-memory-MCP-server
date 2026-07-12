"""Legacy adapter — wraps new repository layer in old SQLiteProvider interface.

This adapter implements the same async CRUD methods as the original
SQLiteProvider but delegates to the new repository layer internally.

Keeps backward compatibility during v0.6 migration.
"""

from typing import Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from memory_server.models import Decision, Fact, MemoryReceipt, Skill
from storage.base import Base
from storage.repositories import (
    DecisionRepository,
    FactRepository,
    ReceiptRepository,
    SkillRepository,
)


class LegacySQLiteProviderAdapter:
    """Backward-compatible adapter that wraps new repository layer.

    Provides the same interface as the old SQLiteProvider but uses
    the new storage architecture internally.
    """

    def __init__(self, url: str = "sqlite+aiosqlite:///memory.db"):
        self._url = url
        self._engine = None
        self._session_factory = None
        self._fact_repo = None
        self._decision_repo = None
        self._skill_repo = None
        self._receipt_repo = None
        self._lifecycle_repo = None

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
