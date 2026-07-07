"""Async SQLite provider using SQLAlchemy + aiosqlite.

Per ADR-002: Facts use SQLite/PostgreSQL storage.
Per ADR-010: v0.1a uses SQLite only.
"""

from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import Float, String, Text, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from memory_server.models import (
    Decision,
    Fact,
    MemoryReceipt,
    Skill,
    VerificationStatus,
)


class Base(DeclarativeBase):
    pass


class FactORM(Base):
    """SQLAlchemy ORM model for Facts."""

    __tablename__ = "facts"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    subject: Mapped[str] = mapped_column(String, nullable=False, index=True)
    predicate: Mapped[str] = mapped_column(String, nullable=False, index=True)
    object: Mapped[str] = mapped_column(String, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, default=1.0)
    source: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(String, default=datetime.now(timezone.utc))

    def to_pydantic(self) -> Fact:
        return Fact(
            id=self.id,
            subject=self.subject,
            predicate=self.predicate,
            object=self.object,
            confidence=self.confidence,
            source=self.source,
            created_at=self.created_at,
        )

    @classmethod
    def from_pydantic(cls, fact: Fact) -> "FactORM":
        return cls(
            id=fact.id,
            subject=fact.subject,
            predicate=fact.predicate,
            object=fact.object,
            confidence=fact.confidence,
            source=fact.source,
            created_at=fact.created_at,
        )


class MemoryReceiptORM(Base):
    """SQLAlchemy ORM model for MemoryReceipts."""

    __tablename__ = "receipts"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    memory_type: Mapped[str] = mapped_column(String, nullable=False, index=True)
    source: Mapped[str] = mapped_column(String, nullable=False, index=True)
    created_by: Mapped[str] = mapped_column(String, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(String, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, default=1.0)
    verification_status: Mapped[str] = mapped_column(String, default="unverified")
    history: Mapped[str] = mapped_column(Text, default="[]")

    def to_pydantic(self) -> MemoryReceipt:
        import json

        return MemoryReceipt(
            id=self.id,
            memory_type=self.memory_type,
            source=self.source,
            created_by=self.created_by,
            timestamp=self.timestamp,
            confidence=self.confidence,
            verification_status=VerificationStatus(self.verification_status),
            history=json.loads(self.history),
        )

    @classmethod
    def from_pydantic(cls, receipt: MemoryReceipt) -> "MemoryReceiptORM":
        import json

        return cls(
            id=receipt.id,
            memory_type=receipt.memory_type,
            source=receipt.source,
            created_by=receipt.created_by,
            timestamp=receipt.timestamp,
            confidence=receipt.confidence,
            verification_status=receipt.verification_status.value,
            history=json.dumps(receipt.history),
        )


class DecisionORM(Base):
    """SQLAlchemy ORM model for Decisions."""

    __tablename__ = "decisions"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    context: Mapped[str] = mapped_column(String, default="")
    choice: Mapped[str] = mapped_column(String, nullable=False)
    rejected_alternatives: Mapped[str] = mapped_column(Text, default="[]")
    reason: Mapped[str] = mapped_column(String, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, default=1.0)
    source: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(String, default=datetime.now(timezone.utc))

    def to_pydantic(self) -> Decision:
        import json

        return Decision(
            id=self.id,
            context=self.context,
            choice=self.choice,
            rejected_alternatives=json.loads(self.rejected_alternatives),
            reason=self.reason,
            source=self.source,
            created_at=self.created_at,
        )

    @classmethod
    def from_pydantic(cls, decision: Decision) -> "DecisionORM":
        import json

        return cls(
            id=decision.id,
            context=decision.context,
            choice=decision.choice,
            rejected_alternatives=json.dumps(decision.rejected_alternatives),
            reason=decision.reason,
            source=decision.source,
            created_at=decision.created_at,
        )


class SkillORM(Base):
    """SQLAlchemy ORM model for Skills."""

    __tablename__ = "skills"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String, default="")
    version: Mapped[str] = mapped_column(String, default="1.0.0")
    purpose: Mapped[str] = mapped_column(String, nullable=False)
    steps: Mapped[str] = mapped_column(Text, default="[]")
    constraints: Mapped[str] = mapped_column(Text, default="[]")
    validation: Mapped[str] = mapped_column(Text, default="[]")
    success_rate: Mapped[float] = mapped_column(Float, default=0.0)
    created_at: Mapped[datetime] = mapped_column(String, default=datetime.now(timezone.utc))

    def to_pydantic(self) -> Skill:
        import json

        return Skill(
            id=self.id,
            name=self.name,
            version=self.version,
            purpose=self.purpose,
            steps=json.loads(self.steps),
            constraints=json.loads(self.constraints),
            validation=json.loads(self.validation),
            success_rate=self.success_rate,
            created_at=self.created_at,
        )

    @classmethod
    def from_pydantic(cls, skill: Skill) -> "SkillORM":
        import json

        return cls(
            id=skill.id,
            name=skill.name,
            version=skill.version,
            purpose=skill.purpose,
            steps=json.dumps(skill.steps),
            constraints=json.dumps(skill.constraints),
            validation=json.dumps(skill.validation),
            success_rate=skill.success_rate,
            created_at=skill.created_at,
        )


class SQLiteProvider:
    """Async SQLite provider for CRUD operations on facts and receipts."""

    def __init__(self, url: str = "sqlite+aiosqlite:///memory.db"):
        self._url = url
        self._engine = None
        self._session_factory = None

    async def initialize(self):
        """Create engine, tables, and session factory."""
        self._engine = create_async_engine(self._url, echo=False)
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

    # --- Fact CRUD ---

    async def create_fact(self, fact: Fact) -> Fact:
        async with await self._get_session() as session:
            orm = FactORM.from_pydantic(fact)
            session.add(orm)
            await session.commit()
            return orm.to_pydantic()

    async def get_fact(self, fact_id: str) -> Optional[Fact]:
        async with await self._get_session() as session:
            result = await session.get(FactORM, fact_id)
            if result is None:
                return None
            return result.to_pydantic()

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
            stmt = select(FactORM)

            if subject is not None:
                stmt = stmt.where(FactORM.subject == subject)
            if predicate is not None:
                stmt = stmt.where(FactORM.predicate == predicate)
            if object is not None:
                stmt = stmt.where(FactORM.object == object)
            if source is not None:
                stmt = stmt.where(FactORM.source == source)
            if text is not None:
                pattern = f"%{text}%"
                stmt = stmt.where(
                    FactORM.subject.like(pattern)
                    | FactORM.predicate.like(pattern)
                    | FactORM.object.like(pattern)
                )
            if min_confidence is not None:
                stmt = stmt.where(FactORM.confidence >= min_confidence)
            if max_confidence is not None:
                stmt = stmt.where(FactORM.confidence <= max_confidence)

            stmt = stmt.limit(limit).order_by(FactORM.created_at.desc())
            result = await session.execute(stmt)
            return [row.to_pydantic() for row in result.scalars().all()]

    async def update_fact(self, fact_id: str, **kwargs: Any) -> Optional[Fact]:
        async with await self._get_session() as session:
            orm = await session.get(FactORM, fact_id)
            if orm is None:
                return None

            for key, value in kwargs.items():
                if hasattr(orm, key):
                    setattr(orm, key, value)

            await session.commit()
            return orm.to_pydantic()

    async def delete_fact(self, fact_id: str) -> bool:
        async with await self._get_session() as session:
            orm = await session.get(FactORM, fact_id)
            if orm is None:
                return False
            await session.delete(orm)
            await session.commit()
            return True

    # --- Decision CRUD ---

    async def create_decision(self, decision: Decision) -> Decision:
        async with await self._get_session() as session:
            orm = DecisionORM.from_pydantic(decision)
            session.add(orm)
            await session.commit()
            return orm.to_pydantic()

    async def get_decision(self, decision_id: str) -> Optional[Decision]:
        async with await self._get_session() as session:
            result = await session.get(DecisionORM, decision_id)
            if result is None:
                return None
            return result.to_pydantic()

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
            stmt = select(DecisionORM)

            if context is not None:
                stmt = stmt.where(DecisionORM.context == context)
            if choice is not None:
                stmt = stmt.where(DecisionORM.choice == choice)
            if reason is not None:
                stmt = stmt.where(DecisionORM.reason == reason)
            if source is not None:
                stmt = stmt.where(DecisionORM.source == source)
            if text is not None:
                pattern = f"%{text}%"
                stmt = stmt.where(
                    DecisionORM.context.like(pattern)
                    | DecisionORM.choice.like(pattern)
                    | DecisionORM.reason.like(pattern)
                )

            stmt = stmt.limit(limit).order_by(DecisionORM.created_at.desc())
            result = await session.execute(stmt)
            return [row.to_pydantic() for row in result.scalars().all()]

    # --- Skill CRUD ---

    async def create_skill(self, skill: Skill) -> Skill:
        async with await self._get_session() as session:
            orm = SkillORM.from_pydantic(skill)
            session.add(orm)
            await session.commit()
            return orm.to_pydantic()

    async def get_skill(self, skill_id: str) -> Optional[Skill]:
        async with await self._get_session() as session:
            result = await session.get(SkillORM, skill_id)
            if result is None:
                return None
            return result.to_pydantic()

    async def search_skills(
        self,
        purpose: Optional[str] = None,
        name: Optional[str] = None,
        text: Optional[str] = None,
        min_success_rate: Optional[float] = None,
        limit: int = 50,
    ) -> list[Skill]:
        async with await self._get_session() as session:
            stmt = select(SkillORM)

            if purpose is not None:
                stmt = stmt.where(SkillORM.purpose == purpose)
            if name is not None:
                stmt = stmt.where(SkillORM.name == name)
            if text is not None:
                pattern = f"%{text}%"
                stmt = stmt.where(
                    SkillORM.purpose.like(pattern)
                    | SkillORM.name.like(pattern)
                )
            if min_success_rate is not None:
                stmt = stmt.where(SkillORM.success_rate >= min_success_rate)

            stmt = stmt.limit(limit).order_by(SkillORM.created_at.desc())
            result = await session.execute(stmt)
            return [row.to_pydantic() for row in result.scalars().all()]

    # --- Receipt CRUD ---

    async def create_receipt(self, receipt: MemoryReceipt) -> MemoryReceipt:
        async with await self._get_session() as session:
            orm = MemoryReceiptORM.from_pydantic(receipt)
            session.add(orm)
            await session.commit()
            return orm.to_pydantic()

    async def get_receipt(self, receipt_id: str) -> Optional[MemoryReceipt]:
        async with await self._get_session() as session:
            result = await session.get(MemoryReceiptORM, receipt_id)
            if result is None:
                return None
            return result.to_pydantic()

    async def search_receipts(
        self,
        source: Optional[str] = None,
        memory_type: Optional[str] = None,
        created_by: Optional[str] = None,
        limit: int = 50,
    ) -> list[MemoryReceipt]:
        async with await self._get_session() as session:
            stmt = select(MemoryReceiptORM)

            if source is not None:
                stmt = stmt.where(MemoryReceiptORM.source == source)
            if memory_type is not None:
                stmt = stmt.where(MemoryReceiptORM.memory_type == memory_type)
            if created_by is not None:
                stmt = stmt.where(MemoryReceiptORM.created_by == created_by)

            stmt = stmt.limit(limit).order_by(MemoryReceiptORM.timestamp.desc())
            result = await session.execute(stmt)
            return [row.to_pydantic() for row in result.scalars().all()]
