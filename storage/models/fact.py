"""Fact ORM model — canonical SQL storage for facts."""

from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Float, Index, String, text
from sqlalchemy.orm import Mapped, mapped_column

from memory_server.models import Fact
from storage.base import Base, utcnow
from storage.dedup import fact_dedup_key


class FactORM(Base):
    """SQLAlchemy ORM model for Facts — canonical fields."""

    __tablename__ = "facts"
    __table_args__ = (
        Index(
            "uq_facts_spo_active",
            "dedup_key",
            unique=True,
            sqlite_where=text("lifecycle_state IN ('candidate', 'validated', 'active')"),
        ),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True)
    subject: Mapped[str] = mapped_column(String, nullable=False, index=True)
    predicate: Mapped[str] = mapped_column(String, nullable=False, index=True)
    object: Mapped[str] = mapped_column(String, nullable=False)
    dedup_key: Mapped[str] = mapped_column(String, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, default=1.0)
    source: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    creator: Mapped[str] = mapped_column(String, default="system")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    verification_status: Mapped[str] = mapped_column(String, default="candidate")
    lifecycle_state: Mapped[str] = mapped_column(String, default="active")
    version: Mapped[str] = mapped_column(String, default="0.1.0")

    def to_pydantic(self) -> Fact:
        return Fact(
            id=self.id,
            subject=self.subject,
            predicate=self.predicate,
            object=self.object,
            dedup_key=self.dedup_key,
            confidence=self.confidence,
            source=self.source,
            creator=self.creator,
            created_at=self.created_at,
            updated_at=self.updated_at,
            verification_status=self.verification_status,
            lifecycle_state=self.lifecycle_state,
            version=self.version,
        )

    @classmethod
    def from_pydantic(cls, fact: Fact) -> "FactORM":
        dedup_key = fact.dedup_key
        if dedup_key is None:
            dedup_key = fact_dedup_key(fact.subject, fact.predicate, fact.object)
        return cls(
            id=fact.id,
            subject=fact.subject,
            predicate=fact.predicate,
            object=fact.object,
            dedup_key=dedup_key,
            confidence=fact.confidence,
            source=fact.source,
            creator=fact.creator,
            created_at=fact.created_at,
            updated_at=fact.updated_at,
            verification_status=fact.verification_status,
            lifecycle_state=fact.lifecycle_state,
            version=str(fact.version),
        )
