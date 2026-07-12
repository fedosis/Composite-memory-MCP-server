"""Belief and Evidence ORM models — canonical SQL storage for beliefs."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.orm import Mapped, mapped_column

from memory_server.models.belief import Belief
from memory_server.models.evidence import Evidence
from storage.base import Base, utcnow


class BeliefORM(Base):
    """SQLAlchemy ORM model for Beliefs."""

    __tablename__ = "beliefs"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    proposition: Mapped[str] = mapped_column(String(2048), nullable=False, index=True)
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.5)
    source: Mapped[str] = mapped_column(String(128), nullable=False, default="system")
    creator: Mapped[str] = mapped_column(String(128), nullable=False, default="system")
    source_ids: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    tags: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)
    last_reinforced_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    verification_status: Mapped[str] = mapped_column(
        String(32), nullable=False, default="candidate"
    )
    lifecycle_state: Mapped[str] = mapped_column(
        String(32), nullable=False, default="active", index=True
    )

    def to_pydantic(self) -> Belief:
        return Belief(
            id=self.id,
            proposition=self.proposition,
            confidence=self.confidence,
            source=self.source,
            creator=self.creator,
            source_ids=list(self.source_ids) if self.source_ids else [],
            tags=list(self.tags) if self.tags else [],
            created_at=self.created_at,
            updated_at=self.updated_at,
            last_reinforced_at=self.last_reinforced_at,
            version=self.version,
            verification_status=self.verification_status,
            lifecycle_state=self.lifecycle_state,
        )

    @classmethod
    def from_pydantic(cls, belief: Belief) -> "BeliefORM":
        return cls(
            id=belief.id,
            proposition=belief.proposition,
            confidence=belief.confidence,
            source=belief.source,
            creator=belief.creator,
            source_ids=belief.source_ids,
            tags=belief.tags,
            created_at=belief.created_at,
            updated_at=belief.updated_at,
            last_reinforced_at=belief.last_reinforced_at,
            version=belief.version,
            verification_status=belief.verification_status,
            lifecycle_state=belief.lifecycle_state,
        )


class EvidenceORM(Base):
    """SQLAlchemy ORM model for Evidence entries."""

    __tablename__ = "evidence"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    belief_id: Mapped[str] = mapped_column(
        String, ForeignKey("beliefs.id"), nullable=False, index=True
    )
    source_type: Mapped[str] = mapped_column(String(32), nullable=False)
    source_id: Mapped[str] = mapped_column(String, nullable=False)
    weight: Mapped[float] = mapped_column(Float, nullable=False, default=0.5)
    contributor: Mapped[str] = mapped_column(String(128), nullable=False, default="system")
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)

    def to_pydantic(self) -> Evidence:
        return Evidence(
            id=self.id,
            belief_id=self.belief_id,
            source_type=self.source_type,
            source_id=self.source_id,
            weight=self.weight,
            contributor=self.contributor,
            created_at=self.created_at,
            note=self.note,
        )

    @classmethod
    def from_pydantic(cls, evidence: Evidence) -> "EvidenceORM":
        return cls(
            id=evidence.id,
            belief_id=evidence.belief_id,
            source_type=evidence.source_type,
            source_id=evidence.source_id,
            weight=evidence.weight,
            contributor=evidence.contributor,
            created_at=evidence.created_at,
            note=evidence.note,
        )
