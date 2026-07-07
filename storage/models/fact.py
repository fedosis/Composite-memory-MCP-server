"""Fact ORM model — canonical SQL storage for facts."""

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import Float, String
from sqlalchemy.orm import Mapped, mapped_column

from memory_server.models import Fact
from storage.base import Base


class FactORM(Base):
    """SQLAlchemy ORM model for Facts — canonical fields."""

    __tablename__ = "facts"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    subject: Mapped[str] = mapped_column(String, nullable=False, index=True)
    predicate: Mapped[str] = mapped_column(String, nullable=False, index=True)
    object: Mapped[str] = mapped_column(String, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, default=1.0)
    source: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    creator: Mapped[str] = mapped_column(String, default="system")
    created_at: Mapped[datetime] = mapped_column(String, default=datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(String, default=datetime.now(timezone.utc))
    verification_status: Mapped[str] = mapped_column(String, default="candidate")
    lifecycle_state: Mapped[str] = mapped_column(String, default="active")
    version: Mapped[str] = mapped_column(String, default="0.1.0")

    def to_pydantic(self) -> Fact:
        return Fact(
            id=self.id,
            subject=self.subject,
            predicate=self.predicate,
            object=self.object,
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
        return cls(
            id=fact.id,
            subject=fact.subject,
            predicate=fact.predicate,
            object=fact.object,
            confidence=fact.confidence,
            source=fact.source,
            creator=fact.creator,
            created_at=fact.created_at,
            updated_at=fact.updated_at,
            verification_status=fact.verification_status,
            lifecycle_state=fact.lifecycle_state,
            version=fact.version,
        )
