"""Claim relation ORM model — canonical SQL storage for inter-claim relations."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from storage.base import Base, utcnow


class ClaimRelationORM(Base):
    """SQLAlchemy ORM model for canonical claim relations."""

    __tablename__ = "claim_relations"
    __table_args__ = (
        Index("s09_order", "created_at", "source_id", "target_id", "relation_type"),
        Index("s09_source_order", "source_id", "created_at", "target_id", "relation_type"),
        Index("s09_target_order", "target_id", "created_at", "source_id", "relation_type"),
    )

    source_id: Mapped[str] = mapped_column(String, primary_key=True)
    target_id: Mapped[str] = mapped_column(String, primary_key=True, index=True)
    relation_type: Mapped[str] = mapped_column(String, primary_key=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)
