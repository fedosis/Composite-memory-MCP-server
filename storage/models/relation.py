"""Claim relation ORM model — canonical SQL storage for inter-claim relations."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

from storage.base import Base, utcnow


class ClaimRelationORM(Base):
    """SQLAlchemy ORM model for canonical claim relations."""

    __tablename__ = "claim_relations"

    source_id: Mapped[str] = mapped_column(String, primary_key=True)
    target_id: Mapped[str] = mapped_column(String, primary_key=True, index=True)
    relation_type: Mapped[str] = mapped_column(String, primary_key=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)
