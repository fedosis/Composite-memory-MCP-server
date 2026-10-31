"""Durable mutation snapshots in the profile's canonical database."""

from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from storage.base import Base


class HttpRequestJournalORM(Base):
    """Final rows share the canonical transaction; timestamps are naive UTC."""

    __tablename__ = "http_request_journal"
    __table_args__ = (
        UniqueConstraint("subject", "profile_id", "operation", "idem_key", name="uq_http_request_journal_scope"),
        Index("ix_http_request_journal_expiry", "profile_id", "expires_at"),
        CheckConstraint("state IN ('final')", name="ck_http_request_journal_state"),
        CheckConstraint("pending_publication IN (0,1)", name="ck_http_request_journal_pending"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    subject: Mapped[str] = mapped_column(String, nullable=False)
    profile_id: Mapped[str] = mapped_column(String, nullable=False)
    operation: Mapped[str] = mapped_column(String, nullable=False)
    idem_key: Mapped[str] = mapped_column(String, nullable=False)
    digest: Mapped[str] = mapped_column(String(64), nullable=False)
    state: Mapped[str] = mapped_column(String, nullable=False, default="final")
    response_snapshot: Mapped[str] = mapped_column(Text, nullable=False)
    result_refs: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    request_id: Mapped[str] = mapped_column(String, nullable=False)
    audit_ticket_id: Mapped[str | None] = mapped_column(String, nullable=True)
    pending_publication: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
