"""Outbox pattern — transactional outbox for async index propagation.

Stores pending indexing operations (Qdrant, graph) that the outbox
worker picks up and processes asynchronously.

Per ADR-011: writes to the primary store (SQL) and the outbox happen
in the same DB transaction. The outbox worker polls for pending entries
and pushes them to Qdrant + graph, retrying up to 3 times before marking
as failed.
"""

import json
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import uuid4

from sqlalchemy import DateTime, Integer, String, Text, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from storage.base import Base

# ──────────────────────────────────────────────────────────────────────
# ORM Model
# ──────────────────────────────────────────────────────────────────────


class OutboxEntryORM(Base):
    """Transactional outbox entry for async index propagation.

    Each entry represents one indexing operation that needs to be
    pushed to Qdrant (vector store) or graph (knowledge graph).

    Columns:
        id:            UUID primary key.
        record_type:   "fact", "decision", or "skill" — the kind of
                       record to index.
        record_id:     ID of the record in the primary store.
        operation:     "index_fact", "index_decision", or "index_skill".
        payload_json:  JSON blob with the data needed to perform the
                       indexing (subject/predicate/object for facts,
                       choice/reason for decisions, purpose/steps for
                       skills, etc.).
        status:        "pending", "processing", "completed", or "failed".
        retry_count:   How many times processing has been attempted.
        error:         Last error message (NULL when no error).
        created_at:    When the entry was created.
        processed_at:  When the entry was last processed (NULL initially).
    """

    __tablename__ = "outbox_entries"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    record_type: Mapped[str] = mapped_column(String, nullable=False, index=True)
    record_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    operation: Mapped[str] = mapped_column(String, nullable=False)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(
        String, nullable=False, default="pending", index=True
    )
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.now(timezone.utc)
    )
    processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)


# ──────────────────────────────────────────────────────────────────────
# Domain Model
# ──────────────────────────────────────────────────────────────────────


class OutboxEntry:
    """Domain model for an outbox entry (not persisted to DB directly)."""

    def __init__(
        self,
        id: str,
        record_type: str,
        record_id: str,
        operation: str,
        payload_json: str,
        status: str = "pending",
        retry_count: int = 0,
        error: Optional[str] = None,
        created_at: Optional[datetime] = None,
        processed_at: Optional[datetime] = None,
    ):
        self.id = id
        self.record_type = record_type
        self.record_id = record_id
        self.operation = operation
        self.payload_json = payload_json
        self.status = status
        self.retry_count = retry_count
        self.error = error
        self.created_at = created_at or datetime.now(timezone.utc)
        self.processed_at = processed_at

    @property
    def payload(self) -> dict[str, Any]:
        """Deserialize the JSON payload."""
        return json.loads(self.payload_json)

    @classmethod
    def from_orm(cls, orm: OutboxEntryORM) -> "OutboxEntry":
        return cls(
            id=orm.id,
            record_type=orm.record_type,
            record_id=orm.record_id,
            operation=orm.operation,
            payload_json=orm.payload_json,
            status=orm.status,
            retry_count=orm.retry_count,
            error=orm.error,
            created_at=orm.created_at,
            processed_at=orm.processed_at,
        )


# ──────────────────────────────────────────────────────────────────────
# Repository
# ──────────────────────────────────────────────────────────────────────


class OutboxRepository:
    """Repository for outbox entries.

    Each method takes an explicit AsyncSession so callers can control
    the transaction boundary. This lets the caller write to the primary
    store AND add the outbox entry in the same transaction.
    """

    MAX_RETRIES = 3

    def __init__(self, session: AsyncSession):
        self._session = session

    async def add_entry(
        self,
        record_type: str,
        record_id: str,
        operation: str,
        payload: dict[str, Any],
    ) -> OutboxEntry:
        """Create a new pending outbox entry.

        Does NOT commit — the caller controls the transaction commit
        so the outbox write shares a transaction with the primary store write.

        Args:
            record_type: "fact", "decision", or "skill".
            record_id: ID of the record in the primary store.
            operation: "index_fact", "index_decision", or "index_skill".
            payload: Data needed for indexing.

        Returns:
            The created OutboxEntry domain model.
        """
        entry_id = str(uuid4())
        orm = OutboxEntryORM(
            id=entry_id,
            record_type=record_type,
            record_id=record_id,
            operation=operation,
            payload_json=json.dumps(payload),
            status="pending",
            retry_count=0,
            error=None,
            created_at=datetime.now(timezone.utc),
            processed_at=None,
        )
        self._session.add(orm)
        # Note: caller must commit
        return OutboxEntry.from_orm(orm)

    async def get_pending(self, limit: int = 50) -> list[OutboxEntry]:
        """Get all pending entries, oldest first.

        Args:
            limit: Maximum entries to return.

        Returns:
            List of pending OutboxEntry domain models.
        """
        stmt = (
            select(OutboxEntryORM)
            .where(OutboxEntryORM.status == "pending")
            .order_by(OutboxEntryORM.created_at.asc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return [
            OutboxEntry.from_orm(row)
            for row in result.scalars().all()
        ]

    async def get_failed(self, limit: int = 50) -> list[OutboxEntry]:
        """Get all failed entries, oldest first.

        Args:
            limit: Maximum entries to return.

        Returns:
            List of failed OutboxEntry domain models.
        """
        stmt = (
            select(OutboxEntryORM)
            .where(OutboxEntryORM.status == "failed")
            .order_by(OutboxEntryORM.processed_at.asc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return [
            OutboxEntry.from_orm(row)
            for row in result.scalars().all()
        ]

    async def mark_processing(self, entry_id: str) -> bool:
        """Mark an entry as being processed.

        Returns True if the entry existed, False otherwise.
        """
        orm = await self._session.get(OutboxEntryORM, entry_id)
        if orm is None:
            return False
        orm.status = "processing"
        # Note: caller must commit
        return True

    async def mark_completed(self, entry_id: str) -> bool:
        """Mark an entry as completed.

        Returns True if the entry existed, False otherwise.
        """
        orm = await self._session.get(OutboxEntryORM, entry_id)
        if orm is None:
            return False
        orm.status = "completed"
        orm.processed_at = datetime.now(timezone.utc)
        # Note: caller must commit
        return True

    async def mark_failed(self, entry_id: str, error_message: str) -> bool:
        """Mark an entry as failed after exhausting retries.

        Args:
            entry_id: The outbox entry ID.
            error_message: The error description.

        Returns:
            True if the entry existed, False otherwise.
        """
        orm = await self._session.get(OutboxEntryORM, entry_id)
        if orm is None:
            return False
        orm.status = "failed"
        orm.error = error_message[:500]  # Truncate to column capacity
        orm.processed_at = datetime.now(timezone.utc)
        # Note: caller must commit
        return True

    async def increment_retry(self, entry_id: str, error_message: str) -> int:
        """Increment retry count and update error message.

        Returns the new retry count.
        """
        orm = await self._session.get(OutboxEntryORM, entry_id)
        if orm is None:
            return -1
        orm.retry_count = (orm.retry_count or 0) + 1
        orm.error = error_message[:500]
        orm.status = "pending"  # Reset to pending for retry
        # Note: caller must commit
        return orm.retry_count

    async def get_pending_count(self) -> int:
        """Get count of pending entries."""
        stmt = (
            select(OutboxEntryORM)
            .where(OutboxEntryORM.status == "pending")
        )
        result = await self._session.execute(stmt)
        return len(result.scalars().all())

    async def get_failed_count(self) -> int:
        """Get count of failed entries."""
        stmt = (
            select(OutboxEntryORM)
            .where(OutboxEntryORM.status == "failed")
        )
        result = await self._session.execute(stmt)
        return len(result.scalars().all())
