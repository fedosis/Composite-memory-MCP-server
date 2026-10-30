"""Outbox pattern — transactional outbox for async index propagation.

Stores pending indexing operations (Qdrant, graph) that the outbox
worker picks up and processes asynchronously.

Per ADR-011: writes to the primary store (SQL) and the outbox happen
in the same DB transaction. The outbox worker polls for pending entries
and pushes them to Qdrant + graph, retrying up to 3 times before marking
as failed.
"""

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Literal, Optional
from uuid import uuid4

from sqlalchemy import DateTime, Integer, String, Text, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from storage.base import Base, utcnow

if TYPE_CHECKING:
    from memory_server.providers.sqlite_provider import DiagnosticsReadWork


LastErrorCode = Literal[
    "E_TIMEOUT",
    "E_STORAGE",
    "E_VECTOR_BACKEND",
    "E_GRAPH_BACKEND",
    "E_PROJECTION_UNAVAILABLE",
    "E_CAPABILITY_UNAVAILABLE",
]
_DIAGNOSTIC_ERROR_CODES = frozenset(
    {
        "E_TIMEOUT",
        "E_STORAGE",
        "E_VECTOR_BACKEND",
        "E_GRAPH_BACKEND",
        "E_PROJECTION_UNAVAILABLE",
        "E_CAPABILITY_UNAVAILABLE",
    }
)


@dataclass(frozen=True, order=True)
class OutboxKey:
    created_at: str
    id: str


@dataclass(frozen=True)
class OutboxDiagnosticRow:
    id: str
    record_id: str
    record_type: str
    operation: str
    status: str
    retry_count: int
    error: str | None
    created_at: datetime
    processed_at: datetime | None


@dataclass(frozen=True)
class OutboxDiagnosticPage:
    rows: tuple[OutboxDiagnosticRow, ...]
    upper: OutboxKey | None


def classify_diagnostic_error(error: object) -> LastErrorCode | None:
    return error if type(error) is str and error in _DIAGNOSTIC_ERROR_CODES else None


def _wire_time(value: datetime) -> str:
    aware = value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
    return aware.isoformat(timespec="microseconds")


def _storage_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("cursor timestamp must be aware")
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)

# ──────────────────────────────────────────────────────────────────────
# ORM Model
# ──────────────────────────────────────────────────────────────────────


class OutboxEntryORM(Base):
    """Transactional outbox entry for async index propagation.

    Each entry represents one indexing operation that needs to be
    pushed to Qdrant (vector store) or graph (knowledge graph).

    Columns:
        id:            UUID primary key.
        record_type:   "fact", "decision", "skill", or "belief" — the kind of
                       record to index.
        record_id:     ID of the record in the primary store.
        operation:     "index_fact", "index_decision", "index_skill", or "index_belief".
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
        DateTime, default=utcnow
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
        self.created_at = created_at or utcnow()
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
    # Test/instrumentation seam: called after candidate selection and before
    # the conditional UPDATE. Production leaves it unset.
    claim_between_select_and_update: Callable[[list[str]], Awaitable[None]] | None = None

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
            record_type: "fact", "decision", "skill", or "belief".
            record_id: ID of the record in the primary store.
            operation: "index_fact", "index_decision", "index_skill", or "index_belief".
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
            created_at=utcnow(),
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

    async def claim_pending(self, limit: int = 50) -> list[OutboxEntry]:
        """Atomically claim the oldest pending entries.

        Only rows that were still pending at UPDATE time are returned.
        """
        pending_ids = (
            select(OutboxEntryORM.id)
            .where(OutboxEntryORM.status == "pending")
            .order_by(OutboxEntryORM.created_at.asc(), OutboxEntryORM.id.asc())
            .limit(limit)
            .cte("pending_ids")
        )
        candidate_result = await self._session.execute(pending_ids.select())
        candidate_ids = [str(row[0]) for row in candidate_result.fetchall()]
        if not candidate_ids:
            return []
        hook = type(self).claim_between_select_and_update
        if hook is not None:
            await hook(candidate_ids)
        stmt = (
            update(OutboxEntryORM)
            .where(
                OutboxEntryORM.id.in_(candidate_ids),
                OutboxEntryORM.status == "pending",
            )
            .values(status="processing", processed_at=utcnow())
            .returning(OutboxEntryORM)
        )
        result = await self._session.execute(stmt)
        return [OutboxEntry.from_orm(row) for row in result.scalars().all()]

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
        stmt = (
            update(OutboxEntryORM)
            .where(OutboxEntryORM.id == entry_id, OutboxEntryORM.status == "pending")
            .values(status="processing", processed_at=utcnow())
            .returning(OutboxEntryORM.id)
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none() is not None

    async def mark_completed(self, entry_id: str) -> bool:
        """Mark an entry as completed.

        Returns True if the entry existed, False otherwise.
        """
        orm = await self._session.get(OutboxEntryORM, entry_id)
        if orm is None:
            return False
        orm.status = "completed"
        orm.processed_at = utcnow()
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
        orm.processed_at = utcnow()
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

    @staticmethod
    def _diagnostic_require(work: "DiagnosticsReadWork", session: AsyncSession) -> None:
        if session.info.get("s10_diagnostics_work") is not work:
            raise ValueError("outbox diagnostics require the active borrowed window")
        work.check()

    async def diagnostic_count(
        self,
        *,
        status: Literal["pending", "processing", "completed", "failed"],
        work: "DiagnosticsReadWork",
    ) -> int:
        self._diagnostic_require(work, self._session)
        stored = "completed" if status == "applied" else status
        if stored not in {"pending", "processing", "completed", "failed"}:
            raise ValueError("invalid outbox status")
        result = await self._session.execute(
            text("SELECT COUNT(*) FROM outbox_entries WHERE status=:status"), {"status": stored}
        )
        work.check()
        return int(result.scalar_one())

    async def diagnostic_status_counts(self, *, work: "DiagnosticsReadWork") -> dict[str, int]:
        self._diagnostic_require(work, self._session)
        result = await self._session.execute(
            text(
                "SELECT CASE WHEN status IN ('pending','processing','completed','failed') "
                "THEN status ELSE 'unknown' END AS state,COUNT(*) AS count "
                "FROM outbox_entries GROUP BY state"
            )
        )
        work.check()
        counts = {key: 0 for key in ("pending", "processing", "completed", "failed")}
        for row in result:
            counts[str(row.state)] = int(row.count)
        return counts

    async def diagnostic_page(
        self,
        *,
        limit: int,
        last: OutboxKey | None,
        upper: OutboxKey | None,
        status: str | None,
        record_id: str | None,
        work: "DiagnosticsReadWork",
    ) -> OutboxDiagnosticPage:
        self._diagnostic_require(work, self._session)
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ValueError("invalid outbox page limit")
        stored = "completed" if status == "applied" else status
        if stored is not None and stored not in {"pending", "processing", "completed", "failed"}:
            raise ValueError("invalid outbox status")
        filters, params = [], {}
        if stored is not None:
            filters.append("status=:status")
            params["status"] = stored
        if record_id is not None:
            filters.append("record_id=:record_id")
            params["record_id"] = record_id
        predicate = " AND ".join(filters) if filters else "1=1"
        if upper is None:
            result = await self._session.execute(
                text(
                    f"SELECT created_at,id FROM outbox_entries WHERE {predicate} "
                    "ORDER BY created_at DESC,id DESC LIMIT 1"
                ),
                params,
            )
            row = result.first()
            if row is None:
                work.check()
                return OutboxDiagnosticPage((), None)
            upper = OutboxKey(_wire_time(row.created_at), str(row.id))
        page_filter = predicate + " AND (created_at,id)<=(:upper_date,:upper_id)"
        page_params = {**params, "upper_date": _storage_time(upper.created_at), "upper_id": upper.id}
        if last is not None:
            page_filter += " AND (created_at,id)>(:last_date,:last_id)"
            page_params.update(last_date=_storage_time(last.created_at), last_id=last.id)
        page_params["limit"] = limit + 1
        result = await self._session.execute(
            text(
                "SELECT id,record_id,record_type,operation,status,retry_count,error,created_at,processed_at "
                f"FROM outbox_entries WHERE {page_filter} ORDER BY created_at,id LIMIT :limit"
            ),
            page_params,
        )
        rows = tuple(OutboxDiagnosticRow(**dict(row)) for row in result.mappings())
        work.check()
        return OutboxDiagnosticPage(rows, upper)

    async def reset_stale_processing(self, max_age_seconds: int = 600) -> int:
        """Reset entries stuck in ``processing`` back to ``pending``.

        A worker crash between claim (mark_processing) and completion would
        otherwise leave entries in ``processing`` forever; they would never
        be re-picked by get_pending(). Entries whose processing started
        longer than ``max_age_seconds`` ago are considered stale and reset.

        Args:
            max_age_seconds: Age threshold for a stale processing entry.

        Returns:
            Number of entries reset.
        """
        cutoff = utcnow() - timedelta(seconds=max_age_seconds)
        stmt = (
            update(OutboxEntryORM)
            .where(
                OutboxEntryORM.status == "processing",
                OutboxEntryORM.processed_at < cutoff,
            )
            .values(status="pending", processed_at=None)
        )
        result = await self._session.execute(stmt)
        return getattr(result, "rowcount", 0) or 0
