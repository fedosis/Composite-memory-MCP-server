"""Decision ORM model — canonical SQL storage for decisions."""

from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Float, Index, String, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from memory_server.models import Decision
from storage.base import Base, utcnow
from storage.dedup import canonical_context, normalize_choice


class DecisionORM(Base):
    """SQLAlchemy ORM model for Decisions — canonical fields.

    Uniqueness (B1 + W3): a *partial* unique index on
    ``(context, dedup_key)`` restricted to ACTIVE lifecycle states guarantees
    one row per normalized decision while it is live. Two concurrent
    ``learn()`` calls that both pass ``find_existing()`` can no longer both
    commit — the race loser hits the index and is treated as a duplicate by
    the ingestion service. The index is partial so a rejected/archived row
    does NOT block re-ingestion of the same decision (W3).
    """

    __tablename__ = "decisions"

    __table_args__ = (
        Index(
            "uq_decisions_context_dedup_active",
            "context",
            "dedup_key",
            unique=True,
            sqlite_where=text(
                "lifecycle_state IN ('candidate', 'validated', 'active')"
            ),
        ),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True)
    #: Canonical context (outer whitespace stripped, Unicode-aware — DB-4).
    #: ``from_pydantic`` stores ``canonical_context(decision.context)`` and the
    #: canonical_decision_context migration backfills legacy rows, so the raw
    #: ``context`` column always holds the canonical value and the partial
    #: unique index below operates on canonical keys.
    context: Mapped[str] = mapped_column(String, default="")
    choice: Mapped[str] = mapped_column(String, nullable=False)
    #: Normalized dedup key (whitespace-collapsed 200-char prefix of choice) —
    #: computed at insert time by ``from_pydantic``. Backfilled for legacy rows
    #: by the add_decision_unique_constraint migration.
    dedup_key: Mapped[str] = mapped_column(String, nullable=False, default="")
    rejected_alternatives: Mapped[str] = mapped_column(Text, default="[]")
    reason: Mapped[str] = mapped_column(String, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, default=1.0)
    source: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    creator: Mapped[str] = mapped_column(String, default="system")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    verification_status: Mapped[str] = mapped_column(String, default="candidate")
    lifecycle_state: Mapped[str] = mapped_column(String, default="active")
    version: Mapped[str] = mapped_column(String, default="0.1.0")

    def to_pydantic(self) -> Decision:
        import json

        return Decision(
            id=self.id,
            context=self.context,
            choice=self.choice,
            rejected_alternatives=json.loads(self.rejected_alternatives),
            reason=self.reason,
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
    def from_pydantic(cls, decision: Decision) -> "DecisionORM":
        import json

        return cls(
            id=decision.id,
            context=canonical_context(decision.context),
            choice=decision.choice,
            dedup_key=normalize_choice(decision.choice),
            rejected_alternatives=json.dumps(decision.rejected_alternatives),
            reason=decision.reason,
            confidence=decision.confidence,
            source=decision.source,
            creator=decision.creator,
            created_at=decision.created_at,
            updated_at=decision.updated_at,
            verification_status=decision.verification_status,
            lifecycle_state=decision.lifecycle_state,
            version=decision.version,
        )
