"""Decision ORM model — canonical SQL storage for decisions."""

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import Float, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from memory_server.models import Decision
from storage.base import Base


class DecisionORM(Base):
    """SQLAlchemy ORM model for Decisions — canonical fields."""

    __tablename__ = "decisions"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    context: Mapped[str] = mapped_column(String, default="")
    choice: Mapped[str] = mapped_column(String, nullable=False)
    rejected_alternatives: Mapped[str] = mapped_column(Text, default="[]")
    reason: Mapped[str] = mapped_column(String, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, default=1.0)
    source: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    creator: Mapped[str] = mapped_column(String, default="system")
    created_at: Mapped[datetime] = mapped_column(String, default=datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(String, default=datetime.now(timezone.utc))
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
            context=decision.context,
            choice=decision.choice,
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
