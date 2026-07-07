"""Lifecycle ORM models — lifecycle state and event history tracking."""

from datetime import datetime, timezone

from sqlalchemy import Float, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from storage.base import Base


class LifecycleStateORM(Base):
    """Current lifecycle state for any entity/fact/decision/skill.

    Tracks the lifecycle_state field provenance independently of
    the main tables, enabling rebuild from canonical SQL.
    """

    __tablename__ = "lifecycle_states"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    memory_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    memory_type: Mapped[str] = mapped_column(String, nullable=False)
    current_state: Mapped[str] = mapped_column(String, nullable=False)
    previous_state: Mapped[str] = mapped_column(String, nullable=True)
    confidence: Mapped[float] = mapped_column(Float, default=0.5)
    updated_at: Mapped[datetime] = mapped_column(String, default=datetime.now(timezone.utc))


class LifecycleEventORM(Base):
    """Audit trail for every lifecycle state transition."""

    __tablename__ = "lifecycle_events"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    memory_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    memory_type: Mapped[str] = mapped_column(String, nullable=False)
    from_state: Mapped[str] = mapped_column(String, nullable=False)
    to_state: Mapped[str] = mapped_column(String, nullable=False)
    reason: Mapped[str] = mapped_column(String, default="")
    triggered_by: Mapped[str] = mapped_column(String, default="system")
    timestamp: Mapped[datetime] = mapped_column(String, default=datetime.now(timezone.utc))
