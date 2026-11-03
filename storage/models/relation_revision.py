"""Append-only canonical relation frontier, registered through existing metadata."""

from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, Integer, String, UniqueConstraint, event
from sqlalchemy.orm import Mapped, mapped_column

from storage.base import Base


class RelationRevisionORM(Base):
    __tablename__ = "relation_revisions"
    __table_args__ = (
        CheckConstraint(
            "typeof(revision) = 'integer' AND revision BETWEEN 1 AND 9223372036854775807", name="s16_revision_positive"
        ),
        CheckConstraint("deleted IN (0, 1)", name="s16_deleted_bool"),
        CheckConstraint(
            "relation_type IN ('supports', 'contradicts', 'derives', 'derived_from')", name="s16_relation_vocabulary"
        ),
        UniqueConstraint("event_id", name="s16_event_unique"),
    )

    source_id: Mapped[str] = mapped_column(String, primary_key=True)
    target_id: Mapped[str] = mapped_column(String, primary_key=True)
    relation_type: Mapped[str] = mapped_column(String, primary_key=True)
    revision: Mapped[int] = mapped_column(Integer, primary_key=True)
    deleted: Mapped[int] = mapped_column(Integer, nullable=False)
    relation_created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    changed_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    event_id: Mapped[str] = mapped_column(String, nullable=False)
    request_id: Mapped[str | None] = mapped_column(String, nullable=True)
    actor: Mapped[str | None] = mapped_column(String, nullable=True)
    action_receipt_id: Mapped[str | None] = mapped_column(String, nullable=True)


def _before_create(metadata, connection, **kwargs):
    from storage.repositories.relation_revision_repo import validate_occupied_relation_revisions

    validate_occupied_relation_revisions(connection)


if not event.contains(Base.metadata, "before_create", _before_create):
    event.listen(Base.metadata, "before_create", _before_create)
