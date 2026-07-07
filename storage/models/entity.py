"""Entity ORM model — canonical SQL storage for entities."""

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import DateTime, Float, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from memory_server.models import Entity
from storage.base import Base


class EntityORM(Base):
    """SQLAlchemy ORM model for Entities — canonical fields."""

    __tablename__ = "entities"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    type: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    attributes: Mapped[str] = mapped_column(Text, default="{}")
    source: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    creator: Mapped[str] = mapped_column(String, default="system")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now(timezone.utc))
    confidence: Mapped[float] = mapped_column(Float, default=1.0)
    verification_status: Mapped[str] = mapped_column(String, default="candidate")
    lifecycle_state: Mapped[str] = mapped_column(String, default="active")
    version: Mapped[str] = mapped_column(String, default="0.1.0")

    def to_pydantic(self) -> Entity:
        import json

        return Entity(
            id=self.id,
            type=self.type,
            name=self.name,
            attributes=json.loads(self.attributes),
            source=self.source,
            creator=self.creator,
            created_at=self.created_at,
            updated_at=self.updated_at,
            confidence=self.confidence,
            verification_status=self.verification_status,
            lifecycle_state=self.lifecycle_state,
            version=self.version,
        )

    @classmethod
    def from_pydantic(cls, entity: Entity) -> "EntityORM":
        import json

        return cls(
            id=entity.id,
            type=entity.type,
            name=entity.name,
            attributes=json.dumps(entity.attributes),
            source=entity.source,
            creator=entity.creator,
            created_at=entity.created_at,
            updated_at=entity.updated_at,
            confidence=entity.confidence,
            verification_status=entity.verification_status,
            lifecycle_state=entity.lifecycle_state,
            version=entity.version,
        )
