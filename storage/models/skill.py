"""Skill ORM model — canonical SQL storage for skills."""

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import DateTime, Float, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from memory_server.models import Skill
from storage.base import Base


class SkillORM(Base):
    """SQLAlchemy ORM model for Skills — canonical fields."""

    __tablename__ = "skills"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String, default="")
    version: Mapped[str] = mapped_column(String, default="1.0.0")
    purpose: Mapped[str] = mapped_column(String, nullable=False)
    steps: Mapped[str] = mapped_column(Text, default="[]")
    constraints: Mapped[str] = mapped_column(Text, default="[]")
    validation: Mapped[str] = mapped_column(Text, default="[]")
    success_rate: Mapped[float] = mapped_column(Float, default=0.0)
    source: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    creator: Mapped[str] = mapped_column(String, default="system")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now(timezone.utc))
    confidence: Mapped[float] = mapped_column(Float, default=0.5)
    verification_status: Mapped[str] = mapped_column(String, default="candidate")
    lifecycle_state: Mapped[str] = mapped_column(String, default="active")

    def to_pydantic(self) -> Skill:
        import json

        return Skill(
            id=self.id,
            name=self.name,
            version=self.version,
            purpose=self.purpose,
            steps=json.loads(self.steps),
            constraints=json.loads(self.constraints),
            validation=json.loads(self.validation),
            success_rate=self.success_rate,
            source=self.source,
            creator=self.creator,
            created_at=self.created_at,
            updated_at=self.updated_at,
            confidence=self.confidence,
            verification_status=self.verification_status,
            lifecycle_state=self.lifecycle_state,
        )

    @classmethod
    def from_pydantic(cls, skill: Skill) -> "SkillORM":
        import json

        return cls(
            id=skill.id,
            name=skill.name,
            version=skill.version,
            purpose=skill.purpose,
            steps=json.dumps(skill.steps),
            constraints=json.dumps(skill.constraints),
            validation=json.dumps(skill.validation),
            success_rate=skill.success_rate,
            source=skill.source,
            creator=skill.creator,
            created_at=skill.created_at,
            updated_at=skill.updated_at,
            confidence=skill.confidence,
            verification_status=skill.verification_status,
            lifecycle_state=skill.lifecycle_state,
        )
