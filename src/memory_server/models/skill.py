"""Skill model — procedural knowledge with versioning."""

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class Skill(BaseModel):
    """Procedural knowledge with steps, constraints, and validation.

    Contains purpose, steps, constraints, and validation criteria.

    Canonical fields per spec: id, type, content, source, creator,
    created_at, updated_at, confidence, verification_status,
    lifecycle_state, version.
    """

    id: str
    name: str
    version: str = "1.0.0"
    purpose: str
    steps: list[str] = Field(..., min_length=1)
    constraints: list[str] = []
    validation: list[str] = []
    success_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    source: Optional[str] = None
    creator: str = "system"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    verification_status: str = "candidate"
    lifecycle_state: str = "active"

    model_config = ConfigDict(from_attributes=True)
